package terminal

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// staleLineBuffer 是读行 goroutine 的缓冲行数。
//
// 为什么要缓冲 + 非阻塞发送：ctx 超时放弃某个提示后，用户迟到的输入不能把读行
// goroutine 永久堵住（那就成了泄漏）。超出的行直接丢弃——它属于一个已经被放弃的
// 提示，不该被下一个审批读到。
const staleLineBuffer = 8

// maxNoteInput 是拒绝备注里回显用户输入的最大长度（防止一行垃圾把事件撑大）。
const maxNoteInput = 32

// maxDetailDisplay 是终端上展示 ApprovalRequest.Detail 的最大长度。
// 比备注宽松得多（审批人要看全参数才能判断），但仍设上限——Detail 由策略引擎生成，
// 理论上可能很长（比如整条 suggested_rules）。
const maxDetailDisplay = 512

// decidedBy 是放行时的决策者标识（契约 ApprovalResponse.decided_by）。
const decidedBy = "user:terminal"

// decidedByDeny 是拒绝时的决策者标识（与 core 的 auto:deny-all 同风格：
// 一看就知道这不是人点的同意）。
const decidedByDeny = "auto:terminal-fail-close"

// Approver 是 core.Approver 的最小终端实现。
//
// 并发安全：内部互斥锁保证一次只问一个审批（见包 doc）。
type Approver struct {
	in  io.Reader
	out io.Writer

	mu     sync.Mutex
	once   sync.Once
	lines  chan string
	failed chan struct{} // 读行 goroutine 退出（EOF/读错误）时关闭
}

// 编译期断言：*Approver 必须满足 core.Approver。
var _ core.Approver = (*Approver)(nil)

// New 造终端审批器。
//
// nil 语义（兜底，不 panic，与 core/doc.go 的"兜底"一档同口径）：
//
//   - in=nil → 换成空 reader：没有任何输入来源 = 无人应答 = 一律 deny（fail-close）；
//   - out=nil → 换成 io.Discard：提示写不出去也不影响"拒绝"这个安全结论。
//
// 两种情况都不会放行任何东西，只是把"没人"显式化。
func New(in io.Reader, out io.Writer) *Approver {
	if in == nil {
		in = strings.NewReader("")
	}
	if out == nil {
		out = io.Discard
	}
	return &Approver{
		in:     in,
		out:    out,
		lines:  make(chan string, staleLineBuffer),
		failed: make(chan struct{}),
	}
}

// Approve implements core.Approver。
//
// 返回 error 恒为 nil：拒绝是**正常结论**，不是异常（调用方按 deny 处理即可，
// 与 policy.Decide 的 approveSafely 同口径）。
//
// **不会自己猜超时**：没人输入时就一直等下去（这正是"等人来决定"的语义）。
// 想限制等待时间，由调用方传带 deadline 的 ctx——provider/审批方都不该替人做决定。
func (a *Approver) Approve(ctx context.Context, req contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	if a == nil {
		// 有类型的 nil 接收者：无人可应答 → deny（fail-close）。
		return denyResponse(req, "fail-close: Approver 为 nil（无人可应答）"), nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		// 上下文已经结束：这次审批注定无人应答，连提示都不必打印。
		return denyResponse(req, fmt.Sprintf("fail-close: 审批前上下文已结束（%v）", err)), nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.writePrompt(req)

	answer, ok := a.readAnswer(ctx)
	if !ok {
		return denyResponse(req, "fail-close: 无人应答（ctx 超时/取消，或输入已结束）"), nil
	}
	// 读到答案后再看一眼 ctx：等输入等太久了，宁可当没答（fail-close）。
	if err := ctx.Err(); err != nil {
		return denyResponse(req, fmt.Sprintf("fail-close: 读到答复但上下文已结束（%v）", err)), nil
	}
	if !isYes(answer) {
		return denyResponse(req, fmt.Sprintf("fail-close: 输入 %q 不是 y/yes，按拒绝处理", truncate(answer, maxNoteInput))), nil
	}
	return contracts.ApprovalResponse{
		V:         1,
		RequestID: req.ID,
		TS:        req.TS,
		Decision:  contracts.ApprovalDecisionAllow,
		DecidedBy: strPtr(decidedBy),
	}, nil
}

// writePrompt 打印审批请求与 [y/N] 提示（提示行不带换行，用户在同一行输入）。
func (a *Approver) writePrompt(req contracts.ApprovalRequest) {
	var sb strings.Builder
	sb.WriteString(strings.Repeat("─", 60) + lineEnd)
	sb.WriteString("审批请求（需人审才能继续）" + lineEnd)
	fmt.Fprintf(&sb, "  触发器:   %s%s", req.Trigger, lineEnd)
	if req.ToolCallID != nil {
		fmt.Fprintf(&sb, "  工具调用: %s%s", *req.ToolCallID, lineEnd)
	}
	if req.SideEffect != nil {
		fmt.Fprintf(&sb, "  副作用:   %s%s", *req.SideEffect, lineEnd)
	}
	if strings.TrimSpace(req.Summary) != "" {
		fmt.Fprintf(&sb, "  摘要:     %s%s", req.Summary, lineEnd)
	}
	if len(req.Detail) > 0 {
		fmt.Fprintf(&sb, "  细节:     %s%s", compactJSON(req.Detail), lineEnd)
	}
	sb.WriteString("允许执行？[y/N] ")

	_, _ = io.WriteString(a.out, sb.String())
}

// readAnswer 读一行答复。
//
// 返回 (行, true) 或 ("", false)——false 覆盖三种"没人答"：ctx 结束、输入流已结束
// （EOF/读错误，见 startReader 关闭的 failed 通道）。
func (a *Approver) readAnswer(ctx context.Context) (string, bool) {
	a.startReader()

	// 已缓冲的行优先：上一个被放弃的提示可能留下了迟到输入。
	select {
	case line := <-a.lines:
		return line, true
	default:
	}

	select {
	case line := <-a.lines:
		return line, true
	case <-a.failed:
		return "", false
	case <-ctx.Done():
		return "", false
	}
}

// startReader 惰性启动唯一的读行 goroutine（每个 Approver 一个，不是每次审批一个）。
func (a *Approver) startReader() {
	a.once.Do(func() {
		go func() {
			defer close(a.failed)
			br := bufio.NewReader(a.in)
			for {
				line, err := br.ReadString('\n')
				// 行尾可能带 \r\n（Windows）：在这里统一去掉。
				if trimmed := strings.TrimRight(line, "\r\n"); trimmed != "" {
					select {
					case a.lines <- trimmed:
					default: // 无人消费（提示已被放弃）：丢弃，不堵住 goroutine
					}
				}
				if err != nil {
					return
				}
			}
		}()
	})
}

// isYes 是唯一的放行判定：**只有** y / yes（大小写不敏感、首尾空白忽略）。
// 其它一切——包括 "always"、"a"、空串、"y "（其实这个算 y）——都算拒绝。
func isYes(answer string) bool {
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		return true
	default:
		return false
	}
}

// denyResponse 造拒绝响应（fail-close 的统一出口，形状对齐 core.DenyAll）。
func denyResponse(req contracts.ApprovalRequest, note string) contracts.ApprovalResponse {
	return contracts.ApprovalResponse{
		V:         1,
		RequestID: req.ID,
		TS:        req.TS,
		Decision:  contracts.ApprovalDecisionDeny,
		Note:      strPtr(note),
		DecidedBy: strPtr(decidedByDeny),
	}
}

// compactJSON 把 Detail 压成一行（失败时退化成原文；两种情况都只影响"看得全不全"，
// 绝不影响审批结论——fail-close 不允许因为格式化失败就放行或拒绝）。
func compactJSON(raw json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return truncate(string(raw), maxDetailDisplay)
	}
	return truncate(buf.String(), maxDetailDisplay)
}

// truncate 按字符数截断（超长文本不该把终端/事件撑爆）。
func truncate(s string, limit int) string {
	s = strings.TrimSpace(s)
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "…"
}

func strPtr(s string) *string { return &s }
