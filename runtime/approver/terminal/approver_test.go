package terminal

import (
	"context"
	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// sampleRequest 造一个典型的策略拦截审批请求（policy_ask + tool_call_id + 副作用 + detail）。
func sampleRequest() contracts.ApprovalRequest {
	side := contracts.SideEffectWrite
	tc := contracts.ProviderId("call_AbC-123")
	return contracts.ApprovalRequest{
		V:          1,
		ID:         contracts.Ulid("01JXXXXXXXXXXXXXXXXXXXXXXXXX"),
		TS:         "2026-10-28T00:00:00Z",
		SessionID:  contracts.Ulid("01JYYYYYYYYYYYYYYYYYYYYYYYYY"),
		AgentID:    contracts.Ulid("01JZZZZZZZZZZZZZZZZZZZZZZZZZ"),
		Trigger:    contracts.ApprovalTriggerPolicyAsk,
		ToolCallID: &tc,
		SideEffect: &side,
		Summary:    "策略拦截：工具 write 需要人审（规则 write-guard）",
		Detail:     []byte(`{"tool":"write","args":{"path":"a.txt"}}`),
	}
}

// normalize 把输出里的平台行尾统一成 \n（Windows 是 \r\n），便于断言。
func normalize(s string) string { return strings.ReplaceAll(s, "\r\n", "\n") }

// decide 跑一次审批，返回 (决策, 备注, 输出)。
func decide(t *testing.T, in string, req contracts.ApprovalRequest) (contracts.ApprovalDecision, string, string) {
	t.Helper()
	var out strings.Builder
	a := New(strings.NewReader(in), &out)
	resp, err := a.Approve(context.Background(), req)
	if err != nil {
		t.Fatalf("Approve 不应返回 error（拒绝是正常结论）：%v", err)
	}
	note := ""
	if resp.Note != nil {
		note = *resp.Note
	}
	return resp.Decision, note, normalize(out.String())
}

func TestApproveAllow(t *testing.T) {
	for _, in := range []string{"y", "y\n", "Y\n", "yes\n", "YES\n", "  y  \n", "y\r\n"} {
		t.Run(strings.TrimSpace(in), func(t *testing.T) {
			req := sampleRequest()
			d, note, out := decide(t, in, req)
			if d != contracts.ApprovalDecisionAllow {
				t.Fatalf("输入 %q → %s，期望 allow（note=%q）", in, d, note)
			}
			if note != "" {
				t.Errorf("放行时不该带拒绝备注：%q", note)
			}
			// 提示必须把关键信息打出来（审批人得知道在批什么）。
			for _, want := range []string{"审批请求", "策略拦截", "write-guard", "call_AbC-123", "write", `{"tool":"write","args":{"path":"a.txt"}}`, "[y/N]"} {
				if !strings.Contains(out, want) {
					t.Errorf("输出缺 %q：\n%s", want, out)
				}
			}
		})
	}
}

func TestApproveDenyFailClose(t *testing.T) {
	// 一切非显式同意都拒绝（含空回车、EOF、乱输、近似词）。
	for _, in := range []string{"", "\n", "n\n", "no\n", "N\n", "always\n", "a\n", "yep\n", "sure\n", "true\n", "1\n", "garbage\n", "yy\n"} {
		t.Run("in="+strings.TrimSpace(in), func(t *testing.T) {
			d, note, _ := decide(t, in, sampleRequest())
			if d != contracts.ApprovalDecisionDeny {
				t.Fatalf("输入 %q → %s，期望 deny（fail-close）", in, d)
			}
			if !strings.Contains(note, "fail-close") {
				t.Errorf("拒绝备注未标 fail-close：%q", note)
			}
		})
	}
}

func TestApproveResponseShape(t *testing.T) {
	req := sampleRequest()
	var out strings.Builder
	a := New(strings.NewReader("y\n"), &out)
	resp, err := a.Approve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.V != 1 {
		t.Errorf("V = %d", resp.V)
	}
	if resp.RequestID != req.ID {
		t.Errorf("RequestID = %q，期望回填请求 id %q", resp.RequestID, req.ID)
	}
	if resp.TS != req.TS {
		t.Errorf("TS = %q，期望 %q", resp.TS, req.TS)
	}
	if resp.DecidedBy == nil || *resp.DecidedBy != decidedBy {
		t.Errorf("DecidedBy = %v，期望 %q", resp.DecidedBy, decidedBy)
	}

	// 拒绝响应：RequestID/TS 同样要回填（审计要对得上）。
	dresp, err := New(strings.NewReader("n\n"), &out).Approve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if dresp.RequestID != req.ID || dresp.TS != req.TS {
		t.Errorf("拒绝响应未回填请求标识：%+v", dresp)
	}
	if dresp.DecidedBy == nil || !strings.HasPrefix(*dresp.DecidedBy, "auto:") {
		t.Errorf("拒绝的 DecidedBy = %v，期望 auto: 前缀（标明不是人同意）", dresp.DecidedBy)
	}
}

func TestApproveMinimalRequest(t *testing.T) {
	// 没有 ToolCallID / SideEffect / Detail 的请求（escalate 场景）也不许 panic。
	req := contracts.ApprovalRequest{
		V:       1,
		ID:      contracts.Ulid("01JXXXXXXXXXXXXXXXXXXXXXXXXX"),
		TS:      "2026-10-28T00:00:00Z",
		Trigger: contracts.ApprovalTriggerEscalate,
		Summary: "agent 主动升级：需要人决定方向",
	}
	var out strings.Builder
	resp, err := New(strings.NewReader("y\n"), &out).Approve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Decision != contracts.ApprovalDecisionAllow {
		t.Errorf("Decision = %s", resp.Decision)
	}
	got := normalize(out.String())
	if !strings.Contains(got, "escalate") || !strings.Contains(got, "[y/N]") {
		t.Errorf("输出 = %q", got)
	}
	if strings.Contains(got, "工具调用:") {
		t.Error("没有 ToolCallID 时不该打印该行")
	}
}

func TestApproveContextTimeout(t *testing.T) {
	// 无人输入 + ctx 超时 → deny，且及时返回（不挂起）。
	pr, pw := io.Pipe()
	defer pw.Close()
	var out strings.Builder
	a := New(pr, &out)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	start := time.Now()
	resp, err := a.Approve(ctx, sampleRequest())
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("超时未及时返回：%v", elapsed)
	}
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Errorf("Decision = %s，期望 deny", resp.Decision)
	}
	if resp.Note == nil || !strings.Contains(*resp.Note, "fail-close") {
		t.Errorf("Note = %v", resp.Note)
	}
	if !strings.Contains(normalize(out.String()), "[y/N]") {
		t.Error("超时前应已打印提示")
	}
}

func TestApproveContextAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out strings.Builder
	resp, err := New(strings.NewReader("y\n"), &out).Approve(ctx, sampleRequest())
	if err != nil {
		t.Fatal(err)
	}
	// 即使输入是 y，上下文已结束也一律 deny（fail-close：不许"事后追认"）。
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Errorf("Decision = %s，期望 deny", resp.Decision)
	}
	if strings.Contains(out.String(), "[y/N]") {
		t.Error("上下文已结束时不该打印提示")
	}
}

func TestApproveEOFDenies(t *testing.T) {
	// 输入流已结束（EOF）：无人应答 → deny。
	d, note, _ := decide(t, "", sampleRequest())
	if d != contracts.ApprovalDecisionDeny {
		t.Fatalf("EOF → %s", d)
	}
	if !strings.Contains(note, "fail-close") {
		t.Errorf("note = %q", note)
	}
}

func TestApproveNilInputAndOutput(t *testing.T) {
	// nil 输入源 = 无人应答；nil 输出 = 提示丢弃。都不许 panic，都必须 deny。
	a := New(nil, nil)
	resp, err := a.Approve(context.Background(), sampleRequest())
	if err != nil {
		t.Fatal(err)
	}
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Errorf("nil in/out → %s，期望 deny", resp.Decision)
	}
}

func TestApproveNilReceiver(t *testing.T) {
	var a *Approver
	resp, err := a.Approve(context.Background(), sampleRequest())
	if err != nil {
		t.Fatal(err)
	}
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Errorf("nil 接收者 → %s，期望 deny", resp.Decision)
	}
	if resp.Note == nil || !strings.Contains(*resp.Note, "fail-close") {
		t.Errorf("Note = %v", resp.Note)
	}
}

// TestApproveSerialized 并发审批必须串行（输出不交错、每人拿到自己的答复）。
func TestApproveSerialized(t *testing.T) {
	var out syncBuilder
	a := New(strings.NewReader("y\ny\ny\n"), &out)
	req := sampleRequest()

	done := make(chan contracts.ApprovalDecision, 3)
	for i := 0; i < 3; i++ {
		go func() {
			resp, err := a.Approve(context.Background(), req)
			if err != nil {
				t.Error(err)
				done <- contracts.ApprovalDecisionDeny
				return
			}
			done <- resp.Decision
		}()
	}
	for i := 0; i < 3; i++ {
		if d := <-done; d != contracts.ApprovalDecisionAllow {
			t.Errorf("第 %d 个审批 = %s，期望 allow（三行 y 应对应三次放行）", i, d)
		}
	}
	// 三个提示块齐全（串行打印，没有交错丢内容）。
	if n := strings.Count(normalize(out.String()), "允许执行？[y/N]"); n != 3 {
		t.Errorf("提示出现 %d 次，期望 3", n)
	}
}

// TestApproveStaleLineDropped 超时放弃后迟到的输入不该被下一个审批误用为同意。
// （设计上迟到输入会进缓冲并被下一个提示消费——这里固化"不会被无限堆积"的行为：
// 缓冲之外的输入被丢弃，读行 goroutine 不会因此堵死。）
func TestApproveStaleLineDropped(t *testing.T) {
	pr, pw := io.Pipe()
	var out strings.Builder
	a := New(pr, &out)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	resp, err := a.Approve(ctx, sampleRequest())
	if err != nil || resp.Decision != contracts.ApprovalDecisionDeny {
		t.Fatalf("超时应 deny：%+v %v", resp, err)
	}

	// 迟到的输入写进管道（此时已无人在等）。
	for i := 0; i < staleLineBuffer+4; i++ {
		if _, err := io.WriteString(pw, "y\n"); err != nil {
			break
		}
	}
	// 再问一次：要么消费到缓冲里的 y（allow），要么缓冲被丢光后无人应答（deny）。
	// 两种都不许挂死。
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	resp2, err := a.Approve(ctx2, sampleRequest())
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Decision != contracts.ApprovalDecisionAllow && resp2.Decision != contracts.ApprovalDecisionDeny {
		t.Errorf("Decision = %s", resp2.Decision)
	}
	pw.Close()
}

// TestIsYes 固化放行判定表（唯一的白名单）。
func TestIsYes(t *testing.T) {
	allow := []string{"y", "Y", "yes", "YES", " y ", "\tyes\r"}
	for _, in := range allow {
		if !isYes(in) {
			t.Errorf("isYes(%q) = false，期望 true", in)
		}
	}
	deny := []string{"", " ", "n", "no", "N", "always", "a", "yep", "sure", "1", "true", "yy", "yes!", "yess"}
	for _, in := range deny {
		if isYes(in) {
			t.Errorf("isYes(%q) = true，期望 false（fail-close：非 y/yes 一律拒）", in)
		}
	}
}

func TestCompactJSONAndTruncate(t *testing.T) {
	if got := compactJSON([]byte(`{"a": 1,  "b": [1, 2]}`)); got != `{"a":1,"b":[1,2]}` {
		t.Errorf("compactJSON = %q", got)
	}
	if got := compactJSON([]byte(`{oops`)); strings.Contains(got, "\n") {
		t.Errorf("非法 JSON 也不该带换行：%q", got)
	}
	if got := truncate(strings.Repeat("x", 100), maxNoteInput); len(got) != maxNoteInput+len("…") {
		t.Errorf("未按上限截断：%d", len(got))
	}
	if got := truncate("short", maxNoteInput); got != "short" {
		t.Errorf("短文本被误截：%q", got)
	}
}

// TestApproverSatisfiesInterface 编译期已断言，这里再验一次"默认形态"可当
// core.Approver 传给 policy.Decide（换实现不动 core 的酸测试）。
func TestApproverSatisfiesInterface(t *testing.T) {
	var a core.Approver = New(strings.NewReader("n\n"), io.Discard)
	resp, err := a.Approve(context.Background(), sampleRequest())
	if err != nil {
		t.Fatal(err)
	}
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Errorf("Decision = %s", resp.Decision)
	}
	// core 自带的两个参照实现仍在（对照用，确认我们没有替换它们的语义）。
	if _, err := (core.DenyAll{}).Approve(context.Background(), sampleRequest()); err != nil {
		t.Fatal(err)
	}
	if _, err := (core.AlwaysAllow{}).Approve(context.Background(), sampleRequest()); err != nil {
		t.Fatal(err)
	}
}

// syncBuilder 是并发安全的 strings.Builder（测试用）。
type syncBuilder struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuilder) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuilder) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
