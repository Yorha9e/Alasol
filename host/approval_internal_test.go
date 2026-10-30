package host

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	"github.com/Yorha9e/alasol/runtime/transport"
)

// testBroadcaster 记录广播的 kind（缓冲通道；测试用 transport.Broadcaster）。
type testBroadcaster struct {
	ch chan string
}

func newTestBroadcaster() *testBroadcaster { return &testBroadcaster{ch: make(chan string, 16)} }

func (b *testBroadcaster) Broadcast(kind string, _ any) {
	select {
	case b.ch <- kind:
	default: // 缓冲区满：丢弃（测试只关心"广播过没有"）
	}
}

// waitKind 等一条广播并断言其 kind（超时 = 失败）。
func (b *testBroadcaster) waitKind(t *testing.T, want string) {
	t.Helper()
	select {
	case k := <-b.ch:
		if k != want {
			t.Fatalf("广播 kind = %q，want %q", k, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("等不到广播 %q", want)
	}
}

func testAskReq() contracts.ApprovalRequest {
	return contracts.ApprovalRequest{
		V: 1, ID: core.NewUlid(), TS: "2026-01-01T00:00:00Z",
		SessionID: core.NewUlid(), AgentID: core.NewUlid(),
		Trigger: contracts.ApprovalTriggerPolicyAsk,
		Summary: "策略拦截：工具 read 需要人审",
	}
}

// TestProtocolApproverFailCloseOnTimeout（G1 fail-close 变异锚）：无人应答（ctx 超时）
// → **deny**，不得挂起、不得默认放行（D14/H8/陷阱 9）。
func TestProtocolApproverFailCloseOnTimeout(t *testing.T) {
	bc := newTestBroadcaster()
	pa := newProtocolApprover(bc)
	req := testAskReq()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	resp, err := pa.Approve(ctx, req)
	if err != nil {
		t.Fatalf("Approve 返回 error（拒绝是正常结论，不该是 error）: %v", err)
	}
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Fatalf("无人应答必须 fail-close 成 deny，得到 %q", resp.Decision)
	}
	if resp.RequestID != req.ID {
		t.Fatalf("deny 响应 request_id = %q，want %q", resp.RequestID, req.ID)
	}
	if resp.Note == nil || resp.DecidedBy == nil {
		t.Fatalf("deny 响应应带 note/decided_by（审计）: %+v", resp)
	}
	// 期间广播过一条 approval.request（请求确实推到了协议面）。
	bc.waitKind(t, transport.KindApprovalRequest)

	// 超时后迟到的应答 → 不被接受（那个 loop 早已按 deny 收场）。
	late := contracts.ApprovalResponse{V: 1, RequestID: req.ID, TS: req.TS, Decision: contracts.ApprovalDecisionAllow}
	if pa.Respond(late) {
		t.Fatalf("超时后迟到的 approval.respond 不该被接受（没有挂起的审批了）")
	}
}

// TestProtocolApproverRespondUnblocks（G1 正向）：应答注入 → Approve 拿到 allow 返回。
func TestProtocolApproverRespondUnblocks(t *testing.T) {
	bc := newTestBroadcaster()
	pa := newProtocolApprover(bc)
	req := testAskReq()

	done := make(chan contracts.ApprovalResponse, 1)
	go func() {
		resp, _ := pa.Approve(context.Background(), req)
		done <- resp
	}()
	// 等请求广播出来（此刻 pending 已注册——广播在注册之后）。
	bc.waitKind(t, transport.KindApprovalRequest)

	if !pa.Respond(contracts.ApprovalResponse{
		V: 1, RequestID: req.ID, TS: req.TS,
		Decision: contracts.ApprovalDecisionAllow, DecidedBy: strPtrApproval("user:test"),
	}) {
		t.Fatalf("挂起中的审批应被 Respond 接受")
	}
	resp := <-done
	if resp.Decision != contracts.ApprovalDecisionAllow {
		t.Fatalf("应答 allow 后 Approve 应返回 allow，得到 %q", resp.Decision)
	}
	// 同一条再 Respond → false（已消费，pending 已摘）。
	if pa.Respond(contracts.ApprovalResponse{RequestID: req.ID, Decision: contracts.ApprovalDecisionAllow}) {
		t.Fatalf("已消费的审批不该被二次接受")
	}
}

// TestProtocolApproverDenyResponsePropagates（G1）：客户端显式 deny → Approve 返回 deny。
func TestProtocolApproverDenyResponsePropagates(t *testing.T) {
	bc := newTestBroadcaster()
	pa := newProtocolApprover(bc)
	req := testAskReq()

	done := make(chan contracts.ApprovalResponse, 1)
	go func() {
		resp, _ := pa.Approve(context.Background(), req)
		done <- resp
	}()
	bc.waitKind(t, transport.KindApprovalRequest)
	pa.Respond(contracts.ApprovalResponse{
		V: 1, RequestID: req.ID, TS: req.TS,
		Decision: contracts.ApprovalDecisionDeny, Note: strPtrApproval("用户拒绝"), DecidedBy: strPtrApproval("user:test"),
	})
	if resp := <-done; resp.Decision != contracts.ApprovalDecisionDeny {
		t.Fatalf("客户端 deny 应透传，得到 %q", resp.Decision)
	}
}

// TestProtocolApproverNilBroadcasterStillFailsClose：broadcaster 为 nil（请求推不出去）
// 也不该挂死——等 ctx 超时兜底 deny（fail-close 不依赖广播成功）。
func TestProtocolApproverNilBroadcasterStillFailsClose(t *testing.T) {
	pa := newProtocolApprover(nil)
	req := testAskReq()
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	resp, _ := pa.Approve(ctx, req)
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Fatalf("无 broadcaster 也必须 fail-close 成 deny，得到 %q", resp.Decision)
	}
}

// TestProtocolApproverRespondRaceWithCtxCancel（P2-4 钉子）：
//
// 竞态：应答被接受（Respond 返回 true = HTTP 200 "已投递"）与等待者的 ctx 结束
// （整轮超时/取消）可能同时发生。铁律：**只要应答被接受过，等待者收场时就不能写
// "无人应答"**——审计不能把"有人答了但没赶上"记成"没人答"。要么按 allow 放行，
// 要么写"已收到应答但上下文已结束"（fail-close：ctx 结束仍落 deny）。
//
// 跑多轮让两种交错都出现过（哪次是哪种交错由调度决定，断言的是**不变量**）。
func TestProtocolApproverRespondRaceWithCtxCancel(t *testing.T) {
	for i := 0; i < 200; i++ {
		bc := newTestBroadcaster()
		pa := newProtocolApprover(bc)
		req := testAskReq()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan contracts.ApprovalResponse, 1)
		go func() {
			resp, _ := pa.Approve(ctx, req)
			done <- resp
		}()
		bc.waitKind(t, transport.KindApprovalRequest)

		// 同时发生：取消 ctx + 投递应答（谁先谁后不确定——这正是要覆盖的竞态）。
		cancel()
		accepted := pa.Respond(contracts.ApprovalResponse{
			V: 1, RequestID: req.ID, TS: req.TS,
			Decision: contracts.ApprovalDecisionAllow, DecidedBy: strPtrApproval("user:test"),
		})
		resp := <-done

		// ctx 已结束 → 一律 fail-close 落 deny（放行与否不因应答到达而改变）。
		if resp.Decision != contracts.ApprovalDecisionDeny {
			t.Fatalf("第 %d 轮：ctx 已结束必须落 deny，得到 %q", i, resp.Decision)
		}
		note := ""
		if resp.Note != nil {
			note = *resp.Note
		}
		if accepted && strings.Contains(note, "无人应答") {
			t.Fatalf("第 %d 轮：应答已被接受（HTTP 200）却审计成%q——"+
				"应答已投递 ≠ 已放行，但审计不能把'有人答了'记成'无人应答'", i, note)
		}
		if accepted && !strings.Contains(note, "已收到应答") {
			t.Fatalf("第 %d 轮：应答已被接受，note 应写'已收到应答但上下文已结束'，得到 %q", i, note)
		}
	}
}

// TestProtocolApproverRespondAfterWaiterGone（P2-4 钉子·另一半）：等待者已退出（ctx 结束）
// 之后来的应答 → **不被接受**（false）：不能把应答投进一个没人读的通道、再回 HTTP 200
// 骗客户端"应答生效了"（那一轮早已按 deny 收场）。
func TestProtocolApproverRespondAfterWaiterGone(t *testing.T) {
	bc := newTestBroadcaster()
	pa := newProtocolApprover(bc)
	req := testAskReq()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan contracts.ApprovalResponse, 1)
	go func() {
		resp, _ := pa.Approve(ctx, req)
		done <- resp
	}()
	bc.waitKind(t, transport.KindApprovalRequest)

	cancel()
	<-done // 等等待者彻底退出（摘表项 + 置位）

	if pa.Respond(contracts.ApprovalResponse{
		V: 1, RequestID: req.ID, TS: req.TS, Decision: contracts.ApprovalDecisionAllow,
	}) {
		t.Fatalf("等待者已退出后的迟到应答不该被接受（不能假成功）")
	}
}

// TestProtocolApproverRespondInvalidDecisionKeepsPending（P3-4 钉子）：非法 decision 的
// 应答 → **不被接受**，且**不消费**挂起的审批（乱批不该把等着人审的那一条吃掉）。
func TestProtocolApproverRespondInvalidDecisionKeepsPending(t *testing.T) {
	bc := newTestBroadcaster()
	pa := newProtocolApprover(bc)
	req := testAskReq()

	done := make(chan contracts.ApprovalResponse, 1)
	go func() {
		resp, _ := pa.Approve(context.Background(), req)
		done <- resp
	}()
	bc.waitKind(t, transport.KindApprovalRequest)

	if pa.Respond(contracts.ApprovalResponse{
		V: 1, RequestID: req.ID, TS: req.TS, Decision: contracts.ApprovalDecision("maybe"),
	}) {
		t.Fatalf("非法 decision 不该被接受")
	}
	// 挂起的审批还在：一条合法 allow 仍被接受。
	if !pa.Respond(contracts.ApprovalResponse{
		V: 1, RequestID: req.ID, TS: req.TS, Decision: contracts.ApprovalDecisionAllow,
	}) {
		t.Fatalf("非法 decision 不该吃掉挂起的审批（合法 allow 应被接受）")
	}
	resp := <-done
	if resp.Decision != contracts.ApprovalDecisionAllow {
		t.Fatalf("合法 allow 后应放行，得到 %q", resp.Decision)
	}
}

// ---------------------------------------------------------------------------
// 确定性钉子：resp 分支的 ctx 复核（P2-4）
// ---------------------------------------------------------------------------

// errOnSecondCallCtx 是**假 ctx**：Err() 第二次调用才返回错误，Done() 永不触发。
//
// 为什么需要它：审批应答与 ctx 结束的竞态在真 ctx 上**不可确定性复现**（谁先谁后
// 由调度决定）。这个假 ctx 把时序钉死：应答先到（走 resp 分支），随后 ctx.Err()
// 才报错 —— 于是"resp 分支的 ctx 复核"（已收到应答但上下文已结束 → deny）这条
// 代码路径被**确定性**走到，删掉它必红。
type errOnSecondCallCtx struct {
	mu    sync.Mutex
	calls int
}

func (c *errOnSecondCallCtx) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *errOnSecondCallCtx) Done() <-chan struct{}       { return nil } // 永不触发
func (c *errOnSecondCallCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls >= 2 {
		return errors.New("假 ctx：第二遍 Err() 才结束")
	}
	return nil
}
func (c *errOnSecondCallCtx) Value(any) any { return nil }

// TestProtocolApproverRespBranchRechecksCtx（P2-4 钉子·确定性）：
//
// resp 分支（**应答先到**）必须**再看一眼 ctx**：ctx 已结束就按 deny 收场（fail-close），
// 且 note 写"**已收到应答**"（与"无人应答"区分——审计不能把"有人答了"记成"没人答"）。
//
// 时序由假 ctx 钉死（Err() 第二遍才报错）：Approve 开头的 ctx 检查（第一遍）通过 →
// 应答注入 → resp 分支的 ctx 复核（第二遍）命中 → deny + "已收到应答"。
func TestProtocolApproverRespBranchRechecksCtx(t *testing.T) {
	bc := newTestBroadcaster()
	pa := newProtocolApprover(bc)
	req := testAskReq()

	ctx := &errOnSecondCallCtx{}
	done := make(chan contracts.ApprovalResponse, 1)
	go func() {
		resp, _ := pa.Approve(ctx, req)
		done <- resp
	}()
	// 等请求广播出来（此刻 pending 已注册）。
	bc.waitKind(t, transport.KindApprovalRequest)

	// 应答先到（被接受 = true = "已投递"）。
	if !pa.Respond(contracts.ApprovalResponse{
		V: 1, RequestID: req.ID, TS: req.TS,
		Decision: contracts.ApprovalDecisionAllow, DecidedBy: strPtrApproval("user:test"),
	}) {
		t.Fatalf("挂起中的审批应被 Respond 接受")
	}

	resp := <-done
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Fatalf("ctx 已结束必须 fail-close 成 deny，得到 %q（应答不该放行）", resp.Decision)
	}
	if resp.Note == nil || !strings.Contains(*resp.Note, "已收到应答") {
		t.Fatalf("note 应写\"已收到应答\"（与\"无人应答\"区分），得到 %v", resp.Note)
	}
	if resp.Note != nil && strings.Contains(*resp.Note, "无人应答") {
		t.Fatalf("note 不该写\"无人应答\"（有人答了），得到 %q", *resp.Note)
	}
}

// ---------------------------------------------------------------------------
// 回归：hub.Close() 撞上在跑的协议面 prompt（p3-1 质量债）
// ---------------------------------------------------------------------------

// safeLogBuf 是并发安全的日志缓冲（prompt goroutine 写、断言方读 → 不能用裸 bytes.Buffer）。
type safeLogBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeLogBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeLogBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitLogContains 等日志里出现某段文字（超时 = 失败）。
func waitLogContains(t *testing.T, buf *safeLogBuf, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s 内日志里没出现 %q；日志内容:\n%s", timeout, want, buf.String())
}

// toolCallProvider：第一次调用发起一次 read 工具调用（→ ask 策略触发审批），
// 之后只回文本。
type toolCallProvider struct {
	calls int
}

func (p *toolCallProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if p.calls == 1 {
		tc := core.ToolCallReq{
			ID:   contracts.ProviderId("toolu_01"),
			Name: "read",
			Args: json.RawMessage(`{"path":"whatever"}`),
		}
		emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 1, OutputTokens: 1}}, nil
	}
	emit(core.ProviderEvent{Kind: "text.delta", Text: "好了"})
	emit(core.ProviderEvent{Kind: "done"})
	return core.ModelResult{Text: "好了", Usage: core.Usage{InputTokens: 1, OutputTokens: 1}}, nil
}

// TestCloseCancelsInFlightProtocolPrompt（p3-1 回归）：协议面**在跑的** prompt 撞上
// hub.Close() → 那一轮**立刻收场**（fail-close：取消即 deny），且**不往已关闭的事件库
// 写事件**。
//
// 钉的是"协议面 prompt 的 ctx 派生自 hub 生命周期"这条：没有它，prompt 的 ctx 是
// context.Background() → 关库后那一轮还挂着（最长一轮 600s），醒来时往**已关的库**
// 写事件（"database is closed"），或者干脆永远挂着（审批等一个已经没人应的门）。
func TestCloseCancelsInFlightProtocolPrompt(t *testing.T) {
	// 捕获日志（宿主层的告警出口是 hubLog）。
	var buf safeLogBuf
	old := hubLog
	hubLog = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	defer func() { hubLog = old }()

	dbPath := filepath.Join(t.TempDir(), "events.db")
	h, err := New(Config{
		DBPath:   dbPath,
		Model:    "scripted",
		Approval: "protocol", // 审批走协议面（ask → 挂起等人应答）
		Ask:      []string{"read"},
		CWD:      t.TempDir(),
		Provider: &toolCallProvider{},
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}

	// 起一轮协议面 prompt（→ ask 触发 → 挂在审批上等人应答）。
	if !h.startPromptAsync(NewSession(), "读一下") {
		t.Fatal("startPromptAsync 应受理")
	}
	// 等它挂到审批上（pending 表非空 = 正在等人应答）。
	deadline := time.Now().Add(10 * time.Second)
	for {
		h.protocolApprover.mu.Lock()
		pending := len(h.protocolApprover.pending)
		h.protocolApprover.mu.Unlock()
		if pending > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("prompt 没挂到审批上（测试前提不成立）")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 关 hub：生命周期 ctx 取消 → 那一轮立刻收场；Close 等它退出后才关库。
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 断言①：那一轮**结束了**（日志里有"协议面 prompt 跑失败"+取消原因）。
	// 没有修复时它一直挂着（ctx 是 Background → 审批永远等不到应答）→ 这里必红。
	waitLogContains(t, &buf, "协议面 prompt 跑失败", 10*time.Second)
	// 断言②：没往**已关的库**写事件（那种噪声正是这条债要消掉的）。
	if s := buf.String(); strings.Contains(s, "database is closed") {
		t.Fatalf("关库后不该再写事件（往已关的库写）:\n%s", s)
	}
	// 断言③：取消的 prompt 不该执行工具（fail-close：取消即 deny）——重开库读事件。
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("重开事件库: %v", err)
	}
	defer func() { _ = st.Close() }()
	evs, err := st.ReadFrom(context.Background(), nil, 1, 0)
	if err != nil {
		t.Fatalf("读事件: %v", err)
	}
	for _, ev := range evs {
		if ev.Kind == "tool.call.completed" {
			t.Fatalf("取消的 prompt 不该执行工具（fail-close）: %+v", ev)
		}
	}
}
