package acp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// 端到端钉子（fake backend + 真 bus）：initialize→new→prompt→update 流→stopReason；
// 工具调用桥；cancel→cancelled；审批桥 allow/deny；未知 method→-32601；stdout 纯净性。

// ---------------------------------------------------------------------------
// fake backend（驱动 ACP 层；用真 bus 让事件桥走真实路径）
// ---------------------------------------------------------------------------

type fakeBackend struct {
	bus   *bus.Bus
	store *sqlite.Store

	mu       sync.Mutex
	sessions map[contracts.Ulid]bool
	onRun    func(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error)
}

func newFakeBackend(t *testing.T) *fakeBackend {
	t.Helper()
	st, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"), sqlite.Options{})
	if err != nil {
		t.Fatalf("开事件库: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return &fakeBackend{
		bus:      bus.New(st),
		store:    st,
		sessions: map[contracts.Ulid]bool{},
	}
}

func (b *fakeBackend) CreateSession(cwd string) (contracts.Ulid, error) {
	sid := core.NewUlid()
	b.mu.Lock()
	b.sessions[sid] = true
	b.mu.Unlock()
	return sid, nil
}

func (b *fakeBackend) RunPrompt(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error) {
	b.mu.Lock()
	on := b.onRun
	b.mu.Unlock()
	if on != nil {
		return on(ctx, sid, prompt)
	}
	return loop.Result{Text: "默认回复"}, nil
}

func (b *fakeBackend) SubscribeEvents(sid contracts.Ulid) *bus.Subscription {
	return b.bus.Subscribe(bus.Filter{SessionIDs: []contracts.Ulid{sid}})
}

// publishDelta 发一条 model.delta 流事件（durable=false，模拟 loop 的 G13 增量）。
func (b *fakeBackend) publishDelta(sid contracts.Ulid, text string) {
	payload, _ := json.Marshal(contracts.ModelDeltaPayload{Turn: 1, Text: &text})
	b.publish(contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: loop.KindModelDelta, TS: nowTS(),
		SessionID: sid, Durable: false, Payload: payload,
	})
}

func (b *fakeBackend) publishToolIntent(sid contracts.Ulid, tcid contracts.ProviderId, tool string, args json.RawMessage) {
	payload, _ := json.Marshal(contracts.ToolCallIntentPayload{ToolCallID: tcid, ToolName: tool, Arguments: args})
	b.publish(contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: loop.KindToolCallIntent, TS: nowTS(),
		SessionID: sid, Durable: true, Payload: payload,
	})
}

func (b *fakeBackend) publishToolCompleted(sid contracts.Ulid, tcid contracts.ProviderId, text string) {
	payload, _ := json.Marshal(contracts.ToolCallCompletedPayload{
		ToolCallID: tcid,
		Result:     contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &text}}},
	})
	b.publish(contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: loop.KindToolCallCompleted, TS: nowTS(),
		SessionID: sid, Durable: true, Payload: payload,
	})
}

func (b *fakeBackend) publish(ev contracts.Event) {
	_ = b.bus.Publish(context.Background(), ev)
}

func nowTS() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// ---------------------------------------------------------------------------
// lineSink：线程安全的出站收集器（stdout 的替身；非阻塞，避免读写死锁）
// ---------------------------------------------------------------------------

type lineSink struct {
	mu    sync.Mutex
	lines []string
	ch    chan struct{}
}

func newLineSink() *lineSink { return &lineSink{ch: make(chan struct{}, 1)} }

func (s *lineSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	for _, ln := range bytes.Split(bytes.TrimRight(p, "\n"), []byte("\n")) {
		if len(bytes.TrimSpace(ln)) > 0 {
			s.lines = append(s.lines, string(ln))
		}
	}
	s.mu.Unlock()
	select {
	case s.ch <- struct{}{}:
	default:
	}
	return len(p), nil
}

func (s *lineSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.lines...)
}

// waitCount 等到至少 n 行（或超时），返回当前快照。
func (s *lineSink) waitCount(n int, timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	for {
		cur := s.snapshot()
		if len(cur) >= n {
			return cur
		}
		rem := time.Until(deadline)
		if rem <= 0 {
			return cur
		}
		select {
		case <-s.ch:
		case <-time.After(rem):
		}
	}
}

// ---------------------------------------------------------------------------
// 客户端 harness
// ---------------------------------------------------------------------------

type acpClient struct {
	t        *testing.T
	w        *io.PipeWriter
	out      *lineSink
	idSeq    int
	serveErr chan error
}

func startACP(t *testing.T, backend Backend, approver *Approver, turnTimeout time.Duration) (*acpClient, func()) {
	t.Helper()
	out := newLineSink()
	pr, pw := io.Pipe()
	serveErr := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		serveErr <- Serve(ctx, Options{Backend: backend, Approver: approver, In: pr, Out: out, TurnTimeout: turnTimeout})
	}()
	c := &acpClient{t: t, w: pw, out: out, serveErr: serveErr}
	cleanup := func() {
		_ = pw.Close() // EOF → Serve 收场
		cancel()
		select {
		case <-serveErr:
		case <-time.After(3 * time.Second):
			t.Error("Serve 未在超时内收场")
		}
	}
	return c, cleanup
}

func (c *acpClient) writeFrame(o outbound) json.RawMessage {
	raw, err := json.Marshal(o)
	if err != nil {
		c.t.Fatalf("序列化请求: %v", err)
	}
	if _, err := c.w.Write(append(raw, '\n')); err != nil {
		c.t.Fatalf("写请求: %v", err)
	}
	return raw
}

// sendRequest 发一个带自增数字 id 的请求，返回该 id。
func (c *acpClient) sendRequest(method string, params any) json.RawMessage {
	c.idSeq++
	id := json.RawMessage(strconv.Itoa(c.idSeq))
	o, err := newRequest(id, method, params)
	if err != nil {
		c.t.Fatalf("构造请求: %v", err)
	}
	c.writeFrame(o)
	return id
}

// sendRawLine 原样发一行（用于坏帧测试）。
func (c *acpClient) sendRawLine(line string) {
	if _, err := c.w.Write([]byte(line + "\n")); err != nil {
		c.t.Fatalf("写原始行: %v", err)
	}
}

// frames 把当前出站快照解成 inbound（供断言）。走 decodeFrame——它设键在位标记，
// isResponse/isRequest 判别才准（裸 json.Unmarshal 不设 hasResult/hasError）。
func (c *acpClient) frames() []inbound {
	var out []inbound
	for _, ln := range c.out.snapshot() {
		f, err := decodeFrame([]byte(ln))
		if err != nil || f == nil {
			out = append(out, inbound{}) // 坏行占位（纯净性测试单独查）
			continue
		}
		out = append(out, *f)
	}
	return out
}

// waitForResult 等某个 id 的成功响应，返回其 result。
func (c *acpClient) waitForResult(id json.RawMessage, timeout time.Duration) (json.RawMessage, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, f := range c.frames() {
			if f.isResponse() && f.Result != nil && f.ID != nil && string(*f.ID) == string(id) {
				return *f.Result, true
			}
		}
		c.out.waitCount(len(c.out.snapshot())+1, 50*time.Millisecond)
	}
	return nil, false
}

// waitForError 等某个 id 的错误响应，返回 error 对象。
func (c *acpClient) waitForError(id json.RawMessage, timeout time.Duration) (*rpcError, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, f := range c.frames() {
			if f.isResponse() && f.Error != nil && f.ID != nil && string(*f.ID) == string(id) {
				return f.Error, true
			}
		}
		c.out.waitCount(len(c.out.snapshot())+1, 50*time.Millisecond)
	}
	return nil, false
}

// findUpdate 找出所有 session/update 通知（按 sessionUpdate 判别值过滤）。
func (c *acpClient) findUpdates(kind string) []sessionUpdate {
	var out []sessionUpdate
	for _, f := range c.frames() {
		if !f.isNotification() || f.Method == nil || *f.Method != "session/update" {
			continue
		}
		var p updateParams
		if json.Unmarshal(f.Params, &p) != nil {
			continue
		}
		if kind == "" || p.Update.SessionUpdate == kind {
			out = append(out, p.Update)
		}
	}
	return out
}

// contentText 从 update.Content（客户端侧解成 any：chunk=单个对象、tool=数组）取文本。
func contentText(v any) string {
	switch c := v.(type) {
	case contentBlock:
		return c.Text
	case map[string]any:
		if t, ok := c["text"].(string); ok {
			return t
		}
	case []any:
		var b strings.Builder
		for _, e := range c {
			b.WriteString(contentText(e))
		}
		return b.String()
	}
	return ""
}

// findRequestPermission 找第一条 session/request_permission 请求（agent→client）。
func (c *acpClient) findRequestPermission() (json.RawMessage, bool) {
	for _, f := range c.frames() {
		if f.isRequest() && f.Method != nil && *f.Method == methodRequestPermission && f.ID != nil {
			return *f.ID, true
		}
	}
	return nil, false
}

// respondPermission 回一条审批响应（client→agent）。
func (c *acpClient) respondPermission(id json.RawMessage, outcome string) {
	o, err := newResult(id, map[string]any{"outcome": outcome})
	if err != nil {
		c.t.Fatalf("构造审批响应: %v", err)
	}
	c.writeFrame(o)
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

// 全链路：initialize → session/new → session/prompt → agent_message_chunk → end_turn。
func TestACP_EndToEnd_PromptStreamsMessage(t *testing.T) {
	backend := newFakeBackend(t)
	backend.onRun = func(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error) {
		backend.publishDelta(sid, "你好")
		backend.publishDelta(sid, "，世界")
		return loop.Result{Text: "你好，世界", Turns: 1}, nil
	}
	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	// initialize
	id0 := client.sendRequest("initialize", map[string]any{"protocolVersion": 1, "clientInfo": map[string]any{"name": "test"}})
	res, ok := client.waitForResult(id0, 2*time.Second)
	if !ok {
		t.Fatal("没收到 initialize 响应")
	}
	var init initializeResult
	if err := json.Unmarshal(res, &init); err != nil {
		t.Fatal(err)
	}
	if init.ProtocolVersion != 1 || init.AgentInfo.Name != agentName {
		t.Errorf("initialize 结果不对: %+v", init)
	}
	if init.AgentCapabilities.LoadSession {
		t.Errorf("loadSession 应声明 false")
	}

	// session/new
	id1 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, ok = client.waitForResult(id1, 2*time.Second)
	if !ok {
		t.Fatal("没收到 session/new 响应")
	}
	var ns newSessionResult
	if err := json.Unmarshal(res, &ns); err != nil {
		t.Fatal(err)
	}
	if !core.ValidUlid(contracts.Ulid(ns.SessionID)) {
		t.Fatalf("sessionId 不是合法 ULID: %q", ns.SessionID)
	}

	// session/prompt
	id2 := client.sendRequest("session/prompt", map[string]any{
		"sessionId": ns.SessionID,
		"prompt":    []map[string]any{{"type": "text", "text": "打招呼"}},
	})
	res, ok = client.waitForResult(id2, 3*time.Second)
	if !ok {
		t.Fatal("没收到 session/prompt 响应")
	}
	var pr promptResult
	if err := json.Unmarshal(res, &pr); err != nil {
		t.Fatal(err)
	}
	if pr.StopReason != "end_turn" {
		t.Errorf("stopReason=%q want end_turn", pr.StopReason)
	}
	// 至少一条 agent_message_chunk（流式桥）。
	msgs := client.findUpdates("agent_message_chunk")
	if len(msgs) == 0 {
		t.Fatal("没桥出 agent_message_chunk")
	}
	// 增量文本应已拼出完整答复（降级分支不该再补一段——已桥过）。
	var joined strings.Builder
	for _, m := range msgs {
		joined.WriteString(contentText(m.Content))
	}
	if joined.String() != "你好，世界" {
		t.Errorf("桥接文本=%q want 你好，世界", joined.String())
	}
}

// 工具调用桥：intent → tool_call；completed → tool_call_update。
func TestACP_ToolCallBridge(t *testing.T) {
	backend := newFakeBackend(t)
	backend.onRun = func(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error) {
		tcid := contracts.ProviderId("toolu_01")
		backend.publishToolIntent(sid, tcid, "read", json.RawMessage(`{"path":"/tmp/x","token":"sk-Z"}`))
		backend.publishToolCompleted(sid, tcid, "文件内容")
		return loop.Result{Text: "读完了", Turns: 1}, nil
	}
	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	_ = client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
	id1 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, _ := client.waitForResult(id1, 2*time.Second)
	var ns newSessionResult
	json.Unmarshal(res, &ns)

	id2 := client.sendRequest("session/prompt", map[string]any{"sessionId": ns.SessionID, "prompt": "读文件"})
	if _, ok := client.waitForResult(id2, 3*time.Second); !ok {
		t.Fatal("没收到 prompt 响应")
	}

	toolCalls := client.findUpdates("tool_call")
	if len(toolCalls) != 1 || toolCalls[0].ToolCallID != "toolu_01" {
		t.Fatalf("tool_call 桥不对: %+v", toolCalls)
	}
	// H18：rawInput 脱敏（token 打码）。
	if bytes.Contains(toolCalls[0].RawInput, []byte("sk-Z")) {
		t.Errorf("工具参数凭据泄漏: %s", toolCalls[0].RawInput)
	}
	updates := client.findUpdates("tool_call_update")
	if len(updates) != 1 || updates[0].ToolCallID != "toolu_01" || updates[0].Status != "completed" {
		t.Fatalf("tool_call_update 桥不对: %+v", updates)
	}
}

// cancel：跑一半取消 → stopReason=cancelled。
func TestACP_Cancel(t *testing.T) {
	backend := newFakeBackend(t)
	started := make(chan struct{})
	backend.onRun = func(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error) {
		close(started)
		<-ctx.Done() // 卡住，直到被取消
		return loop.Result{}, ctx.Err()
	}
	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	_ = client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
	id1 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, _ := client.waitForResult(id1, 2*time.Second)
	var ns newSessionResult
	json.Unmarshal(res, &ns)

	id2 := client.sendRequest("session/prompt", map[string]any{"sessionId": ns.SessionID, "prompt": "跑着"})
	<-started // 等 turn 真正起来再取消
	// session/cancel 是通知（无 id）。
	cn, _ := newNotification("session/cancel", map[string]any{"sessionId": ns.SessionID})
	client.writeFrame(cn)

	res, ok := client.waitForResult(id2, 3*time.Second)
	if !ok {
		t.Fatal("没收到 prompt 响应")
	}
	var pr promptResult
	json.Unmarshal(res, &pr)
	if pr.StopReason != "cancelled" {
		t.Errorf("stopReason=%q want cancelled", pr.StopReason)
	}
}

// 未知 method → -32601。
func TestACP_UnknownMethod(t *testing.T) {
	backend := newFakeBackend(t)
	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	id := client.sendRequest("session/does-not-exist", map[string]any{})
	rpcErr, ok := client.waitForError(id, 2*time.Second)
	if !ok {
		t.Fatal("没收到错误响应")
	}
	if rpcErr.Code != codeMethodNotFound {
		t.Errorf("code=%d want %d (-32601)", rpcErr.Code, codeMethodNotFound)
	}
}

// extractPromptText：多 text block 之间用 "\n" 分隔（不是直接首尾相接粘成一个词）。
func TestExtractPromptText_MultipleBlocksSeparatedByNewline(t *testing.T) {
	raw := json.RawMessage(`[{"type":"text","text":"hello"},{"type":"text","text":"world"}]`)
	if got := extractPromptText(raw); got != "hello\nworld" {
		t.Errorf("多 block 应以 \\n 分隔，得到 %q", got)
	}
	// 裸字符串 / 单 block 仍原样（宽容客户端不回归）。
	if got := extractPromptText(json.RawMessage(`"hi"`)); got != "hi" {
		t.Errorf("裸字符串应原样，得到 %q", got)
	}
	if got := extractPromptText(json.RawMessage(`{"type":"text","text":"hi"}`)); got != "hi" {
		t.Errorf("单 block 应原样，得到 %q", got)
	}
}

// F2 钉子：通知（无 id）**不回帧**——JSON-RPC 2.0 §4.1：通知没有 id，服务器不得回应。
//
// 旧实现的病：通知也走 checkVersion，版本不对就回一条"id: null"的错误帧——等于凭空
// 造一个响应（客户端按 id 配对时对不上；严格的客户端还会判协议违规）。正确处置：
// 通知一律不回帧，版本不对只记 Warn 日志、静默丢弃。
func TestACP_NotificationNeverGetsReplyFrame(t *testing.T) {
	backend := newFakeBackend(t)
	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	// ① 版本不对的通知（jsonrpc 1.0）→ 不回帧（只记日志）。
	client.sendRawLine(`{"jsonrpc":"1.0","method":"session/cancel","params":{"sessionId":"01ARZ3NDEKTSV4RRFFQ69G5FAV"}}`)
	// ② 版本对的未知通知 → 也不回帧。
	client.sendRawLine(`{"jsonrpc":"2.0","method":"session/unknown-notification"}`)

	// 后续请求仍被正常处理（连接没死）。
	id := client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
	if _, ok := client.waitForResult(id, 3*time.Second); !ok {
		t.Fatalf("通知之后请求没被处理（连接死了？）")
	}

	// 出站**只有** initialize 那一条响应——通知一条都不回。
	lines := client.out.snapshot()
	if len(lines) != 1 {
		t.Fatalf("通知不应回帧，出站却有 %d 条：%v", len(lines), lines)
	}
}

// 审批桥 allow：ask → request_permission → client 回 allow → 放行。
func TestACP_ApprovalAllow(t *testing.T) {
	runApprovalFlow(t, "allow", "已放行执行")
}

// 审批桥 deny：client 回 deny → fail-close 拒绝。
func TestACP_ApprovalDeny(t *testing.T) {
	runApprovalFlow(t, "deny", "拒绝执行")
}

func runApprovalFlow(t *testing.T, outcome, wantText string) {
	t.Helper()
	backend := newFakeBackend(t)
	approver := NewApprover()
	backend.onRun = func(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error) {
		resp, _ := approver.Approve(ctx, sampleApprovalReq())
		if resp.Decision == contracts.ApprovalDecisionAllow {
			return loop.Result{Text: "已放行执行", Turns: 1}, nil
		}
		return loop.Result{Text: "拒绝执行", Turns: 1}, nil
	}
	client, cleanup := startACP(t, backend, approver, 5*time.Second)
	defer cleanup()

	_ = client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
	id1 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, _ := client.waitForResult(id1, 2*time.Second)
	var ns newSessionResult
	json.Unmarshal(res, &ns)

	id2 := client.sendRequest("session/prompt", map[string]any{"sessionId": ns.SessionID, "prompt": "跑工具"})

	// 等 request_permission 出现，回 outcome。
	var permID json.RawMessage
	deadline := time.Now().Add(3 * time.Second)
	for {
		if id, ok := client.findRequestPermission(); ok {
			permID = id
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("没收到 session/request_permission")
		}
		client.out.waitCount(len(client.out.snapshot())+1, 50*time.Millisecond)
	}
	client.respondPermission(permID, outcome)

	res, ok := client.waitForResult(id2, 3*time.Second)
	if !ok {
		t.Fatal("没收到 prompt 响应")
	}
	var pr promptResult
	json.Unmarshal(res, &pr)
	if pr.StopReason != "end_turn" {
		t.Errorf("stopReason=%q want end_turn", pr.StopReason)
	}
	// onRun 未发 delta → 降级补发整段 agent_message_chunk，其文本反映审批结论。
	var joined strings.Builder
	for _, m := range client.findUpdates("agent_message_chunk") {
		joined.WriteString(contentText(m.Content))
	}
	if joined.String() != wantText {
		t.Errorf("审批结论未反映到输出：got %q want %q", joined.String(), wantText)
	}
}

// stdout 纯净性：跑完一轮后，出站**每一行**都必须是合法 JSON-RPC 帧（日志泄漏即红）。
func TestACP_StdoutPurity(t *testing.T) {
	backend := newFakeBackend(t)
	backend.onRun = func(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error) {
		backend.publishDelta(sid, "一些输出")
		return loop.Result{Text: "一些输出", Turns: 1}, nil
	}
	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	_ = client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
	id1 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, _ := client.waitForResult(id1, 2*time.Second)
	var ns newSessionResult
	json.Unmarshal(res, &ns)
	id2 := client.sendRequest("session/prompt", map[string]any{"sessionId": ns.SessionID, "prompt": "hi"})
	client.waitForResult(id2, 3*time.Second)

	// 逐行校验：每行都是合法 JSON-RPC 对象（有 jsonrpc 字段），无一行是日志/横幅。
	lines := client.out.snapshot()
	if len(lines) == 0 {
		t.Fatal("stdout 空的（不该）")
	}
	for i, ln := range lines {
		var probe struct {
			JSONRPC string `json:"jsonrpc"`
		}
		if err := json.Unmarshal([]byte(ln), &probe); err != nil {
			t.Fatalf("第 %d 行不是合法 JSON（日志泄漏？）: %q", i, ln)
		}
		if probe.JSONRPC != "2.0" {
			t.Fatalf("第 %d 行缺 jsonrpc=2.0: %q", i, ln)
		}
	}
}

// ---------------------------------------------------------------------------
// F1 / F3 / F4 / F5 的回归钉子
// ---------------------------------------------------------------------------

// F1 钉子（端到端）：一条 >8MB 的帧进来，**出站帧数必须有界**（不能无限自旋狂刷
// 错误帧——事故：8 秒吐 118 万条 parse error、进程永不退出），且**后续合法帧仍被处理**。
func TestACP_OversizedFrame_BoundedAndRecovers(t *testing.T) {
	backend := newFakeBackend(t)
	client, cleanup := startACP(t, backend, nil, 10*time.Second)
	defer cleanup()

	// 一条超上限的帧（> maxFrameBytes）。
	huge := `{"jsonrpc":"2.0","id":99,"method":"session/new","params":{"cwd":"` +
		strings.Repeat("x", maxFrameBytes+4096) + `"}}`
	client.sendRawLine(huge)

	// 后续合法帧仍被处理（连接没死）。
	id := client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
	res, ok := client.waitForResult(id, 8*time.Second)
	if !ok {
		t.Fatalf("超限帧之后，合法帧没被处理（连接死了？）")
	}
	var init initializeResult
	if err := json.Unmarshal(res, &init); err != nil {
		t.Fatal(err)
	}
	if init.ProtocolVersion != protocolVersion {
		t.Errorf("initialize 结果不对: %+v", init)
	}

	// 出站帧数有界（一条错误帧 + 一条 initialize 响应）。
	lines := client.out.snapshot()
	if len(lines) > 8 {
		t.Fatalf("出站帧无界（%d 条）——读循环自旋了？: %v", len(lines), lines)
	}
	// 超限帧只回**一条**错误帧（-32600）。
	var errFrames int
	for _, f := range client.frames() {
		if f.isResponse() && f.Error != nil {
			errFrames++
			if f.Error.Code != codeInvalidRequest {
				t.Errorf("超限帧应回 -32600，得到 %d", f.Error.Code)
			}
		}
	}
	if errFrames != 1 {
		t.Errorf("超限帧应只回一条错误帧，得到 %d 条", errFrames)
	}
}

// F1 钉子（终态错）：底层读错误 → 回**一条**错误帧后**结束连接**（不是无限重试）。
//
// 这是 F1 事故的另一半：旧实现把终态错当"可恢复坏帧"→ 读循环无限自旋狂刷错误帧
// （8 秒 118 万条、进程永不退出）。终态错的正确处置是"回一条、收场"。
func TestACP_TerminalReadError_EndsConnection(t *testing.T) {
	backend := newFakeBackend(t)
	out := newLineSink()
	r := &failingReader{
		data: []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}` + "\n"),
		err:  errors.New("底层流坏了"),
	}
	done := make(chan error, 1)
	go func() {
		done <- Serve(context.Background(), Options{Backend: backend, In: r, Out: out})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("终态读错后端点应正常收场（不是错误）：%v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("终态读错后端点没收场（无限重试？）：已写出 %d 条帧", len(out.snapshot()))
	}

	// 只回**一条**错误帧（不是刷屏）：前面的 initialize 响应 + 这一条错误帧 = 2 条。
	lines := out.snapshot()
	if len(lines) != 2 {
		t.Fatalf("终态读错应只回一条错误帧（连同前面的响应共 2 条），得到 %d 条：%v", len(lines), lines)
	}
	f, err := decodeFrame([]byte(lines[len(lines)-1]))
	if err != nil {
		t.Fatalf("错误帧不是合法帧: %v", err)
	}
	if f.Error == nil || f.Error.Code != codeParseError {
		t.Errorf("终态读错应回 parse error(-32700)，得到 %+v", f.Error)
	}
}

// F11 钉子：ctx 取消 = **正常收场**（Serve 返 nil，不是 error）。
//
// 取消当错误会让 `alasol acp` 的退出码变成 1——脚本没法区分"被叫停"与"崩了"。
// 同时验证：取消能**叫醒**阻塞中的读（否则 Ctrl-C 要等下一帧才收场）。
func TestServe_CtxCancelIsNormal(t *testing.T) {
	backend := newFakeBackend(t)
	out := newLineSink()
	pr, pw := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, Options{Backend: backend, In: pr, Out: out})
	}()
	time.Sleep(50 * time.Millisecond) // 让端点真的读起来
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ctx 取消应正常收场（err=nil），得到 %v", err)
		}
	case <-time.After(10 * time.Second):
		_ = pw.Close()
		t.Fatal("ctx 取消后端点没收场（读没被叫醒？）")
	}
	_ = pw.Close()
}

// F3 钉子：后端真出错 → 客户端收到 **JSON-RPC 错误帧**（-32603），不是 end_turn。
//
// 旧实现出错也回 {stopReason:"end_turn"}——那等于谎报"这轮正常收场"。
func TestACP_RunPromptError_ReturnsErrorFrame(t *testing.T) {
	backend := newFakeBackend(t)
	backend.onRun = func(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error) {
		return loop.Result{}, errors.New("模型端炸了")
	}
	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	_ = client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
	id1 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, _ := client.waitForResult(id1, 2*time.Second)
	var ns newSessionResult
	json.Unmarshal(res, &ns)

	id2 := client.sendRequest("session/prompt", map[string]any{"sessionId": ns.SessionID, "prompt": "hi"})
	rpcErr, ok := client.waitForError(id2, 5*time.Second)
	if !ok {
		t.Fatal("后端出错应收到错误帧（没收到）")
	}
	if rpcErr.Code != codeInternalError {
		t.Errorf("code=%d want %d (-32603)", rpcErr.Code, codeInternalError)
	}
	if rpcErr.Message == "" {
		t.Errorf("错误帧应带错误摘要: %+v", rpcErr)
	}
	// 不能同时有成功的 result（那才是"谎报 end_turn"）。
	for _, f := range client.frames() {
		if f.isResponse() && f.Result != nil && f.ID != nil && string(*f.ID) == string(id2) {
			t.Fatalf("后端出错却回了成功 result: %s", string(*f.Result))
		}
	}
}

// F4 钉子：产出的 stopReason **必须落在 ACP 枚举内**（枚举外取值客户端解不出）。
func TestMapStopReason_OnlyACPEnumValues(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	deadline, cancel2 := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel2()
	time.Sleep(2 * time.Millisecond) // 让 deadline 真的过

	cases := map[string]context.Context{
		"正常":      context.Background(),
		"显式取消":    canceled,
		"超时":      deadline,
		"nil ctx": nil,
	}
	for name, ctx := range cases {
		got := mapStopReason(ctx)
		if !validStopReasons[got] {
			t.Errorf("%s：stopReason=%q 不在 ACP 枚举内（枚举：end_turn/max_tokens/max_turn_requests/refusal/cancelled）", name, got)
		}
	}
}

// F4 钉子（端到端）：实际跑出来的 stopReason 也得在枚举内。
func TestACP_StopReasonInEnum(t *testing.T) {
	backend := newFakeBackend(t)
	backend.onRun = func(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error) {
		return loop.Result{Text: "好", Turns: 1}, nil
	}
	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	_ = client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
	id1 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, _ := client.waitForResult(id1, 2*time.Second)
	var ns newSessionResult
	json.Unmarshal(res, &ns)
	id2 := client.sendRequest("session/prompt", map[string]any{"sessionId": ns.SessionID, "prompt": "hi"})
	res, ok := client.waitForResult(id2, 5*time.Second)
	if !ok {
		t.Fatal("没收到 prompt 响应")
	}
	var pr promptResult
	json.Unmarshal(res, &pr)
	if !validStopReasons[pr.StopReason] {
		t.Errorf("stopReason=%q 不在 ACP 枚举内", pr.StopReason)
	}
}

// capBackend 是一个"报轮数上限"的后端（实现 acp 的可选能力 turnCapAware）。
type capBackend struct {
	*fakeBackend
	cap int
}

// TurnCap 实现可选能力（轮数上限）。
func (b *capBackend) TurnCap() int { return b.cap }

// F4 钉子：轮数用尽（loop 被 MaxTurns 截断）→ max_turn_requests（不是 end_turn）。
func TestACP_TurnsExhausted_MaxTurnRequests(t *testing.T) {
	base := newFakeBackend(t)
	backend := &capBackend{fakeBackend: base, cap: 3}
	backend.onRun = func(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error) {
		return loop.Result{Text: "截断了", Turns: 3}, nil
	}
	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	_ = client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
	id1 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, _ := client.waitForResult(id1, 2*time.Second)
	var ns newSessionResult
	json.Unmarshal(res, &ns)
	id2 := client.sendRequest("session/prompt", map[string]any{"sessionId": ns.SessionID, "prompt": "hi"})
	res, ok := client.waitForResult(id2, 5*time.Second)
	if !ok {
		t.Fatal("没收到 prompt 响应")
	}
	var pr promptResult
	json.Unmarshal(res, &pr)
	if pr.StopReason != stopReasonMaxTurnRequests {
		t.Errorf("轮数用尽应报 %q，得到 %q", stopReasonMaxTurnRequests, pr.StopReason)
	}
}

// F5 钉子：增量只覆盖前半截、最终文本更长 → 客户端要能**收全**（差量补发）。
func TestACP_PartialDelta_ReconcilesWithFinalText(t *testing.T) {
	backend := newFakeBackend(t)
	backend.onRun = func(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error) {
		backend.publishDelta(sid, "前半")
		return loop.Result{Text: "前半和后半", Turns: 1}, nil
	}
	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	_ = client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
	id1 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, _ := client.waitForResult(id1, 2*time.Second)
	var ns newSessionResult
	json.Unmarshal(res, &ns)
	id2 := client.sendRequest("session/prompt", map[string]any{"sessionId": ns.SessionID, "prompt": "hi"})
	if _, ok := client.waitForResult(id2, 5*time.Second); !ok {
		t.Fatal("没收到 prompt 响应")
	}

	var joined strings.Builder
	for _, m := range client.findUpdates("agent_message_chunk") {
		joined.WriteString(contentText(m.Content))
	}
	if joined.String() != "前半和后半" {
		t.Errorf("客户端收不全（增量只覆盖前半时应补差量）：got %q want %q", joined.String(), "前半和后半")
	}
}

// N2 钉子：**多轮**（第 1 轮正文 + 工具调用、第 2 轮最终答复）→ 客户端**不能**收到两份
// 最终答复。
//
// 病根：core/loop 的最终文本（Result.Text）是**最后一轮**的文本（lastText 语义），
// 而桥出去的增量是**所有轮**的拼接 → 两边直接对账会判"分叉" → 整段重发 → 客户端收到
// 两份最终答复。修法：桥按 model.delta 载荷的 turn 分桶累计，对账只取**最后一轮**。
func TestACP_MultiTurnDelta_NoDuplicateFinalAnswer(t *testing.T) {
	backend := newFakeBackend(t)
	backend.onRun = func(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error) {
		// 第 1 轮：正文 + 工具调用（工具调用桥成 tool_call，不进正文对账）。
		turn1 := "第一轮正文"
		p1, _ := json.Marshal(contracts.ModelDeltaPayload{Turn: 1, Text: &turn1})
		backend.publish(contracts.Event{
			V: 1, ID: core.NewUlid(), Kind: loop.KindModelDelta, TS: nowTS(),
			SessionID: sid, Durable: false, Payload: p1,
		})
		backend.publishToolIntent(sid, contracts.ProviderId("toolu_1"), "read", json.RawMessage(`{"path":"x"}`))
		// 第 2 轮：最终答复（= Result.Text 的那一轮）。
		final := "最终答复"
		p2, _ := json.Marshal(contracts.ModelDeltaPayload{Turn: 2, Text: &final})
		backend.publish(contracts.Event{
			V: 1, ID: core.NewUlid(), Kind: loop.KindModelDelta, TS: nowTS(),
			SessionID: sid, Durable: false, Payload: p2,
		})
		return loop.Result{Text: final, Turns: 2}, nil
	}
	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	_ = client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
	id1 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, _ := client.waitForResult(id1, 2*time.Second)
	var ns newSessionResult
	json.Unmarshal(res, &ns)
	id2 := client.sendRequest("session/prompt", map[string]any{"sessionId": ns.SessionID, "prompt": "hi"})
	if _, ok := client.waitForResult(id2, 5*time.Second); !ok {
		t.Fatal("没收到 prompt 响应")
	}

	var joined strings.Builder
	for _, m := range client.findUpdates("agent_message_chunk") {
		joined.WriteString(contentText(m.Content))
	}
	if n := strings.Count(joined.String(), "最终答复"); n != 1 {
		t.Errorf("最终答复应只出现一次（多轮时不该整段重发）：出现 %d 次，拼接=%q", n, joined.String())
	}
}

// N2 钉子（尾随空白）：最终文本带 "\n" 时**不该**触发重发（两侧同一 trim 口径）。
func TestACP_FinalTextWithTrailingNewline_NoResend(t *testing.T) {
	backend := newFakeBackend(t)
	backend.onRun = func(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error) {
		text := "答复正文\n"
		p, _ := json.Marshal(contracts.ModelDeltaPayload{Turn: 1, Text: &text})
		backend.publish(contracts.Event{
			V: 1, ID: core.NewUlid(), Kind: loop.KindModelDelta, TS: nowTS(),
			SessionID: sid, Durable: false, Payload: p,
		})
		return loop.Result{Text: "答复正文\n", Turns: 1}, nil
	}
	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	_ = client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
	id1 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, _ := client.waitForResult(id1, 2*time.Second)
	var ns newSessionResult
	json.Unmarshal(res, &ns)
	id2 := client.sendRequest("session/prompt", map[string]any{"sessionId": ns.SessionID, "prompt": "hi"})
	if _, ok := client.waitForResult(id2, 5*time.Second); !ok {
		t.Fatal("没收到 prompt 响应")
	}

	var joined strings.Builder
	for _, m := range client.findUpdates("agent_message_chunk") {
		joined.WriteString(contentText(m.Content))
	}
	if n := strings.Count(joined.String(), "答复正文"); n != 1 {
		t.Errorf("最终文本带尾随换行不该触发重发：出现 %d 次，拼接=%q", n, joined.String())
	}
}

// N3 钉子（单元层）：后端**显式**标记"这轮被 MaxTurns 截断"→ stopReason=max_turn_requests。
//
// 权威信号 = loop.Result.TruncatedByMaxTurns（loop 亲自打；真 hub 透传）。
func TestACP_TruncatedByMaxTurns_MaxTurnRequests(t *testing.T) {
	backend := newFakeBackend(t)
	backend.onRun = func(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error) {
		return loop.Result{Text: "截断了", Turns: 1, TruncatedByMaxTurns: true}, nil
	}
	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	_ = client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
	id1 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, _ := client.waitForResult(id1, 2*time.Second)
	var ns newSessionResult
	json.Unmarshal(res, &ns)
	id2 := client.sendRequest("session/prompt", map[string]any{"sessionId": ns.SessionID, "prompt": "hi"})
	res, ok := client.waitForResult(id2, 5*time.Second)
	if !ok {
		t.Fatal("没收到 prompt 响应")
	}
	var pr promptResult
	json.Unmarshal(res, &pr)
	if pr.StopReason != stopReasonMaxTurnRequests {
		t.Errorf("被 MaxTurns 截断应报 %q，得到 %q", stopReasonMaxTurnRequests, pr.StopReason)
	}
}

// 缺口 5 第二件的 ACP 侧钉子（critic F4）：后端回 **TruncatedByBudget**（loop 被逐轮
// 成本预算闸门截断）→ stopReason 必须是 **max_turn_requests**。
//
// 映射取舍（如实写明）：ACP 的 stopReason 是**闭合枚举**（end_turn / max_tokens /
// max_turn_requests / refusal / cancelled），**没有**"预算耗尽"这一档 → 取语义最近的
// max_turn_requests（"撞上了上限、没能跑完"）。不引入枚举外的新值（枚举外取值会让现成
// ACP 客户端解不出合法 stopReason）。取舍与代价见 endpoint.go 的 turnsExhausted。
//
// 变异自证：把 turnsExhausted 里的 `res.TruncatedByBudget` 判定去掉 → 本用例红
// （落回 end_turn）。
func TestACP_TruncatedByBudget_MaxTurnRequests(t *testing.T) {
	backend := newFakeBackend(t)
	backend.onRun = func(ctx context.Context, sid contracts.Ulid, prompt string) (loop.Result, error) {
		return loop.Result{
			Text:              "预算闸门挡住了这一轮",
			Turns:             2,
			TruncatedByBudget: true,
			TruncationReason:  "host: session X 已达成本预算上限（累计 600 tokens ≥ 上限 500）",
		}, nil
	}
	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	_ = client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
	id1 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, _ := client.waitForResult(id1, 2*time.Second)
	var ns newSessionResult
	json.Unmarshal(res, &ns)
	id2 := client.sendRequest("session/prompt", map[string]any{"sessionId": ns.SessionID, "prompt": "hi"})
	res, ok := client.waitForResult(id2, 5*time.Second)
	if !ok {
		t.Fatal("没收到 prompt 响应")
	}
	var pr promptResult
	json.Unmarshal(res, &pr)
	if pr.StopReason != stopReasonMaxTurnRequests {
		t.Errorf("被预算闸门截断应报 %q，得到 %q", stopReasonMaxTurnRequests, pr.StopReason)
	}
	// 枚举纪律：产出值必须落在 ACP 的 stopReason 枚举内。
	if !validStopReasons[pr.StopReason] {
		t.Errorf("stopReason=%q 不在 ACP 枚举内", pr.StopReason)
	}
}
