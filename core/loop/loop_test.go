// Package loop_test 是 core/loop 的黑盒测试：只走公开 API（New / Run），
// 用 mock Provider + mock ToolExecutor + 真 bus + 内存/文件 store。
package loop_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
)

// ---------------------------------------------------------------------------
// 测试辅助：顺序日志（把"事件入库序"与"执行器调用序"放进同一条时间线）
// ---------------------------------------------------------------------------

// orderLog 是一条共享的顺序时间线：store 入库与执行器调用都往里写，
// 于是"intent 先于执行、completed 后于执行"可以被直接断言（X1）。
type orderLog struct {
	mu      sync.Mutex
	entries []string
}

func newOrderLog() *orderLog { return &orderLog{} }

func (o *orderLog) add(s string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.entries = append(o.entries, s)
}

func (o *orderLog) all() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.entries...)
}

// storeLabel 把一条入库事件压成时间线上的一格（kind + provider 黑盒 id）。
func storeLabel(ev contracts.Event) string {
	switch ev.Kind {
	case loop.KindToolCallIntent:
		var p contracts.ToolCallIntentPayload
		_ = json.Unmarshal(ev.Payload, &p)
		return "intent:" + string(p.ToolCallID)
	case loop.KindToolCallCompleted:
		var p contracts.ToolCallCompletedPayload
		_ = json.Unmarshal(ev.Payload, &p)
		return "completed:" + string(p.ToolCallID)
	case loop.KindToolCallFailed:
		var p contracts.ToolCallFailedPayload
		_ = json.Unmarshal(ev.Payload, &p)
		return "failed:" + string(p.ToolCallID)
	default:
		return ev.Kind
	}
}

// ---------------------------------------------------------------------------
// 测试辅助：内存 store（分配单调 seq、按 Event.ID 幂等、ReadFrom 按 seq 升序）
// ---------------------------------------------------------------------------

type memStore struct {
	mu     sync.Mutex
	seq    int64
	byID   map[contracts.Ulid]contracts.Event
	events []contracts.Event
	log    *orderLog // 非 nil 时把每次入库记进共享时间线
}

func newMemStore() *memStore { return &memStore{byID: make(map[contracts.Ulid]contracts.Event)} }

func (m *memStore) Append(_ context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]contracts.Event, len(evs))
	for i, ev := range evs {
		if existing, ok := m.byID[ev.ID]; ok {
			out[i] = existing // S7 幂等：去重只看 Event.ID
			continue
		}
		m.seq++
		ev.Seq = ptrInt64(m.seq)
		m.byID[ev.ID] = ev
		m.events = append(m.events, ev)
		if m.log != nil {
			m.log.add(storeLabel(ev))
		}
		out[i] = ev
	}
	return out, nil
}

func (m *memStore) ReadFrom(_ context.Context, sessionID *contracts.Ulid, fromSeq int64, limit int) ([]contracts.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []contracts.Event
	for _, ev := range m.events {
		if ev.Seq == nil || *ev.Seq < fromSeq {
			continue
		}
		if sessionID != nil && ev.SessionID != *sessionID {
			continue
		}
		out = append(out, ev)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (m *memStore) all() []contracts.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]contracts.Event(nil), m.events...)
}

// failStore 总是失败的 store：用来证明"intent 没落盘就绝不执行"（X1）。
type failStore struct{}

func (failStore) Append(context.Context, []contracts.Event) ([]contracts.Event, error) {
	return nil, errors.New("store: append failed (injected)")
}

func (failStore) ReadFrom(context.Context, *contracts.Ulid, int64, int) ([]contracts.Event, error) {
	return nil, nil
}

func ptrInt64(v int64) *int64 { return &v }

// ---------------------------------------------------------------------------
// 测试辅助：mock Provider
// ---------------------------------------------------------------------------

type scriptStep struct {
	result core.ModelResult
	err    error
}

func stepText(s string) scriptStep { return scriptStep{result: core.ModelResult{Text: s}} }

func stepTools(calls ...core.ToolCallReq) scriptStep {
	return scriptStep{result: core.ModelResult{Text: "调用工具", ToolCalls: calls}}
}

func call(id, name, args string) core.ToolCallReq {
	return core.ToolCallReq{ID: contracts.ProviderId(id), Name: name, Args: json.RawMessage(args)}
}

// mockProvider 是脚本化 Provider：按 turn 依次返回预设结果（repeatLast = 最后一次重复）。
type mockProvider struct {
	mu         sync.Mutex
	steps      []scriptStep
	repeatLast bool
	calls      int
	requests   []core.ModelRequest
	emitCalls  int
}

func (p *mockProvider) Run(_ context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu.Lock()
	i := p.calls
	p.calls++
	p.requests = append(p.requests, cloneRequest(req))
	p.mu.Unlock()

	// 顺带发一条流事件：证明 emit 通路被走到，且流事件**不落库**（F8/陷阱 11）。
	if emit != nil {
		emit(core.ProviderEvent{Kind: "text.delta", Text: "…"})
		p.mu.Lock()
		p.emitCalls++
		p.mu.Unlock()
	}

	if len(p.steps) == 0 {
		return core.ModelResult{}, errors.New("mockProvider: 脚本为空")
	}
	st := p.steps[len(p.steps)-1]
	if i < len(p.steps) {
		st = p.steps[i]
	} else if !p.repeatLast {
		return core.ModelResult{}, fmt.Errorf("mockProvider: 脚本已用尽（第 %d 次调用）", i+1)
	}
	if st.err != nil {
		return core.ModelResult{}, st.err
	}
	return st.result, nil
}

func (p *mockProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// reset 清零调用计数与请求记录（harness 在设置脚本时调用：每次脚本都是新的一次 Run）。
func (p *mockProvider) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = 0
	p.requests = nil
	p.emitCalls = 0
}

func (p *mockProvider) reqs() []core.ModelRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]core.ModelRequest(nil), p.requests...)
}

func cloneRequest(req core.ModelRequest) core.ModelRequest {
	out := core.ModelRequest{Model: req.Model, System: req.System}
	if len(req.Messages) > 0 {
		out.Messages = append([]core.Message(nil), req.Messages...)
		for i := range out.Messages {
			if len(req.Messages[i].ToolCalls) > 0 {
				out.Messages[i].ToolCalls = append([]core.ToolCallReq(nil), req.Messages[i].ToolCalls...)
			}
		}
	}
	out.Tools = append([]contracts.Tool(nil), req.Tools...)
	return out
}

// ---------------------------------------------------------------------------
// 测试辅助：mock ToolExecutor
// ---------------------------------------------------------------------------

type execCall struct {
	intent contracts.ToolCallIntentPayload
	env    core.EnvScope
}

type mockExecutor struct {
	name    string
	log     *orderLog
	result  contracts.ToolResult
	err     error
	panicOn bool

	mu    sync.Mutex
	calls []execCall
}

func (e *mockExecutor) Execute(ctx context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	env, _ := core.EnvFrom(ctx)
	e.mu.Lock()
	e.calls = append(e.calls, execCall{intent: intent, env: env})
	e.mu.Unlock()
	if e.log != nil {
		e.log.add("exec:" + string(intent.ToolCallID))
	}
	if e.panicOn {
		panic("boom: mock 执行器故意崩溃")
	}
	if e.err != nil {
		return contracts.ToolResult{}, e.err
	}
	if e.result.Content != nil || e.result.GraphEdges != nil {
		return e.result, nil
	}
	text := "mock:" + intent.ToolName
	return contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &text}}}, nil
}

func (e *mockExecutor) callN() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}

func (e *mockExecutor) seen() []execCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]execCall(nil), e.calls...)
}

// 编译期证明：mock 类型就是 core 定义的那些接口。
var (
	_ core.Provider     = (*mockProvider)(nil)
	_ core.ToolExecutor = (*mockExecutor)(nil)
	_ core.Store        = (*memStore)(nil)
	_ core.Store        = failStore{}
	_ core.Approver     = core.AlwaysAllow{}
	_ core.Approver     = core.DenyAll{}
)

// ---------------------------------------------------------------------------
// 测试辅助：harness
// ---------------------------------------------------------------------------

type harness struct {
	t        *testing.T
	store    *memStore
	bus      *bus.Bus
	reg      *registry.Registry
	engine   *policy.Engine
	log      *orderLog
	provider *mockProvider
	execs    map[string]*mockExecutor
	session  contracts.Ulid
	agent    contracts.Ulid
	// eventsFn 非 nil 时用它读回事件（文件后端测试用；默认读内存 store）。
	eventsFn func() []contracts.Event
}

func newHarness(t *testing.T, rules ...contracts.PolicyRule) *harness {
	t.Helper()
	eng := policy.New(rules...)
	h := &harness{
		t:        t,
		store:    newMemStore(),
		reg:      registry.New(registry.NewPolicyAdjudicator(eng)),
		engine:   eng,
		log:      newOrderLog(),
		provider: &mockProvider{},
		execs:    make(map[string]*mockExecutor),
		session:  core.NewUlid(),
		agent:    core.NewUlid(),
	}
	h.store.log = h.log
	h.bus = bus.New(h.store)
	return h
}

// addTool 注册一个工具并返回它的 mock 执行器（默认返回文本 mock:<工具名>）。
func (h *harness) addTool(name string, mut func(*contracts.Tool)) *mockExecutor {
	h.t.Helper()
	tool := contracts.Tool{
		Name:       name,
		SideEffect: contracts.SideEffectWrite,
		Schema:     json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}}}`),
	}
	if mut != nil {
		mut(&tool)
	}
	exec := &mockExecutor{name: name, log: h.log}
	if err := h.reg.RegisterTool(tool, exec); err != nil {
		h.t.Fatalf("RegisterTool(%q): %v", name, err)
	}
	h.execs[name] = exec
	return exec
}

func (h *harness) script(steps ...scriptStep) {
	h.provider.steps = steps
	h.provider.repeatLast = false
	h.provider.reset()
}

// repeat 让最后一次脚本步骤无限重复（用于 MaxTurns 截断测试）。
func (h *harness) repeat(steps ...scriptStep) {
	h.provider.steps = steps
	h.provider.repeatLast = true
	h.provider.reset()
}

func (h *harness) loop(mut func(*loop.Config)) *loop.Loop {
	h.t.Helper()
	cfg := loop.Config{
		Provider:  h.provider,
		Bus:       h.bus,
		Registry:  h.reg,
		Policy:    h.engine,
		SessionID: h.session,
		AgentID:   h.agent,
		Model:     "test-model",
		System:    "test-system",
	}
	if mut != nil {
		mut(&cfg)
	}
	l, err := loop.New(cfg)
	if err != nil {
		h.t.Fatalf("loop.New: %v", err)
	}
	return l
}

// events 取回全部已入库事件（默认读内存 store；文件后端测试覆盖 eventsFn）。
func (h *harness) events() []contracts.Event {
	if h.eventsFn != nil {
		return h.eventsFn()
	}
	return h.store.all()
}

func (h *harness) kinds() []string {
	var out []string
	for _, ev := range h.events() {
		out = append(out, ev.Kind)
	}
	return out
}

// decode 系列：把事件 payload 解成契约类型（测试自己解，不依赖 loop 的内部表示）。
func decodeIntent(t *testing.T, ev contracts.Event) contracts.ToolCallIntentPayload {
	t.Helper()
	var p contracts.ToolCallIntentPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("解 intent payload: %v", err)
	}
	return p
}

func decodeCompleted(t *testing.T, ev contracts.Event) contracts.ToolCallCompletedPayload {
	t.Helper()
	var p contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("解 completed payload: %v", err)
	}
	return p
}

func decodeFailed(t *testing.T, ev contracts.Event) contracts.ToolCallFailedPayload {
	t.Helper()
	var p contracts.ToolCallFailedPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("解 failed payload: %v", err)
	}
	return p
}

// findEvent 按 kind 取第 n 条（n 从 0 计）。
func findEvent(t *testing.T, evs []contracts.Event, kind string, n int) contracts.Event {
	t.Helper()
	seen := 0
	for _, ev := range evs {
		if ev.Kind != kind {
			continue
		}
		if seen == n {
			return ev
		}
		seen++
	}
	t.Fatalf("找不到第 %d 条 kind=%s 的事件（共 %d 条事件）", n, kind, len(evs))
	return contracts.Event{}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// allowRule / denyRule / askRule 构造策略规则（给个稳定 id 便于审计断言）。
func allowRule(tool string) contracts.PolicyRule {
	return contracts.PolicyRule{ID: strPtr("allow:" + tool), Effect: contracts.EffectAllow, Tool: tool, Source: contracts.RuleSourceManaged}
}

func denyRule(tool string) contracts.PolicyRule {
	return contracts.PolicyRule{ID: strPtr("deny:" + tool), Effect: contracts.EffectDeny, Tool: tool, Source: contracts.RuleSourceManaged}
}

func askRule(tool string) contracts.PolicyRule {
	return contracts.PolicyRule{ID: strPtr("ask:" + tool), Effect: contracts.EffectAsk, Tool: tool, Source: contracts.RuleSourceManaged}
}

func strPtr(s string) *string { return &s }

// ---------------------------------------------------------------------------
// 构造与校验
// ---------------------------------------------------------------------------

func TestNewValidatesRequiredFields(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	base := func() loop.Config {
		return loop.Config{
			Provider:  h.provider,
			Bus:       h.bus,
			Registry:  h.reg,
			Policy:    h.engine,
			SessionID: h.session,
			AgentID:   h.agent,
			Model:     "m",
		}
	}

	cases := []struct {
		name   string
		mutate func(*loop.Config)
	}{
		{"Provider 缺失", func(c *loop.Config) { c.Provider = nil }},
		{"Bus 缺失", func(c *loop.Config) { c.Bus = nil }},
		{"Registry 缺失", func(c *loop.Config) { c.Registry = nil }},
		{"Model 缺失", func(c *loop.Config) { c.Model = "  " }},
		{"SessionID 缺失", func(c *loop.Config) { c.SessionID = "" }},
		{"SessionID 非法", func(c *loop.Config) { c.SessionID = "not-a-ulid" }},
		{"AgentID 缺失", func(c *loop.Config) { c.AgentID = "" }},
		{"AgentID 非法", func(c *loop.Config) { c.AgentID = "x" }},
		{"MaxTurns 为负", func(c *loop.Config) { c.MaxTurns = -1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mutate(&cfg)
			if _, err := loop.New(cfg); err == nil {
				t.Fatalf("New 应拒绝 %s", tc.name)
			}
		})
	}

	// Policy / Approver / SessionEnv / MaxTurns 可缺省（缺省后更严格，fail-close）。
	cfg := base()
	cfg.Policy = nil
	cfg.Approver = nil
	cfg.SessionEnv = nil
	cfg.MaxTurns = 0
	l, err := loop.New(cfg)
	if err != nil {
		t.Fatalf("可缺省字段不该让 New 失败: %v", err)
	}
	if l == nil {
		t.Fatal("New 返回 nil Loop")
	}
}

func TestNilLoopRunReturnsError(t *testing.T) {
	var l *loop.Loop
	if _, err := l.Run(context.Background(), "hi"); err == nil {
		t.Fatal("nil Loop 的 Run 应返回 error")
	}
}

// ---------------------------------------------------------------------------
// 三段写路径：顺序 / 载荷 / id 原样往返
// ---------------------------------------------------------------------------

func TestThreeStageWritePathOrderAndPayloads(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", func(tool *contracts.Tool) {
		tool.Idempotent = true
		tool.Replay = contracts.ReplayKindVerify
	})
	h.script(
		stepTools(call("call_blackBox-01", "echo", `{"text":"hi"}`)),
		stepText("完成"),
	)

	res, err := h.loop(nil).Run(context.Background(), "帮我 echo")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Result：两轮模型调用、一次工具调用、两条 durable 事件。
	if res.Turns != 2 || res.ToolCalls != 1 || res.Events != 2 || res.Text != "完成" {
		t.Fatalf("Result 不符: %+v", res)
	}

	// 事件序：恰好 intent + completed 一对，且 intent 在前（seq 单调）。
	evs := h.events()
	if got, want := h.kinds(), []string{loop.KindToolCallIntent, loop.KindToolCallCompleted}; !equalStrings(got, want) {
		t.Fatalf("事件 kind 序 = %v，want %v", got, want)
	}
	intentEv, doneEv := evs[0], evs[1]
	if intentEv.Seq == nil || doneEv.Seq == nil || *intentEv.Seq >= *doneEv.Seq {
		t.Fatalf("seq 必须单调递增: intent=%v completed=%v", intentEv.Seq, doneEv.Seq)
	}
	for _, ev := range evs {
		if !ev.Durable || ev.V != 1 || ev.TS == "" {
			t.Fatalf("事件 envelope 不符: %+v", ev)
		}
		if ev.SessionID != h.session || ev.AgentID != h.agent {
			t.Fatalf("事件归属不符: %+v", ev)
		}
	}

	// ① intent 载荷：id / 参数原样，idempotent/replay 取自工具描述，policy 快照。
	in := decodeIntent(t, intentEv)
	if string(in.ToolCallID) != "call_blackBox-01" {
		t.Fatalf("intent.tool_call_id = %q（S4：必须原样）", in.ToolCallID)
	}
	if string(in.Arguments) != `{"text":"hi"}` {
		t.Fatalf("intent.arguments = %s（S4：必须原样）", in.Arguments)
	}
	if in.ToolName != "echo" || !in.Idempotent || in.Replay != contracts.ReplayKindVerify {
		t.Fatalf("intent 载荷不符: %+v", in)
	}
	var snap struct {
		Verdict string `json:"verdict"`
		RuleID  string `json:"rule_id"`
		Reason  string `json:"reason"`
	}
	if err := json.Unmarshal(in.PolicySnapshot, &snap); err != nil {
		t.Fatalf("policy_snapshot 不是合法 JSON: %v", err)
	}
	if snap.Verdict != "allow" || snap.RuleID != "allow:echo" || snap.Reason == "" {
		t.Fatalf("policy_snapshot 不符: %+v", snap)
	}

	// ③a completed 载荷：同一个黑盒 id + 工具结果。
	done := decodeCompleted(t, doneEv)
	if string(done.ToolCallID) != "call_blackBox-01" {
		t.Fatalf("completed.tool_call_id = %q（必须与 intent 一致）", done.ToolCallID)
	}
	if len(done.Result.Content) != 1 || done.Result.Content[0].Text == nil || *done.Result.Content[0].Text != "mock:echo" {
		t.Fatalf("completed.result 不符: %+v", done.Result)
	}
	// 因果指针（F3）：completed 指向同一次调用的 intent。
	if doneEv.CausationID == nil || *doneEv.CausationID != intentEv.ID {
		t.Fatalf("completed.CausationID 应指向 intent：%v", doneEv.CausationID)
	}
	// Event.ID 与 provider 的 tool_call_id 永不共字段（S5）。
	if intentEv.ID == contracts.Ulid(in.ToolCallID) {
		t.Fatal("Event.ID 不得等于 tool_call_id（S5）")
	}

	// X1：intent 先于执行、completed 后于执行（共享时间线）。
	if got, want := h.log.all(), []string{"intent:call_blackBox-01", "exec:call_blackBox-01", "completed:call_blackBox-01"}; !equalStrings(got, want) {
		t.Fatalf("写路径顺序 = %v，want %v", got, want)
	}

	// 工具结果回填给模型（role=tool，id 原样）。
	reqs := h.provider.reqs()
	if len(reqs) != 2 {
		t.Fatalf("应有 2 次模型调用，得到 %d", len(reqs))
	}
	second := reqs[1].Messages
	if len(second) != 3 {
		t.Fatalf("第二轮历史应有 3 条消息（user/assistant/tool），得到 %d: %+v", len(second), second)
	}
	if second[1].Role != "assistant" || len(second[1].ToolCalls) != 1 || second[1].ToolCalls[0].ID != "call_blackBox-01" {
		t.Fatalf("assistant 消息未原样带回工具调用: %+v", second[1])
	}
	if second[2].Role != "tool" || second[2].ToolCallID != "call_blackBox-01" || second[2].Content != "mock:echo" {
		t.Fatalf("tool 消息回填不符: %+v", second[2])
	}

	// 可见工具：registry 里 kind=tool 的描述原样进请求（含 Schema）。
	if len(reqs[0].Tools) != 1 || reqs[0].Tools[0].Name != "echo" {
		t.Fatalf("可见工具不符: %+v", reqs[0].Tools)
	}
	if string(reqs[0].Tools[0].Schema) != `{"type":"object","properties":{"text":{"type":"string"}}}` {
		t.Fatalf("工具 Schema 未原样传递: %s", reqs[0].Tools[0].Schema)
	}
	if reqs[0].Model != "test-model" || reqs[0].System != "test-system" {
		t.Fatalf("ModelRequest 的 model/system 不符: %+v", reqs[0])
	}

	// 流事件被 emit 过但**不入库**（F8/陷阱 11）。
	if h.provider.emitCalls == 0 {
		t.Fatal("provider 的 emit 应被调用")
	}
	for _, ev := range evs {
		if !ev.Durable {
			t.Fatalf("不得有非 durable 事件入库: %+v", ev)
		}
	}
}

func TestProviderBlackBoxIDRoundTripUnchanged(t *testing.T) {
	// id 带大小写混合、连字符与下划线：不得折叠、不得规范化（S4/陷阱 1/17）。
	const weird = "Call_AbC-xYz_9f"
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(stepTools(call(weird, "echo", `{}`)), stepText("ok"))

	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	evs := h.events()
	if len(evs) != 2 {
		t.Fatalf("应有 2 条事件，得到 %d", len(evs))
	}
	if got := string(decodeIntent(t, evs[0]).ToolCallID); got != weird {
		t.Fatalf("intent.tool_call_id = %q，want %q", got, weird)
	}
	if got := string(decodeCompleted(t, evs[1]).ToolCallID); got != weird {
		t.Fatalf("completed.tool_call_id = %q，want %q", got, weird)
	}
	reqs := h.provider.reqs()
	last := reqs[len(reqs)-1].Messages
	if last[len(last)-1].ToolCallID != contracts.ProviderId(weird) {
		t.Fatalf("回填消息的 ToolCallID = %q，want %q", last[len(last)-1].ToolCallID, weird)
	}
}

func TestIntentArgumentsPassedThroughVerbatim(t *testing.T) {
	// 参数含空白与嵌套结构：loop 侧必须逐字节原样（只做拷贝，不改写）。
	raw := `{"a": 1, "b": [1, 2], "c": {"d": "x"}}`
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(stepTools(call("c1", "echo", raw)), stepText("ok"))
	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 执行器拿到的是同一份字节（原样，未被规范化/补全）。
	if got := string(h.execs["echo"].seen()[0].intent.Arguments); got != raw {
		t.Fatalf("执行器收到的 arguments 被改写:\n got %s\nwant %s", got, raw)
	}
	// 事件里是语义同一的 JSON：encoding/json 序列化 RawMessage 时会压缩空白
	// （JSON 编码器的行为，不是 loop 改写语义），故按值比较。
	got := decodeIntent(t, h.events()[0]).Arguments
	if !sameJSON(t, got, json.RawMessage(raw)) {
		t.Fatalf("事件 arguments 与模型给的参数不是同一 JSON:\n got %s\nwant %s", got, raw)
	}

	// 紧凑单行 JSON 在事件里逐字节保留（无空白可压缩）。
	h2 := newHarness(t, allowRule("echo"))
	h2.addTool("echo", nil)
	compact := `{"a":1,"b":[1,2]}`
	h2.script(stepTools(call("c2", "echo", compact)), stepText("ok"))
	if _, err := h2.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := string(decodeIntent(t, h2.events()[0]).Arguments); got != compact {
		t.Fatalf("紧凑参数应逐字节保留: got %s want %s", got, compact)
	}
}

// sameJSON 比较两段原始 JSON 是否语义同一。
func sameJSON(t *testing.T, a, b json.RawMessage) bool {
	t.Helper()
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		t.Fatalf("左值不是合法 JSON: %v", err)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		t.Fatalf("右值不是合法 JSON: %v", err)
	}
	return fmt.Sprintf("%#v", av) == fmt.Sprintf("%#v", bv)
}

func TestIntentDefaultsWhenToolDescriptionOmitsThem(t *testing.T) {
	// 工具描述不声明 idempotent/replay → intent 记 idempotent=false、replay=unsafe
	// （M7：不声明就不自动重试；X2：未知结果不盲目重放）。
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(stepTools(call("c1", "echo", `{}`)), stepText("ok"))
	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	in := decodeIntent(t, h.events()[0])
	if in.Idempotent {
		t.Fatal("缺省 idempotent 应为 false")
	}
	if in.Replay != contracts.ReplayKindUnsafe {
		t.Fatalf("缺省 replay = %q，want unsafe", in.Replay)
	}
}

func TestEmptyArgumentsNormalizedToObject(t *testing.T) {
	// provider 给 nil/空参数：补成 {}（不把 null 当参数对象传给工具），其余情况原样。
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(stepTools(call("c1", "echo", "")), stepText("ok"))
	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := string(decodeIntent(t, h.events()[0]).Arguments); got != "{}" {
		t.Fatalf("空参数应补成 {}，得到 %s", got)
	}
}

// ---------------------------------------------------------------------------
// deny / ask 路径（fail-close）
// ---------------------------------------------------------------------------

func TestDenyPathWritesIntentPlusFailedAndNeverExecutes(t *testing.T) {
	cases := []struct {
		name  string
		rules []contracts.PolicyRule
		appr  core.Approver
		want  string // 失败说明里必须出现的片段
		code  string
	}{
		{
			name:  "无命中规则（空引擎）",
			rules: nil,
			appr:  nil,
			want:  "fail-close",
			code:  loop.CodePolicyDeny,
		},
		{
			name:  "显式 deny 规则",
			rules: []contracts.PolicyRule{denyRule("bash")},
			appr:  nil,
			want:  "deny:bash",
			code:  loop.CodePolicyDeny,
		},
		{
			name:  "ask 但 Approver 缺席（DenyAll 默认）",
			rules: []contracts.PolicyRule{askRule("bash")},
			appr:  nil,
			want:  "没有配置 Approver",
			code:  loop.CodePolicyDeny,
		},
		{
			name:  "ask 且 Approver 显式 DenyAll",
			rules: []contracts.PolicyRule{askRule("bash")},
			appr:  core.DenyAll{},
			want:  "fail-close",
			code:  loop.CodePolicyDeny,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, tc.rules...)
			exec := h.addTool("bash", nil)
			h.script(stepTools(call("c1", "bash", `{"cmd":"rm -rf /"}`)), stepText("收尾"))

			res, err := h.loop(func(c *loop.Config) { c.Approver = tc.appr }).Run(context.Background(), "删掉一切")
			if err != nil {
				t.Fatalf("deny 不应让 Run 失败（失败是普通事件，M1）: %v", err)
			}
			if res.ToolCalls != 1 || res.Events != 2 || res.Turns != 2 {
				t.Fatalf("Result 不符: %+v", res)
			}

			evs := h.events()
			if got, want := h.kinds(), []string{loop.KindToolCallIntent, loop.KindToolCallFailed}; !equalStrings(got, want) {
				t.Fatalf("事件 kind 序 = %v，want %v", got, want)
			}
			// X1：intent 落盘了，但没有 completed，也**没有执行**。
			if got := h.log.all(); len(got) != 2 || got[0] != "intent:c1" || got[1] != "failed:c1" {
				t.Fatalf("deny 不应触发执行: %v", got)
			}
			if n := exec.callN(); n != 0 {
				t.Fatalf("deny 路径执行器被调用 %d 次（必须 0）", n)
			}

			fp := decodeFailed(t, evs[1])
			if string(fp.ToolCallID) != "c1" {
				t.Fatalf("failed.tool_call_id = %q", fp.ToolCallID)
			}
			if !strings.Contains(fp.Error.Message, tc.want) {
				t.Fatalf("失败说明应含 %q，得到 %q", tc.want, fp.Error.Message)
			}
			if fp.Error.Code == nil || *fp.Error.Code != tc.code {
				t.Fatalf("失败码 = %v，want %s", fp.Error.Code, tc.code)
			}
			// deny 的快照也落在 intent 里（M9：判定时刻可审计）。
			if snap := decodeIntent(t, evs[0]).PolicySnapshot; !strings.Contains(string(snap), "deny") {
				t.Fatalf("deny 快照应记 verdict=deny: %s", snap)
			}
			// 失败说明回填给模型（M5：模型可读、能自我修正）。
			reqs := h.provider.reqs()
			msgs := reqs[len(reqs)-1].Messages
			if msgs[len(msgs)-1].Role != "tool" || !strings.Contains(msgs[len(msgs)-1].Content, tc.want) {
				t.Fatalf("deny 说明未回填: %+v", msgs[len(msgs)-1])
			}
		})
	}
}

func TestAskWithAlwaysAllowApproverExecutes(t *testing.T) {
	h := newHarness(t, askRule("bash"))
	exec := h.addTool("bash", nil)
	h.script(stepTools(call("c1", "bash", `{"cmd":"ls"}`)), stepText("列完了"))

	res, err := h.loop(func(c *loop.Config) { c.Approver = core.AlwaysAllow{} }).Run(context.Background(), "列目录")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ToolCalls != 1 || res.Events != 2 {
		t.Fatalf("Result 不符: %+v", res)
	}
	if got, want := h.kinds(), []string{loop.KindToolCallIntent, loop.KindToolCallCompleted}; !equalStrings(got, want) {
		t.Fatalf("事件 kind 序 = %v，want %v", got, want)
	}
	if n := exec.callN(); n != 1 {
		t.Fatalf("人审放行后应执行 1 次，得到 %d", n)
	}
	// 快照记的是"人审放行后的 allow"（审计能看出是 ask 被批准的）。
	var snap struct {
		Verdict string `json:"verdict"`
		Reason  string `json:"reason"`
	}
	if err := json.Unmarshal(decodeIntent(t, h.events()[0]).PolicySnapshot, &snap); err != nil {
		t.Fatalf("快照解不开: %v", err)
	}
	if snap.Verdict != "allow" || !strings.Contains(snap.Reason, "人审放行") {
		t.Fatalf("快照不符: %+v", snap)
	}
}

// errApprover 是必然出错的审批人：证明审批故障按 deny 处理（fail-close）。
type errApprover struct{}

func (errApprover) Approve(context.Context, contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	return contracts.ApprovalResponse{}, errors.New("审批服务不可用 (injected)")
}

func TestAskWithApproverErrorFailsClosed(t *testing.T) {
	h := newHarness(t, askRule("bash"))
	exec := h.addTool("bash", nil)
	h.script(stepTools(call("c1", "bash", `{}`)), stepText("收尾"))

	res, err := h.loop(func(c *loop.Config) { c.Approver = errApprover{} }).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("审批失败应落 failed 事件而不是让 Run 失败: %v", err)
	}
	if res.ToolCalls != 1 || res.Events != 2 {
		t.Fatalf("Result 不符: %+v", res)
	}
	if n := exec.callN(); n != 0 {
		t.Fatalf("审批失败时不得执行（调用 %d 次）", n)
	}
	fp := decodeFailed(t, h.events()[1])
	if !strings.Contains(fp.Error.Message, "审批服务不可用") || !strings.Contains(fp.Error.Message, "fail-close") {
		t.Fatalf("失败说明应点名审批故障并按 deny 处理: %q", fp.Error.Message)
	}
}

// ---------------------------------------------------------------------------
// 多工具一轮 / 未知工具 / 执行器 panic
// ---------------------------------------------------------------------------

func TestMultipleToolCallsInOneTurn(t *testing.T) {
	h := newHarness(t, allowRule("echo"), allowRule("grep"))
	echo := h.addTool("echo", nil)
	grep := h.addTool("grep", nil)
	h.script(
		stepTools(call("c1", "echo", `{"text":"a"}`), call("c2", "grep", `{"pat":"x"}`)),
		stepText("都跑完了"),
	)

	res, err := h.loop(nil).Run(context.Background(), "一次两个")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ToolCalls != 2 || res.Events != 4 || res.Turns != 2 {
		t.Fatalf("Result 不符: %+v", res)
	}
	// 每个调用各自成对，且严格串行：intent1 → exec1 → completed1 → intent2 → exec2 → completed2。
	want := []string{
		"intent:c1", "exec:c1", "completed:c1",
		"intent:c2", "exec:c2", "completed:c2",
	}
	if got := h.log.all(); !equalStrings(got, want) {
		t.Fatalf("写路径顺序 = %v，want %v", got, want)
	}
	if echo.callN() != 1 || grep.callN() != 1 {
		t.Fatalf("两个执行器应各调用 1 次: echo=%d grep=%d", echo.callN(), grep.callN())
	}
	// 历史：user + assistant(2 calls) + tool + tool。
	reqs := h.provider.reqs()
	msgs := reqs[len(reqs)-1].Messages
	if len(msgs) != 4 {
		t.Fatalf("第二轮历史应有 4 条消息，得到 %d", len(msgs))
	}
	if len(msgs[1].ToolCalls) != 2 || msgs[1].ToolCalls[0].ID != "c1" || msgs[1].ToolCalls[1].ID != "c2" {
		t.Fatalf("assistant 消息应带回两个调用: %+v", msgs[1].ToolCalls)
	}
	if msgs[2].ToolCallID != "c1" || msgs[3].ToolCallID != "c2" {
		t.Fatalf("tool 消息 id 顺序错: %q %q", msgs[2].ToolCallID, msgs[3].ToolCallID)
	}
	if msgs[2].Content != "mock:echo" || msgs[3].Content != "mock:grep" {
		t.Fatalf("tool 消息内容错: %q %q", msgs[2].Content, msgs[3].Content)
	}
}

func TestUnknownToolNameWritesFailedAndNeverExecutes(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)
	h.script(stepTools(call("c1", "ghost_tool", `{}`)), stepText("收尾"))

	res, err := h.loop(nil).Run(context.Background(), "用一个不存在的工具")
	if err != nil {
		t.Fatalf("未知工具应落 failed 而不是让 Run 失败: %v", err)
	}
	if res.ToolCalls != 1 || res.Events != 2 {
		t.Fatalf("Result 不符: %+v", res)
	}
	if got, want := h.kinds(), []string{loop.KindToolCallIntent, loop.KindToolCallFailed}; !equalStrings(got, want) {
		t.Fatalf("事件 kind 序 = %v，want %v", got, want)
	}
	if exec.callN() != 0 || h.log.all()[len(h.log.all())-1] != "failed:c1" {
		t.Fatalf("未知工具不得执行: %v", h.log.all())
	}
	fp := decodeFailed(t, h.events()[1])
	if !strings.Contains(fp.Error.Message, "未在注册表注册") {
		t.Fatalf("失败说明应点名未注册: %q", fp.Error.Message)
	}
	if fp.Error.Code == nil || *fp.Error.Code != loop.CodeToolNotFound {
		t.Fatalf("失败码 = %v，want %s", fp.Error.Code, loop.CodeToolNotFound)
	}
}

func TestRegisteredButNotAToolExtensionWritesFailed(t *testing.T) {
	// 名字注册过但 kind 不是 tool（Entry.Tool 为 nil）→ 同样不执行、落 failed。
	h := newHarness(t, allowRule("panel"))
	if err := h.reg.Register(contracts.ExtensionMeta{Name: "panel", Kind: "panel"}, "impl"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h.script(stepTools(call("c1", "panel", `{}`)), stepText("收尾"))

	res, err := h.loop(nil).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Events != 2 || res.ToolCalls != 1 {
		t.Fatalf("Result 不符: %+v", res)
	}
	fp := decodeFailed(t, h.events()[1])
	if !strings.Contains(fp.Error.Message, "没有工具描述") {
		t.Fatalf("失败说明应说明缺工具描述: %q", fp.Error.Message)
	}
}

func TestEmptyToolNameFailsClosed(t *testing.T) {
	// 模型给出空工具名：策略侧空 Tool 恒不命中 → fail-close deny，且不得执行任何东西。
	h := newHarness(t, allowRule("*")) // 连通配 allow 也救不了空工具名（Evaluate 里 q.Tool=="" 直接 deny）
	exec := h.addTool("echo", nil)
	h.script(stepTools(call("c1", "", `{}`)), stepText("收尾"))

	res, err := h.loop(nil).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ToolCalls != 1 || res.Events != 2 {
		t.Fatalf("Result 不符: %+v", res)
	}
	if got, want := h.kinds(), []string{loop.KindToolCallIntent, loop.KindToolCallFailed}; !equalStrings(got, want) {
		t.Fatalf("事件 kind 序 = %v，want %v", got, want)
	}
	if n := exec.callN(); n != 0 {
		t.Fatalf("空工具名不得执行（%d 次）", n)
	}
	fp := decodeFailed(t, h.events()[1])
	if !strings.Contains(fp.Error.Message, "fail-close") {
		t.Fatalf("应按 fail-close 拒绝: %q", fp.Error.Message)
	}
}

func TestExecutorPanicIsIsolatedAsFailedEvent(t *testing.T) {
	h := newHarness(t, allowRule("boom"))
	exec := h.addTool("boom", nil)
	exec.panicOn = true
	h.script(stepTools(call("c1", "boom", `{}`)), stepText("崩溃后继续"))

	res, err := h.loop(nil).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("执行器 panic 应被隔离成普通失败，而不是带崩 loop: %v", err)
	}
	if res.ToolCalls != 1 || res.Events != 2 || res.Text != "崩溃后继续" {
		t.Fatalf("Result 不符: %+v", res)
	}
	fp := decodeFailed(t, h.events()[1])
	if !strings.Contains(fp.Error.Message, "panic") {
		t.Fatalf("失败说明应说明 panic: %q", fp.Error.Message)
	}
	if fp.Error.Code == nil || *fp.Error.Code != loop.CodeToolError {
		t.Fatalf("失败码 = %v，want %s", fp.Error.Code, loop.CodeToolError)
	}
	// 崩溃说明回填给模型（M8：失败不是特殊通道）。
	reqs := h.provider.reqs()
	msgs := reqs[len(reqs)-1].Messages
	if !strings.Contains(msgs[len(msgs)-1].Content, "panic") {
		t.Fatalf("panic 说明未回填: %q", msgs[len(msgs)-1].Content)
	}
}

func TestExecutorErrorIsFailedEventAndLoopContinues(t *testing.T) {
	h := newHarness(t, allowRule("flaky"))
	exec := h.addTool("flaky", nil)
	exec.err = errors.New("磁盘满了 (injected)")
	h.script(stepTools(call("c1", "flaky", `{}`)), stepText("我自己重试"))

	res, err := h.loop(nil).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("工具失败不应让 Run 失败: %v", err)
	}
	if res.ToolCalls != 1 || res.Events != 2 {
		t.Fatalf("Result 不符: %+v", res)
	}
	fp := decodeFailed(t, h.events()[1])
	if !strings.Contains(fp.Error.Message, "磁盘满了") {
		t.Fatalf("执行器错误文案应模型可读: %q", fp.Error.Message)
	}
	if exec.callN() != 1 {
		t.Fatalf("执行器应被调用 1 次，得到 %d", exec.callN())
	}
}

// ---------------------------------------------------------------------------
// MaxTurns 截断 / ctx 取消 / provider 错误
// ---------------------------------------------------------------------------

func TestMaxTurnsTruncatesRunawayToolLoop(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)
	h.repeat(stepTools(call("c1", "echo", `{}`)))

	res, err := h.loop(func(c *loop.Config) { c.MaxTurns = 3 }).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Turns != 3 {
		t.Fatalf("Turns = %d，want 3（MaxTurns 截断）", res.Turns)
	}
	if res.ToolCalls != 3 || res.Events != 6 {
		t.Fatalf("Result 不符: %+v", res)
	}
	if n := h.provider.callCount(); n != 3 {
		t.Fatalf("模型调用 %d 次，want 3", n)
	}
	if n := exec.callN(); n != 3 {
		t.Fatalf("执行器调用 %d 次，want 3", n)
	}
}

func TestMaxTurnsDefaultsToTwenty(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.repeat(stepTools(call("c1", "echo", `{}`)))

	res, err := h.loop(nil).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Turns != loop.DefaultMaxTurns {
		t.Fatalf("Turns = %d，want 默认 %d", res.Turns, loop.DefaultMaxTurns)
	}
	if loop.DefaultMaxTurns != 20 {
		t.Fatalf("DefaultMaxTurns = %d，want 20", loop.DefaultMaxTurns)
	}
}

func TestContextCanceledBeforeRunReturnsCtxErr(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(stepText("不该被调用"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := h.loop(nil).Run(ctx, "p")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v，want context.Canceled", err)
	}
	if res.Turns != 0 || res.ToolCalls != 0 || res.Events != 0 {
		t.Fatalf("取消后不应有任何动作: %+v", res)
	}
	if n := h.provider.callCount(); n != 0 {
		t.Fatalf("取消后不应调用模型（%d 次）", n)
	}
	if n := len(h.events()); n != 0 {
		t.Fatalf("取消后不应落任何事件（%d 条）", n)
	}
}

func TestContextCanceledDuringToolExecution(t *testing.T) {
	h := newHarness(t, allowRule("slow"))
	exec := h.addTool("slow", nil)
	ctx, cancel := context.WithCancel(context.Background())
	// 执行器在执行期间取消 ctx 并返回错误（模拟"工具跑到一半被取消"）。
	exec.err = nil
	canceling := &cancelThenFailExecutor{inner: exec, cancel: cancel}
	if err := h.reg.Unregister("slow"); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	if err := h.reg.RegisterTool(contracts.Tool{
		Name: "slow", SideEffect: contracts.SideEffectWrite, Schema: json.RawMessage(`{}`),
	}, canceling); err != nil {
		t.Fatalf("RegisterTool: %v", err)
	}
	h.script(stepTools(call("c1", "slow", `{}`)), stepText("收尾"))

	res, err := h.loop(nil).Run(ctx, "p")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v，want context.Canceled", err)
	}
	// X1：有 intent 就要有结果——尽力补一条 failed，然后才返回取消语义。
	if got, want := h.kinds(), []string{loop.KindToolCallIntent, loop.KindToolCallFailed}; !equalStrings(got, want) {
		t.Fatalf("事件 kind 序 = %v，want %v", got, want)
	}
	if res.Events != 2 || res.ToolCalls != 1 {
		t.Fatalf("Result 不符: %+v", res)
	}
	fp := decodeFailed(t, h.events()[1])
	if !strings.Contains(fp.Error.Message, "取消") {
		t.Fatalf("失败说明应说明被取消: %q", fp.Error.Message)
	}
}

// cancelThenFailExecutor 先取消 ctx 再返回错误（包装真实执行器以保留调用记录）。
type cancelThenFailExecutor struct {
	inner  *mockExecutor
	cancel context.CancelFunc
}

func (e *cancelThenFailExecutor) Execute(ctx context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	res, _ := e.inner.Execute(ctx, intent)
	e.cancel()
	return res, errors.New("工具执行被取消 (injected)")
}

var _ core.ToolExecutor = (*cancelThenFailExecutor)(nil)

func TestProviderErrorPublishesLoopErrorEvent(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	boom := errors.New("模型服务 503 (injected)")
	h.script(scriptStep{err: boom})

	res, err := h.loop(nil).Run(context.Background(), "p")
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v，want 包装后的 %v", err, boom)
	}
	if res.Turns != 1 || res.Events != 1 || res.Text != "" {
		t.Fatalf("Result 不符: %+v", res)
	}
	evs := h.events()
	if len(evs) != 1 || evs[0].Kind != loop.KindLoopError {
		t.Fatalf("应落一条 loop.error 事件，得到 %v", h.kinds())
	}
	if !evs[0].Durable {
		t.Fatal("loop.error 必须是 durable 事实事件")
	}
	var payload struct {
		Turn    int    `json:"turn"`
		Phase   string `json:"phase"`
		Message string `json:"message"`
		Code    string `json:"code"`
	}
	if err := json.Unmarshal(evs[0].Payload, &payload); err != nil {
		t.Fatalf("loop.error payload 解不开: %v", err)
	}
	if payload.Turn != 1 || payload.Phase != "provider" || payload.Code != loop.CodeLoopError {
		t.Fatalf("loop.error payload 不符: %+v", payload)
	}
	if !strings.Contains(payload.Message, "503") {
		t.Fatalf("loop.error 应带模型可读说明: %+v", payload)
	}
}

// ---------------------------------------------------------------------------
// X1 硬保证：intent 没落盘就绝不执行
// ---------------------------------------------------------------------------

func TestIntentPublishFailurePreventsExecution(t *testing.T) {
	eng := policy.New(allowRule("echo"))
	reg := registry.New(registry.NewPolicyAdjudicator(eng))
	exec := &mockExecutor{}
	if err := reg.RegisterTool(contracts.Tool{Name: "echo", Schema: json.RawMessage(`{}`)}, exec); err != nil {
		t.Fatalf("RegisterTool: %v", err)
	}
	prov := &mockProvider{steps: []scriptStep{
		stepTools(call("c1", "echo", `{"text":"x"}`)),
		stepText("收尾"),
	}}
	l, err := loop.New(loop.Config{
		Provider:  prov,
		Bus:       bus.New(failStore{}), // 事件一律落不进去
		Registry:  reg,
		Policy:    eng,
		SessionID: core.NewUlid(),
		AgentID:   core.NewUlid(),
		Model:     "m",
	})
	if err != nil {
		t.Fatalf("loop.New: %v", err)
	}

	if _, err := l.Run(context.Background(), "p"); err == nil {
		t.Fatal("intent 落盘失败时 Run 必须返回 error")
	}
	if n := exec.callN(); n != 0 {
		t.Fatalf("intent 没落盘却执行了 %d 次（违反 X1）", n)
	}
	if n := prov.callCount(); n != 1 {
		t.Fatalf("模型调用 %d 次，want 1", n)
	}
}

// ---------------------------------------------------------------------------
// 幂等：同一 Loop 反复 Run 不撞 id、seq 不重用
// ---------------------------------------------------------------------------

func TestRepeatedRunsDoNotCollide(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	l := h.loop(nil)
	for i := 0; i < 3; i++ {
		h.script(stepTools(call("c1", "echo", `{}`)), stepText(fmt.Sprintf("第 %d 次", i)))
		res, err := l.Run(context.Background(), "p")
		if err != nil {
			t.Fatalf("第 %d 次 Run: %v", i, err)
		}
		if res.Events != 2 || res.ToolCalls != 1 {
			t.Fatalf("第 %d 次 Result 不符: %+v", i, res)
		}
	}
	evs := h.events()
	if len(evs) != 6 {
		t.Fatalf("三次 Run 应有 6 条事件，得到 %d", len(evs))
	}
	seenID := map[contracts.Ulid]bool{}
	seenSeq := map[int64]bool{}
	for _, ev := range evs {
		if seenID[ev.ID] {
			t.Fatalf("Event.ID 重复: %s", ev.ID)
		}
		seenID[ev.ID] = true
		if ev.Seq == nil || seenSeq[*ev.Seq] {
			t.Fatalf("seq 缺失或重复: %v", ev.Seq)
		}
		seenSeq[*ev.Seq] = true
	}
}
