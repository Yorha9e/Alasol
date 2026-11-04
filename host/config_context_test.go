package host_test

// G-h「config.context 上下文装配面」的 host 验收面（设计 15 §3 的第三批字段）：
//
//	① POST /prompt 带 config.context → 202 + 事件流有 config.changed（带 context）+
//	   **本轮生效**（占比分母跟着覆盖的窗口走——窗口换了、占比还按旧窗口算就是失真）；
//	② **持久**：下一条 prompt 不带 config → 覆盖仍在；带 window=0 → 清除回默认；
//	③ **重启生效**：同库重开 hub → fold 回放 → 覆盖仍在；
//	④ **运行中变更**：跑轮中落 config.changed → 本轮快照不受影响、下一轮生效；
//	⑤ **写时校验**：kind 非法 / 数值负数 / 百分比越界 / L2 > L1 → 400 bad_config +
//	   **零事件**；
//	⑥ **全空变更集判定**：`{"config":{}}` 仍不落事件；`{"config":{"context":{}}}` 要落；
//	⑦ **WS 同源**：command.submit 带 config.context 与 HTTP 面同码同形。
//
// 观测面：context.assembled 事件的 tokens_budget（占比分母）= 本轮生效的窗口。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/transport"
)

// ctxSpyCall 是一次模型调用的现场（断言本轮拿到的占比分母）。
type ctxSpyCall struct {
	Tools []string
}

// ctxSpyProvider 记录每次模型调用；blockFirst 让**第一次**调用阻塞在 provider 里
// （"运行中变更"用例据此把一个 Run 钉在半途）。
type ctxSpyProvider struct {
	mu         sync.Mutex
	calls      []ctxSpyCall
	blockFirst bool
	entered    chan struct{}
	release    chan struct{}
}

func (p *ctxSpyProvider) Run(_ context.Context, _ core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu.Lock()
	idx := len(p.calls)
	p.calls = append(p.calls, ctxSpyCall{})
	p.mu.Unlock()
	if p.blockFirst && idx == 0 {
		if p.entered != nil {
			select {
			case p.entered <- struct{}{}:
			default:
			}
		}
		if p.release != nil {
			<-p.release
		}
	}
	return core.ModelResult{Text: "spy", Usage: core.Usage{InputTokens: 1, OutputTokens: 1}}, nil
}

func (p *ctxSpyProvider) snapshot() []ctxSpyCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]ctxSpyCall, len(p.calls))
	copy(out, p.calls)
	return out
}

// ctxHub 起一个挂了 tiered 策略的宿主（窗口 300k → 占比分母基线 = 300k）。
func ctxHub(t *testing.T, prov core.Provider) *host.Hub {
	t.Helper()
	return newTestHub(t, func(c *host.Config) {
		c.Provider = prov
		c.ContextStrategy = "tiered"
		c.ContextStrategyWindow = 300_000 // ≥256k 硬约束；也是占比分母的基线
	})
}

// ctxToolSpyProvider 记录每次模型调用收到的**可见工具名**（kind 覆盖是否真的把
// tiered 策略挂上，用"框架工具 recall/stage 是否可见"当证人）。
type ctxToolSpyProvider struct {
	mu    sync.Mutex
	tools [][]string
}

func (p *ctxToolSpyProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	names := make([]string, 0, len(req.Tools))
	for i := range req.Tools {
		names = append(names, req.Tools[i].Name)
	}
	p.mu.Lock()
	p.tools = append(p.tools, names)
	p.mu.Unlock()
	return core.ModelResult{Text: "spy", Usage: core.Usage{InputTokens: 1, OutputTokens: 1}}, nil
}

func (p *ctxToolSpyProvider) snapshot() [][]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([][]string, len(p.tools))
	copy(out, p.tools)
	return out
}

// hasTool 报告某次调用看得见的工具里有没有这个名字。
func hasTool(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

// tokensBudgetOf 取某 session 最新一条 context.assembled 的 tokens_budget（占比分母）。
func tokensBudgetOf(t *testing.T, evs []contracts.Event) int64 {
	t.Helper()
	snaps := filterKind(evs, "context.assembled")
	if len(snaps) == 0 {
		t.Fatalf("没有 context.assembled 事件（事件 kinds=%v）", kindsOf(evs))
	}
	var snap contracts.ContextSnapshot
	if err := json.Unmarshal(snaps[len(snaps)-1].Payload, &snap); err != nil {
		t.Fatalf("解 context.assembled: %v", err)
	}
	return snap.TokensBudget
}

// ---------------------------------------------------------------------------
// ① 本轮生效 + 事件流带 context
// ---------------------------------------------------------------------------

// TestPromptConfigContextAppliesThisRun：POST /prompt 带
// {"config":{"context":{"window":512000}}} → 202 + config.changed（载荷带 context.window）
// + 本轮占比分母 = 512000（覆盖生效）。
func TestPromptConfigContextAppliesThisRun(t *testing.T) {
	prov := &ctxSpyProvider{}
	h := ctxHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "带上下文覆盖的第一轮",
		"config": map[string]any{"context": map[string]any{"window": 512000}},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}

	evs := waitForSessionEvents(t, srv.URL, sid, "config 轮的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	cc := filterKind(evs, "config.changed")
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1（事件 kinds=%v)", len(cc), kindsOf(evs))
	}
	p := mustConfigPayload(t, cc[0])
	if p.Context == nil || p.Context.Window == nil || *p.Context.Window != 512000 {
		t.Fatalf("config.changed 载荷应带 context.window=512000: %s", string(cc[0].Payload))
	}
	if !cc[0].Durable || cc[0].SessionID != sid {
		t.Fatalf("config.changed envelope 不符: durable=%v sid=%v", cc[0].Durable, cc[0].SessionID)
	}
	// 本轮占比分母 = 覆盖的窗口（覆盖生效的直接证据）。
	if got := tokensBudgetOf(t, evs); got != 512000 {
		t.Fatalf("本轮 tokens_budget = %d，want 512000（覆盖窗口当占比分母）", got)
	}
}

// ---------------------------------------------------------------------------
// ② 持久 + 清除回默认
// ---------------------------------------------------------------------------

// TestPromptConfigContextPersistsAndClears：
//
//	window=512000 → 下一条不带 config 仍生效（持久）→ window=0 清除 → 回默认（300k）。
func TestPromptConfigContextPersistsAndClears(t *testing.T) {
	prov := &ctxSpyProvider{}
	h := ctxHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	prompt := func(what int, body map[string]any) []contracts.Event {
		t.Helper()
		if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", body, nil); code != http.StatusAccepted {
			t.Fatalf("第 %d 条 prompt 状态码 = %d，want 202", what, code)
		}
		return waitForSessionEvents(t, srv.URL, sid,
			fmt.Sprintf("第 %d 条 prompt 的 model.turn", what),
			func(evs []contracts.Event) bool { return kindCount(evs, "model.turn") >= what })
	}

	// ① 覆盖窗口。
	evs := prompt(1, map[string]any{"prompt": "一", "config": map[string]any{
		"context": map[string]any{"window": 512000},
	}})
	if got := tokensBudgetOf(t, evs); got != 512000 {
		t.Fatalf("① 覆盖后 tokens_budget = %d，want 512000", got)
	}
	// ② 不带 config → 仍是 512000（持久：fold 在，每个 Run 开头重取）。
	evs = prompt(2, map[string]any{"prompt": "二"})
	if got := tokensBudgetOf(t, evs); got != 512000 {
		t.Fatalf("② 不带 config 的一轮 tokens_budget = %d，want 512000（持久）", got)
	}
	// ③ window=0 → 清除回默认（宿主窗口 300k）。
	evs = prompt(3, map[string]any{"prompt": "三", "config": map[string]any{
		"context": map[string]any{"window": 0},
	}})
	if got := tokensBudgetOf(t, evs); got != 300000 {
		t.Fatalf("③ 清除后 tokens_budget = %d，want 300000（回宿主默认）", got)
	}
	// config.changed 恰 2 条（覆盖 + 清除；"不带 config"那条不落事件）。
	all := sessionEvents(t, srv.URL, sid)
	if n := kindCount(all, "config.changed"); n != 2 {
		t.Fatalf("config.changed 条数 = %d，want 2", n)
	}
}

// ---------------------------------------------------------------------------
// ③ 重启生效（同库重开 → fold 回放）
// ---------------------------------------------------------------------------

// TestPromptConfigContextSurvivesHubReopen：h1 写入 window 覆盖 → 关掉 → 同一个库
// 重开 h2 → 启动播种全量重放 config.changed → 新一轮仍按覆盖的窗口算占比。
func TestPromptConfigContextSurvivesHubReopen(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "events.db")
	newCfg := func(p core.Provider) host.Config {
		return host.Config{
			DBPath: db, Model: "scripted", System: "cfg-context-reopen", MaxTurns: 4,
			Provider: p, CWD: dir,
			ContextStrategy: "tiered", ContextStrategyWindow: 300_000,
		}
	}

	prov1 := &ctxSpyProvider{}
	h1, err := host.New(newCfg(prov1))
	if err != nil {
		t.Fatalf("host.New #1: %v", err)
	}
	srv1 := httptest.NewServer(h1.Handler())
	sid := host.NewSession()
	if code := postPrompt(t, srv1.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "重启前", "config": map[string]any{"context": map[string]any{"window": 512000}},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv1.URL, sid, "重启前的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	if got := tokensBudgetOf(t, sessionEvents(t, srv1.URL, sid)); got != 512000 {
		t.Fatalf("重启前 tokens_budget = %d，want 512000", got)
	}
	_ = h1.Close()
	srv1.Close()

	// 同库重开 → fold 回放 → 覆盖仍在。
	prov2 := &ctxSpyProvider{}
	h2, err := host.New(newCfg(prov2))
	if err != nil {
		t.Fatalf("host.New #2: %v", err)
	}
	t.Cleanup(func() { _ = h2.Close() })
	srv2 := httptest.NewServer(h2.Handler())
	defer srv2.Close()
	if _, err := h2.RunPrompt(context.Background(), sid, "重启后"); err != nil {
		t.Fatalf("重启后 RunPrompt: %v", err)
	}
	evs := waitForSessionEvents(t, srv2.URL, sid, "重启后的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 2
	})
	if got := tokensBudgetOf(t, evs); got != 512000 {
		t.Fatalf("重启后 tokens_budget = %d，want 512000（fold 回放：上下文覆盖重启仍生效）", got)
	}
}

// ---------------------------------------------------------------------------
// ④ 运行中变更：本轮快照不受影响、下一个 prompt 生效
// ---------------------------------------------------------------------------

// TestPromptConfigContextMidRunChangeAppliesNextRun：轮 A 的 provider 被钉住（快照已冻结，
// 分母 = 基线 300k）→ 期间另一条带 config.context 的 prompt 落 config.changed（轮 B
// 生效 window=512000）→ 放开轮 A（**仍是基线**：运行中变更不打断/不改写本轮）。
func TestPromptConfigContextMidRunChangeAppliesNextRun(t *testing.T) {
	prov := &ctxSpyProvider{
		blockFirst: true,
		entered:    make(chan struct{}, 1),
		release:    make(chan struct{}),
	}
	h := ctxHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	// 轮 A：无 config（分母 = 基线 300k），进 provider 后被钉住。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]any{"prompt": "轮A"}, nil); code != http.StatusAccepted {
		t.Fatalf("轮 A 状态码 = %d，want 202", code)
	}
	select {
	case <-prov.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("10s 内轮 A 未进 provider")
	}

	// 运行中：另一条带 config.context 的 prompt 落 config.changed → 轮 B 起跑。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]any{"prompt": "轮B", "config": map[string]any{
			"context": map[string]any{"window": 512000},
		}}, nil); code != http.StatusAccepted {
		t.Fatalf("轮 B 状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "轮 B 的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	// 轮 B 的分母 = 覆盖值。
	evs := sessionEvents(t, srv.URL, sid)
	if got := tokensBudgetOf(t, evs); got != 512000 {
		t.Fatalf("轮 B tokens_budget = %d，want 512000（运行中落的覆盖生效）", got)
	}

	// 放开轮 A → 收场：轮 A 的分母仍是基线（快照不受运行中变更影响）。
	close(prov.release)
	waitForSessionEvents(t, srv.URL, sid, "轮 A 的 model.turn（放开后）", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 2
	})
	snaps := filterKind(sessionEvents(t, srv.URL, sid), "context.assembled")
	if len(snaps) < 2 {
		t.Fatalf("应有两条 context.assembled，得到 %d 条", len(snaps))
	}
	var first contracts.ContextSnapshot
	if err := json.Unmarshal(snaps[0].Payload, &first); err != nil {
		t.Fatalf("解 context.assembled: %v", err)
	}
	if first.TokensBudget != 300000 {
		t.Fatalf("轮 A 的 tokens_budget = %d，want 300000（运行中变更不影响本轮快照）", first.TokensBudget)
	}
}

// ---------------------------------------------------------------------------
// ⑤ 写时校验（fail-close：非法配置零事件）
// ---------------------------------------------------------------------------

// TestPromptConfigContextValidation：kind 非法 / 窗口负数 / 百分比越界 / L2 > L1 /
// 窗口低于硬约束 → 400 bad_config + **零事件**；合法值 → 202。
func TestPromptConfigContextValidation(t *testing.T) {
	prov := &ctxSpyProvider{}
	h := ctxHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	bad := []struct {
		what string
		ctx  map[string]any
	}{
		{"kind 非法", map[string]any{"kind": "sliding_window"}},
		{"窗口负数", map[string]any{"window": -1}},
		{"规则预算负数", map[string]any{"rules_budget": -5}},
		{"百分比越界", map[string]any{"l1_pct": 200}},
		{"L2 大于 L1", map[string]any{"l1_pct": 5, "l2_pct": 8}},
		{"窗口低于硬约束", map[string]any{"window": 100000}},
	}
	for _, tc := range bad {
		var errBody contracts.TransportError
		code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
			"prompt": tc.what, "config": map[string]any{"context": tc.ctx},
		}, &errBody)
		if code != http.StatusBadRequest {
			t.Fatalf("(%s) 状态码 = %d，want 400", tc.what, code)
		}
		if errBody.Code != "bad_config" {
			t.Fatalf("(%s) 错误码 = %q，want bad_config", tc.what, errBody.Code)
		}
	}
	// 非法配置零事件（config.changed 一条都不该有）。
	if n := kindCount(sessionEvents(t, srv.URL, sid), "config.changed"); n != 0 {
		t.Fatalf("非法配置落了 %d 条 config.changed，want 0（写时 fail-close）", n)
	}

	// 合法值 → 202（且落一条 config.changed）。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "合法", "config": map[string]any{"context": map[string]any{
			"kind": "tiered", "window": 512000, "l1_pct": 8, "l2_pct": 0.5,
			"rules_budget": 20000, "common_sense_budget": 40000,
		}},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("合法配置状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "合法配置的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	if n := kindCount(sessionEvents(t, srv.URL, sid), "config.changed"); n != 1 {
		t.Fatalf("合法配置落了 %d 条 config.changed，want 1", n)
	}
}

// ---------------------------------------------------------------------------
// ⑥ 全空变更集判定（回归）
// ---------------------------------------------------------------------------

// TestPromptConfigContextEmptyChangeSetStillLandsEvent：
//
//	`{"config":{}}` → **不落事件**（全空变更集 = 无操作，回归 G-c 口径）；
//	`{"config":{"context":{}}}` → **要落事件**（context 出现即是有变更）。
func TestPromptConfigContextEmptyChangeSetStillLandsEvent(t *testing.T) {
	prov := &ctxSpyProvider{}
	h := ctxHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "空变更集", "config": map[string]any{},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "第一条的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	if n := kindCount(sessionEvents(t, srv.URL, sid), "config.changed"); n != 0 {
		t.Fatalf("全空变更集落了多少 config.changed = %d，want 0", n)
	}

	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "空 context", "config": map[string]any{"context": map[string]any{}},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "第二条的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 2
	})
	cc := filterKind(sessionEvents(t, srv.URL, sid), "config.changed")
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1（context 出现即是有变更）", len(cc))
	}
	if p := mustConfigPayload(t, cc[0]); p.Context == nil {
		t.Fatalf("事件的载荷应带 context: %s", string(cc[0].Payload))
	}
}

// ---------------------------------------------------------------------------
// ⑦ WS 同源
// ---------------------------------------------------------------------------

// TestWSContextConfigSameSource：WS command.submit 带 config.context ——
// 合法 → command.accepted + config.changed + 覆盖生效；非法 → kind=error 帧
// （code=bad_config，与 HTTP 400 同码）+ 零新增 config.changed。
func TestWSContextConfigSameSource(t *testing.T) {
	prov := &ctxSpyProvider{}
	h := ctxHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := warmupSession(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := transport.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, contracts.SubscribeRequest{}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitWSKind(t, c, transport.KindSubscribed, 10*time.Second)

	// ① 合法：带 context.window 覆盖。
	win := int64(512000)
	msg, err := transport.NewMessage(transport.KindCommandSubmit, contracts.CommandSubmit{
		SessionID: &sid,
		Prompt:    "WS 带上下文覆盖",
		Config:    &contracts.ConfigChangedPayload{Context: &contracts.ContextConfigChange{Window: &win}},
	})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if err := c.SendContext(ctx, msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	waitWSKind(t, c, transport.KindCommandAccepted, 10*time.Second)
	if !waitEventKind(t, c, "model.turn", 10*time.Second) {
		t.Fatal("WS 带配置的轮次没等到 model.turn")
	}
	evs := sessionEvents(t, srv.URL, sid)
	cc := filterKind(evs, "config.changed")
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1", len(cc))
	}
	if p := mustConfigPayload(t, cc[0]); p.Context == nil || p.Context.Window == nil || *p.Context.Window != 512000 {
		t.Fatalf("config.changed 载荷应带 context.window=512000: %s", string(cc[0].Payload))
	}
	if got := tokensBudgetOf(t, evs); got != 512000 {
		t.Fatalf("WS 那一轮 tokens_budget = %d，want 512000（覆盖生效）", got)
	}

	// ② 非法：负数窗口 → kind=error 帧（code 与 HTTP 400 同码）+ 零新增事件。
	badWin := int64(-1)
	msg2, err := transport.NewMessage(transport.KindCommandSubmit, contracts.CommandSubmit{
		SessionID: &sid,
		Prompt:    "WS 坏上下文",
		Config:    &contracts.ConfigChangedPayload{Context: &contracts.ContextConfigChange{Window: &badWin}},
	})
	if err != nil {
		t.Fatalf("NewMessage #2: %v", err)
	}
	if err := c.SendContext(ctx, msg2); err != nil {
		t.Fatalf("send #2: %v", err)
	}
	em := waitWSKind(t, c, transport.KindError, 10*time.Second)
	var te contracts.TransportError
	if err := json.Unmarshal(em.Payload, &te); err != nil {
		t.Fatalf("error payload: %v", err)
	}
	if te.Code != "bad_config" {
		t.Fatalf("错误帧 code = %q，want bad_config（与 HTTP 400 同码）", te.Code)
	}
	if n := kindCount(sessionEvents(t, srv.URL, sid), "config.changed"); n != 1 {
		t.Fatalf("非法配置不该落事件，config.changed = %d 条，want 1", n)
	}
}

// ---------------------------------------------------------------------------
// ⑧ kind 覆盖：session 能把 append 提成 tiered（宿主补挂 tiered 能力工厂）
// ---------------------------------------------------------------------------

// TestPromptConfigContextKindOverrideEnablesTiered：宿主默认是 append（不挂策略）→
// session 提交 config.context.kind=tiered → **下一轮**模型看得见 tiered 的框架工具
// （recall / stage）——策略真挂上了（append 路径下它们不可见）。
func TestPromptConfigContextKindOverrideEnablesTiered(t *testing.T) {
	prov := &ctxToolSpyProvider{}
	h := newTestHub(t, func(c *host.Config) {
		c.Provider = prov
		// 宿主默认 append（不挂上下文策略）；放行框架工具（它们也过策略链）。
		c.Allow = []string{"read", "recall", "stage"}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	prompt := func(what int, body map[string]any) {
		t.Helper()
		if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", body, nil); code != http.StatusAccepted {
			t.Fatalf("第 %d 条 prompt 状态码 = %d，want 202", what, code)
		}
		waitForSessionEvents(t, srv.URL, sid,
			fmt.Sprintf("第 %d 条 prompt 的 model.turn", what),
			func(evs []contracts.Event) bool { return kindCount(evs, "model.turn") >= what })
	}

	// ① 默认 append：框架工具不可见。
	prompt(1, map[string]any{"prompt": "一"})
	// ② kind=tiered → 策略挂上（框架工具可见）。
	prompt(2, map[string]any{"prompt": "二", "config": map[string]any{
		"context": map[string]any{"kind": "tiered"},
	}})

	calls := prov.snapshot()
	if len(calls) < 2 {
		t.Fatalf("provider 调用数 = %d，want ≥2", len(calls))
	}
	if hasTool(calls[0], "recall") {
		t.Errorf("默认 append 那一轮不该看得见 recall（框架工具只在策略路径注入）: %v", calls[0])
	}
	if !hasTool(calls[1], "recall") || !hasTool(calls[1], "stage") {
		t.Errorf("kind=tiered 那一轮应看得见 recall/stage（策略挂上了）: %v", calls[1])
	}
}
