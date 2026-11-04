package host_test

// G-e「config.tools 工具白名单」的 host 验收面（设计 15 §3.5，定稿）：
//
//	① POST /prompt 带 config.tools → 202 + 事件流有 config.changed（带 tools）+
//	   **本轮**模型可见集按白名单收窄（白名单制：不暴露即不存在）；
//	② **持久**：下一条 prompt 不带 config → 白名单仍在；带 tools:{} → 清除回默认；
//	③ **重启生效**：同库重开 hub → fold 回放 → 白名单仍在；
//	④ **运行中变更**：跑轮中落 config.changed → 本轮快照不受影响、下一轮生效；
//	⑤ **全空变更集判定**：`{"config":{}}` 仍不落事件；`{"config":{"tools":{}}}` 要落
//	   （tools 出现即是有变更——清除动作要可审计）；
//	⑥ **写时校验**：元素空串 / 数量超限 → 400 bad_config + **零事件**；重复名可容忍。
//
// 手法：脚本 provider（toolsSpyProvider）记录每次请求的**可见工具名**——
// "本轮模型到底看得见哪些工具"直接断言，不靠猜。

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

// toolsSpyCall 是一次模型调用的现场（断言本轮模型看得见哪些工具）。
type toolsSpyCall struct {
	Tools []string
}

// toolsSpyProvider 记录每次模型调用收到的**可见工具名**。
//
// blockFirst（配合 entered/release 通道）让**第一次**调用阻塞在 provider 里——
// "运行中变更"用例据此把一个 Run 钉在半途，期间从另一个请求落 config.changed。
type toolsSpyProvider struct {
	mu         sync.Mutex
	calls      []toolsSpyCall
	blockFirst bool
	entered    chan struct{}
	release    chan struct{}
}

func (p *toolsSpyProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	call := toolsSpyCall{Tools: visibleToolNames(req.Tools)}
	p.mu.Lock()
	idx := len(p.calls)
	p.calls = append(p.calls, call)
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

// snapshot 返回已记录调用的副本。
func (p *toolsSpyProvider) snapshot() []toolsSpyCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]toolsSpyCall, len(p.calls))
	copy(out, p.calls)
	return out
}

// waitCalls 轮询直到至少 n 次调用被记录（返回副本；超时 fail）。
func (p *toolsSpyProvider) waitCalls(t *testing.T, n int) []toolsSpyCall {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		calls := p.snapshot()
		if len(calls) >= n {
			return calls
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("10s 内 provider 调用数未到 %d（最后 %d 次）", n, len(p.snapshot()))
	return nil
}

// visibleToolNames 取请求里的可见工具名（顺序 = 注册顺序）。
func visibleToolNames(tools []contracts.Tool) []string {
	out := make([]string, 0, len(tools))
	for _, tl := range tools {
		out = append(out, tl.Name)
	}
	return out
}

// equalToolNames 比两个工具名列表（顺序敏感：可见集按注册顺序，确定性）。
func equalToolNames(a, b []string) bool {
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

// toolsHub 起一个带脚本 provider 的宿主（Allow 放行 read + write → 基线可见集
// = [read write]；bash / git_status 未放行 → 策略层就不暴露）。
func toolsHub(t *testing.T, prov *toolsSpyProvider) *host.Hub {
	t.Helper()
	return newTestHub(t, func(c *host.Config) {
		c.Provider = prov
		c.Allow = []string{"read", "write"}
	})
}

// ---------------------------------------------------------------------------
// ① 本轮生效 + 事件流带 tools
// ---------------------------------------------------------------------------

// TestPromptConfigToolsAppliesThisRun：POST /prompt 带 {"config":{"tools":{"only":["read"]}}}
// → 202 + config.changed 落库（载荷带 tools）+ 本轮模型可见集 = [read]（write 被藏起来）。
func TestPromptConfigToolsAppliesThisRun(t *testing.T) {
	prov := &toolsSpyProvider{}
	h := toolsHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	var acc struct {
		SessionID string `json:"session_id"`
		Status    string `json:"status"`
	}
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "带工具白名单的第一轮",
		"config": map[string]any{"tools": map[string]any{"only": []string{"read"}}},
	}, &acc); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	if acc.SessionID != string(sid) || acc.Status != "running" {
		t.Fatalf("受理回执不符: %+v", acc)
	}

	evs := waitForSessionEvents(t, srv.URL, sid, "config 轮的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})

	// 事件流里恰有一条 config.changed，载荷带 tools.only=[read]。
	cc := filterKind(evs, "config.changed")
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1（事件 kinds=%v)", len(cc), kindsOf(evs))
	}
	p := mustConfigPayload(t, cc[0])
	if p.Tools == nil || p.Tools.Only == nil {
		t.Fatalf("config.changed 载荷应带 tools.only: %s", string(cc[0].Payload))
	}
	if len(*p.Tools.Only) != 1 || (*p.Tools.Only)[0] != "read" {
		t.Fatalf("config.changed tools.only = %v，want [read]", *p.Tools.Only)
	}
	if cc[0].SessionID != sid {
		t.Fatalf("config.changed envelope.session_id = %s，want %s", cc[0].SessionID, sid)
	}
	if !cc[0].Durable {
		t.Fatal("config.changed 必须是 durable 事实事件")
	}

	// 本轮可见集 = [read]（白名单制：write 被藏起来 → 模型看不见）。
	calls := prov.waitCalls(t, 1)
	if !equalToolNames(calls[0].Tools, []string{"read"}) {
		t.Fatalf("本轮可见工具 = %v，want [read]（session 白名单生效）", calls[0].Tools)
	}
}

// ---------------------------------------------------------------------------
// ② 持久 + 清除回默认
// ---------------------------------------------------------------------------

// TestPromptConfigToolsPersistsAndClears：
//
//	only=[read] → 下一条不带 config 仍只看得见 read（持久）→ tools:{} 清除 → 回默认
//	（全部已注册且被策略放行的工具可见 = [read write]）。
func TestPromptConfigToolsPersistsAndClears(t *testing.T) {
	prov := &toolsSpyProvider{}
	h := toolsHub(t, prov)
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

	// ① 收窄到 [read]。
	prompt(1, map[string]any{"prompt": "一", "config": map[string]any{
		"tools": map[string]any{"only": []string{"read"}},
	}})
	// ② 不带 config → 仍是 [read]（持久：fold 在，每个 Run 开头重取）。
	prompt(2, map[string]any{"prompt": "二"})
	// ③ tools:{} → 清除回默认（全部已注册且放行的工具可见）。
	prompt(3, map[string]any{"prompt": "三", "config": map[string]any{
		"tools": map[string]any{},
	}})

	calls := prov.waitCalls(t, 3)
	if !equalToolNames(calls[0].Tools, []string{"read"}) {
		t.Fatalf("① 收窄后可见工具 = %v，want [read]", calls[0].Tools)
	}
	if !equalToolNames(calls[1].Tools, []string{"read"}) {
		t.Fatalf("② 不带 config 的下一轮仍应是 [read]（持久），得到 %v", calls[1].Tools)
	}
	if !equalToolNames(calls[2].Tools, []string{"read", "write"}) {
		t.Fatalf("③ 清除后应回默认 [read write]，得到 %v", calls[2].Tools)
	}

	// config.changed 恰 2 条（收窄 + 清除；"不带 config"那条不落事件）。
	evs := sessionEvents(t, srv.URL, sid)
	if n := kindCount(evs, "config.changed"); n != 2 {
		t.Fatalf("config.changed 条数 = %d，want 2", n)
	}
	// 清除那条载荷是 tools:{}（组出现、两子项都缺 = 清除回默认）。
	last := filterKind(evs, "config.changed")[1]
	cp := mustConfigPayload(t, last)
	if cp.Tools == nil {
		t.Fatalf("清除事件的载荷应带 tools（组出现）: %s", string(last.Payload))
	}
	if cp.Tools.Only != nil || cp.Tools.Except != nil {
		t.Fatalf("清除事件的 tools 两子项都该缺: %s", string(last.Payload))
	}
}

// ---------------------------------------------------------------------------
// ③ 全空变更集判定（回归）
// ---------------------------------------------------------------------------

// TestPromptConfigToolsEmptyChangeSetStillLandsEvent：
//
//	`{"config":{}}` → **不落事件**（全空变更集 = 无操作，回归 G-c 口径）；
//	`{"config":{"tools":{}}}` → **要落事件**（tools 出现即是有变更：清除要可审计）。
func TestPromptConfigToolsEmptyChangeSetStillLandsEvent(t *testing.T) {
	prov := &toolsSpyProvider{}
	h := toolsHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	// ① 全空变更集 → 受理但不落事件。
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

	// ② tools:{} → 落事件（清除回默认是一次真变更）。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "清除", "config": map[string]any{"tools": map[string]any{}},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "第二条的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 2
	})
	cc := filterKind(sessionEvents(t, srv.URL, sid), "config.changed")
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1（tools 出现即是有变更）", len(cc))
	}
	cp := mustConfigPayload(t, cc[0])
	if cp.Tools == nil {
		t.Fatalf("清除事件的载荷应带 tools: %s", string(cc[0].Payload))
	}
}

// ---------------------------------------------------------------------------
// ④ 重启生效（同库重开 → fold 回放）
// ---------------------------------------------------------------------------

// TestPromptConfigToolsSurvivesHubReopen：h1 写入 tools.only=[read] → 关掉 →
// 同一个库重开 h2 → 启动播种全量重放 config.changed → 新一轮 Run 仍只看得见 read。
func TestPromptConfigToolsSurvivesHubReopen(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "events.db")
	newCfg := func(p core.Provider) host.Config {
		return host.Config{
			DBPath: db, Model: "scripted", System: "cfg-tools-reopen", MaxTurns: 4,
			Allow: []string{"read", "write"}, Provider: p, CWD: dir,
		}
	}

	prov1 := &toolsSpyProvider{}
	h1, err := host.New(newCfg(prov1))
	if err != nil {
		t.Fatalf("host.New #1: %v", err)
	}
	srv1 := httptest.NewServer(h1.Handler())
	sid := host.NewSession()
	if code := postPrompt(t, srv1.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "重启前", "config": map[string]any{"tools": map[string]any{"only": []string{"read"}}},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("重启前 prompt 状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv1.URL, sid, "重启前的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	srv1.Close()
	if err := h1.Close(); err != nil {
		t.Fatalf("关第一个宿主: %v", err)
	}

	prov2 := &toolsSpyProvider{}
	h2, err := host.New(newCfg(prov2))
	if err != nil {
		t.Fatalf("host.New #2: %v", err)
	}
	t.Cleanup(func() { _ = h2.Close() })
	if _, err := h2.RunPrompt(context.Background(), sid, "重启后"); err != nil {
		t.Fatalf("重启后 RunPrompt: %v", err)
	}
	calls := prov2.snapshot()
	if len(calls) != 1 {
		t.Fatalf("重启后 provider 调用数 = %d，want 1", len(calls))
	}
	if !equalToolNames(calls[0].Tools, []string{"read"}) {
		t.Fatalf("重启后可见工具 = %v，want [read]（fold 回放：工具白名单重启仍生效）", calls[0].Tools)
	}
}

// ---------------------------------------------------------------------------
// ⑤ 运行中变更：本轮快照不受影响、下一个 prompt 生效
// ---------------------------------------------------------------------------

// TestPromptConfigToolsMidRunChangeAppliesNextRun：轮 A 的 provider 被钉住（快照已冻结，
// 可见集 = 默认）→ 期间另一条带 config.tools 的 prompt 落 config.changed（轮 B 生效
// only=[read]）→ 放开轮 A（仍是默认可见集，**运行中变更不打断/不改写本轮**）→
// 下一个 prompt（轮 C）生效 only=[read]。
func TestPromptConfigToolsMidRunChangeAppliesNextRun(t *testing.T) {
	prov := &toolsSpyProvider{
		blockFirst: true,
		entered:    make(chan struct{}, 1),
		release:    make(chan struct{}),
	}
	h := toolsHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	// 轮 A：无 config（默认可见集 = [read write]），进 provider 后被钉住。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]any{"prompt": "轮A"}, nil); code != http.StatusAccepted {
		t.Fatalf("轮 A 状态码 = %d，want 202", code)
	}
	select {
	case <-prov.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("10s 内轮 A 未进 provider")
	}
	if calls := prov.snapshot(); len(calls) != 1 || !equalToolNames(calls[0].Tools, []string{"read", "write"}) {
		t.Fatalf("轮 A 进 provider 时应记录默认可见集 [read write]，得到 %+v", calls)
	}

	// 运行中：另一条带 config.tools 的 prompt 落 config.changed → 轮 B 起跑。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]any{"prompt": "轮B", "config": map[string]any{
			"tools": map[string]any{"only": []string{"read"}},
		}}, nil); code != http.StatusAccepted {
		t.Fatalf("轮 B 状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "轮 B 的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	calls := prov.waitCalls(t, 2)
	if !equalToolNames(calls[1].Tools, []string{"read"}) {
		t.Fatalf("轮 B（运行中落的配置）可见工具 = %v，want [read]", calls[1].Tools)
	}
	// 轮 A 仍钉在 provider 里——它的快照**不受影响**（仍是默认可见集）。
	if got := prov.snapshot()[0].Tools; !equalToolNames(got, []string{"read", "write"}) {
		t.Fatalf("轮 A 快照被运行中变更改写了：%v（应保持 [read write]）", got)
	}

	// 放开轮 A → 收场（快照值不变）。
	close(prov.release)
	waitForSessionEvents(t, srv.URL, sid, "轮 A 的 model.turn（放开后）", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 2
	})
	if got := prov.snapshot()[0].Tools; !equalToolNames(got, []string{"read", "write"}) {
		t.Fatalf("轮 A 收场后可见工具 = %v，want [read write]（运行中变更不影响本轮）", got)
	}
}

// ---------------------------------------------------------------------------
// ⑥ 写时校验（fail-close：非法配置零事件）
// ---------------------------------------------------------------------------

// TestPromptConfigToolsValidation：元素空串 / 数量超限 → 400 bad_config + **零事件**；
// 重复名**可容忍**（名单语义是集合）→ 202。
func TestPromptConfigToolsValidation(t *testing.T) {
	prov := &toolsSpyProvider{}
	h := toolsHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	// ① 空串元素 → 400。
	var errBody contracts.TransportError
	code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "空工具名", "config": map[string]any{"tools": map[string]any{"only": []string{"read", "  "}}},
	}, &errBody)
	if code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，want 400", code)
	}
	if errBody.Code != "bad_config" {
		t.Fatalf("错误码 = %q，want bad_config", errBody.Code)
	}

	// ② 数量超限 → 400。
	tooMany := make([]string, 0, 300)
	for i := 0; i < 300; i++ {
		tooMany = append(tooMany, fmt.Sprintf("t%d", i))
	}
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "太多工具", "config": map[string]any{"tools": map[string]any{"only": tooMany}},
	}, nil); code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，want 400", code)
	}

	// 非法配置**零事件**（写时 fail-close：校验在落事件之前）。
	if n := kindCount(sessionEvents(t, srv.URL, sid), "config.changed"); n != 0 {
		t.Fatalf("非法配置落了 %d 条 config.changed，want 0", n)
	}

	// ③ 重复名可容忍 → 202 + 事件照落（名单是集合，重复项无副作用）。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "重复名", "config": map[string]any{"tools": map[string]any{"only": []string{"read", "read"}}},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202（重复名可容忍）", code)
	}
	evs := waitForSessionEvents(t, srv.URL, sid, "重复名那轮的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	cc := filterKind(evs, "config.changed")
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1", len(cc))
	}
	cp := mustConfigPayload(t, cc[0])
	if cp.Tools == nil || cp.Tools.Only == nil || len(*cp.Tools.Only) != 2 {
		t.Fatalf("重复名应原样落进事件（审计要看到用户写了什么）: %s", string(cc[0].Payload))
	}
}

// TestPromptConfigToolsUnregisteredNameTolerated：点名**未注册**的工具不报错
// （注册表可变 + 白名单语义天然容错：点名不存在的工具 = 该工具不存在）。
func TestPromptConfigToolsUnregisteredNameTolerated(t *testing.T) {
	prov := &toolsSpyProvider{}
	h := toolsHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "点名未注册的工具",
		"config": map[string]any{"tools": map[string]any{"only": []string{"read", "no_such_tool"}}},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202（未注册的工具名不该被拒）", code)
	}
	calls := prov.waitCalls(t, 1)
	if !equalToolNames(calls[0].Tools, []string{"read"}) {
		t.Fatalf("可见工具 = %v，want [read]（点名未注册的工具 = 该工具不存在）", calls[0].Tools)
	}
}

// ---------------------------------------------------------------------------
// ⑦ WS command.submit 同源（HTTP/WS 两面共用同一条校验 + 落事件路径）
// ---------------------------------------------------------------------------

// TestWSSubmitToolsSameSource：WS command.submit 带 config.tools → 与 HTTP 面同源同型：
// command.accepted + config.changed（带 tools）+ 本轮可见集按白名单收窄。
func TestWSSubmitToolsSameSource(t *testing.T) {
	prov := &toolsSpyProvider{}
	h := toolsHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := warmupSession(t, h) // 已托管（warmup 占第 1 次 provider 调用）

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

	only := contracts.ToolNameList{"read"}
	msg, err := transport.NewMessage(transport.KindCommandSubmit, contracts.CommandSubmit{
		SessionID: &sid,
		Prompt:    "WS 带工具白名单",
		Config: &contracts.ConfigChangedPayload{
			Tools: &contracts.ToolsConfigChange{Only: &only},
		},
	})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if err := c.SendContext(ctx, msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	m := waitWSKind(t, c, transport.KindCommandAccepted, 10*time.Second)
	var ca contracts.CommandAccepted
	if err := json.Unmarshal(m.Payload, &ca); err != nil {
		t.Fatalf("command.accepted payload: %v", err)
	}
	if ca.SessionID != sid || ca.Status != "running" {
		t.Fatalf("command.accepted 不符: %+v", ca)
	}
	if !waitEventKind(t, c, "model.turn", 10*time.Second) {
		t.Fatal("WS 带工具白名单的轮次没等到 model.turn")
	}

	// 事件流里有那条 config.changed（载荷带 tools.only=[read]）。
	evs := sessionEvents(t, srv.URL, sid)
	cc := filterKind(evs, "config.changed")
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1", len(cc))
	}
	p := mustConfigPayload(t, cc[0])
	if p.Tools == nil || p.Tools.Only == nil || len(*p.Tools.Only) != 1 || (*p.Tools.Only)[0] != "read" {
		t.Fatalf("config.changed 载荷应带 tools.only=[read]: %s", string(cc[0].Payload))
	}

	// 本轮可见集 = [read]（warmup 那轮是默认可见集）。
	calls := prov.waitCalls(t, 2)
	if !equalToolNames(calls[0].Tools, []string{"read", "write"}) {
		t.Fatalf("warmup 那轮可见工具 = %v，want [read write]", calls[0].Tools)
	}
	if !equalToolNames(calls[1].Tools, []string{"read"}) {
		t.Fatalf("WS 带白名单那轮可见工具 = %v，want [read]", calls[1].Tools)
	}
}
