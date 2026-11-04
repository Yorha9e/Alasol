package host_test

// GET /api/sessions/{id}/config（GUI 配置面板的种子面）的验收面：
//
//	① 全默认：session 没有任何 config 事件 → 生效值 = hub 默认 + overridden_fields 为空；
//	② 部分覆盖：落 config.changed 后回读 → 生效值（hub 默认叠加 session fold）与
//	   overridden_fields（哪些字段是 session 级覆盖）都对；
//	③ 清除态：覆盖过再写空串/0 → 生效值回默认，但 overridden_fields **仍列该字段**
//	   （写过 = 覆盖过；三态口径）；
//	④ 404 unknown_session：事件流里没见过这个 session（错误体同 writeJSONError 形状）；
//	⑤ 未托管的历史 session **照常返回**（fold 从事件流来，读面不校验托管）。

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
)

// sessionConfigHub 起一个带默认配置的宿主（配置面板种子的对照组）。
//
// hub 级默认：模型 scripted、档位 low、凭据变量名 HUB_KEY_ENV、上下文策略 append
// （窗口等六项走 compaction 的缺省）。
func sessionConfigHub(t *testing.T, prov core.Provider) *host.Hub {
	t.Helper()
	return newTestHub(t, func(c *host.Config) {
		c.Provider = prov
		c.ReasoningEffort = "low"
		c.APIKeyEnv = "HUB_KEY_ENV"
	})
}

// getSessionConfig 读某 session 的生效配置（状态码 + 视图）。
func getSessionConfig(t *testing.T, base string, sid contracts.Ulid) (int, host.SessionConfigView) {
	t.Helper()
	var view host.SessionConfigView
	code := getJSON(t, base+"/api/sessions/"+string(sid)+"/config", &view)
	return code, view
}

// TestSessionConfigEndpointDefaults：没有任何 config 事件的 session → 全默认 +
// overridden_fields 为空（[] 不是 null）。
func TestSessionConfigEndpointDefaults(t *testing.T) {
	prov := &configSpyProvider{}
	h := sessionConfigHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]any{"prompt": "配置面板种子（无覆盖）"}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	// 跑一轮 → session 在事件流里（投影判据 = 事实流）。
	waitForSessionEvents(t, srv.URL, sid, "model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})

	code, view := getSessionConfig(t, srv.URL, sid)
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，want 200", code)
	}
	if view.SessionID != string(sid) {
		t.Errorf("session_id = %q，want %q", view.SessionID, sid)
	}
	// 生效值 = hub 默认。
	if view.Model != "scripted" {
		t.Errorf("model = %q，want scripted（hub 默认）", view.Model)
	}
	if view.ReasoningEffort != "low" {
		t.Errorf("reasoning_effort = %q，want low（hub 默认）", view.ReasoningEffort)
	}
	if view.Provider.APIKeyEnv != "HUB_KEY_ENV" {
		t.Errorf("provider.api_key_env = %q，want HUB_KEY_ENV（hub 默认）", view.Provider.APIKeyEnv)
	}
	if view.Provider.BaseURL != "" {
		t.Errorf("provider.base_url = %q，want 空（hub 默认没配端点）", view.Provider.BaseURL)
	}
	// 工具白名单：没写过 → 未设置（不限）——不是"显式空数组"。
	if view.Tools.Only != nil || view.Tools.Except != nil {
		t.Errorf("tools 未设置时不该出现白名单: only=%v except=%v", view.Tools.Only, view.Tools.Except)
	}
	// 上下文面：生效 kind = append（hub 默认没挂 tiered），六项走缺省。
	if view.Context.Kind != "append" {
		t.Errorf("context.kind = %q，want append（hub 默认）", view.Context.Kind)
	}
	if view.Context.Window != 1000000 || view.Context.L1Pct != 10 || view.Context.L2Pct != 1 {
		t.Errorf("context 窗口/百分比应取缺省: %+v", view.Context)
	}
	if view.Context.RulesBudget != 15000 || view.Context.CommonSenseBudget != 30000 {
		t.Errorf("context 注入区预算应取布局缺省: %+v", view.Context)
	}
	// overridden_fields：空数组（不是 null）。
	if view.OverriddenFields == nil {
		t.Fatal("overridden_fields 应是空数组，不是 null")
	}
	if len(view.OverriddenFields) != 0 {
		t.Errorf("无覆盖时 overridden_fields 应为空，实际 %v", view.OverriddenFields)
	}
}

// TestSessionConfigEndpointOverrides：部分覆盖 → 生效值 + overridden_fields 都对。
func TestSessionConfigEndpointOverrides(t *testing.T) {
	prov := &configSpyProvider{}
	h := sessionConfigHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "带覆盖的一轮",
		"config": map[string]any{
			"model":            "m-override",
			"reasoning_effort": "high",
			"tools":            map[string]any{"only": []string{"read"}},
			"context": map[string]any{
				"kind":                "tiered",
				"window":              512000,
				"l1_pct":              8,
				"l2_pct":              0.5,
				"rules_budget":        20000,
				"common_sense_budget": 30000,
			},
		},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "config.changed", func(evs []contracts.Event) bool {
		return kindCount(evs, "config.changed") >= 1
	})

	code, view := getSessionConfig(t, srv.URL, sid)
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，want 200", code)
	}
	if view.Model != "m-override" {
		t.Errorf("model = %q，want m-override（fold 覆盖）", view.Model)
	}
	if view.ReasoningEffort != "high" {
		t.Errorf("reasoning_effort = %q，want high（fold 覆盖）", view.ReasoningEffort)
	}
	// tools：only 有值、except **未设置**（组里没带的子项 = 未设置，不是空数组）。
	if view.Tools.Only == nil || len(*view.Tools.Only) != 1 || (*view.Tools.Only)[0] != "read" {
		t.Errorf("tools.only = %v，want [read]", view.Tools.Only)
	}
	if view.Tools.Except != nil {
		t.Errorf("tools.except 未设置时不该出现: %v", view.Tools.Except)
	}
	// context：六项都是 session 的覆盖值。
	if view.Context.Kind != "tiered" {
		t.Errorf("context.kind = %q，want tiered", view.Context.Kind)
	}
	if view.Context.Window != 512000 || view.Context.L1Pct != 8 || view.Context.L2Pct != 0.5 ||
		view.Context.RulesBudget != 20000 || view.Context.CommonSenseBudget != 30000 {
		t.Errorf("context 生效值应取 fold 覆盖: %+v", view.Context)
	}
	// overridden_fields：session 显式写过的字段（点分路径；确定性顺序）。
	want := []string{
		"model", "reasoning_effort", "tools.only",
		"context.kind", "context.window", "context.l1_pct", "context.l2_pct",
		"context.rules_budget", "context.common_sense_budget",
	}
	if len(view.OverriddenFields) != len(want) {
		t.Fatalf("overridden_fields = %v，want %v", view.OverriddenFields, want)
	}
	for i, w := range want {
		if view.OverriddenFields[i] != w {
			t.Errorf("overridden_fields[%d] = %q，want %q（整体 %v）", i, view.OverriddenFields[i], w, view.OverriddenFields)
		}
	}
}

// TestSessionConfigEndpointProviderOverride：provider 覆盖 → 回显端点 + 凭据**变量名**，
// **绝不含 key 值**（H15）。
func TestSessionConfigEndpointProviderOverride(t *testing.T) {
	t.Setenv("SESSION_KEY_ENV", "session-secret-key-value")
	prov := &configSpyProvider{}
	h := sessionConfigHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	// 端点指向本机一个没人听的端口 → 覆盖生效后那一轮会失败（快），但 config.changed
	// 已经在起跑前同步落库（写路径顺序：先落事件再起跑）。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "带 provider 覆盖的一轮",
		"config": map[string]any{
			"provider": map[string]any{
				"base_url":    "http://127.0.0.1:1/v1",
				"api_key_env": "SESSION_KEY_ENV",
			},
		},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "config.changed", func(evs []contracts.Event) bool {
		return kindCount(evs, "config.changed") >= 1
	})

	code, body := getRaw(t, srv.URL+"/api/sessions/"+string(sid)+"/config")
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，want 200（体=%s）", code, body)
	}
	var view host.SessionConfigView
	if err := json.Unmarshal([]byte(body), &view); err != nil {
		t.Fatalf("解 JSON: %v（体=%s）", err, body)
	}
	if view.Provider.BaseURL != "http://127.0.0.1:1/v1" {
		t.Errorf("provider.base_url = %q，want http://127.0.0.1:1/v1", view.Provider.BaseURL)
	}
	if view.Provider.APIKeyEnv != "SESSION_KEY_ENV" {
		t.Errorf("provider.api_key_env = %q，want SESSION_KEY_ENV（只回显变量名）", view.Provider.APIKeyEnv)
	}
	if strings.Contains(body, "session-secret-key-value") {
		t.Fatalf("响应体**绝不能**含 key 值（H15）: %s", body)
	}
	if !containsStr(view.OverriddenFields, "provider.base_url") ||
		!containsStr(view.OverriddenFields, "provider.api_key_env") {
		t.Errorf("overridden_fields 应含 provider 两项，实际 %v", view.OverriddenFields)
	}
}

// TestSessionConfigEndpointClearSemantics：清除态 —— 覆盖过再写空串/0 → 生效值回默认，
// 但 overridden_fields **仍列该字段**（写过 = 覆盖过；三态口径）。
func TestSessionConfigEndpointClearSemantics(t *testing.T) {
	prov := &configSpyProvider{}
	h := sessionConfigHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	// ① 先覆盖 model 与 context.window。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "先覆盖",
		"config": map[string]any{
			"model":   "m-override",
			"context": map[string]any{"window": 512000},
		},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "第一条 config.changed", func(evs []contracts.Event) bool {
		return kindCount(evs, "config.changed") >= 1
	})
	// ② 再清除：model 写空串、context.window 写 0（都是"清除回默认"）。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "再清除",
		"config": map[string]any{
			"model":   "",
			"context": map[string]any{"window": 0},
		},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "第二条 config.changed", func(evs []contracts.Event) bool {
		return kindCount(evs, "config.changed") >= 2
	})

	code, view := getSessionConfig(t, srv.URL, sid)
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，want 200", code)
	}
	// 生效值回默认。
	if view.Model != "scripted" {
		t.Errorf("清除后 model = %q，want scripted（回 hub 默认）", view.Model)
	}
	if view.Context.Window != 1000000 {
		t.Errorf("清除后 context.window = %d，want 1000000（回 hub 默认）", view.Context.Window)
	}
	// 但"覆盖过"这件事仍在清单里（审计上"曾覆盖过、后来被清掉"是事实）。
	if !containsStr(view.OverriddenFields, "model") {
		t.Errorf("overridden_fields 应含 model（写过空串 = 覆盖过）: %v", view.OverriddenFields)
	}
	if !containsStr(view.OverriddenFields, "context.window") {
		t.Errorf("overridden_fields 应含 context.window（写过 0 = 覆盖过）: %v", view.OverriddenFields)
	}
}

// TestSessionConfigEndpointUnknownSession：事件流里没有的 session → 404 unknown_session
// （错误体照 writeJSONError 的既有形状：{"code":…,"message":…}）。
func TestSessionConfigEndpointUnknownSession(t *testing.T) {
	h := sessionConfigHub(t, &configSpyProvider{})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession() // 从没出现过的事件流
	var errBody contracts.TransportError
	code := getJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/config", &errBody)
	if code != http.StatusNotFound {
		t.Fatalf("状态码 = %d，want 404", code)
	}
	if errBody.Code != "unknown_session" {
		t.Fatalf("错误码 = %q，want unknown_session", errBody.Code)
	}
	if strings.TrimSpace(errBody.Message) == "" {
		t.Error("错误体应带人读说明（message）")
	}
}

// TestSessionConfigEndpointUnmanagedHistoricalSession：未托管的历史 session **照常返回**
// （fold 从事件流来，读面不校验托管——一库一 hub 是预期拓扑）。
func TestSessionConfigEndpointUnmanagedHistoricalSession(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "events.db")
	cfg := host.Config{
		DBPath: dbPath, Model: "scripted", System: "cfg", MaxTurns: 2,
		Allow: []string{"read"}, Provider: &configSpyProvider{}, CWD: dir,
		ReasoningEffort: "low", APIKeyEnv: "HUB_KEY_ENV",
	}

	h1, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New#1: %v", err)
	}
	srv1 := httptest.NewServer(h1.Handler())
	sid := host.NewSession()
	if code := postPrompt(t, srv1.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "落一条配置",
		"config": map[string]any{"model": "m-hist"},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv1.URL, sid, "config.changed", func(evs []contracts.Event) bool {
		return kindCount(evs, "config.changed") >= 1
	})
	srv1.Close()
	if err := h1.Close(); err != nil {
		t.Fatalf("Close#1: %v", err)
	}

	// 重开（同一事件库 = 另一个宿主进程）：**不托管**那个 session，但读得到它的配置。
	h2, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New#2: %v", err)
	}
	t.Cleanup(func() { _ = h2.Close() })
	srv := httptest.NewServer(h2.Handler())
	defer srv.Close()

	code, view := getSessionConfig(t, srv.URL, sid)
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，want 200（未托管的历史 session 照常返回）", code)
	}
	if view.Model != "m-hist" {
		t.Errorf("model = %q，want m-hist（fold 从事件流回放）", view.Model)
	}
	if !containsStr(view.OverriddenFields, "model") {
		t.Errorf("overridden_fields 应含 model: %v", view.OverriddenFields)
	}
}
