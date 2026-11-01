package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/registry"
)

// 本文件是**包内**测试（package host）：扩展注册表的运行期写入面（Register / SetHealth）
// 在包外够不着——Hub.Registry() 只给只读视图（F5：导出接口、不导出可写的 *registry.Registry；
// 与 EventSource 同一纪律）。故"运行期注册一个扩展 → GET /api/extensions 看得到"这条钉子
// 放包内（直接用 h.reg），外部面只钉只读可见的部分。

// TestExtensionsSnapshotRuntimeRegistered 钉死：运行期注册的扩展（申报能力 + 回填 Health）
// 在 GET /api/extensions 快照里如实反映（E3 申报 + E5 回填的观测落点）。
func TestExtensionsSnapshotRuntimeRegistered(t *testing.T) {
	h, err := New(Config{
		DBPath: filepath.Join(t.TempDir(), "events.db"),
		Model:  "scripted",
		CWD:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	desc := "上下文面板扩展"
	if err := h.reg.Register(contracts.ExtensionMeta{
		Name:         "panel.context",
		Kind:         "panel",
		Description:  &desc,
		Capabilities: []string{"fs:read"},
	}, "impl"); err != nil {
		t.Fatalf("Register(panel.context): %v", err)
	}
	msg := "healthy"
	if err := h.reg.SetHealth("panel.context", contracts.ExtensionHealth{
		Status:  contracts.HealthStatusUp,
		Message: &msg,
	}); err != nil {
		t.Fatalf("SetHealth: %v", err)
	}

	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	code, body := getJSONRaw(t, srv.URL+"/api/extensions")
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，want 200（body=%s）", code, body)
	}
	var views []ExtensionView
	if err := json.Unmarshal([]byte(body), &views); err != nil {
		t.Fatalf("解 JSON: %v（body=%s）", err, body)
	}
	byName := make(map[string]ExtensionView, len(views))
	for _, v := range views {
		byName[v.Name] = v
	}

	pv, ok := byName["panel.context"]
	if !ok {
		t.Fatalf("panel.context 不在快照里")
	}
	if pv.Kind != "panel" {
		t.Fatalf("panel.context kind = %q, want panel", pv.Kind)
	}
	if len(pv.Capabilities) != 1 || pv.Capabilities[0] != "fs:read" {
		t.Fatalf("panel.context capabilities = %v, want [fs:read]", pv.Capabilities)
	}
	if pv.Health == nil || pv.Health.Status != contracts.HealthStatusUp {
		t.Fatalf("panel.context health = %+v, want up", pv.Health)
	}
	if pv.Health.Message == nil || *pv.Health.Message != "healthy" {
		t.Fatalf("panel.context health message = %v, want healthy", pv.Health.Message)
	}
	if pv.Description == nil || *pv.Description != desc {
		t.Fatalf("panel.context description = %v", pv.Description)
	}
	// F6：无能力的条目 capabilities 归一为 []（JSON 里不是 null）。
	for _, v := range views {
		if v.Capabilities == nil {
			t.Fatalf("%q capabilities = null，want []（空值归一，F6）", v.Name)
		}
	}
}

// ---------------------------------------------------------------------------
// 缺口 3：POST /api/extensions/{name}/enable|disable 启停端点（包内面）
// ---------------------------------------------------------------------------

// postNoBody 发一个无体 POST（端点不读 body），返回状态码 + 原始响应体。
func postNoBody(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader("{}")) //nolint:gosec // 测试用本地 httptest URL
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读响应体: %v", err)
	}
	return resp.StatusCode, string(b)
}

// extStateOf 从 GET /api/extensions 快照里取指定扩展的 state（找不到即 Fatal）。
func extStateOf(t *testing.T, base, name string) string {
	t.Helper()
	code, body := getJSONRaw(t, base+"/api/extensions")
	if code != http.StatusOK {
		t.Fatalf("GET /api/extensions 状态码 = %d，want 200（body=%s）", code, body)
	}
	var views []ExtensionView
	if err := json.Unmarshal([]byte(body), &views); err != nil {
		t.Fatalf("解 JSON: %v（body=%s）", err, body)
	}
	for _, v := range views {
		if v.Name == name {
			return v.State
		}
	}
	t.Fatalf("扩展 %q 不在快照里", name)
	return ""
}

// TestExtensionEnableDisableRoundTrip 钉启停往返：注册测试扩展（无能力声明）→
// disable → 快照 disabled → 再 disable 仍 200（幂等）→ enable → 快照 enabled。
func TestExtensionEnableDisableRoundTrip(t *testing.T) {
	h, err := New(Config{
		DBPath: filepath.Join(t.TempDir(), "events.db"),
		Model:  "scripted",
		CWD:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if err := h.reg.Register(contracts.ExtensionMeta{
		Name: "tool.gate_probe",
		Kind: "tool",
	}, "impl"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// 初始态 registered（未 Enable）。
	if got := extStateOf(t, srv.URL, "tool.gate_probe"); got != string(registry.StateRegistered) {
		t.Fatalf("初始 state = %q，want registered", got)
	}

	// disable → 200 {"ok":true}，快照立即变 disabled。
	code, body := postNoBody(t, srv.URL+"/api/extensions/tool.gate_probe/disable")
	if code != http.StatusOK || !strings.Contains(body, `"ok":true`) {
		t.Fatalf("disable 状态码 = %d（body=%s），want 200 {\"ok\":true}", code, body)
	}
	if got := extStateOf(t, srv.URL, "tool.gate_probe"); got != string(registry.StateDisabled) {
		t.Fatalf("disable 后 state = %q，want disabled", got)
	}

	// 幂等：已停用再停 → 仍 200（registry.Disable 对已 disabled 是空操作）。
	code, body = postNoBody(t, srv.URL+"/api/extensions/tool.gate_probe/disable")
	if code != http.StatusOK || !strings.Contains(body, `"ok":true`) {
		t.Fatalf("重复 disable 状态码 = %d（body=%s），want 200（幂等）", code, body)
	}

	// enable（无能力声明 = 裁决不拦）→ 200，快照 enabled。
	code, body = postNoBody(t, srv.URL+"/api/extensions/tool.gate_probe/enable")
	if code != http.StatusOK || !strings.Contains(body, `"ok":true`) {
		t.Fatalf("enable 状态码 = %d（body=%s），want 200 {\"ok\":true}", code, body)
	}
	if got := extStateOf(t, srv.URL, "tool.gate_probe"); got != string(registry.StateEnabled) {
		t.Fatalf("enable 后 state = %q，want enabled", got)
	}
}

// TestExtensionEndpointsUnknownName404 钉未知名字 → 404 extension_not_found（两个端点）。
func TestExtensionEndpointsUnknownName404(t *testing.T) {
	h, err := New(Config{
		DBPath: filepath.Join(t.TempDir(), "events.db"),
		Model:  "scripted",
		CWD:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	for _, op := range []string{"enable", "disable"} {
		code, body := postNoBody(t, srv.URL+"/api/extensions/ghost_ext/"+op)
		if code != http.StatusNotFound {
			t.Fatalf("%s 未知名字状态码 = %d，want 404（body=%s）", op, code, body)
		}
		if !strings.Contains(body, "extension_not_found") {
			t.Fatalf("%s 未知名字错误码应为 extension_not_found（body=%s）", op, body)
		}
	}
}

// TestExtensionEnableDeniedByCapability403 钉 E3 fail-close 的 HTTP 面：申报了能力但
// 策略无 allow 命中（宿主 Allow 空 = 一切 deny）→ 403 extension_capability_denied，
// 错误文案点名缺的能力；状态保持 registered（未进入 enabled）。
func TestExtensionEnableDeniedByCapability403(t *testing.T) {
	h, err := New(Config{
		DBPath: filepath.Join(t.TempDir(), "events.db"),
		Model:  "scripted",
		CWD:    t.TempDir(),
		// Allow 空 = 无 allow 规则 = 能力裁决 fail-close deny（H8）。
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if err := h.reg.Register(contracts.ExtensionMeta{
		Name:         "tool.cap_probe",
		Kind:         "tool",
		Capabilities: []string{"fs:write"},
	}, "impl"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	code, body := postNoBody(t, srv.URL+"/api/extensions/tool.cap_probe/enable")
	if code != http.StatusForbidden {
		t.Fatalf("能力被拒的 enable 状态码 = %d，want 403（body=%s）", code, body)
	}
	if !strings.Contains(body, "extension_capability_denied") {
		t.Fatalf("错误码应为 extension_capability_denied（body=%s）", body)
	}
	if !strings.Contains(body, "fs:write") {
		t.Fatalf("错误文案应点名缺的能力 fs:write（body=%s）", body)
	}
	// fail-close：拒绝启用后仍是 registered（不进入 enabled）。
	if got := extStateOf(t, srv.URL, "tool.cap_probe"); got != string(registry.StateRegistered) {
		t.Fatalf("403 后 state = %q，want registered", got)
	}
}

// TestExtensionEnableStatusMapping 钉 Enable 错误 → (状态码, 错误码) 的映射表本身。
//
// ErrStale（409）要靠"裁决期间注销并重注册"的时序才真触发（registry 测试已钉语义、
// 构造时序需要假 Adjudicator）——host 面用合成错误钉这张表，端到端的 403/404 由上面
// 两个测试经真 handler 钉。
func TestExtensionEnableStatusMapping(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"未注册", fmt.Errorf("wrap: %w", registry.ErrNotFound), http.StatusNotFound, "extension_not_found"},
		{"能力未获准", fmt.Errorf("wrap: %w", registry.ErrNotAllowed), http.StatusForbidden, "extension_capability_denied"},
		{"裁决期间实例被替换", fmt.Errorf("wrap: %w", registry.ErrStale), http.StatusConflict, "extension_stale"},
		{"其余", errors.New("boom (injected)"), http.StatusInternalServerError, "extension_enable_failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, code := extensionEnableStatus(tc.err)
			if status != tc.status || code != tc.code {
				t.Fatalf("extensionEnableStatus(%v) = (%d, %q)，want (%d, %q)",
					tc.err, status, code, tc.status, tc.code)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 缺口 3 p3-3：扩展名**字节精确**匹配（与注册表同口径，端点不做 TrimSpace）
// ---------------------------------------------------------------------------

// TestExtensionEndpointsMatchNameExactly 钉 p3-3：路径里的名字**原样**拿去查注册表。
//
// 为什么：注册表的键就是原始字节（Register 只判"去空白后非空"，**不改写**名字），
// 端点若先 TrimSpace 再查，就会把 " x " 错配到 "x" 上——**停错扩展**（停掉一个还在
// 用的工具，或停不掉真正要停的那个）。这是"停用"这种动作最危险的错法。
func TestExtensionEndpointsMatchNameExactly(t *testing.T) {
	h, err := New(Config{
		DBPath: filepath.Join(t.TempDir(), "events.db"),
		Model:  "scripted",
		CWD:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	// 三个**不同**的名字（含空白）："x"、" x"、" x "。
	for _, name := range []string{"x", " x", " x "} {
		if err := h.reg.Register(contracts.ExtensionMeta{Name: name, Kind: "tool"}, "impl"); err != nil {
			t.Fatalf("Register(%q): %v", name, err)
		}
	}

	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// "%20x%20" → 名字 " x "：命中它自己。
	code, body := postNoBody(t, srv.URL+"/api/extensions/%20x%20/disable")
	if code != http.StatusOK {
		t.Fatalf("disable(%q) 状态码 = %d，want 200（body=%s）", " x ", code, body)
	}
	if got := extStateOf(t, srv.URL, " x "); got != string(registry.StateDisabled) {
		t.Fatalf("\" x \" 的 state = %q，want disabled", got)
	}
	// 关键：**不命中** "x"、也不命中 " x"（端点若 TrimSpace，这里就误伤了）。
	if got := extStateOf(t, srv.URL, "x"); got != string(registry.StateRegistered) {
		t.Fatalf("\"x\" 的 state = %q，want registered（端点不得 TrimSpace 后误伤 \"x\"）", got)
	}
	if got := extStateOf(t, srv.URL, " x"); got != string(registry.StateRegistered) {
		t.Fatalf("\" x\" 的 state = %q，want registered", got)
	}

	// "%20x" → 名字 " x"：命中它自己，**不命中** "x"。
	code, body = postNoBody(t, srv.URL+"/api/extensions/%20x/disable")
	if code != http.StatusOK {
		t.Fatalf("disable(%q) 状态码 = %d，want 200（body=%s）", " x", code, body)
	}
	if got := extStateOf(t, srv.URL, " x"); got != string(registry.StateDisabled) {
		t.Fatalf("\" x\" 的 state = %q，want disabled", got)
	}
	if got := extStateOf(t, srv.URL, "x"); got != string(registry.StateRegistered) {
		t.Fatalf("\"x\" 的 state = %q，want registered", got)
	}
}

// ---------------------------------------------------------------------------
// 缺口 3 p3-5：跨面接线钉子——HTTP 端点停用的**就是** loop 门控的那个注册表实例
// ---------------------------------------------------------------------------

// wireProbeExec 是本钉子用的最小执行器（只记调用次数——"不该被执行"是断言点）。
type wireProbeExec struct {
	mu    sync.Mutex
	calls int
}

func (e *wireProbeExec) Execute(_ context.Context, _ contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	text := "probe"
	return contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &text}}}, nil
}

func (e *wireProbeExec) callN() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

// wireProbeProvider 是本钉子用的最小 Provider：首轮发起一次工具调用，其后只回文本；
// 并记录每次请求（可见集断言用）。
type wireProbeProvider struct {
	mu       sync.Mutex
	tool     string
	calls    int
	requests []core.ModelRequest
}

func (p *wireProbeProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, req)
	p.calls++
	if p.calls == 1 {
		return core.ModelResult{
			Text: "调工具",
			ToolCalls: []core.ToolCallReq{{
				ID:   contracts.ProviderId("wire_probe_01"),
				Name: p.tool,
				Args: json.RawMessage(`{}`),
			}},
		}, nil
	}
	return core.ModelResult{Text: "done"}, nil
}

func (p *wireProbeProvider) reqs() []core.ModelRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]core.ModelRequest(nil), p.requests...)
}

// TestExtensionDisableEndpointGatesLoop 钉 p3-5（跨面接线）：POST /api/extensions/{name}/disable
// 之后，用**同一个注册表实例**起 loop 跑一次工具调用 → ① 可见集里没有该工具
// ② 直调落 tool_disabled、执行器零调用。
//
// 为什么这条重要：端点与 loop 门控若各用各的注册表实例（接线错），旋钮就是**假的**
// （HTTP 说停用了，loop 照跑）。故这条从 HTTP 端点出发，一路验到 loop 的执行侧。
//
// 口径说明：loop 用 `Registry: h.reg` 装配——与 hub 自己的 loop 装配（hub.go 的
// `Registry: h.reg`）**同一个实例**；h.Registry()（只读视图）也是这同一个实例。
func TestExtensionDisableEndpointGatesLoop(t *testing.T) {
	const toolName = "tool.wire_probe"
	h, err := New(Config{
		DBPath: filepath.Join(t.TempDir(), "events.db"),
		Model:  "scripted",
		CWD:    t.TempDir(),
		// 让它**本该**可见（否则看不见"被停用"的差别：看不见的东西停不停都一样）。
		Allow: []string{toolName},
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	exec := &wireProbeExec{}
	if err := h.reg.RegisterTool(contracts.Tool{
		Name:       toolName,
		SideEffect: contracts.SideEffectNone,
	}, exec); err != nil {
		t.Fatalf("RegisterTool: %v", err)
	}

	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// 停用**只经 HTTP 端点**（不经 registry.Disable 直调——那正是要验的接线）。
	code, body := postNoBody(t, srv.URL+"/api/extensions/"+toolName+"/disable")
	if code != http.StatusOK || !strings.Contains(body, `"ok":true`) {
		t.Fatalf("disable 状态码 = %d（body=%s），want 200 {\"ok\":true}", code, body)
	}

	// 端点改的就是 hub 装配给 loop 的那个注册表实例（只读视图 = 同一个实例）。
	entry, ok := h.Registry().Get(toolName)
	if !ok {
		t.Fatalf("注册表里没有 %q", toolName)
	}
	if entry.State != registry.StateDisabled {
		t.Fatalf("%q 的 state = %q，want disabled（HTTP 端点与 loop 门控必须同一个实例）",
			toolName, entry.State)
	}

	// 起一个 loop：**同一个注册表实例** + 一个假 Provider（首轮调该工具，其后收尾）。
	prov := &wireProbeProvider{tool: toolName}
	l, err := loop.New(loop.Config{
		Provider:  prov,
		Bus:       h.bus,
		Registry:  h.reg,
		Policy:    h.engine,
		SessionID: core.NewUlid(),
		AgentID:   core.NewUlid(),
		Model:     "test-model",
		System:    "test-system",
	})
	if err != nil {
		t.Fatalf("loop.New: %v", err)
	}
	if _, err := l.Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// ① 第一层门控：可见集里没有该工具。
	reqs := prov.reqs()
	if len(reqs) == 0 {
		t.Fatal("应有模型请求")
	}
	for _, tl := range reqs[0].Tools {
		if tl.Name == toolName {
			t.Fatalf("停用的工具不得进模型可见集: %+v", reqs[0].Tools)
		}
	}

	// ② 第二层门控：直调落 failed（code=tool_disabled），执行器零调用。
	evs, err := h.store.ReadFrom(context.Background(), nil, 1, 0)
	if err != nil {
		t.Fatalf("读事件: %v", err)
	}
	var failed *contracts.ToolCallFailedPayload
	for _, ev := range evs {
		if ev.Kind != loop.KindToolCallFailed {
			continue
		}
		var p contracts.ToolCallFailedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("解 failed payload: %v", err)
		}
		failed = &p
	}
	if failed == nil {
		t.Fatalf("事件流里没有 tool.call.failed（停用工具不该被执行）：kinds=%v", eventKinds(evs))
	}
	if failed.Error.Code == nil || *failed.Error.Code != loop.CodeToolDisabled {
		t.Fatalf("失败码 = %v，want %s", failed.Error.Code, loop.CodeToolDisabled)
	}
	if n := exec.callN(); n != 0 {
		t.Fatalf("停用工具的执行器被调用 %d 次（必须 0）", n)
	}
}

// eventKinds 把事件列表折成 kind 列表（失败时的诊断信息）。
func eventKinds(evs []contracts.Event) []string {
	out := make([]string, 0, len(evs))
	for _, ev := range evs {
		out = append(out, ev.Kind)
	}
	return out
}
