package host_test

// G-i **profile 机制**的协议面验收（设计 15 §6 的 G-i 行）：
//
//	① POST /prompt 带 profile → 展开成变更集 → 落 config.changed（**审计里看得见展开后
//	   的值**）+ 本轮生效；
//	② **叠放次序**：先展开 profile → 再叠显式 config（显式**逐字段压过** profile）；
//	③ 未知 profile 名 → 400 bad_profile（fail-close，**零事件**）；
//	④ 全空判定：只有 profile、展开出的变更集为空（profile 只有执行面）→ 不落事件；
//	⑤ **会话创建执行面**：POST /api/sessions 带 profile → profile 的 cwd / worktree /
//	   run_id 进 SessionSpec（自动 gate 归属可见）；显式 body 字段压过 profile；
//	⑥ WS 同源：command.submit 带 profile 与 HTTP 面同码同形。

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/transport"
)

// profileHub 起一个带 profile 表的宿主（"fast" = 一张命名模板）。
func profileHub(t *testing.T, prov core.Provider) *host.Hub {
	t.Helper()
	return newTestHub(t, func(c *host.Config) {
		c.Provider = prov
		c.Allow = []string{"read"}
		c.Profiles = map[string]host.ProfileConfig{
			"fast": {
				Name:            "fast",
				Model:           "m-profile",
				ReasoningEffort: "low",
				Tools:           host.ProfileToolsConfig{Only: []string{"read"}},
				Context: host.ProfileContextConfig{
					Kind: "tiered", Window: 512000, L1Pct: 8, L2Pct: 0.5,
				},
			},
			// 只有执行面（cwd/worktree/run_id）的 profile：展开出的变更集为空。
			"exec-only": {
				Name:     "exec-only",
				CWD:      "D:/work",
				RunID:    "run-9",
				Worktree: "wt-x",
			},
		}
	})
}

// TestPromptProfileExpandsAndOverlays：profile 展开 + 叠放次序 + 落事件审计。
func TestPromptProfileExpandsAndOverlays(t *testing.T) {
	prov := &configSpyProvider{}
	h := profileHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt":  "带 profile 的一轮",
		"profile": "fast",
		"config":  map[string]any{"model": "m-explicit"},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	evs := waitForSessionEvents(t, srv.URL, sid, "profile 轮的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})

	// 落一条 config.changed：载荷 = **展开后的值**（审计看得见）。
	cc := filterKind(evs, "config.changed")
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1（事件 kinds=%v）", len(cc), kindsOf(evs))
	}
	p := mustConfigPayload(t, cc[0])
	if p.Model == nil || *p.Model != "m-explicit" {
		t.Errorf("model 应是显式值（显式压过 profile）: %v", p.Model)
	}
	if p.ReasoningEffort == nil || *p.ReasoningEffort != "low" {
		t.Errorf("reasoning_effort 应取 profile 的（显式没碰这项）: %v", p.ReasoningEffort)
	}
	if p.Tools == nil || p.Tools.Only == nil || len(*p.Tools.Only) != 1 || (*p.Tools.Only)[0] != "read" {
		t.Errorf("tools 应取 profile 的: %+v", p.Tools)
	}
	if p.Context == nil || p.Context.Kind == nil || *p.Context.Kind != "tiered" ||
		p.Context.Window == nil || *p.Context.Window != 512000 {
		t.Errorf("context 应取 profile 的: %+v", p.Context)
	}
	// 本轮生效：provider 收到 model=m-explicit（显式压过 profile）。
	calls := prov.waitCalls(t, 1)
	if calls[0].Model != "m-explicit" {
		t.Errorf("本轮 model = %q，want m-explicit（叠放后生效）", calls[0].Model)
	}
}

// TestPromptProfileOnlyLandsEvent：只有 profile、没有显式 config → 展开出的变更集
// **照常落事件**（审计里看得见展开后的值——不是"profile 名 + 隐式模板"）。
func TestPromptProfileOnlyLandsEvent(t *testing.T) {
	prov := &configSpyProvider{}
	h := profileHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt":  "只有 profile",
		"profile": "fast",
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	evs := waitForSessionEvents(t, srv.URL, sid, "profile 轮的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	cc := filterKind(evs, "config.changed")
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1（只有 profile 也照常落事件）", len(cc))
	}
	p := mustConfigPayload(t, cc[0])
	if p.Model == nil || *p.Model != "m-profile" {
		t.Errorf("落事件的 model 应是 profile 的: %v", p.Model)
	}
	if p.Context == nil || p.Context.Window == nil || *p.Context.Window != 512000 {
		t.Errorf("落事件的 context 应是 profile 的: %+v", p.Context)
	}
	// 本轮生效：provider 收到 profile 的模型。
	calls := prov.waitCalls(t, 1)
	if calls[0].Model != "m-profile" {
		t.Errorf("本轮 model = %q，want m-profile（profile 展开后生效）", calls[0].Model)
	}
}

// TestPromptProfileUnknownRejected：未知 profile 名 → 400 bad_profile + 零事件。
func TestPromptProfileUnknownRejected(t *testing.T) {
	prov := &configSpyProvider{}
	h := profileHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	var errBody contracts.TransportError
	code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt":  "未知 profile",
		"profile": "nope",
	}, &errBody)
	if code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，want 400", code)
	}
	if errBody.Code != "bad_profile" {
		t.Fatalf("错误码 = %q，want bad_profile", errBody.Code)
	}
	if n := kindCount(sessionEvents(t, srv.URL, sid), "config.changed"); n != 0 {
		t.Fatalf("未知 profile 落了 %d 条 config.changed，want 0（fail-close，零事件）", n)
	}
}

// TestPromptProfileEmptyExpansionNoEvent：只有 profile、展开出的变更集为空（profile 只有
// 执行面）→ 照"全空变更集"口径**不落事件**（F8：落一条空 payload 的 durable 事件只是
// 往唯一事实源里灌噪声）。
func TestPromptProfileEmptyExpansionNoEvent(t *testing.T) {
	prov := &configSpyProvider{}
	h := profileHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt":  "只有执行面的 profile",
		"profile": "exec-only",
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	if n := kindCount(sessionEvents(t, srv.URL, sid), "config.changed"); n != 0 {
		t.Fatalf("空展开落了 %d 条 config.changed，want 0（全空变更集不落事件）", n)
	}
}

// TestSessionCreateProfileExecutionPlane：POST /api/sessions 带 profile → profile 的
// cwd / run_id 进 SessionSpec（**自动 gate 归属**可见）；显式 body 字段压过 profile。
func TestSessionCreateProfileExecutionPlane(t *testing.T) {
	prov := &configSpyProvider{}
	repo := tempRepoOrDir(t)
	h := newTestHub(t, func(c *host.Config) {
		c.Provider = prov
		c.Profiles = map[string]host.ProfileConfig{
			"wt": {Name: "wt", CWD: repo, RunID: "run-9"},
		}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// ① profile 的执行面生效：run_id → 自动 gate workflow:run-9。
	var created struct {
		SessionID string `json:"session_id"`
	}
	if code := postJSON(t, srv.URL+"/api/sessions", map[string]any{"profile": "wt"}, &created); code != http.StatusCreated {
		t.Fatalf("状态码 = %d，want 201", code)
	}
	var view host.GatesView
	if code := getJSON(t, srv.URL+"/api/gates?session_id="+created.SessionID, &view); code != http.StatusOK {
		t.Fatalf("GET /api/gates 状态码 = %d，want 200", code)
	}
	var joined []string
	for _, g := range view.Gates {
		if g.Joined {
			joined = append(joined, g.GateID)
		}
	}
	if !containsStr(joined, "workflow:run-9") {
		t.Errorf("profile 的 run_id 应进自动 gate（workflow:run-9），实际 gates=%v", joined)
	}

	// ② 显式 body 字段压过 profile（请求里显式写的更近）。
	var created2 struct {
		SessionID string `json:"session_id"`
	}
	if code := postJSON(t, srv.URL+"/api/sessions",
		map[string]any{"profile": "wt", "run_id": "run-explicit"}, &created2); code != http.StatusCreated {
		t.Fatalf("状态码 = %d，want 201", code)
	}
	var view2 host.GatesView
	if code := getJSON(t, srv.URL+"/api/gates?session_id="+created2.SessionID, &view2); code != http.StatusOK {
		t.Fatalf("GET /api/gates 状态码 = %d，want 200", code)
	}
	var joined2 []string
	for _, g := range view2.Gates {
		if g.Joined {
			joined2 = append(joined2, g.GateID)
		}
	}
	if containsStr(joined2, "workflow:run-9") {
		t.Errorf("显式 run_id 应压过 profile 的（不该再有 workflow:run-9），实际 gates=%v", joined2)
	}
	if !containsStr(joined2, "workflow:run-explicit") {
		t.Errorf("显式 run_id 应生效（workflow:run-explicit），实际 gates=%v", joined2)
	}

	// ③ 未知 profile 名 → 400 bad_profile。
	var errBody contracts.TransportError
	if code := postJSON(t, srv.URL+"/api/sessions", map[string]any{"profile": "nope"}, &errBody); code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，want 400", code)
	}
	if errBody.Code != "bad_profile" {
		t.Fatalf("错误码 = %q，want bad_profile", errBody.Code)
	}
}

// containsStr 报告列表里有没有这个串。
func containsStr(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// TestWSProfileSameSource：WS command.submit 带 profile —— 与 HTTP 面同源同形：
// 合法 → config.changed（展开后的值）+ 本轮生效；未知 profile → error 帧（bad_profile）。
func TestWSProfileSameSource(t *testing.T) {
	prov := &configSpyProvider{}
	h := profileHub(t, prov)
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

	// ① 合法：带 profile。
	name := "fast"
	msg, err := transport.NewMessage(transport.KindCommandSubmit, contracts.CommandSubmit{
		SessionID: &sid,
		Prompt:    "WS 带 profile",
		Profile:   &name,
	})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if err := c.SendContext(ctx, msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	waitWSKind(t, c, transport.KindCommandAccepted, 10*time.Second)
	if !waitEventKind(t, c, "model.turn", 10*time.Second) {
		t.Fatal("WS 带 profile 的轮次没等到 model.turn")
	}
	evs := sessionEvents(t, srv.URL, sid)
	cc := filterKind(evs, "config.changed")
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1", len(cc))
	}
	p := mustConfigPayload(t, cc[0])
	if p.Model == nil || *p.Model != "m-profile" {
		t.Errorf("WS 面 profile 应展开落事件（model=m-profile）: %v", p.Model)
	}
	if p.Context == nil || p.Context.Window == nil || *p.Context.Window != 512000 {
		t.Errorf("WS 面 profile 的 context 应展开落事件: %+v", p.Context)
	}

	// ② 未知 profile → error 帧（code=bad_profile，与 HTTP 400 同码）+ 零新增事件。
	bad := "nope"
	msg2, err := transport.NewMessage(transport.KindCommandSubmit, contracts.CommandSubmit{
		SessionID: &sid,
		Prompt:    "WS 坏 profile",
		Profile:   &bad,
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
	if te.Code != "bad_profile" {
		t.Fatalf("错误帧 code = %q，want bad_profile（与 HTTP 400 同码）", te.Code)
	}
	if n := kindCount(sessionEvents(t, srv.URL, sid), "config.changed"); n != 1 {
		t.Fatalf("未知 profile 不该落事件，config.changed = %d 条，want 1", n)
	}
}
