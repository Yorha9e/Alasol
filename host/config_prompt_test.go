package host_test

// G-c「per-prompt 配置体系」+ G-d「模型面字段」的 host 验收面（设计 15 §3，定稿）：
//
//	① POST /prompt 带 config.model → 202 + 事件流有 config.changed + 本轮 provider
//	   收到 model=m2 + model.turn.model=m2；
//	② 下一条 prompt 不带 config → 仍是 m2（持久）；带空串清除 → 回 hub 默认；
//	③ **重启生效**：同库重开 hub → fold 回放 → 仍是 m2（照 takeover_test 的 h1→h2 样板）；
//	④ **运行中变更**：跑轮中（provider 钩子里）落 config.changed → 本轮快照不受影响、
//	   下一个 prompt 生效（轮 = 一个 prompt 的 Run）；
//	⑤ reasoning_effort 非法 → 400 bad_config + **零事件**；合法 → 透传 ModelRequest；
//	⑥ provider 凭据纪律：base_url 无 api_key_env → 400；带 env → 走新端点 + 事件
//	   只含变量名不含 key 值；env 缺失 → 本轮 fail-close 且**不回退 hub key**；
//	⑦ WS command.submit 带 config → 与 HTTP 同源同型同码。
//
// 手法：脚本 provider（configSpyProvider）记录每次请求的 Model/ReasoningEffort——
// "本轮到底用了哪个模型/档位"直接断言，不靠猜。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/transport"
)

// configSpyCall 是一次模型调用的现场（断言本轮快照用哪个模型/档位）。
type configSpyCall struct {
	Model  string
	Effort string
}

// configSpyProvider 记录每次模型调用收到的 Model / ReasoningEffort。
//
// blockFirst（配合 entered/release 通道）让**第一次**调用阻塞在 provider 里——
// "运行中变更"用例据此把一个 Run 钉在半途，期间从另一个请求落 config.changed。
type configSpyProvider struct {
	mu         sync.Mutex
	calls      []configSpyCall
	blockFirst bool
	entered    chan struct{}
	release    chan struct{}
}

func (p *configSpyProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	call := configSpyCall{Model: req.Model}
	if req.ReasoningEffort != nil {
		call.Effort = *req.ReasoningEffort
	}
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
func (p *configSpyProvider) snapshot() []configSpyCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]configSpyCall, len(p.calls))
	copy(out, p.calls)
	return out
}

// waitCalls 轮询直到至少 n 次调用被记录（返回副本；超时 fail）。
func (p *configSpyProvider) waitCalls(t *testing.T, n int) []configSpyCall {
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

// sessionEvents 读该 session 的全部事件（GET /api/events?session_id=）。
func sessionEvents(t *testing.T, base string, sid contracts.Ulid) []contracts.Event {
	t.Helper()
	var view struct {
		Events []contracts.Event `json:"events"`
	}
	if code := getJSON(t, base+"/api/events?session_id="+string(sid)+"&limit=10000", &view); code != http.StatusOK {
		t.Fatalf("事件查询状态码 = %d", code)
	}
	return view.Events
}

// waitForSessionEvents 轮询直到 pred 为真（超时 fail；返回最后读到的事件集）。
func waitForSessionEvents(t *testing.T, base string, sid contracts.Ulid, what string, pred func([]contracts.Event) bool) []contracts.Event {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last []contracts.Event
	for time.Now().Before(deadline) {
		last = sessionEvents(t, base, sid)
		if pred(last) {
			return last
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("10s 内未等到：%s（最后 %d 条事件）", what, len(last))
	return last
}

// kindCount 数某 kind 的条数。
func kindCount(evs []contracts.Event, kind string) int {
	n := 0
	for _, ev := range evs {
		if ev.Kind == kind {
			n++
		}
	}
	return n
}

// filterKind 取某 kind 的事件（保持时序）。
func filterKind(evs []contracts.Event, kind string) []contracts.Event {
	out := []contracts.Event{}
	for _, ev := range evs {
		if ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

// mustConfigPayload 解一条 config.changed 的载荷（解不开 = fatal）。
func mustConfigPayload(t *testing.T, ev contracts.Event) contracts.ConfigChangedPayload {
	t.Helper()
	var p contracts.ConfigChangedPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("config.changed 载荷解不开: %v（raw=%s）", err, string(ev.Payload))
	}
	return p
}

// mustModelTurnModel 解一条 model.turn 的 model 字段（缺 = fatal：G-d 要求随覆盖走）。
func mustModelTurnModel(t *testing.T, ev contracts.Event) string {
	t.Helper()
	var p contracts.ModelTurnPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("model.turn 载荷解不开: %v", err)
	}
	if p.Model == nil || *p.Model == "" {
		t.Fatalf("model.turn 缺 model 字段: %s", string(ev.Payload))
	}
	return *p.Model
}

// postPrompt 发 POST /prompt（body = PromptSpec 的 JSON 形状），返回状态码与解出的
// 错误体（202 时 out 解受理回执；失败时 out 解 TransportError——用 any 由调用方定）。
func postPrompt(t *testing.T, url string, body map[string]any, out any) int {
	t.Helper()
	return postJSON(t, url, body, out)
}

// ---------------------------------------------------------------------------
// ① 覆盖本轮生效（model → ModelRequest + model.turn）
// ---------------------------------------------------------------------------

// TestPromptConfigModelOverrideAppliesThisRun：POST /prompt 带 {"config":{"model":"m2"}}
// → 202 + 事件流有 config.changed + 本轮 provider 收到 model=m2 + model.turn.model=m2。
func TestPromptConfigModelOverrideAppliesThisRun(t *testing.T) {
	prov := &configSpyProvider{}
	h := newTestHub(t, func(c *host.Config) { c.Provider = prov })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	var acc struct {
		SessionID string `json:"session_id"`
		Status    string `json:"status"`
	}
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "带模型覆盖的第一轮",
		"config": map[string]any{"model": "m2"},
	}, &acc); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	if acc.SessionID != string(sid) || acc.Status != "running" {
		t.Fatalf("受理回执不符: %+v", acc)
	}

	evs := waitForSessionEvents(t, srv.URL, sid, "config 轮的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})

	// 事件流里恰有一条 config.changed，载荷就是原样提交的变更集。
	cc := filterKind(evs, "config.changed")
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1（事件 kinds=%v)", len(cc), kindsOf(evs))
	}
	if p := mustConfigPayload(t, cc[0]); p.Model == nil || *p.Model != "m2" {
		t.Fatalf("config.changed 载荷 model = %v，want m2", p.Model)
	}
	// envelope 归属 = 该 session（session_id）。
	if cc[0].SessionID != sid {
		t.Fatalf("config.changed envelope.session_id = %s，want %s", cc[0].SessionID, sid)
	}
	if !cc[0].Durable {
		t.Fatal("config.changed 必须是 durable 事实事件")
	}

	// 本轮 provider 收到 model=m2（生效链：fold → 本轮快照 → ModelRequest）。
	calls := prov.waitCalls(t, 1)
	if calls[0].Model != "m2" {
		t.Fatalf("本轮 provider 收到 model = %q，want m2", calls[0].Model)
	}
	// model.turn.model 随之是新模型（G-d：契约零改动，该字段已有）。
	if got := mustModelTurnModel(t, filterKind(evs, "model.turn")[0]); got != "m2" {
		t.Fatalf("model.turn.model = %q，want m2", got)
	}
}

// kindsOf 列出事件 kind（诊断文案用）。
func kindsOf(evs []contracts.Event) []string {
	out := make([]string, 0, len(evs))
	for _, ev := range evs {
		out = append(out, ev.Kind)
	}
	return out
}

// ---------------------------------------------------------------------------
// ② 持久（不带 config 仍是新值）+ 空串清除（回 hub 默认）
// ---------------------------------------------------------------------------

// TestPromptConfigPersistsAndClears：
//
//	覆盖 m2 → 下一条不带 config 仍 m2（持久）→ 空串清除 → 回 hub 默认（scripted）。
func TestPromptConfigPersistsAndClears(t *testing.T) {
	prov := &configSpyProvider{}
	h := newTestHub(t, func(c *host.Config) { c.Provider = prov }) // Model 默认 "scripted"
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

	// ① 覆盖 m2。
	prompt(1, map[string]any{"prompt": "一", "config": map[string]any{"model": "m2"}})
	// ② 不带 config → 仍 m2（持久：fold 在，每个 Run 开头重取）。
	prompt(2, map[string]any{"prompt": "二"})
	// ③ 空串清除 → 回 hub 默认。
	prompt(3, map[string]any{"prompt": "三", "config": map[string]any{"model": ""}})

	calls := prov.waitCalls(t, 3)
	if calls[0].Model != "m2" {
		t.Fatalf("① 覆盖后本轮 model = %q，want m2", calls[0].Model)
	}
	if calls[1].Model != "m2" {
		t.Fatalf("② 不带 config 的下一轮仍应是 m2（持久），得到 %q", calls[1].Model)
	}
	if calls[2].Model != "scripted" {
		t.Fatalf("③ 空串清除后应回 hub 默认 scripted，得到 %q", calls[2].Model)
	}

	// config.changed 恰 2 条（覆盖 + 清除；"不带 config"那条不落事件）。
	evs := sessionEvents(t, srv.URL, sid)
	if n := kindCount(evs, "config.changed"); n != 2 {
		t.Fatalf("config.changed 条数 = %d，want 2", n)
	}
	// 清除那条载荷是显式空串（三态里"显式清除"可审计）。
	last := filterKind(evs, "config.changed")[1]
	if p := mustConfigPayload(t, last); p.Model == nil || *p.Model != "" {
		t.Fatalf("清除事件的 model 应为显式空串，得到 %v", p.Model)
	}
}

// ---------------------------------------------------------------------------
// ③ 重启生效（同库重开 → fold 回放）
// ---------------------------------------------------------------------------

// TestPromptConfigSurvivesHubReopen：h1 写入 m2 → 关掉 → 同一个库重开 h2 →
// 启动播种全量重放 config.changed → 新一轮 Run 仍是 m2（设计 §3.1"重启后仍生效"）。
func TestPromptConfigSurvivesHubReopen(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "events.db")
	newCfg := func(p core.Provider) host.Config {
		return host.Config{
			DBPath: db, Model: "scripted", System: "cfg-reopen", MaxTurns: 4,
			Allow: []string{"read"}, Provider: p, CWD: dir,
		}
	}

	// 第一个宿主：写入覆盖 + 跑一轮，然后关掉（照 takeover_test 的 h1→h2 样板）。
	prov1 := &configSpyProvider{}
	h1, err := host.New(newCfg(prov1))
	if err != nil {
		t.Fatalf("host.New #1: %v", err)
	}
	srv1 := httptest.NewServer(h1.Handler())
	sid := host.NewSession()
	if code := postPrompt(t, srv1.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "重启前", "config": map[string]any{"model": "m2"},
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

	// 同一个库重开：启动播种把 config.changed 折回来。
	prov2 := &configSpyProvider{}
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
	if calls[0].Model != "m2" {
		t.Fatalf("重启后 model = %q，want m2（fold 回放：per-prompt 配置重启仍生效）", calls[0].Model)
	}
}

// ---------------------------------------------------------------------------
// ④ 运行中变更：本轮快照不受影响、下一个 prompt 生效
// ---------------------------------------------------------------------------

// TestPromptConfigMidRunChangeAppliesNextRun：轮 A 的 provider 被钉住（快照已冻结）→
// 期间另一条带 config 的 prompt 落 config.changed（轮 B 生效 m2）→ 放开轮 A（仍是
// 旧模型，**运行中变更不打断/不改写本轮**）→ 下一个 prompt（轮 C）生效 m2。
//
// 这就是设计 §3.1"loop 运行中修改 → 当前轮结束后下一轮生效"（轮 = 一个 prompt 的 Run）。
func TestPromptConfigMidRunChangeAppliesNextRun(t *testing.T) {
	prov := &configSpyProvider{
		blockFirst: true,
		entered:    make(chan struct{}, 1),
		release:    make(chan struct{}),
	}
	h := newTestHub(t, func(c *host.Config) { c.Provider = prov })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	// 轮 A：无 config（hub 默认 scripted），进 provider 后被钉住。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]any{"prompt": "轮A"}, nil); code != http.StatusAccepted {
		t.Fatalf("轮 A 状态码 = %d，want 202", code)
	}
	select {
	case <-prov.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("10s 内轮 A 未进 provider")
	}
	// 快照已冻结：本轮拿到的是 hub 默认（provider 调用已记录）。
	if calls := prov.snapshot(); len(calls) != 1 || calls[0].Model != "scripted" {
		t.Fatalf("轮 A 进 provider 时应记录 scripted，得到 %+v", calls)
	}

	// 运行中：另一条带 config 的 prompt 落 config.changed → 轮 B 起跑。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]any{"prompt": "轮B", "config": map[string]any{"model": "m2"}}, nil); code != http.StatusAccepted {
		t.Fatalf("轮 B 状态码 = %d，want 202", code)
	}
	// 轮 B 完成（不被 blockFirst 挡）→ 它的快照是 m2。
	waitForSessionEvents(t, srv.URL, sid, "轮 B 的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	calls := prov.waitCalls(t, 2)
	if calls[1].Model != "m2" {
		t.Fatalf("轮 B（运行中落的配置）model = %q，want m2", calls[1].Model)
	}
	// 轮 A 仍钉在 provider 里——它的快照**不受影响**（仍是 scripted）。
	if got := prov.snapshot()[0].Model; got != "scripted" {
		t.Fatalf("轮 A 快照被运行中变更改写了：%q（应保持 scripted）", got)
	}

	// 放开轮 A → 收场（model.turn 落下；快照值不变）。
	close(prov.release)
	waitForSessionEvents(t, srv.URL, sid, "轮 A 的 model.turn（放开后）", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 2
	})
	if got := prov.snapshot()[0].Model; got != "scripted" {
		t.Fatalf("轮 A 收场后 model = %q，want scripted（运行中变更不影响本轮）", got)
	}

	// 下一个 prompt（轮 C，不带 config）→ 生效 m2（持久）。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]any{"prompt": "轮C"}, nil); code != http.StatusAccepted {
		t.Fatalf("轮 C 状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "轮 C 的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 3
	})
	calls = prov.waitCalls(t, 3)
	if calls[2].Model != "m2" {
		t.Fatalf("轮 C model = %q，want m2（下一个 prompt 的 Run 生效）", calls[2].Model)
	}
}

// ---------------------------------------------------------------------------
// ⑤ reasoning_effort：非法 400 + 零事件；合法透传
// ---------------------------------------------------------------------------

// TestPromptConfigReasoningEffortValidation：
//
//	非法档位 → 400 bad_config + **零 config.changed / 零 model.turn**（写时 fail-close，
//	校验在落事件之前）；合法档位 → 202 + 透传到本轮 ModelRequest.ReasoningEffort。
func TestPromptConfigReasoningEffortValidation(t *testing.T) {
	prov := &configSpyProvider{}
	h := newTestHub(t, func(c *host.Config) { c.Provider = prov })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	// ① 非法档位。
	var te contracts.TransportError
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "坏档位", "config": map[string]any{"reasoning_effort": "ultra"},
	}, &te); code != http.StatusBadRequest {
		t.Fatalf("非法 effort 状态码 = %d，want 400", code)
	}
	if te.Code != "bad_config" {
		t.Fatalf("错误码 = %q，want bad_config", te.Message)
	}
	if !strings.Contains(te.Message, "reasoning_effort") {
		t.Fatalf("错误文案应点名 reasoning_effort，得到 %q", te.Message)
	}
	// 零事件：config.changed 没落、也没起跑（model.turn 没有）。gate.joined 是
	// ensureHosted 的自动 gate（与配置无关），不在此断言面里。
	evs := sessionEvents(t, srv.URL, sid)
	if n := kindCount(evs, "config.changed"); n != 0 {
		t.Fatalf("非法配置不该落任何 config.changed，得到 %d 条", n)
	}
	if n := kindCount(evs, "model.turn"); n != 0 {
		t.Fatalf("非法配置不该起跑，model.turn = %d 条", n)
	}

	// ② 合法档位 → 透传到 ModelRequest。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "好档位", "config": map[string]any{"reasoning_effort": "high"},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("合法 effort 状态码 = %d，want 202", code)
	}
	evs = waitForSessionEvents(t, srv.URL, sid, "合法 effort 轮的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	calls := prov.waitCalls(t, 1)
	if calls[0].Effort != "high" {
		t.Fatalf("本轮 ModelRequest.ReasoningEffort = %q，want high", calls[0].Effort)
	}
	cc := filterKind(evs, "config.changed")
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1", len(cc))
	}
	if p := mustConfigPayload(t, cc[0]); p.ReasoningEffort == nil || *p.ReasoningEffort != "high" {
		t.Fatalf("config.changed 载荷 reasoning_effort = %v，want high", p.ReasoningEffort)
	}
}

// ---------------------------------------------------------------------------
// ⑥ provider 凭据纪律（gap2/G-d）
// ---------------------------------------------------------------------------

// TestPromptConfigProviderDiscipline：三个子用例（各自独立 session，fold 互不串）。
//
//	(a) base_url 非空无 api_key_env → 400 bad_config（文案点名 gap2）+ 零事件；
//	(b) 带 api_key_env → 本轮走新端点（真实 openai provider 打到 httptest），
//	    Authorization 用 **env 里的 key**；事件流只含变量名、绝不含 key 值（H15/H12）；
//	(c) env 缺失 → 本轮 fail-close 报错，**绝不回退 hub 的 key**（stub provider 零调用、
//	    新端点零请求）。
func TestPromptConfigProviderDiscipline(t *testing.T) {
	prov := &configSpyProvider{}
	h := newTestHub(t, func(c *host.Config) { c.Provider = prov })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// 新端点（真 httptest：断言"真的打过去了"，不是只看构造参数）。
	const sentinelKey = "sk-SENTINEL-CFGKEY-9x7"
	var (
		hitCount atomic.Int32
		hitMu    sync.Mutex
		gotAuth  string
		gotPath  string
		gotModel string
	)
	ep := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitCount.Add(1)
		var body struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		hitMu.Lock()
		gotAuth, gotPath, gotModel = r.Header.Get("Authorization"), r.URL.Path, body.Model
		hitMu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer ep.Close()

	// (a) base_url 非空、无 api_key_env → 400。
	sidA := host.NewSession()
	var te contracts.TransportError
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sidA)+"/prompt", map[string]any{
		"prompt": "缺 key env", "config": map[string]any{
			"provider": map[string]any{"base_url": ep.URL + "/v1"},
		},
	}, &te); code != http.StatusBadRequest {
		t.Fatalf("(a) 状态码 = %d，want 400", code)
	}
	if te.Code != "bad_config" {
		t.Fatalf("(a) 错误码 = %q，want bad_config", te.Code)
	}
	if !strings.Contains(te.Message, "gap2") || !strings.Contains(te.Message, "api_key_env") {
		t.Fatalf("(a) 文案应点名 gap2 纪律与 api_key_env，得到 %q", te.Message)
	}
	if evs := sessionEvents(t, srv.URL, sidA); kindCount(evs, "config.changed") != 0 {
		t.Fatalf("(a) 非法配置零事件，却有 %d 条 config.changed", kindCount(evs, "config.changed"))
	}

	// (b) 带 api_key_env（env 有值）→ 走新端点。
	t.Setenv("ALASOL_TEST_CFG_KEY", sentinelKey)
	sidB := host.NewSession()
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sidB)+"/prompt", map[string]any{
		"prompt": "换端点", "config": map[string]any{
			"model": "m2",
			"provider": map[string]any{
				"base_url":    ep.URL + "/v1",
				"api_key_env": "ALASOL_TEST_CFG_KEY",
			},
		},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("(b) 状态码 = %d，want 202", code)
	}
	evsB := waitForSessionEvents(t, srv.URL, sidB, "(b) 的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	if n := hitCount.Load(); n != 1 {
		t.Fatalf("(b) 新端点请求数 = %d，want 1（本轮应打到覆盖端点）", n)
	}
	hitMu.Lock()
	auth, path, model := gotAuth, gotPath, gotModel
	hitMu.Unlock()
	if path != "/v1/chat/completions" {
		t.Fatalf("(b) 端点路径 = %q，want /v1/chat/completions", path)
	}
	if auth != "Bearer "+sentinelKey {
		t.Fatalf("(b) Authorization 未用 env 里的 key（得到 %q；key 只从环境变量取）", auth)
	}
	if model != "m2" {
		t.Fatalf("(b) 端点收到 model = %q，want m2", model)
	}
	// hub 的 stub provider 一次都没被调（本轮走的是覆盖 provider）。
	if n := len(prov.snapshot()); n != 0 {
		t.Fatalf("(b) hub provider 不该被调用，得到 %d 次", n)
	}
	if got := mustModelTurnModel(t, filterKind(evsB, "model.turn")[0]); got != "m2" {
		t.Fatalf("(b) model.turn.model = %q，want m2", got)
	}
	// 事件里只有变量名、绝无 key 值（H15/H12）。
	for _, ev := range evsB {
		raw := string(ev.Payload)
		if strings.Contains(raw, sentinelKey) {
			t.Fatalf("(b) 事件 %s 泄漏了 key 值: %s", ev.Kind, raw)
		}
	}
	ccB := filterKind(evsB, "config.changed")
	if len(ccB) != 1 {
		t.Fatalf("(b) config.changed 条数 = %d，want 1", len(ccB))
	}
	if raw := string(ccB[0].Payload); !strings.Contains(raw, "ALASOL_TEST_CFG_KEY") ||
		strings.Contains(raw, sentinelKey) {
		t.Fatalf("(b) config.changed 载荷应只含变量名不含 key 值，得到 %s", raw)
	}

	// (c) env 缺失 → 本轮 fail-close，不回退 hub key。
	sidC := host.NewSession()
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sidC)+"/prompt", map[string]any{
		"prompt": "缺 env", "config": map[string]any{
			"model": "m2",
			"provider": map[string]any{
				"base_url":    ep.URL + "/v1",
				"api_key_env": "ALASOL_TEST_CFG_KEY_NEVER_SET_XX",
			},
		},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("(c) 写路径状态码 = %d，want 202（写时只校验形状，env 缺失是运行期 fail-close）", code)
	}
	// 写路径是同步的：202 已回 → config.changed 已 durable。直接 RunPrompt 让错误浮出。
	_, err := h.RunPrompt(context.Background(), sidC, "再试")
	if err == nil {
		t.Fatal("(c) env 缺失时本轮应 fail-close 报错")
	}
	if !strings.Contains(err.Error(), "ALASOL_TEST_CFG_KEY_NEVER_SET_XX") {
		t.Fatalf("(c) 错误应点名缺失的环境变量，得到 %q", err.Error())
	}
	if !strings.Contains(err.Error(), "fail-close") {
		t.Fatalf("(c) 错误应点名 fail-close 口径，得到 %q", err.Error())
	}
	// 不回退 hub key：stub provider 零调用、新端点仍只有 (b) 那一次。
	if n := len(prov.snapshot()); n != 0 {
		t.Fatalf("(c) 绝不回退 hub provider：stub 被调了 %d 次", n)
	}
	if n := hitCount.Load(); n != 1 {
		t.Fatalf("(c) 覆盖端点请求数 = %d，want 1（env 缺失时不该发出任何请求）", n)
	}
	if evs := sessionEvents(t, srv.URL, sidC); kindCount(evs, "model.turn") != 0 {
		t.Fatalf("(c) fail-close 的轮次不该有 model.turn，得到 %d 条", kindCount(evs, "model.turn"))
	}
}

// ---------------------------------------------------------------------------
// ⑦ WS command.submit 同源
// ---------------------------------------------------------------------------

// TestWSSubmitConfigSameSource：WS command.submit 带 config 与 HTTP 面同源同型——
//
//	合法 → command.accepted + config.changed + 本轮 model=m2；
//	非法 → kind=error 帧（code=bad_config，与 HTTP 400 同码）+ 零新增 config.changed。
func TestWSSubmitConfigSameSource(t *testing.T) {
	prov := &configSpyProvider{}
	h := newTestHub(t, func(c *host.Config) { c.Provider = prov })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := warmupSession(t, h) // 已托管（与 takeover 的 WS 用例同款夹具）

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

	// ① 合法：带 config → command.accepted + 本轮 m2。
	msg, err := transport.NewMessage(transport.KindCommandSubmit, contracts.CommandSubmit{
		SessionID: &sid,
		Prompt:    "WS 带配置",
		Config:    &contracts.ConfigChangedPayload{Model: strPtrForTest("m2")},
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
		t.Fatal("WS 带配置的轮次没等到 model.turn")
	}
	// model.turn 落下时 provider 已返回 → 调用已记录：warmup 占第 1 次，带配置的是第 2 次。
	calls := prov.waitCalls(t, 2)
	if calls[1].Model != "m2" {
		t.Fatalf("WS 带配置的轮次 model = %q，want m2（调用序列 %+v）", calls[1].Model, calls)
	}
	// 事件流里有那条 config.changed。
	evs := sessionEvents(t, srv.URL, sid)
	cc := filterKind(evs, "config.changed")
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1", len(cc))
	}
	if p := mustConfigPayload(t, cc[0]); p.Model == nil || *p.Model != "m2" {
		t.Fatalf("config.changed 载荷 model = %v，want m2", p.Model)
	}

	// ② 非法：坏档位 → kind=error 帧（code 与 HTTP 400 同码）。
	msg2, err := transport.NewMessage(transport.KindCommandSubmit, contracts.CommandSubmit{
		SessionID: &sid,
		Prompt:    "WS 坏配置",
		Config:    &contracts.ConfigChangedPayload{ReasoningEffort: strPtrForTest("ultra")},
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
	// 零新增 config.changed（仍是 ① 的那一条）。
	if evs := sessionEvents(t, srv.URL, sid); kindCount(evs, "config.changed") != 1 {
		t.Fatalf("非法配置不该落事件，config.changed = %d 条，want 1", kindCount(evs, "config.changed"))
	}
}

// ---------------------------------------------------------------------------
// F2 / F8 钉子
// ---------------------------------------------------------------------------

// TestPromptConfigIdenticalResubmitLandsSecondEvent（F2 钉现状）：同一 payload 连发
// 两次 → **2 条** config.changed——客户端在首次已落库**之后**重试不被去重（写前
// catchUp 让届数前进 → 新 id；幂等去重只覆盖同届同 payload 的并发/存储层重试）。
// fold 结果不变（同 payload LWW 同值）：后续不带 config 的一轮仍是 m2。
func TestPromptConfigIdenticalResubmitLandsSecondEvent(t *testing.T) {
	prov := &configSpyProvider{}
	h := newTestHub(t, func(c *host.Config) { c.Provider = prov })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	post := func(what string) {
		t.Helper()
		if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
			"prompt": what, "config": map[string]any{"model": "m2"},
		}, nil); code != http.StatusAccepted {
			t.Fatalf("%s 状态码 = %d，want 202", what, code)
		}
	}
	post("同 payload 一")
	waitForSessionEvents(t, srv.URL, sid, "第一次的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	post("同 payload 二") // 与第一次**逐字节相同**的变更集
	waitForSessionEvents(t, srv.URL, sid, "第二次的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 2
	})

	evs := sessionEvents(t, srv.URL, sid)
	cc := filterKind(evs, "config.changed")
	if len(cc) != 2 {
		t.Fatalf("同一 payload 第二次提交应落成第二条等价事件（F2 现状：届数前进 → 新 id，"+
			"客户端重试不去重），config.changed = %d 条，want 2", len(cc))
	}
	if cc[0].ID == cc[1].ID {
		t.Fatalf("两条事件 id 不该相同（同 id 会被存储层去重吃掉第二条）: %s", cc[0].ID)
	}
	p1, p2 := mustConfigPayload(t, cc[0]), mustConfigPayload(t, cc[1])
	if p1.Model == nil || *p1.Model != "m2" || p2.Model == nil || *p2.Model != "m2" {
		t.Fatalf("两条载荷都应是 model=m2: %+v / %+v", p1.Model, p2.Model)
	}
	// fold 结果不变（同 payload LWW 同值）：下一条不带 config 的 prompt 仍是 m2。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]any{"prompt": "验证生效值"}, nil); code != http.StatusAccepted {
		t.Fatalf("第三条 prompt 状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "第三条的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 3
	})
	calls := prov.waitCalls(t, 3)
	if calls[2].Model != "m2" {
		t.Fatalf("重复提交后生效值应仍是 m2（LWW 同值），得到 %q", calls[2].Model)
	}
}

// TestPromptConfigEmptyChangeSetNoEvent（F8）：`{"config":{}}`（model / reasoning_effort
// / provider 全缺失）= **不修改配置** → 不落事件、照常受理（202）并正常跑完
// （"缺失 = 不变"语义的空操作，不往唯一事实源灌 payload `{}` 的噪声）。
func TestPromptConfigEmptyChangeSetNoEvent(t *testing.T) {
	prov := &configSpyProvider{}
	h := newTestHub(t, func(c *host.Config) { c.Provider = prov })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "空变更集", "config": map[string]any{},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("空变更集状态码 = %d，want 202（无操作照常受理）", code)
	}
	evs := waitForSessionEvents(t, srv.URL, sid, "照常跑完的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	if n := kindCount(evs, "config.changed"); n != 0 {
		t.Fatalf("空变更集不该落任何 config.changed，得到 %d 条", n)
	}
	// 照常用 hub 默认模型跑（没有覆盖被应用）。
	calls := prov.waitCalls(t, 1)
	if calls[0].Model != "scripted" {
		t.Fatalf("空变更集不该改模型，model = %q，want scripted", calls[0].Model)
	}
}
