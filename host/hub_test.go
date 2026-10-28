package host_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/transport"
)

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

// scriptedProvider 是脚本 provider：第一轮发起一次 read 工具调用，第二轮收尾。
// 第三轮起（若还要）只回文本，保证 MaxTurns 内收敛。
type scriptedProvider struct {
	calls int
	// args 是第一次工具调用的参数（调用方可注入绝对路径）。
	args string
}

func (s *scriptedProvider) Run(ctx context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	s.calls++
	if s.calls == 1 {
		tc := core.ToolCallReq{
			ID:   contracts.ProviderId("toolu_SCRIPTED_01"),
			Name: "read",
			Args: json.RawMessage(s.args),
		}
		emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 10, OutputTokens: 5}}, nil
	}
	emit(core.ProviderEvent{Kind: "text.delta", Text: "读完了"})
	emit(core.ProviderEvent{Kind: "done"})
	return core.ModelResult{Text: "读完了", Usage: core.Usage{InputTokens: 20, OutputTokens: 3}}, nil
}

// newTestHub 装配一个 Hub：临时事件库 + 脚本 provider + 放行 read 工具。
func newTestHub(t *testing.T, muts ...func(*host.Config)) *host.Hub {
	t.Helper()
	cfg := host.Config{
		DBPath:   filepath.Join(t.TempDir(), "events.db"),
		Model:    "scripted",
		System:   "test-system",
		MaxTurns: 4,
		Allow:    []string{"read"},
	}
	for _, mut := range muts {
		if mut != nil {
			mut(&cfg)
		}
	}
	h, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// withProvider 把脚本 provider 注入 Config（args 是首轮工具调用的参数）。
func withProvider(args string) func(*host.Config) {
	return func(c *host.Config) {
		c.Provider = &scriptedProvider{args: args}
	}
}

// getJSON 发一个 GET 并解 JSON（失败即中止）。
func getJSON(t *testing.T, url string, out any) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("解 %s 的 JSON: %v", url, err)
		}
	}
	return resp.StatusCode
}

// runOneRound 跑一轮真回路（真 store + 真 bus + 真 read 工具），返回 session id。
func runOneRound(t *testing.T, h *host.Hub, prompt string) contracts.Ulid {
	t.Helper()
	sid := host.NewSession()
	res, err := h.RunPrompt(context.Background(), sid, prompt)
	if err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if res.Turns == 0 || res.Text == "" {
		t.Fatalf("Result 不符: %+v", res)
	}
	// 两轮模型调用 → 两条 context.assembled + intent + completed。
	if res.Events != 4 {
		t.Fatalf("Events = %d，want 4（两轮各一条 context.assembled + intent + completed）", res.Events)
	}
	return sid
}

// ---------------------------------------------------------------------------
// API 端点
// ---------------------------------------------------------------------------

// TestAPISessionsContextHealth：三个端点的形状与内容。
func TestAPISessionsContextHealth(t *testing.T) {
	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("alasol host api"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	h := newTestHub(t, withProvider(`{"path":"`+filepath.ToSlash(target)+`"}`))
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// 跑之前：在线表为空，上下文 404。
	var rows []host.SessionRow
	if code := getJSON(t, srv.URL+"/api/sessions", &rows); code != http.StatusOK {
		t.Fatalf("sessions 状态码 = %d", code)
	}
	if len(rows) != 0 {
		t.Fatalf("初始在线表应空，得到 %d 行", len(rows))
	}
	var te contracts.TransportError
	if code := getJSON(t, srv.URL+"/api/context/"+string(host.NewSession()), &te); code != http.StatusNotFound {
		t.Fatalf("未知 session 的 context 状态码 = %d，want 404", code)
	}

	sid := runOneRound(t, h, "读一下目标文件")

	// 在线表：一行，七条事件，最后 kind 是收尾那一轮的 model.turn
	// （v1-S1 起对话入流：user.prompt + 两轮各 context.assembled/model.turn + intent + completed = 7）。
	rows = nil
	if code := getJSON(t, srv.URL+"/api/sessions", &rows); code != http.StatusOK {
		t.Fatalf("sessions 状态码 = %d", code)
	}
	if len(rows) != 1 {
		t.Fatalf("在线表应有 1 行，得到 %d", len(rows))
	}
	row := rows[0]
	if row.SessionID != string(sid) {
		t.Fatalf("session_id = %q，want %q", row.SessionID, sid)
	}
	if row.EventCount != 7 || row.FirstSeq != 1 || row.LastSeq != 7 {
		t.Fatalf("seq/计数不符: %+v", row)
	}
	if row.LastKind != "model.turn" {
		t.Fatalf("last_kind = %q，want model.turn", row.LastKind)
	}
	if row.UpdatedAt == "" || row.Watermark <= 0 {
		t.Fatalf("updated_at / watermark 缺失: %+v", row)
	}

	// 上下文快照：parts 非空、turn 递增到 2、token 口径自洽。
	var snap contracts.ContextSnapshot
	if code := getJSON(t, srv.URL+"/api/context/"+string(sid), &snap); code != http.StatusOK {
		t.Fatalf("context 状态码 = %d", code)
	}
	if snap.SessionID != sid {
		t.Fatalf("快照 session_id = %q，want %q", snap.SessionID, sid)
	}
	if snap.Turn != 2 {
		t.Fatalf("快照 turn = %d，want 2（最后一轮）", snap.Turn)
	}
	if len(snap.Parts) == 0 {
		t.Fatal("快照没有 parts")
	}
	var sum int64
	for _, p := range snap.Parts {
		sum += p.Tokens
		if p.State != contracts.PartStateVerbatim {
			t.Fatalf("v0 无压缩，state = %q", p.State)
		}
	}
	if snap.TokensUsed != sum {
		t.Fatalf("tokens_used = %d，want parts 之和 %d", snap.TokensUsed, sum)
	}
	if snap.TokensBudget <= 0 {
		t.Fatalf("tokens_budget = %d，应为正", snap.TokensBudget)
	}

	// health：ok + 水位。
	var health host.Health
	if code := getJSON(t, srv.URL+"/api/health", &health); code != http.StatusOK {
		t.Fatalf("health 状态码 = %d", code)
	}
	if !health.OK || health.Watermark <= 0 || health.Sessions != 1 {
		t.Fatalf("health 不符: %+v", health)
	}
}

// TestAPIBadSessionID：非法 ULID → 400（S9：不合则拒绝，不修复）。
func TestAPIBadSessionID(t *testing.T) {
	h := newTestHub(t, nil)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	var te contracts.TransportError
	if code := getJSON(t, srv.URL+"/api/context/not-a-ulid", &te); code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，want 400", code)
	}
	if te.Code != "bad_session_id" {
		t.Fatalf("错误码 = %q", te.Code)
	}
}

// TestIndexServesEmbeddedHTMLWithThreePanels：GET / 返回嵌入的 Web UI，
// 且含三块面板的 id（在线表 / 上下文构成 / 事件流）。
func TestIndexServesEmbeddedHTMLWithThreePanels(t *testing.T) {
	h := newTestHub(t, nil)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态码 = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("Content-Type = %q，want text/html", ct)
	}
	body := make([]byte, 0, 64<<10)
	buf := make([]byte, 32<<10)
	for {
		n, err := resp.Body.Read(buf)
		body = append(body, buf[:n]...)
		if err != nil {
			break
		}
	}
	html := string(body)
	for _, id := range []string{"panel-sessions", "panel-context", "panel-events"} {
		if !strings.Contains(html, `id="`+id+`"`) {
			t.Fatalf("index.html 缺面板 id=%s", id)
		}
	}
	// 零依赖：不引 CDN / 外部脚本（离线可用）。
	for _, bad := range []string{"http://cdn", "https://cdn", "unpkg", "jsdelivr"} {
		if strings.Contains(html, bad) {
			t.Fatalf("index.html 不该引外部资源: %s", bad)
		}
	}
}

// TestAskRuleRoutesToApprover：-ask 命中的工具会去问 Approver（D14：审批回路）；
// Approver 放行才执行，缺席/拒绝则 deny（fail-close）。
func TestAskRuleRoutesToApprover(t *testing.T) {
	target := filepath.Join(t.TempDir(), "ask-target.txt")
	if err := os.WriteFile(target, []byte("ask"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}

	// 放行型审批人：记录被问到的工具名，一律同意。
	ap := &recordingApprover{allow: true}
	h := newTestHub(t,
		withProvider(`{"path":"`+filepath.ToSlash(target)+`"}`),
		func(c *host.Config) {
			c.Allow = nil
			c.Ask = []string{"read"}
			c.Approver = ap
		},
	)

	sid := host.NewSession()
	res, err := h.RunPrompt(context.Background(), sid, "读一下")
	if err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if res.ToolCalls != 1 {
		t.Fatalf("ToolCalls = %d，want 1", res.ToolCalls)
	}
	if len(ap.seen) != 1 || !strings.Contains(ap.seen[0], "read") {
		t.Fatalf("Approver 应被问一次 read，得到 %v", ap.seen)
	}
	// ask 被批准 → 工具真的执行了（completed 事件）。
	snap, ok, err := h.ContextSnapshot(context.Background(), sid)
	if err != nil || !ok {
		t.Fatalf("快照缺失: ok=%v err=%v", ok, err)
	}
	var parsed contracts.ContextSnapshot
	if err := json.Unmarshal(snap, &parsed); err != nil {
		t.Fatalf("快照解不开: %v", err)
	}
	if parsed.SessionID != sid {
		t.Fatalf("快照 session = %q", parsed.SessionID)
	}

	// 拒绝型审批人：同一配置下换成 DenyAll → 不执行（fail-close）。
	h2 := newTestHub(t,
		withProvider(`{"path":"`+filepath.ToSlash(target)+`"}`),
		func(c *host.Config) {
			c.Allow = nil
			c.Ask = []string{"read"}
			c.Approver = core.DenyAll{}
		},
	)
	res2, err := h2.RunPrompt(context.Background(), host.NewSession(), "读一下")
	if err != nil {
		t.Fatalf("RunPrompt #2: %v", err)
	}
	if res2.ToolCalls != 1 || res2.Text != "读完了" {
		t.Fatalf("Result 不符: %+v", res2)
	}
}

// recordingApprover 记录被问到的审批请求（按 Summary 认工具），并按 allow 决定放行/拒绝。
type recordingApprover struct {
	allow bool
	seen  []string
}

func (a *recordingApprover) Approve(_ context.Context, req contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	a.seen = append(a.seen, req.Summary)
	if a.allow {
		return contracts.ApprovalResponse{
			V: 1, RequestID: req.ID, TS: req.TS,
			Decision:  contracts.ApprovalDecisionAllow,
			DecidedBy: strPtrForTest("test:recording-allow"),
		}, nil
	}
	return contracts.ApprovalResponse{
		V: 1, RequestID: req.ID, TS: req.TS,
		Decision:  contracts.ApprovalDecisionDeny,
		Note:      strPtrForTest("test:recording-deny"),
		DecidedBy: strPtrForTest("test:recording-deny"),
	}, nil
}

func strPtrForTest(s string) *string { return &s }

var _ core.Approver = (*recordingApprover)(nil)

// TestUnknownStaticPath404：其它静态路径明确 404。
func TestUnknownStaticPath404(t *testing.T) {
	h := newTestHub(t, nil)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/app.js")
	if err != nil {
		t.Fatalf("GET /app.js: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("状态码 = %d，want 404", resp.StatusCode)
	}
}

// TestMaxSessionRowsLimit：服务端就设行数上限（K15），不只靠前端截断。
func TestMaxSessionRowsLimit(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) {
		c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
		c.MaxSessionRows = 2
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	for i := 0; i < 5; i++ {
		sid := host.NewSession()
		if _, err := h.RunPrompt(context.Background(), sid, "读一下"); err != nil {
			t.Fatalf("RunPrompt %d: %v", i, err)
		}
	}
	var rows []host.SessionRow
	if code := getJSON(t, srv.URL+"/api/sessions", &rows); code != http.StatusOK {
		t.Fatalf("sessions 状态码 = %d", code)
	}
	if len(rows) != 2 {
		t.Fatalf("行数 = %d，want 2（MaxSessionRows 上限）", len(rows))
	}
	// 倒序：最新活跃的在前。
	if rows[0].LastSeq < rows[1].LastSeq {
		t.Fatalf("在线表应按最后活跃倒序: %+v", rows)
	}
}

// ---------------------------------------------------------------------------
// 安全：loopback
// ---------------------------------------------------------------------------

// TestLoopbackOnlyBinding：非 loopback 地址一律拒绝（N5b fail-close）。
func TestLoopbackOnlyBinding(t *testing.T) {
	cases := []struct {
		addr    string
		ok      bool
		wantMsg string // 拒绝原因里必须出现的片段（空 = 不检查）
	}{
		{"", true, ""},                                     // 空 = 默认 127.0.0.1:8720
		{"127.0.0.1:9999", true, ""},                       // 显式 loopback
		{"127.0.0.5:9999", true, ""},                       // 127/8 整段
		{"localhost:9999", true, ""},                       // 本地主机名
		{"LocalHost:9999", true, ""},                       // 名字特例大小写不敏感
		{"ip6-localhost:9999", true, ""},                   // 名字特例
		{"[::1]:9999", true, ""},                           // IPv6 loopback
		{"0.0.0.0:8720", false, "loopback"},                // 全网卡 = 暴露
		{"[::]:8720", false, "loopback"},                   // IPv6 全网卡 = 暴露
		{"192.168.1.5:8720", false, "loopback"},            // 内网地址
		{"example.com:8720", false, "loopback"},            // 远端主机
		{"127.0.0.1.evil.example:8720", false, "loopback"}, // 域名伪装成 loopback（P1）
		{"127.0.0.1.nip.io:8720", false, "loopback"},       // 同上（解析到 127.0.0.1）
		{"::", false, ""},                                  // 缺端口，直接拒
	}
	for _, tc := range cases {
		cfg := host.Config{
			DBPath: filepath.Join(t.TempDir(), "events.db"),
			Model:  "m",
			Addr:   tc.addr,
		}
		h, err := host.New(cfg)
		if tc.ok && err != nil {
			t.Errorf("addr=%q 应被接受，得到错误: %v", tc.addr, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("addr=%q 应被拒绝（N5b）", tc.addr)
		}
		if !tc.ok && err != nil && tc.wantMsg != "" && !strings.Contains(err.Error(), tc.wantMsg) {
			t.Errorf("addr=%q 的拒绝原因应点明 %s: %v", tc.addr, tc.wantMsg, err)
		}
		if h != nil {
			if cerr := h.Close(); cerr != nil {
				t.Errorf("close addr=%q: %v", tc.addr, cerr)
			}
		}
	}
}

// TestConfigValidation：必填项缺失即拒。
func TestConfigValidation(t *testing.T) {
	base := func() host.Config {
		return host.Config{DBPath: filepath.Join(t.TempDir(), "e.db"), Model: "m"}
	}
	cases := map[string]func(*host.Config){
		"缺 DBPath":    func(c *host.Config) { c.DBPath = "" },
		"缺 Model":     func(c *host.Config) { c.Model = "  " },
		"MaxTurns 负":  func(c *host.Config) { c.MaxTurns = -1 },
		"Approval 非法": func(c *host.Config) { c.Approval = "sometimes" },
	}
	for name, mut := range cases {
		cfg := base()
		mut(&cfg)
		if _, err := host.New(cfg); err == nil {
			t.Errorf("%s 应被拒绝", name)
		}
	}
}

// ---------------------------------------------------------------------------
// 重开 / 多实例：事件流是唯一事实源，谁读都看到同一条流
// ---------------------------------------------------------------------------

// TestTwoHubsShareOneEventLog：两个 hub 实例指向同一个事件库（G9 Kafka 模式：
// 多写者只管 append，seq 由库原子分配）。各自跑各自的 session，第三个实例
// （后开的）应在线表里看到**全部** session——事件流是唯一事实源。
func TestTwoHubsShareOneEventLog(t *testing.T) {
	db := filepath.Join(t.TempDir(), "shared.db")
	target := filepath.Join(t.TempDir(), "shared-target.txt")
	if err := os.WriteFile(target, []byte("shared"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	newHub := func() *host.Hub {
		return newTestHub(t, withProvider(`{"path":"`+filepath.ToSlash(target)+`"}`), func(c *host.Config) {
			c.DBPath = db
		})
	}

	a := newHub()
	sidA := runOneRound(t, a, "A 的任务")
	b := newHub() // 第二个实例：同一个库
	sidB := runOneRound(t, b, "B 的任务")

	// 第三个实例（a 已经跑完，b 也跑完）重开：应看到两个 session。
	c := newHub()
	rows, err := c.Sessions(context.Background())
	if err != nil {
		t.Fatalf("Sessions: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("在线表应有 2 个 session，得到 %d: %+v", len(rows), rows)
	}
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r.SessionID] = true
		if r.EventCount != 7 {
			t.Fatalf("session %s event_count = %d，want 7", r.SessionID, r.EventCount)
		}
	}
	if !seen[string(sidA)] || !seen[string(sidB)] {
		t.Fatalf("在线表缺 session：A=%v B=%v", seen[string(sidA)], seen[string(sidB)])
	}
	// 两实例的 seq 不重叠（库级原子分配）。
	if err := a.Close(); err != nil {
		t.Fatalf("close a: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close b: %v", err)
	}
}

// TestReopenPreservesOnlineTableAndSnapshot：进程退出再重开（同一事件库），
// 在线表与上下文快照从事件流重建（内存投影启动时播种）。
func TestReopenPreservesOnlineTableAndSnapshot(t *testing.T) {
	db := filepath.Join(t.TempDir(), "events.db")
	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("reopen"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}

	cfg := func() host.Config {
		return host.Config{
			DBPath:   db,
			Model:    "scripted",
			System:   "test-system",
			MaxTurns: 4,
			Allow:    []string{"read"},
			Provider: &scriptedProvider{args: `{"path":"` + filepath.ToSlash(target) + `"}`},
		}
	}

	h1, err := host.New(cfg())
	if err != nil {
		t.Fatalf("host.New #1: %v", err)
	}
	sid := runOneRound(t, h1, "读一下")
	if err := h1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	// 重开：同一 DBPath，provider 不再被用到（不跑新 loop）。
	h2, err := host.New(cfg())
	if err != nil {
		t.Fatalf("host.New #2: %v", err)
	}
	defer h2.Close()

	srv := httptest.NewServer(h2.Handler())
	defer srv.Close()

	var rows []host.SessionRow
	if code := getJSON(t, srv.URL+"/api/sessions", &rows); code != http.StatusOK || len(rows) != 1 {
		t.Fatalf("重开后在线表 = %d 行（code=%d）", len(rows), code)
	}
	if rows[0].EventCount != 7 {
		t.Fatalf("重开后 event_count = %d，want 7", rows[0].EventCount)
	}
	var snap contracts.ContextSnapshot
	if code := getJSON(t, srv.URL+"/api/context/"+string(sid), &snap); code != http.StatusOK {
		t.Fatalf("重开后 context 状态码 = %d", code)
	}
	if snap.SessionID != sid || snap.Turn != 2 || len(snap.Parts) == 0 {
		t.Fatalf("重开后快照不符: %+v", snap)
	}
}

// ---------------------------------------------------------------------------
// WS 事件流（N4）——端到端：真 store 跑一轮，WS 客户端收到事件
// ---------------------------------------------------------------------------

// TestWSReceivesEventsFromRealRun：脚本 provider + 真 store 跑一轮，
// WS 客户端（transport.Client）按订阅收到 context.assembled 等事件。
func TestWSReceivesEventsFromRealRun(t *testing.T) {
	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("ws e2e"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	h := newTestHub(t, withProvider(`{"path":"`+filepath.ToSlash(target)+`"}`))
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := transport.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	// 订阅：只收事实事件里的写路径 + 上下文快照。
	if err := c.Subscribe(ctx, contracts.SubscribeRequest{
		KindPrefixes: []string{"context.assembled", "tool.call."},
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	sid := runOneRound(t, h, "读一下")

	// 收 4 条：两轮 context.assembled + intent + completed。
	got := make(map[string]int)
	for i := 0; i < 4; i++ {
		recvCtx, recvCancel := context.WithTimeout(context.Background(), 5*time.Second)
		m, err := c.RecvContext(recvCtx)
		recvCancel()
		if err != nil {
			t.Fatalf("recv #%d: %v", i, err)
		}
		if m.Kind != transport.KindEvent {
			t.Fatalf("消息 #%d kind = %q，want event", i, m.Kind)
		}
		var ev contracts.Event
		if err := json.Unmarshal(m.Payload, &ev); err != nil {
			t.Fatalf("event payload: %v", err)
		}
		if ev.SessionID != sid {
			t.Fatalf("事件归属 session = %q，want %q", ev.SessionID, sid)
		}
		if ev.Seq == nil {
			t.Fatalf("推送的事件缺 seq（订阅者要靠它推水位）: %+v", ev)
		}
		got[ev.Kind]++
	}
	if got["context.assembled"] != 2 || got["tool.call.intent"] != 1 || got["tool.call.completed"] != 1 {
		t.Fatalf("收到的事件分布 = %v，want context.assembled×2 + intent×1 + completed×1", got)
	}

	// context.assembled 的载荷在事件流里就是 ContextSnapshot 契约对象。
	var snap contracts.ContextSnapshot
	if code := getJSON(t, srv.URL+"/api/context/"+string(sid), &snap); code != http.StatusOK {
		t.Fatalf("context 状态码 = %d", code)
	}
	if snap.Turn != 2 {
		t.Fatalf("快照 turn = %d，want 2", snap.Turn)
	}
}
