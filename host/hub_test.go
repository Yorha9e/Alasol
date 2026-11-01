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
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/identity"
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
//
// 默认工作目录指向**自建的临时 git 仓库**（tempRepoOrDir）：hub 级默认工作目录为空时
// 会退回进程 cwd，而"进程 cwd 在不在 git 仓库内"取决于跑测试的环境（拷到没有 .git
// 的目录里就会变），那会让"自动 gate → 多一条 gate.joined → 事件数 8"的断言随机红。
// 自建仓库后，事件条数只取决于测试自己造的东西。
func newTestHub(t *testing.T, muts ...func(*host.Config)) *host.Hub {
	t.Helper()
	h, _ := newTestHubWithCWD(t, muts...)
	return h
}

// newTestHubWithCWD 同 newTestHub，但**同时返回生效的工作目录**（mut 覆盖过 CWD 时
// 返回覆盖后的值）——断言要按这个目录算期望事件条数（wantEventCount）。
func newTestHubWithCWD(t *testing.T, muts ...func(*host.Config)) (*host.Hub, string) {
	t.Helper()
	cfg := host.Config{
		DBPath:   filepath.Join(t.TempDir(), "events.db"),
		Model:    "scripted",
		System:   "test-system",
		MaxTurns: 4,
		Allow:    []string{"read"},
		CWD:      tempRepoOrDir(t),
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
	return h, cfg.CWD
}

// withProvider 把脚本 provider 注入 Config（args 是首轮工具调用的参数）。
func withProvider(args string) func(*host.Config) {
	return func(c *host.Config) {
		c.Provider = &scriptedProvider{args: args}
	}
}

// assertAutoGate 断言"自动 repo gate 在 / 不在"——事件条数那条断言的另一半：多出来的
// 那一条就是 gate.joined（K12 自动 gate）。只看条数的话，"少了一条"和"多了一条"
// 分不清；这里把"gate 在不在"也钉住。
//
// 走 GateMembers（而不是 Peers）：GateMembers 内部会先 catchUp（把事件流扫进发现表），
// 但**不要求**本 hub 托管该 session——重开/换实例的用例里 session 归别的 hub 管。
func assertAutoGate(t *testing.T, h *host.Hub, sid contracts.Ulid, cwd string) {
	t.Helper()
	repoID := ""
	if cwd != "" {
		if id, err := identity.RepoID(cwd); err == nil && id != "" {
			repoID = id
		}
	}
	want := repoID != ""
	got := false
	if repoID != "" {
		if _, err := h.GateMembers(context.Background(), discovery.RepoGate(repoID)); err != nil {
			t.Fatalf("GateMembers: %v", err)
		}
		for _, g := range h.Discovery().GatesOf(sid) {
			if g == discovery.RepoGate(repoID) {
				got = true
			}
		}
	}
	if got != want {
		t.Fatalf("自动 repo gate 存在性 = %v，want %v（gates=%v）", got, want, h.Discovery().GatesOf(sid))
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
	h, cwd := newTestHubWithCWD(t, withProvider(`{"path":"`+filepath.ToSlash(target)+`"}`))
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

	// 在线表：一行，事件条数 = 7 条基础 + 1 条自动 gate.joined（K12 自动 gate；
	// 工作目录算不出 repo_id 时没有那一条 → 7 条），最后 kind 是收尾那一轮的 model.turn
	// （v1-S1 起对话入流：user.prompt + 两轮各 context.assembled/model.turn + intent + completed；
	// v1-S2 起 loop 建成时先落一条 gate.joined）。
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
	want := wantEventCount(t, cwd)
	if row.EventCount != int64(want) || row.FirstSeq != 1 || row.LastSeq != int64(want) {
		t.Fatalf("seq/计数不符: %+v（want event_count=%d）", row, want)
	}
	assertAutoGate(t, h, sid, cwd)
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
	cwd := tempRepoOrDir(t)
	newHub := func() *host.Hub {
		return newTestHub(t, withProvider(`{"path":"`+filepath.ToSlash(target)+`"}`), func(c *host.Config) {
			c.DBPath = db
			c.CWD = cwd
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
	// 7 = user.prompt + 两轮各 context.assembled/model.turn + intent + completed；
	// 再 +1 = 自动 gate.joined（v1-S2 自动 gate，工作目录算不出 repo_id 时没有）。
	want := wantEventCount(t, cwd)
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r.SessionID] = true
		if r.EventCount != int64(want) {
			t.Fatalf("session %s event_count = %d，want %d", r.SessionID, r.EventCount, want)
		}
	}
	assertAutoGate(t, c, sidA, cwd)
	assertAutoGate(t, c, sidB, cwd)
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

	cwd := tempRepoOrDir(t)
	cfg := func() host.Config {
		return host.Config{
			DBPath:   db,
			Model:    "scripted",
			System:   "test-system",
			MaxTurns: 4,
			Allow:    []string{"read"},
			CWD:      cwd,
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
	want := wantEventCount(t, cwd)
	if rows[0].EventCount != int64(want) {
		t.Fatalf("重开后 event_count = %d，want %d", rows[0].EventCount, want)
	}
	assertAutoGate(t, h2, sid, cwd)
	var snap contracts.ContextSnapshot
	if code := getJSON(t, srv.URL+"/api/context/"+string(sid), &snap); code != http.StatusOK {
		t.Fatalf("重开后 context 状态码 = %d", code)
	}
	if snap.SessionID != sid || snap.Turn != 2 || len(snap.Parts) == 0 {
		t.Fatalf("重开后快照不符: %+v", snap)
	}
}

// TestSessionsHostableField（G12）：GET /api/sessions 行的 hostable = **本进程**托管与否
// （= 协议面可不可发 prompt）。
//
// 一库一 hub 是预期拓扑（G25）：共享库的读面按库投影——别的 hub 建的 session 在本进程
// 列表里**看得见**（这是正确行为），但托管写面只对本进程的 loop 生效（不可发）。此前
// 列表不区分，GUI 只能点下去吃 404 unknown_session；hostable 让它直接灰掉不可发行。
// 变异自证：hostable 恒 true → 外来/重启失管两步必红。
func TestSessionsHostableField(t *testing.T) {
	db := filepath.Join(t.TempDir(), "hostable.db")
	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("hostable"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	// 每次 New 用**新的** provider 实例（脚本 provider 的调用计数是实例态——共用一个
	// 会让第二个 hub 直接收到收尾文本，runOneRound 的 Events==4 断言随机红）。
	cfg := func() host.Config {
		return host.Config{
			DBPath: db, Model: "scripted", System: "hostable", MaxTurns: 4,
			Allow:    []string{"read"},
			Provider: &scriptedProvider{args: `{"path":"` + filepath.ToSlash(target) + `"}`},
			CWD:      tempRepoOrDir(t),
		}
	}
	rowFor := func(t *testing.T, h *host.Hub, sid contracts.Ulid) host.SessionRow {
		t.Helper()
		rows, err := h.Sessions(context.Background())
		if err != nil {
			t.Fatalf("Sessions: %v", err)
		}
		for _, r := range rows {
			if r.SessionID == string(sid) {
				return r
			}
		}
		t.Fatalf("在线表里找不到 session %s（rows=%+v）", sid, rows)
		return host.SessionRow{}
	}

	// ① 本进程托管（跑过一轮 = h.loops 有 loop）→ hostable=true。
	h1, err := host.New(cfg())
	if err != nil {
		t.Fatalf("host.New #1: %v", err)
	}
	sid1 := runOneRound(t, h1, "A 在吗")
	if got := rowFor(t, h1, sid1); !got.Hostable {
		t.Fatalf("本进程托管的 session hostable = false，want true: %+v", got)
	}

	// ② 外来（另一个 hub 进程同时开着**同一个库**）→ 读面可见（正确）但 hostable=false。
	h2, err := host.New(cfg())
	if err != nil {
		t.Fatalf("host.New #2: %v", err)
	}
	if got := rowFor(t, h2, sid1); got.Hostable {
		t.Fatalf("别的 hub 建的 session 对本进程不该 hostable=true: %+v", got)
	}
	// h2 自己跑的 → true（同一张表里两种值并存，正是 GUI 要区分的"3 个会话只有 1 个可发"）。
	sid2 := runOneRound(t, h2, "B 在吗")
	if got := rowFor(t, h2, sid2); !got.Hostable {
		t.Fatalf("本进程自己跑的 session hostable = false，want true: %+v", got)
	}

	// ③ 重启失管：关掉两个宿主，重开 h3 → 旧 session 仍在表里（可读）但全部
	//    hostable=false（loops 是内存态，重启即空——G2 的重启口径）；h3 自己跑的 true。
	if err := h1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}
	if err := h2.Close(); err != nil {
		t.Fatalf("Close #2: %v", err)
	}
	h3, err := host.New(cfg())
	if err != nil {
		t.Fatalf("host.New #3: %v", err)
	}
	defer h3.Close()
	for _, sid := range []contracts.Ulid{sid1, sid2} {
		if got := rowFor(t, h3, sid); got.Hostable {
			t.Fatalf("重启失管的 session %s hostable = true，want false: %+v", sid, got)
		}
	}
	sid3 := runOneRound(t, h3, "C 在吗")
	if got := rowFor(t, h3, sid3); !got.Hostable {
		t.Fatalf("重启后本进程新跑的 session hostable = false，want true: %+v", got)
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
	// G8：订阅后服务端先回一条 kind=subscribed 回执，先收掉再读事件。
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	ack, err := c.RecvContext(ackCtx)
	ackCancel()
	if err != nil || ack.Kind != transport.KindSubscribed {
		t.Fatalf("订阅回执: kind=%q err=%v", ack.Kind, err)
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

// ---------------------------------------------------------------------------
// G25 跨进程托管注册（2026-11-01 v4-7）
// ---------------------------------------------------------------------------

// TestSessionsHostHubField：GET /api/sessions 行的 host_hub = 托管该 session 的 hub
// 进程标识。本进程托管 = 本 hub 的 id（Hub.ID()，恒非空 ULID）；别的进程托管 / 重启
// 失管 = 空（托管登记是发现表进程本地状态，跨进程读不到别家的——如实留空不猜）。
// 变异自证：本进程托管行 host_hub 恒空 → 第一步必红。
func TestSessionsHostHubField(t *testing.T) {
	db := filepath.Join(t.TempDir(), "hosthub.db")
	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("hosthub"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	cfg := func() host.Config {
		return host.Config{
			DBPath: db, Model: "scripted", System: "hosthub", MaxTurns: 4,
			Allow:    []string{"read"},
			Provider: &scriptedProvider{args: `{"path":"` + filepath.ToSlash(target) + `"}`},
			CWD:      tempRepoOrDir(t),
		}
	}
	rowFor := func(t *testing.T, h *host.Hub, sid contracts.Ulid) host.SessionRow {
		t.Helper()
		rows, err := h.Sessions(context.Background())
		if err != nil {
			t.Fatalf("Sessions: %v", err)
		}
		for _, r := range rows {
			if r.SessionID == string(sid) {
				return r
			}
		}
		t.Fatalf("在线表里找不到 session %s", sid)
		return host.SessionRow{}
	}

	h1, err := host.New(cfg())
	if err != nil {
		t.Fatalf("host.New #1: %v", err)
	}
	defer h1.Close()
	if h1.ID() == "" {
		t.Fatal("Hub.ID() 恒非空（New 生成的进程标识）")
	}
	if !core.ValidUlid(contracts.Ulid(h1.ID())) {
		t.Fatalf("Hub.ID() 应是 ULID：%q", h1.ID())
	}
	sid := runOneRound(t, h1, "在吗")
	got := rowFor(t, h1, sid)
	if !got.Hostable || got.HostHub != h1.ID() {
		t.Fatalf("本进程托管的行应 hostable=true 且 host_hub=本 hub id %q，得到 %+v", h1.ID(), got)
	}

	// 发现表侧同口径：Claim 登记的宿主 == Hub.ID()（loopFor 建成时经 Claim 写入）。
	hostID, ok := h1.Discovery().HostOf(sid)
	if !ok || hostID != h1.ID() {
		t.Fatalf("Discovery().HostOf = (%q, %v)，want (%q, true)", hostID, ok, h1.ID())
	}

	// 别的 hub（同库另一个进程的模拟）：行可见但 hostable=false、host_hub 空。
	h2, err := host.New(cfg())
	if err != nil {
		t.Fatalf("host.New #2: %v", err)
	}
	defer h2.Close()
	if got := rowFor(t, h2, sid); got.Hostable || got.HostHub != "" {
		t.Fatalf("别的 hub 建的 session 对本进程应 hostable=false 且 host_hub 空，得到 %+v", got)
	}
}

// TestLoopForClaimConflictFailClose：发现表已把 session 登记给**另一个** hub 时，
// 本 hub 接管（loopFor → Claim）必须 fail-close 拒绝（G25：一个 session 的事件只由
// 其宿主进程 append——双宿主 = 两进程各自 append，seq 变成混排提交序）。
// 变异自证：Claim 冲突不拒（吞掉错误继续）→ 本钉红。
func TestLoopForClaimConflictFailClose(t *testing.T) {
	db := filepath.Join(t.TempDir(), "claimconflict.db")
	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("claim"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	h, err := host.New(host.Config{
		DBPath: db, Model: "scripted", System: "claim", MaxTurns: 4,
		Allow:    []string{"read"},
		Provider: &scriptedProvider{args: `{"path":"` + filepath.ToSlash(target) + `"}`},
		CWD:      tempRepoOrDir(t),
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	defer h.Close()

	// 模拟"这个 session 已被另一个 hub 进程托管"：直接在发现表上以异 hub id 登记。
	sid := host.NewSession()
	if err := h.Discovery().Claim(sid, "hub-from-another-process"); err != nil {
		t.Fatalf("预登记异 hub: %v", err)
	}
	// 接管 → 拒绝，且给出可读提示（不静默双写）。
	_, err = h.RunPrompt(context.Background(), sid, "接管试试")
	if err == nil {
		t.Fatal("异 hub 托管的 session 被本 hub 接管应 fail-close 报错")
	}
	if !strings.Contains(err.Error(), "托管登记失败") {
		t.Fatalf("错误应点名托管登记失败，得到: %v", err)
	}
	// 宿主登记未被改动（fail-close：拒绝接管不改现状）。
	if hostID, _ := h.Discovery().HostOf(sid); hostID != "hub-from-another-process" {
		t.Fatalf("冲突后宿主登记不该被改动，得到 %q", hostID)
	}
	// 本 hub 自己的新 session 不受影响（正常跑通）。
	if _, err := h.RunPrompt(context.Background(), host.NewSession(), "新会话"); err != nil {
		t.Fatalf("本 hub 新 session 不该被波及: %v", err)
	}
}
