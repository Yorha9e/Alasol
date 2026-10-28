package host_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/host"
)

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

// recordingProvider 记录每次请求的**最后一条消息**（在线表注入位置的断言对象），恒回文本收敛。
type recordingProvider struct {
	mu    sync.Mutex
	tails []string
}

func (p *recordingProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu.Lock()
	if len(req.Messages) > 0 {
		p.tails = append(p.tails, req.Messages[len(req.Messages)-1].Content)
	}
	p.mu.Unlock()
	return core.ModelResult{Text: "ok"}, nil
}

func (p *recordingProvider) lastTail() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.tails) == 0 {
		return ""
	}
	return p.tails[len(p.tails)-1]
}

var _ core.Provider = (*recordingProvider)(nil)

// ---------------------------------------------------------------------------
// 端到端：主 agent prompt 末尾注入在线表 + gate 过滤
// ---------------------------------------------------------------------------

// TestOnlineTableInjectedEndToEnd：主 agent 的 provider 请求末尾是在线表；按 gate 过滤
// （同 repo 的 B 可见，别 repo 的 C 不可见——负例；自己恒在）。
func TestOnlineTableInjectedEndToEnd(t *testing.T) {
	repoR := tempRepoOrDir(t) // A、B 同 repo（相交）
	repoZ := tempRepoOrDir(t) // C 别 repo（不相交）
	rp := &recordingProvider{}
	h := newTestHub(t, func(c *host.Config) {
		c.Provider = rp
		c.Allow = nil // 记录型 provider 不调工具
	})

	ctx := context.Background()
	sidA, err := h.CreateSession(host.SessionSpec{CWD: repoR})
	if err != nil {
		t.Fatalf("CreateSession A: %v", err)
	}
	if _, err := h.RunPrompt(ctx, sidA, "A 的任务"); err != nil {
		t.Fatalf("RunPrompt A: %v", err)
	}
	sidB, err := h.CreateSession(host.SessionSpec{CWD: repoR})
	if err != nil {
		t.Fatalf("CreateSession B: %v", err)
	}
	if _, err := h.RunPrompt(ctx, sidB, "B 的任务"); err != nil {
		t.Fatalf("RunPrompt B: %v", err)
	}
	sidC, err := h.CreateSession(host.SessionSpec{CWD: repoZ})
	if err != nil {
		t.Fatalf("CreateSession C: %v", err)
	}
	if _, err := h.RunPrompt(ctx, sidC, "C 的任务"); err != nil {
		t.Fatalf("RunPrompt C: %v", err)
	}

	// 再跑 A（同一 session/loop）：此刻 B 已在线且同 repo → A 的在线表 = [self A, peer B]。
	if _, err := h.RunPrompt(ctx, sidA, "A 再看一眼谁在线"); err != nil {
		t.Fatalf("RunPrompt A #2: %v", err)
	}
	tail := rp.lastTail()
	if !strings.Contains(tail, "[在线表") {
		t.Fatalf("A 的请求末尾应是在线表: %q", tail)
	}
	if !strings.Contains(tail, "[self]") {
		t.Fatalf("在线表应含 self: %q", tail)
	}
	// gate 过滤（负例）：别 repo 的 C 不可见 → 恰好 1 个 peer（B）、共 2 行。
	// （同毫秒 ULID 前 8 字符相同，故用结构化计数而非 id 子串判 C 的缺席。）
	if got := strings.Count(tail, "[peer]"); got != 1 {
		t.Fatalf("应恰好 1 个 peer（B；C 被 gate 过滤），得到 %d: %q", got, tail)
	}
	if !strings.Contains(tail, "以下 2 行") {
		t.Fatalf("应共 2 行（self + B）: %q", tail)
	}
	// 用全 id 精确核对可见面：A 只见 B，不见 C（K11）。
	view, err := h.Peers(ctx, sidA)
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(view.Peers) != 1 || view.Peers[0].SessionID != string(sidB) {
		t.Fatalf("A 的可见对端应恰为 B（全 id）: %+v", view.Peers)
	}
}

// TestOnlineTableRuntimeUpdateAffectsNextTurn：运行期经 HTTP 改配置 → 同一 hub **不重启**，
// 下一轮 prompt 立即反映新参数（暴露等级 + N 截断）（K15 硬要求）。
func TestOnlineTableRuntimeUpdateAffectsNextTurn(t *testing.T) {
	repoR := tempRepoOrDir(t)
	rp := &recordingProvider{}
	h := newTestHub(t, func(c *host.Config) {
		c.Provider = rp
		c.Allow = nil
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	ctx := context.Background()
	sidA, _ := h.CreateSession(host.SessionSpec{CWD: repoR})
	if _, err := h.RunPrompt(ctx, sidA, "A"); err != nil {
		t.Fatalf("RunPrompt A: %v", err)
	}
	sidB, _ := h.CreateSession(host.SessionSpec{CWD: repoR})
	if _, err := h.RunPrompt(ctx, sidB, "B"); err != nil {
		t.Fatalf("RunPrompt B: %v", err)
	}
	// A 第二次：默认 summary，2 行（self + B）。
	if _, err := h.RunPrompt(ctx, sidA, "A2"); err != nil {
		t.Fatalf("RunPrompt A2: %v", err)
	}
	before := rp.lastTail()
	if !strings.Contains(before, "暴露等级=summary") {
		t.Fatalf("改前应 summary: %q", before)
	}

	// 运行期改配置（不重启）：full + N=1。
	if code := postJSON(t, srv.URL+"/api/online-table/config", map[string]any{"level": "full", "max_rows": 1}, nil); code != http.StatusOK {
		t.Fatalf("POST 配置状态码 = %d", code)
	}

	// A 第三次：同一 hub、同一 loop → 下一轮立即反映新参数。
	if _, err := h.RunPrompt(ctx, sidA, "A3"); err != nil {
		t.Fatalf("RunPrompt A3: %v", err)
	}
	after := rp.lastTail()
	if !strings.Contains(after, "暴露等级=full") {
		t.Fatalf("改后应 full: %q", after)
	}
	if !strings.Contains(after, "略去 1 行") {
		t.Fatalf("N=1 应把 2 行截到 1（略去 1 行）: %q", after)
	}
}

// TestOnlineTableConfigEndpoint：GET 读默认 / POST 运行期改 / 非法 400 / 改后 GET 即见（不重启）。
func TestOnlineTableConfigEndpoint(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) {
		c.Provider = &recordingProvider{}
		c.Allow = nil
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// GET 默认（K15：N=50 / 每行 ≤100 token / summary）。
	var v loop.OnlineTableView
	if code := getJSON(t, srv.URL+"/api/online-table/config", &v); code != http.StatusOK {
		t.Fatalf("GET 状态码 = %d", code)
	}
	if v.MaxRows != 50 || v.LineTokenBudget != 100 || v.Level != "summary" {
		t.Fatalf("默认配置不符: %+v", v)
	}

	// POST 部分更新。
	var v2 loop.OnlineTableView
	if code := postJSON(t, srv.URL+"/api/online-table/config", map[string]any{"max_rows": 3, "level": "n", "window_lines": 7}, &v2); code != http.StatusOK {
		t.Fatalf("POST 状态码 = %d", code)
	}
	if v2.MaxRows != 3 || v2.Level != "n" || v2.WindowLines != 7 {
		t.Fatalf("POST 后配置不符: %+v", v2)
	}

	// GET 再读 → 已是新值（不重启）。
	var v3 loop.OnlineTableView
	getJSON(t, srv.URL+"/api/online-table/config", &v3)
	if v3.MaxRows != 3 || v3.Level != "n" || v3.WindowLines != 7 {
		t.Fatalf("GET 应返回更新后的值: %+v", v3)
	}

	// 非法 level → 400，且不改动原配置。
	var te contracts.TransportError
	if code := postJSON(t, srv.URL+"/api/online-table/config", map[string]any{"level": "bogus"}, &te); code != http.StatusBadRequest {
		t.Fatalf("非法 level 应 400，得到 %d", code)
	}
	var v4 loop.OnlineTableView
	getJSON(t, srv.URL+"/api/online-table/config", &v4)
	if v4.Level != "n" {
		t.Fatalf("非法更新不该改动配置（level 应仍为 n）: %+v", v4)
	}
}

// TestOnlineTableStartupConfig：hub 起动时可配（Config 字段 → 生效配置），运行期仍可再调。
func TestOnlineTableStartupConfig(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) {
		c.Provider = &recordingProvider{}
		c.Allow = nil
		c.OnlineTableRows = 7
		c.OnlineTableLevel = "full"
		c.OnlineTableLineTokens = 42
	})
	v := h.OnlineTableView()
	if v.MaxRows != 7 || v.Level != "full" || v.LineTokenBudget != 42 {
		t.Fatalf("起动配置未生效: %+v", v)
	}
}

// TestOnlineTableSurvivesReopen：关掉 hub 再重开（同一事件库），在线表数据源由事件流重建
// （K10/G17b）；重跑两个同 repo 的 session 重新激活后，彼此再次出现在对方的在线表里
// （证明注入 wiring 在 New 里恒建 + 发现表崩溃重启可重建）。
func TestOnlineTableSurvivesReopen(t *testing.T) {
	repoR := tempRepoOrDir(t)
	db := filepath.Join(t.TempDir(), "events.db")

	sidA, sidB := host.NewSession(), host.NewSession()
	newHub := func() (*host.Hub, *recordingProvider) {
		rp := &recordingProvider{}
		return newTestHub(t, func(c *host.Config) {
			c.Provider = rp
			c.Allow = nil
			c.DBPath = db
			c.CWD = repoR
		}), rp
	}

	// hub 1：跑 A、B（同 repo，互见）。
	h1, _ := newHub()
	ctx := context.Background()
	for _, p := range []struct {
		sid    contracts.Ulid
		prompt string
	}{{sidA, "A"}, {sidB, "B"}} {
		if _, err := h1.RunPrompt(ctx, p.sid, p.prompt); err != nil {
			t.Fatalf("hub1 RunPrompt: %v", err)
		}
	}
	if err := h1.Close(); err != nil {
		t.Fatalf("close hub1: %v", err)
	}

	// hub 2（重开，同一库）：**复用** session id 重跑 → 发现表从事件流重建 + 重新激活。
	h2, rp2 := newHub()
	defer h2.Close()
	// A、B 各跑一次（重新激活），再跑 A 一次 → A 的在线表应重新含 peer B。
	for _, p := range []struct {
		sid    contracts.Ulid
		prompt string
	}{{sidA, "A reopen"}, {sidB, "B reopen"}, {sidA, "A 再看"}} {
		if _, err := h2.RunPrompt(ctx, p.sid, p.prompt); err != nil {
			t.Fatalf("hub2 RunPrompt: %v", err)
		}
	}
	tail := rp2.lastTail()
	if !strings.Contains(tail, "[在线表") {
		t.Fatalf("重开后在线表应仍注入: %q", tail)
	}
	if got := strings.Count(tail, "[peer]"); got != 1 {
		t.Fatalf("重跑激活后 A 应再见 B（1 个 peer），得到 %d: %q", got, tail)
	}
}
