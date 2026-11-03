package host

// G-a（session 在线三态）host 层接线的验收用例（缺口 G-a 件 2/3）：
//
//	① SessionRow 三列（online / running / last_activity）的取值口径；
//	② GET /api/sessions?online= 过滤（true|1 / false|0 / 缺省全库 / 非法 400 bad_online）；
//	③ RunPrompt 的 BeginRun/EndRun 配平（跑轮期间 running=true、跑完回落）；
//	④ 活跃窗口 N 的**单源同步**（onlineTableConfig → 发现表：构造 + 运行期 Update）。
//
// 时间控制**不注入时钟**：用"新鲜 ts + 窗口夹具（极小 / 极大）"组合控制——与 core 侧
// online_window_test 同款手法（farWindow / nearWindow 在本文件定义，package host 的
// 其余内部测试共用，如 onlinetable_internal_test 的 G-a 修法）。
//
// 为什么是 internal（package host）：无 ts 的事件要直接往 h.store 造、窗口夹具要直接摸
// h.disc——黑箱面没有"发一条无 ts 事件 / 直接改发现表窗口"的公开 API。

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
)

// farWindow 极大窗口（G-a 夹具"极大"侧）：把远过去的事件 ts 也罩进活跃窗。
const farWindow = 100 * 365 * 24 * time.Hour

// nearWindow 极小窗口（G-a 夹具"极小"侧）：任何真实事件 ts 都在窗外 → 立刻"超窗"。
const nearWindow = time.Nanosecond

// ---------------------------------------------------------------------------
// 夹具
// ---------------------------------------------------------------------------

// newOnlineTestHub 装配一个 Hub：临时事件库 + 单轮收敛的脚本 provider + **非 git 的
// CWD**（无自动 gate → 行数只由测试自己造的东西决定，不依赖环境）。
func newOnlineTestHub(t *testing.T, muts ...func(*Config)) *Hub {
	t.Helper()
	cfg := Config{
		DBPath:   filepath.Join(t.TempDir(), "events.db"),
		Model:    "online-test",
		System:   "online-test",
		MaxTurns: 4,
		Provider: &forkTestProvider{}, // 复用 fork 内部测试的单轮收敛脚本 provider
		CWD:      t.TempDir(),
	}
	for _, mut := range muts {
		if mut != nil {
			mut(&cfg)
		}
	}
	h, err := New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// runOnlinePrompt 建一个 session 并跑一轮（真事件流），返回 session id。
func runOnlinePrompt(t *testing.T, h *Hub, prompt string) contracts.Ulid {
	t.Helper()
	sid, err := h.CreateSession(SessionSpec{})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := h.RunPrompt(context.Background(), sid, prompt); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	return sid
}

// sessionRowOf 从 Sessions() 里取指定 session 的行（不在表里 → 测试失败）。
func sessionRowOf(t *testing.T, h *Hub, sid contracts.Ulid) SessionRow {
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
	t.Fatalf("session %s 不在在线表里: %+v", sid, rows)
	return SessionRow{}
}

// getSessionRows 发 GET 并解成 SessionRow 数组（状态码非 200 → 失败）。
func getSessionRows(t *testing.T, url string) []SessionRow {
	t.Helper()
	code, body := getJSONRaw(t, url)
	if code != http.StatusOK {
		t.Fatalf("GET %s status = %d, body = %s", url, code, body)
	}
	var rows []SessionRow
	if err := json.Unmarshal([]byte(body), &rows); err != nil {
		t.Fatalf("解 SessionRow 数组失败: %v（body=%s）", err, body)
	}
	return rows
}

// sessionIDsOf 把行折成 session id 集合（顺序无关的成员断言用）。
func sessionIDsOf(rows []SessionRow) map[string]bool {
	out := make(map[string]bool, len(rows))
	for _, r := range rows {
		out[r.SessionID] = true
	}
	return out
}

// ---------------------------------------------------------------------------
// ① SessionRow 三列
// ---------------------------------------------------------------------------

// TestSessionRowOnlineThreeStates：online / running / last_activity 三列的取值（G-a）。
func TestSessionRowOnlineThreeStates(t *testing.T) {
	h := newOnlineTestHub(t)
	ctx := context.Background()

	// ① 完全没有事件的 session：CreateSession 只登记归属、不落事件 → 投影里没有它
	//（在线表按事件流投影，"无事件"连行都没有，不是一行空 last_activity）。
	neverRan, err := h.CreateSession(SessionSpec{})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	for _, r := range mustSessionRows(t, h) {
		if r.SessionID == string(neverRan) {
			t.Fatalf("没跑过的 session 不该进在线表: %+v", r)
		}
	}

	// ② 有事件但事件**没有 ts** → 行存在，last_activity=""（无活动证据）、
	//    online=false（activeLocked：lastTS 空 → 不活跃）、running=false。
	sidNoTS := NewSession()
	raw, err := json.Marshal(contracts.UserPromptPayload{Text: "无 ts 事件"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := h.store.Append(ctx, []contracts.Event{{
		V:         1,
		ID:        core.NewUlid(),
		Kind:      "user.prompt",
		TS:        "",
		SessionID: sidNoTS,
		AgentID:   core.NewUlid(),
		Durable:   true,
		Payload:   raw,
	}}); err != nil {
		t.Fatalf("Append(无 ts): %v", err)
	}
	row := sessionRowOf(t, h, sidNoTS)
	if row.LastActivity != "" || row.UpdatedAt != "" {
		t.Fatalf("无 ts 的行 last_activity / updated_at 应都是空串: %+v", row)
	}
	if row.Online || row.Running {
		t.Fatalf("无活动证据的行不该 online / running: %+v", row)
	}

	// ③ 跑一轮（真事件流）：跑完 Running=false（EndRun 已配平）、Online=true（事件新鲜、
	//    默认 15m 窗）、last_activity 是 RFC3339 且**与 updated_at 同源同值**。
	sid := runOnlinePrompt(t, h, "任务")
	row = sessionRowOf(t, h, sid)
	if row.Running {
		t.Fatalf("跑完后 Running 应已 EndRun 配平为 false: %+v", row)
	}
	if !row.Online {
		t.Fatalf("新鲜事件的 session 应 online: %+v", row)
	}
	if row.LastActivity == "" {
		t.Fatalf("有事件的行 last_activity 不该为空: %+v", row)
	}
	if _, err := time.Parse(time.RFC3339, row.LastActivity); err != nil {
		t.Fatalf("last_activity 应是 RFC3339 的最后事件 ts（得到 %q）: %v", row.LastActivity, err)
	}
	if row.LastActivity != row.UpdatedAt {
		t.Fatalf("last_activity 应与 updated_at 同源同值: %q vs %q", row.LastActivity, row.UpdatedAt)
	}

	// ④ 超窗且未 running → online=false（窗口收到极小，事件 ts 立刻落窗外）。
	h.disc.SetActivityWindow(nearWindow)
	row = sessionRowOf(t, h, sid)
	if row.Online {
		t.Fatalf("超窗且未 running 不该 online: %+v", row)
	}

	// ⑤ BeginRun 后：running=true 且 online=true（running 短路，哪怕窗口极小）；
	//    EndRun 后双双回落（配平 + 仍超窗 → online=false）。
	h.disc.BeginRun(sid)
	row = sessionRowOf(t, h, sid)
	if !row.Running || !row.Online {
		t.Fatalf("BeginRun 后应 running=true 且 online=true（running 短路）: %+v", row)
	}
	h.disc.EndRun(sid)
	row = sessionRowOf(t, h, sid)
	if row.Running || row.Online {
		t.Fatalf("EndRun 后应回落（running=false、超窗 online=false）: %+v", row)
	}
}

// mustSessionRows 取全表行（断言辅助，失败即中止）。
func mustSessionRows(t *testing.T, h *Hub) []SessionRow {
	t.Helper()
	rows, err := h.Sessions(context.Background())
	if err != nil {
		t.Fatalf("Sessions: %v", err)
	}
	return rows
}

// ---------------------------------------------------------------------------
// ② ?online= 过滤
// ---------------------------------------------------------------------------

// TestSessionsOnlineQueryFilter：GET /api/sessions?online= 的四种取值、缺省全库、
// 非法值 400 bad_online（过滤发生在补列之后）。
func TestSessionsOnlineQueryFilter(t *testing.T) {
	h := newOnlineTestHub(t)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sidA := runOnlinePrompt(t, h, "A")
	sidB := runOnlinePrompt(t, h, "B")

	// 缺省 = 全库（两行都在线：事件新鲜 + 默认 15m 窗）。
	all := getSessionRows(t, srv.URL+"/api/sessions")
	if len(all) != 2 {
		t.Fatalf("缺省应返回全库 2 行，得到 %d: %+v", len(all), all)
	}

	// 窗口收到极小（**经配置面**走单源同步：UpdateOnlineTable → syncActivityWindow）→
	// 全体超窗；BeginRun(A) → 只有 A 在线（running 短路）。
	near := nearWindow.String() // "1ns"
	if _, err := h.UpdateOnlineTable(loop.OnlineTableUpdate{ActivityWindow: &near}); err != nil {
		t.Fatalf("UpdateOnlineTable(activity_window): %v", err)
	}
	h.disc.BeginRun(sidA)
	defer h.disc.EndRun(sidA)

	onlineTrue := sessionIDsOf(getSessionRows(t, srv.URL+"/api/sessions?online=true"))
	if len(onlineTrue) != 1 || !onlineTrue[string(sidA)] {
		t.Fatalf("?online=true 应只剩在跑的 A: %v", onlineTrue)
	}
	onlineOne := sessionIDsOf(getSessionRows(t, srv.URL+"/api/sessions?online=1"))
	if len(onlineOne) != 1 || !onlineOne[string(sidA)] {
		t.Fatalf("?online=1 应与 true 同口径（只留在线）: %v", onlineOne)
	}
	onlineFalse := sessionIDsOf(getSessionRows(t, srv.URL+"/api/sessions?online=false"))
	if len(onlineFalse) != 1 || !onlineFalse[string(sidB)] {
		t.Fatalf("?online=false 应只剩超窗的 B: %v", onlineFalse)
	}
	onlineZero := sessionIDsOf(getSessionRows(t, srv.URL+"/api/sessions?online=0"))
	if len(onlineZero) != 1 || !onlineZero[string(sidB)] {
		t.Fatalf("?online=0 应与 false 同口径（只留非在线）: %v", onlineZero)
	}
	// 缺省仍全库（A 在线、B 超窗都回来）。
	if got := getSessionRows(t, srv.URL+"/api/sessions"); len(got) != 2 {
		t.Fatalf("缺省 online 参数应返回全库 2 行，得到 %d: %+v", len(got), got)
	}

	// 非法值 → 400 bad_online（错误体是 TransportError JSON）。
	code, body := getJSONRaw(t, srv.URL+"/api/sessions?online=bogus")
	if code != http.StatusBadRequest {
		t.Fatalf("?online=bogus 应 400，得到 %d（body=%s）", code, body)
	}
	var te contracts.TransportError
	if err := json.Unmarshal([]byte(body), &te); err != nil {
		t.Fatalf("解错误体失败: %v（body=%s）", err, body)
	}
	if te.Code != "bad_online" {
		t.Fatalf("错误码应是 bad_online，得到 %q（body=%s）", te.Code, body)
	}
	// P2-6：**显式给了空值**（?online= / ?online）→ 同样 400 bad_online。
	// "给了参数但值非法"不该被当成"没给参数"（缺省才 = 全库）。
	for _, q := range []string{"?online=", "?online"} {
		code, body := getJSONRaw(t, srv.URL+"/api/sessions"+q)
		if code != http.StatusBadRequest {
			t.Fatalf("%s 应 400（显式空值不是缺省），得到 %d（body=%s）", q, code, body)
		}
		var te contracts.TransportError
		if err := json.Unmarshal([]byte(body), &te); err != nil {
			t.Fatalf("解错误体失败: %v（body=%s）", err, body)
		}
		if te.Code != "bad_online" {
			t.Fatalf("%s 的错误码应是 bad_online，得到 %q（body=%s）", q, te.Code, body)
		}
	}
}

// ---------------------------------------------------------------------------
// ③ RunPrompt 的 BeginRun / EndRun
// ---------------------------------------------------------------------------

// observingProvider 每次 provider 调用前触发 observe 钩子（RunPrompt 的 BeginRun 在
// lp.Run **之前** → 钩子里 Sessions() 必须已经看到 Running=true）。
type observingProvider struct {
	mu      sync.Mutex
	observe func()
	calls   int
}

func (p *observingProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu.Lock()
	p.calls++
	fn := p.observe
	p.mu.Unlock()
	if fn != nil {
		fn()
	}
	if emit != nil {
		emit(core.ProviderEvent{Kind: "done"})
	}
	return core.ModelResult{Text: "ok", Usage: core.Usage{InputTokens: 1, OutputTokens: 1}}, nil
}

// TestRunPromptRunningFlag：RunPrompt 打点 BeginRun/EndRun——跑轮期间 Running=true
// （且 running 短路 → online=true），跑完 EndRun 配平 → Running=false、事件新鲜 → online=true。
func TestRunPromptRunningFlag(t *testing.T) {
	prov := &observingProvider{}
	h := newOnlineTestHub(t, func(c *Config) {
		c.Provider = prov
	})
	sid, err := h.CreateSession(SessionSpec{})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	var (
		mu       sync.Mutex
		seen     bool
		running  bool
		online   bool
		snapErr  error
		callSeen int
	)
	prov.observe = func() {
		rows, err := h.Sessions(context.Background())
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			snapErr = err
			return
		}
		callSeen++
		for _, r := range rows {
			if r.SessionID == string(sid) {
				seen, running, online = true, r.Running, r.Online
			}
		}
	}
	if _, err := h.RunPrompt(context.Background(), sid, "跑一轮"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if snapErr != nil {
		t.Fatalf("跑轮期间 Sessions: %v", snapErr)
	}
	if callSeen == 0 || !seen {
		t.Fatalf("provider 被调用时该 session 应已在在线表（user.prompt 先落库）: callSeen=%d seen=%v", callSeen, seen)
	}
	if !running {
		t.Fatal("Run 期间 Running 应为 true（BeginRun 在 lp.Run 之前打点）")
	}
	if !online {
		t.Fatal("Run 期间 running 短路 → online 应为 true")
	}
	// 跑完：EndRun 已配平 + 事件新鲜（默认窗）→ Running=false、online=true。
	row := sessionRowOf(t, h, sid)
	if row.Running {
		t.Fatalf("跑完后 Running 应为 false（EndRun 配平）: %+v", row)
	}
	if !row.Online {
		t.Fatalf("跑完后事件新鲜 → online 应为 true: %+v", row)
	}
}

// ---------------------------------------------------------------------------
// ④ 活跃窗口单源同步（构造 + 运行期 + 配置面回读 + 启动校验 + TOML 解析）
// ---------------------------------------------------------------------------

// TestActivityWindowSingleSource：activity_window 是唯一旋钮——起动 Config 值构造时
// 同步进发现表（syncActivityWindow），运行期 Update 同步生效（见
// TestSessionsOnlineQueryFilter 的窗口段），非法起动值启动即拒，TOML 键认得。
func TestActivityWindowSingleSource(t *testing.T) {
	// ① 起动配置：Config.OnlineTableActivityWindow = "1ns" → 构造路径同步进发现表
	//（onlineCfg.Update → h.syncActivityWindow），刚有事件的 session 也判超窗。
	h := newOnlineTestHub(t, func(c *Config) {
		c.OnlineTableActivityWindow = "1ns"
	})
	sid := runOnlinePrompt(t, h, "刚有事件")
	row := sessionRowOf(t, h, sid)
	if row.Online {
		t.Fatalf("起动窗口=1ns：刚有事件的 session 也该超窗（online=false）: %+v", row)
	}
	// 配置面回读与发现表同值（一个 N）。
	if got := h.OnlineTableView().ActivityWindow; got != "1ns" {
		t.Fatalf("配置面 activity_window = %q, want 1ns", got)
	}
	// BeginRun → running 短路又 online（判定面拿到的窗口与配置面同一个）。
	h.disc.BeginRun(sid)
	if row := sessionRowOf(t, h, sid); !row.Running || !row.Online {
		t.Fatalf("BeginRun 后应 running 且 online: %+v", row)
	}
	h.disc.EndRun(sid)

	// ② 非法起动值 → 启动即拒（validate 的既有风格：点名字段 + 给出合法形态）。
	if _, err := New(Config{
		DBPath:                    filepath.Join(t.TempDir(), "x.db"),
		Model:                     "m",
		OnlineTableActivityWindow: "bogus",
	}); err == nil || !strings.Contains(err.Error(), "OnlineTableActivityWindow") {
		t.Fatalf("非法 activity_window 应启动即拒并点名字段，得到 err=%v", err)
	}
	// 负时长同拒（与 loop.OnlineTableUpdate 的非负口径一致）。
	if _, err := New(Config{
		DBPath:                    filepath.Join(t.TempDir(), "x.db"),
		Model:                     "m",
		OnlineTableActivityWindow: "-5m",
	}); err == nil || !strings.Contains(err.Error(), "OnlineTableActivityWindow") {
		t.Fatalf("负 activity_window 应启动即拒，得到 err=%v", err)
	}

	// ③ TOML 解析：[online_table] activity_window = "2h"（缺省 = 不写 = 默认）。
	p := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(p, []byte("[online_table]\nactivity_window = \"2h\"\n"), 0o600); err != nil {
		t.Fatalf("写配置文件: %v", err)
	}
	fc, err := LoadFileConfig(p)
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if fc.OnlineTable.ActivityWindow != "2h" {
		t.Fatalf("TOML activity_window = %q, want 2h", fc.OnlineTable.ActivityWindow)
	}
	// 缺省 = 不写：不带该键的文件也照常解析（空串 → onlineTableOverrides 不覆盖 → 默认）。
	p2 := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(p2, []byte("[online_table]\nrows = 7\n"), 0o600); err != nil {
		t.Fatalf("写配置文件: %v", err)
	}
	fc2, err := LoadFileConfig(p2)
	if err != nil {
		t.Fatalf("LoadFileConfig(缺省): %v", err)
	}
	if fc2.OnlineTable.ActivityWindow != "" {
		t.Fatalf("没写 activity_window 时应为空串（= 用默认），得到 %q", fc2.OnlineTable.ActivityWindow)
	}
	defCfg := Config{OnlineTableActivityWindow: fc2.OnlineTable.ActivityWindow}
	if got := defCfg.onlineTableOverrides().ActivityWindow; got != nil {
		t.Fatalf("空串不该产生覆盖项: %q", *got)
	}
}

// ---------------------------------------------------------------------------
// ⑤ 过滤在截断**之前**（P1-2）
// ---------------------------------------------------------------------------

// TestSessionsOnlineFilterBeforeTruncation（P1-2）：?online= 的过滤发生在**截断之前**——
// MaxSessionRows=1、两条 session（**在线的那条 seq 小**、离线的那条 seq 大）→
// ?online=true 仍返回在线的那条。
//
// 旧顺序（投影层先按 LastSeq 降序截断 → HTTP 层再过滤）会返回 []：在线的那条排在
// 上限之外，被整段截掉，再怎么筛也筛不回来（critic 用 MaxSessionRows=1 复现）。
//
// 夹具手法（不注入时钟）：窗口收到极小 → 全体超窗；BeginRun(A) → A 靠 running 短路
// 在线；再跑一轮造出 seq 更大的 B（跑完不 running + 超窗 → 离线）。
func TestSessionsOnlineFilterBeforeTruncation(t *testing.T) {
	h := newOnlineTestHub(t, func(c *Config) { c.MaxSessionRows = 1 })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// ① A 先跑一轮（seq 小）。
	sidA := runOnlinePrompt(t, h, "A 的任务")
	// ② 窗口收到极小 → 全体超窗（都不在线）。
	h.disc.SetActivityWindow(nearWindow)
	// ③ A 本轮在跑 → running 短路 → 在线。
	h.disc.BeginRun(sidA)
	defer h.disc.EndRun(sidA)
	// ④ B 后跑一轮 → seq 更大；跑完不 running + 超窗 → 离线。
	sidB := runOnlinePrompt(t, h, "B 的任务")

	// 前提：A（在线）的 seq 比 B（离线）小 → 无过滤时截断只会留下 B。
	//
	// 取**不过限**的原始行（h.proj.rows(wm, 0)）：Sessions() 受 MaxSessionRows=1 限制，
	// 只会回一行（B）——用它查 A 会扑空。online 判定看发现表（判定源在那儿）。
	raw := h.proj.rows(h.proj.scanWatermark(), 0)
	var lastA, lastB int64
	for _, r := range raw {
		switch r.SessionID {
		case string(sidA):
			lastA = r.LastSeq
		case string(sidB):
			lastB = r.LastSeq
		}
	}
	if lastA == 0 || lastB == 0 {
		t.Fatalf("夹具前提破了：A/B 应都在投影里（A=%d, B=%d）", lastA, lastB)
	}
	if lastA >= lastB {
		t.Fatalf("夹具前提破了：A.LastSeq=%d 应 < B.LastSeq=%d", lastA, lastB)
	}
	rowA, _ := h.disc.Session(sidA)
	rowB, _ := h.disc.Session(sidB)
	if !rowA.Online || rowB.Online {
		t.Fatalf("夹具前提破了：A.online=%v（want true）、B.online=%v（want false）", rowA.Online, rowB.Online)
	}

	// 缺省 = 全库**截断后**的一行（seq 最大的 B）——截断口径本身没变。
	all := getSessionRows(t, srv.URL+"/api/sessions")
	if len(all) != 1 || all[0].SessionID != string(sidB) {
		t.Fatalf("缺省应返回截断后的 1 行（seq 最大的 B），得到 %+v", all)
	}
	// ?online=true → 过滤在截断**之前** → 仍返回 A（在线的那条）。
	onlineTrue := getSessionRows(t, srv.URL+"/api/sessions?online=true")
	if len(onlineTrue) != 1 || onlineTrue[0].SessionID != string(sidA) {
		t.Fatalf("?online=true 应返回在线的 A（过滤在截断之前），得到 %+v", onlineTrue)
	}
	// ?online=false → 离线的那条（B）。
	onlineFalse := getSessionRows(t, srv.URL+"/api/sessions?online=false")
	if len(onlineFalse) != 1 || onlineFalse[0].SessionID != string(sidB) {
		t.Fatalf("?online=false 应返回离线的 B，得到 %+v", onlineFalse)
	}
}
