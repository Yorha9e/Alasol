package host_test

// 缺口 G-b「默认接管（懒接管）」的测试面（对 host/takeover.go）：
//
//	① 历史 session 懒接管：有事件但未托管（重启失管）→ POST /prompt → 202 +
//	   hostsSession 翻真 + 事件流里出现 hub.alert（metric=session.takeover）留痕；
//	② Claim 冲突拒：预登记给另一个 hub → POST /prompt → 409 host_conflict +
//	   **不留痕** + **不建 loop**（G25 唯一拒绝线）；
//	③ 照常不留痕：已托管再 POST → 202 且无新的 takeover hub.alert；
//	④ WS 同源：command.submit 走同一个 ensureHosted——冲突回错误帧（同码同文案）、
//	   未托管接管成功（command.accepted + 留痕）。
//
// 附带钉住挂账的一半真相：**同进程**的发现表撞得出 Claim 冲突（②④ 用预登记模拟
// "另一个 hub 持有"）；**跨进程**看不见别家登记（① 里第二个宿主对旧 session 毫无
// 阻力地接管）——这是"一库一 hub 是预期拓扑"前提下的已知边界，见 takeover.go 文件头。

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/workflow/tower"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/transport"
)

// takeoverAlertEvents 读事件流里 metric=session.takeover 的 hub.alert 事件（G-b 懒接管留痕）。
//
// 走 GET /api/events 全量窗口（limit=10000 = 硬上限）：hub.alert 是 hub 级事件
// （session_id = 从库路径派生的伪 id，hubIdentityIDs），**不**按目标 session 过滤。
// 留痕由 ensureHosted **同步**落库（202 / command.accepted 返回之前）→ 本函数不轮询，
// 直接读就是终值——这让"不留痕"的断言也是确定的（0 就是 0，没有等待窗口）。
func takeoverAlertEvents(t *testing.T, base string) []contracts.Event {
	t.Helper()
	var view struct {
		Events []contracts.Event `json:"events"`
	}
	if code := getJSON(t, base+"/api/events?limit=10000", &view); code != http.StatusOK {
		t.Fatalf("事件查询状态码 = %d", code)
	}
	out := []contracts.Event{}
	for _, ev := range view.Events {
		if ev.Kind != "hub.alert" {
			continue
		}
		var p contracts.HubAlertPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("hub.alert 载荷解不开: %v", err)
		}
		if p.Metric == "session.takeover" {
			out = append(out, ev)
		}
	}
	return out
}

// takeoverMessage 取一条接管留痕的正文（断言用）。
func takeoverMessage(t *testing.T, ev contracts.Event) string {
	t.Helper()
	var p contracts.HubAlertPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("hub.alert 载荷解不开: %v", err)
	}
	return p.Message
}

// sessionHostable 查该 session 在 /api/sessions 在线表里的 hostable 列
// （= hostsSession 的对外投影：本进程托管才可发 prompt，G12）。
// 只对**已有事件**的 session 用（投影按事件流出行；新 session 的首轮事件是异步落的）。
func sessionHostable(t *testing.T, h *host.Hub, sid contracts.Ulid) bool {
	t.Helper()
	rows, err := h.Sessions(context.Background())
	if err != nil {
		t.Fatalf("Sessions: %v", err)
	}
	for _, r := range rows {
		if r.SessionID == string(sid) {
			return r.Hostable
		}
	}
	t.Fatalf("在线表里找不到 session %s（它必须已有事件）", sid)
	return false
}

// TestPromptLazyTakeoverHistorySession（G-b ① 历史 session 懒接管）：
// 第一个宿主跑出历史事件后关掉（重启失管），同一个库重开的 hub 对它发 prompt →
// 懒接管成功：202 + hostable 翻真 + 宿主登记为本 hub + 一条 session.takeover 留痕
// （正文带 session id、hub id、"懒接管/G-b"）；历史事件仍在（历史进模型不用新做）；
// 已托管后再发 → 照常 202 且**不再留痕**。
func TestPromptLazyTakeoverHistorySession(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "events.db")
	// 每次 New 造一个新的 provider 实例（与 hub_test 的 reopen 夹具同款纪律）。
	newCfg := func() host.Config {
		return host.Config{
			DBPath: db, Model: "scripted", System: "takeover", MaxTurns: 4,
			Allow:    []string{"read"},
			Provider: &textOnlyProvider{text: "在"},
			CWD:      dir,
		}
	}

	// 第一个宿主：跑一轮历史（事件 + Claim 给它），然后关掉（模拟崩溃/重启失管）。
	h1, err := host.New(newCfg())
	if err != nil {
		t.Fatalf("host.New #1: %v", err)
	}
	t.Cleanup(func() { _ = h1.Close() })
	sid := host.NewSession()
	if _, err := h1.RunPrompt(context.Background(), sid, "历史轮"); err != nil {
		t.Fatalf("历史 RunPrompt: %v", err)
	}
	if err := h1.Close(); err != nil {
		t.Fatalf("关第一个宿主: %v", err)
	}

	// 同一个库重开：loops 空 → 不托管；宿主登记是**进程本地**的 → 也没有旧登记
	//（这正是懒接管该发生、也**拦不住跨进程冲突**的挂账前提，见 takeover.go 文件头）。
	h2, err := host.New(newCfg())
	if err != nil {
		t.Fatalf("host.New #2: %v", err)
	}
	t.Cleanup(func() { _ = h2.Close() })
	if hostID, ok := h2.Discovery().HostOf(sid); ok {
		t.Fatalf("重启后不该残留旧宿主登记（进程本地状态），得到 %q", hostID)
	}
	srv := httptest.NewServer(h2.Handler())
	defer srv.Close()
	if sessionHostable(t, h2, sid) {
		t.Fatalf("重启失管后 hostable 应为 false（loops 空），session=%s", sid)
	}

	// POST /prompt → 懒接管 → 202（受理回执形状不变）。
	var acc struct {
		SessionID string `json:"session_id"`
		Status    string `json:"status"`
	}
	if code := postJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]string{"prompt": "继续"}, &acc); code != http.StatusAccepted {
		t.Fatalf("历史 session 的 prompt 状态码 = %d，want 202（G-b 懒接管）", code)
	}
	if acc.SessionID != string(sid) || acc.Status != "running" {
		t.Fatalf("受理回执不符: %+v", acc)
	}

	// hostsSession 变 true：hostable 翻真 + 宿主登记 = 本 hub。
	if !sessionHostable(t, h2, sid) {
		t.Fatalf("接管后 hostable 应为 true（loop 已建），session=%s", sid)
	}
	if hostID, ok := h2.Discovery().HostOf(sid); !ok || hostID != h2.ID() {
		t.Fatalf("接管后宿主登记 = (%q, %v)，want (%q, true)", hostID, ok, h2.ID())
	}

	// 留痕：一条 hub.alert（metric=session.takeover），正文带 session id + hub id + 懒接管/G-b。
	alerts := takeoverAlertEvents(t, srv.URL)
	if len(alerts) != 1 {
		t.Fatalf("接管留痕条数 = %d，want 1", len(alerts))
	}
	msg := takeoverMessage(t, alerts[0])
	for _, want := range []string{string(sid), h2.ID(), "懒接管", "G-b"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("留痕正文应含 %q，得到 %q", want, msg)
		}
	}

	// 历史进事件流还在（懒接管不重写历史；RunPrompt 每轮折叠事件流带历史）。
	var view struct {
		Events []contracts.Event `json:"events"`
	}
	if code := getJSON(t, srv.URL+"/api/events?session_id="+string(sid), &view); code != http.StatusOK {
		t.Fatalf("事件查询状态码 = %d", code)
	}
	if len(view.Events) == 0 {
		t.Fatal("历史 session 的事件该还在（接管不抹历史）")
	}

	// 已托管后再发 → 照常 202 且不再留痕（"照常不留痕"的另一侧）。
	if code := postJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]string{"prompt": "继续二"}, &acc); code != http.StatusAccepted {
		t.Fatalf("第二次 prompt 状态码 = %d，want 202", code)
	}
	if n := len(takeoverAlertEvents(t, srv.URL)); n != 1 {
		t.Fatalf("已托管路径不该再留痕，接管留痕条数 = %d，want 1", n)
	}
}

// TestPromptTakeoverClaimConflictRejected（G-b ② Claim 冲突 = 唯一拒绝线）：
// 发现表已把 session 登记给另一个 hub（照 hub_test.TestLoopForClaimConflictFailClose
// 的预登记手法：同表先 Claim 给异 hub id——跨进程看不见别家登记，只能这样模拟）→
// POST /prompt → 409 host_conflict（G25 防双写），且**不留痕**、**不建 loop**
// （宿主登记未动、第二次仍 409、该 session 零事件）。
func TestPromptTakeoverClaimConflictRejected(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) { c.Provider = &textOnlyProvider{text: "在"} })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if err := h.Discovery().Claim(sid, "hub-from-another-process"); err != nil {
		t.Fatalf("预登记异 hub: %v", err)
	}

	var te contracts.TransportError
	if code := postJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]string{"prompt": "你好"}, &te); code != http.StatusConflict {
		t.Fatalf("冲突 session 的 prompt 状态码 = %d，want 409", code)
	}
	if te.Code != "host_conflict" {
		t.Fatalf("错误码 = %q，want host_conflict", te.Code)
	}
	if !strings.Contains(te.Message, "拒绝接管") || !strings.Contains(te.Message, "G25") {
		t.Fatalf("冲突文案应点名拒绝接管 + G25，得到 %q", te.Message)
	}

	// 不留痕：接管没发生过（留痕只在 Claim 成功之后）。
	if n := len(takeoverAlertEvents(t, srv.URL)); n != 0 {
		t.Fatalf("冲突不该留痕，接管留痕条数 = %d，want 0", n)
	}
	// 不建 loop：宿主登记没动（fail-close 不改现状）。
	if hostID, _ := h.Discovery().HostOf(sid); hostID != "hub-from-another-process" {
		t.Fatalf("冲突后宿主登记不该被改动，得到 %q", hostID)
	}
	// 不建 loop（第二个证据）：建了 loop 就托管了，第二次 POST 会 202 而不是 409。
	if code := postJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]string{"prompt": "再试"}, &te); code != http.StatusConflict {
		t.Fatalf("第二次 prompt 状态码 = %d，want 409（冲突路径不得建 loop）", code)
	}
	// 也没跑：loopFor 冲突在 Claim 处回滚，Claim 之前没往事件流写过任何东西。
	var view struct {
		Events []contracts.Event `json:"events"`
	}
	if code := getJSON(t, srv.URL+"/api/events?session_id="+string(sid), &view); code != http.StatusOK {
		t.Fatalf("事件查询状态码 = %d", code)
	}
	if len(view.Events) != 0 {
		t.Fatalf("被拒的 session 不该产生任何事件，得到 %d 条", len(view.Events))
	}
}

// TestPromptHostedNoTakeoverAlert（G-b ③ 照常不留痕）：已托管的 session 再发
// prompt → 202，且事件流里**没有** takeover hub.alert（留痕只属于"真接管"）。
func TestPromptHostedNoTakeoverAlert(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) { c.Provider = &textOnlyProvider{text: "在"} })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := warmupSession(t, h) // 跑过一轮 = 本进程托管
	if n := len(takeoverAlertEvents(t, srv.URL)); n != 0 {
		t.Fatalf("预热（RunPrompt 托管）不该产生接管留痕，得到 %d 条", n)
	}
	if code := postJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]string{"prompt": "你好"}, nil); code != http.StatusAccepted {
		t.Fatalf("已托管 session 的 prompt 状态码 = %d，want 202", code)
	}
	if n := len(takeoverAlertEvents(t, srv.URL)); n != 0 {
		t.Fatalf("已托管路径不该留痕，接管留痕条数 = %d，want 0", n)
	}
}

// TestWSSubmitTakeoverSameSource（G-b ④ WS 同源）：command.submit 与 HTTP prompt 走
// 同一个 ensureHosted——
//
//	① Claim 冲突 → kind=error 帧（code=host_conflict，与 HTTP 409 同码同文案）+ 不留痕；
//	② 未托管 → 懒接管成功 → command.accepted（不回错误）+ 留痕 + 宿主登记翻成本 hub。
func TestWSSubmitTakeoverSameSource(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) { c.Provider = &textOnlyProvider{text: "在"} })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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

	// ① Claim 冲突 → kind=error 帧，code=host_conflict（与 HTTP 409 同源同码）。
	foreign := host.NewSession()
	if err := h.Discovery().Claim(foreign, "hub-from-another-process"); err != nil {
		t.Fatalf("预登记异 hub: %v", err)
	}
	msg, err := transport.NewMessage(transport.KindCommandSubmit, contracts.CommandSubmit{
		SessionID: &foreign, Prompt: "读一下",
	})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if err := c.SendContext(ctx, msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	m := waitWSKind(t, c, transport.KindError, 10*time.Second)
	var te contracts.TransportError
	if err := json.Unmarshal(m.Payload, &te); err != nil {
		t.Fatalf("error payload: %v", err)
	}
	if te.Code != "host_conflict" {
		t.Fatalf("错误码 = %q，want host_conflict（与 HTTP 409 同码）", te.Code)
	}
	if !strings.Contains(te.Message, "拒绝接管") {
		t.Fatalf("错误文案应点名拒绝接管，得到 %q", te.Message)
	}
	if n := len(takeoverAlertEvents(t, srv.URL)); n != 0 {
		t.Fatalf("WS 冲突不该留痕，接管留痕条数 = %d，want 0", n)
	}
	if hostID, _ := h.Discovery().HostOf(foreign); hostID != "hub-from-another-process" {
		t.Fatalf("WS 冲突后宿主登记不该被改动，得到 %q", hostID)
	}

	// ② 未托管 → 懒接管成功 → command.accepted（**不**回错误帧）。
	sid := host.NewSession()
	msg2, err := transport.NewMessage(transport.KindCommandSubmit, contracts.CommandSubmit{
		SessionID: &sid, Prompt: "读一下",
	})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if err := c.SendContext(ctx, msg2); err != nil {
		t.Fatalf("send: %v", err)
	}
	m2 := waitWSKind(t, c, transport.KindCommandAccepted, 10*time.Second)
	var ca contracts.CommandAccepted
	if err := json.Unmarshal(m2.Payload, &ca); err != nil {
		t.Fatalf("command.accepted payload: %v", err)
	}
	if ca.SessionID != sid || ca.Status != "running" {
		t.Fatalf("command.accepted 不符: %+v", ca)
	}
	// 受理回执在 ensureHosted **之后**回 → 留痕此刻已落库（不轮询）。
	if n := len(takeoverAlertEvents(t, srv.URL)); n != 1 {
		t.Fatalf("WS 接管留痕条数 = %d，want 1", n)
	}
	if hostID, ok := h.Discovery().HostOf(sid); !ok || hostID != h.ID() {
		t.Fatalf("WS 接管后宿主登记 = (%q, %v)，want (%q, true)", hostID, ok, h.ID())
	}
}

// ---------------------------------------------------------------------------
// P0-1：Claim / 发布竞态（先拿托管权、后发布）
// ---------------------------------------------------------------------------

// postPromptConcurrent 发一个 POST /prompt 并返回状态码（**不碰 testing.T**——并发
// goroutine 里调 t.Fatalf 是错用法：失败信息会丢、Goexit 还会打断 wg 的等待）。
func postPromptConcurrent(url, prompt string) (int, error) {
	body, err := json.Marshal(map[string]string{"prompt": prompt})
	if err != nil {
		return 0, err
	}
	resp, err := http.Post(url, "application/json", strings.NewReader(string(body)))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

// TestPromptTakeoverConcurrentConflictAllRejected（P0-1）：并发 N 个 POST /prompt 打到
// "别的 hub 托管"的 session（照 TestLoopForClaimConflictFailClose 的预登记手法）→
// **全部 409**、**零事件写入**、**零接管留痕**。
//
// 复现 critic 的并发场景：旧的 loopFor 是"建 loop → 写进 h.loops → Claim → 冲突才
// 回滚"四步不原子——并发时一个请求 409，另一个却从**缓存**拿到 loop → 202 并双写事件
// （G25 破），还落一条假"接管成功"留痕。
//
// 修法（P0-1）：**先拿托管权、后发布**——Claim 失败 = 根本没发布，谁都拿不到 loop
// → 一律 409（唯一拒绝线）。
func TestPromptTakeoverConcurrentConflictAllRejected(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) { c.Provider = &textOnlyProvider{text: "在"} })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if err := h.Discovery().Claim(sid, "hub-from-another-process"); err != nil {
		t.Fatalf("预登记异 hub: %v", err)
	}

	const n = 24
	codes := make([]int, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i], errs[i] = postPromptConcurrent(srv.URL+"/api/sessions/"+string(sid)+"/prompt", "并发接管")
		}(i)
	}
	wg.Wait()
	for i := range codes {
		if errs[i] != nil {
			t.Fatalf("第 %d 个请求失败: %v", i, errs[i])
		}
		if codes[i] != http.StatusConflict {
			t.Fatalf("第 %d 个请求状态码 = %d，want 409（Claim 冲突 = 唯一拒绝线；"+
				"不得有人从缓存拿到 loop 去跑——那是双写事件 + 假留痕）", i, codes[i])
		}
	}
	// 零事件：被拒的 session 不该产生任何事件（Claim 失败在**发布之前**，loop 都没建）。
	var view struct {
		Events []contracts.Event `json:"events"`
	}
	if code := getJSON(t, srv.URL+"/api/events?session_id="+string(sid), &view); code != http.StatusOK {
		t.Fatalf("事件查询状态码 = %d", code)
	}
	if len(view.Events) != 0 {
		t.Fatalf("被拒的 session 不该产生任何事件，得到 %d 条", len(view.Events))
	}
	// 零留痕：接管没发生过（留痕只在 Claim 成功**且**真发布之后）。
	if got := len(takeoverAlertEvents(t, srv.URL)); got != 0 {
		t.Fatalf("冲突不该留痕，接管留痕条数 = %d，want 0", got)
	}
	// 宿主登记没动（fail-close 不改现状）。
	if hostID, _ := h.Discovery().HostOf(sid); hostID != "hub-from-another-process" {
		t.Fatalf("冲突后宿主登记不该被改动，得到 %q", hostID)
	}
}

// TestPromptTakeoverConcurrentSameHubOneTrace（P0-1）：**同一个 hub** 并发接管一个正常
// session（没被别的 hub 登记）→ 全部 202，且**恰 1 条**接管留痕。
//
// 留痕条件是"接管前 !hostsSession && created"：并发时只有一个 goroutine 真正建成并
// 发布了 loop（loopFor 的双检锁），其余拿到的是**已发布**的那份（created=false）——
// 于是只留一条痕（复盘时不会读出 N 次没发生过的接管）。
func TestPromptTakeoverConcurrentSameHubOneTrace(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) { c.Provider = &textOnlyProvider{text: "在"} })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	const n = 8
	codes := make([]int, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i], errs[i] = postPromptConcurrent(srv.URL+"/api/sessions/"+string(sid)+"/prompt", "并发接管")
		}(i)
	}
	wg.Wait()
	for i := range codes {
		if errs[i] != nil {
			t.Fatalf("第 %d 个请求失败: %v", i, errs[i])
		}
		if codes[i] != http.StatusAccepted {
			t.Fatalf("第 %d 个请求状态码 = %d，want 202（同 hub 接管不该冲突）", i, codes[i])
		}
	}
	// 恰 1 条留痕（留痕同步落库：ensureHosted 在 202 返回**之前**落完 → 直接读是终值）。
	if got := len(takeoverAlertEvents(t, srv.URL)); got != 1 {
		t.Fatalf("并发接管同一 session 的留痕条数 = %d，want 1（created 判定：只有一个真接管）", got)
	}
}

// TestPromptLoopRebuildNoTakeoverTrace（F3）：**loop 缓存被清掉后的重建不是接管**——
// 托管权从未易主 → 不该落"懒接管"留痕（假阳性）。
//
// 复现 critic 的场景：ActivateWorkflow 会 delete h.loops（system prompt 要变 → 失效
// loop 缓存）→ 再 POST /prompt → loop 被重建。旧判据（"接管前 !hostsSession && created"）
// 会把这次**重建**误报成一次"懒接管"（托管权其实一直是本 hub）。
//
// 新判据：**发现表的宿主登记**（接管前 HostOf 为空或 ≠ 本 hub id = 真易主）。
func TestPromptLoopRebuildNoTakeoverTrace(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) { c.Provider = &textOnlyProvider{text: "在"} })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "第一轮"); err != nil {
		t.Fatalf("预热 RunPrompt: %v", err)
	}
	// 预热是本进程自己跑的（不是接管）→ 零留痕。
	if n := len(takeoverAlertEvents(t, srv.URL)); n != 0 {
		t.Fatalf("预热不该留痕，得到 %d 条", n)
	}

	// ActivateWorkflow → 失效 loop 缓存（h.loops 被删；托管权**没**易主）。
	if _, err := h.ActivateWorkflow(context.Background(), sid, tower.TowerID); err != nil {
		t.Fatalf("ActivateWorkflow: %v", err)
	}

	// 再 POST /prompt → 202（loop 重建，照常受理）。
	if code := postJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]string{"prompt": "第二轮"}, nil); code != http.StatusAccepted {
		t.Fatalf("重建后的 prompt 状态码 = %d，want 202", code)
	}

	// **零**新增接管留痕：宿主登记仍是本 hub → 托管权没易主 → 不是接管。
	if n := len(takeoverAlertEvents(t, srv.URL)); n != 0 {
		t.Fatalf("loop 重建不该落接管留痕（假阳性），得到 %d 条", n)
	}
	// 宿主登记没变（本 hub 一直托管它）。
	if hostID, ok := h.Discovery().HostOf(sid); !ok || hostID != h.ID() {
		t.Fatalf("重建后宿主登记 = (%q, %v)，want (%q, true)", hostID, ok, h.ID())
	}
}
