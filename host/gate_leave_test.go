package host_test

// G15 验收（host 层）：gate.left 管理面——leave 幂等、可见性消失/恢复、事件可回放、
// 枚举面形状；外加 G13 的 WS 端到端（model.delta 流事件经 WS 送达 / durable_only 收不到）
// 与 G14 的配置面（reasoning_effort 透传 + 白名单 fail-close）。

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	"github.com/Yorha9e/alasol/runtime/transport"
)

// gateEventCounts 数事件流里某 session 的 gate.joined / gate.left 条数（按 gate 分组）。
//
// 直接读**事实流**（不经过发现表）：断言的是"落了几条事件"这个存储层事实，而不是某个
// 派生视图的状态（派生视图可能因 catchUp 时序落后）。
func gateEventCounts(t *testing.T, dbPath string, sid contracts.Ulid) (joins, lefts map[string]int) {
	t.Helper()
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	evs, err := st.ReadFrom(context.Background(), &sid, 1, 0)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	joins, lefts = map[string]int{}, map[string]int{}
	for _, ev := range evs {
		switch ev.Kind {
		case "gate.joined":
			var p contracts.GateJoinedPayload
			if json.Unmarshal(ev.Payload, &p) == nil {
				joins[p.GateID]++
			}
		case "gate.left":
			var p contracts.GateLeftPayload
			if json.Unmarshal(ev.Payload, &p) == nil {
				lefts[p.GateID]++
			}
		}
	}
	return joins, lefts
}

// ---------------------------------------------------------------------------
// G15：join → leave → 不可见 → rejoin 恢复
// ---------------------------------------------------------------------------

// TestGateLeaveJoinRejoinLifecycle：显式 leave 让可见性消失；幂等；再 join 恢复。
func TestGateLeaveJoinRejoinLifecycle(t *testing.T) {
	dir := plainDir(t, "work")
	dbPath := filepath.Join(t.TempDir(), "events.db")
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = dir
		c.DBPath = dbPath
		c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
	})
	a := runPromptIn(t, h, dir, "A 的任务")
	b := runPromptIn(t, h, dir, "B 的任务")

	const shared = "team:backend"
	for _, sid := range []contracts.Ulid{a, b} {
		if _, err := h.JoinGate(context.Background(), sid, shared); err != nil {
			t.Fatalf("JoinGate(%s): %v", sid, err)
		}
	}
	if got := peerSessions(t, h, a); len(got) != 1 || got[0] != string(b) {
		t.Fatalf("join 后 a 的可见对端 = %v，want [b]", got)
	}

	// a 离开 → 两侧互不可见（共享 gate 没了）。
	view, err := h.LeaveGate(context.Background(), a, shared)
	if err != nil {
		t.Fatalf("LeaveGate(a): %v", err)
	}
	if len(view.Peers) != 0 {
		t.Fatalf("leave 后 a 的可见对端 = %+v，want 空", view.Peers)
	}
	for _, sid := range []contracts.Ulid{a, b} {
		if got := peerSessions(t, h, sid); len(got) != 0 {
			t.Fatalf("a leave 后 %s 仍可见 %v，want 空", sid, got)
		}
	}
	// GatesOf 不再含该 gate（活跃成员关系已终止）。
	if gates := h.Discovery().GatesOf(a); len(gates) != 0 {
		t.Fatalf("a 的 gates = %v，want 空（leave 后）", gates)
	}

	// 幂等：重复 leave 无害（不报错、不落重复事件）。
	for i := 0; i < 3; i++ {
		if _, err := h.LeaveGate(context.Background(), a, shared); err != nil {
			t.Fatalf("重复 LeaveGate #%d: %v", i, err)
		}
	}
	_, lefts := gateEventCounts(t, dbPath, a)
	if lefts[shared] != 1 {
		t.Fatalf("gate.left 落了 %d 条（want 1：重复 leave 不重复落）", lefts[shared])
	}

	// 再 join 恢复可见（重 join 是新一届事件，成员关系恢复）。
	if _, err := h.JoinGate(context.Background(), a, shared); err != nil {
		t.Fatalf("重 JoinGate: %v", err)
	}
	if got := peerSessions(t, h, a); len(got) != 1 || got[0] != string(b) {
		t.Fatalf("重 join 后 a 的可见对端 = %v，want [b]", got)
	}
	// 重 join 落了**新的一条** gate.joined（届数进了事件 id → 没被 store 去重吃掉）。
	joins, _ := gateEventCounts(t, dbPath, a)
	if joins[shared] != 2 {
		t.Fatalf("gate.joined = %d 条，want 2（leave 后的重 join 是新一届）", joins[shared])
	}
}

// TestGateLeftGenerationGuardJoinLeaveTwice：join → leave → join → leave 的**两届**成员关系
// 必须落**两条** gate.left（届数编进了确定性事件 id）。
//
// 为什么要有这条护栏：gate.left 的 Event.ID 是确定性导出的（存储层 ON CONFLICT(id)
// DO NOTHING 兜住重复发布）。**若把届数从 id 里去掉**，两届的 leave 会算出**同一个 id**
// → 第二次 leave 被存储层当重复事件吃掉 → 成员关系**永远恢复不了**（session 名义上
// leave 了，却一直留在网内：它还看得见别人，别人也看得见它）。
//
// 这条用例就是那个护栏：把届数去掉（变异）→ lefts 只有 1 条 + InGate 仍为 true → 红。
func TestGateLeftGenerationGuardJoinLeaveTwice(t *testing.T) {
	dir := plainDir(t, "work")
	dbPath := filepath.Join(t.TempDir(), "events.db")
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = dir
		c.DBPath = dbPath
		c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
	})
	a := runPromptIn(t, h, dir, "A 的任务")
	const gate = "team:twice"

	// 第 1 届：join → leave。
	if _, err := h.JoinGate(context.Background(), a, gate); err != nil {
		t.Fatalf("JoinGate #1: %v", err)
	}
	if !h.Discovery().InGate(a, gate) {
		t.Fatal("join 后不在网内")
	}
	if _, err := h.LeaveGate(context.Background(), a, gate); err != nil {
		t.Fatalf("LeaveGate #1: %v", err)
	}
	if h.Discovery().InGate(a, gate) {
		t.Fatal("第一次 leave 后仍在网内")
	}

	// 第 2 届：join（重 join = 新一届）→ leave。
	if _, err := h.JoinGate(context.Background(), a, gate); err != nil {
		t.Fatalf("JoinGate #2: %v", err)
	}
	if !h.Discovery().InGate(a, gate) {
		t.Fatal("重 join 后不在网内（成员关系没恢复）")
	}
	if _, err := h.LeaveGate(context.Background(), a, gate); err != nil {
		t.Fatalf("LeaveGate #2: %v", err)
	}

	// 事实流：两届各一条 join / left（届数进了事件 id → 没被 store 去重吃掉）。
	joins, lefts := gateEventCounts(t, dbPath, a)
	if joins[gate] != 2 {
		t.Fatalf("gate.joined = %d 条，want 2（两届各一条）", joins[gate])
	}
	if lefts[gate] != 2 {
		t.Fatalf("gate.left = %d 条，want 2（两届各一条；届数丢了第二条会被存储层去重吃掉，成员关系永远恢复不了）", lefts[gate])
	}
	// 第二次 leave 之后确实不在网内（派生视图与事实流一致）。
	if h.Discovery().InGate(a, gate) {
		t.Fatal("第二次 leave 后仍在网内（成员关系没终止）")
	}
	if gates := h.Discovery().GatesOf(a); len(gates) != 0 {
		t.Fatalf("第二次 leave 后 GatesOf = %v，want 空", gates)
	}
}

// TestGateLeaveOnNeverJoinedIsNoop：从没 join 过就 leave → 幂等空操作（不落事件）。
func TestGateLeaveOnNeverJoinedIsNoop(t *testing.T) {
	dir := plainDir(t, "work")
	dbPath := filepath.Join(t.TempDir(), "events.db")
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = dir
		c.DBPath = dbPath
		c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
	})
	a := runPromptIn(t, h, dir, "A 的任务")
	if _, err := h.LeaveGate(context.Background(), a, "team:never"); err != nil {
		t.Fatalf("LeaveGate(未加入过): %v", err)
	}
	_, lefts := gateEventCounts(t, dbPath, a)
	if len(lefts) != 0 {
		t.Fatalf("未加入过的 gate 不该有 leave 事件: %v", lefts)
	}
}

// TestGateLeftEventDurableAndReplayable：gate.left 是 durable 事实（带 seq、可回放）——
// 用一个**全新的发现表**乱序重放事件流，成员关系与库里的一致（崩溃重启口径）。
func TestGateLeftEventDurableAndReplayable(t *testing.T) {
	dir := plainDir(t, "work")
	dbPath := filepath.Join(t.TempDir(), "events.db")
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = dir
		c.DBPath = dbPath
		c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
	})
	a := runPromptIn(t, h, dir, "A 的任务")
	const shared = "team:x"
	if _, err := h.JoinGate(context.Background(), a, shared); err != nil {
		t.Fatalf("JoinGate: %v", err)
	}
	if _, err := h.LeaveGate(context.Background(), a, shared); err != nil {
		t.Fatalf("LeaveGate: %v", err)
	}

	// 事件流里：gate.left 是 durable 且带 seq（F8/G9）。
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close() // Windows：不关句柄 TempDir 清不掉（"文件正被另一进程使用"）
	evs, err := st.ReadFrom(context.Background(), &a, 1, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var sawLeft bool
	for _, ev := range evs {
		if ev.Kind != "gate.left" {
			continue
		}
		sawLeft = true
		if !ev.Durable || ev.Seq == nil {
			t.Fatalf("gate.left 必须 durable 且带 seq: %+v", ev)
		}
		var p contracts.GateLeftPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("gate.left payload: %v", err)
		}
		if p.GateID != shared || p.Source != contracts.GateJoinSourceExplicit {
			t.Fatalf("gate.left payload = %+v，want gate=%s source=explicit", p, shared)
		}
	}
	if !sawLeft {
		t.Fatal("事件流里没有 gate.left")
	}

	// 全新发现表重放（故意倒序）：成员关系 = joined 减 left → 不在网内。
	fresh := discovery.New()
	for i := len(evs) - 1; i >= 0; i-- {
		fresh.Apply(&evs[i])
	}
	if fresh.InGate(a, shared) {
		t.Fatal("重放后 InGate 仍为 true（leave 事实没被折叠）")
	}
	if got := fresh.GatesOf(a); len(got) != 0 {
		t.Fatalf("重放后 GatesOf = %v，want 空", got)
	}
	// gate 定义仍在（成员 0）——"存在过"不因成员走光消失。
	var sawGate bool
	for _, g := range fresh.Gates() {
		if g.GateID == shared {
			sawGate = true
			if len(g.Members) != 0 {
				t.Errorf("重放后 gate %s 成员 = %v，want 空", shared, g.Members)
			}
		}
	}
	if !sawGate {
		t.Fatalf("重放后枚举里没有 gate %s（定义应持久化）", shared)
	}
}

// TestGateLeaveHTTPValidation：POST /api/gates/leave 的入参口径（400/404/405）与 join 一致。
func TestGateLeaveHTTPValidation(t *testing.T) {
	dir := plainDir(t, "work")
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = dir
		c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
	})
	a := runPromptIn(t, h, dir, "A 的任务")
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// ① 坏 body（缺 gate_id）→ 400。
	var te contracts.TransportError
	if code := postJSON(t, srv.URL+"/api/gates/leave", map[string]string{"session_id": string(a)}, &te); code != http.StatusBadRequest {
		t.Fatalf("缺 gate_id 状态码 = %d，want 400", code)
	}
	// ② 坏 gate_id → 400。
	if code := postJSON(t, srv.URL+"/api/gates/leave",
		map[string]string{"session_id": string(a), "gate_id": "bad id"}, &te); code != http.StatusBadRequest {
		t.Fatalf("坏 gate_id 状态码 = %d，want 400", code)
	}
	// ③ 野 session → 404（且不进事件流）。
	wild := host.NewSession()
	if code := postJSON(t, srv.URL+"/api/gates/leave",
		map[string]string{"session_id": string(wild), "gate_id": "team:x"}, &te); code != http.StatusNotFound {
		t.Fatalf("野 session 状态码 = %d，want 404", code)
	}
	// ④ 方法不符 → 405（路径认得：/api/gates/leave 只有 POST）。
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/gates/leave", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/gates/leave: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /api/gates/leave 状态码 = %d，want 405", resp.StatusCode)
	}

	// ⑤ 正常路径：join → leave（200）→ 重复 leave（幂等 200）。
	if _, err := h.JoinGate(context.Background(), a, "team:http"); err != nil {
		t.Fatalf("JoinGate: %v", err)
	}
	var view host.PeersView
	if code := postJSON(t, srv.URL+"/api/gates/leave",
		map[string]string{"session_id": string(a), "gate_id": "team:http"}, &view); code != http.StatusOK {
		t.Fatalf("leave 状态码 = %d，want 200", code)
	}
	if code := postJSON(t, srv.URL+"/api/gates/leave",
		map[string]string{"session_id": string(a), "gate_id": "team:http"}, &view); code != http.StatusOK {
		t.Fatalf("重复 leave 状态码 = %d，want 200（幂等）", code)
	}
}

// TestGatesEnumerationEndpoint：GET /api/gates 的分层口径——
// 全部 gate 的存在性 + 活跃成员数；caller 在网内的 gate 才带成员详情（K11）。
func TestGatesEnumerationEndpoint(t *testing.T) {
	dir := plainDir(t, "work")
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = dir
		c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
	})
	a := runPromptIn(t, h, dir, "A 的任务")
	b := runPromptIn(t, h, dir, "B 的任务")
	const mine, theirs = "team:mine", "team:theirs"
	if _, err := h.JoinGate(context.Background(), a, mine); err != nil {
		t.Fatalf("JoinGate(a): %v", err)
	}
	if _, err := h.JoinGate(context.Background(), b, theirs); err != nil {
		t.Fatalf("JoinGate(b): %v", err)
	}

	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// gateRow 是断言的松散形状（与 host.GateView 的 JSON 面对齐）。
	type gateRow struct {
		GateID      string `json:"gate_id"`
		MemberCount int    `json:"member_count"`
		Joined      bool   `json:"joined"`
		Members     []struct {
			SessionID string `json:"session_id"`
		} `json:"members"`
	}

	// ① 带 session_id：a 在 team:mine 内 → 该行带成员详情（成员只有 a 自己——b 不在
	//    team:mine）；team:theirs 只给存在性 + 计数，**不给成员身份**（K11）。
	var view struct {
		SessionID string    `json:"session_id"`
		Gates     []gateRow `json:"gates"`
	}
	if code := getJSON(t, srv.URL+"/api/gates?session_id="+string(a), &view); code != http.StatusOK {
		t.Fatalf("GET /api/gates 状态码 = %d", code)
	}
	if view.SessionID != string(a) {
		t.Errorf("session_id 回显 = %q，want %q", view.SessionID, a)
	}
	if len(view.Gates) != 2 {
		t.Fatalf("gates = %d 行，want 2（字典序 team:mine / team:theirs）: %+v", len(view.Gates), view.Gates)
	}
	if view.Gates[0].GateID != "team:mine" || view.Gates[1].GateID != "team:theirs" {
		t.Fatalf("gates 顺序/标识不对：%+v", view.Gates)
	}
	if !view.Gates[0].Joined || view.Gates[0].MemberCount != 1 || len(view.Gates[0].Members) != 1 {
		t.Errorf("team:mine 行 = %+v，want joined + 1 成员 + 成员详情", view.Gates[0])
	}
	if view.Gates[0].Members[0].SessionID != string(a) {
		t.Errorf("team:mine 成员 = %q，want a 自己", view.Gates[0].Members[0].SessionID)
	}
	if view.Gates[1].Joined || view.Gates[1].MemberCount != 1 || len(view.Gates[1].Members) != 0 {
		t.Errorf("team:theirs 行 = %+v，want 未加入 + 计数 1 + **无成员详情**（K11）", view.Gates[1])
	}

	// ② 不带 session_id：纯枚举（存在性 + 计数，无成员详情、无 joined）。
	var plain struct {
		Gates []gateRow `json:"gates"`
	}
	if code := getJSON(t, srv.URL+"/api/gates", &plain); code != http.StatusOK {
		t.Fatalf("GET /api/gates（无 session_id）状态码 = %d", code)
	}
	if len(plain.Gates) != 2 {
		t.Fatalf("纯枚举 gates = %d 行，want 2", len(plain.Gates))
	}
	for _, g := range plain.Gates {
		if g.Joined || len(g.Members) != 0 {
			t.Errorf("纯枚举不该带 joined/成员详情: %+v", g)
		}
		if g.MemberCount != 1 {
			t.Errorf("纯枚举成员计数 = %d，want 1（%s）", g.MemberCount, g.GateID)
		}
	}

	// ③ 坏 session_id → 400。
	var te contracts.TransportError
	if code := getJSON(t, srv.URL+"/api/gates?session_id=not-a-ulid", &te); code != http.StatusBadRequest {
		t.Fatalf("坏 session_id 状态码 = %d，want 400", code)
	}

	// ④ leave 之后：成员计数掉到 0，gate 仍在枚举里（定义持久化）。
	if _, err := h.LeaveGate(context.Background(), b, theirs); err != nil {
		t.Fatalf("LeaveGate(b): %v", err)
	}
	if code := getJSON(t, srv.URL+"/api/gates?session_id="+string(a), &view); code != http.StatusOK {
		t.Fatalf("leave 后 GET /api/gates 状态码 = %d", code)
	}
	for _, g := range view.Gates {
		if g.GateID == "team:theirs" && g.MemberCount != 0 {
			t.Errorf("leave 后 team:theirs 计数 = %d，want 0", g.MemberCount)
		}
	}
}

// TestGatesEnumerationMemberGatesAreIntersection：成员行的 Gates 只给**与查询方相交**的
// gate（与 /api/peers 的 PeerRow.Gates 同源）——不是该成员的**全部** gate。
//
// 为什么：SessionRow.Gates 默认是"该 session 的全部活跃 gate"。直接塞进成员行 =
// 把"它还在别的 gate 里"泄露给查询方（哪怕查询方并不在那些 gate 里）——K11：不在网内的
// gate 只给存在性 + 计数，不给成员身份。
func TestGatesEnumerationMemberGatesAreIntersection(t *testing.T) {
	dir := plainDir(t, "work")
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = dir
		c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
	})
	a := runPromptIn(t, h, dir, "A 的任务")
	b := runPromptIn(t, h, dir, "B 的任务")
	const shared, secret = "team:shared", "team:secret"
	// a 只在 shared；b 在 shared + secret。
	if _, err := h.JoinGate(context.Background(), a, shared); err != nil {
		t.Fatalf("JoinGate(a, shared): %v", err)
	}
	for _, g := range []string{shared, secret} {
		if _, err := h.JoinGate(context.Background(), b, g); err != nil {
			t.Fatalf("JoinGate(b, %s): %v", g, err)
		}
	}

	type memberRow struct {
		SessionID string   `json:"session_id"`
		Gates     []string `json:"gates"`
	}
	type gateRow struct {
		GateID  string      `json:"gate_id"`
		Joined  bool        `json:"joined"`
		Members []memberRow `json:"members"`
	}
	var view struct {
		Gates []gateRow `json:"gates"`
	}
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()
	if code := getJSON(t, srv.URL+"/api/gates?session_id="+string(a), &view); code != http.StatusOK {
		t.Fatalf("GET /api/gates 状态码 = %d", code)
	}

	var sawShared, sawSecret bool
	for _, g := range view.Gates {
		switch g.GateID {
		case shared:
			sawShared = true
			if !g.Joined {
				t.Fatal("a 在 shared 内 → joined 应为 true")
			}
			var sawB bool
			for _, m := range g.Members {
				if m.SessionID != string(b) {
					continue
				}
				sawB = true
				if len(m.Gates) != 1 || m.Gates[0] != shared {
					t.Fatalf("成员 b 的 Gates = %v，want [%s]（只给与查询方相交的 gate；全量会泄露 secret）",
						m.Gates, shared)
				}
			}
			if !sawB {
				t.Fatal("shared 行的成员里应有 b")
			}
		case secret:
			sawSecret = true
			if g.Joined {
				t.Fatal("a 不在 secret 内 → joined 应为 false")
			}
			if len(g.Members) != 0 {
				t.Fatalf("secret 行不该给成员详情（K11）: %+v", g.Members)
			}
		}
	}
	if !sawShared || !sawSecret {
		t.Fatalf("枚举里应同时有 %s 与 %s: %+v", shared, secret, view.Gates)
	}
}

// TestGateJoinLeaveConcurrentSafe：并发 leave 不产生重复事件（per-session 锁 + 确定性 id
// 双防线；与 join 的 TestConcurrentJoinGateFallsToSingleEvent 对称）。
func TestGateJoinLeaveConcurrentSafe(t *testing.T) {
	dir := plainDir(t, "work")
	dbPath := filepath.Join(t.TempDir(), "events.db")
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = dir
		c.DBPath = dbPath
		c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
	})
	a := runPromptIn(t, h, dir, "A 的任务")
	const gate = "team:race"
	if _, err := h.JoinGate(context.Background(), a, gate); err != nil {
		t.Fatalf("JoinGate: %v", err)
	}
	// 16 个并发 leave：只有第一个该落事件，其余幂等空转。
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := h.LeaveGate(context.Background(), a, gate); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("并发 LeaveGate: %v", err)
	}
	_, lefts := gateEventCounts(t, dbPath, a)
	if lefts[gate] != 1 {
		t.Fatalf("并发 leave 落了 %d 条 gate.left（want 1）", lefts[gate])
	}
	if h.Discovery().InGate(a, gate) {
		t.Fatal("并发 leave 后 InGate 仍为 true")
	}
}

// ---------------------------------------------------------------------------
// F1：自动 gate —— 显式 leave 不得在重启后被静默撤销；重启也不得多落 gate.joined
// ---------------------------------------------------------------------------

// autoGateHub 装一个带**自动 workflow gate**（Config.RunID → workflow:<run_id>）的 hub。
//
// 为什么用 workflow gate 而不是 repo gate：repo gate 要能算出 repo_id（依赖 git 与
// 目录形态），workflow gate 只依赖配置 → 用例自洽（不依赖环境）。
//
// CWD 故意用**非 git 目录**：这样只会有 workflow 自动 gate，断言"自动 gate 的条数/事件"
// 不会被 repo gate 干扰。
func autoGateHub(t *testing.T, dbPath, runID string) *host.Hub {
	t.Helper()
	return newTestHub(t, func(c *host.Config) {
		c.CWD = plainDir(t, "work")
		c.DBPath = dbPath
		c.RunID = runID
		c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
	})
}

// TestAutoGateLeaveSurvivesRestart（F1）：自动 gate 被**显式 leave** 后，重启 hub 再跑一轮
// → 显式 leave **不得**被静默撤销（InGate 仍 false），也不得多落 gate.joined。
//
// 回归背景：joinAutoGates 按 JoinGeneration 算确定性事件 id——重启后届数变了 → 新 id →
// store 的 ON CONFLICT(id) DO NOTHING 去重失效 → **新一届 join 盖过 leave**（用户的显式
// 离开被撤销；且每次重启都多一条 gate.joined）。
func TestAutoGateLeaveSurvivesRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "events.db")
	const runID = "run-f1"
	const gate = "workflow:" + runID

	h1 := autoGateHub(t, dbPath, runID)
	a := runPromptIn(t, h1, plainDir(t, "work"), "A 的任务")
	if !h1.Discovery().InGate(a, gate) {
		t.Fatalf("自动 gate 未加入（InGate=false）：%s", gate)
	}
	if _, err := h1.LeaveGate(context.Background(), a, gate); err != nil {
		t.Fatalf("LeaveGate: %v", err)
	}
	if h1.Discovery().InGate(a, gate) {
		t.Fatal("leave 后仍在网内")
	}
	joinsBefore, leftsBefore := gateEventCounts(t, dbPath, a)
	if joinsBefore[gate] != 1 || leftsBefore[gate] != 1 {
		t.Fatalf("leave 前后事件数不对：joins=%d lefts=%d（want 1/1）", joinsBefore[gate], leftsBefore[gate])
	}

	// 重启（同一个事件库）。
	if err := h1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h2 := autoGateHub(t, dbPath, runID)
	if _, err := h2.RunPrompt(context.Background(), a, "再来一轮"); err != nil {
		t.Fatalf("重启后 RunPrompt: %v", err)
	}

	// ① 显式 leave 没被撤销。
	if h2.Discovery().InGate(a, gate) {
		t.Errorf("重启后自动 gate 把显式 leave 撤销了（InGate=true，gate=%s）", gate)
	}
	// ② 事件流没多落 join / left。
	joinsAfter, leftsAfter := gateEventCounts(t, dbPath, a)
	if joinsAfter[gate] != joinsBefore[gate] {
		t.Errorf("重启后 gate.joined 从 %d 变成 %d（不该多落）", joinsBefore[gate], joinsAfter[gate])
	}
	if leftsAfter[gate] != leftsBefore[gate] {
		t.Errorf("重启后 gate.left 从 %d 变成 %d（不该多落）", leftsBefore[gate], leftsAfter[gate])
	}
}

// TestAutoGateJoinNotDuplicatedOnRestart（F1）：**没有** leave 时，重启同样不该多落一条
// gate.joined（自动 gate 的加入是幂等的：一个 session 对同一个自动 gate 至多一条"当前
// 有效"的成员关系）。
//
// 回归背景：届数进了确定性事件 id → 重启后届数变了 → 新 id → 存储层去重失效 → 每次重启
// 多一条 gate.joined（届数虚涨、事件流被无意义地灌水）。
func TestAutoGateJoinNotDuplicatedOnRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "events.db")
	const runID = "run-f1b"
	const gate = "workflow:" + runID

	h1 := autoGateHub(t, dbPath, runID)
	a := runPromptIn(t, h1, plainDir(t, "work"), "A 的任务")
	joinsBefore, _ := gateEventCounts(t, dbPath, a)
	if joinsBefore[gate] != 1 {
		t.Fatalf("初次加入应落 1 条 gate.joined，得到 %d", joinsBefore[gate])
	}

	if err := h1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h2 := autoGateHub(t, dbPath, runID)
	if _, err := h2.RunPrompt(context.Background(), a, "再来一轮"); err != nil {
		t.Fatalf("重启后 RunPrompt: %v", err)
	}

	joinsAfter, _ := gateEventCounts(t, dbPath, a)
	if joinsAfter[gate] != joinsBefore[gate] {
		t.Errorf("重启后 gate.joined 从 %d 变成 %d（重启不该多落 join）", joinsBefore[gate], joinsAfter[gate])
	}
	// 成员关系保持（重启不该把已在网内的 session 踢出去）。
	if !h2.Discovery().InGate(a, gate) {
		t.Errorf("重启后成员关系应保持（InGate=true，gate=%s）", gate)
	}
}

// ---------------------------------------------------------------------------
// G13：WS 端到端——model.delta 流事件经 WS 送达；durable_only 收不到
// ---------------------------------------------------------------------------

// wsDeltaProvider 边收边发 delta 的假 provider（流式形状：增量事件 + 全量结果）。
type wsDeltaProvider struct{}

func (p *wsDeltaProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	for _, d := range []string{"Hel", "lo ", "世界"} {
		emit(core.ProviderEvent{Kind: "text.delta", Text: d})
	}
	emit(core.ProviderEvent{Kind: "done", Text: "Hello 世界"})
	return core.ModelResult{Text: "Hello 世界", Usage: core.Usage{InputTokens: 3, OutputTokens: 2}}, nil
}

// collectWSEvents 收 WS 事件直到看到 wantKind（或超时），返回期间收到的全部事件。
func collectWSEvents(t *testing.T, c *transport.Client, wantKind string, timeout time.Duration) []contracts.Event {
	t.Helper()
	var out []contracts.Event
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		recvCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		m, err := c.RecvContext(recvCtx)
		cancel()
		if err != nil {
			continue
		}
		if m.Kind != transport.KindEvent {
			continue
		}
		var ev contracts.Event
		if err := json.Unmarshal(m.Payload, &ev); err != nil {
			continue
		}
		out = append(out, ev)
		if ev.Kind == wantKind {
			return out
		}
	}
	t.Fatalf("%s 内没收到 kind=%s（只收到 %d 条）", timeout, wantKind, len(out))
	return out
}

// TestWSReceivesModelDeltaAndDurableOnlyFilters：默认订阅经 WS 收到多条 model.delta
// （拼接 == model.turn.text）；durable_only=true 的订阅收不到流事件。
func TestWSReceivesModelDeltaAndDurableOnlyFilters(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) {
		c.Provider = &wsDeltaProvider{}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	// ① 默认订阅（durable_only 缺省 = false：流事件也收）。
	c1, err := transport.Dial(ctx, wsURL)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c1.Close()
	if err := c1.Subscribe(ctx, contracts.SubscribeRequest{}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// ② durable_only 订阅（只收事实事件）。
	c2, err := transport.Dial(ctx, wsURL)
	if err != nil {
		t.Fatalf("dial 2: %v", err)
	}
	defer c2.Close()
	durableOnly := true
	if err := c2.Subscribe(ctx, contracts.SubscribeRequest{DurableOnly: &durableOnly}); err != nil {
		t.Fatalf("subscribe 2: %v", err)
	}

	// 跑一轮（会话先建，保证订阅已在位）。
	sid, err := h.CreateSession(host.SessionSpec{})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := h.RunPrompt(context.Background(), sid, "打招呼"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	// ①：收到流事件，且拼接 == model.turn.text（kind 用字面量：与契约 x-kind-payload 对齐）。
	got := collectWSEvents(t, c1, "model.turn", 10*time.Second)
	var (
		deltas int
		text   string
		turn   string
	)
	for _, ev := range got {
		switch ev.Kind {
		case "model.delta":
			deltas++
			if ev.Durable || ev.Seq != nil {
				t.Fatalf("WS 里的 model.delta 必须 durable=false 且无 seq: %+v", ev)
			}
			var p contracts.ModelDeltaPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Fatalf("model.delta payload: %v", err)
			}
			if p.Text != nil {
				text += *p.Text
			}
		case "model.turn":
			var p contracts.ModelTurnPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Fatalf("model.turn payload: %v", err)
			}
			turn = p.Text
		}
	}
	if deltas < 2 {
		t.Fatalf("model.delta = %d 条，want ≥2（打字机效果）", deltas)
	}
	if text != "Hello 世界" {
		t.Errorf("delta 拼接 = %q，want %q", text, "Hello 世界")
	}
	if turn != text {
		t.Errorf("model.turn.text = %q，应与 delta 拼接一致（%q）", turn, text)
	}

	// ②：durable_only 订阅一条 model.delta 都收不到（但事实事件照收）。
	got2 := collectWSEvents(t, c2, "model.turn", 10*time.Second)
	if len(got2) == 0 {
		t.Fatal("durable_only 订阅没收到任何事实事件（订阅没生效？）")
	}
	for _, ev := range got2 {
		if ev.Kind == "model.delta" {
			t.Fatalf("durable_only 订阅不该收到 model.delta: %+v", ev)
		}
	}
}

// ---------------------------------------------------------------------------
// G14：配置面——reasoning_effort 透传 + 白名单 fail-close
// ---------------------------------------------------------------------------

// effortCaptureProvider 记录最后收到的请求（断言 ReasoningEffort 透传用）。
type effortCaptureProvider struct {
	last core.ModelRequest
}

func (p *effortCaptureProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.last = req
	return core.ModelResult{Text: "ok"}, nil
}

// TestReasoningEffortConfigWiring：host.Config.ReasoningEffort 非法 → New 拒绝
// （fail-close）；合法 → 原样进每轮 ModelRequest（provider 透传给端点）；空 = 不传。
func TestReasoningEffortConfigWiring(t *testing.T) {
	dir := plainDir(t, "work")

	// ① 非法档位启动即拒（**直接调 host.New**：newTestHub 遇错会 t.Fatalf，造不出失败场景）。
	cfg := host.Config{
		DBPath:   filepath.Join(t.TempDir(), "events.db"),
		Model:    "scripted",
		CWD:      dir,
		Allow:    []string{"read"},
		Provider: &scriptedProvider{args: `{"path":"go.mod"}`},
	}
	cfg.ReasoningEffort = "turbo"
	bad, err := host.New(cfg)
	if err == nil {
		_ = bad.Close()
		t.Fatal("非法 reasoning_effort 不该装配成功（fail-close）")
	}
	if !strings.Contains(err.Error(), "ReasoningEffort") {
		t.Errorf("错误文案应点名 ReasoningEffort: %v", err)
	}

	// ② 合法档位：跑一轮 → provider 收到的请求带该档位。
	prov := &effortCaptureProvider{}
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = dir
		c.Provider = prov
		c.ReasoningEffort = "high"
	})
	if _, err := h.RunPrompt(context.Background(), host.NewSession(), "p"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if prov.last.ReasoningEffort == nil || *prov.last.ReasoningEffort != "high" {
		t.Fatalf("provider 收到的 ReasoningEffort = %v，want high", prov.last.ReasoningEffort)
	}

	// ③ 不设（空）= 不传。
	prov2 := &effortCaptureProvider{}
	h2 := newTestHub(t, func(c *host.Config) {
		c.CWD = dir
		c.Provider = prov2
	})
	if _, err := h2.RunPrompt(context.Background(), host.NewSession(), "p"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if prov2.last.ReasoningEffort != nil {
		t.Errorf("未设时 ReasoningEffort 应为 nil，得到 %q", *prov2.last.ReasoningEffort)
	}
}
