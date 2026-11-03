package discovery

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

// mkEvent 组一条 durable 事实事件（seq 显式给，payload 原样 JSON）。
func mkEvent(t *testing.T, kind string, seq int64, session, agent contracts.Ulid, payload any) contracts.Event {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s payload: %v", kind, err)
	}
	return contracts.Event{
		V:         1,
		ID:        core.NewUlid(),
		Kind:      kind,
		TS:        "2026-10-28T00:00:00Z",
		Seq:       &seq,
		SessionID: session,
		AgentID:   agent,
		Durable:   true,
		Payload:   raw,
	}
}

// joinEv 是一条 gate.joined 事件。
func joinEv(t *testing.T, seq int64, session, agent contracts.Ulid, gateID, source string) contracts.Event {
	t.Helper()
	return mkEvent(t, KindGateJoined, seq, session, agent,
		contracts.GateJoinedPayload{GateID: gateID, Source: contracts.GateJoinSource(source)})
}

// applyAll 按顺序 Apply 一批事件。
func applyAll(t *Table, evs ...contracts.Event) {
	for i := range evs {
		t.Apply(&evs[i])
	}
}

// gateIDs 取行里的 gate 清单（转成 map 好比较）。
func gateSet(rows []SessionRow) map[string][]string {
	out := map[string][]string{}
	for _, r := range rows {
		out[r.SessionID] = r.Gates
	}
	return out
}

// ---------------------------------------------------------------------------
// gate id（K12 / S16）
// ---------------------------------------------------------------------------

// TestGateIDHelpers：前缀拼接 + 校验（S9：不合则拒）。
func TestGateIDHelpers(t *testing.T) {
	if got := RepoGate("ABC"); got != "repo:ABC" {
		t.Fatalf("RepoGate = %q", got)
	}
	if got := WorkflowGate("run-1"); got != "workflow:run-1" {
		t.Fatalf("WorkflowGate = %q", got)
	}
	ok := []string{"repo:ABC", "workflow:run-1", "team:backend", "a", "repo:" + string(make([]byte, 0))}
	for _, id := range ok {
		if !ValidGateID(id) {
			t.Errorf("ValidGateID(%q) = false，want true", id)
		}
	}
	bad := []string{"", " ", "repo:A B", "repo:A\tB", "repo:A\nB", "has space", string(make([]byte, MaxGateIDLen+1))}
	for _, id := range bad {
		if ValidGateID(id) {
			t.Errorf("ValidGateID(%q) = true，want false", id)
		}
	}
	// 归一化只去首尾空白，不动内部形态。
	if got, ok := NormalizeGateID("  repo:X  "); !ok || got != "repo:X" {
		t.Fatalf("NormalizeGateID = %q, %v", got, ok)
	}
	if _, ok := NormalizeGateID("repo:A B"); ok {
		t.Fatal("内部含空白的 gate id 该被拒")
	}
}

// ---------------------------------------------------------------------------
// agent 树投影 + 主 agent 推导
// ---------------------------------------------------------------------------

// TestApplyAgentTreeAndMainAgent：agent.spawned/completed/failed 折成树投影，
// 主 agent = 不作为任何 child 出现的最早 agent（口径同 durable.PrimaryAgentID）。
func TestApplyAgentTreeAndMainAgent(t *testing.T) {
	sid := core.NewUlid()
	main := core.NewUlid()
	child := core.NewUlid()
	grand := core.NewUlid()

	tbl := New()
	applyAll(tbl,
		mkEvent(t, "user.prompt", 1, sid, main, contracts.UserPromptPayload{Text: "hi"}),
		mkEvent(t, KindAgentSpawned, 2, sid, child, contracts.AgentSpawnedPayload{
			ParentAgentID: main, ChildAgentID: child, AgentType: "scout", Role: "backend", Depth: 2,
			SpawnRequestID: core.NewUlid(),
		}),
		mkEvent(t, KindAgentSpawned, 3, sid, grand, contracts.AgentSpawnedPayload{
			ParentAgentID: child, ChildAgentID: grand, AgentType: "reader", Role: "infra", Depth: 3,
			SpawnRequestID: core.NewUlid(),
		}),
		mkEvent(t, KindAgentCompleted, 4, sid, grand, contracts.AgentCompletedPayload{
			AgentID: grand, Summary: contracts.AgentSummary{Text: "done", Taint: "clean"},
		}),
		mkEvent(t, KindAgentFailed, 5, sid, child, contracts.AgentFailedPayload{
			AgentID: child, Error: contracts.ToolCallError{Message: "stuck"},
		}),
	)

	row, ok := tbl.Session(sid)
	if !ok {
		t.Fatal("session 不在表里")
	}
	if row.MainAgentID != string(main) {
		t.Fatalf("MainAgentID = %q，want %q", row.MainAgentID, main)
	}
	if row.AgentCount != 3 {
		t.Fatalf("AgentCount = %d，want 3", row.AgentCount)
	}
	if row.LastKind != KindAgentFailed || row.LastSeq != 5 {
		t.Fatalf("最近活动不符: %+v", row)
	}

	agents := tbl.Agents(sid)
	if len(agents) != 3 {
		t.Fatalf("agents = %d 行", len(agents))
	}
	byID := map[string]AgentRow{}
	for _, a := range agents {
		byID[a.AgentID] = a
	}
	if !byID[string(main)].Main || byID[string(main)].ParentID != "" || byID[string(main)].Depth != 1 {
		t.Fatalf("主 agent 行不符: %+v", byID[string(main)])
	}
	if c := byID[string(child)]; c.Main || c.ParentID != string(main) || c.Type != "scout" ||
		c.Role != "backend" || c.Depth != 2 || c.Status != StatusFailed {
		t.Fatalf("子 agent 行不符: %+v", c)
	}
	if g := byID[string(grand)]; g.ParentID != string(child) || g.Depth != 3 || g.Status != StatusCompleted {
		t.Fatalf("孙 agent 行不符: %+v", g)
	}

	// 全局索引（K18 Discover 的底层）。
	if got, ok := tbl.Agent(child); !ok || got.SessionID != string(sid) {
		t.Fatalf("Agent(%s) = %+v, %v", child, got, ok)
	}
}

// TestTerminalStatusIsMonotone：终态不可逆（F6：历史不重写）——completed 之后又来一条
// failed 不改状态。
func TestTerminalStatusIsMonotone(t *testing.T) {
	sid := core.NewUlid()
	main := core.NewUlid()
	child := core.NewUlid()
	tbl := New()
	applyAll(tbl,
		mkEvent(t, "user.prompt", 1, sid, main, contracts.UserPromptPayload{Text: "hi"}),
		mkEvent(t, KindAgentSpawned, 2, sid, child, contracts.AgentSpawnedPayload{
			ParentAgentID: main, ChildAgentID: child, AgentType: "scout", Depth: 2, SpawnRequestID: core.NewUlid(),
		}),
		mkEvent(t, KindAgentCompleted, 3, sid, child, contracts.AgentCompletedPayload{
			AgentID: child, Summary: contracts.AgentSummary{Text: "ok", Taint: "clean"},
		}),
		mkEvent(t, KindAgentFailed, 4, sid, child, contracts.AgentFailedPayload{
			AgentID: child, Error: contracts.ToolCallError{Message: "late"},
		}),
	)
	for _, a := range tbl.Agents(sid) {
		if a.AgentID == string(child) && a.Status != StatusCompleted {
			t.Fatalf("终态被改写: %+v", a)
		}
	}
}

// ---------------------------------------------------------------------------
// gate 成员关系（K12）
// ---------------------------------------------------------------------------

// TestApplyGateJoinedMembership：join 折叠出成员关系；同一 (session, gate) LWW；
// 来源可审计；Gates() 给历史成员、GateMembers() 只给活跃成员。
func TestApplyGateJoinedMembership(t *testing.T) {
	a, b, c := core.NewUlid(), core.NewUlid(), core.NewUlid()
	ma, mb, mc := core.NewUlid(), core.NewUlid(), core.NewUlid()
	tbl := New()
	applyAll(tbl,
		joinEv(t, 1, a, ma, "repo:X", SourceAutoRepo),
		joinEv(t, 2, b, mb, "repo:X", SourceAutoRepo),
		joinEv(t, 3, c, mc, "repo:Y", SourceAutoWorkflow),
		// b 显式再join一次同一个 gate：LWW，来源更新为 explicit。
		joinEv(t, 4, b, mb, "repo:X", SourceExplicit),
	)
	tbl.Activate(a)
	tbl.Activate(b)
	// c 不激活（模拟已结束的 session）。

	if got := tbl.GatesOf(a); !reflect.DeepEqual(got, []string{"repo:X"}) {
		t.Fatalf("GatesOf(a) = %v", got)
	}
	if got := tbl.GatesOf(c); !reflect.DeepEqual(got, []string{"repo:Y"}) {
		t.Fatalf("GatesOf(c) = %v", got)
	}

	// 历史成员：三个都算 join 过（可审计）。
	gates := tbl.Gates()
	if len(gates) != 2 || gates[0].GateID != "repo:X" || gates[1].GateID != "repo:Y" {
		t.Fatalf("Gates() = %+v", gates)
	}
	if len(gates[0].Members) != 2 ||
		!((gates[0].Members[0] == string(a) && gates[0].Members[1] == string(b)) ||
			(gates[0].Members[0] == string(b) && gates[0].Members[1] == string(a))) {
		t.Fatalf("repo:X 历史成员 = %v（want a 与 b）", gates[0].Members)
	}

	// 活跃成员：只有存活的（G17b）。
	live := tbl.GateMembers("repo:X")
	if len(live) != 2 {
		t.Fatalf("repo:X 活跃成员 = %d 个，want 2", len(live))
	}
	for _, r := range live {
		if r.SessionID == string(c) {
			t.Fatal("未激活的 session 不该是活跃成员")
		}
	}
	if got := tbl.GateMembers("repo:Y"); len(got) != 0 {
		t.Fatalf("repo:Y 活跃成员应为空，得到 %+v", got)
	}

	// join 历史可审计（含来源与 seq）。
	joins := tbl.JoinHistory()
	if len(joins) != 3 {
		t.Fatalf("JoinHistory = %d 条，want 3", len(joins))
	}
	if joins[0].Seq != 1 || joins[0].Source != SourceAutoRepo || joins[0].GateID != "repo:X" {
		t.Fatalf("join[0] = %+v", joins[0])
	}
	if joins[2].SessionID != string(b) || joins[2].Source != SourceExplicit {
		t.Fatalf("LWW 后 b 的 join 来源应是 explicit: %+v", joins[2])
	}
}

// TestDeactivateDropsActiveMembership：Deactivate 后成员关系失效（G17b：session 结束
// 即失效），但 join 历史仍可查（可审计、可重建）。
func TestDeactivateDropsActiveMembership(t *testing.T) {
	a, b := core.NewUlid(), core.NewUlid()
	tbl := New()
	applyAll(tbl,
		joinEv(t, 1, a, core.NewUlid(), "repo:X", SourceAutoRepo),
		joinEv(t, 2, b, core.NewUlid(), "repo:X", SourceAutoRepo),
	)
	tbl.Activate(a)
	tbl.Activate(b)
	if len(tbl.GateMembers("repo:X")) != 2 {
		t.Fatal("两个都该是活跃成员")
	}
	tbl.Deactivate(b)
	if !tbl.IsLive(a) || tbl.IsLive(b) {
		t.Fatalf("IsLive 不符: a=%v b=%v", tbl.IsLive(a), tbl.IsLive(b))
	}
	if got := tbl.GateMembers("repo:X"); len(got) != 1 || got[0].SessionID != string(a) {
		t.Fatalf("Deactivate 后活跃成员 = %+v", got)
	}
	if got := tbl.GatesOf(b); !reflect.DeepEqual(got, []string{"repo:X"}) {
		t.Fatalf("join 历史该留着: %v", got)
	}
	if got := tbl.VisiblePeers(a); len(got) != 0 {
		t.Fatalf("对端已走，不该还可见: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// 水平可见性（K11①）
// ---------------------------------------------------------------------------

// TestVisiblePeersHorizontalOnly：K11 —— 只有 session 的**主 agent**之间按 gate 相交
// 互见；无相交 gate 不可见；别 session 的 subagent 完全不可见。
func TestVisiblePeersHorizontalOnly(t *testing.T) {
	a, b, c, d := core.NewUlid(), core.NewUlid(), core.NewUlid(), core.NewUlid()
	ma, mb, mc, md := core.NewUlid(), core.NewUlid(), core.NewUlid(), core.NewUlid()
	subOfD := core.NewUlid()

	tbl := New()
	// G-a：mkEvent 夹具的 ts 写死在 2026-10-28（已超默认 15m 活跃窗）——显式放大窗口，
	// 让"激活的对端可见"这组既有断言在活跃判定下继续成立（活跃 = running ∨ 窗口内）。
	tbl.SetActivityWindow(farWindow)
	applyAll(tbl,
		// a：repo:X
		mkEvent(t, "user.prompt", 1, a, ma, contracts.UserPromptPayload{Text: "a"}),
		joinEv(t, 2, a, ma, "repo:X", SourceAutoRepo),
		// b：repo:X（与 a 相交）+ team:backend
		mkEvent(t, "user.prompt", 3, b, mb, contracts.UserPromptPayload{Text: "b"}),
		joinEv(t, 4, b, mb, "repo:X", SourceAutoRepo),
		joinEv(t, 5, b, mb, "team:backend", SourceExplicit),
		// c：repo:Y（与 a 不相交）
		mkEvent(t, "user.prompt", 6, c, mc, contracts.UserPromptPayload{Text: "c"}),
		joinEv(t, 7, c, mc, "repo:Y", SourceAutoRepo),
		// d：repo:X，且带一个 subagent
		mkEvent(t, "user.prompt", 8, d, md, contracts.UserPromptPayload{Text: "d"}),
		joinEv(t, 9, d, md, "repo:X", SourceAutoRepo),
		mkEvent(t, KindAgentSpawned, 10, d, subOfD, contracts.AgentSpawnedPayload{
			ParentAgentID: md, ChildAgentID: subOfD, AgentType: "scout", Depth: 2, SpawnRequestID: core.NewUlid(),
		}),
	)
	tbl.Activate(a)
	tbl.Activate(b)
	tbl.Activate(c)
	tbl.Activate(d)

	peers := tbl.VisiblePeers(a)
	if len(peers) != 2 {
		t.Fatalf("a 的可见对端 = %d 个，want 2（b 与 d）: %+v", len(peers), peers)
	}
	seen := map[string]PeerRow{}
	for _, p := range peers {
		seen[p.SessionID] = p
	}
	if p, ok := seen[string(b)]; !ok {
		t.Fatal("b 该可见")
	} else if p.MainAgentID != string(mb) {
		t.Fatalf("b 的对端行必须带**主 agent**: %+v", p)
	} else if !reflect.DeepEqual(p.Gates, []string{"repo:X"}) {
		t.Fatalf("b 的相交 gate 应只有 repo:X（team:backend 不相交）: %+v", p)
	}
	if p, ok := seen[string(d)]; !ok {
		t.Fatal("d 该可见")
	} else if p.MainAgentID != string(md) {
		t.Fatalf("d 的对端行必须带主 agent（不是它的 subagent）: %+v", p)
	}
	if _, ok := seen[string(c)]; ok {
		t.Fatal("c 与 a 无相交 gate，不该可见")
	}
	// 别 session 的 subagent 完全不可见：任何对端行里都不该出现 subOfD。
	for _, p := range peers {
		if p.MainAgentID == string(subOfD) || p.SessionID == string(subOfD) {
			t.Fatalf("别 session 的 subagent 泄漏进可见对端: %+v", p)
		}
	}
	// 查询方自己不在结果里。
	for _, p := range peers {
		if p.SessionID == string(a) {
			t.Fatal("自己不该出现在自己的对端里")
		}
	}
	// G-a（在线三态）：Deactivate 只撤"托管"（live），活跃判定看 running ∨ 窗口——
	// 要让 d 退出对端可见面，得让它落出活跃（窗口收到极小，全体超窗）；b 本轮在跑
	//（BeginRun 短路）→ 活跃，所以 a 的可见对端恰剩 b。
	tbl.Deactivate(d)
	tbl.SetActivityWindow(nearWindow)
	tbl.BeginRun(b)
	if got := tbl.VisiblePeers(a); len(got) != 1 || got[0].SessionID != string(b) {
		t.Fatalf("d 落出活跃后 a 的可见对端 = %+v", got)
	}
	tbl.EndRun(b)
	// 未知 session → 空（不是 nil，也不是 panic）。
	if got := tbl.VisiblePeers(core.NewUlid()); len(got) != 0 {
		t.Fatalf("未知 session 的可见对端应为空: %+v", got)
	}
}

// TestVisiblePeersIgnoresLiveButEventlessSession：激活了但一条事件都没有的 session
// 推不出主 agent → 不参与水平可见性（宁可少可见，不可挂个空行）。
func TestVisiblePeersIgnoresLiveButEventlessSession(t *testing.T) {
	a, b := core.NewUlid(), core.NewUlid()
	tbl := New()
	applyAll(tbl,
		mkEvent(t, "user.prompt", 1, a, core.NewUlid(), contracts.UserPromptPayload{Text: "a"}),
		joinEv(t, 2, a, core.NewUlid(), "repo:X", SourceAutoRepo),
	)
	tbl.Activate(a)
	tbl.Activate(b) // b 没有任何事件
	if got := tbl.VisiblePeers(a); len(got) != 0 {
		t.Fatalf("无事件的存活 session 不该可见: %+v", got)
	}
	// 它自己也是（没有主 agent → 对端为空，但 LiveSessions 里有它）。
	if got := tbl.VisiblePeers(b); len(got) != 0 {
		t.Fatalf("b 的可见对端应为空: %+v", got)
	}
	if got := tbl.LiveSessions(); len(got) != 2 {
		t.Fatalf("LiveSessions = %d，want 2", len(got))
	}
}

// ---------------------------------------------------------------------------
// 幂等 / 宽容 / 重建
// ---------------------------------------------------------------------------

// TestApplyIdempotentAndOutOfOrder：同一事件重复 Apply、乱序 Apply 都不改变状态
// （所有写入都是集合语义或 seq 守卫的 LWW，没有计数器）。
func TestApplyIdempotentAndOutOfOrder(t *testing.T) {
	sid := core.NewUlid()
	main := core.NewUlid()
	child := core.NewUlid()
	evs := []contracts.Event{
		mkEvent(t, "user.prompt", 1, sid, main, contracts.UserPromptPayload{Text: "hi"}),
		joinEv(t, 2, sid, main, "repo:X", SourceAutoRepo),
		mkEvent(t, KindAgentSpawned, 3, sid, child, contracts.AgentSpawnedPayload{
			ParentAgentID: main, ChildAgentID: child, AgentType: "scout", Depth: 2, SpawnRequestID: core.NewUlid(),
		}),
		mkEvent(t, "model.turn", 4, sid, main, contracts.ModelTurnPayload{Text: "done"}),
	}

	base := New()
	applyAll(base, evs...)
	want := base.Snapshot()

	twice := New()
	applyAll(twice, evs...)
	applyAll(twice, evs...) // 整批重放一遍
	if got := twice.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("重复 Apply 改了状态:\n got %+v\nwant %+v", got, want)
	}

	reversed := New()
	for i := len(evs) - 1; i >= 0; i-- {
		reversed.Apply(&evs[i])
	}
	got := reversed.Snapshot()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("乱序 Apply 改了状态:\n got %+v\nwant %+v", got, want)
	}
}

// TestUnknownKindAndStreamEventsIgnored：D7 宽容（未知 kind 忽略但推进最近活动）+
// F8（durable=false 的流事件不是事实，Apply 直接忽略）。
func TestUnknownKindAndStreamEventsIgnored(t *testing.T) {
	sid := core.NewUlid()
	main := core.NewUlid()
	tbl := New()
	applyAll(tbl,
		mkEvent(t, "user.prompt", 1, sid, main, contracts.UserPromptPayload{Text: "hi"}),
		mkEvent(t, "policy.asked", 2, sid, main, map[string]any{"tool": "bash"}), // 未知 kind
		mkEvent(t, "workflow.stage.changed", 3, sid, main, map[string]any{"stage": "build"}),
	)
	row, ok := tbl.Session(sid)
	if !ok {
		t.Fatal("session 不在表里")
	}
	if row.LastKind != "workflow.stage.changed" || row.LastSeq != 3 {
		t.Fatalf("未知 kind 该推进最近活动但不算专属投影: %+v", row)
	}
	if row.AgentCount != 1 || row.MainAgentID != string(main) {
		t.Fatalf("未知 kind 不该造出 agent: %+v", row)
	}

	// 流事件（durable=false）不入表。
	stream := mkEvent(t, "model.delta", 4, sid, main, map[string]any{"text": "..."})
	stream.Durable = false
	tbl.Apply(&stream)
	if row, _ = tbl.Session(sid); row.LastSeq != 3 {
		t.Fatalf("流事件不该进表: %+v", row)
	}

	// nil / 空 session 的事件也不该炸。
	tbl.Apply(nil)
	tbl.Apply(&contracts.Event{V: 1, Kind: "user.prompt", Durable: true})
}

// TestBadPayloadIsolated：已知 kind 的坏载荷跳过专属投影，不影响其余事件
// （D7 宽容，与 runtime/indexer 的坏载荷隔离同口径）。
func TestBadPayloadIsolated(t *testing.T) {
	sid := core.NewUlid()
	main := core.NewUlid()
	bad := mkEvent(t, KindGateJoined, 2, sid, main, nil)
	bad.Payload = []byte("{not json")
	tbl := New()
	applyAll(tbl,
		mkEvent(t, "user.prompt", 1, sid, main, contracts.UserPromptPayload{Text: "hi"}),
		bad,
		joinEv(t, 3, sid, main, "repo:Y", SourceExplicit),
	)
	if got := tbl.GatesOf(sid); !reflect.DeepEqual(got, []string{"repo:Y"}) {
		t.Fatalf("坏载荷被隔离后，后续 join 该正常折进来: %v", got)
	}
	row, _ := tbl.Session(sid)
	if row.LastSeq != 3 {
		t.Fatalf("坏载荷不该卡住水位: %+v", row)
	}
}

// TestResetAndReplayRebuilds：重建 = Reset + 从 seq 0 全量重放（验收②的 core 侧）。
// 折叠态必须与重建前一致；**live 不回来**（G17b：进程本地，重启后老成员全部失活）。
func TestResetAndReplayRebuilds(t *testing.T) {
	a, b := core.NewUlid(), core.NewUlid()
	ma, mb := core.NewUlid(), core.NewUlid()
	subOfB := core.NewUlid()
	evs := []contracts.Event{
		mkEvent(t, "user.prompt", 1, a, ma, contracts.UserPromptPayload{Text: "a"}),
		joinEv(t, 2, a, ma, "repo:X", SourceAutoRepo),
		mkEvent(t, "user.prompt", 3, b, mb, contracts.UserPromptPayload{Text: "b"}),
		joinEv(t, 4, b, mb, "repo:X", SourceExplicit),
		mkEvent(t, KindAgentSpawned, 5, b, subOfB, contracts.AgentSpawnedPayload{
			ParentAgentID: mb, ChildAgentID: subOfB,
			AgentType: "scout", Depth: 2, SpawnRequestID: core.NewUlid(),
		}),
	}

	before := New()
	applyAll(before, evs...)
	before.Activate(a)
	before.Activate(b)
	want := before.Snapshot()
	if len(want.Sessions) != 2 || len(want.Agents) != 3 || len(want.Gates) != 1 || len(want.Joins) != 2 {
		t.Fatalf("重建前快照形状不对: %+v", want)
	}
	// G-a：夹具 ts 写死在 2026-10-28（超默认 15m 活跃窗）——用 running 短路让重建前的
	// 可见性断言成立。BeginRun 放在取快照**之后**：running 不进 Snapshot（进程本地），
	// 崩溃前后两边都是 0 → 下面的 DeepEqual 比较不受影响。
	before.BeginRun(b)
	if len(before.VisiblePeers(a)) != 1 {
		t.Fatal("重建前 a 该看得见 b")
	}

	// 崩溃重启：新实例 + 全量重放。
	after := New()
	after.Reset() // 空表上也该安全（重建入口）
	applyAll(after, evs...)

	got := after.Snapshot()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("重放后的折叠态与崩溃前不一致:\n got %+v\nwant %+v", got, want)
	}
	// 老成员全部失活（G17b）。
	if after.IsLive(a) || after.IsLive(b) {
		t.Fatal("重建后老 session 不该还是存活的")
	}
	if peers := after.VisiblePeers(a); len(peers) != 0 {
		t.Fatalf("重建后不该还有可见对端: %+v", peers)
	}
	if members := after.GateMembers("repo:X"); len(members) != 0 {
		t.Fatalf("重建后 gate 的活跃成员该为空: %+v", members)
	}
	// join 历史仍在（可审计、可重建）。
	if len(after.JoinHistory()) != 2 {
		t.Fatalf("重建后 join 历史丢了: %+v", after.JoinHistory())
	}
	// hub 重新激活 → 恢复**托管**（resume 的路径）。G-a：托管 ≠ 活跃——对端可见面还
	// 要 running ∨ 窗口内；夹具 ts 超默认窗，b 本轮在跑（BeginRun）即活跃 → 恢复可见。
	after.Activate(a)
	after.Activate(b)
	after.BeginRun(b)
	if peers := after.VisiblePeers(a); len(peers) != 1 || peers[0].SessionID != string(b) {
		t.Fatalf("重新激活 + 本轮在跑后该恢复可见: %+v", peers)
	}
}

// TestSnapshotShapeIsStable：快照的排序确定（同状态两次取样相等），且空表给空切片
// 而不是 nil（JSON 输出与 DeepEqual 都稳定）。
func TestSnapshotShapeIsStable(t *testing.T) {
	empty := New().Snapshot()
	if !reflect.DeepEqual(empty.Sessions, []SessionRow{}) ||
		!reflect.DeepEqual(empty.Agents, []AgentRow{}) ||
		!reflect.DeepEqual(empty.Gates, []GateRow{}) ||
		!reflect.DeepEqual(empty.Joins, []GateJoin{}) {
		t.Fatalf("空快照该是空切片: %+v", empty)
	}

	a, b := core.NewUlid(), core.NewUlid()
	tbl := New()
	applyAll(tbl,
		joinEv(t, 2, b, core.NewUlid(), "repo:X", SourceAutoRepo),
		mkEvent(t, "user.prompt", 1, a, core.NewUlid(), contracts.UserPromptPayload{Text: "a"}),
		joinEv(t, 3, a, core.NewUlid(), "repo:X", SourceAutoRepo),
	)
	s1 := tbl.Snapshot()
	s2 := tbl.Snapshot()
	if !reflect.DeepEqual(s1, s2) {
		t.Fatal("同一状态两次快照不一致（排序不确定）")
	}
	// 最后活跃倒序：a（seq 3）在前。
	if s1.Sessions[0].SessionID != string(a) {
		t.Fatalf("session 排序该按最后活跃倒序: %+v", s1.Sessions)
	}
	// 乱序 Apply 不影响最终快照（上面的测试已覆盖，这里顺带确认 gate 成员集合语义）。
	if len(s1.Gates[0].Members) != 2 {
		t.Fatalf("gate 成员该有 2 个: %+v", s1.Gates[0].Members)
	}
	if s1.Gates[0].Members[0] > s1.Gates[0].Members[1] {
		t.Fatalf("gate 成员该按字典序: %+v", s1.Gates[0].Members)
	}
}

// TestNilTableIsSafe：nil 表的所有方法都不该 panic（宿主装配失败路径的纵深防御）。
func TestNilTableIsSafe(t *testing.T) {
	var tbl *Table
	tbl.Apply(&contracts.Event{Kind: "user.prompt", Durable: true})
	tbl.Activate(core.NewUlid())
	tbl.Deactivate(core.NewUlid())
	tbl.Reset()
	if got := tbl.Sessions(); len(got) != 0 {
		t.Errorf("Sessions = %+v", got)
	}
	if got := tbl.LiveSessions(); len(got) != 0 {
		t.Errorf("LiveSessions = %+v", got)
	}
	if got := tbl.VisiblePeers(core.NewUlid()); len(got) != 0 {
		t.Errorf("VisiblePeers = %+v", got)
	}
	if got := tbl.GateMembers("repo:X"); len(got) != 0 {
		t.Errorf("GateMembers = %+v", got)
	}
	if got := tbl.JoinHistory(); len(got) != 0 {
		t.Errorf("JoinHistory = %+v", got)
	}
	if got := tbl.Snapshot(); !reflect.DeepEqual(got, Snapshot{
		Sessions: []SessionRow{}, Agents: []AgentRow{}, Gates: []GateRow{}, Joins: []GateJoin{},
	}) {
		t.Errorf("Snapshot = %+v", got)
	}
}
