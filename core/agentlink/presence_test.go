package agentlink_test

// K18 Presence + Discover 的单测（core/agentlink）。
//
// 覆盖：在线画像（自己 + 水平对端 + live）、按 gate 查活跃成员、按 agent 查（含子树）、
// 查询条件非法、依赖缺失时 fail-close。

import (
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/discovery"
)

func TestPresenceReportsSelfPeersAndLiveness(t *testing.T) {
	w := newWorld(t)
	self := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}

	rep, err := w.link.Presence(self, w.sidA)
	if err != nil {
		t.Fatalf("Presence: %v", err)
	}
	if !rep.Known {
		t.Fatalf("session A 应在发现表里（Known=false）")
	}
	if rep.Self.SessionID != string(w.sidA) || rep.Self.MainAgentID != string(w.amA) {
		t.Fatalf("Self 不符: %+v", rep.Self)
	}
	if rep.Self.AgentCount != 4 {
		t.Fatalf("A 的 agent 数 = %d，want 4（主 + 3 subagent）", rep.Self.AgentCount)
	}
	if len(rep.Self.Gates) != 1 || rep.Self.Gates[0] != w.gateX {
		t.Fatalf("A 的 gates = %v，want [%s]", rep.Self.Gates, w.gateX)
	}
	if !rep.Self.Live {
		t.Fatalf("A 应存活（本进程持有）")
	}
	// K11①：只有 gate 相交的**对端主 agent**——B 在列，C 不在，A 自己也不在。
	if len(rep.Peers) != 1 || rep.Peers[0].SessionID != string(w.sidB) || rep.Peers[0].MainAgentID != string(w.amB) {
		t.Fatalf("A 的水平对端 = %+v，want 只有 B 的主 agent", rep.Peers)
	}
	if len(rep.Peers[0].Gates) != 1 || rep.Peers[0].Gates[0] != w.gateX {
		t.Fatalf("对端 Gates 应为相交 gate: %+v", rep.Peers[0])
	}

	// C 在另一个 gate：与谁都不可见。
	repC, err := w.link.Presence(agentlink.Identity{SessionID: w.sidC, AgentID: w.amC}, w.sidC)
	if err != nil {
		t.Fatalf("Presence(C): %v", err)
	}
	if len(repC.Peers) != 0 {
		t.Fatalf("C 的水平对端应为空，得到 %+v", repC.Peers)
	}
	// 不存在的 session：Known=false，不是 panic。
	rep, err = w.link.Presence(self, contracts.Ulid("ZZZZZZZZZZZZZZZZZZZZZZZZZZ"))
	if err != nil {
		t.Fatalf("Presence(未知 session): %v", err)
	}
	if rep.Known || len(rep.Peers) != 0 {
		t.Fatalf("未知 session 的 Presence 应零值: %+v", rep)
	}

	online := w.link.Online()
	if len(online) != 3 {
		t.Fatalf("在线 session = %d，want 3", len(online))
	}
	// 结束一个 session → 从在线表消失（G17b），但**仍在发现表里**（join 历史可审计）。
	w.table.Deactivate(w.sidB)
	if online = w.link.Online(); len(online) != 2 {
		t.Fatalf("B 结束后在线 session = %d，want 2", len(online))
	}
	// G-a（在线三态）：Deactivate 只撤"托管"（live），活跃判定看 running ∨ 窗口——B 的
	// 事件还新鲜，默认窗内仍活跃、仍进对端可见面。把窗口收到极小（全体超窗）→ 对端
	// 不可见（self 行不动）。活跃维的正例 / running 短路见 online_window_test.go。
	w.table.SetActivityWindow(nearWindow)
	rep, err = w.link.Presence(self, w.sidA)
	if err != nil {
		t.Fatalf("Presence#2: %v", err)
	}
	if len(rep.Peers) != 0 {
		t.Fatalf("超窗后 A 的水平对端应为空，得到 %+v", rep.Peers)
	}
	if _, ok := w.table.Session(w.sidB); !ok {
		t.Fatalf("B 结束后仍应在发现表里（历史可审计）")
	}
}

// TestPresenceVisibilityK11：跨 session 的 Presence 只对"可见对端"开放（F4）。
func TestPresenceVisibilityK11(t *testing.T) {
	w := newWorld(t)
	// A 的主 agent 看 B（gate 相交）→ 允许，且只回对端视角。
	rep, err := w.link.Presence(agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}, w.sidB)
	if err != nil {
		t.Fatalf("A 看 B 应允许: %v", err)
	}
	if !rep.Known || rep.Self.SessionID != string(w.sidB) || rep.Self.MainAgentID != string(w.amB) {
		t.Fatalf("对端画像不符: %+v", rep.Self)
	}
	// 只给**相交**的 gate，不泄漏对端的全部 gate；agent 树规模不泄漏。
	if len(rep.Self.Gates) != 1 || rep.Self.Gates[0] != w.gateX {
		t.Fatalf("对端 gate 应只给相交的: %+v", rep.Self.Gates)
	}
	if rep.Self.AgentCount != 0 {
		t.Fatalf("对端 agent 树规模不该泄漏: %+v", rep.Self)
	}
	// C 与 A 的 gate 不相交 → 不可见（负例）。
	if _, err := w.link.Presence(agentlink.Identity{SessionID: w.sidC, AgentID: w.amC}, w.sidA); err == nil {
		t.Fatalf("C 看 A 应被拒（gate 不相交）")
	}
	// 别的 session 的 subagent：别 session 的 subagent 完全不可见（负例）。
	if _, err := w.link.Presence(agentlink.Identity{SessionID: w.sidB, AgentID: w.subB1}, w.sidA); err == nil {
		t.Fatalf("B 的 subagent 看 A 应被拒（K11①）")
	}
	// 身份不完整 → 拒。
	if _, err := w.link.Presence(agentlink.Identity{SessionID: w.sidA}, w.sidA); err == nil {
		t.Fatalf("缺 agent_id 的身份应被拒")
	}
}

func TestDiscoverByGateAndByAgentTree(t *testing.T) {
	w := newWorld(t)
	caller := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}

	// 按 gate：活跃成员（A、B；C 不在 repo:X）。
	res, err := w.link.Discover(agentlink.DiscoverQuery{Caller: caller, GateID: w.gateX})
	if err != nil {
		t.Fatalf("Discover(gate): %v", err)
	}
	if len(res.Sessions) != 2 || res.Sessions[0].SessionID != string(w.sidA) && res.Sessions[1].SessionID != string(w.sidA) {
		t.Fatalf("repo:X 成员 = %+v，want A 与 B", res.Sessions)
	}
	if len(res.Agents) != 0 {
		t.Fatalf("gate 查询不该给 agent 清单（K11①：水平粒度是 session 主 agent）: %+v", res.Agents)
	}

	// 未知 gate：空结果，不是错误。
	res, err = w.link.Discover(agentlink.DiscoverQuery{Caller: caller, GateID: "repo:不存在"})
	if err != nil || len(res.Sessions) != 0 {
		t.Fatalf("未知 gate: err=%v sessions=%+v", err, res.Sessions)
	}

	// 按 agent：所在 session + 该 agent 一行。
	res, err = w.link.Discover(agentlink.DiscoverQuery{Caller: caller, AgentID: w.childA1})
	if err != nil {
		t.Fatalf("Discover(agent): %v", err)
	}
	if len(res.Sessions) != 1 || res.Sessions[0].SessionID != string(w.sidA) {
		t.Fatalf("agent 所在 session = %+v，want A", res.Sessions)
	}
	if len(res.Agents) != 1 || res.Agents[0].AgentID != string(w.childA1) || res.Agents[0].ParentID != string(w.amA) {
		t.Fatalf("agent 行 = %+v，want childA1（父=amA）", res.Agents)
	}
	if res.Agents[0].Depth != 2 || res.Agents[0].Type != "scout" || res.Agents[0].Role != "recon" {
		t.Fatalf("agent 行深度/类型/角色不符: %+v", res.Agents[0])
	}

	// 按 agent + Subtree：子树含自己与全部后代（childA1 → childA1 + grandA1），
	// **不含兄弟** siblingA2（K11②）。
	res, err = w.link.Discover(agentlink.DiscoverQuery{Caller: caller, AgentID: w.childA1, Subtree: true})
	if err != nil {
		t.Fatalf("Discover(subtree): %v", err)
	}
	got := map[string]bool{}
	for _, a := range res.Agents {
		got[a.AgentID] = true
	}
	if len(got) != 2 || !got[string(w.childA1)] || !got[string(w.grandA1)] {
		t.Fatalf("childA1 的子树 = %v，want {childA1, grandA1}", got)
	}
	if got[string(w.sibA2)] {
		t.Fatalf("兄弟 siblingA2 不该出现在 childA1 的子树里（K11②）")
	}

	// 从主 agent 出发的子树 = 该 session 全部 agent。
	res, _ = w.link.Discover(agentlink.DiscoverQuery{Caller: caller, AgentID: w.amA, Subtree: true})
	if len(res.Agents) != 4 {
		t.Fatalf("amA 的子树 = %d 个 agent，want 4", len(res.Agents))
	}

	// 未知 agent：空结果（不是错误）。
	res, err = w.link.Discover(agentlink.DiscoverQuery{Caller: caller, AgentID: contracts.Ulid("MMMMMMMMMMMMMMMMMMMMMMM")})
	if err != nil || len(res.Sessions) != 0 || len(res.Agents) != 0 {
		t.Fatalf("未知 agent: err=%v res=%+v", err, res)
	}

	// 查询条件非法：都空 / 都非空。
	if _, err := w.link.Discover(agentlink.DiscoverQuery{Caller: caller}); err == nil {
		t.Fatalf("空查询应报错")
	}
	if _, err := w.link.Discover(agentlink.DiscoverQuery{Caller: caller, GateID: w.gateX, AgentID: w.amA}); err == nil {
		t.Fatalf("gate+agent 同时给出应报错")
	}
	// 缺调用方身份 → 拒（F4：没有调用方就没有可见性过滤）。
	if _, err := w.link.Discover(agentlink.DiscoverQuery{GateID: w.gateX}); err == nil {
		t.Fatalf("缺 caller 的 Discover 应报错")
	}

	// Subtree 便捷方法：与 Discover(Subtree:true) 同结论。
	sub := w.link.Subtree(w.childA1)
	if len(sub) != 2 || sub[0].AgentID != string(w.childA1) {
		t.Fatalf("Subtree(childA1) = %+v", sub)
	}
	if got := w.link.Subtree(contracts.Ulid("MMMMMMMMMMMMMMMMMMMMMMM")); len(got) != 0 {
		t.Fatalf("未知 agent 的 Subtree 应为空")
	}
}

// TestDiscoverVisibilityK11：跨 session 的 Discover 只对"可见对端主 agent"开放（F4）。
func TestDiscoverVisibilityK11(t *testing.T) {
	w := newWorld(t)
	aMain := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}

	// ① gate 查询：调用方必须与该 gate 相交——C 与 repo:X 不相交 → 拒（负例）。
	if _, err := w.link.Discover(agentlink.DiscoverQuery{
		Caller: agentlink.Identity{SessionID: w.sidC, AgentID: w.amC}, GateID: w.gateX,
	}); err == nil {
		t.Fatalf("C 查 repo:X 应被拒（gate 不相交，F4）")
	}
	// ② 跨 session 的 agent 查询：只允许可见对端的**主 agent**。
	res, err := w.link.Discover(agentlink.DiscoverQuery{Caller: aMain, AgentID: w.amB})
	if err != nil {
		t.Fatalf("A 查 B 的主 agent 应允许: %v", err)
	}
	if len(res.Agents) != 1 || res.Agents[0].AgentID != string(w.amB) {
		t.Fatalf("跨 session 的 agent 行 = %+v，want 只有 B 的主 agent一行", res.Agents)
	}
	// 对端只给**相交**的 gate，不泄漏它的全部 gate / agent 树规模。
	if len(res.Sessions) != 1 || len(res.Sessions[0].Gates) != 1 || res.Sessions[0].Gates[0] != w.gateX {
		t.Fatalf("对端 session 行 = %+v，want 只给相交 gate", res.Sessions)
	}
	if res.Sessions[0].AgentCount != 0 {
		t.Fatalf("对端 agent 树规模不该泄漏: %+v", res.Sessions[0])
	}
	// ③ 跨 session 的 **subagent** 行：完全不可见（负例）。
	if _, err := w.link.Discover(agentlink.DiscoverQuery{Caller: aMain, AgentID: w.subB1}); err == nil {
		t.Fatalf("A 查 B 的 subagent 应被拒（别 session 的 subagent 完全不可见）")
	}
	// ④ 对端的子树也不给（Subtree=true 也不行）。
	res, err = w.link.Discover(agentlink.DiscoverQuery{Caller: aMain, AgentID: w.amB, Subtree: true})
	if err != nil {
		t.Fatalf("Discover(跨 session 主 agent + subtree): %v", err)
	}
	if len(res.Agents) != 1 || res.Agents[0].AgentID != string(w.amB) {
		t.Fatalf("跨 session 不该给子树: %+v", res.Agents)
	}
	// ⑤ 别的 session 的 subagent 当调用方 → 不可见（负例）。
	if _, err := w.link.Discover(agentlink.DiscoverQuery{
		Caller: agentlink.Identity{SessionID: w.sidB, AgentID: w.subB1}, AgentID: w.amA,
	}); err == nil {
		t.Fatalf("B 的 subagent 查 A 应被拒（K11①）")
	}
}

func TestPresenceDiscoverFailClosedWithoutTable(t *testing.T) {
	// 只注入 reader/sink（没有发现表）：Presence/Discover 零值，Broadcast 明确报错。
	l := agentlink.New(agentlink.Deps{Reader: newMemStore(), Sink: newMemStore()})
	if _, err := l.Presence(agentlink.Identity{}, contracts.Ulid("AAAAAAAAAAAAAAAAAAAAAAAA")); err == nil {
		t.Fatalf("无表时 Presence 应报错（fail-close）")
	}
	if got := l.Online(); len(got) != 0 {
		t.Fatalf("无表时 Online 应为空")
	}
	if _, err := l.Discover(agentlink.DiscoverQuery{GateID: "repo:X"}); err == nil {
		t.Fatalf("无表时 Discover 应报错（fail-close）")
	}
	// nil Link：全部 fail-close。
	var nilLink *agentlink.Link
	if _, err := nilLink.Presence(agentlink.Identity{}, contracts.Ulid("AAAAAAAAAAAAAAAAAAAAAAAA")); err == nil {
		t.Fatalf("nil Link 的 Presence 应报错")
	}
	if _, err := nilLink.Discover(agentlink.DiscoverQuery{GateID: "repo:X"}); err == nil {
		t.Fatalf("nil Link 的 Discover 应报错")
	}
}

func TestDiscoveryTableIsExposedForDiagnostics(t *testing.T) {
	w := newWorld(t)
	if w.link.Table() != w.table {
		t.Fatalf("Table() 应回报注入的发现表")
	}
	if got := w.link.Table().GatesOf(w.sidA); len(got) != 1 || got[0] != w.gateX {
		t.Fatalf(" GatesOf(A) = %v", got)
	}
	_ = discovery.KindGateJoined // 常量可用性（镜像与契约对齐）
}

// TestDiscoverGateQueryHidesPeerGatesAndAgentCount（#3）：gate 查询里**对端**的行只给
// 与调用方**相交**的 gate，不泄漏对端的全部 gate 与 agent 树规模（口径与
// peerSessionPresence 一致——K11①：对端只以"主 agent"的身份可见）。
func TestDiscoverGateQueryHidesPeerGatesAndAgentCount(t *testing.T) {
	w := newWorld(t)
	// B 再加入另一个 gate：对端的**全部** gate = 2 个，与 A 只相交 1 个。
	// （重扫会把活状态也清掉——G17b：活状态是进程本地的，扫完要重新 Activate。）
	w.gate(w.sidB, w.amB, w.gateY)
	w.rescan()
	w.table.Activate(w.sidA)
	w.table.Activate(w.sidB)
	w.table.Activate(w.sidC)

	caller := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}
	res, err := w.link.Discover(agentlink.DiscoverQuery{Caller: caller, GateID: w.gateX})
	if err != nil {
		t.Fatalf("Discover(gate): %v", err)
	}
	bySession := map[string]agentlink.SessionPresence{}
	for _, s := range res.Sessions {
		bySession[s.SessionID] = s
	}
	// 自己：完整画像（自己的 gate 列表与 agent 树规模不是泄漏面）。
	self, ok := bySession[string(w.sidA)]
	if !ok {
		t.Fatalf("结果里没有调用方自己: %+v", res.Sessions)
	}
	if len(self.Gates) != 1 || self.Gates[0] != w.gateX {
		t.Fatalf("自己的 gate = %v，want [%s]", self.Gates, w.gateX)
	}
	if self.AgentCount != 4 {
		t.Fatalf("自己的 agent 数 = %d，want 4", self.AgentCount)
	}
	// 对端：只给相交的 gate，且不给 agent 树规模。
	peer, ok := bySession[string(w.sidB)]
	if !ok {
		t.Fatalf("结果里没有对端 B: %+v", res.Sessions)
	}
	if len(peer.Gates) != 1 || peer.Gates[0] != w.gateX {
		t.Fatalf("对端行泄漏了它自己的 gate 列表: %+v", peer.Gates)
	}
	if peer.AgentCount != 0 {
		t.Fatalf("对端行泄漏了 agent 树规模: %+v", peer)
	}
}
