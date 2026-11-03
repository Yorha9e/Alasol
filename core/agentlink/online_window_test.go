package agentlink_test

// G-a（session 在线三态）：agentlink 的 Presence / Discover / Online 按 **session 粒度**
// 过滤活跃对端——只保留 Table.SessionActive 为真的行（self 行不动；agent 行跟随其所属
// session）。时间控制不注入时钟：世界夹具的事件 ts 是 time.Now（新鲜），用
// SetActivityWindow(极小) + BeginRun/EndRun 组合控制超窗 / 活跃。

import (
	"errors"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/core/agentlink"
)

// nearWindow 极小窗口：任何真实事件 ts 都在窗外 → 立刻"超窗"（G-a 夹具，极小侧）。
const nearWindow = time.Nanosecond

// TestPresenceDiscoverOnlineActiveFilter：超窗对端不出现在在线面、活跃对端出现；
// self 行不受过滤影响；Online 同口径过滤。
func TestPresenceDiscoverOnlineActiveFilter(t *testing.T) {
	w := newWorld(t)
	caller := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}

	// 默认窗 + 事件新鲜（世界夹具用 time.Now）：B 活跃 → 在 A 的对端里，Live=true。
	rep, err := w.link.Presence(caller, w.sidA)
	if err != nil {
		t.Fatalf("Presence: %v", err)
	}
	if len(rep.Peers) != 1 || rep.Peers[0].SessionID != string(w.sidB) {
		t.Fatalf("默认窗下 A 的水平对端应只有 B: %+v", rep.Peers)
	}
	if !rep.Peers[0].Live {
		t.Fatalf("进面的对端 Live 应为真（= SessionActive 判定结果）: %+v", rep.Peers[0])
	}

	// 窗口收到极小 → 全体超窗：B 不再出现在 A 的对端里；self 行不动（仍 Known）。
	w.table.SetActivityWindow(nearWindow)
	rep, err = w.link.Presence(caller, w.sidA)
	if err != nil {
		t.Fatalf("Presence(超窗): %v", err)
	}
	if len(rep.Peers) != 0 {
		t.Fatalf("超窗后 A 的水平对端应为空: %+v", rep.Peers)
	}
	if !rep.Known || rep.Self.SessionID != string(w.sidA) {
		t.Fatalf("self 行不该被过滤（G-a 只滤对端）: %+v", rep.Self)
	}

	// 跨 session 看超窗对端 → ErrNotVisible（fail-close，不把超窗当在线报出去）。
	if _, err := w.link.Presence(caller, w.sidB); err == nil {
		t.Fatal("超窗对端的跨 session Presence 应被拒")
	}

	// B 本轮在跑（BeginRun 短路）→ 活跃 → 重新进面。
	w.table.BeginRun(w.sidB)
	rep, err = w.link.Presence(caller, w.sidA)
	if err != nil {
		t.Fatalf("Presence(B running): %v", err)
	}
	if len(rep.Peers) != 1 || rep.Peers[0].SessionID != string(w.sidB) {
		t.Fatalf("B 在跑时该重新进面: %+v", rep.Peers)
	}
	w.table.EndRun(w.sidB)

	// Discover gate 分支：GateMembers = {A, B}（live），对端超窗不列、self 行照列。
	res, err := w.link.Discover(agentlink.DiscoverQuery{Caller: caller, GateID: w.gateX})
	if err != nil {
		t.Fatalf("Discover(gate): %v", err)
	}
	bySession := map[string]bool{}
	for _, s := range res.Sessions {
		bySession[s.SessionID] = true
	}
	if len(res.Sessions) != 1 || !bySession[string(w.sidA)] {
		t.Fatalf("超窗后 gate 查询应只剩 self（A）: %+v", res.Sessions)
	}
	// B 恢复活跃（running）→ 对端行回来（agent 行跟随 session 的上游：session 进面）。
	w.table.BeginRun(w.sidB)
	res, err = w.link.Discover(agentlink.DiscoverQuery{Caller: caller, GateID: w.gateX})
	if err != nil {
		t.Fatalf("Discover(gate, B running): %v", err)
	}
	if len(res.Sessions) != 2 {
		t.Fatalf("B 在跑时 gate 查询应含 A、B: %+v", res.Sessions)
	}
	w.table.EndRun(w.sidB)

	// Discover agent 分支（跨 session）：agent 行跟随其所属 session——B 超窗 → 视同
	// **不可见**（ErrNotVisible，与"不可见"同形状；批次1 P2-3：G-a 之前"对端不可见"
	// 回的就是 403，不活跃不该换成更松的 200 空结果）。
	res, err = w.link.Discover(agentlink.DiscoverQuery{Caller: caller, AgentID: w.amB})
	if !errors.Is(err, agentlink.ErrNotVisible) {
		t.Fatalf("超窗 session 的跨 session agent 查询应回 ErrNotVisible（得到 err=%v, res=%+v）", err, res)
	}
	if len(res.Sessions) != 0 || len(res.Agents) != 0 {
		t.Fatalf("超窗 session 的 agent 行不该出现（session 粒度过滤）: %+v", res)
	}
	// B 在跑 → 回来（只给主 agent 一行，K11 口径不变）。
	w.table.BeginRun(w.sidB)
	res, err = w.link.Discover(agentlink.DiscoverQuery{Caller: caller, AgentID: w.amB})
	if err != nil {
		t.Fatalf("Discover(agent, B running): %v", err)
	}
	if len(res.Sessions) != 1 || len(res.Agents) != 1 || res.Agents[0].AgentID != string(w.amB) {
		t.Fatalf("B 在跑时跨 session agent 查询应给 B 主 agent 一行: %+v", res)
	}
	w.table.EndRun(w.sidB)

	// Online = LiveSessions ∩ SessionActive：默认窗 3 → 超窗 0 → A 在跑后 1。
	w.table.SetActivityWindow(0) // 恢复缺省 15m
	if got := w.link.Online(); len(got) != 3 {
		t.Fatalf("默认窗在线 session = %d，want 3: %+v", len(got), got)
	}
	w.table.SetActivityWindow(nearWindow)
	if got := w.link.Online(); len(got) != 0 {
		t.Fatalf("全体超窗后在线清单应为空（托管但超窗不列）: %+v", got)
	}
	w.table.BeginRun(w.sidA)
	if got := w.link.Online(); len(got) != 1 || got[0].SessionID != string(w.sidA) {
		t.Fatalf("A 在跑后在线清单应只有 A: %+v", got)
	}
}
