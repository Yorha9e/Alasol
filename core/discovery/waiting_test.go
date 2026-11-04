package discovery

// G-f（standby 挂起期的 waiting 态，15 号设计 §4）判定原语的单测：
//
//	① BeginWait / EndWait：窗口极小时 BeginWait → SessionActive=true、行 Waiting=true、
//	   不刷最后事件 ts；EndWait 后回落（窗口极小时不活跃）；未知 session 的 BeginWait
//	   （早于任何事件）也算活跃；
//	② 计数下限 0（重复 End 不打负）+ Reset 清空 + nil 表纵深防御；
//	③ waiting 算活跃 → 超窗对端靠 waiting 短路进 VisiblePeers，EndWait 后落出
//	   （豁免窗口判定，一处判定 activeLocked）。
//
// 时间控制与 online_window_test 同款（不注入时钟）：写死的事件 ts 夹具 +
// SetActivityWindow（nearWindow 极小窗）组合控制。

import (
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// TestBeginWaitActiveAndRowFlag：① BeginWait/EndWait 的活跃短路 + 行出口 Waiting 现算
// + 不刷最后事件 ts。
func TestBeginWaitActiveAndRowFlag(t *testing.T) {
	tbl := New()
	sid := core.NewUlid()
	ts := applyAt(t, tbl, "user.prompt", 1, sid, core.NewUlid(),
		time.Now().Add(-2*time.Hour), contracts.UserPromptPayload{Text: "old"})
	tbl.SetActivityWindow(nearWindow) // 极小窗：任何真实事件 ts 都超窗

	// 挂起前：超窗 → 不活跃、Waiting=false。
	if tbl.SessionActive(sid) {
		t.Fatal("超窗的 session 挂起前不该活跃")
	}
	row, ok := tbl.Session(sid)
	if !ok {
		t.Fatal("session 不在表里")
	}
	if row.Waiting || row.Online {
		t.Fatalf("挂起前该行不该 Waiting/Online: %+v", row)
	}

	// BeginWait：窗口极小也活跃（waiting 短路，豁免窗口判定）；行 Waiting=true、
	// Online=true、Running=false（waiting 不是 running）；UpdatedAt 仍是挂起前的
	// 最后事件 ts（不刷最后事件 ts——挂起期零事件天然满足"不造假活动"）。
	tbl.BeginWait(sid)
	if !tbl.SessionActive(sid) {
		t.Fatal("BeginWait 后窗口极小也应活跃（waiting 短路，不看 ts）")
	}
	row, _ = tbl.Session(sid)
	if !row.Waiting {
		t.Fatalf("BeginWait 后行应 Waiting=true: %+v", row)
	}
	if !row.Online || row.Running {
		t.Fatalf("waiting 算活跃但不是 running: %+v", row)
	}
	if row.UpdatedAt != ts {
		t.Fatalf("waiting 不刷最后事件 ts：UpdatedAt 应仍是 %q，得到 %q", ts, row.UpdatedAt)
	}

	// 未知 session（BeginWait 早于任何事件落地）：照算活跃（同 running 短路口径）。
	unk := core.NewUlid()
	tbl.BeginWait(unk)
	if !tbl.SessionActive(unk) {
		t.Fatal("BeginWait 早于事件的未知 session 也应活跃")
	}

	// EndWait 配平回落（窗口极小 → 不活跃）；行 Waiting=false；ts 仍不动。
	tbl.EndWait(sid)
	tbl.EndWait(unk)
	if tbl.SessionActive(sid) {
		t.Fatal("EndWait 后窗口极小应回落为不活跃")
	}
	row, _ = tbl.Session(sid)
	if row.Waiting {
		t.Fatalf("EndWait 后行应 Waiting=false: %+v", row)
	}
	if row.UpdatedAt != ts {
		t.Fatalf("EndWait 也不刷最后事件 ts：%q vs %q", ts, row.UpdatedAt)
	}
}

// TestEndWaitFloorZeroAndReset：② 计数下限 0（重复 End 不打负）+ Reset 清空 + nil 表防御。
func TestEndWaitFloorZeroAndReset(t *testing.T) {
	tbl := New()
	sid := core.NewUlid()
	tbl.SetActivityWindow(nearWindow) // 无新鲜事件：活跃只可能来自 waiting 短路

	tbl.BeginWait(sid)
	tbl.EndWait(sid)
	tbl.EndWait(sid) // 多余的一次：下限 0（无下限会把计数打成负）
	if tbl.SessionActive(sid) {
		t.Fatal("重复 End 后计数应为 0（不该活跃）")
	}
	// 单次 Begin 必须单次 End 即配平：一次 Begin 应立刻活跃——证明重复 End 没把
	// 计数打负（打负的话 Begin 后计数仍是 0/负 → 不活跃）。
	tbl.BeginWait(sid)
	if !tbl.SessionActive(sid) {
		t.Fatal("BeginWait 后应活跃（重复 End 没把计数打负）")
	}
	tbl.EndWait(sid)
	if tbl.SessionActive(sid) {
		t.Fatal("一次 End 应把单次 Begin 配平（计数没被打负）")
	}

	// Reset 清空 waiting（重建入口口径同 running）。
	tbl.BeginWait(sid)
	tbl.Reset()
	if tbl.SessionActive(sid) {
		t.Fatal("Reset 应清空 waiting 计数")
	}
	if _, ok := tbl.Session(sid); ok {
		t.Fatal("Reset 后 session 应已清空")
	}

	// nil 表纵深防御（与 BeginRun/EndRun 同口径）。
	var nilTbl *Table
	nilTbl.BeginWait(core.NewUlid())
	nilTbl.EndWait(core.NewUlid())
	if nilTbl.SessionActive(core.NewUlid()) {
		t.Fatal("nil 表的 SessionActive 应为 false")
	}
}

// TestVisiblePeersIncludesWaiting：③ waiting 算活跃 → 超窗对端靠 waiting 短路进
// 可见面（豁免窗口判定），EndWait 后落出。
func TestVisiblePeersIncludesWaiting(t *testing.T) {
	now := time.Now()
	a := core.NewUlid() // 查询方
	b := core.NewUlid() // 对端：事件新鲜但窗口极小 → 超窗
	agent := core.NewUlid()

	tbl := New()
	tbl.SetActivityWindow(nearWindow)
	joinAt := func(sid contracts.Ulid, at time.Time, seq int64) {
		applyAt(t, tbl, KindGateJoined, seq, sid, agent, at,
			contracts.GateJoinedPayload{GateID: "repo:X", Source: contracts.GateJoinSource(SourceAutoRepo)})
	}

	applyAt(t, tbl, "user.prompt", 1, a, agent, now, contracts.UserPromptPayload{Text: "a"})
	joinAt(a, now, 2)
	applyAt(t, tbl, "user.prompt", 3, b, agent, now, contracts.UserPromptPayload{Text: "b"})
	joinAt(b, now, 4)

	// 极小窗：全体超窗 → 对端不可见。
	if got := tbl.VisiblePeers(a); len(got) != 0 {
		t.Fatalf("极小窗下不该有可见对端: %+v", got)
	}
	// BeginWait(b)：waiting 短路 → b 算活跃 → 进可见面（同一 activeLocked，一处判定）。
	tbl.BeginWait(b)
	peers := tbl.VisiblePeers(a)
	if len(peers) != 1 || peers[0].SessionID != string(b) {
		t.Fatalf("waiting 的超窗对端应进可见面: %+v", peers)
	}
	// EndWait 回落 → 对端重新超窗、落出。
	tbl.EndWait(b)
	if got := tbl.VisiblePeers(a); len(got) != 0 {
		t.Fatalf("EndWait 后对端应回到超窗不可见: %+v", got)
	}
}
