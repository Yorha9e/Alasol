package discovery

// G-a（session 在线三态）判定原语的单测：
//
//	① 活跃窗口：窗口内 / 窗口外 / 无 ts / running 短路（BeginRun 后即使无 ts 也活跃，
//	   EndRun 后回落）；
//	② SetActivityWindow：0 → 恢复缺省（core.DefaultActivityWindow）、显式覆盖生效；
//	③ VisiblePeers 按活跃过滤：活跃对端在列、超窗对端不在列、live 但无事件的不在列；
//	④ 行出口 SessionRow.Online / Running 现算。
//
// 时间控制**不注入时钟**：用"写死的事件 ts 夹具（过去 / 现在的 RFC3339 字符串）+
// SetActivityWindow（极小 / 极大）"组合控制（见 farWindow / nearWindow / applyAt）。

import (
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// farWindow 极大窗口（"极大"侧夹具）：把远过去的事件 ts 也罩进活跃窗。
const farWindow = 100 * 365 * 24 * time.Hour

// nearWindow 极小窗口（"极小"侧夹具）：任何真实事件 ts 都在窗外 → 立刻"超窗"。
const nearWindow = time.Nanosecond

// applyAt 组一条事件、把 TS 覆写成 at（RFC3339）再 Apply——相对时间夹具，
// 不依赖系统绝对日期（"5 分钟前""2 小时前"在任何跑测时刻都成立）。
func applyAt(t *testing.T, tbl *Table, kind string, seq int64, session, agent contracts.Ulid, at time.Time, payload any) string {
	t.Helper()
	ev := mkEvent(t, kind, seq, session, agent, payload)
	ev.TS = at.UTC().Format(time.RFC3339)
	tbl.Apply(&ev)
	return ev.TS
}

// TestSessionActiveWindowAndRunning：① 活跃判定的两个分量——窗口（事件 ts）与 running。
func TestSessionActiveWindowAndRunning(t *testing.T) {
	tbl := New()
	sid := core.NewUlid()

	// 未知 session → 不活跃（没有活动证据）。
	if tbl.SessionActive(sid) {
		t.Fatal("未知 session 不该活跃")
	}

	// 窗口内事件（5 分钟前 < 默认 15m）→ 活跃。
	recent := core.NewUlid()
	applyAt(t, tbl, "user.prompt", 1, recent, core.NewUlid(),
		time.Now().Add(-5*time.Minute), contracts.UserPromptPayload{Text: "recent"})
	if !tbl.SessionActive(recent) {
		t.Fatal("窗口内事件的 session 应活跃（默认 15m 窗）")
	}

	// 窗口外事件（2 小时前 > 默认 15m）→ 不活跃。
	old := core.NewUlid()
	applyAt(t, tbl, "user.prompt", 1, old, core.NewUlid(),
		time.Now().Add(-2*time.Hour), contracts.UserPromptPayload{Text: "old"})
	if tbl.SessionActive(old) {
		t.Fatal("超窗事件的 session 不该活跃（默认 15m 窗）")
	}

	// 无 ts（TS=""）→ 不活跃：判定源是事件 ts，没有 ts 就没有活动证据。
	noTS := core.NewUlid()
	ev := mkEvent(t, "user.prompt", 1, noTS, core.NewUlid(), contracts.UserPromptPayload{Text: "no ts"})
	ev.TS = ""
	tbl.Apply(&ev)
	if tbl.SessionActive(noTS) {
		t.Fatal("无 ts 的 session 不该活跃")
	}

	// running 短路：BeginRun 后即使超窗 / 无 ts 也算活跃；EndRun 回落。
	tbl.BeginRun(old)
	if !tbl.SessionActive(old) {
		t.Fatal("BeginRun 后即使超窗也应活跃（running 短路，不看 ts）")
	}
	tbl.BeginRun(noTS)
	if !tbl.SessionActive(noTS) {
		t.Fatal("BeginRun 后即使无 ts 也应活跃")
	}

	// EndRun 配平回落；重复 End 下限 0（不会把计数打成负——负计数会永远"在跑"）。
	tbl.EndRun(old)
	tbl.EndRun(old) // 多余的一次：下限 0
	if tbl.SessionActive(old) {
		t.Fatal("EndRun 后应回落为不活跃（重复 End 不残留计数）")
	}
	tbl.EndRun(noTS)
	if tbl.SessionActive(noTS) {
		t.Fatal("无 ts + 无 running 应回落为不活跃")
	}
	// 单次 Begin 必须单次 End 即配平（证明重复 End 没把计数打负）。
	tbl.BeginRun(old)
	if !tbl.SessionActive(old) {
		t.Fatal("BeginRun 后应活跃")
	}
	tbl.EndRun(old)
	if tbl.SessionActive(old) {
		t.Fatal("一次 End 应把单次 Begin 配平（计数没被打负）")
	}
}

// TestSetActivityWindowDefaultAndOverride：② SetActivityWindow——0 → 恢复缺省、显式覆盖生效。
func TestSetActivityWindowDefaultAndOverride(t *testing.T) {
	tbl := New()
	sid := core.NewUlid()
	applyAt(t, tbl, "user.prompt", 1, sid, core.NewUlid(),
		time.Now().Add(-2*time.Hour), contracts.UserPromptPayload{Text: "old"})

	// 零值表 = 缺省 15m：2 小时前的事件超窗 → 不活跃。
	if tbl.SessionActive(sid) {
		t.Fatal("零值窗口应是缺省 15m：2 小时前的事件不该活跃")
	}
	// 显式覆盖生效：3h 窗把 2 小时前的事件罩进来。
	tbl.SetActivityWindow(3 * time.Hour)
	if !tbl.SessionActive(sid) {
		t.Fatal("显式覆盖 3h 窗应让 2 小时前的事件活跃")
	}
	// d<=0 → 恢复缺省（"0 = 用默认"口径）：再次超窗。
	tbl.SetActivityWindow(0)
	if tbl.SessionActive(sid) {
		t.Fatal("SetActivityWindow(0) 应恢复缺省 15m 窗（2 小时前重新超窗）")
	}
	tbl.SetActivityWindow(-time.Minute) // 负值同样恢复缺省
	if tbl.SessionActive(sid) {
		t.Fatal("SetActivityWindow(负) 应恢复缺省 15m 窗")
	}
	// nil 表不 panic（纵深防御，与其余方法同口径）。
	var nilTbl *Table
	nilTbl.SetActivityWindow(time.Hour)
	nilTbl.BeginRun(core.NewUlid())
	nilTbl.EndRun(core.NewUlid())
	if nilTbl.SessionActive(core.NewUlid()) {
		t.Fatal("nil 表的 SessionActive 应为 false")
	}
}

// TestVisiblePeersFiltersInactivePeers：③ VisiblePeers 条件①（G-a）——对端必须活跃
// （running ∨ 窗口内），live/托管不再是条件。
func TestVisiblePeersFiltersInactivePeers(t *testing.T) {
	now := time.Now()
	a := core.NewUlid() // 查询方（活跃）
	b := core.NewUlid() // 对端：窗口内事件 → 活跃
	c := core.NewUlid() // 对端：2 小时前事件 → 超窗
	d := core.NewUlid() // 对端：live 但一条事件都没有
	agent := core.NewUlid()

	tbl := New()
	joinAt := func(sid contracts.Ulid, at time.Time, seq int64) {
		applyAt(t, tbl, KindGateJoined, seq, sid, agent, at,
			contracts.GateJoinedPayload{GateID: "repo:X", Source: contracts.GateJoinSource(SourceAutoRepo)})
	}

	applyAt(t, tbl, "user.prompt", 1, a, agent, now, contracts.UserPromptPayload{Text: "a"})
	joinAt(a, now, 2)
	applyAt(t, tbl, "user.prompt", 3, b, agent, now, contracts.UserPromptPayload{Text: "b"})
	joinAt(b, now, 4)
	applyAt(t, tbl, "user.prompt", 5, c, agent, now.Add(-2*time.Hour), contracts.UserPromptPayload{Text: "c"})
	joinAt(c, now.Add(-2*time.Hour), 6)
	tbl.Activate(a)
	tbl.Activate(b)
	tbl.Activate(c)
	tbl.Activate(d) // d：托管但无事件

	peers := tbl.VisiblePeers(a)
	if len(peers) != 1 || peers[0].SessionID != string(b) {
		t.Fatalf("默认窗下 a 的可见对端应只有活跃的 b，得到 %+v（c 超窗、d 无事件）", peers)
	}

	// 显式放大窗口：c 变活跃 → 进列（d 仍无事件、推不出主 agent，恒不在）。
	tbl.SetActivityWindow(farWindow)
	if got := tbl.VisiblePeers(a); len(got) != 2 {
		t.Fatalf("极大窗口下 b、c 都该在列: %+v", got)
	}

	// 极小窗口（全体超窗）+ 只有 b 在跑：b 靠 running 短路仍在列，c 落出。
	tbl.SetActivityWindow(nearWindow)
	tbl.BeginRun(b)
	peers = tbl.VisiblePeers(a)
	if len(peers) != 1 || peers[0].SessionID != string(b) {
		t.Fatalf("超窗后仅 running 的 b 该在列: %+v", peers)
	}
	tbl.EndRun(b)
	if got := tbl.VisiblePeers(a); len(got) != 0 {
		t.Fatalf("全体超窗且无 running → 对端应为空: %+v", got)
	}
}

// TestSessionRowOnlineRunning：④ 行出口（Session / LiveSessions / Snapshot 同一口径）
// 的 Online / Running 现算。
func TestSessionRowOnlineRunning(t *testing.T) {
	tbl := New()
	sid := core.NewUlid()
	ts := applyAt(t, tbl, "user.prompt", 1, sid, core.NewUlid(),
		time.Now().Add(-2*time.Hour), contracts.UserPromptPayload{Text: "old"})

	// 超窗且未 running：Online=false、Running=false；UpdatedAt 仍是最后事件 ts（不动）。
	row, ok := tbl.Session(sid)
	if !ok {
		t.Fatal("session 不在表里")
	}
	if row.Online || row.Running {
		t.Fatalf("超窗且未 running 的行不该 Online/Running: %+v", row)
	}
	if row.UpdatedAt != ts {
		t.Fatalf("UpdatedAt 应仍是最后事件 ts %q，得到 %q", ts, row.UpdatedAt)
	}

	// BeginRun → Running=true、Online=true（running 短路）。
	tbl.BeginRun(sid)
	row, _ = tbl.Session(sid)
	if !row.Running || !row.Online {
		t.Fatalf("BeginRun 后行应 Running+Online: %+v", row)
	}
	tbl.EndRun(sid)

	// 显式放大窗口 → Online=true、Running=false。
	tbl.SetActivityWindow(farWindow)
	row, _ = tbl.Session(sid)
	if !row.Online || row.Running {
		t.Fatalf("大窗口下行应 Online 且不 Running: %+v", row)
	}

	// Snapshot / LiveSessions 的行出口同口径（同一 activeLocked，一处判定）。
	tbl.Activate(sid)
	for _, r := range tbl.Snapshot().Sessions {
		if r.SessionID == string(sid) && (!r.Online || r.Running) {
			t.Fatalf("Snapshot 行 Online/Running 不符: %+v", r)
		}
	}
	for _, r := range tbl.LiveSessions() {
		if r.SessionID == string(sid) && (!r.Online || r.Running) {
			t.Fatalf("LiveSessions 行 Online/Running 不符: %+v", r)
		}
	}
}
