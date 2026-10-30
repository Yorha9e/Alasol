package discovery

// G15 验收：gate.left 折叠口径（成员 = joined 减 left）+ 届数计数（重 join 的新事件 id 基础）。
//
// 折叠必须**乱序稳定**：Apply 允许乱序/重复（表重建时事件不保证有序到达），所以 leave 与
// join 各记各的最大 seq、活跃判据 = join.seq > left.seq——先到后到结论一致。

import (
	"reflect"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// leftEv 是一条 gate.left 事件（与 joinEv 对称）。
func leftEv(t *testing.T, seq int64, session, agent contracts.Ulid, gateID, source string) contracts.Event {
	t.Helper()
	return mkEvent(t, KindGateLeft, seq, session, agent,
		contracts.GateLeftPayload{GateID: gateID, Source: contracts.GateJoinSource(source)})
}

// TestGateLeftRemovesMembership：join→left 后该 gate 不再是活跃成员（GatesOf/InGate/
// GateMembers/Gates 全口径一致），但 **gate 定义仍在**（成员数 0，不删键）。
func TestGateLeftRemovesMembership(t *testing.T) {
	s, a := core.NewUlid(), core.NewUlid()
	tbl := New()
	applyAll(tbl,
		joinEv(t, 1, s, a, "team:x", "explicit"),
		leftEv(t, 2, s, a, "team:x", "explicit"),
	)
	if tbl.InGate(s, "team:x") {
		t.Fatal("leave 后 InGate 仍为 true（成员关系未终止）")
	}
	if got := tbl.GatesOf(s); len(got) != 0 {
		t.Fatalf("leave 后 GatesOf = %v，want 空", got)
	}
	if got := tbl.GateMembers("team:x"); len(got) != 0 {
		t.Fatalf("leave 后 GateMembers = %v，want 空", got)
	}
	// gate 定义持久化（G17b）：成员走光了 gate 仍在枚举里（成员数 0）。
	gates := tbl.Gates()
	if len(gates) != 1 || gates[0].GateID != "team:x" || len(gates[0].Members) != 0 {
		t.Fatalf("Gates = %+v，want 恰一条 team:x 且 0 成员", gates)
	}
	// join 事实仍在（审计：JoinHistory 只答"加入过没有"，不答"现在在不在"）。
	if got := tbl.JoinHistory(); len(got) != 1 || got[0].GateID != "team:x" {
		t.Fatalf("JoinHistory = %+v，want 一条 team:x（leave 不擦除 join 事实）", got)
	}
}

// TestGateLeftThenRejoinRestores：leave 之后再 join 恢复可见，且届数 +1
// （host 靠届数算确定性事件 id：重 join 必须是一届新事件，否则被 store 去重吃掉）。
func TestGateLeftThenRejoinRestores(t *testing.T) {
	s, a := core.NewUlid(), core.NewUlid()
	tbl := New()
	tbl.Activate(s) // GateMembers = 活跃成员 ∩ 存活（G17b）
	applyAll(tbl, joinEv(t, 1, s, a, "team:x", "explicit"))
	if gen := tbl.JoinGeneration(s, "team:x"); gen != 1 {
		t.Fatalf("join 后 JoinGeneration = %d，want 1", gen)
	}
	applyAll(tbl, leftEv(t, 2, s, a, "team:x", "explicit"))
	if gen := tbl.JoinGeneration(s, "team:x"); gen != 1 {
		t.Fatalf("leave 后 JoinGeneration = %d，want 1（leave 不计届）", gen)
	}
	applyAll(tbl, joinEv(t, 3, s, a, "team:x", "explicit"))
	if !tbl.InGate(s, "team:x") {
		t.Fatal("重 join 后 InGate 仍为 false（成员关系未恢复）")
	}
	if gen := tbl.JoinGeneration(s, "team:x"); gen != 2 {
		t.Fatalf("重 join 后 JoinGeneration = %d，want 2", gen)
	}
	if got := tbl.GateMembers("team:x"); len(got) != 1 || got[0].SessionID != string(s) {
		t.Fatalf("重 join 后 GateMembers = %+v，want [%s]", got, s)
	}
}

// TestGateLeftFoldIsOrderIndependent：乱序/重复 Apply 结论一致（表重建的硬要求）。
//
// 三个排列（leave 先到 / join 先到 / 重复投递）必须折出同一个成员关系。
func TestGateLeftFoldIsOrderIndependent(t *testing.T) {
	s, a := core.NewUlid(), core.NewUlid()

	// 排列①：join(5) → leave(7) → 重复投递两者。
	t1 := New()
	applyAll(t1,
		joinEv(t, 5, s, a, "team:x", "explicit"),
		leftEv(t, 7, s, a, "team:x", "explicit"),
		joinEv(t, 5, s, a, "team:x", "explicit"), // 重复
		leftEv(t, 7, s, a, "team:x", "explicit"), // 重复
	)
	// 排列②：leave(7) 先到（其时还没有 join），join(5) 后到 → leave 仍胜（seq 大）。
	t2 := New()
	applyAll(t2,
		leftEv(t, 7, s, a, "team:x", "explicit"),
		joinEv(t, 5, s, a, "team:x", "explicit"),
	)
	// 排列③：旧 leave(3) 晚到 → 盖不掉新 join(5)，仍活跃。
	t3 := New()
	applyAll(t3,
		joinEv(t, 5, s, a, "team:x", "explicit"),
		leftEv(t, 3, s, a, "team:x", "explicit"),
	)

	for name, tbl := range map[string]*Table{"①重复投递": t1, "②leave先到": t2, "③旧leave晚到": t3} {
		want := name == "③旧leave晚到" // ①②都该"已离开"；③该"仍在网内"
		if got := tbl.InGate(s, "team:x"); got != want {
			t.Errorf("%s：InGate = %v，want %v", name, got, want)
		}
	}
	// ①②：join 事实仍在（审计），成员关系已终止。
	for _, tbl := range []*Table{t1, t2} {
		if got := len(tbl.JoinHistory()); got != 1 {
			t.Errorf("JoinHistory = %d 条，want 1（join 事实不因 leave 消失）", got)
		}
	}
	// 届数只由 join 折叠推进：①②各 1 次 join → 1；③ 同。
	for _, tbl := range []*Table{t1, t2, t3} {
		if gen := tbl.JoinGeneration(s, "team:x"); gen != 1 {
			t.Errorf("JoinGeneration = %d，want 1（重复/乱序不累加届数）", gen)
		}
	}
}

// TestGateLeftWithZeroSeqRemovesMembership（F5）：**seq=0** 的 leave（导入 / 合成 / 还没
// 落库的事件都可能没有 seq）也必须终止成员关系。
//
// 判据是"lefts 里有没有这个 gate 的键"，**不是** "leave.seq > 0" 哨兵——用 seq>0 当哨兵
// 会把"leave 过但 seq=0"当成"从没 leave 过"→ 成员关系被静默复活（宿主据此判断"显式
// 离开过"时也会判错 → 自动 gate 把用户的显式离开撤销）。
func TestGateLeftWithZeroSeqRemovesMembership(t *testing.T) {
	s, a := core.NewUlid(), core.NewUlid()
	tbl := New()
	tbl.Activate(s)
	applyAll(tbl,
		joinEv(t, 0, s, a, "team:x", "explicit"), // seq=0 的 join
		leftEv(t, 0, s, a, "team:x", "explicit"), // seq=0 的 leave
	)
	if tbl.InGate(s, "team:x") {
		t.Error("seq=0 的 leave 后 InGate 仍为 true（成员关系没终止）")
	}
	if got := tbl.GatesOf(s); len(got) != 0 {
		t.Errorf("seq=0 的 leave 后 GatesOf = %v，want 空", got)
	}
	if got := tbl.GateMembers("team:x"); len(got) != 0 {
		t.Errorf("seq=0 的 leave 后 GateMembers = %v，want 空", got)
	}
	// LeftGate：曾显式离开过 = true（宿主据此**不自动复活**该 gate 的成员关系）。
	if !tbl.LeftGate(s, "team:x") {
		t.Error("LeftGate = false，want true（seq=0 的 leave 也是 leave）")
	}
	// 没 leave 过的 gate → LeftGate = false（"从没 join 过"与"leave 过"必须分得开）。
	if tbl.LeftGate(s, "team:y") {
		t.Error("LeftGate(没 leave 过的 gate) = true，want false")
	}
	// join 事实仍在（审计）：leave 不擦除 join 历史。
	if got := tbl.JoinHistory(); len(got) != 1 {
		t.Errorf("JoinHistory = %d 条，want 1（leave 不擦除 join 事实）", len(got))
	}
}

// TestGateLeftBreaksVisibility：K11 水平可见性跟上 fold——任一侧 leave 后两侧互不可见。
func TestGateLeftBreaksVisibility(t *testing.T) {
	s1, a1 := core.NewUlid(), core.NewUlid()
	s2, a2 := core.NewUlid(), core.NewUlid()
	tbl := New()
	tbl.Activate(s1)
	tbl.Activate(s2)
	applyAll(tbl,
		joinEv(t, 1, s1, a1, "team:x", "explicit"),
		joinEv(t, 2, s2, a2, "team:x", "explicit"),
	)
	if got := len(tbl.VisiblePeers(s1)); got != 1 {
		t.Fatalf("join 后 s1 的可见对端 = %d，want 1", got)
	}
	// s2 离开 → 两侧都看不见对方（共享 gate 没了）。
	applyAll(tbl, leftEv(t, 3, s2, a2, "team:x", "explicit"))
	if got := len(tbl.VisiblePeers(s1)); got != 0 {
		t.Fatalf("s2 leave 后 s1 的可见对端 = %d，want 0", got)
	}
	if got := len(tbl.VisiblePeers(s2)); got != 0 {
		t.Fatalf("s2 leave 后 s2 的可见对端 = %d，want 0", got)
	}
	// s2 重 join → 恢复互见。
	applyAll(tbl, joinEv(t, 4, s2, a2, "team:x", "explicit"))
	if got := len(tbl.VisiblePeers(s1)); got != 1 {
		t.Fatalf("s2 重 join 后 s1 的可见对端 = %d，want 1", got)
	}
}

// TestGateLeftSurvivesRebuild：leave 事件进快照口径——Reset 后全量重放得到同一个 Snapshot
// （崩溃重启后成员关系一致：leave 是 durable 事实，回放即恢复）。
func TestGateLeftSurvivesRebuild(t *testing.T) {
	s, a := core.NewUlid(), core.NewUlid()
	// 事件只造一次（id 固定）：快照含 EventID，重放必须用**同一批**事件（只是顺序不同），
	// 否则比的是两批不同的随机 id——那是测试噪音不是折叠回归。
	evs := []contracts.Event{
		joinEv(t, 1, s, a, "team:x", "explicit"),
		joinEv(t, 2, s, a, "team:y", "auto_repo"),
		leftEv(t, 3, s, a, "team:x", "explicit"),
	}
	tbl := New()
	tbl.Activate(s)
	applyAll(tbl, evs...)
	before := tbl.Snapshot()

	tbl.Reset()
	// 乱序重放（模拟"事件不保证有序到达"的重建窗口）。
	applyAll(tbl, evs[2], evs[1], evs[0])
	if got := tbl.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("重放后快照不一致：\n got %+v\nwant %+v", got, before)
	}
	// 快照口径：team:x 已 leave（0 活跃成员）、team:y 仍在（1 成员）。
	for _, g := range before.Gates {
		switch g.GateID {
		case "team:x":
			if len(g.Members) != 0 {
				t.Errorf("team:x 成员 = %v，want 空（已 leave）", g.Members)
			}
		case "team:y":
			if len(g.Members) != 1 || g.Members[0] != string(s) {
				t.Errorf("team:y 成员 = %v，want [%s]", g.Members, s)
			}
		default:
			t.Errorf("意外的 gate: %s", g.GateID)
		}
	}
}
