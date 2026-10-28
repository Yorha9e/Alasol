package discovery

// 并发回归：Snapshot 的**递归取锁**死锁（p1）。
//
// 现象：Snapshot 持读锁时又调公开的取锁方法（Gates / JoinHistory 内部再 RLock）。
// Go 的 RWMutex 写者优先：一旦有写者（Apply 的 Lock）在等锁，递归的 RLock 永久阻塞
// → 读者等写者、写者等读者 → 死锁。-race 查不出（逻辑死锁，不是数据竞态）。
//
// 本测试用「多读者循环 Snapshot + 一写者循环 Apply」压出来；修好后必须**快速结束**
// （超时兜底：死锁时会被打断，测试红而不是整个包挂死）。
//
// 关键手法：**所有会卡死的循环都放在 goroutine 里**，主 goroutine 只负责等 + 超时。
// 若把 Snapshot 循环留在主 goroutine，主 goroutine 自己就卡死在 Snapshot 里，
// 超时兜底永远等不到 → 只能靠 go test 的全局 -timeout 兜底（红是红，但慢且信息差）。

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// TestSnapshotConcurrentWithApplyNoDeadlock：读者循环取快照 + 写者循环应用事件，
// 必须在超时内跑完（不 Deadlock）。
func TestSnapshotConcurrentWithApplyNoDeadlock(t *testing.T) {
	const (
		readers    = 4
		writers    = 1
		iterations = 200
	)
	tbl := New()
	sess := core.NewUlid()
	agent := core.NewUlid()

	// 预置一点数据（让快照非空）。
	applyAll(tbl, joinEv(t, 1, sess, agent, "repo:x", "auto"))

	// 事件在 goroutine 外造好（t.Fatalf 只能在测试 goroutine 里调）。
	evs := make([]contracts.Event, iterations)
	for i := range evs {
		evs[i] = joinEv(t, int64(2+i), sess, agent, "repo:y", "auto")
	}

	var wg sync.WaitGroup

	// 读者：不停取快照（走 Gates/JoinHistory 两条路径）。
	readersDone := make(chan struct{})
	var readersWG sync.WaitGroup
	for r := 0; r < readers; r++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for i := 0; i < iterations; i++ {
				snap := tbl.Snapshot()
				if len(snap.Sessions) == 0 {
					t.Error("快照里没有 session")
					return
				}
			}
		}()
	}
	go func() {
		readersWG.Wait()
		close(readersDone)
	}()

	// 写者：不停应用事件（触发写锁 → 逼出「写者优先」的死锁窗口）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range evs {
			ev := evs[i]
			tbl.Apply(&ev)
		}
	}()

	// 超时保护：死锁时这里会超时 → 测试红（而不是整个包挂死）。
	waitWithTimeout(t, readersDone, 20*time.Second)
	waitGroupWithTimeout(t, &wg, 20*time.Second)
}

// TestSnapshotAtomicUnderConcurrentWrites：快照必须是**原子**的——Sessions / Gates /
// Joins 三个切片必须来自**同一时刻**的表状态。
//
// # 为什么这版的数据形态是关键（修掉"虚假信心"）
//
// 旧版用「1 个 session × 1 个 gate × 300 条同 gate 事件」：join 按 (session, gate)
// 折叠，所以快照恒为 sessions=1、joins=1 → 断言 len(sessions) != len(joins) **恒不成立**
// → 把 Snapshot 改成非原子实现（先取 sessions、解锁、再取 gates/joins）后这个用例
// **照样 PASS**：它测的不是原子性，是"没有断言"。
//
// 现在三处一起改：
//  1. 数据改成**多 session、每个恰好 1 个 join** → 每条新事件同时 +1 session、+1 join，
//     计数不再恒定；
//  2. 写者**持续改表**（Reset + 重放 = 发现表崩溃重建的生产路径，discoveryFeed.rebuild
//     就是它）→ 取样几乎总会跨越一次写入；
//  3. 断言改成**跨切片自洽**（Sessions ↔ Gates ↔ Joins 双向一致），不只数个数。
//
// # 断言（原子快照下恒成立；非原子取样必破其中一条）
//
//	I1 |sessions| == |joins|                     每个 session 恰好 1 个 join
//	I2 joins 的 session id 都在 sessions 里       （配合 I1 即集合相等）
//	I3 每个 gate 的每个成员都在 sessions 里，且该 session 的 Gates 里含这个 gate
//	I4 每个 session 的 Gates 里的每个 gate 都在 gates 里，且该 gate 的成员含这个 session
//	I5 每条 join 的 gate 都在 gates 里，且该 gate 的成员含这条 join 的 session
//
// I3/I4/I5 是三个切片的**双向**一致性：任一切片早取/晚取一帧就会破坏其中一条
// （只数个数的话，"多了一个 session 但 join 还是旧的"这种错位会被漏掉）。
func TestSnapshotAtomicUnderConcurrentWrites(t *testing.T) {
	const (
		cycleSize = 48 // 每轮写入的 session 数（每个恰好 1 个 join）
		cycles    = 4  // 轮数：**每轮用不同的 session**（跨轮的 session 集合不同）
		gates     = 8  //
		snapshots = 2000
	)
	tbl := New()

	// 事件在 goroutine 外造好（t.Fatalf 只能在测试 goroutine 里调）。
	// 每轮用**不同的 session**：跨轮边界的撕裂会让"计数相等但成员不同"——
	// 只有跨切片自洽检查（I2/I3/I4/I5）能抓到，单数个数抓不到。
	evs := make([][]contracts.Event, cycles)
	for c := range evs {
		evs[c] = make([]contracts.Event, cycleSize)
		for i := range evs[c] {
			sess := core.NewUlid()
			agent := core.NewUlid()
			evs[c][i] = joinEv(t, int64(i+1), sess, agent, fmt.Sprintf("team:g%d", i%gates), "auto")
		}
	}

	// 写者：持续改表。Reset + 重放是发现表崩溃重建的生产路径（rebuild 就是它），
	// 同时它让表**规模有界**（不会把快照成本越推越高）而**内容一直在变**。
	// 写者不停 → 读者取样几乎总会跨越一次写入 → 非原子实现必被抓。
	stop := make(chan struct{})
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for c := 0; ; c++ {
			select {
			case <-stop:
				return
			default:
			}
			tbl.Reset()
			for i := range evs[c%cycles] {
				ev := evs[c%cycles][i]
				tbl.Apply(&ev)
			}
		}
	}()

	// 快照循环在 goroutine 里跑（主 goroutine 才能守住超时——快照死锁时循环里的
	// goroutine 永远回不来，主 goroutine 直接调 Snapshot 也会跟着卡死）。
	mismatch := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < snapshots; i++ {
			if msg := checkSnapshotAtomic(tbl.Snapshot()); msg != "" {
				select {
				case mismatch <- msg:
				default:
				}
				return
			}
		}
	}()

	waitWithTimeout(t, done, 30*time.Second)
	close(stop)
	waitGroupWithTimeout(t, &writerWG, 30*time.Second)
	select {
	case msg := <-mismatch:
		t.Fatal(msg)
	default:
	}
}

// TestCheckSnapshotAtomicDetectsTornSnapshots：**确定性地**证明跨切片自洽检查真的有
// 牙齿——不是只有"数个数"这一条。
//
// 下面每个用例都是"计数自洽、成员不自洽"的撕裂快照（|sessions| == |joins|，
// session id 集合也相等），只有 I3/I4/I5 能抓到；旧版那种
// `len(snap.Sessions) != len(snap.Joins)` 断言对它们**完全无感**（恒成立 → 虚假信心）。
func TestCheckSnapshotAtomicDetectsTornSnapshots(t *testing.T) {
	const (
		s1 = "S1"
		s2 = "S2"
		g1 = "team:g1"
		g2 = "team:g2"
	)
	cases := []struct {
		name string
		snap Snapshot
	}{
		{
			// gate 的成员比 session 行多一个（gate 切片晚取了一帧）。
			name: "gate 成员里有 session 行没记的 gate",
			snap: Snapshot{
				Sessions: []SessionRow{{SessionID: s1, Gates: []string{g1}}},
				Gates:    []GateRow{{GateID: g1, Members: []string{s1, s2}}},
				Joins:    []GateJoin{{GateID: g1, SessionID: s1}},
			},
		},
		{
			// session 行里的 gate 在 gates 切片里查无此人（session 切片晚取了一帧）。
			name: "session 行里的 gate 不在 gates 里",
			snap: Snapshot{
				Sessions: []SessionRow{{SessionID: s1, Gates: []string{g1}}},
				Gates:    []GateRow{},
				Joins:    []GateJoin{{GateID: g1, SessionID: s1}},
			},
		},
		{
			// gate 切片里有这个 gate，但成员表里没有这个 session（成员表早取了一帧）。
			name: "gate 的成员表缺 join 指向的 session",
			snap: Snapshot{
				Sessions: []SessionRow{{SessionID: s1, Gates: []string{g1}}, {SessionID: s2, Gates: []string{g1}}},
				Gates:    []GateRow{{GateID: g1, Members: []string{s1}}},
				Joins:    []GateJoin{{GateID: g1, SessionID: s1}, {GateID: g1, SessionID: s2}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := checkSnapshotAtomic(tc.snap)
			if msg == "" {
				t.Fatalf("撕裂快照被判为自洽（虚假信心）：%+v", tc.snap)
			}
			// 计数必须相等——否则这条用例证明不了"跨切片检查比数个数更强"。
			if len(tc.snap.Sessions) != len(tc.snap.Joins) {
				t.Fatalf("用例前提不成立：|sessions|=%d != |joins|=%d",
					len(tc.snap.Sessions), len(tc.snap.Joins))
			}
		})
	}

	// 反向：一份**自洽**的快照必须通过（否则检查器太严，会误报）。
	ok := Snapshot{
		Sessions: []SessionRow{{SessionID: s1, Gates: []string{g1}}, {SessionID: s2, Gates: []string{g2}}},
		Gates:    []GateRow{{GateID: g1, Members: []string{s1}}, {GateID: g2, Members: []string{s2}}},
		Joins:    []GateJoin{{GateID: g1, SessionID: s1}, {GateID: g2, SessionID: s2}},
	}
	if msg := checkSnapshotAtomic(ok); msg != "" {
		t.Fatalf("自洽快照被误判：%s", msg)
	}
}

// checkSnapshotAtomic 校验快照的**跨切片自洽**（Sessions ↔ Gates ↔ Joins 双向一致）。
// 自洽返回 ""，否则返回可读的失败原因。
func checkSnapshotAtomic(snap Snapshot) string {
	// I1：每个 session 恰好 1 个 join → 两个切片等长。
	if len(snap.Sessions) != len(snap.Joins) {
		return fmt.Sprintf("快照不自洽：|sessions|=%d != |joins|=%d",
			len(snap.Sessions), len(snap.Joins))
	}

	// 建索引：session → gate 集合；gate → 成员集合。
	sessGates := make(map[string]map[string]bool, len(snap.Sessions))
	for _, s := range snap.Sessions {
		g := make(map[string]bool, len(s.Gates))
		for _, gid := range s.Gates {
			g[gid] = true
		}
		sessGates[s.SessionID] = g
	}
	gateMembers := make(map[string]map[string]bool, len(snap.Gates))
	for _, g := range snap.Gates {
		m := make(map[string]bool, len(g.Members))
		for _, sid := range g.Members {
			m[sid] = true
		}
		gateMembers[g.GateID] = m
	}

	// I2：join 指向的 session 必须在 sessions 里（配合 I1 即两边集合相等）。
	for _, j := range snap.Joins {
		if _, ok := sessGates[j.SessionID]; !ok {
			return fmt.Sprintf("快照不自洽：join(%s) 指向的 session %s 不在 sessions 里",
				j.GateID, j.SessionID)
		}
	}
	// I3：gate 的成员都在 sessions 里，且该 session 的行里有这个 gate。
	for _, g := range snap.Gates {
		for sid := range gateMembers[g.GateID] {
			gs, ok := sessGates[sid]
			if !ok {
				return fmt.Sprintf("快照不自洽：gate %s 的成员 %s 不在 sessions 里", g.GateID, sid)
			}
			if !gs[g.GateID] {
				return fmt.Sprintf("快照不自洽：gate %s 的成员 %s 的 session 行里没有这个 gate",
					g.GateID, sid)
			}
		}
	}
	// I4：session 的 gate 都在 gates 里，且该 gate 的成员含这个 session。
	for _, s := range snap.Sessions {
		for gid := range sessGates[s.SessionID] {
			m, ok := gateMembers[gid]
			if !ok {
				return fmt.Sprintf("快照不自洽：session %s 的 gate %s 不在 gates 里", s.SessionID, gid)
			}
			if !m[s.SessionID] {
				return fmt.Sprintf("快照不自洽：session %s 的 gate %s 的成员表里没有它",
					s.SessionID, gid)
			}
		}
	}
	// I5：join 的 gate 都在 gates 里，且该 gate 的成员含这条 join 的 session。
	for _, j := range snap.Joins {
		m, ok := gateMembers[j.GateID]
		if !ok {
			return fmt.Sprintf("快照不自洽：join(%s) 的 gate %s 不在 gates 里", j.SessionID, j.GateID)
		}
		if !m[j.SessionID] {
			return fmt.Sprintf("快照不自洽：join(%s,%s) 的 gate 成员表里没有它",
				j.GateID, j.SessionID)
		}
	}
	return ""
}

// waitWithTimeout 等一个 channel（或 WaitGroup），超时即失败（死锁时给出可读的失败
// 而不是挂死）。
func waitWithTimeout(t *testing.T, done <-chan struct{}, d time.Duration) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("并发读写 %v 未结束（死锁？）", d)
	}
}

// waitGroupWithTimeout 等 WaitGroup，超时即失败（死锁时给出可读的失败而不是挂死）。
func waitGroupWithTimeout(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	waitWithTimeout(t, ch, d)
}

// 编译期提醒：contracts 的事件类型在并发用例里用到（防止 import 被误删）。
var _ = contracts.Event{}
