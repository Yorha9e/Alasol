package indexer_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime"
	"github.com/Yorha9e/alasol/runtime/indexer"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

var (
	tSession = contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")
	tAgent   = contracts.Ulid("TEECY7V57SP0J9WZ620EHHP02M")
)

// ev 造一个通用 durable 事件。
func ev(id contracts.Ulid, kind string, session contracts.Ulid, payload string) contracts.Event {
	return contracts.Event{
		V: 1, ID: id, Kind: kind, TS: "2025-03-01T10:00:00.000Z",
		SessionID: session, AgentID: tAgent, Durable: true, Payload: json.RawMessage(payload),
	}
}

// ledgerEv 造一条 ledger.updated 全量快照事件。
func ledgerEv(id contracts.Ulid, session contracts.Ulid, goals, constraints, completed, pending []string) contracts.Event {
	p := contracts.LedgerUpdatedPayload{Goals: goals, Constraints: constraints, Completed: completed, Pending: pending}
	b, _ := json.Marshal(p)
	return ev(id, "ledger.updated", session, string(b))
}

// boardEv 造一条 board.written 事件（value 为原始 JSON；tombstone=true 为删除）。
func boardEv(id contracts.Ulid, session contracts.Ulid, key, value, author string, tombstone bool) contracts.Event {
	p := contracts.BoardWrittenPayload{Key: key, Value: json.RawMessage(value), Author: &author}
	if tombstone {
		tr := true
		p.Tombstone = &tr
	}
	b, _ := json.Marshal(p)
	return ev(id, "board.written", session, string(b))
}

// setup 开库 + 追加给定事件 + 建索引器。
//
// 索引器由 sqlite.NewIndexer 装配：派生表写句柄由存储实现内部交给索引器，
// 外部包拿不到（G17）。
func setup(t *testing.T, mode string, batch int, events []contracts.Event) (*sqlite.Store, *indexer.Indexer) {
	t.Helper()
	path := ":memory:"
	if mode == "file" {
		path = t.TempDir() + "/idx.db"
	}
	st, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ctx := context.Background()
	if len(events) > 0 {
		if _, err := st.Append(ctx, events); err != nil {
			t.Fatalf("seed append: %v", err)
		}
	}
	ix, err := sqlite.NewIndexer(st, batch)
	if err != nil {
		t.Fatalf("new indexer: %v", err)
	}
	return st, ix
}

// ---- 投影读取助手（读也走索引器：外部包拿不到裸 SQL 句柄，G17）----

func readLedger(t *testing.T, ix *indexer.Indexer, sid contracts.Ulid) indexer.LedgerState {
	t.Helper()
	st, ok, err := ix.ReadLedger(context.Background(), sid)
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	if !ok {
		t.Fatalf("ledger state of %s not found", sid)
	}
	return st
}

func readBoard(t *testing.T, ix *indexer.Indexer, sid contracts.Ulid, key string) (indexer.BoardEntry, bool) {
	t.Helper()
	e, ok, err := ix.ReadBoard(context.Background(), sid, key)
	if err != nil {
		t.Fatalf("read board: %v", err)
	}
	return e, ok
}

func readCost(t *testing.T, ix *indexer.Indexer, sid contracts.Ulid) indexer.CostStats {
	t.Helper()
	st, ok, err := ix.ReadCostStats(context.Background(), sid)
	if err != nil {
		t.Fatalf("read cost: %v", err)
	}
	if !ok {
		t.Fatalf("cost stats of %s not found", sid)
	}
	return st
}

func readSession(t *testing.T, ix *indexer.Indexer, sid contracts.Ulid) indexer.SessionStats {
	t.Helper()
	st, ok, err := ix.ReadSession(context.Background(), sid)
	if err != nil {
		t.Fatalf("read session: %v", err)
	}
	if !ok {
		t.Fatalf("session %s not found", sid)
	}
	return st
}

// drain 反复 Step 到追平。
func drain(t *testing.T, ix *indexer.Indexer) int {
	t.Helper()
	total := 0
	for {
		n, err := ix.Step(context.Background())
		if err != nil {
			t.Fatalf("step: %v", err)
		}
		if n == 0 {
			return total
		}
		total += n
	}
}

// TestStepAdvancesWatermark：Step 推进水位，追平后返回 0。
func TestStepAdvancesWatermark(t *testing.T) {
	events := []contracts.Event{
		ev(core.NewUlid(), "tool.call.intent", tSession, `{}`),
		ev(core.NewUlid(), "tool.call.completed", tSession, `{}`),
		ledgerEv(core.NewUlid(), tSession, []string{"g"}, nil, nil, nil),
	}
	_, ix := setup(t, "memory", 100, events)
	ctx := context.Background()

	if wm, _ := ix.Watermark(); wm != 0 {
		t.Fatalf("initial watermark = %d, want 0", wm)
	}
	n, err := ix.Step(ctx)
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n != 3 {
		t.Fatalf("Step processed %d, want 3", n)
	}
	if wm, _ := ix.Watermark(); wm != 3 {
		t.Fatalf("watermark after step = %d, want 3", wm)
	}
	if n, _ := ix.Step(ctx); n != 0 {
		t.Fatalf("Step when drained returned %d, want 0", n)
	}
}

// TestUnknownKindTolerated：未知 kind 忽略但计数（D7），水位照常推进。
func TestUnknownKindTolerated(t *testing.T) {
	events := []contracts.Event{
		ev(core.NewUlid(), "tool.call.intent", tSession, `{"x":1}`), // 索引器不识别
		ev(core.NewUlid(), "policy.decided", tSession, `{}`),        // 也不识别
	}
	_, ix := setup(t, "memory", 100, events)
	drain(t, ix)

	stats := readCost(t, ix, tSession)
	if stats.EventCount != 2 {
		t.Fatalf("cost total = %d, want 2 (unknown kinds still counted)", stats.EventCount)
	}
	if stats.ByKind["tool.call.intent"] != 1 || stats.ByKind["policy.decided"] != 1 {
		t.Fatalf("by_kind = %v, want both unknown kinds counted", stats.ByKind)
	}
	if wm, _ := ix.Watermark(); wm != 2 {
		t.Fatalf("watermark = %d, want 2 (advanced past unknown kinds)", wm)
	}
}

// TestLedgerLWW：ledger.updated 全量快照 LWW（G23/G8）。
func TestLedgerLWW(t *testing.T) {
	events := []contracts.Event{
		ledgerEv(core.NewUlid(), tSession, []string{"goal-A"}, []string{"c1"}, []string{"done1"}, []string{"p1"}),
		ledgerEv(core.NewUlid(), tSession, []string{"goal-B", "goal-B2"}, []string{"c2"}, nil, []string{"p2"}),
	}
	_, ix := setup(t, "memory", 100, events)
	drain(t, ix)

	led := readLedger(t, ix, tSession)
	if len(led.Goals) != 2 || led.Goals[0] != "goal-B" {
		t.Fatalf("goals = %v, want last-write-wins snapshot", led.Goals)
	}
	if led.LastSeq != 2 {
		t.Fatalf("ledger last_seq = %d, want 2", led.LastSeq)
	}
	if len(led.Constraints) != 1 || led.Constraints[0] != "c2" {
		t.Fatalf("constraints = %v, want [c2]", led.Constraints)
	}
	// nil completed 规范化成空切片。
	if len(led.Completed) != 0 {
		t.Fatalf("completed = %v, want empty (nil normalized)", led.Completed)
	}
}

// TestBoardLWWAndTombstone：黑板键级 LWW + 墓碑删除（K22）。
func TestBoardLWWAndTombstone(t *testing.T) {
	st, ix := setup(t, "memory", 100, nil)
	ctx := context.Background()

	appendOne := func(e contracts.Event) {
		if _, err := st.Append(ctx, []contracts.Event{e}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	appendOne(boardEv(core.NewUlid(), tSession, "K", `1`, "agentA", false))
	appendOne(boardEv(core.NewUlid(), tSession, "K", `2`, "agentB", false))
	drain(t, ix)
	e, ok := readBoard(t, ix, tSession, "K")
	if !ok || e.Value != "2" || e.Author != "agentB" || e.UpdatedSeq != 2 {
		t.Fatalf("board K = (%+v,ok=%v), want (2,agentB,2)", e, ok)
	}

	// 墓碑删除（读取时墓碑行视为不存在）。
	appendOne(boardEv(core.NewUlid(), tSession, "K", ``, "", true))
	drain(t, ix)
	if _, ok := readBoard(t, ix, tSession, "K"); ok {
		t.Fatalf("board K should be gone after tombstone")
	}

	// 删除后可再写（墓碑不是永久禁写）。
	appendOne(boardEv(core.NewUlid(), tSession, "K", `3`, "agentC", false))
	drain(t, ix)
	e, ok = readBoard(t, ix, tSession, "K")
	if !ok || e.Value != "3" || e.UpdatedSeq != 4 {
		t.Fatalf("board K after rewrite = (%+v,ok=%v), want (3,4)", e, ok)
	}
}

// TestBoardEmptyValueIsReadable：写了空值（非墓碑）的键仍可读（value 存 NULL，不炸）。
func TestBoardEmptyValueIsReadable(t *testing.T) {
	st, ix := setup(t, "memory", 100, nil)
	ctx := context.Background()
	if _, err := st.Append(ctx, []contracts.Event{boardEv(core.NewUlid(), tSession, "E", ``, "agentA", false)}); err != nil {
		t.Fatalf("append: %v", err)
	}
	drain(t, ix)
	e, ok := readBoard(t, ix, tSession, "E")
	if !ok {
		t.Fatalf("空值键应存在（不是墓碑）")
	}
	if e.Value != "" || e.Author != "agentA" {
		t.Fatalf("board E = %+v, want value=\"\" author=agentA", e)
	}
}

// TestSessionsProjection：session 目录（首/末 seq、事件数、最后 kind）。
func TestSessionsProjection(t *testing.T) {
	events := []contracts.Event{
		ev(core.NewUlid(), "tool.call.intent", tSession, `{}`),
		ev(core.NewUlid(), "ledger.updated", tSession, `{"goals":[],"constraints":[],"completed":[],"pending":[]}`),
		ev(core.NewUlid(), "board.written", tSession, `{"key":"k","value":1}`),
	}
	_, ix := setup(t, "memory", 100, events)
	drain(t, ix)

	s := readSession(t, ix, tSession)
	if s.FirstSeq != 1 || s.LastSeq != 3 || s.EventCount != 3 {
		t.Fatalf("sessions = %+v, want (first=1,last=3,count=3)", s)
	}
	if s.LastKind != "board.written" {
		t.Fatalf("last_kind = %q, want board.written", s.LastKind)
	}
}

// TestRebuildSameTerminalState：Rebuild 重建到与增量一致的终态（G24②）。
func TestRebuildSameTerminalState(t *testing.T) {
	events := []contracts.Event{
		ev(core.NewUlid(), "tool.call.intent", tSession, `{}`),
		ledgerEv(core.NewUlid(), tSession, []string{"g1"}, nil, nil, nil),
		ledgerEv(core.NewUlid(), tSession, []string{"g2"}, []string{"c"}, nil, nil),
		boardEv(core.NewUlid(), tSession, "K", `1`, "a", false),
		boardEv(core.NewUlid(), tSession, "K", ``, "", true), // 删除 K
		boardEv(core.NewUlid(), tSession, "J", `9`, "b", false),
		ev(core.NewUlid(), "unknown.kind", tSession, `{}`),
	}
	_, ix := setup(t, "file", 100, events)
	ctx := context.Background()
	drain(t, ix)

	snapshot := func() string {
		led, _, _ := ix.ReadLedger(ctx, tSession)
		boardK, okK, _ := ix.ReadBoard(ctx, tSession, "K")
		boardJ, okJ, _ := ix.ReadBoard(ctx, tSession, "J")
		cost, _, _ := ix.ReadCostStats(ctx, tSession)
		sess, _, _ := ix.ReadSession(ctx, tSession)
		return jsonOf(led, boardK, okK, boardJ, okJ, cost, sess)
	}

	before := snapshot()
	wmBefore, _ := ix.Watermark()
	if err := ix.Rebuild(ctx); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	after := snapshot()
	wmAfter, _ := ix.Watermark()

	if before != after {
		t.Fatalf("Rebuild changed terminal state:\n--- before ---\n%s\n--- after ---\n%s", before, after)
	}
	if wmBefore != wmAfter {
		t.Fatalf("Rebuild changed watermark: before=%d after=%d", wmBefore, wmAfter)
	}
}

// jsonOf 把投影读面拼成可比较字符串（快照用）。
func jsonOf(v ...any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// TestCrashResume：中途 Close 再 Open，从水位继续，不丢不重（G24①）。
func TestCrashResume(t *testing.T) {
	path := t.TempDir() + "/crash.db"
	ctx := context.Background()

	// 造 10 条事件。
	events := make([]contracts.Event, 10)
	for i := range events {
		kind := "tool.call.intent"
		if i == 5 {
			kind = "ledger.updated"
			events[i] = ledgerEv(core.NewUlid(), tSession, []string{"g"}, nil, nil, nil)
			continue
		}
		events[i] = ev(core.NewUlid(), kind, tSession, `{}`)
	}

	st, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := st.Append(ctx, events); err != nil {
		t.Fatalf("append: %v", err)
	}
	ix, err := sqlite.NewIndexer(st, 4) // 小批，分多步
	if err != nil {
		t.Fatalf("new indexer: %v", err)
	}
	// 处理两批（8 条）。
	if n, _ := ix.Step(ctx); n != 4 {
		t.Fatalf("step1 = %d, want 4", n)
	}
	if n, _ := ix.Step(ctx); n != 4 {
		t.Fatalf("step2 = %d, want 4", n)
	}
	if wm, _ := ix.Watermark(); wm != 8 {
		t.Fatalf("watermark before crash = %d, want 8", wm)
	}
	// 崩溃：关闭 DB。
	if err := st.Close(); err != nil {
		t.Fatalf("close (crash): %v", err)
	}

	// 重启：重开 store + indexer。
	st2, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	ix2, err := sqlite.NewIndexer(st2, 4)
	if err != nil {
		t.Fatalf("new indexer 2: %v", err)
	}
	if wm, _ := ix2.Watermark(); wm != 8 {
		t.Fatalf("watermark after restart = %d, want 8 (persisted)", wm)
	}
	// 续跑剩余 2 条。
	if n, _ := ix2.Step(ctx); n != 2 {
		t.Fatalf("resume step = %d, want 2", n)
	}
	if n, _ := ix2.Step(ctx); n != 0 {
		t.Fatalf("drained step = %d, want 0", n)
	}
	if wm, _ := ix2.Watermark(); wm != 10 {
		t.Fatalf("final watermark = %d, want 10", wm)
	}

	// 不丢不重：10 条事件各计一次。
	if total := readCost(t, ix2, tSession).EventCount; total != 10 {
		t.Fatalf("cost total = %d, want 10 (no loss, no dup)", total)
	}
	if count := readSession(t, ix2, tSession).EventCount; count != 10 {
		t.Fatalf("session event_count = %d, want 10", count)
	}
}

// TestWatermarkMonotonic：多次 Step 水位只增不减。
func TestWatermarkMonotonic(t *testing.T) {
	st, ix := setup(t, "memory", 2, nil)
	ctx := context.Background()
	for i := 0; i < 7; i++ {
		if _, err := st.Append(ctx, []contracts.Event{ev(core.NewUlid(), "k", tSession, `{}`)}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	var last int64 = -1
	for {
		if _, err := ix.Step(ctx); err != nil {
			t.Fatalf("step: %v", err)
		}
		wm, _ := ix.Watermark()
		if wm < last {
			t.Fatalf("watermark decreased: %d -> %d", last, wm)
		}
		last = wm
		if wm >= 7 {
			break
		}
	}
}

// TestRunDrainsAndCancels：Run 追平后按轮询等待，ctx 取消即返回。
// 用文件库（WAL）：并发「读（Run）+ 写（append）」是真实场景，WAL 下读写不互斥；
// :memory: 的 cache=shared 在跨连接读-写并发下会 SQLITE_LOCKED（busy_timeout 不覆盖），
// 故并发场景走文件库。
func TestRunDrainsAndCancels(t *testing.T) {
	st, ix := setup(t, "file", 100, nil)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- ix.Run(ctx) }()

	// 追加 5 条，等 Run 追平。
	events := make([]contracts.Event, 5)
	for i := range events {
		events[i] = ev(core.NewUlid(), "k", tSession, `{}`)
	}
	if _, err := st.Append(context.Background(), events); err != nil {
		t.Fatalf("append: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if wm, _ := ix.Watermark(); wm >= 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Run did not drain to watermark 5 (wm=%d)", func() int64 { w, _ := ix.Watermark(); return w }())
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return after cancel")
	}
}

// ---------------------------------------------------------------------------
// P1-2：坏载荷隔离（不致命）
// ---------------------------------------------------------------------------

// TestBadPayloadIsolatedNotFatal：[好, 坏, 好] 的事件流必须走完：
// Step / Rebuild 都不失败、水位推进、坏载荷计一次 "indexer.bad_payload"。
func TestBadPayloadIsolatedNotFatal(t *testing.T) {
	events := []contracts.Event{
		ledgerEv(core.NewUlid(), tSession, []string{"g1"}, nil, nil, nil), // 好
		ev(core.NewUlid(), "ledger.updated", tSession, `{这不是 JSON`),       // 坏
		ledgerEv(core.NewUlid(), tSession, []string{"g2"}, nil, nil, nil), // 好
	}
	_, ix := setup(t, "memory", 100, events)
	ctx := context.Background()

	n, err := ix.Step(ctx)
	if err != nil {
		t.Fatalf("坏载荷不得让 Step 失败: %v", err)
	}
	if n != 3 {
		t.Fatalf("Step processed %d, want 3", n)
	}
	if wm, _ := ix.Watermark(); wm != 3 {
		t.Fatalf("watermark = %d, want 3（坏载荷也要推进水位）", wm)
	}

	stats := readCost(t, ix, tSession)
	if stats.BadPayloadCount != 1 {
		t.Fatalf("bad_payload 计数 = %d, want 1（独立列，不在 by_kind 里）", stats.BadPayloadCount)
	}
	// by_kind 的划分语义：每个事件恰好落一个 kind → 求和 == event_count。
	if _, polluted := stats.ByKind["indexer.bad_payload"]; polluted {
		t.Fatalf("by_kind 不得混入隔离标记：%v", stats.ByKind)
	}
	var sum int64
	for _, n := range stats.ByKind {
		sum += n
	}
	if sum != stats.EventCount {
		t.Fatalf("by_kind 求和 = %d, want event_count = %d（求和必须相等）", sum, stats.EventCount)
	}
	if stats.EventCount != 3 {
		t.Fatalf("event_count = %d, want 3（坏事件也计入总数）", stats.EventCount)
	}
	// 坏事件被跳过，其余投影照常（最后一条好快照生效）。
	led := readLedger(t, ix, tSession)
	if len(led.Goals) != 1 || led.Goals[0] != "g2" {
		t.Fatalf("ledger goals = %v, want [g2]（跳过坏事件，其余照常）", led.Goals)
	}

	// Rebuild 也必须走完（否则重建同死）。
	if err := ix.Rebuild(ctx); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	stats2 := readCost(t, ix, tSession)
	if stats2.BadPayloadCount != 1 {
		t.Fatalf("Rebuild 后 bad_payload 计数 = %d, want 1", stats2.BadPayloadCount)
	}
	if wm, _ := ix.Watermark(); wm != 3 {
		t.Fatalf("Rebuild 后水位 = %d, want 3", wm)
	}
}

// TestBadPayloadIsolatedForEveryKind：两种已知 kind 的坏载荷都被隔离（各自计一次）。
func TestBadPayloadIsolatedForEveryKind(t *testing.T) {
	events := []contracts.Event{
		ev(core.NewUlid(), "ledger.updated", tSession, `{坏`), // 坏（ledger）
		ev(core.NewUlid(), "board.written", tSession, `{也坏`), // 坏（board）
		ledgerEv(core.NewUlid(), tSession, []string{"g"}, nil, nil, nil),
	}
	_, ix := setup(t, "memory", 100, events)
	ctx := context.Background()
	if _, err := ix.Step(ctx); err != nil {
		t.Fatalf("Step: %v", err)
	}
	stats := readCost(t, ix, tSession)
	if stats.BadPayloadCount != 2 {
		t.Fatalf("bad_payload 计数 = %d, want 2（独立列）", stats.BadPayloadCount)
	}
	var sum int64
	for _, n := range stats.ByKind {
		sum += n
	}
	if sum != stats.EventCount {
		t.Fatalf("by_kind 求和 = %d, want event_count = %d", sum, stats.EventCount)
	}
}

// TestFoldLedgerSkipsBadPayload：FoldLedger 与索引器口径一致——跳坏取末。
func TestFoldLedgerSkipsBadPayload(t *testing.T) {
	events := []contracts.Event{
		ledgerEv(core.NewUlid(), tSession, []string{"g1"}, nil, nil, nil),
		ev(core.NewUlid(), "ledger.updated", tSession, `{坏载荷`),
		ledgerEv(core.NewUlid(), tSession, []string{"g2"}, nil, nil, nil),
	}
	led := runtime.FoldLedger(events)
	if len(led.Goals) != 1 || led.Goals[0] != "g2" {
		t.Fatalf("FoldLedger goals = %v, want [g2]（跳坏取末）", led.Goals)
	}
}

// ---------------------------------------------------------------------------
// P1-3：Step 串行化（并发计数必须精确）
// ---------------------------------------------------------------------------

// TestConcurrentStepCountsExactly：8 个 goroutine 并发 Step，计数必须精确
// （不加锁会重复应用同一批 → 计数翻倍）。
func TestConcurrentStepCountsExactly(t *testing.T) {
	st, ix := setup(t, "file", 50, nil)
	ctx := context.Background()

	const (
		goroutines = 8
		perG       = 50
	)
	total := goroutines * perG
	events := make([]contracts.Event, total)
	for i := range events {
		events[i] = ev(core.NewUlid(), "k", tSession, `{}`)
	}
	if _, err := st.Append(ctx, events); err != nil {
		t.Fatalf("append: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perG)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if _, err := ix.Step(ctx); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Step: %v", err)
	}

	// 计数精确：每条事件只计一次。
	if got := readCost(t, ix, tSession).EventCount; got != int64(total) {
		t.Fatalf("cost event_count = %d, want %d（并发重复应用会翻倍）", got, total)
	}
	if got := readSession(t, ix, tSession).EventCount; got != int64(total) {
		t.Fatalf("session event_count = %d, want %d", got, total)
	}
	if wm, _ := ix.Watermark(); wm != int64(total) {
		t.Fatalf("watermark = %d, want %d", wm, total)
	}
}
