package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// 固定但合法的框架 id（26 字符 Crockford base32），用于确定性测试。
var (
	testSession  = contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")
	testSession2 = contracts.Ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	testAgent    = contracts.Ulid("TEECY7V57SP0J9WZ620EHHP02M")
)

// mkEvent 造一个 durable 事实事件；id/kind/session 由调用方定，其余填合法默认值。
func mkEvent(id contracts.Ulid, kind string, session contracts.Ulid, payload string) contracts.Event {
	return contracts.Event{
		V:         1,
		ID:        id,
		Kind:      kind,
		TS:        "2025-03-01T10:00:00.000Z",
		SessionID: session,
		AgentID:   testAgent,
		Durable:   true,
		Payload:   json.RawMessage(payload),
	}
}

// openStore 按模式开库：file 用临时目录，memory 用 :memory:。
func openStore(t *testing.T, mode string) *Store {
	t.Helper()
	path := ":memory:"
	if mode == "file" {
		path = t.TempDir() + "/events.db"
	}
	st, err := Open(path, Options{BusyTimeoutMS: 5000})
	if err != nil {
		t.Fatalf("Open(%s): %v", mode, err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// TestAppendRoundTrip 覆盖 CRUD + 字段往返（含可空 causation_id / binary_ref + payload）。
func TestAppendRoundTrip(t *testing.T) {
	for _, mode := range []string{"file", "memory"} {
		t.Run(mode, func(t *testing.T) {
			st := openStore(t, mode)
			ctx := context.Background()

			cause := core.NewUlid()
			bref := core.NewUlid()
			ev := mkEvent(core.NewUlid(), "tool.call.intent", testSession, `{"tool_name":"read_file"}`)
			ev.CausationID = &cause
			ev.BinaryRef = &bref

			out, err := st.Append(ctx, []contracts.Event{ev})
			if err != nil {
				t.Fatalf("Append: %v", err)
			}
			if len(out) != 1 {
				t.Fatalf("Append returned %d events, want 1", len(out))
			}
			if out[0].Seq == nil {
				t.Fatalf("Seq is nil after Append")
			}
			if *out[0].Seq != 1 {
				t.Fatalf("Seq = %d, want 1", *out[0].Seq)
			}
			if out[0].ID != ev.ID {
				t.Fatalf("ID = %s, want %s", out[0].ID, ev.ID)
			}
			if out[0].CausationID == nil || *out[0].CausationID != cause {
				t.Fatalf("CausationID round-trip failed: %v", out[0].CausationID)
			}
			if out[0].BinaryRef == nil || *out[0].BinaryRef != bref {
				t.Fatalf("BinaryRef round-trip failed: %v", out[0].BinaryRef)
			}
			if string(out[0].Payload) != `{"tool_name":"read_file"}` {
				t.Fatalf("Payload = %s", out[0].Payload)
			}
			if !out[0].Durable {
				t.Fatalf("Durable lost")
			}

			// ReadFrom 读回应一致。
			got, err := st.ReadFrom(ctx, nil, 0, 0)
			if err != nil {
				t.Fatalf("ReadFrom: %v", err)
			}
			if len(got) != 1 || got[0].ID != ev.ID || *got[0].Seq != 1 {
				t.Fatalf("ReadFrom mismatch: %+v", got)
			}
		})
	}
}

// TestAppendEmptyPayload 覆盖 payload 缺省（空串 → nil，保留 omitempty 语义）。
func TestAppendEmptyPayload(t *testing.T) {
	st := openStore(t, "memory")
	ctx := context.Background()
	ev := mkEvent(core.NewUlid(), "context.assembled", testSession, "")
	out, err := st.Append(ctx, []contracts.Event{ev})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if out[0].Payload != nil {
		t.Fatalf("empty payload should round-trip to nil, got %s", out[0].Payload)
	}
}

// TestAppendIdempotentByID：同 ID 重复 Append 不产生重复行，返回库中原事件（G11/S7）。
func TestAppendIdempotentByID(t *testing.T) {
	for _, mode := range []string{"file", "memory"} {
		t.Run(mode, func(t *testing.T) {
			st := openStore(t, mode)
			ctx := context.Background()
			id := core.NewUlid()
			ev := mkEvent(id, "tool.call.intent", testSession, `{"n":1}`)

			first, err := st.Append(ctx, []contracts.Event{ev})
			if err != nil {
				t.Fatalf("first Append: %v", err)
			}
			// 重复追加同一事件（模拟崩溃重试）。
			second, err := st.Append(ctx, []contracts.Event{ev})
			if err != nil {
				t.Fatalf("second Append (idempotent): %v", err)
			}
			if *first[0].Seq != *second[0].Seq {
				t.Fatalf("idempotent Append changed seq: first=%d second=%d", *first[0].Seq, *second[0].Seq)
			}
			got, err := st.ReadFrom(ctx, nil, 0, 0)
			if err != nil {
				t.Fatalf("ReadFrom: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("duplicate Append produced %d rows, want 1 (G11)", len(got))
			}
		})
	}
}

// TestAppendRejectsNonDurable：durable=false 直接报错（F8 第二道防线）。
func TestAppendRejectsNonDurable(t *testing.T) {
	st := openStore(t, "memory")
	ctx := context.Background()
	stream := mkEvent(core.NewUlid(), "model.delta", testSession, `{"text":"hi"}`)
	stream.Durable = false
	if _, err := st.Append(ctx, []contracts.Event{stream}); err == nil {
		t.Fatalf("Append accepted a non-durable event (F8 must reject)")
	}
	// 库里不应有任何东西。
	got, _ := st.ReadFrom(ctx, nil, 0, 0)
	if len(got) != 0 {
		t.Fatalf("non-durable event leaked %d rows into the store", len(got))
	}
}

// TestAppendRejectsDuplicateInBatch：同一批内 id 重复 → 报错（输出语义歧义）。
func TestAppendRejectsDuplicateInBatch(t *testing.T) {
	st := openStore(t, "memory")
	ctx := context.Background()
	id := core.NewUlid()
	ev := mkEvent(id, "tool.call.intent", testSession, `{}`)
	if _, err := st.Append(ctx, []contracts.Event{ev, ev}); err == nil {
		t.Fatalf("Append accepted duplicate ids in one batch")
	}
}

// TestSeqMonotonic：多批 append，seq 严格递增、无重复（G9/G10）。
func TestSeqMonotonic(t *testing.T) {
	st := openStore(t, "file")
	ctx := context.Background()
	var last int64
	for i := 0; i < 50; i++ {
		batch := []contracts.Event{
			mkEvent(core.NewUlid(), "tool.call.intent", testSession, `{}`),
			mkEvent(core.NewUlid(), "tool.call.completed", testSession, `{}`),
		}
		out, err := st.Append(ctx, batch)
		if err != nil {
			t.Fatalf("Append batch %d: %v", i, err)
		}
		for _, ev := range out {
			if ev.Seq == nil || *ev.Seq <= last {
				t.Fatalf("seq not monotonic: got %v after %d", ev.Seq, last)
			}
			last = *ev.Seq
		}
	}
	if last != 100 {
		t.Fatalf("final seq = %d, want 100", last)
	}
}

// TestSeqHolesOnlyFromInsertConflicts：seq 的空洞**只来自插入冲突**
// （ON CONFLICT DO NOTHING，G11 幂等去重那条路径）；**事务回滚不吃号**
// （sqlite_sequence 的更新随事务一起回滚）——这是 store doc 里 seq 口径的实测依据。
func TestSeqHolesOnlyFromInsertConflicts(t *testing.T) {
	st := openStore(t, "file")
	ctx := context.Background()

	// 1) 回滚不吃号：手工插一行再回滚 → 下一个真实事件仍拿 seq=1。
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO events (id, v, ts, session_id, agent_id, kind, durable, payload)
		 VALUES ('rolled-back-id', 1, 'ts', ?, ?, 'k', 1, '')`,
		string(testSession), string(testAgent)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	evs, err := st.Append(ctx, []contracts.Event{mkEvent(core.NewUlid(), "tool.call.intent", testSession, `{}`)})
	if err != nil {
		t.Fatalf("append after rollback: %v", err)
	}
	if *evs[0].Seq != 1 {
		t.Fatalf("事件拿到 seq=%d, want 1（事务回滚不吃号）", *evs[0].Seq)
	}

	// 2) 插入冲突吃号：幂等重追加同一 ID → 冲突吃掉一个号（下一个事件 seq 跳一格）。
	id := core.NewUlid()
	first, err := st.Append(ctx, []contracts.Event{mkEvent(id, "tool.call.intent", testSession, `{}`)})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := st.Append(ctx, []contracts.Event{mkEvent(id, "tool.call.intent", testSession, `{}`)}); err != nil {
		t.Fatalf("idempotent append: %v", err)
	}
	third, err := st.Append(ctx, []contracts.Event{mkEvent(core.NewUlid(), "tool.call.intent", testSession, `{}`)})
	if err != nil {
		t.Fatalf("append after conflict: %v", err)
	}
	// seq=2 给 first，冲突吃掉 seq=3 → 下一个是 4。
	if want := *first[0].Seq + 2; *third[0].Seq != want {
		t.Fatalf("冲突后的事件 seq=%d, want %d（插入冲突吃掉一个号）", *third[0].Seq, want)
	}
}

// TestReadFromFilterAndLimit：session 过滤 + limit + fromSeq 水位。
func TestReadFromFilterAndLimit(t *testing.T) {
	st := openStore(t, "file")
	ctx := context.Background()
	// session A 3 条，session B 2 条，交错。
	seq := int64(0)
	appendOne := func(kind string, session contracts.Ulid) int64 {
		out, err := st.Append(ctx, []contracts.Event{mkEvent(core.NewUlid(), kind, session, `{}`)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		seq = *out[0].Seq
		return seq
	}
	appendOne("k", testSession)  // seq 1 (A)
	appendOne("k", testSession2) // seq 2 (B)
	appendOne("k", testSession)  // seq 3 (A)
	appendOne("k", testSession2) // seq 4 (B)
	appendOne("k", testSession)  // seq 5 (A)

	// 全部，从 seq 3 起。
	from3, err := st.ReadFrom(ctx, nil, 3, 0)
	if err != nil {
		t.Fatalf("ReadFrom from 3: %v", err)
	}
	if len(from3) != 3 || *from3[0].Seq != 3 || *from3[2].Seq != 5 {
		t.Fatalf("ReadFrom(from=3) = %v, want seqs 3,4,5", seqs(from3))
	}

	// 只看 session A。
	onlyA, err := st.ReadFrom(ctx, &testSession, 0, 0)
	if err != nil {
		t.Fatalf("ReadFrom A: %v", err)
	}
	if len(onlyA) != 3 {
		t.Fatalf("session A has %d events, want 3", len(onlyA))
	}
	for _, ev := range onlyA {
		if ev.SessionID != testSession {
			t.Fatalf("filter leaked session %s", ev.SessionID)
		}
	}

	// limit。
	limited, err := st.ReadFrom(ctx, &testSession, 0, 2)
	if err != nil {
		t.Fatalf("ReadFrom limit: %v", err)
	}
	if len(limited) != 2 || *limited[0].Seq != 1 || *limited[1].Seq != 3 {
		t.Fatalf("limit read = %v, want seqs 1,3", seqs(limited))
	}
}

// ---------------------------------------------------------------------------
// G5：ReadSlice（seq 闭区间分段读）——与 ReadFrom 全量等价 + 边界。
// ---------------------------------------------------------------------------

// TestReadSliceMatchesReadFrom：分段拼接（步长 3）与 ReadFrom 全量结果逐事件一致
// （两 session 交错 + 幂等重追加制造 seq 空洞——空洞必须被区间扫描跳过，不产出假事件）。
func TestReadSliceMatchesReadFrom(t *testing.T) {
	st := openStore(t, "file")
	ctx := context.Background()

	// 交错 append，并中途重追加制造 seq 空洞（冲突吃号，不产生新行）。
	for i := 0; i < 8; i++ {
		sess := testSession
		if i%2 == 1 {
			sess = testSession2
		}
		if _, err := st.Append(ctx, []contracts.Event{mkEvent(core.NewUlid(), "k", sess, fmt.Sprintf(`{"i":%d}`, i))}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if i == 2 {
			// 追加 + 立即幂等重追加同一 ID → 吃掉一个 seq 号（空洞）。
			e := mkEvent(core.NewUlid(), "k", sess, `{"hole":true}`)
			if _, err := st.Append(ctx, []contracts.Event{e}); err != nil {
				t.Fatalf("append hole: %v", err)
			}
			if _, err := st.Append(ctx, []contracts.Event{e}); err != nil {
				t.Fatalf("re-append hole: %v", err)
			}
		}
	}
	full, err := st.ReadFrom(ctx, nil, 0, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(full) != 9 {
		t.Fatalf("全量应 9 条（重追加不产新行），得到 %d", len(full))
	}

	// 分段拼接：闭区间步进 3（1..3, 4..6, ...），与全量逐事件比对。
	var got []contracts.Event
	for start := int64(1); start <= 16; start += 3 {
		slice, err := st.ReadSlice(ctx, nil, start, start+2, 0)
		if err != nil {
			t.Fatalf("ReadSlice [%d,%d]: %v", start, start+2, err)
		}
		got = append(got, slice...)
	}
	if len(got) != len(full) {
		t.Fatalf("分段拼接 %d 条 != ReadFrom 全量 %d 条", len(got), len(full))
	}
	for i := range got {
		if got[i].ID != full[i].ID || got[i].Seq == nil || *got[i].Seq != *full[i].Seq ||
			got[i].Kind != full[i].Kind || string(got[i].Payload) != string(full[i].Payload) {
			t.Fatalf("第 %d 条不一致：slice=%+v full=%+v", i, got[i], full[i])
		}
	}
	// tip 之后的区间 → 空。
	after, err := st.ReadSlice(ctx, nil, 100, 200, 0)
	if err != nil {
		t.Fatalf("ReadSlice beyond tip: %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("tip 之后的区间应为空，得到 %d 条", len(after))
	}
}

// TestReadSliceBoundaries：空区间 / 超界 limit / 缺 seq / session 过滤 / 单点区间。
func TestReadSliceBoundaries(t *testing.T) {
	st := openStore(t, "file")
	ctx := context.Background()

	// session A seq 1、B seq 2、A seq 3、B seq 4、A seq 5（交错）+ 空洞。
	appendOne := func(session contracts.Ulid) int64 {
		out, err := st.Append(ctx, []contracts.Event{mkEvent(core.NewUlid(), "k", session, `{}`)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		return *out[0].Seq
	}
	appendOne(testSession)  // 1 A
	appendOne(testSession2) // 2 B
	a3 := appendOne(testSession)
	appendOne(testSession2) // 4 B
	appendOne(testSession)  // 5 A
	appendOne(testSession)  // 6 A
	// 空洞：e 落库拿 seq 7，幂等重追加吃掉 seq 8（无行）。
	e := mkEvent(core.NewUlid(), "k", testSession, `{}`)
	if _, err := st.Append(ctx, []contracts.Event{e}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := st.Append(ctx, []contracts.Event{e}); err != nil { // 吃掉 seq 8
		t.Fatalf("re-append: %v", err)
	}
	appendOne(testSession) // seq 9 A

	// ① 空区间（倒挂）→ 空切片、不报错。
	empty, err := st.ReadSlice(ctx, nil, 5, 1, 0)
	if err != nil {
		t.Fatalf("倒挂区间不应报错: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("倒挂区间应为空，得到 %d 条", len(empty))
	}

	// ② 单点区间：只回那一条。
	one, err := st.ReadSlice(ctx, nil, a3, a3, 0)
	if err != nil {
		t.Fatalf("单点区间: %v", err)
	}
	if len(one) != 1 || *one[0].Seq != a3 {
		t.Fatalf("单点区间 [%d] 应恰 1 条，得到 %v", a3, seqs(one))
	}

	// ③ 超界 limit：limit > 区间内行数 → 全量返回，不报错。
	big, err := st.ReadSlice(ctx, nil, 1, 100, 1000)
	if err != nil {
		t.Fatalf("超界 limit: %v", err)
	}
	all, _ := st.ReadFrom(ctx, nil, 0, 0)
	if len(big) != len(all) {
		t.Fatalf("超界 limit 应回全量 %d 条，得到 %d 条", len(all), len(big))
	}

	// ④ 小 limit：区间内前 N 条。
	lim, err := st.ReadSlice(ctx, nil, 1, 100, 2)
	if err != nil {
		t.Fatalf("limit=2: %v", err)
	}
	if len(lim) != 2 || *lim[0].Seq != 1 || *lim[1].Seq != 2 {
		t.Fatalf("limit=2 应得 seqs 1,2，得到 %v", seqs(lim))
	}

	// ⑤ 缺 seq：区间横跨空洞（8 无行）→ 只回有行的 seq。
	holes, err := st.ReadSlice(ctx, nil, 1, 100, 0)
	if err != nil {
		t.Fatalf("跨空洞: %v", err)
	}
	want := []int64{1, 2, 3, 4, 5, 6, 7, 9}
	gotSeqs := seqs(holes)
	if len(gotSeqs) != len(want) {
		t.Fatalf("跨空洞应得 %v，得到 %v", want, gotSeqs)
	}
	for i := range want {
		if gotSeqs[i] != want[i] {
			t.Fatalf("跨空洞应得 %v，得到 %v", want, gotSeqs)
		}
	}

	// ⑥ session 过滤 + 区间：只回 A 的、且在区间内。
	onlyA, err := st.ReadSlice(ctx, &testSession, 3, 100, 0)
	if err != nil {
		t.Fatalf("session 过滤: %v", err)
	}
	for _, ev := range onlyA {
		if ev.SessionID != testSession {
			t.Fatalf("session 过滤泄漏: %s", ev.SessionID)
		}
		if *ev.Seq < 3 {
			t.Fatalf("区间下界泄漏: seq=%d", *ev.Seq)
		}
	}
	if len(onlyA) != 5 { // A 的 seq 3,5,6,7,9
		t.Fatalf("A 在 [3,∞) 应 5 条，得到 %d", len(onlyA))
	}
}

// TestCloseIdempotent：Close 可重复调用。
func TestCloseIdempotent(t *testing.T) {
	for _, mode := range []string{"file", "memory"} {
		t.Run(mode, func(t *testing.T) {
			path := ":memory:"
			if mode == "file" {
				path = t.TempDir() + "/close.db"
			}
			st, err := Open(path, Options{})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := st.Close(); err != nil {
				t.Fatalf("first Close: %v", err)
			}
			if err := st.Close(); err != nil {
				t.Fatalf("second Close should be nil, got %v", err)
			}
		})
	}
}

// TestReopenPersistence：关闭再打开，数据保留、seq 继续（G9 AUTOINCREMENT 持久）。
func TestReopenPersistence(t *testing.T) {
	path := t.TempDir() + "/reopen.db"
	ctx := context.Background()

	st, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	out, err := st.Append(ctx, []contracts.Event{
		mkEvent(core.NewUlid(), "tool.call.intent", testSession, `{"i":0}`),
		mkEvent(core.NewUlid(), "tool.call.intent", testSession, `{"i":1}`),
	})
	if err != nil {
		t.Fatalf("Append #1: %v", err)
	}
	if *out[1].Seq != 2 {
		t.Fatalf("second seq = %d, want 2", *out[1].Seq)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 重开。
	st2, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	defer st2.Close()

	prev, err := st2.ReadFrom(ctx, nil, 0, 0)
	if err != nil {
		t.Fatalf("ReadFrom after reopen: %v", err)
	}
	if len(prev) != 2 {
		t.Fatalf("after reopen got %d events, want 2", len(prev))
	}
	out2, err := st2.Append(ctx, []contracts.Event{mkEvent(core.NewUlid(), "tool.call.intent", testSession, `{"i":2}`)})
	if err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}
	if *out2[0].Seq != 3 {
		t.Fatalf("seq after reopen = %d, want 3 (AUTOINCREMENT persisted)", *out2[0].Seq)
	}
}

// TestConcurrentAppend：100 goroutine × 20 事件 = 2000 append，零失败、seq 唯一（G9/G13）。
// 文件模式（真实 WAL + txlock + busy_timeout）；-race 下也必须过。
func TestConcurrentAppend(t *testing.T) {
	st := openStore(t, "file")
	ctx := context.Background()

	const workers = 100
	const perWorker = 20

	var (
		wg      sync.WaitGroup
		errN    int64
		mu      sync.Mutex
		errMsgs []string
		allSeq  = make(map[int64]bool, workers*perWorker)
		seqMu   sync.Mutex
	)

	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start // 统一发令，最大化竞争
			for i := 0; i < perWorker; i++ {
				ev := mkEvent(core.NewUlid(), "tool.call.intent", testSession, fmt.Sprintf(`{"w":%d,"i":%d}`, w, i))
				out, err := st.Append(ctx, []contracts.Event{ev})
				if err != nil {
					mu.Lock()
					errN++
					if len(errMsgs) < 5 {
						errMsgs = append(errMsgs, err.Error())
					}
					mu.Unlock()
					continue
				}
				seqMu.Lock()
				if allSeq[*out[0].Seq] {
					mu.Lock()
					errMsgs = append(errMsgs, fmt.Sprintf("duplicate seq %d", *out[0].Seq))
					mu.Unlock()
				}
				allSeq[*out[0].Seq] = true
				seqMu.Unlock()
			}
		}(w)
	}
	close(start)
	wg.Wait()

	if errN != 0 {
		t.Fatalf("concurrent append failed %d times: %v", errN, errMsgs)
	}
	if len(errMsgs) != 0 {
		t.Fatalf("concurrent append produced duplicate seqs: %v", errMsgs)
	}
	total := workers * perWorker
	if len(allSeq) != total {
		t.Fatalf("got %d unique seqs, want %d", len(allSeq), total)
	}
	// 落库行数核对。
	got, err := st.ReadFrom(ctx, nil, 0, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(got) != total {
		t.Fatalf("store has %d rows, want %d", len(got), total)
	}
}

// seqs 抽取 seq 列表，用于报错信息。
func seqs(evs []contracts.Event) []int64 {
	out := make([]int64, len(evs))
	for i, e := range evs {
		if e.Seq != nil {
			out[i] = *e.Seq
		}
	}
	return out
}

// TestConcurrentIdempotentAppend：多 goroutine 并发 append **同一批** id 完全重叠的事件，
// 每个 id 只能落一行（G11 幂等 + id UNIQUE，并发下不重行、不错行）。
func TestConcurrentIdempotentAppend(t *testing.T) {
	st := openStore(t, "file")
	ctx := context.Background()

	const n = 50
	base := make([]contracts.Event, n)
	for i := range base {
		base[i] = mkEvent(core.NewUlid(), "tool.call.intent", testSession, `{}`)
	}

	const workers = 8
	var wg sync.WaitGroup
	var errN int64
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := st.Append(ctx, base); err != nil { // 同一批，id 完全重叠
				atomic.AddInt64(&errN, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if errN != 0 {
		t.Fatalf("concurrent idempotent append failed %d times", errN)
	}
	got, err := st.ReadFrom(ctx, nil, 0, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(got) != n {
		t.Fatalf("got %d rows, want %d (idempotent under concurrency: no duplicate rows)", len(got), n)
	}
	// seq 唯一且为 1..n。
	seen := make(map[int64]bool, n)
	for _, e := range got {
		if e.Seq == nil || seen[*e.Seq] {
			t.Fatalf("duplicate/nil seq under concurrency: %v", e.Seq)
		}
		seen[*e.Seq] = true
	}
}

// TestConcurrentReadDuringWrite：写 burst 期间并发读不报错（G14：WAL 读写不互斥）。
func TestConcurrentReadDuringWrite(t *testing.T) {
	st := openStore(t, "file")
	ctx := context.Background()

	var readers, writers sync.WaitGroup
	var readErrN, writeErrN int64
	stop := make(chan struct{})

	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if _, err := st.ReadFrom(ctx, nil, 0, 0); err != nil {
						atomic.AddInt64(&readErrN, 1)
					}
				}
			}
		}()
	}
	for w := 0; w < 4; w++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := 0; i < 50; i++ {
				if _, err := st.Append(ctx, []contracts.Event{mkEvent(core.NewUlid(), "k", testSession, `{}`)}); err != nil {
					atomic.AddInt64(&writeErrN, 1)
				}
			}
		}()
	}
	writers.Wait()
	close(stop)
	readers.Wait()

	if readErrN != 0 {
		t.Fatalf("reads failed %d times during writes (WAL must allow concurrent reads)", readErrN)
	}
	if writeErrN != 0 {
		t.Fatalf("writes failed %d times", writeErrN)
	}
	got, err := st.ReadFrom(ctx, nil, 0, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(got) != 200 {
		t.Fatalf("store has %d rows, want 200", len(got))
	}
}
