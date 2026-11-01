package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	coregraph "github.com/Yorha9e/alasol/core/graph"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/runtime/graph"
)

// 文档族（G2 第 5 族）的 SQLite 实现测试。
//
// # 为什么用 file 模式而不是 :memory:（spike 实测，不是偏好）
//
// ncruces 的 WASM 驱动里 `mode=memory&cache=shared` **不跨连接共享**：每条连接是
// 独立 WASM 实例、各自一座空库（实测：keepalive + 一条已建 schema 的连接都开着时，
// 新开第三条连接仍报 "no such table"）。
//
// **版本检查已经改走调用方事务**（F1/F2：同一连接、同一快照 → 看得见本事务未提交的写）：
// 所以"检查"这一步不再需要第二条连接——**带版本的写在内存库上也是可用的**（写与检查
// 在同一个 Tx 里）。
//
// 但**跨连接读**仍是驱动属性（不是本实现能修的）：EnsureSchema 的 DDL 与读面
// （Get/List）走自持句柄（另一条连接），:memory: 下那条连接看到的是**另一座空库**
// → "no such table"。而且 "Upsert 后未 commit 前 Get 读不到、Commit 后读到" 的 ACID
// 断言本身也要求**跨连接**读到已提交快照，只有 file+WAL 给得了（实测：写事务期间池读
// 见旧快照、提交后见新行）。
//
// 语义上 file 模式还更贴生产（宿主用文件库；:memory: 只是 store 的测试便利）。

// ---------------------------------------------------------------------------
// 工具
// ---------------------------------------------------------------------------

// newDocStore 开 store、建 DocStore、EnsureSchema（表就绪才进用例）。
func newDocStore(t *testing.T, mode string) (*Store, *DocStore) {
	t.Helper()
	st := openStore(t, mode)
	ds, err := NewDocStore(st)
	if err != nil {
		t.Fatalf("NewDocStore: %v", err)
	}
	if err := ds.EnsureSchema(context.Background()); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	return st, ds
}

// runTx 在一个真实写事务里执行 fn：成功提交、fn 出错回滚并把错误交还
// （冲突路径要拿 errors.Is 判定，所以不 Fatal）。
func runTx(t *testing.T, st *Store, fn func(tx coregraph.Tx) error) error {
	t.Helper()
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := fn(graph.SQLTx(tx)); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return nil
}

// mustTx 同 runTx，但 fn 必须成功。
func mustTx(t *testing.T, st *Store, fn func(tx coregraph.Tx) error) {
	t.Helper()
	if err := runTx(t, st, fn); err != nil {
		t.Fatalf("tx: %v", err)
	}
}

// execOnlyTx 是"只有 Exec、读不回自己"的 Tx（形状照 graph.SQLExec 适配出来的纯执行面）。
//
// 用途：钉死 Upsert 的 **fail-close** 口径——库内版本检查需要 QueryContext 能力，
// 这种 Tx 缺能力时必须**显式报错**，而不是静默退回"看不见未提交写"的旧读法。
type execOnlyTx struct{}

func (execOnlyTx) Exec(context.Context, string, ...any) error { return nil }

// docLine 把一条文档压成确定性字符串（酸测试对答案用）。
func docLine(d storage.Doc) string {
	return fmt.Sprintf("%s/%s rev=%d seq=%d at=%s body=%s",
		d.Collection, d.Key, d.Revision, d.UpdatedSeq, d.UpdatedAt, d.Body)
}

func docLines(docs []storage.Doc) string {
	parts := make([]string, len(docs))
	for i, d := range docs {
		parts[i] = docLine(d)
	}
	return "[" + strings.Join(parts, " | ") + "]"
}

// ---------------------------------------------------------------------------
// 构造 / 往返 / 缺失
// ---------------------------------------------------------------------------

// TestNewDocStoreNilStore：nil Store 报错（与 NewGraphProjection 口径一致）。
func TestNewDocStoreNilStore(t *testing.T) {
	if _, err := NewDocStore(nil); err == nil {
		t.Fatal("NewDocStore(nil) 应报错")
	}
}

// TestDocStoreRoundTrip：Upsert→Get 字段逐字往返（Body/Revision/UpdatedSeq/UpdatedAt
// 原样——不自动分配、不自动盖时间戳）；Get 缺失 → ErrNotFound。
func TestDocStoreRoundTrip(t *testing.T) {
	st, ds := newDocStore(t, "file")
	ctx := context.Background()

	in := storage.Doc{
		Collection: "project_rules",
		Key:        "f7",
		Body:       json.RawMessage(`{"rule":"只增不删","scope":"contracts"}`),
		Revision:   7,
		UpdatedSeq: 42,
		UpdatedAt:  "2026-01-29T12:00:00Z",
	}
	mustTx(t, st, func(tx coregraph.Tx) error { return ds.Upsert(ctx, tx, []storage.Doc{in}) })

	got, err := ds.Get(ctx, "project_rules", "f7")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("往返不一致:\n got=%+v\nwant=%+v", got, in)
	}

	// nil Body 原样（NULL 读回 nil）；UpdatedAt 空串原样。
	empty := storage.Doc{Collection: "know", Key: "nb", Revision: 1}
	mustTx(t, st, func(tx coregraph.Tx) error { return ds.Upsert(ctx, tx, []storage.Doc{empty}) })
	got, err = ds.Get(ctx, "know", "nb")
	if err != nil {
		t.Fatalf("Get(nil body): %v", err)
	}
	if !reflect.DeepEqual(got, empty) {
		t.Fatalf("nil body 往返不一致: got=%+v want=%+v", got, empty)
	}

	// 缺失 → ErrNotFound（errors.Is），且错误形态带 doc collection/key。
	_, err = ds.Get(ctx, "project_rules", "nope")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("缺失应 ErrNotFound，得到 %v", err)
	}
	if !strings.Contains(err.Error(), "doc project_rules/nope") {
		t.Fatalf("错误形态应含 doc collection/key，得到 %v", err)
	}
	// 同键不同集合互不串（主键是二元组）。
	if _, err := ds.Get(ctx, "other", "f7"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("不同集合的同名键应不存在，得到 %v", err)
	}
}

// TestDocStoreEnsureSchemaIdempotent：连跑两次（三次）不炸，且建完能读写；
// **prefix-only 列举要用的单列索引**（idx_docs_key）也建出来了（幂等，老库自动补）。
func TestDocStoreEnsureSchemaIdempotent(t *testing.T) {
	st, ds := newDocStore(t, "file")
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := ds.EnsureSchema(ctx); err != nil {
			t.Fatalf("EnsureSchema #%d: %v", i+1, err)
		}
	}
	mustTx(t, st, func(tx coregraph.Tx) error {
		return ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: "k", Revision: 1}})
	})
	if _, err := ds.Get(ctx, "c", "k"); err != nil {
		t.Fatalf("幂等建表后读写应正常: %v", err)
	}
	// 索引在不在（prefix-only 列举走它；缺了 = 全表扫 + 排序）。
	var idx int
	err := st.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_docs_key'`).Scan(&idx)
	if err != nil {
		t.Fatalf("查索引: %v", err)
	}
	if idx != 1 {
		t.Fatalf("EnsureSchema 应建出单列 key 索引 idx_docs_key，得到 %d 个", idx)
	}
}

// ---------------------------------------------------------------------------
// List：过滤 / 字节前缀 / 排序 / Limit
// ---------------------------------------------------------------------------

// TestDocStoreList：集合过滤、字节前缀大小写敏感、排序确定、Limit 截断/不截断/负值。
func TestDocStoreList(t *testing.T) {
	st, ds := newDocStore(t, "file")
	ctx := context.Background()

	// 乱序插入（插入序 ≠ 输出序，钉死 ORDER BY 语义）。
	mustTx(t, st, func(tx coregraph.Tx) error {
		return ds.Upsert(ctx, tx, []storage.Doc{
			{Collection: "rules", Key: "m", Revision: 1},
			{Collection: "aaa", Key: "b", Revision: 1},
			{Collection: "rules", Key: "z", Revision: 1},
			{Collection: "rules", Key: "a", Revision: 1},
			{Collection: "rules", Key: "Ab-1", Revision: 1},
			{Collection: "rules", Key: "ab-1", Revision: 1},
		})
	})

	keys := func(docs []storage.Doc) []string {
		out := make([]string, len(docs))
		for i, d := range docs {
			out[i] = d.Key
		}
		return out
	}

	// 排序确定：按 (collection, key) 字节序。
	all, err := ds.List(ctx, storage.DocQuery{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := []string{"b", "Ab-1", "a", "ab-1", "m", "z"}
	if got := keys(all); !reflect.DeepEqual(got, want) {
		t.Fatalf("排序/全量不对:\n got=%v\nwant=%v", got, want)
	}

	// 集合过滤。
	rules, err := ds.List(ctx, storage.DocQuery{Collection: "rules"})
	if err != nil {
		t.Fatalf("List(rules): %v", err)
	}
	wantRules := []string{"Ab-1", "a", "ab-1", "m", "z"}
	if got := keys(rules); !reflect.DeepEqual(got, wantRules) {
		t.Fatalf("集合过滤不对:\n got=%v\nwant=%v", got, wantRules)
	}

	// 字节前缀、大小写敏感（陷阱 17：LIKE 会把 "Ab" 串到 "ab-1"——这里绝不许）。
	upper, err := ds.List(ctx, storage.DocQuery{Prefix: "Ab"})
	if err != nil {
		t.Fatalf("List(prefix=Ab): %v", err)
	}
	if got := keys(upper); !reflect.DeepEqual(got, []string{"Ab-1"}) {
		t.Fatalf("前缀 Ab 应只命中 Ab-1，得到 %v", got)
	}
	lower, err := ds.List(ctx, storage.DocQuery{Prefix: "ab"})
	if err != nil {
		t.Fatalf("List(prefix=ab): %v", err)
	}
	if got := keys(lower); !reflect.DeepEqual(got, []string{"ab-1"}) {
		t.Fatalf("前缀 ab 应只命中 ab-1，得到 %v", got)
	}
	// 集合 + 前缀叠加。
	both, err := ds.List(ctx, storage.DocQuery{Collection: "rules", Prefix: "a"})
	if err != nil {
		t.Fatalf("List(rules/a): %v", err)
	}
	if got := keys(both); !reflect.DeepEqual(got, []string{"a", "ab-1"}) {
		t.Fatalf("集合+前缀叠加不对: %v", got)
	}

	// Limit 截断（取排序后的前 N，不是插入序）。
	lim, err := ds.List(ctx, storage.DocQuery{Limit: 2})
	if err != nil {
		t.Fatalf("List(limit=2): %v", err)
	}
	if got := keys(lim); !reflect.DeepEqual(got, []string{"b", "Ab-1"}) {
		t.Fatalf("Limit=2 截断不对: %v", got)
	}
	// Limit 超量 → 全量（不报错）。
	big, err := ds.List(ctx, storage.DocQuery{Limit: 100})
	if err != nil {
		t.Fatalf("List(limit=100): %v", err)
	}
	if len(big) != len(all) {
		t.Fatalf("超量 Limit 应全量: got %d want %d", len(big), len(all))
	}
	// Limit=0 → 不截断（契约"0 = 实现默认"，本实现取全返回）。
	full, err := ds.List(ctx, storage.DocQuery{Limit: 0})
	if err != nil {
		t.Fatalf("List(limit=0): %v", err)
	}
	if len(full) != len(all) {
		t.Fatalf("Limit=0 不该截断: got %d want %d", len(full), len(all))
	}
	// 负 Limit → ErrBadQuery（契约明文；替身没查，实现要守）。
	if _, err := ds.List(ctx, storage.DocQuery{Limit: -1}); !errors.Is(err, storage.ErrBadQuery) {
		t.Fatalf("负 Limit 应 ErrBadQuery，得到 %v", err)
	}
}

// TestDocStorePrefixByteExact：前缀的字节级精确性——含 0xFF 上界与全 0xFF 无上界分支。
func TestDocStorePrefixByteExact(t *testing.T) {
	st, ds := newDocStore(t, "file")
	ctx := context.Background()

	mustTx(t, st, func(tx coregraph.Tx) error {
		return ds.Upsert(ctx, tx, []storage.Doc{
			{Collection: "c", Key: "k\xffz", Revision: 1},
			{Collection: "c", Key: "k\xfez", Revision: 1},
			{Collection: "c", Key: "l", Revision: 1},
			{Collection: "c", Key: "\xff\x01", Revision: 1},
			{Collection: "c", Key: "\x7f", Revision: 1},
		})
	})
	keys := func(docs []storage.Doc) []string {
		out := make([]string, len(docs))
		for i, d := range docs {
			out[i] = d.Key
		}
		return out
	}

	// "k\xff" → 上界 "l"：圈住 k\xffz、排除 k\xfez 与 l。
	got, err := ds.List(ctx, storage.DocQuery{Prefix: "k\xff"})
	if err != nil {
		t.Fatalf("List(prefix=k\\xff): %v", err)
	}
	if ks := keys(got); !reflect.DeepEqual(ks, []string{"k\xffz"}) {
		t.Fatalf("前缀 k\\xff 应只命中 k\\xffz，得到 %q", ks)
	}
	// 全 0xFF 前缀 → 无上界，只有下界：\xff\x01 命中、\x7f 排除。
	got, err = ds.List(ctx, storage.DocQuery{Prefix: "\xff"})
	if err != nil {
		t.Fatalf("List(prefix=\\xff): %v", err)
	}
	if ks := keys(got); !reflect.DeepEqual(ks, []string{"\xff\x01"}) {
		t.Fatalf("前缀 \\xff 应只命中 \\xff\\x01，得到 %q", ks)
	}
}

// TestPrefixUpperBound：纯函数钉死上界算法（0xFF 进位/截断/全 0xFF）。
func TestPrefixUpperBound(t *testing.T) {
	for _, tc := range []struct {
		prefix string
		upper  string
		ok     bool
	}{
		{"Ab", "Ac", true},
		{"k\xff", "l", true},
		{"a\xff\xff", "b", true},
		{"ab\xff", "ac", true},
		{"\xff\xff", "", false},
		{"\x00", "\x01", true},
	} {
		upper, ok := prefixUpperBound(tc.prefix)
		if upper != tc.upper || ok != tc.ok {
			t.Fatalf("prefixUpperBound(%q) = (%q, %v), want (%q, %v)",
				tc.prefix, upper, ok, tc.upper, tc.ok)
		}
		// 不变量（ok 时）：圈内恰是 HasPrefix 的全体——抽查上界本身不带前缀。
		if ok && strings.HasPrefix(tc.upper, tc.prefix) {
			t.Fatalf("上界 %q 不该还带前缀 %q（会把上界自己圈进来）", tc.upper, tc.prefix)
		}
	}
}

// ---------------------------------------------------------------------------
// 冲突语义（整批拒绝；与 mapDocs/logDocs 逐条同口径）
// ---------------------------------------------------------------------------

// TestDocStoreConflicts 钉死写面冲突口径：
//   - 批内同键不同版本 → 整批拒（零写入）；
//   - 与库内版本不符 → 整批拒（**冲突批次零写入**：新键不出现、已有键版本不变）；
//   - 不对称口径：[rev0, rev2] 冲突、[rev2, rev0] 不冲突（照 mapDocs，不许对称化）；
//   - rev0 = LWW 免检（总是允许覆盖）；
//   - Delete 后再写同键不冲突（删除 = 没有现存版本）。
func TestDocStoreConflicts(t *testing.T) {
	st, ds := newDocStore(t, "file")
	ctx := context.Background()
	up := func(docs ...storage.Doc) error {
		return runTx(t, st, func(tx coregraph.Tx) error { return ds.Upsert(ctx, tx, docs) })
	}
	getRev := func(key string) (storage.Doc, error) { return ds.Get(ctx, "c", key) }

	// 种子：c/ok rev1。
	if err := up(storage.Doc{Collection: "c", Key: "ok", Revision: 1}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// ① 批内冲突：同键 [rev1, rev2] → ErrConflict，dup 不落库。
	err := up(
		storage.Doc{Collection: "c", Key: "dup", Revision: 1},
		storage.Doc{Collection: "c", Key: "dup", Revision: 2},
	)
	if !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("批内冲突应 ErrConflict，得到 %v", err)
	}
	if _, err := getRev("dup"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("冲突批次零写入（dup 不该出现），得到 %v", err)
	}

	// ② 与库内版本不符 → 整批拒；**零写入**（新键 fresh 不出现、ok 版本不变）。
	err = up(
		storage.Doc{Collection: "c", Key: "fresh", Revision: 1},
		storage.Doc{Collection: "c", Key: "ok", Revision: 7},
	)
	if !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("库版本冲突应 ErrConflict，得到 %v", err)
	}
	if _, err := getRev("fresh"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("冲突批次零写入（fresh 不该出现），得到 %v", err)
	}
	okDoc, err := getRev("ok")
	if err != nil {
		t.Fatalf("Get(ok): %v", err)
	}
	if okDoc.Revision != 1 {
		t.Fatalf("冲突批次不该改已有键（版本仍 1），得到 %d", okDoc.Revision)
	}

	// ③ 不对称口径（mapDocs 逐字行为）：
	//    [rev0, rev2] → 冲突（rev2>0 且与 pending 里的 rev0 不同）；
	//    [rev2, rev0] → 不冲突（rev0 那条 doc.Revision>0 不成立，pending 被 rev0 覆盖 → LWW 免检）。
	err = up(
		storage.Doc{Collection: "c", Key: "asym", Revision: 0},
		storage.Doc{Collection: "c", Key: "asym", Revision: 2},
	)
	if !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("[rev0, rev2] 应冲突（照 mapDocs，别对称化），得到 %v", err)
	}
	if _, err := getRev("asym"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("[rev0,rev2] 冲突批次零写入，得到 %v", err)
	}
	if err := up(
		storage.Doc{Collection: "c", Key: "asym", Revision: 2},
		storage.Doc{Collection: "c", Key: "asym", Revision: 0, Body: json.RawMessage(`{"v":0}`)},
	); err != nil {
		t.Fatalf("[rev2, rev0] 不该冲突（照 mapDocs），得到 %v", err)
	}
	asym, err := getRev("asym")
	if err != nil {
		t.Fatalf("Get(asym): %v", err)
	}
	if asym.Revision != 0 || string(asym.Body) != `{"v":0}` {
		t.Fatalf("[rev2,rev0] 写序后者覆盖前者，得到 rev=%d body=%s", asym.Revision, asym.Body)
	}

	// ④ rev0 = LWW 免检：库里 rev1，写 rev0 允许且覆盖。
	if err := up(storage.Doc{Collection: "c", Key: "lww", Revision: 1}); err != nil {
		t.Fatalf("seed lww: %v", err)
	}
	if err := up(storage.Doc{Collection: "c", Key: "lww", Revision: 0, Body: json.RawMessage(`{"lww":1}`)}); err != nil {
		t.Fatalf("rev0 LWW 覆盖应允许，得到 %v", err)
	}
	lww, err := getRev("lww")
	if err != nil {
		t.Fatalf("Get(lww): %v", err)
	}
	if lww.Revision != 0 || string(lww.Body) != `{"lww":1}` {
		t.Fatalf("rev0 覆盖没生效: %+v", lww)
	}

	// 同版本（rev 相同）覆盖 = LWW 允许。
	if err := up(storage.Doc{Collection: "c", Key: "lww", Revision: 0, Body: json.RawMessage(`{"lww":2}`)}); err != nil {
		t.Fatalf("同版本覆盖应允许，得到 %v", err)
	}

	// ⑤ Delete 后再写同键：没有现存版本 → 不冲突。
	mustTx(t, st, func(tx coregraph.Tx) error { return ds.Delete(ctx, tx, "c", []string{"ok"}) })
	if _, err := getRev("ok"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("删后不该读到，得到 %v", err)
	}
	if err := up(storage.Doc{Collection: "c", Key: "ok", Revision: 7}); err != nil {
		t.Fatalf("删后再写不该冲突（删除 = 没有现存版本），得到 %v", err)
	}
	// 反面防线还在：没删时版本不同仍冲突。
	if err := up(storage.Doc{Collection: "c", Key: "ok", Revision: 9}); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("未删时版本不同应 ErrConflict，得到 %v", err)
	}
}

// ---------------------------------------------------------------------------
// 事务原子性（SQL 写面的独特 ACID 性质；白盒走 st.db.BeginTx）
// ---------------------------------------------------------------------------

// TestDocStoreUpsertSameTxVersionConflict（F1 回归）：**同一事务里**二次 Upsert 同一键、
// 版本不同 → 必须 ErrConflict（冲突被吞 = 静默覆盖）。
//
// 旧口径：库内版本检查走实现**自持的读句柄**（读已提交态）→ 看不见本事务里未提交的
// rev1 → "现存版本"查不到（= 不存在 = 无冲突）→ rev2 静默落库（**冲突被吞**）。
//
// 现在的口径：检查经**调用方 Tx**（同一连接、同一快照）→ 看得见未提交的 rev1 → 如实判冲突。
//
// 对照基准：mapDocs 同场景返回 ErrConflict（core/storage/compose_test.go 的替身口径）。
func TestDocStoreUpsertSameTxVersionConflict(t *testing.T) {
	st, ds := newDocStore(t, "file")
	ctx := context.Background()

	// ① 同一事务：先写 rev1，再写 rev2（同键）→ 必须 ErrConflict。
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := ds.Upsert(ctx, graph.SQLTx(tx), []storage.Doc{
		{Collection: "c", Key: "k", Revision: 1},
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("第一次 Upsert(rev1) 应成功: %v", err)
	}
	err = ds.Upsert(ctx, graph.SQLTx(tx), []storage.Doc{
		{Collection: "c", Key: "k", Revision: 2},
	})
	if !errors.Is(err, storage.ErrConflict) {
		_ = tx.Rollback()
		t.Fatalf("同事务二次 Upsert 不同版本应 ErrConflict（冲突被吞 = 静默覆盖），得到 %v", err)
	}
	// 事务内仍是 rev1（rev2 没生效）：检查读的与写的是同一快照。
	var rev int64
	if err := tx.QueryRowContext(ctx,
		`SELECT revision FROM docs WHERE collection = ? AND key = ?`, "c", "k").Scan(&rev); err != nil {
		_ = tx.Rollback()
		t.Fatalf("读事务内版本: %v", err)
	}
	if rev != 1 {
		_ = tx.Rollback()
		t.Fatalf("事务内版本应仍是 1（rev2 没生效），得到 %d", rev)
	}
	// 回滚：这次事务整体不留（写面不替调用方吞错）。
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, err := ds.Get(ctx, "c", "k"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("回滚后不该有残留，得到 %v", err)
	}

	// ② 已提交的 rev1 不被后来的冲突批次改写（**库里保持 rev1**）。
	mustTx(t, st, func(tx coregraph.Tx) error {
		return ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: "k", Revision: 1}})
	})
	err = runTx(t, st, func(tx coregraph.Tx) error {
		return ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: "k", Revision: 9}})
	})
	if !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("版本不符应 ErrConflict，得到 %v", err)
	}
	got, err := ds.Get(ctx, "c", "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Revision != 1 {
		t.Fatalf("冲突批次不该改已有键（库里应保持 rev1），得到 %d", got.Revision)
	}
}

// TestDocStoreUpsertWithoutQueryCapFailsClosed（F1 的 fail-close 面）：传入的 Tx
// **没有 QueryContext 能力**（纯 Exec）而这一批又有 revision>0 的文档 → **显式报错**，
// 绝不静默退回"看不见未提交写"的旧读法（那会把冲突吞掉）。
//
// revision=0 的批次（纯 LWW）不需要查库 → 任何 Tx 都能写。
func TestDocStoreUpsertWithoutQueryCapFailsClosed(t *testing.T) {
	_, ds := newDocStore(t, "file")
	ctx := context.Background()

	// execOnlyTx 是"只有 Exec"的 Tx（形状照 graph.SQLExec 适配出来的纯执行面）。
	tx := execOnlyTx{}
	err := ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: "k", Revision: 1}})
	if err == nil {
		t.Fatal("Tx 读不回自己时，带版本的 Upsert 应显式报错（fail-close）")
	}
	if !errors.Is(err, storage.ErrUnsupported) {
		t.Fatalf("能力缺失应包 ErrUnsupported 哨兵（可 errors.Is 分类），得到 %v", err)
	}
	if !strings.Contains(err.Error(), "QueryContext") {
		t.Fatalf("错误应点明缺 QueryContext 能力，得到 %v", err)
	}
	// rev=0 批次（LWW 免检，不查库）→ 任何 Tx 都能写。
	if err := ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: "k", Revision: 0}}); err != nil {
		t.Fatalf("rev0 的 LWW 批次不该需要 QueryContext，得到 %v", err)
	}
}

// TestDocStoreCompositeKeyNoCollision（F3 回归）：**复合主键不许撞键**。
//
// 库里已有 ("a\x00b", "c") rev1；一批里同时写两条**不同**的文档：
//
//	("a\x00b", "c") rev9 —— 与库里 rev1 冲突（必须 ErrConflict）；
//	("a", "b\x00c")  rev0 —— 另一条文档（LWW 免检，本来不冲突）。
//
// 旧口径（pending 键 = collection + "\x00" + key）把这两条当成"批内同键" → pending 被
// rev0 覆盖 → 免检 → **rev9 静默落库**（冲突被吞）。
//
// 对照基准：mapDocs/logDocs 同场景返回 ErrConflict（core/storage/compose_test.go）。
func TestDocStoreCompositeKeyNoCollision(t *testing.T) {
	st, ds := newDocStore(t, "file")
	ctx := context.Background()

	mustTx(t, st, func(tx coregraph.Tx) error {
		return ds.Upsert(ctx, tx, []storage.Doc{{Collection: "a\x00b", Key: "c", Revision: 1}})
	})

	err := runTx(t, st, func(tx coregraph.Tx) error {
		return ds.Upsert(ctx, tx, []storage.Doc{
			{Collection: "a\x00b", Key: "c", Revision: 9},
			{Collection: "a", Key: "b\x00c", Revision: 0},
		})
	})
	if !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("复合键撞键会让 rev9 静默落库（应 ErrConflict），得到 %v", err)
	}
	// 整批拒绝（零写入）：库里那条仍是 rev1；另一条压根不该出现。
	got, err := ds.Get(ctx, "a\x00b", "c")
	if err != nil {
		t.Fatalf("Get(a\\x00b/c): %v", err)
	}
	if got.Revision != 1 {
		t.Fatalf("冲突批次不该改已有键（版本仍应是 1），得到 %d", got.Revision)
	}
	if _, err := ds.Get(ctx, "a", "b\x00c"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("冲突批次零写入（a/b\\x00c 不该存在），得到 %v", err)
	}
}

// TestDocStoreTxAtomicity：真实 BeginTx 下——
//  1. Upsert 后未 commit 前 Get/List 读不到（跨连接读已提交快照）；
//  2. Rollback 后彻底没有；
//  3. Commit 后读到（含 List）。
func TestDocStoreTxAtomicity(t *testing.T) {
	st, ds := newDocStore(t, "file")
	ctx := context.Background()
	doc := storage.Doc{Collection: "c", Key: "k", Body: json.RawMessage(`{"a":1}`), Revision: 1}

	// ① 未提交：读不到。
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := ds.Upsert(ctx, graph.SQLTx(tx), []storage.Doc{doc}); err != nil {
		t.Fatalf("Upsert(未提交): %v", err)
	}
	if _, err := ds.Get(ctx, "c", "k"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("未提交前 Get 应 ErrNotFound，得到 %v", err)
	}
	if got, err := ds.List(ctx, storage.DocQuery{}); err != nil || len(got) != 0 {
		t.Fatalf("未提交前 List 应空 (err=%v, n=%d)", err, len(got))
	}
	// ② Rollback：彻底没有。
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, err := ds.Get(ctx, "c", "k"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Rollback 后 Get 应 ErrNotFound，得到 %v", err)
	}
	// ③ Commit：读到。
	tx2, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin #2: %v", err)
	}
	if err := ds.Upsert(ctx, graph.SQLTx(tx2), []storage.Doc{doc}); err != nil {
		t.Fatalf("Upsert(待提交): %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	got, err := ds.Get(ctx, "c", "k")
	if err != nil {
		t.Fatalf("Commit 后 Get: %v", err)
	}
	if !reflect.DeepEqual(got, doc) {
		t.Fatalf("Commit 后往返不一致: got=%+v want=%+v", got, doc)
	}
	all, err := ds.List(ctx, storage.DocQuery{})
	if err != nil || len(all) != 1 {
		t.Fatalf("Commit 后 List 应 1 条 (err=%v, n=%d)", err, len(all))
	}

	// 事务内冲突：Upsert 报错 → 回滚 → 库里零变化（整批拒绝 + 事务回滚双保险）。
	tx3, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin #3: %v", err)
	}
	err = ds.Upsert(ctx, graph.SQLTx(tx3), []storage.Doc{
		{Collection: "c", Key: "new", Revision: 1},
		{Collection: "c", Key: "k", Revision: 99}, // 与库内 rev1 冲突
	})
	if !errors.Is(err, storage.ErrConflict) {
		_ = tx3.Rollback()
		t.Fatalf("库内冲突应 ErrConflict，得到 %v", err)
	}
	_ = tx3.Rollback()
	if _, err := ds.Get(ctx, "c", "new"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("冲突事务回滚后不该有 new，得到 %v", err)
	}
}

// ---------------------------------------------------------------------------
// 空 Delete keys / Clear 等边角
// ---------------------------------------------------------------------------

// TestDocStoreDeleteClearEdges：空 keys 直接成功；删不存在的键幂等；Clear 后全缺失。
func TestDocStoreDeleteClearEdges(t *testing.T) {
	st, ds := newDocStore(t, "file")
	ctx := context.Background()

	mustTx(t, st, func(tx coregraph.Tx) error {
		return ds.Upsert(ctx, tx, []storage.Doc{
			{Collection: "c", Key: "a", Revision: 1},
			{Collection: "c", Key: "b", Revision: 1},
			{Collection: "d", Key: "a", Revision: 1},
		})
	})

	// 空 keys → nil，不动任何行。
	mustTx(t, st, func(tx coregraph.Tx) error { return ds.Delete(ctx, tx, "c", nil) })
	mustTx(t, st, func(tx coregraph.Tx) error { return ds.Delete(ctx, tx, "c", []string{}) })
	if got, err := ds.List(ctx, storage.DocQuery{}); err != nil || len(got) != 3 {
		t.Fatalf("空 Delete 不该动行 (err=%v, n=%d)", err, len(got))
	}
	// 删不存在的键 → 幂等成功。
	mustTx(t, st, func(tx coregraph.Tx) error { return ds.Delete(ctx, tx, "c", []string{"ghost"}) })

	// 只删指定集合的指定键（d/a 不受 c/a 删除影响）。
	mustTx(t, st, func(tx coregraph.Tx) error { return ds.Delete(ctx, tx, "c", []string{"a", "b"}) })
	if _, err := ds.Get(ctx, "c", "a"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("c/a 删后应缺失，得到 %v", err)
	}
	if _, err := ds.Get(ctx, "d", "a"); err != nil {
		t.Fatalf("d/a 不该被 c 的删除波及: %v", err)
	}

	// Clear → 全缺失。
	mustTx(t, st, func(tx coregraph.Tx) error { return ds.Clear(ctx, tx) })
	if got, err := ds.List(ctx, storage.DocQuery{}); err != nil || len(got) != 0 {
		t.Fatalf("Clear 后 List 应空 (err=%v, n=%d)", err, len(got))
	}
	if _, err := ds.Get(ctx, "d", "a"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Clear 后 Get 应 ErrNotFound，得到 %v", err)
	}
	// Clear 幂等（空表再清不炸）。
	mustTx(t, st, func(tx coregraph.Tx) error { return ds.Clear(ctx, tx) })
}

// ---------------------------------------------------------------------------
// Transact（宿主 ingest 面的事务跑道：BeginTx → fn → Commit/Rollback 收口）
// ---------------------------------------------------------------------------

// TestDocStoreTransactCommit：Transact 提交后可见（Get / List 都读到）。
func TestDocStoreTransactCommit(t *testing.T) {
	_, ds := newDocStore(t, "file")
	ctx := context.Background()
	doc := storage.Doc{Collection: "c", Key: "k", Body: json.RawMessage(`{"a":1}`), Revision: 1}
	if err := ds.Transact(ctx, func(tx coregraph.Tx) error {
		return ds.Upsert(ctx, tx, []storage.Doc{doc})
	}); err != nil {
		t.Fatalf("Transact: %v", err)
	}
	got, err := ds.Get(ctx, "c", "k")
	if err != nil {
		t.Fatalf("提交后 Get: %v", err)
	}
	if !reflect.DeepEqual(got, doc) {
		t.Fatalf("提交后往返不一致: got=%+v want=%+v", got, doc)
	}
	all, err := ds.List(ctx, storage.DocQuery{})
	if err != nil || len(all) != 1 {
		t.Fatalf("提交后 List 应 1 条 (err=%v, n=%d)", err, len(all))
	}
}

// TestDocStoreTransactFnErrorRollsBack：fn 报错 → 整体回滚——回滚后 Get 缺失；
// 已提交的既有键不受影响（回滚不波及事务外的行）。
func TestDocStoreTransactFnErrorRollsBack(t *testing.T) {
	_, ds := newDocStore(t, "file")
	ctx := context.Background()
	// 种子：一条已提交的键（回滚后必须还在）。
	if err := ds.Transact(ctx, func(tx coregraph.Tx) error {
		return ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: "seed", Revision: 1}})
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	boom := errors.New("fn 报错")
	err := ds.Transact(ctx, func(tx coregraph.Tx) error {
		if uerr := ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: "k", Revision: 1}}); uerr != nil {
			return uerr
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Transact 应原样交还 fn 的错误，得到 %v", err)
	}
	if _, err := ds.Get(ctx, "c", "k"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("回滚后 Get 应 ErrNotFound，得到 %v", err)
	}
	if _, err := ds.Get(ctx, "c", "seed"); err != nil {
		t.Fatalf("回滚不该波及已提交的 seed: %v", err)
	}
}

// TestDocStoreTransactWriteThenErrorLeavesNothing：fn 里写一条再报错——写下的那条
// 也不留（整事务回滚）；同批写两条后报错，两条都不留。
func TestDocStoreTransactWriteThenErrorLeavesNothing(t *testing.T) {
	_, ds := newDocStore(t, "file")
	ctx := context.Background()
	boom := errors.New("写后报错")
	// ① 写一条再报错 → 该条不留。
	err := ds.Transact(ctx, func(tx coregraph.Tx) error {
		if uerr := ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: "only", Revision: 1}}); uerr != nil {
			return uerr
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("应原样交还 fn 的错误，得到 %v", err)
	}
	if _, err := ds.Get(ctx, "c", "only"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("fn 写一条再报错，该条不该留，得到 %v", err)
	}
	// ② 同批写两条（一条成功落语句）后报错 → 两条都不留。
	err = ds.Transact(ctx, func(tx coregraph.Tx) error {
		for _, k := range []string{"a", "b"} {
			if uerr := ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: k, Revision: 1}}); uerr != nil {
				return uerr
			}
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("应原样交还 fn 的错误，得到 %v", err)
	}
	for _, k := range []string{"a", "b"} {
		if _, err := ds.Get(ctx, "c", k); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("报错后 %s 不该留（另一条也不留），得到 %v", k, err)
		}
	}
	if got, err := ds.List(ctx, storage.DocQuery{}); err != nil || len(got) != 0 {
		t.Fatalf("回滚后 List 应空 (err=%v, n=%d)", err, len(got))
	}
}

// TestDocStoreTransactPanicReleasesLock（F4 回归）：fn **panic** 时不能把写锁悬着。
//
// `_txlock=immediate` ⇒ **BeginTx 即拿写锁**（runtime/store/sqlite 的 buildDSN）。
// 旧口径只在 fn **返回错误**时 Rollback —— fn panic（panic 穿过 Transact 向上传播）时
// 事务悬着：那条连接一直被事务占着、写锁一直被占着 → 同一库后续所有写卡到
// busy_timeout 超时才失败（"一把锁卡死整库"）。
//
// 现在的口径：BeginTx 之后立刻 `defer tx.Rollback()`（panic 展开时也会跑）→ 写锁释放。
func TestDocStoreTransactPanicReleasesLock(t *testing.T) {
	_, ds := newDocStore(t, "file")
	ctx := context.Background()

	// fn panic：测试里 recover 住（模拟上层有 recover 的真实调用方）。
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("fn 的 panic 应穿过 Transact 向上传播")
			}
		}()
		_ = ds.Transact(ctx, func(tx coregraph.Tx) error {
			// 事务里真写一行（锁与写都真的发生，不是空转）。
			if err := ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: "k", Revision: 1}}); err != nil {
				return err
			}
			panic("boom")
		})
	}()

	// panic 之后：同一库继续写必须成功（写锁没卡住）。
	if err := ds.Transact(ctx, func(tx coregraph.Tx) error {
		return ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: "after", Revision: 1}})
	}); err != nil {
		t.Fatalf("panic 后同一库继续写应成功（写锁没泄漏），得到 %v", err)
	}
	if _, err := ds.Get(ctx, "c", "after"); err != nil {
		t.Fatalf("panic 后写下的文档应读得到: %v", err)
	}
	// panic 那次事务整体回滚（不该留下半条写入）。
	if _, err := ds.Get(ctx, "c", "k"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("panic 的事务应整体回滚（c/k 不该存在），得到 %v", err)
	}
}

// ---------------------------------------------------------------------------
// 换实现零改动酸测试（同一份 consumer 跑内存替身 vs 真 SQLite）
// ---------------------------------------------------------------------------

// nopTx 是"空事务"（内存替身用：Exec 直接成功——SQL 文本归实现，替身不执行 SQL）。
type nopTx struct{}

func (nopTx) Exec(context.Context, string, ...any) error { return nil }

// refDocs 是最小内存文档实现（mapDocs 的简化版，语义照抄：整批拒绝 + 不对称批内口径）。
// 只活在测试里——证明"消费方只认接口，换实现零改动"。
type refDocs struct {
	mu   sync.RWMutex
	data map[refKey]storage.Doc
}

func newRefDocs() *refDocs { return &refDocs{data: map[refKey]storage.Doc{}} }

// refKey 是复合主键（collection + key）——**结构体键**，不是拼字符串
// （拼字符串会撞键：("a\x00b","c") 与 ("a","b\x00c") 拼出来一样 → 两条不同文档被当成一条）。
type refKey struct {
	collection string
	key        string
}

func newRefKey(collection, key string) refKey { return refKey{collection: collection, key: key} }

var (
	_ storage.Documents          = (*refDocs)(nil)
	_ storage.DocumentProjection = (*refDocs)(nil)
)

func (m *refDocs) EnsureSchema(context.Context) error { return nil }

func (m *refDocs) Get(_ context.Context, collection, key string) (storage.Doc, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	doc, ok := m.data[newRefKey(collection, key)]
	if !ok {
		return storage.Doc{}, fmt.Errorf("%w: doc %s/%s", storage.ErrNotFound, collection, key)
	}
	return doc, nil
}

func (m *refDocs) List(_ context.Context, q storage.DocQuery) ([]storage.Doc, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []storage.Doc
	for _, doc := range m.data {
		if q.Collection != "" && doc.Collection != q.Collection {
			continue
		}
		if q.Prefix != "" && !strings.HasPrefix(doc.Key, q.Prefix) {
			continue
		}
		out = append(out, doc)
	}
	// 按 (collection, key) 排序——与 SQLite 的 BINARY 序一致。
	for i := 1; i < len(out); i++ {
		for j := i; j > 0; j-- {
			a, b := out[j-1], out[j]
			if a.Collection < b.Collection || (a.Collection == b.Collection && a.Key < b.Key) {
				break
			}
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

func (m *refDocs) Upsert(_ context.Context, _ coregraph.Tx, docs []storage.Doc) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// ① 先整批校验（批内 + 与库内现存版本），② 再整批写——整批拒绝。
	pending := map[refKey]storage.Doc{}
	for _, doc := range docs {
		k := newRefKey(doc.Collection, doc.Key)
		if prev, ok := pending[k]; ok && doc.Revision > 0 && prev.Revision != doc.Revision {
			return fmt.Errorf("%w: 批内 doc %s 版本 %d ≠ %d",
				storage.ErrConflict, k, doc.Revision, prev.Revision)
		}
		pending[k] = doc
	}
	for k, doc := range pending {
		if old, ok := m.data[k]; ok && doc.Revision > 0 && old.Revision != doc.Revision {
			return fmt.Errorf("%w: doc %s 版本 %d ≠ %d",
				storage.ErrConflict, k, doc.Revision, old.Revision)
		}
	}
	for _, doc := range docs {
		m.data[newRefKey(doc.Collection, doc.Key)] = doc
	}
	return nil
}

func (m *refDocs) Delete(_ context.Context, _ coregraph.Tx, collection string, keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		delete(m.data, newRefKey(collection, k))
	}
	return nil
}

func (m *refDocs) Clear(_ context.Context, _ coregraph.Tx) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = map[refKey]storage.Doc{}
	return nil
}

// docTxRunner 把一批写面调用放进一个可提交的写事务（内存替身 = 立即"提交"的空事务）。
type docTxRunner func(fn func(tx coregraph.Tx) error) error

// docConsumer 是**只认接口**的剧本（host / 索引器的缩影）：拿到读面 + 写面 + 事务跑道，
// 跑一段写→读→冲突→删→读的全链路，产出确定性日志。
// **这份函数在两套实现之间一字不改**——它就是"换实现零改动"的被测对象。
func docConsumer(ctx context.Context, r storage.Documents, w storage.DocumentProjection, run docTxRunner) ([]string, error) {
	var log []string
	rec := func(format string, args ...any) { log = append(log, fmt.Sprintf(format, args...)) }

	if err := w.EnsureSchema(ctx); err != nil {
		return nil, fmt.Errorf("ensureschema: %w", err)
	}
	// ① 写一批（两个集合、乱序键、带全套元数据）。
	if err := run(func(tx coregraph.Tx) error {
		return w.Upsert(ctx, tx, []storage.Doc{
			{Collection: "rules", Key: "b2", Body: json.RawMessage(`{"n":2}`), Revision: 2, UpdatedSeq: 20, UpdatedAt: "2026-01-01T00:00:02Z"},
			{Collection: "rules", Key: "a1", Body: json.RawMessage(`{"n":1}`), Revision: 1, UpdatedSeq: 10, UpdatedAt: "2026-01-01T00:00:01Z"},
			{Collection: "know", Key: "z9", Revision: 1},
		})
	}); err != nil {
		return nil, fmt.Errorf("seed upsert: %w", err)
	}

	// ② Get 往返。
	d, err := r.Get(ctx, "rules", "a1")
	if err != nil {
		return nil, fmt.Errorf("get a1: %w", err)
	}
	rec("get a1 = %s", docLine(d))
	// ③ 缺失。
	if _, err := r.Get(ctx, "rules", "nope"); !errors.Is(err, storage.ErrNotFound) {
		return nil, fmt.Errorf("get missing: want ErrNotFound, got %v", err)
	}
	rec("get missing = notfound")

	// ④ List：全量 / 集合 / 字节前缀（大小写敏感）/ Limit。
	all, err := r.List(ctx, storage.DocQuery{})
	if err != nil {
		return nil, fmt.Errorf("list all: %w", err)
	}
	rec("list all = %s", docLines(all))
	rules, err := r.List(ctx, storage.DocQuery{Collection: "rules"})
	if err != nil {
		return nil, fmt.Errorf("list rules: %w", err)
	}
	rec("list rules = %s", docLines(rules))
	pref, err := r.List(ctx, storage.DocQuery{Prefix: "b"})
	if err != nil {
		return nil, fmt.Errorf("list prefix b: %w", err)
	}
	rec("list prefix b = %s", docLines(pref))
	lim, err := r.List(ctx, storage.DocQuery{Limit: 2})
	if err != nil {
		return nil, fmt.Errorf("list limit 2: %w", err)
	}
	rec("list limit2 = %s", docLines(lim))

	// ⑤ 冲突：与库版本不符 → 整批拒 + 零写入。
	err = run(func(tx coregraph.Tx) error {
		return w.Upsert(ctx, tx, []storage.Doc{{Collection: "rules", Key: "a1", Revision: 9}})
	})
	if !errors.Is(err, storage.ErrConflict) {
		return nil, fmt.Errorf("db conflict: want ErrConflict, got %v", err)
	}
	rec("conflict rev9 = conflict")
	err = run(func(tx coregraph.Tx) error {
		return w.Upsert(ctx, tx, []storage.Doc{
			{Collection: "rules", Key: "fresh", Revision: 1},
			{Collection: "rules", Key: "a1", Revision: 9},
		})
	})
	if !errors.Is(err, storage.ErrConflict) {
		return nil, fmt.Errorf("mixed batch conflict: want ErrConflict, got %v", err)
	}
	if _, err := r.Get(ctx, "rules", "fresh"); !errors.Is(err, storage.ErrNotFound) {
		return nil, fmt.Errorf("conflict batch must be zero-write, got %v", err)
	}
	rec("conflict batch zero-write = ok")

	// ⑥ 同版本覆盖（LWW on equal rev）。
	if err := run(func(tx coregraph.Tx) error {
		return w.Upsert(ctx, tx, []storage.Doc{
			{Collection: "rules", Key: "a1", Body: json.RawMessage(`{"n":1,"v":2}`), Revision: 1, UpdatedSeq: 11},
		})
	}); err != nil {
		return nil, fmt.Errorf("same-rev overwrite: %w", err)
	}
	d, err = r.Get(ctx, "rules", "a1")
	if err != nil {
		return nil, fmt.Errorf("get a1 after overwrite: %w", err)
	}
	rec("get a1 after overwrite = %s", docLine(d))

	// ⑦ Delete → 缺失；再写回（删后无现存版本，不冲突）。
	if err := run(func(tx coregraph.Tx) error { return w.Delete(ctx, tx, "rules", []string{"b2"}) }); err != nil {
		return nil, fmt.Errorf("delete b2: %w", err)
	}
	if _, err := r.Get(ctx, "rules", "b2"); !errors.Is(err, storage.ErrNotFound) {
		return nil, fmt.Errorf("deleted b2: want ErrNotFound, got %v", err)
	}
	rec("get b2 after delete = notfound")
	if err := run(func(tx coregraph.Tx) error {
		return w.Upsert(ctx, tx, []storage.Doc{{Collection: "rules", Key: "b2", Revision: 5}})
	}); err != nil {
		return nil, fmt.Errorf("rewrite after delete: %w", err)
	}
	rec("rewrite after delete = ok")

	// ⑧ 终态 List。
	final, err := r.List(ctx, storage.DocQuery{})
	if err != nil {
		return nil, fmt.Errorf("final list: %w", err)
	}
	rec("final list = %s", docLines(final))
	return log, nil
}

// TestDocStoreSwapImplZeroChange：换实现零改动酸测试——
// 同一份 docConsumer 跑 (内存 refDocs + nopTx) 与 (DocStore + 真实事务)，逐条对答案。
func TestDocStoreSwapImplZeroChange(t *testing.T) {
	ctx := context.Background()

	// 实现 A：内存 map（nopTx 跑事务）。
	ref := newRefDocs()
	logA, err := docConsumer(ctx, ref, ref, func(fn func(tx coregraph.Tx) error) error {
		return fn(nopTx{})
	})
	if err != nil {
		t.Fatalf("实现 A（内存）: %v", err)
	}

	// 实现 B：真 SQLite（真实 BeginTx 跑事务）。
	st, ds := newDocStore(t, "file")
	logB, err := docConsumer(ctx, ds, ds, func(fn func(tx coregraph.Tx) error) error {
		return runTx(t, st, fn)
	})
	if err != nil {
		t.Fatalf("实现 B（SQLite）: %v", err)
	}

	// 防"两套退回同一实现"：具体类型必须不同（否则酸测试是假绿）。
	if reflect.TypeOf(ref) == reflect.TypeOf(ds) {
		t.Fatalf("两套实现是同一类型 %v（酸测试对文档族等于没测）", reflect.TypeOf(ref))
	}

	// 逐条比对（两边日志必须一字不差）。
	if len(logA) != len(logB) {
		t.Fatalf("日志条数不一致: A=%d B=%d\nA=%v\nB=%v", len(logA), len(logB), logA, logB)
	}
	for i := range logA {
		if logA[i] != logB[i] {
			t.Fatalf("第 %d 条答案不一致（换实现零改动失败）:\nA=%s\nB=%s", i, logA[i], logB[i])
		}
	}

	// 抽查具体值（防空对空的假绿）。
	joined := strings.Join(logB, "\n")
	for _, want := range []string{
		"get a1 = rules/a1 rev=1 seq=10 at=2026-01-01T00:00:01Z body={\"n\":1}",
		"get missing = notfound",
		"conflict rev9 = conflict",
		"conflict batch zero-write = ok",
		"get b2 after delete = notfound",
		"rewrite after delete = ok",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("终稿日志缺 %q:\n%s", want, joined)
		}
	}
	// 前缀答案钉死：prefix=b 只命中 b2（a1/z9 不带 b；大小写语义由 TestDocStoreList 盖）。
	if !strings.Contains(joined, "list prefix b = [rules/b2 rev=2 seq=20 at=2026-01-01T00:00:02Z body={\"n\":2}]") {
		t.Fatalf("前缀过滤答案不对:\n%s", joined)
	}
}

// ---------------------------------------------------------------------------
// 并发写（F10 真实不变量）与 :memory: 能力面（F2 修复面）
// ---------------------------------------------------------------------------

// TestDocStoreConcurrentUpsertSameKey（F10）：并发多客户端 upsert **同一键**（版本各异）
// → 恰好 1 成功 + 5 ErrConflict——**不是"最后写赢"**。
//
// 来源是两件结构事实（见 DocStore 注释）：检查+写都在调用方事务里，而 `_txlock=immediate`
// 让 BeginTx 即拿写锁 → 后拿锁的读到的已是前一个提交后的版本 → 如实判冲突。
// 这条用例把那个结论钉进仓库（此前只有注释承载，重构会无声回退）。
func TestDocStoreConcurrentUpsertSameKey(t *testing.T) {
	_, ds := newDocStore(t, "file")
	ctx := context.Background()

	const n = 6
	results := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(rev int64) {
			defer wg.Done()
			results <- ds.Transact(ctx, func(tx coregraph.Tx) error {
				return ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: "k", Revision: rev}})
			})
		}(int64(i + 1))
	}
	wg.Wait()
	close(results)

	var okCount, conflictCount int
	for err := range results {
		switch {
		case err == nil:
			okCount++
		case errors.Is(err, storage.ErrConflict):
			conflictCount++
		default:
			t.Fatalf("并发 upsert 应成功或 ErrConflict，得到 %v", err)
		}
	}
	if okCount != 1 || conflictCount != n-1 {
		t.Fatalf("并发同键 upsert 应恰好 1 成功 + %d 冲突（不是最后写赢），得到 ok=%d conflict=%d",
			n-1, okCount, conflictCount)
	}
	// 存活那条的版本就是唯一成功者的版本（1..6 之一，不指定哪个——先拿锁的赢）。
	got, err := ds.Get(ctx, "c", "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Revision < 1 || got.Revision > n {
		t.Fatalf("存活版本应在 1..%d 之间，得到 %d", n, got.Revision)
	}
}

// TestDocStoreMemoryModeSameTxVersionCheck（F2 修复面）：:memory: 上**带版本的写可用**——
// 版本检查走调用方 Tx（与写同连接），不再需要第二条连接（旧口径在内存库上撞
// "no such table"：第二条连接是另一座空库）。
//
// 注意边界：**跨连接读**（Get/List 走自持句柄）在 :memory: 下受驱动属性限制
// （见文件头注释）——本用例只钉"检查+写同事务"这条修复面，不越界。
func TestDocStoreMemoryModeSameTxVersionCheck(t *testing.T) {
	_, ds := newDocStore(t, "memory")
	ctx := context.Background()

	// 同一事务：rev1 → rev2（同键）→ 必须 ErrConflict（检查看得见未提交的 rev1）。
	err := ds.Transact(ctx, func(tx coregraph.Tx) error {
		if uerr := ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: "k", Revision: 1}}); uerr != nil {
			return uerr
		}
		return ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: "k", Revision: 2}})
	})
	if !errors.Is(err, storage.ErrConflict) {
		t.Fatalf(":memory: 同事务二次 Upsert 不同版本应 ErrConflict，得到 %v", err)
	}
	// rev0 的 LWW 批次（免检）在 :memory: 上也照常写。
	if err := ds.Transact(ctx, func(tx coregraph.Tx) error {
		return ds.Upsert(ctx, tx, []storage.Doc{{Collection: "c", Key: "k", Revision: 0}})
	}); err != nil {
		t.Fatalf(":memory: rev0 LWW 写应成功，得到 %v", err)
	}
}
