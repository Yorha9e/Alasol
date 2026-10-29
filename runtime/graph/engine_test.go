package graph_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	_ "github.com/ncruces/go-sqlite3/driver"

	"github.com/Yorha9e/alasol/contracts"
	coregraph "github.com/Yorha9e/alasol/core/graph"
	"github.com/Yorha9e/alasol/runtime/graph"
)

// ---------------------------------------------------------------------------
// 测试装置：真 SQLite 文件库 + 写面灌边 + 计数的读句柄
// ---------------------------------------------------------------------------

// openDB 开一个真文件库（DSN 口径与 sqlite.Open 一致：busy_timeout + _txlock=immediate）。
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "graph.db")
	dsn := "file:" + filepath.ToSlash(path) + "?_pragma=busy_timeout%3d5000&_txlock=immediate"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// countingQuerier 包一层计数的读句柄（断言"缓存命中时不落库"用）。
//
// 顺带记住**最后一条查询的参数**（N4 边界钉子用：断言 LIMIT 参数不是负数——
// 负 LIMIT 在 SQLite 里是"无上限"，超级节点护栏会被悄悄绕过）。
type countingQuerier struct {
	inner graph.Querier
	mu    sync.Mutex
	n     int
	// lastArgs 是最后一次 QueryContext 的参数。
	lastArgs []any
}

func (c *countingQuerier) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	c.mu.Lock()
	c.n++
	c.lastArgs = append([]any(nil), args...)
	c.mu.Unlock()
	return c.inner.QueryContext(ctx, q, args...)
}

func (c *countingQuerier) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
	return c.inner.QueryRowContext(ctx, q, args...)
}

func (c *countingQuerier) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// lastLimitArg 返回最后一条查询里最后一个整数参数（邻居查询的 LIMIT 参数）。
func (c *countingQuerier) lastLimitArg() (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.lastArgs) - 1; i >= 0; i-- {
		switch v := c.lastArgs[i].(type) {
		case int64:
			return v, true
		case int:
			return int64(v), true
		}
	}
	return 0, false
}

// seed 建图表 + 灌一批边（走**写面** Projection——与索引器同一条路，不是测试私连写）。
func seed(t *testing.T, db *sql.DB, proj coregraph.GraphProjection, seq int64, edges ...contracts.GraphEdge) {
	t.Helper()
	ctx := context.Background()
	if proj == nil {
		// 写面**自建表**（F6）：DDL 不塞进调用方事务 → 写面自带执行句柄。
		proj = graph.NewProjection(nil, dbQuerier{db: db})
	}
	if err := proj.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := proj.ApplyEdges(ctx, graph.SQLTx(tx), seq, edges); err != nil {
		t.Fatalf("apply edges: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	proj.Committed() // 索引器提交成功后也会调这一下（G20 失效通知）
}

func edge(src, dst, kind string) contracts.GraphEdge {
	return contracts.GraphEdge{Src: src, Dst: dst, Kind: kind}
}

// depGraph 是测试用的依赖图（kind "imports"）：
//
//	app → core → util ；cli → core
//	另有一条 tests 边 app → thelper（用来验 kind 过滤）。
func depGraph(t *testing.T, db *sql.DB, proj coregraph.GraphProjection) {
	t.Helper()
	// 每条边一个 seq（增量加载的水位是按 seq 走的：同 seq 的一批边会被一次拉完，
	// 但测试要能分别制造"新写入"，所以分开给）。
	seedSeq(t, db, proj,
		edge("app", "core", "imports"),
		edge("cli", "core", "imports"),
		edge("core", "util", "imports"),
		edge("app", "thelper", "tests"),
	)
}

// seedSeq 给每条边分一个递增 seq 后逐条灌（测试里要能分别制造"新写入"）。
func seedSeq(t *testing.T, db *sql.DB, proj coregraph.GraphProjection, edges ...contracts.GraphEdge) {
	t.Helper()
	for i, e := range edges {
		seed(t, db, proj, int64(i+1), e)
	}
}

// newEngine 建一个包了计数句柄的引擎。
func newEngine(t *testing.T, db *sql.DB, limits coregraph.Limits) (*graph.Engine, *countingQuerier) {
	t.Helper()
	cq := &countingQuerier{inner: dbQuerier{db: db}}
	eng, err := graph.NewEngine(cq, limits)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return eng, cq
}

// dbQuerier 把 *sql.DB 适配成 graph.Querier（同时满足 graph.SQLExecTx：建表用）。
type dbQuerier struct{ db *sql.DB }

func (q dbQuerier) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return q.db.QueryContext(ctx, query, args...)
}

func (q dbQuerier) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return q.db.QueryRowContext(ctx, query, args...)
}

func (q dbQuerier) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return q.db.ExecContext(ctx, query, args...)
}

// ---------------------------------------------------------------------------
// 一跳/两跳：走 SQLite 双向倒排（G18）
// ---------------------------------------------------------------------------

// TestNeighborsOneHopBothDirections：一跳双向倒排命中正确，且**直查库**（内存图还是空的）。
func TestNeighborsOneHopBothDirections(t *testing.T) {
	db := openDB(t)
	depGraph(t, db, nil)
	eng, _ := newEngine(t, db, graph.Limits{})
	ctx := context.Background()

	// 查询前内存图必须是空的：这样才能证明答案来自 SQLite 倒排而不是内存遍历。
	if st := eng.Stats(); st.Nodes != 0 || st.Edges != 0 {
		t.Fatalf("查询前内存图应空，得到 %+v", st)
	}

	// 入向（谁依赖 core）= app / cli。
	in, err := eng.Neighbors(ctx, graph.NeighborQuery{Node: "core", Kind: "imports", Direction: graph.DirectionIn, Hops: 1})
	if err != nil {
		t.Fatalf("Neighbors in: %v", err)
	}
	if strings.Join(in, ",") != "app,cli" {
		t.Errorf("in = %v, want [app cli]", in)
	}
	// 出向（core 依赖谁）= util。
	out, err := eng.Neighbors(ctx, graph.NeighborQuery{Node: "core", Kind: "imports", Direction: graph.DirectionOut, Hops: 1})
	if err != nil {
		t.Fatalf("Neighbors out: %v", err)
	}
	if strings.Join(out, ",") != "util" {
		t.Errorf("out = %v, want [util]", out)
	}
	// 内存图仍然空（一跳压根不碰它）。
	if st := eng.Stats(); st.Nodes != 0 {
		t.Errorf("一跳查询后内存图应仍空，得到 %+v", st)
	}
}

// TestNeighborsTwoHop：两跳（join 倒排）双向都正确。
func TestNeighborsTwoHop(t *testing.T) {
	db := openDB(t)
	depGraph(t, db, nil)
	eng, _ := newEngine(t, db, graph.Limits{})
	ctx := context.Background()

	// 入向两跳：谁"间接"依赖 util → core 的依赖者 = app / cli（core 本人是一跳，不在两跳结果里）。
	in, err := eng.Neighbors(ctx, graph.NeighborQuery{Node: "util", Kind: "imports", Direction: graph.DirectionIn, Hops: 2})
	if err != nil {
		t.Fatalf("Neighbors 2 in: %v", err)
	}
	if strings.Join(in, ",") != "app,cli" {
		t.Errorf("in = %v, want [app cli]", in)
	}
	// 出向两跳：app → core → util。
	out, err := eng.Neighbors(ctx, graph.NeighborQuery{Node: "app", Kind: "imports", Direction: graph.DirectionOut, Hops: 2})
	if err != nil {
		t.Fatalf("Neighbors 2 out: %v", err)
	}
	if strings.Join(out, ",") != "util" {
		t.Errorf("out = %v, want [util]", out)
	}
}

// TestNeighborsKindFilterAndSelfLoop：kind 过滤 + 起点自身被滤掉。
func TestNeighborsKindFilterAndSelfLoop(t *testing.T) {
	db := openDB(t)
	depGraph(t, db, nil)
	seed(t, db, nil, 2, edge("self", "self", "imports"))
	eng, _ := newEngine(t, db, graph.Limits{})
	ctx := context.Background()

	// kind=tests：app 的 tests 边指向 thelper。
	tests, err := eng.Neighbors(ctx, graph.NeighborQuery{Node: "app", Kind: "tests", Direction: graph.DirectionOut, Hops: 1})
	if err != nil {
		t.Fatalf("Neighbors tests: %v", err)
	}
	if strings.Join(tests, ",") != "thelper" {
		t.Errorf("tests = %v, want [thelper]", tests)
	}
	// kind 为空 = 不限：app 出向 = core + thelper。
	all, err := eng.Neighbors(ctx, graph.NeighborQuery{Node: "app", Direction: graph.DirectionOut, Hops: 1})
	if err != nil {
		t.Fatalf("Neighbors all: %v", err)
	}
	if strings.Join(all, ",") != "core,thelper" {
		t.Errorf("all = %v, want [core thelper]", all)
	}
	// 自环：self 的邻居不该含自己。
	self, err := eng.Neighbors(ctx, graph.NeighborQuery{Node: "self", Kind: "imports", Direction: graph.DirectionOut, Hops: 1})
	if err != nil {
		t.Fatalf("Neighbors self: %v", err)
	}
	if len(self) != 0 {
		t.Errorf("self = %v, want 空", self)
	}
}

// TestNeighborsBadHops：hops 只支持 1|2（更多走内存 BFS，G18）。
func TestNeighborsBadHops(t *testing.T) {
	db := openDB(t)
	depGraph(t, db, nil)
	eng, _ := newEngine(t, db, graph.Limits{})
	for _, hops := range []int{0, -1, 3, 10} {
		if _, err := eng.Neighbors(context.Background(), graph.NeighborQuery{Node: "app", Hops: hops}); !errors.Is(err, coregraph.ErrBadHops) {
			t.Errorf("hops=%d err = %v, want ErrBadHops", hops, err)
		}
	}
	if _, err := eng.Neighbors(context.Background(), graph.NeighborQuery{Node: "", Hops: 1}); err == nil {
		t.Error("空节点应报错")
	}
}

// ---------------------------------------------------------------------------
// 多跳：内存图 BFS（G18）+ 闭包缓存（G20）
// ---------------------------------------------------------------------------

// TestReachableMultiHop：多跳 BFS 结果正确（含跨层菱形与环）。
func TestReachableMultiHop(t *testing.T) {
	db := openDB(t)
	depGraph(t, db, nil)
	eng, _ := newEngine(t, db, graph.Limits{})
	ctx := context.Background()

	out, err := eng.Reachable(ctx, graph.ReachQuery{Sources: []string{"app"}, Kind: "imports", Direction: graph.DirectionOut})
	if err != nil {
		t.Fatalf("Reachable out: %v", err)
	}
	if strings.Join(out.Nodes, ",") != "core,util" {
		t.Errorf("Nodes = %v, want [core util]", out.Nodes)
	}
	if out.Depth != 2 {
		t.Errorf("Depth = %d, want 2", out.Depth)
	}
	in, err := eng.Reachable(ctx, graph.ReachQuery{Sources: []string{"util"}, Kind: "imports", Direction: graph.DirectionIn})
	if err != nil {
		t.Fatalf("Reachable in: %v", err)
	}
	// 层序：core（一跳）→ app, cli（两跳）。层内排序，层间按深度。
	if strings.Join(in.Nodes, ",") != "core,app,cli" {
		t.Errorf("Nodes = %v, want [core app cli]", in.Nodes)
	}
	if len(in.Levels) != 2 || strings.Join(in.Levels[0], ",") != "core" || strings.Join(in.Levels[1], ",") != "app,cli" {
		t.Errorf("Levels = %v, want [[core] [app cli]]", in.Levels)
	}
}

// TestClosureCacheNoDBOnHit：缓存命中时**一次 SQL 都不发**（G20）。
func TestClosureCacheNoDBOnHit(t *testing.T) {
	db := openDB(t)
	depGraph(t, db, nil)
	eng, cq := newEngine(t, db, graph.Limits{})
	ctx := context.Background()
	q := graph.ReachQuery{Sources: []string{"app"}, Kind: "imports", Direction: graph.DirectionIn}

	if _, err := eng.Reachable(ctx, q); err != nil {
		t.Fatalf("Reachable 1: %v", err)
	}
	first := cq.count()
	if first == 0 {
		t.Fatal("第一次查询至少该发一条 SQL（增量加载）")
	}
	if _, err := eng.Reachable(ctx, q); err != nil {
		t.Fatalf("Reachable 2: %v", err)
	}
	if cq.count() != first {
		t.Errorf("第二次查询又发了 %d 条 SQL，want 0（闭包缓存命中不落库）", cq.count()-first)
	}
	st := eng.Stats()
	if st.CacheHits != 1 || st.CacheMisses != 1 {
		t.Errorf("CacheHits/Misses = %d/%d, want 1/1", st.CacheHits, st.CacheMisses)
	}
	// 换起点 → 新键 → miss（不会互相污染）。
	if _, err := eng.Reachable(ctx, graph.ReachQuery{Sources: []string{"cli"}, Kind: "imports", Direction: graph.DirectionIn}); err != nil {
		t.Fatalf("Reachable cli: %v", err)
	}
	if st := eng.Stats(); st.CacheMisses != 2 {
		t.Errorf("CacheMisses = %d, want 2", st.CacheMisses)
	}
}

// TestInvalidateAfterWrite：图写入后缓存失效（G20）——索引器提交 → Committed → 查询看到新边。
func TestInvalidateAfterWrite(t *testing.T) {
	db := openDB(t)
	depGraph(t, db, nil)
	// 写面接上引擎：seed 里的 proj.Committed() 就是索引器提交后的那一拍。
	eng, _ := newEngine(t, db, graph.Limits{})
	proj := graph.NewProjection(eng, dbQuerier{db: db})
	ctx := context.Background()
	q := graph.ReachQuery{Sources: []string{"util"}, Kind: "imports", Direction: graph.DirectionIn}

	before, err := eng.Reachable(ctx, q)
	if err != nil {
		t.Fatalf("Reachable before: %v", err)
	}
	if strings.Join(before.Nodes, ",") != "core,app,cli" {
		t.Fatalf("before Nodes = %v, want [core app cli]", before.Nodes)
	}

	// 新边：util → app（util 的依赖者多了一个 app）。
	seed(t, db, proj, 9, edge("app", "util", "imports"))

	after, err := eng.Reachable(ctx, q)
	if err != nil {
		t.Fatalf("Reachable after: %v", err)
	}
	// app 现在也是一跳（它直接依赖 util），cli 变成两跳。
	if strings.Join(after.Nodes, ",") != "app,core,cli" {
		t.Errorf("after Nodes = %v, want [app core cli]（写入后缓存必须失效）", after.Nodes)
	}
}

// TestSyncPicksUpExternalWrites：别的进程写进来的边，Sync 后可见（跨进程最终一致）。
func TestSyncPicksUpExternalWrites(t *testing.T) {
	db := openDB(t)
	depGraph(t, db, nil)
	eng, _ := newEngine(t, db, graph.Limits{})
	ctx := context.Background()
	q := graph.ReachQuery{Sources: []string{"util"}, Kind: "imports", Direction: graph.DirectionIn}

	if _, err := eng.Reachable(ctx, q); err != nil {
		t.Fatalf("Reachable 1: %v", err)
	}
	// 模拟**另一个进程**直接写库（本引擎完全不知情，也没有 Committed 通知）。
	seed(t, db, nil, 9, edge("app", "util", "imports"))
	// 不 Sync 就直接查：缓存还温着 → 旧答案（这是已声明的取舍：跨进程要等一次 Sync）。
	stale, err := eng.Reachable(ctx, q)
	if err != nil {
		t.Fatalf("Reachable stale: %v", err)
	}
	if strings.Join(stale.Nodes, ",") != "core,app,cli" {
		t.Errorf("未 Sync 时应返回旧答案 %v（缓存语义）", stale.Nodes)
	}
	// Sync → 增量加载新边 + 失效缓存 → 查询拿到新答案。
	if err := eng.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	fresh, err := eng.Reachable(ctx, q)
	if err != nil {
		t.Fatalf("Reachable fresh: %v", err)
	}
	if strings.Join(fresh.Nodes, ",") != "app,core,cli" {
		t.Errorf("Sync 后 Nodes = %v, want [app core cli]", fresh.Nodes)
	}
	if st := eng.Stats(); st.Edges != 5 {
		t.Errorf("Edges = %d, want 5", st.Edges)
	}
}

// TestReachableLimitsAndCancel：引擎层的上限与 context 取消。
func TestReachableLimitsAndCancel(t *testing.T) {
	db := openDB(t)
	depGraph(t, db, nil)
	eng, _ := newEngine(t, db, graph.Limits{})
	ctx := context.Background()

	// 结果上限：1 个就截断（core 的依赖者一跳有 app/cli 两个）。
	tr, err := eng.Reachable(ctx, graph.ReachQuery{Sources: []string{"util"}, Kind: "imports", Direction: graph.DirectionIn, MaxResults: 1})
	if err != nil {
		t.Fatalf("Reachable limit: %v", err)
	}
	if !tr.Truncated || len(tr.Nodes) != 1 {
		t.Errorf("Truncated=%v Nodes=%v, want true + 1 个", tr.Truncated, tr.Nodes)
	}
	// 深度上限：1 跳。
	d1, err := eng.Reachable(ctx, graph.ReachQuery{Sources: []string{"util"}, Kind: "imports", Direction: graph.DirectionIn, MaxDepth: 1})
	if err != nil {
		t.Fatalf("Reachable depth: %v", err)
	}
	if d1.Depth != 1 || strings.Join(d1.Nodes, ",") != "core" {
		t.Errorf("Depth=%d Nodes=%v, want 1 + [core]", d1.Depth, d1.Nodes)
	}
	// 取消：已取消的 ctx → 查询返回 context.Canceled。
	//
	// **分层说明（诚实口径）**：这一层断言的是用户可见契约；"取消必须由 BFS 自己发现"
	// 的隔离证明在 core/graph（TestMemGraphContextCancel 直接对 MemGraph.Reach 取消，
	// 不经任何库调用）。引擎这条路径上取消也可能先被库层发现——那也是正确行为。
	warm := graph.ReachQuery{Sources: []string{"util"}, Kind: "imports", Direction: graph.DirectionIn}
	if _, err := eng.Reachable(ctx, warm); err != nil {
		t.Fatalf("Reachable warm: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	other := graph.ReachQuery{Sources: []string{"cli"}, Kind: "imports", Direction: graph.DirectionIn}
	if _, err := eng.Reachable(cctx, other); !errors.Is(err, context.Canceled) {
		t.Errorf("取消后 err = %v, want context.Canceled", err)
	}
}

// TestEngineMemoryLimits：触到内存上界 → 查询 fail-close 报错（不返回不完整的影响面）。
func TestEngineMemoryLimits(t *testing.T) {
	db := openDB(t)
	depGraph(t, db, nil)
	// 只给 2 个节点/2 条边的额度（图里有 5 个节点 4 条边）。
	eng, _ := newEngine(t, db, graph.Limits{MaxNodes: 2, MaxEdges: 2})
	err := eng.Sync(context.Background())
	if !errors.Is(err, coregraph.ErrGraphFull) {
		t.Fatalf("Sync err = %v, want ErrGraphFull", err)
	}
	if _, err := eng.Reachable(context.Background(), graph.ReachQuery{Sources: []string{"app"}, Kind: "imports", Direction: graph.DirectionIn}); !errors.Is(err, coregraph.ErrGraphFull) {
		t.Errorf("Reachable err = %v, want ErrGraphFull", err)
	}
	// 一跳/两跳不受影响（直查库，恒完整）。
	if got, err := eng.Neighbors(context.Background(), graph.NeighborQuery{Node: "core", Kind: "imports", Direction: graph.DirectionIn, Hops: 1}); err != nil {
		t.Errorf("Neighbors err = %v, want nil（倒排直查不依赖内存图）", err)
	} else if strings.Join(got, ",") != "app,cli" {
		t.Errorf("Neighbors = %v, want [app cli]", got)
	}
}

// TestNoTableIsAnError：图投影表不存在 → 显式报错（不静默当空图）。
func TestNoTableIsAnError(t *testing.T) {
	db := openDB(t)
	eng, _ := newEngine(t, db, graph.Limits{})
	if _, err := eng.Neighbors(context.Background(), graph.NeighborQuery{Node: "app", Kind: "imports", Hops: 1}); err == nil {
		t.Error("表不存在时 Neighbors 应报错（不能返回空 = '没有影响面'）")
	}
	if _, err := eng.Reachable(context.Background(), graph.ReachQuery{Sources: []string{"app"}}); err == nil {
		t.Error("表不存在时 Reachable 应报错")
	}
}

// TestWriteFaceDropsInvalidEdges：空端点/空 kind 的边被写面丢弃（不猜默认值）。
func TestWriteFaceDropsInvalidEdges(t *testing.T) {
	db := openDB(t)
	seed(t, db, nil, 1,
		contracts.GraphEdge{Src: "", Dst: "b", Kind: "imports"},
		contracts.GraphEdge{Src: "a", Dst: "", Kind: "imports"},
		contracts.GraphEdge{Src: "a", Dst: "b", Kind: ""},
		edge("a", "b", "imports"),
		edge("a", "b", "imports"), // 重复：幂等
	)
	eng, _ := newEngine(t, db, graph.Limits{})
	if err := eng.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	st := eng.Stats()
	if st.Edges != 1 || st.Nodes != 2 {
		t.Errorf("Stats = %+v, want 1 条边 / 2 个节点", st)
	}
}

// TestCaseSensitiveNodeIDs：node id 在持久层也大小写敏感（陷阱 17，COLLATE BINARY）。
func TestCaseSensitiveNodeIDs(t *testing.T) {
	db := openDB(t)
	seed(t, db, nil, 1, edge("Foo", "Bar", "imports"), edge("foo", "bar", "imports"))
	eng, _ := newEngine(t, db, graph.Limits{})
	ctx := context.Background()
	got, err := eng.Neighbors(ctx, graph.NeighborQuery{Node: "Foo", Kind: "imports", Direction: graph.DirectionOut, Hops: 1})
	if err != nil {
		t.Fatalf("Neighbors: %v", err)
	}
	if strings.Join(got, ",") != "Bar" {
		t.Errorf("got %v, want [Bar]（foo→bar 是另一条边）", got)
	}
	if err := eng.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if st := eng.Stats(); st.Nodes != 4 || st.Edges != 2 {
		t.Errorf("Stats = %+v, want 4 节点 / 2 边", st)
	}
}

// ---------------------------------------------------------------------------
// 并发：查询与写入同时进行（engine 会被多个 HTTP handler 并发调用）
// ---------------------------------------------------------------------------

// TestEngineConcurrentQueriesAndWrites：并发查询与写入同时进行（engine 会被多个
// HTTP handler 并发调用）。
//
// **写侧只用一个 goroutine**：这不是偷懒，是钉住引擎增量水位依赖的那条纪律——
// 图投影的写者只有一个（G17：索引器是派生表唯一写者），且它按 **seq 升序、按批原子**
// 提交（applyBatch 在一个事务里写边 + 推水位）。所以库里可见的边集永远是 seq 的**前缀**，
// "WHERE seq > 水位" 才不会漏。若让多个写者各带任意 seq 并发提交，提交序 ≠ seq 序，
// 水位就会跳过后提交的小 seq——那种写法在生产里不存在（单索引器，G24④）。
func TestEngineConcurrentQueriesAndWrites(t *testing.T) {
	db := openDB(t)
	depGraph(t, db, nil)
	eng, _ := newEngine(t, db, graph.Limits{})
	proj := graph.NewProjection(eng, dbQuerier{db: db})
	ctx := context.Background()

	const writers, readers = 1, 8
	const perWriter = 20
	var wg sync.WaitGroup
	errCh := make(chan error, writers+readers)

	// 写：单写者、seq 严格升序（模拟索引器的投影事务）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < perWriter; i++ {
			src := "w" + itoa(i)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				errCh <- err
				return
			}
			if err := proj.ApplyEdges(ctx, graph.SQLTx(tx), int64(100+i), []contracts.GraphEdge{
				{Src: src, Dst: "core", Kind: "imports"},
			}); err != nil {
				_ = tx.Rollback()
				errCh <- err
				return
			}
			if err := tx.Commit(); err != nil {
				errCh <- err
				return
			}
			proj.Committed() // 索引器提交成功后也会调这一拍（G20）
		}
	}()
	// 读：并发查一跳（直查库）与多跳（内存图 + 闭包缓存）。
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if _, err := eng.Neighbors(ctx, graph.NeighborQuery{
					Node: "core", Kind: "imports", Direction: graph.DirectionIn, Hops: 1,
				}); err != nil {
					errCh <- err
					return
				}
				if _, err := eng.Reachable(ctx, graph.ReachQuery{
					Sources: []string{"util"}, Kind: "imports", Direction: graph.DirectionIn,
				}); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("并发期间出错: %v", err)
	}

	// 收尾查询：新写的边全都该看得见（core 的依赖者 = 原有 2 个 + 新写 20 个）。
	if err := eng.Sync(ctx); err != nil {
		t.Fatalf("final sync: %v", err)
	}
	got, err := eng.Neighbors(ctx, graph.NeighborQuery{Node: "core", Kind: "imports", Direction: graph.DirectionIn, Hops: 1})
	if err != nil {
		t.Fatalf("final neighbors: %v", err)
	}
	if len(got) != 2+perWriter {
		t.Errorf("core 的依赖者 = %d 个, want %d（并发写入不该丢边）", len(got), 2+perWriter)
	}
	if st := eng.Stats(); st.Edges != 4+perWriter {
		t.Errorf("Edges = %d, want %d", st.Edges, 4+perWriter)
	}
}

// itoa 是测试用的小整数转字符串（免一个 strconv import）。
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf []byte
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}

// ---------------------------------------------------------------------------
// F1：单事件超大批次的边**一条都不能丢**
// ---------------------------------------------------------------------------

// TestSingleEventManyEdgesAllLoaded：同一个事件产出 6000 条边（共用一个 seq）→
// 增量加载必须**全部**进图（F1 的钉子）。
//
// 为什么这是个坑：写面按事件落 seq（一个事件 = 一个 seq），增量加载按批拉
// （loadBatch=5000）。旧口径 `WHERE seq > 水位 LIMIT 5000` 会把这个 seq 组
// **切成两半**：第一批 5000 条、水位推到该 seq，第二批 `seq > 水位` 就把这组
// 剩下的 1000 条**永久丢掉**——查询看起来正常，影响面悄悄少报 1000 条边。
//
// 修法：水位改 (seq, rowid) 二元组，同 seq 组内按 rowid（写入顺序）继续推进。
func TestSingleEventManyEdgesAllLoaded(t *testing.T) {
	db := openDB(t)
	const n = 6000
	edges := make([]contracts.GraphEdge, 0, n)
	for i := 0; i < n; i++ {
		edges = append(edges, contracts.GraphEdge{Src: "n" + itoa(i), Dst: "hub", Kind: "imports"})
	}
	// **同一个 seq** = 同一个事件的 6000 条边。
	seed(t, db, nil, 7, edges...)

	eng, _ := newEngine(t, db, graph.Limits{})
	ctx := context.Background()
	if err := eng.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	st := eng.Stats()
	if st.Edges != n {
		t.Errorf("Edges = %d, want %d（单事件 %d 条边一条都不能丢）", st.Edges, n, n)
	}
	if st.Nodes != n+1 {
		t.Errorf("Nodes = %d, want %d", st.Nodes, n+1)
	}
	// 一跳倒排直查库：hub 的依赖者必须是全部 6000 个。
	got, err := eng.Neighbors(ctx, graph.NeighborQuery{Node: "hub", Kind: "imports", Direction: graph.DirectionIn, Hops: 1, Limit: n})
	if err != nil {
		t.Fatalf("Neighbors: %v", err)
	}
	if len(got) != n {
		t.Errorf("Neighbors = %d 个, want %d", len(got), n)
	}
}

// TestSingleEventManyEdgesMultiHop：同一事件的超大批次边在**多跳**查询里也完整。
func TestSingleEventManyEdgesMultiHop(t *testing.T) {
	db := openDB(t)
	const n = 6000
	edges := make([]contracts.GraphEdge, 0, n)
	for i := 0; i < n; i++ {
		edges = append(edges, contracts.GraphEdge{Src: "n" + itoa(i), Dst: "hub", Kind: "imports"})
	}
	seed(t, db, nil, 7, edges...)
	eng, _ := newEngine(t, db, graph.Limits{})
	ctx := context.Background()
	cl, err := eng.Reachable(ctx, graph.ReachQuery{Sources: []string{"hub"}, Kind: "imports", Direction: graph.DirectionIn, MaxResults: n})
	if err != nil {
		t.Fatalf("Reachable: %v", err)
	}
	if len(cl.Nodes) != n {
		t.Errorf("Nodes = %d, want %d（多跳也不能丢尾巴）", len(cl.Nodes), n)
	}
}

// TestNeighborsLimitNoOverflow：结果上限取到 maxInt 时**不能溢出**（N4）。
//
// 坑：判截断要"多取一行"（limit+1）。limit == maxInt 时 limit+1 溢出成负数 →
// SQLite 把负 LIMIT 当成"**无上限**" → 超级节点护栏被悄悄绕过（一次拉爆）。
//
// 钉子：LIMIT 参数必须为正；结果仍然正确（行数与库里的边一致）。
func TestNeighborsLimitNoOverflow(t *testing.T) {
	db := openDB(t)
	edges := make([]contracts.GraphEdge, 0, 5)
	for i := 0; i < 5; i++ {
		edges = append(edges, contracts.GraphEdge{Src: "n" + itoa(i), Dst: "hub", Kind: "imports"})
	}
	seed(t, db, nil, 1, edges...)
	eng, cq := newEngine(t, db, graph.Limits{})
	ctx := context.Background()

	res, err := eng.NeighborsResult(ctx, graph.NeighborQuery{
		Node: "hub", Kind: "imports", Direction: graph.DirectionIn, Hops: 1,
		Limit: int(^uint(0) >> 1), // maxInt：limit+1 会溢出
	})
	if err != nil {
		t.Fatalf("NeighborsResult: %v", err)
	}
	limitArg, ok := cq.lastLimitArg()
	if !ok {
		t.Fatal("没抓到 LIMIT 参数")
	}
	if limitArg <= 0 {
		t.Errorf("LIMIT 参数 = %d, want > 0（负 LIMIT 在 SQLite 里是'无上限'，护栏失效，N4）", limitArg)
	}
	if len(res.Nodes) != 5 {
		t.Errorf("Nodes = %d, want 5", len(res.Nodes))
	}
	// 溢出分支取不满上限 → 不该报截断（5 << maxInt）。
	if res.Truncated {
		t.Error("Truncated = true, want false（远没取满上限）")
	}
}

// ---------------------------------------------------------------------------
// F4：增量加载走 seq 索引（老库自动补建）
// ---------------------------------------------------------------------------

// TestSeqIndexExists：图投影表上必须有 seq 索引（F4）。
//
// 没有它，每次 Sync 都是全表扫 + TEMP B-TREE 排序（20 万边冷启动 ~1.4s）。
func TestSeqIndexExists(t *testing.T) {
	db := openDB(t)
	depGraph(t, db, nil)
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_graph_edges_seq'`).Scan(&n); err != nil {
		t.Fatalf("sqlite_master: %v", err)
	}
	if n != 1 {
		t.Errorf("idx_graph_edges_seq 不存在（F4：增量加载要靠它）")
	}
}

// TestSeqIndexMigratedOnOldDB：老库（没有 seq 索引的）重跑 EnsureSchema → 自动补上。
//
// 迁移纪律：独立 `CREATE INDEX IF NOT EXISTS`（幂等），不用写迁移脚本。
func TestSeqIndexMigratedOnOldDB(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	// 造一个"老库"：建表但**不建** seq 索引。
	oldSchema := `
CREATE TABLE IF NOT EXISTS graph_nodes (id TEXT PRIMARY KEY COLLATE BINARY);
CREATE TABLE IF NOT EXISTS graph_edges (
  src  TEXT    NOT NULL COLLATE BINARY,
  dst  TEXT    NOT NULL COLLATE BINARY,
  kind TEXT    NOT NULL COLLATE BINARY,
  seq  INTEGER NOT NULL,
  PRIMARY KEY (src, kind, dst)
);
CREATE INDEX IF NOT EXISTS idx_graph_edges_dst ON graph_edges(dst, kind);
CREATE TABLE IF NOT EXISTS graph_meta (id INTEGER PRIMARY KEY CHECK (id = 1), generation INTEGER NOT NULL);
INSERT OR IGNORE INTO graph_meta (id, generation) VALUES (1, 0);
`
	if _, err := db.ExecContext(ctx, oldSchema); err != nil {
		t.Fatalf("建老库 schema: %v", err)
	}
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_graph_edges_seq'`).Scan(&n); err != nil {
		t.Fatalf("sqlite_master: %v", err)
	}
	if n != 0 {
		t.Fatalf("前置条件失败：老库不该有 seq 索引")
	}
	// 走写面建表（幂等）→ 老库自动补上索引。
	proj := graph.NewProjection(nil, dbQuerier{db: db})
	if err := proj.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_graph_edges_seq'`).Scan(&n); err != nil {
		t.Fatalf("sqlite_master: %v", err)
	}
	if n != 1 {
		t.Errorf("老库重跑 EnsureSchema 后仍无 seq 索引（F4 的老库迁移）")
	}
}

// ---------------------------------------------------------------------------
// F12：邻居查询有结果上限 + 截断标记
// ---------------------------------------------------------------------------

// TestNeighborsLimitAndTruncated：超级节点一次拉爆 → 结果被上限截断并**显式标记**。
func TestNeighborsLimitAndTruncated(t *testing.T) {
	db := openDB(t)
	edges := make([]contracts.GraphEdge, 0, 50)
	for i := 0; i < 50; i++ {
		edges = append(edges, contracts.GraphEdge{Src: "n" + itoa(i), Dst: "hub", Kind: "imports"})
	}
	seed(t, db, nil, 1, edges...)
	eng, _ := newEngine(t, db, graph.Limits{})
	ctx := context.Background()

	// 显式上限 10 → 只回 10 个 + 截断标记。
	res, err := eng.NeighborsResult(ctx, graph.NeighborQuery{Node: "hub", Kind: "imports", Direction: graph.DirectionIn, Hops: 1, Limit: 10})
	if err != nil {
		t.Fatalf("NeighborsResult: %v", err)
	}
	if len(res.Nodes) != 10 {
		t.Errorf("Nodes = %d 个, want 10（上限生效）", len(res.Nodes))
	}
	if !res.Truncated {
		t.Error("Truncated = false, want true（命中上限必须标记）")
	}
	if res.Limit != 10 {
		t.Errorf("Limit = %d, want 10", res.Limit)
	}
	// 不截断时（结果数 < 上限）不置标记。
	res2, err := eng.NeighborsResult(ctx, graph.NeighborQuery{Node: "hub", Kind: "imports", Direction: graph.DirectionIn, Hops: 1, Limit: 100})
	if err != nil {
		t.Fatalf("NeighborsResult 2: %v", err)
	}
	if len(res2.Nodes) != 50 || res2.Truncated {
		t.Errorf("Nodes = %d / Truncated = %v, want 50 / false", len(res2.Nodes), res2.Truncated)
	}
	// 负值上限 → 拒绝。
	if _, err := eng.NeighborsResult(ctx, graph.NeighborQuery{Node: "hub", Hops: 1, Limit: -1}); !errors.Is(err, coregraph.ErrBadLimits) {
		t.Errorf("负上限 err = %v, want ErrBadLimits", err)
	}
	// 默认上限（0 = DefaultNeighborLimit）生效。
	def, err := eng.NeighborsResult(ctx, graph.NeighborQuery{Node: "hub", Hops: 1})
	if err != nil {
		t.Fatalf("NeighborsResult 默认: %v", err)
	}
	if def.Limit != coregraph.DefaultNeighborLimit {
		t.Errorf("默认 Limit = %d, want %d", def.Limit, coregraph.DefaultNeighborLimit)
	}
}
