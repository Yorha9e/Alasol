package graph_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/ncruces/go-sqlite3/driver"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/graph"
	"github.com/Yorha9e/alasol/runtime/indexer"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// ---------------------------------------------------------------------------
// G22 端到端：工具产出边 → 事件落盘 → 索引器建边 → 查询可见
// ---------------------------------------------------------------------------

// toolCompletedEv 造一条 tool.call.completed 事件，返回值里带 graph_edges（G22）。
func toolCompletedEv(t *testing.T, sid contracts.Ulid, edges []contracts.GraphEdge) contracts.Event {
	t.Helper()
	text := "ok"
	p := contracts.ToolCallCompletedPayload{
		ToolCallID: "toolu_01ABC", // provider 黑盒 id 原样（S4/S5）
		Result: contracts.ToolResult{
			Content:    []contracts.ContentPart{{Type: "text", Text: &text}},
			GraphEdges: edges,
		},
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: "tool.call.completed",
		TS: "2026-01-29T10:00:00.000Z", SessionID: sid, AgentID: contracts.Ulid("TEECY7V57SP0J9WZ620EHHP02M"),
		Durable: true, Payload: raw,
	}
}

// openGraphHub 开一个"图投影已启用"的最小装配：事件库 + 索引器（带图写面）+ 读引擎。
func openGraphHub(t *testing.T, path string, limits graph.Limits) (*sqlite.Store, *indexer.Indexer, *graph.Engine) {
	t.Helper()
	st, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	eng, err := sqlite.NewGraphEngine(st, limits)
	if err != nil {
		t.Fatalf("new graph engine: %v", err)
	}
	// 写面**自建表**（F6）：DDL 归实现侧 → 写面要自己的执行句柄（存储实现给）。
	proj, err := sqlite.NewGraphProjection(st, eng)
	if err != nil {
		t.Fatalf("new graph projection: %v", err)
	}
	ix, err := sqlite.NewIndexer(st, 0, indexer.WithGraphProjection(proj))
	if err != nil {
		t.Fatalf("new indexer: %v", err)
	}
	return st, ix, eng
}

// TestG22ChainToolEdgesToQuery：端到端（G22 定案 A）。
//
//	工具返回带 graph_edges → tool.call.completed 落盘 → 索引器建边 → 一跳/两跳/多跳/
//	change_impact 全都看得见。
func TestG22ChainToolEdgesToQuery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "g22.db")
	st, ix, eng := openGraphHub(t, path, graph.Limits{})
	ctx := context.Background()
	sid := contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")

	// 工具产出：app 依赖 core；cli 也依赖 core；core 依赖 util。
	edges := []contracts.GraphEdge{
		{Src: "app", Dst: "core", Kind: "imports"},
		{Src: "cli", Dst: "core", Kind: "imports"},
		{Src: "core", Dst: "util", Kind: "imports"},
	}
	if _, err := st.Append(ctx, []contracts.Event{toolCompletedEv(t, sid, edges)}); err != nil {
		t.Fatalf("append: %v", err)
	}
	// 索引器消费事件流 → 建边。
	for {
		n, err := ix.Step(ctx)
		if err != nil {
			t.Fatalf("step: %v", err)
		}
		if n == 0 {
			break
		}
	}

	// 一跳（入向）：谁依赖 core。
	in, err := eng.Neighbors(ctx, graph.NeighborQuery{Node: "core", Kind: "imports", Direction: graph.DirectionIn, Hops: 1})
	if err != nil {
		t.Fatalf("Neighbors: %v", err)
	}
	if strings.Join(in, ",") != "app,cli" {
		t.Errorf("Neighbors in = %v, want [app cli]", in)
	}
	// 两跳（入向）：谁间接依赖 util。
	in2, err := eng.Neighbors(ctx, graph.NeighborQuery{Node: "util", Kind: "imports", Direction: graph.DirectionIn, Hops: 2})
	if err != nil {
		t.Fatalf("Neighbors 2: %v", err)
	}
	if strings.Join(in2, ",") != "app,cli" {
		t.Errorf("Neighbors 2 in = %v, want [app cli]", in2)
	}
	// 多跳（内存图 BFS）：util 的全部被依赖者。
	cl, err := eng.Reachable(ctx, graph.ReachQuery{Sources: []string{"util"}, Kind: "imports", Direction: graph.DirectionIn})
	if err != nil {
		t.Fatalf("Reachable: %v", err)
	}
	if strings.Join(cl.Nodes, ",") != "core,app,cli" {
		t.Errorf("Reachable = %v, want [core app cli]", cl.Nodes)
	}
	// change_impact：改 core 的影响面。
	rep, err := eng.Impact(ctx, graph.ImpactRequest{
		Roots:      []string{"core"},
		Kind:       "imports",
		Core:       graph.ParseCoreSet([]string{"core"}),
		Thresholds: graph.Thresholds{MaxReachable: 100, MaxDirect: 100, MaxDiffLines: 100000},
	})
	if err != nil {
		t.Fatalf("Impact: %v", err)
	}
	if strings.Join(rep.Direct, ",") != "app,cli" {
		t.Errorf("Direct = %v, want [app cli]", rep.Direct)
	}
	if rep.ReachableCount != 2 {
		t.Errorf("ReachableCount = %d, want 2", rep.ReachableCount)
	}
	// core 自身被声明为核心接口 → 触及（默认阈值 0 → escalate）。
	if strings.Join(rep.CoreTouched, ",") != "core" || !rep.Escalate {
		t.Errorf("CoreTouched=%v Escalate=%v, want [core] + true", rep.CoreTouched, rep.Escalate)
	}
	// 报告可序列化（喂给 v3 escalate 消费端的形状）。
	if _, err := json.Marshal(rep); err != nil {
		t.Errorf("marshal report: %v", err)
	}
}

// TestGraphDisabledNoTables（酸测试，G2f）：未启用图 → **不建表、不加载、不驻留**。
func TestGraphDisabledNoTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "off.db")
	st, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	// **不传** WithGraphProjection = 图投影未启用。
	ix, err := sqlite.NewIndexer(st, 0)
	if err != nil {
		t.Fatalf("new indexer: %v", err)
	}
	ctx := context.Background()
	sid := contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")
	if _, err := st.Append(ctx, []contracts.Event{toolCompletedEv(t, sid, []contracts.GraphEdge{
		{Src: "app", Dst: "core", Kind: "imports"},
	})}); err != nil {
		t.Fatalf("append: %v", err)
	}
	for {
		n, err := ix.Step(ctx)
		if err != nil {
			t.Fatalf("step: %v", err)
		}
		if n == 0 {
			break
		}
	}

	// ① 库里**没有** graph_nodes / graph_edges 表（不建表）。
	raw := openRaw(t, path)
	for _, tbl := range []string{"graph_nodes", "graph_edges", "idx_graph_edges_dst"} {
		var n int
		if err := raw.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table','index') AND name = ?`, tbl).Scan(&n); err != nil {
			t.Fatalf("sqlite_master: %v", err)
		}
		if n != 0 {
			t.Errorf("未启用图却存在 %s（G2f 酸测试：不建表）", tbl)
		}
	}
	// ② 即便硬建一个引擎去查，也**显式报错**而不是返回"空图"——
	// "没有影响面"对 escalate 闸门是危险答案。
	eng, err := sqlite.NewGraphEngine(st, graph.Limits{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if _, err := eng.Neighbors(ctx, graph.NeighborQuery{Node: "app", Kind: "imports", Hops: 1}); err == nil {
		t.Error("表不存在时应报错（不能静默当空图）")
	}
	if st := eng.Stats(); st.Nodes != 0 || st.Edges != 0 || st.Syncs != 0 {
		t.Errorf("未启用图时引擎不该有任何加载动作：%+v", st)
	}
}

// TestRebuildClearsGraph：Rebuild 连图投影一起清空（G24②）。
func TestRebuildClearsGraph(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rebuild.db")
	st, ix, eng := openGraphHub(t, path, graph.Limits{})
	ctx := context.Background()
	sid := contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")
	if _, err := st.Append(ctx, []contracts.Event{toolCompletedEv(t, sid, []contracts.GraphEdge{
		{Src: "app", Dst: "core", Kind: "imports"},
	})}); err != nil {
		t.Fatalf("append: %v", err)
	}
	for {
		n, err := ix.Step(ctx)
		if err != nil {
			t.Fatalf("step: %v", err)
		}
		if n == 0 {
			break
		}
	}
	// 让引擎加载一次（多跳查询/Sync 才会碰持久层）。
	if err := eng.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if st := eng.Stats(); st.Edges != 1 {
		t.Fatalf("Edges = %d, want 1", st.Edges)
	}
	if err := ix.Rebuild(ctx); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	// 事件还在（Rebuild 只清派生表）→ 重放后边又回来；引擎靠**重建代**发现投影被清过，
	// 下次 Sync 丢掉旧内存图从 0 重载。
	if err := eng.Sync(ctx); err != nil {
		t.Fatalf("sync after rebuild: %v", err)
	}
	if st := eng.Stats(); st.Edges != 1 {
		t.Errorf("Rebuild 后重放 Edges = %d, want 1（事件流是唯一事实源）", st.Edges)
	}
	// 再手动清一次表并确认引擎看得到（代数变了就该重载成空）。
	raw := openRaw(t, path)
	tx, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM graph_edges; DELETE FROM graph_nodes;
		UPDATE graph_meta SET generation = generation + 1 WHERE id = 1;`); err != nil {
		t.Fatalf("manual clear: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := eng.Sync(ctx); err != nil {
		t.Fatalf("sync after manual clear: %v", err)
	}
	if st := eng.Stats(); st.Edges != 0 || st.Nodes != 0 {
		t.Errorf("清空后 Stats = %+v, want 0/0", st)
	}
}

// TestGraphBadPayloadIsolated：tool.call.completed 载荷坏 → 隔离不致命（P1-2），
// 索引器继续跑，坏载荷计数进 cost_stats 独立列。
func TestGraphBadPayloadIsolated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.db")
	st, ix, eng := openGraphHub(t, path, graph.Limits{})
	ctx := context.Background()
	sid := contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")

	bad := contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: "tool.call.completed",
		TS: "2026-01-29T10:00:00.000Z", SessionID: sid, AgentID: contracts.Ulid("TEECY7V57SP0J9WZ620EHHP02M"),
		Durable: true, Payload: json.RawMessage(`{"tool_call_id":`), // 截断的 JSON
	}
	good := toolCompletedEv(t, sid, []contracts.GraphEdge{{Src: "app", Dst: "core", Kind: "imports"}})
	if _, err := st.Append(ctx, []contracts.Event{bad, good}); err != nil {
		t.Fatalf("append: %v", err)
	}
	for {
		n, err := ix.Step(ctx)
		if err != nil {
			t.Fatalf("step 不该因坏载荷失败: %v", err)
		}
		if n == 0 {
			break
		}
	}
	// 坏载荷被隔离计数，好事件的边照常入图。
	cost, ok, err := ix.ReadCostStats(ctx, sid)
	if err != nil || !ok {
		t.Fatalf("read cost: %v ok=%v", err, ok)
	}
	if cost.BadPayloadCount != 1 {
		t.Errorf("BadPayloadCount = %d, want 1", cost.BadPayloadCount)
	}
	if err := eng.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if st := eng.Stats(); st.Edges != 1 {
		t.Errorf("Edges = %d, want 1（坏载荷不影响好事件）", st.Edges)
	}
}

// TestRebuildInvalidatesClosureCache：Rebuild 之后闭包缓存**必须失效**（F14）。
//
// Rebuild 清空了图投影 → 读侧引擎的缓存里还温着"重建前"的闭包 →
// 不发失效通知的话，下一次查询会拿旧答案当新答案（影响面少报 → escalate 闸门被蒙）。
//
// 口径：Rebuild 之后缓存条目数归零（写面 Clear 之后必须调 Committed()）。
func TestRebuildInvalidatesClosureCache(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rebuildcache.db")
	st, ix, eng := openGraphHub(t, path, graph.Limits{})
	ctx := context.Background()
	sid := contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")
	if _, err := st.Append(ctx, []contracts.Event{toolCompletedEv(t, sid, []contracts.GraphEdge{
		{Src: "app", Dst: "core", Kind: "imports"},
	})}); err != nil {
		t.Fatalf("append: %v", err)
	}
	for {
		n, err := ix.Step(ctx)
		if err != nil {
			t.Fatalf("step: %v", err)
		}
		if n == 0 {
			break
		}
	}
	// 先查一次 → 闭包进缓存。
	if _, err := eng.Reachable(ctx, graph.ReachQuery{Sources: []string{"core"}, Kind: "imports", Direction: graph.DirectionIn}); err != nil {
		t.Fatalf("reachable: %v", err)
	}
	if st := eng.Stats(); st.CacheEntries == 0 {
		t.Fatalf("前置条件失败：闭包应已进缓存（%+v）", st)
	}
	// Rebuild → 写面 Clear 后必须发 Committed() → 读侧缓存失效。
	if err := ix.Rebuild(ctx); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if st := eng.Stats(); st.CacheEntries != 0 {
		t.Errorf("Rebuild 后缓存条目 = %d, want 0（F14：重建必须发失效通知）", st.CacheEntries)
	}
	// 再查一次：走重算（miss），答案仍然正确（事件流是唯一事实源）。
	cl, err := eng.Reachable(ctx, graph.ReachQuery{Sources: []string{"core"}, Kind: "imports", Direction: graph.DirectionIn})
	if err != nil {
		t.Fatalf("reachable after rebuild: %v", err)
	}
	if strings.Join(cl.Nodes, ",") != "app" {
		t.Errorf("Nodes = %v, want [app]", cl.Nodes)
	}
}

// openRaw 用第二个连接打开同一个文件库（测试里要能查 sqlite_master）。
func openRaw(t *testing.T, path string) *sql.DB {
	t.Helper()
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	dsn := "file:" + filepath.ToSlash(abs) + "?_pragma=busy_timeout%3d5000&_txlock=immediate"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping raw: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
