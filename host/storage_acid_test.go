package host_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	_ "github.com/ncruces/go-sqlite3/driver"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	coregraph "github.com/Yorha9e/alasol/core/graph"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/runtime/graph"
	"github.com/Yorha9e/alasol/runtime/indexer"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// 对齐核对（v2-S5 项 2）：SQLite 图实现确实落在 core/graph 的接口面上，
// 且经 core/storage 的族别名出口仍然成立（别名不制造第二份定义，F7）。
var (
	_ storage.Graph           = (*graph.Engine)(nil)
	_ storage.GraphProjection = (*graph.Projection)(nil)
	_ storage.EventLog        = (*sqlite.Store)(nil)
	_ coregraph.BoundedGraph  = (*graph.Engine)(nil)
	_ coregraph.Invalidator   = (*graph.Engine)(nil)
)

// 本文件是 G1/G2 的**换实现零改动酸测试（存储侧）**：同一份消费者代码（只认接口）跑在
//   - SQLite 实现（runtime/graph.Engine：node/edge 表持久化 + 内存图多跳，G2c）；
//   - 内存实现（core/graph.MemGraph 适配：纯内存，无任何持久化）；
//
// 两套实现的答案逐字段一致 → 消费者（host / 索引器 / v3 escalate 闸门）换存储引擎时
// **一行不改**。core/storage 的 compose_test.go 覆盖各族接口面的组合；本文件补上
// "真 SQLite 实现也在可换集合里"这一环（core 包不许 import runtime，故酸测试落在这里）。

// memGraphEngine 是 core/graph.MemGraph 的最小 Graph 适配（**纯内存**实现）。
//
// 与 runtime/graph.Engine 的内部结构完全不同（无 SQL、无倒排索引、无增量水位）——
// 存在的理由是当"另一个实现"，不是当产品实现。
type memGraphEngine struct {
	g   *coregraph.MemGraph
	out map[string][]contracts.GraphEdge // src → 出边
	in  map[string][]contracts.GraphEdge // dst → 入边
}

func newMemGraphEngine(t *testing.T) *memGraphEngine {
	t.Helper()
	g, err := coregraph.NewMemGraph(coregraph.DefaultLimits())
	if err != nil {
		t.Fatalf("NewMemGraph: %v", err)
	}
	return &memGraphEngine{g: g, out: map[string][]contracts.GraphEdge{}, in: map[string][]contracts.GraphEdge{}}
}

func (m *memGraphEngine) ApplyEdges(_ context.Context, _ coregraph.Tx, _ int64, edges []contracts.GraphEdge) error {
	for _, e := range edges {
		if err := m.g.AddEdge(e.Src, e.Dst, e.Kind); err != nil {
			return err
		}
		m.out[e.Src] = append(m.out[e.Src], e)
		m.in[e.Dst] = append(m.in[e.Dst], e)
	}
	return nil
}

func (m *memGraphEngine) EnsureSchema(context.Context) error { return nil }
func (m *memGraphEngine) Clear(_ context.Context, _ coregraph.Tx) error {
	m.g.Reset()
	m.out, m.in = map[string][]contracts.GraphEdge{}, map[string][]contracts.GraphEdge{}
	return nil
}
func (m *memGraphEngine) Committed() {}

func (m *memGraphEngine) Neighbors(_ context.Context, q coregraph.NeighborQuery) ([]string, error) {
	idx := m.out
	if q.Direction == coregraph.DirectionIn {
		idx = m.in
	}
	level := []string{q.Node}
	seen := map[string]bool{q.Node: true}
	for hop := 0; hop < q.Hops; hop++ {
		var next []string
		for _, n := range level {
			for _, e := range idx[n] {
				if q.Kind != "" && e.Kind != q.Kind {
					continue
				}
				far := e.Dst
				if q.Direction == coregraph.DirectionIn {
					far = e.Src
				}
				if seen[far] {
					continue
				}
				seen[far] = true
				next = append(next, far)
			}
		}
		level = next
	}
	sort.Strings(level)
	return level, nil
}

func (m *memGraphEngine) Reachable(ctx context.Context, q coregraph.ReachQuery) (coregraph.Closure, error) {
	depth, maxResults := q.MaxDepth, q.MaxResults
	if depth <= 0 {
		depth = coregraph.DefaultMaxDepth
	}
	if maxResults <= 0 {
		maxResults = coregraph.DefaultMaxResults
	}
	return m.g.Reach(ctx, q.Sources, q.Kind, q.Direction, depth, maxResults)
}

func (m *memGraphEngine) Impact(ctx context.Context, req coregraph.ImpactRequest) (coregraph.ImpactReport, error) {
	c, err := m.Reachable(ctx, coregraph.ReachQuery{
		Sources: req.Roots, Kind: req.Kind, Direction: coregraph.DirectionIn,
		MaxDepth: req.MaxDepth, MaxResults: req.MaxResults,
	})
	if err != nil {
		return coregraph.ImpactReport{}, err
	}
	return coregraph.AssembleImpact(req, c), nil
}

func (m *memGraphEngine) Sync(context.Context) error { return nil }
func (m *memGraphEngine) Stats() coregraph.Stats {
	return coregraph.Stats{Nodes: m.g.NodeCount(), Edges: m.g.EdgeCount()}
}

// impactConsumer 是**只认 coregraph.Graph** 的消费者（host 的 /api/graph/impact 与
// v3 escalate 闸门的缩影）。它在两次运行之间一字不改——它就是被测对象。
func impactConsumer(ctx context.Context, g coregraph.Graph) (string, error) {
	in1, err := g.Neighbors(ctx, coregraph.NeighborQuery{
		Node: "core/store.go", Kind: "imports", Direction: coregraph.DirectionIn, Hops: 1,
	})
	if err != nil {
		return "", err
	}
	in2, err := g.Neighbors(ctx, coregraph.NeighborQuery{
		Node: "runtime/store/sqlite", Kind: "imports", Direction: coregraph.DirectionIn, Hops: 2,
	})
	if err != nil {
		return "", err
	}
	rep, err := g.Impact(ctx, coregraph.ImpactRequest{
		Roots: []string{"core/store.go"}, Kind: "imports", MaxDepth: 4,
		Core: coregraph.ParseCoreSet([]string{"core/*"}),
	})
	if err != nil {
		return "", err
	}
	parts := []string{
		"in1=" + strings.Join(in1, ","),
		"in2=" + strings.Join(in2, ","),
		"direct=" + strings.Join(rep.Direct, ","),
		"transitive=" + strings.Join(rep.Transitive, ","),
		"core=" + strings.Join(rep.CoreTouched, ","),
		"escalate=" + boolStr(rep.Escalate),
		"reachable=" + strconv.Itoa(rep.ReachableCount),
		"direction=" + rep.Direction.String(),
	}
	return strings.Join(parts, "|"), nil
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// TestSwapGraphImplementationZeroConsumerChange：SQLite 实现 ↔ 内存实现，消费者零改动。
func TestSwapGraphImplementationZeroConsumerChange(t *testing.T) {
	ctx := context.Background()
	edges := []contracts.GraphEdge{
		{Src: "host/hub.go", Dst: "core/store.go", Kind: "imports"},
		{Src: "core/loop/loop.go", Dst: "core/store.go", Kind: "imports"},
		{Src: "cmd/alasol/main.go", Dst: "host/hub.go", Kind: "imports"},
		{Src: "runtime/store/sqlite", Dst: "core/store.go", Kind: "imports"},
	}

	// 实现 A：SQLite（node/edge 表 + 索引器建边 + 读引擎）。
	path := filepath.Join(t.TempDir(), "swap.db")
	st, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	eng, err := sqlite.NewGraphEngine(st, graph.Limits{})
	if err != nil {
		t.Fatalf("NewGraphEngine: %v", err)
	}
	proj, err := sqlite.NewGraphProjection(st, eng)
	if err != nil {
		t.Fatalf("NewGraphProjection: %v", err)
	}
	ix, err := sqlite.NewIndexer(st, 0, indexer.WithGraphProjection(proj))
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	// 边经事件流进来（G22：工具产出 → tool.call.completed → 索引器建边）。
	sid := contracts.Ulid(core.NewUlid())
	text := "ok"
	payload, err := json.Marshal(contracts.ToolCallCompletedPayload{
		ToolCallID: "toolu_01SWAP",
		Result:     contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &text}}, GraphEdges: edges},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := st.Append(ctx, []contracts.Event{{
		V: 1, ID: core.NewUlid(), Kind: "tool.call.completed", TS: "2026-01-29T10:00:00.000Z",
		SessionID: sid, AgentID: contracts.Ulid(core.NewUlid()), Durable: true, Payload: payload,
	}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	for {
		n, err := ix.Step(ctx)
		if err != nil {
			t.Fatalf("indexer step: %v", err)
		}
		if n == 0 {
			break
		}
	}
	if err := eng.Sync(ctx); err != nil {
		t.Fatalf("engine sync: %v", err)
	}

	// 实现 B：纯内存（MemGraph 适配）。
	mem := newMemGraphEngine(t)
	if err := mem.ApplyEdges(ctx, &nopTx{}, 1, edges); err != nil {
		t.Fatalf("mem ApplyEdges: %v", err)
	}

	gotSQLite, err := impactConsumer(ctx, eng)
	if err != nil {
		t.Fatalf("SQLite 实现: %v", err)
	}
	gotMem, err := impactConsumer(ctx, mem)
	if err != nil {
		t.Fatalf("内存实现: %v", err)
	}
	if gotSQLite != gotMem {
		t.Fatalf("换图实现后消费者结论变了（零改动酸测试失败）:\nsqlite=%s\nmem   =%s", gotSQLite, gotMem)
	}
	// 顺带钉住答案本身（防两个实现一起错）：改 core/store.go 的直接下游是两个一跳 + 两个两跳外的传递下游。
	if !strings.Contains(gotSQLite, "in1=core/loop/loop.go,host/hub.go,runtime/store/sqlite") {
		t.Fatalf("一跳下游不对: %s", gotSQLite)
	}
	if !strings.Contains(gotSQLite, "direct=core/loop/loop.go,host/hub.go,runtime/store/sqlite") {
		t.Fatalf("直接影响面不对: %s", gotSQLite)
	}
	if !strings.Contains(gotSQLite, "transitive=cmd/alasol/main.go") {
		t.Fatalf("传递影响面不对: %s", gotSQLite)
	}
	if !strings.Contains(gotSQLite, "core=core/loop/loop.go,core/store.go") || !strings.Contains(gotSQLite, "escalate=true") {
		t.Fatalf("触及核心接口应 escalate: %s", gotSQLite)
	}
}

// nopTx 是内存实现用的最小 Tx（图写入不走事务——纯内存没有"一批要么全成要么全不成"的
// 中间态；ApplyEdges 内部是逐个 add，任一失败即返回错误、由调用方决定是否 Clear）。
type nopTx struct{}

func (nopTx) Exec(context.Context, string, ...any) error { return nil }
