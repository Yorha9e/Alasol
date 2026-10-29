package host_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	coregraph "github.com/Yorha9e/alasol/core/graph"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/graph"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	_ "github.com/ncruces/go-sqlite3/driver"
)

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

// newGraphHub 装配一个图投影相关配置的 Hub，并返回**事件库路径**（测试要能自己往里追事件）。
func newGraphHub(t *testing.T, muts ...func(*host.Config)) (*host.Hub, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.db")
	cfg := host.Config{
		DBPath:   path,
		Model:    "scripted",
		System:   "test-system",
		MaxTurns: 4,
		Allow:    []string{"read"},
		CWD:      tempRepoOrDir(t),
	}
	for _, mut := range muts {
		if mut != nil {
			mut(&cfg)
		}
	}
	h, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h, path
}

// toolEdgesEv 造一条带 graph_edges 的 tool.call.completed 事件（G22：边的来源 = 工具产出）。
func toolEdgesEv(t *testing.T, sid contracts.Ulid, edges []contracts.GraphEdge) contracts.Event {
	t.Helper()
	text := "ok"
	p := contracts.ToolCallCompletedPayload{
		ToolCallID: "toolu_HOST_01",
		Result: contracts.ToolResult{
			Content:    []contracts.ContentPart{{Type: "text", Text: &text}},
			GraphEdges: edges,
		},
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: "tool.call.completed",
		TS: "2026-01-29T10:00:00.000Z", SessionID: sid, AgentID: contracts.Ulid("TEECY7V57SP0J9WZ620EHHP02M"),
		Durable: true, Payload: raw,
	}
}

// appendEdges 用**真 store** 往事件库里追一条带边的工具完成事件，返回它的 seq。
func appendEdges(t *testing.T, path string, edges []contracts.GraphEdge) int64 {
	t.Helper()
	st, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	sid := contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")
	got, err := st.Append(context.Background(), []contracts.Event{toolEdgesEv(t, sid, edges)})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if len(got) != 1 || got[0].Seq == nil {
		t.Fatalf("append 结果异常: %+v", got)
	}
	return *got[0].Seq
}

// waitWatermark 轮询等 hub 的索引器追到 target（毫秒级；索引器 50ms 一轮）。
func waitWatermark(t *testing.T, h *host.Hub, target int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		wm, err := h.Watermark()
		if err != nil {
			t.Fatalf("watermark: %v", err)
		}
		if wm >= target {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("索引器 3s 内没追到 seq %d（当前 %d）", target, wm)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// 测试用的依赖图（kind "imports"）：app / cli → core → util。
var hostEdges = []contracts.GraphEdge{
	{Src: "app", Dst: "core", Kind: "imports"},
	{Src: "cli", Dst: "core", Kind: "imports"},
	{Src: "core", Dst: "util", Kind: "imports"},
}

// ---------------------------------------------------------------------------
// 酸测试：未启用图 = 零图开销（G2f）
// ---------------------------------------------------------------------------

// TestHubGraphDisabledZeroOverhead：默认不启用图 → 不建表 / 不建引擎 / 查询明确报错。
func TestHubGraphDisabledZeroOverhead(t *testing.T) {
	h, path := newGraphHub(t) // 不传 Graph=true
	ctx := context.Background()

	if h.GraphEnabled() {
		t.Error("GraphEnabled() = true, want false（默认不启用）")
	}
	if h.Graph() != nil {
		t.Error("Graph() 应为 nil（不建引擎、不驻留内存）")
	}
	if _, err := h.ChangeImpact(ctx, graph.ImpactRequest{Roots: []string{"core"}}); !errors.Is(err, coregraph.ErrGraphDisabled) {
		t.Errorf("ChangeImpact err = %v, want ErrGraphDisabled", err)
	}
	if _, err := h.GraphReachable(ctx, graph.ReachQuery{Sources: []string{"core"}}); !errors.Is(err, coregraph.ErrGraphDisabled) {
		t.Errorf("GraphReachable err = %v, want ErrGraphDisabled", err)
	}
	if _, err := h.GraphNeighbors(ctx, graph.NeighborQuery{Node: "core", Hops: 1}); !errors.Is(err, coregraph.ErrGraphDisabled) {
		t.Errorf("GraphNeighbors err = %v, want ErrGraphDisabled", err)
	}

	// HTTP：503 + 明确错误码（不返回空影响面）。
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/api/graph/impact?roots=core")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}

	// 库里没有图表（第二个连接直查 sqlite_master：不建表）。
	raw := openRawGraphDB(t, path)
	for _, tbl := range []string{"graph_nodes", "graph_edges", "graph_meta"} {
		var n int
		if err := raw.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM sqlite_master WHERE name = ?`, tbl).Scan(&n); err != nil {
			t.Fatalf("sqlite_master: %v", err)
		}
		if n != 0 {
			t.Errorf("未启用图却存在 %s", tbl)
		}
	}
}

// ---------------------------------------------------------------------------
// 启用后的端到端：工具边 → 索引器 → 查询 → HTTP
// ---------------------------------------------------------------------------

// TestHubGraphEnabledImpact：启用图 → change_impact 报告正确（含"触及核心接口"判定）。
func TestHubGraphEnabledImpact(t *testing.T) {
	h, path := newGraphHub(t, func(c *host.Config) {
		c.Graph = true
		c.GraphCoreNodes = []string{"core"}
		c.GraphEscalateReachable = 100
	})
	ctx := context.Background()
	if !h.GraphEnabled() {
		t.Fatal("GraphEnabled() = false, want true")
	}

	seq := appendEdges(t, path, hostEdges)
	waitWatermark(t, h, seq)

	// 一跳（入向）：谁依赖 core。
	in, err := h.GraphNeighbors(ctx, graph.NeighborQuery{Node: "core", Kind: "imports", Direction: graph.DirectionIn, Hops: 1})
	if err != nil {
		t.Fatalf("GraphNeighbors: %v", err)
	}
	if strings.Join(in, ",") != "app,cli" {
		t.Errorf("Neighbors = %v, want [app cli]", in)
	}
	// 一跳（出向）：core 依赖谁（两个方向必须分开——倒排方向错了这里就红）。
	out, err := h.GraphNeighbors(ctx, graph.NeighborQuery{Node: "core", Kind: "imports", Direction: graph.DirectionOut, Hops: 1})
	if err != nil {
		t.Fatalf("GraphNeighbors out: %v", err)
	}
	if strings.Join(out, ",") != "util" {
		t.Errorf("Neighbors out = %v, want [util]", out)
	}
	// 多跳：util 的全部被依赖者。
	cl, err := h.GraphReachable(ctx, graph.ReachQuery{Sources: []string{"util"}, Kind: "imports", Direction: graph.DirectionIn})
	if err != nil {
		t.Fatalf("GraphReachable: %v", err)
	}
	if strings.Join(cl.Nodes, ",") != "core,app,cli" {
		t.Errorf("Reachable = %v, want [core app cli]", cl.Nodes)
	}
	// change_impact：改 core 的影响面。
	rep, err := h.ChangeImpact(ctx, graph.ImpactRequest{
		Roots: []string{"core"},
		Kind:  "imports",
		Diff:  &graph.DiffStat{Files: 2, Added: 30, Deleted: 4},
	})
	if err != nil {
		t.Fatalf("ChangeImpact: %v", err)
	}
	if strings.Join(rep.Direct, ",") != "app,cli" {
		t.Errorf("Direct = %v, want [app cli]", rep.Direct)
	}
	if rep.ReachableCount != 2 {
		t.Errorf("ReachableCount = %d, want 2", rep.ReachableCount)
	}
	// 宿主配置的核心接口集生效：core 自身被命中 → 触及核心接口 → escalate。
	if strings.Join(rep.CoreTouched, ",") != "core" {
		t.Errorf("CoreTouched = %v, want [core]（宿主配置的核心接口集必须生效）", rep.CoreTouched)
	}
	if !rep.Escalate || rep.Scale != graph.ScaleLarge {
		t.Errorf("Escalate=%v Scale=%q, want true + large", rep.Escalate, rep.Scale)
	}
	if rep.Diff == nil || rep.Diff.Lines() != 34 {
		t.Errorf("Diff = %+v, want Lines()=34", rep.Diff)
	}
}

// TestHubGraphHTTPImpact：HTTP 端点的形状与错误码。
func TestHubGraphHTTPImpact(t *testing.T) {
	h, path := newGraphHub(t, func(c *host.Config) {
		c.Graph = true
		c.GraphCoreNodes = []string{"core"}
	})
	seq := appendEdges(t, path, hostEdges)
	waitWatermark(t, h, seq)

	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	// 200 + 报告 JSON。
	resp, err := http.Get(srv.URL + "/api/graph/impact?roots=core&kind=imports&diff_files=1&diff_added=10&diff_deleted=2")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	var rep graph.ImpactReport
	if err := json.NewDecoder(resp.Body).Decode(&rep); err != nil {
		resp.Body.Close()
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if strings.Join(rep.Direct, ",") != "app,cli" {
		t.Errorf("Direct = %v, want [app cli]", rep.Direct)
	}
	if rep.Diff == nil || rep.Diff.Lines() != 12 {
		t.Errorf("Diff = %+v, want 12 行", rep.Diff)
	}

	// 400：缺 roots。
	resp, err = http.Get(srv.URL + "/api/graph/impact")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("缺 roots status = %d, want 400", resp.StatusCode)
	}
	// 400：depth 非法。
	resp, err = http.Get(srv.URL + "/api/graph/impact?roots=core&depth=abc")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("坏 depth status = %d, want 400", resp.StatusCode)
	}
}

// TestHubGraphHTTPImpactDisabled：未启用 → 503（明说，不返回空影响面）。
func TestHubGraphHTTPImpactDisabled(t *testing.T) {
	h, _ := newGraphHub(t)
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/api/graph/impact?roots=core")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

// TestHubGraphConfigValidation：图相关配置的负值在校验期就被拒（不带到运行期）。
func TestHubGraphConfigValidation(t *testing.T) {
	base := func() host.Config {
		return host.Config{DBPath: filepath.Join(t.TempDir(), "x.db"), Model: "m", Addr: "127.0.0.1:1"}
	}
	for _, mut := range []func(*host.Config){
		func(c *host.Config) { c.GraphMaxNodes = -1 },
		func(c *host.Config) { c.GraphMaxEdges = -1 },
		func(c *host.Config) { c.GraphMaxDepth = -1 },
		func(c *host.Config) { c.GraphMaxResults = -1 },
		func(c *host.Config) { c.GraphEscalateReachable = -1 },
	} {
		cfg := base()
		mut(&cfg)
		h, err := host.New(cfg)
		if err == nil {
			_ = h.Close()
			t.Error("负值配置应被拒绝")
		}
	}
	// 0 = 默认：合法。
	cfg := base()
	cfg.Graph = true
	h, err := host.New(cfg)
	if err != nil {
		t.Errorf("0 值（默认）应合法: %v", err)
	} else {
		t.Cleanup(func() { _ = h.Close() })
	}
}

// TestHubGraphConfigDepthResultsUsed：宿主配置的深度/结果上限**真的生效**（F5）。
//
// 旋钮不许是死的：-graph-max-depth / -graph-max-results 配了就得被用上。
// 这里配 depth=1（只看一跳）+ results=1（结果上限 1 → 截断）。
func TestHubGraphConfigDepthResultsUsed(t *testing.T) {
	h, path := newGraphHub(t, func(c *host.Config) {
		c.Graph = true
		c.GraphMaxDepth = 1
		c.GraphMaxResults = 1
	})
	ctx := context.Background()
	seq := appendEdges(t, path, hostEdges)
	waitWatermark(t, h, seq)

	rep, err := h.ChangeImpact(ctx, graph.ImpactRequest{Roots: []string{"util"}, Kind: "imports"})
	if err != nil {
		t.Fatalf("ChangeImpact: %v", err)
	}
	// 深度上限 1 被回显（= 配置值，不是框架默认 8）。
	if rep.MaxDepth != 1 {
		t.Errorf("MaxDepth = %d, want 1（宿主配置必须生效，F5）", rep.MaxDepth)
	}
	// 结果上限 1 → 只回一个节点 + 截断标记（影响面不完整要如实说）。
	if rep.ReachableCount != 1 {
		t.Errorf("ReachableCount = %d, want 1（结果上限 1 生效）", rep.ReachableCount)
	}
	if !rep.Truncated {
		t.Error("Truncated = false, want true（命中结果上限）")
	}
	// 请求里显式给的上限**优先于**配置（请求级覆盖 > 配置）。
	rep2, err := h.ChangeImpact(ctx, graph.ImpactRequest{Roots: []string{"util"}, Kind: "imports", MaxDepth: 3, MaxResults: 100})
	if err != nil {
		t.Fatalf("ChangeImpact 2: %v", err)
	}
	if rep2.MaxDepth != 3 {
		t.Errorf("MaxDepth = %d, want 3（请求级覆盖优先）", rep2.MaxDepth)
	}
	if rep2.Truncated {
		t.Error("Truncated = true, want false（结果上限 100 够用）")
	}
}

// TestHubGraphReachableSyncOnce：宿主每次请求**只同步一次**（F13）。
//
// 口径：缓存命中时**零 SQL**（G20）——所以第二次同样的查询不该再触发一次 Sync。
// 旧口径：宿主显式 Sync 一次 + 引擎内部 Sync 一次 → 每次请求都打库。
func TestHubGraphReachableSyncOnce(t *testing.T) {
	h, path := newGraphHub(t, func(c *host.Config) { c.Graph = true })
	ctx := context.Background()
	seq := appendEdges(t, path, hostEdges)
	waitWatermark(t, h, seq)

	q := graph.ReachQuery{Sources: []string{"util"}, Kind: "imports", Direction: graph.DirectionIn}
	if _, err := h.GraphReachable(ctx, q); err != nil {
		t.Fatalf("GraphReachable 1: %v", err)
	}
	st1, err := h.GraphStats()
	if err != nil {
		t.Fatalf("GraphStats: %v", err)
	}
	if _, err := h.GraphReachable(ctx, q); err != nil {
		t.Fatalf("GraphReachable 2: %v", err)
	}
	st2, err := h.GraphStats()
	if err != nil {
		t.Fatalf("GraphStats 2: %v", err)
	}
	if st2.Syncs != st1.Syncs {
		t.Errorf("第二次查询又同步了一次（Syncs %d → %d）：缓存命中不该落库（F13/G20）", st1.Syncs, st2.Syncs)
	}
	if st2.CacheHits == 0 {
		t.Error("第二次查询应命中闭包缓存")
	}
}

// openRawGraphDB 用第二个连接打开同一个事件库（测试里直查 sqlite_master 用）。
func openRawGraphDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	db, err := sql.Open("sqlite3", "file:"+filepath.ToSlash(abs)+"?_pragma=busy_timeout%3d5000")
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping raw: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
