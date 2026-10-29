package host

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	coregraph "github.com/Yorha9e/alasol/core/graph"
	"github.com/Yorha9e/alasol/runtime/graph"
)

// 本文件把 v2-S2 图投影（G2f/G18/G19/G20/G21）装配进 hub。
//
// **图是可选投影，不进 core 必经路径**（G2f）：Config.Graph 为 false（默认）时
//   - 不建图查询引擎（不驻留内存）、不建 graph_nodes/graph_edges 表、
//     索引器连 tool.call.completed 的载荷都不解析 —— **零图开销**（酸测试）；
// 为 true 时：索引器多写两张派生表（写面 graph.Projection），hub 多持一个读引擎
// （graph.Engine），并经 HTTP 暴露 change_impact 影响面报告（G21/W16）。
//
// # change_impact 的入口形态：core 能力接口 + host 装配（不是框架工具）
//
// 理由（三条，写在这里免得后人改成工具再返工）：
//  1. **它是只读查询，没有副作用**。X1 三段写路径（intent → 执行 → result）是为
//     "有副作用的操作"设计的：intent 先落盘才能审计/恢复副作用。给一次纯查询落两条
//     durable 事件 = 污染事件流 + 污染成本统计，还违反 G7"投影只读、不参与写路径"。
//  2. **工具可见性是 R5 的决策，归 v3 工作流引擎**（哪些 agent_type 看得见
//     change_impact 是工作流的事）。本刀先落"能力 + 装配面"，v3 若要暴露成工具，
//     包一个薄 core.ToolExecutor 调 Hub.ChangeImpact 即可，接口不用改。
//  3. **消费端是 escalate 闸门**（W16/W17：框架提供客观依据，agent/工作流判断）。
//     报告是纯数据 + JSON tag，可直接喂给它。
//
// 因此本刀的对外形态：Go API（Hub.ChangeImpact / Hub.GraphReachable /
// Hub.GraphNeighbors）+ 一个 HTTP 端点 GET /api/graph/impact。

// graphLimits 把宿主配置折成内存图上界（G19：可配；0 = 默认 10 万节点/100 万边）。
func (c *Config) graphLimits() graph.Limits {
	return graph.Limits{MaxNodes: c.GraphMaxNodes, MaxEdges: c.GraphMaxEdges}
}

// graphCoreSet 解析"核心接口"节点声明（W16 第二个客观依据）。
func (c *Config) graphCoreSet() graph.CoreSet {
	return graph.ParseCoreSet(c.GraphCoreNodes)
}

// graphThresholds 取 escalate 判定阈值（W15：风险闸门；0 = 默认）。
func (c *Config) graphThresholds() graph.Thresholds {
	return graph.Thresholds{MaxReachable: c.GraphEscalateReachable}
}

// graphReachLimits 把宿主配置折成遍历上限（F5：旋钮不许是死的）。
//
// 口径：**调用方没给（0）才用配置值**；调用方给了就以调用方为准（请求级覆盖 > 配置）。
// 配置值本身是 0 时 → 0（框架默认，见 core/graph.DefaultMaxDepth/DefaultMaxResults）。
func (c *Config) graphReachLimits(depth, results int) (int, int) {
	if depth == 0 {
		depth = c.GraphMaxDepth
	}
	if results == 0 {
		results = c.GraphMaxResults
	}
	return depth, results
}

// Graph 返回图查询引擎（nil = 图投影未启用，G2f）。
func (h *Hub) Graph() *graph.Engine {
	if h == nil {
		return nil
	}
	return h.graphEngine
}

// GraphEnabled 报告图投影是否启用（UI/CLI 用来决定要不要展示图面板）。
func (h *Hub) GraphEnabled() bool { return h != nil && h.graphEngine != nil }

// requireGraph 取引擎，未启用 → ErrGraphDisabled（fail-close：不返回空影响面）。
func (h *Hub) requireGraph() (*graph.Engine, error) {
	if h == nil || h.graphEngine == nil {
		return nil, coregraph.ErrGraphDisabled
	}
	return h.graphEngine, nil
}

// GraphNeighbors 一跳/两跳邻居查询（G18：直查持久层双向倒排）。
//
// 方向语义：DirectionOut = "我依赖谁"；DirectionIn = "谁依赖我"。
//
// 口径：**恰好 q.Hops 跳**（不含一跳邻居）；结果有上限（q.Limit，0 = 默认 1000）——
// 想知道"是不是被上限截断了"用 GraphNeighborsResult。
func (h *Hub) GraphNeighbors(ctx context.Context, q graph.NeighborQuery) ([]string, error) {
	eng, err := h.requireGraph()
	if err != nil {
		return nil, err
	}
	return eng.Neighbors(ctx, q)
}

// GraphNeighborsResult 一跳/两跳邻居 + **截断标记**（F12：超级节点保护）。
//
// 与 GraphNeighbors 同口径，额外给出 Truncated（结果是否被上限截断）与生效的 Limit。
func (h *Hub) GraphNeighborsResult(ctx context.Context, q graph.NeighborQuery) (graph.NeighborResult, error) {
	eng, err := h.requireGraph()
	if err != nil {
		return graph.NeighborResult{}, err
	}
	return eng.NeighborsResult(ctx, q)
}

// GraphReachable 多跳可达闭包（G18：内存图 BFS + G20 闭包缓存）。
//
// **同步在引擎内部做**（F13）：缓存命中 → 零 SQL 直接返回；未命中 → 增量同步 + BFS。
// 宿主不再额外 Sync 一次（那会让每次请求打两条水位查询，而缓存命中的收益全被吃掉）。
//
// **口径：缓存命中不触发同步**（N6）——所以**跨进程**写入（另一个索引器写进同一个库）
// 对"命中缓存的查询"不可见，要等下一次 cache miss（或者宿主在 Run 结束后的
// catch-up 把缓存失效掉）。**同进程**写入不受影响：索引器提交后写面发 Committed()
// → 缓存失效 → 下一次查询必然 cache miss 并同步到最新边。当前部署的索引器与宿主
// 同进程，所以这条差异在现网观察不到；**跨进程部署前要意识到它**。
//
// 上限口径：请求没给（0）时用宿主配置（-graph-max-depth / -graph-max-results，F5）。
func (h *Hub) GraphReachable(ctx context.Context, q graph.ReachQuery) (graph.Closure, error) {
	eng, err := h.requireGraph()
	if err != nil {
		return graph.Closure{}, err
	}
	q.MaxDepth, q.MaxResults = h.cfg.graphReachLimits(q.MaxDepth, q.MaxResults)
	return eng.Reachable(ctx, q)
}

// ChangeImpact 生成 change_impact 影响面报告（G21/W16：依赖图可达性 → 改动影响面）。
//
// 入参 roots 是**改动起点**（diff 触及的节点 id，由调用方从 git diff 等处取得——
// 框架不自己算 diff）。请求里的 Core / Thresholds 为空时用宿主配置补
// （-graph-core / -graph-escalate-reachable）： escalate 判定要用配置的核心接口集，
// 否则"触及核心接口"这个客观依据永远不生效。
//
// **口径同 GraphReachable**：同步在引擎内部做（F13），**缓存命中不触发同步**（N6）——
// 跨进程写入对命中缓存的查询不可见（同进程写入经 Committed() 失效缓存，可见）。
//
// 上限口径：请求没给（0）时用宿主配置（F5）。
func (h *Hub) ChangeImpact(ctx context.Context, req graph.ImpactRequest) (graph.ImpactReport, error) {
	eng, err := h.requireGraph()
	if err != nil {
		return graph.ImpactReport{}, err
	}
	if req.Core.Empty() {
		req.Core = h.cfg.graphCoreSet()
	}
	if req.Thresholds == (graph.Thresholds{}) {
		req.Thresholds = h.cfg.graphThresholds()
	}
	req.MaxDepth, req.MaxResults = h.cfg.graphReachLimits(req.MaxDepth, req.MaxResults)
	return eng.Impact(ctx, req)
}

// GraphStats 返回图引擎统计（诊断/观测面）。
func (h *Hub) GraphStats() (graph.Stats, error) {
	eng, err := h.requireGraph()
	if err != nil {
		return graph.Stats{}, err
	}
	return eng.Stats(), nil
}

// ---------------------------------------------------------------------------
// HTTP：GET /api/graph/impact（change_impact 影响面报告，G21/W16）
// ---------------------------------------------------------------------------

// serveGraphImpact 服务 GET /api/graph/impact。
//
//	query: roots=a,b,c（必填，改动起点）& kind=imports（可选，边类型）
//	       & depth=5（可选）& limit=1000（可选）
//	       & core=x,y（可选，覆盖宿主配置的核心接口集；`*` 结尾 = 前缀）
//	       & diff_files=3&diff_added=120&diff_deleted=40（可选，diff 规模）
//	200   → ImpactReport（含 Scale / suggest_escalate / escalate_reasons）
//	400   → roots 为空 / 数值参数非法
//	503   → 图投影未启用（G2f 可选投影：明说，不返回空影响面）
func (h *Hub) serveGraphImpact(w http.ResponseWriter, r *http.Request) {
	if h.graphEngine == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "graph_disabled",
			"图投影未启用（G2f 可选投影）：用 -graph 启用后再查影响面")
		return
	}
	q := r.URL.Query()
	roots := splitCSV(q.Get("roots"))
	if len(roots) == 0 {
		writeJSONError(w, http.StatusBadRequest, "bad_roots",
			"roots 必填（逗号分隔的改动起点节点 id，如 core/store.go,contracts/event.go）")
		return
	}
	req := graph.ImpactRequest{
		Roots: roots,
		Kind:  q.Get("kind"),
	}
	if v := strings.TrimSpace(q.Get("depth")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeJSONError(w, http.StatusBadRequest, "bad_depth", "depth 必须是正整数")
			return
		}
		req.MaxDepth = n
	}
	if v := strings.TrimSpace(q.Get("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeJSONError(w, http.StatusBadRequest, "bad_limit", "limit 必须是正整数")
			return
		}
		req.MaxResults = n
	}
	if v := strings.TrimSpace(q.Get("core")); v != "" {
		req.Core = graph.ParseCoreSet(splitCSV(v))
	}
	// diff 规模（W16 第一个客观依据）：三个都可选，给了任何一个就当"提供了 diff"。
	files, added, deleted := queryInt(q, "diff_files"), queryInt(q, "diff_added"), queryInt(q, "diff_deleted")
	if files > 0 || added > 0 || deleted > 0 {
		req.Diff = &graph.DiffStat{Files: files, Added: added, Deleted: deleted}
	}
	rep, err := h.ChangeImpact(r.Context(), req)
	if err != nil {
		if errors.Is(err, coregraph.ErrGraphDisabled) {
			writeJSONError(w, http.StatusServiceUnavailable, "graph_disabled", err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "impact_failed", err.Error())
		return
	}
	if rep.Direct == nil {
		rep.Direct = []string{}
	}
	if rep.Transitive == nil {
		rep.Transitive = []string{}
	}
	writeJSONBody(w, http.StatusOK, rep)
}

// queryInt 读一个非负整数 query 参数（缺失/非法/负 → 0）。
func queryInt(q map[string][]string, key string) int {
	vals := q[key]
	if len(vals) == 0 {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(vals[0]))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// splitCSV 把逗号分隔的 query 参数切开（去空白、丢空项）。
//
// node id 可能含 `/` `.` `_` `-` 但**不会含逗号**（逗号在本查询里是分隔符）——
// 所以逗号分隔安全；含空白/逗号的 id 请走 Go API（不走 query string）。
func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// 编译期断言：hub 的图查询方法与 core/graph 的形状一致（改签名时这里先红）。
var (
	_ func(context.Context, graph.NeighborQuery) ([]string, error)             = (*Hub)(nil).GraphNeighbors
	_ func(context.Context, graph.NeighborQuery) (graph.NeighborResult, error) = (*Hub)(nil).GraphNeighborsResult
	_ func(context.Context, graph.ReachQuery) (graph.Closure, error)           = (*Hub)(nil).GraphReachable
	_ func(context.Context, graph.ImpactRequest) (graph.ImpactReport, error)   = (*Hub)(nil).ChangeImpact
	_ coregraph.Graph                                                          = (*graph.Engine)(nil)
	_ coregraph.BoundedGraph                                                   = (*graph.Engine)(nil)
)
