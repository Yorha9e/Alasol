package graph

import (
	"context"
	"errors"

	"github.com/Yorha9e/alasol/contracts"
)

// Direction 是遍历方向（边的 src→dst 语义由工具自己解释，框架不猜）。
type Direction int

const (
	// DirectionOut 沿 src→dst 走 = "我依赖谁"（出边）。
	DirectionOut Direction = iota
	// DirectionIn 沿 dst→src 走 = "谁依赖我"（入边）。
	//
	// **change_impact 用的是这个**：改了 X，受影响的是"依赖 X 的那些"。
	DirectionIn
)

// String 给日志/报告用的方向名。
func (d Direction) String() string {
	switch d {
	case DirectionIn:
		return "in"
	default:
		return "out"
	}
}

// 默认上限（G18/G19：深度/结果/内存都有上界；0 = 用默认，负值 = 拒绝）。
const (
	// DefaultMaxDepth 是多跳遍历的默认深度上限。
	DefaultMaxDepth = 8
	// DefaultMaxResults 是多跳遍历的默认结果上限。
	DefaultMaxResults = 5000
	// DefaultMaxNodes 是内存图的默认节点上界（G19 量级口径）。
	DefaultMaxNodes = 100000
	// DefaultMaxEdges 是内存图的默认边上界（G19 量级口径）。
	DefaultMaxEdges = 1000000
	// DefaultEscalateReachable 是 change_impact 的默认 escalate 阈值（可达节点数）。
	DefaultEscalateReachable = 50
	// DefaultEscalateDirect 是 change_impact 的默认 escalate 阈值（一跳下游数）。
	DefaultEscalateDirect = 10
	// DefaultEscalateDiffLines 是 change_impact 的默认 escalate 阈值（diff 行数）。
	DefaultEscalateDiffLines = 500
	// MaxHopsSQL 是"走持久层倒排"的最大跳数（G18：一跳/两跳走 SQLite，更多走内存 BFS）。
	MaxHopsSQL = 2
)

// 错误（哨兵，调用方用 errors.Is 判定）。
var (
	// ErrBadHops 是跳数不合法（只支持 1..MaxHopsSQL）。
	ErrBadHops = errors.New("graph: hops 只支持 1|2（更多跳用 Reachable 走内存 BFS）")
	// ErrNoSources 是查询没有起点。
	ErrNoSources = errors.New("graph: 查询至少需要一个起点")
	// ErrGraphFull 是内存图触到配置上界（G19：fail-close，不返回可能不完整的结果）。
	ErrGraphFull = errors.New("graph: 内存图已达配置上界（MaxNodes/MaxEdges）→ 停止加载")
	// ErrBadLimits 是上限配置非法（负值）。
	ErrBadLimits = errors.New("graph: 内存上界不能为负（0 = 用默认）")
	// ErrGraphDisabled 是图投影未启用（G2f：可选投影）。
	ErrGraphDisabled = errors.New("graph: 图投影未启用（可选投影，G2f）")
)

// Limits 是内存图的上界（G19：10 万节点/100 万边 ~100–200MB 量级，可配）。
// 0 = 用默认；负值 = 拒绝（ErrBadLimits）。
type Limits struct {
	MaxNodes int
	MaxEdges int
}

// DefaultLimits 返回默认上界（G19 量级口径）。
func DefaultLimits() Limits {
	return Limits{MaxNodes: DefaultMaxNodes, MaxEdges: DefaultMaxEdges}
}

// normalize 归一化上界（0 → 默认；负 → 错）。
func (l Limits) normalize() (Limits, error) {
	out := l
	if out.MaxNodes < 0 || out.MaxEdges < 0 {
		return Limits{}, ErrBadLimits
	}
	if out.MaxNodes == 0 {
		out.MaxNodes = DefaultMaxNodes
	}
	if out.MaxEdges == 0 {
		out.MaxEdges = DefaultMaxEdges
	}
	return out, nil
}

// Tx 是"一批写要么全成要么全不成"的最小面（G1/G2：存储接口**不泄漏引擎特性**，G2b）。
//
// core 只声明形状、不含 IO；SQLite 的 *sql.Tx 只是它的一个实现（适配在 runtime/graph）。
// 换引擎（换一个图存储）时 core 侧一行不改。
//
// **core 不含任何 SQL 文本**（F6）：Tx 只声明"执行一条参数化语句"这个动作，
// 语句文本（upsert / delete / …）由**实现侧**（runtime/graph）持有并传入；
// **DDL 不走这条面**——建表由实现自建（见 GraphProjection.EnsureSchema），
// 免得把"建表 DDL"塞进调用方的事务里（core 契约不该管别人的事务边界）。
type Tx interface {
	// Exec 执行一条参数化语句（结果用不到就不返回）。
	Exec(ctx context.Context, query string, args ...any) error
}

// GraphProjection 是图投影的**写面**（G17：派生表只由索引器写；G22：边来自工具产出）。
//
// 索引器在消费事件流时调用它：建表（EnsureSchema）→ 在一个批次里写边（ApplyEdges，
// 与水位推进**同一事务**，G24：投影+水位原子，崩溃整体回滚）→ 提交成功后通知
// （Committed → 读侧失效闭包缓存，G20）。Rebuild 时清空（Clear）。
//
// 为什么写面在 core 而不在 runtime：G1/G2 要求"一组可组合的存储接口"，接口由**消费方**
// 定义；实现（SQLite node/edge 表）在 runtime/graph。
type GraphProjection interface {
	// EnsureSchema 建图投影表（幂等）。索引器确保自身 schema 时调用一次。
	//
	// **实现自建表**（F6/G2b）：DDL 文本与它的事务边界都归实现——core 契约里
	// 不出现 SQL 文本，也不替调用方决定建表该落在哪个事务里。
	EnsureSchema(ctx context.Context) error
	// ApplyEdges 在一个事务里写入一批边（端点节点随之建立）。
	// seq 是该批事件最后一条的 seq（读侧按它做增量加载水位）。
	ApplyEdges(ctx context.Context, tx Tx, seq int64, edges []contracts.GraphEdge) error
	// Clear 清空图投影（索引器 Rebuild 用，G24②）。
	Clear(ctx context.Context, tx Tx) error
	// Committed 在索引器**提交成功**一个写了边的批次后调用（G20：闭包缓存随之失效）。
	Committed()
}

// NeighborQuery 是一跳/两跳查询（G18：走持久层双向倒排）。
type NeighborQuery struct {
	// Node 是起点节点 id（**大小写敏感、字节精确**：不折叠大小写，S4/陷阱 17 同款纪律）。
	Node string
	// Kind 是边类型（开放字符串；"" = 不限 kind）。同样大小写敏感。
	Kind string
	// Direction 是方向（出 = 我依赖谁；入 = 谁依赖我）。
	Direction Direction
	// Hops 是跳数：1 或 2（其它值 → ErrBadHops；更多跳用 Reachable）。
	//
	// **口径：恰好 N 跳**（F8）——hops=2 的答案是"正好隔两层"的那批节点，
	// **不含**一跳邻居（除非它同时也是两跳邻居）。要"depth 以内"的闭包语义
	// 请用 Reachable（ReachQuery.MaxDepth）。
	//
	// **别拿邻居查询当影响面用**（N7）：① 恰好 N 跳 ≠ 闭包（两跳不含一跳邻居）；
	// ② 结果有默认上限（Limit，0 = DefaultNeighborLimit = 1000），而
	// Graph.Neighbors 的最小面**看不到截断标记**（要拿 Truncated 得用
	// BoundedGraph.NeighborsResult）——被截断的答案会被误当成"完整的影响面"。
	// **影响面用途一律走 Reachable / Impact**（它们按 ≤ depth 出闭包，且报告里
	// 有 ReachableCount / Truncated / UnknownRoots 这些诚实信号）。
	Hops int
	// Limit 是结果上限（0 = DefaultNeighborLimit；负值拒绝 ErrBadLimits）。
	//
	// 为什么需要：超级节点（一个节点连着十万条边）一次就能把结果集拉爆——
	// 内存、传输、调用方的渲染都扛不住（F12）。
	Limit int
}

// DefaultNeighborLimit 是一跳/两跳邻居查询的默认结果上限（F12：超级节点保护）。
const DefaultNeighborLimit = 1000

// NeighborResult 是一跳/两跳邻居的**完整**结果（带截断标记，F12）。
//
// Graph 接口只暴露最小面（Neighbors → []string）；想知道"结果是不是被上限截断了"
// 的调用方用实现侧额外提供的本接口（见 BoundedGraph）。
type NeighborResult struct {
	// Nodes 是邻居节点 id（已排序；不含起点自身）。
	Nodes []string `json:"nodes"`
	// Truncated 表示命中结果上限被截断（**同层不保证完整**——命中断了就停）。
	Truncated bool `json:"truncated"`
	// Limit 是生效的结果上限（0 = 默认 DefaultNeighborLimit）。
	Limit int `json:"limit"`
}

// BoundedGraph 是"带上限与截断标记"的邻居查询面（F12 的可选扩展面）。
//
// core 的 Graph 接口刻意只留最小面（Neighbors → []string，不破坏既有调用方）；
// 想知道结果是否被上限截断的实现额外实现本接口。
type BoundedGraph interface {
	// NeighborsResult 返回带截断标记的邻居结果。
	NeighborsResult(ctx context.Context, q NeighborQuery) (NeighborResult, error)
}

// ReachQuery 是多跳可达查询（G18：内存图 BFS）。
type ReachQuery struct {
	// Sources 是起点集合（至少一个；多起点 = 多源 BFS，change_impact 用）。
	Sources []string
	// Kind 是边类型（"" = 不限）。
	Kind string
	// Direction 是方向（影响面 = DirectionIn）。
	Direction Direction
	// MaxDepth 是深度上限（0 = DefaultMaxDepth；负值拒绝）。
	//
	// 口径是"**≤ MaxDepth**"（depth 以内全要），与 NeighborQuery.Hops 的
	// "**恰好 N 跳**"不同（F8）——要哪一口径就用哪个查询面，别混用。
	MaxDepth int
	// MaxResults 是结果上限（0 = DefaultMaxResults；负值拒绝）。
	//
	// 口径是"**总**上限"（所有层加起来 ≤ MaxResults，F2）——不是每层各一个。
	MaxResults int
}

// Closure 是多跳可达结果。
type Closure struct {
	// Roots 是实际使用的起点（查询时存在于图里的那些）。
	Roots []string `json:"roots"`
	// MissingRoots 是**图里没有**的起点（诚实信号：工具还没产出过这些边）。
	MissingRoots []string `json:"missing_roots,omitempty"`
	// Kind / Direction 回显查询条件。
	Kind      string    `json:"kind"`
	Direction Direction `json:"direction"`
	// MaxDepth 回显生效的深度上限。
	MaxDepth int `json:"max_depth"`
	// Depth 是实际达到的最大深度（0 = 没有可达节点）。
	Depth int `json:"depth"`
	// Nodes 是全部可达节点（**不含起点**），按层序；已排序（确定性输出）。
	Nodes []string `json:"nodes"`
	// Levels 是分层结果：Levels[0] = 一跳，Levels[1] = 两跳……
	// （change_impact 的"直接影响面 / 传递影响面"就从这里切。）
	Levels [][]string `json:"levels,omitempty"`
	// Truncated 表示命中**总**结果上限被截断（MaxResults 是所有层加起来的上限，
	// F2——命中即停，同层不保证完整）。
	Truncated bool `json:"truncated"`
	// Edges 是本次遍历扫过的边数（诊断/成本观测）。
	Edges int `json:"edges"`
}

// Stats 是图查询面的运行时统计（诊断 + 测试断言用）。
type Stats struct {
	// Nodes / Edges 是内存图当前规模。
	Nodes int `json:"nodes"`
	Edges int `json:"edges"`
	// LoadedSeq 是内存图已应用到的边 seq 水位（增量加载的游标）。
	LoadedSeq int64 `json:"loaded_seq"`
	// Saturated 表示触到内存上界后停止加载（此时多跳查询 fail-close 报错）。
	Saturated bool `json:"saturated"`
	// CacheEntries / CacheHits / CacheMisses 是闭包缓存的计数（G20）。
	CacheEntries int   `json:"cache_entries"`
	CacheHits    int64 `json:"cache_hits"`
	CacheMisses  int64 `json:"cache_misses"`
	// Syncs 是从持久层增量加载的次数（每次可能加载 0 条）。
	Syncs int64 `json:"syncs"`
}

// Graph 是图的读查询面（G1/G2：一类可组合的存储接口；core 定义、runtime 实现）。
//
// 实现必须是并发安全的（宿主会从多个 HTTP handler 并发查询）。
type Graph interface {
	// Neighbors 一跳/两跳邻居（G18：直查持久层双向倒排，不走内存图）。
	//
	// 结果**有上限**（NeighborQuery.Limit，0 = 默认 DefaultNeighborLimit）：
	// 超级节点一次能连出十万条边，不限就会把调用方拉爆（F12）。
	// 想知道"是否被截断"用 BoundedGraph.NeighborsResult。
	//
	// **口径提醒**：恰好 N 跳（不含一跳邻居）+ 可能有上限 → 影响面用途走
	// Reachable / Impact（N7）。
	Neighbors(ctx context.Context, q NeighborQuery) ([]string, error)
	// Reachable 多跳可达闭包（G18：内存图 BFS + 闭包缓存 G20）。
	Reachable(ctx context.Context, q ReachQuery) (Closure, error)
	// Impact 生成 change_impact 影响面报告（G21/W16）。
	Impact(ctx context.Context, req ImpactRequest) (ImpactReport, error)
	// Sync 把持久层的增量边并进内存图（冷启动流式加载 + 跨进程写入可见）。
	Sync(ctx context.Context) error
	// Stats 返回运行时统计。
	Stats() Stats
}

// Invalidator 是"图投影刚写入"的通知面（G20：闭包缓存必须随之失效）。
//
// 索引器提交一个写了边的批次后调用它。实现方（引擎）只需清缓存——
// 下一次多跳查询自然走增量同步把新边取回来。
type Invalidator interface {
	Invalidate()
}
