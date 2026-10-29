package graph

import (
	"context"
	"sort"
	"sync"
)

// nodeID 是 interned 后的节点 id（G19：不用原始长字符串做键——10 万节点时
// map[string]… 的键开销与比较成本都会失控）。
type nodeID int32

// kindID 是 interned 后的边类型 id（kind 集合很小，intern 后边记录只剩两个 int32）。
type kindID int32

// memEdge 是邻接表里的一条边（8 字节：目标节点 + 边类型）。
type memEdge struct {
	to   nodeID
	kind kindID
}

// edgeKey 是去重键（同一 (src,kind,dst) 只留一条邻接项）。
//
// 为什么需要去重：内存图是持久层的缓存，增量加载重试（Sync 失败后重跑同一区间）
// 会把同一条边再Apply 一次。不去重则邻接表膨胀、EdgeCount 失真。
type edgeKey struct {
	from nodeID
	to   nodeID
	kind kindID
}

// MemGraph 是自写内存图：interned 邻接表 + visited set 的多跳遍历（G18/G19）。
//
// 并发：自身持锁；遍历走读锁（只读邻接表），加边走写锁。
type MemGraph struct {
	mu sync.RWMutex

	// ids/names 是 node id 的 intern 表与反查表（G19）。
	ids   map[string]nodeID
	names []string
	// kindIDs/kinds 是边类型的 intern 表与反查表。
	kindIDs map[string]kindID
	kinds   []string

	// out/in 是双向邻接表（出边 / 入边）——BFS 两个方向都不需要扫全表。
	out map[nodeID][]memEdge
	in  map[nodeID][]memEdge

	// seen 是已加入的边集合（去重）。
	seen map[edgeKey]struct{}

	limits    Limits
	saturated bool
}

// NewMemGraph 建一个空内存图（limits 0 = 默认上界）。
func NewMemGraph(limits Limits) (*MemGraph, error) {
	nl, err := limits.normalize()
	if err != nil {
		return nil, err
	}
	return &MemGraph{
		ids:     make(map[string]nodeID),
		kindIDs: make(map[string]kindID),
		out:     make(map[nodeID][]memEdge),
		in:      make(map[nodeID][]memEdge),
		seen:    make(map[edgeKey]struct{}),
		limits:  nl,
	}, nil
}

// AddEdge 加一条边（端点节点随之建立）。同一 (src,kind,dst) 重复加是幂等的。
//
// 触到上界 → ErrGraphFull（此后 saturated 恒为 true：**不再接受新边**，
// 调用方必须把"图不完整"当错误上报，而不是当空结果返回）。
func (g *MemGraph) AddEdge(src, dst, kind string) error {
	if src == "" || dst == "" || kind == "" {
		// 空端点/空 kind 的边没有意义（契约里三个字段都是 required）→ 丢弃。
		// 不猜默认值：猜一个"默认 kind"会让查询语义悄悄变形。
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.addEdgeLocked(src, dst, kind)
}

// addEdgeLocked 是 AddEdge 的内部实现（调用方必须持有写锁）。
func (g *MemGraph) addEdgeLocked(src, dst, kind string) error {
	if g.saturated {
		return ErrGraphFull
	}
	s := g.internLocked(src)
	d := g.internLocked(dst)
	k := g.internKindLocked(kind)
	key := edgeKey{from: s, to: d, kind: k}
	if _, dup := g.seen[key]; dup {
		return nil // 幂等：同一条边不重复占内存
	}
	if len(g.seen) >= g.limits.MaxEdges || len(g.names) > g.limits.MaxNodes {
		g.saturated = true
		return ErrGraphFull
	}
	e := memEdge{to: d, kind: k}
	g.out[s] = append(g.out[s], e)
	g.in[d] = append(g.in[d], memEdge{to: s, kind: k})
	g.seen[key] = struct{}{}
	return nil
}

// internLocked 把节点名折成整数 id（已存在则复用——**同名节点不膨胀**，G19）。
func (g *MemGraph) internLocked(name string) nodeID {
	if id, ok := g.ids[name]; ok {
		return id
	}
	id := nodeID(len(g.names))
	g.ids[name] = id
	g.names = append(g.names, name)
	return id
}

// internKindLocked 把边类型折成整数 id。
func (g *MemGraph) internKindLocked(kind string) kindID {
	if id, ok := g.kindIDs[kind]; ok {
		return id
	}
	id := kindID(len(g.kinds))
	g.kindIDs[kind] = id
	g.kinds = append(g.kinds, kind)
	return id
}

// HasNode 报告某节点名是否在图里。
func (g *MemGraph) HasNode(name string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.ids[name]
	return ok
}

// Reset 清空整张内存图（投影被重建后从 0 重载用；saturated 一并复位）。
func (g *MemGraph) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ids = make(map[string]nodeID)
	g.names = nil
	g.kindIDs = make(map[string]kindID)
	g.kinds = nil
	g.out = make(map[nodeID][]memEdge)
	g.in = make(map[nodeID][]memEdge)
	g.seen = make(map[edgeKey]struct{})
	g.saturated = false
}

// NodeCount / EdgeCount / Saturated / Limits 是规模、状态与上界查询（Stats 用）。
func (g *MemGraph) NodeCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.names)
}

func (g *MemGraph) EdgeCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.seen)
}

func (g *MemGraph) Saturated() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.saturated
}

// Limits 返回生效的上界（0 值已归一成默认）。
func (g *MemGraph) Limits() Limits {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.limits
}

// Reach 多源 BFS（G18：邻接表 + visited set + 深度上限 + 结果上限 + context 取消）。
//
// 语义口径：
//   - **起点不进结果**（自环与"绕回起点"的环都被滤掉）——查询语义是"除我之外还有谁"；
//   - 深度从起点算 1（Levels[0] = 一跳）；
//   - **结果上限是全局的**（所有层加起来 ≤ maxResults，F2）：命中即停（Truncated=true）——
//     "每层各 maxResults 个"会让总结果变成 depth × maxResults，污染影响面计数
//     （ImpactReport.ReachableCount）与 escalate 判定；
//   - 深度/结果上限：0 = 默认；**负值拒绝**（ErrBadLimits，与契约一致，F9）；
//   - 方向 DirectionIn = 追"谁依赖我"（change_impact 的语义）。
func (g *MemGraph) Reach(ctx context.Context, sources []string, kind string, dir Direction, maxDepth, maxResults int) (Closure, error) {
	if maxDepth < 0 {
		return Closure{}, ErrBadLimits
	}
	if maxResults < 0 {
		return Closure{}, ErrBadLimits
	}
	if maxDepth == 0 {
		maxDepth = DefaultMaxDepth
	}
	if maxResults == 0 {
		maxResults = DefaultMaxResults
	}
	out := Closure{Kind: kind, Direction: dir, MaxDepth: maxDepth}
	if len(sources) == 0 {
		return out, ErrNoSources
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	visited := make(map[nodeID]struct{}, len(sources))
	frontier := make([]nodeID, 0, len(sources))
	for _, name := range sources {
		id, ok := g.ids[name]
		if !ok {
			out.MissingRoots = append(out.MissingRoots, name)
			continue
		}
		out.Roots = append(out.Roots, name)
		if _, dup := visited[id]; dup {
			continue // 同一起点给两遍只算一次
		}
		visited[id] = struct{}{}
		frontier = append(frontier, id)
	}
	sort.Strings(out.Roots)
	sort.Strings(out.MissingRoots)

	levels := make([][]string, 0, 8)
	scanned := 0
	// total 是**全局**结果计数（所有层加起来）：MaxResults 是总上限，不是每层上限（F2）。
	total := 0
	for depth := 1; depth <= maxDepth && len(frontier) > 0; depth++ {
		// context 取消：每层查一次，层内每 1024 条边查一次（大图层内耗时可能很长）。
		if err := ctx.Err(); err != nil {
			return Closure{}, err
		}
		var next []nodeID
		var level []string
		for _, n := range frontier {
			adj := g.out[n]
			if dir == DirectionIn {
				adj = g.in[n]
			}
			for _, e := range adj {
				scanned++
				if scanned%1024 == 0 {
					if err := ctx.Err(); err != nil {
						return Closure{}, err
					}
				}
				if kind != "" && g.kinds[e.kind] != kind {
					continue
				}
				if _, seen := visited[e.to]; seen {
					continue
				}
				visited[e.to] = struct{}{}
				level = append(level, g.names[e.to])
				total++
				if total >= maxResults {
					// 命中**总**上限：本层就此打住，标记截断后返回（同层不保证完整）。
					sort.Strings(level)
					levels = append(levels, level)
					out.Edges = scanned
					out.Truncated = true
					out.Depth = depth
					out.Levels = levels
					out.Nodes = flatten(levels)
					return out, nil
				}
				next = append(next, e.to)
			}
		}
		if len(level) == 0 {
			break // 这一层没有新节点 → 到底了
		}
		sort.Strings(level)
		levels = append(levels, level)
		out.Depth = depth
		frontier = next
	}
	out.Levels = levels
	out.Nodes = flatten(levels)
	out.Edges = scanned
	return out, nil
}

// flatten 把分层结果摊平（Nodes = 各层拼接，层内已排序 → 输出确定）。
func flatten(levels [][]string) []string {
	n := 0
	for _, lv := range levels {
		n += len(lv)
	}
	if n == 0 {
		return nil
	}
	out := make([]string, 0, n)
	for _, lv := range levels {
		out = append(out, lv...)
	}
	return out
}

// ---------------------------------------------------------------------------
// 闭包缓存的键与实现见 cache.go（G20：简版 = 内存 map + 图写入失效）
// ---------------------------------------------------------------------------
