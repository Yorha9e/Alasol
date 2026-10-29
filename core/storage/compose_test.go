package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	coregraph "github.com/Yorha9e/alasol/core/graph"
)

// ---------------------------------------------------------------------------
// 编译期断言：测试替身确实实现各族接口（形状漂移立刻红）
// ---------------------------------------------------------------------------

var (
	_ EventLog              = (*memEventLog)(nil)
	_ EventLog              = (*logEventLog)(nil)
	_ Graph                 = (*mapGraph)(nil)
	_ coregraph.Invalidator = (*mapGraph)(nil)
	_ Graph                 = (*indexedGraph)(nil)
	_ Documents             = (*mapDocs)(nil)
	_ Documents             = (*logDocs)(nil)
	_ DocumentProjection    = (*mapDocs)(nil)
	_ DocumentProjection    = (*logDocs)(nil)
	_ Blob                  = (*mapBlob)(nil)
	_ Blob                  = (*chunkBlob)(nil)
	_ Retriever             = (*scanVectors)(nil)
	_ Retriever             = (*normVectors)(nil)
	_ VectorIndex           = (*normVectors)(nil)
	// 只读替身**故意不实现**写面（那是它们的重点：只挂读面是合法组合）。
	_ Graph     = (*readOnlyGraph)(nil)
	_ Documents = (*readOnlyDocs)(nil)
	_ Retriever = (*readOnlyVectors)(nil)
)

// ---------------------------------------------------------------------------
// 测试替身（**只活在 _test.go 里**）
//
// 纪律：这些不是产品实现——v4 才有真实现（文件系统 / 对象存储 / DB 大字段 / 真向量库）。
// 它们存在的唯一理由是**证明接口可组合、可换实现**：两套内部结构完全不同的替身
// （map vs 排序索引、整块存 vs 分块存、map vs 追加日志、现算余弦 vs 归一化点积）
// 跑同一份消费者代码，答案一致。
// ---------------------------------------------------------------------------

// ---- 事件日志替身：内存 + seq 单调 + 按 Event.ID 幂等（G9/G10/G11 的口径）----

type memEventLog struct {
	mu   sync.Mutex
	seq  int64
	byID map[contracts.Ulid]contracts.Event
}

func newMemEventLog() *memEventLog {
	return &memEventLog{byID: map[contracts.Ulid]contracts.Event{}}
}

func (s *memEventLog) Append(_ context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]contracts.Event, 0, len(evs))
	for _, ev := range evs {
		if !ev.Durable {
			return nil, fmt.Errorf("memEventLog: 拒绝非 durable 事件（F8）: %s", ev.ID)
		}
		if old, ok := s.byID[ev.ID]; ok {
			out = append(out, old) // 幂等：返回库中原事件（含原 seq）
			continue
		}
		s.seq++
		ev.Seq = &s.seq
		s.byID[ev.ID] = ev
		out = append(out, ev)
	}
	return out, nil
}

func (s *memEventLog) ReadFrom(_ context.Context, sessionID *contracts.Ulid, fromSeq int64, limit int) ([]contracts.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []contracts.Event
	for _, ev := range s.byID {
		if ev.Seq == nil || *ev.Seq < fromSeq {
			continue
		}
		if sessionID != nil && ev.SessionID != *sessionID {
			continue
		}
		all = append(all, ev)
	}
	sort.Slice(all, func(i, j int) bool { return *all[i].Seq < *all[j].Seq })
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

// ---- 事件日志替身 B：追加日志 + id→下标索引（内部结构与替身 A 不同）----
//
// 替身 A 是 map（id → 事件）配一个 seq 计数器；替身 B 是**只追加的日志**配一张
// id → 下标表：seq 由"日志长度"决定（追加序 = seq 序），读按日志顺序扫。
// 两者对外行为必须一致（G11 幂等 + G9/G10 seq 单调 + F8 只收 durable）。

type logEventLog struct {
	mu   sync.Mutex
	log  []contracts.Event
	byID map[contracts.Ulid]int
}

func newLogEventLog() *logEventLog {
	return &logEventLog{byID: map[contracts.Ulid]int{}}
}

func (s *logEventLog) Append(_ context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]contracts.Event, 0, len(evs))
	for _, ev := range evs {
		if !ev.Durable {
			return nil, fmt.Errorf("logEventLog: 拒绝非 durable 事件（F8）: %s", ev.ID)
		}
		if i, ok := s.byID[ev.ID]; ok {
			out = append(out, s.log[i]) // 幂等：返回库中原事件（含原 seq）
			continue
		}
		seq := int64(len(s.log)) + 1
		ev.Seq = &seq
		s.log = append(s.log, ev)
		s.byID[ev.ID] = len(s.log) - 1
		out = append(out, ev)
	}
	return out, nil
}

func (s *logEventLog) ReadFrom(_ context.Context, sessionID *contracts.Ulid, fromSeq int64, limit int) ([]contracts.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []contracts.Event
	for _, ev := range s.log {
		if ev.Seq == nil || *ev.Seq < fromSeq {
			continue
		}
		if sessionID != nil && ev.SessionID != *sessionID {
			continue
		}
		all = append(all, ev)
	}
	// 日志本身按 seq 升序（追加序 = seq 序）——不需要再排。
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

// ---- 只读替身：只实现**读面**（写面不在实现里）----
//
// 真实世界里"只读实现"就是这种形状（视图 / 只读副本 / 外部系统的读端）：
// G17 规定写面只归索引器写，所以一个实现**完全可以没有写面**。
// 组合面必须能表达这种组合（只挂读面）——取写面要带 ok，不许硬转型 panic（p2-3）。

type readOnlyGraph struct{ g *mapGraph }

func (r readOnlyGraph) Neighbors(ctx context.Context, q coregraph.NeighborQuery) ([]string, error) {
	return r.g.Neighbors(ctx, q)
}
func (r readOnlyGraph) Reachable(ctx context.Context, q coregraph.ReachQuery) (coregraph.Closure, error) {
	return r.g.Reachable(ctx, q)
}
func (r readOnlyGraph) Impact(ctx context.Context, req coregraph.ImpactRequest) (coregraph.ImpactReport, error) {
	return r.g.Impact(ctx, req)
}
func (r readOnlyGraph) Sync(context.Context) error { return nil }
func (r readOnlyGraph) Stats() coregraph.Stats     { return r.g.Stats() }

type readOnlyDocs struct{ d *mapDocs }

func (r readOnlyDocs) Get(ctx context.Context, collection, key string) (Doc, error) {
	return r.d.Get(ctx, collection, key)
}
func (r readOnlyDocs) List(ctx context.Context, q DocQuery) ([]Doc, error) {
	return r.d.List(ctx, q)
}

type readOnlyVectors struct{ v *scanVectors }

func (r readOnlyVectors) Retrieve(ctx context.Context, q VectorQuery) ([]VectorHit, error) {
	return r.v.Retrieve(ctx, q)
}

// ---- 图替身 A：邻接 map + 线性扫 ----

type mapGraph struct {
	mu      sync.RWMutex
	edges   []contracts.GraphEdge
	ensure  int
	clears  int
	commits int
}

// EnsureSchema / Clear / Committed 让替身同时实现 GraphProjection 写面
// （G17：派生表只归索引器写）——ApplyEdges 与 Tx 由调用方（消费者）传入。
func (g *mapGraph) EnsureSchema(context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ensure++
	return nil
}

func (g *mapGraph) Clear(ctx context.Context, tx coregraph.Tx) error {
	if err := tx.Exec(ctx, "fake.graph.clear"); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.edges = nil
	g.clears++
	return nil
}

func (g *mapGraph) Committed() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.commits++
}

func (g *mapGraph) ApplyEdges(ctx context.Context, tx coregraph.Tx, _ int64, edges []contracts.GraphEdge) error {
	// 写必须走**调用方传入的 Tx**（G2b：共享事务面）——真实现在这里执行参数化语句，
	// 替身用一句标记语句占位（测试断言的就是"用了这个 Tx"）。
	if err := tx.Exec(ctx, "fake.graph.apply", len(edges)); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.edges = append(g.edges, edges...)
	return nil
}

func (g *mapGraph) Neighbors(_ context.Context, q coregraph.NeighborQuery) ([]string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	// 恰好 N 跳：逐层扩张，取最后一层（与 runtime/graph 的 SQL 实现同口径）。
	level := []string{q.Node}
	seen := map[string]bool{q.Node: true}
	for hop := 0; hop < q.Hops; hop++ {
		var next []string
		for _, n := range level {
			for _, e := range g.edges {
				if e.Kind != "" && q.Kind != "" && e.Kind != q.Kind {
					continue
				}
				var dst string
				switch q.Direction {
				case coregraph.DirectionIn:
					if e.Dst == n {
						dst = e.Src
					}
				default:
					if e.Src == n {
						dst = e.Dst
					}
				}
				if dst == "" || seen[dst] {
					continue
				}
				seen[dst] = true
				next = append(next, dst)
			}
		}
		level = next
	}
	sort.Strings(level)
	return level, nil
}

func (g *mapGraph) Reachable(_ context.Context, q coregraph.ReachQuery) (coregraph.Closure, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	depth, maxResults := q.MaxDepth, q.MaxResults
	if depth <= 0 {
		depth = coregraph.DefaultMaxDepth
	}
	if maxResults <= 0 {
		maxResults = coregraph.DefaultMaxResults
	}
	seen := map[string]bool{}
	for _, s := range q.Sources {
		seen[s] = true
	}
	var levels [][]string
	total := 0
	cur := append([]string(nil), q.Sources...)
	for d := 0; d < depth; d++ {
		var next []string
		for _, n := range cur {
			for _, e := range g.edges {
				if q.Kind != "" && e.Kind != q.Kind {
					continue
				}
				var dst string
				if q.Direction == coregraph.DirectionIn {
					if e.Dst == n {
						dst = e.Src
					}
				} else if e.Src == n {
					dst = e.Dst
				}
				if dst == "" || seen[dst] {
					continue
				}
				seen[dst] = true
				next = append(next, dst)
			}
		}
		if len(next) == 0 {
			break
		}
		sort.Strings(next)
		if total+len(next) > maxResults {
			next = next[:maxResults-total]
			levels = append(levels, next)
			total = maxResults
			break
		}
		levels = append(levels, next)
		total += len(next)
		cur = next
	}
	return coregraph.Closure{
		Roots:     append([]string(nil), q.Sources...),
		Kind:      q.Kind,
		Direction: q.Direction,
		MaxDepth:  depth,
		Depth:     len(levels),
		Nodes:     flattenLevels(levels),
		Levels:    levels,
		Truncated: total >= maxResults,
		Edges:     len(g.edges),
	}, nil
}

func (g *mapGraph) Impact(_ context.Context, req coregraph.ImpactRequest) (coregraph.ImpactReport, error) {
	c, err := g.Reachable(context.Background(), coregraph.ReachQuery{
		Sources: req.Roots, Kind: req.Kind, Direction: coregraph.DirectionIn,
		MaxDepth: req.MaxDepth, MaxResults: req.MaxResults,
	})
	if err != nil {
		return coregraph.ImpactReport{}, err
	}
	return coregraph.AssembleImpact(req, c), nil
}

func (g *mapGraph) Sync(context.Context) error { return nil }
func (g *mapGraph) Stats() coregraph.Stats {
	g.mu.RLock()
	defer g.mu.RUnlock()
	nodes := map[string]bool{}
	for _, e := range g.edges {
		nodes[e.Src], nodes[e.Dst] = true, true
	}
	return coregraph.Stats{Nodes: len(nodes), Edges: len(g.edges)}
}
func (g *mapGraph) Invalidate() {}

func flattenLevels(levels [][]string) []string {
	var out []string
	for _, l := range levels {
		out = append(out, l...)
	}
	sort.Strings(out)
	return out
}

// ---- 图替身 B：按 (src,kind) / (dst,kind) 建**排序索引**，二分查邻居 ----
//
// 与替身 A 的内部结构完全不同（A 是全表线性扫，B 是倒排索引 + 二分）——
// 用来证明"换图实现不改消费者"。

type indexedGraph struct {
	mu      sync.RWMutex
	out     []contracts.GraphEdge // 按 (Src,Kind,Dst) 排序：出向倒排
	in      []contracts.GraphEdge // 按 (Dst,Kind,Src) 排序：入向倒排
	ensure  int
	clears  int
	commits int
}

func (g *indexedGraph) EnsureSchema(context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ensure++
	return nil
}

func (g *indexedGraph) Clear(ctx context.Context, tx coregraph.Tx) error {
	if err := tx.Exec(ctx, "fake.graph.clear"); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.out, g.in = nil, nil
	g.clears++
	return nil
}

func (g *indexedGraph) Committed() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.commits++
}

func (g *indexedGraph) ApplyEdges(ctx context.Context, tx coregraph.Tx, _ int64, edges []contracts.GraphEdge) error {
	if err := tx.Exec(ctx, "fake.graph.apply", len(edges)); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.out = append(g.out, edges...)
	g.in = append(g.in, edges...)
	sort.Slice(g.out, func(i, j int) bool {
		a, b := g.out[i], g.out[j]
		if a.Src != b.Src {
			return a.Src < b.Src
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.Dst < b.Dst
	})
	sort.Slice(g.in, func(i, j int) bool {
		a, b := g.in[i], g.in[j]
		if a.Dst != b.Dst {
			return a.Dst < b.Dst
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.Src < b.Src
	})
	return nil
}

// edgeKey 取这条边在给定方向上的"起点"（出向 = Src，入向 = Dst）。
func edgeKey(e contracts.GraphEdge, dir coregraph.Direction) string {
	if dir == coregraph.DirectionIn {
		return e.Dst
	}
	return e.Src
}

// edgeFar 取这条边在给定方向上的"对端"（出向 = Dst，入向 = Src）。
func edgeFar(e contracts.GraphEdge, dir coregraph.Direction) string {
	if dir == coregraph.DirectionIn {
		return e.Src
	}
	return e.Dst
}

// indexOf 按方向取对应倒排索引（调用方持锁）。
func (g *indexedGraph) indexOf(dir coregraph.Direction) []contracts.GraphEdge {
	if dir == coregraph.DirectionIn {
		return g.in
	}
	return g.out
}

func (g *indexedGraph) Neighbors(_ context.Context, q coregraph.NeighborQuery) ([]string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	// 走倒排索引：按方向取排序好的边表，对 key 做二分定位起点，同 key 的边连续 → 区间扫。
	idx := g.indexOf(q.Direction)
	level := []string{q.Node}
	seen := map[string]bool{q.Node: true}
	for hop := 0; hop < q.Hops; hop++ {
		var next []string
		for _, n := range level {
			lo := sort.Search(len(idx), func(i int) bool { return edgeKey(idx[i], q.Direction) >= n })
			for i := lo; i < len(idx); i++ {
				e := idx[i]
				if edgeKey(e, q.Direction) != n {
					break // 同 key 的边已扫完（排序保证连续）
				}
				if q.Kind != "" && e.Kind != q.Kind {
					continue
				}
				far := edgeFar(e, q.Direction)
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

func (g *indexedGraph) Reachable(ctx context.Context, q coregraph.ReachQuery) (coregraph.Closure, error) {
	// 逐跳调 Neighbors（内部走倒排索引）——与替身 A 不同算法路径，同口径答案。
	depth, maxResults := q.MaxDepth, q.MaxResults
	if depth <= 0 {
		depth = coregraph.DefaultMaxDepth
	}
	if maxResults <= 0 {
		maxResults = coregraph.DefaultMaxResults
	}
	seen := map[string]bool{}
	for _, s := range q.Sources {
		seen[s] = true
	}
	var levels [][]string
	total := 0
	cur := append([]string(nil), q.Sources...)
	for d := 0; d < depth; d++ {
		var next []string
		for _, n := range cur {
			ns, err := g.Neighbors(ctx, coregraph.NeighborQuery{
				Node: n, Kind: q.Kind, Direction: q.Direction, Hops: 1,
			})
			if err != nil {
				return coregraph.Closure{}, err
			}
			for _, x := range ns {
				if !seen[x] {
					seen[x] = true
					next = append(next, x)
				}
			}
		}
		if len(next) == 0 {
			break
		}
		sort.Strings(next)
		if total+len(next) > maxResults {
			next = next[:maxResults-total]
			levels = append(levels, next)
			total = maxResults
			break
		}
		levels = append(levels, next)
		total += len(next)
		cur = next
	}
	return coregraph.Closure{
		Roots: append([]string(nil), q.Sources...), Kind: q.Kind, Direction: q.Direction,
		MaxDepth: depth, Depth: len(levels), Nodes: flattenLevels(levels), Levels: levels,
		Truncated: total >= maxResults,
	}, nil
}

func (g *indexedGraph) Impact(ctx context.Context, req coregraph.ImpactRequest) (coregraph.ImpactReport, error) {
	c, err := g.Reachable(ctx, coregraph.ReachQuery{
		Sources: req.Roots, Kind: req.Kind, Direction: coregraph.DirectionIn,
		MaxDepth: req.MaxDepth, MaxResults: req.MaxResults,
	})
	if err != nil {
		return coregraph.ImpactReport{}, err
	}
	return coregraph.AssembleImpact(req, c), nil
}

func (g *indexedGraph) Sync(context.Context) error { return nil }
func (g *indexedGraph) Stats() coregraph.Stats {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return coregraph.Stats{Nodes: len(g.out) + 1, Edges: len(g.out)}
}
func (g *indexedGraph) Invalidate() {}

// ---- 图投影写面替身（复用 graph.Tx：事务最小面不泄漏引擎特性）----

type txRecorder struct {
	mu    sync.Mutex
	stmts []string
}

func (t *txRecorder) Exec(_ context.Context, query string, _ ...any) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stmts = append(t.stmts, query)
	return nil
}

func (t *txRecorder) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.stmts)
}

// ---- blob 替身 A：整块存内存 ----

type mapBlob struct {
	mu   sync.RWMutex
	data map[contracts.Ulid][]byte
	meta map[contracts.Ulid]BlobMeta
}

func newMapBlob() *mapBlob {
	return &mapBlob{data: map[contracts.Ulid][]byte{}, meta: map[contracts.Ulid]BlobMeta{}}
}

func (b *mapBlob) Put(_ context.Context, meta BlobMeta, content io.Reader) (BlobMeta, error) {
	raw, err := io.ReadAll(content)
	if err != nil {
		return BlobMeta{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if meta.ID == "" {
		meta.ID = core.NewUlid()
	} else if old, ok := b.data[meta.ID]; ok && !bytes.Equal(old, raw) {
		return BlobMeta{}, fmt.Errorf("%w: blob %s 内容不一致", ErrConflict, meta.ID)
	}
	meta.Size = int64(len(raw))
	if meta.CreatedAt == "" {
		meta.CreatedAt = "2026-01-29T00:00:00Z"
	}
	b.data[meta.ID] = raw
	b.meta[meta.ID] = meta
	return meta, nil
}

func (b *mapBlob) Get(_ context.Context, id contracts.Ulid) (io.ReadCloser, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	raw, ok := b.data[id]
	if !ok {
		return nil, fmt.Errorf("%w: blob %s", ErrNotFound, id)
	}
	return io.NopCloser(bytes.NewReader(raw)), nil
}

func (b *mapBlob) Stat(_ context.Context, id contracts.Ulid) (BlobMeta, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	m, ok := b.meta[id]
	if !ok {
		return BlobMeta{}, fmt.Errorf("%w: blob %s", ErrNotFound, id)
	}
	return m, nil
}

func (b *mapBlob) Delete(_ context.Context, id contracts.Ulid) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.data, id)
	delete(b.meta, id)
	return nil
}

// ---- blob 替身 B：按 7 字节分块存（内部结构完全不同；Get 时重组）----

type chunkBlob struct {
	mu      sync.RWMutex
	chunks  map[contracts.Ulid][][]byte
	meta    map[contracts.Ulid]BlobMeta
	chunkSz int
}

func newChunkBlob(chunkSz int) *chunkBlob {
	return &chunkBlob{chunks: map[contracts.Ulid][][]byte{}, meta: map[contracts.Ulid]BlobMeta{}, chunkSz: chunkSz}
}

func (b *chunkBlob) Put(_ context.Context, meta BlobMeta, content io.Reader) (BlobMeta, error) {
	raw, err := io.ReadAll(content)
	if err != nil {
		return BlobMeta{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if meta.ID == "" {
		meta.ID = core.NewUlid()
	} else if old, ok := b.chunks[meta.ID]; ok && !bytes.Equal(joinChunks(old), raw) {
		return BlobMeta{}, fmt.Errorf("%w: blob %s 内容不一致", ErrConflict, meta.ID)
	}
	var parts [][]byte
	for off := 0; off < len(raw); off += b.chunkSz {
		end := off + b.chunkSz
		if end > len(raw) {
			end = len(raw)
		}
		parts = append(parts, append([]byte(nil), raw[off:end]...))
	}
	meta.Size = int64(len(raw))
	if meta.CreatedAt == "" {
		meta.CreatedAt = "2026-01-29T00:00:00Z"
	}
	b.chunks[meta.ID] = parts
	b.meta[meta.ID] = meta
	return meta, nil
}

func (b *chunkBlob) Get(_ context.Context, id contracts.Ulid) (io.ReadCloser, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	parts, ok := b.chunks[id]
	if !ok {
		return nil, fmt.Errorf("%w: blob %s", ErrNotFound, id)
	}
	return io.NopCloser(bytes.NewReader(joinChunks(parts))), nil
}

func (b *chunkBlob) Stat(_ context.Context, id contracts.Ulid) (BlobMeta, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	m, ok := b.meta[id]
	if !ok {
		return BlobMeta{}, fmt.Errorf("%w: blob %s", ErrNotFound, id)
	}
	return m, nil
}

func (b *chunkBlob) Delete(_ context.Context, id contracts.Ulid) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.chunks, id)
	delete(b.meta, id)
	return nil
}

func joinChunks(parts [][]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// ---- 文档替身 A：map ----

type mapDocs struct {
	mu   sync.RWMutex
	data map[string]Doc
}

func newMapDocs() *mapDocs {
	return &mapDocs{data: map[string]Doc{}}
}

func docKey(collection, key string) string { return collection + "\x00" + key }

func (d *mapDocs) EnsureSchema(context.Context) error { return nil }
func (d *mapDocs) Upsert(ctx context.Context, tx coregraph.Tx, docs []Doc) error {
	if err := tx.Exec(ctx, "fake.docs.upsert", len(docs)); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	// ① **先整批校验**（批内冲突 + 与库里现存版本的冲突），② 再整批写。
	//
	// 契约是"整批拒绝"（DocumentProjection.Upsert）：一批里有一条冲突，这一批
	// **一条都不许写**——写一半 = 半个真相（调用方以为整批成了），比全拒更坏。
	if err := d.checkBatch(docs); err != nil {
		return err
	}
	for _, doc := range docs {
		d.data[docKey(doc.Collection, doc.Key)] = doc
	}
	return nil
}

// checkBatch 校验一批文档的写入口径（调用方持有锁）：批内同一键的版本必须一致，
// 且与库里现存版本一致（doc.Revision 非 0 时）。
func (d *mapDocs) checkBatch(docs []Doc) error {
	pending := map[string]Doc{}
	for _, doc := range docs {
		k := docKey(doc.Collection, doc.Key)
		if prev, ok := pending[k]; ok && doc.Revision > 0 && prev.Revision != doc.Revision {
			return fmt.Errorf("%w: 批内 doc %s 版本 %d ≠ %d", ErrConflict, k, doc.Revision, prev.Revision)
		}
		pending[k] = doc
	}
	for k, doc := range pending {
		if old, ok := d.data[k]; ok && doc.Revision > 0 && old.Revision != doc.Revision {
			return fmt.Errorf("%w: doc %s 版本 %d ≠ %d", ErrConflict, k, doc.Revision, old.Revision)
		}
	}
	return nil
}
func (d *mapDocs) Delete(ctx context.Context, tx coregraph.Tx, collection string, keys []string) error {
	if err := tx.Exec(ctx, "fake.docs.delete", len(keys)); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, k := range keys {
		delete(d.data, docKey(collection, k))
	}
	return nil
}
func (d *mapDocs) Clear(ctx context.Context, tx coregraph.Tx) error {
	if err := tx.Exec(ctx, "fake.docs.clear"); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.data = map[string]Doc{}
	return nil
}
func (d *mapDocs) Get(_ context.Context, collection, key string) (Doc, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	doc, ok := d.data[docKey(collection, key)]
	if !ok {
		return Doc{}, fmt.Errorf("%w: doc %s/%s", ErrNotFound, collection, key)
	}
	return doc, nil
}
func (d *mapDocs) List(_ context.Context, q DocQuery) ([]Doc, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var out []Doc
	for _, doc := range d.data {
		if q.Collection != "" && doc.Collection != q.Collection {
			continue
		}
		if q.Prefix != "" && !strings.HasPrefix(doc.Key, q.Prefix) {
			continue
		}
		out = append(out, doc)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Collection != out[j].Collection {
			return out[i].Collection < out[j].Collection
		}
		return out[i].Key < out[j].Key
	})
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

// ---- 文档替身 B：追加日志 + 读时折叠（"事件日志式"内部结构）----

type logDocs struct {
	mu   sync.RWMutex
	log  []Doc
	seen map[string]int // docKey → 最新版本在 log 里的下标
}

func newLogDocs() *logDocs {
	return &logDocs{seen: map[string]int{}}
}

func (d *logDocs) EnsureSchema(context.Context) error { return nil }
func (d *logDocs) Upsert(ctx context.Context, tx coregraph.Tx, docs []Doc) error {
	if err := tx.Exec(ctx, "fake.docs.upsert", len(docs)); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	// ① 先整批校验（批内 + 与库里现存版本），② 再整批写——契约是"整批拒绝"
	// （追加日志实现最容易写坏这条：边追加边判冲突会留下半批写入）。
	if err := d.checkBatch(docs); err != nil {
		return err
	}
	for _, doc := range docs {
		k := docKey(doc.Collection, doc.Key)
		d.log = append(d.log, doc)
		d.seen[k] = len(d.log) - 1
	}
	return nil
}

// checkBatch 校验一批文档的写入口径（调用方持有锁）。
func (d *logDocs) checkBatch(docs []Doc) error {
	pending := map[string]Doc{}
	for _, doc := range docs {
		k := docKey(doc.Collection, doc.Key)
		if prev, ok := pending[k]; ok && doc.Revision > 0 && prev.Revision != doc.Revision {
			return fmt.Errorf("%w: 批内 doc %s 版本 %d ≠ %d", ErrConflict, k, doc.Revision, prev.Revision)
		}
		pending[k] = doc
	}
	for k, doc := range pending {
		i, ok := d.seen[k]
		if !ok {
			continue // 从没写过 → 没有现存版本
		}
		cur := d.log[i]
		if cur.Key == "" {
			// **墓碑 = 这条被删掉了**（Delete 清空内容留行）→ 没有现存版本，
			// 写回来是允许的（契约：删除后不存在冲突）。
			// 不看墓碑的 Revision（它是 0）——否则"删了再写同一键"会被误判成冲突
			// （与 mapDocs 替身口径不一致：那边 Delete 直接摘掉键）。
			continue
		}
		if doc.Revision > 0 && cur.Revision != doc.Revision {
			return fmt.Errorf("%w: doc %s 版本 %d ≠ %d", ErrConflict, k, doc.Revision, cur.Revision)
		}
	}
	return nil
}
func (d *logDocs) Delete(ctx context.Context, tx coregraph.Tx, collection string, keys []string) error {
	if err := tx.Exec(ctx, "fake.docs.delete", len(keys)); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, k := range keys {
		key := docKey(collection, k)
		if i, ok := d.seen[key]; ok {
			d.log[i] = Doc{} // 墓碑：保留行、内容清空（读时视为不存在）
		}
	}
	return nil
}
func (d *logDocs) Clear(ctx context.Context, tx coregraph.Tx) error {
	if err := tx.Exec(ctx, "fake.docs.clear"); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.log, d.seen = nil, map[string]int{}
	return nil
}
func (d *logDocs) Get(_ context.Context, collection, key string) (Doc, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	i, ok := d.seen[docKey(collection, key)]
	if !ok || d.log[i].Key == "" {
		return Doc{}, fmt.Errorf("%w: doc %s/%s", ErrNotFound, collection, key)
	}
	return d.log[i], nil
}
func (d *logDocs) List(_ context.Context, q DocQuery) ([]Doc, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var out []Doc
	for i := range d.log {
		doc := d.log[i]
		if doc.Key == "" || d.seen[docKey(doc.Collection, doc.Key)] != i {
			continue // 被后来的版本/墓碑盖掉
		}
		if q.Collection != "" && doc.Collection != q.Collection {
			continue
		}
		if q.Prefix != "" && !strings.HasPrefix(doc.Key, q.Prefix) {
			continue
		}
		out = append(out, doc)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Collection != out[j].Collection {
			return out[i].Collection < out[j].Collection
		}
		return out[i].Key < out[j].Key
	})
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

// ---- 向量替身：线性扫 + 余弦 ----

type scanVectors struct {
	mu   sync.RWMutex
	byID map[contracts.Ulid]Vector
}

func newScanVectors() *scanVectors {
	return &scanVectors{byID: map[contracts.Ulid]Vector{}}
}

func (v *scanVectors) Upsert(_ context.Context, vs []Vector) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, x := range vs {
		v.byID[x.ID] = x
	}
	return nil
}

func (v *scanVectors) Delete(_ context.Context, ids []contracts.Ulid) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, id := range ids {
		delete(v.byID, id)
	}
	return nil
}

func (v *scanVectors) Clear(context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.byID = map[contracts.Ulid]Vector{}
	return nil
}

func (v *scanVectors) Retrieve(_ context.Context, q VectorQuery) ([]VectorHit, error) {
	if len(q.Embedding) == 0 {
		return nil, fmt.Errorf("%w: 查询向量为空", ErrBadQuery)
	}
	if q.K < 0 {
		return nil, fmt.Errorf("%w: K 不能为负", ErrBadQuery)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	type scored struct {
		hit   VectorHit
		score float32
	}
	var all []scored
	for id, x := range v.byID {
		if len(x.Embedding) != len(q.Embedding) {
			continue
		}
		if !matchFilter(x, q.Filter) {
			continue
		}
		all = append(all, scored{
			hit:   VectorHit{ID: id, Score: cosine(q.Embedding, x.Embedding), Payload: x.Payload, SourceRef: x.SourceRef},
			score: cosine(q.Embedding, x.Embedding),
		})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].score != all[j].score {
			return all[i].score > all[j].score
		}
		return all[i].hit.ID < all[j].hit.ID // 同分确定性
	})
	k := q.K
	if k <= 0 {
		k = 10
	}
	if k > len(all) {
		k = len(all)
	}
	out := make([]VectorHit, 0, k)
	for i := 0; i < k; i++ {
		out = append(out, all[i].hit)
	}
	return out, nil
}

// ---- 向量替身 B：归一化存储 + 点积（内部结构**与算法**都不同于替身 A）----
//
// 替身 A 是"原样存 + 现算余弦"；替身 B 是"**存时就归一化** + 检索时点积"
// （归一化向量的点积 = 余弦：数学等价、浮点路径不同）。
// 换实现后**排序口径不变**（top-1 是同一条）——那才是消费方依赖的东西
// （Score 的**数值**跨实现不可比：接口契约明写"口径由实现定"，见 VectorHit.Score）。

type normVectors struct {
	mu   sync.RWMutex
	byID map[contracts.Ulid]normVector
}

type normVector struct {
	unit []float32 // 归一化后的向量（零向量保持零）
	orig Vector
}

func newNormVectors() *normVectors {
	return &normVectors{byID: map[contracts.Ulid]normVector{}}
}

func (v *normVectors) Upsert(_ context.Context, vs []Vector) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, x := range vs {
		v.byID[x.ID] = normVector{unit: normalizeVec(x.Embedding), orig: x}
	}
	return nil
}

func (v *normVectors) Delete(_ context.Context, ids []contracts.Ulid) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, id := range ids {
		delete(v.byID, id)
	}
	return nil
}

func (v *normVectors) Clear(context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.byID = map[contracts.Ulid]normVector{}
	return nil
}

func (v *normVectors) Retrieve(_ context.Context, q VectorQuery) ([]VectorHit, error) {
	if len(q.Embedding) == 0 {
		return nil, fmt.Errorf("%w: 查询向量为空", ErrBadQuery)
	}
	if q.K < 0 {
		return nil, fmt.Errorf("%w: K 不能为负", ErrBadQuery)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	unit := normalizeVec(q.Embedding)
	type scored struct {
		hit   VectorHit
		score float32
	}
	var all []scored
	for id, x := range v.byID {
		if len(x.unit) != len(unit) {
			continue
		}
		if !matchFilter(x.orig, q.Filter) {
			continue
		}
		all = append(all, scored{
			hit:   VectorHit{ID: id, Score: dot(unit, x.unit), Payload: x.orig.Payload, SourceRef: x.orig.SourceRef},
			score: dot(unit, x.unit),
		})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].score != all[j].score {
			return all[i].score > all[j].score
		}
		return all[i].hit.ID < all[j].hit.ID // 同分确定性
	})
	k := q.K
	if k <= 0 {
		k = 10
	}
	if k > len(all) {
		k = len(all)
	}
	out := make([]VectorHit, 0, k)
	for i := 0; i < k; i++ {
		out = append(out, all[i].hit)
	}
	return out, nil
}

// normalizeVec 归一化一个向量（零向量保持零）。
func normalizeVec(v []float32) []float32 {
	out := make([]float32, len(v))
	var sum float32
	for _, x := range v {
		sum += x * x
	}
	if sum <= 0 {
		copy(out, v)
		return out
	}
	inv := float32(1) / sqrt32(sum)
	for i, x := range v {
		out[i] = x * inv
	}
	return out
}

// dot 两个向量的点积（维度不同 → 0）。
func dot(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

func matchFilter(v Vector, filter map[string]string) bool {
	for k, want := range filter {
		var meta map[string]string
		if len(v.Payload) == 0 {
			return false
		}
		if err := json.Unmarshal(v.Payload, &meta); err != nil {
			return false
		}
		if meta[k] != want {
			return false
		}
	}
	return true
}

func cosine(a, b []float32) float32 {
	var dot, na, nb float32
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (sqrt32(na) * sqrt32(nb))
}

func sqrt32(x float32) float32 {
	// 小自写开方（避免为了一个测试替身引 math：math 在 core 里也不该出现）
	// ——牛顿迭代，10 轮对 float32 已足够（测试只比较排序，不校验数值精度）。
	if x <= 0 {
		return 0
	}
	guess := x
	for i := 0; i < 20; i++ {
		guess = (guess + x/guess) / 2
	}
	return guess
}

// ---------------------------------------------------------------------------
// 酸测试：换实现零改动
// ---------------------------------------------------------------------------

// auditReport 是消费者跑一遍的结论（纯数据，便于两套实现对答案）。
type auditReport struct {
	EventKind   string   `json:"event_kind"`
	EventSeq    int64    `json:"event_seq"`
	BinaryRef   string   `json:"binary_ref"`
	BlobText    string   `json:"blob_text"`
	BlobSize    int64    `json:"blob_size"`
	DocBody     string   `json:"doc_body"`
	DocRevision int64    `json:"doc_revision"`
	Neighbors   []string `json:"neighbors"`
	Reachable   []string `json:"reachable"`
	Direct      []string `json:"direct"`
	Escalate    bool     `json:"escalate"`
	TopVector   string   `json:"top_vector"`
	TopScore    float32  `json:"top_score"`
}

// auditConsumer 是**只认接口**的消费者（host / 索引器的缩影）。
//
// 它拿到的只有 Set（各族接口），不知道背后是 map、排序索引、分块还是追加日志。
// **这个函数在两次运行之间一字不改**——它就是"换实现零改动"的被测对象。
func auditConsumer(ctx context.Context, s Set) (auditReport, error) {
	if !s.HasEventLog() {
		return auditReport{}, errors.New("consumer: 事件日志是必填族（F5）")
	}
	var rep auditReport

	// ① blob：写大对象 → 引用挂到事件上（Event.binary_ref，D4/G2e）。
	if s.Blob == nil {
		return rep, errors.New("consumer: blob 族未挂（nil 槽位）")
	}
	big := strings.Repeat("alasol-blob-", 64) // 远超一行事件该有的大小
	meta, err := s.Blob.Put(ctx, BlobMeta{MediaType: "text/plain", Name: "tool-output.txt"},
		strings.NewReader(big))
	if err != nil {
		return rep, err
	}
	rep.BinaryRef, rep.BlobSize = string(meta.ID), meta.Size

	// ② 事件日志：落一条带 binary_ref 的 durable 事件，再按 seq 读回。
	ev := contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: "tool.call.completed",
		TS: "2026-01-29T12:00:00.000Z", SessionID: contracts.Ulid(core.NewUlid()),
		AgentID: contracts.Ulid(core.NewUlid()), Durable: true,
		Payload:   json.RawMessage(`{"tool_call_id":"toolu_01ACID","result":{"content":[{"type":"text","text":"ok"}]}}`),
		BinaryRef: meta.Ref(),
	}
	stored, err := s.EventLog.Append(ctx, []contracts.Event{ev})
	if err != nil {
		return rep, err
	}
	if len(stored) != 1 || stored[0].Seq == nil {
		return rep, errors.New("consumer: Append 应返回带 seq 的事件")
	}
	rep.EventKind, rep.EventSeq = stored[0].Kind, *stored[0].Seq
	back, err := s.EventLog.ReadFrom(ctx, nil, *stored[0].Seq, 0)
	if err != nil || len(back) != 1 || back[0].BinaryRef == nil {
		return rep, fmt.Errorf("consumer: 读回事件失败（err=%v, n=%d）", err, len(back))
	}
	if string(*back[0].BinaryRef) != rep.BinaryRef {
		return rep, errors.New("consumer: binary_ref 往返不一致")
	}
	rc, err := s.Blob.Get(ctx, *back[0].BinaryRef)
	if err != nil {
		return rep, err
	}
	raw, err := io.ReadAll(rc)
	if err != nil {
		return rep, err
	}
	if err := rc.Close(); err != nil {
		return rep, err
	}
	rep.BlobText = string(raw)

	// ③ 文档：写面（走 Tx）→ 读面读回。
	//
	// 写面是**可选能力**（G17：写面只归索引器写）——取写面必须带 ok：
	// 硬转型（s.Documents.(DocumentProjection)）在"只挂读面"的合法组合上直接 panic（p2-3）。
	if s.Documents == nil {
		return rep, errors.New("consumer: 文档族未挂（nil 槽位）")
	}
	docWriter, ok := s.DocumentWriter()
	if !ok {
		return rep, errors.New("consumer: 文档族没有写面（只挂了读面）")
	}
	tx := &txRecorder{}
	if err := docWriter.EnsureSchema(ctx); err != nil {
		return rep, err
	}
	docBody := json.RawMessage(`{"rule":"只增不删","scope":"contracts"}`)
	if err := docWriter.Upsert(ctx, tx, []Doc{{
		Collection: "project_rules", Key: "f7", Body: docBody, Revision: 1, UpdatedSeq: rep.EventSeq,
	}}); err != nil {
		return rep, err
	}
	doc, err := s.Documents.Get(ctx, "project_rules", "f7")
	if err != nil {
		return rep, err
	}
	rep.DocBody, rep.DocRevision = string(doc.Body), doc.Revision

	// ④ 图：写面建边 → 读面查一跳邻居 + 影响面（G21/W16 用途链的读侧）。
	if s.Graph == nil {
		return rep, errors.New("consumer: 图族未挂（nil 槽位）")
	}
	graphWriter, ok := s.GraphWriter()
	if !ok {
		return rep, errors.New("consumer: 图族没有写面（只挂了读面）")
	}
	edges := []contracts.GraphEdge{
		{Src: "core/store.go", Dst: "runtime/store/sqlite", Kind: "imports"},
		{Src: "host/hub.go", Dst: "core/store.go", Kind: "imports"},
		{Src: "cmd/alasol/main.go", Dst: "host/hub.go", Kind: "imports"},
	}
	if err := graphWriter.ApplyEdges(ctx, tx, rep.EventSeq, edges); err != nil {
		return rep, err
	}
	ns, err := s.Graph.Neighbors(ctx, coregraph.NeighborQuery{
		Node: "core/store.go", Kind: "imports", Direction: coregraph.DirectionIn, Hops: 1,
	})
	if err != nil {
		return rep, err
	}
	rep.Neighbors = ns
	impact, err := s.Graph.Impact(ctx, coregraph.ImpactRequest{
		Roots: []string{"core/store.go"}, Kind: "imports", MaxDepth: 4,
		Core: coregraph.ParseCoreSet([]string{"core/*"}),
	})
	if err != nil {
		return rep, err
	}
	rep.Reachable = append(append([]string(nil), impact.Direct...), impact.Transitive...)
	sort.Strings(rep.Reachable)
	rep.Direct = impact.Direct
	rep.Escalate = impact.Escalate

	// ⑤ 向量：写入 → top-1 检索（G2d RAG 保留位的读侧）。
	if s.Vectors == nil {
		return rep, errors.New("consumer: 向量族未挂（nil 槽位）")
	}
	vecWriter, ok := s.VectorWriter()
	if !ok {
		return rep, errors.New("consumer: 向量族没有写面（只挂了读面）")
	}
	vid := contracts.Ulid(core.NewUlid())
	if err := vecWriter.Upsert(ctx, []Vector{{
		ID: vid, Embedding: []float32{1, 0, 0}, Payload: json.RawMessage(`{"kind":"doc","path":"docs/07.md"}`),
		SourceRef: "e:1",
	}, {
		ID: contracts.Ulid(core.NewUlid()), Embedding: []float32{0, 1, 0},
		Payload: json.RawMessage(`{"kind":"code","path":"core/store.go"}`), SourceRef: "e:2",
	}}); err != nil {
		return rep, err
	}
	hits, err := s.Vectors.Retrieve(ctx, VectorQuery{Embedding: []float32{0.9, 0.1, 0}, K: 1})
	if err != nil {
		return rep, err
	}
	if len(hits) != 1 {
		return rep, fmt.Errorf("consumer: top-1 应返回 1 条，得到 %d", len(hits))
	}
	if hits[0].ID != vid {
		return rep, errors.New("consumer: top-1 命中错误（余弦排序不对）")
	}
	rep.TopVector = string(hits[0].ID)
	rep.TopScore = hits[0].Score
	return rep, nil
}

// TestSwapImplementationsZeroConsumerChange 是 G2 的**换实现零改动酸测试**：
// 同一份消费者代码跑在两套内部结构完全不同的实现上，结论逐字段一致。
//
// **五族都换**（F6）：事件日志（map ↔ 追加日志）、图（邻接 map ↔ 倒排索引）、
// 文档（map ↔ 追加日志）、blob（整块 ↔ 7 字节分块）、向量（现算余弦 ↔ 归一化点积）。
// 之前只换了图/文档/blob 三族——事件日志与向量族一直是同一套实现，等于没测。
func TestSwapImplementationsZeroConsumerChange(t *testing.T) {
	ctx := context.Background()

	// 实现族 A：邻接 map 图 + 整块 blob + map 文档 + map 事件日志 + 现算余弦向量。
	setA := Set{
		EventLog:  newMemEventLog(),
		Graph:     &mapGraph{},
		Documents: newMapDocs(),
		Blob:      newMapBlob(),
		Vectors:   newScanVectors(),
	}
	// 实现族 B：倒排索引图 + 7 字节分块 blob + 追加日志文档 + 追加日志事件日志 +
	// 归一化点积向量。
	setB := Set{
		EventLog:  newLogEventLog(),
		Graph:     &indexedGraph{},
		Documents: newLogDocs(),
		Blob:      newChunkBlob(7),
		Vectors:   newNormVectors(),
	}

	// 防"两族退回同一实现"（p3-D）：每个槽位的**具体类型**必须不同——否则那一族
	// 变成"同一实现跑两遍"，什么都没测出来，结论却照样"一致"（假绿）。
	assertDistinctImplTypes(t, setA, setB)

	repA, err := auditConsumer(ctx, setA)
	if err != nil {
		t.Fatalf("实现族 A: %v", err)
	}
	repB, err := auditConsumer(ctx, setB)
	if err != nil {
		t.Fatalf("实现族 B: %v", err)
	}

	// 逐字段比对（seq / blob id / vector id 是每次运行新发的，除外）：
	// 用 JSON 比对而不是 == ——报告里带切片（邻居/可达集），不可直接比较。
	repA.EventSeq, repB.EventSeq = 0, 0
	repA.BinaryRef, repB.BinaryRef = "", ""
	repA.TopVector, repB.TopVector = "", ""
	// Score 的**数值**跨实现不可比（接口契约明写"口径由实现定"：余弦/内积/距离取反
	// ——归一化点积与现算余弦数学等价但浮点路径不同）→ 比对里排除；
	// **排序**仍由消费者自己断言（top-1 必须是同一条，见 auditConsumer）。
	repA.TopScore, repB.TopScore = 0, 0
	ja, err := json.Marshal(repA)
	if err != nil {
		t.Fatalf("marshal A: %v", err)
	}
	jb, err := json.Marshal(repB)
	if err != nil {
		t.Fatalf("marshal B: %v", err)
	}
	if string(ja) != string(jb) {
		t.Fatalf("换实现后消费者结论变了（零改动酸测试失败）:\nA=%s\nB=%s", ja, jb)
	}
	if repA.BlobSize != int64(len(strings.Repeat("alasol-blob-", 64))) {
		t.Fatalf("blob 大小不对: %d", repA.BlobSize)
	}
	if repA.BlobText != strings.Repeat("alasol-blob-", 64) {
		t.Fatalf("blob 内容往返不一致（分块实现丢了字节？）")
	}
	if !repA.Escalate {
		t.Fatalf("触及核心接口应建议 escalate（G21/W16）: %+v", repA)
	}
}

// TestSetWithoutWriteFacesIsNotPanic（p2-3 回归）：只挂**读面**的组合是合法的
// （G17：写面只归索引器写；只读实现天然没有写面）——消费方取写面时必须拿到
// ok=false，**不许 panic**（旧口径硬转型 `s.Graph.(GraphProjection)` 直接崩）。
func TestSetWithoutWriteFacesIsNotPanic(t *testing.T) {
	ctx := context.Background()
	readOnly := Set{
		EventLog:  newMemEventLog(),
		Graph:     readOnlyGraph{g: &mapGraph{}},
		Documents: readOnlyDocs{d: newMapDocs()},
		Blob:      newMapBlob(),
		Vectors:   readOnlyVectors{v: newScanVectors()},
	}
	if _, ok := readOnly.GraphWriter(); ok {
		t.Fatal("只读实现不该给出图族写面")
	}
	if _, ok := readOnly.DocumentWriter(); ok {
		t.Fatal("只读实现不该给出文档族写面")
	}
	if _, ok := readOnly.VectorWriter(); ok {
		t.Fatal("只读实现不该给出向量族写面")
	}
	// 消费者遇到"没有写面"要**明确报错**，不是 panic（组合面允许只挂读面）。
	if _, err := auditConsumer(ctx, readOnly); err == nil {
		t.Fatal("缺写面时消费者应报错（而不是 panic）")
	}
	// 空槽位（族没挂）同样不许 panic。
	empty := Set{EventLog: newMemEventLog()}
	if _, ok := empty.GraphWriter(); ok {
		t.Fatal("未挂的族不该给出写面")
	}
	if _, err := auditConsumer(ctx, empty); err == nil {
		t.Fatal("未挂的族应报错（而不是 panic）")
	}
	// 反面：挂了写面的实现，取面必须成功（别把"取不到"写成常态）。
	full := Set{
		EventLog:  newMemEventLog(),
		Graph:     &mapGraph{},
		Documents: newMapDocs(),
		Blob:      newMapBlob(),
		Vectors:   newScanVectors(),
	}
	if _, ok := full.GraphWriter(); !ok {
		t.Fatal("提供写面的实现应能取到图族写面")
	}
	if _, ok := full.DocumentWriter(); !ok {
		t.Fatal("提供写面的实现应能取到文档族写面")
	}
	if _, ok := full.VectorWriter(); !ok {
		t.Fatal("提供写面的实现应能取到向量族写面")
	}
}

// TestDocumentProjectionRejectsWholeBatch（F3 回归）：文档写面的口径是"**整批拒绝**"——
// 一批里有一条冲突，这一批**一条都不许写**。
//
// 旧替身边写边判冲突：前半批已经落库、后半批才报冲突 → 调用方拿到错误却面对
// "半个真相"（写了一半），比全拒更坏（K24 同款口径：宁可全拒不可半成）。
func TestDocumentProjectionRejectsWholeBatch(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		impl interface {
			DocumentProjection
			Documents
		}
	}{
		{"map 文档", newMapDocs()},
		{"追加日志文档", newLogDocs()},
	} {
		tx := &txRecorder{}
		if err := tc.impl.EnsureSchema(ctx); err != nil {
			t.Fatalf("%s: EnsureSchema: %v", tc.name, err)
		}
		if err := tc.impl.Upsert(ctx, tx, []Doc{{Collection: "c", Key: "ok", Revision: 1}}); err != nil {
			t.Fatalf("%s: Upsert: %v", tc.name, err)
		}
		// 一批里第二条与库里现存版本冲突 → 整批拒绝。
		err := tc.impl.Upsert(ctx, tx, []Doc{
			{Collection: "c", Key: "fresh", Revision: 1},
			{Collection: "c", Key: "ok", Revision: 7},
		})
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("%s: 冲突批次应 ErrConflict，得到 %v", tc.name, err)
		}
		// **冲突批次不留任何写入**：新键不该出现。
		if _, err := tc.impl.Get(ctx, "c", "fresh"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s: 冲突批次不该留下任何写入（fresh 不该存在），得到 %v", tc.name, err)
		}
		// 已有文档也不能被这批改掉（版本仍是 1）。
		doc, err := tc.impl.Get(ctx, "c", "ok")
		if err != nil {
			t.Fatalf("%s: Get(ok): %v", tc.name, err)
		}
		if doc.Revision != 1 {
			t.Fatalf("%s: 冲突批次不该改动已有文档（版本仍应是 1），得到 %d", tc.name, doc.Revision)
		}
		// 批内冲突（同键不同版本）同样整批拒绝。
		err = tc.impl.Upsert(ctx, tx, []Doc{
			{Collection: "c", Key: "dup", Revision: 1},
			{Collection: "c", Key: "dup", Revision: 2},
		})
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("%s: 批内冲突应 ErrConflict，得到 %v", tc.name, err)
		}
		if _, err := tc.impl.Get(ctx, "c", "dup"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s: 批内冲突不该留下任何写入，得到 %v", tc.name, err)
		}
	}
}

// TestDocumentProjectionDeleteThenUpsert（p3-B 回归）：**删掉之后再写**同一键必须成功
// （删除 = 没有现存版本，不是"版本 0 的旧文档"）——两套替身口径必须一致。
//
// 旧口径：logDocs 的墓碑（Delete 清空内容留行）参与版本比对 → 墓碑的 Revision=0
// 与新写的 Revision=1 不等 → 误报 ErrConflict；而 mapDocs 直接摘键 → 不冲突。
// 两套替身对同一条契约给出不同答案 = 契约没被钉住。
func TestDocumentProjectionDeleteThenUpsert(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		impl interface {
			DocumentProjection
			Documents
		}
	}{
		{"map 文档", newMapDocs()},
		{"追加日志文档", newLogDocs()},
	} {
		tx := &txRecorder{}
		if err := tc.impl.EnsureSchema(ctx); err != nil {
			t.Fatalf("%s: EnsureSchema: %v", tc.name, err)
		}
		if err := tc.impl.Upsert(ctx, tx, []Doc{{Collection: "c", Key: "k", Revision: 1}}); err != nil {
			t.Fatalf("%s: Upsert: %v", tc.name, err)
		}
		// 删掉。
		if err := tc.impl.Delete(ctx, tx, "c", []string{"k"}); err != nil {
			t.Fatalf("%s: Delete: %v", tc.name, err)
		}
		if _, err := tc.impl.Get(ctx, "c", "k"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s: 删后不该读得到，得到 %v", tc.name, err)
		}
		// 删掉之后再写同一键 → 必须成功（没有现存版本，不是冲突）。
		if err := tc.impl.Upsert(ctx, tx, []Doc{{Collection: "c", Key: "k", Revision: 2}}); err != nil {
			t.Fatalf("%s: 删后再写不该冲突（删除 = 没有现存版本），得到 %v", tc.name, err)
		}
		doc, err := tc.impl.Get(ctx, "c", "k")
		if err != nil {
			t.Fatalf("%s: 删后再写应读得到: %v", tc.name, err)
		}
		if doc.Revision != 2 {
			t.Fatalf("%s: 删后再写的版本应是 2，得到 %d", tc.name, doc.Revision)
		}
		// 反面：没删的时候版本不同 → 仍然冲突（别把防线一起拆了）。
		if err := tc.impl.Upsert(ctx, tx, []Doc{{Collection: "c", Key: "k", Revision: 7}}); !errors.Is(err, ErrConflict) {
			t.Fatalf("%s: 未删时版本不同应 ErrConflict，得到 %v", tc.name, err)
		}
	}
}

// assertDistinctImplTypes 断言两套 Set 的**每个槽位**用的是**不同**的具体实现。
//
// 为什么要有这条（p3-D）：酸测试的价值全在"两套实现不同"上——某一族悄悄退回同一套
// 实现（例如两边的图都用 mapGraph），那条族就变成"同一实现跑两遍"，什么都没测出来，
// 而结论照样"一致"（假绿）。
func assertDistinctImplTypes(t *testing.T, a, b Set) {
	t.Helper()
	pairs := []struct {
		family string
		x, y   any
	}{
		{"eventlog", a.EventLog, b.EventLog},
		{"graph", a.Graph, b.Graph},
		{"documents", a.Documents, b.Documents},
		{"blob", a.Blob, b.Blob},
		{"vectors", a.Vectors, b.Vectors},
	}
	for _, p := range pairs {
		tx, ty := reflect.TypeOf(p.x), reflect.TypeOf(p.y)
		if tx == nil || ty == nil {
			t.Fatalf("%s 族有空槽位（酸测试必须两套都挂上）", p.family)
		}
		if tx == ty {
			t.Fatalf("%s 族两套实现是同一个具体类型 %v（酸测试对这一族等于没测）",
				p.family, tx)
		}
	}
}

// TestSetKindsAndOptionalFamilies 钉死"可组合"的形状：只挂事件日志 = 与 v0/v1 等价。
func TestSetKindsAndOptionalFamilies(t *testing.T) {
	bare := Set{EventLog: newMemEventLog()}
	if got := bare.Kinds(); len(got) != 1 || got[0] != "eventlog" {
		t.Fatalf("只挂事件日志时 Kinds=%v，want [eventlog]", got)
	}
	if !bare.HasEventLog() {
		t.Fatal("HasEventLog 应为 true")
	}
	var empty Set
	if empty.HasEventLog() {
		t.Fatal("空组合不该报告有事件日志（F5：没有事实源的组合不合法）")
	}
	if got := empty.Kinds(); len(got) != 0 {
		t.Fatalf("空组合 Kinds=%v，want []", got)
	}
	full := Set{
		EventLog: newMemEventLog(), Graph: &mapGraph{}, Documents: newMapDocs(),
		Blob: newMapBlob(), Vectors: newScanVectors(),
	}
	want := []string{"eventlog", "graph", "documents", "blob", "vectors"}
	got := full.Kinds()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("Kinds=%v，want %v", got, want)
	}
}

// TestBlobPutIdempotentAndConflict 钉死 blob 的写入口径（G2e + 幂等纪律）。
func TestBlobPutIdempotentAndConflict(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		blob Blob
	}{
		{"整块实现", newMapBlob()},
		{"分块实现", newChunkBlob(5)},
	} {
		id := contracts.Ulid(core.NewUlid())
		first, err := tc.blob.Put(ctx, BlobMeta{ID: id, MediaType: "text/plain"}, strings.NewReader("hello"))
		if err != nil {
			t.Fatalf("%s: Put: %v", tc.name, err)
		}
		if first.Size != 5 {
			t.Fatalf("%s: Size=%d，want 5", tc.name, first.Size)
		}
		if first.Ref() == nil || string(*first.Ref()) != string(id) {
			t.Fatalf("%s: Ref() 应回指 id（binary_ref 挂接点）", tc.name)
		}
		// 同 id 同内容 → 幂等（不写第二份）。
		again, err := tc.blob.Put(ctx, BlobMeta{ID: id, MediaType: "text/plain"}, strings.NewReader("hello"))
		if err != nil {
			t.Fatalf("%s: 同内容重复 Put 不该报错: %v", tc.name, err)
		}
		if again.Size != first.Size {
			t.Fatalf("%s: 幂等 Put 改变了大小", tc.name)
		}
		// 同 id 不同内容 → 冲突（不许静默覆盖：引用可能已落库）。
		if _, err := tc.blob.Put(ctx, BlobMeta{ID: id}, strings.NewReader("other!")); !errors.Is(err, ErrConflict) {
			t.Fatalf("%s: 同 id 不同内容应 ErrConflict，得到 %v", tc.name, err)
		}
		// 取不到 → ErrNotFound（fail-close，不返回空内容）。
		if _, err := tc.blob.Get(ctx, contracts.Ulid(core.NewUlid())); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s: 不存在的 blob 应 ErrNotFound，得到 %v", tc.name, err)
		}
		// 删除幂等。
		if err := tc.blob.Delete(ctx, id); err != nil {
			t.Fatalf("%s: Delete: %v", tc.name, err)
		}
		if err := tc.blob.Delete(ctx, id); err != nil {
			t.Fatalf("%s: 重复 Delete 应幂等: %v", tc.name, err)
		}
		if _, err := tc.blob.Stat(ctx, id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s: 删后 Stat 应 ErrNotFound，得到 %v", tc.name, err)
		}
	}
}

// TestBlobMetaRefEmpty 钉死引用挂接点的空值语义（空 id → 不外置）。
func TestBlobMetaRefEmpty(t *testing.T) {
	if (BlobMeta{}).Ref() != nil {
		t.Fatal("空 id 的 Ref() 应为 nil（事件不带 binary_ref）")
	}
	m := BlobMeta{ID: contracts.Ulid(core.NewUlid())}
	if m.Ref() == nil || string(*m.Ref()) != string(m.ID) {
		t.Fatal("非空 id 的 Ref() 应回指该 id")
	}
}

// TestVectorRetrieverContract 钉死向量检索面的口径（top-k / 过滤 / 诚实降级）。
func TestVectorRetrieverContract(t *testing.T) {
	ctx := context.Background()
	v := newScanVectors()
	if err := v.Upsert(ctx, []Vector{
		{ID: contracts.Ulid(core.NewUlid()), Embedding: []float32{1, 0, 0}, SourceRef: "e:1"},
		{ID: contracts.Ulid(core.NewUlid()), Embedding: []float32{0, 1, 0}, SourceRef: "e:2"},
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	hits, err := v.Retrieve(ctx, VectorQuery{Embedding: []float32{1, 0, 0}, K: 5})
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if len(hits) != 2 || hits[0].SourceRef != "e:1" {
		t.Fatalf("top-k 排序/回显不对: %+v", hits)
	}
	// 空查询向量 / 负 K → ErrBadQuery（fail-close，不返回"空结果"冒充"没有相似内容"）。
	if _, err := v.Retrieve(ctx, VectorQuery{}); !errors.Is(err, ErrBadQuery) {
		t.Fatalf("空查询向量应 ErrBadQuery，得到 %v", err)
	}
	if _, err := v.Retrieve(ctx, VectorQuery{Embedding: []float32{1}, K: -1}); !errors.Is(err, ErrBadQuery) {
		t.Fatalf("负 K 应 ErrBadQuery，得到 %v", err)
	}
}

// TestWriteFaceLifecycleSharesTx：写面的生命周期形状（建表 → 经共享 Tx 写 → 清空）
// 在**每一族**上都成立，且写必须走各族共用的 Tx 面（G2b：不泄漏引擎特性）。
//
// 为什么值得单测：换实现时最容易悄悄退化的就是这条——实现如果改成"自己开事务/自己落库"，
// 消费方（索引器）的"投影 + 水位同事务原子"（G24）就被绕过去了，而接口签名看不出来。
func TestWriteFaceLifecycleSharesTx(t *testing.T) {
	ctx := context.Background()

	// 图族：EnsureSchema → ApplyEdges(经 Tx) → 读面看得见 → Clear(经 Tx) → 读面空了。
	for _, tc := range []struct {
		name string
		impl interface {
			GraphProjection
			Graph
		}
	}{
		{"邻接 map 图", &mapGraph{}},
		{"倒排索引图", &indexedGraph{}},
	} {
		tx := &txRecorder{}
		if err := tc.impl.EnsureSchema(ctx); err != nil {
			t.Fatalf("%s: EnsureSchema: %v", tc.name, err)
		}
		if err := tc.impl.ApplyEdges(ctx, tx, 1, []contracts.GraphEdge{
			{Src: "a", Dst: "b", Kind: "imports"},
		}); err != nil {
			t.Fatalf("%s: ApplyEdges: %v", tc.name, err)
		}
		if n := tx.count(); n == 0 {
			t.Fatalf("%s: 图写面必须经调用方传入的 Tx（共享事务面），一次都没用", tc.name)
		}
		in, err := tc.impl.Neighbors(ctx, coregraph.NeighborQuery{
			Node: "b", Kind: "imports", Direction: coregraph.DirectionIn, Hops: 1,
		})
		if err != nil || len(in) != 1 || in[0] != "a" {
			t.Fatalf("%s: 写完应能入向查到 a: %v err=%v", tc.name, in, err)
		}
		if err := tc.impl.Clear(ctx, tx); err != nil {
			t.Fatalf("%s: Clear: %v", tc.name, err)
		}
		in, err = tc.impl.Neighbors(ctx, coregraph.NeighborQuery{
			Node: "b", Kind: "imports", Direction: coregraph.DirectionIn, Hops: 1,
		})
		if err != nil || len(in) != 0 {
			t.Fatalf("%s: Clear 后入向应为空: %v err=%v", tc.name, in, err)
		}
	}

	// 文档族：同款生命周期（Upsert → Get → Clear → Get 不到）。
	for _, tc := range []struct {
		name string
		impl interface {
			DocumentProjection
			Documents
		}
	}{
		{"map 文档", newMapDocs()},
		{"追加日志文档", newLogDocs()},
	} {
		tx := &txRecorder{}
		if err := tc.impl.EnsureSchema(ctx); err != nil {
			t.Fatalf("%s: EnsureSchema: %v", tc.name, err)
		}
		if err := tc.impl.Upsert(ctx, tx, []Doc{{Collection: "c", Key: "k", Revision: 1}}); err != nil {
			t.Fatalf("%s: Upsert: %v", tc.name, err)
		}
		if n := tx.count(); n == 0 {
			t.Fatalf("%s: 文档写面也必须经 Tx", tc.name)
		}
		if _, err := tc.impl.Get(ctx, "c", "k"); err != nil {
			t.Fatalf("%s: 写完应读得到: %v", tc.name, err)
		}
		if err := tc.impl.Clear(ctx, tx); err != nil {
			t.Fatalf("%s: Clear: %v", tc.name, err)
		}
		if _, err := tc.impl.Get(ctx, "c", "k"); err == nil {
			t.Fatalf("%s: Clear 后不该读得到", tc.name)
		}
	}
}
