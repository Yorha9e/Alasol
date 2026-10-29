package graph

import (
	"strconv"
	"strings"
	"sync"
)

// ClosureKey 是闭包缓存的键：`(起点集, kind, 方向, 深度, 结果上限)`（G20）。
//
// 起点集是**集合语义**（排序后 join）：`[A,B]` 与 `[B,A]` 命中同一条缓存——
// 同一批改动的 diff 顺序不该影响缓存命中。
type ClosureKey struct {
	Sources    string
	Kind       string
	Direction  Direction
	MaxDepth   int
	MaxResults int
}

// NormalizeReach 归一化 ReachQuery：上限 0 → 默认、负值 → 拒、起点去重去空排序。
//
// **必须在折缓存键之前调用**：否则 `MaxDepth=0`（默认 8）与 `MaxDepth=8` 会算成两个键，
// 同一查询因写法不同反复 miss。
func NormalizeReach(q ReachQuery) (ReachQuery, error) {
	out := q
	if out.MaxDepth < 0 {
		return ReachQuery{}, ErrBadLimits
	}
	if out.MaxResults < 0 {
		return ReachQuery{}, ErrBadLimits
	}
	if out.MaxDepth == 0 {
		out.MaxDepth = DefaultMaxDepth
	}
	if out.MaxResults == 0 {
		out.MaxResults = DefaultMaxResults
	}
	srcs := dedupSorted(out.Sources)
	if len(srcs) == 0 {
		return ReachQuery{}, ErrNoSources
	}
	out.Sources = srcs
	return out, nil
}

// ClosureKeyOf 由查询条件折成缓存键。
//
// 起点集在**这里**排序后 join（集合语义：`[A,B]` 与 `[B,A]` 同键）——
// 不依赖调用方先归一化，免得"同一次改动因 diff 顺序不同而反复 miss"。
func ClosureKeyOf(q ReachQuery) ClosureKey {
	return ClosureKey{
		Sources:    sortedJoin(q.Sources),
		Kind:       q.Kind,
		Direction:  q.Direction,
		MaxDepth:   q.MaxDepth,
		MaxResults: q.MaxResults,
	}
}

// cacheEntry 是缓存里的一条闭包结果，**带它被写入时的代号**（epoch）。
//
// 为什么带代号（F3）：读侧是"先 Get → 再算 → 再 Put"。若这期间图被写入并失效了缓存，
// 这次 Put 会把**过期闭包**写回缓存（写者以为清干净了，实际刚又塞进去一条旧的）。
// 代号让这种"读旧值 → 写新值"的竞态露出来：Put 时代号对不上就**不写**。
type cacheEntry struct {
	// epoch 是写入时的代号（见 ClosureCache.Epoch）。
	epoch uint64
	// closure 是缓存的闭包结果。
	closure Closure
}

// ClosureCache 是闭包结果缓存（G20 **简版**：内存 map + 图写入失效，不做复杂淘汰）。
//
// 取舍（写在这里免得后人误加 LRU）：淘汰策略会让"同一查询两次答案不同"——
// 对 change_impact 这种要给 escalate 闸门当依据的东西，**确定性比命中率重要**。
// 所以简版就是"写入即整表清空"；热点查询天然集中在最近一次改动上，清空的代价可接受。
//
// # 代号（epoch）护栏（F3）
//
// 每次**外部写入**（Invalidate）都把代号 +1。条目记着写入时的代号：
//   - Get 只认**当前代号**的条目（过期条目当没命中）；
//   - Put 只在"写入者手里的代号 == 当前代号"时才落盘——竞态窗口里算出来的旧闭包
//     因此**写不进去**（宁可多算一次 BFS，也不能给 escalate 闸门一份过期答案）。
//
// 读侧自己同步到的新边走 Clear（清条目、**不动代号**）：那份闭包正是按同步后的
// 状态算的，不该被自己的同步挡住写回。
type ClosureCache struct {
	mu      sync.Mutex
	entries map[ClosureKey]cacheEntry
	// epoch 是失效代号（Invalidate 自增）：条目与它对不上就是过期的。
	epoch  uint64
	hits   int64
	misses int64
}

// NewClosureCache 建一个空缓存。
func NewClosureCache() *ClosureCache {
	return &ClosureCache{entries: make(map[ClosureKey]cacheEntry)}
}

// Epoch 返回当前代号（读侧算完闭包后拿它去 Put：代号变了就说明期间有写入，别写）。
func (c *ClosureCache) Epoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

// Get 查缓存（同时累计 hits/misses，供 Stats 与测试断言）。
//
// **只认当前代号的条目**：过期条目（写入后发生过失效）当没命中——宁可重算，
// 也不能把过期闭包当新答案返回（G20/F3）。
func (c *ClosureCache) Get(k ClosureKey) (Closure, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[k]
	if !ok || e.epoch != c.epoch {
		c.misses++
		return Closure{}, false
	}
	c.hits++
	return e.closure, true
}

// Put 写缓存：**只有代号仍然一致时才写**（返回是否真的落盘）。
//
// epoch 是调用方在**开始算之前**取的代号（见 Epoch）：期间若有写入发生（代号变了），
// 这次算出来的闭包可能已经过期 → 不写（下次重算，宁慢不错）。
func (c *ClosureCache) Put(k ClosureKey, v Closure, epoch uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != c.epoch {
		return false // 期间发生过失效：这份闭包可能过期 → 丢弃
	}
	c.entries[k] = cacheEntry{epoch: epoch, closure: v}
	return true
}

// Invalidate 失效全部条目并**推进代号**（G20：图写入后缓存必须失效）。
//
// 代号 = "**外部**写入发生过几次"：读侧拿它当护栏——算闭包之前取一次，算完比对，
// 不一致就不写回（免得把过期闭包塞回缓存）。**读侧自己同步到的新边不算**
// （那是读侧自己的状态，闭包就是按它算的 → 用 Clear，不动代号）。
func (c *ClosureCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epoch++
	c.entries = make(map[ClosureKey]cacheEntry)
}

// Clear 清空条目但**不推进代号**（读侧同步到新边时用：条目过期了，但闭包是按
// 同步后的状态算的 → 允许写回）。
func (c *ClosureCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[ClosureKey]cacheEntry)
}

// Len / Hits / Misses 是统计面。
func (c *ClosureCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

func (c *ClosureCache) Hits() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits
}

func (c *ClosureCache) Misses() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.misses
}

// sortedJoin 把起点集合折成缓存键里的字符串（导出仅用于测试/诊断）。
//
// **长度前缀 join**（F10）：分隔符 join 会撞键——`["a\x00b"]` 与 `["a","b"]` 折出同一个串，
// 于是"一个含 NUL 的节点 id"和"两个节点"共用一条缓存条目（答案可能完全不同）。
// 长度前缀让编码**无歧义**：`len(s) ":" s` 逐段拼，段边界由长度决定。
//
// **集合语义**：去重 + 排序（`[A,B]` 与 `[B,A,A]` 同键）——与 NormalizeReach 的
// 起点归一一致（不依赖调用方先归一化）。
func sortedJoin(sources []string) string {
	cp := dedupSorted(sources)
	var sb strings.Builder
	for _, s := range cp {
		sb.WriteString(strconv.Itoa(len(s)))
		sb.WriteByte(':')
		sb.WriteString(s)
	}
	return sb.String()
}
