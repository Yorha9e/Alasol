package graph

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// diamond 造一个菱形依赖图（kind 统一 "imports"）：
//
//	app → liba → core → util
//	app → libb → core
//
// 出向（谁依赖谁）：app 依赖 liba/libb，二者都依赖 core，core 依赖 util。
// 入向（谁依赖我）：core 的被依赖者 = liba/libb → app。
func diamond(t *testing.T) *MemGraph {
	t.Helper()
	g, err := NewMemGraph(DefaultLimits())
	if err != nil {
		t.Fatalf("NewMemGraph: %v", err)
	}
	edges := [][3]string{
		{"app", "liba", "imports"},
		{"app", "libb", "imports"},
		{"liba", "core", "imports"},
		{"libb", "core", "imports"},
		{"core", "util", "imports"},
	}
	for _, e := range edges {
		if err := g.AddEdge(e[0], e[1], e[2]); err != nil {
			t.Fatalf("AddEdge(%v): %v", e, err)
		}
	}
	return g
}

// TestMemGraphReachDiamond：菱形依赖的多跳 BFS（层序、深度、方向都正确）。
func TestMemGraphReachDiamond(t *testing.T) {
	g := diamond(t)
	ctx := context.Background()

	// 出向：app 的下游 = liba/libb（1 跳）→ core（2 跳）→ util（3 跳）。
	out, err := g.Reach(ctx, []string{"app"}, "imports", DirectionOut, 0, 0)
	if err != nil {
		t.Fatalf("Reach out: %v", err)
	}
	if out.Depth != 3 {
		t.Errorf("Depth = %d, want 3", out.Depth)
	}
	want := []string{"liba", "libb", "core", "util"}
	if strings.Join(out.Nodes, ",") != strings.Join(want, ",") {
		t.Errorf("Nodes = %v, want %v", out.Nodes, want)
	}
	if len(out.Levels) != 3 {
		t.Fatalf("Levels = %d 层, want 3", len(out.Levels))
	}
	if strings.Join(out.Levels[0], ",") != "liba,libb" {
		t.Errorf("Levels[0] = %v, want [liba libb]", out.Levels[0])
	}
	if out.Truncated {
		t.Error("Truncated = true, want false")
	}
	if len(out.Roots) != 1 || out.Roots[0] != "app" {
		t.Errorf("Roots = %v, want [app]", out.Roots)
	}

	// 入向：core 的被依赖者 = liba/libb（1 跳）→ app（2 跳）；util 不在内（它被 core 依赖）。
	in, err := g.Reach(ctx, []string{"core"}, "imports", DirectionIn, 0, 0)
	if err != nil {
		t.Fatalf("Reach in: %v", err)
	}
	if in.Depth != 2 {
		t.Errorf("Depth = %d, want 2", in.Depth)
	}
	if strings.Join(in.Nodes, ",") != "liba,libb,app" {
		t.Errorf("Nodes = %v, want [liba libb app]", in.Nodes)
	}
}

// TestMemGraphReachCycle：环不会让遍历打结，且**起点不进结果**。
func TestMemGraphReachCycle(t *testing.T) {
	g, err := NewMemGraph(DefaultLimits())
	if err != nil {
		t.Fatalf("NewMemGraph: %v", err)
	}
	// 两环：A→B→A，外加自环 C→C。
	for _, e := range [][3]string{{"A", "B", "imports"}, {"B", "A", "imports"}, {"C", "C", "imports"}} {
		if err := g.AddEdge(e[0], e[1], e[2]); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	out, err := g.Reach(context.Background(), []string{"A"}, "imports", DirectionOut, 0, 0)
	if err != nil {
		t.Fatalf("Reach: %v", err)
	}
	if strings.Join(out.Nodes, ",") != "B" {
		t.Errorf("Nodes = %v, want [B]（A 绕回来也不算自己的邻居）", out.Nodes)
	}
	// 自环：C 的邻居不该包含 C 自己。
	self, err := g.Reach(context.Background(), []string{"C"}, "imports", DirectionOut, 0, 0)
	if err != nil {
		t.Fatalf("Reach self: %v", err)
	}
	if len(self.Nodes) != 0 {
		t.Errorf("Nodes = %v, want 空（自环不把自己算成邻居）", self.Nodes)
	}
}

// TestMemGraphDepthLimit：深度上限生效（超层的节点不出现）。
func TestMemGraphDepthLimit(t *testing.T) {
	g := diamond(t)
	out, err := g.Reach(context.Background(), []string{"app"}, "imports", DirectionOut, 1, 0)
	if err != nil {
		t.Fatalf("Reach: %v", err)
	}
	if out.Depth != 1 {
		t.Errorf("Depth = %d, want 1", out.Depth)
	}
	if strings.Join(out.Nodes, ",") != "liba,libb" {
		t.Errorf("Nodes = %v, want [liba libb]", out.Nodes)
	}
	if out.MaxDepth != 1 {
		t.Errorf("MaxDepth = %d, want 1", out.MaxDepth)
	}
}

// TestMemGraphResultLimit：结果上限生效并置 Truncated（命中即停，同层不保证完整）。
func TestMemGraphResultLimit(t *testing.T) {
	g := diamond(t)
	out, err := g.Reach(context.Background(), []string{"app"}, "imports", DirectionOut, 0, 2)
	if err != nil {
		t.Fatalf("Reach: %v", err)
	}
	if len(out.Nodes) != 2 {
		t.Fatalf("len(Nodes) = %d, want 2", len(out.Nodes))
	}
	if !out.Truncated {
		t.Error("Truncated = false, want true（命中结果上限必须标记）")
	}
}

// TestMemGraphResultLimitIsGlobal：结果上限是**全局**的，不是每层各一个（F2）。
//
// 钉子口径：纯链 a→b→c→d（每层只有一个节点），MaxResults=2 →
//   - 旧口径（每层各 2 个）：能走 4 层、返回 4 个节点、Truncated=false
//     → 总结果 = depth × MaxResults，污染影响面计数（ReachableCount）与 escalate 判定；
//   - 新口径（全局 2 个）：第 2 层就命中上限 → 返回 2 个节点 + Truncated=true。
func TestMemGraphResultLimitIsGlobal(t *testing.T) {
	g, err := NewMemGraph(DefaultLimits())
	if err != nil {
		t.Fatalf("NewMemGraph: %v", err)
	}
	for _, e := range [][3]string{
		{"a", "b", "imports"},
		{"b", "c", "imports"},
		{"c", "d", "imports"},
	} {
		if err := g.AddEdge(e[0], e[1], e[2]); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	out, err := g.Reach(context.Background(), []string{"a"}, "imports", DirectionOut, 0, 2)
	if err != nil {
		t.Fatalf("Reach: %v", err)
	}
	if len(out.Nodes) != 2 {
		t.Errorf("len(Nodes) = %d, want 2（MaxResults 是**总**上限，不是每层各一个）", len(out.Nodes))
	}
	if !out.Truncated {
		t.Error("Truncated = false, want true（命中总上限必须标记）")
	}
	if strings.Join(out.Nodes, ",") != "b,c" {
		t.Errorf("Nodes = %v, want [b c]", out.Nodes)
	}
	// 层数也不能超过总上限允许的层（这里第 2 层就停了）。
	if len(out.Levels) != 2 {
		t.Errorf("Levels = %d 层, want 2", len(out.Levels))
	}
}

// TestMemGraphReachNegativeLimits：负值上限**拒绝**（F9：与契约一致；0 = 默认）。
func TestMemGraphReachNegativeLimits(t *testing.T) {
	g := diamond(t)
	if _, err := g.Reach(context.Background(), []string{"app"}, "imports", DirectionOut, -1, 0); !errors.Is(err, ErrBadLimits) {
		t.Errorf("负深度 err = %v, want ErrBadLimits", err)
	}
	if _, err := g.Reach(context.Background(), []string{"app"}, "imports", DirectionOut, 0, -1); !errors.Is(err, ErrBadLimits) {
		t.Errorf("负结果上限 err = %v, want ErrBadLimits", err)
	}
	// 0 走默认（不报错）。
	out, err := g.Reach(context.Background(), []string{"app"}, "imports", DirectionOut, 0, 0)
	if err != nil {
		t.Fatalf("Reach(0,0): %v", err)
	}
	if out.MaxDepth != DefaultMaxDepth {
		t.Errorf("MaxDepth = %d, want %d", out.MaxDepth, DefaultMaxDepth)
	}
}

// TestMemGraphContextCancel：context 取消生效（遍历返回取消错误，不是半成品）。
func TestMemGraphContextCancel(t *testing.T) {
	g := diamond(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.Reach(ctx, []string{"app"}, "imports", DirectionOut, 0, 0); !errors.Is(err, context.Canceled) {
		t.Errorf("Reach err = %v, want context.Canceled", err)
	}
}

// TestMemGraphInternedIDs：同名节点不膨胀；长 id 也能 intern 并原样取回。
func TestMemGraphInternedIDs(t *testing.T) {
	g, err := NewMemGraph(DefaultLimits())
	if err != nil {
		t.Fatalf("NewMemGraph: %v", err)
	}
	long := strings.Repeat("x", 4096) + "/very/long/node/id.go"
	for i := 0; i < 100; i++ {
		// 同一个名字反复加：节点数必须还是 1（intern 复用）。
		if err := g.AddEdge(long, "dst", "imports"); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	if got := g.NodeCount(); got != 2 {
		t.Errorf("NodeCount = %d, want 2（同名节点不膨胀，G19）", got)
	}
	if got := g.EdgeCount(); got != 1 {
		t.Errorf("EdgeCount = %d, want 1（重复加同一条边是幂等的）", got)
	}
	if !g.HasNode(long) {
		t.Error("HasNode(long) = false, want true（intern 后仍能按原名查到）")
	}
	out, err := g.Reach(context.Background(), []string{long}, "imports", DirectionOut, 0, 0)
	if err != nil {
		t.Fatalf("Reach: %v", err)
	}
	if len(out.Nodes) != 1 || out.Nodes[0] != "dst" {
		t.Errorf("Nodes = %v, want [dst]（长 id 原样往返）", out.Nodes)
	}
}

// TestMemGraphCaseSensitive：node id / kind 大小写敏感、字节精确（陷阱 17）。
func TestMemGraphCaseSensitive(t *testing.T) {
	g, err := NewMemGraph(DefaultLimits())
	if err != nil {
		t.Fatalf("NewMemGraph: %v", err)
	}
	for _, e := range [][3]string{
		{"Foo", "Bar", "imports"},
		{"foo", "bar", "IMPORTS"},
	} {
		if err := g.AddEdge(e[0], e[1], e[2]); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	if got := g.NodeCount(); got != 4 {
		t.Errorf("NodeCount = %d, want 4（Foo≠foo、Bar≠bar：不折叠大小写）", got)
	}
	if got := g.EdgeCount(); got != 2 {
		t.Errorf("EdgeCount = %d, want 2（imports≠IMPORTS）", got)
	}
	// 查 "Foo" 不该命中 "foo" 的边。
	out, err := g.Reach(context.Background(), []string{"Foo"}, "imports", DirectionOut, 0, 0)
	if err != nil {
		t.Fatalf("Reach: %v", err)
	}
	if strings.Join(out.Nodes, ",") != "Bar" {
		t.Errorf("Nodes = %v, want [Bar]", out.Nodes)
	}
}

// TestMemGraphKindFilter：kind 过滤器只放行同 kind 的边（开放字符串，不猜语义）。
func TestMemGraphKindFilter(t *testing.T) {
	g, err := NewMemGraph(DefaultLimits())
	if err != nil {
		t.Fatalf("NewMemGraph: %v", err)
	}
	for _, e := range [][3]string{
		{"a", "b", "imports"},
		{"a", "c", "tests"},
		{"a", "d", "documents"},
	} {
		if err := g.AddEdge(e[0], e[1], e[2]); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	all, err := g.Reach(context.Background(), []string{"a"}, "", DirectionOut, 0, 0)
	if err != nil {
		t.Fatalf("Reach all: %v", err)
	}
	if strings.Join(all.Nodes, ",") != "b,c,d" {
		t.Errorf("Nodes = %v, want [b c d]", all.Nodes)
	}
	only, err := g.Reach(context.Background(), []string{"a"}, "imports", DirectionOut, 0, 0)
	if err != nil {
		t.Fatalf("Reach imports: %v", err)
	}
	if strings.Join(only.Nodes, ",") != "b" {
		t.Errorf("Nodes = %v, want [b]", only.Nodes)
	}
}

// TestMemGraphEmptyEdgeDropped：空端点/空 kind 的边被丢弃（不猜默认值）。
func TestMemGraphEmptyEdgeDropped(t *testing.T) {
	g, err := NewMemGraph(DefaultLimits())
	if err != nil {
		t.Fatalf("NewMemGraph: %v", err)
	}
	for _, e := range [][3]string{{"", "b", "imports"}, {"a", "", "imports"}, {"a", "b", ""}} {
		if err := g.AddEdge(e[0], e[1], e[2]); err != nil {
			t.Fatalf("AddEdge(%v): %v", e, err)
		}
	}
	if got := g.EdgeCount(); got != 0 {
		t.Errorf("EdgeCount = %d, want 0（空字段的边没有意义）", got)
	}
}

// TestMemGraphLimits：触到内存上界 → ErrGraphFull（fail-close，G19）。
func TestMemGraphLimits(t *testing.T) {
	g, err := NewMemGraph(Limits{MaxNodes: 3, MaxEdges: 2})
	if err != nil {
		t.Fatalf("NewMemGraph: %v", err)
	}
	if err := g.AddEdge("a", "b", "imports"); err != nil {
		t.Fatalf("AddEdge 1: %v", err)
	}
	if err := g.AddEdge("b", "c", "imports"); err != nil {
		t.Fatalf("AddEdge 2: %v", err)
	}
	// 第三条边要新节点 d → 超 MaxNodes=3（a,b,c 已满）。
	if err := g.AddEdge("c", "d", "imports"); !errors.Is(err, ErrGraphFull) {
		t.Errorf("AddEdge 3 err = %v, want ErrGraphFull", err)
	}
	if !g.Saturated() {
		t.Error("Saturated() = false, want true")
	}
	// 触界后不再接受新边（恒 fail-close，不会悄悄放行几条）。
	if err := g.AddEdge("a", "b", "imports"); !errors.Is(err, ErrGraphFull) {
		t.Errorf("AddEdge after full err = %v, want ErrGraphFull", err)
	}
}

// TestMemGraphLimitsNegative：负上界 → ErrBadLimits（0 = 默认）。
func TestMemGraphLimitsNegative(t *testing.T) {
	if _, err := NewMemGraph(Limits{MaxNodes: -1}); !errors.Is(err, ErrBadLimits) {
		t.Errorf("err = %v, want ErrBadLimits", err)
	}
}

// TestNormalizeReach：归一化（0→默认、负→拒、起点去重去空排序）。
func TestNormalizeReach(t *testing.T) {
	q, err := NormalizeReach(ReachQuery{Sources: []string{"b", "a", "b", ""}})
	if err != nil {
		t.Fatalf("NormalizeReach: %v", err)
	}
	if strings.Join(q.Sources, ",") != "a,b" {
		t.Errorf("Sources = %v, want [a b]（去重去空排序）", q.Sources)
	}
	if q.MaxDepth != DefaultMaxDepth || q.MaxResults != DefaultMaxResults {
		t.Errorf("上限 = %d/%d, want 默认 %d/%d", q.MaxDepth, q.MaxResults, DefaultMaxDepth, DefaultMaxResults)
	}
	if _, err := NormalizeReach(ReachQuery{Sources: nil}); !errors.Is(err, ErrNoSources) {
		t.Errorf("空起点 err = %v, want ErrNoSources", err)
	}
	if _, err := NormalizeReach(ReachQuery{Sources: []string{"a"}, MaxDepth: -1}); !errors.Is(err, ErrBadLimits) {
		t.Errorf("负深度 err = %v, want ErrBadLimits", err)
	}
	// 键必须与"显式写默认值"一致（否则同一查询两种写法反复 miss）。
	k1 := ClosureKeyOf(q)
	k2 := ClosureKeyOf(ReachQuery{Sources: []string{"b", "a"}, MaxDepth: DefaultMaxDepth, MaxResults: DefaultMaxResults})
	if k1 != k2 {
		t.Errorf("键不一致：%+v vs %+v（顺序无关 + 0=默认必须归一）", k1, k2)
	}
}

// TestClosureCacheHitAndInvalidate：缓存命中 + 图写入后失效（G20）。
func TestClosureCacheHitAndInvalidate(t *testing.T) {
	c := NewClosureCache()
	key := ClosureKey{Sources: "app", Kind: "imports", Direction: DirectionIn, MaxDepth: 8, MaxResults: 5000}
	if _, ok := c.Get(key); ok {
		t.Fatal("空缓存不该命中")
	}
	c.Put(key, Closure{Nodes: []string{"liba"}}, c.Epoch())
	got, ok := c.Get(key)
	if !ok {
		t.Fatal("Put 后应命中")
	}
	if strings.Join(got.Nodes, ",") != "liba" {
		t.Errorf("Nodes = %v, want [liba]", got.Nodes)
	}
	if c.Hits() != 1 || c.Misses() != 1 {
		t.Errorf("Hits/Misses = %d/%d, want 1/1", c.Hits(), c.Misses())
	}
	// 图写入 → 整表失效（G20 简版：不做复杂淘汰）。
	c.Clear()
	if _, ok := c.Get(key); ok {
		t.Error("Clear 后不该再命中")
	}
	if c.Len() != 0 {
		t.Errorf("Len = %d, want 0", c.Len())
	}
}

// TestClosureCacheEpochGuard：缓存项带代号，竞态窗口里算出来的旧闭包**写不进去**（F3）。
//
// 复现的竞态：读侧 Get（未命中）→ 图被写入并失效缓存 → 读侧算完 → Put。
// 没有代号护栏时，这次 Put 会把**过期闭包**塞回缓存（写者以为清干净了）。
func TestClosureCacheEpochGuard(t *testing.T) {
	c := NewClosureCache()
	key := ClosureKey{Sources: "app", Kind: "imports", Direction: DirectionIn, MaxDepth: 8, MaxResults: 5000}

	// 读侧在"算之前"取代号。
	epoch := c.Epoch()
	// 期间图被写入 → 失效 → 代号推进。
	c.Invalidate()
	if c.Epoch() == epoch {
		t.Fatal("Invalidate 后代号应推进")
	}
	// 读侧算完来写回：代号对不上 → **不写**（返回 false）。
	if c.Put(key, Closure{Nodes: []string{"stale"}}, epoch) {
		t.Error("Put（过期代号）应被拒（过期闭包不能进缓存）")
	}
	if _, ok := c.Get(key); ok {
		t.Error("过期闭包不该在缓存里")
	}
	// 用**当前**代号写 → 落盘并命中。
	cur := c.Epoch()
	if !c.Put(key, Closure{Nodes: []string{"fresh"}}, cur) {
		t.Error("Put（当前代号）应成功")
	}
	got, ok := c.Get(key)
	if !ok || strings.Join(got.Nodes, ",") != "fresh" {
		t.Errorf("Get = %v/%v, want [fresh]/true", got.Nodes, ok)
	}
	// 再次失效 → 旧条目作废（Get 校验代号）。
	c.Invalidate()
	if _, ok := c.Get(key); ok {
		t.Error("失效后不该再命中（Get 要校验代号）")
	}
}

// TestClosureCacheKeyNoCollision：缓存键的起点集编码**无歧义**（F10）。
//
// 旧口径用 "\x00" join：`["a\x00b"]`（一个含 NUL 的 id）与 `["a","b"]`（两个 id）
// 折出同一个键 → 两种完全不同的查询共用一条缓存条目。
func TestClosureCacheKeyNoCollision(t *testing.T) {
	k1 := ClosureKeyOf(ReachQuery{Sources: []string{"a\x00b"}})
	k2 := ClosureKeyOf(ReachQuery{Sources: []string{"a", "b"}})
	if k1.Sources == k2.Sources {
		t.Errorf("起点集编码撞键：%q == %q（长度前缀 join 才能避免）", k1.Sources, k2.Sources)
	}
	// 集合语义仍然成立：顺序无关。
	k3 := ClosureKeyOf(ReachQuery{Sources: []string{"b", "a"}})
	if k2.Sources != k3.Sources {
		t.Errorf("顺序不同应同键：%q vs %q", k2.Sources, k3.Sources)
	}
	// 去重后同键。
	k4 := ClosureKeyOf(ReachQuery{Sources: []string{"a", "a", "b"}})
	if k2.Sources != k4.Sources {
		t.Errorf("重复起点应同键：%q vs %q", k2.Sources, k4.Sources)
	}
}
