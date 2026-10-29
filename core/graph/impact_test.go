package graph

import (
	"context"
	"strings"
	"testing"
)

// impactGraph 造一张 change_impact 用的依赖图（kind "imports"，X→Y = X 依赖 Y）：
//
//	app                     → core/store.go
//	cmd/cli                 → core/store.go
//	host/hub                → core/store.go
//	host/hub                → runtime/graph/engine.go
//	runtime/graph/engine.go → core/graph
//
// 即：改 core/store.go 会影响 app / cmd/cli / host/hub（一跳，全是直接依赖者）；
// 改 core/graph 会影响 engine（一跳）→ host/hub（两跳）。
func impactGraph(t *testing.T) *MemGraph {
	t.Helper()
	g, err := NewMemGraph(DefaultLimits())
	if err != nil {
		t.Fatalf("NewMemGraph: %v", err)
	}
	for _, e := range [][3]string{
		{"app", "core/store.go", "imports"},
		{"cmd/cli", "core/store.go", "imports"},
		{"host/hub", "core/store.go", "imports"},
		{"host/hub", "runtime/graph/engine.go", "imports"},
		{"runtime/graph/engine.go", "core/graph", "imports"},
	} {
		if err := g.AddEdge(e[0], e[1], e[2]); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	return g
}

// reachIn 是测试助手：跑一次入向可达遍历。
func reachIn(t *testing.T, g *MemGraph, roots ...string) Closure {
	t.Helper()
	c, err := g.Reach(context.Background(), roots, "imports", DirectionIn, 0, 0)
	if err != nil {
		t.Fatalf("Reach: %v", err)
	}
	return c
}

// TestAssembleImpactSmallGraph：小依赖图上的影响面正确（一跳/传递/核心接口/规模）。
func TestAssembleImpactSmallGraph(t *testing.T) {
	g := impactGraph(t)
	c := reachIn(t, g, "core/store.go")
	rep := AssembleImpact(ImpactRequest{
		Roots: []string{"core/store.go"},
		Kind:  "imports",
		Core:  ParseCoreSet([]string{"core/graph", "core/*"}),
	}, c)

	if strings.Join(rep.Roots, ",") != "core/store.go" {
		t.Errorf("Roots = %v", rep.Roots)
	}
	// 一跳下游 = 直接依赖 core/store.go 的三个。
	if strings.Join(rep.Direct, ",") != "app,cmd/cli,host/hub" {
		t.Errorf("Direct = %v, want [app cmd/cli host/hub]", rep.Direct)
	}
	// 传递下游 = 两跳及以后（这张图里没人依赖 app/cmd/cli/host/hub → 空）。
	if len(rep.Transitive) != 0 {
		t.Errorf("Transitive = %v, want 空", rep.Transitive)
	}
	if rep.ReachableCount != 3 {
		t.Errorf("ReachableCount = %d, want 3", rep.ReachableCount)
	}
	// 触及的核心接口：起点 core/store.go 自身命中 core/* 前缀（精确 id 那条不命中）。
	if strings.Join(rep.CoreTouched, ",") != "core/store.go" {
		t.Errorf("CoreTouched = %v, want [core/store.go]", rep.CoreTouched)
	}
	if rep.Truncated {
		t.Error("Truncated = true, want false")
	}
	if len(rep.UnknownRoots) != 0 {
		t.Errorf("UnknownRoots = %v, want 空", rep.UnknownRoots)
	}
	// 默认阈值（可达 50 / 一跳 10 / 核心 0）：3 个可达 + 3 个一跳都不超，
	// 但**触及核心接口**（1 > 0）→ escalate + large。
	if !rep.Escalate {
		t.Error("Escalate = false, want true（触及核心接口）")
	}
	if rep.Scale != ScaleLarge {
		t.Errorf("Scale = %q, want %q", rep.Scale, ScaleLarge)
	}
	if len(rep.Reasons) != 1 || !strings.Contains(rep.Reasons[0], "触及核心接口") {
		t.Errorf("Reasons = %v, want 一条'触及核心接口'", rep.Reasons)
	}
}

// TestAssembleImpactThresholds：阈值可配，判定随之变（W15：风险闸门）。
func TestAssembleImpactThresholds(t *testing.T) {
	g := impactGraph(t)
	c := reachIn(t, g, "core/store.go")
	req := ImpactRequest{Roots: []string{"core/store.go"}, Kind: "imports"}

	// 阈值放到很大 → 不触发（影响面确实不算大）。
	rep := AssembleImpact(req, c)
	_ = rep
	rep = AssembleImpact(ImpactRequest{
		Roots:      req.Roots,
		Kind:       req.Kind,
		Thresholds: Thresholds{MaxReachable: 100, MaxDirect: 100, MaxDiffLines: 100000, MaxCoreTouched: 5},
	}, c)
	if rep.Escalate {
		t.Errorf("Escalate = true, want false（阈值放到 100/100/100000/5）: %v", rep.Reasons)
	}
	if rep.Scale != ScaleSmall {
		t.Errorf("Scale = %q, want %q", rep.Scale, ScaleSmall)
	}

	// 可达阈值压到 2（3 > 2）→ 触发。
	rep = AssembleImpact(ImpactRequest{
		Roots:      req.Roots,
		Kind:       req.Kind,
		Thresholds: Thresholds{MaxReachable: 2, MaxDirect: 100, MaxDiffLines: 100000, MaxCoreTouched: 5},
	}, c)
	if !rep.Escalate {
		t.Error("Escalate = false, want true（可达 3 > 阈值 2）")
	}
	if rep.Scale != ScaleLarge {
		t.Errorf("Scale = %q, want %q", rep.Scale, ScaleLarge)
	}

	// 中档：可达阈值 5（3 未超但 3*2 > 5）→ medium 不 escalate。
	rep = AssembleImpact(ImpactRequest{
		Roots:      req.Roots,
		Kind:       req.Kind,
		Thresholds: Thresholds{MaxReachable: 5, MaxDirect: 100, MaxDiffLines: 100000, MaxCoreTouched: 5},
	}, c)
	if rep.Escalate {
		t.Errorf("Escalate = true, want false: %v", rep.Reasons)
	}
	if rep.Scale != ScaleMedium {
		t.Errorf("Scale = %q, want %q", rep.Scale, ScaleMedium)
	}
}

// TestAssembleImpactDiffStat：diff 规模参与判定（W16 第一个客观依据）。
func TestAssembleImpactDiffStat(t *testing.T) {
	g := impactGraph(t)
	c := reachIn(t, g, "core/store.go")
	req := ImpactRequest{
		Roots:      []string{"core/store.go"},
		Kind:       "imports",
		Thresholds: Thresholds{MaxReachable: 100, MaxDirect: 100, MaxCoreTouched: 5},
	}
	// 不给 diff → 不因 diff 触发。
	rep := AssembleImpact(req, c)
	if rep.Escalate {
		t.Errorf("Escalate = true, want false（无 diff）: %v", rep.Reasons)
	}
	if rep.Diff != nil {
		t.Error("Diff 应为 nil")
	}
	// 给一个超阈值的大 diff → 触发，且理由里带行数。
	req.Diff = &DiffStat{Files: 12, Added: 800, Deleted: 300}
	req.Thresholds.MaxDiffLines = 500
	rep = AssembleImpact(req, c)
	if !rep.Escalate {
		t.Fatal("Escalate = false, want true（diff 1100 行 > 500）")
	}
	found := false
	for _, r := range rep.Reasons {
		if strings.Contains(r, "diff 1100 行") {
			found = true
		}
	}
	if !found {
		t.Errorf("Reasons = %v, want 含 diff 行数说明", rep.Reasons)
	}
	if rep.Diff == nil || rep.Diff.Lines() != 1100 {
		t.Errorf("Diff = %+v, want Lines()=1100", rep.Diff)
	}
}

// TestAssembleImpactUnknownRootsAndTruncated：诚实信号（图里没有的起点 / 被截断）。
func TestAssembleImpactUnknownRootsAndTruncated(t *testing.T) {
	g := impactGraph(t)
	c := reachIn(t, g, "core/store.go", "not/in/graph.go")
	if strings.Join(c.MissingRoots, ",") != "not/in/graph.go" {
		t.Errorf("MissingRoots = %v, want [not/in/graph.go]", c.MissingRoots)
	}
	rep := AssembleImpact(ImpactRequest{Roots: []string{"core/store.go", "not/in/graph.go"}, Kind: "imports"}, c)
	if strings.Join(rep.UnknownRoots, ",") != "not/in/graph.go" {
		t.Errorf("UnknownRoots = %v, want [not/in/graph.go]", rep.UnknownRoots)
	}
	// 截断必须显式列进理由（否则"看起来影响面不大"其实是被上限截掉了）。
	c.Truncated = true
	rep = AssembleImpact(ImpactRequest{Roots: []string{"core/store.go"}, Kind: "imports"}, c)
	if !rep.Truncated || !rep.Escalate {
		t.Errorf("Truncated=%v Escalate=%v, want 都 true", rep.Truncated, rep.Escalate)
	}
}

// TestAssembleImpactMultiRoot：多起点（diff 触及多个文件）取并集。
func TestAssembleImpactMultiRoot(t *testing.T) {
	g := impactGraph(t)
	c := reachIn(t, g, "core/store.go", "core/graph")
	rep := AssembleImpact(ImpactRequest{Roots: []string{"core/store.go", "core/graph"}, Kind: "imports"}, c)
	// core/graph 的被依赖者 = engine（一跳）→ host/hub（两跳）。
	if strings.Join(rep.Direct, ",") != "app,cmd/cli,host/hub,runtime/graph/engine.go" {
		t.Errorf("Direct = %v", rep.Direct)
	}
	// 并集 = app + cmd/cli + host/hub + engine。
	if rep.ReachableCount != 4 {
		t.Errorf("ReachableCount = %d, want 4", rep.ReachableCount)
	}
}

// TestAssembleImpactEchoesDirection：报告**回显方向**（F16）。
//
// 影响面 = 入向（谁依赖我）；报告不能靠"注释说影响面 = 入向"来暗示方向——
// 消费端（escalate 闸门）直接看 Direction 字段就知道 Direct/Transitive 是哪边的下游。
func TestAssembleImpactEchoesDirection(t *testing.T) {
	g := impactGraph(t)
	c := reachIn(t, g, "core/store.go")
	rep := AssembleImpact(ImpactRequest{Roots: []string{"core/store.go"}, Kind: "imports"}, c)
	if rep.Direction != DirectionIn {
		t.Errorf("Direction = %v, want DirectionIn（入向 = 影响面）", rep.Direction)
	}
	// 出向遍历的报告也要如实回显（不假装自己是影响面）。
	out, err := g.Reach(context.Background(), []string{"core/store.go"}, "imports", DirectionOut, 0, 0)
	if err != nil {
		t.Fatalf("Reach out: %v", err)
	}
	repOut := AssembleImpact(ImpactRequest{Roots: []string{"core/store.go"}, Kind: "imports"}, out)
	if repOut.Direction != DirectionOut {
		t.Errorf("Direction = %v, want DirectionOut", repOut.Direction)
	}
}

// TestScaleOfHalfThreshold：量级分档的边界是"**达到**阈值的一半"（F11）。
//
// 注释写"达到一半 = medium"，代码却是严格大于 → 边界上（正好一半）判成 small，
// 注释与行为对不上。这里钉住"达到即 medium"。
func TestScaleOfHalfThreshold(t *testing.T) {
	th := Thresholds{MaxReachable: 10, MaxDirect: 10, MaxDiffLines: 10}
	// 可达数正好是阈值的一半（5 = 10/2）→ medium。
	if got := scaleOf(5, 0, nil, th, false); got != ScaleMedium {
		t.Errorf("scaleOf(5, 阈值10) = %q, want %q（达到一半即 medium）", got, ScaleMedium)
	}
	// 不到一半 → small。
	if got := scaleOf(4, 0, nil, th, false); got != ScaleSmall {
		t.Errorf("scaleOf(4, 阈值10) = %q, want %q", got, ScaleSmall)
	}
	// escalate 优先 → large。
	if got := scaleOf(100, 0, nil, th, true); got != ScaleLarge {
		t.Errorf("scaleOf(escalate) = %q, want %q", got, ScaleLarge)
	}
}

// TestParseCoreSet：核心接口声明的两种形态（精确 id / `*` 前缀），大小写敏感。
func TestParseCoreSet(t *testing.T) {
	cs := ParseCoreSet([]string{"core/store.go", " core/* ", "", "contracts/*"})
	if len(cs.IDs) != 1 || cs.IDs[0] != "core/store.go" {
		t.Errorf("IDs = %v, want [core/store.go]", cs.IDs)
	}
	if len(cs.Prefixes) != 2 || cs.Prefixes[0] != "core/" || cs.Prefixes[1] != "contracts/" {
		t.Errorf("Prefixes = %v, want [core/ contracts/]", cs.Prefixes)
	}
	if !cs.IsCore("core/store.go") || !cs.IsCore("core/graph/engine.go") {
		t.Error("IsCore 应命中精确 id 与前缀")
	}
	if cs.IsCore("Core/store.go") {
		t.Error("IsCore(Core/...) = true, want false（大小写敏感，陷阱 17）")
	}
	if (CoreSet{}).Empty() != true {
		t.Error("空 CoreSet.Empty() 应为 true")
	}
	if (CoreSet{}).IsCore("anything") {
		t.Error("空 CoreSet 不该命中任何东西")
	}
}

// TestThresholdsNormalize：阈值归一（0→默认、负→拒）。
func TestThresholdsNormalize(t *testing.T) {
	got, err := Thresholds{}.normalize()
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if got != DefaultThresholds() {
		t.Errorf("零值归一 = %+v, want %+v", got, DefaultThresholds())
	}
	if _, err := (Thresholds{MaxReachable: -1}).normalize(); err == nil {
		t.Error("负阈值应报错")
	}
}
