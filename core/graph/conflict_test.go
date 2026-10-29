package graph

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// conflictGraph 造一张小依赖图（kind "imports"，X→Y = X 依赖 Y），两条独立依赖链：
//
//	a1 → shared1        b1 → shared2
//	a2 → shared1        b2 → shared2
//
// 改 shared1 影响 a1/a2（入向）；改 shared2 影响 b1/b2。两条链不相交。
func conflictGraph(t *testing.T) *MemGraph {
	t.Helper()
	g, err := NewMemGraph(DefaultLimits())
	if err != nil {
		t.Fatalf("NewMemGraph: %v", err)
	}
	for _, e := range [][3]string{
		{"a1", "shared1", "imports"},
		{"a2", "shared1", "imports"},
		{"b1", "shared2", "imports"},
		{"b2", "shared2", "imports"},
	} {
		if err := g.AddEdge(e[0], e[1], e[2]); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	return g
}

// siblingGraph 造一张"共同下游"的图（p2-1 的误报用例）：c 同时依赖 a 与 b。
//
//	c → a        c → b
//
// 改 a 的影响面 = {a, c}；改 b 的影响面 = {b, c}。两边**影响面有交集**（c），但 a 与 b
// 是**兄弟叶子**——谁也不碰谁，不该判撞车。
func siblingGraph(t *testing.T) *MemGraph {
	t.Helper()
	g, err := NewMemGraph(DefaultLimits())
	if err != nil {
		t.Fatalf("NewMemGraph: %v", err)
	}
	for _, e := range [][3]string{
		{"c", "a", "imports"},
		{"c", "b", "imports"},
	} {
		if err := g.AddEdge(e[0], e[1], e[2]); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	return g
}

// consumerGraph 造一张"库 + 消费者"的图：app 依赖 lib。
//
//	app → lib
//
// 改 lib（库）的影响面含 app（消费者）；改 app（消费者）的影响面 = {app}。
func consumerGraph(t *testing.T) *MemGraph {
	t.Helper()
	g, err := NewMemGraph(DefaultLimits())
	if err != nil {
		t.Fatalf("NewMemGraph: %v", err)
	}
	if err := g.AddEdge("app", "lib", "imports"); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	return g
}

// memReach 把 MemGraph 包成 ConflictReach（入向影响闭包；测试直接喂，免 SQLite）。
func memReach(g *MemGraph, kind string) ConflictReach {
	return func(ctx context.Context, roots []string) (Closure, error) {
		return g.Reach(ctx, roots, kind, DirectionIn, 0, 0)
	}
}

// riskOf 从报告里取某分支的预判（测试助手）。
func riskOf(rep ConflictReport, branch string) (ConflictRisk, bool) {
	for _, r := range rep.Risks {
		if r.Branch == branch {
			return r, true
		}
	}
	return ConflictRisk{}, false
}

// TestPredictConflictsCollision：两分支改同一依赖链 → 预警撞车（依据是传递影响面相交）。
//
// 变异自证：此撞车**只有靠可达性**才能发现——合并方改 shared1（影响面含 a1），未合并方改 a1
// （a1 的触点）。两者**直接触点**（shared1 vs a1）不相交，但 a1 落在合并方的影响面内。
// 若把"可达性/影响面比对"去掉（只比 roots 直接触点），本测试会红。
func TestPredictConflictsCollision(t *testing.T) {
	g := conflictGraph(t)
	reach := memReach(g, "imports")

	merged := ChangeSet{Label: "merged", Roots: []string{"shared1"}}
	others := []ChangeSet{
		{Label: "branch-a1", Roots: []string{"a1"}}, // 改 a1，而 a1 在 merged 的影响面内
	}
	rep := PredictConflicts(context.Background(), reach, merged, others)

	if !rep.Available {
		t.Errorf("图应可用：Available=false, reason=%q", rep.UnavailableReason)
	}
	if rep.Collisions != 1 {
		t.Errorf("Collisions = %d, want 1", rep.Collisions)
	}
	r, ok := riskOf(rep, "branch-a1")
	if !ok {
		t.Fatal("应有 branch-a1 的预判")
	}
	if !r.Collides {
		t.Error("branch-a1 应预判撞车（a1 落在 merged 影响面内）")
	}
	if !r.Available {
		t.Error("branch-a1 预测应可用")
	}
	// 依据含 a1（撞点），命中类型 = one-side（传递命中，不是同触点）。
	if strings.Join(r.Basis, ",") != "one-side:a1" {
		t.Errorf("Basis = %v, want [one-side:a1]（传递命中）", r.Basis)
	}
}

// TestPredictConflictsNoCollision：两条独立依赖链、无交集 → 不预警（且明确"无交集"可用）。
func TestPredictConflictsNoCollision(t *testing.T) {
	g := conflictGraph(t)
	reach := memReach(g, "imports")

	merged := ChangeSet{Label: "merged", Roots: []string{"shared1"}}
	others := []ChangeSet{
		{Label: "branch-b", Roots: []string{"shared2"}}, // 完全不同的链
	}
	rep := PredictConflicts(context.Background(), reach, merged, others)

	if !rep.Available {
		t.Errorf("图应可用：%q", rep.UnavailableReason)
	}
	if rep.Collisions != 0 {
		t.Errorf("Collisions = %d, want 0（无交集）", rep.Collisions)
	}
	r, _ := riskOf(rep, "branch-b")
	if r.Collides {
		t.Error("无交集不应预警")
	}
	if !r.Available {
		t.Error("无交集且图可用 → Available 应为 true")
	}
	if r.Reason != ReasonNoOverlap {
		t.Errorf("Reason = %q, want %q", r.Reason, ReasonNoOverlap)
	}
}

// TestPredictConflictsGraphDisabled：图未启用 → 诚实降级（说"预测不可用"，绝不说"无冲突"）。
func TestPredictConflictsGraphDisabled(t *testing.T) {
	merged := ChangeSet{Label: "merged", Roots: []string{"shared1"}}
	others := []ChangeSet{
		{Label: "branch-other", Roots: []string{"a1"}}, // 不同触点、图又没开
	}
	// reach=nil = 图投影未启用（G2f）。
	rep := PredictConflicts(context.Background(), nil, merged, others)

	if rep.Available {
		t.Error("图未启用 → Available 应为 false")
	}
	if rep.UnavailableReason != ReasonGraphDisabled {
		t.Errorf("UnavailableReason = %q, want %q", rep.UnavailableReason, ReasonGraphDisabled)
	}
	r, _ := riskOf(rep, "branch-other")
	if r.Available {
		t.Error("图未启用 → 该分支预测不可用")
	}
	// 无直接触点重叠 + 图不可用 → 不撞车，但理由是"无法排除"，**不是**"无交集"（诚实降级）。
	if r.Collides {
		t.Error("无直接触点重叠不该预警撞车")
	}
	if r.Reason != ReasonCannotExclude {
		t.Errorf("Reason = %q, want %q（诚实降级，非无冲突）", r.Reason, ReasonCannotExclude)
	}
}

// TestPredictConflictsDirectOverlapWithoutGraph：图未启用时，**直接触点重叠**仍是确定信号。
func TestPredictConflictsDirectOverlapWithoutGraph(t *testing.T) {
	merged := ChangeSet{Label: "merged", Roots: []string{"shared1"}}
	others := []ChangeSet{
		{Label: "branch-same", Roots: []string{"shared1"}}, // 同一触点
	}
	rep := PredictConflicts(context.Background(), nil, merged, others)

	r, _ := riskOf(rep, "branch-same")
	if !r.Collides {
		t.Error("同一改动触点（shared1）应预警撞车（不需图也能确定的信号）")
	}
	if strings.Join(r.Basis, ",") != "direct:shared1" {
		t.Errorf("Basis = %v, want [direct:shared1]", r.Basis)
	}
	// 但仍诚实标注图不可用（传递面未知）。
	if r.Available {
		t.Error("图未启用 → Available 应为 false")
	}
}

// TestPredictConflictsRootsUnknown：合并改动节点不在图中 → 整体预测不可用（诚实）。
func TestPredictConflictsRootsUnknown(t *testing.T) {
	g := conflictGraph(t)
	reach := memReach(g, "imports")

	merged := ChangeSet{Label: "merged", Roots: []string{"not/in/graph"}}
	others := []ChangeSet{{Label: "branch-a1", Roots: []string{"a1"}}}
	rep := PredictConflicts(context.Background(), reach, merged, others)

	if rep.Available {
		t.Error("合并改动节点不在图中 → Available 应为 false")
	}
	if rep.UnavailableReason != ReasonRootsUnknown {
		t.Errorf("UnavailableReason = %q, want %q", rep.UnavailableReason, ReasonRootsUnknown)
	}
}

// TestPredictConflictsPartiallyMissingRoots（p1-1）：**部分**触点不在图中 → 覆盖度不足，
// 不能报"影响面无交集"（假无冲突）。
//
// 变异自证：若 impactOf 只看"有没有至少一个 root 在图里"（丢弃 MissingRoots），本测试会红
// ——merged 的两个触点里有一个不在图里，影响面只追了一半，却报 Available=true +
// "影响面无交集"。
func TestPredictConflictsPartiallyMissingRoots(t *testing.T) {
	g := conflictGraph(t)
	reach := memReach(g, "imports")

	// shared1 在图里、ghost 不在图里 → **部分**覆盖。
	merged := ChangeSet{Label: "merged", Roots: []string{"shared1", "ghost"}}
	others := []ChangeSet{{Label: "branch-b", Roots: []string{"shared2"}}}
	rep := PredictConflicts(context.Background(), reach, merged, others)

	if rep.Available {
		t.Errorf("部分触点不在图中 → 整体 Available 应为 false（reason=%q）", rep.UnavailableReason)
	}
	if rep.UnavailableReason != ReasonRootsUnknown {
		t.Errorf("UnavailableReason = %q, want %q", rep.UnavailableReason, ReasonRootsUnknown)
	}
	r, _ := riskOf(rep, "branch-b")
	if r.Available {
		t.Error("部分触点不在图中 → 该分支预测不可用")
	}
	if r.Reason == ReasonNoOverlap {
		t.Error("覆盖不足时理由不得是\"影响面无交集\"（那是假无冲突）")
	}
	if r.Reason != ReasonCannotExclude {
		t.Errorf("Reason = %q, want %q（无法排除口径）", r.Reason, ReasonCannotExclude)
	}
	// 覆盖度信号要带出来（消费端据此知道"为什么不可用"）。
	if len(r.UnknownRoots) == 0 {
		t.Error("应带出 unknown_roots（覆盖度信号）")
	}
}

// TestPredictConflictsTruncated（p1-1）：闭包被上限截断 → 影响面不完整 → 不能说"无交集"。
//
// 变异自证：若把 Truncated 丢弃（当作完整影响面），本测试会红。
func TestPredictConflictsTruncated(t *testing.T) {
	g := conflictGraph(t)
	inner := memReach(g, "imports")
	reach := func(ctx context.Context, roots []string) (Closure, error) {
		cl, err := inner(ctx, roots)
		if err != nil {
			return cl, err
		}
		cl.Truncated = true // 模拟命中结果上限被截断
		return cl, nil
	}

	merged := ChangeSet{Label: "merged", Roots: []string{"shared1"}}
	others := []ChangeSet{{Label: "branch-b", Roots: []string{"shared2"}}}
	rep := PredictConflicts(context.Background(), reach, merged, others)

	if rep.Available {
		t.Error("闭包被截断 → 整体 Available 应为 false")
	}
	// 报告级的降级理由只说"影响面没追全"（ReasonTruncated 与 ReasonRootsUnknown 同值）；
	// 精确到"被截断"由逐分支的 truncated 字段给出。
	if rep.UnavailableReason != ReasonRootsUnknown {
		t.Errorf("UnavailableReason = %q, want %q（影响面没追全）", rep.UnavailableReason, ReasonRootsUnknown)
	}
	if ReasonTruncated != ReasonRootsUnknown {
		t.Errorf("ReasonTruncated 应与 ReasonRootsUnknown 同值（都是\"没追全\"），实际 %q vs %q", ReasonTruncated, ReasonRootsUnknown)
	}
	r, _ := riskOf(rep, "branch-b")
	if r.Available {
		t.Error("闭包被截断 → 该分支预测不可用")
	}
	if r.Reason == ReasonNoOverlap {
		t.Error("截断时理由不得是\"影响面无交集\"（影响面可能不完整）")
	}
	if !r.Truncated {
		t.Error("应带出 truncated 覆盖度信号")
	}
}

// TestPredictConflictsReachError（p3-4）：追闭包**出错** ≠ "节点不在图里"。
//
// 变异自证：若把错误也归到 ReasonRootsUnknown，本测试会红。
func TestPredictConflictsReachError(t *testing.T) {
	reach := func(ctx context.Context, roots []string) (Closure, error) {
		return Closure{}, errors.New("graph: 查询炸了")
	}
	merged := ChangeSet{Label: "merged", Roots: []string{"shared1"}}
	others := []ChangeSet{{Label: "branch-a1", Roots: []string{"a1"}}}
	rep := PredictConflicts(context.Background(), reach, merged, others)

	if rep.Available {
		t.Error("追闭包出错 → 整体 Available 应为 false")
	}
	if rep.UnavailableReason != ReasonReachFailed {
		t.Errorf("UnavailableReason = %q, want %q（错误 ≠ roots 不在图）", rep.UnavailableReason, ReasonReachFailed)
	}
	r, _ := riskOf(rep, "branch-a1")
	if r.Available {
		t.Error("追闭包出错 → 该分支预测不可用")
	}
}

// TestPredictConflictsWrongDirection（p2-2）：闭包方向不是 DirectionIn → 按"图不可用"降级，
// 绝不能拿方向错的闭包当影响面（静默假阴性）。
//
// 变异自证：若不校验 cl.Direction，本测试会红（出向闭包会让 merged 的影响面变成
// "我依赖谁"，判据全对不上）。
func TestPredictConflictsWrongDirection(t *testing.T) {
	g := conflictGraph(t)
	// 方向接成 DirectionOut（"我依赖谁"）——语义要的是入向（"谁依赖我"）。
	reach := func(ctx context.Context, roots []string) (Closure, error) {
		return g.Reach(ctx, roots, "imports", DirectionOut, 0, 0)
	}
	merged := ChangeSet{Label: "merged", Roots: []string{"shared1"}}
	others := []ChangeSet{{Label: "branch-a1", Roots: []string{"a1"}}}
	rep := PredictConflicts(context.Background(), reach, merged, others)

	if rep.Available {
		t.Error("闭包方向不对 → 整体 Available 应为 false")
	}
	if rep.UnavailableReason != ReasonWrongDirection {
		t.Errorf("UnavailableReason = %q, want %q", rep.UnavailableReason, ReasonWrongDirection)
	}
	r, _ := riskOf(rep, "branch-a1")
	if r.Available {
		t.Error("闭包方向不对 → 该分支预测不可用")
	}
	// 方向错时**不能**拿错方向的闭包判撞车（那是从错的影响面推出的假信号）。
	if r.Collides {
		t.Errorf("方向不符的闭包不得产生撞车结论：basis=%v", r.Basis)
	}
}

// TestPredictConflictsAggregateHonesty（p2 / N-1）：**聚合层**不许报假"无冲突"。
//
// 合并侧可评估、但某个未合并分支的改动不在图覆盖内 → 聚合层曾报
// available=true + collisions=0 → 只读聚合的消费端读成"可用且 0 撞车" = **假"无冲突"**。
//
// 变异自证：若聚合层只看合并侧（rep.Available = mergedResolved），本测试会红。
func TestPredictConflictsAggregateHonesty(t *testing.T) {
	g := conflictGraph(t)
	reach := memReach(g, "imports")

	merged := ChangeSet{Label: "merged", Roots: []string{"shared1"}} // 合并侧可评估
	others := []ChangeSet{
		{Label: "branch-b", Roots: []string{"shared2"}},   // 可评估、无交集
		{Label: "branch-ghost", Roots: []string{"ghost"}}, // 不可评估（不在图里）
	}
	rep := PredictConflicts(context.Background(), reach, merged, others)

	if rep.Available {
		t.Errorf("有分支不可评估 → 聚合 Available 必须为 false（reason=%q）", rep.UnavailableReason)
	}
	if rep.UnavailableReason != ReasonBranchesUnevaluated {
		t.Errorf("UnavailableReason = %q, want %q", rep.UnavailableReason, ReasonBranchesUnevaluated)
	}
	if rep.Unevaluated != 1 {
		t.Errorf("Unevaluated = %d, want 1（不可评估分支数）", rep.Unevaluated)
	}
	// collisions 只数**确认命中**：不可评估的分支不计入（它是"无法排除"，不是"没撞"）。
	if rep.Collisions != 0 {
		t.Errorf("Collisions = %d, want 0（无确认命中）", rep.Collisions)
	}
	// 逐条看：可评估的那条仍能给结论，不可评估的那条明说"无法排除"。
	rOK, _ := riskOf(rep, "branch-b")
	if !rOK.Available || rOK.Reason != ReasonNoOverlap {
		t.Errorf("可评估分支应给\"无交集\"结论：%+v", rOK)
	}
	rBad, _ := riskOf(rep, "branch-ghost")
	if rBad.Available || rBad.Reason != ReasonCannotExclude {
		t.Errorf("不可评估分支应说\"无法排除\"：%+v", rBad)
	}
}

// TestPredictConflictsAggregateCountsConfirmedOnly（p2 / N-1）：确认命中照报、不可评估另计。
//
// 变异自证：若把不可评估分支也算进 collisions（"疑似撞车"），本测试会红。
func TestPredictConflictsAggregateCountsConfirmedOnly(t *testing.T) {
	g := conflictGraph(t)
	reach := memReach(g, "imports")

	merged := ChangeSet{Label: "merged", Roots: []string{"shared1"}}
	others := []ChangeSet{
		{Label: "branch-a1", Roots: []string{"a1"}},   // 确认命中（a1 在 merged 影响面内）
		{Label: "branch-ghost", Roots: []string{"g"}}, // 不可评估
	}
	rep := PredictConflicts(context.Background(), reach, merged, others)

	if rep.Collisions != 1 {
		t.Errorf("Collisions = %d, want 1（只数确认命中）", rep.Collisions)
	}
	if rep.Unevaluated != 1 {
		t.Errorf("Unevaluated = %d, want 1", rep.Unevaluated)
	}
	if rep.Available {
		t.Error("有分支不可评估 → 聚合 Available 必须为 false")
	}
}

// TestPredictConflictsAggregateAvailableWhenAllEvaluable（p2 / N-1）：全部可评估 → 聚合可用。
func TestPredictConflictsAggregateAvailableWhenAllEvaluable(t *testing.T) {
	g := conflictGraph(t)
	reach := memReach(g, "imports")

	merged := ChangeSet{Label: "merged", Roots: []string{"shared1"}}
	others := []ChangeSet{
		{Label: "branch-b", Roots: []string{"shared2"}},
		{Label: "branch-a1", Roots: []string{"a1"}},
	}
	rep := PredictConflicts(context.Background(), reach, merged, others)

	if !rep.Available {
		t.Errorf("全部可评估 → 聚合 Available 应为 true（reason=%q）", rep.UnavailableReason)
	}
	if rep.Unevaluated != 0 {
		t.Errorf("Unevaluated = %d, want 0", rep.Unevaluated)
	}
}

// TestReasonWordingNeutral（p3 / N-3）："影响面没追全"的文案**中性**——不得把截断说成
// "改动节点不在依赖图中"（成因归因错类）。
//
// 变异自证：若文案写死"改动节点不在依赖图中"，本测试会红。
func TestReasonWordingNeutral(t *testing.T) {
	if strings.Contains(ReasonRootsUnknown, "不在依赖图中") {
		t.Errorf("共享降级理由文案应中性（成因分不清时别硬指一种）：%q", ReasonRootsUnknown)
	}
	if !strings.Contains(ReasonRootsUnknown, "影响面没追全") {
		t.Errorf("降级理由应说明\"影响面没追全\"：%q", ReasonRootsUnknown)
	}
	// 截断场景的报告级理由同样中性（它与"触点不在图"共用一个常量）。
	g := conflictGraph(t)
	inner := memReach(g, "imports")
	reach := func(ctx context.Context, roots []string) (Closure, error) {
		cl, err := inner(ctx, roots)
		if err != nil {
			return cl, err
		}
		cl.Truncated = true
		return cl, nil
	}
	rep := PredictConflicts(context.Background(), reach,
		ChangeSet{Label: "merged", Roots: []string{"shared1"}},
		[]ChangeSet{{Label: "branch-b", Roots: []string{"shared2"}}})
	if strings.Contains(rep.UnavailableReason, "不在依赖图中") {
		t.Errorf("截断时的报告级理由不得说\"不在依赖图中\"：%q", rep.UnavailableReason)
	}
}

// TestPredictConflictsSiblingLeavesNoCollision（p2-1）：**兄弟叶子**不报撞车。
//
// 图 c→a、c→b：改 a 与改 b 的影响面**有交集**（都含 c），但 a 与 b 谁也不碰谁。
// 变异自证：若用对称判据（影响面相交），本测试会红。
func TestPredictConflictsSiblingLeavesNoCollision(t *testing.T) {
	g := siblingGraph(t)
	reach := memReach(g, "imports")

	merged := ChangeSet{Label: "merged", Roots: []string{"a"}}
	others := []ChangeSet{{Label: "branch-b", Roots: []string{"b"}}}
	rep := PredictConflicts(context.Background(), reach, merged, others)

	if rep.Collisions != 0 {
		t.Errorf("兄弟叶子不该判撞车：collisions=%d, risks=%+v", rep.Collisions, rep.Risks)
	}
	r, _ := riskOf(rep, "branch-b")
	if r.Collides {
		t.Errorf("兄弟叶子不该预警：basis=%v", r.Basis)
	}
	if !r.Available {
		t.Error("图可用且两边都在图里 → Available 应为 true")
	}
	if r.Reason != ReasonNoOverlap {
		t.Errorf("Reason = %q, want %q", r.Reason, ReasonNoOverlap)
	}
}

// TestPredictConflictsConsumerVsLibrary（p2-1）：**消费者 vs 库**要报撞车（传递信号）。
//
// 合并方改库 lib，未合并方改它的消费者 app：app 落在合并方的影响面内 → 撞车。
// 变异自证：若只比 roots 直接触点（lib vs app 不相交），本测试会红。
func TestPredictConflictsConsumerVsLibrary(t *testing.T) {
	g := consumerGraph(t)
	reach := memReach(g, "imports")

	merged := ChangeSet{Label: "merged", Roots: []string{"lib"}}
	others := []ChangeSet{{Label: "branch-app", Roots: []string{"app"}}}
	rep := PredictConflicts(context.Background(), reach, merged, others)

	if rep.Collisions != 1 {
		t.Fatalf("消费者 vs 库应判撞车：collisions=%d, risks=%+v", rep.Collisions, rep.Risks)
	}
	r, _ := riskOf(rep, "branch-app")
	if !r.Collides {
		t.Fatal("消费者 vs 库应预警撞车")
	}
	if strings.Join(r.Basis, ",") != "one-side:app" {
		t.Errorf("Basis = %v, want [one-side:app]（传递命中）", r.Basis)
	}
}

// TestConflictPredictedEvent：报告 → conflict.predicted payload 映射（含诚实降级字段）。
func TestConflictPredictedEvent(t *testing.T) {
	rep := ConflictReport{
		MergedBranch:      "merged",
		Available:         false,
		UnavailableReason: ReasonGraphDisabled,
		Collisions:        1,
		Unevaluated:       1,
		Risks: []ConflictRisk{
			{Branch: "branch-same", Collides: true, Basis: []string{"direct:shared1"}, Available: false, Reason: "改动触点重叠"},
		},
	}
	p := ConflictPredictedEvent(rep)
	if p.MergedBranch != "merged" || p.Available || p.Collisions != 1 {
		t.Errorf("映射错：%+v", p)
	}
	if p.UnavailableReason == nil || *p.UnavailableReason != ReasonGraphDisabled {
		t.Errorf("UnavailableReason 应透传：%v", p.UnavailableReason)
	}
	if p.Unevaluated == nil || *p.Unevaluated != 1 {
		t.Errorf("unevaluated 应透传（不可评估分支数）：%v", p.Unevaluated)
	}
	if len(p.Risks) != 1 || p.Risks[0].Branch != "branch-same" || !p.Risks[0].Collides {
		t.Errorf("risks 映射错：%+v", p.Risks)
	}
	if p.Risks[0].Reason == nil || *p.Risks[0].Reason != "改动触点重叠" {
		t.Errorf("risk.reason 应透传：%v", p.Risks[0].Reason)
	}
}

// TestConflictPredictedEventOmitsZeroUnevaluated：没有不可评估分支时**不带** unevaluated
// （可选字段：0 = "没有"，省略更干净；契约是 *int64）。
func TestConflictPredictedEventOmitsZeroUnevaluated(t *testing.T) {
	rep := ConflictReport{
		MergedBranch: "merged",
		Available:    true,
		Collisions:   0,
		Risks: []ConflictRisk{
			{Branch: "branch-b", Collides: false, Available: true, Reason: ReasonNoOverlap},
		},
	}
	p := ConflictPredictedEvent(rep)
	if p.Unevaluated != nil {
		t.Errorf("无不可评估分支时 unevaluated 应缺省：%v", *p.Unevaluated)
	}
}

// TestConflictPredictedEventCopiesBasis（p3-5）：映射出去的 basis 是**拷贝**——调用方改它
// 不得改坏报告（对齐 core/gate 的 cloneStrings 纪律）。
//
// 变异自证：若映射时直接共享切片，本测试会红。
func TestConflictPredictedEventCopiesBasis(t *testing.T) {
	rep := ConflictReport{
		MergedBranch: "merged",
		Available:    true,
		Risks: []ConflictRisk{
			{Branch: "b", Collides: true, Basis: []string{"direct:x"}, Available: true},
		},
	}
	p := ConflictPredictedEvent(rep)
	if len(p.Risks) != 1 || len(p.Risks[0].Basis) != 1 {
		t.Fatalf("basis 映射错：%+v", p.Risks)
	}
	p.Risks[0].Basis[0] = "MUTATED"
	if rep.Risks[0].Basis[0] != "direct:x" {
		t.Errorf("映射出去的 basis 应是拷贝：报告里的 basis 被改成了 %q", rep.Risks[0].Basis[0])
	}
}

// TestConflictPredictedEventCarriesCoverage（p1-1 契约链）：覆盖度信号要进 payload
// （unknown_roots / truncated），消费端才知道"为什么不可用"。
func TestConflictPredictedEventCarriesCoverage(t *testing.T) {
	rep := ConflictReport{
		MergedBranch: "merged",
		Available:    false,
		Risks: []ConflictRisk{
			{
				Branch:       "b",
				Collides:     false,
				Available:    false,
				UnknownRoots: []string{"ghost"},
				Truncated:    true,
				Reason:       ReasonCannotExclude,
			},
		},
	}
	p := ConflictPredictedEvent(rep)
	r := p.Risks[0]
	if len(r.UnknownRoots) != 1 || r.UnknownRoots[0] != "ghost" {
		t.Errorf("unknown_roots 应透传：%+v", r.UnknownRoots)
	}
	if r.Truncated == nil || !*r.Truncated {
		t.Errorf("truncated 应透传：%+v", r.Truncated)
	}
}

// TestPredictConflictsEmptyOthers：无未合并分支 → 空报告、无撞车。
func TestPredictConflictsEmptyOthers(t *testing.T) {
	g := conflictGraph(t)
	rep := PredictConflicts(context.Background(), memReach(g, "imports"),
		ChangeSet{Label: "merged", Roots: []string{"shared1"}}, nil)
	if len(rep.Risks) != 0 || rep.Collisions != 0 {
		t.Errorf("无未合并分支应无撞车：%+v", rep)
	}
	if !rep.Available {
		t.Error("merged 改动可追 → Available 应为 true")
	}
}
