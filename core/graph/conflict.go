package graph

import (
	"context"
	"sort"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
)

// V7 合并后冲突预测（07 V 区 V7）：合并**成功后**，对"本次改动 vs 未合并的分支/任务"预警
// 撞车。**用依赖图可达性**（G 区 change_impact 的底层）比 tower 的"三点 diff 求交集"更准——
// 三点 diff 只看到文本触点相交，看不到"我改的依赖被你的改动传递影响"这种语义撞车。
//
// # 输入：diff 由调用方喂（v2-S2 口径，框架不算 diff）
//
// 每个 ChangeSet 只带 diff 触及的节点 id（Roots）；框架**不自己算 diff**（那是 git/工作流的
// 事），只拿 roots 追**入向影响闭包**（DirectionIn = 谁依赖我）做比对。
//
// # 撞车判据：**非对称**（p2-1 修正）
//
// 判据（三条命中类型）：
//
//   - **direct**（同触点）：rootsA ∩ rootsB ≠ ∅——两边改了同一个节点。**确定信号**，
//     不需要图；
//   - **one-side**（一侧触点落在另一侧影响面内）：rootsA ∩ closureB ≠ ∅（A 的触点是 B 的
//     下游/消费者）或 rootsB ∩ closureA ≠ ∅（closureX = X 的入向闭包节点，**不含** X 自己的
//     触点——触点相同的情形已由 direct 归类）。**传递信号**，依赖图的可达性结论。
//
// **不用**"两边影响面相交"（对称判据）：那是**误报源**——两条改动"共同影响同一个下游"
// 不等于撞车。图 `c→a`、`c→b`（c 同时依赖 a 与 b）时，改 a 与改 b 只是**兄弟叶子**，
// 谁也不碰谁，却会被对称判据判成撞车——连三点 diff 求交集都比它准。真实撞车要么是
// **同触点**（direct），要么是**一方的触点落在另一方的影响面里**（one-side）。
//
// # 判据的代价（明写，免得后人当成"无损替换"）
//
// 丢掉对称相交**同时也丢掉一类真阳性**：同一个消费者**同时依赖两个都被改的库**时
// （c→a、c→b，且 a、b 都在本次改动里），两边的影响面**相交于 c**，但按非对称判据
// **不报**（a、b 互不为对方的消费者）。这是个 **weak 信号**（"可能经由共同下游互相影响"），
// 不是确定撞车——留给消费端/后续按需叠加，框架**不**把它混进 confirmed collisions
// （否则又是一条误报源）。真要报它，判据得回到"影响面相交"并另配置信度分级。
//
// # 诚实降级（N2，v2-S2 挂账口径）
//
// 图投影被重建时清空与重放不是原子的——窗口内影响面可能"干净地为空"，消费端分不清"真没
// 影响"与"正在重建"。因此预测**绝不把"图不可用/覆盖不足"当成"无冲突"**：
//
//   - 图未启用（reach==nil）/ 追闭包出错 / 闭包方向不对 / 改动节点**部分**不在图中 /
//     闭包被上限**截断** → Available=false，报告明说"预测不可用"（UnavailableReason），
//     逐分支标 Available=false；
//   - 即便图不可用，**直接改动触点重叠**仍是确定信号（roots ∩ roots，不需图）→ 照常报
//     Collides；
//   - 图不可用且无直接触点重叠 → Collides=false 但 Available=false + ReasonCannotExclude
//     （"无法排除传递撞车"），**不是**"无冲突"。
//
// # 聚合层也得诚实（N-1）
//
// 逐条诚实还不够：**只读聚合层**的消费端（不逐条看 risks[]）会把"合并侧可评估、但某个
// 未合并分支不可评估"读成"available=true + collisions=0" = **假"无冲突"**。所以聚合层
// 的 Available = 合并侧可评估 **且** 每个未合并分支都可评估；不可评估的分支数记在
// Unevaluated，Collisions 只数**确认命中**（不可评估 ≠ 没撞）。
//
// # 覆盖度信号不得被丢弃（p1-1 修正）
//
// 闭包的 MissingRoots（部分触点不在图里）与 Truncated（被结果上限截断）是**覆盖度**信号：
// 它们不影响"已经找到的撞点"（找到的就是真的），但**决定"没找到"能不能说"无交集"**。
// 丢掉它们 = 把"只覆盖了一半的影响面"当成"完全可用" → 报 Available=true + "影响面无交集"
// ——**假"无冲突"**。所以：任一触点缺失 / 闭包截断 → 该侧影响面**不可信** → 整体
// Available=false，且逐分支的 Reason **禁用**"影响面无交集"措辞（改"无法排除"口径）。

// 降级 / 结论理由（进 ConflictRisk.Reason 与 ConflictReport.UnavailableReason；导出供测试断言）。
//
// 报告级只分**四类**降级：图未启用 / 查询出错 / 闭包方向不对 / **影响面没追全**
// （触点不在图里 **或** 闭包被截断）。最后一类共用一个理由常量
// （ReasonRootsUnknown ≡ ReasonTruncated，**同值**）——报告级只说"没追全"（中性文案：
// 两种成因都不是"节点不在图里"这一条能概括的），**精确到哪一种**由逐分支的
// unknown_roots / truncated 字段给出。
const (
	// ReasonGraphDisabled：图投影未启用（G2f 可选投影）。
	ReasonGraphDisabled = "图投影未启用（G2f 可选投影）：冲突预测不可用"
	// ReasonRootsUnknown：**影响面没追全**——触点不在依赖图里 **或** 闭包被结果上限截断。
	//
	// 文案中性（不把两种成因互相误指）：名字里的 "Roots" 指"影响面没追全"这一类降级。
	ReasonRootsUnknown = "影响面没追全（触点不在图里或闭包被截断）：冲突预测不可用"
	// ReasonCannotExclude：图未覆盖该分支，传递撞车无法排除（诚实降级，非无冲突）。
	ReasonCannotExclude = "图未覆盖该分支改动，传递撞车无法排除（诚实降级，非无冲突）"
	// ReasonNoOverlap：图可用且影响面无交集（这才是真的"不撞车"）。
	ReasonNoOverlap = "影响面无交集"
	// ReasonReachFailed：追闭包**出错**（可达性查询失败）——与"节点不在图里"不同类，
	// 不该混为一谈（p3-4：追不出 ≠ 不在图）。
	ReasonReachFailed = "依赖图可达性查询失败，传递影响面无法追：冲突预测不可用"
	// ReasonWrongDirection：闭包方向**不是** DirectionIn（语义要入向；接成出向会静默
	// 假阴性——把"谁依赖我"追成"我依赖谁"，影响面对不上还不报错）。
	ReasonWrongDirection = "可达性闭包方向不是入向（DirectionIn），影响面语义不符：冲突预测不可用"
	// ReasonTruncated：闭包被结果上限**截断**——影响面可能不完整，不能断言"无交集"。
	//
	// **与 ReasonRootsUnknown 同值**（刻意）：报告级的降级理由只说"影响面没追全"
	// （两者都是这一类），精确到"触点不在图"还是"被截断"由逐分支的 unknown_roots /
	// truncated 字段给出——不把两种成因硬塞进一句话里互相误指。
	ReasonTruncated = ReasonRootsUnknown
	// ReasonBranchesUnevaluated：**有未合并分支不可评估**（合并侧可评估，但某些分支的改动
	// 不在图覆盖内 / 影响面没追全）→ 报告级诚实降级（不是"无冲突"）。
	ReasonBranchesUnevaluated = "有未合并分支不可评估（改动不在图覆盖内或影响面没追全），传递撞车无法排除（诚实降级，非无冲突）"
)

// Basis 命中类型（Basis 条目的前缀：`<命中类型>:<节点 id>`）。
const (
	// BasisDirect：同触点（两边改了同一个节点）——**确定信号**，不需要图。
	BasisDirect = "direct"
	// BasisOneSide：一侧触点落在另一侧影响面内——**传递信号**，依赖图的可达性结论。
	BasisOneSide = "one-side"
)

// ChangeSet 描述一次改动（一个分支 / 任务）的 diff 触点（V7）。
type ChangeSet struct {
	// Label 是分支 / 任务标识（报告与事件用）。
	Label string
	// Roots 是 diff 触及的节点 id（改动起点；由调用方从 git diff 等处取得，框架不算 diff）。
	Roots []string
}

// ConflictRisk 是对单个未合并分支的撞车预判（V7）。
type ConflictRisk struct {
	// Branch 是未合并分支 / 任务标识。
	Branch string `json:"branch"`
	// Collides 是否预判撞车（按非对称判据命中 direct / one-side）。
	Collides bool `json:"collides"`
	// Basis 是撞车依据（`<命中类型>:<节点>`，已排序；Collides=true 时非空）。
	//
	// 命中类型分级可辨：**direct** = 同触点（确定信号）；**one-side** = 一侧触点落在
	// 另一侧影响面内（传递信号）。消费端据此区分"必然撞"与"可能撞"。
	Basis []string `json:"basis,omitempty"`
	// Available 预测是否可用（两侧影响面都**完整可信**：触点全在图里、未截断、方向正确、
	// 查询无错）。
	// false 且 Collides=false = 预测不可用（诚实降级），**不是"无冲突"**（N2）。
	Available bool `json:"available"`
	// UnknownRoots 是**不在图里**的改动触点（两侧合并；诚实信号：这些触点的传递影响面
	// 追不出去 → 不能断言"无交集"）。
	UnknownRoots []string `json:"unknown_roots,omitempty"`
	// Truncated 表示影响面被结果上限截断（可能不完整 → 不能断言"无交集"）。
	Truncated bool `json:"truncated,omitempty"`
	// Reason 是人类可读说明（撞车依据 / 无交集 / 图不可用）。
	Reason string `json:"reason,omitempty"`
}

// ConflictReport 是合并后冲突预测报告（V7）。
type ConflictReport struct {
	// MergedBranch 是本次合并的分支标识。
	MergedBranch string `json:"merged_branch"`
	// Available **聚合**预测是否可用 = 合并侧可评估 **且** 每个未合并分支都可评估。
	//
	// **不许只读聚合层**（N-1）：合并侧可评估、但某些分支不可评估时，聚合层曾报
	// available=true + collisions=0 → 只读聚合的消费端把它读成"可用且 0 撞车" =
	// **假"无冲突"**。现在聚合层诚实化：任一分支不可评估 → available=false +
	// Unevaluated>0 + UnavailableReason 说明。
	//
	// 即便聚合层可用，逐条结论仍以 Risks[].Available 为准（每条分支各自的覆盖度）。
	Available bool `json:"available"`
	// UnavailableReason 是 Available=false 时的诚实说明（图未启用 / 追闭包出错 / 方向不对 /
	// 影响面没追全 / 有分支不可评估）。
	UnavailableReason string `json:"unavailable_reason,omitempty"`
	// Collisions 是**确认命中**（判据命中）的未合并分支数。
	//
	// **只计确认命中**：不可评估的分支不计入（它们的传递撞车**无法排除**，不是"没撞"），
	// 那些分支的条数在 Unevaluated 里。
	Collisions int `json:"collisions"`
	// Unevaluated 是**不可评估**的未合并分支数（该分支的传递撞车无法排除）。
	//
	// 恒等式：len(Risks) = 确认命中数 + 不可评估数（可评估且未命中的分支既不撞车也不计入
	// 本字段）。
	Unevaluated int `json:"unevaluated,omitempty"`
	// Risks 是逐分支的撞车预判明细（**逐条看 Risks[].Available**——它才是每条分支自己的
	// 覆盖度结论）。
	Risks []ConflictRisk `json:"risks"`
}

// ConflictReach 是冲突预测依赖的可达性查询（V7）。
//
// 语义：给定改动起点 roots，返回其**入向影响闭包**（DirectionIn = 谁依赖这些节点；
// change_impact 的口径）。kind / 深度 / 上限等查询参数由调用方**烘焙进闭包**——
// 生产侧用 core/graph.Graph.Reachable 适配，测试侧可直接喂 MemGraph.Reach 的薄包装。
//
// # 调用方必须烘焙的两件事（p2-2）
//
//   - **方向必须是 DirectionIn**（入向 = 谁依赖我）：闭包方向不对时，本包**按"图不可用"
//     降级**（UnavailableReason=ReasonWrongDirection），**不**拿方向不符的闭包当影响面
//     ——出向闭包会把"谁依赖我"追成"我依赖谁"，撞车判据全部对不上，且**静默假阴性**
//     （看上去"无交集"，其实方向反了）。
//   - **kind 由调用方选**（"" = 不限边类型）：不限 kind 会**放大命中面**（把不相关的边
//     也算进影响面 → 撞车预警变多）。生产侧应选**依赖类 kind**（如 "imports"），
//     把命中面收在真正的依赖边上。
//
// reach == nil = 图投影未启用（G2f）→ 预测降级为仅比对直接改动触点（诚实标注不可用）。
type ConflictReach func(ctx context.Context, roots []string) (Closure, error)

// PredictConflicts 是合并后冲突预测（V7）：对刚合并的改动 vs 未合并分支，用依赖图可达性
// 预判撞车。
//
// reach 提供入向影响闭包查询（nil = 图未启用）。merged 是本次合并的改动，others 是未合并
// 分支们的改动。返回诚实报告：撞车的分支 + 依据；图不可用 / 覆盖不足时明说"预测不可用"
// 而非"无冲突"。
func PredictConflicts(ctx context.Context, reach ConflictReach, merged ChangeSet, others []ChangeSet) ConflictReport {
	if ctx == nil {
		ctx = context.Background()
	}
	rep := ConflictReport{MergedBranch: merged.Label}

	graphEnabled := reach != nil

	// 本次合并改动的影响集（roots ∪ 入向闭包）+ 覆盖度信号。
	mergedImpact := impactOf(ctx, reach, merged.Roots)
	// 合并侧是否可评估（**逐分支结论的基准**：合并侧不可信 → 每个分支都不可评估）。
	mergedOK := graphEnabled && mergedImpact.resolved
	switch {
	case !graphEnabled:
		rep.UnavailableReason = ReasonGraphDisabled
	case mergedImpact.err != nil:
		rep.UnavailableReason = ReasonReachFailed
	case mergedImpact.wrongDir:
		rep.UnavailableReason = ReasonWrongDirection
	case len(mergedImpact.missing) > 0:
		rep.UnavailableReason = ReasonRootsUnknown
	case mergedImpact.truncated:
		rep.UnavailableReason = ReasonTruncated
	case len(mergedImpact.roots) == 0:
		// 没有触点 = 改了什么都不知道 → 传递撞车无法排除（不是"无冲突"）。
		rep.UnavailableReason = ReasonRootsUnknown
	}

	for _, o := range others {
		risk := ConflictRisk{Branch: o.Label}
		oImpact := impactOf(ctx, reach, o.Roots)
		risk.Available = mergedOK && oImpact.resolved
		risk.UnknownRoots = unionSorted(mergedImpact.missing, oImpact.missing)
		risk.Truncated = mergedImpact.truncated || oImpact.truncated

		basis := basisOf(mergedImpact, oImpact)
		switch {
		case len(basis) > 0:
			risk.Collides = true
			risk.Basis = basis
			risk.Reason = basisReason(basis, risk.Available)
		case !risk.Available:
			// 无命中，但影响面不可信 → 无法排除传递撞车（诚实降级，非无冲突）。
			// **禁用"影响面无交集"措辞**（p1-1：覆盖不足时说"无交集" = 假无冲突）。
			risk.Reason = ReasonCannotExclude
		default:
			risk.Reason = ReasonNoOverlap
		}
		if !risk.Available {
			// 不可评估的分支：传递撞车**无法排除**（不是"没撞"）→ 计入 Unevaluated，
			// 不计入 Collisions（Collisions 只数确认命中）。
			rep.Unevaluated++
		}
		if risk.Collides {
			rep.Collisions++
		}
		rep.Risks = append(rep.Risks, risk)
	}
	// **聚合诚实化**（N-1）：合并侧可评估 **且** 每个分支都可评估 → 聚合层才敢叫"可用"。
	// 否则聚合层报 available=true + collisions=0，只读聚合的消费端会把它读成
	// "可用且 0 撞车" = **假"无冲突"**。
	rep.Available = mergedOK && rep.Unevaluated == 0
	if !rep.Available && rep.UnavailableReason == "" {
		// 合并侧没问题，是**分支侧**不可评估 → 说清楚是哪一类。
		rep.UnavailableReason = ReasonBranchesUnevaluated
	}
	return rep
}

// impact 是一个改动的影响面结果（roots ∪ 入向闭包）+ **覆盖度诚实信号**。
//
// 覆盖度信号（missing / truncated / wrongDir / err）不影响"已经找到的撞点"（找到的就是
// 真的），但决定"没找到"能不能说"无交集"——这是 p1-1 的核心。
type impact struct {
	// roots 是改动触点（去空、去重、排序）。
	roots []string
	// rootSet 是 roots 的集合视图。
	rootSet map[string]bool
	// closure 是**闭包节点**（不含 roots 自身；方向不对 / 查询出错时为空）。
	closure map[string]bool
	// missing 是图里没有的触点（Closure.MissingRoots，已排序）。
	missing []string
	// truncated 表示闭包被结果上限截断（影响面可能不完整）。
	truncated bool
	// wrongDir 表示闭包方向不是 DirectionIn（语义不符，闭包不可用）。
	wrongDir bool
	// err 是追闭包时的错误（非 nil → 闭包不可用）。
	err error
	// resolved 表示影响面**完整可信**：有触点、触点全在图里、未截断、方向正确、无错。
	resolved bool
}

// impactOf 算一个改动的影响面 = roots ∪ 入向闭包，并带回**覆盖度诚实信号**。
//
// 返回的 resolved=false 时：影响集退化为"仅 roots"（方向不对 / 查询出错）或"部分闭包"
// （有缺失触点 / 截断）——调用方据此**禁用**"影响面无交集"的结论（N2）。
func impactOf(ctx context.Context, reach ConflictReach, roots []string) impact {
	im := impact{rootSet: map[string]bool{}, closure: map[string]bool{}}
	for _, r := range roots {
		if r == "" || im.rootSet[r] {
			continue
		}
		im.rootSet[r] = true
		im.roots = append(im.roots, r)
	}
	sort.Strings(im.roots)
	if reach == nil || len(im.roots) == 0 {
		return im // 没有图 / 没有触点 → 影响面不可信
	}
	cl, err := reach(ctx, im.roots)
	if err != nil {
		// 追闭包**出错** ≠ "节点不在图里"（p3-4）：两类原因分开记。
		im.err = err
		return im
	}
	if cl.Direction != DirectionIn {
		// 方向不符 → 闭包不可用（出向闭包 = 追错了方向，静默假阴性）。
		im.wrongDir = true
		return im
	}
	im.missing = append([]string(nil), cl.MissingRoots...)
	sort.Strings(im.missing)
	im.truncated = cl.Truncated
	for _, n := range cl.Nodes {
		if n != "" {
			im.closure[n] = true
		}
	}
	// 完整可信 = 至少一个触点在图里 + **没有**缺失触点 + **没有**截断（p1-1）。
	im.resolved = len(cl.Roots) > 0 && len(im.missing) == 0 && !im.truncated
	return im
}

// basisOf 按**非对称**判据算撞车依据（p2-1）。
//
// 判据（命中类型分级）：
//
//   - direct：rootsA ∩ rootsB ≠ ∅（同触点；不依赖图，确定信号）；
//   - one-side：rootsA ∩ closureB ≠ ∅（A 的触点是 B 的下游/消费者）；
//   - one-side：rootsB ∩ closureA ≠ ∅（B 的触点是 A 的下游/消费者）。
//
// **不**用 impactA ∩ impactB（对称判据）："共同影响同一个下游"不等于撞车
// （图 c→a、c→b：改 a 与改 b 是兄弟叶子，谁也不碰谁——对称判据会误报）。
//
// 输出按"先 direct、后 one-side"分组，组内按节点 id 排序（确定性输出）。
func basisOf(a, b impact) []string {
	direct := sortedIntersect(a.rootSet, b.rootSet)
	// one-side：A 的触点落在 B 的影响面内 / B 的触点落在 A 的影响面内。
	oneA := sortedIntersect(a.rootSet, b.closure)
	oneB := sortedIntersect(b.rootSet, a.closure)

	out := make([]string, 0, len(direct)+len(oneA)+len(oneB))
	hitDirect := map[string]bool{}
	for _, n := range direct {
		hitDirect[n] = true
		out = append(out, BasisDirect+":"+n)
	}
	for _, set := range [][]string{oneA, oneB} {
		for _, n := range set {
			if hitDirect[n] {
				continue // 已是 direct，不再重复记为 one-side
			}
			out = append(out, BasisOneSide+":"+n)
		}
	}
	return out
}

// basisReason 把撞车依据折成人类可读理由（命中类型分级可辨）。
//
// available=false 时补一句"图覆盖不足"——此时撞点是真的，但**传递面可能不完整**
// （还可能有别的撞点没查出来）。
func basisReason(basis []string, available bool) string {
	var direct, oneSide []string
	for _, b := range basis {
		kind, node := splitBasis(b)
		switch kind {
		case BasisDirect:
			direct = append(direct, node)
		default:
			oneSide = append(oneSide, node)
		}
	}
	var head string
	switch {
	case len(direct) > 0 && len(oneSide) > 0:
		head = "改动触点重叠 + 一侧触点落在另一侧影响面内"
	case len(direct) > 0:
		head = "改动触点重叠（同触点，确定信号）"
	default:
		head = "一侧触点落在另一侧影响面内（传递信号）"
	}
	nodes := append(append([]string{}, direct...), oneSide...)
	reason := head + "：" + strings.Join(nodes, ", ")
	if !available {
		reason += "（图覆盖不足：传递面可能不完整）"
	}
	return reason
}

// splitBasis 拆 Basis 条目的命中类型与节点 id（`<命中类型>:<节点>`）。
func splitBasis(entry string) (kind, node string) {
	if i := strings.Index(entry, ":"); i >= 0 {
		return entry[:i], entry[i+1:]
	}
	return "", entry
}

// sortedIntersect 返回两个集合的交集（排序，确定性输出）。
func sortedIntersect(a, b map[string]bool) []string {
	var out []string
	for k := range a {
		if b[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// unionSorted 合并两个已排序的字符串集合（去重、保序、已排序）。
func unionSorted(a, b []string) []string {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, list := range [][]string{a, b} {
		for _, s := range list {
			if s == "" || seen[s] {
				continue
			}
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// KindConflictPredicted 是合并后冲突预测落库的事件 kind（durable=true；V7）。
// 与 core/gate.KindReviewRecorded 同款口径：core 定义 kind 常量，装配层包 envelope + 落库。
const KindConflictPredicted = "conflict.predicted"

// ConflictPredictedEvent 把冲突预测报告映射成 conflict.predicted 事件的 payload（V7）。
// 与 core/gate.CheckedEvent 同款：只产 payload，包 envelope / 落库在装配层。
//
// Basis **拷贝**后映射（p3-5：对齐 core/gate 的 cloneStrings 纪律——映射出去的可变切片
// 不与报告共享底层数组，否则调用方一改就把报告改了）。
func ConflictPredictedEvent(rep ConflictReport) contracts.ConflictPredictedPayload {
	risks := make([]contracts.ConflictRisk, 0, len(rep.Risks))
	for _, r := range rep.Risks {
		cr := contracts.ConflictRisk{
			Branch:       r.Branch,
			Collides:     r.Collides,
			Basis:        copyStrings(r.Basis),
			Available:    r.Available,
			UnknownRoots: copyStrings(r.UnknownRoots),
		}
		if r.Truncated {
			// 契约里 truncated 是**可选**布尔（codegen → *bool）：只在真的截断时给出。
			truncated := true
			cr.Truncated = &truncated
		}
		if r.Reason != "" {
			reason := r.Reason
			cr.Reason = &reason
		}
		risks = append(risks, cr)
	}
	p := contracts.ConflictPredictedPayload{
		MergedBranch: rep.MergedBranch,
		Available:    rep.Available,
		Collisions:   int64(rep.Collisions),
		Risks:        risks,
	}
	if rep.Unevaluated > 0 {
		// 契约里 unevaluated 是**可选**整数（codegen → *int64）：只在真有不可评估分支时给出。
		unevaluated := int64(rep.Unevaluated)
		p.Unevaluated = &unevaluated
	}
	if rep.UnavailableReason != "" {
		reason := rep.UnavailableReason
		p.UnavailableReason = &reason
	}
	return p
}

// copyStrings 拷贝字符串切片（映射出去的可变切片不与源共享底层数组）。
func copyStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}
