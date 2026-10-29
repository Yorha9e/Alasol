package graph

import (
	"fmt"
	"sort"
	"strings"
)

// DiffStat 是 diff 规模（W16 的两个客观依据之一："diff 规模"）。
//
// 框架不自己算 diff（那是 git/v3 工作流层的事）——调用方把规模喂进来，
// 本包只负责把它折进影响面判定。
type DiffStat struct {
	// Files 是改动文件数。
	Files int `json:"files"`
	// Added / Deleted 是增删行数。
	Added   int `json:"added"`
	Deleted int `json:"deleted"`
}

// Lines 是 diff 的总行数（增+删）。
func (d DiffStat) Lines() int { return d.Added + d.Deleted }

// Scale 是影响面量级（给 UI / agent 的粗分类；判定依据见 AssembleImpact）。
type Scale string

const (
	// ScaleSmall 影响面小（各项都远低于阈值）。
	ScaleSmall Scale = "small"
	// ScaleMedium 影响面中等（达到 escalate 阈值的一半）。
	ScaleMedium Scale = "medium"
	// ScaleLarge 影响面大（触发 escalate 条件）。
	ScaleLarge Scale = "large"
)

// Thresholds 是 change_impact 的 escalate 判定阈值（W15/W16：风险闸门而非预算上限）。
//
// 口径：**超过**阈值才触发（不是达到）。0 = 用默认；负值 = 拒绝。
type Thresholds struct {
	// MaxReachable 可达节点数阈值（0 = DefaultEscalateReachable）。
	MaxReachable int `json:"max_reachable"`
	// MaxDirect 一跳下游数阈值（0 = DefaultEscalateDirect）。
	MaxDirect int `json:"max_direct"`
	// MaxDiffLines diff 行数阈值（0 = DefaultEscalateDiffLines）。
	MaxDiffLines int `json:"max_diff_lines"`
	// MaxCoreTouched 触及核心接口数阈值（0 = 触及**任何一个**核心接口即 escalate）。
	MaxCoreTouched int `json:"max_core_touched"`
}

// DefaultThresholds 返回默认阈值。
func DefaultThresholds() Thresholds {
	return Thresholds{
		MaxReachable:   DefaultEscalateReachable,
		MaxDirect:      DefaultEscalateDirect,
		MaxDiffLines:   DefaultEscalateDiffLines,
		MaxCoreTouched: 0,
	}
}

// normalize 归一化阈值（0 → 默认；负 → 错）。
func (t Thresholds) normalize() (Thresholds, error) {
	out := t
	if out.MaxReachable < 0 || out.MaxDirect < 0 || out.MaxDiffLines < 0 || out.MaxCoreTouched < 0 {
		return Thresholds{}, fmt.Errorf("graph: escalate 阈值不能为负（0 = 默认）")
	}
	if out.MaxReachable == 0 {
		out.MaxReachable = DefaultEscalateReachable
	}
	if out.MaxDirect == 0 {
		out.MaxDirect = DefaultEscalateDirect
	}
	if out.MaxDiffLines == 0 {
		out.MaxDiffLines = DefaultEscalateDiffLines
	}
	return out, nil
}

// CoreSet 是"核心接口"节点集（W16 的第二个客观依据：改动是否触及核心接口）。
//
// **大小写敏感、字节精确**（陷阱 17：node id / edge key 不折叠大小写——
// "Core" 与 "core" 是两个不同节点，折叠会让判定悄悄放过/错杀）。
type CoreSet struct {
	// IDs 是精确匹配的节点 id。
	IDs []string
	// Prefixes 是字节前缀匹配（如 "core/" = core 目录下一切节点）。
	Prefixes []string
}

// Empty 报告核心接口集是否为空（空集 = 不做"触及核心接口"判定）。
func (c CoreSet) Empty() bool { return len(c.IDs) == 0 && len(c.Prefixes) == 0 }

// IsCore 判定某节点是否是核心接口。
func (c CoreSet) IsCore(id string) bool {
	for _, exact := range c.IDs {
		if id == exact {
			return true
		}
	}
	for _, p := range c.Prefixes {
		if p != "" && strings.HasPrefix(id, p) {
			return true
		}
	}
	return false
}

// ParseCoreSet 把配置里的声明解析成 CoreSet。
//
// 形态：以 `*` 结尾的是**前缀**（去掉 `*` 后按字节前缀匹配），其余是**精确 id**。
// 例：["core/store.go", "core/*", "contracts/*"]。
func ParseCoreSet(specs []string) CoreSet {
	var out CoreSet
	for _, raw := range specs {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if strings.HasSuffix(s, "*") {
			out.Prefixes = append(out.Prefixes, strings.TrimSuffix(s, "*"))
			continue
		}
		out.IDs = append(out.IDs, s)
	}
	return out
}

// ImpactRequest 是 change_impact 的输入（G21/W16）。
type ImpactRequest struct {
	// Roots 是改动起点（diff 触及的节点 id；由调用方从 git diff 等处取得）。
	Roots []string
	// Kind 是沿哪种边追影响面（"" = 不限 kind）。大小写敏感。
	Kind string
	// MaxDepth / MaxResults 是遍历上限（0 = 默认；负值拒绝）。
	//
	// MaxDepth 是"≤ depth"口径；MaxResults 是**总**上限（所有层加起来，F2）。
	MaxDepth   int
	MaxResults int
	// Core 是核心接口集（W16：判定"触及的核心接口"）。
	Core CoreSet
	// Thresholds 是 escalate 判定阈值（0 = 默认）。
	Thresholds Thresholds
	// Diff 是 diff 规模（可选；不提供就不参与判定）。
	Diff *DiffStat
}

// ImpactReport 是 change_impact 的影响面报告（G21/W16）——escalate 闸门的**客观依据**。
//
// 设计约束：**纯数据 + JSON tag**，可直接喂给 v3 的 escalate/workflow 消费端
// （本刀不接消费端，见包 doc）。判定结论（Scale / Escalate / Reasons）与事实
// （Direct / Transitive / CoreTouched / Diff）分开列——消费端可以只看事实自己判，
// 也可以直接用框架的结论。
type ImpactReport struct {
	// Roots 是输入起点（已去重、去空、排序）。
	Roots []string `json:"roots"`
	// Kind / MaxDepth 回显查询条件。
	Kind     string `json:"kind"`
	MaxDepth int    `json:"max_depth"`
	// Direction 回显**实际遍历的方向**（F16）。
	//
	// 报告不再靠"注释里说影响面 = 入向"来暗示方向：消费端（escalate 闸门）
	// 直接看这个字段就知道 Direct/Transitive 是沿哪边追出来的
	// （影响面 = DirectionIn = 1；出向 = 0）。
	Direction Direction `json:"direction"`
	// Direct 是一跳下游（"直接依赖改动面的那些"）。
	Direct []string `json:"direct_dependents"`
	// Transitive 是传递下游（两跳及以后）。
	Transitive []string `json:"transitive_dependents"`
	// ReachableCount 是受影响节点总数（Direct + Transitive，不含起点自身）。
	ReachableCount int `json:"reachable_count"`
	// CoreTouched 是受影响节点里**触及的核心接口**（W16 第二个客观依据）。
	CoreTouched []string `json:"core_interfaces_touched,omitempty"`
	// UnknownRoots 是图里没有的起点（诚实信号：这些改动面的下游追不出去）。
	UnknownRoots []string `json:"unknown_roots,omitempty"`
	// Truncated 表示遍历被上限截断（影响面可能不完整）。
	Truncated bool `json:"truncated"`
	// Diff 回显 diff 规模（若提供）。
	Diff *DiffStat `json:"diff,omitempty"`
	// Scale 是影响面量级。
	Scale Scale `json:"scale"`
	// Escalate 是框架给的 escalate 建议（W15：风险闸门；最终决策权在 agent/工作流）。
	Escalate bool `json:"suggest_escalate"`
	// Reasons 是判定理由（模型可读；空 = 不触发 escalate）。
	Reasons []string `json:"escalate_reasons,omitempty"`
}

// AssembleImpact 把一次可达性遍历的结果装配成 change_impact 报告（纯函数，可单测）。
//
// 输入 closure 通常是 DirectionIn（影响面 = 谁依赖我）的遍历结果；传入其它方向
// 会被当成"沿该方向的可达集"处理（不猜、不改写），**方向由报告的 Direction 字段回显**
// （F16）——消费端据此判断 Direct/Transitive 是哪边的下游。
func AssembleImpact(req ImpactRequest, c Closure) ImpactReport {
	th, err := req.Thresholds.normalize()
	if err != nil {
		th = DefaultThresholds()
	}
	maxDepth := req.MaxDepth
	if maxDepth <= 0 {
		maxDepth = DefaultMaxDepth
	}

	roots := dedupSorted(req.Roots)

	// Direct = 一跳层；Transitive = 其余层。
	var direct, transitive []string
	if len(c.Levels) > 0 {
		direct = append(direct, c.Levels[0]...)
	}
	directSet := make(map[string]struct{}, len(direct))
	for _, d := range direct {
		directSet[d] = struct{}{}
	}
	for _, n := range c.Nodes {
		if _, ok := directSet[n]; !ok {
			transitive = append(transitive, n)
		}
	}
	sort.Strings(direct)
	sort.Strings(transitive)

	rep := ImpactReport{
		Roots:          roots,
		Kind:           req.Kind,
		MaxDepth:       maxDepth,
		Direction:      c.Direction,
		Direct:         direct,
		Transitive:     transitive,
		ReachableCount: len(c.Nodes),
		UnknownRoots:   c.MissingRoots,
		Truncated:      c.Truncated,
		Diff:           req.Diff,
	}

	// 触及的核心接口（W16）：**改动起点自身 + 受影响节点** ∩ 核心接口集。
	// 起点自身要算进去——"改的正是核心接口"是最强的影响面信号，漏掉它整个判定就失真。
	// 按**名字**匹配（不要求起点在图里）：声明是名字级的，与有没有边无关。
	var coreTouched []string
	if !req.Core.Empty() {
		seen := make(map[string]struct{})
		add := func(id string) {
			if id == "" {
				return
			}
			if _, dup := seen[id]; dup {
				return
			}
			seen[id] = struct{}{}
			if req.Core.IsCore(id) {
				coreTouched = append(coreTouched, id)
			}
		}
		for _, r := range rep.Roots {
			add(r)
		}
		for _, n := range c.Nodes {
			add(n)
		}
		sort.Strings(coreTouched)
		rep.CoreTouched = coreTouched
	}

	// 判定：超过阈值才触发（W15：风险闸门，不是预算上限）。
	var reasons []string
	if rep.ReachableCount > th.MaxReachable {
		reasons = append(reasons, fmt.Sprintf("影响面可达 %d 个节点，超过阈值 %d", rep.ReachableCount, th.MaxReachable))
	}
	if len(direct) > th.MaxDirect {
		reasons = append(reasons, fmt.Sprintf("一跳下游 %d 个，超过阈值 %d", len(direct), th.MaxDirect))
	}
	if req.Diff != nil && req.Diff.Lines() > th.MaxDiffLines {
		reasons = append(reasons, fmt.Sprintf("diff %d 行（+%d/-%d，%d 个文件），超过阈值 %d",
			req.Diff.Lines(), req.Diff.Added, req.Diff.Deleted, req.Diff.Files, th.MaxDiffLines))
	}
	if len(coreTouched) > th.MaxCoreTouched {
		reasons = append(reasons, fmt.Sprintf("触及核心接口 %d 个（%s），超过阈值 %d",
			len(coreTouched), strings.Join(coreTouched, ", "), th.MaxCoreTouched))
	}
	if c.Truncated {
		// 截断 = 影响面**可能**比报告出来的大 → 对 escalate 闸门这是"偏大"的信号，
		// 必须显式列出（否则"看起来影响面不大"其实是被上限截掉了）。
		reasons = append(reasons, "遍历被结果上限截断，影响面可能不完整（调大 limit 或缩小起点集再判）")
	}
	rep.Reasons = reasons
	rep.Escalate = len(reasons) > 0
	rep.Scale = scaleOf(rep.ReachableCount, len(direct), req.Diff, th, rep.Escalate)
	return rep
}

// scaleOf 折影响面量级：触发 escalate = large；**达到**阈值一半 = medium；其余 small。
//
// 口径与注释对齐（F11）："达到一半"含等于——`reachable*2 >= MaxReachable` 即 medium。
// （escalate 判定本身是"**超过**阈值"，见上面的 reasons 拼装。）
func scaleOf(reachable, direct int, diff *DiffStat, th Thresholds, escalate bool) Scale {
	if escalate {
		return ScaleLarge
	}
	if reachable*2 >= th.MaxReachable || direct*2 >= th.MaxDirect {
		return ScaleMedium
	}
	if diff != nil && diff.Lines()*2 >= th.MaxDiffLines {
		return ScaleMedium
	}
	return ScaleSmall
}

// dedupSorted 去重 + 去空 + 排序（报告输出要确定：同一输入两次报告逐字节相同）。
func dedupSorted(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil
	}
	return out
}
