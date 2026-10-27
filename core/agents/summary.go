package agents

import (
	"context"

	"github.com/Yorha9e/alasol/contracts"
)

// Summary 是子 agent 的结构化完成汇报（K24）。直接复用契约类型 contracts.AgentSummary
// （text / refs / taint / abnormal），loop 据此构造 agent.completed 事件，无转换边界。
//
// summary 是 subagent 回到主 agent 上下文的**唯一通道**（K24：上下文隔离器的关键机制）。
type Summary = contracts.AgentSummary

// 污点级（Q7/H13）：core 定义的级别集。v0 固化 clean / untrusted 两级；索引即严重度
// （clean 最可信）。taint 与 contracts.ToolResult.Taint 同源（继承来源最高污点级）。
const (
	TaintClean     = "clean"
	TaintUntrusted = "untrusted"
)

// taintOrder 是污点级总序（索引 = 严重度）。
var taintOrder = []string{TaintClean, TaintUntrusted}

// TaintRank 返回污点级的严重度序号；未知级按最严（len(taintOrder)）处理（fail-close：
// 读不出的污点级不当成 clean）。
func TaintRank(level string) int {
	for i, l := range taintOrder {
		if l == level {
			return i
		}
	}
	return len(taintOrder)
}

// NormalizeTaint 归一化污点级：空 → clean；其余原样返回（保留未知级，交由 IsTainted 判定）。
func NormalizeTaint(level string) string {
	if level == "" {
		return TaintClean
	}
	return level
}

// MaxTaint 取更不可信的一级（Q7 保守规则：untrusted 只能升不能降）。
func MaxTaint(a, b string) string {
	na, nb := NormalizeTaint(a), NormalizeTaint(b)
	if TaintRank(na) >= TaintRank(nb) {
		return na
	}
	return nb
}

// IsTainted 判定污点级是否非 clean（tainted summary 按不可信数据处理，不当指令执行）。
func IsTainted(level string) bool {
	return NormalizeTaint(level) != TaintClean
}

// TaintSummary 把 inherited（来源事件最高污点级）合并进 s.Taint（Q7：summary 强制带 taint，
// 继承来源最高级；只能升不能降），返回归一化后的 summary（Taint 至少为 clean）。
//
// 典型用法：子 agent 全程 clean → "clean"；子 agent 读过 untrusted 内容（其 loop 累积的
// 最高污点级）→ 即使模型自称 clean，也升到 inherited 的更高级。
func TaintSummary(s Summary, inherited string) Summary {
	s.Taint = NormalizeTaint(MaxTaint(s.Taint, inherited))
	return s
}

// RefVerifier 核验单条 summary 引用是否可验证（K24/Q7 钩子）。loop 传入实现；
// 典型实现按 ref 形态分派：e:<seq> 查事件流、文件路径查文件系统存在性。
//
// 返回 false = 该 ref 当前不可核验（不是"引用为假"，只是"此刻核验不了"）。
type RefVerifier interface {
	Verify(ctx context.Context, ref string) bool
}

// VerifyRefs 用 v 逐条核验 refs，返回与 refs 等长的结果切片与"是否全部通过"。
//
// v 为 nil 时视为全部不可核验（fail-close：没有核验器就不能声称引用可核验）——但空 refs
// 时 vacuously 全部通过（results 为空、allOK=true）。
func VerifyRefs(ctx context.Context, v RefVerifier, refs []string) ([]bool, bool) {
	results := make([]bool, len(refs))
	allOK := true
	for i, ref := range refs {
		ok := v != nil && v.Verify(ctx, ref)
		results[i] = ok
		if !ok {
			allOK = false
		}
	}
	return results, allOK
}
