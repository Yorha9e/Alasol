package compaction

import (
	"strconv"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
)

// context.assembled 的 parts[].source 取值（06 §4.2 口径：base / ledger / rules / tools /
// history / stage:N）。base / tools / online_table 由 core/loop 自己产；本包只产下面几个
// （stage:N 是动态的，用 StageRef(n)）。点亮这些预留来源是 v2-S1 的一件事（见 core/loop/context.go）。
const (
	// sourceLedger goal ledger 稳定半注入（J19/X6）。
	sourceLedger = "ledger"
	// sourceRules 项目规则注入（AGENTS.md 等；稳定前缀的一部分，Q2/Q3）。
	sourceRules = "rules"
	// sourceCommonSense 常识区注入（13 §6：重复阅读的公共内容，偶变）。
	sourceCommonSense = "common_sense"
	// sourceHistory 当前工作区（verbatim：full 的 N-1 + 当前 N）。与 loop 内建 history 同名，
	// 口径一致（kind=history / state=verbatim）。
	sourceHistory = "history"
)

// part 是 contracts.ContextPart 的构造助手（位置参数多，用名字清晰的构造函数避免错位）。
type partSpec struct {
	Source       string
	Kind         contracts.PartKind
	Entries      int64
	Tokens       int64
	State        contracts.PartState
	Origins      []string
	TargetTokens *int64
	SummaryRef   *string
}

// newPart 由一个 spec 造 ContextPart。
func newPart(s partSpec) contracts.ContextPart {
	return contracts.ContextPart{
		Source:       s.Source,
		Kind:         s.Kind,
		Entries:      s.Entries,
		Tokens:       s.Tokens,
		State:        s.State,
		Origins:      s.Origins,
		TargetTokens: s.TargetTokens,
		SummaryRef:   s.SummaryRef,
	}
}

// tierInstruction 返回某留存档位的压缩指令（给压缩模型；含 J9 目标 token 预算）。
// 指令强调"只基于给定内容、不要编造、保留/提炼关键信息"，并**不要求读别的阶段**——
// 输入已由 engine 限定为 ledger + 本阶段全文（级联 C 案）。
func tierInstruction(tier contracts.ContextTier, p Params) string {
	switch tier {
	case contracts.ContextTierL1:
		return "将以下阶段工作压缩为浅层摘要：保留具体工程操作（改了哪些文件、跑了哪些命令、关键中间结论），" +
			"目标 ≤" + strconv.FormatInt(p.L1TargetTokens(), 10) + " token。只基于给定内容，不要编造。"
	case contracts.ContextTierL2:
		return "将以下阶段工作提炼为一句话阶段结论（做了什么 / 结论 / 下一步），" +
			"目标 ≤" + strconv.FormatInt(p.L2TargetTokens(), 10) + " token。只基于给定内容，不要编造。"
	default:
		return "简要概括以下阶段工作。只基于给定内容，不要编造。"
	}
}

// renderTierEntry 渲染一个已压缩阶段条目（13 §5：二级条目自带 recall 使用说明）。
// full 档不该走到这（它 verbatim）；此处对 full 退化成原文（防御）。
//
// ref 是给模型的稳定指针（带 seq 锚，F9）；recallHint=false 时不附 recall 说明
// （recall 被策略 deny → 不提示一个调不到的工具，F27）。
func renderTierEntry(tier contracts.ContextTier, number int, ref, summary string, recallHint bool) string {
	switch tier {
	case contracts.ContextTierL1:
		return RenderL1Entry(number, ref, summary, recallHint)
	case contracts.ContextTierL2:
		return RenderL2Entry(number, ref, summary, recallHint)
	default:
		return "【阶段 " + strconv.Itoa(number) + " · 全文】" + summary
	}
}

// tierTarget 返回某档位的目标残留 token（J9：L1≤10% / L2≤1% 窗口；full 无目标）。
func tierTarget(p Params, tier contracts.ContextTier) int64 {
	switch tier {
	case contracts.ContextTierL1:
		return p.L1TargetTokens()
	case contracts.ContextTierL2:
		return p.L2TargetTokens()
	default:
		return 0
	}
}

// renderLedger 把 goal ledger 全量快照渲染成注入文本（级联 C 案的"总体局势"输入用）。
// 四张清单（goals/constraints/completed/pending）都带上——目标偏移的根治靠它常驻上下文
// （J19）。
//
// 注意：**注入上下文时不用这个全量版本**，而是拆成稳定半 / 易变半两段分别注入
// （renderLedgerStable / renderLedgerVolatile，见 F5 与 13 §6 缓存定律）。
func renderLedger(lp contracts.LedgerUpdatedPayload) string {
	var b strings.Builder
	b.WriteString("[goal ledger / 目标账本]\n")
	writeList(&b, "目标", lp.Goals)
	writeList(&b, "约束", lp.Constraints)
	writeList(&b, "已完成", lp.Completed)
	writeList(&b, "待解", lp.Pending)
	return b.String()
}

// renderLedgerStable 渲染 ledger 的**稳定半**（目标 / 约束）——罕变，进稳定前缀
// （13 §6：稳定内容永远在最前；F5）。
func renderLedgerStable(lp contracts.LedgerUpdatedPayload) string {
	var b strings.Builder
	b.WriteString("[goal ledger / 目标账本 · 稳定半]\n")
	writeList(&b, "目标", lp.Goals)
	writeList(&b, "约束", lp.Constraints)
	return b.String()
}

// renderLedgerVolatile 渲染 ledger 的**易变半**（已完成 / 待解）——每轮都可能变，
// 贴生长前沿（当前工作区之前）注入，绝不混进稳定前缀（F5：混进去 = 前缀缓存全废）。
func renderLedgerVolatile(lp contracts.LedgerUpdatedPayload) string {
	var b strings.Builder
	b.WriteString("[goal ledger / 目标账本 · 易变半]\n")
	writeList(&b, "已完成", lp.Completed)
	writeList(&b, "待解", lp.Pending)
	return b.String()
}

func writeList(b *strings.Builder, label string, items []string) {
	b.WriteString(label)
	b.WriteString(":")
	if len(items) == 0 {
		b.WriteString(" （无）")
	}
	for _, it := range items {
		b.WriteString("\n  - ")
		b.WriteString(it)
	}
	b.WriteString("\n")
}
