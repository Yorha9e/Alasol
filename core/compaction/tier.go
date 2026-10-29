package compaction

import (
	"strconv"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// tokensPerChar 是 token 估算的分母（J21 口径：4 字节 ≈ 1 token；与 core/loop 的
// estimateTokens 同一口径，此处复刻而非 import core/loop——依赖方向是 loop → compaction，
// 不能反向）。它是**占比/预算可视化**用的相对量，不是计费量。
const tokensPerChar = 4

// estimateTokens 估算文本 token：len/4，非空至少 1（与 loop 同口径，向上取整）。
func estimateTokens(text string) int64 {
	if text == "" {
		return 0
	}
	return int64((len(text) + tokensPerChar - 1) / tokensPerChar)
}

// tierRank 给出留存档位的序（full=0 < l1=1 < l2=2）：数字越大压得越深。
// 降级单向（full → l1 → l2，永不回升）靠比较 rank 实现。
func tierRank(t contracts.ContextTier) int {
	switch t {
	case contracts.ContextTierFull:
		return 0
	case contracts.ContextTierL1:
		return 1
	case contracts.ContextTierL2:
		return 2
	default:
		return 3 // 未知档位视为最深（fail-close：不轻信未知档）
	}
}

// canDowngrade 判定 from → to 是否是合法的**向下**降级（to 比 from 更深，或相等=幂等空操作）。
// 永不回升：to 比 from 更浅（rank 更小）→ false。
func canDowngrade(from, to contracts.ContextTier) bool {
	return tierRank(to) >= tierRank(from)
}

// nextTier 返回下一级更深的档位（full→l1→l2→l2 触底不变）。
func nextTier(t contracts.ContextTier) contracts.ContextTier {
	switch t {
	case contracts.ContextTierFull:
		return contracts.ContextTierL1
	case contracts.ContextTierL1:
		return contracts.ContextTierL2
	default:
		return contracts.ContextTierL2
	}
}

// stageRefPrefix 是 LLM 指针的前缀（13 §5：ref 用阶段号 stage:N，给 LLM）。
const stageRefPrefix = "stage:"

// StageRef 返回阶段 n 的**短指针** "stage:N"（给模型调 recall 取回原文全文）。
//
// 短指针只是**本 Run 内**的别名：引擎是 per-Run 的（compaction.AssemblerFactory），
// 每次 Run 的阶段号都从 1 重排 → 跨 Run 同一个 "stage:3" 指的是不同内容（F9）。
// 跨 Run / 跨会话要定位同一段原文，用带 seq 锚的 StageRefAnchored（机器指针口径）。
func StageRef(n int) string { return stageRefPrefix + strconv.Itoa(n) }

// StageRefAnchored 返回阶段 n 的**稳定指针** "stage:N@e:<from>..e:<to>"（F9）。
//
// 锚 = 该阶段原文的事件 seq 区间（与 context.compacted 的 seq_from/seq_to 同口径）：
// 机器指针以 seq 区间为准，阶段号只是给人看的别名。带锚的指针**跨 Run 稳定**——
// 同一个 seq 区间永远指向同一段持久化原文。
func StageRefAnchored(n int, fromSeq, toSeq int64) string {
	return StageRef(n) + "@" + seqRangeRef(fromSeq, toSeq)
}

// ParseRef 解析指针 → 阶段号 + 可选的 seq 锚。
//
//	"stage:5"            → (5, 0, 0, false, true)  —— 本 Run 别名
//	"stage:5@e:10..e:20" → (5, 10, 20, true, true) —— 稳定锚（跨 Run 可解析）
//
// 非法（非 stage: 前缀 / 阶段号非正 / 锚区间非法）→ ok=false。
//
// ref 只指向**持久化原文**（13 §5：refs 永不指摘要）；阶段号 ≤0 非法。
func ParseRef(ref string) (n int, fromSeq, toSeq int64, anchored, ok bool) {
	if !strings.HasPrefix(ref, stageRefPrefix) {
		return 0, 0, 0, false, false
	}
	body := strings.TrimPrefix(ref, stageRefPrefix)
	if at := strings.Index(body, "@"); at >= 0 {
		anchor := body[at+1:]
		body = body[:at]
		f, t, parsed := parseSeqRange(anchor)
		if !parsed {
			return 0, 0, 0, false, false
		}
		n, ok = parseStageNumber(body)
		if !ok {
			return 0, 0, 0, false, false
		}
		return n, f, t, true, true
	}
	n, ok = parseStageNumber(body)
	if !ok {
		return 0, 0, 0, false, false
	}
	return n, 0, 0, false, true
}

// ParseStageRef 解析 "stage:N"（或带锚的 "stage:N@e:..e:.."）指针 → 阶段号。
func ParseStageRef(ref string) (n int, ok bool) {
	n, _, _, _, ok = ParseRef(ref)
	return n, ok
}

// parseStageNumber 解析阶段号文本（正整数）。
func parseStageNumber(s string) (int, bool) {
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return 0, false
	}
	return v, true
}

// RenderL1Entry 渲染一个 L1（浅摘要）阶段条目：摘要**含具体工程操作** + 指针使用说明
// （13 §5：二级条目自带"调用 recall 取回全文"）。summary 由压缩模型生成（含工程操作），
// 此处只套上阶段头与 recall 指引——**ref 指向持久化原文，不是别的摘要**。
//
// recallHint=false 时**不附** recall 说明：recall 被策略 deny 时，提示一个调不到的
// 工具就是误导（F27：提示只在 recall 真的可见时才给）。
func RenderL1Entry(stage int, ref, summary string, recallHint bool) string {
	out := "【阶段 " + strconv.Itoa(stage) + " · L1 摘要】" + summary
	return out + recallSuffix(ref, recallHint)
}

// RenderL2Entry 渲染一个 L2（深压缩）阶段条目：小阶段结论 + **一根指针**（13 §5）。
func RenderL2Entry(stage int, ref, conclusion string, recallHint bool) string {
	out := "【阶段 " + strconv.Itoa(stage) + " · 结论：" + conclusion + "】"
	return out + recallSuffix(ref, recallHint)
}

// recallSuffix 渲染条目尾部的"存档 + 可回捞"说明（recall 可见时才给）。
func recallSuffix(ref string, recallHint bool) string {
	if !recallHint {
		return "现场细节已存档（原文可按指针回捞）。"
	}
	return "现场细节已存档，调用 recall(\"" + ref + "\") 取回全文"
}

// messagesText 把一段对话消息拍成纯文本（供压缩输入 = 被压区间全文，以及 token 估算）。
//
// 每条消息带 role 前缀；assistant 的工具调用与 tool 结果都纳入（它们是阶段工作的一部分）。
// 这是**被压区间的原文**，是压缩模型的输入素材（级联 C 案：压缩输入 = ledger + 此全文，
// 永不读别的阶段摘要）。
func messagesText(msgs []core.Message) string {
	var b strings.Builder
	for _, m := range msgs {
		role := m.Role
		if role == "" {
			role = "?"
		}
		b.WriteString(role)
		b.WriteString(": ")
		b.WriteString(m.Content)
		for i := range m.ToolCalls {
			tc := m.ToolCalls[i]
			b.WriteString("\n  [tool_call ")
			b.WriteString(tc.Name)
			b.WriteString(" ")
			b.WriteString(string(tc.Args))
			b.WriteString("]")
		}
		b.WriteString("\n")
	}
	return b.String()
}

// ConversationText 把一段对话消息拍成纯文本（导出给宿主的 TextSource：recall 取回的原文渲染）。
// 与 messagesText 同一口径（role 前缀 + 工具调用），只是对外暴露供宿主复用。
func ConversationText(msgs []core.Message) string { return messagesText(msgs) }

// messagesTokens 估算一段对话消息占用的 token（J21 len/4；assistant 工具调用参数也算）。
func messagesTokens(msgs []core.Message) int64 {
	var n int64
	for _, m := range msgs {
		n += estimateTokens(m.Content)
		for i := range m.ToolCalls {
			n += estimateTokens(m.ToolCalls[i].Name)
			n += estimateTokens(string(m.ToolCalls[i].Args))
		}
	}
	return n
}
