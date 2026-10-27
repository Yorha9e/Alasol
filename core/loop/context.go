package loop

import (
	"context"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// KindContextAssembled 是"本轮上下文已装配完成"的事实事件 kind（06 §4.2：
// 上下文构成清单）。每轮模型调用**前**落一条 durable 事件 → 上下文可见性的数据源
// （UI 的上下文构成面板、事后复盘"当时到底给模型看了什么"都读它）。
//
// 载荷形状 = contracts.ContextSnapshot（event.json 的 x-kind-payload 增量登记）。
const KindContextAssembled = "context.assembled"

// DefaultContextBudget 是 Config.ContextBudget 缺省值（0 = 用它）：
// 200k token 是常见现代模型的窗口量级（06 §4.2 示例同量级）。它只用于
// ContextSnapshot.tokens_budget 的**占比分母**，不参与任何装配/压缩决策
// （v0 没有压缩，见下）。
const DefaultContextBudget int64 = 200_000

// tokensPerChar 是 token 估算的分母（J21 兜底）：**4 字节 ≈ 1 token**。
//
// 为什么是估算：v0 在**调用前**落 context.assembled，此时本轮 provider usage
// 还不存在（usage 只有调用后才知道）→ 只能自算。口径刻意粗：对英文约 4 字符/token、
// 对中文约 1–2 字符/token，误差可能到 2–3 倍。它是**占比可视化**用的相对量，
// 不是计费量；契约注释（tokens_used：provider usage 优先、自算兜底）已标明这一点。
// 等有了 usage 回填（后续阶段），parts[].tokens 的绝对值精度才会提高。
const tokensPerChar = 4

// parts[].source 标识（06 §4.2 的取值口径：base / ledger / rules / tools / history / stage:N）。
//
// v0 只有三项粗粒度来源：
//   - base    → system 提示（PartKindSystem）
//   - tools   → 模型可见工具 schema（PartKindSchema；条数 = 可见工具数）
//   - history → 对话历史（PartKindHistory；条数 = 消息条数）
//
// 预留未实现（等对应机制落地再补，不提前造假数据）：
//   - ledger / rules（goal ledger 与项目规则注入）；
//   - injected（在线表 / 注入载荷——K15 在线表注入）；
//   - stage:N（分级压缩的阶段摘要，06 §4.2 的 tiered 策略）。
const (
	sourceBase    = "base"
	sourceTools   = "tools"
	sourceHistory = "history"
)

// estimateTokens 是 J21 兜底 token 估算：len(text)/4（ASCII 字符口径，见
// tokensPerChar 的说明——**近似值**，只用于构成占比，不当计费/预算判定用）。
//
// **向上取整**：一段非空文本至少记 1 token。否则"有内容却记 0"会让 UI 的占比
// 条形图把真实存在的贡献者渲染成 0 宽（看不见），也会让 tokens_used=0 被误读成
// "这一轮什么都没给模型看"。
func estimateTokens(text string) int64 {
	if text == "" {
		return 0
	}
	return int64((len(text) + tokensPerChar - 1) / tokensPerChar)
}

// estimateToolTokens 估算一个工具描述占用的 token：名字 + 说明 + 参数 schema 原文字节数
// （schema 是模型真正要吃进去的部分，通常也是工具侧最大头）。
func estimateToolTokens(t *contracts.Tool) int64 {
	if t == nil {
		return 0
	}
	n := estimateTokens(t.Name)
	if t.Description != nil {
		n += estimateTokens(*t.Description)
	}
	n += estimateTokens(string(t.Schema))
	return n
}

// estimateMessageTokens 估算一条对话消息占用的 token：正文 + 工具调用的工具名与参数
// （assistant 的 ToolCall.Args 是模型生成的 JSON，也要占窗口）。
func estimateMessageTokens(m core.Message) int64 {
	n := estimateTokens(m.Content)
	for i := range m.ToolCalls {
		n += estimateTokens(m.ToolCalls[i].Name)
		n += estimateTokens(string(m.ToolCalls[i].Args))
	}
	return n
}

// contextSnapshot 组装本轮（turn）的上下文构成清单。
//
// **粗粒度口径**（v0 有意为之）：只有 base / tools / history 三项，不试图复刻
// provider 真实的 tokenize 结果。state 一律 verbatim——v0 没有任何压缩策略
// （06 §4.2：v0/v1 只预留接口，每个阶段先做最浅层压缩；本阶段连最浅层都没实现），
// 所以 origins / target_tokens / summary_ref 全部缺省（契约里都是可选字段）。
//
// 没有条目就没有 part：System 为空 / 无可见工具 / 无消息时对应 source 根本不出现
// （"谁都没贡献"用"没有这个 source"表达，比渲染一个 0 宽分段诚实）。
func (l *Loop) contextSnapshot(turn int, req core.ModelRequest) contracts.ContextSnapshot {
	snap := contracts.ContextSnapshot{
		SessionID:    l.sessionID,
		Turn:         int64(turn),
		TokensBudget: l.contextBudget,
		Parts:        []contracts.ContextPart{},
	}
	var used int64

	// base：system 提示（每轮原样带上，Config.System）。
	if base := estimateTokens(l.system); base > 0 {
		snap.Parts = append(snap.Parts, contracts.ContextPart{
			Source:  sourceBase,
			Kind:    contracts.PartKindSystem,
			Entries: 1,
			Tokens:  base,
			State:   contracts.PartStateVerbatim,
		})
		used += base
	}

	// tools：模型可见工具 schema（条数 = 可见工具数；可见性过滤后的结果，R5/H4）。
	toolTokens := int64(0)
	for i := range req.Tools {
		toolTokens += estimateToolTokens(&req.Tools[i])
	}
	if len(req.Tools) > 0 {
		snap.Parts = append(snap.Parts, contracts.ContextPart{
			Source:  sourceTools,
			Kind:    contracts.PartKindSchema,
			Entries: int64(len(req.Tools)),
			Tokens:  toolTokens,
			State:   contracts.PartStateVerbatim,
		})
		used += toolTokens
	}

	// history：对话历史（user prompt + assistant 文本/工具调用 + tool 结果）。
	histTokens := int64(0)
	for _, m := range req.Messages {
		histTokens += estimateMessageTokens(m)
	}
	if len(req.Messages) > 0 {
		snap.Parts = append(snap.Parts, contracts.ContextPart{
			Source:  sourceHistory,
			Kind:    contracts.PartKindHistory,
			Entries: int64(len(req.Messages)),
			Tokens:  histTokens,
			State:   contracts.PartStateVerbatim,
		})
		used += histTokens
	}

	snap.TokensUsed = used
	return snap
}

// phaseContextAssembled 是 loop.error 事件里标记"context.assembled 落库失败"的阶段名
// （loopErrorPayload.Phase 是开放字符串，此处只新增一个取值）。
const phaseContextAssembled = "context.assembled"

// publishContextAssembled 在本轮模型调用**前**落一条 context.assembled durable 事件，
// 返回成功落库条数（计入 Result.Events）。
//
// 失败语义（与 X1 的三段写路径**不同**，刻意区分）：
//   - intent 落盘失败 → 拒绝执行该工具调用（没有意图记录就不许有副作用）；
//   - context.assembled 落盘失败 → **不中止 loop**，只尽力补一条 loop.error
//     （phase=context.assembled）在事件流里留痕。
//
// 理由：它是**观测性事实**（让上下文可见），不是执行前提。一个坏掉的 store 确实会让
// 所有 durable 事件都落不下去（那种情况下 loop.error 同样落不下，agent 也跑不动），
// 但把"可视化"做成 agent 的执行前提会让可用性被一个非核心特性绑架。诚实取舍：
// 落不下就在事件流里记一笔，绝不在调用前伪造一条没落库的快照。
func (l *Loop) publishContextAssembled(ctx context.Context, turn int, req core.ModelRequest) int {
	ev, err := l.newEvent(KindContextAssembled, nil, l.contextSnapshot(turn, req))
	if err != nil {
		l.publishLoopError(ctx, turn, phaseContextAssembled, err)
		return 0
	}
	n, err := l.publish(ctx, ev)
	if err != nil {
		l.publishLoopError(ctx, turn, phaseContextAssembled, err)
		return n
	}
	return n
}
