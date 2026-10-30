package loop

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// 本包负责把三段写路径的事实变成 contracts.Event 并经 bus 落库。
//
// 纪律：
//   - 事件 id 用 core.NewUlid()（框架 id 的唯一生成处），与 provider 的 tool_call_id
//     **永不共字段**（S5：去重只看 Event.ID，S7）；
//   - 事件内容只含审计所需的结构化事实；**env 值、密钥、原始凭据一律不进 payload**
//     （H12/H19），policy_snapshot 只记判定结论；
//   - durable=true 才落库占 seq（F8/陷阱 11）。

// loopErrorPayload 是 loop.error 的载荷（kind 自定义，契约未固化）。
//
// 只记录"这一轮没能继续"的事实：第几轮、哪个阶段、模型可读的说明。
// 原始 error 只以文案形式出现，不含 env 值/密钥。
type loopErrorPayload struct {
	Turn    int    `json:"turn"`
	Phase   string `json:"phase"` // "provider" / "publish.intent"
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
}

// newEvent 组装一条本 session/agent 归属的 durable 事件。
//
// causation 是因果指针（F3：触发本事件的那个事件 id）：
// completed/failed 指向同一次调用的 intent；intent 与 loop.error 无上游事件 → nil。
func (l *Loop) newEvent(kind string, causation *contracts.Ulid, payload any) (contracts.Event, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		// payload 序列化失败 = 这条事实无法诚实落库 → 不执行（调用方按错误中止）。
		return contracts.Event{}, fmt.Errorf("loop: 序列化 %s 事件 payload 失败: %w", kind, err)
	}
	return contracts.Event{
		V:           1,
		ID:          core.NewUlid(),
		Kind:        kind,
		TS:          nowRFC3339(),
		SessionID:   l.sessionID,
		AgentID:     l.agentID,
		CausationID: causation,
		Durable:     true,
		Payload:     raw,
	}, nil
}

// newAgentEvent 组装一条归属到**指定 agent**（而非 l.agentID）的 durable 事件（阶段 5）。
//
// 用于父 loop 代子 agent 落生命周期事件：agent.spawned / agent.failed 由父 loop 发出，但
// agent_id 必须是 child（K25：靠 agent_id 区分视角；这些事件描述的是 child 的生命周期）。
// session 仍用 l.sessionID（同 session 单流，K25）。
func (l *Loop) newAgentEvent(agentID contracts.Ulid, kind string, causation *contracts.Ulid, payload any) (contracts.Event, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return contracts.Event{}, fmt.Errorf("loop: 序列化 %s 事件 payload 失败: %w", kind, err)
	}
	return contracts.Event{
		V:           1,
		ID:          core.NewUlid(),
		Kind:        kind,
		TS:          nowRFC3339(),
		SessionID:   l.sessionID,
		AgentID:     agentID,
		CausationID: causation,
		Durable:     true,
		Payload:     raw,
	}, nil
}

// auditSettleTimeout 是审计事件 settle ctx 的**单次 publish 预算**（5s）。
//
// 口径（别读成别的）：这是**每一次 publish** 的预算，**不是**整轮收场的硬上界——
// 一轮里可能有多次 publish（intent / failed / loop.error …），各自 5s、**可以叠加**；
// 预算耗尽或库不可用时，那条审计**落不下即丢**（best-effort）：宁可丢一条，也不把这次
// 调用的收场无限期拖住（宿主库卡死时尤其重要——不能无界挂起）。
const auditSettleTimeout = 5 * time.Second

// publish 经 bus 发布一批 durable 事件，返回成功落库条数（用于 Result.Events 计数）。
//
// bus.Publish 对 durable 事件是"整批入库成功才通知"，失败即返回 error——调用方据此
// 判定事实是否已落地（X1：intent 没落地就不许执行）。
//
// # 两条纪律分开：审计必须留痕，执行必须守约
//
// Run ctx 已取消/超时（典型联调实测：policy.Decide 在 ask 上阻塞等审批，一直等到
// PromptRunTimeout 兜底才 fail-close deny 收场）时，这批 durable 事件是那次工具调用
// 仅存的审计痕迹——拿原 ctx 发布会被 store 以 "begin: context deadline exceeded"
// 拒掉，事件流从此对这次调用**零留痕**（intent/failed/loop.error 整链断裂）。
// 故此处派生短命 settle ctx（WithoutCancel + 5s）**专供事件发布**；它绝不会流进
// 工具执行 / 审批等待 / 模型调用——执行侧仍严格拿原 ctx（取消即中止、不编造结果，
// X1 语义不变）。
//
// 原 ctx 仍活时原样透传：发布仍受 Run 期限约束，既有行为零变化。
//
// settle ctx 只在**原 ctx 已死**时派生，且**只服务这一次 publish**（每次 publish 各拿一份
// 5s 预算 → 可叠加；落不下即丢，best-effort）。它绝不跨调用外传，更绝不流进执行侧。
func (l *Loop) publish(ctx context.Context, evs ...contracts.Event) (int, error) {
	if ctx != nil && ctx.Err() != nil {
		// 原 ctx 已死（取消/超时）：派生短命 settle ctx（WithoutCancel + 5s 预算）专供本次
		// 发布——审计必须留痕（见上）。**只用于发布**：调用方拿到的仍是原 ctx 的语义。
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), auditSettleTimeout)
		defer cancel()
	}
	if err := l.bus.Publish(ctx, evs...); err != nil {
		return 0, err
	}
	return len(evs), nil
}

// emitFailed 落一条 tool.call.failed（三段之③b），返回落库条数与错误。
func (l *Loop) emitFailed(
	ctx context.Context,
	causation *contracts.Ulid,
	tcID contracts.ProviderId,
	message, code string,
	details json.RawMessage,
) (int, error) {
	ev, err := l.newEvent(KindToolCallFailed, causation, contracts.ToolCallFailedPayload{
		ToolCallID: tcID, // 与 intent 同一个黑盒 id（S4）
		Error: contracts.ToolCallError{
			Message: message, // 模型可读（M5）
			Code:    strPtr(code),
			Details: details,
		},
	})
	if err != nil {
		return 0, err
	}
	return l.publish(ctx, ev)
}

// emitFailedBestEffort 是 emitFailed 的尽力而为版：用于 ctx 已取消、store 可能也不可用
// 的场景（X1：有意图就要有结果——能记就记，记不下也不掩盖取消语义）。
//
// 注意：ctx 已死不等于记不下——publish 会为已死的 ctx 派生 settle ctx（见上），
// 此处"尽力"的真正残余失败是 store 本身不可用（库已关等），此时返回 0 继续收场。
func (l *Loop) emitFailedBestEffort(
	ctx context.Context,
	causation *contracts.Ulid,
	tcID contracts.ProviderId,
	message, code string,
	details json.RawMessage,
) int {
	n, err := l.emitFailed(ctx, causation, tcID, message, code, details)
	if err != nil {
		return 0
	}
	return n
}

// publishLoopError 落一条 loop.error durable 事件（M1：失败是普通事件），返回落库条数。
//
// 尽力而为：错误通道自己失败时不能掩盖原始错误，故此处不向上传播。
func (l *Loop) publishLoopError(ctx context.Context, turn int, phase string, cause error) int {
	if cause == nil {
		return 0
	}
	ev, err := l.newEvent(KindLoopError, nil, loopErrorPayload{
		Turn:    turn,
		Phase:   phase,
		Message: cause.Error(),
		Code:    CodeLoopError,
	})
	if err != nil {
		return 0
	}
	n, err := l.publish(ctx, ev)
	if err != nil {
		return 0
	}
	return n
}

// 对话入流观测的 loop.error 阶段名（phase 是开放字符串，此处只新增两个取值）。
const (
	phaseUserPrompt = "user.prompt"
	phaseModelTurn  = "model.turn"
	// phaseContextStrategy 标记 context_strategy（v2-S1 compaction.Assembler）装配失败
	// （fail-open 到 append 后的留痕；见 requestWithStrategy）。
	phaseContextStrategy = "context.strategy"
)

// publishUserPrompt 在 Run 开始时落一条 user.prompt durable 事件（F1：对话内容进事件流）。
//
// **旁路观测**（与 X1 三段写路径刻意分离）：best-effort 落库，落不下只在事件流里补一条
// loop.error 留痕，**不中止 Run、不计入 Result.Events**——它记录"用户说了什么"这一事实，
// 不是任何执行的前提。causation=nil（对话的根，无上游事件）。
func (l *Loop) publishUserPrompt(ctx context.Context, prompt string) {
	ev, err := l.newEvent(KindUserPrompt, nil, contracts.UserPromptPayload{Text: prompt})
	if err != nil {
		l.publishLoopError(ctx, 0, phaseUserPrompt, err)
		return
	}
	if _, err := l.publish(ctx, ev); err != nil {
		l.publishLoopError(ctx, 0, phaseUserPrompt, err)
	}
}

// publishModelTurn 在一轮 provider 返回后落一条 model.turn durable 事件（F1），携带本轮文本
// 与 usage。
//
// **旁路观测**：best-effort 落库、不计入 Result.Events、不改变控制流（同 publishUserPrompt）。
// usage **只做成本记录**（J21）：绝不回填 context 快照（快照=事前估算为正式口径），更不接入
// 任何预算/门禁判断——这里只是把 provider 上报的 token 数原样搬进事件，供后续成本统计读。
// causation=nil（模型的响应是 originating 事实，无单一上游事件）。
//
// G14：reasoning（思维链整段）**provider 上报才带**——解析不到就是空串，空串不留字段
// （不拿空串占位，"缺省"才是"没有"的诚实表示）。
func (l *Loop) publishModelTurn(ctx context.Context, turn int, mr core.ModelResult) {
	payload := contracts.ModelTurnPayload{
		Text: mr.Text,
		// model 名随事件落库（成本统计按模型分组用，J21）：它**不是凭据**（模型名可安全
		// 进事件/日志），仅用于聚合口径。
		Model: strPtr(l.model),
		Usage: &contracts.ModelTurnUsage{
			InputTokens:  mr.Usage.InputTokens,
			OutputTokens: mr.Usage.OutputTokens,
		},
	}
	if mr.Reasoning != "" {
		payload.Reasoning = &mr.Reasoning
	}
	ev, err := l.newEvent(KindModelTurn, nil, payload)
	if err != nil {
		l.publishLoopError(ctx, turn, phaseModelTurn, err)
		return
	}
	if _, err := l.publish(ctx, ev); err != nil {
		l.publishLoopError(ctx, turn, phaseModelTurn, err)
	}
}

// publishModelDelta 发一条 model.delta **流事件**（G13：打字机效果的增量片段）。
//
// 与三条既有通道的关系（都是独立通道，互不占用）：
//
//	X1 三段写路径（intent/completed/failed）   —— durable 事实，执行前提
//	user.prompt / model.turn 旁路              —— durable 事实，可回放
//	model.delta（本函数）                      —— **流事件**，不可回放
//
// 流事件口径（F8/陷阱 11，逐条对应）：
//   - **不入库**：bus 对 durable=false 的批次不调用 store.Append → 不占 seq；
//   - **不进 Result.Events**：调用方不拿它的返回值计数（它也没有"落库条数"的含义）；
//   - **不阻塞 Run**：best-effort，发布失败即丢（订阅通道满时 bus 自己会计数 Lagging；
//     为一条打字机片段补 durable loop.error 是把噪声放大成事实，不做）；
//   - **不幂等**：转发性投递，订阅者可能收到重复/丢失——流事件的已知取舍（bus 包 doc）。
//
// text / reasoning 都空的片段不发（没有可显示的内容；provider 侧本就不该发，
// 这里是纵深防御）。
//
// **归组口径（已知边界，别当精确口径用）**：payload 的 turn 是**每次 Run 内**的轮次号，
// 而 model.delta **没有 run 判别位**（run_id / started_at 都没加——加它要走全链：
// schema + 生成物 + 样本 + 消费端）→ **同 session 并发跑两轮时，两轮的 turn 会撞号，
// 订阅端无法可靠归组**。归组只对"单 session 串行跑"成立（宿主默认口径：一个 session
// 同时只跑一轮）。契约侧的同一说明见 contracts/schema/event.json 的 ModelDeltaPayload。
func (l *Loop) publishModelDelta(ctx context.Context, turn int, ev core.ProviderEvent) {
	payload := contracts.ModelDeltaPayload{Turn: int64(turn)}
	if ev.Text != "" {
		text := ev.Text
		payload.Text = &text
	}
	if ev.Reasoning != "" {
		reasoning := ev.Reasoning
		payload.Reasoning = &reasoning
	}
	if payload.Text == nil && payload.Reasoning == nil {
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return // 不可达（纯字符串结构体）；真失败就当这条片段丢了
	}
	// 手工装配而不是走 newEvent：那个恒设 Durable=true（三段写路径/旁路观测都是事实事件），
	// 流事件必须 Durable=false 且**无 seq**（bus 会拒：durable=false 携带 seq = 违约）。
	stream := contracts.Event{
		V:         1,
		ID:        core.NewUlid(), // 框架 id（订阅端可据此去重呈现）
		Kind:      KindModelDelta,
		TS:        nowRFC3339(),
		SessionID: l.sessionID,
		AgentID:   l.agentID,
		Durable:   false,
		Payload:   raw,
	}
	// 经同一个 publish：ctx 已死时它派生 settle ctx（流事件不落库，通知立刻返回，
	// 不会真的等满 5s）；返回值忽略（流事件没有落库计数含义）。
	_, _ = l.publish(ctx, stream)
}

// policySnapshot 把判定时刻的策略结论序列化成 intent 的 policy_snapshot
// （M9/陷阱 15：策略在 T0 判定、副作用持续到 Tn，「判定只约束发起时刻」是已知边界，
// 快照供事后审计）。
//
// 只记结论（verdict / rule_id / reason），不记参数、不记 env（H19）。
func policySnapshot(dec contracts.Decision) json.RawMessage {
	type snapshot struct {
		Verdict string `json:"verdict"`
		RuleID  string `json:"rule_id,omitempty"`
		Reason  string `json:"reason,omitempty"`
	}
	raw, err := json.Marshal(snapshot{
		Verdict: string(dec.Verdict),
		RuleID:  deref(dec.RuleID),
		Reason:  deref(dec.Reason),
	})
	if err != nil {
		// 不可达（纯字符串结构体）；真失败时按最严结论记账，绝不写成 allow。
		return json.RawMessage(`{"verdict":"deny","reason":"policy snapshot marshal failed"}`)
	}
	return raw
}

// deref 取可选字符串的值（nil → 空串）。
func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// strPtr 取字符串地址。
func strPtr(s string) *string { return &s }
