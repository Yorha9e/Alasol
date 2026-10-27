package loop

import (
	"context"
	"encoding/json"
	"fmt"

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

// publish 经 bus 发布一批 durable 事件，返回成功落库条数（用于 Result.Events 计数）。
//
// bus.Publish 对 durable 事件是"整批入库成功才通知"，失败即返回 error——调用方据此
// 判定事实是否已落地（X1：intent 没落地就不许执行）。
func (l *Loop) publish(ctx context.Context, evs ...contracts.Event) (int, error) {
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
