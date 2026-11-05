package loop

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/durable"
	"github.com/Yorha9e/alasol/core/registry"
)

// 编译期证明：*Loop 实现 durable.Replayer（X2 第一类 safe / verify 不通过 的重放执行侧）。
var _ durable.Replayer = (*Loop)(nil)

// Replay 重放一个 in-flight 操作（durable.Replayer，X2）：再次执行工具调用并落一条
// completed / failed 结果事件（causation 指向原 intent，F3）。
//
// durable.Resume 在处置 replay=safe（或 verify 核验不通过）的操作时调用本方法；本方法用
// loop 已装配的 registry + bus 完成「真正再次执行 + 落结果」，使该 op 不再是 in-flight。
//
// 与 runToolCall 的区别（刻意）：
//   - **不重跑策略链**：该操作在 T0 已判定放行（policy_snapshot 记在 intent 里，M9「判定只约束
//     发起时刻」）——resume 重放的是**同一个已授权操作**的未知结果，不是发起新调用；
//   - **但查注册表 State**：State 是**装配事实**不是策略判定——工具在两次进程之间被停用
//     了就不该再执行（缺口 3 p3-6；否则"停用"在崩溃续跑路径上失效）；
//   - **不走模型**：直接按 op.Intent 执行工具，不发起 provider.Run；
//   - 归属用 op.SessionID / op.AgentID（可能是子 agent 的调用，未必是 l.agentID）。
//
// 结果语义（区分成败，P2）：
//   - (ReplayResult{OK: true}, nil)：重放**成功**（结果事件已落 completed）；
//   - (ReplayResult{OK: false}, nil)：重放执行了但**失败**（结果事件已落 failed，失败码带在
//     结果里）→ durable 记 replayed_failed（需人工重做）；
//   - (_, err != nil)：**写路径断裂**（ctx 取消、结果事件落盘失败）→ durable.Resume
//     据此中止并可重试。
func (l *Loop) Replay(ctx context.Context, op durable.Op) (durable.ReplayResult, error) {
	if l == nil {
		return durable.ReplayResult{}, fmt.Errorf("loop: Loop 为 nil")
	}
	if err := ctx.Err(); err != nil {
		return durable.ReplayResult{}, err
	}
	intent := op.Intent
	causation := &op.IntentEventID

	entry, known := l.registry.Get(intent.ToolName)
	desc := entry.Tool

	// 装配检查（fail-close：工具没了/已停用/实现不是执行器 → 落 failed，不进执行）。
	if !known || desc == nil {
		return l.replayFailed(ctx, op, causation,
			fmt.Sprintf("fail-close: 工具 %q 未在注册表注册或无工具描述 → 重放时无法执行", intent.ToolName),
			CodeToolNotFound)
	}
	// State 复查（缺口 3 p3-6）：重放路径**也**要查注册表 State。
	//
	// 为什么：重放**不重跑策略链**（该操作在 T0 已判定放行，见本函数头注释），但
	// "注册表 State=disabled"是**装配事实**、不是策略判定——工具在两次进程之间被运维停用
	// 了，重放就**不该**再执行它（否则"停用"在崩溃续跑路径上失效 = 又一个假旋钮）。
	//
	// 处置与 runToolCall 同款：走既有失败形态（replayFailed：落一条 tool.call.failed、
	// 返回 OK=false + 失败码），失败码复用 tool_disabled（与执行路径同一码）。
	if entry.State == registry.StateDisabled {
		return l.replayFailed(ctx, op, causation,
			fmt.Sprintf("fail-close: 工具 %q 已停用（registry State=disabled）→ 重放时不执行", intent.ToolName),
			CodeToolDisabled)
	}
	exec, ok := entry.Impl.(core.ToolExecutor)
	if !ok {
		return l.replayFailed(ctx, op, causation,
			fmt.Sprintf("fail-close: 工具 %q 的注册实现不是 core.ToolExecutor（实际 %T）→ 重放时无法执行", intent.ToolName, entry.Impl),
			CodeExecutorMissing)
	}

	// v4-4-B：blob.plan——外置**计划**。重放路径的 intent 是**历史事件**（上一进程落的，
	// 没法"排在它前面"），故计划写在**重放动手前**（本路径最早的写点）：崩溃续跑据此
	// 对账"这个 tool_call 的结果没落"。turn=0：重放没有"第几轮"的概念，如实记 0。
	l.planExternalize(0, intent.ToolCallID)

	// 执行 env：重放不注入新凭据（H21：session/单次注入是发起时刻的事）；用进程 env 经 ctx。
	execCtx := core.WithEnv(ctx, l.sessionEnv)
	// 执行作用域（L2 per-session 执行上下文 · 件 1，**执行点 B**）：重放同样要把"本次在
	// 哪个工作区跑"带给执行器。
	//
	// 为什么这里**必须**注入（漏了 = 最隐蔽的一种错）：重放的是上一次进程发起的同一个
	// 已授权操作，它要落在**同一个工作区**——漏注入时工具按进程 cwd 解析相对路径，
	// 于是重放把文件写到了别处：结果**看起来成功**（有 completed 事件），但内容错位，
	// 而且没有任何报错能提示（"写成功了"）。比"重放失败"更糟——失败至少是显性的。
	//
	// 与执行点 A（toolcall.go）同款：只加 ctx 值，X1 三段形状与落盘序一字不动。
	execCtx = l.withExecScope(execCtx)
	res, execErr := executeSafely(execCtx, exec, intent)
	if execErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			_, _ = l.replayFailed(ctx, op, causation, "重放被取消: "+execErr.Error(), CodeToolError)
			return durable.ReplayResult{}, ctxErr
		}
		return l.replayFailed(ctx, op, causation,
			fmt.Sprintf("工具 %s 重放执行失败: %v", intent.ToolName, execErr), CodeToolError)
	}

	// 成功 → 落 completed（result  durable 落盘，X1 收口）。
	//
	// D4（v4-3-A，G2e）：重放产出的 completed **同样走外置**（与原执行同口径，见
	// toolcall.go 的 runToolCall）——重放落的是同一形状的事件（intent → completed），
	// 不外置就会让"大 payload 不进事件流"这条规则在 **resume 重放路径**上失效：
	// 一次重放就能把几十 MB 的 result 塞回事件流（事件流要被回放/转发/索引，F12）。
	//
	// 口径与原执行**逐字一致**：只改 completed 的 result 载荷内容 + envelope.BinaryRef；
	// 三段结构与落盘序一字不动；外置失败 fail-open（原样内联，completed 仍落）。
	payload := contracts.ToolCallCompletedPayload{
		ToolCallID: intent.ToolCallID,
		Result:     res,
	}
	// turn=0：重放没有“第几轮”的概念（Replay 不在 Run 的轮次循环里），如实记 0。
	binaryRef := l.externalizeResult(ctx, &payload)
	doneEv, err := l.newReplayEvent(op, KindToolCallCompleted, causation, payload)
	if err != nil {
		return durable.ReplayResult{}, err
	}
	doneEv.BinaryRef = binaryRef // nil = 不外置（默认/酸测试路径）
	if _, err := l.publish(ctx, doneEv); err != nil {
		return durable.ReplayResult{}, fmt.Errorf("loop: 重放 %q 的 completed 事件落盘失败: %w", intent.ToolName, err)
	}
	return durable.ReplayResult{OK: true}, nil
}

// newReplayEvent 组装一条归属到 op（session/agent）的 durable 事件（causation 指向原 intent）。
func (l *Loop) newReplayEvent(op durable.Op, kind string, causation *contracts.Ulid, payload any) (contracts.Event, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return contracts.Event{}, fmt.Errorf("loop: 序列化 %s 事件 payload 失败: %w", kind, err)
	}
	return contracts.Event{
		V:           1,
		ID:          core.NewUlid(),
		Kind:        kind,
		TS:          nowRFC3339(),
		SessionID:   op.SessionID,
		AgentID:     op.AgentID,
		CausationID: causation,
		Durable:     true,
		Payload:     raw,
	}, nil
}

// replayFailed 落一条 tool.call.failed（重放失败是**已解析的结果**：op 已收口，
// 返回 OK=false + 失败码/说明；只有落盘断裂才返回 error）。
func (l *Loop) replayFailed(ctx context.Context, op durable.Op, causation *contracts.Ulid, message, code string) (durable.ReplayResult, error) {
	ev, err := l.newReplayEvent(op, KindToolCallFailed, causation, contracts.ToolCallFailedPayload{
		ToolCallID: op.Intent.ToolCallID,
		Error:      contracts.ToolCallError{Message: message, Code: strPtr(code)},
	})
	if err != nil {
		return durable.ReplayResult{}, err
	}
	if _, err := l.publish(ctx, ev); err != nil {
		return durable.ReplayResult{}, fmt.Errorf("loop: 重放 %q 的 failed 事件落盘失败: %w", op.Intent.ToolName, err)
	}
	return durable.ReplayResult{OK: false, FailureCode: code, FailureMessage: message}, nil
}
