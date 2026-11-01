package loop

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/compaction"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
)

// runToolCall 执行一次工具调用的三段写路径（X1）。
//
// 返回：
//   - text：回填给模型的文本（成功 = 工具输出；deny/失败 = 模型可读的错误说明，M5）；
//   - events：本调用成功落库的 durable 事件数（恰好 0 或 2：intent + (completed|failed)）；
//   - err：**仅**表示写路径断裂（intent / 结果事件落盘失败）或 ctx 取消——此时调用方
//     必须中止 loop，因为"有意图无结果"或"连意图都没记下"都不允许继续编造。
//
// 事件序（每个调用恰好一对，store 里必然是这个顺序）：
//
//	tool.call.intent → （不执行）tool.call.failed      ← deny / 工具缺失 / 执行失败
//	tool.call.intent → 执行 → tool.call.completed      ← 执行成功
func (l *Loop) runToolCall(ctx context.Context, turn int, tc core.ToolCallReq, st *runState) (string, int, error) {
	// 工具解析（v2-S1）：策略框架工具（recall / stage）优先，其次注册表；二者都走同一 X1 路径。
	//
	// 解析**前置**到策略判定之前（缺口 3 p3-1）：State 门控要在策略链**之前**短路。
	rt := l.resolveTool(tc.Name, st.strategyTools)
	desc := rt.desc
	known := rt.known

	// State 门控（第二层，缺口 3）：注册表条目 State=disabled → 本次调用直接走 disabled
	// 失败路径，**不调 policy.Decide**。
	//
	// 为什么排在策略链**之前**（p3-1）：
	//   - **不产生 ask 人审**：停用的工具压根不可执行——让人审"要不要执行"是白问
	//     （人审台前不该出现不可用的东西）；
	//   - **不误归因 policy_deny**：停用是**注册表事实**，不是策略判定结果——归到
	//     policy_deny 会让审计把"运维停用"误读成"策略拒绝"。
	//
	// 框架工具（strategyTools 命中）不经注册表、没有 State → 不走此判定（维持现状）。
	disabled := rt.fromRegistry && rt.state == registry.StateDisabled

	// a. policy 判定（fail-close）：判定只约束发起时刻（M9/陷阱 15），快照进 intent。
	//    停用工具**短路策略链**（不问人、不误归因）→ dec 保持零值（policy 未判定）。
	var dec contracts.Decision
	if !disabled {
		dec = policy.Decide(ctx, l.policy, l.approver, policy.Query{
			Tool:      tc.Name,
			Args:      tc.Args,
			SessionID: &l.sessionID,
			AgentID:   &l.agentID,
		})
	}

	// 参数形状校验（契约边界）：arguments 必须是合法 JSON **对象**。
	// 不满足时 arguments 置 {}（intent 仍要落盘，但不能落一个违反契约的载荷），
	// 走普通失败路径（failed/code=bad_arguments），loop 继续不中止。
	args, argsProblem := validateArgs(tc.Args)

	// b. ① intent durable 落盘（先于任何执行；落盘失败就不执行，X1）。
	intent := contracts.ToolCallIntentPayload{
		ToolCallID: tc.ID, // provider 黑盒 id 原样（S4/S5）
		ToolName:   tc.Name,
		Arguments:  args,
		Idempotent: desc != nil && desc.Idempotent, // 缺省 false（M7：不声明不自动重试）
		Replay:     replayOf(desc),                 // 缺省 unsafe（X2：未知结果不盲目重放）
		// M9/陷阱 15：判定时刻的策略快照。
		// 停用工具**短路策略链**（不问人、不误归因）→ policy 未判定 → dec 是零值 →
		// 快照按 policySnapshot(零值) 的既有输出填（verdict 空串；**不伪造** allow/deny）。
		PolicySnapshot: policySnapshot(dec),
	}
	intentEv, err := l.newEvent(KindToolCallIntent, nil, intent)
	if err != nil {
		return "", 0, err // 组装不出合法 intent = 不许执行
	}
	// v4-4-B：blob.plan——外置**计划**写在 intent **发布之前**（写点全在三段窗口之外，
	// critic F1）。此刻结果还不存在 → 体量未知（0）；计划只用于崩溃续跑时对账"这个
	// tool_call 的结果没落"。
	l.planExternalize(turn, tc.ID)
	n, err := l.publish(ctx, intentEv)
	if err != nil {
		// 意图没落盘 → 绝不执行（X1：没有意图记录的副作用不可审计、不可恢复）。
		n += l.publishLoopError(ctx, turn, "publish.intent", err)
		return "", n, fmt.Errorf("loop: 工具 %q 的 intent 事件落盘失败 → 拒绝执行（X1）: %w", tc.Name, err)
	}
	events := n
	intentID := intentEv.ID

	// c'. 停用工具短路失败路径（缺口 3 第二层，p3-1）：State=disabled → 直接落 failed，
	// code=tool_disabled（**不是** policy_deny、也不是 tool_not_found）。
	//
	// X1 三段形状一字不动：intent 已落盘 → 补一条 failed → completed 不落 → 执行器零调用。
	//
	// 排在参数校验 / 权限 / 策略判定**之前**：停用的工具压根不可用——先告诉模型"它被停用了"
	//（改参数也救不回来），别让它去修参数；更不让人审（ask）与误归因（policy_deny）。
	if disabled {
		msg := fmt.Sprintf("fail-close: 工具 %q 已停用（registry State=disabled）→ 不执行；请改用可见工具列表里的工具，或等待重新启用后再调用", tc.Name)
		return l.failToolCall(ctx, &intentID, tc.ID, msg, CodeToolDisabled, nil, events)
	}

	// c''. 参数不合契约 → 普通失败（M1/M8：失败是普通事件，不是特殊通道）：
	// intent 已落盘（arguments={}），补一条 failed，模型可读地说明怎么改，loop 继续。
	if argsProblem != "" {
		msg := fmt.Sprintf("参数不是合法 JSON 对象：%s；请重新给出 JSON 对象（形如 {\"key\":\"value\"}）", argsProblem)
		return l.failToolCall(ctx, &intentID, tc.ID, msg, CodeBadArguments, badArgsDetails(tc.Args), events)
	}

	// c'''. 生效权限收窄（H11/H22：实际 = 请求 ∩ 策略上限 ∩ 父级授予）：
	// 声明的权限是**上限**，超出即 deny，**不可被全局 allow 规则放宽**。
	//
	// side 从**解析结果**的描述取（框架工具的副作用档位也算数——p3 修：此前只看注册表，
	// 框架工具（ledger 是写账本）一律按 SideEffectNone 处理 = 绕过 perm 校验）。
	side := contracts.SideEffectNone
	if desc != nil {
		side = desc.SideEffect
	}
	if denied, reason := l.permDenial(tc.Name, side); denied {
		return l.failToolCall(ctx, &intentID, tc.ID, reason, CodePermDenied, nil, events)
	}

	// c. deny（含 ask 被 DenyAll 拒、无命中 fail-close）→ ③ failed，**不执行**。
	if dec.Verdict != contracts.EffectAllow {
		// 工具压根没注册时，主因是"工具不存在"（策略无命中只是连带现象）——
		// 失败码取更能让模型自我修正的那个。
		code := CodePolicyDeny
		if !known {
			code = CodeToolNotFound
		}
		// 语义诚实（审计要能分辨两种事实）：**人审明确拒绝**（人点了 deny）与
		// **没人来得及答**（审批失败 / 无人应答 / ctx 取消或超时）是两种事实，文案必须
		// 可区分（审计靠它分辨"被拒"与"没人答"）。
		//
		// 判据取**判定里记下的成因**（优先 contracts.Decision.Code 结构化成因码，
		// code 缺省才退回 Reason 文案匹配），**不取事后读到的 ctx.Err() 快照**——
		// ctx 快照是**另一个瞬间**的事实：人明确拒绝后 ctx 可能恰好到期（判据竞态），
		// 按快照加"超时"前缀会把"被拒绝"误记成"没人来得及答"，审计归因就错了。
		// 注意：此处无论哪种收场都**不执行**（deny 路径本来就不走执行）。
		msg := denyMessage(tc.Name, dec, known)
		if isNoAnswerDeny(dec) {
			msg = denyTimeoutPrefix + msg
		}
		return l.failToolCall(ctx, &intentID, tc.ID, msg, code, policySnapshot(dec), events)
	}

	// allow 之后的装配检查：未知工具 / 无工具描述 / 执行器不是 ToolExecutor
	// → 落 failed，不进执行（fail-close：宁可拒绝也不猜）。
	//
	// 注：State=disabled 的检查**不在这里**——它排在策略链之前（见上面的 c'.），
	// 停用工具压根走不到这里（不进策略链、不产生 ask、不误归因 policy_deny）。
	if !known {
		msg := fmt.Sprintf("fail-close: 工具 %q 未在注册表注册（模型给出的工具名查无此扩展）→ 不执行；请只使用可见工具列表里的工具", tc.Name)
		return l.failToolCall(ctx, &intentID, tc.ID, msg, CodeToolNotFound, nil, events)
	}
	if desc == nil {
		// 注册过但 kind 不是 tool（只有 RegisterTool 会带上 Tool 描述）。
		msg := fmt.Sprintf("fail-close: 工具 %q 已注册（kind=%q）但没有工具描述（Entry.Tool 为 nil）→ 不执行", tc.Name, rt.metaKind)
		return l.failToolCall(ctx, &intentID, tc.ID, msg, CodeToolNotFound, nil, events)
	}
	if rt.exec == nil {
		// 装配错误：注册进来的实现不是执行器（RegisterTool 会拦住，这里是纵深防御）。
		msg := fmt.Sprintf("fail-close: 工具 %q 的注册实现不是 core.ToolExecutor（实际 %T）→ 不执行", tc.Name, rt.impl)
		return l.failToolCall(ctx, &intentID, tc.ID, msg, CodeExecutorMissing, nil, events)
	}
	exec := rt.exec

	// ②″ 执行前**再查一次** State（第二层门控的真正时点，与 runtime/mcpserver 的
	// checkUsable 对齐——"list/判定之后、执行之前"被停用的窗口）：
	//
	// 为什么必须**再读一次**（而不是沿用 resolveTool 时读到的快照）：那份快照是**判定
	// 时刻**的事实，而"ask 规则 → 人审等待"这段窗口可能有几十秒——期间运维完全可能
	// 停用该工具。快照停在判定时刻 → 审批放行后工具照常执行 → **停用不生效**
	// （探针：ask 规则 + 审批期间 Disable → 执行器被调 1 次）。
	//
	// 处置：读到 disabled → 走既有失败路径（CodeToolDisabled；intent 已落、failed 落、
	// completed 不落——X1 三段形状一字不动），文案点名"执行前复查发现已停用"。
	//
	// 框架工具（strategyTools 命中）不经注册表 → 不查（维持现状；也不能拿同名注册表
	// 条目的状态去卡框架工具）。
	//
	// 注：这里**不**再查"条目是否还在"（未注册的处置口径本刀不扩——见报告）。
	if rt.fromRegistry {
		if cur, ok := l.registry.Get(tc.Name); ok && cur.State == registry.StateDisabled {
			msg := fmt.Sprintf("fail-close: 工具 %q 执行前复查发现已停用（registry State=disabled）→ 不执行；该工具在本次调用期间被停用，请改用可见工具列表里的工具，或等待重新启用后再调用", tc.Name)
			return l.failToolCall(ctx, &intentID, tc.ID, msg, CodeToolDisabled, nil, events)
		}
	}

	// ②′ 执行前护栏（fail-close）：Run ctx 已取消/超时 → **绝不执行**。
	//
	// 为什么必须**显式**查（不能靠"intent 落盘失败"顺带挡住）：审计落盘现在走 settle ctx
	// （WithoutCancel + 5s/次，见 events.go publish）——ctx 死了**照样落得下** intent。于是
	// "ctx 已死 ⇒ intent 落盘失败 ⇒ 绝不执行"这条**隐式**护栏就没了（护栏回退 = 取消/
	// 超时之后仍有副作用）。护栏不能靠副作用兜底，只能自己查：ctx 已死 = 这一轮已被
	// 取消/超时，**不许再有副作用**（执行侧严格尊重原 ctx：取消即中止、不编造结果）。
	//
	// 失败事件如实写"因 Run 上下文取消/超时未执行"——审计要能分辨"没执行"与"执行失败"。
	if ctxErr := ctx.Err(); ctxErr != nil {
		msg := fmt.Sprintf("因 Run 上下文取消/超时未执行：工具 %s 未被执行（%v）——Run 上下文已结束（取消/超时），fail-close：ctx 已死不再发起任何副作用",
			tc.Name, ctxErr)
		return l.failToolCall(ctx, &intentID, tc.ID, msg, CodeToolError, nil, events)
	}

	// ② 执行：执行 env 只经 ctx 传递（H12/H19：值永不进事件/日志）。
	execScope, secrets := l.buildExecEnv(ctx, desc)
	execCtx := core.WithEnv(ctx, execScope)
	res, execErr := executeSafely(execCtx, exec, intent)
	// 秘密收口（P1-1）：工具输出（命令回显）进事件、进下一轮 prompt 之前，
	// 把**框架注入的值**逐个精确替换成 "***"。命令自己的输出是数据，原样保留。
	res = redactResult(res, secrets)
	// Q7：累积本次 Run 的最高污点级（工具输出可能带 taint；供 complete 时继承）。
	st.acc.observe(res.Taint)
	if execErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// ctx 取消：尽力补一条 failed（X1：有意图就要有结果），再返回取消语义。
			events += l.emitFailedBestEffort(ctx, &intentID, tc.ID,
				redactSecrets(fmt.Sprintf("工具 %s 执行被取消: %v", tc.Name, execErr), secrets), CodeToolError, nil)
			return "", events, ctxErr
		}
		// 执行失败是普通失败（M1/M8）：落 failed，把模型可读的说明回填，loop 继续。
		msg := redactSecrets(fmt.Sprintf("工具 %s 执行失败: %v", tc.Name, execErr), secrets)
		return l.failToolCall(ctx, &intentID, tc.ID, msg, CodeToolError, nil, events)
	}

	// ③a completed：结果 durable 落盘（X1）。
	//
	// D4（v4-3-A，G2e）：result 超阈值时整体外置 blob——payload 换摘要形态、envelope.BinaryRef
	// 指过去。**只改 completed 的 result 载荷内容 + BinaryRef**，intent→completed 三段结构与
	// 落盘序一字不动（intent 已在上方落盘，这里照旧 publish 一条 completed）。关（未挂 blob /
	// 阈值 0）= externalizeResult 直接返回 nil、payload 一字不动 → 与现状逐字节等价（酸测试）。
	// 外置失败 = fail-open（原样内联完整 result），不破坏"有 intent 就要有 completed"（X1）。
	//
	// **崩溃窗口（如实记口径，消不掉）**：fail-open 只覆盖 Put **返错**这一种失败。blob 写成功、
	// completed 落盘前进程崩掉 → 磁盘上多一份没人引用的 blob（孤儿内容）；本实现"先写 blob
	// 成功、再落引用"的顺序保证**引用与内容最终一致**（引用不会指向空处），崩溃续跑按事件里的
	// `binary_ref` 调 blob.Get 补取全文。孤儿 blob / .tmp 残留的回收走 blobfs.GC
	// （CLI：`alasol blob-gc`；Retention=0 默认只做安全两扫；mtime 清扫的**龄期 = 最后外置
	// 时间**——幂等 Put 刷新 meta.json 的 mtime，故重复引用过的 blob 不被当超期残留删掉）。
	//
	// 注意：回填给模型的始终是**完整** res（末尾 toolResultText(res)）——外置只瘦身事件流，
	// 不让模型看不到完整工具输出。
	payload := contracts.ToolCallCompletedPayload{
		ToolCallID: tc.ID, // 与 intent 同一个黑盒 id（S4：原样往返）
		Result:     res,
	}
	binaryRef := l.externalizeResult(ctx, &payload)
	doneEv, err := l.newEvent(KindToolCallCompleted, &intentID, payload)
	if err != nil {
		return "", events, err
	}
	doneEv.BinaryRef = binaryRef // nil = 不外置（默认/酸测试路径）
	n, err = l.publish(ctx, doneEv)
	events += n
	if err != nil {
		return "", events, fmt.Errorf("loop: 工具 %q 的 completed 事件落盘失败: %w", tc.Name, err)
	}
	return toolResultText(res), events, nil
}

// resolvedTool 是一次工具调用的解析结果：策略框架工具优先，其次注册表（v2-S1）。
type resolvedTool struct {
	desc     *contracts.Tool // 工具描述（框架工具必有；注册表工具可能为 nil）
	exec     core.ToolExecutor
	known    bool
	metaKind string         // 注册表 kind（错误信息用；框架工具 = "tool"）
	impl     any            // 原始注册实现（错误信息用；框架工具 = 其 Exec）
	state    registry.State // 注册表 State（执行前复查用；框架工具 = 零值 ""，不经注册表恒非 disabled）
	// fromRegistry 标记"这次解析走的是注册表分支"（框架工具 = false）。
	// State 门控只对注册表条目生效——框架工具不经注册表、没有 State 可言（缺口 3 p3-1）。
	fromRegistry bool
}

// resolveTool 解析一个工具：**策略框架工具（recall / stage）优先**，其次注册表。
//
// strategyTools 是本次 Run 的策略框架工具（per-Run，见 runState.strategyTools）。框架工具
// per-Run 持有、不经共享 registry——所以多 session 各挂各的策略时 recall/stage 不会重名冲突。
// strategyTools 为空（未挂策略）= 直接走 registry，行为与旧版完全一致（酸测试不回归）。
//
// **优先级与可见集一致**（F12）：可见集（visibleTools）里同名工具只出现一次，执行侧
// 也只解析到一个实现。同名在装配期就已 fail-close 拒绝（collectStrategyTools），
// 所以这里的"框架工具优先"只是**兜底顺序**，不是静默遮蔽。
//
// state 是注册表条目的 State（缺口 3 执行前复查的输入）：框架工具不经注册表，零值
// （""）恒不等于 StateDisabled——State 门控只作用于注册表条目，与可见集第一层同口径。
// fromRegistry 把这条口径写成**显式判据**（不靠"零值恰好不等于 disabled"）。
func (l *Loop) resolveTool(name string, strategyTools map[string]compaction.FrameworkTool) resolvedTool {
	if ft, ok := strategyTools[name]; ok {
		d := ft.Desc
		return resolvedTool{desc: &d, exec: ft.Exec, known: true, metaKind: "tool", impl: ft.Exec}
	}
	entry, known := l.registry.Get(name)
	var exec core.ToolExecutor
	if e, ok := entry.Impl.(core.ToolExecutor); ok {
		exec = e
	}
	return resolvedTool{
		desc:         entry.Tool,
		exec:         exec,
		known:        known,
		metaKind:     entry.Meta.Kind,
		impl:         entry.Impl,
		state:        entry.State,
		fromRegistry: true,
	}
}

// 生效权限 = 请求 ∩ ceiling ∩ 父级授予（树里节点的 perm；闸门③保证子 ⊆ 父）。
// 这层约束是**强制并入**的：超出即 deny，**不可被全局 allow 规则放宽**
// ——声明的权限是上限，不是建议（否则"声明 write=none 的子 agent"照样能写）。
//
// 工具所需的权限维度由 side_effect + 是否执行类推导（agents.RequiredCapabilities）。
//
// side 由调用方从**解析结果**的描述取（desc.SideEffect）：框架工具（recall / stage /
// ledger）**不在注册表里**——只看注册表会把它们一律当成 SideEffectNone，等于**绕过**
// perm 校验（ledger 是写账本的 SideEffectWrite，却按"无副作用"放行）。p3 修。
//
// 单 agent 模式（没有 agent 树）→ 不受 perm 约束（ok=false）。
func (l *Loop) permDenial(toolName string, side contracts.SideEffect) (bool, string) {
	perm, ok := l.effectivePerm()
	if !ok {
		return false, "" // 单 agent 模式：无 perm 约束
	}
	needs := agents.RequiredCapabilities(toolName, side)
	var missing []string
	for _, c := range needs {
		if !agents.PermCovers(perm, c) {
			missing = append(missing, string(c))
		}
	}
	if len(missing) == 0 {
		return false, ""
	}
	return true, fmt.Sprintf(
		"fail-close: 工具 %q 需要权限 %s，但本 agent 的生效权限是 %s，未授予 %s（声明的权限是上限，不可被 allow 规则放宽）",
		toolName, strings.Join(missing, "/"), agents.PermString(&perm), strings.Join(missing, "/"))
}

// effectivePerm 取本 agent 的生效权限（树里节点的 perm）。
//
// 没有 agent 树（单 agent 模式）→ ok=false = 不受 perm 约束。
func (l *Loop) effectivePerm() (contracts.PermissionRequest, bool) {
	if l == nil || l.agents == nil || l.agents.Tree == nil {
		return contracts.PermissionRequest{}, false
	}
	return l.agents.Tree.PermOf(l.agentID)
}

// failToolCall 落一条 tool.call.failed 并规约成 runToolCall 的返回三元组：
// 说明文本回填给模型；结果事件没落成 = 写路径断裂 → 向上传错误。
//
// base 是本次调用**已经**落库的事件数（intent 那一条），返回值里的 events 是合计值。
func (l *Loop) failToolCall(
	ctx context.Context,
	causation *contracts.Ulid,
	tcID contracts.ProviderId,
	message, code string,
	details json.RawMessage,
	base int,
) (string, int, error) {
	n, err := l.emitFailed(ctx, causation, tcID, message, code, details)
	if err != nil {
		return "", base + n, fmt.Errorf("loop: 工具 %q 的 failed 事件落盘失败: %w", tcID, err)
	}
	return message, base + n, nil
}

// executeSafely 调用执行器并隔离 panic（E5 同款失败隔离：坏实现不得把 loop 带崩，
// panic 归类为一次普通的工具失败）。
func executeSafely(ctx context.Context, exec core.ToolExecutor, intent contracts.ToolCallIntentPayload) (res contracts.ToolResult, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			res = contracts.ToolResult{}
			err = fmt.Errorf("工具 %q 执行器 panic: %v", intent.ToolName, rec)
		}
	}()
	return exec.Execute(ctx, intent)
}

// replayOf 取工具的重放语义（X2）；描述缺失或未声明时按 unsafe 处理
// （未知结果不盲目重放——崩溃续跑阶段 8 依此分三类处置）。
func replayOf(desc *contracts.Tool) contracts.ReplayKind {
	if desc == nil || desc.Replay == "" {
		return contracts.ReplayKindUnsafe
	}
	return desc.Replay
}

// validateArgs 校验并规范化工具参数（契约边界：arguments 必须是 JSON **对象**）。
//
// 返回 (normalized, problem)：
//   - problem == "" → 参数合法：normalized 是**原样字节**（逐字节保留，S4 纪律：
//     不解析、不改写、不补全；只做拷贝防调用方随后改写切片改到事件里）；
//     完全空缺时补 {}（避免下游把 null 参数当解组错误）。
//   - problem != "" → 参数不合法（坏 JSON / 顶层不是对象）：normalized 是 "{}"，
//     调用方走**普通失败路径**（intent 落盘 + failed/code=bad_arguments），
//     loop 继续不中止——不能因为模型给错参数形状就把整轮 Run 打死。
func validateArgs(args json.RawMessage) (json.RawMessage, string) {
	trimmed := bytesTrimSpace(args)
	if len(trimmed) == 0 {
		return json.RawMessage("{}"), "" // 空参数合法（补 {}）
	}
	var probe any
	if err := json.Unmarshal(trimmed, &probe); err != nil {
		return json.RawMessage("{}"), fmt.Sprintf("不是合法 JSON（%v）", err)
	}
	if _, ok := probe.(map[string]any); !ok {
		return json.RawMessage("{}"), fmt.Sprintf("顶层是 %s，不是 JSON 对象", jsonTypeName(probe))
	}
	out := make(json.RawMessage, len(args))
	copy(out, args)
	return out, ""
}

// badArgsDetails 把不合法的参数原文包成 details（契约：detail 是 JSON 对象）。
//
// 原文只作为**诊断信息**留档（模型可据此看出自己截断在哪），不参与执行。
func badArgsDetails(raw json.RawMessage) json.RawMessage {
	b, err := json.Marshal(map[string]string{"raw_arguments": string(raw)})
	if err != nil {
		return nil
	}
	return b
}

// jsonTypeName 给出 JSON 值的类型名（给模型看的说明用）。
func jsonTypeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "布尔值"
	case float64:
		return "数字"
	case string:
		return "字符串"
	case []any:
		return "数组"
	case map[string]any:
		return "对象"
	default:
		return "未知类型"
	}
}

// redactSecrets 把框架注入的秘密值从文本里**逐个精确替换**成 "***"。
//
// 精确字符串替换（零猜测零误伤）：只动框架自己注入的那些值；命令自己的输出是数据，
// 原样保留。空值跳过（空串会替换掉一切）。
func redactSecrets(text string, secrets []string) string {
	if text == "" || len(secrets) == 0 {
		return text
	}
	for _, s := range secrets {
		if s == "" {
			continue
		}
		text = strings.ReplaceAll(text, s, "***")
	}
	return text
}

// redactResult 给工具结果做秘密收口（P1-1）。
//
// 覆盖工具输出的全部载体：Content[].Text（命令 stdout/stderr 走这里）、
// Content[].Data（结构化输出）与 GraphEdges（工具产出的图边端点/类型）。
func redactResult(res contracts.ToolResult, secrets []string) contracts.ToolResult {
	if len(secrets) == 0 {
		return res
	}
	out := res
	if len(res.Content) > 0 {
		out.Content = make([]contracts.ContentPart, len(res.Content))
		copy(out.Content, res.Content)
		for i := range out.Content {
			p := &out.Content[i]
			if p.Text != nil {
				t := redactSecrets(*p.Text, secrets)
				p.Text = &t
			}
			if len(p.Data) > 0 {
				p.Data = redactRaw(p.Data, secrets)
			}
		}
	}
	if len(res.GraphEdges) > 0 {
		out.GraphEdges = make([]contracts.GraphEdge, len(res.GraphEdges))
		copy(out.GraphEdges, res.GraphEdges)
		for i := range out.GraphEdges {
			e := &out.GraphEdges[i]
			e.Src = redactSecrets(e.Src, secrets)
			e.Dst = redactSecrets(e.Dst, secrets)
			e.Kind = redactSecrets(e.Kind, secrets)
		}
	}
	return out
}

// redactRaw 对原始 JSON 数据做替换。
//
// 若替换后不再是合法 JSON（秘密里带引号/反斜杠等），退化成 "***"——
// 宁可丢掉结构，也不能泄密，更不能产出非法 JSON（契约要求 data 是合法 JSON）。
func redactRaw(data json.RawMessage, secrets []string) json.RawMessage {
	out := string(data)
	for _, s := range secrets {
		if s == "" {
			continue
		}
		out = strings.ReplaceAll(out, s, "***")
	}
	if json.Valid(data) && !json.Valid([]byte(out)) {
		return json.RawMessage(`"***"`)
	}
	return json.RawMessage(out)
}

// bytesTrimSpace 去掉 JSON 参数首尾空白（只用于判空/校验，不改变入账字节）。
func bytesTrimSpace(b json.RawMessage) []byte {
	start := 0
	for start < len(b) && isJSONSpace(b[start]) {
		start++
	}
	end := len(b)
	for end > start && isJSONSpace(b[end-1]) {
		end--
	}
	return b[start:end]
}

func isJSONSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// denyMessage 组装 deny 的失败说明（M5：模型可读、能自我修正）。
// 工具未注册时额外点名，避免模型反复幻觉同一个工具名。
func denyMessage(name string, dec contracts.Decision, known bool) string {
	reason := ""
	if dec.Reason != nil {
		reason = *dec.Reason
	}
	if reason == "" {
		reason = "（策略未给出理由）"
	}
	extra := ""
	if !known {
		extra = fmt.Sprintf("（另：工具 %q 未在注册表注册）", name)
	}
	return fmt.Sprintf("fail-close: 工具 %s 的策略判定为 %s，未执行：%s%s", name, dec.Verdict, reason, extra)
}

// denyTimeoutPrefix 是"**没人来得及答**"这一类 deny 的文案前缀（与"人审明确拒绝"区分）。
//
// 审计要能分辨两种事实：人点了 deny（**被拒绝**）与审批没等到人（**没人来得及答**）——
// 处置与复盘口径不同，故文案必须可区分。前缀里的"非人审明确拒绝"是**标记位**
// （测试按它在/不在断言归因对不对）。
const denyTimeoutPrefix = "审批未放行（超时/无人应答/审批失败，非人审明确拒绝）→ fail-close 未放行、未执行："

// isNoAnswerDeny 判定 deny 的成因是不是"**没人应答**"这一类（非人审明确拒绝）。
//
// 判据优先取**结构化成因码**（contracts.Decision.Code，v4-7 后半 F7 只增）：
//
//   - approval_failed / approval_context_ended → 没人来得及答（审批人报错/超时、
//     审批前上下文已取消）；
//   - approval_denied → "人审未放行"——但**协议面/终端面的"无人应答"也走 deny 响应**
//     （Approver 返回 deny 而非 error，note 里写"无人应答"），故此码仍要**文案兜底**：
//     note 含无人应答标记 → 归"没人答"；不含 → 人明确拒；
//   - 其余码（rule_matched / policy_default / rule_rejected / capability_denied /
//     approver_absent / approval_context_incomplete / approval_allowed）→ 与"审批没人答"
//     无关，直接 false。
//
// code 缺省（旧判定 / 外部构造的 Decision）→ 退回文案匹配（同源标记，见下）：
//   - "审批失败"：审批人报错/超时（policy.Decide 的审批错误出口）；
//   - "无人应答"：没有人应答（协议面/终端面审批人的无人应答出口）；
//   - "上下文已结束" / "ctx 超时" / "ctx 取消" / ctx 错误原文：审批前、或读到答复后
//     Run 上下文已取消/超时（policy 与各审批人的文案都把这几种写法混着用）。
//
// 为什么不用 ctx.Err() 快照当判据：人明确拒绝后 ctx 可能恰好到期（判据竞态）——
// 快照是**另一个瞬间**的事实，按它归因会把"被拒绝"误记成"没人来得及答"。
//
// 已知边角（仅 code 缺省与 approval_denied 的 note 兜底时存在）：文案匹配可能被含标记词
// 的人写备注误判——比"漏判"少见的误判方向，代价可接受；其余码在场时该边角消失。
func isNoAnswerDeny(dec contracts.Decision) bool {
	if dec.Code != nil {
		switch *dec.Code {
		case contracts.DecisionCodeApprovalFailed, contracts.DecisionCodeApprovalContextEnded:
			return true
		case contracts.DecisionCodeApprovalDenied:
			// 人明确拒 vs 协议面无人应答：同码，靠 note 文案区分（见上）。
			return reasonHasNoAnswerMarker(dec.Reason)
		default:
			return false
		}
	}
	return reasonHasNoAnswerMarker(dec.Reason)
}

// reasonHasNoAnswerMarker 做"没人应答"的文案兜底匹配（与 core/policy、host 协议审批、
// runtime 终端审批的 fail-close 文案同源）。
func reasonHasNoAnswerMarker(reason *string) bool {
	if reason == nil {
		return false
	}
	for _, marker := range []string{
		"审批失败",             // 审批人报错/超时（policy.Decide 的审批错误出口）
		"无人应答",             // 没有人应答（协议面/终端面审批人的无人应答出口）
		"上下文已结束",           // 审批前、或读到答复后 Run 上下文已取消/超时
		"ctx 超时", "ctx 取消", // 同上（审批人文案里的写法）
		"deadline exceeded", "context canceled", // 同上（ctx 错误的原文）
	} {
		if strings.Contains(*reason, marker) {
			return true
		}
	}
	return false
}

// toolResultText 把 ToolResult 拍成回填给模型的文本（text 块拼接，结构化块原样带上）。
func toolResultText(res contracts.ToolResult) string {
	var parts []string
	for _, p := range res.Content {
		switch {
		case p.Text != nil && *p.Text != "":
			parts = append(parts, *p.Text)
		case len(p.Data) > 0:
			parts = append(parts, string(p.Data))
		}
	}
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	default:
		out := parts[0]
		for _, p := range parts[1:] {
			out += "\n" + p
		}
		return out
	}
}
