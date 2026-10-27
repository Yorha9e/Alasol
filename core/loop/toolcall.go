package loop

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/policy"
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
func (l *Loop) runToolCall(ctx context.Context, turn int, tc core.ToolCallReq) (string, int, error) {
	// a. policy 先判（fail-close）：判定只约束发起时刻（M9/陷阱 15），快照进 intent。
	dec := policy.Decide(ctx, l.policy, l.approver, policy.Query{
		Tool:      tc.Name,
		Args:      tc.Args,
		SessionID: &l.sessionID,
		AgentID:   &l.agentID,
	})

	entry, known := l.registry.Get(tc.Name)
	desc := entry.Tool // 未注册时是零值 Entry，Tool 为 nil

	// 参数形状校验（契约边界）：arguments 必须是合法 JSON **对象**。
	// 不满足时 arguments 置 {}（intent 仍要落盘，但不能落一个违反契约的载荷），
	// 走普通失败路径（failed/code=bad_arguments），loop 继续不中止。
	args, argsProblem := validateArgs(tc.Args)

	// b. ① intent durable 落盘（先于任何执行；落盘失败就不执行，X1）。
	intent := contracts.ToolCallIntentPayload{
		ToolCallID:     tc.ID, // provider 黑盒 id 原样（S4/S5）
		ToolName:       tc.Name,
		Arguments:      args,
		Idempotent:     desc != nil && desc.Idempotent, // 缺省 false（M7：不声明不自动重试）
		Replay:         replayOf(desc),                 // 缺省 unsafe（X2：未知结果不盲目重放）
		PolicySnapshot: policySnapshot(dec),            // M9/陷阱 15
	}
	intentEv, err := l.newEvent(KindToolCallIntent, nil, intent)
	if err != nil {
		return "", 0, err // 组装不出合法 intent = 不许执行
	}
	n, err := l.publish(ctx, intentEv)
	if err != nil {
		// 意图没落盘 → 绝不执行（X1：没有意图记录的副作用不可审计、不可恢复）。
		n += l.publishLoopError(ctx, turn, "publish.intent", err)
		return "", n, fmt.Errorf("loop: 工具 %q 的 intent 事件落盘失败 → 拒绝执行（X1）: %w", tc.Name, err)
	}
	events := n
	intentID := intentEv.ID

	// c'. 参数不合契约 → 普通失败（M1/M8：失败是普通事件，不是特殊通道）：
	// intent 已落盘（arguments={}），补一条 failed，模型可读地说明怎么改，loop 继续。
	if argsProblem != "" {
		msg := fmt.Sprintf("参数不是合法 JSON 对象：%s；请重新给出 JSON 对象（形如 {\"key\":\"value\"}）", argsProblem)
		return l.failToolCall(ctx, &intentID, tc.ID, msg, CodeBadArguments, badArgsDetails(tc.Args), events)
	}

	// c. deny（含 ask 被 DenyAll 拒、无命中 fail-close）→ ③ failed，**不执行**。
	if dec.Verdict != contracts.EffectAllow {
		// 工具压根没注册时，主因是"工具不存在"（策略无命中只是连带现象）——
		// 失败码取更能让模型自我修正的那个。
		code := CodePolicyDeny
		if !known {
			code = CodeToolNotFound
		}
		return l.failToolCall(ctx, &intentID, tc.ID, denyMessage(tc.Name, dec, known), code, policySnapshot(dec), events)
	}

	// allow 之后的装配检查：未知工具 / 无工具描述 / 执行器不是 ToolExecutor
	// → 落 failed，不进执行（fail-close：宁可拒绝也不猜）。
	if !known {
		msg := fmt.Sprintf("fail-close: 工具 %q 未在注册表注册（模型给出的工具名查无此扩展）→ 不执行；请只使用可见工具列表里的工具", tc.Name)
		return l.failToolCall(ctx, &intentID, tc.ID, msg, CodeToolNotFound, nil, events)
	}
	if desc == nil {
		// 注册过但 kind 不是 tool（只有 RegisterTool 会带上 Tool 描述）。
		msg := fmt.Sprintf("fail-close: 工具 %q 已注册（kind=%q）但没有工具描述（Entry.Tool 为 nil）→ 不执行", tc.Name, entry.Meta.Kind)
		return l.failToolCall(ctx, &intentID, tc.ID, msg, CodeToolNotFound, nil, events)
	}
	exec, ok := entry.Impl.(core.ToolExecutor)
	if !ok {
		// 装配错误：注册进来的实现不是执行器（RegisterTool 会拦住，这里是纵深防御）。
		msg := fmt.Sprintf("fail-close: 工具 %q 的注册实现不是 core.ToolExecutor（实际 %T）→ 不执行", tc.Name, entry.Impl)
		return l.failToolCall(ctx, &intentID, tc.ID, msg, CodeExecutorMissing, nil, events)
	}

	// ② 执行：执行 env 只经 ctx 传递（H12/H19：值永不进事件/日志）。
	execScope, secrets := l.buildExecEnv(ctx, desc)
	execCtx := core.WithEnv(ctx, execScope)
	res, execErr := executeSafely(execCtx, exec, intent)
	// 秘密收口（P1-1）：工具输出（命令回显）进事件、进下一轮 prompt 之前，
	// 把**框架注入的值**逐个精确替换成 "***"。命令自己的输出是数据，原样保留。
	res = redactResult(res, secrets)
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
	doneEv, err := l.newEvent(KindToolCallCompleted, &intentID, contracts.ToolCallCompletedPayload{
		ToolCallID: tc.ID, // 与 intent 同一个黑盒 id（S4：原样往返）
		Result:     res,
	})
	if err != nil {
		return "", events, err
	}
	n, err = l.publish(ctx, doneEv)
	events += n
	if err != nil {
		return "", events, fmt.Errorf("loop: 工具 %q 的 completed 事件落盘失败: %w", tc.Name, err)
	}
	return toolResultText(res), events, nil
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
