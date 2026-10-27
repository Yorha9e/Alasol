// Package loop 实现 agent loop 状态机与工具执行的三段事件写路径（X1）。
//
// # 定位
//
// 本包是 core 的"骨架逻辑"里唯一驱动一轮轮模型调用的地方：组请求 → 调 provider →
// 执行工具 → 回填结果 → 下一轮，直到模型不再发起工具调用（或被 MaxTurns 截断）。
// 它只依赖 core 定义的接口（Provider / ToolExecutor / Approver / Store 经 bus），
// 不含任何 I/O 实现（SQLite / 进程 / 网络都在 runtime）。
//
// # 状态机（每一轮 turn）
//
//	┌──────────────────────────────────────────────────────────────┐
//	│ 1. 组 core.ModelRequest                                       │
//	│      System + 历史 Messages + 可见工具（registry 里 kind=tool   │
//	│      的 Entry.Tool，Schema 原样传给模型，R5/R6）                │
//	│ 2. Provider.Run(ctx, req, emit)                               │
//	│      emit 的流式事件**不落库**（model.delta 是 durable=false     │
//	│      流事件，F8/陷阱 11：不入库、不占 seq）；v0 不转发流事件，    │
//	│      只把返回值用于写路径                                       │
//	│ 3. ModelResult.ToolCalls 逐个走三段写路径（见下）               │
//	│ 4. 工具结果回填 Messages（role="tool"，ToolCallID 原样）→ 下一轮 │
//	│ 5. 模型无 ToolCalls → 结束，返回 Result.Text                    │
//	└──────────────────────────────────────────────────────────────┘
//
// Provider 出错不是工具调用失败（M1：失败是普通事件）：Run 返回 error 由调用方处理，
// 同时尽力补一条 durable 的 loop.error 事件供事后审计。ctx 取消立即返回 ctx.Err()。
//
// # 三段事件写路径（X1，每个工具调用恰好一对：intent + (completed|failed)）
//
//	① tool.call.intent  durable 落盘（tool_call_id / arguments 原样、idempotent /
//	  replay 取自 registry 里的工具描述、policy_snapshot）
//	         │  intent 落盘成功才允许继续（落盘失败 → 拒绝执行，Run 返回 error）
//	         ▼
//	② 执行（deny 时不执行）：registry 取执行器 Entry.Impl.(core.ToolExecutor)，
//	   执行 ctx = core.WithEnv(ctx, 执行env)
//	         ▼
//	③ tool.call.completed（result=ToolResult）或 tool.call.failed（error 模型可读，M5/M8）
//
// 崩溃语义（X1）：**有 intent 无 result = 未知结果，不盲目重跑**。本层只保证写路径顺序
// （intent durable 成功才执行），恢复/重放在阶段 8 按 X2 的 replay 语义处理。
//
// # fail-close 铁律（H8/陷阱 9）
//
//   - 策略无命中 / 引擎为 nil / Approver 为 nil（= DenyAll）/ 审批出错 → 一律 deny，不执行；
//   - 工具未注册 / 注册实现不是 core.ToolExecutor → 不执行，落 failed；
//   - 执行器 panic 被隔离成一次普通失败（E5 同款失败隔离），不带崩 loop；
//   - env 键未获准（policy deny）→ 不注入。
//
// # 凭据纪律（H12/H17/H19/H21）
//
// 执行 env = ScrubEnv(进程env ∪ SessionEnv) ∪ 单次调用注入：
// 工具描述里的 RequiredEnv 键逐个过策略（Query{Tool:"env:"+key}），allow 才从进程 env
// 取值注入。env 值只经 ctx 传给执行器，**永不进事件 payload / Message / 日志**；
// 事件里连 env 键名都不记（H19 只允许记键名，v0 选择完全不记）。
//
// 但命令自己 `echo $VAR` 会把注入的值**打进 stdout**——那是工具输出。于是有一道
// 收口（P1-1）：框架知道自己注入了哪些值（session 级 + 单次调用级获批注入），
// 在「工具输出 → ToolResult」这一步把这些值**逐个精确字符串替换**成 `***`
// （零猜测零误伤：只动注入值，命令自己的输出原样保留）。收口点在 loop 的
// runToolCall 收结果处 → 覆盖全部工具，completed 事件与下一轮 prompt 都拿不到真值。
//
// # 参数形状（P1-2）
//
// 工具参数必须是**合法 JSON 对象**（契约：arguments 是 object）。不合时走普通失败
// 路径：intent 照落（arguments 置 {}，原文进 error.details）→ tool.call.failed
// （code=bad_arguments，文案告诉模型怎么改）→ loop 继续不中止。
package loop
