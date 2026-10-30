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
// 在 2 之前还有一步 1.5：**落一条 context.assembled durable 事件**（载荷 =
// contracts.ContextSnapshot，见 context.go）。此刻装配已定型，记录"这一轮给模型看了
// 什么"——上下文可见性的数据源（06 §4.2）。它是观测性事实，落库失败不中止 loop
// （与 X1 的 intent 写路径刻意区分）。
//
// # 对话入事件流（v1-S1，补 F1 缺口）
//
// 除三段写路径外，loop 还把**对话内容**落进事件流，使回放/resume 能重建完整对话历史：
//
//   - Run 开始落一条 user.prompt（载荷 contracts.UserPromptPayload，text=prompt 原文）；
//   - 每轮 provider 返回后落一条 model.turn（contracts.ModelTurnPayload，text + usage）。
//
// 二者是**旁路观测**：best-effort 落库、**不计入 Result.Events**、不改变任何控制流，与 X1
// 三段写路径刻意分离（写路径形状一字不动）。usage 只做成本记录（J21），绝不回填 context
// 快照、不接入任何预算/门禁判断。runtime/replay.go 的 FoldConversation 从这三类事件
// （user.prompt / model.turn / tool.call.*）fold 出可供 resume 续跑的对话历史。
//
// **续跑**：RunWithHistory(ctx, history, prompt) 把既有历史（通常是 FoldConversation 的
// 产出）排在本次 prompt 之前发给模型——对话进了事件流就能被回放重建，而重建出来的历史
// 必须能喂回 loop 才叫续跑。Run = RunWithHistory(ctx, nil, prompt)（语义不变）。
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
//	② 执行（deny 时不执行；Run ctx 已取消/超时时**也不执行**）：registry 取执行器
//	   Entry.Impl.(core.ToolExecutor)，执行 ctx = core.WithEnv(ctx, 执行env)
//	         ▼
//	③ tool.call.completed（result=ToolResult）或 tool.call.failed（error 模型可读，M5/M8）
//
// 崩溃语义（X1）：**有 intent 无 result = 未知结果，不盲目重跑**。本层只保证写路径顺序
// （intent durable 成功才执行），恢复/重放在阶段 8 按 X2 的 replay 语义处理。
//
// 审计与执行是**两条分开的纪律**（审批超时断链修复，events.go publish）：
// **审计必须留痕**——Run ctx 已取消/超时（如 ask 审批一直等到 PromptRunTimeout 兜底才
// fail-close deny 收场）后，intent / failed / loop.error 经短命 settle ctx
// （WithoutCancel + 5s/次，失败即丢）照样落盘，事件流不留断点；**执行必须守约**——
// settle ctx 绝不流进工具执行，**执行前显式查 ctx.Err()**：ctx 已死（取消/超时）就
// **绝不执行**（落 failed，如实写"因 Run 上下文取消/超时未执行"），执行本身也严格尊重
// 原 ctx（取消即中止、不编造结果）。
//
// 为什么护栏必须**显式**：settle ctx 让"ctx 已死 ⇒ intent 落盘失败 ⇒ 绝不执行"这条
// **隐式**护栏失效（ctx 死了 intent 照样落得下）。护栏不能靠副作用兜底——只能自己查。
// 同一条纪律覆盖多 agent 生命周期工具（spawn）：ctx 已死就不派生（建子节点、跑子 loop
// 都是副作用），已派生出来的子节点必须**收口**（agent.failed + Tree.Fail），不许停在
// running（树里留下常驻僵尸节点，事后没人说得清它怎么了）。
//
// # 阶段 8 durable 集成（core/durable）
//
//   - Config.Store 非 nil + Config.CheckpointEvery>0 → 每个有工具调用的 turn 末自动建一次
//     checkpoint（X3，尽力而为，落盘失败不中止 Run）；Loop.Checkpoint(ctx) 手动建；
//   - *Loop 同时实现 core/durable.Replayer：崩溃恢复时 durable.Resume 对 replay=safe（及
//     verify 核验不通过）的操作调 Loop.Replay——按原 intent **再次执行工具并落结果事件**，
//     不重跑策略链（M9：T0 已授权）、不走模型。恢复编排 / verify 核验 / ownership 在 core/durable。
//
// # fail-close 铁律（H8/陷阱 9）
//
//   - 策略无命中 / 引擎为 nil / Approver 为 nil（= DenyAll）/ 审批出错 → 一律 deny，不执行；
//   - 工具未注册 / 注册实现不是 core.ToolExecutor → 不执行，落 failed；
//   - Run ctx 已取消/超时 → 执行前显式挡住（绝不执行），落 failed 记"因 Run 上下文
//     取消/超时未执行"（审计要能分辨"没执行"与"执行失败"）；生命周期工具 spawn 同款
//     （不派生子 agent，落 failed 记"因 Run 上下文取消/超时未发起"）；已派生的子节点
//     必须收口（agent.failed + Tree.Fail），不许停在 running；
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
//
// # 多 agent（阶段 5）
//
// Config.Agents 非 nil 时启用（nil = 单 agent，默认，向后兼容）。本 loop 即其 AgentID 对应
// agent 的一次执行；注入两个**框架内部**生命周期工具（不经注册表/policy，仍遵循 X1）：
//
//   - spawn{agent_type,role,task,request_id}：过 core/agents 树的三闸门（K17：深度/类型/权限）
//     → 落 agent.spawned（agent_id=child）→ 起一个**同 Bus/Registry/Provider/SessionID** 的子
//     loop（AgentID=child.ID，Model/System/tools 取自 child 的 AgentType）同步跑 task → 收回
//     子经 complete 产出的 summary 作为本调用的 ToolResult 返回父。闸门拒绝 → tool.call.failed
//     （模型可读，loop 继续）；子出错 / MaxTurns 截断 → 异常冒泡（agent.failed + abnormal summary）。
//   - complete{text,refs,taint}（子 agent 专属）：落 agent.completed 并停本 loop（Result 收敛）。
//
// 可见性由类型自然涌现（K20/K21）：spawn 只对 can_spawn 非空的 agent 可见；complete 只对
// 子 agent（depth>1）可见；注册工具再按 agent_type.Tools 收窄（R5）。summary 强制带 taint
// （Q7：继承来源最高污点级，只能升不能降）；父收到 tainted summary 时工具结果加
// ⚠ [untrusted data] 前缀并按不可信数据处理。
//
// X5 幂等：同 **(parent_agent_id, spawn_request_id)** 的重复 spawn 返回同一 child、不重跑；
// 换父或换类型都要换 request_id（换类型复用 → 冲突错误，不把别人的 child 交出去）。
//
// 生命周期工具（spawn/complete）**也要过策略**（与普通工具同款）：被 deny 的不进模型
// 可见集，硬调也落 failed 不执行。子 agent 的**生效权限**（请求 ∩ ceiling ∩ 父级授予）
// 在运行期约束它的工具调用：所需权限维度超出即 deny，全局 allow 规则不能放宽它
// （H11/H22：声明的权限是上限）。Loop 仍无可变状态（污点累积 / spawn 缓存都是 Run 的
// 局部变量）→ 可并发 Run。
package loop
