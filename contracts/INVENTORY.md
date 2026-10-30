# contracts · 跨边界契约唯一清单（D21 唯一清单源）

> **地位**：本清单是跨边界契约的**唯一定义处**（`07` D21）。其他文档只**引用不重抄**；契约计数以本清单生成（当前：**11**）。
> **状态**：清单已冻结（2026-10-27）。阶段 1 已落地：schema 文件（12 份）+ 自写 codegen + Go 类型 + TS 消费端 + CI schema job。
> **口径**：只收**跨边界**契约（跨进程 / 跨语言 / 跨时间流动的数据）。进程内接口（Provider / Store 接口 / Tool 执行器 / Approver / Sandbox / Scheduler / CompactionStrategy / ContextAssembler / Telemetry 等）用原生 Go interface，**不进本清单**（`07` D1，`06` §3.2）。

---

## 1. 清单（11 个）

| # | 契约 | schema 文件 | 内容要点 | 关键约束 |
|---|---|---|---|---|
| 1 | **Event** | `schema/event.json` | 唯一事实流 envelope：`v` / `id` / `kind` / `seq` / `ts` / `session_id` / `agent_id` / `causation_id` / `durable` / `payload` / `binary_ref` | F3/F7/F8；kind 开放字符串 + 点分命名空间（D6）；未知 kind 宽容：忽略但持久化（D7） |
| 2 | **Command** | `schema/command.json` | 意图；可从进程外提交，经 core 处理产生 Event（D9） | — |
| 3 | **Transport 消息** | `schema/transport.json` | 客户端/服务端通信 envelope（`ws+ndjson` / `stdio` / `inproc` 同一契约）：订阅（subscribe→subscribed 回执）、事件推送（event）、心跳（ping/pong）、对话发起（command.submit→command.accepted）、审批应答（approval.request/approval.respond）、错误（error）（N4/N6） | NDJSON 行式；kind 开放字符串（v0 固化上述几个）；`TransportMessage.id` 是**客户端关联 id**（G9：服务端只做 reply_to 回指、不去重幂等，客户端可造 ULID 或任意不透明字符串） |
| 4 | **Extension 元数据** | `schema/extension.json` | `kind` / `schema` / `capabilities` / `priority` / `reloadable` / `health`（D17） | — |
| 5 | **Tool.schema** | `schema/tool.json` | 工具描述：`schema` / `side_effect`（none/read/write/destructive）/ `cancelable` / `approvable` / `idempotent` / `replay`（D12/D22） | `schema` 字段**值本身是 JSON Schema**（元嵌套）→ codegen 当原始 JSON 处理 |
| 6 | **Tool 返回值** | `schema/tool-result.json` | 工具返回值：`graph_edges: [{src,dst,kind}]` / `taint` / 可重放标记（D22/G22/Q7） | 跨边界：MCP 与进程级扩展都要传 |
| 7 | **Policy 规则** | `schema/policy.json` | 规则表达式（配置文件形态）+ 判定结果 `Decision`：`allow`/`deny`/`ask`，ask 携带 `suggested_rules`（D13/H5） | workflow 侧规则**只可收窄**（H5/H24） |
| 8 | **Store 持久化格式** | `schema/store.json` | 事件日志持久化/导出格式（NDJSON 事件记录）（G 系） | 大 payload 走 `binary_ref` 外置（D4） |
| 9 | **Workflow** | `schema/workflow.json` | 工作流定义（纯数据）：`philosophy` / `agent_types[]` / `context_strategy` / `gates[]` / `grants[]`（`06` §4.1） | `permission_request`/`grants` 是请求，实际 = 请求 ∩ 策略上限 ∩ 父级授予（H11/H22） |
| 10 | **ContextSnapshot** | `schema/context-snapshot.json` | 上下文构成清单：`parts[]`（source/kind/entries/tokens/state/origins/summary_ref）、`state ∈ verbatim/compacted/dropped/deferred`（`06` §4.2） | 每轮一条 durable 事件 `context.assembled` |
| 11 | **Approval 消息** | `schema/approval.json` | 审批请求 / 答复（跨进程：TUI / GUI / 远端）（D21 补入） | **无人应答 = deny，不得挂起**（D14/H8，fail-close） |

## 2. 事件 core kind 子集（随 Event 契约定稿）

kind 是开放字符串（D6），core 定义核心子集。**kind→payload 已定稿全集**（与 `schema/event.json` 的 `x-kind-payload` 同步，2026-10-28 v1-S2 补全此前漏账）：

| kind | durable | 说明 |
|---|---|---|
| `user.prompt` | true | **对话入流**（F1，v1-S1）：用户提示原文；Run 开始落（旁路观测，不计 Result.Events） |
| `model.turn` | true | **对话入流**（F1，v1-S1）：模型回复 + `ModelTurnUsage`（usage **只进成本统计**，J21——不回填快照） |
| `tool.call.intent` | true | **三段事件之①**（X1）：tool_call_id / 参数 / idempotent / replay 标记 / 外部可核验锚点（如 commit sha） |
| `tool.call.completed` | true | **三段事件之③a**（X1）：结果（Tool 返回值契约） |
| `tool.call.failed` | true | **三段事件之③b**（X1）：失败（错误为普通事件，M1/M8） |
| `ledger.updated` | true | **goal ledger 全量快照**（`LedgerUpdatedPayload`，G8/G23）：goals / constraints / completed / pending 四张清单**全文**；整帧 LWW（seq 大者胜），当前态 = 索引器投影（ledger 不是独立可变存储，事件流是唯一事实源） |
| `board.written` | true | **共享黑板键写入**（`BoardWrittenPayload`，K22/G23）：键 + 原始 JSON 值 + 作者；`tombstone=true` = 删除该键（**墓碑行**语义：保留行、值为 NULL、读取时视为不存在；旧墓碑盖不掉新值、旧值复活不了已删键） |
| `agent.spawned` | true | subagent 诞生（`spawn_request_id` 幂等键 X5、`role` K13） |
| `agent.completed` | true | subagent 收敛：结构化 summary 是回主 agent 的**唯一通道**（K24；taint 继承 Q7） |
| `agent.failed` | true | subagent 失败（异常向上冒泡 K23） |
| `gate.checked` | true | CI 门禁检查结果（receipt 四要素校验 V2；`elapsed_ms` V5） |
| `context.assembled` | true | 每轮模型调用前的上下文快照（G12，载荷 = ContextSnapshot 契约） |
| `checkpoint.created` | true | durable checkpoint 四要素（X3） |
| `session.resumed` | true | resume 流程完成标记（X4） |
| `session.resume_claimed` | true | in-flight 原子认领（X5 防并发重放） |
| `gate.joined` | true | session 加入 gate（K12，v1-S2）：`gate_id` + `source`（auto_repo/auto_workflow/explicit）；无 `gate.left`——离开 = session 结束 |
| `agentlink.broadcast` | true | 广播消息（K18，v1-S3）：`target`（gate/subtree）+ `text`；拉模型投递，收件方 = 目标 ∩ 发送方可见集（K11/K27） |
| `agentlink.handoff` | true | 结构化交接（K18，v1-S3）：broadcast + 开放结构 `payload`（工作流自定义，框架不解释） |
| `context.compacted` | true | **一次压缩动作的事实记录**（`ContextCompactedPayload`，v2-S1 级联 C 案）：两级压缩 L1/L2 + 兜底三级 + 留存降级（full→l1→l2 最老先降、永不回升）；`action`=downgrade/split/budget_warning/fallback（压缩模型失败→退化截断，**留痕**），`tier`=full/l1/l2，`seq_from`/`seq_to`=被压原文机器区间，`ref`=stage:N@e:from..e:to（阶段号 + seq 锚，跨 Run 稳定）给 LLM，`usage`/`model`=压缩模型调用的 token 计量与模型名（成本统计口径，J21）。**refs 永不指摘要**；压缩输入=goal ledger+被压区间全文（永不读他阶段摘要，J17/J18/J19）；阶段大小分布/L1L2 残留/降级兜底触发可从事件流+`context.assembled` 复盘（13 §9） |
| `review.recorded` | true | **一条分支 review 结论的事实记录**（`ReviewRecordedPayload`，v2-S3 门禁增强 V6）：`branch` + `tip_commit`（被 review 时的分支 tip，失效判定基准）+ `conclusion`（approved/changes_requested）+ 可选 `note`。review 后分支 tip 变 → 结论失效 → 要求重审（事件流天然可检测：被 review 的 tip 就在 payload 里，合并门禁拿它比对分支当前 tip）。**reviewer 身份不进 payload**（V9：无独立 reviewer，结论来自 agent 树角色，身份由 `envelope.agent_id` 承载；字段形状不绑死） |
| `conflict.predicted` | true | **一次合并后冲突预测的事实记录**（`ConflictPredictedPayload`，v2-S3 门禁增强 V7）：合并成功后对"本次改动 vs 未合并分支"用依赖图可达性预警撞车（比三点 diff 求交集更准）。`merged_branch` + `available`（**聚合**可用性 = 合并侧可评估 **且** 每个未合并分支都可评估）+ `collisions`（**确认命中**的分支数）+ 可选 `unevaluated`（**不可评估**分支数）+ `risks[]`（逐分支 `ConflictRisk`：branch/collides/basis/available/reason + 可选 `unknown_roots`/`truncated`）。diff 由调用方喂（roots，v2-S2 口径：框架不算 diff）。**判据是非对称的**：`basis` 条目 = `<命中类型>:<节点>`——**direct**=同触点（确定信号）/ **one-side**=一侧触点落在另一侧影响面内（传递信号）；**前缀取值见 schema 的 `x-item-prefix` 注记**（`direct|one-side`）；不用"影响面相交"那种对称判据（"共同影响同一个下游"≠撞车：兄弟叶子会误报；代价=同一消费者同时依赖两个被改的库时不报，weak 信号留给消费端）。**诚实降级（N2）**：图未启用/追闭包出错/闭包方向不对/**影响面没追全**（触点不在图里或闭包被截断）/**有未合并分支不可评估**时 `available=false` + `unavailable_reason`——明说"预测不可用"而非"无冲突"；逐分支另给 `unknown_roots`/`truncated` 标出**覆盖度**（缺这两个信号时说"影响面无交集"就是假话）。**不许只读聚合层**：`available=true` 时不存在 `risks[].available=false` 的分支（`unevaluated` 说明还有几个分支在黑箱里）；逐条结论以 `risks[].available` 为准 |
| `hub.alert` | true | **K9 hub 级资源监控的一条超阈值告警**（`HubAlertPayload`，v2-S5 清账②：告警此前只进 slog 日志，事件流里看不到、崩溃后不可复盘 → 补成 durable 事实事件，F7 只增）：`metric`（cpu/mem/goroutines）+ `value`（实测值）+ `threshold`（生效阈值）+ `message`（人读正文）。**envelope 归属伪 session**：hub 级事件不属于任何 agent session，而 envelope 强制要求 session_id/agent_id（ULID）→ 宿主按**事件库路径确定性派生**一对伪 id（跨重启稳定，同一库的告警聚成一条可查的流；不是 agent session，不参与在线表/gate 语义）。payload **只记结论**，不含任何会话内容/凭据 |
| `btw.forked` | true | **一次 /btw 旁支 fork 的事实记录**（`BtwForkedPayload`，v3-S2 fork+/btw，K29 拍板）：`source_session_id`（发起 /btw 的主 session，fork 的上下文来源）+ `question`（/btw 问题原文）。payload **只记来源与问题原文**，**不含主 session 的对话内容**/凭据（问题原文本身是对话内容，故不能说"不含任何对话内容"）。fork **只附带上下文**（对话历史，FoldConversation 同源，**带条数/长度上限 + 截断告知**）、**不复制 ledger/gate 全套**；**历史口径：主轮次不带历史、/btw 带历史**（主 session 每一轮只带本轮 prompt，历史只在 resume 与 /btw 两处被带回去）。**envelope 归属 fork（旁支）session**（session_id/agent_id = fork 的 id，与主 session 完全不同）→ 主 session 事件流**零新增**（/btw 结果只给用户、不回灌主上下文）。**预埋"从 /btw 生成全新 session"钩子（只埋点不实现）**：本事件 + 宿主 `BtwPromote` 回调即扩展点；把 fork 提成一流 session 需 gate 归属/在线表可见性/agent 树/UI 多方面微调，v3-S2 不实现（埋点≠实现） |
| `workflow.activated` | true | **一个 session 激活一个工作流能力包的事实记录**（`WorkflowActivatedPayload`，v3-S3 工作流引擎，W1/W3/W11）：`workflow_id` + 可选 `command`（**实际触发的** slash 命令名；缺省 = 显式触发）+ `philosophy`（注入 system prompt 的哲学原文）。工作流是**能力包**（哲学 + agent_types + context_strategy + gates/grants），**不是流程、非强制**（W1/W2）——激活只把哲学文本注入 system prompt 供主 agent 选择（W3），框架**不校验**编排序列（W21，主 agent 自决）。偏离/自造流程记入事件流（W11 动态性）。记录 `philosophy` 原文供审计（事后可复盘当时注入了什么）。**`command` 记的是实际敲出来的命令**：能力包自己声明的 `command`（如 tower 的 `tower`）在 chat/TUI 里是**真能敲的入口**（`/tower` 即激活该包），不是没人用的字段；走 `/workflow <id>` 显式激活时 `command` 留空（= 显式触发） |
| `workflow.milestone` | true | **一次工作流里程碑被处理的事实记录**（`WorkflowMilestonePayload`，v3-S3 工作流引擎，W14 代码扩展点之一 MilestoneHandler）：`workflow_id` + `milestone`（里程碑名，开放字符串，如 validation.passed / merge.ci）+ `passed`（是否通过）+ 可选 `note`。里程碑 = 框架确认的**验证通过点**（合并前仿 CI 检查通过 / critic→reviewer 循环收敛），由工作流包的 MilestoneHandler 处理；`passed=true` 时可联动阶段硬边界（v2-S1 `MarkValidationPassed` = 验证通过点的生产路径）。**框架不强制序列**（W2/W21）：里程碑是能力包部件，主 agent 自决何时触发 |

写路径铁律（X1）：`intent` **durable 落盘** → 执行 → `result` durable 落盘。**有意图无结果 = 未知结果，不盲目重跑**。

其余 core kind（`policy.decided` 等）随对应阶段补 payload schema，envelope 复用 `event.json`；kind→payload 全量映射以 `schema/event.json` 的 `x-kind-payload` 为准（D21：源头一处）。

### 2b. Transport 消息 kind 子集（v3-S4 补全，G1/G8）

`TransportMessage.kind` 是**开放字符串**（D6/D7 同 Event：未知 kind 宽容）。v0 固化的几个（**不是**事件 kind——不进事件流、不占 seq，是 WS 连接上的控制/对话消息）：

| kind | 方向 | 载荷 | 说明 |
|---|---|---|---|
| `subscribe` | client→server | `SubscribeRequest` | 订阅事件流（K4 远程订阅；重复 = 替换） |
| `subscribed` | server→client | `SubscribeAck` | **订阅生效回执**（G8）：回显生效过滤条件 + `replaced`；reply_to 指向原 subscribe |
| `event` | server→client | `Event` | 事件推送（按订阅过滤）；客户端发 = `forbidden_kind` |
| `ping` / `pong` | client→server / server→client | — | 心跳（pong 的 reply_to = ping.id） |
| `command.submit` | client→server | `CommandSubmit` | **对话发起**（G1）：提交一轮 prompt，服务端异步跑、结果经事件流回流 |
| `command.accepted` | server→client | `CommandAccepted` | command.submit 受理回执：带回生效 session id + status=running（reply_to 指向原 submit） |
| `approval.request` | server→client | `ApprovalRequest`（approval.json） | **审批请求**（G1）：ask 策略触发时经 WS 广播推给所有连接；载荷四要素 + trigger/tool_call_id/side_effect/summary/detail |
| `approval.respond` | client→server | `ApprovalResponse`（approval.json） | **审批应答**（G1）：按 `payload.request_id` 注入挂起的审批；无人应答/超时 = deny（fail-close，D14/H8） |
| `error` | server→client | `TransportError` | 协议层错误（reply_to 指向出错消息） |

**审批不是事件 kind**：审批是**实时交互**（必须人在线应答，回放一条几天前的 approval.request 没有意义），故走 WS 控制消息而非 durable 事件流。fail-close 铁律：无人应答 = deny，不得挂起（D14/H8/陷阱 9）。

**载荷映射机器可读**：`schema/transport.json` 的 `$defs.TransportMessage["x-kind-payload"]` 给出 kind→载荷 `$defs` 的映射（同事件 schema 的 `x-kind-payload` 约定，跨文件写成显式 ref，如 `approval.json#/$defs/ApprovalRequest`）。跨语言消费端据此校验载荷，**不硬编码** kind→载荷表（消费端见 `tools/ts-consumer/reader.ts`）。

**消息 id 的口径（G9）**：`TransportMessage.id` / `reply_to` 是**客户端关联 id**，schema 形状 = `string`（**不限格式**：框架 ULID 或任意不透明字符串皆可），服务端**只回指、不校验、不去重**。它是**客户端** id，不是框架 id——故不标 `x-id-kind: framework`、不套 ULID pattern（见 §5 变更记账）。

## 3. 生成与编码约定（schema 与 codegen 共同遵守）

- **演进铁律（F7）**：只增不删、不改义；弃用字段标 `x-deprecated`；envelope 带版本字段 `v`
- **破坏性变更的分界规则**：**对外发布前**允许破坏性变更（直接改字段名 / 删字段），但必须在本清单 §5「契约变更记账」留痕；**发布后**必须走 `x-deprecated` + 版本递增，只增不删（改字段名会打断已发布的消费端——发布前可以，发布后不行）
- **可选字段一律指针 + `json:",omitempty"`**（D2）——**例外：`json.RawMessage` 字段保持裸类型（nil 即缺省），见 D2 实现**；枚举 = 命名类型 + 常量；union = 接口 + Marshal/Unmarshal 包装
- **codegen = 自写受限子集**（D2）：object / array / string / number / bool / enum / required / optional / nested / 非循环 `$ref`；禁 `$ref` 循环与复杂组合
- **id 纪律（S 系，`08`）**：
  - JSON 里 id 一律 **string**（S6）；序号（`seq`/`turn_id`/`step_id`）用 JSON integer
  - 框架 id 字段 schema 标 `pattern: "^[0-9A-HJKMNP-TV-Z]{26}$"`（ULID 26 字符）+ `x-id-kind: "framework"`（S1/S12）
  - provider `tool_call_id` = 不透明 string（`x-id-kind: "provider"`），与 `Event.id` **永不共字段**（S5）；原样存原样回传
  - id 不做修复：字符集/长度不合则拒绝并记事件（S9）
- **序列化**：JSON Schema + NDJSON（D3）；schema 导出 `contracts/schema/`，随契约版本发布（D20）。**基线 = JSON Schema 2020-12**（`$defs`），各文件自带 `$id`（`https://github.com/Yorha9e/alasol/contracts/schema/<file>.json` 命名空间，跨文件 `$ref` 可自解析）；`x-` 注记分两类：**codegen 消费**（`x-go-type`：决定 Go 类型，取值受校验，与 `$ref` 并存时优先）与 **codegen 忽略**的纯注记（`x-id-kind` / `x-invariants` / `x-kind-payload` / `x-deprecated` 等机器可读约定）
- **消费端校验**：`x-` 注记（`x-go-type` / `x-id-kind` / `x-invariants` / `x-kind-payload` / `x-deprecated` 等）是自定义关键字，ajv 默认 `strict` 模式会拒——消费端需 `strict:false` 或显式注册这些关键字（`tools/ts-consumer/reader.ts` 用 `strict:false`）
- **CI**：生成物入库，「再生成 diff 为空」（阶段 1 点亮 ci.yml 的 schema job）。`-check` 同时查**孤儿生成物**：schema 改名/删除后残留的生成物必须报错并清掉（手写文件如 `contracts/doc.go` 永不被动）

## 4. 阶段 1 施工顺序（11 §2）

1. ✅ 冻结本清单（2026-10-27）
2. ✅ 手写 JSON Schema（2026-10-27 完成）：`contracts/schema/` 共 **12 文件 = 11 契约 + `common.json` 共享原语**（`Ulid` / `ProviderId` / `ReplayKind` / `SideEffect`，其余文件 `$ref` 引用——一个类型只定义一次，同包生成无重名）；含 §2 三段事件 payload。跨文件 `$ref` 形如 `tool-result.json#/$defs/ToolResult`（非循环）
3. ✅ 自写受限 codegen（schema → Go 类型，生成物入库）：`internal/codegen` + `cmd/codegen`，受限子集 / 保序 / 确定性 / fail-closed；生成物在 `contracts/*.go`（`contracts/doc.go` 是手写的，不归 codegen 管）
4. ✅ TS 最小消费端验证（能读事件流，跨语言中立性）：`tools/ts-consumer/`（ajv 2020-12），kind→payload 映射读 `event.json` 的 `x-kind-payload`，命名空间从各 schema 的 `$id` 反推
5. ✅ CI schema job 点亮（再生成 diff 为空 + TS 消费端 + 类型检查 + 坏样本必须被拒）

**硬门槛（11 §2 阶段 1 验收）**：生成物入库 + 再生成 diff 为空 · 可选字段指针+omitempty · TS 消费端可读 · `Tool.schema` 元嵌套按原始 JSON 处理。**契约稳定前不写 core 实现。**

## 5. 契约变更记账

**破坏性契约变更**（改字段名 / 删字段 / 改语义）在这里留痕——口径见 §3 的分界规则：**发布前**允许直接改（但必须记账），**发布后**只走 `x-deprecated` + 版本递增。非破坏性的说明性澄清（只改 description）不记账。

| 日期 | 契约 | 变更 | 原因 | 阶段 |
|---|---|---|---|---|
| 2026-10-27 | `transport.SubscribeRequest` | `kinds` → `kind_prefixes` | 字段名与语义漂移：原名 `kinds` 暗示"精确 kind 列表"，description 却写"前缀匹配由实现定义"——改为 `kind_prefixes` 并把语义钉死为**按 kind 前缀匹配**（如 `tool.call.`）。**发布前破例一次**（发布后须走 `x-deprecated` + 版本递增） | 阶段 2 core 骨架（P2-7 修语义漂移） |
| 2026-10-30 | `transport.TransportMessage.id` / `.reply_to` | `$ref: common.json#/$defs/Ulid`（26 字符 ULID pattern）→ `{"type":"string"}`（**任意字符串**） | **契约文字与实现矛盾**（G9）：description 早就写"客户端可造框架 ULID，也可造任意不透明字符串（服务端不校验其格式，只原样回指）"，实现也只原样回指（transport 层的 id 不是框架 id，无 `x-id-kind`），schema 却仍是 ULID pattern → 第三方 GUI 用自己的关联 id 会被 schema 判非法。**放宽到与文字/实现一致**（发布前破例一次）。配套：Go 侧 `TransportMessage.ID` 由 `Ulid` 变 `string`、`ReplyTo` 变 `*string`（codegen 全链）；钉子 = transport 侧"不透明 id 原样回指"用例 + TS 消费端的 transport 样本 | v3-S4 协议面补全（P2-3 修契约矛盾） |

---

## 相关

- 源头裁决：`docs/research/07-design-review-checklist.md`（D1/D2/D21/D22、F3/F7/F8、S 系、X1）
- 形状细节：`docs/research/06-skeleton-design.md`（§3 契约、§4.1 Workflow 示例、§4.2 ContextSnapshot 示例）
- ID 纪律：`docs/research/08-identity-model.md`
- 施工与验收：`docs/research/11-construction-plan.md`（阶段 1 节）
