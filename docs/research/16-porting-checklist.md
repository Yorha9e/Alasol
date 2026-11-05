# 16 · 移植全景清单（Alasol → 另一 harness）

> **用途**：把本 harness（Alasol）整套"骨架 + 契约 + 运行面 + 观测面"摊成一张**可逐条勾销的验收清单**。移植方拿它对照自己的实现，逐项打勾 / 打叉。
> **用法**：§1「硬约束」一节**任何一条破坏 = 移植失败**（整节全绿才算过）；§2–§13 的五张全表（事件 kind / HTTP 端点 / 工具 / CLI 子命令 / TOML 键）是**防缺漏骨架**，逐条对照，不许凭印象补。
> **权威档案指针**：`07`（280 条决策总表，唯一裁决来源，逐条对照）、`11`（施工计划）、`15`（session/per-prompt 设计）、`README`（状态与文档索引）。本清单**不重抄**决策正文，只给**出处 + 可核对的实物**。
> **纪律**：本清单里每一条都能在仓里点到实物（文件、行号、事件 kind、端点路径、工具名、配置键）。**本文档全部条目已逐条核实**（五张全表与代码做了全量交叉核对）；凡是**核对不实**的才标「（待核）」——不要照抄没核实的东西。

---

## 0. 系统一句话 + 架构骨架

### 0.1 一句话

**Alasol 是一个"事件流唯一事实源"的 agent harness**：一轮 prompt = 一串 durable 事件，**当前态一律由事件流 fold 得出**（不另设可变状态源）；core 是纯逻辑骨架（**无任何 IO 实现**），runtime 是 core 定义的接口在真实世界的实现，host 是**唯一组合根**，cmd 是入口薄壳。

出处：`07` F1 / F5 / F4；`09-package-boundaries.md` §1；`core/doc.go`。

### 0.2 依赖方向（硬约束，机械可查）

```
cmd/  →  host/  →  core/  →  contracts/
          │         ↓  runtime/  →  core/(接口) + contracts/
          └────────────→ ui/      （ui 只 import contracts，不 import core）
```

- `core` **不得** import `runtime` / `host` / `ui`（07 C2；`09` §2 规则 1）——可测性铁律：core 的单测不需要任何真实 IO。（实测：全仓 `core/` 无一处 import `runtime`。）
- `contracts` **不 import 任何内部包**（`09` §2 规则 2）——契约要能被任何语言独立消费。
- `runtime` 只**实现** core 定义的接口（exec / fs / net / proc），core 只定义接口（Go 惯例：接口由消费方定义）。
- 跨边界的类型**只能在 `contracts` 定义**；core / runtime 内部类型不得外泄到边界（`09` §2 规则 6）。
- **ui 的窄例外（如实标注）**：`ui/web`（Web UI）不 import core；但 **`ui/tui` import 了 core**——它要实现 `core.Approver` 这个进程内接口（`ui/tui/approver.go` 的 `var _ core.Approver = (*Approver)(nil)` 断言 + 审批桥）。这与 `ui/doc.go` 自述的"只依赖 contracts，不 import core"有出入，**是规则的窄例外，不是 bug**；移植时要重新决定：要么复刻（带审批桥），要么把审批桥挪进 host（更贴 `09` 的规则）。

### 0.3 包职责（仓内实物为准，一行一个）

| 包 / 目录 | 一句话职责 | 关键子目录 |
|---|---|---|
| `contracts/` | 跨边界契约唯一清单源（D21）：11 个契约 + `common.json` 共享原语；schema → codegen → Go 类型 | `schema/`（12 份）、`INVENTORY.md` |
| `core/` | agent 骨架逻辑：loop、事件总线、策略链、上下文装配、扩展注册、发现表、门禁判定、重试控制流；**只定义能力接口，不含 IO** | `loop/` `bus/` `policy/` `registry/` `discovery/` `agents/` `agentlink/` `compaction/` `durable/` `sessioncfg/` `graph/` `gate/` `retry/` `storage/` `workflow/` |
| `runtime/` | core 接口的真实实现：SQLite 存、进程起、沙箱落、provider 调、WS/stdio 传输、索引器 | `store/` `cap/` `provider/` `transport/` `indexer/` `tools/` `mcp/` `mcpserver/` `acp/` `approver/` `models/` `telemetry/` `worktree/` `verify/` `identity/` |
| `host/` | **唯一组合根**：装配 core + runtime，对外暴露 HTTP / WS / Web UI | `hub.go` `http.go` `approval.go` `takeover.go` `onlinetable.go` `standby.go` `fork.go` `config*.go` `wal.go` `budget.go` `blob*` `vectors.go` `docs.go` `graph.go` `mergegate.go` `mcp.go` `extensions.go` `workflow.go` `agentlink.go` `discovery.go` `monitor.go` `telemetry.go` `git.go` `sandbox.go` `worktree.go` |
| `cmd/alasol/` | 可执行入口薄壳（逻辑都在 core / runtime） | `main.go` `run.go` `resume.go` `serve.go` `chat.go` `tui.go` `acp.go` `mcp_serve.go` `models.go` `wal.go` `wal_gc.go` `blob_gc.go` `eval.go` `wire.go` `config.go` |
| `cmd/codegen/` + `internal/codegen/` | 自写受限子集 codegen：schema → Go 类型 | 生成物入 `contracts/*.go` |
| `ui/` | Web UI（`ui/web`，嵌入 `index.html`）+ 三面 TUI（`ui/tui`，bubbletea）；只渲染 core 给出的结论，**不做语义判断**（`ui/web` 不 import core；`ui/tui` 为实现 `core.Approver` 桥 import core —— **窄例外**，见 §0.2） | — |
| `sdk/` | 给外部宿主的最小稳定入口（当前仅 `doc.go`） | — |
| `internal/` | 不对外承诺的实现细节 | `codegen/` |
| `tools/` | 非 Go 资产：码点扫描器 + **TS 消费端三件套**（跨语言中立性验证） | `scan_codepoints.py` `ts-consumer/` |

---

## 1. 硬约束（不可破坏，每条给出处）

> 移植验收 = 本节**逐条全绿**。任一条破坏 = 移植失败（整节全绿才算过）。
> 「出处」列给 **07 的条目号**（`docs/research/07-design-review-checklist.md`）+ **仓内可点的实物**。

| # | 硬约束 | 出处（07 条目 + 仓内实物） | 移植验收问法 |
|---|---|---|---|
| **F1** | **一切皆事件**：模型输出 / 工具调用 / 策略判定 / 压缩 / 审批全是事件；**`user.prompt` 与 `model.turn` 也入流**（对话内容进事件流，旁路观测，**不计** `Result.Events`） | 07 **F1**；`contracts/INVENTORY.md` §2 表首两行；`contracts/event.go`（`UserPromptPayload` / `ModelTurnPayload`）；`runtime/replay.go` `FoldConversation` | 新 harness 的事件流里能不能查到"这轮用户说了什么 + 模型答了什么"？ |
| **F5** | **当前态 = fold**：一切状态由事件流 fold 得出，**不另设可变状态源**（含 goal ledger 与黑板） | 07 **F5** / **G23**；`core/sessioncfg/`（config fold）；`runtime/indexer/`（ledger / board / cost 投影）；`contracts/INVENTORY.md` §2（`ledger.updated` / `board.written` 两行都写明"当前态 = 索引器投影"） | 有没有哪个"当前状态"是直接写进某个可变表、而不是从事件流 fold 出来的？ |
| **X1** | **三段写路径 + 落盘序铁律**：工具调用 = `tool.call.intent`（**durable 落盘**）→ 执行 → `tool.call.completed` / `tool.call.failed`（**durable 落盘**）。**有意图无结果 = 未知结果，不盲目重跑** | 07 **X1** / **X2**；`core/loop/toolcall.go`；`contracts/event.go:33`（intent payload 注释）；`contracts/INVENTORY.md` §2 尾段"写路径铁律（X1）" | intent 是不是**先落盘再执行**？崩溃后能不能只看事件流就知道"这次调用的结果未知"？ |
| **F7** | **契约只增不删、不改义**；弃用字段标 `x-deprecated`；envelope 带版本字段 `v` | 07 **F7**；`contracts/INVENTORY.md` §3「演进铁律」+ §5「契约变更记账」 | 新字段有没有走全链（schema → codegen → INVENTORY → 样本 → TS 三件套）？ |
| **G25** | **session 亲和写**：一个 session 的事件**只由其宿主进程 append**；跨进程接管必须先 `Claim`，冲突 **fail-close 拒绝** | 07 **G25**；`core/discovery/`（`Claim` / `HostOf` / `ErrHostConflict`）；`host/takeover.go`；`host/approval.go`（`host_conflict` → 409） | 两个进程会不会同时往一个 session 写事件？冲突时是否**唯一拒绝线**（不双写）？ |
| **G17b** | **活状态进程本地、无租约、无心跳**；session 生命周期 = 进程生命周期 | 07 **G17b**；`core/discovery/`（live 层 vs 事件折叠层）；`host/doc.go` | 活跃判定是不是**只**靠事件流的最后事件 ts（不靠租约）？重启后会不会"假装还在线"？ |
| **H12/H15/H18** | **凭据纪律**：凭据按作用域注入 + 日志脱敏；来源 = 进程 env + 用户级 TOML 文件（**env > 文件**）；**值永不进 argv / 事件 / 日志 / prompt**；事件只记 env **键名** | 07 **H12** / **H15** / **H18** / **H19** / **H21**；`core/env.go`（`ScrubEnv` / `IsCredentialKey` / `IsCredentialValue`）、`core/redact.go`、`core/loop/env.go`（`RequiredEnv` 裁决后注入）、`host/config_file.go`（`Redacted()`）、`cmd/alasol/config.go`（`host.ResolveCredential`） | 拿 grep 扫一遍：凭据值有没有可能出现在 argv、事件 payload、日志行里？ |
| **N5b** | **只监听 loopback**：非 loopback 地址直接拒绝；WS 校验 Origin（同源 / 非浏览器客户端放行，跨源 403 `origin_rejected`） | 07 **N5b** / **N4b**；`host/doc.go`（"只监听 loopback"段）；`runtime/transport/server.go` `checkOrigin`；`cmd/alasol/serve.go`（`-addr` 默认 `127.0.0.1:8720`） | 监听地址能不能被配成非 loopback？跨源网页能不能直连 WS / `/api/*`？ |
| **fail-close** | **fail-close 哲学**：无人应答 = **deny**（不得挂起）；审批缺席 / ctx 结束 / 裁决 panic / 无裁决器 → 一律 deny；nil 依赖按"缺失后仍不能安全放行吗"判：能拒绝就**兜底**、不能就 **panic** | 07 **D14** / **H7** / **H8**；`core/doc.go`（nil 语义三条）；`core/registry/registry.go` `adjudicate`（panic → deny）；`host/approval.go` `protocolApprover.Approve`（无人应答 → deny）；`contracts/INVENTORY.md` §2 尾段（"fail-close 铁律：无人应答 = deny，不得挂起"） | 任何一个"拿不准"的分支，默认动作是 allow 还是 deny？ |
| **D7** | **宽容读**：core 只对核心事件敏感，**未知 kind 忽略但持久化**（不丢事实、不猜语义） | 07 **D7**；`contracts/INVENTORY.md` §1 第 1 行（"未知 kind 宽容：忽略但持久化（D7）"） | 新 harness 收到不认识的 kind 时，是**跳过但存下来**，还是直接报错 / 丢弃？ |
| **G12/G25** | **一库一 hub 拓扑**：多个 hub 共享同一个事件库 = 读面串台、事件无 hub 归属；**预期拓扑 = 一库一 hub**，这是契约级限制 | 07 **G25**（口径段）+ **G12**；`host/doc.go:153`；`host/http.go`（`writeHubError` 里 `unknown_session` 文案点名"一库一 hub 是预期拓扑"） | 新 harness 是否明确声明"一库一 hub"？多 hub 共享库时会不会双写？ |
| **可见≠可执行** | **可见 ≠ 可执行**：工具可见集（类型白名单 ∩ session 白名单 − except − 被 deny 的）与执行许可（policy / perm）**两层分开**，任一层都不放宽另一层 | `core/loop/loop.go:1231` `visibleTools`（四道过滤 + 生命周期工具 + 框架工具）；`contracts/event.go:481`（`ToolsConfigChange` 注释）；`core/loop/tools_visibility_test.go:179` | 隐藏一个工具之后，直接点名调用它会不会仍然被执行？ |
| **半接比没有更坏** | **接线不全（半接）比不接更坏**：声明了却没接线的钩子必须 **fail-close 收口**（如 `agent.failed`），**不静默降级**成别的行为 | `host/config_file.go:359-362`（未实现的 `context_strategy.kind` → core fail-close 收口成 `agent.failed`，不静默降级成 append）；`cmd/alasol/multiagent.go:131-143`（run/resume 没接 `ChildStrategy` → fail-close）；`host/config_file.go:300`（`telemetry.mode="otlp"` 装配期拒，不静默降级） | 声明支持的特性是不是都真的接线了？没接的会不会**悄悄换个行为**？ |

---

## 2. 契约面（`contracts/`）

> **移植验收**：新 harness 的契约面必须与本节**逐字对齐**（字段名、kind 拼写、durable 列）。契约面是"跨进程 / 跨语言 / 跨时间流动"的唯一清单源（07 **D21**）。

### 2.1 Event envelope 字段全表（11 字段）

出处：`contracts/event.go`（`type Event struct`）+ `contracts/schema/event.json` `$defs.Event`。

| 字段 | JSON 名 | 类型 | 必填 | 语义（照抄契约口径） |
|---|---|---|---|---|
| 契约版本 | `v` | integer | 是 | envelope 契约版本（F7 只增）。 |
| 事件 id | `id` | string(ULID 26 位) | 是 | 框架事件 id，**幂等去重键**（S7：去重只看本字段）；schema 带 `pattern: ^[0-9A-HJKMNP-TV-Z]{26}$` + `x-id-kind: framework`。 |
| 事件种类 | `kind` | string | 是 | **开放字符串** + 点分命名空间（`tool.call.intent`）；core 定义核心子集（D6）。 |
| 全局序号 | `seq` | integer | 否（omitempty） | 由存储在 append 时分配（全局单调，AUTOINCREMENT，G9/G10）；`durable=false` 的流事件**不占 seq**，故可缺省（F8）。 |
| 时间戳 | `ts` | string(date-time) | 是 | RFC 3339。 |
| 会话 id | `session_id` | string(ULID) | 是 | envelope 强制要求（hub 级伪 session 由宿主按事件库路径确定性派生一对伪 id）。 |
| agent id | `agent_id` | string(ULID) | 是 | 同上。 |
| 因果指针 | `causation_id` | string(ULID) | 否 | 触发本事件的那个事件 id（扁平因果，**不引入 span 模型**，F2）。 |
| 事实标记 | `durable` | bool | 是 | `true` = 入库、占 seq、可回放；`false` = 总线直发订阅者、**不入库、不占 seq、不回放**（F8）。 |
| 载荷 | `payload` | object(raw JSON) | 否 | 按 kind 解释；已知 kind 的形状见 `schema/event.json` 的 `$defs`。 |
| 二进制引用 | `binary_ref` | string(ULID) | 否 | 大 payload 外置 blob 引用（D4）；挂接点唯一 = `BlobMeta.Ref()`。 |

**id 纪律**（`contracts/INVENTORY.md` §3、07 S 系）：JSON 里 id 一律 string；序号用 integer；provider 的 `tool_call_id` = 不透明 string（`x-id-kind: provider`），**与 `Event.id` 永不共字段**；**id 不做修复**——字符集/长度不合则**拒绝并记事件**（S9）。

### 2.2 事件 kind 全表（28 条 · 契约基线）

出处：`contracts/INVENTORY.md` §2（表）+ `contracts/schema/event.json` `$defs.Event.x-kind-payload`（28 条，与表**逐条一致**）。durable 列 = 该 kind 是否入库可回放。

| kind | durable | payload `$defs` | 要点（照抄 INVENTORY §2，压缩自原文） |
|---|---|---|---|
| `user.prompt` | true | `UserPromptPayload` | **对话入流**（F1）：用户提示原文；Run 开始落（旁路观测，不计 `Result.Events`）。 |
| `model.turn` | true | `ModelTurnPayload` | **对话入流**（F1）：模型回复 + `ModelTurnUsage`（usage **只进成本统计**，J21，不回填快照）+ 可选 `reasoning`（G14）。 |
| `model.delta` | **false** | `ModelDeltaPayload` | **流式增量片段**（打字机效果）：`turn` + 可选 `run_id` + `text`/`reasoning` 片段。**不入库、不占 seq、不回放**（F8/陷阱 11）；`durable_only=true` 的订阅收不到。归组口径 = `(run_id, turn)`；`run_id` 缺省时退回"单 session 串行"近似切分。 |
| `tool.call.intent` | true | `ToolCallIntentPayload` | **三段事件之①**（X1）：tool_call_id / 参数 / idempotent / replay 标记 / 外部可核验锚点 / `policy_snapshot`（M9 判定快照）。 |
| `tool.call.completed` | true | `ToolCallCompletedPayload` | **三段事件之③a**（X1）：结果（Tool 返回值契约）。 |
| `tool.call.failed` | true | `ToolCallFailedPayload` | **三段事件之③b**（X1）：失败（错误为**普通事件**，M1/M8）。 |
| `ledger.updated` | true | `LedgerUpdatedPayload` | goal ledger **全量快照**：goals / constraints / completed / pending 四张清单**全文**；整帧 LWW（seq 大者胜）；当前态 = 索引器投影（ledger 不是独立可变存储）。 |
| `board.written` | true | `BoardWrittenPayload` | 共享黑板键写入：键 + 原始 JSON 值 + 作者；`tombstone=true` = 删除（**墓碑行**：保留行、值为 NULL、读取时视为不存在）。 |
| `agent.spawned` | true | `AgentSpawnedPayload` | subagent 诞生（`spawn_request_id` 幂等键 X5、`role` K13）。 |
| `agent.completed` | true | `AgentCompletedPayload` | subagent 收敛：结构化 summary 是回主 agent 的**唯一通道**（K24；taint 继承 Q7）。 |
| `agent.failed` | true | `AgentFailedPayload` | subagent 失败（异常向上冒泡 K23）。 |
| `gate.checked` | true | `GateCheckedPayload` | CI 门禁检查结果（receipt 四要素校验 V2；`elapsed_ms` V5）。 |
| `context.assembled` | true | `ContextSnapshot` | 每轮模型调用前的上下文快照（G12）；载荷 = ContextSnapshot 契约（`parts[]`，`state ∈ verbatim/compacted/dropped/deferred`）。 |
| `checkpoint.created` | true | `CheckpointCreatedPayload` | durable checkpoint 四要素（X3）。 |
| `session.resumed` | true | `SessionResumedPayload` | resume 流程完成标记（X4）。 |
| `session.resume_claimed` | true | `ResumeClaimedPayload` | in-flight 原子认领（X5 防并发重放）。 |
| `gate.joined` | true | `GateJoinedPayload` | session 加入 gate（K12）：`gate_id` + `source`（auto_repo / auto_workflow / explicit）。 |
| `gate.left` | true | `GateLeftPayload` | session **显式离开** gate（G15）：`gate_id` + `source`；幂等（已不在网内的重复 leave 不落事件）。成员折叠 = joined 减 left（同 session+gate，按 seq 序折叠，乱序 Apply 稳定）。 |
| `agentlink.broadcast` | true | `AgentLinkBroadcastPayload` | 广播消息（K18）：`target`（gate/subtree）+ `text`；拉模型投递，收件方 = 目标 ∩ 发送方可见集（K11/K27）。 |
| `agentlink.handoff` | true | `AgentLinkHandoffPayload` | 结构化交接（K18）：broadcast + 开放结构 `payload`（工作流自定义，框架不解释）。 |
| `context.compacted` | true | `ContextCompactedPayload` | 一次压缩动作的事实记录：两级 L1/L2 + 兜底三级 + 留存降级（full→l1→l2，**永不回升**）；`action` = downgrade/split/budget_warning/fallback；`tier` = full/l1/l2；`seq_from`/`seq_to` = 被压原文机器区间；`ref` = `stage:N@e:from..e:to`；`usage`/`model` = 压缩模型计量。**refs 永不指摘要**；压缩输入 = goal ledger + 被压区间全文。 |
| `review.recorded` | true | `ReviewRecordedPayload` | 一条分支 review 结论：`branch` + `tip_commit`（失效判定基准）+ `conclusion`（approved/changes_requested）+ 可选 `note`；tip 变 → 结论失效 → 要求重审。**reviewer 身份不进 payload**（V9）。 |
| `conflict.predicted` | true | `ConflictPredictedPayload` | 合并后冲突预测：`merged_branch` + `available` + `collisions` + 可选 `unevaluated` + `risks[]`（`basis` 取值 `direct` / `one-side`，**非对称判据**）；**诚实降级**（N2）：不可评估时 `available=false` + `unavailable_reason`；`available=true` 时不存在 `risks[].available=false`。 |
| `hub.alert` | true | `HubAlertPayload` | hub 级资源监控的一条超阈值告警：`metric`（cpu/mem/goroutines）+ `value` + `threshold` + `message`；**归属伪 session**（按事件库路径确定性派生 id，跨重启稳定）；payload **只记结论**，不含会话内容/凭据。 |
| `btw.forked` | true | `BtwForkedPayload` | 一次 `/btw` 旁支 fork 的事实记录：`source_session_id` + `question`；**只附带上下文**（对话历史，带条数/长度上限 + 截断告知）、**不复制 ledger/gate**；envelope 归属 fork session → 主 session **零新增**；预埋 `BtwPromote` 钩子（只埋点不实现）。 |
| `workflow.activated` | true | `WorkflowActivatedPayload` | 一个 session 激活一个工作流能力包：`workflow_id` + 可选 `command`（**实际敲出来的** slash 命令名）+ `philosophy` 原文；能力包**不是流程、非强制**（W1/W2）。 |
| `workflow.milestone` | true | `WorkflowMilestonePayload` | 一次工作流里程碑被处理：`workflow_id` + `milestone`（开放字符串）+ `passed` + 可选 `note`；**框架不强制序列**（W2/W21）。 |
| `config.changed` | true | `ConfigChangedPayload` | per-prompt 配置变更的事实记录：`model` / `reasoning_effort` / `provider{base_url, api_key_env}` / `tools{only, except}` / `context{kind, window, l1_pct, l2_pct, rules_budget, common_sense_budget}`；**当前配置 = 事件 fold**（逐字段 LWW）；envelope 归属被改配置的 session。 |

**kind 表的纪律**（移植时最容易破的三处）：
1. **kind 是开放字符串**（D6）——不是封闭枚举；新 harness 收到未知 kind 要**跳过但持久化**（D7）。
2. **durable 列是契约的一部分**——`model.delta` 是唯一的 `false`（其余 27 条全 `true`）。
3. **审批不是事件 kind**——审批是实时交互，走 WS 控制帧（见 §3.4），**不进 durable 事件流**。

### 2.3 契约文件清单

| # | 契约（`contracts/INVENTORY.md` §1） | schema 文件 | Go 生成物 | 关键约束 |
|---|---|---|---|---|
| 1 | Event | `schema/event.json` | `contracts/event.go` | F3/F7/F8；kind 开放字符串；未知 kind 宽容（D7） |
| 2 | Command | `schema/command.json` | `contracts/command.go` | 意图；可从进程外提交（D9） |
| 3 | Transport 消息 | `schema/transport.json` | `contracts/transport.go` | `ws+ndjson` / `stdio` / `inproc` 同一契约；NDJSON 行式 |
| 4 | Extension 元数据 | `schema/extension.json` | `contracts/extension.go` | `kind` / `schema` / `capabilities` / `priority` / `reloadable` / `health`（D17） |
| 5 | Tool.schema | `schema/tool.json` | `contracts/tool.go` | `schema` 字段**值本身是 JSON Schema**（元嵌套）→ codegen 当原始 JSON 处理 |
| 6 | Tool 返回值 | `schema/tool-result.json` | `contracts/tool_result.go` | `graph_edges` / `taint` / 可重放标记 |
| 7 | Policy 规则 | `schema/policy.json` | `contracts/policy.go` | `Decision` = allow/deny/ask；ask 带 `suggested_rules`；可选 `code`（`DecisionCode` 十码）；workflow 侧规则**只可收窄** |
| 8 | Store 持久化格式 | `schema/store.json` | `contracts/store.go` | NDJSON 事件记录；大 payload 走 `binary_ref`（D4） |
| 9 | Workflow | `schema/workflow.json` | `contracts/workflow.go` | 纯数据；`grants` / `permission_request` 是**请求**，实际 = 请求 ∩ 策略上限 ∩ 父级授予 |
| 10 | ContextSnapshot | `schema/context-snapshot.json` | `contracts/context_snapshot.go` | 每轮一条 durable 事件 `context.assembled` |
| 11 | Approval 消息 | `schema/approval.json` | `contracts/approval.go` | **无人应答 = deny，不得挂起**（D14/H8） |
| — | 共享原语 | `schema/common.json` | `contracts/common.go` | `Ulid` / `ProviderId` / `ReplayKind` / `SideEffect`（一个类型只定义一次） |

**生成与门禁链**（移植时必须重建的一条链）：

| 环节 | 实物 | 纪律 |
|---|---|---|
| 手写 schema | `contracts/schema/*.json`（12 份 = 11 契约 + `common.json`） | JSON Schema **2020-12**（`$defs`），各文件自带 `$id`（`https://github.com/Yorha9e/alasol/contracts/schema/<file>.json`），跨文件 `$ref` 非循环。 |
| codegen | `internal/codegen/` + `cmd/codegen/`（**自写受限子集**：object/array/string/number/bool/enum/required/optional/nested/非循环 `$ref`） | 生成物入 `contracts/*.go`；`contracts/doc.go` 是手写的、不归 codegen 管。 |
| **CI 门禁** | `cmd/codegen -check` | ① 再生成 diff 为空；② **查孤儿生成物**（schema 改名/删除后残留的生成物必须报错）；手写文件永不被动。 |
| 可选字段 | codegen 全链 | 一律 **指针 + `json:",omitempty"`**（D2）；例外：`json.RawMessage` 保持裸类型（nil 即缺省）。 |
| TS 消费端（三件套） | `tools/ts-consumer/reader.ts` + `package.json` + `tsconfig.json` | ajv 2020-12、**`strict:false`**（`x-` 注记是自定义关键字）；kind→payload 映射**读 `event.json` 的 `x-kind-payload`**（不硬编码表）；命名空间从各 schema 的 `$id` 反推。 |
| 样本（4 份） | `tools/ts-consumer/sample-events.ndjson` / `sample-transport.ndjson` / `broken-events.ndjson` / `broken-transport.ndjson` | 好样本必须过 TS 校验；**坏样本必须被拒**（CI 硬门槛）。 |

**`x-` 注记分两类**（`contracts/INVENTORY.md` §3）：codegen 消费（`x-go-type`）vs codegen 忽略的纯注记（`x-id-kind` / `x-invariants` / `x-kind-payload` / `x-deprecated`）。

### 2.4 transport 面帧形状（WS + NDJSON / stdio / inproc 同一契约）

envelope（`contracts/transport.go` `TransportMessage`）：`v` / `id` / `kind` / `reply_to?` / `session_id?` / `payload?`。
**`id` / `reply_to` 是客户端关联 id**（G9）：schema 形状 = `string`（**不限格式**），服务端**只回指、不校验、不去重**；**不是**框架 id（不标 `x-id-kind`、不套 ULID pattern）。

| kind | 方向 | 载荷 | 说明（`contracts/INVENTORY.md` §2b） |
|---|---|---|---|
| `subscribe` | client→server | `SubscribeRequest{session_id?, kind_prefixes?, durable_only?}` | 订阅事件流（K4）；**重复 subscribe = 替换**。过滤三项 AND 语义，全空 = 全收。 |
| `subscribed` | server→client | `SubscribeAck{session_id?, kind_prefixes?, durable_only?, replaced}` | **订阅生效回执**（G8）：回显**生效的**过滤条件 + `replaced`；`reply_to` 指向原 subscribe。 |
| `event` | server→client | `Event` | 事件推送（按订阅过滤）；**客户端发 = `forbidden_kind`**。 |
| `ping` / `pong` | client→server / server→client | — | 心跳（pong 的 `reply_to` = ping.id）。 |
| `command.submit` | client→server | `CommandSubmit{session_id?, prompt, config?, profile?}` | **对话发起**（G1）：服务端异步跑、结果经事件流回流。`config` 槽位 = per-prompt 配置变更集；`profile` 槽位 = 命名 profile（**先展开 profile、再让 config 逐字段压过**，合成一条 `config.changed`；未知 profile 名 → 拒绝）。 |
| `command.accepted` | server→client | `CommandAccepted{session_id, status}` | 受理回执（`status=running`）；`reply_to` 指向原 submit。**受理 ≠ 跑完**。 |
| `approval.request` | server→client | `ApprovalRequest`（`approval.json`） | 审批请求（ask 策略触发时广播给所有连接）；载荷四要素 + trigger / tool_call_id / side_effect / summary / detail。 |
| `approval.respond` | client→server | `ApprovalResponse`（`approval.json`） | 审批应答：按 `payload.request_id` 注入挂起的审批；**无人应答/超时 = deny**（fail-close）。 |
| `error` | server→client | `TransportError{code, message}` | 协议层错误；`reply_to` 指向出错消息。 |

---

## 3. HTTP/WS 面

> 出处：`host/http.go`（路由注册面 `reg(...)` 助手，**唯一路由表**）+ 各 `host/serve*.go` 文件。
> 移植验收：新 harness 的端点集合与本文**逐条对齐**（方法 + 路径 + 关键错误码）。

### 3.1 全端点表（48 条 = 45 条 `/api/*` + 3 条非 API）

| # | 方法 | 路径 | 一句话 | 关键错误码 |
|---|---|---|---|---|
| 1 | GET | `/` | Web UI 单页（`ui/web` 嵌入的 `index.html`） | 500 `static_missing` |
| 2 | GET | `/index.html` | 同上 | 500 `static_missing` |
| 3 | GET | `/ws` | **WS 事件流**（NDJSON 行式；`transport.Server.ServeWS`） | 403 `origin_rejected`（跨源 Origin） |
| 4 | GET | `/api/health` | `{"ok","watermark","sessions"}`（watermark = **索引器水位**，与 `/api/events` 的事实流 tip 不同口径） | 500 `health_failed` |
| 5 | GET | `/api/monitor` | K9 监控视图（当前采样 + 阈值 + 告警环） | 500 `monitor_unavailable` |
| 6 | GET | `/api/git` | hub 工作目录的 git 快照（branch/dirty/last_commit）；`?path=` 覆盖采样目录；**HTTP-only 不进事件流** | 非仓库 → `repo=false` + error（**不是** 404/500） |
| 7 | GET | `/api/extensions` | 扩展注册表**只读**快照（全 kind + Health；空能力列表归一为 `[]`） | — |
| 8 | POST | `/api/extensions/{name}/enable` | E3 能力裁决后启用 | 200 / 403（能力未获准）/ 404 `extension_not_found` / 409（`ErrStale`）/ 400 `bad_extension_name` |
| 9 | POST | `/api/extensions/{name}/disable` | 软停用（幂等，可再 enable） | 200 / 404 `extension_not_found` / 400 `bad_extension_name` / 500 `extension_disable_failed` |
| 10 | GET | `/api/sessions` | 在线表（最多 `MaxSessionRows` 行；`hostable` / `host_hub` / `online` / `running` / `last_activity` / `waiting`）；`?online=true\|false\|1\|0` | 400 `bad_online`（**显式给了空值也 400**）/ 500 `sessions_failed` |
| 11 | POST | `/api/sessions` | 开新会话（201 `{"session_id"}`；**不自动跑 prompt**）；体 `{"cwd","run_id","worktree","profile"}` | 400 `bad_body` / 400 `session_create_failed` / 400 `bad_profile` / 500 `session_host_failed` |
| 12 | GET | `/api/cost` | 跨 session token 成本汇总（J21） | 500 `cost_failed` |
| 13 | GET | `/api/cost/{session_id}` | 单 session token 成本（按模型分组） | 400 `bad_session_id` / 404 `no_cost` / 500 `cost_failed` |
| 14 | GET | `/api/context/{session_id}` | 最新一条 `context.assembled` 快照 | 400 `bad_session_id` / 404 `no_context_snapshot` / 500 `context_failed` |
| 15 | GET | `/api/peers` | K11 水平可见性（gate 相交 ∩ 活跃的对端主 agent）；`?session_id=` | 400 `bad_session_id` / 404 `unknown_session` / 403 `not_visible` |
| 16 | POST | `/api/gates/join` | 显式 join（幂等）；体 `{"session_id","gate_id"}` | 400 `bad_body` / `bad_session_id` / `bad_gate_id` / 404 `unknown_session` |
| 17 | POST | `/api/gates/leave` | 显式 leave（幂等，**重复 leave 不落事件**）；体同 join | 同上 |
| 18 | GET | `/api/gates` | gate 枚举（存在性 + 活跃成员数；带 `session_id` 时额外给成员详情） | 400 `bad_session_id` / 500 `hub_failed` |
| 19 | GET | `/api/graph/impact` | G2f 可选投影：change_impact 影响面；`?roots=&kind=&depth=&limit=&core=` | 503 `graph_disabled`（未启用）/ 400 `bad_roots`/`bad_depth`/`bad_limit` / 500 `impact_failed` |
| 20 | POST | `/api/graph/rebuild` | 图投影重建（N2 运维入口） | 500 `rebuild_failed` |
| 21 | GET | `/api/blobs/{id}` | 取回被外置的完整 result（D4 读面；流式） | 400 `bad_blob_id`（文案**点名 blob id**）/ 404 `blob_disabled`/`blob_not_found` / 500 `blob_stat_failed`/`blob_read_failed` |
| 22 | GET | `/api/vectors/search` | **条件注册**（`Config.VectorsEnabled=true` 才注册）；`?q=&k=&filter=` | 404 `vectors_disabled` / 503 `embedding_provider_unconfigured`/`vector_rebuilding`/`upstream_embedding_unavailable` / 400 `bad_query`/`bad_k`/`bad_filter` / 502 `upstream_embedding_rejected` / 500 `vector_failed` |
| 23 | POST | `/api/vectors` | **条件注册**：手动 ingest；体 `{"texts","source_refs"}`（单批 ≤ 512 条） | 同上 + 400 `bad_body`/`empty_texts`/`too_many_texts`/`bad_source_refs` |
| 24 | GET | `/api/docs/{collection}/{key}` | **条件注册**（`Config.DocumentsEnabled=true`）：取单条 Doc | 404 `documents_disabled`/`doc_not_found` / 500 `doc_failed` |
| 25 | GET | `/api/docs` | **条件注册**：列文档（`?collection=&prefix=&limit=`；按 (collection,key) 字节序） | 400 `bad_doc_query` / 404 `documents_disabled` / 500 `doc_failed` |
| 26 | POST | `/api/docs/upsert` | **条件注册**：整批 upsert（版本冲突 → 整批拒） | 409 `doc_conflict` / 500 `doc_failed` |
| 27 | POST | `/api/docs/delete` | **条件注册**：删除（幂等） | 500 `doc_failed` |
| 28 | POST | `/api/merge/review` | 落一条 review 结论（V6）；体 `{"session_id","agent_id","branch","tip_commit","conclusion"}` | 400 `bad_session`/`bad_agent`/`bad_review`/`bad_conclusion` / 500 `review_failed` |
| 29 | POST | `/api/merge/check` | 合并前查放行；体 `{"session_id","branch","current_tip"}` | 400 `bad_session`/`bad_branch`/`bad_tip` / 500 `merge_check_failed` |
| 30 | POST | `/api/merge/predict` | 合并后冲突预测；体 `{"session_id","agent_id","kind","merged","others"}` | 400 `bad_session`/`bad_agent`/`bad_merged` / 500 `predict_failed` |
| 31 | GET | `/api/worktrees` | 已登记 worktree 列表（按 name 排序） | — |
| 32 | POST | `/api/worktrees` | 建一个；体 `{"repo","name"}` | 400 `bad_body` / 400 `worktree_create_failed` |
| 33 | DELETE | `/api/worktrees/{name}` | junction-safe 清理 → `{"removed":name}` | 400 `worktree_remove_failed` |
| 34 | GET | `/api/online-table/config` | 当前生效的 N / 每行 token 预算 / 暴露等级 / 窗口行数 / 活跃窗口 | 500 `hub_failed` |
| 35 | POST | `/api/online-table/config` | 部分更新（**运行期可配、不重启**，下一轮生效）；体 `{max_rows, line_token_budget, level, window_lines, activity_window}`（缺省字段不变） | 400 `bad_body` / 400 `bad_online_table_config` |
| 36 | GET | `/api/presence` | agentlink Presence（谁在线 / 在干什么 / 属于哪些 gate）；`?session_id=` | 400 `bad_session_id` / 404 `unknown_session` |
| 37 | GET | `/api/discover` | agentlink Discover（按 gate / 按 agent 树）；`?session_id=&gate_id=&agent_id=&subtree=` | 400 `bad_session_id` / `bad_gate_id` / `bad_agent` / 404 `unknown_target` |
| 38 | GET | `/api/inquiry` | agentlink Inquiry（结构化摘要 + 事件窗口 + refs 核验）；`?session_id=&agent_id=&from=&to=&limit=` | 400 `bad_session_id`/`bad_agent`/`bad_from`/`bad_to`/`bad_limit` / 404 `unknown_target` |
| 39 | GET | `/api/inbox` | agentlink Inbox（消息 + 游标水位 + has_more）；`?session_id=&since=&limit=` | 400 `bad_session_id`/`bad_since`/`bad_limit` / 404 `unknown_target` |
| 40 | POST | `/api/agentlink/broadcast` | 落一条 durable `agentlink.broadcast`（拉模型投递） | 400 `bad_body`/`bad_identity`/`bad_target`/`bad_payload`/`empty_text`/`cross_session_subtree` / 403 `not_visible` |
| 41 | POST | `/api/agentlink/handoff` | 落一条 durable `agentlink.handoff`（broadcast + 结构化 payload） | 同上 |
| 42 | GET | `/api/events` | 事件回放（G3）：`?from=&to=&limit=&session_id=` → `{events, watermark, from, to, truncated}` | 400 `bad_from`/`bad_to`/`bad_limit`/`bad_session_id` / 500 `events_failed` |
| 43 | POST | `/api/sessions/{session_id}/prompt` | **对话发起**（G1）；体 `{"prompt","config"?,"profile"?}`；202 `{"session_id","status":"running"}` | 400 `bad_session_id`/`bad_body`/`empty_prompt`/`bad_config`/`bad_profile` / 409 `host_conflict` / 500 `takeover_failed`/`config_write_failed` / 503 `hub_closed` |
| 44 | POST | `/api/approvals/{request_id}/respond` | 注入挂起的审批应答 | 400 `bad_request_id`/`bad_decision` / 404 `no_pending_approval` |
| 45 | GET | `/api/approvals/pending` | 挂起审批枚举面（G11；空表 → `[]`） | — |
| 46 | GET | `/api/sessions/{session_id}/agents` | agent 树只读快照（`agent_id`/`parent_agent_id`/`depth`/`agent_type`/`role`/`status` + summary） | 404 `unknown_session`（既没树也不由本进程托管） |
| 47 | GET | `/api/profiles` | 命名 profile 表（按 name 升序；空表 → `{"profiles":[]}`；**只出 `api_key_env` 变量名，绝不含 key 值**） | — |
| 48 | GET | `/api/sessions/{session_id}/config` | 该 session 当前**生效**配置（嵌套 `config` 组 + 平铺五项 + `overridden_fields`） | 404 `unknown_session`（事件流里没见过）/ 500 `session_config_failed` |

**路由面纪律**：
- **第 1–3 行不走 `reg`**：它们用 `mux.HandleFunc` 直接注册（静态面与 WS 面不进"路径→方法"表，故不受 405 口径管）；第 4 行起的 45 条 `/api/*` **全部经 `reg(...)`**。
- `reg(...)` 是**唯一路由表**：同一个助手同时写 `mux` 与"路径→方法"表（`apiTable`），**不另抄一份**（D21：一处定义，避免"注册了但忘了记方法"的漂移）。
- `/api/*` 未命中具体路由 → **路径认得但方法不符 = 405**（带 `Allow` 头）；**路径不认得 = 404**；两者一律 JSON 错误体（**不再**掉进静态通配报"没有这个静态资源"，G7）。
- 非 `/api` 的未知静态路径 → JSON 404 `not_found`。
- **条件注册的端点**（`/api/vectors*`、`/api/docs*`）在未启用时**路由根本不注册** → 路径认不得 → 404（与 blob 同口径 = 零行为变化）。
- **故意不暴露**的读面：发现表的其余查询面（跨 session 的 agent 清单是宿主内部视图）。

### 3.2 错误体形状与错误码族

**错误体形状（照抄实现，与常见直觉不同，移植时别写错）**：

```json
{"code":"not_found","message":"没有这个 API 端点: /api/nope"}
```

- HTTP 与 WS 的 `error` 帧**同形状**：`contracts.TransportError{Code string `json:"code"`, Message string `json:"message"`}`（`contracts/transport.go`）。
- **是平铺的 `code`/`message`，不是 `{"error":{"code":…,"message":…}}` 的嵌套形状**（`host/http.go` `writeJSONError` → `writeJSONBody(w, code, contracts.TransportError{…})`）。
- WS 的 `error` 帧把这对字段放在 `payload` 里：`{"kind":"error","payload":{"code":…,"message":…}}`（`ui/web/index.html` 按 `m.kind === "error"` 读 `p.code` / `p.message`）。

**错误码族全表**（`host/` 非测试文件的 `writeJSONError` 全量提取，按状态码归类）：

| 状态 | 码 |
|---|---|
| 400 | `bad_agent` `bad_as_session` `bad_blob_id` `bad_body` `bad_branch` `bad_conclusion` `bad_config` `bad_decision` `bad_depth` `bad_doc_query` `bad_extension_name` `bad_filter` `bad_from` `bad_gate_id` `bad_identity` `bad_k` `bad_limit` `bad_merged` `bad_online` `bad_online_table_config` `bad_payload` `bad_profile` `bad_query` `bad_request_id` `bad_review` `bad_roots` `bad_session` `bad_session_id` `bad_since` `bad_source_refs` `bad_target` `bad_tip` `bad_to` `bad_vector_query` `cross_session_subtree` `empty_prompt` `empty_text` `empty_texts` `session_create_failed` `too_many_texts` `worktree_create_failed` `worktree_remove_failed` |
| 403 | `not_visible`（K11 挡住 = "你不该看这个"，**不是**"参数错了"） |
| 404 | `blob_disabled` `blob_not_found` `doc_not_found` `documents_disabled` `extension_not_found` `no_context_snapshot` `no_cost` `no_pending_approval` `not_found` `unknown_session` `unknown_target` `vectors_disabled` |
| 405 | `method_not_allowed`（带 `Allow` 头） |
| 409 | `doc_conflict` `host_conflict`（**Claim 冲突 = 唯一拒绝线**，G25） |
| 500 | `blob_read_failed` `blob_stat_failed` `config_write_failed` `context_failed` `cost_failed` `doc_failed` `encode_failed` `events_failed` `extension_disable_failed` `health_failed` `hub_failed` `hub_nil` `impact_failed` `merge_check_failed` `monitor_unavailable` `predict_failed` `rebuild_failed` `review_failed` `session_config_failed` `session_host_failed` `sessions_failed` `static_missing` `takeover_failed` `vector_failed` |
| 502 | `upstream_embedding_rejected`（上游 4xx 拒了我们的请求） |
| 503 | `embedding_provider_unconfigured` `graph_disabled` `hub_closed` `upstream_embedding_unavailable`（上游 5xx/429） `vector_rebuilding` |

**错误映射的分层口径**（`host/http.go` `writeHubError`）：400 = 参数/身份/目标不合法（客户端能改）；403 = 可见性拒绝；404 = 目标不存在或不受托管；500 = 其余。**上游错误按状态码分流**（上游 4xx → 502、5xx/429 → 503），**不一律 500**——"宿主 500"会把排查方向指错。

### 3.3 WS 协议

| 事项 | 口径 | 出处 |
|---|---|---|
| 帧格式 | **NDJSON 行式**，一行一帧；`kind` 判别 | `runtime/transport/`；`contracts/transport.json` |
| 订阅过滤 | `SubscribeRequest{session_id?, kind_prefixes?, durable_only?}`，三项 **AND** 语义，**全空 = 全收**；`kind_prefixes` 是**前缀匹配**（如 `tool.call.`） | `runtime/transport/server.go` `subscribeAckFrom`；`core/bus` `FilterFromRequest` |
| 订阅回执 | `subscribed` 回显**生效的**过滤条件 + `replaced`（重复 subscribe = 替换） | `contracts/transport.go` `SubscribeAck` |
| 广播不走过滤 | `approval.request` 广播给**所有**连接（不过订阅过滤） | `runtime/transport/doc.go` |
| **at-least-once 口径** | ① WS 断线即丢（订阅者通道满会丢，`bus.Lagging`）；② 新连接**重发未决审批**（客户端按 `request_id` 去重）；③ 补读面 = `GET /api/events`（按 seq），**只含 durable 事件**（流事件丢了就是丢了） | `host/approval.go` `ResendPending`；`host/events.go`；07 **F8** / **G3** |
| 回放可见性 | `/api/events` 是**全局事实流**的补读面，**不做 K11 过滤**（原始事件日志面）；要 K11 过滤 + 跨 session 摘要核验用 `/api/inquiry` | `host/events.go` 文件头 |
| Origin 校验 | 无 `Origin` 头 = 非浏览器客户端 → 放行；同源 → 放行；**loopback 互认**（三元比对）→ 放行；其余 → 403 `origin_rejected` | `runtime/transport/server.go` `checkOrigin` |
| 客户端 id | `TransportMessage.id` / `reply_to` = **客户端关联 id**，不限格式；服务端**只回指、不校验、不去重** | 07 **G9**；`contracts/INVENTORY.md` §2b 末 |

### 3.4 CORS（opt-in 白名单）

| 事项 | 口径 | 出处 |
|---|---|---|
| 开关 | **只走文件配置面**（TOML 顶层 `cors_origins`），**不给 flag**（命令行泄进 shell 历史 / ps） | `host/config_file.go:125-138` |
| 缺省 | **空 = 关 = 零行为变化**：一个 CORS 头都不发，OPTIONS 照旧 405/404 | `host/cors.go` 文件头 |
| 粒度 | **精确 origin**（scheme+host+port 齐全、无路径、无尾斜杠）；**不支持通配 `*`**（写了**启动即拒**） | `host/config_file.go`；`host/config.go` `ValidateCORSOrigins` |
| 作用范围 | 只对 **`/api/*`**（含未命中路由的 404）；**静态面（GET /、/index.html）与 /ws 不动** | `host/cors.go` 文件头 |
| 方法/头 | 放行 `GET, POST, OPTIONS` + 请求头 `Content-Type`；其余不放行 | `host/cors.go` `corsAllowMethods` / `corsAllowHeaders` |
| 响应头 | 命中名单 → `Access-Control-Allow-Origin: <Origin 原样>` + `Vary: Origin`；未命中/无 Origin → 什么都不加 | `host/doc.go` |

---

## 4. 运行面（一轮 prompt 的一生）

> 出处：`host/hub.go` `RunPrompt` / `host/approval.go` `runPromptAsync` + `core/loop/`。

### 4.1 一轮 prompt 的九步

| 步 | 做什么 | 关键口径 | 出处 |
|---|---|---|---|
| 1 | **托管判定**（懒接管） | 已托管 → 照常；未托管 → `loopFor` **懒接管**（`Claim` + 建 loop + 从事件流恢复主 agent id）；**Claim 冲突 = 唯一拒绝线**（HTTP 409 `host_conflict` / WS `error` 帧）；接管成功落一条 `hub.alert`（`metric=session.takeover`）留痕 | 07 **G25**；`host/takeover.go`；`host/approval.go`（`servePrompt`） |
| 2 | **配置写路径**（若有 `config`/`profile`） | 顺序 = 托管 → 校验（fail-close）→ 落 `config.changed` → 起跑；落事件失败 → 500 `config_write_failed`（**不带未持久化的配置开跑**）；未知名 profile → 400 `bad_profile`（**零事件**） | `host/approval.go`（`servePrompt` 文件头）；`host/config_fold.go` `expandProfile` |
| 3 | **历史折叠** | `RunPrompt` 每轮**折叠事件流带历史**（多轮对话同源）：`runtime.FoldConversation` → `RunWithHistory`；fold **按主 agent 视角过滤**（子 agent 产出经 spawn 的 `tool.call.completed` 回灌，不以 user/assistant 混入） | 07 **F1**（落地回填）；`host/hub.go:943`；`runtime/replay.go` |
| 4 | **生效链** | `hub 启动默认（Config）` → `session 配置（config.changed 折叠）` → `本轮快照（Run 开始时冻结；运行中变更下一轮生效）` | `15` §3.3；`core/sessioncfg/` |
| 5 | **turns** | 每轮 = 一次 `provider.Run`；`MaxTurns` 兜底（`0` = 默认 20）；turn 内：`context.assembled` 快照 → 模型调用 → 工具三段 → 下一轮 | `core/loop/loop.go` |
| 6 | **`model.delta` 流** | **不落库口径**：`durable=false` → 总线直发订阅者、**不入库、不占 seq、不回放**；WS 订阅默认收得到，`durable_only=true` 收不到；归组按 `(run_id, turn)` | 07 **F8**；`contracts/INVENTORY.md` §2 |
| 7 | **工具三段** | `tool.call.intent`（durable 落盘）→ 执行 → `tool.call.completed` / `tool.call.failed`（durable 落盘）；`policy_snapshot` 记进 intent（M9） | 07 **X1**；`core/loop/toolcall.go` |
| 8 | **审批** | ask 策略触发 → 协议面 `approval.request` 广播 → `approval.respond` 应答；**四档模式**（见 §4.2）；**无人应答 = deny**（不得挂起） | 07 **D14** / **H8**；`host/approval.go` |
| 9 | **预算闸门 + 重试 + turn budget** | W15 两层闸门（见 §4.3）+ M7 幂等才可重试（见 §4.4）+ `TurnBudget`（见 §4.5） | `host/budget.go`；`core/retry/`；`core/loop/loop.go` |

### 4.2 审批四档模式

| 模式（`-approval`） | 行为 | 出处 |
|---|---|---|
| `deny-all`（**默认**） | 一律拒绝（`core.DenyAll`）；fail-close | `host/hub.go:469`；`cmd/alasol/serve.go:105` |
| `always-allow` | 一律放行（`core.AlwaysAllow`） | `host/hub.go:463` |
| `terminal` | 终端里问人（`runtime/approver/terminal`） | `host/hub.go:465` |
| `protocol` | **经 WS/HTTP 面应答**（`protocolApprover`：广播 `approval.request` + `approval.respond` 应答）；**无人应答/超时 = deny** | `host/hub.go:467`；`host/approval.go` `protocolApprover` |

- **ACP 模式忽略本 flag**：审批恒走 ACP 的 `session/request_permission`，无人应答 = deny。
- 审批**不是事件 kind**（不进 durable 流）：审批是实时交互，回放一条几天前的 approval.request 没有意义。
- **成因码**（D13/D14 落地）：`Decision.code` 填齐 `approver_absent` / `approval_context_ended` / `approval_context_incomplete` / `approval_failed` / `approval_denied` / `approval_allowed`；`isNoAnswerDeny` **优先按 code** 判，code 缺省才退回文案匹配。
- **超时 deny 后 intent + failed 成对留痕**（不是零留痕断链）。

### 4.3 预算闸门（W15 两层）

| 层 | 位置 | 口径 |
|---|---|---|
| **per-Run** | `host/budget.go` `checkBudget`（`RunPrompt` 入口拒 + 跑完越界告警 + `RunBtw` 旁支拒） | 数据源 = 索引器 `cost_tokens` 派生表；口径 = input + output 累计；读之前先等索引追平（≤250ms 有界等待） |
| **per-turn** | `host/budget.go` `turnBudgetFor` → `loop.Config.TurnBudget` | 每轮 `provider.Run` **之前**再查一次；越界时**下一轮不再发起**（Run 以 `Result.TruncatedByBudget` 收口） |

- **默认不设上限**（`[budget].max_tokens = 0`）→ 闸门**整个不生效**：一次查询都不发、钩子恒 nil（零行为变化）。理由（W15 拍板）："长时程自主是卖点，预算不可控"。
- **fail-open + 留痕**：读成本失败（索引器卡了）→ **不拒本轮**（观测面故障不阻断对话），但告警一条。
- **fork 的既有缺口（如实标注）**：旁支 `RunBtw` **只查主 session 的账**，旁支自身花费记在 fork session 名下、不在本闸门口径内。

### 4.4 重试纪律（M7）

| 纪律 | 口径 | 出处 |
|---|---|---|
| 只有**声明幂等**的操作才自动重试 | 工具契约 `idempotent: bool` + `replay: replayKind`（缺省 false/unsafe = **不自动重试**）；`bash` 默认**不可**重试 | 07 **M7**；`contracts/tool.go` |
| 重试资格须显式声明 | `core/retry` 的 `RetryIf=nil` = **不重试任何错误**（fail-close） | 07 **M6** 落地注记 |
| provider 级白名单 | 只重试 429/5xx + 瞬时网络错；**永久传输错不重试**（TLS 证书类 / DNS `IsNotFound`）；服务端建议的 `Retry-After` **不采信** | 07 **M7** 补注（2026-11-01） |
| 错误不是崩溃 | 重试耗尽 → 返回聚合错误 → **转成模型可见的工具结果** + 记一条失败事件（M8）；执行器 panic 被 `executeSafely` recover 成一次普通工具失败 | 07 **M8**；`core/loop/toolcall.go` |
| 策略判定不重跑 | resume 重放同一操作时**刻意不重跑策略链**（判定快照已记进 intent） | 07 **M9** |

### 4.5 turn budget 与轮次上限

- `MaxTurns` = 单次 Run 的最大轮数（`-max-turns`，`0` = 默认 20）；到顶收口（`Result.Truncated` 一类），不是崩溃。
- `TurnBudget` = 轮间成本闸门钩子（见 §4.3）。
- 每个 agent 的 turn 超时 + 工具超时（K23 兜底）；异常必须**向上冒泡**（subagent 失败 → 直系父 agent 的 summary 必须报告它）。

### 4.6 历史折叠的三口径（F1 落地）

| 口径 | 带多少历史 | 出处 |
|---|---|---|
| `resume`（续跑） | **整场 session 全景**（不区分 agent 视角） | `cmd/alasol/resume.go`；07 **F1** 落地回填 |
| `fork`（`/btw` 旁支） | **整场**（但**带条数/长度上限 + 截断告知**：16 条 / 6000 字符，含工具参数）；**不带** ledger / gate | `host/fork.go` `RunBtw`；07 **K29** |
| 主轮次（每轮 prompt） | **主 agent 视角**（子 agent 产出经 spawn 的 `tool.call.completed` 回灌，不以 user/assistant 混入） | `host/hub.go:943` |

---

## 5. 工具面

### 5.1 工具全表（12 个名字 · 名字在描述/注册表 key/模型调用/事件里四处必须一致）

| 工具名 | 归属 | 用途 | side_effect | approvable | idempotent | replay | 装配方 / 出处 |
|---|---|---|---|---|---|---|---|
| `read` | 内置三件 | 读文件 | `read` | false | true | `safe` | `runtime/tools/read.go`；`runtime/tools/helpers.go:97` |
| `write` | 内置三件 | 整文件覆盖写（**目录不自动创建**） | `write` | **可配**（装配方给 true/false） | true | `verify` | `runtime/tools/write.go` `WriteDescriptor(approvable)` |
| `bash` | 内置三件 | 执行 shell 命令（Windows 走 `cmd`、其他走 `bash -c`） | `write` | **true** | **false** | `unsafe` | `runtime/tools/bash.go`；`shell_windows.go` / `shell_other.go` |
| `git_status` | 只读观测件（J3） | 查 git 状态 | `read` | false | true | `safe` | `runtime/tools/git_status.go`；**单独注册**，不进 `Descriptors()`；**不进 prompt** |
| `vector_search` | RAG（G2d，opt-in） | 语义向量检索 | `none` | false | true | `safe` | `host/vectors.go:53` `ToolVectorSearch`；**仅在 `[vectors].enabled=true` 时注册** |
| `standby` | 协作待机（G-f/G-j，opt-in） | 挂起本轮，等消息/交接/事件 | `none` | false | true | `safe` | `host/standby.go:157` `ToolStandby` |
| `recall` | 框架工具（压缩，v2-S1） | 取回阶段/seq 区间的**持久化原文** | `none` | — | true | `safe` | `core/compaction/recall.go:22` `ToolRecall`；refs **永不指摘要** |
| `stage` | 框架工具（压缩，v2-S1） | 在验证通过点显式标记阶段边界 | `none` | — | true | `safe` | `core/compaction/recall.go:23` `ToolStage` |
| `ledger` | 框架工具（goal ledger，J19/X6） | 更新 goal ledger 四张清单 | `write` | — | — | `unsafe` | `core/loop/ledger.go:39` `ToolLedger`；**增量更新**（未给字段保持原值） |
| `spawn` | 生命周期工具（多 agent） | 派生子 agent | `write` | — | — | `unsafe` | `core/loop/agenttools.go:32` `ToolSpawn`；`request_id` = X5 幂等键；**只对 `can_spawn` 非空的类型可见** |
| `complete` | 生命周期工具（多 agent） | 子 agent 提交结构化 summary 并收敛 | `none` | — | — | `unsafe` | `core/loop/agenttools.go:34` `ToolComplete`；**只对子 agent 可见**；summary 是回主 agent 的**唯一通道**（K24） |
| `mcp__<server>__<tool>` | MCP client（N1） | 外部 MCP server 的工具 | 随外部 | 随外部 | 随外部 | 随外部 | `runtime/mcp/manager.go`；**进同一个注册表**（同一 X1 三段路径、同一策略链、同一可见性） |

**工具契约字段全表**（`contracts/tool.go` + `contracts/schema/tool.json`）：`name` / `description?` / `schema`（**值本身是 JSON Schema**，元嵌套）/ `side_effect`（`none`/`read`/`write`/`destructive`）/ `cancelable` / `approvable` / `idempotent` / `replay`（`safe`/`verify`/`unsafe`）/ `required_env?`（**只声明键名**）/ `returns.graph_edges?`。

**注册表机制**（`core/registry/`）：`Register`（**name 全局唯一**，重名拒）→ `Enable`（**E3 能力裁决**，任一能力非 allow → **先撤销已 enabled** 再拒，fail-close；裁决在锁外；`ErrStale` 挡住 TOCTOU 与 lost update）→ `Disable`（幂等软停用，**同值 Disable 也抬状态版本号**）→ `Unregister`。裁决方 panic → 一律 deny（E5 失败隔离）。

### 5.2 框架工具与生命周期工具的边界

- **生命周期工具（`spawn` / `complete`）不经注册表**，由 loop 在装配可见集时**注入**（`core/loop/loop.go:1275-1286`）；但它们**也要过策略**（被 deny 的不进可见集）。
- **框架工具（`recall` / `stage`）** 由上下文策略引擎提供（`strategyTools`），同样过策略 + agent 类型白名单，按名字排序保证确定性。
- **`ledger`** 由 `-goal-ledger` / `[…].goal_ledger = true` 开关装配；默认关 = 零行为变化。

### 5.3 三层可见性（"可见 ≠ 可执行"）

`core/loop/loop.go:1231` `visibleTools` 的**过滤链**（顺序即语义）：

| 层 | 判据 | 出处 |
|---|---|---|
| ① **注册表 State** | `disabled` 的条目**不进模型可见集**（停用即不暴露）；执行侧另有第二次复查（堵"可见集刷新前的运行期停用窗口"） | `core/registry/registry.go`（`State` 三态）；`core/loop/loop.go:1254` |
| ② **R5 类型白名单** | agent 类型的 `tools[]` 不含它 → **不暴露**（白名单制：不暴露即不存在） | 07 **R5**；`core/loop/loop.go:1259`；`host/agenttypes.go:334-336` |
| ③ **H4 策略 deny** | 裸工具名 deny = **移出模型可见集**（与参数化 deny = 拦截调用不同） | 07 **H4**；`core/loop/loop.go:1262` |
| ④ **G-e session 工具白名单** | `config.tools{only, except}`：`only` ∩ 已注册 − `except`；`only` 缺省 = 不限 | 07 G-e；`core/loop/loop.go:1265`；`contracts/event.go:481` |

- **两层白名单相交、互不突破**：类型白名单 `[read]` ∩ session `only=[bash]` → 可见集为空。
- **执行许可链一字不改**：policy / perm 层独立判定；可见集只是"模型看到什么"（`core/loop/loop.go:1223`）。
- **执行期第二次复查**：`runToolCall` 在策略链前短路 disabled（不误归因 policy_deny），并在 `executeSafely` 前再查一次。
- **生命周期工具与框架工具不受 session 白名单管**（挂账，见 §14）。

---

## 6. 多 agent 面

### 6.1 agent 树与 spawn 三闸门

| 事项 | 口径 | 出处 |
|---|---|---|
| **闸门① 深度** | `child.depth ≤ max_agent_depth`（默认两级委派，数字计数器只是**兜底**——主设计是**类型可达性**） | 07 **K16** / **K17**；`core/agents/tree.go` |
| **闸门② 类型** | `child.type ∈ parent.can_spawn`；`can_spawn` 有向无环，**注册期环检测**（加载期 fail-close 点名） | 07 **K17** / **K20**；`core/agents/type.go` |
| **闸门③ 权限** | `child.permission` 只能是 `parent.permission ∩ policy_ceiling` 的**子集**（read / write / exec 三维**只收窄**；未知级别 fail-close 拒） | 07 **H11** / **H22** / **K17**；`core/agents/perm.go` |
| 任一闸门拒 | 落 `tool.call.failed`、**不建节点** | 07 **K17** |
| `agent_id` | **ULID**（不是递增序列——并发 spawn 下不安全） | 07 **K26** |
| `role` | 部门标签，**父 agent 在 spawn 时指定**；与 `agent_type` 正交 | 07 **K13** |
| 内建类型 | `planner` / `coder` / `critic` / `reviewer` / `observer`（**observer = 只读型**，`can_spawn` 空 → 第 4 层类型层面不可达）；用户 `[[agent_types]]` **同名覆盖内建** | 07 **K20**；`host/agenttypes.go` |
| 保留名 | `root` 是树根类型专用名，**不进注册表**（注册了会与树根同名 → 语义漂移）；**大小写敏感**（`Root` 不是 `root`） | `cmd/alasol/multiagent.go` `reservedAgentTypeIDs`；`host/agenttypes.go` |
| 单流多 agent 视图 | 主 / 子 agent **共用全局事件流**，靠 `agent_id` 区分；子 loop 与父同 Bus/SessionID、仅 AgentID 不同 | 07 **K25**；`core/loop/events.go` |
| 收敛通道 | `complete(summary)` 的结构化 summary 是**回主 agent 上下文的唯一通道**（tainted 加 ⚠ 前缀）；`refs` 可核验（`Inquiry` 拉原文） | 07 **K24**；`core/agents/summary.go` |
| 异常冒泡 | subagent 失败 / 卡住 → 直系父 agent 的 summary **必须报告它**（K23 兜底） | 07 **K23** |

### 6.2 K11 双正交可见性

| 维度 | 口径 | 出处 |
|---|---|---|
| **水平（gate）** | 只有 **session 的主 agent** 之间互相可见：`gates(A) ∩ gates(B) ≠ ∅` **且双方活跃**（`running ∨ 最后事件 ts ≤ 活跃窗口`） | 07 **K11**（G-a 收窄）；`core/discovery/activeLocked` |
| **垂直（agent 树）** | **父 ↔ 直系子**可见；**兄弟 subagent 互相不可见**；别 session 的 subagent 完全不可见 | 07 **K11** / **K27**；`core/discovery` `VisiblePeers` |
| 跨层上查 | **不是**「全祖先自动可见」——`Inquiry` **沿父链逐级显式调用**（每跳一次是一次调用） | 07 **K11** / **K27** |
| 孤儿防护 | 靠「父可见子 + 异常向上冒泡（K23）」闭环 | 07 **K11** |
| 兄弟协作 | **A 共享契约前置**（主 agent 在 spawn 时把表结构/API 签名写进子提示词）+ **B 共享黑板**（`board.written` 事件化）；**不开 C**（兄弟直接互聊） | 07 **K22** |
| 可见 ≠ 可执行（跨面） | agentlink 的可见集与工具可见集是**两回事**：K11 管"能看见谁"，工具链管"能调什么" | `core/loop/loop.go:1231`；`core/discovery` |

### 6.3 K14 / K15 在线表注入

| 事项 | 口径 | 出处 |
|---|---|---|
| 位置 | **恒在 prompt 末尾**（Q4：秒级变动 → 必须放末尾，否则打穿前缀 prompt cache）；注入 = **本轮请求的尾部消息**（不入持久历史）+ `context.assembled` 落 `online_table` part | 07 **Q4** / **K15**；`core/loop/onlinetable.go` |
| 默认参数 | **N=50 行**、每行 summary **≤100 token**、按 gate 过滤（复用 `VisiblePeers`） | 07 **K15**；`core/loop/onlinetable.go:39` |
| 暴露等级（三级） | `summary`（默认，一行摘要，受每行 token 预算截断）/ `n`（按需升，每行展开至多 `WindowLines` 行）/ `full`（**需显式**，全字段、全 id、不截断、不限行） | 07 **K14**；`core/loop/onlinetable.go:55-62` |
| 运行期可配 | `GET/POST /api/online-table/config`（**不重启**，改完下一轮生效）；CLI flag + TOML + flag 三处 | 07 **K15**；`host/onlinetable.go` |
| 活跃窗口 | 挂 `onlineTableConfig.activity_window`（缺省 **15 分钟**），**注入与 peers 共用同一个 N**（`host.syncActivityWindow` 单源下发） | 07 **K14/K15**；`core/discovery/SetActivityWindow` |
| 行集收窄 | 注入行集按"**活跃**"收窄（防默认接管后四件膨胀） | `15` §1.2；07 **K14** 落地回填 |

### 6.4 发现表（`core/discovery`）

| 事项 | 口径 | 出处 |
|---|---|---|
| 性质 | core 的**内存活状态**组件，**不是扩展点**；双层语义 = **事件折叠层**（可重建，`Apply` 幂等）+ **进程本地 live 层** | 07 **K10**；`core/discovery/discovery.go` |
| 数据流入 | **拉式扫描**（跟在线表同口径，防重复计数）；崩溃重启从事件流**全量重放**重建 | 07 **K10** |
| 成员关系 | `gate.joined` 落 → 成员折叠 = joined **减** `gate.left`（同 session+gate，按 seq 序折叠，乱序 Apply 稳定）；auto gate 显式离开后**不自动复活** | 07 **K12** / **G15**；`contracts/INVENTORY.md` §2 |
| 自动 gate | `repo:<repo_id>` / `workflow:<run_id>`；显式 `join_gate(gate_id)`（幂等：确定性 Event.ID + store ON CONFLICT 去重 + per-session 锁） | 07 **K12**；`host/discovery.go` |
| `Claim` | 在 `Activate` 之上登记宿主 hub 进程标识（Hub.New 生成 ULID）；`HostOf` 可查"谁托管"；冲突 **fail-close 拒绝接管**（`ErrHostConflict`，登记**不被改动**） | 07 **G25**；`core/discovery/claim.go` |
| 活跃判定 | `activeLocked` **一处**判定：`running`（`BeginRun`/`EndRun` 计数）∨ 最后事件 ts ≤ 窗口；**waiting 短路**（G-f standby） | `core/discovery/discovery.go`；`15` §1.2 |
| waiting 态 | 算活跃、**豁免活跃窗口**，但**不刷最后事件 ts**（不造假活动） | `core/discovery/BeginWait` / `EndWait`；`host/standby.go` |

### 6.5 agentlink 五件套（`core/agentlink/`，库形态、不绑定协议）

| 件 | 语义 | 协议面 | 出处 |
|---|---|---|---|
| `Presence` | 在线表：谁在线 / 在干什么 / 属于哪些 gate | `GET /api/presence` | 07 **K18** |
| `Discover` | 按 gate / 按 agent 树查找 | `GET /api/discover` | 同上 |
| `Inquiry` | 深入查询某 session 的事件窗口（摘要 + 窗口 + refs 核验，**token 锚定**防子串伪造） | `GET /api/inquiry` | 同上；07 **Q6** |
| `Broadcast` | 向 gate / 子树发消息 | `POST /api/agentlink/broadcast` | 同上 |
| `Handoff` | 结构化交接（broadcast + 开放 `payload`） | `POST /api/agentlink/handoff` | 同上 |

**投递口径**：
- **拉模型投递**：广播/交接**不直接推给收件方的上下文**，而是落一条 durable 事件（`agentlink.broadcast` / `agentlink.handoff`），由收件方**自己拉**（幂等键含发送方 + store 去重 + 回读归属校验）。
- **收件方 = 目标 ∩ 发送方可见集**（K11/K27 过滤）。
- **subtree 射程 = 目标子树 ∩ 发送方一跳可见集**（父 + 直系子）——**K27 显式逐跳，不做传递闭包**（"子树"命名大于实际射程）。
- 查询面全带 caller，按 K11 裁剪（`cross_session_subtree` / `not_visible` 是拒绝线）。

---

## 7. session / per-prompt 面（15 号设计的落地）

> 出处：`docs/research/15-session-per-prompt-design.md`（设计定稿）+ `host/`、`core/sessioncfg/`、`core/loop/` 的落地。

### 7.1 在线表三态

| 态 | 判定 | 出现处 | 出处 |
|---|---|---|---|
| **活跃 active** | `running`（本轮在跑）∨ 最后事件 ts ≤ 活跃窗口（缺省 15min） | 在线表注入 / peers / presence / GUI 在线表 | `15` §1.2；`core/discovery/activeLocked` |
| **托管 hosted** | 本进程持有活 loop（新建或默认接管） | 可立即对话；**不在对端在线表** | 同上 |
| **历史** | 未托管（库里存在） | session 历史表（可读；"继续"触发默认接管） | 同上 |

- **判定源 = 事件流的最后事件 ts**（全库可见 → 各容器判定一致；无租约无心跳不破；重启判定不丢）+ 本进程 `running` 短路。
- **四个消费方同刀改齐**（不留半截）：① 在线表注入行集 ② peers 的 ∩ 活跃 ③ presence / discover 过滤 ④ `GET /api/sessions` 每行加 `online` / `running` / `last_activity` + `?online=` **先过滤后截断**。
- **fork 旁支整族排除**（不是一等会话）；hub 伪 session 也不进表。

### 7.2 默认接管（懒接管）

```
POST /prompt → hostsSession? ──否──→ loopFor 懒接管（Claim + 建 loop + 从事件流恢复主 agent id）
                  │                     ├─ Claim 成功（原宿主失管）→ 跑 + hub.alert 留痕
                  │                     └─ Claim 冲突（另一活宿主持有）→ 拒（唯一拒绝线）
                  是 → 照常
```

| 事项 | 口径 | 出处 |
|---|---|---|
| 唯一拒绝线 | G25 双写防线（两进程同时写一个 session = 亲和写破）→ HTTP **409 `host_conflict`** / WS `error` 帧 | `host/takeover.go`；`host/approval.go` |
| 留痕 | 落一条 `hub.alert`（`metric=session.takeover`；判据 = HostOf 易主 **且** created）——**零契约改动**（复用既有事件 kind） | 07 **G25** 落地回填 |
| 并发正确性 | Claim **先拿权后发布** + **双检锁**修并发绕过（24 并发全 409、零写入零留痕） | `core/discovery/claim.go` |
| 读面 vs 写面 | 协议面**写**端点懒接管；**读**端点（`/api/peers`、`/api/gates/join`、`/api/agentlink/*`）对野 session 仍 404 | `host/approval.go` 文件头 |
| 已知窗口（如实标注） | closed 检查与 ensureHosted 之间有竞态窗口：接管已发生但受理被拒 → 留痕可能落在一个**没跑成**的 prompt 上；**有意接受**（留痕语义 = "接管了"，不是"这一轮受理了"） | `host/approval.go`（P2-5 注记） |

### 7.3 `config.changed` 生效链

```
hub 启动默认（Config）
  → session 配置（config.changed 折叠；新建 session 继承 hub 默认）
    → 本轮快照（Run 开始时冻结；运行中变更下一轮生效）
```

**六组字段与三态语义**（`contracts/event.go` `ConfigChangedPayload` + `core/sessioncfg/`）：

| 组 | 字段 | LWW 口径 | 三态 |
|---|---|---|---|
| 模型 | `model` | 标量 LWW | 出现 = 覆盖 / 缺失 = 不变 / 空串 = 清除回 hub 默认 |
| 档位 | `reasoning_effort` | 标量 LWW | 同上；取值白名单 `low`/`medium`/`high`/`xhigh`（G14），**host 层校验** |
| provider | `provider{base_url, api_key_env}` | **子字段各自 LWW** | 同上；**改 `base_url` 必须同时给 `api_key_env`**（不静默把 hub 凭据发给第三方端点）；值只从 env 取 |
| 工具 | `tools{only, except}` | **整组 LWW**（不拆子字段跨事件合并） | 组出现 = 整组覆盖 / 组缺失 = 不变 / `tools:{}` = 清除回默认；组内子项：出现 = 覆盖、**空数组 = 清空该项**、缺失 = 该项未设置 |
| 上下文 | `context{kind, window, l1_pct, l2_pct, rules_budget, common_sense_budget}` | **子字段各自 LWW**（不学 tools 的整组 LWW） | 出现 = 覆盖 / 缺失 = 不变 / **数值 0 = 清除回默认** / `kind` 空串 = 清除回默认 |
| 策略 | `policy` | **不实现** | G-g 拍板不做运行时权限修改面（见 §9.6） |

**`kind` 的值域（可来回切）**：`""`（清除回默认）/ `append`（显式追加式，session 能把 tiered 显式切回来）/ `tiered`。**"来回切"是无损的**：tiered 段有 `stage:N` 压缩痕迹，append 段全量原样，**切回 append 后全量历史完整**（压缩摘要是投影，丢的是摘要不是原文）。

**写时校验（host 层，fail-close、零事件）**：`reasoning_effort` 白名单；`base_url` 必须配 `api_key_env`；`tools` 元素非空串、**单数组上限 256**、**不校验是否已注册**；`context.kind` 白名单、数值非负、tiered 时 window ≥ **256k**、百分比 ∈ (0,100] 且 L2 ≤ L1、布局自洽（唯一事实源 = `compaction.ValidateLayout`）。**生效组合校验** = fold + 本次提交 + 宿主默认叠起来仍合法。

**profile 叠放次序**（G-i）：**先展开 profile → 再让显式 config 逐字段压过 → 合成一条 `config.changed`**（审计里看得见**展开后的值**）。各组按自己的 LWW 口径压。**profile 的空字段 = 不覆盖**（模板语义），**不是**"清除"语义（清除是显式 config 层的空串）。

### 7.4 profiles（命名 profile）

| 事项 | 口径 | 出处 |
|---|---|---|
| 声明面 | TOML **数组表** `[[profiles]]`（**不是 map**：map 下钻的 unknown-key 闸门查不到"map 里某个键名写错了"） | `host/config_file.go` `ProfileFileConfig` |
| 两个面 | **配置面**（`model` / `reasoning_effort` / `provider` / `tools` / `context`）+ **执行面**（`cwd` / `worktree` / `run_id` / `remote_host` **声明**） | 同上 |
| 展开入口 | `POST /prompt` body 的 `profile` 槽位 / WS `command.submit` 的 `profile` 槽位 / `POST /api/sessions` body 的 `profile` 槽位 | `host/config_fold.go` `expandProfile`；`host/http.go` `serveSessionCreate` |
| 未知名 | **fail-close**：HTTP 400 `bad_profile` / WS `error` 帧 `bad_profile`，**零事件** | 同上 |
| 重名 | 装载期 **fail-close**（同名两张表落哪张是随机的） | `host/config_file.go` `ProfileConfigs` |
| 读面 | `GET /api/profiles`（按 name 升序；空表 → `{"profiles":[]}`；**只出 `api_key_env` 变量名**） | `host/profiles.go` |
| 执行面边界 | `POST /api/sessions` 用 profile 填 `SessionSpec`（显式 body 字段**压过** profile）；**`remote_host` 只留痕、不校验、不换 hub 级执行环境** | `host/http.go` `serveSessionCreate`；`host/worktree.go` 文件头 |

### 7.5 standby（待机工具）

```
standby({wait: "message"|"handoff"|"event", timeout_min: ≤30, on_timeout: "return"|"sleep"})
```

| 事项 | 口径 | 出处 |
|---|---|---|
| 挂起语义 | 本轮**挂起**：`tool.call.intent` 已落、`completed` **延迟**到唤醒/超时——**X1 三段形状不变** | `15` §4；`host/standby.go` |
| waiting 态 | 在线表标注 `waiting`：**算活跃、豁免活跃窗口，但不刷最后事件 ts** | `core/discovery/BeginWait` |
| 唤醒 | agentlink 消息 / handoff 到达（跨进程可见：轮询事件流）；或**事件到达**（G-j：`event_kind` 精确或尾缀通配 `prefix*` + `event_scope` self\|any 默认 self；只认 durable，排除本次 intent 自身，流式固定缓冲 ≤32 候选取最新） | `host/standby.go`；`15` §4 |
| 交付集合去重 | **"交付过的 seq"单一集合**（消息与事件共用同一个全局 seq 空间，跨 kind 不互踩）；消息面早到 = 全历史 − 交付集合；事件面早到 = 仅本轮内（`model.turn` 后至 intent 前） | 07 批次3 收口回填 |
| 超时两态 | `return` = 立刻如实收口并回答"未等到"（`timeout_min` 是超时点）；`sleep` = 等到消息到达或 **30 分钟硬上限**（`timeout_min` **不适用**——旧口径的"软检查点"无任何可观测差异，已取消） | `host/standby.go:165-190` |
| 硬上限 | 一轮待机 **30 分钟**（`standbyHardCapDefault`）；零 WS 连接、零模型轮（挂起期零 provider 调用） | `host/standby.go:193` |
| 预算语义 | `runClock` **预借 + 实占结算**（挂起期预算暂停） | 07 批次2 落地回填 |
| resume 重放 | 崩溃时 standby 中的工具是 in-flight → 处置要能**重新挂起或如实收口**；resume 重放**带原 id**（消息已到 → 如实收口；未到 → 重挂） | `15` §4；`host/standby_resume.go` |
| WAL 对账 | standby **不产生"在飞轮次"误报**（它是工具不是轮次）；WAL facts 侧排除 | `15` §4 |

---

## 8. 可靠性面

### 8.1 resume（X4）

| 步 | 内容 | 出处 |
|---|---|---|
| 1 | 读 `checkpoint.created`（四要素：X3） | `core/durable/checkpoint.go` |
| 2 | fold 事件流重建当前态 | `core/durable/resume.go` |
| 3 | **认领**（`session.resume_claimed`，X5 防并发重放；原子；`ErrStale`/`-force` 处理"上次崩在认领之后"） | `core/durable/claim.go`；`cmd/alasol/resume.go`（`-force`） |
| 4 | **处置 in-flight**（三类）：`safe` = 可直接重放 / `verify` = 先核验再定（`git-commit` 锚点核验，`-verify git\|none`）/ `unsafe` = **转人工**（不盲目重跑） | 07 **X2** / **X4**；`contracts/common.go` `ReplayKindSafe/Verify/Unsafe`；`cmd/alasol/resume.go`（`-verify`） |
| 5 | 落 `session.resumed` 完成标记 | `core/durable/resume.go` |

- **`-verify none`** = 全部 verify 类转人工。
- 打印处置明细（safe 重放 / verify 先核验 / unsafe 转人工）。
- **fail 重试 ≠ 干净收场**：WAL 六个写点的顺序保证"有 begin 无 end 无 failed"才是崩溃残留（普通失败有 `turn.failed` 收口，不误报）。

### 8.2 WAL 预写日志（六个写点 + reconcile + GC）

**六个写点**（`core/wal.go` 词汇表 + `core/loop/wal.go` 写点；**全在 X1 三段写路径之外**）：

| kind | 写点位置 | 用途 |
|---|---|---|
| `turn.begin` | `provider.Run` **之前** | 崩溃后"第 N 轮在跑 X 模型" |
| `turn.delta` | 流式增量**逐条** | 本刀最大价值：`model.delta` 不进事件流，崩溃后整轮蒸发；WAL 逐条记下 |
| `turn.end` | `model.turn` **落盘之后** | 顺序有意（反过来会把崩溃报成干净收场 = 假阴性） |
| `turn.failed` | `provider` 返错时 | 普通失败收口（与崩溃残留区分） |
| `blob.plan` | `tool.call.intent` **发布之前** | 外置计划（不 fsync） |
| `turn.retry` | `provider.Run` **内部**重试发起前 | 重试留痕（纯观测，不进对账） |

| 事项 | 口径 | 出处 |
|---|---|---|
| 文件形状 | per-session 一个 `<session>.wal`（NDJSON 追加）；**逐条不 fsync、轮次边界 fsync**（进程崩溃不需 fsync；机器掉电时尾部 delta 可能丢——**有意取舍**） | `runtime/store/walfile` |
| 损坏容忍 | 半截行计入 `CorruptLines`；**超 1MB 的行跳过后继续读后面的行**（不中断扫描） | `runtime/store/walfile` |
| reconcile | 对账产出：在飞轮次 / 孤儿 blob / 作废计划；**不误报**（有对应 completed 带 binary_ref → 两个名单都不进） | `runtime/store/walfile/reconcile.go` |
| GC | `alasol wal-gc`：按**显式龄期**删（`-retention` **必给**，缺省 0 = 退出码 2，不替你猜）；判据 = mtime 超龄 **且** 对账干净（无在飞轮次/孤儿候选 + 合法 `<session>.wal` 形态）；**作废计划不算脏**；`-dry-run` 只报不删 | `cmd/alasol/wal_gc.go`；`host/config_file.go` |
| **WAL 不是事实源** | 事件流（`core.Store`）仍是唯一事实源（F5 不受影响）；WAL 可丢弃 / 可截断 / 可 GC（**删掉整个目录不丢任何事实**）；恢复报告是**观察面不是写回面**（delta 可供展示，**绝不自动补进事件流**） | `host/config_file.go` `WalFileConfig` 文件头 |
| 写失败 | **不阻断主流程**（尽力而为，一条 Warn 留痕） | 同上 |
| 观察面 CLI | `alasol wal -session <id>`：崩溃恢复报告（单行 JSON；未配 `[wal].dir` → `wal_exists=false`，**不报错**） | `cmd/alasol/wal.go` |

### 8.3 blob 外置（D4）

| 事项 | 口径 | 出处 |
|---|---|---|
| 触发 | `tool.call.completed` 的 result **超阈值**整体外置；事件里留 `binary_ref` + 摘要 text（**占位标记**点名 blob id + 前 256 字预览）；Data 置空、Data-only 块给占位不留空块 | `core/loop/blob.go`；`host/config_file.go` `BlobFileConfig` |
| 零契约改动 | `ToolResult` 形状不变；`Event.binary_ref` 早已在契约里 → **不走 F7 全链** | 07 **D4** |
| 有效阈值 | **T = max(阈值, 摘要预算 B)**，`B = min(2KB, max(阈值/2, 768))` → **结构性保证"外置 = 瘦身"**（摘要不会比原文还大） | 07 **D4** 终查收尾 |
| 内容寻址 id | `contentBlobID = Crockford26(SHA-256("blob\x00" ‖ raw)[:16])`（与 vec 域隔开）；同内容重放 → 同 id → blobfs 幂等路径**不写第二份**；碰撞概率 2^-128 → `ErrConflict` → fail-open 原样内联 | 07 **D4** 缺口补全回填 |
| 实现 | `runtime/store/blobfs`：一 id 一目录 = `meta.json` + content；临时文件 + rename 防半截；幂等 / `ErrConflict` / `Delete` 不级联 | `runtime/store/blobfs` |
| 失败口径 | **外置失败 fail-open**（保 X1）；`failed` **不外置** | 07 **D4** |
| GC | `alasol blob-gc`：**三扫**（`.tmp` 残留 / 孤儿目录 / mtime 清扫）；`-retention` 缺省 0 = 只安全两扫；`-grace` 缺省 1h（**显式 0 仍用 1h——安全网不给关**）；负值 = 非法配置 → 退出码 2；**龄期 = 最后外置时间**（幂等 Put 触碰 meta mtime 不重写内容）；`binary_ref` 不动（读侧报"内容已回收"） | `cmd/alasol/blob_gc.go` |
| 已知缺口（如实标注） | 折叠侧**不看 envelope**（只读 payload）→ 续跑看到的是摘要 / 占位；**跨进程唯一防线是 Grace**（锁只在同进程内互斥） | 07 **D4** |

### 8.4 崩溃恢复口径（一句话版）

| 场景 | 恢复口径 |
|---|---|
| 崩在模型调用中 | WAL `turn.begin` 在、`turn.end`/`turn.failed` 不在 → **在飞轮次**（`alasol wal` 报告出来） |
| 崩在工具执行中 | 事件流里有 `tool.call.intent`、无 completed/failed → **未知结果，不盲目重跑**（X1）；`replay=verify` 的先核验锚点，`unsafe` 转人工 |
| 崩在外置写中 | `blob.plan` 已落 → 孤儿 blob 候选（GC 扫）；`binary_ref` 引用与内容最终一致，续跑按 `binary_ref` 补取 |
| 崩在 standby 中 | in-flight 工具 → 重新挂起或如实收口（**不许当垃圾清掉协作状态**） |
| 崩在索引器 | 水位持久化在 `indexer_watermark` 单行表 → 从 N 续跑；投影 + 水位同事务原子、整体回滚（07 **G24**） |
| 崩在 hub 启动期图重建 | `graph_meta.rebuilding` 标记 → 启动期自动恢复一次（人工重试 = `POST /api/graph/rebuild`） |

---

## 9. 安全面

### 9.1 网络面（loopback-only + Origin 校验）

| 事项 | 口径 | 出处 |
|---|---|---|
| 监听地址 | **只接受 loopback**（默认 `127.0.0.1:8720`，可配端口**不可配成非 loopback**） | 07 **N5b**；`cmd/alasol/serve.go:101`；`host/doc.go:18-25` |
| 域名陷阱 | `127.0.0.1.evil.example` 这类域名**会**解析到 loopback（实测能绑上监听）→ 校验按**字面 IP** 判 | `host/doc.go:21` |
| WS Origin | 同源 / 无 Origin（非浏览器客户端）/ loopback 互认 → 放行；其余 → 403 `origin_rejected` | `runtime/transport/server.go` `checkOrigin` |
| 前端接入 | **必须同源或自备代理**；**不要**指望跨源 fetch / 直连 ws（浏览器会因 CORS / Origin 校验被拒） | `host/doc.go:188-198` |
| 远程接入 | SSH 隧道：`ssh -L <port>:127.0.0.1:<port> <user>@<host>`；**鉴权交系统 SSH** | `host/doc.go:235` |

### 9.2 CORS 白名单

见 §3.4（**opt-in、精确 origin、不支持 `*`、只作用 `/api/*`**）。

### 9.3 沙箱（K8 / H9）

| 事项 | 口径 | 出处 |
|---|---|---|
| 形态 | Windows **无原生沙箱可行面** → 务实组合：**job object + 受限 token + 目录 ACL**；可拓展埋点留 Linux / mac（seatbelt / bwrap 后续挂） | 07 **H9**；`runtime/cap/sandbox` |
| job object | `KILL_ON_JOB_CLOSE` 收容整棵进程树 + `ACTIVE_PROCESS` 上限（默认 **64**，fork 炸弹有界化） | 同上 |
| 受限 token | 组 SID 全量收紧 deny-only + **13 特权删除** | 同上 |
| 目录 ACL | `write_dirs` 里的根设 DACL，**根之外的写被内核拒绝**（OS 强制，不是路径守卫）；**追加式**授权、只收窄（M） | 同上 |
| 已知边界三条（如实标注） | ① 默认模式 `BUILTIN\Users` 授权位置（如 `C:\ProgramData`）仍可写（`-sandbox-strict-groups` 可关，代价 = overlapped I/O 全挂）；② 读边界**只挡用户私有文件**（`.env` / `.ssh`），系统位置默认仍可读；③ 允许根 DACL **追加传播不还原** | 07 **H9** 落地注记；`host/config_file.go` `SandboxFileConfig` |
| 换 env 不动 agent 逻辑 | K8 酸测试：换 `ExecutionEnv` 实现不动 agent 代码 | 07 **K8** |
| 配置 | `[sandbox]`：`enabled` / `write_dirs` / `max_active_processes` / `acl_max_objects` / `strict_groups`；**默认关 = 走 local，与 v1 等价** | `host/config_file.go` |

### 9.4 remote SSH 执行面（K19）

| 事项 | 口径 | 出处 |
|---|---|---|
| 定位 | **不做分布式存储 / 分布式发现**；跨机 = 把 session 暴露成 **ACP agent 端点**，用现成协议接入 | 07 **K19** |
| 执行面 | `RemoteExecutionEnv`（`runtime/cap/remote`）：**Exec + Fs** 经 SSH 落地（Fs 是最小可用档：cat / stat / ls）；**Proc / Net 显式 `ErrUnsupported`**（预留位）——所以 `read`/`write` 也接到 remote env，会经 ssh 通道读写**远端**文件 | `host/hub.go`（远程装配段）；`cmd/alasol/wire.go` |
| 凭据 | **鉴权交系统 ssh，此处不传任何凭据**（H18） | 07 **K19** |
| ACP 端点 | `alasol acp`：JSON-RPC 2.0 over stdio（自实现帧编解码、不引依赖）；`initialize` / `session/new`（sessionId = hub ULID 原样）/ `session/prompt`（回 stopReason）/ `session/update`（桥 bus 增量）/ `session/cancel` / `session/request_permission`（ask 审批桥，fail-close deny）/ `session/load`（可选能力，后端没实现则 `loadSession=false` **如实**） | 07 **K19** 落地注记；`runtime/acp`；`cmd/alasol/acp.go` |
| ACP 纪律 | **stdout 只走 RPC 帧、日志走 stderr**；**审批恒走 ACP 的 `session/request_permission`**，`-approval` flag 被忽略 | `cmd/alasol/acp.go`；`cmd/alasol/main.go` usage |
| 脱敏 | api-key 连字符键 / `AKIA` / PEM 三类补齐；两档分级降误伤；收窄如实（两侧空白与 PEM 短尾行不遮） | 07 **K19** 缺口补全回填 |

### 9.5 审批鉴权边界（如实写，不美化）

- **审批应答没有鉴权**：任何能连上 hub 的客户端都可以应答审批。
- 这是**有意的口径**，不是待修的 bug——v0 的威胁模型 = 只监听 loopback + 无 cookie + 无 token；loopback 上的其它进程与 hub **同权**。
- 收紧记 v4（鉴权面随那时一起设计）。
- 出处：`host/doc.go:177-181`；07 **D14**（"应答无鉴权——任何能连上 hub 的客户端可批，收紧记 v4"）。

### 9.6 G-g 拍板不做（运行时权限修改面）

- **拍板（2026-11-03 用户）：不做运行时权限修改面**——`config.policy` 字段**不实现**。
- 权限**维持现状（两层静态）**：① hub 启动级（`-allow` / 策略规则 / 审批模式四档）② agent 树 perm（子级权限不超出父级）。
- 理由：subagent 频繁弹审批伤体验；实际部署基本 always-allow / 全开，运行时修改面只有摩擦没有收益。
- F7 只增，**随时可补**（届时随 v4 鉴权面一起设计放宽语义）。
- 出处：`15` §5 尾条 + §6 的 G-g 行；07 批次3 收口回填。

### 9.7 三层权限模型与求值链

| 层 | 口径 | 出处 |
|---|---|---|
| 决策层（prompt） | 工具白名单 / 提示词引导 | 07 **H1** |
| 策略层 | 有序链，**首个命中即返回**，顺序 deny → ask → allow；**deny 优先且不能写 allow 例外** | 07 **H2** / **H3** |
| 强制层 | OS 沙箱（§9.3） | 07 **H1** / **H9** |
| 规则来源 | **deny / ask 跨所有来源取并集**（任一来源命中即 deny/ask，不看优先级）；**只有 allow 才按来源优先级解析**：managed > CLI > project > **workflow** > local > user > session | 07 **H5** |
| workflow 规则 | **只允许收窄、不允许放宽**（workflow 是用户/agent 可写数据，能放宽就是未审计的注入通道） | 07 **H24** / **H5** |
| 两种 deny | **裸工具名 deny** = 移出模型可见集；**参数化 deny** = 拦截调用 | 07 **H4** |
| 子 agent 权限 | **只能收窄，deny 强制继承**；实际 = 请求 ∩ 策略上限 ∩ 父级授予 | 07 **H11** / **H22** |
| 凭据注入 | 工具声明 `required_env` → **策略裁决后**才注入该次调用（deny/ask 未放行 → 不注入）；注入值进 secrets 供输出脱敏 | 07 **H17**；`core/loop/env.go` |
| 执行 env 默认 | 工具默认继承**净化后**的父 env（凭据类键与值形态剥离）；普通 env（PATH / HOME 等）保留 | 07 **H16**；`core/env.go` `ScrubEnv` |
| 事件记录 | **只记 env 键名，不记值** | 07 **H19** |

---

## 10. 观测与协调面

### 10.1 事件回放（`GET /api/events`）

| 事项 | 口径 | 出处 |
|---|---|---|
| 参数 | `from`（缺省 1）/ `to`（缺省不设上界）/ `limit`（缺省 **1000**，硬上限 **10000**，**`limit=0` 钳到 1**）/ `session_id`（**投影，非可见性过滤**） | `host/events.go` |
| 响应 | `{events[], watermark, from, to, truncated}`；`watermark` = **事实流 tip**（store 的 MAX(seq)），**≠** `/api/health` 的索引器水位 | `host/events.go` `eventsView` |
| 截断信号 | `truncated=true` = 还有更多 → 客户端按最后一条的 seq+1 继续翻 | 同上 |
| 只含 durable | 流事件（`durable=false`）**不入库、不回放**（丢了就是丢了） | 07 **F8** |
| 可见性 | **不做 K11 过滤**（原始事件日志面）；要 K11 过滤 + 跨 session 摘要核验 → `/api/inquiry` | `host/events.go` 文件头 |
| 为什么要它 | WS 断线即丢（订阅者通道满会丢，`bus.Lagging`；连接断了更是什么都收不到）→ 没有补读面的话，第三方 GUI 重连后 `[first_seq,last_seq]` 会出现**永久空洞**，"事件流是唯一事实源"对进程外消费者不成立 | 同上 |

### 10.2 上下文快照（`context.assembled`）

| 事项 | 口径 | 出处 |
|---|---|---|
| 落点 | 每轮模型调用**之前**落一条 durable 事件（`ContextSnapshot` 契约）；**落库失败不中止 loop**，只补一条 `loop.error` | 07 **G12**；`core/loop/context.go` |
| 形状 | `parts[]`（source / kind / entries / tokens / state / origins / summary_ref），`state ∈ verbatim/compacted/dropped/deferred` | `contracts/schema/context-snapshot.json` |
| 读面 | `GET /api/context/{session_id}`（最新一条）；无 → 404 `no_context_snapshot` | `host/http.go` |
| 计量口径 | `context.assembled` = **调用前快照 = 正式估算口径**；**provider usage 只进成本统计、不回填快照**（快照生成时 usage 尚不存在） | 07 **J21** |
| 压缩痕迹 | `online_table` part 落在快照里；`context.compacted` 记每次压缩动作（refs 永不指摘要） | `core/loop/onlinetable.go`；`contracts/INVENTORY.md` §2 |

### 10.3 monitor + `hub.alert`

| 事项 | 口径 | 出处 |
|---|---|---|
| monitor | hub 级三档监控（CPU / 内存 / goroutine 数）；周期采样、阈值 **flag > env > 文件 > 默认**；**有界告警环** | 07 **K9**；`host/monitor.go` |
| 读面 | `GET /api/monitor` | `host/http.go` |
| `hub.alert` | 告警**进事件流**（v2-S5 清账②：此前只进 slog 日志，事件流里看不到、崩溃后不可复盘）；`metric`（cpu/mem/goroutines）+ `value` + `threshold` + `message`；**归属伪 session**（按事件库路径确定性派生 id，跨重启稳定）；**payload 只记结论**，不含会话内容/凭据 | `contracts/INVENTORY.md` §2；`host/alert_event.go` |
| 用途二 | `hub.alert` 也是**默认接管**的留痕载体（`metric=session.takeover`，零契约改动） | §7.2 |

### 10.4 graph 影响面（可选投影）

| 事项 | 口径 | 出处 |
|---|---|---|
| 默认 | **关**（`[graph].enabled=false` / `-graph` 不开）→ 不建图表、不建引擎、索引器连 `tool.call.completed` 载荷都不解析（**零图开销**） | 07 **G2f**；`host/config_file.go` `GraphFileConfig` |
| 读面 | `GET /api/graph/impact?roots=&kind=&depth=&limit=&core=` → `ImpactReport`（一跳/传递下游 + 触及的核心接口 + diff 规模 + `Scale` + `suggest_escalate`） | 07 **G21**；`host/graph.go` |
| 未启用 | 503 `graph_disabled`（**明说"没开"，不返回空影响面**——"没有影响面"对 escalate 是危险答案） | `host/http.go` |
| 重建 | `POST /api/graph/rebuild`（N2 运维入口）；`graph_meta.rebuilding` 标记 + 启动期自动恢复一次 | 07 **G21** 已清；`host/graph_rebuild.go` |
| 边界 | 节点 id 用整数 / interned string；上界 10 万节点 / 100 万边（可配）；`core` 里 `*` 结尾 = **字节前缀**（大小写敏感） | 07 **G19**；`host/config_file.go` |
| 用途链 | 图不是"未来可能用"——它是 `change_impact` 的底层实现 → 影响面 → escalate 闸门，**三者是一条链** | 07 **G21** |

### 10.5 telemetry（L2 / D19）

| 事项 | 口径 | 出处 |
|---|---|---|
| 定位 | **独立管道**，默认关 / 脱敏 / 单向；**telemetry 不是 trace**（trace = 事件流本身，L1） | 07 **L1** / **L2**；`core/telemetry.go` |
| 三实现 | `noop`（默认）/ `file`（NDJSON 单文件 `telemetry.ndjson`）/ `otlp`（**留位**，`Emit` 恒返 `ErrUnsupported`，**装配期拒**） | 07 **D19** 落地回填；`runtime/telemetry` |
| 三个发点 | `run.begin` / `tool.call` / `run.end`——**全在 X1 三段写路径之外**（不侵入驻点窗口） | `core/loop/telemetry.go` |
| 脱敏 | emit 前过 `core.Redact`（去掉它 → 脱敏钉子红）；`Emit` 失败只 Warn 不阻断 | 07 **L2** |
| 关 = 零行为变化 | `loop.Config.TelemetrySink` 恒 nil → 一条 record 都不造、一次 Emit 都不调（酸测试：关/开逐事件等价，**含归一化 payload 深度比对**） | 07 **L2** |
| 配置 | `[telemetry]`：`mode`（`""`/`none`=关 / `file` / `otlp`=拒）/ `dir`（mode=file 时**必填**，启动即拒，不静默关） | `host/config_file.go` `TelemetryFileConfig` |

### 10.6 merge 三面（门禁增强 V6 / V7）

| 端点 | 语义 | 请求体 | 出处 |
|---|---|---|---|
| `POST /api/merge/review` | 落一条 `review.recorded`（`branch` + `tip_commit` + `conclusion`） | `{session_id, agent_id, branch, tip_commit, conclusion}` | `host/mergegate.go:338`；07 **V6** |
| `POST /api/merge/check` | 合并前查放行 → `{allowed, re_review_required, has_review, conclusion, reason}` | `{session_id, branch, current_tip}` | `host/mergegate.go:304` |
| `POST /api/merge/predict` | 合并后冲突预测 → `ConflictPredictedPayload` | `{session_id, agent_id, kind, merged, others}` | `host/mergegate.go:386`；07 **V7** |
| **失效判定** | 被 review 的分支 tip 变 → 结论失效 → 要求重审（tip 就在 payload 里，合并门禁拿它比对当前 tip） | — | 07 **V6** |
| **诚实降级** | 图未启用 / 闭包出错 / 影响面没追全 / 有未合并分支不可评估 → `available=false` + `unavailable_reason`（**明说"预测不可用"而非"无冲突"**） | — | 07 **V7** |

### 10.7 git 观测面（`GET /api/git`）

| 事项 | 口径 | 出处 |
|---|---|---|
| 形态 | hub 工作目录的 git 状态快照（branch / dirty / last_commit）；**拉式现采、HTTP-only、不进事件流**（git 状态是进程本地派生观测，非事实流事实） | 07 **J3** 补面；`host/git.go` |
| 与模型工具同源 | 复用 `tools.SnapshotGitStatus`——与 `git_status` 工具**同源**，两面状态恒一致 | 同上 |
| 参数 | `?path=` 覆盖采样目录；非仓库 → `repo=false` + error（**不是** 404/500） | `host/git.go:53` |

### 10.8 成本观测（`GET /api/cost`）

- `/api/cost` = 跨 session 汇总；`/api/cost/{session_id}` = 单 session（按模型分组）。
- 数据源 = 索引器 `cost_tokens` 派生表；口径 = input + output 累计。
- usage **只进成本统计、不回填快照**（J21）。

---

## 11. CLI 面（`cmd/alasol` 子命令全表 · 12 条）

| # | 子命令 | 一句话 | 主要 flag | 出处 |
|---|---|---|---|---|
| 1 | `run` | 跑一轮真机回路（单次） | `-prompt`（必填）`-model` `-base-url` `-api-key-env` `-approval` `-allow` `-db` `-max-turns` `-checkpoint-every` `-system` `-reasoning-effort` `-config` `-sandbox` `-sandbox-write-dirs` `-sandbox-max-active` `-sandbox-acl-max-objects` `-sandbox-strict-groups` `-remote` `-remote-host` | `cmd/alasol/run.go` |
| 2 | `resume` | 恢复一个 session（X4：处置 in-flight；`-continue` 续跑一轮） | `-session`（必填）`-db` `-verify git\|none` `-force` `-continue` `-model` `-base-url` `-api-key-env` `-reasoning-effort` `-sandbox*` `-remote*` `-config` | `cmd/alasol/resume.go` |
| 3 | `serve` | 起常驻 hub（Web UI + WS 事件流 + HTTP API） | 全量 hub flag（见 §12.3） | `cmd/alasol/serve.go` |
| 4 | `acp` | 起 ACP agent 端点（JSON-RPC 2.0 over stdio；K19 跨机，鉴权交 SSH） | 复用 hub flag 面；**`-approval` 被忽略**（恒走 `session/request_permission`，无人应答 = deny） | `cmd/alasol/acp.go` |
| 5 | `mcp-serve` | 起 MCP server（B 类：把 harness 工具面暴露成 MCP tools，stdio） | 复用 hub flag 面；暴露面由 `[mcp_server].expose_tools` 决定（**空 = 零暴露**，fail-close） | `cmd/alasol/mcp_serve.go` |
| 6 | `chat` | 终端 REPL（对话 / `/table` / `/ctx` / `/cost` / `/quit`） | `-session` + hub flag 面 | `cmd/alasol/chat.go` |
| 7 | `tui` | 三面 TUI（对话 / 审批 / 在线表，bubbletea） | `-session` + hub flag 面 | `cmd/alasol/tui.go` |
| 8 | `models` | 列出端点支持的模型（模型名**端点相关、无万能默认**） | `-base-url` `-api-key-env` `-config` `-offline` | `cmd/alasol/models.go` |
| 9 | `wal` | 崩溃恢复报告（扫该 session 的 WAL 与事件流对账；**观察面，不写回事件流**） | `-session`（必填）`-db` `-wal-dir` `-config` `-json` | `cmd/alasol/wal.go` |
| 10 | `wal-gc` | WAL 回收（按**显式龄期**删"已没用"的 session WAL） | `-config` `-wal-dir` `-retention`（**必给**，缺省 0 = 退出码 2）`-dry-run` | `cmd/alasol/wal_gc.go` |
| 11 | `blob-gc` | blob 外置仓库 GC（回收崩溃残留） | `-config` `-retention`（缺省 0 = 只安全两扫）`-grace`（缺省 1h）`-dry-run` | `cmd/alasol/blob_gc.go` |
| 12 | `eval` | 最小评测面（事件流即数据集，折叠逐 turn 指标；**不建独立评测框架**） | `-session`（必填）`-db` | `cmd/alasol/eval.go` |

**CLI 纪律**：
- 审批**默认 deny-all**（fail-close）；`always-allow` / `terminal` / `protocol` 必须显式选。
- `-allow` 生成 `source=cli` 的 allow 规则；**空 = 一切工具 deny**（fail-close）。
- **API key 只经 env 注入**（`-api-key-env` 只传**变量名**），**永不进 argv / 事件 / 日志**。
- **hub 只监听 loopback**（N5b），非 loopback 地址直接拒绝。
- 退出码：`0` = 正常收场；`1` = 端点 / 装配错误；`2` = 配置被拒（档位白名单 fail-close）。

---

## 12. 配置面

### 12.1 TOML 全键表（94 个叶子键 = 16 个顶层标量 + 13 个段 + 3 个数组表）

出处：`host/config_file.go` `FileConfig`（**反射派生**的合法键清单与结构体永不漂移）。文件路径：`%APPDATA%\alasol\config.toml`（Windows）/ `$XDG_CONFIG_HOME/alasol/config.toml`（其他平台）。

| 段 | 键 | 口径 |
|---|---|---|
| **顶层** | `base_url` | provider 端点（空 = 官方） |
| | `model` | 模型名（端点相关，**无万能默认**；flag > env > 文件，双空 fail-close） |
| | `api_key` | 凭据直填值（**env 优先**：同名 env 有值则 env 赢）；**永不进 prompt/事件/日志** |
| | `api_key_env` | 持有 API key 的**环境变量名**（缺省 `OPENAI_API_KEY`） |
| | `system` | 系统提示 |
| | `approval` | 审批策略：`deny-all` / `always-allow` / `terminal` / `protocol` |
| | `max_turns` | 单次 Run 最大轮数（0 = 框架默认） |
| | `context_budget` | 上下文窗口预算 token（0 = 框架默认；只用于占比分母） |
| | `allow` | 允许的工具名（**空 = 一切 deny**，fail-close） |
| | `ask` | 需要人审的工具名 |
| | `reasoning_effort` | 思维链档位（`low`/`medium`/`high`/`xhigh`；白名单校验） |
| | `context_rules` | 项目规则文本（AGENTS.md 等；稳定前缀的一部分；空 = 不注入） |
| | `workflow_dir` | 工作流能力包目录（纯数据 JSON；空 = 只用内建 tower） |
| | `goal_ledger` | 是否注入 goal ledger 更新工具（默认 false = 零行为变化） |
| | `multi_agent` | 是否给每个 session 装配 agent 树 + `loop.Agents`（默认 false = 单 agent 零行为变化） |
| | `cors_origins` | CORS 来源白名单（**opt-in**；空 = 关 = 一个 CORS 头都不发；**不支持 `*`**；非法条目**启动即拒**） |
| `[online_table]` | `rows` / `line_tokens` / `level` / `window` / `activity_window` | 在线表注入（K14/K15）：行数 N（0 = 默认 50）/ 每行 token 预算（默认 100）/ 暴露等级（`summary`\|`n`\|`full`）/ level=n 的窗口行数 / **G-a 活跃窗口**（时长串，如 `15m`） |
| `[monitor]` | `cpu_percent` / `mem_mb` / `goroutines` / `interval_sec` | hub 监控阈值（0 = 框架默认） |
| `[context_strategy]` | `kind` / `window` / `l1_pct` / `l2_pct` / `rules_budget` / `common_sense_budget` | 上下文装配：`kind` 空 = `append`（默认）/ `tiered`（window 须 ≥ **256k**）/ L1 目标残留 %（默认 10）/ L2 %（默认 1）/ 规则注入上限（默认 15k）/ 常识区上限（默认 30k） |
| `[graph]` | `enabled` / `max_nodes` / `max_edges` / `max_depth` / `max_results` / `core` / `escalate_reachable` | 图投影（**默认关 = 零图开销**）；上界 0 = 框架默认（10 万 / 100 万 / 8 / 5000 / 50）；`core` 里 `*` 结尾 = 字节前缀 |
| `[sandbox]` | `enabled` / `write_dirs` / `max_active_processes` / `acl_max_objects` / `strict_groups` | 沙箱（**默认关 = 走 local**）；上界 0 = 默认（64 / 20 万）；`strict_groups` = 严格组 SID 模式（代价 = overlapped I/O 全挂） |
| `[mcp]` | `servers` | **数组表** `[[mcp.servers]]`（下见） |
| `[[mcp.servers]]` | `name` / `transport` / `enabled` / `command` / `args` / `env` / `env_from` / `url` / `headers` / `timeout_ms` | MCP client（N1）：`transport` = `stdio`（缺省）/ `http`；**`enabled` 未显式写 = 按 true 处理**（"写了就要连"）；`env`/`headers` 可能含凭据 → **永不进事件/日志**；工具名 = `mcp__<server>__<tool>` |
| `[mcp_server]` | `expose_tools` | MCP **server** 面（B 类）：暴露白名单；**空 = 零暴露**（fail-close）；**只收非 `mcp__` 名**；只走文件配置面（不给 flag） |
| `[[agent_types]]` | `id` / `prompt` / `tools` / `model` / `can_spawn` | 用户自定义 agent 类型：**同名覆盖内建五类**；只在 `multi_agent=true` 时生效（关 = 不建注册表、**不校验** = 零行为变化）；装载期 fail-close（id 空/重名/**保留名 `root`** / `can_spawn` 悬空·自环·成环 / 权限级别非法 / `context_strategy.kind` 越枚举 → 启动即拒并点名） |
| `[agent_types.permission_request]` | `read` / `write` / `exec` | 权限**请求**（H22：实际 = 请求 ∩ 策略上限 ∩ 父级授予）；`read`/`write` = `none`\|`workspace`\|`full`；`exec` = `none`\|`allowlist`\|`full`；**空字段归一为 `none`**（least privilege） |
| `[agent_types.context_strategy]` | `kind` | 该类型子 agent 的上下文策略（W6-A：**显式声明、不继承主 agent**）；`append`\|`stage_scoped`\|`sliding_window`\|`tiered` |
| `[[agent_types]]`（顶层字段） | `share_project_assets` | 项目资产（rules）共享开关（**顶层字段，不在 `[agent_types.context_strategy]` 下**——写错段位 TOML 会静默忽略，故未知键 fail-close 点名） |
| `[[profiles]]` | `name` / `description` / `model` / `reasoning_effort` | 命名 profile（G-i）：**数组表**（不是 map）；重名 → 装载期拒；**空字段 = 不覆盖**（模板语义） |
| `[profiles.provider]` | `base_url` / `api_key_env` | provider 覆盖（**成对**；只存变量名） |
| `[profiles.tools]` | `only` / `except` | 工具白名单覆盖（同 `config.tools` 口径） |
| `[profiles.context]` | `kind` / `window` / `l1_pct` / `l2_pct` / `rules_budget` / `common_sense_budget` | 上下文面覆盖（同 `config.context` 口径） |
| `[[profiles]]`（顶层字段） | `cwd` / `worktree` / `run_id` / `remote_host` | **执行面**（会话创建时进 `SessionSpec`）；`remote_host` = **纯声明**（v1 不实现，只留痕不校验） |
| `[blob]` | `dir` / `max_inline` | 大 payload 外置（D4）：`dir` 空 = 关 = 零行为变化；`max_inline` = 阈值字节（0 = 不外置；**负值非法 = 启动即拒**） |
| `[vectors]` | `enabled` / `top_k` | 向量引擎（RAG，G2d）：**默认关**（关 = 不建库、`vector_search` 不注册、`/api/vectors*` 路由不注册 → 404）；`top_k`（0 = vecmem 默认 10；负值非法 = 启动即拒） |
| `[embedding]` | `model` / `base_url` / `api_key_env` | embeddings 提供者（**框架不算 embedding**）：`model` 空 = 不接提供者 = 零行为变化；`base_url` 空 = 回退 chat 的 `base_url`；**`api_key_env` 显式指定（且 ≠ chat 的）→ 值只从那个变量取，取不到启动即拒**（不静默跨边界回退）；未指定 → 沿用 chat 的凭据（**变量名与值都沿用**） |
| `[documents]` | `enabled` | 文档族投影（G2 第 5 族）：**默认关**（关 = 不建 DocStore、四个端点 404） |
| `[wal]` | `dir` | WAL 预写日志：**空 = 关 = 一个文件都不建、行为逐事件等价**；**WAL 不是事实源** |
| `[telemetry]` | `mode` / `dir` | `mode` = `""`\|`none`（关，默认）/ `file` / `otlp`（**留位 = 装配期拒**）；`mode=file` 时 `dir` **必填**（启动即拒，不静默关） |
| `[budget]` | `max_tokens` | 成本预算闸门（W15）：**默认 0 = 不设上限**（闸门整个不生效、零查询）；非 0 时 per-Run + per-turn 两层检查；**负值非法 = 启动即拒** |

**装载纪律**（`host/config_file.go` `LoadFileConfig`）：
- 文件**不存在** → `(nil, nil)`（"没有配置文件"是合法状态）；**存在但解析失败** → error（fail-close：损坏的配置可能正是用户指望的默认值/凭据来源）。
- **未知键 fail-close（F3）**：TOML 解析器对没被认领的键是**静默忽略** → 装载期点名报错（**错误里只含键名，不回显键值**——凭据安全）。
- 错误里附**合法键清单**（顶层由**反射派生**，永不与结构体漂移）+ 坏键所在那张表的合法键 + 嵌套表位置指引。

**优先级（H15）**：**显式 flag > 进程 env > 配置文件**（`host.Precedence(flag, env, file)` 是唯一实现处；"显式"由 flag 的**默认值不高于文件**保证——否则 deny-all / `OPENAI_API_KEY` 这类默认会永远掩盖配置文件）。
env 面固定：`ALASOL_MODEL` / `ALASOL_BASE_URL` / `ALASOL_EMBEDDING_MODEL` / `ALASOL_EMBEDDING_BASE_URL`。

### 12.2 flag 面主要参数（`alasol serve`）

`cmd/alasol/serve.go`（`registerHubFlags` 是 serve / chat / tui / acp / mcp-serve 复用的共有 flag 面）：

| 组 | 参数（flag 名） |
|---|---|
| 端点与凭据 | `-addr`（默认 `127.0.0.1:8720`）`-model` `-base-url` `-api-key-env` `-reasoning-effort` |
| 审批与工具 | `-approval`（`deny-all`\|`always-allow`\|`terminal`\|`protocol`）`-allow` `-ask` |
| 存储与轮次 | `-db` `-max-turns` `-system` `-checkpoint-every` |
| 在线表（K14/K15） | `-online-rows` `-online-line-tokens` `-online-level` `-online-window` `-online-activity-window` |
| 监控（K9） | `-monitor-goroutines` `-monitor-interval` |
| 上下文策略 | `-context-strategy`（空 = `append`\|`tiered`） |
| 图投影 | `-graph` `-graph-max-nodes` `-graph-max-edges` `-graph-max-depth` `-graph-max-results` `-graph-core` `-graph-escalate-reachable` |
| 工作流 / ledger | `-workflow-dir` `-goal-ledger` |
| 沙箱 | `-sandbox` `-sandbox-write-dirs` `-sandbox-max-active` `-sandbox-acl-max-objects` `-sandbox-strict-groups` |
| 远程 | `-remote` `-remote-host` |
| embedding | `-embedding-model` `-embedding-base-url` `-embedding-api-key-env` |
| 其它 | `-cwd` `-run-id` `-indexer-batch` `-max-session-rows` `-prompt-timeout` `-config` |

**flag 面纪律**：`cors_origins` / `[[mcp.servers]]` / `[[agent_types]]` / `[[profiles]]` / `[telemetry]` / `[wal]` / `[blob]` **只走文件配置面，不给 flag**——它们要么是安全敏感的名单，要么含凭据（命令行会泄进 shell 历史 / ps，H12/H19）。

### 12.3 「0 = 默认、负 = 拒」的口径

| 值 | 口径 |
|---|---|
| `0`（数值） | **= 用框架默认**（不是"关"）：`max_turns` / `context_budget` / `online_rows` / `online_line_tokens` / `online_window` / `monitor_*` / `graph_max_*` / `graph_escalate_reachable` / `sandbox_max_active` / `sandbox_acl_max_objects` / `context_strategy.window` / `context_strategy.l1_pct` / `context_strategy.l2_pct` / `context_strategy.rules_budget` / `context_strategy.common_sense_budget` / `budget.max_tokens`（= 不设上限） |
| **负值** | **非法配置 = 启动即拒**（`host.Config.validate` 逐项判）：`max_turns` / `context_budget` / `online_table.rows`·`line_tokens`·`window` / `monitor.*` / `graph.max_*`·`escalate_reachable` / `sandbox.max_active_processes`·`acl_max_objects` / `context_strategy.window`·`rules_budget`·`common_sense_budget` / `[blob].max_inline`（唯一事实源 = `host.ValidateBlobMaxInline`）/ `[vectors].top_k` / `[budget].max_tokens`；CLI 侧 `blob-gc -retention` / `-grace` 负值 → **退出码 2**；`wal-gc -retention` 缺省 0 → 退出码 2（**必给**，不替你猜） |
| 字符串键 | `""` = 缺省/清除（`[context_strategy].kind=""` = append；`[profiles]` 的空字段 = **不覆盖**） |
| 布尔键 | TOML bool 零值是 false → "没写"与"写了 false"**不可区分**：MCP 的 `enabled` 因此用**指针**（没写 = 按 true）；其余布尔键靠"默认关"语义对齐 |

---

## 13. 工作流与扩展面

### 13.1 workflow 能力包（W1）

| 事项 | 口径 | 出处 |
|---|---|---|
| 本质 | **能力包**（哲学 + agent_types + context_strategy + gates/grants），**不是流程、非强制** | 07 **W1** / **W2**；`contracts/INVENTORY.md` §2（`workflow.activated`） |
| 触发 | 三条：显式 `/workflow <id>` / **能力包自己声明的 slash 命令**（如 tower 的 `tower`——在 chat/TUI 里是**真能敲的入口**）/ 事件触发 | 07 **W3**；`host/workflow.go` `WorkflowCommandTrigger` |
| 注入 | 激活只把**哲学文本**注入 system prompt 供主 agent 选择；框架**不校验**编排序列（主 agent 自决，W21） | 07 **W3** / **W21** |
| 事件 | `workflow.activated`（`workflow_id` + **实际敲出来的** `command` + `philosophy` 原文）/ `workflow.milestone`（`workflow_id` + `milestone` 开放字符串 + `passed` + 可选 `note`） | `contracts/INVENTORY.md` §2 |
| 里程碑 | 里程碑 = 框架确认的**验证通过点**（合并前仿 CI 检查通过 / critic→reviewer 循环收敛）；`passed=true` 时可联动阶段硬边界 | 07 **W14**；`host/workflow.go` `TriggerMilestone` |
| 权限 | `permission_request` / `grants` 是**请求**；实际 = 请求 ∩ 策略上限 ∩ 父级授予；**workflow 规则只可收窄** | 07 **H22** / **H24** |
| 目录 | `[workflow_dir]`（纯数据 JSON）；空 = 只用内建 tower | `host/config_file.go` |

### 13.2 goal ledger（J19 / X6）

- **在上下文之外**的独立持久化：`ledger.updated` durable 事件（全量快照）；**当前态 = 索引器物化的投影**（LWW，seq 守卫）。
- 工具 `ledger`（`core/loop/ledger.go`）：四张清单 `goals` / `constraints` / `completed` / `pending`；**增量更新**（未给的字段保持原值）；`goals`/`constraints` = 关键字段（立即落盘），`completed`/`pending` = 过程字段（turn 末合并）。
- **级联 C 案**：压缩输入 = goal ledger + 被压区间全文，**永不读他阶段摘要** → 切断级联链。
- 开关：`-goal-ledger` / `goal_ledger = true`（默认关 = 零行为变化）；**仍受策略链 deny-all 约束**。

### 13.3 `/btw` 旁支 fork（K29）

- fork **只附带上下文**（对话历史，`FoldConversation` 同源；**带条数/长度上限 + 截断告知**：16 条 / 6000 字符，含工具参数）；**不复制 ledger / gate 全套**。
- **历史口径**：主轮次**不带**历史、/btw **带**历史（主 session 每一轮只带本轮 prompt）。
- envelope 归属 fork session → **主 session 事件流零新增**；**结果只给用户、不回灌主上下文**。
- 事件 `btw.forked`（`source_session_id` + `question`；**不含主 session 的对话内容**/凭据——问题原文本身是对话内容）。
- **四面过滤**：在线表 / peers / presence / discover 都排除 fork 族。
- **预埋钩子**：`SetBtwPromoteHook` + `btw.forked` 事件 = 扩展点（**只埋点不实现**：提成一流 session 需 gate 归属 / 在线表可见性 / agent 树 / UI 多方面微调）。

### 13.4 MCP client（N1）+ MCP server 暴露（v4-5）

| 方向 | 口径 | 出处 |
|---|---|---|
| **client**（连外部 server） | 外部工具注册进**同一个注册表**（同一 X1 三段路径、同一策略链、同一可见性）；工具名 = `mcp__<server>__<tool>`；默认关（无 `[[mcp.servers]]` = 不装 MCP）；deny-all 默认（`allow` 里列 `mcp__<server>__<tool>` 才放行） | 07 **N1**；`runtime/mcp/manager.go`；`host/mcp.go` |
| **server**（暴露本 harness） | `alasol mcp-serve`：stdio 传输，按**白名单**把 harness 工具面暴露成 MCP tools；**fail-close**：空白名单 = 零暴露（`tools/list` 空集、任何 `tools/call` 命中 unknown tool）；**不收 `mcp__*`**（client 侧已聚合过的外部工具不二次暴露，白名单点名也拒）；**注册表 State 参与暴露与执行判定**（disabled 不暴露 + 调用前复查，防"list 后才被停用"）；非法 MCP 工具名不挂载；**审批闸不适用**（approvable 工具如 bash 被点名即**零审批**执行）；stdout 只走 MCP 帧、日志走 stderr | 07 **E2** v4-5 落地回填；`runtime/mcpserver/` |

### 13.5 extensions 注册表（E3 / E5 / E6）

| 事项 | 口径 | 出处 |
|---|---|---|
| 元数据 | `kind` / `schema` / `capabilities` / `priority` / `reloadable` / `health`（D17） | `contracts/extension.go` |
| 状态机 | `registered`（初始）→ `enabled`（能力已获准）→ `disabled`（软停用，可再 Enable） | `core/registry/registry.go` |
| 能力裁决（E3） | `Enable` 时逐个 capability 交裁决器；**任一非 allow → 先撤销已 enabled 再拒**（fail-close）；裁决**在锁外**；`ErrStale` 挡住"裁决期间换实例"与"裁决期间状态被写过"（**状态版本号 CAS**，含同值 Disable） | 同上 |
| 失败隔离（E5） | 裁决方 panic → 一律 deny；扩展/裁决异常**不得带崩 core** | `core/registry/registry.go` `adjudicate` |
| 热重载（E6） | 进程级可热插拔；接口实现需重启（换 Provider / Store 通常伴随重启） | 07 **E6** |
| 观测面 | `GET /api/extensions`（全 kind + Health；空能力列表归一为 `[]`；**只读**） | `host/extensions.go` |
| 管理面 | `POST /api/extensions/{name}/enable|disable`（**无鉴权**，拍板交 SSH/系统）；**停用进程内有效、重启失效**（不落事件流，持久化挂账） | 07 **E2** 缺口补全回填 |
| 留位的 kind | `compaction` / `assembler` / `scheduler` **只在开放取值清单里占位，未建机制**（14 第 5 项拍板"extension runtime 重点不是新机制"） | 07 **E2** |

### 13.6 vectors / RAG（G2d）

- `runtime/store/vecmem`：Retriever + VectorIndex 两面同一实例；余弦相似度 top-k、同分按 ID 升序定序、**K=0 默认上限 10**、负 K / 空 Embedding / 维度不符 → `ErrBadQuery`、**首条维度锁定**（库空即解锁，Rebuild 后可换维度）。
- 条目 id = `Crockford26(SHA-256("vec\x00" ‖ u64be(len(ref)) ‖ ref ‖ u64be(len(text)) ‖ text)[:16])`（两段各带 8 字节大端长度前缀 → 单射，含 NUL 不撞 id）。
- Filter = 对条目 payload JSON **顶层字符串字段**字节精确匹配（AND；键缺失 / 值非字符串 / 坏 JSON → **不命中**，故不报 `ErrUnsupported`）。
- `truncated` 是**每条命中**上的诚实信号（被 K 截断时全部置位）。
- **不持久化**（可整体重建的缓存；`SourceRef` 指回事件流，丢了不丢事实 F5）。
- **框架不算 embedding**——`EmbeddingProvider` 是宿主槽位，生产 nil = 工具不注册 / 端点 503 = 零行为变化。
- 工具 `vector_search` + HTTP `GET /api/vectors/search` / `POST /api/vectors`（单批 ≤ **512** 条）。

### 13.7 documents（G2 第 5 族）

- `runtime/store/sqlite/docs.go` `DocStore`：**事件库的同库单表**（SQLite `docs` 表，`(collection,key)` 主键），表由 `EnsureSchema` 建——**要换实现是换存储的事**（G2b），消费方零改动。
- 四个端点：`GET /api/docs/{collection}/{key}` / `GET /api/docs` / `POST /api/docs/upsert` / `POST /api/docs/delete`（**未启用时路由不注册 → 404**）。
- 写**只经宿主 ingest 入口**（经 `Transact` 收口事务，G17 单写者）；**整批拒绝**（版本冲突 → 409 `doc_conflict`）；复合键结构体化防 NUL 撞键。
- 文档是**投影**，事件流才是事实源（F5）。

---

## 14. 已知挂账与「有意不做」（移植时别当 bug 修、也别顺手丢）

> 全部出自 `07` 尾注的挂账段、`15` §6 的缺口表、以及各 `cmd` / `host` 文件头的"已知缺口"标注。**移植方照抄这些口径**（别默默"修好"——它们多数是**有意的取舍**，有理由）。

### 14.1 跨进程 / 拓扑类

| 挂账 | 内容 | 出处 |
|---|---|---|
| **跨进程 Claim 不可探测** | `Claim` 登记是**发现表的进程本地状态**（与 live 同款，不进 Snapshot）→ **跨进程冲突检测不到**。预期拓扑 = **一库一 hub**；跨进程互斥留 **durable claim**（F7 新事件 kind，未做） | 07 **G25** 落地回填挂账；`host/doc.go:131-136` |
| 多 hub 共享库 | 读面串台、事件无 hub 归属；契约级根治（`hub_id` + 跨进程路由）**不做** | 07 **G25** 口径段 |
| **remote_host 纯声明** | profile 的 `remote_host` **只留痕、不校验、不换 hub 级执行环境**（per-session 换执行环境属后置）；与 hub 级 `-remote` 不符时只 **WARN**（不拒） | `host/http.go` `serveSessionCreate`；`host/worktree.go` 文件头 |
| 跨容器 waiting 不成立 | standby 的 waiting 态是**进程本地**的（一库一 hub 内有效） | 07 批次2 落地回填挂账 |

### 14.2 历史 / 折叠类

| 挂账 | 内容 | 出处 |
|---|---|---|
| **FoldConversation 孤儿 tool_call** | 有 intent 无结果的 assistant tool_calls，resume / fork 的整场折叠在多 agent session **必踩**（OpenAI 端点 400）——**既有缺陷，另刀修**（不是移植要修的） | 07 **F1** 落地回填挂账 |
| **主轮次历史无上限** | append 基线**无截断上限**（封顶走 tiered / G-h） | 07 **F1** 落地回填挂账 |
| fork / CLI resume 部分口径不继承 | `/btw` fork **不继承**父 session 的 `tools` 白名单与 `context` 面（fork = 新 session 继承 hub 默认）；**CLI resume 挂不起上下文策略**（`kind` 覆盖在 CLI 不生效，窗口等参数生效——它换的是占比分母） | 07 批次3 / 批次4 挂账 |
| resume 续跑折叠不区分 agent 视角 | `alasol resume -continue` 折叠**整场 session** 的对话、**不区分 agent 视角**（子 agent 的对话也会进入上下文；按 agent 过滤是后续增强，尚未实现） | `cmd/alasol/main.go` usage；`cmd/alasol/resume.go` |

### 14.3 事件 / 幂等类

| 挂账 | 内容 | 出处 |
|---|---|---|
| **客户端重试落第二条等价事件** | WS / HTTP 提交侧**没有幂等键**（`command.submit` 不幂等；`approval.respond` 按 `payload.request_id` 幂等）→ 客户端重试会落**第二条等价**事件（需契约幂等键） | 07 批次2 挂账；批次3 / 批次4 沿用 |
| 重试落等价事件（观测侧） | provider 层重试走 emit 通道（`provider.retry` → WAL `turn.retry`），**纯观测不进对账** | 07 v4-4-B 挂账③（已落地） |
| standby 交付集合只增不减 | 交付集合只增不减、并发复查有窄窗 | 07 批次2 挂账 |
| standby 事件早到窗口极窄 | 事件面早到窗口实际**微秒级**（端到端几乎撞不上），单元级夹具固化 | 07 批次3 收口挂账 |
| 富表达式 | 历史事件检索 / 字段级条件属**富表达式**（G-j v1 只做 kind + scope） | 同上 |

### 14.4 「有意不做」清单（拍板项，移植时别当缺口补）

| 项 | 拍板内容 | 出处 |
|---|---|---|
| **G-g 运行时权限修改面** | **不做**——`config.policy` 字段不实现；权限维持两层静态 | 07 批次3 收口；`15` §5 |
| **兄弟互聊（C）** | **不开** C：兄弟 subagent 互相不可见；解法 = A 共享契约前置 + B 共享黑板 | 07 **K22** |
| **通信预算 / 配额** | **不做**——靠协调者居中（`coordinator` type）+ 基础兜底（turn 超时 / 工具超时 / 异常向上冒泡） | 07 **K23** |
| **分布式存储 / 分布式发现** | **不做**——跨机走 ACP 端点，鉴权交 SSH | 07 **K19** |
| **scheduler 扩展点** | 并入工作流语义，**不单建** | 07 **E2** |
| **解释型扩展运行时（B4）** | **继续保留候选、暂不选型**（Starlark / CEL / goja / WASM） | 07 **B4** |
| **`stage_scoped` / `sliding_window` 上下文策略** | 枚举**保留未实现**；宿主只实现 `tiered`，其余 kind 由宿主一律按 append 处理（core fail-close 收口） | 07 **J8**；`host/config_file.go:357-362` |
| **otlp telemetry** | **留位**——`Emit` 恒返 `ErrUnsupported`，装配期拒（不静默降级） | 07 **D19** |
| **assembler / compaction / scheduler 三个 extension kind** | 只在开放取值清单占位，**未建机制** | 07 **E2** |
| **审批鉴权** | **有意无鉴权**（loopback 同权），收紧记 v4 | §9.5 |

### 14.5 其他已知缺口（照抄，不修）

| 挂账 | 内容 | 出处 |
|---|---|---|
| blob 折叠侧不看 envelope | 折叠只读 payload → 续跑看到的是摘要 / 占位（占位文本里含 blob id） | 07 **D4** |
| blob 跨进程锁 | 锁只在**同进程**内互斥，跨进程唯一防线是 Grace | 07 **D4** 缺口补全回填 |
| blob 崩溃窗口 | 引用与内容**最终一致**（引用先落、内容后到），续跑按 `binary_ref` 补取 | 同上 |
| WAL 掉电尾部 | 机器掉电时尾部未 fsync 的 delta 可能丢（逐条 fsync 会把打字机效果卡死，**有意取舍**） | 07 v4-4-B 挂账② |
| blob 外置时计划噪音 | standby / fork 场景 blob 外置会多出计划记录 | 07 批次2 挂账 |
| telemetry model 未随覆盖 | telemetry record 的 model 字段不随 per-Run 覆盖更新 | 同上 |
| standby 可被白名单藏掉 | session 工具白名单能把 `standby` 藏掉（**预期语义**） | 07 批次3 收口挂账 |
| fork 预算口径 | `RunBtw` 只查**主 session** 的账；旁支花费记在 fork session 名下、不在闸门口径内 | `host/budget.go` 文件头 |
| worktree 工具级隔离 | 工具仍跑进程 cwd（worktree 登记表**不持久化**） | 07 v1-S5 挂账段 |
| `tui` 自动化交互测试 | 未做 | 07 v1-S5 挂账段 |
| **`ui` 依赖规则的窄例外** | `ui/doc.go` 自述"只依赖 contracts，不 import core"，但 `ui/tui/approver.go` 为实现 `core.Approver` 桥 import 了 core（接口断言）——**如实标注，移植时重新决定去留** | §0.2 |
| 二进制打包 A/B/C | **二进制打包 A/B/C**（GUI 页面嵌入与否）：待拍板（建议 C） | `14-v4-plan.md:34` |

---

## 15. 档案指针

| 文档 | 内容 | 移植时怎么用 |
|---|---|---|
| `docs/research/07-design-review-checklist.md` | **设计决策总表（唯一裁决来源）**：**280 条**编号决策（✅ 已定 206 / ★ 待议 0 / · 默认通过 74，共 280） | **移植验收的逐条对照源**：本清单每条硬约束都标了 07 的条目号；移植方按 07 全表逐条对照 |
| `docs/research/11-construction-plan.md` | 施工计划（阶段划分 + 硬验收 + 20 条施工期陷阱清单） | 移植排期与"别踩的坑" |
| `docs/research/12-roadmap.md` | 版本路线图（v1–v4，每条给 07 决策点引用） | 判断哪些能力属于"未来版本" |
| `docs/research/13-context-system.md` | 上下文系统（两级压缩 L1/L2、注入区预算、级联 C 案） | §4 / §13 的上下文面细节 |
| `docs/research/14-v4-plan.md` | v4 排期与下一阶段段 | 未做项的归属 |
| `docs/research/15-session-per-prompt-design.md` | **session / per-prompt 设计定稿**（在线表三态 / 默认接管 / per-prompt 配置 / standby / 权限面拍板 / 缺口批次表） | §7 全面的设计依据；§6 末尾的缺口批次表 = 施工排期 |
| `contracts/INVENTORY.md` | **跨边界契约唯一清单源**（11 个契约 + §2 kind→payload 全表 28 条 + §2b transport kind 子集 + §3 生成约定 + §5 契约变更记账） | §2 的权威出处；**kind 全表以它为准** |
| `contracts/schema/*.json` | 12 份手写 JSON Schema（2020-12） | 契约的机器可读源 |
| `README.md` | 项目状态与文档索引 | 入口 |
| `docs/research/09-package-boundaries.md` | 包边界（存在理由 / 变更轴 / 依赖硬约束） | §0.2 / §0.3 的依据 |
| `docs/research/08-identity-model.md` | 身份与 ID 设计（13 个 id 命名空间、ULID 选型、字符集纪律） | §2.1 的 id 纪律依据 |
| `docs/research/06-skeleton-design.md` | 骨架规格（contract-first 分层、契约、扩展点地图、工作流模型、上下文策略） | 架构骨架的原始规格 |

---

## 附：本清单的核对方法（给移植方）

1. **五张全表逐条对照**：事件 kind（28）/ HTTP 端点（48）/ 工具（12）/ CLI 子命令（12）/ TOML 键（94）。任一条对不上 = 缺口。
2. **硬约束逐条打勾**：§1 全绿才算"移植成功"，一红 = 移植失败。
3. **抽查方式**（三个例子）：
   - 随机抽一个 kind → 在 `contracts/schema/event.json` 的 `$defs.Event.x-kind-payload` 里查得到 + durable 列对得上；
   - 随机抽一个端点 → 在 `host/http.go` 的 `reg(...)` 里查得到 + 方法/路径逐字一致；
   - 随机抽一个工具名 → 在 `runtime/tools/` / `core/compaction/` / `core/loop/` / `host/` 里有**同名常量**（工具名四处一致：描述 / 注册表 key / 模型调用 / 事件）。
4. **不要凭印象补**：仓里没有的东西不要写进移植清单（例如"审批鉴权""分布式发现""兄弟互聊"都是**有意不做**，见 §14.4）。

---

本清单生成于 2026-11-05，基线 commit f52d7d6
