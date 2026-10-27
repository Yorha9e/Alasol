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
| 3 | **Transport 消息** | `schema/transport.json` | 客户端/服务端通信 envelope（`ws+ndjson` / `stdio` / `inproc` 同一契约）：订阅、命令提交、审批转发（N4/N6） | NDJSON 行式 |
| 4 | **Extension 元数据** | `schema/extension.json` | `kind` / `schema` / `capabilities` / `priority` / `reloadable` / `health`（D17） | — |
| 5 | **Tool.schema** | `schema/tool.json` | 工具描述：`schema` / `side_effect`（none/read/write/destructive）/ `cancelable` / `approvable` / `idempotent` / `replay`（D12/D22） | `schema` 字段**值本身是 JSON Schema**（元嵌套）→ codegen 当原始 JSON 处理 |
| 6 | **Tool 返回值** | `schema/tool-result.json` | 工具返回值：`graph_edges: [{src,dst,kind}]` / `taint` / 可重放标记（D22/G22/Q7） | 跨边界：MCP 与进程级扩展都要传 |
| 7 | **Policy 规则** | `schema/policy.json` | 规则表达式（配置文件形态）+ 判定结果 `Decision`：`allow`/`deny`/`ask`，ask 携带 `suggested_rules`（D13/H5） | workflow 侧规则**只可收窄**（H5/H24） |
| 8 | **Store 持久化格式** | `schema/store.json` | 事件日志持久化/导出格式（NDJSON 事件记录）（G 系） | 大 payload 走 `binary_ref` 外置（D4） |
| 9 | **Workflow** | `schema/workflow.json` | 工作流定义（纯数据）：`philosophy` / `agent_types[]` / `context_strategy` / `gates[]` / `grants[]`（`06` §4.1） | `permission_request`/`grants` 是请求，实际 = 请求 ∩ 策略上限 ∩ 父级授予（H11/H22） |
| 10 | **ContextSnapshot** | `schema/context-snapshot.json` | 上下文构成清单：`parts[]`（source/kind/entries/tokens/state/origins/summary_ref）、`state ∈ verbatim/compacted/dropped/deferred`（`06` §4.2） | 每轮一条 durable 事件 `context.assembled` |
| 11 | **Approval 消息** | `schema/approval.json` | 审批请求 / 答复（跨进程：TUI / GUI / 远端）（D21 补入） | **无人应答 = deny，不得挂起**（D14/H8，fail-close） |

## 2. 事件 core kind 子集（随 Event 契约定稿）

kind 是开放字符串（D6），core 定义核心子集。**本阶段必须定稿 payload 形状的**：

| kind | durable | 说明 |
|---|---|---|
| `tool.call.intent` | true | **三段事件之①**（X1）：tool_call_id / 参数 / idempotent / replay 标记 / 外部可核验锚点（如 commit sha） |
| `tool.call.completed` | true | **三段事件之③a**（X1）：结果（Tool 返回值契约） |
| `tool.call.failed` | true | **三段事件之③b**（X1）：失败（错误为普通事件，M1/M8） |

写路径铁律（X1）：`intent` **durable 落盘** → 执行 → `result` durable 落盘。**有意图无结果 = 未知结果，不盲目重跑**。

其余 core kind（`context.assembled` / `policy.decided` / `ledger.updated` / `board.written` / `checkpoint.created` / `session.resumed` 等）随对应阶段补 payload schema，envelope 复用 `event.json`。

## 3. 生成与编码约定（schema 与 codegen 共同遵守）

- **演进铁律（F7）**：只增不删、不改义；弃用字段标 `x-deprecated`；envelope 带版本字段 `v`
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

---

## 相关

- 源头裁决：`docs/research/07-design-review-checklist.md`（D1/D2/D21/D22、F3/F7/F8、S 系、X1）
- 形状细节：`docs/research/06-skeleton-design.md`（§3 契约、§4.1 Workflow 示例、§4.2 ContextSnapshot 示例）
- ID 纪律：`docs/research/08-identity-model.md`
- 施工与验收：`docs/research/11-construction-plan.md`（阶段 1 节）
