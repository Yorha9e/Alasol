# 骨架设计：Contract-first Extensible Harness

> 目标：搭一套**可自定义、可拓展**的骨架，在它之上向任意方向扩展。
> ACP 不是目标之一（v0 不做）。当前阶段只解决"基础打得好不好"。
> **验收标准（酸测试）**：把任意一个扩展点的实现整个换掉，**core 代码一行不改**。做不到就是骨架没立住。

---

## 0. 骨架的评判标准

功能型项目的验收是"能干活"；骨架型项目的验收是：

| 标准 | 含义 |
|---|---|
| **可替换性** | 换 provider / 换存储 / 换沙箱 / 换 UI，core 不动 |
| **可扩展性** | 加一个新能力（工具、策略、面板、端点），不需要改已有代码 |
| **契约中立** | 契约是**数据格式**（JSON Schema），不是 Go 类型 → 前端可用任意语言 |
| **多客户端就绪** | core 不假设"只有一个前端""只有一个进程"；hub 是**加法**不是重写 |
| **失败隔离** | 任何扩展崩溃都不带崩 core；任何子系统缺席都有安全默认 |
| **可观测性内置** | 事件流一次实现，UI / 存储 / 遥测 / 评测 / 回放五处受益 |

---

## 1. 三条不可协商的原则

### 1.1 一切皆事件
Agent 的每一步——模型开始输出、工具被调用、策略判定、上下文压缩、审批请求——都是**事件**。事件流是唯一事实源。

推论：不需要额外的"追踪系统"，回放、调试、成本统计、评测全部是事件流的消费者。

### 1.2 core 不知道任何实现
core 只依赖 `contracts` 包。存储、进程、沙箱、provider 的具体实现全部在 `runtime`，由 `host` 在装配时注入。

Go 惯例强化：**接口由消费方（core）定义，实现由 runtime 提供** → core 永不 import runtime。

### 1.3 扩展点是设计产物，不是事后补丁
每个扩展点在设计时就定义好：契约、schema、能力声明、生命周期钩子、热重载语义、失败语义。

没有明确定义的扩展点 = 将来会变成 `if provider == "xxx"` 的分支。

---

## 2. 分层与依赖方向

```
                    ┌─────────────────────────────┐
  L4  UI / Client   │  TUI │ GUI │ IDE │ HTTP API │ 脚本 │
                    └──────────────┬──────────────┘
                                   │ 只依赖 contracts
                    ┌──────────────▼──────────────┐
  L3  Host          │  hub daemon │ CLI │ 组合根 │ 传输 │
                    └──────────────┬──────────────┘
                                   │ 装配
        ┌──────────────────────────┴──────────────────────────┐
        │                                                     │
  ┌─────▼──────┐                                       ┌──────▼──────┐
  │ L1 Core    │  loop / bus / registry / policy / ctx   │ L2 Extension│
  │            │                                        │    Host     │
  └─────┬──────┘                                        │ 加载/生命周期│
        │ 实现 core 定义的接口                            │ 能力裁决    │
        │                                               └─────────────┘
        │
  ┌─────▼──────────────────────────────────────────────┐
  │ L0 Runtime  store / process / sandbox / provider / net │
  └────────────────────────────────────────────────────┘
        ▲
        │ 只依赖 contracts
  ┌─────┴──────────────────────────────────────────────┐
  │ contracts   纯数据契约 + JSON Schema（跨语言、无逻辑）  │
  └────────────────────────────────────────────────────┘
```

**依赖规则（硬约束）**：
- `contracts` 依赖 **零**（不 import 任何内部包）
- `core` → `contracts`（仅此一项）
- `runtime` → `core`（实现接口）+ `contracts`
- `host` → `core` + `runtime`（唯一做装配的地方）
- `ui` → `contracts`（**不 import core**，只消费事件流 + 调命令）

> `ui` 不依赖 `core` 是刻意的：UI 通过协议/命令与 core 交互，这样 TUI / GUI / IDE 插件可以用不同语言实现。

---

## 3. 契约清单（骨架的真正内容）

`contracts` 包内只有类型与 schema，无逻辑。全部导出 JSON Schema（`contracts/schema/*.json`）。

> **唯一清单源**：`contracts/` 维护跨边界契约的唯一清单（见 D21）；其他文档只**引用不重抄**，**计数由清单生成**。

| # | 契约 | 要点 |
|---|---|---|
| 1 | `Event` | 唯一事实流。`kind`（命名空间点分，如 `tool.call.started`）、`seq`、`ts`、`session_id`、`agent_id`、`payload`（JSON） |
| 2 | `Command` | 意图。→ 经 core 处理后产生 Event |
| 3 | `Context` | 装配后的模型上下文（provider 无关的中间表示） |
| 4 | `Model` / `Provider` | 模型能力描述与 provider 能力声明，**不含任何 SDK 类型** |
| 5 | `Tool` | `schema`、`side_effect`（none/read/write/destructive）、`cancelable`、`approvable`、执行器 |
| 6 | `Policy` / `Decision` | 判定结果 `allow` / `deny` / `ask`；`ask` 携带 `suggested_rules` |
| 7 | `Approver` | 人审抽象。**无人应答必须返回 deny，不得挂起** |
| 8 | `Store` / `Projection` | 事件追加 + 视图注册。**接口在 core，实现在 runtime** |
| 9 | `Session` / `Agent` | 生命周期句柄（创建/挂起/恢复/终止） |
| 10 | `Extension` | 扩展注册元数据：`kind`、`schema`、`capabilities`、`priority`、`reloadable` |
| 11 | `Transport` | 客户端/服务端通信。`inproc` / `ws+ndjson` / `stdio` 同一接口 |
| 12 | `Telemetry` | Sink 契约（OTLP / 文件 / 丢弃） |
| 13 | `Workflow` | **工作流定义**（纯数据）：哲学 / agent_types / context_strategy / milestones / gates / grants |
| 14 | `ContextSnapshot` | 上下文构成清单（谁贡献了多少 token、什么被压过）→ 上下文可见性 |
| 15 | `ToolResult` | **工具返回值契约**（含 `graph_edges`、`taint`、可重放标记） |
| 16 | `Approval` 消息 | 审批请求 / 答复（跨进程：TUI / GUI / 远端） |

### 3.1 事件的两种语义（✅ 已定，F8）
- **流事件**（`model.delta`、`tool.output.chunk`）：`durable=false` → **不入库、不占 seq**，总线直发订阅者
- **事实事件**（`tool.call.completed`、`policy.decided`、`context.compacted`）：`durable=true` → 必须持久化、可回放

用 `Event.durable: bool` 区分，**同一条流**；**回放对象只有事实事件**。

### 3.2 跨边界 vs 进程内（✅ 已定，D1/D2）

**不是所有契约都需要中立。** 按"数据会不会跨出进程 / 语言 / 时间"分两类：

| 类别 | 契约 | 要求 |
|---|---|---|
| **跨边界**（清单见 D21） | `Event`（落盘 + 远端 UI）、`Command`（外部提交）、`Transport` 消息、`Extension` 元数据、`Tool.schema`（模型看 + MCP）、`Policy` 规则表达式（配置文件）、`Store` 持久化格式、**`Workflow` 工作流定义（用户/agent 可写）** | **手写 JSON Schema 优先 + codegen Go 类型**；schema 是唯一真相 |
| **进程内**（8 个） | `Provider` `Store` `Tool` 执行器 `Approver` `Sandbox` `Scheduler` `CompactionStrategy` `ContextAssembler` `Telemetry` | 纯 Go interface，不 JSON 化 |

### 3.3 硬规则（✅ 已定，D1/D2）
- **可选字段一律用指针 + `json:",omitempty"`**（Go 零值 ≠ JSON 缺失，契约语义不能漂移）
- 枚举用命名类型 + 常量；union 用接口 + `MarshalJSON`/`UnmarshalJSON` 包装
- 生成器**自写受限子集**（禁 `$ref` 循环与复杂组合），约 300–500 行
- CI 纪律：schema 变更必须重新生成，生成物入库，CI 校验"再生成 diff 为空"
- 进程内接口的 Go 类型**不得**直接用于跨边界序列化

### 3.4 序列化技术选择
- **JSON Schema + NDJSON**（不是 protobuf）
  - 理由：可读、调试友好、与 ACP/MCP 生态一致、任意语言可实现、前端用 TS 时零学习成本
  - 代价：体积与解析开销 → 预留 `Event.binary_ref` 指向外部 blob（大工具输出）
- **契约是数据不是 Go 类型**：跨边界那 7 个由 schema 定义、由 codegen 产出 Go 类型；进程内接口用原生 Go


---

## 4. 扩展点地图（"可自定义可拓展"的具体含义）

每个扩展点统一元数据：**能力声明**（安装时申报要什么权限，运行时由策略引擎裁决）、**优先级**、**健康状态**、**可热重载标识**。

| 扩展点 | 契约 | v0 状态 | 典型实现 |
|---|---|---|---|
| Provider | `Provider` | 契约 + Mock 实现 | Anthropic / OpenAI / Google / 自建网关 |
| Model routing | `RoutingPolicy` | 契约 | 按任务/成本/延迟选模型 |
| Tool | `Tool` | 契约 + 2 个内置（read/write） | bash/glob/grep/MCP/自定义 |
| Context assembler | `ContextAssembler` | 契约 | 项目规则/skills/记忆注入 |
| Compaction | `CompactionStrategy` | 契约 | 摘要 / 指针 / 外部记忆 |
| Policy | `Policy` | 契约 + 规则链实现 | 规则/角色/环境化 |
| Approver | `Approver` | 契约 + deny-all 实现 | TUI / GUI / 自动审批 |
| Sandbox | `Sandbox` | 契约 | seatbelt / bwrap / Win32 原生 / 容器 |
| Storage engine | `Store` | 契约 + SQLite 实现 | bbolt / pebble / 远端 |
| Projection | `Projection` | 契约 | session 列表 / 成本视图 / 评测视图 |
| Scheduler | `Scheduler` | 契约 | 并发配额 / 优先级 / 背压 |
| Workflow | `Workflow`（**跨边界数据契约**） | 契约 + 阶段上下文装配 | 指导性路线图 + 每阶段能力授予（**非 DAG 执行器**） |
| Panel | `Panel` | 仅契约 | TUI 面板 / GUI 组件 |
| Endpoint | `Endpoint` | 仅契约 | HTTP / gRPC / 自定义方法 |
| Transport | `Transport` | 契约 + inproc 实现 | ws+ndjson / stdio |
| Telemetry | `Telemetry` | 契约 + noop 实现 | OTLP / 文件 |

**16 个扩展点 = 16 个"可以动"的地方**。骨架的价值就在这张表：方向未定，但接口已定。

---

## 4.1 工作流模型（能力包 / 设计哲学 / 主 agent 自编排）

**不是 LangGraph 式的静态 DAG 执行器，也不是"阶段序列"。** 工作流是**能力包的提供与批准**——它把一套**工作哲学**、一组**子代理类型**、一套**上下文管理方式**、若干**关键阶段部件**和一批**预批准**交给主 agent，之后**由主 agent 自行编排**。

### 五条定位
1. **工作流 = 能力包 + 批准**，不是流程
   - 交付给主 agent 的是"一套处理理论 + 能调用的能力 + 预先批好的权限"
   - 不是"照着走完的步骤列表"
2. **阶段 = 设计哲学**，不是强制序列
   - 例：`plan → coder → critic → review(可选)` 是**流程管理理论**，交给 agent 参考
   - 框架**不规定**"这个阶段必须派出哪些 subagent"
3. **触发 = 放进 system prompt**
   - 用户用 slash 命令或其他显式方式触发 → 工作流的哲学文本进 system prompt，**供主 agent 选择**
   - agent 据此生成初步计划；没问题就一轮走完
   - 有问题 → **在后续追加一个 loop**（如 review-critic）继续解决 → 工作流**可组合、可追加**
4. **上下文管理方式是工作流的一部分**
   - 某些工作流用**特殊的上下文管理方式**（区别于默认的追加式）来辅助
   - 例：阶段性独立上下文 + 交接摘要、滑动窗口、大产物外置
5. **工作流可自带验收目标与部件**（里程碑只是其中一例）
   - 工作流可定义自己的**验收目标**：自写 CI 流程、写屏障、合并前检查……
   - 实现上分两类：
     - **同步门禁类**（写屏障、权限门）→ 必须是**数据驱动的策略规则**，进程内、同步路径生效
     - **异步任务类**（CI、验收、构建）→ **进程级组件**，可热插拔、可替换
   - `advance` / `milestone` 类工具：① 展现进度 ② 标记重要阶段 ③ **触发关联部件**；**可做可不做**

### 契约形态（跨边界 JSON Schema，纯数据）

```json
{
  "version": 1,
  "id": "build",
  "command": "build",
  "philosophy": "你正在执行一次构建型任务……（方法论文本，进 system prompt）",
  "agent_types": [
    {
      "id": "planner",
      "prompt": "你是计划者……",
      "tools": ["read", "glob", "grep"],
      "permission_request": { "read": "workspace", "write": "none", "exec": "none" },
      "model": "default"
    },
    {
      "id": "coder",
      "prompt": "你是实现者……",
      "tools": ["read", "edit", "write", "bash(test:*)"],
      "permission_request": { "read": "workspace", "write": "workspace", "exec": "allowlist" },
      "model": "default"
    }
  ],
  "context_strategy": { "kind": "append", "params": {} },
  "gates": [
    { "id": "write-barrier", "kind": "policy", "rules": ["deny:write(**/*.lock)", "deny:write(migrations/**)"] },
    { "id": "verify", "kind": "component", "component": "ci.check", "params": { "cmd": "make ci" } }
  ],
  "grants": [
    { "tool": "read", "scope": "workspace" },
    { "tool": "edit", "scope": "workspace" }
  ]
}
```

`gates[]` 是广义的验收目标与门禁：`kind: "policy"` = 同步规则（数据）；`kind: "component"` = 进程级异步部件（可热插拔、可替换）。

### 权限如何落地（与 H11 对齐）

`agent_types[].permission_request` 与 `grants` 都是**请求**，不是授予。实际生效 scope：

```
实际授予 = 请求 ∩ 策略上限（managed/项目规则） ∩ 父级已授予
```

- 落在已授予 scope 内的工具调用**直接放行**，不走逐次 ask（避免审批疲劳）
- scope 外的调用走**完整策略链**（可能 ask / deny）
- 子 agent 的 scope 永远 ⊆ 父级 —— 工作流也不能突破（H11 生效）

所以"用户自定义子代理的工具列表、提示词、权限范围"是成立的：**用户能自由声明想要什么，实际拿到的由策略引擎裁决交集**。

### 动态性与可组合
- 工作流**可追加**：会话中再触发一个（如 review-critic loop）→ 追加进 system prompt 与能力池
- 主 agent **自行编排**：开几个 subagent、用哪个 agent_type、干什么、并行与否
- 任何偏离（跳过里程碑、自造流程、多开 subagent）**记入事件流** → 可观测、可回放、可评测
- 工作流文档被 agent 修改时，**作为事件追加**，不原地覆盖（F6）

### 归属与组成
- 工作流是**绑定在 session 上的一份配置**，不是 core 全局状态
- 绝大部分是**纯数据** → 跨边界契约（可被任何语言写、组合、动态追加）
- 只有两处是真正的代码扩展：
  1. **里程碑部件**（`MilestoneHandler`：`worktree.create` / `ci.check` / …）
  2. **自定义上下文策略**（超出内置枚举时）
- v0 实现：philosophy 注入 + agent_types 能力池 + 上下文策略选择 + 里程碑标记与触发 + grants 授予。**不做**可视化编辑器、DAG 调度器、流程校验

---

## 4.2 上下文管理策略（可自定义）

`context_strategy` 是工作流的一部分。内置四种，**用户可再写自己的**（"完全自定义的框架"）。

| kind | 机制 | 适用 | 主要代价 |
|---|---|---|---|
| `append` | 追加式（默认） | 短会话 | 撞窗口后需压缩 |
| `stage_scoped` | 阶段性独立上下文 + 交接摘要 | 阶段边界清晰 | **交接点是信息损失点**（约束容易在交接时丢） |
| `sliding_window` | 滑动窗口，最近 N token 为热区 | 连续长会话 | **无天然阶段边界，摘要退化最严重** |
| `tiered` | **分级压缩 + 指针下钻** | **长时程自主任务** | 实现复杂（需 LLM 切分阶段） |

### `tiered`：按"占用百分比"分级的压缩

**压缩等级 = 目标占用上下文窗口的百分比档位**，不是离散编号。

- 每个阶段有一个"最低压缩等级"，例如只占 **0.1%** 窗口
- 于是 **500 个阶段才占满 50%**；此时每个阶段的摘要只能是"对当前阶段的简要概括"
- 需要深度压缩某个阶段时，才把它压到更小的占用档位

**tier 标签的作用 = 给机械化流程的定位标记**，让框架能识别出"LLM 要求压缩的范围"：

```
① LLM 标记一批条目（带 tier 标签）——"这一段要压，压到 X%"
② 框架机械地取出这批条目
③ 交给 LLM 压缩
④ 取回摘要，放回上下文
```

**v0/v1 策略**：只**预留接口**，每个阶段先做**最浅层压缩**；不做深度压缩，不做级联。

**触发点**：占用到 **50%** 时压缩。依据是实测——**30%~50% 区间 LLM 能力损失较小**。该数值**用户可调**。

### 未解问题：级联压缩

> 深度压缩阶段 A 时，LLM 需要"总体局势" + A 的全文；若"总体局势"来自其他阶段的摘要，而压缩 A 又改变了 A 的摘要 → 级联失效。

**候选解法**：

| 方案 | 做法 | 代价 |
|---|---|---|
| A. 域隔离 | 把阶段分域（探索域 / 实现域），压缩实现域时不读探索域的阶段摘要，只读域级顶层摘要 | 级联深度被限制为 1，但需要维护域划分 |
| B. 结论不可变 | 摘要只允许引用"已定结论"（约束、决策），结论写下后不再改 → 压缩下游不会让上游失效 | 需要区分"结论"与"过程"，agent 要配合 |
| C. **独立全局状态（goal ledger）** | **"总体局势"不来自任何阶段摘要，而来自一份独立维护的持久账本**（目标、约束、已完成项、待解项）。压缩任一阶段只读账本 + 目标阶段全文 → **级联链被切断** | 需要 agent 在关键点显式更新账本 |
| D. 级联预算 | 接受级联，用预算限制重压深度 | 治标不治本 |

**倾向 C（+ A 辅助）**。理由：级联的根因是**"总体局势"寄生在阶段摘要里**；把目标与约束搬到上下文之外的独立存储，级联就不存在了。而这恰好也命中了根因本身——

> 多次压缩导致"目标和限制偏移"的根因是：**目标与约束只存在于上下文中**，被反复压缩。解法是让它们有一个**上下文之外的一等存储**。

（见 `G` 区的 goal ledger 提案）

### 上下文管理的三个正交钩子（✅ 已定）

上下文管理是**工作流的一部分**，第三方要能方便地加自己的方案。所以不设一个大接口，拆成三个正交钩子：

```go
type Assembler interface { Provide(ctx AssemblyCtx) ([]Entry, error) }  // ① 注入什么
type Budgeter  interface { Allocate(items []Item, budget Budget) Plan } // ② 保真度怎么分配
type Compactor interface { Compact(ctx CompactCtx, items []Item, target Budget) (Summary, error) } // ③ 超了怎么压
```

| 第三方想改什么 | 只需实现 |
|---|---|
| 加一个新注入源（RAG 结果、项目规则、别的记忆系统） | `Assembler` |
| 改预算分配（**tier 分级就是其中一种实现**） | `Budgeter` |
| 改压缩方式（**分级压缩就是其中一种实现**） | `Compactor` |

工作流侧：`context_strategy` = 三个钩子的实现引用 + 参数。排布顺序等更细的项后续追加（v0 不设 `Layout` 钩子）。

### `ContextSnapshot`：让上下文可见（✅ 已定）

**痛点**：总体上下文不可见。**解法不是让每个策略自己实现可视化，而是 core 自动生成与策略无关的清单。**

```json
{
  "session_id": "...",
  "turn": 42,
  "tokens_used": 120000,
  "tokens_budget": 200000,
  "parts": [
    { "source": "base",     "kind": "system",   "entries": 1,   "tokens": 1200,   "state": "verbatim" },
    { "source": "ledger",   "kind": "state",    "entries": 1,   "tokens": 800,    "state": "verbatim" },
    { "source": "rules",    "kind": "injected", "entries": 3,   "tokens": 4000,   "state": "verbatim" },
    { "source": "tools",    "kind": "schema",   "entries": 18,  "tokens": 6500,   "state": "verbatim" },
    { "source": "history",  "kind": "history",  "entries": 420, "tokens": 105000, "state": "verbatim" },
    { "source": "stage:12", "kind": "history",  "entries": 87,  "tokens": 1200,   "state": "compacted",
      "target_tokens": 1200, "origins": ["e:1203..e:1290"], "summary_ref": "sum:8821" }
  ]
}
```

`state` ∈ `verbatim` / `compacted` / `dropped` / `deferred`

**它解决四件事**：上下文构成可见 · 压缩效果可度量（`origins` + `target_tokens`）· 污染来源可见（哪个 source 塞了多少）· 可回溯（`summary_ref` → 沿指针精准捞回）

**关键**：
- 由 core 生成，**与策略无关** → 任何第三方钩子组合出来的上下文都能被同一套 UI 渲染（"可替换"与"可视化"因此能共存）
- **每轮一条 durable 事件** `context.assembled` → 回放时可重建**任意时刻**的上下文构成
- **跨边界契约**（UI 语言自由）→ D 区跨边界契约 8 → **9 个**

### 指针解析 vs RAG（✅ 已定）

| | 指针解析 | RAG 检索 |
|---|---|---|
| 性质 | **确定性**（ref 精确指向） | **概率性**（相似度排序） |
| 归属 | **core 的基础能力** `Resolve(ref)` | **独立扩展点** `Retriever.retrieve(query)` |

RAG 结果通过 `Assembler` 注入，与项目规则、ledger 走同一条注入路径，不需要特殊通道。

### `Retriever`（保留位）

```
Retriever.retrieve(query) → [片段]
  ├─ PointerRetriever  —— 沿摘要指针下钻（分级压缩）
  └─ SemanticRetriever —— 向量 / 关键词检索（未来的 RAG 接入）
```

这样后续接入 RAG **不需要改 core**，只是多一个 `Retriever` 实现。

---

## 5. v0 骨架的范围（做什么 / 不做什么）

### 做（做完骨架才成立）—— 按 v0 垂直切片顺序（审阅 D8 结论：② → ③，① 留 v1）

**切片 ②（多 agent + 上下文隔离器）—— v0 的核心差异化**
1. `contracts` 全部类型 + 导出 JSON Schema（唯一清单见 D21）
2. 自写受限子集 **codegen**（schema → Go 类型）+ CI「再生成 diff 为空」校验
3. 事件总线（发布/订阅 + durable 追加钩子；`durable=false` 流事件不入库不占 seq，F8）
4. 扩展注册表 + 生命周期 + **能力裁决**；`agent_type.can_spawn` + **类型可达性**深度限制（K16/K17）
5. 策略引擎（求值链 deny→ask→allow，**fail-close**，**deny/ask 跨来源取并集**，workflow 只可收窄 H23）
6. `Store` 接口 + **SQLite 实现**（`ncruces/go-sqlite3`）+ **索引器**（事件化 ledger + seq 水位 + 重建 G23/G24）
7. 最小 agent loop + **spawn + 结构化 summary**（含 `refs` 可核验、`taint` 污点继承 Q7）+ agent 树 + **父↔直系子可见**（K11/K27）
8. **环境变量注入四层**（凭据 / 执行 / 上下文 / 作用域，H14–H20）

**切片 ③（真机回路 + 最小门禁）**
9. **真 provider**（Anthropic 或 OpenAI 兼容）+ 真实工具（read / write / bash）
10. **最小 receipt 门禁**：不透明命令（仓库级注入）+ **一次性快照 worktree** + **receipt 四要素校验**（V1/V2）
11. **极简 CLI**（终端原生交互：对话 / 审批 / 在线表）+ **Web UI**（深度视图：上下文可视化 / 多 session 面板）走 WS+NDJSON

**贯穿全程**
12. **durable 语义**（X 区）：intent → 执行 → result 三段落盘、checkpoint、resume、ownership
13. 契约跨语言可用性验证：导出的 JSON Schema 能被 TS 侧消费

### 不做（明确推迟，避免骨架被功能绑架）
OS 沙箱（V0 明确声明边界）/ 完整 TUI / MCP server（v0 只做 client）/ 评测框架 / 深度压缩 / 完整工作流引擎 / **多 session 发现表与 hub（切片 ①，v1）** / 多 worktree 隔离

### v0 的验收
- [ ] 把 `Store` 实现从 SQLite 换成内存实现 → **core 零改动**
- [ ] 注册一个新的自定义 `Tool` → **不修改任何已有文件**（只加一个新文件）
- [ ] 把 `Approver` 从 deny-all 换成 always-allow → **core 零改动**
- [ ] 换一个 `Provider` 实现（Mock → 另一个 Mock）→ **core 零改动**
- [ ] 换一个 `Transport`（inproc → ws+ndjson）→ **core 零改动**
- [ ] 事件流能完整回放出一场 session 的全过程
- [ ] `core` 在**没有 runtime** 的情况下能跑完全部单测
- [ ] **主 agent 拆任务 → spawn 带 role 的 subagent → 收回结构化 summary → summary 的 refs 可核验**
- [ ] 深度超限的 spawn 被**运行时拒绝**（不是提示词约束）
- [ ] receipt 与当前 tip 不一致时**门禁拒绝通过**
- [ ] kill -9 后能 resume 且**不重复副作用**

---

## 6. 目录结构（Go）

```
harness/
├── contracts/            # 纯契约 + JSON Schema（零依赖）
│   ├── event.go  command.go  context.go  model.go
│   ├── tool.go    policy.go  approver.go   store.go
│   ├── session.go extension.go transport.go  telemetry.go
│   └── schema/           # 导出的 *.json
├── core/                 # 依赖仅 contracts
│   ├── loop/             # agent loop 状态机
│   ├── bus/              # 事件总线
│   ├── registry/         # 扩展注册表 + 生命周期
│   ├── policy/           # 策略求值链
│   └── context/          # 上下文装配
├── runtime/              # 实现 core 定义的接口
│   ├── store/sqlite/     # SQLite 实现 + 投影
│   ├── process/          # 进程管理
│   ├── sandbox/          # win32 / seatbelt / bwrap
│   ├── provider/         # provider 适配器
│   └── transport/        # inproc / ws+ndjson / stdio
├── extensions/           # 官方扩展（也是示例）
│   ├── builtin.tools/    # read / write / bash
│   ├── builtin.policy/   # 规则链
│   └── builtin.approver/ # TUI 审批 / deny-all
├── host/
│   ├── hub/              # 常驻协调进程（v1）
│   └── cli/              # v0 最小宿主
├── ui/
│   └── tui/              # bubbletea
└── cmd/harness/
```

**注意 `extensions/` 在顶层不在 `runtime/`** —— 官方扩展与第三方扩展用同一套注册机制，官方扩展本身就是"如何写扩展"的活文档。

---

## 7. 骨架必须先天具备的"多客户端就绪"

用户的首要痛点是"多 session 无法形成数据中心式的沟通"。v0 不做 hub，但**契约层从第一天就按多进程假设设计**：

| 现在就做（成本低） | 留给 v1（只加不改） |
|---|---|
| `Transport` 抽象（inproc / ws+ndjson 同接口） | hub daemon 进程 |
| 事件流可被**远程**订阅（订阅者按 `session_id` 过滤） | 多客户端同时连接 |
| `Session` / `Agent` 是可插拔实体，有稳定句柄 | session 跨进程迁移 |
| `Command` 可从外部提交（不只在进程内调用） | 远程提交命令 |
| 所有契约是数据格式 → 任意语言客户端 | TS GUI / IDE 插件 |
| core 不持有全局单例状态（状态显式传递） | 分布式部署 |

**规则**：任何"以后可能要变成多进程"的决定，现在就按多进程设计——这是把 hub 从重写降级为加法的唯一办法。

---

## 7.1 活状态与持久状态的分野（Registry / Gate）

**session 不是隔离单元，是协作单元。** 隔离的是上下文、工具集、权限；共享的是身份、状态、产出。

### 两类状态，别混

| | 持久投影（落库） | 活状态（只在 core 内存） |
|---|---|---|
| 内容 | session 目录、图投影、成本、ledger、快照当前态 | **发现表**：谁在线 / 是谁 / 在干什么 / 属于哪些 gate |
| 生命周期 | 永久 | **与进程同寿，进程结束自行消失** |
| 维护者 | 索引器（单写者消费事件流） | core 的 `registry`（事件驱动改内存） |

**不需要租约、不需要心跳、不需要"离线 session 保留"**——session 生命周期 = 进程生命周期。

### 可见性 = 两个正交维度

| 维度 | 规则 | 谁能看见谁 |
|---|---|---|
| **水平（gate）** | gate 是**发现条件**，不是通讯录 | **只有 session 的主 agent** 之间互相可见（`gates(A) ∩ gates(B) ≠ ∅`） |
| **垂直（agent 树）** | 同一 session 内的层级 | **父 ↔ 直系子 可见**（每个 agent 可见自己的父 + 自己的全部直系子）；**兄弟 subagent 互相不可见**；**别 session 的 subagent 完全不可见**；**跨层上查 = `Inquiry` 沿父链逐级显式调用**（K11/K27） |

> 设计哲学：**subagent 是一个分支**——不需要让协调者看到分支。

### 暴露等级 = 注入长度

不是字段级过滤，是**窗口长度**：

| level | 注入内容 |
|---|---|
| `summary` | 0 条事件 + 1 条摘要（说明这个 session 在干什么） |
| `n` | 最近（或最先）n 条事件 |
| `full` | 全部事件 |

主 agent 上下文里**常驻一张在线表**（每个可见 session 一行 summary）。当它判断某个 session "运行有问题"时，主动把那个 session 的窗口展开到 n 条事件。
⚠️ 在线表成本随 session 数线性增长 → 必须有上限与筛选（按 gate 过滤、最多 N 个、summary 限长）。

### 深度限制：默认总深度 3（两级委派）

```
主 agent (depth 1) → subagent (depth 2) → sub-subagent (depth 3) → 禁止
```

深度 4 几乎不产生实际收益，但 summary 链 / 权限链 / 审计链的成本线性增长。**建议可配置**（`max_agent_depth`）。

**强制方式（✅ K16/K17）**：**类型可达性为主**——`agent_type.can_spawn` 决定"我能派谁"，第 4 层在类型层面不可达；**数字计数器为兜底**（`child.depth ≤ max_agent_depth`）。spawn 时超出即**直接拒绝**——不靠提示词说"别再派了"。与 H11（权限只能收窄）同类机制。

### 交流能力做成可复用组件

```
core/agentlink/
  Presence   在线表
  Discover   按 gate / 树查找
  Inquiry    深入查询某 session 的事件窗口
  Broadcast  向 gate / 子树发消息
  Handoff    结构化交接（带 payload）
```

**library 形态，不绑定任何协议**——任何宿主都能用。

### 跨机：不自己发明分布式协议

跨机时把 session 暴露成 **ACP agent 端点**，客户端用现成协议接入。分布式存储 / 分布式发现都不做。

---

## 8. 待拍板（我的倾向已给出）

| # | 问题 | 我的倾向 | 理由 |
|---|---|---|---|
| 1 | 契约序列化格式 | **JSON Schema + NDJSON** | 跨语言、跨进程、可读、与 ACP/MCP 生态一致；protobuf 留作后续性能优化 |
| 2 | 扩展执行模型 | ⚠️ **已由 E1/E2 取代**：**A 接口实现为主 + B 进程级为边界** | A 覆盖 12 个扩展点；B 只给 MCP server / GUI 面板 / 第三方工具 / 不可信扩展 |
| 3 | 事件流是否分两套 | **单一流 + `durable` 标志位** | 避免两套通道的顺序/丢失问题 |
| 4 | v0 是否含 hub | **不含；但契约按多进程设计** | 先证明 core 可用，同时保证加 hub 不用改 core |
| 5 | 存储引擎首选 | ⚠️ **已由 G2b 定案**：`github.com/ncruces/go-sqlite3` v0.35.6（实测并发写 1000/1000、跨进程已验证），`modernc.org/sqlite` 为备选 | 详见 `spike/sqlite-drivers/` |
| 6 | 事件日志与投影的边界 | **事件日志是事实源，投影只读** | 压缩/修复只能追加事件，永不重写历史（omkc 踩过的坑） |

---

## 9. 之后自然长出的方向（不在 v0，但骨架为其留位）

1. **多 session 数据中心**（hub + 黑板 + 消息路由 + 任务图）
2. **Windows 沙箱**（`x/sys/windows` 原生）
3. **真 provider 接入 + 跨 provider 上下文交接**（行业真空，可差异化）
4. **自热插拔工作流引擎**（声明式 DAG）
5. **GUI / IDE 插件**（TS 侧，走同一契约）
6. **ACP**（社区 Go 库或按官方 schema 自实现）
7. **评测**（事件流即数据集）

---

## 参考

- `00-overview.md`（横切面缺口清单）、`01-permissions-and-sandbox.md`（三层权限模型）、`05-architecture-pivot.md`（转向与语言定案）
- `ho_3b68059bcbfc`（omkc 回函：策略链形态、auto 模式话术、危险默认、包边界教训）
- `pi-durable` / `pi-chord` 设计文档（长时程与跨进程对象模型的规格参考）
