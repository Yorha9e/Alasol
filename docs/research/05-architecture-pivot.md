# 架构方向转向：独立 harness 的语言与核心设计

> 触发：本轮拍板三个约束——① 终局形态 = **独立产品 / 独立 harness**，与 omkc 关系不大 ② **Windows 是一等目标** ③ **不复用 omkc 的包**（fork 也不要）。
> 触发原因（用户指出 omkc 的三个不优雅点）：TUI 下无法形成"数据中心"式的常驻进程让多个 session 互相沟通；自定义与热插拔能力不足；底层全用 JSONL + minidb 不合适。
> **本文修订了 `02-runtime-and-stack.md` 与 `04-handoff-reply-and-language-decision.md` 的选型结论。**

---

## 0. 前序结论的作废范围

| 文档 | 原结论 | 状态 |
|---|---|---|
| `02-runtime-and-stack.md` | "TS/Node 核心 + 小型原生 helper"，因为 pi 是 TS、复用全家桶 | **作废**。前提（复用 pi）不成立 |
| `04-...` | "TS 核心 + Go helper"，因为 50k 行 omkc 资产在 TS 一侧 | **作废**。前提（复用 omkc）不成立 |
| `03-provider-sdks-and-go.md` | Go 路线复刻成本清单 | **仍然有效**，但复刻对象变成"pi-durable 语义 + kosong 方言归一"，不再包含 omkc 资产 |
| `01-permissions-and-sandbox.md` | 三层权限模型、各平台沙箱选型 | **完全有效**，与语言无关 |
| `00-overview.md` | 横切面缺口清单 | **完全有效**，且用户提的三个不满正对应其中三项 |

**仍然成立的结论**：
- 权限三层不可互替、规则链首个命中即返回、子 agent 权限只能收窄、无人值守要让 ask 工具失败+注入话术、压缩只追加事件不重写历史
- provider 层可自研，不必借 pi（omkc 的 kosong 是独立佐证）
- 跨 provider thinking 交接是**行业级真空**（kosong 静默丢弃，pi 的 transform-messages 不完整）——仍是可抢占的差异化点

---

## 1. 新结论：Go 占优

在"独立产品 + Windows 一等 + 不复用 omkc"三个约束下，**全 Go 的理由成立**。逐条重算：

### 1.1 Go 赢的地方，全部命中我们的核心诉求

| 我们的诉求 | Go 的对应能力 |
|---|---|
| **数据中心式常驻进程，多 session 互相沟通** | 真并发（goroutine）+ 低内存（每 session 栈 2KB 起）+ 强类型消息路由。几百个 session 挂在一个 hub 上，Go 的资源占用比 Node 低一个量级（Node 共享堆有 GC 停顿与单事件循环瓶颈——这正是 omkc 承认的"已知架构债"） |
| **Windows 一等目标** | `golang.org/x/sys/windows` 直接绑定 `CreateRestrictedToken` / JobObject / ACL / 防火墙规则，**无需 C++/Rust helper**。这是 TS 侧拿不到的 |
| **热插拔** | 独立进程式插件（协议隔离）+ 编译期注册 + 配置驱动流水线；解释型扩展可选 Starlark（`go.starlark.net`）/ CEL（`cel-go`）/ goja |
| **存储层重设计** | bbolt（事务 KV）/ pebble（LSM 高写吞吐）/ badger / `modernc.org/sqlite`（纯 Go，无 cgo）；消息路由有 `watermill` 这类现成库 |
| **独立分发** | 天然单二进制；多 UI 端（TUI/GUI/IDE）作为独立进程通过协议接入即可 |
| **TUI** | bubbletea + lipgloss 生态成熟度**高于** TS 侧的 pi-tui（这是 Go 的优势，不是劣势） |

### 1.2 Go 输的地方，以及为什么这次不致命

| 劣势 | 这次为什么不致命 |
|---|---|
| ACP 无官方 Go SDK | 有活跃社区库 `caelis-labs/acp-go-sdk`（v1.5.1 / 2026-10-16，声明 ACP wire v1、schema-v1.25.0，且与官方 TS/Rust SDK 做四方向互操作矩阵）。且**我们可以自己按官方 schema 生成 Go 类型**——ACP 的 schema 是公开的 |
| 官方 MCP Go SDK 成熟度 | `modelcontextprotocol/go-sdk` v1.8.0（2026-09-04，官方 + Google 维护），支持 tools/resources/prompts + stdio/HTTP/SSE，**足够** |
| 没有现成件可抄 | 我们本来就不打算抄 omkc；pi 那边真正值得读的是 `pi-durable` 的**设计语义**（durable 三水位、ownership index、replay: safe），不是代码 |
| 迭代速度 | 早期会慢一些。可用「先 TS 写原型验证架构 → 再 Go 实现」的过渡，但**不建议**：契约漂移成本高。建议直接 Go，用接口先行 |
| 跨 provider 方言归一成本 | kosong 花了约 5k 行；Go 侧预计 6–8k 行。**这是必须付的成本，与语言无关**（ kosong 也没解决 thinking 交接） |

### 1.3 混合方案的必要性：UI 侧可能要 TS

数据中心式架构天然是多进程多前端。Go 适合做 **hub（协调 + 存储 + 策略）+ TUI**，但 GUI / IDE 插件侧 Web 技术栈更顺。

**建议的最终形态**：
```
Go hub（常驻 daemon）
  ├─ 协调：session 注册表、消息路由、黑板、任务图
  ├─ 存储：事件日志 + 投影 + 索引
  ├─ 策略：权限三层 + 审批
  ├─ 工具/MCP/ACP 宿主
  └─ 可插拔 agent worker（进程内 goroutine 或独立进程）
        ↕ 统一协议（WebSocket / gRPC / 自定义 NDJSON）
   TUI (Go/bubbletea)  GUI (TS/任意)  IDE 插件 (TS)  脚本/自动化 (任意)
```
即：**核心与协调用 Go，所有前端通过协议接入，前端语言自由**。这既满足"数据中心"诉求，又不被某个前端技术栈绑死。

---

## 2. 用户提出的三个不优雅点 → 我们的三个核心架构

这三个点不是"要修的 bug"，而是**这个产品存在的理由**。它们正好落在我们第一轮识别的横切面缺口上。

### 2.1 数据中心式协调（对应缺口 B：多 agent 编排）

**omkc 的现状**：子 agent 是**进程内 Agent 级 DI scope**，不是独立进程；session 之间靠单事件循环串行调度；TUI 是唯一前端，进程内一切。

**我们的形态**：
- **Hub 常驻进程**：持有 session 注册表、消息路由、黑板、任务图；自身是唯一的持久化写入者（解决 omkc "跨进程无 session 级文件锁"的问题）
- **Session = 可插拔实体**：每 session 有独立 context、工具集、权限、工作目录、模型绑定；可以是 hub 内的 goroutine，也可以是独立进程（需要强隔离时）
- **Session 间通信**：显式的消息通道（请求/响应 + 事件广播 + 黑板状态），而不是"共享一个事件循环"
- **背压与配额**：并发上限（**任务 token 预算不设上限**，见 W15）、优先级；provider 限流要能级联到调度层（omkc 的 swarm 首屏 5 + 700ms 递增是我们要抄的形态，但要做得更好）
- **崩溃域**：单个 session 崩溃/卡死不影响其他 session，也不拖垮 hub

> 参考：`pi-chord` 的跨进程对象模型（引用而非拷贝、principal 权限、delta 同步、hot reload）与我们想要的"数据中心"高度同构，值得精读其设计文档。

### 2.2 扩展与热插拔（对应议题 9：外接端点）

**omkc 的现状**：扩展性不足。

**我们的形态**（分层，从弱到强）：
| 层级 | 机制 | 适用 | 热插拔 |
|---|---|---|---|
| 1. 声明式 | 配置 + 提示词模板 + skill（渐进披露） | 改行为不改代码 | 立即 |
| 2. 工具扩展 | 独立进程/MCP server + schema 注册 | 加能力 | 起停进程 |
| 3. 流程扩展 | 可组合的工作流/管线（声明式 DAG） | 自定义多步流程 | 重载配置 |
| 4. 代码扩展 | 宿主内嵌（Go plugin / 解释型：Starlark / CEL / goja） | 深度定制 | 重载模块 |
| 5. 外接端点 | 自定义 HTTP/gRPC 端点 + ACP 自定义方法（`_` 前缀 + `_meta`） | 外部系统集成 | 重载端点 |

设计原则（照抄 omkc 已被验证的形态并改进）：
- **契约先行**：扩展点都用 schema 描述（Go 侧用 `jsonschema` 生成校验）
- **权限声明前置**：扩展安装时声明它要什么能力，运行时由策略引擎裁决
- **UI 无关**：扩展点不假设有 UI（对应 omkc 提的"5 项必须 UI 无关"）
- **失败隔离**：扩展崩溃不能带崩 hub

### 2.3 存储层重设计（对应议题 2/3）

**omkc 的现状与问题**：
- 每 agent 一条 append-only wire.jsonl（55 种事件）= 唯一事实源 → **长会话 fold 重放指数恶化**（他们自己列为待办）
- MiniDb（18.9k 行自研 KV+WAL+全文/trigram 索引+分片）→ 重且自研包袱大
- 压缩产物是摘要不是指针 → 模型视角丢细节不可逆

**我们的方向**（需要单独设计，先列原则）：
1. **事实源与投影分离**：事件日志是事实源，但**分段 + 偏移索引 + 惰性加载**，让"打开一个 10 万轮的会话"不必重放全部
2. **摘要 + 指针双轨**：压缩产物 = 结构化摘要 **+ 原文区间指针**，模型上下文里用摘要，磁盘上保留原文且**可按需回捞**（解决 omkc "不可逆丢细节"）
3. **不用自研存储引擎**：优先成熟嵌入式引擎（SQLite/现代c 纯 Go 版、bbolt、pebble、badger），**我们的代码只写 schema 与索引策略**
4. **单写者模型**：hub 是唯一写者，避免 omkc 的"跨进程无锁"问题；写入用批量提交降低 fsync 开销
5. **环境信息分层缓存**：cwd / 项目规则(AGENTS.md) / 环境变量 / git 状态 按变化频率分层——静态部分烘进 system prompt，动态部分按需注入（omkc 的 `system.md` 模板变量 + agentsMdReminder 形态是对的，但 git 状态不进 prompt 这条要保留）
6. **成本与用量可追溯**：每轮 token/耗时/工具延迟入事件流（omkc 缺独立评测框架，事件日志就是天然的评测数据源）

---

## 3. 语言决策（更新）

| 维度 | 结论 |
|---|---|
| **核心 / 协调 / 存储 / 策略 / 工具宿主** | **Go** |
| **TUI** | **Go**（bubbletea + lipgloss） |
| **GUI / IDE 插件 / Web UI** | 语言自由（TS 生态更顺），通过协议接入 hub |
| **Windows 沙箱** | **Go**（`x/sys/windows` 原生实现，进程内直接调 Win32 API，无需 helper） |
| **可执行扩展** | Go 进程 + 解释型（Starlark/CEL/goja） |
| **是否仍需 pi** | **不引用其代码**；`pi-durable` / `pi-chord` 的**设计文档**作为长时程与多进程对象模型的规格参考；provider 契约自研 |

**一句话**：Go 做内核与协调，协议做边界，前端语言自由。

---

## 4. 待拍板 / 待设计

1. **统一前端协议**：hub ↔ UI 用什么？WebSocket + NDJSON（最省事，ACP 生态已有 NDJSON 惯例）/ gRPC / 自定义。倾向 WS+NDJSON，理由：浏览器友好、调试方便、与 ACP 传输一致
2. **Session 隔离粒度**：默认 hub 内 goroutine，还是默认独立进程？建议**默认独立进程可选、按需启用 goroutine 模式**（安全默认优先）
   > ⚠️ **2026-10-27 已被 `07` 的 K1 取代** —— 实际定为**默认 hub 内 goroutine**、独立进程按需。理由：subagent 是自 spawn 的自家代码，OS 级隔离属于**工具 / ExecutionEnv**（K8）；且 v0 无沙箱时进程隔离也拦不住写盘，「安全默认」名不副实。
3. **扩展语言选型**：Starlark（表达力强、Go 原生维护、无副作用设计）vs CEL（表达力弱但极快，适合策略表达）vs goja（JS 全兼容，生态熟悉）
4. **存储引擎选型**：SQLite（纯 Go 版）/ bbolt / pebble / badger，取决于"事件日志 + 投影 + 全文检索 + 多 session 并发读"的具体需求
5. **ACP 的投入度**：一等公民（用社区 Go 库）还是自研（按官方 schema 生成类型 + 自己实现）？
6. **是否与 omkc 保持协议互通**：如果未来要接管/对接现役 harness，需要约定 wire 事件格式的兼容层

---

## 参考

- 约束变更来源：用户本轮拍板 + `ho_3b68059bcbfc`（omkc 维护方回函）
- `pi-durable` / `pi-chord` 设计文档：作为长时程与跨进程对象模型的规格参考
- Go 生态实测版本：`docs/research/03-provider-sdks-and-go.md`
- 权限模型：`docs/research/01-permissions-and-sandbox.md`
