# 运行时与技术栈选型（多 agent / 多 session 场景）

> ⚠️ **本文结论已作废**（2026-10-26）。前提"复用 pi 全家桶"不成立——新框架是独立产品，不复用 pi 也不复用 omkc。最新结论见 [05-architecture-pivot.md](./05-architecture-pivot.md)：**核心与协调用 Go**。本文保留作为推理过程记录（"多 session 压垮 Node"是架构问题而非语言问题这一判断仍然成立）。
>
> 触发问题：既然 pi 是 Node/TS，复用它的 provider 层就等于绑定 Node；而"多 agent + 多 session + 长时程"听起来又像是 Node 撑不住的负载。这个矛盾是真的吗？
> 结论：**矛盾是假的，但需要用一层解耦动作来消除。**

---

## 1. 结论（先行）

| 问题 | 答案 |
|---|---|
| Node 能不能做多 agent / 多 session？ | **能。** 这类负载是 I/O 密集（模型 API 延迟、子进程 I/O、文件读），正是事件循环的舒适区 |
| "多 agent 撑不住 Node" 的真实来源 | **不是语言限制，是"每 agent 一进程"这个架构选择**。Node 每进程基线内存 40–60MB，20 个隔离 agent 就 >1GB 纯运行时开销 |
| 要不要因为 pi 是 TS 就锁死 TS？ | **不要。** 用一层薄 provider 契约把 pi-ai 降级为"可插拔实现"，语言绑定即刻解除 |
| 推荐方案 | **TS/Node 核心 + 小型原生 helper（仅 OS 边界）**，即 Anthropic / Codex 的形态 |

---

## 2. 为什么"多 session"让人怀疑 Node —— 拆开看

### 2.1 负载画像
Agent loop 的时间构成：

| 阶段 | 性质 | 典型耗时 | Node 适配性 |
|---|---|---|---|
| 模型 API 调用 | 网络 I/O | 秒级 | 极好（异步流式） |
| 工具执行（bash/git） | 子进程 I/O | 百毫秒–秒级 | 极好（`child_process` + stdio 管道） |
| 文件读写 | 磁盘 I/O | 毫秒级 | 好（异步 fs） |
| 大文件解析 / diff 计算 / 索引 | **CPU 密集** | 百毫秒–秒级 | **差（阻塞事件循环）** |
| 上下文装配 / JSON 序列化 | CPU 密集 | 毫秒级 | 差（尤��超大 session log） |

**结论**：绝大多数时间是等待，CPU 密集只占一小撮且可以用 `worker_threads` 或子进程挪走。Node 的单线程事件循环在这里是优势（单连接池、共享 keep-alive、零拷贝流式），不是劣势。

### 2.2 真实的三类风险（这些才是要设计的）

| 风险 | 现象 | 对策 |
|---|---|---|
| **事件循环阻塞** | 一个 session 读 50MB 文件 → 同进程所有 session 的流式输出卡住 | CPU 密集操作走 `worker_threads`；fs 一律 async；`node:sqlite` 的同步 API 只用于有界短事务 |
| **进程内存放大** | 每 agent 一进程 → N × (40–60MB 基线) | **默认单进程多 session**；只有需要独立 OS 隔离/独立崩溃域的 agent 才 fork 子进程 |
| **GC 压力与长驻堆** | 长期运行的 daemon 持有全部历史 → 堆膨胀、停顿 | 内存里只保留**已装配的上下文窗口**，完整历史溢出到存储层；定期裁剪 |

### 2.3 架构选择 vs 语言限制
"多 agent 撑不住"这个判断，99% 出自**选了进程级隔离模型**（每 agent 一进程），然后把成本归因给语言。换成"单进程多 session + 按需 fork 隔离 agent"，Node 的每 session 边际成本就是一个 async 任务 + 一段堆内存，量级完全不同。

**多 agent 的难点在架构而非语言**：隔离（权限 / worktree / 进程）、调度与配额、状态与持久化、冲突合并——这四项都与运行时无关。

---

## 3. Node 在本项目里的决定性优势（不是"勉强能用"）

### 3.1 复用的不只是 provider 层
pi 是一套 TS monorepo，能作为**库**在同进程内导入的包括：

| 包 | 复用价值 | 对本项目的意义 |
|---|---|---|
| `@earendil-works/pi-ai` | 统一 provider 抽象、模型目录、流式事件协议 | 议题 1 |
| `@earendil-works/pi-agent-core` | agent loop、工具调用、状态管理 | **议题 5 直接落位** |
| `@earendil-works/pi-coding-agent` | `createAgentSession` / `SessionManager`（in-memory 与文件两种）、扩展机制 | **议题 2、3、9 直接落位** |
| `@earendil-works/pi-tui` | 差分渲染 TUI 组件库 | 议题 10 |
| `@earendil-works/pi-web-ui` | Web 组件 | 议题 10（GUI 路线） |

换 Rust 意味着这些**全部复刻**，不是只复刻 provider。而复刻的难点不在写代码量，在**行为一致性**（树形 session 的分支语义、compaction 产物的形态、跨 provider 上下文交接的 thinking 块转换）——这些是最容易做出"看着能用、实际对不上"的地方。

### 3.2 协议 SDK 的成熟度分布
- **ACP 官方 SDK**：TypeScript / Python / Rust / Kotlin / Java 全覆盖，全部 Apache-2.0；npm 包 `@agentclientprotocol/sdk`（约 1.4MB，zod 作为 peer dep）
- **主流 ACP 实现是 TS**：`claude-agent-acp`（TS）、`codex-acp`（TS）
- **MCP**：官方 TypeScript SDK 是参考实现；Rust SDK 属社区维护

议题 8（ACP）和议题 7（MCP）都是一等公民，TS 生态在这两处是主场。

### 3.3 嵌入现役 harness 的可行性
从 moamcp 的会话痕迹看，团队的路径是"把 moamcp 移植进 harness、不再走插件模式"。若目标宿主是 Node/TS 系（claude-code 是 npm 包、kimi-code-community 是其社区分叉），那么 TS 核心可以**同进程嵌入**；Rust 核心只能走 FFI 或 sidecar 进程 + IPC。

### 3.4 存储已内置
`node:sqlite`（Node 22.5.0 引入，Node 23 稳定，**Node 24+ 默认可用**）：`DatabaseSync` 同步 API、预编译语句、零外部依赖。OpenCode 已从 JSON 文件迁移到 SQLite 并使用 `node:sqlite`——**这是多 session 存储的现成参考实现**。

---

## 4. 混合栈：TS 核心 + 原生 helper

Anthropic 与 OpenAI 都是这个形态，不是权宜之计：

```
┌─────────────────────────────────────────────┐
│ TS/Node 进程（主 harness）                   │
│  · provider 层（pi-ai）                      │
│  · agent loop / 调度 / 策略引擎               │
│  · session store（node:sqlite + JSONL）       │
│  · MCP / ACP 服务端                          │
│  · TUI / GUI 宿主                            │
└──────────────┬──────────────────────────────┘
               │ JSONL over stdio（schema 校验）
┌──────────────▼──────────────────────────────┐
│ 原生 helper（仅 OS 边界）                     │
│  · Windows 受限令牌 / Job Object / ACL        │
│  · 必要时：diff 计算、索引                    │
└─────────────────────────────────────────────┘
```
IPC 协议可参考现成先例：Anthropic 的 sandbox-runtime 用文件式 JSONL 协议；pi 自身的 RPC 模式也是 JSONL over stdio。

**原生的使用边界极窄**：只有 OS 强制边界（Windows 沙箱）和明确 CPU 密集的热点才下沉。其余全留 TS。

---

## 5. 关键解耦动作：定义自己的薄 provider 契约

这是把"语言决策"从"架构决策"里剥离出来的唯一有效手段。

```ts
// 我们的内部契约（自有，不依赖 pi）
export type StreamFn = (
  model: Model,
  context: Context,
  options?: RequestOptions,
) => AsyncIterable<AssistantEvent>;

export interface Model {
  id: string;
  provider: string;
  api: string;              // 协议标识，如 "anthropic-messages"
  contextWindow: number;
  pricing?: Pricing;
  // ... 能力元数据
}
```

- pi-ai 作为**其中一个实现**接入（adapter 包一层）
- 好处：
  1. 将来换 Rust/Go，只需重写这一个模块，上层零改动
  2. provider 行为差异被隔离在边界内，agent loop 不被污染
  3. 可以额外接入非 pi 来源的 provider（自建网关、其他 SDK）
- 代价：多一层薄接口（几百行），换掉"全盘绑定某个库"的风险

> **不要**因为"复用 pi-ai"就让 pi-ai 的类型渗透进 agent loop / 上下文 / 存储层。渗透一旦发生，替换成本从"改一个模块"变成"改整个代码库"。

---

## 6. 三方案对比

| 维度 | A. 全 TS | B. TS 核心 + 原生 helper（推荐） | C. 纯 Rust |
|---|---|---|---|
| pi 复用度 | 全量（provider + agent core + session + TUI） | 同 A | **零，需复刻** |
| 复刻成本 | 0 | 0 + 一份小 helper | 高（provider + loop + session 树 + TUI） |
| ACP/MCP SDK | 主场 | 主场 | ACP 有官方 SDK；MCP 为社区实现 |
| 嵌入 Node 宿主 | 天然 | 天然 | 需 FFI 或 sidecar + IPC |
| Windows 沙箱 | 仍需原生 helper（同 B） | helper 承担 | 原生承担（可复用 Codex 沙箱实现） |
| 单二进制分发 | 需 SEA/pkg/bun compile | 需打包 helper | 天然 |
| 多 session 成本 | 单进程多 session 最省 | 同 A | 同 A（且并发上限更高） |
| 团队迭代速度 | **最快** | 快 | 慢（编译周期 + FFI 边界） |

**选 B 的理由**：拿 A 的开发速度 + C 的 OS 边界能力，代价只是维护一份小 helper（Codex 的沙箱实现是开源的，可直接借鉴受限令牌 + ACL + 防火墙的组合）。

---

## 7. 多 session 的具体落地形态（Node 下的建议）

### 7.1 进程模型（混合，按需）
| 场景 | 模型 |
|---|---|
| 普通会话（同一用户、多个任务） | **单进程多 session**：每 session 一个 async 任务，共享 HTTP 连接池与 SQLite 连接 |
| 需要独立 OS 隔离 / 独立崩溃域 / 独立权限的 agent | fork 子进程（`child_process.spawn` + `AbortSignal`） |
| 子 agent | 默认**同进程任务**（独立 context window、继承并收窄权限）；需要 worktree 隔离时配 git worktree，需要 OS 隔离时才 fork |

### 7.2 存储
- **索引 / 元数据**：`node:sqlite`（WAL 模式，支持多进程并发写）——存 session 列表、配置、token 统计
- **会话正文**：JSONL 追加（pi 的树形 `id`/`parentId` 结构），每 session 一个文件 → 天然规避并发写冲突
- **职责切分**：SQLite 管"查得到、查得快"；JSONL 管"不丢、可回放、可分支"

### 7.3 并发与取消
- 并行工具调用：每个工具调用一个 Promise，共享 `AbortSignal`
- 取消：`AbortController` 从 loop 一路贯穿到 HTTP 请求与子进程 kill
- 流式：`AsyncIterable` 是统一形态，贯穿 provider → loop → ACP `session/update` → TUI 渲染

---

## 8. 决策树

```
宿主 harness 是 Node/TS？
├─ 是 → TS（无争议）
└─ 否 → 需要独占进程且要求单二进制 + 强隔离？
         ├─ 是 → 评估 C（纯 Rust），但接受复刻 pi 全部的代价
         └─ 否 → B（TS 核心 + 原生 helper）
```

**唯一需要动摇 C 的理由**：目标是一个**独立的、要求单二进制分发的产品**，且不打算嵌入现有 Node 宿主。如果目标是"嵌进现役 harness 的一个 harness"，C 不成立。

---

## 9. 待核对项

1. 现役 harness（团队维护的那个）的语言/运行时与进程模型 —— 已通过 handoff `ho_2a14886be7a6` 询问，回复后回填本文
2. pi 当前版本的 LICENSE 与包名（`@earendil-works/pi-*`）—— 见 `00-overview.md`
3. `node:sqlite` 在目标 Node 版本上的可用性（需 Node 24+ 才默认开启）
4. Anthropic `sandbox-runtime` 对 Windows 的实际支持度 —— 见 `01-permissions-and-sandbox.md`

---

## 参考来源

- ACP 官方仓库与 SDK 列表：https://github.com/agentclientprotocol
- ACP TypeScript SDK：npm `@agentclientprotocol/sdk`
- `node:sqlite` 文档：https://nodejs.cn/api/v24/sqlite.html
- OpenCode 迁移到 SQLite + node:sqlite：https://github.com/microsoft/AI-Engineering-Coach/issues/84
- pi 会话管理与扩展机制：第三方源码拆解文章（见 `00-overview.md` 第 2 节）
