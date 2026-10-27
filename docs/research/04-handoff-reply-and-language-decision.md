# 回函对照 + Go / Node 决策分析

> ⚠️ **本文第 2–4 节的选型结论已作废**（2026-10-26）。当时的前提是"复用 omkc 的 ~50k 行 TS 资产"，用户随后拍板：新框架是**独立产品，与 omkc 关系不大，不复用其包**。该前提消失后，Go 的天平优势反转。最新结论见 [05-architecture-pivot.md](./05-architecture-pivot.md)。
> **本文第 1 节（回函对照）与第 5 节（待拍板问题）仍然有效**，且是转向的直接依据。
>
> 依据：`ho_3b68059bcbfc`（oh-my-kimi-code / kimi-code-community 维护方对 `ho_2a14886be7a6` 的 Q1–Q7 权威答复）+ 本地实测（pi 仓库结构、Go SDK 版本）。
> 用途：把回函结论与我们自己的调研对齐，并据此做语言选型决策。

---

## 1. 回函对我们的结论：印证 / 修正 / 新增

### 1.1 印证（我们的判断被现役 harness 的实践证实）

| 我们的结论 | 回函证实 |
|---|---|
| 沙箱与审批是正交维度；纯策略层不够 | omkc **没有 OS 沙箱**，只有三层策略（权限模式 + 有序策略链 + 路径守卫）。"按命令沙箱覆盖不到 MCP/hooks"这条他们认同，因此把安全预算全放在策略层 |
| 无人值守必须内建 dontAsk→deny | 他们的 auto 模式就是：**让提问工具本身失败 + 注入"自行决策、不要重试、不要绕过"的拒绝话术**（`policies/auto-mode-ask-user-question-deny.ts:12-20`）。不是"关弹窗"——否则模型会反复重试被拒动作直到耗尽 turn |
| 规则求值是有序链、首个命中即返回 | 13 条 policy 有序链，首个命中即返回（`permissionPolicyService.ts:37-62`），与 Claude Code 的 deny→ask→allow 顺序同构 |
| 压缩只追加事件、不能重写历史 | `context.apply_compaction` 是追加的 durable 事件；wire 永不因压缩重写；溢出恢复最多 3 次按 [0.7, 0.5, 0.35] 缩历史 |
| 子 agent 权限只能收窄 | 他们的做法是**只继承不收窄**（`subagentService.ts:197-199`）——他们自己标注这是反例。我们的设计更好 |
| 并行工具调用需要冲突调度而非裸 Promise.all | `ToolScheduler` + `ToolAccesses.conflict`（读读并行、读写/写写互斥，`toolExecutorService.ts:456-501`） |
| 多 agent 并发必须自适应限流 | AgentSwarm：上限 128，首屏 5 + 每 700ms 放 1 个，命中 provider 限流指数回退 |
| 跨 provider thinking 交接是行业级难题 | kosong **无中央清洗逻辑**——契约存 `{think, encrypted(signature)}`，不支持的适配器在序列化边界**静默丢弃**。也就是说这个问题至今没有好答案 |

### 1.2 修正（我们要改口的地方）

| 原结论 | 修正后 | 原因 |
|---|---|---|
| "多 agent / 多 session 是架构问题不是语言问题" | **方向对，但落点要改**：真正的解法是 **session 级进程隔离**，而不是换语言 | 回函原话："我们所有 session 共享一个 V8 堆和一条事件循环，这是已知架构债"；"认真考虑 session 级进程隔离——**这比换语言收益大得多**" |
| "Go 在多 session 上有真并发优势" | **优势被高估**：如果本来就要进程隔离，那么"共享一个事件循环"就不是瓶颈了，两种语言都要面对同样的隔离开销 | 隔离之后，Node 每 session 一个进程；Go 每 session 一个进程。差别变成"每进程多少 session" |
| 以为"从 pi 借 provider"是既定前提 | **前提不成立**：现役 harness **没用 pi-ai**，全仓唯一 pi 依赖是 `@moonshot-ai/pi-tui`（pi-mono 0.80.2 的 vendored fork，纯 TUI）。provider 是自研 kosong | 他们独立演化出了同一套东西，证明 provider 层不是"必须借 pi"，而是可以自研/自选 |

### 1.3 新增（我们之前没意识到的关键事实）

| 事实 | 影响 |
|---|---|
| 现役 harness 是 **纯 TS/Node ≥ 24.15**，非 TS 组件只有 3 类：node-pty、pi-tui 的 2 个 NAPI 插件、剪贴板分平台包 | TS 也能做到"零原生依赖"（bash 解析器纯 TS 重写、WebP 内联 wasm）——Node 的"必须有原生模块"印象不成立 |
| **子 agent 是进程内 Agent 级 DI scope，不是子进程** | 与我们设想的"子 agent = 独立进程/沙箱"差距很大。omkc 用 scope 隔离上下文与权限，目录隔离只在 tower 域（git worktree） |
| 存储：**每 agent 一条 append-only wire.jsonl 事件日志（55 种事件）= 唯一事实源**；state.json 只放元数据；MiniDb 只做读模型 | 我们的"JSONL 管正文 + SQLite 管索引"分工与之一致。差异：他们是 **每 agent 一条**、55 种事件类型、恢复时 fold 重放 |
| **跨进程没有 session 级文件锁**，靠单 daemon 拥有 | 多 session 并发写的教训 |
| 模型目录双轨：静态快照 + 在线拉 models.dev/api.json，6h 刷新 | 现成方案，可直接抄 |
| 环境信息：`system.md` 模板变量 `${cwd}/${cwd_listing}/${agents_md}`，并行采集；AGENTS.md 按 `$OMKC_HOME` → `~/.agents/` → git root→cwd 逐级发现，超 32KB 警告；**git 状态不进 prompt** | 议题 3 的现成答案 |
| **ACP 用官方 TS SDK `@agentclientprotocol/sdk@^1.3.0`**，agent 侧；fs 反向 RPC 有；terminal 反向 RPC 有条件支持（客户端声明才启用）；elicitation 降级 request_permission；**未接 ACP Registry** | ACP 侧 TS 有官方 SDK、Go 只有社区库——这是 Go 路线的实质劣势 |
| MCP **client-only**，`sampling/elicitation/roots` 全未实现 | 我们的 MCP 议题可以从"client + server"起步，但注意这条生态位是空的 |
| **没有独立评测框架**；vis Timeline 是调试面不是评测 | 横切面 E 无人替我们做，得自建 |
| 全部包 `private: true`，复用 = fork | 复用是 fork 不是依赖 |
| 两个危险默认：**审批服务缺席时默认批准**；interaction 挂起无超时 | 我们设计时必须反过来：缺席 = deny |
| kosong 有**两份**（`packages/kosong` 与引擎内 vendored 副本），`packages/kosong` 无生产调用者 | 抄的时候只抄一份，别两边都改 |

---

## 2. 选型分析：把变量拆开

### 2.1 先确定一个决定性前提

**这个新框架的终局形态是什么？**

| 形态 | 含义 | 语言影响 |
|---|---|---|
| **A. 独立产品 / 独立 harness** | 自己发布、自己分发，和现役 harness 平行 | 语言自由，Go 可全押 |
| **B. 并入 / 移植进现役 omkc harness** | 成为 harness 的一个可插拔组件 | **强烈倾向 TS**——跨语言要重写契约层并引入 IPC 边界 |
| **C. 两者的中间态** | 独立进程 + 通过协议接入（如 ACP） | 语言自由，但契约要跨语言维护 |

> 这个问题的答案比"Go 还是 Node"本身更能决定结果。如果目标是把 moamcp 那类能力**移植进 harness**（moamcp 侧的既有计划正是"移植到一个 harness 内，不采用插件模式"），那么 B 成立，语言问题基本被回答。

### 2.2 逐维度对照

| 维度 | Node（现役路径） | Go | 谁赢 | 备注 |
|---|---|---|---|---|
| **现成可复用资产** | minidb 18.9k + transcript 3.5k + tree-sitter-bash 4.8k + kosong 契约 0.9k + 适配器 5k + pi-tui 17.2k ≈ **50k 行 TS** | 需重写等价的 minidb / transcript / bash 解析器 / provider 契约与适配器 | **Node（压倒性）** | 纯 Go 方案至少新增 20–30k 行重写 |
| **provider SDK** | 官方 TS 全覆盖 | **官方 Go 全覆盖**（实测 2026-10 版本） | 平手 | 这层不是差异点 |
| **ACP** | 官方 SDK（omkc 已在用） | **无官方 Go SDK**，只有社区库 | **Node** | Go 侧要自维护类型生成与兼容性 |
| **MCP** | 官方（参考实现） | 官方 go-sdk v1.8.0 | 平手 | |
| **多 session 并发** | 共享 V8 堆 + 单事件循环（已知架构债）→ 需 session 级进程隔离 | goroutine 真并发；但若要进程隔离，优势消失 | **看隔离方案** | 回函明确：隔离收益 > 换语言 |
| **session 级进程隔离的代价** | 每进程 40–60MB 基线；启动 ~100ms+ | 每进程 ~5–15MB；启动 ~5ms | **Go** | 20 个 session：Node ~1GB，Go ~200MB |
| **CPU 密集**（大 diff / 索引） | 需 worker thread（omkc 用了 2 处） | 原生并行 | **Go** | omkc 已用 worker + resourceLimits 解决 |
| **GC / 停顿** | 有 | 无（GC 停顿 <1ms） | **Go** | 长驻 daemon 场景差异可见 |
| **取消传播** | `AbortSignal` 贯穿（omkc 已做 turn/step/queued 三层 + durable 事件双写） | `context.Context` 天生更顺 | 略 Go | Node 侧已有成熟实践 |
| **Windows 沙箱** | 需 C++/Rust helper 或 FFI | **`golang.org/x/sys/windows` 直接绑定受限令牌/JobObject/ACL** | **Go（强）** | omkc **完全无 OS 沙箱**——这是我们的差异化机会点 |
| **TUI** | pi-tui（已 fork 17.2k 行，含 8 处本地修正） | bubbletea + lipgloss，需重写 | **Node** | Go 生态原生更成熟，但要重做 |
| **单二进制分发** | 需 SEA / pkg / bun compile（omkc 已用 SEA） | 天然 | **Go** | |
| **迭代速度** | 快 | 中（编译周期 + 更啰嗦的代码） | **Node** | 早期阶段速度影响很大 |
| **嵌入式进现有 harness** | 同进程，共享契约 | 独立进程 + IPC，契约要跨语言 | **Node（决定性）** | |

### 2.3 成本量化

**Node 路线（复用为主）**
```
直接 fork/抄：minidb + transcript + tree-sitter-bash + kosong 契约 + pi-tui ≈ 45k 行（TS）
新写：durable 语义 + 多 agent 编排 + Windows 沙箱 helper + 评测
跨语言成本：0
```

**Go 路线（重写为主）**
```
重写：provider 契约 + 5k 行方言适配器 ≈ 6–8k 行（Go 更啰嗦）
重写：minidb 等价物（KV + WAL + 全文/trigram 索引 + 分片）≈ 8–12k 行
重写：transcript 等价物（幂等 ops + block-delta 订阅 + 视图注册）≈ 4–6k 行
重写：bash 解析器（纯 TS 无原生依赖的 Go 等价物）≈ 5k 行
重写：TUI ≈ 3–5k 行
新写：durable 语义 + 多 agent + Windows 沙箱（x/sys/windows 是 Go 的优势，此处反而省）
跨语言成本：若最终要并入 omkc，还要维护一层跨语言契约
```

**差距量级：约 20–30k 行的重写 + 一个跨语言边界，换来的实际收益集中在：内存占用、启动速度、CPU 并行、Windows 沙箱原生绑定。**

---

## 3. 三条可行路径

### 路径 1：全 TS（复用优先）
- 抄 kosong 三件套契约（~900 行）+ fork minidb / transcript / tree-sitter-bash / pi-tui
- 自己写 durable 语义（照 pi-durable 的设计文档）+ 多 agent 编排 + 权限策略链（照 omkc 13 条 policy 形态，改进为"只能收窄"）
- Windows 沙箱：写一个 Rust/C++ helper（Anthropic sandbox-runtime 路线）
- **代价**：几乎全部现成件都拿到手；**代价是 Windows 沙箱要跨语言，且现役 harness 的"共享事件循环"债会被继承**

### 路径 2：TS 核心 + Go helper（推荐候选）
- 上面全部，但 **Windows 沙箱 helper 用 Go 写**（`x/sys/windows` 直接实现受限令牌 + JobObject + ACL + 防火墙规则），IPC 走 JSONL over stdio
- Go helper 是**独立进程**，天然有 OS 级边界，崩溃不影响主进程
- 好处：TS 生态全拿，Windows 沙箱不跨 C++/Rust，且 Go 单文件分发简单
- **代价**：多一个语言、一个进程边界、一个 IPC 契约

### 路径 3：全 Go（自研优先）
- provider 用 Go 官方 SDK（实测齐全）；MCP 用官方 go-sdk；ACP 用社区库并自维护 schema 生成
- **必须接受**：minidb / transcript / bash 解析器 / TUI 全部重写；kosong 的方言归一（reasoning 三方混战、tool_call_id 清洗、finishReason 归一、context overflow 错误分类）自己踩一遍
- 独有优势：内存、启动、CPU 并行、**Windows 沙箱零跨语言**
- 独有劣势：ACP 无官方、现成件全丢、与现役 harness 的融合要跨语言

---

## 4. 我的建议

**路径 2（TS 核心 + Go helper）**，理由排序：

1. **沉没成本分布**：现成件（~50k 行 TS）全部在 TS 一侧。Go 只能吃到 OS 边界那一块（~1–2k 行），而那一块恰好是 Go 相对 TS 的**强项**——分工按优势划分，而不是按偏好划分。
2. **回函指出的最大风险是架构债不是语言债**：session 级进程隔离 + checkpoint/分段索引的收益 > 换语言。我们先做这两件事，无论哪个语言都受益。
3. **ACP 官方 SDK 只在 TS 侧**。ACP 是我们的一等议题（议题 8），不放弃官方 SDK。
4. **跨 provider thinking 交接是真空地带**（kosong 静默丢弃，pi 有 `transform-messages` 但不完整）——这是一个**我们可以做出差异化的点**，而它属于 provider 契约层，在 TS 侧我们能直接对照 kosong 的契约和 pi 的转换逻辑来设计。

**翻转条件**（满足任一则改推全 Go）：
- 新框架确定为**独立产品**，明确不并入 omkc，也不追求复用 omkc 资产
- 目标平台以 Linux 服务器为主、Windows 只是次要（那 Go 的沙箱优势贬值，而 Node 的现成件优势不变但相对权重下降）
- 明确要求单二进制分发 + 大量并发 session 的资源上限
- 团队 Go 熟练度显著高于 TS（工程现实，压倒技术论证）

---

## 5. 需要你拍板的问题

1. **终局形态**：独立产品，还是并入/移植进现役 omkc harness？（决定性）
2. **平台优先级**：Windows 是一等目标还是次要？（决定 Go helper 是否值得）
3. **是否愿意 fork**：现役所有包 `private: true`，复用 = fork。我们接受在 omkc 仓库里长出新模块，还是要做成完全独立的仓库？
4. **若选 TS，Windows 沙箱怎么办**：写 Rust/C++ helper（照 sandbox-runtime）/ 用 Go 写 helper / 第一版不做只做策略层（omkc 的现状）？
5. **评测框架**：现役没有（vis 是调试面）。我们自建还是等 pi-evals 的思路成熟？

---

## 参考

- 回函原文：`handoff/ho_3b68059bcbfc`（board key，global scope）
- 本地实测：pi 仓库 11 个包结构、`packages/ai/src/api/` 协议适配器清单、Go module proxy 版本元数据
- 详见 `00-overview.md` / `01-permissions-and-sandbox.md` / `02-runtime-and-stack.md` / `03-provider-sdks-and-go.md`
