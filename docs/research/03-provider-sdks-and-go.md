# Provider 协议的 Go SDK 覆盖 + Go 路线的复刻成本

> 触发问题：框架侧要从 pi 借 provider，但 pi 是 TS；团队倾向 Go。Go 到底覆盖不到哪些协议？"复刻"具体要复刻什么？
> 数据来源：`proxy.golang.org` 版本元数据 + GitHub API 拉取 pi 仓库一手目录（经 10808 代理）。**均为 2026-10-26 实测**。

---

## 1. 直接回答：五个主协议在 Go 上都有活跃的官方 SDK

| pi-ai 协议适配器 | 协议 | Go SDK | 模块路径 | 最新版 | 发布日期 |
|---|---|---|---|---|---|
| `anthropic-messages.ts` | Anthropic Messages | ✅ **官方** | `github.com/anthropics/anthropic-sdk-go/v2` | v2.3.0 | 2026-10-23 |
| `openai-completions.ts` | OpenAI Chat Completions | ✅ **官方** | `github.com/openai/openai-go/v3` | v3.86.0 | 2026-10-23 |
| `openai-responses.ts` | OpenAI Responses | ✅ **官方** | 同上（`client.Responses`） | 同上 | 同上 |
| `google-generative-ai.ts` | Google Generative AI（Gemini API） | ✅ **官方** | `google.golang.org/genai` | v1.76.0 | 2026-10-22 |
| `google-vertex.ts` | Google Vertex AI | ✅ **官方** | 同上，`vertexai: true` 切换 | 同上 | 同上 |

**结论：协议覆盖不是 Go 的障碍。** 四个（Google 两个算一个 SDK）全部是厂商官方维护，2026 年 10 月仍在高频发版。

### 1.1 两个会踩的版本陷阱

| 陷阱 | 现象 | 正确做法 |
|---|---|---|
| Go module 主版本路径 | `proxy.golang.org/.../openai-go/@latest` 返回 **v1.12.0 / 2025-07-30**，看起来像一年没更新 | 主版本 ≥2 必须带 `/vN` 后缀。查 `github.com/openai/openai-go/v3/@latest` 才是 v3.86.0。`anthropic-sdk-go` 同理，v1 路径停在 v1.84.0，v2 已是 v2.3.0 |
| Google 旧库 | `google.golang.org/generative-ai`（旧）已于 2025-11-30 弃用 | 统一用 `google.golang.org/genai`（2025-05 起 GA，覆盖 Gemini Developer API + Vertex AI） |

### 1.2 pi 支持但你没点名的协议（顺带核了）

| 协议 | Go 现状 | 备注 |
|---|---|---|
| AWS Bedrock Converse | ✅ `github.com/aws/aws-sdk-go-v2/service/bedrockruntime` v1.63.7（2026-10-22） | AWS 官方 |
| Mistral Conversations | ❌ 无官方 Go 模块 | `github.com/mistralai/mistral-go` 在 module proxy 上不存在；只能走 OpenAI 兼容端点 |
| Azure OpenAI Responses | ⚠️ 复用 openai-go，改 baseURL | pi 用独立的 `azure-openai-responses.ts` 处理 |
| OpenAI Codex Responses | ❌ 内部协议 | 无法第三方实现 |
| pi-messages / typesafe-system-one / llama-cpp-classify | ❌ pi/厂商私有 | — |

### 1.3 协议层之外

| 协议 | Go 现状 | 版本 |
|---|---|---|
| **MCP** | ✅ **官方** `github.com/modelcontextprotocol/go-sdk`（MCP 组织 + Google 维护，支持 tools/resources/prompts、stdio/HTTP/SSE 传输） | v1.8.0 / 2026-09-04 |
| MCP 社区 | `github.com/mark3labs/mcp-go` | v1.3.0 / 2026-10-14（多数项目已迁往官方） |
| **ACP** | ⚠️ **无官方 Go SDK**。社区两个：<br>`github.com/caelis-labs/acp-go-sdk` — v1.5.1 / 2026-10-16，声明 ACP wire protocol v1、schema-v1.25.0，并与官方 TS/Rust SDK 做四方向互操作矩阵<br>`github.com/coder/acp-go-sdk` — v0.13.5 / 2026-06-02，含 Claude Code / Gemini CLI 桥接示例 | — |

> ACP 官方 SDK 只有 TypeScript / Python / Rust / Kotlin / Java。Go 属于社区实现路径——**这是 Go 路线唯一真正"没有官方背书"的协议**。

---

## 2. 关键发现：pi 远不止 provider 层

一手读取 `earendil-works/pi` 仓库，根包有 **11 个包**：

```
agent  ai  chord  codemode  coding-agent  durable  env  evals  mcp  telemetry  tui
```

对照我们最初列的横切面，**其中 5 个正好一一对应**：

| pi 包 | 对应我们的议题 | 内容 |
|---|---|---|
| `pi-ai` | 议题 1 provider | 统一 provider、模型目录、auth、事件协议、**`transform-messages.ts`（跨 provider 上下文交接）** |
| `pi-agent` | 议题 5 agent loop | agent runtime、工具调用、状态 |
| `pi-coding-agent` | 议题 2/3/9 | `createAgentSession`、`SessionManager`（树形 JSONL）、扩展机制、技能、提示模板 |
| `pi-durable` | **横切面 C + B** | **完整的 durable agent harness**（见下） |
| `pi-env` | 议题 4 运行时 | 远程执行环境，**Rust daemon over SSH**，Windows 走 PowerShell 部署 |
| `pi-chord` | 多进程/多 agent 底座 | 跨进程对象 + 热重载 + JSON delta + IPC/WebSocket link |
| `pi-mcp` | 议题 7 | MCP 支持 |
| `pi-evals` | 横切面 E | 评测 |
| `pi-codemode` | 工具形态 | Code Mode |
| `pi-tui` | 议题 10 | TUI 组件库 |
| `pi-telemetry` | 横切面 E | 遥测 |

### 2.1 `pi-durable` 已经是我们要的"长时程 harness"

README 原话：*"A durable agent harness. Conversations, model turns, tool calls, and your own state are committed to storage before anything is shown. If the process dies mid-turn, reopening the storage picks the work up where it stopped."*

它已经实现的语义（读自 `packages/durable/README.md`，885 行）：

| 我们讨论过的横切面 | pi-durable 的现成实现 |
|---|---|
| **幂等性** | 工具可声明 `replay: "safe"` —— 崩溃重跑会找到同一个 child 和同一次 submission |
| **多 agent 隔离** | 子 agent 通过 `ownership: { kind: "task", taskId }` 创建，**ownership index 让重跑复用同一个 child**（不是新建） |
| **上下文压缩** | `reserveTokens` / `keepRecentTokens` / `backgroundTokens` **三水位**；支持 `modelOverrides` 按模型覆盖阈值；上下文溢出时自动压缩并重试一次；`beforeCompact` hook 可拒绝或自备摘要 |
| **上下文交接** | `reset("handoff note")` —— 模型看不到旧条目但条目留在存储里；工具可返回 `control: { handoff }` 触发同一机制；队列语义明确（reset 结束当前 run，未答输入 settle 为 `unanswered`/reason=reset） |
| **取消语义** | `abort()` 撤回队列输入、中止当前任务、直到 idle 才 resolve |
| **多会话/并发** | 每个 conversation 独立；"busy conversation"、watch 快照 + delta、per-conversation agent |
| **成本核算** | `pi.usage` 统计，压缩产生的花费也计入 |

**这份清单几乎逐条命中我们上两轮讨论出的横切面缺口（A/B/C/D/E）。**

---

## 3. Go 路线的复刻成本清单（按难度分级）

如果确定走 Go，"复用 pi"实际退化为**只复用 pi-ai 的 provider 层**——而那一层恰好是 Go 官方 SDK 最齐的地方（好消息）。代价在下面：

| 级别 | 要复刻的东西 | 难度 | 说明 |
|---|---|---|---|
| **P0** | 统一 provider 层：Model/Provider/Api 判别、事件归一化、模型目录、auth 解析、token/成本核算 | 中 | 约 3–5k 行机械代码；Go 官方 SDK 提供原料 |
| **P0** | **`transform-messages` 语义**：Anthropic thinking → OpenAI 兼容消息格式的跨 provider 上下文交接 | **高** | pi-ai 里最难自己发明的一块。Go 路线下**必须复刻**，否则换模型即断上下文 |
| **P1** | agent loop + 工具抽象 + 错误模型 + 取消 | 中 | 常规工作 |
| **P1** | 会话树（JSONL，`id`/`parentId`，compaction / branch_summary / label 节点） | 中 | 格式明确，照抄结构即可 |
| **P2** | **durable 语义**：提交先于可见、崩溃续跑、tool 级 `replay: safe`、ownership index、压缩三水位、handoff 队列 | **最高** | 这是整个项目最难自己长出来的部分 |
| **P2** | chord 式跨进程对象模型（delta、hot reload、principal 权限） | 高 | 如果要做多 agent 跨进程通信 |
| **P2** | `pi-env` 远程执行（SSH + daemon 部署 + host key 校验） | 中 | Go 可用 `golang.org/x/crypto/ssh` 自己写，比 pi 的 Rust daemon 更统一 |
| **P3** | TUI | 低 | Go 有 bubbletea + lipgloss，**这块 Go 生态比 TS 更成熟** |
| **P3** | ACP agent 侧 | 中 | 无官方 Go SDK，只能基于社区库或自己按 schema 生成类型 |

---

## 4. 诚实的对照：Go 在这个场景的真实优势

| 维度 | Node | Go | 谁赢 |
|---|---|---|---|
| 多 session 并发 | 单事件循环（协作式） | **真并发 goroutine，栈 2KB 起** | **Go** |
| GC 停顿影响全局 | 有（长驻 daemon 场景明显） | **无** | **Go** |
| 取消传播 | `AbortController`（可用但需手工贯穿） | **`context.Context` 天生设计** | **Go** |
| 子进程取消 | `spawn` + `AbortSignal` + 手工 kill 树 | `exec.CommandContext` + `Process.Kill` | Go（略优） |
| 单二进制分发 | 需 SEA / pkg / bun compile | **天然** | **Go** |
| Windows 沙箱原生绑定 | 需 C++/Rust helper 或 FFI | **`golang.org/x/sys/windows` 直接调 `CreateRestrictedToken` / JobObject / ACL** | **Go** |
| 流式事件模型 | AsyncIterable / Web Streams | channel + `io.Reader` | 平手 |
| provider SDK 成熟度 | 全（TS 为主） | **全（官方）** | 平手 |
| MCP SDK | 官方（参考实现） | **官方** | 平手 |
| ACP SDK | **官方** | 无官方（社区） | **Node** |
| 现成 harness 可复用度 | **高**（pi 全家桶） | 无 | **Node** |

**Go 的优势集中在"运行时/系统层"，短板在"现成件"。** 而我们这个项目缺的恰好是系统层的东西（多 session 并发、Windows 沙箱、长时程恢复），现成件缺的恰好也是系统层的（pi 的 durable 在 TS 里）。

> 关键洞察：**Go 不是"更好的语言"，是"更匹配的形状"**——但 pi-durable 恰恰是整个体系里最需要"行为一致性"的部分，而它恰好是最难跨语言搬运的。

---

## 5. 推荐路径

### 5.1 混合路线（推荐）

```
Go 核心（provider / loop / session / policy / sandbox / MCP / ACP / TUI）
  └─ 引用 pi 的**设计文档**作为规格书，不引用其代码
```

- **provider 层不用 pi-ai**，直接用 Go 官方 SDK + 自己写薄统一层；**但把 `transform-messages` 的语义当作必做项照着实现**
- **ACP 用 `caelis-labs/acp-go-sdk`**（活跃度更好、声明与官方 SDK 互操作验证过），并预留按官方 schema 重新生成类型的路径
- **MCP 用官方 `modelcontextprotocol/go-sdk`**
- **TUI 用 bubbletea + lipgloss**
- **Windows 沙箱用 `golang.org/x/sys/windows` 原生实现**，省掉 helper 进程
- **durable 语义分两步**：先做 checkpoint/resume + 工具级幂等（`replay: safe` 等价物）+ ownership index；压缩三水位放第二阶段

### 5.2 如果时间预算允许的更激进选项

**保留 pi 的 TS 实现作为"参考实现"甚至"双实现"**：Go 版本和 pi 版本共享同一套**测试用例与评测集**（`pi-evals` 的思路可借鉴），用行为对比来保证一致性。这比纯靠读文档复刻可靠得多。

### 5.3 阶段划分

| 阶段 | 目标 | 验收 |
|---|---|---|
| S0 | Go provider 薄层：4 个协议 → 统一事件流 | 同一份请求在 Anthropic / OpenAI(Chat+Responses) / Google(Gemini+Vertex) 上跑出同构事件序列 |
| S1 | agent loop + 工具 + 错误/取消 + JSONL 会话树 | 单 agent 能改代码并通过测试 |
| S2 | durable：先 checkpoint/resume + 幂等工具 | 进程中途 kill 后能续跑且不重复副作用 |
| S3 | 多 agent：task ownership + child task + worktree 隔离 | 并行子 agent 互不污染 |
| S4 | 压缩三水位 + handoff | 长会话不溢出，压缩后可回溯 |
| S5 | ACP（agent 侧）+ MCP + 策略/沙箱 + TUI | 能被 Zed/IDE 驱动 |

---

## 6. 待核对项

1. `github.com/anthropics/anthropic-sdk-go/v2` 的流式 API 形态（`Messages.NewStreaming` / 累积器）与 **Bedrock 支持**（仓库内有 `aws/`、`bedrock/` 目录，需确认是否官方支持 Bedrock 而非仅附带）
2. `google.golang.org/genai` 对 **Vertex AI 专属特性**（如 Vertex 的 grounding / 私有模型）覆盖到何种程度
3. `caelis-labs/acp-go-sdk` 与 `coder/acp-go-sdk` 的实际完整度、是否覆盖 `session/load`、fs/terminal 可选能力
4. pi-ai `transform-messages.ts` 的完整转换规则（跨 provider 上下文交接是 Go 路线下的必做项，需要精读）
5. `pi-durable` 的 `docs/` 设计文档清单（长时程语义的事实规格来源）
6. 本机 Windows 版本是否支持所需的 `x/sys/windows` 调用（JobObject / ACL / 受限令牌）

---

## 参考来源

- Go module proxy 元数据：`https://proxy.golang.org/<module>/@latest`（经 10808 代理）
- pi 仓库目录：`https://api.github.com/repos/earendil-works/pi/contents/...`（经 10808 代理）
- pi-ai 协议适配器清单：`packages/ai/src/api/`（一手，含 `transform-messages.ts`、`openai-prompt-cache.ts`、`constrained-sampling.ts`）
- pi-durable README：`packages/durable/README.md`（885 行）
- pi-env README：`packages/env/README.md`（Rust daemon over SSH）
- pi-chord README：`packages/chord/README.md`（跨进程对象 / 热重载 / delta）
- pi LICENSE：MIT（GitHub API 确认）
- MCP 官方 Go SDK：`github.com/modelcontextprotocol/go-sdk` v1.8.0
- ACP 社区 Go SDK：`github.com/caelis-labs/acp-go-sdk` v1.5.1、`github.com/coder/acp-go-sdk` v0.13.5
