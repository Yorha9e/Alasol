# Agent Harness 框架调研总览

> 状态：调研阶段，2026-10 起始
> 范围：以 coding harness 侧为主的 agent 框架。目标不是"再做一个 agent 框架"，而是确定一个多 agent + 长时程自主方向的 harness 架构。

## 0. 原始议题清单（需求方给出）

1. Provider 层 —— 计划从 pi 借用
2. 基本信息存储方式与存储形式（历史记录、环境信息）
3. 环境变量与历史记录读取方式
4. Agent 容器与运行时（主打多 agent + 长时程自主任务）
5. Agent loop 实现
6. 上下文管理
7. Function 与 MCP 支持
8. ACP 支持
9. 预留外接端点（自定义工作流、subagent、子 agent 的工具列表/提示词/权限范围、其他标记）
10. UI：TUI 与 GUI 选型

**核心判断**：这份清单覆盖了"Agent 能干什么"，但没覆盖横切面。横切面决定第 1–10 项怎么实现，且后期改动最贵。

---

## 1. 缺失的横切面（按必须现在讨论 / 可延后排序）

### A. 权限与沙箱 — 必须现在定（详见 [01-permissions-and-sandbox.md](./01-permissions-and-sandbox.md)）
- 三层分离：决策层（Prompt 自限）→ 策略层（规则 + 审批）→ 强制层（OS 沙箱）
- 沙箱后端选型：macOS Seatbelt / Linux bubblewrap+seccomp / Windows（最贵，三条路）
- 审批流：谁发起、谁应答、是否进 session log
- 凭据注入与脱敏
- 不可信输入边界（MCP 输出、网页、issue 都是 prompt injection 面）

### B. 多 Agent 编排语义 — 必须现在定
- 拓扑：主从 subagent / 同伴 / supervisor / 黑板
- 隔离维度：context window、工具集、权限、工作目录
- 通信：返回结果 vs 共享黑板 vs 事件总线
- 并发与 token 配额、模型分配（子 agent 可用更便宜的模型）
- 多 agent 改同一文件的冲突合并
- 生命周期与僵尸回收
- **与议题 9 的切分**：议题 9 是"单 agent 的可插拔点"，B 是"多 agent 的运行时语义"，必须分开设计

### C. 长时程自治的执行语义 — 必须现在定
- checkpoint / resume / replay
- 幂等性：工具副作用必须先落盘再执行，否则崩溃恢复会重复副作用
- 停止条件：预算 / 轮次 / 时间上限 / 独立 verifier（不能用"直到完成"）
- 循环检测、上下文腐化检测、watchdog
- 后台定时任务与心跳

### D. 错误模型与取消语义 — 必须现在定
- 模型层错误（限流 / 超长 / 内容过滤）与工具层错误如何统一表达给模型
- 可重试 vs 致命的分界
- 中断：用户 ESC、工具超时、session 级取消；取消后状态如何落盘
- 并行工具部分失败的处理

### E. 可观测性、评测与调试 — 可延后到骨架之后
- 结构化事件流 → OTEL；会话回放/时间旅行（与 C 的事件日志共用底座）
- 评测集（SWE-bench / Terminal-bench 类 + 自建回归），重点是"换模型 / 改 prompt / 改工具描述"后有基线可比
- trace 视图、diff 回滚

### F. 配置、身份与发布 — 可延后
- 配置分层（global / project / local + profile）
- 凭据存储（keychain）、多账号
- monorepo 包边界、core/CLI/UI 三层、语义化版本、插件分发（可参考 ACP Registry）

### G. UI 的隐含前提 — 需要在选型前先回答
- 比"TUI 还是 GUI"更前置：**core 是否 UI 无关**——同一 headless core 能否同时服务 TUI / GUI / ACP client / headless CLI / JSONL RPC
- TUI 技术路线：自研差分渲染 / ratatui(Rust) / Bubble Tea(Go) / Ink / Textual(Python)
- GUI：VS Code 扩展 / Electron / Tauri / 本地 Web
- Windows 终端兼容、可访问性、i18n

### H. ACP / MCP 之外的互操作
- ACP 的 client capability 有 `fs`/`terminal` 等可选能力，**client 不提供 fs 能力时 agent 侧如何降级**必须提前设计
- ACP 官方扩展机制：`_meta` 字段 + `_` 前缀自定义方法 + 初始化时声明自定义 capability → 正好承接议题 9
- AG-UI（agent↔前端）、A2A（agent↔agent）、LSP（诊断）、git（patch 应用）
- MCP：当前稳定 spec `2025-11-25`；二级资料称 `2026-07-28` 修订将 Sampling / Roots / Logging 标记弃用（12 个月弃用期，只保留 Elicitation）—— **必须以官方 changelog 核对**，直接影响 tool 层是否实现采样

### I. 上下文管理需要拆成子议题
- 装配层次：system / 项目规则 / skills / tool schema / 历史 / 检索结果 / 产物
- 按需披露（skill 渐进加载、tool schema 裁剪）
- 压缩触发阈值与压缩产物形态（结构化摘要 + 原文指针，保证可回溯）
- token 预算分配、prompt caching 与 cache key 设计

---

## 2. 关于"从 pi 借用 provider"的前提对齐

**已核实（来源：第三方源码拆解文章，官方仓库直读在本机被网络策略拦截）**
- 仓库/包已改名：`badlogic/pi-mono` → `earendil-works/pi`，包名 `@mariozechner/pi-*` → `@earendil-works/pi-*`
- `pi-ai` 抽象切法：`Model = 元数据 / Provider = 行为`；`Model<Api>` 用 discriminated union 绑定协议（`anthropic-messages` / `openai-completions` / `openai-responses` / `google-generative-ai` …）；provider 独有特性通过 per-provider options 保留，不做有损降级
- 统一事件流 12 类：`start / text_* / thinking_* / toolcall_* / done|error`，三类内容统一"开始-增量-结束"；`Models.stream()`（原始事件）与 `streamSimple()`（归一化事件）两个入口
- 显式支持**跨 provider 上下文交接**（把 Anthropic 的 thinking 轨迹转成 OpenAI 兼容消息格式）
- 会话存储：`~/.pi/agent/sessions/--<编码后的cwd>--/<timestamp>_<uuid>.jsonl`，JSONL 追加写，**树形结构**（每条 entry 带 `id` / `parentId`，`leafId` 指向当前分支），entry 类型含 `message` / `compaction` / `branch_summary` / `label` / `custom`；首行是 `SessionHeader`（`type/version/id/timestamp/cwd/parentSession`）
- `SessionManager.inMemory()` / `SessionManager.open(file)` 两种形态

**待自行核对**
- LICENSE（社区多说 MIT）、当前版本号
- `pi-tui` 究竟是 TS 自研差分渲染还是基于 ratatui（中文文章两种说法都有，包名为 TS 包，倾向自研）

**两个决策点**
1. fork `pi-ai` 还是直接依赖 npm 包？
2. 是否保留"无损跨 provider 上下文交接"？成本高，但长时程任务换模型时体验差别很大。

---

## 3. 必须先拍板的非功能约束（不定，后面全是返工）

1. 语言/运行时：TS / Rust / Python / Go？（pi 是 TS；Rust 换来性能与单二进制）
2. 平台优先级：Windows 优先还是 Linux 优先？（沙箱与容器在 Windows 上最贵）
3. 开源还是内部？许可证策略（MIT / Apache-2.0 / BUSL）—— 决定能否直接复用 pi 代码
4. 本地单用户 vs 服务端多租户 —— 决定 session 存储、凭据、审批模型
5. 差异化定位：相对 pi / Claude Code / Codex / OpenCode / goose，我们做什么、明确不做什么
6. 成功标准：第一条垂直切片是什么（例如"在本地仓库里自主改一个 bug 并通过测试"）

---

## 4. 建议的讨论顺序

| 批次 | 内容 | 产出 |
|---|---|---|
| 第一批（地基） | 非功能约束 → 权限与沙箱 → 状态存储模型（议题 2/3 落地） | 架构决策记录 ADR |
| 第二批（骨架） | provider 抽象边界 → agent loop → 上下文装配 → 工具与错误模型 | 可运行骨架 |
| 第三批（长尾） | 多 agent 编排 → ACP/MCP → UI → 评测 | 可用产品 |

---

## 5. 一句话概括缺口

清单没回答四个问题，它们分别对应缺失的横切面：

- "它能安全地干多久" → A 权限与沙箱
- "干错了怎么回滚" → C 持久化与幂等
- "多个怎么不打架" → B 多 agent 编排
- "你怎么证明它干对了" → E 评测与验收

---

## 参考来源

- ACP 官方文档：https://agentclientprotocol.com/overview/introduction ，https://agentclientprotocol.com/protocol/overview
- Claude Code 权限文档：https://code.claude.com/docs/zh-CN/permissions
- Codex 沙箱文档：https://developers.openai.ac.cn/codex/windows
- Anthropic sandbox-runtime：npm `@anthropic-ai/sandbox-runtime`（Apache-2.0）
- Claude Code 沙箱环境：https://code.claude.com/docs/zh-CN/sandbox-environments
- MCP 规范（2025-11-25）与 2026-07-28 修订 —— 需以官方 changelog 核对
