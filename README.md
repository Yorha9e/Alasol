# AI Agent Harness

面向 coding harness 侧的 agent 框架（产品名 **alasol**）。设计已收敛，处于**施工**阶段——从 [`docs/research/11-construction-plan.md`](docs/research/11-construction-plan.md) 开始读。

## 文档

| 文档 | 内容 |
|---|---|
| [docs/research/00-overview.md](docs/research/00-overview.md) | 框架调研总览：原始议题清单、缺失的横切面、pi provider 借用前提、待拍板的非功能约束、讨论顺序 |
| [docs/research/01-permissions-and-sandbox.md](docs/research/01-permissions-and-sandbox.md) | 权限与沙箱深挖：三层模型、规则求值链、各平台沙箱后端选型、多 agent 隔离、落地方案草案 |
| [docs/research/02-runtime-and-stack.md](docs/research/02-runtime-and-stack.md) | 运行时与技术栈选型：多 agent / 多 session 下 Node 的真实瓶颈、TS 核心 + 原生 helper 混合栈、薄 provider 契约解耦方案 |
| [docs/research/03-provider-sdks-and-go.md](docs/research/03-provider-sdks-and-go.md) | Provider 协议的 Go SDK 覆盖（实测版本）、pi 全部 11 个包的结构、Go 路线的复刻成本分级、推荐落地阶段 |
| [docs/research/04-handoff-reply-and-language-decision.md](docs/research/04-handoff-reply-and-language-decision.md) | omkc 维护方回函（ho_3b68059bcbfc）对照我们的结论、Go/Node 逐维度选型、三条路径与成本量化、待拍板问题 |
| [docs/research/05-architecture-pivot.md](docs/research/05-architecture-pivot.md) | **架构转向（现行结论）**：独立产品约束下的语言定案（核心与协调用 Go）、数据中心式协调 / 扩展热插拔 / 存储层重设计三大核心架构 |
| [docs/research/06-skeleton-design.md](docs/research/06-skeleton-design.md) | **骨架规格（当前施工依据）**：contract-first 分层与依赖方向、12 类契约、16 个扩展点地图、工作流模型（能力包 / 设计哲学 / 部件与验收目标）、上下文策略（含分级压缩）、v0 范围与验收（酸测试）、Go 目录结构 |
| [docs/research/07-design-review-checklist.md](docs/research/07-design-review-checklist.md) | **设计决策总表（唯一裁决来源）**：280 条编号决策项（目标 / 技术栈 / 分层 / 契约 / 扩展 / 事件 / 存储 / 权限 / env / 工具角色 / 工作流 / 上下文 / 协议 / UI / 工程），每条含主张、备选、状态与 ★ 待议标记 |
| [docs/research/08-identity-model.md](docs/research/08-identity-model.md) | **身份与 ID 设计**：13 个 id 命名空间表、ULID 选型、字符集纪律、provider id 黑盒规则、MCP 侧标识隔离、10 条"工具投递失败"死法清单 |
| [docs/research/09-package-boundaries.md](docs/research/09-package-boundaries.md) | **包边界（待审）**：每个包的"为什么存在 / 什么变化会导致它变 / 什么不会导致它变"、依赖硬约束、三个易腐化边界点、审阅清单 |
| [docs/research/10-design-review-report.md](docs/research/10-design-review-report.md) | **外部审阅报告**：7 阻断 / 16 应修 / 9 建议 + 14 条矛盾速查 + 「只改三处」建议 |
| [docs/research/11-construction-plan.md](docs/research/11-construction-plan.md) | **施工计划（施工 session 入口）**：阶段划分与硬验收、20 条施工期陷阱清单、改决策的纪律 |
| [docs/research/12-roadmap.md](docs/research/12-roadmap.md) | **版本路线图（v1–v4）**：多 session 数据中心 / 深度执行与安全 / 生态互操作 / 规模化评测——每条给 07 决策点引用，动工前收敛 ★ 条目 |
| [spike/sqlite-drivers/README.md](spike/sqlite-drivers/README.md) | **存储选型 spike（已跑出结果）**：纯 Go SQLite 驱动实测——并发写 / WAL / busy_timeout / 跨进程 / 交叉编译，结论与网络说法纠正 |

> ⚠️ `02` 与 `04` 的选型结论已被 `05` 修订（前两者的前提"复用 pi / omkc 资产"不再成立）。`01`、`03` 仍然有效。
> `07` 是**活文档 + 唯一裁决来源**——每轮讨论后回填状态，★ 项逐个收敛；数字与清单只在源头定义一次，其余引用不重抄（D21 纪律）。
> 施工从 `11` 开始读。

## 当前状态

> **本项目已全量交接**：设计侧工作收尾，后续施工全部在另一 session 进行。
> 施工入口 → [`docs/research/11-construction-plan.md`](docs/research/11-construction-plan.md)（含**环境与工具链事实**、阶段划分、硬验收、20 条施工期陷阱清单）。
> **v0 已完成并通过终验收**（2026-10-27，ACCEPTED）：阶段 0–8 全部完成（每阶段通过 critic→reviewer 退出评审循环），v0 酸测试 11 条逐条实测核销，**真模型端到端已实测**（本地网关 + gpt-4o-mini，模型自主调工具、三段事件完整）。用法：`alasol run/chat/serve/resume/models`。命名：`alasol` · `github.com/Yorha9e/alasol`。后续版本规划 → [`docs/research/12-roadmap.md`](docs/research/12-roadmap.md)（v1–v4）。

- 目标已收敛：**搭一套可自定义、可拓展的骨架**（ACP 等具体特性后置），在骨架之上向任意方向扩展
- **骨架验收标准（酸测试）**：把任意一个扩展点的实现整个换掉，core 代码一行不改
- 议题清单已整理，缺口已识别（权限沙箱 / 多 agent 编排 / 长时程执行语义 / 错误与取消 / 评测 / 配置发布 / UI 前提 / 协议互操作 / 上下文子议题）
- 权限与沙箱已完成第一轮深挖，含落地方案草案与分阶段计划
- 方向已转向（2026-10-26）：新框架定为**独立产品**，不并入 omkc、不复用其包；Windows 为一等目标 → 结论改为 **Go 做核心/协调/存储/策略/TUI，前端（GUI/IDE）语言自由并通过协议接入 hub**。详见 05
- 骨架规格已出（06）：contract-first 分层、12 类契约、16 个扩展点、v0 范围与验收清单
- 已发出 handoff `ho_2a14886be7a6`（userglobal），向现役 harness 维护方索取既有实现清单与选型权威答案（已获回函 `ho_3b68059bcbfc`）

## 约定

文档中的事实性内容区分三类来源：官方文档 > 第三方源码拆解文章 > 推断。无法在本机直接核实的内容（一手仓库/包被网络策略拦截）一律在文末"待核对项"列出，不当作已验证事实使用。
