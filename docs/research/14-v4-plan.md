# 14 · v4 排期表

> **定调（2026-10-31 用户）**：v4 解冻——"**现在做的就等于做 v4**"，版本叙事合并：v3-S6 起的施工都属 v4 阶段，本表是唯一排期口径。
> **SSH 远程定调（2026-10-31 用户）**：远端经 **SSH 连接 harness 机器**进行远程控制；**鉴权交系统/SSH 自管**（密钥体系），我们只做 SSH 适配层、**不做自研鉴权**；先预留接口。
> 对应 07 拍板行内注记（★ 22→0 那批），细节以 07 为准（D21：不重抄）。

---

## 排期（按优先级；做完一笔勾一笔）

| # | 项 | 内容 | 对应 07 | 状态 |
|---|---|---|---|---|
| 1 | **v3-S6 subagent 产品化** | host 装配 agent_type 注册表（内建 **planner/coder/critic/reviewer/observer** 五类 + TOML 自定义）→ `loop.Config.Agents` 接线 → spawn/complete 对模型可见（三闸门+策略链既有）→ 协议面/GUI agent 树真能派活。**W6 方案 A**：agent_type 显式声明 context_strategy，未声明=append 默认（**不继承主 agent**）；项目级资产（rules/常识区）作显式共享开关。K20：observer=只读型（spawn 权限只收窄）。W12：归属 session+父 agent。验收=里程碑部件（W7） | W5/W6/W7/W11/W12/W13/W15(部分)/W18/W9b/K20/R8/H22/H23 | **✅ 完成（2026-10-31，v3-S6-A+B，评审循环至 clean）**；挂账：/btw fork 不装配 agent 树（→ 本表第 7 项旁支收口） |
| 2 | **SSH 远程适配** | 预留接口 + 适配层（远程控制 harness 机器）；鉴权交系统。含 K19 跨机 | N4b/N5/K19 | **✅ 完成（2026-10-31，v4-2-A+B，评审循环至 clean）**：RemoteExecutionEnv（K8 第三档，系统 ssh）+ 隧道面（Origin 互认/ssh -L）+ `alasol acp` ACP 端点（现成协议客户端经 `ssh host alasol acp` 接入）；挂账：Origin 监听地址注入、RemoteExecutionEnv Fs/Proc 面 |
| 3 | **blob + 向量（RAG）** | G2e blob 实现（接口已立、`BlobMeta.Ref` 挂接点已备）→ G2d 向量/RAG（Retriever 保留位） | G2e/G2d | 排期 |
| 4 | **预算与重试补完** | W15 完整预算功能（成本上限等） + M6（provider 级小重试+通用重试组件） | W15/M6 | ✅ 完成（2026-11-01 v4-4-A + v4-4-B）：**M6 全笔**（v4-4-A `core/retry` + provider 级小重试）；**W15 成本预算闸门（顺手小刀）**——**默认仍不设上限**（W15 拍板），`[budget].max_tokens` / `host.Config.BudgetMaxTokens` 配了才在 **Run 前/后** 设检查点（超限拒续本轮 / 越界告警），超限启动即拒；**B 半 = WAL 预写日志**辅带落地（本刀主体，见 11 v4-4 行；**同日 critic 修复轮九条**：blob.plan 写点挪到 intent 前、加 turn.failed 区分"普通失败"与"崩溃残留"、>1MB 行不中断扫描、Close 补 fsync、酸测试补 payload 比对、Warn 文案改准、fork 旁支同口径挂 WAL + /btw 接预算闸门、文档乱码修复、resume 失败也打 WAL 摘要；**同日终查 findings 四条修复轮**：对账改**按记录流分段归组**（轮次号跨 Run 撞号 → 悬空轮次不再被后一次 Run 的 turn.end 抹平 = 修假阴性）、补 ctx 取消路径的 turn.failed 钉子、补 planExternalize 的 spawn/replay 两路径钉子、补 Close 的 fsync 观测钉子）。**挂账**：W15 的逐轮刹车（turn 内成本预算闸门）未做（需 loop 侧接线）；重试过程留痕未接（provider 上下文无 WAL 句柄，不为它穿层） |
| 5 | **E2 剩余** | extension runtime（E6 热重载不在内——W11 已定不做热重载）+ **MCP server**（B 类） | E2 | ✅ 完成（2026-11-01 v4-5）：**A 类剩余收口 + E3/E5 做实**（`core/registry` 统一注册表：能力裁决 fail-close、裁决期 panic 隔离、撤销也 fail-close；观测面 = `GET /api/extensions` 只读快照，空能力列表归一为 `[]`）；**B 类 MCP server 落**（`alasol mcp-serve`，`runtime/mcpserver`：stdio 传输，按**白名单**把 harness 工具面暴露成 MCP tools，鉴权 = 谁拉起谁负责；暴露面 fail-close：空名单零暴露、`mcp__*` 内部命名空间不暴露、注册表 State 参与暴露与执行判定、非法工具名不挂载、**审批闸不适用**）；**compaction / assembler / scheduler 三个 kind 留位、当前不建机制**（本项拍板"extension runtime 重点不是新机制"）；挂账：POST /api/extensions/{name}/enable\|disable 仍不挂（注册表 State 尚未门控 loop 侧解析路径，挂了就是假旋钮）、GUI 面板 / IDE 插件 / 第三方工具 / 不可信扩展仍属 B 类后续 |
| 6 | **观测/工程缺口** | telemetry/trace（L1/L2/D19）、长历史分段惰性加载（G5）、模型路由（I7）、git 状态按需查（J3，07 口径） | ·83 真缺口部分 | **b 半 = telemetry/trace（L1/L2/D19）✅ 完成（2026-11-01）**：事件流即 trace（不另建 trace 系统）+ 独立 telemetry 管道（默认关/脱敏/单向；`runtime/telemetry` noop/file/otlp 留位 + host `[telemetry]`，见 07 L1/L2/D19 行内）；**G5 ✅ 完成（2026-11-01）**：长历史分段 + 偏移索引 + 惰性加载——core.Store 只增只读增量面 `SliceReader.ReadSlice`（seq 闭区间分段读，Append/ReadFrom 一字不改、零契约改动）+ sqlite 实现（seq 主键范围扫描 + LIMIT）+ 消费点接上（host storeTextSource recall 逐段惰性读，不认 SliceReader 自动退回 ReadFrom；酸测试关/开逐字节一致、变异自证钉子红，见 07 G5 行内）；I7 路由扩展点 + J3 git_status 工具已落（530b7ae）——**第 6 项全件收口** |
| 7 | **原 v4 冻结遗留** | 跨进程托管注册（G2/G25 根治）、hub 归属字段（G12）、run_id 归组（G13）、policy 结构化成因码、G17 git 观测、评测（L4/L5） | 挂账注记 | **GUI 反馈三件已根治（2026-11-01 v4-7）**：① 跨进程托管注册（G25）——core/discovery `Claim`/`HostOf`（Activate 之上登记宿主 hub 进程标识，冲突 fail-close 拒绝接管）+ host loopFor 接线 + `Hub.ID()`；② hub 归属字段（GUI G12）——/api/sessions 行增 `host_hub`（与 hostable 组合，HTTP 面只增）；③ run_id 归组（GUI G13）——`ModelDeltaPayload` 增可选 `run_id`（F7 只增：schema→codegen→INVENTORY→样本→TS 三件套），loop Run 起始生成 ULID 随 delta 带出，订阅端按 (run_id, turn) 归组；变异自证：不带 run_id → 归组钉红。钉子：discovery/host_claim_test.go、host TestSessionsHostHubField+TestLoopForClaimConflictFailClose、loop TestModelDeltaRunIDPresent+TestModelDeltaConcurrentRunsGroupByRunID。**后半三小件已根治（2026-11-01 v4-7 后半）**：④ policy 结构化成因码——`Decision.code`（DecisionCode 十码，F7 只增全链）+ core/policy·core/registry 全出口填码 + policy_snapshot 透出 + isNoAnswerDeny 改按 code 判（07 D13/D14 行内）；⑤ G17 git 观测——`GET /api/git`（共享 SnapshotGitStatus 探测逻辑；HTTP-only 不进事件流、拉式现采、非仓如实 repo=false，07 J3 行内）；⑥ 评测（L4/L5）——`alasol eval -session <id>`（事件流即数据集、逐 turn 指标单行 JSON、不建独立评测框架；07 L4/L5 ·→✅ 合计同步）。钉子：policy/registry 成因码钉 + host TestGitEndpoint 三钉 + cmd TestEvalCLI 三钉。**第 7 项全件收口** |

## 不做（已拍板排除）

- W11 热重载（启动装载+运行期挂/卸够用）
- 自研鉴权（交 SSH/系统）
- G16 人类操作者身份（GUI 反馈轮用户裁决撤）

## 等数据

- **13-context-system ⏸ 转正**：真实多 session 负载跑出数据（沟通/工作量比、切后回捞率、summary 够用度、阶段大小分布）后复审——观测面已埋（v2-S1）。
- **二进制打包 A/B/C**（GUI 页面嵌入与否）：待用户拍板（建议 C）。

## 相关

`07`（唯一裁决来源，★ 已清零）· `11`（施工计划与验收）· `12`（路线图）· `13`（上下文候选设计 ⏸）
