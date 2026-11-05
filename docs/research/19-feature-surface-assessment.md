# 19 · 功能面与 GUI 暴露面评估（2026-11-05）

> **用途**：暂停施工做一次全面体检——①功能面还缺什么（对照档案与实现）②暴露给 GUI 的协议面是否充足（按真实 agent GUI 的使用场景逐项打分）。
> **方法**：全量勘察 `host/http.go` 注册面（48 端点）+ WS 帧（9 kind）+ `contracts` 事件面 + 16 §14 挂账账本 + 07 回填段挂账关键词 + 代码内诚实边界注释；基线 commit `efcc425`。
> **结论先行**：核心骨架完整且自洽（事件溯源/多 agent/per-prompt/可靠性/安全五面闭环）；缺口集中在**产品化外围**——会话管理域整体缺位、协议面三个硬缺口（取消/配置独立写/delta 断线）、审计读面三处缺位；GUI 面"对话主干充足、产品化不足"。

---

## 0. 总结论（一句话版）

**能跑 agent，但还不像产品**：一个真实 GUI 要的"停止按钮、会话改名、配置单独保存、审批回看"今天都做不了；而这些不是实现漏了，是**设计面上从未存在**（48 端点与 16 清单逐条一致、零漂移——文档与代码在缺位上是诚实一致的）。

---

## 1. 功能面三层评估

### 1.1 核心骨架：完整

对照 16 号清单的 12 硬约束 + 五张全表逐条核过：事件溯源（F1/F5 + model-visible ⟺ logged）、X1 三段、契约 F7、多 agent（树/三闸门/K11/agentlink 五件套）、per-prompt 配置生效链、上下文 L1/L2/recall、可靠性（resume/WAL/blob/GC）、安全（loopback/CORS 白名单/凭据纪律/审批 fail-close）、L2 会话工作区——全部闭环且有钉子。**骨架层没有欠账**。

### 1.2 显性挂账账本（16 §14 共 35 条 + 07 尾注 46 处"挂账"）分三类

| 类 | 含义 | 代表条目 |
|---|---|---|
| **A. 拍板不做 / 有意不做** | 决策过、不欠账 | G-g 运行时权限面、兄弟互聊（K22）、通信配额（K23）、分布式（K19）、scheduler 单建（E2）、审批鉴权（loopback 同权论）、WAL 掉电尾部丢 delta（取舍） |
| **B. v1 边界 / 后置** | 声明过、等升级 | remote_host 纯声明、工作区软边界（词法判定）、stage_scoped/sliding_window 策略枚举、otlp 留位、sandbox 非 Windows 后端预留、per-session 执行面切换（sandbox/remote） |
| **C. 真缺口（欠账）** | 知道错/知道缺、没修 | ①FoldConversation 孤儿 tool_call（多 agent resume/fork 必踩 OpenAI 400）②客户端重试落等价事件（缺幂等键）③跨进程 Claim 不可探测（durable claim 记 F7）④fork/CLI resume 不继承（tools/context/kind）⑤telemetry model 不随覆盖⑥跨容器 waiting 不成立⑦TUI 自动化测试⑧二进制打包 A/B/C **待用户拍板**（唯一还开着的决策） |

### 1.3 本轮新发现的结构性缺口（勘察信号，此前档案未点名）

1. **`board.written` 是孤儿事件**：有契约（event.go）、有投影（indexer.ReadBoard 已实现）、**无生产者、无读面**——board_state 表恒空。K22 的"共享黑板"在代码层是空的。要么补生产者+读端点，要么把契约占位摘掉。
2. **审批是"只存在于当下"的域**：审批不是事件 kind（INVENTORY 明文）→ 已应答/已超时的审批任何地方查不到；GUI 做审批审计只能靠 policy_snapshot 的 code 反推。
3. **`/btw` 协议面完全不可达**：Hub.RunBtw 只有 CLI/TUI 两个调用方；结果同步返回、fork session 不进会话表、agent 树用完即弃——GUI 触发不了旁支、也回看不了。
4. **`model.delta` 断线即丢**：durable=false 不入库，`/api/events` 补不回——GUI 刷新后流式文本消失（整轮 text 还在 model.turn，降级可用）。
5. **L1 没做**（L2 的遗留）：workspace 生效值只在单会话 config 端点，**会话列表没有 workspace 列/过滤**。
6. **配置写与发消息耦合**：唯一 config 写路径 = POST /prompt 且强制非空 prompt——"只改配置"必须烧一轮模型。

---

## 2. GUI 暴露面充足性（逐场景打分）

### 2.1 齐（GUI 可直接用）

对话主干（prompt/config/profile 同发、懒接管续聊）、WS 流式 + 事件回放、配置读（生效值/声明值/overridden_fields 三分）、profile 列表、审批当下态（pending + respond）、多 agent 树（结构够画树）、协调四面（gates/peers/presence/discover）、观测主体（events 分页/context/cost/monitor/health/git/graph）、管理主体（extensions 启停/vectors/docs/worktree CRUD）。

### 2.2 不足清单（按对 GUI 的杀伤力排序）

**P0 —— 挡住核心体验**
| # | 缺 | 现状证据 |
|---|---|---|
| 1 | **取消在跑轮** | 无 HTTP 端点、无 WS cancel 帧、无事件 kind；唯一取消在 ACP stdio。GUI 的"停止"按钮做不了 |
| 2 | **配置独立写端点** | `POST /prompt` 强制非空 prompt → 配置面板"保存"必须烧一轮模型（改配置附带一次模型调用） |

**P1 —— 产品化必需**
| # | 缺 | 现状证据 |
|---|---|---|
| 3 | **会话管理域**：改名/标题、删除/归档、单会话详情聚合、**workspace 列（L1）**、创建时间列 | 全部无端点无字段（改名/删除全仓零命中）；详情要拼 5 个端点；创建时刻连字段都没有 |
| 4 | **/btw 协议面入口 + 结果可回看** | 见 §1.3-3 |
| 5 | **审批历史**（落事件（F7）或历史端点） | 见 §1.3-2 |
| 6 | **delta 断线补救**（重连补发摘要/断点标记） | 见 §1.3-4 |

**P2 —— 增强**
| # | 缺 |
|---|---|
| 7 | review 记录列表读面（现在只有单分支 check）；board 读写（或摘契约占位）；outbox（发出的消息）；agent 类型表 HTTP 面 |
| 8 | 附件上传面（prompt 无 attachment 字段；blobs 只读工具结果外置）；事件导出；/api/events 的 agent_id 过滤；WAL 报告端点；worktree 受管根查询 |
| 9 | 会话标签/项目分组（discovery 明文"不放宽到 project_id"——要做得先设计） |

### 2.3 协议面读写对称性（信号）

只写不读四件（prompt 轮次、review、broadcast/handoff 发件、审批）、只读不写三件（profiles、agents 树、docs/vectors 路径不配对）——GUI 的"历史/回看"需求普遍落在只写面之后，这是产品化缺口的共同形状。

---

## 3. 建议的刀序（下一阶段提案，供拍板）

1. **协议面补缺刀**（P0 两件 + delta 补救评估）：`POST /api/sessions/{id}/cancel`（复用 ACP 的 in-flight cancel 机制）+ 独立 `POST /api/sessions/{id}/config`（复用 recordConfigChanged 写路径，prompt 可空）。小刀、解锁 GUI 核心交互。
2. **会话管理域刀**（P1-3）：改名/标题（落事件还是投影字段要先拍）+ 删除/归档（软删 or 硬删？事件流是事实源——删除语义要设计）+ workspace 列（L1，纯只增）+ created_at 列 + 单会话详情聚合端点。
3. **审计读面刀**（P1-5 + P2-7）：审批历史（F7 落事件最顺）+ review 列表 + board 决策（补产或摘契约）。
4. **挂账修复刀**（1.2-C 类按序）：孤儿 tool_call（一直挂着）→ 重试幂等键 → fork/CLI 继承口径。
5. **待你拍板一项**：二进制打包 A/B/C（GUI 页面嵌入与否，14 号里建议 C）。

---

## 4. 证据基线

- 端点全表：16 §3.1（48 条）与 `host/http.go` 注册面逐条一致（本次复核零漂移）；
- 挂账账本：16 §14（35 条）+ 07 尾注（"挂账"46 处，注意与"已清"混排——不能按关键词计数当未完成清单）；
- 场景打分与信号的原始取证：本次勘察纪要（会话管理 12 子项 / 对话 7 子项 / 多 agent 4 子项 / 配置 9 子项 / 协调 5 子项 / 观测 8 子项 / 管理 8 子项，逐项带 file:line）。
