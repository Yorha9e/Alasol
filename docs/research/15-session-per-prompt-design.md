# 15 · session 与 per-prompt 面设计（2026-11-03 定稿）

> 地位：**设计定稿**（讨论已全部收敛，用户拍板）。施工排期见 `11` 尾部与 `14` 下一阶段段；决策指针注记见 `07` 相关行。
> 背景：缺口 1–8 收口后，用户提出两类补强——①旧 session 继承（hub 重启后会话失管）② per-prompt 配置（"很多内容实际都需要 per-prompt"）。本文是两者的设计定稿。

## 1. 在线表三态（跨容器一致的完整设计）

### 1.1 为什么不是"一个前端字段"

在线表牵扯 **agent 容器间信息交换**的三条通道，必须同一判定、一次改齐：

- **K14/K15**：在线表注入 prompt 末尾（模型看到的对端表）；
- **K11**：peers 水平可见性（gate 相交 ∩ 活跃成员）；
- **agentlink**：presence / discover（容器间查询）。

三处判定不一致 = 模型看到的对端、peers 可见面、presence 回答互相矛盾。

### 1.2 三态定义

| 态 | 判定 | 出现处 |
|---|---|---|
| **活跃 active** | `running`（本轮在跑）∨ 最后事件 ts ≤ 活跃窗口 | 在线表注入 / peers / presence / GUI 在线表 |
| **托管 hosted** | 本进程持有活 loop（新建或默认接管） | 可立即对话；不在对端在线表 |
| **历史** | 未托管（库里存在） | session 历史表（可读；点"继续"触发默认接管） |

- **判定源 = 事件流的最后事件 ts**（全库可见 → 各容器判定一致；无租约无心跳（G17b）不破；重启判定不丢）+ 本进程 `running` 短路。
- **活跃窗口 N = 15 分钟**（缺省），挂 `onlineTableConfig`（`/api/online-table/config` 运行期可调，在线表注入与 peers 共用同一个 N）。超窗 = **默认离线**（回托管/历史态）。
- fork 旁支整族排除（不是一等会话）。
- **动机**（用户确认）：默认接管放开后，若所有历史 session 都算在线，会把 hub 拖垮——统一泄压四件：WS 订阅数 / 在线表注入行数（prompt 成本）/ peers 可见面 / 跨 hub 交换表。

### 1.3 消费方对齐清单（同刀改齐，不留半截）

1. 在线表注入的行集判定（K14/K15）；
2. peers 的 `∩ 活跃`（K11）；
3. presence / discover 过滤（agentlink）；
4. `GET /api/sessions` 每行加 `online` / `running` / `last_activity` + `?online=` 过滤（历史表 = 不带过滤的全库）。

## 2. 默认接管（懒接管）

```
POST /prompt → hostsSession? ──否──→ loopFor 懒接管（Claim + 建 loop + 事件流恢复主 agent id）
                  │                     ├─ Claim 成功（原宿主失管）→ 跑 + hub.alert 留痕
                  │                     └─ Claim 冲突（另一活宿主持有）→ 拒（唯一拒绝线）
                  是 → 照常
```

- **唯一拒绝线** = G25 双写防线（两进程同时写一个 session 的事件 = 亲和写破）。
- 历史进模型不用新做：`RunPrompt` 每轮折叠事件流带历史（多轮对话同源）；主 agent id 经既有机制从事件流恢复（v3-S6-B F1）。
- 留痕用 `hub.alert`（既有事件，零契约）；接不进就注释挂账，不加新事件 kind。

## 3. per-prompt = 会话配置修改（不是单轮覆盖）

### 3.1 语义（用户定义）

`POST /prompt` body 带 `config` = **修改该 session 的配置**：

- **本轮生效**（这个 prompt 的 Run 就按新配置跑）；
- **持久化**影响**后续所有轮**；
- **重启后仍生效**；
- **loop 运行中修改** → 不打断当前轮，**当前轮结束后下一轮**生效。

### 3.2 持久化载体 = `config.changed` 事件（F7 契约增量，只增全链）

配置变更落事件流，**当前配置 = 事件 fold**（F5 纪律：当前态 = fold；ledger/board/发现表同款口径）。重启后回放即得，审计免费（谁何时改了什么都在事件流）。

### 3.3 生效链

```
hub 启动默认（Config）
  → session 配置（config.changed 折叠；新建 session 继承 hub 默认）
    → 本轮快照（Run 开始时冻结；运行中变更下一轮生效）
```

### 3.4 PromptSpec 形状（讨论稿定形）

```json
{
  "prompt": "...",
  "config": {
    "model": "gpt-4o-mini",
    "reasoning_effort": "high",
    "provider": {"base_url": "http://...", "api_key_env": "MY_KEY_ENV"},
    "tools": {"only": ["read", "standby"], "except": ["bash"]},
    "context": {},
    "policy": {}
  }
}
```

第一批字段（批次 2）：

- `model`：per-run 覆盖进 ModelRequest（`model.turn` 已带 model 字段，零契约压力）；
- `reasoning_effort`：`ValidateReasoningEffort` 白名单（low/medium/high/xhigh，G14）；
- `provider`：**凭据纪律（gap2 同款）**——换 `base_url` 必须显式给 `api_key_env`（不静默把 hub key 发给第三方端点）；只传**变量名**（H15），值不进请求体/事件。

### 3.5 config.tools = 工具白名单修改（白名单语义）

- 语义 = 修改 session 的工具白名单（对**已注册**工具的可见性增删，持久化）；
- **白名单制**（R5/R3：不暴露即不存在）——点名白名单外的工具 = 该工具不存在，天然无"越权"概念（早期草案的"收窄/放宽"框架作废，用户纠正）；
- **可见 ≠ 可执行**：执行许可归 policy/perm 层（见 §5）；
- 与 agent 类型级白名单（spawn 时的类型过滤，W5）各自独立、互不突破。

## 4. standby 待机工具（多 agent 协作防掉线）

```
standby({wait: "message"|"handoff"|"event", timeout_min: ≤30, on_timeout: "return"|"sleep"})
```

- **场景**：A 等 B 的 handoff/回复，等待期没事件 → 被 15 分钟窗口判离线 → 协作断链。
- **语义**：agent 调用 → 本轮**挂起**（X1：`tool.call.intent` 已落、`completed` 延迟到唤醒/超时——**三段形状不变**）；挂起期间 session 进 **waiting 态**：
  - 在线表标注 `waiting`（算活跃、**豁免活跃窗口**，但**不刷最后事件 ts**——不造假活动）；
  - 唤醒 = agentlink 消息/handoff 到达 或 超时（**硬上限 30 分钟一轮**，到期 completed 返回"未等到"）；
  - **零 WS 连接、零模型轮**（挂起期零 provider 调用），hub 侧只是阻塞的工具执行 goroutine。
- **膨胀控制**：待机是有界占位（超时上限 + 唤醒即出态）——在线表 waiting 行数 ≈ 正在协作的对数，不是 session 总数。
- **三个交互点（实现硬约束）**：
  1. X1：intent→completed 跨长时间，但形状与落盘序不动；
  2. **resume/X4**：崩溃时 standby 中的工具是 in-flight——处置要能**重新挂起或如实收口**（不许当垃圾清掉协作状态）；
  3. WAL/reconcile：standby 不产生"在飞轮次"误报（它是工具不是轮次；核对账口径）。
- **等待条件批次**：第一批 = `message` + `timeout`（随 G-f 落）；`event`（等任意事件流条件）**第二批 = G-j**（表达力强但解析面大，单列缺口跟踪）。

## 5. 权限面（真缺口；安全语义开工前拍板）

- 现状只有两层**静态**：hub 启动级（`-allow` / 策略规则 / 审批模式四档）+ agent 树 H22 perm（子级权限不超出父级）。**无任何运行时/per-prompt 修改面**。
- 按 §3 语义应为 `config.policy` 维度（改了持久生效）。
- **开工 G-g 前必须单独拍板的安全语义**：per-prompt/运行时**放宽**权限要不要受限（谁能改、能不能降审批档位、放宽是否要审批）。

## 6. 施工缺口与批次

| # | 缺口 | 批次 |
|---|---|---|
| G-a | 在线表体系（三态 + 15min 窗口 + 四消费方对齐） | 1（与 G-b 同刀） |
| G-b | 默认接管（懒接管 + Claim 冲突线 + hub.alert 留痕） | 1 |
| G-c | per-prompt 配置体系（PromptSpec.config + `config.changed`（F7 全链）+ 生效链 + 每轮装配读 fold 配置） | 2 |
| G-d | 模型面字段（model / reasoning_effort / provider） | 2 |
| G-f | standby 工具（waiting 态 + message/timeout + 三交互点） | 2 |
| G-e | config.tools 工具白名单修改 | 3 |
| G-g | 权限面（config.policy）——开工前拍安全语义 | 3 |
| G-j | standby 的 `event` 等待条件（等任意事件流条件的表达式与匹配） | 3（G-f 本体之后） |
| G-h | 上下文面（context_strategy/注入区配置化） | 4 |
| G-i | profile 机制（命名装配模板）+ 执行面（cwd/worktree/remote） | 4/后置 |

## 7. 相关

`07`（决策注记）/ `11`（施工排期）/ `12`（路线图）/ `14`（v4 排期与下一阶段段）/ handoff `ho_779ccad48c1f`（跨 session 索引版）
