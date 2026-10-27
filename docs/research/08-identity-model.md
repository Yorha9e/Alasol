# 身份与 ID 设计

> 触发：MCP 工具经常因 id 问题投递失败。
> 核心主张：**一个 id 一个命名空间，一个字段一个来源。** 框架的 id 和 provider 的 id 是两类东西，混用即故障。

---

## 1. 命名空间表

| 命名空间 | 生成者 | 唯一性范围 | 格式 | 稳定性 | 用途 |
|---|---|---|---|---|---|
| `session_id` | 框架 | 全局 | ULID（26 字符 Crockford base32） | 终身 | 会话引用、目录名、UI 键 |
| `agent_id` | 框架 | 全局 | ULID | 终身 | subagent 引用 |
| `event_id` | 框架 | 全局 | ULID | 终身 | **幂等去重键** |
| `turn_id` | 框架 | session 内 | int64 序号 | 终身 | 轮次引用（可比较、可排序） |
| `step_id` | 框架 | turn 内 | int64 序号 | 终身 | 步骤引用 |
| `tool_call_id` | **provider** | provider 内 | **不透明字符串** | 一次调用 | **必须原样回传 provider** |
| `blob_id` | 框架 | 全局 | ULID | 终身 | 大对象引用 |
| `artifact_id` | 框架 | 全局 | ULID | 终身 | 产物引用 |
| `project_id` | 框架 | 全局 | `hash(canonical_cwd)` 短 base32 | 路径变更则变 | 目录分组 |
| `repo_id` | 框架 | 全局 | `hash(canonical_git_root)` | 同上 | 跨 worktree 聚合 |
| `node_nid` | 框架 | 图内 | int64 | 永久 | 图内部节点 id（对外用名字） |
| `mcp_session_id` | MCP | MCP 会话内 | 协议定义 | — | MCP 协议专用 |
| `causation_id` | 框架 | 全局 | ULID | 终身 | 事件的因果指针（指向触发它的那个事件） |
| `parent_agent_id` | 框架 | 全局 | ULID 或 null | 终身 | agent 树的父边 |
| `gate_id` | 框架 | 全局 | ULID | 终身 | 发现条件（可见性作用域） |
| `workflow_run_id` | 框架 | 全局 | ULID | 一次工作流运行 | 自动 gate 的组成部分 |
| `summary_ref` | 框架 | 全局 | ULID | 终身 | 摘要引用（指向一段被压缩内容的原文） |
| `sum:` 前缀 | 框架 | 全局 | 引用串 | — | `ContextSnapshot` 中 `summary_ref` 的序列化前缀形式 |
| `mcp_request_id` | MCP | 请求内 | 协议定义 | — | MCP 协议专用 |

**关键分层**：
- **框架 id**：我们自己生成，格式受控，可比较，可排序，可做文件名
- **provider id**：模型生成的，**完全不透明**，原样存、原样回传
- **序号**：session / turn 内的递增整数，用于"第几轮""第几步"——**不用 ULID**（序号天然可比较、可排序，省一个索引）
- **哈希**：只用于路径分组（`project_id` / `repo_id`），**不用作身份**

---

## 2. 框架 id：为什么是 ULID

| 要求 | ULID 满足 |
|---|---|
| 全局唯一 | 48 bit 时间戳 + 80 bit 随机 |
| 同秒不撞 | 80 bit 随机部分兜底（对比哈希方案同秒必撞） |
| 字典序 = 时间序 | 前 48 bit 是时间戳 → 列表不用额外索引 |
| 自带创建时间 | 不需要额外字段 |
| 文件名安全 | Crockford base32：`0-9 A-H J K M N P-T V-Z`（排除 I L O U） |
| URL 安全 | 同上，无 `+ / =` |
| 大小写不敏感友好 | Crockford 本身大小写不敏感；Windows 文件系统友好 |
| 不混淆 | 刻意排除 `I/L/O/U` → 避免 `1/I`、`0/O` 人工误读 |

**不选 UUIDv4**：完全随机 → 不可排序、不含时间、需要额外索引列
**不选 UUIDv7**：可用（v7 也是时间前缀），但带连字符且大小写敏感，不如 ULID 适合做文件名

---

## 3. 字符集与编码纪律

| 规则 | 原因 |
|---|---|
| **JSON 里所有 id 一律是 string** | int64 / ULID 转 JSON number 会丢精度（JS 的 number 是 float64，> 2^53 失真） |
| **绝不用标准 base64** | 含 `+` `/` `=` → URL 与文件名都要转义 |
| **绝不让 id 只靠大小写区分** | Windows 文件系统、Postgres 某些 collation 会折叠大小写 |
| **框架 id 与 provider id 不进同一个字段** | 见 §4 |
| **provider id 不做大小写折叠、不做 Unicode 规范化** | 折叠后回传给 provider 就对不上了 |
| **id 不做"尽力修复"** | 字符集不合规则**拒绝**，不截断、不转义、不补零 |
| **文件名只用框架 id** | provider id 可能含 `/`、`:`、空格 |
| **短前缀仅用于展示** | 日志/UI 用 `session_id[:8]`；**比较一律用完整值** |

---

## 4. provider id：黑盒，永不改写

这是"工具投递失败"最常见的根因。

```
模型返回 tool_call_id = "toolu_01ABC..."
        ↓
我们存进事件 payload，必须 byte-for-byte 原样
        ↓
下一轮把 tool_result 按这个 id 回传给 provider
        ↓
provider 用 id 匹配 → 匹配不上 → 报错或静默丢弃
```

**死法清单**：

| # | 死法 | 症状 |
|---|---|---|
| 1 | 把 provider 的 `tool_call_id` 当成自己的 `event_id` 存 | 回传时传错 id，provider 永不匹配 |
| 2 | 重新生成 / 改写 provider id（加前缀、转 base64、统一大小写） | 同上 |
| 3 | provider 返回数字 id，存成 JSON number | 类型不匹配；大数字丢精度 |
| 4 | 同一 id 在两个字段用了不同编码 | 一处 hex 一处 base64，匹配不上 |
| 5 | 系统折叠大小写（Windows FS / `citext`） | 匹配不上 |
| 6 | id 含 `/` `:` 被当路径分隔符解析 | 解析出错误的层级 |
| 7 | **事件去重时看了 payload 里的 id** | 两个不同事件被判成同一个 → 事件丢失 → 状态错乱 |
| 8 | id 存进 `varchar(16)` 被截断 | 匹配不上 |
| 9 | 并发下 id 生成器非原子 / 计数器非线程安全 | id 重复 → 事件互相覆盖 |
| 10 | MCP `requestId`（协议内数字串）被拿来当业务幂等键 | 与 provider id 撞车 |

**防御规则**：
1. `Event.id`（框架 ULID）与 `Event.payload.tool_call_id`（provider 不透明串）**永远不同字段**
2. provider id 在框架内**只做两件事**：原样存储、原样回传。不解析、不比较大小写、不做规范化
3. **事件去重只看 `Event.id`**，绝不看 payload 里的任何 id
4. provider id 若需要索引（回传时按 id 找 call），用**精确匹配 + 原值**，且存成独立的映射表，键类型为 `TEXT`（collation 用 `BINARY`）

---

## 5. 变更规则

| 场景 | 行为 |
|---|---|
| resume / 恢复 | **同一个 id**，不变 |
| fork / clone | **新 id** + 同时写 `parent_id`（指向源）；不重写源 id |
| 迁移 / 改名 | 不改 id；改的是 `cwd` / `project_id` 等属性 |
| 归档 | 不改 id |

**id 只能通过显式的 fork/clone 产生新值**。任何"重新生成 id 以修复某种不一致"的代码都是 bug 来源。

---

## 6. 可执行的防线

1. **入 JSON 前统一校验**：字符集（框架 id 限 `[0-9A-HJKMNP-TV-Z]`）、长度、非空、来源标记。不合规则**拒绝并记事件**，不修复
2. **一个结构体一个 id 字段**，不共用；不做 `map[string]string` 存 id
3. **id 生成器**：ULID 实现必须线程安全（时间戳单调 + 随机源加锁 / atomic 计数器）
4. **测试必备用例**：id 往返（生成→序列化→反序列化→相等）、大小写、URL 编码、文件名合法性、并发唯一性
5. **契约测试**：`contracts` 的 schema 里给每个 id 字段标注 `format` / `pattern` / `x-id-kind`（框架 / provider / 序号 / 哈希），让跨语言实现也能遵守

---

## 7. 与 MCP 的关系

MCP 自己有几类独立的标识，不要与我们的混用：

| MCP 概念 | 语义 | 我们的对应 |
|---|---|---|
| MCP `sessionId` | MCP 协议内的会话 | 与我们的 `session_id` **不是一回事**，各自独立 |
| MCP `requestId` | 协议内请求编号（数字串） | 不可用作业务幂等键 |
| MCP `progressToken` | 进度通知令牌 | 与事件 id 无关 |
| MCP `callTool` 的 `content[]` | 工具结果结构 | 原样透传，不加工 |

**结论**：MCP 层出现的任何 id，都**不进入我们的 id 命名空间**；我们给 MCP 的会话/请求时，另用我们自己的 id，并在边界做映射。

> 2026-10-27 补：上表补入 `causation_id` / `parent_agent_id` / `gate_id` / `workflow_run_id` / `summary_ref`（审阅报告 P2-2 / S16）。
