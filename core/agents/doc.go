// Package agents 实现多 agent 的骨架逻辑（阶段 5，v0 切片②核心差异化）：agent 类型体系
// （K16 类型可达性）、session 内 agent 树（K26）、spawn 三闸门（K17）、父子可见性
// （K11/K27）、结构化 summary 与污点继承（K24/Q7）。
//
// # 定位与边界
//
// 本包只含**内存活状态与纯逻辑**，不做任何 I/O、不写事件、不起子 loop：
//   - 类型可达性（can_spawn 环检测、三闸门判定）在这里；
//   - 事件的落库（agent.spawned/completed/failed）与子 loop 的构造/运行在 core/loop；
//   - 跨边界形状（AgentType/AgentSummary/权限级别）复用 contracts，无转换边界（D21）。
//
// 因此 agents 只依赖 contracts 与 core（NewUlid），**不 import loop**——依赖单向：
// loop → agents。树是并发安全的（Spawn/Complete/Fail/Visible 可并发）。
//
// # 三道闸门（K17，全部运行时强制，不靠提示词）
//
//	① 深度：child.depth ≤ maxDepth（默认 3 = 两级委派，06 §7.1）；
//	② 类型：child.type ∈ parent.can_spawn（第 N 层在类型层面就不可达，K16）；
//	③ 权限：child.permission ⊆ parent.permission ∩ ceiling（H11 只收窄）。
//
// can_spawn 环在**类型注册时**校验（K16/K17，陷阱16 第一层）；workflow 加载时的不可信
// 配置校验是另一层（陷阱16 第二层），由装配层负责，不在本包。
//
// # X5 幂等键的作用域
//
// 幂等键是 **(parent_agent_id, spawn_request_id)**（不是全局的 request_id）：契约
// AgentSpawnedPayload.spawn_request_id 的描述写明"按 (parent_agent_id, spawn_request_id)
// 找回同一 child"。全局索引会让"第二个任务复用同一个 request_id"静默不执行，还拿到
// 第一个 child 的 summary。命中时还会校验**类型**：同父同 request_id 换类型 →
// ErrRequestConflict（不把别人的 child 交出去）。
//
// # 生效权限的运行期约束
//
// 节点的有效权限（请求 ∩ ceiling ∩ 父级授予）由 PermOf 暴露给调用方（core/loop 用它
// 在**运行期**约束该 agent 的工具调用：所需权限维度超出即 deny，全局 allow 规则不能
// 放宽它——声明的权限是上限，不是建议）。工具 → 权限维度的推导见 RequiredCapabilities。
//
// # 可见性（K11/K27）
//
// 每个 agent 可见：**自己 + 父 + 全部直系子**。兄弟 subagent 互相不可见；别 session 的
// subagent 完全不可见（树按 session 作用域隔离）。跨层上查 = Inquiry 沿父链逐级显式调用
// （Ancestors 提供父链支撑），不是「全祖先自动可见」。
//
// # summary 污点（Q7/陷阱13）
//
// subagent 的 summary 是回到主 agent 上下文的**唯一通道** → 若 subagent 读到被注入的内容，
// summary 就是攻击载荷出口。故 summary 强制带 taint（继承来源事件最高污点级），且
// **只能升不能降**（MaxTaint）；主 agent 对 tainted summary 按不可信数据处理。
package agents
