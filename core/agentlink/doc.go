// Package agentlink 是 K18 的**交流能力五件套**：Presence / Discover / Inquiry /
// Broadcast / Handoff。属于 E8 通用组件区——框架自带、可被任何工作流/宿主复用，
// **library 形态，不绑定任何协议**（没有 transport、没有 wire format、没有推送队列；
// 协议面在 host/cmd，本包只提供能力）。
//
// # 五件套分别回答什么
//
//	Presence  谁在线 / 在干什么 / 属于哪些 gate      → 发现表（core/discovery）的面向视图
//	Discover  按 gate / 按 agent 树查找 session/agent → 同上，加树投影
//	Inquiry   深入查询某 session：**结构化摘要 + 事件窗口**（Q6 两者都给）
//	Broadcast 向 gate / 子树发消息                    → 写成 durable 事件，可见者拉得到
//	Handoff   结构化交接（带 payload）                → 同上，多一个开放载荷
//
// # 定位：core 子包，不做 IO（C2/C3 分层）
//
//	读：EventWindowReader（接口，core 定义）——宿主注入实现，典型就是 core.Store 本身
//	    （ReadFrom 签名一致，无需适配层）；
//	写：EventSink（接口，core 定义）——宿主注入实现（host 用"经 bus 发布 + 回读"的适配器，
//	    见 host/agentlink.go）；
//	状态：*discovery.Table（core 组件，宿主喂事件）——Presence/Discover/可见性全走它，
//	    **不复制 K11 逻辑**。
//
// 于是本包可在无 IO 的环境里完整单测（内存 fake 即可跑五件套 + 端到端核验）。
//
// # 消息 = 事件（F7 只增 / G23 事件化）
//
// Broadcast/Handoff 落两个 durable kind（D6 点分命名空间）：
//
//	agentlink.broadcast  投递目标（gate 或子树）+ 正文
//	agentlink.handoff    同上 + 结构化 payload（开放对象，工作流自定义，框架不解释）
//
// 事件化换来三件事：**可审计**（谁在什么时候对谁说了什么，一条不漏）、**可重建**
// （FoldMessages 把事件流折回全部消息历史）、**可回放**（与对话/工具调用同一条全局流，
// G3：跨 session 统一时间轴）。契约只增不改义（F7）：后续要加"已读回执"之类就是新 kind。
//
// # 投递语义 = 拉模型，不是推送队列
//
// "投递"只意味着两件事：① 消息写进事件流；② **可见者查询得到**（Inbox）。没有队列、
// 没有 per-recipient 投递记录、没有在线推送——与在线表/发现表同哲学（扫描/查询，不订阅），
// 理由是重复计数比漏计数难发现，且拉模型天然可重放、可补拉（SinceSeq 增量）。
//
// 可见性（K11）在**写时算一次**（Broadcast/Handoff 的 Recipients，供审计与 UI），
// **读时再算一次**（Inbox 按当前 membership 过滤）——两处都只用 discovery 的现有查询。
// 两者不一致是特性不是缺陷：写时告诉你"此刻谁能收到"，读时按"此刻的成员关系"给
// （一个后来 join 同一 gate 的 peer 拉得到更早的广播——拉模型下这正当且有用）。
//
// # 可见性口径（K11，本包只消费不重制）
//
//	水平（gate）：只有 session 的**主 agent**之间互相可见，条件 gates(A) ∩ gates(B) ≠ ∅
//	             → gate 目标的收件方 = 该 gate 活跃成员的主 agent；
//	垂直（agent 树）：父 ↔ 直系子可见 → subtree 目标的收件方 =
//	             **目标子树 ∩ 发送方的可见集**（见下）；兄弟 subagent 互相不可见；
//	             **别 session 的 subagent 完全不可见**（跨 session 的 subtree 目标在
//	             写入时就拒：ErrCrossSessionSubtree）。
//
// subtree 收件方的**精确口径**（写时 Recipients 与读时 Inbox 用同一份实现：
// subtreeRecipients——两处不一致就是审计与实际对不上）：
//
//	收件方 = **目标子树 ∩ 发送方的 K11 可见集**，
//	可见集 = 自己的父 + 自己的直系子（K11② 的一跳；K27：不是「全祖先/全后代自动可见」）。
//
// 于是：**兄弟永远不在收件方里**；目标指向祖先时不再把整棵子树都算上（旧口径取的是
// 子树的传递闭包 → 兄弟与隔代都收到了消息，违 K11②/K27）；父向下广播只到直系子，
// 要再往下走由那一层自己发（逐级显式，K27）。
//
// # 查询面必须带调用方身份（F4）
//
//	Presence / Discover **必须**给调用方身份（Identity：session_id + agent_id）：
//	没有它就没有可见性过滤，任意客户端就能枚举任意 gate 的成员、任意 session 的
//	agent 树与 gate 列表。查询结果按 K11 裁剪：
//
//	Presence   同 session → 自己 + 水平可见对端；
//	           跨 session → 只有可见对端（gate 相交的对端主 agent）能查，且只回
//	                     对端视角（gate 只给相交的，不泄漏对端 agent 树规模）；
//	Discover   gate 查询 → 调用方必须与该 gate 相交，否则 ErrNotVisible；
//	           agent 查询 → 自己树内给完整投影；跨 session 只允许可见对端的
//	                     **主 agent**一行（不给它的子树——别 session 的 subagent
//	                     完全不可见）。
//
// # Inquiry 的窗口投影（F8）
//
//	窗口是**要进模型上下文**的东西，可见性必须在这里落地：
//
//	同 session（默认）→ 只回调用方可见集（自己 + 父 + 直系子）的事件；
//	                   兄弟与隔代 agent 的事件**不在窗口里**（K11②：同 session 的
//	                   兄弟本就该不可见；隔代要沿父链逐级显式调用，K27）；
//	显式投影          → 只回指定 agent 的事件（同 session 时它必须在调用方的可见集里）；
//	跨 session        → 可见对端看整个被查 session（对端视角）。
//
// # Inbox 的游标（F5）
//
//	Limit 作用于**投递给查询方的条数**（不是"扫到的消息条数"）：流里全是别人的消息
//	时也要继续扫，直到凑够 limit 或扫到流尾——否则噪声占掉额度，真消息永远拉不到。
//	返回带**扫描位置**（next_seq，保守下界、只增不减）与 has_more：游标不必再靠"收到的
//	最后一条消息"反推（那条消息可能根本没投给自己，反推会卡死）；追平态也回填它——
//	归零会让调用方退回 0、从头重扫、重复投递。扫描是**流式**的：逐批过滤，不留全量。
//
// # nil 语义（core/doc.go 三处口径）
//
// New 永不失败也不 panic：依赖允许部分缺失（典型如只用 Presence 的调用方不需要 reader/sink），
// 缺失的依赖让**用它的那个方法** fail-close 返回错误（ErrNoReader / ErrNoSink），
// 而不是让整个组件不可用。*Link 自身为 nil 时全部方法同样 fail-close（返回零值 + 错误），
// 与 discovery.Table 的 nil 容忍同口径。
package agentlink
