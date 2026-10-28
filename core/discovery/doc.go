// Package discovery 是 K10 的**发现表**：core 的内存活状态组件，回答"谁在线 / 在干什么 /
// 属于哪些 gate"（K10 原文叫 Registry，但 core/registry 已是扩展注册表（E3/E5/E6/D17），
// 撞名 → 落本包 core/discovery）。
//
// # 定位（K10 + G17b）
//
//	core 内存活状态，**不是扩展点**（没有接口、没有注册表、不可替换实现），**不做 I/O**
//	（纯状态机：宿主喂事件进来，查询出去）。属于"容器管理 + subagent 管理"的一部分，
//	不是协议层——协议面在 v1-S3 的 core/agentlink（K18）。
//
// 与 G17b 的两分法严格对齐：
//
//	持久投影（落库，G17）：session 目录 / ledger / 黑板 / 成本 —— 归 runtime/indexer；
//	**活状态（只在 core 内存，进程结束自行消失）**：presence / 在线状态 / **gate 成员** —— 归本包。
//
// 于是**不需要租约、不需要心跳、不需要"离线 session 保留"**：session 生命周期 = 进程
// 生命周期。本表因此区分两种"知道"：
//
//	事件折叠态（可重建）：session 见过、agent 树投影、gate join 历史 —— 崩溃重启后从
//	                          事件流重放即得（F5：唯一事实源）；
//	进程本地活状态（不可重建）：哪个 session 此刻由**本进程**持有 —— 由宿主调
//	                          Activate/Deactivate 维护，重启后老成员全部失活（这是特性：
//	                          它的宿主进程已经不在了）。
//
// # 数据流入：宿主拉式扫描，不是 bus 推式
//
// 与 host/doc.go 的在线表同口径：宿主持一个 seq 水位增量 ReadFrom，把事件喂进 Apply。
// 不用 bus.Subscribe 的理由——**重复计数比漏计数难发现**：同一事件会同时走"扫描"和"订阅"
// 两条路（在线表已经在扫描），叠加上来就是双倍；而漏计数在水位上立刻可见。扫描还有持久
// 水位、可重放、幂等三个订阅没有的性质。宿主侧实现在 host/discovery.go（core 不做 IO）。
//
// # 可见性（K11）只做水平那一维
//
//	水平（gate）：只有 session 的**主 agent**之间互相可见，条件 gates(A) ∩ gates(B) ≠ ∅
//	             → VisiblePeers；别 session 的 subagent 完全不可见（不出现在任何对端查询里）；
//	垂直（agent 树）：父 ↔ 直系子 —— 已由 core/agents 管（K11/K27），本包**不重做**，
//	                 只把树投影折出来供宿主/在线表观察。
//
// 跨层上查 = Inquiry 沿父链逐级显式调用（K18，v1-S3），不是"全祖先自动可见"。
//
// # 幂等
//
// Apply 可重复、可乱序（同一事件 Apply 两次不改变状态）：所有写入都是集合语义或
// "seq 更大才覆盖"的 LWW，**没有任何计数器**——所以不需要记"哪些事件已 Apply"的集合
// （那会随事件数无限增长）。唯一的副作用类是 last_seq/last_kind/updated_at，用 seq 守卫。
package discovery
