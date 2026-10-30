// Package host 是**唯一的组合根与传输入口**（09）：把 core 与 runtime 装配起来，
// 对外暴露 HTTP（在线表 / 上下文快照 / 静态 Web UI）+ WS+NDJSON 事件流。
//
// # 它做什么
//
//	┌─ Hub（本包）──────────────────────────────────────────────┐
//	│ store(sqlite) → bus → indexer(后台 Run)                    │
//	│ registry + policy + loop 工厂（每个 session 一个 Loop）       │
//	│ transport.Server（/ws，N4）+ HTTP API（/api/*）+ Web UI（/）  │
//	└────────────────────────────────────────────────────────────┘
//
// 依赖方向（09 的硬约束）：cmd → host → core → contracts，host → runtime（实现）。
// 业务语义在 core，I/O 实现在 runtime，**本包只装配**——所以换 provider / 换存储 /
// 加工具都不该改这里（除装配清单本身）。
//
// # 安全边界（N5b / O7，v0）
//
//   - 只监听 **loopback**（默认 127.0.0.1:8720，可配端口但不可配成非 loopback 地址——
//     New 直接拒绝，fail-close）。判定**按 IP**（127.0.0.0/8 与 ::1）外加两个名字特例
//     （localhost / ip6-localhost，大小写不敏感）；**其余任何非 IP 主机名一律拒绝**
//     ——"127.0.0.1.evil.example" 这类域名会解析到 loopback（实测能绑上监听），
//     用字符串前缀匹配放行它就等于绕过了整个 v0 安全模型；
//   - **不使用 cookie**，本服务不设任何会话凭证（远程化需要的是 N4b 那六条，
//     v0 明确不做）；
//   - WS 连接由 runtime/transport 校验 Origin（同源 / 非浏览器客户端才放行）。
//
// 于是"任意网页向 loopback 发 ws://"这条攻击面只剩"能读到本地事件流"——
// v0 的威胁模型里 loopback 上的其它进程本来就与 hub 同权。
//
// # 在线表的数据来源
//
// 在线表（GET /api/sessions）由本包的**内存投影**提供：启动时从事件库扫一遍
// （catch-up），之后每个请求增量扫一次（一次 ReadFrom，通常返回 0 条）。
// 为什么不用索引器的派生表：G17 规定派生表的读只走索引器的 Read* 方法，
// 而索引器**没有"列举全部 session"的读面**（ReadSession 要先用 session_id 查）。
// 投影口径与索引器的 sessions 表一致（first_seq / last_seq / event_count /
// last_kind），watermark 则直接取 indexer.Watermark()（"派生表已追至 seq N"）。
//
// 拉式（scan）而不是推式（bus 订阅）：扫描有持久水位、可重放、幂等；
// 订阅会和扫描**重复计数**（同一事件两条路都到），而重复计数比漏计数更难发现。
//
// # 发现表（K10）与 gate（K12）
//
// 发现表落在 **core/discovery**（纯状态机，不做 IO），由本包拉式喂事件（host/discovery.go，
// 与上面在线表同口径：扫描而非订阅，理由相同）。它回答"谁在线 / 在干什么 / 属于哪些 gate"，
// 并实现 K11 的水平可见性（只有 session 的**主 agent**之间按 gate 相交互见）：
//
//	GET  /api/peers?session_id=... → 该 session 的可见对端（主 agent）
//	POST /api/gates/join           → 显式 join_gate（幂等）
//
// gate 成员关系是**活**的（G17b：session 生命周期 = 进程生命周期，无租约无心跳），
// join 事实则作为 durable 事件 `gate.joined` 进事件流（F7 只增）→ 崩溃重启后老成员
// 全部失活，join 历史由事件流重放重建；resume 的 session 由 hub 重新激活
// （自动 gate 重算 + 历史重放）。自动 gate 在 session 的 loop 建成时加入：
// `repo:<repo_id>`（runtime/identity.RepoID，worktree 按 git root 聚合）与
// `workflow:<run_id>`（有 run_id 才加；工作流引擎在 v3，钩子先留）。
//
// # /btw 旁支 fork（K29，v3-S2）
//
// `/btw <问题>` 跑一条**旁支执行线**：从主 session 的对话历史（FoldConversation 折叠，
// 与 resume 同源）分叉出一个**全新 session_id + agent_id**，旁支带 system + 折叠历史、
// **不带** ledger/gate（不挂上下文策略、不 join gate），跑一轮后结果**只回给用户**、
// **不回灌**主上下文（主 session 的事件流零新增）。契约见 `btw.forked`
// （contracts.BtwForkedPayload），实现见 host/fork.go。
//
// **旁支不是一等会话**（v3-S2 修的口径）：host 侧维护 fork session 集合
// （discoveryFeed.forks；数据来自 btw.forked 事件，重启后重扫即恢复），在
// **presence / peers / 在线表注入 / gate 成员面**统一过滤——core/discovery 不改
// （X1 纪律），过滤落在 host 层，于是"旁支不可见"不再依赖"它恰好没 join gate、
// 恰好没被激活"这种副产物。原始发现表（Hub.Discovery()，宿主内部 / 审计面）里
// 仍能看到旁支（可审计、可回放）——这是**有意保留**的边界。
//
// **历史口径**：主轮次不带历史（每轮只带本轮 prompt，历史只存在于事件流里），
// /btw 带历史（带条数/长度上限 + 截断告知：模型侧一条 system 说明 + 用户侧一行提示）。
//
// # agentlink（K18 五件套）
//
// 交流能力落在 **core/agentlink**（E8 通用组件：library 形态、不绑协议、不做 IO），
// 本包只做装配（host/agentlink.go）：reader 直接传 store（`core.Store.ReadFrom` 与
// `agentlink.EventWindowReader` 同签名），sink 用"经 bus 发布 + 回读拿 seq"的适配器，
// 协议面（HTTP）也在这里：
//
//	GET  /api/presence?session_id=         → 在线画像 + 水平可见对端（Presence）
//	GET  /api/discover?session_id=&gate_id=|agent_id= → 按 gate / 按 agent 树查找（Discover）
//	GET  /api/inquiry?session_id=...       → 结构化摘要 + 事件窗口 + refs 核验（Inquiry/Q6/K24）
//	GET  /api/inbox?session_id=...         → 拉模型收件箱 + 游标水位（Broadcast/Handoff 的另一半）
//	POST /api/agentlink/broadcast          → 落一条 `agentlink.broadcast` durable 事件
//	POST /api/agentlink/handoff            → 落一条 `agentlink.handoff` durable 事件
//
// 投递是**拉模型**（与在线表/发现表同哲学）：消息写成 durable 事件（可审计、可重建），
// 可见性按 K11 在写时算一次（回报给调用方）并在读时再算一次（Inbox 按当前成员关系过滤）。
// 别 session 的 subagent 完全不可见：水平收件方恒为对端 session 的**主 agent**，
// 跨 session 的 subtree 目标在写入时就被拒。
//
// **查询面一律带调用方身份**（K11 过滤的前提）：`/api/presence` 的 session_id 与
// `/api/discover` 的 session_id 是**调用方**（必须受本 hub 托管，否则 404）——
// 没有调用方身份就没有可见性过滤，任意客户端就能枚举任意 gate 的成员与任意 session
// 的 agent 树。`/api/inquiry` 的 `agent_id` 只是"调用方身份"，窗口投影用
// `project_agent_id`（缺省 = 调用方的可见集：自己 + 父 + 直系子）。
//
// # serve 的定位（v3-S4，G2 口径统一）：**观察 + 协调 daemon**
//
// `alasol serve` 与 `alasol chat` 是**两个进程、两个 hub、共享同一个事件库**。由此，
// 写端点分两类，校验口径**有意不同**（不是遗漏）：
//
//   - **托管型写面**（/api/peers、/api/gates/join、/api/agentlink/*、**/api/sessions/{id}/prompt**）：
//     要求目标 session 由**本进程**托管（hostsSession 查进程内 h.loops，G25 session 亲和写）。
//     serve 进程 loops 为空 → 这些端点对 chat 进程建的 session 一律 404 unknown_session。这是**设计**：
//     水平可见性/广播依赖**进程内活状态**（发现表的存活成员是进程本地的，无租约无心跳），
//     跨进程没有"谁在线"可判；
//   - **协调型写面**（/api/merge/review|check|predict）：**直查库、不校验托管**——merge 是
//     协调动作（review 结论/合并检查是事件流里的持久事实 + git 观测），不需要目标 session 在线，
//     任何拿得到 session id 的一方都能记一条 review。**故意不补托管校验**（补了就成了"只能
//     给自己进程的 session 记 review"，与它作为协调动作的定位矛盾）。
//
// **对话发起端点也在托管型写面里**（v3-S4 G1 + G25）：POST /api/sessions/{id}/prompt 若对
// **别的进程**托管的 session 起 prompt，本进程会**就地新建一个 loop**（hub.go 的 loopFor）
// → 两个进程往同一个 session/agent 写事件 → **G25 破**（全局 seq = 提交序 ≠ 因果序）。
// 所以它要求 hostsSession，否则 404 unknown_session（与 peers/gates/agentlink 同口径）。
// 协议面**开新会话**有两条同源同口径的路：WS `command.submit`（**不带** session_id →
// 服务端新建一个 session，本进程即宿主）与 **POST /api/sessions**（REST 入口，v3-S5 ①：
// CreateSession + 本进程建 loop、返回 session_id、不自动跑 prompt——之后对该 id 的
// /prompt 直接可用）；带上 session_id 的 command.submit 同样要求本进程托管，否则回 kind=error。
//
// 于是同一 session：读面（/api/sessions、/api/context、/api/cost）与协调写面（merge.*）全通
// （投影/索引扫的是共享库），托管写面（peers/gates/join/agentlink/prompt）404。**跨进程托管注册不做
// （记 v4）**——那需要会话亲和/活状态同步一整套（G25 的部署层约束），不是补个端点的事。
//
// **预期拓扑是"一库一 hub"（G25/G12，契约级限制）**：多 hub 共享同一个事件库时，读面
// 按**库**投影——A 进程建的 session 在 B 进程的列表里看得见（这是**正确行为**），但
// 事件**没有 hub 归属**、托管写面对非本进程的 session 一律 404——"看得见却发不了
// prompt"对第三方不直观。呈现辅助（v3-S5 G12）：GET /api/sessions 行的 **hostable**
// 字段（bool：本进程托管 = 可发 prompt；只增不破坏）标出"可发/不可发"，GUI 直接灰掉
// 不可发行；404 unknown_session 的文案也点明出路（POST /api/sessions 开新会话继续）。
// **多 hub 请各自一个库**；跨进程托管/路由记 v4，本刀不做。
//
// **重启同理**（活状态是进程本地的）：hub 重启后，**重启前**那些 session 一律**失管**
// ——本进程没有它们的 loop（h.loops 是内存态，重启即空），发现表的"存活成员"也不会
// 复活（老 session 不会因重扫事件流而重新变成 live，这是设计）。于是重启后：
// 读面照常（事件库里的历史还在）、协调写面照常，而托管写面（含 prompt）对**旧 session**
// 一律 404 unknown_session。**协议面要继续对话就得重新开会话**（WS command.submit 不带
// session_id → 服务端新建一个 session，本进程即宿主；或 POST /api/sessions——同源同口径）；
// 想接上旧 session 的上下文，
// 走 `alasol resume -session <id> -continue "<提示>"`（CLI 的恢复+续跑：**另一个进程**里
// 自己建 loop 并接管）——协议面**不硬接**旧 session（那正是 G25 要防的双写）。
//
// # 审批应答的鉴权口径（v0 的诚实边界，**有意不做鉴权**）
//
// 挂起审批的两个补面（v3-S5，G11）：approval.request 是**一次性广播**——页面刷新 /
// WS 重连后看不到已挂起的审批。于是 **GET /api/approvals/pending** 枚举当前挂起的
// （完整 request 形状 + waiting_since/deadline；空表 → []），WS **新连接建立时**把仍
// 挂起的重发给那条连接（transport onConnect → protocolApprover.ResendPending）。
// 两处皆 at-least-once：投递窗口内被应答的仍可能送达，客户端按 request_id 去重/
// 幂等呈现；重复应答服务端回 404（不假成功）。
//
// 协议审批面（POST /api/approvals/{id}/respond、WS approval.respond）**没有鉴权**：
// 任何能连上 hub 的客户端都能应答审批（v0 只监听 loopback + 无 cookie + 无 token，
// 见上面的"安全边界"一节）。fail-close 只保证"**无人应答 = deny**"（不得挂起、不得默认
// 放行，D14/H8），**不防乱批**：能连上的人可以抢先 approve 一条本不该放行的调用。
// 收紧方向记 v4（凭证只走 header/query + 按设备签发/可撤销 + 审批与调用方身份绑定 +
// 全量审计）。这是**有意的口径**，不是待修的 bug——在 loopback 威胁模型里，
// 能连上 hub 的进程本来就与 hub 同权。
//
// 另：**应答被接受（HTTP 200 / WS 无错）= 应答已投递 ≠ 这一轮被放行**。等待者
// （loop）可能在应答到达的同一瞬间 ctx 已结束（整轮超时/取消）→ 按 fail-close 仍落 deny，
// note 写"已收到应答但上下文已结束"（与"无人应答"区分，便于事后复盘）。
//
// # 前端接入方式（v3-S4，G5 口径）：**必须同源，或自备代理**
//
// hub 只监听 loopback（N5b）+ WS 校验 Origin（同源才放行）+ **/api/* 不返回 CORS 头**——
// 这是**有意的安全口径**（无 cookie、无 token，跨源页面不该能驱动本地 hub）。因此第三方前端：
//
//   - **同源托管**：把前端静态文件放到 hub 的 Web UI 位（GET / 由嵌入的 index.html 服务），
//     或用一个**同源反向代理**把静态资源 + /api + /ws 一起转发到 hub；
//   - **不要**指望跨源 fetch / 直连 ws（浏览器会因 CORS / Origin 校验被拒——403 origin_rejected）。
//
// 非浏览器客户端（CLI / 脚本 / curl，无 Origin 头）不受此限，可直连。
//
// # 计数 / 时间 / 分页口径（v3-S4，A1/A3/A5 一次说清）
//
//   - **A1 /api/sessions 计数口径**：在线表是 **agent session 面板**——hub 伪 session（hub.*
//     事件族，如 hub.alert）与 /btw 旁支 fork session **都不进表**（前者无 agent，后者不是一等
//     会话，见 host/fork.go）。所以"事件流里有 N 条、表里 M 行"对不上是正常的：表只列 agent session。
//   - **A3 告警时间字段两副面孔**：/api/monitor 的 `Alert.at` 是 RFC3339 **带时区**（活状态、每次
//     采样一条）；hub.alert 事件的时间在 `envelope.ts`（RFC3339Nano **UTC**，60s 抑制一条）。同一事实
//     两个消费面，字段名/位置都不同——消费者按面适配（详见 host/alert_event.go）。
//   - **A5 分页口径**：三个读面各有"还有没有更多"的信号，**形态不统一是故意的**——/api/inbox 给
//     **next_seq 游标 + has_more**（拉模型收件箱，游标翻页）；/api/inquiry 给 **truncated 信号**
//     （事件窗口，无游标，要更多得自己抬 from）；**/api/events**（G3 回放面）给 **from/to/limit +
//     truncated**（按 seq 翻页，追到 watermark 即追平事实流）。消费端按面读各自的信号。
//
// # G10：派活 handoff 的端点清单与代码有出入（笔误）——**以 host/http.go 的注册面为准**。
// （handoff 曾把 GET /api/merge/check 写成 GET，实为 POST；且漏了 /api/graph/impact、
// /api/graph/rebuild、/api/merge/review|predict。代码是唯一事实源，本文不再复抄清单。）
package host
