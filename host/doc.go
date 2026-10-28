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
package host
