// Package transport 实现 hub↔客户端的事件传输（N4：**WebSocket + NDJSON**）。
//
// # 协议（contracts/schema/transport.json）
//
// 传输是**行式**的：一个 WebSocket 文本消息 = 一行 NDJSON = 一个
// contracts.TransportMessage（出站带结尾 "\n"，入站容忍一行消息里带换行的多行）。
// 选 WS+NDJSON 而不是 protobuf/gRPC 的理由见 07 的 N4：与 D1/D2 已定契约一致、
// 不引入 protobuf 工具链、浏览器/手机原生可用零额外组件、`curl` 即可调试流。
//
// 消息流：
//
//	client ── kind=subscribe（payload=SubscribeRequest）──▶ server
//	server ── kind=subscribed（payload=SubscribeAck，reply_to=subscribe.id）──▶ client  （G8 订阅回执）
//	client ── kind=ping ──▶ server ── kind=pong（reply_to=ping.id）──▶ client
//	server ── kind=event（payload=contracts.Event）──▶ client   （按订阅过滤推送）
//	server ── kind=error（payload=TransportError）──▶ client     （协议违规 / 订阅丢事件）
//
// 断线补读（v3-S4，G3）：WS 断线即丢，**事实事件**另有 HTTP 回放面——
//
//	GET /api/events?from=<seq>&limit=<n>&session_id=<ulid>
//	  → {"events":[…],"watermark":N,"from":X,"to":Y,"truncated":bool}
//
//	客户端按自己最后收到的 seq 续读（from=last_seq+1），直到 truncated=false；
//	watermark 是**事实流 tip**（store 的 MAX(seq)），追平它即追上唯一事实源。
//	流事件（durable=false）不入库、不回放——丢了就是丢了。
//
// 遥控面（v3-S4，G1；需宿主经 SetInboundHandler 注入 InboundHandler 才启用）：
//
//	client ── kind=command.submit（payload=CommandSubmit）──▶ server ── kind=command.accepted ──▶ client
//	server ── kind=approval.request（payload=ApprovalRequest）──▶ client   （Broadcast：所有连接，不经订阅过滤）
//	client ── kind=approval.respond（payload=ApprovalResponse）──▶ server  （注入挂起的审批；无人应答 = deny，fail-close）
//
// 订阅过滤口径 = bus.FilterFromRequest（K4 远程订阅）：session_id / kind_prefixes /
// durable_only 三项，AND 语义，全空 = 全收。**Broadcast 不走订阅过滤**（approval.request
// 要所有连接都看到、都能应答）。
//
// # loopback 安全边界（N5b / O7，v0 的口径）
//
// WebSocket **不受同源策略约束**：hub 即使只监听 127.0.0.1，浏览器里的任意恶意网页
// 都能发起 `ws://127.0.0.1:port` 连接。若凭证用 cookie，浏览器会自动携带 → 等于把 hub
// 暴露给任何网页。因此 v0 的凭证方案是**两条一起**：
//
//  1. hub 只监听 loopback（host 包负责，默认 127.0.0.1:8720）；
//  2. **不使用 cookie**，本连接上没有任何浏览器会自动携带的凭证；
//     于是这里的 Origin 校验只是"拒绝跨源页面"这一层，而不是认证层。
//
// 判据（checkOrigin）：无 Origin 头（非浏览器客户端：CLI / 脚本 / curl）放行；
// Origin 的 host 与请求 Host 相同（hub 自己的 Web UI，同源）放行；其余 403。
//
// **第三方前端必须同源或自备代理**（v3-S4，G5）：跨源页面（哪怕也是 127.0.0.1 的不同端口）
// 会被这里拒（403 origin_rejected），且 hub 的 /api/* 不返回 CORS 头 → 浏览器跨源 fetch 也被拦。
// 接入方式 = 同源托管前端，或一个**同源反向代理**把静态资源 + /api + /ws 一起转发到 hub
// （见 host/doc.go 的"前端接入方式"一节）。这是有意的安全口径，不是待修的 bug。
//
// ⚠ 这不是认证，也**不足以**支撑远程访问（N4b）：远程化需要凭证只走
// header/query（绝不走 cookie）+ token 可撤销/按设备签发 + 降权视图 + 全量审计 +
// 公网 `wss://`。v0 不做远程，见 docs/research/07 的 N4b/N5b。
//
// # 审批应答的鉴权口径（v0：**有意不做鉴权**）
//
// 审批面（WS approval.respond / HTTP POST /api/approvals/{id}/respond）**没有鉴权**：
// 任何**能连上 hub** 的客户端都能应答审批（v0 只监听 loopback + 无 cookie + 无 token）。
// fail-close 只保证"**无人应答 = deny**"（不得挂起、不得默认放行，D14/H8），**不防乱批**——
// 能连上的人可以抢先 approve 一条本不该放行的调用。收紧方向记 v4（凭证 + 按设备签发/
// 可撤销 + 审批与调用方身份绑定 + 全量审计）。这是**有意的口径**，不是待修的 bug。
//
// 另：**应答被接受（无 kind=error）= 应答已投递 ≠ 这一轮被放行**——等待者可能在应答
// 到达的同一瞬间 ctx 已结束（整轮超时/取消）→ 按 fail-close 仍落 deny。
//
// # 丢失语义（诚实取舍）
//
// 订阅者通道满即丢（bus 的 Lagging 计数）——事实事件仍在库里，客户端可按自己的
// seq 水位经**协议面**的回放端点补读（GET /api/events?from=&limit=，见上"消息流"一节；
// 不是 Go API Store.ReadFrom——进程外客户端没有那个 API）；本包在检测到丢弃时会主动推一条
// kind=error（code=subscription_lagging）告知丢了多少条，不假装"一条没少"。
package transport
