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
//	client ── kind=ping ──▶ server ── kind=pong（reply_to=ping.id）──▶ client
//	server ── kind=event（payload=contracts.Event）──▶ client   （按订阅过滤推送）
//	server ── kind=error（payload=TransportError）──▶ client     （协议违规 / 订阅丢事件）
//
// 订阅过滤口径 = bus.FilterFromRequest（K4 远程订阅）：session_id / kind_prefixes /
// durable_only 三项，AND 语义，全空 = 全收。
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
// ⚠ 这不是认证，也**不足以**支撑远程访问（N4b）：远程化需要凭证只走
// header/query（绝不走 cookie）+ token 可撤销/按设备签发 + 降权视图 + 全量审计 +
// 公网 `wss://`。v0 不做远程，见 docs/research/07 的 N4b/N5b。
//
// # 丢失语义（诚实取舍）
//
// 订阅者通道满即丢（bus 的 Lagging 计数）——事实事件仍在库里，客户端可按自己的
// seq 水位经 store.ReadFrom 补读；本包在检测到丢弃时会主动推一条 kind=error
// （code=subscription_lagging）告知丢了多少条，不假装"一条没少"。
package transport
