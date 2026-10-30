package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/coder/websocket"
)

// log 是传输层的告警出口（Origin 拒绝 / 订阅丢事件）。用 slog，可被宿主接管。
var log = slog.Default()

// Server 把一条 core/bus 事件流通过 WS+NDJSON 暴露给客户端（N4）。
//
// 并发：每个连接一个 ServeWS goroutine（自己的读循环 + 一个推送 goroutine），
// 互不共享可变状态；唯一共享的是 *bus.Bus（其本身并发安全）。连接注册表
// （conns）由 connsMu 保护，仅服务于 Broadcast（广播不经订阅过滤的控制消息）。
type Server struct {
	bus *bus.Bus

	// maxRead 单条入站消息的字节上限（<=0 用 maxLineBytes）。
	maxRead int64

	// inbound 处理需要宿主回应的入站 kind（command.submit / approval.respond）。
	// nil = 纯事件服务端（这些 kind 走 unknown_kind）。经 SetInboundHandler 注入。
	inbound InboundHandler

	// conns 是活动连接注册表（Broadcast 用：向所有连接推控制消息，如 approval.request）。
	connsMu sync.Mutex
	conns   map[*wsConn]struct{}
}

// InboundHandler 处理需要宿主回应的入站消息（command.submit / approval.respond，G1）。
//
// transport 只做协议分发，业务在宿主（library 不绑协议，协议绑定在 host 层——C2/C3）。
// 返回要回给客户端的应答消息（nil = 只收不回，效果经事件流可见）；返回 error 时
// transport 回一条 kind=error（reply_to 指向原消息）但**不关连接**（D7 宽容）。
type InboundHandler interface {
	HandleInbound(ctx context.Context, m contracts.TransportMessage) (*contracts.TransportMessage, error)
}

// Broadcaster 向所有活动连接广播一条消息（不经订阅过滤）。实现是 *Server。
//
// 用于 approval.request 这类"任何窗口的客户端都该看到、都能应答"的控制消息——
// 订阅过滤（session/kind/durable）是事件流的口径，不适用于审批广播。
type Broadcaster interface {
	Broadcast(kind string, payload any)
}

// NewServer 构造传输服务端。bus 必填（nil → panic：没有事件流的 WS 服务没有意义，
// 静默降级只会把故障推迟到更难诊断的地方）。
func NewServer(b *bus.Bus) *Server {
	if b == nil {
		panic("transport: NewServer 需要非 nil 的 *bus.Bus")
	}
	return &Server{bus: b, maxRead: maxLineBytes, conns: make(map[*wsConn]struct{})}
}

// SetInboundHandler 注入入站处理器（command.submit / approval.respond）。
// 传 nil = 回到纯事件服务端（这些 kind 走 unknown_kind）。装配期调用一次。
func (s *Server) SetInboundHandler(h InboundHandler) {
	s.inbound = h
}

// Broadcast 向所有活动连接广播一条消息（不经订阅过滤）。
//
// 单条消息序列化失败（理论不可达）→ 静默跳过（不因一条坏广播打断其它连接）。
// 向某个连接写失败（连接已死）→ 忽略（它的 ServeWS defer 会摘除它）。
func (s *Server) Broadcast(kind string, payload any) {
	if s == nil {
		return
	}
	m, err := NewMessage(kind, payload)
	if err != nil {
		return
	}
	// 快照连接（持锁仅拷指针），锁外发送——避免持 connsMu 时被一个慢/死连接阻塞。
	s.connsMu.Lock()
	conns := make([]*wsConn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.connsMu.Unlock()
	for _, c := range conns {
		_ = c.send(m)
	}
}

// track / untrack 维护活动连接注册表（Broadcast 用）。
func (s *Server) track(c *wsConn) {
	s.connsMu.Lock()
	s.conns[c] = struct{}{}
	s.connsMu.Unlock()
}

func (s *Server) untrack(c *wsConn) {
	s.connsMu.Lock()
	delete(s.conns, c)
	s.connsMu.Unlock()
}

// ServeWS 升级 HTTP 连接并按 NDJSON 行式协议服务（挂到 GET /ws 上）。
//
// 顺序：Origin 校验（N5b）→ websocket.Accept → 读循环（subscribe/ping）→ 推送循环
// （kind=event）。Accept 自己也会做一次同源校验（不传 OriginPatterns 时），
// 这里显式先做一遍是为了给出 JSON 错误体与一条可审计的告警，而不是默认的纯文本 403。
func (s *Server) ServeWS(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.bus == nil {
		writeHTTPError(w, http.StatusInternalServerError, CodeServerNotReady, "transport: 服务端未装配事件总线")
		return
	}
	if err := checkOrigin(r); err != nil {
		log.Warn("transport: 拒绝跨源 WebSocket 连接（N5b）",
			"origin", r.Header.Get("Origin"), "host", r.Host, "remote", r.RemoteAddr, "reason", err.Error())
		writeHTTPError(w, http.StatusForbidden, CodeOriginRejected, err.Error())
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{})
	if err != nil {
		// Accept 已写好错误响应；这里只记一笔（恶意/畸形握手不该刷屏，故用 Info）。
		log.Info("transport: WebSocket 握手失败", "remote", r.RemoteAddr, "err", err.Error())
		return
	}
	limit := s.maxRead
	if limit <= 0 {
		limit = maxLineBytes
	}
	conn.SetReadLimit(limit)

	// 连接生命周期上下文：派生自请求上下文（服务器关停时传导），
	// 但握手后不再直接使用 r（http.Hijacker 的已知坑）。
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	c := &wsConn{conn: conn, bus: s.bus, ctx: ctx}
	s.track(c)
	defer func() {
		// 先摘订阅（关通道 → 推送 goroutine 退出）再关连接，避免向已关闭连接写。
		c.unsubscribe()
		s.untrack(c)
		_ = conn.CloseNow()
	}()

	s.serveConn(ctx, c)
}

// serveConn 是单连接的读循环：解每一行、分发、回复。
func (s *Server) serveConn(ctx context.Context, c *wsConn) {
	for {
		typ, data, err := c.conn.Read(ctx)
		if err != nil {
			if ctx.Err() == nil && !isNormalClose(err) {
				log.Info("transport: 连接读失败", "err", err.Error())
			}
			return
		}
		if typ != websocket.MessageText && typ != websocket.MessageBinary {
			continue // 控制帧由库处理；其余类型忽略
		}
		msgs, derr := decodeLines(data)
		if derr != nil {
			if serr := c.sendError(nil, CodeBadJSON, derr.Error()); serr != nil {
				return
			}
			continue // 一条坏消息不打死连接（D7 宽容）
		}
		for _, m := range msgs {
			if err := s.handle(ctx, c, m); err != nil {
				return
			}
		}
	}
}

// handle 分发一条入站消息。
func (s *Server) handle(ctx context.Context, c *wsConn, m contracts.TransportMessage) error {
	switch m.Kind {
	case KindPing:
		pong, err := NewMessage(KindPong, nil)
		if err != nil {
			return c.sendError(&m.ID, CodeUnknownKind, err.Error())
		}
		pong.ReplyTo = &m.ID
		pong.SessionID = m.SessionID
		return c.send(pong)

	case KindSubscribe:
		var req contracts.SubscribeRequest
		if err := decodePayload(m, &req); err != nil {
			return c.sendError(&m.ID, CodeBadPayload, err.Error())
		}
		// 过滤口径与总线一致（K4）；切片已被 FilterFromRequest 复制。
		replaced := c.subscribe(bus.FilterFromRequest(req))
		// G8：订阅生效回执（回显生效过滤条件 + 是否替换了前一个订阅），让客户端
		// 不必靠"预期事件到不到"反推订阅是否生效。
		ack, err := NewMessage(KindSubscribed, subscribeAckFrom(c.filter(), replaced))
		if err != nil {
			return c.sendError(&m.ID, CodeUnknownKind, err.Error())
		}
		ack.ReplyTo = &m.ID
		ack.SessionID = m.SessionID
		return c.send(ack)

	case KindCommandSubmit, KindApprovalRespond:
		// 需要宿主回应的入站 kind（G1 遥控面）。未装配 InboundHandler → 不支持。
		if s.inbound == nil {
			return c.sendError(&m.ID, CodeUnknownKind,
				fmt.Sprintf("服务端未装配入站处理器，不支持 kind=%q", m.Kind))
		}
		reply, herr := s.inbound.HandleInbound(ctx, m)
		if herr != nil {
			// 业务错误回一条 kind=error 但**不关连接**（D7：一条坏消息不打死连接）。
			return c.sendError(&m.ID, CodeBadPayload, herr.Error())
		}
		if reply != nil {
			reply.ReplyTo = &m.ID
			if reply.SessionID == nil {
				reply.SessionID = m.SessionID
			}
			return c.send(*reply)
		}
		return nil

	case KindEvent:
		// 事件只能由服务端发（客户端发 = 协议违规，可能是把角色搞反了的脚本）。
		return c.sendError(&m.ID, CodeForbiddenKind,
			"kind=event 只能由服务端推送；客户端请用 kind=subscribe 订阅")

	default:
		return c.sendError(&m.ID, CodeUnknownKind, fmt.Sprintf("不支持的 kind=%q", m.Kind))
	}
}

// subscribeAckFrom 把生效的过滤条件折成订阅回执载荷（G8）。
func subscribeAckFrom(f bus.Filter, replaced bool) contracts.SubscribeAck {
	ack := contracts.SubscribeAck{Replaced: replaced}
	if len(f.SessionIDs) == 1 {
		sid := f.SessionIDs[0]
		ack.SessionID = &sid
	}
	if len(f.KindPrefixes) > 0 {
		ack.KindPrefixes = append([]string(nil), f.KindPrefixes...)
	}
	if f.DurableOnly {
		v := true
		ack.DurableOnly = &v
	}
	return ack
}

// checkOrigin 实现 N5b 的 Origin 判据（见包 doc 的安全边界一节）。
func checkOrigin(r *http.Request) error {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return nil // 非浏览器客户端（CLI / 脚本 / curl）：无 Origin 头
	}
	u, err := url.Parse(origin)
	if err != nil {
		return fmt.Errorf("Origin %q 不是合法 URL: %w", origin, err)
	}
	if u.Host == "" {
		return fmt.Errorf("Origin %q 不是带 host 的合法 URL", origin)
	}
	if strings.EqualFold(u.Host, r.Host) {
		return nil // 同源：hub 自己的 Web UI
	}
	return fmt.Errorf("Origin %q 与请求 Host %q 不同源（N5b：v0 只监听 loopback + 无 cookie，拒绝跨源页面）", u.Host, r.Host)
}

// isNormalClose 判定是否为正常的连接关闭（客户端主动走人 / 服务端关停）。
func isNormalClose(err error) bool {
	return errors.Is(err, context.Canceled) ||
		websocket.CloseStatus(err) == websocket.StatusNormalClosure ||
		websocket.CloseStatus(err) == websocket.StatusGoingAway
}

// writeHTTPError 在握手前/失败时写一个 JSON 错误体（与 WS 内的 TransportError 同形状）。
func writeHTTPError(w http.ResponseWriter, code int, errCode, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(contracts.TransportError{Code: errCode, Message: message})
}

// ---------------------------------------------------------------------------
// 单连接封装：写互斥 + 订阅生命周期 + 丢事件告知
// ---------------------------------------------------------------------------

// wsConn 是一个已升级的连接：持有一个（可替换的）订阅与连接生命周期上下文。
type wsConn struct {
	conn *websocket.Conn
	bus  *bus.Bus
	// ctx 是连接生命周期上下文：取消即"连接结束"，推送 goroutine 据此退出、
	// 进行中的写被中断（否则向一个已死的 TCP 连接写会阻塞到超时）。
	ctx context.Context

	mu     sync.Mutex
	sub    *bus.Subscription
	curFil bus.Filter // 当前订阅的过滤条件（subscribed 回执回显用，与 sub 同锁更新）
}

// subscribe 换上新订阅：旧订阅先摘（关通道 → 旧推送 goroutine 自然退出）。
//
// 重复 subscribe = 替换（契约的 SubscribeRequest 只有一组过滤条件，一个连接一条订阅）。
// 返回是否替换了此前的订阅（供 subscribed 回执的 replaced 字段，G8）。
func (c *wsConn) subscribe(f bus.Filter) (replaced bool) {
	c.mu.Lock()
	if c.sub != nil {
		c.sub.Close()
		c.sub = nil
		replaced = true
	}
	sub := c.bus.Subscribe(f)
	c.sub = sub
	c.curFil = f
	c.mu.Unlock()

	go c.pushLoop(sub)
	return replaced
}

// filter 返回当前订阅的过滤条件（subscribed 回执回显用；无订阅 → 零值 Filter）。
func (c *wsConn) filter() bus.Filter {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curFil
}

// unsubscribe 摘掉当前订阅（幂等）：关通道 → 推送 goroutine 退出。
func (c *wsConn) unsubscribe() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sub != nil {
		c.sub.Close()
		c.sub = nil
	}
}

// pushLoop 把订阅里的事件按行推给客户端。
//
// 丢事件要**说出来**（Lagging）：事实事件仍在库里，客户端可按 seq 水位补读，
// 但"假装一条没少"会让客户端的窗口出现无法解释的空洞。
func (c *wsConn) pushLoop(sub *bus.Subscription) {
	var reported uint64
	for {
		select {
		case <-c.ctx.Done():
			return
		case ev, ok := <-sub.Events():
			if !ok {
				return // 订阅被摘除（换订阅 / 连接结束）
			}
			m, err := NewMessage(KindEvent, ev)
			if err != nil {
				continue // 单条事件序列化失败不该打断连接（理论上不可达）
			}
			m.SessionID = &ev.SessionID
			if err := c.send(m); err != nil {
				return // 连接已死：推送 goroutine 退出
			}
			if n := sub.Lagging(); n > reported {
				reported = n
				// 补读指引指向**协议面**的端点（v3-S4 G3）：此前这里写的是 Go API
				// （Store.ReadFrom）——进程外客户端根本没有那个 API，断线后的事件窗口
				// 补不回来（"事件流是唯一事实源"对进程外消费者不成立）。
				msg, _ := NewErrorMessage(nil, CodeSubscriptionLagging,
					fmt.Sprintf("订阅通道满，已丢弃 %d 条通知；事实事件仍在事件库里，"+
						"请按自己的 seq 水位用 GET /api/events?from=&limit= 补读"+
						"（流事件丢了就是丢了）", n))
				if err := c.send(msg); err != nil {
					return
				}
			}
		}
	}
}

// send 写一条消息（一行 NDJSON）。写互斥保证一行不被并发写交错
// （c.mu 同时保护订阅替换；两件事都极轻，共用一把锁）。
func (c *wsConn) send(m contracts.TransportMessage) error {
	line, err := encodeLine(m)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Write(c.ctx, websocket.MessageText, line)
}

// sendError 回一条 kind=error（reply_to 指向出错消息；nil = 无对应消息）。
//
// reply_to 是**客户端关联 id**（与 TransportMessage.id 同一形状：string，不限格式——
// 客户端可以用任意不透明 id，服务端原样回指，G9）。
func (c *wsConn) sendError(replyTo *string, code, message string) error {
	m, err := NewErrorMessage(replyTo, code, message)
	if err != nil {
		return err
	}
	return c.send(m)
}
