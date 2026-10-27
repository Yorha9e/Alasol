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
// 互不共享可变状态；唯一共享的是 *bus.Bus（其本身并发安全）。
type Server struct {
	bus *bus.Bus

	// maxRead 单条入站消息的字节上限（<=0 用 maxLineBytes）。
	maxRead int64
}

// NewServer 构造传输服务端。bus 必填（nil → panic：没有事件流的 WS 服务没有意义，
// 静默降级只会把故障推迟到更难诊断的地方）。
func NewServer(b *bus.Bus) *Server {
	if b == nil {
		panic("transport: NewServer 需要非 nil 的 *bus.Bus")
	}
	return &Server{bus: b, maxRead: maxLineBytes}
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
	defer func() {
		// 先摘订阅（关通道 → 推送 goroutine 退出）再关连接，避免向已关闭连接写。
		c.unsubscribe()
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
		c.subscribe(bus.FilterFromRequest(req))
		return nil

	case KindEvent:
		// 事件只能由服务端发（客户端发 = 协议违规，可能是把角色搞反了的脚本）。
		return c.sendError(&m.ID, CodeForbiddenKind,
			"kind=event 只能由服务端推送；客户端请用 kind=subscribe 订阅")

	default:
		return c.sendError(&m.ID, CodeUnknownKind, fmt.Sprintf("不支持的 kind=%q", m.Kind))
	}
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

	mu  sync.Mutex
	sub *bus.Subscription
}

// subscribe 换上新订阅：旧订阅先摘（关通道 → 旧推送 goroutine 自然退出）。
//
// 重复 subscribe = 替换（契约的 SubscribeRequest 只有一组过滤条件，一个连接一条订阅）。
func (c *wsConn) subscribe(f bus.Filter) {
	c.mu.Lock()
	if c.sub != nil {
		c.sub.Close()
		c.sub = nil
	}
	sub := c.bus.Subscribe(f)
	c.sub = sub
	c.mu.Unlock()

	go c.pushLoop(sub)
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
				msg, _ := NewErrorMessage(nil, CodeSubscriptionLagging,
					fmt.Sprintf("订阅通道满，已丢弃 %d 条通知；事实事件仍在事件库里，"+
						"请按自己的 seq 水位用 Store.ReadFrom 补读（流事件丢了就是丢了）", n))
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
func (c *wsConn) sendError(replyTo *contracts.Ulid, code, message string) error {
	m, err := NewErrorMessage(replyTo, code, message)
	if err != nil {
		return err
	}
	return c.send(m)
}
