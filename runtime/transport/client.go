package transport

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/coder/websocket"
)

// inboxBuffer 是读循环→调用方之间的缓冲条数。
//
// 为什么要缓冲 + 独立的读循环：底层库的语义是"传给 Read 的 ctx 被取消 = 连接关闭"
// （setupReadTimeout 里 context.AfterFunc 直接 c.close()）。若每次 Recv 都带超时
// ctx，"等一条消息但没等到"就会把连接打死——CLI 的轮询场景完全没法用。于是读循环
// 用**背景**上下文常驻，超时只影响"这一次调用等不等"，不影响连接。
const inboxBuffer = 256

// Client 是 CLI / 脚本用的 WS+NDJSON 客户端（与 Server 同一套行式协议）。
//
// 用法：
//
//	c, err := transport.Dial(ctx, "ws://127.0.0.1:8720/ws")
//	defer c.Close()
//	_ = c.Subscribe(ctx, contracts.SubscribeRequest{KindPrefixes: []string{"context.assembled"}})
//	for {
//	    m, err := c.Recv()          // 阻塞到有一条消息
//	    if m.Kind == transport.KindEvent { ... }
//	}
//
// 并发：Send 可被多个 goroutine 调用（内部写互斥）；**Recv 不要并发调用**
// （一个连接只有一个读循环，与 Server 侧同口径）。
type Client struct {
	conn *websocket.Conn

	inbox   chan contracts.TransportMessage // 读循环 → 调用方
	quit    chan struct{}                   // Close 时关闭（读循环据此退出）
	dead    chan struct{}                   // 读循环退出时关闭
	closed  atomic.Bool
	readErr atomic.Pointer[error] // 读循环退出原因（只写一次）
}

// Dial 连接一个 WS+NDJSON 服务端（url 形如 ws://127.0.0.1:8720/ws）。
//
// ctx 只作用于握手（拨号 + 读响应头）；连接建立后的读写在 Recv/Send 的生命周期里，
// 由 Close 结束（内部读循环用背景上下文，见 inboxBuffer 的说明）。
func Dial(ctx context.Context, url string) (*Client, error) {
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		// v0 不协商子协议：NDJSON 行式不需要。
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return nil, fmt.Errorf("transport: dial %s: %w", url, err)
	}
	conn.SetReadLimit(maxLineBytes)
	c := &Client{
		conn:  conn,
		inbox: make(chan contracts.TransportMessage, inboxBuffer),
		quit:  make(chan struct{}),
		dead:  make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

// readLoop 常驻读循环：解每一行、投进 inbox。
//
// 退出条件：读错误（对端关闭 / 网络断）或 Close（quit）。退出原因记进 readErr，
// 供 Recv 返回（连接正常结束 vs 真故障，调用方看得见）。
func (c *Client) readLoop() {
	defer close(c.dead)
	for {
		typ, data, err := c.conn.Read(context.Background())
		if err != nil {
			c.readErr.CompareAndSwap(nil, &err)
			return
		}
		if typ != websocket.MessageText && typ != websocket.MessageBinary {
			continue
		}
		msgs, derr := decodeLines(data)
		if derr != nil {
			// 坏行跳过（服务端协议违规时它自己会发 kind=error，那才是可读的信号）。
			continue
		}
		for _, m := range msgs {
			select {
			case c.inbox <- m:
			case <-c.quit:
				return
			}
		}
	}
}

// Subscribe 发送一条 kind=subscribe 请求（K4 远程订阅）。
//
// **有回执**（G8）：服务端先回一条 kind=subscribed（payload=SubscribeAck，reply_to 指向
// 本条 subscribe），回显**生效的**过滤条件与 replaced（是否替换了此前的订阅）——
// 想确认订阅生效就收这条消息（别再用"预期事件到不到"反推）。
func (c *Client) Subscribe(ctx context.Context, req contracts.SubscribeRequest) error {
	m, err := NewMessage(KindSubscribe, req)
	if err != nil {
		return err
	}
	return c.SendContext(ctx, m)
}

// Ping 发一条心跳请求（不等应答；要等应答用 Recv 收 kind=pong）。
func (c *Client) Ping(ctx context.Context) error {
	m, err := NewMessage(KindPing, nil)
	if err != nil {
		return err
	}
	return c.SendContext(ctx, m)
}

// Send 写一条消息（阻塞版，用背景上下文）。
func (c *Client) Send(m contracts.TransportMessage) error {
	return c.SendContext(context.Background(), m)
}

// SendContext 写一条消息（一行 NDJSON）。
func (c *Client) SendContext(ctx context.Context, m contracts.TransportMessage) error {
	if c == nil || c.conn == nil {
		return errors.New("transport: Client 为 nil")
	}
	line, err := encodeLine(m)
	if err != nil {
		return err
	}
	if c.closed.Load() {
		return errors.New("transport: 连接已关闭")
	}
	return c.conn.Write(ctx, websocket.MessageText, line)
}

// Recv 阻塞读取下一条消息（背景上下文版）。
func (c *Client) Recv() (contracts.TransportMessage, error) {
	return c.RecvContext(context.Background())
}

// RecvContext 读取下一条消息；ctx 超时/取消只让**这一次调用**返回
// （连接仍然活着，见 inboxBuffer 的说明）。
//
// 连接结束（对端关闭 / 自己 Close）→ 返回 error：
// errors.Is(err, net.ErrClosed) 或 websocket.CloseStatus(err) != -1 表示正常结束。
func (c *Client) RecvContext(ctx context.Context) (contracts.TransportMessage, error) {
	if c == nil || c.conn == nil {
		return contracts.TransportMessage{}, errors.New("transport: Client 为 nil")
	}
	// 连接已结束但 inbox 里可能还有未取走的残留：先取干净再报错。
	select {
	case m := <-c.inbox:
		return m, nil
	default:
	}
	select {
	case m := <-c.inbox:
		return m, nil
	case <-c.dead:
		// 再抢一次：读循环退出前的最后几条可能还在管道里。
		select {
		case m := <-c.inbox:
			return m, nil
		default:
		}
		if p := c.readErr.Load(); p != nil {
			return contracts.TransportMessage{}, fmt.Errorf("transport: 连接结束: %w", *p)
		}
		return contracts.TransportMessage{}, errors.New("transport: 连接已关闭")
	case <-ctx.Done():
		return contracts.TransportMessage{}, ctx.Err()
	}
}

// Close 关闭连接（幂等）：关 quit → 读循环退出 → 关底层连接。
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(c.quit)
	return c.conn.Close(websocket.StatusNormalClosure, "")
}
