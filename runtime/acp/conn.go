package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// conn 是 ACP 的 stdio 连接：一侧是 newline-delimited 入站帧（frameScanner），
// 另一侧是**唯一**的出站字节流（io.Writer，产品面 = os.Stdout）。
//
// # 写互斥（stdout 纯净性的另一半）
//
// 出站帧可能来自多个 goroutine：读循环（对 initialize/session/new 的同步响应）、
// 每轮 prompt 的 goroutine（session/update 通知 + session/prompt 响应）、审批器
// （session/request_permission 请求）。wmu 保证"一行一帧"不被并发写交缠——一行里
// 混进两条 JSON 就是对客户端的违约。**除本类型外没有任何代码能写 Out**（日志走
// 独立 logger，见 doc.go），这是 stdout 纯净性的结构保证。
type conn struct {
	scanner *frameScanner
	out     io.Writer
	wmu     sync.Mutex

	// idSeq 只增计数，配 idPrefix 成对外请求的字符串 id（见 newID）。
	idSeq    atomic.Int64
	idPrefix string

	// pending  correlating 我们发出的请求（session/request_permission）与客户端回响应。
	// 键 = 请求 id 的字符串形（见 idString），值 = 投递应答的缓冲通道（buffered 1）。
	mu      sync.Mutex
	pending map[string]chan inboundResponse
}

// inboundResponse 是客户端对某个出站请求的响应载荷（result 与 err 二选一，
// 与 JSON-RPC 响应形状一致；都空 = 客户端回了空 result）。
type inboundResponse struct {
	result json.RawMessage
	err    *rpcError
}

func newConn(r *bufio.Reader, out io.Writer) *conn {
	return &conn{
		scanner:  newFrameScanner(r),
		out:      out,
		idPrefix: "alasol-acp",
		pending:  make(map[string]chan inboundResponse),
	}
}

// newID 生成一个**字符串**请求 id（JSON-RPC 允许字符串 id；逐连接单调递增，
// 单进程单连接足够唯一）。用字符串而非数字：pending 表按字符串键匹配更直白，
// 也避免与客户端可能用的数字 id 混淆（id 必须同型回指）。
func (c *conn) newID() json.RawMessage {
	n := c.idSeq.Add(1)
	raw, _ := json.Marshal(fmt.Sprintf("%s-%d", c.idPrefix, n))
	return raw
}

// writeFrame 序列化并写出一帧（raw + "\n"，单次 Write 尽量保证一行原子）。
//
// 持 wmu：多 goroutine 写不交缠。序列化失败 = 这一帧发不出（调用方记日志继续——
// 单帧丢不该打断连接，D7）。
func (c *conn) writeFrame(o outbound) error {
	raw, err := json.Marshal(o)
	if err != nil {
		return fmt.Errorf("acp: 序列化出站帧失败: %w", err)
	}
	buf := make([]byte, 0, len(raw)+1)
	buf = append(buf, raw...)
	buf = append(buf, '\n')
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if _, err := c.out.Write(buf); err != nil {
		return fmt.Errorf("acp: 写出站帧失败: %w", err)
	}
	return nil
}

// sendResult 回一条成功响应（回指 id）。
func (c *conn) sendResult(id json.RawMessage, result any) error {
	o, err := newResult(id, result)
	if err != nil {
		return err
	}
	return c.writeFrame(o)
}

// sendError 回一条错误响应（回指 id）。
func (c *conn) sendError(id json.RawMessage, code int, message string, data any) error {
	o, err := newError(id, code, message, data)
	if err != nil {
		return err
	}
	return c.writeFrame(o)
}

// sendNotification 发一条通知（无 id）。
func (c *conn) sendNotification(method string, params any) error {
	o, err := newNotification(method, params)
	if err != nil {
		return err
	}
	return c.writeFrame(o)
}

// call 发一条请求并**阻塞等**客户端回响应（或 ctx 结束）。
//
// 用于 agent→client 的 session/request_permission。返回：
//
//	(result, nil, nil)  客户端回了成功响应（result 可能为 nil = 空 result）
//	(nil, rpcErr, nil)  客户端回了 JSON-RPC 错误
//	(?, ?, ctxErr)      ctx 先结束（超时/取消）——fail-close；若应答与 ctx 结束竞态
//	                    到达，result/rpcErr 仍被带回供上层写更准的审计 note
//	(nil, nil, writeErr) 请求根本没发出去（序列化/写失败）
//
// pending 注册在**写之前**：写失败也要摘掉，否则这条 id 永远等不到应答（泄漏）。
func (c *conn) call(ctx context.Context, method string, params any) (json.RawMessage, *rpcError, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err // 已结束：不必发
	}
	id := c.newID()
	key, _ := idString(id)
	ch := make(chan inboundResponse, 1)

	c.mu.Lock()
	if c.pending == nil {
		c.pending = make(map[string]chan inboundResponse)
	}
	c.pending[key] = ch
	c.mu.Unlock()

	o, err := newRequest(id, method, params)
	if err != nil {
		c.removePending(key)
		return nil, nil, err
	}
	if err := c.writeFrame(o); err != nil {
		c.removePending(key)
		return nil, nil, err
	}

	select {
	case resp := <-ch:
		return resp.result, resp.err, nil
	case <-ctx.Done():
		c.removePending(key)
		// 竞态复核：应答可能与 ctx 结束同一瞬间到达。非阻塞看一眼——到了就把载荷带回
		// （上层据此区分"无人应答"与"已投递但未放行"，与 host protocolApprover 同口径）。
		select {
		case resp := <-ch:
			return resp.result, resp.err, ctx.Err()
		default:
		}
		return nil, nil, ctx.Err()
	}
}

// deliverResponse 把客户端回的一条响应投递给等待中的 call（读循环调）。
//
// 未知 id（没有 pending / id 非同型字符串）→ 静默忽略：那可能是迟到的重复响应，
// 或客户端自发主动的响应——都不该惊动连接。
func (c *conn) deliverResponse(resp inboundResponse, idKey string) {
	c.mu.Lock()
	ch, ok := c.pending[idKey]
	if ok {
		delete(c.pending, idKey)
	}
	c.mu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- resp: // buffered 1：不阻塞
	default: // 理论上不可达（每 id 只投一次）；防守性不阻塞
	}
}

// removePending 摘掉一条 pending（幂等；call 的失败/ctx 路径用）。
func (c *conn) removePending(key string) {
	c.mu.Lock()
	if _, ok := c.pending[key]; ok {
		delete(c.pending, key)
	}
	c.mu.Unlock()
}
