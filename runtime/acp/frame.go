// Package acp 的 JSON-RPC 2.0 帧编解码（newline-delimited JSON over stdio）。
//
// 本文件**只**负责"一条消息 ⇄ 一行 JSON"的编解码与三形态判别，不做任何 I/O 策略、
// 不 import host（09 依赖方向：runtime 不 import host）。ACP（Agent Client Protocol，
// Zed 主推）的传输约定：
//
//   - **JSON-RPC 2.0**，每条消息是**一行**紧凑 JSON，以 "\n" 分隔（newline-delimited）；
//   - **stdout 只走 RPC 帧**，一切日志走 stderr（见 doc.go 的"stdout 纯净性"铁律）；
//   - 三形态：请求（带 id + method）、通知（带 method、无 id）、响应（带 id +
//     result 或 error）。批量（batch，JSON 数组）**本实现不支持**——收到即 -32600
//     （Invalid Request；批量语义需要成组的响应数组，本实现不做——见 decodeFrame）。
//
// 为什么自实现而不复用 mcp go-sdk 的 jsonrpc：那个包与 MCP 的会话/能力模型耦合，
// 是 mcp 专用；ACP 的帧面简单到自写更诚实（不引新依赖，go.mod 不动）。
package acp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// JSON-RPC 2.0 标准错误码（规范固化值，逐条对应 spec §5.1）。
const (
	codeParseError     = -32700 // 不是合法 JSON
	codeInvalidRequest = -32600 // 是合法 JSON 但不是合法 Request 对象
	codeMethodNotFound = -32601 // 方法不存在
	codeInvalidParams  = -32602 // 参数不合法
	codeInternalError  = -32603 // 内部错误
	// codeServerError 是**实现自定义**服务器错误的起始（规范保留 -32000..-32099）。
	codeServerError = -32000
)

// rpcError 是 JSON-RPC 的 error 对象（code / message 必填，data 可选）。
type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message) }

// inbound 是**入站**帧的原始形态。
//
// 判别用**键在位**标记（hasID / hasResult / hasError，由 decodeFrame 从原始键集设）而非
// 指针判空——因为 `"result":null` / `"error":null` 经 json.Unmarshal 进 *json.RawMessage
// 会把指针设成 nil，若用"指针非空"判就会把"带了 result 键但值为 null 的响应"误判成非响应。
// 键在位才忠于"客户端到底带没带这个键"：
//
//   - hasResult / hasError 任一 → 这是**响应**；
//   - Method 非 nil 且 hasID → **请求**；
//   - Method 非 nil 且 !hasID → **通知**；
//   - 其余 → 非法请求对象（-32600）。
//
// ID 用 json.RawMessage 原样保留（数字或字符串皆可），回响应时**逐字节回指**——服务端
// 不解释、不改写客户端的关联 id（G9 同款口径）。
type inbound struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      *json.RawMessage `json:"id"`
	Method  *string          `json:"method"`
	Params  json.RawMessage  `json:"params"`
	Result  *json.RawMessage `json:"result"`
	Error   *rpcError        `json:"error"`

	// 键在位标记（decodeFrame 设；区分"没带键"与"带了键但值为 null"）。
	hasID     bool
	hasResult bool
	hasError  bool
}

// isResponse 判别是否为响应（带了 result 或 error 键）。
func (m *inbound) isResponse() bool { return m.hasResult || m.hasError }

// isRequest 判别是否为请求（有 method 且有 id 键）。
func (m *inbound) isRequest() bool { return m.Method != nil && m.hasID }

// isNotification 判别是否为通知（有 method 且无 id 键）。
func (m *inbound) isNotification() bool { return m.Method != nil && !m.hasID }

// idString 把入站 id 规范成 pending 表的键（**仅对字符串 id 有意义**）。
//
// 本实现对外只发字符串 id（见 conn.newID），故客户端回响应时 id 也应是同一个字符串；
// 数字 id / null 不参与 pending 匹配（JSON-RPC 要求 id 同型回指，异型即客户端违约，
// 按"未知响应"忽略，见 conn 的读循环）。
func idString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

// outbound 是**出站**帧（请求 / 响应 / 通知共用的编码形状）。
//
// 三个构造函数各设自己关心的字段；json 的 omitempty 保证"响应只带 result **或**
// error 之一""通知不带 id"（发一个不带 id 的请求 / 带 id 的通知都是违约，客户端会拒）。
type outbound struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`     // 请求 / 响应有；通知无
	Method  string          `json:"method,omitempty"` // 请求 / 通知有；响应无
	Params  json.RawMessage `json:"params,omitempty"` // 请求 / 通知的 params
	Result  json.RawMessage `json:"result,omitempty"` // 响应（成功）
	Error   *rpcError       `json:"error,omitempty"`  // 响应（失败）
}

// newResult 造一条成功响应（回指 id，result 为给定对象；nil result → {} 占位，
// 避免发出一个既无 result 又无 error 的违约响应）。
func newResult(id json.RawMessage, result any) (outbound, error) {
	raw, err := marshalResult(result)
	if err != nil {
		return outbound{}, err
	}
	return outbound{JSONRPC: "2.0", ID: id, Result: raw}, nil
}

// newError 造一条错误响应（回指 id；data 可空）。
func newError(id json.RawMessage, code int, message string, data any) (outbound, error) {
	var raw json.RawMessage
	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			return outbound{}, err
		}
		raw = b
	}
	return outbound{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: message, Data: raw}}, nil
}

// newNotification 造一条通知（无 id；params 为给定对象）。
func newNotification(method string, params any) (outbound, error) {
	raw, err := marshalParams(params)
	if err != nil {
		return outbound{}, err
	}
	return outbound{JSONRPC: "2.0", Method: method, Params: raw}, nil
}

// newRequest 造一条请求（带 id；params 为给定对象）。用于 agent→client 的
// session/request_permission。
func newRequest(id json.RawMessage, method string, params any) (outbound, error) {
	raw, err := marshalParams(params)
	if err != nil {
		return outbound{}, err
	}
	return outbound{JSONRPC: "2.0", ID: id, Method: method, Params: raw}, nil
}

// marshalParams 序列化 params（nil → 缺省字段，由 omitempty 消化）。
func marshalParams(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("acp: 序列化 params 失败: %w", err)
	}
	return b, nil
}

// marshalResult 序列化 result（nil → `{}`，保证成功响应必带 result 字段）。
func marshalResult(v any) (json.RawMessage, error) {
	if v == nil {
		return json.RawMessage(`{}`), nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("acp: 序列化 result 失败: %w", err)
	}
	return b, nil
}

// maxFrameBytes 是单条入站帧的字节上限（防一条无界帧把内存吃满）。
//
// 与 runtime/transport 的 maxLineBytes 同量级（8 MiB）：prompt 原文 + 参数可长，
// 但不该无限。超限的帧按"坏帧"处理：读循环回**一条** -32600（Invalid Request）
// 后**跳过该行继续读后续帧**——一条坏帧不该把连接打死（D7 宽容）。
//
// 实现口径（frameScanner.readLine）：缓冲到上限即**丢弃该行剩余**（内存有界），
// 故既不吃内存、也不卡住读循环；这是 F1 的修复点——旧实现用 bufio.Scanner，它的
// 错误是**粘性**的（ErrTooLong 之后每次 Scan 都失败），读循环把终态错当"可恢复
// 坏帧"无限重试 → 8 秒吐了 118 万条错误帧、进程永不退出。
//
// 额度口径：**帧体**字节数（不含行分隔符）≤ maxFrameBytes —— 恰好 8MiB 的帧**合法**。
const maxFrameBytes = 8 << 20

// 帧层的哨兵错误（读循环用 errors.Is 区分"可恢复的坏帧"与"终态错"）。
var (
	// errFrameTooLong：本帧超限（该行已被跳到行尾——**连接可继续**）。
	errFrameTooLong = errors.New("acp: 帧超过长度上限")
	// errBatchUnsupported：本帧是 JSON 数组（批量）——本实现不支持批量（-32600）。
	errBatchUnsupported = errors.New("acp: 不支持批量数组请求")
)

// terminalReadError 标记**终态**读错误（底层流坏了：连接没法继续）。
//
// 终态错与"单帧解码错"必须分开：前者回**一条**错误帧后**结束连接**，后者回错误帧后
// **继续读**。混为一谈就是 F1 那个无限自旋（终态错被无限重复回帧）。
type terminalReadError struct{ err error }

func (e *terminalReadError) Error() string { return "acp: 底层读错误: " + e.err.Error() }
func (e *terminalReadError) Unwrap() error { return e.err }

// frameReadBufSize 是入站行读取器的缓冲大小（bufio.Reader）。
//
// 只影响读的分块粒度（不影响单帧上限 maxFrameBytes）：默认 4KB 太碎——大 prompt
// 会被切成上千块。64KB 是"通用帧"与"内存"的平衡点。
const frameReadBufSize = 64 << 10

// decodeFrame 把**一行**字节解成入站帧（不含换行符）。
//
// 返回 error 的两种口径（读循环据此分流）：
//
//   - errBatchUnsupported：顶层是 JSON 数组（批量）——本实现不支持批量（-32600）；
//   - 其它 error：不是合法 JSON 对象（顶层是**标量**（数字/字符串/布尔）或语法错）——
//     调用方按 -32700 处理。
//
// **顶层 `null` 例外**：JSON 的 null 解进 map 不报错（键集为 nil）→ 本函数**不返错误**，
// 而是交出一个"空帧"（无 method / 无 id / 无 result / 无 error）→ 端点按
// isResponse/isRequest/isNotification 都判不出 → 回 **-32600**（Invalid Request：
// 既非请求/通知也非响应），见 endpoint.dispatch 的 default 分支。
//
// 形状合法但缺 method 也缺 result/error 的，同上：由端点判不出后回 -32600。
//
// 键在位标记（hasID/hasResult/hasError）经一次 map 解出：`"result":null` 这类"带了键但
// 值为 null"要靠键在位识别（指针解会把值 nil 化，见 inbound 的说明）。
//
// 宽容（D7）：行首尾空白裁掉；**空行 / 全空白行跳过**（返回 nil, nil，调用方继续读
// 下一行——客户端偶发空行不该被当成坏帧）。
func decodeFrame(line []byte) (*inbound, error) {
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 {
		return nil, nil // 空行：跳过
	}
	if trimmed[0] == '[' {
		// 批量（JSON 数组）：本实现不支持——回 -32600（与本文件头注释口径一致）。
		return nil, errBatchUnsupported
	}
	// 先解成键集拿"键在位"（顶层非对象会在这里报错：标量一概拒）。
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &keys); err != nil {
		return nil, fmt.Errorf("acp: 帧不是合法 JSON 对象: %w", err)
	}
	var m inbound
	if err := json.Unmarshal(trimmed, &m); err != nil {
		return nil, fmt.Errorf("acp: 帧不是合法 JSON: %w", err)
	}
	_, m.hasID = keys["id"]
	_, m.hasResult = keys["result"]
	_, m.hasError = keys["error"]
	return &m, nil
}

// frameScanner 读帧的行读取器（newline-delimited；带单帧字节上限）。
//
// 为什么不用 bufio.Scanner：Scanner 的错误是**粘性**的——一旦 ErrTooLong（帧超限）
// 或底层读出错，之后每次 Scan 都直接返回 false 且 Err() 不变，调用方**无法**区分
// "这一帧坏了"与"整个流死了"。F1 的事故就是这么来的：超长帧 → Scanner 终态错 →
// 读循环当"可恢复坏帧"无限重试 → 8 秒吐 118 万条错误帧、进程永不退出。
//
// 为什么不用 json.Decoder：它按 JSON 值流式读，**不保证**"一行一帧"（一条消息被拆成
// 多行 / 多条挤在一行都能解），与"newline-delimited"的边界约定不符。
//
// 本实现（bufio.Reader 自实现行读取）把三种结果分得干干净净：
//
//	可恢复：单帧解码错（坏 JSON / 批量数组）/ 超限帧（该行已跳到行尾）——回错误后继续读；
//	终态：底层读错误（*terminalReadError）——回**一条**错误帧后结束连接；
//	正常收场：EOF（(nil, nil)）。
type frameScanner struct {
	r *bufio.Reader
}

func newFrameScanner(r *bufio.Reader) *frameScanner {
	return &frameScanner{r: r}
}

// next 读下一行并解成入站帧。
//
//	(nil, nil)    → 流结束（EOF：客户端断开）
//	(nil, err)    → 坏帧（坏 JSON / 批量数组 / 超限帧）：**可恢复**——调用方回错误后继续 next
//	               底层读错误则包成 *terminalReadError：**终态**——调用方回一条错误帧后结束连接
//	(frame, nil)  → 一帧
func (fs *frameScanner) next() (*inbound, error) {
	for {
		line, err := fs.readLine()
		if errors.Is(err, io.EOF) {
			return nil, nil // EOF：调用方以 (nil, nil) 感知流结束
		}
		if err != nil {
			return nil, err
		}
		f, derr := decodeFrame(line)
		if derr != nil {
			return nil, derr
		}
		if f == nil {
			continue // 空行：跳过，读下一行
		}
		return f, nil
	}
}

// readLine 读**一行**（不含换行符）。
//
//	(line, nil)             → 一行
//	(nil, io.EOF)           → 流结束（**必须与空行区分**：空行要继续读下一行，
//	                           EOF 要收场——混同就是"EOF 后无限空转"）
//	(nil, errFrameTooLong)  → 本行超上限（已跳到行尾丢弃；连接可继续）
//	(nil, *terminalReadError) → 底层读错误（终态：连接必须结束）
//
// 超限行的处理是"缓冲到上限即丢弃该行剩余（内存有界）"：超限后不再累积（剩下的内容
// 直接丢），也不停在行首（否则后续帧会被当成超限行的尾巴继续丢）。
func (fs *frameScanner) readLine() ([]byte, error) {
	var acc []byte
	tooLong := false
	for {
		chunk, err := fs.r.ReadSlice('\n')
		if len(chunk) > 0 && !tooLong {
			// 额度只算**帧体**字节：行分隔符（"\n"，CRLF 客户端还有 "\r"）**不算**进额度。
			//
			// 为什么（off-by-one）：额度本意是"一帧最多 8MiB"，而分隔符不是帧的一部分——
			// 把它算进去的话，**恰好 8MiB 的帧**会因多一个 "\n" 被误拒（客户端发了个
			// 合法的大帧，却被回 -32600）。
			body := len(chunk)
			if body > 0 && chunk[body-1] == '\n' {
				body-- // 行分隔符
				if body > 0 && chunk[body-1] == '\r' {
					body-- // CRLF 的 "\r"
				}
			}
			if len(acc)+body > maxFrameBytes {
				// 超限：停止累积（后面的内容直接丢）。
				tooLong = true
				acc = nil
			} else {
				acc = append(acc, chunk...)
			}
		}
		switch {
		case err == nil:
			// 读到换行：一行读完。
			if tooLong {
				return nil, errFrameTooLong
			}
			return trimEOL(acc), nil
		case errors.Is(err, bufio.ErrBufferFull):
			// 行比**读缓冲**还长（≠ 超上限）：继续读下一块并累积
			//（累积到 maxFrameBytes 由上面的长度检查收口——内存有界）。
			continue
		case errors.Is(err, io.EOF):
			if len(acc) > 0 {
				// 末行没有换行（宽容：仍按一行处理）。
				return trimEOL(acc), nil
			}
			if tooLong {
				return nil, errFrameTooLong
			}
			return nil, io.EOF // 流结束
		default:
			// 底层读错误：**终态**（不是"这一帧坏了"）。
			return nil, &terminalReadError{err: err}
		}
	}
}

// trimEOL 去掉行尾的换行符（以及它前面的回车——容忍 CRLF 客户端）。
func trimEOL(line []byte) []byte {
	line = bytes.TrimSuffix(line, []byte("\n"))
	line = bytes.TrimSuffix(line, []byte("\r"))
	return line
}
