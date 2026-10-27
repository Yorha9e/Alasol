package transport

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// 消息 kind（契约 transport.json 的开放字符串；这里固化 v0 用到的几个）。
const (
	// KindSubscribe 客户端请求订阅事件流（payload = contracts.SubscribeRequest）。
	KindSubscribe = "subscribe"
	// KindEvent 服务端推送一条事件（payload = contracts.Event）。
	KindEvent = "event"
	// KindPing 客户端心跳请求。
	KindPing = "ping"
	// KindPong 服务端心跳应答（reply_to = 对应 ping 的消息 id）。
	KindPong = "pong"
	// KindError 协议层错误（payload = contracts.TransportError）。
	KindError = "error"
)

// TransportError.Code 的取值（开放字符串；v0 固化这几个）。
const (
	// CodeBadJSON 一行不是合法 JSON（或不是对象）。
	CodeBadJSON = "bad_json"
	// CodeBadPayload 消息合法但载荷解不开（如 subscribe 的 payload 不是对象）。
	CodeBadPayload = "bad_payload"
	// CodeUnknownKind 不认识的 kind（含客户端擅自发 kind=event —— 事件只能由服务端发）。
	CodeUnknownKind = "unknown_kind"
	// CodeForbiddenKind 客户端发了只有服务端才能发的 kind（如 event）。
	CodeForbiddenKind = "forbidden_kind"
	// CodeOriginRejected Origin 校验失败（N5b：跨源页面拒绝）。
	CodeOriginRejected = "origin_rejected"
	// CodeServerNotReady 服务端未装配好（编程错误：bus 缺失）。
	CodeServerNotReady = "server_not_ready"
	// CodeSubscriptionLagging 订阅通道满、有事件被丢弃（告知客户端按 seq 水位补读）。
	CodeSubscriptionLagging = "subscription_lagging"
)

// ContractVersion 是 TransportMessage.V 的取值（F7：契约版本，只增不删不改义）。
const ContractVersion = 1

// maxLineBytes 是单条 NDJSON 行的字节上限（防一条垃圾消息把内存吃满）。
// context.assembled 的载荷可能不小（parts 列表），8 MiB 对 v0 足够宽松。
const maxLineBytes = 8 << 20

// NewMessage 组装一条 TransportMessage（id 用 core.NewUlid()——框架 id 的唯一生成处）。
//
// payload 为 nil 时消息不带 payload 字段（契约里 payload 可选）。
func NewMessage(kind string, payload any) (contracts.TransportMessage, error) {
	m := contracts.TransportMessage{
		V:    ContractVersion,
		ID:   core.NewUlid(),
		Kind: kind,
	}
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return contracts.TransportMessage{}, fmt.Errorf("transport: 序列化 %s 载荷失败: %w", kind, err)
		}
		m.Payload = raw
	}
	return m, nil
}

// NewErrorMessage 组装一条 kind=error 消息（reply_to 指向出错的那条消息，可为空）。
func NewErrorMessage(replyTo *contracts.Ulid, code, message string) (contracts.TransportMessage, error) {
	m, err := NewMessage(KindError, contracts.TransportError{Code: code, Message: message})
	if err != nil {
		return contracts.TransportMessage{}, err
	}
	m.ReplyTo = replyTo
	return m, nil
}

// encodeLine 把一条消息序列化成一行 NDJSON（json.Marshal 本身不产生换行，
// 这里显式补 "\n" 让"一行一消息"对任何行式消费者都成立：WS 消息边界 + 换行符双framing）。
func encodeLine(m contracts.TransportMessage) ([]byte, error) {
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("transport: 序列化消息失败: %w", err)
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("transport: 消息序列化结果不是合法 JSON: %s", raw)
	}
	return append(raw, '\n'), nil
}

// decodeLines 把一个入站消息体解成若干条 TransportMessage。
//
// 宽容两点（D7）：
//   - 一个 WS 消息里可以带多行（客户端攒了一批一起发）→ 逐行解；
//   - 空行 / 只有空白的行跳过。
//
// 任一行解不开 → 返回 error（调用方回一条 CodeBadJSON，然后继续读后续消息：
// 一条坏消息不该把连接打死）。
func decodeLines(data []byte) ([]contracts.TransportMessage, error) {
	var out []contracts.TransportMessage
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 4096), maxLineBytes)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var m contracts.TransportMessage
		if err := json.Unmarshal(line, &m); err != nil {
			return nil, fmt.Errorf("transport: 行不是合法 TransportMessage: %w", err)
		}
		if strings.TrimSpace(m.Kind) == "" {
			return nil, fmt.Errorf("transport: 消息缺 kind")
		}
		out = append(out, m)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("transport: 读入站消息失败: %w", err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("transport: 空消息（没有任何一行）")
	}
	return out, nil
}

// decodePayload 把消息载荷解成目标类型（nil / 空载荷 → 保持零值，不报错）。
func decodePayload(m contracts.TransportMessage, out any) error {
	if len(m.Payload) == 0 {
		return nil
	}
	if err := json.Unmarshal(m.Payload, out); err != nil {
		return fmt.Errorf("transport: kind=%s 的载荷解不开: %w", m.Kind, err)
	}
	return nil
}
