package acp

import (
	"encoding/json"
	"strings"
	"sync"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/loop"
)

// 本文件是 session/update 通知的**数据源桥**（v4-2-B 要求 3）：把 hub 事件流（bus 订阅）
// 的增量映射成 ACP 的 session/update 通知。
//
// # 映射表（doc 口径，逐条对应）
//
//	model.delta (text)      → agent_message_chunk   （模型正文增量，G13 流式）
//	model.delta (reasoning) → agent_thought_chunk   （思维链增量，G14；有就发）
//	tool.call.intent        → tool_call（status=pending，初报）
//	tool.call.completed     → tool_call_update（status=completed + 结果文本）
//	tool.call.failed        → tool_call_update（status=failed + 错误文本）
//	user.prompt             → **不发**（用户自己说的，回显是噪声）
//	model.turn / 其它       → **不发**（model.turn 是整段事实，delta 已覆盖增量；
//	                           再发一遍会与 delta 重复）
//
// # 为什么接 bus 而不是轮询 store
//
// model.delta 是 durable=false 的流事件（F8/陷阱 11）——**不入库**，只能从 bus 订阅拿到。
// 轮询 store 永远拿不到打字机增量。故端点订阅 bus；订阅不可用（backend 返 nil）时降级为
// "整轮结束发一整段 agent_message_chunk"（见 endpoint.runTurn 的降级分支与 doc.go）。
//
// tool_call_id 关联：intent/completed/failed 共用 provider 的 tool_call_id（黑盒，S4），
// 原样作 ACP 的 toolCallId——客户端据此把初报与后续 update 归到同一次调用。

// contentBlock 是 ACP 的内容块（v1 只用 text 型）。
type contentBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// sessionUpdate 是 session/update 通知里 update 字段的** tagged union**（sessionUpdate
// 判别）。Content 用 any：chunk 的 content 是**单个** block，tool_call 的 content 是
// **block 数组**（ACP 两种形状不同，用 any 让 json.Marshal 各按其值产出）。
type sessionUpdate struct {
	SessionUpdate string          `json:"sessionUpdate"`
	ToolCallID    string          `json:"toolCallId,omitempty"`
	Title         string          `json:"title,omitempty"`
	Kind          string          `json:"kind,omitempty"`
	Status        string          `json:"status,omitempty"`
	RawInput      json.RawMessage `json:"rawInput,omitempty"`
	Content       any             `json:"content,omitempty"`

	// turn 是这条增量所属的**轮次**（contracts.ModelDeltaPayload.Turn；每次 Run 内从 1 递增）。
	//
	// **不外发**（非导出字段 → 不进 JSON）：它只给桥接层做"按轮归组"用——最终文本对账
	// 必须**按轮**比（core/loop 的 lastText 语义 = **最后一轮**的文本；而增量是**所有轮**
	// 的拼接：混在一起比会判"分叉" → 整段重发 → 客户端收到两份最终答复）。
	turn int64
}

// updateParams 是 session/update 通知的 params。
type updateParams struct {
	SessionID string        `json:"sessionId"`
	Update    sessionUpdate `json:"update"`
}

func messageChunkUpdate(text string) sessionUpdate {
	return sessionUpdate{SessionUpdate: "agent_message_chunk", Content: contentBlock{Type: "text", Text: text}}
}

func thoughtChunkUpdate(text string) sessionUpdate {
	return sessionUpdate{SessionUpdate: "agent_thought_chunk", Content: contentBlock{Type: "text", Text: text}}
}

func toolCallUpdateStarted(id, title string, rawInput json.RawMessage) sessionUpdate {
	return sessionUpdate{
		SessionUpdate: "tool_call",
		ToolCallID:    id,
		Title:         title,
		Kind:          title, // ACP 的 kind 是可选分类；这里借工具名占位（read/write/bash）
		Status:        "pending",
		RawInput:      rawInput,
	}
}

func toolCallUpdateCompleted(id, text string) sessionUpdate {
	return sessionUpdate{
		SessionUpdate: "tool_call_update",
		ToolCallID:    id,
		Status:        "completed",
		Content:       []contentBlock{{Type: "text", Text: text}},
	}
}

func toolCallUpdateFailed(id, text string) sessionUpdate {
	return sessionUpdate{
		SessionUpdate: "tool_call_update",
		ToolCallID:    id,
		Status:        "failed",
		Content:       []contentBlock{{Type: "text", Text: text}},
	}
}

// turnBridge 是一轮 prompt 的桥接状态（跨 bridge goroutine 与 runTurn 共享）。
type turnBridge struct {
	// mu 保护 byTurn / lastTurn（桥 goroutine 写、runTurn 读）。
	mu sync.Mutex
	// byTurn 是**按轮次**累计的正文增量（key = contracts.ModelDeltaPayload.Turn；
	// 每次 Run 内从 1 递增）。
	//
	// 为什么要按轮分桶：core/loop 的最终文本（Result.Text）是**最后一轮**的文本
	// （lastText 语义），而桥出去的增量是**所有轮**的拼接。第 1 轮"正文 + 工具调用"、
	// 第 2 轮"最终答复"时，两边直接比会判"分叉" → 整段重发 → 客户端收到两份最终答复。
	// 按轮分桶后，只拿**最后一轮**的增量与最终文本对账（N2）。
	byTurn map[int64]string
	// lastTurn 是已见过的**最大轮次**（= 最后一轮）。
	lastTurn int64
}

// appendSent 累计某一轮的正文增量（桥 goroutine 调）。
func (tb *turnBridge) appendSent(turn int64, text string) {
	if text == "" {
		return
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.byTurn == nil {
		tb.byTurn = make(map[int64]string)
	}
	tb.byTurn[turn] += text
	if turn > tb.lastTurn {
		tb.lastTurn = turn
	}
}

// sentText 取**最后一轮**的正文增量全文（runTurn 调；对账用）。
//
// 最后一轮没有正文增量（模型那轮只发工具调用 / provider 没流式）→ 空串 → 对账走
// "补发整段"的降级路径（以最终文本为准）。
func (tb *turnBridge) sentText() string {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.byTurn[tb.lastTurn]
}

// bridge 把订阅里的事件持续映射成 session/update 通知，直到订阅关闭（通道排干后退出）。
//
// 订阅关闭时机：runTurn 在 RunPrompt 返回后 sub.Close()——桥把已缓冲的增量排干，再看到
// 通道关闭退出（close done）。这保证所有 update 先于 session/prompt 响应送达客户端。
func (e *endpoint) bridge(sub *bus.Subscription, sid contracts.Ulid, tb *turnBridge, done chan struct{}) {
	defer close(done)
	for ev := range sub.Events() {
		for _, u := range mapEvent(ev) {
			if u.SessionUpdate == "agent_message_chunk" {
				// 累计已发正文（**按轮**分桶，对账用，见 turnBridge.byTurn）。
				tb.appendSent(u.turn, contentTextOf(u))
			}
			e.sendUpdate(sid, u)
		}
	}
	// 队列满被丢过（流事件丢了就是丢了——durable=false 不入库）：如实记一条 Warn。
	//
	// 为什么要记：丢了增量 → 最终文本对账会判"分叉" → 整段重发（客户端会看到重复），
	// 而"为什么重复"只有日志说得清（诚实边界：流事件不可补读）。
	if n := sub.Lagging(); n > 0 {
		e.log.Warn("acp: 事件订阅队列满，丢了增量通知（最终文本会以整段补发为准）",
			"session", string(sid), "lagging", n)
	}
}

// contentTextOf 取一条 update 里的正文文本（chunk 的 content 是单个 block）。
func contentTextOf(u sessionUpdate) string {
	switch c := u.Content.(type) {
	case contentBlock:
		return c.Text
	case *contentBlock:
		if c == nil {
			return ""
		}
		return c.Text
	default:
		return ""
	}
}

// sendUpdate 发一条 session/update 通知。
func (e *endpoint) sendUpdate(sid contracts.Ulid, u sessionUpdate) {
	if err := e.conn.sendNotification("session/update", updateParams{SessionID: string(sid), Update: u}); err != nil {
		e.log.Error("acp: 发 session/update 失败", "session", string(sid), "err", err.Error())
	}
}

// sendMessageChunk 发一整段 agent_message_chunk（降级路径 / 非流式兜底）。
func (e *endpoint) sendMessageChunk(sid contracts.Ulid, text string) {
	e.sendUpdate(sid, messageChunkUpdate(text))
}

// mapEvent 把一条 hub 事件映射成 0/1/2 条 sessionUpdate（见文件头的映射表）。
func mapEvent(ev contracts.Event) []sessionUpdate {
	switch ev.Kind {
	case loop.KindModelDelta:
		var p contracts.ModelDeltaPayload
		if json.Unmarshal(ev.Payload, &p) != nil {
			return nil
		}
		var out []sessionUpdate
		// 思维链优先发（"先想后说"的阅读序），再发正文增量。两者都空 = 无内容（发布端本
		// 就不该发，这里是纵深防御）。
		if p.Reasoning != nil && *p.Reasoning != "" {
			out = append(out, thoughtChunkUpdate(*p.Reasoning))
		}
		if p.Text != nil && *p.Text != "" {
			// 带上轮次（turn 不外发）：桥接层按轮归组正文增量，最终文本对账要用。
			u := messageChunkUpdate(*p.Text)
			u.turn = p.Turn
			out = append(out, u)
		}
		return out
	case loop.KindToolCallIntent:
		var p contracts.ToolCallIntentPayload
		if json.Unmarshal(ev.Payload, &p) != nil {
			return nil
		}
		// rawInput 脱敏（H18：工具参数可能含凭据，与 request_permission 同口径）。
		return []sessionUpdate{toolCallUpdateStarted(string(p.ToolCallID), p.ToolName, redactJSON(p.Arguments))}
	case loop.KindToolCallCompleted:
		var p contracts.ToolCallCompletedPayload
		if json.Unmarshal(ev.Payload, &p) != nil {
			return nil
		}
		return []sessionUpdate{toolCallUpdateCompleted(string(p.ToolCallID), toolResultText(p.Result))}
	case loop.KindToolCallFailed:
		var p contracts.ToolCallFailedPayload
		if json.Unmarshal(ev.Payload, &p) != nil {
			return nil
		}
		return []sessionUpdate{toolCallUpdateFailed(string(p.ToolCallID), p.Error.Message)}
	default:
		// user.prompt / model.turn / context.assembled / agent.* 等：不桥（见映射表）。
		return nil
	}
}

// toolResultText 把 ToolResult 的文本内容块拼成一串（供 tool_call_update 的 content）。
func toolResultText(tr contracts.ToolResult) string {
	var b strings.Builder
	for _, part := range tr.Content {
		if part.Type == "text" && part.Text != nil {
			b.WriteString(*part.Text)
		}
	}
	return b.String()
}
