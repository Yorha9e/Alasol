package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// ReplaySession 按 seq 完整返回一场 session 的全部 durable 事件（验收①）。
//
// 回放对象只有事实事件（F8）：durable=false 的流事件从未入库，故这里读到的就是
// 该 session 的完整事实历史，按全局 seq 升序。上层据此重建/核验任意状态。
func ReplaySession(ctx context.Context, store core.Store, sessionID contracts.Ulid) ([]contracts.Event, error) {
	// fromSeq=1（首个合法 seq）、limit=0（不限）：拿全场。
	events, err := store.ReadFrom(ctx, &sessionID, 1, 0)
	if err != nil {
		return nil, fmt.Errorf("runtime: replay session %s: %w", sessionID, err)
	}
	return events, nil
}

// FoldLedger 从事件流 fold 出当前 goal ledger 态：取**最后一条** ledger.updated 的
// 全量快照（G23/G8：ledger 不是独立可变存储，当前态 = 对事件的投影，天然 LWW）。
//
// 传入的事件应按 seq 升序（如 ReplaySession 的返回值）。没有 ledger.updated 时
// 返回零值（四个 nil 切片）。
//
// **容错口径与索引器一致**（D7 宽容）：载荷不可解析的 ledger.updated 被**跳过**
// （不中断 fold、不影响其余事件），与 indexer 对坏载荷的"隔离而非致命"处理同源
// （索引器另计一次 cost_stats["indexer.bad_payload"]）。
func FoldLedger(events []contracts.Event) contracts.LedgerUpdatedPayload {
	var out contracts.LedgerUpdatedPayload
	for i := range events {
		if events[i].Kind != kindLedgerUpdated {
			continue
		}
		var p contracts.LedgerUpdatedPayload
		if err := json.Unmarshal(events[i].Payload, &p); err != nil {
			continue // 宽容（D7）：跳过无法解析的 ledger 事件，不影响其余 fold。
		}
		out = p // 后写覆盖先写 = LWW
	}
	return out
}

// kindLedgerUpdated 与 indexer 保持一致；这里 fold 只关心这一种 kind。
const kindLedgerUpdated = "ledger.updated"

// 对话入流的 kind（v1-S1，F1）：与 core/loop 落的 kind 一致，此处 fold 重建对话历史。
// 用字符串字面量而非 import core/loop——runtime 不依赖 loop 包，fold 只关心这几种 kind。
const (
	kindUserPrompt        = "user.prompt"
	kindModelTurn         = "model.turn"
	kindToolCallIntent    = "tool.call.intent"
	kindToolCallCompleted = "tool.call.completed"
	kindToolCallFailed    = "tool.call.failed"
)

// FoldConversation 从事件流 fold 出对话历史（[]core.Message，F1：事件流是唯一事实源）。
//
// v1-S1 起，user.prompt / model.turn / tool.call.* 三类事件共同构成完整对话；本函数把它们
// 拍成 provider 无关的对话历史，**可直接喂给 Loop 续跑**（resume 的对话重建）。传入事件应按
// seq 升序（如 ReplaySession 的返回值）。重建规则与 core/loop.Run 写历史的顺序对应：
//
//	user.prompt         → Message{Role:"user",    Content:text}
//	model.turn          → Message{Role:"assistant",Content:text}   （工具调用挂在它身上）
//	tool.call.intent    → 作为 ToolCallReq 追加到**最近一条** assistant 的 ToolCalls
//	                      （一个 model.turn 后的多个 intent 按序复原该轮多次调用）
//	tool.call.completed → Message{Role:"tool", Content:结果文本, ToolCallID}
//	tool.call.failed    → Message{Role:"tool", Content:错误说明, ToolCallID}
//
// 容错（D7 宽容）：载荷不可解析的已知 kind 被跳过，不中断 fold（同 FoldLedger）。
// 多 agent 场景下同一 session 混有多个 agent_id 的对话——调用方应按 agent_id 预过滤
// （事件本身带 agent_id），本函数只按给定顺序 fold，不替调用方拆分视角。
func FoldConversation(events []contracts.Event) []core.Message {
	var msgs []core.Message
	lastAssistant := -1 // 最近一条 assistant 消息的下标（-1 = 尚无；工具调用挂它身上）
	for i := range events {
		ev := &events[i]
		switch ev.Kind {
		case kindUserPrompt:
			var p contracts.UserPromptPayload
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue
			}
			msgs = append(msgs, core.Message{Role: "user", Content: p.Text})
			lastAssistant = -1
		case kindModelTurn:
			var p contracts.ModelTurnPayload
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue
			}
			msgs = append(msgs, core.Message{Role: "assistant", Content: p.Text})
			lastAssistant = len(msgs) - 1
		case kindToolCallIntent:
			var p contracts.ToolCallIntentPayload
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue
			}
			if lastAssistant >= 0 {
				m := &msgs[lastAssistant]
				m.ToolCalls = append(m.ToolCalls, core.ToolCallReq{
					ID:   p.ToolCallID, // provider 黑盒 id 原样（S4）
					Name: p.ToolName,
					Args: p.Arguments,
				})
			}
		case kindToolCallCompleted:
			var p contracts.ToolCallCompletedPayload
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue
			}
			msgs = append(msgs, core.Message{
				Role:       "tool",
				Content:    toolResultText(p.Result),
				ToolCallID: p.ToolCallID,
			})
		case kindToolCallFailed:
			var p contracts.ToolCallFailedPayload
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue
			}
			msgs = append(msgs, core.Message{
				Role:       "tool",
				Content:    p.Error.Message,
				ToolCallID: p.ToolCallID,
			})
		}
	}
	return msgs
}

// toolResultText 把 ToolResult 拍成文本（text 块拼接、结构化块原样带上）——与 core/loop 回填
// 给模型的 toolResultText 同口径，使 fold 出的 tool 消息内容与当初写进历史的一致。此处复刻
// 而非 import core/loop：runtime 不依赖 loop 包，且这是个定义良好的纯变换。
func toolResultText(res contracts.ToolResult) string {
	var parts []string
	for _, p := range res.Content {
		switch {
		case p.Text != nil && *p.Text != "":
			parts = append(parts, *p.Text)
		case len(p.Data) > 0:
			parts = append(parts, string(p.Data))
		}
	}
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	default:
		out := parts[0]
		for _, p := range parts[1:] {
			out += "\n" + p
		}
		return out
	}
}
