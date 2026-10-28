package runtime_test

// v1-S1：FoldConversation 的单元测试——从 user.prompt / model.turn / tool.call.* 三类
// 事件 fold 出完整对话历史（F1：事件流是唯一事实源）。

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

func textPtr(s string) *string { return &s }

func mustPayload(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return string(b)
}

// TestFoldConversation：一场含工具成功 + 工具失败、跨两轮 prompt 的会话，fold 出完整对话。
func TestFoldConversation(t *testing.T) {
	events := []contracts.Event{
		durable(core.NewUlid(), "user.prompt", mustPayload(t, contracts.UserPromptPayload{Text: "Q1"})),
		durable(core.NewUlid(), "model.turn", mustPayload(t, contracts.ModelTurnPayload{
			Text: "A1", Usage: &contracts.ModelTurnUsage{InputTokens: 3, OutputTokens: 4},
		})),
		durable(core.NewUlid(), "tool.call.intent", mustPayload(t, contracts.ToolCallIntentPayload{
			ToolCallID: "tc1", ToolName: "read", Arguments: json.RawMessage(`{"path":"a"}`),
			Idempotent: true, Replay: contracts.ReplayKindSafe,
		})),
		durable(core.NewUlid(), "tool.call.completed", mustPayload(t, contracts.ToolCallCompletedPayload{
			ToolCallID: "tc1",
			Result:     contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: textPtr("file-a")}}},
		})),
		durable(core.NewUlid(), "model.turn", mustPayload(t, contracts.ModelTurnPayload{Text: "A2"})),
		durable(core.NewUlid(), "user.prompt", mustPayload(t, contracts.UserPromptPayload{Text: "Q2"})),
		durable(core.NewUlid(), "model.turn", mustPayload(t, contracts.ModelTurnPayload{Text: "A3"})),
		durable(core.NewUlid(), "tool.call.intent", mustPayload(t, contracts.ToolCallIntentPayload{
			ToolCallID: "tc2", ToolName: "write", Arguments: json.RawMessage(`{}`), Replay: contracts.ReplayKindUnsafe,
		})),
		durable(core.NewUlid(), "tool.call.failed", mustPayload(t, contracts.ToolCallFailedPayload{
			ToolCallID: "tc2", Error: contracts.ToolCallError{Message: "denied by policy"},
		})),
	}

	msgs := runtime.FoldConversation(events)

	want := []core.Message{
		{Role: "user", Content: "Q1"},
		{Role: "assistant", Content: "A1", ToolCalls: []core.ToolCallReq{
			{ID: "tc1", Name: "read", Args: json.RawMessage(`{"path":"a"}`)},
		}},
		{Role: "tool", Content: "file-a", ToolCallID: "tc1"},
		{Role: "assistant", Content: "A2"},
		{Role: "user", Content: "Q2"},
		{Role: "assistant", Content: "A3", ToolCalls: []core.ToolCallReq{
			{ID: "tc2", Name: "write", Args: json.RawMessage(`{}`)},
		}},
		{Role: "tool", Content: "denied by policy", ToolCallID: "tc2"},
	}
	if !reflect.DeepEqual(msgs, want) {
		t.Fatalf("FoldConversation =\n%+v\nwant\n%+v", msgs, want)
	}
}

// TestFoldConversationMultiToolCallInOneTurn：一个 model.turn 后的多个 intent/completed
// 按序复原为该 assistant 消息的多次工具调用（X1 写路径序：intent→completed 逐个）。
func TestFoldConversationMultiToolCallInOneTurn(t *testing.T) {
	events := []contracts.Event{
		durable(core.NewUlid(), "user.prompt", mustPayload(t, contracts.UserPromptPayload{Text: "go"})),
		durable(core.NewUlid(), "model.turn", mustPayload(t, contracts.ModelTurnPayload{Text: ""})),
		durable(core.NewUlid(), "tool.call.intent", mustPayload(t, contracts.ToolCallIntentPayload{
			ToolCallID: "c1", ToolName: "read", Arguments: json.RawMessage(`{}`), Replay: contracts.ReplayKindSafe,
		})),
		durable(core.NewUlid(), "tool.call.completed", mustPayload(t, contracts.ToolCallCompletedPayload{
			ToolCallID: "c1", Result: contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: textPtr("r1")}}},
		})),
		durable(core.NewUlid(), "tool.call.intent", mustPayload(t, contracts.ToolCallIntentPayload{
			ToolCallID: "c2", ToolName: "read", Arguments: json.RawMessage(`{}`), Replay: contracts.ReplayKindSafe,
		})),
		durable(core.NewUlid(), "tool.call.completed", mustPayload(t, contracts.ToolCallCompletedPayload{
			ToolCallID: "c2", Result: contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: textPtr("r2")}}},
		})),
	}
	msgs := runtime.FoldConversation(events)
	if len(msgs) != 4 {
		t.Fatalf("fold 出 %d 条，want 4（user + assistant + 2 tool）: %+v", len(msgs), msgs)
	}
	asst := msgs[1]
	if asst.Role != "assistant" || len(asst.ToolCalls) != 2 {
		t.Fatalf("assistant 应挂 2 个工具调用: %+v", asst)
	}
	if string(asst.ToolCalls[0].ID) != "c1" || string(asst.ToolCalls[1].ID) != "c2" {
		t.Fatalf("工具调用顺序错: %+v", asst.ToolCalls)
	}
	if msgs[2].Content != "r1" || string(msgs[2].ToolCallID) != "c1" {
		t.Fatalf("msgs[2] = %+v，want r1/c1", msgs[2])
	}
	if msgs[3].Content != "r2" || string(msgs[3].ToolCallID) != "c2" {
		t.Fatalf("msgs[3] = %+v，want r2/c2", msgs[3])
	}
}

// TestFoldConversationToleranceAndEmpty：空流 → 空对话；坏载荷的已知 kind 被跳过（D7 宽容），
// 不影响其余事件。
func TestFoldConversationToleranceAndEmpty(t *testing.T) {
	if msgs := runtime.FoldConversation(nil); len(msgs) != 0 {
		t.Fatalf("空事件流应 fold 出空对话，得到 %+v", msgs)
	}
	events := []contracts.Event{
		durable(core.NewUlid(), "user.prompt", `{坏 JSON`), // 坏载荷 → 跳过
		durable(core.NewUlid(), "user.prompt", mustPayload(t, contracts.UserPromptPayload{Text: "ok"})),
		durable(core.NewUlid(), "model.turn", `也坏`),                                                                     // 坏载荷 → 跳过
		durable(core.NewUlid(), "ledger.updated", mustPayload(t, contracts.LedgerUpdatedPayload{Goals: []string{"g"}})), // 非对话 kind → 忽略
	}
	msgs := runtime.FoldConversation(events)
	if len(msgs) != 1 || msgs[0].Role != "user" || msgs[0].Content != "ok" {
		t.Fatalf("坏载荷应被跳过、只留合法 user: %+v", msgs)
	}
}

// TestFoldConversationFromStoredSession：走真 SQLite store 落一场会话，回放 + fold（验收①的落盘形态）。
func TestFoldConversationFromStoredSession(t *testing.T) {
	st, err := sqlite.Open(t.TempDir()+"/conv.db", sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	ctx := context.Background()

	want := []contracts.Event{
		durable(core.NewUlid(), "user.prompt", mustPayload(t, contracts.UserPromptPayload{Text: "读文件"})),
		durable(core.NewUlid(), "model.turn", mustPayload(t, contracts.ModelTurnPayload{Text: "好的"})),
		durable(core.NewUlid(), "tool.call.intent", mustPayload(t, contracts.ToolCallIntentPayload{
			ToolCallID: "t1", ToolName: "read", Arguments: json.RawMessage(`{"path":"x"}`), Replay: contracts.ReplayKindVerify,
		})),
		durable(core.NewUlid(), "tool.call.completed", mustPayload(t, contracts.ToolCallCompletedPayload{
			ToolCallID: "t1", Result: contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: textPtr("内容")}}},
		})),
	}
	if _, err := st.Append(ctx, want); err != nil {
		t.Fatalf("append: %v", err)
	}

	events, err := runtime.ReplaySession(ctx, st, replaySessionID())
	if err != nil {
		t.Fatalf("ReplaySession: %v", err)
	}
	msgs := runtime.FoldConversation(events)
	if len(msgs) != 3 {
		t.Fatalf("fold 出 %d 条，want 3: %+v", len(msgs), msgs)
	}
	if msgs[0].Content != "读文件" || msgs[1].Content != "好的" || msgs[2].Content != "内容" {
		t.Fatalf("fold 内容不符: %+v", msgs)
	}
	if string(msgs[2].ToolCallID) != "t1" || len(msgs[1].ToolCalls) != 1 {
		t.Fatalf("tool 调用/结果配对错: %+v", msgs)
	}
}
