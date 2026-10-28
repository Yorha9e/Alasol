package loop_test

// v1-S1：对话入事件流（F1）的测试——user.prompt / model.turn 作为旁路观测落库，
// 且回放 + fold 能从事件流重建完整对话历史（prompt / model / tool 事件全在）。

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/runtime"
)

// stepWithUsage 造一个带 usage 的脚本步骤（mockProvider 的脚本默认不带 usage）。
func stepWithUsage(text string, calls []core.ToolCallReq, in, out int64) scriptStep {
	return scriptStep{result: core.ModelResult{
		Text:      text,
		ToolCalls: calls,
		Usage:     core.Usage{InputTokens: in, OutputTokens: out},
	}}
}

// TestConversationEntersEventStream：Run 落一条 user.prompt（原文）、每轮落一条 model.turn
// （含 usage）；都是 durable；Result.Events **不含**它们（旁路观测，口径同 checkpoint.created）。
func TestConversationEntersEventStream(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(
		stepWithUsage("调用工具", []core.ToolCallReq{call("c1", "echo", `{"text":"hi"}`)}, 11, 2),
		stepWithUsage("完成", nil, 22, 5),
	)

	res, err := h.loop(nil).Run(context.Background(), "帮我 echo")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 旁路观测不计入 Result.Events：两轮 = 2×context.assembled + intent + completed = 4。
	if res.Events != 4 {
		t.Fatalf("Result.Events = %d，want 4（user.prompt/model.turn 不计入）", res.Events)
	}

	var (
		prompts []contracts.UserPromptPayload
		turns   []contracts.ModelTurnPayload
		ctxN    int
	)
	for _, ev := range h.events() {
		if !ev.Durable {
			t.Fatalf("对话入流事件 %s 必须 durable（F1）", ev.Kind)
		}
		switch ev.Kind {
		case loop.KindUserPrompt:
			var p contracts.UserPromptPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Fatalf("user.prompt payload: %v", err)
			}
			prompts = append(prompts, p)
		case loop.KindModelTurn:
			var p contracts.ModelTurnPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Fatalf("model.turn payload: %v", err)
			}
			turns = append(turns, p)
		case loop.KindContextAssembled:
			ctxN++
		}
	}

	// 恰好一条 user.prompt，原文 = prompt。
	if len(prompts) != 1 || prompts[0].Text != "帮我 echo" {
		t.Fatalf("user.prompt = %+v，want 一条 text=帮我 echo", prompts)
	}
	// 每轮一条 model.turn（2 轮 → 2 条），各带 usage。
	if len(turns) != 2 {
		t.Fatalf("model.turn = %d 条，want 2（每轮一条）", len(turns))
	}
	if turns[0].Usage == nil || turns[0].Usage.InputTokens != 11 || turns[0].Usage.OutputTokens != 2 {
		t.Fatalf("turn1 usage = %+v，want {11,2}", turns[0].Usage)
	}
	if turns[1].Text != "完成" || turns[1].Usage == nil || turns[1].Usage.InputTokens != 22 || turns[1].Usage.OutputTokens != 5 {
		t.Fatalf("turn2 = %+v，want text=完成 usage={22,5}", turns[1])
	}
	if ctxN != 2 {
		t.Fatalf("context.assembled = %d，want 2（观测事件仍在）", ctxN)
	}
	// user.prompt 是首条事件（Run 开始落），先于一切。
	if got := h.events()[0].Kind; got != loop.KindUserPrompt {
		t.Fatalf("首条事件 kind = %q，want user.prompt", got)
	}
}

// TestReplayFoldsFullConversation：端到端——真 loop 跑一场含工具调用的会话，从 store 回放
// 事实流，FoldConversation 重建出完整对话（user / assistant+工具调用 / tool / assistant）。
func TestReplayFoldsFullConversation(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(
		stepWithUsage("调用工具", []core.ToolCallReq{call("c1", "echo", `{"text":"hi"}`)}, 11, 2),
		stepWithUsage("完成", nil, 22, 5),
	)
	if _, err := h.loop(nil).Run(context.Background(), "帮我 echo"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	events, err := runtime.ReplaySession(context.Background(), h.store, h.session)
	if err != nil {
		t.Fatalf("ReplaySession: %v", err)
	}
	msgs := runtime.FoldConversation(events)

	// 期望复原 loop 写进历史的结构：
	//   [user "帮我 echo", assistant "调用工具"{c1}, tool "mock:echo", assistant "完成"]
	if len(msgs) != 4 {
		t.Fatalf("fold 出 %d 条消息，want 4: %+v", len(msgs), msgs)
	}
	if msgs[0].Role != "user" || msgs[0].Content != "帮我 echo" {
		t.Fatalf("msgs[0] = %+v，want user 帮我 echo", msgs[0])
	}
	if msgs[1].Role != "assistant" || msgs[1].Content != "调用工具" || len(msgs[1].ToolCalls) != 1 {
		t.Fatalf("msgs[1] = %+v，want assistant 带 1 个工具调用", msgs[1])
	}
	tc := msgs[1].ToolCalls[0]
	if string(tc.ID) != "c1" || tc.Name != "echo" || string(tc.Args) != `{"text":"hi"}` {
		t.Fatalf("复原的工具调用 = %+v，want c1/echo/{\"text\":\"hi\"}（provider id 原样，S4）", tc)
	}
	if msgs[2].Role != "tool" || msgs[2].Content != "mock:echo" || string(msgs[2].ToolCallID) != "c1" {
		t.Fatalf("msgs[2] = %+v，want tool mock:echo (c1)", msgs[2])
	}
	if msgs[3].Role != "assistant" || msgs[3].Content != "完成" || len(msgs[3].ToolCalls) != 0 {
		t.Fatalf("msgs[3] = %+v，want assistant 完成", msgs[3])
	}
}
