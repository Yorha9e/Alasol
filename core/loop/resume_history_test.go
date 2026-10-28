package loop_test

// v1-S1 收尾：resume 续跑的**最小闭环**——
//	跑一段对话 → 回放事件流 fold 出历史 → 带历史续跑 → 新一轮的请求里带着此前上下文。
//
// 为什么要有这个闭环：对话内容进了事件流（F1）就能被回放重建，但"重建出来的历史"
// 必须能**喂回 loop** 才叫续跑——否则 FoldConversation 只是素材，不是能力。

import (
	"context"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime"
)

// TestRunWithHistoryContinuesFoldedConversation：跑一段 → 回放 fold 出历史 → 带历史续跑 →
// 新一轮的模型请求里带着此前的上下文（user 问题 / assistant 文本 / tool 结果）。
func TestRunWithHistoryContinuesFoldedConversation(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)

	// ① 跑一段：模型发起一次工具调用后收敛。
	h.script(
		stepTools(call("c1", "echo", `{"text":"hi"}`)),
		stepText("第一段收尾"),
	)
	if _, err := h.loop(nil).Run(context.Background(), "第一个问题"); err != nil {
		t.Fatalf("首段 Run: %v", err)
	}

	// ② 回放事件流 → fold 出对话历史（事件流是唯一事实源）。
	history := runtime.FoldConversation(h.events())
	if len(history) == 0 {
		t.Fatal("回放 fold 不出历史")
	}
	// 历史含三种角色：user / assistant / tool。
	var roles []string
	for _, m := range history {
		roles = append(roles, m.Role)
	}
	if len(roles) < 3 || roles[0] != "user" {
		t.Fatalf("fold 出的历史形状不对（roles=%v）", roles)
	}
	if history[0].Content != "第一个问题" {
		t.Fatalf("历史首条应是此前的 user 问题：%q", history[0].Content)
	}

	// ③ 带历史续跑。
	h.script(stepText("第二段收尾"))
	if _, err := h.loop(nil).RunWithHistory(context.Background(), history, "第二个问题"); err != nil {
		t.Fatalf("续跑 RunWithHistory: %v", err)
	}

	// 新一轮的请求 = 既有历史 + 本次 prompt。
	reqs := h.provider.reqs()
	if len(reqs) == 0 {
		t.Fatal("续跑没有模型请求")
	}
	msgs := reqs[len(reqs)-1].Messages
	if len(msgs) != len(history)+1 {
		t.Fatalf("续跑请求的历史条数 = %d，want %d（既有历史 + 本次 prompt）", len(msgs), len(history)+1)
	}
	for i, m := range history {
		if msgs[i].Role != m.Role || msgs[i].Content != m.Content {
			t.Fatalf("历史第 %d 条被改写：%+v，want %+v", i, msgs[i], m)
		}
	}
	last := msgs[len(msgs)-1]
	if last.Role != "user" || last.Content != "第二个问题" {
		t.Fatalf("续跑请求末条应是本次 prompt：%+v", last)
	}
	// 此前那一轮的工具结果也在上下文里（模型看得见）。
	var sawTool bool
	for _, m := range msgs {
		if m.Role == "tool" {
			sawTool = true
		}
	}
	if !sawTool {
		t.Fatalf("续跑请求应含此前的 tool 结果（上下文要完整）：%+v", msgs)
	}
}

// TestRunWithHistoryDoesNotMutateCallerHistory：调用方传进来的历史不被本次 Run 改写
// （续跑方可能复用同一个切片）。
func TestRunWithHistoryDoesNotMutateCallerHistory(t *testing.T) {
	h := newHarness(t)
	history := []core.Message{{Role: "user", Content: "旧问题"}}
	before := len(history)

	h.script(stepText("答"))
	if _, err := h.loop(nil).RunWithHistory(context.Background(), history, "新问题"); err != nil {
		t.Fatalf("RunWithHistory: %v", err)
	}
	if len(history) != before {
		t.Fatalf("调用方的历史被改写：%d → %d 条", before, len(history))
	}
	if history[0].Content != "旧问题" {
		t.Fatalf("调用方的历史内容被改写：%q", history[0].Content)
	}
}

// TestFoldedConversationFeedsContinuationWithToolCalls：fold 出的历史里带工具调用时，
// 续跑的请求也要把工具调用原样带回（provider 黑盒 id 原样，S4）。
func TestFoldedConversationFeedsContinuationWithToolCalls(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)

	h.script(
		stepTools(call("c1", "echo", `{"text":"hi"}`)),
		stepText("收尾"),
	)
	if _, err := h.loop(nil).Run(context.Background(), "问题"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	history := runtime.FoldConversation(h.events())
	var assistant *core.Message
	for i := range history {
		if history[i].Role == "assistant" && len(history[i].ToolCalls) > 0 {
			assistant = &history[i]
		}
	}
	if assistant == nil {
		t.Fatalf("fold 出的历史应含带工具调用的 assistant 消息：%+v", history)
	}
	if string(assistant.ToolCalls[0].ID) != "c1" {
		t.Fatalf("工具调用 id 未原样保留：%q", assistant.ToolCalls[0].ID)
	}

	h.script(stepText("续"))
	if _, err := h.loop(nil).RunWithHistory(context.Background(), history, "继续"); err != nil {
		t.Fatalf("RunWithHistory: %v", err)
	}
	reqs := h.provider.reqs()
	msgs := reqs[len(reqs)-1].Messages
	var carried bool
	for _, m := range msgs {
		if m.Role == "assistant" && len(m.ToolCalls) > 0 && string(m.ToolCalls[0].ID) == "c1" {
			carried = true
		}
	}
	if !carried {
		t.Fatalf("续跑请求应带回此前的工具调用：%+v", msgs)
	}
}

// 编译期提醒：contracts 的事件类型在 fold 侧用到（防止 import 被误删）。
var _ = contracts.UserPromptPayload{}
