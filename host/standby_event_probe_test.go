package host

// G-j 事件面的**早到窗口**（critic P1 的 PROBE 场景固化）。
//
// 场景（critic PROBE3）：第二轮 standby 等 `model.turn` → 2.6ms 就被**上一轮**的
// model.turn 唤醒（假阳性）；等 `tool.call.*` → 被上一轮的 completed 唤醒。
//
// 修法：事件面早到窗口 = (本轮 model.turn 的 seq, 本次 intent 的 seq]——
// 上一轮自己的事件是**历史**，不是"早到"。

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestStandbyEventWaitModelTurnNotWokenByPreviousTurn（critic P1 PROBE3）：第二轮
// standby 等 `model.turn` → **不**被上一轮自己的 model.turn 唤醒（真等 → 如实
// 超时收口）。
//
// 消息面**不受影响**（那是消息面的核心价值：对端的回话早于本轮也该捞到）。
func TestStandbyEventWaitModelTurnNotWokenByPreviousTurn(t *testing.T) {
	standbyTimeKnobs(t, 150*time.Millisecond, 5*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		// 第一轮：普通对话 → 落一条 model.turn（上一轮的历史事件）。
		"round1": {{text: "上一轮"}},
		// 第二轮：等 model.turn。
		"round2": {
			{tool: ToolStandby, args: `{"wait":"event","event_kind":"model.turn","timeout_min":1,"on_timeout":"return"}`},
			{text: "收尾"},
		},
	})
	sid := NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "round1"); err != nil {
		t.Fatalf("RunPrompt(round1): %v", err)
	}
	if _, err := h.RunPrompt(context.Background(), sid, "round2"); err != nil {
		t.Fatalf("RunPrompt(round2): %v", err)
	}
	text := standbyCompletedText(t, h, sid)
	if !strings.Contains(text, "未等到") {
		t.Fatalf("不该被上一轮的 model.turn 唤醒（该真等），得到 %q", text)
	}
	if strings.Contains(text, "model.turn") {
		t.Fatalf("completed 不该带 model.turn 事件的摘要，得到 %q", text)
	}
}

// TestStandbyEventWaitToolCallNotWokenByPreviousCompleted（critic P1 PROBE1）：等
// `tool.call.*` → **不**被上一轮的 tool.call.completed 唤醒（真等）。
func TestStandbyEventWaitToolCallNotWokenByPreviousCompleted(t *testing.T) {
	standbyTimeKnobs(t, 150*time.Millisecond, 5*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		// 第一轮：调一次 standby（超时收口 → 落 tool.call.intent + tool.call.completed）。
		"round1": {
			{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`},
			{text: "收尾一"},
		},
		// 第二轮：等 tool.call.*。
		"round2": {
			{tool: ToolStandby, args: `{"wait":"event","event_kind":"tool.call.*","timeout_min":1,"on_timeout":"return"}`},
			{text: "收尾二"},
		},
	})
	sid := NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "round1"); err != nil {
		t.Fatalf("RunPrompt(round1): %v", err)
	}
	if _, err := h.RunPrompt(context.Background(), sid, "round2"); err != nil {
		t.Fatalf("RunPrompt(round2): %v", err)
	}
	text := standbyCompletedText(t, h, sid)
	if !strings.Contains(text, "未等到") {
		t.Fatalf("不该被上一轮的 tool.call.* 唤醒（该真等），得到 %q", text)
	}
	if strings.Contains(text, "tool.call.completed") {
		t.Fatalf("completed 不该带上一轮 completed 事件的摘要，得到 %q", text)
	}
}
