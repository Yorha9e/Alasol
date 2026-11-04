package host

// G-j：standby 的 **event 等待条件**（v1 表达式：kind + scope）的验收用例。
//
// # v1 表达式（够用即止；更复杂的表达式挂账）
//
//	standby({wait: "event", event_kind: "<kind>", event_scope?: "self"|"any",
//	         timeout_min, on_timeout})
//
//	- **event_kind 必填**：精确 kind 或**尾缀通配** "prefix*"（如 "agent.*"）；
//	- **event_scope 可选，默认 self**（只匹配本 session）；"any" = 全事件流；
//	- **只认 durable 事件**（非 durable 不入库 → 扫不到，F8）；
//	- 唤醒/交付语义**全套复用**消息面：候选 = 匹配 kind ∧ seq ∉ 已交付集合 → 早到
//	  检查 + 增量轮询 + 交付前 fresh 复查 + 取最新；completed = 事件摘要 +
//	  交付标记（进**同一**交付集合）。
//
// # 六组用例
//
//	① 精确 kind：等待中落一条匹配事件 → completed 带摘要；不匹配的不唤醒；
//	② 前缀通配 "agent.*"：agent.completed 唤醒、model.turn 不唤醒；
//	③ scope：self（默认）不被别 session 的匹配事件唤醒；event_scope="any" 会；
//	④ 早到：等待前已到达的匹配事件 → 立即 completed + 标记；第二次 standby 不重复交付；
//	⑤ 非 durable 事件不唤醒（store 直接拒收 → 扫不到）；
//	⑥ message/handoff 与 event 交替不互踩（跨 kind 交付集合）。
//
// 为什么是 internal（package host）：事件等待的内部口径（候选过滤、交付集合）与
// 时间口径（standbyMinute / standbyHardCap / standbyPollInterval）都是包内符号。

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
)

// standbyAppendEvent 往事件库落一条 durable 事件（"外部事件"夹具：等事件条件用）。
//
// 直接落库（不经 hub 的 bus）：standby 读的是**事件流**（store），不依赖投影/在线表。
func standbyAppendEvent(t *testing.T, h *Hub, sid contracts.Ulid, kind, payload string) contracts.Event {
	t.Helper()
	stored, err := h.store.Append(context.Background(), []contracts.Event{{
		V: 1, ID: core.NewUlid(), Kind: kind, TS: time.Now().UTC().Format(time.RFC3339),
		SessionID: sid, AgentID: contracts.Ulid(core.NewUlid()), Durable: true,
		Payload: json.RawMessage(payload),
	}})
	if err != nil {
		t.Fatalf("Append(%s): %v", kind, err)
	}
	if len(stored) != 1 {
		t.Fatalf("Append(%s) 该回 1 条，得到 %d", kind, len(stored))
	}
	return stored[0]
}

// standbyEventSteps 是"等一条事件"的剧本（第一个 step = standby 调用）。
func standbyEventSteps(args string) map[string][]standbyStep {
	return map[string][]standbyStep{
		"waiter": {
			{tool: ToolStandby, args: args},
			{text: "收到"},
		},
	}
}

// standbyEventRun 跑一轮"等事件"的 standby（后台），返回"收场"信号。
func standbyEventRun(t *testing.T, h *Hub, sid contracts.Ulid) chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := h.RunPrompt(context.Background(), sid, "waiter")
		done <- err
	}()
	return done
}

// ---------------------------------------------------------------------------
// ① 精确 kind
// ---------------------------------------------------------------------------

// TestStandbyEventWaitExactKind：wait="event" + event_kind="test.completed" →
// 等待中落一条 **test.completed** → 唤醒（completed 带事件摘要）；**不匹配**的
// kind（test.other）不唤醒。
func TestStandbyEventWaitExactKind(t *testing.T) {
	standbyTimeKnobs(t, 10*time.Second, 30*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, standbyEventSteps(
		`{"wait":"event","event_kind":"test.completed","timeout_min":10,"on_timeout":"return"}`))
	sid := NewSession()
	done := standbyEventRun(t, h, sid)
	standbyWaitWaiting(t, h, sid, true)

	// 不匹配的 kind → 不唤醒。
	standbyAppendEvent(t, h, sid, "test.other", `{"x":1}`)
	time.Sleep(300 * time.Millisecond)
	if _, completed, _ := standbyToolCalls(t, h, sid); len(completed) != 0 {
		t.Fatalf("不匹配的事件不该唤醒 standby（completed 已有 %d 条）", len(completed))
	}

	// 匹配的 kind → 唤醒。
	ev := standbyAppendEvent(t, h, sid, "test.completed", `{"done":true}`)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunPrompt: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("standby 没被匹配的事件唤醒")
	}
	text := standbyCompletedText(t, h, sid)
	if !strings.Contains(text, "test.completed") {
		t.Fatalf("completed 应带事件 kind，得到 %q", text)
	}
	if !strings.Contains(text, fmt.Sprintf("seq=%d", *ev.Seq)) {
		t.Fatalf("completed 应带事件 seq（%d），得到 %q", *ev.Seq, text)
	}
	if !strings.Contains(text, "done") {
		t.Fatalf("completed 应带 payload 预览，得到 %q", text)
	}
}

// ---------------------------------------------------------------------------
// ② 前缀通配
// ---------------------------------------------------------------------------

// TestStandbyEventWaitPrefixWildcard：event_kind="agent.*" → **agent.completed**
// 唤醒；**model.turn** 不唤醒（本 session 自己的 model.turn 事件不算命中）。
func TestStandbyEventWaitPrefixWildcard(t *testing.T) {
	standbyTimeKnobs(t, 10*time.Second, 30*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, standbyEventSteps(
		`{"wait":"event","event_kind":"agent.*","timeout_min":10,"on_timeout":"return"}`))
	sid := NewSession()
	done := standbyEventRun(t, h, sid)
	standbyWaitWaiting(t, h, sid, true)

	// 不匹配（model.turn：本 session 自己就有这类事件）→ 不唤醒。
	standbyAppendEvent(t, h, sid, "model.turn", `{"text":"hi"}`)
	time.Sleep(300 * time.Millisecond)
	if _, completed, _ := standbyToolCalls(t, h, sid); len(completed) != 0 {
		t.Fatalf("model.turn 不该匹配 agent.* （completed 已有 %d 条）", len(completed))
	}

	// 匹配（agent.completed）→ 唤醒。
	standbyAppendEvent(t, h, sid, "agent.completed", `{"agent":"x"}`)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunPrompt: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("standby 没被 agent.completed 唤醒")
	}
	text := standbyCompletedText(t, h, sid)
	if !strings.Contains(text, "agent.completed") {
		t.Fatalf("completed 应带事件 kind，得到 %q", text)
	}
}

// ---------------------------------------------------------------------------
// ③ scope
// ---------------------------------------------------------------------------

// TestStandbyEventWaitScope：scope 默认 **self** → 别 session 的匹配事件**不**唤醒；
// event_scope="any" → 别 session 的匹配事件**也**唤醒。
func TestStandbyEventWaitScope(t *testing.T) {
	t.Run("self（默认）不被别 session 的事件唤醒", func(t *testing.T) {
		standbyTimeKnobs(t, 10*time.Second, 30*time.Second, 20*time.Millisecond)
		h := newStandbyTestHub(t, standbyEventSteps(
			`{"wait":"event","event_kind":"test.completed","timeout_min":10,"on_timeout":"return"}`))
		sid := NewSession()
		done := standbyEventRun(t, h, sid)
		standbyWaitWaiting(t, h, sid, true)

		// 别 session 的匹配事件 → 不唤醒（self 口径）。
		standbyAppendEvent(t, h, NewSession(), "test.completed", `{"other":true}`)
		time.Sleep(300 * time.Millisecond)
		if _, completed, _ := standbyToolCalls(t, h, sid); len(completed) != 0 {
			t.Fatalf("self 口径不该被别 session 的事件唤醒（completed 已有 %d 条）", len(completed))
		}
		// 本 session 的匹配事件 → 唤醒（证明只是"别 session"被挡住了）。
		standbyAppendEvent(t, h, sid, "test.completed", `{"mine":true}`)
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("RunPrompt: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("standby 没被本 session 的事件唤醒")
		}
	})

	t.Run("any 会被别 session 的事件唤醒", func(t *testing.T) {
		standbyTimeKnobs(t, 10*time.Second, 30*time.Second, 20*time.Millisecond)
		h := newStandbyTestHub(t, standbyEventSteps(
			`{"wait":"event","event_kind":"test.completed","event_scope":"any","timeout_min":10,"on_timeout":"return"}`))
		sid := NewSession()
		done := standbyEventRun(t, h, sid)
		standbyWaitWaiting(t, h, sid, true)

		standbyAppendEvent(t, h, NewSession(), "test.completed", `{"other":true}`)
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("RunPrompt: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("event_scope=any 该被别 session 的事件唤醒")
		}
		text := standbyCompletedText(t, h, sid)
		if !strings.Contains(text, "test.completed") {
			t.Fatalf("completed 应带事件 kind，得到 %q", text)
		}
	})
}

// ---------------------------------------------------------------------------
// ④ 早到 + 不重复交付
// ---------------------------------------------------------------------------

// TestStandbyEventArrivedBeforeTurnIsNotEarly（critic P1）：匹配事件**在等待开始前**
// 就到达 → **不算早到**（那是**历史**）→ standby 真等 → 如实超时收口「未等到」。
//
// 事件面与消息面的"早到"定义**不同**（见 host/standby.go 的文件头）：消息面早到 =
// 外部事实未交付过（早于本轮也该捞到）；事件面早到 = **本轮内**到达（窗口 =
// (本轮 model.turn, 本次 intent]）——否则第二轮 standby 等 model.turn 会被**上一轮**
// 的 model.turn 秒唤醒。
//
// 本轮内到达的那条仍算早到 → 见 host/standby_event_window_test.go 的
// TestStandbyEventEarlyArrivedWithinTurn（单元级夹具）。
func TestStandbyEventArrivedBeforeTurnIsNotEarly(t *testing.T) {
	standbyTimeKnobs(t, 150*time.Millisecond, 5*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, standbyEventSteps(
		`{"wait":"event","event_kind":"test.completed","timeout_min":1,"on_timeout":"return"}`))
	sid := NewSession()
	// 事件**先到**（本轮之前 = 历史）。
	standbyAppendEvent(t, h, sid, "test.completed", `{"old":true}`)

	if _, err := h.RunPrompt(context.Background(), sid, "waiter"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	text := standbyCompletedText(t, h, sid)
	if !strings.Contains(text, "未等到") {
		t.Fatalf("等待前到达的事件是历史、不该当早到交付，得到 %q", text)
	}
	if strings.Contains(text, "test.completed") {
		t.Fatalf("completed 不该带那条历史事件的摘要，得到 %q", text)
	}
}

// ---------------------------------------------------------------------------
// ⑤ 非 durable 事件不唤醒
// ---------------------------------------------------------------------------

// TestStandbyEventWaitIgnoresNonDurable：非 durable 事件**不入库**（F8：store 的
// Append 直接拒收）→ 扫不到 → 不唤醒。
func TestStandbyEventWaitIgnoresNonDurable(t *testing.T) {
	standbyTimeKnobs(t, 10*time.Second, 30*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, standbyEventSteps(
		`{"wait":"event","event_kind":"test.completed","timeout_min":10,"on_timeout":"return"}`))
	sid := NewSession()
	done := standbyEventRun(t, h, sid)
	standbyWaitWaiting(t, h, sid, true)

	// 非 durable 事件：store 直接拒收（根本落不进去）。
	if _, err := h.store.Append(context.Background(), []contracts.Event{{
		V: 1, ID: core.NewUlid(), Kind: "test.completed", TS: time.Now().UTC().Format(time.RFC3339),
		SessionID: sid, AgentID: contracts.Ulid(core.NewUlid()), Durable: false,
		Payload: json.RawMessage(`{"x":1}`),
	}}); err == nil {
		t.Fatal("非 durable 事件不该被 store 收下（F8）")
	}
	time.Sleep(300 * time.Millisecond)
	if _, completed, _ := standbyToolCalls(t, h, sid); len(completed) != 0 {
		t.Fatalf("非 durable 事件不该唤醒 standby（completed 已有 %d 条）", len(completed))
	}

	// durable 的同 kind 事件 → 唤醒（证明只是"非 durable"被挡住了）。
	standbyAppendEvent(t, h, sid, "test.completed", `{"ok":true}`)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunPrompt: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("standby 没被 durable 事件唤醒")
	}
}

// ---------------------------------------------------------------------------
// ⑥ message 与 event 交替不互踩（跨 kind 交付集合）
// ---------------------------------------------------------------------------

// TestStandbyMessageAndEventDoNotCollide：同 session **同一轮**里先等**消息**
// （交付了一条 broadcast）→ 再等**事件**（event_kind="agentlink.*"、scope=any）→
// 那条已交付的 broadcast **不重复交付**（事件与消息共用一个交付集合：同一全局 seq
// 空间）。
//
// 为什么必须"同一轮"：事件面的早到窗口是 (本轮 model.turn, 本次 intent]——那条
// broadcast 是在**本轮**（第一个 standby 的等待期）到达的，正好落在第二个 standby
// 的窗口里 → 它是候选，但**已在交付集合里** → 被排除 ✓（这才是"跨 kind 不互踩"
// 要考的那条路径）。
func TestStandbyMessageAndEventDoNotCollide(t *testing.T) {
	standbyTimeKnobs(t, 200*time.Millisecond, 30*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		"peer":  {{text: "在"}},
		"hello": {{text: "我先热身"}},
		// 同一轮里连调两次 standby：先等消息，再等事件。
		"waiter": {
			{tool: ToolStandby, args: `{"wait":"message","timeout_min":10,"on_timeout":"return"}`},
			{tool: ToolStandby, args: `{"wait":"event","event_kind":"agentlink.*","event_scope":"any","timeout_min":1,"on_timeout":"return"}`},
			{text: "收尾"},
		},
	})
	ctx := context.Background()
	sidPeer := standbyStartPeer(t, h, ctx, "peer", standbyGate)
	sidWait := NewSession()
	if _, err := h.RunPrompt(ctx, sidWait, "hello"); err != nil {
		t.Fatalf("RunPrompt(hello): %v", err)
	}
	if _, err := h.JoinGate(ctx, sidWait, standbyGate); err != nil {
		t.Fatalf("JoinGate(waiter): %v", err)
	}

	// 后台跑那一轮（两次 standby）；广播在第一个 standby 等待期到达 → 被消息面交付。
	done := make(chan error, 1)
	go func() {
		_, err := h.RunPrompt(ctx, sidWait, "waiter")
		done <- err
	}()
	standbyWaitWaiting(t, h, sidWait, true)
	if _, err := h.Broadcast(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.BroadcastRequest{
		Target: standbyGateTarget(standbyGate),
		Text:   "一条广播",
	}); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunPrompt(waiter): %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("那一轮没跑完")
	}

	// 第一条 completed = 消息面交付那条广播。
	_, completed, failed := standbyToolCalls(t, h, sidWait)
	if len(failed) != 0 {
		t.Fatalf("不该有 failed：%s", standbyFailedText(t, h, sidWait))
	}
	if len(completed) != 2 {
		t.Fatalf("tool.call.completed 应恰好 2 条（两次 standby），得到 %d", len(completed))
	}
	var first contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(completed[0].Payload, &first); err != nil {
		t.Fatalf("解 completed 载荷失败: %v", err)
	}
	if !strings.Contains(standbyResultText(first.Result), "一条广播") {
		t.Fatalf("第一次 standby（等消息）该交付那条广播，得到 %q", standbyResultText(first.Result))
	}
	// 第二条 completed = 事件面**没有**重复交付那条广播（已在交付集合里）。
	var second contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(completed[1].Payload, &second); err != nil {
		t.Fatalf("解 completed 载荷失败: %v", err)
	}
	text2 := standbyResultText(second.Result)
	if strings.Contains(text2, "一条广播") {
		t.Fatalf("已交付的消息不该被 event 等待重复交付，得到 %q", text2)
	}
	if !strings.Contains(text2, "未等到") {
		t.Fatalf("第二次 standby（等事件）该如实超时收口，得到 %q", text2)
	}
}
