package loop_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/durable"
	"github.com/Yorha9e/alasol/core/loop"
)

// TestLoopCheckpointEveryAuto：Config.CheckpointEvery=1 + Config.Store → 每个有工具调用的 turn
// 末自动落一条 checkpoint.created（X3）。
func TestLoopCheckpointEveryAuto(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)
	h.script(stepTools(call("c1", "echo", `{"text":"hi"}`)), stepText("完成"))

	l := h.loop(func(c *loop.Config) {
		c.Store = h.store
		c.CheckpointEvery = 1
	})
	res, err := l.Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ToolCalls != 1 {
		t.Fatalf("ToolCalls = %d, want 1", res.ToolCalls)
	}
	// turn 1 有工具调用 → turn 末自动 checkpoint 一次；turn 2 收敛（不进 checkpoint）。
	if n := countKind(h, "checkpoint.created"); n != 1 {
		t.Fatalf("checkpoint.created = %d, want 1（每有工具调用的 turn 末一次）", n)
	}
	// checkpoint 的四要素合理：水位 = seed 前的事件 tip，in_flight 空（turn 末无未决调用）。
	cp := lastCheckpoint(t, h)
	if cp.SeqWatermark == 0 {
		t.Fatalf("checkpoint seq_watermark 不该为 0: %+v", cp)
	}
	if len(cp.InFlight) != 0 {
		t.Fatalf("turn 末不应有 in-flight，得到 %+v", cp.InFlight)
	}
	// 执行器仍只被调一次（checkpoint 不重放任何东西）。
	if exec.callN() != 1 {
		t.Fatalf("echo 执行次数 = %d, want 1", exec.callN())
	}
}

// TestLoopManualCheckpoint：Loop.Checkpoint 手动建 checkpoint，四要素齐全。
func TestLoopManualCheckpoint(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(stepTools(call("c1", "echo", `{}`)), stepText("完成"))
	l := h.loop(func(c *loop.Config) { c.Store = h.store })
	if _, err := l.Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	cp, err := l.Checkpoint(context.Background())
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if cp.SessionID != h.session {
		t.Fatalf("checkpoint session = %q, want %q", cp.SessionID, h.session)
	}
	if cp.SeqWatermark == 0 || cp.LedgerSeq != 0 || cp.TreeVersion != 0 {
		t.Fatalf("checkpoint 四要素异常（无 ledger/agent 事件应为 0）: %+v", cp)
	}
	if countKind(h, "checkpoint.created") != 1 {
		t.Fatalf("checkpoint.created = %d, want 1", countKind(h, "checkpoint.created"))
	}
}

// TestLoopCheckpointRequiresStore：未配 Store 时 Checkpoint 报错（不静默）。
func TestLoopCheckpointRequiresStore(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	l := h.loop(nil) // 不设 Store
	if _, err := l.Checkpoint(context.Background()); err == nil {
		t.Fatal("未配 Store 时 Checkpoint 应报错")
	}
}

// TestLoopAsDurableReplayer：*Loop 实现 durable.Replayer——一条「有 intent 无 result」的调用
// 经 durable.Resume 由 loop 重放：执行器再跑一次、落 completed、disposition=replayed。
func TestLoopAsDurableReplayer(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)
	l := h.loop(func(c *loop.Config) { c.Store = h.store })

	// 直接往 store 追加一条 intent（无 result）——模拟「执行到一半崩了」的未知结果调用。
	intent := contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: loop.KindToolCallIntent, TS: "2026-10-28T00:00:00Z",
		SessionID: h.session, AgentID: h.agent, Durable: true,
		Payload: mustJSON(t, contracts.ToolCallIntentPayload{
			ToolCallID: "c-crash", ToolName: "echo",
			Arguments: json.RawMessage(`{"text":"replay-me"}`), Idempotent: true, Replay: contracts.ReplayKindSafe,
		}),
	}
	if _, err := h.store.Append(context.Background(), []contracts.Event{intent}); err != nil {
		t.Fatalf("append intent: %v", err)
	}

	// durable.Resume 用 loop 作 Replayer 重放。
	rep, err := durable.Resume(context.Background(), h.store, h.session, durable.ResumeOptions{Replayer: l})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if len(rep.Dispositions) != 1 || rep.Dispositions[0].Disposition != contracts.ResumeDispositionReplayed {
		t.Fatalf("dispositions = %+v, want [replayed]", rep.Dispositions)
	}
	// 执行器被重放调了一次；落了一条 completed（ causation 指向原 intent）。
	if exec.callN() != 1 {
		t.Fatalf("echo 重放次数 = %d, want 1", exec.callN())
	}
	completed := 0
	for _, ev := range h.events() {
		if ev.Kind == loop.KindToolCallCompleted {
			completed++
			if ev.CausationID == nil || *ev.CausationID != intent.ID {
				t.Fatalf("重放的 completed 未指向原 intent: %v", ev.CausationID)
			}
		}
	}
	if completed != 1 {
		t.Fatalf("completed 事件数 = %d, want 1", completed)
	}
}

// lastCheckpoint 取 store 里最后一条 checkpoint.created 的 payload（测试辅助）。
func lastCheckpoint(t *testing.T, h *harness) contracts.CheckpointCreatedPayload {
	t.Helper()
	var out contracts.CheckpointCreatedPayload
	found := false
	for _, ev := range h.events() {
		if ev.Kind != "checkpoint.created" {
			continue
		}
		if err := json.Unmarshal(ev.Payload, &out); err != nil {
			t.Fatalf("解 checkpoint payload: %v", err)
		}
		found = true
	}
	if !found {
		t.Fatal("没有 checkpoint.created 事件")
	}
	return out
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// countKind 统计 harness 事件流里某 kind 的条数（测试辅助）。
func countKind(h *harness, kind string) int {
	n := 0
	for _, ev := range h.events() {
		if ev.Kind == kind {
			n++
		}
	}
	return n
}
