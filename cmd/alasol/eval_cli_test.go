package main

// `alasol eval -session <id>` 子命令的端到端钉子（L4/L5 最小评测面，v4-7 后半）。
//
// 口径：
//   - **事件流即数据集**：seed 一段确定性事件流（user.prompt + 两轮 model.turn（带 usage）
//     + 中间的 tool.call.intent）→ eval 打印单行 JSON，逐 turn 指标与 totals 全对；
//   - 观察面：只读事件库，不写回、不建框架；
//   - 坏输入（空/非法 -session）→ 退出码 2；无事件的 session → 空报告、退出码 0。

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// evalEvent 是一条 seed 事件的简写。
func evalEvent(t *testing.T, kind, ts string, session, agent contracts.Ulid, payload any) contracts.Event {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s: %v", kind, err)
	}
	return contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: kind, TS: ts,
		SessionID: session, AgentID: agent, Durable: true, Payload: raw,
	}
}

// seedEvalSession 建库并塞一段两轮对话（第一轮带 2 次工具调用）：
//
//	user.prompt        t0
//	model.turn   #1   t0+1s   usage 100/20   ← 第 1 轮
//	tool.call.intent   t0+2s
//	tool.call.intent   t0+3s
//	model.turn   #2   t0+5s   usage 50/400   ← 第 2 轮（无工具调用，收敛轮）
//
// 期望指标：
//
//	turn1: tool_calls=2 tokens=100/20 duration=4s（t0+1s → t0+5s）
//	turn2: tool_calls=0 tokens=50/400 duration=1s（t0+5s → 末事件 t0+6s）
//	totals: turns=2 tool_calls=2 tokens=150/420 duration=6s
func seedEvalSession(t *testing.T, dbPath string) contracts.Ulid {
	t.Helper()
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	session, agent := core.NewUlid(), core.NewUlid()
	model := "gpt-4o-mini"
	evs := []contracts.Event{
		evalEvent(t, "user.prompt", "2026-11-01T00:00:00Z", session, agent,
			contracts.UserPromptPayload{Text: "评测种子"}),
		evalEvent(t, "model.turn", "2026-11-01T00:00:01Z", session, agent,
			contracts.ModelTurnPayload{
				Text: "先查一下", Model: &model,
				Usage: &contracts.ModelTurnUsage{InputTokens: 100, OutputTokens: 20},
			}),
		evalEvent(t, "tool.call.intent", "2026-11-01T00:00:02Z", session, agent,
			contracts.ToolCallIntentPayload{
				ToolCallID: "tc_eval_1", ToolName: "read",
				Arguments: json.RawMessage(`{}`), Idempotent: true, Replay: contracts.ReplayKindSafe,
			}),
		evalEvent(t, "tool.call.intent", "2026-11-01T00:00:03Z", session, agent,
			contracts.ToolCallIntentPayload{
				ToolCallID: "tc_eval_2", ToolName: "read",
				Arguments: json.RawMessage(`{}`), Idempotent: true, Replay: contracts.ReplayKindSafe,
			}),
		evalEvent(t, "model.turn", "2026-11-01T00:00:05Z", session, agent,
			contracts.ModelTurnPayload{
				Text: "总结完了", Model: &model,
				Usage: &contracts.ModelTurnUsage{InputTokens: 50, OutputTokens: 400},
			}),
	}
	// 末事件：tool.call.completed（无 usage；只推末 ts，给第 2 轮 duration 与 totals duration 供底）。
	evs = append(evs, evalEvent(t, "tool.call.completed", "2026-11-01T00:00:06Z", session, agent,
		contracts.ToolCallCompletedPayload{
			ToolCallID: "tc_eval_1",
			Result:     contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text"}}},
		}))
	if _, err := st.Append(context.Background(), evs); err != nil {
		t.Fatalf("append: %v", err)
	}
	return session
}

// TestEvalCLIReturnsPerTurnMetrics：seed 事件流 → eval 单行 JSON → 逐 turn 指标全对。
func TestEvalCLIReturnsPerTurnMetrics(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "eval.db")
	sid := seedEvalSession(t, dbPath)

	out := captureStdout(t)
	code := evalMain([]string{"-session", string(sid), "-db", dbPath})
	got := out()
	if code != 0 {
		t.Fatalf("evalMain 退出码 = %d，want 0（stdout=%s）", code, got)
	}
	lines := strings.Split(strings.TrimSpace(got), "\n")
	if len(lines) != 1 {
		t.Fatalf("stdout 应只有一行 JSON，got %d 行:\n%s", len(lines), got)
	}
	var rep EvalReport
	if err := json.Unmarshal([]byte(lines[0]), &rep); err != nil {
		t.Fatalf("stdout 不是合法 JSON: %v\n%s", err, got)
	}

	if rep.SessionID != string(sid) {
		t.Fatalf("session_id = %q，want %q", rep.SessionID, sid)
	}
	if rep.Events != 6 {
		t.Fatalf("events = %d，want 6", rep.Events)
	}
	if len(rep.Turns) != 2 {
		t.Fatalf("turns = %d，want 2（%+v）", len(rep.Turns), rep.Turns)
	}

	turn1 := rep.Turns[0]
	if turn1.Turn != 1 || turn1.Model != "gpt-4o-mini" {
		t.Fatalf("turn1 形状不符: %+v", turn1)
	}
	if turn1.ToolCalls != 2 {
		t.Fatalf("turn1.tool_calls = %d，want 2（两条 intent 归第 1 轮）", turn1.ToolCalls)
	}
	if turn1.InputTokens != 100 || turn1.OutputTokens != 20 {
		t.Fatalf("turn1 tokens = %d/%d，want 100/20", turn1.InputTokens, turn1.OutputTokens)
	}
	if turn1.DurationMS != 4000 {
		t.Fatalf("turn1.duration_ms = %d，want 4000（t0+1s → t0+5s）", turn1.DurationMS)
	}

	turn2 := rep.Turns[1]
	if turn2.Turn != 2 || turn2.ToolCalls != 0 {
		t.Fatalf("turn2 形状不符: %+v", turn2)
	}
	if turn2.InputTokens != 50 || turn2.OutputTokens != 400 {
		t.Fatalf("turn2 tokens = %d/%d，want 50/400", turn2.InputTokens, turn2.OutputTokens)
	}
	if turn2.DurationMS != 1000 {
		t.Fatalf("turn2.duration_ms = %d，want 1000（t0+5s → 末事件 t0+6s）", turn2.DurationMS)
	}

	tot := rep.Totals
	if tot.Turns != 2 || tot.ToolCalls != 2 {
		t.Fatalf("totals 计数不符: %+v", tot)
	}
	if tot.InputTokens != 150 || tot.OutputTokens != 420 {
		t.Fatalf("totals tokens = %d/%d，want 150/420", tot.InputTokens, tot.OutputTokens)
	}
	if tot.DurationMS != 6000 {
		t.Fatalf("totals.duration_ms = %d，want 6000（整场 t0 → t0+6s）", tot.DurationMS)
	}
}

// TestEvalCLIEmptySession：无事件的 session → 空报告（turns=[]）、退出码 0（不是错误）。
func TestEvalCLIEmptySession(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "empty.db")
	sid := core.NewUlid()

	out := captureStdout(t)
	code := evalMain([]string{"-session", string(sid), "-db", dbPath})
	got := out()
	if code != 0 {
		t.Fatalf("空 session 退出码 = %d，want 0", code)
	}
	var rep EvalReport
	if err := json.Unmarshal([]byte(strings.TrimSpace(got)), &rep); err != nil {
		t.Fatalf("stdout 不是合法 JSON: %v\n%s", err, got)
	}
	if rep.Events != 0 || len(rep.Turns) != 0 || rep.Totals.Turns != 0 {
		t.Fatalf("空 session 应报全零，得到 %+v", rep)
	}
}

// TestEvalCLIRejectsBadSession：-session 空 / 非 ULID → 退出码 2（flag 口径错误）。
func TestEvalCLIRejectsBadSession(t *testing.T) {
	if code := evalMain([]string{"-session", "", "-db", "x.db"}); code != 2 {
		t.Fatalf("空 -session 退出码 = %d，want 2", code)
	}
	if code := evalMain([]string{"-session", "not-a-ulid", "-db", "x.db"}); code != 2 {
		t.Fatalf("非 ULID -session 退出码 = %d，want 2", code)
	}
}
