package runtime_test

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

func replaySessionID() contracts.Ulid { return contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6") }
func replayAgentID() contracts.Ulid   { return contracts.Ulid("TEECY7V57SP0J9WZ620EHHP02M") }

// durable 造一条 durable 事实事件。
func durable(id contracts.Ulid, kind, payload string) contracts.Event {
	return contracts.Event{
		V: 1, ID: id, Kind: kind, TS: "2025-03-01T10:00:00.000Z",
		SessionID: replaySessionID(), AgentID: replayAgentID(), Durable: true,
		Payload: json.RawMessage(payload),
	}
}

// ledgerPayload 序列化一个 ledger.updated payload。
func ledgerPayload(goals, completed []string) string {
	b, _ := json.Marshal(contracts.LedgerUpdatedPayload{
		Goals: goals, Constraints: []string{"pure-Go"}, Completed: completed, Pending: []string{},
	})
	return string(b)
}

// TestReplaySessionAndFoldLedger 写一场多轮 session（含交错流事件、未知 kind），
// 只把 durable 事件入库，回放逐条比对 + FoldLedger 取末条 ledger（验收①）。
func TestReplaySessionAndFoldLedger(t *testing.T) {
	st, err := sqlite.Open(t.TempDir()+"/replay.db", sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	ctx := context.Background()

	intent1 := core.NewUlid()
	// 期望入库的 durable 事件（按发生顺序 = 期望 seq 顺序）。
	want := []contracts.Event{
		durable(intent1, "tool.call.intent", `{"tool_name":"read_file"}`),
		durable(core.NewUlid(), "tool.call.completed", `{"ok":true}`),
		durable(core.NewUlid(), "ledger.updated", ledgerPayload([]string{"build API"}, nil)),
		durable(core.NewUlid(), "board.written", `{"key":"plan","value":"draft"}`),
		durable(core.NewUlid(), "weird.unknown.kind", `{"anything":1}`), // 未知 kind：忽略但持久化（D7）
		durable(core.NewUlid(), "ledger.updated", ledgerPayload([]string{"build API", "write tests"}, []string{"design"})),
		durable(core.NewUlid(), "tool.call.intent", `{"tool_name":"run_tests"}`),
	}

	// 只追加 durable 事件（durable=false 的流事件由总线直发，不入库——这里甚至不去 Append 它们）。
	stored, err := st.Append(ctx, want)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	// 入库时 seq 已回填。
	for i := range stored {
		if stored[i].Seq == nil {
			t.Fatalf("stored[%d] has no seq", i)
		}
		want[i].Seq = stored[i].Seq
	}

	// 试图追加一条流事件必须被拒（F8 第二道防线），且不入库。
	stream := durable(core.NewUlid(), "model.delta", `{"text":"hi"}`)
	stream.Durable = false
	if _, err := st.Append(ctx, []contracts.Event{stream}); err == nil {
		t.Fatalf("store accepted a non-durable stream event")
	}

	// 回放：逐条比对。
	got, err := runtime.ReplaySession(ctx, st, replaySessionID())
	if err != nil {
		t.Fatalf("ReplaySession: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("replayed %d events, want %d (stream events must not be stored)", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID {
			t.Fatalf("event %d: id = %s, want %s", i, got[i].ID, want[i].ID)
		}
		if got[i].Kind != want[i].Kind {
			t.Fatalf("event %d: kind = %s, want %s", i, got[i].Kind, want[i].Kind)
		}
		if *got[i].Seq != int64(i+1) {
			t.Fatalf("event %d: seq = %d, want %d (ascending, contiguous)", i, *got[i].Seq, i+1)
		}
		if !jsonEqual(got[i].Payload, want[i].Payload) {
			t.Fatalf("event %d: payload = %s, want %s", i, got[i].Payload, want[i].Payload)
		}
	}

	// FoldLedger：末条 ledger.updated 的全量快照。
	led := runtime.FoldLedger(got)
	if !reflect.DeepEqual(led.Goals, []string{"build API", "write tests"}) {
		t.Fatalf("FoldLedger goals = %v, want [build API write tests]", led.Goals)
	}
	if !reflect.DeepEqual(led.Completed, []string{"design"}) {
		t.Fatalf("FoldLedger completed = %v, want [design]", led.Completed)
	}
	if !reflect.DeepEqual(led.Constraints, []string{"pure-Go"}) {
		t.Fatalf("FoldLedger constraints = %v, want [pure-Go]", led.Constraints)
	}
}

// TestFoldLedgerEmpty：没有 ledger.updated 时返回零值。
func TestFoldLedgerEmpty(t *testing.T) {
	events := []contracts.Event{
		durable(core.NewUlid(), "tool.call.intent", `{}`),
		durable(core.NewUlid(), "board.written", `{"key":"k"}`),
	}
	led := runtime.FoldLedger(events)
	if led.Goals != nil || led.Constraints != nil || led.Completed != nil || led.Pending != nil {
		t.Fatalf("FoldLedger with no ledger events should be zero value, got %+v", led)
	}
}

// TestReplaySessionIsolation：回放只返回目标 session 的事件（按 session_id 过滤）。
func TestReplaySessionIsolation(t *testing.T) {
	st, err := sqlite.Open(t.TempDir()+"/iso.db", sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	ctx := context.Background()

	other := contracts.Ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	mine := func(kind string) contracts.Event {
		return contracts.Event{V: 1, ID: core.NewUlid(), Kind: kind, TS: "2025-03-01T10:00:00Z",
			SessionID: replaySessionID(), AgentID: replayAgentID(), Durable: true, Payload: json.RawMessage(`{}`)}
	}
	theirs := func(kind string) contracts.Event {
		return contracts.Event{V: 1, ID: core.NewUlid(), Kind: kind, TS: "2025-03-01T10:00:00Z",
			SessionID: other, AgentID: replayAgentID(), Durable: true, Payload: json.RawMessage(`{}`)}
	}
	if _, err := st.Append(ctx, []contracts.Event{mine("a"), theirs("a"), mine("b"), theirs("b")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	got, err := runtime.ReplaySession(ctx, st, replaySessionID())
	if err != nil {
		t.Fatalf("ReplaySession: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("replayed %d events for my session, want 2 (other session filtered)", len(got))
	}
	for _, e := range got {
		if e.SessionID != replaySessionID() {
			t.Fatalf("replay leaked session %s", e.SessionID)
		}
	}
}

func jsonEqual(a, b []byte) bool {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}
	return reflect.DeepEqual(va, vb)
}
