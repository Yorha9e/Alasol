package durable_test

import (
	"context"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/durable"
)

// TestCheckpointFourElements 验证 checkpoint 四要素（X3）正确，含 in-flight 识别
// （intent 无对应 result = in-flight；有 completed/failed = 不在列）。
func TestCheckpointFourElements(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	session := core.NewUlid()
	agent := core.NewUlid()

	// 造一场 session：ledger、两个 spawn、一次 completed 的调用、两次 in-flight 调用
	//（一个 safe 无结果、一个 unsafe 无结果）、一次 failed 的调用（有结果 → 不在列）。
	seed := []contracts.Event{
		evLedger(session, agent, []string{"g1"}),                                      // seq1 → ledger_seq
		evIntent(session, agent, "c-done", "read", contracts.ReplayKindSafe, nil),     // seq2
		evResult(session, agent, "tool.call.completed", "c-done"),                     // seq3 → 有结果，非 in-flight
		evIntent(session, agent, "c-failed", "bash", contracts.ReplayKindVerify, nil), // seq4
		evResult(session, agent, "tool.call.failed", "c-failed"),                      // seq5 → 有结果，非 in-flight
		evSpawned(session, agent, core.NewUlid(), "worker", "be", 2, core.NewUlid()),  // seq6 → tree_version 之一
		evIntent(session, agent, "c-safe", "echo", contracts.ReplayKindSafe,
			[]contracts.Anchor{{Kind: "git-commit", Value: "abc123"}}), // seq7 → in-flight (safe)
		evIntent(session, agent, "c-unsafe", "sendmail", contracts.ReplayKindUnsafe, nil), // seq8 → in-flight (unsafe)
	}
	if _, err := st.Append(ctx, seed); err != nil {
		t.Fatalf("seed append: %v", err)
	}

	cp, err := durable.Create(ctx, st, session)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// ① seq_watermark = 本 session 事件最大 seq（seed 8 条；checkpoint.created 自身不计入水位）。
	if cp.SeqWatermark != 8 {
		t.Fatalf("seq_watermark = %d, want 8", cp.SeqWatermark)
	}
	// ② ledger_seq = 最后一条 ledger.updated 的 seq = 1。
	if cp.LedgerSeq != 1 {
		t.Fatalf("ledger_seq = %d, want 1", cp.LedgerSeq)
	}
	// ④ tree_version = 最后一条 agent.* 的 seq = 6。
	if cp.TreeVersion != 6 {
		t.Fatalf("tree_version = %d, want 6", cp.TreeVersion)
	}
	// ③ in_flight = 只有 c-safe 与 c-unsafe（有 result 的 c-done / c-failed 不在列）。
	if len(cp.InFlight) != 2 {
		t.Fatalf("in_flight = %d 项, want 2: %+v", len(cp.InFlight), cp.InFlight)
	}
	got := map[contracts.ProviderId]contracts.InFlightOp{}
	for _, op := range cp.InFlight {
		got[op.ToolCallID] = op
	}
	safe, ok := got["c-safe"]
	if !ok || safe.Replay != contracts.ReplayKindSafe || safe.ToolName != "echo" {
		t.Fatalf("in_flight 缺 c-safe 或字段错: %+v", safe)
	}
	if len(safe.Anchors) != 1 || safe.Anchors[0].Kind != "git-commit" || safe.Anchors[0].Value != "abc123" {
		t.Fatalf("c-safe 的 anchors 未从 intent 透传: %+v", safe.Anchors)
	}
	if unsafe, ok := got["c-unsafe"]; !ok || unsafe.Replay != contracts.ReplayKindUnsafe {
		t.Fatalf("in_flight 缺 c-unsafe 或 replay 错: %+v", unsafe)
	}

	// checkpoint 本身落为一条 checkpoint.created durable 事件。
	if n := st.countKind("checkpoint.created"); n != 1 {
		t.Fatalf("checkpoint.created 事件数 = %d, want 1", n)
	}
}

// TestFindInFlightWindow：FindInFlight 的 fromSeq 下界 + 正确配对需从 1 扫起的口径。
func TestFindInFlightWindow(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	session := core.NewUlid()
	agent := core.NewUlid()
	seed := []contracts.Event{
		evIntent(session, agent, "c1", "echo", contracts.ReplayKindSafe, nil), // seq1
		evResult(session, agent, "tool.call.completed", "c1"),                 // seq2
		evIntent(session, agent, "c2", "echo", contracts.ReplayKindSafe, nil), // seq3 → in-flight
	}
	if _, err := st.Append(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// 从 1 扫：只有 c2（c1 有结果）。
	refs, err := durable.FindInFlight(ctx, st, session, 1)
	if err != nil {
		t.Fatalf("FindInFlight: %v", err)
	}
	if len(refs) != 1 || string(refs[0].ToolCallID) != "c2" {
		t.Fatalf("FindInFlight(1) = %+v, want [c2]", refs)
	}
	// 从 seq4 扫（窗口内无事件）→ 空。
	refs, _ = durable.FindInFlight(ctx, st, session, 4)
	if len(refs) != 0 {
		t.Fatalf("FindInFlight(4) = %+v, want 空", refs)
	}
}

// TestFindInFlightNoResultsReturnsEmptySlice：无 in-flight 时空清单（不是 nil，便于 JSON 落 []）。
func TestFindInFlightNoResultsReturnsEmptySlice(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	session := core.NewUlid()
	agent := core.NewUlid()
	if _, err := st.Append(ctx, []contracts.Event{
		evIntent(session, agent, "c1", "echo", contracts.ReplayKindSafe, nil),
		evResult(session, agent, "tool.call.completed", "c1"),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	refs, err := durable.FindInFlight(ctx, st, session, 1)
	if err != nil {
		t.Fatalf("FindInFlight: %v", err)
	}
	if refs == nil {
		t.Fatal("FindInFlight 应返回非 nil 空切片")
	}
	if len(refs) != 0 {
		t.Fatalf("FindInFlight = %+v, want 空", refs)
	}
}
