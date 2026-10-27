package durable_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/durable"
)

// mockReplayer 记录每次重放并按 tool_call_id 落一条 completed 事件（模拟副作用 + 结果落盘）。
//
// failOn 里的 tool_call_id 走"执行了但失败"（落 failed → OK=false + 失败码）；
// errOn 里的走"写路径断裂"（返回 error）。
type mockReplayer struct {
	store  core.Store
	calls  map[contracts.ProviderId]int
	failOn map[contracts.ProviderId]bool
	errOn  map[contracts.ProviderId]bool
}

func newMockReplayer(store core.Store) *mockReplayer {
	return &mockReplayer{
		store:  store,
		calls:  map[contracts.ProviderId]int{},
		failOn: map[contracts.ProviderId]bool{},
		errOn:  map[contracts.ProviderId]bool{},
	}
}

func (r *mockReplayer) Replay(_ context.Context, op durable.Op) (durable.ReplayResult, error) {
	r.calls[op.Ref.ToolCallID]++
	if r.errOn[op.Ref.ToolCallID] {
		return durable.ReplayResult{}, errors.New("replay injected failure")
	}
	if r.failOn[op.Ref.ToolCallID] {
		// 执行了但失败 → 落 failed（op 已收口），返回 OK=false + 失败码/说明。
		failed := evResult(op.SessionID, op.AgentID, "tool.call.failed", string(op.Ref.ToolCallID))
		failed.CausationID = &op.IntentEventID
		if _, err := r.store.Append(context.Background(), []contracts.Event{failed}); err != nil {
			return durable.ReplayResult{}, err
		}
		return durable.ReplayResult{OK: false, FailureCode: "tool_error", FailureMessage: "replay injected tool failure"}, nil
	}
	// 落一条 completed（causation 指向原 intent），让该 op 不再是 in-flight。
	done := evResult(op.SessionID, op.AgentID, "tool.call.completed", string(op.Ref.ToolCallID))
	done.CausationID = &op.IntentEventID
	if _, err := r.store.Append(context.Background(), []contracts.Event{done}); err != nil {
		return durable.ReplayResult{}, err
	}
	return durable.ReplayResult{OK: true}, nil
}

func (r *mockReplayer) callN(tcID string) int { return r.calls[contracts.ProviderId(tcID)] }

// mockVerifier 按预设表返回核验结论；不在表里的默认不通过。
type mockVerifier struct {
	pass map[contracts.ProviderId]bool
	err  map[contracts.ProviderId]bool
}

func (v mockVerifier) Verify(_ context.Context, op contracts.InFlightOp) (bool, error) {
	if v.err[op.ToolCallID] {
		return false, errors.New("verify injected error")
	}
	return v.pass[op.ToolCallID], nil
}

// dispositionOf 从报告里取某 tool_call_id 的处置结论。
func dispositionOf(t *testing.T, r durable.ResumeReport, tcID string) contracts.ResumeDisposition {
	t.Helper()
	for _, d := range r.Dispositions {
		if string(d.ToolCallID) == tcID {
			return d.Disposition
		}
	}
	t.Fatalf("报告里没有 %s 的处置（dispositions=%+v）", tcID, r.Dispositions)
	return ""
}

// TestResumeThreeCategories：safe→replayed、unsafe→manual、verify(通过)→verified 不重放。
func TestResumeThreeCategories(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	session := core.NewUlid()
	agent := core.NewUlid()
	seed := []contracts.Event{
		evIntent(session, agent, "c-safe", "echo", contracts.ReplayKindSafe, nil),
		evIntent(session, agent, "c-unsafe", "sendmail", contracts.ReplayKindUnsafe, nil),
		evIntent(session, agent, "c-verify", "push", contracts.ReplayKindVerify,
			[]contracts.Anchor{{Kind: "git-commit", Value: "deadbeef"}}),
	}
	if _, err := st.Append(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	rp := newMockReplayer(st)
	vf := mockVerifier{pass: map[contracts.ProviderId]bool{"c-verify": true}}
	rep, err := durable.Resume(ctx, st, session, durable.ResumeOptions{Verifier: vf, Replayer: rp})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	if got := dispositionOf(t, rep, "c-safe"); got != contracts.ResumeDispositionReplayed {
		t.Fatalf("c-safe disposition = %q, want replayed", got)
	}
	if got := dispositionOf(t, rep, "c-unsafe"); got != contracts.ResumeDispositionManual {
		t.Fatalf("c-unsafe disposition = %q, want manual", got)
	}
	if got := dispositionOf(t, rep, "c-verify"); got != contracts.ResumeDispositionVerified {
		t.Fatalf("c-verify disposition = %q, want verified", got)
	}

	// safe 重放执行 1 次并落 completed；unsafe 不执行；verify 通过**不**执行（不重放）。
	if rp.callN("c-safe") != 1 {
		t.Fatalf("c-safe 重放次数 = %d, want 1", rp.callN("c-safe"))
	}
	if rp.callN("c-unsafe") != 0 {
		t.Fatalf("c-unsafe 不应重放，得到 %d", rp.callN("c-unsafe"))
	}
	if rp.callN("c-verify") != 0 {
		t.Fatalf("c-verify 核验通过不应重放，得到 %d", rp.callN("c-verify"))
	}
	// verify 通过由 durable 补了一条 completed（合成结果）。
	if n := st.countKind("tool.call.completed"); n != 2 {
		t.Fatalf("completed 事件数 = %d, want 2（safe 重放 + verify 合成）", n)
	}
	// session.resumed 落了一条。
	if rep.ResumedEventID == "" {
		t.Fatal("report 未记录 session.resumed 事件 id")
	}
	if n := st.countKind("session.resumed"); n != 1 {
		t.Fatalf("session.resumed 事件数 = %d, want 1", n)
	}
}

// TestResumeVerifyFailReplays：verify 核验**不通过** → 当作未完成，走重放。
func TestResumeVerifyFailReplays(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	session := core.NewUlid()
	agent := core.NewUlid()
	if _, err := st.Append(ctx, []contracts.Event{
		evIntent(session, agent, "c-verify", "push", contracts.ReplayKindVerify, nil),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rp := newMockReplayer(st)
	vf := mockVerifier{pass: map[contracts.ProviderId]bool{}} // 不通过
	rep, err := durable.Resume(ctx, st, session, durable.ResumeOptions{Verifier: vf, Replayer: rp})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := dispositionOf(t, rep, "c-verify"); got != contracts.ResumeDispositionReplayed {
		t.Fatalf("verify 不通过应重放，得到 %q", got)
	}
	if rp.callN("c-verify") != 1 {
		t.Fatalf("verify 不通过应重放 1 次，得到 %d", rp.callN("c-verify"))
	}
}

// TestResumeVerifyErrorGoesManual：核验过程出错 → fail-safe 不盲目重放，转 manual。
func TestResumeVerifyErrorGoesManual(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	session := core.NewUlid()
	agent := core.NewUlid()
	if _, err := st.Append(ctx, []contracts.Event{
		evIntent(session, agent, "c-verify", "push", contracts.ReplayKindVerify, nil),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rp := newMockReplayer(st)
	vf := mockVerifier{err: map[contracts.ProviderId]bool{"c-verify": true}}
	rep, err := durable.Resume(ctx, st, session, durable.ResumeOptions{Verifier: vf, Replayer: rp})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := dispositionOf(t, rep, "c-verify"); got != contracts.ResumeDispositionManual {
		t.Fatalf("核验出错应转 manual，得到 %q", got)
	}
	if rp.callN("c-verify") != 0 {
		t.Fatalf("核验出错不应重放，得到 %d", rp.callN("c-verify"))
	}
}

// TestResumeIdempotent：连跑两次不重复处置（safe/verify 已有结果 → 扫不到；unsafe 已在
// 已处置集合 → 跳过）。第二次 Dispositions 为空、无新副作用、无新 completed。
func TestResumeIdempotent(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	session := core.NewUlid()
	agent := core.NewUlid()
	if _, err := st.Append(ctx, []contracts.Event{
		evIntent(session, agent, "c-safe", "echo", contracts.ReplayKindSafe, nil),
		evIntent(session, agent, "c-unsafe", "sendmail", contracts.ReplayKindUnsafe, nil),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rp := newMockReplayer(st)
	vf := mockVerifier{}

	rep1, err := durable.Resume(ctx, st, session, durable.ResumeOptions{Verifier: vf, Replayer: rp})
	if err != nil {
		t.Fatalf("Resume#1: %v", err)
	}
	if len(rep1.Dispositions) != 2 {
		t.Fatalf("第一次应处置 2 项，得到 %d", len(rep1.Dispositions))
	}
	completedAfter1 := st.countKind("tool.call.completed")
	resumedAfter1 := st.countKind("session.resumed")
	safeCalls := rp.callN("c-safe")

	// 第二次：幂等。
	rep2, err := durable.Resume(ctx, st, session, durable.ResumeOptions{Verifier: vf, Replayer: rp})
	if err != nil {
		t.Fatalf("Resume#2: %v", err)
	}
	if len(rep2.Dispositions) != 0 {
		t.Fatalf("第二次不应重复处置，得到 %d: %+v", len(rep2.Dispositions), rep2.Dispositions)
	}
	if rp.callN("c-safe") != safeCalls {
		t.Fatalf("safe 被二次重放：%d → %d", safeCalls, rp.callN("c-safe"))
	}
	if st.countKind("tool.call.completed") != completedAfter1 {
		t.Fatalf("第二次新增了 completed：%d → %d", completedAfter1, st.countKind("tool.call.completed"))
	}
	if st.countKind("session.resumed") != resumedAfter1+1 {
		t.Fatalf("第二次应再落一条 session.resumed（空处置）：%d → %d", resumedAfter1, st.countKind("session.resumed"))
	}
	// c-unsafe 仍在 in-flight（无结果），但已被记为 manual、不再重复处置。
	if len(rep2.InFlight) != 1 || string(rep2.InFlight[0].ToolCallID) != "c-unsafe" {
		t.Fatalf("第二次 InFlight = %+v, want [c-unsafe]", rep2.InFlight)
	}
}

// TestResumeNoCheckpointFullRebuild：无 checkpoint 的 session → 全量重建（不报错）。
func TestResumeNoCheckpointFullRebuild(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	session := core.NewUlid()
	agent := core.NewUlid()
	if _, err := st.Append(ctx, []contracts.Event{
		evLedger(session, agent, []string{"g1"}),
		evIntent(session, agent, "c-safe", "echo", contracts.ReplayKindSafe, nil),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rp := newMockReplayer(st)
	rep, err := durable.Resume(ctx, st, session, durable.ResumeOptions{Replayer: rp})
	if err != nil {
		t.Fatalf("无 checkpoint 的 Resume 应全量重建而非报错: %v", err)
	}
	if rep.HadCheckpoint {
		t.Fatal("不应报告有 checkpoint")
	}
	if rep.FromCheckpointSeq != 0 {
		t.Fatalf("from_checkpoint_seq = %d, want 0", rep.FromCheckpointSeq)
	}
	// 全量重建仍 fold 出了 ledger 与 in-flight。
	if len(rep.Ledger.Goals) != 1 || rep.Ledger.Goals[0] != "g1" {
		t.Fatalf("ledger 未重建: %+v", rep.Ledger)
	}
	if dispositionOf(t, rep, "c-safe") != contracts.ResumeDispositionReplayed {
		t.Fatal("全量重建后 c-safe 应被重放")
	}
}

// TestResumeWithCheckpointUsesWatermark：有 checkpoint 时 from_checkpoint_seq 取其水位。
func TestResumeWithCheckpointUsesWatermark(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	session := core.NewUlid()
	agent := core.NewUlid()
	if _, err := st.Append(ctx, []contracts.Event{
		evLedger(session, agent, []string{"g1"}),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cp, err := durable.Create(ctx, st, session)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rep, err := durable.Resume(ctx, st, session, durable.ResumeOptions{})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if !rep.HadCheckpoint || rep.FromCheckpointSeq != cp.SeqWatermark {
		t.Fatalf("from_checkpoint_seq = %d (had=%v), want %d", rep.FromCheckpointSeq, rep.HadCheckpoint, cp.SeqWatermark)
	}
	if rep.SeqWatermark != cp.SeqWatermark {
		t.Fatalf("seq_watermark = %d, want %d", rep.SeqWatermark, cp.SeqWatermark)
	}
}
