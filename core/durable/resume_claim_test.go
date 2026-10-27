package durable_test

// 阶段 8 收尾的两条：
//
//   - P1：并发 Resume 不得把同一个 in-flight 重放两次（会话级原子认领）；
//   - P2：重放**失败**不得记成 replayed（显示像成功）→ 独立的 replayed_failed 结论。

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/durable"
)

// barrierStore 在「认领写」上设卡：等两个 Resume 都到齐才放行（制造**确定的**重叠，
// 否则两个 Resume 可能一个跑完另一个才开始，测不到并发路径）。
type barrierStore struct {
	core.Store
	mu      sync.Mutex
	arrived int
	release chan struct{}
}

func newBarrierStore(inner core.Store) *barrierStore {
	return &barrierStore{Store: inner, release: make(chan struct{})}
}

func (b *barrierStore) Append(ctx context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	if len(evs) == 1 && evs[0].Kind == "session.resume_claimed" {
		b.mu.Lock()
		b.arrived++
		if b.arrived == 2 {
			close(b.release)
		}
		b.mu.Unlock()
		select {
		case <-b.release:
		case <-time.After(3 * time.Second): // 兜底：别把测试挂死
		}
	}
	return b.Store.Append(ctx, evs)
}

// TestConcurrentResumeReplaysInFlightOnlyOnce：两个 Resume 并发处置同一 session →
// 同一个 in-flight 只被重放一次（另一方报「已有 resume 在进行」并退出）。
//
// 旧行为：幂等只靠"已写的 session.resumed"顺序判据 → 两个 Resume 都扫到同一个
// in-flight → verify 类工具有副作用（被重放两次）。
func TestConcurrentResumeReplaysInFlightOnlyOnce(t *testing.T) {
	inner := newMemStore()
	st := newBarrierStore(inner)
	sid := core.NewUlid()
	agent := core.NewUlid()
	if _, err := inner.Append(context.Background(), []contracts.Event{
		evIntent(sid, agent, "c1", "echo", contracts.ReplayKindSafe, nil),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	rp := newMockReplayer(inner)
	opts := durable.ResumeOptions{Replayer: rp}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = durable.Resume(context.Background(), st, sid, opts)
		}(i)
	}
	wg.Wait()

	// ① 同一个 in-flight 只被重放一次（verify 类工具有副作用，重放两次就是重复副作用）。
	if n := rp.callN("c1"); n != 1 {
		t.Fatalf("Replayer 被调用 %d 次（并发 resume 重复重放了同一个 in-flight），want 1", n)
	}
	// ② 一方成功、一方报「已有 resume 在进行」。
	var ok, busy int
	for _, err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, durable.ErrResumeInProgress):
			busy++
		default:
			t.Fatalf("意外的 error: %v", err)
		}
	}
	if ok != 1 || busy != 1 {
		t.Fatalf("应恰好一方成功、一方报「已有 resume 在进行」，得到 ok=%d busy=%d（errs=%v）", ok, busy, errs)
	}
}

// TestResumeAfterCompletionIsNotBlocked：正常完成后再跑一次 resume（顺序幂等）
// → 不被"已有 resume 在进行"挡住（认领已被 session.resumed 释放）。
func TestResumeAfterCompletionIsNotBlocked(t *testing.T) {
	st := newMemStore()
	sid := core.NewUlid()
	agent := core.NewUlid()
	if _, err := st.Append(context.Background(), []contracts.Event{
		evIntent(sid, agent, "c1", "echo", contracts.ReplayKindSafe, nil),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rp := newMockReplayer(st)
	opts := durable.ResumeOptions{Replayer: rp}

	if _, err := durable.Resume(context.Background(), st, sid, opts); err != nil {
		t.Fatalf("首次 resume: %v", err)
	}
	// 第二次：认领已释放 → 正常走（且不重复重放：op 已处置）。
	if _, err := durable.Resume(context.Background(), st, sid, opts); err != nil {
		t.Fatalf("二次 resume 不该被挡: %v", err)
	}
	if n := rp.callN("c1"); n != 1 {
		t.Fatalf("顺序跑两次不应重复重放，Replayer 调用 %d 次", n)
	}
}

// TestResumeForceTakesOverUnreleasedClaim：上一次 resume 崩在处置中途（认领没释放）
// → 默认报「已有 resume 在进行」；ForceResume 才能接管。
func TestResumeForceTakesOverUnreleasedClaim(t *testing.T) {
	st := newMemStore()
	sid := core.NewUlid()
	agent := core.NewUlid()
	if _, err := st.Append(context.Background(), []contracts.Event{
		evIntent(sid, agent, "c1", "echo", contracts.ReplayKindSafe, nil),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rp := newMockReplayer(st)

	// 模拟"崩在认领之后、session.resumed 之前"：手工写一条认领事件。
	claimRaw, _ := json.Marshal(contracts.ResumeClaimedPayload{SessionID: sid, AttemptID: core.NewUlid(), Round: 0})
	if _, err := st.Append(context.Background(), []contracts.Event{
		{Kind: "session.resume_claimed", SessionID: sid, AgentID: agent, Durable: true,
			Payload: claimRaw},
	}); err != nil {
		t.Fatalf("seed claim: %v", err)
	}

	// 默认：报「已有 resume 在进行」。
	_, err := durable.Resume(context.Background(), st, sid, durable.ResumeOptions{Replayer: rp})
	if !errors.Is(err, durable.ErrResumeInProgress) {
		t.Fatalf("未释放的旧认领应报 ErrResumeInProgress，得到 %v", err)
	}
	if rp.callN("c1") != 0 {
		t.Fatal("被挡住时不得处置任何 in-flight")
	}

	// 显式接管：处置继续。
	if _, err := durable.Resume(context.Background(), st, sid,
		durable.ResumeOptions{Replayer: rp, ForceResume: true}); err != nil {
		t.Fatalf("ForceResume 接管应成功: %v", err)
	}
	if rp.callN("c1") != 1 {
		t.Fatalf("接管后应处置一次，Replayer 调用 %d 次", rp.callN("c1"))
	}
}

// TestReplayFailureIsRecordedAsReplayedFailed（P2）：重放执行了但**失败** →
// 处置结论是 replayed_failed（不是 replayed），detail 带失败码。
func TestReplayFailureIsRecordedAsReplayedFailed(t *testing.T) {
	st := newMemStore()
	sid := core.NewUlid()
	agent := core.NewUlid()
	if _, err := st.Append(context.Background(), []contracts.Event{
		evIntent(sid, agent, "c1", "echo", contracts.ReplayKindSafe, nil),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rp := newMockReplayer(st)
	rp.failOn["c1"] = true // 重放执行了但失败

	rep, err := durable.Resume(context.Background(), st, sid, durable.ResumeOptions{Replayer: rp})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	var disp *contracts.InFlightDisposition
	for i := range rep.Dispositions {
		if rep.Dispositions[i].ToolCallID == "c1" {
			disp = &rep.Dispositions[i]
		}
	}
	if disp == nil {
		t.Fatalf("报告里没有 c1 的处置（dispositions=%+v）", rep.Dispositions)
	}
	if disp.Disposition != contracts.ResumeDispositionReplayedFailed {
		t.Fatalf("重放失败应记 replayed_failed，得到 %q", disp.Disposition)
	}
	if disp.Detail == nil || !strings.Contains(*disp.Detail, "tool_error") {
		t.Fatalf("detail 应带失败码，得到 %v", disp.Detail)
	}
	if disp.Detail == nil || !strings.Contains(*disp.Detail, "失败") {
		t.Fatalf("detail 应说明失败，得到 %v", disp.Detail)
	}
}
