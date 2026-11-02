package loop_test

// 缺口 5 第二件（B：provider 重试留痕进 WAL）的 loop 侧钉子：emitFunc 收到
// provider.retry → walAppend 一条 turn.retry（fake sink 收到载荷）；sink nil 不炸；
// 留痕**纯观测**（不进事件流、不占 Result.Events、不改 Run 的控制流）。
//
// 口径（core/loop/wal.go 与 core/wal.go）：
//   - 写点在 X1 三段写路径**之外**（重试发生在 provider.Run 内部，本来就不在
//     intent→completed/failed 窗口里）；
//   - WAL 关（WalSink=nil）→ 零行为变化（一个字节都不多写）。

import (
	"context"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
)

// retryEmitProvider 每轮 emit 一条 provider.retry（模拟 provider 内部重试留痕）+
// 一条 text delta + done，然后正常返回——留痕不该改变最终结果。
type retryEmitProvider struct{ calls int }

func (p *retryEmitProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	emit(core.ProviderEvent{
		Kind:  core.KindProviderRetry,
		Retry: &core.RetryInfo{Attempt: 2, Error: "openai: HTTP 429 Too Many Requests: rate limited", Backoff: 150 * time.Millisecond},
	})
	emit(core.ProviderEvent{Kind: core.KindTextDelta, Text: "…"})
	emit(core.ProviderEvent{Kind: core.KindDone, Text: "成了"})
	return core.ModelResult{Text: "成了"}, nil
}

// TestWALTurnRetryOnProviderRetry（变异自证③）：fake sink 必须收到一条 turn.retry，
// 载荷 = turn/attempt/错误摘要/退避毫秒；序列上它夹在 turn.begin 与 turn.delta/end 之间
// （重试发生在 provider.Run 内部）。
//
// 钉子：把 emitFunc 里的 KindProviderRetry 分支去掉 → 本用例红。
func TestWALTurnRetryOnProviderRetry(t *testing.T) {
	h := newHarness(t)
	w := &recWAL{}
	prov := &retryEmitProvider{}

	res, err := h.loop(func(c *loop.Config) {
		c.Provider = prov
		c.WalSink = w
	}).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "成了" || res.Turns != 1 {
		t.Errorf("留痕不该改变结果: %+v", res)
	}
	// 留痕**不进事件流**：事件仍只有旁路观测三条（user.prompt / context.assembled /
	// model.turn），Result.Events 仍只计 context.assembled（=1）——没有因 retry 多出事件。
	if res.Events != 1 {
		t.Errorf("Events = %d，期望 1（只有 context.assembled；turn.retry 不进事件流）", res.Events)
	}
	if got, want := h.kinds(), []string{loop.KindUserPrompt, loop.KindContextAssembled, loop.KindModelTurn}; !equalStrings(got, want) {
		t.Errorf("事件 kind 序 = %v，want %v（provider.retry 不该落进事件流）", got, want)
	}

	kinds, _ := w.snapshot()
	want := []string{core.KindWalTurnBegin, core.KindWalTurnRetry, core.KindWalTurnDelta, core.KindWalTurnEnd}
	if !equalStrings(kinds, want) {
		t.Fatalf("WAL kind 序 = %v，want %v", kinds, want)
	}

	pls := payloadsOf(t, w, core.KindWalTurnRetry)
	if len(pls) != 1 {
		t.Fatalf("turn.retry 条数 = %d，期望 1", len(pls))
	}
	p := pls[0]
	if got := p["turn"]; got != float64(1) {
		t.Errorf("turn = %v，want 1", got)
	}
	if got := p["attempt"]; got != float64(2) {
		t.Errorf("attempt = %v，want 2", got)
	}
	errText, _ := p["error"].(string)
	if errText == "" || len(errText) > 1024 {
		t.Errorf("error 摘要应非空且 ≤1KB: %q", errText)
	}
	// backoff_ms = RetryInfo.Backoff.Milliseconds()（150ms → 150）。
	if got := p["backoff_ms"]; got != float64(150) {
		t.Errorf("backoff_ms = %v，want 150", got)
	}
}

// TestWALTurnRetryNilSinkNoPanic：WAL 关（sink nil）→ 收到 provider.retry 也不炸、
// 不多写任何东西（零行为变化口径）。
func TestWALTurnRetryNilSinkNoPanic(t *testing.T) {
	h := newHarness(t)
	prov := &retryEmitProvider{}

	res, err := h.loop(func(c *loop.Config) {
		c.Provider = prov
	}).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("nil sink 不该影响 Run: %v", err)
	}
	if res.Text != "成了" {
		t.Errorf("Text = %q", res.Text)
	}
	if prov.calls != 1 {
		t.Errorf("provider 被调 %d 次，期望 1", prov.calls)
	}
}
