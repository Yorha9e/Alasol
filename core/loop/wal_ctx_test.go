package loop_test

// WAL 在 **ctx 取消**路径上的收口钉子（F2）：Run 被取消 → WAL 以 **turn.failed** 收尾，
// 不是留下一条悬空的 turn.begin。
//
// # 为什么要有这组钉子
//
// 对账口径里"在飞轮次 = 有 turn.begin、无 turn.end **也无 turn.failed**"。模型调用失败
// （429/超时/端点报错）已经由 turn.failed 收口（见 core/loop/wal_test.go 的
// TestWALTurnFailedOnProviderError），但**ctx 取消**这条路径此前**没有钉子**——
// 一旦哪天有人把取消分支的 walTurnFailed 删掉（或漏写），WAL 里就只剩悬空的 turn.begin →
// 对账把它当成"崩溃点" → 恢复报告谎报"疑似崩溃残留"（**假阳性**：一次正常的取消/超时被
// 报成丢了东西，使用者据此以为要重跑恢复）。

import (
	"context"
	"errors"
	"testing"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
)

// cancelProvider 在第一次模型调用时**取消 ctx** 并返回 ctx.Err()（模拟 Run 被取消：
// 上游超时 / 用户中断 / 宿主关停）。
type cancelProvider struct {
	cancel context.CancelFunc
	calls  int
}

func (p *cancelProvider) Run(ctx context.Context, _ core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if p.calls == 1 {
		p.cancel()
		return core.ModelResult{}, ctx.Err()
	}
	return core.ModelResult{Text: "不该走到第二轮"}, nil
}

// TestWALTurnFailedOnContextCancel（F2）：Run 被 ctx 取消 → WAL 以 **turn.failed** 收尾
// （**没有** turn.end），且 Run 的错误是 context.Canceled（不被包装成"模型错误"）。
//
// 钉子：删掉取消分支里的 walTurnFailed → 本用例红（WAL 末尾变成 turn.begin）。
func TestWALTurnFailedOnContextCancel(t *testing.T) {
	h := newHarness(t)
	rec := &recWAL{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prov := &cancelProvider{cancel: cancel}

	_, err := h.loop(func(c *loop.Config) {
		c.WalSink = rec
		c.Provider = prov
	}).Run(ctx, "跑一轮")
	if err == nil {
		t.Fatal("ctx 被取消时 Run 应返错")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run 错误 = %v，want context.Canceled（取消不该被包装成模型错误）", err)
	}

	kinds, _ := rec.snapshot()
	if len(kinds) == 0 {
		t.Fatal("WAL 一条记录都没有（写点没接上？）")
	}
	if kinds[len(kinds)-1] != core.KindWalTurnFailed {
		t.Fatalf("WAL 应以 turn.failed 收尾（取消也是一次失败收口），got %v", kinds)
	}
	for _, k := range kinds {
		if k == core.KindWalTurnEnd {
			t.Fatalf("被取消的轮次不该有 turn.end，got %v", kinds)
		}
	}
	// 载荷：轮次号 + 取消原因（诊断用）。
	failed := payloadsOf(t, rec, core.KindWalTurnFailed)
	if len(failed) != 1 {
		t.Fatalf("turn.failed = %d 条，want 1", len(failed))
	}
	if got, ok := failed[0]["turn"].(float64); !ok || int(got) != 1 {
		t.Errorf("turn.failed.turn = %v，want 1", failed[0]["turn"])
	}
	if msg, _ := failed[0]["error"].(string); msg == "" {
		t.Error("turn.failed.error 不该为空（取消原因要留痕）")
	}
}

// TestWALNoTurnFailedWhenCancelledBeforeRun（F2 的另一半）：**Run 还没开始**就被取消
// （ctx 已死）→ 不该有任何轮次记录，更不该有 turn.failed（那一轮压根没开跑）。
//
// 为什么钉：turn.failed 的语义是"这一轮开了、没跑完"；ctx 已死时 loop 在**模型调用之前**
// 就返回了 → 写一条 turn.failed 会凭空造出"跑过一轮"的假象（对账里多一段）。
func TestWALNoTurnFailedWhenCancelledBeforeRun(t *testing.T) {
	h := newHarness(t)
	rec := &recWAL{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Run 之前就取消

	_, err := h.loop(func(c *loop.Config) { c.WalSink = rec }).Run(ctx, "跑一轮")
	if err == nil {
		t.Fatal("ctx 已死时 Run 应返错")
	}
	kinds, _ := rec.snapshot()
	for _, k := range kinds {
		if k == core.KindWalTurnFailed || k == core.KindWalTurnBegin {
			t.Fatalf("ctx 已死时不该开跑（不该有 turn.begin/turn.failed），got %v", kinds)
		}
	}
}
