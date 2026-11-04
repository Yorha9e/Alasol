package host

// 本文件是 G-f（**standby 待机不被整轮预算误杀**）的验收用例，两组：
//
//	① 软时钟本体（host/runctx.go）：到点取消 / Extend 只增不减 / 父取消可区分 /
//	   并发安全（-race）/ 取钟路径（派生 ctx 仍取得到钟）；
//	② 协议面整轮预算（promptRunTimeout）与 standby 待机的关系：
//	   待机**不被误杀**（预算豁免）、无 standby 的轮次**照常被掐**（豁免不是作废预算）、
//	   用户/hub 取消**不被豁免**（取消 ≠ 超预算，两者可区分）。
//
// 为什么是 internal（package host）：软时钟（newRunClock / runClock / runClockFrom）
// 与 standby 的时间口径（standbyMinute / standbyHardCap / standbyPollInterval）都是
// 包内符号——黑箱面既取不到钟，也没法把超时路径缩到毫秒级。

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/loop"
)

// ---------------------------------------------------------------------------
// 夹具
// ---------------------------------------------------------------------------

// newRunBudgetTestHub 装配一个**带整轮预算**（PromptRunTimeoutSec，秒）的 hub：
// 协议面的一轮 prompt 只有这么多秒（0 = 框架默认 600s）。
func newRunBudgetTestHub(t *testing.T, timeoutSec int, provider core.Provider) *Hub {
	t.Helper()
	cfg := Config{
		DBPath:              filepath.Join(t.TempDir(), "events.db"),
		Model:               "runctx-test",
		System:              "runctx-test",
		MaxTurns:            4,
		Allow:               []string{ToolStandby},
		Provider:            provider,
		CWD:                 t.TempDir(),
		PromptRunTimeoutSec: timeoutSec,
	}
	h, err := New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// standbyProviderCalls 读脚本 provider 的调用次数（同一把锁 → 与写入同步）。
func standbyProviderCalls(p *standbyScriptProvider, key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counts[key]
}

// standbyWaitProviderCalls 等 provider 的调用次数到达 want（超时即失败）。
func standbyWaitProviderCalls(t *testing.T, p *standbyScriptProvider, key string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if standbyProviderCalls(p, key) >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%v 内 provider 调用次数没到 %d（现值 %d）", timeout, want, standbyProviderCalls(p, key))
}

// standbyModelTurnTexts 取某 session 全部 model.turn 事件的正文。
func standbyModelTurnTexts(t *testing.T, h *Hub, sid contracts.Ulid) []string {
	t.Helper()
	var out []string
	for _, ev := range standbySessionEvents(t, h, sid) {
		if ev.Kind != loop.KindModelTurn {
			continue
		}
		var p contracts.ModelTurnPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("解 model.turn 载荷失败: %v", err)
		}
		out = append(out, p.Text)
	}
	return out
}

// runBudgetSlowProvider 是"慢慢说话"的 provider：只在 ctx 结束或延时到点才返回。
//
// 用来考整轮预算：**没有 standby 的轮次**该在预算到点处被掐断（而不是无限等）。
type runBudgetSlowProvider struct {
	delay time.Duration
}

// Run 等到延时到点或 ctx 结束（ctx 结束 → 原样把 ctx.Err() 交回去）。
func (p *runBudgetSlowProvider) Run(ctx context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	select {
	case <-time.After(p.delay):
		emit(core.ProviderEvent{Kind: "text.delta", Text: "慢了"})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{Text: "慢了"}, nil
	case <-ctx.Done():
		return core.ModelResult{}, ctx.Err()
	}
}

// ---------------------------------------------------------------------------
// ① 软时钟本体
// ---------------------------------------------------------------------------

// TestRunClockExpiresAtDeadline：到点即取消，且错误语义是**超预算**
// （errors.Is(…DeadlineExceeded) 为真 → 既有判 deadline 的代码不用改），
// 与"用户取消"（context.Canceled）可区分。
func TestRunClockExpiresAtDeadline(t *testing.T) {
	ctx, clock := newRunClock(context.Background(), 50*time.Millisecond)
	defer clock.Stop()
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("软时钟到点没取消 ctx")
	}
	err := ctx.Err()
	if err == nil {
		t.Fatal("ctx 结束后 Err() 该非空")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("超预算的 Err() = %v，want errors.Is(…DeadlineExceeded) 为真", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("超预算不该呈取消语义：%v", err)
	}
	if !clock.isExpired() {
		t.Fatal("到点后 isExpired() 该为真")
	}
}

// TestRunClockExtendPushesDeadline：Extend 把截止推后 → 原截止点过了也不掐；
// 新的截止到点才掐（豁免的机制本体）。
func TestRunClockExtendPushesDeadline(t *testing.T) {
	ctx, clock := newRunClock(context.Background(), 60*time.Millisecond)
	defer clock.Stop()
	clock.Extend(400 * time.Millisecond)
	// 原截止（60ms）早已过：没有豁免的话此刻 ctx 已死。
	time.Sleep(150 * time.Millisecond)
	if err := ctx.Err(); err != nil {
		t.Fatalf("Extend 后原截止点不该再掐死：%v", err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("延展后的截止也没到点")
	}
	if !clock.isExpired() {
		t.Fatal("延展后的截止到点该呈超预算语义")
	}
}

// TestRunClockExtendNeverShortens：Extend **只增不减**——负值不许把截止拉回来
// （否则一次误延展就能把整轮预算缩掉）。
//
// 同时钉住**暂停语义**（critic F6）：Extend(d) 的新截止 = **旧截止 + d**
// （不是 now+d）——待机占用多久，预算就往后挪多久。
func TestRunClockExtendNeverShortens(t *testing.T) {
	_, clock := newRunClock(context.Background(), 10*time.Second)
	defer clock.Stop()
	d0, _ := clock.deadlineVal()
	clock.Extend(-time.Hour) // 负值 → 不动
	d1, _ := clock.deadlineVal()
	if !d1.Equal(d0) {
		t.Fatalf("Extend 只增不减：截止从 %v 变成 %v", d0, d1)
	}
	clock.Extend(30 * time.Second)
	d2, _ := clock.deadlineVal()
	if !d2.After(d0) {
		t.Fatalf("Extend(30s) 该把截止推后：%v → %v", d0, d2)
	}
	// 暂停语义：新截止 = 旧截止 + 30s（**不是** now+30s——那样会"烧预算"）。
	if got, want := d2.Sub(d0), 30*time.Second; got != want {
		t.Fatalf("Extend 是暂停语义：新截止该 = 旧截止 + 30s（%v），得到 %v", want, got)
	}
}

// TestRunClockExtendAfterExpiryIsNoop：已收场（到点 / Stop）后 Extend 是 no-op
// ——ctx 已死，延展无意义（也不许把"已超预算"洗成"没超"）。
func TestRunClockExtendAfterExpiryIsNoop(t *testing.T) {
	ctx, clock := newRunClock(context.Background(), 30*time.Millisecond)
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("软时钟到点没取消 ctx")
	}
	d0, _ := clock.deadlineVal()
	clock.Extend(time.Hour)
	d1, _ := clock.deadlineVal()
	if !d1.Equal(d0) {
		t.Fatalf("已收场后 Extend 该是 no-op：%v → %v", d0, d1)
	}
}

// TestRunClockParentCancelIsNotBudget：父 ctx 取消（hub 关闭 / 显式取消）→ 呈
// **取消**语义，不是超预算（fail-close 靠它区分"没人要了"与"没时间了"）。
func TestRunClockParentCancelIsNotBudget(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	ctx, clock := newRunClock(parent, 10*time.Second)
	defer clock.Stop()
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("父取消没传导到软时钟的 ctx")
	}
	err := ctx.Err()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("父取消的 Err() = %v，want context.Canceled", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("父取消不该呈超预算语义：%v", err)
	}
	if clock.isExpired() {
		t.Fatal("父取消不是超预算：isExpired() 该为假")
	}
}

// TestRunClockStopIsCancelNotBudget：Stop（跑完收场）→ 取消语义，且幂等。
func TestRunClockStopIsCancelNotBudget(t *testing.T) {
	ctx, clock := newRunClock(context.Background(), 10*time.Second)
	clock.Stop()
	err := ctx.Err()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop 后的 Err() = %v，want context.Canceled", err)
	}
	if clock.isExpired() {
		t.Fatal("Stop 不是超预算：isExpired() 该为假")
	}
	clock.Stop() // 幂等
}

// TestRunClockConcurrentExtend：并发 Extend / 读截止 / 读 Err 不出数据竞争
// （-race 钉子；同时覆盖"到点回调与 Extend 并发"这条真实竞争面）。
func TestRunClockConcurrentExtend(t *testing.T) {
	ctx, clock := newRunClock(context.Background(), 40*time.Millisecond)
	defer clock.Stop()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				clock.Extend(time.Millisecond)
				_, _ = clock.deadlineVal()
				_ = clock.isExpired()
				_ = ctx.Err()
			}
		}()
	}
	wg.Wait()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("并发 Extend 后软时钟没到点")
	}
}

// TestRunClockFromContext：取钟路径——本轮 ctx 上取得到；**派生** ctx（loop 与工具
// 执行器拿到的都是派生 ctx）上同样取得到；取不到的路径（CLI / ACP / 直调 RunPrompt）
// → nil（调用方按"不延展"处理，行为同现在）。
func TestRunClockFromContext(t *testing.T) {
	ctx, clock := newRunClock(context.Background(), time.Minute)
	defer clock.Stop()
	if got := runClockFrom(ctx); got != clock {
		t.Fatalf("runClockFrom(ctx) = %p，want %p", got, clock)
	}
	derived, cancel := context.WithCancel(ctx)
	defer cancel()
	if got := runClockFrom(derived); got != clock {
		t.Fatalf("派生 ctx 上取不到钟：%p", got)
	}
	withEnv := core.WithEnv(derived, core.EnvScope{})
	if got := runClockFrom(withEnv); got != clock {
		t.Fatalf("注入执行 env 的 ctx 上取不到钟：%p", got)
	}
	if got := runClockFrom(context.Background()); got != nil {
		t.Fatalf("普通 ctx 上不该取到钟：%p", got)
	}
	if got := runClockFrom(nil); got != nil {
		t.Fatalf("nil ctx 上不该取到钟：%p", got)
	}
}

// TestRunClockDeadlineFollowsExtend：Deadline() 报告**当前**软截止（Extend 后随之
// 推后；父 deadline 更早时以父为准）——审批枚举面与派生 ctx 的 min 口径都吃它。
func TestRunClockDeadlineFollowsExtend(t *testing.T) {
	ctx, clock := newRunClock(context.Background(), 10*time.Second)
	defer clock.Stop()
	d0, ok := ctx.Deadline()
	if !ok {
		t.Fatal("软时钟该报 deadline")
	}
	if left := time.Until(d0); left < 9*time.Second || left > 11*time.Second {
		t.Fatalf("deadline 口径不对：还剩 %v（want ~10s）", left)
	}
	clock.Extend(30 * time.Second)
	d1, ok := ctx.Deadline()
	if !ok {
		t.Fatal("Extend 后该仍报 deadline")
	}
	if !d1.After(d0) {
		t.Fatalf("Extend 后 deadline 该推后：%v → %v", d0, d1)
	}
	// 父 deadline 更早 → 以父为准（不越权承诺更长的预算）。
	parent, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pctx, pclock := newRunClock(parent, time.Hour)
	defer pclock.Stop()
	pd, ok := pctx.Deadline()
	if !ok {
		t.Fatal("该报 deadline")
	}
	if left := time.Until(pd); left > 3*time.Second {
		t.Fatalf("父 deadline 更早时该以父为准：还剩 %v", left)
	}
}

// ---------------------------------------------------------------------------
// ② 协议面整轮预算 × standby 待机
// ---------------------------------------------------------------------------

// TestStandbyWaitExemptFromRunBudget（G-f 主用例）：协议面整轮预算只有 2 秒，
// standby 要等 4 秒——待机**不被整轮预算误杀**（预算豁免：待机时长另计），
// 如实收口后**本轮还能继续跑**（下一次模型调用照常发生）。
//
// 没有豁免时的失败形态（钉住对照）：2 秒处 ctx 到点 → 待机被掐 → 落
// tool.call.failed（"等待被取消"），没有 completed，本轮也不再继续。
func TestStandbyWaitExemptFromRunBudget(t *testing.T) {
	standbyTimeKnobs(t, 4*time.Second, 30*time.Second, 100*time.Millisecond)
	provider := &standbyScriptProvider{
		plans: map[string][]standbyStep{
			"waiter": {
				{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`},
				{text: "收尾"},
			},
		},
		counts: map[string]int{},
	}
	h := newRunBudgetTestHub(t, 2, provider) // 整轮预算 2 秒 < 待机 4 秒
	sid := NewSession()
	start := time.Now()
	if !h.startPromptAsync(sid, "waiter") {
		t.Fatal("startPromptAsync 应受理")
	}
	// 等这一轮收尾（第二次模型调用 = 收尾那一轮）。
	standbyWaitProviderCalls(t, provider, "waiter", 2, 20*time.Second)
	elapsed := time.Since(start)

	// 断言①：待机**等满了**（没在 2 秒预算处被掐断）。
	if elapsed < 3500*time.Millisecond {
		t.Fatalf("待机被提前掐断：耗时 %v（standby 应等满 4 秒）", elapsed)
	}
	// 断言②：standby 如实收口（completed），不是被取消（failed）。
	intents, completed, failed := standbyToolCalls(t, h, sid)
	if len(intents) != 1 {
		t.Fatalf("tool.call.intent 应恰好 1 条，得到 %d", len(intents))
	}
	if len(completed) != 1 {
		t.Fatalf("豁免生效时待机该落 1 条 completed，得到 %d（failed %d 条）", len(completed), len(failed))
	}
	if len(failed) != 0 {
		t.Fatalf("豁免后不该有 failed：%s", standbyFailedText(t, h, sid))
	}
	if text := standbyCompletedText(t, h, sid); !strings.Contains(text, "未等到") {
		t.Fatalf("completed 文案 = %q，want 含「未等到」（超时如实收口）", text)
	}
	// 断言③：收口后**本轮继续跑**（下一次模型调用发生，且它的答复进了事件流）。
	if got := standbyProviderCalls(provider, "waiter"); got != 2 {
		t.Fatalf("provider 调用次数 = %d，want 2（待机收口后还要再调一次模型）", got)
	}
	texts := standbyModelTurnTexts(t, h, sid)
	found := false
	for _, txt := range texts {
		if strings.Contains(txt, "收尾") {
			found = true
		}
	}
	if !found {
		t.Fatalf("事件流里没有收尾那一轮的 model.turn（%v）", texts)
	}
}

// TestStandbyExtendsRunBudgetAsPause（critic F6 的验收口径）：预算 60 秒、待机
// 300ms → 待机**之后**的截止相对**旧截止**后移 ≥ 待机时长 + 余量
// （standbyResumeMargin）。
//
// 这钉的是"预算豁免 = 真**暂停**"：旧口径（Extend = max(当前, now+d)）在预算充足时
// 只把截止推到 now+d（≈没动）→ 待机期间预算**照样在烧** → 这一轮的预算被待机吃掉了。
// 新的 Extend 语义是"从**当前截止**后移 d"：待机占掉多久，预算就往后挪多久。
func TestStandbyExtendsRunBudgetAsPause(t *testing.T) {
	const waitDur = 300 * time.Millisecond
	standbyTimeKnobs(t, waitDur, 5*time.Second, 20*time.Millisecond)
	provider := &standbyScriptProvider{
		plans: map[string][]standbyStep{
			"waiter": {
				{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`},
				{text: "收尾"},
			},
		},
		counts: map[string]int{},
	}
	h := newRunBudgetTestHub(t, 60, provider) // 整轮预算 60 秒（充足 → 旧口径≈没动）
	sid := NewSession()
	ctx, clock := newRunClock(context.Background(), 60*time.Second)
	defer clock.Stop()
	d0, _ := clock.deadlineVal()
	if _, err := h.RunPrompt(ctx, sid, "waiter"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	d1, _ := clock.deadlineVal()
	shifted := d1.Sub(d0)
	want := waitDur + standbyResumeMargin
	if shifted < want {
		t.Fatalf("预算豁免该是暂停语义：截止后移 %v，want ≥ 待机 %v + 余量 %v = %v",
			shifted, waitDur, standbyResumeMargin, want)
	}
}

// TestStandbyBudgetPauseAccumulates（critic F6）：**多次** standby 的豁免**累积**
// （每一轮各延展一次"应等时长 + 余量"），不是只认第一次。
func TestStandbyBudgetPauseAccumulates(t *testing.T) {
	const waitDur = 300 * time.Millisecond
	standbyTimeKnobs(t, waitDur, 5*time.Second, 20*time.Millisecond)
	provider := &standbyScriptProvider{
		plans: map[string][]standbyStep{
			"waiter": {
				{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`},
				{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`},
				{text: "收尾"},
			},
		},
		counts: map[string]int{},
	}
	h := newRunBudgetTestHub(t, 60, provider)
	sid := NewSession()
	ctx, clock := newRunClock(context.Background(), 60*time.Second)
	defer clock.Stop()
	d0, _ := clock.deadlineVal()
	if _, err := h.RunPrompt(ctx, sid, "waiter"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	d1, _ := clock.deadlineVal()
	shifted := d1.Sub(d0)
	want := 2*waitDur + 2*standbyResumeMargin
	if shifted < want {
		t.Fatalf("两次 standby 的豁免该累积：截止后移 %v，want ≥ %v", shifted, want)
	}
}

// standbyWakeRun 跑一轮 standby（prompt 选剧本），等它挂起后立刻发消息把它叫醒，
// 返回这一轮的耗时（**早醒**场景用：计划等很久、实际秒级醒）。
func standbyWakeRun(t *testing.T, h *Hub, ctx context.Context, sidPeer, sidWait contracts.Ulid, prompt, msg string) time.Duration {
	t.Helper()
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		_, err := h.RunPrompt(ctx, sidWait, prompt)
		done <- err
	}()
	standbyWaitWaiting(t, h, sidWait, true)
	if _, err := h.Broadcast(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.BroadcastRequest{
		Target: standbyGateTarget(standbyGate),
		Text:   msg,
	}); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunPrompt(%s): %v", prompt, err)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("%s 挂死了", prompt)
	}
	return time.Since(start)
}

// TestStandbyEarlyWakeDoesNotInflateBudget（critic P2）：**早醒**的 standby 不该把
// 整轮预算撑大——计划等 1 分钟、实际几十毫秒就被消息唤醒 → 结束后截止 ≈ 开始截止 +
// **实占** + 余量（**不是** +1min+余量）。
//
// 口径：**预借**（planned+余量，保证等得到）+ **实占结算**（结束后把多借的退回）。
// 只预借不结算 → timeout_min=1min 却 50ms 醒的 standby 照样把截止推后 1min+余量。
func TestStandbyEarlyWakeDoesNotInflateBudget(t *testing.T) {
	// 计划等 1 分钟（timeout_min=1 × standbyMinute=1 分钟）；硬上限 30 分钟。
	standbyTimeKnobs(t, time.Minute, 30*time.Minute, 20*time.Millisecond)
	provider := &standbyScriptProvider{
		plans: map[string][]standbyStep{
			"peer":   {{text: "在"}},
			"hello":  {{text: "我先热身"}},
			"waiter": {{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`}, {text: "收尾"}},
		},
		counts: map[string]int{},
	}
	h := newRunBudgetTestHub(t, 60, provider)
	base := context.Background()
	sidPeer := standbyStartPeer(t, h, base, "peer", standbyGate)
	sidWait := NewSession()
	if _, err := h.RunPrompt(base, sidWait, "hello"); err != nil {
		t.Fatalf("RunPrompt(hello): %v", err)
	}
	if _, err := h.JoinGate(base, sidWait, standbyGate); err != nil {
		t.Fatalf("JoinGate(waiter): %v", err)
	}

	// 软时钟：预算 60 秒。
	runCtx, clock := newRunClock(context.Background(), 60*time.Second)
	defer clock.Stop()
	d0, _ := clock.deadlineVal()

	roundElapsed := standbyWakeRun(t, h, runCtx, sidPeer, sidWait, "waiter", "马上到")

	d1, _ := clock.deadlineVal()
	shifted := d1.Sub(d0)
	// 结算后：截止 ≈ 开始截止 + 实占 + 余量（计划时长不参与）。
	maxShift := roundElapsed + standbyResumeMargin + 2*time.Second
	if shifted > maxShift {
		t.Fatalf("早醒的 standby 不该把预算撑大：截止后移 %v，want ≤ 实耗 %v + 余量 %v",
			shifted, roundElapsed, standbyResumeMargin)
	}
	// 下限保护：结算后至少留出余量（收尾那一轮模型调用要有预算）。
	if shifted < standbyResumeMargin {
		t.Fatalf("结算后至少要留出余量：截止后移 %v，want ≥ %v", shifted, standbyResumeMargin)
	}
}

// TestStandbyMultipleWaitsDoNotInflateBudget（critic P2）：**多次** standby 的预算
// 结算按**实占**累积，不按"计划时长"累积（每次早醒 → 每次只结算实占 + 余量）。
func TestStandbyMultipleWaitsDoNotInflateBudget(t *testing.T) {
	standbyTimeKnobs(t, time.Minute, 30*time.Minute, 20*time.Millisecond)
	provider := &standbyScriptProvider{
		plans: map[string][]standbyStep{
			"peer":    {{text: "在"}},
			"hello":   {{text: "我先热身"}},
			"waiter1": {{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`}, {text: "收尾一"}},
			"waiter2": {{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`}, {text: "收尾二"}},
		},
		counts: map[string]int{},
	}
	h := newRunBudgetTestHub(t, 60, provider)
	base := context.Background()
	sidPeer := standbyStartPeer(t, h, base, "peer", standbyGate)
	sidWait := NewSession()
	if _, err := h.RunPrompt(base, sidWait, "hello"); err != nil {
		t.Fatalf("RunPrompt(hello): %v", err)
	}
	if _, err := h.JoinGate(base, sidWait, standbyGate); err != nil {
		t.Fatalf("JoinGate(waiter): %v", err)
	}

	runCtx, clock := newRunClock(context.Background(), 60*time.Second)
	defer clock.Stop()
	d0, _ := clock.deadlineVal()

	el1 := standbyWakeRun(t, h, runCtx, sidPeer, sidWait, "waiter1", "第一条")
	el2 := standbyWakeRun(t, h, runCtx, sidPeer, sidWait, "waiter2", "第二条")

	d1, _ := clock.deadlineVal()
	shifted := d1.Sub(d0)
	roundElapsed := el1 + el2
	maxShift := roundElapsed + 2*standbyResumeMargin + 2*time.Second
	if shifted > maxShift {
		t.Fatalf("多次 standby 的结算该按实占累积：截止后移 %v，want ≤ 实耗 %v + 2×余量 %v",
			shifted, roundElapsed, standbyResumeMargin)
	}
	if shifted < 2*standbyResumeMargin {
		t.Fatalf("两次 standby 至少要各留一份余量：截止后移 %v，want ≥ %v", shifted, 2*standbyResumeMargin)
	}
}

// TestRunBudgetStillCutsRoundWithoutStandby：**没有 standby 的轮次照常被整轮预算掐**
// （豁免只作用于待机，不是把预算作废），且错误语义是超预算（DeadlineExceeded），
// 不是取消。
func TestRunBudgetStillCutsRoundWithoutStandby(t *testing.T) {
	h := newRunBudgetTestHub(t, 2, &runBudgetSlowProvider{delay: 30 * time.Second})
	sid := NewSession()
	ctx, clock := newRunClock(context.Background(), 2*time.Second)
	defer clock.Stop()
	start := time.Now()
	_, err := h.RunPrompt(ctx, sid, "慢一点")
	if err == nil {
		t.Fatal("整轮预算耗尽该报错")
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("预算该在 2 秒处掐断，实际耗时 %v", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("错误 = %v，want errors.Is(…DeadlineExceeded)（超预算语义）", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("超预算不该呈取消语义：%v", err)
	}
}

// TestProtocolPromptBudgetCutsWithoutStandby（协议面接线）：协议面 prompt 走
// runPromptAsync 的软时钟——没有 standby 时**照常**被整轮预算掐（日志里是"整轮预算
// 耗尽"，不是"取消"）。钉的是"软时钟接进了协议面"这条接线。
func TestProtocolPromptBudgetCutsWithoutStandby(t *testing.T) {
	var buf safeLogBuf
	old := hubLog
	hubLog = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	defer func() { hubLog = old }()

	h := newRunBudgetTestHub(t, 2, &runBudgetSlowProvider{delay: 30 * time.Second})
	if !h.startPromptAsync(NewSession(), "慢一点") {
		t.Fatal("startPromptAsync 应受理")
	}
	waitLogContains(t, &buf, "协议面 prompt 跑失败", 20*time.Second)
	if s := buf.String(); !strings.Contains(s, "整轮预算耗尽") {
		t.Fatalf("日志该说明是整轮预算耗尽：\n%s", s)
	}
}

// TestRunBudgetCancelNotExempt：豁免**不豁免取消**——待机中把父 ctx 取消 → 这一轮
// 立刻中止（tool.call.failed，文案说"取消"），错误语义是取消（context.Canceled），
// 与超预算可区分（fail-close 靠这个区分"没人要了"与"没时间了"）。
func TestRunBudgetCancelNotExempt(t *testing.T) {
	standbyTimeKnobs(t, 10*time.Second, 30*time.Second, 50*time.Millisecond)
	provider := &standbyScriptProvider{
		plans: map[string][]standbyStep{
			"waiter": {
				{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`},
				{text: "收尾"},
			},
		},
		counts: map[string]int{},
	}
	h := newRunBudgetTestHub(t, 2, provider)
	sid := NewSession()
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, clock := newRunClock(parent, 2*time.Second)
	defer clock.Stop()
	done := make(chan error, 1)
	go func() {
		_, err := h.RunPrompt(ctx, sid, "waiter")
		done <- err
	}()
	// 等待机真的挂上了（此刻它已把截止延展到 ~20 秒——豁免生效中）。
	standbyWaitWaiting(t, h, sid, true)

	cancelParent()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("取消后这一轮该报错")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("错误 = %v，want context.Canceled（取消 ≠ 超预算）", err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("取消不该呈超预算语义：%v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("取消后 standby 挂死了")
	}
	// X1：有意图就要有结果——取消走 failed（不是 completed）。
	_, completed, failed := standbyToolCalls(t, h, sid)
	if len(failed) != 1 {
		t.Fatalf("取消路径该落 1 条 tool.call.failed，得到 %d", len(failed))
	}
	if len(completed) != 0 {
		t.Fatalf("取消路径不该有 completed，得到 %d 条", len(completed))
	}
	if msg := standbyFailedText(t, h, sid); !strings.Contains(msg, "取消") {
		t.Fatalf("failed 文案 %q 应说明是取消", msg)
	}
	// waiting 态配平（取消也要 EndWait）。
	standbyWaitWaiting(t, h, sid, false)
}
