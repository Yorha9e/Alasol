package retry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- 测试基建 ----

// fakeClock 是假时钟：记录每次退避的时长，不真睡。
//
// 为什么必须假：退避断言要验"节奏对不对"（几次、多长、封顶没），真睡会让单测变成
// 计时器测试（慢 + flaky）。
type fakeClock struct {
	mu    sync.Mutex
	slept []time.Duration
}

func (f *fakeClock) sleep(_ context.Context, d time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.slept = append(f.slept, d)
	return nil
}

func (f *fakeClock) sleeps() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]time.Duration, len(f.slept))
	copy(out, f.slept)
	return out
}

// errTransient 是测试用的"可重试"错误（哨兵，供 errors.Is 断言穿透）。
var errTransient = errors.New("boom: 瞬时故障")

// errPermanent 是测试用的"不可重试"错误。
var errPermanent = errors.New("boom: 请求本身错了")

// transient 是可重试分类（除了 errTransient 之外都不重试）。
func transient(err error) bool { return errors.Is(err, errTransient) }

// failingFn 造一个"前 fail 次返回 err，之后成功"的 fn，并记录调用次数。
func failingFn(fail int, err error) (func(context.Context) error, *int) {
	var calls int
	fn := func(context.Context) error {
		calls++
		if calls <= fail {
			return fmt.Errorf("第 %d 次: %w", calls, err)
		}
		return nil
	}
	return fn, &calls
}

// TestSleepCtx 直接钉住真实睡眠路径的三个形状（不注入假时钟时才走这里）。
func TestSleepCtx(t *testing.T) {
	if err := sleepCtx(context.Background(), 0); err != nil {
		t.Errorf("d=0 应立即返回：%v", err)
	}
	start := time.Now()
	if err := sleepCtx(context.Background(), 20*time.Millisecond); err != nil {
		t.Fatalf("err = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 10*time.Millisecond {
		t.Errorf("几乎没睡：%v", elapsed)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepCtx(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Errorf("已取消的 ctx 上睡该立即返回取消错误：%v", err)
	}
}

// ---- 成功路径 ----

func TestDoSuccessFirstAttempt(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(0, errTransient) // 一次都不失败
	if err := Do(context.Background(), Policy{MaxAttempts: 3, RetryIf: transient, sleep: fc.sleep}, fn); err != nil {
		t.Fatalf("Do = %v，期望 nil", err)
	}
	if *calls != 1 {
		t.Errorf("fn 被调用 %d 次，期望 1（成功不该重试）", *calls)
	}
	if got := fc.sleeps(); len(got) != 0 {
		t.Errorf("成功路径不该睡：%v", got)
	}
}

func TestDoRetryableThenSuccess(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(2, errTransient) // 前两次失败，第三次成功
	p := Policy{MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond, RetryIf: transient, sleep: fc.sleep}
	if err := Do(context.Background(), p, fn); err != nil {
		t.Fatalf("Do = %v，期望重试后成功", err)
	}
	if *calls != 3 {
		t.Errorf("fn 被调用 %d 次，期望 3", *calls)
	}
	if got := fc.sleeps(); len(got) != 2 {
		t.Errorf("睡了 %d 次，期望 2（每次失败后一次）：%v", len(got), got)
	}
}

// ---- 聚合错误 ----

func TestDoExhaustedAggregatesEveryAttempt(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(99, errTransient) // 永远失败
	p := Policy{MaxAttempts: 3, InitialBackoff: time.Millisecond, RetryIf: transient, sleep: fc.sleep}
	err := Do(context.Background(), p, fn)
	if err == nil {
		t.Fatal("重试耗尽必须报错")
	}
	if *calls != 3 {
		t.Errorf("fn 被调用 %d 次，期望 3（MaxAttempts 含第一次）", *calls)
	}

	var agg *Aggregated
	if !errors.As(err, &agg) {
		t.Fatalf("错误不是 *Aggregated：%T %v", err, err)
	}
	// 每条尝试都保留（M6：聚合错误要说清每次败在哪儿）。
	if len(agg.Attempts) != 3 {
		t.Fatalf("Attempts = %d，期望 3", len(agg.Attempts))
	}
	for i, a := range agg.Attempts {
		if a.N != i+1 {
			t.Errorf("Attempts[%d].N = %d，期望 %d（序号即调用次序）", i, a.N, i+1)
		}
		if a.Err == nil {
			t.Fatalf("Attempts[%d].Err 为 nil", i)
		}
		if !errors.Is(a.Err, errTransient) {
			t.Errorf("Attempts[%d].Err 丢了底层哨兵：%v", i, a.Err)
		}
	}
	// 聚合错误本身也要能判底层（errors.Is/As 穿透 Unwrap() []error）。
	if !errors.Is(err, errTransient) {
		t.Error("errors.Is(聚合错误, errTransient) 为假：底层没被保留")
	}
	if agg.Last() != agg.Attempts[2].Err {
		t.Error("Last() 不是最后一次尝试的错误")
	}
	// 文案要能数出次数、逐条列出（诊断用）。
	msg := err.Error()
	for _, want := range []string{"3 次尝试", "第 1 次", "第 2 次", "第 3 次", "boom: 瞬时故障"} {
		if !strings.Contains(msg, want) {
			t.Errorf("错误文案缺 %q：%s", want, msg)
		}
	}
	if len(fc.sleeps()) != 2 {
		t.Errorf("睡了 %d 次，期望 2（最后一次失败后不再睡）", len(fc.sleeps()))
	}
}

func TestDoExhaustedTwoAttemptsAggregates(t *testing.T) {
	fc := &fakeClock{}
	fn, _ := failingFn(99, errTransient)
	err := Do(context.Background(), Policy{MaxAttempts: 2, RetryIf: transient, sleep: fc.sleep}, fn)
	var agg *Aggregated
	if !errors.As(err, &agg) || len(agg.Attempts) != 2 {
		t.Fatalf("2 次尝试耗尽应聚合成 2 条：%T %v", err, err)
	}
}

func TestDoSingleAttemptReturnsRawError(t *testing.T) {
	// MaxAttempts=1（含零值 Policy）→ 没有"重试"发生 → 原样返回，不用聚合掩盖。
	fc := &fakeClock{}
	fn, calls := failingFn(99, errTransient)
	err := Do(context.Background(), Policy{RetryIf: transient, sleep: fc.sleep}, fn)
	if err == nil {
		t.Fatal("期望报错")
	}
	var agg *Aggregated
	if errors.As(err, &agg) {
		t.Errorf("单次尝试不该包成聚合错误：%v", err)
	}
	if !errors.Is(err, errTransient) {
		t.Errorf("errors.Is 丢失底层：%v", err)
	}
	if *calls != 1 {
		t.Errorf("fn 被调用 %d 次，期望 1", *calls)
	}
	if len(fc.sleeps()) != 0 {
		t.Errorf("不该睡：%v", fc.sleeps())
	}
}

// ---- fail-close 缺省（M7 钉子）----

// TestDoRetryIfNilRetriesNothing 是 fail-close 钉子：RetryIf 缺省 = 不重试任何错误。
// 若把缺省改成 RetryAll，本用例立刻红。
func TestDoRetryIfNilRetriesNothing(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(99, errTransient)
	// 故意不设 RetryIf：调用方没声明什么可重试 → 一次都不重试。
	err := Do(context.Background(), Policy{MaxAttempts: 5, InitialBackoff: time.Millisecond, sleep: fc.sleep}, fn)
	if err == nil {
		t.Fatal("期望报错")
	}
	if *calls != 1 {
		t.Errorf("RetryIf=nil 时 fn 被调用 %d 次，期望 1（fail-close：不声明不重试）", *calls)
	}
	var agg *Aggregated
	if errors.As(err, &agg) {
		t.Errorf("不该出现聚合错误（一次都没重试）：%v", err)
	}
	if len(fc.sleeps()) != 0 {
		t.Errorf("不该睡：%v", fc.sleeps())
	}
}

func TestDoZeroPolicyIsSingleShot(t *testing.T) {
	// 零值 Policy：MaxAttempts=0 → 1 次；RetryIf=nil → 不重试；InitialBackoff=0 → 不睡。
	fc := &fakeClock{}
	fn, calls := failingFn(99, errTransient)
	err := Do(context.Background(), Policy{sleep: fc.sleep}, fn)
	if err == nil || *calls != 1 || len(fc.sleeps()) != 0 {
		t.Fatalf("零值 Policy 应=单发：err=%v calls=%d sleeps=%v", err, *calls, fc.sleeps())
	}
}

func TestDoNonRetryableStopsImmediately(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(99, errPermanent) // 永远失败，但不可重试
	err := Do(context.Background(), Policy{MaxAttempts: 5, RetryIf: transient, sleep: fc.sleep}, fn)
	if err == nil {
		t.Fatal("期望报错")
	}
	if *calls != 1 {
		t.Errorf("不可重试错误被重试了：%d 次", *calls)
	}
	if !errors.Is(err, errPermanent) {
		t.Errorf("errors.Is 丢失底层：%v", err)
	}
	var agg *Aggregated
	if errors.As(err, &agg) {
		t.Errorf("不可重试错误不该包聚合：%v", err)
	}
}

func TestDoRetryThenNonRetryableAggregates(t *testing.T) {
	// 第 1 次可重试、第 2 次不可重试 → 返回**聚合**（M6："聚合错误含各次尝试"：
	// 前次的可重试失败不能被吞掉——"429 重试后拿到 400"这类事实在诊断里很值钱），
	// 但 errors.Is/As 仍能穿透到最后那个（不可重试的）错误。
	fc := &fakeClock{}
	var calls int
	fn := func(context.Context) error {
		calls++
		if calls == 1 {
			return errTransient
		}
		return errPermanent
	}
	err := Do(context.Background(), Policy{MaxAttempts: 5, RetryIf: transient, sleep: fc.sleep}, fn)
	var agg *Aggregated
	if !errors.As(err, &agg) {
		t.Fatalf("重试后撞上不可重试错误应返回聚合：%T %v", err, err)
	}
	if len(agg.Attempts) != 2 {
		t.Fatalf("聚合了 %d 条尝试，期望 2（前一次 + 最后一次）", len(agg.Attempts))
	}
	if !errors.Is(err, errPermanent) {
		t.Errorf("errors.Is 找不到最后的不可重试错误：%v", err)
	}
	if !errors.Is(err, errTransient) {
		t.Errorf("前次的可重试失败被吞掉了：%v", err)
	}
	if agg.Last() != errPermanent {
		t.Errorf("Last() = %v，期望最后一个错误", agg.Last())
	}
	if calls != 2 {
		t.Errorf("calls = %d，期望 2", calls)
	}
}

// TestDoFirstFailureNonRetryableStaysRaw 是上一条的对偶：第一次就失败且不可重试 →
// **原样**返回（不包聚合）：调用方要 errors.As 的就是那个错误，没有"前次尝试"可聚合。
func TestDoFirstFailureNonRetryableStaysRaw(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(99, errPermanent)
	err := Do(context.Background(), Policy{MaxAttempts: 5, RetryIf: transient, sleep: fc.sleep}, fn)
	if !errors.Is(err, errPermanent) {
		t.Fatalf("err = %v", err)
	}
	var agg *Aggregated
	if errors.As(err, &agg) {
		t.Errorf("第一次就失败不该包聚合：%v", err)
	}
	if *calls != 1 {
		t.Errorf("fn 被调用 %d 次，期望 1", *calls)
	}
}

// ---- Unwrap 次序（F4 钉子）----

// TestAggregatedUnwrapOrderNewestFirst 钉住 Aggregated.Unwrap 的次序：**最近一次在前**。
//
// 429（可重试）→ 400（不可重试）→ 聚合错误里 errors.As(*StatusError) 必须取到 **400**
// （Unwrap 序列中最近一次匹配的那个），不是 429。
//
// 把 Unwrap 改成"最早一次在前"（变异）→ 本用例立刻红（取到 429）。
func TestAggregatedUnwrapOrderNewestFirst(t *testing.T) {
	fc := &fakeClock{}
	var calls int
	fn := func(context.Context) error {
		calls++
		if calls == 1 {
			return &fakeStatusError{code: 429, msg: "rate limited"}
		}
		return &fakeStatusError{code: 400, msg: "bad request"}
	}
	retryIf429 := func(err error) bool {
		var se *fakeStatusError
		return errors.As(err, &se) && se.code == 429
	}
	err := Do(context.Background(), Policy{MaxAttempts: 5, RetryIf: retryIf429, sleep: fc.sleep}, fn)
	if err == nil {
		t.Fatal("期望报错")
	}
	var agg *Aggregated
	if !errors.As(err, &agg) {
		t.Fatalf("重试后撞上不可重试错误应返回聚合：%T %v", err, err)
	}
	if len(agg.Attempts) != 2 {
		t.Fatalf("聚合了 %d 条尝试，期望 2（前一次 + 最后一次）", len(agg.Attempts))
	}
	// Attempts 恒为**发生序**：第 1 次 429、第 2 次 400。
	for i, want := range []int{429, 400} {
		var se *fakeStatusError
		if !errors.As(agg.Attempts[i].Err, &se) || se.code != want {
			t.Errorf("Attempts[%d] 不是 %d：%v", i, want, agg.Attempts[i].Err)
		}
	}
	// errors.As 取"最近一次匹配"：Unwrap 最近一次在前 → 取到 400（不是 429）。
	var se *fakeStatusError
	if !errors.As(err, &se) {
		t.Fatalf("聚合错误里取不出 *fakeStatusError：%v", err)
	}
	if se.code != 400 {
		t.Errorf("errors.As 取到的是 %d，期望 400（Unwrap 必须最近一次在前）", se.code)
	}
}

// ---- ctx 取消 ----

func TestDoCtxAlreadyCancelledNeverRuns(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(0, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Do(ctx, Policy{MaxAttempts: 3, RetryIf: transient, sleep: fc.sleep}, fn)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v，期望 context.Canceled", err)
	}
	if err != context.Canceled {
		t.Errorf("取消错误被包装了：%v（调用方要能 err == context.Canceled）", err)
	}
	if *calls != 0 {
		t.Errorf("ctx 已取消还发了 %d 次尝试", *calls)
	}
}

func TestDoCancelDuringBackoffStops(t *testing.T) {
	// fn 第一次失败后把 ctx 取消 → 退避期间就停，不再发第二次。
	fc := &fakeClock{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	fn := func(context.Context) error {
		calls++
		cancel() // 模拟"调用方在重试间隔里取消"
		return errTransient
	}
	err := Do(ctx, Policy{MaxAttempts: 5, RetryIf: transient, sleep: fc.sleep}, fn)
	if calls != 1 {
		t.Fatalf("calls = %d，期望 1（取消后不再发起新尝试）", calls)
	}
	var agg *Aggregated
	if !errors.As(err, &agg) {
		t.Fatalf("错误不是 *Aggregated：%T %v", err, err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("取消事实没进错误面：%v", err)
	}
	if len(agg.Attempts) != 1 || agg.Interrupted == nil {
		t.Errorf("Attempts=%d Interrupted=%v，期望 1 条尝试 + 取消记录", len(agg.Attempts), agg.Interrupted)
	}
}

func TestDoCancelBetweenAttemptsStops(t *testing.T) {
	// ctx 在两次尝试**之间**被取消（不走退避，直接在看门狗那一步被发现）。
	fc := &fakeClock{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	fn := func(context.Context) error {
		calls++
		cancel()
		return errTransient
	}
	// MaxAttempts=2：第一次失败 → 退避（假时钟不等）→ 循环开头发现 ctx 已结束。
	err := Do(ctx, Policy{MaxAttempts: 2, RetryIf: transient, InitialBackoff: 0, sleep: fc.sleep}, fn)
	if calls != 1 {
		t.Fatalf("calls = %d，期望 1", calls)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v，期望能判出 context.Canceled", err)
	}
}

// TestDoRealSleeperIsInterruptible 走**真**睡眠路径（不注入假时钟）：验证退避可被
// ctx 及时打断（不是睡满整个 backoff 才返回）。
func TestDoRealSleeperIsInterruptible(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls int
	fn := func(context.Context) error {
		calls++
		cancel()
		return errTransient
	}
	start := time.Now()
	err := Do(ctx, Policy{MaxAttempts: 3, InitialBackoff: 2 * time.Second, RetryIf: transient}, fn)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("取消后仍睡了 %v（退避没被 ctx 打断）", elapsed)
	}
	if calls != 1 {
		t.Errorf("calls = %d，期望 1", calls)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
}

// ---- 退避节奏 ----

func TestDoBackoffScheduleExactWithoutJitter(t *testing.T) {
	fc := &fakeClock{}
	fn, _ := failingFn(99, errTransient)
	// 无抖动：100 → 200 → 400（MaxBackoff 封顶）。
	p := Policy{
		MaxAttempts:    4,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     400 * time.Millisecond,
		Multiplier:     2,
		RetryIf:        transient,
		sleep:          fc.sleep,
	}
	if err := Do(context.Background(), p, fn); err == nil {
		t.Fatal("期望耗尽报错")
	}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	if got := fc.sleeps(); !equalDurations(got, want) {
		t.Errorf("退避序列 = %v，期望 %v（指数 + 封顶）", got, want)
	}
}

func TestDoBackoffJitterWithinBounds(t *testing.T) {
	fc := &fakeClock{}
	fn, _ := failingFn(99, errTransient)
	p := Policy{
		MaxAttempts:    7,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     300 * time.Millisecond,
		Multiplier:     2,
		Jitter:         0.5,
		RetryIf:        transient,
		sleep:          fc.sleep,
	}
	if err := Do(context.Background(), p, fn); err == nil {
		t.Fatal("期望耗尽报错")
	}
	got := fc.sleeps()
	if len(got) != 6 {
		t.Fatalf("睡了 %d 次，期望 6", len(got))
	}
	base := 50 * time.Millisecond
	for i, d := range got {
		lo := time.Duration(float64(base) * 0.5)
		hi := time.Duration(float64(base) * 1.5)
		if d < lo || d > hi {
			t.Errorf("第 %d 次退避 %v 越界（期望 [%v, %v]，base=%v）", i+1, d, lo, hi, base)
		}
		if d > 300*time.Millisecond {
			t.Errorf("第 %d 次退避 %v 超过 MaxBackoff", i+1, d)
		}
		if d < 0 {
			t.Errorf("退避为负：%v", d)
		}
		base *= 2
		if base > 300*time.Millisecond {
			base = 300 * time.Millisecond
		}
	}
}

func TestDoBackoffMultiplierAtMostOneIsConstant(t *testing.T) {
	fc := &fakeClock{}
	fn, _ := failingFn(99, errTransient)
	p := Policy{
		MaxAttempts:    4,
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     time.Second,
		Multiplier:     0.5, // <=1 → 归一为 1（恒定退避，不该越睡越短）
		RetryIf:        transient,
		sleep:          fc.sleep,
	}
	if err := Do(context.Background(), p, fn); err == nil {
		t.Fatal("期望耗尽报错")
	}
	want := []time.Duration{20 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond}
	if got := fc.sleeps(); !equalDurations(got, want) {
		t.Errorf("退避序列 = %v，期望 %v", got, want)
	}
}

func TestDoBackoffZeroInitialNeverSleeps(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(99, errTransient)
	p := Policy{MaxAttempts: 3, InitialBackoff: 0, RetryIf: transient, sleep: fc.sleep}
	if err := Do(context.Background(), p, fn); err == nil {
		t.Fatal("期望耗尽报错")
	}
	if got := fc.sleeps(); len(got) != 0 {
		t.Errorf("InitialBackoff=0 不该睡：%v", got)
	}
	if *calls != 3 {
		t.Errorf("calls = %d，期望 3", *calls)
	}
}

// ---- Retry-After（服务端建议的等待）----

// retryAfterError 是"带服务端建议等待"的错误（形状：实现 RetryAfterCarrier）。
type retryAfterError struct {
	wait time.Duration
}

func (e *retryAfterError) Error() string { return "429 rate limited" }

// RetryAfter 是 core/retry 认的建议等待（结构形状接口）。
func (e *retryAfterError) RetryAfter() time.Duration { return e.wait }

// retryAfterDelayError 是同一份信息的**别名形状**（方法名带 Delay 后缀）。
type retryAfterDelayError struct {
	wait time.Duration
}

func (e *retryAfterDelayError) Error() string { return "503 overloaded" }

// RetryAfterDelay 同 retryAfterError.RetryAfter（core 两种形状都认）。
func (e *retryAfterDelayError) RetryAfterDelay() time.Duration { return e.wait }

// retryIfRateLimited 只重试"限流类"的错误（测试用的白名单）。
func retryIfRateLimited(err error) bool {
	var a *retryAfterError
	var b *retryAfterDelayError
	return errors.As(err, &a) || errors.As(err, &b)
}

// TestDoHonorsRetryAfter 是 F1 的钉子：429 + Retry-After: 2 → **实际等 ≥2s**，
// 而不是按 100ms/200ms 的自造节奏白烧（端点的限流窗口不会因为我们重试得快而缩短）。
//
// 把"采信 Retry-After"去掉（只按计算退避）→ 本用例立刻红（睡的是 100ms）。
func TestDoHonorsRetryAfter(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(1, &retryAfterError{wait: 2 * time.Second})
	p := Policy{
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		Multiplier:     2,
		RetryIf:        retryIfRateLimited,
		sleep:          fc.sleep,
	}
	if err := Do(context.Background(), p, fn); err != nil {
		t.Fatalf("Do = %v，期望重试后成功", err)
	}
	if *calls != 2 {
		t.Errorf("fn 被调用 %d 次，期望 2", *calls)
	}
	got := fc.sleeps()
	if len(got) != 1 {
		t.Fatalf("睡了 %d 次，期望 1", len(got))
	}
	if got[0] < 2*time.Second {
		t.Errorf("退避 %v，期望 ≥2s（Retry-After: 2 必须被采信）", got[0])
	}
}

// TestDoRetryAfterAliasShape 被采信的是"别名形状"（RetryAfterDelay）时同样要生效——
// provider 侧的错误类型常把建议时长存成同名字段，只能换个方法名暴露。
func TestDoRetryAfterAliasShape(t *testing.T) {
	fc := &fakeClock{}
	fn, _ := failingFn(1, &retryAfterDelayError{wait: 3 * time.Second})
	p := Policy{MaxAttempts: 3, InitialBackoff: 100 * time.Millisecond, RetryIf: retryIfRateLimited, sleep: fc.sleep}
	if err := Do(context.Background(), p, fn); err != nil {
		t.Fatalf("Do = %v", err)
	}
	got := fc.sleeps()
	if len(got) != 1 || got[0] < 3*time.Second {
		t.Errorf("退避 = %v，期望 ≥3s（别名形状也要被采信）", got)
	}
}

// TestDoWithoutRetryAfterKeepsNormalBackoff 是 F1 的对偶钉子：没有 Retry-After →
// **照旧退避**（不能因为加了这条口径就把所有等待都拉长）。
func TestDoWithoutRetryAfterKeepsNormalBackoff(t *testing.T) {
	fc := &fakeClock{}
	fn, _ := failingFn(1, errTransient)
	p := Policy{
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		Multiplier:     2,
		RetryIf:        transient,
		sleep:          fc.sleep,
	}
	if err := Do(context.Background(), p, fn); err != nil {
		t.Fatalf("Do = %v", err)
	}
	got := fc.sleeps()
	if len(got) != 1 {
		t.Fatalf("睡了 %d 次，期望 1", len(got))
	}
	if got[0] != 100*time.Millisecond {
		t.Errorf("退避 %v，期望 100ms（没有 Retry-After 时照旧退避）", got[0])
	}
}

// TestDoRetryAfterWithinMaxBackoffIsHonored 是等待口径的**采信**那一半：Retry-After
// ≤ MaxBackoff → 实际等待 = Retry-After（不是自造的 100ms 节奏）。
//
// 把"采信 Retry-After"去掉（只按计算退避）→ 本用例立刻红（睡的是 100ms）。
func TestDoRetryAfterWithinMaxBackoffIsHonored(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(1, &retryAfterError{wait: 2 * time.Second})
	p := Policy{
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		RetryIf:        retryIfRateLimited,
		sleep:          fc.sleep,
	}
	if err := Do(context.Background(), p, fn); err != nil {
		t.Fatalf("Do = %v", err)
	}
	if *calls != 2 {
		t.Errorf("fn 被调用 %d 次，期望 2", *calls)
	}
	got := fc.sleeps()
	if len(got) != 1 {
		t.Fatalf("睡了 %d 次，期望 1", len(got))
	}
	if got[0] != 2*time.Second {
		t.Errorf("退避 %v，期望 2s（Retry-After ≤ MaxBackoff → 等待被采信）", got[0])
	}
}

// TestDoRetryAfterOverMaxBackoffStopsRetrying 是 F1 的钉子（core/retry 侧）：服务端要的
// 窗口（30s）**超过**我们的等待上限（MaxBackoff=1s）→ **不重试**（注定撞窗，还会加剧
// 限流），并把窗口抛给调用方（错误里能读到 RetryAfter）。
//
// 把"RA > 封顶就停重试"这条去掉（照旧按封顶后的 1s 等下再试）→ 本用例立刻红
// （fn 被调 3 次）。
func TestDoRetryAfterOverMaxBackoffStopsRetrying(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(99, &retryAfterError{wait: 30 * time.Second})
	p := Policy{
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     time.Second,
		RetryIf:        retryIfRateLimited,
		sleep:          fc.sleep,
	}
	err := Do(context.Background(), p, fn)
	if err == nil {
		t.Fatal("期望报错")
	}
	if *calls != 1 {
		t.Errorf("fn 被调用 %d 次，期望 1（RA 30s > MaxBackoff 1s → 不重试）", *calls)
	}
	if got := fc.sleeps(); len(got) != 0 {
		t.Errorf("不该睡（根本没重试）：%v", got)
	}
	// 停重试仍返回聚合错误：这次尝试的事实（状态码 + 窗口）不能被吞掉。
	var agg *Aggregated
	if !errors.As(err, &agg) {
		t.Fatalf("停重试应返回聚合错误：%T %v", err, err)
	}
	if len(agg.Attempts) != 1 {
		t.Errorf("聚合了 %d 条尝试，期望 1", len(agg.Attempts))
	}
	// 窗口要抛给调用方：错误链里能读到 RetryAfter（30s）。
	var ra *retryAfterError
	if !errors.As(err, &ra) {
		t.Fatalf("错误链里读不到 RetryAfter 载体：%v", err)
	}
	if ra.wait != 30*time.Second {
		t.Errorf("RetryAfter = %v，期望 30s（窗口没抛给调用方）", ra.wait)
	}
}

// TestDoRetryAfterEqualMaxBackoffStillRetries 钉住边界：Retry-After **等于** MaxBackoff
// 不算超限（等得了 → 照采信、照重试）。把判定从 ">" 改成 ">=" → 本用例立刻红。
func TestDoRetryAfterEqualMaxBackoffStillRetries(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(1, &retryAfterError{wait: time.Second})
	p := Policy{
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     time.Second,
		RetryIf:        retryIfRateLimited,
		sleep:          fc.sleep,
	}
	if err := Do(context.Background(), p, fn); err != nil {
		t.Fatalf("Do = %v", err)
	}
	if *calls != 2 {
		t.Errorf("fn 被调用 %d 次，期望 2（RA == MaxBackoff → 照采信、照重试）", *calls)
	}
	got := fc.sleeps()
	if len(got) != 1 || got[0] != time.Second {
		t.Errorf("退避 = %v，期望恰好 1s", got)
	}
}

// TestDoSingleAttemptWithRetryAfterKeepsRawError 钉住"MaxAttempts=1 → 原样返回"的口径：
// 没有重试可言（哪怕服务端建议的等待超了封顶），也不该用聚合错误掩盖原始错误。
func TestDoSingleAttemptWithRetryAfterKeepsRawError(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(99, &retryAfterError{wait: 30 * time.Second})
	p := Policy{
		MaxAttempts: 1,
		MaxBackoff:  time.Second,
		RetryIf:     retryIfRateLimited,
		sleep:       fc.sleep,
	}
	err := Do(context.Background(), p, fn)
	if err == nil {
		t.Fatal("期望报错")
	}
	if *calls != 1 {
		t.Errorf("fn 被调用 %d 次，期望 1", *calls)
	}
	var agg *Aggregated
	if errors.As(err, &agg) {
		t.Errorf("单次尝试不该包聚合（没有重试发生）：%v", err)
	}
	// 原始错误仍要能被读到（窗口没丢）。
	var ra *retryAfterError
	if !errors.As(err, &ra) || ra.wait != 30*time.Second {
		t.Errorf("原始错误里的窗口丢了：%v", err)
	}
}

// ---- 预算离谱时的内存安全 ----

// TestDoHugeMaxAttemptsDoesNotOOM 是 F5 的钉子：MaxAttempts=1e9 时，若按 max 预分配
// 尝试记录，一次申请几十 GB → 还没开始重试就 OOM。预分配必须封顶。
func TestDoHugeMaxAttemptsDoesNotOOM(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(0, errTransient) // 第一次就成功
	p := Policy{MaxAttempts: 1e9, RetryIf: transient, sleep: fc.sleep}
	if err := Do(context.Background(), p, fn); err != nil {
		t.Fatalf("Do = %v，期望成功", err)
	}
	if *calls != 1 {
		t.Errorf("fn 被调用 %d 次，期望 1", *calls)
	}
	if got := fc.sleeps(); len(got) != 0 {
		t.Errorf("不该睡：%v", got)
	}
}

// TestDoHugeMaxAttemptsFailsFast 同一枚钉子的另一半：第一次就失败且不可重试 → 立刻
// 返回（不会被 1e9 的预算拖着跑）。
func TestDoHugeMaxAttemptsFailsFast(t *testing.T) {
	fc := &fakeClock{}
	fn, calls := failingFn(99, errPermanent)
	p := Policy{MaxAttempts: 1e9, RetryIf: transient, sleep: fc.sleep}
	err := Do(context.Background(), p, fn)
	if !errors.Is(err, errPermanent) {
		t.Fatalf("err = %v", err)
	}
	if *calls != 1 {
		t.Errorf("fn 被调用 %d 次，期望 1", *calls)
	}
}

// TestPreallocAttemptsCapsBudget 直接钉住预分配上限（把封顶去掉或放大 → 红）。
//
// 上限值写死成 64（不引用常量）：常量被改动时这条会红——它钉的是"有上限"这件事，
// 不是"上限恰好等于某个数"。
func TestPreallocAttemptsCapsBudget(t *testing.T) {
	cases := []struct{ max, want int }{
		{0, 0},
		{1, 1},
		{3, 3},
		{64, 64},
		{65, 64},
		{1e9, 64},
	}
	for _, c := range cases {
		if got := preallocAttempts(c.max); got != c.want {
			t.Errorf("preallocAttempts(%d) = %d，期望 %d", c.max, got, c.want)
		}
	}
	if maxPreallocAttempts != 64 {
		t.Errorf("maxPreallocAttempts = %d，期望 64（预分配上限被改动）", maxPreallocAttempts)
	}
}

// ---- nil 语义 ----

func TestDoNilFnIsProgrammingError(t *testing.T) {
	if err := Do(context.Background(), Policy{}, nil); err == nil {
		t.Error("fn 为 nil 必须报错（没有可执行的操作）")
	}
}

func TestDoNilCtxFallsBackToBackground(t *testing.T) {
	//nolint:staticcheck // 故意传 nil ctx：验证兜底不 panic
	if err := Do(nil, Policy{MaxAttempts: 2, RetryIf: transient}, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("nil ctx 应兜底成 Background：%v", err)
	}
}

// ---- 帮助函数 ----

type fakeStatusError struct {
	code int
	msg  string
}

func (e *fakeStatusError) Error() string   { return fmt.Sprintf("HTTP %d: %s", e.code, e.msg) }
func (e *fakeStatusError) HTTPStatus() int { return e.code }

func TestRetryIfStatus(t *testing.T) {
	retryIf := RetryIfStatus(429, 500, 503)
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"429", &fakeStatusError{code: 429, msg: "rate limited"}, true},
		{"503", &fakeStatusError{code: 503, msg: "unavailable"}, true},
		{"500", &fakeStatusError{code: 500, msg: "boom"}, true},
		{"400", &fakeStatusError{code: 400, msg: "bad request"}, false},
		{"401", &fakeStatusError{code: 401, msg: "unauthorized"}, false},
		{"包装后仍命中", fmt.Errorf("openai: %w", &fakeStatusError{code: 429}), true},
		{"非 HTTP 错误", errTransient, false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := retryIf(c.err); got != c.want {
				t.Errorf("RetryIfStatus(429,500,503)(%v) = %v，期望 %v", c.err, got, c.want)
			}
		})
	}
}

func TestRetryAll(t *testing.T) {
	if !RetryAll(errPermanent) || !RetryAll(errTransient) {
		t.Error("RetryAll 对任何错误都该为真（危险帮助函数，语义就是全放行）")
	}
}

// ---- 并发 ----

// TestDoConcurrent 验证无共享状态：多个 goroutine 各跑自己的 Do，结果互不污染
// （-race 下跑；退避抖动用共享的全局 rand 源，其内部已加锁）。
func TestDoConcurrent(t *testing.T) {
	const goroutines = 16
	fc := &fakeClock{}
	base := Policy{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     4 * time.Millisecond,
		Multiplier:     2,
		Jitter:         0.3,
		RetryIf:        transient,
		sleep:          fc.sleep,
	}
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 偶数号：第 2 次才成功；奇数号：永远失败（聚合 3 条）。
			fail := 2
			if i%2 == 1 {
				fail = 99
			}
			fn, _ := failingFn(fail, errTransient)
			err := Do(context.Background(), base, fn)
			if i%2 == 0 {
				if err != nil {
					errs <- fmt.Errorf("goroutine %d: 期望重试后成功，得到 %v", i, err)
				}
				return
			}
			var agg *Aggregated
			if !errors.As(err, &agg) || len(agg.Attempts) != 3 {
				errs <- fmt.Errorf("goroutine %d: 期望 3 条聚合，得到 %T %v", i, err, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// ---- 小工具 ----

func equalDurations(a, b []time.Duration) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
