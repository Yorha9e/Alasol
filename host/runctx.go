package host

// 本文件是 G-f 的**整轮预算软时钟**（runClock）：协议面整轮预算（promptRunTimeout）
// 不再用 context.WithTimeout 一把掐死，而是"到点照常掐、待机可豁免"的软时钟。
//
// # 为什么不能直接用 context.WithTimeout
//
//	ctx 的 deadline 一旦设定就**不可延长**（Go 语义），而 standby 可挂 ≤30min——
//	待机中段整轮预算必然到点，把这一轮误杀；唤醒后 ctx 已死 = 白醒。于是把"到点
//	取消"从 ctx 的内建 deadline 挪到本文件的软时钟：ctx 本体只带 cancel（**不带内建
//	deadline**），deadline 由 runClock 持有，到点由内部定时器触发 cancel。
//
// # 语义不变式（别破坏）
//
//   - **无 standby 的轮次**：没人调 Extend → 到点照常掐，与 context.WithTimeout 等价
//     （Err 呈 DeadlineExceeded 语义：errors.Is(err, context.DeadlineExceeded) 为真，
//     既有判 deadline 的代码不用改）；
//   - **超预算 vs 用户取消可区分**：超预算 → errRunBudget（包装 DeadlineExceeded）；
//     parent 取消（hub 关闭 / 显式取消）→ context.Canceled，**不被豁免**；
//   - **Extend 只增不减**：新截止 = **当前截止 + d**（"暂停 d 时长"语义，critic F6；
//     不是 now+d——那会让预算充足时待机照样烧预算），已收场后是 no-op；
//   - **预借 + 实占结算**（Pause / Settle，critic F6 + critic P2）：standby 待机前
//     预借「计划时长 + 余量」（否则等不到结束就被掐），待机结束后把截止**结算为**
//     「预借前的截止 + 实占时长 + 余量」——多借的退回（早醒的 standby 不该把整轮
//     预算撑大），结算后的截止不低于 now+余量（收尾那一轮要有预算）；
//   - **取不到时钟的路径**（CLI / ACP / 直调 RunPrompt 等自己造 ctx 的路径）：
//     runClockFrom 返回 nil → 不延展，行为同现在。
//
// 经 ctx value 传递（runClockFrom）：standby 待机前取钟并 Pause、待机结束后 Settle
// （预算豁免，G-f，见 host/standby.go）。Deadline() 报告**当前**软截止——审批枚举面
// （GET /api/approvals/pending 的 deadline 字段）与派生 ctx 的 min 口径都吃它。

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// errRunBudget 是"整轮预算耗尽"的错误：包装 context.DeadlineExceeded——
// 既有判 deadline 的代码（errors.Is(err, context.DeadlineExceeded)）不用改，
// 同时与"用户取消 / hub 关闭"（context.Canceled）可区分。
var errRunBudget = fmt.Errorf("host: 整轮预算耗尽（promptRunTimeout）: %w", context.DeadlineExceeded)

// runClockKey 是 runClock 在 ctx value 链上的取值键（未导出类型键：外部包造不出同键）。
type runClockKey struct{}

// runClock 是一轮协议面 prompt 的**软时钟**：内部定时器到点 cancel，截止可 Extend。
//
// 并发安全：mu 保护 deadline / timer / expired / stopped——Extend 可能与到点回调
// 并发（standby 推表 vs 定时器触发），必须无数据竞争（-race 钉子在 runctx_internal_test.go）。
type runClock struct {
	mu       sync.Mutex
	deadline time.Time
	timer    *time.Timer
	base     context.Context    // 底层 WithCancel 的 ctx（Done / 父取消传导走它）
	cancel   context.CancelFunc // 收场时释放 base
	expired  bool               // 定时器到点（超预算）并已 cancel
	stopped  bool               // 已收场（到点 / Stop）：不再接受 Extend、回调不再动作
}

// runCtx 是 runClock 的 ctx 面：Done / 父取消传导走底层 cancel ctx，
// Deadline / Err / Value 走软时钟（截止可延展、到点呈 DeadlineExceeded 语义）。
type runCtx struct {
	context.Context // 底层 WithCancel 的 ctx
	clock           *runClock
}

// newRunClock 造一轮的软时钟：ctx 无内建 deadline（不可延长的那个），到点由
// runClock 内部定时器 cancel；d ≤ 0 视为立即到点（与 context.WithTimeout 同口径）。
func newRunClock(parent context.Context, d time.Duration) (context.Context, *runClock) {
	if parent == nil {
		parent = context.Background()
	}
	base, cancel := context.WithCancel(parent)
	c := &runClock{base: base, cancel: cancel}
	// 装表也在锁内：fire() 拿同一把锁读 c.timer，先写后用必须有同步边（-race）。
	c.mu.Lock()
	c.deadline = time.Now().Add(d)
	c.timer = time.AfterFunc(d, c.fire)
	c.mu.Unlock()
	return &runCtx{Context: base, clock: c}, c
}

// fire 是到点回调：截止被 Extend 推后 → 本轮到点作废（Extend 在同一把锁里
// 重装过定时器）；父先取消 → 不是超预算，别把取消误报成超时；
// 否则标记 expired 并 cancel（先解锁再 cancel：被 Done 唤醒的读者会来拿同一把锁）。
func (c *runClock) fire() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	// 到点回调与 Extend 并发：截止已推后 → 作废本轮到点（定时器已被 Extend 重装）。
	if now := time.Now(); now.Before(c.deadline) {
		c.mu.Unlock()
		return
	}
	// 父 ctx 先取消了（hub 关闭 / 显式取消）→ 不是超预算。
	if c.base.Err() != nil {
		c.stopped = true
		c.mu.Unlock()
		return
	}
	c.expired = true
	c.stopped = true
	c.mu.Unlock()
	c.cancel()
}

// Extend 把截止**只增不减**地推后：新截止 = **当前截止 + d**（"暂停 d 时长"语义，
// critic F6）——不是 now+d。
//
// 为什么是"从当前截止后移"而不是"从现在后移"：standby 待机前要豁免的是**这次
// 等待占掉的那段时间**（等待时长另计）。若按 now+d 算，预算充足时（截止还很远）
// 待机**照样烧预算**：等 5 分钟但预算还剩 1 小时 → 截止只推到 now+5min（≈没动）
// → 那一轮的预算其实在待机期间被白白消耗。改成 deadline+d 才是真"暂停"：
// 待机多久，预算就往后挪多久。
//
// 仍然**只增不减**：d ≤ 0 不动截止（负值/零不许把截止拉回来——一次误延展就能把
// 整轮预算缩掉）。已收场（到点 / Stop）后是 no-op：ctx 已死，延展无意义。
//
// G-f 的调用点：standby 待机前（host/standby.go）——现在走 Pause/Settle 两段式
// （预借 + 实占结算），Extend 保留为"只增不减"的原语。
func (c *runClock) Extend(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.extendLocked(d)
}

// extendLocked 推后截止（调用方持锁；只增不减）。
func (c *runClock) extendLocked(d time.Duration) {
	if c.stopped {
		return
	}
	nd := c.deadline.Add(d)
	if !nd.After(c.deadline) {
		return // 只增不减
	}
	c.deadline = nd
	c.timer.Reset(time.Until(nd))
}

// Pause 预借：把截止后移 d（只增不减），返回**预借前**的截止（结算基准 base）。
//
// 这是"暂停"语义的两段式之一（G-f / critic F6 + critic P2）：
//
//	预借（Pause）→ 保证**等待期间**预算够用（否则等不到结束就被掐）；
//	结算（Settle）→ 等待结束后把截止结算为「base + 实占 + 余量」，多借的退回。
//
// 只预借不结算 → 早醒的 standby 把整轮预算撑大（planned=1min、实际 50ms 醒，
// 却把截止推后了 1min + 余量；sleep 模式一次推 30min + 余量）。
//
// 已收场（到点 / Stop）后是 no-op（ctx 已死，延展无意义）：base 仍返回当前截止
// （调用方据此结算也不会越权——Settle 同样在 stopped 时 no-op）。
func (c *runClock) Pause(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	base := c.deadline
	c.extendLocked(d)
	return base
}

// Settle 结算：把截止**结算为** base + elapsed + margin（多借的退回，少补的补上）。
//
// 下限保护：结算后的截止**不得早于 now + margin**——收尾那一轮模型调用必须有预算
// （否则待机豁免了预算、却在收尾处被掐 = 白等）。
//
// 与 Extend 的分工：Extend/Pause **只增不减**（幂等加法，预借用），Settle 是**结算**
// （可以把多借的那截还回去，收尾用）。
//
// 已收场（到点 / Stop）后是 no-op（ctx 已死）。
func (c *runClock) Settle(base time.Time, elapsed, margin time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	nd := base.Add(elapsed + margin)
	if floor := time.Now().Add(margin); nd.Before(floor) {
		nd = floor // 下限保护：收尾那一轮要有预算
	}
	if nd.Equal(c.deadline) {
		return
	}
	c.deadline = nd
	c.timer.Reset(time.Until(nd))
}

// Stop 收场：停定时器 + 取消 ctx（幂等；runPromptAsync 的 defer 用——
// 与旧的 `defer cancel()` 同职责：跑完把 ctx 交回，别让定时器悬着）。
// 不改 expired：到点过的轮次 Err 仍呈超预算语义。
func (c *runClock) Stop() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped = true
	c.timer.Stop()
	c.mu.Unlock()
	c.cancel()
}

// deadlineVal 报告当前软截止（始终 ok=true：本时钟就是这一轮的上界）。
func (c *runClock) deadlineVal() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadline, true
}

// isExpired 报告是否已按"超预算"收场（Err 据此在 DeadlineExceeded 与 Canceled 间分流）。
func (c *runClock) isExpired() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.expired
}

// runClockFrom 从 ctx 取本轮软时钟；取不到（CLI / ACP / 直调 RunPrompt 等无时钟路径）
// → nil，调用方按"不延展"处理（行为同现在）。
func runClockFrom(ctx context.Context) *runClock {
	if ctx == nil {
		return nil
	}
	c, _ := ctx.Value(runClockKey{}).(*runClock)
	return c
}

// Deadline 报告**当前**软截止（Extend 后随之推后；父 deadline 更早时以父为准）。
// 审批枚举面（pending 的 deadline 字段）与派生 ctx 的 min 口径都吃这个值。
func (c *runCtx) Deadline() (time.Time, bool) {
	d, _ := c.clock.deadlineVal()
	if pd, ok := c.Context.Deadline(); ok && pd.Before(d) {
		return pd, true
	}
	return d, true
}

// Err：ctx 未结束 → nil；软时钟到点 → errRunBudget（errors.Is … DeadlineExceeded 为真）；
// 父取消 / 显式 Stop → 底层错误（context.Canceled）——"超预算"与"用户取消"可区分。
func (c *runCtx) Err() error {
	err := c.Context.Err()
	if err == nil {
		return nil
	}
	if c.clock.isExpired() {
		return errRunBudget
	}
	return err
}

// Value 暴露 runClock 给 runClockFrom（其余键走底层链，含 stdlib 的 &cancelCtxKey——
// 派生 ctx 的父子挂接靠它，不能吞）。
func (c *runCtx) Value(key any) any {
	if _, ok := key.(runClockKey); ok {
		return c.clock
	}
	return c.Context.Value(key)
}
