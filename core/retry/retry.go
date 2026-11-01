package retry

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// Policy 描述一次重试的**预算与节奏**（值类型；零值可用 = 只调一次、不睡、不重试）。
//
// 字段语义与缺省（缺省一律取更严格的一侧，fail-close）：
//
//	MaxAttempts    最多调用 fn 几次（**含第一次**）。<=0 → 1（= 不重试，只调一次）。
//	               **不封顶**（预算是调用方的声明）；但尝试记录的预分配另有上限，
//	               MaxAttempts 离谱时不会 OOM（见 maxPreallocAttempts）。
//	InitialBackoff 第一次重试前睡多久。<=0 → 0（立即重试，不睡）。
//	MaxBackoff     单次退避的上限（封顶）。<=0 → 不封顶（退避只按 InitialBackoff×Multiplier^n 长）。
//	               **也是服务端建议等待（Retry-After）的硬上界**（见 backoffFor 与
//	               retryAfterExceedsCap）：Retry-After ≤ MaxBackoff → 等待被采信；
//	               Retry-After > MaxBackoff → **不重试**（注定撞窗），窗口抛给调用方。
//	Multiplier     退避的倍增因子。<=1 → 1（恒定退避：每次都睡 InitialBackoff）。
//	Jitter         抖动比例，语义区间 [0,1]：实际退避 = base×(1±Jitter) 的均匀随机量。
//	               <0 → 0（不抖动）；>1 → 1（最大抖动）。抖动只为**打散**多客户端同步重试
//	               （雷鸣群），不改变量级。
//	RetryIf        判定一个错误值不值得重试。**nil = 不重试任何错误**（fail-close：重试
//	               资格必须由调用方显式声明，与 M7"只有声明幂等的操作才自动重试"同精神）。
//	               现成口径见本包的 RetryAll / RetryIfStatus，或调用方自己写闭包。
//
// sleep 是**不导出**的注入点（测试用假时钟，避免真睡）：生产路径恒为 nil（= 真睡）。
// 不导出的理由：调用方没有理由改"怎么等待"，导出了就等于把等待实现变成对外契约。
type Policy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
	Jitter         float64
	RetryIf        func(err error) bool

	sleep func(ctx context.Context, d time.Duration) error
}

// attempts 是有效尝试次数（<=0 归一为 1：没有"重试"这回事时也至少把 fn 调一次）。
//
// 注意：**不封顶**。预算是调用方的声明（声明了 1e9 次就是 1e9 次的预算——那是调用方
// 的事，本组件不替他省）；但"预分配"另有上限（见 maxPreallocAttempts），不会因为预算
// 离谱而在**还没开始重试**时就 OOM。
func (p Policy) attempts() int {
	if p.MaxAttempts < 1 {
		return 1
	}
	return p.MaxAttempts
}

// retryable 是"这个错误值得重试吗"（nil RetryIf = 一律不重试，fail-close）。
func (p Policy) retryable(err error) bool {
	if p.RetryIf == nil {
		return false
	}
	return p.RetryIf(err)
}

// backoff 是第 n 次尝试失败后、第 n+1 次尝试前的退避时长（n 从 1 起）。
//
// 形状：InitialBackoff × Multiplier^(n-1)，封顶 MaxBackoff，再乘 (1±Jitter)。
// 下限 0（抖动不能让退避变负——负睡眠等于立即重试，会把指数退避的意义抹掉）。
func (p Policy) backoff(n int) time.Duration {
	base := p.InitialBackoff
	if base < 0 {
		base = 0
	}
	if base > 0 && n > 1 {
		mult := p.Multiplier
		if mult <= 1 {
			mult = 1
		}
		for i := 1; i < n; i++ {
			if mult == 1 {
				break // 恒定退避：不增长
			}
			grown := time.Duration(float64(base) * mult)
			if grown <= base {
				// int64 溢出（配置离谱到 292 年级）：停止增长，交给 MaxBackoff 封顶。
				break
			}
			base = grown
			if p.MaxBackoff > 0 && base >= p.MaxBackoff {
				break
			}
		}
	}
	if p.MaxBackoff > 0 && base > p.MaxBackoff {
		base = p.MaxBackoff
	}
	base = p.jittered(base)
	// MaxBackoff 是**硬上界**：抖动在封顶之后仍受它约束（否则"上限 1s"会被
	// 0.5 的抖动放大成 1.5s，上限名不副实）。
	if p.MaxBackoff > 0 && base > p.MaxBackoff {
		base = p.MaxBackoff
	}
	return base
}

// jittered 在 base 上叠加 ±Jitter 的均匀抖动（Jitter<=0 或 base<=0 时原样返回）。
func (p Policy) jittered(base time.Duration) time.Duration {
	j := p.Jitter
	if j <= 0 || base <= 0 {
		return base
	}
	if j > 1 {
		j = 1
	}
	// factor ∈ [1-j, 1+j]；不裁剪下界到 0 以上——0 退避是合法的（InitialBackoff=0 时）。
	factor := 1 + j*(2*randFloat64()-1)
	d := time.Duration(float64(base) * factor)
	if d < 0 {
		d = 0
	}
	return d
}

// wait 睡 d（可被 ctx 打断）。policy.sleep 是测试注入点（nil = 真睡）。
func (p Policy) wait(ctx context.Context, d time.Duration) error {
	if p.sleep != nil {
		return p.sleep(ctx, d)
	}
	return sleepCtx(ctx, d)
}

// backoffFor 是第 n 次尝试失败后、第 n+1 次尝试前**实际要等**的时长。
//
// 口径（两条，顺序即优先级）：
//
//  1. 计算退避 = backoff(n)（InitialBackoff×Multiplier^(n-1)，封顶 MaxBackoff，带抖动）；
//  2. **服务端建议**（Retry-After）取大者：max(计算退避, RetryAfter)。
//     端点说"过 30 秒再来"（429 + Retry-After: 30）时，按 100ms 的自造节奏重试等于
//     白烧——端点的限流窗口不会因为我们重试得快而缩短。Retry-After **不抖动**：
//     抖动只用于打散同步重试，不该削掉服务端要求的等待。
//
// 说明：**Retry-After > MaxBackoff 的情形走不到这里**——那种情况下 Do 已经停了
// （见 retryAfterExceedsCap：服务端要的窗口比我们愿意等的上限还长 → 不重试，
// 窗口抛给调用方）。所以这里拿到的 Retry-After 一定 ≤ MaxBackoff，MaxBackoff 的
// 封顶不会削掉服务端要求的等待（MaxBackoff<=0 = 不封顶）。
func (p Policy) backoffFor(n int, err error) time.Duration {
	d := p.backoff(n)
	if ra := retryAfterOf(err); ra > d {
		d = ra
	}
	if p.MaxBackoff > 0 && d > p.MaxBackoff {
		d = p.MaxBackoff
	}
	return d
}

// retryAfterExceedsCap 判定"服务端建议的等待"是否**超过**本 policy 的等待上限。
//
// 口径：MaxBackoff 是"我们最多愿意为一次重试等多久"的硬上界。服务端明确要求的窗口
// 比它还长时，继续重试**注定撞窗**（而且只会加剧限流，白烧请求）→ 不重试，把窗口
// 抛给调用方（调用方据此用 ctx deadline 或上层重试去等那个窗口）。
//
// MaxBackoff<=0 = 不封顶 → 没有上界可言 → 永远不算超限（等得起就等）。
//
// 边界：Retry-After **==** MaxBackoff 不算超限（等得了 → 照采信）。
func (p Policy) retryAfterExceedsCap(err error) bool {
	if p.MaxBackoff <= 0 {
		return false
	}
	return retryAfterOf(err) > p.MaxBackoff
}

// randFloat64 是 [0,1) 的均匀随机数。
//
// 用 math/rand 的**全局源**（Go 1.20+ 自动播种；内部加锁 → 并发安全）：重试抖动不需要
// 密码学强度，也不值得为每次重试各造一个源（那要么重复播种、要么传源进 Policy）。
func randFloat64() float64 { return rand.Float64() }

// Do 按 policy 的预算与节奏执行 fn：直到成功、错误不可重试、预算耗尽或 ctx 结束。
//
// fn 每次尝试都拿到**同一个** ctx（重试不换上下文：取消的意图跨尝试有效）。
//
// 返回（五态）：
//   - nil —— 某一次尝试成功（此前的失败被吞掉，不进错误面）；
//   - 原始错误 —— **第一次**尝试就失败且**不**可重试（或 MaxAttempts=1，根本没有重试）：
//     不包聚合，调用方的 errors.Is/As 直接命中那个错误；
//   - *Aggregated —— ① 重试过且仍失败（预算耗尽）；② 重试后撞上**不可重试**的错误
//     （前几次的可重试失败一并留在聚合里——M6："聚合错误含各次尝试"）；③ 退避期间被
//     ctx 打断（Aggregated.Interrupted 带取消错误）；④ **服务端建议的等待超出我们的
//     等待上限**（Retry-After > MaxBackoff → 不再重试，见 retryAfterExceedsCap：
//     窗口留在错误里供调用方读；MaxAttempts=1 时没有重试可言 → 走上面"原样返回"口径）。
//     四种成因下 errors.Is/As 都逐条可判；
//   - ctx.Err() 本身 —— 还没开始任何尝试就被取消（不包装）。
//
// # 幂等纪律（M7）：Do 只是机制
//
// 本函数不知道 fn 有没有副作用。调用方必须**自证** fn 幂等（同一输入重放一次不产生
// 额外效果）才配把可重试的错误交给它：bash 这类有副作用的工具默认不可重试（契约
// Idempotent 缺省 false），模型推理请求只读故可重试（07 拍板：模型 API 429/5xx 可重试），
// CI 检查幂等故可重试。
func Do(ctx context.Context, policy Policy, fn func(ctx context.Context) error) error {
	if ctx == nil {
		// nil ctx 兜底成 Background（core doc 的"兜底"一档）：缺参数后系统仍能安全地
		// 做事，不存在"不放行任何东西"的问题。
		ctx = context.Background()
	}
	if fn == nil {
		return errors.New("retry: fn 为 nil（编程错误：没有可执行的操作）")
	}
	max := policy.attempts()
	attempts := make([]Attempt, 0, preallocAttempts(max))
	for n := 1; n <= max; n++ {
		// ctx 已结束 → 不再发起新尝试（取消不是可重试错误，M7）。
		if ctxErr := ctx.Err(); ctxErr != nil {
			if len(attempts) == 0 {
				return ctxErr // 一次都没跑：原样返回取消错误（不包装）
			}
			return &Aggregated{Attempts: attempts, Interrupted: ctxErr}
		}
		err := fn(ctx)
		if err == nil {
			return nil
		}
		attempts = append(attempts, Attempt{N: n, Err: err})
		if !policy.retryable(err) {
			if n == 1 {
				// 第一次就失败且不可重试 → **原样**返回（不包聚合：调用方要 errors.As 的
				// 就是这个错误）。
				return err
			}
			// 重试后撞上不可重试的错误 → 仍返回**聚合**（M6：聚合错误要含各次尝试；
			// 前几次的可重试失败不能被吞掉——"429 重试后拿到 400"这类事实在诊断里很值钱）。
			// errors.Is/As 仍能穿透到最后那个错误（Unwrap() []error）。
			return &Aggregated{Attempts: attempts}
		}
		if n == max {
			break // 预算耗尽（没有下一次可试 → 走下面的"原样/聚合"出口）
		}
		// **服务端要的窗口 > 我们的等待上限 → 不重试**（Retry-After > MaxBackoff）：
		// 端点明确说了"过 60s 再来"，而我们最多只愿意等 2s → 再试也是撞在同一个限流
		// 窗口里（还会加剧限流，白烧请求）→ 停。窗口（Retry-After）留在错误里，
		// 调用方自己决定等多久再来（ctx deadline / 上层重试）。
		// 仍返回**聚合**：这次尝试的事实（状态码 + 窗口）不能被吞掉。
		if policy.retryAfterExceedsCap(err) {
			return &Aggregated{Attempts: attempts}
		}
		// 退避 0（InitialBackoff=0）= 不睡，也不打扰 sleeper：立即发起下一次尝试。
		// 取消仍由下一轮循环开头的 ctx.Err() 检查兜住（见 TestDoCancelBetweenAttemptsStops）。
		if d := policy.backoffFor(n, err); d > 0 {
			if waitErr := policy.wait(ctx, d); waitErr != nil {
				// 退避期间被取消：停。attempts 里是真实跑过的尝试，取消事实进 Interrupted。
				return &Aggregated{Attempts: attempts, Interrupted: waitErr}
			}
		}
	}
	if len(attempts) <= 1 {
		// 只跑了一次（MaxAttempts=1）：没有"重试"发生，别用聚合错误掩盖原始错误。
		return attempts[0].Err
	}
	return &Aggregated{Attempts: attempts}
}

// maxPreallocAttempts 是尝试记录切片的**预分配**上限。
//
// 为什么要有：MaxAttempts 由调用方给，**没有上界**（1e9 也是合法的声明）。直接
// make([]Attempt, 0, max) 会按 max 预分配——MaxAttempts=1e9 时一次申请几十 GB，
// **还没开始重试就 OOM**。预分配只是省几次 append 的拷贝，超过这个量级用 append 惰性
// 增长即可：内存随**真实尝试数**增长，不随声明的预算增长。
const maxPreallocAttempts = 64

// preallocAttempts 是预分配容量 = min(max, maxPreallocAttempts)。
func preallocAttempts(max int) int {
	if max <= 0 {
		return 0
	}
	if max > maxPreallocAttempts {
		return maxPreallocAttempts
	}
	return max
}

// sleepCtx 睡 d，可被 ctx 打断（返回 ctx.Err()）。
//
// d<=0 是 defensive 分支（Do 现在不会用 0 退避调用它）：直接返回，但**仍给 ctx 一次
// 机会——零退避也该能被取消。
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
