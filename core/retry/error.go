package retry

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Attempt 是一次尝试的事实（M6：聚合错误要能说清"每次败在哪儿"，而不只是最后一次）。
type Attempt struct {
	// N 是第几次尝试（从 1 起；与 fn 被调用的次序一致）。
	N int
	// Err 是该次返回的错误（永不 nil：成功的尝试不进聚合）。
	Err error
}

// Aggregated 是重试耗尽（或重试后撞上不可重试的错误、或退避中被 ctx 打断、或服务端
// 建议的等待超出我们的等待上限而停重试）后的聚合错误。
//
// 不变量：len(Attempts) == fn 实际被调用的次数；Attempts[i].N == i+1。
//
// # 为什么不用 errors.Join
//
// errors.Join 也能让 errors.Is/As 穿透，但它把"每次尝试"压成一条等权的错误链：没有
// 次数、没有序号、没有"到底重试了几回"的总结，也无法表达"退避中被 ctx 打断"这个与
// 尝试无关的事实。聚合错误的首要用途是**诊断**（"网关抖了三次，每次错得不一样"），
// 所以自实现一个带结构的类型——errors.Join 的等权语义在这里是信息损失。
type Aggregated struct {
	// Attempts 是每次尝试的错误（按发生序；见不变量）。
	Attempts []Attempt
	// Interrupted 是"退避期间 ctx 结束"的取消错误（nil = 正常耗尽）。
	// 它**不是**一次尝试（没有真的调用 fn），故单列一个字段，免得污染
	// "len(Attempts) == fn 调用次数"这个不变量。
	Interrupted error
}

// Last 返回最后一次尝试的错误（nil-safe：没有尝试时返回 nil）。
//
// 给"只关心最后败在哪儿"的调用方省事；要每一次就用 Attempts。
func (e *Aggregated) Last() error {
	if e == nil || len(e.Attempts) == 0 {
		return nil
	}
	return e.Attempts[len(e.Attempts)-1].Err
}

// Unwrap 返回每一次尝试的错误（外加 Interrupted）——Go 1.20+ 的多错误展开：
// errors.Is / errors.As 因此能逐条穿透到底层（如 *StatusError 的 429）。
//
// 顺序：**最近一次在前**（Interrupted 若有也在最前）。理由：调用方最关心"最后败在
// 哪儿"——"429 重试后拿到 400"里，要处理的是 400，429 是历史。按发生序取每一次尝试
// 用 Attempts 字段（Attempts[i].N == i+1，恒为发生序）。
//
// **errors.As 取到的是"最近一次匹配"**：errors.As 沿 Unwrap 序列从前向后找**第一个
// 类型匹配**的错误 → 命中的是最近一次"类型对得上"的尝试（若最后一次尝试的类型不匹配
// 目标类型，取到的是更早的那次，不是"最后一次尝试"）。完整尝试史见 Attempts 字段。
func (e *Aggregated) Unwrap() []error {
	if e == nil {
		return nil
	}
	out := make([]error, 0, len(e.Attempts)+1)
	if e.Interrupted != nil {
		out = append(out, e.Interrupted)
	}
	for i := len(e.Attempts) - 1; i >= 0; i-- {
		out = append(out, e.Attempts[i].Err)
	}
	return out
}

// Error implements error。
func (e *Aggregated) Error() string {
	if e == nil {
		return "retry: <nil 聚合错误>"
	}
	if len(e.Attempts) == 0 && e.Interrupted == nil {
		return "retry: 无尝试记录（空聚合）"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "retry: %d 次尝试后仍失败：", len(e.Attempts))
	for i, a := range e.Attempts {
		if i > 0 {
			b.WriteString("；")
		}
		fmt.Fprintf(&b, "第 %d 次：%v", a.N, a.Err)
	}
	if e.Interrupted != nil {
		fmt.Fprintf(&b, "；退避中被 ctx 中止：%v", e.Interrupted)
	}
	return b.String()
}

// StatusCarrier 是"能报出 HTTP 状态码的错误"的最小接口（**结构形状**，不 import 任何
// provider 包：core 不得 import runtime，09）。
//
// runtime/provider/openai 的 *StatusError 满足它（HTTPStatus() 方法）；自有的网关错误
// 类型加一个 HTTPStatus() int 方法即可接入 RetryIfStatus。
type StatusCarrier interface {
	error
	// HTTPStatus 返回 HTTP 状态码（0 = 不是 HTTP 错误 / 不知道）。
	HTTPStatus() int
}

// RetryAll 是"任何错误都重试"的 RetryIf。
//
// **危险**：它把"什么可重试"的判断整个外包给了运气——只有 fn 确实幂等、且失败面确实
// 是瞬时面（典型：模型推理请求，07 拍板 429/5xx 可重试）时才用。bash 这类有副作用的
// 操作配上它 = 重复副作用（M7）。
//
// Do 只在 fn 返回**非 nil** 错误时请教 RetryIf，所以成功路径不受它影响。
func RetryAll(error) bool { return true }

// RetryAfterCarrier 是"能报出**服务端建议的等待时长**"的最小接口（**结构形状**，不 import
// 任何 provider 包：core 不得 import runtime，09）。
//
// 与 StatusCarrier 同款思路：core 不认识任何具体 provider 的错误类型，只按形状取。
// runtime/provider/openai 的 *StatusError 通过 RetryAfterDelay 暴露同一份信息
// （Retry-After 头解析出来的秒数；解析不出 = 0）。
type RetryAfterCarrier interface {
	error
	// RetryAfter 返回"下次再试前至少等多久"（0 = 没有建议 / 解析不出 / 不适用）。
	RetryAfter() time.Duration
}

// RetryAfterDelayCarrier 是 RetryAfterCarrier 的**等价别名形状**（方法名带 Delay 后缀）。
//
// 为什么认两种形状：provider 侧的错误类型通常把建议时长存成**同名字段**（RetryAfter），
// 而 Go 不允许字段与方法同名 → 只能换个方法名暴露（RetryAfterDelay）。core 这边两种
// 都认，调用方不必为了接入 core/retry 而改自己的字段名。
type RetryAfterDelayCarrier interface {
	error
	// RetryAfterDelay 同 RetryAfterCarrier.RetryAfter（语义一字不差）。
	RetryAfterDelay() time.Duration
}

// retryAfterOf 从错误里取服务端建议的等待时长（0 = 没有建议 / 解析不出 / 负值）。
//
// 走 errors.As：**穿透包装**（fmt.Errorf("...: %w", statusErr) 仍能命中）。
func retryAfterOf(err error) time.Duration {
	if err == nil {
		return 0
	}
	var c RetryAfterCarrier
	if errors.As(err, &c) {
		return nonNegative(c.RetryAfter())
	}
	var d RetryAfterDelayCarrier
	if errors.As(err, &d) {
		return nonNegative(d.RetryAfterDelay())
	}
	return 0
}

// nonNegative 把负时长归零（负等待 = 立即重试，比"没有建议"更糟）。
func nonNegative(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}

// RetryIfStatus 返回一个只对给定 HTTP 状态码重试的 RetryIf（M7 拍板的那一类瞬时面）。
//
// 匹配走 errors.As（**穿透包装**）：fmt.Errorf("...: %w", statusErr) 仍能命中。
// 状态码不在集合里 → false：4xx（非 429）、以及一切非 HTTP 错误（含 ctx 取消）都不重试。
func RetryIfStatus(codes ...int) func(error) bool {
	set := make(map[int]bool, len(codes))
	for _, c := range codes {
		set[c] = true
	}
	return func(err error) bool {
		if err == nil {
			return false
		}
		var sc StatusCarrier
		if !errors.As(err, &sc) {
			return false
		}
		return set[sc.HTTPStatus()]
	}
}
