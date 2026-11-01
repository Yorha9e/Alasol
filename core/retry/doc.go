// Package retry 是通用重试组件（07 M6）：Do(ctx, policy, fn) 把一个"值得重试的操作"
// 按**预算与节奏**重放，重试耗尽后返回**聚合错误**（含每一次尝试）。
//
// # 定位：机制，不是安全判断（M7 纪律）
//
// 本包只回答"怎么重试"（几次、隔多久、隔多久算完），**不回答**"该不该重试"——
// 后者是调用方的责任：只有**声明为幂等**的操作才配自动重试（M7：有副作用的操作重试
// 会重复副作用）。契约侧的 Idempotent / Replay 字段（contracts/tool.go）就是这份
// 声明的落点；模型 API 429/5xx 按 07 拍板可重试（推理请求只读）；CI 检查幂等可重试；
// bash 等有副作用工具缺省 false → 不可自动重试。
//
// 把非幂等操作包进 Do = 重复副作用，那是调用方的 bug，不是本组件的。
//
// # 形态
//
//	policy := retry.Policy{
//		MaxAttempts:    3,
//		InitialBackoff: 100 * time.Millisecond,
//		MaxBackoff:     time.Second,
//		Multiplier:     2,
//		Jitter:         0.2,
//		RetryIf:        retry.RetryIfStatus(http.StatusTooManyRequests, 503), // 显式声明
//	}
//	err := retry.Do(ctx, policy, func(ctx context.Context) error { ... })
//
// 主 agent 与 subagent **共用同一套**（M6）：工具调用、模型请求、CI 检查都走它——
// 重试口径只有一处，改节奏只改一处。
//
// # fail-close 缺省
//
// Policy.RetryIf 为 **nil = 不重试任何错误**。重试资格必须由调用方显式声明
// （RetryIfStatus / RetryAll / 自己写闭包），与 M7"不声明不自动重试"同精神。
//
// # 错误面
//
//   - 成功 → nil；
//   - **第一次**就失败且不可重试（或 MaxAttempts=1，根本没重试）→ **原始错误**（不包聚合，
//     调用方的 errors.Is/As 直接命中）；
//   - 重试过仍失败 → *Aggregated（每条尝试一个条目；Unwrap() []error 让 errors.Is/As
//     逐条穿透到底层，如 *StatusError 的 429）；
//   - 重试后撞上**不可重试**的错误 → 仍返回 *Aggregated（前面的可重试失败一并留着：
//     "429 重试后拿到 400"这类事实是诊断的关键，M6 拍板"聚合错误含各次尝试"）；
//   - **服务端建议的等待超出我们的等待上限**（Retry-After > MaxBackoff）→ 停重试，
//     返回 *Aggregated（这次尝试的事实与窗口都留在里面；MaxAttempts=1 时没有重试可言
//     → 按上面"原始错误"的口径）；
//   - 还没开始就被 ctx 取消 → ctx.Err() 本身（不包装）。
//
// # 等待节奏：Retry-After（服务端建议的等待）
//
// 端点可以在错误里给出"下次再试前至少等多久"（HTTP 429/503 的 Retry-After 头）。
// Do 按**两条**口径处理它（Retry-After 不抖动——抖动只为打散同步重试，不该削掉服务端
// 要求的等待）：
//
//   - **Retry-After ≤ MaxBackoff → 等待被采信**：实际等待 = max(计算退避, Retry-After)
//     （端点要 30s 而封顶 60s 时就等 30s；MaxBackoff 是硬上界，采信到的等待不超过它）。
//   - **Retry-After > MaxBackoff → 停止重试，窗口抛给调用方**：服务端明确要求的窗口
//     比我们愿意等的上限还长时，继续重试**注定撞窗**（而且只会加剧限流，白烧请求）→
//     不再重试，直接返回聚合错误，窗口（Retry-After）留在错误里供调用方读（provider
//     侧在 *StatusError.RetryAfter 上；调用方据此用 ctx deadline 或上层重试去等那个
//     窗口）。
//
// MaxBackoff<=0 = 不封顶 → 没有上界可言 → Retry-After 永远"等得起"（照采信）。
//
// 识别方式走**结构形状**接口（RetryAfterCarrier / RetryAfterDelayCarrier，见 error.go）：
// core 不 import runtime，故不认识任何具体 provider 的错误类型，只按形状取。
//
// # ctx 取消
//
// 取消**立即停**：不把取消当可重试错误（M7：取消是调用方的意图，不是瞬断）。
// 退避期间被取消同样停，取消事实记在 Aggregated.Interrupted。
//
// # 并发
//
// Do 无共享可变状态（退避抖动用 math/rand 的全局源，其内部已加锁）：同一个 Policy
// 可被任意多 goroutine 并发使用，各次调用的尝试记录都是栈上局部量。
//
// # 包纪律（09 / core doc）
//
// 只依赖标准库，**不含任何 I/O**（不发请求、不跑命令、不落库）——等待是唯一与外部
// 的交互，且可被 ctx 打断。core 不得 import runtime，所以本包不认识任何具体 provider
// 的错误类型：状态码分类走 StatusCarrier 这个**结构形状**接口（见 RetryIfStatus），
// 服务端建议等待走 RetryAfterCarrier / RetryAfterDelayCarrier（见「等待节奏」）。
//
// # 内存安全
//
// Policy.MaxAttempts **不封顶**（预算是调用方的声明），但尝试记录的**预分配封顶**
// （64 条）：MaxAttempts=1e9 时不会一次申请几十 GB 而 OOM——超出部分由 append 惰性
// 增长，内存随**真实尝试数**增长，不随声明的预算增长。
//
// # 消费方现状（v4-4-A 落地）
//
//   - runtime/provider/openai：**provider 级小重试**（429/5xx 与网络瞬断，3 次尝试，
//     指数退避加抖动，Retry-After 被采信）；永久传输错（TLS 证书错、DNS 查无此域）
//     不重试；流式已开始输出后不重试（半截流重试会双发 delta）。
//   - core/loop：模型请求调用点**不接**——provider 内部已重试，loop 再包一层会把退避
//     与请求量放大（两层重试的预算相乘），且 loop 认不出哪些失败值得再试一次。
//   - host/mergegate：CI 检查面**不接**（挂账）——该文件的形态是"读事件流判定 + 发射
//     事件"，没有可重试的瞬时 I/O；CI 命令的执行在 runtime/gate.Run，那里一次运行
//     产出一条 receipt（重跑 = 第二条 receipt，改变门禁语义），接线点另议。
package retry
