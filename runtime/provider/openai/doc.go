// Package openai 是 core.Provider 的 OpenAI 兼容实现（chat completions）。
//
// # 选型（v0）
//
// 协议层定案：**OpenAI 兼容 HTTP 接口**（chat completions），用标准库 raw net/http 实现，
// **零新依赖**（依据 docs/research/03-provider-sdks-and-go.md 的实测结论）。任何
// OpenAI 风格端点都能接——官方 api.openai.com、Azure 兼容端点、本地网关（vLLM /
// Ollama / one-api 之类）——只改 BaseURL 即可，调用方代码不动。
//
// # 身份与黑盒规则（S4 / 陷阱 1 / 陷阱 17）
//
// provider 生成的 tool_call_id 是**黑盒**：从响应里原样收进 core.ToolCallReq.ID
// （不改写、不折叠大小写、不 trim、不重新编码），调用方原样回传时也原样进请求体的
// tool_call_id。本包不解析、不规范化任何 provider id。Event.id 与 tool_call_id
// 永不共字段（那是 loop 层的事）。
//
// # env 与凭据
//
// 本包**不自己读环境变量**：APIKey 由装配方（host）从 env 取出后经 Options 传入。
// 凭据只以 Authorization 头的形式出现在出站请求里，**不进事件、不进日志、不进错误文案**。
//
// # 流式与非流式
//
// Options.Stream=false（默认）→ 请求体 stream:false，一次性解析响应体，直接组装
// core.ModelResult；为保持事件形状一致，会补发一条 text.delta（全文）、每个工具一条
// toolcall.start、最后一条 done。
//
// Options.Stream=true → 请求体 stream:true + stream_options.include_usage=true，
// 按 SSE 逐行解析（data: 行 / [DONE] 哨兵），增量 emit text.delta / toolcall.start /
// toolcall.delta，流结束后组装同一个 core.ModelResult。两种模式返回的 ModelResult
// 形状完全一致，调用方可以不关心走了哪条路。
//
// # 错误
//
//   - HTTP 非 2xx → *StatusError（带状态码、**截断到 1KB** 的响应体，以及服务端建议的
//     等待时长 RetryAfter——来自 Retry-After 头，解析不出 = 0；该窗口同时出现在**错误
//     文案**里（"(retry after 60s)"），不只躺在字段里）；
//   - ctx 取消/超时 → 包装 ctx.Err() 的错误（errors.Is(err, context.Canceled) 为真）；
//   - 响应体不可解析 / 结构非法（缺 choices、tool_call 缺 name、消息角色不认识）→ 明文错误。
//
// 所有错误在返回前都会先 emit 一条 kind="error" 的事件（Text=错误文案），
// 保证 UI 能看到失败原因；emit 之后仍照常返回 error（调用方两条路都走得到）。
//
// # 重试（v4-4-A，07 M6 通用重试组件 + M7 重试安全性）
//
// **provider 级小重试**（经 core/retry）：3 次尝试，指数退避 100ms→200ms（硬上界 2s）
// 加 20% 抖动。重试资格是**白名单**（retryableErr）：
//
//   - 可重试：HTTP 429 与 5xx（*StatusError + retryableStatus）、传输层网络错误
//     （net.Error：连接被重置、连接被拒、DNS 抖动、网关瞬断这一类）；
//   - 不可重试：4xx（非 429）等**请求侧**错误（该改请求，不是重发同样的请求）、
//     ctx 取消/超时（取消是调用方的意图，不是瞬断）、构造请求失败、**永久传输错**
//     （TLS 证书类错误、DNS 查无此域、**https 打明文端口 / 协议不支持**这类**配置错**
//     ——重发同一个请求还是同样的错，见 isPermanentTransportErr）、畸形响应体。
//
// 三条边界（都是"不双发"的硬约束）：
//
//  1. **重试期间不发 error 事件**——只有最终失败才 emit 一条。UI 不该看到"抖了一下
//     又好了"的红色闪烁；失败事实由最终那条 error + 返回值承载。
//  2. **响应体一开始解析就绝不重试**。重试只覆盖"发请求 → 拿到响应状态行"：流式一旦
//     发出第一条 delta，中途失败只上报不重试（半截流重试会双发 delta，消费端会把同一
//     段文本显示两遍）；非流式一旦开始解码也不重试（重发可能让端点把同一轮补全算两次钱）。
//  3. **ctx 取消不重试**（core/retry 的口径：取消立即停）。
//
// # 等待节奏：Retry-After（服务端建议的等待）
//
// 429/503 可以带 Retry-After 头（"120"= 延迟 120 秒，或一个 HTTP-date）。本包解析它
// 并放进 *StatusError.RetryAfter，core/retry 按**两条**口径处理：
//
//   - **Retry-After ≤ MaxBackoff（本包 2s）→ 等待被采信**：实际等待 = max(计算退避,
//     RetryAfter)。没有它的话，429 + Retry-After: 2 会被按 100ms/200ms 的自造节奏
//     重试——端点的限流窗口不会因为我们重试得快而缩短，那是**白烧**重试。
//   - **Retry-After > MaxBackoff → 停止重试，窗口抛给调用方**：端点要 30s 而我们最多
//     等 2s → 再试也是撞在同一个限流窗口里（还会加剧限流，白烧请求）→ 不重试，
//     只发一次请求，把窗口留在 *StatusError.RetryAfter 里给调用方（要等更久由调用方用
//     ctx deadline 或上层重试兜）。窗口同时出现在错误文案里（"(retry after 60s)"），
//     不只躺在字段里。
//
// # 连接复用
//
// 每个响应体都会被**排空 + 关闭**（drainClose，排空上限 64KB）：不关 = 连接泄漏；
// 不排空 = 连接回不到空闲池 → 每次重试都新建 TCP+TLS（重试反而更慢）。排空上限之外的
// 巨大错误体排不干净 → 该连接不用了（关掉，不泄漏）。
//
// 重试耗尽（或重试后撞上不可重试的错误、或因 Retry-After 超出等待上限而停重试）后返回的
// 是 core/retry 的**聚合错误**（每条尝试一个条目；文案形如 "retry: 3 次尝试后仍失败：
// 第 1 次：…；第 2 次：…"），errors.As 仍能取出 *StatusError（取到的是 Unwrap 序列中
// **最近一次匹配**的那个——完整尝试史见 Aggregated.Attempts）。
//
// 幂等性自证（M7）：chat completions 是只读推理请求，同一个 payload 重发一次得到的仍是
// 同一轮补全，不写任何系统状态 → 配自动重试。有副作用的操作（bash 那类工具）不在此列。
//
// **loop 层不再包一层重试**（v4-4-A 决策）：provider 内部已重试，两层重试会让退避与
// 请求量相乘放大，且 loop 认不出哪些失败值得再试一次。要调节奏改 providerRetryPolicy
// 一处即可。
//
// # 超时
//
// 本包不设默认超时：请由调用方传带 deadline 的 ctx，或注入带 Timeout 的 http.Client
// （Options.HTTPClient）。挂死的端点不应由 provider 猜一个超时值。
//
// # 并发
//
// Provider 是**无共享可变状态**的值：可被多个 goroutine 同时 Run（每次调用的解析状态
// 都是栈上局部变量）。压测见 provider_test.go 的并发用例（-race 下验证）。
package openai
