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
//   - HTTP 非 2xx → *StatusError（带状态码与**截断到 1KB** 的响应体）；
//   - ctx 取消/超时 → 包装 ctx.Err() 的错误（errors.Is(err, context.Canceled) 为真）；
//   - 响应体不可解析 / 结构非法（缺 choices、tool_call 缺 name、消息角色不认识）→ 明文错误。
//
// 所有错误在返回前都会先 emit 一条 kind="error" 的事件（Text=错误文案），
// 保证 UI 能看到失败原因；emit 之后仍照常返回 error（调用方两条路都走得到）。
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
