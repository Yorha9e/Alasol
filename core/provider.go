package core

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Yorha9e/alasol/contracts"
)

// provider 流事件 kind（**唯一事实源**：core 定义，runtime/provider/openai 与 core/loop
// 都引用这里——不再各写一份字面量，免得改了一边忘了另一边）。
//
// 这些是 core.ProviderEvent.Kind 的 v0 子集（I3 的开放字符串）：消费端按 kind 分流。
const (
	// KindTextDelta 增量文本（ProviderEvent.Text = 片段）。
	KindTextDelta = "text.delta"
	// KindReasoningDelta 增量思维链（ProviderEvent.Reasoning = 片段；G14）。
	// 与 KindTextDelta 同层级：都是"边收边发"的增量，消费端按 Kind 分流到正文/思维链
	// 两个显示区。
	KindReasoningDelta = "reasoning.delta"
	// KindToolCallStart 一个工具调用开始（ToolCall.ID / ToolCall.Name 已知，Args 可能还空）。
	KindToolCallStart = "toolcall.start"
	// KindToolCallDelta 工具调用参数片段（ToolCall.ID 已锁定，Args = **这一个片段**的原始字节）。
	KindToolCallDelta = "toolcall.delta"
	// KindDone 正常结束（Text = 最终全文，ToolCall = nil）。
	KindDone = "done"
	// KindError 失败（Text = 错误文案）；返回前必发，发完照常返回 error。
	KindError = "error"
	// KindProviderRetry provider 内部**重试发生时**发的观测事件（v4-4-B 缺口 5：重试留痕）。
	// 在"第一次失败后、第 n 次尝试发起前"发（attempt = 2..n；第一次尝试不发）；
	// 最终结果仍由 done/error 事件与返回值表达——本事件**纯观测**，不改变任何控制流，
	// 重试后成功时最终结果与不重试路径一字不差。载荷见 ProviderEvent.Retry。
	KindProviderRetry = "provider.retry"
)

// Provider 是模型提供方的进程内接口（D11：契约不含任何 SDK 类型；实现如 Anthropic/OpenAI 在 runtime）。
//
// 一次 Run = 一次模型调用：流式事件经 emit 回调上报（I3 统一事件的 v0 子集），
// 返回最终结果。provider 的 tool_call_id 是黑盒（S4）：原样进 ToolCallReq.ID、原样回传。
type Provider interface {
	Run(ctx context.Context, req ModelRequest, emit func(ProviderEvent)) (ModelResult, error)
}

// ModelRequest 是 provider 无关的模型调用请求（D10 中间表示的调用侧）。
type ModelRequest struct {
	Model    string
	System   string
	Messages []Message
	Tools    []contracts.Tool // 已按可见性过滤的工具描述（R5/R6）
	// ReasoningEffort 是思维链强度档位（G14；nil / 空 = **不传**该字段）。
	// 取值是网关支持的档位（如 low/medium/high/xhigh）——provider 原样透传给端点，
	// core 不解释也不校验（档位集随网关演进，硬编码会把合法配置拒掉）。
	ReasoningEffort *string
}

// Message 是一轮对话消息（provider 无关中间表示）。
type Message struct {
	Role       string // "user" | "assistant" | "tool"
	Content    string
	ToolCalls  []ToolCallReq        // assistant 发起的工具调用
	ToolCallID contracts.ProviderId // role="tool" 时回填对应调用（S4 黑盒）
}

// ToolCallReq 是一次工具调用的请求形态。
type ToolCallReq struct {
	ID   contracts.ProviderId // provider 生成，黑盒原样回传（S4/S5）
	Name string
	Args json.RawMessage
}

// ProviderEvent 是流式上报的统一事件（I3 的 v0 子集，kind 开放字符串）。
type ProviderEvent struct {
	// Kind 是事件种类（取值见本包的 Kind* 常量：KindTextDelta / KindReasoningDelta /
	// KindToolCallStart / KindToolCallDelta / KindDone / KindError / KindProviderRetry）。
	Kind string
	Text string
	// Reasoning 是思维链增量片段（G14；Kind=KindReasoningDelta 时非空）。
	Reasoning string
	ToolCall  *ToolCallReq
	// Retry 是重试留痕的载荷（Kind=KindProviderRetry 时非 nil；其余 kind 恒 nil）。
	// **additive 字段**（既有 kind 的形状一字不动）：provider 经 emit 通道上报重试现场，
	// loop 收到后转成 WAL 的 turn.retry（见 core/loop/wal.go——provider 侧不穿 WAL 句柄）。
	Retry *RetryInfo
}

// RetryInfo 是 KindProviderRetry 事件的载荷：一次**已决定要发起**的重试现场。
type RetryInfo struct {
	// Attempt 是**即将发起**的尝试序号（2 = 第一次重试；首次尝试不发重试事件）。
	Attempt int
	// Error 是上一次失败的摘要：**截断**过的错误文本（openai 的 statusErrorOf 口径——
	// 响应体截到 1KB；见 runtime/provider/openai）。
	//
	// **口径收窄（如实写明，别读成"绝对不含凭据"）**：**不含我们设置的 Authorization 头**
	// （凭据是我们自己加的请求头，不进错误文案）；但**响应体与请求 URL 原样截断进摘要**
	// ——与 `turn.failed` 的 WAL 摘要是**同一口径**（端点若在响应体里回显凭据，风险与既有
	// 面相同；这不是本刀新引入的暴露面）。
	Error string
	// Backoff 是这次重试前实际等待的时长（实测：上一次失败 → 本次尝试开始，含
	// core/retry 的计算退避 + 抖动 + Retry-After 采信）。0 = 没等到就发起了（如实记）。
	Backoff time.Duration
}

// ModelResult 是一次模型调用的最终结果。
type ModelResult struct {
	Text string
	// Reasoning 是本轮的**思维链整段**（G14：provider 上报 reasoning_content /
	// reasoning 字段时逐段组装；网关不上报 → 空串 = 解析不到，诚实留空）。
	Reasoning string
	ToolCalls []ToolCallReq
	Usage     Usage
}

// Usage 是 token 计量（J21：provider usage 优先，自算兜底）。
type Usage struct {
	InputTokens  int64
	OutputTokens int64
}
