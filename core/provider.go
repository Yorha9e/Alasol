package core

import (
	"context"
	"encoding/json"

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
	// KindToolCallStart / KindToolCallDelta / KindDone / KindError）。
	Kind string
	Text string
	// Reasoning 是思维链增量片段（G14；Kind=KindReasoningDelta 时非空）。
	Reasoning string
	ToolCall  *ToolCallReq
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
