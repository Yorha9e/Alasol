package core

import (
	"context"
	"encoding/json"

	"github.com/Yorha9e/alasol/contracts"
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
	Kind     string // "text.delta" / "toolcall.start" / "toolcall.delta" / "done" / "error"
	Text     string
	ToolCall *ToolCallReq
}

// ModelResult 是一次模型调用的最终结果。
type ModelResult struct {
	Text      string
	ToolCalls []ToolCallReq
	Usage     Usage
}

// Usage 是 token 计量（J21：provider usage 优先，自算兜底）。
type Usage struct {
	InputTokens  int64
	OutputTokens int64
}
