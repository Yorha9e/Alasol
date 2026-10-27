package openai

import (
	"bytes"
	"encoding/json"
	"strings"
)

// 这一文件是 chat completions 的**线上形状**（wire format）：请求/响应/流式 chunk 的
// JSON 结构与两个"宽容解析"的自定义类型。与 provider.go（业务映射）分开，改协议细节
// 不用翻业务代码。

// chatResponse 是非流式响应体。
type chatResponse struct {
	Choices []chatChoice `json:"choices"`
	Usage   *usageWire   `json:"usage"`
	Error   *errorWire   `json:"error"`
}

type chatChoice struct {
	Index        int           `json:"index"`
	Message      chatAssistant `json:"message"`
	FinishReason string        `json:"finish_reason"`
}

type chatAssistant struct {
	Role      string         `json:"role"`
	Content   flexText       `json:"content"`
	ToolCalls []respToolCall `json:"tool_calls"`
}

type respToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function chatFunction `json:"function"`
}

// chatChunk 是流式 chunk（delta 形态）。
type chatChunk struct {
	Choices []chunkChoice `json:"choices"`
	Usage   *usageWire    `json:"usage"`
	Error   *errorWire    `json:"error"`
}

type chunkChoice struct {
	Index        int       `json:"index"`
	Delta        chatDelta `json:"delta"`
	FinishReason *string   `json:"finish_reason"`
}

type chatDelta struct {
	Role      string          `json:"role"`
	Content   flexText        `json:"content"`
	ToolCalls []chunkToolCall `json:"tool_calls"`
}

// chunkToolCall 是流式 tool_call 增量。Index 为指针：部分网关省略 index，
// 此时回退成"在数组中的位置"（见 streamAccumulator.applyToolCalls）。
type chunkToolCall struct {
	Index    *int         `json:"index"`
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function chatFunction `json:"function"`
}

type usageWire struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
}

type errorWire struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    any    `json:"code"`
}

// flexText 宽容解析 content：官方形态是字符串，部分兼容网关发分块数组
// （[{"type":"text","text":"..."}]，Anthropic 风格）。两种都吃，取出纯文本。
type flexText string

// UnmarshalJSON implements json.Unmarshaler.
func (f *flexText) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "" || s == "null" {
		*f = ""
		return nil
	}
	if s[0] == '"' {
		var str string
		if err := json.Unmarshal(b, &str); err != nil {
			return err
		}
		*f = flexText(str)
		return nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(b, &parts); err != nil {
		return err
	}
	var sb strings.Builder
	for _, p := range parts {
		if p.Type == "" || p.Type == "text" {
			sb.WriteString(p.Text)
		}
	}
	*f = flexText(sb.String())
	return nil
}

// flexArgs 宽容解析 function.arguments：官方形态是**内含 JSON 的字符串**
// （"{\"path\":\"a\"}"），部分网关直接给对象。两种都收成"原始 JSON 字节"，
// 再由 normalizeArgs 兜底空值。**不做重新编码**——保留 provider 原文（S4 精神）。
type flexArgs []byte

// UnmarshalJSON implements json.Unmarshaler.
func (f *flexArgs) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "" || s == "null" {
		*f = nil
		return nil
	}
	if s[0] == '"' {
		var str string
		if err := json.Unmarshal(b, &str); err != nil {
			return err
		}
		*f = flexArgs(str) // 字符串内容即 JSON 文本
		return nil
	}
	*f = flexArgs(bytes.TrimSpace(b))
	return nil
}

// MarshalJSON implements json.Marshaler（请求体回传 tool_calls 时 arguments 必须是字符串）。
func (f flexArgs) MarshalJSON() ([]byte, error) {
	if len(bytes.TrimSpace(f)) == 0 {
		return json.Marshal("")
	}
	// arguments 在协议里是字符串：把原始 JSON 字节再包一层成 JSON 字符串。
	return json.Marshal(string(f))
}
