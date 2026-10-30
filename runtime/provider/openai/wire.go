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
	// ReasoningContent / Reasoning 是思维链字段（G14）：DeepSeek 风网关用
	// reasoning_content（流式在 delta.reasoning_content），部分端点用 reasoning。
	// **两个都看**、reasoning_content 优先（见 reasoningOf）；解析不到就留空——
	// 不猜别的厂商私有字段（诚实）。
	//
	// 用**独立的宽容类型**（flexReasoning）而不是复用 flexText：思维链是**可选信息**，
	// 它的畸形形态（对象 / 布尔 / 数字）绝不该让整轮模型调用失败（G14 回归修复）。
	ReasoningContent flexReasoning `json:"reasoning_content"`
	Reasoning        flexReasoning `json:"reasoning"`
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
	// ReasoningContent / Reasoning 是流式思维链增量（G14）：DeepSeek 风最常见
	// （delta.reasoning_content），部分端点用 delta.reasoning。两个都看、
	// reasoning_content 优先（见 reasoningOf）。
	//
	// 同 chatAssistant：**独立宽容类型**——畸形形态不留空即失败（见 flexReasoning）。
	ReasoningContent flexReasoning `json:"reasoning_content"`
	Reasoning        flexReasoning `json:"reasoning"`
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
	// G14 口径（写清，免得后人"补"出一个重复计量）：常见网关（OpenAI o 系 /
	// DeepSeek）的 completion_tokens **已包含**思维链 token（OpenAI 的
	// completion_tokens_details.reasoning_tokens 是它的**子集**，不是额外量）。
	// 故本包**不解析**单独的 reasoning token 字段——加了就是把思维链 token 数两遍
	// （成本虚高）。真出现"分开报且不含在 completion_tokens 里"的网关时，
	// 在 ModelResult 组装处按那个网关的口径另算，而不是在这里猜。
}

type errorWire struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    any    `json:"code"`
}

// flexText 宽容解析 content：官方形态是字符串，部分兼容网关发分块数组
// （[{"type":"text","text":"..."}]，Anthropic 风格）或**字符串数组**
// （["a","b"]）。三种都吃，取出纯文本。
//
// **未知形状兜底空串**（G14 回归修复）：对象 / 布尔 / 数字等形态**不再让整个响应
// 反序列化失败**——一条畸形字段不该把整轮模型调用打掉。兜底口径 = "解析不到就留空"
// （诚实：不猜别的厂商私有字段，也不拿空串当"有内容"）。
//
// 为什么以前会硬失败：G14 之前 content 畸形只影响 content 自己；G14 引入思维链字段后
// 思维链复用了本类型，于是"思维链是对象"这种**完全无害**的畸形会把整个响应解码打掉
// → 整轮模型调用报错（一次网关的私有形态就够了）。现在解析失败一律吞掉返回 ""。
type flexText string

// UnmarshalJSON implements json.Unmarshaler.
func (f *flexText) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "" || s == "null" {
		*f = ""
		return nil
	}
	switch s[0] {
	case '"':
		var str string
		if err := json.Unmarshal(b, &str); err != nil {
			*f = "" // 畸形字符串 → 留空（不硬失败）
			return nil
		}
		*f = flexText(str)
		return nil
	case '[':
		var parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(b, &parts); err != nil {
			// **字符串数组**兜底（["a","b"]）：有些网关把 content 直接发成字符串数组。
			// 解不出分块数组时按字符串数组拼（宽容方向一致：能还原文本就别静默空答——
			// 静默空答会让用户以为模型什么都没说）。
			var strs []string
			if err := json.Unmarshal(b, &strs); err == nil {
				*f = flexText(strings.Join(strs, ""))
				return nil
			}
			*f = "" // 真的解不出 → 留空（不硬失败）
			return nil
		}
		var sb strings.Builder
		for _, p := range parts {
			if p.Type == "" || p.Type == "text" {
				sb.WriteString(p.Text)
			}
		}
		*f = flexText(sb.String())
		return nil
	default:
		// 对象 / 布尔 / 数字：**未知形状** → 留空（解析不到就留空，诚实）。
		*f = ""
		return nil
	}
}

// flexReasoning 是思维链字段的**独立宽容类型**（G14 回归修复）。
//
// 为什么单独一个类型（而不是复用 flexText）：思维链是**可选信息**，它的畸形形态
// （对象 / 布尔 / 数字……）必须被**吞掉**——"解析不到就留空"要真的兑现：
//   - 不报错：一条畸形思维链**绝不该**让整轮模型调用失败（那是最严重的回归：
//     模型本来答得好好的，只因为网关把思维链包成对象就整轮报废）；
//   - 不编造：留空串 = "没有思维链"，不猜别的厂商私有字段（诚实）。
//
// 它复用 flexText 的取值口径（字符串 / 分块数组），只是**永不返回错误**。
type flexReasoning string

// UnmarshalJSON implements json.Unmarshaler.
func (r *flexReasoning) UnmarshalJSON(b []byte) error {
	var t flexText
	if err := t.UnmarshalJSON(b); err != nil {
		*r = "" // 解析失败 → 吞掉，留空（兑现"解析不到留空"）
		return nil
	}
	*r = flexReasoning(t)
	return nil
}

// flexArgs 宽容解析 function.arguments：官方形态是**内含 JSON 的字符串**
// （"{\"path\":\"a\"}"），部分网关直接给对象。两种都收成"原始 JSON 字节"，
// 再由 normalizeArgs 兜底空值。**不做重新编码**——保留 provider 原文（S4 精神）。
type flexArgs []byte

// reasoningOf 从思维链的两个候选字段取非空值（G14）：reasoning_content 优先
// （DeepSeek 风最常见），退到 reasoning。两个都空 → 空串（解析不到就留空——
// 不猜别的厂商私有字段，诚实）。
//
// 两个字段都是 flexReasoning（独立宽容类型）：畸形形态已被它们各自吞掉，这里
// 只做"取非空"。
func reasoningOf(primary, fallback flexReasoning) string {
	if s := string(primary); s != "" {
		return s
	}
	return string(fallback)
}

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
