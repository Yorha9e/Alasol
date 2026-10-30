package openai

// G14 验收：思维链（reasoning）的**解析**与 reasoning_effort 的**透传**。
//
// 解析口径（诚实，不猜）：只看网关最常见的两种字段——DeepSeek 风 `reasoning_content`
// （流式在 delta.reasoning_content）与部分端点的 `reasoning`；reasoning_content 优先。
// 两个都没有 → Reasoning 空串（"解析不到"不留空串占位，也不编别的厂商私有字段）。
// 透传口径：仅当 ModelRequest.ReasoningEffort 设置了才下发 reasoning_effort。

import (
	"context"
	"reflect"
	"testing"

	"github.com/Yorha9e/alasol/core"
)

func effPtr(s string) *string { return &s }

// TestReasoningEffortPassthrough：设了 → 请求体带 reasoning_effort（trim 后原样）；
// 没设（nil / 空白串）→ **不下发该字段**（端点用默认档）。
func TestReasoningEffortPassthrough(t *testing.T) {
	cases := []struct {
		name string
		req  *string
		want *string // nil = 请求体不该有该字段
	}{
		{"nil 不传", nil, nil},
		{"空串不传", effPtr(""), nil},
		{"空白串不传", effPtr("   "), nil},
		{"low 透传", effPtr("low"), effPtr("low")},
		{"xhigh 透传", effPtr("xhigh"), effPtr("xhigh")},
		{"带空白 trim 后透传", effPtr("  high  "), effPtr("high")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got capturedRequest
			_, p := newTestServer(t, func(cr *capturedRequest) { got = *cr }, jsonWrite(`{
				"choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}}]
			}`))
			_, err := p.Run(context.Background(), core.ModelRequest{Model: "m", ReasoningEffort: tc.req}, func(core.ProviderEvent) {})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if !reflect.DeepEqual(got.ReasoningEffort, tc.want) {
				t.Errorf("请求体 reasoning_effort = %v，want %v", got.ReasoningEffort, tc.want)
			}
		})
	}
}

// TestNonStreamReasoningContent：非流式响应带 message.reasoning_content（DeepSeek 风）
// → ModelResult.Reasoning 收录 + 先发一条 reasoning.delta 再发正文 delta。
func TestNonStreamReasoningContent(t *testing.T) {
	_, p := newTestServer(t, nil, jsonWrite(`{
		"choices": [{"index": 0, "message": {"role": "assistant", "content": "答案", "reasoning_content": "先想一步"}, "finish_reason": "stop"}]
	}`))
	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Reasoning != "先想一步" {
		t.Errorf("Reasoning = %q，want 先想一步", res.Reasoning)
	}
	want := []string{KindReasoningDelta, KindTextDelta, KindDone}
	if k := kinds(*evs); !reflect.DeepEqual(k, want) {
		t.Fatalf("事件序列 = %v，want %v", k, want)
	}
	if (*evs)[0].Reasoning != "先想一步" || (*evs)[0].Text != "" {
		t.Errorf("reasoning.delta = %+v（只带 Reasoning，不带 Text）", (*evs)[0])
	}
	if (*evs)[1].Text != "答案" || (*evs)[1].Reasoning != "" {
		t.Errorf("text.delta = %+v（只带 Text）", (*evs)[1])
	}
}

// TestNonStreamReasoningFallbackField：部分端点用 message.reasoning（非 _content 后缀）
// ——同样要解析到。
func TestNonStreamReasoningFallbackField(t *testing.T) {
	_, p := newTestServer(t, nil, jsonWrite(`{
		"choices": [{"index": 0, "message": {"role": "assistant", "content": "答案", "reasoning": "另一个字段名"}}]
	}`))
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Reasoning != "另一个字段名" {
		t.Errorf("Reasoning = %q，want 另一个字段名", res.Reasoning)
	}
}

// TestNonStreamNoReasoningStaysEmpty：响应没有任何思维链字段 → Reasoning 空串、
// 且**不发** reasoning.delta（没有内容就不造事件）。
func TestNonStreamNoReasoningStaysEmpty(t *testing.T) {
	_, p := newTestServer(t, nil, jsonWrite(`{
		"choices": [{"index": 0, "message": {"role": "assistant", "content": "答案"}}]
	}`))
	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Reasoning != "" {
		t.Errorf("Reasoning = %q，want 空（解析不到就留空）", res.Reasoning)
	}
	if k := kinds(*evs); !reflect.DeepEqual(k, []string{KindTextDelta, KindDone}) {
		t.Errorf("事件序列 = %v，want [text.delta done]（无思维链则无 reasoning.delta）", k)
	}
}

// TestStreamReasoningContentDeltas：流式 delta.reasoning_content 逐段累积 +
// 边收边发 reasoning.delta（与正文 delta 交错，消费端按 Kind 分流）。
func TestStreamReasoningContentDeltas(t *testing.T) {
	_, p := newTestServer(t, nil, sseWrite(
		`{"id":"c1","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"让"}}]}`,
		`{"choices":[{"index":0,"delta":{"reasoning_content":"我想想"}}]}`,
		`{"choices":[{"index":0,"delta":{"content":"答"}}]}`,
		`{"choices":[{"index":0,"delta":{"content":"案"}}]}`,
		`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		`[DONE]`,
	))
	p.stream = true
	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Reasoning != "让我想想" {
		t.Errorf("Reasoning = %q，want 让我想想", res.Reasoning)
	}
	if res.Text != "答案" {
		t.Errorf("Text = %q，want 答案", res.Text)
	}
	want := []string{
		KindReasoningDelta, KindReasoningDelta,
		KindTextDelta, KindTextDelta,
		KindDone,
	}
	if k := kinds(*evs); !reflect.DeepEqual(k, want) {
		t.Fatalf("事件序列 = %v，want %v", k, want)
	}
	if (*evs)[0].Reasoning != "让" || (*evs)[1].Reasoning != "我想想" {
		t.Errorf("reasoning 片段 = %q / %q", (*evs)[0].Reasoning, (*evs)[1].Reasoning)
	}
}

// TestStreamReasoningFallbackField：流式 delta.reasoning（无 _content 后缀）同样解析。
func TestStreamReasoningFallbackField(t *testing.T) {
	_, p := newTestServer(t, nil, sseWrite(
		`{"choices":[{"index":0,"delta":{"reasoning":"推理"}}]}`,
		`{"choices":[{"index":0,"delta":{"content":"结论"}}]}`,
		`[DONE]`,
	))
	p.stream = true
	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Reasoning != "推理" || res.Text != "结论" {
		t.Errorf("res = %+v，want Reasoning=推理 Text=结论", res)
	}
	if k := kinds(*evs); !reflect.DeepEqual(k, []string{KindReasoningDelta, KindTextDelta, KindDone}) {
		t.Errorf("事件序列 = %v", k)
	}
}

// TestStreamReasoningContentWinsOverReasoning：同一 chunk 两个字段都带时
// reasoning_content 优先（不拼接、不双发——避免同一段思维链被记两遍）。
func TestStreamReasoningContentWinsOverReasoning(t *testing.T) {
	_, p := newTestServer(t, nil, sseWrite(
		`{"choices":[{"index":0,"delta":{"reasoning_content":"正主","reasoning":"备胎"}}]}`,
		`[DONE]`,
	))
	p.stream = true
	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Reasoning != "正主" {
		t.Errorf("Reasoning = %q，want 正主（reasoning_content 优先）", res.Reasoning)
	}
	if len(*evs) != 2 || (*evs)[0].Reasoning != "正主" {
		t.Errorf("事件 = %+v，want 恰一条 reasoning.delta(正主) + done", *evs)
	}
}

// TestStreamNoReasoningStaysEmpty：流式无任何思维链字段 → 空 + 无 reasoning.delta。
func TestStreamNoReasoningStaysEmpty(t *testing.T) {
	_, p := newTestServer(t, nil, sseWrite(
		`{"choices":[{"index":0,"delta":{"content":"就"}}]}`,
		`{"choices":[{"index":0,"delta":{"content":"这样"}}]}`,
		`[DONE]`,
	))
	p.stream = true
	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Reasoning != "" {
		t.Errorf("Reasoning = %q，want 空", res.Reasoning)
	}
	for _, ev := range *evs {
		if ev.Kind == KindReasoningDelta {
			t.Fatalf("不该有 reasoning.delta：%+v", ev)
		}
	}
}
