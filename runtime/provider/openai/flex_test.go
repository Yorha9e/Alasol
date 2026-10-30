package openai

// G14 回归修复的钉子：**畸形形态的思维链字段不许打掉整轮模型调用**。
//
// 背景：G14 引入思维链字段时复用了 flexText（content 的宽容类型）。flexText 当时遇到
// **对象 / 布尔 / 数字**形态会**返回错误** → 整个响应反序列化失败 → 整轮模型调用报错。
// 可网关把思维链包成对象是完全无害的事（模型照样答得好好的）——于是一条畸形字段就把
// 整轮打掉（G14 之前这些字段压根不存在，未知字段被忽略，无害）。
//
// 修法：思维链字段用**独立宽容类型**（flexReasoning：解析失败吞掉、返回 ""），且
// flexText 自身对未知形状也兜底空串（content 一并受益）。

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Yorha9e/alasol/core"
)

// TestObjectShapedReasoningDoesNotFailResponse：**对象形态**的 reasoning_content →
// 整轮调用**不报错**，思维链留空（解析不到就留空——不猜别的厂商私有字段），正文照常。
func TestObjectShapedReasoningDoesNotFailResponse(t *testing.T) {
	_, p := newTestServer(t, nil, jsonWrite(`{
		"choices": [{"index": 0, "message": {"role": "assistant", "content": "答案",
			"reasoning_content": {"type": "text", "text": "网关包成对象的思维链"}},
			"finish_reason": "stop"}]
	}`))
	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err != nil {
		t.Fatalf("对象形态的思维链不该让整轮失败: %v", err)
	}
	if res.Text != "答案" {
		t.Errorf("Text = %q，want 答案（正文不受影响）", res.Text)
	}
	if res.Reasoning != "" {
		t.Errorf("Reasoning = %q，want 空（对象形态解析不到 → 留空，不猜）", res.Reasoning)
	}
	// 没有思维链内容 → 不发 reasoning.delta（没有内容就不造事件）。
	want := []string{KindTextDelta, KindDone}
	if k := kinds(*evs); !reflect.DeepEqual(k, want) {
		t.Errorf("事件序列 = %v，want %v", k, want)
	}
}

// TestMalformedReasoningShapesDoNotFailResponse：布尔 / 数字 / 空数组等畸形形态 →
// 一律**不报错**、思维链留空。
func TestMalformedReasoningShapesDoNotFailResponse(t *testing.T) {
	shapes := []string{
		`true`,
		`false`,
		`123`,
		`[]`,
		`{"type":"reasoning"}`,
		`[{"type":"text"}]`,
	}
	for _, shape := range shapes {
		t.Run(shape, func(t *testing.T) {
			_, p := newTestServer(t, nil, jsonWrite(`{
				"choices": [{"index": 0, "message": {"role": "assistant", "content": "答案",
					"reasoning_content": `+shape+`}, "finish_reason": "stop"}]
			}`))
			res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
			if err != nil {
				t.Fatalf("畸形思维链（%s）不该让整轮失败: %v", shape, err)
			}
			if res.Text != "答案" {
				t.Errorf("Text = %q，want 答案", res.Text)
			}
			if res.Reasoning != "" {
				t.Errorf("Reasoning = %q，want 空（畸形 → 留空）", res.Reasoning)
			}
		})
	}
}

// TestStreamObjectShapedReasoningDoesNotFailStream：流式同理——畸形思维链不许打断整条流。
func TestStreamObjectShapedReasoningDoesNotFailStream(t *testing.T) {
	_, p := newTestServer(t, nil, sseWrite(
		`{"choices":[{"index":0,"delta":{"reasoning_content":{"type":"text","text":"对象思维链"}}}]}`,
		`{"choices":[{"index":0,"delta":{"content":"答"}}]}`,
		`{"choices":[{"index":0,"delta":{"content":"案"}}]}`,
		`[DONE]`,
	))
	p.stream = true
	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err != nil {
		t.Fatalf("流式畸形思维链不该让整轮失败: %v", err)
	}
	if res.Text != "答案" {
		t.Errorf("Text = %q，want 答案", res.Text)
	}
	if res.Reasoning != "" {
		t.Errorf("Reasoning = %q，want 空（畸形 → 留空）", res.Reasoning)
	}
	for _, ev := range *evs {
		if ev.Kind == KindReasoningDelta {
			t.Fatalf("不该有 reasoning.delta（没有思维链内容）: %+v", ev)
		}
	}
}

// TestObjectShapedContentDoesNotFailResponse：content 自身是对象形态 → 同样**不报错**
// （content 也受益：未知形状兜底空串，而不是把整轮打掉）。
func TestObjectShapedContentDoesNotFailResponse(t *testing.T) {
	_, p := newTestServer(t, nil, jsonWrite(`{
		"choices": [{"index": 0, "message": {"role": "assistant",
			"content": {"type": "text", "text": "对象形态的正文"}}, "finish_reason": "stop"}]
	}`))
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err != nil {
		t.Fatalf("对象形态的 content 不该让整轮失败: %v", err)
	}
	if res.Text != "" {
		t.Errorf("Text = %q，want 空（对象形态不在已知形态内 → 留空，不猜）", res.Text)
	}
}

// TestFlexTextUnknownShapesAreEmpty：flexText 对未知形状一律留空、**不返回错误**。
func TestFlexTextUnknownShapesAreEmpty(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{`"abc"`, "abc"},
		{`null`, ""},
		{`[]`, ""},
		{`[{"type":"text","text":"a"},{"type":"text","text":"b"}]`, "ab"},
		{`["a","b"]`, "ab"}, // 字符串数组 → 拼接（不许静默空答）
		{`["只有一段"]`, "只有一段"},
		{`{"type":"text","text":"x"}`, ""},
		{`true`, ""},
		{`123`, ""},
	}
	for _, tc := range cases {
		var ft flexText
		if err := json.Unmarshal([]byte(tc.raw), &ft); err != nil {
			t.Errorf("flexText(%s) 返回错误 %v（不该硬失败）", tc.raw, err)
			continue
		}
		if string(ft) != tc.want {
			t.Errorf("flexText(%s) = %q，want %q", tc.raw, string(ft), tc.want)
		}
	}
}

// TestStringArrayContentDoesNotAnswerEmpty（F4）：content 是**字符串数组**（["a","b"]）→
// 拼成 "ab"；**不许静默空答**（空答会让用户以为模型什么都没说）。
func TestStringArrayContentDoesNotAnswerEmpty(t *testing.T) {
	_, p := newTestServer(t, nil, jsonWrite(`{
		"choices": [{"index": 0, "message": {"role": "assistant",
			"content": ["第一段", "第二段"]}, "finish_reason": "stop"}]
	}`))
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err != nil {
		t.Fatalf("字符串数组形态的 content 不该让整轮失败: %v", err)
	}
	if res.Text != "第一段第二段" {
		t.Errorf("Text = %q，want %q（字符串数组应拼接，不该静默空答）", res.Text, "第一段第二段")
	}
}

// TestFlexReasoningNeverFails：flexReasoning **永不返回错误**（兑现"解析不到留空"）。
func TestFlexReasoningNeverFails(t *testing.T) {
	for _, raw := range []string{`"x"`, `{"a":1}`, `true`, `123`, `null`, `[{"type":"text","text":"y"}]`} {
		var fr flexReasoning
		if err := json.Unmarshal([]byte(raw), &fr); err != nil {
			t.Errorf("flexReasoning(%s) 返回错误 %v（思维链不许硬失败）", raw, err)
		}
	}
}
