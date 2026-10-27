package openai

// P2-4：SSE 里缺 tool_calls[].index 的处理。
//
// 多调用流里缺 index = 这个增量可能属于**别的**调用（按位置兜底会把参数并到别的
// 调用上 → 污染参数）→ 必须 fail-loud（emit error + 返回错误）；
// 单调用场景保留兜底（个别网关不发 index）。

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core"
)

// TestStreamSingleCallWithoutIndexKeepsFallback：整条流只有**一个**调用且全程缺
// index → 保留兜底（参数照样拼对）。
func TestStreamSingleCallWithoutIndexKeepsFallback(t *testing.T) {
	_, p := newTestServer(t, nil, sseWrite(
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"id":"call_A","function":{"name":"read","arguments":"{\"pa"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"function":{"arguments":"th\":\"a.txt\"}"}}]}}]}`,
		`[DONE]`,
	))
	p.stream = true

	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err != nil {
		t.Fatalf("单调用缺 index 应保留兜底: %v", err)
	}
	if len(res.ToolCalls) != 1 {
		t.Fatalf("ToolCalls = %+v，want 1", res.ToolCalls)
	}
	if string(res.ToolCalls[0].ID) != "call_A" || string(res.ToolCalls[0].Name) != "read" {
		t.Fatalf("tool_call = %+v", res.ToolCalls[0])
	}
	if string(res.ToolCalls[0].Args) != `{"path":"a.txt"}` {
		t.Fatalf("args 拼接错：%q", res.ToolCalls[0].Args)
	}
	if k := kinds(*evs); !reflect.DeepEqual(k, []string{KindToolCallStart, KindToolCallDelta, KindToolCallDelta, KindDone}) {
		t.Fatalf("事件序列 = %v", k)
	}
}

// TestStreamMultiCallMissingIndexFailsLoud：多调用流里出现缺 index 的 chunk →
// fail-loud：返回错误 + emit 一条 error 事件（不把参数并到别的调用上）。
func TestStreamMultiCallMissingIndexFailsLoud(t *testing.T) {
	cases := []struct {
		name   string
		chunks []string
	}{
		{
			name: "先有带 index 的调用，随后出现缺 index 的调用",
			chunks: []string{
				`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_A","function":{"name":"read","arguments":"{}"}}]}}]}`,
				`{"choices":[{"index":0,"delta":{"tool_calls":[{"id":"call_B","function":{"name":"bash","arguments":"{}"}}]}}]}`,
				`[DONE]`,
			},
		},
		{
			name: "先缺 index（兜底），随后出现第二个带 index 的调用",
			chunks: []string{
				`{"choices":[{"index":0,"delta":{"tool_calls":[{"id":"call_A","function":{"name":"read","arguments":"{}"}}]}}]}`,
				`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"call_B","function":{"name":"bash","arguments":"{}"}}]}}]}`,
				`[DONE]`,
			},
		},
		{
			name: "同一个 chunk 里两个 delta，其中一个缺 index",
			chunks: []string{
				`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_A","function":{"name":"read","arguments":"{}"}},{"id":"call_B","function":{"name":"bash","arguments":"{}"}}]}}]}`,
				`[DONE]`,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, p := newTestServer(t, nil, sseWrite(tc.chunks...))
			p.stream = true

			emit, evs := collectEvents()
			res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
			if err == nil {
				t.Fatalf("多调用流缺 index 必须 fail-loud（不得猜归属），得到 %+v", res.ToolCalls)
			}
			if !strings.Contains(err.Error(), "tool_calls[].index") {
				t.Fatalf("错误信息应说明缺 index: %v", err)
			}
			// fail() 统一先 emit error 事件再返回错误（UI 看得到原因）。
			k := kinds(*evs)
			if len(k) == 0 || k[len(k)-1] != KindError {
				t.Fatalf("应 emit 一条 error 事件，事件序列 = %v", k)
			}
		})
	}
}
