package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// ---- 测试基建 ----

// capturedRequest 是测试里用来核对请求体形状的松散结构（不引入第二套 wire 类型）。
type capturedRequest struct {
	Model         string           `json:"model"`
	Stream        bool             `json:"stream"`
	Messages      []map[string]any `json:"messages"`
	Tools         []map[string]any `json:"tools"`
	StreamOptions map[string]any   `json:"stream_options"`
	// ReasoningEffort 思维链档位（G14）：**指针**——JSON 里缺省时为 nil，
	// 借此区分"没发该字段"与"发了空串"（契约口径：不设就不发）。
	ReasoningEffort *string `json:"reasoning_effort"`
}

// newTestServer 起一个 httptest 服务，把请求体解码后交给 check（可为 nil），
// 再用 write 回包。返回服务与 Provider（BaseURL 指向该服务）。
func newTestServer(t *testing.T, check func(*capturedRequest), write func(w http.ResponseWriter, r *http.Request)) (*httptest.Server, *Provider) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("请求路径 = %q，期望 /v1/chat/completions", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("请求方法 = %q，期望 POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q，期望 application/json", got)
		}
		if check != nil {
			var cr capturedRequest
			if err := json.NewDecoder(r.Body).Decode(&cr); err != nil {
				t.Errorf("解码请求体失败：%v", err)
				return
			}
			check(&cr)
		}
		write(w, r)
	}))
	t.Cleanup(srv.Close)
	p := New(Options{BaseURL: srv.URL + "/v1", APIKey: "test-key"})
	return srv, p
}

// sseWrite 把若干 chunk 以 SSE 形态写出（每块一帧，立即 flush）。
func sseWrite(chunks ...string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			if fl != nil {
				fl.Flush()
			}
		}
	}
}

// jsonWrite 回一个非流式 JSON 包。
func jsonWrite(body string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}
}

// collectEvents 收事件供断言。
func collectEvents() (func(core.ProviderEvent), *[]core.ProviderEvent) {
	var mu sync.Mutex
	evs := &[]core.ProviderEvent{}
	return func(ev core.ProviderEvent) {
		mu.Lock()
		defer mu.Unlock()
		*evs = append(*evs, ev)
	}, evs
}

func kinds(evs []core.ProviderEvent) []string {
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.Kind)
	}
	return out
}

func strPtr(s string) *string { return &s }

// ---- 构造 ----

func TestNewDefaults(t *testing.T) {
	if got := New(Options{}).BaseURL(); got != DefaultBaseURL {
		t.Errorf("零值 BaseURL = %q，期望 %q", got, DefaultBaseURL)
	}
	if got := New(Options{BaseURL: "http://127.0.0.1:1/v1/"}).BaseURL(); got != "http://127.0.0.1:1/v1" {
		t.Errorf("尾斜杠未去掉：%q", got)
	}
	if got := New(Options{BaseURL: "   "}).BaseURL(); got != DefaultBaseURL {
		t.Errorf("空白 BaseURL = %q，期望默认值", got)
	}
}

// TestOptionsStreamFlag 走公开构造入口打开流式（其余流式用例为省事直接翻 p.stream，
// 这里补一条"Options.Stream=true → 请求体 stream:true + include_usage"的端到端验证）。
func TestOptionsStreamFlag(t *testing.T) {
	var got capturedRequest
	srv, _ := newTestServer(t, func(cr *capturedRequest) { got = *cr }, sseWrite(`[DONE]`))
	p := New(Options{BaseURL: srv.URL + "/v1", APIKey: "k", Stream: true})

	if _, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !got.Stream {
		t.Error("Options.Stream=true 未下发 stream:true")
	}
	if got.StreamOptions["include_usage"] != true {
		t.Errorf("stream_options = %v，期望 include_usage=true", got.StreamOptions)
	}
	if got.Model != "m" {
		t.Errorf("model = %q", got.Model)
	}
}

// ---- 非流式 ----

func TestRunNonStreamText(t *testing.T) {
	var got capturedRequest
	_, p := newTestServer(t, func(cr *capturedRequest) { got = *cr }, jsonWrite(`{
		"id": "chatcmpl-1",
		"choices": [{"index": 0, "message": {"role": "assistant", "content": "你好，世界"}, "finish_reason": "stop"}],
		"usage": {"prompt_tokens": 11, "completion_tokens": 3, "total_tokens": 14}
	}`))

	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{
		Model:  "gpt-test",
		System: "你是助手",
		Messages: []core.Message{
			{Role: "user", Content: "hi"},
		},
	}, emit)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "你好，世界" {
		t.Errorf("Text = %q", res.Text)
	}
	if res.Usage.InputTokens != 11 || res.Usage.OutputTokens != 3 {
		t.Errorf("Usage = %+v", res.Usage)
	}
	if len(res.ToolCalls) != 0 {
		t.Errorf("不该有 ToolCalls：%+v", res.ToolCalls)
	}
	if k := kinds(*evs); !reflect.DeepEqual(k, []string{KindTextDelta, KindDone}) {
		t.Errorf("事件序列 = %v，期望 [text.delta done]", k)
	}
	if (*evs)[0].Text != "你好，世界" || (*evs)[1].Text != "你好，世界" {
		t.Errorf("事件文本 = %q / %q", (*evs)[0].Text, (*evs)[1].Text)
	}

	// 请求体：stream=false、system 置顶、模型名透传。
	if got.Model != "gpt-test" {
		t.Errorf("请求 model = %q", got.Model)
	}
	if got.Stream {
		t.Error("非流式 Provider 不应下发 stream=true")
	}
	if got.StreamOptions != nil {
		t.Errorf("非流式不应带 stream_options：%v", got.StreamOptions)
	}
	if len(got.Messages) != 2 || got.Messages[0]["role"] != "system" || got.Messages[1]["role"] != "user" {
		t.Errorf("消息映射不对：%v", got.Messages)
	}
	if got.Messages[0]["content"] != "你是助手" {
		t.Errorf("system 内容 = %v", got.Messages[0]["content"])
	}
}

func TestRunNonStreamToolCalls(t *testing.T) {
	const verbatimID = "Call_AbC-xyz_9" // 故意混大小写 + 特殊字符：黑盒不许被规范化（S4/陷阱 17）
	var got capturedRequest
	_, p := newTestServer(t, func(cr *capturedRequest) { got = *cr }, jsonWrite(`{
		"choices": [{"index": 0, "message": {"role": "assistant", "content": "", "tool_calls": [
			{"id": "`+verbatimID+`", "type": "function", "function": {"name": "read", "arguments": "{\"path\":\"a.txt\"}"}},
			{"id": "call_2", "type": "function", "function": {"name": "bash", "arguments": ""}}
		]}, "finish_reason": "tool_calls"}],
		"usage": {"prompt_tokens": 5, "completion_tokens": 6}
	}`))

	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{
		Model: "m",
		Tools: []contracts.Tool{
			{Name: "read", Description: strPtr("读文件"), Schema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`)},
			{Name: "bash", Schema: nil}, // 无 Schema → 兜底 parameters
		},
	}, emit)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.ToolCalls) != 2 {
		t.Fatalf("ToolCalls = %+v", res.ToolCalls)
	}
	if string(res.ToolCalls[0].ID) != verbatimID {
		t.Errorf("tool_call_id 被改写：%q，期望原样 %q", res.ToolCalls[0].ID, verbatimID)
	}
	if res.ToolCalls[0].Name != "read" {
		t.Errorf("name = %q", res.ToolCalls[0].Name)
	}
	if string(res.ToolCalls[0].Args) != `{"path":"a.txt"}` {
		t.Errorf("args 不是 provider 原文：%q", res.ToolCalls[0].Args)
	}
	if string(res.ToolCalls[1].Args) != `{}` {
		t.Errorf("空 arguments 应规范化为 {}：%q", res.ToolCalls[1].Args)
	}

	// 事件：空文本不发 text.delta；两个工具各一条 start；最后 done。
	if k := kinds(*evs); !reflect.DeepEqual(k, []string{KindToolCallStart, KindToolCallStart, KindDone}) {
		t.Errorf("事件序列 = %v", k)
	}
	if (*evs)[0].ToolCall == nil || string((*evs)[0].ToolCall.ID) != verbatimID {
		t.Errorf("start 事件没带上原样 id：%+v", (*evs)[0].ToolCall)
	}

	// 请求体：tools 映射成 function 形态，parameters 用 Schema 原样。
	if len(got.Tools) != 2 {
		t.Fatalf("tools = %v", got.Tools)
	}
	if got.Tools[0]["type"] != "function" {
		t.Errorf("tools[0].type = %v", got.Tools[0]["type"])
	}
	fn, _ := got.Tools[0]["function"].(map[string]any)
	if fn["name"] != "read" || fn["description"] != "读文件" {
		t.Errorf("function = %v", fn)
	}
	wantParams := map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}, "required": []any{"path"}}
	if !reflect.DeepEqual(fn["parameters"], wantParams) {
		t.Errorf("parameters 未原样透传：%#v", fn["parameters"])
	}
	if fn2, _ := got.Tools[1]["function"].(map[string]any); !reflect.DeepEqual(fn2["parameters"], map[string]any{"type": "object", "properties": map[string]any{}}) {
		t.Errorf("无 Schema 时应落兜底 parameters：%#v", fn2["parameters"])
	}
}

func TestRunNonStreamContentArrayForm(t *testing.T) {
	// 部分兼容网关把 content 发成分块数组（Anthropic 风格）——宽容解析。
	_, p := newTestServer(t, nil, jsonWrite(`{
		"choices": [{"index": 0, "message": {"role": "assistant", "content": [
			{"type": "text", "text": "第一段"},
			{"type": "text", "text": "第二段"}
		]}}]
	}`))
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "第一段第二段" {
		t.Errorf("Text = %q", res.Text)
	}
}

func TestRunNonStreamArgumentsObjectForm(t *testing.T) {
	// 部分网关把 arguments 直接发成对象而不是字符串。
	_, p := newTestServer(t, nil, jsonWrite(`{
		"choices": [{"index": 0, "message": {"role": "assistant", "tool_calls": [
			{"id": "c1", "function": {"name": "read", "arguments": {"path": "a.txt"}}}
		]}}]
	}`))
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.ToolCalls) != 1 || string(res.ToolCalls[0].Args) != `{"path": "a.txt"}` {
		t.Errorf("Args = %q（对象形态应原样保留字节，不重新编码）", res.ToolCalls[0].Args)
	}
}

// ---- 消息映射 ----

func TestRunMessageMappingToolRoundTrip(t *testing.T) {
	const toolCallID = "call_RoundTrip_42"
	var got capturedRequest
	_, p := newTestServer(t, func(cr *capturedRequest) { got = *cr }, jsonWrite(`{"choices":[{"message":{"content":"ok"}}]}`))

	_, err := p.Run(context.Background(), core.ModelRequest{
		Model: "m",
		Messages: []core.Message{
			{Role: "user", Content: "读一下"},
			{Role: "assistant", Content: "好", ToolCalls: []core.ToolCallReq{
				{ID: contracts.ProviderId(toolCallID), Name: "read", Args: json.RawMessage(`{"path":"a.txt"}`)},
			}},
			{Role: "tool", ToolCallID: contracts.ProviderId(toolCallID), Content: "文件内容"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got.Messages) != 3 {
		t.Fatalf("messages = %v", got.Messages)
	}
	asst := got.Messages[1]
	if asst["role"] != "assistant" || asst["content"] != "好" {
		t.Errorf("assistant = %v", asst)
	}
	tcs, _ := asst["tool_calls"].([]any)
	if len(tcs) != 1 {
		t.Fatalf("tool_calls = %v", asst["tool_calls"])
	}
	tc, _ := tcs[0].(map[string]any)
	if tc["id"] != toolCallID || tc["type"] != "function" {
		t.Errorf("tool_call = %v", tc)
	}
	fn, _ := tc["function"].(map[string]any)
	if fn["name"] != "read" {
		t.Errorf("function = %v", fn)
	}
	// arguments 必须是**字符串**（内含 JSON），不是嵌套对象。
	if fn["arguments"] != `{"path":"a.txt"}` {
		t.Errorf("arguments = %#v，期望原样 JSON 字符串", fn["arguments"])
	}
	tool := got.Messages[2]
	if tool["role"] != "tool" || tool["tool_call_id"] != toolCallID || tool["content"] != "文件内容" {
		t.Errorf("tool 消息 = %v", tool)
	}
}

func TestRunInvalidRequests(t *testing.T) {
	cases := []struct {
		name string
		req  core.ModelRequest
		want string
	}{
		{"缺 model", core.ModelRequest{Messages: []core.Message{{Role: "user"}}}, "Model"},
		{"角色不认识", core.ModelRequest{Model: "m", Messages: []core.Message{{Role: "banana"}}}, "Role"},
		{"tool 消息缺 id", core.ModelRequest{Model: "m", Messages: []core.Message{{Role: "tool", Content: "x"}}}, "ToolCallID"},
		{"assistant 工具调用缺 id", core.ModelRequest{Model: "m", Messages: []core.Message{{Role: "assistant", ToolCalls: []core.ToolCallReq{{Name: "read"}}}}}, "ID"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, p := newTestServer(t, func(*capturedRequest) {
				t.Error("非法请求不应发到端点（fail-loud 在本地就拦掉）")
			}, jsonWrite(`{"choices":[{"message":{"content":"不该到达"}}]}`))
			emit, evs := collectEvents()
			_, err := p.Run(context.Background(), c.req, emit)
			if err == nil {
				t.Fatal("期望报错")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("错误文案 %q 未提及 %q", err, c.want)
			}
			if k := kinds(*evs); len(k) != 1 || k[0] != KindError {
				t.Errorf("事件 = %v，期望单条 error", k)
			}
		})
	}
}

// ---- 流式 ----

func TestRunStreamTextAndToolCalls(t *testing.T) {
	var got capturedRequest
	srv, p := newTestServer(t, func(cr *capturedRequest) { got = *cr }, sseWrite(
		`{"id":"c1","choices":[{"index":0,"delta":{"role":"assistant","content":"你"}}]}`,
		`{"choices":[{"index":0,"delta":{"content":"好"}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_A","type":"function","function":{"name":"read","arguments":"{\"pa"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"th\":\"a.txt\"}"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"call_B","function":{"name":"bash","arguments":"{}"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		`{"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":9,"total_tokens":16}}`,
		`[DONE]`,
	))
	_ = srv
	p.stream = true // 直接翻开关（Options.Stream 的入口由 TestRunStreamOptions 覆盖）

	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "你好" {
		t.Errorf("Text = %q，期望 \"你好\"（分片要拼起来）", res.Text)
	}
	if len(res.ToolCalls) != 2 {
		t.Fatalf("ToolCalls = %+v", res.ToolCalls)
	}
	if string(res.ToolCalls[0].ID) != "call_A" || res.ToolCalls[0].Name != "read" || string(res.ToolCalls[0].Args) != `{"path":"a.txt"}` {
		t.Errorf("tool_calls[0] = %+v", res.ToolCalls[0])
	}
	if string(res.ToolCalls[1].ID) != "call_B" || res.ToolCalls[1].Name != "bash" || string(res.ToolCalls[1].Args) != `{}` {
		t.Errorf("tool_calls[1] = %+v", res.ToolCalls[1])
	}
	if res.Usage.InputTokens != 7 || res.Usage.OutputTokens != 9 {
		t.Errorf("Usage = %+v（usage-only chunk 要能被吃到）", res.Usage)
	}

	want := []string{
		KindTextDelta, KindTextDelta,
		KindToolCallStart, KindToolCallDelta, KindToolCallDelta,
		KindToolCallStart, KindToolCallDelta,
		KindDone,
	}
	if k := kinds(*evs); !reflect.DeepEqual(k, want) {
		t.Errorf("事件序列 = %v\n期望 %v", k, want)
	}
	// 第一个 start 必须一次带全 id + name（消费端不用拼）。
	sc := (*evs)[2]
	if sc.ToolCall == nil || string(sc.ToolCall.ID) != "call_A" || sc.ToolCall.Name != "read" {
		t.Errorf("start 事件 = %+v", sc.ToolCall)
	}
	// delta 只带**片段**，不是完整 JSON。
	if string((*evs)[3].ToolCall.Args) != `{"pa` {
		t.Errorf("delta[0] = %q", (*evs)[3].ToolCall.Args)
	}

	if !got.Stream || got.StreamOptions["include_usage"] != true {
		t.Errorf("流式请求体 = stream:%v options:%v", got.Stream, got.StreamOptions)
	}
}

func TestRunStreamToolCallIDArrivesLate(t *testing.T) {
	// 个别网关先给 name/args，id 在后续 chunk 才到：最终 ID 必须收得到，且不被改写。
	const lateID = "call_Late-ID_7"
	_, p := newTestServer(t, nil, sseWrite(
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"read","arguments":"{\"path\":"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"`+lateID+`","function":{"arguments":"\"x.txt\"}"}}]}}]}`,
		`[DONE]`,
	))
	p.stream = true

	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.ToolCalls) != 1 || string(res.ToolCalls[0].ID) != lateID {
		t.Fatalf("ToolCalls = %+v", res.ToolCalls)
	}
	if string(res.ToolCalls[0].Args) != `{"path":"x.txt"}` {
		t.Errorf("args 拼接错：%q", res.ToolCalls[0].Args)
	}
	if k := kinds(*evs); !reflect.DeepEqual(k, []string{KindToolCallStart, KindToolCallDelta, KindToolCallDelta, KindDone}) {
		t.Errorf("事件序列 = %v", k)
	}
}

func TestRunStreamRepeatedName(t *testing.T) {
	// 有的网关每个 chunk 重复完整 name：不能累加成 "readread"。
	_, p := newTestServer(t, nil, sseWrite(
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c","function":{"name":"read","arguments":"{\"a\":"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"read","arguments":"1}"}}]}}]}`,
		`[DONE]`,
	))
	p.stream = true
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ToolCalls[0].Name != "read" {
		t.Errorf("name = %q，期望 read（重复 name 不能被累加）", res.ToolCalls[0].Name)
	}
}

func TestRunStreamSplitName(t *testing.T) {
	// 另一种极端：name 被拆成多个片段，要能拼回。
	_, p := newTestServer(t, nil, sseWrite(
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c","function":{"name":"wr","arguments":"{"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"ite","arguments":"}"}}]}}]}`,
		`[DONE]`,
	))
	p.stream = true
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ToolCalls[0].Name != "write" {
		t.Errorf("name = %q，期望 write", res.ToolCalls[0].Name)
	}
}

func TestRunStreamContentArrayForm(t *testing.T) {
	_, p := newTestServer(t, nil, sseWrite(
		`{"choices":[{"index":0,"delta":{"content":[{"type":"text","text":"甲"},{"type":"text","text":"乙"}]}}]}`,
		`[DONE]`,
	))
	p.stream = true
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "甲乙" {
		t.Errorf("Text = %q", res.Text)
	}
}

func TestRunStreamMissingDoneSentinel(t *testing.T) {
	// 网关不发送 [DONE]：自然 EOF 也要能用已收到的内容组装结果。
	_, p := newTestServer(t, nil, sseWrite(`{"choices":[{"index":0,"delta":{"content":"半句话"}}]}`))
	p.stream = true
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "半句话" {
		t.Errorf("Text = %q", res.Text)
	}
}

func TestRunStreamCommentsAndUnknownLines(t *testing.T) {
	// 注释行（":" 开头）、event:/id: 行、空行都要被忽略。
	_, p := newTestServer(t, nil, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, ": keep-alive\n\nevent: message\nid: 7\n\n")
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	p.stream = true
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "x" {
		t.Errorf("Text = %q", res.Text)
	}
}

func TestRunStreamMidStreamError(t *testing.T) {
	_, p := newTestServer(t, nil, sseWrite(
		`{"choices":[{"index":0,"delta":{"content":"部分"}}]}`,
		`{"error":{"message":"upstream exploded","type":"server_error","code":"boom"}}`,
	))
	p.stream = true
	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err == nil {
		t.Fatal("期望流中错误")
	}
	if !strings.Contains(err.Error(), "upstream exploded") {
		t.Errorf("错误文案 = %v", err)
	}
	if !reflect.DeepEqual(res, core.ModelResult{}) {
		t.Errorf("失败时应返回零值 ModelResult：%+v", res)
	}
	if k := kinds(*evs); len(k) != 2 || k[0] != KindTextDelta || k[1] != KindError {
		t.Errorf("事件序列 = %v", k)
	}
}

func TestRunStreamUnparsableChunk(t *testing.T) {
	_, p := newTestServer(t, nil, sseWrite(`{"choices":[{"index":0,`))
	p.stream = true
	if _, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil); err == nil {
		t.Fatal("期望解析失败")
	}
}

func TestRunStreamToolCallWithoutName(t *testing.T) {
	_, p := newTestServer(t, nil, sseWrite(
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c","function":{"arguments":"{}"}}]}}]}`,
		`[DONE]`,
	))
	p.stream = true
	if _, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil); err == nil {
		t.Fatal("缺 name 的 tool_call 必须 fail-loud")
	}
}

// ---- HTTP 错误 ----

func TestRunHTTPStatusErrors(t *testing.T) {
	longBody := strings.Repeat("x", 4096)
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"400", http.StatusBadRequest, `{"error":{"message":"bad request"}}`},
		{"401", http.StatusUnauthorized, `{"error":{"message":"invalid api key"}}`},
		{"429", http.StatusTooManyRequests, `{"error":{"message":"rate limited"}}`},
		{"500", http.StatusInternalServerError, longBody},
		{"503", http.StatusServiceUnavailable, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, p := newTestServer(t, nil, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(c.status)
				fmt.Fprint(w, c.body)
			})
			emit, evs := collectEvents()
			_, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
			if err == nil {
				t.Fatal("期望非 2xx 报错")
			}
			var se *StatusError
			if !errors.As(err, &se) {
				t.Fatalf("错误不是 *StatusError：%T %v", err, err)
			}
			if se.StatusCode != c.status {
				t.Errorf("StatusCode = %d，期望 %d", se.StatusCode, c.status)
			}
			if len(se.Body) > maxErrorBody+64 {
				t.Errorf("body 未截断：%d 字节", len(se.Body))
			}
			if c.body != "" && !strings.Contains(se.Error(), "bad request") && c.status == 400 {
				t.Errorf("错误文案未带 body：%v", se)
			}
			if k := kinds(*evs); len(k) != 1 || k[0] != KindError {
				t.Errorf("事件 = %v，期望单条 error", k)
			}
		})
	}
}

func TestRunErrorObjectWith200(t *testing.T) {
	// 个别网关用 200 + error 对象表达失败。
	_, p := newTestServer(t, nil, jsonWrite(`{"error":{"message":"quota exceeded","type":"quota"}}`))
	if _, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil); err == nil ||
		!strings.Contains(err.Error(), "quota exceeded") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunEmptyChoices(t *testing.T) {
	_, p := newTestServer(t, nil, jsonWrite(`{"usage":{"prompt_tokens":1,"completion_tokens":0}}`))
	if _, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil); err == nil {
		t.Fatal("缺 choices 必须报错")
	}
}

// ---- 取消 / 超时 ----

func TestRunContextTimeoutNonStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer srv.Close()
	p := New(Options{BaseURL: srv.URL + "/v1"})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := p.Run(ctx, core.ModelRequest{Model: "m"}, nil)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("超时未及时返回：%v", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v，期望 errors.Is DeadlineExceeded", err)
	}
}

func TestRunContextTimeoutMidStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"开头\"}}]}\n\n")
		if fl != nil {
			fl.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer srv.Close()
	p := New(Options{BaseURL: srv.URL + "/v1", Stream: true})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	emit, evs := collectEvents()
	_, err := p.Run(ctx, core.ModelRequest{Model: "m"}, emit)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v，期望 DeadlineExceeded", err)
	}
	if k := kinds(*evs); len(k) != 2 || k[0] != KindTextDelta || k[1] != KindError {
		t.Errorf("事件序列 = %v，期望先 delta 后 error", k)
	}
}

func TestRunContextAlreadyCancelled(t *testing.T) {
	_, p := newTestServer(t, nil, jsonWrite(`{"choices":[{"message":{"content:"x"}}]}`))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Run(ctx, core.ModelRequest{Model: "m"}, nil); err == nil {
		t.Fatal("已取消的 ctx 必须报错")
	}
}

// ---- 并发 ----

func TestRunConcurrent(t *testing.T) {
	_, p := newTestServer(t, nil, sseWrite(
		`{"choices":[{"index":0,"delta":{"content":"并"}}]}`,
		`{"choices":[{"index":0,"delta":{"content":"发"}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c","function":{"name":"read","arguments":"{\"path\":\"p\"}"}}]}}]}`,
		`{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}`,
		`[DONE]`,
	))
	p.stream = true

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := p.Run(context.Background(), core.ModelRequest{Model: "m", Messages: []core.Message{{Role: "user", Content: "x"}}}, nil)
			if err != nil {
				errs <- err
				return
			}
			if res.Text != "并发" || len(res.ToolCalls) != 1 || string(res.ToolCalls[0].Args) != `{"path":"p"}` {
				errs <- fmt.Errorf("结果被并发污染：%+v", res)
				return
			}
			if res.Usage.OutputTokens != 2 {
				errs <- fmt.Errorf("usage 串了：%+v", res.Usage)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// ---- nil 语义 ----

func TestRunNilProviderAndNilEmit(t *testing.T) {
	var nilP *Provider
	if _, err := nilP.Run(context.Background(), core.ModelRequest{Model: "m"}, nil); err == nil {
		t.Error("nil provider 必须报错")
	}
	_, p := newTestServer(t, nil, jsonWrite(`{"choices":[{"message":{"content":"ok"}}]}`))
	// emit=nil 不应 panic。
	if _, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil); err != nil {
		t.Fatalf("emit=nil 时应照常工作：%v", err)
	}
}

// TestOpenAIWireContract 固化线上形状的关键点（协议漂移会被这里抓到）。
func TestOpenAIWireContract(t *testing.T) {
	// 请求体：arguments 序列化成字符串（不是嵌套对象）。
	fn := chatFunction{Name: "read", Arguments: flexArgs(`{"path":"a.txt"}`)}
	b, err := json.Marshal(chatToolCall{ID: "c1", Type: "function", Function: fn})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"arguments":"{\"path\":\"a.txt\"}"`) {
		t.Errorf("arguments 未按字符串序列化：%s", b)
	}
	// flexText：字符串与数组两种形态都吃。
	var ft flexText
	if err := json.Unmarshal([]byte(`"abc"`), &ft); err != nil || string(ft) != "abc" {
		t.Errorf("flexText(string) = %q err=%v", ft, err)
	}
	if err := json.Unmarshal([]byte(`[{"type":"text","text":"a"},{"type":"text","text":"b"}]`), &ft); err != nil || string(ft) != "ab" {
		t.Errorf("flexText(array) = %q err=%v", ft, err)
	}
	if err := json.Unmarshal([]byte(`null`), &ft); err != nil || string(ft) != "" {
		t.Errorf("flexText(null) = %q err=%v", ft, err)
	}
	// normalizeArgs：空 → {}，非空原样。
	if got := normalizeArgs(nil); string(got) != "{}" {
		t.Errorf("normalizeArgs(nil) = %q", got)
	}
	if got := normalizeArgs([]byte("  ")); string(got) != "{}" {
		t.Errorf("normalizeArgs(空白) = %q", got)
	}
	if got := normalizeArgs([]byte(`{"a":1}`)); string(got) != `{"a":1}` {
		t.Errorf("normalizeArgs 改了原文：%q", got)
	}
}

// TestLineReader 覆盖 SSE 行读取的三个边界：正常行、末行无换行、超长行。
func TestLineReader(t *testing.T) {
	lr := newLineReader(io.NewSectionReader(strings.NewReader("data: a\n\ndata: b\n"), 0, 100))
	if line, err := lr.readLine(); err != nil || line != "data: a\n" {
		t.Errorf("line=%q err=%v", line, err)
	}
	if line, err := lr.readLine(); err != nil || line != "\n" {
		t.Errorf("line=%q err=%v", line, err)
	}
	if line, err := lr.readLine(); err != nil || line != "data: b\n" {
		t.Errorf("line=%q err=%v", line, err)
	}
	if _, err := lr.readLine(); err == nil {
		t.Error("流结束应返回错误")
	}

	// 末行无换行：数据不丢，下一次才 EOF。
	lr2 := newLineReader(strings.NewReader("data: tail"))
	if line, err := lr2.readLine(); err != nil || line != "data: tail" {
		t.Errorf("末行丢失：line=%q err=%v", line, err)
	}
	if _, err := lr2.readLine(); err == nil {
		t.Error("第二次读取应 EOF")
	}

	// 超长行：报错而不是吃光内存。
	lr3 := newLineReader(strings.NewReader(strings.Repeat("z", maxSSELineBytes+10)))
	if _, err := lr3.readLine(); err == nil {
		t.Error("超长行应报错")
	}
}
