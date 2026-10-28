package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// DefaultBaseURL 是 OpenAI 官方 chat completions 端点。
// 接本地网关 / Azure 兼容端点时由装配方覆盖（Options.BaseURL）。
const DefaultBaseURL = "https://api.openai.com/v1"

// chatCompletionsPath 是 chat completions 的相对路径（拼在 BaseURL 之后）。
const chatCompletionsPath = "/chat/completions"

// maxErrorBody 是 HTTP 错误响应体进入错误文案前的截断上限（1KB）：
// 网关的 5xx 页面可能几百 KB，全塞进事件/日志会把它们撑爆。
const maxErrorBody = 1024

// 流式事件的 kind（core.ProviderEvent.Kind 的 v0 子集，I3 的开放字符串）。
// loop / UI 侧应对照这些常量而不是裸字符串字面量。
const (
	// KindTextDelta 增量文本（Text=片段）。
	KindTextDelta = "text.delta"
	// KindToolCallStart 一个工具调用开始（ToolCall.ID / ToolCall.Name 已知，Args 可能还空）。
	KindToolCallStart = "toolcall.start"
	// KindToolCallDelta 工具调用参数片段（ToolCall.ID 已锁定，Args=**这一个片段**的原始字节）。
	KindToolCallDelta = "toolcall.delta"
	// KindDone 正常结束（Text=最终全文，ToolCall=nil）。
	KindDone = "done"
	// KindError 失败（Text=错误文案）；返回前必发，发完照常返回 error。
	KindError = "error"
)

// Options 是 Provider 的装配参数（零值可用：BaseURL 落默认值、无 APIKey、无流式）。
type Options struct {
	// BaseURL 是 OpenAI 兼容端点根路径（默认 DefaultBaseURL）。
	// 尾部的 "/" 会被去掉，避免拼出 "//chat/completions"。
	BaseURL string

	// APIKey 是 Bearer 凭据。**由装配方从 env 取出后传入**——本包不读 env（见包 doc）。
	// 为空时不下发 Authorization 头（本地网关常不需要）。
	APIKey string

	// HTTPClient 可注入（测试用 httptest.Server，或调用方想统一设置超时/代理/传输层）。
	// 为 nil 时用不带超时的 *http.Client：超时归调用方 ctx 管（见包 doc「超时」）。
	HTTPClient *http.Client

	// Stream 是否走 SSE 流式（默认 false=非流式）。两种模式返回的 ModelResult 形状一致。
	Stream bool
}

// Provider 是 core.Provider 的 OpenAI 兼容实现。
//
// 并发安全：无共享可变状态，可多 goroutine 同时 Run。
type Provider struct {
	baseURL string
	apiKey  string
	client  *http.Client
	stream  bool
}

// 编译期断言：Provider 必须是 core.Provider（改签名立刻编译失败，而不是到 loop 层才炸）。
var _ core.Provider = (*Provider)(nil)

// New 构造 Provider。零值 Options 合法（默认官方 BaseURL、无凭据、非流式）。
//
// nil 语义：不 panic——缺参数后用更安全的默认值顶上（BaseURL 兜底、无凭据时不下发
// Authorization 头）。这与 core/doc.go 的「兜底」一档同口径：缺参数后系统仍能安全地
// 做事（发请求，让端点去拒绝），不存在"不放行任何东西"的问题。
func New(o Options) *Provider {
	base := normalizeBaseURL(o.BaseURL)
	if base == "" {
		base = DefaultBaseURL
	}
	client := o.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	return &Provider{
		baseURL: base,
		apiKey:  strings.TrimSpace(o.APIKey),
		client:  client,
		stream:  o.Stream,
	}
}

// BaseURL 返回实际使用的端点根路径（测试/诊断用）。
func (p *Provider) BaseURL() string {
	if p == nil {
		return ""
	}
	return p.baseURL
}

// Run implements core.Provider：一次 Run = 一次 chat completions 调用。
//
// 返回的 ModelResult 与 emit 的事件描述**同一次**调用：流式下事件是增量、Result 是
// 组装后的全量；非流式下事件是"一次性补发"、Result 直接来自响应体。
func (p *Provider) Run(ctx context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	if p == nil {
		return core.ModelResult{}, errors.New("openai: provider 为 nil（New 未被调用）")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if emit == nil {
		// 允许不传 emit（纯结果调用）：内部统一走 nil 安全的 emitEvent。
		emit = func(core.ProviderEvent) {}
	}
	if strings.TrimSpace(req.Model) == "" {
		return fail(emit, errors.New("openai: ModelRequest.Model 不能为空（chat completions 必须指定模型）"))
	}

	payload, err := p.buildRequest(req)
	if err != nil {
		return fail(emit, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+chatCompletionsPath, bytes.NewReader(payload))
	if err != nil {
		return fail(emit, fmt.Errorf("openai: 构造 HTTP 请求失败：%w", err))
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if p.stream {
		httpReq.Header.Set("Accept", "text/event-stream")
	} else {
		httpReq.Header.Set("Accept", "application/json")
	}
	if p.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)
	}

	resp, err := p.client.Do(httpReq)
	if err != nil {
		// ctx 取消要能被 errors.Is 认出来：包一层但不吞掉原错误。
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fail(emit, fmt.Errorf("openai: 请求中止（%w）", ctxErr))
		}
		return fail(emit, fmt.Errorf("openai: 请求失败：%w", err))
	}
	// 无论走哪条路都要把 body 关掉；非 2xx 已读的部分也要排空，便于连接复用。
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBody))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody+1))
		return fail(emit, &StatusError{
			StatusCode: resp.StatusCode,
			Status:     strings.TrimSpace(resp.Status),
			Body:       truncateString(string(raw), maxErrorBody),
		})
	}

	if p.stream {
		return p.runStream(ctx, resp.Body, emit)
	}
	return p.runOnce(resp.Body, emit)
}

// fail 统一「先 emit error 事件、再返回错误」的出口（保证 UI 看得到失败原因）。
func fail(emit func(core.ProviderEvent), err error) (core.ModelResult, error) {
	emitEvent(emit, core.ProviderEvent{Kind: KindError, Text: err.Error()})
	return core.ModelResult{}, err
}

// emitEvent 是 nil 安全的 emit（调用方可以不传回调）。
func emitEvent(emit func(core.ProviderEvent), ev core.ProviderEvent) {
	if emit == nil {
		return
	}
	emit(ev)
}

// ---- 请求体 ----

// chatRequest 是 chat completions 的请求体（只写用得到的字段，保持零依赖）。
type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
	Tools    []chatTool    `json:"tools,omitempty"`
	Stream   bool          `json:"stream"`
	// StreamOptions 仅流式下发：让最后一个 chunk 带 usage（否则流式拿不到 token 计量）。
	StreamOptions *chatStreamOptions `json:"stream_options,omitempty"`
}

type chatStreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type chatMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content"`
	ToolCalls  []chatToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type chatToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function chatFunction `json:"function"`
}

type chatFunction struct {
	Name      string   `json:"name"`
	Arguments flexArgs `json:"arguments"`
}

type chatTool struct {
	Type     string           `json:"type"`
	Function chatToolFunction `json:"function"`
}

type chatToolFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
}

// defaultParameters 是工具未提供 Schema 时的兜底参数模式（OpenAI 要求 function 带 parameters）。
var defaultParameters = json.RawMessage(`{"type":"object","properties":{}}`)

// buildRequest 把 provider 无关的 ModelRequest 映射成 chat completions 请求体。
//
// 消息映射（角色一一对应，不重排、不合并——重排历史是 loop/上下文层的职责，v0 不做）：
//
//	system            → role=system（req.System 非空时置顶）
//	user              → role=user
//	assistant         → role=assistant（带 ToolCalls 时附 tool_calls 数组）
//	tool              → role=tool + tool_call_id（**provider 原样 id**，S4）
//
// 工具映射：contracts.Tool → {"type":"function","function":{name,description,parameters}}，
// parameters 用 Tool.Schema **原样字节**（元嵌套：值本身就是 JSON Schema，不重新编码）。
func (p *Provider) buildRequest(req core.ModelRequest) ([]byte, error) {
	msgs := make([]chatMessage, 0, len(req.Messages)+1)
	if strings.TrimSpace(req.System) != "" {
		msgs = append(msgs, chatMessage{Role: "system", Content: req.System})
	}
	for i, m := range req.Messages {
		cm, err := mapMessage(i, m)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, cm)
	}

	tools, err := mapTools(req.Tools)
	if err != nil {
		return nil, err
	}

	cr := chatRequest{
		Model:    req.Model,
		Messages: msgs,
		Tools:    tools,
		Stream:   p.stream,
	}
	if p.stream {
		cr.StreamOptions = &chatStreamOptions{IncludeUsage: true}
	}
	return json.Marshal(cr)
}

// mapMessage 映射单条消息。角色不认识 / tool 消息缺 id / assistant 工具调用缺 id 一律报错：
// 这些是调用方 bug，静默丢字段会让模型拿到错位的历史，比当场失败难查得多。
func mapMessage(i int, m core.Message) (chatMessage, error) {
	switch m.Role {
	case "system":
		return chatMessage{Role: "system", Content: m.Content}, nil
	case "user":
		return chatMessage{Role: "user", Content: m.Content}, nil
	case "assistant":
		cm := chatMessage{Role: "assistant", Content: m.Content}
		if len(m.ToolCalls) > 0 {
			cm.ToolCalls = make([]chatToolCall, 0, len(m.ToolCalls))
			for j, tc := range m.ToolCalls {
				if tc.ID == "" {
					return chatMessage{}, fmt.Errorf("openai: Messages[%d].ToolCalls[%d].ID 为空（provider 的 tool_call_id 必须原样回传，S4）", i, j)
				}
				if strings.TrimSpace(tc.Name) == "" {
					return chatMessage{}, fmt.Errorf("openai: Messages[%d].ToolCalls[%d].Name 为空", i, j)
				}
				cm.ToolCalls = append(cm.ToolCalls, chatToolCall{
					ID:       string(tc.ID), // 黑盒原样（S4/陷阱 17）
					Type:     "function",
					Function: chatFunction{Name: tc.Name, Arguments: flexArgs(tc.Args)},
				})
			}
		}
		return cm, nil
	case "tool":
		if m.ToolCallID == "" {
			return chatMessage{}, fmt.Errorf("openai: Messages[%d] 角色为 tool 但 ToolCallID 为空（必须回填对应的 provider tool_call_id，S4）", i)
		}
		return chatMessage{
			Role:       "tool",
			Content:    m.Content,
			ToolCallID: string(m.ToolCallID), // 黑盒原样（S4/陷阱 17）
		}, nil
	default:
		return chatMessage{}, fmt.Errorf("openai: Messages[%d].Role=%q 不是受支持的角色（v0：system/user/assistant/tool）", i, m.Role)
	}
}

// mapTools 映射工具描述。Schema 为空时落 defaultParameters（否则 OpenAI 侧会拒请求）。
func mapTools(tools []contracts.Tool) ([]chatTool, error) {
	if len(tools) == 0 {
		return nil, nil
	}
	out := make([]chatTool, 0, len(tools))
	for i, t := range tools {
		if strings.TrimSpace(t.Name) == "" {
			return nil, fmt.Errorf("openai: Tools[%d].Name 为空", i)
		}
		params := json.RawMessage(t.Schema)
		if len(bytes.TrimSpace(params)) == 0 {
			params = defaultParameters
		}
		ct := chatTool{
			Type: "function",
			Function: chatToolFunction{
				Name:       t.Name,
				Parameters: params,
			},
		}
		if t.Description != nil {
			ct.Function.Description = *t.Description
		}
		out = append(out, ct)
	}
	return out, nil
}

// ---- 响应体（非流式） ----

// runOnce 解析非流式响应体并组装 ModelResult。
func (p *Provider) runOnce(body io.Reader, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	var cr chatResponse
	if err := json.NewDecoder(body).Decode(&cr); err != nil {
		return fail(emit, fmt.Errorf("openai: 解析响应体失败：%w", err))
	}
	if cr.Error != nil && strings.TrimSpace(cr.Error.Message) != "" {
		return fail(emit, fmt.Errorf("openai: 端点返回错误对象（type=%s code=%v）：%s",
			cr.Error.Type, cr.Error.Code, truncateString(cr.Error.Message, maxErrorBody)))
	}
	if len(cr.Choices) == 0 {
		return fail(emit, errors.New("openai: 响应缺少 choices（既无消息也无 usage，视为畸形响应）"))
	}

	ch := cr.Choices[0]
	res := core.ModelResult{Text: string(ch.Message.Content)}
	for i, tc := range ch.Message.ToolCalls {
		if strings.TrimSpace(tc.Function.Name) == "" {
			return fail(emit, fmt.Errorf("openai: choices[0].tool_calls[%d]（id=%q）缺少 function.name", i, tc.ID))
		}
		res.ToolCalls = append(res.ToolCalls, core.ToolCallReq{
			ID:   contracts.ProviderId(tc.ID), // 黑盒原样（S4/陷阱 17）
			Name: tc.Function.Name,
			Args: normalizeArgs(tc.Function.Arguments),
		})
	}
	if cr.Usage != nil {
		res.Usage = core.Usage{InputTokens: cr.Usage.PromptTokens, OutputTokens: cr.Usage.CompletionTokens}
	}

	// 非流式也按流式的形状补发事件：一条全文 delta + 每个工具一条 start + done。
	// 消费端因此不必区分两种模式。
	if res.Text != "" {
		emitEvent(emit, core.ProviderEvent{Kind: KindTextDelta, Text: res.Text})
	}
	for i := range res.ToolCalls {
		tc := res.ToolCalls[i]
		emitEvent(emit, core.ProviderEvent{Kind: KindToolCallStart, ToolCall: &tc})
	}
	emitEvent(emit, core.ProviderEvent{Kind: KindDone, Text: res.Text})
	return res, nil
}

// ---- 响应体（流式） ----

// runStream 按 SSE 解析流式响应体。
//
// SSE 形状（一行一条）：
//
//	data: {"id":"...","choices":[{"index":0,"delta":{"content":"你"}}]}
//	data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_x","function":{"name":"read","arguments":"{\"pa"}}]}}]}
//	data: [DONE]
//
// 以 ":" 开头的行是注释/心跳，直接忽略；空行是分帧，忽略；"[DONE]" 是结束哨兵。
// 流自然 EOF 但没有 [DONE] 时**容忍**（用已收到的内容组装结果）——部分网关不发送哨兵。
// 空流（只有 [DONE]）→ 返回空 ModelResult 且不报错：部分网关对"无内容可返回"这么回，
// 交给上层（loop）判断要不要重试，provider 不猜。
func (p *Provider) runStream(ctx context.Context, body io.Reader, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	acc := newStreamAccumulator(emit)
	br := newLineReader(body)

	for {
		if err := ctx.Err(); err != nil {
			return fail(emit, fmt.Errorf("openai: 流式中止（%w）", err))
		}
		line, err := br.readLine()
		if line != "" {
			if perr := acc.handleLine(line); perr != nil {
				return fail(emit, perr)
			}
			if acc.done {
				break
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // 流结束（[DONE] 或自然 EOF 都走这里）
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return fail(emit, fmt.Errorf("openai: 读取流失败（%w）", ctxErr))
			}
			return fail(emit, fmt.Errorf("openai: 读取流失败：%w", err))
		}
	}

	res := acc.result()
	if err := acc.validate(); err != nil {
		return fail(emit, err)
	}
	emitEvent(emit, core.ProviderEvent{Kind: KindDone, Text: res.Text})
	return res, nil
}

// toolAccum 是一个 tool_call 在流过程中的累积态（按 chunk 里的 index 归并）。
type toolAccum struct {
	id      string
	name    string
	args    strings.Builder
	started bool
}

// streamAccumulator 累积一次流式调用的全部增量。
type streamAccumulator struct {
	emit    func(core.ProviderEvent)
	text    strings.Builder
	order   []int // tool_call 的 index 出现顺序（保序：模型并行调用的顺序有意义）
	byIndex map[int]*toolAccum
	usage   *core.Usage
	done    bool
	// indexlessFallback 记录"曾用过缺 index 的位置兜底"——一旦流里出现第二个
	// 调用，兜底那次归属就是猜的 → 必须 fail-loud（见 applyToolCalls）。
	indexlessFallback bool
}

func newStreamAccumulator(emit func(core.ProviderEvent)) *streamAccumulator {
	return &streamAccumulator{emit: emit, byIndex: map[int]*toolAccum{}}
}

// handleLine 处理一条 SSE 行。
func (a *streamAccumulator) handleLine(line string) error {
	line = strings.TrimRight(line, "\r\n")
	if line == "" || strings.HasPrefix(line, ":") {
		return nil // 空行分帧 / 注释心跳
	}
	if !strings.HasPrefix(line, "data:") {
		return nil // event: / id: / retry: 等行 v0 不用
	}
	payload := strings.TrimSpace(line[len("data:"):])
	if payload == "" {
		return nil
	}
	if payload == "[DONE]" {
		a.done = true
		return nil
	}

	var ch chatChunk
	if err := json.Unmarshal([]byte(payload), &ch); err != nil {
		return fmt.Errorf("openai: 解析 SSE 数据块失败：%w（块已截断：%s）", err, truncateString(payload, maxErrorBody))
	}
	if ch.Error != nil && strings.TrimSpace(ch.Error.Message) != "" {
		return fmt.Errorf("openai: 流中返回错误对象（type=%s code=%v）：%s",
			ch.Error.Type, ch.Error.Code, truncateString(ch.Error.Message, maxErrorBody))
	}
	if ch.Usage != nil {
		a.usage = &core.Usage{InputTokens: ch.Usage.PromptTokens, OutputTokens: ch.Usage.CompletionTokens}
	}
	for _, choice := range ch.Choices {
		if delta := choice.Delta.Content; delta != "" {
			a.text.WriteString(string(delta))
			emitEvent(a.emit, core.ProviderEvent{Kind: KindTextDelta, Text: string(delta)})
		}
		if err := a.applyToolCalls(choice.Delta.ToolCalls); err != nil {
			return err // fail-loud：缺 index 且无法安全归并（见 applyToolCalls）
		}
	}
	return nil
}

// errMissingToolCallIndex 是"chunk 缺 tool_calls[].index 且无法安全归并"的错误。
//
// 多调用流里缺 index = 这个增量可能属于**别的**调用（按位置兜底会把参数并到
// 别的调用上 → 污染参数）。宁可当场报错（fail-loud），不猜。
var errMissingToolCallIndex = errors.New("openai: chunk 缺 tool_calls[].index，无法安全归并")

// applyToolCalls 归并一个 chunk 里的 tool_call 增量。
//
// 顺序很重要：先把整个数组归并完，再对"本轮首次出现"的 tool_call 发 start——
// 这样 id 和 name 同在首个 chunk 到达时（常见形态），start 事件就一次带全，
// 消费端不必拼多个事件才知道"这个调用叫什么"。
//
// **缺 index 的处理**（fail-loud）：OpenAI 官方总带 index。缺 index 时按位置兜底
// 只在"整条流只有这一个调用"时安全；一旦流里已有/将有多个调用，兜底就会把参数
// 并到别的调用上（污染参数）→ 当场报错（emit error + 返回错误），不猜。
func (a *streamAccumulator) applyToolCalls(deltas []chunkToolCall) error {
	if len(deltas) == 0 {
		return nil
	}
	// 一个 chunk 里多个 delta 时，缺 index 的那个无法归属（不能按位置猜）。
	if len(deltas) > 1 {
		for _, d := range deltas {
			if d.Index == nil {
				return errMissingToolCallIndex
			}
		}
	}
	type frag struct {
		acc  *toolAccum
		args string
	}
	frags := make([]frag, 0, len(deltas))
	touched := make([]*toolAccum, 0, len(deltas))

	for i, d := range deltas {
		idx := i
		if d.Index != nil {
			idx = *d.Index // OpenAI 官方总带 index
			if a.indexlessFallback {
				// 之前用过"缺 index 的位置兜底"，现在又出现带 index 的调用 →
				// 流里不止一个调用，而兜底那次归属是猜的 → 不可信（fail-loud）。
				return errMissingToolCallIndex
			}
		} else {
			switch {
			case len(a.byIndex) == 0:
				// 第一个调用：按位置兜底（若后面出现第二个调用 → 立即报错）。
				idx = 0
				a.indexlessFallback = true
			case len(a.byIndex) == 1 && a.indexlessFallback:
				// 仍是那唯一一个调用（续传）→ 同一个槽位。
				idx = 0
			default:
				// 已经有别的调用 → 这个增量属于谁无法判断 → 不安全。
				return errMissingToolCallIndex
			}
		}
		acc, ok := a.byIndex[idx]
		if !ok {
			acc = &toolAccum{}
			a.byIndex[idx] = acc
			a.order = append(a.order, idx)
		}
		touched = append(touched, acc)

		// id：首个非空即锁定，后续 chunk 即使再带同名 id 也不改（S4 黑盒，不改写）。
		if acc.id == "" && d.ID != "" {
			acc.id = d.ID
		}
		// name：绝大多数端点只在首 chunk 给一次；少数网关会把 name 拆成多个片段，
		// 也有网关每个 chunk 重复完整 name。这里用"追加 + 重复检测"两种形态都兼容：
		// 片段等于已累积的前缀时判定为重复发送，跳过。
		if n := d.Function.Name; n != "" {
			if acc.name == "" || !strings.HasPrefix(acc.name, n) {
				acc.name += n
			}
		}
		if args := string(d.Function.Arguments); args != "" {
			acc.args.WriteString(args)
			frags = append(frags, frag{acc: acc, args: args})
		}
	}

	for _, acc := range touched {
		if acc.started {
			continue
		}
		if acc.id == "" && acc.name == "" {
			continue // 还没有任何可识别信息，等下一个 chunk
		}
		acc.started = true
		emitEvent(a.emit, core.ProviderEvent{Kind: KindToolCallStart, ToolCall: &core.ToolCallReq{
			ID:   contracts.ProviderId(acc.id), // 黑盒原样（S4/陷阱 17）
			Name: acc.name,
		}})
	}
	for _, f := range frags {
		emitEvent(a.emit, core.ProviderEvent{Kind: KindToolCallDelta, ToolCall: &core.ToolCallReq{
			ID:   contracts.ProviderId(f.acc.id), // 黑盒原样
			Name: f.acc.name,
			Args: json.RawMessage(f.args), // 只是一个片段，不是完整 JSON
		}})
	}
	return nil
}

// result 按 index 出现顺序组装 ModelResult（流已结束、尚未校验）。
func (a *streamAccumulator) result() core.ModelResult {
	res := core.ModelResult{Text: a.text.String()}
	if len(a.order) > 0 {
		res.ToolCalls = make([]core.ToolCallReq, 0, len(a.order))
		for _, idx := range a.order {
			acc := a.byIndex[idx]
			res.ToolCalls = append(res.ToolCalls, core.ToolCallReq{
				ID:   contracts.ProviderId(acc.id), // 黑盒原样（S4/陷阱 17）
				Name: acc.name,
				Args: normalizeArgs([]byte(acc.args.String())),
			})
		}
	}
	if a.usage != nil {
		res.Usage = *a.usage
	}
	return res
}

// validate 校验组装结果：缺 name 的 tool_call 无法分发，必须当场失败（fail-loud），
// 否则 loop 会拿着一个空名字去查工具注册表，报一个离现场很远的错。
func (a *streamAccumulator) validate() error {
	for i, idx := range a.order {
		if strings.TrimSpace(a.byIndex[idx].name) == "" {
			return fmt.Errorf("openai: 流式 tool_calls[%d]（id=%q）始终没有 function.name", i, a.byIndex[idx].id)
		}
	}
	return nil
}

// normalizeArgs 把 arguments 规范化为 json.RawMessage：
// 空/纯空白 → "{}"（下游要 unmarshal 进参数结构体，空 RawMessage 会直接报错）；
// 非空 → **原样字节**（不重新编码，保留 provider 原文，S4 精神）。
func normalizeArgs(raw []byte) json.RawMessage {
	b := bytes.TrimSpace(raw)
	if len(b) == 0 {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(b)
}

// ---- 错误类型 ----

// StatusError 是 HTTP 非 2xx 的错误（调用方可 errors.As 取出状态码做分类，如 429 重试）。
type StatusError struct {
	StatusCode int
	Status     string // 形如 "400 Bad Request"
	Body       string // 已截断到 maxErrorBody
}

// Error implements error。
func (e *StatusError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "openai: HTTP %d", e.StatusCode)
	if e.Status != "" {
		b.WriteString(" " + e.Status)
	}
	if e.Body != "" {
		b.WriteString(": " + e.Body)
	}
	return b.String()
}

// truncateString 按字节截断并标注（截断后不超过 limit 字节 + 标注）。
func truncateString(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + fmt.Sprintf("…[truncated %d bytes]", len(s)-limit)
}
