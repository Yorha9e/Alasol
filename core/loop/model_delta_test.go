package loop_test

// G13/G14 的 loop 层验收：流式 model.delta 旁路（durable=false）+ model.turn.reasoning
// + ReasoningEffort 透传进 ModelRequest。
//
// 口径（F8/陷阱 11）：model.delta **不入库、不占 seq、不进 Result.Events**；总线订阅
// 默认收得到，durable_only=true 收不到。X1 三段写路径与 user.prompt/model.turn 旁路的
// 形状一字不动（model.turn 只增可选 reasoning 字段）。

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/loop"
)

// deltaProvider 是流式假 provider：Run 时**边收边发**若干增量片段（text 与 reasoning
// 交替），最后返回组装好的全量结果——形状与真 provider（边收边发 + 返回全量）一致。
type deltaProvider struct {
	textDeltas   []string
	reasonDeltas []string
	// lastReq 记录最后一次收到的请求（断言 ReasoningEffort 透传用）。
	lastReq core.ModelRequest
}

func (p *deltaProvider) Run(_ context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.lastReq = req
	var text, reasoning string
	// 交错发送：正文片段与思维链片段交替（消费端要能按 Kind 分流，不依赖顺序）。
	for i := 0; i < len(p.textDeltas) || i < len(p.reasonDeltas); i++ {
		if i < len(p.reasonDeltas) {
			reasoning += p.reasonDeltas[i]
			emit(core.ProviderEvent{Kind: "reasoning.delta", Reasoning: p.reasonDeltas[i]})
		}
		if i < len(p.textDeltas) {
			text += p.textDeltas[i]
			emit(core.ProviderEvent{Kind: "text.delta", Text: p.textDeltas[i]})
		}
	}
	emit(core.ProviderEvent{Kind: "done", Text: text})
	return core.ModelResult{Text: text, Reasoning: reasoning, Usage: core.Usage{InputTokens: 7, OutputTokens: 3}}, nil
}

// drainSub 把订阅里当前可收的事件一次性收干（Publish 是同步投递，Run 返回后事件已在
// 通道里；这里仍用一个短轮询窗口兜底，避免把测试写成"恰好同步"的时序假设）。
func drainSub(t *testing.T, sub *bus.Subscription) []contracts.Event {
	t.Helper()
	var out []contracts.Event
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-time.After(50 * time.Millisecond):
			return out
		}
	}
	return out
}

// TestModelDeltaStreamEventsNotDurable：流式增量 → 多条 model.delta（durable=false、
// 无 seq、turn 正确）；拼接 == model.turn.text；reasoning 片段同理；**不入 store**
// （seq 不变）；Result.Events 不含它们。
func TestModelDeltaStreamEventsNotDurable(t *testing.T) {
	h := newHarness(t)
	prov := &deltaProvider{
		textDeltas:   []string{"你", "好", "，", "世界"},
		reasonDeltas: []string{"用户", "在打招呼"},
	}
	lp := h.loop(func(c *loop.Config) { c.Provider = prov })

	// 订阅（默认过滤 = 流事件也收）+ durable_only 订阅（只收事实事件）。
	all := h.bus.Subscribe(bus.Filter{})
	defer all.Close()
	durableOnly := h.bus.Subscribe(bus.Filter{DurableOnly: true})
	defer durableOnly.Close()

	res, err := lp.Run(context.Background(), "打招呼")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// ---- 默认订阅：收到流事件 ----
	var (
		deltas    []contracts.Event
		text      string
		reasoning string
	)
	for _, ev := range drainSub(t, all) {
		if ev.Kind == loop.KindModelDelta {
			deltas = append(deltas, ev)
			var p contracts.ModelDeltaPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Fatalf("model.delta payload: %v", err)
			}
			if p.Turn != 1 {
				t.Errorf("delta turn = %d，want 1", p.Turn)
			}
			if p.Text != nil {
				text += *p.Text
			}
			if p.Reasoning != nil {
				reasoning += *p.Reasoning
			}
		}
	}
	if len(deltas) != 6 {
		t.Fatalf("model.delta = %d 条，want 6（4 正文 + 2 思维链）", len(deltas))
	}
	if text != "你好，世界" {
		t.Errorf("delta 正文拼接 = %q，want %q（与 model.turn.text 一致）", text, "你好，世界")
	}
	if reasoning != "用户在打招呼" {
		t.Errorf("delta 思维链拼接 = %q，want %q", reasoning, "用户在打招呼")
	}
	for _, ev := range deltas {
		if ev.Durable {
			t.Errorf("model.delta 必须 durable=false（F8），得到 %+v", ev)
		}
		if ev.Seq != nil {
			t.Errorf("model.delta 不得携带 seq（durable=false ⇒ seq 缺省），得到 %d", *ev.Seq)
		}
		if ev.SessionID != h.session || ev.AgentID != h.agent {
			t.Errorf("model.delta 归属错：%+v", ev)
		}
	}

	// ---- durable_only 订阅：收不到流事件 ----
	for _, ev := range drainSub(t, durableOnly) {
		if ev.Kind == loop.KindModelDelta {
			t.Fatalf("durable_only 订阅不该收到 model.delta（流事件被过滤）")
		}
	}

	// ---- 不入 store：事件库里只有 durable 事实，且没有 model.delta ----
	var (
		turns    []contracts.ModelTurnPayload
		ctxCount int
	)
	for _, ev := range h.events() {
		if ev.Kind == loop.KindModelDelta {
			t.Fatalf("model.delta 不得入库（流事件，F8/陷阱 11）")
		}
		switch ev.Kind {
		case loop.KindModelTurn:
			var p contracts.ModelTurnPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Fatalf("model.turn payload: %v", err)
			}
			turns = append(turns, p)
		case loop.KindContextAssembled:
			ctxCount++
		}
	}
	if len(turns) != 1 {
		t.Fatalf("model.turn = %d 条，want 1", len(turns))
	}
	// G14：思维链整段落 model.turn.reasoning（与 delta 拼接一致）。
	if turns[0].Reasoning == nil || *turns[0].Reasoning != "用户在打招呼" {
		t.Fatalf("model.turn.reasoning = %v，want \"用户在打招呼\"", turns[0].Reasoning)
	}
	if turns[0].Text != "你好，世界" {
		t.Errorf("model.turn.text = %q，want 你好，世界", turns[0].Text)
	}
	if ctxCount != 1 {
		t.Errorf("context.assembled = %d，want 1", ctxCount)
	}
	// 旁路/流事件都不占 Result.Events：一轮无工具 = 1 条 context.assembled。
	if res.Events != 1 {
		t.Errorf("Result.Events = %d，want 1（流事件与旁路观测均不计）", res.Events)
	}
}

// TestModelDeltaTurnNumbersAdvance：多轮对话里 delta 的 turn 随轮次递增（订阅端据此归组）。
func TestModelDeltaTurnNumbersAdvance(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	// 两轮：第一轮带一次工具调用，第二轮收敛。两轮都发 delta。
	h.script(
		scriptStep{result: core.ModelResult{Text: "调工具", ToolCalls: []core.ToolCallReq{call("c1", "echo", `{"text":"hi"}`)}}},
		scriptStep{result: core.ModelResult{Text: "收尾"}},
	)
	all := h.bus.Subscribe(bus.Filter{})
	defer all.Close()

	if _, err := h.loop(nil).Run(context.Background(), "两轮"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	turns := map[int64]int{}
	for _, ev := range drainSub(t, all) {
		if ev.Kind != loop.KindModelDelta {
			continue
		}
		var p contracts.ModelDeltaPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("payload: %v", err)
		}
		turns[p.Turn]++
	}
	// 两轮各一条 mockProvider 的 "…" delta（harness 的 mockProvider 每轮发一条）。
	if turns[1] != 1 || turns[2] != 1 {
		t.Fatalf("各轮 delta 条数 = %v，want {1:1, 2:1}", turns)
	}
}

// TestReasoningEffortPassthrough：Config.ReasoningEffort → ModelRequest.ReasoningEffort；
// 空 = nil（provider 不下发该字段）。
func TestReasoningEffortPassthrough(t *testing.T) {
	// ① 设了 → 指针非空且值原样。
	h := newHarness(t)
	prov := &deltaProvider{textDeltas: []string{"hi"}}
	lp := h.loop(func(c *loop.Config) {
		c.Provider = prov
		c.ReasoningEffort = "high"
	})
	if _, err := lp.Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if prov.lastReq.ReasoningEffort == nil {
		t.Fatal("设了 ReasoningEffort 但 ModelRequest.ReasoningEffort 为 nil")
	}
	if *prov.lastReq.ReasoningEffort != "high" {
		t.Errorf("ReasoningEffort = %q，want high", *prov.lastReq.ReasoningEffort)
	}

	// ② 没设 → nil（"空 = 不传"）。
	h2 := newHarness(t)
	prov2 := &deltaProvider{textDeltas: []string{"hi"}}
	lp2 := h2.loop(func(c *loop.Config) { c.Provider = prov2 })
	if _, err := lp2.Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if prov2.lastReq.ReasoningEffort != nil {
		t.Errorf("未设 ReasoningEffort 时 ModelRequest 应为 nil，得到 %q", *prov2.lastReq.ReasoningEffort)
	}
}

// ---------------------------------------------------------------------------
// G13 run_id 归组（2026-11-01 v4-7）
// ---------------------------------------------------------------------------

// concurrentDeltaProvider 是并发安全的流式假 provider：每次 Run 发一条可辨识的
// delta（片段文本带 prompt 标签），供"同 session 并发 Run 归组"钉子区分来源。
type concurrentDeltaProvider struct{}

func (concurrentDeltaProvider) Run(_ context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	// 取最后一条用户消息做标签（两次 Run 用不同 prompt）。
	tag := "?"
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			tag = req.Messages[i].Content
			break
		}
	}
	emit(core.ProviderEvent{Kind: "text.delta", Text: tag + "-1 "})
	return core.ModelResult{Text: tag + "-1 "}, nil
}

// TestModelDeltaRunIDPresent：单次 Run 的全部 delta 携带同一个非空 run_id（Run 起始
// 生成的 ULID）。变异自证：Run 不带 run_id（publishModelDelta 不设 RunID）→ 本钉红。
func TestModelDeltaRunIDPresent(t *testing.T) {
	h := newHarness(t)
	prov := &deltaProvider{textDeltas: []string{"你", "好"}}
	lp := h.loop(func(c *loop.Config) { c.Provider = prov })
	sub := h.bus.Subscribe(bus.Filter{})
	defer sub.Close()

	if _, err := lp.Run(context.Background(), "打招呼"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	seen := map[string]int{}
	for _, ev := range drainSub(t, sub) {
		if ev.Kind != loop.KindModelDelta {
			continue
		}
		var p contracts.ModelDeltaPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("payload: %v", err)
		}
		if p.RunID == nil || *p.RunID == "" {
			t.Fatalf("model.delta 缺 run_id（G13 归组判别位）：%s", ev.Payload)
		}
		if !core.ValidUlid(contracts.Ulid(*p.RunID)) {
			t.Fatalf("run_id 不是合法 ULID：%q", *p.RunID)
		}
		seen[*p.RunID]++
	}
	if len(seen) != 1 {
		t.Fatalf("单次 Run 的 delta 应共享同一个 run_id，得到 %d 个：%v", len(seen), seen)
	}
}

// TestModelDeltaConcurrentRunsGroupByRunID：同 session **并发**跑两轮——两轮的 delta
// 按 run_id 可分成两组、互不混淆（G13 根治钉子）。变异自证：delta 不带 run_id →
// 两轮 turn 都从 1 起、片段文本交错，按 (run_id, turn) 归组退化成一锅粥 → 本钉红。
func TestModelDeltaConcurrentRunsGroupByRunID(t *testing.T) {
	h := newHarness(t)
	lp := h.loop(func(c *loop.Config) { c.Provider = concurrentDeltaProvider{} })
	sub := h.bus.Subscribe(bus.Filter{})
	defer sub.Close()

	// 同一 Loop 并发 Run 两轮（loop 无可变状态，见包 doc）。
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, prompt := range []string{"alpha", "beta"} {
		p := prompt
		go func() {
			<-start
			_, err := lp.Run(context.Background(), p)
			errs <- err
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("Run: %v", err)
		}
	}

	// 按 run_id 归组：每组的文本拼接应完整等于某一轮的 prompt 标签（不混另一轮）。
	groups := map[string]string{}
	total := 0
	for _, ev := range drainSub(t, sub) {
		if ev.Kind != loop.KindModelDelta {
			continue
		}
		var p contracts.ModelDeltaPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("payload: %v", err)
		}
		if p.RunID == nil || *p.RunID == "" {
			t.Fatalf("并发 Run 的 delta 缺 run_id：%s（无法归组）", ev.Payload)
		}
		if p.Text != nil {
			groups[*p.RunID] += *p.Text
		}
		total++
	}
	if total != 2 {
		t.Fatalf("delta 总数 = %d，want 2（每轮一条）", total)
	}
	if len(groups) != 2 {
		t.Fatalf("按 run_id 分组 = %d 组，want 2（并发两轮可分）：%v", len(groups), groups)
	}
	for rid, text := range groups {
		if text != "alpha-1 " && text != "beta-1 " {
			t.Fatalf("run_id=%s 组的拼接文本 = %q，want \"alpha-1 \" 或 \"beta-1 \"（不混组）", rid, text)
		}
	}
}
