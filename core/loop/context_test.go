package loop_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/loop"
)

// decodeSnapshot 把一条 context.assembled 事件的载荷解成契约类型。
func decodeSnapshot(t *testing.T, ev contracts.Event) contracts.ContextSnapshot {
	t.Helper()
	var s contracts.ContextSnapshot
	if err := json.Unmarshal(ev.Payload, &s); err != nil {
		t.Fatalf("解 context.assembled payload: %v", err)
	}
	return s
}

// TestContextAssembledEmittedEveryTurn：每轮模型调用前落一条 context.assembled
// （06 §4.2：上下文可见性的数据源），且形状符合契约。
func TestContextAssembledEmittedEveryTurn(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(
		stepTools(call("c1", "echo", `{"text":"hi"}`)),
		stepTools(call("c2", "echo", `{"text":"again"}`)),
		stepText("完成"),
	)

	res, err := h.loop(nil).Run(context.Background(), "帮我 echo")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Turns != 3 {
		t.Fatalf("Turns = %d，want 3", res.Turns)
	}

	ctxs := contextEvents(h.events())
	if len(ctxs) != res.Turns {
		t.Fatalf("每轮应恰有一条 context.assembled：得到 %d 条，want %d", len(ctxs), res.Turns)
	}

	// turn 从 1 递增、session 归属正确、都是 durable 事实事件。
	for i, ev := range ctxs {
		if !ev.Durable {
			t.Fatalf("第 %d 条 context.assembled 不是 durable 事件", i)
		}
		if ev.SessionID != h.session || ev.AgentID != h.agent {
			t.Fatalf("第 %d 条 context.assembled 归属不符: %+v", i, ev)
		}
		snap := decodeSnapshot(t, ev)
		if snap.Turn != int64(i+1) {
			t.Fatalf("第 %d 条快照 turn = %d，want %d", i, snap.Turn, i+1)
		}
		if snap.SessionID != h.session {
			t.Fatalf("第 %d 条快照 session_id = %q，want %q", i, snap.SessionID, h.session)
		}
	}

	// 装配前落盘：第一轮的 context.assembled 必须先于同轮的 intent（seq 更小）。
	all := h.events()
	var firstCtxSeq, firstIntentSeq int64
	for _, ev := range all {
		if ev.Kind == loop.KindContextAssembled && firstCtxSeq == 0 {
			firstCtxSeq = *ev.Seq
		}
		if ev.Kind == loop.KindToolCallIntent && firstIntentSeq == 0 {
			firstIntentSeq = *ev.Seq
		}
	}
	if firstCtxSeq == 0 || firstCtxSeq >= firstIntentSeq {
		t.Fatalf("context.assembled 必须先于 intent 落盘（ctx seq=%d intent seq=%d）", firstCtxSeq, firstIntentSeq)
	}

	// parts 形状：base / tools / history 三项粗粒度构成；state 一律 verbatim
	// （v0 无压缩 → origins / summary_ref / target_tokens 全缺省）。
	snap := decodeSnapshot(t, ctxs[0])
	gotSources := make([]string, 0, len(snap.Parts))
	var sum int64
	for _, p := range snap.Parts {
		gotSources = append(gotSources, p.Source)
		sum += p.Tokens
		if p.State != contracts.PartStateVerbatim {
			t.Fatalf("v0 无压缩，part %s 的 state = %q，want verbatim", p.Source, p.State)
		}
		if len(p.Origins) != 0 || p.SummaryRef != nil || p.TargetTokens != nil {
			t.Fatalf("v0 无压缩，part %s 不该带 origins/summary_ref/target_tokens: %+v", p.Source, p)
		}
		if p.Entries <= 0 || p.Tokens <= 0 {
			t.Fatalf("part %s 应有正贡献（entries=%d tokens=%d）", p.Source, p.Entries, p.Tokens)
		}
	}
	if want := []string{"base", "tools", "history"}; !equalStrings(gotSources, want) {
		t.Fatalf("parts 顺序 = %v，want %v（Q3：稳定在前、变动在后）", gotSources, want)
	}
	if snap.TokensUsed != sum {
		t.Fatalf("tokens_used = %d，want 各 part 之和 %d", snap.TokensUsed, sum)
	}
	if snap.TokensBudget != loop.DefaultContextBudget {
		t.Fatalf("tokens_budget = %d，want 缺省 %d", snap.TokensBudget, loop.DefaultContextBudget)
	}

	// entries 与真实装配一致：tools = 可见工具数；history = 该轮消息条数。
	reqs := h.provider.reqs()
	if len(reqs) != 3 {
		t.Fatalf("应有 3 次模型调用，得到 %d", len(reqs))
	}
	for i, p := range snap.Parts {
		switch p.Source {
		case "tools":
			if p.Entries != int64(len(reqs[0].Tools)) {
				t.Fatalf("tools entries = %d，want 可见工具数 %d", p.Entries, len(reqs[0].Tools))
			}
		case "history":
			if p.Entries != int64(len(reqs[0].Messages)) {
				t.Fatalf("history entries = %d，want 消息条数 %d", p.Entries, len(reqs[0].Messages))
			}
		default:
			if p.Entries != 1 {
				t.Fatalf("%s entries = %d，want 1", p.Source, p.Entries)
			}
		}
		_ = i
	}

	// 历史随轮次增长：第三轮的 history entries 应大于第一轮。
	snap3 := decodeSnapshot(t, ctxs[2])
	var hist3, hist1 int64
	for _, p := range snap3.Parts {
		if p.Source == "history" {
			hist3 = p.Entries
		}
	}
	for _, p := range snap.Parts {
		if p.Source == "history" {
			hist1 = p.Entries
		}
	}
	if hist3 <= hist1 {
		t.Fatalf("第三轮历史应比第一轮长（%d → %d）", hist1, hist3)
	}
}

// TestContextAssembledBudgetConfigurable：tokens_budget 可用 Config.ContextBudget 覆盖。
func TestContextAssembledBudgetConfigurable(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(stepText("ok"))

	if _, err := h.loop(func(c *loop.Config) { c.ContextBudget = 12345 }).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	snap := decodeSnapshot(t, contextEvents(h.events())[0])
	if snap.TokensBudget != 12345 {
		t.Fatalf("tokens_budget = %d，want 配置的 12345", snap.TokensBudget)
	}
}

// TestContextAssembledWithoutSystemOrTools：system 为空、无可见工具时
// 不产生 0 贡献的 part（"谁都没贡献"用"没有这个 source"表达）。
func TestContextAssembledWithoutSystemOrTools(t *testing.T) {
	h := newHarness(t)
	h.script(stepText("ok"))

	if _, err := h.loop(func(c *loop.Config) { c.System = "" }).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	snap := decodeSnapshot(t, contextEvents(h.events())[0])
	if len(snap.Parts) != 1 || snap.Parts[0].Source != "history" {
		t.Fatalf("只剩 history 一个贡献者，得到 %+v", snap.Parts)
	}
	if snap.TokensUsed <= 0 {
		t.Fatalf("user prompt 应有正 token 估算，得到 %d", snap.TokensUsed)
	}
}

// failAfterStore 前 left 次 Append 成功、之后全部失败：用来证明 context.assembled
// 落库失败只是观测性事实丢失，不中止 loop（与 X1 的 intent 写路径区分）。
type failAfterStore struct {
	inner core.Store
	left  int
}

func (s *failAfterStore) Append(ctx context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	if s.left <= 0 {
		return nil, errors.New("store: append failed (injected)")
	}
	s.left--
	return s.inner.Append(ctx, evs)
}

func (s *failAfterStore) ReadFrom(ctx context.Context, sessionID *contracts.Ulid, fromSeq int64, limit int) ([]contracts.Event, error) {
	return s.inner.ReadFrom(ctx, sessionID, fromSeq, limit)
}

// TestContextAssembledPublishFailureDoesNotAbortRun：context.assembled 落库失败
// 只是观测性事实丢失——不中止 loop（与 X1 的 intent 写路径刻意区分）。
func TestContextAssembledPublishFailureDoesNotAbortRun(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(stepText("ok")) // 不发起工具调用 → 没有 intent 写路径

	// 第一条（turn 1 的 context.assembled）之后全部落不进去。
	h.bus = bus.New(&failAfterStore{inner: h.store, left: 1})
	res, err := h.loop(nil).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("context.assembled 落库失败不该中止 Run: %v", err)
	}
	if res.Turns != 1 || res.Text != "ok" {
		t.Fatalf("Result 不符: %+v", res)
	}
}

// TestContextAssembledSubagentAlsoEmitted：子 agent 的 loop 同样每轮落一条
// （多 agent 下每个 agent 的上下文构成都可见；agent_id = child，K25）。
func TestContextAssembledSubagentAlsoEmitted(t *testing.T) {
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"scout"}, Model: maRootModel}
	h := newMAHarness(t, rootType, 3)
	h.registerType(agents.AgentType{ID: "scout", Tools: []string{"read"}, Model: "scout-model"})
	h.addTool("read", nil)

	reqID := core.NewUlid()
	h.prov.script(maRootModel,
		stepTools(call("spawn1", loop.ToolSpawn, string(spawnArgs("scout", "recon", "读一下 README", reqID)))),
		stepText("主 agent 收尾"),
	)
	h.prov.script("scout-model",
		stepText("scout 汇报完"),
	)

	if _, err := h.rootLoop(10).Run(context.Background(), "拆个任务"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	spawned := h.byKind(loop.KindAgentSpawned)
	if len(spawned) != 1 {
		t.Fatalf("应有 1 条 agent.spawned，得到 %d", len(spawned))
	}
	child := decodeAgentSpawned(t, spawned[0]).ChildAgentID

	var rootN, childN int
	for _, ev := range h.events() {
		if ev.Kind != loop.KindContextAssembled {
			continue
		}
		switch ev.AgentID {
		case child:
			childN++
		case h.rootID:
			rootN++
		}
	}
	if rootN == 0 || childN == 0 {
		t.Fatalf("父（%d 条）与子（%d 条）的 loop 都应落 context.assembled", rootN, childN)
	}
}
