package loop_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/compaction"
	"github.com/Yorha9e/alasol/core/loop"
)

// 本文件测 v2-S1 context_strategy 的 loop 薄装配点：
//   - 酸测试：不挂策略（nil）与显式 AppendAssembler 的装配输出逐字节等价（= v1）；
//   - 挂上 tiered 策略 → context.assembled 点亮 stage:N / ledger / rules 来源。

// ---------------------------------------------------------------------------
// engine 依赖的最小替身（loop_test 侧自备，不依赖 compaction 包的白盒替身）
// ---------------------------------------------------------------------------

type intSeq struct{ tip int64 }

func (s intSeq) Tip(context.Context) (int64, error) { return s.tip, nil }

type intLedger struct {
	lp contracts.LedgerUpdatedPayload
}

func (l intLedger) Ledger(context.Context) (contracts.LedgerUpdatedPayload, error) { return l.lp, nil }

type intSummarizer struct{}

func (intSummarizer) Summarize(_ context.Context, _, text string) (compaction.SummaryResult, error) {
	// 取前 40 字符当摘要（确定性，够小）。
	usage := &contracts.ModelTurnUsage{InputTokens: 12, OutputTokens: 6}
	if len(text) > 40 {
		return compaction.SummaryResult{Text: "摘要:" + text[:40], Model: "fake-model", Usage: usage}, nil
	}
	return compaction.SummaryResult{Text: "摘要:" + text, Model: "fake-model", Usage: usage}, nil
}

type recEmitter struct {
	mu     sync.Mutex
	events []contracts.ContextCompactedPayload
}

func (e *recEmitter) EmitCompacted(_ context.Context, p contracts.ContextCompactedPayload) error {
	e.mu.Lock()
	e.events = append(e.events, p)
	e.mu.Unlock()
	return nil
}

// snapshotSources 取一条 context.assembled 快照的 parts 来源列表。
func snapshotSources(snap contracts.ContextSnapshot) []string {
	out := make([]string, 0, len(snap.Parts))
	for _, p := range snap.Parts {
		out = append(out, p.Source)
	}
	return out
}

// hasSource 报告来源列表里是否有给定 source（stage:N 用前缀匹配）。
func hasSource(sources []string, want string) bool {
	for _, s := range sources {
		if s == want {
			return true
		}
		if strings.HasPrefix(want, "stage:") && strings.HasPrefix(s, "stage:") {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// 酸测试：不挂策略 = v1 等价；显式 append 策略 = 不挂
// ---------------------------------------------------------------------------

func TestContextStrategyNilEqualsAppendEquivalent(t *testing.T) {
	script := func() []scriptStep {
		return []scriptStep{
			stepTools(call("c1", "echo", `{"text":"hi"}`)),
			stepText("完成"),
		}
	}
	run := func(strategy compaction.AssemblerFactory) ([]core.Message, []contracts.ContextPart) {
		h := newHarness(t, allowRule("echo"))
		h.addTool("echo", nil)
		h.provider.steps = script()
		h.provider.repeatLast = false
		h.provider.reset()
		if _, err := h.loop(func(c *loop.Config) { c.ContextStrategy = strategy }).Run(context.Background(), "帮我 echo"); err != nil {
			t.Fatalf("Run: %v", err)
		}
		reqs := h.provider.reqs()
		// 第一轮的请求消息（策略只可能改 history 区；system/tools 不变）。
		snap := decodeSnapshot(t, contextEvents(h.events())[0])
		return reqs[0].Messages, snap.Parts
	}

	nilMsgs, nilParts := run(nil)
	appMsgs, appParts := run(func() compaction.Assembler { return compaction.AppendAssembler{} })

	// 消息逐字节等价（append 不改写 history）。
	if len(nilMsgs) != len(appMsgs) {
		t.Fatalf("nil 与 AppendAssembler 消息数不同：%d vs %d", len(nilMsgs), len(appMsgs))
	}
	for i := range nilMsgs {
		if nilMsgs[i].Role != appMsgs[i].Role || nilMsgs[i].Content != appMsgs[i].Content {
			t.Fatalf("消息 %d 不一致：%+v vs %+v", i, nilMsgs[i], appMsgs[i])
		}
	}
	// 快照 parts 等价（都不含 stage:N/ledger/rules，只有 base/tools/history）。
	if len(nilParts) != len(appParts) {
		t.Fatalf("nil 与 AppendAssembler parts 数不同：%d vs %d", len(nilParts), len(appParts))
	}
	for _, p := range nilParts {
		if hasSource([]string{p.Source}, "stage:") || p.Source == "ledger" || p.Source == "rules" {
			t.Fatalf("不挂策略不该出现 stage:N/ledger/rules：%s", p.Source)
		}
	}
}

// ---------------------------------------------------------------------------
// 挂上 tiered 策略：点亮 stage:N / ledger / rules
// ---------------------------------------------------------------------------

func TestContextStrategyAttachedLightsUpSources(t *testing.T) {
	h := newHarness(t, allowRule("echo"), allowRule(compaction.ToolStage), allowRule(compaction.ToolRecall))
	h.addTool("echo", nil)

	emit := &recEmitter{}
	engine, err := compaction.NewEngine(
		compaction.Params{Window: 256_000},
		intSeq{tip: 1_000_000},
		nil, // text source：本测试不触发 recall
		intLedger{lp: contracts.LedgerUpdatedPayload{
			Goals:       []string{"交付 stage1"},
			Constraints: []string{"不破坏 v1"},
		}},
		intSummarizer{},
		emit,
	)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	engine.SetRules("[项目规则] 遵循 AGENTS.md")
	// recall / stage 由 loop 随策略自动注入为框架工具（per-loop，不经 registry）；这里只需
	// 放行它们（策略可见集仍过 policy：deny 的不进可见集）。

	// 脚本：多个 turn，中间夹 stage 边界调用，造出多个已完成阶段（供压缩/降级）。
	h.provider.steps = []scriptStep{
		stepTools(call("s1", compaction.ToolStage, `{"label":"阶段1通过"}`)),
		stepTools(call("s2", compaction.ToolStage, `{"label":"阶段2通过"}`)),
		stepTools(call("s3", compaction.ToolStage, `{"label":"阶段3通过"}`)),
		stepTools(call("e1", "echo", `{"text":"x"}`)),
		stepText("收尾"),
	}
	h.provider.repeatLast = false
	h.provider.reset()

	l := h.loop(func(c *loop.Config) {
		c.ContextStrategy = func() compaction.Assembler { return engine }
	})
	if _, err := l.Run(context.Background(), "长程任务"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 最后一轮的 context.assembled 应点亮 ledger / rules / stage:N 来源。
	snaps := contextEvents(h.events())
	if len(snaps) == 0 {
		t.Fatal("应有 context.assembled 事件")
	}
	last := decodeSnapshot(t, snaps[len(snaps)-1])
	sources := snapshotSources(last)
	if !hasSource(sources, "ledger") {
		t.Errorf("context.assembled 应出现 ledger 来源：%v", sources)
	}
	if !hasSource(sources, "rules") {
		t.Errorf("context.assembled 应出现 rules 来源：%v", sources)
	}
	if !hasSource(sources, "stage:") {
		t.Errorf("context.assembled 应出现 stage:N 来源（分级压缩）：%v", sources)
	}

	// stage:N part 必带 origins（seq 区间）与 target_tokens（J9 目标残留）。
	for _, p := range last.Parts {
		if !strings.HasPrefix(p.Source, "stage:") {
			continue
		}
		if len(p.Origins) == 0 {
			t.Errorf("stage:N part %s 应带 origins（seq 区间）", p.Source)
		}
		if p.TargetTokens == nil {
			t.Errorf("stage:N part %s 应带 target_tokens（J9）", p.Source)
		}
		if p.State != contracts.PartStateCompacted {
			t.Errorf("stage:N part %s state 应=compacted，得到 %s", p.Source, p.State)
		}
	}

	// 阶段降级发生了（多个已完成阶段 → 最老降 L2）→ engine 发了 downgrade 事件。
	var downgrades int
	for _, ev := range emit.events {
		if ev.Action == contracts.ContextCompactionActionDowngrade {
			downgrades++
			// refs 永不指摘要：ref 只指 stage:N（原文）。
			if !strings.HasPrefix(ev.Ref, "stage:") {
				t.Errorf("downgrade 事件 ref 应以 stage: 前缀：%s", ev.Ref)
			}
		}
	}
	if downgrades == 0 {
		t.Error("多个已完成阶段应触发 downgrade 事件")
	}

	// 阶段工具走了 X1 三段路径（intent → completed），且在写路径时间线上。
	stageIntents := 0
	for _, ev := range h.events() {
		if ev.Kind == loop.KindToolCallIntent {
			if p := decodeIntent(t, ev); p.ToolName == compaction.ToolStage {
				stageIntents++
			}
		}
	}
	if stageIntents == 0 {
		t.Error("stage 工具应走 X1 intent 落盘")
	}
}

// 编译期证明：本文件的替身就是 compaction 包的窄接口。
var (
	_ compaction.SeqSource    = intSeq{}
	_ compaction.LedgerSource = intLedger{}
	_ compaction.Summarizer   = intSummarizer{}
	_ compaction.Emitter      = (*recEmitter)(nil)
)

// ---------------------------------------------------------------------------
// F1：框架工具（recall / stage）同样过 agent 类型白名单（R5：不暴露即不存在）
// ---------------------------------------------------------------------------

func TestFrameworkToolsRespectAgentTypeWhitelist(t *testing.T) {
	rootType := agents.AgentType{ID: "root", Tools: []string{"read"}, Model: maRootModel, PermissionRequest: permFull()}
	h := newMAHarness(t, rootType, 3,
		allowRule("read"),
		allowRule(compaction.ToolRecall),
		allowRule(compaction.ToolStage),
	)
	h.addTool("read", nil)

	engine, err := compaction.NewEngine(
		compaction.Params{Window: 256_000},
		intSeq{tip: 1_000_000},
		nil,
		nil,
		intSummarizer{},
		&recEmitter{},
	)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	// 根 agent 的类型只列了 "read" → recall/stage **不该**进模型可见集。
	l, err := loop.New(loop.Config{
		Provider:        core.Provider(h.prov),
		Bus:             h.bus,
		Registry:        h.reg,
		Policy:          h.engine,
		Approver:        core.DenyAll{},
		SessionID:       h.session,
		AgentID:         h.tree.Root(),
		Model:           maRootModel,
		MaxTurns:        3,
		Agents:          &loop.Agents{Tree: h.tree, Verifier: h.verifier},
		ContextStrategy: func() compaction.Assembler { return engine },
	})
	if err != nil {
		t.Fatalf("loop.New: %v", err)
	}
	h.prov.script(maRootModel, stepText("ok"))
	if _, err := l.Run(context.Background(), "干活"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	reqs := h.prov.reqsFor(maRootModel)
	if len(reqs) == 0 {
		t.Fatal("应有模型请求")
	}
	tools := reqs[0].Tools
	if hasTool(tools, compaction.ToolRecall) {
		t.Errorf("类型白名单外的 recall 不该可见（F1）: %v", toolNames(tools))
	}
	if hasTool(tools, compaction.ToolStage) {
		t.Errorf("类型白名单外的 stage 不该可见（F1）: %v", toolNames(tools))
	}
	if !hasTool(tools, "read") {
		t.Errorf("类型白名单内的 read 应可见: %v", toolNames(tools))
	}
}

// ---------------------------------------------------------------------------
// F12：同名工具（框架工具 vs 注册表工具）在装配期 fail-close 拒绝
// ---------------------------------------------------------------------------

func TestDuplicateToolNameFailsClosed(t *testing.T) {
	h := newHarness(t, allowRule(compaction.ToolRecall))
	h.addTool(compaction.ToolRecall, nil) // 注册表里有个同名工具

	engine, err := compaction.NewEngine(
		compaction.Params{Window: 256_000},
		intSeq{tip: 1},
		nil,
		nil,
		intSummarizer{},
		&recEmitter{},
	)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	l := h.loop(func(c *loop.Config) {
		c.ContextStrategy = func() compaction.Assembler { return engine }
	})
	_, err = l.Run(context.Background(), "干活")
	if err == nil {
		t.Fatal("同名工具应在装配期 fail-close 拒绝（不许静默遮蔽）")
	}
	if !strings.Contains(err.Error(), "重名") {
		t.Errorf("错误应说明是重名冲突: %v", err)
	}
	// fail-close 发生在**装配期**：模型一次都不该被调用。
	if reqs := h.provider.reqs(); len(reqs) != 0 {
		t.Errorf("重名冲突应在装配期拒绝，模型不该被调用（收到 %d 次请求）", len(reqs))
	}
	// 可见集里也不该出现两个同名工具。
	reqs := h.provider.reqs()
	if len(reqs) > 0 {
		seen := map[string]int{}
		for _, tl := range reqs[0].Tools {
			seen[tl.Name]++
		}
		for name, n := range seen {
			if n > 1 {
				t.Errorf("可见集里 %q 出现了 %d 次（F12：按名字去重）", name, n)
			}
		}
	}
}
