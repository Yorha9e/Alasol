package loop_test

// core/loop 的 **上下文面覆盖**（G-h）验收面：
//
//	① per-Run 生效：本轮快照里的 context 参数灌进本轮的装配器（引擎吃到覆盖值）；
//	② **无覆盖零行为变化**（酸测）：快照不带 context → 装配器保持自己的基线参数；
//	③ 占比分母随覆盖走：session 覆盖了窗口 → context.assembled 的 tokens_budget 换；
//	④ kind 覆盖：快照带 kind=tiered → 走策略路径；带 kind="" → 走追加式（不挂策略）；
//	⑤ 子 loop 透传父快照的参数（kind 归类型声明）。

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

// newTieredEngine 造一个最小可用的 tiered 引擎（loop 侧只关心它吃不吃参数）。
func newTieredEngine(t *testing.T, p compaction.Params) *compaction.Engine {
	t.Helper()
	engine, err := compaction.NewEngine(p, intSeq{tip: 1_000}, nil, nil, intSummarizer{}, &recEmitter{})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return engine
}

// TestContextOverrideReachesAssembler：per-Run 生效——本轮快照里的窗口/百分比/注入区
// 预算灌进本轮装配器（引擎的 Params() 变成覆盖后的值）。
func TestContextOverrideReachesAssembler(t *testing.T) {
	h := newHarness(t)
	h.script(stepText("好"))

	base := compaction.Params{Window: 256_000, L1TargetPct: 10, L2TargetPct: 1}
	engine := newTieredEngine(t, base)

	l := h.loop(func(c *loop.Config) {
		c.ContextStrategy = func() compaction.Assembler { return engine }
		c.RunOverrides = func(context.Context) (loop.RunOverrides, error) {
			return loop.RunOverrides{
				Context: &loop.ContextParams{
					Kind:              "tiered",
					Window:            512_000,
					L1Pct:             8,
					L2Pct:             0.5,
					RulesBudget:       20_000,
					CommonSenseBudget: 40_000,
				},
			}, nil
		}
	})
	if _, err := l.Run(context.Background(), "跑一轮"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := engine.Params()
	if got.Window != 512_000 {
		t.Errorf("引擎窗口 = %d，want 512000（per-Run 覆盖生效）", got.Window)
	}
	if got.L1TargetPct != 8 || got.L2TargetPct != 0.5 {
		t.Errorf("引擎 L1/L2 百分比 = %v/%v，want 8/0.5", got.L1TargetPct, got.L2TargetPct)
	}
	if got.RulesBudget != 20_000 || got.CommonSenseBudget != 40_000 {
		t.Errorf("引擎注入区上限 = %d/%d，want 20000/40000", got.RulesBudget, got.CommonSenseBudget)
	}
}

// TestContextNoOverrideKeepsBaseline（酸测）：快照不带 context → 装配器保持基线参数
// （**零行为变化**：覆盖前是什么样，覆盖后还是什么样）。
func TestContextNoOverrideKeepsBaseline(t *testing.T) {
	h := newHarness(t)
	h.script(stepText("好"))

	base := compaction.Params{Window: 256_000, L1TargetPct: 10, L2TargetPct: 1}
	engine := newTieredEngine(t, base)

	l := h.loop(func(c *loop.Config) {
		c.ContextStrategy = func() compaction.Assembler { return engine }
		// 快照不带 context（老装配：没挂覆盖钩子）。
	})
	if _, err := l.Run(context.Background(), "跑一轮"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := engine.Params()
	if got.Window != base.Window || got.L1TargetPct != base.L1TargetPct || got.L2TargetPct != base.L2TargetPct {
		t.Errorf("无覆盖时引擎参数不该变：%+v", got)
	}
}

// TestContextEmptyOverrideKeepsBaseline：快照带 context 但**各项都没覆盖**（全零）→
// 装配器保持基线参数（0 = 不覆盖，不是"覆盖成 0"）。
func TestContextEmptyOverrideKeepsBaseline(t *testing.T) {
	h := newHarness(t)
	h.script(stepText("好"))

	base := compaction.Params{Window: 256_000, L1TargetPct: 10, L2TargetPct: 1}
	engine := newTieredEngine(t, base)

	l := h.loop(func(c *loop.Config) {
		c.ContextStrategy = func() compaction.Assembler { return engine }
		c.RunOverrides = func(context.Context) (loop.RunOverrides, error) {
			return loop.RunOverrides{Context: &loop.ContextParams{Kind: "tiered"}}, nil
		}
	})
	if _, err := l.Run(context.Background(), "跑一轮"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := engine.Params()
	if got.Window != base.Window || got.L1TargetPct != base.L1TargetPct {
		t.Errorf("无覆盖值时引擎参数不该变：%+v", got)
	}
}

// TestContextKindSelectsStrategy：kind 覆盖决定走不走策略——
// ① 带 kind=tiered → 造装配器（策略路径）；② 带 kind="" → 追加式（**不**造装配器）。
func TestContextKindSelectsStrategy(t *testing.T) {
	h := newHarness(t)
	h.script(stepText("好"))

	var built int
	engine := newTieredEngine(t, compaction.Params{Window: 256_000})

	// ① kind=tiered → 造装配器。
	l := h.loop(func(c *loop.Config) {
		c.ContextStrategy = func() compaction.Assembler {
			built++
			return engine
		}
		c.RunOverrides = func(context.Context) (loop.RunOverrides, error) {
			return loop.RunOverrides{Context: &loop.ContextParams{Kind: "tiered"}}, nil
		}
	})
	if _, err := l.Run(context.Background(), "跑一轮"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if built != 1 {
		t.Fatalf("kind=tiered 时应造一次装配器，实际 %d 次", built)
	}

	// ② kind=""（追加式）→ 不造装配器。
	built = 0
	h2 := newHarness(t)
	h2.script(stepText("好"))
	l2 := h2.loop(func(c *loop.Config) {
		c.ContextStrategy = func() compaction.Assembler {
			built++
			return engine
		}
		c.RunOverrides = func(context.Context) (loop.RunOverrides, error) {
			return loop.RunOverrides{Context: &loop.ContextParams{Kind: ""}}, nil
		}
	})
	if _, err := l2.Run(context.Background(), "跑一轮"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if built != 0 {
		t.Fatalf("kind 为空（追加式）时不该造装配器，实际 %d 次", built)
	}
}

// TestContextWindowOverrideMovesTokensBudget：占比分母随覆盖走——session 覆盖了窗口
// → context.assembled 的 tokens_budget 用覆盖值（否则窗口换了、占比按旧窗口算 = 失真）。
func TestContextWindowOverrideMovesTokensBudget(t *testing.T) {
	h := newHarness(t)
	h.script(stepText("好"))

	l := h.loop(func(c *loop.Config) {
		c.ContextBudget = 12345
		c.RunOverrides = func(context.Context) (loop.RunOverrides, error) {
			return loop.RunOverrides{Context: &loop.ContextParams{Window: 512_000}}, nil
		}
	})
	if _, err := l.Run(context.Background(), "跑一轮"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	snaps := contextEvents(h.events())
	if len(snaps) == 0 {
		t.Fatal("应有 context.assembled 事件")
	}
	snap := decodeSnapshot(t, snaps[0])
	if snap.TokensBudget != 512_000 {
		t.Errorf("tokens_budget = %d，want 512000（覆盖窗口当占比分母）", snap.TokensBudget)
	}
}

// TestContextNoWindowOverrideKeepsTokensBudget（酸测）：没覆盖窗口 → 分母保持宿主给的。
func TestContextNoWindowOverrideKeepsTokensBudget(t *testing.T) {
	h := newHarness(t)
	h.script(stepText("好"))

	l := h.loop(func(c *loop.Config) {
		c.ContextBudget = 12345
		c.RunOverrides = func(context.Context) (loop.RunOverrides, error) {
			return loop.RunOverrides{Context: &loop.ContextParams{L1Pct: 8}}, nil
		}
	})
	if _, err := l.Run(context.Background(), "跑一轮"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	snaps := contextEvents(h.events())
	snap := decodeSnapshot(t, snaps[0])
	if snap.TokensBudget != 12345 {
		t.Errorf("tokens_budget = %d，want 12345（没覆盖窗口时保持基线）", snap.TokensBudget)
	}
}

// TestContextInvalidParamsFailClose：覆盖值不合法（窗口 < 256k 硬约束）→ 本轮 fail-close
// （不许静默降级成"用户配的旋钮没生效"）。
func TestContextInvalidParamsFailClose(t *testing.T) {
	h := newHarness(t)
	h.script(stepText("好"))

	engine := newTieredEngine(t, compaction.Params{Window: 256_000})
	l := h.loop(func(c *loop.Config) {
		c.ContextStrategy = func() compaction.Assembler { return engine }
		c.RunOverrides = func(context.Context) (loop.RunOverrides, error) {
			return loop.RunOverrides{Context: &loop.ContextParams{Kind: "tiered", Window: 100_000}}, nil
		}
	})
	if _, err := l.Run(context.Background(), "跑一轮"); err == nil {
		t.Fatal("窗口 < 256k 硬约束应 fail-close（不静默降级）")
	}
}

// TestContextChildLoopInheritsParams：子 loop 透传父快照的**参数**（窗口等）——子 agent
// 是同一轮的一部分，父子用同一套上下文预算口径（**kind 不透传**：子 agent 的策略 kind
// 归类型声明，W6-A）。
func TestContextChildLoopInheritsParams(t *testing.T) {
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"worker"}, Model: maRootModel, PermissionRequest: permFull()}
	h := newMAHarness(t, rootType, 3)
	h.registerType(agents.AgentType{
		ID:                "worker",
		Tools:             []string{"read"},
		Model:             "worker-model",
		PermissionRequest: permRO(),
		ContextStrategy:   kindPtr(contracts.ContextStrategyKindTiered),
	})
	h.addTool("read", nil)

	// 子 agent 的装配器：记录它吃到的参数（父快照的窗口应透传下来）。
	child := &paramsRecorder{params: compaction.Params{Window: 256_000}}
	agentsEnv := &loop.Agents{
		Tree: h.tree,
		ChildStrategy: func(agents.Node, agents.AgentType) compaction.AssemblerFactory {
			return func() compaction.Assembler { return child }
		},
	}
	l, err := loop.New(loop.Config{
		Provider:  core.Provider(h.prov),
		Bus:       h.bus,
		Registry:  h.reg,
		Policy:    h.engine,
		Approver:  core.DenyAll{},
		SessionID: h.session,
		AgentID:   h.tree.Root(),
		Model:     maRootModel,
		System:    "root-system",
		MaxTurns:  3,
		Agents:    agentsEnv,
		// 主 agent 也挂策略（快照里的 kind=tiered → 主 agent 走策略路径）。
		ContextStrategy: func() compaction.Assembler {
			return &paramsRecorder{params: compaction.Params{Window: 256_000}}
		},
		// 主 agent 的本轮快照：kind=tiered + 窗口覆盖。
		RunOverrides: func(context.Context) (loop.RunOverrides, error) {
			return loop.RunOverrides{
				Context: &loop.ContextParams{Kind: "tiered", Window: 512_000},
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("loop.New: %v", err)
	}

	reqID := core.NewUlid()
	h.prov.script(maRootModel,
		stepTools(call("sp1", loop.ToolSpawn, string(spawnArgs("worker", "w", "干活", reqID)))),
		stepText("root done"),
	)
	h.prov.script("worker-model",
		stepTools(call("done1", loop.ToolComplete, string(completeArgs("干完了", nil, agents.TaintClean)))),
	)
	if _, err := l.Run(context.Background(), "任务"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := child.got()
	if got.Window != 512_000 {
		t.Errorf("子 agent 装配器窗口 = %d，want 512000（父快照的参数透传给子）", got.Window)
	}
}

// paramsRecorder 是一个记录"收到的参数"的假装配器（实现 ParamsSetter / ParamsGetter）。
type paramsRecorder struct {
	mu     sync.Mutex
	params compaction.Params
}

func (p *paramsRecorder) Assemble(context.Context, []core.Message, int) (compaction.Assembled, error) {
	return compaction.Assembled{}, nil
}

func (p *paramsRecorder) SetParams(params compaction.Params) error {
	p.mu.Lock()
	p.params = params
	p.mu.Unlock()
	return nil
}

func (p *paramsRecorder) Params() compaction.Params {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.params
}

func (p *paramsRecorder) got() compaction.Params { return p.Params() }

// TestContextInjectionTruncationReportedAfterParamOverride：注入区截断**不许静默**
// （critic P2）：本轮把注入区预算改小 → 引擎按新预算**重新截断** → loop 灌参后**回读**
// 截断观测面 → 回调宿主（留痕）。
//
// 手法：给引擎灌一段超预算的规则文本 + 本轮把 RulesBudget 改到极小 → 截断发生 →
// 观察回调被调（条数 + 标志）。
func TestContextInjectionTruncationReportedAfterParamOverride(t *testing.T) {
	h := newHarness(t)
	h.script(stepText("好"))

	engine := newTieredEngine(t, compaction.Params{Window: 256_000})
	engine.SetRules(string(make([]byte, 0))) // 先清空（下面按本轮预算注入大文本）
	// 规则文本远超本轮预算（RulesBudget=100）→ 灌参后必截断。
	engine.SetRules(strings.Repeat("x", 4000))

	var gotRules, gotCommon bool
	var calls int
	l := h.loop(func(c *loop.Config) {
		c.ContextStrategy = func() compaction.Assembler { return engine }
		c.ContextInjectionObserver = func(rules, commonSense bool) {
			calls++
			gotRules, gotCommon = rules, commonSense
		}
		c.RunOverrides = func(context.Context) (loop.RunOverrides, error) {
			return loop.RunOverrides{
				Context: &loop.ContextParams{Kind: "tiered", RulesBudget: 100},
			}, nil
		}
	})
	if _, err := l.Run(context.Background(), "跑一轮"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if calls == 0 {
		t.Fatal("注入区被截断应回调宿主留痕（灌参后回读截断观测面）")
	}
	if !gotRules {
		t.Errorf("回调应报规则被截断: rules=%v commonSense=%v", gotRules, gotCommon)
	}
}
