package host

// 本文件钉死 fork 旁支装配的 **v3-S6-B 缺口 6**（对照 hub.go loopFor 的 loop.Agents 装配）：
//
//  1. MultiAgent 开 → fork loop 装配拿到的 Agents 非 nil、树 root = forkAgent（fork 是
//     自己 session 的主 agent）；RunBtw 跑的时候树在（钩子观测点），**跑完即回收**
//     （用完即弃——留着就是每 /btw 一次泄漏一棵，T4-1）；
//  2. MultiAgent 关（默认）→ Agents nil = 单 agent 模式零行为变化（fork session 无树）；
//  3. fork 的 ChildStrategy 钩子绑**fork 自己的 session**（T4-3：绑主 session 会让旁支
//     的压缩事件落进主会话的流）；
//  4. 防回归（fork.go 文件头 / RunBtw ④ 的刻意决定）：fork 的 ContextStrategy 恒 nil、
//     TurnBudget 恒 nil——不许顺手"补全"。
//
// internal（package host）原因：要直接断言 forkLoopConfig 的装配结果与 h.agentTrees
// （黑盒面没有"取 fork loop 配置"的公开 API）；形态照 host/agenttypes_internal_test.go
// 的 MultiAgent 测法（直接调私有装配函数 + 断言树 root）。

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/compaction"
)

// newForkAgentsTestHub 装配一个 Hub（与 fork_internal_test.go 的 newForkTestHub 同款：
// 临时事件库 + 脚本 provider + 非 git CWD = 无自动 gate、确定性），只多一个 MultiAgent 开关。
func newForkAgentsTestHub(t *testing.T, prov core.Provider, multiAgent bool) *Hub {
	t.Helper()
	h, err := New(Config{
		DBPath:     filepath.Join(t.TempDir(), "events.db"),
		Model:      "fork-agents-test",
		System:     "fork-agents-system",
		MaxTurns:   4,
		Provider:   prov,
		CWD:        t.TempDir(), // 非 git 目录 → 无自动 repo gate
		MultiAgent: multiAgent,
	})
	if err != nil {
		t.Fatalf("host.New(MultiAgent=%v): %v", multiAgent, err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// MultiAgent 开：fork loop 装配拿到的 Agents 非 nil、树 root = forkAgent、ChildStrategy
// 绑 fork session；同一棵树登记进 h.agentTrees[forkSID]。防回归：ContextStrategy / TurnBudget
// 仍 nil（fork 的刻意决定）。
func TestForkLoopConfigMultiAgentOn(t *testing.T) {
	h := newForkAgentsTestHub(t, &forkTestProvider{}, true)
	forkSID := core.NewUlid()
	forkAgent := core.NewUlid()

	cfg := h.forkLoopConfig(forkSID, forkAgent)

	if cfg.Agents == nil {
		t.Fatal("MultiAgent 开 → fork loop 装配应拿到非 nil 的 Agents")
	}
	if cfg.Agents.Tree == nil {
		t.Fatal("Agents.Tree 应非 nil")
	}
	if got := cfg.Agents.Tree.Root(); got != forkAgent {
		t.Fatalf("树 root 应 = forkAgent（fork 是自己 session 的主 agent），得到 %q want %q", got, forkAgent)
	}
	if cfg.Agents.Verifier != nil {
		t.Error("Verifier 应 nil（与 loopFor 同口径：宿主暂无生产实现）")
	}
	if cfg.Agents.ChildStrategy == nil {
		t.Error("ChildStrategy 应非 nil（W6-A 子策略钩子绑 fork session）")
	}
	// 树登记进 h.agentTrees（可查面；取用照 agentTreeForLocked 的要求持 h.mu）。
	h.mu.Lock()
	tree := h.agentTrees[forkSID]
	h.mu.Unlock()
	if tree != cfg.Agents.Tree {
		t.Fatal("装配拿到的 Tree 应与 h.agentTrees[forkSID] 是同一棵树")
	}
	// 防回归：fork 的两条刻意决定。
	if cfg.ContextStrategy != nil {
		t.Fatal("fork 的 ContextStrategy 应恒 nil（不挂上下文策略——RunBtw ④ 的刻意决定）")
	}
	if cfg.TurnBudget != nil {
		t.Fatal("fork 的 TurnBudget 应恒 nil（不接逐轮预算——RunBtw ④ 的刻意决定）")
	}
}

// MultiAgent 关（默认）：Agents 留 nil = 单 agent 模式零行为变化；同样钉防回归口径。
func TestForkLoopConfigMultiAgentOff(t *testing.T) {
	h := newForkAgentsTestHub(t, &forkTestProvider{}, false)

	cfg := h.forkLoopConfig(core.NewUlid(), core.NewUlid())

	if cfg.Agents != nil {
		t.Fatalf("MultiAgent 关 → Agents 应 nil（单 agent 零行为变化），得到 %+v", cfg.Agents)
	}
	if cfg.ContextStrategy != nil {
		t.Fatal("fork 的 ContextStrategy 应恒 nil（关态同样不挂——刻意决定）")
	}
	if cfg.TurnBudget != nil {
		t.Fatal("fork 的 TurnBudget 应恒 nil（关态同样不接——刻意决定）")
	}
}

// 端到端（MultiAgent 开）：RunBtw 跑完后 fork session 的树**已被回收**（用完即弃）——
// 跑之前树在（root = 本次 forkAgent，BtwResult.ForkAgentID），跑完就没了。
//
// 为什么"跑完就该没"（T4-1 泄漏修）：树登记在 h.agentTrees（进程内活状态），**永不回收**
// → 每跑一次 /btw 就泄漏一棵（节点 + summary 都在里面）；长期跑的 hub 上是无界增长。
// 旁支是一次性执行线（结果只回给用户、不回灌主上下文），它的树**只服务这一轮**。
//
// 观测点：**预埋钩子**（BtwPromoteHook）在 RunBtw 返回前触发 —— 那时树还在（钩子要能
// 观测树）；RunBtw 返回后（defer 已回收）树不在。
//
// 变异自证：去掉 RunBtw 里的 defer h.dropForkAgentTree(forkSID) → 本用例红
// （跑完树还在 = 泄漏）。
func TestRunBtwMultiAgentOnDropsForkAgentTreeAfterRun(t *testing.T) {
	h := newForkAgentsTestHub(t, &forkTestProvider{}, true)
	ctx := context.Background()
	sid := NewSession()

	if _, err := h.RunPrompt(ctx, sid, "主问题"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	// 预埋钩子：跑的过程中抓树（钩子触发时树还在）。
	var seenRoot contracts.Ulid
	var seenTree *agents.Tree
	h.SetBtwPromoteHook(func(res BtwResult) {
		h.mu.Lock()
		seenTree = h.agentTrees[res.ForkSessionID]
		h.mu.Unlock()
		if seenTree != nil {
			seenRoot = seenTree.Root()
		}
	})

	res, err := h.RunBtw(ctx, sid, "旁支问题")
	if err != nil {
		t.Fatalf("RunBtw: %v", err)
	}

	// ① 跑的过程中：树在，root = 本次 forkAgent。
	if seenTree == nil {
		t.Fatal("MultiAgent 开：RunBtw 期间 fork session 应有 agent 树（钩子观测点）")
	}
	if seenRoot != res.ForkAgentID {
		t.Fatalf("fork 树 root 应 = forkAgent %q，得到 %q", res.ForkAgentID, seenRoot)
	}

	// ② 跑完之后：树**已回收**（用完即弃——不再泄漏）。
	h.mu.Lock()
	tree := h.agentTrees[res.ForkSessionID]
	h.mu.Unlock()
	if tree != nil {
		t.Fatal("RunBtw 跑完后 fork session 的 agent 树应被回收（用完即弃；留着就是每 /btw 一次泄漏一棵）")
	}
}

// 端到端（MultiAgent 关）：RunBtw 跑完后 fork session **没有**树（零行为变化）。
func TestRunBtwMultiAgentOffNoForkAgentTree(t *testing.T) {
	h := newForkAgentsTestHub(t, &forkTestProvider{}, false)
	ctx := context.Background()
	sid := NewSession()

	if _, err := h.RunPrompt(ctx, sid, "主问题"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	res, err := h.RunBtw(ctx, sid, "旁支问题")
	if err != nil {
		t.Fatalf("RunBtw: %v", err)
	}

	h.mu.Lock()
	tree := h.agentTrees[res.ForkSessionID]
	h.mu.Unlock()
	if tree != nil {
		t.Fatal("MultiAgent 关：fork session 不应建 agent 树（单 agent 零行为变化）")
	}
}

// TestForkChildStrategyBoundToForkSession（T4-3 钉子）：fork loop 的 ChildStrategy 钩子
// 必须绑**fork 自己的 session**（子 agent 与 fork 主 agent同 session，K25）——不能绑主
// session（那会让旁支的压缩事件落到主会话的流里：主 session 零新增被破坏）。
//
// 观测点：钩子造出的引擎发 `context.compacted` 事件时，**归属的 session_id** = 闭包捕获
// 的 sessionID。故：触发一次压缩 → 读 fork session 的事件流 → 该事件必须在里面，
// 且**主 session 的事件流里不能有**它。
//
// 变异自证：把 forkLoopConfig 里的 h.childStrategyFactory(forkSID) 改成绑主 session
// → 本用例红（事件落到主 session，fork session 里没有）。
func TestForkChildStrategyBoundToForkSession(t *testing.T) {
	h := newForkAgentsTestHub(t, &forkTestProvider{}, true)
	ctx := context.Background()
	mainSID := NewSession()
	if _, err := h.RunPrompt(ctx, mainSID, "主问题"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	forkSID := core.NewUlid()
	forkAgent := core.NewUlid()
	cfg := h.forkLoopConfig(forkSID, forkAgent)
	if cfg.Agents == nil || cfg.Agents.ChildStrategy == nil {
		t.Fatal("MultiAgent 开：fork loop 装配应有 ChildStrategy 钩子")
	}

	// 钩子造引擎（子类型声明 tiered → 宿主实现 kind → 产出工厂）。
	childID := core.NewUlid()
	tiered := &contracts.ContextStrategy{Kind: contracts.ContextStrategyKindTiered}
	f := cfg.Agents.ChildStrategy(
		agents.Node{ID: childID, Depth: 2, Type: "x"},
		agents.AgentType{ID: "x", Model: "m", ContextStrategy: tiered},
	)
	if f == nil {
		t.Fatal("tiered 子类型应产出工厂")
	}
	engine, ok := f().(*compaction.Engine)
	if !ok {
		t.Fatalf("工厂应产 *compaction.Engine，得到 %T", f())
	}
	// 三个阶段：最老阶段相对当前 aging 到 l1 → 降级 → 触发压缩（发 context.compacted）。
	hist := []core.Message{{Role: "user", Content: "a"}}
	if _, err := engine.Assemble(ctx, hist, 1); err != nil {
		t.Fatalf("Assemble 1: %v", err)
	}
	engine.MarkBoundary()
	hist = append(hist, core.Message{Role: "user", Content: "b"})
	if _, err := engine.Assemble(ctx, hist, 2); err != nil {
		t.Fatalf("Assemble 2: %v", err)
	}
	engine.MarkBoundary()
	hist = append(hist, core.Message{Role: "user", Content: "c"})
	if _, err := engine.Assemble(ctx, hist, 3); err != nil {
		t.Fatalf("Assemble 3: %v", err)
	}

	// ① 事件必须落在 **fork session** 的事件流里。
	forkEvents, err := h.store.ReadFrom(ctx, &forkSID, 1, 0)
	if err != nil {
		t.Fatalf("读 fork session 事件: %v", err)
	}
	var sawForkCompacted bool
	for _, ev := range forkEvents {
		if ev.Kind == compaction.KindContextCompacted && ev.SessionID == forkSID && ev.AgentID == childID {
			sawForkCompacted = true
		}
	}
	if !sawForkCompacted {
		t.Fatalf("fork 钩子的压缩事件应归属 fork session %s（子 agent %s）", forkSID, childID)
	}
	// ② **主 session** 的事件流里不能有它（旁支不污染主上下文——fork 的核心不变式）。
	mainEvents, err := h.store.ReadFrom(ctx, &mainSID, 1, 0)
	if err != nil {
		t.Fatalf("读主 session 事件: %v", err)
	}
	for _, ev := range mainEvents {
		if ev.Kind == compaction.KindContextCompacted {
			t.Fatalf("旁支的压缩事件落到了主 session %s（钩子绑错 session）", mainSID)
		}
	}
}
