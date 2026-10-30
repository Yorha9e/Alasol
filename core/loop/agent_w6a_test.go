// W6-A（2026-10-31 拍板）钉子：子 agent 的上下文策略**按类型显式声明**装配，未声明 =
// append 默认（**不继承主 agent 的策略**）；声明非 append 但宿主没接钩子/钩子造不出工厂 →
// spawn 的子 agent 以 agent.failed 收口（fail-close，不静默降级成 append）。
//
// 七组用例：
//
//   - A：子类型未声明 + 主 agent 挂了策略 → 子跑追加式（钩子不被调用）——"不继承"钉子；
//   - B：子类型声明 tiered + 有钩子 → 钩子被调用（收到该 child 节点 + childType），子 loop
//     用其工厂；钩子拿到的 child.ID 与树节点一致（P2-1：宿主靠它绑定子 agent 身份）；
//   - C：子类型声明 tiered + 无钩子（或钩子返回 nil）→ 子以 agent.failed 收口且写明原因；
//   - D：子类型声明了 context_strategy 但 kind 为空（非法声明）→ 同样 fail-close 收口
//     （不静默降级成 append）。
//   - E：子类型**显式声明** kind=append → 与"未声明"同口径：钩子 0 次调、无 fail-close
//     （append 是合法声明，不是坏声明）；
//   - F：子类型声明非 append + 钩子**造出 nil Assembler**（工厂指针非 nil，装配器造不出来）
//     → 子 loop 装配期 fail-close → 子以 agent.failed 收口，原因写明 nil Assembler
//     （不静默降级成 append）。
//   - G：子类型声明的 kind **越枚举**（装载期后共享的那份声明被改写）→ 同样 fail-close
//     收口，原因写明 fail-close 与那个越界的 kind（不静默降级成 append）。
package loop_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/compaction"
	"github.com/Yorha9e/alasol/core/loop"
)

// countingAssembler 是个最小策略实现：记录 Assemble 被调次数（子 loop 用没用策略一看便知）。
type countingAssembler struct {
	mu    sync.Mutex
	calls int
}

func (a *countingAssembler) Assemble(_ context.Context, history []core.Message, _ int) (compaction.Assembled, error) {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	return compaction.Assembled{Messages: history}, nil
}

func (a *countingAssembler) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

// 策略工厂记录器：记录钩子调用与收到的 child 节点 / childType。
type strategyRecorder struct {
	mu       sync.Mutex
	hookCall int
	gotChild agents.Node
	gotType  agents.AgentType
	asm      *countingAssembler
}

// hook 是 loop.Agents.ChildStrategy 钩子（签名带子 agent 身份：child 节点 + 类型声明）。
func (r *strategyRecorder) hook(child agents.Node, childType agents.AgentType) compaction.AssemblerFactory {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hookCall++
	r.gotChild = child
	r.gotType = childType
	if r.asm == nil {
		r.asm = &countingAssembler{}
	}
	return func() compaction.Assembler { return r.asm }
}

func (r *strategyRecorder) hookCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hookCall
}

func boolPtr(b bool) *bool { return &b }

func kindPtr(k contracts.ContextStrategyKind) *contracts.ContextStrategy {
	return &contracts.ContextStrategy{Kind: k}
}

// buildW6Loop 建一个带 ChildStrategy 钩子的多 agent root loop（钩子可为 nil）。
// parentCalls 返回主 agent 策略工厂的调用计数（每次 Run 造一个 fresh Assembler）：
// 若子 loop 误继承了主 agent 的策略工厂，子 Run 会再调一次 → 计数变 2（A 用例的证人）。
func buildW6Loop(t *testing.T, h *maHarness, rec *strategyRecorder) (*loop.Loop, *int64) {
	t.Helper()
	agentsEnv := &loop.Agents{Tree: h.tree, Verifier: h.verifier}
	if rec != nil {
		agentsEnv.ChildStrategy = rec.hook
	}
	calls := new(int64)
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
		// 主 agent 挂一个策略（W6-A 的"不继承"对照组：子类型未声明时不得被它污染）。
		ContextStrategy: func() compaction.Assembler { atomic.AddInt64(calls, 1); return &countingAssembler{} },
	})
	if err != nil {
		t.Fatalf("loop.New: %v", err)
	}
	return l, calls
}

// A：子类型未声明 context_strategy → 子跑追加式，钩子不被调用（不继承主 agent 的策略）。
func TestW6AUndeclaredChildIsAppendNotInherited(t *testing.T) {
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"worker"}, Model: maRootModel, PermissionRequest: permFull()}
	h := newMAHarness(t, rootType, 3)
	h.registerType(agents.AgentType{ID: "worker", Tools: []string{"read"}, Model: "worker-model", PermissionRequest: permRO()})
	h.addTool("read", nil)

	rec := &strategyRecorder{}
	l, parentCalls := buildW6Loop(t, h, rec)

	reqID := core.NewUlid()
	h.prov.script(maRootModel,
		stepTools(call("sp1", loop.ToolSpawn, string(spawnArgs("worker", "w", "干活", reqID)))),
		stepText("root done"),
	)
	h.prov.script("worker-model", stepText("worker done")) // 不调 complete → 异常收口也行，这里看策略装配
	if _, err := l.Run(context.Background(), "任务"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if rec.hookCalls() != 0 {
		t.Errorf("子类型未声明 context_strategy → 钩子不该被调用（W6-A 不继承），调了 %d 次", rec.hookCalls())
	}
	// 主策略工厂只被 root 自己的 Run 调过 1 次：子若误继承（直抄 l.ctxStrategy）会再调 1 次。
	if atomic.LoadInt64(parentCalls) != 1 {
		t.Errorf("子 agent 应不继承主 agent 的策略工厂：主工厂被调 %d 次（期望 1 = 仅 root 自己）", atomic.LoadInt64(parentCalls))
	}
}

// E：子类型**显式声明** context_strategy kind=append → 与"未声明"同口径：走追加式，
// **钩子不被调用**，也**不 fail-close**。
//
// 为什么单列一条钉子：append 是**合法的默认策略声明**（不是坏声明）——它与 A 的"未声明"
// 走同一条路，但声明本身是**显式**的（W6-A：子 agent 的策略显式声明、不继承主 agent）。
// 若实现把"显式 append"误判成非法声明（或误送进装配钩子），本条会红。
func TestW6AExplicitAppendDeclarationRunsAppend(t *testing.T) {
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"worker"}, Model: maRootModel, PermissionRequest: permFull()}
	h := newMAHarness(t, rootType, 3)
	h.registerType(agents.AgentType{
		ID:                "worker",
		Tools:             []string{"read"},
		Model:             "worker-model",
		PermissionRequest: permRO(),
		ContextStrategy:   kindPtr(contracts.ContextStrategyKindAppend), // 显式声明 append
	})
	h.addTool("read", nil)

	rec := &strategyRecorder{}
	l, parentCalls := buildW6Loop(t, h, rec)

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

	// ① 显式 append 无需造工厂 → 钩子不该被调用。
	if rec.hookCalls() != 0 {
		t.Errorf("显式声明 kind=append → 钩子不该被调用，调了 %d 次", rec.hookCalls())
	}
	// ② append 是**合法声明** → 不得 fail-close（不该有 agent.failed）。
	if fails := h.byKind(loop.KindAgentFailed); len(fails) != 0 {
		t.Errorf("显式声明 kind=append 不该 fail-close，却落了 %d 条 agent.failed", len(fails))
	}
	// ③ 子正常收敛（complete 走通）→ 树里子节点是 completed，不是 failed。
	kids := h.tree.Children(h.rootID)
	if len(kids) != 1 || kids[0].Status != agents.StatusCompleted {
		t.Errorf("显式声明 kind=append 的子应正常收敛: %+v", kids)
	}
	// ④ 显式 append 仍**不继承**主 agent 的策略（主工厂只被 root 自己调过 1 次）。
	if atomic.LoadInt64(parentCalls) != 1 {
		t.Errorf("子 agent 应不继承主 agent 的策略工厂：主工厂被调 %d 次（期望 1 = 仅 root 自己）", atomic.LoadInt64(parentCalls))
	}
}

// B：子类型声明非 append + 有钩子 → 钩子收到该 childType，子 loop 用其工厂装配。
func TestW6ADeclaredChildUsesHookFactory(t *testing.T) {
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"worker"}, Model: maRootModel, PermissionRequest: permFull()}
	h := newMAHarness(t, rootType, 3)
	h.registerType(agents.AgentType{
		ID:                 "worker",
		Tools:              []string{"read"},
		Model:              "worker-model",
		PermissionRequest:  permRO(),
		ContextStrategy:    kindPtr(contracts.ContextStrategyKindTiered),
		ShareProjectAssets: boolPtr(true),
	})
	h.addTool("read", nil)

	rec := &strategyRecorder{}
	l, _ := buildW6Loop(t, h, rec)

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

	if rec.hookCalls() != 1 {
		t.Fatalf("子类型声明 tiered → 钩子应被调用 1 次，得到 %d 次", rec.hookCalls())
	}
	rec.mu.Lock()
	got := rec.gotType
	gotChild := rec.gotChild
	rec.mu.Unlock()
	if got.ID != "worker" || got.ContextStrategy == nil || got.ContextStrategy.Kind != contracts.ContextStrategyKindTiered {
		t.Errorf("钩子收到的 childType 不对: %+v", got)
	}
	if got.ShareProjectAssets == nil || !*got.ShareProjectAssets {
		t.Errorf("钩子应收到 share_project_assets=true（W6-A 共享开关随类型传入）: %+v", got)
	}
	// 钩子拿到的**子 agent 身份**必须与树里的节点一致（P2-1：宿主要靠 child.ID 把子
	// agent 绑进 per-Run 策略工厂——recall 视角隔离 / 事件归属）。
	kids := h.tree.Children(h.rootID)
	if len(kids) != 1 {
		t.Fatalf("树里应有 1 个子节点，得到 %d 个", len(kids))
	}
	if gotChild.ID == "" || gotChild.ID != kids[0].ID {
		t.Errorf("钩子拿到的 child.ID 与树节点不一致: hook=%q tree=%q", gotChild.ID, kids[0].ID)
	}
	if gotChild.Type != "worker" {
		t.Errorf("钩子拿到的 child.Type 应为 worker，得到 %q", gotChild.Type)
	}
	if rec.asm.count() == 0 {
		t.Errorf("子 loop 应经钩子工厂装配（Assemble 至少 1 次），得到 %d 次", rec.asm.count())
	}
}

// C：子类型声明非 append 但钩子缺失/造不出工厂 → 子以 agent.failed 收口（fail-close）。
func TestW6ADeclaredChildWithoutHookFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		nilHook bool
		nilFact bool
	}{
		{name: "无钩子", nilHook: true},
		{name: "钩子返回nil工厂", nilFact: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
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

			agentsEnv := &loop.Agents{Tree: h.tree, Verifier: h.verifier}
			if !tc.nilHook {
				agentsEnv.ChildStrategy = func(agents.Node, agents.AgentType) compaction.AssemblerFactory {
					if tc.nilFact {
						return nil
					}
					return func() compaction.Assembler { return &countingAssembler{} }
				}
			}
			l, err := loop.New(loop.Config{
				Provider: core.Provider(h.prov), Bus: h.bus, Registry: h.reg,
				Policy: h.engine, Approver: core.DenyAll{},
				SessionID: h.session, AgentID: h.tree.Root(),
				Model: maRootModel, System: "root-system", MaxTurns: 3,
				Agents: agentsEnv,
			})
			if err != nil {
				t.Fatalf("loop.New: %v", err)
			}

			reqID := core.NewUlid()
			h.prov.script(maRootModel,
				stepTools(call("sp1", loop.ToolSpawn, string(spawnArgs("worker", "w", "干活", reqID)))),
				stepText("root done"),
			)
			h.prov.script("worker-model", stepText("worker done"))
			if _, err := l.Run(context.Background(), "任务"); err != nil {
				t.Fatalf("Run: %v", err)
			}

			// 子应以 agent.failed 收口，且原因写明 fail-close（不静默降级成 append）。
			fails := h.byKind(loop.KindAgentFailed)
			if len(fails) != 1 {
				t.Fatalf("应有 1 条 agent.failed（fail-close 收口），得到 %d 条", len(fails))
			}
			fp := decodeAgentFailed(t, fails[0])
			if !strings.Contains(fp.Error.Message, "fail-close") {
				t.Errorf("agent.failed 原因应写明 fail-close: %q", fp.Error.Message)
			}
			if !strings.Contains(fp.Error.Message, "tiered") {
				t.Errorf("agent.failed 原因应点名声明的策略 kind: %q", fp.Error.Message)
			}
			// 树里子节点已收口为 failed（不留 running 僵尸）。
			kids := h.tree.Children(h.rootID)
			if len(kids) != 1 || kids[0].Status != agents.StatusFailed {
				t.Errorf("子节点应 failed 收口: %+v", kids)
			}
		})
	}
}

// D：子类型**声明了 context_strategy 但 kind 为空**（非法声明）→ fail-close 收口，
// 不静默降级成 append（"声明了却没说什么" = 策略语义悄悄变味）。
//
// **装载期已被挡住**（W6-A：能力包与类型注册表两条装载路径都拒空 kind 的声明）——但
// AgentType.ContextStrategy 是**指针**：注册表拷的是结构体、共享的是那个 ContextStrategy
// 本身。注册之后它被改成空 kind（配置热改 / 复用同一份声明对象），运行期就会拿到一个
// "kind 为空"的坏声明 → 运行期必须自己 fail-close（不能指望装载期那道闸）。
func TestW6AEmptyKindDeclarationFailsClosed(t *testing.T) {
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"worker"}, Model: maRootModel, PermissionRequest: permFull()}
	h := newMAHarness(t, rootType, 3)
	cs := kindPtr(contracts.ContextStrategyKindTiered)
	h.registerType(agents.AgentType{
		ID:                "worker",
		Tools:             []string{"read"},
		Model:             "worker-model",
		PermissionRequest: permRO(),
		ContextStrategy:   cs,
	})
	// 装载期之后，共享的那份声明被改成"声明了，却没说什么"（指针共享 → 运行期拿到坏声明）。
	cs.Kind = ""
	h.addTool("read", nil)

	rec := &strategyRecorder{}
	l, _ := buildW6Loop(t, h, rec)

	reqID := core.NewUlid()
	h.prov.script(maRootModel,
		stepTools(call("sp1", loop.ToolSpawn, string(spawnArgs("worker", "w", "干活", reqID)))),
		stepText("root done"),
	)
	h.prov.script("worker-model", stepText("worker done"))
	if _, err := l.Run(context.Background(), "任务"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 非法声明不得进钩子（连"按 kind 造工厂"都无从谈起）——更不得静默按 append 跑。
	if rec.hookCalls() != 0 {
		t.Errorf("kind 为空的非法声明不该进装配钩子，调了 %d 次", rec.hookCalls())
	}
	fails := h.byKind(loop.KindAgentFailed)
	if len(fails) != 1 {
		t.Fatalf("应有 1 条 agent.failed（fail-close 收口），得到 %d 条", len(fails))
	}
	fp := decodeAgentFailed(t, fails[0])
	if !strings.Contains(fp.Error.Message, "fail-close") {
		t.Errorf("agent.failed 原因应写明 fail-close: %q", fp.Error.Message)
	}
	if !strings.Contains(fp.Error.Message, "kind") {
		t.Errorf("agent.failed 原因应点名 kind 为空: %q", fp.Error.Message)
	}
	kids := h.tree.Children(h.rootID)
	if len(kids) != 1 || kids[0].Status != agents.StatusFailed {
		t.Errorf("子节点应 failed 收口: %+v", kids)
	}
}

// G：子类型声明的 kind **越枚举**（装载期后共享的那份声明被改写）→ 同样 fail-close 收口，
// 原因写明 fail-close 与那个越界的 kind（不静默降级成 append）。
//
// **装载期已被挡住**（能力包 / 类型注册表两条装载路径都拒越枚举的 kind）——但
// AgentType.ContextStrategy 是**指针**：注册表拷的是结构体、共享的是那个 ContextStrategy
// 本身。注册之后它被改成一个越枚举的 kind（配置热改 / 复用同一份声明对象），运行期就会
// 拿到一个"宿主无从装配"的坏声明 → 运行期必须自己 fail-close（不能指望装载期那道闸）。
//
// 与 D（kind 为空）是**两条不同的坏法**：空 kind 有"声明了却没说什么"的兜底，越枚举没有
// ——不补这道闸，越枚举的声明会一路走到装配钩子（宿主造不出对应策略 → 策略语义变味）。
func TestW6AOutOfEnumKindDeclarationFailsClosed(t *testing.T) {
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"worker"}, Model: maRootModel, PermissionRequest: permFull()}
	h := newMAHarness(t, rootType, 3)
	cs := kindPtr(contracts.ContextStrategyKindTiered)
	h.registerType(agents.AgentType{
		ID:                "worker",
		Tools:             []string{"read"},
		Model:             "worker-model",
		PermissionRequest: permRO(),
		ContextStrategy:   cs,
	})
	// 装载期之后，共享的那份声明被改成一个**越枚举**的 kind（宿主无从装配）。
	cs.Kind = "custom_thing"
	h.addTool("read", nil)

	rec := &strategyRecorder{}
	l, _ := buildW6Loop(t, h, rec)

	reqID := core.NewUlid()
	h.prov.script(maRootModel,
		stepTools(call("sp1", loop.ToolSpawn, string(spawnArgs("worker", "w", "干活", reqID)))),
		stepText("root done"),
	)
	h.prov.script("worker-model", stepText("worker done"))
	if _, err := l.Run(context.Background(), "任务"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 越枚举的声明不该进装配钩子（宿主根本造不出对应策略）——更不得静默按 append 跑。
	if rec.hookCalls() != 0 {
		t.Errorf("越枚举的 kind 不该进装配钩子，调了 %d 次", rec.hookCalls())
	}
	fails := h.byKind(loop.KindAgentFailed)
	if len(fails) != 1 {
		t.Fatalf("应有 1 条 agent.failed（fail-close 收口），得到 %d 条", len(fails))
	}
	fp := decodeAgentFailed(t, fails[0])
	if !strings.Contains(fp.Error.Message, "fail-close") {
		t.Errorf("agent.failed 原因应写明 fail-close: %q", fp.Error.Message)
	}
	if !strings.Contains(fp.Error.Message, "custom_thing") {
		t.Errorf("agent.failed 原因应点名越界的 kind: %q", fp.Error.Message)
	}
	kids := h.tree.Children(h.rootID)
	if len(kids) != 1 || kids[0].Status != agents.StatusFailed {
		t.Errorf("子节点应 failed 收口: %+v", kids)
	}
}

// F：子类型声明非 append + 钩子**造出 nil Assembler**（工厂指针非 nil，但装配器造不出来）
// → 子 loop 装配期 fail-close → 子以 agent.failed 收口，原因写明 nil Assembler。
//
// 为什么单列一条钉子：坏装配有两种坏法——钩子**没接**（指针为 nil）与钩子**造不出装配器**
// （工厂返回 nil Assembler）。后者更隐蔽：工厂指针非 nil，childStrategyFor 看不出来，
// 若下游静默当成"没挂策略"走 append，用户配了策略却跑的是追加式（策略语义悄悄变味）。
// 故 loop 的 Run 在装配期就 fail-close：loop.error 留痕 + 子 agent 以 agent.failed 收口。
func TestW6ANilAssemblerFromHookFailsClosed(t *testing.T) {
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

	agentsEnv := &loop.Agents{Tree: h.tree, Verifier: h.verifier}
	agentsEnv.ChildStrategy = func(agents.Node, agents.AgentType) compaction.AssemblerFactory {
		// 工厂指针非 nil，但造出来的装配器是 nil（坏装配）。
		return func() compaction.Assembler { return nil }
	}
	l, err := loop.New(loop.Config{
		Provider: core.Provider(h.prov), Bus: h.bus, Registry: h.reg,
		Policy: h.engine, Approver: core.DenyAll{},
		SessionID: h.session, AgentID: h.tree.Root(),
		Model: maRootModel, System: "root-system", MaxTurns: 3,
		Agents: agentsEnv,
	})
	if err != nil {
		t.Fatalf("loop.New: %v", err)
	}

	reqID := core.NewUlid()
	h.prov.script(maRootModel,
		stepTools(call("sp1", loop.ToolSpawn, string(spawnArgs("worker", "w", "干活", reqID)))),
		stepText("root done"),
	)
	h.prov.script("worker-model", stepText("worker done"))
	if _, err := l.Run(context.Background(), "任务"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 子应以 agent.failed 收口，且原因写明 nil Assembler（不静默降级成 append）。
	fails := h.byKind(loop.KindAgentFailed)
	if len(fails) != 1 {
		t.Fatalf("应有 1 条 agent.failed（fail-close 收口），得到 %d 条", len(fails))
	}
	fp := decodeAgentFailed(t, fails[0])
	if !strings.Contains(fp.Error.Message, "nil Assembler") {
		t.Errorf("agent.failed 原因应写明 nil Assembler: %q", fp.Error.Message)
	}
	if !strings.Contains(fp.Error.Message, "fail-close") {
		t.Errorf("agent.failed 原因应写明 fail-close: %q", fp.Error.Message)
	}
	// 装配残缺要留痕（loop.error，phase=context.strategy）——故障可复盘。
	sawTrace := false
	for _, ev := range h.byKind(loop.KindLoopError) {
		if strings.Contains(string(ev.Payload), "nil Assembler") {
			sawTrace = true
			break
		}
	}
	if !sawTrace {
		t.Error("装配残缺应落一条 loop.error 留痕（写明 nil Assembler）")
	}
	// 树里子节点已收口为 failed（不留 running 僵尸）。
	kids := h.tree.Children(h.rootID)
	if len(kids) != 1 || kids[0].Status != agents.StatusFailed {
		t.Errorf("子节点应 failed 收口: %+v", kids)
	}
}
