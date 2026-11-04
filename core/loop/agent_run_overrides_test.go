package loop_test

// G-c/G-d **F1** 的回归钉子：子 agent 类型**未绑定模型**时，buildChildLoop 的模型
// 兜底必须取**父本轮生效模型**（RunOverrides.Model 覆盖），不是 loop 基值（hub 默认）——
// 否则父换了模型/端点、子却拿默认模型打覆盖端点（模型/端点错配；critic 实测
// `[override-model, hub-default, override-model]`，第 2 次是子 agent）。
//
// 夹具：**两个** provider 实例——base（loop 基值，覆盖生效时一次都不该被调）与
// over（RunOverrides.Provider，本轮唯一该用的）。脚本按"请求里有没有 spawn 工具"
// 区分父/子（父类型 can_spawn、子类型没有）。

import (
	"context"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/loop"
)

// overrideSpy 记录每次收到的请求原文（断言 model/provider 面），并按父/子脚本收口。
type overrideSpy struct {
	mu   sync.Mutex
	reqs []core.ModelRequest
}

func (p *overrideSpy) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu.Lock()
	p.reqs = append(p.reqs, req)
	p.mu.Unlock()
	if spyHasTool(req, loop.ToolSpawn) {
		// 父：首轮 spawn 一个子任务，spawn 回来后次轮收敛。
		if len(req.Messages) <= 1 {
			return core.ModelResult{ToolCalls: []core.ToolCallReq{{
				ID:   "spawn-ov",
				Name: loop.ToolSpawn,
				Args: spawnArgs("scout", "recon", "子任务", core.NewUlid()),
			}}}, nil
		}
		return core.ModelResult{Text: "root done"}, nil
	}
	// 子：complete 收敛。
	return core.ModelResult{ToolCalls: []core.ToolCallReq{{
		ID:   "done-ov",
		Name: loop.ToolComplete,
		Args: completeArgs("child done", nil, agents.TaintClean),
	}}}, nil
}

func (p *overrideSpy) snapshot() []core.ModelRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]core.ModelRequest, len(p.reqs))
	copy(out, p.reqs)
	return out
}

func (p *overrideSpy) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.reqs)
}

// spyHasTool 报告一次请求的可见工具里有没有指定工具（区分父/子调用的判据）。
func spyHasTool(req core.ModelRequest, name string) bool {
	for _, tl := range req.Tools {
		if tl.Name == name {
			return true
		}
	}
	return false
}

// TestChildAgentUsesParentEffectiveModelWhenTypeUnbound（F1）：父 Run 冻结
// RunOverrides{Model:"override-model", Provider:over} + 子类型 Model 为空 →
//
//	子请求的 model == override-model（兜底用父本轮生效模型，**不是** hub 基值）；
//	父与子的调用**全部**落在 over 上（provider 随快照走），base 零调用。
func TestChildAgentUsesParentEffectiveModelWhenTypeUnbound(t *testing.T) {
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"scout"}, Model: maRootModel, PermissionRequest: permFull()}
	h := newMAHarness(t, rootType, 3)
	// 子类型**不绑模型**（Model 留空）——F1 的前提：兜底路径必须被走到。
	h.registerType(agents.AgentType{ID: "scout", CanSpawn: nil, Tools: nil, PermissionRequest: permRO()})

	base := &overrideSpy{} // loop 基值 provider（hub 默认）：覆盖生效时不该被调
	over := &overrideSpy{} // 覆盖 provider（RunOverrides.Provider）

	root, err := loop.New(loop.Config{
		Provider:  base,
		Bus:       h.bus,
		Registry:  h.reg,
		Policy:    h.engine,
		Approver:  core.DenyAll{},
		SessionID: h.session,
		AgentID:   h.tree.Root(),
		Model:     maRootModel, // hub 基值（与覆盖不同，才能分辨兜底取了哪个）
		MaxTurns:  10,
		Agents:    &loop.Agents{Tree: h.tree, Verifier: h.verifier},
		// G-c/G-d：父 Run 开始冻结的本轮快照 = model 覆盖 + provider 覆盖。
		RunOverrides: func(context.Context) (loop.RunOverrides, error) {
			return loop.RunOverrides{Model: "override-model", Provider: over}, nil
		},
	})
	if err != nil {
		t.Fatalf("loop.New(root): %v", err)
	}

	res, err := root.Run(context.Background(), "派个活")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "root done" {
		t.Fatalf("父应收敛: %q", res.Text)
	}

	// provider 面：hub 基值 provider 零调用（本轮只该用 over）。
	if n := base.count(); n != 0 {
		t.Fatalf("基值 provider 被调 %d 次（覆盖生效时本轮只该用 over）", n)
	}
	overReqs := over.snapshot()
	if len(overReqs) < 3 {
		t.Fatalf("over 收到 %d 次调用，want ≥3（父 spawn + 子 + 父收敛）", len(overReqs))
	}
	// 全部调用（父与子）都是本轮生效模型。
	for i, req := range overReqs {
		if req.Model != "override-model" {
			t.Fatalf("第 %d 次调用 model = %q，want override-model（父/子都应是本轮生效模型）: tools=%v",
				i, req.Model, req.Tools)
		}
	}
	// 显式钉子 agent 那一支：请求里**没有** spawn 工具的调用 = 子 loop 发的
	//（子类型 CanSpawn 为空）——F1 的正主就是它。
	childSeen := false
	for _, req := range overReqs {
		if spyHasTool(req, loop.ToolSpawn) {
			continue
		}
		childSeen = true
		if req.Model != "override-model" {
			t.Fatalf("子 agent model = %q，want override-model（F1：类型未绑定 → 父本轮生效模型，"+
				"不是 hub 基值）", req.Model)
		}
	}
	if !childSeen {
		t.Fatal("没抓到子 agent 的调用（spawn 没跑起来？断言失去意义）")
	}
}
