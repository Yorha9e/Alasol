package loop_test

// G14 修复的回归：子 agent 必须**继承**父的思维链档位（ReasoningEffort）。
//
// 为什么不用 mockProvider.reqs()：reqs() 返回的是 cloneRequest 的**拷贝**——克隆会丢
// 字段（历史上 cloneRequest 就漏了 ReasoningEffort）→ 拿它断言档位会假绿/假红。
// 本文件让 provider **直接记录收到的请求原文**，断言的是线上真正发出去的那份。

import (
	"context"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/loop"
)

// effortSpyProvider 按 model 分派脚本，并**记录每次收到的请求原文**。
type effortSpyProvider struct {
	mu   sync.Mutex
	reqs []core.ModelRequest
}

func (p *effortSpyProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu.Lock()
	p.reqs = append(p.reqs, req)
	p.mu.Unlock()
	switch req.Model {
	case maRootModel:
		if len(req.Messages) <= 1 {
			return core.ModelResult{ToolCalls: []core.ToolCallReq{{
				ID:   "spawn1",
				Name: loop.ToolSpawn,
				Args: spawnArgs("scout", "recon", "查点资料", core.NewUlid()),
			}}}, nil
		}
		return core.ModelResult{Text: "root done"}, nil
	case "scout-model":
		return core.ModelResult{ToolCalls: []core.ToolCallReq{{
			ID:   "done1",
			Name: loop.ToolComplete,
			Args: completeArgs("scout done", nil, agents.TaintClean),
		}}}, nil
	}
	return core.ModelResult{Text: "?"}, nil
}

// effortFor 返回某个 model 收到的**第一次**请求里的档位（nil = 没带）。
func (p *effortSpyProvider) effortFor(model string) *string {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, req := range p.reqs {
		if req.Model == model {
			if req.ReasoningEffort == nil {
				return nil
			}
			v := *req.ReasoningEffort
			return &v
		}
	}
	return nil
}

func (p *effortSpyProvider) sawModel(model string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, req := range p.reqs {
		if req.Model == model {
			return true
		}
	}
	return false
}

// TestCloneRequestKeepsReasoningEffort：测试辅助 cloneRequest **不许丢档位**。
//
// 为什么盯这个：cloneRequest 是"把 provider 收到的请求抄一份给断言用"的克隆器。历史上
// 它漏了 ReasoningEffort → 拿 reqs() 断言"档位透传"会**假绿**（"没带档位"其实只是
// 克隆丢了，不是真的没带）。克隆必须与原请求在档位上**逐字等价**（指针也要深拷贝）。
func TestCloneRequestKeepsReasoningEffort(t *testing.T) {
	eff := "high"
	req := core.ModelRequest{Model: "m", ReasoningEffort: &eff}
	got := cloneRequest(req)
	if got.ReasoningEffort == nil {
		t.Fatal("cloneRequest 丢了 ReasoningEffort（断言用例会假绿）")
	}
	if *got.ReasoningEffort != "high" {
		t.Errorf("cloneRequest 的 ReasoningEffort = %q，want high", *got.ReasoningEffort)
	}
	// 深拷贝：改克隆体不得影响原请求（否则断言之间互相污染）。
	*got.ReasoningEffort = "low"
	if *req.ReasoningEffort != "high" {
		t.Errorf("cloneRequest 共享了指针：改克隆体把原请求改成了 %q", *req.ReasoningEffort)
	}
	// 原请求没档位 → 克隆体也没有（nil ≠ &""）。
	if c := cloneRequest(core.ModelRequest{Model: "m"}); c.ReasoningEffort != nil {
		t.Errorf("未设档位时克隆体带上了 %q，want nil", *c.ReasoningEffort)
	}
}

// TestChildAgentInheritsReasoningEffort：父 loop 配了档位 → **子 agent 的每轮请求也带
// 同一档位**（G14 修复：buildChildLoop 漏传 → 子 agent 静默退回端点默认档）。
func TestChildAgentInheritsReasoningEffort(t *testing.T) {
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"scout"}, Model: maRootModel, PermissionRequest: permFull()}
	h := newMAHarness(t, rootType, 3)
	h.registerType(agents.AgentType{ID: "scout", CanSpawn: nil, Tools: nil, Model: "scout-model", PermissionRequest: permRO()})

	prov := &effortSpyProvider{}
	root, err := loop.New(loop.Config{
		Provider:  prov,
		Bus:       h.bus,
		Registry:  h.reg,
		Policy:    h.engine,
		Approver:  core.DenyAll{},
		SessionID: h.session,
		AgentID:   h.tree.Root(),
		Model:     maRootModel,
		MaxTurns:  10,
		Agents:    &loop.Agents{Tree: h.tree, Verifier: h.verifier},
		// 思维链档位（G14）：父配了 high → 子必须继承。
		ReasoningEffort: "high",
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
	if !prov.sawModel("scout-model") {
		t.Fatal("子 agent 根本没被调用（spawn 没跑起来？）")
	}

	got := prov.effortFor("scout-model")
	if got == nil {
		t.Fatal("子 agent 的请求没有 ReasoningEffort（buildChildLoop 漏传？）")
	}
	if *got != "high" {
		t.Errorf("子 agent 的 ReasoningEffort = %q，want high（与父同源）", *got)
	}
	// 父自己的请求当然也要带（对照组：证明是"继承"而不是 provider 侧的巧合）。
	if p := prov.effortFor(maRootModel); p == nil || *p != "high" {
		t.Errorf("父 agent 的 ReasoningEffort = %v，want high", p)
	}
}

// TestChildAgentWithoutReasoningEffortStaysEmpty：父没配档位 → 子也不带（nil = 不传，
// 端点用默认档）。防"继承"写成了"硬塞一个空串"。
func TestChildAgentWithoutReasoningEffortStaysEmpty(t *testing.T) {
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"scout"}, Model: maRootModel, PermissionRequest: permFull()}
	h := newMAHarness(t, rootType, 3)
	h.registerType(agents.AgentType{ID: "scout", CanSpawn: nil, Tools: nil, Model: "scout-model", PermissionRequest: permRO()})

	prov := &effortSpyProvider{}
	root, err := loop.New(loop.Config{
		Provider:  prov,
		Bus:       h.bus,
		Registry:  h.reg,
		Policy:    h.engine,
		Approver:  core.DenyAll{},
		SessionID: h.session,
		AgentID:   h.tree.Root(),
		Model:     maRootModel,
		MaxTurns:  10,
		Agents:    &loop.Agents{Tree: h.tree, Verifier: h.verifier},
		// 不配 ReasoningEffort → 空 = 不传。
	})
	if err != nil {
		t.Fatalf("loop.New(root): %v", err)
	}
	if _, err := root.Run(context.Background(), "派个活"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !prov.sawModel("scout-model") {
		t.Fatal("子 agent 根本没被调用")
	}
	if got := prov.effortFor("scout-model"); got != nil {
		t.Errorf("未设档位时子 agent 的 ReasoningEffort = %q，want nil（不传）", *got)
	}
}
