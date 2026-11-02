package loop_test

// 缺口 5 第二件的**子 agent 覆盖**钉子（critic F3）：子 loop **必须**接上逐轮预算钩子
// （buildChildLoop 透传父的 TurnBudget）——不接 = 子 agent 的花费**不受逐轮刹车约束**
// （父的钩子只覆盖父自己的轮次；子 loop 的轮次对父的钩子不可见）→ 多 agent 模式下预算
// 闸门**半边失效**。
//
// 探针口径：**3 次 provider 调用 = 3 次钩子调用**（父轮1 spawn → 子轮1 complete → 父轮2
// 收尾；每轮 provider.Run 之前查一次）。
//
// 语义成立的前提：钩子闭包绑的 session id 对父子**是同一个**（父子同 session，K25 单流
// 多 agent）→ 查的是同一份账。
//
// 变异自证：把 buildChildLoop 里的 `TurnBudget: l.turnBudget` 去掉 → 本用例红
// （钩子只被调 2 次 = 父的两轮；子那一轮没查）。

import (
	"context"
	"testing"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/loop"
)

// TestChildLoopInheritsTurnBudget：父 + 子各跑一轮 → 钩子调用次数 = 总轮数（3）。
func TestChildLoopInheritsTurnBudget(t *testing.T) {
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"scout"}, Model: maRootModel, PermissionRequest: permFull()}
	h := newMAHarness(t, rootType, 3, allowRule("read"))
	h.registerType(agents.AgentType{ID: "scout", CanSpawn: nil, Tools: []string{"read"}, Model: "scout-model", PermissionRequest: permRO()})
	h.addTool("read", nil)

	reqID := core.NewUlid()
	h.prov.script(maRootModel,
		stepTools(call("spawn1", loop.ToolSpawn, string(spawnArgs("scout", "recon", "读一下 README 并总结", reqID)))),
		stepText("主 agent 收尾"),
	)
	h.prov.script("scout-model",
		stepTools(call("done1", loop.ToolComplete, string(completeArgs("读完并总结", nil, agents.TaintClean)))),
	)

	calls := 0
	l, err := loop.New(loop.Config{
		Provider:  h.prov,
		Bus:       h.bus,
		Registry:  h.reg,
		Policy:    h.engine,
		Approver:  core.DenyAll{}, // fail-close；allow 规则不触发审批
		SessionID: h.session,
		AgentID:   h.tree.Root(),
		Model:     maRootModel,
		MaxTurns:  10,
		Agents:    &loop.Agents{Tree: h.tree},
		TurnBudget: func(context.Context) error {
			calls++
			return nil // 一律放行（本用例只数"查了几次"）
		},
	})
	if err != nil {
		t.Fatalf("loop.New: %v", err)
	}

	res, err := l.Run(context.Background(), "拆个任务")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "主 agent 收尾" {
		t.Fatalf("根 agent 应收敛到自己的文本: %q", res.Text)
	}

	// provider 侧口径：父 2 轮 + 子 1 轮 = 3 次模型调用。
	parentReqs := len(h.prov.reqsFor(maRootModel))
	childReqs := len(h.prov.reqsFor("scout-model"))
	if parentReqs != 2 || childReqs != 1 {
		t.Fatalf("provider 调用次数 = 父 %d + 子 %d，want 2 + 1", parentReqs, childReqs)
	}

	// 钩子口径：**每轮 provider.Run 之前一次** → 3 次（父轮1 + 子轮1 + 父轮2）。
	if calls != parentReqs+childReqs {
		t.Fatalf("钩子被调 %d 次，want %d（每轮 provider.Run 前一次；子 agent 也走这道闸门）",
			calls, parentReqs+childReqs)
	}
}
