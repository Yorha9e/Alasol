package loop_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/loop"
)

// TestGoalLedgerToolEmitsUpdated：J19——agent 调 ledger 工具 → ledger.updated 事件进流，
// 且 ledger 工具进了模型可见集（X6：关键字段立即落盘）。
func TestGoalLedgerToolEmitsUpdated(t *testing.T) {
	h := newHarness(t, allowRule(loop.ToolLedger))
	// 脚本：turn1 调 ledger 更新目标；turn2 收敛。
	h.script(
		stepTools(call("tc-ledger", loop.ToolLedger, `{"goals":["ship v3"],"constraints":["no breaking change"]}`)),
		stepText("done"),
	)
	l := h.loop(func(c *loop.Config) {
		c.Store = h.store
		c.GoalLedger = true
	})
	res, err := l.Run(context.Background(), "开始")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Turns != 2 {
		t.Fatalf("Turns = %d, want 2", res.Turns)
	}

	// ledger 工具进了模型可见集（turn1 的请求里能看到它）。
	reqs := h.provider.reqs()
	if len(reqs) == 0 {
		t.Fatal("没有请求记录")
	}
	if !toolVisibleIn(reqs[0].Tools, loop.ToolLedger) {
		t.Error("GoalLedger 开启时 ledger 工具应进模型可见集")
	}

	// ledger.updated 事件进了流，载荷带 goals/constraints（关键字段立即落盘）。
	var ledgers []contracts.LedgerUpdatedPayload
	for _, ev := range h.events() {
		if ev.Kind == "ledger.updated" {
			var p contracts.LedgerUpdatedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Fatalf("解 ledger.updated: %v", err)
			}
			ledgers = append(ledgers, p)
		}
	}
	if len(ledgers) == 0 {
		t.Fatal("事件流里没有 ledger.updated（J19 生产者缺失）")
	}
	last := ledgers[len(ledgers)-1]
	if len(last.Goals) != 1 || last.Goals[0] != "ship v3" {
		t.Errorf("ledger goals = %v, want [ship v3]", last.Goals)
	}
	if len(last.Constraints) != 1 || last.Constraints[0] != "no breaking change" {
		t.Errorf("ledger constraints = %v", last.Constraints)
	}
}

// TestGoalLedgerOffByDefault：GoalLedger 关（默认）→ ledger 工具**不**注入、零行为变化（酸测试）。
func TestGoalLedgerOffByDefault(t *testing.T) {
	h := newHarness(t, allowRule(loop.ToolLedger))
	h.script(stepText("hi"))
	l := h.loop(func(c *loop.Config) {
		c.Store = h.store
		// GoalLedger 不设 → 默认 false。
	})
	if _, err := l.Run(context.Background(), "开始"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	reqs := h.provider.reqs()
	if len(reqs) > 0 && toolVisibleIn(reqs[0].Tools, loop.ToolLedger) {
		t.Error("GoalLedger 默认关时 ledger 工具不应进可见集")
	}
	// 没有 ledger.updated 事件。
	for _, ev := range h.events() {
		if ev.Kind == "ledger.updated" {
			t.Error("GoalLedger 默认关时不应有 ledger.updated 事件")
		}
	}
}

// TestGoalLedgerProceduralFlushedAtTurnEnd：X6——过程字段（completed/pending）turn 末合并
// 落**一条** ledger.updated（不是每次 UpdateProcedural 都落）。
func TestGoalLedgerProceduralFlushedAtTurnEnd(t *testing.T) {
	h := newHarness(t, allowRule(loop.ToolLedger))
	h.script(
		stepTools(call("tc-ledger", loop.ToolLedger, `{"goals":["g"]}`)),
		stepTools(call("tc-ledger2", loop.ToolLedger, `{"completed":["step1"],"pending":["step2"]}`)),
		stepText("done"),
	)
	l := h.loop(func(c *loop.Config) {
		c.Store = h.store
		c.GoalLedger = true
	})
	if _, err := l.Run(context.Background(), "开始"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 数 ledger.updated：turn1 的 goals 是关键的（立即落 1 条）；turn2 的过程字段 turn 末合并（1 条）。
	var ledgers []contracts.LedgerUpdatedPayload
	for _, ev := range h.events() {
		if ev.Kind == "ledger.updated" {
			var p contracts.LedgerUpdatedPayload
			_ = json.Unmarshal(ev.Payload, &p)
			ledgers = append(ledgers, p)
		}
	}
	if len(ledgers) != 2 {
		t.Fatalf("ledger.updated 条数 = %d, want 2（turn1 关键字段 1 条 + turn2 过程字段合并 1 条）", len(ledgers))
	}
	// 最后一条应带 completed/pending（过程字段已合并落盘）。
	last := ledgers[len(ledgers)-1]
	if len(last.Completed) != 1 || last.Completed[0] != "step1" {
		t.Errorf("最后一条 completed = %v, want [step1]", last.Completed)
	}
	if len(last.Goals) != 1 || last.Goals[0] != "g" {
		t.Errorf("过程字段合并时应带上既有 goals（全量快照）: %v", last.Goals)
	}
}

// TestGoalLedgerCriticalFieldsSurviveAcrossRuns 是 **p1 回归**：goal ledger 的关键字段
// （目标 / 约束）**跨 Run 不能被抹掉**。
//
// 背景：ledger.updated 是**全量快照 + LWW**（G23/G8）——每条事件带四张清单的全文，后写的
// 覆盖先写的。若写入器每次 Run 都从**空**账本起步（不加载当前态），Run2 只更新过程字段
// （completed）时，落盘的快照里 goals/constraints 就是空的 → Run1 写下的关键字段被静默
// 抹掉（没有任何报错）。修法：写入器每个 Run 先 LoadLedger 折出当前账本做基线。
func TestGoalLedgerCriticalFieldsSurviveAcrossRuns(t *testing.T) {
	h := newHarness(t, allowRule(loop.ToolLedger))
	// 脚本跨两次 Run 连着走：Run1 设关键字段 → Run2 只报过程字段。
	h.script(
		stepTools(call("tc-ledger-1", loop.ToolLedger, `{"goals":["ship v3"],"constraints":["no breaking change"]}`)),
		stepText("done"),
		stepTools(call("tc-ledger-2", loop.ToolLedger, `{"completed":["step1"]}`)),
		stepText("done again"),
	)
	l := h.loop(func(c *loop.Config) {
		c.Store = h.store
		c.GoalLedger = true
	})
	ctx := context.Background()
	if _, err := l.Run(ctx, "第一轮"); err != nil {
		t.Fatalf("Run #1: %v", err)
	}
	if _, err := l.Run(ctx, "第二轮"); err != nil {
		t.Fatalf("Run #2: %v", err)
	}

	var ledgers []contracts.LedgerUpdatedPayload
	for _, ev := range h.events() {
		if ev.Kind != "ledger.updated" {
			continue
		}
		var p contracts.LedgerUpdatedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("解 ledger.updated: %v", err)
		}
		ledgers = append(ledgers, p)
	}
	if len(ledgers) < 2 {
		t.Fatalf("ledger.updated 条数 = %d, want >= 2（Run1 关键字段 1 条 + Run2 过程字段合并 1 条）", len(ledgers))
	}
	last := ledgers[len(ledgers)-1]
	if len(last.Goals) != 1 || last.Goals[0] != "ship v3" {
		t.Errorf("跨 Run 后 goals 被抹掉了：%v（关键字段必须仍在）", last.Goals)
	}
	if len(last.Constraints) != 1 || last.Constraints[0] != "no breaking change" {
		t.Errorf("跨 Run 后 constraints 被抹掉了：%v（关键字段必须仍在）", last.Constraints)
	}
	if len(last.Completed) != 1 || last.Completed[0] != "step1" {
		t.Errorf("Run2 的过程字段没落上：%v", last.Completed)
	}
}

// TestGoalLedgerRespectsEffectivePermission 是 **p3 回归**：**框架工具也过 perm 校验**。
//
// ledger 是写账本的（SideEffectWrite）——本 agent 的生效权限没有 write → 必须被挡住。
// 背景：此前 permDenial 只查**注册表**，而框架工具（recall / stage / ledger）不在注册表里
// → 一律按 SideEffectNone 处理 = **绕过**了 perm 校验（声明的权限是上限，不可被 allow 放宽）。
func TestGoalLedgerRespectsEffectivePermission(t *testing.T) {
	// root 类型声明 write=none（只读）——生效权限没有 write 维度。
	rootType := agents.AgentType{ID: "root", CanSpawn: nil, Model: maRootModel, PermissionRequest: permRO()}
	// 策略**放行** ledger（allow 规则不能放宽声明的权限——正是要验的那条）。
	h := newMAHarness(t, rootType, 3, allowRule(loop.ToolLedger))
	h.prov.script(maRootModel,
		stepTools(call("led1", loop.ToolLedger, `{"goals":["g"]}`)),
		stepText("done"),
	)
	l, err := loop.New(loop.Config{
		Provider:   h.prov,
		Bus:        h.bus,
		Registry:   h.reg,
		Policy:     h.engine,
		Approver:   core.DenyAll{},
		SessionID:  h.session,
		AgentID:    h.rootID,
		Model:      maRootModel,
		System:     "root-system",
		MaxTurns:   4,
		Agents:     &loop.Agents{Tree: h.tree},
		Store:      h.store,
		GoalLedger: true,
	})
	if err != nil {
		t.Fatalf("loop.New: %v", err)
	}
	if _, err := l.Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 工具被生效权限挡住（failed + permission_denied），账本**没有**被写。
	failed := findToolFailedFor(t, h.events(), "led1")
	if failed.Error.Code == nil || *failed.Error.Code != loop.CodePermDenied {
		t.Fatalf("失败码 = %v, want %s（框架工具不得绕过 perm 校验）", failed.Error.Code, loop.CodePermDenied)
	}
	for _, ev := range h.events() {
		if ev.Kind == "ledger.updated" {
			t.Error("被 perm 挡下的 ledger 调用不应写账本")
		}
	}
}

// toolVisibleIn 报告某工具是否在可见集里。
func toolVisibleIn(tools []contracts.Tool, name string) bool {
	for _, tl := range tools {
		if tl.Name == name {
			return true
		}
	}
	return false
}
