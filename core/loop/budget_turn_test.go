package loop_test

// 缺口 5 第二件（A：逐轮预算制动）的 loop 侧钉子：Config.TurnBudget 钩子在每轮
// provider.Run **之前**调用；返回 error → 本轮不发起、Run 以"预算截断"收口
//（照 MaxTurns 截断的口径：Result.TruncatedByBudget + TruncationReason，Run 正常返回）。
//
// 口径（core/loop/loop.go）：
//   - nil 钩子 = 不查 = 零行为变化（默认路径）；
//   - 被拒的那一轮**什么都没跑**：没有 context.assembled、没有 model.turn、没有 turn.begin，
//     provider 零调用；已跑轮次的结果照常收口（Text / Events / ToolCalls 不丢）；
//   - 事件流形状合法：X1 三段写路径序列不变（writePathKinds 只看到已跑轮次的 intent/…）。

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core/loop"
)

// budgetErr 是测试钩子返回的错误（文案要能被 TruncationReason 带回给调用方）。
var budgetErr = errors.New("host: session X 已达成本预算上限（累计 600 tokens ≥ 上限 500）")

// TestTurnBudgetRejectFirstTurn（变异自证①a）：钩子第一轮就拒 → Run **正常返回**
// （不是 error），TruncatedByBudget=true + TruncationReason 带钩子文案；provider **零调用**；
// 事件流只有 user.prompt（这轮什么都没跑，X1 写路径为空）。
//
// 钉子：把轮间钩子的调用去掉（或改成不 return 截断）→ 本用例红。
func TestTurnBudgetRejectFirstTurn(t *testing.T) {
	h := newHarness(t)
	h.script(stepText("不该被调用"))

	calls := 0
	res, err := h.loop(func(c *loop.Config) {
		c.TurnBudget = func(context.Context) error {
			calls++
			return budgetErr
		}
	}).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("预算截断不该是 error（照 MaxTurns 口径：Result 收口）: %v", err)
	}
	if !res.TruncatedByBudget {
		t.Fatalf("TruncatedByBudget 应为 true: %+v", res)
	}
	if !strings.Contains(res.TruncationReason, "成本预算上限") {
		t.Errorf("TruncationReason 应带回钩子文案: %q", res.TruncationReason)
	}
	if res.Turns != 0 || res.ToolCalls != 0 || res.Events != 0 {
		t.Errorf("被拒的轮次什么都没跑: %+v", res)
	}
	if res.Text != "" {
		t.Errorf("没有跑过的 Run 不该有 Text: %q", res.Text)
	}
	if n := h.provider.callCount(); n != 0 {
		t.Errorf("provider 被调了 %d 次，期望 0（本轮不发起）", n)
	}
	if calls != 1 {
		t.Errorf("钩子被调 %d 次，期望 1（第一轮发起前查一次）", calls)
	}
	// 事件流：只有 user.prompt 旁路观测；X1 三段写路径为空（没跑 → 没有 intent/…）。
	if got, want := h.kinds(), []string{loop.KindUserPrompt}; !equalStrings(got, want) {
		t.Errorf("事件 kind 序 = %v，want %v", got, want)
	}
	if got := h.writePathKinds(); len(got) != 0 {
		t.Errorf("X1 写路径不该有事件: %v", got)
	}
}

// TestTurnBudgetTruncatesMidRun（变异自证①b）：第 1 轮跑完（工具调用收口）、第 2 轮
// 发起前被拒 → 已跑轮次的结果照常收口（Turns/Events/ToolCalls/Text），provider 只被
// 调过 1 次；事件流只有第 1 轮的写路径 + 观测，**没有**第 2 轮的 context.assembled/model.turn。
func TestTurnBudgetTruncatesMidRun(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)
	h.script(stepTools(call("c1", "echo", `{}`)), stepText("不该跑到这"))

	calls := 0
	res, err := h.loop(func(c *loop.Config) {
		c.TurnBudget = func(context.Context) error {
			calls++
			if calls >= 2 { // 第 1 轮放行、第 2 轮发起前拒绝
				return budgetErr
			}
			return nil
		}
	}).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("预算截断不该是 error: %v", err)
	}
	if !res.TruncatedByBudget {
		t.Fatalf("TruncatedByBudget 应为 true: %+v", res)
	}
	if !strings.Contains(res.TruncationReason, "500") {
		t.Errorf("TruncationReason 应带回钩子文案: %q", res.TruncationReason)
	}
	// 已跑轮次的结果照常收口（第 1 轮：一次工具调用）。
	if res.Turns != 1 || res.ToolCalls != 1 {
		t.Errorf("Result 不符（第 1 轮该收口）: %+v", res)
	}
	if n := h.provider.callCount(); n != 1 {
		t.Errorf("provider 被调 %d 次，期望 1（第 2 轮不发起）", n)
	}
	if n := exec.callN(); n != 1 {
		t.Errorf("执行器被调 %d 次，期望 1", n)
	}
	if calls != 2 {
		t.Errorf("钩子被调 %d 次，期望 2（每轮 provider.Run 前一次）", calls)
	}
	// 事件流形状：第 1 轮完整（intent → completed + 观测），没有第 2 轮的装配/响应观测。
	if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallCompleted}; !equalStrings(got, want) {
		t.Errorf("X1 写路径 kind 序 = %v，want %v", got, want)
	}
	var assembled, modelTurns int
	for _, k := range h.kinds() {
		switch k {
		case loop.KindContextAssembled:
			assembled++
		case loop.KindModelTurn:
			modelTurns++
		}
	}
	if assembled != 1 || modelTurns != 1 {
		t.Errorf("观测事件 assembled=%d model.turn=%d，都应为 1（被拒轮次不造观测）", assembled, modelTurns)
	}
}

// TestTurnBudgetNilHookZeroChange（酸测试）：不配钩子 → 行为与现状逐字节等价
// （正常收敛、TruncatedByBudget=false、TruncationReason 空、钩子调用计数恒 0）。
func TestTurnBudgetNilHookZeroChange(t *testing.T) {
	h := newHarness(t)
	h.script(stepText("收尾"))

	res, err := h.loop(nil).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.TruncatedByBudget || res.TruncationReason != "" {
		t.Errorf("nil 钩子不该截断: %+v", res)
	}
	if res.Text != "收尾" || res.Turns != 1 {
		t.Errorf("正常收敛被改动了: %+v", res)
	}
	if n := h.provider.callCount(); n != 1 {
		t.Errorf("provider 被调 %d 次，期望 1", n)
	}
}

// TestTurnBudgetErrorPreservesPriorText：被拒轮次的 Text 是**最后一轮**的文本
// （与 MaxTurns 截断同口径：Text = 最后一轮的文本，不被截断清空）。
func TestTurnBudgetErrorPreservesPriorText(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(stepTools(call("c1", "echo", `{}`)), stepText("第二轮文本"))

	calls := 0
	res, err := h.loop(func(c *loop.Config) {
		c.TurnBudget = func(context.Context) error {
			calls++
			if calls >= 2 {
				return budgetErr
			}
			return nil
		}
	}).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.TruncatedByBudget {
		t.Fatalf("应被截断: %+v", res)
	}
	// 第 1 轮的文本（工具调用轮的 Text）就是"最后一轮的文本"。
	if res.Text != "调用工具" {
		t.Errorf("Text = %q，want 最后一轮的文本 %q", res.Text, "调用工具")
	}
}
