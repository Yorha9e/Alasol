package loop_test

// G-e「config.tools 工具白名单」的 core/loop 验收面（设计 15 §3.5 白名单语义）：
//
//	① **可见集**（模型看到什么）：only / except / 两者同时 / 空 only（一个都不暴露）/
//	   清除回默认（全部已注册可见）——都只改**模型可见集**；
//	② **两层白名单相交**：session 白名单与 agent 类型白名单（W5）**互不突破**——
//	   类型只许 [read]、session only=[bash] → 可见集为空（不是并集）；
//	③ **可见 ≠ 可执行**：白名单只管可见性，执行许可链（policy/perm）一字不改——
//	   被 except 藏起来的工具，模型若仍发起调用，执行侧的判定与没有白名单时**完全一样**。

import (
	"context"
	"testing"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/loop"
)

// visibleNames 跑一轮并返回模型可见的工具名（模型看到的全部工具）。
func visibleNames(t *testing.T, h *harness, vis loop.ToolVisibility) []string {
	t.Helper()
	h.script(stepText("ok"))
	if _, err := h.loop(func(c *loop.Config) {
		c.RunOverrides = func(context.Context) (loop.RunOverrides, error) {
			return loop.RunOverrides{Tools: vis}, nil
		}
	}).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	reqs := h.provider.reqs()
	if len(reqs) == 0 {
		t.Fatal("应有模型请求")
	}
	return toolNames(reqs[0].Tools)
}

// TestSessionToolVisibilityOnly：only = 白名单（可见集 = only ∩ 已注册）。
func TestSessionToolVisibilityOnly(t *testing.T) {
	h := newHarness(t, allowRule("read"), allowRule("write"), allowRule("bash"))
	h.addTool("read", nil)
	h.addTool("write", nil)
	h.addTool("bash", nil)

	got := visibleNames(t, h, loop.ToolVisibility{Only: []string{"read"}})
	if !equalStrings(got, []string{"read"}) {
		t.Fatalf("可见集 = %v，want [read]（only = 白名单）", got)
	}

	// only 里有未注册的工具名 = 该工具不存在（白名单语义天然容错，不报错）。
	got = visibleNames(t, h, loop.ToolVisibility{Only: []string{"read", "not_registered"}})
	if !equalStrings(got, []string{"read"}) {
		t.Fatalf("可见集 = %v，want [read]（点名未注册的工具 = 该工具不存在）", got)
	}
}

// TestSessionToolVisibilityExcept：except = 从可见集里减去。
func TestSessionToolVisibilityExcept(t *testing.T) {
	h := newHarness(t, allowRule("read"), allowRule("write"), allowRule("bash"))
	h.addTool("read", nil)
	h.addTool("write", nil)
	h.addTool("bash", nil)

	got := visibleNames(t, h, loop.ToolVisibility{Except: []string{"write"}})
	if !equalStrings(got, []string{"read", "bash"}) {
		t.Fatalf("可见集 = %v，want [read bash]（except 减掉 write）", got)
	}
}

// TestSessionToolVisibilityBoth：only + except 同时给（only ∩ 已注册 - except）。
func TestSessionToolVisibilityBoth(t *testing.T) {
	h := newHarness(t, allowRule("read"), allowRule("write"), allowRule("bash"))
	h.addTool("read", nil)
	h.addTool("write", nil)
	h.addTool("bash", nil)

	got := visibleNames(t, h, loop.ToolVisibility{
		Only:   []string{"read", "write"},
		Except: []string{"write"},
	})
	if !equalStrings(got, []string{"read"}) {
		t.Fatalf("可见集 = %v，want [read]（only ∩ 已注册 - except）", got)
	}
}

// TestSessionToolVisibilityEmptyOnlyHidesAll：only = **空数组** = 一个工具都不暴露
// （空数组 ≠ 缺失：缺失 = 不限）。
func TestSessionToolVisibilityEmptyOnlyHidesAll(t *testing.T) {
	h := newHarness(t, allowRule("read"), allowRule("write"))
	h.addTool("read", nil)
	h.addTool("write", nil)

	got := visibleNames(t, h, loop.ToolVisibility{Only: []string{}})
	if len(got) != 0 {
		t.Fatalf("可见集 = %v，want 空（only 空数组 = 一个工具都不暴露）", got)
	}
}

// TestSessionToolVisibilityClearedIsDefault：清除回默认（零值白名单）= 全部已注册工具
// 可见（仍受策略链与类型白名单管辖）。
func TestSessionToolVisibilityClearedIsDefault(t *testing.T) {
	h := newHarness(t, allowRule("read"), allowRule("write"))
	h.addTool("read", nil)
	h.addTool("write", nil)

	got := visibleNames(t, h, loop.ToolVisibility{})
	if !equalStrings(got, []string{"read", "write"}) {
		t.Fatalf("可见集 = %v，want [read write]（清除回默认 = 全部已注册可见）", got)
	}
}

// TestSessionToolVisibilityIntersectsTypeWhitelist：两层白名单**相交**，互不突破——
//
//	agent 类型白名单（W5）只许 [read]；session 白名单 only=[bash]
//	→ 可见集 = （已注册 ∩ 类型白名单）∩ session 白名单 = **空**（不是并集）。
func TestSessionToolVisibilityIntersectsTypeWhitelist(t *testing.T) {
	rootType := agents.AgentType{
		ID:                "root",
		Tools:             []string{"read"}, // 类型白名单：只许 read
		PermissionRequest: permFull(),
	}
	h := newMAHarness(t, rootType, 3, allowRule("read"), allowRule("bash"))
	h.addTool("read", nil)
	h.addTool("bash", nil)
	h.prov.script(maRootModel, stepText("ok"), stepText("ok")) // 两轮 Run 各一步

	newLoopWith := func(vis loop.ToolVisibility) *loop.Loop {
		t.Helper()
		l, err := loop.New(loop.Config{
			Provider:  h.prov,
			Bus:       h.bus,
			Registry:  h.reg,
			Policy:    h.engine,
			Approver:  core.DenyAll{},
			SessionID: h.session,
			AgentID:   h.tree.Root(),
			Model:     maRootModel,
			MaxTurns:  5,
			Agents:    &loop.Agents{Tree: h.tree, Verifier: h.verifier},
			RunOverrides: func(context.Context) (loop.RunOverrides, error) {
				return loop.RunOverrides{Tools: vis}, nil
			},
		})
		if err != nil {
			t.Fatalf("loop.New: %v", err)
		}
		return l
	}

	// 基线：只有类型白名单时，可见集 = [read]（bash 被类型白名单挡住）。
	if _, err := newLoopWith(loop.ToolVisibility{}).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	reqs := h.prov.reqsFor(maRootModel)
	if len(reqs) == 0 {
		t.Fatal("应有模型请求")
	}
	base := toolNames(reqs[0].Tools)
	if !equalStrings(base, []string{"read"}) {
		t.Fatalf("基线可见集 = %v，want [read]（类型白名单只许 read）", base)
	}

	// 再叠 session 白名单 only=[bash]：与类型白名单**不相交** → 可见集为空
	// （session 白名单**不能**把类型白名单挡掉的 bash 放进来）。
	if _, err := newLoopWith(loop.ToolVisibility{Only: []string{"bash"}}).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	reqs = h.prov.reqsFor(maRootModel)
	if len(reqs) < 2 {
		t.Fatalf("应有两次模型请求，得到 %d", len(reqs))
	}
	got := toolNames(reqs[1].Tools)
	if len(got) != 0 {
		t.Fatalf("可见集 = %v，want 空（类型白名单 [read] ∩ session only [bash] = 空，两层相交不突破）", got)
	}
}

// TestSessionToolVisibilityDoesNotChangeExecution：**可见 ≠ 可执行**——白名单只改可见集，
// 执行许可链（policy/perm）一字不改：被 except 藏起来的工具，模型若仍发起调用，
// 执行侧照旧按策略判定（allow → 照常执行；执行器被调用、X1 三段路径照旧）。
func TestSessionToolVisibilityDoesNotChangeExecution(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)
	h.script(stepTools(call("c1", "echo", `{}`)), stepText("done"))

	// 白名单把 echo 藏起来（模型看不到它）——但执行侧的许可链不变。
	l := h.loop(func(c *loop.Config) {
		c.RunOverrides = func(context.Context) (loop.RunOverrides, error) {
			return loop.RunOverrides{Tools: loop.ToolVisibility{Except: []string{"echo"}}}, nil
		}
	})
	res, err := l.Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 可见集里没有 echo（模型看不到）。
	reqs := h.provider.reqs()
	if len(reqs) == 0 {
		t.Fatal("应有模型请求")
	}
	if hasTool(reqs[0].Tools, "echo") {
		t.Fatalf("except 之后 echo 不该可见: %v", toolNames(reqs[0].Tools))
	}
	// 执行侧照旧：策略 allow → 执行器被调用、X1 三段路径照走（intent + completed）。
	if res.ToolCalls != 1 {
		t.Fatalf("ToolCalls = %d，want 1（执行许可链不受白名单影响）", res.ToolCalls)
	}
	if n := exec.callN(); n != 1 {
		t.Fatalf("执行器被调用 %d 次，want 1（可见 ≠ 可执行：执行侧判定照旧）", n)
	}
	if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallCompleted}; !equalStrings(got, want) {
		t.Fatalf("事件 kind 序 = %v，want %v（X1 三段路径零改动）", got, want)
	}
}
