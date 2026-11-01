package loop_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/compaction"
	"github.com/Yorha9e/alasol/core/durable"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/registry"
)

// 本文件钉缺口 3：注册表 State 门控（双层口径，与 runtime/mcpserver 的 F1 同款）——
//   - 第一层：visibleTools 过滤——State=disabled 的条目不进模型可见集（停用不暴露）；
//   - 第二层：执行前复查——resolveTool 拿到条目后 State=disabled → 既有失败路径
//     （intent + failed，code=tool_disabled），兜住"可见集刷新前的运行期停用窗口"
//     （模型幻觉 / Disable 发生在本轮可见集快照之后）。
//
// X1 三段写路径形状一字不动：拒绝路径仍是 intent 落盘 → failed 落盘、completed 不落、
// 执行器零调用；"未停用"路径零行为变化。

// TestDisabledToolNotVisibleToModel 钉第一层：Disable 后的注册表工具不进 ModelRequest.Tools
// （与 mcpserver tools/list 的"停用不暴露"同口径）。
func TestDisabledToolNotVisibleToModel(t *testing.T) {
	h := newHarness(t, allowRule("read"), allowRule("write"))
	h.addTool("read", nil)
	h.addTool("write", nil)
	if err := h.reg.Disable("write"); err != nil {
		t.Fatalf("Disable(write): %v", err)
	}
	h.script(stepText("ok"))

	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	reqs := h.provider.reqs()
	if len(reqs) == 0 {
		t.Fatal("应有模型请求")
	}
	names := make([]string, 0, len(reqs[0].Tools))
	for _, tl := range reqs[0].Tools {
		names = append(names, tl.Name)
	}
	if !equalStrings(names, []string{"read"}) {
		t.Fatalf("可见工具 = %v，want [read]（write 已停用必须移出可见集）", names)
	}
}

// TestDisabledToolCallWritesFailedAndNeverExecutes 钉第二层：绕过可见集直接调用被停用的
// 工具（模型幻觉 / "可见集刷新前的停用窗口"）→ X1 三段形状不变（intent 落、failed 落、
// completed 不落）、执行器零调用、失败码 tool_disabled 与 unknown 的 tool_not_found 区分。
func TestDisabledToolCallWritesFailedAndNeverExecutes(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)
	if err := h.reg.Disable("echo"); err != nil {
		t.Fatalf("Disable(echo): %v", err)
	}
	h.script(stepTools(call("c1", "echo", `{"text":"hi"}`)), stepText("收尾"))

	res, err := h.loop(nil).Run(context.Background(), "用一个已停用的工具")
	if err != nil {
		t.Fatalf("停用拒绝是普通失败，不该让 Run 失败（M1）: %v", err)
	}
	if res.ToolCalls != 1 || res.Events != 4 {
		t.Fatalf("Result 不符: %+v", res)
	}

	// X1 三段形状：intent + failed，completed 不落。
	if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallFailed}; !equalStrings(got, want) {
		t.Fatalf("事件 kind 序 = %v，want %v", got, want)
	}
	// 时间线：intent:c1 → failed:c1，且执行器从未被调用。
	if got := h.log.all(); len(got) != 2 || got[0] != "intent:c1" || got[1] != "failed:c1" {
		t.Fatalf("停用工具不得执行: %v", got)
	}
	if n := exec.callN(); n != 0 {
		t.Fatalf("停用工具的执行器被调用 %d 次（必须 0）", n)
	}

	fp := decodeFailed(t, writePath(h.events())[1])
	if string(fp.ToolCallID) != "c1" {
		t.Fatalf("failed.tool_call_id = %q", fp.ToolCallID)
	}
	// 文案点名"已停用（registry State=disabled）"。
	if !strings.Contains(fp.Error.Message, "已停用") || !strings.Contains(fp.Error.Message, "registry State=disabled") {
		t.Fatalf("失败说明应点名已停用（registry State=disabled）: %q", fp.Error.Message)
	}
	// 失败码与 unknown 工具区分（tool_disabled ≠ tool_not_found）。
	if fp.Error.Code == nil || *fp.Error.Code != loop.CodeToolDisabled {
		t.Fatalf("失败码 = %v，want %s", fp.Error.Code, loop.CodeToolDisabled)
	}
	// 失败说明回填给模型（M5）。
	reqs := h.provider.reqs()
	msgs := reqs[len(reqs)-1].Messages
	if msgs[len(msgs)-1].Role != "tool" || !strings.Contains(msgs[len(msgs)-1].Content, "已停用") {
		t.Fatalf("停用说明未回填: %+v", msgs[len(msgs)-1])
	}
}

// TestNotDisabledToolExecutesNormally 钉零行为变化：未 Disable 的注册表工具
// （State=registered 初始态 / enabled 都一样）照常可见、照常执行。
func TestNotDisabledToolExecutesNormally(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil) // 初始 State=registered（未 Enable 也照常可用）
	h.script(stepTools(call("c1", "echo", `{}`)), stepText("done"))

	res, err := h.loop(nil).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ToolCalls != 1 || res.Events != 4 {
		t.Fatalf("Result 不符: %+v", res)
	}
	if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallCompleted}; !equalStrings(got, want) {
		t.Fatalf("事件 kind 序 = %v，want %v（未停用照常执行）", got, want)
	}
	if n := exec.callN(); n != 1 {
		t.Fatalf("执行器应被调用 1 次，得到 %d", n)
	}
	// 可见集也不受影响。
	reqs := h.provider.reqs()
	if !hasTool(reqs[0].Tools, "echo") {
		t.Fatalf("未停用的 echo 应可见: %v", toolNames(reqs[0].Tools))
	}
}

// TestReEnabledToolVisibleAndExecutesAgain 钉 disable → enable 往返后两层门控都恢复：
// 可见集重新出现、直接调用照常执行（与 host 的 enable 端点同一条 registry.Enable 路径）。
func TestReEnabledToolVisibleAndExecutesAgain(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)
	if err := h.reg.Disable("echo"); err != nil {
		t.Fatalf("Disable(echo): %v", err)
	}
	if err := h.reg.Enable(context.Background(), "echo"); err != nil {
		t.Fatalf("Enable(echo): %v", err)
	}
	h.script(stepTools(call("c1", "echo", `{}`)), stepText("done"))

	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	reqs := h.provider.reqs()
	if !hasTool(reqs[0].Tools, "echo") {
		t.Fatalf("重新 Enable 后 echo 应回到可见集: %v", toolNames(reqs[0].Tools))
	}
	if n := exec.callN(); n != 1 {
		t.Fatalf("重新 Enable 后执行器应被调用 1 次，得到 %d", n)
	}
}

// ---------------------------------------------------------------------------
// 缺口 3 p3-1：停用工具**短路策略链**（不问人、不误归因 policy_deny）
// ---------------------------------------------------------------------------

// countingApprover 记录审批调用次数（钉"停用工具不进人审台"）。
type countingApprover struct {
	mu    sync.Mutex
	calls int
}

func (a *countingApprover) Approve(_ context.Context, _ contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	return contracts.ApprovalResponse{Decision: contracts.ApprovalDecisionAllow}, nil
}

func (a *countingApprover) callN() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

var _ core.Approver = (*countingApprover)(nil)

// TestDisabledToolSkipsPolicyChainUnderAskRule 钉 p3-1：ask 规则下调用**已停用**的工具 →
// 审批器**零调用**（停用的工具压根不可执行，不该把人叫到审批台前），失败码是
// tool_disabled（**不是** policy_deny——停用是注册表事实，不是策略判定结果）。
func TestDisabledToolSkipsPolicyChainUnderAskRule(t *testing.T) {
	h := newHarness(t, askRule("echo"))
	exec := h.addTool("echo", nil)
	if err := h.reg.Disable("echo"); err != nil {
		t.Fatalf("Disable(echo): %v", err)
	}
	appr := &countingApprover{}
	h.script(stepTools(call("c1", "echo", `{}`)), stepText("done"))

	if _, err := h.loop(func(c *loop.Config) { c.Approver = appr }).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 审批器零调用（ask 短路：停用工具不进人审）。
	if n := appr.callN(); n != 0 {
		t.Fatalf("停用工具不得进人审（ask 短路策略链），审批器被调用 %d 次", n)
	}
	if n := exec.callN(); n != 0 {
		t.Fatalf("停用工具执行器被调用 %d 次（必须 0）", n)
	}

	// 失败码：tool_disabled，**不是** policy_deny（不误归因）。
	fp := decodeFailed(t, writePath(h.events())[1])
	if fp.Error.Code == nil {
		t.Fatal("失败码不得为空")
	}
	if *fp.Error.Code != loop.CodeToolDisabled {
		t.Fatalf("失败码 = %q，want %q（停用不是 policy_deny：%q）",
			*fp.Error.Code, loop.CodeToolDisabled, loop.CodePolicyDeny)
	}

	// intent 的 PolicySnapshot：policy **未判定** → 零值快照（verdict 空串），
	// 不伪造 allow/deny（伪造会把"没判定"记成"判定过"）。
	intent := decodeIntent(t, writePath(h.events())[0])
	var snap struct {
		Verdict string `json:"verdict"`
	}
	if err := json.Unmarshal(intent.PolicySnapshot, &snap); err != nil {
		t.Fatalf("解 policy_snapshot: %v（raw=%s）", err, string(intent.PolicySnapshot))
	}
	if snap.Verdict != "" {
		t.Fatalf("停用工具短路策略链 → 快照应是零值（verdict 空串），got %q", snap.Verdict)
	}
}

// TestDisabledToolUnderAllowRuleStillToolDisabled 钉 p3-1 的另一半：allow 规则下调用
// 已停用的工具 → 同样是 tool_disabled（不是"放行后被执行"、也不是 policy_deny）。
func TestDisabledToolUnderAllowRuleStillToolDisabled(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)
	if err := h.reg.Disable("echo"); err != nil {
		t.Fatalf("Disable(echo): %v", err)
	}
	h.script(stepTools(call("c1", "echo", `{}`)), stepText("done"))

	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := exec.callN(); n != 0 {
		t.Fatalf("停用工具执行器被调用 %d 次（必须 0）", n)
	}
	fp := decodeFailed(t, writePath(h.events())[1])
	if fp.Error.Code == nil || *fp.Error.Code != loop.CodeToolDisabled {
		t.Fatalf("失败码 = %v，want %s", fp.Error.Code, loop.CodeToolDisabled)
	}
}

// ---------------------------------------------------------------------------
// 缺口 3 p3-5（跨面钉子，loop 侧）：经 **registry API** 停用 → 门控生效
//
// host 的 POST /api/extensions/{name}/disable 内部**就是** registry.Disable（同一入口、
// 同一实例——hub 把 h.reg 装配给 loop）→ 故这里用 registry.Disable 直调路径钉门控，
// host 面另有一条接线钉子（host/extensions_internal_test.go）钉"HTTP 端点改的就是
// loop 用的那个注册表实例"。
// ---------------------------------------------------------------------------

// TestDisableViaRegistryAPIGatesLoop 钉 p3-5（loop 侧）：**只经 registry API**
// （registry.Disable = HTTP 端点的同一入口）停用 → 两层门控都生效：可见集里没有它、
// 直调落 tool_disabled 且执行器零调用。
func TestDisableViaRegistryAPIGatesLoop(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)

	// 唯一动作：经注册表 API 停用（与 HTTP 端点同一个入口）。
	if err := h.reg.Disable("echo"); err != nil {
		t.Fatalf("registry.Disable(echo): %v", err)
	}
	h.script(stepTools(call("c1", "echo", `{}`)), stepText("done"))

	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 第一层：可见集里没有它。
	reqs := h.provider.reqs()
	if hasTool(reqs[0].Tools, "echo") {
		t.Fatalf("停用工具不得进可见集: %v", toolNames(reqs[0].Tools))
	}
	// 第二层：直调落 tool_disabled，执行器零调用。
	fp := decodeFailed(t, writePath(h.events())[1])
	if fp.Error.Code == nil || *fp.Error.Code != loop.CodeToolDisabled {
		t.Fatalf("失败码 = %v，want %s", fp.Error.Code, loop.CodeToolDisabled)
	}
	if n := exec.callN(); n != 0 {
		t.Fatalf("停用工具执行器被调用 %d 次（必须 0）", n)
	}
}

// ---------------------------------------------------------------------------
// 缺口 3 F2：执行前**再查一次** State（ask 人审等待期间被停用 → 放行后不得执行）
// ---------------------------------------------------------------------------

// disablingApprover 在**人审应答前**把工具停用（模拟"审批等待期间运维停用了该工具"）。
type disablingApprover struct {
	reg  *registry.Registry
	name string

	mu    sync.Mutex
	calls int
}

func (a *disablingApprover) Approve(_ context.Context, _ contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	if err := a.reg.Disable(a.name); err != nil {
		return contracts.ApprovalResponse{}, err
	}
	return contracts.ApprovalResponse{Decision: contracts.ApprovalDecisionAllow}, nil
}

func (a *disablingApprover) callN() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

var _ core.Approver = (*disablingApprover)(nil)

// TestDisabledDuringApprovalIsNotExecuted 钉 F2：ask 规则 → 人审等待期间该工具被停用 →
// 审批放行后**执行器零调用** + 失败码 tool_disabled（文案点名"执行前复查发现已停用"）。
//
// 为什么必须钉：State 只在 resolveTool 读**一次**（判定时刻的快照）时，ask 的等待窗口
// 里停用**不生效**——审批放行后工具照常执行（探针：执行器被调 1 次）→ "停用"又是个
// 半旋钮。执行前必须**再读一次**注册表（与 runtime/mcpserver 的 checkUsable 时点对齐）。
func TestDisabledDuringApprovalIsNotExecuted(t *testing.T) {
	h := newHarness(t, askRule("echo"))
	exec := h.addTool("echo", nil)
	appr := &disablingApprover{reg: h.reg, name: "echo"}
	h.script(stepTools(call("c1", "echo", `{}`)), stepText("done"))

	if _, err := h.loop(func(c *loop.Config) { c.Approver = appr }).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 人审确实发生过（调用开始时工具**还没**被停用 → 走 ask 链路）。
	if n := appr.callN(); n != 1 {
		t.Fatalf("审批器应被调用 1 次（调用开始时工具尚未停用 → 走人审），got %d", n)
	}
	// 关键：放行后**不得执行**（停用生效）。
	if n := exec.callN(); n != 0 {
		t.Fatalf("审批期间被停用的工具不得执行（执行器被调用 %d 次，必须 0）", n)
	}
	// 失败码 tool_disabled + 文案点名"执行前复查"。
	fp := decodeFailed(t, writePath(h.events())[1])
	if fp.Error.Code == nil || *fp.Error.Code != loop.CodeToolDisabled {
		t.Fatalf("失败码 = %v，want %s", fp.Error.Code, loop.CodeToolDisabled)
	}
	if !strings.Contains(fp.Error.Message, "执行前复查发现已停用") {
		t.Fatalf("失败文案应点名「执行前复查发现已停用」: %q", fp.Error.Message)
	}
	// X1 三段形状：intent + failed，completed 不落。
	if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallFailed}; !equalStrings(got, want) {
		t.Fatalf("事件 kind 序 = %v，want %v", got, want)
	}
}

// ---------------------------------------------------------------------------
// 缺口 3 p3-6：重放（Replay）路径**也**查注册表 State
// ---------------------------------------------------------------------------

// TestReplayDisabledToolFailsWithoutExecuting 钉 p3-6：重放一个"工具后来被停用"的
// in-flight 操作 → 重放**不执行**（落 failed，code=tool_disabled），执行器零调用。
//
// 为什么：重放**不重跑策略链**（T0 已判定放行），但"停用"是**装配事实**不是策略判定——
// 两次进程之间工具被运维停用了就不该再执行（否则"停用"在崩溃续跑路径上失效）。
func TestReplayDisabledToolFailsWithoutExecuting(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)
	if err := h.reg.Disable("echo"); err != nil {
		t.Fatalf("Disable(echo): %v", err)
	}

	op := durable.Op{
		Intent: contracts.ToolCallIntentPayload{
			ToolCallID: "tc_DISABLED_REPLAY",
			ToolName:   "echo",
			Arguments:  json.RawMessage(`{"text":"hi"}`),
		},
		IntentEventID: core.NewUlid(),
		SessionID:     h.session,
		AgentID:       h.agent,
	}
	res, err := h.loop(nil).Replay(context.Background(), op)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.OK {
		t.Fatalf("停用工具的重放必须失败（不执行），got %+v", res)
	}
	if res.FailureCode != loop.CodeToolDisabled {
		t.Fatalf("重放失败码 = %q，want %q", res.FailureCode, loop.CodeToolDisabled)
	}
	if n := exec.callN(); n != 0 {
		t.Fatalf("停用工具的重放不得执行（执行器被调用 %d 次）", n)
	}
	// 重放落的是 failed（不是 completed）。
	failedEv := findEvent(t, h.events(), loop.KindToolCallFailed, 0)
	fp := decodeFailed(t, failedEv)
	if fp.Error.Code == nil || *fp.Error.Code != loop.CodeToolDisabled {
		t.Fatalf("重放 failed 的失败码 = %v，want %s", fp.Error.Code, loop.CodeToolDisabled)
	}
	if got := h.kinds(); len(got) != 1 || got[0] != loop.KindToolCallFailed {
		t.Fatalf("重放只应落一条 failed（没执行 → 无 completed），got %v", got)
	}
}

// ---------------------------------------------------------------------------
// 策略框架工具（recall / stage 那一类）不经注册表 → 不受 State 门控影响
// ---------------------------------------------------------------------------

// toolAsmFake 是最小的 ToolProvider 装配器替身：透传 history + 注入一个框架工具。
type toolAsmFake struct {
	ft compaction.FrameworkTool
}

func (a *toolAsmFake) Assemble(_ context.Context, history []core.Message, _ int) (compaction.Assembled, error) {
	return compaction.Assembled{Messages: history}, nil
}

func (a *toolAsmFake) Tools() []compaction.FrameworkTool { return []compaction.FrameworkTool{a.ft} }

// TestFrameworkToolUnaffectedByRegistryState 钉：策略框架工具不经注册表（没有 State），
// 注册表的 enable/disable 不影响它的可见性与执行——State 门控只作用于注册表条目。
func TestFrameworkToolUnaffectedByRegistryState(t *testing.T) {
	h := newHarness(t, allowRule("recall_fake"))
	fexec := &mockExecutor{name: "recall_fake", log: h.log}
	asm := &toolAsmFake{ft: compaction.FrameworkTool{
		Desc: contracts.Tool{Name: "recall_fake", SideEffect: contracts.SideEffectNone},
		Exec: fexec,
	}}
	h.script(stepTools(call("c1", "recall_fake", `{}`)), stepText("done"))

	l := h.loop(func(c *loop.Config) {
		c.ContextStrategy = func() compaction.Assembler { return asm }
	})
	if _, err := l.Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	reqs := h.provider.reqs()
	if !hasTool(reqs[0].Tools, "recall_fake") {
		t.Fatalf("框架工具应可见（不经注册表，无 State 可言）: %v", toolNames(reqs[0].Tools))
	}
	if n := fexec.callN(); n != 1 {
		t.Fatalf("框架工具应照常执行 1 次，得到 %d", n)
	}
	if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallCompleted}; !equalStrings(got, want) {
		t.Fatalf("事件 kind 序 = %v，want %v", got, want)
	}
}
