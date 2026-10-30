// ctx 取消/超时后的**执行护栏**与 **deny 文案归因**（快修 critic F1/F2）的钉子。
//
// 背景（审批超时审计修复引入的两个回归）：
//
//   - F1：审计事件改走 settle ctx（WithoutCancel + 5s/次，events.go publish）后，
//     "ctx 已死 ⇒ intent 落盘失败 ⇒ 绝不执行"这条**隐式**护栏失效——ctx 死了 intent
//     照样落得下，于是"取消/超时之后仍有副作用"。护栏必须**显式**：执行前查 ctx.Err()，
//     ctx 已死就绝不执行（落 failed，如实写"因 Run 上下文取消/超时未执行"）。
//   - F2：deny 的文案要能分辨"**人审明确拒绝**"与"**没人来得及答**"；判据取**判定里
//     记下的成因**（dec.Reason），不取事后读到的 ctx.Err() 快照（人拒了但 ctx 恰好
//     到期 = 判据竞态，按快照归因会把"被拒绝"误记成"没人来得及答"）。
package loop_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/loop"
)

// noAnswerMarker 是 deny 文案里"没人应答/超时"前缀的**标记位**（loop 包内
// denyTimeoutPrefix 的一部分）：在 = 归因成"没人来得及答"；不在 = 归因成"人审明确拒绝"。
//
// 用它断言而不是断言整条文案：文案可以改措辞，但**归因**（在/不在）不能变。
const noAnswerMarker = "非人审明确拒绝"

// cancelOnReturnProvider 在**返回工具调用那一刻**取消 Run ctx：
// 模拟"模型把工具调用交回来了，但 Run 的 ctx 恰好在这一刻死了"（取消/超时与 provider
// 返回撞在一起）。provider 本身不报错——它确实返回了工具调用（ctx 死后 loop 仍会处理它）。
type cancelOnReturnProvider struct {
	cancel context.CancelFunc
	calls  int
}

func (p *cancelOnReturnProvider) Run(_ context.Context, _ core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if p.calls == 1 {
		p.cancel() // ctx 在工具调用交付前就死了
		return core.ModelResult{
			Text: "调用工具",
			ToolCalls: []core.ToolCallReq{{
				ID:   contracts.ProviderId("c1"),
				Name: "bash",
				Args: json.RawMessage(`{"cmd":"ls"}`),
			}},
		}, nil
	}
	return core.ModelResult{Text: "收尾"}, nil
}

var _ core.Provider = (*cancelOnReturnProvider)(nil)

// TestCancelAfterProviderToolCallNeverExecutes（F1 钉子）：provider 返回工具调用时 ctx
// 已死（取消与返回撞在一起）→
//
//	① 执行器**零调用**（ctx 已死 = 这一轮已被取消/超时，不许再有副作用）；
//	② 写路径仍留痕：intent + failed 一对（审计必须留痕），**没有** completed；
//	③ failed 文案写明"因 Run 上下文取消/超时未执行"（审计要能分辨"没执行"与"执行失败"）。
//
// 变异自证：去掉执行前的 ctx.Err() 检查 → ①/② 全红（执行器被调 1 次 + completed 出现）。
func TestCancelAfterProviderToolCallNeverExecutes(t *testing.T) {
	h := newHarness(t, allowRule("bash"))
	// 尊重 ctx 的 store（对齐 sqlite 实测行为）：ctx 死了 intent 只能靠 settle ctx 落盘——
	// 顺带钉住"审计留痕"这条纪律（settle ctx 退回去 → ② 全红）。
	h.bus = bus.New(&ctxAwareStore{memStore: h.store})
	exec := h.addTool("bash", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prov := &cancelOnReturnProvider{cancel: cancel}

	res, err := h.loop(func(c *loop.Config) { c.Provider = prov }).Run(ctx, "跑一下")

	// ctx 已死 → Run 按取消语义收场（不是写路径断裂）。
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v，want context.Canceled（ctx 已死 = 取消收场）", err)
	}
	if strings.Contains(err.Error(), "落盘失败") {
		t.Fatalf("Run 收场错误不该是写路径断裂（intent 落盘失败）: %v", err)
	}

	// ① 护栏：ctx 已死 = 不许再有副作用。
	if n := exec.callN(); n != 0 {
		t.Fatalf("ctx 已死后执行器被调用 %d 次（必须 0：取消/超时后不许再有副作用）", n)
	}

	// ② 审计留痕：intent + failed 一对，**无** completed。
	if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallFailed}; !equalStrings(got, want) {
		t.Fatalf("写路径 kind 序 = %v，want %v（ctx 已死也要留痕：intent+failed，无 completed）", got, want)
	}
	if got, want := h.log.all(), []string{"intent:c1", "failed:c1"}; !equalStrings(got, want) {
		t.Fatalf("写路径时间线 = %v，want %v（不许出现 exec/completed）", got, want)
	}
	if res.ToolCalls != 1 || res.Events != 3 {
		// 3 = 本轮 context.assembled + intent + failed（user.prompt/model.turn 是旁路观测，不计数）。
		t.Fatalf("Result 不符: %+v（want ToolCalls=1 Events=3）", res)
	}

	// ③ 文案：如实写"因 Run 上下文取消/超时未执行"（不是"执行失败"）。
	evs := writePath(h.events())
	msg := decodeFailed(t, evs[1]).Error.Message
	if !strings.Contains(msg, "因 Run 上下文取消/超时未执行") {
		t.Fatalf("failed 文案应写明「因 Run 上下文取消/超时未执行」: %q", msg)
	}
	if !strings.Contains(msg, "未执行") {
		t.Fatalf("failed 文案应写明未执行（ctx 取消/超时）: %q", msg)
	}
	if !strings.Contains(msg, "取消") && !strings.Contains(msg, "超时") {
		t.Fatalf("failed 文案应写明是 ctx 取消/超时导致未执行: %q", msg)
	}
}

// denyApprover 是"**人明确拒绝**"的审批人：返回 deny + 人类备注。
//
// cancel 非 nil 时在应答前取消 ctx——模拟"人拒了，但 Run 的 ctx 恰好同时到期"
// （判据竞态：成因是人拒了，ctx 的死活是另一个瞬间的事实）。
type denyApprover struct {
	note   string
	cancel context.CancelFunc
}

func (a denyApprover) Approve(_ context.Context, _ contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	if a.cancel != nil {
		a.cancel()
	}
	note := a.note
	return contracts.ApprovalResponse{Decision: contracts.ApprovalDecisionDeny, Note: &note}, nil
}

var _ core.Approver = denyApprover{}

// TestDenyWordingNoTimeoutPrefixForHumanRejection（F2 负向钉子）：ctx 一直活着、人明确
// 点了 deny → 失败文案**不含**"没人应答/超时"前缀（那是"被拒绝"，不是"没人来得及答"）。
//
// 变异自证：把前缀改成**无条件加** → 本用例红（人审拒绝被误记成超时）。
func TestDenyWordingNoTimeoutPrefixForHumanRejection(t *testing.T) {
	h := newHarness(t, askRule("bash"))
	exec := h.addTool("bash", nil)
	h.script(stepTools(call("c1", "bash", `{"cmd":"ls"}`)), stepText("收尾"))

	res, err := h.loop(func(c *loop.Config) {
		c.Approver = denyApprover{note: "这活儿今天不干"}
	}).Run(context.Background(), "跑一下")
	if err != nil {
		t.Fatalf("Run err = %v（人审拒绝是普通失败，不该让 Run 失败）", err)
	}
	if n := exec.callN(); n != 0 {
		t.Fatalf("人审拒绝后执行器被调用 %d 次（必须 0）", n)
	}
	if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallFailed}; !equalStrings(got, want) {
		t.Fatalf("写路径 kind 序 = %v，want %v", got, want)
	}

	msg := decodeFailed(t, writePath(h.events())[1]).Error.Message
	if strings.Contains(msg, noAnswerMarker) {
		t.Fatalf("人审明确拒绝不该带超时/无人应答前缀（成因是「被拒绝」）: %q", msg)
	}
	if !strings.Contains(msg, "人审未放行") {
		t.Fatalf("failed 文案应保留人审拒绝的成因（人审未放行）: %q", msg)
	}
	if res.ToolCalls != 1 {
		t.Fatalf("Result.ToolCalls = %d，want 1", res.ToolCalls)
	}
}

// TestDenyWordingUsesDecisionCauseNotCtxSnapshot（F2 判据竞态钉子）：人明确拒绝，但
// **同一瞬间** Run ctx 也到期了 —— 成因是"人拒了"，文案**仍不该**带超时/无人应答前缀。
//
// 判据必须取判定里记下的成因（dec.Reason），不能取事后读到的 ctx.Err() 快照
// （快照是另一个瞬间：按它归因 = 把"被拒绝"误记成"没人来得及答"）。
//
// 变异自证：把判据改回 ctx.Err() 快照 → 本用例红（前缀被误加）。
func TestDenyWordingUsesDecisionCauseNotCtxSnapshot(t *testing.T) {
	h := newHarness(t, askRule("bash"))
	h.bus = bus.New(&ctxAwareStore{memStore: h.store})
	exec := h.addTool("bash", nil)
	h.script(stepTools(call("c1", "bash", `{"cmd":"ls"}`)), stepText("收尾"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := h.loop(func(c *loop.Config) {
		c.Approver = denyApprover{note: "这活儿今天不干", cancel: cancel}
	}).Run(ctx, "跑一下")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v，want context.Canceled（ctx 已死 = 取消收场）", err)
	}
	if n := exec.callN(); n != 0 {
		t.Fatalf("人审拒绝后执行器被调用 %d 次（必须 0）", n)
	}
	// 审计留痕：ctx 已死，intent/failed 仍要落盘（settle ctx）。
	if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallFailed}; !equalStrings(got, want) {
		t.Fatalf("写路径 kind 序 = %v，want %v（ctx 已死也要留痕）", got, want)
	}

	msg := decodeFailed(t, writePath(h.events())[1]).Error.Message
	if strings.Contains(msg, noAnswerMarker) {
		t.Fatalf("人审明确拒绝（ctx 恰好同时到期）不该带超时/无人应答前缀: %q", msg)
	}
	if !strings.Contains(msg, "人审未放行") {
		t.Fatalf("failed 文案应保留人审拒绝的成因（人审未放行）: %q", msg)
	}
}
