// 审批超时审计断链的钉子（联调实测复盘）：
//
// policy.Decide 在 ask 上阻塞等审批（受 Run ctx 约束，PromptRunTimeoutSec 兜底），
// 审批一直等到 Run ctx 超时才 fail-close deny 收场 → 此后若拿**已过期的 ctx** 发
// tool.call.intent，store 会以 "begin: context deadline exceeded" 拒绝 → 事件流里
// 这次工具调用**零留痕**（intent/failed 整链断裂），Run 以"intent 落盘失败"收场。
//
// 修复：审计事件经 settle ctx（WithoutCancel + 5s，见 events.go publish）落盘；
// 执行仍严格尊重原 ctx（deny 路径不执行）。
package loop_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/loop"
)

// ctxAwareStore 包装 memStore：Append **尊重 ctx**（已取消/超时即拒绝入库）——
// 对齐真实 sqlite store 的行为（BeginTx 检查 ctx，联调实测报 "sqlite: begin:
// context deadline exceeded"）。memStore 忽略 ctx，不套这层就复现不了断链，
// 钉子也钉不住变异（settle ctx 退回原 ctx 后必须变红）。
type ctxAwareStore struct {
	*memStore
}

func (s *ctxAwareStore) Append(ctx context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("store: begin: %w", err)
	}
	return s.memStore.Append(ctx, evs)
}

var _ core.Store = (*ctxAwareStore)(nil)

// blockUntilCtxApprover 是阻塞到 ctx 结束才收场的审批人（模拟"人一直没来"）：
// 与 host 的 protocolApprover 无人应答路径同形——ctx.Done 后返回 ctx.Err()，
// policy.Decide 按 fail-close deny 收场。
type blockUntilCtxApprover struct{}

func (blockUntilCtxApprover) Approve(ctx context.Context, _ contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	<-ctx.Done()
	return contracts.ApprovalResponse{}, ctx.Err()
}

var _ core.Approver = blockUntilCtxApprover{}

// TestApprovalWaitUntilRunCtxTimeoutLeavesAuditTrail（钉子）：审批一直等到 Run ctx
// 超时才 fail-close deny 收场时——
//
//	① run 不 panic 不挂死（按取消语义收场，err = context.DeadlineExceeded）；
//	② 事件流里**有** tool.call.intent + tool.call.failed（reason 含超时/未放行语义），
//	   且无 tool.call.completed（deny 不执行）；
//	③ 失败文案不是"intent 落盘失败"（写路径没断，是策略 fail-close 的正常 deny）。
//
// 变异自证：把 events.go publish 里的 settle ctx 退回原 ctx → intent 落盘被
// ctx-aware store 拒掉 → 本用例 ②③ 与 Events 计数全红。
func TestApprovalWaitUntilRunCtxTimeoutLeavesAuditTrail(t *testing.T) {
	h := newHarness(t, askRule("bash"))
	// 换成尊重 ctx 的 store（对齐 sqlite 实测行为）——否则旧 bug 在 memStore 下复现不了。
	h.bus = bus.New(&ctxAwareStore{memStore: h.store})
	exec := h.addTool("bash", nil)
	// 第二轮的收尾文本用不到：ctx 会在审批等待中耗尽，turn 2 在 provider 调用前就中止。
	h.script(stepTools(call("c1", "bash", `{"cmd":"ls"}`)), stepText("收尾"))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	type runOut struct {
		res loop.Result
		err error
	}
	done := make(chan runOut, 1)
	go func() {
		res, err := h.loop(func(c *loop.Config) { c.Approver = blockUntilCtxApprover{} }).Run(ctx, "p")
		done <- runOut{res: res, err: err}
	}()

	// ① 不挂死：审批等到 ctx 超时后必须收场（挂死 = 这里超时红）。
	var out runOut
	select {
	case out = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Run 挂死：审批等待 Run ctx 超时后没有收场")
	}

	// ① 按取消语义收场，而不是写路径断裂（"intent 落盘失败"）。
	if !errors.Is(out.err, context.DeadlineExceeded) {
		t.Fatalf("err = %v，want context.DeadlineExceeded", out.err)
	}
	if strings.Contains(out.err.Error(), "落盘失败") {
		t.Fatalf("Run 收场错误不该是写路径断裂（intent 落盘失败）: %v", out.err)
	}

	// ② 事件流：恰好 intent + failed 一对（审计留痕），顺序不倒。
	if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallFailed}; !equalStrings(got, want) {
		t.Fatalf("事件 kind 序 = %v，want %v（审批等到 ctx 超时也必须留下 intent+failed）", got, want)
	}
	// X1 守约：fail-close deny **不执行**——settle ctx 只供发布，绝不把执行放行。
	if n := exec.callN(); n != 0 {
		t.Fatalf("审批超时 fail-close 后执行器被调用 %d 次（必须 0）", n)
	}
	if out.res.ToolCalls != 1 || out.res.Events != 3 {
		// 3 = 本轮 context.assembled + intent + failed（user.prompt/model.turn 是旁路观测，不计数）。
		t.Fatalf("Result 不符: %+v（want ToolCalls=1 Events=3）", out.res)
	}

	evs := writePath(h.events())
	// intent 载荷完好：快照记的是 deny（判定时刻可审计），因果指针把 failed 链回 intent。
	if snap := decodeIntent(t, evs[0]).PolicySnapshot; !strings.Contains(string(snap), "deny") {
		t.Fatalf("intent 的 policy_snapshot 应记 deny: %s", snap)
	}
	if evs[1].CausationID == nil || *evs[1].CausationID != evs[0].ID {
		t.Fatalf("failed.CausationID 应指向 intent：%v", evs[1].CausationID)
	}

	// ③ 失败文案：区分"等待审批时 Run 超时（fail-close deny）"与"人审拒绝"，
	//    且绝不能是"intent 落盘失败"。
	fp := decodeFailed(t, evs[1])
	if string(fp.ToolCallID) != "c1" {
		t.Fatalf("failed.tool_call_id = %q", fp.ToolCallID)
	}
	msg := fp.Error.Message
	if !strings.Contains(msg, "超时") {
		t.Fatalf("失败文案应含超时语义（区分人审拒绝）: %q", msg)
	}
	if !strings.Contains(msg, "未放行") && !strings.Contains(msg, "未执行") {
		t.Fatalf("失败文案应含未放行/未执行语义: %q", msg)
	}
	// 归因：审批失败（审批人等到 ctx 结束才收场）= "**没人来得及答**"，文案必须带
	// "非人审明确拒绝"标记位（与"人审明确拒绝"区分——见 ctx_guard_test.go 的负向钉子）。
	if !strings.Contains(msg, "非人审明确拒绝") {
		t.Fatalf("审批等不到人应答的失败文案应带超时/无人应答前缀: %q", msg)
	}
	if strings.Contains(msg, "落盘失败") {
		t.Fatalf("失败文案不该是写路径断裂（intent 落盘失败）: %q", msg)
	}
	if fp.Error.Code == nil || *fp.Error.Code != loop.CodePolicyDeny {
		t.Fatalf("失败码 = %v, want %s", fp.Error.Code, loop.CodePolicyDeny)
	}

	// 写路径时间线：intent → failed，中间没有执行。
	if got, want := h.log.all(), []string{"intent:c1", "failed:c1"}; !equalStrings(got, want) {
		t.Fatalf("写路径顺序 = %v，want %v", got, want)
	}
}
