package policy

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// ---- 测试用 Approver ----

// errApprover 一律返回 error（审批服务故障/超时）。
type errApprover struct{ err error }

func (a errApprover) Approve(context.Context, contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	return contracts.ApprovalResponse{}, a.err
}

// panicApprover 总是 panic（验证失败隔离，E5）。
type panicApprover struct{}

func (panicApprover) Approve(context.Context, contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	panic("审批人炸了")
}

// scriptApprover 记录请求并按脚本返回（可断言请求字段）。
type scriptApprover struct {
	mu      sync.Mutex
	reqs    []contracts.ApprovalRequest
	resp    func(contracts.ApprovalRequest) (contracts.ApprovalResponse, error)
	entered chan struct{}
}

func newScriptApprover(f func(contracts.ApprovalRequest) (contracts.ApprovalResponse, error)) *scriptApprover {
	return &scriptApprover{resp: f, entered: make(chan struct{}, 8)}
}

func (a *scriptApprover) Approve(_ context.Context, req contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	a.mu.Lock()
	a.reqs = append(a.reqs, req)
	a.mu.Unlock()
	select {
	case a.entered <- struct{}{}:
	default:
	}
	return a.resp(req)
}

func (a *scriptApprover) calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.reqs)
}

func allowResp(req contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	return contracts.ApprovalResponse{
		V:         1,
		RequestID: req.ID,
		TS:        req.TS,
		Decision:  contracts.ApprovalDecisionAllow,
		DecidedBy: sp("user:tester"),
	}, nil
}

func denyResp(req contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	return contracts.ApprovalResponse{
		V:         1,
		RequestID: req.ID,
		TS:        req.TS,
		Decision:  contracts.ApprovalDecisionDeny,
		Note:      sp("用户拒绝"),
		DecidedBy: sp("user:tester"),
	}, nil
}

func askEngine() *Engine {
	return New(mkRule("ask-bash", contracts.EffectAsk, "bash", nil, contracts.RuleSourceUser))
}

// ctxQuery 返回带 session/agent 上下文的查询。
//
// 审批请求的契约要求 session_id / agent_id 是必填的合法 ULID → 走 ask 路径的用例
// 必须带上下文（否则 Decide 会 fail-close 拒绝，见 Decide 的上下文缺失分支）。
func ctxQuery(t *testing.T, tool string) Query {
	t.Helper()
	sid := contracts.Ulid(core.NewUlid())
	aid := contracts.Ulid(core.NewUlid())
	return Query{Tool: tool, SessionID: &sid, AgentID: &aid}
}

// ---- Decide：ask → 人审（D13/D14/H8）----

func TestDecideAskWithDenyAllFallsToDeny(t *testing.T) {
	d := Decide(context.Background(), askEngine(), core.DenyAll{}, Query{Tool: "bash"})
	if d.Verdict != contracts.EffectDeny {
		t.Fatalf("verdict=%s, want deny", d.Verdict)
	}
	if d.Reason == nil || !strings.Contains(*d.Reason, "fail-close") {
		t.Fatalf("Reason=%v, 需含 fail-close", d.Reason)
	}
}

func TestDecideAskWithAlwaysAllowFallsToAllow(t *testing.T) {
	d := Decide(context.Background(), askEngine(), core.AlwaysAllow{}, ctxQuery(t, "bash"))
	if d.Verdict != contracts.EffectAllow {
		t.Fatalf("verdict=%s, want allow", d.Verdict)
	}
	if d.RuleID == nil || *d.RuleID != "ask-bash" {
		t.Fatalf("RuleID=%v, want ask-bash（保留命中的 ask 规则）", d.RuleID)
	}
	if d.Reason == nil || !strings.Contains(*d.Reason, "allow") {
		t.Fatalf("Reason=%v, 需说明依据", d.Reason)
	}
}

func TestDecideAskWithApproverErrorFallsToDeny(t *testing.T) {
	ap := errApprover{err: errors.New("审批服务不可达")}
	d := Decide(context.Background(), askEngine(), ap, ctxQuery(t, "bash"))
	if d.Verdict != contracts.EffectDeny {
		t.Fatalf("verdict=%s, want deny", d.Verdict)
	}
	if d.Reason == nil || !strings.Contains(*d.Reason, "审批服务不可达") {
		t.Fatalf("Reason=%v, 需含原始错误", d.Reason)
	}
}

func TestDecideAskWithApproverDenyFallsToDeny(t *testing.T) {
	d := Decide(context.Background(), askEngine(), newScriptApprover(denyResp), ctxQuery(t, "bash"))
	if d.Verdict != contracts.EffectDeny {
		t.Fatalf("verdict=%s, want deny", d.Verdict)
	}
	if d.Reason == nil || !strings.Contains(*d.Reason, "用户拒绝") {
		t.Fatalf("Reason=%v, 需含审批备注", d.Reason)
	}
}

func TestDecideAskWithNilApproverFallsToDeny(t *testing.T) {
	d := Decide(context.Background(), askEngine(), nil, Query{Tool: "bash"})
	if d.Verdict != contracts.EffectDeny {
		t.Fatalf("verdict=%s, want deny", d.Verdict)
	}
	if d.Reason == nil || !strings.Contains(*d.Reason, "Approver") {
		t.Fatalf("Reason=%v, 需说明没有 Approver", d.Reason)
	}
}

func TestDecideAskWithCanceledContextFallsToDeny(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ap := newScriptApprover(allowResp)
	d := Decide(ctx, askEngine(), ap, Query{Tool: "bash"})
	if d.Verdict != contracts.EffectDeny {
		t.Fatalf("verdict=%s, want deny", d.Verdict)
	}
	if ap.calls() != 0 {
		t.Fatalf("上下文已取消时不应调用 Approver, calls=%d", ap.calls())
	}
}

func TestDecideNonAskNeverCallsApprover(t *testing.T) {
	denyEngine := New(mkRule("d", contracts.EffectDeny, "bash", nil, contracts.RuleSourceManaged))
	allowEngine := New(mkRule("a", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged))

	for name, e := range map[string]*Engine{"deny": denyEngine, "allow": allowEngine} {
		ap := newScriptApprover(allowResp)
		d := Decide(context.Background(), e, ap, Query{Tool: "bash"})
		if ap.calls() != 0 {
			t.Fatalf("%s: 非 ask 判定不应调用 Approver, calls=%d", name, ap.calls())
		}
		if d.Verdict != contracts.Effect(name) {
			t.Fatalf("%s: verdict=%s", name, d.Verdict)
		}
	}
}

func TestDecideApprovalRequestShape(t *testing.T) {
	sid := contracts.Ulid(core.NewUlid())
	aid := contracts.Ulid(core.NewUlid())
	ap := newScriptApprover(allowResp)
	q := Query{Tool: "bash", Args: rawArgs(t, `{"cmd":"git push"}`), SessionID: &sid, AgentID: &aid}
	d := Decide(context.Background(), askEngine(), ap, q)
	if d.Verdict != contracts.EffectAllow {
		t.Fatalf("verdict=%s, want allow", d.Verdict)
	}
	<-ap.entered
	if ap.calls() != 1 {
		t.Fatalf("calls=%d, want 1", ap.calls())
	}
	req := ap.reqs[0]
	if req.V != 1 {
		t.Errorf("req.V=%d, want 1", req.V)
	}
	if !core.ValidUlid(req.ID) {
		t.Errorf("req.ID=%q 不是合法 ULID", req.ID)
	}
	ts, err := time.Parse(time.RFC3339, req.TS)
	if err != nil {
		t.Fatalf("req.TS=%q 不是 RFC3339: %v", req.TS, err)
	}
	if time.Since(ts) > time.Minute {
		t.Errorf("req.TS=%q 与当前时间相差过大", req.TS)
	}
	if req.Trigger != contracts.ApprovalTriggerPolicyAsk {
		t.Errorf("req.Trigger=%s, want policy_ask", req.Trigger)
	}
	if req.SessionID != sid || req.AgentID != aid {
		t.Errorf("req.SessionID=%q req.AgentID=%q, want %q/%q", req.SessionID, req.AgentID, sid, aid)
	}
	if req.Summary == "" {
		t.Errorf("req.Summary 不能为空")
	}
	if req.ToolCallID != nil {
		t.Errorf("req.ToolCallID 应为空（Query 不携带），got %v", req.ToolCallID)
	}
	if len(req.Detail) == 0 || !strings.Contains(string(req.Detail), "suggested_rules") {
		t.Errorf("req.Detail 应携带 suggested_rules，got %s", req.Detail)
	}
}

func TestDecideNilEngineFallsToDeny(t *testing.T) {
	var e *Engine
	d := Decide(context.Background(), e, core.AlwaysAllow{}, Query{Tool: "bash"})
	if d.Verdict != contracts.EffectDeny {
		t.Fatalf("verdict=%s, want deny", d.Verdict)
	}
}

// 审批人崩溃 = 无人应答 = deny（E5 失败隔离）：不得把 core 带崩。
func TestDecideApproverPanicFallsToDeny(t *testing.T) {
	ap := panicApprover{}
	d := Decide(context.Background(), askEngine(), ap, ctxQuery(t, "bash"))
	if d.Verdict != contracts.EffectDeny {
		t.Fatalf("verdict=%s, want deny（审批人崩溃不得放行）", d.Verdict)
	}
	if d.Reason == nil || !strings.Contains(*d.Reason, "fail-close") {
		t.Fatalf("Reason=%v, 需含 fail-close", d.Reason)
	}
}

// 有类型的 nil 审批人（接口非 nil、值是 nil）：必须落 deny，不得 panic。
func TestDecideTypedNilApproverFallsToDeny(t *testing.T) {
	var d *core.DenyAll
	got := Decide(context.Background(), askEngine(), d, ctxQuery(t, "bash"))
	if got.Verdict != contracts.EffectDeny {
		t.Fatalf("verdict=%s, want deny", got.Verdict)
	}
	if got.Reason == nil || !strings.Contains(*got.Reason, "fail-close") {
		t.Fatalf("Reason=%v, 需含 fail-close", got.Reason)
	}
}

// P1-3：ask 路径缺少 session/agent 上下文 → 直接 fail-close deny，
// **不构造 ApprovalRequest、不调用 Approver**（否则会产出违反自家契约的审批请求）。
func TestDecideMissingContextDeniesWithoutApprover(t *testing.T) {
	cases := []struct {
		name    string
		q       Query
		missing string
	}{
		{"缺 session", Query{Tool: "bash"}, "session_id"},
		{"缺 agent", func() Query {
			sid := contracts.Ulid(core.NewUlid())
			return Query{Tool: "bash", SessionID: &sid}
		}(), "agent_id"},
		{"两者都缺", Query{Tool: "bash"}, "session_id/agent_id"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ap := newScriptApprover(allowResp)
			d := Decide(context.Background(), askEngine(), ap, c.q)
			if d.Verdict != contracts.EffectDeny {
				t.Fatalf("verdict=%s, want deny（缺上下文不得放行）", d.Verdict)
			}
			if ap.calls() != 0 {
				t.Fatalf("缺上下文时不得调用 Approver, calls=%d", ap.calls())
			}
			if d.Reason == nil || !strings.Contains(*d.Reason, "missing session/agent context") {
				t.Fatalf("Reason=%v, 需说明缺少 session/agent 上下文", d.Reason)
			}
			if d.Reason == nil || !strings.Contains(*d.Reason, c.missing) {
				t.Fatalf("Reason=%v, 需点名缺失字段 %s", d.Reason, c.missing)
			}
		})
	}
}
