package policy

// 结构化成因码钉子（v4-7 后半 F7 只增）：每类判定出口的 Decision.Code 必须正确。
//
// 三类判定（+审批子路径）：
//  1. rule_matched   —— 命中规则（deny/ask/allow 皆是）
//  2. policy_default —— 无命中 fail-close 默认 deny（含 nil 引擎）
//  3. 审批系         —— approver_absent / approval_context_ended /
//                       approval_context_incomplete / approval_failed /
//                       approval_denied / approval_allowed
//
// 变异自证：把任一出口的 Code 填充去掉 → 本文件对应钉子必红。

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// codeOf 取判定的成因码（nil → 空串，测试断言用）。
func codeOf(d contracts.Decision) string {
	if d.Code == nil {
		return ""
	}
	return string(*d.Code)
}

func mustCode(t *testing.T, d contracts.Decision, want contracts.DecisionCode, what string) {
	t.Helper()
	if got := codeOf(d); got != string(want) {
		t.Fatalf("%s: code = %q, want %q（verdict=%s reason=%v）", what, got, want, d.Verdict, deref(d.Reason))
	}
}

// TestDecisionCodeRuleMatched：命中规则的 deny / ask / allow 三种 verdict 都是 rule_matched。
func TestDecisionCodeRuleMatched(t *testing.T) {
	e := New(
		mkRule("d1", contracts.EffectDeny, "bash", nil, contracts.RuleSourceManaged),
		mkRule("a1", contracts.EffectAsk, "rm", nil, contracts.RuleSourceManaged),
		mkRule("l1", contracts.EffectAllow, "read", nil, contracts.RuleSourceManaged),
	)
	mustCode(t, e.Evaluate(context.Background(), Query{Tool: "bash"}),
		contracts.DecisionCodeRuleMatched, "deny 命中规则")
	mustCode(t, e.Evaluate(context.Background(), Query{Tool: "rm"}),
		contracts.DecisionCodeRuleMatched, "ask 命中规则")
	mustCode(t, e.Evaluate(context.Background(), Query{Tool: "read"}),
		contracts.DecisionCodeRuleMatched, "allow 命中规则")
}

// TestDecisionCodePolicyDefault：无命中 / nil 引擎 / 空工具名 = policy_default。
func TestDecisionCodePolicyDefault(t *testing.T) {
	e := New(mkRule("l1", contracts.EffectAllow, "read", nil, contracts.RuleSourceManaged))
	mustCode(t, e.Evaluate(context.Background(), Query{Tool: "unknown"}),
		contracts.DecisionCodePolicyDefault, "无命中 fail-close")
	mustCode(t, (*Engine)(nil).Evaluate(context.Background(), Query{Tool: "read"}),
		contracts.DecisionCodePolicyDefault, "nil 引擎")
	mustCode(t, e.Evaluate(context.Background(), Query{Tool: ""}),
		contracts.DecisionCodePolicyDefault, "空工具名")
}

// TestDecisionCodeRuleRejected：被非法规则污染的工具 deny = rule_rejected。
func TestDecisionCodeRuleRejected(t *testing.T) {
	// 空 Tool 的规则不污染（无作用范围）；这里用**非空 Tool 的非法 effect**触发污染。
	e := New(contracts.PolicyRule{
		Effect: contracts.Effect("explode"), Tool: "bash", Source: contracts.RuleSourceManaged,
	})
	mustCode(t, e.Evaluate(context.Background(), Query{Tool: "bash"}),
		contracts.DecisionCodeRuleRejected, "污染 deny")
}

// TestDecisionCodeApprovalPaths：Decide 的审批子路径成因码逐一对号。
func TestDecisionCodeApprovalPaths(t *testing.T) {
	sid, aid := core.NewUlid(), core.NewUlid()
	askEngine := New(mkRule("a1", contracts.EffectAsk, "rm", nil, contracts.RuleSourceManaged))
	ask := func() Query {
		return Query{Tool: "rm", SessionID: &sid, AgentID: &aid}
	}

	// approver_absent：ask 但没有 Approver。
	mustCode(t, Decide(context.Background(), askEngine, nil, ask()),
		contracts.DecisionCodeApproverAbsent, "无 Approver")

	// approval_context_ended：审批前 ctx 已取消。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mustCode(t, Decide(ctx, askEngine, core.AlwaysAllow{}, ask()),
		contracts.DecisionCodeApprovalContextEnded, "ctx 已取消")

	// approval_context_incomplete：缺 session/agent 上下文。
	mustCode(t, Decide(context.Background(), askEngine, core.AlwaysAllow{}, Query{Tool: "rm"}),
		contracts.DecisionCodeApprovalContextIncomplete, "缺上下文")

	// approval_failed：审批人报错。
	mustCode(t, Decide(context.Background(), askEngine, errApprover{err: errors.New("炸了")}, ask()),
		contracts.DecisionCodeApprovalFailed, "审批人报错")

	// approval_denied：人审未放行。
	mustCode(t, Decide(context.Background(), askEngine, core.DenyAll{}, ask()),
		contracts.DecisionCodeApprovalDenied, "人审拒绝")

	// approval_allowed：人审显式放行。
	mustCode(t, Decide(context.Background(), askEngine, core.AlwaysAllow{}, ask()),
		contracts.DecisionCodeApprovalAllowed, "人审放行")
}

// TestDecisionCodeSerializesInJSON：code 字段在 Decision 的 JSON 形态里（契约透出面）。
func TestDecisionCodeSerializesInJSON(t *testing.T) {
	e := New() // 空引擎 → policy_default
	d := e.Evaluate(context.Background(), Query{Tool: "x"})
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !bytes.Contains(b, []byte(`"code":"policy_default"`)) {
		t.Fatalf("Decision JSON 应含 code 字段: %s", b)
	}
}
