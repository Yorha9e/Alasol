package registry

// 能力裁决成因码钉子（v4-7 后半 F7 只增）：能力裁决出口的 deny 必须带
// capability_denied（与工具策略的 rule_matched / policy_default 区分开——
// 审计要能分辨"工具被策略拒"与"扩展能力没拿到背书"）。
//
// 变异自证：去掉 adjudicate / PolicyAdjudicator 的 Code 填充 → 本文件钉子必红。

import (
	"context"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/policy"
)

func codeString(d contracts.Decision) string {
	if d.Code == nil {
		return ""
	}
	return string(*d.Code)
}

// TestAdjudicateCodeCapabilityDenied：无裁决器 / 裁决 panic → capability_denied。
func TestAdjudicateCodeCapabilityDenied(t *testing.T) {
	d := adjudicate(nil, context.Background(), "fs:write")
	if d.Verdict != contracts.EffectDeny || codeString(d) != string(contracts.DecisionCodeCapabilityDenied) {
		t.Fatalf("无裁决器应 deny+capability_denied，得到 verdict=%s code=%q", d.Verdict, codeString(d))
	}

	d = adjudicate(panicAdjudicator{}, context.Background(), "fs:write")
	if d.Verdict != contracts.EffectDeny || codeString(d) != string(contracts.DecisionCodeCapabilityDenied) {
		t.Fatalf("裁决 panic 应 deny+capability_denied，得到 verdict=%s code=%q", d.Verdict, codeString(d))
	}
}

// TestPolicyAdjudicatorCodeCapabilityDenied：无策略引擎 / 空能力名 → capability_denied；
// 正常求值的 deny（无命中 fail-close）→ 由 policy 引擎给 policy_default（不覆盖）。
func TestPolicyAdjudicatorCodeCapabilityDenied(t *testing.T) {
	var nilAdj *PolicyAdjudicator
	d := nilAdj.Adjudicate(context.Background(), "fs:write")
	if d.Verdict != contracts.EffectDeny || codeString(d) != string(contracts.DecisionCodeCapabilityDenied) {
		t.Fatalf("nil 裁决器应 deny+capability_denied，得到 verdict=%s code=%q", d.Verdict, codeString(d))
	}

	adj := NewPolicyAdjudicator(policy.New(mkAllow("fs:read")))
	d = adj.Adjudicate(context.Background(), "")
	if d.Verdict != contracts.EffectDeny || codeString(d) != string(contracts.DecisionCodeCapabilityDenied) {
		t.Fatalf("空能力名应 deny+capability_denied，得到 verdict=%s code=%q", d.Verdict, codeString(d))
	}

	// allow 命中 → 规则命中系成因码（rule_matched），不是 capability_denied。
	d = adj.Adjudicate(context.Background(), "fs:read")
	if d.Verdict != contracts.EffectAllow || codeString(d) != string(contracts.DecisionCodeRuleMatched) {
		t.Fatalf("allow 命中应 allow+rule_matched，得到 verdict=%s code=%q", d.Verdict, codeString(d))
	}

	// 无命中 fail-close → policy_default（policy 引擎的出口，不是能力裁决壳的）。
	d = adj.Adjudicate(context.Background(), "fs:write")
	if d.Verdict != contracts.EffectDeny || codeString(d) != string(contracts.DecisionCodePolicyDefault) {
		t.Fatalf("无命中应 deny+policy_default，得到 verdict=%s code=%q", d.Verdict, codeString(d))
	}
}

// mkAllow 造一条 allow 规则（测试辅助）。
func mkAllow(tool string) contracts.PolicyRule {
	return contracts.PolicyRule{Effect: contracts.EffectAllow, Tool: tool, Source: contracts.RuleSourceManaged}
}
