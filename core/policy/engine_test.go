package policy

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
)

// ---- 测试辅助 ----

func sp(s string) *string { return &s }

func mkRule(id string, eff contracts.Effect, tool string, args *string, src contracts.RuleSource) contracts.PolicyRule {
	return contracts.PolicyRule{
		ID:          sp(id),
		Effect:      eff,
		Tool:        tool,
		ArgsPattern: args,
		Source:      src,
		Reason:      sp("规则 " + id + " 的理由"),
	}
}

func rawArgs(t *testing.T, s string) json.RawMessage {
	t.Helper()
	if s == "" {
		return nil
	}
	if !json.Valid([]byte(s)) {
		t.Fatalf("测试参数不是合法 JSON: %s", s)
	}
	return json.RawMessage(s)
}

func mustVerdict(t *testing.T, e *Engine, q Query, want contracts.Effect) contracts.Decision {
	t.Helper()
	d := e.Evaluate(context.Background(), q)
	if d.Verdict != want {
		t.Fatalf("Evaluate(%+v) verdict=%s, want %s (reason=%v, rule=%v)", q, d.Verdict, want, deref(d.Reason), deref(d.RuleID))
	}
	return d
}

func deref(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func ruleIDOf(d contracts.Decision) string {
	if d.RuleID == nil {
		return "<nil>"
	}
	return *d.RuleID
}

// ---- 1. 规则命中矩阵 ----

func TestEvaluateHitMatrix(t *testing.T) {
	glob := func(p string) *string { return &p }

	cases := []struct {
		name    string
		rules   []contracts.PolicyRule
		query   Query
		want    contracts.Effect
		wantRID string
	}{
		{
			name:  "工具名精确命中",
			rules: []contracts.PolicyRule{mkRule("r1", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged)},
			query: Query{Tool: "bash"},
			want:  contracts.EffectAllow, wantRID: "r1",
		},
		{
			name:  "工具名不命中 → fail-close deny",
			rules: []contracts.PolicyRule{mkRule("r1", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged)},
			query: Query{Tool: "read"},
			want:  contracts.EffectDeny,
		},
		{
			name:  "通配工具名 * 命中任意工具",
			rules: []contracts.PolicyRule{mkRule("r1", contracts.EffectAllow, "*", nil, contracts.RuleSourceManaged)},
			query: Query{Tool: "whatever"},
			want:  contracts.EffectAllow, wantRID: "r1",
		},
		{
			name: "args glob 命中顶层字符串参数",
			rules: []contracts.PolicyRule{mkRule("r1", contracts.EffectDeny, "write",
				glob("*.lock"), contracts.RuleSourceProject)},
			query: Query{Tool: "write", Args: rawArgs(t, `{"path":"go.sum.lock"}`)},
			want:  contracts.EffectDeny, wantRID: "r1",
		},
		{
			name: "args glob 未命中 → fail-close deny",
			rules: []contracts.PolicyRule{mkRule("r1", contracts.EffectDeny, "write",
				glob("*.lock"), contracts.RuleSourceProject)},
			query: Query{Tool: "write", Args: rawArgs(t, `{"path":"main.go"}`)},
			want:  contracts.EffectDeny,
		},
		{
			name: "args glob 命中嵌套对象里的字符串",
			rules: []contracts.PolicyRule{mkRule("r1", contracts.EffectAsk, "edit",
				sp("migrations/*"), contracts.RuleSourceProject)},
			query: Query{Tool: "edit", Args: rawArgs(t, `{"target":{"path":"migrations/001_init.sql"}}`)},
			want:  contracts.EffectAsk, wantRID: "r1",
		},
		{
			name: "args glob 对嵌套值逐个试（含后缀匹配）",
			rules: []contracts.PolicyRule{mkRule("r1", contracts.EffectDeny, "edit",
				sp("*.sql"), contracts.RuleSourceProject)},
			query: Query{Tool: "edit", Args: rawArgs(t, `{"target":{"dir":"migrations","name":"001_init.sql"}}`)},
			want:  contracts.EffectDeny, wantRID: "r1",
		},
		{
			name: "args glob 命中数组元素里的字符串",
			rules: []contracts.PolicyRule{mkRule("r1", contracts.EffectDeny, "rm",
				glob("*.env"), contracts.RuleSourceProject)},
			query: Query{Tool: "rm", Args: rawArgs(t, `{"paths":["a.txt",".env","b.go"]}`)},
			want:  contracts.EffectDeny, wantRID: "r1",
		},
		{
			name: "args 非字符串值不参与 glob 匹配",
			rules: []contracts.PolicyRule{mkRule("r1", contracts.EffectAllow, "sleep",
				glob("*.lock"), contracts.RuleSourceProject)},
			query: Query{Tool: "sleep", Args: rawArgs(t, `{"ms":1000,"flag":true}`)},
			want:  contracts.EffectDeny,
		},
		{
			name: "args 不是合法 JSON → 参数化规则不命中",
			rules: []contracts.PolicyRule{mkRule("r1", contracts.EffectAllow, "bash",
				sp("*"), contracts.RuleSourceProject)},
			query: Query{Tool: "bash", Args: json.RawMessage(`{oops`)},
			want:  contracts.EffectDeny,
		},
		{
			name: "args 缺席 → 参数化规则不命中",
			rules: []contracts.PolicyRule{mkRule("r1", contracts.EffectAllow, "bash",
				glob("*"), contracts.RuleSourceProject)},
			query: Query{Tool: "bash"},
			want:  contracts.EffectDeny,
		},
		{
			name:  "无 ArgsPattern 时不看参数",
			rules: []contracts.PolicyRule{mkRule("r1", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged)},
			query: Query{Tool: "bash", Args: rawArgs(t, `{"cmd":"rm -rf /"}`)},
			want:  contracts.EffectAllow, wantRID: "r1",
		},
		{
			name:  "空 ArgsPattern 视为无模式",
			rules: []contracts.PolicyRule{mkRule("r1", contracts.EffectAllow, "bash", glob(""), contracts.RuleSourceManaged)},
			query: Query{Tool: "bash", Args: rawArgs(t, `{"cmd":"ls"}`)},
			want:  contracts.EffectAllow, wantRID: "r1",
		},
		{
			name:  "Query.Tool 为空 → fail-close deny",
			rules: []contracts.PolicyRule{mkRule("r1", contracts.EffectAllow, "*", nil, contracts.RuleSourceManaged)},
			query: Query{Tool: ""},
			want:  contracts.EffectDeny,
		},
		{
			name:  "nil Engine → fail-close deny",
			rules: nil,
			query: Query{Tool: "bash"},
			want:  contracts.EffectDeny,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := New(c.rules...)
			d := mustVerdict(t, e, c.query, c.want)
			if c.wantRID != "" && ruleIDOf(d) != c.wantRID {
				t.Fatalf("RuleID=%q, want %q", ruleIDOf(d), c.wantRID)
			}
		})
	}
}

// ---- 2. deny 跨来源取并集（H5）----

func TestDenyUnionAcrossSources(t *testing.T) {
	// 最低优先级的 session deny 也必须压过最高优先级的 managed allow。
	e := New(
		mkRule("allow-managed", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged),
		mkRule("deny-session", contracts.EffectDeny, "bash", nil, contracts.RuleSourceSession),
	)
	d := mustVerdict(t, e, Query{Tool: "bash"}, contracts.EffectDeny)
	if ruleIDOf(d) != "deny-session" {
		t.Fatalf("RuleID=%q, want deny-session", ruleIDOf(d))
	}

	// workflow 来源的 deny 同样进并集（H24：deny/ask 照常参与）。
	e2 := New(
		mkRule("allow-managed", contracts.EffectAllow, "write", nil, contracts.RuleSourceManaged),
		mkRule("deny-workflow", contracts.EffectDeny, "write", nil, contracts.RuleSourceWorkflow),
	)
	if got := ruleIDOf(mustVerdict(t, e2, Query{Tool: "write"}, contracts.EffectDeny)); got != "deny-workflow" {
		t.Fatalf("RuleID=%q, want deny-workflow", got)
	}
}

// ---- 3. ask 优先于 allow（H2）----

func TestAskBeatsAllow(t *testing.T) {
	e := New(
		mkRule("allow-managed", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged),
		mkRule("ask-user", contracts.EffectAsk, "bash", nil, contracts.RuleSourceUser),
	)
	d := mustVerdict(t, e, Query{Tool: "bash"}, contracts.EffectAsk)
	if ruleIDOf(d) != "ask-user" {
		t.Fatalf("RuleID=%q, want ask-user", ruleIDOf(d))
	}
	if len(d.SuggestedRules) != 1 {
		t.Fatalf("SuggestedRules=%d 条, want 1", len(d.SuggestedRules))
	}
}

// ---- 4. allow 按来源优先级解析（H5）----

func TestAllowSourcePriority(t *testing.T) {
	order := []contracts.RuleSource{
		contracts.RuleSourceManaged,
		contracts.RuleSourceCli,
		contracts.RuleSourceProject,
		contracts.RuleSourceWorkflow,
		contracts.RuleSourceLocal,
		contracts.RuleSourceUser,
		contracts.RuleSourceSession,
	}
	// 反向注册（session 在前、managed 在后）：仍应取 managed —— 优先级压倒输入顺序。
	var rules []contracts.PolicyRule
	for i := len(order) - 1; i >= 0; i-- {
		rules = append(rules, mkRule("allow-"+string(order[i]), contracts.EffectAllow, "bash", nil, order[i]))
	}
	e := New(rules...)
	d := mustVerdict(t, e, Query{Tool: "bash"}, contracts.EffectAllow)
	// workflow allow 被忽略 → 最高优先级来源是 managed。
	if ruleIDOf(d) != "allow-managed" {
		t.Fatalf("RuleID=%q, want allow-managed", ruleIDOf(d))
	}

	// 去掉 managed 后由 cli 胜出。
	var rules2 []contracts.PolicyRule
	for _, s := range order[1:] {
		rules2 = append(rules2, mkRule("allow-"+string(s), contracts.EffectAllow, "bash", nil, s))
	}
	e2 := New(rules2...)
	if got := ruleIDOf(mustVerdict(t, e2, Query{Tool: "bash"}, contracts.EffectAllow)); got != "allow-cli" {
		t.Fatalf("RuleID=%q, want allow-cli", got)
	}
}

// ---- 5. workflow 来源只可收窄（H5/H24）----

func TestWorkflowAllowIsIgnored(t *testing.T) {
	// 同 tool 下只有 workflow allow → 必须 deny（不放宽）。
	e := New(mkRule("wf-allow", contracts.EffectAllow, "bash", nil, contracts.RuleSourceWorkflow))
	d := mustVerdict(t, e, Query{Tool: "bash"}, contracts.EffectDeny)
	if deref(d.Reason) != "fail-close: no matching rule" {
		t.Fatalf("Reason=%q, want fail-close: no matching rule", deref(d.Reason))
	}

	// workflow allow + user allow → user allow 生效（workflow 那条被忽略）。
	e2 := New(
		mkRule("wf-allow", contracts.EffectAllow, "bash", nil, contracts.RuleSourceWorkflow),
		mkRule("user-allow", contracts.EffectAllow, "bash", nil, contracts.RuleSourceUser),
	)
	if got := ruleIDOf(mustVerdict(t, e2, Query{Tool: "bash"}, contracts.EffectAllow)); got != "user-allow" {
		t.Fatalf("RuleID=%q, want user-allow", got)
	}

	// workflow ask 照常参与并集（压过 managed allow）。
	e3 := New(
		mkRule("managed-allow", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged),
		mkRule("wf-ask", contracts.EffectAsk, "bash", nil, contracts.RuleSourceWorkflow),
	)
	if got := ruleIDOf(mustVerdict(t, e3, Query{Tool: "bash"}, contracts.EffectAsk)); got != "wf-ask" {
		t.Fatalf("RuleID=%q, want wf-ask", got)
	}
}

// ---- 6. fail-close（H8）----

func TestFailClosedNoMatch(t *testing.T) {
	e := New(mkRule("r1", contracts.EffectAllow, "read", nil, contracts.RuleSourceManaged))
	d := mustVerdict(t, e, Query{Tool: "bash"}, contracts.EffectDeny)
	if deref(d.Reason) != "fail-close: no matching rule" {
		t.Fatalf("Reason=%q, want %q", deref(d.Reason), "fail-close: no matching rule")
	}
	if d.RuleID != nil {
		t.Fatalf("fail-close 时 RuleID 应为 nil, got %q", *d.RuleID)
	}

	// 空引擎同样 fail-close。
	if got := mustVerdict(t, New(), Query{Tool: "bash"}, contracts.EffectDeny); deref(got.Reason) != "fail-close: no matching rule" {
		t.Fatalf("空引擎 Reason=%q", deref(got.Reason))
	}
}

// ---- 7. 确定性：同类效果内取输入顺序首个命中 ----

func TestDeterministicFirstHitWithinEffect(t *testing.T) {
	e := New(
		mkRule("deny-1", contracts.EffectDeny, "bash", nil, contracts.RuleSourceSession),
		mkRule("deny-2", contracts.EffectDeny, "bash", nil, contracts.RuleSourceManaged),
	)
	for i := 0; i < 50; i++ {
		if got := ruleIDOf(mustVerdict(t, e, Query{Tool: "bash"}, contracts.EffectDeny)); got != "deny-1" {
			t.Fatalf("第 %d 次 RuleID=%q, want deny-1（输入顺序首个命中）", i, got)
		}
	}

	// 未填 ID 的规则也有稳定可审计标识。
	e2 := New(contracts.PolicyRule{Effect: contracts.EffectDeny, Tool: "bash", Source: contracts.RuleSourceCli})
	d := mustVerdict(t, e2, Query{Tool: "bash"}, contracts.EffectDeny)
	if ruleIDOf(d) != "rule#0" {
		t.Fatalf("RuleID=%q, want rule#0", ruleIDOf(d))
	}
}

// ---- 8. suggested_rules 形状（D13）----

func TestSuggestedRulesShape(t *testing.T) {
	pattern := "git push *"
	ask := mkRule("ask-git-push", contracts.EffectAsk, "bash", &pattern, contracts.RuleSourceUser)
	ask.Reason = sp("推送远端有副作用")
	e := New(ask)
	d := mustVerdict(t, e, Query{Tool: "bash", Args: rawArgs(t, `{"cmd":"git push origin main"}`)}, contracts.EffectAsk)

	if len(d.SuggestedRules) != 1 {
		t.Fatalf("SuggestedRules=%d 条, want 1", len(d.SuggestedRules))
	}
	s := d.SuggestedRules[0]
	if s.Effect != contracts.EffectAllow {
		t.Errorf("建议规则 Effect=%s, want allow", s.Effect)
	}
	if s.Tool != "bash" {
		t.Errorf("建议规则 Tool=%q, want bash", s.Tool)
	}
	if s.ArgsPattern == nil || *s.ArgsPattern != pattern {
		t.Errorf("建议规则 ArgsPattern=%v, want %q", s.ArgsPattern, pattern)
	}
	if s.Source != contracts.RuleSourceUser {
		t.Errorf("建议规则 Source=%s, want user", s.Source)
	}
	if s.Reason == nil || !strings.Contains(*s.Reason, "suggested from ask") {
		t.Errorf("建议规则 Reason=%v, 需含 \"suggested from ask\"", s.Reason)
	}
	if s.ID == nil || *s.ID == "" {
		t.Errorf("建议规则 ID 不能为空")
	}
	// 非 ask 判定不带建议规则。
	e2 := New(mkRule("d", contracts.EffectDeny, "bash", nil, contracts.RuleSourceManaged))
	if d2 := mustVerdict(t, e2, Query{Tool: "bash"}, contracts.EffectDeny); len(d2.SuggestedRules) != 0 {
		t.Errorf("deny 判定不应带 SuggestedRules, got %d", len(d2.SuggestedRules))
	}
}

// ---- 9. New 拒绝非法规则（fail-close）----

func TestNewRejectsInvalidRules(t *testing.T) {
	bad := []struct {
		name string
		rule contracts.PolicyRule
		// wantTainted：该非法规则是否污染它的 Tool。
		// 判据是 effect：deny/ask（以及读不出的 effect）污染；allow 不污染（丢的是许可）。
		wantTainted bool
	}{
		{"非法 glob（未闭合字符类）", mkRule("bad1", contracts.EffectAllow, "bash", sp("["), contracts.RuleSourceManaged), false},
		{"非法 glob（尾部反斜杠）", mkRule("bad2", contracts.EffectAllow, "bash", sp(`a\`), contracts.RuleSourceProject), false},
		{"空 Tool", mkRule("bad3", contracts.EffectAllow, "", nil, contracts.RuleSourceManaged), false},
		// effect 读不出来：它可能是被写坏的限制 → 保守归宿 = 污染。
		{"未知 Effect", contracts.PolicyRule{ID: sp("bad4"), Effect: contracts.Effect("permit"), Tool: "bash", Source: contracts.RuleSourceManaged}, true},
		{"非法 glob 的 deny", mkRule("bad5", contracts.EffectDeny, "bash", sp("["), contracts.RuleSourceManaged), true},
		{"非法 glob 的 ask", mkRule("bad6", contracts.EffectAsk, "bash", sp("["), contracts.RuleSourceManaged), true},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			e := New(c.rule)
			if e.Len() != 0 {
				t.Fatalf("非法规则应被拒绝, Len=%d", e.Len())
			}
			if len(e.Rejected()) != 1 {
				t.Fatalf("Rejected=%d, want 1", len(e.Rejected()))
			}
			// 被拒后引擎对该工具 fail-close deny（绝不当成 allow）。
			d := mustVerdict(t, e, Query{Tool: "bash"}, contracts.EffectDeny)
			if c.wantTainted {
				if !strings.Contains(deref(d.Reason), "tainted by rejected rule") {
					t.Fatalf("Reason=%q, 需说明工具被非法规则污染", deref(d.Reason))
				}
				if ruleIDOf(d) != ruleID(c.rule, 0) {
					t.Fatalf("RuleID=%q, want %s（指向污染源规则）", ruleIDOf(d), ruleID(c.rule, 0))
				}
			} else {
				// 未污染：就是普通的"无匹配规则"fail-close（丢的是许可/无作用范围）。
				if deref(d.Reason) != "fail-close: no matching rule" {
					t.Fatalf("Reason=%q, want fail-close: no matching rule", deref(d.Reason))
				}
			}
			// 严格模式直接报错。
			if _, err := NewStrict(c.rule); err == nil {
				t.Fatalf("NewStrict 应返回 error")
			}
		})
	}

	// 合法规则不受影响（部分非法不污染合法规则）。
	e := New(
		mkRule("ok", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged),
		mkRule("bad", contracts.EffectDeny, "read", sp("["), contracts.RuleSourceManaged),
	)
	if e.Len() != 1 || len(e.Rejected()) != 1 {
		t.Fatalf("Len=%d Rejected=%d, want 1/1", e.Len(), len(e.Rejected()))
	}
	if got := ruleIDOf(mustVerdict(t, e, Query{Tool: "bash"}, contracts.EffectAllow)); got != "ok" {
		t.Fatalf("RuleID=%q, want ok", got)
	}
	// read 的 deny 规则非法被拒 → read 被污染 → deny（fail-close）。
	d := mustVerdict(t, e, Query{Tool: "read"}, contracts.EffectDeny)
	if !strings.Contains(deref(d.Reason), "tainted by rejected rule") {
		t.Fatalf("Reason=%q, 需说明 read 被非法规则污染", deref(d.Reason))
	}
	if ruleIDOf(d) != "bad" {
		t.Fatalf("RuleID=%q, want bad（指向污染源规则）", ruleIDOf(d))
	}
}

// ruleID 取规则的稳定标识（与引擎内部一致：未填 ID 时用 rule#<序号>）。

// P1-1（fail-open）：被拒的 deny 规则不得静默消失——非法 deny + 合法 allow → 必须 deny。
func TestRejectedDenyTaintsTool(t *testing.T) {
	// 非法 deny（glob 写坏了）+ 同工具的合法 allow → 必须 deny，不得 allow。
	e := New(
		mkRule("broken-deny", contracts.EffectDeny, "bash", sp("["), contracts.RuleSourceManaged),
		mkRule("valid-allow", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged),
	)
	d := mustVerdict(t, e, Query{Tool: "bash"}, contracts.EffectDeny)
	if !strings.Contains(deref(d.Reason), "tainted by rejected rule") {
		t.Fatalf("Reason=%q, 需说明被非法规则污染", deref(d.Reason))
	}
	if !strings.Contains(deref(d.Reason), "broken-deny") {
		t.Fatalf("Reason=%q, 需点名污染源规则 id", deref(d.Reason))
	}
	if !strings.Contains(deref(d.Reason), "deny") {
		t.Fatalf("Reason=%q, 需注明污染源规则的效果", deref(d.Reason))
	}
	if ruleIDOf(d) != "broken-deny" {
		t.Fatalf("RuleID=%q, want broken-deny", ruleIDOf(d))
	}
	if got := e.Tainted(); len(got) != 1 || got[0] != "bash" {
		t.Fatalf("Tainted()=%v, want [bash]", got)
	}
}

// 全局污染：被拒规则的 Tool 是 "*" → 所有工具一律 deny。
func TestRejectedGlobalRuleTaintsEverything(t *testing.T) {
	e := New(
		mkRule("broken-global", contracts.EffectDeny, "*", sp("["), contracts.RuleSourceManaged),
		mkRule("valid-allow", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged),
	)
	for _, tool := range []string{"bash", "read", "anything"} {
		d := mustVerdict(t, e, Query{Tool: tool}, contracts.EffectDeny)
		if !strings.Contains(deref(d.Reason), "tainted by rejected rule") {
			t.Fatalf("%s: Reason=%q, 需说明被全局污染", tool, deref(d.Reason))
		}
	}
}

// 污染只影响被污染的工具：别的工具不受牵连。
func TestTaintIsScopedToTheTool(t *testing.T) {
	e := New(
		mkRule("broken-deny", contracts.EffectDeny, "bash", sp("["), contracts.RuleSourceManaged),
		mkRule("allow-read", contracts.EffectAllow, "read", nil, contracts.RuleSourceManaged),
	)
	if got := ruleIDOf(mustVerdict(t, e, Query{Tool: "read"}, contracts.EffectAllow)); got != "allow-read" {
		t.Fatalf("RuleID=%q, want allow-read（未污染的工具不受牵连）", got)
	}
	mustVerdict(t, e, Query{Tool: "bash"}, contracts.EffectDeny)
}

// N4：污染判据改为**被丢规则的 effect**——被拒的 deny / ask（任意来源）一律全局污染：
// 丢掉的是一条跨来源生效的限制，非法版本不得比它的合法版本更宽松
// （非法 workflow deny + managed 合法 allow → 必须 deny，而不是 allow）。
func TestRejectedDenyTaintsToolRegardlessOfSource(t *testing.T) {
	for _, src := range []contracts.RuleSource{
		contracts.RuleSourceWorkflow,
		contracts.RuleSourceUser,
		contracts.RuleSourceSession,
		contracts.RuleSourceLocal,
		contracts.RuleSourceManaged,
		contracts.RuleSourceCli,
		contracts.RuleSourceProject,
	} {
		e := New(
			mkRule("broken-"+string(src), contracts.EffectDeny, "bash", sp("["), src),
			mkRule("allow-managed", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged),
			mkRule("allow-user", contracts.EffectAllow, "bash", nil, contracts.RuleSourceUser),
		)
		d := mustVerdict(t, e, Query{Tool: "bash"}, contracts.EffectDeny)
		if !strings.Contains(deref(d.Reason), "tainted by rejected rule") {
			t.Fatalf("%s: Reason=%q, 需说明被非法规则污染", src, deref(d.Reason))
		}
		if !strings.Contains(deref(d.Reason), "source="+string(src)) {
			t.Fatalf("%s: Reason=%q, 需点名来源（审计：谁写坏了）", src, deref(d.Reason))
		}
		if ruleIDOf(d) != "broken-"+string(src) {
			t.Fatalf("%s: RuleID=%q, want broken-%s（指向污染源规则）", src, ruleIDOf(d), src)
		}
	}
}

// N4：被拒的 **ask** 同样污染（ask 也是跨来源并集的限制，人审不能被笔误绕过）。
func TestRejectedWorkflowAskTaintsTool(t *testing.T) {
	e := New(
		mkRule("broken-ask", contracts.EffectAsk, "bash", sp("["), contracts.RuleSourceWorkflow),
		mkRule("allow-managed", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged),
	)
	d := mustVerdict(t, e, Query{Tool: "bash"}, contracts.EffectDeny)
	if !strings.Contains(deref(d.Reason), "tainted by rejected rule") {
		t.Fatalf("Reason=%q, 需说明被非法 ask 规则污染", deref(d.Reason))
	}
	if !strings.Contains(deref(d.Reason), "effect=ask") {
		t.Fatalf("Reason=%q, 需注明污染源规则的效果", deref(d.Reason))
	}
	if !strings.Contains(deref(d.Reason), "source=workflow") {
		t.Fatalf("Reason=%q, 需点名来源 workflow", deref(d.Reason))
	}
}

// N4：被拒的 **allow** 不污染（丢的是许可；没有其他 allow 时 fail-close 默认已 deny）。
func TestRejectedAllowDoesNotTaintTool(t *testing.T) {
	// 被拒的 allow + 同一工具的合法 allow → 正常放行（被拒的 allow 没有污染它）。
	e := New(
		mkRule("broken-allow", contracts.EffectAllow, "read", sp("["), contracts.RuleSourceManaged),
		mkRule("valid-allow", contracts.EffectAllow, "read", nil, contracts.RuleSourceManaged),
	)
	d := mustVerdict(t, e, Query{Tool: "read"}, contracts.EffectAllow)
	if ruleIDOf(d) != "valid-allow" {
		t.Fatalf("RuleID=%q, want valid-allow（被拒的 allow 不污染）", ruleIDOf(d))
	}
	if got := e.Tainted(); len(got) != 0 {
		t.Fatalf("Tainted()=%v, want 空（被拒的 allow 不污染）", got)
	}
	// 被拒的 allow + 无其他 allow → 普通 fail-close deny（理由不是"污染"）。
	e2 := New(mkRule("broken-allow", contracts.EffectAllow, "read", sp("["), contracts.RuleSourceManaged))
	d2 := mustVerdict(t, e2, Query{Tool: "read"}, contracts.EffectDeny)
	if deref(d2.Reason) != "fail-close: no matching rule" {
		t.Fatalf("Reason=%q, want fail-close: no matching rule", deref(d2.Reason))
	}
	// 被拒的 allow 不影响别的工具。
	e3 := New(
		mkRule("broken-allow", contracts.EffectAllow, "read", sp("["), contracts.RuleSourceManaged),
		mkRule("allow-bash", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged),
	)
	if got := ruleIDOf(mustVerdict(t, e3, Query{Tool: "bash"}, contracts.EffectAllow)); got != "allow-bash" {
		t.Fatalf("RuleID=%q, want allow-bash", got)
	}
}

// N4：合法规则的语义完全不变——合法 deny 仍跨来源取并集（H5），
// 不受污染机制影响（一条合法的 workflow deny 就是全局 deny）。
func TestLegalDenySemanticsUnchangedByTaint(t *testing.T) {
	e := New(
		mkRule("legal-workflow-deny", contracts.EffectDeny, "bash", nil, contracts.RuleSourceWorkflow),
		mkRule("allow-managed", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged),
	)
	d := mustVerdict(t, e, Query{Tool: "bash"}, contracts.EffectDeny)
	if ruleIDOf(d) != "legal-workflow-deny" {
		t.Fatalf("RuleID=%q, want legal-workflow-deny（合法 deny 仍跨来源并集）", ruleIDOf(d))
	}
	if strings.Contains(deref(d.Reason), "tainted") {
		t.Fatalf("合法规则不该被报成污染：%q", deref(d.Reason))
	}
}

// ---- 10. 并发求值（只读引擎）----

func TestEvaluateConcurrent(t *testing.T) {
	e := New(
		mkRule("allow-managed", contracts.EffectAllow, "bash", nil, contracts.RuleSourceManaged),
		mkRule("ask-user", contracts.EffectAsk, "bash", nil, contracts.RuleSourceUser),
	)
	done := make(chan contracts.Effect, 8)
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 200; j++ {
				done <- e.Evaluate(context.Background(), Query{Tool: "bash"}).Verdict
			}
		}()
	}
	for i := 0; i < 8*200; i++ {
		if got := <-done; got != contracts.EffectAsk {
			t.Fatalf("并发求值出现 %s, want ask", got)
		}
	}
}
