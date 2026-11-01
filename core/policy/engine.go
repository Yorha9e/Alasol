package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// Query 是一次策略求值的输入。
//
// Tool 是要调用的工具名（也可以是能力名，如 "fs:write"——注册表的能力裁决复用同一套求值，E3）；
// Args 是原始参数 JSON（参数化匹配只在这上面做，H4）；
// SessionID / AgentID 为归属信息（可空），仅用于构造审批请求与审计，不参与命中判定。
type Query struct {
	Tool      string
	Args      json.RawMessage
	SessionID *contracts.Ulid
	AgentID   *contracts.Ulid
}

// Engine 是一份不可变的策略规则集。
//
// 构造后只读（rules / rejected / tainted 不再变更）→ 多 goroutine 并发 Evaluate 安全，无需加锁。
type Engine struct {
	// rules 是编译后保序的规则（输入顺序 = 同类效果内取首个命中的顺序，确定性）。
	rules []compiledRule
	// rejected 是被 New 拒绝的规则（非法 pattern / 空 Tool / 未知 Effect）。
	// fail-close：拒绝 = 该规则完全不生效，绝不当成 allow。
	rejected []contracts.PolicyRule
	// tainted 是被非法规则污染的工具（工具名 → 污染源列表；"*" = 作用于所有工具）。
	//
	// 非法规则等于"这条限制没生效"：若它本来是 deny / ask，丢掉它就是把一条
	// **跨来源生效**的限制静默拆掉（非法 deny + 合法 allow = allow，人审被笔误绕过，
	// fail-open）。因此被拒的 deny / ask 规则会让它的 Tool 被污染：一律 deny。
	//
	// 判据是**被丢规则的 effect**（不是来源）：
	//   - deny / ask（以及任何读不出的 effect）：丢掉的是**限制** → 保守归宿 = 该工具一律 deny
	//     （"宁可不做也不错"：笔误不该让限制比它的合法版本更宽松）；
	//   - allow：丢掉的是**许可** → 不污染（没有其他 allow 时 fail-close 默认已 deny）。
	//
	// 注意：**合法规则**的求值语义完全不变（H5：deny 跨来源取并集，照旧）。
	tainted map[string][]taintInfo
}

// taintInfo 记录一条被拒绝的非法规则污染了哪个工具、来自哪个来源。
type taintInfo struct {
	id     string               // 污染源规则 id（可审计）
	tool   string               // 被污染的工具名（"*" = 作用于所有工具）
	effect contracts.Effect     // 该规则声明的效果（deny / ask / 读不出的值）
	source contracts.RuleSource // 该规则来自哪个来源（审计：谁写坏了）
	// trusted 只作审计信息：来源是否受信（managed/cli/project）。它**不参与判定**
	// ——污染的作用域由 effect 决定。
	trusted bool
}

// compiledRule 是预编译的规则：命中判定所需的字段一次性备好。
type compiledRule struct {
	raw  contracts.PolicyRule
	id   string // 命中时回填 Decision.RuleID；原规则 ID 为 nil 时用 "rule#<输入序号>" 兜底
	args string // 参数 glob（空 = 不要求参数匹配）
	prio int    // allow 来源优先级（RuleSource 枚举序，越小越高；未知来源排最后）
}

// New 构造策略引擎。
//
// fail-close（H8/陷阱 9）：**非法规则在此被拒绝**——
//   - Tool 为空；
//   - Effect 不是 allow / deny / ask；
//   - ArgsPattern 不是合法 glob（path.Match 报 ErrBadPattern）。
//
// 被拒绝的规则**不进入规则集**（绝不当成 allow，也不"猜测"修复）；
// 可通过 Rejected() 观测，以便装配层记事件/报错（S9：不合则拒绝并记事件，不修复）。
//
// **被拒规则还会污染它的 Tool**（fail-close，见 taint）：被丢掉的 deny / ask = 限制静默消失
// → 该工具一律 deny（判据是 effect，与来源无关）；被丢掉的 allow = 丢的是许可 → 不污染。
// 空 Tool 的规则没有作用范围（它本来也命中不了任何工具），不污染任何工具。
// 需要严格模式（任一规则非法即整体失败）用 NewStrict。
func New(rules ...contracts.PolicyRule) *Engine {
	e := &Engine{tainted: make(map[string][]taintInfo)}
	for i := range rules {
		cr, ok := compileRule(i, rules[i])
		if !ok {
			e.rejected = append(e.rejected, rules[i])
			e.taint(rules[i], i)
			continue
		}
		e.rules = append(e.rules, cr)
	}
	return e
}

// taint 把一条被拒绝的非法规则记成"工具污染"（fail-close）。
//
// 判据是**被丢规则的 effect**：
//
//   - deny / ask（以及任何读不出的 effect）：丢掉的是一条**跨来源生效的限制**
//     （deny/ask 本来就是跨来源并集），丢掉它 = 限制被静默拆掉，非法版本甚至比它的
//     合法版本更宽松（非法 deny + 合法 allow = allow，人审被笔误绕过）→ 保守归宿：
//     该工具一律 deny（"宁可不做也不错"）；
//   - allow：丢掉的是**许可**——没有其他 allow 时 fail-close 默认已经 deny，
//     不需要额外动作 → 不污染。
//
// 空 Tool 的规则不污染任何工具：它没有作用范围（本来也命中不了任何工具），
// 丢掉它不构成"限制消失"；这类错误由 Rejected() 观测、NewStrict 整体失败。
func (e *Engine) taint(r contracts.PolicyRule, index int) {
	tool := r.Tool
	if tool == "" {
		return
	}
	if r.Effect == contracts.EffectAllow {
		return // 丢的是许可，不是限制 → 不污染
	}
	info := taintInfo{
		id:      ruleID(r, index),
		tool:    tool,
		effect:  r.Effect,
		source:  r.Source,
		trusted: trustedSource(r.Source),
	}
	for _, existing := range e.tainted[tool] {
		// 同一工具 + 同一来源 + 同一效果 → 首个污染源即可（理由取第一条，便于稳定复现）。
		if existing.source == info.source && existing.effect == info.effect {
			return
		}
	}
	e.tainted[tool] = append(e.tainted[tool], info)
}

// trustedSource 报告来源是否受信（managed/cli/project）。
//
// **仅作审计信息**（写进 deny 的 Reason，让运维知道"是谁写坏了这条规则"）——
// 污染的作用域由**被丢规则的 effect** 决定，与来源无关。
func trustedSource(s contracts.RuleSource) bool {
	switch s {
	case contracts.RuleSourceManaged, contracts.RuleSourceCli, contracts.RuleSourceProject:
		return true
	default:
		return false
	}
}

// Tainted 返回被非法规则污染的工具名（已排序；"*" 表示污染作用于所有工具）。
func (e *Engine) Tainted() []string {
	if e == nil || len(e.tainted) == 0 {
		return nil
	}
	out := make([]string, 0, len(e.tainted))
	for tool := range e.tainted {
		out = append(out, tool)
	}
	sort.Strings(out)
	return out
}

// NewStrict 是 New 的严格版：任一规则非法即返回 error 且不产出引擎（fail-close）。
func NewStrict(rules ...contracts.PolicyRule) (*Engine, error) {
	e := New(rules...)
	if len(e.rejected) > 0 {
		return nil, fmt.Errorf("policy: 拒绝 %d 条非法规则（首条: %+v）", len(e.rejected), e.rejected[0])
	}
	return e, nil
}

// compileRule 校验并编译一条规则；ok=false 表示应被拒绝。
func compileRule(index int, r contracts.PolicyRule) (compiledRule, bool) {
	if r.Tool == "" {
		return compiledRule{}, false
	}
	switch r.Effect {
	case contracts.EffectAllow, contracts.EffectDeny, contracts.EffectAsk:
	default:
		return compiledRule{}, false
	}
	cr := compiledRule{raw: r, id: ruleID(r, index), prio: sourcePriority(r.Source)}
	if r.ArgsPattern != nil && *r.ArgsPattern != "" {
		// 非法 pattern 在 New 时校验并拒绝：求值期不再判错（求值热路径只做机械匹配）。
		if _, err := path.Match(*r.ArgsPattern, ""); err != nil {
			return compiledRule{}, false
		}
		cr.args = *r.ArgsPattern
	}
	return cr, true
}

// ruleID 取规则的稳定标识；原规则未填 ID 时用输入序号合成（保证 Decision.RuleID 可审计）。
func ruleID(r contracts.PolicyRule, index int) string {
	if r.ID != nil && *r.ID != "" {
		return *r.ID
	}
	return fmt.Sprintf("rule#%d", index)
}

// sourcePriority 返回 allow 的解析优先级（H5：枚举序恰好就是优先级序）。
// 未知来源排最后（= 永远赢不过任何已知来源），保证 fail-close 方向。
func sourcePriority(s contracts.RuleSource) int {
	switch s {
	case contracts.RuleSourceManaged:
		return 0
	case contracts.RuleSourceCli:
		return 1
	case contracts.RuleSourceProject:
		return 2
	case contracts.RuleSourceWorkflow:
		return 3
	case contracts.RuleSourceLocal:
		return 4
	case contracts.RuleSourceUser:
		return 5
	case contracts.RuleSourceSession:
		return 6
	default:
		return 7
	}
}

// Len 返回生效规则数。
func (e *Engine) Len() int {
	if e == nil {
		return 0
	}
	return len(e.rules)
}

// Rejected 返回被 New 拒绝的规则（调用方不得修改返回值）。
func (e *Engine) Rejected() []contracts.PolicyRule {
	if e == nil || len(e.rejected) == 0 {
		return nil
	}
	out := make([]contracts.PolicyRule, len(e.rejected))
	copy(out, e.rejected)
	return out
}

// Evaluate 求值（H2/H3/H5/H24/D13），机械顺序：
//
//  0. 污染优先：工具被**被拒的 deny / ask 规则**污染 → 直接 deny（fail-close，理由点名污染源）；
//  1. deny：任意来源命中 → deny（跨来源并集，H5；合法规则的语义不受污染机制影响）；
//  2. ask ：否则任意来源命中 → ask（跨来源并集），并带 suggested_rules（D13）；
//  3. allow：否则在**非 workflow 来源**的 allow 命中里取来源优先级最高者（H5/H24）；
//  4. 以上全无命中 → deny，Reason = "fail-close: no matching rule"（H8）。
//
// 同类效果内按规则输入顺序取**首个**命中做 RuleID（确定性）。
// ctx 不参与命中判定（纯函数），仅为将来接入审计/超时预留；nil Engine 视为无规则 → fail-close deny。
func (e *Engine) Evaluate(ctx context.Context, q Query) contracts.Decision {
	_ = ctx
	if e == nil {
		return failClosed()
	}
	// 污染优先：被丢掉的 deny / ask = 一条跨来源的限制消失 → 该工具一律 deny（fail-close）。
	if t, ok := e.globalTaint(q.Tool); ok {
		return taintDecision(q.Tool, t)
	}
	if len(e.rules) == 0 || q.Tool == "" {
		return failClosed()
	}

	var firstDeny, firstAsk, bestAllow *compiledRule
	for i := range e.rules {
		r := &e.rules[i]
		if !r.matches(q) {
			continue
		}
		switch r.raw.Effect {
		case contracts.EffectDeny:
			if firstDeny == nil {
				firstDeny = r
			}
		case contracts.EffectAsk:
			if firstAsk == nil {
				firstAsk = r
			}
		case contracts.EffectAllow:
			// H5/H24：workflow 来源只可收窄 → 其 allow 完全忽略（不放宽任何东西）。
			if r.raw.Source == contracts.RuleSourceWorkflow {
				continue
			}
			if bestAllow == nil || r.prio < bestAllow.prio {
				bestAllow = r
			}
		}
	}

	switch {
	case firstDeny != nil:
		return newDecision(contracts.EffectDeny, firstDeny,
			fmt.Sprintf("deny: 规则 %s（source=%s）拒绝工具 %s", firstDeny.id, firstDeny.raw.Source, q.Tool))
	case firstAsk != nil:
		d := newDecision(contracts.EffectAsk, firstAsk,
			fmt.Sprintf("ask: 规则 %s（source=%s）要求人审工具 %s", firstAsk.id, firstAsk.raw.Source, q.Tool))
		// D13：把命中的 ask 翻成一条建议 allow，供审批 UI 一次性收敛。
		d.SuggestedRules = []contracts.PolicyRule{suggestAllowRule(firstAsk)}
		return d
	case bestAllow != nil:
		return newDecision(contracts.EffectAllow, bestAllow,
			fmt.Sprintf("allow: 规则 %s（source=%s，优先级 %d）放行工具 %s", bestAllow.id, bestAllow.raw.Source, bestAllow.prio, q.Tool))
	default:
		return failClosed()
	}
}

// matches 判定单条规则是否命中（H2/H4）。
func (r *compiledRule) matches(q Query) bool {
	if r.raw.Tool != "*" && r.raw.Tool != q.Tool {
		return false
	}
	if r.args != "" {
		// 参数化匹配（H4）：args 里任一字符串型参数值被 glob 命中即算命中。
		for _, s := range jsonStrings(q.Args) {
			if ok, err := path.Match(r.args, s); err == nil && ok {
				return true
			}
		}
		return false
	}
	return true
}

// taintsFor 返回作用于该工具的全部污染（工具级 + "*" 全局级）。
func (e *Engine) taintsFor(tool string) []taintInfo {
	if e == nil || len(e.tainted) == 0 {
		return nil
	}
	var out []taintInfo
	out = append(out, e.tainted[tool]...)
	if global, ok := e.tainted["*"]; ok {
		out = append(out, global...)
	}
	return out
}

// globalTaint 返回对该工具的污染（找不到 = 无）。
//
// 污染的作用域是**全局**的：被丢掉的 deny / ask 是一条跨来源生效的限制，
// 丢掉它之后该工具必须一律 deny（与来源无关——非法版本不得比合法版本更宽松）。
func (e *Engine) globalTaint(tool string) (taintInfo, bool) {
	taints := e.taintsFor(tool)
	if len(taints) == 0 {
		return taintInfo{}, false
	}
	return taints[0], true
}

// taintDecision 是被污染工具的 deny 判定（fail-close 的一个出口）。
//
// RuleID 指向污染源规则；Reason 里点名规则 id、效果、**来源**与来源是否受信
// （可审计：运维能直接定位到"谁写坏了哪条规则"）。
func taintDecision(tool string, t taintInfo) contracts.Decision {
	id := t.id
	return contracts.Decision{
		Verdict: contracts.EffectDeny,
		Code:    codePtr(contracts.DecisionCodeRuleRejected),
		RuleID:  &id,
		Reason: strPtr(fmt.Sprintf(
			"deny: 工具 %s 被非法规则污染（tainted by rejected rule %s，effect=%s，source=%s，rule.tool=%s，trusted=%t）→ 一律拒绝（fail-close）",
			tool, t.id, t.effect, t.source, t.tool, t.trusted)),
	}
}

// Decide 在 Evaluate 之上接人审（D13/D14/H8/陷阱 9）：
//
//   - verdict != ask → 原样返回（deny/allow 不问人，也不构造审批请求）；
//   - ask 且 Approver 为 nil → deny（无人可应答 = deny，不挂起）；
//   - ask 且 ctx 已取消/超时 → deny；
//   - ask 且缺少 session/agent 上下文 → deny（**不构造审批请求**，见下）；
//   - Approver 返回 error → deny；
//   - Approver 返回非 allow（含 deny）→ deny；
//   - 仅当 Approver **显式 allow** 才落 allow。
//
// 上下文缺失为什么必须拒绝：契约 ApprovalRequest 的 session_id / agent_id 是
// **必填的合法 ULID**，填空串会产出违反自家契约的审批请求（ajv 校验 FAIL），
// 审批 UI/审计都拿不到归属信息。宁可当场拒绝（fail-close），也不发一个非法请求。
func Decide(ctx context.Context, e *Engine, a core.Approver, q Query) contracts.Decision {
	d := e.Evaluate(ctx, q)
	if d.Verdict != contracts.EffectAsk {
		return d
	}

	ruleID := ""
	if d.RuleID != nil {
		ruleID = *d.RuleID
	}

	// fail-close：没有审批人 = 拒绝，绝不挂起（H8/D14）。
	if a == nil {
		return denyNow(contracts.DecisionCodeApproverAbsent, fmt.Sprintf(
			"fail-close: 工具 %s 需要人审（规则 %s）但没有配置 Approver", q.Tool, ruleID))
	}
	// fail-close：上下文已取消/超时 = 无人应答 = 拒绝。
	if err := ctx.Err(); err != nil {
		return denyNow(contracts.DecisionCodeApprovalContextEnded, fmt.Sprintf(
			"fail-close: 审批前上下文已结束（%v），工具 %s（规则 %s）按拒绝处理", err, q.Tool, ruleID))
	}
	// fail-close：缺少 session/agent 上下文 → 无法构造合法的审批请求 → 直接拒绝，
	// **不构造 ApprovalRequest、不调用 Approver**。
	if missing := missingContext(q); missing != "" {
		return denyNow(contracts.DecisionCodeApprovalContextIncomplete, fmt.Sprintf(
			"fail-close: missing session/agent context（缺少 %s）→ 无法构造合法的审批请求，工具 %s（规则 %s）按拒绝处理",
			missing, q.Tool, ruleID))
	}

	req := contracts.ApprovalRequest{
		V:         1,
		ID:        core.NewUlid(),
		TS:        nowRFC3339(),
		SessionID: *q.SessionID,
		AgentID:   *q.AgentID,
		Trigger:   contracts.ApprovalTriggerPolicyAsk,
		Summary:   fmt.Sprintf("策略拦截：工具 %s 需要人审（规则 %s）", q.Tool, ruleID),
		Detail:    approvalDetail(q, d),
	}

	resp, err := approveSafely(ctx, a, req)
	if err != nil {
		// fail-close：审批服务出错/超时 = 拒绝（不是批准）。
		return denyNow(contracts.DecisionCodeApprovalFailed, fmt.Sprintf(
			"fail-close: 审批失败（%v），工具 %s（规则 %s）按拒绝处理", err, q.Tool, ruleID))
	}
	if resp.Decision != contracts.ApprovalDecisionAllow {
		note := "无备注"
		if resp.Note != nil && *resp.Note != "" {
			note = *resp.Note
		}
		return denyNow(contracts.DecisionCodeApprovalDenied, fmt.Sprintf(
			"fail-close: 人审未放行（%s），工具 %s（规则 %s）按拒绝处理", note, q.Tool, ruleID))
	}

	by := "approver"
	if resp.DecidedBy != nil && *resp.DecidedBy != "" {
		by = *resp.DecidedBy
	}
	return contracts.Decision{
		Verdict: contracts.EffectAllow,
		Code:    codePtr(contracts.DecisionCodeApprovalAllowed),
		RuleID:  d.RuleID,
		Reason:  strPtr(fmt.Sprintf("allow: 人审放行（%s）工具 %s（策略 ask 规则 %s）", by, q.Tool, ruleID)),
	}
}

// approvalDetail 组装审批请求的附加细节（契约允许携带 suggested_rules）。
func approvalDetail(q Query, d contracts.Decision) json.RawMessage {
	type detail struct {
		Tool           string                 `json:"tool"`
		RuleID         *string                `json:"rule_id,omitempty"`
		Args           json.RawMessage        `json:"args,omitempty"`
		SuggestedRules []contracts.PolicyRule `json:"suggested_rules,omitempty"`
	}
	b, err := json.Marshal(detail{Tool: q.Tool, RuleID: d.RuleID, Args: q.Args, SuggestedRules: d.SuggestedRules})
	if err != nil {
		return nil
	}
	return b
}

// suggestAllowRule 把命中的 ask 规则翻成一条建议 allow（D13：同 Tool/ArgsPattern/Source，Effect=allow）。
func suggestAllowRule(ask *compiledRule) contracts.PolicyRule {
	reason := fmt.Sprintf("suggested from ask rule %s", ask.id)
	if ask.raw.Reason != nil && *ask.raw.Reason != "" {
		reason = fmt.Sprintf("suggested from ask: %s", *ask.raw.Reason)
	}
	id := ask.id + ".allow"
	return contracts.PolicyRule{
		ID:          &id,
		Effect:      contracts.EffectAllow,
		Tool:        ask.raw.Tool,
		ArgsPattern: ask.raw.ArgsPattern,
		Source:      ask.raw.Source,
		Reason:      &reason,
	}
}

// newDecision 组装一条判定（RuleID/Reason/Code 必填，便于审计；code = rule_matched——
// deny/ask/allow 三种 verdict 都是"命中规则"的产物，区分见 verdict 字段）。
func newDecision(v contracts.Effect, r *compiledRule, reason string) contracts.Decision {
	id := r.id
	return contracts.Decision{
		Verdict: v,
		Code:    codePtr(contracts.DecisionCodeRuleMatched),
		RuleID:  &id,
		Reason:  strPtr(reason),
	}
}

// denyNow 是一条带理由与成因码的 deny 判定（fail-close 系出口的统一组装）。
func denyNow(code contracts.DecisionCode, reason string) contracts.Decision {
	return contracts.Decision{Verdict: contracts.EffectDeny, Code: codePtr(code), Reason: strPtr(reason)}
}

// failClosed 是无命中时的 fail-close 判定（H8：缺席默认 deny；含 nil 引擎/空工具名）。
func failClosed() contracts.Decision {
	return contracts.Decision{
		Verdict: contracts.EffectDeny,
		Code:    codePtr(contracts.DecisionCodePolicyDefault),
		Reason:  strPtr("fail-close: no matching rule"),
	}
}

// codePtr 取成因码地址（契约可选字段一律指针 + omitempty，D2）。
func codePtr(c contracts.DecisionCode) *contracts.DecisionCode { return &c }

// jsonStrings 递归收集 JSON 里的所有**字符串型参数值**（对象值/数组元素/顶层字符串）。
// map 遍历顺序随机，但调用方只做布尔 OR → 判定结果与顺序无关（确定性不受影响）。
func jsonStrings(data json.RawMessage) []string {
	if len(data) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return nil // 参数不是合法 JSON → 无字符串值可匹配 → 参数化规则不命中（fail-close 方向）
	}
	var out []string
	var walk func(x any)
	walk = func(x any) {
		switch t := x.(type) {
		case string:
			out = append(out, t)
		case []any:
			for _, e := range t {
				walk(e)
			}
		case map[string]any:
			for _, e := range t {
				walk(e)
			}
		}
	}
	walk(v)
	return out
}

// approveSafely 调用审批人并隔离 panic（与注册表的能力裁决同款失败隔离，E5）。
//
// 审批人缺席/崩溃 = 无人应答 = deny（H8/D14）：绝不让一个坏实现把 core 带崩，
// 也绝不把"没拿到答复"当成放行。
//
// 顺带覆盖"有类型的 nil 审批人"（如 var d *DenyAll；把 &d 传给 Decide）：
// 那不是接口 nil（a == nil 判不出来），调用会在方法里解引用 nil 而 panic ——
// 这里把它变成一次普通的失败 → deny。
func approveSafely(ctx context.Context, a core.Approver, req contracts.ApprovalRequest) (resp contracts.ApprovalResponse, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			resp = contracts.ApprovalResponse{}
			err = fmt.Errorf("审批人 panic: %v", rec)
		}
	}()
	return a.Approve(ctx, req)
}

// missingContext 返回缺失的上下文字段名（空 = 上下文完整）。
//
// 契约要求审批请求带 session_id / agent_id（必填 ULID），缺一不可：
// 缺失就构造不出合法请求（见 Decide 的 fail-close 分支）。
func missingContext(q Query) string {
	var missing []string
	if q.SessionID == nil {
		missing = append(missing, "session_id")
	}
	if q.AgentID == nil {
		missing = append(missing, "agent_id")
	}
	return strings.Join(missing, "/")
}

// nowRFC3339 返回 UTC 的 RFC 3339 时间戳（契约 ts 字段格式）。
func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }

// strPtr 取字符串地址（契约可选字段一律指针 + omitempty，D2）。
func strPtr(s string) *string { return &s }
