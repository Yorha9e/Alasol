package sessioncfg_test

// core/sessioncfg 的 **上下文装配面**（G-h）折叠口径：
//
//	① **子字段各自 LWW**：context 组里的每个子项（kind / window / l1_pct / l2_pct /
//	   rules_budget / common_sense_budget）各带一个戳——后写的子项只覆盖自己那一项，
//	   没带的子项沿用上一次的值（**不学 tools 的整组 LWW**：整组 LWW 下"只带一项的
//	   提交"会把别的项静默打回未设置）；
//	② **子项三态**：未写过 / 显式 0（数值）或空串（kind）= 清除回默认 / 显式非零 = 覆盖；
//	③ 乱序容忍：旧事件（seq 小）盖不掉新事件的子项；重复投递幂等。

import (
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/sessioncfg"
)

func i64(v int64) *int64 { return &v }

func f64(v float64) *float64 { return &v }

// TestContextSubFieldLWW：子字段各自 LWW——后写的一组只覆盖**它带的那些**子项，
// 没带的子项沿用上一次的值（不静默丢字段）。
//
//	E1: context{kind:tiered, window:500000}
//	E2: context{l1_pct:20}                ← 只带 l1_pct
//	→ 结果：kind=tiered、window=500000 **仍在**，l1_pct=20。
func TestContextSubFieldLWW(t *testing.T) {
	tab := sessioncfg.New()

	tab.Apply(ev(t, "E1", 1, "2026-11-05T10:00:01Z", contracts.ConfigChangedPayload{
		Context: &contracts.ContextConfigChange{
			Kind:   str("tiered"),
			Window: i64(500_000),
		},
	}))
	tab.Apply(ev(t, "E2", 2, "2026-11-05T10:00:02Z", contracts.ConfigChangedPayload{
		Context: &contracts.ContextConfigChange{L1Pct: f64(20)},
	}))

	got := tab.Current("S1").Context
	if !got.Touched {
		t.Fatalf("context 出现过应 Touched=true: %+v", got)
	}
	if !got.Kind.HasValue() || got.Kind.Value != "tiered" {
		t.Fatalf("kind 应仍是 tiered（子字段各自 LWW）: %+v", got.Kind)
	}
	if !got.Window.HasValue() || got.Window.Value != 500_000 {
		t.Fatalf("window 应仍是 500000（子字段各自 LWW）: %+v", got.Window)
	}
	if !got.L1Pct.HasValue() || got.L1Pct.Value != 20 {
		t.Fatalf("l1_pct 应被覆盖成 20: %+v", got.L1Pct)
	}
	// 没带过的子项 = 未设置。
	if got.L2Pct.Written || got.RulesBudget.Written || got.CommonSenseBudget.Written {
		t.Fatalf("没带过的子项应未设置: %+v", got)
	}
}

// TestContextNumericTriState：数值子项三态——未写过 / 显式 0（清除回默认）/ 显式非零。
//
//	0 **不是**"窗口为 0"，而是"清除该项、回 hub 默认"（沿用 host.Config 的 0 = 默认口径）。
func TestContextNumericTriState(t *testing.T) {
	tab := sessioncfg.New()

	// ① 覆盖 window = 500000。
	tab.Apply(ev(t, "E1", 1, "2026-11-05T10:00:01Z", contracts.ConfigChangedPayload{
		Context: &contracts.ContextConfigChange{Window: i64(500_000)},
	}))
	got := tab.Current("S1").Context
	if !got.Window.HasValue() || got.Window.Value != 500_000 {
		t.Fatalf("window 应被覆盖成 500000: %+v", got.Window)
	}

	// ② window = 0 → 显式清除（Written=true、Value=0、生效上回默认）。
	tab.Apply(ev(t, "E2", 2, "2026-11-05T10:00:02Z", contracts.ConfigChangedPayload{
		Context: &contracts.ContextConfigChange{Window: i64(0)},
	}))
	got = tab.Current("S1").Context
	if !got.Window.Written {
		t.Fatalf("window=0 是显式清除记录（Written 应为 true）: %+v", got.Window)
	}
	if got.Window.HasValue() {
		t.Fatalf("window=0 不该是生效覆盖（生效值回默认）: %+v", got.Window)
	}

	// ③ 百分比同理（浮点三态）。
	tab.Apply(ev(t, "E3", 3, "2026-11-05T10:00:03Z", contracts.ConfigChangedPayload{
		Context: &contracts.ContextConfigChange{L2Pct: f64(0.5)},
	}))
	got = tab.Current("S1").Context
	if !got.L2Pct.HasValue() || got.L2Pct.Value != 0.5 {
		t.Fatalf("l2_pct 应被覆盖成 0.5: %+v", got.L2Pct)
	}
	tab.Apply(ev(t, "E4", 4, "2026-11-05T10:00:04Z", contracts.ConfigChangedPayload{
		Context: &contracts.ContextConfigChange{L2Pct: f64(0)},
	}))
	got = tab.Current("S1").Context
	if !got.L2Pct.Written || got.L2Pct.HasValue() {
		t.Fatalf("l2_pct=0 应是显式清除: %+v", got.L2Pct)
	}
}

// TestContextKindTriState：kind 是**字符串**三态——未写过 / 空串（清除回默认）/ 非空（覆盖）。
func TestContextKindTriState(t *testing.T) {
	tab := sessioncfg.New()

	tab.Apply(ev(t, "E1", 1, "2026-11-05T10:00:01Z", contracts.ConfigChangedPayload{
		Context: &contracts.ContextConfigChange{Kind: str("tiered")},
	}))
	got := tab.Current("S1").Context
	if !got.Kind.HasValue() || got.Kind.Value != "tiered" {
		t.Fatalf("kind 应被覆盖成 tiered: %+v", got.Kind)
	}

	// 空串 = 清除回默认（Written=true、Value=""、生效上回默认）。
	tab.Apply(ev(t, "E2", 2, "2026-11-05T10:00:02Z", contracts.ConfigChangedPayload{
		Context: &contracts.ContextConfigChange{Kind: str("")},
	}))
	got = tab.Current("S1").Context
	if !got.Kind.Written {
		t.Fatalf("kind 空串是显式清除记录（Written 应为 true）: %+v", got.Kind)
	}
	if got.Kind.HasValue() {
		t.Fatalf("kind 空串不该是生效覆盖: %+v", got.Kind)
	}
}

// TestContextAbsentKeepsState：context 组缺失 = 整组不变（"缺失 = 不变"）。
func TestContextAbsentKeepsState(t *testing.T) {
	tab := sessioncfg.New()

	tab.Apply(ev(t, "E1", 1, "2026-11-05T10:00:01Z", contracts.ConfigChangedPayload{
		Context: &contracts.ContextConfigChange{Kind: str("tiered"), Window: i64(500_000)},
	}))
	// 后一条事件只改 model，**不带 context** → context 面不动。
	tab.Apply(ev(t, "E2", 2, "2026-11-05T10:00:02Z", contracts.ConfigChangedPayload{
		Model: str("m2"),
	}))
	got := tab.Current("S1").Context
	if !got.Kind.HasValue() || got.Kind.Value != "tiered" {
		t.Fatalf("kind 不该被不带 context 的事件改动: %+v", got.Kind)
	}
	if !got.Window.HasValue() || got.Window.Value != 500_000 {
		t.Fatalf("window 不该被不带 context 的事件改动: %+v", got.Window)
	}
}

// TestContextOutOfOrderKeepsNewest：乱序容忍——旧事件（seq 小）盖不掉新事件的子项。
func TestContextOutOfOrderKeepsNewest(t *testing.T) {
	tab := sessioncfg.New()

	tab.Apply(ev(t, "E2", 2, "2026-11-05T10:00:02Z", contracts.ConfigChangedPayload{
		Context: &contracts.ContextConfigChange{Window: i64(900_000)},
	}))
	tab.Apply(ev(t, "E1", 1, "2026-11-05T10:00:01Z", contracts.ConfigChangedPayload{
		Context: &contracts.ContextConfigChange{Window: i64(100_000)},
	}))
	got := tab.Current("S1").Context
	if !got.Window.HasValue() || got.Window.Value != 900_000 {
		t.Fatalf("旧事件不该盖掉新事件的 window: %+v", got.Window)
	}
}

// TestContextDuplicateApplyIdempotent：同一条事件重复投递 → 结果不变（幂等）。
func TestContextDuplicateApplyIdempotent(t *testing.T) {
	tab := sessioncfg.New()
	payload := contracts.ConfigChangedPayload{
		Context: &contracts.ContextConfigChange{
			Kind:              str("tiered"),
			Window:            i64(500_000),
			L1Pct:             f64(8),
			L2Pct:             f64(0.5),
			RulesBudget:       i64(20_000),
			CommonSenseBudget: i64(30_000),
		},
	}
	tab.Apply(ev(t, "E1", 1, "2026-11-05T10:00:01Z", payload))
	tab.Apply(ev(t, "E1", 1, "2026-11-05T10:00:01Z", payload))
	got := tab.Current("S1").Context
	if !got.Kind.HasValue() || got.Kind.Value != "tiered" {
		t.Fatalf("重复投递后 kind 漂移: %+v", got.Kind)
	}
	if !got.Window.HasValue() || got.Window.Value != 500_000 {
		t.Fatalf("重复投递后 window 漂移: %+v", got.Window)
	}
	if !got.L1Pct.HasValue() || got.L1Pct.Value != 8 {
		t.Fatalf("重复投递后 l1_pct 漂移: %+v", got.L1Pct)
	}
	if !got.RulesBudget.HasValue() || got.RulesBudget.Value != 20_000 {
		t.Fatalf("重复投递后 rules_budget 漂移: %+v", got.RulesBudget)
	}
	if !got.CommonSenseBudget.HasValue() || got.CommonSenseBudget.Value != 30_000 {
		t.Fatalf("重复投递后 common_sense_budget 漂移: %+v", got.CommonSenseBudget)
	}
}
