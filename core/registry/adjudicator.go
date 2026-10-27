package registry

import (
	"context"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/policy"
)

// Adjudicator 是能力裁决器（E3）：把一个 capability 字符串裁决成 contracts.Decision。
//
// 进程内接口（D1：不进跨边界契约）。默认实现是 PolicyAdjudicator（包一层 policy.Engine），
// 因此能力规则与工具规则是**同一套求值**——规则可以直接写
// contracts.PolicyRule{Tool: "fs:write", Effect: contracts.EffectDeny}。
type Adjudicator interface {
	Adjudicate(ctx context.Context, capability string) contracts.Decision
}

// PolicyAdjudicator 把 capability 当"工具名"交给策略引擎求值（E3）。
//
// fail-close（H8）：引擎为 nil 或规则集为空 → 无 allow 命中 → deny，
// 与 policy.Engine.Evaluate 的缺席默认保持一致。
type PolicyAdjudicator struct {
	engine *policy.Engine
}

// NewPolicyAdjudicator 用给定策略引擎构造能力裁决器。
//
// engine 为 nil 时用**空引擎兜底**：没有任何 allow 规则 → 一切能力被拒（fail-close，H8）。
//
// nil 语义口径（三处统一，见 core/doc.go 的"nil 语义"一节）：
//   - **兜底**（本处）：engine=nil → 换成空引擎 → 一切能力被拒（fail-close）；
//   - **编程错误**（bus.New）：store=nil → 直接 panic。
//
// 判据：参数缺失后系统仍能安全地做"拒绝"→ 兜底；否则 panic。
func NewPolicyAdjudicator(engine *policy.Engine) *PolicyAdjudicator {
	if engine == nil {
		engine = policy.New()
	}
	return &PolicyAdjudicator{engine: engine}
}

// Engine 返回底层策略引擎（测试/装配观测用）。
func (a *PolicyAdjudicator) Engine() *policy.Engine {
	if a == nil {
		return nil
	}
	return a.engine
}

// Adjudicate implements Adjudicator。
func (a *PolicyAdjudicator) Adjudicate(ctx context.Context, capability string) contracts.Decision {
	if a == nil || a.engine == nil {
		return contracts.Decision{Verdict: contracts.EffectDeny, Reason: strPtr("fail-close: 无策略引擎")}
	}
	if capability == "" {
		// 空能力名无法被任何规则命中（空 Tool 规则在 New 时即被拒绝）→ deny。
		return contracts.Decision{Verdict: contracts.EffectDeny, Reason: strPtr("fail-close: 能力名为空")}
	}
	return a.engine.Evaluate(ctx, policy.Query{Tool: capability})
}
