package host

import (
	"context"
	"fmt"

	"github.com/Yorha9e/alasol/contracts"
)

// 本文件是 W15 成本预算的**可选闸门**（v4-4-B 顺手小刀；逐轮面 = 缺口 5 第二件）：
// Config.BudgetMaxTokens > 0 时，hub 在每个 prompt 之前检查该 session 的累计 token 成本，
// 超限就拒绝发起本轮；并把同一个检查接进 loop 的**轮间钩子**（loop.Config.TurnBudget）
// ——每轮 provider.Run 之前再查一次，中途越界的 Run 在下一轮被截断。
//
// # 默认不设上限（W15 拍板）
//
// BudgetMaxTokens <= 0（默认）= 整个闸门不生效：checkBudget 直接返回 nil、一次查询都不发
// （连索引器都不碰）→ **零行为变化**（酸测试）；turnBudgetFor 也返回 **nil 钩子**（loop
// 侧一轮都不查，零查询）。"长时程自主是卖点，预算不可控"是既定口径，
// 这个闸门是**可选**的刹车，不是默认行为。
//
// # 数据源与口径
//
// 数据源 = 索引器的 cost_tokens 派生表（J21：usage 只进成本，绝不回填 context 快照/预算）。
// 口径 = **input + output 累计**（TokenCostView.TotalTokens），跨轮跨 Run 积累（一个
// session 一份账）。读之前先 catchUp + 等索引追平（与 GET /api/cost/{id} 同口径），
// 否则"刚跑完一轮"查到的还是上一轮的账。
//
// # 两层检查（缺口 5 第二件兑现后）：per-Run + per-turn，互不冲突
//
//   - **per-Run**（本文件的 checkBudget / warnIfOverBudget）：RunPrompt 入口拒 + Run 后
//     越界告警 + fork 路径（RunBtw）拒——旁支是真实开销，仍只走这层；
//   - **per-turn**（turnBudgetFor → loop.Config.TurnBudget）：Run 内每轮 provider.Run
//     之前查一次累计——一轮把累计推过上限时，**下一轮不再发起**（Run 以
//     Result.TruncatedByBudget 收口，见 core/loop 的口径）。
//
// 两层读同一份账、口径一致：Run 前拒 + 轮间拒。per-Run 层保留（Run 前的拒绝不必先
// 进 loop；fork 没有轮间层也照样被拒）。
//
// **fork 的口径（如实标注，既有缺口）**：fork（`RunBtw`）**只查主 session 的账**——
// 旁支自身的花费记在 **fork session** 名下，不在本闸门口径内（旁支没有轮间层，也没有
// 按 fork session 记账的检查）。这是**既有缺口**，本刀不掩盖：主 session 的闸门照常生效，
// 旁支的花费只能事后在成本视图里看到。
//
// **成本口径（逐轮查询的代价，如实写明）**：轮间每轮**一次索引查询**（`TokenCost` →
// 投影增量扫描 + **≤250ms 有界等待**等索引追平）——一个 20 轮的 Run 最多因此多花**几秒**
// （每轮几十到几百毫秒量级，具体取决于索引追平耗时）。**默认关**（`BudgetMaxTokens <= 0`
// → 零查询、零钩子、零行为变化）；开了才付这份代价——这是"要刹车就得付查询钱"的取舍。
//
// # 查不动怎么办（fail-open，有说明）
//
// 读成本失败（索引器卡了 / 库不可用）时 checkBudget **不拒绝本轮**：成本查询是观测面的
// 事，为它拒掉一次用户明确发起的对话，比"多烧一轮"更糟。但**告警一条**（看得见），
// 且 warnIfOverBudget 同理（查不动就静默跳过——已经跑完的事，告警也只是噪音）。
// 轮间钩子复用同一个 checkBudget → **同一口径**：查询失败不拒本轮（钩子返 nil）但 warn 留痕。

// budgetMaxTokens 取生效的成本上限（<=0 = 不设上限 = 关）。
func (h *Hub) budgetMaxTokens() int64 {
	if h == nil {
		return 0
	}
	return h.cfg.BudgetMaxTokens
}

// checkBudget 在**发起一轮之前**检查该 session 的累计 token 成本（超限 → 拒绝）。
//
// 关（BudgetMaxTokens<=0）→ nil（零行为变化，一次查询都不发）。
func (h *Hub) checkBudget(ctx context.Context, sessionID contracts.Ulid) error {
	limit := h.budgetMaxTokens()
	if limit <= 0 {
		return nil // 默认不设上限（W15 拍板）
	}
	view, ok, err := h.TokenCost(ctx, sessionID)
	if err != nil {
		// fail-open（见文件头）：查询失败不拒本轮，但留痕。
		hubLog.Warn("host: 成本预算闸门查询失败（本轮放行；观测面故障不阻断对话）",
			"session_id", string(sessionID), "budget_max_tokens", limit, "err", err.Error())
		return nil
	}
	if !ok {
		return nil // 这个 session 还没有带 usage 的轮 → 没花过钱
	}
	if view.TotalTokens < limit {
		return nil
	}
	return fmt.Errorf("host: session %s 已达成本预算上限（累计 %d tokens ≥ 上限 %d）："+
		"拒绝发起新一轮（W15 预算闸门；上限经 host.Config.BudgetMaxTokens 调整，0 = 不设上限）",
		sessionID, view.TotalTokens, limit)
}

// turnBudgetFor 造**轮间**预算钩子（loop.Config.TurnBudget；缺口 5 第二件）。
//
// BudgetMaxTokens <= 0（默认）→ **nil 钩子**：loop 一轮都不查（零查询、零行为变化，
// 与 checkBudget 的"关 = 一次查询都不发"同口径）。>0 → 闭包每轮调一次 checkBudget
// （fail-open 同款：查询失败不拒本轮，但 warn 留痕；超限 → 返错 → loop 截断本轮）。
//
// 钩子在 **loop 装配期定格**（loopFor 建 loop 时取一次，与 Model/MaxTurns 等装配期字段
// 同款）：装配后改 BudgetMaxTokens 不影响已建的 loop（重建 loop / 重启才生效）。
// 装配期读 budgetMaxTokens() 正是为了"关 = 连闭包都不建"的零查询口径。
func (h *Hub) turnBudgetFor(sessionID contracts.Ulid) func(context.Context) error {
	if h.budgetMaxTokens() <= 0 {
		return nil // 默认不设上限（W15 拍板）：钩子为 nil = 零查询、零行为变化
	}
	return func(ctx context.Context) error {
		return h.checkBudget(ctx, sessionID)
	}
}

// warnIfOverBudget 在**一轮跑完之后**检查：越界就告警一条（本轮已经跑完，只能留痕 +
// 让下一轮被拒——per-Run 层的 Run 后告警；轮间层的截断由 loop 的 TurnBudget 负责，
// 见文件头"两层检查"）。
func (h *Hub) warnIfOverBudget(ctx context.Context, sessionID contracts.Ulid) {
	limit := h.budgetMaxTokens()
	if limit <= 0 {
		return
	}
	view, ok, err := h.TokenCost(ctx, sessionID)
	if err != nil || !ok {
		return // 查不动就静默（已经跑完的事，告警也只是噪音）
	}
	if view.TotalTokens >= limit {
		hubLog.Warn("host: session 已达/超过成本预算上限（本轮已跑完；下一轮将被拒）",
			"session_id", string(sessionID), "total_tokens", view.TotalTokens, "budget_max_tokens", limit)
	}
}
