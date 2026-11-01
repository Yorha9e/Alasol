package host

import (
	"context"
	"fmt"

	"github.com/Yorha9e/alasol/contracts"
)

// 本文件是 W15 成本预算的**可选闸门**（v4-4-B 顺手小刀）：Config.BudgetMaxTokens > 0 时，
// hub 在每个 prompt 之前检查该 session 的累计 token 成本，超限就拒绝发起本轮。
//
// # 默认不设上限（W15 拍板）
//
// BudgetMaxTokens <= 0（默认）= 整个闸门不生效：checkBudget 直接返回 nil、一次查询都不发
//（连索引器都不碰）→ **零行为变化**（酸测试）。"长时程自主是卖点，预算不可控"是既定口径，
// 这个闸门是**可选**的刹车，不是默认行为。
//
// # 数据源与口径
//
// 数据源 = 索引器的 cost_tokens 派生表（J21：usage 只进成本，绝不回填 context 快照/预算）。
// 口径 = **input + output 累计**（TokenCostView.TotalTokens），跨轮跨 Run 累积（一个
// session 一份账）。读之前先 catchUp + 等索引追平（与 GET /api/cost/{id} 同口径），
// 否则"刚跑完一轮"查到的还是上一轮的账。
//
// # 诚实边界：per-Run 粒度，不是 turn 内逐轮刹车
//
// 检查点在 **Run 前 / Run 后**（一个 prompt = 一次 Run = 若干 turn）。一轮跑到一半越界时，
// **本轮会跑完**，下一轮才被拒。逐轮刹车要把成本面接进 loop（loop.Config 加预算 + 每轮读
// 累计），那是另一笔（挂账见 11 施工计划 v4-4 行）——这里不假装做到了。
//
// # 查不动怎么办（fail-open，有说明）
//
// 读成本失败（索引器卡了 / 库不可用）时 checkBudget **不拒绝本轮**：成本查询是观测面的
// 事，为它拒掉一次用户明确发起的对话，比"多烧一轮"更糟。但**告警一条**（看得见），
// 且 warnIfOverBudget 同理（查不动就静默跳过——已经跑完的事，告警也只是噪音）。

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

// warnIfOverBudget 在**一轮跑完之后**检查：越界就告警一条（本轮已经跑完，只能留痕 +
// 让下一轮被拒；见文件头的诚实边界）。
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
