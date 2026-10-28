package host

import (
	"context"
	"fmt"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/indexer"
)

// 本文件是 token 成本统计（J21）的 hub 查询面：usage 只进成本，**绝不回填 context
// 快照/预算**（那是事前估算的正式口径）。数据源是索引器的 cost_tokens 派生表
// （G17：成本统计是索引器投影的既有职责），本层只做读取 + 折成 JSON 视图 + 等索引追平。
//
// 与 /api/sessions 同口径：先扫平内存投影拿水位，再有界等后台索引器追到该水位
// （刚跑完一轮时查成本就能看到最新 usage，而不是等下一轮）。

// ModelUsage 是单模型的 token 计量（透传索引器形状，供 JSON 序列化）。
type ModelUsage = indexer.ModelUsage

// TokenCostView 是单 session 的 token 成本视图（GET /api/cost/{session_id}）。
type TokenCostView struct {
	SessionID    string                `json:"session_id"`
	InputTokens  int64                 `json:"input_tokens"`
	OutputTokens int64                 `json:"output_tokens"`
	TotalTokens  int64                 `json:"total_tokens"`
	ModelTurns   int64                 `json:"model_turns"`
	ByModel      map[string]ModelUsage `json:"by_model"`
}

// TokenTotalView 是跨 session 的 token 成本汇总（GET /api/cost）。
type TokenTotalView struct {
	Sessions     int64                    `json:"sessions"`
	InputTokens  int64                    `json:"input_tokens"`
	OutputTokens int64                    `json:"output_tokens"`
	TotalTokens  int64                    `json:"total_tokens"`
	ModelTurns   int64                    `json:"model_turns"`
	ByModel      map[string]ModelUsage    `json:"by_model"`
	PerSession   map[string]TokenCostView `json:"per_session"`
}

// TokenCost 查某 session 的 token 成本（不存在 → ok=false：该 session 还没有带 usage 的轮）。
func (h *Hub) TokenCost(ctx context.Context, sessionID contracts.Ulid) (TokenCostView, bool, error) {
	if h == nil {
		return TokenCostView{}, false, fmt.Errorf("host: Hub 为 nil")
	}
	if !core.ValidUlid(sessionID) {
		return TokenCostView{}, false, fmt.Errorf("host: session_id 不是合法 ULID: %q", sessionID)
	}
	if err := h.catchUpProjection(ctx); err != nil {
		return TokenCostView{}, false, err
	}
	if _, err := h.waitIndexer(h.proj.scanWatermark(), indexerCatchUpWait); err != nil {
		return TokenCostView{}, false, fmt.Errorf("host: 等索引追平: %w", err)
	}
	ts, ok, err := h.indexer.ReadTokenStats(ctx, sessionID)
	if err != nil {
		return TokenCostView{}, false, fmt.Errorf("host: 读 token 成本: %w", err)
	}
	if !ok {
		return TokenCostView{}, false, nil
	}
	return tokenCostViewOf(string(sessionID), ts), true, nil
}

// TokenCostTotal 查跨 session 的 token 成本汇总（/api/cost 总额）。
func (h *Hub) TokenCostTotal(ctx context.Context) (TokenTotalView, error) {
	if h == nil {
		return TokenTotalView{}, fmt.Errorf("host: Hub 为 nil")
	}
	if err := h.catchUpProjection(ctx); err != nil {
		return TokenTotalView{}, err
	}
	if _, err := h.waitIndexer(h.proj.scanWatermark(), indexerCatchUpWait); err != nil {
		return TokenTotalView{}, fmt.Errorf("host: 等索引追平: %w", err)
	}
	all, err := h.indexer.ReadAllTokenStats(ctx)
	if err != nil {
		return TokenTotalView{}, fmt.Errorf("host: 读全部 token 成本: %w", err)
	}
	view := TokenTotalView{
		ByModel:    map[string]ModelUsage{},
		PerSession: map[string]TokenCostView{},
	}
	for sid, ts := range all {
		view.Sessions++
		view.InputTokens += ts.InputTokens
		view.OutputTokens += ts.OutputTokens
		view.ModelTurns += ts.ModelTurns
		for model, mu := range ts.ByModel {
			agg := view.ByModel[model]
			agg.InputTokens += mu.InputTokens
			agg.OutputTokens += mu.OutputTokens
			agg.Turns += mu.Turns
			view.ByModel[model] = agg
		}
		view.PerSession[string(sid)] = tokenCostViewOf(string(sid), ts)
	}
	view.TotalTokens = view.InputTokens + view.OutputTokens
	return view, nil
}

// catchUpProjection 扫平内存投影（拿全局水位供等索引用）；失败不致命时由调用方判。
func (h *Hub) catchUpProjection(ctx context.Context) error {
	if _, err := h.proj.catchUp(ctx); err != nil {
		return fmt.Errorf("host: 扫事件流: %w", err)
	}
	return nil
}

// tokenCostViewOf 把索引器的 TokenStats 折成 JSON 视图。
func tokenCostViewOf(sid string, ts indexer.TokenStats) TokenCostView {
	byModel := ts.ByModel
	if byModel == nil {
		byModel = map[string]ModelUsage{}
	}
	return TokenCostView{
		SessionID:    sid,
		InputTokens:  ts.InputTokens,
		OutputTokens: ts.OutputTokens,
		TotalTokens:  ts.InputTokens + ts.OutputTokens,
		ModelTurns:   ts.ModelTurns,
		ByModel:      byModel,
	}
}
