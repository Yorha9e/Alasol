package host

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	coregate "github.com/Yorha9e/alasol/core/gate"
	coregraph "github.com/Yorha9e/alasol/core/graph"
)

// 本文件把 v2-S3 的**门禁能力面**接进合并门禁流程（v3-S3 清 V6/V7 挂账）：
//
//   - V6 review-tip 失效：review 结论记录（review.recorded 发射）→ 合并前查 Approved
//     （tip 变则失效要求重审）→ "review 后又推一行"被拦下要求重审；
//   - V7 合并后冲突预测：合并成功后跑 PredictConflicts（对未合并分支预警）+ 发射
//     conflict.predicted。
//
// # 形态：core 能力接口 + host 装配（HTTP/CLI 都能接）
//
// 与 change_impact 同款口径（见 host/graph.go 的文件头）：这些是 host 的 Go API +
// HTTP 端点，不是 loop 框架工具——它们是一次性的协调动作（记 review / 判合并 / 预测冲突），
// 由协调人或 CLI 驱动，不进 agent 的工具可见集。core/gate 与 core/graph 只给纯逻辑
// （无 I/O），本文件负责读事件流、发射事件、接图引擎。
//
// # tip 观测：调用方喂（与 gate.Verify 同口径）
//
// 当前分支 tip 由**调用方**经 git 观测后作为参数喂入（rev-parse，与 runtime/verify 的
// anchor 核验同源）——host 不自己跑 git（那要把 git 依赖和 cwd 语义拉进 host）。被 review
// 的 tip 在 review.recorded 事件里（core/gate V6），合并门禁拿"事件里记的 tip" vs "调用方
// 喂的当前 tip"比对判定失效。
//
// # 重试接线（v4-4-A，07 M6/M7）：**不接，挂账**
//
// M6 说"CI 检查也走 core/retry"，但本文件的形态接不上，理由（不是懒，是形状不对）：
//
//   - 这里的三个操作都是"**读事件流判定** + **发射一条事件**"（RecordReview /
//     CheckMerge / PredictPostMergeConflicts）。读面失败（store 错误）不是瞬时网络错误，
//     而是库坏/句柄错——重试同一读三遍只会把同一个错报三遍；
//   - 发射面（bus.Publish）有**副作用**（事件只增、不可重复消费），且未声明幂等 → 按 M7
//     "只有声明幂等的操作才自动重试"**不可**自动重试；
//   - 真正的"CI 检查"在 `runtime/gate.RunWith`：它跑一条不透明 shell 命令并产出一条
//     receipt。那里重试 = 同一条命令跑两遍 = 两条 receipt（门禁语义从"一次门禁一次事实"
//     变成"多次取最后一次"），且 CI 命令本身是否幂等由写命令的人决定、框架无从判定。
//     要接得先定"一条门禁允许多少次运行"的语义，超出本刀范围。
//
// 结论：v4-4-A 只在 `runtime/provider/openai` 落 provider 级小重试（模型 API 429/5xx，
// 07 拍板可重试的那一类），本文件与 runtime/gate 留作挂账；core/retry 的能力面已就位
// （Do + Policy + 聚合错误），后续接线不必改框架。

// RecordReview 记录一条分支 review 结论（V6）：发射一条 review.recorded durable 事件。
//
// reviewer 身份**不进 payload**（V9：无独立 reviewer，结论来自 agent 树角色）——由事件的
// envelope.agent_id 承载（调用方传入的 agentID）。分支名原样落 payload，归一见读取侧
// （core/gate.NormalizeBranch）。
//
// 事件经 bus 发射（WS 订阅者可见 + 入库，F7 只增）。返回落库后的事件（Seq 已填）。
func (h *Hub) RecordReview(ctx context.Context, sessionID, agentID contracts.Ulid, r coregate.Review) error {
	if h == nil {
		return fmt.Errorf("host: Hub 为 nil")
	}
	if err := h.ensureOpen(); err != nil {
		return err
	}
	payload, err := json.Marshal(coregate.ReviewRecordedEvent(r))
	if err != nil {
		return fmt.Errorf("host: 编 review.recorded 载荷: %w", err)
	}
	ev := contracts.Event{
		V:         1,
		ID:        core.NewUlid(),
		Kind:      coregate.KindReviewRecorded,
		TS:        time.Now().UTC().Format(time.RFC3339),
		SessionID: sessionID,
		AgentID:   agentID,
		Durable:   true,
		Payload:   payload,
	}
	if err := h.bus.Publish(ctx, ev); err != nil {
		return fmt.Errorf("host: 发射 review.recorded 事件: %w", err)
	}
	return nil
}

// MergeVerdict 是合并门禁的判定结论（V6）。
type MergeVerdict struct {
	// Allowed 是否允许合并（Approved = 有记录 ∧ approved ∧ 未失效）。
	Allowed bool `json:"allowed"`
	// ReReviewRequired 是否**要求重审**（从未审过 / tip 变了失效 / 观测不到 tip 的 fail-close）。
	ReReviewRequired bool `json:"re_review_required"`
	// HasReview 是否有该分支的 review 记录。
	HasReview bool `json:"has_review"`
	// Conclusion 最新 review 结论（approved / changes_requested；无记录时为空）。
	Conclusion string `json:"conclusion,omitempty"`
	// Reason 人读说明（为什么允许 / 为什么拦下）。
	Reason string `json:"reason"`
}

// CheckMerge 合并门禁判定（V6 端到端）：给定 session 的事件流、分支、**当前** tip，
// 是否允许合并。
//
// 判定走 core/gate 的组合谓词（有记录 ∧ approved ∧ 未失效）+ ReReviewRequired：
//
//   - 从未 review → ReReviewRequired=true，Allowed=false（首次必审）；
//   - review 后**又推一行**（当前 tip ≠ 被 review 的 tip）→ 结论失效 → ReReviewRequired=true，
//     Allowed=false（**这正是 V6 要拦的**：不许拿旧批准放行）；
//   - approved 且 tip 未变 → Allowed=true；
//   - changes_requested → Allowed=false（没批）。
//
// **读面是缓存的**（p3 修：全量读事件流是 O(n)）：review 结论按 session 缓存，靠一次
// **便宜的探测读**精确判失效（见 reviewsFor 的说明）——长会话里反复查合并门禁不再每次
// 全量扫事件流。判定语义仍由 core/gate 给（本文件不自己拼）。
func (h *Hub) CheckMerge(ctx context.Context, sessionID contracts.Ulid, branch, currentTip string) (MergeVerdict, error) {
	if h == nil {
		return MergeVerdict{}, fmt.Errorf("host: Hub 为 nil")
	}
	if err := h.ensureOpen(); err != nil {
		return MergeVerdict{}, err
	}
	reviews, err := h.reviewsFor(ctx, sessionID)
	if err != nil {
		return MergeVerdict{}, err
	}
	r, has := reviews[coregate.NormalizeBranch(branch)]
	v := MergeVerdict{
		HasReview:        has,
		ReReviewRequired: coregate.ReReviewRequiredReview(r, has, currentTip),
		Allowed:          coregate.ApprovedReview(r, has, currentTip),
	}
	if has {
		v.Conclusion = string(r.Conclusion)
	}
	switch {
	case v.Allowed:
		v.Reason = "review approved 且分支 tip 未变：允许合并"
	case v.ReReviewRequired && !has:
		v.Reason = "该分支从未 review：要求先审（首次必审）"
	case v.ReReviewRequired:
		v.Reason = fmt.Sprintf("review 后分支 tip 又变（被 review 的 tip=%s，当前 tip=%s）→ 结论失效：要求重审", r.TipCommit, currentTip)
	case v.Conclusion == string(contracts.ReviewConclusionChangesRequested):
		v.Reason = "最新 review 结论是 changes_requested：未批准，不能合并"
	default:
		v.Reason = "不满足合并条件"
	}
	return v, nil
}

// mergeReviewCache 是某 session 的 review 结论缓存（V6 合并门禁读面）。
//
// lastSeq 是**上次 fold 时该 session 的最大 seq**（探测读的起点）。
type mergeReviewCache struct {
	lastSeq int64
	reviews map[string]coregate.Review // 归一后的分支名 → 最新结论
}

// reviewsFor 取某 session 的 review 结论（按分支归一）。
//
// # 缓存与**精确**失效（p3 修：全量读事件流 O(n)）
//
// 此前每次判定都全量读该 session 的事件流再 fold——事件流越长越慢。缓存的失效判定
// **不是**"过一会儿就过期"（那会漏掉新的 review 结论 → 拿旧批准放行 = fail-open），
// 而是**精确**的：
//
//  1. 命中缓存 → 一次**便宜的探测读**：ReadFrom(session, lastSeq+1, limit=1)
//     （走 (session_id, seq) 索引，O(log n)）问"这个 session 之后还有新事件吗"；
//     没有 → 事件集未变 → 缓存的结论仍然成立（直接用，零全量读）；
//  2. 有新事件（或没有缓存）→ 全量重读 + 重新 fold（结论可能变了）。
//
// 口径：缓存的是**review 记录**（分支 → 最新结论），判定语义由 core/gate 给
// （ApprovedReview / ReReviewRequiredReview）——本文件不自己拼判定。
func (h *Hub) reviewsFor(ctx context.Context, sessionID contracts.Ulid) (map[string]coregate.Review, error) {
	// ① 命中缓存 → 探测读（便宜）：该 session 没有新事件 → 缓存仍成立。
	h.mu.Lock()
	cache, ok := h.mergeCache[sessionID]
	h.mu.Unlock()
	if ok {
		fresh, err := h.sessionUnchanged(ctx, sessionID, cache.lastSeq)
		if err != nil {
			return nil, err
		}
		if fresh {
			return cache.reviews, nil
		}
	}
	// ② 全量读 + fold（结论可能变了）。
	events, err := h.store.ReadFrom(ctx, &sessionID, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("host: 读 session %s 事件流: %w", sessionID, err)
	}
	reviews := coregate.FoldReviews(events)
	var lastSeq int64
	for i := range events {
		if events[i].Seq != nil && *events[i].Seq > lastSeq {
			lastSeq = *events[i].Seq
		}
	}
	h.mu.Lock()
	if h.mergeCache == nil {
		h.mergeCache = make(map[contracts.Ulid]*mergeReviewCache)
	}
	h.mergeCache[sessionID] = &mergeReviewCache{lastSeq: lastSeq, reviews: reviews}
	h.mu.Unlock()
	return reviews, nil
}

// sessionUnchanged 报告某 session 在 lastSeq 之后**有没有新事件**（便宜的探测读）。
//
// ReadFrom(session, lastSeq+1, limit=1) 走 (session_id, seq) 索引 → O(log n)；
// 返回空 = 该 session 的事件集没变（缓存仍成立）。
func (h *Hub) sessionUnchanged(ctx context.Context, sessionID contracts.Ulid, lastSeq int64) (bool, error) {
	events, err := h.store.ReadFrom(ctx, &sessionID, lastSeq+1, 1)
	if err != nil {
		return false, fmt.Errorf("host: 探测 session %s 是否有新事件: %w", sessionID, err)
	}
	return len(events) == 0, nil
}

// PredictPostMergeConflicts 合并后冲突预测（V7）：对刚合并的改动 vs 未合并分支，用依赖图
// 可达性预判撞车，并发射一条 conflict.predicted durable 事件（消费端就位）。
//
// kind 是追影响面的边类型（生产侧应选依赖类，如 "imports"；"" = 不限 kind，会放大命中面）。
// 图投影未启用（Config.Graph=false）→ reach=nil → PredictConflicts 诚实降级为
// "预测不可用"（ReasonGraphDisabled），**绝不**报"无冲突"。
//
// **N2 联动**：图投影正在重建时 GraphReachable fail-close 报 ErrRebuilding → reach 出错 →
// PredictConflicts 降级为 ReasonReachFailed（"预测不可用"），同样不报"无冲突"。
func (h *Hub) PredictPostMergeConflicts(ctx context.Context, sessionID, agentID contracts.Ulid, kind string, merged coregraph.ChangeSet, others []coregraph.ChangeSet) (coregraph.ConflictReport, error) {
	if h == nil {
		return coregraph.ConflictReport{}, fmt.Errorf("host: Hub 为 nil")
	}
	if err := h.ensureOpen(); err != nil {
		return coregraph.ConflictReport{}, err
	}
	var reach coregraph.ConflictReach
	if h.graphEngine != nil {
		reach = func(rctx context.Context, roots []string) (coregraph.Closure, error) {
			return h.GraphReachable(rctx, coregraph.ReachQuery{
				Sources:   roots,
				Kind:      kind,
				Direction: coregraph.DirectionIn, // 影响面 = 谁依赖我（V7 口径）
			})
		}
	}
	rep := coregraph.PredictConflicts(ctx, reach, merged, others)

	// 发射 conflict.predicted（V7 消费端就位；经 bus，WS 可见 + 入库）。
	payload, err := json.Marshal(coregraph.ConflictPredictedEvent(rep))
	if err != nil {
		return rep, fmt.Errorf("host: 编 conflict.predicted 载荷: %w", err)
	}
	ev := contracts.Event{
		V:         1,
		ID:        core.NewUlid(),
		Kind:      coregraph.KindConflictPredicted,
		TS:        time.Now().UTC().Format(time.RFC3339),
		SessionID: sessionID,
		AgentID:   agentID,
		Durable:   true,
		Payload:   payload,
	}
	if err := h.bus.Publish(ctx, ev); err != nil {
		return rep, fmt.Errorf("host: 发射 conflict.predicted 事件: %w", err)
	}
	return rep, nil
}

// ensureOpen 报告 hub 是否已关（关后不再接受合并门禁操作）。
func (h *Hub) ensureOpen() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fmt.Errorf("host: Hub 已关闭")
	}
	return nil
}

// ---------------------------------------------------------------------------
// HTTP：合并门禁端点（V6/V7 消费端就位；协调人 / CLI 可接）
// ---------------------------------------------------------------------------

// maxMergeGateBody 限制合并门禁 POST 请求体大小（loopback 上也别裸奔）。
const maxMergeGateBody = 64 << 10 // 64KB

// decodeMergeBody 解一个合并门禁 POST 请求体（有界读；非法 → 400 并返回 false）。
func decodeMergeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxMergeGateBody))
	if err := dec.Decode(v); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_body", "请求体不是合法 JSON 对象: "+err.Error())
		return false
	}
	return true
}

// serveMergeCheck 服务 POST /api/merge/check（V6：合并前查是否放行）。
//
//	body: {"session_id":"...","branch":"feature/x","current_tip":"abc123"}
//	200  → MergeVerdict（allowed / re_review_required / reason）
//	400  → 体非法 / 缺字段
func (h *Hub) serveMergeCheck(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SessionID  contracts.Ulid `json:"session_id"`
		Branch     string         `json:"branch"`
		CurrentTip string         `json:"current_tip"`
	}
	if !decodeMergeBody(w, r, &req) {
		return
	}
	if req.SessionID == "" || !core.ValidUlid(req.SessionID) {
		writeJSONError(w, http.StatusBadRequest, "bad_session", "session_id 必填且合法（ULID）")
		return
	}
	if req.Branch == "" {
		writeJSONError(w, http.StatusBadRequest, "bad_branch", "branch 必填（被合并的分支名）")
		return
	}
	if req.CurrentTip == "" {
		writeJSONError(w, http.StatusBadRequest, "bad_tip", "current_tip 必填（调用方经 git 观测的当前分支 tip）")
		return
	}
	v, err := h.CheckMerge(r.Context(), req.SessionID, req.Branch, req.CurrentTip)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "merge_check_failed", err.Error())
		return
	}
	writeJSONBody(w, http.StatusOK, v)
}

// serveMergeReview 服务 POST /api/merge/review（V6：记录一条分支 review 结论）。
//
//	body: {"session_id":"...","agent_id":"...","branch":"feature/x","tip_commit":"abc123","conclusion":"approved"}
//	200  → {"recorded":true}
//	400  → 体非法 / 缺字段
func (h *Hub) serveMergeReview(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SessionID  contracts.Ulid             `json:"session_id"`
		AgentID    contracts.Ulid             `json:"agent_id"`
		Branch     string                     `json:"branch"`
		TipCommit  string                     `json:"tip_commit"`
		Conclusion contracts.ReviewConclusion `json:"conclusion"`
	}
	if !decodeMergeBody(w, r, &req) {
		return
	}
	if req.SessionID == "" || !core.ValidUlid(req.SessionID) {
		writeJSONError(w, http.StatusBadRequest, "bad_session", "session_id 必填且合法（ULID）")
		return
	}
	if req.AgentID == "" || !core.ValidUlid(req.AgentID) {
		writeJSONError(w, http.StatusBadRequest, "bad_agent", "agent_id 必填且合法（ULID，reviewer 身份由 envelope 承载）")
		return
	}
	if req.Branch == "" || req.TipCommit == "" {
		writeJSONError(w, http.StatusBadRequest, "bad_review", "branch 与 tip_commit 必填（tip 是 V6 失效判定基准）")
		return
	}
	switch req.Conclusion {
	case contracts.ReviewConclusionApproved, contracts.ReviewConclusionChangesRequested:
	default:
		writeJSONError(w, http.StatusBadRequest, "bad_conclusion", "conclusion 只支持 approved / changes_requested")
		return
	}
	err := h.RecordReview(r.Context(), req.SessionID, req.AgentID, coregate.Review{
		Branch:     req.Branch,
		TipCommit:  req.TipCommit,
		Conclusion: req.Conclusion,
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "review_failed", err.Error())
		return
	}
	writeJSONBody(w, http.StatusOK, map[string]any{"recorded": true})
}

// serveMergePredict 服务 POST /api/merge/predict（V7：合并后冲突预测）。
//
//	body: {"session_id":"...","agent_id":"...","kind":"imports",
//	      "merged":{"label":"feature/x","roots":["core/store.go"]},
//	      "others":[{"label":"feature/y","roots":["core/store.go","cli/main.go"]}]}
//	200  → ConflictReport（available / collisions / unevaluated / risks[]）
//	400  → 体非法 / 缺字段
func (h *Hub) serveMergePredict(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SessionID contracts.Ulid        `json:"session_id"`
		AgentID   contracts.Ulid        `json:"agent_id"`
		Kind      string                `json:"kind"`
		Merged    coregraph.ChangeSet   `json:"merged"`
		Others    []coregraph.ChangeSet `json:"others"`
	}
	if !decodeMergeBody(w, r, &req) {
		return
	}
	if req.SessionID == "" || !core.ValidUlid(req.SessionID) {
		writeJSONError(w, http.StatusBadRequest, "bad_session", "session_id 必填且合法（ULID）")
		return
	}
	if req.AgentID == "" || !core.ValidUlid(req.AgentID) {
		writeJSONError(w, http.StatusBadRequest, "bad_agent", "agent_id 必填且合法（ULID）")
		return
	}
	if req.Merged.Label == "" {
		writeJSONError(w, http.StatusBadRequest, "bad_merged", "merged.label 必填（本次合并的分支标识）")
		return
	}
	rep, err := h.PredictPostMergeConflicts(r.Context(), req.SessionID, req.AgentID, req.Kind, req.Merged, req.Others)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "predict_failed", err.Error())
		return
	}
	// 空切片规范成 []（JSON 不出 null）。
	if rep.Risks == nil {
		rep.Risks = []coregraph.ConflictRisk{}
	}
	writeJSONBody(w, http.StatusOK, rep)
}
