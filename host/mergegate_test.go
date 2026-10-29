package host_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	coregate "github.com/Yorha9e/alasol/core/gate"
	coregraph "github.com/Yorha9e/alasol/core/graph"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// ---------------------------------------------------------------------------
// V6 端到端：review-tip 失效（"review 后又推一行"被拦下要求重审）
// ---------------------------------------------------------------------------

// TestMergeGateV6ReReviewRequired 是 V6 的端到端酸测试：
// 在 tip A review 通过 → 又推一行（当前 tip=B）→ 合并门禁拦下要求重审；
// 在 tip B 重新 review 通过 → 放行。
func TestMergeGateV6ReReviewRequired(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	sid := host.NewSession()
	agent := core.NewUlid()

	const (
		branch = "feature/x"
		tipA   = "commitAAAAAAAAAA"
		tipB   = "commitBBBBBBBBBB"
	)

	// ① 从未 review → 要求首审，不放行。
	v, err := h.CheckMerge(ctx, sid, branch, tipA)
	if err != nil {
		t.Fatal(err)
	}
	if v.Allowed || !v.ReReviewRequired {
		t.Errorf("从未 review 应拦下要求重审: %+v", v)
	}

	// ② 在 tip A review 通过 → 当前 tip 仍是 A → 放行。
	if err := h.RecordReview(ctx, sid, agent, coregate.Review{
		Branch: branch, TipCommit: tipA, Conclusion: contracts.ReviewConclusionApproved,
	}); err != nil {
		t.Fatalf("RecordReview: %v", err)
	}
	v, err = h.CheckMerge(ctx, sid, branch, tipA)
	if err != nil {
		t.Fatal(err)
	}
	if !v.Allowed {
		t.Errorf("tip 未变 + approved 应放行: %+v", v)
	}

	// ③ **又推一行**（当前 tip=B ≠ 被 review 的 tipA）→ 结论失效 → 拦下要求重审。
	v, err = h.CheckMerge(ctx, sid, branch, tipB)
	if err != nil {
		t.Fatal(err)
	}
	if v.Allowed {
		t.Error("review 后又推一行不应放行（V6：旧批准不得放行新 tip）")
	}
	if !v.ReReviewRequired {
		t.Error("review 后又推一行应要求重审")
	}

	// ④ 在 tip B 重新 review 通过 → 放行。
	if err := h.RecordReview(ctx, sid, agent, coregate.Review{
		Branch: branch, TipCommit: tipB, Conclusion: contracts.ReviewConclusionApproved,
	}); err != nil {
		t.Fatalf("RecordReview 2: %v", err)
	}
	v, err = h.CheckMerge(ctx, sid, branch, tipB)
	if err != nil {
		t.Fatal(err)
	}
	if !v.Allowed {
		t.Errorf("重新 review + tip 匹配应放行: %+v", v)
	}

	// changes_requested 不放行。
	if err := h.RecordReview(ctx, sid, agent, coregate.Review{
		Branch: branch, TipCommit: tipB, Conclusion: contracts.ReviewConclusionChangesRequested,
	}); err != nil {
		t.Fatal(err)
	}
	v, _ = h.CheckMerge(ctx, sid, branch, tipB)
	if v.Allowed {
		t.Error("changes_requested 不应放行")
	}
}

// TestMergeGateCacheNeverServesStaleConclusion（p3 修的回归）：合并门禁的读面带缓存
// （全量读事件流是 O(n)），但缓存**不得**给出过期结论——新的 review 记录一落库，
// 判定必须立刻跟着变（否则就是"拿旧批准放行"的 fail-open）。
func TestMergeGateCacheNeverServesStaleConclusion(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()
	sid := host.NewSession()
	agent := core.NewUlid()
	const (
		branch = "feature/x"
		tip    = "commitAAAAAAAAAA"
	)

	// ① 先 approved → 放行（同时把缓存填上）。
	if err := h.RecordReview(ctx, sid, agent, coregate.Review{
		Branch: branch, TipCommit: tip, Conclusion: contracts.ReviewConclusionApproved,
	}); err != nil {
		t.Fatalf("RecordReview: %v", err)
	}
	v, err := h.CheckMerge(ctx, sid, branch, tip)
	if err != nil {
		t.Fatal(err)
	}
	if !v.Allowed {
		t.Fatalf("approved + tip 匹配应放行: %+v", v)
	}

	// ② 又记一条 changes_requested → **缓存必须失效** → 不再放行。
	if err := h.RecordReview(ctx, sid, agent, coregate.Review{
		Branch: branch, TipCommit: tip, Conclusion: contracts.ReviewConclusionChangesRequested,
	}); err != nil {
		t.Fatalf("RecordReview 2: %v", err)
	}
	v, err = h.CheckMerge(ctx, sid, branch, tip)
	if err != nil {
		t.Fatal(err)
	}
	if v.Allowed {
		t.Error("最新结论是 changes_requested：不应放行（缓存不得给出过期结论）")
	}
	if v.Conclusion != string(contracts.ReviewConclusionChangesRequested) {
		t.Errorf("结论应是 changes_requested，得到 %q", v.Conclusion)
	}
}

// TestMergeGateV6HTTP：V6 的三个 HTTP 端点跑通（review 记录 + check 判定）。
func TestMergeGateV6HTTP(t *testing.T) {
	h := newTestHub(t)
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)
	sid := host.NewSession()
	agent := core.NewUlid()

	// 记录一条 approved review（tip=A）。
	reviewBody, _ := json.Marshal(map[string]any{
		"session_id": string(sid), "agent_id": string(agent),
		"branch": "feature/http", "tip_commit": "commitAAAAAAAAAA", "conclusion": "approved",
	})
	resp, err := http.Post(srv.URL+"/api/merge/review", "application/json", bytes.NewReader(reviewBody))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("review 状态码 = %d, want 200", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// check：当前 tip=B（推了一行）→ 应拦下。
	checkBody, _ := json.Marshal(map[string]any{
		"session_id": string(sid), "branch": "feature/http", "current_tip": "commitBBBBBBBBBB",
	})
	resp2, err := http.Post(srv.URL+"/api/merge/check", "application/json", bytes.NewReader(checkBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("check 状态码 = %d, want 200", resp2.StatusCode)
	}
	var verdict host.MergeVerdict
	if err := json.NewDecoder(resp2.Body).Decode(&verdict); err != nil {
		t.Fatal(err)
	}
	if verdict.Allowed || !verdict.ReReviewRequired {
		t.Errorf("HTTP check：推了一行应拦下要求重审: %+v", verdict)
	}
}

// ---------------------------------------------------------------------------
// V7 消费端：合并后冲突预测 + conflict.predicted 事件
// ---------------------------------------------------------------------------

// TestMergeGateV7ConflictPredicted 是 V7 的消费端测试：
// 启用图 + 灌边 → 合并 core 的改动，对同样改了 core 的未合并分支预警撞车；
// 且 conflict.predicted 事件进了事件流。
func TestMergeGateV7ConflictPredicted(t *testing.T) {
	h, path := newGraphHub(t, func(c *host.Config) { c.Graph = true })
	ctx := context.Background()
	sid := contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6") // 与 appendEdges 同 session
	agent := core.NewUlid()

	// 灌依赖边并等索引器追上。
	seq := appendEdges(t, path, hostEdges)
	waitWatermark(t, h, seq)

	// 合并改动 = core；未合并分支 app 也依赖 core（改了 core 会撞上 app 这个消费者）。
	rep, err := h.PredictPostMergeConflicts(ctx, sid, agent, "imports",
		coregraph.ChangeSet{Label: "feature/core-fix", Roots: []string{"core"}},
		[]coregraph.ChangeSet{
			{Label: "feature/app", Roots: []string{"app"}},
			{Label: "feature/util", Roots: []string{"util"}}, // util 是 core 的下游，不与 app 撞
		},
	)
	if err != nil {
		t.Fatalf("PredictPostMergeConflicts: %v", err)
	}
	// app 依赖 core（改 core 的一侧触点落在 app 影响面内 → one-side 撞车）。
	var appRisk *coregraph.ConflictRisk
	for i := range rep.Risks {
		if rep.Risks[i].Branch == "feature/app" {
			appRisk = &rep.Risks[i]
		}
	}
	if appRisk == nil {
		t.Fatal("rep 里没有 feature/app 的风险条目")
	}
	if !appRisk.Collides {
		t.Errorf("feature/app 应预判撞车（改了它依赖的 core）: %+v", appRisk)
	}

	// conflict.predicted 事件进了事件流（V7 消费端就位）。
	events, err := readSessionEvents(t, path, sid)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ev := range events {
		if ev.Kind == coregraph.KindConflictPredicted {
			found = true
			var p contracts.ConflictPredictedPayload
			if json.Unmarshal(ev.Payload, &p) != nil || p.MergedBranch != "feature/core-fix" {
				t.Errorf("conflict.predicted 载荷异常: %s", ev.Payload)
			}
		}
	}
	if !found {
		t.Error("事件流里没有 conflict.predicted（V7 发射缺失）")
	}
}

// TestMergeGateV7GraphDisabled：图未启用 → 冲突预测**诚实降级**（预测不可用，非无冲突）。
func TestMergeGateV7GraphDisabled(t *testing.T) {
	h := newTestHub(t) // 不启用图
	ctx := context.Background()
	sid := host.NewSession()
	agent := core.NewUlid()

	rep, err := h.PredictPostMergeConflicts(ctx, sid, agent, "imports",
		coregraph.ChangeSet{Label: "feature/x", Roots: []string{"core"}},
		[]coregraph.ChangeSet{{Label: "feature/y", Roots: []string{"core"}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	// 图未启用 → Available=false + UnavailableReason（诚实降级）。
	if rep.Available {
		t.Error("图未启用时聚合层不应报 available=true")
	}
	if rep.UnavailableReason == "" {
		t.Error("图未启用时应给 UnavailableReason（明说预测不可用）")
	}
	// 但直接触点重叠（core ∩ core）仍是确定信号 → 照常报撞车。
	if rep.Collisions != 1 {
		t.Errorf("直接触点重叠应报 1 个撞车（确定信号，不需图），得到 %d", rep.Collisions)
	}
}

// readSessionEvents 读一个 session 的全部事件（测试断言冲突/审查事件用）。
func readSessionEvents(t *testing.T, path string, sid contracts.Ulid) ([]contracts.Event, error) {
	t.Helper()
	st, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = st.Close() }()
	return st.ReadFrom(context.Background(), &sid, 0, 0)
}
