package gate

import (
	"encoding/json"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
)

// reviewEvent 组一条 review.recorded durable 事件（测试助手；装配层的发射形状）。
func reviewEvent(t *testing.T, agentID string, r Review) contracts.Event {
	t.Helper()
	raw, err := json.Marshal(ReviewRecordedEvent(r))
	if err != nil {
		t.Fatalf("marshal review payload: %v", err)
	}
	return contracts.Event{
		Kind:    KindReviewRecorded,
		AgentID: contracts.Ulid(agentID),
		Durable: true,
		Payload: raw,
	}
}

const (
	reviewerAgent = "TEECY7V57SP0J9WZ620EHHP02M" // agent 树里的审查角色（ULID）
	tipV1         = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678"
	tipV2         = "b2c3d4e5f60718293a4b5c6d7e8f90123456789a1"
)

// TestReviewStale：失效判定——tip 未变有效、推了一行失效、空 tip fail-close。
//
// 变异自证：若把失效判定去掉（Stale 恒 false），"推了一行"与"空 tip"两条会红。
func TestReviewStale(t *testing.T) {
	r := Review{Branch: "feature/x", TipCommit: tipV1, Conclusion: contracts.ReviewConclusionApproved}

	// tip 未变 → 结论有效（不失效）。
	if r.Stale(tipV1) {
		t.Error("tip 未变时结论应有效（Stale=false）")
	}
	// review 后又推了一行 → tip 变 → 失效。
	if !r.Stale(tipV2) {
		t.Error("review 后推了一行（tip 变）应判失效（Stale=true）")
	}
	// fail-close：当前 tip 观测不到 → 无法确认仍有效 → 失效。
	if !r.Stale("") {
		t.Error("当前 tip 观测不到应 fail-close 判失效")
	}
	// fail-close：记录时没记 tip（畸形 review）→ 失效。
	if !(Review{Branch: "feature/x", Conclusion: contracts.ReviewConclusionApproved}).Stale(tipV1) {
		t.Error("记录时缺 tip 的畸形 review 应判失效")
	}
}

// TestReReviewRequiredReviewThenPush：构造"review 后又推一行"用例——失效并要求重审。
func TestReReviewRequiredReviewThenPush(t *testing.T) {
	events := []contracts.Event{
		reviewEvent(t, reviewerAgent, Review{
			Branch: "feature/x", TipCommit: tipV1, Conclusion: contracts.ReviewConclusionApproved,
		}),
	}
	// review 时 tip 还是 v1 → 不要求重审（结论有效）。
	if ReReviewRequired(events, "feature/x", tipV1) {
		t.Error("刚 review 完（tip 未变）不应要求重审")
	}
	// 之后推了一行 → 分支 tip 到 v2 → 结论失效 → 要求重审。
	if !ReReviewRequired(events, "feature/x", tipV2) {
		t.Error("review 后又推一行（tip 变）应要求重审")
	}
	// 从未 review 过的分支 → 要求（首次必审）。
	if !ReReviewRequired(events, "feature/y", tipV1) {
		t.Error("从未 review 的分支应要求重审")
	}
}

// TestFoldReviewsReplay：事件回放重建——同分支后者覆盖前者、异分支独立、reviewer 取自 agent_id。
func TestFoldReviewsReplay(t *testing.T) {
	events := []contracts.Event{
		// 一条非 review 事件应被忽略。
		{Kind: "gate.checked", Payload: json.RawMessage(`{"passed":true}`)},
		reviewEvent(t, reviewerAgent, Review{
			Branch: "feature/x", TipCommit: tipV1, Conclusion: contracts.ReviewConclusionChangesRequested,
		}),
		reviewEvent(t, "01M6148R1AGAHVSZJDDW5B4P4D", Review{
			Branch: "feature/y", TipCommit: tipV1, Conclusion: contracts.ReviewConclusionApproved,
		}),
		// feature/x 又被重审（tip 变了、结论变 approved）→ 应覆盖上一条。
		reviewEvent(t, reviewerAgent, Review{
			Branch: "feature/x", TipCommit: tipV2, Conclusion: contracts.ReviewConclusionApproved,
		}),
	}
	folded := FoldReviews(events)
	if len(folded) != 2 {
		t.Fatalf("应折出 2 个分支，得到 %d：%v", len(folded), folded)
	}
	x := folded["feature/x"]
	if x.TipCommit != tipV2 || x.Conclusion != contracts.ReviewConclusionApproved {
		t.Errorf("feature/x 应取最新 review（tip=%s, approved），得到 tip=%s conclusion=%s", tipV2, x.TipCommit, x.Conclusion)
	}
	if x.ReviewerID != reviewerAgent {
		t.Errorf("reviewer 应取自 envelope.agent_id = %q，得到 %q", reviewerAgent, x.ReviewerID)
	}
	if y := folded["feature/y"]; y.TipCommit != tipV1 || y.Conclusion != contracts.ReviewConclusionApproved {
		t.Errorf("feature/y 应独立保留，得到 %+v", y)
	}
}

// TestFoldReviewsSkipsBadPayload：坏 payload 不阻断折叠（F7 宽容），好的仍折出。
func TestFoldReviewsSkipsBadPayload(t *testing.T) {
	events := []contracts.Event{
		{Kind: KindReviewRecorded, AgentID: contracts.Ulid(reviewerAgent), Payload: json.RawMessage(`{bad json`)},
		reviewEvent(t, reviewerAgent, Review{Branch: "feature/x", TipCommit: tipV1, Conclusion: contracts.ReviewConclusionApproved}),
	}
	folded := FoldReviews(events)
	if len(folded) != 1 || folded["feature/x"].TipCommit != tipV1 {
		t.Errorf("坏 payload 应被跳过、好的应折出，得到 %v", folded)
	}
}

// TestNormalizeBranch：分支名归一——只去**写法差异**（ref 前缀 / 远端名 / 空白 / 尾斜杠），
// 不改分支身份（大小写敏感、不猜 origin/ 前缀）。
func TestNormalizeBranch(t *testing.T) {
	cases := map[string]string{
		"feature/x":                     "feature/x",
		"  feature/x  ":                 "feature/x",
		"refs/heads/feature/x":          "feature/x",
		"refs/remotes/origin/feature/x": "feature/x",
		"refs/tags/v1":                  "tags/v1",
		"feature/x/":                    "feature/x",
		"Feature/X":                     "Feature/X",        // 大小写敏感（git 分支名敏感）
		"origin/feature/x":              "origin/feature/x", // 没写 refs/remotes/ 前缀 → 不猜
		"":                              "",
	}
	for in, want := range cases {
		if got := NormalizeBranch(in); got != want {
			t.Errorf("NormalizeBranch(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestFoldReviewsNormalizesBranch（p3-6）：**分支名先归一再归并**——同一个分支的不同写法
// 必须折成一条（否则"已审过"的事实凭空丢失）。
func TestFoldReviewsNormalizesBranch(t *testing.T) {
	events := []contracts.Event{
		reviewEvent(t, reviewerAgent, Review{
			Branch: "refs/heads/feature/x", TipCommit: tipV1, Conclusion: contracts.ReviewConclusionChangesRequested,
		}),
		// 同一个分支的另一种写法 → 覆盖前一条（last-write-wins）。
		reviewEvent(t, reviewerAgent, Review{
			Branch: "feature/x", TipCommit: tipV2, Conclusion: contracts.ReviewConclusionApproved,
		}),
	}
	folded := FoldReviews(events)
	if len(folded) != 1 {
		t.Fatalf("同一分支的不同写法应折成 1 条，得到 %d：%v", len(folded), folded)
	}
	r, ok := folded["feature/x"]
	if !ok {
		t.Fatalf("应折出归一后的键 feature/x，得到 %v", folded)
	}
	if r.TipCommit != tipV2 || r.Conclusion != contracts.ReviewConclusionApproved {
		t.Errorf("应取最新一条（tip=%s, approved），得到 tip=%s conclusion=%s", tipV2, r.TipCommit, r.Conclusion)
	}
	// 查的时候用哪种写法都能查到。
	if _, ok := LatestReview(events, "refs/heads/feature/x"); !ok {
		t.Error("用 refs/heads/ 写法查也应查到")
	}
}

// TestApproved（p3-6）：组合谓词"能合" = 有记录 ∧ approved ∧ 未失效。
//
// 变异自证：若去掉"未失效"这一项（只看结论），"review 后又推一行"那条会红。
func TestApproved(t *testing.T) {
	events := []contracts.Event{
		reviewEvent(t, reviewerAgent, Review{
			Branch: "feature/x", TipCommit: tipV1, Conclusion: contracts.ReviewConclusionApproved,
		}),
	}
	// 已批准且 tip 未变 → 能合。
	if !Approved(events, "feature/x", tipV1) {
		t.Error("已批准且结论未失效 → 应能合")
	}
	// review 后又推一行 → 结论失效 → 不能合（旧批准不能放行）。
	if Approved(events, "feature/x", tipV2) {
		t.Error("review 后又推一行（结论失效）→ 不能合")
	}
	// 当前 tip 观测不到（fail-close）→ 不能合。
	if Approved(events, "feature/x", "") {
		t.Error("当前 tip 观测不到（fail-close）→ 不能合")
	}
	// 从未 review → 不能合。
	if Approved(events, "feature/y", tipV1) {
		t.Error("从未 review 的分支 → 不能合")
	}
	// 结论是 changes_requested → 不能合。
	events2 := []contracts.Event{
		reviewEvent(t, reviewerAgent, Review{
			Branch: "feature/x", TipCommit: tipV1, Conclusion: contracts.ReviewConclusionChangesRequested,
		}),
	}
	if Approved(events2, "feature/x", tipV1) {
		t.Error("changes_requested → 不能合")
	}
}

// TestReviewRecordedEventMapping：Review → payload 映射（branch/tip/conclusion 原样，reviewer 不进 payload）。
func TestReviewRecordedEventMapping(t *testing.T) {
	r := Review{Branch: "feature/x", TipCommit: tipV1, Conclusion: contracts.ReviewConclusionApproved, ReviewerID: reviewerAgent}
	p := ReviewRecordedEvent(r)
	if p.Branch != "feature/x" || p.TipCommit != tipV1 || p.Conclusion != contracts.ReviewConclusionApproved {
		t.Errorf("映射错：%+v", p)
	}
	// payload 经 JSON 往返应还原 Review（折叠路径的一致性）。
	raw, _ := json.Marshal(p)
	var back contracts.ReviewRecordedPayload
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back != p {
		t.Errorf("往返不一致：%+v vs %+v", back, p)
	}
}
