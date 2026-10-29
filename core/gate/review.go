package gate

import (
	"encoding/json"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
)

// V6 review-tip 失效检测（07 V 区 V6）：「review 后又推了一行」是最常见的 merge block——
// 分支 tip 变了 → 已有 review 结论失效 → 要求重审。事件流天然可检测：**被 review 的 tip
// 就在 review.recorded 事件的 payload 里**，合并门禁拿它比对分支**当前** tip 即可判定失效，
// 无需三点 diff。
//
// # tip 观测走哪条通道（与 X2 git anchor / replay verify 的关系）
//
// 走**事件流**这条：review.recorded 事件把"被 review 时的 tip commit"落进 payload（durable，
// 可回放、可审计）；当前 tip 由合并门禁经 git 观测（rev-parse，与 runtime/verify 的 anchor
// 核验同源——只是这里比的是"事件里记的 tip" vs "现在观测到的 tip"，不是核验 commit 存在性）。
// 选事件流这条的理由：结论的**基准**必须可回放（崩溃/换进程后仍知道"当初 review 的是哪个
// tip"），git 现状会漂而事件不会。
//
// # reviewer 身份：agent 树角色，不绑死（V9）
//
// 我们没有 mission/deps/独立 reviewer（对应物是 agent 树 + 协调人）。review 结论来自 agent
// 树里的角色（协调人 / 审查 agent），其身份由事件的 **envelope.agent_id** 承载，**不进
// payload**——字段形状因此不绑死任何"独立 reviewer"概念，后续换角色不改契约。
//
// 本文件是**纯逻辑**（无 I/O）：Review 记录、失效判定、从事件流折叠、事件映射。
// review.recorded 事件的**发射**在装配层（合并门禁，v3 工作流引擎接线；本刀只给能力面，
// 与 gate.checked 当前"无生产发射方"同款口径）。
//
// # 分支名归一（归并的前提）
//
// 记录方与查询方写的分支名可能不是同一个字符串（"refs/heads/feature/x" vs "feature/x"、
// "refs/remotes/origin/feature/x" vs "feature/x"）。**先归一再归并**（NormalizeBranch）——
// 不归一，同一个分支会裂成多条记录，"已审过"的事实凭空丢失（门禁据此误判"要重审"，
// 或更糟：拿一份对不上的旧结论当批准）。
//
// # 判定入口
//
//   - ReReviewRequired：要不要重审（失效检测）；
//   - Approved：**能不能合**（approved ∧ 未失效）的组合谓词——把"能合"的语义钉在框架，
//     调用方不必自己拼"有记录 + 结论 + 未失效"三段。

// KindReviewRecorded 是 review 结论落库的事件 kind（durable=true）。
const KindReviewRecorded = "review.recorded"

// Review 是一条分支 review 结论的记录（V6）：被 review 时的分支 tip + 结论 + reviewer。
//
// TipCommit 是失效判定的基准：分支 tip 变了 → 结论失效（见 Stale）。
type Review struct {
	// Branch 是被 review 的分支名 / ref（**从事件流折出时已归一**，见 NormalizeBranch）。
	Branch string
	// TipCommit 是被 review 时该分支的 tip commit（失效判定基准）。
	TipCommit string
	// Conclusion 是 review 结论（approved / changes_requested；契约枚举，形状不绑死 reviewer）。
	Conclusion contracts.ReviewConclusion
	// ReviewerID 是记录该结论的 agent id（= review.recorded 事件的 envelope.agent_id；
	// agent 树里的角色，非独立 reviewer，V9）。
	ReviewerID string
}

// Stale 报告一条 review 结论是否因分支 tip 变动而**失效**（V6：失效 → 要求重审）。
//
// 判定 = 记录时的 tip ≠ 当前 tip。**fail-close**：记录时没记 tip（畸形 review）或当前 tip
// 观测不到（git 查询失败）时，无法确认结论仍有效 → 一律判失效（要求重审），绝不因"比对不了"
// 就放行——那会让"推了一行但没重审"蒙过合并门禁。
func (r Review) Stale(currentTip string) bool {
	if r.TipCommit == "" || currentTip == "" {
		return true
	}
	return r.TipCommit != currentTip
}

// ReviewRecordedEvent 把一条 review 结论映射成 review.recorded 事件的 payload（V6）。
//
// reviewer 身份**不进 payload**（V9）：由装配层填事件的 envelope.agent_id。映射口径与
// core/gate.CheckedEvent 一致——本函数只产 payload，包 envelope / 落库在装配层。
//
// 分支名**原样落 payload**（不归一）：payload 记的是"当时观察到的事实"；归一发生在
// **读取侧**（FoldReviews / LatestReview）——同一分支的两种写法在折叠时合成一条。
func ReviewRecordedEvent(r Review) contracts.ReviewRecordedPayload {
	return contracts.ReviewRecordedPayload{
		Branch:     r.Branch,
		TipCommit:  r.TipCommit,
		Conclusion: r.Conclusion,
	}
}

// FoldReviews 从事件流重建每个分支的**最新** review 结论（V6：事件回放重建）。
//
// 后到的 review.recorded 覆盖同分支的旧结论（last-write-wins，按事件顺序）。
// **前置条件**：events 按 seq 升序（durable 事件流的自然序；core/durable / store.ReadFrom
// 均如此）。跳过 payload 解析失败的条目（坏事件不阻断折叠，F7 宽容）。
//
// **分支名先归一再归并**（NormalizeBranch）："refs/heads/feature/x" 与 "feature/x" 是**同
// 一个分支**——不归一就会裂成两条记录，"分支被审过"的事实凭空丢失（门禁据此要求重审，
// 或更糟：拿旧结论放行）。
//
// 返回 map[归一后的分支名] → 最新 Review；某分支无记录 = map 里无该键（调用方据此判
// "尚未 review"）。
func FoldReviews(events []contracts.Event) map[string]Review {
	out := make(map[string]Review)
	for _, ev := range events {
		if ev.Kind != KindReviewRecorded {
			continue
		}
		var p contracts.ReviewRecordedPayload
		if json.Unmarshal(ev.Payload, &p) != nil {
			continue
		}
		branch := NormalizeBranch(p.Branch)
		if branch == "" {
			continue // 无分支的记录无法归属，跳过
		}
		out[branch] = Review{
			Branch:     branch,
			TipCommit:  p.TipCommit,
			Conclusion: p.Conclusion,
			ReviewerID: string(ev.AgentID),
		}
	}
	return out
}

// NormalizeBranch 归一分支名（V6：review 结论按分支归并，**名字写法不同不该裂成两条**）。
//
// 归一规则（按序）：
//
//  1. 去首尾空白；
//  2. 去 "refs/heads/" 前缀（本地分支的完整 ref）；
//  3. 去 "refs/remotes/<remote>/" 前缀（远端跟踪分支的完整 ref——**含远端名**，
//     "refs/remotes/origin/feature/x" → "feature/x"）；
//  4. 去其余 "refs/" 前缀；
//  5. 去尾斜杠。
//
// **大小写敏感**（git 分支名大小写敏感，折叠大小写会把两个分支误并成一条）。
// 归一**只去写法差异**，不改分支身份："origin/feature/x"（没写 refs/remotes/ 前缀）
// 保持原样——它可能就是本地真有一个叫 "origin/feature/x" 的分支，不该猜。
func NormalizeBranch(branch string) string {
	s := strings.TrimSpace(branch)
	if s == "" {
		return ""
	}
	const (
		headsPrefix   = "refs/heads/"
		remotesPrefix = "refs/remotes/"
		refsPrefix    = "refs/"
	)
	switch {
	case strings.HasPrefix(s, headsPrefix):
		s = strings.TrimPrefix(s, headsPrefix)
	case strings.HasPrefix(s, remotesPrefix):
		// "refs/remotes/<remote>/<branch>" → 去掉远端名那一段。
		rest := strings.TrimPrefix(s, remotesPrefix)
		if i := strings.Index(rest, "/"); i >= 0 {
			s = rest[i+1:]
		} else {
			s = rest
		}
	case strings.HasPrefix(s, refsPrefix):
		s = strings.TrimPrefix(s, refsPrefix)
	}
	return strings.TrimRight(s, "/")
}

// LatestReview 取某分支的最新 review 结论（FoldReviews 的单分支版，供合并门禁按分支查）。
// 分支名按 NormalizeBranch 归一后查（查的写法与记的写法不同也能对上）。
// 第二个返回值报告该分支是否**有** review 记录（false = 尚未 review）。
func LatestReview(events []contracts.Event, branch string) (Review, bool) {
	r, ok := FoldReviews(events)[NormalizeBranch(branch)]
	return r, ok
}

// ReReviewRequired 是合并门禁的判定入口（V6）：给定事件流、分支、当前 tip，是否**要求重审**。
//
//   - 该分支**从未** review 过 → 要求（首次必审）；
//   - 最新 review 的 tip ≠ 当前 tip（含观测不到 tip 的 fail-close）→ 要求重审（失效）；
//   - 最新 review 的 tip == 当前 tip → 不要求（结论仍有效；至于结论是 approved 还是
//     changes_requested 由调用方另判，本函数只回答"要不要重审"这一件事）。
func ReReviewRequired(events []contracts.Event, branch, currentTip string) bool {
	r, ok := LatestReview(events, branch)
	if !ok {
		return true
	}
	return r.Stale(currentTip)
}

// Approved 是合并门禁的**组合谓词**（V6）：给定事件流、分支、当前 tip，该分支是否
// **已获批且结论仍然有效**（= 可以合）。
//
// 组合语义（把"能合"这件事钉在框架里，调用方不必自己拼）：
//
//	Approved = 有 review 记录 ∧ 结论 = approved ∧ **未失效**（tip 未变）
//
// 三者缺一即 false：
//
//   - 没有 review 记录 → 没审过，不能合（首次必审）；
//   - 结论是 changes_requested → 没批，不能合；
//   - 结论失效（review 后又推了一行 / 当前 tip 观测不到）→ **不能拿旧批准放行**
//     （fail-close：那正是 V6 要防的"推了一行却蒙过门禁"）。
func Approved(events []contracts.Event, branch, currentTip string) bool {
	r, ok := LatestReview(events, branch)
	if !ok {
		return false
	}
	if r.Conclusion != contracts.ReviewConclusionApproved {
		return false
	}
	return !r.Stale(currentTip)
}
