package agentlink_test

// K18 Inquiry 的单测（core/agentlink）：Q6「结构化摘要 + 事件窗口」两者都给，
// K24「summary 的 refs 可被拉原文核验」正反两例，K11 可见性，窗口边界/agent 投影/截断。

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/agentlink"
)

func TestInquiryReturnsSummaryAndEventWindow(t *testing.T) {
	w := newWorld(t)
	intent := w.touch(w.childA1, "src/a.go") // 证据：文件路径出现在 intent 的 arguments 里
	// summary 先落盘、再引用自己的 seq（"先有事件再引用它"的顺序显式）。
	doneSeq := w.appendCompleted(w.childA1, "读完了 src/a.go，三段结构", nil)
	w.rewriteSummaryRefs(doneSeq, []string{fmt.Sprintf("e:%d", doneSeq), "src/a.go"})

	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA,
	})
	if err != nil {
		t.Fatalf("Inquiry: %v", err)
	}

	// Q6 之一：结构化摘要（给模型直接消费）。
	if len(res.Summaries) != 1 {
		t.Fatalf("Summaries = %d 条，want 1", len(res.Summaries))
	}
	s := res.Summaries[0]
	if s.AgentID != string(w.childA1) || s.Seq != doneSeq {
		t.Fatalf("SummaryEntry 归属/seq 不符: %+v（want agent=%s seq=%d）", s, w.childA1, doneSeq)
	}
	if s.Summary.Text != "读完了 src/a.go，三段结构" {
		t.Fatalf("summary 文本不符: %q", s.Summary.Text)
	}
	if len(s.Summary.Refs) != 2 {
		t.Fatalf("summary refs = %v，want 2 条", s.Summary.Refs)
	}

	// Q6 之二：事件窗口（给模型按需翻）——含 intent 与 completed 两条原文。
	if len(res.Window) != 7 {
		t.Fatalf("窗口 = %d 条，want 7（A 的 5 条开局 + intent + completed）", len(res.Window))
	}
	if res.WindowRange.FromSeq != 1 || res.WindowRange.ToSeq != doneSeq {
		t.Fatalf("窗口区间 = [%d,%d]，want [1,%d]", res.WindowRange.FromSeq, res.WindowRange.ToSeq, doneSeq)
	}
	var sawIntent bool
	for _, ev := range res.Window {
		if ev.ID == intent.ID {
			sawIntent = true
		}
	}
	if !sawIntent {
		t.Fatalf("窗口里应能翻到 childA1 的 tool.call.intent 原文")
	}
	if res.Truncated {
		t.Fatalf("未设 Limit 不该报截断")
	}

	// K24：refs 逐条核验通过（默认取窗口内全部 summary 的 refs）。
	if len(res.RefChecks) != 2 || !res.RefsAllOK {
		t.Fatalf("RefChecks = %+v allOK=%v，want 2 条全过", res.RefChecks, res.RefsAllOK)
	}
	for _, c := range res.RefChecks {
		if !c.OK {
			t.Fatalf("ref %q 应核验通过: %+v", c.Ref, c)
		}
	}
}

func TestInquiryVerifiesRefsAgainstPulledWindow(t *testing.T) {
	w := newWorld(t)
	w.touch(w.childA1, "docs/plan.md")
	first, last := w.sessionContiguousRange(w.sidA)
	doneSeq := w.appendCompleted(w.childA1, "改了三处", []string{
		"docs/plan.md",
		fmt.Sprintf("e:%d", last),
		fmt.Sprintf("e:%d-%d", first, last), // seq 区间（K24 的另一种形态）
	})

	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.childA1},
		SessionID: w.sidA,
	})
	if err != nil {
		t.Fatalf("Inquiry: %v", err)
	}
	if len(res.RefChecks) != 3 {
		t.Fatalf("RefChecks = %d，want 3", len(res.RefChecks))
	}
	for _, c := range res.RefChecks {
		if !c.OK {
			t.Fatalf("ref %q 应通过（窗口内有证据）: %+v", c.Ref, c)
		}
	}
	if res.Summaries[0].Seq != doneSeq {
		t.Fatalf("summary seq = %d，want %d", res.Summaries[0].Seq, doneSeq)
	}

	// 显式指定 refs：只核验给出的那些（不自动展开 summary refs）。
	res, err = w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA,
		Refs:      []string{fmt.Sprintf("e:%d", doneSeq)},
	})
	if err != nil {
		t.Fatalf("Inquiry(显式 refs): %v", err)
	}
	if len(res.RefChecks) != 1 || !res.RefChecks[0].OK {
		t.Fatalf("显式 refs 核验 = %+v", res.RefChecks)
	}
}

// TestInquiryRejectsForgedRefs 是 K24 的**反例**：篡改/伪造的 refs 必须核验失败。
func TestInquiryRejectsForgedRefs(t *testing.T) {
	w := newWorld(t)
	w.touch(w.childA1, "src/a.go")
	last := w.sessionLastSeq(w.sidA)
	w.appendCompleted(w.childA1, "读完了", []string{"src/a.go", fmt.Sprintf("e:%d", last)})

	forged := []string{
		fmt.Sprintf("e:%d", last+1000),  // seq 不存在
		"e:1-99999",                     // 区间比窗口还长
		"e:",                            // 解不出序号
		"nowhere/nope.txt",              // 从没被读过的文件
		"src/b.go",                      // 同目录下另一个文件（没动过）
		"../../etc/passwd",              // 路径穿越（窗口里没有）
		"",                              // 空引用
		"github.com/Yorha9e/alasol.git", // 不像路径也不像 seq 的玩意
	}
	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA,
		Refs:      forged,
	})
	if err != nil {
		t.Fatalf("Inquiry(forged): %v", err)
	}
	if res.RefsAllOK {
		t.Fatalf("伪造 refs 不该全部通过")
	}
	if len(res.RefChecks) != len(forged) {
		t.Fatalf("RefChecks = %d，want %d", len(res.RefChecks), len(forged))
	}
	for i, c := range res.RefChecks {
		if c.OK {
			t.Fatalf("伪造 ref %q 竟核验通过: %+v", c.Ref, c)
		}
		if c.Detail == "" {
			t.Fatalf("失败的 ref %q 应给出原因（fail-close 也要让人知道卡在哪）", c.Ref)
		}
		if c.Ref != forged[i] {
			t.Fatalf("RefChecks[%d].Ref = %q，want %q（顺序应与请求一致）", i, c.Ref, forged[i])
		}
	}

	// 篡改：把一条真 ref 的 seq 改错（内容对、坐标错）→ 也要失败。
	tampered := []string{fmt.Sprintf("e:%d", last), fmt.Sprintf("e:%d", last+100)}
	res, err = w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA,
		Refs:      tampered,
	})
	if err != nil {
		t.Fatalf("Inquiry(tampered): %v", err)
	}
	if !res.RefChecks[0].OK || res.RefChecks[1].OK || res.RefsAllOK {
		t.Fatalf("篡改 ref 的核验结果不符: %+v", res.RefChecks)
	}
}

func TestInquiryWindowBoundsAgentProjectionAndLimit(t *testing.T) {
	w := newWorld(t)
	w.touch(w.childA1, "src/a.go")
	w.touch(w.amA, "README.md")
	mid := w.sessionLastSeq(w.sidA)
	w.appendCompleted(w.childA1, "读完了", []string{"src/a.go"})

	caller := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}

	// 上界裁剪：只要 seq <= mid 的窗口。
	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller: caller, SessionID: w.sidA, Window: agentlink.EventWindow{ToSeq: mid},
	})
	if err != nil {
		t.Fatalf("Inquiry(ToSeq): %v", err)
	}
	if res.WindowRange.ToSeq != mid {
		t.Fatalf("窗口上界 = %d，want %d", res.WindowRange.ToSeq, mid)
	}
	if len(res.Summaries) != 0 {
		t.Fatalf("completed 在上界之外，不该有 summary: %+v", res.Summaries)
	}

	// 下界 + agent 投影（K25：按 agent_id 对全局流做投影）。
	res, err = w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller: caller, SessionID: w.sidA, AgentID: w.childA1, Window: agentlink.EventWindow{FromSeq: mid},
	})
	if err != nil {
		t.Fatalf("Inquiry(AgentID): %v", err)
	}
	if len(res.Window) == 0 {
		t.Fatalf("childA1 在 mid 之后应有事件")
	}
	for _, ev := range res.Window {
		if ev.AgentID != w.childA1 {
			t.Fatalf("窗口里混进了别 agent 的事件: %s（agent=%s）", ev.Kind, ev.AgentID)
		}
	}

	// Limit：截断要诚实报告。
	res, err = w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller: caller, SessionID: w.sidA, Limit: 2,
	})
	if err != nil {
		t.Fatalf("Inquiry(Limit): %v", err)
	}
	if len(res.Window) != 2 || !res.Truncated {
		t.Fatalf("Limit=2 → 窗口 %d 条 / Truncated=%v", len(res.Window), res.Truncated)
	}

	// 没有 refs 也没有 summary：不是错误，RefChecks 空、vacuously 全过。
	res, err = w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller: caller, SessionID: w.sidA, Window: agentlink.EventWindow{FromSeq: 1, ToSeq: 1},
	})
	if err != nil {
		t.Fatalf("Inquiry(无 refs): %v", err)
	}
	if len(res.RefChecks) != 0 || !res.RefsAllOK {
		t.Fatalf("无 refs 时应空且 vacuously 全过: %+v", res.RefChecks)
	}
}

// TestInquiryVisibilityK11：跨 session 查询只有"gate 相交的**主 agent**"通行（K11①）。
func TestInquiryVisibilityK11(t *testing.T) {
	w := newWorld(t)
	w.appendCompleted(w.childA1, "读完了", nil)

	bMain := agentlink.Identity{SessionID: w.sidB, AgentID: w.amB}
	bSub := agentlink.Identity{SessionID: w.sidB, AgentID: w.subB1}
	cMain := agentlink.Identity{SessionID: w.sidC, AgentID: w.amC}
	aSub := agentlink.Identity{SessionID: w.sidA, AgentID: w.childA1}

	// ① B 的主 agent：与 A gate 相交 → 可以 Inquiry A。
	if _, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{Caller: bMain, SessionID: w.sidA}); err != nil {
		t.Fatalf("B 的主 agent 应能 Inquiry A（gate 相交）: %v", err)
	}
	// ② B 的 **subagent**：别 session 的 subagent 完全不可见。
	if _, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{Caller: bSub, SessionID: w.sidA}); err == nil {
		t.Fatalf("B 的 subagent 不该能 Inquiry A（K11：别 session 的 subagent 完全不可见）")
	}
	// ③ C 的主 agent：gate 不相交 → 不可见。
	if _, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{Caller: cMain, SessionID: w.sidA}); err == nil {
		t.Fatalf("C 的主 agent 不该能 Inquiry A（gate 不相交）")
	}
	// ④ A 自己的 subagent：同 session 恒可见（父查自己的子树）。
	if _, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{Caller: aSub, SessionID: w.sidA}); err != nil {
		t.Fatalf("A 的 subagent 应能 Inquiry 自己的 session: %v", err)
	}
	// ⑤ 不存在的 session：明确报"不知道"，不是"不可见"。
	if _, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller: bMain, SessionID: contracts.Ulid("ZZZZZZZZZZZZZZZZZZZZZZZZZZ"),
	}); err == nil {
		t.Fatalf("未知 session 应报错")
	}
	// ⑥ 身份不完整：拒（envelope 必填 + 可见性要用的字段）。
	if _, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller: agentlink.Identity{SessionID: w.sidB}, SessionID: w.sidA,
	}); err == nil {
		t.Fatalf("缺 agent_id 的身份应拒")
	}
}

func TestInquiryFailClosedWithoutReader(t *testing.T) {
	w := newWorld(t)
	l := agentlink.New(agentlink.Deps{Table: w.table}) // 只有表，没有 reader
	if _, err := l.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA,
	}); err == nil {
		t.Fatalf("无 reader 时 Inquiry 应报错（fail-close）")
	}
	var nilLink *agentlink.Link
	if _, err := nilLink.Inquiry(context.Background(), agentlink.InquiryRequest{}); err == nil {
		t.Fatalf("nil Link 的 Inquiry 应报错")
	}
}

// ---------------------------------------------------------------------------
// world 辅助：造"subagent 干完活"的事实链
// ---------------------------------------------------------------------------

// touch 造一条 tool.call.intent（agent 读了/改了某个文件）——文件路径 refs 的证据来源。
func (w *world) touch(agent contracts.Ulid, path string) contracts.Event {
	w.t.Helper()
	args, err := json.Marshal(map[string]string{"path": path})
	if err != nil {
		w.t.Fatalf("marshal args: %v", err)
	}
	return w.raw(w.sidA, agent, "tool.call.intent", contracts.ToolCallIntentPayload{
		ToolCallID: contracts.ProviderId("toolu_" + string(agent)[:6]), ToolName: "read",
		Arguments: args, Idempotent: true, Replay: contracts.ReplayKindSafe,
	})
}

// appendCompleted 造一条 agent.completed（summary 带 refs），返回其 seq。
func (w *world) appendCompleted(agent contracts.Ulid, text string, refs []string) int64 {
	w.t.Helper()
	ev := w.raw(w.sidA, agent, "agent.completed", contracts.AgentCompletedPayload{
		AgentID: agent,
		Summary: contracts.AgentSummary{Text: text, Refs: refs, Taint: "clean"},
	})
	if ev.Seq == nil {
		w.t.Fatalf("agent.completed 没有 seq")
	}
	return *ev.Seq
}

// rewriteSummaryRefs 把最后一条 agent.completed 的 refs 改写成给定值（测试内校正用）。
func (w *world) rewriteSummaryRefs(seq int64, refs []string) {
	w.t.Helper()
	for _, ev := range w.st.all() {
		if ev.Seq != nil && *ev.Seq == seq && ev.Kind == "agent.completed" {
			var p contracts.AgentCompletedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				w.t.Fatalf("unmarshal: %v", err)
			}
			p.Summary.Refs = refs
			b, err := json.Marshal(p)
			if err != nil {
				w.t.Fatalf("marshal: %v", err)
			}
			ev.Payload = b
			w.st.replace(ev)
			return
		}
	}
	w.t.Fatalf("找不到 seq=%d 的 agent.completed", seq)
}

// sessionContiguousRange 返回该 session 在全局流里**连续独占**的 seq 区间
// （从它的第一条事件起，直到出现别 session 的 seq 为止）。seq 区间引用（K24）只能用这种
// 区间——区间按全局流解释，夹着别 session 的 seq 是核不过的（见 windowVerifier.checkSeqRef）。
func (w *world) sessionContiguousRange(session contracts.Ulid) (int64, int64) {
	w.t.Helper()
	all := w.st.all()
	from, to := int64(0), int64(0)
	for _, ev := range all {
		if ev.SessionID != session {
			if from > 0 && to > 0 && to < *ev.Seq-1 {
				break // 已经收集完一段连续区间，后面是别的 session
			}
			continue
		}
		if from == 0 {
			from = *ev.Seq
		}
		to = *ev.Seq
	}
	return from, to
}

func (w *world) sessionFirstSeq(session contracts.Ulid) int64 {
	evs, _ := w.st.ReadFrom(context.Background(), &session, 1, 0)
	if len(evs) == 0 {
		return 0
	}
	return *evs[0].Seq
}

func (w *world) sessionLastSeq(session contracts.Ulid) int64 {
	evs, _ := w.st.ReadFrom(context.Background(), &session, 1, 0)
	if len(evs) == 0 {
		return 0
	}
	return *evs[len(evs)-1].Seq
}

// ---------------------------------------------------------------------------
// 评审轮修复的回归用例（F2 / F8 / F10）
// ---------------------------------------------------------------------------

// TestF2ForgedPathRefsRejected（F2）：路径型 ref 必须是窗口里的**完整路径 token**，
// 不能是原文的任意子串。
//
// 旧口径用 strings.Contains 在裸文本上找 → `"{"`、`"a.go"`、`"src/a"`、`"e"` 这种
// 子串都能蒙混过关（伪造的 refs 核验通过 = 引用造假没被发现）。
func TestF2ForgedPathRefsRejected(t *testing.T) {
	w := newWorld(t)
	w.touch(w.childA1, "src/a.go")
	last := w.sessionLastSeq(w.sidA)
	w.appendCompleted(w.childA1, "读完了", []string{"src/a.go", fmt.Sprintf("e:%d", last)})

	forged := []string{
		"{",          // JSON 结构符
		"}",          // JSON 结构符
		`"`,          // 引号
		",",          // 逗号
		"a.go",       // 文件名碎片（是 src/a.go 的子串）
		"src/a",      // 目录碎片（是 src/a.go 的子串）
		"e",          // 既不像路径也不像 seq
		"go",         // 扩展名碎片
		"src/a.go/x", // 路径前缀
		"a.go ",      // 带空白的碎片
	}
	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA,
		Refs:      forged,
	})
	if err != nil {
		t.Fatalf("Inquiry(forged): %v", err)
	}
	if res.RefsAllOK {
		t.Fatalf("伪造 refs 不该全部通过: %+v", res.RefChecks)
	}
	if len(res.RefChecks) != len(forged) {
		t.Fatalf("RefChecks = %d，want %d", len(res.RefChecks), len(forged))
	}
	for i, c := range res.RefChecks {
		if c.OK {
			t.Fatalf("子串式伪造 ref %q 竟核验通过（F2）: %+v", forged[i], c)
		}
		if c.Detail == "" {
			t.Fatalf("失败的 ref %q 应给出原因", forged[i])
		}
	}
	// 真 ref 仍然通过（token 口径不是"全拒"）。
	ok, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA,
		Refs:      []string{"src/a.go", fmt.Sprintf("e:%d", last)},
	})
	if err != nil {
		t.Fatalf("Inquiry(真 refs): %v", err)
	}
	if !ok.RefsAllOK {
		t.Fatalf("真 refs 应核验通过: %+v", ok.RefChecks)
	}
}

// TestF2PathRefNeedsPathShape（F2）：不像路径的形态 fail-close（不认识的形态不可核验）。
func TestF2PathRefNeedsPathShape(t *testing.T) {
	w := newWorld(t)
	w.touch(w.childA1, "src/a.go")
	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA,
		Refs:      []string{"a.go", "README", "plan"},
	})
	if err != nil {
		t.Fatalf("Inquiry: %v", err)
	}
	for _, c := range res.RefChecks {
		if c.OK {
			t.Fatalf("不像路径的 ref %q 不该核验通过: %+v", c.Ref, c)
		}
	}
}

// TestF8InquirySameSessionProjectionFollowsK11（F8）：同 session 查询默认投影到
// **调用方的可见集**（自己 + 父 + 直系子）——同 session 的兄弟本就该不可见。
func TestF8InquirySameSessionProjectionFollowsK11(t *testing.T) {
	w := newWorld(t)
	// 兄弟各留一条自己的事件（tool.call.intent 带路径，便于断言）。
	w.touch(w.childA1, "src/a.go")
	w.touch(w.sibA2, "src/b.go")

	sawSibling := func(res agentlink.InquiryResult) bool {
		for _, ev := range res.Window {
			if ev.AgentID == w.sibA2 {
				return true
			}
		}
		return false
	}
	// ① 调用方 = childA1（兄弟 sibA2 的事件不可见）。
	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.childA1},
		SessionID: w.sidA,
	})
	if err != nil {
		t.Fatalf("Inquiry(兄弟视角): %v", err)
	}
	if sawSibling(res) {
		t.Fatalf("兄弟 siblingA2 的事件不该出现在 childA1 的窗口里（F8）: %+v", res.Window)
	}
	// ② 调用方 = amA（sibA2 是它的直系子 → 可见）。
	res, err = w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA,
	})
	if err != nil {
		t.Fatalf("Inquiry(主 agent 视角): %v", err)
	}
	if !sawSibling(res) {
		t.Fatalf("主 agent 应看得见直系子的事件: %+v", res.Window)
	}
	// ③ 显式投影到兄弟 → 拒（K11：兄弟不可见）。
	if _, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.childA1},
		SessionID: w.sidA,
		AgentID:   w.sibA2,
	}); err == nil {
		t.Fatalf("显式投影到兄弟应被拒（F8/K11）")
	}
}

// TestF10InquiryTruncatedOnlyWhenMoreThanLimit（F10）：恰好读满 limit 时**不**报截断。
//
// 旧口径是 len(events) >= limit → 恰好读完也被说成"没读完"（假截断，调用方白翻页）。
func TestF10InquiryTruncatedOnlyWhenMoreThanLimit(t *testing.T) {
	w := newWorld(t)
	n := int(w.sessionLastSeq(w.sidA)) // session A 的事件数（它的 seq 是连续的 1..n）
	caller := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}

	// 恰好读完：Truncated 必须为 false。
	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller: caller, SessionID: w.sidA, Limit: n,
	})
	if err != nil {
		t.Fatalf("Inquiry(Limit=n): %v", err)
	}
	if res.Truncated {
		t.Fatalf("恰好读满 limit 不该报截断（F10）: window=%d n=%d", len(res.Window), n)
	}
	if len(res.Window) != n {
		t.Fatalf("窗口 = %d 条，want %d", len(res.Window), n)
	}
	// 少读一条：Truncated 必须为 true。
	res, err = w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller: caller, SessionID: w.sidA, Limit: n - 1,
	})
	if err != nil {
		t.Fatalf("Inquiry(Limit=n-1): %v", err)
	}
	if !res.Truncated {
		t.Fatalf("没读完就该报截断: window=%d n=%d", len(res.Window), n)
	}
	if len(res.Window) != n-1 {
		t.Fatalf("窗口 = %d 条，want %d", len(res.Window), n-1)
	}
}

// TestF2PathWithSpacesVerifiesWholePathOnly（#1）：含**空格**的路径必须能核验通过，
// 而它被空白切出的碎片必须被拒。
//
// 旧口径把空白当 token 边界 → `C:\Users\John Doe\proj\a.txt` 被切碎 →
// ① 完整路径核验不过（假阴性：含空格的机器上硬验收直接红）；
// ② 碎片 `C:\Users\John` 反而核验通过（假阳性残留）。
func TestPathRefWithSpacesVerifiesWholePathOnly(t *testing.T) {
	w := newWorld(t)
	const spaced = "C:\\Users\\John Doe\\proj\\a.txt"
	w.touch(w.childA1, spaced)
	last := w.sessionLastSeq(w.sidA)
	w.appendCompleted(w.childA1, "读完了", []string{spaced, fmt.Sprintf("e:%d", last)})

	// ① 完整路径（含空格）必须核验通过。
	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA,
		Refs:      []string{spaced},
	})
	if err != nil {
		t.Fatalf("Inquiry(含空格路径): %v", err)
	}
	if len(res.RefChecks) != 1 || !res.RefChecks[0].OK {
		t.Fatalf("含空格的完整路径应核验通过（#1 假阴性）: %+v", res.RefChecks)
	}

	// ② 空白切出的碎片必须被拒（#1 假阳性残留）。
	fragments := []string{"C:\\Users\\John", "Doe\\proj\\a.txt"}
	res, err = w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA,
		Refs:      fragments,
	})
	if err != nil {
		t.Fatalf("Inquiry(碎片): %v", err)
	}
	for i, c := range res.RefChecks {
		if c.OK {
			t.Fatalf("空格切出的碎片 %q 不该核验通过（#1 假阳性）: %+v", fragments[i], c)
		}
		if c.Detail == "" {
			t.Fatalf("失败的 ref %q 应给出原因", fragments[i])
		}
	}

	// ③ 散文里提到的路径仍要能核验（切分碎片那条路不能被堵死）。
	w.touch(w.childA1, "docs/plan.md")
	res, err = w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA,
		Refs:      []string{"docs/plan.md"},
	})
	if err != nil {
		t.Fatalf("Inquiry(散文里的路径): %v", err)
	}
	if len(res.RefChecks) != 1 || !res.RefChecks[0].OK {
		t.Fatalf("路径 token 应核验通过: %+v", res.RefChecks)
	}
}

// TestPathRefProseWithSeveralSpaceSeparatedPaths（v2-S5 清账⑥）：散文里用**空白并列**的
// 几个路径必须各自可核验（旧口径把它们当成"一条带空格的路径"，只入整串 → 两条真实路径
// 全核不过 = 假阴性；而模型是如实引用证据里的路径的）。
func TestPathRefProseWithSeveralSpaceSeparatedPaths(t *testing.T) {
	w := newWorld(t)
	const (
		a = "core/store.go"
		b = "core/graph/graph.go"
	)
	// 证据：一条 summary 正文里空白并列两个路径（无标点，模型很常这么写）。
	w.touch(w.childA1, a+" "+b)
	last := w.sessionLastSeq(w.sidA)
	w.appendCompleted(w.childA1, "改完了", []string{last2ref(last)})

	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA, Refs: []string{a, b},
	})
	if err != nil {
		t.Fatalf("Inquiry: %v", err)
	}
	for i, want := range []string{a, b} {
		c := res.RefChecks[i]
		if !c.OK {
			t.Fatalf("散文里并列的路径 %q 应核验通过（假阴性）: %+v", want, c)
		}
	}
}

// TestPathRefSingleSpacedPathStillWholeOnly：收窄⑥**不能**放松假阳性防线——
// 真正"一条带空格的路径"仍然只认整串，碎片必须被拒（否则伪造 ref 能蒙过去）。
func TestPathRefSingleSpacedPathStillWholeOnly(t *testing.T) {
	w := newWorld(t)
	const spaced = "C:\\Users\\John Doe\\proj\\a.txt"
	w.touch(w.childA1, spaced)
	last := w.sessionLastSeq(w.sidA)
	w.appendCompleted(w.childA1, "读完了", []string{last2ref(last)})

	// 整串核得过。
	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA, Refs: []string{spaced},
	})
	if err != nil {
		t.Fatalf("Inquiry(整串): %v", err)
	}
	if len(res.RefChecks) != 1 || !res.RefChecks[0].OK {
		t.Fatalf("含空格的完整路径应核验通过: %+v", res.RefChecks)
	}
	// 碎片必须被拒（假阳性防线不收窄）。
	res, err = w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA, Refs: []string{"C:\\Users\\John", "Doe\\proj\\a.txt"},
	})
	if err != nil {
		t.Fatalf("Inquiry(碎片): %v", err)
	}
	for i, c := range res.RefChecks {
		if c.OK {
			t.Fatalf("碎片 %q 不该核验通过（假阳性）: %+v", []string{"C:\\Users\\John", "Doe\\proj\\a.txt"}[i], c)
		}
	}
}

// last2ref 把 seq 折成 `e:<seq>` 引用（测试辅助）。
func last2ref(seq int64) string { return fmt.Sprintf("e:%d", seq) }

// TestPathRefSpacedUserDirFragmentRejected（p2-5 回归）：`C:\Users\John Doe` 这种
// "目录名带空格"的路径，其**空白分片**（`C:\Users\John`）不许进证据表——
// 否则伪造的 ref `C:\Users\John` 能核验通过（假阳性：证据里根本没有那个目录，
// 只是路径被空白切碎了；K24 口径：宁可误报不可放过）。
//
// 旧口径要求"每个空白分片都像路径"（`Doe` 不像 → 整串被判成"散文里并列的路径"）
// → 分片入表 → 伪造 ref 蒙过去。
func TestPathRefSpacedUserDirFragmentRejected(t *testing.T) {
	w := newWorld(t)
	const spaced = "C:\\Users\\John Doe"
	w.touch(w.childA1, spaced)
	last := w.sessionLastSeq(w.sidA)
	w.appendCompleted(w.childA1, "读完了", []string{last2ref(last)})

	// 整串核得过（它就是证据里的那条路径）。
	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA, Refs: []string{spaced},
	})
	if err != nil {
		t.Fatalf("Inquiry(整串): %v", err)
	}
	if len(res.RefChecks) != 1 || !res.RefChecks[0].OK {
		t.Fatalf("整串应核验通过: %+v", res.RefChecks)
	}
	// 空白切出的碎片必须被拒（假阳性防线）。
	forged := []string{"C:\\Users\\John", "Doe"}
	res, err = w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA, Refs: forged,
	})
	if err != nil {
		t.Fatalf("Inquiry(碎片): %v", err)
	}
	for i, c := range res.RefChecks {
		if c.OK {
			t.Fatalf("空白切出的碎片 %q 不该核验通过（假阳性）: %+v", forged[i], c)
		}
		if c.Detail == "" {
			t.Fatalf("失败的 ref %q 应给出原因", forged[i])
		}
	}
}

// TestPathRefProsePathStillVerifies（p2-5 的反面）：收窄假阳性**不能**把真引用一起收掉——
// 散文里提到的路径（"读完了 src/a.go"）仍要能核验。
//
// 判据：空白左侧的分片不是路径分片（这里是中文词"读完了"）→ 那是**词边界**，
// 碎片照常入表 → 那条路径仍可核验。
func TestPathRefProsePathStillVerifies(t *testing.T) {
	w := newWorld(t)
	// 证据只在 summary 正文里（没有独立的参数值兜底）。
	w.appendCompleted(w.childA1, "读完了 src/a.go，三段结构", nil)
	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA, Refs: []string{"src/a.go"},
	})
	if err != nil {
		t.Fatalf("Inquiry: %v", err)
	}
	if len(res.RefChecks) != 1 || !res.RefChecks[0].OK {
		t.Fatalf("散文里提到的路径应核验通过（假阴性）: %+v", res.RefChecks)
	}
}

// ---------------------------------------------------------------------------
// p2-A 回归：碎片抑制**按边界** + 触发只认**根路径**（上一版整串抑制 → 合法引用大面积核不过）
// ---------------------------------------------------------------------------

// TestPathRefAsciiProseExtensionlessPath（p2-A 回归①）：ASCII 散文里的**无扩展名**路径
// 必须能核验（`updated core/storage and committed` → 引用 `core/storage` 要过）。
//
// 上一版口径把"左片段像路径 + 空白两侧是路径字符"判成"路径内部空格"→ 整串只留整串
// → `core/storage` 核不过（合法引用假阴性）。收窄后：左片段是**相对**路径 → 不触发。
func TestPathRefAsciiProseExtensionlessPath(t *testing.T) {
	w := newWorld(t)
	w.touch(w.childA1, "updated core/storage and committed")
	last := w.sessionLastSeq(w.sidA)
	w.appendCompleted(w.childA1, "done", []string{last2ref(last)})

	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA, Refs: []string{"core/storage"},
	})
	if err != nil {
		t.Fatalf("Inquiry: %v", err)
	}
	if len(res.RefChecks) != 1 || !res.RefChecks[0].OK {
		t.Fatalf("散文里的无扩展名路径应核验通过（p2-A 假阴性）: %+v", res.RefChecks)
	}
}

// TestPathRefSeveralPathsInOneString（p2-A 回归②）：**同串多路径**时，
// 带空格的根路径只压掉**它自己**的碎片，同串的其它路径照常可核验
// （`C:\Users\John Doe core/store.go` → 引用 `core/store.go` 要过、
// 引用 `C:\Users\John` 必须被拒）。
//
// 上一版是**整串抑制**（串里任一空白像路径内部 → 整串只留整串）→ `core/store.go` 连坐
// 核不过（合法引用假阴性）；本版按边界抑制 → 各归各的。
func TestPathRefSeveralPathsInOneString(t *testing.T) {
	w := newWorld(t)
	const evidence = "C:\\Users\\John Doe core/store.go"
	w.touch(w.childA1, evidence)
	last := w.sessionLastSeq(w.sidA)
	w.appendCompleted(w.childA1, "done", []string{last2ref(last)})

	// ① 同串的另一条路径必须能核验。
	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA, Refs: []string{"core/store.go"},
	})
	if err != nil {
		t.Fatalf("Inquiry: %v", err)
	}
	if len(res.RefChecks) != 1 || !res.RefChecks[0].OK {
		t.Fatalf("同串的其它路径应核验通过（p2-A 假阴性）: %+v", res.RefChecks)
	}

	// ② 带空格根路径的碎片仍必须被拒（假阳性防线不能松）。
	res, err = w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA, Refs: []string{"C:\\Users\\John"},
	})
	if err != nil {
		t.Fatalf("Inquiry(碎片): %v", err)
	}
	if len(res.RefChecks) != 1 || res.RefChecks[0].OK {
		t.Fatalf("带空格根路径的碎片不该核验通过（假阳性）: %+v", res.RefChecks)
	}
	if res.RefChecks[0].Detail == "" {
		t.Fatal("失败的 ref 应给出原因（fail-close 也要让人知道卡在哪）")
	}
}

// TestPathRefExtensionPathFollowedByAsciiWord（p2-A 回归③）：**扩展名**路径后面跟一个
// ASCII 词（`src/a.go updated`）时，那条路径必须能核验。
//
// 收窄后的口径：只有根路径（盘符/前导 /）的左片段才触发"内部空格"判定，
// 相对路径一律不触发 → 路径照常入表。
func TestPathRefExtensionPathFollowedByAsciiWord(t *testing.T) {
	w := newWorld(t)
	w.touch(w.childA1, "src/a.go updated")
	last := w.sessionLastSeq(w.sidA)
	w.appendCompleted(w.childA1, "done", []string{last2ref(last)})

	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA, Refs: []string{"src/a.go"},
	})
	if err != nil {
		t.Fatalf("Inquiry: %v", err)
	}
	if len(res.RefChecks) != 1 || !res.RefChecks[0].OK {
		t.Fatalf("扩展名路径应核验通过（p2-A 假阴性）: %+v", res.RefChecks)
	}
}

// TestPathRefRootPathWithExtFollowedByAsciiWord（p3-G）：**根路径带扩展名**后面跟 ASCII 词
// （`C:\Users\John\report.doc final`）时，那条路径必须能核验——路径已完整，空白是词边界。
//
// p2-A 收窄后只有根路径触发"内部空格"判定；若不看扩展名，该形态会被误判成
// "带空格的路径"整条压掉（`C:\Users\John\report.doc` 核不过 = 假阴性）。
// 反面：无扩展名的前缀碎片仍必须被拒（假阳性防线不松）。
func TestPathRefRootPathWithExtFollowedByAsciiWord(t *testing.T) {
	w := newWorld(t)
	w.touch(w.childA1, "C:\\Users\\John\\report.doc final")
	last := w.sessionLastSeq(w.sidA)
	w.appendCompleted(w.childA1, "done", []string{last2ref(last)})

	// ① 带扩展名的根路径必须能核验（p3-G 假阴性）。
	res, err := w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA, Refs: []string{"C:\\Users\\John\\report.doc"},
	})
	if err != nil {
		t.Fatalf("Inquiry: %v", err)
	}
	if len(res.RefChecks) != 1 || !res.RefChecks[0].OK {
		t.Fatalf("带扩展名的根路径应核验通过（p3-G 假阴性）: %+v", res.RefChecks)
	}

	// ② 前缀碎片仍必须被拒（假阳性防线）。
	res, err = w.link.Inquiry(context.Background(), agentlink.InquiryRequest{
		Caller:    agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		SessionID: w.sidA, Refs: []string{"C:\\Users\\John"},
	})
	if err != nil {
		t.Fatalf("Inquiry(碎片): %v", err)
	}
	if len(res.RefChecks) != 1 || res.RefChecks[0].OK {
		t.Fatalf("前缀碎片不该核验通过（假阳性）: %+v", res.RefChecks)
	}
}
