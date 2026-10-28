package agentlink_test

// K18 Broadcast / Handoff / Inbox 的单测（core/agentlink）：K11 投递语义（gate 相交才收得到、
// 别 session subagent 收不到——负例）、拉模型收件箱、事件可重建、幂等、并发。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/discovery"
)

// gateTarget / subtreeTarget 构造投递目标（契约类型，指针字段由助手补上）。
func gateTarget(gateID string) contracts.AgentLinkTarget {
	return contracts.AgentLinkTarget{
		Type:   contracts.AgentLinkTargetTypeGate,
		GateID: ptrStr(gateID),
	}
}

func subtreeTarget(agent contracts.Ulid) contracts.AgentLinkTarget {
	return contracts.AgentLinkTarget{
		Type:    contracts.AgentLinkTargetTypeSubtree,
		AgentID: &agent,
	}
}

func TestBroadcastToGateDeliversToPeerMainAgents(t *testing.T) {
	w := newWorld(t)
	res, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From:   agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		Target: gateTarget(w.gateX),
		Text:   "我读完了 plan.md，谁要上下文",
	})
	if err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	if res.Kind != agentlink.KindBroadcast {
		t.Fatalf("Kind = %q，want %q", res.Kind, agentlink.KindBroadcast)
	}
	if res.Seq == 0 || res.EventID == "" {
		t.Fatalf("落库结果不完整: %+v", res)
	}
	// K11①：收件方 = 该 gate 活跃成员的**主 agent**——只有 B 的 amB；A 自己是发送方。
	if len(res.Recipients) != 1 {
		t.Fatalf("Recipients = %+v，want 1 个", res.Recipients)
	}
	if res.Recipients[0].SessionID != string(w.sidB) || res.Recipients[0].AgentID != string(w.amB) {
		t.Fatalf("Recipient 不符: %+v", res.Recipients[0])
	}
	if res.Recipients[0].Via != "gate" || len(res.Recipients[0].Gates) != 1 || res.Recipients[0].Gates[0] != w.gateX {
		t.Fatalf("Recipient.Via/Gates 不符: %+v", res.Recipients[0])
	}
	// 审计：发送方自己被挡下（reason=self）。
	if len(res.Dropped) != 1 || res.Dropped[0].SessionID != string(w.sidA) || res.Dropped[0].Reason != "self" {
		t.Fatalf("Dropped = %+v，want 只有发送方自己", res.Dropped)
	}

	// 消息是**事实**：durable 事件进共享流（F7/G23），一条不多一条不少。
	msgs := w.st.byKind(agentlink.KindBroadcast)
	if len(msgs) != 1 {
		t.Fatalf("事件流里应有 1 条 broadcast，得到 %d", len(msgs))
	}
	if msgs[0].Seq == nil || *msgs[0].Seq != res.Seq || msgs[0].ID != contracts.Ulid(res.EventID) {
		t.Fatalf("事件与结果对不上: %+v vs %+v", msgs[0], res)
	}
	if !msgs[0].Durable || msgs[0].SessionID != w.sidA || msgs[0].AgentID != w.amA {
		t.Fatalf("事件 envelope 不符: %+v", msgs[0])
	}
}

func TestBroadcastToSubtreeDeliversWithinTreeOnly(t *testing.T) {
	w := newWorld(t)
	res, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From:   agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		Target: subtreeTarget(w.childA1),
		Text:   "这块交给你，往下传",
	})
	if err != nil {
		t.Fatalf("Broadcast(subtree): %v", err)
	}
	// K11②：收件方 = 目标子树 ∩ 发送方的可见集（自己的父 + 自己的直系子）——
	// amA 的直系子是 childA1 与 siblingA2，与子树 {childA1, grandA1} 相交 = **只有 childA1**
	// （可见性是一跳：grandA1 是 amA 的**孙级**，不在可见集里，K27「不是全后代自动可见」）。
	got := map[string]bool{}
	for _, r := range res.Recipients {
		if r.Via != "subtree" || r.SessionID != string(w.sidA) {
			t.Fatalf("subtree 收件方形状不符: %+v", r)
		}
		got[r.AgentID] = true
	}
	if len(got) != 1 || !got[string(w.childA1)] {
		t.Fatalf("subtree 收件方 = %v，want {childA1}", got)
	}
	// 被挡掉的候选要有原因（兄弟 outside_subtree、隔代 not_visible、发送方自己 self）。
	reasons := map[string]string{}
	for _, d := range res.Dropped {
		reasons[d.AgentID] = d.Reason
	}
	if reasons[string(w.sibA2)] != "outside_subtree" {
		t.Fatalf("兄弟 siblingA2 应被挡（outside_subtree）: %+v", res.Dropped)
	}
	if reasons[string(w.grandA1)] != "not_visible" {
		t.Fatalf("孙级 grandA1 应被挡（not_visible）: %+v", res.Dropped)
	}
	if reasons[string(w.amA)] != "self" {
		t.Fatalf("发送方自己应被挡（self）: %+v", res.Dropped)
	}
}

func TestBroadcastRejectsCrossSessionSubtree(t *testing.T) {
	w := newWorld(t)
	// K11：别 session 的 subagent 完全不可见——跨 session 的 subtree 目标在写入时就拒。
	_, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From:   agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		Target: subtreeTarget(w.amB), // amB 属于 session B
		Text:   "越界投递",
	})
	if err == nil {
		t.Fatalf("跨 session subtree 目标应被拒")
	}
	if n := len(w.st.byKind(agentlink.KindBroadcast)); n != 0 {
		t.Fatalf("被拒的广播不该落库，得到 %d 条", n)
	}
	// 同理：subtree 指向不存在的 agent。
	if _, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From:   agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		Target: subtreeTarget(contracts.Ulid("MMMMMMMMMMMMMMMMMMMMMMM")),
		Text:   "指向空气",
	}); err == nil {
		t.Fatalf("未知 agent 的 subtree 目标应被拒")
	}
}

func TestBroadcastValidation(t *testing.T) {
	w := newWorld(t)
	from := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}
	cases := []struct {
		name string
		req  agentlink.BroadcastRequest
	}{
		{"空正文", agentlink.BroadcastRequest{From: from, Target: gateTarget(w.gateX), Text: "   "}},
		{"坏 gate", agentlink.BroadcastRequest{From: from, Target: gateTarget("bad gate 空格"), Text: "x"}},
		{"空 gate", agentlink.BroadcastRequest{From: from, Target: gateTarget(""), Text: "x"}},
		{"坏身份", agentlink.BroadcastRequest{From: agentlink.Identity{SessionID: w.sidA}, Target: gateTarget(w.gateX), Text: "x"}},
		{"坏目标类型", agentlink.BroadcastRequest{From: from, Target: contracts.AgentLinkTarget{Type: "everyone"}, Text: "x"}},
		{"subtree 缺 agent_id", agentlink.BroadcastRequest{From: from, Target: contracts.AgentLinkTarget{Type: contracts.AgentLinkTargetTypeSubtree}, Text: "x"}},
	}
	for _, c := range cases {
		if _, err := w.link.Broadcast(context.Background(), c.req); err == nil {
			t.Fatalf("%s：应被拒", c.name)
		}
	}
	if n := len(w.st.byKind(agentlink.KindBroadcast)); n != 0 {
		t.Fatalf("全部被拒的用例都不该落库，得到 %d 条", n)
	}

	// 缺 sink：fail-close 报错，不静默丢消息。
	noSink := agentlink.New(agentlink.Deps{Table: w.table})
	if _, err := noSink.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From: from, Target: gateTarget(w.gateX), Text: "x",
	}); err == nil {
		t.Fatalf("无 sink 时应报错")
	}
}

func TestHandoffCarriesStructuredPayload(t *testing.T) {
	w := newWorld(t)
	payload := json.RawMessage(`{"scope":"docs/","findings":["plan.md 三段"],"next":["写实现"],"open_questions":["留不留 spike"]}`)
	res, err := w.link.Handoff(context.Background(), agentlink.HandoffRequest{
		From:    agentlink.Identity{SessionID: w.sidA, AgentID: w.childA1},
		Target:  subtreeTarget(w.grandA1),
		Text:    "侦察完，交给下一层",
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("Handoff: %v", err)
	}
	if res.Kind != agentlink.KindHandoff {
		t.Fatalf("Kind = %q，want %q", res.Kind, agentlink.KindHandoff)
	}
	if len(res.Recipients) != 1 || res.Recipients[0].AgentID != string(w.grandA1) {
		t.Fatalf("Handoff 收件方 = %+v，want grandA1", res.Recipients)
	}

	// payload 原样进事件（开放对象，框架不解释），读回来一字不差。
	msgs := w.st.byKind(agentlink.KindHandoff)
	if len(msgs) != 1 {
		t.Fatalf("应有 1 条 handoff，得到 %d", len(msgs))
	}
	var p contracts.AgentLinkHandoffPayload
	if err := json.Unmarshal(msgs[0].Payload, &p); err != nil {
		t.Fatalf("unmarshal handoff payload: %v", err)
	}
	if string(p.Payload) != string(payload) {
		t.Fatalf("payload 被改写:\n got %s\nwant %s", p.Payload, payload)
	}
	if p.Text != "侦察完，交给下一层" {
		t.Fatalf("handoff text = %q", p.Text)
	}

	// 非对象 payload：拒（契约规定 type=object）。
	if _, err := w.link.Handoff(context.Background(), agentlink.HandoffRequest{
		From: agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}, Target: gateTarget(w.gateX),
		Text: "x", Payload: json.RawMessage(`["不是对象"]`),
	}); err == nil {
		t.Fatalf("非对象 payload 应被拒")
	}
	// 空 payload 合法（text 已承载交接说明）。
	if _, err := w.link.Handoff(context.Background(), agentlink.HandoffRequest{
		From: agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}, Target: gateTarget(w.gateX), Text: "只有文字",
	}); err != nil {
		t.Fatalf("空 payload 的 handoff 应合法: %v", err)
	}
}

func TestBroadcastIdempotencyKeyDeduplicates(t *testing.T) {
	w := newWorld(t)
	from := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}
	key := "run-42/broadcast/1"
	first, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From: from, Target: gateTarget(w.gateX), Text: "同一条消息", IdempotencyKey: key,
	})
	if err != nil {
		t.Fatalf("Broadcast#1: %v", err)
	}
	second, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From: from, Target: gateTarget(w.gateX), Text: "同一条消息（重试）", IdempotencyKey: key,
	})
	if err != nil {
		t.Fatalf("Broadcast#2: %v", err)
	}
	// 同一把钥匙 → 同一个事件 id、同一个 seq（存储层 ON CONFLICT(id) DO NOTHING 兜住）。
	if first.EventID != second.EventID || first.Seq != second.Seq {
		t.Fatalf("幂等钥匙没生效: %+v vs %+v", first, second)
	}
	if n := len(w.st.byKind(agentlink.KindBroadcast)); n != 1 {
		t.Fatalf("事件流里应只有 1 条，得到 %d", n)
	}
	// 换钥匙 → 新消息。
	third, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From: from, Target: gateTarget(w.gateX), Text: "另一条", IdempotencyKey: key + "/2",
	})
	if err != nil {
		t.Fatalf("Broadcast#3: %v", err)
	}
	if third.EventID == first.EventID {
		t.Fatalf("换钥匙应得到新事件")
	}
	// 无钥匙 → 每次一条新消息（广播不是"只能发一次"的操作）。
	if _, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From: from, Target: gateTarget(w.gateX), Text: "无钥匙",
	}); err != nil {
		t.Fatalf("Broadcast(无钥匙): %v", err)
	}
	if n := len(w.st.byKind(agentlink.KindBroadcast)); n != 3 {
		t.Fatalf("事件流里应有 3 条，得到 %d", n)
	}
}

// TestInboxPullsOnlyVisibleMessages 是 K11 投递语义的**负例**主场：
// gate 相交才收得到、别 session 的 subagent 收不到、兄弟 subtree 不可见、发送方看不到自己。
func TestInboxPullsOnlyVisibleMessages(t *testing.T) {
	w := newWorld(t)
	fromA := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}

	// ① 向 gate 广播（A → repo:X）。
	gateMsg, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From: fromA, Target: gateTarget(w.gateX), Text: "对 gate 说",
	})
	if err != nil {
		t.Fatalf("Broadcast(gate): %v", err)
	}
	// ② 向子树广播（A 的 childA1 子树）。
	subMsg, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From: fromA, Target: subtreeTarget(w.childA1), Text: "对子树说",
	})
	if err != nil {
		t.Fatalf("Broadcast(subtree): %v", err)
	}

	inbox := func(session, agent contracts.Ulid) []agentlink.Message {
		t.Helper()
		res, err := w.link.Inbox(context.Background(), agentlink.InboxQuery{SessionID: session, AgentID: agent})
		if err != nil {
			t.Fatalf("Inbox(%s): %v", session, err)
		}
		return res.Messages
	}
	countText := func(msgs []agentlink.Message, text string) int {
		n := 0
		for _, m := range msgs {
			if m.Text == text {
				n++
			}
		}
		return n
	}

	// gate 消息：B 的**主 agent**收得到（K11①）。
	if n := countText(inbox(w.sidB, w.amB), "对 gate 说"); n != 1 {
		t.Fatalf("B 的主 agent 应收到 1 条 gate 广播，得到 %d", n)
	}
	// B 的 subagent 收不到——**别 session 的 subagent 完全不可见**（负例）。
	if n := countText(inbox(w.sidB, w.subB1), "对 gate 说"); n != 0 {
		t.Fatalf("B 的 subagent 不该收到 gate 广播（K11①），得到 %d", n)
	}
	// C（gate 不相交）收不到。
	if n := countText(inbox(w.sidC, w.amC), "对 gate 说"); n != 0 {
		t.Fatalf("C 不该收到 A 的广播（gate 不相交），得到 %d", n)
	}
	// 发送方自己收不到（不给自己投递）。
	if n := countText(inbox(w.sidA, w.amA), "对 gate 说"); n != 0 {
		t.Fatalf("发送方不该收到自己的广播，得到 %d", n)
	}
	// session 级查询（不指定 agent）= 主 agent 视角。
	if n := countText(inbox(w.sidB, ""), "对 gate 说"); n != 1 {
		t.Fatalf("B 的 session 级查询应看到 gate 广播，得到 %d", n)
	}

	// subtree 消息：只在发送方 session 内、且只在**收件方集合**里
	// （= 目标子树 ∩ 发送方的 K11 可见集：amA 的直系子 childA1；孙级 grandA1 不可见）。
	if n := countText(inbox(w.sidA, w.childA1), "对子树说"); n != 1 {
		t.Fatalf("childA1 应收到子树广播，得到 %d", n)
	}
	if n := countText(inbox(w.sidA, w.grandA1), "对子树说"); n != 0 {
		t.Fatalf("grandA1 是 amA 的孙级，不该收到（K11/K27），得到 %d", n)
	}
	if n := countText(inbox(w.sidA, w.sibA2), "对子树说"); n != 0 {
		t.Fatalf("兄弟 siblingA2 不该收到（K11②），得到 %d", n)
	}
	if n := countText(inbox(w.sidA, w.amA), "对子树说"); n != 0 {
		t.Fatalf("发送方 amA 不该收到，得到 %d", n)
	}
	// 别 session 完全看不到这条子树消息（负例）。
	if n := countText(inbox(w.sidB, w.amB), "对子树说"); n != 0 {
		t.Fatalf("B 不该看到 A 的子树消息（别 session subagent 完全不可见），得到 %d", n)
	}
	// A 的 session 级视图能看到自己 session 里的子树消息。
	if n := countText(inbox(w.sidA, ""), "对子树说"); n != 1 {
		t.Fatalf("A 的 session 级查询应看到子树消息，得到 %d", n)
	}

	// 消息内容与原样一致（kind/seq/发送方/目标）。
	msgs := inbox(w.sidB, w.amB)
	if len(msgs) != 1 {
		t.Fatalf("B 的 inbox 应只有 1 条，得到 %d: %+v", len(msgs), msgs)
	}
	m := msgs[0]
	if m.Kind != agentlink.KindBroadcast || m.Seq != gateMsg.Seq || m.EventID != gateMsg.EventID {
		t.Fatalf("消息与落库结果对不上: %+v vs %+v", m, gateMsg)
	}
	if m.FromSessionID != string(w.sidA) || m.FromAgentID != string(w.amA) {
		t.Fatalf("消息发送方不符: %+v", m)
	}
	if m.Target.Type != contracts.AgentLinkTargetTypeGate || m.Target.GateID == nil || *m.Target.GateID != w.gateX {
		t.Fatalf("消息目标不符: %+v", m.Target)
	}
	_ = subMsg
}

func TestInboxSinceSeqAndLimit(t *testing.T) {
	w := newWorld(t)
	fromA := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}
	for i := 0; i < 3; i++ {
		if _, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
			From: fromA, Target: gateTarget(w.gateX), Text: fmt.Sprintf("第 %d 条", i),
		}); err != nil {
			t.Fatalf("Broadcast#%d: %v", i, err)
		}
	}
	// 全量：3 条。
	all, err := w.link.Inbox(context.Background(), agentlink.InboxQuery{SessionID: w.sidB, AgentID: w.amB})
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(all.Messages) != 3 {
		t.Fatalf("B 应收到 3 条，得到 %d", len(all.Messages))
	}
	// 增量：从最后一条的 seq 之后拉 → 空（拉模型的可重复轮询）。
	tail, err := w.link.Inbox(context.Background(), agentlink.InboxQuery{
		SessionID: w.sidB, AgentID: w.amB, SinceSeq: all.Messages[2].Seq,
	})
	if err != nil {
		t.Fatalf("Inbox(SinceSeq): %v", err)
	}
	if len(tail.Messages) != 0 {
		t.Fatalf("SinceSeq 之后不该有新消息，得到 %d", len(tail.Messages))
	}
	// 增量：从第一条的 seq 之后拉 → 后 2 条。
	rest, err := w.link.Inbox(context.Background(), agentlink.InboxQuery{
		SessionID: w.sidB, AgentID: w.amB, SinceSeq: all.Messages[0].Seq,
	})
	if err != nil {
		t.Fatalf("Inbox(SinceSeq#2): %v", err)
	}
	if len(rest.Messages) != 2 || rest.Messages[0].Text != "第 1 条" {
		t.Fatalf("增量拉取结果 = %+v", rest.Messages)
	}
	// Limit：按**交付条数**截断（不是"读多少条事件"）。
	lim, err := w.link.Inbox(context.Background(), agentlink.InboxQuery{
		SessionID: w.sidB, AgentID: w.amB, Limit: 1,
	})
	if err != nil {
		t.Fatalf("Inbox(Limit): %v", err)
	}
	if len(lim.Messages) != 1 {
		t.Fatalf("Limit=1 → %d 条", len(lim.Messages))
	}
}

// TestMessagesRebuildFromEventStream：广播/交接历史可从事件流**重建**（K18 可审计可重建）。
func TestMessagesRebuildFromEventStream(t *testing.T) {
	w := newWorld(t)
	fromA := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}
	if _, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From: fromA, Target: gateTarget(w.gateX), Text: "广播一",
	}); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	if _, err := w.link.Handoff(context.Background(), agentlink.HandoffRequest{
		From: fromA, Target: subtreeTarget(w.childA1), Text: "交接一",
		Payload: json.RawMessage(`{"k":"v"}`),
	}); err != nil {
		t.Fatalf("Handoff: %v", err)
	}

	// 从事件流重放折回全部消息（纯函数，无 IO）。
	folded := agentlink.FoldMessages(w.st.all())
	if len(folded) != 2 {
		t.Fatalf("折回 %d 条消息，want 2", len(folded))
	}
	if folded[0].Kind != agentlink.KindBroadcast || folded[0].Text != "广播一" {
		t.Fatalf("第一条 = %+v", folded[0])
	}
	if folded[1].Kind != agentlink.KindHandoff || folded[1].Text != "交接一" || string(folded[1].Payload) != `{"k":"v"}` {
		t.Fatalf("第二条 = %+v", folded[1])
	}
	// 顺序按 seq 升序（重建的时间轴 = 事件流的顺序）。
	if folded[0].Seq >= folded[1].Seq {
		t.Fatalf("折回的消息未按 seq 升序: %d, %d", folded[0].Seq, folded[1].Seq)
	}

	// 与"实时拉取"对账：B 的 inbox ∪ A 的 inbox ⊆ 折回的全集。
	for _, q := range []agentlink.InboxQuery{
		{SessionID: w.sidB, AgentID: w.amB},
		{SessionID: w.sidA},
	} {
		got, err := w.link.Inbox(context.Background(), q)
		if err != nil {
			t.Fatalf("Inbox: %v", err)
		}
		for _, m := range got.Messages {
			found := false
			for _, f := range folded {
				if f.EventID == m.EventID {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("inbox 里的消息 %s 不在重建结果里", m.EventID)
			}
		}
	}

	// 重建容错：坏载荷跳过（D7），不影响其余消息。
	evs := w.st.all()
	evs[len(evs)-1].Payload = json.RawMessage(`{"target":`)
	if got := agentlink.FoldMessages(evs); len(got) != 1 {
		t.Fatalf("坏载荷应被跳过，得到 %d 条", len(got))
	}
	// 非 agentlink kind 一律忽略。
	if got := agentlink.FoldMessages(nil); len(got) != 0 {
		t.Fatalf("空输入应折出空结果")
	}
}

// TestConcurrentBroadcastIsLossless：并发广播不丢不重（race 下验证）。
func TestConcurrentBroadcastIsLossless(t *testing.T) {
	w := newWorld(t)
	fromA := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}
	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
				From: fromA, Target: gateTarget(w.gateX), Text: fmt.Sprintf("并发 %d", i),
			}); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("并发广播出错: %v", err)
	}
	if got := len(w.st.byKind(agentlink.KindBroadcast)); got != n {
		t.Fatalf("并发广播落了 %d 条，want %d", got, n)
	}
	// 收件方拉得到全部 n 条（顺序按 seq）。
	msgs, err := w.link.Inbox(context.Background(), agentlink.InboxQuery{SessionID: w.sidB, AgentID: w.amB})
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(msgs.Messages) != n {
		t.Fatalf("B 收到 %d 条，want %d", len(msgs.Messages), n)
	}
	for i := 1; i < len(msgs.Messages); i++ {
		if msgs.Messages[i-1].Seq >= msgs.Messages[i].Seq {
			t.Fatalf("inbox 未按 seq 升序: %d, %d", msgs.Messages[i-1].Seq, msgs.Messages[i].Seq)
		}
	}

	// 同一把钥匙并发重试：只落一条（幂等由存储层保证，不靠锁）。
	var wg2 sync.WaitGroup
	for i := 0; i < n; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			if _, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
				From: fromA, Target: gateTarget(w.gateX), Text: "同钥匙", IdempotencyKey: "same-key",
			}); err != nil {
				t.Errorf("同钥匙广播出错: %v", err)
			}
		}()
	}
	wg2.Wait()
	if got := len(w.st.byKind(agentlink.KindBroadcast)); got != n+1 {
		t.Fatalf("同钥匙并发应只多落 1 条，现有 %d 条（want %d）", got, n+1)
	}
}

// TestInboxFailClosed：缺 reader / 坏 session id / nil Link。
func TestInboxFailClosed(t *testing.T) {
	w := newWorld(t)
	noReader := agentlink.New(agentlink.Deps{Table: w.table})
	if _, err := noReader.Inbox(context.Background(), agentlink.InboxQuery{SessionID: w.sidB}); err == nil {
		t.Fatalf("无 reader 时 Inbox 应报错")
	}
	if _, err := w.link.Inbox(context.Background(), agentlink.InboxQuery{SessionID: "不是ULID"}); err == nil {
		t.Fatalf("坏 session id 应报错")
	}
	var nilLink *agentlink.Link
	if _, err := nilLink.Inbox(context.Background(), agentlink.InboxQuery{SessionID: w.sidB}); err == nil {
		t.Fatalf("nil Link 应报错")
	}
	// 空事件流：空 inbox，不是错误。
	fresh := agentlink.New(agentlink.Deps{Table: discovery.New(), Reader: newMemStore(), Sink: newMemStore()})
	got, err := fresh.Inbox(context.Background(), agentlink.InboxQuery{SessionID: w.sidB})
	if err != nil || len(got.Messages) != 0 {
		t.Fatalf("空流 inbox: err=%v got=%+v", err, got)
	}
}

// ---------------------------------------------------------------------------
// 评审轮修复的回归用例（F1 / F3 / F5 / F6）
// ---------------------------------------------------------------------------

// TestF1IdempotencyKeyIsolatedPerSender（F1）：幂等钥匙**按发送方隔离**。
//
// 不隔离的后果：两个不同 sender 用同一把钥匙 → 同一个事件 id → 存储层的
// ON CONFLICT(id) DO NOTHING 把第二条**静默吃掉**，而 MessageResult 会把**别人**那条
// 事件的 id/seq 当成自己的结果报出去（跨 session 消息静默丢失）。
func TestF1IdempotencyKeyIsolatedPerSender(t *testing.T) {
	w := newWorld(t)
	key := "run-42/broadcast/1"
	fromA := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}
	fromB := agentlink.Identity{SessionID: w.sidB, AgentID: w.amB}

	resA, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From: fromA, Target: gateTarget(w.gateX), Text: "A 说的", IdempotencyKey: key,
	})
	if err != nil {
		t.Fatalf("Broadcast(A): %v", err)
	}
	resB, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From: fromB, Target: gateTarget(w.gateX), Text: "B 说的", IdempotencyKey: key,
	})
	if err != nil {
		t.Fatalf("Broadcast(B): %v", err)
	}
	// 两条事件都在（各归各的发送方）。
	msgs := w.st.byKind(agentlink.KindBroadcast)
	if len(msgs) != 2 {
		t.Fatalf("事件流里应有 2 条 broadcast（A 与 B 各一条），得到 %d", len(msgs))
	}
	bySession := map[string]contracts.Event{}
	for _, ev := range msgs {
		bySession[string(ev.SessionID)] = ev
	}
	evA, okA := bySession[string(w.sidA)]
	evB, okB := bySession[string(w.sidB)]
	if !okA || !okB {
		t.Fatalf("两条消息的归属不对: %+v", msgs)
	}
	if evA.AgentID != w.amA || evB.AgentID != w.amB {
		t.Fatalf("envelope 的 agent 归属不对: A=%s B=%s", evA.AgentID, evB.AgentID)
	}
	// 各自的结果报告**自己**那条事件（不是别人的）。
	if resA.EventID != string(evA.ID) || resA.Seq != *evA.Seq {
		t.Fatalf("A 的结果指向了别人的事件: %+v vs %+v", resA, evA)
	}
	if resB.EventID != string(evB.ID) || resB.Seq != *evB.Seq {
		t.Fatalf("B 的结果指向了别人的事件: %+v vs %+v", resB, evB)
	}
	if resA.EventID == resB.EventID {
		t.Fatalf("不同 sender 的同钥匙撞了同一个事件 id: %s", resA.EventID)
	}
	// 同一 sender 的同一把钥匙仍然是幂等的（重试不重复落库）。
	again, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From: fromA, Target: gateTarget(w.gateX), Text: "A 说的（重试）", IdempotencyKey: key,
	})
	if err != nil {
		t.Fatalf("Broadcast(A 重试): %v", err)
	}
	if again.EventID != resA.EventID || again.Seq != resA.Seq {
		t.Fatalf("同一 sender 的同钥匙应幂等: %+v vs %+v", again, resA)
	}
	if n := len(w.st.byKind(agentlink.KindBroadcast)); n != 2 {
		t.Fatalf("重试不该多落事件，现有 %d 条", n)
	}
}

// TestF1ForeignEventReadBackFailsClosed（F1 后半段）：落库回读的行归属不符 → 报错。
//
// 这是"事件 id 撞了别人的那条"的最后一道闸：宁可报错，也不把别人的 id/seq 当成
// 自己的结果报出去。
func TestF1ForeignEventReadBackFailsClosed(t *testing.T) {
	w := newWorld(t)
	bad := agentlink.New(agentlink.Deps{Table: w.table, Sink: foreignSink{session: w.sidB}})
	_, err := bad.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From:   agentlink.Identity{SessionID: w.sidA, AgentID: w.amA},
		Target: gateTarget(w.gateX),
		Text:   "撞车了",
	})
	if err == nil {
		t.Fatalf("回读归属不符时应报错（fail-close）")
	}
	if !errors.Is(err, agentlink.ErrForeignEvent) {
		t.Fatalf("错误类型 = %v，want ErrForeignEvent", err)
	}
}

// foreignSink 是个"坏"存储：落库回读时把事件的归属换成别人的 session
// （模拟事件 id 撞了别人的那条）。
type foreignSink struct{ session contracts.Ulid }

func (s foreignSink) Append(_ context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	out := make([]contracts.Event, 0, len(evs))
	for _, ev := range evs {
		ev.SessionID = s.session
		out = append(out, ev)
	}
	return out, nil
}

// TestF3SubtreeTargetAtAncestorHidesSiblings（F3）：subtree 目标指向**祖先**时，
// 兄弟（与旁支）收不到——K11②「兄弟 subagent 互相不可见」+ K27「向上只一跳」。
func TestF3SubtreeTargetAtAncestorHidesSiblings(t *testing.T) {
	w := newWorld(t)
	// 子 → 父：childA1 向 subtree(amA) 发消息。
	res, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From:   agentlink.Identity{SessionID: w.sidA, AgentID: w.childA1},
		Target: subtreeTarget(w.amA),
		Text:   "往上报",
	})
	if err != nil {
		t.Fatalf("Broadcast(subtree=祖先): %v", err)
	}
	got := map[string]bool{}
	for _, r := range res.Recipients {
		if r.Via != "subtree" || r.SessionID != string(w.sidA) {
			t.Fatalf("subtree 收件方形状不符: %+v", r)
		}
		got[r.AgentID] = true
	}
	if got[string(w.sibA2)] {
		t.Fatalf("兄弟 siblingA2 不该收到（K11②）：%+v", res.Recipients)
	}
	// 父可见（向上一跳）：amA 收得到。
	if !got[string(w.amA)] {
		t.Fatalf("父 amA 应收到（父↔直系子可见）：%+v", res.Recipients)
	}
	// 被挡掉的候选带原因。
	reasons := map[string]string{}
	for _, d := range res.Dropped {
		reasons[d.AgentID] = d.Reason
	}
	if reasons[string(w.sibA2)] != "not_visible" {
		t.Fatalf("兄弟应被记为 not_visible：%+v", res.Dropped)
	}
	// 读时同口径：兄弟的 inbox 里没有这条消息（critic 实测的泄漏点）。
	msgs, err := w.link.Inbox(context.Background(), agentlink.InboxQuery{SessionID: w.sidA, AgentID: w.sibA2})
	if err != nil {
		t.Fatalf("Inbox(兄弟): %v", err)
	}
	for _, m := range msgs.Messages {
		if m.Text == "往上报" {
			t.Fatalf("兄弟 siblingA2 的 inbox 收到了祖先目标的消息（F3 泄漏）")
		}
	}
	// 父的 inbox 收得到。
	msgs, err = w.link.Inbox(context.Background(), agentlink.InboxQuery{SessionID: w.sidA, AgentID: w.amA})
	if err != nil {
		t.Fatalf("Inbox(父): %v", err)
	}
	found := false
	for _, m := range msgs.Messages {
		if m.Text == "往上报" {
			found = true
		}
	}
	if !found {
		t.Fatalf("父 amA 的 inbox 应收到这条消息")
	}
}

// TestF3SubtreeTargetAcrossBranchesDeliversNothing（F3）：兄弟分支之间互相不可见——
// 向**别人的子树**发消息，投递不到任何人。
func TestF3SubtreeTargetAcrossBranchesDeliversNothing(t *testing.T) {
	w := newWorld(t)
	res, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
		From:   agentlink.Identity{SessionID: w.sidA, AgentID: w.sibA2},
		Target: subtreeTarget(w.childA1),
		Text:   "越界投递",
	})
	if err != nil {
		t.Fatalf("Broadcast(兄弟的子树): %v", err)
	}
	if len(res.Recipients) != 0 {
		t.Fatalf("兄弟分支之间不该有收件方: %+v", res.Recipients)
	}
	// 被投的那棵子树里的 agent 也拉不到。
	for _, a := range []contracts.Ulid{w.childA1, w.grandA1} {
		msgs, err := w.link.Inbox(context.Background(), agentlink.InboxQuery{SessionID: w.sidA, AgentID: a})
		if err != nil {
			t.Fatalf("Inbox(%s): %v", a, err)
		}
		for _, m := range msgs.Messages {
			if m.Text == "越界投递" {
				t.Fatalf("agent %s 收到了兄弟分支的消息（F3 泄漏）", a)
			}
		}
	}
}

// TestF5InboxLimitCountsDeliveredMessages（F5）：Limit 按**交付条数**算。
//
// 旧口径按"扫到的 agentlink 消息条数"收手 → 噪声消息占掉额度后，真消息永远扫不到
// （limit=1 时永久 0 条，而且游标只能靠收到的消息推导 → 卡死）。
func TestF5InboxLimitCountsDeliveredMessages(t *testing.T) {
	w := newWorld(t)
	fromA := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}
	// 噪声：向 B 根本不在的 gate 广播 300 条（B 收不到，但它们占"扫到的消息"额度）。
	for i := 0; i < 300; i++ {
		if _, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
			From: fromA, Target: gateTarget("repo:噪声"), Text: fmt.Sprintf("噪声 %d", i),
		}); err != nil {
			t.Fatalf("Broadcast(噪声#%d): %v", i, err)
		}
	}
	// 真消息：向 B 在的 gate 广播 3 条。
	for i := 0; i < 3; i++ {
		if _, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
			From: fromA, Target: gateTarget(w.gateX), Text: fmt.Sprintf("真消息 %d", i),
		}); err != nil {
			t.Fatalf("Broadcast(真#%d): %v", i, err)
		}
	}
	// limit=1：必须拿到**那条真消息**（不是 0 条）。
	one, err := w.link.Inbox(context.Background(), agentlink.InboxQuery{
		SessionID: w.sidB, AgentID: w.amB, Limit: 1,
	})
	if err != nil {
		t.Fatalf("Inbox(limit=1): %v", err)
	}
	if len(one.Messages) != 1 || !strings.HasPrefix(one.Messages[0].Text, "真消息") {
		t.Fatalf("limit=1 应拿到 1 条真消息，得到 %+v", one.Messages)
	}
	// limit=10：3 条真消息全拿到（噪声不该占额度）。
	ten, err := w.link.Inbox(context.Background(), agentlink.InboxQuery{
		SessionID: w.sidB, AgentID: w.amB, Limit: 10,
	})
	if err != nil {
		t.Fatalf("Inbox(limit=10): %v", err)
	}
	if len(ten.Messages) != 3 {
		t.Fatalf("limit=10 应拿到 3 条真消息，得到 %d: %+v", len(ten.Messages), ten.Messages)
	}
	// 游标水位：扫过的都推进了（噪声也算——否则下一轮还要重扫）。
	if one.NextSeq == 0 || ten.NextSeq == 0 {
		t.Fatalf("Inbox 应回报扫描水位 next_seq: %+v / %+v", one, ten)
	}
	if ten.NextSeq < one.NextSeq {
		t.Fatalf("水位不该倒退: %d < %d", ten.NextSeq, one.NextSeq)
	}
	// 游标可用：拿水位接着拉 → 剩下的真消息补得回来（不再"永久 0 条"）。
	rest, err := w.link.Inbox(context.Background(), agentlink.InboxQuery{
		SessionID: w.sidB, AgentID: w.amB, SinceSeq: one.NextSeq, Limit: 10,
	})
	if err != nil {
		t.Fatalf("Inbox(续拉): %v", err)
	}
	if len(rest.Messages) != 2 {
		t.Fatalf("续拉应拿到剩下 2 条，得到 %d: %+v", len(rest.Messages), rest.Messages)
	}
}

// TestF6InboxScansInBoundedBatches（F6/#4）：Inbox **流式**扫描——逐批过滤、结果里只留
// 交付出去的消息，不把整条流攒在内存里。
//
// 断言要**能真的红**（#4）：夹具事件数必须**远大于**单批上限，否则"每批 <= 上限"恒真
// （变异"一次把整条流读进来"也测不出来）。这里造 3000 条消息（≈3009 条事件，
// 单批上限 InboxScanBatch=256）：
//
//	批大小 ≤ InboxScanBatch*2   → 一次读完（3009 条）必红；
//	批次数 ≥ ceil(N/批上限)    → 一次读完（1 次）必红。
func TestF6InboxScansInBoundedBatches(t *testing.T) {
	w := newWorld(t)
	fromA := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}
	const msgs = 3000
	for i := 0; i < msgs; i++ {
		if _, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
			From: fromA, Target: gateTarget(w.gateX), Text: fmt.Sprintf("第 %d 条", i),
		}); err != nil {
			t.Fatalf("Broadcast#%d: %v", i, err)
		}
	}
	counting := &countingReader{inner: w.st}
	l := agentlink.New(agentlink.Deps{Table: w.table, Reader: counting, Sink: w.st})
	res, err := l.Inbox(context.Background(), agentlink.InboxQuery{SessionID: w.sidB, AgentID: w.amB})
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(res.Messages) != msgs {
		t.Fatalf("应交付 %d 条，得到 %d", msgs, len(res.Messages))
	}
	// 事件总数（消息 + 世界的开局事件）——批次数的下界按它算。
	total := 0
	for _, n := range counting.sizes {
		total += n
	}
	if total < msgs {
		t.Fatalf("读到的件数 = %d，want >= %d", total, msgs)
	}
	// ① 每批有界：不超过单批上限的两倍（一次读完会读到几千条 → 红）。
	for _, n := range counting.sizes {
		if n > agentlink.InboxScanBatch*2 {
			t.Fatalf("单批读太多事件: %d（上限 %d）——不该一次把整条流读进来", n, agentlink.InboxScanBatch*2)
		}
	}
	// ② 批次数下界：至少要 ceil(总事件/单批上限) 次读（一次读完 → 红）。
	wantCalls := (total + agentlink.InboxScanBatch - 1) / agentlink.InboxScanBatch
	if counting.calls < wantCalls {
		t.Fatalf("分批读次数 = %d，want >= %d（总事件 %d，单批上限 %d）",
			counting.calls, wantCalls, total, agentlink.InboxScanBatch)
	}
}

// countingReader 记读了多少批、每批多少条（测试夹具）。
type countingReader struct {
	inner interface {
		ReadFrom(ctx context.Context, sessionID *contracts.Ulid, fromSeq int64, limit int) ([]contracts.Event, error)
	}
	calls int
	sizes []int
}

func (r *countingReader) ReadFrom(ctx context.Context, sessionID *contracts.Ulid, fromSeq int64, limit int) ([]contracts.Event, error) {
	evs, err := r.inner.ReadFrom(ctx, sessionID, fromSeq, limit)
	r.calls++
	r.sizes = append(r.sizes, len(evs))
	return evs, err
}

// TestF5InboxCursorNeverGoesBackwards（#2）：追平后再轮询，next_seq **不变小**、
// 已投的消息**不重复投递**。
//
// 旧口径：追平态（这轮没扫到新事件）next_seq 归零 → 调用方按文档"SinceSeq=NextSeq"
// 轮询会退回 0 → 从头重扫 → 已投的消息重复投递、永不收敛（critic 实测 4 轮每条投 3 次）。
func TestInboxCursorNeverGoesBackwardsOnCaughtUp(t *testing.T) {
	w := newWorld(t)
	fromA := agentlink.Identity{SessionID: w.sidA, AgentID: w.amA}
	const n = 3
	for i := 0; i < n; i++ {
		if _, err := w.link.Broadcast(context.Background(), agentlink.BroadcastRequest{
			From: fromA, Target: gateTarget(w.gateX), Text: fmt.Sprintf("消息 %d", i),
		}); err != nil {
			t.Fatalf("Broadcast#%d: %v", i, err)
		}
	}
	// 连续轮询 4 轮，每轮都按上一轮的 next_seq 继续（调用方的标准姿势）。
	cursor := int64(0)
	delivered := 0
	for round := 1; round <= 4; round++ {
		res, err := w.link.Inbox(context.Background(), agentlink.InboxQuery{
			SessionID: w.sidB, AgentID: w.amB, SinceSeq: cursor,
		})
		if err != nil {
			t.Fatalf("Inbox(第 %d 轮): %v", round, err)
		}
		if res.NextSeq < cursor {
			t.Fatalf("第 %d 轮：next_seq 倒退了（%d < %d）——追平态不能把游标归零",
				round, res.NextSeq, cursor)
		}
		delivered += len(res.Messages)
		cursor = res.NextSeq
	}
	if delivered != n {
		t.Fatalf("4 轮累计交付 %d 条，want %d（追平后不得重复投递）", delivered, n)
	}
	// 追平态本身也要回填游标（不是零值）。
	res, err := w.link.Inbox(context.Background(), agentlink.InboxQuery{
		SessionID: w.sidB, AgentID: w.amB, SinceSeq: cursor,
	})
	if err != nil {
		t.Fatalf("Inbox(追平): %v", err)
	}
	if len(res.Messages) != 0 {
		t.Fatalf("追平后不该再交付消息: %+v", res.Messages)
	}
	if res.NextSeq != cursor {
		t.Fatalf("追平态的 next_seq = %d，want %d（只增不减）", res.NextSeq, cursor)
	}
}
