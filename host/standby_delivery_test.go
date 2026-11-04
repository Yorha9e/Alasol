package host

// 本文件是 standby **交付去重**（交付集合排除）的验收用例（critic 终验）。
//
// # 机制（集合，不是水位）
//
//	一次早到检查 = **宽范围**（SinceSeq=0）里找「匹配 wait kind ∧ **seq ∉ 已交付
//	集合**」的最新一条；交付后把它的 seq 加进集合（记在 completed 结果的结构化
//	载荷里：{"delivered_seq":N}）。
//
//	**为什么不能用"水位"**（已交付的最大 seq）：水位把"已交付"约成了"seq ≤ 某值"
//	——别的 kind 的旧消息（handoff）会被一次 broadcast 的交付顺带盖掉（seq 更小 →
//	永远捞不到），同 kind 的多条未交付消息也被最新那条永久盖住。集合排除只问
//	"这条消息**具体**交付过没有"。
//
// # 六条钉子
//
//	① 跨 kind：handoff 早到 → standby(message) 交付 broadcast → standby(handoff)
//	   **能**交付那条 handoff（不被水位盖掉）；
//	② 重复交付：连调两次（1 条旧消息）→ 第二次真等待不重复；
//	③ 同 kind 多条未交付：先交付最新，下一次交付旧的（逐条补交）；
//	④ 误认领：认领键是 **CausationID**（不是 tool_call_id）→ 同 id 的别的工具不被认领；
//	⑤ blob 外置：标记经 blob 读回 → 第二次不重复交付；
//	⑥ 超时不进集合：超时的交付标记是 0 → 之后到达的消息下一次能被捞到。
//
// 为什么是 internal（package host）：交付集合的执行器内部口径（deliveredSeqs /
// 交付标记）与时间口径都是包内符号——黑箱面看不见"交付了哪一条"。

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// standbyDeliveryResult 造一份带交付标记的工具结果（测试夹具）。
func standbyDeliveryResult(seq int64) contracts.ToolResult {
	data, _ := json.Marshal(standbyDelivery{DeliveredSeq: seq})
	text := "夹具"
	return contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &text, Data: data}}}
}

// standbyCompletedDelivery 取一条 completed 事件里的交付标记（没有 → 0）。
//
// 读法与执行器同款：结构化载荷（Content[0].Data）；外置了（BinaryRef 非空、Data 空）
// → **读不到**（返回 0 + false）——测试据此能看出"标记被外置了"。
func standbyCompletedDelivery(ev contracts.Event) (int64, bool) {
	var p contracts.ToolCallCompletedPayload
	if json.Unmarshal(ev.Payload, &p) != nil {
		return 0, false
	}
	seq, ok := deliverySeqInResult(p.Result)
	return seq, ok
}

// ---------------------------------------------------------------------------
// ① 跨 kind：handoff 早到不被 broadcast 的交付盖掉
// ---------------------------------------------------------------------------

// TestStandbyCrossKindDeliveryNotCovered：handoff 早到（seq 更早）→ 第一次 standby
// （等 message）交付 broadcast → 第二次 standby（等 handoff）**能**交付那条 handoff。
//
// 水位口径会在这里失灵：第一次交付的是 broadcast（seq 更大）→ 水位抬到它 →
// handoff（seq 更小）落在水位之下 → 第二次 standby 永远捞不到（"未等到"）。
func TestStandbyCrossKindDeliveryNotCovered(t *testing.T) {
	standbyTimeKnobs(t, 200*time.Millisecond, 30*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		"peer":  {{text: "在"}},
		"hello": {{text: "我先热身"}},
		"waiter1": {
			{tool: ToolStandby, args: `{"wait":"message","timeout_min":10,"on_timeout":"return"}`},
			{text: "收到一"},
		},
		"waiter2": {
			{tool: ToolStandby, args: `{"wait":"handoff","timeout_min":10,"on_timeout":"return"}`},
			{text: "收到二"},
		},
	})
	ctx := context.Background()
	sidPeer := standbyStartPeer(t, h, ctx, "peer", standbyGate)
	sidWait := NewSession()
	if _, err := h.RunPrompt(ctx, sidWait, "hello"); err != nil {
		t.Fatalf("RunPrompt(hello): %v", err)
	}
	if _, err := h.JoinGate(ctx, sidWait, standbyGate); err != nil {
		t.Fatalf("JoinGate(waiter): %v", err)
	}

	// 先 handoff（seq 更早），后 broadcast（seq 更晚）。
	if _, err := h.Handoff(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.HandoffRequest{
		Target: standbyGateTarget(standbyGate),
		Text:   "交接消息",
	}); err != nil {
		t.Fatalf("Handoff: %v", err)
	}
	if _, err := h.Broadcast(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.BroadcastRequest{
		Target: standbyGateTarget(standbyGate),
		Text:   "广播消息",
	}); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}

	// 第一次 standby（等 message）→ 交付 broadcast。
	if _, err := h.RunPrompt(ctx, sidWait, "waiter1"); err != nil {
		t.Fatalf("RunPrompt(waiter1): %v", err)
	}
	text := standbyCompletedText(t, h, sidWait)
	if !strings.Contains(text, "广播消息") {
		t.Fatalf("第一次 standby（等 message）该交付广播，得到 %q", text)
	}

	// 第二次 standby（等 handoff）→ **能**交付那条早到的 handoff。
	if _, err := h.RunPrompt(ctx, sidWait, "waiter2"); err != nil {
		t.Fatalf("RunPrompt(waiter2): %v", err)
	}
	text = standbyCompletedText(t, h, sidWait)
	if !strings.Contains(text, "交接消息") {
		t.Fatalf("第二次 standby（等 handoff）该交付早到的交接（不能被水位盖掉），得到 %q", text)
	}
}

// ---------------------------------------------------------------------------
// ③ 同 kind 多条未交付：逐条补交
// ---------------------------------------------------------------------------

// TestStandbySameKindMultipleUndeliveredDeliveredOneByOne：同 kind 的两条消息都早到
// 未交付 → 第一次 standby 交付**最新**那条 → 第二次 standby 交付**旧的**那条
// （逐条补交：不被最新那条永久盖住）。
func TestStandbySameKindMultipleUndeliveredDeliveredOneByOne(t *testing.T) {
	standbyTimeKnobs(t, 200*time.Millisecond, 30*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		"peer":  {{text: "在"}},
		"hello": {{text: "我先热身"}},
		"waiter1": {
			{tool: ToolStandby, args: `{"wait":"message","timeout_min":10,"on_timeout":"return"}`},
			{text: "收到一"},
		},
		"waiter2": {
			{tool: ToolStandby, args: `{"wait":"message","timeout_min":10,"on_timeout":"return"}`},
			{text: "收到二"},
		},
	})
	ctx := context.Background()
	sidPeer := standbyStartPeer(t, h, ctx, "peer", standbyGate)
	sidWait := NewSession()
	if _, err := h.RunPrompt(ctx, sidWait, "hello"); err != nil {
		t.Fatalf("RunPrompt(hello): %v", err)
	}
	if _, err := h.JoinGate(ctx, sidWait, standbyGate); err != nil {
		t.Fatalf("JoinGate(waiter): %v", err)
	}

	// 两条同类消息都早到（先旧后新）。
	if _, err := h.Broadcast(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.BroadcastRequest{
		Target: standbyGateTarget(standbyGate),
		Text:   "旧的那条",
	}); err != nil {
		t.Fatalf("Broadcast(旧): %v", err)
	}
	if _, err := h.Broadcast(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.BroadcastRequest{
		Target: standbyGateTarget(standbyGate),
		Text:   "新的那条",
	}); err != nil {
		t.Fatalf("Broadcast(新): %v", err)
	}

	// 第一次 standby → 交付**最新**那条。
	if _, err := h.RunPrompt(ctx, sidWait, "waiter1"); err != nil {
		t.Fatalf("RunPrompt(waiter1): %v", err)
	}
	text := standbyCompletedText(t, h, sidWait)
	if !strings.Contains(text, "新的那条") {
		t.Fatalf("第一次 standby 该交付最新那条，得到 %q", text)
	}

	// 第二次 standby → 交付**旧的**那条（逐条补交）。
	if _, err := h.RunPrompt(ctx, sidWait, "waiter2"); err != nil {
		t.Fatalf("RunPrompt(waiter2): %v", err)
	}
	text = standbyCompletedText(t, h, sidWait)
	if !strings.Contains(text, "旧的那条") {
		t.Fatalf("第二次 standby 该补交旧的那条（不被最新那条盖住），得到 %q", text)
	}
}

// ---------------------------------------------------------------------------
// ④ 认领键 = CausationID（不是 tool_call_id）
// ---------------------------------------------------------------------------

// TestStandbyDeliveredSetClaimsByCausationID：认领历史 completed 用
// **CausationID**（completed 指向同一次调用的 intent 事件 id）——不是 tool_call_id。
//
// 为什么：completed 载荷里**没有工具名**，按 tool_call_id 认领会**误认领**别的工具
// （provider 复用 tool_call_id 时：standby(id=SB1) 与 read(id=SB1) 的 completed
// 都叫 SB1 → read 的交付记录被当成 standby 的 → 集合里混进不该有的 seq）。
//
// 夹具：standby 的 completed（delivered_seq=100）+ read 的 completed（同样
// tool_call_id=SB1，delivered_seq=999）→ 集合**只该含 100**。
func TestStandbyDeliveredSetClaimsByCausationID(t *testing.T) {
	st, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"), sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	sid := contracts.Ulid(core.NewUlid())
	aid := contracts.Ulid(core.NewUlid())
	standbyIntentID := core.NewUlid()
	readIntentID := core.NewUlid()
	const callID = "SB1"

	appendIntent := func(id contracts.Ulid, toolName string) {
		t.Helper()
		raw, merr := json.Marshal(contracts.ToolCallIntentPayload{
			ToolCallID: contracts.ProviderId(callID),
			ToolName:   toolName,
		})
		if merr != nil {
			t.Fatalf("Marshal: %v", merr)
		}
		if _, aerr := st.Append(context.Background(), []contracts.Event{{
			V: 1, ID: id, Kind: loop.KindToolCallIntent, TS: "2026-01-01T00:00:00Z",
			SessionID: sid, AgentID: aid, Durable: true, Payload: raw,
		}}); aerr != nil {
			t.Fatalf("Append(intent): %v", aerr)
		}
	}
	appendCompleted := func(causation contracts.Ulid, delivered int64) {
		t.Helper()
		raw, merr := json.Marshal(contracts.ToolCallCompletedPayload{
			ToolCallID: contracts.ProviderId(callID),
			Result:     standbyDeliveryResult(delivered),
		})
		if merr != nil {
			t.Fatalf("Marshal: %v", merr)
		}
		cause := causation
		if _, aerr := st.Append(context.Background(), []contracts.Event{{
			V: 1, ID: core.NewUlid(), Kind: loop.KindToolCallCompleted, TS: "2026-01-01T00:00:01Z",
			SessionID: sid, AgentID: aid, Durable: true, CausationID: &cause, Payload: raw,
		}}); aerr != nil {
			t.Fatalf("Append(completed): %v", aerr)
		}
	}

	// standby 的这次调用（工具名 standby）。
	appendIntent(standbyIntentID, ToolStandby)
	appendCompleted(standbyIntentID, 100)
	// 别的工具（read）**复用了同一个 tool_call_id**。
	appendIntent(readIntentID, "read")
	appendCompleted(readIntentID, 999)

	ex := newStandbyExecutor(standbyDeps{store: st})
	ref := standbyIntentRef{SessionID: sid, AgentID: aid, Seq: 1}
	set, err := ex.deliveredSeqs(context.Background(), ref)
	if err != nil {
		t.Fatalf("deliveredSeqs: %v", err)
	}
	if len(set) != 1 || !set[100] {
		t.Fatalf("已交付集合 = %v，want {100}（read 的 completed 不该被认领）", set)
	}
}

// ---------------------------------------------------------------------------
// ⑤ blob 外置：标记经 blob 读回
// ---------------------------------------------------------------------------

// TestStandbyDeliveredSetSurvivesBlobExternalization：结果被**外置成 blob**时
// （BinaryRef 非空、结构化载荷整体在 blob 里）→ 交付标记要**经 blob 读回**，
// 否则已交付集合退化 → 第二次 standby 重复交付同一条旧消息。
//
// 口径：BlobMaxInline=512（有效阈值 = max(512, 摘要预算 768) = 768 字节）+ 一条
// 足够长的中文消息（唤醒摘要 ≈ 800+ 字节）→ completed 的 result 确实被外置。
func TestStandbyDeliveredSetSurvivesBlobExternalization(t *testing.T) {
	standbyTimeKnobs(t, time.Second, 30*time.Second, 20*time.Millisecond)
	dir := t.TempDir()
	h, err := New(Config{
		DBPath:   filepath.Join(dir, "events.db"),
		Model:    "standby-blob-test",
		System:   "standby-blob-test",
		MaxTurns: 4,
		Allow:    []string{ToolStandby},
		Provider: &standbyScriptProvider{plans: map[string][]standbyStep{
			"peer":  {{text: "在"}},
			"hello": {{text: "我先热身"}},
			"waiter1": {
				{tool: ToolStandby, args: `{"wait":"message","timeout_min":10,"on_timeout":"return"}`},
				{text: "收到一"},
			},
			"waiter2": {
				{tool: ToolStandby, args: `{"wait":"message","timeout_min":10,"on_timeout":"return"}`},
				{text: "收到二"},
			},
		}, counts: map[string]int{}},
		CWD:           t.TempDir(),
		BlobDir:       filepath.Join(dir, "blobs"),
		BlobMaxInline: 512,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	sidPeer := standbyStartPeer(t, h, ctx, "peer", standbyGate)
	sidWait := NewSession()
	if _, err := h.RunPrompt(ctx, sidWait, "hello"); err != nil {
		t.Fatalf("RunPrompt(hello): %v", err)
	}
	if _, err := h.JoinGate(ctx, sidWait, standbyGate); err != nil {
		t.Fatalf("JoinGate(waiter): %v", err)
	}

	// 一条足够长的中文消息（唤醒摘要要超过外置阈值 768 字节）。
	if _, err := h.Broadcast(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.BroadcastRequest{
		Target: standbyGateTarget(standbyGate),
		Text:   strings.Repeat("很", 300),
	}); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}

	// 第一次 standby → 交付（结果被外置：completed 带 binary_ref，结构化载荷不在事件里）。
	if _, err := h.RunPrompt(ctx, sidWait, "waiter1"); err != nil {
		t.Fatalf("RunPrompt(waiter1): %v", err)
	}
	_, completed, _ := standbyToolCalls(t, h, sidWait)
	if len(completed) != 1 {
		t.Fatalf("tool.call.completed 应恰好 1 条，得到 %d", len(completed))
	}
	if completed[0].BinaryRef == nil {
		t.Fatalf("本用例的前提是结果被外置（BinaryRef 非空）——实际没外置，阈值口径变了？")
	}
	if _, ok := standbyCompletedDelivery(completed[0]); ok {
		t.Fatalf("外置后事件里不该留结构化载荷（Data 已置空）——口径变了？")
	}

	// 第二次 standby：标记经 blob 读回 → 不重复交付（真等待）。
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := h.RunPrompt(runCtx, sidWait, "waiter2")
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		row, ok := h.disc.Session(sidWait)
		_, comps, _ := standbyToolCalls(t, h, sidWait)
		if len(comps) > 1 || (ok && row.Waiting) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, comps, _ := standbyToolCalls(t, h, sidWait); len(comps) != 1 {
		t.Fatalf("第二次 standby 不该重复交付旧消息（标记该经 blob 读回）：completed 已有 %d 条", len(comps))
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("取消后 standby 没收场")
	}
}

// ---------------------------------------------------------------------------
// ⑥ 超时不进集合
// ---------------------------------------------------------------------------

// TestStandbyTimeoutDeliveryMarkerIsZero：超时收口（"未等到"）的交付标记是 **0**
// ——**不进集合**（否则之后到达的消息会被误当成"已交付"而捞不到）。
func TestStandbyTimeoutDeliveryMarkerIsZero(t *testing.T) {
	standbyTimeKnobs(t, 100*time.Millisecond, 5*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		"waiter": {
			{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`},
			{text: "收尾"},
		},
	})
	sid := NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "waiter"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	_, completed, failed := standbyToolCalls(t, h, sid)
	if len(failed) != 0 {
		t.Fatalf("不该有 failed：%s", standbyFailedText(t, h, sid))
	}
	if len(completed) != 1 {
		t.Fatalf("tool.call.completed 应恰好 1 条，得到 %d", len(completed))
	}
	seq, ok := standbyCompletedDelivery(completed[0])
	if !ok {
		t.Fatalf("超时收口也该带交付标记（delivered_seq=0）")
	}
	if seq != 0 {
		t.Fatalf("超时收口的交付标记 = %d，want 0（超时不进集合）", seq)
	}
}

// ---------------------------------------------------------------------------
// 装置（单元级：直接摸"集合 / 候选 / 轮询"这些内部口径）
// ---------------------------------------------------------------------------

// standbyDeliveryRig 是交付集合相关的**单元级**装置：事件库 + 发现表 + agentlink +
// standby 执行器（不走 hub——要能直接摸内部口径）。
//
// 事件流内容：等待方加入 gate（K11 的前提）+ 对端发的若干条广播 + 等待方的一条
// standby intent（轮询游标的起点）。
type standbyDeliveryRig struct {
	store    *sqlite.Store
	disc     *discovery.Table
	link     *agentlink.Link
	ex       *standbyExecutor
	sid      contracts.Ulid
	aid      contracts.Ulid
	peer     contracts.Ulid
	gate     string
	intentID contracts.Ulid
	// msgSeqs 是对端发的那些广播的 seq（按发送顺序）。
	msgSeqs []int64
}

// newStandbyDeliveryRig 造装置：等待方已加入 gate 且是活跃成员，对端发了 msgs 条广播。
func newStandbyDeliveryRig(t *testing.T, msgs ...string) *standbyDeliveryRig {
	t.Helper()
	st, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"), sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ctx := context.Background()

	r := &standbyDeliveryRig{
		store: st,
		disc:  discovery.New(),
		sid:   contracts.Ulid(core.NewUlid()),
		aid:   contracts.Ulid(core.NewUlid()),
		peer:  contracts.Ulid(core.NewUlid()),
		gate:  "team:standby-delivery",
	}
	gateID := r.gate
	joined, _ := json.Marshal(contracts.GateJoinedPayload{GateID: gateID, Source: contracts.GateJoinSourceExplicit})
	evs := []contracts.Event{{
		V: 1, ID: core.NewUlid(), Kind: discovery.KindGateJoined, TS: "2026-01-01T00:00:00Z",
		SessionID: r.sid, AgentID: r.aid, Durable: true, Payload: joined,
	}}
	// standby 的 intent（轮询游标 = 它的事件 seq）。
	//
	// **intent 在消息之前**：轮询路径的游标 = intent 的 seq → 消息的 seq 必须更大
	// 才在轮询范围内（真实场景就是这样：intent 落盘 → 工具挂着 → 消息到达）。
	intentRaw, _ := json.Marshal(contracts.ToolCallIntentPayload{
		ToolCallID: "toolu_DELIVERY_01",
		ToolName:   ToolStandby,
		Arguments:  json.RawMessage(`{"wait":"message","timeout_min":1,"on_timeout":"return"}`),
	})
	r.intentID = core.NewUlid()
	evs = append(evs, contracts.Event{
		V: 1, ID: r.intentID, Kind: loop.KindToolCallIntent, TS: "2026-01-01T00:00:02Z",
		SessionID: r.sid, AgentID: r.aid, Durable: true, Payload: intentRaw,
	})
	for _, text := range msgs {
		raw, _ := json.Marshal(contracts.AgentLinkBroadcastPayload{
			Target: contracts.AgentLinkTarget{Type: contracts.AgentLinkTargetTypeGate, GateID: &gateID},
			Text:   text,
		})
		evs = append(evs, contracts.Event{
			V: 1, ID: core.NewUlid(), Kind: agentlink.KindBroadcast, TS: "2026-01-01T00:00:01Z",
			SessionID: r.peer, AgentID: contracts.Ulid(core.NewUlid()), Durable: true, Payload: raw,
		})
	}
	stored, err := st.Append(ctx, evs)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	for _, ev := range stored {
		if ev.Kind == agentlink.KindBroadcast && ev.Seq != nil {
			r.msgSeqs = append(r.msgSeqs, *ev.Seq)
		}
	}

	// 发现表：折 gate 成员关系 + 把等待方置为活跃（否则 K11 看不见它的消息）。
	feed := newDiscoveryFeed(st, r.disc)
	if _, err := feed.catchUp(ctx); err != nil {
		t.Fatalf("catchUp: %v", err)
	}
	r.disc.Activate(r.sid)
	r.link = agentlink.New(agentlink.Deps{Table: r.disc, Reader: st})
	r.ex = newStandbyExecutor(standbyDeps{
		store:   st,
		link:    r.link,
		disc:    r.disc,
		catchUp: feed.catchUp,
	})
	return r
}

// ref 返回本装置的 intent 定位结果（session + 游标起点）。
func (r *standbyDeliveryRig) ref(t *testing.T) standbyIntentRef {
	t.Helper()
	evs, err := r.store.ReadFrom(context.Background(), &r.sid, 1, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	for _, ev := range evs {
		if ev.Kind == loop.KindToolCallIntent && ev.ID == r.intentID && ev.Seq != nil {
			return standbyIntentRef{SessionID: r.sid, AgentID: r.aid, Seq: *ev.Seq}
		}
	}
	t.Fatal("事件流里没有 standby 的 intent")
	return standbyIntentRef{}
}

// appendConcurrentDelivery 模拟"**另一个** standby 交付了某条消息"：一条
// CausationID 指向 standby intent 的 completed（标记 = deliveredSeq）。
func (r *standbyDeliveryRig) appendConcurrentDelivery(t *testing.T, deliveredSeq int64) {
	t.Helper()
	raw, err := json.Marshal(contracts.ToolCallCompletedPayload{
		ToolCallID: "toolu_OTHER_STANDBY",
		Result:     standbyDeliveryResult(deliveredSeq),
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cause := r.intentID
	if _, err := r.store.Append(context.Background(), []contracts.Event{{
		V: 1, ID: core.NewUlid(), Kind: loop.KindToolCallCompleted, TS: "2026-01-01T00:00:03Z",
		SessionID: r.sid, AgentID: r.aid, Durable: true, CausationID: &cause, Payload: raw,
	}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

// ---------------------------------------------------------------------------
// P2-1 并发交付可见性（交付前复查）
// ---------------------------------------------------------------------------

// TestStandbyFreshCheckBeforeDelivery（critic P2-1）：delivered 是**等待开始时的
// 快照**——另一个 standby 在我们等待期间交付了同一条消息 → 交付前**复查**一次
// → 不再交付那条（并发 standby 不重复交付同一条）。
//
// 场景：快照（空）→ 另一个 standby 交付了那条消息 → 早到检查命中它 → 复查发现它
// 已被交付 → **不交付**（没有别的候选 → 这次检查没有命中）。
func TestStandbyFreshCheckBeforeDelivery(t *testing.T) {
	r := newStandbyDeliveryRig(t, "一条消息")
	ref := r.ref(t)
	ctx := context.Background()

	// 等待开始时的快照：空（还没交付过任何东西）。
	snapshot, err := r.ex.deliveredSeqs(ctx, ref)
	if err != nil {
		t.Fatalf("deliveredSeqs: %v", err)
	}
	if len(snapshot) != 0 {
		t.Fatalf("快照该是空的，得到 %v", snapshot)
	}
	// 并发：另一个 standby 交付了那条消息。
	r.appendConcurrentDelivery(t, r.msgSeqs[0])

	found, msg, err := r.ex.checkArrived(ctx, ref, standbyArgs{Wait: standbyWaitMessage}, snapshot)
	if err != nil {
		t.Fatalf("checkArrived: %v", err)
	}
	if found {
		t.Fatalf("交付前复查该发现那条已被并发交付 → 不该再交付，得到 seq=%d", msg.Seq)
	}
}

// ---------------------------------------------------------------------------
// P2-2 轮询路径的集合排除
// ---------------------------------------------------------------------------

// TestStandbyPollPathExcludesDelivered（critic P2-2）：轮询路径**同样**按交付集合
// 排除——重放游标 = 原 intent 的 seq，那条消息已被**更晚**的 standby 交付过 →
// 轮询**不**再交付它。
//
// 这是重放路径的真实场景：原 intent → 消息到达 → 之后某个 standby 交付了它 →
// 重放（游标 = 原 intent 的 seq）扫到它 → 若不排除就会重复交付。
func TestStandbyPollPathExcludesDelivered(t *testing.T) {
	r := newStandbyDeliveryRig(t, "一条消息")
	ref := r.ref(t)
	ctx := context.Background()
	if len(r.msgSeqs) != 1 {
		t.Fatalf("夹具该有 1 条消息，得到 %d", len(r.msgSeqs))
	}
	msgSeq := r.msgSeqs[0]
	if msgSeq <= ref.Seq {
		t.Fatalf("夹具的顺序不对：消息 seq=%d 该大于 intent seq=%d", msgSeq, ref.Seq)
	}

	// 轮询（游标 = 原 intent 的 seq）：那条消息**未**交付 → 命中。
	found, msg, _, err := r.ex.checkOnce(ctx, ref, standbyArgs{Wait: standbyWaitMessage}, ref.Seq, map[int64]bool{})
	if err != nil {
		t.Fatalf("checkOnce: %v", err)
	}
	if !found || msg.Seq != msgSeq {
		t.Fatalf("未交付的消息该被轮询命中：found=%v seq=%d（want %d）", found, msg.Seq, msgSeq)
	}

	// 同一场景，但那条消息**已被交付**（更晚的 standby 交付过）→ 不再交付。
	found, _, _, err = r.ex.checkOnce(ctx, ref, standbyArgs{Wait: standbyWaitMessage}, ref.Seq,
		map[int64]bool{msgSeq: true})
	if err != nil {
		t.Fatalf("checkOnce: %v", err)
	}
	if found {
		t.Fatalf("已交付的消息不该被轮询重复交付（seq=%d）", msgSeq)
	}
}

// ---------------------------------------------------------------------------
// P2-3 选取口径统一（轮询取最新）
// ---------------------------------------------------------------------------

// TestStandbyPollPathTakesLatest（critic P2-3）：轮询路径与早到检查**同一口径**——
// 都交付**最新**那条（seq 最大）。等待中到两条消息 → 先交付新的那条。
//
// 旧口径：早到取最新、轮询取最旧（先到的那条）——不一致，且"等的是新消息"的语义
// 上应当交付最新。
func TestStandbyPollPathTakesLatest(t *testing.T) {
	r := newStandbyDeliveryRig(t, "旧的那条", "新的那条")
	ref := r.ref(t)
	ctx := context.Background()
	if len(r.msgSeqs) != 2 {
		t.Fatalf("夹具该有 2 条消息，得到 %d", len(r.msgSeqs))
	}
	found, msg, _, err := r.ex.checkOnce(ctx, ref, standbyArgs{Wait: standbyWaitMessage}, ref.Seq, map[int64]bool{})
	if err != nil {
		t.Fatalf("checkOnce: %v", err)
	}
	if !found {
		t.Fatal("轮询该命中消息")
	}
	if msg.Seq != r.msgSeqs[1] {
		t.Fatalf("轮询该交付最新那条（seq=%d），得到 seq=%d", r.msgSeqs[1], msg.Seq)
	}
}

// ---------------------------------------------------------------------------
// P1 分块扫（大流）
// ---------------------------------------------------------------------------

// TestStandbyDeliveredSetChunkedScan（critic P1）：折交付集合**分块扫**（不整场读
// 进内存）——3×chunk 的事件量下，集合结果与**整场读**的口径**一致**（分块边界不能
// 漏事件）。
//
// 夹具把 standby 的 intent+completed 对**放在分块边界上**（第 chunk 条、第 2×chunk
// 条、第 3×chunk 条与中段各一处）：分块边界的 off-by-one 会正好漏掉它们。
func TestStandbyDeliveredSetChunkedScan(t *testing.T) {
	st, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"), sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()
	ctx := context.Background()

	sid := contracts.Ulid(core.NewUlid())
	aid := contracts.Ulid(core.NewUlid())
	const chunk = standbyIntentChunk
	const total = 3*chunk + 100
	// 交付标记的 seq（与真实消息 seq 无关——这里只考"折出来对不对"）。
	delivered := []int64{7001, 7002, 7003, 7004}
	// standby 的 completed 位置（1-based）：放在**每一块的第一条**（chunk+1、2*chunk+1、
	// 3*chunk+1）+ 中段一处。分块边界的 off-by-one（下一块的起点算错）会正好漏掉它们
	// （intent 紧挨在 completed 前面 → 认领键也在同一块里）。
	completedPos := []int{chunk + 1, 2*chunk + 1, 3*chunk + 1, 500}

	evs := make([]contracts.Event, 0, total)
	noise := json.RawMessage(`{}`)
	for i := 1; i <= total; i++ {
		evs = append(evs, contracts.Event{
			V: 1, ID: core.NewUlid(), Kind: "noise.event", TS: "2026-01-01T00:00:00Z",
			SessionID: sid, AgentID: aid, Durable: true, Payload: noise,
		})
	}
	for idx, pos := range completedPos {
		intentID := core.NewUlid()
		intentRaw, _ := json.Marshal(contracts.ToolCallIntentPayload{
			ToolCallID: contracts.ProviderId("toolu_CHUNK_" + strconv.Itoa(idx)),
			ToolName:   ToolStandby,
		})
		evs[pos-2] = contracts.Event{
			V: 1, ID: intentID, Kind: loop.KindToolCallIntent, TS: "2026-01-01T00:00:00Z",
			SessionID: sid, AgentID: aid, Durable: true, Payload: intentRaw,
		}
		doneRaw, _ := json.Marshal(contracts.ToolCallCompletedPayload{
			ToolCallID: contracts.ProviderId("toolu_CHUNK_" + strconv.Itoa(idx)),
			Result:     standbyDeliveryResult(delivered[idx]),
		})
		cause := intentID
		evs[pos-1] = contracts.Event{
			V: 1, ID: core.NewUlid(), Kind: loop.KindToolCallCompleted, TS: "2026-01-01T00:00:00Z",
			SessionID: sid, AgentID: aid, Durable: true, CausationID: &cause, Payload: doneRaw,
		}
	}
	// 分批落库（一次一万多条的 Append 太重）。
	for i := 0; i < len(evs); i += 1000 {
		end := i + 1000
		if end > len(evs) {
			end = len(evs)
		}
		if _, err := st.Append(ctx, evs[i:end]); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	ex := newStandbyExecutor(standbyDeps{store: st})
	ref := standbyIntentRef{SessionID: sid, AgentID: aid, Seq: 1}
	got, err := ex.deliveredSeqs(ctx, ref)
	if err != nil {
		t.Fatalf("deliveredSeqs: %v", err)
	}
	// 参考：整场读一遍折（同一口径）。
	want := standbyDeliveredSetWholeRead(t, st, sid)
	if len(got) != len(want) {
		t.Fatalf("分块扫的集合 = %v，want %v（与整场读一致）", got, want)
	}
	for seq := range want {
		if !got[seq] {
			t.Fatalf("分块扫漏了 seq=%d（集合 = %v，want %v）——分块边界 off-by-one？", seq, got, want)
		}
	}
}

// standbyDeliveredSetWholeRead 是**整场读**折集合的参考实现（测试内）：拿它跟分块
// 扫的结果对账（分块不能漏事件）。
func standbyDeliveredSetWholeRead(t *testing.T, st *sqlite.Store, sid contracts.Ulid) map[int64]bool {
	t.Helper()
	evs, err := st.ReadFrom(context.Background(), &sid, 1, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	intentIDs := map[contracts.Ulid]bool{}
	out := map[int64]bool{}
	for i := range evs {
		ev := &evs[i]
		switch ev.Kind {
		case loop.KindToolCallIntent:
			var p contracts.ToolCallIntentPayload
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue
			}
			if p.ToolName == ToolStandby {
				intentIDs[ev.ID] = true
			}
		case loop.KindToolCallCompleted:
			if ev.CausationID == nil || !intentIDs[*ev.CausationID] {
				continue
			}
			var p contracts.ToolCallCompletedPayload
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue
			}
			seq, ok := deliverySeqInResult(p.Result)
			if ok && seq > 0 {
				out[seq] = true
			}
		}
	}
	return out
}
