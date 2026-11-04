package host

// G-f standby 待机工具的验收用例（六组）：
//
//	① X1 形状：intent →（等待）→ completed 序；同一个 tool_call_id；无 failed。
//	② 唤醒：投递给本 session 的对应 kind 消息到达即醒（秒级）；别 session 的消息不唤醒；
//	   wait="message" 不被 handoff 唤醒（反之亦然）；**等待开始前**就到达的消息
//	   同样唤醒（completed 带正文 + 「早于本次等待已到达」，critic F1）。
//	③ 超时两态：return 到点收口「未等到（已等 N 分钟）」；sleep **不适用 timeout_min**
//	   （挂到硬上限才收口「未等到（硬上限 …；sleep 模式不适用 timeout_min）」，critic F3）。
//	④ 等待期状态：BeginWait/EndWait 配平（waiting 真→假）；挂起期零新事件（ts 不刷）。
//	⑤ 非法参数：wait="event" / timeout_min 越界 / 未知取值 → failed 且文案点名。
//	⑥ 取消：run ctx 取消 → failed，不挂死。
//
// 为什么是 internal（package host）：时间口径（standbyMinute / standbyHardCap /
// standbyPollInterval）是包内变量，测试要把超时路径缩到毫秒级；waiting 态与事件流
// 也要直接摸 h.disc / h.store（黑箱面没有这些入口）。

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// standbyGate 是本文件用例共用的显式 gate（不走自动 repo gate：那要 git 仓库，
// 而这里考的是 standby 本身，不该让结论依赖环境里有没有 git）。
const standbyGate = "team:standby"

// standbyOtherGate 是"别 session 的消息"用例里的另一个 gate（等待方不在里面）。
const standbyOtherGate = "team:standby-other"

// ---------------------------------------------------------------------------
// 夹具
// ---------------------------------------------------------------------------

// standbyStep 是一次 provider 调用的动作。
type standbyStep struct {
	// tool 非空 → 本次发起一次该工具调用；否则回 text。
	tool string
	args string
	text string
}

// standbyScriptProvider 按 prompt 文本分派的脚本 provider。
//
// 为什么按 prompt 文本分派：ModelRequest 里没有 session id（core 不往请求里塞
// session），但**用户 prompt 原文**在 messages 里——按它分派就能让同一个 hub 里
// 多个 session 各走各的脚本（一个发消息、一个待机）。
type standbyScriptProvider struct {
	mu     sync.Mutex
	plans  map[string][]standbyStep
	counts map[string]int
}

// Run 走脚本的一步（第一个还没用过的 step）。
func (p *standbyScriptProvider) Run(ctx context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	key := p.promptKey(req)
	p.mu.Lock()
	steps := p.plans[key]
	idx := p.counts[key]
	p.counts[key] = idx + 1
	p.mu.Unlock()

	step := standbyStep{text: "（脚本已用尽）"}
	if idx < len(steps) {
		step = steps[idx]
	}
	if step.tool != "" {
		tc := core.ToolCallReq{
			ID:   contracts.ProviderId("toolu_ST_" + key + "_" + strconv.Itoa(idx)),
			Name: step.tool,
			Args: json.RawMessage(step.args),
		}
		emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 10, OutputTokens: 5}}, nil
	}
	emit(core.ProviderEvent{Kind: "text.delta", Text: step.text})
	emit(core.ProviderEvent{Kind: "done"})
	return core.ModelResult{Text: step.text, Usage: core.Usage{InputTokens: 20, OutputTokens: 3}}, nil
}

// promptKey 取本次请求的分派键：**最后一个能对上剧本键的 user 消息**。
//
// 为什么不是"最后一条 user 消息"：loop 会把在线表当 role=user 的消息追加到请求
// 最末尾（Q4/K14），那条正文不会命中任何剧本键——按"能对上键"筛，天然避开它，
// 也避得开历史里更早的 user 消息（多轮对话时那不是本次 prompt）。
func (p *standbyScriptProvider) promptKey(req core.ModelRequest) string {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		m := req.Messages[i]
		if m.Role != "user" {
			continue
		}
		key := strings.TrimSpace(m.Content)
		p.mu.Lock()
		_, ok := p.plans[key]
		p.mu.Unlock()
		if ok {
			return key
		}
	}
	return ""
}

// standbyTimeKnobs 把待机的时间口径缩到毫秒级（t.Cleanup 还原生产值）。
//
// 写走 **atomic**（critic F7）：裸包变量被无锁写 = 数据竞争（后台轮询 goroutine
// 同时在读 → -race 报）。存取口径与生产路径同一个访问器。
func standbyTimeKnobs(t *testing.T, minute, hardCap, poll time.Duration) {
	t.Helper()
	oldMinute, oldCap, oldPoll := standbyTime.minute.Load(), standbyTime.hardCap.Load(), standbyTime.poll.Load()
	standbyTime.minute.Store(int64(minute))
	standbyTime.hardCap.Store(int64(hardCap))
	standbyTime.poll.Store(int64(poll))
	t.Cleanup(func() {
		standbyTime.minute.Store(oldMinute)
		standbyTime.hardCap.Store(oldCap)
		standbyTime.poll.Store(oldPoll)
	})
}

// newStandbyTestHub 装配一个 hub：临时事件库 + 脚本 provider + 只放行 standby。
func newStandbyTestHub(t *testing.T, plans map[string][]standbyStep) *Hub {
	t.Helper()
	cfg := Config{
		DBPath:   filepath.Join(t.TempDir(), "events.db"),
		Model:    "standby-test",
		System:   "standby-test",
		MaxTurns: 4,
		Allow:    []string{ToolStandby},
		Provider: &standbyScriptProvider{plans: plans, counts: map[string]int{}},
		CWD:      t.TempDir(),
	}
	h, err := New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// standbyGateTarget 造一个 gate 目标。
func standbyGateTarget(gateID string) contracts.AgentLinkTarget {
	id := gateID
	return contracts.AgentLinkTarget{Type: contracts.AgentLinkTargetTypeGate, GateID: &id}
}

// standbySessionEvents 读某 session 名下的全部事件（按 seq 升序）。
func standbySessionEvents(t *testing.T, h *Hub, sid contracts.Ulid) []contracts.Event {
	t.Helper()
	evs, err := h.store.ReadFrom(context.Background(), &sid, 1, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	return evs
}

// standbyToolCalls 抽出某 session 名下的三段事件（intent / completed / failed）。
func standbyToolCalls(t *testing.T, h *Hub, sid contracts.Ulid) (intents, completed, failed []contracts.Event) {
	t.Helper()
	for _, ev := range standbySessionEvents(t, h, sid) {
		switch ev.Kind {
		case loop.KindToolCallIntent:
			intents = append(intents, ev)
		case loop.KindToolCallCompleted:
			completed = append(completed, ev)
		case loop.KindToolCallFailed:
			failed = append(failed, ev)
		}
	}
	return intents, completed, failed
}

// standbyResultText 把一条工具结果摊成文本（completed 载荷的正文）。
func standbyResultText(res contracts.ToolResult) string {
	var b strings.Builder
	for _, part := range res.Content {
		if part.Text != nil {
			b.WriteString(*part.Text)
		}
	}
	return b.String()
}

// standbyCompletedText 取本 session 最后一条 completed 的正文。
func standbyCompletedText(t *testing.T, h *Hub, sid contracts.Ulid) string {
	t.Helper()
	_, completed, _ := standbyToolCalls(t, h, sid)
	if len(completed) == 0 {
		t.Fatalf("session %s 没有 tool.call.completed 事件", sid)
	}
	var p contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(completed[len(completed)-1].Payload, &p); err != nil {
		t.Fatalf("解 completed 载荷失败: %v", err)
	}
	return standbyResultText(p.Result)
}

// standbyFailedText 取本 session 最后一条 failed 的错误文案。
func standbyFailedText(t *testing.T, h *Hub, sid contracts.Ulid) string {
	t.Helper()
	_, _, failed := standbyToolCalls(t, h, sid)
	if len(failed) == 0 {
		t.Fatalf("session %s 没有 tool.call.failed 事件", sid)
	}
	var p contracts.ToolCallFailedPayload
	if err := json.Unmarshal(failed[len(failed)-1].Payload, &p); err != nil {
		t.Fatalf("解 failed 载荷失败: %v", err)
	}
	return p.Error.Message
}

// standbyWaitWaiting 等到该 session 的 waiting 态到达期望值（超时即失败）。
func standbyWaitWaiting(t *testing.T, h *Hub, sid contracts.Ulid, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		row, ok := h.disc.Session(sid)
		if ok && row.Waiting == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	row, _ := h.disc.Session(sid)
	t.Fatalf("session %s 的 waiting 态没到 %v（现值 %v）", sid, want, row.Waiting)
}

// standbyStartPeer 起一个"对端" session：跑一轮拿到主 agent，再加入 gate。
func standbyStartPeer(t *testing.T, h *Hub, ctx context.Context, prompt string, gates ...string) contracts.Ulid {
	t.Helper()
	sid := NewSession()
	if _, err := h.RunPrompt(ctx, sid, prompt); err != nil {
		t.Fatalf("RunPrompt(peer): %v", err)
	}
	for _, g := range gates {
		if _, err := h.JoinGate(ctx, sid, g); err != nil {
			t.Fatalf("JoinGate(%s): %v", g, err)
		}
	}
	return sid
}

// ---------------------------------------------------------------------------
// ① X1 形状：intent →（等待）→ completed
// ---------------------------------------------------------------------------

// TestStandbyIntentThenCompleted：intent 先落、completed 后落、同一 tool_call_id、
// 没有 failed（X1 三段口径一字不动）。
func TestStandbyIntentThenCompleted(t *testing.T) {
	standbyTimeKnobs(t, 60*time.Millisecond, 5*time.Second, 10*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		"shape": {
			{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`},
			{text: "收尾"},
		},
	})
	sid := NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "shape"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	intents, completed, failed := standbyToolCalls(t, h, sid)
	if len(intents) != 1 {
		t.Fatalf("tool.call.intent 应恰好 1 条，得到 %d", len(intents))
	}
	if len(completed) != 1 {
		t.Fatalf("tool.call.completed 应恰好 1 条，得到 %d", len(completed))
	}
	if len(failed) != 0 {
		t.Fatalf("成功路径不该有 tool.call.failed，得到 %d 条：%s", len(failed), standbyFailedText(t, h, sid))
	}
	if intents[0].Seq == nil || completed[0].Seq == nil || *intents[0].Seq >= *completed[0].Seq {
		t.Fatalf("intent 应在 completed 之前：intent=%v completed=%v", intents[0].Seq, completed[0].Seq)
	}
	var ip contracts.ToolCallIntentPayload
	if err := json.Unmarshal(intents[0].Payload, &ip); err != nil {
		t.Fatalf("解 intent 载荷失败: %v", err)
	}
	var cp contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(completed[0].Payload, &cp); err != nil {
		t.Fatalf("解 completed 载荷失败: %v", err)
	}
	if ip.ToolCallID != cp.ToolCallID {
		t.Fatalf("intent/completed 的 tool_call_id 应同一个：%q vs %q", ip.ToolCallID, cp.ToolCallID)
	}
	if ip.ToolName != ToolStandby {
		t.Fatalf("intent 的 tool_name = %q，want %q", ip.ToolName, ToolStandby)
	}
	if got := standbyResultText(cp.Result); !strings.Contains(got, "未等到") {
		t.Fatalf("completed 正文应如实说没等到，得到 %q", got)
	}
}

// ---------------------------------------------------------------------------
// ② 唤醒
// ---------------------------------------------------------------------------

// TestStandbyWakesOnBroadcast：挂起中收到投递给本 session 的 agentlink.broadcast
// → 立刻醒，completed 带发送方与内容摘要。
func TestStandbyWakesOnBroadcast(t *testing.T) {
	standbyTimeKnobs(t, 10*time.Second, 30*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		"peer":  {{text: "在"}},
		"hello": {{text: "我先热身"}},
		"waiter": {
			{tool: ToolStandby, args: `{"wait":"message","timeout_min":10,"on_timeout":"return"}`},
			{text: "收到，继续"},
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

	// 等待方在后台挂起。
	done := make(chan error, 1)
	go func() {
		_, err := h.RunPrompt(ctx, sidWait, "waiter")
		done <- err
	}()
	standbyWaitWaiting(t, h, sidWait, true)

	start := time.Now()
	if _, err := h.Broadcast(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.BroadcastRequest{
		Target: standbyGateTarget(standbyGate),
		Text:   "我改完了 README",
	}); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunPrompt: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("standby 没被广播唤醒（挂死）")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("唤醒太慢：%v（应秒级）", elapsed)
	}
	text := standbyCompletedText(t, h, sidWait)
	if !strings.Contains(text, "我改完了 README") {
		t.Fatalf("completed 应带消息内容摘要，得到 %q", text)
	}
	if !strings.Contains(text, string(sidPeer)) {
		t.Fatalf("completed 应带发送方 session，得到 %q", text)
	}
	if !strings.Contains(text, agentlink.KindBroadcast) {
		t.Fatalf("completed 应点名消息种类，得到 %q", text)
	}
}

// TestStandbyWakesOnAlreadyArrivedMessage：消息**在等待开始前**就到达 → standby
// **立即**收口（completed 带消息正文 + 「早于本次等待已到达」）。
//
// 这是典型场景（A 派活 → B 秒回 → A 下一轮才调 standby）：那条消息的 seq **小于**
// 本次 tool.call.intent 的 seq → Inbox 的 SinceSeq（严格大于）看不见它 → 旧实现
// 必然假阴性（烧满超时后报"未等到"，而那条消息模型没有第二条路看到它——
// agentlink 消息不进 FoldConversation）。
//
// 钉子：completed 正文带**消息正文**与「早于本次等待已到达」字样，且**不是**
// "未等到"（= 没睡满超时）。
func TestStandbyWakesOnAlreadyArrivedMessage(t *testing.T) {
	standbyTimeKnobs(t, 150*time.Millisecond, 5*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		"peer":  {{text: "在"}},
		"hello": {{text: "我先热身"}},
		"waiter": {
			{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`},
			{text: "收到，继续"},
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

	// 消息**先到**（standby 还没开始等）：它的 seq < 本次 tool.call.intent 的 seq。
	if _, err := h.Broadcast(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.BroadcastRequest{
		Target: standbyGateTarget(standbyGate),
		Text:   "我改完了 README",
	}); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}

	// 然后才 standby。
	if _, err := h.RunPrompt(ctx, sidWait, "waiter"); err != nil {
		t.Fatalf("RunPrompt(waiter): %v", err)
	}
	if _, _, failed := standbyToolCalls(t, h, sidWait); len(failed) != 0 {
		t.Fatalf("不该有 failed：%s", standbyFailedText(t, h, sidWait))
	}
	text := standbyCompletedText(t, h, sidWait)
	if !strings.Contains(text, "我改完了 README") {
		t.Fatalf("completed 应带消息正文，得到 %q", text)
	}
	if !strings.Contains(text, "早于本次等待已到达") {
		t.Fatalf("completed 应注明「早于本次等待已到达」，得到 %q", text)
	}
	if strings.Contains(text, "未等到") {
		t.Fatalf("消息已到就不该报\"未等到\"，得到 %q", text)
	}
}

// TestStandbyDoesNotRedeliverOldMessage（critic P1）：同 session **连调两次**
// standby → 第二次**不**重复交付第一条已交付的旧消息，而是**真的进入等待**；
// 等到**新**消息才交付新消息。
//
// 为什么要有下界：消息**不删** + K11 按读时成员关系判 → 不带下界的早到检查会把
// 历史上任何投递给本 session 的同类消息都命中 → 两次 standby 的 completed **逐字
// 相同**（同一条旧消息），第二次 2ms 空转没等（critic 实测）。多轮协作里"A 再等
// C 的回话"会拿到 B 的旧回话、误判为新回复。
func TestStandbyDoesNotRedeliverOldMessage(t *testing.T) {
	standbyTimeKnobs(t, time.Second, 30*time.Second, 20*time.Millisecond)
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

	// 旧消息先到（第一次 standby 之前）。
	if _, err := h.Broadcast(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.BroadcastRequest{
		Target: standbyGateTarget(standbyGate),
		Text:   "旧消息：第一次的回复",
	}); err != nil {
		t.Fatalf("Broadcast(旧): %v", err)
	}

	// 第一次 standby：早到检查交付那条旧消息。
	if _, err := h.RunPrompt(ctx, sidWait, "waiter1"); err != nil {
		t.Fatalf("RunPrompt(waiter1): %v", err)
	}
	text := standbyCompletedText(t, h, sidWait)
	if !strings.Contains(text, "旧消息：第一次的回复") {
		t.Fatalf("第一次 standby 应交付早到的旧消息，得到 %q", text)
	}

	// 第二次 standby：不该重复交付旧消息，要真的等。
	done := make(chan error, 1)
	go func() {
		_, err := h.RunPrompt(ctx, sidWait, "waiter2")
		done <- err
	}()
	// 等到"进入等待"或"已经收口"（谁先来都算）——然后断言它**没有**立刻收口。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		row, ok := h.disc.Session(sidWait)
		_, completed, _ := standbyToolCalls(t, h, sidWait)
		if len(completed) > 1 || (ok && row.Waiting) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, completed, _ := standbyToolCalls(t, h, sidWait); len(completed) != 1 {
		t.Fatalf("第二次 standby 不该立刻收口（旧消息被逐字重复交付）：completed 已有 %d 条", len(completed))
	}

	// 新消息到达 → 交付新消息。
	if _, err := h.Broadcast(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.BroadcastRequest{
		Target: standbyGateTarget(standbyGate),
		Text:   "新消息：第二次的回复",
	}); err != nil {
		t.Fatalf("Broadcast(新): %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunPrompt(waiter2): %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("第二次 standby 挂死了（既没等到新消息也没超时收口）")
	}
	text = standbyCompletedText(t, h, sidWait)
	if !strings.Contains(text, "新消息：第二次的回复") {
		t.Fatalf("第二次 standby 应交付新消息，得到 %q", text)
	}
	if strings.Contains(text, "旧消息：第一次的回复") {
		t.Fatalf("不该重复交付旧消息，得到 %q", text)
	}
}

// TestStandbyTimeoutDoesNotRaiseLowerBound（critic P1）：超时收口"未等到"**不抬**
// 已交付下界 → 之后到达的消息，下一次 standby 的早到检查仍能捞到。
func TestStandbyTimeoutDoesNotRaiseLowerBound(t *testing.T) {
	standbyTimeKnobs(t, 150*time.Millisecond, 5*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		"peer":  {{text: "在"}},
		"hello": {{text: "我先热身"}},
		"waiter1": {
			{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`},
			{text: "收尾一"},
		},
		"waiter2": {
			{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`},
			{text: "收尾二"},
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

	// 第一次 standby：没有消息 → 超时收口（"未等到"，delivered_seq=0）。
	if _, err := h.RunPrompt(ctx, sidWait, "waiter1"); err != nil {
		t.Fatalf("RunPrompt(waiter1): %v", err)
	}
	if text := standbyCompletedText(t, h, sidWait); !strings.Contains(text, "未等到") {
		t.Fatalf("第一次 standby 该超时收口，得到 %q", text)
	}

	// 超时之后消息才到。
	if _, err := h.Broadcast(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.BroadcastRequest{
		Target: standbyGateTarget(standbyGate),
		Text:   "迟到的消息",
	}); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	// 第二次 standby：早到检查把它捞到（超时没抬下界）。
	if _, err := h.RunPrompt(ctx, sidWait, "waiter2"); err != nil {
		t.Fatalf("RunPrompt(waiter2): %v", err)
	}
	text := standbyCompletedText(t, h, sidWait)
	if !strings.Contains(text, "迟到的消息") {
		t.Fatalf("第二次 standby 的早到检查该捞到迟到的消息，得到 %q", text)
	}
	if !strings.Contains(text, "早于本次等待已到达") {
		t.Fatalf("completed 应注明「早于本次等待已到达」，得到 %q", text)
	}
}

// TestStandbyWakesOnHandoff：wait="handoff" 时 agentlink.handoff 唤醒它。
func TestStandbyWakesOnHandoff(t *testing.T) {
	standbyTimeKnobs(t, 10*time.Second, 30*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		"peer":   {{text: "在"}},
		"hello":  {{text: "我先热身"}},
		"waiter": {{tool: ToolStandby, args: `{"wait":"handoff","timeout_min":10,"on_timeout":"return"}`}, {text: "收到"}},
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
	done := make(chan error, 1)
	go func() {
		_, err := h.RunPrompt(ctx, sidWait, "waiter")
		done <- err
	}()
	standbyWaitWaiting(t, h, sidWait, true)

	if _, err := h.Handoff(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.HandoffRequest{
		Target: standbyGateTarget(standbyGate),
		Text:   "交接：docs/ 写完了，下一步写实现",
	}); err != nil {
		t.Fatalf("Handoff: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunPrompt: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("standby 没被交接唤醒（挂死）")
	}
	text := standbyCompletedText(t, h, sidWait)
	if !strings.Contains(text, "交接") {
		t.Fatalf("completed 应带交接内容摘要，得到 %q", text)
	}
	if !strings.Contains(text, agentlink.KindHandoff) {
		t.Fatalf("completed 应点名消息种类，得到 %q", text)
	}
}

// TestStandbyNotWokenByWrongKindOrOtherSession：两类"不该醒"——
// ① 消息种类不对（wait=message 不被 handoff 唤醒，反之亦然）；
// ② 消息投给别的 session（别的 gate）→ 不唤醒。
// 两种都该走到超时收口（文案说"未等到"）。
func TestStandbyNotWokenByWrongKindOrOtherSession(t *testing.T) {
	cases := []struct {
		name    string
		wait    string
		send    string // broadcast / handoff
		gate    string
		wantKey string
	}{
		{"message 不被 handoff 唤醒", "message", "handoff", standbyGate, "未等到"},
		{"handoff 不被 broadcast 唤醒", "handoff", "broadcast", standbyGate, "未等到"},
		{"别 session 的消息不唤醒", "message", "broadcast", standbyOtherGate, "未等到"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			standbyTimeKnobs(t, 150*time.Millisecond, 5*time.Second, 20*time.Millisecond)
			h := newStandbyTestHub(t, map[string][]standbyStep{
				"peer":  {{text: "在"}},
				"hello": {{text: "我先热身"}},
				"waiter": {
					{tool: ToolStandby, args: `{"wait":"` + tc.wait + `","timeout_min":1,"on_timeout":"return"}`},
					{text: "收尾"},
				},
			})
			ctx := context.Background()
			// 对端加入两个 gate：能往 standbyGate 发，也能往 standbyOtherGate 发。
			sidPeer := standbyStartPeer(t, h, ctx, "peer", standbyGate, standbyOtherGate)
			sidWait := NewSession()
			if _, err := h.RunPrompt(ctx, sidWait, "hello"); err != nil {
				t.Fatalf("RunPrompt(hello): %v", err)
			}
			// 等待方**只在 standbyGate**：standbyOtherGate 的消息对它不可见。
			if _, err := h.JoinGate(ctx, sidWait, standbyGate); err != nil {
				t.Fatalf("JoinGate(waiter): %v", err)
			}
			done := make(chan error, 1)
			go func() {
				_, err := h.RunPrompt(ctx, sidWait, "waiter")
				done <- err
			}()
			standbyWaitWaiting(t, h, sidWait, true)

			if tc.send == "broadcast" {
				_, err := h.Broadcast(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.BroadcastRequest{
					Target: standbyGateTarget(tc.gate),
					Text:   "一条不该唤醒你的消息",
				})
				if err != nil {
					t.Fatalf("Broadcast: %v", err)
				}
			} else {
				_, err := h.Handoff(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.HandoffRequest{
					Target: standbyGateTarget(tc.gate),
					Text:   "一条不该唤醒你的交接",
				})
				if err != nil {
					t.Fatalf("Handoff: %v", err)
				}
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("RunPrompt: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("standby 挂死了（既没被唤醒也没超时）")
			}
			if _, _, failed := standbyToolCalls(t, h, sidWait); len(failed) != 0 {
				t.Fatalf("不该有 failed：%s", standbyFailedText(t, h, sidWait))
			}
			text := standbyCompletedText(t, h, sidWait)
			if !strings.Contains(text, tc.wantKey) {
				t.Fatalf("completed 应如实收口（%q），得到 %q", tc.wantKey, text)
			}
			if strings.Contains(text, "一条不该唤醒你的") {
				t.Fatalf("不该唤醒的消息不该出现在 completed 里：%q", text)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ③ 超时两态
// ---------------------------------------------------------------------------

// TestStandbyTimeoutReturn：on_timeout="return" → 等满 timeout_min 就收口
// 「未等到（已等 N 分钟）」，不等硬上限。
func TestStandbyTimeoutReturn(t *testing.T) {
	standbyTimeKnobs(t, 150*time.Millisecond, 5*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		"waiter": {
			{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"return"}`},
			{text: "收尾"},
		},
	})
	sid := NewSession()
	start := time.Now()
	if _, err := h.RunPrompt(context.Background(), sid, "waiter"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 3*time.Second {
		t.Fatalf("return 模式不该等到硬上限：耗时 %v", elapsed)
	}
	text := standbyCompletedText(t, h, sid)
	if !strings.Contains(text, "未等到（已等 1 分钟）") {
		t.Fatalf("completed 文案 = %q，want 含「未等到（已等 1 分钟）」", text)
	}
}

// TestStandbyTimeoutSleepIgnoresTimeoutMin：on_timeout="sleep" → **timeout_min
// 不适用**（critic F3）：语义是"等到消息或硬上限"，本次等待的终点 = 硬上限
// （timeout_min=1 的软点 = 100ms，硬上限 = 600ms → 必须挂到 ~600ms 才收口）。
//
// 文案也要如实说清：「未等到（硬上限 …；sleep 模式不适用 timeout_min）」。
func TestStandbyTimeoutSleepIgnoresTimeoutMin(t *testing.T) {
	const hardCap = 600 * time.Millisecond
	standbyTimeKnobs(t, 100*time.Millisecond, hardCap, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		"waiter": {
			{tool: ToolStandby, args: `{"wait":"message","timeout_min":1,"on_timeout":"sleep"}`},
			{text: "收尾"},
		},
	})
	sid := NewSession()
	start := time.Now()
	if _, err := h.RunPrompt(context.Background(), sid, "waiter"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	elapsed := time.Since(start)
	// timeout_min 不适用 → 必须挂到**硬上限**（不是 100ms 那个软点）。
	if elapsed+20*time.Millisecond < hardCap {
		t.Fatalf("sleep 模式该挂到硬上限 %v（timeout_min 不适用），实际耗时 %v", hardCap, elapsed)
	}
	// 也不该远超硬上限（不许挂死）。
	if elapsed > hardCap+3*time.Second {
		t.Fatalf("sleep 模式不该超过硬上限太多：耗时 %v", elapsed)
	}
	text := standbyCompletedText(t, h, sid)
	if !strings.Contains(text, "未等到（硬上限") {
		t.Fatalf("completed 文案 = %q，want 含「未等到（硬上限 …）」", text)
	}
	if !strings.Contains(text, standbyDurText(hardCap)) {
		t.Fatalf("completed 文案 = %q，want 含硬上限时长 %q", text, standbyDurText(hardCap))
	}
	// 如实说清 timeout_min 在这条路径上不适用（模型据此不再误以为它有意义）。
	if !strings.Contains(text, "timeout_min") {
		t.Fatalf("completed 文案 %q 应说明 sleep 模式不适用 timeout_min", text)
	}
	// sleep 模式**不是** return 模式 → 不该用"已等 N 分钟"那种措辞。
	if strings.Contains(text, "已等") {
		t.Fatalf("sleep 模式不该用 return 模式的措辞，得到 %q", text)
	}
}

// ---------------------------------------------------------------------------
// ④ 等待期状态
// ---------------------------------------------------------------------------

// TestStandbyWaitingStateBalanced：挂起期 waiting=true、事件流零新事件（ts 不刷）；
// 收场后 waiting=false（BeginWait/EndWait 配平）。
func TestStandbyWaitingStateBalanced(t *testing.T) {
	standbyTimeKnobs(t, 10*time.Second, 30*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		"peer":  {{text: "在"}},
		"hello": {{text: "我先热身"}},
		"waiter": {
			{tool: ToolStandby, args: `{"wait":"message","timeout_min":10,"on_timeout":"return"}`},
			{text: "收到"},
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
	// 热身那一轮的行（挂起前的最后事件 ts / seq）。
	before, ok := h.disc.Session(sidWait)
	if !ok {
		t.Fatal("等待方不在发现表里")
	}

	done := make(chan error, 1)
	go func() {
		_, err := h.RunPrompt(ctx, sidWait, "waiter")
		done <- err
	}()
	standbyWaitWaiting(t, h, sidWait, true)

	// 挂起期：waiting=true；本 session 的最后一条事件仍是上一轮那条（ts 不刷）。
	row, ok := h.disc.Session(sidWait)
	if !ok || !row.Waiting {
		t.Fatalf("挂起期该 waiting=true：%+v", row)
	}
	if row.UpdatedAt != before.UpdatedAt {
		t.Fatalf("挂起期不该刷最后事件 ts：%q vs %q", row.UpdatedAt, before.UpdatedAt)
	}
	// 事件流：等待方的最后一条事件是 tool.call.intent（completed 还没落）。
	evs := standbySessionEvents(t, h, sidWait)
	last := evs[len(evs)-1]
	if last.Kind != loop.KindToolCallIntent {
		t.Fatalf("挂起期最后一条事件应是 tool.call.intent，得到 %q", last.Kind)
	}

	// 唤醒 → 收场 → waiting 回落。
	if _, err := h.Broadcast(ctx, agentlink.Identity{SessionID: sidPeer}, agentlink.BroadcastRequest{
		Target: standbyGateTarget(standbyGate),
		Text:   "开工了",
	}); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunPrompt: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("standby 挂死了")
	}
	standbyWaitWaiting(t, h, sidWait, false)
}

// ---------------------------------------------------------------------------
// ⑤ 非法参数
// ---------------------------------------------------------------------------

// TestStandbyInvalidArgs：非法参数 → tool.call.failed，且文案点名问题。
func TestStandbyInvalidArgs(t *testing.T) {
	cases := []struct {
		name string
		args string
		want []string
	}{
		{"wait=event 未实现", `{"wait":"event","timeout_min":1,"on_timeout":"return"}`, []string{"G-j", "wait"}},
		{"timeout_min=0", `{"wait":"message","timeout_min":0,"on_timeout":"return"}`, []string{"timeout_min", "1..30"}},
		{"timeout_min=31", `{"wait":"message","timeout_min":31,"on_timeout":"return"}`, []string{"timeout_min", "1..30"}},
		{"wait 未知取值", `{"wait":"whatever","timeout_min":1,"on_timeout":"return"}`, []string{"wait", "message"}},
		{"on_timeout 未知取值", `{"wait":"message","timeout_min":1,"on_timeout":"whatever"}`, []string{"on_timeout", "return"}},
		{"缺 wait", `{"timeout_min":1,"on_timeout":"return"}`, []string{"wait"}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			standbyTimeKnobs(t, 100*time.Millisecond, 5*time.Second, 20*time.Millisecond)
			h := newStandbyTestHub(t, map[string][]standbyStep{
				"bad": {{tool: ToolStandby, args: tc.args}, {text: "收尾"}},
			})
			sid := NewSession()
			// 参数非法 → 工具失败 → loop 继续（模型自己收尾），整体不该报错。
			if _, err := h.RunPrompt(context.Background(), sid, "bad"); err != nil {
				t.Fatalf("RunPrompt: %v", err)
			}
			intents, completed, failed := standbyToolCalls(t, h, sid)
			if len(intents) != 1 {
				t.Fatalf("tool.call.intent 应恰好 1 条（X1：有意图就要有结果），得到 %d", len(intents))
			}
			if len(failed) != 1 {
				t.Fatalf("tool.call.failed 应恰好 1 条，得到 %d", len(failed))
			}
			if len(completed) != 0 {
				t.Fatalf("失败路径不该有 completed，得到 %d 条", len(completed))
			}
			msg := standbyFailedText(t, h, sid)
			for _, want := range tc.want {
				if !strings.Contains(msg, want) {
					t.Fatalf("failed 文案 %q 应点名 %q", msg, want)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ⑥ 取消
// ---------------------------------------------------------------------------

// TestStandbyCancelStopsWaiting：run ctx 取消 → 待机中止（tool.call.failed），
// 不挂死。
func TestStandbyCancelStopsWaiting(t *testing.T) {
	standbyTimeKnobs(t, 10*time.Second, 30*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		"hello":  {{text: "我先热身"}},
		"waiter": {{tool: ToolStandby, args: `{"wait":"message","timeout_min":10,"on_timeout":"return"}`}, {text: "收到"}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	sid := NewSession()
	if _, err := h.RunPrompt(ctx, sid, "hello"); err != nil {
		t.Fatalf("RunPrompt(hello): %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := h.RunPrompt(ctx, sid, "waiter")
		done <- err
	}()
	standbyWaitWaiting(t, h, sid, true)

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("ctx 取消后这一轮该报错")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ctx 取消后 standby 挂死了")
	}
	// waiting 态配平（取消也要 EndWait）。
	standbyWaitWaiting(t, h, sid, false)
	// X1：有意图就要有结果——取消走 failed。
	_, _, failed := standbyToolCalls(t, h, sid)
	if len(failed) != 1 {
		t.Fatalf("取消路径该落 1 条 tool.call.failed，得到 %d", len(failed))
	}
	if msg := standbyFailedText(t, h, sid); !strings.Contains(msg, "取消") {
		t.Fatalf("failed 文案 %q 应说明是取消", msg)
	}
}

// TestStandbyHubCloseStopsWaiting：hub 关库 → 生命周期 ctx 取消 → 待机中止，
// 不挂死（收场细节不钉：库已关，failed 事件可能落不下，但**不许挂住**）。
func TestStandbyHubCloseStopsWaiting(t *testing.T) {
	standbyTimeKnobs(t, 10*time.Second, 30*time.Second, 20*time.Millisecond)
	h := newStandbyTestHub(t, map[string][]standbyStep{
		"hello":  {{text: "我先热身"}},
		"waiter": {{tool: ToolStandby, args: `{"wait":"message","timeout_min":10,"on_timeout":"return"}`}, {text: "收到"}},
	})
	ctx := context.Background()
	sid := NewSession()
	if _, err := h.RunPrompt(ctx, sid, "hello"); err != nil {
		t.Fatalf("RunPrompt(hello): %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := h.RunPrompt(ctx, sid, "waiter")
		done <- err
	}()
	standbyWaitWaiting(t, h, sid, true)

	if err := h.Close(); err != nil {
		t.Logf("Close: %v", err)
	}
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("hub 关库后 standby 挂死了")
	}
}

// ---------------------------------------------------------------------------
// 时间口径（原子量 + clamp）
// ---------------------------------------------------------------------------

// TestStandbyTimeKnobsAtomicAndClamped（critic F7）：时间口径是**原子量**——并发写 +
// 读不出数据竞争（-race 钉子；裸包变量在这里必报）；读取时**clamp**（≤0 → 默认值，
// 防 time.NewTicker(≤0) panic）。
func TestStandbyTimeKnobsAtomicAndClamped(t *testing.T) {
	// 还原生产值（本用例会写旋钮）。
	oldMinute, oldCap, oldPoll := standbyTime.minute.Load(), standbyTime.hardCap.Load(), standbyTime.poll.Load()
	t.Cleanup(func() {
		standbyTime.minute.Store(oldMinute)
		standbyTime.hardCap.Store(oldCap)
		standbyTime.poll.Store(oldPoll)
	})

	// ① 默认值 = 生产值。
	if got := standbyMinute(); got != standbyMinuteDefault {
		t.Fatalf("standbyMinute() = %v，want %v", got, standbyMinuteDefault)
	}
	if got := standbyHardCap(); got != standbyHardCapDefault {
		t.Fatalf("standbyHardCap() = %v，want %v", got, standbyHardCapDefault)
	}
	if got := standbyPollInterval(); got != standbyPollIntervalDefault {
		t.Fatalf("standbyPollInterval() = %v，want %v", got, standbyPollIntervalDefault)
	}

	// ② 写成 0 / 负数 → 读取回落默认值（不许把 0 喂给 NewTicker）。
	standbyTime.minute.Store(0)
	standbyTime.hardCap.Store(-1)
	standbyTime.poll.Store(0)
	if got := standbyMinute(); got != standbyMinuteDefault {
		t.Fatalf("≤0 该回落默认值：standbyMinute() = %v，want %v", got, standbyMinuteDefault)
	}
	if got := standbyHardCap(); got != standbyHardCapDefault {
		t.Fatalf("≤0 该回落默认值：standbyHardCap() = %v，want %v", got, standbyHardCapDefault)
	}
	if got := standbyPollInterval(); got != standbyPollIntervalDefault {
		t.Fatalf("≤0 该回落默认值：standbyPollInterval() = %v，want %v", got, standbyPollIntervalDefault)
	}

	// ③ 并发写 + 读（-race 钉子：这里用裸包变量必报竞争）。
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				standbyTime.minute.Store(int64(time.Millisecond * time.Duration(j%7+1)))
				standbyTime.poll.Store(int64(time.Millisecond * time.Duration(j%5+1)))
				_ = standbyMinute()
				_ = standbyPollInterval()
				_ = standbyHardCap()
			}
		}(i)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// 游标与归属 session 的取法（回查 tool.call.intent）
// ---------------------------------------------------------------------------

// TestStandbyLocateIntentGivesSessionAndCursor：归属 session 与重放游标都来自
// 事件流里那条 tool.call.intent（envelope.session_id / envelope.seq）——
// 不是"现在"，也不是猜的。
func TestStandbyLocateIntentGivesSessionAndCursor(t *testing.T) {
	st, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"), sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	sid := contracts.Ulid(core.NewUlid())
	aid := contracts.Ulid(core.NewUlid())
	intent := contracts.ToolCallIntentPayload{
		ToolCallID: "toolu_LOCATE_01",
		ToolName:   ToolStandby,
		Arguments:  json.RawMessage(`{"wait":"message","timeout_min":1,"on_timeout":"return"}`),
	}
	raw, err := json.Marshal(intent)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	evs, err := st.Append(context.Background(), []contracts.Event{{
		V: 1, ID: core.NewUlid(), Kind: loop.KindToolCallIntent, TS: "2026-01-01T00:00:00Z",
		SessionID: sid, AgentID: aid, Durable: true, Payload: raw,
	}})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(evs) != 1 || evs[0].Seq == nil {
		t.Fatalf("intent 事件该带回 seq：%+v", evs)
	}
	// 再写一条**别的** tool_call_id 的 intent（证明定位不是"取最后一条"）。
	other := intent
	other.ToolCallID = "toolu_LOCATE_02"
	otherRaw, _ := json.Marshal(other)
	if _, err := st.Append(context.Background(), []contracts.Event{{
		V: 1, ID: core.NewUlid(), Kind: loop.KindToolCallIntent, TS: "2026-01-01T00:00:01Z",
		SessionID: contracts.Ulid(core.NewUlid()), AgentID: aid, Durable: true, Payload: otherRaw,
	}}); err != nil {
		t.Fatalf("Append(other): %v", err)
	}

	ex := newStandbyExecutor(standbyDeps{store: st})
	ref, ok, err := ex.locateIntent(context.Background(), intent)
	if err != nil {
		t.Fatalf("locateIntent: %v", err)
	}
	if !ok {
		t.Fatal("locateIntent 该找到本次 intent")
	}
	if ref.SessionID != sid {
		t.Fatalf("归属 session = %q，want %q（intent 事件的 envelope.session_id）", ref.SessionID, sid)
	}
	if ref.AgentID != aid {
		t.Fatalf("发起 agent = %q，want %q", ref.AgentID, aid)
	}
	if ref.Seq != *evs[0].Seq {
		t.Fatalf("重放游标 seq = %d，want %d（本次 tool.call.intent 的 seq，不是\"现在\"）", ref.Seq, *evs[0].Seq)
	}

	// 查不到 → ok=false（调用方据此明确失败，不猜 session）。
	if _, ok, err := ex.locateIntent(context.Background(), contracts.ToolCallIntentPayload{
		ToolCallID: "toolu_NOT_THERE", ToolName: ToolStandby,
	}); err != nil {
		t.Fatalf("locateIntent(缺失): %v", err)
	} else if ok {
		t.Fatal("查不到的 tool_call_id 不该返回候选")
	}
}

// TestStandbyLocateIntentAmbiguousFails：两个 session **同一个 tool_call_id**、
// 参数又对不上、且没有一条的 session 在跑 → **查不到**（ok=false）：**不猜归属**
// （critic F2：旧实现的"档 3"会取 seq 最大的那条 = 猜 session → waiting 态挂到
// 别人的 session 上、K11 按错的 session 过滤，两处都错）。
//
// 对照：参数对得上（档 1）→ 照样能定位（见上一个用例）。
func TestStandbyLocateIntentAmbiguousFails(t *testing.T) {
	st, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"), sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	const callID = "toolu_AMBIG_01"
	sidA := contracts.Ulid(core.NewUlid())
	sidB := contracts.Ulid(core.NewUlid())
	aid := contracts.Ulid(core.NewUlid())
	appendIntent := func(sid contracts.Ulid, args string) {
		t.Helper()
		raw, merr := json.Marshal(contracts.ToolCallIntentPayload{
			ToolCallID: callID,
			ToolName:   ToolStandby,
			Arguments:  json.RawMessage(args),
		})
		if merr != nil {
			t.Fatalf("Marshal: %v", merr)
		}
		if _, aerr := st.Append(context.Background(), []contracts.Event{{
			V: 1, ID: core.NewUlid(), Kind: loop.KindToolCallIntent, TS: "2026-01-01T00:00:00Z",
			SessionID: sid, AgentID: aid, Durable: true, Payload: raw,
		}}); aerr != nil {
			t.Fatalf("Append: %v", aerr)
		}
	}
	// 同一个 tool_call_id、**不同参数**、不同 session。
	appendIntent(sidA, `{"wait":"message","timeout_min":1,"on_timeout":"return"}`)
	appendIntent(sidB, `{"wait":"handoff","timeout_min":5,"on_timeout":"sleep"}`)

	ex := newStandbyExecutor(standbyDeps{store: st})
	ref, ok, err := ex.locateIntent(context.Background(), contracts.ToolCallIntentPayload{
		ToolCallID: callID,
		ToolName:   ToolStandby,
		Arguments:  json.RawMessage(`{"wait":"handoff","timeout_min":9,"on_timeout":"return"}`),
	})
	if err != nil {
		t.Fatalf("locateIntent: %v", err)
	}
	if ok {
		t.Fatalf("同 id 不同参数且都不在跑 → 该查不到（不猜 session），却返回了 %+v", ref)
	}
}
