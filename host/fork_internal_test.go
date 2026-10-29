package host

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/runtime"
)

// 本文件是 v3-S2 fork + /btw（K29）的**验收用例**（11 §2 v3-S2 行的三条断言逐条钉死）：
//
//  1. /btw 结果不进主上下文：跑 /btw 前后主 session 的事件流**逐条零变化**；
//  2. fork 附带上下文且不带 ledger/gate：旁支"记得"主对话内容，但**不含**主 session 的
//     ledger / gate（连内容都不进旁支的模型请求）；
//  3. 生成新 session 钩子埋点存在且确实不做实现：钩子可触发/可观测，但触发不"生成"任何
//     一流 session（框架只给事实）。
//
// 为什么是 internal（package host）：要能往主 session 注入一条 ledger.updated / gate.joined
// （经 h.bus）来造"主 session 有账本/门禁"的场景，并直接 ReplaySession(h.store, ...) 断言
// 主/旁支两条流——黑箱面没有"任意发事件"的公开 API。

// forkTestProvider 是脚本 provider：每轮返回一段带序号的文本（无工具调用 → 单轮收敛），
// 并**记录每次请求的 messages**——于是"旁支的模型请求带没带主上下文"可被直接断言。
type forkTestProvider struct {
	mu       sync.Mutex
	requests [][]core.Message
}

func (p *forkTestProvider) Run(_ context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu.Lock()
	msgs := append([]core.Message(nil), req.Messages...)
	p.requests = append(p.requests, msgs)
	n := len(p.requests)
	p.mu.Unlock()
	if emit != nil {
		emit(core.ProviderEvent{Kind: "done"})
	}
	return core.ModelResult{
		Text:  fmt.Sprintf("答复-%d", n),
		Usage: core.Usage{InputTokens: 1, OutputTokens: 1},
	}, nil
}

// lastRequest 返回最近一次模型请求的 messages（fork 的那一轮 = 最后一次）。
func (p *forkTestProvider) lastRequest() []core.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.requests) == 0 {
		return nil
	}
	return p.requests[len(p.requests)-1]
}

// newForkTestHub 装配一个 Hub：临时事件库 + 脚本 provider + **非 git 的 CWD**（无自动 gate，
// 门禁由测试自注入，确定性）。
func newForkTestHub(t *testing.T, prov core.Provider) *Hub {
	t.Helper()
	h, err := New(Config{
		DBPath:   filepath.Join(t.TempDir(), "events.db"),
		Model:    "fork-test",
		System:   "fork-system",
		MaxTurns: 4,
		Provider: prov,
		CWD:      t.TempDir(), // 非 git 目录 → 无自动 repo gate
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// publishToSession 经 bus 往指定 session 注入一条 durable 事件（测试造"主 session 有账本/门禁"）。
func publishToSession(t *testing.T, h *Hub, sid, agentID contracts.Ulid, kind string, payload any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s: %v", kind, err)
	}
	ev := contracts.Event{
		V:         1,
		ID:        core.NewUlid(),
		Kind:      kind,
		TS:        time.Now().UTC().Format(time.RFC3339),
		SessionID: sid,
		AgentID:   agentID,
		Durable:   true,
		Payload:   raw,
	}
	if err := h.bus.Publish(context.Background(), ev); err != nil {
		t.Fatalf("publish %s: %v", kind, err)
	}
}

// eventSig 把一条事件压成可比较的签名（kind + payload；seq/ts 不纳入——主 session 事件不可变，
// 但注入事件的 ts 会变，这里只比"事实内容"）。
func eventSig(ev contracts.Event) string {
	return ev.Kind + "|" + string(ev.Payload)
}

// sigs 取一个 session 的事件签名列表（按 seq 升序 = ReplaySession 的返回序）。
func sigs(t *testing.T, h *Hub, sid contracts.Ulid) []string {
	t.Helper()
	evs, err := runtime.ReplaySession(context.Background(), h.store, sid)
	if err != nil {
		t.Fatalf("replay %s: %v", sid, err)
	}
	out := make([]string, len(evs))
	for i, ev := range evs {
		out[i] = eventSig(ev)
	}
	return out
}

func hasKind(evs []contracts.Event, kind string) bool {
	for _, ev := range evs {
		if ev.Kind == kind {
			return true
		}
	}
	return false
}

func messagesContain(msgs []core.Message, sub string) bool {
	for _, m := range msgs {
		if strings.Contains(m.Content, sub) {
			return true
		}
	}
	return false
}

// 断言①：/btw 结果不进主上下文——主 session 事件流跑 /btw 前后**逐条零变化**。
func TestRunBtwMainContextZeroChange(t *testing.T) {
	prov := &forkTestProvider{}
	h := newForkTestHub(t, prov)
	ctx := context.Background()
	sid := NewSession()

	// 主 session 跑两轮对话（每轮 RunPrompt 是独立 loop.Run，不带前文——对话历史靠 fold 重建）。
	if _, err := h.RunPrompt(ctx, sid, "主问题-A"); err != nil {
		t.Fatalf("RunPrompt A: %v", err)
	}
	if _, err := h.RunPrompt(ctx, sid, "主问题-B"); err != nil {
		t.Fatalf("RunPrompt B: %v", err)
	}

	before := sigs(t, h, sid)
	beforeKinds := kindCounts(t, h, sid)
	if len(before) == 0 {
		t.Fatal("主 session 居然没有事件")
	}

	res, err := h.RunBtw(ctx, sid, "旁支问题")
	if err != nil {
		t.Fatalf("RunBtw: %v", err)
	}
	if res.Text == "" {
		t.Fatal("旁支无文本输出")
	}
	if res.ForkSessionID == sid {
		t.Fatal("fork session 不该等于主 session（归属必须可辨）")
	}

	// 主 session 事件流零变化（/btw 不回灌主上下文）——逐条签名全等。
	after := sigs(t, h, sid)
	if len(before) != len(after) {
		t.Fatalf("主 session 事件条数被 /btw 改动：%d → %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("主 session 第 %d 条事件被 /btw 改动：\n before %s\n after  %s", i, before[i], after[i])
		}
	}

	// 显式钉住验收口径：user.prompt / model.turn 条数 + context.assembled 历史零变化。
	afterKinds := kindCounts(t, h, sid)
	for _, k := range []string{"user.prompt", "model.turn", "context.assembled"} {
		if beforeKinds[k] != afterKinds[k] {
			t.Fatalf("主 session %s 条数被 /btw 改动：%d → %d", k, beforeKinds[k], afterKinds[k])
		}
	}
}

// kindCounts 统计一个 session 里各 kind 的条数（诊断/断言用）。
func kindCounts(t *testing.T, h *Hub, sid contracts.Ulid) map[string]int {
	t.Helper()
	evs, err := runtime.ReplaySession(context.Background(), h.store, sid)
	if err != nil {
		t.Fatalf("replay %s: %v", sid, err)
	}
	out := map[string]int{}
	for _, ev := range evs {
		out[ev.Kind]++
	}
	return out
}

// 断言②：fork 附带上下文且不带 ledger/gate。
func TestRunBtwCarriesContextNotLedgerGate(t *testing.T) {
	prov := &forkTestProvider{}
	h := newForkTestHub(t, prov)
	ctx := context.Background()
	sid := NewSession()

	// 主 session 两轮对话 → 事件流里有可折叠的对话历史。
	if _, err := h.RunPrompt(ctx, sid, "主对话-甲"); err != nil {
		t.Fatalf("RunPrompt 甲: %v", err)
	}
	if _, err := h.RunPrompt(ctx, sid, "主对话-乙"); err != nil {
		t.Fatalf("RunPrompt 乙: %v", err)
	}

	// 取主 session 的主 agent（注入事件归属用）。
	mainEvents, err := runtime.ReplaySession(ctx, h.store, sid)
	if err != nil {
		t.Fatalf("replay main: %v", err)
	}
	if len(mainEvents) == 0 {
		t.Fatal("主 session 无事件")
	}
	mainAgent := mainEvents[0].AgentID

	// 往主 session 注入一条 ledger.updated + 一条 gate.joined（造"主有账本/门禁"的场景）。
	// 用独特哨兵字符串，便于断言"旁支的模型请求不含它们"。
	publishToSession(t, h, sid, mainAgent, "ledger.updated", contracts.LedgerUpdatedPayload{
		Goals:       []string{"LEDGER-SENTINEL-GOAL"},
		Constraints: []string{"LEDGER-SENTINEL-CONSTRAINT"},
	})
	publishToSession(t, h, sid, mainAgent, "gate.joined", contracts.GateJoinedPayload{
		GateID: "GATE-SENTINEL-ID",
		Source: contracts.GateJoinSourceExplicit,
	})

	// 主 session 确实有 ledger / gate（否则下面的"旁支没有"是空断言）。
	if !hasKind(mustReplay(t, h, sid), "ledger.updated") || !hasKind(mustReplay(t, h, sid), "gate.joined") {
		t.Fatal("主 session 应有注入的 ledger.updated + gate.joined")
	}

	// /btw 旁支。
	res, err := h.RunBtw(ctx, sid, "旁支提问")
	if err != nil {
		t.Fatalf("RunBtw: %v", err)
	}

	// 旁支 session 的事件流：有 btw.forked + 对话入流，但**没有** ledger/gate。
	forkEvents := mustReplay(t, h, res.ForkSessionID)
	if !hasKind(forkEvents, KindBtwForked) {
		t.Fatal("旁支应有 btw.forked 标记")
	}
	if !hasKind(forkEvents, "user.prompt") || !hasKind(forkEvents, "model.turn") {
		t.Fatal("旁支应有对话入流（user.prompt / model.turn）")
	}
	if hasKind(forkEvents, "ledger.updated") {
		t.Fatal("旁支不该有 ledger.updated（fork 不带 ledger）")
	}
	if hasKind(forkEvents, "gate.joined") {
		t.Fatal("旁支不该有 gate.joined（fork 不带 gate）")
	}

	// 旁支"记得"主上下文：最后一次模型请求（fork 那一轮）带着主对话的折叠历史。
	forkReq := prov.lastRequest()
	for _, want := range []string{"主对话-甲", "主对话-乙", "答复-1", "答复-2", "旁支提问"} {
		if !messagesContain(forkReq, want) {
			t.Fatalf("旁支的模型请求缺主上下文 %q（fork 应附带对话上下文）", want)
		}
	}
	// 旁支**不带** ledger/gate：连内容都不进模型请求（哨兵字符串不得出现）。
	for _, forbidden := range []string{"LEDGER-SENTINEL-GOAL", "LEDGER-SENTINEL-CONSTRAINT", "GATE-SENTINEL-ID"} {
		if messagesContain(forkReq, forbidden) {
			t.Fatalf("旁支的模型请求含 ledger/gate 内容 %q（fork 不该带 ledger/gate）", forbidden)
		}
	}
}

func mustReplay(t *testing.T, h *Hub, sid contracts.Ulid) []contracts.Event {
	t.Helper()
	evs, err := runtime.ReplaySession(context.Background(), h.store, sid)
	if err != nil {
		t.Fatalf("replay %s: %v", sid, err)
	}
	return evs
}

// storeSessionIDs 取库里出现过的全部 session id（扫全事件流 = "库内 session 集合"）。
func storeSessionIDs(t *testing.T, h *Hub) map[contracts.Ulid]bool {
	t.Helper()
	evs, err := h.store.ReadFrom(context.Background(), nil, 1, 0)
	if err != nil {
		t.Fatalf("读事件流: %v", err)
	}
	out := make(map[contracts.Ulid]bool, len(evs))
	for _, ev := range evs {
		out[ev.SessionID] = true
	}
	return out
}

// specHas 报告某 session 是否在 hub 的 specs 登记表里（"只登记、不落事件"的形态）。
func specHas(h *Hub, sid contracts.Ulid) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.specs[sid]
	return ok
}

// forkGuardrailSnapshot 是"库内 session 集合 + hub 内存登记"的合并快照
// （护栏的"之前"基线）。
//
// 为什么要把**内存登记**也算进去（N4）：只比"落过事件的 session"的话，一个
// **只登记不落事件**的 session 会从护栏眼皮底下溜过去——框架真把旁支登记成
// 一等 session 时正是这种形态（CreateSession 只写 specs，一个事件都不落）。
type forkGuardrailSnapshot struct {
	// events 是事件流里出现过的 session（落过事件的）。
	events map[contracts.Ulid]bool
	// specs 是 hub 内存里登记了归属的 session（CreateSession 写的 specs）。
	specs map[contracts.Ulid]bool
	// loops 是 hub 托管的 session（loop 建成 = 一等会话）。
	loops map[contracts.Ulid]bool
}

// takeForkGuardrailSnapshot 取当前快照（事件流 + 两张内存登记表）。
func takeForkGuardrailSnapshot(t *testing.T, h *Hub) forkGuardrailSnapshot {
	t.Helper()
	h.mu.Lock()
	specs := make(map[contracts.Ulid]bool, len(h.specs))
	for sid := range h.specs {
		specs[sid] = true
	}
	loops := make(map[contracts.Ulid]bool, len(h.loops))
	for sid := range h.loops {
		loops[sid] = true
	}
	h.mu.Unlock()
	return forkGuardrailSnapshot{
		events: storeSessionIDs(t, h),
		specs:  specs,
		loops:  loops,
	}
}

// extraSessions 返回"多出来的 session"（排除 fork 自身与基线里已有的）。
//
// 这是"埋点≠实现"的**护栏探针**：框架/钩子若真生成了新的一流 session，它必然在
// **事件流**或**内存登记**（specs / loops）里留下一条——本探针把两处都抓出来
// （探针本身在对照用例里被验证有效，见
// TestForkPromoteHookGuardrailDetectsGeneratedSession）。
func (s forkGuardrailSnapshot) extraSessions(t *testing.T, h *Hub, forkSID contracts.Ulid) []contracts.Ulid {
	t.Helper()
	now := takeForkGuardrailSnapshot(t, h)
	seen := map[contracts.Ulid]bool{}
	var out []contracts.Ulid
	for _, set := range []map[contracts.Ulid]bool{now.events, now.specs, now.loops} {
		for sid := range set {
			if sid == forkSID || seen[sid] {
				continue
			}
			if s.events[sid] || s.specs[sid] || s.loops[sid] {
				continue // 基线里就有 → 不是"多出来的"
			}
			seen[sid] = true
			out = append(out, sid)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// 断言③：生成新 session 钩子埋点存在，且确实不做实现。
//
// 护栏有三条（缺一条，这条断言就会被"框架真生成新 session"的变异蒙混过去）：
//
//	① **session 集合前后对比**（事件流 **+ 内存登记**）：跑 /btw 前后，只允许多出
//	   fork 自己那条 session（旁支）；多出任何别的 session = 框架/钩子生成了新的一流
//	   session。只比"落过事件的"不够（N4）：只登记不落事件的形态（CreateSession）也
//	   逃不出这张网；
//	② fork **不是本 hub 托管的 session**（hostsSession==false）、发现表里也**不是 live**
//	   （IsLive==false）——旁支不是一等会话；
//	③ 对照用例（TestForkPromoteHookGuardrailDetectsGeneratedSession）：钩子里**故意**
//	   造 session（跑过一轮的 + 只登记的），同一把探针必须把它们都抓出来——证明护栏
//	   不是空转。
func TestRunBtwFiresPromoteHookWithoutImplementing(t *testing.T) {
	prov := &forkTestProvider{}
	h := newForkTestHub(t, prov)
	ctx := context.Background()
	sid := NewSession()
	if _, err := h.RunPrompt(ctx, sid, "主问题"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	before := takeForkGuardrailSnapshot(t, h)

	// 埋点可观测：挂钩子后跑 /btw，钩子应触发一次并拿到事实。
	var got []BtwResult
	h.SetBtwPromoteHook(func(r BtwResult) { got = append(got, r) })

	res, err := h.RunBtw(ctx, sid, "钩子问题")
	if err != nil {
		t.Fatalf("RunBtw: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("钩子应触发一次，得到 %d 次", len(got))
	}
	if got[0].ForkSessionID != res.ForkSessionID || got[0].SourceSessionID != sid {
		t.Fatalf("钩子拿到的事实不符：%+v", got[0])
	}
	if got[0].Question != "钩子问题" {
		t.Fatalf("钩子拿到的 question 不符：%q", got[0].Question)
	}

	// 护栏①：**事件流 + 内存登记**都只多出 fork 自己那条 session（钩子/框架**不生成**
	// 新的一流 session）。内存登记也算：只登记不落事件的形态（CreateSession）同样
	// 逃不出这张网（N4）。
	if extra := before.extraSessions(t, h, res.ForkSessionID); len(extra) != 0 {
		t.Fatalf("埋点不该生成新 session，库里/内存里多出：%v", extra)
	}
	// 护栏②：fork 不是托管 session（没建 loop）、也不是 live session（旁支不是一等会话）。
	if h.hostsSession(res.ForkSessionID) {
		t.Fatal("fork session 不该被本 hub 托管（一流会话才建 loop）")
	}
	if h.Discovery().IsLive(res.ForkSessionID) {
		t.Fatal("fork session 不该是 live session（旁支不是一等会话）")
	}

	// 在线表里只该有主 session（fork 被过滤、钩子不生成 session）。
	rows, err := h.Sessions(ctx)
	if err != nil {
		t.Fatalf("Sessions: %v", err)
	}
	for _, r := range rows {
		if r.SessionID == string(res.ForkSessionID) {
			t.Fatal("fork session 不该出现在在线表（一次性旁支）")
		}
	}
	// 摘钩子后再跑一次：不再触发（默认 = 埋点存在但无人消费）。
	h.SetBtwPromoteHook(nil)
	got = nil
	if _, err := h.RunBtw(ctx, sid, "再问"); err != nil {
		t.Fatalf("RunBtw 2: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("摘钩子后不该再触发，得到 %d", len(got))
	}
}

// TestForkPromoteHookGuardrailDetectsGeneratedSession：**对照用例**——证明上面那条
// 护栏真的有杀伤力（不是空转的断言）。
//
// 钩子里**故意**生成一个新 session（= "框架真把 /btw 提成一流 session"的那个变异），
// 同一把探针（extraSessions）必须把它抓出来；抓不到就说明护栏形同虚设。
func TestForkPromoteHookGuardrailDetectsGeneratedSession(t *testing.T) {
	prov := &forkTestProvider{}
	h := newForkTestHub(t, prov)
	ctx := context.Background()
	sid := NewSession()
	if _, err := h.RunPrompt(ctx, sid, "主问题"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	before := takeForkGuardrailSnapshot(t, h)

	// 钩子里故意造两个 session（模拟"框架真生成新 session"的变异）：
	//   - 一个跑过一轮（事件流 + 托管 loop 都有）；
	//   - 一个只登记归属（CreateSession：**一个事件都不落**，只写内存里的 specs）——
	//     这正是"只比落事件的 session"会漏掉的形态（N4）。
	ran := NewSession()
	h.SetBtwPromoteHook(func(r BtwResult) {
		if _, err := h.RunPrompt(ctx, ran, "钩子里造一个 session"); err != nil {
			t.Errorf("钩子里的 RunPrompt: %v", err)
		}
		if _, err := h.CreateSession(SessionSpec{}); err != nil {
			t.Errorf("钩子里的 CreateSession: %v", err)
		}
	})

	res, err := h.RunBtw(ctx, sid, "对照问题")
	if err != nil {
		t.Fatalf("RunBtw: %v", err)
	}
	extra := before.extraSessions(t, h, res.ForkSessionID)
	if len(extra) != 2 {
		t.Fatalf("护栏探针应抓到钩子生成的 2 个 session，实际 %d 个：%v", len(extra), extra)
	}
	got := map[contracts.Ulid]bool{}
	for _, s := range extra {
		got[s] = true
	}
	if !got[ran] {
		t.Fatalf("护栏探针没抓到钩子里跑过的 session：%v", extra)
	}
	// 只登记不落事件的形态也必须被抓到（N4 的逃逸形态）。
	specHit := false
	for _, s := range extra {
		if s != ran && specHas(h, s) {
			specHit = true
		}
	}
	if !specHit {
		t.Fatalf("护栏探针没抓到只登记不落事件的 session（N4 的逃逸形态）：%v", extra)
	}
}

// 空问题 → 报错，且不产任何 fork（fail-close：不跑一个没有问题的旁支）。
func TestRunBtwEmptyQuestionRejected(t *testing.T) {
	prov := &forkTestProvider{}
	h := newForkTestHub(t, prov)
	ctx := context.Background()
	sid := NewSession()
	if _, err := h.RunPrompt(ctx, sid, "主问题"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	before := sigs(t, h, sid)

	if _, err := h.RunBtw(ctx, sid, "   "); err == nil {
		t.Fatal("空 /btw 问题应报错")
	}
	after := sigs(t, h, sid)
	if len(before) != len(after) {
		t.Fatalf("空 /btw 不该改动主 session：%d → %d", len(before), len(after))
	}
}

// /btw 命令解析（CLI 与 TUI 共用 host.ParseBtwCommand）：**space 与 tab 都算分隔符**
// （挂账 e 的"tab 分隔变体也要认"同款口径），"/btwxyz" 不当命令。
func TestParseBtwCommand(t *testing.T) {
	cases := []struct {
		line     string
		isBtw    bool
		question string
	}{
		{"/btw", true, ""},
		{"/btw ", true, ""},
		{"/btw 问题", true, "问题"},
		{"/btw\t问题", true, "问题"},
		{"/btw\t\t问题", true, "问题"},
		{"  /btw 问题  ", true, "问题"},
		{"/btwxyz", false, ""},
		{"/btw。", false, ""},
		{"普通问题", false, ""},
		{"", false, ""},
	}
	for _, c := range cases {
		q, isBtw := ParseBtwCommand(c.line)
		if isBtw != c.isBtw || q != c.question {
			t.Fatalf("ParseBtwCommand(%q) = (%q, %v)，want (%q, %v)",
				c.line, q, isBtw, c.question, c.isBtw)
		}
	}
}

// fork session 不进在线表（一次性旁支执行线，结果只回给用户）。
func TestForkSessionHiddenFromOnlineTable(t *testing.T) {
	prov := &forkTestProvider{}
	h := newForkTestHub(t, prov)
	ctx := context.Background()
	sid := NewSession()
	if _, err := h.RunPrompt(ctx, sid, "主问题"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	res, err := h.RunBtw(ctx, sid, "旁支问题")
	if err != nil {
		t.Fatalf("RunBtw: %v", err)
	}
	rows, err := h.Sessions(ctx)
	if err != nil {
		t.Fatalf("Sessions: %v", err)
	}
	sawMain, sawFork := false, false
	for _, r := range rows {
		if r.SessionID == string(sid) {
			sawMain = true
		}
		if r.SessionID == string(res.ForkSessionID) {
			sawFork = true
		}
	}
	if !sawMain {
		t.Fatal("主 session 该出现在在线表")
	}
	if sawFork {
		t.Fatal("fork session 不该出现在在线表（/btw 一次性旁支）")
	}
}

// ---------------------------------------------------------------------------
// F4：主 session id 的 fail-close 校验
// ---------------------------------------------------------------------------

// /btw 的主 session id 必须合法（fail-close）：空 id / 非法 id 一律拒，**不许**落
// btw.forked ——否则载荷里会出现 source_session_id: ""（契约非法：id 必须是 26 字符
// Crockford base32）。
func TestRunBtwRejectsInvalidMainSessionID(t *testing.T) {
	prov := &forkTestProvider{}
	h := newForkTestHub(t, prov)
	ctx := context.Background()

	for _, bad := range []contracts.Ulid{"", "not-a-ulid", "01M60WTV5JWQSX95814RZ291A"} {
		res, err := h.RunBtw(ctx, bad, "问题")
		if err == nil {
			t.Fatalf("主 session id %q 应被拒（不该产出一条没有合法来源的旁支）", bad)
		}
		if !strings.Contains(err.Error(), "ULID") {
			t.Fatalf("错误应说明 id 非法：%v", err)
		}
		if res.ForkSessionID != "" {
			t.Fatalf("被拒时不该产出 fork session：%s", res.ForkSessionID)
		}
	}
	// 库里不该出现任何 btw.forked（fail-close：非法 id 不许落事件）。
	evs, err := h.store.ReadFrom(ctx, nil, 1, 0)
	if err != nil {
		t.Fatalf("读事件流: %v", err)
	}
	for _, ev := range evs {
		if ev.Kind == KindBtwForked {
			t.Fatal("非法 id 的 /btw 不该落 btw.forked 事件")
		}
	}
}

// /btw 一个"id 合法但没有任何事件"的 session → **可区分**的提示（不是"id 非法"）。
//
// 为什么要可区分：用户手敲错 id 与"session 还没跑过对话"是两回事，提示混在一起
// 用户就没法判断该改什么。
func TestRunBtwDistinguishesEmptySession(t *testing.T) {
	prov := &forkTestProvider{}
	h := newForkTestHub(t, prov)
	ctx := context.Background()

	empty := NewSession() // 合法 ULID，但事件流里没有它
	_, err := h.RunBtw(ctx, empty, "问题")
	if err == nil {
		t.Fatal("没有任何事件的主 session 应报错（不许静默跑一个没有上下文的旁支）")
	}
	if !strings.Contains(err.Error(), "还没有任何事件") {
		t.Fatalf("错误应可区分（提示 session 没有事件）：%v", err)
	}
	if strings.Contains(err.Error(), "ULID") {
		t.Fatalf("不该报成 id 非法：%v", err)
	}
}

// ---------------------------------------------------------------------------
// F7：旁支带的主对话历史有上限 + 截断告知
// ---------------------------------------------------------------------------

// 旁支带的主对话历史**有上限**，超了要截断并**告知**（F7：旁支比主会话记得多，
// 不设界就是 token/费用双爆，且用户会误以为旁支"记得全部"）。
func TestRunBtwTruncatesLongHistory(t *testing.T) {
	prov := &forkTestProvider{}
	h := newForkTestHub(t, prov)
	ctx := context.Background()
	sid := NewSession()

	// 主 session 跑很多轮（每轮一条 user + 一条 assistant）→ 折叠历史远超条数上限。
	const rounds = maxForkHistoryMessages + 6
	for i := 0; i < rounds; i++ {
		if _, err := h.RunPrompt(ctx, sid, fmt.Sprintf("主问题-%02d", i)); err != nil {
			t.Fatalf("RunPrompt %d: %v", i, err)
		}
	}

	res, err := h.RunBtw(ctx, sid, "旁支问题")
	if err != nil {
		t.Fatalf("RunBtw: %v", err)
	}
	if !res.HistoryTruncated {
		t.Fatal("超限的主对话历史应被截断")
	}
	if res.HistoryMessages > maxForkHistoryMessages {
		t.Fatalf("旁支带了 %d 条主对话（上限 %d）", res.HistoryMessages, maxForkHistoryMessages)
	}
	// 用户侧：截断这件事要能被调用方看见（CLI/TUI 据此提示用户）。
	if res.HistoryMessages == 0 {
		t.Fatal("截断后仍应保留最近的主对话")
	}

	// 模型侧：请求里要有截断说明，且最早/最近的对话取舍正确（保尾）。
	req := prov.lastRequest()
	if !messagesContain(req, "截断") {
		t.Fatalf("模型请求里应有截断告知（不许静默截断）")
	}
	if messagesContain(req, "主问题-00") {
		t.Fatal("最早的主对话应被截掉")
	}
	if !messagesContain(req, fmt.Sprintf("主问题-%02d", rounds-1)) {
		t.Fatal("最近的主对话应保留（保尾）")
	}
}

// ---------------------------------------------------------------------------
// F7 的裁切细节：上限计参数（N2）、两种裁切分标志（N5）、按 id 配平（N6）
// ---------------------------------------------------------------------------

// 旁支上下文上限**要算工具参数**（N2）：assistant 的 ToolCall.Args 是模型要吃进去的
// 窗口（core/loop 的 estimateMessageTokens 同口径），一条 write 大文件的参数就能
// 单独顶掉整个额度——只算正文时上限形同虚设。
func TestTrimForkHistoryCountsToolCallArgs(t *testing.T) {
	big := core.Message{
		Role:    "assistant",
		Content: "写文件",
		ToolCalls: []core.ToolCallReq{{
			ID:   "tc-big",
			Name: "write",
			Args: json.RawMessage(`{"content":"` + strings.Repeat("x", maxForkHistoryChars+100) + `"}`),
		}},
	}
	msgs := []core.Message{
		{Role: "user", Content: "问题"},
		big,
		{Role: "user", Content: "后续问题"},
		{Role: "assistant", Content: "答复"},
	}
	kept, count, byLimit, byBalance := trimForkHistory(msgs)
	if !byLimit {
		t.Fatal("工具参数超限时也应触发 byLimit（参数同样占模型窗口）")
	}
	if count == 0 {
		t.Fatal("不该把历史全丢")
	}
	// 被裁掉的必须是那条超大参数的消息（它的正文很短——不算参数就抓不到它）。
	for _, m := range kept {
		for _, tc := range m.ToolCalls {
			if len(tc.Args) > maxForkHistoryChars {
				t.Fatal("超大工具参数的消息没被裁掉（上限没算参数）")
			}
		}
	}
	// 裁后的总成本在预算内（口径同 forkMessageChars）。
	total := 0
	for _, m := range kept {
		total += forkMessageChars(m)
	}
	if total > maxForkHistoryChars {
		t.Fatalf("裁后成本 %d 超过上限 %d", total, maxForkHistoryChars)
	}
	_ = byBalance
}

// 配平丢弃与上限截断是**两回事**（N5）：tool_calls 没配齐 tool 结果的 assistant 被丢，
// 报的是 byBalance（"这段 provider 收不了"），不是 byLimit（"太长装不下"）。
func TestTrimForkHistoryReportsBalanceTrimSeparately(t *testing.T) {
	msgs := []core.Message{
		{Role: "user", Content: "问题"},
		// 声明了工具调用却没有结果 → 配平阶段丢弃。
		{Role: "assistant", Content: "我要写文件", ToolCalls: []core.ToolCallReq{{ID: "tc1", Name: "write"}}},
	}
	kept, count, byLimit, byBalance := trimForkHistory(msgs)
	if byLimit {
		t.Fatal("没触上限，不该报 byLimit")
	}
	if !byBalance {
		t.Fatal("配平丢弃应报 byBalance")
	}
	if count != 1 {
		t.Fatalf("kept = %d 条，want 1（只留 user 那条）", count)
	}
	for _, m := range kept {
		if len(m.ToolCalls) > 0 {
			t.Fatal("配对不完整的 assistant 不该留在历史里")
		}
	}
}

// balanceHistory 按 **id** 配对，不按位置（N6）：tool 的结果**晚到**时（排在别的
// assistant 之后），按"紧邻的一段"来配就会认错人——把别人的 tool 结果算到自己头上，
// 结果是留下一条**孤立**的 tool 消息（provider 会拒掉整条请求）。
func TestBalanceHistoryMatchesToolCallsByID(t *testing.T) {
	msgs := []core.Message{
		{Role: "user", Content: "问题"},
		// 先调 a，再调 b；**a 的结果晚到**（排在 assistant b 之后）。
		{Role: "assistant", Content: "先调 a", ToolCalls: []core.ToolCallReq{{ID: "a", Name: "t"}}},
		{Role: "assistant", Content: "再调 b", ToolCalls: []core.ToolCallReq{{ID: "b", Name: "t"}}},
		{Role: "tool", Content: "结果A", ToolCallID: "a"},
		// 声明 c1/c2 两个调用，却只收到 c1 的结果 → 这条 assistant 不完整 → 它**和它名下
		// 的 tool(c1)** 一起被删（按 id 删：只删 assistant 会留下孤立的 tool 消息）。
		{Role: "assistant", Content: "两个调用", ToolCalls: []core.ToolCallReq{{ID: "c1", Name: "t"}, {ID: "c2", Name: "t"}}},
		{Role: "tool", Content: "结果C1", ToolCallID: "c1"},
	}
	kept := balanceHistory(msgs)

	// ① 声明 a 的 assistant 与**它自己的**结果都该留下（按 id 配对，与位置无关）。
	sawA, sawResultA := false, false
	for _, m := range kept {
		if m.Role == "assistant" && len(m.ToolCalls) == 1 && m.ToolCalls[0].ID == "a" {
			sawA = true
		}
		if m.Role == "tool" && m.ToolCallID == "a" {
			sawResultA = true
		}
	}
	if !sawA || !sawResultA {
		t.Fatalf("按 id 配对失败：assistant a=%v 结果 a=%v（留下：%+v）", sawA, sawResultA, kept)
	}
	// ② 声明 b 的 assistant 没有结果 → 丢它（连同它名下的 tool 消息——这里没有）。
	for _, m := range kept {
		if m.Role == "assistant" && len(m.ToolCalls) == 1 && m.ToolCalls[0].ID == "b" {
			t.Fatal("配对不完整的 assistant 不该留下")
		}
	}
	// ③ 不许留下孤立的 tool 消息（每条 tool 结果都要有**声明它的** assistant）。
	declared := map[string]bool{}
	for _, m := range kept {
		if m.Role == "assistant" {
			for _, tc := range m.ToolCalls {
				declared[string(tc.ID)] = true
			}
		}
	}
	for _, m := range kept {
		if m.Role == "tool" && !declared[string(m.ToolCallID)] {
			t.Fatalf("留下孤立的 tool 消息（provider 会拒掉整条请求）: %+v", m)
		}
	}
	// ④ 声明 c1/c2 却只收到 c1 的结果 → 这条 assistant **和它名下的 tool(c1)** 一起
	//    被删（按 id 删；只删 assistant 会留下孤立的 tool 消息——provider 会拒掉整条请求）。
	for _, m := range kept {
		if m.Role == "assistant" && len(m.ToolCalls) == 2 {
			t.Fatal("配对不完整的 assistant（c1/c2）不该留下")
		}
		if m.Role == "tool" && m.ToolCallID == "c1" {
			t.Fatal("被删 assistant 名下的 tool(c1) 不该留下（会成孤立消息）")
		}
	}
}

// ---------------------------------------------------------------------------
// F3：旁支不是一等会话（presence/peers 面统一过滤）
// ---------------------------------------------------------------------------

// 旁支 fork **不是**一等会话：即使它被误激活、误 join gate，presence/peers 面也
// 不把它当对端（v3-S2 K29：host 层维护 fork 集合并在 presence/peers 侧过滤，
// core/discovery 不改——X1 纪律）。
//
// 本用例特意造"误配置"（激活 + join 同一个 gate）：不过滤的话旁支就会出现在对端里，
// 过滤才有意义（否则是空断言）。
func TestForkSessionFilteredFromPresenceFaces(t *testing.T) {
	prov := &forkTestProvider{}
	h := newForkTestHub(t, prov)
	ctx := context.Background()
	sid := NewSession()
	if _, err := h.RunPrompt(ctx, sid, "主问题"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	// 主 session 进一个 gate（旁支要能"看见"自己被过滤掉，才说明过滤在起作用）。
	if _, err := h.JoinGate(ctx, sid, "repo:fork-test"); err != nil {
		t.Fatalf("JoinGate: %v", err)
	}
	res, err := h.RunBtw(ctx, sid, "旁支问题")
	if err != nil {
		t.Fatalf("RunBtw: %v", err)
	}

	// 造"误配置"：旁支被激活 + 也 join 了同一个 gate。
	h.Discovery().Activate(res.ForkSessionID)
	ev, err := newGateJoinedEvent(res.ForkSessionID, res.ForkAgentID,
		autoGate{gateID: "repo:fork-test", source: contracts.GateJoinSourceExplicit})
	if err != nil {
		t.Fatalf("newGateJoinedEvent: %v", err)
	}
	if err := h.bus.Publish(ctx, ev); err != nil {
		t.Fatalf("publish gate.joined: %v", err)
	}
	if _, err := h.discFeed.catchUp(ctx); err != nil {
		t.Fatalf("catchUp: %v", err)
	}

	// 对照前提：核心发现表（core/discovery，X1 不改）确实看得见旁支 → 过滤是 host 层做的。
	sawInCore := false
	for _, p := range h.Discovery().VisiblePeers(sid) {
		if p.SessionID == string(res.ForkSessionID) {
			sawInCore = true
		}
	}
	if !sawInCore {
		t.Fatal("对照前提不成立：核心发现表里旁支应当可见（否则本用例是空断言）")
	}

	// host 层：Peers 面过滤掉旁支。
	view, err := h.Peers(ctx, sid)
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	for _, p := range view.Peers {
		if p.SessionID == string(res.ForkSessionID) {
			t.Fatal("旁支 fork 不该出现在 peers 面")
		}
	}
	// host 层：Presence 面过滤掉旁支。
	rep, err := h.Presence(ctx, sid)
	if err != nil {
		t.Fatalf("Presence: %v", err)
	}
	for _, p := range rep.Peers {
		if p.SessionID == string(res.ForkSessionID) {
			t.Fatal("旁支 fork 不该出现在 presence 面")
		}
	}
	// host 层：注入给主 agent 的在线表也过滤掉旁支（与在线表/presence 同一口径）。
	for _, r := range h.onlineSource.OnlineRows(ctx, sid) {
		if r.SessionID == string(res.ForkSessionID) {
			t.Fatal("旁支 fork 不该出现在注入给主 agent 的在线表")
		}
	}
	// host 层：**Discover 的 agent 面**也要过滤（N7）：只滤成员面会留下"成员里没有它、
	// agent 树里却有它"的自相矛盾。
	disc, err := h.Discover(ctx, agentlink.DiscoverQuery{
		Caller:  agentlink.Identity{SessionID: sid},
		AgentID: res.ForkAgentID,
	})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	for _, a := range disc.Agents {
		if a.SessionID == string(res.ForkSessionID) {
			t.Fatal("旁支 fork 的 agent 不该出现在 discover 的 agent 面")
		}
	}
	for _, r := range disc.Sessions {
		if r.SessionID == string(res.ForkSessionID) {
			t.Fatal("旁支 fork 不该出现在 discover 的成员面")
		}
	}
	// gate 成员面同样过滤。
	members, err := h.GateMembers(ctx, "repo:fork-test")
	if err != nil {
		t.Fatalf("GateMembers: %v", err)
	}
	for _, m := range members {
		if m.SessionID == string(res.ForkSessionID) {
			t.Fatal("旁支 fork 不该出现在 gate 成员面")
		}
	}
	// fork 集合本身可见（审计面）：旁支有 id 可查，只是不进"在场"的面。
	found := false
	for _, sid := range h.ForkSessions() {
		if sid == res.ForkSessionID {
			found = true
		}
	}
	if !found {
		t.Fatal("fork 集合里应有旁支 session（审计面要能查到）")
	}
}
