package host

// G-f standby 的**恢复路径**验收：崩溃（intent 已落、completed 未落）之后 resume，
// 那个 in-flight 的 standby 调用怎么处置。
//
// 三组用例：
//
//	① 消息**已到**（崩溃期间到达）→ resume 后**立即**收口成 completed（带消息摘要，如实收口）；
//	② 消息**未到** → resume 后**重新挂起**（waiting 态亮起）→ 到点如实收口（"未等到"）；
//	③ X1 配对完整：收口那条 completed 与原 intent **同一个 tool_call_id**（且没有 failed）。
//
// # 为什么走 durable.Resume 而不是直接调执行器
//
// 处置链在 core/durable（disposeOne → Replayer.Replay），Replayer 的实现是 *core/loop.Loop
// （按 op.Intent.ToolName 去**注册表**取执行器）。这里把 standby 注册进注册表，复现
// "重放带**原 tool_call_id** 走同一个执行器"这条真实链路——游标 = 原 intent 的 seq，
// 中断期间到达的消息才看得见（standby 的重放游标机制，见 host/standby.go）。
//
// 崩溃态是**手工构造**的（照 core/durable/kill9_test.go 的样板）：事件流里只有
// tool.call.intent（无 completed/failed）= 未知结果 = in-flight。

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/core/durable"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// standbyResumeGate 是本文件用例的 gate（收件方要是它的活跃成员，K11）。
const standbyResumeGate = "team:standby-resume"

// standbyResumeCallID 是崩溃那次 standby 调用的 tool_call_id（provider 黑盒 id，S4）。
const standbyResumeCallID = "toolu_STANDBY_RESUME_01"

// standbyResumeMsg 是"崩溃期间到达"的那条消息正文。
const standbyResumeMsg = "我改完了 README"

// standbyResumeProvider 是恢复路径的占位 provider（重放**不调模型**，见 core/loop/replay.go）。
type standbyResumeProvider struct{}

func (standbyResumeProvider) Run(context.Context, core.ModelRequest, func(core.ProviderEvent)) (core.ModelResult, error) {
	return core.ModelResult{}, nil
}

// standbyResumeRig 是"崩溃态 + 恢复"的测试装置。
type standbyResumeRig struct {
	store *sqlite.Store
	lp    *loop.Loop
	disc  *discovery.Table
	sid   contracts.Ulid
	aid   contracts.Ulid
	peer  contracts.Ulid
}

// newStandbyResumeRig 装配恢复路径的最小装置：事件库 + 发现表 + agentlink +
// 注册了 standby 的注册表 + 一个只作 durable.Replayer 的 loop。
func newStandbyResumeRig(t *testing.T) *standbyResumeRig {
	t.Helper()
	st, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"), sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	sid := contracts.Ulid(core.NewUlid())
	aid := contracts.Ulid(core.NewUlid())
	peer := contracts.Ulid(core.NewUlid())

	disc := discovery.New()
	link := agentlink.New(agentlink.Deps{Table: disc, Reader: st})
	feed := newDiscoveryFeed(st, disc)
	reg := registry.New(registry.NewPolicyAdjudicator(policy.New()))
	if err := reg.RegisterTool(standbyDescriptor(), newStandbyExecutor(standbyDeps{
		store:   st,
		link:    link,
		disc:    disc,
		catchUp: feed.catchUp,
	})); err != nil {
		t.Fatalf("注册 standby 工具: %v", err)
	}
	// 正在恢复的 session 由本进程托管 → live（否则 K11 的 gate 成员过滤看不见它）。
	disc.Activate(sid)

	lp, err := loop.New(loop.Config{
		Provider:  standbyResumeProvider{},
		Bus:       bus.New(st),
		Registry:  reg,
		Policy:    policy.New(),
		SessionID: sid,
		AgentID:   aid,
		Model:     "standby-resume-test",
		Store:     st,
	})
	if err != nil {
		t.Fatalf("loop.New: %v", err)
	}
	return &standbyResumeRig{store: st, lp: lp, disc: disc, sid: sid, aid: aid, peer: peer}
}

// appendCrashState 造"崩溃态"事件流：
//
//	① gate.joined（K11 可见性的前提：收件方要是该 gate 的活跃成员）；
//	② standby 的 tool.call.intent（**没有** completed —— 进程崩在等待中）；
//	③ withMessage=true 时再落一条"中断期间到达"的消息（seq 在 intent 之后 →
//	   重放游标（原 intent 的 seq）能看见它）。
func (r *standbyResumeRig) appendCrashState(t *testing.T, withMessage bool) {
	t.Helper()
	ctx := context.Background()
	append := func(evs ...contracts.Event) {
		t.Helper()
		if _, err := r.store.Append(ctx, evs); err != nil {
			t.Fatalf("append 事件失败: %v", err)
		}
	}
	gateID := standbyResumeGate
	joined, err := json.Marshal(contracts.GateJoinedPayload{
		GateID: gateID,
		Source: contracts.GateJoinSourceExplicit,
	})
	if err != nil {
		t.Fatalf("序列化 gate.joined: %v", err)
	}
	append(contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: discovery.KindGateJoined, TS: "2026-01-01T00:00:00Z",
		SessionID: r.sid, AgentID: r.aid, Durable: true, Payload: joined,
	})

	intentRaw, err := json.Marshal(contracts.ToolCallIntentPayload{
		ToolCallID: contracts.ProviderId(standbyResumeCallID),
		ToolName:   ToolStandby,
		Arguments:  json.RawMessage(`{"wait":"message","timeout_min":1,"on_timeout":"return"}`),
		Idempotent: true,
		Replay:     contracts.ReplayKindSafe,
	})
	if err != nil {
		t.Fatalf("序列化 intent: %v", err)
	}
	append(contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: loop.KindToolCallIntent, TS: "2026-01-01T00:00:01Z",
		SessionID: r.sid, AgentID: r.aid, Durable: true, Payload: intentRaw,
	})

	if !withMessage {
		return
	}
	msgRaw, err := json.Marshal(contracts.AgentLinkBroadcastPayload{
		Target: contracts.AgentLinkTarget{
			Type:   contracts.AgentLinkTargetTypeGate,
			GateID: &gateID,
		},
		Text: standbyResumeMsg,
	})
	if err != nil {
		t.Fatalf("序列化广播: %v", err)
	}
	append(contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: agentlink.KindBroadcast, TS: "2026-01-01T00:00:02Z",
		SessionID: r.peer, AgentID: contracts.Ulid(core.NewUlid()), Durable: true, Payload: msgRaw,
	})
}

// resume 跑一次 durable.Resume（Replayer = 那个带 standby 的 loop）。
func (r *standbyResumeRig) resume(t *testing.T) durable.ResumeReport {
	t.Helper()
	rep, err := durable.Resume(context.Background(), r.store, r.sid, durable.ResumeOptions{Replayer: r.lp})
	if err != nil {
		t.Fatalf("durable.Resume: %v", err)
	}
	return rep
}

// toolCallResults 取出本 session 的三段事件（intent / completed / failed）。
func (r *standbyResumeRig) toolCallResults(t *testing.T) (completed, failed []contracts.Event) {
	t.Helper()
	evs, err := r.store.ReadFrom(context.Background(), &r.sid, 1, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	for _, ev := range evs {
		switch ev.Kind {
		case loop.KindToolCallCompleted:
			completed = append(completed, ev)
		case loop.KindToolCallFailed:
			failed = append(failed, ev)
		}
	}
	return completed, failed
}

// completedTextOf 取一条 completed 的正文。
func completedTextOf(t *testing.T, ev contracts.Event) string {
	t.Helper()
	var p contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("解 completed 载荷失败: %v", err)
	}
	var b strings.Builder
	for _, part := range p.Result.Content {
		if part.Text != nil {
			b.WriteString(*part.Text)
		}
	}
	return b.String()
}

// dispositionOf 取某 tool_call_id 的处置结论。
func dispositionOf(t *testing.T, rep durable.ResumeReport, id contracts.ProviderId) string {
	t.Helper()
	for _, d := range rep.Dispositions {
		if d.ToolCallID == id {
			return string(d.Disposition)
		}
	}
	t.Fatalf("报告里没有 %s 的处置结论：%+v", id, rep.Dispositions)
	return ""
}

// ---------------------------------------------------------------------------
// ① 消息已到 → 立即收口（如实收口）
// ---------------------------------------------------------------------------

// TestStandbyResumeMessageArrivedClosesAsCompleted：崩溃态 + 消息**已到** →
// resume 后那个 in-flight 被收口成 completed（带消息摘要），处置结论 replayed。
//
// 关键点：收口走的是**重放**（Replay=safe）——重放带**原 tool_call_id** 执行同一个执行器，
// 执行器回查事件流里那条原 intent → 重放游标 = 原 intent 的 seq → 中断期间到达的
// 那条消息（seq 更大）**看得见** → 立刻醒 → 如实收口。
func TestStandbyResumeMessageArrivedClosesAsCompleted(t *testing.T) {
	rig := newStandbyResumeRig(t)
	rig.appendCrashState(t, true) // 消息已到

	rep := rig.resume(t)

	if got := dispositionOf(t, rep, contracts.ProviderId(standbyResumeCallID)); got != string(contracts.ResumeDispositionReplayed) {
		t.Fatalf("处置结论 = %q，want replayed（重放成功即已收口）", got)
	}
	completed, failed := rig.toolCallResults(t)
	if len(failed) != 0 {
		t.Fatalf("不该有 tool.call.failed（成功收口），得到 %d 条", len(failed))
	}
	if len(completed) != 1 {
		t.Fatalf("tool.call.completed 应恰好 1 条，得到 %d", len(completed))
	}
	// X1 配对：收口的 completed 与原 intent **同一个 tool_call_id**。
	var p contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(completed[0].Payload, &p); err != nil {
		t.Fatalf("解 completed 载荷失败: %v", err)
	}
	if string(p.ToolCallID) != standbyResumeCallID {
		t.Fatalf("completed 的 tool_call_id = %q，want %q（X1：与原 intent 同 id）",
			p.ToolCallID, standbyResumeCallID)
	}
	// 如实收口：completed 带消息摘要（发送方 + 正文）。
	text := completedTextOf(t, completed[0])
	if !strings.Contains(text, standbyResumeMsg) {
		t.Fatalf("completed 正文应带消息摘要，得到 %q", text)
	}
	if !strings.Contains(text, string(rig.peer)) {
		t.Fatalf("completed 正文应带发送方 session，得到 %q", text)
	}
}

// ---------------------------------------------------------------------------
// ② 消息未到 → 重新挂起 → 到点收口
// ---------------------------------------------------------------------------

// TestStandbyResumeMessageMissingRehangsThenTimesOut：崩溃态 + 消息**未到** →
// resume 后**重新挂起**（waiting 态亮起 = 真的在等），到点如实收口（"未等到"）。
//
// 这里的"重新挂起"是 standby 的重放语义（Replay=safe = 重放 = 重新执行 = 重新挂起）：
// 执行器拿原 tool_call_id 回查原 intent → 归属 session 与游标都对 → 重新进入等待。
func TestStandbyResumeMessageMissingRehangsThenTimesOut(t *testing.T) {
	// 时间口径缩到毫秒级（超时路径要能在测试里跑完）。
	standbyTimeKnobs(t, 400*time.Millisecond, 5*time.Second, 20*time.Millisecond)
	rig := newStandbyResumeRig(t)
	rig.appendCrashState(t, false) // 消息未到

	type outcome struct {
		rep durable.ResumeReport
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		rep, err := durable.Resume(context.Background(), rig.store, rig.sid,
			durable.ResumeOptions{Replayer: rig.lp})
		done <- outcome{rep: rep, err: err}
	}()

	// 重新挂起：waiting 态亮起（BeginWait/EndWait 配平）。
	deadline := time.Now().Add(3 * time.Second)
	sawWaiting := false
	for time.Now().Before(deadline) {
		if row, ok := rig.disc.Session(rig.sid); ok && row.Waiting {
			sawWaiting = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !sawWaiting {
		t.Fatal("resume 后 standby 没有重新挂起（waiting 态没亮）")
	}

	var res outcome
	select {
	case res = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("resume 挂死了（既没等到消息也没超时收口）")
	}
	if res.err != nil {
		t.Fatalf("durable.Resume: %v", res.err)
	}
	if got := dispositionOf(t, res.rep, contracts.ProviderId(standbyResumeCallID)); got != string(contracts.ResumeDispositionReplayed) {
		t.Fatalf("处置结论 = %q，want replayed（重新挂起后按超时收口 = 已收口）", got)
	}
	completed, failed := rig.toolCallResults(t)
	if len(failed) != 0 {
		t.Fatalf("不该有 tool.call.failed，得到 %d 条", len(failed))
	}
	if len(completed) != 1 {
		t.Fatalf("tool.call.completed 应恰好 1 条，得到 %d", len(completed))
	}
	var p contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(completed[0].Payload, &p); err != nil {
		t.Fatalf("解 completed 载荷失败: %v", err)
	}
	if string(p.ToolCallID) != standbyResumeCallID {
		t.Fatalf("completed 的 tool_call_id = %q，want %q（X1：与原 intent 同 id）",
			p.ToolCallID, standbyResumeCallID)
	}
	text := completedTextOf(t, completed[0])
	if !strings.Contains(text, "未等到") {
		t.Fatalf("completed 正文应如实说没等到，得到 %q", text)
	}
	// waiting 态配平（收口后回落）。
	if row, ok := rig.disc.Session(rig.sid); ok && row.Waiting {
		t.Fatalf("收口后 waiting 该回落：%+v", row)
	}
}
