package host

// G-j 事件面的**早到窗口**与候选上限（critic P1/P2 的钉子）。
//
// # 两面"早到"的定义不同（v1）
//
//	消息面：早到 = **外部事实**（对端的回话）**还没被交付过** → 早于本轮也该捞到
//	（"B 的回话在 A 决定等之前就到了"正是消息面的核心价值）；
//	事件面：早到 = **本轮内**到达的事件 → 窗口 = (本轮 model.turn 的 seq,
//	本次 intent 的 seq]。上一轮自己的 model.turn / tool.call.* / 旧
//	agent.completed 是**历史**，不是"早到"（否则第二轮 standby 等 model.turn 会被
//	上一轮的 model.turn 秒唤醒）。
//
// # 本文件的用例
//
//	① 第二轮 standby 等 model.turn → **不**被上一轮的 model.turn 唤醒（真等）；
//	② 等 tool.call.* → 不被上一轮的 completed 唤醒；
//	③ 本轮内（model.turn 后、intent 前）到达的匹配事件**仍**算早到；
//	④ 自醒排除：等 tool.call.intent / tool.call.* → 本次 intent 不自醒；
//	⑤ 候选上限 32：>32 条匹配事件 → 交付最新、更早的下次仍有资格。

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// standbyEventRig 是事件面等待的**单元级**装置：事件流里按序落
//
//	[model.turn] → [n 条匹配事件] → [standby intent]
//
// （可插在中间的"匹配事件"就是"本轮内到达"的那些）。
type standbyEventRig struct {
	store *sqlite.Store
	ex    *standbyExecutor
	sid   contracts.Ulid
	aid   contracts.Ulid
	// intentSeq 是 standby intent 的 seq（= 游标起点 / 窗口上界）。
	intentSeq int64
	// modelTurnSeq 是那条 model.turn 的 seq（= 窗口下界）。
	modelTurnSeq int64
	// matchSeqs 是那些匹配事件的 seq（升序）。
	matchSeqs []int64
}

// newStandbyEventRig 造装置：落一条 model.turn + n 条 kind 匹配事件 + standby intent。
func newStandbyEventRig(t *testing.T, kind string, n int) *standbyEventRig {
	t.Helper()
	st, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"), sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ctx := context.Background()

	r := &standbyEventRig{
		store: st,
		sid:   contracts.Ulid(core.NewUlid()),
		aid:   contracts.Ulid(core.NewUlid()),
	}
	appendEv := func(evKind string, payload string) contracts.Event {
		t.Helper()
		stored, aerr := st.Append(ctx, []contracts.Event{{
			V: 1, ID: core.NewUlid(), Kind: evKind, TS: "2026-01-01T00:00:00Z",
			SessionID: r.sid, AgentID: r.aid, Durable: true,
			Payload: json.RawMessage(payload),
		}})
		if aerr != nil {
			t.Fatalf("Append(%s): %v", evKind, aerr)
		}
		return stored[0]
	}
	// ① 本轮的 model.turn（窗口下界）。
	mt := appendEv(loop.KindModelTurn, `{"text":"我去等"}`)
	r.modelTurnSeq = *mt.Seq
	// ② 本轮内到达的匹配事件（窗口内）。
	for i := 0; i < n; i++ {
		ev := appendEv(kind, `{"i":1}`)
		r.matchSeqs = append(r.matchSeqs, *ev.Seq)
	}
	// ③ standby 的 intent（窗口上界 = 游标起点）。
	intentRaw, _ := json.Marshal(contracts.ToolCallIntentPayload{
		ToolCallID: "toolu_EVENT_RIG",
		ToolName:   ToolStandby,
		Arguments:  json.RawMessage(`{"wait":"event","event_kind":"` + kind + `","timeout_min":1,"on_timeout":"return"}`),
	})
	stored, err := st.Append(ctx, []contracts.Event{{
		V: 1, ID: core.NewUlid(), Kind: loop.KindToolCallIntent, TS: "2026-01-01T00:00:00Z",
		SessionID: r.sid, AgentID: r.aid, Durable: true, Payload: intentRaw,
	}})
	if err != nil {
		t.Fatalf("Append(intent): %v", err)
	}
	r.intentSeq = *stored[0].Seq
	r.ex = newStandbyExecutor(standbyDeps{store: st})
	return r
}

// ref 返回本装置的 intent 定位结果。
func (r *standbyEventRig) ref() standbyIntentRef {
	return standbyIntentRef{SessionID: r.sid, AgentID: r.aid, Seq: r.intentSeq}
}

// ---------------------------------------------------------------------------
// ③ 本轮内到达的匹配事件仍算早到
// ---------------------------------------------------------------------------

// TestStandbyEventEarlyArrivedWithinTurn：本轮 model.turn **之后**、本次 intent
// **之前**到达的匹配事件 → **仍**算早到候选（窗口 = (model.turn, intent]）。
//
// 构造（单元级）：事件流按序落 model.turn → 匹配事件 → standby intent，
// 再调早到检查（窗口内的那条应被命中）。
func TestStandbyEventEarlyArrivedWithinTurn(t *testing.T) {
	r := newStandbyEventRig(t, "test.completed", 1)
	ctx := context.Background()
	found, hit, err := r.ex.checkArrived(ctx, r.ref(), standbyArgs{
		Wait:       standbyWaitEvent,
		EventKind:  "test.completed",
		EventScope: standbyScopeSelf,
	}, map[int64]bool{})
	if err != nil {
		t.Fatalf("checkArrived: %v", err)
	}
	if !found {
		t.Fatal("本轮内（model.turn 后、intent 前）到达的匹配事件该算早到")
	}
	if hit.Seq != r.matchSeqs[0] {
		t.Fatalf("早到候选 seq = %d，want %d", hit.Seq, r.matchSeqs[0])
	}
}

// ---------------------------------------------------------------------------
// ④ 自醒排除
// ---------------------------------------------------------------------------

// TestStandbyEventWaitDoesNotSelfWake：等 **tool.call.intent**（精确）与
// **tool.call.***（通配）→ 本次 intent **不自醒**（否则 standby 会自己叫醒自己）
// → 等满超时，如实收口「未等到」。
func TestStandbyEventWaitDoesNotSelfWake(t *testing.T) {
	for _, kind := range []string{"tool.call.intent", "tool.call.*"} {
		kind := kind
		t.Run(kind, func(t *testing.T) {
			standbyTimeKnobs(t, 150*time.Millisecond, 5*time.Second, 20*time.Millisecond)
			h := newStandbyTestHub(t, standbyEventSteps(
				`{"wait":"event","event_kind":"`+kind+`","timeout_min":1,"on_timeout":"return"}`))
			sid := NewSession()
			if _, err := h.RunPrompt(context.Background(), sid, "waiter"); err != nil {
				t.Fatalf("RunPrompt: %v", err)
			}
			text := standbyCompletedText(t, h, sid)
			if !strings.Contains(text, "未等到") {
				t.Fatalf("本次 intent 不该自醒（该等满超时收口），得到 %q", text)
			}
			if strings.Contains(text, "tool.call.intent") {
				t.Fatalf("completed 不该带本次 intent 自己的摘要，得到 %q", text)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ⑤ 候选上限 32
// ---------------------------------------------------------------------------

// TestStandbyEventCandidateCap：候选扫描**流式固定缓冲** → 候选条数恒 ≤ 32
// （内存 O(1) 候选，不是 O(匹配事件数)）；且**最新**的排在最前。
func TestStandbyEventCandidateCap(t *testing.T) {
	const total = 100
	r := newStandbyEventRig(t, "test.completed", total)
	ctx := context.Background()
	cands, _, err := r.ex.eventCandidates(ctx, r.ref(), standbyArgs{
		Wait:       standbyWaitEvent,
		EventKind:  "test.completed",
		EventScope: standbyScopeSelf,
	}, r.modelTurnSeq, r.intentSeq, map[int64]bool{})
	if err != nil {
		t.Fatalf("eventCandidates: %v", err)
	}
	if len(cands) > standbyEventCandidateCap {
		t.Fatalf("候选条数 = %d，want ≤ %d（流式固定缓冲）", len(cands), standbyEventCandidateCap)
	}
	if len(cands) == 0 {
		t.Fatal("候选不该为空")
	}
	if cands[0].Seq != r.matchSeqs[total-1] {
		t.Fatalf("最新那条该排最前：cands[0].seq=%d，want %d", cands[0].Seq, r.matchSeqs[total-1])
	}
}

// TestStandbyEventOlderCandidatesStillEligible：>32 条匹配事件时，交付**最新**那条；
// 更早的未交付事件**下次仍有资格**（逐条补交——交付集合只挡"已交付过的"）。
func TestStandbyEventOlderCandidatesStillEligible(t *testing.T) {
	const total = 40
	r := newStandbyEventRig(t, "test.completed", total)
	ctx := context.Background()
	delivered := map[int64]bool{}

	// 第一次：交付最新那条。
	cands, _, err := r.ex.eventCandidates(ctx, r.ref(), standbyArgs{
		Wait:       standbyWaitEvent,
		EventKind:  "test.completed",
		EventScope: standbyScopeSelf,
	}, r.modelTurnSeq, r.intentSeq, delivered)
	if err != nil {
		t.Fatalf("eventCandidates: %v", err)
	}
	if len(cands) == 0 || cands[0].Seq != r.matchSeqs[total-1] {
		t.Fatalf("该交付最新那条（seq=%d）", r.matchSeqs[total-1])
	}
	delivered[cands[0].Seq] = true

	// 第二次（已交付最新那条）：下一新的那条仍有资格。
	cands2, _, err := r.ex.eventCandidates(ctx, r.ref(), standbyArgs{
		Wait:       standbyWaitEvent,
		EventKind:  "test.completed",
		EventScope: standbyScopeSelf,
	}, r.modelTurnSeq, r.intentSeq, delivered)
	if err != nil {
		t.Fatalf("eventCandidates: %v", err)
	}
	if len(cands2) == 0 {
		t.Fatal("更早的未交付事件该仍有资格")
	}
	if cands2[0].Seq != r.matchSeqs[total-2] {
		t.Fatalf("下一条该是 seq=%d，得到 %d", r.matchSeqs[total-2], cands2[0].Seq)
	}
}
