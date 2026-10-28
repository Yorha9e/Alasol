package durable

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// Op 是一次 in-flight 操作的**完整解析**：契约引用记录（Ref，checkpoint 里存的那份）加上
// 从事件流按 tool_call_id 回查到的原始 intent（重放所需）。 durable 包内部用它驱动 Replayer /
// Verifier；对外（checkpoint 事件、report）只暴露 Ref。
type Op struct {
	// Ref 是契约引用记录（tool_call_id / tool_name / replay / anchors）。
	Ref contracts.InFlightOp
	// Intent 是最初那条 tool.call.intent 的载荷（含 arguments / idempotent / anchors / policy_snapshot）。
	Intent contracts.ToolCallIntentPayload
	// IntentEventID 是 intent 事件的 Event.ID（重放落 result 时作 causation 指针，F3）。
	IntentEventID contracts.Ulid
	// AgentID 是该调用归属的 agent（事件流里 intent 事件的 agent_id）。
	AgentID contracts.Ulid
	// SessionID 是该调用归属的 session（补 completed 等结果事件时用）。
	SessionID contracts.Ulid
}

// Create 收集四项 checkpoint 内容（X3）并落一条 checkpoint.created durable 事件，
// 返回落盘的 payload（含分配到的 seq 水位由 store 决定，payload 本身即四项快照）。
//
// 四项：
//
//	① seq_watermark = 本 session 事件的最大 seq（事实流 tip）；
//	② ledger_seq    = 最后一条 ledger.updated 的 seq（0 = 无）；
//	③ in_flight     = 扫全流找出「有 intent 无 result」的操作（X1）；
//	④ tree_version  = 最后一条 agent.* 事件的 seq（0 = 无）。
func Create(ctx context.Context, store core.Store, sessionID contracts.Ulid) (contracts.CheckpointCreatedPayload, error) {
	if err := ctx.Err(); err != nil {
		return contracts.CheckpointCreatedPayload{}, err
	}
	events, err := readSession(ctx, store, sessionID)
	if err != nil {
		return contracts.CheckpointCreatedPayload{}, err
	}
	return createFromEvents(ctx, store, sessionID, events)
}

// createFromEvents 是 Create 的主体（events 已读好）；拆出来便于测试复用同一次读。
func createFromEvents(ctx context.Context, store core.Store, sessionID contracts.Ulid, events []contracts.Event) (contracts.CheckpointCreatedPayload, error) {
	payload := contracts.CheckpointCreatedPayload{
		SessionID:    sessionID,
		SeqWatermark: maxSeq(events),
		LedgerSeq:    lastKindSeq(events, KindLedgerUpdated),
		InFlight:     findInFlightRefs(events),
		TreeVersion:  lastAgentSeq(events),
	}
	if payload.InFlight == nil {
		payload.InFlight = []contracts.InFlightOp{} // required 字段：空清单落 [] 而非 null
	}

	agentID := PrimaryAgentID(events)
	ev, err := newDurableEvent(sessionID, agentID, KindCheckpointCreated, nil, payload)
	if err != nil {
		return contracts.CheckpointCreatedPayload{}, err
	}
	stored, err := store.Append(ctx, []contracts.Event{ev})
	if err != nil {
		return contracts.CheckpointCreatedPayload{}, fmt.Errorf("durable: 落 checkpoint.created 事件: %w", err)
	}
	_ = stored // payload 已定型；checkpoint 事件的 seq 由 store 分配，无需回填进 payload
	return payload, nil
}

// FindInFlight 扫 session 事件流，返回「有 tool.call.intent 而无对应 completed/failed」的操作引用。
//
// fromSeq 是扫描下界（只读 seq >= fromSeq 的事件）。**正确配对必须从 1 扫起**：一个 intent 的
// result 可能落在任何更晚的 seq，若 fromSeq 落在 intent 之前、result 之后，会漏判（把已完成的
// 当成 in-flight）或把 intent 之前的窗口外的 in-flight 漏掉。Checkpoint.Create 因此用 fromSeq=1。
//
// 配对键是 provider 黑盒 tool_call_id（S4：一次调用一个 id，原样往返）。
func FindInFlight(ctx context.Context, store core.Store, sessionID contracts.Ulid, fromSeq int64) ([]contracts.InFlightOp, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	events, err := readSession(ctx, store, sessionID)
	if err != nil {
		return nil, err
	}
	refs := findInFlightRefs(filterFromSeq(events, fromSeq))
	if refs == nil {
		refs = []contracts.InFlightOp{}
	}
	return refs, nil
}

// scanInFlight 是 FindInFlight 的「完整解析」版：返回 []Op（含原始 intent 供重放/核验）。
func scanInFlight(events []contracts.Event) []Op {
	return scanInFlightFrom(events, 1)
}

// scanInFlightFrom 从 seq>=fromSeq 的事件窗口扫 in-flight，返回完整 Op（按 intent 出现序）。
func scanInFlightFrom(events []contracts.Event, fromSeq int64) []Op {
	type entry struct {
		op       Op
		resolved bool
	}
	// 配对：tool_call_id → entry。同一个 id 只会有一个未决 intent（provider 保证一次调用一个 id）。
	byID := make(map[contracts.ProviderId]*entry)
	var order []contracts.ProviderId
	for i := range events {
		ev := &events[i]
		if ev.Seq == nil || *ev.Seq < fromSeq {
			continue
		}
		switch ev.Kind {
		case KindToolCallIntent:
			var p contracts.ToolCallIntentPayload
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue // 宽容（D7）：坏载荷跳过，不影响其余配对
			}
			if _, seen := byID[p.ToolCallID]; !seen {
				byID[p.ToolCallID] = &entry{op: Op{
					Ref: contracts.InFlightOp{
						ToolCallID: p.ToolCallID,
						ToolName:   p.ToolName,
						Replay:     normalizeReplay(p.Replay),
						Anchors:    p.Anchors,
					},
					Intent:        p,
					IntentEventID: ev.ID,
					AgentID:       ev.AgentID,
					SessionID:     ev.SessionID,
				}}
				order = append(order, p.ToolCallID)
			}
		case KindToolCallCompleted, KindToolCallFailed:
			var p struct {
				ToolCallID contracts.ProviderId `json:"tool_call_id"`
			}
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue
			}
			if e, ok := byID[p.ToolCallID]; ok {
				e.resolved = true // 有结果 → 不再是 in-flight
			}
		}
	}
	var out []Op
	for _, id := range order {
		if e := byID[id]; e != nil && !e.resolved {
			out = append(out, e.op)
		}
	}
	return out
}

// findInFlightRefs 扫全流（fromSeq=1）返回 in-flight 引用（checkpoint / FindInFlight 用）。
func findInFlightRefs(events []contracts.Event) []contracts.InFlightOp {
	ops := scanInFlight(events)
	if len(ops) == 0 {
		return nil
	}
	refs := make([]contracts.InFlightOp, len(ops))
	for i := range ops {
		refs[i] = ops[i].Ref
	}
	return refs
}

// normalizeReplay 归一化重放语义：空/未知 → unsafe（X2：未知结果不盲目重放，fail-safe）。
func normalizeReplay(r contracts.ReplayKind) contracts.ReplayKind {
	switch r {
	case contracts.ReplayKindSafe, contracts.ReplayKindVerify:
		return r
	default:
		return contracts.ReplayKindUnsafe
	}
}

// filterFromSeq 返回 seq >= fromSeq 的事件子集（保持原顺序）。
func filterFromSeq(events []contracts.Event, fromSeq int64) []contracts.Event {
	if fromSeq <= 1 {
		return events
	}
	var out []contracts.Event
	for i := range events {
		if events[i].Seq != nil && *events[i].Seq >= fromSeq {
			out = append(out, events[i])
		}
	}
	return out
}

// maxSeq 返回事件切片里的最大 seq（空 → 0）。
func maxSeq(events []contracts.Event) int64 {
	var m int64
	for i := range events {
		if events[i].Seq != nil && *events[i].Seq > m {
			m = *events[i].Seq
		}
	}
	return m
}

// lastKindSeq 返回某 kind 最后一条事件的 seq（无 → 0）。
func lastKindSeq(events []contracts.Event, kind string) int64 {
	var last int64
	for i := range events {
		if events[i].Kind == kind && events[i].Seq != nil {
			last = *events[i].Seq
		}
	}
	return last
}

// lastAgentSeq 返回最后一条 agent.* 事件的 seq（树投影版本，X3④；无 → 0）。
func lastAgentSeq(events []contracts.Event) int64 {
	var last int64
	for i := range events {
		switch events[i].Kind {
		case KindAgentSpawned, KindAgentCompleted, KindAgentFailed:
			if events[i].Seq != nil {
				last = *events[i].Seq
			}
		}
	}
	return last
}
