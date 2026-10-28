package durable

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// ReplayResult 是重放的结果（区分「成功完成」与「执行了但失败」）。
//
// 区分是必须的：两者都**落了结果事件**（completed / failed），op 都不再是 in-flight；
// 但它们对人的意义完全不同——成功=已搞定，失败=**需人工重做**。
type ReplayResult struct {
	// OK = 重放成功（工具成功执行，结果事件已落 completed）。
	OK bool
	// FailureCode = 失败分类码（OK=false 时有值；对应 tool.call.failed 的 error.code）。
	FailureCode string
	// FailureMessage = 失败说明（OK=false 时有值）。
	FailureMessage string
}

// Replayer 重放一个 in-flight 操作（X2 第一类 safe，以及 verify 核验不通过后的重放）。
//
// 实现方（loop / host / CLI）持有 registry + 执行器 + bus，负责**真正再次执行工具并落
// completed / failed 结果事件**（causation 用 op.IntentEventID）。durable 只做决策与编排，
// 不自己执行工具（core 不依赖 registry / tool 执行器）。
//
// 返回值语义：
//   - (ReplayResult{OK: true}, nil)：重放**成功**（结果事件已落 completed）；
//   - (ReplayResult{OK: false}, nil)：重放执行了但**失败**（结果事件已落 failed，
//     失败码/说明带在结果里）→ 处置结论 replayed_failed（需人工重做）；
//   - (_, err != nil)：**写路径断裂**（结果事件没落成 / ctx 取消）→ Resume 中止并返回
//     该错误（op 仍未落结果 → 下次 Resume 可重试，幂等）。
type Replayer interface {
	Replay(ctx context.Context, op Op) (ReplayResult, error)
}

// ResumeOptions 配置一次 Resume。
type ResumeOptions struct {
	// Verifier 核验 replay=verify 操作的外部状态（X2 第二类）。nil = 无法核验 → 该类操作
	// fail-safe 转 manual（不盲目重放）。
	Verifier Verifier
	// Replayer 重放 replay=safe（及 verify 不通过）的操作。nil = 无法重放 → 该类操作转 manual。
	Replayer Replayer
	// ActingAgentID 是 session.resumed 事件的 agent_id（envelope 必需）。空 = 推导主 agent。
	ActingAgentID contracts.Ulid
	// ForceResume 接管"未释放的旧认领"（上一次 resume 崩溃在处置中途）。
	//
	// 默认 false：遇到未释放的认领 → 报「已有 resume 在进行」并退出（不重放，
	// 免得同一个 in-flight 被处置两次）。确认上一次已崩溃时才显式打开它。
	ForceResume bool
}

// ResumeReport 是一次 Resume 的产出（供 CLI 打印 / 调用方接手续跑）。
//
// JSON tag 统一 snake_case（与契约一致），便于 CLI / 跨端消费整份报告。
type ResumeReport struct {
	SessionID     contracts.Ulid `json:"session_id"`
	HadCheckpoint bool           `json:"had_checkpoint"`
	// FromCheckpointSeq = 最后一个 checkpoint 的 seq 水位；0 = 无 checkpoint（全量重建）。
	FromCheckpointSeq int64 `json:"from_checkpoint_seq"`
	// SeqWatermark = fold 到的水位（有 checkpoint 取其 seq_watermark；否则取当前事实流 tip）。
	SeqWatermark int64 `json:"seq_watermark"`
	// Ledger / LedgerSeq = fold 出的 ledger 当前态及其版本（末条 ledger.updated 的 seq）。
	Ledger    contracts.LedgerUpdatedPayload `json:"ledger"`
	LedgerSeq int64                          `json:"ledger_seq"`
	// Tree / TreeVersion = fold 出的 agent 树投影及其版本（末条 agent.* 的 seq）。
	Tree        *RestoredTree `json:"tree"`
	TreeVersion int64         `json:"tree_version"`
	// InFlight = 本次发现的全部 in-flight 操作引用（含往届已处置、这次跳过的）。
	InFlight []contracts.InFlightOp `json:"in_flight"`
	// Dispositions = 本次**实际**处置的明细（幂等：往届已处置的不在此列）。
	Dispositions []contracts.InFlightDisposition `json:"dispositions"`
	// Ownership = fold 出的 spawn 归属记录（X5；(parent, request_id) → child），供 CLI/调用方观测。
	Ownership []Ownership `json:"ownership"`
	// ResumedEventID = 落盘的 session.resumed 事件 id。
	ResumedEventID contracts.Ulid `json:"resumed_event_id"`
}

// Resume 执行 X4 的恢复流程：读最后一个 checkpoint → fold 事件重建状态 → 处置 in-flight →
// 落 session.resumed。返回 ResumeReport。
//
// **无 checkpoint 的 session**：选**全量重建**（而非报错）——fold 整个事件流、从 seq 1 扫
// in-flight、from_checkpoint_seq=0。这直接支撑 kill -9 场景（crash 前可能还没建过 checkpoint）。
//
// **幂等**：连跑两次不重复处置。replayed / verified 的 op 首次已落结果事件 → 二次扫不到；
// manual（unsafe）的 op 不落结果，但首次的 session.resumed 已把它记进「已处置集合」→ 二次跳过。
func Resume(ctx context.Context, store core.Store, sessionID contracts.Ulid, opts ResumeOptions) (ResumeReport, error) {
	if err := ctx.Err(); err != nil {
		return ResumeReport{}, err
	}
	events, err := readSession(ctx, store, sessionID)
	if err != nil {
		return ResumeReport{}, err
	}

	report := ResumeReport{SessionID: sessionID}

	// ① 读最后一个 checkpoint（无 → 全量重建）。
	var cp *contracts.CheckpointCreatedPayload
	for i := range events {
		if events[i].Kind != KindCheckpointCreated {
			continue
		}
		var p contracts.CheckpointCreatedPayload
		if json.Unmarshal(events[i].Payload, &p) != nil {
			continue // 宽容（D7）
		}
		cp = &p
	}
	if cp != nil {
		report.HadCheckpoint = true
		report.FromCheckpointSeq = cp.SeqWatermark
		report.SeqWatermark = cp.SeqWatermark
	} else {
		report.SeqWatermark = maxSeq(events)
	}

	// ② fold 事件重建状态：ledger（LWW，语义同 runtime.FoldLedger）+ agent 树 / ownership。
	report.Ledger = foldLedger(events)
	report.LedgerSeq = lastKindSeq(events, KindLedgerUpdated)
	report.Tree = foldTree(events)
	report.TreeVersion = lastAgentSeq(events)
	report.Ownership = report.Tree.Ownership()

	// in-flight：扫全流（fromSeq=1，正确配对）拿完整 Op；report 里放引用。
	ops := scanInFlight(events)
	report.InFlight = make([]contracts.InFlightOp, 0, len(ops))
	for i := range ops {
		report.InFlight = append(report.InFlight, ops[i].Ref)
	}

	// 幂等：fold 往届 session.resumed，收集已处置的 tool_call_id。
	disposed := alreadyDisposed(events)

	// ②' **认领**（并发互斥）：处置 in-flight 之前先原子认领本 session 的 resume 权。
	//
	// 拿不到 → 直接退出（不进处置循环）：两个 resume 并发会把同一个 in-flight 重放两次
	//（verify 类工具有副作用）。顺序幂等挡不住这个——两个 resume 都在"已处置集合"
	// 落盘之前扫到同一个 in-flight。
	if _, err := claimResume(ctx, store, sessionID, events, opts.ForceResume); err != nil {
		return report, err
	}

	// ③ 逐个处置 in-flight（X2 三类）。
	report.Dispositions = make([]contracts.InFlightDisposition, 0, len(ops))
	for i := range ops {
		op := ops[i]
		if disposed[op.Ref.ToolCallID] {
			continue // 往届已处置 → 跳过（幂等）
		}
		disp, detail, err := disposeOne(ctx, store, opts, op)
		if err != nil {
			// 重放/落盘失败：中止（不写 session.resumed，下次可重试；已成功的 op 已落结果，
			// 天然不会被二次重放）。manual 的过程性说明若在失败点之前未落盘会重报——v0 已知取舍。
			report.Dispositions = append(report.Dispositions,
				contracts.InFlightDisposition{ToolCallID: op.Ref.ToolCallID, Disposition: contracts.ResumeDispositionAborted, Detail: strPtr(err.Error())})
			return report, err
		}
		report.Dispositions = append(report.Dispositions,
			contracts.InFlightDisposition{ToolCallID: op.Ref.ToolCallID, Disposition: disp, Detail: detail})
	}

	// ⑤ 落 session.resumed（④ 树已 fold 进 report.Tree；真正续跑 turn 由调用方 loop 接手）。
	agentID := opts.ActingAgentID
	if agentID == "" {
		agentID = PrimaryAgentID(events)
	}
	resumedEv, err := newDurableEvent(sessionID, agentID, KindSessionResumed, nil, contracts.SessionResumedPayload{
		SessionID:           sessionID,
		FromCheckpointSeq:   report.FromCheckpointSeq,
		InFlightDisposition: report.Dispositions,
	})
	if err != nil {
		return report, err
	}
	stored, err := store.Append(ctx, []contracts.Event{resumedEv})
	if err != nil {
		return report, fmt.Errorf("durable: 落 session.resumed 事件: %w", err)
	}
	if len(stored) > 0 {
		report.ResumedEventID = stored[0].ID
	}
	return report, nil
}

// disposeOne 处置单个 in-flight 操作，返回 (disposition, detail, error)。
// error 非 nil 仅表示「需要执行却执行失败」（safe 重放 / verify 不通过后重放）→ 上层中止。
//
// 重放**成功**与重放**失败**是两种结论（P2）：成功 = replayed（已搞定）；失败 =
// replayed_failed（工具侧失败，**需人工重做**）——不能都记成 replayed（显示像成功）。
func disposeOne(ctx context.Context, store core.Store, opts ResumeOptions, op Op) (contracts.ResumeDisposition, *string, error) {
	switch op.Ref.Replay {
	case contracts.ReplayKindSafe:
		// 第一类：自动重放（重复执行无副作用）。
		if opts.Replayer == nil {
			return manualNoReplayer(op)
		}
		res, err := opts.Replayer.Replay(ctx, op)
		if err != nil {
			return "", nil, fmt.Errorf("durable: 重放 %s(%s) 失败: %w", op.Ref.ToolName, op.Ref.ToolCallID, err)
		}
		if !res.OK {
			return contracts.ResumeDispositionReplayedFailed, strPtr(replayFailedDetail("safe", res)), nil
		}
		return contracts.ResumeDispositionReplayed, strPtr("safe：已自动重放并落结果事件"), nil

	case contracts.ReplayKindVerify:
		// 第二类：先核验外部状态再决定。
		if opts.Verifier == nil {
			return contracts.ResumeDispositionManual, strPtr("verify：未配置 Verifier，无法核验外部状态，转人工/agent 判定"), nil
		}
		ok, verr := opts.Verifier.Verify(ctx, op.Ref)
		if verr != nil {
			// 核验过程出错 → 无法确认 → fail-safe 不盲目重放，转人工。
			return contracts.ResumeDispositionManual, strPtr("verify：核验出错（" + verr.Error() + "），无法确认外部状态，转人工/agent 判定"), nil
		}
		if ok {
			// 核验通过 = 外部事实表明已完成 → 补一条 completed（合成结果），不重放。
			if err := writeVerifiedCompleted(ctx, store, op); err != nil {
				return "", nil, err
			}
			return contracts.ResumeDispositionVerified, strPtr("verify：外部状态核验通过（已完成），未重放"), nil
		}
		// 核验不通过 = 未完成 → 重放。
		if opts.Replayer == nil {
			return manualNoReplayer(op)
		}
		res, err := opts.Replayer.Replay(ctx, op)
		if err != nil {
			return "", nil, fmt.Errorf("durable: verify 不通过后重放 %s(%s) 失败: %w", op.Ref.ToolName, op.Ref.ToolCallID, err)
		}
		if !res.OK {
			return contracts.ResumeDispositionReplayedFailed, strPtr(replayFailedDetail("verify", res)), nil
		}
		return contracts.ResumeDispositionReplayed, strPtr("verify：外部状态核验不通过（未完成），已重放"), nil

	default: // unsafe（含未知 → normalizeReplay 已归一为 unsafe）
		// 第三类：不重放（未知结果不可盲跑），转人工 / agent 判定。
		return contracts.ResumeDispositionManual,
			strPtr("unsafe：未知结果不盲目重放（X2），转人工/agent 判定"), nil
	}
}

// replayFailedDetail 组装"重放执行了但失败"的处置说明（带失败码，便于人工重做）。
func replayFailedDetail(kind string, res ReplayResult) string {
	msg := res.FailureMessage
	if msg == "" {
		msg = "（无说明）"
	}
	code := res.FailureCode
	if code == "" {
		code = "unknown"
	}
	return fmt.Sprintf("%s：已自动重放但**失败**（code=%s）：%s —— 需人工重做", kind, code, msg)
}

// manualNoReplayer 在「本应重放却没有 Replayer」时转 manual（fail-safe：不能执行就不声称已完成）。
func manualNoReplayer(op Op) (contracts.ResumeDisposition, *string, error) {
	return contracts.ResumeDispositionManual,
		strPtr("未配置 Replayer，无法重放 " + op.Ref.ToolName + "，转人工/agent 判定"), nil
}

// writeVerifiedCompleted 为「核验通过」的 op 补一条 tool.call.completed（合成结果）。
//
// 我们**没有**真正的工具返回值（外部已完成，本进程没执行）——合成一个可读结果说明「已核验完成」，
// 让该 op 不再是 in-flight（下次 Resume 扫不到），从而幂等。causation 指向原 intent（F3）。
func writeVerifiedCompleted(ctx context.Context, store core.Store, op Op) error {
	ev, err := newDurableEvent(op.SessionID, op.AgentID, KindToolCallCompleted, &op.IntentEventID, contracts.ToolCallCompletedPayload{
		ToolCallID: op.Ref.ToolCallID,
		Result:     verifiedResult(op),
	})
	if err != nil {
		return err
	}
	if _, err := store.Append(ctx, []contracts.Event{ev}); err != nil {
		return fmt.Errorf("durable: 落 verify 通过的 completed 事件: %w", err)
	}
	return nil
}

// verifiedResult 合成「已核验完成」的工具结果（Content 一条可读文本）。
func verifiedResult(op Op) contracts.ToolResult {
	msg := "verify：外部状态核验通过，该调用已被确认为完成；未在本进程重放（X2）。"
	return contracts.ToolResult{
		Content: []contracts.ContentPart{{Type: "text", Text: &msg}},
	}
}

// foldLedger 从事件流 fold 出当前 ledger 态：最后一条 ledger.updated 的全量快照（LWW，G23/G8）。
// 语义同 runtime.FoldLedger（core 不 import runtime，C2，在此复刻）；坏载荷跳过（D7 宽容）。
func foldLedger(events []contracts.Event) contracts.LedgerUpdatedPayload {
	var out contracts.LedgerUpdatedPayload
	for i := range events {
		if events[i].Kind != KindLedgerUpdated {
			continue
		}
		var p contracts.LedgerUpdatedPayload
		if json.Unmarshal(events[i].Payload, &p) != nil {
			continue
		}
		out = p
	}
	return out
}

// alreadyDisposed fold 所有 session.resumed 事件，收集已处置的 tool_call_id 集合（幂等键）。
func alreadyDisposed(events []contracts.Event) map[contracts.ProviderId]bool {
	set := make(map[contracts.ProviderId]bool)
	for i := range events {
		if events[i].Kind != KindSessionResumed {
			continue
		}
		var p contracts.SessionResumedPayload
		if json.Unmarshal(events[i].Payload, &p) != nil {
			continue
		}
		for _, d := range p.InFlightDisposition {
			set[d.ToolCallID] = true
		}
	}
	return set
}

// strPtr 取字符串地址（detail 可选字段）。
func strPtr(s string) *string { return &s }
