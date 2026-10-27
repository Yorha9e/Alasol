package durable

import (
	"context"
	"fmt"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// LedgerWriter 落地 X6 的 ledger 写放大控制：区分**关键字段立即落盘**与**过程字段 turn 末合并**。
//
// 背景（X6 / G8 / G23）：ledger 不是独立可变存储，当前态 = 对 ledger.updated 事件的 LWW 投影，
// 事件流是唯一事实源。每条 ledger.updated 仍是**全文快照**（goals / constraints / completed /
// pending 四张清单一次落全）；X6 不改变这个「全量快照」语义，只控制**写频率**：
//
//   - UpdateCritical（目标 / 约束变更）→ **立即**落一条 ledger.updated（崩溃不丢关键字段）；
//   - UpdateProcedural（已完成 / 待解变更）→ 只改内存缓冲，**不落盘**；同一 turn 内多次更新
//     被合并，直到 Flush（turn 末）落**一条** ledger.updated。
//
// 进程中途崩溃时，未 Flush 的过程性变更会丢（它们不是关键字段，可从中断的 turn 重算/重报）——
// 这正是 X6 的取舍：关键字段不丢，过程字段以更低写频率为代价。
//
// LedgerWriter 经 core.Store 直接 append（与 Checkpoint / Resume 同口径，Kafka 模式）。
// 非并发安全：一个 session 的一个 turn 内由单写者使用（G25 session 亲和写）。
type LedgerWriter struct {
	store     core.Store
	sessionID contracts.Ulid
	agentID   contracts.Ulid

	// 当前全量快照（LWW 基线）。
	goals       []string
	constraints []string
	completed   []string
	pending     []string

	// dirty 标记有未 Flush 的过程性变更。
	dirty bool
}

// NewLedgerWriter 构造一个绑定到 (session, agent) 的 ledger 写入器。
func NewLedgerWriter(store core.Store, sessionID, agentID contracts.Ulid) *LedgerWriter {
	return &LedgerWriter{
		store:     store,
		sessionID: sessionID,
		agentID:   agentID,
	}
}

// UpdateCritical 更新关键字段（目标 / 约束）并**立即**落一条 ledger.updated（X6）。
//
// 立即落盘时会把当前缓冲的过程性变更一并带上（快照保持四张清单一致），并清空 dirty。
func (w *LedgerWriter) UpdateCritical(ctx context.Context, goals, constraints []string) error {
	w.goals = cloneStrings(goals)
	w.constraints = cloneStrings(constraints)
	return w.appendSnapshot(ctx, "critical")
}

// UpdateProcedural 更新过程字段（已完成 / 待解），**只改内存缓冲、不落盘**（X6 turn 末合并）。
func (w *LedgerWriter) UpdateProcedural(completed, pending []string) {
	w.completed = cloneStrings(completed)
	w.pending = cloneStrings(pending)
	w.dirty = true
}

// Flush 在 turn 末把缓冲的过程性变更合并落**一条** ledger.updated；无未决变更时什么都不做。
func (w *LedgerWriter) Flush(ctx context.Context) error {
	if !w.dirty {
		return nil
	}
	if err := w.appendSnapshot(ctx, "procedural"); err != nil {
		return err
	}
	return nil
}

// appendSnapshot 落一条 ledger.updated 全文快照，并清 dirty。
func (w *LedgerWriter) appendSnapshot(ctx context.Context, note string) error {
	if w.store == nil {
		return fmt.Errorf("durable: LedgerWriter 未绑定 store")
	}
	payload := contracts.LedgerUpdatedPayload{
		Goals:       nonNilStrings(w.goals),
		Constraints: nonNilStrings(w.constraints),
		Completed:   nonNilStrings(w.completed),
		Pending:     nonNilStrings(w.pending),
		Note:        &note,
	}
	ev, err := newDurableEvent(w.sessionID, w.agentID, KindLedgerUpdated, nil, payload)
	if err != nil {
		return err
	}
	if _, err := w.store.Append(ctx, []contracts.Event{ev}); err != nil {
		return fmt.Errorf("durable: 落 ledger.updated(%s): %w", note, err)
	}
	w.dirty = false
	return nil
}

// Snapshot 返回当前内存里的全量快照（未 Flush 的过程性变更也在内）——供调用方观测/测试。
func (w *LedgerWriter) Snapshot() contracts.LedgerUpdatedPayload {
	return contracts.LedgerUpdatedPayload{
		Goals:       cloneStrings(w.goals),
		Constraints: cloneStrings(w.constraints),
		Completed:   cloneStrings(w.completed),
		Pending:     cloneStrings(w.pending),
	}
}

// cloneStrings 复制字符串切片（nil → nil），防调用方随后改写原切片改到内部状态。
func cloneStrings(s []string) []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s...)
}

// nonNilStrings 把 nil 规范成空切片（ledger.updated 的四张清单 required，落 [] 而非 null）。
func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
