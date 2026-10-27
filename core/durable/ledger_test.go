package durable_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/durable"
)

// TestLedgerWriterCriticalImmediateProceduralCoalesced（X6）：
// 关键字段 UpdateCritical 立即落一条；过程字段 UpdateProcedural 只缓冲、turn 末 Flush 合并为一条。
func TestLedgerWriterCriticalImmediateProceduralCoalesced(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	session := core.NewUlid()
	agent := core.NewUlid()
	w := durable.NewLedgerWriter(st, session, agent)

	// 关键字段：立即落 1 条。
	if err := w.UpdateCritical(ctx, []string{"g1"}, []string{"c1"}); err != nil {
		t.Fatalf("UpdateCritical: %v", err)
	}
	if n := st.countKind("ledger.updated"); n != 1 {
		t.Fatalf("UpdateCritical 后 ledger.updated = %d, want 1（关键字段立即落盘）", n)
	}

	// 过程字段：同一 turn 内多次更新，不落盘。
	w.UpdateProcedural([]string{"done-1"}, []string{"todo-1"})
	w.UpdateProcedural([]string{"done-1", "done-2"}, []string{"todo-2"})
	w.UpdateProcedural([]string{"done-1", "done-2", "done-3"}, []string{"todo-3"})
	if n := st.countKind("ledger.updated"); n != 1 {
		t.Fatalf("UpdateProcedural 不应落盘，ledger.updated = %d, want 1", n)
	}
	// 内存快照已含最新过程字段。
	snap := w.Snapshot()
	if len(snap.Completed) != 3 || len(snap.Pending) != 1 {
		t.Fatalf("过程字段缓冲不符: %+v", snap)
	}

	// turn 末 Flush：合并为 1 条（含关键字段，全文快照）。
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n := st.countKind("ledger.updated"); n != 2 {
		t.Fatalf("Flush 后 ledger.updated = %d, want 2（过程性合并为一条）", n)
	}
	// 再次 Flush 无变更 → 不落盘。
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush#2: %v", err)
	}
	if n := st.countKind("ledger.updated"); n != 2 {
		t.Fatalf("空 Flush 不应落盘，ledger.updated = %d, want 2", n)
	}

	// 落下的末条是全文快照（关键 + 过程字段都在）。
	led := foldLastLedger(t, st)
	if len(led.Goals) != 1 || led.Goals[0] != "g1" {
		t.Fatalf("末条 ledger goals = %+v", led.Goals)
	}
	if len(led.Completed) != 3 || len(led.Pending) != 1 {
		t.Fatalf("末条 ledger 过程字段 = %+v", led)
	}
}

// TestLedgerWriterCriticalFlushesBufferedProcedural：UpdateCritical 会把缓冲的过程字段一并带上。
func TestLedgerWriterCriticalFlushesBufferedProcedural(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	session := core.NewUlid()
	agent := core.NewUlid()
	w := durable.NewLedgerWriter(st, session, agent)

	w.UpdateProcedural([]string{"done-1"}, []string{"todo-1"}) // 缓冲，未落
	if err := w.UpdateCritical(ctx, []string{"g-new"}, []string{"c-new"}); err != nil {
		t.Fatalf("UpdateCritical: %v", err)
	}
	// 立即落的这一条应同时含关键与过程字段（快照一致）。
	led := foldLastLedger(t, st)
	if len(led.Goals) != 1 || led.Goals[0] != "g-new" {
		t.Fatalf("关键字段未更新: %+v", led.Goals)
	}
	if len(led.Completed) != 1 || led.Completed[0] != "done-1" {
		t.Fatalf("过程字段未随关键字段一并落盘: %+v", led.Completed)
	}
	// dirty 已清：再 Flush 不落盘。
	before := st.countKind("ledger.updated")
	_ = w.Flush(ctx)
	if st.countKind("ledger.updated") != before {
		t.Fatal("UpdateCritical 后不应再有未 flush 的过程字段")
	}
}

// foldLastLedger 从 store 读末条 ledger.updated 的快照（测试辅助）。
func foldLastLedger(t *testing.T, st *memStore) contracts.LedgerUpdatedPayload {
	t.Helper()
	var out contracts.LedgerUpdatedPayload
	for _, ev := range st.all() {
		if ev.Kind != "ledger.updated" {
			continue
		}
		var p contracts.LedgerUpdatedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("解 ledger payload: %v", err)
		}
		out = p
	}
	return out
}
