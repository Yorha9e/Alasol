package walfile_test

// 对账的**不误报**口径（G-f）：standby 待机中不算崩溃残留。
//
// blob.plan 是**每个**工具调用一条（写在 intent 之前，与结果体量无关——见
// core/loop/blob.go 的 planExternalize），而"孤儿 blob 候选"名单的含义是"结果**可能**
// 已写成 blob、引用没落"。standby 的结果恒为短文本 → 永不外置 → 它那条计划不可能
// 对应一份孤儿 blob；待机中（intent 有、completed 没有）更是**正在等**，不是崩溃残留。
//
// 故事实面给一个"已知在飞"集合（Facts.PendingToolCalls）：这些 id 照实列进
// Report.PendingToolCalls（不藏证据），但**不进**孤儿候选 → Crashed() 为假。
//
// 本文件同时钉住**不越界**：没被标成"已知在飞"的没收口调用**仍然**是孤儿候选
// （排除只能针对确实在飞的那些，别把崩溃信号一起消掉）。

import (
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/walfile"
)

// TestReconcilePendingToolCallIsNotOrphan：已知在飞的 tool_call（standby 待机中）→
// 不进孤儿候选、不算崩溃残留，但**照实列出来**。
func TestReconcilePendingToolCallIsNotOrphan(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
			recOf(core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
			recOf(core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 1, ToolCallID: "tc_WAITING"}),
		},
	}
	facts := walfile.Facts{
		CompletedToolCalls:  map[string]bool{},
		CompletedBinaryRefs: map[string]bool{},
		FailedToolCalls:     map[string]bool{},
		// 宿主折出来的事实：那个 standby 还在等（intent 有、completed 没有）。
		PendingToolCalls: map[string]bool{"tc_WAITING": true},
	}
	rep := walfile.Reconcile(scan, sid, facts)
	if len(rep.OrphanBlobToolCalls) != 0 {
		t.Fatalf("待机中的调用不该进孤儿候选，got %v", rep.OrphanBlobToolCalls)
	}
	if rep.Crashed() {
		t.Error("待机不是崩溃：Crashed() 应为 false")
	}
	if len(rep.PendingToolCalls) != 1 || rep.PendingToolCalls[0] != "tc_WAITING" {
		t.Fatalf("在飞工具调用 = %v，want [tc_WAITING]", rep.PendingToolCalls)
	}
	// 不藏证据：摘要里要看得见"还有一个没收口的调用"。
	if s := rep.Summary(); !strings.Contains(s, "tc_WAITING") {
		t.Errorf("摘要应点名在飞工具调用，got %q", s)
	}
}

// TestReconcilePendingDoesNotMaskRealOrphans：排除**只**针对确实在飞的那些——
// 没被标成"已知在飞"的没收口调用仍然是孤儿候选（崩溃信号不能被一起消掉）。
func TestReconcilePendingDoesNotMaskRealOrphans(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			recOf(core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 1, ToolCallID: "tc_WAITING"}),
			recOf(core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 1, ToolCallID: "tc_CRASHED"}),
		},
	}
	facts := walfile.Facts{
		CompletedToolCalls:  map[string]bool{},
		CompletedBinaryRefs: map[string]bool{},
		FailedToolCalls:     map[string]bool{},
		PendingToolCalls:    map[string]bool{"tc_WAITING": true},
	}
	rep := walfile.Reconcile(scan, sid, facts)
	if len(rep.OrphanBlobToolCalls) != 1 || rep.OrphanBlobToolCalls[0] != "tc_CRASHED" {
		t.Fatalf("孤儿候选 = %v，want [tc_CRASHED]（排除不能越界）", rep.OrphanBlobToolCalls)
	}
	if !rep.Crashed() {
		t.Error("真有没收口的调用 → Crashed() 应为 true")
	}
}

// TestReconcilePendingListedWithoutBlobPlan（critic F8）：**blob 关**（WAL 里没有
// blob.plan）而事实面说"有个未收口的调用" → 报告**照样要列出**它。
//
// 旧口径把列示挂在 blob.plan 的名单循环里 → blob 关（默认口径）时那个循环一次都
// 不跑 → 未收口的 standby 调用在报告里**一言不发**（藏证据：明明有个调用没收口，
// 报告却说"没有"）。
func TestReconcilePendingListedWithoutBlobPlan(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists:  true,
		Records: []core.WalRecord{recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"})},
	}
	facts := walfile.Facts{
		CompletedToolCalls:  map[string]bool{},
		CompletedBinaryRefs: map[string]bool{},
		FailedToolCalls:     map[string]bool{},
		PendingToolCalls:    map[string]bool{"tc_WAITING": true},
	}
	rep := walfile.Reconcile(scan, sid, facts)
	if len(rep.PendingToolCalls) != 1 || rep.PendingToolCalls[0] != "tc_WAITING" {
		t.Fatalf("未收口的调用 = %v，want [tc_WAITING]（blob 关也要列——不藏证据）", rep.PendingToolCalls)
	}
	// 不误报：它不是崩溃残留（没有孤儿候选）。
	if len(rep.OrphanBlobToolCalls) != 0 {
		t.Fatalf("未收口的 standby 调用不该进孤儿候选：%v", rep.OrphanBlobToolCalls)
	}
	// 摘要里也要看得见。
	if s := rep.Summary(); !strings.Contains(s, "tc_WAITING") {
		t.Errorf("摘要应点名未收口的调用，got %q", s)
	}
}

// TestReconcilePendingSkipsClosedCalls：事实面说了、但事件流里**已经收口**
// （completed / failed）→ 不该再列进"未收口"（报告不替宿主猜）。
func TestReconcilePendingSkipsClosedCalls(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{Exists: true}
	facts := walfile.Facts{
		CompletedToolCalls:  map[string]bool{"tc_DONE": true},
		CompletedBinaryRefs: map[string]bool{},
		FailedToolCalls:     map[string]bool{"tc_FAIL": true},
		PendingToolCalls:    map[string]bool{"tc_DONE": true, "tc_FAIL": true},
	}
	rep := walfile.Reconcile(scan, sid, facts)
	if len(rep.PendingToolCalls) != 0 {
		t.Fatalf("已收口的调用不该列进未收口名单：%v", rep.PendingToolCalls)
	}
}
