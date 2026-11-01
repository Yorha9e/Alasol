package walfile_test

// 恢复面对账（Reconcile）的钉子：在飞轮次 / 孤儿 blob / 作废计划 / 不误报。
//
// **报告是观察面，不是写回面**（F5 不受影响）：对账只读两侧，不写事件流、不补 delta。
// 本组测试全部走纯函数 walfile.Reconcile（seed 好的 ScanResult + Facts）——"模拟崩溃"
// 就是"seed 一份没有对应 completed 的 WAL"。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/walfile"
)

// recOf 造一条 WAL 记录（测试seed用；与 Writer 写出的形状一致）。
func recOf(kind string, payload any) core.WalRecord {
	raw, _ := json.Marshal(payload)
	return core.WalRecord{Kind: kind, TS: "2026-11-01T00:00:00Z", Payload: raw}
}

func strPtr(s string) *string { return &s }

// TestReconcileInFlightTurn（F1）：**有 begin 无 end** 的轮次 = 崩溃点（第 N 轮在跑什么模型、
// 吐过几段 delta、吐了什么）；turn.end 之后的轮次不误报。
func TestReconcileInFlightTurn(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Path:   "/x/" + string(sid) + ".wal",
		Exists: true,
		Records: []core.WalRecord{
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "gpt-4o-mini", Messages: 3, Tools: 2}),
			recOf(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 1, Text: strPtr("正在读")}),
			recOf(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 1, Text: strPtr("文件")}),
			recOf(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 1, Reasoning: strPtr("先看目录")}),
			recOf(core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
			// 第 2 轮：只发了 begin + 两条 delta 就"崩了"（无 end）。
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 2, Model: "gpt-4o-mini", Messages: 5, Tools: 2}),
			recOf(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 2, Text: strPtr("好，我看完了")}),
			recOf(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 2, Text: strPtr("，接下来")}),
		},
	}

	rep := walfile.Reconcile(scan, sid, walfile.Facts{})
	if !rep.WALExists || rep.Records != 8 {
		t.Fatalf("WALExists=%v Records=%d，want true/7", rep.WALExists, rep.Records)
	}
	if rep.DeltaTotal != 5 {
		t.Errorf("DeltaTotal = %d，want 5", rep.DeltaTotal)
	}
	if len(rep.Turns) != 2 {
		t.Fatalf("Turns = %d，want 2", len(rep.Turns))
	}
	if !rep.Turns[0].Ended {
		t.Error("第 1 轮应标记 Ended（有 turn.end）")
	}
	if rep.Turns[1].Ended {
		t.Error("第 2 轮不该标记 Ended（崩溃点）")
	}
	// 崩溃点 = 最后一个有 begin 无 end 的轮次。
	if rep.InFlightTurn == nil {
		t.Fatal("应识别出在飞轮次（第 2 轮）")
	}
	got := rep.InFlightTurn
	if got.Turn != 2 || got.Model != "gpt-4o-mini" {
		t.Errorf("InFlightTurn = %+v，want turn=2 model=gpt-4o-mini", got)
	}
	if got.Deltas != 2 {
		t.Errorf("InFlightTurn.Deltas = %d，want 2", got.Deltas)
	}
	// delta 文本**拼得回来**（观察面的展示价值），且字符数如实。
	if got.Text != "好，我看完了，接下来" {
		t.Errorf("InFlightTurn.Text = %q", got.Text)
	}
	if got.TextChars != 10 { // "好，我看完了，接下来" = 10 个字符（含两个逗号）
		t.Errorf("InFlightTurn.TextChars = %d，want 10", got.TextChars)
	}
	if got.TextTruncated {
		t.Error("短文本不该标记截断")
	}
	if !rep.Crashed() {
		t.Error("有在飞轮次 → Crashed() 应为 true")
	}
}

// TestReconcileNoInFlightWhenAllEnded（F2，不误报）：每个 begin 都有 end → 无在飞轮次。
func TestReconcileNoInFlightWhenAllEnded(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
			recOf(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 1, Text: strPtr("a")}),
			recOf(core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
		},
	}
	rep := walfile.Reconcile(scan, sid, walfile.Facts{})
	if rep.InFlightTurn != nil {
		t.Fatalf("全部正常收口时不该有在飞轮次，got %+v", rep.InFlightTurn)
	}
	if rep.Crashed() {
		t.Error("无在飞轮次/孤儿时 Crashed() 应为 false")
	}
	if s := rep.Summary(); s == "" || !strings.Contains(s, "无在飞轮次") {
		t.Errorf("Summary 应说明已正常收口，got %q", s)
	}
}

// TestReconcileOrphanBlob（F3，孤儿识别）：blob.plan 在 WAL、事件流**无对应 completed**
// → 孤儿候选（"内容可能写了、引用没落"）。
func TestReconcileOrphanBlob(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
			recOf(core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 1, ToolCallID: "tc_ORPHAN", ResultBytes: 9000, Threshold: 64}),
		},
	}
	// 事件流里有别的 completed（就是没有 tc_ORPHAN 的）。
	facts := walfile.Facts{
		CompletedToolCalls:  map[string]bool{"tc_OTHER": true},
		CompletedBinaryRefs: map[string]bool{"tc_OTHER": true},
	}
	rep := walfile.Reconcile(scan, sid, facts)
	if len(rep.OrphanBlobToolCalls) != 1 || rep.OrphanBlobToolCalls[0] != "tc_ORPHAN" {
		t.Fatalf("孤儿候选 = %v，want [tc_ORPHAN]", rep.OrphanBlobToolCalls)
	}
	if len(rep.StaleBlobPlans) != 0 {
		t.Errorf("作废计划应为空，got %v", rep.StaleBlobPlans)
	}
	if s := rep.Summary(); !strings.Contains(s, "tc_ORPHAN") {
		t.Errorf("Summary 应点名孤儿 tool_call，got %q", s)
	}
}

// TestReconcileNoFalsePositiveWithBinaryRef（F4，不误报）：plan 在 WAL 且事件流有
// **带 binary_ref** 的 completed → 两个名单都不进（"计划过"不等于"孤儿"）。
func TestReconcileNoFalsePositiveWithBinaryRef(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
			recOf(core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 1, ToolCallID: "tc_OK"}),
			recOf(core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
		},
	}
	facts := walfile.Facts{
		CompletedToolCalls:  map[string]bool{"tc_OK": true},
		CompletedBinaryRefs: map[string]bool{"tc_OK": true},
	}
	rep := walfile.Reconcile(scan, sid, facts)
	if len(rep.OrphanBlobToolCalls) != 0 || len(rep.StaleBlobPlans) != 0 {
		t.Fatalf("有 completed + binary_ref 时不该报孤儿/作废：orphan=%v stale=%v",
			rep.OrphanBlobToolCalls, rep.StaleBlobPlans)
	}
	if rep.Crashed() {
		t.Error("该用例无崩溃残留")
	}
}

// TestReconcileStalePlanWhenNoBinaryRef（F5）：completed 落了但**没 binary_ref** →
// 作废计划（Put 失败 fail-open / 阈值变化；与孤儿辨不细，如实单列一类）。
func TestReconcileStalePlanWhenNoBinaryRef(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
			recOf(core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 1, ToolCallID: "tc_STALE"}),
			recOf(core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
		},
	}
	facts := walfile.Facts{
		CompletedToolCalls:  map[string]bool{"tc_STALE": true},
		CompletedBinaryRefs: map[string]bool{},
	}
	rep := walfile.Reconcile(scan, sid, facts)
	if len(rep.OrphanBlobToolCalls) != 0 {
		t.Errorf("有 completed 就不该算孤儿，got %v", rep.OrphanBlobToolCalls)
	}
	if len(rep.StaleBlobPlans) != 1 || rep.StaleBlobPlans[0] != "tc_STALE" {
		t.Fatalf("作废计划 = %v，want [tc_STALE]", rep.StaleBlobPlans)
	}
}

// TestReconcileDuplicatePlanCountedOnce（F6）：同一 tool_call 多次 plan（理论上是重试/重放）
// → 名单里只出现一次（去重，不刷屏）。
func TestReconcileDuplicatePlanCountedOnce(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			recOf(core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 1, ToolCallID: "tc_DUP"}),
			recOf(core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 2, ToolCallID: "tc_DUP"}),
		},
	}
	rep := walfile.Reconcile(scan, sid, walfile.Facts{})
	if len(rep.OrphanBlobToolCalls) != 1 {
		t.Fatalf("重复 plan 应去重，got %v", rep.OrphanBlobToolCalls)
	}
}

// TestReconcileCorruptPayloadSkipped（F7）：payload 解不动 → 跳过（不猜、不当空载荷）。
func TestReconcileCorruptPayloadSkipped(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			{Kind: core.KindWalTurnBegin, Payload: json.RawMessage(`{"turn":`)}, // 坏 payload
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
			recOf(core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
		},
	}
	rep := walfile.Reconcile(scan, sid, walfile.Facts{})
	if rep.InFlightTurn != nil {
		t.Fatalf("坏 payload 被跳过，剩下的 begin/end 配对 → 不该有在飞轮次，got %+v", rep.InFlightTurn)
	}
	if len(rep.Turns) != 1 || rep.Turns[0].Turn != 1 {
		t.Fatalf("Turns = %+v，want 只有第 1 轮", rep.Turns)
	}
}

// TestReconcileLongTextCapped（F8）：delta 文本超上限 → 截断 + 置 Truncated（诚实说明
// "还有，但没全带回来"），不静默丢。
func TestReconcileLongTextCapped(t *testing.T) {
	sid := core.NewUlid()
	long := strings.Repeat("中", walfile.RecoveredTextMaxBytes/3+1000) // 必超字节上限
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
			recOf(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 1, Text: &long}),
		},
	}
	rep := walfile.Reconcile(scan, sid, walfile.Facts{})
	if rep.InFlightTurn == nil {
		t.Fatal("应有在飞轮次")
	}
	if !rep.InFlightTurn.TextTruncated {
		t.Error("超长文本应标记 TextTruncated")
	}
	if len(rep.InFlightTurn.Text) > walfile.RecoveredTextMaxBytes {
		t.Errorf("带回的文本 %d 字节，超出上限 %d", len(rep.InFlightTurn.Text), walfile.RecoveredTextMaxBytes)
	}
	// 字符数仍是**全量**的（截断只作用于带回的正文，计数要诚实）。
	if rep.InFlightTurn.TextChars != len([]rune(long)) {
		t.Errorf("TextChars = %d，want %d（全量）", rep.InFlightTurn.TextChars, len([]rune(long)))
	}
}

// TestFactsFromEvents（F9）：从事件流折对账事实——只认 tool.call.completed、按 tool_call_id
// 收、**带 binary_ref 的才进 CompletedBinaryRefs**；坏 payload 跳过不猜。
func TestFactsFromEvents(t *testing.T) {
	mk := func(id string, withRef bool) contracts.Event {
		raw, _ := json.Marshal(contracts.ToolCallCompletedPayload{
			ToolCallID: contracts.ProviderId(id),
			Result:     contracts.ToolResult{},
		})
		ev := contracts.Event{Kind: "tool.call.completed", Payload: raw}
		if withRef {
			ref := contracts.Ulid(core.NewUlid())
			ev.BinaryRef = &ref
		}
		return ev
	}
	events := []contracts.Event{
		{Kind: "tool.call.intent"},
		mk("tc_A", true),
		mk("tc_B", false),
		{Kind: "tool.call.completed", Payload: json.RawMessage(`{`)}, // 坏 payload → 跳过
		{Kind: "model.turn"},
	}
	f := walfile.FactsFromEvents(events)
	if !f.CompletedToolCalls["tc_A"] || !f.CompletedToolCalls["tc_B"] {
		t.Errorf("CompletedToolCalls = %v，want 含 tc_A/tc_B", f.CompletedToolCalls)
	}
	if len(f.CompletedToolCalls) != 2 {
		t.Errorf("CompletedToolCalls 应只有 2 条（坏 payload 跳过），got %v", f.CompletedToolCalls)
	}
	if !f.CompletedBinaryRefs["tc_A"] || f.CompletedBinaryRefs["tc_B"] {
		t.Errorf("CompletedBinaryRefs = %v，want 只有 tc_A（tc_B 没外置）", f.CompletedBinaryRefs)
	}
}

// TestReconcileSessionMissingDir（F10）：WAL 目录下没有该 session 的文件 → wal_exists=false
// 且**不报错**（"没配 WAL"是合法状态，不是失败）。
func TestReconcileSessionMissingDir(t *testing.T) {
	dir := t.TempDir()
	sid := core.NewUlid()
	rep, err := walfile.ReconcileSession(dir, sid, walfile.Facts{})
	if err != nil {
		t.Fatalf("没有 WAL 不该报错: %v", err)
	}
	if rep.WALExists {
		t.Error("wal_exists 应为 false")
	}
	if rep.Crashed() {
		t.Error("没有 WAL 时不该报崩溃残留")
	}
	if s := rep.Summary(); s != "" {
		t.Errorf("没有 WAL 时 Summary 应为空串（不打印噪音），got %q", s)
	}
}

// TestReconcileTurnFailedIsNotCrash（F11，critic F2）：**普通模型失败**（provider 返错）
// → loop 写 turn.failed → 对账**不算在飞**、**不报崩溃残留**。
//
// 为什么钉：崩溃点 = "有 begin 无 end"。但模型调用失败（429/超时/端点报错）也留下
// "有 begin 无 end"——不区分的话，一次模型报错就被报成"疑似崩溃残留"（假阳性：使用者
// 据此以为丢了东西，其实只是模型报错）。钉子：去掉 turn.failed 写入 → 本用例红。
func TestReconcileTurnFailedIsNotCrash(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
			recOf(core.KindWalTurnFailed, core.WalTurnFailedPayload{Turn: 1, Error: "provider: 端点 500"}),
		},
	}
	rep := walfile.Reconcile(scan, sid, walfile.Facts{})
	if rep.InFlightTurn != nil {
		t.Fatalf("普通失败（begin + failed）不该算在飞，got %+v", rep.InFlightTurn)
	}
	if rep.Crashed() {
		t.Error("普通失败不该报崩溃残留（Crashed() 应为 false）")
	}
	if len(rep.Turns) != 1 || !rep.Turns[0].Failed {
		t.Errorf("Turns = %+v，want 1 条且 Failed=true", rep.Turns)
	}
	if rep.Turns[0].Ended {
		t.Error("失败轮次不该标 Ended（那是正常结束）")
	}
}

// TestReconcileTurnBeginOnlyIsCrash（F11 的另一半，critic F2）：**真悬空**（只有 begin，
// 无 end 无 failed）→ 仍是崩溃残留（在飞轮次 + Crashed()=true）。
func TestReconcileTurnBeginOnlyIsCrash(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 7, Model: "crashy"}),
		},
	}
	rep := walfile.Reconcile(scan, sid, walfile.Facts{})
	if rep.InFlightTurn == nil {
		t.Fatal("只有 begin（无 end 无 failed）= 真悬空 → 该报在飞轮次")
	}
	if rep.InFlightTurn.Turn != 7 {
		t.Errorf("InFlightTurn.Turn = %d，want 7", rep.InFlightTurn.Turn)
	}
	if !rep.Crashed() {
		t.Error("真悬空 → Crashed() 应为 true")
	}
}

// TestReconcileFailedToolCallIsNotOrphan（F12，critic F1 的连带）：工具调用**失败收口**
// （tool.call.failed：deny/参数坏/执行失败）→ 不算"孤儿 blob 候选"。
//
// 为什么钉：blob.plan 现在写在 intent **之前**（每个工具调用一条），若对账不认 failed，
// 一次正常的策略拒绝就会被报成"孤儿 blob 候选"（还会让 Crashed() 假阳性）。
func TestReconcileFailedToolCallIsNotOrphan(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			recOf(core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 1, ToolCallID: "tc_DENIED"}),
		},
	}
	// 事件流里：那个工具调用是 failed（正常收口）。
	facts := walfile.Facts{
		CompletedToolCalls:  map[string]bool{},
		CompletedBinaryRefs: map[string]bool{},
		FailedToolCalls:     map[string]bool{"tc_DENIED": true},
	}
	rep := walfile.Reconcile(scan, sid, facts)
	if len(rep.OrphanBlobToolCalls) != 0 {
		t.Errorf("失败的工具调用不该算孤儿候选，got %v", rep.OrphanBlobToolCalls)
	}
	if rep.Crashed() {
		t.Error("失败的工具调用不该让报告说崩溃残留")
	}
	// FactsFromEvents 也要认 tool.call.failed（否则事实折不出来 → 又变成误报）。
	raw, _ := json.Marshal(contracts.ToolCallFailedPayload{ToolCallID: "tc_DENIED"})
	f2 := walfile.FactsFromEvents([]contracts.Event{{Kind: "tool.call.failed", Payload: raw}})
	if !f2.FailedToolCalls["tc_DENIED"] {
		t.Errorf("FactsFromEvents 应认 tool.call.failed，got %v", f2.FailedToolCalls)
	}
}
