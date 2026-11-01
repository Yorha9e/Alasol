package walfile_test

// 对账**分段归组**（F1）的钉子：轮次号跨 Run 撞号 → 悬空的那一轮不能被后一次 Run 的
// turn.end"抹平"。
//
// # 为什么要有这组钉子
//
// WAL 载荷里的轮次号是"**每次 Run 内从 1 起**"（core/loop/loop.go 的轮次循环）→ 同一个
// session 跑第二次 Run 时，轮次号又从 1 开始。若对账按轮次号**全局归集**，第二次 Run 的
// turn.end 会把第一次 Run 留下的**悬空 turn.begin** 判成"已收口"→ 报告谎报"无崩溃残留"
// （**假阴性**：崩溃现场被抹掉——比误报更坏，使用者据此以为没丢东西）。
//
// 故归组单位是**段**（一次 Run 的一轮）：每条 turn.begin 开一个新段，段内的
// begin/delta/end/failed 归这一段；段无收口 → 在飞。
//
// 变异自证：把分段归组改回"按轮次号全局归集" → 本组钉子红（见交付说明）。

import (
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/walfile"
)

// TestReconcileSegmentKeepsDanglingTurnAcrossRuns（F1 主钉）：**上一次 Run 崩在轮中**
// （有 turn.begin、无收口）+ **这一次 Run 正常跑完**（begin/delta/end）→ 报告**仍要报
// 1 个在飞轮次**、Crashed()=true。
//
// 复现的正是终查 F1 的现象：同 session 两次 RunPrompt（第一次崩在轮中）→ 旧口径报
// Turns=1、InFlightTurn=nil、Crashed=false（谎报"无崩溃残留"）。
func TestReconcileSegmentKeepsDanglingTurnAcrossRuns(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Path:   "/x/" + string(sid) + ".wal",
		Exists: true,
		Records: []core.WalRecord{
			// ── Run1：崩在第 1 轮（begin 落了，end/failed 都没落）──
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "model-A", Messages: 3, Tools: 1}),
			recOf(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 1, Text: strPtr("崩前吐的")}),
			// ── Run2：轮次号**又从 1 起**（撞号），这一轮正常收口 ──
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "model-B", Messages: 5, Tools: 2}),
			recOf(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 1, Text: strPtr("这次跑完了")}),
			recOf(core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1, ToolCalls: 0}),
		},
	}

	rep := walfile.Reconcile(scan, sid, walfile.Facts{})

	// ① 崩溃点**不能被后一次 Run 的 turn.end 抹平**：仍有 1 个在飞轮次。
	if rep.InFlightTurn == nil {
		t.Fatalf("上一次 Run 的悬空 turn.begin 被后一次 Run 的 turn.end 抹平了（InFlightTurn=nil）——"+
			"对账假阴性：谎报\"无崩溃残留\"。Turns=%+v", rep.Turns)
	}
	if !rep.Crashed() {
		t.Error("有悬空轮次 → Crashed() 应为 true")
	}
	if rep.InFlightTurns != 1 {
		t.Errorf("InFlightTurns = %d，want 1（只有上一次 Run 那一段悬空）", rep.InFlightTurns)
	}
	// ② 在飞的是**上一次 Run 那一段**：模型名/文本都不该与后一次 Run 混。
	if rep.InFlightTurn.Model != "model-A" {
		t.Errorf("InFlightTurn.Model = %q，want model-A（后一次 Run 的段不该顶替它）", rep.InFlightTurn.Model)
	}
	if rep.InFlightTurn.Text != "崩前吐的" {
		t.Errorf("InFlightTurn.Text = %q，want \"崩前吐的\"（两段的 delta 不该混在一起）", rep.InFlightTurn.Text)
	}
	if rep.InFlightTurn.Deltas != 1 {
		t.Errorf("InFlightTurn.Deltas = %d，want 1", rep.InFlightTurn.Deltas)
	}
	// ③ 报告条目按**段**列（两段 → 两项；轮次号重复是正常的：它每次 Run 内从 1 起）。
	if len(rep.Turns) != 2 {
		t.Fatalf("Turns = %d，want 2（一项 = 一段 = 一次 Run 的一轮）", len(rep.Turns))
	}
	if rep.Turns[0].Ended || rep.Turns[0].Failed {
		t.Errorf("第 1 段（悬空那次 Run）不该标 Ended/Failed：%+v", rep.Turns[0])
	}
	if !rep.Turns[1].Ended {
		t.Errorf("第 2 段（正常收口那次 Run）应标 Ended：%+v", rep.Turns[1])
	}
	// ④ 摘要要说清"崩在哪儿"（人话面不许把悬空段藏掉）。
	s := rep.Summary()
	if !strings.Contains(s, "model-A") {
		t.Errorf("摘要应点名崩溃那一段的模型，got %q", s)
	}
}

// TestReconcileSegmentAllClosedNoFalsePositive（F1 的另一半，不误报）：两次 Run **都**
// 正常收口（每段都有 turn.end）→ 无在飞轮次、Crashed()=false。
//
// 为什么钉：分段归组若写错（例如"有 begin 就报在飞"），会把每一次正常跑完的 session
// 都报成"疑似崩溃残留"（假阳性）——比假阴性更吵，使用者会学会忽略这个报告。
func TestReconcileSegmentAllClosedNoFalsePositive(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			// Run1：两轮，都收口。
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
			recOf(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 1, Text: strPtr("a")}),
			recOf(core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 2, Model: "m"}),
			recOf(core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 2}),
			// Run2：轮次号又从 1 起，同样收口。
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
			recOf(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 1, Text: strPtr("b")}),
			recOf(core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
		},
	}

	rep := walfile.Reconcile(scan, sid, walfile.Facts{})
	if rep.InFlightTurn != nil {
		t.Fatalf("全部收口时不该有在飞轮次，got %+v", rep.InFlightTurn)
	}
	if rep.InFlightTurns != 0 {
		t.Errorf("InFlightTurns = %d，want 0", rep.InFlightTurns)
	}
	if rep.Crashed() {
		t.Error("全部收口时 Crashed() 应为 false（不误报）")
	}
	if len(rep.Turns) != 3 {
		t.Errorf("Turns = %d，want 3（三段）", len(rep.Turns))
	}
	for i, tn := range rep.Turns {
		if !tn.Ended {
			t.Errorf("第 %d 段应标 Ended：%+v", i, tn)
		}
	}
}

// TestReconcileSegmentFailedIsNotInFlight（F1 与 turn.failed 的合流）：**普通失败**
// （模型调用失败 → turn.failed）的那一段**不算在飞**——哪怕后面又来了一次 Run。
//
// 为什么钉：分段归组后，"failed 算收口"这条口径必须**逐段**成立；否则一次模型报错会被
// 报成"疑似崩溃残留"（假阳性）。
func TestReconcileSegmentFailedIsNotInFlight(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			// Run1：模型调用失败（普通失败）→ turn.failed 收口。
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
			recOf(core.KindWalTurnFailed, core.WalTurnFailedPayload{Turn: 1, Error: "provider: 端点 500"}),
			// Run2：正常收口。
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
			recOf(core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
		},
	}
	rep := walfile.Reconcile(scan, sid, walfile.Facts{})
	if rep.InFlightTurn != nil {
		t.Fatalf("普通失败（begin+failed）不该算在飞，got %+v", rep.InFlightTurn)
	}
	if rep.Crashed() {
		t.Error("普通失败不该报崩溃残留")
	}
	if len(rep.Turns) != 2 || !rep.Turns[0].Failed {
		t.Errorf("Turns = %+v，want 2 段且第 1 段 Failed=true", rep.Turns)
	}
}

// TestReconcileSegmentCountsMultipleDanglingTurns（F1 的诚实面）：**崩了两次**（两段都
// 悬空）→ 未收口段数如实报 2；在飞轮次带**最近的那一段**。
//
// 为什么钉：分段归组之后，"未收口"可能不止一段（每崩一次留一段）。只报一段 = 藏证据
// （与 F1 修掉的"把悬空 begin 判成已收口"同一类毛病，只是方向相反）。
func TestReconcileSegmentCountsMultipleDanglingTurns(t *testing.T) {
	sid := core.NewUlid()
	scan := walfile.ScanResult{
		Exists: true,
		Records: []core.WalRecord{
			// Run1 崩在轮中。
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "old-crash"}),
			// Run2 也崩在轮中。
			recOf(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "new-crash"}),
		},
	}
	rep := walfile.Reconcile(scan, sid, walfile.Facts{})
	if rep.InFlightTurns != 2 {
		t.Errorf("InFlightTurns = %d，want 2（崩了两次就如实说两次）", rep.InFlightTurns)
	}
	if rep.InFlightTurn == nil || rep.InFlightTurn.Model != "new-crash" {
		t.Fatalf("InFlightTurn = %+v，want 最近一段（model=new-crash）", rep.InFlightTurn)
	}
	if s := rep.Summary(); !strings.Contains(s, "另有 1 段") {
		t.Errorf("摘要应说明还有更早的未收口段，got %q", s)
	}
}
