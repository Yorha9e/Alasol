package loop_test

// WAL 外置**计划**（blob.plan）在 **spawn / replay** 两条路径上的钉子（F3）。
//
// # 背景：blob.plan 是什么
//
// blob.plan 记的是"这个 tool_call 的结果**可能**会被外置成 blob"的**计划**——写在
// tool.call.intent **之前**（X1 三段窗口之外：intent→completed 之间插写点 = 在关键路径上
// 多一次落盘）。崩溃续跑据此对账"这个 tool_call 的结果没落"→ 列出孤儿 blob 候选
// （v4-3-A 的崩溃窗口：blob 写成功、引用没落）。
//
// # 为什么这两条路径此前没有钉子
//
// 普通工具调用的写点有钉子（core/loop/wal_test.go 的 TestWALBlobPlanBeforeIntent），但：
//
//   - **spawn**（生命周期工具）：它的**结果**（子 agent 的 summary）同样走外置
//     （finishSpawn → externalizeResult）→ 同样需要一条计划；
//   - **replay**（resume 续跑路径）：重放的 intent 是**历史事件**（上一进程落的），没法
//     "排在它前面" → 计划只能落在**重放动手前**（本路径最早的写点）。
//
// 两条路径的写点被挪走/漏写都不会有人发现——故各补一条钉子。

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/durable"
	"github.com/Yorha9e/alasol/core/loop"
)

// TestWALBlobPlanBeforeSpawnIntent（F3 之一，spawn 路径）：模型发起 **spawn** →
// ① WAL 里有这条 spawn 的 **blob.plan**（对账键 = spawn 的 tool_call_id）；
// ② 计划写在 spawn 的 **tool.call.intent 之前**（写点全在三段窗口之外）；
// ③ spawn 的 summary **确实走了外置**（completed 带 binary_ref）——即这条计划不是空写。
//
// 钉子：把 spawn 路径的 planExternalize 挪到 intent 之后（或删掉）→ ①/② 红。
func TestWALBlobPlanBeforeSpawnIntent(t *testing.T) {
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"scout"}, Tools: nil, Model: maRootModel, PermissionRequest: permFull()}
	h := newMAHarness(t, rootType, 3, allowRule("read"))
	h.registerType(agents.AgentType{ID: "scout", CanSpawn: nil, Tools: []string{"read"}, Model: "scout-model", PermissionRequest: permRO()})
	h.addTool("read", nil)
	h.verifier = &recordingVerifier{}

	// 顺序时间线：WAL 追加与事件入库共用一条（直接比先后）。
	tl := newOrderLog()
	ms, ok := h.store.(*memStore)
	if !ok {
		t.Fatalf("store 不是 *memStore（%T）", h.store)
	}
	ms.log = tl

	rec := &recWAL{}
	wal := walSinkFunc(func(kind string, payload any) error {
		if kind == core.KindWalBlobPlan {
			tl.add("wal:" + core.KindWalBlobPlan)
		}
		return rec.Append(kind, payload)
	})
	blob := newFakeBlob()

	// 子 agent 的 summary 很大 → 必外置（有效阈值 = max(64, 摘要预算 768) = 768）。
	longSummary := strings.Repeat("总结正文", 2000)
	reqID := core.NewUlid()
	h.prov.script(maRootModel,
		stepTools(call("spawn1", loop.ToolSpawn, string(spawnArgs("scout", "recon", "读一下 README 并总结", reqID)))),
		stepText("主 agent 收尾"),
	)
	h.prov.script("scout-model",
		stepTools(call("done1", loop.ToolComplete, string(completeArgs(longSummary, nil, agents.TaintClean)))),
	)

	l, err := loop.New(loop.Config{
		Provider:      h.prov,
		Bus:           h.bus,
		Registry:      h.reg,
		Policy:        h.engine,
		Approver:      core.DenyAll{},
		SessionID:     h.session,
		AgentID:       h.tree.Root(),
		Model:         maRootModel,
		System:        "root-system",
		MaxTurns:      10,
		Agents:        &loop.Agents{Tree: h.tree, Verifier: h.verifier},
		WalSink:       wal,
		BlobSink:      blob,
		BlobMaxInline: 64,
	})
	if err != nil {
		t.Fatalf("loop.New: %v", err)
	}
	if _, err := l.Run(context.Background(), "拆个任务"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// ① WAL 里有这条 spawn 的外置计划（对账键 = spawn 的 tool_call_id）。
	plans := payloadsOf(t, rec, core.KindWalBlobPlan)
	if len(plans) != 1 {
		t.Fatalf("blob.plan = %d 条，want 1（spawn 路径也要记计划）", len(plans))
	}
	if plans[0]["tool_call_id"] != "spawn1" {
		t.Errorf("blob.plan.tool_call_id = %v，want spawn1（对账键）", plans[0]["tool_call_id"])
	}
	if got, ok := plans[0]["turn"].(float64); !ok || int(got) != 1 {
		t.Errorf("blob.plan.turn = %v，want 1（父的轮次）", plans[0]["turn"])
	}

	// ② 顺序：计划早于 spawn 的 intent（写点在 X1 窗口之外）。
	entries := tl.all()
	planAt := indexOfEntry(entries, "wal:"+core.KindWalBlobPlan)
	intentAt := indexOfEntry(entries, "intent:spawn1")
	if planAt < 0 || intentAt < 0 {
		t.Fatalf("顺序日志缺格子（plan=%d intent=%d）：%v", planAt, intentAt, entries)
	}
	if planAt > intentAt {
		t.Errorf("blob.plan 应早于 spawn 的 tool.call.intent（X1 三段窗口之外），got %v", entries)
	}

	// ③ spawn 的 summary 确实外置了（completed 带 binary_ref）——这条计划有实际对象。
	var spawnDone contracts.Event
	for _, ev := range h.events() {
		if ev.Kind != loop.KindToolCallCompleted {
			continue
		}
		var p contracts.ToolCallCompletedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			continue
		}
		if string(p.ToolCallID) == "spawn1" {
			spawnDone = ev
			break
		}
	}
	if spawnDone.ID == "" {
		t.Fatal("找不到 spawn1 的 tool.call.completed")
	}
	if spawnDone.BinaryRef == nil {
		t.Error("spawn 的 summary 应外置（BinaryRef 非空）——否则这条 blob.plan 是空写")
	}
}

// TestWALBlobPlanBeforeReplayExec（F3 之二，replay 路径）：**重放**（resume 续跑路径）
// ① WAL 里有这条重放的外置计划（tool_call_id = 被重放的那个，turn=0）；
// ② 计划写在**动手执行之前**（重放路径没有"intent 落盘"这个动作——它的 intent 是历史
// 事件，所以最早的写点就是"动手前"）；③ 计划早于重放落下的 completed。
//
// 钉子：把 replay 路径的 planExternalize 删掉（或挪到执行之后）→ ①/② 红。
func TestWALBlobPlanBeforeReplayExec(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	big := strings.Repeat("R", 5000)
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &big}}}

	rec := &recWAL{}
	wal := walSinkFunc(func(kind string, payload any) error {
		if kind == core.KindWalBlobPlan {
			h.log.add("wal:" + core.KindWalBlobPlan)
		}
		return rec.Append(kind, payload)
	})
	blob := newFakeBlob()
	l := h.loop(func(c *loop.Config) {
		c.WalSink = wal
		c.BlobSink = blob
		c.BlobMaxInline = 64 // 小阈值 → 5000 字节必外置
	})

	op := durable.Op{
		Intent: contracts.ToolCallIntentPayload{
			ToolCallID: "tc_WAL_REPLAY",
			ToolName:   "echo",
			Arguments:  json.RawMessage(`{"text":"hi"}`),
		},
		IntentEventID: core.NewUlid(),
		SessionID:     h.session,
		AgentID:       h.agent,
	}
	res, err := l.Replay(context.Background(), op)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !res.OK {
		t.Fatalf("重放应成功，got %+v", res)
	}

	// ① 计划存在，且键/轮次号如实（重放没有"第几轮"的概念 → turn=0）。
	plans := payloadsOf(t, rec, core.KindWalBlobPlan)
	if len(plans) != 1 {
		t.Fatalf("blob.plan = %d 条，want 1（重放路径也要记计划）", len(plans))
	}
	if plans[0]["tool_call_id"] != "tc_WAL_REPLAY" {
		t.Errorf("blob.plan.tool_call_id = %v，want tc_WAL_REPLAY", plans[0]["tool_call_id"])
	}
	if got, ok := plans[0]["turn"].(float64); !ok || int(got) != 0 {
		t.Errorf("blob.plan.turn = %v，want 0（重放路径如实记 0）", plans[0]["turn"])
	}

	// ② 顺序：计划早于**动手执行**、早于重放落下的 completed。
	entries := h.log.all()
	planAt := indexOfEntry(entries, "wal:"+core.KindWalBlobPlan)
	execAt := indexOfEntry(entries, "exec:tc_WAL_REPLAY")
	doneAt := indexOfEntry(entries, "completed:tc_WAL_REPLAY")
	if planAt < 0 || execAt < 0 || doneAt < 0 {
		t.Fatalf("顺序日志缺格子（plan=%d exec=%d completed=%d）：%v", planAt, execAt, doneAt, entries)
	}
	if planAt > execAt {
		t.Errorf("blob.plan 应早于重放动手执行，got %v", entries)
	}
	if planAt > doneAt {
		t.Errorf("blob.plan 应早于重放落下的 completed，got %v", entries)
	}
	// ③ 重放产出的 completed 确实外置了（这条计划有实际对象）。
	var done contracts.Event
	for _, ev := range h.events() {
		if ev.Kind == loop.KindToolCallCompleted {
			var p contracts.ToolCallCompletedPayload
			if err := json.Unmarshal(ev.Payload, &p); err == nil && string(p.ToolCallID) == "tc_WAL_REPLAY" {
				done = ev
				break
			}
		}
	}
	if done.ID == "" {
		t.Fatal("找不到 tc_WAL_REPLAY 的 tool.call.completed")
	}
	if done.BinaryRef == nil {
		t.Error("重放的大 result 应外置（BinaryRef 非空）")
	}
}
