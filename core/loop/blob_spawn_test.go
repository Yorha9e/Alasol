package loop_test

// F1 的钉子：**finishSpawn 路径**（spawn 生命周期工具的 tool.call.completed）同样外置，
// 且**父拿到的 tool 消息仍是完整 summary**。
//
// # 为什么这条路径要单独钉
//
// core/loop 里写 tool.call.completed 的**三条**路径是各自造事件的：
//
//	① runToolCall —— 普通工具结果（core/loop/toolcall.go）
//	② Replay —— X2 重放落的结果（core/loop/replay.go）
//	③ finishSpawn —— spawn 生命周期工具的结果（core/loop/agenttools.go）
//
// 只接①② = "大 payload 不进事件流"在**多 agent**路径上失效：子 agent 的 summary 很大时
// （子交上来的报告动辄几十 KB），父的 spawn completed 会把整份 summary 原样塞进事件流。
//
// # 为什么还要钉"父拿到的仍是完整 summary"
//
// 外置只该瘦身**事件流**，不该改变**模型看到的东西**。finishSpawn 的回填文本取自
// `toolResultText(result)`——那是**外置前**的完整 result（外置换的是 payload.Result，
// 不是本地变量）。若哪天有人图省事改成回填 payload.Result（摘要），父 agent 就会拿到
// 一份被截断的 summary：**子 agent 的结论在父这里被静默截断**（模型看不出少了内容——
// 摘要自解释，但父的推理会基于残缺信息做）。
//
// # 变异自证
//
// 撤掉 finishSpawn 里的 externalizeResult → 本钉子红（BinaryRef 为空）。

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/loop"
)

// TestD4FinishSpawnCompletedExternalized（F1 的钉子）：spawn 的 completed 超阈值时
// ① BinaryRef 非空 ② payload 是摘要形态 ③ blob 里有全文 ④ **父拿到的 tool 消息仍是
// 完整 summary**。
func TestD4FinishSpawnCompletedExternalized(t *testing.T) {
	rootType := agents.AgentType{
		ID: "root", CanSpawn: []string{"scout"}, Model: maRootModel, PermissionRequest: permFull(),
	}
	h := newMAHarness(t, rootType, 3, allowRule("read"))
	h.registerType(agents.AgentType{
		ID: "scout", CanSpawn: nil, Tools: nil, Model: "scout-model", PermissionRequest: permRO(),
	})

	// 子 agent 交一份**大** summary（5KB）——超过阈值 → 父的 spawn completed 必被外置。
	big := strings.Repeat("S", 5000)
	h.prov.script(maRootModel,
		stepTools(call("spawn1", loop.ToolSpawn, string(spawnArgs("scout", "recon", "读点东西", core.NewUlid())))),
		stepText("主 agent 收尾"),
	)
	h.prov.script("scout-model",
		stepTools(call("done1", loop.ToolComplete, string(completeArgs(big, nil, agents.TaintClean)))),
	)

	blob := newFakeBlob()
	root, err := loop.New(loop.Config{
		Provider:  h.prov,
		Bus:       h.bus,
		Registry:  h.reg,
		Policy:    h.engine,
		Approver:  core.DenyAll{},
		SessionID: h.session,
		AgentID:   h.tree.Root(),
		Model:     maRootModel,
		MaxTurns:  10,
		Agents:    &loop.Agents{Tree: h.tree, Verifier: h.verifier},
		// D4：父挂了 blob → spawn 的 completed 同样外置（finishSpawn 是第三条写路径）。
		BlobSink:      blob,
		BlobMaxInline: 1024,
	})
	if err != nil {
		t.Fatalf("loop.New(root): %v", err)
	}
	if _, err := root.Run(context.Background(), "派个活"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 父的 spawn completed（tool_call_id = spawn1）。
	var done *contracts.Event
	for i := range h.events() {
		ev := h.events()[i]
		if ev.Kind != loop.KindToolCallCompleted {
			continue
		}
		var p contracts.ToolCallCompletedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			continue
		}
		if p.ToolCallID == "spawn1" {
			done = &h.events()[i]
			break
		}
	}
	if done == nil {
		t.Fatal("没有 spawn 的 tool.call.completed（spawn 路径没落结果？）")
	}

	// ① BinaryRef 非空（spawn 路径接上了外置）。
	if done.BinaryRef == nil {
		t.Fatal("spawn 的大 summary 应外置：BinaryRef 不应为空（finishSpawn 没走 externalizeResult？）")
	}

	// ② payload 是**摘要**形态：占位在、完整 summary 不在。
	var got contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(done.Payload, &got); err != nil {
		t.Fatalf("completed payload 不可解: %v", err)
	}
	if len(got.Result.Content) != 1 || got.Result.Content[0].Text == nil {
		t.Fatalf("摘要应保留一个 part: %+v", got.Result.Content)
	}
	summary := *got.Result.Content[0].Text
	if !strings.Contains(summary, blobSummaryMarkerText) {
		t.Fatalf("spawn 摘要应含外置占位标记 %q，got %q", blobSummaryMarkerText, summary)
	}
	if strings.Contains(summary, big) {
		t.Fatal("spawn 摘要里不该留着完整 summary（外置没生效？）")
	}

	// ③ blob 里有**全文**（读侧按 BinaryRef 取回）。
	stored := blob.data[*done.BinaryRef]
	var full contracts.ToolResult
	if err := json.Unmarshal(stored, &full); err != nil {
		t.Fatalf("blob 内容不是合法 ToolResult JSON: %v", err)
	}
	if len(full.Content) != 1 || full.Content[0].Text == nil {
		t.Fatalf("blob 里应是完整 result: %+v", full.Content)
	}
	if !strings.Contains(*full.Content[0].Text, big) {
		t.Fatal("blob 里应是完整 summary（父拿不到的那份全文必须在 blob 里）")
	}

	// ④ **父拿到的 tool 消息仍是完整 summary**（外置只瘦身事件流，不改模型看到的东西）。
	reqs := h.prov.reqsFor(maRootModel)
	if len(reqs) < 2 {
		t.Fatalf("父应有第 2 轮请求（spawn 结果回填后），got %d", len(reqs))
	}
	var toolMsg string
	var sawTool bool
	for _, m := range reqs[1].Messages {
		if m.Role == "tool" {
			toolMsg = m.Content
			sawTool = true
		}
	}
	if !sawTool {
		t.Fatalf("父的第 2 轮请求里没有 tool 消息: %+v", reqs[1].Messages)
	}
	if !strings.Contains(toolMsg, big) {
		t.Fatalf("父拿到的 tool 消息应是**完整** summary（%d 字节），got %d 字节"+
			"（外置不许改坏回填：模型看到的必须是完整内容）", len(big), len(toolMsg))
	}
}
