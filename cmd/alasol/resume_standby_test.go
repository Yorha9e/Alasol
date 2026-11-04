package main

// `alasol resume` 对 **standby 待机调用**的处置（CLI 口径，G-f）。
//
// 崩溃时 standby 的 tool.call.intent 已落、completed 未落 → 恢复流程把它当 in-flight op
// （Replay=safe = 重放 = 重新执行 = 重新挂起）。重放走 *core/loop.Loop：它按
// op.Intent.ToolName 去**注册表**取执行器——standby 此前只在 hub 里注册（hub 没有恢复
// 入口），CLI 的注册表只有内置工具 → 查不到执行器 → 重放 fail-close（落 failed、
// 处置结论 replayed_failed = "需人工重做"）。
//
// 本用例钉住：**standby 在恢复路径上要能被重放**（处置结论 replayed，且收口的 completed
// 与原 intent 同一个 tool_call_id——X1 配对完整）。
//
// 用例走"消息**已到**"这条：重放后执行器回查原 intent（游标 = 原 intent 的 seq）→ 看见
// 崩溃期间到达的那条消息 → 立刻醒 → 如实收口（不等超时，测试秒级跑完）。

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// seedStandbyCrashedSession 造一个"崩溃在 standby 等待中"的事件库：
// gate.joined + standby 的 tool.call.intent（**无** completed）+ 一条崩溃期间到达的广播。
func seedStandbyCrashedSession(t *testing.T, dbPath string) (contracts.Ulid, string) {
	t.Helper()
	return seedStandbyCrashedSessionWith(t, dbPath, true)
}

// seedStandbyCrashedSessionWith：withMessage=false → **消息永远不来**（重放只能等到
// 恢复路径的上界；critic F4 的用例要的就是这条）。
func seedStandbyCrashedSessionWith(t *testing.T, dbPath string, withMessage bool) (contracts.Ulid, string) {
	t.Helper()
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	session := core.NewUlid()
	agent := core.NewUlid()
	peer := core.NewUlid()
	const callID = "toolu_STANDBY_CLI_01"
	const gateID = "team:standby-cli"
	const msgText = "我改完了 README"

	joined, _ := json.Marshal(contracts.GateJoinedPayload{GateID: gateID, Source: contracts.GateJoinSourceExplicit})
	intent, _ := json.Marshal(contracts.ToolCallIntentPayload{
		ToolCallID: contracts.ProviderId(callID),
		ToolName:   "standby",
		Arguments:  json.RawMessage(`{"wait":"message","timeout_min":1,"on_timeout":"return"}`),
		Idempotent: true,
		Replay:     contracts.ReplayKindSafe,
	})
	gate := gateID
	broadcast, _ := json.Marshal(contracts.AgentLinkBroadcastPayload{
		Target: contracts.AgentLinkTarget{Type: contracts.AgentLinkTargetTypeGate, GateID: &gate},
		Text:   msgText,
	})
	seed := []contracts.Event{
		{V: 1, ID: core.NewUlid(), Kind: discovery.KindGateJoined, TS: "2026-10-28T00:00:00Z",
			SessionID: session, AgentID: agent, Durable: true, Payload: joined},
		{V: 1, ID: core.NewUlid(), Kind: "tool.call.intent", TS: "2026-10-28T00:00:01Z",
			SessionID: session, AgentID: agent, Durable: true, Payload: intent},
	}
	if withMessage {
		seed = append(seed, contracts.Event{
			V: 1, ID: core.NewUlid(), Kind: agentlink.KindBroadcast, TS: "2026-10-28T00:00:02Z",
			SessionID: peer, AgentID: core.NewUlid(), Durable: true, Payload: broadcast,
		})
	}
	if _, err := st.Append(context.Background(), seed); err != nil {
		t.Fatalf("append: %v", err)
	}
	return session, callID
}

// TestResumeCLIReplaysStandbyInFlight：恢复路径上 standby 的 in-flight 调用要被**重放**
// （处置结论 replayed），且收口的 completed 与原 intent 同一个 tool_call_id。
func TestResumeCLIReplaysStandbyInFlight(t *testing.T) {
	db := filepath.Join(t.TempDir(), "resume.db")
	session, callID := seedStandbyCrashedSession(t, db)

	out := captureStdout(t)
	code := resumeMain([]string{"-session", string(session), "-db", db, "-verify", "none"})
	got := out()

	if code != 0 {
		t.Fatalf("resumeMain 退出码 = %d, want 0\n%s", code, got)
	}
	var rep struct {
		Dispositions []struct {
			ToolCallID  contracts.ProviderId `json:"tool_call_id"`
			Disposition string               `json:"disposition"`
		} `json:"dispositions"`
	}
	if err := json.Unmarshal([]byte(got), &rep); err != nil {
		t.Fatalf("报告不是合法 JSON: %v\n%s", err, got)
	}
	var disp string
	for _, d := range rep.Dispositions {
		if d.ToolCallID == contracts.ProviderId(callID) {
			disp = d.Disposition
		}
	}
	if disp != "replayed" {
		t.Fatalf("standby 的处置结论 = %q，want replayed（恢复路径上 standby 要能被重放；"+
			"replayed_failed 说明执行器没进注册表）", disp)
	}

	// X1 配对：事件流里有 completed，且与原 intent 同一个 tool_call_id（没有 failed）。
	st, err := sqlite.Open(db, sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	evs, err := st.ReadFrom(context.Background(), &session, 1, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	var completedText string
	for _, ev := range evs {
		switch ev.Kind {
		case "tool.call.completed":
			var p contracts.ToolCallCompletedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Fatalf("解 completed 载荷失败: %v", err)
			}
			if string(p.ToolCallID) != callID {
				t.Fatalf("completed 的 tool_call_id = %q，want %q", p.ToolCallID, callID)
			}
			for _, part := range p.Result.Content {
				if part.Text != nil {
					completedText += *part.Text
				}
			}
		case "tool.call.failed":
			t.Fatal("不该有 tool.call.failed（standby 应重放成功并如实收口）")
		}
	}
	if !strings.Contains(completedText, "我改完了 README") {
		t.Fatalf("completed 正文应带消息摘要，得到 %q", completedText)
	}
}

// TestResumeCLIBoundedReplay（critic F4）：恢复路径的重放**有上界**——把上界调小
// （resumeReplayCap），一条"消息永远不来"的 standby 重放必须在该上界内**返回**
// （如实收场），而不是静默挂 30 分钟（看着像卡死）。
//
// 还要钉住"重挂前报一行"：人得知道命令在等什么、还能等多久。
func TestResumeCLIBoundedReplay(t *testing.T) {
	old := resumeReplayCap
	resumeReplayCap = 500 * time.Millisecond
	t.Cleanup(func() { resumeReplayCap = old })

	db := filepath.Join(t.TempDir(), "resume.db")
	session, _ := seedStandbyCrashedSessionWith(t, db, false) // 消息永远不来

	outStop := captureStdout(t)
	errStop := captureStderr(t)
	start := time.Now()
	code := resumeMain([]string{"-session", string(session), "-db", db, "-verify", "none"})
	elapsed := time.Since(start)
	_ = outStop()
	errOut := errStop()

	if elapsed > 20*time.Second {
		t.Fatalf("恢复不该静默挂住（重放无上界）：耗时 %v", elapsed)
	}
	if !strings.Contains(errOut, "standby 重挂中") {
		t.Fatalf("重挂前该报一行「standby 重挂中（最多 N 分钟）」：\n%s", errOut)
	}
	// 上界到点 → 重放如实失败（退出码 1 = 需人工重做），不是挂死。
	if code != 1 {
		t.Fatalf("退出码 = %d，want 1（上界到点后如实收场）", code)
	}
}
