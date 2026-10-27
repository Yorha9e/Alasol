package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// seedCrashedSession 建一个含「有 intent 无 result」调用的事件库（safe/verify/unsafe 各一），
// 用未注册的工具名（重放→tool_not_found→failed，无真实副作用），返回 session id。
func seedCrashedSession(t *testing.T, dbPath string) contracts.Ulid {
	t.Helper()
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	session := core.NewUlid()
	agent := core.NewUlid()
	mk := func(tcID, tool string, replay contracts.ReplayKind, anchors []contracts.Anchor) contracts.Event {
		p := contracts.ToolCallIntentPayload{
			ToolCallID: contracts.ProviderId(tcID), ToolName: tool,
			Arguments: json.RawMessage(`{}`), Replay: replay, Anchors: anchors,
		}
		raw, _ := json.Marshal(p)
		return contracts.Event{V: 1, ID: core.NewUlid(), Kind: "tool.call.intent", TS: "2026-10-28T00:00:00Z",
			SessionID: session, AgentID: agent, Durable: true, Payload: raw}
	}
	seed := []contracts.Event{
		mk("c-safe", "nosuchtool", contracts.ReplayKindSafe, nil),
		mk("c-verify", "nosuchtool", contracts.ReplayKindVerify,
			[]contracts.Anchor{{Kind: "git-commit", Value: "abc"}}),
		mk("c-unsafe", "nosuchtool", contracts.ReplayKindUnsafe, nil),
	}
	if _, err := st.Append(context.Background(), seed); err != nil {
		t.Fatalf("append: %v", err)
	}
	return session
}

// TestResumeCLINoneVerifier：alasol resume -verify none 跑恢复流程，打印处置明细 JSON。
// safe→replayed_failed（工具未注册→重放失败→failed，已收口但**需人工重做**，P2）；
// verify→manual（无 Verifier）；unsafe→manual。
func TestResumeCLINoneVerifier(t *testing.T) {
	db := filepath.Join(t.TempDir(), "resume.db")
	session := seedCrashedSession(t, db)

	out := captureStdout(t)
	code := resumeMain([]string{"-session", string(session), "-db", db, "-verify", "none"})
	got := out()

	if code != 0 {
		t.Fatalf("resumeMain 退出码 = %d, want 0\n%s", code, got)
	}
	var rep struct {
		SessionID         contracts.Ulid `json:"session_id"`
		FromCheckpointSeq int64          `json:"from_checkpoint_seq"`
		Dispositions      []struct {
			ToolCallID  contracts.ProviderId `json:"tool_call_id"`
			Disposition string               `json:"disposition"`
		} `json:"dispositions"`
	}
	if err := json.Unmarshal([]byte(got), &rep); err != nil {
		t.Fatalf("报告不是合法 JSON: %v\n%s", err, got)
	}
	if rep.SessionID != session {
		t.Fatalf("报告 session = %q, want %q", rep.SessionID, session)
	}
	disp := map[contracts.ProviderId]string{}
	for _, d := range rep.Dispositions {
		disp[d.ToolCallID] = d.Disposition
	}
	// 重放**失败**不得显示成成功（P2）：safe 的工具未注册 → 重放落 failed → replayed_failed。
	if disp["c-safe"] != "replayed_failed" {
		t.Fatalf("c-safe = %q, want replayed_failed（重放失败不得记成 replayed）", disp["c-safe"])
	}
	if disp["c-verify"] != "manual" {
		t.Fatalf("c-verify（无 Verifier）= %q, want manual", disp["c-verify"])
	}
	if disp["c-unsafe"] != "manual" {
		t.Fatalf("c-unsafe = %q, want manual", disp["c-unsafe"])
	}
}

// TestResumeCLIBadSession：非法 session id / 空 session → 退出码 2（不打开库）。
func TestResumeCLIBadSession(t *testing.T) {
	out := captureStdout(t)
	if code := resumeMain([]string{"-session", "", "-db", filepath.Join(t.TempDir(), "x.db")}); code != 2 {
		t.Fatalf("空 -session 应退出码 2，得到 %d", code)
	}
	_ = out()
	out = captureStdout(t)
	if code := resumeMain([]string{"-session", "not-a-ulid", "-db", filepath.Join(t.TempDir(), "x.db")}); code != 2 {
		t.Fatalf("非法 -session 应退出码 2，得到 %d", code)
	}
	_ = out()
}

// TestResumeCLIIdempotent：连跑两次，第二次无新处置（幂等）。
func TestResumeCLIIdempotent(t *testing.T) {
	db := filepath.Join(t.TempDir(), "resume.db")
	session := seedCrashedSession(t, db)

	run := func() int {
		out := captureStdout(t)
		code := resumeMain([]string{"-session", string(session), "-db", db, "-verify", "none"})
		_ = out()
		return code
	}
	if code := run(); code != 0 {
		t.Fatalf("第一次 resume 退出码 = %d", code)
	}
	// 第二次：捕获输出解析 Dispositions 应为空。
	out := captureStdout(t)
	code := resumeMain([]string{"-session", string(session), "-db", db, "-verify", "none"})
	got := out()
	if code != 0 {
		t.Fatalf("第二次 resume 退出码 = %d", code)
	}
	var rep struct {
		Dispositions []map[string]any `json:"dispositions"`
	}
	if err := json.Unmarshal([]byte(got), &rep); err != nil {
		t.Fatalf("第二次报告不是合法 JSON: %v", err)
	}
	if len(rep.Dispositions) != 0 {
		t.Fatalf("第二次 resume 不应重复处置，得到 %d: %s", len(rep.Dispositions), got)
	}
}

// TestResumeCLISummarySeparatesReplayedFailed（P2）：CLI 汇总必须把
// 「重放失败需人工重做」**单列**——混在"已重放成功"里会让人以为都搞定了。
func TestResumeCLISummarySeparatesReplayedFailed(t *testing.T) {
	db := filepath.Join(t.TempDir(), "resume.db")
	session := seedCrashedSession(t, db)

	stopOut := captureStdout(t)
	stopErr := captureStderr(t)
	code := resumeMain([]string{"-session", string(session), "-db", db, "-verify", "none"})
	_ = stopOut()
	summary := stopErr()

	if code != 0 {
		t.Fatalf("resumeMain 退出码 = %d, want 0\n%s", code, summary)
	}
	if !strings.Contains(summary, "已重放成功") {
		t.Fatalf("汇总应有「已重放成功」一栏：%s", summary)
	}
	if !strings.Contains(summary, "重放失败需人工重做") {
		t.Fatalf("汇总应有「重放失败需人工重做」一栏：%s", summary)
	}
	if !strings.Contains(summary, "需人工重做") {
		t.Fatalf("汇总应提示有调用需人工重做：%s", summary)
	}
}
