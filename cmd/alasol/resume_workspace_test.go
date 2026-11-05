package main

// L2 per-session 执行上下文 · 件 2 的 **CLI resume 面**钉子（critic P1-2）。
//
// 背景：resume 重放的是"上一次进程发起的同一个已授权操作"——它要落在**同一个工作区**
// 里。漏了工作区解析 → 工具按**进程 cwd** 解析相对路径 → 重放把文件写到别处：结果
// **看起来成功**（有 completed 事件），但内容错位，且没有任何报错能提示（比"重放失败"
// 更糟——失败至少是显性的）。

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// seedWorkspaceSession 建一个事件库：一条 config.changed{exec:{cwd}}（会话工作区登记）
// + 一条**在飞**的 write intent（相对路径 → 重放要落在工作区里）。
//
// 返回 session id。
func seedWorkspaceSession(t *testing.T, dbPath, workspace string) contracts.Ulid {
	t.Helper()
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	session := core.NewUlid()
	agent := core.NewUlid()

	// ① 会话工作区登记（config.changed{exec}——L2 件 2 的持久化载体）。
	cfgRaw, err := json.Marshal(contracts.ConfigChangedPayload{
		Exec: &contracts.ExecConfigChange{Cwd: &workspace},
	})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	// ② 在飞的 write intent（相对路径；safe = 可重放）。
	intentRaw, err := json.Marshal(contracts.ToolCallIntentPayload{
		ToolCallID: contracts.ProviderId("c-ws"),
		ToolName:   "write",
		Arguments:  json.RawMessage(`{"path":"replay.txt","content":"重放内容"}`),
		Replay:     contracts.ReplayKindSafe,
	})
	if err != nil {
		t.Fatalf("marshal intent: %v", err)
	}
	seed := []contracts.Event{
		{V: 1, ID: core.NewUlid(), Kind: "config.changed", TS: "2026-10-28T00:00:00Z",
			SessionID: session, AgentID: agent, Durable: true, Payload: cfgRaw},
		{V: 1, ID: core.NewUlid(), Kind: "tool.call.intent", TS: "2026-10-28T00:00:01Z",
			SessionID: session, AgentID: agent, Durable: true, Payload: intentRaw},
	}
	if _, err := st.Append(context.Background(), seed); err != nil {
		t.Fatalf("append: %v", err)
	}
	return session
}

// TestResumeReplaysIntoSessionWorkspace：resume 重放**落在会话工作区**里
// （critic P1-2：漏解析工作区 → 文件落进程 cwd → 静默错位）。
func TestResumeReplaysIntoSessionWorkspace(t *testing.T) {
	db := filepath.Join(t.TempDir(), "resume.db")
	ws := t.TempDir()
	// 进程 cwd 换到别处 → 负断言"不落进程 cwd"才有意义。
	procCwd := t.TempDir()
	t.Chdir(procCwd)

	session := seedWorkspaceSession(t, db, ws)

	out := captureStdout(t)
	code := resumeMain([]string{"-session", string(session), "-db", db, "-verify", "none"})
	got := out()
	if code != 0 {
		t.Fatalf("resumeMain 退出码 = %d, want 0\n%s", code, got)
	}
	// 正断言：文件落在**会话工作区**。
	if _, err := os.Stat(filepath.Join(ws, "replay.txt")); err != nil {
		t.Fatalf("重放的文件应落在会话工作区 %s: %v\n%s", ws, err, got)
	}
	// 负断言：**不该**落在进程 cwd（那正是"静默错位"的形态）。
	if _, err := os.Stat(filepath.Join(procCwd, "replay.txt")); err == nil {
		t.Fatalf("重放的文件不该落在进程 cwd %s（工作区没生效 → 静默错位）", procCwd)
	}
}

// TestResumeWithoutWorkspaceKeepsProcessCwd：**没有**会话工作区时 → 重放仍按进程 cwd
// （零行为变化：件 1 的"不锚定"路径）。
func TestResumeWithoutWorkspaceKeepsProcessCwd(t *testing.T) {
	db := filepath.Join(t.TempDir(), "resume.db")
	procCwd := t.TempDir()
	t.Chdir(procCwd)

	// 只 seed 在飞 intent（**没有** config.changed{exec}）。
	st, err := sqlite.Open(db, sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	session := core.NewUlid()
	agent := core.NewUlid()
	raw, _ := json.Marshal(contracts.ToolCallIntentPayload{
		ToolCallID: contracts.ProviderId("c-plain"),
		ToolName:   "write",
		Arguments:  json.RawMessage(`{"path":"plain.txt","content":"内容"}`),
		Replay:     contracts.ReplayKindSafe,
	})
	if _, err := st.Append(context.Background(), []contracts.Event{
		{V: 1, ID: core.NewUlid(), Kind: "tool.call.intent", TS: "2026-10-28T00:00:01Z",
			SessionID: session, AgentID: agent, Durable: true, Payload: raw},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	_ = st.Close()

	out := captureStdout(t)
	code := resumeMain([]string{"-session", string(session), "-db", db, "-verify", "none"})
	got := out()
	if code != 0 {
		t.Fatalf("resumeMain 退出码 = %d, want 0\n%s", code, got)
	}
	if _, err := os.Stat(filepath.Join(procCwd, "plain.txt")); err != nil {
		t.Fatalf("无工作区时重放应落在进程 cwd %s: %v\n%s", procCwd, err, got)
	}
}
