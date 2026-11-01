package main

// `alasol wal <session>` 子命令的端到端钉子（v4-4-B 恢复面 CLI）。
//
// 口径：
//   - 单行 JSON 到 stdout（机器可消费），人话摘要到 stderr；
//   - **观察面不是写回面**：命令只读 WAL + 事件库，不写事件流、不补 delta；
//   - 未配置 WAL（-wal-dir 空且配置文件的 [wal].dir 空）→ wal_exists=false，**退出码 0**
//     （"没配"是合法状态，不是错误）。

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	"github.com/Yorha9e/alasol/runtime/store/walfile"
)

// walSessionWithEvents 建一个事件库并塞一条 tool.call.completed（tool_call_id 与下面 seed
// 的 blob.plan **不同**）——即"事件流里有 completed，但没有 seed 那个 tool_call 的"。
func walSessionWithEvents(t *testing.T, dbPath string) contracts.Ulid {
	t.Helper()
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	session, agent := core.NewUlid(), core.NewUlid()
	raw, _ := json.Marshal(contracts.ToolCallCompletedPayload{
		ToolCallID: "tc_SOME_OTHER_CALL",
		Result:     contracts.ToolResult{},
	})
	ref := core.NewUlid()
	ev := contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: "tool.call.completed", TS: "2026-11-01T00:00:00Z",
		SessionID: session, AgentID: agent, Durable: true, Payload: raw, BinaryRef: &ref,
	}
	if _, err := st.Append(context.Background(), []contracts.Event{ev}); err != nil {
		t.Fatalf("append: %v", err)
	}
	return session
}

// walStoreFor 开一个只读用途的事件库句柄（测试辅助：walResumeSummary 要读事件流）。
func walStoreFor(t *testing.T, dbPath string) core.Store {
	t.Helper()
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// seedWAL 直接往 WAL 目录写一份"崩溃现场"（begin + delta + blob.plan，无 end）——
// 这就是"崩溃后磁盘上剩下的东西"。
func seedWAL(t *testing.T, dir string, sid contracts.Ulid, orphanToolCall string) {
	t.Helper()
	st, err := walfile.New(dir)
	if err != nil {
		t.Fatalf("walfile.New: %v", err)
	}
	defer st.Close()
	w, err := st.Writer(sid)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if err := w.Append(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 3, Model: "crashy", Messages: 6, Tools: 2}); err != nil {
		t.Fatal(err)
	}
	txt := "崩在这"
	if err := w.Append(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 3, Text: &txt}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 3, ToolCallID: orphanToolCall, ResultBytes: 9000, Threshold: 64}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestWALCLIReportsCrashResidue（F1）：seed 一份崩溃现场 → `alasol wal` 打印单行 JSON，
// 含在飞轮次（turn/model/delta 文本）与孤儿 tool_call；退出码 0。
func TestWALCLIReportsCrashResidue(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	dbPath := filepath.Join(dir, "events.db")
	sid := walSessionWithEvents(t, dbPath) // 建库 + 塞一条不对应的 completed

	seedWAL(t, walDir, sid, "tc_CLI_ORPHAN")

	out := captureStdout(t)
	code := walMain([]string{
		"-session", string(sid),
		"-db", dbPath,
		"-wal-dir", walDir,
		"-json",
	})
	got := out()
	if code != 0 {
		t.Fatalf("walMain 退出码 = %d，want 0", code)
	}
	// 单行 JSON。
	lines := strings.Split(strings.TrimSpace(got), "\n")
	if len(lines) != 1 {
		t.Fatalf("stdout 应只有一行 JSON，got %d 行:\n%s", len(lines), got)
	}
	var rep walfile.Report
	if err := json.Unmarshal([]byte(lines[0]), &rep); err != nil {
		t.Fatalf("stdout 不是合法 JSON: %v\n%s", err, got)
	}
	if rep.SessionID != string(sid) || !rep.WALExists {
		t.Fatalf("报告 = %+v", rep)
	}
	if rep.InFlightTurn == nil || rep.InFlightTurn.Turn != 3 || rep.InFlightTurn.Model != "crashy" {
		t.Fatalf("InFlightTurn = %+v，want turn=3 model=crashy", rep.InFlightTurn)
	}
	if rep.InFlightTurn.Text != "崩在这" {
		t.Errorf("InFlightTurn.Text = %q，want 崩在这", rep.InFlightTurn.Text)
	}
	if len(rep.OrphanBlobToolCalls) != 1 || rep.OrphanBlobToolCalls[0] != "tc_CLI_ORPHAN" {
		t.Fatalf("孤儿候选 = %v，want [tc_CLI_ORPHAN]", rep.OrphanBlobToolCalls)
	}
}

// TestWALResumeSummaryPrintedOnFailure（F9，critic）：**resume 失败**时 stderr 仍要有
// [wal] 摘要——"上次崩在哪儿"恰恰是恢复失败时最该看见的那句话（resume 失败常常就是
// 上次崩在半路的延续）。旧实现在失败分支直接 return 1，把摘要一起吞掉了。
//
// 造法：seed 一个**未释放**的 resume 认领（session.resume_claimed 之后没有
// session.resumed）→ durable.Resume 认领失败 → resumeMain 走失败分支（退出码 1）。
func TestWALResumeSummaryPrintedOnFailure(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "events.db")
	walDir := filepath.Join(dir, "wal")
	sid := walSessionWithEvents(t, dbPath)
	seedWAL(t, walDir, sid, "tc_RESUME_ORPHAN")
	seedUnreleasedResumeClaim(t, dbPath, sid)

	// WAL 目录走配置文件（[wal].dir）——resume 没有 -wal-dir flag（与 run/serve 同口径）。
	cfgPath := filepath.Join(dir, "config.toml")
	toml := "[wal]\ndir = \"" + strings.ReplaceAll(walDir, "\\", "\\\\") + "\"\n"
	if err := os.WriteFile(cfgPath, []byte(toml), 0o600); err != nil {
		t.Fatal(err)
	}

	_ = captureStdout(t)
	errOut := captureStderr(t)
	code := resumeMain([]string{
		"-session", string(sid),
		"-db", dbPath,
		"-config", cfgPath,
		"-verify", "none",
	})
	if code != 1 {
		t.Fatalf("resume 失败时退出码 = %d，want 1", code)
	}
	got := errOut()
	if !strings.Contains(got, "[wal]") {
		t.Errorf("resume 失败时 stderr 仍应有 [wal] 摘要:\n%s", got)
	}
	if !strings.Contains(got, "tc_RESUME_ORPHAN") {
		t.Errorf("[wal] 摘要应点名孤儿 tool_call:\n%s", got)
	}
}

// seedUnreleasedResumeClaim 往事件库塞一条**未释放**的 resume 认领（上次 resume 崩了/
// 还在跑）→ 下一次 resume 认领失败（走失败分支）。
func seedUnreleasedResumeClaim(t *testing.T, dbPath string, sid contracts.Ulid) {
	t.Helper()
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	raw, _ := json.Marshal(contracts.ResumeClaimedPayload{
		SessionID: sid,
		AttemptID: core.NewUlid(),
		Round:     0,
		ClaimedAt: strPtrOrNil("2026-11-01T00:00:00Z"),
	})
	ev := contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: "session.resume_claimed", TS: "2026-11-01T00:00:00Z",
		SessionID: sid, AgentID: core.NewUlid(), Durable: true, Payload: raw,
	}
	if _, err := st.Append(context.Background(), []contracts.Event{ev}); err != nil {
		t.Fatalf("append: %v", err)
	}
}

// strPtrOrNil 取一个字符串指针（测试辅助）。
func strPtrOrNil(s string) *string { return &s }

// TestWALCLIWithoutWALDirSaysSo（F2）：未配置 WAL → wal_exists=false、退出码 0（不报错）。
func TestWALCLIWithoutWALDirSaysSo(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "events.db")
	sid := walSessionWithEvents(t, dbPath)

	out := captureStdout(t)
	code := walMain([]string{
		"-session", string(sid),
		"-db", dbPath,
		// 不给 -wal-dir，且不传 -config（不会读到任何 [wal].dir）。
		"-json",
	})
	got := out()
	if code != 0 {
		t.Fatalf("未配置 WAL 不该报错，退出码 = %d（%s）", code, got)
	}
	var rep walfile.Report
	if err := json.Unmarshal([]byte(strings.TrimSpace(got)), &rep); err != nil {
		t.Fatalf("stdout 不是合法 JSON: %v\n%s", err, got)
	}
	if rep.WALExists {
		t.Error("未配置 WAL 时 wal_exists 应为 false")
	}
	if rep.Crashed() {
		t.Error("未配置 WAL 时不该报崩溃残留")
	}
}

// TestWALCLIBadSessionExit2（F3）：session id 不合法 → 退出码 2（配置类错误口径）。
func TestWALCLIBadSessionExit2(t *testing.T) {
	out := captureStdout(t)
	code := walMain([]string{"-session", "not-a-ulid", "-db", filepath.Join(t.TempDir(), "x.db")})
	_ = out()
	if code != 2 {
		t.Fatalf("非法 session id 退出码 = %d，want 2", code)
	}
}

// TestWALCLIReadsWalDirFromConfigFile（F4）：-wal-dir 没给时读配置文件的 [wal].dir
// （与 run/resume/serve 同一条配置面）。
func TestWALCLIReadsWalDirFromConfigFile(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	dbPath := filepath.Join(dir, "events.db")
	sid := walSessionWithEvents(t, dbPath)
	seedWAL(t, walDir, sid, "tc_CFG_ORPHAN")

	cfgPath := filepath.Join(dir, "config.toml")
	// Windows 路径在 TOML 里要转义反斜杠。
	toml := "[wal]\ndir = \"" + strings.ReplaceAll(walDir, "\\", "\\\\") + "\"\n"
	if err := os.WriteFile(cfgPath, []byte(toml), 0o600); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t)
	code := walMain([]string{"-session", string(sid), "-db", dbPath, "-config", cfgPath, "-json"})
	got := out()
	if code != 0 {
		t.Fatalf("退出码 = %d，want 0\n%s", code, got)
	}
	var rep walfile.Report
	if err := json.Unmarshal([]byte(strings.TrimSpace(got)), &rep); err != nil {
		t.Fatalf("stdout 不是合法 JSON: %v\n%s", err, got)
	}
	if !rep.WALExists {
		t.Fatal("应从配置文件的 [wal].dir 找到 WAL")
	}
	if len(rep.OrphanBlobToolCalls) != 1 || rep.OrphanBlobToolCalls[0] != "tc_CFG_ORPHAN" {
		t.Fatalf("孤儿候选 = %v，want [tc_CFG_ORPHAN]", rep.OrphanBlobToolCalls)
	}
}

// TestWALResumeSummarySilentWhenClean（F5）：无崩溃残留 → 一行摘要**不打印**（空串：
// 没话说的时候不说话）；未配置 WAL 同样空串。
func TestWALResumeSummarySilentWhenClean(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "events.db")
	sid := walSessionWithEvents(t, dbPath)

	// 未配置 WAL。
	if s := walResumeSummary(context.Background(), walStoreFor(t, dbPath), "", sid); s != "" {
		t.Errorf("未配置 WAL 时摘要应为空串，got %q", s)
	}
	// 配了 WAL 但里面没有这个 session 的记录（干净）。
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if s := walResumeSummary(context.Background(), walStoreFor(t, dbPath), walDir, sid); s != "" {
		t.Errorf("无 WAL 记录时摘要应为空串，got %q", s)
	}
}

// TestWALResumeSummarySpeaksUpOnResidue（F6）：有崩溃残留 → 摘要非空且点名轮次/孤儿。
func TestWALResumeSummarySpeaksUpOnResidue(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	dbPath := filepath.Join(dir, "events.db")
	sid := walSessionWithEvents(t, dbPath)
	seedWAL(t, walDir, sid, "tc_SUM_ORPHAN")

	s := walResumeSummary(context.Background(), walStoreFor(t, dbPath), walDir, sid)
	if s == "" {
		t.Fatal("有崩溃残留时摘要不该为空")
	}
	for _, want := range []string{"第 3 轮", "crashy", "tc_SUM_ORPHAN"} {
		if !strings.Contains(s, want) {
			t.Errorf("摘要缺 %q:\n%s", want, s)
		}
	}
}

// TestRunCLIWALWrites（F7）：`alasol run` 这条 CLI 装配路径同样挂 WAL（D4 的 F1 钉子之
// 同款纪律：CLI 走的是另一套装配——newRunner，不经 host.New/loopFor）。
//
// 酸测试同在这里：不配 WalDir → **一个文件都不建**。
func TestRunCLIWALWrites(t *testing.T) {
	target, _ := writeBigCLIFile(t)
	args := `{"path":"` + filepath.ToSlash(target) + `"}`

	t.Run("启用：落 WAL", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "events.db")
		walDir := filepath.Join(t.TempDir(), "wal")
		r, err := newRunner(runnerOpts{
			Model:    "scripted",
			DBPath:   dbPath,
			Approval: "deny-all",
			Allow:    "read",
			provider: &blobCLIScriptedProvider{args: args},
			WalDir:   walDir,
		})
		if err != nil {
			t.Fatalf("装配失败: %v", err)
		}
		defer r.Close()

		if _, err := r.Run(context.Background(), "读大文件"); err != nil {
			t.Fatalf("Run: %v", err)
		}
		// per-session WAL 文件：<walDir>/<session>.wal（runner 的 session id 装配时新造）。
		entries, err := os.ReadDir(walDir)
		if err != nil {
			t.Fatalf("读 WAL 目录: %v", err)
		}
		if len(entries) != 1 || !strings.HasSuffix(entries[0].Name(), ".wal") {
			t.Fatalf("WAL 目录应恰好一个 <session>.wal，got %v", entries)
		}
		raw, err := os.ReadFile(filepath.Join(walDir, entries[0].Name()))
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		for _, want := range []string{`"kind":"turn.begin"`, `"kind":"turn.delta"`, `"kind":"turn.end"`} {
			if !strings.Contains(text, want) {
				t.Errorf("WAL 缺 %s:\n%s", want, text)
			}
		}
	})

	t.Run("默认关：不建任何文件", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "events.db")
		walDir := filepath.Join(t.TempDir(), "wal") // 故意不给 runner
		r, err := newRunner(runnerOpts{
			Model:    "scripted",
			DBPath:   dbPath,
			Approval: "deny-all",
			Allow:    "read",
			provider: &blobCLIScriptedProvider{args: args},
		})
		if err != nil {
			t.Fatalf("装配失败: %v", err)
		}
		defer r.Close()
		if _, err := r.Run(context.Background(), "读大文件"); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if _, err := os.Stat(walDir); !os.IsNotExist(err) {
			t.Errorf("未配 WalDir 时不该出现 WAL 目录（stat err = %v）", err)
		}
	})
}
