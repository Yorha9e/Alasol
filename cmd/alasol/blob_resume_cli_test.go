package main

// F2 的钉子：**resume CLI 装配面**同样接上 blob 外置（D4，G2e）——重放落下的 completed
// 与原执行同口径外置。
//
// # 为什么这条路径要单独钉
//
// `alasol resume` 走的是**另一套装配**（cmd/alasol/resume.go 自己装配 loop.Config），
// 不经 host.New / host.loopFor。只给 hub 那条路径挂 blob，resume 重放落下的 completed
// 就会把大 result **原样**塞回事件流——一次重放就能在事件流里落下几十 MB（事件流要被
// 回放/转发/索引，F12）。"大 payload 不进事件流"这条规则在 resume 路径上就失效了。
//
// 另一个易犯的错：resume 的重放**真的会再次执行工具**（read 一个大文件 → 大 result），
// 这条路径上的外置必须与原执行**同口径**（同一形状：摘要 + BinaryRef）。
//
// # 变异自证
//
// 撤掉 resume.go 的 BlobSink（不传给 loop.Config）→ 本钉子红（BinaryRef 为空）。

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/blobfs"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// seedInFlightReadCall 建一个含「有 intent 无 result」调用的事件库：工具 = read（**已注册**、
// 重放会真的执行）、参数 = 读一个大文件、replay=safe（→ resume 会自动重放）。
func seedInFlightReadCall(t *testing.T, dbPath, bigFile string) contracts.Ulid {
	t.Helper()
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	session := core.NewUlid()
	agent := core.NewUlid()
	args, _ := json.Marshal(map[string]string{"path": filepath.ToSlash(bigFile)})
	p := contracts.ToolCallIntentPayload{
		ToolCallID: "c-blob-read", ToolName: "read",
		Arguments: json.RawMessage(args), Replay: contracts.ReplayKindSafe,
	}
	raw, _ := json.Marshal(p)
	ev := contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: "tool.call.intent", TS: "2026-10-31T00:00:00Z",
		SessionID: session, AgentID: agent, Durable: true, Payload: raw,
	}
	if _, err := st.Append(context.Background(), []contracts.Event{ev}); err != nil {
		t.Fatalf("append: %v", err)
	}
	return session
}

// writeBlobConfigTOML 写一份带 `[blob]` 段的配置文件（dir + max_inline）。
func writeBlobConfigTOML(t *testing.T, path, dir string, maxInline int64) {
	t.Helper()
	body := "[blob]\ndir = \"" + filepath.ToSlash(dir) + "\"\nmax_inline = " + strconv.FormatInt(maxInline, 10) + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
}

// TestResumeCLIBlobExternalizedOnReplay（F2 的钉子）：in-flight（replay=safe）的大 result
// op → `alasol resume` 重放 → ① 重放落下的 completed.BinaryRef 非空 ② blob 里有全文
// ③ payload 是摘要形态。
func TestResumeCLIBlobExternalizedOnReplay(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "resume.db")
	blobDir := filepath.Join(dir, "blobs")
	bigFile := filepath.Join(dir, "big.txt")
	content := strings.Repeat("ALASOL-RESUME-BLOB-", 600) // 9600 字节：> 阈值 1024，< 工具 64KB 上限
	if err := os.WriteFile(bigFile, []byte(content), 0o600); err != nil {
		t.Fatalf("写大文件: %v", err)
	}

	session := seedInFlightReadCall(t, db, bigFile)
	cfg := filepath.Join(dir, "config.toml")
	writeBlobConfigTOML(t, cfg, blobDir, 1024)

	out := captureStdout(t)
	code := resumeMain([]string{
		"-session", string(session),
		"-db", db,
		"-verify", "none",
		"-config", cfg,
	})
	report := out()
	if code != 0 {
		t.Fatalf("resumeMain 退出码 = %d，want 0\n%s", code, report)
	}
	if !strings.Contains(report, "replayed") {
		t.Fatalf("报告里应有重放处置（replayed）:\n%s", report)
	}

	// ① 重放落下的 completed：BinaryRef 非空。
	evs := readAllEvents(t, db)
	var done *contracts.Event
	for i := range evs {
		if evs[i].Kind == "tool.call.completed" {
			done = &evs[i]
			break
		}
	}
	if done == nil {
		t.Fatalf("重放没有落 tool.call.completed（kinds=%v）", kindsOf(evs))
	}
	if done.BinaryRef == nil {
		t.Fatal("resume 重放的大 result 应外置：completed.BinaryRef 不应为空" +
			"（resume.go 的 BlobSink 没接到 loop.Config？）")
	}

	// ② blob 里有**全文**（读侧按 BinaryRef 取回）。
	bs, err := blobfs.New(blobDir)
	if err != nil {
		t.Fatalf("blobfs.New: %v", err)
	}
	rc, err := bs.Get(context.Background(), *done.BinaryRef)
	if err != nil {
		t.Fatalf("读 blob: %v", err)
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("读 blob 内容: %v", err)
	}
	var full contracts.ToolResult
	if err := json.Unmarshal(raw, &full); err != nil {
		t.Fatalf("blob 内容不是合法 ToolResult JSON: %v", err)
	}
	if len(full.Content) != 1 || full.Content[0].Text == nil || *full.Content[0].Text != content {
		t.Fatalf("blob 里应是完整原文（%d 字节），got %d 字节", len(content), len(raw))
	}

	// ③ payload 是**摘要**形态（占位在、原文不在）。
	var p contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(done.Payload, &p); err != nil {
		t.Fatalf("completed payload 不可解: %v", err)
	}
	if len(p.Result.Content) != 1 || p.Result.Content[0].Text == nil {
		t.Fatalf("摘要应保留一个 text part: %+v", p.Result.Content)
	}
	if !strings.Contains(*p.Result.Content[0].Text, "[已外置：全文见 blob ") {
		t.Fatalf("重放摘要应含外置占位标记，got %q", *p.Result.Content[0].Text)
	}
	if strings.Contains(*p.Result.Content[0].Text, content) {
		t.Fatal("重放摘要里不该留着完整原文（外置没生效？）")
	}
}
