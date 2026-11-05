package host

// L2 per-session 执行上下文 · 件 2 的**宿主内部**钉子（critic P2-1）。
//
// 为什么放内部测试：宿主层的告警出口是 **hubLog**（包级变量，外部测试包换不掉——
// 只有包内测试能把它换成捕获用的 handler）。

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// wsWriteProvider 是最小 provider：第一轮发起一次 write（相对路径），第二轮收尾。
type wsWriteProvider struct {
	calls int
}

func (p *wsWriteProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if p.calls == 1 {
		tc := core.ToolCallReq{
			ID:   contracts.ProviderId("toolu_WS_01"),
			Name: "write",
			Args: json.RawMessage(`{"path":"face.txt","content":"内容"}`),
		}
		emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 1, OutputTokens: 1}}, nil
	}
	emit(core.ProviderEvent{Kind: "text.delta", Text: "写完了"})
	emit(core.ProviderEvent{Kind: "done"})
	return core.ModelResult{Text: "写完了", Usage: core.Usage{InputTokens: 1, OutputTokens: 1}}, nil
}

// TestExecWorkspaceWarnsOnNonLocalExecPlane（critic P2-1）：**换执行面重启**后，老
// session 的工作区**照常生效**（不硬拒——为这点拒跑整个宿主太不合算，旧会话全废），
// 但要**告警**：per-session 工作区在 sandbox/remote 执行面上语义有限
// （工作区只是路径锚点；隔离与远端语义由执行面决定）。
//
// 同时钉住"工作区在沙箱允许集内**可写**"——换面不是"把会话打死"，是"语义变窄"。
func TestExecWorkspaceWarnsOnNonLocalExecPlane(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("沙箱后端只在 Windows 实现（Linux/mac 的 seatbelt/bwrap 是预留挂点，H9）")
	}
	dbPath := filepath.Join(t.TempDir(), "events.db")
	ws := t.TempDir()

	// ① local 执行面下建带工作区的 session（落一条 config.changed{exec}）。
	h1, err := New(Config{
		DBPath:   dbPath,
		Model:    "scripted",
		Allow:    []string{"write"},
		CWD:      t.TempDir(),
		Provider: &wsWriteProvider{},
	})
	if err != nil {
		t.Fatalf("host.New(local): %v", err)
	}
	sid, err := h1.CreateSession(SessionSpec{CWD: ws})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := h1.Close(); err != nil {
		t.Fatalf("Close(local): %v", err)
	}

	// ② 换执行面重启（sandbox）——**不硬拒**，但要告警。
	var buf safeLogBuf
	old := hubLog
	hubLog = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	defer func() { hubLog = old }()

	h2, err := New(Config{
		DBPath:           dbPath,
		Model:            "scripted",
		Allow:            []string{"write"},
		CWD:              t.TempDir(),
		Provider:         &wsWriteProvider{},
		Sandbox:          true,
		SandboxWriteDirs: []string{ws}, // 工作区在沙箱允许集内 → 仍可写
	})
	if err != nil {
		t.Fatalf("host.New(sandbox): %v", err)
	}
	defer h2.Close()

	// 触发装配（loopFor → 解析会话工作区 → 告警）。
	if _, err := h2.RunPrompt(context.Background(), sid, "写个文件"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if !strings.Contains(buf.String(), "per-session 工作区在 sandbox/remote 执行面上语义有限") {
		t.Fatalf("换执行面后该告警\"per-session 工作区在 sandbox/remote 执行面上语义有限\"，日志 = %q", buf.String())
	}
	// 工作区在沙箱允许集内 → 文件仍落在工作区（换面不是把会话打死）。
	if _, err := os.Stat(filepath.Join(ws, "face.txt")); err != nil {
		t.Fatalf("文件应仍落在会话工作区 %s: %v", ws, err)
	}
}
