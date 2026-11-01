package host_test

// WAL 在 **ctx 取消**路径上的端到端钉子（F2）：Run 被取消 → WAL 以 **turn.failed** 收尾
// → 恢复报告**不误报**"疑似崩溃残留"。
//
// # 为什么要有这组钉子
//
// 对账口径："在飞轮次 = 有 turn.begin、无 turn.end **也无 turn.failed**"。ctx 取消（超时/
// 用户中断/宿主关停）是**正常收口**，不是崩溃——它也写 turn.failed。若这条路径漏写，
// WAL 里就只剩悬空的 turn.begin → 恢复报告把一次正常的取消报成"疑似崩溃残留"
// （**假阳性**：使用者据此以为丢了东西、要去人工恢复，其实只是被取消了）。

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
)

// cancelProvider 在模型调用时**取消 ctx** 并返回 ctx.Err()（模拟 Run 被取消）。
type cancelProvider struct {
	cancel context.CancelFunc
}

func (p *cancelProvider) Run(ctx context.Context, _ core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.cancel()
	return core.ModelResult{}, ctx.Err()
}

// TestHubWALContextCancelIsNotCrashResidue（F2 端到端）：Run 被 ctx 取消 →
//
//	① WAL 以 turn.failed 收尾（没有 turn.end）；
//	② 恢复报告**无在飞轮次**、Crashed()=false（不误报"疑似崩溃残留"）。
func TestHubWALContextCancelIsNotCrashResidue(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := host.New(host.Config{
		DBPath:         filepath.Join(t.TempDir(), "wal-cancel.db"),
		Model:          "scripted",
		Addr:           "127.0.0.1:0",
		Approval:       "deny-all",
		WorktreeRoot:   filepath.Join(t.TempDir(), "wts"),
		WalDir:         walDir,
		MaxSessionRows: 10,
		Provider:       &cancelProvider{cancel: cancel},
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	sid := host.NewSession()

	if _, err := h.RunPrompt(ctx, sid, "跑一轮"); err == nil {
		t.Fatal("ctx 被取消时 RunPrompt 应返错")
	} else if !errors.Is(err, context.Canceled) {
		t.Errorf("RunPrompt 错误 = %v，want context.Canceled", err)
	}

	// ① WAL 侧：以 turn.failed 收尾，没有 turn.end。
	raw, rerr := os.ReadFile(filepath.Join(walDir, string(sid)+".wal"))
	if rerr != nil {
		t.Fatalf("读 WAL: %v", rerr)
	}
	text := string(raw)
	if !strings.Contains(text, `"kind":"turn.failed"`) {
		t.Errorf("被取消的 Run 应在 WAL 里留下 turn.failed:\n%s", text)
	}
	if strings.Contains(text, `"kind":"turn.end"`) {
		t.Errorf("被取消的轮次不该有 turn.end:\n%s", text)
	}

	// ② 对账侧：不误报。
	rep, err := h.WALReport(context.Background(), sid)
	if err != nil {
		t.Fatalf("WALReport: %v", err)
	}
	if rep.InFlightTurn != nil {
		t.Errorf("被取消的 Run 不该有在飞轮次，got %+v", rep.InFlightTurn)
	}
	if rep.InFlightTurns != 0 {
		t.Errorf("InFlightTurns = %d，want 0", rep.InFlightTurns)
	}
	if rep.Crashed() {
		t.Errorf("被取消不该报崩溃残留，got %+v", rep)
	}
	if s := rep.Summary(); strings.Contains(s, "疑似崩溃残留") {
		t.Errorf("摘要不该说疑似崩溃残留:\n%s", s)
	}
}
