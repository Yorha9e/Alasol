package host_test

// WAL 对账**分段归组**（F1）的端到端钉子：同一个 session 跑两次 RunPrompt，第一次崩在
// 轮中（悬空的 turn.begin），第二次正常跑完 → 恢复报告**仍要报出那次崩溃**。
//
// # 为什么要有这组钉子
//
// WAL 载荷里的轮次号是"**每次 Run 内从 1 起**"→ **跨 Run 撞号**。若对账按轮次号全局归集，
// 第二次 Run 的 turn.end 会把第一次留下的**悬空 turn.begin** 判成"已收口"→ 报告谎报
// "无崩溃残留"（**假阴性**：崩溃现场被抹掉——使用者据此以为没丢东西）。
//
// 钉子的判据：**在飞轮次必须指向崩掉的那一段**（模型名可辨），而不是被后一次 Run 的同名
// 轮次顶替。

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/store/walfile"
)

// TestHubWALReportKeepsCrashAcrossRuns（F1 端到端）：同 session 两次 RunPrompt——
// 第一次**崩在轮中**（WAL 里留下悬空的 turn.begin），第二次**正常跑完** → 报告仍报 1 个
// 在飞轮次（且指向崩掉的那一段）、Crashed()=true。
//
// 造法：RunPrompt #1（正常）→ 直接往同一个 WAL 文件追加"崩溃现场"（turn.begin 无收口，
// 模拟进程在轮中被杀）→ RunPrompt #2（正常，它会写自己那一轮的 begin/end）→ 对账。
func TestHubWALReportKeepsCrashAcrossRuns(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	h, sid := newWalHub(t, walDir)

	// 第一次 RunPrompt：正常跑完（WAL 里落下第 1、2 轮的 begin/end）。
	if _, err := h.RunPrompt(context.Background(), sid, "第一次"); err != nil {
		t.Fatalf("RunPrompt #1: %v", err)
	}

	// 崩在轮中：往同一个 WAL 文件追加一条**没有收口**的 turn.begin（模拟进程在轮中被杀）。
	st, err := walfile.New(walDir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := st.Writer(sid)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(core.KindWalTurnBegin, core.WalTurnBeginPayload{
		Turn: 1, Model: "crashy-model", Messages: 4, Tools: 1,
	}); err != nil {
		t.Fatal(err)
	}
	crashText := "崩在这一句"
	if err := w.Append(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 1, Text: &crashText}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// 第二次 RunPrompt：正常跑完（**轮次号又从 1 起** → 与上面那段撞号）。
	if _, err := h.RunPrompt(context.Background(), sid, "第二次"); err != nil {
		t.Fatalf("RunPrompt #2: %v", err)
	}

	rep, err := h.WALReport(context.Background(), sid)
	if err != nil {
		t.Fatalf("WALReport: %v", err)
	}
	if rep.InFlightTurn == nil {
		t.Fatalf("上一次崩在轮中的那轮被后一次 Run 的 turn.end 抹平了（InFlightTurn=nil）——"+
			"对账假阴性：谎报\"无崩溃残留\"。Turns=%+v", rep.Turns)
	}
	if !rep.Crashed() {
		t.Error("有悬空轮次 → Crashed() 应为 true")
	}
	if rep.InFlightTurns != 1 {
		t.Errorf("InFlightTurns = %d，want 1", rep.InFlightTurns)
	}
	// 在飞轮次必须指向**崩掉的那一段**（模型名可辨），不是后一次 Run 的同名轮次。
	if rep.InFlightTurn.Model != "crashy-model" {
		t.Errorf("InFlightTurn.Model = %q，want crashy-model（后一次 Run 的轮次不该顶替它）",
			rep.InFlightTurn.Model)
	}
	if rep.InFlightTurn.Text != crashText {
		t.Errorf("InFlightTurn.Text = %q，want %q", rep.InFlightTurn.Text, crashText)
	}
	// 崩溃现场那段没有 turn.end（真的没收口）。
	if s := rep.Summary(); s == "" {
		t.Error("有崩溃残留时摘要不该为空")
	}
}

// oneTurnProvider 每次模型调用都直接收敛（一轮一次调用）——让"两次 RunPrompt = 两段"
// 这件事在计数上确定（hub 自带的 provider 第一次 Run 会发工具调用，轮数不固定）。
type oneTurnProvider struct{}

func (p *oneTurnProvider) Run(context.Context, core.ModelRequest, func(core.ProviderEvent)) (core.ModelResult, error) {
	return core.ModelResult{Text: "ok"}, nil
}

// newOneTurnWalHub 建一个每轮即收敛的 hub（带 WAL 目录）。
func newOneTurnWalHub(t *testing.T, walDir string) (*host.Hub, contracts.Ulid) {
	t.Helper()
	h, err := host.New(host.Config{
		DBPath:         filepath.Join(t.TempDir(), "wal-seg.db"),
		Model:          "scripted",
		Addr:           "127.0.0.1:0",
		Approval:       "deny-all",
		WorktreeRoot:   filepath.Join(t.TempDir(), "wts"),
		WalDir:         walDir,
		MaxSessionRows: 10,
		Provider:       &oneTurnProvider{},
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h, host.NewSession()
}

// TestHubWALReportCleanAcrossRuns（F1 的另一半，不误报）：同 session 两次 RunPrompt，
// **两次都正常跑完** → 报告无在飞轮次、Crashed()=false（分段归组不许把正常收口报成崩溃）。
func TestHubWALReportCleanAcrossRuns(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	h, sid := newOneTurnWalHub(t, walDir)

	if _, err := h.RunPrompt(context.Background(), sid, "第一次"); err != nil {
		t.Fatalf("RunPrompt #1: %v", err)
	}
	if _, err := h.RunPrompt(context.Background(), sid, "第二次"); err != nil {
		t.Fatalf("RunPrompt #2: %v", err)
	}

	rep, err := h.WALReport(context.Background(), sid)
	if err != nil {
		t.Fatalf("WALReport: %v", err)
	}
	if rep.InFlightTurn != nil {
		t.Errorf("两次都正常跑完不该有在飞轮次，got %+v", rep.InFlightTurn)
	}
	if rep.InFlightTurns != 0 {
		t.Errorf("InFlightTurns = %d，want 0", rep.InFlightTurns)
	}
	if rep.Crashed() {
		t.Errorf("两次都正常跑完不该报崩溃残留，got %+v", rep)
	}
	// WAL 文件里确实有**两段**（两次 Run 各一段：轮次号又从 1 起 → 撞号）。
	raw, err := os.ReadFile(filepath.Join(walDir, string(sid)+".wal"))
	if err != nil {
		t.Fatalf("读 WAL: %v", err)
	}
	if got := strings.Count(string(raw), `"kind":"turn.begin"`); got != 2 {
		t.Errorf("turn.begin = %d 条，want 2（两次 Run 各一轮）", got)
	}
	if len(rep.Turns) != 2 {
		t.Errorf("Turns = %d，want 2（一项 = 一段 = 一次 Run 的一轮）", len(rep.Turns))
	}
}
