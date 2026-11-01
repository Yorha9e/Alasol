package host_test

// WAL 预写日志（v4-4-B）的宿主集成钉子：装配 → 落文件 → 对账报告；以及**酸测试**
//（未配置 = 一个文件都不建）。
//
// 口径（以 host/wal.go 与 runtime/store/walfile 为准）：
//   - **WAL 不是事实源**（F5 不受影响）：事件流仍是唯一事实源，WAL 可丢弃/可截断/可 GC；
//   - 恢复报告是**观察面不是写回面**（对账只读两侧，不写事件流、不补 delta）；
//   - 关（Config.WalDir 空）= 一个文件都不建 = 零行为变化。

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/store/walfile"
)

// walHubProvider 是带流式 delta 的脚本 provider：第一轮发 delta + 一次工具调用，
// 第二轮发 delta 后收尾（两轮都留 WAL 痕迹）。
type walHubProvider struct {
	calls int
}

func (p *walHubProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if p.calls == 1 {
		// 第 1 轮：1 条正文 delta + 1 条思维链 delta，然后发起一次 read 工具调用。
		emit(core.ProviderEvent{Kind: core.KindTextDelta, Text: "第一段"})
		emit(core.ProviderEvent{Kind: core.KindReasoningDelta, Reasoning: "想一下"})
		tc := core.ToolCallReq{ID: contracts.ProviderId("tc_WALHUB"), Name: "read", Args: json.RawMessage(`{"path":"x"}`)}
		emit(core.ProviderEvent{Kind: core.KindToolCallStart, ToolCall: &tc})
		emit(core.ProviderEvent{Kind: core.KindDone})
		return core.ModelResult{Text: "调工具", ToolCalls: []core.ToolCallReq{tc}}, nil
	}
	// 第 2 轮（收敛）：1 条正文 delta。
	emit(core.ProviderEvent{Kind: core.KindTextDelta, Text: "收尾"})
	emit(core.ProviderEvent{Kind: core.KindDone, Text: "收尾"})
	return core.ModelResult{Text: "收尾"}, nil
}

// newWalHub 建一个带 WAL 的 hub（脚本 provider + 放行 read）。
func newWalHub(t *testing.T, walDir string) (*host.Hub, contracts.Ulid) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "wal-hub.db")
	h, err := host.New(host.Config{
		DBPath:         dbPath,
		Model:          "scripted",
		Addr:           "127.0.0.1:0",
		Approval:       "deny-all",
		Allow:          []string{"read"},
		Provider:       &walHubProvider{},
		WorktreeRoot:   filepath.Join(t.TempDir(), "wts"),
		WalDir:         walDir,
		MaxSessionRows: 10,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h, host.NewSession()
}

// TestHubWALWritesAndReconcilesClean（F1）：跑完一轮 → WAL 文件在、四类记录齐全；
// 报告**不误报**（全部轮次有 end、无孤儿）。
func TestHubWALWritesAndReconcilesClean(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	h, sid := newWalHub(t, walDir)

	if _, err := h.RunPrompt(context.Background(), sid, "跑一轮"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if h.WALDir() != walDir {
		t.Fatalf("WALDir() = %q，want %q", h.WALDir(), walDir)
	}

	// per-session 文件：<walDir>/<session>.wal。
	path := filepath.Join(walDir, string(sid)+".wal")
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("WAL 文件应存在: %v", err)
	}
	if fi.Size() == 0 {
		t.Fatal("WAL 文件不该为空")
	}

	rep, err := h.WALReport(context.Background(), sid)
	if err != nil {
		t.Fatalf("WALReport: %v", err)
	}
	if !rep.WALExists {
		t.Fatal("报告应说 WAL 存在")
	}
	if len(rep.Turns) != 2 {
		t.Fatalf("Turns = %d，want 2", len(rep.Turns))
	}
	for _, tn := range rep.Turns {
		if !tn.Ended {
			t.Errorf("第 %d 轮应有 turn.end（正常收口）", tn.Turn)
		}
	}
	if rep.InFlightTurn != nil {
		t.Errorf("正常跑完不该有在飞轮次，got %+v", rep.InFlightTurn)
	}
	if len(rep.OrphanBlobToolCalls) != 0 {
		t.Errorf("不该有孤儿候选（本用例没外置 blob），got %v", rep.OrphanBlobToolCalls)
	}
	if rep.DeltaTotal != 3 { // 轮1: 1 text + 1 reasoning；轮2: 1 text
		t.Errorf("DeltaTotal = %d，want 3", rep.DeltaTotal)
	}
}

// TestHubWALReportDetectsCrashResidue（F2）：**模拟崩溃**——seed 一份"没有对应 completed"
// 的 WAL（turn.begin + delta + blob.plan，无 turn.end）→ 报告列出在飞轮次与孤儿。
func TestHubWALReportDetectsCrashResidue(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	h, sid := newWalHub(t, walDir)

	// 先跑一轮（让 session 在事件流里有真实事件：user.prompt / context.assembled /
	// model.turn / intent / completed——**但 completed 的 tool_call_id 不是下面 seed 的那个**）。
	if _, err := h.RunPrompt(context.Background(), sid, "跑一轮"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	// 再直接往同一个 WAL 文件追加"崩溃现场"（这就是"崩溃后磁盘上剩下的东西"）。
	st, err := walfile.New(walDir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := st.Writer(sid)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 99, Model: "crashy-model", Messages: 7, Tools: 3}); err != nil {
		t.Fatal(err)
	}
	crashText := "崩在这儿"
	if err := w.Append(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 99, Text: &crashText}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 99, ToolCallID: "tc_CRASH_ORPHAN", ResultBytes: 9000, Threshold: 64}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	rep, err := h.WALReport(context.Background(), sid)
	if err != nil {
		t.Fatalf("WALReport: %v", err)
	}
	if rep.InFlightTurn == nil {
		t.Fatal("应识别出在飞轮次（第 99 轮只有 begin）")
	}
	if rep.InFlightTurn.Turn != 99 || rep.InFlightTurn.Model != "crashy-model" {
		t.Errorf("InFlightTurn = %+v，want turn=99 model=crashy-model", rep.InFlightTurn)
	}
	if rep.InFlightTurn.Text != "崩在这儿" {
		t.Errorf("InFlightTurn.Text = %q，want 崩在这儿（delta 拼得回来）", rep.InFlightTurn.Text)
	}
	if len(rep.OrphanBlobToolCalls) != 1 || rep.OrphanBlobToolCalls[0] != "tc_CRASH_ORPHAN" {
		t.Fatalf("孤儿候选 = %v，want [tc_CRASH_ORPHAN]", rep.OrphanBlobToolCalls)
	}
	// 已正常收口的那两轮不误报。
	if !rep.Turns[0].Ended || !rep.Turns[1].Ended {
		t.Errorf("前两轮不该被误报成在飞：%+v", rep.Turns)
	}
	// 摘要一行说得清（resume 时打印的就是它）。
	s := rep.Summary()
	for _, want := range []string{"第 99 轮", "crashy-model", "tc_CRASH_ORPHAN", "观察面不是写回面"} {
		if !strings.Contains(s, want) {
			t.Errorf("摘要缺 %q:\n%s", want, s)
		}
	}
}

// TestHubWALBlobPlanNotOrphanAfterCompleted（F3）：外置真的发生了 → completed 带 binary_ref
// → 报告**不误报**（有对应 completed + binary_ref 的 plan 两个名单都不进）。
func TestHubWALBlobPlanNotOrphanAfterCompleted(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	blobDir := filepath.Join(t.TempDir(), "blobs")
	dbPath := filepath.Join(t.TempDir(), "wal-blob.db")

	// 造一个真的大文件（read 的结果 > 1024 字节阈值 → 必外置 → blob.plan + BinaryRef）。
	bigPath := filepath.Join(t.TempDir(), "big.txt")
	if err := os.WriteFile(bigPath, []byte(strings.Repeat("B", 5000)), 0o600); err != nil {
		t.Fatal(err)
	}
	h, err := host.New(host.Config{
		DBPath:         dbPath,
		Model:          "scripted",
		Addr:           "127.0.0.1:0",
		Approval:       "deny-all",
		Allow:          []string{"read"},
		WorktreeRoot:   filepath.Join(t.TempDir(), "wts"),
		WalDir:         walDir,
		BlobDir:        blobDir,
		BlobMaxInline:  1024, // 小阈值 → 必外置
		MaxSessionRows: 10,
		Provider:       &walBlobProvider{path: bigPath},
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "读大文件"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	// 事件流里那条 completed 必须带 binary_ref（外置真的发生了）。
	var ref contracts.Ulid
	found := false
	for _, ev := range readAllEvents(t, dbPath) {
		if ev.Kind == loop.KindToolCallCompleted && ev.BinaryRef != nil {
			ref = *ev.BinaryRef
			found = true
		}
	}
	if !found {
		t.Fatal("本用例应真的外置（completed 带 binary_ref）——否则钉子失去意义")
	}
	if ref == "" {
		t.Fatal("binary_ref 不应为空")
	}

	rep, err := h.WALReport(context.Background(), sid)
	if err != nil {
		t.Fatalf("WALReport: %v", err)
	}
	if len(rep.OrphanBlobToolCalls) != 0 {
		t.Errorf("有 completed + binary_ref 不该报孤儿，got %v", rep.OrphanBlobToolCalls)
	}
	if len(rep.StaleBlobPlans) != 0 {
		t.Errorf("有 binary_ref 不该报作废计划，got %v", rep.StaleBlobPlans)
	}
}

// walBlobProvider 第一轮 read 一个**真的大文件**（read 的结果必超阈值 → 外置），第二轮收尾。
type walBlobProvider struct {
	path  string
	calls int
}

func (p *walBlobProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if p.calls == 1 {
		args, _ := json.Marshal(map[string]string{"path": p.path})
		tc := core.ToolCallReq{ID: contracts.ProviderId("tc_WALBLOB"), Name: "read", Args: args}
		emit(core.ProviderEvent{Kind: core.KindDone})
		return core.ModelResult{Text: "读", ToolCalls: []core.ToolCallReq{tc}}, nil
	}
	emit(core.ProviderEvent{Kind: core.KindDone, Text: "完"})
	return core.ModelResult{Text: "完"}, nil
}

// TestHubWALOffCreatesNoFiles（F4，酸测试）：**未配置 WAL = 一个文件都不建**（hub 起来 +
// 跑完一轮，WAL 目录里零条目；loop.Config.WalSink 恒 nil）。
func TestHubWALOffCreatesNoFiles(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal") // 故意**不**配给 hub
	dbPath := filepath.Join(t.TempDir(), "wal-off.db")
	h, err := host.New(host.Config{
		DBPath:         dbPath,
		Model:          "scripted",
		Addr:           "127.0.0.1:0",
		Approval:       "deny-all",
		Allow:          []string{"read"},
		WorktreeRoot:   filepath.Join(t.TempDir(), "wts"),
		MaxSessionRows: 10,
		Provider:       &walHubProvider{},
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "跑一轮"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if h.WALDir() != "" {
		t.Errorf("未配置时 WALDir() 应为空，got %q", h.WALDir())
	}
	// 目录**根本不该被建出来**（未配置 = 零行为变化：连空目录都不留）。
	if _, err := os.Stat(walDir); !os.IsNotExist(err) {
		t.Errorf("未配置 WAL 时不该出现 WAL 目录（stat err = %v）", err)
	}
	// 报告如实说"没有 WAL"（不是错误）。
	rep, rerr := h.WALReport(context.Background(), sid)
	if rerr != nil {
		t.Fatalf("WALReport 对未配置 WAL 不该报错: %v", rerr)
	}
	if rep.WALExists {
		t.Error("wal_exists 应为 false")
	}
}

// TestHubWALCloseIsIdempotent（F5）：hub.Close 收口 WAL（幂等；fd 不泄漏），且**关闭后
// WAL 文件仍在盘上**——它是崩溃恢复面：进程没了，记录还在。
func TestHubWALCloseIsIdempotent(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	h, sid := newWalHub(t, walDir)
	if _, err := h.RunPrompt(context.Background(), sid, "跑一轮"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("二次 Close 应幂等: %v", err)
	}
	if _, err := os.Stat(filepath.Join(walDir, string(sid)+".wal")); err != nil {
		t.Fatalf("hub 关闭后 WAL 文件应仍在（崩溃恢复面：进程没了记录还在）: %v", err)
	}
}

// TestHubWALForkSessionHasTraces（F8，critic F7）：/btw 旁支（fork session）的 loop
// **同样挂 WAL**（与 loopFor 同口径）——旁支崩了也要能查"崩在第几轮、吐过什么"。
//
// 钉子：fork loop 漏传 WalSink → 旁支 session 的 WAL 文件不在 → 本用例红。
func TestHubWALForkSessionHasTraces(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	h, sid := newWalHub(t, walDir)
	if _, err := h.RunPrompt(context.Background(), sid, "跑一轮"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	res, err := h.RunBtw(context.Background(), sid, "旁支问题")
	if err != nil {
		t.Fatalf("RunBtw: %v", err)
	}
	if res.ForkSessionID == "" {
		t.Fatal("RunBtw 没给旁支 session id")
	}
	path := filepath.Join(walDir, string(res.ForkSessionID)+".wal")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("旁支 session 的 WAL 应存在（fork loop 漏挂 WalSink？）: %v", err)
	}
	if !strings.Contains(string(raw), `"kind":"turn.begin"`) {
		t.Errorf("旁支 WAL 应有 turn.begin 记录:\n%s", raw)
	}
}

// failProvider 是**总是失败**的 provider（模拟模型调用报错：429/超时/端点 500）。
type failProvider struct{}

func (p *failProvider) Run(context.Context, core.ModelRequest, func(core.ProviderEvent)) (core.ModelResult, error) {
	return core.ModelResult{}, errors.New("provider: 端点 500")
}

// TestHubWALProviderFailureIsNotCrashResidue（F9，critic F2 的**端到端**钉子）：模型调用
// 失败（**普通失败**，不是崩溃）→ 恢复报告**不**说"疑似崩溃残留"（Crashed()=false）、
// **无在飞轮次**。
//
// 端到端链：loop 写 turn.failed → walfile 对账把它与"崩溃残留"分开（begin 后跟 failed =
// 正常失败）。钉子：去掉 turn.failed 写入 → WAL 里只剩悬空的 turn.begin → 被当成在飞
// 轮次 → 本用例红（这就是"普通失败被误报成疑似崩溃"）。
func TestHubWALProviderFailureIsNotCrashResidue(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	h, err := host.New(host.Config{
		DBPath:         filepath.Join(t.TempDir(), "wal-fail.db"),
		Model:          "scripted",
		Addr:           "127.0.0.1:0",
		Approval:       "deny-all",
		WorktreeRoot:   filepath.Join(t.TempDir(), "wts"),
		WalDir:         walDir,
		MaxSessionRows: 10,
		Provider:       &failProvider{},
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	sid := host.NewSession()

	if _, err := h.RunPrompt(context.Background(), sid, "跑一轮"); err == nil {
		t.Fatal("provider 失败时 RunPrompt 应返错")
	}

	rep, err := h.WALReport(context.Background(), sid)
	if err != nil {
		t.Fatalf("WALReport: %v", err)
	}
	if rep.InFlightTurn != nil {
		t.Errorf("模型调用失败（普通失败）不该有在飞轮次，got %+v", rep.InFlightTurn)
	}
	if rep.Crashed() {
		t.Errorf("模型调用失败不该报\"疑似崩溃残留\"，got %+v", rep)
	}
	if s := rep.Summary(); strings.Contains(s, "疑似崩溃残留") {
		t.Errorf("摘要不该说疑似崩溃残留:\n%s", s)
	}
}

// TestConfigRejectsBadWALDir（F6）：WAL 目录建不出来（路径是个**文件**）→ 拒绝启动
// （"显式要的能力没起来"，比静默不外记好排查——与 blobfs 同口径）。
func TestConfigRejectsBadWALDir(t *testing.T) {
	dir := t.TempDir()
	fileAsDir := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(fileAsDir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := host.New(host.Config{
		DBPath:         filepath.Join(dir, "db.sqlite"),
		Model:          "scripted",
		Addr:           "127.0.0.1:0",
		WorktreeRoot:   filepath.Join(dir, "wts"),
		WalDir:         fileAsDir,
		MaxSessionRows: 10,
		Provider:       &walHubProvider{},
	})
	if err == nil {
		t.Fatal("WAL 目录建不出来应拒绝启动")
	}
	if !strings.Contains(err.Error(), "WAL") {
		t.Errorf("错误应点名 WAL，got %v", err)
	}
}

// TestConfigWhitespaceWalDirIsOff（F7）：WalDir 只有空白 → 归一成"关"（零行为变化：
// 不建目录、不建任何文件）。走公开装配面（host.New + RunPrompt）钉 observable 行为。
func TestConfigWhitespaceWalDirIsOff(t *testing.T) {
	probe := filepath.Join(t.TempDir(), "should-not-exist")
	dbPath := filepath.Join(t.TempDir(), "wal-ws.db")
	h, err := host.New(host.Config{
		DBPath:         dbPath,
		Model:          "scripted",
		Addr:           "127.0.0.1:0",
		Approval:       "deny-all",
		Allow:          []string{"read"},
		WorktreeRoot:   filepath.Join(t.TempDir(), "wts"),
		WalDir:         "   ", // 只有空白 = 关
		MaxSessionRows: 10,
		Provider:       &walHubProvider{},
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "跑一轮"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if h.WALDir() != "" {
		t.Errorf("空白 WalDir 应归一成关（WALDir() 空），got %q", h.WALDir())
	}
	if _, err := os.Stat(probe); !os.IsNotExist(err) {
		t.Errorf("空白 WalDir 不该建出任何目录（stat err = %v）", err)
	}
}
