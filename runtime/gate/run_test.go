package gateexec

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core/gate"
)

// TestRunPassingCommand：跑一个退出码 0 的命令，receipt 事实齐全，四查全过。
func TestRunPassingCommand(t *testing.T) {
	repo, commit := initRepo(t)
	dir, cleanup, err := Snapshot(context.Background(), repo, commit)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer cleanup()

	cmd := passCmd()
	r, err := Run(context.Background(), dir, cmd)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.ExitCode != 0 {
		t.Errorf("ExitCode = %d，期望 0（LogTail=%q）", r.ExitCode, r.LogTail)
	}
	if r.Commit != commit {
		t.Errorf("Commit = %q，期望快照 commit %q", r.Commit, commit)
	}
	if r.ObservedCommit != commit {
		t.Errorf("ObservedCommit = %q，期望 %q（未动 HEAD 时应相等）", r.ObservedCommit, commit)
	}
	if r.Command != cmd {
		t.Errorf("Command = %q，期望 %q", r.Command, cmd)
	}
	if len(r.Failures) != 0 {
		t.Errorf("退出码 0 时不应有 Failures：%v", r.Failures)
	}
	// 干净 receipt 应过 core/gate 四查。
	if err := gate.Verify(&r, gate.VerifyRequest{TipCommit: commit, Config: gate.Config{Command: cmd}}); err != nil {
		t.Errorf("干净 receipt 应过四查：%v", err)
	}
}

// TestRunNonZeroExitIsNotError：退出码非零是事实，不是 error（V2①）；Failures 被提取。
func TestRunNonZeroExitIsNotError(t *testing.T) {
	repo, commit := initRepo(t)
	dir, cleanup, _ := Snapshot(context.Background(), repo, commit)
	defer cleanup()

	r, err := Run(context.Background(), dir, failCmd())
	if err != nil {
		t.Fatalf("非零退出不应返回 error：%v", err)
	}
	if r.ExitCode != 3 {
		t.Errorf("ExitCode = %d，期望 3", r.ExitCode)
	}
	if len(r.Failures) == 0 {
		t.Error("失败命令应提取出 Failures")
	}
}

// TestRunInjectsConventionEnv：验证 V3 约定 env 注入（CI_WORKTREE / CI_COMMIT / CI_PORT_BASE）。
func TestRunInjectsConventionEnv(t *testing.T) {
	repo, commit := initRepo(t)
	dir, cleanup, _ := Snapshot(context.Background(), repo, commit)
	defer cleanup()

	r, err := Run(context.Background(), dir, echoEnvCmd())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	lines := strings.Split(strings.TrimRight(r.LogTail, "\r\n"), "\n")
	if len(lines) < 3 {
		t.Fatalf("期望 3 行输出，得到 %q", r.LogTail)
	}
	if got := strings.TrimSpace(lines[0]); got != commit {
		t.Errorf("CI_COMMIT = %q，期望 %q", got, commit)
	}
	if got := strings.TrimSpace(lines[1]); !strings.Contains(got, "alasol-gate-snap") {
		t.Errorf("CI_WORKTREE = %q，应指向快照临时目录（含 alasol-gate-snap）", got)
	}
	port, err := strconv.Atoi(strings.TrimSpace(lines[2]))
	if err != nil {
		t.Fatalf("CI_PORT_BASE 不是整数：%q（%v）", lines[2], err)
	}
	if port < portBaseLow || port >= portBaseLow+portBaseSpan {
		t.Errorf("CI_PORT_BASE = %d，期望在 [%d, %d)", port, portBaseLow, portBaseLow+portBaseSpan)
	}
}

// TestRunTruncatesLogTail：V5——LogTail 只留尾部 ≤64KB（首标记被截掉、尾标记保留）。
func TestRunTruncatesLogTail(t *testing.T) {
	repo, commit := initRepo(t)
	dir, cleanup, _ := Snapshot(context.Background(), repo, commit)
	defer cleanup()

	r, err := Run(context.Background(), dir, bigOutputCmd())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(r.LogTail) > maxLogTail {
		t.Errorf("LogTail 未截断：%d > %d", len(r.LogTail), maxLogTail)
	}
	if len(r.LogTail) == 0 {
		t.Fatal("LogTail 为空")
	}
	if !strings.Contains(r.LogTail, "line 9000") {
		t.Error("LogTail 应保留尾部（含最后的 line 9000）")
	}
	if strings.Contains(r.LogTail, "SENTINEL_FIRST") {
		t.Error("LogTail 应已截掉头部（不应含 SENTINEL_FIRST）")
	}
}

// TestRunRecordsElapsed：V5——ElapsedMS 必记，且反映真实耗时。
func TestRunRecordsElapsed(t *testing.T) {
	repo, commit := initRepo(t)
	dir, cleanup, _ := Snapshot(context.Background(), repo, commit)
	defer cleanup()

	r, err := Run(context.Background(), dir, sleepCmd(300))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.ElapsedMS <= 0 {
		t.Error("ElapsedMS 应 > 0")
	}
	if r.ElapsedMS < 100 {
		t.Errorf("ElapsedMS = %d，命令应跑了约数百 ms（下限 100）", r.ElapsedMS)
	}
	if r.StartedAt.IsZero() {
		t.Error("StartedAt 应被记录")
	}
}

// TestRunObservesHeadChange：V2③——命令在跑期间移动 HEAD，ObservedCommit 会 ≠ Commit，被四查③抓住。
func TestRunObservesHeadChange(t *testing.T) {
	repo, commit1 := initRepo(t)
	commit2 := commitFile(t, repo, "a.txt", "committed-a-v2", "v2")

	dir, cleanup, _ := Snapshot(context.Background(), repo, commit1)
	defer cleanup()

	// 命令在快照里 checkout 到 commit2 → HEAD 从 commit1 移到 commit2。
	moveCmd := "git checkout -q " + commit2
	r, err := Run(context.Background(), dir, moveCmd)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.Commit != commit1 {
		t.Errorf("Commit 应为快照 commit1 %q，得到 %q", commit1, r.Commit)
	}
	if r.ObservedCommit != commit2 {
		t.Errorf("ObservedCommit 应捕获到 HEAD 移到 commit2 %q，得到 %q", commit2, r.ObservedCommit)
	}
	if r.ObservedCommit == r.Commit {
		t.Fatal("命令移动了 HEAD，ObservedCommit 不应等于 Commit")
	}
	// 四查③ 应拒绝这个 receipt（即便 tip 还是 commit1、命令匹配）。
	err = gate.Verify(&r, gate.VerifyRequest{TipCommit: commit1, Config: gate.Config{Command: moveCmd}})
	if err == nil || !strings.Contains(err.Error(), "观测 commit") {
		t.Errorf("四查③ 应因 observed≠commit 拒绝，得到 %v", err)
	}
}

// TestRunEmptyCommand：空命令是编程错误，直接报错不跑。
func TestRunEmptyCommand(t *testing.T) {
	repo, commit := initRepo(t)
	dir, cleanup, _ := Snapshot(context.Background(), repo, commit)
	defer cleanup()
	if _, err := Run(context.Background(), dir, "   "); err == nil {
		t.Error("空命令应报错（W9：无默认）")
	}
}

// TestRunContextCancelled：ctx 取消 → Run 返回 error（基础设施失败，非"命令失败"）。
func TestRunContextCancelled(t *testing.T) {
	repo, commit := initRepo(t)
	dir, cleanup, _ := Snapshot(context.Background(), repo, commit)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Run(ctx, dir, passCmd()); err == nil {
		t.Error("已取消的 ctx 应使 Run 报错")
	}
}
