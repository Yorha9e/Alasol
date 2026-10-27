package gateexec

// 静默失败的端到端复现（critic 场景）：真 runtime（真 git 快照）+ 零输出的非零退出命令。
//
// 旧行为：Run 提取不到任何失败条目 → receipt.Failures 为空 → Judge 认为"没有新增失败"
// → 门禁**放行**（静默失败绕过门禁）。
// 新行为：Run 显式标注 FailuresExtractionEmpty → Judge 按"未知失败"fail-close 处理。

import (
	"context"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core/gate"
)

// silentFailCmd 一个"静默失败"的命令：非零退出、**零输出**（OOM kill / 崩溃无输出 /
// 命令自己静默退出的典型形态）。
//
//	Windows：cmd /c exit 3     其他：bash -c exit 3
func silentFailCmd() string { return "exit 3" }

// TestRunSilentFailureIsBlockedEndToEnd：静默失败必须被门禁拦下。
func TestRunSilentFailureIsBlockedEndToEnd(t *testing.T) {
	repo, commit := initRepo(t)
	dir, cleanup, err := Snapshot(context.Background(), repo, commit)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer cleanup()

	cmd := silentFailCmd()
	r, err := Run(context.Background(), dir, cmd)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.ExitCode == 0 {
		t.Fatalf("静默失败命令应非零退出（LogTail=%q）", r.LogTail)
	}
	if len(r.Failures) != 0 {
		t.Fatalf("静默失败不应提取出失败条目：%v", r.Failures)
	}
	if !r.FailuresExtractionEmpty {
		t.Fatal("Run 应显式标注 FailuresExtractionEmpty（判定侧不靠空=未知隐式推断）")
	}

	// ① 非空 baseline + 静默失败 → blocked。
	baseline := &gate.Baseline{}
	known := gate.Receipt{Command: cmd, ExitCode: 1, Failures: []string{"FAIL: known_x"}}
	if _, err := gate.Judge(&known, baseline); err != nil {
		t.Fatalf("建基线: %v", err)
	}
	if len(baseline.Entries) == 0 {
		t.Fatal("baseline 应有 entries")
	}

	v, err := gate.Judge(&r, baseline)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v.Kind != gate.VerdictBlocked {
		t.Fatalf("静默失败应 VerdictBlocked，得到 %v（new=%v）", v.Kind, v.NewFailures)
	}
	if len(v.NewFailures) != 1 || !strings.Contains(v.NewFailures[0], "未产出可归一化的失败条目") {
		t.Fatalf("失败说明应讲清是未知失败：%v", v.NewFailures)
	}
}

// TestRunSilentFailureRecordThenKnownEndToEnd：记录模式建基线 → 同款静默失败再跑 → passed
// （红仓能建基线；已知失败不误伤）。
func TestRunSilentFailureRecordThenKnownEndToEnd(t *testing.T) {
	repo, commit := initRepo(t)
	dir, cleanup, err := Snapshot(context.Background(), repo, commit)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer cleanup()

	cmd := silentFailCmd()
	r, err := Run(context.Background(), dir, cmd)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// ② 记录模式 → recorded，且 baseline 里有未知失败的指纹。
	baseline := &gate.Baseline{}
	v, err := gate.Judge(&r, baseline)
	if err != nil {
		t.Fatalf("Judge(record): %v", err)
	}
	if v.Kind != gate.VerdictRecorded {
		t.Fatalf("记录模式应 VerdictRecorded，得到 %v", v.Kind)
	}
	want := gate.FailureFingerprint(gate.UnknownFailureEntry(r.ExitCode))
	found := false
	for _, e := range baseline.Entries {
		if e.Hash == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("baseline 应含未知失败指纹 %s（entries=%+v）", want, baseline.Entries)
	}

	// ③ 再跑同款静默失败 → passed（已知）。
	v2, err := gate.Judge(&r, baseline)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v2.Kind != gate.VerdictPassed {
		t.Fatalf("已记录的静默失败应 VerdictPassed，得到 %v（new=%v）", v2.Kind, v2.NewFailures)
	}
}
