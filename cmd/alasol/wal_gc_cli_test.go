package main

// `alasol wal-gc` 子命令的端到端钉子（缺口 5 第一件 CLI 面；形态照 blob_gc_cli_test.go）。
//
// 口径：
//   - 单行 JSON 报告到 stdout（照 `alasol wal` / `alasol blob-gc` 的输出口径），错误到 stderr；
//   - dir 读配置文件的 [wal].dir（与 `alasol wal`/run/serve 同一条配置面）；
//   - **-retention 必给**：缺省 0 / 负值 → 退出码 2 + 点名 -retention（龄期没有安全默认，
//     与库层 walfile.GC 同口径；注意与 blob-gc 的"0 = 只安全两扫"不同——那边 0 是合法策略，
//     这边"没给龄期"不是策略）；
//   - [wal].dir 未配置/目录不存在 → 退出码 2 + 点名（不静默"扫了 0 个"的假成功）；
//   - -dry-run 只报不删（报告如实）；
//   - **诚实边界**：CLI 不读事件库 → 空事实对账 → 有 blob.plan 的 WAL 恒判脏保守不删。
//
// 变异自证：
//   - CLI 去掉 retention<=0 拒绝 → 缺省/负值钉子红；
//   - CLI 的 retention 映射丢值（传 0 进库）→ 真跑钉子红（库层报错 → 退出码 1）。

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/walfile"
)

// writeWalGCConfig 写一份只含 [wal].dir 的 TOML 配置（Windows 路径转义反斜杠）。
func writeWalGCConfig(t *testing.T, dir, walDir string) string {
	t.Helper()
	cfgPath := filepath.Join(dir, "config.toml")
	toml := "[wal]\ndir = \"" + strings.ReplaceAll(walDir, "\\", "\\\\") + "\"\n"
	if err := os.WriteFile(cfgPath, []byte(toml), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	return cfgPath
}

// walGCLine 造一行 WAL NDJSON（测试 seed 用；与 Writer 写出的形状一致）。
func walGCLine(t *testing.T, kind string, payload any) string {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	b, err := json.Marshal(core.WalRecord{Kind: kind, TS: "2026-11-02T00:00:00Z", Payload: raw})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return string(b)
}

// seedWalGCFile 在 walDir 下写 `<sid>.wal` 并把 mtime 钉成 age 之前（超龄靶子）。
func seedWalGCFile(t *testing.T, walDir string, sid contracts.Ulid, age time.Duration, lines ...string) string {
	t.Helper()
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("建 WAL 目录: %v", err)
	}
	path := filepath.Join(walDir, string(sid)+".wal")
	content := ""
	if len(lines) > 0 {
		content = strings.Join(lines, "\n") + "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("写 WAL: %v", err)
	}
	when := time.Now().Add(-age)
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	return path
}

// TestWalGCCLIRequiresExplicitRetention：-retention 缺省（0）/显式 0/负值 → 退出码 2 +
// 点名 -retention；失败时 stdout 为空（不打假报告）。
func TestWalGCCLIRequiresExplicitRetention(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	cfgPath := writeWalGCConfig(t, dir, walDir)

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"缺省（没给）", nil},
		{"显式 0", []string{"-retention", "0"}},
		{"负值", []string{"-retention", "-1h"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := captureStdout(t)
			errOut := captureStderr(t)
			code := walGCMain(append([]string{"-config", cfgPath}, tc.args...))
			got, gotErr := out(), errOut()
			if code != 2 {
				t.Fatalf("退出码 = %d，want 2（龄期必须显式给）\nstdout: %s\nstderr: %s", code, got, gotErr)
			}
			if !strings.Contains(gotErr, "-retention") {
				t.Errorf("stderr 应点名 -retention，got: %s", gotErr)
			}
			if strings.TrimSpace(got) != "" {
				t.Errorf("失败时 stdout 应为空（不打假报告），got %q", got)
			}
		})
	}
}

// TestWalGCCLIWithoutWalDirExits2：[wal].dir 未配置/为空/目录不存在 → 退出码 2 + 点名
// （GC 没有操作对象 = 不是合法状态；同 blob-gc）。
func TestWalGCCLIWithoutWalDirExits2(t *testing.T) {
	dir := t.TempDir()

	t.Run("配置无 wal 段", func(t *testing.T) {
		cfgPath := filepath.Join(dir, "empty.toml")
		if err := os.WriteFile(cfgPath, []byte("# 什么都没配\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		out := captureStdout(t)
		errOut := captureStderr(t)
		code := walGCMain([]string{"-config", cfgPath, "-retention", "24h"})
		got, gotErr := out(), errOut()
		if code != 2 {
			t.Fatalf("退出码 = %d，want 2\nstdout: %s", code, got)
		}
		if !strings.Contains(gotErr, "[wal].dir") {
			t.Errorf("stderr 应点名 [wal].dir 未配置，got: %s", gotErr)
		}
	})

	t.Run("dir 为空串", func(t *testing.T) {
		cfgPath := filepath.Join(dir, "emptydir.toml")
		if err := os.WriteFile(cfgPath, []byte("[wal]\ndir = \"\"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		out := captureStdout(t)
		errOut := captureStderr(t)
		code := walGCMain([]string{"-config", cfgPath, "-retention", "24h"})
		got, gotErr := out(), errOut()
		if code != 2 {
			t.Fatalf("退出码 = %d，want 2\nstdout: %s", code, got)
		}
		if !strings.Contains(gotErr, "[wal].dir") {
			t.Errorf("stderr 应点名 [wal].dir 为空，got: %s", gotErr)
		}
	})

	t.Run("目录不存在", func(t *testing.T) {
		ghost := filepath.Join(dir, "no-such-wal-dir")
		cfgPath := writeWalGCConfig(t, dir, ghost)
		out := captureStdout(t)
		errOut := captureStderr(t)
		code := walGCMain([]string{"-config", cfgPath, "-retention", "24h"})
		got, gotErr := out(), errOut()
		if code != 2 {
			t.Fatalf("退出码 = %d，want 2\nstdout: %s", code, got)
		}
		if !strings.Contains(gotErr, "不可用") && !strings.Contains(gotErr, ghost) {
			t.Errorf("stderr 应点名目录不可用，got: %s", gotErr)
		}
	})
}

// TestWalGCCLIDryRunAndReal：dry-run 报告 + 真跑真删 + 诚实边界（有 blob.plan 的 WAL
// 空事实下保守不删）。
func TestWalGCCLIDryRunAndReal(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	cfgPath := writeWalGCConfig(t, dir, walDir)

	clean := core.NewUlid()
	planned := core.NewUlid()
	cleanPath := seedWalGCFile(t, walDir, clean, 48*time.Hour,
		walGCLine(t, core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
		walGCLine(t, core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
	)
	plannedPath := seedWalGCFile(t, walDir, planned, 48*time.Hour,
		walGCLine(t, core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
		walGCLine(t, core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
		walGCLine(t, core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 1, ToolCallID: "tc_PLAN"}),
	)
	foreign := filepath.Join(walDir, "notes.txt")
	if err := os.WriteFile(foreign, []byte("别动我"), 0o644); err != nil {
		t.Fatal(err)
	}

	run := func(args ...string) walfile.GCReport {
		t.Helper()
		out := captureStdout(t)
		errOut := captureStderr(t)
		code := walGCMain(append([]string{"-config", cfgPath, "-retention", "24h"}, args...))
		got, gotErr := out(), errOut()
		if code != 0 {
			t.Fatalf("walGCMain(%v) 退出码 = %d，want 0\nstderr: %s", args, code, gotErr)
		}
		lines := strings.Split(strings.TrimSpace(got), "\n")
		if len(lines) != 1 {
			t.Fatalf("stdout 应只有一行 JSON，got %d 行:\n%s", len(lines), got)
		}
		var rep walfile.GCReport
		if err := json.Unmarshal([]byte(lines[0]), &rep); err != nil {
			t.Fatalf("stdout 不是合法 JSON: %v\n%s", err, got)
		}
		return rep
	}

	// -dry-run：一个不删，报告如实（干净删、plan 留）。
	rep := run("-dry-run")
	if !rep.DryRun {
		t.Error("-dry-run 应映射到 GCOptions.DryRun")
	}
	if rep.ScannedSessions != 2 || rep.Removed != 1 || rep.Skipped != 1 {
		t.Fatalf("dry-run 报告 = %+v，want scanned=2 removed=1 skipped=1", rep)
	}
	for _, p := range []string{cleanPath, plannedPath, foreign} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("dry-run 后 %s 应还在: %v", p, err)
		}
	}

	// 真跑：干净超龄的真删；有 blob.plan 的保守不删（CLI 空事实——诚实边界钉子）。
	rep = run()
	if rep.DryRun {
		t.Error("默认不该是 DryRun")
	}
	if rep.ScannedSessions != 2 || rep.Removed != 1 || rep.Skipped != 1 {
		t.Fatalf("真跑报告 = %+v，want scanned=2 removed=1 skipped=1", rep)
	}
	if _, err := os.Stat(cleanPath); !os.IsNotExist(err) {
		t.Error("干净+超龄的 WAL 应被真删（CLI 不是只打报告）")
	}
	if _, err := os.Stat(plannedPath); err != nil {
		t.Errorf("有 blob.plan 的 WAL 在空事实下应保守不删: %v", err)
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Errorf("外来文件不该被删: %v", err)
	}
}

// TestWalGCCLIWalDirFlagOverridesConfig：**-wal-dir 显式 flag 优先**于配置文件的
// [wal].dir（与 `alasol wal` 同一条口径：flag > 配置文件）。
//
// 两半都钉：
//   - 给了 -wal-dir → 扫 **flag 指向的目录**（配置文件里那个目录原封不动）；
//   - 不给 -wal-dir → 读配置文件的 [wal].dir（flag 只是"没给"，不是"禁用配置文件"）。
//
// 变异自证：把 CLI 里的 flag 优先逻辑去掉（永远读配置文件）→ 本用例前半红。
func TestWalGCCLIWalDirFlagOverridesConfig(t *testing.T) {
	dir := t.TempDir()
	flagDir := filepath.Join(dir, "flag-wal")
	cfgDir := filepath.Join(dir, "cfg-wal")
	cfgPath := writeWalGCConfig(t, dir, cfgDir)

	// 两个目录各放一个"干净 + 超龄"的 WAL（都该被回收）。
	cfgTarget := seedWalGCFile(t, cfgDir, core.NewUlid(), 48*time.Hour,
		walGCLine(t, core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
		walGCLine(t, core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
	)
	flagTarget := seedWalGCFile(t, flagDir, core.NewUlid(), 48*time.Hour,
		walGCLine(t, core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
		walGCLine(t, core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
	)

	run := func(args ...string) walfile.GCReport {
		t.Helper()
		out := captureStdout(t)
		errOut := captureStderr(t)
		code := walGCMain(append([]string{"-config", cfgPath, "-retention", "24h"}, args...))
		got, gotErr := out(), errOut()
		if code != 0 {
			t.Fatalf("walGCMain(%v) 退出码 = %d，want 0\nstdout: %s\nstderr: %s", args, code, got, gotErr)
		}
		lines := strings.Split(strings.TrimSpace(got), "\n")
		if len(lines) != 1 {
			t.Fatalf("stdout 应只有一行 JSON，got %d 行:\n%s", len(lines), got)
		}
		var rep walfile.GCReport
		if err := json.Unmarshal([]byte(lines[0]), &rep); err != nil {
			t.Fatalf("stdout 不是合法 JSON: %v\n%s", err, got)
		}
		return rep
	}

	// ① 给了 -wal-dir → 扫 flag 目录；配置文件里那个目录**原封不动**。
	rep := run("-wal-dir", flagDir)
	if rep.ScannedSessions != 1 || rep.Removed != 1 {
		t.Fatalf("报告 = %+v，want scanned=1 removed=1（-wal-dir 优先于配置文件）", rep)
	}
	if _, err := os.Stat(flagTarget); !os.IsNotExist(err) {
		t.Error("-wal-dir 指向的目录里的超龄 WAL 应被删")
	}
	if _, err := os.Stat(cfgTarget); err != nil {
		t.Errorf("配置文件指向的目录不该被扫（-wal-dir 优先）: %v", err)
	}

	// ② 不给 -wal-dir → 读配置文件的 [wal].dir（flag 缺省 = 用配置，不是禁用配置）。
	rep = run()
	if rep.ScannedSessions != 1 || rep.Removed != 1 {
		t.Fatalf("报告 = %+v，want scanned=1 removed=1（没给 -wal-dir → 读 [wal].dir）", rep)
	}
	if _, err := os.Stat(cfgTarget); !os.IsNotExist(err) {
		t.Error("[wal].dir 里的超龄 WAL 应被删（没给 -wal-dir 时读配置）")
	}
}
