package main

// `alasol blob-gc` 子命令的端到端钉子（缺口 4 后半 CLI 面）。
//
// 口径：
//   - 单行 JSON 报告到 stdout（照 `alasol wal` 的输出口径），错误到 stderr；
//   - dir 读配置文件的 [blob].dir（与 run/serve/resume 同一条配置面）；
//   - [blob].dir 未配置/为空 → **退出码 2 + 明确报错**（不静默"扫了 0 个"的假成功）；
//   - -retention 默认 0 = 只安全两扫；-dry-run 只报不删；
//   - **负值即拒**（-retention/-grace < 0 → 退出码 2 + 点名 flag）；
//   - **-grace 0 = 用默认 1h**（安全网不给关）；**-grace 非默认值真生效**。
//
// 变异自证：
//   - CLI 去掉负值拒绝 → 负值钉子红；
//   - CLI 的 -grace 映射改成 Grace:0（非默认值失效）→ 宽限钉子红。

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/runtime/store/blobfs"
)

// writeBlobGCConfig 写一份只含 [blob].dir 的 TOML 配置（Windows 路径转义反斜杠）。
func writeBlobGCConfig(t *testing.T, dir, blobDir string) string {
	t.Helper()
	cfgPath := filepath.Join(dir, "config.toml")
	toml := "[blob]\ndir = \"" + strings.ReplaceAll(blobDir, "\\", "\\\\") + "\"\n"
	if err := os.WriteFile(cfgPath, []byte(toml), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	return cfgPath
}

// seedBlobGCResidueYoung 在 blobDir 里造一个**年轻**孤儿目录（Grace 保护的靶子）。
func seedBlobGCResidueYoung(t *testing.T, blobDir string) string {
	t.Helper()
	id := core.NewUlid()
	orphan := filepath.Join(blobDir, string(id))
	if err := os.MkdirAll(orphan, 0o755); err != nil {
		t.Fatalf("建年轻孤儿: %v", err)
	}
	if err := os.WriteFile(filepath.Join(orphan, "content"), []byte("in-flight"), 0o644); err != nil {
		t.Fatalf("写 content: %v", err)
	}
	return orphan
}

// seedBlobGCResidue 在 blobDir 里造一个老孤儿目录（GC 的靶子）。
func seedBlobGCResidue(t *testing.T, blobDir string) string {
	t.Helper()
	id := core.NewUlid()
	orphan := filepath.Join(blobDir, string(id))
	if err := os.MkdirAll(orphan, 0o755); err != nil {
		t.Fatalf("建孤儿: %v", err)
	}
	if err := os.WriteFile(filepath.Join(orphan, "content"), []byte("crash"), 0o644); err != nil {
		t.Fatalf("写 content: %v", err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(orphan, old, old); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	return orphan
}

// TestBlobGCCLIReadsDirAndReports：-config 读 [blob].dir → GC 跑起来 → 单行 JSON 报告
// 到 stdout，孤儿真被删（不是只打个报告）。
func TestBlobGCCLIReadsDirAndReports(t *testing.T) {
	dir := t.TempDir()
	blobDir := filepath.Join(dir, "blobs")
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		t.Fatal(err)
	}
	orphan := seedBlobGCResidue(t, blobDir)
	cfgPath := writeBlobGCConfig(t, dir, blobDir)

	out := captureStdout(t)
	errOut := captureStderr(t)
	code := blobGCMain([]string{"-config", cfgPath})
	got, gotErr := out(), errOut()
	if code != 0 {
		t.Fatalf("blobGCMain 退出码 = %d，want 0\nstderr: %s", code, gotErr)
	}
	// 单行 JSON。
	lines := strings.Split(strings.TrimSpace(got), "\n")
	if len(lines) != 1 {
		t.Fatalf("stdout 应只有一行 JSON，got %d 行:\n%s", len(lines), got)
	}
	var rep blobfs.GCReport
	if err := json.Unmarshal([]byte(lines[0]), &rep); err != nil {
		t.Fatalf("stdout 不是合法 JSON: %v\n%s", err, got)
	}
	if rep.DryRun {
		t.Error("默认不该是 DryRun")
	}
	if rep.RemovedOrphans != 1 {
		t.Errorf("RemovedOrphans = %d，want 1（配置里的目录被扫了）", rep.RemovedOrphans)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Error("老孤儿应被真删（CLI 不是只打报告）")
	}
}

// TestBlobGCCLIWithoutBlobDirExits2：[blob].dir 未配置/为空 → 退出码 2 + 点名报错
// （不静默成功——"没配"对 GC 来说不是合法状态，是没有操作对象）。
func TestBlobGCCLIWithoutBlobDirExits2(t *testing.T) {
	dir := t.TempDir()
	// 配置文件存在但没有 [blob].dir（或压根没给 -config → 默认位也读不到 dir）。
	cfgPath := filepath.Join(dir, "empty.toml")
	if err := os.WriteFile(cfgPath, []byte("# 什么都没配\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("配置无 blob 段", func(t *testing.T) {
		out := captureStdout(t)
		errOut := captureStderr(t)
		code := blobGCMain([]string{"-config", cfgPath})
		got, gotErr := out(), errOut()
		if code != 2 {
			t.Fatalf("退出码 = %d，want 2\nstdout: %s", code, got)
		}
		if strings.TrimSpace(got) != "" {
			t.Errorf("失败时 stdout 应为空（不打假报告），got %q", got)
		}
		if !strings.Contains(gotErr, "[blob].dir") {
			t.Errorf("stderr 应点名 [blob].dir 未配置，got: %s", gotErr)
		}
	})

	t.Run("dir 为空串", func(t *testing.T) {
		emptyCfg := filepath.Join(dir, "emptydir.toml")
		if err := os.WriteFile(emptyCfg, []byte("[blob]\ndir = \"\"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		out := captureStdout(t)
		errOut := captureStderr(t)
		code := blobGCMain([]string{"-config", emptyCfg})
		got, gotErr := out(), errOut()
		if code != 2 {
			t.Fatalf("退出码 = %d，want 2\nstdout: %s", code, got)
		}
		if !strings.Contains(gotErr, "[blob].dir") {
			t.Errorf("stderr 应点名 [blob].dir 为空，got: %s", gotErr)
		}
	})
}

// TestBlobGCCLIFlagsMapToGCOptions：flag → GCOptions 映射的轻钉——
//   - -dry-run：报告 DryRun=true 且磁盘不动；
//   - -retention 显式 >0：老 meta 的完整 blob 被扫（默认 0 时不扫）；
//   - -grace：年轻残留不动（默认 1h 下年轻孤儿留）。
func TestBlobGCCLIFlagsMapToGCOptions(t *testing.T) {
	dir := t.TempDir()
	blobDir := filepath.Join(dir, "blobs")
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfgPath := writeBlobGCConfig(t, dir, blobDir)

	// 现场：老孤儿 + 老 meta 完整 blob + 年轻孤儿。
	orphanOld := seedBlobGCResidue(t, blobDir)
	st, err := blobfs.New(blobDir)
	if err != nil {
		t.Fatalf("blobfs.New: %v", err)
	}
	completeOld, err := st.Put(t.Context(), storage.BlobMeta{MediaType: "application/json"}, strings.NewReader(`{"old":1}`))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	oldMeta := filepath.Join(blobDir, string(completeOld.ID), "meta.json")
	if err := os.Chtimes(oldMeta, time.Now().Add(-48*time.Hour), time.Now().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// 年轻孤儿（Grace 保护）。
	orphanYoung := seedBlobGCResidueYoung(t, blobDir)

	run := func(args ...string) (blobfs.GCReport, int, string) {
		t.Helper()
		out := captureStdout(t)
		errOut := captureStderr(t)
		code := blobGCMain(append([]string{"-config", cfgPath}, args...))
		got, gotErr := out(), errOut()
		if code != 0 {
			t.Fatalf("blobGCMain(%v) 退出码 = %d，want 0\nstderr: %s", args, code, gotErr)
		}
		var rep blobfs.GCReport
		if err := json.Unmarshal([]byte(strings.TrimSpace(got)), &rep); err != nil {
			t.Fatalf("stdout 不是单行 JSON: %v\n%s", err, got)
		}
		return rep, code, got
	}

	// -dry-run：一个不删，报告如实。
	rep, _, _ := run("-dry-run", "-retention", "24h")
	if !rep.DryRun {
		t.Fatal("-dry-run 应映射到 GCOptions.DryRun")
	}
	if rep.RemovedOrphans != 1 || rep.RemovedAged != 1 {
		t.Fatalf("dry-run 报告应数出会删的：orphan=%d aged=%d，want 1/1", rep.RemovedOrphans, rep.RemovedAged)
	}
	for _, p := range []string{orphanOld, filepath.Join(blobDir, string(completeOld.ID))} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("dry-run 后 %s 应还在: %v", p, err)
		}
	}

	// 默认（-retention 0）：只安全两扫——老孤儿删、老完整 blob 留。
	rep, _, _ = run()
	if rep.RemovedOrphans != 1 {
		t.Errorf("默认 RemovedOrphans = %d，want 1", rep.RemovedOrphans)
	}
	if rep.RemovedAged != 0 {
		t.Errorf("默认 -retention 0 → RemovedAged = %d，want 0（不清扫完整 blob）", rep.RemovedAged)
	}
	if _, err := os.Stat(filepath.Join(blobDir, string(completeOld.ID))); err != nil {
		t.Errorf("默认不该删完整 blob: %v", err)
	}
	if _, err := os.Stat(orphanYoung); err != nil {
		t.Errorf("年轻孤儿（Grace 保护）该留: %v", err)
	}
	if _, err := os.Stat(orphanOld); !os.IsNotExist(err) {
		t.Error("老孤儿该删")
	}

	// 显式 -retention 24h：老 meta 完整 blob 进入清扫（年轻孤儿仍受 -grace 保护）。
	rep, _, _ = run("-retention", "24h")
	if rep.RemovedAged != 1 {
		t.Errorf("-retention 24h → RemovedAged = %d，want 1", rep.RemovedAged)
	}
	if _, err := os.Stat(filepath.Join(blobDir, string(completeOld.ID))); !os.IsNotExist(err) {
		t.Error("超期完整 blob 应被删")
	}
	if _, err := os.Stat(orphanYoung); err != nil {
		t.Errorf("-grace 默认 1h：年轻孤儿仍该留: %v", err)
	}
}

// seedBlobGCOrphanAged 造一个**指定年龄**的孤儿目录（年龄 = 目录 mtime；宽限期钉子用）。
func seedBlobGCOrphanAged(t *testing.T, blobDir string, age time.Duration) string {
	t.Helper()
	id := core.NewUlid()
	orphan := filepath.Join(blobDir, string(id))
	if err := os.MkdirAll(orphan, 0o755); err != nil {
		t.Fatalf("建孤儿: %v", err)
	}
	if err := os.WriteFile(filepath.Join(orphan, "content"), []byte("crash"), 0o644); err != nil {
		t.Fatalf("写 content: %v", err)
	}
	when := time.Now().Add(-age)
	if err := os.Chtimes(orphan, when, when); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	return orphan
}

// TestBlobGCRejectsNegativeFlags（F4 钉子）：-retention < 0 / -grace < 0 → **退出码 2 +
// 点名 flag**（负值 = 非法配置即拒；静默改写成 0/默认值 = 悄悄换了一套回收策略）。
//
// 变异自证：CLI 去掉负值拒绝 → 本钉子红（退出码变 0，报告照打）。
func TestBlobGCRejectsNegativeFlags(t *testing.T) {
	dir := t.TempDir()
	blobDir := filepath.Join(dir, "blobs")
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfgPath := writeBlobGCConfig(t, dir, blobDir)

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"-retention 负值", []string{"-retention", "-1h"}, "-retention"},
		{"-grace 负值", []string{"-grace", "-1h"}, "-grace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := captureStdout(t)
			errOut := captureStderr(t)
			code := blobGCMain(append([]string{"-config", cfgPath}, tc.args...))
			got, gotErr := out(), errOut()
			if code != 2 {
				t.Fatalf("退出码 = %d，want 2（负值即拒）\nstdout: %s\nstderr: %s", code, got, gotErr)
			}
			if !strings.Contains(gotErr, tc.want) {
				t.Errorf("stderr 应点名 %s，got: %s", tc.want, gotErr)
			}
			if strings.TrimSpace(got) != "" {
				t.Errorf("失败时 stdout 应为空（不打假报告），got %q", got)
			}
		})
	}
}

// TestBlobGCGraceZeroUsesDefaultAndNonDefaultTakesEffect（F4 + F5② 钉子）：
//
//   - **-grace 0 = 用默认 1h**（安全网不给关：零宽限 = 立刻全删 = 会误删在途写）；
//   - **-grace 非默认值真生效**（-grace 10s → 30s 龄的孤儿该被回收）。
//
// 变异自证：CLI 的 -grace 映射改成 Grace:0（丢掉用户给的值）→ 第二段红（30s 龄的孤儿
// 在默认 1h 下不会被删）。
func TestBlobGCGraceZeroUsesDefaultAndNonDefaultTakesEffect(t *testing.T) {
	dir := t.TempDir()
	blobDir := filepath.Join(dir, "blobs")
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfgPath := writeBlobGCConfig(t, dir, blobDir)

	run := func(args ...string) blobfs.GCReport {
		t.Helper()
		out := captureStdout(t)
		errOut := captureStderr(t)
		code := blobGCMain(append([]string{"-config", cfgPath}, args...))
		got, gotErr := out(), errOut()
		if code != 0 {
			t.Fatalf("blobGCMain(%v) 退出码 = %d，want 0\nstderr: %s", args, code, gotErr)
		}
		var rep blobfs.GCReport
		if err := json.Unmarshal([]byte(strings.TrimSpace(got)), &rep); err != nil {
			t.Fatalf("stdout 不是单行 JSON: %v\n%s", err, got)
		}
		return rep
	}

	// 30s 龄的孤儿：默认宽限 1h → 留；-grace 10s → 回收。
	orphan := seedBlobGCOrphanAged(t, blobDir, 30*time.Second)

	// -grace 0：语义写死 = 用默认 1h → 30s 龄的孤儿**不该**被删。
	rep := run("-grace", "0")
	if rep.RemovedOrphans != 0 {
		t.Errorf("-grace 0 → RemovedOrphans = %d，want 0（显式 0 = 默认 1h，安全网不给关）", rep.RemovedOrphans)
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Errorf("-grace 0（= 默认 1h）不该删 30s 龄的孤儿: %v", err)
	}

	// -grace 10s：非默认值**真生效** → 30s 龄的孤儿被回收。
	rep = run("-grace", "10s")
	if rep.RemovedOrphans != 1 {
		t.Errorf("-grace 10s → RemovedOrphans = %d，want 1（-grace 的值真生效）", rep.RemovedOrphans)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Error("-grace 10s：30s 龄的孤儿应被回收")
	}
}
