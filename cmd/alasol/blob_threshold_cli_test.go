package main

// F3 的钉子：**负阈值在 run / resume 装配面也必须报错**（与 serve 装配面同款），
// 而不是"静默当成 0（关）"。
//
// # 原症状（F3）：口径漂移
//
// serve 装配面（host.Config.validate）遇到负阈值 → **fail-close 拒启动**；
// run / resume 装配面（不经 host.Config，自己装配 loop.Config）遇到负阈值 → **静默关掉**
// （core/loop 的口径是"阈值 <= 0 = 不外置"）。同一个坏配置，三条装配面两种表现：
//
//	serve：启动就报错（人立刻知道配置写错了）
//	run：跑起来了，但大 payload **根本没外置**（事件流全量内联、读面没有全文）——
//	      用户以为"外置开着"，现场**没有任何线索**（更糟：它看起来一切正常）。
//
// # 口径（写明）
//
// 唯一事实源 = host.ValidateBlobMaxInline（host/config.go）：
//
//	< 0 → 报错（三条装配面同一句文案）
//	= 0 → 不外置（关；**合法**，零行为变化）
//	> 0 → 超过它的 result 整体外置
//
// run / resume 在**装配期**（任何执行 / 任何重放之前）就拒——重放会真的执行工具，
// 配置错了就该先拒，不该先把工具跑一遍。

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/host"
)

// blobThresholdConfigTOML 写一份只带 `[blob] max_inline` 的配置文件。
func blobThresholdConfigTOML(t *testing.T, path string, maxInline int64) {
	t.Helper()
	body := "[blob]\nmax_inline = " + strconv.FormatInt(maxInline, 10) + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
}

// TestRunRejectsNegativeBlobThreshold（F3 的钉子之一）：`alasol run` 遇负阈值 → 启动期拒
// （退出码 2 = 配置被拒），且**报错文案与 serve 装配面同款**。
func TestRunRejectsNegativeBlobThreshold(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.toml")
	blobThresholdConfigTOML(t, cfg, -1)
	db := filepath.Join(dir, "events.db")

	errOut := captureStderr(t)
	code := runMain([]string{
		"-prompt", "你好",
		"-model", "test-model",
		"-db", db,
		"-base-url", "http://127.0.0.1:1/v1",
		"-config", cfg,
	})
	msg := errOut()

	if code != 2 {
		t.Fatalf("负阈值的退出码 = %d，want 2（配置被拒，启动期 fail-close）\n%s", code, msg)
	}
	// 同款错误：文案必须含 host.ValidateBlobMaxInline 的原文（唯一事实源）。
	wantErr := host.ValidateBlobMaxInline(-1)
	if wantErr == nil {
		t.Fatal("host.ValidateBlobMaxInline(-1) 应报错")
	}
	if !strings.Contains(msg, wantErr.Error()) {
		t.Fatalf("报错文案应与 serve 装配面同款（含 %q）:\n%s", wantErr.Error(), msg)
	}
	// 负阈值不该装配出事件库（连装配都没走到）。
	if _, err := os.Stat(db); err == nil {
		t.Error("非法阈值不该装配出事件库")
	}
}

// TestResumeRejectsNegativeBlobThreshold（F3 的钉子之二）：`alasol resume` 遇负阈值 →
// 启动期拒（退出码 2），且**任何重放都没发生**（重放会真的执行工具——配置错了先拒）。
func TestResumeRejectsNegativeBlobThreshold(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.toml")
	blobThresholdConfigTOML(t, cfg, -1)
	db := filepath.Join(dir, "resume.db")
	session, _ := seedCrashedSession(t, db)

	errOut := captureStderr(t)
	code := resumeMain([]string{
		"-session", string(session),
		"-db", db,
		"-verify", "none",
		"-config", cfg,
	})
	msg := errOut()

	if code != 2 {
		t.Fatalf("负阈值的退出码 = %d，want 2（配置被拒，启动期 fail-close）\n%s", code, msg)
	}
	wantErr := host.ValidateBlobMaxInline(-1)
	if !strings.Contains(msg, wantErr.Error()) {
		t.Fatalf("报错文案应与 serve 装配面同款（含 %q）:\n%s", wantErr.Error(), msg)
	}
	// 任何重放都不该发生（负阈值 → 装配期就拒）。
	for _, ev := range readAllEvents(t, db) {
		if ev.Kind == "tool.call.completed" || ev.Kind == "tool.call.failed" {
			t.Fatalf("负阈值时不该有任何重放结果事件，got %s", ev.Kind)
		}
	}
}

// TestRunAcceptsNonNegativeBlobThreshold（F3 的钉子之三）：0 是**合法**值（= 不外置），
// 不该被阈值校验拦下（闸门不是过滤器）。
func TestRunAcceptsNonNegativeBlobThreshold(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.toml")
	blobThresholdConfigTOML(t, cfg, 0)

	errOut := captureStderr(t)
	// 不给 -model：让它停在"缺模型名"（退出码 2，但**不是**被阈值校验拦的）。
	code := runMain([]string{
		"-prompt", "你好",
		"-db", filepath.Join(dir, "events.db"),
		"-config", cfg,
	})
	msg := errOut()

	wantErr := host.ValidateBlobMaxInline(-1)
	if strings.Contains(msg, wantErr.Error()) {
		t.Fatalf("阈值 0 是合法值（= 不外置），不该被阈值校验拦:\n%s", msg)
	}
	if code != 2 {
		t.Fatalf("退出码 = %d（这里应是缺模型名的 2），got:\n%s", code, msg)
	}
}
