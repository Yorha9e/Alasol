package gateexec

import (
	"errors"
	"os"
	"testing"

	"github.com/Yorha9e/alasol/core/gate"
)

// TestLoadConfigNotFound：缺 .alasol/gate.json → gate.ErrNotConfigured（fail-close，未配置 ≠ 通过）。
func TestLoadConfigNotFound(t *testing.T) {
	_, err := LoadConfig(t.TempDir())
	if !errors.Is(err, gate.ErrNotConfigured) {
		t.Errorf("缺 gate.json 应返回 ErrNotConfigured，得到 %v", err)
	}
}

// TestLoadConfigRoundTrip：写好 gate.json 后能读回（含 .alasol 子目录由调用方/写入方建）。
func TestLoadConfigRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir+string(os.PathSeparator)+".alasol", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gate.ConfigPath(dir), []byte(`{"command":"make ci"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := LoadConfig(dir)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if c.Command != "make ci" {
		t.Errorf("Command = %q，期望 make ci", c.Command)
	}
}

// TestLoadConfigBadContent：文件在但内容坏 → 报错（不吞成 ErrNotConfigured）。
func TestLoadConfigBadContent(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir+string(os.PathSeparator)+".alasol", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gate.ConfigPath(dir), []byte(`{"command":}`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(dir)
	if err == nil || errors.Is(err, gate.ErrNotConfigured) {
		t.Errorf("内容坏的 gate.json 应报解析错误（非 ErrNotConfigured），得到 %v", err)
	}
}

// TestBaselineSaveLoadRoundTrip 是持久化/重开证据：SaveBaseline 落盘 → 关闭 → LoadBaseline 读回，entries 一致。
func TestBaselineSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	b := &gate.Baseline{}
	// 造两条已知失败（复用 core/gate 的 record 路径：跑一次 Judge 记录）。
	r := gate.Receipt{ExitCode: 1, Command: "make ci", Failures: gate.ExtractFailures("FAIL: a\nFAIL: b")}
	if _, err := gate.Judge(&r, b); err != nil {
		t.Fatalf("Judge: %v", err)
	}

	if err := SaveBaseline(dir, b); err != nil {
		t.Fatalf("SaveBaseline: %v", err)
	}
	// 文件确实落盘。
	if _, err := os.Stat(gate.BaselinePath(dir)); err != nil {
		t.Fatalf("baseline 文件应存在：%v", err)
	}
	// 重新读回（模拟重开）。
	got, err := LoadBaseline(dir)
	if err != nil {
		t.Fatalf("LoadBaseline: %v", err)
	}
	if got.Command != "make ci" {
		t.Errorf("Command = %q，期望 make ci", got.Command)
	}
	if len(got.Entries) != 2 {
		t.Fatalf("entries = %d，期望 2", len(got.Entries))
	}
	for i := range b.Entries {
		if got.Entries[i] != b.Entries[i] {
			t.Errorf("entries[%d] = %+v，期望 %+v", i, got.Entries[i], b.Entries[i])
		}
	}
}

// TestLoadBaselineNotFound：缺 baseline → 空 baseline（首建模式），非 error。
func TestLoadBaselineNotFound(t *testing.T) {
	b, err := LoadBaseline(t.TempDir())
	if err != nil {
		t.Fatalf("缺 baseline 不应报错：%v", err)
	}
	if len(b.Entries) != 0 {
		t.Errorf("缺失应返回空 baseline，得到 %d 条", len(b.Entries))
	}
}
