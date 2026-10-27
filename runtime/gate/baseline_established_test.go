package gateexec

// 「基线文件是否存在」进入判定的端到端口径：
//
//   - 文件不存在 → 首建（Established=false）→ 记录模式（红仓第一次跑靠它记录、不阻断）；
//   - 文件存在但条目为空 → Established=true → **不是**首建，而是 fail-close 拦下
//     （否则空基线会把任何失败都记成"已知"后放行 → 门禁静默失效）；
//   - 绿仓（exit 0）跑 -record 也不落盘空基线。

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core/gate"
)

// writeBaselineFile 在仓库根写一个 baseline 文件（内容原样）。
func writeBaselineFile(t *testing.T, repoRoot, content string) {
	t.Helper()
	dir := filepath.Join(repoRoot, ".alasol")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gate.BaselinePath(repoRoot), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// failingReceipt 造一个"命令失败"的 receipt（失败条目从输出提取）。
func failingReceipt(command, output string) gate.Receipt {
	return gate.Receipt{
		Commit:   "c0ffee00000000000000000000000000000001",
		Command:  command,
		ExitCode: 1,
		Failures: gate.ExtractFailures(output),
		LogTail:  output,
	}
}

// TestLoadBaselineMissingFileIsFirstBuild：文件不存在 → 未建立 → 首建（记录模式）。
func TestLoadBaselineMissingFileIsFirstBuild(t *testing.T) {
	dir := t.TempDir()
	b, err := LoadBaseline(dir)
	if err != nil {
		t.Fatalf("缺 baseline 不应报错：%v", err)
	}
	if b.Established {
		t.Fatal("文件不存在 → 基线未建立（Established 应为 false）")
	}
	// 首建模式：失败被记录（不阻断）。
	r := failingReceipt("make ci", "FAIL: a")
	v, err := gate.Judge(&r, b)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v.Kind != gate.VerdictRecorded {
		t.Fatalf("首建应 VerdictRecorded，得到 %v", v.Kind)
	}
	if len(b.Entries) == 0 {
		t.Fatal("首建应记入失败指纹")
	}
}

// TestLoadBaselineExistingEmptyFileIsBlocked：文件存在但**条目为空** → 已建立 →
// fail-close 拦下 + 可读理由。
func TestLoadBaselineExistingEmptyFileIsBlocked(t *testing.T) {
	dir := t.TempDir()
	writeBaselineFile(t, dir, `{"version":1,"command":"make ci","entries":[]}`)

	b, err := LoadBaseline(dir)
	if err != nil {
		t.Fatalf("LoadBaseline: %v", err)
	}
	if !b.Established {
		t.Fatal("文件存在 → 基线已建立（Established 应为 true）")
	}
	if len(b.Entries) != 0 {
		t.Fatalf("空文件应没有条目，得到 %d 条", len(b.Entries))
	}

	r := failingReceipt("make ci", "FAIL: a")
	v, err := gate.Judge(&r, b)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v.Kind != gate.VerdictBlocked {
		t.Fatalf("空基线（文件存在）应 VerdictBlocked，得到 %v（new=%v）", v.Kind, v.NewFailures)
	}
	if len(v.NewFailures) != 1 || !strings.Contains(v.NewFailures[0], "基线文件为空") {
		t.Fatalf("应给出基线为空/被清空的可读理由，得到 %v", v.NewFailures)
	}
	if !strings.Contains(v.NewFailures[0], "-record") {
		t.Fatalf("理由应提示用 -record 重建：%v", v.NewFailures)
	}
}

// TestLoadBaselineOldFileWithoutEstablishedField：旧文件（没有 established 字段）→
// 按"文件存在即已建立"处理（有条目的正常比对，空条目的 fail-close 拦下）。
func TestLoadBaselineOldFileWithoutEstablishedField(t *testing.T) {
	dir := t.TempDir()
	writeBaselineFile(t, dir, `{"version":1,"command":"make ci","entries":[{"hash":"h1","label":"FAIL: a"}]}`)

	b, err := LoadBaseline(dir)
	if err != nil {
		t.Fatalf("LoadBaseline: %v", err)
	}
	if !b.Established {
		t.Fatal("旧文件也应视为已建立（存在即已建立）")
	}
	if len(b.Entries) != 1 {
		t.Fatalf("旧文件的条目应保留，得到 %d 条", len(b.Entries))
	}
}

// TestSaveBaselineSkipsEmptyBaseline：绿仓（exit 0）跑 -record 也不写空基线文件。
func TestSaveBaselineSkipsEmptyBaseline(t *testing.T) {
	dir := t.TempDir()
	b := &gate.Baseline{Command: "make ci"} // 没有任何条目（exit 0 的绿仓）

	if err := SaveBaseline(dir, b); err != nil {
		t.Fatalf("SaveBaseline: %v", err)
	}
	if _, err := os.Stat(gate.BaselinePath(dir)); !os.IsNotExist(err) {
		t.Fatalf("空基线不应落盘（stat err=%v）", err)
	}
	// 再 LoadBaseline → 仍是"未建立"（首建）。
	got, err := LoadBaseline(dir)
	if err != nil {
		t.Fatalf("LoadBaseline: %v", err)
	}
	if got.Established {
		t.Fatal("没写盘 → 基线仍未建立")
	}
}

// TestSaveBaselineWithEntriesWritesEstablishedFile：有条目时落盘，且文件标记为已建立。
func TestSaveBaselineWithEntriesWritesEstablishedFile(t *testing.T) {
	dir := t.TempDir()
	b := &gate.Baseline{}
	r := failingReceipt("make ci", "FAIL: a")
	if _, err := gate.Judge(&r, b); err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if err := SaveBaseline(dir, b); err != nil {
		t.Fatalf("SaveBaseline: %v", err)
	}
	got, err := LoadBaseline(dir)
	if err != nil {
		t.Fatalf("LoadBaseline: %v", err)
	}
	if !got.Established {
		t.Fatal("落盘后读回应标记为已建立")
	}
	if len(got.Entries) != 1 {
		t.Fatalf("entries = %d，期望 1", len(got.Entries))
	}
}
