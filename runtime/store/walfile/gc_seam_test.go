package walfile

// GC 的**读失败路径**钉子（测试缝，与 sync_seam_test.go 注入记账假文件同一思路）：
// reconcile 读失败 → 该文件 Skipped 且**保留**，且**不中止整轮**（后面的照删）。
//
// 为什么走缝而不是黑盒构造：普通文件的"读失败"在各平台都不可靠构造（Windows 要 ACL/
// 共享模式、POSIX 有 root 豁免），拼出来的钉子在有的机器上是空的。gcReadFile 是包级
// 入口（生产恒为 ReadFile），本文件临时换掉它 → 处置口径在任何机器上都钉得住。
//
// 钉子：把 GC 对读失败的 Skipped 分支删掉（读失败也照删/照报）→ 本钉子红。

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// gcSeamLine 造一行 WAL NDJSON（与 Writer 写出的形状一致）。
func gcSeamLine(t *testing.T, kind string, payload any) string {
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

// gcSeamSeed 写一个干净收口的 WAL 并把 mtime 钉老（超龄靶子）。
func gcSeamSeed(t *testing.T, dir string, sid contracts.Ulid) string {
	t.Helper()
	path := filepath.Join(dir, string(sid)+".wal")
	content := strings.Join([]string{
		gcSeamLine(t, core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
		gcSeamLine(t, core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("写 WAL: %v", err)
	}
	when := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	return path
}

// TestGCReadFailureSkippedKeepsFileAndKeepsGoing：第一个文件读失败 → Skipped 且留；
// 第二个照删（不中止整轮）；报告如实。
func TestGCReadFailureSkippedKeepsFileAndKeepsGoing(t *testing.T) {
	dir := t.TempDir()
	sidBad := core.NewUlid()
	sidOK := core.NewUlid()
	badPath := gcSeamSeed(t, dir, sidBad)
	okPath := gcSeamSeed(t, dir, sidOK)

	orig := gcReadFile
	defer func() { gcReadFile = orig }()
	gcReadFile = func(path string) (ScanResult, error) {
		if strings.HasSuffix(path, string(sidBad)+".wal") {
			return ScanResult{Path: path}, errors.New("注入：读失败")
		}
		return orig(path)
	}

	rep, err := GC(dir, GCOptions{Retention: time.Hour})
	if err != nil {
		t.Fatalf("读失败不该让整轮报错（跳过即可）: %v", err)
	}
	if rep.ScannedSessions != 2 || rep.Removed != 1 || rep.Skipped != 1 {
		t.Fatalf("报告 = %+v，want scanned=2 removed=1 skipped=1", rep)
	}
	if _, err := os.Stat(badPath); err != nil {
		t.Errorf("读失败的文件应保留（宁留不删）: %v", err)
	}
	if _, err := os.Stat(okPath); !os.IsNotExist(err) {
		t.Error("读失败不该中止整轮：干净超龄的另一个照删")
	}
}

// TestGCDeleteFailureDivergesDryVsReal（critic F10 的**已知分叉**钉子）：**删不掉**时，
// DryRun 与真跑的 Removed 会**分叉**——真跑把"判据命中但删不掉"的那个记成 **Skipped**
// （不是 Removed），而 DryRun **不真删**、测不出删不掉 → 仍记 **Removed**。
//
// 为什么这是**如实**而不是 bug：Removed 的口径是"**判据命中数**"（同一个"该删"判定，
// 两模式同源）；分叉只来自"删除动作是否真的成功"这一**执行事实**。GCReport.Removed 的
// 注释与 CLI 帮助都写明了这条分叉——报告不谎报。
//
// 为什么走缝：普通文件的"删不掉"在各平台都不可靠构造（Windows 上打开的文件不可删、
// POSIX 上 unlink 打开的文件是允许的）→ gcRemove 是包级入口（生产恒为 Remove），注入它
// 就能在任何机器上钉住这条口径。
//
// 钉子：把"删失败 → 计入 Skipped"改成"照样计 Removed"→ 真跑那半红（Skipped 应为 1）。
func TestGCDeleteFailureDivergesDryVsReal(t *testing.T) {
	dir := t.TempDir()
	sid := core.NewUlid()
	path := gcSeamSeed(t, dir, sid)

	orig := gcRemove
	defer func() { gcRemove = orig }()
	gcRemove = func(string, contracts.Ulid) error {
		return errors.New("注入：删不掉（sharing violation 之类）")
	}

	// DryRun：删不掉测不出来 → 判据命中数照实记 Removed。
	dry, err := GC(dir, GCOptions{Retention: time.Hour, DryRun: true})
	if err != nil {
		t.Fatalf("GC(DryRun): %v", err)
	}
	if dry.Removed != 1 || dry.Skipped != 0 {
		t.Fatalf("DryRun 报告 = %+v，want removed=1 skipped=0（dry 不真删 → 测不出删不掉）", dry)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("DryRun 不该动文件: %v", err)
	}

	// 真跑：删不掉 → 计入 Skipped（"报了却没删"是假账）。
	real, err := GC(dir, GCOptions{Retention: time.Hour})
	if err != nil {
		t.Fatalf("GC(real): %v", err)
	}
	if real.Removed != 0 || real.Skipped != 1 {
		t.Fatalf("真跑报告 = %+v，want removed=0 skipped=1（删不掉 → Skipped，不报 Removed）", real)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("删不掉的文件应保留: %v", err)
	}
}
