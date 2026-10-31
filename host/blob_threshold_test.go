package host_test

// F3 的钉子（host 侧 = 参照面）：**负阈值拒绝启动**；0 是合法值（= 不外置）。
//
// 为什么 host 侧也要钉：host.Config.validate 是这条口径的**参照实现**（run / resume 两条
// 不经 host.Config 的装配面复用同一个 host.ValidateBlobMaxInline）。参照面自己松了，
// "三面同款"就成了空话——负值在 serve 上被静默关掉、在 run 上被静默关掉、在 resume 上被
// 静默关掉：用户配了负阈值却以为"外置开着"，现场零线索。
//
// 口径：< 0 → 报错；= 0 → 不外置（关，零行为变化，**合法**）；> 0 → 超过它的 result 外置。

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/host"
)

// TestBlobNegativeThresholdRejectedAtStartup：负阈值 → host.New 直接报错（fail-close）。
func TestBlobNegativeThresholdRejectedAtStartup(t *testing.T) {
	_, err := host.New(host.Config{
		DBPath:        filepath.Join(t.TempDir(), "events.db"),
		Model:         "blob-threshold",
		BlobDir:       filepath.Join(t.TempDir(), "blobs"),
		BlobMaxInline: -1,
	})
	if err == nil {
		t.Fatal("负阈值应被拒（启动期 fail-close，不许静默当成 0 = 关）")
	}
	if !strings.Contains(err.Error(), "BlobMaxInline 不能为负") {
		t.Fatalf("报错应点名 BlobMaxInline 不能为负，got %q", err.Error())
	}
}

// TestBlobZeroThresholdIsLegal：0 = 不外置（关）——**合法**，启动不该被拦。
func TestBlobZeroThresholdIsLegal(t *testing.T) {
	h, err := host.New(host.Config{
		DBPath:        filepath.Join(t.TempDir(), "events.db"),
		Model:         "blob-threshold",
		BlobDir:       filepath.Join(t.TempDir(), "blobs"),
		BlobMaxInline: 0,
	})
	if err != nil {
		t.Fatalf("阈值 0 是合法值（= 不外置），不该被拦: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
}

// TestValidateBlobMaxInlineSharedRules：共享校验函数的口径（唯一事实源）。
func TestValidateBlobMaxInlineSharedRules(t *testing.T) {
	if err := host.ValidateBlobMaxInline(0); err != nil {
		t.Fatalf("阈值 0 应合法: %v", err)
	}
	if err := host.ValidateBlobMaxInline(65536); err != nil {
		t.Fatalf("阈值 65536 应合法: %v", err)
	}
	err := host.ValidateBlobMaxInline(-1)
	if err == nil {
		t.Fatal("负阈值应报错")
	}
	if !strings.Contains(err.Error(), "BlobMaxInline 不能为负") {
		t.Fatalf("报错文案应点名 BlobMaxInline，got %q", err.Error())
	}
}
