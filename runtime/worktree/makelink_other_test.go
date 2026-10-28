//go:build !windows

package worktree

import (
	"os"
	"testing"
)

// makeLink 造一个指向 target 的**符号链接**（Unix：os.Symlink）。
func makeLink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("无法造符号链接: %v", err)
	}
}
