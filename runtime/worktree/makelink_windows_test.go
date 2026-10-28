//go:build windows

package worktree

import (
	"os/exec"
	"testing"
)

// makeLink 造一个指向 target 的 **junction**（Windows：mklink /J，免管理员）。
// 造不出 → skip（不影响被测逻辑，只是这条用例的环境前提）。
func makeLink(t *testing.T, target, link string) {
	t.Helper()
	cmd := exec.Command("cmd", "/c", "mklink", "/J", link, target)
	hideWindow(cmd)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("无法造 junction（mklink /J）：%v：%s", err, out)
	}
}
