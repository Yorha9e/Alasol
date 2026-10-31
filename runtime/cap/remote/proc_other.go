//go:build !windows

package remote

import (
	"os/exec"
)

// hideWindow 在非 Windows 平台是空操作（没有控制台窗口这回事）。
// 与 proc_windows.go 成对存在，让 exec.go 不用写任何平台分支。
func hideWindow(cmd *exec.Cmd) {
	_ = cmd
}
