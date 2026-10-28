//go:build windows

package worktree

import (
	"os/exec"
	"syscall"
)

// hideWindow 在 Windows 上给子进程设 HideWindow（陷阱 5：压制 git 的控制台弹窗）。
func hideWindow(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
}
