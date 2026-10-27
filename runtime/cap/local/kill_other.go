//go:build !windows

package local

import (
	"os/exec"
)

// killTree 杀进程树（非 Windows：直接 Kill 直接子进程）。
//
// Unix 没有 Windows 那种"孙子继承管道句柄导致 Wait 挂起"的普遍问题
// （job control / 进程组是另一套机制，V1 沙箱再管），所以这里只杀直接子进程。
func killTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
}
