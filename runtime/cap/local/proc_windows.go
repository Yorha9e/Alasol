//go:build windows

package local

import (
	"os/exec"
	"syscall"
)

// hideWindow 给子进程加 windowsHide（陷阱 5）：不起控制台窗口。
//
// 为什么必须 build tag：syscall.SysProcAttr 的字段集**按平台不同**——
// Windows 有 HideWindow，Unix 没有；写在通用文件里另一边编译不过。
//
// 只做 HideWindow：WER 崩溃弹窗的完整压制要靠 JobObject / 受限令牌，
// 那是沙箱层（V1）的职责，V0 不做也不假装做了。
func hideWindow(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
}
