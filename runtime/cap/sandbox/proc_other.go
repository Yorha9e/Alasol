//go:build !windows

package sandbox

import (
	"os/exec"
)

// 平台相关的"怎么起进程 / 怎么杀进程"细节（与 proc_windows.go 成对）。
//
// 非 Windows 上本包的 env 永远构造不出来（后端 fail-close），所以这两个函数
// 只是为了让 exec.go 的**通用**代码能编译；它们不会被真实调用。

// hideWindow 在非 Windows 平台是空操作（没有控制台窗口这回事）。
func hideWindow(cmd *exec.Cmd) {
	_ = cmd
}

// applySpawn 在非 Windows 平台是空操作（syscall.SysProcAttr 没有 Token 字段）。
func applySpawn(cmd *exec.Cmd, opts SpawnOptions) {
	_ = cmd
	_ = opts
}

// killTree 在非 Windows 平台只杀直接子进程（与 local.kill_other.go 同款）。
func killTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
}

// isProcessGone 在非 Windows 平台恒 false（没有可判定的"进程已退出"错误码）。
func isProcessGone(err error) bool {
	_ = err
	return false
}

// resumeProcess 在非 Windows 平台是空操作（没有 CREATE_SUSPENDED 这回事）。
func resumeProcess(pid int) error {
	_ = pid
	return nil
}
