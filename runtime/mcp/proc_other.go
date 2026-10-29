//go:build !windows

package mcp

import (
	"log/slog"
	"os/exec"
)

// hideWindow 非 Windows 是空操作（没有控制台窗口这个概念）。
func hideWindow(cmd *exec.Cmd) {}

// killTree 杀进程树（非 Windows：直接 Kill 直接子进程）。
//
// Unix 没有 Windows 那种"孙子继承管道句柄导致 Wait 挂起"的普遍问题
// （job control / 进程组是另一套机制），所以这里只杀直接子进程
// （runtime/cap/local 同款口径）。
func killTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	if err := cmd.Process.Kill(); err != nil {
		mcpLog.Debug("mcp: Kill 失败（进程可能已退出）", "pid", cmd.Process.Pid, "err", err.Error())
	}
}

// mcpLog 是本包的内部告警出口（收尸失败这类"不致命但要留痕"的事）。
//
// 与 host.hubLog 同性质：默认 slog.Default()，**不记录任何凭据/配置值**。
var mcpLog = slog.Default()
