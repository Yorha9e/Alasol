//go:build windows

package mcp

import (
	"log/slog"
	"os/exec"
	"strconv"
	"syscall"
)

// hideWindow 给 server 子进程加 windowsHide（陷阱 5）：不起控制台窗口。
//
// 为什么必须 build tag：syscall.SysProcAttr 的字段集**按平台不同**——
// Windows 有 HideWindow，Unix 没有；写在通用文件里另一边编译不过
// （runtime/cap/local 同款做法）。
func hideWindow(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
}

// killTree 杀进程树（Windows：taskkill /T /F /PID，陷阱 5 的定案做法）。
//
// 为什么必须杀树而不是 cmd.Process.Kill()：孙子进程会继承 stdout/stderr 管道的
// 写端，只杀直接子进程的话 cmd.Wait 要等孙子自己退出才返回——runtime/cap/local
// 的 exec.go 里有本机实测（cmd /c ping -n 30 取消后 **29 秒**才回来）。
//
// taskkill 自身也带 windowsHide：杀进程不该再弹一个黑窗。
// taskkill 失败（不存在 / 进程已退出）时静默返回——由 WaitDelay 兜底。
func killTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	tk := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(pid))
	tk.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if err := tk.Run(); err != nil {
		// 进程已退出时 taskkill 报错，忽略（收尸路径不该把噪声变成失败）。
		mcpLog.Debug("mcp: taskkill 失败（进程可能已退出）", "pid", pid, "err", err.Error())
	}
}

// mcpLog 是本包的内部告警出口（收尸失败这类"不致命但要留痕"的事）。
//
// 与 host.hubLog 同性质：默认 slog.Default()，**不记录任何凭据/配置值**。
var mcpLog = slog.Default()
