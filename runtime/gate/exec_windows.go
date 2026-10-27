//go:build windows

package gateexec

import (
	"os/exec"
	"strconv"
	"syscall"
)

// setProcAttr 给子进程加 windowsHide（陷阱 5 / V10）：不起控制台窗口。
// 为什么 build tag：syscall.SysProcAttr 的字段集按平台不同，Windows 有 HideWindow。
func setProcAttr(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
}

// killTree 杀进程树（Windows：taskkill /T /F /PID，陷阱 5 / V10 的定案做法）。
//
// 为什么必须杀树而不是 cmd.Process.Kill()：孙子进程继承 stdout/stderr 管道写端，
// 只杀直接子进程的话 cmd.Wait 要等孙子自己退出才返回。杀树让管道立刻 EOF。
// taskkill 自身也带 HideWindow；失败（不存在 / 进程已退出）时静默——由 WaitDelay 兜底。
func killTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	tk := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(pid))
	tk.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	_ = tk.Run()
}
