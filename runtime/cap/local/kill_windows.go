//go:build windows

package local

import (
	"os/exec"
	"strconv"
	"syscall"
)

// killTree 杀进程树（Windows：taskkill /T /F /PID，陷阱 5 的定案做法）。
//
// 为什么必须杀树而不是 cmd.Process.Kill()：孙子进程会继承 stdout/stderr 管道的写端，
// 只杀直接子进程的话 cmd.Wait 要等孙子自己退出才返回——本机实测 cmd /c ping -n 30
// 取消后 **29 秒**才回来。杀树让管道立刻 EOF，Wait ~0.2s 返回。
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
	_ = tk.Run() // 进程已退出时 taskkill 报错，忽略（Cancel 不该把噪声变成失败）
}
