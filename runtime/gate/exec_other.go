//go:build !windows

package gateexec

import (
	"os/exec"
	"syscall"
)

// setProcAttr 让子进程独立成进程组（Setpgid），killTree 才能整组杀，避免孙子进程
// 攥着管道写端让 Wait 挂起。与 exec_windows.go 成对，exec.go 不写平台分支。
func setProcAttr(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killTree 杀进程组（Unix：对负 pid 发 SIGKILL，一次带走整组）。
func killTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	// Setpgid 让子进程自成进程组，pgid == 子进程 pid；杀 -pgid 即杀整组。
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
