//go:build !windows

package gateexec

// 非 Windows 经 bash -c 跑不透明门禁命令（W9：命令是不透明 shell 字符串）。
// 与 shell_windows.go 成对，run.go 不写平台分支。
const (
	shellName = "bash"
	shellFlag = "-c"
)
