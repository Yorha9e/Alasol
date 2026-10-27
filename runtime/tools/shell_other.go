//go:build !windows

package tools

// 非 Windows 用 bash -c 执行 bash 工具的命令。
// 与 shell_windows.go 成对存在，tools.go 不写任何平台分支。
const (
	shellName = "bash"
	shellFlag = "-c"
	// shellSyntax 是给人/模型看的语法标签（用于错误文案与工具描述）。
	shellSyntax = "POSIX bash"
)
