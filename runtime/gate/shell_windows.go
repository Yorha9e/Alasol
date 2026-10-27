//go:build windows

package gateexec

// Windows 上经 cmd /c 跑不透明门禁命令（W9：命令是不透明 shell 字符串，
// 这是全包唯一走 shell 的地方）。与 runtime/tools 的 shell_windows.go 同款约定。
const (
	shellName = "cmd"
	shellFlag = "/c"
)
