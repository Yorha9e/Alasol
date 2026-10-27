//go:build !windows

package terminal

// lineEnd 是非 Windows 的行尾（\n）。与 newline_windows.go 成对存在。
const lineEnd = "\n"
