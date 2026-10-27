//go:build windows

package tools

// Windows 上用 cmd /c 执行 bash 工具的命令。
//
// 为什么 build tag：shell 的名字和传脚本的参数**按平台不同**，写在通用文件里
// 就没法编译（且容易在 runtime.GOOS 分支里漏掉一边）。
//
// 已知边界：cmd 的引号/元字符语义与 bash 不完全一致（&&、||、重定向的细节、
// %VAR% 展开时机）。给模型的说明里应提示"命令按 Windows cmd 语法解释"。
const (
	shellName = "cmd"
	shellFlag = "/c"
	// shellSyntax 是给人/模型看的语法标签（用于错误文案与工具描述）。
	shellSyntax = "Windows cmd"
)
