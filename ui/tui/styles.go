package tui

import "github.com/charmbracelet/lipgloss"

// 用 lipgloss 做极简样式（B2：Go + lipgloss）。这里只包两个辅助，保持 TUI 轻量——
// 深度样式留给 Web 深度视图（O6）。

var (
	boldStyle = lipgloss.NewStyle().Bold(true)
	dimStyle  = lipgloss.NewStyle().Faint(true)
)

// bold 加粗（当前激活面标题）。
func bold(s string) string { return boldStyle.Render(s) }

// dim 变暗（非激活面 / 提示行）。
func dim(s string) string { return dimStyle.Render(s) }
