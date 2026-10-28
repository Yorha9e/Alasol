package tui

import (
	tea "github.com/charmbracelet/bubbletea"
)

// Run 起 TUI（阻塞到用户退出）。
//
// 装配（进程内 hub + bubbletea 程序 + 审批桥）：
//  1. 建审批桥（此时还没有 program，稍后补）；
//  2. 建模型（持 hub/approver 引用）；
//  3. 建 tea 程序（AltScreen 全屏）；
//  4. 把 program 补进审批桥（Approve 要靠它 Send 消息进事件循环）；
//  5. 装配 hub（Approver = 审批桥，由调用方经 Options 传入已装好的 hub）；
//  6. 把 hub/session 补进模型，跑程序。
//
// opts.Hub 为 nil → 返回错误（调用方先装配好 hub 再进来；hub 的唯一组合根是 host.New）。
func Run(opts Options) error {
	if opts.Hub == nil {
		return errNoHub
	}
	appr := opts.Approver
	if appr == nil {
		appr = NewApprover()
	}
	m := newModel(Options{Hub: opts.Hub, Session: opts.Session, Approver: appr})
	p := tea.NewProgram(m, tea.WithAltScreen())
	appr.program = p // 审批桥现在能把请求 Send 进事件循环
	_, err := p.Run()
	return err
}

// errNoHub 是 Run 的前置错误（hub 必填）。
type errNoHubT struct{}

func (errNoHubT) Error() string { return "tui: Options.Hub 必填（先用 host.New 装配 hub）" }

var errNoHub = errNoHubT{}
