// Package tui 是 alasol 的**终端日常交互面**（O6/B2）：bubbletea + lipgloss 的三面 TUI
// —— 对话 / 审批 / 在线表。独立包 + 独立进程（O6：core 只依赖 contracts，TUI 随时可加）。
//
// 定位（O6）：TUI 是终端里的日常交互面（对话 / 审批 / 流式），Web 是深度视图（可视化 /
// 多面板）。本 TUI 以**进程内 hub** 为后端（与 `alasol chat` 同源，但用 bubbletea 渲染），
// 审批经 approver 桥接进 bubbletea 事件循环（终端人审在 UI 内完成，不冲突 bubbletea 的
// 终端接管）。
//
// 技术栈（B2）：Go + bubbletea / lipgloss。输入框手写（不引 bubbles，少一个依赖）。
package tui

import (
	"context"
	"sync"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	tea "github.com/charmbracelet/bubbletea"
)

// Approver 把 hub 的人审请求桥接进 bubbletea 事件循环（core.Approver 实现）。
//
// 流程：loop 在工具调用处同步调 Approve → 本实现把请求 Send 进 tea 程序（render 到审批面），
// 然后阻塞等用户在 UI 里按 y/n → Update 调 resolve → 唤醒 Approve 返回决策给 loop。
//
// fail-close（D14/H8/陷阱 9）：ctx 取消（会话被中止）→ 返回 **deny**（不挂起、不放行）。
type Approver struct {
	program *tea.Program

	mu      sync.Mutex
	pending map[contracts.Ulid]chan contracts.ApprovalDecision
}

func NewApprover() *Approver {
	return &Approver{pending: map[contracts.Ulid]chan contracts.ApprovalDecision{}}
}

// Approve implements core.Approver：把请求转交 UI 并阻塞等决策。
func (a *Approver) Approve(ctx context.Context, req contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	ch := make(chan contracts.ApprovalDecision, 1)
	a.mu.Lock()
	a.pending[req.ID] = ch
	a.mu.Unlock()

	if a.program != nil {
		tool := ""
		if req.ToolCallID != nil {
			tool = string(*req.ToolCallID)
		}
		a.program.Send(approvalRequestMsg{id: req.ID, summary: req.Summary, tool: tool})
	}

	select {
	case dec := <-ch:
		return approvalResponse(req.ID, dec, "user:tui"), nil
	case <-ctx.Done():
		a.mu.Lock()
		delete(a.pending, req.ID)
		a.mu.Unlock()
		// fail-close：取消 = 无人显式放行 = deny。
		return approvalResponse(req.ID, contracts.ApprovalDecisionDeny, "auto:ctx-cancelled"), nil
	}
}

// resolve 供 UI 回填决策（唤醒阻塞中的 Approve）。未知/已处理的 id → 忽略。
func (a *Approver) resolve(id contracts.Ulid, allow bool) {
	a.mu.Lock()
	ch := a.pending[id]
	delete(a.pending, id)
	a.mu.Unlock()
	if ch == nil {
		return
	}
	dec := contracts.ApprovalDecisionDeny
	if allow {
		dec = contracts.ApprovalDecisionAllow
	}
	ch <- dec
}

// approvalResponse 组一条 ApprovalResponse。
func approvalResponse(id contracts.Ulid, dec contracts.ApprovalDecision, by string) contracts.ApprovalResponse {
	return contracts.ApprovalResponse{
		V:         1,
		RequestID: id,
		TS:        time.Now().UTC().Format(time.RFC3339),
		Decision:  dec,
		DecidedBy: strptr(by),
	}
}

func strptr(s string) *string { return &s }

// 确保实现 core.Approver。
var _ core.Approver = (*Approver)(nil)
