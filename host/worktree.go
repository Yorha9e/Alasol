package host

import (
	"context"
	"errors"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/runtime/worktree"
)

// 本文件是 hub 侧的多 worktree 隔离 API（v1-S5 D）：并行任务各占一个独立 git worktree
// 工作区。底层创建/清理/junction-safe 全在 runtime/worktree；本层只做装配面的方法 +
// session 绑定（SessionSpec.Worktree，见 discovery.go）+ HTTP 绑定。
//
// 边界说明：worktree 的**磁盘隔离**（独立工作区、junction-safe 清理）在本段落地；但工具
// 执行（read/write/bash）仍跑在 hub 进程 cwd——把工具执行真正 chroot 进各 worktree 需要
// 改 core/loop 的工具执行上下文，属更深改动（与 X1 冲突），留作后续（见汇报"缺口"）。
//
// 因此 session 绑到 worktree（SessionSpec.Worktree）**只**影响自动 gate 的 repo_id 计算与
// 工作区登记，**不**改变工具的工作目录：CreateSession 命中 Worktree 时会打一条醒目告警
//（"⚠ 工具级隔离未启用…"，见 host/discovery.go 的 worktreeIsolationNotice），
// SessionSpec 的文档也据此写清了边界——别让用户以为"绑了 worktree = 工具在 worktree 里跑"。

// CreateWorktree 在 repoPath 仓库上建一个 worktree 并登记（并行任务工作区）。
func (h *Hub) CreateWorktree(ctx context.Context, repoPath, name string) (worktree.Worktree, error) {
	if h == nil {
		return worktree.Worktree{}, errors.New("host: Hub 为 nil")
	}
	return h.worktrees.Create(ctx, repoPath, name)
}

// RemoveWorktree 注销并删除一个 worktree（junction-safe）。
func (h *Hub) RemoveWorktree(ctx context.Context, name string) error {
	if h == nil {
		return errors.New("host: Hub 为 nil")
	}
	return h.worktrees.Remove(ctx, name)
}

// Worktrees 返回所有已登记 worktree（按 name 排序）。
func (h *Hub) Worktrees() []worktree.Worktree {
	if h == nil {
		return nil
	}
	return h.worktrees.List()
}

// Worktree 取一个已登记 worktree（不存在 → ok=false）。
func (h *Hub) Worktree(name string) (worktree.Worktree, bool) {
	if h == nil {
		return worktree.Worktree{}, false
	}
	return h.worktrees.Get(name)
}

// WorktreeRoot 返回受管 worktree 根目录（诊断）。
func (h *Hub) WorktreeRoot() string {
	if h == nil {
		return ""
	}
	return h.worktrees.Root()
}

// SessionWorktree 返回某 session 绑定的 worktree 名（未绑 → 空）。
func (h *Hub) SessionWorktree(sessionID string) string {
	if h == nil {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.specs[contracts.Ulid(sessionID)].Worktree
}
