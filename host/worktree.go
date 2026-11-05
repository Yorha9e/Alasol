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
// # 边界（L2 per-session 执行上下文 · 件 2 落地后的现状）
//
// worktree 的**磁盘隔离**（独立工作区、junction-safe 清理）在本段落地；**工具执行**现在
// 也**落在该 worktree 里**了：session 绑到 worktree → 该 worktree 的路径就是本 session
// 的**会话工作区**（工具相对路径的锚点 + bash / git 的工作目录）——解析见
// host/exec_workspace.go，执行见件 1 的 core.ExecContext / runtime/tools。
//
// **但那仍是软边界，不是安全边界**：绝对路径原样透传（不被限制在工作区内）、越界判定
// 是**词法**的（不解析符号链接 / junction）。真隔离归沙箱（runtime/cap/sandbox 的 ACL +
// 受限 token，内核强制）。CreateSession 命中 Worktree 时会打一条醒目告警把这条边界
// 说清楚（见 host/discovery.go 的 worktreeIsolationNotice）。
//
// **sandbox / remote 执行面下不支持 per-session 工作区（v1）**：那两个执行面换掉了整个
// 工具执行环境，per-session 工作区在它们上面没有确定语义 → **创建时拒绝**（fail-close，
// 不静默回落成"照旧在进程 cwd 跑"）。

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
