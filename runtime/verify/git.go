// Package verify 提供 core/durable.Verifier 的 runtime 侧实现：基于 anchors 核验外部状态（X2 第二类）。
//
// core 只定义 Verifier 接口（C2/D15：core 不碰 I/O）；本包用 git 落地「anchor kind=git-commit」
// 的核验——崩溃续跑时，一个 replay=verify 的操作（如 git push）在重放前先查外部事实（目标 commit
// 是否已存在/已并入），核验通过=已完成不重放，不通过=重放。
//
// 依赖方向：runtime → core（C3），故可 import core/durable 做编译期断言。
package verify

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/durable"
)

// anchorGitCommit 是核验 git 提交的锚点类型（与 intent 里 Anchor.Kind 对应）。
const anchorGitCommit = "git-commit"

// 核验模式。
const (
	// ModeExists（默认）查 commit 对象是否存在（git cat-file -e <sha>^{commit}）。
	ModeExists = "exists"
	// ModeAncestor 查 commit 是否为 HEAD 的祖先（git merge-base --is-ancestor <sha> HEAD）——
	// 即该 commit 是否已并入当前分支。
	ModeAncestor = "ancestor"
)

// 编译期证明：GitCommitVerifier 实现 durable.Verifier。
var _ durable.Verifier = GitCommitVerifier{}

// GitCommitVerifier 按 anchor kind=git-commit 用 git 核验外部状态（X2 verify）。
type GitCommitVerifier struct {
	// RepoDir 是 git 仓库工作目录（git 命令的 cwd）。
	RepoDir string
	// Mode 核验模式：ModeExists（默认）或 ModeAncestor。
	Mode string
}

// Verify 核验 op 携带的所有 git-commit 锚点（X2）。
//
//   - 无 git-commit 锚点 → 返回 error（本核验器只懂 git-commit；交给 durable 转 manual，fail-safe）；
//   - 全部锚点通过 → (true, nil) = 外部事实表明已完成 → 不重放；
//   - 任一锚点查无此 commit → (false, nil) = 未完成 → 重放；
//   - git 命令本身出错（仓库不可用等）→ (false, err) → durable 转 manual（无法确认，不盲目重放）。
func (v GitCommitVerifier) Verify(ctx context.Context, op contracts.InFlightOp) (bool, error) {
	mode := v.Mode
	if mode == "" {
		mode = ModeExists
	}
	if mode != ModeExists && mode != ModeAncestor {
		return false, fmt.Errorf("verify: 未知核验模式 %q（want %q 或 %q）", v.Mode, ModeExists, ModeAncestor)
	}
	var shas []string
	for _, a := range op.Anchors {
		if a.Kind == anchorGitCommit && strings.TrimSpace(a.Value) != "" {
			shas = append(shas, strings.TrimSpace(a.Value))
		}
	}
	if len(shas) == 0 {
		return false, fmt.Errorf("verify: op %s 无 %s 锚点可核验", op.ToolCallID, anchorGitCommit)
	}
	for _, sha := range shas {
		ok, err := v.checkOne(ctx, mode, sha)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil // 有一个不存在 → 未完成
		}
	}
	return true, nil
}

// checkOne 用 git 核验单个 commit。
func (v GitCommitVerifier) checkOne(ctx context.Context, mode, sha string) (bool, error) {
	git, err := gitExecutable()
	if err != nil {
		return false, err
	}
	var args []string
	switch mode {
	case ModeAncestor:
		args = []string{"merge-base", "--is-ancestor", sha, "HEAD"}
	default:
		args = []string{"cat-file", "-e", sha + "^{commit}"}
	}
	cmd := exec.CommandContext(ctx, git, args...)
	cmd.Dir = v.RepoDir
	if err := cmd.Run(); err != nil {
		// 非零退出 = 核验不通过（commit 不存在/非祖先）；把"命令起不来"与"查无此物"区分开。
		if ee, ok := err.(*exec.ExitError); ok {
			_ = ee
			return false, nil
		}
		return false, fmt.Errorf("verify: git %s %q 执行失败: %w", strings.Join(args, " "), sha, err)
	}
	return true, nil
}

// gitExecutable 固化 git 绝对路径（V10 / 陷阱 7：绝对路径 execFile，不走 shell，防 cwd 劫持）。
var (
	gitOnce sync.Once
	gitBin  string
	gitErr  error
)

func gitExecutable() (string, error) {
	gitOnce.Do(func() {
		p, err := exec.LookPath("git")
		if err != nil {
			gitErr = fmt.Errorf("verify: 找不到 git（%w）", err)
			return
		}
		abs, err := filepath.Abs(p)
		if err != nil {
			gitErr = fmt.Errorf("verify: 解析 git 绝对路径失败: %w", err)
			return
		}
		gitBin = abs
	})
	return gitBin, gitErr
}
