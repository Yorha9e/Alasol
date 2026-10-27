package gateexec

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// gitExecutable 固化 git 的**绝对路径**（V10 / 陷阱 7：绝对路径 execFile git.exe，不经 shell）。
//
// exec.LookPath 只拿一次，随即 filepath.Abs 固化成绝对路径并 sync.Once 缓存——
// 之后所有 git 调用都走这个绝对路径，杜绝"走 shell 被 cwd 劫持"。
var (
	gitOnce sync.Once
	gitBin  string
	gitErr  error
)

func gitExecutable() (string, error) {
	gitOnce.Do(func() {
		p, err := exec.LookPath("git")
		if err != nil {
			gitErr = fmt.Errorf("gateexec: 找不到 git（%w）", err)
			return
		}
		abs, err := filepath.Abs(p)
		if err != nil {
			gitErr = fmt.Errorf("gateexec: 解析 git 绝对路径失败（%w）", err)
			return
		}
		gitBin = abs
	})
	return gitBin, gitErr
}

// gitRun 跑一条 git 命令（绝对路径、不走 shell、ctx 可取消、杀进程树）。
// dir 作为 git 的工作目录（仓库或快照 worktree）；env=nil 继承父 env（git 不需要注入）。
func gitRun(ctx context.Context, dir string, args ...string) (stdout string, exitCode int, err error) {
	git, err := gitExecutable()
	if err != nil {
		return "", -1, err
	}
	return runProcess(ctx, dir, nil, append([]string{git}, args...))
}

// revParse 解析一个 rev（如 HEAD / commit sha）为完整 sha；失败返回 error。
func revParse(ctx context.Context, dir, rev string) (string, error) {
	out, code, err := gitRun(ctx, dir, "rev-parse", rev)
	if err != nil {
		return "", err
	}
	if code != 0 {
		return "", fmt.Errorf("gateexec: git rev-parse %s 退出码 %d", rev, code)
	}
	sha := strings.TrimSpace(out)
	if sha == "" {
		return "", fmt.Errorf("gateexec: git rev-parse %s 输出为空", rev)
	}
	return sha, nil
}

// Snapshot 把 commit 检出到一次性 detached worktree（V1：未提交改动结构性不进快照）。
//
// 返回快照目录、cleanup 与 error。cleanup 幂等可多次调用：先 git worktree remove --force
// 注销并删快照目录，再兜底删临时父目录（junction 防御，见 removeTreeSafe）。
// 用 background ctx 做清理——cleanup 可能在调用方 ctx 取消后才执行。
func Snapshot(ctx context.Context, repoPath, commit string) (dir string, cleanup func(), err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	if strings.TrimSpace(commit) == "" {
		return "", nil, fmt.Errorf("gateexec: commit 为空（快照需要一个 commit）")
	}
	if _, err := gitExecutable(); err != nil {
		return "", nil, err
	}
	// 先确认 commit 可解析（^{commit} 要求它确实指向一个 commit 对象）。
	if _, err := revParse(ctx, repoPath, commit+"^{commit}"); err != nil {
		return "", nil, fmt.Errorf("gateexec: commit %q 无法解析为 commit：%w", commit, err)
	}

	base, err := os.MkdirTemp("", "alasol-gate-snap-")
	if err != nil {
		return "", nil, fmt.Errorf("gateexec: 创建临时目录失败：%w", err)
	}
	// worktree 路径让 git 自己建（不存在），放在临时父目录下，cleanup 连带删父目录。
	wtDir := filepath.Join(base, "worktree")

	_, code, err := gitRun(ctx, repoPath, "worktree", "add", "--detach", wtDir, commit)
	if err != nil || code != 0 {
		_ = removeTreeSafe(base)
		return "", nil, fmt.Errorf("gateexec: 创建快照 worktree 失败（exit=%d）：%v", code, err)
	}

	cleanup = func() {
		bg := context.Background()
		// 注销 + 删快照目录。失败（如已被外部删）不阻塞兜底删除。
		_, _, _ = gitRun(bg, repoPath, "worktree", "remove", "--force", wtDir)
		_ = removeTreeSafe(wtDir)
		_ = removeTreeSafe(base)
	}
	return wtDir, cleanup, nil
}
