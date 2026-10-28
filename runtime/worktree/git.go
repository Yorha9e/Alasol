package worktree

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"sync"
)

// 固化 git 绝对路径 + 不经 shell 执行（镜像 runtime/gate 的成熟做法，陷阱 7：git 走 shell
// 会被 cwd 劫持；绝对路径 execFile git.exe 杜绝之）。windowsHide 压制控制台弹窗（陷阱 5）。
var (
	gitOnce sync.Once
	gitBin  string
	gitErr  error
)

func gitExecutable() (string, error) {
	gitOnce.Do(func() {
		p, err := exec.LookPath("git")
		if err != nil {
			gitErr = fmt.Errorf("worktree: 找不到 git（%w）", err)
			return
		}
		abs, err := filepath.Abs(p)
		if err != nil {
			gitErr = fmt.Errorf("worktree: 解析 git 绝对路径失败: %w", err)
			return
		}
		gitBin = abs
	})
	return gitBin, gitErr
}

// gitRun 跑一条 git 命令（绝对路径、不走 shell、ctx 可取消）。dir 作为 git 工作目录。
func gitRun(ctx context.Context, dir string, args ...string) (stdout string, exitCode int, err error) {
	git, err := gitExecutable()
	if err != nil {
		return "", -1, err
	}
	cmd := exec.CommandContext(ctx, git, args...)
	cmd.Dir = dir
	hideWindow(cmd) // Windows：压制弹窗（陷阱 5）
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return string(out), ee.ExitCode(), nil
		}
		return string(out), -1, err
	}
	return string(out), 0, nil
}
