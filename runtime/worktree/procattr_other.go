//go:build !windows

package worktree

import "os/exec"

// hideWindow 在非 Windows 上是空操作（无控制台弹窗问题）。
func hideWindow(_ *exec.Cmd) {}
