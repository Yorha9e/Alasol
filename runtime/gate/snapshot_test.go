package gateexec

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestGitExecutableAbsolute 钉住 V10/陷阱7：git 走绝对路径 execFile（拿到后固化），不经 shell。
func TestGitExecutableAbsolute(t *testing.T) {
	gitAvailable(t)
	git, err := gitExecutable()
	if err != nil {
		t.Fatalf("gitExecutable: %v", err)
	}
	if !filepath.IsAbs(git) {
		t.Errorf("git 路径应为绝对路径，得到 %q", git)
	}
	// 二次调用返回同一路径（sync.Once 固化）。
	git2, _ := gitExecutable()
	if git2 != git {
		t.Errorf("gitExecutable 应固化：%q vs %q", git, git2)
	}
}

// TestSnapshotExcludesUncommittedChanges 是验收②：未提交改动结构性不进 CI（快照只含 commit 内容）。
//
// 造脏工作区（改 a.txt 未提交 + 加未跟踪 b.txt），对 HEAD 建快照：
// 快照里的 a.txt 必须是**提交时**的内容（不是脏改动），b.txt 根本不存在。
func TestSnapshotExcludesUncommittedChanges(t *testing.T) {
	repo, commit := initRepo(t) // a.txt = "committed-a"
	// 造脏工作区。
	writeFile(t, filepath.Join(repo, "a.txt"), "dirty-uncommitted-change")
	writeFile(t, filepath.Join(repo, "b.txt"), "untracked-should-not-appear")

	dir, cleanup, err := Snapshot(context.Background(), repo, commit)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer cleanup()

	if got := readFile(t, filepath.Join(dir, "a.txt")); got != "committed-a" {
		t.Errorf("快照含未提交改动：a.txt = %q，期望 committed-a", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "b.txt")); !os.IsNotExist(err) {
		t.Errorf("未跟踪文件不应进快照：b.txt stat err = %v（应为 NotExist）", err)
	}
}

// TestSnapshotCleanupRemovesWorktree：cleanup 后快照目录消失、worktree 注册注销；cleanup 幂等。
func TestSnapshotCleanupRemovesWorktree(t *testing.T) {
	repo, commit := initRepo(t)
	dir, cleanup, err := Snapshot(context.Background(), repo, commit)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("快照目录应存在：%v", err)
	}

	cleanup()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("cleanup 后快照目录应消失，stat err = %v", err)
	}
	// worktree 注册应注销：list 只剩主 worktree（1 条 "worktree " 记录）。
	out, _, _ := gitRun(context.Background(), repo, "worktree", "list", "--porcelain")
	if n := strings.Count(out, "worktree "); n != 1 {
		t.Errorf("cleanup 后应只剩 1 个 worktree，得到 %d：\n%s", n, out)
	}
	// 幂等：再调一次不 panic、不报错。
	cleanup()
}

// TestSnapshotRejectsBadCommit：无法解析的 commit 报错，且不留下临时目录。
func TestSnapshotRejectsBadCommit(t *testing.T) {
	repo, _ := initRepo(t)
	_, cleanup, err := Snapshot(context.Background(), repo, "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	if err == nil {
		cleanup()
		t.Fatal("无法解析的 commit 应报错")
	}
}

// TestConcurrentSnapshots：并发建多个快照互不干扰（-race 下跑），各自目录独立、内容正确。
func TestConcurrentSnapshots(t *testing.T) {
	repo, commit := initRepo(t)
	const n = 5
	var wg sync.WaitGroup
	dirs := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dir, cleanup, err := Snapshot(context.Background(), repo, commit)
			if err != nil {
				errs[i] = err
				return
			}
			defer cleanup()
			dirs[i] = dir
			if got := readFile(t, filepath.Join(dir, "a.txt")); got != "committed-a" {
				errs[i] = errBadContent(got)
			}
		}(i)
	}
	wg.Wait()

	seen := make(map[string]bool, n)
	for i := range dirs {
		if errs[i] != nil {
			t.Errorf("快照 %d：%v", i, errs[i])
		}
		if dirs[i] == "" {
			continue
		}
		if seen[dirs[i]] {
			t.Errorf("快照目录重复：%q", dirs[i])
		}
		seen[dirs[i]] = true
	}
}

func errBadContent(got string) error {
	return &contentError{got}
}

type contentError struct{ got string }

func (e *contentError) Error() string { return "快照 a.txt 内容错：" + e.got }
