package worktree

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// gitAvailable 报告 git 是否可用（不可用则相关用例 skip 并注明，符合任务要求）。
func gitAvailable(t *testing.T) bool {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skipf("git 不可用，跳过 worktree git 用例: %v", err)
		return false
	}
	return true
}

// gitInitRepo 建一个带一次提交的临时 git 仓库，返回其路径。
func gitInitRepo(t *testing.T) string {
	t.Helper()
	if !gitAvailable(t) {
		return ""
	}
	dir := t.TempDir()
	testGit(t, dir, "init", "-q", "--initial-branch=main")
	testGit(t, dir, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "--allow-empty", "-q", "-m", "init")
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("写文件: %v", err)
	}
	testGit(t, dir, "add", "a.txt")
	testGit(t, dir, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-q", "-m", "add a")
	return dir
}

// testGit 在测试里直接跑 git（装配仓库用；非被测对象）。
func testGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	hideWindow(cmd)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

// TestValidName 钉死 name 校验：拒路径分隔符 / 父目录跳转 / 通配 / 空白。
func TestValidName(t *testing.T) {
	good := []string{"task-1", "abc_DEF-123", "a"}
	for _, n := range good {
		if err := validName(n); err != nil {
			t.Fatalf("合法名 %q 被拒: %v", n, err)
		}
	}
	bad := []string{"", "  ", "../evil", "a/b", `a\b`, "..", "C:x", "a*b", "a b", "a\tb", strings.Repeat("x", maxNameLen+1)}
	for _, n := range bad {
		if err := validName(n); err == nil {
			t.Fatalf("非法名 %q 应被拒", n)
		}
	}
}

// TestWorktreeCreateIsolateRemove 钉死 worktree 创建/隔离/清理（git 不可用则 skip）。
func TestWorktreeCreateIsolateRemove(t *testing.T) {
	repo := gitInitRepo(t)
	if repo == "" {
		return
	}
	root := filepath.Join(t.TempDir(), "wts")
	mgr, err := NewManager(root)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	ctx := context.Background()
	wt, err := mgr.Create(ctx, repo, "task-1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// ① worktree 检出了仓库内容（隔离工作区）。
	if _, err := os.Stat(filepath.Join(wt.Path, "a.txt")); err != nil {
		t.Fatalf("worktree 应含 a.txt: %v", err)
	}
	// ② 登记可查。
	if got, ok := mgr.Get("task-1"); !ok || got.Path != wt.Path {
		t.Fatalf("Get(task-1) = %+v ok=%v", got, ok)
	}

	// ③ 隔离：在 worktree 里新写文件，不污染主仓库工作区。
	if err := os.WriteFile(filepath.Join(wt.Path, "only-in-wt.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("写 worktree 文件: %v", err)
	}
	if _, err := os.Stat(filepath.Join(repo, "only-in-wt.txt")); !os.IsNotExist(err) {
		t.Fatalf("worktree 的新文件泄漏进了主仓库（隔离失败）: %v", err)
	}

	// ④ Remove：清理干净（junction-safe）。
	if err := mgr.Remove(ctx, "task-1"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := os.Lstat(wt.Path); !os.IsNotExist(err) {
		t.Fatalf("Remove 后 worktree 路径应不存在: %v", err)
	}
	if _, ok := mgr.Get("task-1"); ok {
		t.Fatal("Remove 后登记应清除")
	}
	// 主仓库的 a.txt 仍在（删 worktree 不碰主仓库）。
	if _, err := os.Stat(filepath.Join(repo, "a.txt")); err != nil {
		t.Fatalf("主仓库 a.txt 应仍在: %v", err)
	}
}

// TestWorktreeDuplicateName 钉死重复 name 拒绝。
func TestWorktreeDuplicateName(t *testing.T) {
	repo := gitInitRepo(t)
	if repo == "" {
		return
	}
	mgr, err := NewManager(filepath.Join(t.TempDir(), "wts"))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	ctx := context.Background()
	if _, err := mgr.Create(ctx, repo, "dup"); err != nil {
		t.Fatalf("首次 Create: %v", err)
	}
	if _, err := mgr.Create(ctx, repo, "dup"); err == nil {
		t.Fatal("重复 name 应被拒")
	}
}

// TestWorktreeJunctionDefense 钉死 junction-safe 清理（陷阱 6）：worktree 里放一个指向
// 仓库外目标的 junction/符号链接，Remove 后**目标必须完好**（不跟穿）。
func TestWorktreeJunctionDefense(t *testing.T) {
	repo := gitInitRepo(t)
	if repo == "" {
		return
	}
	root := filepath.Join(t.TempDir(), "wts")
	mgr, err := NewManager(root)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	ctx := context.Background()
	wt, err := mgr.Create(ctx, repo, "junc")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// 仓库外目标（带 marker），在 worktree 里造指向它的链接。
	outside := t.TempDir()
	marker := filepath.Join(outside, "marker.txt")
	if err := os.WriteFile(marker, []byte("precious"), 0o644); err != nil {
		t.Fatalf("写 marker: %v", err)
	}
	link := filepath.Join(wt.Path, "escape")
	makeLink(t, outside, link)

	if err := mgr.Remove(ctx, "junc"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	// 目标完好（链接没被跟穿删除）。
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("junction/符号链接被跟穿了！marker 应仍在: %v", err)
	}
	// 链接本身已随 worktree 删除。
	if _, err := os.Lstat(link); !os.IsNotExist(err) {
		t.Fatalf("worktree 内的链接应已删除: %v", err)
	}
}

// TestRemoveTreeSafeBasics：删普通目录树 + 删不存在路径不报错。
func TestRemoveTreeSafeBasics(t *testing.T) {
	tree := filepath.Join(t.TempDir(), "tree")
	if err := os.MkdirAll(filepath.Join(tree, "sub"), 0o755); err != nil {
		t.Fatalf("建树: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tree, "sub", "f.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("写文件: %v", err)
	}
	if err := removeTreeSafe(tree); err != nil {
		t.Fatalf("removeTreeSafe: %v", err)
	}
	if _, err := os.Lstat(tree); !os.IsNotExist(err) {
		t.Fatalf("树应被删尽: %v", err)
	}
	// 不存在 → 不报错（幂等）。
	if err := removeTreeSafe(filepath.Join(t.TempDir(), "nope")); err != nil {
		t.Fatalf("删不存在路径应幂等: %v", err)
	}
}
