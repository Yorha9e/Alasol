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

// TestWorktreeRegistryRebuildAfterRestart（v2-S5 清账③）：登记表**不再只是进程内存**——
// hub 重启后从磁盘的 git 元数据重建（`git worktree list --porcelain`）。
//
// 场景：进程 A 建了两个 worktree → 进程 B（新的 Manager，同一 root）启动时登记表是空的
// → Rebuild 后两个都回来，且 Path/Repo 与磁盘事实一致。
func TestWorktreeRegistryRebuildAfterRestart(t *testing.T) {
	repo := gitInitRepo(t)
	if repo == "" {
		return
	}
	root := filepath.Join(t.TempDir(), "wts")
	ctx := context.Background()

	// 进程 A：建两个 worktree。
	mgrA, err := NewManager(root)
	if err != nil {
		t.Fatalf("NewManager A: %v", err)
	}
	for _, name := range []string{"task-1", "task-2"} {
		if _, err := mgrA.Create(ctx, repo, name); err != nil {
			t.Fatalf("Create %s: %v", name, err)
		}
	}

	// 进程 B：同一 root，全新 Manager（= hub 重启）。
	mgrB, err := NewManager(root)
	if err != nil {
		t.Fatalf("NewManager B: %v", err)
	}
	if got := mgrB.List(); len(got) != 0 {
		t.Fatalf("新 Manager 的登记表应为空（进程内存），得到 %d 条", len(got))
	}
	n, err := mgrB.Rebuild(ctx)
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if n != 2 {
		t.Fatalf("Rebuild 应补登 2 条，得到 %d", n)
	}
	list := mgrB.List()
	if len(list) != 2 || list[0].Name != "task-1" || list[1].Name != "task-2" {
		t.Fatalf("重建后的登记表不对: %+v", list)
	}
	for _, wt := range list {
		if wt.Path != filepath.Join(root, wt.Name) {
			t.Fatalf("%s 的 Path 错: %q", wt.Name, wt.Path)
		}
		if wt.Repo == "" {
			t.Fatalf("%s 的 Repo（主仓库）应重建出来", wt.Name)
		}
		if wt.CreatedAt.IsZero() {
			t.Fatalf("%s 的 CreatedAt 应有近似值（目录 mtime）", wt.Name)
		}
		if _, err := os.Stat(filepath.Join(wt.Path, "a.txt")); err != nil {
			t.Fatalf("%s 的工作区应仍在磁盘上: %v", wt.Name, err)
		}
	}
	// 重建出来的登记能直接 Remove（清理路径与 Create 出来的一致）。
	if err := mgrB.Remove(ctx, "task-1"); err != nil {
		t.Fatalf("重建后 Remove: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "task-1")); !os.IsNotExist(err) {
		t.Fatalf("Remove 后 task-1 应删掉")
	}
}

// TestWorktreeRebuildIdempotentAndSkipsStale：Rebuild 幂等（不覆盖已登记元数据），
// 且**不把非 worktree 的目录登记进来**（没有 git 元数据支撑的登记是假登记）。
func TestWorktreeRebuildIdempotentAndSkipsStale(t *testing.T) {
	repo := gitInitRepo(t)
	if repo == "" {
		return
	}
	root := filepath.Join(t.TempDir(), "wts")
	ctx := context.Background()
	mgr, err := NewManager(root)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	created, err := mgr.Create(ctx, repo, "task-1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// root 下放一个**不是** git worktree 的目录（残留/杂物）。
	stale := filepath.Join(root, "stale-dir")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatalf("建 stale 目录: %v", err)
	}

	// 重复 Rebuild：第二次不该改任何东西（幂等合并）。
	n1, err := mgr.Rebuild(ctx)
	if err != nil {
		t.Fatalf("Rebuild 1: %v", err)
	}
	if n1 != 0 {
		t.Fatalf("已登记的条目不该被重复补登，得到 %d", n1)
	}
	got, ok := mgr.Get("task-1")
	if !ok || got.Path != created.Path || !got.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("Rebuild 不该覆盖已登记元数据: got=%+v want=%+v", got, created)
	}
	// 再跑一次仍幂等。
	if n2, err := mgr.Rebuild(ctx); err != nil || n2 != 0 {
		t.Fatalf("Rebuild 2 = (%d, %v)，want (0, nil)", n2, err)
	}
	// stale 目录不在登记表里。
	if _, ok := mgr.Get("stale-dir"); ok {
		t.Fatal("非 git worktree 的目录不该被登记")
	}
	if list := mgr.List(); len(list) != 1 || list[0].Name != "task-1" {
		t.Fatalf("登记表应只有 task-1: %+v", list)
	}
}

// TestWorktreeRebuildNeverRegistersMainRepo（p2-1 回归）：**主仓库永不进登记表**——
// 主仓库完全可能就落在受管 root 下（用户把 root 指到仓库旁边/里面）。
//
// 旧口径只判"目录名相等"：root/mainrepo 既是主仓库、目录名又与被扫的目录同名时，
// 主仓库会被登记成受管 worktree → 随后的 Remove **直接删掉用户主仓库的工作区**
// （不可逆！）。修法：git worktree list 的**第一条**（主仓库）按序号跳过，不靠名字猜。
func TestWorktreeRebuildNeverRegistersMainRepo(t *testing.T) {
	repo := gitInitRepo(t)
	if repo == "" {
		return
	}
	// 把主仓库搬进受管 root 下（模拟"用户把 root 指到仓库所在目录"）。
	root := filepath.Join(t.TempDir(), "wts")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("建 root: %v", err)
	}
	mainInRoot := filepath.Join(root, "mainrepo")
	if err := os.Rename(repo, mainInRoot); err != nil {
		t.Skipf("搬不动仓库（跨设备/占用中），跳过: %v", err)
	}
	ctx := context.Background()
	mgr, err := NewManager(root)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	// 从 root 里的主仓库拉一个 linked worktree（也在 root 下）。
	if _, err := mgr.Create(ctx, mainInRoot, "task-1"); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// 全新 Manager（= 重启）→ Rebuild。
	mgr2, err := NewManager(root)
	if err != nil {
		t.Fatalf("NewManager 2: %v", err)
	}
	n, err := mgr2.Rebuild(ctx)
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if n != 1 {
		t.Fatalf("Rebuild 应只补登 task-1（主仓库永不登记），得到 %d", n)
	}
	list := mgr2.List()
	if len(list) != 1 || list[0].Name != "task-1" {
		t.Fatalf("登记表里不该有主仓库: %+v", list)
	}
	if _, ok := mgr2.Get("mainrepo"); ok {
		t.Fatalf("主仓库被登记成受管 worktree 了（Remove 会删掉用户主仓库的工作区）: %+v", list)
	}

	// 主仓库目录必须还在（Remove 不得删主仓库）。
	if err := mgr2.Remove(ctx, "mainrepo"); err == nil {
		t.Fatal("未登记的主仓库不该能被 Remove")
	}
	if _, err := os.Stat(filepath.Join(mainInRoot, "a.txt")); err != nil {
		t.Fatalf("主仓库工作区必须完好: %v", err)
	}
	// task-1 仍能正常清理（登记的是真 worktree）。
	if err := mgr2.Remove(ctx, "task-1"); err != nil {
		t.Fatalf("Remove task-1: %v", err)
	}
	if _, err := os.Stat(filepath.Join(mainInRoot, "a.txt")); err != nil {
		t.Fatalf("主仓库工作区必须完好: %v", err)
	}
}
