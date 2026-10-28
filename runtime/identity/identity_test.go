package identity

// runtime/identity 的测试：路径规范化 + project_id / repo_id（S13/S14 + 08）。
// 覆盖：同 repo 多 worktree 聚为同 repo_id、不同 project_id；大小写差异（Windows）；
// junction（Windows）/ symlink；.git 为文件（worktree）与目录（普通 repo）都要认。

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// 测试辅助（git 相关，同 runtime/verify 约定：git 不可用则跳过）
// ---------------------------------------------------------------------------

func gitAvailable(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skipf("git 不可用，跳过：%v", err)
	}
}

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v (dir=%s): %v\n%s", args, dir, err, out)
	}
	return strings.TrimSpace(string(out))
}

func writeFileT(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("写 %s: %v", path, err)
	}
}

// validShortID 校验短 ID 格式：26 字符 Crockford base32（与 ULID 同字符集，排除 I/L/O/U）。
func validShortID(s string) bool {
	if len(s) != 26 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'A' || c > 'Z') {
			return false
		}
		if c == 'I' || c == 'L' || c == 'O' || c == 'U' {
			return false
		}
	}
	return true
}

// initRepo 建一个带一次提交的普通 git 仓库（.git 为目录），返回其路径。
func initRepo(t *testing.T, dir string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	runGit(t, dir, "init", "-q")
	runGit(t, dir, "config", "user.email", "test@example.com")
	runGit(t, dir, "config", "user.name", "test")
	runGit(t, dir, "config", "commit.gpgsign", "false")
	writeFileT(t, filepath.Join(dir, "README.md"), "hello")
	runGit(t, dir, "add", ".")
	runGit(t, dir, "commit", "-q", "-m", "init")
	return dir
}

// ---------------------------------------------------------------------------
// CanonicalPath 纯变换 + 归一
// ---------------------------------------------------------------------------

func TestCanonicalizeTrailingSlashAndSeparator(t *testing.T) {
	if got := canonicalize("/a/b/"); got != "/a/b" {
		t.Fatalf("去尾斜杠: %q，want /a/b", got)
	}
	if got := canonicalize("/a/b///"); got != "/a/b" {
		t.Fatalf("多个尾斜杠: %q，want /a/b", got)
	}
	if got := canonicalize(""); got != "" {
		t.Fatalf("空串: %q，want 空", got)
	}
	if runtime.GOOS == "windows" {
		if got := canonicalize(`C:\Users\Foo\`); got != "c:/users/foo" {
			t.Fatalf("Windows 反斜杠+小写+去尾: %q，want c:/users/foo", got)
		}
		if got := canonicalize(`C:\`); got != "c:/" {
			t.Fatalf("盘根: %q，want c:/", got)
		}
	}
}

// TestCanonicalPathStableForSameDir：同一目录的多种写法（尾斜杠 / 相对）归一为同一规范路径。
func TestCanonicalPathStableForSameDir(t *testing.T) {
	base := t.TempDir()
	sub := filepath.Join(base, "proj")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	a, err := CanonicalPath(sub)
	if err != nil {
		t.Fatalf("CanonicalPath(%q): %v", sub, err)
	}
	b, err := CanonicalPath(sub + string(filepath.Separator)) // 带尾斜杠
	if err != nil {
		t.Fatalf("CanonicalPath(带尾斜杠): %v", err)
	}
	if a != b {
		t.Fatalf("尾斜杠未归一: %q vs %q", a, b)
	}
	if strings.HasSuffix(a, "/") {
		t.Fatalf("规范路径不应带尾斜杠: %q", a)
	}
}

// ---------------------------------------------------------------------------
// .git 目录 vs 文件（worktree）+ repo_id 聚合
// ---------------------------------------------------------------------------

// TestRepoIDAggregatesWorktrees：同 repo 的主仓库 + worktree 聚为同一 repo_id、不同 project_id；
// 且 .git 在主仓库是目录、在 worktree 是文件（两者都要认）。
func TestRepoIDAggregatesWorktrees(t *testing.T) {
	gitAvailable(t)
	root := t.TempDir()
	main := initRepo(t, filepath.Join(root, "main"))
	wt := filepath.Join(root, "wt")
	runGit(t, main, "worktree", "add", "-q", "--detach", wt)

	// 形态确认：main/.git 是目录，wt/.git 是文件。
	if fi, err := os.Stat(filepath.Join(main, ".git")); err != nil || !fi.IsDir() {
		t.Fatalf("main/.git 应为目录: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(wt, ".git")); err != nil || fi.IsDir() {
		t.Fatalf("wt/.git 应为文件（worktree）: %v", err)
	}

	mainRepo, err := RepoID(main)
	if err != nil {
		t.Fatalf("RepoID(main): %v", err)
	}
	wtRepo, err := RepoID(wt)
	if err != nil {
		t.Fatalf("RepoID(wt): %v", err)
	}
	if mainRepo != wtRepo {
		t.Fatalf("同 repo 多 worktree 应聚为同一 repo_id：main=%s wt=%s", mainRepo, wtRepo)
	}
	if !validShortID(mainRepo) {
		t.Fatalf("repo_id 格式不符（26 字符 Crockford）：%q", mainRepo)
	}

	mainProj, err := ProjectID(main)
	if err != nil {
		t.Fatalf("ProjectID(main): %v", err)
	}
	wtProj, err := ProjectID(wt)
	if err != nil {
		t.Fatalf("ProjectID(wt): %v", err)
	}
	if mainProj == wtProj {
		t.Fatalf("不同 worktree 应是不同 project_id（都 = %s）", mainProj)
	}
	if !validShortID(mainProj) {
		t.Fatalf("project_id 格式不符：%q", mainProj)
	}
}

// TestRepoIDAndProjectIDDeterministic：repo_id 确定；仓库内子目录与仓库根同 repo_id、
// 不同 project_id；且子目录的 project_id ≠ repo_id（cwd ≠ git root）。
func TestRepoIDAndProjectIDDeterministic(t *testing.T) {
	gitAvailable(t)
	repo := initRepo(t, filepath.Join(t.TempDir(), "r"))
	sub := filepath.Join(repo, "pkg", "inner")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	rRoot, err := RepoID(repo)
	if err != nil {
		t.Fatalf("RepoID(repo): %v", err)
	}
	rRoot2, _ := RepoID(repo)
	if rRoot != rRoot2 {
		t.Fatalf("repo_id 应确定：%s vs %s", rRoot, rRoot2)
	}

	rSub, err := RepoID(sub)
	if err != nil {
		t.Fatalf("RepoID(sub): %v", err)
	}
	pRoot, _ := ProjectID(repo)
	pSub, _ := ProjectID(sub)
	if rSub != rRoot {
		t.Fatalf("仓库内子目录应与仓库根同 repo_id：%s vs %s", rSub, rRoot)
	}
	if pSub == pRoot {
		t.Fatalf("子目录应与仓库根不同 project_id（都 = %s）", pSub)
	}
	if pSub == rSub {
		t.Fatalf("子目录的 project_id 不应等于 repo_id（cwd ≠ git root）")
	}
}

// TestRepoIDNotAGitRepo：不在任何 git 仓库内 → 错误（fail-close）。
func TestRepoIDNotAGitRepo(t *testing.T) {
	sub := filepath.Join(t.TempDir(), "plain", "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if hasGitAncestor(sub) {
		t.Skip("temp 目录恰有 .git 祖先（环境产物），跳过非 git 仓库用例")
	}
	if _, err := RepoID(sub); err == nil {
		t.Fatal("不在 git 仓库内应返回错误（fail-close：宁可无 repo_id 也不乱聚）")
	}
}

func hasGitAncestor(dir string) bool {
	d, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return true // 解不动就保守跳过
	}
	for {
		if _, err := os.Stat(filepath.Join(d, ".git")); err == nil {
			return true
		}
		p := filepath.Dir(d)
		if p == d {
			return false
		}
		d = p
	}
}

// ---------------------------------------------------------------------------
// 符号链接 / junction 归一（Windows 陷阱 4/6）
// ---------------------------------------------------------------------------

// TestCanonicalPathResolvesSymlink：符号链接解析到目标 → 同一规范路径 / 同一 ID。
func TestCanonicalPathResolvesSymlink(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if err := os.MkdirAll(real, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("symlink 不可用（可能缺权限）: %v", err)
	}
	cReal, err := CanonicalPath(real)
	if err != nil {
		t.Fatalf("CanonicalPath(real): %v", err)
	}
	cLink, err := CanonicalPath(link)
	if err != nil {
		t.Fatalf("CanonicalPath(link): %v", err)
	}
	if cReal != cLink {
		t.Fatalf("symlink 应解析到目标: %q vs %q", cReal, cLink)
	}
	pReal, _ := ProjectID(real)
	pLink, _ := ProjectID(link)
	if pReal != pLink {
		t.Fatalf("symlink 两侧 project_id 应相同: %s vs %s", pReal, pLink)
	}
}

// TestCanonicalPathResolvesJunction：Windows junction（目录联接，无需管理员权限）解析到目标。
func TestCanonicalPathResolvesJunction(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("junction 是 Windows 专属")
	}
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if err := os.MkdirAll(real, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	junction := filepath.Join(base, "junc")
	out, err := exec.Command("cmd", "/c", "mklink", "/J", junction, real).CombinedOutput()
	if err != nil {
		t.Skipf("mklink /J 失败: %v\n%s", err, out)
	}
	cReal, err := CanonicalPath(real)
	if err != nil {
		t.Fatalf("CanonicalPath(real): %v", err)
	}
	cJunc, err := CanonicalPath(junction)
	if err != nil {
		t.Fatalf("CanonicalPath(junction): %v", err)
	}
	if cReal != cJunc {
		t.Fatalf("junction 应解析到目标: %q vs %q", cReal, cJunc)
	}
	pReal, _ := ProjectID(real)
	pJunc, _ := ProjectID(junction)
	if pReal != pJunc {
		t.Fatalf("junction 两侧 project_id 应相同: %s vs %s", pReal, pJunc)
	}
}

// TestCanonicalPathCaseInsensitiveOnWindows：大小写差异归一（Windows FS 大小写不敏感，陷阱 4/17）。
func TestCanonicalPathCaseInsensitiveOnWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("大小写归一是 Windows 专属口径")
	}
	base := t.TempDir()
	real := filepath.Join(base, "MixedCase")
	if err := os.MkdirAll(real, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	other := filepath.Join(base, "mixedcase") // 仅末段大小写不同
	cReal, err := CanonicalPath(real)
	if err != nil {
		t.Fatalf("CanonicalPath(real): %v", err)
	}
	cOther, err := CanonicalPath(other)
	if err != nil {
		t.Fatalf("CanonicalPath(other): %v", err)
	}
	if cReal != cOther {
		t.Fatalf("大小写差异应归一: %q vs %q", cReal, cOther)
	}
	pReal, _ := ProjectID(real)
	pOther, _ := ProjectID(other)
	if pReal != pOther {
		t.Fatalf("大小写差异应同 project_id: %s vs %s", pReal, pOther)
	}
}

// TestCanonicalPathAbsolutizesRelativePath：相对路径与同一目录的绝对路径写法必须归一为
// 同一规范路径 / 同一 project_id——否则同一目录会因写法不同裂成两个 id（08 §1 的分组就废了）。
func TestCanonicalPathAbsolutizesRelativePath(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	cRel, err := CanonicalPath(".")
	if err != nil {
		t.Fatalf("CanonicalPath(\".\"): %v", err)
	}
	cAbs, err := CanonicalPath(cwd)
	if err != nil {
		t.Fatalf("CanonicalPath(%q): %v", cwd, err)
	}
	if cRel != cAbs {
		t.Fatalf("相对路径与绝对路径应归一为同一规范路径：%q vs %q", cRel, cAbs)
	}
	if !filepath.IsAbs(cRel) && runtime.GOOS != "windows" {
		// Windows 的绝对路径形如 "c:/..."（无盘符斜杠形态），非 Windows 以 "/" 开头。
		t.Fatalf("规范路径应是绝对路径：%q", cRel)
	}

	pRel, err := ProjectID(".")
	if err != nil {
		t.Fatalf("ProjectID(\".\"): %v", err)
	}
	pAbs, err := ProjectID(cwd)
	if err != nil {
		t.Fatalf("ProjectID(%q): %v", cwd, err)
	}
	if pRel != pAbs {
		t.Fatalf("同一目录的相对/绝对写法应同 project_id：%s vs %s", pRel, pAbs)
	}
}
