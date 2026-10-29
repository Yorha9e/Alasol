package gateexec

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core/gate"
	"github.com/Yorha9e/alasol/runtime/identity"
)

// echoCacheEnvCmd 打印两个缓存 env（各占一行），用于验证 V4 注入。
func echoCacheEnvCmd() string {
	if runtime.GOOS == "windows" {
		return "echo %CI_CACHE_KEY%&echo %CI_CACHE_DIR%"
	}
	return `printf '%s\n%s\n' "$CI_CACHE_KEY" "$CI_CACHE_DIR"`
}

// TestDeriveCacheContentAddressed：deriveCache 从快照读清单 → 内容寻址键 + 目录。
//
// 变异自证：键必须由**内容**派生——改了 go.mod 内容，键与目录都要变（否则缓存会串版本）。
func TestDeriveCacheContentAddressed(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "go.mod"), "module example.com/m\n\ngo 1.22\n")
	writeFile(t, filepath.Join(dir, "go.sum"), "example.com/dep v1.0.0 h1:abc=\n")
	// 一个不参与默认集的杂文件（不该进键）。
	writeFile(t, filepath.Join(dir, "README.md"), "hello\n")

	key, cacheDir := deriveCache(dir, gate.CacheConfig{})
	wantKey := gate.CacheKey(repoScope(dir, gate.CacheConfig{}), map[string][]byte{
		"go.mod": []byte("module example.com/m\n\ngo 1.22\n"),
		"go.sum": []byte("example.com/dep v1.0.0 h1:abc=\n"),
	})
	if key != wantKey {
		t.Errorf("key = %q，期望按 go.mod+go.sum 内容折成 %q", key, wantKey)
	}
	wantDir := gate.CacheDir(defaultCacheRoot(), wantKey)
	if cacheDir != wantDir {
		t.Errorf("cacheDir = %q，期望 %q", cacheDir, wantDir)
	}

	// 改 go.mod 一个字节 → 键与目录都变（内容寻址：不同版本不串缓存）。
	writeFile(t, filepath.Join(dir, "go.mod"), "module example.com/m\n\ngo 1.23\n")
	key2, cacheDir2 := deriveCache(dir, gate.CacheConfig{})
	if key2 == key {
		t.Error("改 go.mod 内容应变键（内容寻址）")
	}
	if cacheDir2 == cacheDir {
		t.Error("变键应变目录（版本隔离：不同版本互不干扰）")
	}
}

// TestDeriveCacheRepoScope（p2-3）：键并入**仓库作用域**——不同仓库的同内容清单不得共用
// 缓存目录（那是串仓）。
//
// 变异自证：若键只含清单内容（不含仓库作用域），本测试会红。
func TestDeriveCacheRepoScope(t *testing.T) {
	// 两个不同的目录（各自一份同样内容的清单）。
	dirA := t.TempDir()
	dirB := t.TempDir()
	writeFile(t, filepath.Join(dirA, "go.mod"), "module example.com/m\n")
	writeFile(t, filepath.Join(dirB, "go.mod"), "module example.com/m\n")

	keyA, cacheDirA := deriveCache(dirA, gate.CacheConfig{})
	keyB, cacheDirB := deriveCache(dirB, gate.CacheConfig{})
	if keyA == keyB {
		t.Error("不同仓库的同内容清单应得不同键（否则跨仓库共用缓存目录 = 串仓）")
	}
	if cacheDirA == cacheDirB {
		t.Error("不同仓库应派生不同缓存目录（仓库隔离）")
	}
	// 同一目录两次派生 → 同键同目录（命中复用）。
	keyA2, cacheDirA2 := deriveCache(dirA, gate.CacheConfig{})
	if keyA2 != keyA || cacheDirA2 != cacheDirA {
		t.Error("同一仓库两次派生应同键同目录（命中复用）")
	}
}

// TestDeriveCacheScopeOverride（p3 / N-5②）：显式配置的 **scope** 覆盖默认的仓库根哈希
// ——跨机器共享同一个缓存目录就靠它（默认路径哈希跨机不共享）。
//
// 变异自证：若 scope 没进键（配置被忽略），本测试会红。
func TestDeriveCacheScopeOverride(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	writeFile(t, filepath.Join(dirA, "go.mod"), "module example.com/m\n")
	writeFile(t, filepath.Join(dirB, "go.mod"), "module example.com/m\n")

	cfg := gate.CacheConfig{Scope: "team-shared-cache"}
	keyA, dirCacheA := deriveCache(dirA, cfg)
	keyB, dirCacheB := deriveCache(dirB, cfg)
	if keyA != keyB {
		t.Errorf("同一 scope 的两个目录应得同键（跨机共享）：%q vs %q", keyA, keyB)
	}
	if dirCacheA != dirCacheB {
		t.Errorf("同一 scope 的两个目录应得同缓存目录：%q vs %q", dirCacheA, dirCacheB)
	}
	// 不配 scope → 两个目录不同键（默认按仓库根哈希，跨仓库隔离）。
	if kA, _ := deriveCache(dirA, gate.CacheConfig{}); kA == keyA {
		t.Error("默认作用域（仓库根哈希）应与显式 scope 不同键")
	}
}

// TestDeriveCacheRepoScopeSameRepoWorktrees（p2-3）：**同一仓库**的多个快照（worktree）
// 必须算出同一个仓库作用域 → 同键同目录（缓存才命中得了）。
func TestDeriveCacheRepoScopeSameRepoWorktrees(t *testing.T) {
	repo, commit := initRepo(t)
	commit = commitFile(t, repo, "go.mod", "module example.com/app\n", "add go.mod")

	snap1, cleanup1, err := Snapshot(context.Background(), repo, commit)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer cleanup1()
	snap2, cleanup2, err := Snapshot(context.Background(), repo, commit)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer cleanup2()

	k1, d1 := deriveCache(snap1, gate.CacheConfig{})
	k2, d2 := deriveCache(snap2, gate.CacheConfig{})
	if k1 != k2 || d1 != d2 {
		t.Errorf("同一仓库的两个快照应同作用域同键同目录：(%q,%q) vs (%q,%q)", k1, d1, k2, d2)
	}
}

// TestDeriveCacheRelativeDirIsStable（p3 / N-2）：相对 `cache.dir` 必须落在**稳定**的落点
// （主仓库根），**不是**一次性快照目录——否则每次跑都换目录、随快照删除，缓存永不命中。
//
// 变异自证：若相对路径仍按快照目录解析，本测试会红（两次快照的目录不同）。
func TestDeriveCacheRelativeDirIsStable(t *testing.T) {
	repo, commit := initRepo(t)
	commit = commitFile(t, repo, "go.mod", "module example.com/app\n", "add go.mod")

	cfg := gate.CacheConfig{Dir: filepath.Join("data", "cache")}
	run := func() (string, string) {
		snap, cleanup, err := Snapshot(context.Background(), repo, commit)
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		defer cleanup()
		return deriveCache(snap, cfg)
	}
	_, dir1 := run()
	_, dir2 := run()
	if dir1 != dir2 {
		t.Errorf("同一仓库的两次快照应解析到同一缓存目录（稳定落点）：%q vs %q", dir1, dir2)
	}
	// 落点应在**主仓库根**下（不是快照目录：快照是一次性的）。
	repoRoot, err := identity.CanonicalPath(repo)
	if err != nil {
		t.Fatalf("CanonicalPath: %v", err)
	}
	wantPrefix := filepath.Join(repoRoot, "data", "cache") + string(filepath.Separator)
	if !strings.HasPrefix(dir1, wantPrefix) {
		t.Errorf("缓存目录 = %q，应落在主仓库根下的 %q", dir1, wantPrefix)
	}
}

// TestDeriveCacheRejectsPathTraversal（p3-1）：清单名越界（绝对路径 / ".." 穿越 / 盘符）
// 一律**不读**——快照外的文件不许进键（键与目录都不可信）。
//
// 变异自证：若不做越界校验，本测试会红（越界文件的内容进了键）。
func TestDeriveCacheRejectsPathTraversal(t *testing.T) {
	dir := t.TempDir()
	// 快照外的一个"秘密"文件。
	outside := filepath.Join(filepath.Dir(dir), "secret.txt")
	writeFile(t, outside, "SECRET-CONTENT")
	defer os.Remove(outside)

	emptyKey, _ := deriveCache(dir, gate.CacheConfig{ManifestFiles: []string{"nope.txt"}})

	for _, name := range []string{
		"../secret.txt",
		"../../secret.txt",
		"a/../../secret.txt",
		outside, // 绝对路径
		"..\\secret.txt",
	} {
		cfg := gate.CacheConfig{ManifestFiles: []string{name}}
		key, _ := deriveCache(dir, cfg)
		if key != emptyKey {
			t.Errorf("越界清单名 %q 不该进键（key=%q）", name, key)
		}
	}
	// 盘符路径（Windows）：拒绝。
	if runtime.GOOS == "windows" {
		key, _ := deriveCache(dir, gate.CacheConfig{ManifestFiles: []string{`C:\Windows\win.ini`}})
		if key != emptyKey {
			t.Errorf("盘符清单名不该进键（key=%q）", key)
		}
	}
}

// TestDeriveCacheSkipsSymlinkedManifest（p3 / N-4）：**符号链接**指向快照外的文件不许进键
// （os.Open 会跟随软链 → 快照外的内容会折进键）。
//
// 变异自证：若不判软链（直接 open 跟随），本测试会红。
func TestDeriveCacheSkipsSymlinkedManifest(t *testing.T) {
	dir := t.TempDir()
	// 快照外的"秘密"文件。
	outside := filepath.Join(filepath.Dir(dir), "outside-secret.txt")
	writeFile(t, outside, "SECRET-CONTENT")
	defer os.Remove(outside)

	// 快照内一个指向它的软链。
	link := filepath.Join(dir, "go.mod")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("建符号链接失败（Windows 可能没权限），跳过：%v", err)
	}
	emptyKey, _ := deriveCache(dir, gate.CacheConfig{ManifestFiles: []string{"nope.txt"}})
	key, _ := deriveCache(dir, gate.CacheConfig{})
	if key != emptyKey {
		t.Errorf("指向快照外的软链不该进键（key=%q）", key)
	}
}

// TestReadManifestRejectsOutsidePath（p3 / N-4）：快照**外**的路径一律不得读进键
// （"解析后的物理路径仍在快照内"这道复核）。
//
// 变异自证：若去掉物理路径复核，本测试会红。
func TestReadManifestRejectsOutsidePath(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(filepath.Dir(dir), "outside-secret.txt")
	writeFile(t, outside, "SECRET-CONTENT")
	defer os.Remove(outside)

	if _, ok := readManifest(dir, outside); ok {
		t.Error("快照外的文件不该被读进键（物理路径复核失效）")
	}
	// 快照内的普通文件照常读得到（复核不是"一律拒绝"）。
	inside := filepath.Join(dir, "go.mod")
	writeFile(t, inside, "module x\n")
	if _, ok := readManifest(dir, inside); !ok {
		t.Error("快照内的普通清单应能读到")
	}
}

// TestDeriveCacheSkipsOversizedManifest（p3-1）：单个清单文件有**大小上限**——超限跳过
// （不参与键），不 fail 门禁。
//
// 变异自证：若没有上限，本测试会红（超大文件进了键）。
func TestDeriveCacheSkipsOversizedManifest(t *testing.T) {
	dir := t.TempDir()
	big := make([]byte, maxManifestBytes+1)
	for i := range big {
		big[i] = 'x'
	}
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), big, 0o644); err != nil {
		t.Fatalf("写超大清单: %v", err)
	}
	// 超限的清单**不参与键** → 键 = 空清单集的键（但仍带仓库作用域）。
	key, _ := deriveCache(dir, gate.CacheConfig{})
	want := gate.CacheKey(repoScope(dir, gate.CacheConfig{}), nil)
	if key != want {
		t.Errorf("超大清单应被上限跳过，key = %q，期望 %q", key, want)
	}
}

// TestDeriveCacheCustomConfig：可配清单集 + 缓存根（V4 可配覆盖）。
func TestDeriveCacheCustomConfig(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "go.mod"), "module x\n")      // 默认集里有，但被覆盖排除
	writeFile(t, filepath.Join(dir, "mylock.lock"), "pinned-abc") // 自定义集里

	absRoot := filepath.Join(t.TempDir(), "cache")
	cfg := gate.CacheConfig{
		ManifestFiles: []string{"mylock.lock"}, // 只认这个
		Dir:           absRoot,
	}
	key, cacheDir := deriveCache(dir, cfg)
	// 键只含 mylock.lock（go.mod 被排除 → 改了 go.mod 不该变键）。
	wantKey := gate.CacheKey(repoScope(dir, cfg), map[string][]byte{"mylock.lock": []byte("pinned-abc")})
	if key != wantKey {
		t.Errorf("key = %q，期望只按 mylock.lock 折成 %q", key, wantKey)
	}
	if want := filepath.Join(absRoot, wantKey); cacheDir != want {
		t.Errorf("cacheDir = %q，期望用自定义根 %q", cacheDir, want)
	}

	// 验证覆盖确实排除了 go.mod：改 go.mod 不影响键。
	writeFile(t, filepath.Join(dir, "go.mod"), "module CHANGED\n")
	if key2, _ := deriveCache(dir, cfg); key2 != key {
		t.Error("自定义清单集排除了 go.mod，改它不该变键")
	}
}

// TestDeriveCacheNoManifests：无清单 → 空集稳定键（不报错、不 fail）。
func TestDeriveCacheNoManifests(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "main.go"), "package main\n")
	key, cacheDir := deriveCache(dir, gate.CacheConfig{})
	if key != gate.CacheKey(repoScope(dir, gate.CacheConfig{}), nil) {
		t.Errorf("无清单应得空集键 %q，得到 %q", gate.CacheKey(repoScope(dir, gate.CacheConfig{}), nil), key)
	}
	if !strings.HasPrefix(cacheDir, defaultCacheRoot()) {
		t.Errorf("cacheDir = %q，应在默认根 %q 下", cacheDir, defaultCacheRoot())
	}
}

// TestRunInjectsCacheEnv：e2e——门禁跑快照命令，注入的 CI_CACHE_KEY / CI_CACHE_DIR
// 出现在执行环境，且与 deriveCache 派生的一致（对齐 V3 的测试形态）。
func TestRunInjectsCacheEnv(t *testing.T) {
	repo, commit := initRepo(t)
	// 提交一个 go.mod，让快照里有清单可寻址。
	commit = commitFile(t, repo, "go.mod", "module example.com/app\n\ngo 1.22\n", "add go.mod")

	dir, cleanup, err := Snapshot(context.Background(), repo, commit)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer cleanup()

	r, err := Run(context.Background(), dir, echoCacheEnvCmd())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.ExitCode != 0 {
		t.Fatalf("exit=%d，LogTail=%q", r.ExitCode, r.LogTail)
	}
	lines := strings.Split(strings.TrimRight(r.LogTail, "\r\n"), "\n")
	if len(lines) < 2 {
		t.Fatalf("期望 2 行输出，得到 %q", r.LogTail)
	}
	gotKey := strings.TrimSpace(lines[0])
	gotDir := strings.TrimSpace(lines[1])

	wantKey, wantDir := deriveCache(dir, gate.CacheConfig{})
	if gotKey != wantKey {
		t.Errorf("CI_CACHE_KEY = %q，期望 %q（go.mod 内容寻址）", gotKey, wantKey)
	}
	if gotDir != wantDir {
		t.Errorf("CI_CACHE_DIR = %q，期望 %q", gotDir, wantDir)
	}
	// 目录 = <默认根>/<键>。
	if !strings.HasSuffix(gotDir, gotKey) {
		t.Errorf("CI_CACHE_DIR = %q，应以键 %q 结尾", gotDir, gotKey)
	}
}

// TestRunCacheHitReusesDir：命中不重复构建——同一 commit 的两次快照派生同键同目录。
func TestRunCacheHitReusesDir(t *testing.T) {
	repo, commit := initRepo(t)
	commit = commitFile(t, repo, "go.sum", "example.com/dep v1.0.0 h1:xyz=\n", "add go.sum")

	run := func() (key, dir string) {
		snap, cleanup, err := Snapshot(context.Background(), repo, commit)
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		defer cleanup()
		r, err := Run(context.Background(), snap, echoCacheEnvCmd())
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		lines := strings.Split(strings.TrimRight(r.LogTail, "\r\n"), "\n")
		return strings.TrimSpace(lines[0]), strings.TrimSpace(lines[1])
	}
	k1, d1 := run()
	k2, d2 := run()
	if k1 != k2 || d1 != d2 {
		t.Errorf("同 commit 两次应同键同目录（命中复用）：(%q,%q) vs (%q,%q)", k1, d1, k2, d2)
	}
}

// TestRunWithCustomCacheEnv：RunWith 用自定义缓存配置，env 反映自定义根与清单集。
func TestRunWithCustomCacheEnv(t *testing.T) {
	repo, commit := initRepo(t)
	commit = commitFile(t, repo, "go.mod", "module example.com/app\n\ngo 1.22\n", "add go.mod")

	dir, cleanup, err := Snapshot(context.Background(), repo, commit)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer cleanup()

	// 相对缓存根 → 按**主仓库根**解析成绝对路径（稳定落点，不是快照目录）。
	cfg := gate.CacheConfig{Dir: filepath.Join("persistent", "cache")}
	r, err := RunWith(context.Background(), dir, echoCacheEnvCmd(), cfg)
	if err != nil {
		t.Fatalf("RunWith: %v", err)
	}
	lines := strings.Split(strings.TrimRight(r.LogTail, "\r\n"), "\n")
	gotDir := strings.TrimSpace(lines[1])
	if !filepath.IsAbs(gotDir) {
		t.Errorf("CI_CACHE_DIR = %q，应是绝对路径（相对 Dir 解析成绝对）", gotDir)
	}
	repoRoot, err := identity.CanonicalPath(repo)
	if err != nil {
		t.Fatalf("CanonicalPath: %v", err)
	}
	wantPrefix := filepath.Join(repoRoot, "persistent", "cache") + string(filepath.Separator)
	if !strings.HasPrefix(gotDir, wantPrefix) {
		t.Errorf("CI_CACHE_DIR = %q，应落在主仓库根下的 %q（稳定落点）", gotDir, wantPrefix)
	}
	// **不能**落在快照目录下（快照是一次性的：落这里 = 缓存永不命中）。
	if strings.HasPrefix(gotDir, dir+string(filepath.Separator)) {
		t.Errorf("CI_CACHE_DIR = %q，不该落在一次性快照目录 %q 下", gotDir, dir)
	}
}
