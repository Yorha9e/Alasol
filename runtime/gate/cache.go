package gateexec

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/gate"
	"github.com/Yorha9e/alasol/runtime/identity"
)

// 本文件是 V4 依赖缓存的**执行侧**：从快照目录读清单文件 → 调 core/gate 的纯函数折
// 内容寻址键与缓存目录 → 供 Run 注入约定 env（CI_CACHE_KEY / CI_CACHE_DIR）。
//
// 边界（与 core/gate/cache.go 一致）：框架**只读清单、只派生路径与键**，不创建缓存目录、
// 不加锁、不清缓存、不装依赖——依赖安装是命令（不透明 shell 字符串，W9）自己的事，命令自暖。
//
// # 键 = (仓库作用域, 清单内容)
//
// 清单内容相同但**仓库不同**的两个仓库，不能共用同一个缓存目录（那是串仓：A 仓的依赖
// 漏进 B 仓的缓存目录）。所以键的第一维是**仓库作用域**：主仓库根的规范路径哈希
// （identity.RepoID = core.ShortHash(canonical git root)，与 08 身份模型的 repo_id 同款——
// 同一仓库的多个快照/工作树算出**同一个**作用域，缓存才命中得了；不同仓库必不同）。
//
// **跨机器共享**：默认作用域是**路径哈希**，跨机器路径不同 → 哈希不同 → 目录不共享。
// 要跨机共享同一个缓存目录，显式配 `cache.scope`（团队约定同一个字符串）——这是**显式的
// 选择**，不是默认行为（默认宁可不共享，也不把两个不相干的仓库塞进一个目录）。

const (
	// cacheRootDirName 是未配置 cache.dir 时的共享缓存根目录名（落在本机临时目录下）。
	cacheRootDirName = "alasol-gate-cache"
	// maxManifestBytes 是单个清单文件的读取上限（32MB）。
	//
	// 清单是依赖锁文件，正常是 KB 级；上限防"把快照里的超大文件整个读进内存折键"
	// （快照内容不可信：门禁跑的是被检代码的仓库）。超限 → 该文件**不参与键**
	// （缓存是优化，不该为它 fail 掉整个门禁）。
	maxManifestBytes = 32 << 20
)

// defaultCacheRoot 返回未配置 cache.dir 时的共享缓存根。
//
// 取舍：默认落**临时目录**（跨会话可能被系统清 → 缓存不持久，但正确性不受影响：
// 清空只是让命令重新暖一次）。生产应配置**持久**根（gate.json 的 cache.dir）。
func defaultCacheRoot() string {
	return filepath.Join(os.TempDir(), cacheRootDirName)
}

// userCacheRoot 返回**逐用户**的稳定缓存根（os.UserCacheDir）。
//
// 用途：相对 cache.dir 的兜底基准——**必须是稳定落点**（跨会话、跨快照都不变）。
func userCacheRoot() string {
	if dir, err := os.UserCacheDir(); err == nil && dir != "" {
		return filepath.Join(dir, cacheRootDirName)
	}
	return defaultCacheRoot()
}

// deriveCache 读快照 dir 里的清单文件，派生内容寻址键与缓存目录（V4）。
//
//   - 只读 cfg.ManifestSet() 列出的文件：存在即读入键，**不存在 / 读不了 / 越界 / 符号链接
//     / 超大 → 跳过**（缓存是优化，绝不因"某个清单读不了"而 fail 掉整个门禁——fail-open 于缓存、
//     fail-close 于判定，两者不相干）；
//   - 键 = core/gate.CacheKey(仓库作用域, 读到的文件内容)（内容寻址：同内容同键、
//     改内容变键；仓库作用域并入键 → 跨仓库不共用目录）；
//   - 目录 = core/gate.CacheDir(root, key)，root = cfg.Dir（相对路径按**主仓库根**解析成
//     绝对路径）或本机默认根。
//
// 永不返回 error：缓存派生失败不该阻塞门禁执行（最坏情况是命令拿不到有用的缓存，
// 自己全量装一次）。
func deriveCache(dir string, cfg gate.CacheConfig) (key, cacheDir string) {
	files := make(map[string][]byte)
	for _, name := range cfg.ManifestSet() {
		path, ok := safeManifestPath(dir, name)
		if !ok {
			continue // 越界清单名（绝对路径 / ".." 穿越 / 盘符）→ 不参与键
		}
		b, ok := readManifest(dir, path)
		if !ok {
			continue // 不存在 / 读不了 / 符号链接 / 超大 → 不参与键
		}
		files[name] = b
	}
	key = gate.CacheKey(repoScope(dir, cfg), files)
	cacheDir = gate.CacheDir(cacheRoot(cfg.Dir, dir), key)
	return key, cacheDir
}

// safeManifestPath 把清单名解析成快照目录内的路径；越界一律拒绝（false）。
//
// 拒绝：绝对路径、盘符/UNC 前缀（filepath.VolumeName 非空）、含 ".." 的穿越路径、
// 以及拼接后仍落在目录外的路径（filepath.Rel 复核）。清单名来自仓库里的配置
// （.alasol/gate.json）——**配置也是输入**：清单名越界 = 读快照外的文件，键与目录都不可信。
func safeManifestPath(dir, name string) (string, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", false
	}
	if filepath.IsAbs(name) || filepath.VolumeName(name) != "" {
		return "", false // 绝对路径 / 盘符 / UNC
	}
	clean := filepath.Clean(filepath.FromSlash(name))
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", false // 向上穿越
	}
	p := filepath.Join(dir, clean)
	rel, err := filepath.Rel(dir, p)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false // 拼出来仍在目录外（复核，宁可多拒）
	}
	return p, true
}

// readManifest 读一个清单文件；不存在 / 读不了 / 不是普通文件 / 是符号链接 / 超过大小上限
// → false（不参与键；缓存是优化，不 fail 门禁）。
//
// **符号链接防御**（N-4）：os.Open 会跟随符号链接 → 快照里一个指向快照外的软链能把
// **快照外的文件内容**折进键（键不可信、目录可能建在不该建的地方）。所以：
//
//  1. Lstat 判软链本身 → 直接跳过（不解析、不读）；
//  2. 再把路径**解析成物理路径**（identity.CanonicalPath：解析符号链接 / junction，
//     Windows 上 junction 也认）复核它**仍在快照目录内**——挡住"中间目录是链接"的越界
//     （清单名 "sub/go.mod"，而 sub 指向快照外）。
//
// 代价（明写）：被跳过的清单**不参与键** → 该清单的改动**不会**改键（与"读不了"同款
// 处理：缓存是优化，键少一项只影响命中率与正确性边界——依赖清单本身仍由命令自己校验）。
func readManifest(snapshotDir, path string) ([]byte, bool) {
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, false // 不存在
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return nil, false // 软链 → 不解析、不读（可能指向快照外）
	}
	if !fi.Mode().IsRegular() {
		return nil, false // 目录 / 设备文件等 → 不参与键
	}
	// 解析物理路径后复核仍在快照内（中间目录是链接的情形）。
	canonDir, err := identity.CanonicalPath(snapshotDir)
	if err != nil {
		return nil, false
	}
	canonPath, err := identity.CanonicalPath(path)
	if err != nil {
		return nil, false
	}
	if canonPath != canonDir && !strings.HasPrefix(canonPath, strings.TrimSuffix(canonDir, "/")+"/") {
		return nil, false // 解析后落在快照外 → 不参与键
	}
	if fi.Size() > maxManifestBytes {
		return nil, false // 超上限 → 不参与键
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, false
	}
	defer f.Close()
	// 再用 LimitReader 兜一道（读的过程中文件可能变大——stat 的快照不保证）。
	b, err := io.ReadAll(io.LimitReader(f, maxManifestBytes+1))
	if err != nil || len(b) > maxManifestBytes {
		return nil, false
	}
	return b, true
}

// repoScope 派生**仓库作用域**（缓存键的第一维）。
//
// 口径：
//
//   - **显式配置优先**（cfg.Scope）：团队约定的字符串 → 跨机器共享同一个缓存目录；
//   - 否则 = 主仓库根的规范路径哈希（identity.RepoID = core.ShortHash(canonical git root)）——
//     与 08 身份模型的 repo_id 同款：同一仓库的多个快照/工作树算出**同一个**作用域
//     （缓存才命中得了），不同仓库必不同（不串仓）；**跨机器不共享**（路径哈希）。
//
// 兜底：不在 git 仓库里（或路径解析失败）→ 用快照目录绝对路径的哈希。宁可命中率低，
// 也不跨仓库共用目录。
func repoScope(dir string, cfg gate.CacheConfig) string {
	if s := strings.TrimSpace(cfg.Scope); s != "" {
		return s
	}
	if id, err := identity.RepoID(dir); err == nil && id != "" {
		return id
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		abs = dir
	}
	return core.ShortHash(abs)
}

// cacheRoot 解析缓存根：未配置 → 本机默认根；配置为**相对路径** → 按**主仓库根**
// （稳定落点）解析成**绝对路径**。
//
// 为什么相对路径必须落到**稳定**的地方（而不是快照目录）：
//
//   - 快照目录是**一次性**的（worktree，跑完就删）→ 拿它当基准 = 缓存目录每次换地方、
//     随快照一起消失 → 缓存**永不命中**（每跑一次都重新暖一遍）；
//   - 也不能随调用方的 cwd 漂移（同一份配置在不同机器/不同目录下落到不同地方）。
//
// 主仓库根两者都满足：同一仓库的每次门禁都落到同一个地方（命中），跨机器各自一份
// （不共享——要共享就配 cache.scope）。不在 git 仓库里 → 兜底到逐用户的缓存目录
// （仍是稳定落点）。
func cacheRoot(cfgDir, snapshotDir string) string {
	cfgDir = strings.TrimSpace(cfgDir)
	if cfgDir == "" {
		return defaultCacheRoot()
	}
	if filepath.IsAbs(cfgDir) {
		return filepath.Clean(cfgDir)
	}
	base, err := identity.GitRoot(snapshotDir)
	if err != nil || base == "" {
		base = userCacheRoot()
	}
	return filepath.Join(base, cfgDir)
}
