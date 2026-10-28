// Package identity 提供路径规范化与 project_id / repo_id 的计算（08 身份模型 + S13/S14）。
//
// core 不做 I/O（P0-4），故 ID 计算落在 runtime 层：本包做**文件系统遍历**（EvalSymlinks、
// 向上找 .git），纯变换部分（hash / base32）复用 core.ShortHash（core/hash.go）。
//
// 为什么要有规范化（11 §3 陷阱 4/6/17）：Windows 路径大小写不敏感、分隔符混用、符号链接/
// junction 会让"同一个目录"有多个写法。hash(cwd) 前必须：解析符号链接 → 转小写 → 统一
// 分隔符 → 去尾斜杠，否则同一项目/仓库会裂成多个 ID（分组失效、跨 worktree 聚不起来）。
//
// 口径（08 §1）：
//   - project_id = hash(canonical_cwd)——目录分组，路径变更则变；
//   - repo_id    = hash(canonical_git_root)——跨 worktree 聚合（同 repo 的多个 worktree
//     归一为同一 repo_id）。git root 向上找 .git：**目录或文件都算**（worktree 的 .git 是文件）。
//
// 哈希格式：SHA-256 截短到 128 bit → Crockford base32 26 字符（core.ShortHash，与 ULID 同
// 字符集/同长）。08 未规定哈希长度，取 128 bit 是为复用 ULID 编码并对路径分组场景碰撞可忽略。
package identity

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/Yorha9e/alasol/core"
)

// CanonicalPath 规范化一个路径（陷阱 4/6/17）：
//  1. **绝对化**（Abs）：相对路径的解析依赖调用点的 cwd——不绝对化，同一目录会因写法
//     不同（"." vs 绝对路径）裂成两个 project_id；
//  2. 解析符号链接 / junction（resolveLinks：见下，为什么不直接用 filepath.EvalSymlinks）；
//  3. Windows 路径转小写（Windows FS 大小写不敏感——不折叠会把 C:\Foo 与 c:\foo 当两个路径）；
//  4. 统一分隔符为 "/"；
//  5. 去尾斜杠（保留盘根 "/" 与 POSIX 根的那一个斜杠）。
//
// 步骤 1–2 碰文件系统（只在本 runtime 包做）；3–5 是纯字符串变换（canonicalize）。
func CanonicalPath(path string) (string, error) {
	resolved, err := resolveLinks(path)
	if err != nil {
		return "", fmt.Errorf("identity: 解析 %q: %w", path, err)
	}
	return canonicalize(resolved), nil
}

// maxLinkDepth 是链接解析的层数上限（环路保护）：一条正常路径的组件数 + 少量链接跳转
// 远小于此；符号链接/junction 成环时到此上限即报错，不会无限递归。
const maxLinkDepth = 128

// resolveLinks 解析 path 中的所有符号链接与 Windows junction，返回物理路径。
//
// **先绝对化**（Abs）：相对路径（"."、"sub/dir"）的解析结果取决于**调用点的 cwd**——
// 同一目录换个写法就裂成两个 project_id（08 §1 的目录分组就废了）。Abs 顺带做 Clean
// （去尾分隔符并归一化；否则 filepath.Dir("a/b/") 会返回 "a/b"（保留尾组件），令下面的
// 逐组件递归多拼一层——曾因尾斜杠踩坑）。
//
// 为什么不直接用 filepath.EvalSymlinks：它在 Windows 上只认 ModeSymlink，**不解析 junction**
// （目录联接），且当**中间组件**是 junction 时直接报错（"cannot find the path"）——而 junction
// 在 Windows 上很常见（陷阱 6：误删/路径归一都踩它）。os.Readlink 则对 symlink 与 junction
// 一视同仁（都读 reparse point 返回目标），故逐组件自实现。
//
// 与 EvalSymlinks 一致：path 不存在则报错（fail-closed，不静默归一一个不存在的路径）。
func resolveLinks(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("identity: 路径不能为空")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("identity: 取 %q 的绝对路径失败: %w", path, err)
	}
	path = filepath.Clean(abs)
	if _, err := os.Lstat(path); err != nil {
		return "", err
	}
	return resolveLinksDepth(path, 0)
}

// resolveLinksDepth 递归解析：先看 path 本身是不是链接（是 → 读目标、相对解析、对目标递归，
// 天然处理链接链与"链接指向含链接的多级路径"）；不是 → 递归解析父目录，再把末组件拼回去。
// 到文件系统根（filepath.Dir 不动）即收口。
func resolveLinksDepth(path string, depth int) (string, error) {
	if depth > maxLinkDepth {
		return "", fmt.Errorf("identity: 符号链接/junction 层数过深（疑似成环）: %q", path)
	}
	if target, err := os.Readlink(path); err == nil {
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(path), target)
		}
		return resolveLinksDepth(filepath.Clean(target), depth+1)
	}
	parent := filepath.Dir(path)
	if parent == path {
		return path, nil // 文件系统根
	}
	resolvedParent, err := resolveLinksDepth(parent, depth+1)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolvedParent, filepath.Base(path)), nil
}

// canonicalize 是 CanonicalPath 的纯变换部分：统一分隔符 →（Windows）转小写 → 去尾斜杠。
//
// 小写**只对 Windows**：Linux/macOS 常见大小写敏感 FS，折叠会把两个真实不同的路径误并。
// （macOS APFS 默认大小写不敏感，但 08/S13 口径按 Windows 定义，此处不扩展到非 Windows。）
func canonicalize(p string) string {
	p = filepath.ToSlash(p)
	if runtime.GOOS == "windows" {
		p = strings.ToLower(p)
	}
	if p == "" {
		return p
	}
	trimmed := strings.TrimRight(p, "/")
	switch {
	case trimmed == "":
		return "/" // 输入全是斜杠（"/" 或 "///"）
	case runtime.GOOS == "windows" && len(trimmed) == 2 && trimmed[1] == ':':
		return trimmed + "/" // 盘根 "c:" 补回斜杠成 "c:/"，与子路径形态一致
	default:
		return trimmed
	}
}

// ProjectID = hash(canonical_cwd)（08 §1：目录分组；路径变更则变）。
func ProjectID(cwd string) (string, error) {
	canonical, err := CanonicalPath(cwd)
	if err != nil {
		return "", err
	}
	return core.ShortHash(canonical), nil
}

// RepoID = hash(canonical_git_root)（08 §1：跨 worktree 聚合）。
//
// 不在任何 git 仓库内（向上到顶仍无 .git）→ 返回错误（fail-close：宁可无 repo_id 也不乱聚）。
func RepoID(path string) (string, error) {
	root, err := gitRoot(path)
	if err != nil {
		return "", err
	}
	if root == "" {
		return "", fmt.Errorf("identity: %q 不在任何 git 仓库内（向上找不到 .git）", path)
	}
	canonical, err := CanonicalPath(root)
	if err != nil {
		return "", err
	}
	return core.ShortHash(canonical), nil
}

// gitRoot 从 start 向上找 .git，返回**主仓库根**的绝对路径（已 EvalSymlinks，未小写/未去尾
// 斜杠——那由 CanonicalPath 收尾）。找不到 → ("", nil)（调用方据此判"非 git 仓库"）；
// 找到 .git 却解析不了（worktree 的 .git 文件读不懂）→ 返回错误（fail-close）。
func gitRoot(start string) (string, error) {
	dir, err := resolveLinks(start)
	if err != nil {
		return "", fmt.Errorf("identity: 解析 %q: %w", start, err)
	}
	dir = filepath.Clean(dir)
	for {
		gitPath := filepath.Join(dir, ".git")
		info, err := os.Stat(gitPath)
		if err == nil {
			if info.IsDir() {
				return dir, nil // 普通仓库：.git 是目录，所在目录即 git root
			}
			// .git 是文件 → git worktree：读 gitdir，回溯到主仓库根。
			root, ok := worktreeMainRoot(gitPath)
			if !ok {
				return "", fmt.Errorf("identity: 无法解析 worktree 的 .git 文件 %q（fail-close：不当成 git root）", gitPath)
			}
			return root, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", nil // 到文件系统顶，不在任何 git 仓库内
		}
		dir = parent
	}
}

// worktreeMainRoot 解析 worktree 的 .git 文件，回溯到**主仓库根**。
//
// .git 文件内容形如 "gitdir: /主仓库/.git/worktrees/<name>"。gitdir 指向该 worktree 的独立
// git 目录，位于主仓库 .git 的 worktrees/ 子目录下。向上从 gitdir 找到第一个 .git **目录**
// （= 主仓库的公共 git dir），其父目录即主仓库根——于是主仓库与它的所有 worktree 归一为同一
// canonical git root → 同一 repo_id（08 §1：跨 worktree 聚合）。
func worktreeMainRoot(gitFile string) (string, bool) {
	data, err := os.ReadFile(gitFile)
	if err != nil {
		return "", false
	}
	line := strings.TrimSpace(string(data))
	const prefix = "gitdir:"
	if !strings.HasPrefix(line, prefix) {
		return "", false
	}
	gitdir := strings.TrimSpace(line[len(prefix):])
	if gitdir == "" {
		return "", false
	}
	if !filepath.IsAbs(gitdir) {
		gitdir = filepath.Join(filepath.Dir(gitFile), gitdir)
	}
	gitdir, err = resolveLinks(gitdir)
	if err != nil {
		return "", false
	}
	// 向上找 .git 目录（公共 git dir），其父即主仓库根。
	d := filepath.Clean(gitdir)
	for {
		if filepath.Base(d) == ".git" {
			if info, statErr := os.Stat(d); statErr == nil && info.IsDir() {
				return filepath.Dir(d), true
			}
		}
		parent := filepath.Dir(d)
		if parent == d {
			return "", false
		}
		d = parent
	}
}
