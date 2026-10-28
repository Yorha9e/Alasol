// Package worktree 提供**多 worktree 隔离**（v1-S5 D）：并行任务各占一个独立 git worktree
// 工作区——创建 / 登记 / junction-safe 清理。
//
// 与 runtime/gate 的一次性快照 worktree（V1）是同族机制但**生命周期不同**：gate 快照是
// "跑一次 CI 就删"的临时检出；本包的 worktree 是"一个长时任务一个工作区"的常驻隔离，
// 由 Manager 登记、显式 Remove。底层同样踩 Windows 三坑（陷阱 4-7）：
//   - git 走**绝对路径 execFile git.exe**，不经 shell（陷阱 7：防 cwd 劫持）；
//   - spawn 加 **windowsHide**（陷阱 5：压制控制台弹窗）；
//   - 清理走 **removeTreeSafe**（陷阱 6：Lstat 不跟穿 junction，先解再删）。
//
// 登记表是**进程内内存**（name → 元数据）：worktree 在磁盘上的 git 元数据（.git/worktrees）
// 跨进程存活，但"谁创建了哪个"的登记随 hub 进程生命周期（与发现表活状态同性质，G17b）。
package worktree

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// maxNameLen 限制 worktree 名长度（用作目录名 + git 参数，宁短勿长）。
const maxNameLen = 64

// Worktree 是一个已登记的 worktree。
type Worktree struct {
	// Name 是登记名（调用方给的短名，用作 root 下的子目录名）。
	Name string
	// Path 是 worktree 的绝对路径（root/<name>）。
	Path string
	// Repo 是它从哪个仓库检出（主仓库或另一个 worktree 的路径）。
	Repo string
	// CreatedAt 创建时刻。
	CreatedAt time.Time
}

// Manager 登记并管理一组 worktree（全部位于 root 下）。
//
// 并发：mu 保护 entries；Create/Remove 串行化关键区（git 操作 + 登记）。
type Manager struct {
	root string

	mu      sync.Mutex
	entries map[string]Worktree
}

// NewManager 建 Manager 并确保 root 目录存在（所有 worktree 都建在 root/<name> 下）。
func NewManager(root string) (*Manager, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, errors.New("worktree: root 不能为空")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("worktree: 解析 root 绝对路径: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("worktree: 建 root 目录 %s: %w", abs, err)
	}
	return &Manager{root: abs, entries: map[string]Worktree{}}, nil
}

// Root 返回受管根目录。
func (m *Manager) Root() string { return m.root }

// validName 校验 worktree 名：非空、长度受限、**不含路径分隔符 / 父目录跳转 / 盘符**
// （name 会拼进路径并作 git 参数，宁严勿松——防 ../../ 逃逸 root）。
func validName(name string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("worktree: name 不能为空")
	}
	if len(name) > maxNameLen {
		return fmt.Errorf("worktree: name 超过 %d 字符", maxNameLen)
	}
	if strings.ContainsAny(name, `/\`) || strings.Contains(name, "..") ||
		strings.ContainsAny(name, ":*?\"<>|") || strings.ContainsAny(name, "\x00\r\n\t ") {
		return fmt.Errorf("worktree: name %q 含非法字符（路径分隔符/空白/通配等）", name)
	}
	return nil
}

// Create 在 repoPath 仓库上检出一个 detached HEAD worktree 到 root/<name>，并登记。
//
// detached HEAD（与 gate.Snapshot 同口径）：任务在独立工作区里改动，未提交改动不污染主仓库，
// Remove 时整体删除。重复 name → 拒绝（不覆盖已有登记）。
func (m *Manager) Create(ctx context.Context, repoPath, name string) (Worktree, error) {
	if err := validName(name); err != nil {
		return Worktree{}, err
	}
	repo := strings.TrimSpace(repoPath)
	if repo == "" {
		return Worktree{}, errors.New("worktree: repoPath 不能为空")
	}
	if _, err := gitExecutable(); err != nil {
		return Worktree{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.entries[name]; ok {
		return Worktree{}, fmt.Errorf("worktree: %q 已存在（先 Remove 再重建）", name)
	}
	wtPath := filepath.Join(m.root, name)
	if _, err := os.Lstat(wtPath); err == nil {
		return Worktree{}, fmt.Errorf("worktree: 目标路径已存在: %s", wtPath)
	}

	// git worktree add --detach <path>（在 repoPath 上执行，绝对路径 git、不经 shell）。
	_, code, err := gitRun(ctx, repo, "worktree", "add", "--detach", wtPath)
	if err != nil || code != 0 {
		return Worktree{}, fmt.Errorf("worktree: git worktree add 失败（exit=%d）：%v", code, err)
	}
	wt := Worktree{Name: name, Path: wtPath, Repo: repo, CreatedAt: time.Now()}
	m.entries[name] = wt
	return wt, nil
}

// Remove 注销并删除一个 worktree。**顺序要紧**：先 removeTreeSafe 删工作区（junction-safe，
// 不跟穿），再 git worktree prune 清 git 元数据。
//
// 为什么不先 `git worktree remove --force`：实测它在 Windows 上**会跟穿 junction** 递归删，
// 把仓库外目标一起删掉（陷阱 6 正是要防的）。故工作区由 removeTreeSafe 自己删（Lstat 不跟随
// 链接），git 侧只做元数据注销（工作区已删 → remove 会报"不存在"，用 prune 兜底）。
// 幂等：删已不存在的 → 清登记即返回。
func (m *Manager) Remove(ctx context.Context, name string) error {
	m.mu.Lock()
	wt, ok := m.entries[name]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("worktree: %q 未登记", name)
	}
	m.mu.Unlock()

	// ① junction-safe 删工作区（自己删，不交给 git——git 会跟穿 junction）。
	if err := removeTreeSafe(wt.Path); err != nil {
		return fmt.Errorf("worktree: 删除 %s: %w", wt.Path, err)
	}
	// ② 清 git 元数据（清理可能比调用方 ctx 活得更久 → 用 background）。
	_, _, _ = gitRun(context.Background(), wt.Repo, "worktree", "prune")

	m.mu.Lock()
	delete(m.entries, name)
	m.mu.Unlock()
	return nil
}

// Get 取一个已登记 worktree（不存在 → ok=false）。
func (m *Manager) Get(name string) (Worktree, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	wt, ok := m.entries[name]
	return wt, ok
}

// List 返回所有已登记 worktree（按 name 排序，确定性）。
func (m *Manager) List() []Worktree {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Worktree, 0, len(m.entries))
	for _, wt := range m.entries {
		out = append(out, wt)
	}
	sortByName(out)
	return out
}

func sortByName(ws []Worktree) {
	for i := 1; i < len(ws); i++ {
		for j := i; j > 0 && ws[j].Name < ws[j-1].Name; j-- {
			ws[j], ws[j-1] = ws[j-1], ws[j]
		}
	}
}
