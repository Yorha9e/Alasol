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
// 登记表是**进程内内存**（name → 元数据），但**可重建**（v2-S5 清账③）：worktree 在磁盘上的
// git 元数据（.git/worktrees）跨进程存活，所以"谁创建了哪个"不必随 hub 进程一起消失——
// 启动时 Manager.Rebuild 用 `git worktree list --porcelain` 从磁盘把登记表重建回来
// （主仓库自身不登记；重建是幂等合并，不覆盖已登记的元数据）。
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
//
// JSON 形状走 **snake_case**（G6：对齐全仓视图惯例——SessionRow/MonitorView/
// TokenTotalView/OnlineTableView 都是 snake_case；此前无 tag 落到 PascalCase，
// 第三方 GUI 要为这一个类型特例）。worktree 是 **HTTP 视图**（GET /api/worktrees），
// **不是** contracts 的跨边界契约类型（不进 INVENTORY 的 11 个契约）——改 tag 只动
// 本 HTTP 响应形状，不触契约冻结面。
type Worktree struct {
	// Name 是登记名（调用方给的短名，用作 root 下的子目录名）。
	Name string `json:"name"`
	// Path 是 worktree 的绝对路径（root/<name>）。
	Path string `json:"path"`
	// Repo 是它从哪个仓库检出（主仓库或另一个 worktree 的路径）。
	Repo string `json:"repo"`
	// CreatedAt 创建时刻。
	CreatedAt time.Time `json:"created_at"`
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

// Rebuild 从**磁盘上的 git 元数据**重建登记表（v2-S5 清账③）。
//
// 口径：
//   - 扫 root 下的每个子目录，对仍是 git worktree 的目录跑 `git worktree list --porcelain`；
//   - 该命令的**第一条**记录是主仓库（main worktree）——**永不登记**（本管理器只登记
//     自己建的 root/<name>）；**按序号跳过，不靠目录名比对**（主仓库完全可能就落在 root
//     下：目录名一旦撞上（root/mainrepo 里放主仓库、worktree 也叫 mainrepo），
//     "名字相等"就会把**用户的主仓库**登记成受管 worktree → Remove 直接删掉它的工作区）；
//   - 其余记录里路径落在 root 下的 → 登记回来（Name = 相对 root 的目录名，
//     Repo = 主仓库路径，即"它从哪个仓库检出"）；
//   - **幂等合并**：已登记的同名条目不覆盖（Create 时记的元数据更准——尤其 CreatedAt），
//     只补丢失的；root 下的目录已不在 git 的 worktree 清单里（被 prune 过/手工删了）
//     → 不登记（没有元数据支撑的登记是假登记）。
//
// CreatedAt 的诚实口径：git **不记** worktree 创建时刻 → 重建时取目录 mtime 作近似
// （诊断字段，不参与任何判定；宁给近似值不给假精确值）。
//
// 为什么不做成全自动"权威源"：登记表仍是进程内存（host 的 Worktrees()/SessionWorktree 靠它），
// 磁盘只是**重建依据**——两者不一致时以登记表为准（它是本进程刚做过的事实的记录），
// 重建只负责"重启后别丢"。
//
// 返回重建补登的条数。git 不可用 / root 为空 → (0, nil)（"没什么可重建"不是错误）。
//
// 成本口径：每个**未登记**的子目录各跑一次 git（已登记的跳过，见实现）——启动期一次性，
// 量级 = root 下的 worktree 数 × 单次 git（十毫秒级）。不并发跑：启动路径要确定性，
// 且这点耗时相对"开库 + 播种投影 + 起索引器"可以忽略。
func (m *Manager) Rebuild(ctx context.Context) (int, error) {
	if m == nil {
		return 0, errors.New("worktree: Manager 为 nil")
	}
	entries, err := os.ReadDir(m.root)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("worktree: 扫 root %s: %w", m.root, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	added := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		// **幂等合并的唯一关口**：已登记的同名条目不碰（保留 Create 时记的元数据）。
		// 顺带省掉对已知条目跑 git（启动时 root 下每个目录都要问一次 git，能不跑就不跑）。
		// 下面内层不再重复判存在性——root 下的目录名互不相同，且只登记 n == name 的路径，
		// 一个条目只有一个来源（判据重复两遍只会让"哪一处在保证不变式"变含糊）。
		if _, ok := m.entries[name]; ok {
			continue
		}
		path := filepath.Join(m.root, name)
		// 对该目录问 git（它是某个仓库的 linked worktree）→ 拿到该仓库的全部 worktree 清单。
		repo, paths, ok, err := gitWorktreeList(ctx, path)
		if err != nil || !ok {
			continue // 不是 git worktree / git 不可用：跳过（不拿猜测当事实）
		}
		for i, p := range paths {
			if i == 0 {
				// **第一条 = 主仓库（main worktree）**：永不登记（它不是本管理器建的，
				// 甚至可能就在 root 下——按序号跳过才不会被"目录名撞上"骗进登记表）。
				continue
			}
			if !withinRoot(m.root, p) {
				continue // 别处的 worktree：不是本管理器建的
			}
			n := filepath.Base(p)
			if n == name && n != "." && n != string(filepath.Separator) {
				m.entries[n] = Worktree{Name: n, Path: p, Repo: repo, CreatedAt: dirModTime(p)}
				added++
			}
		}
	}
	return added, nil
}

// withinRoot 报告 path 是否位于 root 之下（路径前缀比较，先统一分隔符与大小写口径
// 由调用方保证——root 与 git 输出都来自同一台机器同一套路径）。
func withinRoot(root, path string) bool {
	if root == "" || path == "" {
		return false
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	if rel == "." {
		return false // root 自身（主仓库恰好放在 root 里）不算
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// dirModTime 取目录 mtime（CreatedAt 的近似值；失败 → 零值，由消费方决定怎么显示）。
func dirModTime(path string) time.Time {
	fi, err := os.Stat(path)
	if err != nil {
		return time.Time{}
	}
	return fi.ModTime()
}

// gitWorktreeList 在 dir（一个 linked worktree）上跑 `git worktree list --porcelain`，
// 返回 (主仓库路径, 全部 worktree 路径, ok, error)。
//
// porcelain 输出的每个记录是一组 `key value` 行：
//
//	worktree /path/to/main      ← 第一条 = 主仓库
//	HEAD <sha>
//	branch refs/heads/main      （detached worktree 这里是 "detached"）
//	worktree /path/to/linked
//	HEAD <sha>
//	detached
//
// 只取 `worktree` 行（路径）；**不解析 branch/HEAD**——登记表不需要它们
// （要分支信息的是 gate 快照，另一条路径）。
//
// **返回的 paths[0] 恒为主仓库**（git 的输出顺序：主仓库在前）——调用方**按序号**跳过它，
// 不靠目录名/路径前缀猜（主仓库完全可能就落在受管 root 下）。
func gitWorktreeList(ctx context.Context, dir string) (repo string, paths []string, ok bool, err error) {
	if _, err := gitExecutable(); err != nil {
		return "", nil, false, err // git 不可用：调用方按"跳过"处理
	}
	out, code, err := gitRun(ctx, dir, "worktree", "list", "--porcelain")
	if err != nil || code != 0 {
		// 不是 git worktree（或 git 报错）→ 没法重建，不是故障。
		return "", nil, false, nil
	}
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimRight(line, "\r")
		if !strings.HasPrefix(line, "worktree ") {
			continue
		}
		p := strings.TrimSpace(strings.TrimPrefix(line, "worktree "))
		if p == "" {
			continue
		}
		// git 输出的是正斜杠（它内部统一用 /）→ 转回平台分隔符，
		// 与 Create 记的 Path（filepath.Join）同形状（否则同一个 worktree 两种路径表示）。
		p = filepath.FromSlash(p)
		if repo == "" {
			repo = p // 第一条 = 主仓库
		}
		paths = append(paths, p)
	}
	return repo, paths, len(paths) > 0, nil
}
