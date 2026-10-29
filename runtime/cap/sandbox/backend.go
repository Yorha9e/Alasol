package sandbox

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/Yorha9e/alasol/core"
)

// Backend 是沙箱后端（H9 埋点）：把"进程约束 / 文件约束"施加到子进程的**可插拔实现**。
//
// # 为什么是接口而不是一堆 if runtime.GOOS
//
// H9 的原话是"可拓展埋点，留 Linux/mac 沙箱（seatbelt/bwrap 后续挂）"。做成接口之后：
//
//   - Windows 三件套（job object + 受限 token + 目录 ACL）只是**其中一个**后端；
//   - Linux / macOS 的 seatbelt / bubblewrap 是预留挂点（见 backend_other.go 的接线说明），
//     将来实现时**新写一个包实现这个接口**，本包与 host 一行不改；
//   - 测试可以注入一个"记账后端"来酸测试 K8 的"换 env 不动 agent 逻辑"，
//     而不必在每个平台上都真起沙箱。
//
// # 生命周期
//
//	env := New(policy)        → 选后端 → backend.ApplyFileACL() → 每次 Run：
//	  exec := backend.Begin(spec)   // 分配本次执行的约束（job object / token）
//	  opts := exec.Spawn()          // 起进程时要带的东西（受限 token、env 覆盖）
//	  exec.Attach(pid)              // 进程起来后把约束施加上去（纳入 job）
//	  exec.Release()                // 本次执行结束：撤约束（关 job → KILL_ON_JOB_CLOSE）
//	env.Close()               → backend.Close()
//
// 顺序约束：Attach 必须在进程**起来之后**（AssignProcessToJobObject 需要进程句柄），
// Release 必须在 Wait 返回**之后**（提前关 job 会把还活着的子进程误杀——那正是
// KILL_ON_JOB_CLOSE 的语义，用在正常路径上就是 bug）。
type Backend interface {
	// Name 返回后端标识（埋点/诊断用；稳定字符串，如 "windows-job+token+acl"）。
	Name() string

	// Diagnostics 返回后端累计的埋点记录（只增；并发安全）。
	//
	// 这是"诚实边界"的出口：哪条腿真的施加成功了、哪条因为缺特权/缺平台退化了，
	// 都记在这里，装配方与测试都能读。沙箱最怕的不是"没做到"，是"以为做到了"。
	Diagnostics() []Diag

	// FileRoots 返回"允许写"的根集合（绝对路径、已归一化）。
	FileRoots() []string

	// ApplyFileACL 把允许根变成 OS 强制的可写面。
	//
	// Windows：SetNamedSecurityInfo 设带可继承 ACE 的 DACL（见 acl_windows.go）。
	// Linux/mac：seatbelt profile / bwrap 参数（预留挂点）。
	ApplyFileACL(ctx context.Context) error

	// Begin 为一次执行分配进程约束（返回的 Execution 绑定这次执行）。
	Begin(ctx context.Context, spec core.ExecSpec) (Execution, error)

	// Close 释放后端自身资源（WER 排除项回收、临时目录等）。
	Close() error
}

// rootsAware 是"允许根由 Env 注入"的后端（**可选**接口，F1）。
//
// 为什么需要它：允许根由 Env 统一派生（沙箱临时根**必须**在里面——子进程要写自己的
// %TEMP%），而后端手里那份 policy 副本可能是在 Env 派生之前造的（NewWithBackend 的
// 注入路径）。两份 policy 各算各的允许集 → 后端算出的那份少一个根 → 子进程写
// %TEMP% 被 ACL 拒（"允许集内也写不进去"，最难查的一类症状）。
//
// 实现了这个接口的后端，Env 会在施加 ACL **之前**把生效允许根告诉它——
// 于是"允许集"只有一份事实来源（Env），不再有第二份副本（D21）。
//
// 方法名故意**小写**：这是包内的可选钩子，不是公开 API——外部实现的后端
// 不可能也不需要实现它（没实现就退回"后端自己算"，行为与从前一致）。
type rootsAware interface {
	// setRoots 告知后端本次装配生效的允许根（绝对路径、已归一化去重）。
	setRoots(roots []string)
}

// Execution 是"一次执行"的进程约束句柄。
type Execution interface {
	// Spawn 返回这次执行的启动参数（受限 token、env 覆盖等）。
	Spawn() SpawnOptions

	// Attach 在进程启动后把约束施加到它（Windows：AssignProcessToJobObject）。
	//
	// 进程可能已经退出（短命令）：此时"没什么可收容的"，实现应当容忍而不是报错。
	Attach(pid int) error

	// Release 本次执行结束：撤约束（Windows：关 job 句柄 → KILL_ON_JOB_CLOSE 收尾）。
	Release() error
}

// SpawnOptions 是后端对"怎么起这个进程"的要求（平台中立形状）。
type SpawnOptions struct {
	// Token 是起子进程用的主 token 句柄（不透明值；Windows 后端给 windows.Token，
	// nil = 用宿主身份）。放成 any 是为了让 Backend 接口本身能跨平台编译——
	// syscall.Token 是 Windows 专有类型。
	Token any

	// Env 是沙箱要**追加/覆盖**的子进程环境变量（如 TMP/TEMP 重定向）。
	Env []string

	// Suspend 表示"进程要以**挂起**状态创建"（Windows: CREATE_SUSPENDED）。
	//
	// 为什么需要它（F13）：约束是"进程起来之后"施加的（Attach → 纳入 job），
	// 那中间有个窗口——进程已经在跑了，它可以在被纳入 job 之前 fork 出后代，
	// 那个后代**永远不会**被 job 收容（"进程树收容"就漏了）。
	// 以挂起状态创建 → 进程一行代码都还没跑 → 先纳入 job → 再恢复执行，
	// 窗口被彻底关掉。
	//
	// 平台实现负责"恢复"：Windows 走 NtResumeProcess（见 proc_windows.go）。
	Suspend bool
}

// Diag 是一条沙箱埋点记录（H9：把"哪些约束真的施加成功了"如实暴露出来）。
type Diag struct {
	// Backend 是产出这条记录的后端名。
	Backend string
	// Kind 是约束类别（"job-object" | "restricted-token" | "file-acl" | "wer" | "backend"）。
	Kind string
	// OK 表示这条约束是否成功生效。
	OK bool
	// Detail 是人类可读的细节（失败原因、施加到的对象、退化说明）。
	Detail string
}

// 后端注册表（H9 埋点）：名字 → 构造器。
var (
	backendMu    sync.RWMutex
	backendCtors = map[string]func(Policy) (Backend, error){}
)

// RegisterBackend 注册一个沙箱后端（新平台 / 新后端在这里挂）。
//
// 由各平台文件在 init() 里调用（backend_windows.go / backend_other.go）。
// 重名注册会 panic——那是编译期就该发现的编程错误，不是运行期该容忍的配置。
func RegisterBackend(name string, ctor func(Policy) (Backend, error)) {
	backendMu.Lock()
	defer backendMu.Unlock()
	if _, dup := backendCtors[name]; dup {
		panic("sandbox: 后端名重复注册：" + name)
	}
	backendCtors[name] = ctor
}

// BackendByName 取已注册的后端构造器。
func BackendByName(name string) (func(Policy) (Backend, error), bool) {
	backendMu.RLock()
	defer backendMu.RUnlock()
	c, ok := backendCtors[name]
	return c, ok
}

// BackendNames 返回已注册的后端名（排序，便于诊断输出稳定）。
func BackendNames() []string {
	backendMu.RLock()
	defer backendMu.RUnlock()
	out := make([]string, 0, len(backendCtors))
	for n := range backendCtors {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// PlatformBackendName 返回当前平台的默认后端名（由平台文件在 init 里登记）。
var PlatformBackendName = func() string { return "" }

// diagLog 是后端累计埋点的小工具（并发安全、只增、可读快照）。
type diagLog struct {
	mu   sync.Mutex
	name string
	recs []Diag
}

func (d *diagLog) add(kind string, ok bool, format string, args ...any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.recs = append(d.recs, Diag{
		Backend: d.name,
		Kind:    kind,
		OK:      ok,
		Detail:  fmt.Sprintf(format, args...),
	})
}

func (d *diagLog) snapshot() []Diag {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]Diag, len(d.recs))
	copy(out, d.recs)
	return out
}

// joined 把若干路径拼成人类可读的一行（埋点输出用）。
func joined(paths []string) string {
	if len(paths) == 0 {
		return "(none)"
	}
	return strings.Join(paths, ", ")
}
