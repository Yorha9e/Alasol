//go:build windows

package sandbox

import (
	"fmt"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

// 本文件是 Windows 三件套的 **a. job object**（进程树收容）。
//
// # 为什么 job object 是"进程树收容"的正解
//
// runtime/cap/local 用 taskkill /T /F /PID 杀树，只能杀**已知**的树：命令自己
// daemonize / detach 出去的进程（脱离了树）它看不见（local 包文档的"仍属已知边界"）。
// job object 不一样：子进程**连同它后代创建的一切进程**都在 job 里，只要 job 句柄
// 还开着，它们就跑；句柄一关（JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE），整锅端走。
//
// 本机实测（Windows 11 / Go 1.26.5，非管理员）：
//   - cmd /c 起的子进程里再 `start /b` 一个孙子进程，孙子活得比子进程长；
//   - 子进程退出后、job 句柄关闭前，孙子**仍然活着**（收容生效）；
//   - 关闭 job 句柄后，孙子在数百毫秒内被杀掉。
//
// 见 job_windows_test.go 的 TestJobObjectKillOnJobCloseContainsDetachedGrandchild。

// jobObjectExtendedLimitInformation 是 SetInformationJobObject 的信息类（class 9）。
const jobObjectExtendedLimitInformation = 9

// 与 x/sys 的 JOBOBJECT_BASIC_LIMIT_INFORMATION 逐字段对齐的结构体。
//
// 为什么自己抄一份而不用 x/sys 的常量：x/sys/windows 只导出了 LimitFlags 的各位掩码
// （JOB_OBJECT_LIMIT_*）与信息类编号，**没有**导出 BASIC/EXTENDED 这两个结构体。
// 抄结构体比 unsafe 拼字节数组可读得多；字段顺序与对齐按 Win32 SDK 原样抄。
type jobBasicLimitInformation struct {
	PerProcessUserTimeLimit int64
	PerJobUserTimeLimit     int64
	LimitFlags              uint32
	MinimumWorkingSetSize   uintptr
	MaximumWorkingSetSize   uintptr
	ActiveProcessLimit      uint32
	Affinity                uintptr
	PriorityClass           uint32
	SchedulingClass         uint32
}

// jobExtendedLimitInformation 对应 JOBOBJECT_EXTENDED_LIMIT_INFORMATION。
//
// IoInfo 是 IO_COUNTERS（6 个 ULONGLONG）——漏掉它会让 SetInformationJobObject
// 报 ERROR_BAD_LENGTH（本机实测踩过：把它写成一个 uintptr 直接 "命令长度不正确"）。
type jobExtendedLimitInformation struct {
	BasicLimitInformation jobBasicLimitInformation
	IoInfo                [6]uint64
	ProcessMemoryLimit    uintptr
	JobMemoryLimit        uintptr
	PeakProcessMemoryUsed uintptr
	PeakJobMemoryUsed     uintptr
}

// newJobObject 建一个无名 job object 并施加限制。
//
// 无名（name = nil）的理由：命名的 job object 会被**同会话的其他进程**按名字打开，
// 那样"我们的沙箱 job"就不再是我们的了。无名 job 只经句柄可达，天然私有。
//
// maxActive ≤ 0 → 取 DefaultMaxActiveProcesses（F3）：Policy 文档承诺"0 = 默认 64"，
// 这里必须真的设上限——之前 0 被当成"不设上限"，fork 炸弹于是无界。
func newJobObject(maxActive int) (windows.Handle, error) {
	if maxActive <= 0 {
		maxActive = DefaultMaxActiveProcesses
	}
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return 0, fmt.Errorf("CreateJobObject: %w", err)
	}
	lim := jobExtendedLimitInformation{}
	lim.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	lim.BasicLimitInformation.LimitFlags |= windows.JOB_OBJECT_LIMIT_ACTIVE_PROCESS
	lim.BasicLimitInformation.ActiveProcessLimit = uint32(maxActive)
	if _, err := windows.SetInformationJobObject(job, jobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&lim)), uint32(unsafe.Sizeof(lim))); err != nil {
		windows.CloseHandle(job)
		return 0, fmt.Errorf("SetInformationJobObject: %w", err)
	}
	return job, nil
}

// assignToJob 把 pid 纳入 job。
//
// 打开进程只需要 PROCESS_SET_QUOTA | PROCESS_TERMINATE（AssignProcessToJobObject 的
// 文档要求）——**不要**用 PROCESS_ALL_ACCESS：那会要一堆用不到的特权，普通用户反而开不了。
//
// 进程已经退出时 OpenProcess 会失败：这不是错误（"没什么可收容的"），由调用方判 ENOENT。
func assignToJob(job windows.Handle, pid int) error {
	ph, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		return fmt.Errorf("OpenProcess(%d): %w", pid, err)
	}
	defer windows.CloseHandle(ph)
	if err := windows.AssignProcessToJobObject(job, ph); err != nil {
		return fmt.Errorf("AssignProcessToJobObject(%d): %w", pid, err)
	}
	return nil
}

// killTree 的实现在 proc_windows.go（平台相关文件，与 proc_other.go 成对）。
//
// 这里只留 job object 本身的机械原理，避免一个文件塞三种关注点。

// pidGone 判定"进程已退出"这一良性情形（Attach 时进程可能已经跑完）。
func pidGone(err error) bool {
	return err != nil && isProcessGone(err)
}

// jobRegistry 记录本后端还开着的 job 句柄（Close 时统一收口）。
//
// 为什么要它：正常路径上每次 Run 的 Release 都会关掉自己的 job；但如果 Run 中途
// panic / 被测代码忘了 Release（我们的 defer 保证不会，但别处可能直接用 Backend），
// 泄漏的 job 句柄会让 KILL_ON_JOB_CLOSE 迟迟不触发。Close 时兜底全杀。
//
// # 句柄只能关一次（F10）
//
// Windows 的句柄值**会被复用**：同一个句柄被关两次，第二次可能正好关掉系统刚发给
// 别人的那个句柄（别人的 job / 文件 / 进程就凭空消失了）。所以"从登记表里摘掉"与
// "关句柄"必须在**同一把锁里**做完，closeAll 也要在锁里关——
// 否则 Release 与 closeAll 交错时会双关同一个句柄（已复现）。
type jobRegistry struct {
	mu   sync.Mutex
	jobs map[windows.Handle]struct{}
}

func newJobRegistry() *jobRegistry {
	return &jobRegistry{jobs: map[windows.Handle]struct{}{}}
}

func (r *jobRegistry) add(h windows.Handle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs[h] = struct{}{}
}

// release 从登记表里摘掉 h 并**在锁内**关闭它（摘 + 关必须原子，F10）。
//
// 幂等：h 不在登记表里（已经被 closeAll 收走）就直接返回 nil，绝不重复关。
func (r *jobRegistry) release(h windows.Handle) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.jobs[h]; !ok {
		return nil // 已经收口过（幂等；也可能是别的 execution 刚关的）
	}
	delete(r.jobs, h)
	return windows.CloseHandle(h)
}

// closeAll 关掉所有还开着的 job 句柄（KILL_ON_JOB_CLOSE 随之收口）。
//
// 关闭动作在锁内完成：否则与 Release 交错会双关同一个句柄（F10）。
func (r *jobRegistry) closeAll() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for h := range r.jobs {
		windows.CloseHandle(h)
		delete(r.jobs, h)
		n++
	}
	return n
}
