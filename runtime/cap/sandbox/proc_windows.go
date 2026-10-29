//go:build windows

package sandbox

import (
	"fmt"
	"os/exec"
	"strconv"
	"syscall"

	"golang.org/x/sys/windows"
)

// 平台相关的"怎么起进程 / 怎么杀进程"细节（与 proc_other.go 成对）。
//
// 为什么拆成 build tag 文件：syscall.SysProcAttr 的字段集**按平台不同**——
// Windows 有 HideWindow / Token，Unix 没有；写在通用文件里另一边编译不过。
// 这也是 local 包同样的拆分方式（proc_windows.go / proc_other.go）。

// createSuspended 是 CREATE_SUSPENDED（Windows 创建标志）。
//
// 以它创建的进程**一行代码都还没跑**（主线程被挂起）——沙箱要的就是这个：
// 先把进程纳入 job object，再恢复它执行，中间没有"已经能 fork 了"的窗口（F13）。
const createSuspended = 0x00000004

// procNtResumeProcess 是 ntdll.NtResumeProcess（未公开但长期稳定的导出）。
//
// 为什么不用 ResumeThread：os/exec 在 CreateProcess 之后**立刻关掉主线程句柄**
// （见 Go 源码 syscall/exec_windows.go 的 `defer CloseHandle(Handle(pi.Thread))`），
// 所以拿不到 ResumeThread 需要的线程句柄。NtResumeProcess 只需要**进程**句柄，
// 把该进程所有线程的挂起计数减一——对"只挂起了主线程"的新进程等效于 ResumeThread。
var procNtResumeProcess = windows.NewLazySystemDLL("ntdll").NewProc("NtResumeProcess")

// hideWindow 给子进程加 windowsHide（陷阱 5）：不起控制台窗口。
func hideWindow(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	attr := newSysProcAttr()
	cmd.SysProcAttr = &attr
}

// applySpawn 把后端的 SpawnOptions 合进 exec.Cmd。
//
// Token 是本次执行的核心：受限主 token 交给 os/exec 后，Windows 上走的是
// CreateProcessAsUser（见 syscall/exec_windows.go），子进程因此以受限身份运行。
// token 为 nil（后端没给 / 策略关掉受限 token）→ 走普通 CreateProcess，宿主身份。
//
// Suspend 为真时加 CREATE_SUSPENDED（F13）：进程挂起创建，等 Attach 纳入 job
// 之后由 resumeProcess 恢复。
func applySpawn(cmd *exec.Cmd, opts SpawnOptions) {
	if cmd == nil {
		return
	}
	attr := newSysProcAttr()
	if opts.Token != nil {
		if tok, ok := opts.Token.(windows.Token); ok && tok != 0 {
			attr.Token = syscall.Token(tok)
		}
	}
	if opts.Suspend {
		attr.CreationFlags |= createSuspended
	}
	cmd.SysProcAttr = &attr
}

// resumeProcess 恢复一个以 CREATE_SUSPENDED 创建的进程（F13 的"另一半"）。
//
// 必须在进程被纳入 job object **之后**调用——顺序反了就有窗口。
func resumeProcess(pid int) error {
	if err := procNtResumeProcess.Find(); err != nil {
		return fmt.Errorf("ntdll.NtResumeProcess 不可用: %w", err)
	}
	// PROCESS_SUSPEND_RESUME 是恢复挂起进程所需的最小权利（同用户创建的子进程拿得到）。
	h, err := windows.OpenProcess(windows.PROCESS_SUSPEND_RESUME, false, uint32(pid))
	if err != nil {
		return fmt.Errorf("OpenProcess(%d) 以恢复挂起权限打开失败: %w", pid, err)
	}
	defer windows.CloseHandle(h)
	r1, _, callErr := procNtResumeProcess.Call(uintptr(h))
	if r1 != 0 { // NTSTATUS != 0 → 失败
		return fmt.Errorf("NtResumeProcess(%d): status=0x%x %v", pid, r1, callErr)
	}
	return nil
}

// newSysProcAttr 造一个带上限的 SysProcAttr（windowsHide 恒开）。
func newSysProcAttr() syscall.SysProcAttr {
	return syscall.SysProcAttr{HideWindow: true}
}

// killTreeFallbackEnabled 是"要不要用 taskkill 兜底"的开关（**仅测试用**）。
//
// 为什么需要它（F22）：沙箱里"进程被杀掉"有两条路径——ctx 取消时的 taskkill 兜底，
// 与 job object 的 KILL_ON_JOB_CLOSE。测试要证明"是 job 收口的"时，得先把兜底关掉，
// 否则两条路径的效果一样，证明不了任何事。
//
// 导出开关在 export_windows_test.go（测试专用导出，不是产品 API）。
var killTreeFallbackEnabled = true

// killTree 杀进程树（Windows：taskkill /T /F /PID，陷阱 5 的定案做法）。
//
// 与 local.killTree 同款：孙子进程继承 stdout/stderr 管道写端，只杀直接子进程会让
// cmd.Wait 挂到孙子自己退出（local 实测 29 秒）。杀树让管道立刻 EOF。
// taskkill 失败（不存在 / 进程已退出）时静默返回——由 WaitDelay 兜底。
//
// 沙箱里它只是**兜底**：正常收口靠 job object 的 KILL_ON_JOB_CLOSE。留着 taskkill
// 的理由是"job 没建成 / 进程已脱离 job"时仍然要把已知的树杀掉。
func killTree(cmd *exec.Cmd) {
	if !killTreeFallbackEnabled {
		return // 测试模式：只留 job object 收口（证明"是 job 干的"）
	}
	if cmd == nil || cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	tk := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(pid))
	hideWindow(tk)
	_ = tk.Run() // 进程已退出时 taskkill 报错，忽略（Cancel 不该把噪声变成失败）
}

// isProcessGone 判定错误是否"进程已退出"这一良性情形。
//
// Attach 在一个已经跑完的短命令上时会走到这里：OpenProcess 对不存在的 pid 报
// ERROR_INVALID_PARAMETER（不是 ERROR_NOT_FOUND——本机实测）。当成"没什么可收容的"
// 处理，不让整个 Run 失败。
//
// 注意 ERROR_ACCESS_DENIED **不算**：那是真的没权限打开进程，是约束没施加成功，
// 必须报错（fail-close）。
func isProcessGone(err error) bool {
	if err == nil {
		return false
	}
	return err == windows.ERROR_INVALID_PARAMETER || err == windows.ERROR_NOT_FOUND
}
