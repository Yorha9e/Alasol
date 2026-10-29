//go:build windows

package sandbox

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

// 本文件落地 V10 的挂账项："WER 崩溃弹窗完整压制属沙箱层"。
//
// runtime/cap/local 只做 windowsHide（不起控制台窗口）；"子进程崩了别弹窗炸桌面"
// 整套落在本包。两道防线：
//
//  1. **SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX)**
//     ——进程级 error mode，**子进程会继承**（CreateProcess 带走），所以设一次
//     就覆盖整棵进程树。这是主力：它压掉"严重错误"对话框与 GP-fault 弹窗。
//  2. **WerAddExcludedApplication(exeName, bAllUsers=false)**
//     ——把可执行名写进 WER 的排除表（HKCU\...\Windows Error Reporting\ExcludedApplications，
//     **不需要管理员**，本机实测写入成功）。压的是 WER 自己那套"发送错误报告"流程。
//
// 纪律：WerAddExcludedApplication 是**持久的用户级注册表写入**，所以：
//   - 只加我们真正要起的可执行名（宿主自己 + 每次 Begin 时的子进程名）；
//   - Close 时用 WerRemoveExcludedApplication **只删我们加过的**（记账），
//     绝不删用户原本就有的排除项。
//
// 本机实测备注：WerAddExcludedApplication 返回的是 **HRESULT**（S_OK = 0），
// 不是 BOOL——照 BOOL 读会把成功当成失败。wer.dll 里没有 WerSetFlags（本机 Win11 26100），
// 所以不走那条路。

// SEM_* 常量（x/sys 未导出）。
const (
	semFailCriticalErrors = 0x0001
	semNoGPFaultErrorBox  = 0x0002
)

// procWerAddExcludedApplication / procWerRemoveExcludedApplication 在 wer.dll。
//
// 返回值是 HRESULT（S_OK = 0），不是 BOOL——别把 0 当失败。
var (
	procWerAddExcludedApplication    = windows.NewLazySystemDLL("wer").NewProc("WerAddExcludedApplication")
	procWerRemoveExcludedApplication = windows.NewLazySystemDLL("wer").NewProc("WerRemoveExcludedApplication")
)

// procGetErrorMode 是 kernel32.GetErrorMode（Windows 10 1607+）。
//
// **为什么不能拿 SetErrorMode 当读取用**（F4）：SetErrorMode 是"写"——
// SetErrorMode(0) 会把进程的 error mode **清零**再返回旧值。
// 拿它"读"当前值，等于每次读都把压制拆了（而且旧值被下一次读覆盖）。
// GetErrorMode 才是纯读。
var procGetErrorMode = windows.NewLazySystemDLL("kernel32").NewProc("GetErrorMode")

// suppressErrorMode 设置进程级 error mode（子进程继承），返回**之前**的值。
//
// **按位或**上压制位（F5）：不能整块覆盖写——那会抹掉宿主进程自己设的别的位。
// （SetErrorMode 的返回值就是旧值，所以"设之前的值"拿得到。）
func suppressErrorMode() uint32 {
	want := uint32(semFailCriticalErrors | semNoGPFaultErrorBox)
	if cur, ok := readErrorMode(); ok {
		windows.SetErrorMode(cur | want)
		return cur
	}
	// 老系统没有 GetErrorMode：退回 SetErrorMode 的返回值语义（旧值），
	// 这条路径会覆盖写（丢位）——只影响没有 GetErrorMode 的远古系统。
	return windows.SetErrorMode(want)
}

// readErrorMode 读当前进程级 error mode（**不改任何位**）。第二个返回值是"读到了吗"。
func readErrorMode() (uint32, bool) {
	if err := procGetErrorMode.Find(); err != nil {
		return 0, false
	}
	r1, _, _ := procGetErrorMode.Call()
	return uint32(r1), true
}

// restoreErrorMode 把 error mode 复原成旧值（Close 时调用，F5）。
//
// 压制是"借"宿主进程的 error mode 用的：Close 时必须还回去，
// 否则宿主后续的崩溃弹窗策略被沙箱永久改掉了。
func restoreErrorMode(prev uint32) {
	windows.SetErrorMode(prev)
}

// currentErrorMode 读当前 error mode（不改动）。
func currentErrorMode() uint32 {
	if v, ok := readErrorMode(); ok {
		return v
	}
	// 没有 GetErrorMode 的老系统：SetErrorMode 返回旧值，但**先把值写回去**
	// （不能传 0——那是"清零"，F4）。
	cur := windows.SetErrorMode(semFailCriticalErrors | semNoGPFaultErrorBox)
	windows.SetErrorMode(cur)
	return cur
}

// werExclude 把一个可执行名加进 WER 排除表（HRESULT 语义：0 = S_OK）。
func werExclude(exeName string) error {
	if exeName == "" {
		return fmt.Errorf("wer: 空的可执行名")
	}
	p, err := syscall.UTF16PtrFromString(exeName)
	if err != nil {
		return err
	}
	// bAllUsers = FALSE → 写 HKCU（不需要管理员）。
	r1, _, callErr := procWerAddExcludedApplication.Call(uintptr(unsafe.Pointer(p)), 0)
	if r1 != 0 { // HRESULT 失败
		return fmt.Errorf("WerAddExcludedApplication(%s): %w", exeName, callErr)
	}
	return nil
}

// werUnexclude 把可执行名从 WER 排除表移除（只删我们加过的）。
func werUnexclude(exeName string) error {
	if exeName == "" {
		return nil
	}
	p, err := syscall.UTF16PtrFromString(exeName)
	if err != nil {
		return err
	}
	r1, _, callErr := procWerRemoveExcludedApplication.Call(uintptr(unsafe.Pointer(p)), 0)
	if r1 != 0 {
		return fmt.Errorf("WerRemoveExcludedApplication(%s): %w", exeName, callErr)
	}
	return nil
}

// exeBaseName 取"可执行文件名"（WER 排除表按**文件名**记，不带路径）。
//
// spec.Name 可能是绝对路径（陷阱 7：git 必须 execFile git.exe）、可能是 PATH 里的
// 裸名、也可能带 .exe 后缀或不带。统一成 base name：`C:\...\git.exe` → `git.exe`。
func exeBaseName(name string) string {
	b := filepath.Base(strings.TrimSpace(name))
	if b == "." || b == string(filepath.Separator) {
		return ""
	}
	return b
}

// realExeBaseName 取"真实映像名"（F6）。
//
// WER 的排除表按**映像文件名**记（"cmd.exe"），而调用方给的是命令名（"cmd"）。
// 照抄调用方的字符串 = 往排除表里塞一个不存在的条目 → 压制不生效。
// 所以先用 LookPath 把 PATH 里的裸名解析成真实路径，再取 base name。
//
// 解析不出来（命令不存在 / 是绝对路径但没后缀）时退回调用方给的名字——
// 宁可排错一项，也不至于把"没解析出来"变成"崩溃"。
func realExeBaseName(name string) string {
	n := strings.TrimSpace(name)
	if n == "" {
		return ""
	}
	if !strings.ContainsAny(n, `\/`) {
		// PATH 里的裸名：LookPath 在 Windows 上会补 .exe 后缀（"cmd" → "cmd.exe"）。
		if lp, err := exec.LookPath(n); err == nil && lp != "" {
			return exeBaseName(lp)
		}
	}
	return exeBaseName(n)
}

// WER 排除表在注册表里的两个位置（HKCU，不需要管理员）。
//
// 本机实测（Windows 11 / 26100，WerAddExcludedApplication 写入后看注册表）：
//
//	HKCU\Software\Microsoft\Windows\Windows Error Reporting\ExcludedApplications
//	    └── 子键，**每个可执行名一个"值名"**（值数据为空）  ← 真正被写入的位置
//	HKCU\Software\Microsoft\Windows\Windows Error Reporting
//	    └── "ExcludedApplications"（REG_MULTI_SZ）              ← 旧版/其他工具的写法
//
// 之前只读第二个位置 → 永远读不到东西 → "用户本来就有的排除项"判断恒为假
// → Close 时把**用户自己**的排除项一起删了（F7/N1）。
//
// 两个位置都查：WerAddExcludedApplication 写的是子键，但别的工具/旧版可能写
// REG_MULTI_SZ 那条——漏掉哪边都可能误删用户的东西。
const (
	// werExclusionKey 是父键（…\Windows Error Reporting）。
	werExclusionKey = `Software\Microsoft\Windows\Windows Error Reporting`
	// werExclusionSubKey 是子键（…\Windows Error Reporting\ExcludedApplications）。
	werExclusionSubKey = werExclusionKey + `\ExcludedApplications`
)

// werAlreadyExcluded 报告该可执行名**本来就在**用户的 WER 排除表里（F7/N1）。
//
// 为什么必须先查：WerRemoveExcludedApplication 是按名字删的——
// 如果用户自己早就把 "cmd.exe" 排除了，我们 add 之后又 remove，就把**用户的**那一条
// 一起删了（用户的崩溃弹窗设置被我们悄悄改掉）。
//
// 查不到 / 读失败时一律按"不在表里"处理：宁可多留一条我们自己的排除项，
// 也不要误删用户自己的（F7 的取舍）。
func werAlreadyExcluded(exeName string) bool {
	if exeName == "" {
		return false
	}
	// 位置一（真机实测的真正位置）：子键的值名。
	if key, err := registry.OpenKey(registry.CURRENT_USER, werExclusionSubKey, registry.QUERY_VALUE); err == nil {
		names, err := key.ReadValueNames(-1)
		key.Close()
		if err == nil {
			for _, n := range names {
				if strings.EqualFold(strings.TrimSpace(n), exeName) {
					return true
				}
			}
		}
	}
	// 位置二（旧版/其他工具的写法）：父键下的 REG_MULTI_SZ 值。
	if key, err := registry.OpenKey(registry.CURRENT_USER, werExclusionKey, registry.QUERY_VALUE); err == nil {
		names, _, err := key.GetStringsValue("ExcludedApplications")
		key.Close()
		if err == nil {
			for _, n := range names {
				if strings.EqualFold(strings.TrimSpace(n), exeName) {
					return true
				}
			}
		}
	}
	return false
}

// werBookkeeping 记录"我们加过哪些 WER 排除项"（Close 时只删这些）。
//
// **必须加锁**：Begin 会被并发 Run 调用（沙箱 env 是并发安全的），而 add 会写 map。
// 本机 -race 实测抓到这个裸 map 的写竞争（见 git 历史里的修复）。
type werBookkeeping struct {
	mu    sync.Mutex
	added map[string]struct{}
}

func newWerBookkeeping() *werBookkeeping {
	return &werBookkeeping{added: map[string]struct{}{}}
}

// add 加排除项并记账（已记过的不重复加，返回 nil 表示"已在表里"）。
//
// F7/N1：**用户本来就有的**排除项不记账——记账了 Close 就会把它删掉，
// 那是删用户自己的设置。别人的排除项我们不碰。
//
// "本来就有"的判定靠 werAlreadyExcluded（它查的是**真机实测**的注册表位置：
// 子键 …\Windows Error Reporting\ExcludedApplications 的值名 + 父键的
// REG_MULTI_SZ 兼容位）。查错了位置，判断恒为假 → 用户原有项被误删（N1）。
func (w *werBookkeeping) add(name string) error {
	if name == "" {
		return nil
	}
	w.mu.Lock()
	if _, ok := w.added[name]; ok {
		w.mu.Unlock()
		return nil
	}
	w.mu.Unlock()
	if werAlreadyExcluded(name) {
		// 用户自己就排除了：不动它，也**不记账**（Close 不该删它）。
		return nil
	}
	if err := werExclude(name); err != nil {
		return err
	}
	w.mu.Lock()
	w.added[name] = struct{}{}
	w.mu.Unlock()
	return nil
}

// has 报告 name 是否已由**我们**加过（埋点据此区分"新加的"与"本来就在"）。
func (w *werBookkeeping) has(name string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.added[name]
	return ok
}

// removeAll 撤掉我们加过的全部排除项（返回失败项，不中断）。
func (w *werBookkeeping) removeAll() []error {
	w.mu.Lock()
	names := make([]string, 0, len(w.added))
	for name := range w.added {
		names = append(names, name)
	}
	w.added = map[string]struct{}{}
	w.mu.Unlock()

	var errs []error
	for _, name := range names {
		if err := werUnexclude(name); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// hostExeName 返回宿主进程的可执行文件名（沙箱自己也该被排除出 WER 弹窗）。
func hostExeName() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	return exeBaseName(exe)
}
