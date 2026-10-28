//go:build windows

package host

import "golang.org/x/sys/windows"

// cpuTimeNanos 返回进程累计 CPU 时间（纳秒）：GetProcessTimes 的 KernelTime + UserTime
// （FILETIME，单位 100ns，自 1601 起——此处只用**增量**，绝对基点无所谓）。
// CurrentProcess 是伪句柄，无需关闭。取不到 → 0（CPU% 退化为不判）。
func cpuTimeNanos() int64 {
	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(windows.CurrentProcess(), &creation, &exit, &kernel, &user); err != nil {
		return 0
	}
	ftTo100ns := func(ft windows.Filetime) int64 {
		return int64(uint64(ft.HighDateTime)<<32 | uint64(ft.LowDateTime))
	}
	return (ftTo100ns(kernel) + ftTo100ns(user)) * 100 // 100ns → ns
}
