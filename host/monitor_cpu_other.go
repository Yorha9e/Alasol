//go:build !linux && !windows

package host

// cpuTimeNanos 在未实现平台采样器的系统上返回 0：CPU% 退化为"不判"（恒 0，不触发 CPU
// 告警），内存与 goroutine 两项监控仍生效（跨平台可移植部分）。darwin 若要 CPU 需
// libproc/ host_statistics，留待需要时补。
func cpuTimeNanos() int64 { return 0 }
