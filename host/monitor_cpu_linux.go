//go:build linux

package host

import (
	"os"
	"strconv"
	"strings"
)

// cpuTimeNanos 返回进程累计 CPU 时间（纳秒）：读 /proc/self/stat 的 utime + stime
// （第 14、15 字段，单位 clock tick，Linux 上 USER_HZ 恒为 100）。comm 字段可能含
// 空格/括号，故在**最后一个 ')' 之后**切分。读失败 → 0（CPU% 退化为不判，不影响其它指标）。
func cpuTimeNanos() int64 {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0
	}
	s := string(data)
	rp := strings.LastIndexByte(s, ')')
	if rp < 0 || rp+2 > len(s) {
		return 0
	}
	// ')' 之后以空格分隔；该处第一个 token 是 field 3（state）。
	fields := strings.Fields(s[rp+2:])
	// utime = field 14 → index 14-3 = 11；stime = field 15 → index 12。
	const userHz = 100
	if len(fields) <= 12 {
		return 0
	}
	utime, err1 := strconv.ParseInt(fields[11], 10, 64)
	stime, err2 := strconv.ParseInt(fields[12], 10, 64)
	if err1 != nil || err2 != nil {
		return 0
	}
	return (utime + stime) * (1_000_000_000 / userHz)
}
