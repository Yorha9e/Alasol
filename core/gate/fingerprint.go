package gate

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

// 失败输出归一化用的正则（去"时间戳/临时路径"等每次跑都不同的噪声，V2/W9c）。
//
// 指纹是**启发式**（v0）：目标只是让"同一条失败"在两次跑里产出同一个指纹，
// 从而区分"已知失败"与"新增失败"。它不追求语义完备——规范化的取舍
// （哪些算噪声）是刻意的、可预期的：宁可把易变的部分抹平成占位符，
// 也不要让一条稳定失败因为一个时间戳被误判成"新增"。
var (
	// ISO 8601 / 常见日期时间：2025-01-02T03:04:05.678Z / 2025-01-02 03:04:05+08:00。
	reTimestamp = regexp.MustCompile(`\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?`)
	// 时刻：03:04:05(.678)。
	reTime = regexp.MustCompile(`\b\d{2}:\d{2}:\d{2}(?:\.\d+)?\b`)
	// 时长：1.5s / 230ms / 50µs / 40us / 12ns（只收这些单位，避免把 "5 files" 误伤）。
	reDuration = regexp.MustCompile(`\b\d+(?:\.\d+)?(?:ms|µs|us|ns|s)\b`)
	// 十六进制地址 / 大数：0x7ffd1234。
	reHexAddr = regexp.MustCompile(`0x[0-9a-fA-F]+`)
	// Unix 临时目录：/tmp/... /var/folders/... /private/... 。
	reUnixTmp = regexp.MustCompile(`/(?:tmp|var/folders|var/tmp|private)/[^\s]*`)
	// Windows 临时目录：C:\...\Temp\... / ...\Tmp\... （大小写不敏感）。
	reWinTmp = regexp.MustCompile(`(?i)[a-z]:\\[^\s]*\\(?:temp|tmp)\\[^\s]*`)
	// Windows 用户目录：C:\Users\<name>\... （大小写不敏感）。
	reWinUser = regexp.MustCompile(`(?i)[a-z]:\\users\\[^\s]+`)
)

// ExtractFailures 从门禁输出中提取失败条目（v0 启发式：每个非空的规范化行 = 一个失败条目）。
//
// 去重保序（同一行重复出现只留一条）。退出码为 0 时调用方不应调用（无失败）；
// 传空串返回 nil。
func ExtractFailures(output string) []string {
	if output == "" {
		return nil
	}
	seen := make(map[string]bool)
	var out []string
	for _, line := range strings.Split(output, "\n") {
		norm := normalizeFailureLine(line)
		if norm == "" || seen[norm] {
			continue
		}
		seen[norm] = true
		out = append(out, norm)
	}
	return out
}

// FailureFingerprint 计算单个失败条目的指纹：规范化后 SHA-256，hex 编码。
//
// 对已规范化的条目（ExtractFailures 的产出）再次调用是幂等的（占位符不会被正则二次命中），
// 因此 Judge 可直接对 Receipt.Failures 逐条指纹，与记录时一致。
func FailureFingerprint(failure string) string {
	sum := sha256.Sum256([]byte(normalizeFailureLine(failure)))
	return hex.EncodeToString(sum[:])
}

// normalizeFailureLine 归一化一行失败输出：去首尾空白 + 把易变片段抹成占位符。
// 幂等：占位符（<TS>/<TIME>/...）不会被我方正则再次命中。
func normalizeFailureLine(line string) string {
	line = strings.TrimSpace(line)
	if line == "" {
		return ""
	}
	line = reTimestamp.ReplaceAllString(line, "<TS>")
	line = reTime.ReplaceAllString(line, "<TIME>")
	line = reDuration.ReplaceAllString(line, "<DUR>")
	line = reHexAddr.ReplaceAllString(line, "<ADDR>")
	line = reUnixTmp.ReplaceAllString(line, "<TMP>")
	line = reWinTmp.ReplaceAllString(line, "<TMP>")
	line = reWinUser.ReplaceAllString(line, "<HOME>")
	return strings.TrimSpace(line)
}
