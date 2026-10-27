package gate

import (
	"testing"
)

// TestNormalizationTimestampPathInvariance 钉住归一化的核心目标（W9c 指纹启发式）：
// 两条失败只在"时间戳 / 临时路径"上不同 → 归一化后同一个指纹。
// 否则一条稳定失败会因每次不同的时间戳被误判成"新增"，baseline 永远建不起来。
func TestNormalizationTimestampPathInvariance(t *testing.T) {
	a := "FAIL: boom at 2025-01-02T03:04:05.678Z in /tmp/alasol-123/x after 1.5s"
	b := "FAIL: boom at 2026-12-31T23:59:59.000Z in /var/folders/ab/cd/T/alasol-999/y after 230ms"
	if FailureFingerprint(a) != FailureFingerprint(b) {
		t.Errorf("仅时间戳/临时路径/时长不同的两条失败应同指纹：\n a=%q\n b=%q", a, b)
	}
}

// TestNormalizationContentSensitive：内容真的不同 → 指纹必须不同（否则漏拦新增失败）。
func TestNormalizationContentSensitive(t *testing.T) {
	if FailureFingerprint("FAIL: test_alpha") == FailureFingerprint("FAIL: test_beta") {
		t.Error("不同内容的失败应有不同指纹")
	}
}

// TestNormalizationIdempotent：对已规范化的条目再归一化应稳定（Judge 对 Receipt.Failures 逐条指纹依赖此）。
func TestNormalizationIdempotent(t *testing.T) {
	line := "FAIL: boom at 2025-01-02T03:04:05Z in /tmp/x"
	once := normalizeFailureLine(line)
	twice := normalizeFailureLine(once)
	if once != twice {
		t.Errorf("归一化应幂等：once=%q twice=%q", once, twice)
	}
	if FailureFingerprint(line) != FailureFingerprint(once) {
		t.Error("对原始行与已规范化行算指纹应一致")
	}
}

func TestNormalizeFailureLine(t *testing.T) {
	cases := []struct{ in, want string }{
		{"  FAIL: x  ", "FAIL: x"},                                 // 去首尾空白
		{"at 2025-01-02T03:04:05Z", "at <TS>"},                     // ISO 时间戳
		{"took 230ms / 1.5s / 40us", "took <DUR> / <DUR> / <DUR>"}, // 时长
		{"addr 0x7ffd1234", "addr <ADDR>"},                         // 十六进制地址
		{"in /tmp/foo/bar", "in <TMP>"},                            // Unix 临时路径
		{`in C:\Users\me\AppData\Local\Temp\x`, "in <TMP>"},        // Windows 临时路径
	}
	for _, tc := range cases {
		if got := normalizeFailureLine(tc.in); got != tc.want {
			t.Errorf("normalizeFailureLine(%q) = %q，期望 %q", tc.in, got, tc.want)
		}
	}
}

func TestExtractFailures(t *testing.T) {
	// 空输出 → nil。
	if got := ExtractFailures(""); got != nil {
		t.Errorf("空输出应得 nil，得到 %v", got)
	}
	// 去重 + 去空行 + 保序。
	out := ExtractFailures("FAIL: a\n\nFAIL: b\nFAIL: a\n   \nFAIL: c")
	want := []string{"FAIL: a", "FAIL: b", "FAIL: c"}
	if len(out) != len(want) {
		t.Fatalf("ExtractFailures = %v，期望 %v", out, want)
	}
	for i := range want {
		if out[i] != want[i] {
			t.Errorf("out[%d] = %q，期望 %q", i, out[i], want[i])
		}
	}
}

// TestExtractFailuresCollapsesTimestampNoise：同一失败两次输出（时间戳不同）→ 提取后相同 → 指纹相同。
func TestExtractFailuresCollapsesTimestampNoise(t *testing.T) {
	a := ExtractFailures("FAIL: flaky at 2025-01-01T00:00:00Z")
	b := ExtractFailures("FAIL: flaky at 2025-06-06T12:34:56Z")
	if len(a) != 1 || len(b) != 1 || a[0] != b[0] {
		t.Fatalf("仅时间戳不同的失败应归一为同一条：a=%v b=%v", a, b)
	}
}
