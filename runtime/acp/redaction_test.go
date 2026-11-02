package acp

// 脱敏**误伤面**的钉子（T3-4）：
//
//  1. 参数化鉴权头的**通用词**（response/signature/nonce/opaque/pwd）只认**紧贴**形态
//     `name=value` ——散文里的 "The response: 200 OK" 不该被整段打码；
//  2. 高特异凭据名（api-key/password/token/…）**照旧**遮（`api-key: xxx` 这种冒号
//     赋值形态是常见真凭据，收紧误伤不能把覆盖丢了）；
//  3. **截断 PEM 块**（缺 END）不再吞掉整段余文（旧实现用 `$` 兜底）：只遮到行尾 +
//     其后的 base64 行，且限长 8KB。

import (
	"strings"
	"testing"
)

// TestRedactSecretText_ProseNotMasked：普通散文**不该**被当成凭据参数打码。
//
// 病根（旧实现）：一张名单 + `[:=]` 分隔 + 值前允许空白 → "The response: 200 OK" 里的
// "response: 200" 被整段打码（response 本是通用词，审批人看不到原文 = 噪声）。
func TestRedactSecretText_ProseNotMasked(t *testing.T) {
	cases := []string{
		"The response: 200 OK",
		"server response: 404 not found",
		"the nonce: 12345",
		"signature: verified",
	}
	for _, in := range cases {
		got := redactSecretText(in)
		if got != in {
			t.Errorf("散文不该被打码：%q → %q", in, got)
		}
	}
}

// TestRedactSecretText_GenericParamsStillMaskedInTightForm：通用词的**紧贴**形态
// （`name=value`）仍要遮——Digest 鉴权的 `response="abc…"` 正是这个形态（不能因收紧
// 误伤而漏放真凭据）。
func TestRedactSecretText_GenericParamsStillMaskedInTightForm(t *testing.T) {
	const secret = "abc123def456"
	cases := []string{
		`Authorization: Digest username="u", response="` + secret + `"`,
		"signature=" + secret,
		"nonce=" + secret,
		"pwd=" + secret,
	}
	for _, in := range cases {
		got := redactSecretText(in)
		if strings.Contains(got, secret) {
			t.Errorf("通用词的紧贴形态应打码：%q → %q", in, got)
		}
	}
}

// TestRedactSecretText_HighSpecificityParamsStillMasked：高特异凭据名**照旧**遮
// （含冒号 + 空格的赋值形态——那是常见真凭据，收紧误伤不许把覆盖丢了）。
func TestRedactSecretText_HighSpecificityParamsStillMasked(t *testing.T) {
	cases := []struct {
		in     string
		secret string
	}{
		{"curl -H \"api-key: zzzqqqxxxwwwvvv111\" https://api.example.com", "zzzqqqxxxwwwvvv111"},
		{"password: hunter2secret", "hunter2secret"},
		{"passwd=hunter2secret", "hunter2secret"},
		{"token=abc123def456", "abc123def456"},
		{"access_token=abc123def456", "abc123def456"},
		{"credential=AKIAEXAMPLE", "AKIAEXAMPLE"},
	}
	for _, c := range cases {
		got := redactSecretText(c.in)
		if strings.Contains(got, c.secret) {
			t.Errorf("高特异凭据名应打码：%q → %q", c.in, got)
		}
	}
}

// TestRedactPEMBlocks_TruncatedBlockDoesNotSwallowTail：**截断** PEM 块（缺 END）不再
// 吞掉整段余文——旧实现用 `$` 兜底，一行散文里的 "-----BEGIN" 能把后面几十 KB 全吃掉
// （误伤面失控）；新口径：只遮到行尾 + 其后连续的 base64 行（真密钥体），限长 8KB。
func TestRedactPEMBlocks_TruncatedBlockDoesNotSwallowTail(t *testing.T) {
	const body = "MIIEpAIBAAKCAQEAyabc123SECRETbodyZZZZ"
	const tail = "rest of the message"
	in := "-----BEGIN RSA PRIVATE KEY-----\n" + body + "\n" + tail

	got := redactSecretText(in)

	if strings.Contains(got, body) {
		t.Errorf("截断块的密钥体应打码：%q", got)
	}
	if !strings.Contains(got, tail) {
		t.Errorf("截断块不该吞掉整段余文（余文应保留）：%q", got)
	}
}

// TestRedactPEMBlocks_CompleteBlockMaskedEntirely：完整 PEM 块仍**整块**打码
// （body 是密钥，必须遮；END 之后的余文不受影响）。
func TestRedactPEMBlocks_CompleteBlockMaskedEntirely(t *testing.T) {
	const body = "MIIEpAIBAAKCAQEAyabc123SECRETbody"
	const tail = "after the block"
	in := "-----BEGIN RSA PRIVATE KEY-----\n" + body + "\n-----END RSA PRIVATE KEY-----\n" + tail

	got := redactSecretText(in)

	if strings.Contains(got, body) {
		t.Errorf("完整 PEM 块应整块打码：%q", got)
	}
	if strings.Contains(got, "BEGIN RSA PRIVATE KEY") {
		t.Errorf("PEM 头也应打码：%q", got)
	}
	if !strings.Contains(got, tail) {
		t.Errorf("块之后的余文应保留：%q", got)
	}
}
