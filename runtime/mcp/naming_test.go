package mcp

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestToolNamePrefix：注册名一律带 mcp__<server>__ 前缀，wire 名不受影响。
func TestToolNamePrefix(t *testing.T) {
	got, err := RegisterableName("fs", "read_file")
	if err != nil {
		t.Fatalf("RegisterableName: %v", err)
	}
	if want := "mcp__fs__read_file"; got != want {
		t.Fatalf("注册名 = %q，期望 %q", got, want)
	}
	// 前缀常量与 Separator 是唯一口径（D21）：这里钉死拼法。
	if want := Prefix + Separator + "fs" + Separator + "read_file"; got != want {
		t.Fatalf("注册名 = %q，期望按常量拼出 %q", got, want)
	}
}

// TestRegisterableNameNormalizesDot：`.` 归一化成 `_`（provider 函数名里不合法），
// 且归一化后仍然指向同一个 wire 工具（executor 持有原样 wire 名）。
func TestRegisterableNameNormalizesDot(t *testing.T) {
	got, err := RegisterableName("gh", "create.issue")
	if err != nil {
		t.Fatalf("RegisterableName: %v", err)
	}
	if want := "mcp__gh__create_issue"; got != want {
		t.Fatalf("注册名 = %q，期望 %q", got, want)
	}
}

// TestRegisterableNameRejectsBadNames：越界字符/空名/超长一律拒（fail-close），
// 不静默丢字符——静默丢会让两个工具名变成一个（静默遮蔽比报错糟）。
func TestRegisterableNameRejectsBadNames(t *testing.T) {
	cases := []struct {
		name, server, wire, want string
	}{
		{"空工具名", "fs", "", "为空"},
		{"空白工具名", "fs", "   ", "为空"},
		{"含空格", "fs", "read file", "非法字符"},
		{"含斜杠", "fs", "read/file", "非法字符"},
		{"含中文", "fs", "读取", "非法字符"},
		{"超长工具名", "fs", strings.Repeat("a", MaxWireToolNameLen+1), "超过"},
		{"空 server 名", "", "x", "server 名不能为空"},
		{"server 名带空白", " fs", "x", "首尾空白"},
		{"server 名含分隔符", "a__b", "x", "分隔符"},
		{"server 名首字符为连字符", "-fs", "x", "首字符"},
		{"server 名超长", strings.Repeat("s", MaxServerNameLen+1), "x", "超过"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := RegisterableName(c.server, c.wire)
			mustContain(t, err, c.want)
		})
	}
}

// TestRegisterableNameLengthCap：注册名（含前缀）超上限即拒，且错误里带原名。
func TestRegisterableNameLengthCap(t *testing.T) {
	long := strings.Repeat("t", MaxToolNameLen)
	_, err := RegisterableName("s", long)
	mustContain(t, err, "超过")
	mustContain(t, err, "注册名")
}

// TestMaxToolNameLenMatchesProviderLimit：注册名上限必须 **≤ provider 的函数名
// 上限**（OpenAI 是 64 字符）。
//
// 为什么这条是硬约束：注册名会原样进 provider 的函数名，超限的后果不是
// "这个工具不能用"，而是**整轮请求被 provider 拒**——一个外部 server 声明
// 一个长工具名就能把整轮对话打穿（外部输入打穿内部请求，最坏的一类面）。
// 因此上限取 provider 的约束值，超限在装配期 fail-close。
func TestMaxToolNameLenMatchesProviderLimit(t *testing.T) {
	const providerFuncNameLimit = 64
	if MaxToolNameLen > providerFuncNameLimit {
		t.Fatalf("注册名上限 %d 超过 provider 函数名上限 %d（超限会让整轮请求被拒）",
			MaxToolNameLen, providerFuncNameLimit)
	}
}

// TestServerNameCaseSensitive：server 名大小写**不折叠**（陷阱 17）——
// "Fs" 与 "fs" 是两个不同 server，工具名前缀不同，不会互相遮蔽。
func TestServerNameCaseSensitive(t *testing.T) {
	a, err := RegisterableName("Fs", "x")
	if err != nil {
		t.Fatalf("RegisterableName(Fs): %v", err)
	}
	b, err := RegisterableName("fs", "x")
	if err != nil {
		t.Fatalf("RegisterableName(fs): %v", err)
	}
	if a == b {
		t.Fatalf("大小写不同的 server 名产生了同一个工具名 %q（陷阱 17：不许折叠大小写）", a)
	}
	if a != "mcp__Fs__x" || b != "mcp__fs__x" {
		t.Fatalf("大小写未原样保留：%q / %q", a, b)
	}
}

// TestValidateRejectsDuplicateServerName：server 名字节相同即拒（前缀唯一性来源）。
// 大小写不同的重复**不拒**（陷阱 17；且策略是精确匹配，不存在"以为授权了其实没有"的反向意外）。
func TestValidateRejectsDuplicateServerName(t *testing.T) {
	dup := Config{Servers: []ServerConfig{
		{Name: "fs", Command: "x", Enabled: true},
		{Name: "fs", Command: "y", Enabled: true},
	}}
	mustContain(t, dup.Validate(), "重复声明")

	cased := Config{Servers: []ServerConfig{
		{Name: "fs", Command: "x", Enabled: true},
		{Name: "Fs", Command: "y", Enabled: true},
	}}
	if err := cased.Validate(); err != nil {
		t.Fatalf("大小写不同的 server 名应被接受（陷阱 17）：%v", err)
	}
}

// TestValidateTransport：stdio 必须给 command、http 必须给 url、sse 明确不接线。
func TestValidateTransport(t *testing.T) {
	cases := []struct {
		name string
		cfg  ServerConfig
		want string
	}{
		{"stdio 缺 command", ServerConfig{Name: "a"}, "command"},
		{"stdio 带 url", ServerConfig{Name: "a", Command: "x", URL: "http://h"}, "不需要 url"},
		{"http 缺 url", ServerConfig{Name: "a", Transport: TransportHTTP}, "url"},
		{"http 带 command", ServerConfig{Name: "a", Transport: TransportHTTP, URL: "http://h", Command: "x"}, "不需要 command"},
		{"sse 未接线", ServerConfig{Name: "a", Transport: TransportSSE, URL: "http://h"}, "未接线"},
		{"未知传输", ServerConfig{Name: "a", Transport: "carrier-pigeon"}, "不支持"},
		{"负超时", ServerConfig{Name: "a", Command: "x", TimeoutMs: -1}, "不能为负"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := Config{Servers: []ServerConfig{c.cfg}}
			mustContain(t, cfg.Validate(), c.want)
		})
	}
	// 合法形态：stdio 只给 command；http 只给 url。
	ok := Config{Servers: []ServerConfig{
		{Name: "a", Command: "x", Enabled: true},
		{Name: "b", Transport: TransportHTTP, URL: "http://127.0.0.1:1/mcp", Enabled: true},
	}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("合法配置被拒: %v", err)
	}
	// Transport 空值归一化成 stdio。
	if got := ok.Servers[0].Transport; got != TransportStdio {
		t.Fatalf("空 transport 未归一化成 stdio（得到 %q）", got)
	}
}

// TestRedactedHidesCredentials：Redacted() 打码 Env/Headers 的值，保留键名与
// EnvFrom 的变量名（与 host.FileConfig.Redacted 同口径）。
func TestRedactedHidesCredentials(t *testing.T) {
	cfg := Config{Servers: []ServerConfig{{
		Name: "a", Command: "x",
		Env:     map[string]string{"TOKEN": "sk-supersecret"},
		Headers: map[string]string{"Authorization": "Bearer sk-supersecret"},
		EnvFrom: []string{"MY_TOKEN"},
		Args:    []string{"-y", "pkg"},
	}}}
	red := cfg.Redacted()
	s := red.Servers[0]
	if s.Env["TOKEN"] != "***" {
		t.Fatalf("Env 值未打码：%q", s.Env["TOKEN"])
	}
	if s.Headers["Authorization"] != "***" {
		t.Fatalf("Headers 值未打码：%q", s.Headers["Authorization"])
	}
	if len(s.EnvFrom) != 1 || s.EnvFrom[0] != "MY_TOKEN" {
		t.Fatalf("EnvFrom（变量名，非值）应原样保留：%v", s.EnvFrom)
	}
	// 原配置不被改写（Redacted 返回副本）。
	if cfg.Servers[0].Env["TOKEN"] != "sk-supersecret" {
		t.Fatalf("Redacted 改到了原配置")
	}
	// 打码后的结构可以安全日志化：整份序列化不含凭据值。
	if strings.Contains(mustJSON(red), "sk-supersecret") {
		t.Fatalf("Redacted() 输出仍含凭据值")
	}
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}
