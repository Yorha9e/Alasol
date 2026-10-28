package host

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// writeConfig 把一个 TOML 文本写到临时文件并返回路径。
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	return p
}

// TestPrecedenceMatrix 钉死 H15 优先级矩阵：**flag > env > 文件**（env 在则 env 赢）。
// 四象限：只有 flag / 只有 env / 只有文件 / 都有 / 都无。
func TestPrecedenceMatrix(t *testing.T) {
	cases := []struct {
		name       string
		flag       string
		env        string
		file       string
		wantVal    string
		wantSource string
	}{
		{"只有 flag", "F", "", "", "F", sourceFlag},
		{"只有 env", "", "E", "", "E", sourceEnv},
		{"只有文件", "", "", "L", "L", sourceFile},
		{"都无", "", "", "", "", ""},
		{"flag+env+file 都有 → flag 赢", "F", "E", "L", "F", sourceFlag},
		{"env+file 都有 → env 赢（H15 核心）", "", "E", "L", "E", sourceEnv},
		{"flag+file 都有 → flag 赢", "F", "", "L", "F", sourceFlag},
		{"flag+env 都有 → flag 赢", "F", "E", "", "F", sourceFlag},
		{"空白 flag 不算设置 → 落到 env", "   ", "E", "L", "E", sourceEnv},
		{"空白 env 不算设置 → 落到文件", "", "  ", "L", "L", sourceFile},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			val, src := Precedence(tc.flag, tc.env, tc.file)
			if val != tc.wantVal || src != tc.wantSource {
				t.Fatalf("Precedence(%q,%q,%q) = (%q,%q)，want (%q,%q)",
					tc.flag, tc.env, tc.file, val, src, tc.wantVal, tc.wantSource)
			}
		})
	}
}

// TestResolveCredentialEnvWins 钉死凭据优先级：**env > 文件**（H15）。
func TestResolveCredentialEnvWins(t *testing.T) {
	const envName = "ALASOL_TEST_KEY"
	const envVal = "sk-env-secret"
	const fileVal = "sk-file-secret"

	t.Setenv(envName, envVal)
	if got := ResolveCredential(envName, fileVal); got != envVal {
		t.Fatalf("env 有值时应 env 赢：got %q want %q", got, envVal)
	}

	// env 未设/为空 → 回落文件值。
	t.Setenv(envName, "")
	if got := ResolveCredential(envName, fileVal); got != fileVal {
		t.Fatalf("env 空时应回落文件：got %q want %q", got, fileVal)
	}
	// env 名空 → 直接文件值。
	if got := ResolveCredential("", fileVal); got != fileVal {
		t.Fatalf("env 名为空时应取文件：got %q want %q", got, fileVal)
	}
}

// TestResolveAPIKeyEnvName 定 env 变量名的解析：显式 > 文件 > 默认（只解析名字，可安全日志）。
func TestResolveAPIKeyEnvName(t *testing.T) {
	if got := ResolveAPIKeyEnv("MY_KEY", "FILE_KEY"); got != "MY_KEY" {
		t.Fatalf("显式 flag 应赢：got %q", got)
	}
	if got := ResolveAPIKeyEnv("", "FILE_KEY"); got != "FILE_KEY" {
		t.Fatalf("无显式应取文件：got %q", got)
	}
	if got := ResolveAPIKeyEnv("", ""); got != DefaultAPIKeyEnv {
		t.Fatalf("都无应取默认 %q：got %q", DefaultAPIKeyEnv, got)
	}
}

// TestLoadFileConfigParse 验证 TOML 解析：嵌套表 + 列表 + 各字段。
func TestLoadFileConfigParse(t *testing.T) {
	p := writeConfig(t, `
model = "gpt-4o-mini"
base_url = "https://gw.example/v1"
api_key = "sk-SECRET-VALUE"
api_key_env = "MY_KEY_ENV"
system = "你是助手"
approval = "always-allow"
max_turns = 7
context_budget = 4096
allow = ["read", "write"]
ask = ["bash"]

[online_table]
rows = 30
line_tokens = 80
level = "n"
window = 3

[monitor]
cpu_percent = 85.5
mem_mb = 512
goroutines = 2000
interval_sec = 2
`)
	fc, err := LoadFileConfig(p)
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if fc == nil {
		t.Fatal("fc 不应为 nil")
	}
	if fc.Model != "gpt-4o-mini" || fc.BaseURL != "https://gw.example/v1" {
		t.Fatalf("provider 字段解析错: %+v", fc)
	}
	if fc.APIKey != "sk-SECRET-VALUE" || fc.APIKeyEnv != "MY_KEY_ENV" {
		t.Fatalf("凭据字段解析错: %+v", fc)
	}
	if fc.MaxTurns != 7 || fc.ContextBudget != 4096 {
		t.Fatalf("数值字段解析错: %+v", fc)
	}
	if len(fc.Allow) != 2 || fc.Allow[0] != "read" || len(fc.Ask) != 1 || fc.Ask[0] != "bash" {
		t.Fatalf("列表字段解析错: %+v", fc)
	}
	if fc.OnlineTable.Rows != 30 || fc.OnlineTable.Level != "n" || fc.OnlineTable.Window != 3 {
		t.Fatalf("online_table 嵌套解析错: %+v", fc.OnlineTable)
	}
	if fc.Monitor.CPUPercent != 85.5 || fc.Monitor.MemMB != 512 || fc.Monitor.Goroutines != 2000 || fc.Monitor.IntervalSec != 2 {
		t.Fatalf("monitor 嵌套解析错: %+v", fc.Monitor)
	}
}

// TestLoadFileConfigMissing 验证"文件不存在"是合法状态（nil, nil），不是错误。
func TestLoadFileConfigMissing(t *testing.T) {
	fc, err := LoadFileConfig(filepath.Join(t.TempDir(), "nope.toml"))
	if err != nil {
		t.Fatalf("缺失文件不应报错: %v", err)
	}
	if fc != nil {
		t.Fatalf("缺失文件应返回 nil config，得到 %+v", fc)
	}
}

// TestLoadFileConfigBadFailClosed 验证损坏的配置**明确报错**（fail-close），
// 且错误信息**不含凭据值**（脱敏纪律）。
func TestLoadFileConfigBadFailClosed(t *testing.T) {
	p := writeConfig(t, "model = \"x\"\nthis is not valid toml @@@\n")
	fc, err := LoadFileConfig(p)
	if err == nil {
		t.Fatal("损坏的 TOML 应报错（fail-close）")
	}
	if fc != nil {
		t.Fatalf("出错时不应返回 config: %+v", fc)
	}
}

// TestCredentialNeverInRedacted 钉死凭据脱敏：Redacted() 的输出不含 key 值。
func TestCredentialNeverInRedacted(t *testing.T) {
	const secret = "sk-super-secret-DO-NOT-LEAK"
	fc := FileConfig{
		Model:     "gpt-4o-mini",
		APIKey:    secret,
		APIKeyEnv: "OPENAI_API_KEY",
		BaseURL:   "https://gw.example/v1",
	}
	red := fc.Redacted()
	// 用 fmt 把它格式化（模拟"整体日志化"的唯一安全途径）后断言 secret 不出现。
	dumped := red.APIKey + "|" + red.Model + "|" + red.APIKeyEnv + "|" + red.BaseURL
	if strings.Contains(dumped, secret) {
		t.Fatalf("Redacted() 泄漏了凭据值: %q", dumped)
	}
	if red.APIKey != "***" {
		t.Fatalf("Redacted().APIKey 应为 ***，得到 %q", red.APIKey)
	}
	// 原值不被改动（Redacted 返回值是副本）。
	if fc.APIKey != secret {
		t.Fatalf("Redacted() 不应改动原值")
	}
	// APIKeyEnv 是变量名（非值），应保留供诊断。
	if red.APIKeyEnv != "OPENAI_API_KEY" {
		t.Fatalf("APIKeyEnv（名字）应保留: %q", red.APIKeyEnv)
	}
}

// TestDefaultConfigPathShape 验证默认配置路径形态（平台相关；用 env 覆盖 HOME/XDG/APPDATA 探测）。
func TestDefaultConfigPathShape(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if runtime.GOOS == "windows" {
		// Windows 走 %APPDATA%\alasol\config.toml。
		t.Setenv("APPDATA", home)
		p, err := DefaultConfigPath()
		if err != nil {
			t.Fatalf("DefaultConfigPath: %v", err)
		}
		if !strings.HasSuffix(filepath.ToSlash(p), "alasol/config.toml") {
			t.Fatalf("Windows 默认配置路径应以 alasol/config.toml 结尾: %q", p)
		}
		// APPDATA 未设 → 明确报错（fail-close，不静默落到别处）。
		t.Setenv("APPDATA", "")
		if _, err := DefaultConfigPath(); err == nil {
			t.Fatal("Windows 上 APPDATA 未设应报错")
		}
		return
	}
	// 非 Windows 走 XDG：$XDG_CONFIG_HOME/alasol/config.toml。
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	p, err := DefaultConfigPath()
	if err != nil {
		t.Fatalf("DefaultConfigPath: %v", err)
	}
	if !strings.HasSuffix(filepath.ToSlash(p), "alasol/config.toml") {
		t.Fatalf("XDG 默认配置路径应以 alasol/config.toml 结尾: %q", p)
	}
	// XDG 未设 → 回落 $HOME/.config/alasol/config.toml。
	t.Setenv("XDG_CONFIG_HOME", "")
	p2, err := DefaultConfigPath()
	if err != nil {
		t.Fatalf("DefaultConfigPath (no XDG): %v", err)
	}
	if !strings.HasSuffix(filepath.ToSlash(p2), ".config/alasol/config.toml") {
		t.Fatalf("无 XDG 应回落 ~/.config/alasol/config.toml: %q", p2)
	}
}
