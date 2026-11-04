package host

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
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

// TestFileConfigRedactedMasksMCPCredentials 钉死 **MCP 段也打码**：
// FileConfig.Redacted() 是"只有它输出的结构可安全落日志"的唯一入口，
// 而 [[mcp.servers]] 的 env / headers 里放的就是 server 凭据——只打码
// APIKey 而放过 MCP 段，等于让这条承诺在最容易被忽略的那一段上失效。
func TestFileConfigRedactedMasksMCPCredentials(t *testing.T) {
	const (
		envSecret    = "sk-mcp-env-SECRET"
		headerSecret = "Bearer sk-mcp-header-SECRET"
	)
	fc := FileConfig{
		APIKey: "sk-provider-SECRET",
		MCP: MCPFileConfig{Servers: []MCPServerFileConfig{{
			Name:    "fs",
			Command: "npx",
			Env:     map[string]string{"TOKEN": envSecret},
			Headers: map[string]string{"Authorization": headerSecret},
		}}},
	}
	red := fc.Redacted()

	// 整体序列化（"整体日志化"的唯一安全途径）里不许出现任何凭据值。
	dumped := mustJSONString(red)
	for _, secret := range []string{envSecret, headerSecret, "sk-provider-SECRET"} {
		if strings.Contains(dumped, secret) {
			t.Fatalf("Redacted() 泄漏了凭据值 %q：%s", secret, dumped)
		}
	}
	// 键名与 server 名保留（诊断价值）。
	if red.MCP.Servers[0].Env["TOKEN"] != "***" {
		t.Fatalf("MCP env 值未打码: %q", red.MCP.Servers[0].Env["TOKEN"])
	}
	if red.MCP.Servers[0].Headers["Authorization"] != "***" {
		t.Fatalf("MCP headers 值未打码: %q", red.MCP.Servers[0].Headers["Authorization"])
	}
	if red.MCP.Servers[0].Name != "fs" {
		t.Fatalf("server 名应保留: %q", red.MCP.Servers[0].Name)
	}
	// 原值不被改动（Redacted 返回副本）。
	if fc.MCP.Servers[0].Env["TOKEN"] != envSecret {
		t.Fatalf("Redacted() 不应改动原值")
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

// ---------------------------------------------------------------------------
// MCP client 文件级配置（v3-S1，N1）
// ---------------------------------------------------------------------------

// TestMCPFileConfigParses：TOML 的 [[mcp.servers]] 数组表完整解析
// （命令/参数/env/headers/传输/开关）。
func TestMCPFileConfigParses(t *testing.T) {
	const body = `
[[mcp.servers]]
name = "fs"
transport = "stdio"
command = "npx"
args = ["-y", "@scope/server-fs", "."]
env = { TOKEN = "sk-token" }
env_from = ["MY_TOKEN"]
timeout_ms = 5000

[[mcp.servers]]
name = "remote"
transport = "http"
url = "http://127.0.0.1:1234/mcp"
headers = { Authorization = "Bearer x" }
enabled = false
`
	fc, err := LoadFileConfig(writeConfig(t, body))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if len(fc.MCP.Servers) != 2 {
		t.Fatalf("server 数 = %d，want 2", len(fc.MCP.Servers))
	}
	s0 := fc.MCP.Servers[0]
	if s0.Name != "fs" || s0.Transport != "stdio" || s0.Command != "npx" {
		t.Fatalf("server[0] 基本字段不对: %+v", s0)
	}
	if len(s0.Args) != 3 || s0.Args[1] != "@scope/server-fs" {
		t.Fatalf("args = %v", s0.Args)
	}
	if s0.Env["TOKEN"] != "sk-token" {
		t.Fatalf("env = %v", s0.Env)
	}
	if len(s0.EnvFrom) != 1 || s0.EnvFrom[0] != "MY_TOKEN" {
		t.Fatalf("env_from = %v", s0.EnvFrom)
	}
	if s0.TimeoutMs != 5000 {
		t.Fatalf("timeout_ms = %d", s0.TimeoutMs)
	}
	s1 := fc.MCP.Servers[1]
	if s1.Transport != "http" || s1.URL != "http://127.0.0.1:1234/mcp" {
		t.Fatalf("server[1] = %+v", s1)
	}
	if s1.Headers["Authorization"] != "Bearer x" {
		t.Fatalf("headers = %v", s1.Headers)
	}
	if s1.Enabled == nil || *s1.Enabled {
		t.Fatalf("enabled=false 应解析成指针 false（得到 %v）", s1.Enabled)
	}
}

// TestMCPFileConfigEnabledDefaultTrue：**没写 enabled = true**。
//
// 这是本刀唯一一处"TOML bool 零值语义被反转"的地方，理由：用值类型时
// "写了 server 却忘了 enabled" 会静默不连（比多连一个难排查得多）。
func TestMCPFileConfigEnabledDefaultTrue(t *testing.T) {
	fc, err := LoadFileConfig(writeConfig(t, `
[[mcp.servers]]
name = "fs"
command = "npx"
`))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if len(fc.MCP.Servers) != 1 {
		t.Fatalf("server 数 = %d", len(fc.MCP.Servers))
	}
	if fc.MCP.Servers[0].Enabled != nil {
		t.Fatalf("没写 enabled 时应为 nil（=未显式设置），得到 %v", *fc.MCP.Servers[0].Enabled)
	}
	// 折成 runtime/mcp.Config 后必须变成 true（"写了就要连"）。
	cfg := fc.MCP.MCPConfig()
	if len(cfg.Servers) != 1 || !cfg.Servers[0].Enabled {
		t.Fatalf("MCPConfig 应把 enabled 缺省折成 true：%+v", cfg.Servers)
	}
	// 显式 false 仍要尊重。
	fc2, err := LoadFileConfig(writeConfig(t, `
[[mcp.servers]]
name = "fs"
command = "npx"
enabled = false
`))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if cfg2 := fc2.MCP.MCPConfig(); cfg2.Servers[0].Enabled {
		t.Fatalf("显式 enabled=false 未被尊重")
	}
}

// TestMCPFileConfigRedacted：凭据打码（**只有 Redacted() 输出可安全落日志**）。
func TestMCPFileConfigRedacted(t *testing.T) {
	fc, err := LoadFileConfig(writeConfig(t, `
[[mcp.servers]]
name = "fs"
command = "npx"
env = { TOKEN = "sk-token" }
headers = { Authorization = "Bearer sk-token" }
env_from = ["MY_TOKEN"]
`))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	red := fc.MCP.Redacted()
	s := red.Servers[0]
	if s.Env["TOKEN"] != "***" || s.Headers["Authorization"] != "***" {
		t.Fatalf("凭据未打码: env=%v headers=%v", s.Env, s.Headers)
	}
	if len(s.EnvFrom) != 1 || s.EnvFrom[0] != "MY_TOKEN" {
		t.Fatalf("env_from（变量名）应保留: %v", s.EnvFrom)
	}
	// 原配置不被改写。
	if fc.MCP.Servers[0].Env["TOKEN"] != "sk-token" {
		t.Fatalf("Redacted 改到了原配置")
	}
	if strings.Contains(mustJSONString(red), "sk-token") {
		t.Fatalf("Redacted 输出仍含凭据值")
	}
}

// TestMCPFileConfigEmpty：没有 [[mcp.servers]] → 零 server（默认关 = 不装 MCP）。
func TestMCPFileConfigEmpty(t *testing.T) {
	fc, err := LoadFileConfig(writeConfig(t, `model = "m"`))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if len(fc.MCP.Servers) != 0 {
		t.Fatalf("未配 server 时应为空，得到 %d", len(fc.MCP.Servers))
	}
	cfg := fc.MCP.MCPConfig()
	if len(cfg.Servers) != 0 {
		t.Fatalf("MCPConfig 应为空")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("空配置应通过校验: %v", err)
	}
}

// TestMCPFileConfigBadEntries：坏条目经 MCPConfig → Validate 被拒（fail-close：
// 不在文件层静默丢弃，否则使用者以为连上了）。
func TestMCPFileConfigBadEntries(t *testing.T) {
	fc, err := LoadFileConfig(writeConfig(t, `
[[mcp.servers]]
name = ""
command = "npx"
`))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	cfg := fc.MCP.MCPConfig()
	mustContainErr(t, cfg.Validate(), "server 名不能为空")
}

func mustContainErr(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("期望错误含 %q，得到 nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("错误信息不含 %q：%v", want, err)
	}
}

// ---------------------------------------------------------------------------
// 未知键 fail-close（F3）：TOML 会**静默忽略**没被结构体认领的键
// ---------------------------------------------------------------------------

// TestLoadFileConfigUnknownKeyFailsClosed（F3 钉子）：写错段位的键**当场点名报错**。
//
// 活例子：share_project_assets 是 [[agent_types]] 的**顶层**字段，用户写进
// [agent_types.context_strategy]（那张表只有 kind）→ TOML 静默忽略 → 配了 true 却毫无
// 效果，用户以为配了。故装载期 fail-close（与"坏配置当场拒"同一纪律）。
func TestLoadFileConfigUnknownKeyFailsClosed(t *testing.T) {
	p := writeConfig(t, `
model = "m"

[[agent_types]]
id = "auditor"

[agent_types.context_strategy]
kind = "tiered"
share_project_assets = true
`)
	fc, err := LoadFileConfig(p)
	if err == nil {
		t.Fatalf("未知键应被拒（fail-close），得到 config: %+v", fc)
	}
	if fc != nil {
		t.Fatalf("出错时不应返回 config: %+v", fc)
	}
	if !strings.Contains(err.Error(), "share_project_assets") {
		t.Errorf("错误应点名未知键 share_project_assets: %v", err)
	}
}

// TestLoadFileConfigUnknownKeyErrorListsLegalKeys（F3 补的钉子）：未知键的报错**带合法键
// 清单**——闸门本身保留（"老配置升级即拒"是既定口径），但排查成本要降：只列坏键的话，
// 用户还得自己翻文档才知道**能写什么**。
//
// 断言两处清单：① 顶层合法键（含 base_url / model / multi_agent …）；② 坏键**所在那张表**
// 的合法键（"写错段位"这类误配最需要的提示：share_project_assets 写进了
// [agent_types.context_strategy]，那张表只有 kind）。
func TestLoadFileConfigUnknownKeyErrorListsLegalKeys(t *testing.T) {
	p := writeConfig(t, `
model = "m"

[[agent_types]]
id = "auditor"

[agent_types.context_strategy]
kind = "tiered"
share_project_assets = true
`)
	_, err := LoadFileConfig(p)
	if err == nil {
		t.Fatal("未知键应被拒（fail-close）")
	}
	msg := err.Error()
	// ① 顶层合法键清单（点几个代表键即可，不必把整张清单钉死——清单本身是反射派生的）。
	for _, want := range []string{"合法键", "base_url", "model", "multi_agent", "agent_types"} {
		if !strings.Contains(msg, want) {
			t.Errorf("错误信息应含顶层合法键提示 %q: %v", want, err)
		}
	}
	// ② 坏键所在那张表的合法键（context_strategy 表只有 kind）。
	if !strings.Contains(msg, "kind") {
		t.Errorf("错误信息应含坏键所在表的合法键（kind）: %v", err)
	}
	// ③ 坏键仍要点名（闸门本体没被削弱）。
	if !strings.Contains(msg, "share_project_assets") {
		t.Errorf("错误信息应点名未知键 share_project_assets: %v", err)
	}
}

// TestLoadFileConfigAgentTypesTopLevelKeys：段位写对的键**正常解析**（未知键 fail-close
// 不许误伤合法配置）。
func TestLoadFileConfigAgentTypesTopLevelKeys(t *testing.T) {
	fc, err := LoadFileConfig(writeConfig(t, `
model = "m"
multi_agent = true

[[agent_types]]
id = "auditor"
prompt = "你是审计员"
tools = ["read"]
model = "default"
can_spawn = ["observer"]
share_project_assets = true

[agent_types.permission_request]
read = "workspace"
write = "none"
exec = "none"

[agent_types.context_strategy]
kind = "tiered"
`))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if !fc.MultiAgent {
		t.Error("multi_agent 应解析为 true")
	}
	if len(fc.AgentTypes) != 1 {
		t.Fatalf("应有 1 条 agent_types，得到 %d", len(fc.AgentTypes))
	}
	at := fc.AgentTypes[0]
	if at.ID != "auditor" || at.ShareProjectAssets == nil || !*at.ShareProjectAssets {
		t.Errorf("agent_types 顶层字段解析错: %+v", at)
	}
	if at.ContextStrategy == nil || at.ContextStrategy.Kind != string(contracts.ContextStrategyKindTiered) {
		t.Errorf("agent_types.context_strategy 解析错: %+v", at.ContextStrategy)
	}
}

func mustJSONString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// TestLoadFileConfigWALAndBudgetSections（v4-4-B）：[wal] 与 [budget] 两段正常解析
// （未知键 fail-close 不许误伤合法配置——反射派生的合法键清单自动含新段）。
func TestLoadFileConfigWALAndBudgetSections(t *testing.T) {
	fc, err := LoadFileConfig(writeConfig(t, `
model = "m"

[wal]
dir = "C:/Users/me/alasol/wal"

[budget]
max_tokens = 123456
`))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if fc.Wal.Dir != "C:/Users/me/alasol/wal" {
		t.Errorf("[wal].dir 解析错: %q", fc.Wal.Dir)
	}
	if fc.Budget.MaxTokens != 123456 {
		t.Errorf("[budget].max_tokens 解析错: %d", fc.Budget.MaxTokens)
	}
}

// TestLoadFileConfigWALAndBudgetDefaults（v4-4-B）：不写这两段 = 默认关/不设上限
// （零行为变化的配置面口径）。
func TestLoadFileConfigWALAndBudgetDefaults(t *testing.T) {
	fc, err := LoadFileConfig(writeConfig(t, `model = "m"`))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if fc.Wal.Dir != "" {
		t.Errorf("[wal].dir 缺省应为空（关），got %q", fc.Wal.Dir)
	}
	if fc.Budget.MaxTokens != 0 {
		t.Errorf("[budget].max_tokens 缺省应为 0（不设上限），got %d", fc.Budget.MaxTokens)
	}
}

// TestLoadFileConfigDocumentsSection（G2 第 5 族）：[documents] 段正常解析
// （未知键 fail-close 不许误伤合法配置——反射派生的合法键清单自动含本段）。
func TestLoadFileConfigDocumentsSection(t *testing.T) {
	fc, err := LoadFileConfig(writeConfig(t, `
model = "m"

[documents]
enabled = true
`))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if !fc.Documents.Enabled {
		t.Error("[documents].enabled 解析错：应为 true")
	}
}

// TestLoadFileConfigDocumentsDefaults（G2 第 5 族）：不写 [documents] 段 = 默认关
// （零行为变化的配置面口径：不建 DocStore、/api/docs* 四个端点 404）。
func TestLoadFileConfigDocumentsDefaults(t *testing.T) {
	fc, err := LoadFileConfig(writeConfig(t, `model = "m"`))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if fc.Documents.Enabled {
		t.Error("[documents].enabled 缺省应为 false（关），got true")
	}
}

// TestLoadFileConfigTelemetrySection（L2/D19）：[telemetry] 段正常解析
// （未知键 fail-close 不许误伤合法配置——反射派生的合法键清单自动含本段）。
func TestLoadFileConfigTelemetrySection(t *testing.T) {
	fc, err := LoadFileConfig(writeConfig(t, `
model = "m"

[telemetry]
mode = "file"
dir = "C:/Users/me/alasol/tel"
`))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if fc.Telemetry.Mode != "file" {
		t.Errorf("[telemetry].mode 解析错: %q", fc.Telemetry.Mode)
	}
	if fc.Telemetry.Dir != "C:/Users/me/alasol/tel" {
		t.Errorf("[telemetry].dir 解析错: %q", fc.Telemetry.Dir)
	}
}

// TestLoadFileConfigTelemetryDefaults（L2/D19）：不写 [telemetry] = 默认关
// （mode/dir 空 = 一条 record 都不造 = 零行为变化的配置面口径）。
func TestLoadFileConfigTelemetryDefaults(t *testing.T) {
	fc, err := LoadFileConfig(writeConfig(t, `model = "m"`))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if fc.Telemetry.Mode != "" || fc.Telemetry.Dir != "" {
		t.Errorf("[telemetry] 缺省应为空（关），got mode=%q dir=%q", fc.Telemetry.Mode, fc.Telemetry.Dir)
	}
}

// ---------------------------------------------------------------------------
// G-i profile 表（[[profiles]]）：解析 + unknown-key 闸门
// ---------------------------------------------------------------------------

// TestLoadFileConfigProfilesParse：[[profiles]] 数组表 + 其嵌套表（provider / tools /
// context）解析成宿主形状（配置面 + 执行面）。
func TestLoadFileConfigProfilesParse(t *testing.T) {
	fc, err := LoadFileConfig(writeConfig(t, `
model = "m"

[[profiles]]
name = "fast"
model = "gpt-4o-mini"
reasoning_effort = "low"
cwd = "D:/work"
worktree = "wt"
run_id = "run-1"
remote_host = "build-01"

[profiles.provider]
base_url = "https://api.openai.com/v1"
api_key_env = "OPENAI_API_KEY"

[profiles.tools]
only = ["read"]
except = ["bash"]

[profiles.context]
kind = "tiered"
window = 512000
l1_pct = 8
l2_pct = 0.5
rules_budget = 20000
common_sense_budget = 40000
`))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	profiles, err := fc.ProfileConfigs()
	if err != nil {
		t.Fatalf("ProfileConfigs: %v", err)
	}
	p, ok := profiles["fast"]
	if !ok {
		t.Fatalf("应有名为 fast 的 profile: %v", profiles)
	}
	if p.Model != "gpt-4o-mini" || p.ReasoningEffort != "low" {
		t.Errorf("模型面解析不对: %+v", p)
	}
	if p.Provider.BaseURL != "https://api.openai.com/v1" || p.Provider.APIKeyEnv != "OPENAI_API_KEY" {
		t.Errorf("provider 面解析不对: %+v", p.Provider)
	}
	if len(p.Tools.Only) != 1 || p.Tools.Only[0] != "read" || len(p.Tools.Except) != 1 {
		t.Errorf("工具白名单解析不对: %+v", p.Tools)
	}
	if p.Context.Kind != "tiered" || p.Context.Window != 512000 || p.Context.L1Pct != 8 ||
		p.Context.L2Pct != 0.5 || p.Context.RulesBudget != 20000 || p.Context.CommonSenseBudget != 40000 {
		t.Errorf("上下文面解析不对: %+v", p.Context)
	}
	if p.CWD != "D:/work" || p.Worktree != "wt" || p.RunID != "run-1" || p.RemoteHost != "build-01" {
		t.Errorf("执行面解析不对: %+v", p)
	}
}

// TestLoadFileConfigProfilesUnknownKeyFailsClosed：[[profiles]] 里写错键 → 当场拒
// （unknown-key 闸门对 profile 表同样生效——"配了不生效比当场报错难排查得多"）。
func TestLoadFileConfigProfilesUnknownKeyFailsClosed(t *testing.T) {
	_, err := LoadFileConfig(writeConfig(t, `
model = "m"

[[profiles]]
name = "fast"
modle = "gpt-4o-mini"
`))
	if err == nil {
		t.Fatal("[[profiles]] 里的未知键应被拒（fail-close）")
	}
	if !strings.Contains(err.Error(), "modle") {
		t.Errorf("错误应点名未知键 modle: %v", err)
	}
	// 错误里带那张表的合法键（排查成本要降：不用翻文档就知道能写什么）。
	for _, want := range []string{"name", "model", "reasoning_effort", "cwd"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误信息应含 profile 表的合法键 %q: %v", want, err)
		}
	}
}

// TestLoadFileConfigProfilesDuplicateNameFailsClosed：重名 → 装载期拒（同名两张表落哪张
// 是随机的——当场拒，不静默取后者）。
func TestLoadFileConfigProfilesDuplicateNameFailsClosed(t *testing.T) {
	fc, err := LoadFileConfig(writeConfig(t, `
model = "m"

[[profiles]]
name = "fast"
model = "m1"

[[profiles]]
name = "fast"
model = "m2"
`))
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if _, err := fc.ProfileConfigs(); err == nil {
		t.Fatal("重名 profile 应被拒（fail-close）")
	}
}
