package main

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/mcp"
)

// buildConfigFrom 是测试辅助：注册 flag → parse 给定 argv → buildConfig。
func buildConfigFrom(t *testing.T, args ...string) (host.Config, error) {
	t.Helper()
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	f := registerHubFlags(fs)
	f.parse(fs, args)
	return f.config()
}

// TestBuildConfigModelPrecedence 钉死 model 的 flag>env>文件 优先级 + 双空 fail-close。
func TestBuildConfigModelPrecedence(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(cfgFile, []byte("model = \"file-model\"\nbase_url = \"https://file.example/v1\"\n"), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}

	// ① 文件有 model、无 flag 无 env → 用文件。
	hc, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("① 意外错误: %v", err)
	}
	if hc.Model != "file-model" {
		t.Fatalf("① 文件 model 应生效: %q", hc.Model)
	}
	if hc.BaseURL != "https://file.example/v1" {
		t.Fatalf("① 文件 base_url 应生效: %q", hc.BaseURL)
	}

	// ② env 覆盖文件（H15 核心：env > 文件）。
	t.Setenv("ALASOL_MODEL", "env-model")
	hc2, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("② 意外错误: %v", err)
	}
	if hc2.Model != "env-model" {
		t.Fatalf("② env 应覆盖文件: %q", hc2.Model)
	}

	// ③ flag 覆盖 env + 文件。
	hc3, err := buildConfigFrom(t, "-config", cfgFile, "-model", "flag-model")
	if err != nil {
		t.Fatalf("③ 意外错误: %v", err)
	}
	if hc3.Model != "flag-model" {
		t.Fatalf("③ flag 应覆盖 env+文件: %q", hc3.Model)
	}

	// ④ 都无 → fail-close 报错（model 空）。
	t.Setenv("ALASOL_MODEL", "")
	_, err4 := buildConfigFrom(t, "-config", filepath.Join(t.TempDir(), "empty.toml"))
	if err4 == nil {
		t.Fatal("④ 都无 model 应报错（fail-close）")
	}
	if !strings.Contains(err4.Error(), "缺模型名") {
		t.Fatalf("④ 错误信息应提示缺模型名: %v", err4)
	}
}

// TestBuildConfigCredentialPrecedence 钉死凭据 env>文件 + 凭据不残留。
func TestBuildConfigCredentialPrecedence(t *testing.T) {
	const fileKey = "sk-file-secret"
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	body := "model=\"m\"\napi_key=\"" + fileKey + "\"\napi_key_env=\"ALASOL_CFG_KEY\"\n"
	if err := os.WriteFile(cfgFile, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}

	// ① 只有文件凭据（env 未设）→ 用文件值。
	t.Setenv("ALASOL_CFG_KEY", "")
	hc, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("① 意外错误: %v", err)
	}
	if hc.APIKey != fileKey {
		t.Fatalf("① 文件 api_key 应生效: %q", hc.APIKey)
	}

	// ② env 有值 → env 赢，文件旧值不得残留。
	t.Setenv("ALASOL_CFG_KEY", "sk-env-secret")
	hc2, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("② 意外错误: %v", err)
	}
	if hc2.APIKey != "sk-env-secret" {
		t.Fatalf("② env 应覆盖文件凭据: %q", hc2.APIKey)
	}
	if strings.Contains(hc2.APIKey, fileKey) {
		t.Fatalf("② env 赢时文件值不得残留: %q", hc2.APIKey)
	}
}

// TestBuildConfigBadFileFailClosed：-config 指向损坏文件 → 明确报错（fail-close）。
func TestBuildConfigBadFileFailClosed(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "bad.toml")
	if err := os.WriteFile(cfgFile, []byte("model = \"x\"\n@@@ not toml\n"), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	_, err := buildConfigFrom(t, "-config", cfgFile)
	if err == nil {
		t.Fatal("损坏的配置文件应报错（fail-close）")
	}
}

// TestBuildConfigContextStrategyPrecedence 钉死上下文策略参数面的优先级
// （F30/F10）：显式 flag > 配置文件 > 框架默认；kind 与 window/l1_pct/l2_pct 同一口径。
func TestBuildConfigContextStrategyPrecedence(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	body := "model = \"m\"\n" +
		"[context_strategy]\n" +
		"kind = \"tiered\"\n" +
		"window = 300000\n" +
		"l1_pct = 12.0\n" +
		"l2_pct = 2.0\n"
	if err := os.WriteFile(cfgFile, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}

	// ① 只有文件 → 用文件的 kind 与参数。
	hc, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("① 意外错误: %v", err)
	}
	if hc.ContextStrategy != "tiered" {
		t.Fatalf("① 文件的 context_strategy.kind 应生效: %q", hc.ContextStrategy)
	}
	if hc.ContextStrategyWindow != 300000 {
		t.Fatalf("① 文件 window 应生效: %d", hc.ContextStrategyWindow)
	}
	if hc.ContextStrategyL1Pct != 12.0 || hc.ContextStrategyL2Pct != 2.0 {
		t.Fatalf("① 文件 l1/l2 百分比应生效: %v / %v", hc.ContextStrategyL1Pct, hc.ContextStrategyL2Pct)
	}

	// ② 显式 flag 覆盖文件（flag > 文件，F30）。
	hc2, err := buildConfigFrom(t, "-config", cfgFile,
		"-context-strategy", "tiered",
		"-context-window", "512000",
		"-context-l1-pct", "8",
		"-context-l2-pct", "0.5",
	)
	if err != nil {
		t.Fatalf("② 意外错误: %v", err)
	}
	if hc2.ContextStrategy != "tiered" {
		t.Fatalf("② flag 的 kind 应生效: %q", hc2.ContextStrategy)
	}
	if hc2.ContextStrategyWindow != 512000 {
		t.Fatalf("② flag window 应覆盖文件: %d", hc2.ContextStrategyWindow)
	}
	if hc2.ContextStrategyL1Pct != 8 || hc2.ContextStrategyL2Pct != 0.5 {
		t.Fatalf("② flag l1/l2 百分比应覆盖文件: %v / %v", hc2.ContextStrategyL1Pct, hc2.ContextStrategyL2Pct)
	}

	// ③ flag 显式给空 kind → 退回 append（显式 flag 高于文件）。
	hc3, err := buildConfigFrom(t, "-config", cfgFile, "-context-strategy", "")
	if err != nil {
		t.Fatalf("③ 意外错误: %v", err)
	}
	if hc3.ContextStrategy != "" {
		t.Fatalf("③ 显式空 flag 应退回 append: %q", hc3.ContextStrategy)
	}
}

// TestBuildConfigContextRulesFromFile：项目规则文本走配置文件面（稳定前缀的注入源）。
func TestBuildConfigContextRulesFromFile(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	body := "model = \"m\"\ncontext_rules = \"[项目规则] 遵循 AGENTS.md\"\n"
	if err := os.WriteFile(cfgFile, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	hc, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("意外错误: %v", err)
	}
	if !strings.Contains(hc.ContextRules, "遵循 AGENTS.md") {
		t.Fatalf("文件的 context_rules 应生效: %q", hc.ContextRules)
	}
}

// TestBuildConfigContextInjectionBudgets：注入区上限（规则/常识区）走配置文件面
// （不可压缩的注入内容必须有上限，否则一条超长规则就能顶穿窗口）。
func TestBuildConfigContextInjectionBudgets(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	body := "model = \"m\"\n" +
		"[context_strategy]\n" +
		"kind = \"tiered\"\n" +
		"rules_budget = 8000\n" +
		"common_sense_budget = 20000\n"
	if err := os.WriteFile(cfgFile, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	hc, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("意外错误: %v", err)
	}
	if hc.ContextStrategyRulesBudget != 8000 {
		t.Errorf("文件的 rules_budget 应生效: %d", hc.ContextStrategyRulesBudget)
	}
	if hc.ContextStrategyCommonSenseBudget != 20000 {
		t.Errorf("文件的 common_sense_budget 应生效: %d", hc.ContextStrategyCommonSenseBudget)
	}
}

// TestBuildConfigGraphPrecedence：图投影开关与参数的 flag>文件 优先级（v2-S2，G2f）。
//
// 默认关 = 零图开销（不进 core 必经路径）；显式 flag 可开，配置文件也能开。
func TestBuildConfigGraphPrecedence(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	body := "model = \"m\"\n" +
		"[graph]\n" +
		"enabled = true\n" +
		"max_nodes = 200000\n" +
		"max_edges = 2000000\n" +
		"max_depth = 4\n" +
		"max_results = 800\n" +
		"escalate_reachable = 20\n" +
		"core = [\"core/*\", \"contracts/event.go\"]\n"
	if err := os.WriteFile(cfgFile, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}

	// ① 只有文件 → 用文件的开关与参数。
	hc, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("① 意外错误: %v", err)
	}
	if !hc.Graph {
		t.Fatal("① 文件的 graph.enabled 应生效")
	}
	if hc.GraphMaxNodes != 200000 || hc.GraphMaxEdges != 2000000 {
		t.Errorf("① 文件的上界应生效: %d/%d", hc.GraphMaxNodes, hc.GraphMaxEdges)
	}
	if hc.GraphMaxDepth != 4 || hc.GraphMaxResults != 8*100 {
		t.Errorf("① 文件的遍历上限应生效: %d/%d", hc.GraphMaxDepth, hc.GraphMaxResults)
	}
	if hc.GraphEscalateReachable != 20 {
		t.Errorf("① 文件的 escalate 阈值应生效: %d", hc.GraphEscalateReachable)
	}
	if len(hc.GraphCoreNodes) != 2 || hc.GraphCoreNodes[0] != "core/*" {
		t.Errorf("① 文件的核心接口声明应生效: %v", hc.GraphCoreNodes)
	}

	// ② 什么都不给 → 默认关（G2f：可选投影，零图开销）。
	noneFile := filepath.Join(t.TempDir(), "none.toml")
	if err := os.WriteFile(noneFile, []byte("model = \"m\"\n"), 0o600); err != nil {
		t.Fatalf("写空配置: %v", err)
	}
	hc2, err := buildConfigFrom(t, "-config", noneFile)
	if err != nil {
		t.Fatalf("② 意外错误: %v", err)
	}
	if hc2.Graph {
		t.Error("② 无配置时图投影应默认关闭")
	}

	// ③ 显式 flag 覆盖文件（flag > 文件）。
	hc3, err := buildConfigFrom(t, "-config", cfgFile,
		"-graph", "-graph-max-depth", "9", "-graph-escalate-reachable", "77",
		"-graph-core", "host/*,runtime/graph/*")
	if err != nil {
		t.Fatalf("③ 意外错误: %v", err)
	}
	if !hc3.Graph {
		t.Error("③ flag -graph 应生效")
	}
	if hc3.GraphMaxDepth != 9 || hc3.GraphEscalateReachable != 77 {
		t.Errorf("③ flag 的上限/阈值应覆盖文件: %d/%d", hc3.GraphMaxDepth, hc3.GraphEscalateReachable)
	}
	if len(hc3.GraphCoreNodes) != 2 || hc3.GraphCoreNodes[0] != "host/*" {
		t.Errorf("③ flag 的核心接口声明应覆盖文件: %v", hc3.GraphCoreNodes)
	}
}

// ---------------------------------------------------------------------------
// 文档族配置面（G2 第 5 族）
// ---------------------------------------------------------------------------

// TestBuildConfigDocumentsFromFile 钉死 **[documents] 段 → host.Config.DocumentsEnabled**
// 的映射（cmd/alasol 装配面）。
//
// 口径：**只走文件配置面**（不给 flag——与 [vectors] / [blob] 同款：一个布尔开关 +
// 默认关）。enabled=false = 不建 DocStore、/api/docs* 四个端点不注册（404）= 零行为变化。
func TestBuildConfigDocumentsFromFile(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	body := "model = \"m\"\n" +
		"[documents]\n" +
		"enabled = true\n"
	if err := os.WriteFile(cfgFile, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}

	// ① 文件里 enabled = true → host.Config.DocumentsEnabled = true。
	hc, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("① 意外错误: %v", err)
	}
	if !hc.DocumentsEnabled {
		t.Error("① 文件的 [documents].enabled=true 应映射到 host.Config.DocumentsEnabled")
	}

	// ② 没写 [documents] 段 → 默认关（零行为变化，酸测试）。
	noneFile := filepath.Join(t.TempDir(), "none.toml")
	if err := os.WriteFile(noneFile, []byte("model = \"m\"\n"), 0o600); err != nil {
		t.Fatalf("写空配置: %v", err)
	}
	hc2, err := buildConfigFrom(t, "-config", noneFile)
	if err != nil {
		t.Fatalf("② 意外错误: %v", err)
	}
	if hc2.DocumentsEnabled {
		t.Error("② 无 [documents] 段时文档族应默认关闭")
	}

	// ③ 写了 enabled = false → 显式关（不是"没写"）。
	offFile := filepath.Join(t.TempDir(), "off.toml")
	if err := os.WriteFile(offFile, []byte("model = \"m\"\n[documents]\nenabled = false\n"), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	hc3, err := buildConfigFrom(t, "-config", offFile)
	if err != nil {
		t.Fatalf("③ 意外错误: %v", err)
	}
	if hc3.DocumentsEnabled {
		t.Error("③ [documents].enabled=false 应映射为关闭")
	}
}

// ---------------------------------------------------------------------------
// MCP client 配置面（v3-S1，N1）
// ---------------------------------------------------------------------------

// TestBuildConfigMCPFromFile：[[mcp.servers]] 从文件流进 host.Config.MCP。
//
// 口径：**只走文件配置面**（不给 flag——server 列表是结构化数据，硬塞进命令行
// 既难写又容易把凭据泄进 shell 历史与 ps 输出）。
func TestBuildConfigMCPFromFile(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	body := `
model = "m"

[[mcp.servers]]
name = "fs"
command = "npx"
args = ["-y", "@scope/server-fs"]
env = { TOKEN = "sk-token" }

[[mcp.servers]]
name = "remote"
transport = "http"
url = "http://127.0.0.1:1234/mcp"
enabled = false
`
	if err := os.WriteFile(cfgFile, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	hc, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if len(hc.MCP.Servers) != 2 {
		t.Fatalf("MCP server 数 = %d，want 2", len(hc.MCP.Servers))
	}
	s0 := hc.MCP.Servers[0]
	if s0.Name != "fs" || s0.Command != "npx" || !s0.Enabled {
		t.Fatalf("server[0] = %+v", s0)
	}
	if len(s0.Args) != 2 || s0.Args[1] != "@scope/server-fs" {
		t.Fatalf("args = %v", s0.Args)
	}
	s1 := hc.MCP.Servers[1]
	if s1.Transport != mcp.TransportHTTP || s1.Enabled {
		t.Fatalf("server[1] = %+v", s1)
	}
}

// TestBuildConfigMCPDefaultOff：没写 [[mcp.servers]] → 零 server（默认关 =
// 不装 MCP，与 v2 行为一致）。
func TestBuildConfigMCPDefaultOff(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(cfgFile, []byte("model = \"m\"\n"), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	hc, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if len(hc.MCP.Servers) != 0 {
		t.Fatalf("未配 server 时 MCP 应为空，得到 %+v", hc.MCP.Servers)
	}
}

// TestBuildConfigMCPBadConfigSurfaces：坏 MCP 配置**不会被静默吞掉**——
// buildConfig 原样搬进 host.Config.MCP，由 host.Config.validate 拒（fail-close）。
// （host 侧的 TestHubMCPBadConfigIsFatal 验的就是那个拒绝点；这里验"搬对了 +
//
//	校验能抓到"。）
func TestBuildConfigMCPBadConfigSurfaces(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	body := `
model = "m"

[[mcp.servers]]
name = "a"
command = "x"

[[mcp.servers]]
name = "a"
command = "y"
`
	if err := os.WriteFile(cfgFile, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	hc, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("buildConfig 自身不该拒（校验归 host）: %v", err)
	}
	if len(hc.MCP.Servers) != 2 {
		t.Fatalf("两个 server 都应被搬进来（让校验去拒）：%+v", hc.MCP.Servers)
	}
	verr := hc.MCP.Validate()
	if verr == nil {
		t.Fatalf("重复 server 名应被拒")
	}
	if !strings.Contains(verr.Error(), "重复声明") {
		t.Fatalf("错误信息应点名重复：%v", verr)
	}
}

// ---------------------------------------------------------------------------
// 工作流引擎配置面（v3-S3，W1）
// ---------------------------------------------------------------------------

// TestBuildConfigGoalLedgerPrecedence 钉死 goal_ledger 的**布尔口径**（显式 flag > 文件 >
// 默认关）：`-goal-ledger=false` 必须能**压过**文件里的 goal_ledger=true。
//
// 背景（p3 修）：此前写的是 `*f.goalLedger || fc.GoalLedger`——"||" 只会把 false 变成
// true，用户在命令行显式关掉的东西压不过配置文件（与 sandbox 同款坑，见
// sandboxEnabledFlag 的说明）。
func TestBuildConfigGoalLedgerPrecedence(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	body := "model = \"m\"\ngoal_ledger = true\n"
	if err := os.WriteFile(cfgFile, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}

	// ① 只有文件 → 用文件的 true。
	hc, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("① 意外错误: %v", err)
	}
	if !hc.GoalLedger {
		t.Error("① 文件的 goal_ledger=true 应生效")
	}

	// ② 显式 `-goal-ledger=false` → **压过**文件（用户显式关掉就是关掉）。
	hc2, err := buildConfigFrom(t, "-config", cfgFile, "-goal-ledger=false")
	if err != nil {
		t.Fatalf("② 意外错误: %v", err)
	}
	if hc2.GoalLedger {
		t.Error("② 显式 -goal-ledger=false 应压过文件里的 goal_ledger=true")
	}

	// ③ 显式 `-goal-ledger` → 开。
	hc3, err := buildConfigFrom(t, "-config", cfgFile, "-goal-ledger=true")
	if err != nil {
		t.Fatalf("③ 意外错误: %v", err)
	}
	if !hc3.GoalLedger {
		t.Error("③ 显式 -goal-ledger=true 应生效")
	}

	// ④ 没有文件配置 → 默认关（零行为变化，酸测试）。
	noneFile := filepath.Join(t.TempDir(), "none.toml")
	if err := os.WriteFile(noneFile, []byte("model = \"m\"\n"), 0o600); err != nil {
		t.Fatalf("写空配置: %v", err)
	}
	hc4, err := buildConfigFrom(t, "-config", noneFile)
	if err != nil {
		t.Fatalf("④ 意外错误: %v", err)
	}
	if hc4.GoalLedger {
		t.Error("④ 无配置时 goal ledger 应默认关闭")
	}
}

// TestBuildConfigPromptTimeoutFlag 钉死 -prompt-timeout：协议面一轮对话的**整轮**上限
// （秒）。缺省 = 0（= 框架默认 600 秒，见 host.Config.PromptRunTimeoutSec）。
func TestBuildConfigPromptTimeoutFlag(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(cfgFile, []byte("model = \"m\"\n"), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}

	// ① 不给 flag → 0（未设 → 框架默认）。
	hc, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("① 意外错误: %v", err)
	}
	if hc.PromptRunTimeoutSec != 0 {
		t.Fatalf("① 缺省 PromptRunTimeoutSec = %d，want 0（未设）", hc.PromptRunTimeoutSec)
	}

	// ② 显式给 flag → 生效。
	hc2, err := buildConfigFrom(t, "-config", cfgFile, "-prompt-timeout", "42")
	if err != nil {
		t.Fatalf("② 意外错误: %v", err)
	}
	if hc2.PromptRunTimeoutSec != 42 {
		t.Fatalf("② -prompt-timeout 42 → PromptRunTimeoutSec = %d，want 42", hc2.PromptRunTimeoutSec)
	}
}

// ---------------------------------------------------------------------------
// embedding 提供者配置面（v4-3-B 装配面）
// ---------------------------------------------------------------------------

// TestBuildConfigEmbeddingProvider 钉死 embedding 提供者的接线口径：
//   - embedding-model 空 → Config.EmbeddingProvider == nil（零行为变化）；
//   - flag 配了 → != nil（openai.Embedder 真客户端）；
//   - TOML [embedding] 三字段（model/base_url/api_key_env）映射正确。
func TestBuildConfigEmbeddingProvider(t *testing.T) {
	// ① 什么都不给 → EmbeddingProvider 保持 nil（零行为变化）。
	noneFile := filepath.Join(t.TempDir(), "none.toml")
	if err := os.WriteFile(noneFile, []byte("model = \"m\"\n"), 0o600); err != nil {
		t.Fatalf("写空配置: %v", err)
	}
	hc, err := buildConfigFrom(t, "-config", noneFile)
	if err != nil {
		t.Fatalf("① 意外错误: %v", err)
	}
	if hc.EmbeddingProvider != nil {
		t.Fatal("① 未配 embedding-model 时 EmbeddingProvider 应保持 nil（零行为变化）")
	}

	// ② flag 路径：-embedding-model 非空 → != nil。
	hc2, err := buildConfigFrom(t, "-config", noneFile, "-embedding-model", "text-embedding-3-small")
	if err != nil {
		t.Fatalf("② 意外错误: %v", err)
	}
	if hc2.EmbeddingProvider == nil {
		t.Fatal("② 配了 -embedding-model 后 EmbeddingProvider 应非 nil")
	}

	// ③ TOML [embedding] 三字段映射：model/base_url/api_key_env 全部生效。
	//
	// 注意：文件里**显式**指定了 api_key_env → 按 F1 的口径值只从那个变量取，变量没设
	// 就**启动即拒**（fail-close，见 TestBuildConfigEmbeddingCredentialFailClose）。
	// 本用例要验的是"三字段映射"，故把那个变量设上。
	t.Setenv("ALASOL_EMB_KEY", "sk-emb-key")
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	body := "model = \"m\"\n" +
		"[embedding]\n" +
		"model = \"emb-file-model\"\n" +
		"base_url = \"https://emb.example/v1\"\n" +
		"api_key_env = \"ALASOL_EMB_KEY\"\n"
	if err := os.WriteFile(cfgFile, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	hc3, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("③ 意外错误: %v", err)
	}
	if hc3.EmbeddingProvider == nil {
		t.Fatal("③ 文件配了 [embedding].model 后 EmbeddingProvider 应非 nil")
	}
	fc, err := host.LoadFileConfig(cfgFile)
	if err != nil {
		t.Fatalf("③ LoadFileConfig: %v", err)
	}
	if fc == nil {
		t.Fatal("③ LoadFileConfig 返回 nil")
	}
	if fc.Embedding.Model != "emb-file-model" || fc.Embedding.BaseURL != "https://emb.example/v1" ||
		fc.Embedding.APIKeyEnv != "ALASOL_EMB_KEY" {
		t.Fatalf("[embedding] 三字段映射不对: %+v", fc.Embedding)
	}
	if b, ok := hc3.EmbeddingProvider.(interface{ BaseURL() string }); !ok || b.BaseURL() != "https://emb.example/v1" {
		t.Fatalf("EmbeddingProvider.BaseURL() = %v, want https://emb.example/v1", b)
	}

	// ④ flag 覆盖文件（显式 flag > 文件，与 model 同款口径）。
	hc4, err := buildConfigFrom(t, "-config", cfgFile, "-embedding-model", "flag-emb")
	if err != nil {
		t.Fatalf("④ 意外错误: %v", err)
	}
	if hc4.EmbeddingProvider == nil {
		t.Fatal("④ flag 覆盖文件后 EmbeddingProvider 应非 nil")
	}
}

// TestBuildConfigEmbeddingPrecedence 钉死 embedding 的 flag>env>文件 优先级
// （与 chat model 同款 Precedence 形态；env = ALASOL_EMBEDDING_MODEL / ALASOL_EMBEDDING_BASE_URL）。
// base_url 三级回退用 Embedder.BaseURL() 观测；model 用"有没有接上提供者"观测。
func TestBuildConfigEmbeddingPrecedence(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	body := "model = \"m\"\n" +
		"base_url = \"https://chat.example/v1\"\n" +
		"[embedding]\n" +
		"model = \"file-emb\"\n" +
		"base_url = \"https://file-emb.example/v1\"\n"
	if err := os.WriteFile(cfgFile, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}

	// ① 只有文件 → FileConfig 映射正确 + base_url 生效。
	fc, err := host.LoadFileConfig(cfgFile)
	if err != nil || fc == nil {
		t.Fatalf("LoadFileConfig: fc=%v err=%v", fc, err)
	}
	if fc.Embedding.Model != "file-emb" || fc.Embedding.BaseURL != "https://file-emb.example/v1" {
		t.Fatalf("[embedding] 文件映射不对: %+v", fc.Embedding)
	}
	hc1, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("① 意外错误: %v", err)
	}
	if hc1.EmbeddingProvider == nil {
		t.Fatal("① 文件配了 [embedding].model 后 EmbeddingProvider 应非 nil")
	}
	if b := embBaseURLOf(hc1.EmbeddingProvider); b != "https://file-emb.example/v1" {
		t.Fatalf("① 文件 base_url 应生效: %q", b)
	}

	// ② env 覆盖文件（H15：env > 文件）。
	t.Setenv("ALASOL_EMBEDDING_BASE_URL", "https://env-emb.example/v1")
	hc2, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("② 意外错误: %v", err)
	}
	if b := embBaseURLOf(hc2.EmbeddingProvider); b != "https://env-emb.example/v1" {
		t.Fatalf("② env 应覆盖文件 base_url, got %q", b)
	}

	// ③ flag 覆盖 env + 文件（显式 flag 最高）。
	hc3, err := buildConfigFrom(t, "-config", cfgFile,
		"-embedding-base-url", "https://flag-emb.example/v1")
	if err != nil {
		t.Fatalf("③ 意外错误: %v", err)
	}
	if b := embBaseURLOf(hc3.EmbeddingProvider); b != "https://flag-emb.example/v1" {
		t.Fatalf("③ flag 应覆盖 env+文件 base_url, got %q", b)
	}

	// ④ embedding base_url 全空 → 回退 chat 的 base-url。
	emptyEmb := filepath.Join(t.TempDir(), "empty-emb.toml")
	emptyBody := "model = \"m\"\nbase_url = \"https://chat.example/v1\"\n"
	if err := os.WriteFile(emptyEmb, []byte(emptyBody), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	t.Setenv("ALASOL_EMBEDDING_BASE_URL", "")
	hc4, err := buildConfigFrom(t, "-config", emptyEmb, "-embedding-model", "e")
	if err != nil {
		t.Fatalf("④ 意外错误: %v", err)
	}
	if b := embBaseURLOf(hc4.EmbeddingProvider); b != "https://chat.example/v1" {
		t.Fatalf("④ 空 embedding-base-url 应回退 chat base-url, got %q", b)
	}
}

// embBaseURLOf 取 EmbeddingProvider 的 BaseURL（openai.Embedder 的观测面）。
func embBaseURLOf(p host.EmbeddingProvider) string {
	if b, ok := p.(interface{ BaseURL() string }); ok {
		return b.BaseURL()
	}
	return ""
}

// ---------------------------------------------------------------------------
// embedding 凭据解析（F1：不静默跨边界回退）
// ---------------------------------------------------------------------------

// embAuthRecorder 是一个假 /embeddings 端点：记录每次请求的 Authorization 头，
// 并回一个合法的 embeddings 响应（让装配面能真的发一次请求来验凭据）。
type embAuthRecorder struct {
	mu    sync.Mutex
	auths []string
}

func (rec *embAuthRecorder) snapshot() []string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]string(nil), rec.auths...)
}

// newEmbAuthServer 起假端点（形态照 host/vectors_e2e_test.go 的假 /embeddings）。
func newEmbAuthServer(t *testing.T) (*embAuthRecorder, string) {
	t.Helper()
	rec := &embAuthRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.auths = append(rec.auths, r.Header.Get("Authorization"))
		rec.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		body, _ := json.Marshal(map[string]any{
			"object": "list",
			"data": []map[string]any{
				{"object": "embedding", "index": 0, "embedding": []float32{0.1, 0.2}},
			},
		})
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return rec, srv.URL
}

// embProbe 让装配面造出的 EmbeddingProvider 真发一次请求（验凭据必须真的发出去，
// 否则断言的就是"没发请求"而不是"发了哪个凭据"）。
func embProbe(t *testing.T, p host.EmbeddingProvider) {
	t.Helper()
	if p == nil {
		t.Fatal("EmbeddingProvider 为 nil（装配面没造出提供者）")
	}
	if _, err := p.Embed(context.Background(), "凭据探针"); err != nil {
		t.Fatalf("Embed 探针请求失败: %v", err)
	}
}

// TestBuildConfigEmbeddingCredentialExplicitEnv 钉 F1 的三种口径之一：
// **显式指定** embedding 凭据的变量名 → 值**只**从那个变量取（不发 chat 的凭据）。
//
// 断言打到假 /embeddings 端点的 Authorization 头（形态照 host/vectors_e2e_test.go）。
func TestBuildConfigEmbeddingCredentialExplicitEnv(t *testing.T) {
	const (
		chatKey = "sk-chat-secret"
		embKey  = "sk-emb-secret"
	)
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(cfgFile, []byte("model = \"m\"\n"), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	rec, srvURL := newEmbAuthServer(t)
	t.Setenv("ALASOL_CHAT_KEY", chatKey)
	t.Setenv("ALASOL_EMB_KEY", embKey)

	hc, err := buildConfigFrom(t, "-config", cfgFile,
		"-api-key-env", "ALASOL_CHAT_KEY",
		"-embedding-model", "emb-model",
		"-embedding-base-url", srvURL,
		"-embedding-api-key-env", "ALASOL_EMB_KEY",
	)
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if hc.EmbeddingProvider == nil {
		t.Fatal("配了 -embedding-model 后 EmbeddingProvider 应非 nil")
	}
	embProbe(t, hc.EmbeddingProvider)

	auths := rec.snapshot()
	if len(auths) != 1 {
		t.Fatalf("假端点应被打到 1 次，得到 %d 次（auths=%v）", len(auths), auths)
	}
	if auths[0] != "Bearer "+embKey {
		t.Fatalf("Authorization = %q，期望 Bearer <embedding key>（显式指定时只发那个变量里的值）", auths[0])
	}
	if strings.Contains(auths[0], chatKey) {
		t.Fatalf("显式指定 embedding 凭据时不得发 chat 的凭据: %q", auths[0])
	}
}

// TestBuildConfigEmbeddingCredentialFailClose 钉 F1 的 fail-close：
// **显式指定**了 embedding 凭据的变量名，但那个变量没设 → **启动即拒**（不静默回退
// 到 chat 的凭据），且**错误文案不含任何 key 值**（只点名变量名）。
//
// 变异口径：把 fail-close 改回静默回退 → 本测试红（err == nil）。
func TestBuildConfigEmbeddingCredentialFailClose(t *testing.T) {
	const chatKey = "sk-chat-secret-should-not-leak"
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(cfgFile, []byte("model = \"m\"\n"), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	t.Setenv("ALASOL_CHAT_KEY", chatKey)
	t.Setenv("ALASOL_EMB_KEY", "") // 显式指定的变量**没设**

	_, err := buildConfigFrom(t, "-config", cfgFile,
		"-api-key-env", "ALASOL_CHAT_KEY",
		"-embedding-model", "emb-model",
		"-embedding-api-key-env", "ALASOL_EMB_KEY",
	)
	if err == nil {
		t.Fatal("显式指定 embedding 凭据变量却没设 → 应启动即拒（fail-close，不静默回退 chat 的凭据）")
	}
	if !strings.Contains(err.Error(), "ALASOL_EMB_KEY") {
		t.Fatalf("错误文案应点名变量名: %v", err)
	}
	if strings.Contains(err.Error(), chatKey) {
		t.Fatalf("错误文案泄漏了 chat 的 key 值: %v", err)
	}
}

// TestBuildConfigEmbeddingCredentialFallbackToChat 钉 F1 的第三种口径：
// **未显式指定** embedding 凭据的变量名 → **沿用 chat 的凭据**（变量名与值都沿用）。
func TestBuildConfigEmbeddingCredentialFallbackToChat(t *testing.T) {
	const chatKey = "sk-chat-secret"
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(cfgFile, []byte("model = \"m\"\n"), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	rec, srvURL := newEmbAuthServer(t)
	t.Setenv("ALASOL_CHAT_KEY", chatKey)
	t.Setenv("ALASOL_EMB_KEY", "sk-emb-should-not-be-used")

	// 不给 -embedding-api-key-env → 未显式指定 → 沿用 chat 的凭据。
	hc, err := buildConfigFrom(t, "-config", cfgFile,
		"-api-key-env", "ALASOL_CHAT_KEY",
		"-embedding-model", "emb-model",
		"-embedding-base-url", srvURL,
	)
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if hc.EmbeddingProvider == nil {
		t.Fatal("配了 -embedding-model 后 EmbeddingProvider 应非 nil")
	}
	embProbe(t, hc.EmbeddingProvider)

	auths := rec.snapshot()
	if len(auths) != 1 {
		t.Fatalf("假端点应被打到 1 次，得到 %d 次", len(auths))
	}
	if auths[0] != "Bearer "+chatKey {
		t.Fatalf("Authorization = %q，期望沿用 chat 的凭据 Bearer <chat key>", auths[0])
	}
}

// TestBuildConfigEmbeddingCredentialFileEnvSameAsChat 钉"显式指定了**与 chat 同名**的
// 变量名"这一支：同一凭据是意图 → 与 chat 同款（值 = chat 已收敛的那个值，env>文件）。
func TestBuildConfigEmbeddingCredentialFileEnvSameAsChat(t *testing.T) {
	const chatKey = "sk-chat-secret"
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	body := "model = \"m\"\n" +
		"api_key = \"sk-file-key\"\n" +
		"[embedding]\n" +
		"model = \"emb-model\"\n" +
		"api_key_env = \"ALASOL_CHAT_KEY\"\n"
	if err := os.WriteFile(cfgFile, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	rec, srvURL := newEmbAuthServer(t)
	t.Setenv("ALASOL_CHAT_KEY", chatKey)

	hc, err := buildConfigFrom(t, "-config", cfgFile,
		"-api-key-env", "ALASOL_CHAT_KEY",
		"-embedding-base-url", srvURL,
	)
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if hc.EmbeddingProvider == nil {
		t.Fatal("文件配了 [embedding].model 后 EmbeddingProvider 应非 nil")
	}
	embProbe(t, hc.EmbeddingProvider)

	auths := rec.snapshot()
	if len(auths) != 1 {
		t.Fatalf("假端点应被打到 1 次，得到 %d 次", len(auths))
	}
	if auths[0] != "Bearer "+chatKey {
		t.Fatalf("Authorization = %q，期望与 chat 同款（同名 = 同一凭据）", auths[0])
	}
}
