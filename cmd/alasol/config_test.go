package main

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/host"
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
