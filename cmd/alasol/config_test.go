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
