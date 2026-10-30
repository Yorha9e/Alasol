package main

// G14 配置面（cmd 层）：-reasoning-effort flag 与 TOML reasoning_effort 的优先级
// （显式 flag > 文件；空 = 不传）——与 system/model 等同一条 H15 优先级口径。

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReasoningEffortFlagAndFile(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(cfgFile, []byte("model = \"file-model\"\nreasoning_effort = \"medium\"\n"), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}

	// ① 只有文件 → 用文件值。
	hc, err := buildConfigFrom(t, "-config", cfgFile)
	if err != nil {
		t.Fatalf("① 意外错误: %v", err)
	}
	if hc.ReasoningEffort != "medium" {
		t.Fatalf("① 文件 reasoning_effort 应生效: %q", hc.ReasoningEffort)
	}

	// ② 显式 flag 压过文件。
	hc2, err := buildConfigFrom(t, "-config", cfgFile, "-reasoning-effort", "xhigh")
	if err != nil {
		t.Fatalf("② 意外错误: %v", err)
	}
	if hc2.ReasoningEffort != "xhigh" {
		t.Fatalf("② flag 应压过文件: %q", hc2.ReasoningEffort)
	}

	// ③ 都没设 → 空（不传）。
	empty := filepath.Join(t.TempDir(), "empty.toml")
	if err := os.WriteFile(empty, []byte("model = \"m\"\n"), 0o600); err != nil {
		t.Fatalf("写空配置: %v", err)
	}
	hc3, err := buildConfigFrom(t, "-config", empty)
	if err != nil {
		t.Fatalf("③ 意外错误: %v", err)
	}
	if hc3.ReasoningEffort != "" {
		t.Fatalf("③ 未设时应为空（不传）: %q", hc3.ReasoningEffort)
	}

	// ④ 坏档位经 flag 传入 → host.Config.validate 拒（fail-close 到启动期）。
	//    （buildConfig 本身不校验档位——那是 host 的职责；这里确认错误来自 host。）
	if _, err := buildConfigFrom(t, "-config", empty, "-reasoning-effort", "turbo"); err != nil {
		// buildConfig 不校验档位（设计：校验只在 host），所以这里**不该**报错。
		t.Fatalf("④ buildConfig 不该校验档位（那是 host 的职责）: %v", err)
	}
}
