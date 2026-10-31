package main

import (
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
)

// TestRegisterBuiltinToolsRemoteSandboxMutuallyExclusive 钉死 v4-2-A 的互斥校验：
// remote 与 sandbox 双开 → 拒（两者都是"换执行环境"，语义冲突）。
func TestRegisterBuiltinToolsRemoteSandboxMutuallyExclusive(t *testing.T) {
	reg := registry.New(registry.NewPolicyAdjudicator(policy.New()))
	_, err := registerBuiltinTools(reg, runnerOpts{
		Sandbox:          true,
		Remote:           true,
		RemoteHost:       "user@harness",
		SandboxWriteDirs: []string{t.TempDir()},
	})
	if err == nil {
		t.Fatal("remote + sandbox 双开应该被拒（互斥）")
	}
	if !strings.Contains(err.Error(), "互斥") {
		t.Errorf("错误信息没说清是互斥：%v", err)
	}
}

// TestRegisterBuiltinToolsRemoteWires 验证 remote 单独启用能装配成功（不真连 ssh）：
// 返回的 sandbox env 为 nil（remote 不需要 Close），read/write/bash 三个工具都注册上了。
func TestRegisterBuiltinToolsRemoteWires(t *testing.T) {
	reg := registry.New(registry.NewPolicyAdjudicator(policy.New()))
	sbEnv, err := registerBuiltinTools(reg, runnerOpts{Remote: true, RemoteHost: "user@harness"})
	if err != nil {
		t.Fatalf("remote 单独启用应该装配成功: %v", err)
	}
	if sbEnv != nil {
		t.Error("remote 模式不该返回 sandbox env（remote 无生命周期需 Close）")
	}
}

// TestRegisterBuiltinToolsRemoteNeedsHost 验证 remote 启用但缺 host → 装配失败（fail-close）。
func TestRegisterBuiltinToolsRemoteNeedsHost(t *testing.T) {
	reg := registry.New(registry.NewPolicyAdjudicator(policy.New()))
	if _, err := registerBuiltinTools(reg, runnerOpts{Remote: true, RemoteHost: "  "}); err == nil {
		t.Error("remote 启用但 RemoteHost 为空应该装配失败")
	}
}
