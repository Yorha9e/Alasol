package main

// run/resume 的**多 agent 装配面**的钉子（T5-2 / T5-3）：
//
//  1. **纯恢复**（`alasol resume` 不带 -continue）**不该**被坏 agent_types 拦下——它压根
//     不建 agent 树、不派生子 agent（用不到的能力不许拒绝启动）；
//  2. **续跑**（-continue）才装配多 agent 环境 → 那时才校验 agent_types（坏配置照旧
//     fail-close 点名）；
//  3. 装配期对"**本进程不支持的** context_strategy kind"（非 append）**点名 Warn**
//     （不阻断——fail-close 发生在 spawn 时是 core 的既有语义，装载期提前告知即可）。

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/registry"
)

// badAgentTypesTOML 是一份**故意坏**的 agent_types（can_spawn 悬空 → 装载期 fail-close）。
const badAgentTypesTOML = "[[agent_types]]\nid = \"ghost\"\nprompt = \"x\"\ncan_spawn = [\"nonexistent\"]\n"

// TestResumePureRecoveryIgnoresBadAgentTypes（T5-3）：`alasol resume`（**纯恢复**，不带
// -continue）+ 坏 agent_types → **成功**（退出码 0）。
//
// 病根（修前）：多 agent 环境在**全路径**装配 → 纯恢复也被 agent_types 的装载期校验拦下
// → 退出码 1。同一份 TOML 在 serve 能跑（host 装配），在纯恢复里却起不来——违反
// "用不到的能力不许拒绝启动"。
//
// 变异自证：把 runnerAgentsEnv 挪回全路径（不进 -continue 分支）→ 本用例红。
func TestResumePureRecoveryIgnoresBadAgentTypes(t *testing.T) {
	db := filepath.Join(t.TempDir(), "resume.db")
	session, _ := seedCrashedSession(t, db)
	cfg := maWriteConfig(t, "multi_agent = true\n\n"+badAgentTypesTOML)

	out := captureStdout(t)
	errOut := captureStderr(t)
	code := resumeMain([]string{
		"-session", string(session), "-db", db, "-verify", "none",
		"-config", cfg,
	})
	got := out()
	msg := errOut()

	if code != 0 {
		t.Fatalf("纯恢复不该被坏 agent_types 拦（退出码 %d）:\nstdout:\n%s\nstderr:\n%s", code, got, msg)
	}
	if strings.Contains(msg, "装配多 agent 环境") {
		t.Fatalf("纯恢复不该装配多 agent 环境（更不该校验 agent_types）:\n%s", msg)
	}
}

// TestResumeContinueStillValidatesAgentTypes（T5-3 的另一半）：**续跑**（-continue）才
// 装配多 agent 环境 → 坏 agent_types 照旧 fail-close 点名（该用到的能力不许悄悄放过）。
func TestResumeContinueStillValidatesAgentTypes(t *testing.T) {
	db := filepath.Join(t.TempDir(), "resume.db")
	session, _ := seedCrashedSession(t, db)
	cfg := maWriteConfig(t, "multi_agent = true\n\n"+badAgentTypesTOML)

	out := captureStdout(t)
	errOut := captureStderr(t)
	code := resumeMain([]string{
		"-session", string(session), "-db", db, "-verify", "none",
		"-config", cfg, "-model", "scripted", "-base-url", "http://127.0.0.1:1/v1",
		"-continue", "继续干活",
	})
	_ = out()
	msg := errOut()

	if code == 0 {
		t.Fatalf("续跑路径装配多 agent 环境时，坏 agent_types 应 fail-close（退出码 0 不对）:\n%s", msg)
	}
	if !strings.Contains(msg, "装配多 agent 环境") || !strings.Contains(msg, "ghost") {
		t.Fatalf("续跑路径应点名坏 agent_types:\n%s", msg)
	}
}

// TestRunnerAgentsEnvWarnsUnsupportedContextStrategyKind（T5-2）：装配期对"本进程
// **不支持**的 context_strategy kind"（非 append）**点名 Warn**——不阻断（fail-close
// 发生在 spawn 时是 core 的既有语义），但要**提前告知**（否则现场一头雾水）。
//
// 附带的口径：**未声明**（= append 默认）与**声明 append** 都不 Warn（core 根本不调
// 钩子，没有 fail-close 可发生）。
func TestRunnerAgentsEnvWarnsUnsupportedContextStrategyKind(t *testing.T) {
	cases := []struct {
		name     string
		custom   []contracts.AgentType
		wantWarn bool
	}{
		{
			name: "声明tiered_本进程没接钩子_要点名",
			custom: []contracts.AgentType{{
				ID: "tiered_child", Prompt: "x",
				ContextStrategy: &contracts.ContextStrategy{Kind: contracts.ContextStrategyKindTiered},
			}},
			wantWarn: true,
		},
		{
			name: "声明stage_scoped_要点名",
			custom: []contracts.AgentType{{
				ID: "scoped_child", Prompt: "x",
				ContextStrategy: &contracts.ContextStrategy{Kind: contracts.ContextStrategyKindStageScoped},
			}},
			wantWarn: true,
		},
		{
			name:     "未声明_不Warn",
			custom:   []contracts.AgentType{{ID: "plain_child", Prompt: "x"}},
			wantWarn: false,
		},
		{
			name: "声明append_不Warn",
			custom: []contracts.AgentType{{
				ID: "append_child", Prompt: "x",
				ContextStrategy: &contracts.ContextStrategy{Kind: contracts.ContextStrategyKindAppend},
			}},
			wantWarn: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			errOut := captureStderr(t)
			_, err := runnerAgentsEnv(true, "scripted", c.custom, core.NewUlid(), registry.New(nil))
			msg := errOut()
			if err != nil {
				t.Fatalf("Warn 不该阻断装配: %v\n%s", err, msg)
			}
			warned := strings.Contains(msg, "context_strategy.kind")
			if warned != c.wantWarn {
				t.Fatalf("Warn 出现与否不对（want %v）:\n%s", c.wantWarn, msg)
			}
			if c.wantWarn && !strings.Contains(msg, "fail-close") {
				t.Errorf("Warn 应写明后果（spawn 时 fail-close）:\n%s", msg)
			}
		})
	}
}

// TestRunMainWarnsUnsupportedContextStrategyKind（T5-2 的 CLI 面）：配置里的
// [[agent_types.context_strategy]] kind=tiered → `alasol run` 装配期点名 Warn，
// 且**不阻断**（不会因为这个声明而拒绝启动）。
func TestRunMainWarnsUnsupportedContextStrategyKind(t *testing.T) {
	const tieredTypes = "[[agent_types]]\nid = \"tiered_child\"\nprompt = \"x\"\n" +
		"[agent_types.context_strategy]\nkind = \"tiered\"\n"
	cfg := maWriteConfig(t, "multi_agent = true\n\n"+tieredTypes)
	db := filepath.Join(t.TempDir(), "events.db")

	out := captureStdout(t)
	errOut := captureStderr(t)
	code := runMain([]string{
		"-prompt", "你好", "-model", "test-model", "-db", db,
		"-base-url", "http://127.0.0.1:1/v1", "-config", cfg,
	})
	_ = out()
	msg := errOut()

	if strings.Contains(msg, "装配 agent 类型注册表") {
		t.Fatalf("声明了不支持的 kind 不该在装配期被拒（fail-close 发生在 spawn 时）:\n%s", msg)
	}
	if !strings.Contains(msg, "tiered_child") || !strings.Contains(msg, "tiered") {
		t.Fatalf("装配期应点名告知不支持的 context_strategy kind:\n%s", msg)
	}
	// 死端点 → 运行失败（退出码 1），但**不是**因为装配被拒。
	if code != 1 || !strings.Contains(msg, "运行失败") {
		t.Fatalf("应越过装配闸走到 Run（死端点 → 运行失败）, code=%d\n%s", code, msg)
	}
}
