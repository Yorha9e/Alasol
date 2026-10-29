package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/approver/terminal"
	"github.com/Yorha9e/alasol/runtime/cap/local"
	"github.com/Yorha9e/alasol/runtime/cap/sandbox"
	"github.com/Yorha9e/alasol/runtime/provider/openai"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	"github.com/Yorha9e/alasol/runtime/tools"
)

// runnerOpts 是装配参数（main 的 flag 与测试共用）。
type runnerOpts struct {
	Model      string
	BaseURL    string
	APIKey     string // 已从 env 取出的凭据；不落任何持久化/日志
	Approval   string // deny-all | always-allow | terminal
	Allow      string // 逗号分隔的工具名
	DBPath     string
	MaxTurns   int
	System     string
	SessionEnv core.EnvScope

	// CheckpointEvery 每 N 轮自动 checkpoint（X3；0 = 只手动）。透传给 loop.Config。
	CheckpointEvery int

	// Sandbox 是否启用 SandboxedExecutionEnv（v2-S4，K8/H9；默认 false = local）。
	Sandbox bool
	// SandboxWriteDirs 是沙箱允许写根（空 = CWD + 沙箱临时根）。
	SandboxWriteDirs []string
	// SandboxMaxActiveProcesses / SandboxACLMaxObjects 是沙箱的两个上限（0 = 框架默认）。
	SandboxMaxActiveProcesses int
	SandboxACLMaxObjects      int
	// SandboxStrictGroups 是否启用严格组 SID 模式（见 sandbox.Policy.StrictGroupSIDs）。
	SandboxStrictGroups bool

	// provider 覆盖（测试注入脚本 provider；nil = openai）
	provider core.Provider
	// approver 覆盖（测试注入；nil = 按 Approval 选）
	approver core.Approver
}

// runner 是装配好的真机回路。
type runner struct {
	loop  *loop.Loop
	store *sqlite.Store
	// sandbox 是沙箱执行环境（nil = 未启用，走 local）。
	//
	// runner **持有**它是为了 Close 时能收掉（F8）：WER 排除项是持久的用户级注册表
	// 写入，进程退出不 Close 就会一直留在用户机器上（下次跑 alasol 会看到
	// "别人加过的"排除项，而第一个的痕迹永远不撤）。
	sandbox *sandbox.Env
}

// newRunner 装配：store → bus → policy → registry（注册 read/write/bash）→ provider → approver → loop。
func newRunner(opts runnerOpts) (*runner, error) {
	if opts.Model == "" {
		return nil, fmt.Errorf("Model 必填")
	}
	st, err := sqlite.Open(opts.DBPath, sqlite.Options{})
	if err != nil {
		return nil, fmt.Errorf("打开事件库: %w", err)
	}

	// 策略：-allow 生成 source=cli 的 allow 规则；空规则集 = 一切 deny（fail-close）
	var rules []contracts.PolicyRule
	for _, name := range strings.Split(opts.Allow, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		rules = append(rules, contracts.PolicyRule{
			Effect: contracts.EffectAllow,
			Tool:   name,
			Source: contracts.RuleSourceCli,
		})
	}
	eng := policy.New(rules...)

	reg := registry.New(registry.NewPolicyAdjudicator(eng))
	sbEnv, err := registerBuiltinTools(reg, opts)
	if err != nil {
		st.Close()
		return nil, err
	}

	prov := opts.provider
	if prov == nil {
		prov = openai.New(openai.Options{BaseURL: opts.BaseURL, APIKey: opts.APIKey, Stream: true})
	}
	ap := opts.approver
	if ap == nil {
		switch opts.Approval {
		case "always-allow":
			ap = core.AlwaysAllow{}
		case "terminal":
			ap = terminal.New(os.Stdin, os.Stdout)
		default: // deny-all（默认，fail-close）
			ap = core.DenyAll{}
		}
	}

	b := bus.New(st)
	cfg := loop.Config{
		Provider:        prov,
		Bus:             b,
		Registry:        reg,
		Policy:          eng,
		Approver:        ap,
		SessionID:       core.NewUlid(),
		AgentID:         core.NewUlid(),
		Model:           opts.Model,
		System:          opts.System,
		SessionEnv:      opts.SessionEnv,
		MaxTurns:        opts.MaxTurns,
		Store:           st, // 阶段 8：同一 store 供 checkpoint / resume
		CheckpointEvery: opts.CheckpointEvery,
	}
	lp, err := loop.New(cfg)
	if err != nil {
		st.Close()
		if sbEnv != nil {
			_ = sbEnv.Close()
		}
		return nil, fmt.Errorf("构造 loop: %w", err)
	}
	return &runner{loop: lp, store: st, sandbox: sbEnv}, nil
}

// registerBuiltinTools 把 read/write/bash 三个内置工具注册进 reg（run 与 resume 共用）。
//
// 执行环境（K8：可插拔 ExecutionEnv）：**默认 local**（v1 等价）；opts.Sandbox 为真时
// 换成 sandbox.Env——工具代码一行不动，这就是 K8 酸测试要的"换 env 不动 agent 逻辑"。
//
// 返回值是装配出来的沙箱 env（未启用沙箱时为 nil）：调用方**必须**在收尾时 Close 它
// （F8——WER 排除项是持久的用户级注册表写入，没人 Close 就永远留在用户机器上）。
func registerBuiltinTools(reg *registry.Registry, opts runnerOpts) (*sandbox.Env, error) {
	fsImpl, execImpl := core.FSCapability(local.FS{}), core.ExecCapability(local.Exec{})
	var env *sandbox.Env
	if opts.Sandbox {
		e, err := sandbox.New(opts.sandboxPolicy())
		if err != nil {
			return nil, fmt.Errorf("装配 SandboxedExecutionEnv: %w", err)
		}
		env = e
		fsImpl, execImpl = e, e
	}
	execs := map[string]core.ToolExecutor{
		"read":  tools.NewRead(fsImpl),
		"write": tools.NewWrite(fsImpl),
		"bash":  tools.NewBash(execImpl),
	}
	for _, d := range tools.Descriptors() {
		ex, ok := execs[d.Name]
		if !ok {
			if env != nil {
				_ = env.Close()
			}
			return nil, fmt.Errorf("工具 %q 无执行器", d.Name)
		}
		if err := reg.RegisterTool(d, ex); err != nil {
			if env != nil {
				_ = env.Close()
			}
			return nil, fmt.Errorf("注册工具 %s: %w", d.Name, err)
		}
	}
	return env, nil
}

// Run 跑一轮真机回路。
func (r *runner) Run(ctx context.Context, prompt string) (loop.Result, error) {
	return r.loop.Run(ctx, prompt)
}

// Close 释放存储与沙箱（沙箱的 WER 排除项必须撤掉——F8）。
func (r *runner) Close() {
	if r.store != nil {
		r.store.Close()
	}
	if r.sandbox != nil {
		_ = r.sandbox.Close()
	}
}

// sandboxPolicy 把 runnerOpts 折成 sandbox.Policy。
//
// 口径在 host.SandboxPolicy（**唯一装配处**）：run / resume / serve 走同一条路，
// 不会出现"host 并入 CWD、run 不并入"这种漂移（F21）。
func (o runnerOpts) sandboxPolicy() sandbox.Policy {
	return host.SandboxPolicy(host.SandboxOptions{
		WriteDirs:          o.SandboxWriteDirs,
		MaxActiveProcesses: o.SandboxMaxActiveProcesses,
		ACLMaxObjects:      o.SandboxACLMaxObjects,
		StrictGroups:       o.SandboxStrictGroups,
	})
}
