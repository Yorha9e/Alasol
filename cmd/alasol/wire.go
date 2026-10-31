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
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/approver/terminal"
	"github.com/Yorha9e/alasol/runtime/cap/local"
	"github.com/Yorha9e/alasol/runtime/cap/remote"
	"github.com/Yorha9e/alasol/runtime/cap/sandbox"
	"github.com/Yorha9e/alasol/runtime/mcp"
	"github.com/Yorha9e/alasol/runtime/provider/openai"
	"github.com/Yorha9e/alasol/runtime/store/blobfs"
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

	// ReasoningEffort 模型思维链强度档位（G14；空 = 不传）。透传给 loop.Config →
	// ModelRequest.ReasoningEffort → provider 的 reasoning_effort。
	ReasoningEffort string

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

	// Remote 是否启用 RemoteExecutionEnv（v4-2-A，K8 第三档：执行面经系统 ssh 到远端）。
	// 默认 false = 走 local。与 Sandbox **互斥**（registerBuiltinTools 拒双开）。
	// 鉴权交系统 ssh，本结构不收任何凭据（H18）。
	Remote bool
	// RemoteHost 是 ssh 目标（user@host 或 ~/.ssh/config 别名）。Remote 启用时必填。
	RemoteHost string

	// MCP 是 MCP client 配置（v3-S1，N1：client 先）。**只走文件配置面**
	// （[[mcp.servers]]）——不给 flag：server 列表是结构化数据（命令/参数/env），
	// 硬塞进命令行既难写又容易把凭据泄进 shell 历史与 ps 输出（H12/H19）。
	// 零值 = 不装 MCP（不连外部进程、不注册工具，与 v2 行为一致）。
	MCP mcp.Config

	// BlobDir 是大 payload 外置 blob 的仓库目录（v4-3-A，G2e/D4；**空 = 关** = 零行为变化）。
	// 只走文件配置面（TOML `[blob]`）——不给 flag（与 multi_agent 同款：一个目录路径 +
	// 一个阈值，结构化数据 + 默认关）。
	BlobDir string
	// BlobMaxInline 是 result 内联阈值（字节；<=0 = 不外置，即使配了 BlobDir）。
	BlobMaxInline int64

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
	// mcp 是 MCP client 管理器（v3-S1；nil = 未装配）。runner **持有**它是为了
	// Close 时收尸——stdio server 是本进程拉起的，杀树归本进程（陷阱 5：
	// 谁拉起谁收尸）。工具在装配期就进了 reg，loop 眼里它们与 read/write/bash 无异。
	mcp *mcp.Manager
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

	// MCP client（v3-S1，N1）：外部 server 的工具注册进**同一个** reg——与内置工具
	// 同一个入口、同一条 X1 三段写路径、同一套策略链与 R5 可见性。
	//
	// 配置错误（server 名重复 / transport 缺 command 或 url）→ 致命，拒绝装配：
	// 那是使用者要修的。连接失败（命令不存在 / 握手超时 / tools/list 失败）→ 非致命，
	// 记下来继续（坏 server 不该让 `alasol run` 跑不起来）。
	mcpMgr, mcpErrs, err := assembleRunnerMCP(context.Background(), opts.MCP, reg)
	if err != nil {
		st.Close()
		if sbEnv != nil {
			_ = sbEnv.Close()
		}
		return nil, err
	}
	for _, e := range mcpErrs {
		fmt.Fprintf(os.Stderr, "[alasol] MCP server 装配有问题（该 server 的工具未注册）: %v\n", e)
	}
	if names := mcpMgr.ToolNames(); len(names) > 0 {
		fmt.Fprintf(os.Stderr, "[alasol] MCP 工具已注册：%v\n", names)
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

	// 大 payload 外置 blob（v4-3-A，G2e/D4）：**dir 非空才建**（默认关 = 零行为变化，
	// completed 载荷逐字节等价）。与 host.New 同口径：目录建不出来 = "显式要的能力没起来"
	// → 装配失败（exit 1），不静默不外置。
	//
	// 阈值（BlobMaxInline）<= 0 = 不外置（存储仍建——读侧/诊断需要它）。
	var blobStore storage.Blob
	if dir := strings.TrimSpace(opts.BlobDir); dir != "" {
		bs, berr := blobfs.New(dir)
		if berr != nil {
			st.Close()
			if sbEnv != nil {
				_ = sbEnv.Close()
			}
			if mcpMgr != nil {
				_ = mcpMgr.Close()
			}
			return nil, fmt.Errorf("建 blob 外置存储 %q: %w", dir, berr)
		}
		blobStore = bs
	}

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
		ReasoningEffort: opts.ReasoningEffort, // 思维链档位（G14；空 = 不传）
		// D4：大 payload 外置（v4-3-A，G2e）——loop 侧只管**写**（外置）；读侧经
		// host 的 GET /api/blobs/{id}（CLI 这条路径没有 HTTP 面，全文仍在 blob 里）。
		BlobSink:      blobStore,
		BlobMaxInline: opts.BlobMaxInline,
	}
	lp, err := loop.New(cfg)
	if err != nil {
		st.Close()
		if sbEnv != nil {
			_ = sbEnv.Close()
		}
		if mcpMgr != nil {
			_ = mcpMgr.Close()
		}
		return nil, fmt.Errorf("构造 loop: %w", err)
	}
	return &runner{loop: lp, store: st, sandbox: sbEnv, mcp: mcpMgr}, nil
}

// assembleRunnerMCP 连接 opts.MCP 声明的 server 并把工具注册进 reg
// （run / resume 与 host.New 共用同一套口径，见 host/mcp.go 的注释）。
//
// 返回 (manager, 非致命错误, 致命错误)。零 server → (nil, nil, nil)（不装 MCP）。
func assembleRunnerMCP(ctx context.Context, cfg mcp.Config, reg *registry.Registry) (*mcp.Manager, []error, error) {
	if len(cfg.Servers) == 0 {
		return nil, nil, nil
	}
	mgr, err := mcp.New(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("MCP 配置: %w", err)
	}
	return mgr, mgr.Attach(ctx, reg), nil
}

// registerBuiltinTools 把 read/write/bash 三个内置工具注册进 reg（run 与 resume 共用）。
//
// 执行环境（K8：可插拔 ExecutionEnv）：**默认 local**（v1 等价）；opts.Sandbox 为真时
// 换成 sandbox.Env——工具代码一行不动，这就是 K8 酸测试要的"换 env 不动 agent 逻辑"。
//
// 返回值是装配出来的沙箱 env（未启用沙箱时为 nil）：调用方**必须**在收尾时 Close 它
// （F8——WER 排除项是持久的用户级注册表写入，没人 Close 就永远留在用户机器上）。
func registerBuiltinTools(reg *registry.Registry, opts runnerOpts) (*sandbox.Env, error) {
	// remote 与 sandbox 互斥（v4-2-A）：两者都是"换执行环境"，双开语义冲突 → 拒。
	if opts.Sandbox && opts.Remote {
		return nil, fmt.Errorf("remote 与 sandbox 互斥：不能同时启用（二选一：本机沙箱 or 远端 SSH）")
	}
	fsImpl, execImpl := core.FSCapability(local.FS{}), core.ExecCapability(local.Exec{})
	var env *sandbox.Env
	switch {
	case opts.Sandbox:
		e, err := sandbox.New(opts.sandboxPolicy())
		if err != nil {
			return nil, fmt.Errorf("装配 SandboxedExecutionEnv: %w", err)
		}
		env = e
		fsImpl, execImpl = e, e
	case opts.Remote:
		// RemoteExecutionEnv（v4-2-A）：只有 Exec 经 SSH 落地；Fs/Proc/Net 显式
		// ErrUnsupported（预留位）——read/write 接到 remote env 会诚实报"未支持"。
		renv, err := remote.New(remote.Config{Host: opts.RemoteHost})
		if err != nil {
			return nil, fmt.Errorf("装配 RemoteExecutionEnv: %w", err)
		}
		fsImpl, execImpl = renv, renv
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

// Close 释放存储、沙箱与 MCP 连接（沙箱的 WER 排除项必须撤掉——F8；
// MCP 的 server 进程树必须收尸——陷阱 5）。
func (r *runner) Close() {
	if r.store != nil {
		r.store.Close()
	}
	if r.sandbox != nil {
		_ = r.sandbox.Close()
	}
	if r.mcp != nil {
		_ = r.mcp.Close()
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
