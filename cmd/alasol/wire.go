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
	"github.com/Yorha9e/alasol/runtime/approver/terminal"
	"github.com/Yorha9e/alasol/runtime/cap/local"
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

	// provider 覆盖（测试注入脚本 provider；nil = openai）
	provider core.Provider
	// approver 覆盖（测试注入；nil = 按 Approval 选）
	approver core.Approver
}

// runner 是装配好的真机回路。
type runner struct {
	loop  *loop.Loop
	store *sqlite.Store
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
	if err := registerBuiltinTools(reg); err != nil {
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
		return nil, fmt.Errorf("构造 loop: %w", err)
	}
	return &runner{loop: lp, store: st}, nil
}

// registerBuiltinTools 把 read/write/bash 三个内置工具注册进 reg（run 与 resume 共用）。
func registerBuiltinTools(reg *registry.Registry) error {
	fsImpl := local.FS{}
	execImpl := local.Exec{}
	execs := map[string]core.ToolExecutor{
		"read":  tools.NewRead(fsImpl),
		"write": tools.NewWrite(fsImpl),
		"bash":  tools.NewBash(execImpl),
	}
	for _, d := range tools.Descriptors() {
		ex, ok := execs[d.Name]
		if !ok {
			return fmt.Errorf("工具 %q 无执行器", d.Name)
		}
		if err := reg.RegisterTool(d, ex); err != nil {
			return fmt.Errorf("注册工具 %s: %w", d.Name, err)
		}
	}
	return nil
}

// Run 跑一轮真机回路。
func (r *runner) Run(ctx context.Context, prompt string) (loop.Result, error) {
	return r.loop.Run(ctx, prompt)
}

// Close 释放存储。
func (r *runner) Close() {
	if r.store != nil {
		r.store.Close()
	}
}
