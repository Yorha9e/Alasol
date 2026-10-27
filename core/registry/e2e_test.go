package registry_test

// 端到端装配冒烟（跨 core/policy + core/registry 两个包）：
// 扩展申报能力 → 策略引擎裁决 → 注册表启用 → 工具调用再经 policy.Decide 过一遍人审。
//
// 目的：验证两个包能只靠导出 API 组合出一个完整的"能力授予 → 调用放行"链路，
// 且全程 fail-close（无 allow 规则的每一步都落 deny）。

import (
	"context"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
)

type noopExec struct{}

func (noopExec) Execute(context.Context, contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	return contracts.ToolResult{}, nil
}

func TestEndToEndCapabilityGrantThenToolCall(t *testing.T) {
	ctx := context.Background()

	// 规则集：managed 放行 fs:read；project deny 掉 fs:write（跨来源并集，H5）；
	// workflow 试图 allow fs:write —— 必须被忽略（H24）。
	eng := policy.New(
		contracts.PolicyRule{ID: sp("m-allow-read"), Effect: contracts.EffectAllow, Tool: "fs:read", Source: contracts.RuleSourceManaged},
		contracts.PolicyRule{ID: sp("p-deny-write"), Effect: contracts.EffectDeny, Tool: "fs:write", Source: contracts.RuleSourceProject},
		contracts.PolicyRule{ID: sp("w-allow-write"), Effect: contracts.EffectAllow, Tool: "fs:write", Source: contracts.RuleSourceWorkflow},
		contracts.PolicyRule{ID: sp("u-ask-bash"), Effect: contracts.EffectAsk, Tool: "bash", Source: contracts.RuleSourceUser},
	)
	r := registry.New(registry.NewPolicyAdjudicator(eng))

	// 1) 只读扩展：能力获准 → 可启用。
	if err := r.RegisterTool(contracts.Tool{Name: "read", SideEffect: contracts.SideEffectRead}, noopExec{}); err != nil {
		t.Fatalf("RegisterTool(read): %v", err)
	}
	if err := r.Register(contracts.ExtensionMeta{Name: "fs.reader", Kind: "tool", Capabilities: []string{"fs:read"}}, nil); err != nil {
		t.Fatalf("Register(fs.reader): %v", err)
	}
	if err := r.Enable(ctx, "fs.reader"); err != nil {
		t.Fatalf("fs.reader 应可启用: %v", err)
	}

	// 2) 写扩展：workflow 的 allow 被忽略、project 的 deny 进并集 → 拒绝启用（fail-close）。
	if err := r.RegisterTool(contracts.Tool{Name: "write", SideEffect: contracts.SideEffectWrite}, noopExec{}); err != nil {
		t.Fatalf("RegisterTool(write): %v", err)
	}
	if err := r.Register(contracts.ExtensionMeta{Name: "fs.writer", Kind: "tool", Capabilities: []string{"fs:write"}}, nil); err != nil {
		t.Fatalf("Register(fs.writer): %v", err)
	}
	err := r.Enable(ctx, "fs.writer")
	if err == nil {
		t.Fatalf("fs.writer 不应被启用（workflow allow 不可放宽）")
	}
	if !strings.Contains(err.Error(), "fs:write") {
		t.Fatalf("错误应指出未获准能力: %v", err)
	}
	if e, _ := r.Get("fs.writer"); e.State != registry.StateRegistered {
		t.Fatalf("fs.writer State=%q, want registered", e.State)
	}

	// 3) 工具调用链：fs:read 直接放行；bash 走 ask → DenyAll → deny（fail-close）。
	if d := policy.Decide(ctx, eng, core.DenyAll{}, policy.Query{Tool: "fs:read"}); d.Verdict != contracts.EffectAllow {
		t.Fatalf("fs:read verdict=%s, want allow", d.Verdict)
	}
	// ask 路径必须带 session/agent 上下文（审批请求的契约必填项），
	// 否则 Decide 直接 fail-close 拒绝（不构造非法审批请求）。
	sid := contracts.Ulid(core.NewUlid())
	aid := contracts.Ulid(core.NewUlid())
	askQuery := policy.Query{Tool: "bash", SessionID: &sid, AgentID: &aid}
	d := policy.Decide(ctx, eng, core.DenyAll{}, askQuery)
	if d.Verdict != contracts.EffectDeny {
		t.Fatalf("bash verdict=%s, want deny（无人应答 = deny）", d.Verdict)
	}
	if d.Reason == nil || !strings.Contains(*d.Reason, "fail-close") {
		t.Fatalf("bash Reason=%v, 需含 fail-close", d.Reason)
	}
	// 换成显式 always-allow 才放行（默认永远是 deny，D14/H8）。
	if d := policy.Decide(ctx, eng, core.AlwaysAllow{}, askQuery); d.Verdict != contracts.EffectAllow {
		t.Fatalf("bash(always-allow) verdict=%s, want allow", d.Verdict)
	}

	// 4) 已启用的只读扩展可以停用/注销（生命周期闭环）。
	if err := r.Disable("fs.reader"); err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if err := r.Unregister("fs.reader"); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	if _, ok := r.Get("fs.reader"); ok {
		t.Fatalf("注销后仍存在")
	}
}
