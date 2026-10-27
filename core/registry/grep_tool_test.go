package registry_test

// 本文件是 v0 酸测试（06 §5 验收）的落地：**注册一个全新 Tool 只新增一个 _test.go 文件，
// 不改任何已有文件**——core/registry 的公开 API 足够表达"加一个工具"这件事。
//
// 为了证明这一点，这里用外部测试包（package registry_test）：
// 只用 registry / contracts / core / policy 的导出 API，不触碰任何内部实现。

import (
	"context"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
)

// grepExec 是一个全新的工具执行器实现（与 registry_test.go 里的 fakeExec 无关，
// 也与其他任何已有文件无关——换实现零改动）。
type grepExec struct {
	// hits 记录被调用次数，用于验证注册表带回的就是这个实现。
	hits int
}

func (g *grepExec) Execute(_ context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	g.hits++
	return contracts.ToolResult{
		Content: []contracts.ContentPart{{Type: "text", Text: sp("grep ok")}},
	}, nil
}

func TestRegisterBrandNewToolWithOnlyANewFile(t *testing.T) {
	// 策略只放行 grep 需要的 search 能力；其余一律 fail-close deny。
	eng := policy.New(contracts.PolicyRule{
		ID:     sp("allow-search"),
		Effect: contracts.EffectAllow,
		Tool:   "search",
		Source: contracts.RuleSourceManaged,
	})
	r := registry.New(registry.NewPolicyAdjudicator(eng))

	// 1) 声明工具（跨边界契约形状）。
	tool := contracts.Tool{
		Name:        "grep",
		Description: sp("按正则检索工作区"),
		Schema:      []byte(`{"type":"object","properties":{"pattern":{"type":"string"}}}`),
		SideEffect:  contracts.SideEffectRead,
		Cancelable:  true,
		Approvable:  false,
		Idempotent:  true,
		Replay:      contracts.ReplayKindSafe,
	}

	// 2) 注册：meta 由 Tool 派生，impl 是全新的 grepExec。
	exec := &grepExec{}
	if err := r.RegisterTool(tool, exec); err != nil {
		t.Fatalf("RegisterTool: %v", err)
	}

	// 3) 能力裁决通过后才可启用（E3）。
	if err := r.Register(contracts.ExtensionMeta{
		Name:         "grep.caps",
		Kind:         "policy",
		Capabilities: []string{"search"},
	}, nil); err != nil {
		t.Fatalf("Register(grep.caps): %v", err)
	}
	if err := r.Enable(context.Background(), "grep.caps"); err != nil {
		t.Fatalf("Enable(grep.caps): %v", err)
	}
	// 未申报能力的工具扩展也能直接启用。
	if err := r.Enable(context.Background(), "grep"); err != nil {
		t.Fatalf("Enable(grep): %v", err)
	}

	// 4) 取回并执行：注册表带回的正是这个实现（换实现零改动的另一半）。
	e, ok := r.Get("grep")
	if !ok {
		t.Fatalf("Get(grep) 未找到")
	}
	if e.Meta.Kind != registry.ExtensionKindTool {
		t.Fatalf("Kind=%q, want tool", e.Meta.Kind)
	}
	if e.State != registry.StateEnabled {
		t.Fatalf("State=%q, want enabled", e.State)
	}
	got, ok := e.Impl.(core.ToolExecutor)
	if !ok {
		t.Fatalf("Impl 不是 core.ToolExecutor: %T", e.Impl)
	}
	res, err := got.Execute(context.Background(), contracts.ToolCallIntentPayload{
		ToolCallID: "call_1",
		ToolName:   "grep",
		Arguments:  []byte(`{"pattern":"TODO"}`),
		Idempotent: true,
		Replay:     contracts.ReplayKindSafe,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(res.Content) != 1 || res.Content[0].Text == nil || *res.Content[0].Text != "grep ok" {
		t.Fatalf("结果内容不符: %+v", res.Content)
	}
	if exec.hits != 1 {
		t.Fatalf("hits=%d, want 1", exec.hits)
	}

	// 5) 列出来应含两个条目，按注册顺序。
	list := r.List("")
	if len(list) != 2 || list[0].Meta.Name != "grep" || list[1].Meta.Name != "grep.caps" {
		t.Fatalf("List=%+v", list)
	}

	// 6) 换一个实现：只需卸载再注册，已有文件一行不改。
	if err := r.Unregister("grep"); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	exec2 := &grepExec{}
	if err := r.RegisterTool(tool, exec2); err != nil {
		t.Fatalf("RegisterTool(v2): %v", err)
	}
	e2, _ := r.Get("grep")
	if e2.Impl != any(exec2) {
		t.Fatalf("换实现后 Impl=%v, want exec2", e2.Impl)
	}
}

func sp(s string) *string { return &s }
