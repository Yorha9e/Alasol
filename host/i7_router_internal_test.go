package host

import (
	"testing"

	"github.com/Yorha9e/alasol/contracts"
)

// fakeRouter 是 core.Router 的测试替身：按 map 路由，并记录被咨询过的类型 id。
type fakeRouter struct {
	models map[string]string
	calls  []string
}

func (f *fakeRouter) RouteModel(agentType string) string {
	f.calls = append(f.calls, agentType)
	return f.models[agentType]
}

func (f *fakeRouter) consulted(id string) bool {
	for _, c := range f.calls {
		if c == id {
			return true
		}
	}
	return false
}

// I7：router 非 nil 时**优先**用其结果——内建 coder/planner 声明 model="default"
// （= 主模型 "main-model"），咨询 fake router 后应变成 router 给的模型。
//
// **变异自证（钉子）**：把 resolveAgentModel 里的 router 咨询块删掉 → coder.Model 退回
// "main-model" → 本测试变红。
func TestResolveAgentModelRouterConsulted(t *testing.T) {
	r := &fakeRouter{models: map[string]string{
		"coder":   "routed-coder",
		"planner": "routed-planner",
	}}
	reg, ids, err := buildAgentTypeRegistry("main-model", nil, r)
	if err != nil {
		t.Fatalf("buildAgentTypeRegistry: %v", err)
	}
	if len(ids) == 0 {
		t.Fatal("ids 为空")
	}
	coder, ok := reg.Get("coder")
	if !ok {
		t.Fatal("coder 类型未注册")
	}
	if coder.Model != "routed-coder" {
		t.Errorf("coder.Model = %q，期望 router 结果 %q", coder.Model, "routed-coder")
	}
	planner, ok := reg.Get("planner")
	if !ok {
		t.Fatal("planner 类型未注册")
	}
	if planner.Model != "routed-planner" {
		t.Errorf("planner.Model = %q，期望 router 结果 %q", planner.Model, "routed-planner")
	}
	if !r.consulted("coder") || !r.consulted("planner") {
		t.Error("router 应按类型 id 被咨询（coder / planner）")
	}
}

// I7：router 对某类型"无意见"（RouteModel 返回空）→ 回落既有口径（default/空 = 主模型）。
func TestResolveAgentModelRouterNoOpinionFallsBack(t *testing.T) {
	r := &fakeRouter{models: map[string]string{}} // 任何类型都返回 ""
	if got := resolveAgentModel("main-model", "coder", "default", r); got != "main-model" {
		t.Errorf("router 无意见时应回落主模型，得到 %q", got)
	}
	if got := resolveAgentModel("main-model", "coder", "gpt-x", r); got != "gpt-x" {
		t.Errorf("router 无意见时应回落到类型声明的显式模型，得到 %q", got)
	}
}

// I7：nil router = 不路由 = 与 router 咨询加入前**逐字等价**（生产默认 nil = 零行为变化）。
func TestResolveAgentModelNilRouterZeroChange(t *testing.T) {
	cases := []struct{ name, m, want string }{
		{"default sentinel = 主模型", "default", "main-model"},
		{"空 = 主模型", "", "main-model"},
		{"显式模型名原样", "gpt-x", "gpt-x"},
		{"trim 后原样", "  gpt-y  ", "gpt-y"},
	}
	for _, tc := range cases {
		if got := resolveAgentModel("main-model", "coder", tc.m, nil); got != tc.want {
			t.Errorf("%s: resolveAgentModel(nil router) = %q，期望 %q", tc.name, got, tc.want)
		}
	}
}

// I7：自定义类型同样过 router（buildAgentTypeRegistry 对 custom 分支也咨询 router）。
func TestBuildAgentTypeRegistryRouterCustomType(t *testing.T) {
	r := &fakeRouter{models: map[string]string{"ops": "routed-ops"}}
	custom := []contracts.AgentType{{ID: "ops", Model: "default"}}
	reg, _, err := buildAgentTypeRegistry("main-model", custom, r)
	if err != nil {
		t.Fatalf("buildAgentTypeRegistry: %v", err)
	}
	ops, ok := reg.Get("ops")
	if !ok {
		t.Fatal("自定义类型 ops 未注册")
	}
	if ops.Model != "routed-ops" {
		t.Errorf("ops.Model = %q，期望 router 结果 %q", ops.Model, "routed-ops")
	}
}
