package workflow

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
)

// fakeMilestone 是测试用的里程碑部件（按名记数）。
type fakeMilestone struct {
	name   string
	passed bool
	calls  int
}

func (f *fakeMilestone) Name() string { return f.name }
func (f *fakeMilestone) Handle(_ context.Context, _ Milestone) (MilestoneResult, error) {
	f.calls++
	return MilestoneResult{Passed: f.passed, Note: "fake"}, nil
}

func baseSpec() contracts.Workflow {
	return contracts.Workflow{
		V:               1,
		ID:              "build",
		Philosophy:      "构建类哲学。",
		AgentTypes:      []contracts.AgentType{{ID: "coder", Prompt: "p", Tools: []string{"read"}, Model: "default"}},
		ContextStrategy: contracts.ContextStrategy{Kind: contracts.ContextStrategyKindAppend},
	}
}

// W3：AugmentSystem 把哲学文本拼进 system prompt 供主 agent 选择。
func TestAugmentSystem(t *testing.T) {
	p, err := NewPackage(baseSpec())
	if err != nil {
		t.Fatal(err)
	}
	if got := p.AugmentSystem("BASE"); got != "BASE\n\n构建类哲学。" {
		t.Errorf("AugmentSystem(BASE) = %q", got)
	}
	if got := p.AugmentSystem(""); got != "构建类哲学。" {
		t.Errorf("AugmentSystem(empty) = %q", got)
	}
	// 不改动入参（返回新串）。
	base := "BASE"
	_ = p.AugmentSystem(base)
	if base != "BASE" {
		t.Errorf("AugmentSystem 改动了入参: %q", base)
	}
}

// fail-close：空 id / 空 philosophy / 重名里程碑部件一律拒绝。
func TestNewPackageValidation(t *testing.T) {
	if _, err := NewPackage(contracts.Workflow{Philosophy: "x"}); err == nil {
		t.Error("空 id 应拒绝")
	}
	if _, err := NewPackage(contracts.Workflow{ID: "x"}); err == nil {
		t.Error("空 philosophy 应拒绝")
	}
	_, err := NewPackage(baseSpec(), &fakeMilestone{name: "m"}, &fakeMilestone{name: "m"})
	if err == nil {
		t.Error("重名里程碑部件应拒绝")
	}
}

// TestNewPackageRejectsBadAgentTypes 是 **p3 回归**：能力包声明的 agent_types 是
// **不可信配置**（可被任何语言写/分发，W13）——装载期就要 fail-close 拒掉：
// 重名 id、can_spawn 悬空 / 自环 / 有环。
func TestNewPackageRejectsBadAgentTypes(t *testing.T) {
	cases := []struct {
		name  string
		types []contracts.AgentType
		want  string // 错误信息里应出现的关键词（点名问题）
	}{
		{
			name: "重名 id",
			types: []contracts.AgentType{
				{ID: "coder", Prompt: "a"},
				{ID: "coder", Prompt: "b"},
			},
			want: "重名",
		},
		{
			name:  "can_spawn 悬空",
			types: []contracts.AgentType{{ID: "coder", Prompt: "a", CanSpawn: []string{"ghost"}}},
			want:  "未声明",
		},
		{
			name:  "can_spawn 自环",
			types: []contracts.AgentType{{ID: "coder", Prompt: "a", CanSpawn: []string{"coder"}}},
			want:  "自环",
		},
		{
			name: "can_spawn 成环",
			types: []contracts.AgentType{
				{ID: "a", Prompt: "a", CanSpawn: []string{"b"}},
				{ID: "b", Prompt: "b", CanSpawn: []string{"a"}},
			},
			want: "派生环",
		},
		{
			name:  "空 id",
			types: []contracts.AgentType{{ID: "", Prompt: "a"}},
			want:  "空 id",
		},
	}
	for _, tc := range cases {
		spec := baseSpec()
		spec.AgentTypes = tc.types
		_, err := NewPackage(spec)
		if err == nil {
			t.Errorf("%s：应拒绝装载", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s：错误信息应点名问题（%q），得到 %v", tc.name, tc.want, err)
		}
	}

	// 合法的包（含一条正常的 can_spawn）应通过。
	ok := baseSpec()
	ok.AgentTypes = []contracts.AgentType{
		{ID: "planner", Prompt: "p"},
		{ID: "coder", Prompt: "c", CanSpawn: []string{"planner"}},
	}
	if _, err := NewPackage(ok); err != nil {
		t.Errorf("合法的 agent_types 应通过: %v", err)
	}
}

// TestLoadDirNamesBadFile：装载期拒绝要**点名文件**（用户得知道改哪个）。
func TestLoadDirNamesBadFile(t *testing.T) {
	dir := t.TempDir()
	bad := `{"v":1,"id":"bad","philosophy":"坏包","agent_types":[{"id":"a","can_spawn":["a"]}]}`
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadDir(dir)
	if err == nil {
		t.Fatal("坏能力包应让装载失败（fail-close）")
	}
	if !strings.Contains(err.Error(), "bad.json") {
		t.Errorf("错误信息应点名文件（bad.json），得到 %v", err)
	}
}

// W14：HandleMilestone 按名派发；无部件 → 错误（不静默当通过）。
func TestHandleMilestone(t *testing.T) {
	fm := &fakeMilestone{name: "validation.passed", passed: true}
	p, err := NewPackage(baseSpec(), fm)
	if err != nil {
		t.Fatal(err)
	}
	res, err := p.HandleMilestone(context.Background(), Milestone{Kind: "validation.passed"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Passed || fm.calls != 1 {
		t.Errorf("派发失败: res=%+v calls=%d", res, fm.calls)
	}
	if _, err := p.HandleMilestone(context.Background(), Milestone{Kind: "nope"}); err == nil {
		t.Error("无部件的里程碑应报错")
	}
}

// W4：Compose 把追加段并进基础包（哲学并陈 + agent_types 去重追加 + 策略可换）。
func TestCompose(t *testing.T) {
	base := baseSpec()
	base.Grants = []contracts.Grant{{Tool: "write", Scope: "workspace"}}
	extra := contracts.Workflow{
		ID:              "build-review-critic",
		Philosophy:      "追加 loop 哲学。",
		AgentTypes:      []contracts.AgentType{{ID: "coder", Prompt: "p2", Tools: []string{"read"}, Model: "default"}, {ID: "critic", Prompt: "c", Tools: []string{"read"}, Model: "default"}},
		Grants:          []contracts.Grant{{Tool: "write", Scope: "workspace"}, {Tool: "read", Scope: "workspace"}},
		ContextStrategy: contracts.ContextStrategy{Kind: contracts.ContextStrategyKindTiered},
	}
	out := Compose(base, extra)
	if out.ID != "build" {
		t.Errorf("compose 应保留 base id，得到 %q", out.ID)
	}
	if out.Philosophy != "构建类哲学。\n\n追加 loop 哲学。" {
		t.Errorf("哲学并陈错: %q", out.Philosophy)
	}
	// coder 去重（不重复），critic 新增。
	var ids []string
	for _, at := range out.AgentTypes {
		ids = append(ids, at.ID)
	}
	if len(ids) != 2 || ids[0] != "coder" || ids[1] != "critic" {
		t.Errorf("agent_types 去重追加错: %v", ids)
	}
	// grants 去重追加（write/workspace 重复 → 只一条；read/workspace 新增）。
	if len(out.Grants) != 2 {
		t.Errorf("grants 去重追加错: %+v", out.Grants)
	}
	// extra 显式要 tiered → 用它。
	if out.ContextStrategy.Kind != contracts.ContextStrategyKindTiered {
		t.Errorf("compose 应采用 extra 的 tiered 策略，得到 %q", out.ContextStrategy.Kind)
	}
}

// TestComposeDoesNotAliasInputs 是 **p3 回归**：Compose 的产物与入参**必须解耦**。
//
// 背景：`out := base` 只拷了切片头——后续 append 在 cap 有余量时写进 **base 的底层数组**
// （改到入参），元素内部的切片（AgentType.Tools 等）更是直接共享。组合产物必须是独立的一份。
func TestComposeDoesNotAliasInputs(t *testing.T) {
	base := baseSpec()
	base.AgentTypes = []contracts.AgentType{{ID: "coder", Prompt: "p", Tools: []string{"read"}, Model: "default"}}
	// 多留点 cap：浅拷贝下 append 会直接写进 base 的底层数组（别名 bug 的触发条件）。
	base.AgentTypes = append(base.AgentTypes, contracts.AgentType{ID: "spare", Prompt: "s", Tools: []string{"read"}, Model: "default"})
	base.Gates = []contracts.Gate{{ID: "g1", Kind: contracts.GateKindPolicy}}
	base.Grants = []contracts.Grant{{Tool: "write", Scope: "workspace"}}
	extra := contracts.Workflow{
		ID:              "build-review-critic",
		Philosophy:      "追加 loop 哲学。",
		AgentTypes:      []contracts.AgentType{{ID: "critic", Prompt: "c", Tools: []string{"read"}, Model: "default"}},
		ContextStrategy: contracts.ContextStrategy{Kind: contracts.ContextStrategyKindTiered},
	}

	out := Compose(base, extra)
	if len(out.AgentTypes) != 3 {
		t.Fatalf("compose 后 agent_types 应为 3 条，得到 %d", len(out.AgentTypes))
	}

	// ① 改 base 的元素内容 → 产物不受影响。
	base.AgentTypes[0].Tools[0] = "MUTATED"
	if out.AgentTypes[0].Tools[0] != "read" {
		t.Errorf("产物与 base 共享了 Tools 底层数组：%v", out.AgentTypes[0].Tools)
	}
	// ② 往 base 的切片里 append → 产物不受影响（浅拷贝下这里会串味）。
	base.AgentTypes = append(base.AgentTypes, contracts.AgentType{ID: "intruder", Prompt: "x"})
	if len(out.AgentTypes) != 3 {
		t.Errorf("往 base 追加后产物的 agent_types 变了（共享底层数组）：%d 条", len(out.AgentTypes))
	}
	for _, at := range out.AgentTypes {
		if at.ID == "intruder" {
			t.Error("产物里出现了 base 后加的类型（共享底层数组）")
		}
	}
	// ③ 改 extra 的元素 → 产物不受影响。
	extra.AgentTypes[0].Tools[0] = "MUTATED2"
	if out.AgentTypes[2].Tools[0] != "read" {
		t.Errorf("产物与 extra 共享了 Tools 底层数组：%v", out.AgentTypes[2].Tools)
	}
}

// Registry：登记 / 查 id / 查命令 / 重名 fail-close / List 确定性。
func TestRegistry(t *testing.T) {
	reg := NewRegistry()
	cmd := "build"
	p1, _ := NewPackage(contracts.Workflow{V: 1, ID: "build", Command: &cmd, Philosophy: "x"})
	p2, _ := NewPackage(contracts.Workflow{V: 1, ID: "tower", Philosophy: "y"}) // 无命令
	if err := reg.Register(p1); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(p2); err != nil {
		t.Fatal(err)
	}
	// 重名 id → 拒。
	if err := reg.Register(p1); err == nil {
		t.Error("重名 id 应拒绝")
	}
	// 撞命令 → 拒。
	dupCmd := "build"
	p3, _ := NewPackage(contracts.Workflow{V: 1, ID: "build2", Command: &dupCmd, Philosophy: "z"})
	if err := reg.Register(p3); err == nil {
		t.Error("撞 slash 命令应拒绝")
	}
	if got, ok := reg.ByCommand("build"); !ok || got.ID() != "build" {
		t.Errorf("ByCommand(build) = %v %v", got, ok)
	}
	if _, ok := reg.ByCommand("tower"); ok {
		t.Error("无命令的包不应被 ByCommand 找到")
	}
	list := reg.List()
	if len(list) != 2 || list[0].ID() != "build" || list[1].ID() != "tower" {
		t.Errorf("List 排序/内容错: %v", list)
	}
}

// 目录加载：读 *.json → 纯数据包；损坏文件 → 整体失败；目录不存在 → (nil,nil)。
func TestLoadDir(t *testing.T) {
	// 目录不存在 → nil, nil。
	if pkgs, err := LoadDir(filepath.Join(t.TempDir(), "nope")); err != nil || pkgs != nil {
		t.Errorf("不存在目录应返回 (nil,nil)，得到 %v %v", pkgs, err)
	}
	dir := t.TempDir()
	good := `{"v":1,"id":"fromfile","philosophy":"文件哲学。","context_strategy":{"kind":"append"}}`
	if err := os.WriteFile(filepath.Join(dir, "a.json"), []byte(good), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ignore.txt"), []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	pkgs, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(pkgs) != 1 || pkgs[0].ID() != "fromfile" {
		t.Fatalf("加载错: %+v", pkgs)
	}
	// 文件加载的包不带里程碑部件。
	if len(pkgs[0].MilestoneHandlers()) != 0 {
		t.Error("文件加载的包不应带里程碑部件")
	}
	// 损坏 JSON → 整体失败并点名。
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDir(dir); err == nil {
		t.Error("损坏的能力包文件应让整体加载失败")
	}
}

// ParsePackage：JSON 形态可解析（W13 跨边界契约）。
func TestParsePackage(t *testing.T) {
	data := `{"v":1,"id":"x","philosophy":"p","agent_types":[{"id":"coder","prompt":"pp","tools":["read"],"model":"default"}],"context_strategy":{"kind":"tiered"}}`
	p, err := ParsePackage([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if p.ID() != "x" || p.ContextStrategyKind() != contracts.ContextStrategyKindTiered {
		t.Errorf("解析错: id=%q kind=%q", p.ID(), p.ContextStrategyKind())
	}
	if len(p.Spec().AgentTypes) != 1 {
		t.Error("agent_types 应解析出 1 个")
	}
	// round-trip：Spec 可再序列化（跨边界中立）。
	if _, err := json.Marshal(p.Spec()); err != nil {
		t.Errorf("Spec 再序列化失败: %v", err)
	}
}
