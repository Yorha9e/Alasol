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

// boolPtr 取 bool 指针（W6-A 的 share_project_assets 是 *bool）。
func boolPtr(b bool) *bool { return &b }

// strPtr 取 string 指针（Workflow.Command 是 *string）。
func strPtr(s string) *string { return &s }

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

// TestNewPackageRejectsBadContextStrategy 是 **W6-A 装载期 fail-close**：能力包的
// agent_types 是不可信配置——声明了 context_strategy 就必须说清楚 kind：
//
//   - **kind 为空** = "声明了却没说什么" → 运行期会被静默当成 append（策略语义悄悄变味）；
//   - **kind 不在枚举内** = 宿主无从装配（造不出工厂）→ 派出去才炸。
//
// 两种都在**装载期**拒（整个包拒），别让半坏的声明溜进运行期。
func TestNewPackageRejectsBadContextStrategy(t *testing.T) {
	cases := []struct {
		name string
		cs   *contracts.ContextStrategy
		want string // 错误信息里应出现的关键词（点名问题）
	}{
		{name: "kind 为空", cs: &contracts.ContextStrategy{Kind: ""}, want: "kind 为空"},
		{name: "kind 不在枚举", cs: &contracts.ContextStrategy{Kind: "custom_thing"}, want: "不在枚举内"},
	}
	for _, tc := range cases {
		spec := baseSpec()
		spec.AgentTypes = []contracts.AgentType{{ID: "coder", Prompt: "p", ContextStrategy: tc.cs}}
		_, err := NewPackage(spec)
		if err == nil {
			t.Errorf("%s：应拒绝装载", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s：错误信息应点名问题（%q），得到 %v", tc.name, tc.want, err)
		}
	}

	// 合法声明（枚举内）与未声明都应通过。
	ok := baseSpec()
	ok.AgentTypes = []contracts.AgentType{
		{ID: "coder", Prompt: "p"}, // 未声明 = append 默认（W6-A）
		{ID: "planner", Prompt: "p", ContextStrategy: &contracts.ContextStrategy{Kind: contracts.ContextStrategyKindTiered}},
		{ID: "scout", Prompt: "p", ContextStrategy: &contracts.ContextStrategy{Kind: contracts.ContextStrategyKindAppend}},
	}
	if _, err := NewPackage(ok); err != nil {
		t.Errorf("合法的 context_strategy 声明应通过: %v", err)
	}
}

// TestNewPackageRejectsBadWorkflowContextStrategy 是 **F1 回归**：**工作流级**
// context_strategy 是**必填**声明（契约 schema：required ["kind"]）——声明了就得说清楚 kind：
//
//   - **kind 为空** = 非法声明 → 运行期宿主无从装配 → 静默按 append 处理（策略语义悄悄变味）；
//   - **kind 不在枚举内** = 宿主造不出工厂 → 与其派出去才炸，不如装载期就拒。
//
// 两种都在**装载期**拒（整个包拒），别让半坏的声明溜进运行期。
//
// 与 agent_type 级（TestNewPackageRejectsBadContextStrategy）**同一张枚举表**——两处口径
// 不许分叉（分叉 = "agent_type 拒 / 工作流级收下" 的两套口径；契约文案承诺的是两处都受约束）。
func TestNewPackageRejectsBadWorkflowContextStrategy(t *testing.T) {
	cases := []struct {
		name string
		kind contracts.ContextStrategyKind
		want string // 错误信息里应出现的关键词（点名问题）
	}{
		{name: "kind 为空", kind: "", want: "kind 为空"},
		{name: "kind 不在枚举", kind: "custom_thing", want: "不在枚举内"},
	}
	for _, tc := range cases {
		spec := baseSpec()
		spec.ContextStrategy = contracts.ContextStrategy{Kind: tc.kind}
		_, err := NewPackage(spec)
		if err == nil {
			t.Errorf("%s：应拒绝装载", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s：错误信息应点名问题（%q），得到 %v", tc.name, tc.want, err)
		}
		// 错误点名**包 id**（定位用）。
		if !strings.Contains(err.Error(), spec.ID) {
			t.Errorf("%s：错误信息应点名包 id（%q），得到 %v", tc.name, spec.ID, err)
		}
		// 越枚举时错误点名**越界的 kind**（知道该改哪个值）。
		if tc.kind != "" && !strings.Contains(err.Error(), string(tc.kind)) {
			t.Errorf("%s：错误信息应点名越界的 kind（%q），得到 %v", tc.name, tc.kind, err)
		}
	}

	// 四个枚举取值都应通过装载（枚举内 = 合法声明；宿主实现与否是**运行期**的事）。
	for _, k := range []contracts.ContextStrategyKind{
		contracts.ContextStrategyKindAppend,
		contracts.ContextStrategyKindStageScoped,
		contracts.ContextStrategyKindSlidingWindow,
		contracts.ContextStrategyKindTiered,
	} {
		spec := baseSpec()
		spec.ContextStrategy = contracts.ContextStrategy{Kind: k}
		if _, err := NewPackage(spec); err != nil {
			t.Errorf("枚举内的 kind %q 应通过装载: %v", k, err)
		}
	}
}

// TestLoadDirNamesBadFile：装载期拒绝要**点名文件**（用户得知道改哪个）。
func TestLoadDirNamesBadFile(t *testing.T) {
	dir := t.TempDir()
	bad := `{"v":1,"id":"bad","philosophy":"坏包","context_strategy":{"kind":"append"},"agent_types":[{"id":"a","can_spawn":["a"]}]}`
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
	base.Gates = []contracts.Gate{{ID: "g1", Kind: contracts.GateKindPolicy, Params: json.RawMessage(`{"cmd":"make ci"}`)}}
	base.Grants = []contracts.Grant{{Tool: "write", Scope: "workspace"}}
	// command 是 *string：`out := base` 只拷指针 → 产物与 base 共享同一个字符串。
	base.Command = strPtr("build")
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

	// ④ W6-A 新字段（context_strategy 指针 + Params []byte、share_project_assets 指针）
	// 同样不得与入参共享——指针浅拷是最隐蔽的别名。
	share := true
	base.AgentTypes[0].ContextStrategy = &contracts.ContextStrategy{
		Kind:   contracts.ContextStrategyKindTiered,
		Params: json.RawMessage(`{"window":256000}`),
	}
	base.AgentTypes[0].ShareProjectAssets = &share
	out2 := Compose(base, extra)
	base.AgentTypes[0].ContextStrategy.Kind = contracts.ContextStrategyKindAppend
	base.AgentTypes[0].ContextStrategy.Params[2] = 'X'
	*base.AgentTypes[0].ShareProjectAssets = false
	got := out2.AgentTypes[0]
	if got.ContextStrategy == nil || got.ContextStrategy.Kind != contracts.ContextStrategyKindTiered {
		t.Errorf("产物与 base 共享了 context_strategy 指针：%+v", got.ContextStrategy)
	}
	if string(got.ContextStrategy.Params) != `{"window":256000}` {
		t.Errorf("产物与 base 共享了 Params 底层数组：%s", got.ContextStrategy.Params)
	}
	if got.ShareProjectAssets == nil || !*got.ShareProjectAssets {
		t.Errorf("产物与 base 共享了 share_project_assets 指针：%v", got.ShareProjectAssets)
	}

	// ⑤ **Gate.Params（json.RawMessage，[]byte 底）**与 **Workflow.Command（*string）**
	// 同样不得与入参共享——这两处别名比切片头更隐蔽：cloneGate 漏拷 Params 时，调用方改
	// 入参的 params 字节会**改到产物**；`out := base` 漏拷 Command 时，产物与 base 指向
	// 同一个字符串（改 base 的 command 会改到产物）。
	base.Gates[0].Params[2] = 'X'
	*base.Command = "mutated"
	if string(out.Gates[0].Params) != `{"cmd":"make ci"}` {
		t.Errorf("产物与 base 共享了 Gate.Params 底层数组：%s", out.Gates[0].Params)
	}
	if out.Command == nil || *out.Command != "build" {
		t.Errorf("产物与 base 共享了 Command 指针：%v", out.Command)
	}
}

// TestComposeDoesNotAliasWorkflowContextStrategyParams 是 **p3 回归**：**工作流级**
// context_strategy.params 是 json.RawMessage（[]byte 底）——只拷结构头（Kind）会与入参
// **共享底层数组**：调用方改入参 → 产物跟着变（组合产物必须与入参完全解耦）。
//
// 两条路径都要拷：① base 路径（`out := base` 带过来的）；② extra 覆盖分支（追加段换策略）。
func TestComposeDoesNotAliasWorkflowContextStrategyParams(t *testing.T) {
	base := baseSpec()
	base.ContextStrategy = contracts.ContextStrategy{
		Kind:   contracts.ContextStrategyKindTiered,
		Params: json.RawMessage(`{"window":256000}`),
	}
	extra := contracts.Workflow{
		ID:         "build-review-critic",
		Philosophy: "追加段。",
		ContextStrategy: contracts.ContextStrategy{
			Kind:   contracts.ContextStrategyKindSlidingWindow,
			Params: json.RawMessage(`{"window":128000}`),
		},
	}

	// ① base 路径（extra 没换策略 → 留 base 的）。
	outBase := Compose(base, contracts.Workflow{ID: "x", Philosophy: "y"})
	// ② extra 覆盖分支（extra 显式要了非 append 策略 → 用 extra 的）。
	outExtra := Compose(base, extra)

	// 改入参的 Params 底层数组 → 产物不受影响。
	base.ContextStrategy.Params[2] = 'X'
	extra.ContextStrategy.Params[2] = 'Y'
	if string(outBase.ContextStrategy.Params) != `{"window":256000}` {
		t.Errorf("工作流级 context_strategy.params 与 base 共享了底层数组：%s", outBase.ContextStrategy.Params)
	}
	if string(outExtra.ContextStrategy.Params) != `{"window":128000}` {
		t.Errorf("工作流级 context_strategy.params 与 extra 共享了底层数组：%s", outExtra.ContextStrategy.Params)
	}
	if outExtra.ContextStrategy.Kind != contracts.ContextStrategyKindSlidingWindow {
		t.Errorf("extra 的非 append 策略应胜出，得到 %q", outExtra.ContextStrategy.Kind)
	}

	// nil 保 nil（没配参数 ≠ 配了空参数）。
	noParams := Compose(baseSpec(), contracts.Workflow{ID: "x", Philosophy: "y"})
	if noParams.ContextStrategy.Params != nil {
		t.Errorf("未配 params 时应保持 nil，得到 %s", noParams.ContextStrategy.Params)
	}
}

// TestComposeSameAgentTypeIDKeepsBaseDeclaration 钉住 **同 id 的合并口径**：base 整条胜出，
// extra 同 id 类型的 context_strategy / share_project_assets 声明**一并丢弃**（不静默拼装
// ——"谁赢"必须确定，不能取决于装载顺序）。要覆盖请改 base 或换 id。
func TestComposeSameAgentTypeIDKeepsBaseDeclaration(t *testing.T) {
	base := baseSpec()
	base.AgentTypes = []contracts.AgentType{{
		ID:                 "coder",
		Prompt:             "base 的 coder",
		ContextStrategy:    &contracts.ContextStrategy{Kind: contracts.ContextStrategyKindTiered},
		ShareProjectAssets: boolPtr(true),
	}}
	extra := contracts.Workflow{
		ID:         "build-review-critic",
		Philosophy: "追加段。",
		AgentTypes: []contracts.AgentType{{
			ID:                 "coder",
			Prompt:             "extra 的 coder",
			ContextStrategy:    &contracts.ContextStrategy{Kind: contracts.ContextStrategyKindSlidingWindow},
			ShareProjectAssets: boolPtr(false),
		}},
	}
	out := Compose(base, extra)
	if len(out.AgentTypes) != 1 {
		t.Fatalf("同 id 应只保留 1 条，得到 %d 条", len(out.AgentTypes))
	}
	got := out.AgentTypes[0]
	if got.Prompt != "base 的 coder" {
		t.Errorf("同 id 时 base 应整条胜出，得到 prompt=%q", got.Prompt)
	}
	if got.ContextStrategy == nil || got.ContextStrategy.Kind != contracts.ContextStrategyKindTiered {
		t.Errorf("同 id 时 base 的 context_strategy 声明应胜出，得到 %+v", got.ContextStrategy)
	}
	if got.ShareProjectAssets == nil || !*got.ShareProjectAssets {
		t.Errorf("同 id 时 base 的 share_project_assets 声明应胜出，得到 %v", got.ShareProjectAssets)
	}
}

// Registry：登记 / 查 id / 查命令 / 重名 fail-close / List 确定性。
func TestRegistry(t *testing.T) {
	reg := NewRegistry()
	cmd := "build"
	// 工作流级 context_strategy 是必填声明（kind 必须非空且在枚举内）——这里给 append。
	p1, _ := NewPackage(contracts.Workflow{V: 1, ID: "build", Command: &cmd, Philosophy: "x", ContextStrategy: contracts.ContextStrategy{Kind: contracts.ContextStrategyKindAppend}})
	p2, _ := NewPackage(contracts.Workflow{V: 1, ID: "tower", Philosophy: "y", ContextStrategy: contracts.ContextStrategy{Kind: contracts.ContextStrategyKindAppend}}) // 无命令
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
	p3, _ := NewPackage(contracts.Workflow{V: 1, ID: "build2", Command: &dupCmd, Philosophy: "z", ContextStrategy: contracts.ContextStrategy{Kind: contracts.ContextStrategyKindAppend}})
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
