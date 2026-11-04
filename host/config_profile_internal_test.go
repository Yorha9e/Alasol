package host

// G-i **profile 机制**的内部单测（config 面）：
//
//	① [[profiles]] 折叠：名字唯一（重名 → 装载期 fail-close）+ 空名 → 拒；
//	② profile → config 变更集：**空字段 = 不覆盖**（模板语义，不是"清除"语义）；
//	③ 叠放次序：显式 config **逐字段压过** profile（provider/context 子字段各自压，
//	   tools 整组压——与各自的 LWW 口径一致）。

import (
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
)

func strp(s string) *string { return &s }
func i64p(v int64) *int64   { return &v }
func f64p(v float64) *float64 {
	return &v
}

// TestProfileConfigsRejectsDuplicateName：重名 → 报错（fail-close：同名两张表落哪张是
// 随机的——"配了不生效"且不报错是最难排查的一类坑）。
func TestProfileConfigsRejectsDuplicateName(t *testing.T) {
	fc := FileConfig{Profiles: []ProfileFileConfig{
		{Name: "fast", Model: "m1"},
		{Name: "fast", Model: "m2"},
	}}
	if _, err := fc.ProfileConfigs(); err == nil {
		t.Fatal("重名 profile 应被拒（fail-close）")
	} else if !strings.Contains(err.Error(), "fast") {
		t.Errorf("错误应点名重名的 profile: %v", err)
	}
}

// TestProfileConfigsRejectsEmptyName：空名 → 拒（没法按名引用）。
func TestProfileConfigsRejectsEmptyName(t *testing.T) {
	fc := FileConfig{Profiles: []ProfileFileConfig{{Name: "  ", Model: "m1"}}}
	if _, err := fc.ProfileConfigs(); err == nil {
		t.Fatal("空名 profile 应被拒（fail-close）")
	}
}

// TestProfileConfigsFoldsFields：折叠到宿主形状（配置面 + 执行面）。
func TestProfileConfigsFoldsFields(t *testing.T) {
	fc := FileConfig{Profiles: []ProfileFileConfig{{
		Name:            " fast ",
		Model:           "m1",
		ReasoningEffort: "low",
		Provider:        ProfileProviderFileConfig{BaseURL: "https://x/v1", APIKeyEnv: "K"},
		Tools:           ProfileToolsFileConfig{Only: []string{"read"}, Except: []string{"bash"}},
		Context:         ProfileContextFileConfig{Kind: "tiered", Window: 512000, L1Pct: 8, L2Pct: 0.5},
		CWD:             " D:/work ",
		Worktree:        "wt",
		RunID:           "run-1",
		RemoteHost:      "build-01",
	}}}
	profiles, err := fc.ProfileConfigs()
	if err != nil {
		t.Fatalf("ProfileConfigs: %v", err)
	}
	p, ok := profiles["fast"]
	if !ok {
		t.Fatalf("折叠后应有名为 fast 的 profile: %v", profiles)
	}
	if p.Model != "m1" || p.ReasoningEffort != "low" {
		t.Errorf("模型面折叠不对: %+v", p)
	}
	if p.Provider.BaseURL != "https://x/v1" || p.Provider.APIKeyEnv != "K" {
		t.Errorf("provider 面折叠不对: %+v", p.Provider)
	}
	if len(p.Tools.Only) != 1 || p.Tools.Only[0] != "read" || len(p.Tools.Except) != 1 {
		t.Errorf("工具白名单折叠不对: %+v", p.Tools)
	}
	if p.Context.Kind != "tiered" || p.Context.Window != 512000 || p.Context.L1Pct != 8 {
		t.Errorf("上下文面折叠不对: %+v", p.Context)
	}
	if p.CWD != "D:/work" || p.Worktree != "wt" || p.RunID != "run-1" || p.RemoteHost != "build-01" {
		t.Errorf("执行面折叠不对: %+v", p)
	}
}

// TestProfileConfigPayloadEmptyMeansNoOverride：profile 的**空字段 = 不覆盖**（模板语义）：
// 展开后的变更集里不出现那些项（出现 = 覆盖；缺失 = 不变）。
func TestProfileConfigPayloadEmptyMeansNoOverride(t *testing.T) {
	p := ProfileConfig{Name: "x"}
	got := profileConfigPayload(p)
	if got.Model != nil || got.ReasoningEffort != nil || got.Provider != nil ||
		got.Tools != nil || got.Context != nil {
		t.Fatalf("空 profile 不该产出任何变更项: %+v", got)
	}
}

// TestOverlayConfigExplicitWinsPerField：叠放次序——**显式逐字段压过** profile：
// model 被压；provider 的 base_url 被压而 api_key_env 保留 profile 的；context 的 window
// 被压而 kind 保留 profile 的；tools 整组被压。
func TestOverlayConfigExplicitWinsPerField(t *testing.T) {
	base := &contracts.ConfigChangedPayload{
		Model:           strp("m-profile"),
		ReasoningEffort: strp("low"),
		Provider: &contracts.ProviderConfigChange{
			BaseURL: strp("https://profile/v1"), APIKeyEnv: strp("PROFILE_KEY"),
		},
		Tools: &contracts.ToolsConfigChange{
			Only: &contracts.ToolNameList{"read"},
		},
		Context: &contracts.ContextConfigChange{
			Kind: strp("tiered"), Window: i64p(512000),
		},
	}
	top := &contracts.ConfigChangedPayload{
		Model: strp("m-explicit"),
		Provider: &contracts.ProviderConfigChange{
			BaseURL: strp("https://explicit/v1"),
		},
		Tools: &contracts.ToolsConfigChange{
			Except: &contracts.ToolNameList{"bash"},
		},
		Context: &contracts.ContextConfigChange{
			Window: i64p(256000),
		},
	}
	got := overlayConfig(base, top)

	if got.Model == nil || *got.Model != "m-explicit" {
		t.Errorf("model 应被显式压过: %v", got.Model)
	}
	if got.ReasoningEffort == nil || *got.ReasoningEffort != "low" {
		t.Errorf("没被显式碰的项应保留 profile 的: %v", got.ReasoningEffort)
	}
	if got.Provider.BaseURL == nil || *got.Provider.BaseURL != "https://explicit/v1" {
		t.Errorf("provider.base_url 应被显式压过: %v", got.Provider.BaseURL)
	}
	if got.Provider.APIKeyEnv == nil || *got.Provider.APIKeyEnv != "PROFILE_KEY" {
		t.Errorf("provider.api_key_env 应保留 profile 的（子字段各自压）: %v", got.Provider.APIKeyEnv)
	}
	if got.Tools.Only != nil {
		t.Errorf("tools 是整组 LWW：显式带 tools 就整组压过 profile 的: %+v", got.Tools)
	}
	if got.Tools.Except == nil || len(*got.Tools.Except) != 1 {
		t.Errorf("tools.except 应取显式的: %+v", got.Tools)
	}
	if got.Context.Kind == nil || *got.Context.Kind != "tiered" {
		t.Errorf("context.kind 应保留 profile 的（子字段各自压）: %v", got.Context.Kind)
	}
	if got.Context.Window == nil || *got.Context.Window != 256000 {
		t.Errorf("context.window 应被显式压过: %v", got.Context.Window)
	}
}

// TestOverlayConfigDoesNotMutateInputs：叠放不改调用方的结构体（副本语义）。
func TestOverlayConfigDoesNotMutateInputs(t *testing.T) {
	base := &contracts.ConfigChangedPayload{Model: strp("m-profile")}
	top := &contracts.ConfigChangedPayload{Model: strp("m-explicit")}
	_ = overlayConfig(base, top)
	if *base.Model != "m-profile" {
		t.Errorf("叠放不该改 base: %v", *base.Model)
	}
	if *top.Model != "m-explicit" {
		t.Errorf("叠放不该改 top: %v", *top.Model)
	}
}

// TestValidateProfilesRejectsInvalidContext：装载期校验——profile 的配置面不合法 → 拒
// （复用写时那道校验，同一口径）。
func TestValidateProfilesRejectsInvalidContext(t *testing.T) {
	c := &Config{Profiles: map[string]ProfileConfig{
		"bad": {Name: "bad", Context: ProfileContextConfig{Window: 100000}}, // < 256k 硬约束
	}}
	if err := c.validateProfiles(); err == nil {
		t.Fatal("不合法 profile 应在装载期被拒（fail-close）")
	}
	c2 := &Config{Profiles: map[string]ProfileConfig{
		"good": {Name: "good", Model: "m", Context: ProfileContextConfig{Kind: "tiered", Window: 512000}},
	}}
	if err := c2.validateProfiles(); err != nil {
		t.Fatalf("合法 profile 不该被拒: %v", err)
	}
	_ = f64p(1)
}

// TestValidateProfilesChecksMapKey：装载期校验按**表键**（= 按名引用的那个）判——
// 空表键拒；Name 字段与表键不一致（程序化构造的错配）也拒。
func TestValidateProfilesChecksMapKey(t *testing.T) {
	// ① 空表键 → 拒。
	c := &Config{Profiles: map[string]ProfileConfig{"": {Name: "x"}}}
	if err := c.validateProfiles(); err == nil {
		t.Fatal("空表键的 profile 应被拒（没法按名引用）")
	}
	// ② Name 与表键不一致 → 拒（按名引用用的是表键）。
	c2 := &Config{Profiles: map[string]ProfileConfig{"fast": {Name: "slow"}}}
	err := c2.validateProfiles()
	if err == nil {
		t.Fatal("Name 与表键不一致应被拒")
	}
	if !strings.Contains(err.Error(), "fast") {
		t.Errorf("错误应点名表键 fast: %v", err)
	}
	// ③ Name 留空（表键就是名字）→ 合法。
	c3 := &Config{Profiles: map[string]ProfileConfig{"fast": {Model: "m"}}}
	if err := c3.validateProfiles(); err != nil {
		t.Fatalf("Name 留空（表键即名字）不该被拒: %v", err)
	}
}
