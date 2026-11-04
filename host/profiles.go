package host

// 本文件是 **G-i profile 机制**（命名装配模板）的宿主侧形状与展开逻辑。
//
// profile = 一张**命名的装配模板**："review-fast" = 某个模型 + 某个端点 + 某套工具
// 白名单 + 某个上下文窗口 + 一份工作目录。用户在 POST /prompt（或 WS command.submit）
// 里带 profile="review-fast" → 宿主把它**展开**成一份 config 变更集，再与请求里的显式
// config **叠放**（显式逐字段压过 profile），合成**一条** config.changed 落流——
// 审计里看得见**展开后的值**（不是"profile 名 + 隐式模板"，那种审计看不懂）。
//
// 叠放次序（定稿）：**先展开 profile → 再叠显式 config** → 合成一个 ConfigChangedPayload。
//
// 两张表：
//
//   - **配置面**（model / reasoning_effort / provider / tools / context）→ 展开成
//     config.changed 变更集（本轮生效 + 持久 + 重启生效，生效链同 config.model）。
//   - **执行面**（cwd / worktree / run_id / remote_host 声明）→ 进 SessionSpec
//     （会话创建面；见 discovery.go 的 SessionSpec 与 http.go 的 serveSessionCreate）。
//
// **remote_host 的 v1 边界**（钉死）：本版本**不实现** per-session 远程执行——profile 里的
// remote_host 只是**声明**：**只留痕、不校验**（装载期不判它、它不参与任何判定），会话
// 创建时若与 hub 级 -remote 不符只 WARN，**不改变 hub 级执行环境**（执行器一次性装配，
// per-session 换执行环境属后置——见 host/worktree.go 文件头的同款边界说明）。

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
)

// ProfileProviderConfig 是 profile 的 provider 面（G-d 同款：端点 + 凭据**变量名**）。
type ProfileProviderConfig struct {
	// BaseURL provider 端点（空 = 不覆盖，用 hub 默认）。
	BaseURL string
	// APIKeyEnv 凭据的环境变量名（空 = 不覆盖；**只存名字**，值永不进配置/事件/日志，H15）。
	APIKeyEnv string
}

// ProfileToolsConfig 是 profile 的工具白名单面（G-e 同款）。
type ProfileToolsConfig struct {
	// Only 可见集（空 = 不限）。
	Only []string
	// Except 排除集（空 = 不排除）。
	Except []string
}

// ProfileContextConfig 是 profile 的上下文装配面（G-h 同款：六项旋钮）。
type ProfileContextConfig struct {
	// Kind 上下文策略 kind（"" = 不覆盖；"append" / "tiered" 显式值）。
	Kind string
	// Window 上下文窗口预算 W（token；0 = 不覆盖）。
	Window int64
	// L1Pct / L2Pct L1/L2 目标残留百分比（0 = 不覆盖）。
	L1Pct float64
	L2Pct float64
	// RulesBudget / CommonSenseBudget 注入区 token 上限（0 = 不覆盖）。
	RulesBudget       int64
	CommonSenseBudget int64
}

// ProfileConfig 是一张**命名 profile**（G-i）的宿主级形状。
//
// 一张 profile = 一组 session 配置旋钮（配置面）+ 一组会话创建执行参数（执行面）。
type ProfileConfig struct {
	// Name profile 名（唯一；重名 → 装载期 fail-close）。
	Name string
	// Model 模型名覆盖（空 = 这张 profile 不覆盖模型）。
	Model string
	// ReasoningEffort 思维链档位覆盖（空 = 不覆盖）。
	ReasoningEffort string
	// Provider 端点覆盖（空字段 = 不覆盖）。
	Provider ProfileProviderConfig
	// Tools 工具白名单覆盖（空列表 = 不覆盖）。
	Tools ProfileToolsConfig
	// Context 上下文装配面覆盖（零值 = 不覆盖）。
	Context ProfileContextConfig
	// CWD 工作目录（会话创建面；空 = 用 hub 级默认）。
	CWD string
	// Worktree 绑定的 worktree 名（会话创建面；空 = 不绑）。
	Worktree string
	// RunID 工作流运行 id（会话创建面；空 = 不加 workflow gate）。
	RunID string
	// RemoteHost 远程执行主机的**声明**（v1：**只留痕、不校验**——它既不换 hub 级执行
	// 环境，也不参与任何判定；与 hub 级 -remote 不符时只 WARN，见 http.go 的会话创建面）。
	RemoteHost string
}

// profileConfigPayload 把 profile 的**配置面**折成一份 config 变更集。
//
// **空字段 = 不覆盖**（profile 是模板，不是"清除"指令）：空串 / 0 / 空列表的项**不出现**
// 在变更集里（出现 = 覆盖；缺失 = 不变）。故 profile 里写 model = "" 不会把 hub 默认
// 模型"清空"（清空是显式 config 层的语义：空串 = 清除回默认）。
func profileConfigPayload(p ProfileConfig) *contracts.ConfigChangedPayload {
	out := &contracts.ConfigChangedPayload{}
	if v := strings.TrimSpace(p.Model); v != "" {
		out.Model = &v
	}
	if v := strings.TrimSpace(p.ReasoningEffort); v != "" {
		out.ReasoningEffort = &v
	}
	prov := &contracts.ProviderConfigChange{}
	if v := strings.TrimSpace(p.Provider.BaseURL); v != "" {
		prov.BaseURL = &v
	}
	if v := strings.TrimSpace(p.Provider.APIKeyEnv); v != "" {
		prov.APIKeyEnv = &v
	}
	if prov.BaseURL != nil || prov.APIKeyEnv != nil {
		out.Provider = prov
	}
	if len(p.Tools.Only) > 0 || len(p.Tools.Except) > 0 {
		tools := &contracts.ToolsConfigChange{}
		if len(p.Tools.Only) > 0 {
			list := contracts.ToolNameList(append([]string{}, p.Tools.Only...))
			tools.Only = &list
		}
		if len(p.Tools.Except) > 0 {
			list := contracts.ToolNameList(append([]string{}, p.Tools.Except...))
			tools.Except = &list
		}
		out.Tools = tools
	}
	ctx := &contracts.ContextConfigChange{}
	if v := strings.TrimSpace(p.Context.Kind); v != "" {
		ctx.Kind = &v
	}
	if p.Context.Window != 0 {
		v := p.Context.Window
		ctx.Window = &v
	}
	if p.Context.L1Pct != 0 {
		v := p.Context.L1Pct
		ctx.L1Pct = &v
	}
	if p.Context.L2Pct != 0 {
		v := p.Context.L2Pct
		ctx.L2Pct = &v
	}
	if p.Context.RulesBudget != 0 {
		v := p.Context.RulesBudget
		ctx.RulesBudget = &v
	}
	if p.Context.CommonSenseBudget != 0 {
		v := p.Context.CommonSenseBudget
		ctx.CommonSenseBudget = &v
	}
	if ctx.Kind != nil || ctx.Window != nil || ctx.L1Pct != nil || ctx.L2Pct != nil ||
		ctx.RulesBudget != nil || ctx.CommonSenseBudget != nil {
		out.Context = ctx
	}
	return out
}

// overlayConfig 把**显式**变更集叠到 profile 展开值上（显式逐字段压过 profile）。
//
// 叠放口径（逐字段，**不是**整组替换）：
//
//   - model / reasoning_effort：显式出现（含空串 = 清除）→ 压过 profile；
//   - provider：**子字段各自**压（profile 的 base_url + 显式的 api_key_env 可以并存——
//     凭据纪律由合并后的变更集统一校验）；
//   - tools：**整组**压（tools 组本来就是整组 LWW——显式带 tools 就整组覆盖 profile 的）；
//   - context：**子字段各自**压（context 面的子字段各自 LWW——显式只改窗口不该把
//     profile 的百分比一起打回未设置）。
//
// 返回**新**对象（不改调用方的结构体）。
func overlayConfig(base, top *contracts.ConfigChangedPayload) *contracts.ConfigChangedPayload {
	if base == nil {
		return top
	}
	if top == nil {
		return base
	}
	out := *base
	if top.Model != nil {
		v := *top.Model
		out.Model = &v
	}
	if top.ReasoningEffort != nil {
		v := *top.ReasoningEffort
		out.ReasoningEffort = &v
	}
	if top.Provider != nil {
		prov := &contracts.ProviderConfigChange{}
		if base.Provider != nil {
			*prov = *base.Provider
		}
		if top.Provider.BaseURL != nil {
			v := *top.Provider.BaseURL
			prov.BaseURL = &v
		}
		if top.Provider.APIKeyEnv != nil {
			v := *top.Provider.APIKeyEnv
			prov.APIKeyEnv = &v
		}
		out.Provider = prov
	}
	if top.Tools != nil {
		tools := *top.Tools
		out.Tools = &tools
	}
	if top.Context != nil {
		ctx := &contracts.ContextConfigChange{}
		if base.Context != nil {
			*ctx = *base.Context
		}
		if top.Context.Kind != nil {
			v := *top.Context.Kind
			ctx.Kind = &v
		}
		if top.Context.Window != nil {
			v := *top.Context.Window
			ctx.Window = &v
		}
		if top.Context.L1Pct != nil {
			v := *top.Context.L1Pct
			ctx.L1Pct = &v
		}
		if top.Context.L2Pct != nil {
			v := *top.Context.L2Pct
			ctx.L2Pct = &v
		}
		if top.Context.RulesBudget != nil {
			v := *top.Context.RulesBudget
			ctx.RulesBudget = &v
		}
		if top.Context.CommonSenseBudget != nil {
			v := *top.Context.CommonSenseBudget
			ctx.CommonSenseBudget = &v
		}
		out.Context = ctx
	}
	return &out
}

// validateProfiles 校验 profile 表（装载期 fail-close，G-i）。
//
// 三道闸：
//
//  1. **表键非空**（表键 = 按名引用的那个；空键没法按名引用）；
//  2. **Name 字段与表键一致**（不一致 → 拒：程序化构造的错配会让"按哪个名引用"变成
//     两说——TOML 折叠那步两者同源，这里是最后一道闸）；
//  3. **配置面取值合法**（复用写时那道 validatePromptConfig：kind 白名单 / 数值非负 /
//     窗口硬约束 / 百分比范围与 L2≤L1——**同一口径**，不另抄一份）；
//  4. ~~生效组合（布局）~~ **不在这里判**：布局 = 窗口 − 注入区 − 阶段槽 − 尾部，
//     依赖**真实生效窗口**（宿主默认）与**本次提交的其余覆盖**——判在**写时**（宿主层
//     validateContextEffectiveParams：同一套口径、真实生效窗口）。装载期只判取值
//     （第 3 条）——否则"profile 破布局"会变成**启动拒**，而不是请求期的 400
//     （critic 的第三枚钉要的就是 400）。
//
// **名字唯一**在 TOML 折叠那一步判（ProfileConfigs：同名两张表落哪张是随机的——当场拒）。
//
// remote_host：**只留痕、不校验**（v1 不实现 per-session 远程执行；它既不换执行环境、
// 也不参与任何判定——与 hub 级 -remote 不符时只 WARN）。
func (c *Config) validateProfiles() error {
	for _, key := range sortedProfileNames(c.Profiles) {
		p := c.Profiles[key]
		if strings.TrimSpace(key) == "" {
			return errors.New("host: profile 表里有一张空名（表键是 profile 名，缺名没法按名引用）")
		}
		if p.Name != "" && p.Name != key {
			return fmt.Errorf("host: profile %q 的 Name 字段是 %q，与表键不一致（按名引用用的是表键——"+
				"错配会让\"按哪个名引用\"变成两说，当场拒）", key, p.Name)
		}
		payload := profileConfigPayload(p)
		if err := validatePromptConfig(payload); err != nil {
			return fmt.Errorf("host: profile %q 的配置不合法: %w", key, err)
		}
	}
	return nil
}

// sortedProfileNames 列 profile 名（字典序；诊断文案用）。
func sortedProfileNames(profiles map[string]ProfileConfig) []string {
	out := make([]string, 0, len(profiles))
	for name := range profiles {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
