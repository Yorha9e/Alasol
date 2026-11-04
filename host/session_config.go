package host

// 本文件是 per-prompt 配置体系（G-c/G-d/G-e/G-h）的**只读回显面**：
// GET /api/sessions/{session_id}/config —— "该 session 当前**生效**的配置"。
//
// # 定位（GUI 配置面板的种子面）
//
// 配置面板要按 session 预填：当前模型 / 思维链档位 / 端点 / 工具白名单 / 上下文装配 ——
// 这正是**生效链**（hub 默认叠加 session fold）的合成结果。本端点把这条链的**当前态**
// 一次性回显，外加"哪些字段是 session 级覆盖"（overridden_fields）——面板据此把
// "这条是 session 特意改的"标出来。
//
// # 生效值口径（**复用既有口径，不另写一份合并逻辑**）
//
//   - model / reasoning_effort：fold 的**生效覆盖**（HasValue）优先，否则 hub 默认
//     （空串 = 清除回默认，三态口径）；
//   - provider：走 providerOverrideFromFold（= RunOverridesFromConfig 的取值口，
//     同一份实现）——有端点覆盖就回显那对（端点 + 凭据**变量名**），否则回显 hub 级
//     provider；
//   - tools：整组 LWW 的折叠态折成可见性（toolVisibilityFromFold）——**未设置 ≠ 显式
//     空数组**（前者不限、后者一个工具都不暴露）；
//   - context：kind 走 resolveContextKindWith（session 覆盖叠加基线 kind），数值走
//     effectiveContextParams（写时校验与本读面共用同一份合并），再按 compaction 的
//     缺省归一（Params.Normalized）。
//
// # 凭据纪律（H15）
//
// 回显里**只有凭据变量名**（api_key_env），**绝不含 key 值**：fold 与 hub 配置都不存值，
// 值只在解析后交给 provider（见 RunOverridesFromConfig）。
//
// # 存在性判据（404 unknown_session）
//
// 判据 = **事件流里有没有这个 session**（投影口径）——不是"本进程是否托管它"。
// fold 与投影都来自事件流（唯一事实源，F5）：别的进程建的历史 session 一样读得到
// （读面按库投影，一库一 hub 是预期拓扑，G25）；**写面**才管托管。

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/sessioncfg"
)

// SessionConfigView 是 GET /api/sessions/{id}/config 的响应形状（配置面板种子化用）。
//
// 形状（与 GUI 建议一致）：
//
//	{"session_id":"…",
//	 "model":"…","reasoning_effort":"…",
//	 "provider":{"base_url":"…","api_key_env":"…"},
//	 "tools":{"only":[…],"except":[…]},
//	 "context":{"kind":"…","window":…,"l1_pct":…,"l2_pct":…,
//	             "rules_budget":…,"common_sense_budget":…},
//	 "overridden_fields":["model","context.kind",…]}
type SessionConfigView struct {
	// SessionID 被查询的 session id。
	SessionID string `json:"session_id"`
	// Model 当前生效模型（fold 覆盖优先；否则 hub 默认）。
	Model string `json:"model,omitempty"`
	// ReasoningEffort 当前生效的思维链档位（空 = 不传，端点用默认档）。
	ReasoningEffort string `json:"reasoning_effort,omitempty"`
	// Provider 当前生效的 provider（端点 + 凭据**变量名**；**绝不含凭据值**）。
	Provider SessionProviderView `json:"provider"`
	// Tools 当前生效的工具白名单（未设置 = 不限）。
	Tools SessionToolsView `json:"tools"`
	// Context 当前生效的上下文装配面。
	Context SessionContextView `json:"context"`
	// OverriddenFields 是"session 级显式写过"的字段清单（点分路径；空 = 全用 hub 默认）。
	//
	// 恒为数组（空 = [] 不是 null——前端 forEach 不必判空）。
	OverriddenFields []string `json:"overridden_fields"`
}

// SessionProviderView 是生效的 provider 面（端点 + 凭据**变量名**）。
//
// **绝不含 key 值**（H15）：这里只有 api_key_env 这个**名字**——值在运行期解析后
// 直接交给 provider，不进事件 / 日志 / 视图。
type SessionProviderView struct {
	// BaseURL 生效的端点（空 = 官方端点）。
	BaseURL string `json:"base_url,omitempty"`
	// APIKeyEnv 生效的凭据环境变量名（空 = 没有凭据名）。
	APIKeyEnv string `json:"api_key_env,omitempty"`
}

// SessionToolsView 是生效的工具白名单（**三态可见**：未设置 / 显式空 / 有值）。
//
// 用指针承载"未设置 ≠ 显式空数组"这个区别：未设置 = 不限（全部已注册工具可见），
// 显式空数组 = **一个工具都不暴露**——两者语义相反，不能都折成"空"。
type SessionToolsView struct {
	// Only 可见集：nil = 未设置（不限）；[] = 显式清空（一个工具都不暴露）。
	Only *[]string `json:"only,omitempty"`
	// Except 排除集：nil = 未设置（不排除）；[] = 显式清空（不排除任何工具）。
	Except *[]string `json:"except,omitempty"`
}

// SessionContextView 是生效的上下文装配面（G-h：策略 kind + 六项旋钮）。
type SessionContextView struct {
	// Kind 生效的策略 kind："tiered"（两级压缩）| "append"（追加式）。
	//
	// **回显的是协议面的显式取值**：loop 侧的生效 kind 用 "" 表示追加式（见
	// resolveContextKindWith），本视图把 "" 映射回 "append"——面板要看到一个能选的
	// 策略名，而不是"空 = 不知道"。
	Kind string `json:"kind"`
	// Window 生效的上下文窗口预算 W（token）。
	Window int64 `json:"window"`
	// L1Pct / L2Pct 生效的 L1（浅摘要）/ L2（深压缩）目标残留百分比。
	L1Pct float64 `json:"l1_pct"`
	L2Pct float64 `json:"l2_pct"`
	// RulesBudget / CommonSenseBudget 生效的注入区 token 上限。
	RulesBudget       int64 `json:"rules_budget"`
	CommonSenseBudget int64 `json:"common_sense_budget"`
}

// serveSessionConfig 服务 GET /api/sessions/{session_id}/config（配置面板的种子面）。
//
//	200 → SessionConfigView（当前生效配置 + 哪些字段是 session 级覆盖）
//	400 → session_id 不合法（bad_session_id）
//	404 → 事件流里没有这个 session（unknown_session）
//
// **不校验托管**：fold 与投影都来自事件流——别的进程建的历史 session 一样读得到
// （读面按库投影；写面才管托管）。
func (h *Hub) serveSessionConfig(w http.ResponseWriter, r *http.Request) {
	sid, err := parseSessionID(r.PathValue("session_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_session_id", err.Error())
		return
	}
	view, ok, err := h.sessionConfigView(r.Context(), sid)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "session_config_failed", err.Error())
		return
	}
	if !ok {
		writeJSONError(w, http.StatusNotFound, "unknown_session",
			fmt.Sprintf("session %s 不在本宿主的事件流里（配置来自事件流：没见过它就没有它的配置）", sid))
		return
	}
	writeJSONBody(w, http.StatusOK, view)
}

// sessionConfigView 取该 session 的当前生效配置 + 覆盖字段清单。
//
// ok=false = 事件流里没见过这个 session → 404 unknown_session（不编造"全默认"答案：
// 一个从没存在过的 id 报"生效配置 = 默认"是**假答案**，比 404 更坏）。
func (h *Hub) sessionConfigView(ctx context.Context, sid contracts.Ulid) (SessionConfigView, bool, error) {
	if h == nil {
		return SessionConfigView{}, false, errors.New("host: Hub 为 nil")
	}
	// ① 投影扫平：投影是"事件流里见过哪些 session"的目录（判据 = 事实流）。
	if h.proj != nil {
		if _, err := h.proj.catchUp(ctx); err != nil {
			return SessionConfigView{}, false, fmt.Errorf("host: 扫事件流（session 目录）: %w", err)
		}
		if !h.proj.hasSession(sid) {
			return SessionConfigView{}, false, nil
		}
	}
	// ② fold 扫平：配置 fold 同样来自事件流（写路径落的 config.changed 在此进 fold）。
	if _, err := h.cfgFeed.catchUp(ctx); err != nil {
		return SessionConfigView{}, false, fmt.Errorf("host: 扫配置事件流（G-c 生效链）: %w", err)
	}
	return h.effectiveSessionConfig(sid, h.cfgTable.Current(sid)), true, nil
}

// effectiveSessionConfig 把 fold 态 + hub 默认合成"当前生效配置"（+ 覆盖字段清单）。
//
// 口径见文件头（各面都走既有取值口，不另写一份合并逻辑）。
func (h *Hub) effectiveSessionConfig(sid contracts.Ulid, cur sessioncfg.SessionConfig) SessionConfigView {
	cfg := h.cfg
	view := SessionConfigView{
		SessionID:       string(sid),
		Model:           pickEffectiveString(cur.Model, cfg.Model),
		ReasoningEffort: pickEffectiveString(cur.ReasoningEffort, cfg.ReasoningEffort),
		// overridden_fields 恒为数组（空 = [] 不是 null）。
		OverriddenFields: overriddenFieldsOf(cur),
	}
	// provider：走 RunOverridesFromConfig 的取值口（providerOverrideFromFold）——
	// 有端点覆盖就回显那对（端点 + 凭据变量名），否则回显 hub 级 provider。
	if ov, ok := providerOverrideFromFold(cur); ok {
		view.Provider = SessionProviderView{BaseURL: ov.BaseURL, APIKeyEnv: ov.APIKeyEnv}
	} else {
		view.Provider = SessionProviderView{
			BaseURL:   strings.TrimSpace(cfg.BaseURL),
			APIKeyEnv: strings.TrimSpace(cfg.APIKeyEnv),
		}
	}
	// tools：整组 LWW 的折叠态 → 可见性（toolVisibilityFromFold）。
	//
	// **未设置 ≠ 显式空数组**：只有 Written 的子项才出现在视图里（显式空数组 = []，
	// 语义是"一个工具都不暴露"；未设置 = 不限）。
	if cur.Tools.Touched {
		vis := toolVisibilityFromFold(cur.Tools)
		if cur.Tools.Only.Written {
			v := vis.Only
			view.Tools.Only = &v
		}
		if cur.Tools.Except.Written {
			v := vis.Except
			view.Tools.Except = &v
		}
	}
	// context：kind = session 覆盖叠加基线 kind（resolveContextKindWith；基线 = 宿主
	// 默认 / 工作流声明）；数值 = 写时校验同一份合并（effectiveContextParams），
	// 再按 compaction 的缺省归一（Normalized：窗口 0 → 1M、注入区预算 0 → 布局默认）。
	mergedKind := ""
	if cur.Context.Kind.HasValue() {
		mergedKind = cur.Context.Kind.Value
	}
	kind := resolveContextKindWith(mergedKind, h.contextBaseKind(sid))
	if kind == "" {
		kind = contextStrategyAppend // loop 侧的 "" = 追加式；视图回显协议面的显式取值
	}
	params := effectiveContextParams(cfg.contextStrategyParams(), contextParamsFromFold(cur.Context)).Normalized()
	view.Context = SessionContextView{
		Kind:              kind,
		Window:            params.Window,
		L1Pct:             params.L1TargetPct,
		L2Pct:             params.L2TargetPct,
		RulesBudget:       params.RulesBudget,
		CommonSenseBudget: params.CommonSenseBudget,
	}
	return view
}

// pickEffectiveString 取"fold 有生效覆盖就用它，否则用 hub 默认"的字符串值（三态口径）。
//
// 空串 = 显式清除 → 生效上回默认（HasValue 为 false）；非空 = 覆盖值。
func pickEffectiveString(f sessioncfg.Field, def string) string {
	if f.HasValue() {
		return f.Value
	}
	return def
}

// overriddenFieldsOf 列 fold 里"显式写过"的字段（点分路径；确定性顺序）。
//
// 三态口径：**Written 即算"覆盖过"**——写过空串 / 0（清除）也算（审计上"曾覆盖过、
// 后来被清掉"是事实，面板据此标出"这条被 session 改过"）。
//
// 清单 = 配置面的全部可覆盖字段：model / reasoning_effort / provider 两项 / tools 两项 /
// context 六项。顺序固定（声明序）——不给前端"每次刷新顺序不一样"的抖动。
func overriddenFieldsOf(cur sessioncfg.SessionConfig) []string {
	out := []string{}
	if cur.Model.Written {
		out = append(out, "model")
	}
	if cur.ReasoningEffort.Written {
		out = append(out, "reasoning_effort")
	}
	if cur.Provider.BaseURL.Written {
		out = append(out, "provider.base_url")
	}
	if cur.Provider.APIKeyEnv.Written {
		out = append(out, "provider.api_key_env")
	}
	if cur.Tools.Only.Written {
		out = append(out, "tools.only")
	}
	if cur.Tools.Except.Written {
		out = append(out, "tools.except")
	}
	if cur.Context.Kind.Written {
		out = append(out, "context.kind")
	}
	if cur.Context.Window.Written {
		out = append(out, "context.window")
	}
	if cur.Context.L1Pct.Written {
		out = append(out, "context.l1_pct")
	}
	if cur.Context.L2Pct.Written {
		out = append(out, "context.l2_pct")
	}
	if cur.Context.RulesBudget.Written {
		out = append(out, "context.rules_budget")
	}
	if cur.Context.CommonSenseBudget.Written {
		out = append(out, "context.common_sense_budget")
	}
	return out
}
