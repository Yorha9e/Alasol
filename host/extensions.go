package host

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/registry"
)

// 本文件是扩展注册表的**管理/观测面**（v4-5 extension runtime 收口，E2 A 类剩余 +
// E3/E5 做实；缺口 3 补上启停端点）：
//
//   - GET  /api/extensions                → 全 kind 快照 + 每条的 Health（只读观测）；
//   - POST /api/extensions/{name}/enable  → registry.Enable（E3 能力裁决，fail-close）；
//   - POST /api/extensions/{name}/disable → registry.Disable（软停用，可再 enable）。
//
// # 门控前提：State 真正门控两层执行面（缺口 3 之前的"假承诺"已清）
//
// 这两个 POST 之所以敢挂，是因为注册表 State 现在**真的**管住执行（此前挂 = 假旋钮）：
//
//   - **core/loop（主执行路径）双层口径**：第一层 visibleTools 过滤——State=disabled 的
//     条目不进模型可见集（模型看不到就不会调）；第二层执行前复查——**两个时点**：① 判定前
//     （排在策略链**之前**：不问人、不误归因 policy_deny）State=disabled → 直接短路；
//     ② 真正执行前**再读一次**注册表（兜住"ask 人审等待期间被停用"的窗口）→ 走既有失败
//     路径（intent + failed，code=tool_disabled）。与 runtime/mcpserver 的 F1 口径同款
//     （list 不暴露 + call 前复查）；
//   - **runtime/mcpserver（MCP 面）**：tools/list 不暴露停用条目、tools/call 前再查一次
//     （checkUsable，防"list 之后才被停用"）。
//
// 于是 disable 后**立即**生效于两条出口（可见集下一轮刷新 + 执行前复查），enable 恢复。
//
// # enable 是 E3 能力裁决；disable 是软停用
//
//   - enable：meta.Capabilities 逐个交给 Adjudicator 裁决，**任一非 allow → 拒绝**
//     （fail-close：403 点名缺的能力——这不是"永远停用"，补上 allow 规则后可再试）；
//     裁决期间同名扩展被注销并重新注册 → 409（ErrStale：裁决背书的是另一个实例，
//     重注册后需重新 Enable）；裁决期间扩展的**状态被改动**（如 Disable）同样 → 409
//     （ErrStale：写回会吞掉后来的状态 = lost update，需重新 Enable）；
//   - disable：软停用（任何状态都可 Disable，幂等——已停用再停是空操作），可再 enable。
//
// # 两条诚实口径（不建机制，只说清边界）
//
//   - **停用不持久（进程内有效、重启失效）**：停用是**纯内存态**——不落事件流、无审计痕迹；
//     进程重启后所有停用**全部消失**（注册表是进程内活状态，重启即按装配重新注册）。
//     要"重启后仍停用"必须落**契约事件 kind**（持久化语义进事件流）——那是契约改动，
//     本刀**不做**（零契约改动优先），挂账在 11 施工计划。
//   - **管理面无鉴权**：两个 POST 与 /api/approvals/{id}/respond 同类——默认信任本机调用方；
//     鉴权**不自研**（07 已拍板交 SSH/系统，"过策略/鉴权接入"在 07 挂账里）。

// ExtensionView 是 GET /api/extensions 的一个扩展条目快照（只读；不含 Impl 实现体）。
type ExtensionView struct {
	Name         string   `json:"name"`
	Kind         string   `json:"kind"`
	State        string   `json:"state"`
	Reloadable   bool     `json:"reloadable"`
	Capabilities []string `json:"capabilities"`
	Description  *string  `json:"description,omitempty"`
	// Health 是运行时回填的健康状态（E5；nil = 没人回过 = 未知，不臆造 up）。
	Health *contracts.ExtensionHealth `json:"health,omitempty"`
}

// RegistryView 是扩展注册表的**只读视图**（F5：对外导出接口，不导出可写的 *registry.Registry）。
//
// 方法集刻意小：List（按 kind 快照）+ Get（按名取一条）。
//
// **为什么不导出 *registry.Registry**：那等于把注册表的写面（Register / Enable / Disable /
// Unregister / SetHealth）整个交给调用方——"约定只读"靠不住（谁都能 Disable 掉别的扩展、
// Unregister 掉内置工具）。接口收窄 = 写面留在宿主手里（装配期 host.New + 本文件的
// 启停管理端点）。
//
// 附带好处：注册表将来加写方法时，不会顺着这个接口漏出去。
type RegistryView interface {
	// List 按注册顺序返回条目快照；kind 为空 = 全部（core/registry 的同款口径）。
	List(kind string) []registry.Entry
	// Get 按名取条目快照（第二个返回值为 false = 没这条）。
	Get(name string) (registry.Entry, bool)
}

// serveExtensions 服务 GET /api/extensions：注册表全 kind 快照 + Health（只读）。
//
// 返回按注册顺序（registry.List 的顺序来源）的条目数组；kind 为空 = 全部。
// 只读消费 h.reg.List，不触发任何状态变更（观测面，无副作用）。
func (h *Hub) serveExtensions(w http.ResponseWriter, r *http.Request) {
	if h.reg == nil {
		writeJSONBody(w, http.StatusOK, []ExtensionView{})
		return
	}
	entries := h.reg.List("") // 空 kind = 全部
	out := make([]ExtensionView, 0, len(entries))
	for _, e := range entries {
		out = append(out, extensionViewOf(e))
	}
	writeJSONBody(w, http.StatusOK, out)
}

// extensionViewOf 把一条注册表条目折成对外快照（Impl 实现体**不外泄**——只给元数据）。
//
// **空能力列表归一为 []**（F6）：没申报能力的条目（内置工具都是）在 JSON 里给 `[]` 而不是
// `null`——与 /api/sessions 的"空表返回 [] 而不是 null"同一纪律（前端 rows.forEach 不必
// 判 null）。注意 core/registry 的快照会把空切片拷成 nil，故这里按"nil → []"归一。
func extensionViewOf(e registry.Entry) ExtensionView {
	caps := e.Meta.Capabilities
	if caps == nil {
		caps = []string{}
	}
	return ExtensionView{
		Name:         e.Meta.Name,
		Kind:         e.Meta.Kind,
		State:        string(e.State),
		Reloadable:   e.Meta.Reloadable,
		Capabilities: caps,
		Description:  e.Meta.Description,
		Health:       e.Meta.Health,
	}
}

// serveExtensionEnable 服务 POST /api/extensions/{name}/enable——E3 能力裁决启用
// （fail-close：裁决不过就是 403，**不是永远停用**——补上 allow 规则后可再试）。
//
//	200 → {"ok":true}（全部能力获准，State=enabled）
//	403 → 能力裁决未获准（deny / ask / 无命中；错误文案点名缺的能力，extension_capability_denied）
//	404 → 名字未注册（extension_not_found）
//	409 → 裁决期间同名扩展被注销并重新注册（ErrStale：裁决背书的是另一个实例，
//	      需重新 Enable；extension_stale）／裁决期间扩展的**状态被改动**（如 Disable）
//	      同样 ErrStale：写回会吞掉后来的状态 → 需重新 Enable
//	500 → 其余错误（extension_enable_failed）
//
// 停用后重新 Enable 走的就是本端点：State=disabled → 裁决全过 → enabled → 两层门控
// （可见集 + 执行前复查）恢复放行。
//
// # 口径一：名字**字节精确**（与注册表同口径）
//
// 路径里的 {name} **原样**拿去查注册表，**不做 TrimSpace**：注册表的键就是原始字节，
// 端点若先 TrimSpace 再查，就会把 " x " 这种名字错配到 "x" 上（停错扩展 = 停错东西）。
// 只判空（空名 → 400 bad_extension_name）；非空但查无此扩展 → 404。
//
// # 口径二：**无鉴权、默认信任本机调用方**
//
// 本端点与 /api/approvals/{id}/respond 同类：都是**本机管理面**，调用方默认可信。
// 鉴权**不自研**（07 已拍板：鉴权交 SSH/系统——"过策略/鉴权接入"在 07 挂账里，
// 由 SSH/系统层承担；这里建半吊子钩子只会制造假安全感）。
func (h *Hub) serveExtensionEnable(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		writeJSONError(w, http.StatusBadRequest, "bad_extension_name", "扩展名不能为空")
		return
	}
	if h.reg == nil {
		writeJSONError(w, http.StatusNotFound, "extension_not_found", fmt.Sprintf("扩展 %q 未注册", name))
		return
	}
	if err := h.reg.Enable(r.Context(), name); err != nil {
		status, errCode := extensionEnableStatus(err)
		writeJSONError(w, status, errCode, err.Error())
		return
	}
	writeJSONBody(w, http.StatusOK, map[string]any{"ok": true})
}

// extensionEnableStatus 把 Enable 的错误折成 (HTTP 状态码, 错误码)。
//
// 单独成函数是为了可测：ErrStale 要靠"裁决期间注销并重注册"的时序才能真触发
// （registry 测试已钉语义），host 面用合成错误钉这张映射表本身。
//
// 注：**裁决器 panic 落在 403**（E3 fail-close：异常一律折成 deny → 呈现
// "能力未获准"，与"能力真的没获准"**不可区分**）。这是有意为之：panic 是裁决方的
// 内部异常，不是调用方能补救的信号；统一折成 deny = 失败面最小、语义不撒谎
// （"没获准"是真的）。影响面≈0——内建 policy.Engine 不 panic（registry 侧已隔离
// 裁决方 panic 并按 deny 处理）。
func extensionEnableStatus(err error) (int, string) {
	switch {
	case errors.Is(err, registry.ErrNotFound):
		return http.StatusNotFound, "extension_not_found"
	case errors.Is(err, registry.ErrNotAllowed):
		// E3 fail-close：能力未获准 = 403（错误文案里 registry 已点名是哪个能力）。
		// 裁决器 panic 也落这里（fail-close 折成 deny，见本函数头注释）。
		return http.StatusForbidden, "extension_capability_denied"
	case errors.Is(err, registry.ErrStale):
		// P2-5：裁决背书的是被替换掉的旧实例 → 409，调用方重新 Enable。
		// 缺口 3 p2-1：裁决期间状态被改动（Disable）同样是 ErrStale → 409。
		return http.StatusConflict, "extension_stale"
	default:
		return http.StatusInternalServerError, "extension_enable_failed"
	}
}

// serveExtensionDisable 服务 POST /api/extensions/{name}/disable——软停用
// （可再 enable；registry.Disable 幂等：已停用再停是空操作，同样 200）。
//
//	200 → {"ok":true}（State=disabled；重复停用也 200）
//	404 → 名字未注册（extension_not_found）
//	500 → 其余错误（extension_disable_failed）
//
// 停用**立即**生效于两层门控：core/loop 可见集下一轮刷新不再暴露 + 执行前复查直接拒
// （runtime/mcpserver 的 tools/list / tools/call 同款）。
//
// # 口径一：**进程内有效、重启失效**
//
// 停用是**纯内存态**：不落事件流、无审计痕迹；进程重启后所有停用**全部消失**
// （注册表本身是进程内活状态，重启即按装配重新注册 → 全部回到 registered）。
// 要"重启后仍停用"必须落**契约事件 kind**（持久化语义进事件流）——那是契约改动，
// 本刀**不做**（零契约改动优先），挂账在 11 施工计划。
//
// # 口径二：名字**字节精确**（与注册表同口径，不做 TrimSpace；见 serveExtensionEnable）。
//
// # 口径三：**无鉴权、默认信任本机调用方**（与 /api/approvals/{id}/respond 同类既有面；
// 鉴权拍板交 SSH/系统——07 挂账"过策略/鉴权接入"，这里不自研鉴权/策略钩子）。
func (h *Hub) serveExtensionDisable(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		writeJSONError(w, http.StatusBadRequest, "bad_extension_name", "扩展名不能为空")
		return
	}
	if h.reg == nil {
		writeJSONError(w, http.StatusNotFound, "extension_not_found", fmt.Sprintf("扩展 %q 未注册", name))
		return
	}
	if err := h.reg.Disable(name); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "extension_not_found", err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "extension_disable_failed", err.Error())
		return
	}
	writeJSONBody(w, http.StatusOK, map[string]any{"ok": true})
}
