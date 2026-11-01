package host

import (
	"net/http"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/registry"
)

// 本文件是扩展注册表的**管理/观测面**（v4-5 extension runtime 收口，E2 A 类剩余 +
// E3/E5 做实）：GET /api/extensions 返回注册表全 kind 的快照 + 每条的 Health（只读）。
//
// # 为什么只要只读（E5 健康面 + E3 能力裁决的观测）
//
// 注册表已有的机制（core/registry）：Register / Enable（Adjudicator 裁决 Capabilities，
// fail-close）/ Disable / Unregister / SetHealth。本面把它们**暴露成可观测的快照**：
// 谁装了哪些扩展、什么 kind、什么生命周期状态、申报了哪些能力、健康度如何。
//
// # POST enable/disable 暂不挂（诚实口径）
//
// 任务书把 POST /api/extensions/{name}/enable|disable 列为"可选，挂了要过策略"。
// 这里**只挂 GET**，理由（避免留一个误导性旋钮）：
//
//   - loop 的工具解析（core/loop.resolveTool）按**注册表名字**取执行器，**不 consult
//     注册表的 State**——所以运行期 Disable 一个工具**不会**阻止 loop 调用它。挂一个
//     "disable 后就不能调"的旋钮会是假承诺；
//   - Enable 虽走能力裁决（E3，fail-close），但同样不改变 loop 的可见性/可调性
//     （那由策略链 + agent 类型白名单决定，R5）；
//   - 要让"注册表 State 真正门控执行"需要动 core/loop 的解析路径（让 disabled 条目
//     解析失败），那是超出本刀范围的 core 行为变更，且要与 X1 失败码/重放语义一起想清楚。
//
// 因此：先落**只读快照**（真实有价值：运维/GUI 看得见装了什么、健康如何），POST 写面
// 挂账待"State 门控执行"的 core 变更一并做（否则宁可不给旋钮，也不给假旋钮）。
//
// # 口径分界：State 门控只落在 **MCP server 面**（v4-5 B 类），不落在 loop 面
//
// 上一条说的"loop 侧不 consult State"**仍然成立**（core/loop 的解析路径没动）。但
// **MCP server 面（runtime/mcpserver） consult State**：停用（State=disabled）的工具不进
// tools/list、调用前再查一次（防"list 之后才被停用"）。那是**本包自己的暴露面判定**
// （fail-close：宁可少暴露），**不是** loop 侧的执行门控——两条路互不冒充。

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
// Unregister 掉内置工具）。接口收窄 = 写面留在宿主手里（装配期 host.New + 未来的管理面）。
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
