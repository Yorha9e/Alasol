package host_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/host"
)

// 扩展注册表管理/观测面（v4-5 extension runtime 收口）：GET /api/extensions 返回全 kind
// 快照 + Health（只读）。钉三点：
//   - 内置工具（read/write/bash）以 kind=tool 出现在快照里；
//   - 空能力列表归一为 **[]**（不是 null；与 /api/sessions 同一纪律，F6）；
//   - 只读面：GET 200、方法/路径口径由 /api/ 回退面统一管（此处不断言 405，交给回退面测试）。
//
// "运行期注册一个扩展 → 快照里看得到"那条钉子放在**包内**测试（host/extensions_internal_test.go）：
// Hub.Registry() 只给只读视图（F5：导出接口、不导出可写的 *registry.Registry），包外拿不到
// 注册表的写面——这是有意的收窄，不是漏口。

// TestAPIExtensionsSnapshot 钉死 GET /api/extensions 返回注册表全 kind 快照。
func TestAPIExtensionsSnapshot(t *testing.T) {
	h := newTestHub(t, nil)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	var views []host.ExtensionView
	if code := getJSON(t, srv.URL+"/api/extensions", &views); code != http.StatusOK {
		t.Fatalf("状态码 = %d，want 200", code)
	}

	byName := make(map[string]host.ExtensionView, len(views))
	for _, v := range views {
		byName[v.Name] = v
	}

	// 内置三件套以 kind=tool 出现（RegisterTool 派生的条目；未 Enable → registered）。
	for _, name := range []string{"read", "write", "bash"} {
		v, ok := byName[name]
		if !ok {
			t.Fatalf("内置工具 %q 不在 /api/extensions 快照里（有：%v）", name, keys(byName))
		}
		if v.Kind != registry.ExtensionKindTool {
			t.Fatalf("%q kind = %q, want tool", name, v.Kind)
		}
		if v.State != string(registry.StateRegistered) {
			t.Fatalf("%q state = %q, want registered（内置工具不 Enable）", name, v.State)
		}
		// F6：没申报能力的条目，capabilities 归一为 **[]**（不是 null）——前端拿到 [] 就能
		// 直接遍历，不用判 null（与 /api/sessions 的空表纪律同款）。
		if v.Capabilities == nil {
			t.Fatalf("%q capabilities = null，want []（空值归一，F6）", name)
		}
		if len(v.Capabilities) != 0 {
			t.Fatalf("%q capabilities = %v，want 空", name, v.Capabilities)
		}
	}
}

// TestAPIExtensionsMissingHealthIsNil 钉死：没人回过 Health 的扩展，快照里 Health 为 nil
// （不臆造 up——"未知"与"健康"是两件事，E5 观测要能分辨）。
func TestAPIExtensionsMissingHealthIsNil(t *testing.T) {
	h := newTestHub(t, nil)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	var views []host.ExtensionView
	if code := getJSON(t, srv.URL+"/api/extensions", &views); code != http.StatusOK {
		t.Fatalf("状态码 = %d", code)
	}
	for _, v := range views {
		if v.Name == "read" && v.Health != nil {
			t.Fatalf("read 未回过 Health，快照却给了 %+v（不应臆造）", v.Health)
		}
	}
}

func keys(m map[string]host.ExtensionView) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
