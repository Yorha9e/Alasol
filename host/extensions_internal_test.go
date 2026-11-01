package host

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
)

// 本文件是**包内**测试（package host）：扩展注册表的运行期写入面（Register / SetHealth）
// 在包外够不着——Hub.Registry() 只给只读视图（F5：导出接口、不导出可写的 *registry.Registry；
// 与 EventSource 同一纪律）。故"运行期注册一个扩展 → GET /api/extensions 看得到"这条钉子
// 放包内（直接用 h.reg），外部面只钉只读可见的部分。

// TestExtensionsSnapshotRuntimeRegistered 钉死：运行期注册的扩展（申报能力 + 回填 Health）
// 在 GET /api/extensions 快照里如实反映（E3 申报 + E5 回填的观测落点）。
func TestExtensionsSnapshotRuntimeRegistered(t *testing.T) {
	h, err := New(Config{
		DBPath: filepath.Join(t.TempDir(), "events.db"),
		Model:  "scripted",
		CWD:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	desc := "上下文面板扩展"
	if err := h.reg.Register(contracts.ExtensionMeta{
		Name:         "panel.context",
		Kind:         "panel",
		Description:  &desc,
		Capabilities: []string{"fs:read"},
	}, "impl"); err != nil {
		t.Fatalf("Register(panel.context): %v", err)
	}
	msg := "healthy"
	if err := h.reg.SetHealth("panel.context", contracts.ExtensionHealth{
		Status:  contracts.HealthStatusUp,
		Message: &msg,
	}); err != nil {
		t.Fatalf("SetHealth: %v", err)
	}

	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	code, body := getJSONRaw(t, srv.URL+"/api/extensions")
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，want 200（body=%s）", code, body)
	}
	var views []ExtensionView
	if err := json.Unmarshal([]byte(body), &views); err != nil {
		t.Fatalf("解 JSON: %v（body=%s）", err, body)
	}
	byName := make(map[string]ExtensionView, len(views))
	for _, v := range views {
		byName[v.Name] = v
	}

	pv, ok := byName["panel.context"]
	if !ok {
		t.Fatalf("panel.context 不在快照里")
	}
	if pv.Kind != "panel" {
		t.Fatalf("panel.context kind = %q, want panel", pv.Kind)
	}
	if len(pv.Capabilities) != 1 || pv.Capabilities[0] != "fs:read" {
		t.Fatalf("panel.context capabilities = %v, want [fs:read]", pv.Capabilities)
	}
	if pv.Health == nil || pv.Health.Status != contracts.HealthStatusUp {
		t.Fatalf("panel.context health = %+v, want up", pv.Health)
	}
	if pv.Health.Message == nil || *pv.Health.Message != "healthy" {
		t.Fatalf("panel.context health message = %v, want healthy", pv.Health.Message)
	}
	if pv.Description == nil || *pv.Description != desc {
		t.Fatalf("panel.context description = %v", pv.Description)
	}
	// F6：无能力的条目 capabilities 归一为 []（JSON 里不是 null）。
	for _, v := range views {
		if v.Capabilities == nil {
			t.Fatalf("%q capabilities = null，want []（空值归一，F6）", v.Name)
		}
	}
}
