package host_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	_ "github.com/ncruces/go-sqlite3/driver"
)

// ---------------------------------------------------------------------------
// N2 砖化修复（v3-S3）：崩溃遗留的图投影重建窗口 → 启动期自动恢复
// ---------------------------------------------------------------------------

// graphRebuildHub 起一个**开着图投影**的 hub（复用 newGraphHub 的装配口径）。
func graphRebuildHub(t *testing.T, path string) *host.Hub {
	t.Helper()
	cfg := host.Config{
		DBPath:   path,
		Model:    "scripted",
		System:   "test-system",
		MaxTurns: 4,
		Allow:    []string{"read"},
		CWD:      tempRepoOrDir(t),
		Graph:    true,
	}
	h, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// readRebuildingFlag 读 graph_meta.rebuilding（1 = 处于/曾处于重建窗口）。
func readRebuildingFlag(t *testing.T, path string) int {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("开库: %v", err)
	}
	defer func() { _ = db.Close() }()
	var v int
	if err := db.QueryRow(`SELECT rebuilding FROM graph_meta WHERE id = 1`).Scan(&v); err != nil {
		t.Fatalf("读 graph_meta.rebuilding: %v", err)
	}
	return v
}

// setRebuildingFlag 直接置 graph_meta.rebuilding（模拟"进程死在重建窗口里"）。
func setRebuildingFlag(t *testing.T, path string, v int) {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("开库: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`UPDATE graph_meta SET rebuilding = ? WHERE id = 1`, v); err != nil {
		t.Fatalf("置 rebuilding=%d: %v", v, err)
	}
}

// countHubAlerts 数事件流里 kind=hub.alert 的条数（重建中断/恢复留痕）。
func countHubAlerts(t *testing.T, path string) int {
	t.Helper()
	st, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("开事件库: %v", err)
	}
	defer func() { _ = st.Close() }()
	events, err := st.ReadFrom(context.Background(), nil, 0, 0)
	if err != nil {
		t.Fatalf("读事件流: %v", err)
	}
	n := 0
	for _, ev := range events {
		if ev.Kind == "hub.alert" {
			n++
		}
	}
	return n
}

// TestGraphRebuildCrashLeftoverAutoRecovered 是 **N2 砖化修复**的回归：
// 崩溃遗留的重建窗口（graph_meta.rebuilding=1）必须被**启动期**发现并自动恢复——
//
// 修之前：标记永久留在库里 → 读侧引擎一直 fail-close 报 ErrRebuilding（影响面/冲突预测
// 永久不可用），而 Rebuild 没有生产调用方 → 没人恢复（砖化）。
// 修之后：重开 hub → 启动期自动重跑一次重建 → 标记清 0（读侧恢复作答）+ 事件流留痕
// （hub.alert：中断与恢复各一条）。
func TestGraphRebuildCrashLeftoverAutoRecovered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")

	// ① 先起一次 hub（建图表 + 索引器），再关掉。
	h := graphRebuildHub(t, path)
	_ = h.Close()

	// ② 模拟"进程死在重建窗口里"：直接把 rebuilding 置 1（绕过索引器 = 崩溃现场）。
	setRebuildingFlag(t, path, 1)
	if got := readRebuildingFlag(t, path); got != 1 {
		t.Fatalf("前置条件失败：rebuilding = %d, want 1", got)
	}

	// ③ 重开 hub → 启动期应自动重跑重建。
	h2 := graphRebuildHub(t, path)

	// ④ 标记已被清掉（重建窗口关闭 → 读侧恢复作答）。
	if got := readRebuildingFlag(t, path); got != 0 {
		t.Errorf("启动期自动恢复后 rebuilding 应为 0，实际 %d（重建窗口没关闭 → 影响面永久不可用）", got)
	}
	// 图引擎此刻不再报"正在重建"（读侧恢复正常）。
	if h2 == nil {
		t.Fatal("hub 不应为空")
	}

	// ⑤ 事件流留痕：中断 + 恢复各一条 hub.alert（"重建中断"必须可复盘）。
	if n := countHubAlerts(t, path); n < 2 {
		t.Errorf("hub.alert 条数 = %d, want >= 2（重建中断 + 恢复各留一条）", n)
	}
}

// TestGraphRebuildOpsEntryPoint：运维入口（POST /api/graph/rebuild）能手工重跑重建。
func TestGraphRebuildOpsEntryPoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	h := graphRebuildHub(t, path)
	if err := h.RebuildGraph(context.Background()); err != nil {
		t.Fatalf("RebuildGraph: %v", err)
	}
	if got := readRebuildingFlag(t, path); got != 0 {
		t.Errorf("重建后 rebuilding 应为 0，实际 %d", got)
	}
}
