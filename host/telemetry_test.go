package host_test

// telemetry（L2/D19）的宿主集成钉子：装配 → file 落盘 → 默认关零行为变化；
// 以及配置面 fail-close（mode=file 缺 dir / otlp 留位 / 未知 mode）。
//
// 口径（以 host/telemetry.go 与 runtime/telemetry 为准）：
//   - **独立管道，默认关、脱敏、单向**：关 = 一个文件都不建 = 零行为变化（酸测试）；
//   - mode=file：单文件 `<dir>/telemetry.ndjson` NDJSON；
//   - mode=otlp 留位：装配期拒绝（不静默降级成关）。

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
)

// telHubProvider 是单轮收敛的脚本 provider（一轮就够：begin + end 各一条）。
type telHubProvider struct{}

func (p *telHubProvider) Run(_ context.Context, _ core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	return core.ModelResult{Text: "收尾"}, nil
}

// newTelHub 建一个带 telemetry 的 hub。
func newTelHub(t *testing.T, mode, dir string) *host.Hub {
	t.Helper()
	h, err := host.New(host.Config{
		DBPath:         filepath.Join(t.TempDir(), "tel.db"),
		Model:          "scripted",
		Addr:           "127.0.0.1:0",
		Approval:       "deny-all",
		Provider:       &telHubProvider{},
		WorktreeRoot:   filepath.Join(t.TempDir(), "wts"),
		MaxSessionRows: 10,
		Telemetry:      mode,
		TelemetryDir:   dir,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// TestHubTelemetryOffNoFile（酸测试）：默认关（mode 空）= 一个文件都不建。
func TestHubTelemetryOffNoFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tel")
	h := newTelHub(t, "", dir)
	if _, err := h.RunPrompt(context.Background(), host.NewSession(), "p"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("默认关不该建 telemetry 目录（stat err=%v）", err)
	}
	if h.TelemetryDir() != "" {
		t.Errorf("TelemetryDir() = %q，want 空（未启用）", h.TelemetryDir())
	}
}

// TestHubTelemetryFileWritesNDJSON：mode=file → 跑一轮后 telemetry.ndjson 里有
// run.begin / run.end（值是合法 NDJSON，session_id 归属可辨）。
func TestHubTelemetryFileWritesNDJSON(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tel")
	h := newTelHub(t, "file", dir)
	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "跑一轮"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	path := filepath.Join(dir, "telemetry.ndjson")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("telemetry 落盘文件应存在: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) < 2 {
		t.Fatalf("落盘行数 = %d，want ≥2（run.begin + run.end）", len(lines))
	}
	var kinds []string
	var sawSID bool
	for _, line := range lines {
		var rec core.Record
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("NDJSON 行不可解: %v（%s）", err, line)
		}
		kinds = append(kinds, rec.Kind)
		if rec.SessionID == string(sid) {
			sawSID = true
		}
	}
	if kinds[0] != core.KindTelRunBegin {
		t.Errorf("首条 kind = %q，want %q", kinds[0], core.KindTelRunBegin)
	}
	if kinds[len(kinds)-1] != core.KindTelRunEnd {
		t.Errorf("末条 kind = %q，want %q", kinds[len(kinds)-1], core.KindTelRunEnd)
	}
	if !sawSID {
		t.Errorf("record 应带本次 session_id=%s", sid)
	}
	// Close 后文件仍在（Close 只关 fd，不删文件）。
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("Close 后文件应仍在: %v", err)
	}
}

// TestHubTelemetryFileWithoutDirRejected：mode=file 缺 dir = 配置残缺，启动即拒。
func TestHubTelemetryFileWithoutDirRejected(t *testing.T) {
	_, err := host.New(host.Config{
		DBPath:    filepath.Join(t.TempDir(), "x.db"),
		Model:     "m",
		Addr:      "127.0.0.1:0",
		Approval:  "deny-all",
		Provider:  &telHubProvider{},
		Telemetry: "file", // 无 Dir
	})
	if err == nil || !strings.Contains(err.Error(), "TelemetryDir") {
		t.Fatalf("mode=file 缺 dir 应拒并点名 TelemetryDir，得到 %v", err)
	}
}

// TestHubTelemetryOTLPReservedRejected：mode=otlp 留位，装配期拒绝（不静默降级）。
func TestHubTelemetryOTLPReservedRejected(t *testing.T) {
	_, err := host.New(host.Config{
		DBPath:       filepath.Join(t.TempDir(), "x.db"),
		Model:        "m",
		Addr:         "127.0.0.1:0",
		Approval:     "deny-all",
		Provider:     &telHubProvider{},
		Telemetry:    "otlp",
		TelemetryDir: filepath.Join(t.TempDir(), "t"),
	})
	if err == nil || !strings.Contains(err.Error(), "留位") {
		t.Fatalf("mode=otlp 应拒并说明留位，得到 %v", err)
	}
}

// TestHubTelemetryUnknownModeRejected：未知 mode 启动即拒。
func TestHubTelemetryUnknownModeRejected(t *testing.T) {
	_, err := host.New(host.Config{
		DBPath:    filepath.Join(t.TempDir(), "x.db"),
		Model:     "m",
		Addr:      "127.0.0.1:0",
		Approval:  "deny-all",
		Provider:  &telHubProvider{},
		Telemetry: "jaeger",
	})
	if err == nil || !strings.Contains(err.Error(), "Telemetry") {
		t.Fatalf("未知 mode 应拒并点名 Telemetry，得到 %v", err)
	}
}
