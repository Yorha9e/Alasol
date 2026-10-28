package host

import (
	"context"
	"testing"
	"time"
)

// fakeSampleSource 是测试用的可注入采样源（**不真烧 CPU**）：恒返回给定样本。
type fakeSampleSource struct {
	s Sample
}

func (f *fakeSampleSource) Sample() Sample { return f.s }

// highSample 造一个三项全超默认阈值的样本。
func highSample() Sample {
	return Sample{
		CPUPercent: 999.0, // > 默认 400
		MemBytes:   8 << 30,
		Goroutines: 99999, // > 默认 10000
		At:         time.Now(),
	}
}

// TestMonitorThresholdBreach 钉死 K9 硬验收：超阈值 → 记告警（可注入采样源，不真烧 CPU）。
func TestMonitorThresholdBreach(t *testing.T) {
	m := NewMonitor(monitorConfig{CPUPercent: 90, MemMB: 100, Goroutines: 1000, IntervalSec: 1},
		&fakeSampleSource{s: Sample{CPUPercent: 95, MemBytes: 200 << 20, Goroutines: 1500}})
	// 直接 tick（确定性，不依赖时序）：三项都应超。
	m.tick()

	alerts := m.Alerts()
	if len(alerts) != 3 {
		t.Fatalf("应记 3 条告警（cpu/mem/goroutines），得到 %d: %+v", len(alerts), alerts)
	}
	got := map[string]bool{}
	for _, a := range alerts {
		got[a.Metric] = true
	}
	for _, metric := range []string{"cpu", "mem", "goroutines"} {
		if !got[metric] {
			t.Fatalf("缺 %s 告警: %+v", metric, alerts)
		}
	}
}

// TestMonitorNoBreachNoAlert 验证未超阈值不记告警。
func TestMonitorNoBreachNoAlert(t *testing.T) {
	m := NewMonitor(monitorConfig{CPUPercent: 90, MemMB: 100, Goroutines: 1000, IntervalSec: 1},
		&fakeSampleSource{s: Sample{CPUPercent: 10, MemBytes: 1 << 20, Goroutines: 5}})
	m.tick()
	if len(m.Alerts()) != 0 {
		t.Fatalf("未超阈值不应记告警: %+v", m.Alerts())
	}
}

// TestMonitorDefaults 验证 0 值折成框架默认阈值。
func TestMonitorDefaults(t *testing.T) {
	m := NewMonitor(monitorConfig{}, &fakeSampleSource{})
	if m.cfg.CPUPercent != defaultMonitorCPU || m.cfg.MemMB != defaultMonitorMemMB ||
		m.cfg.Goroutines != defaultMonitorGoroutines || m.cfg.IntervalSec != defaultMonitorIntervalSec {
		t.Fatalf("默认阈值没生效: %+v", m.cfg)
	}
}

// TestMonitorStartStopLifecycle 验证 Start/Stop 生命周期：起来采到样、Stop 干净退出
// （不泄漏 goroutine）。用可取消 ctx + Stop 双保险。
func TestMonitorStartStopLifecycle(t *testing.T) {
	src := &fakeSampleSource{s: Sample{CPUPercent: 1, MemBytes: 2, Goroutines: 3}}
	m := NewMonitor(monitorConfig{IntervalSec: 1}, src)

	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	// Start 里先 tick 一次 → latest 应有样。
	deadline := time.Now().Add(2 * time.Second)
	for m.Latest().Goroutines == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if m.Latest().Goroutines != 3 {
		t.Fatalf("Start 后应采到样: %+v", m.Latest())
	}

	cancel()
	m.Stop()
	m.Stop() // 幂等：再 Stop 不应慌
}

// TestMonitorViewJSON 验证 /api/monitor 视图形状（阈值回显 + 采样 + 告警环）。
func TestMonitorViewJSON(t *testing.T) {
	m := NewMonitor(monitorConfig{CPUPercent: 90, MemMB: 100, Goroutines: 1000, IntervalSec: 2},
		&fakeSampleSource{s: Sample{CPUPercent: 95, MemBytes: 200 << 20, Goroutines: 1500}})
	m.tick()
	v := m.View()
	if v.Thresholds.CPUPercent != 90 || v.Thresholds.MemBytes != 100*1024*1024 {
		t.Fatalf("阈值回显错: %+v", v.Thresholds)
	}
	if v.Latest.CPUPercent != 95 {
		t.Fatalf("latest 采样错: %+v", v.Latest)
	}
	if len(v.Alerts) != 3 {
		t.Fatalf("告警环应有 3 条: %+v", v.Alerts)
	}
}
