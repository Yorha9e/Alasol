package host

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"
)

// 本文件实现 K9 的 **hub 级 CPU/内存监控告警**：对冲 goroutine 隔离的代价（OS 级崩溃
// 带走整个 hub / 死循环 goroutine 抢占 / 内存泄漏拖垮 hub）。周期采样 → 超阈值记告警
// （slog 留痕 + 内存有界环 + /api/monitor 可查）。
//
// 可测性设计：采样经 **SampleSource 接口**注入——测试用假源直接返回超阈值样本触发告警，
// **不真烧 CPU**（硬验收要求）。真实采样（内存/goroutine/CPU）在 defaultSampleSource，
// CPU 的累积计时按平台分文件（linux /proc、windows GetProcessTimes、其它尽力而为）。

// monitorLog 是监控告警的出口（可被宿主接管）。
var monitorLog = slog.Default()

// 默认阈值（"明显不对"级别，避免正常多核负载误报；全部可配）：
const (
	// defaultMonitorCPU 进程 CPU%（单核等价，可 >100）。默认 400 ≈ 4 核饱和——
	// hub 正常多核干活不该误报，只有真 runaway 才触发。
	defaultMonitorCPU = 400.0
	// defaultMonitorMemMB 进程内存（MB，runtime 口径）。
	defaultMonitorMemMB = 2048.0
	// defaultMonitorGoroutines goroutine 数（K9：死循环 goroutine 抢占）。
	defaultMonitorGoroutines = 10000
	// defaultMonitorIntervalSec 采样周期（秒）。
	defaultMonitorIntervalSec = 5
	// maxAlerts 告警环大小（有界，防内存泄漏反被监控拖垮）。
	maxAlerts = 50
)

// Sample 是 hub 的一次资源采样快照。
type Sample struct {
	// CPUPercent 进程 CPU 百分比（单核等价，可 >100；不支持的平台为 0）。
	CPUPercent float64
	// MemBytes 进程占用内存字节（runtime.MemStats.Sys 口径）。
	MemBytes uint64
	// Goroutines 采样时刻的 goroutine 数。
	Goroutines int
	// At 采样时刻。
	At time.Time
}

// SampleSource 产出一次采样（可注入：测试用假源，别真烧 CPU）。
type SampleSource interface {
	Sample() Sample
}

// Alert 是一条超阈值告警（只记结论，不含任何凭据/会话内容）。
type Alert struct {
	At        time.Time `json:"at"`
	Metric    string    `json:"metric"`    // "cpu" / "mem" / "goroutines"
	Value     float64   `json:"value"`     // 触发时的实测值
	Threshold float64   `json:"threshold"` // 触发时的阈值
	Message   string    `json:"message"`
}

// monitorConfig 是监控阈值（由 host.Config 的 Monitor* 字段折来）。
type monitorConfig struct {
	CPUPercent  float64
	MemMB       float64
	Goroutines  int
	IntervalSec int
}

// withDefaults 把 0 值折成框架默认（0 = 用默认，与 host.Config.validate 同口径）。
func (c monitorConfig) withDefaults() monitorConfig {
	if c.CPUPercent <= 0 {
		c.CPUPercent = defaultMonitorCPU
	}
	if c.MemMB <= 0 {
		c.MemMB = defaultMonitorMemMB
	}
	if c.Goroutines <= 0 {
		c.Goroutines = defaultMonitorGoroutines
	}
	if c.IntervalSec <= 0 {
		c.IntervalSec = defaultMonitorIntervalSec
	}
	return c
}

// Monitor 周期采样并在超阈值时记告警（K9）。
//
// 并发：mu 保护 latest 与 alerts；采样只在 Monitor 自己的 goroutine 里跑（Start 起、
// Stop 停），Latest/Alerts 可被任意 goroutine 读。
type Monitor struct {
	src      SampleSource
	cfg      monitorConfig
	memLimit uint64 // MemMB → bytes

	mu     sync.Mutex
	latest Sample
	alerts []Alert

	running bool
	stop    chan struct{}
	done    chan struct{}
}

// NewMonitor 建监控器（应用默认阈值）。src 为 nil → 用 defaultSampleSource（真实采样）。
func NewMonitor(cfg monitorConfig, src SampleSource) *Monitor {
	cfg = cfg.withDefaults()
	if src == nil {
		src = &defaultSampleSource{}
	}
	return &Monitor{
		src:      src,
		cfg:      cfg,
		memLimit: uint64(cfg.MemMB * 1024 * 1024),
	}
}

// Start 起后台采样 goroutine（幂等保护由调用方保证：hub 只在 New 起一次）。
// ctx 取消或 Stop 都会让它退出。
func (m *Monitor) Start(ctx context.Context) {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return
	}
	m.running = true
	m.stop = make(chan struct{})
	m.done = make(chan struct{})
	stop, done := m.stop, m.done
	m.mu.Unlock()

	interval := time.Duration(m.cfg.IntervalSec) * time.Second
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		// 起来先采一次（不必等一个周期才有 latest）。
		m.tick()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				m.tick()
			}
		}
	}()
}

// Stop 停后台采样并等待其退出（幂等；防 goroutine 泄漏，-race 下也要干净）。
func (m *Monitor) Stop() {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}
	m.running = false
	close(m.stop)
	done := m.done
	m.mu.Unlock()
	<-done
}

// tick 采一次样、判阈值、记告警。
func (m *Monitor) tick() {
	s := m.src.Sample()
	if s.At.IsZero() {
		s.At = time.Now()
	}
	m.mu.Lock()
	m.latest = s
	m.mu.Unlock()
	m.check(s)
}

// check 对一次采样判三档阈值（cpu/mem/goroutines），超了各记一条告警 + 留痕。
func (m *Monitor) check(s Sample) {
	type breach struct {
		metric    string
		value     float64
		threshold float64
	}
	var breaches []breach
	if s.CPUPercent > m.cfg.CPUPercent {
		breaches = append(breaches, breach{"cpu", s.CPUPercent, m.cfg.CPUPercent})
	}
	if m.memLimit > 0 && float64(s.MemBytes) > float64(m.memLimit) {
		breaches = append(breaches, breach{"mem", float64(s.MemBytes), float64(m.memLimit)})
	}
	if m.cfg.Goroutines > 0 && float64(s.Goroutines) > float64(m.cfg.Goroutines) {
		breaches = append(breaches, breach{"goroutines", float64(s.Goroutines), float64(m.cfg.Goroutines)})
	}
	for _, b := range breaches {
		msg := fmt.Sprintf("hub %s 超阈值: %.1f > %.1f", b.metric, b.value, b.threshold)
		m.record(Alert{At: s.At, Metric: b.metric, Value: b.value, Threshold: b.threshold, Message: msg})
		monitorLog.Warn("K9 hub 监控告警", "metric", b.metric, "value", b.value, "threshold", b.threshold)
	}
}

// record 把告警压入内存环（有界：超过 maxAlerts 丢最旧）。
func (m *Monitor) record(a Alert) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alerts = append(m.alerts, a)
	if len(m.alerts) > maxAlerts {
		m.alerts = m.alerts[len(m.alerts)-maxAlerts:]
	}
}

// Latest 返回最近一次采样（没采过 → 零值）。
func (m *Monitor) Latest() Sample {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.latest
}

// Alerts 返回告警环的副本（最新在后；空 → 无告警）。
func (m *Monitor) Alerts() []Alert {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Alert, len(m.alerts))
	copy(out, m.alerts)
	return out
}

// MonitorView 是 /api/monitor 的形状（当前采样 + 阈值 + 告警环）。
type MonitorView struct {
	Thresholds MonitorThresholds `json:"thresholds"`
	Latest     MonitorSample     `json:"latest"`
	Alerts     []Alert           `json:"alerts"`
}

// MonitorThresholds 回显生效阈值（诊断：当前按什么判）。
type MonitorThresholds struct {
	CPUPercent  float64 `json:"cpu_percent"`
	MemBytes    uint64  `json:"mem_bytes"`
	Goroutines  int     `json:"goroutines"`
	IntervalSec int     `json:"interval_sec"`
}

// MonitorSample 是 JSON 形状的采样（与 Sample 同字段，独立类型以稳定 JSON）。
type MonitorSample struct {
	CPUPercent float64   `json:"cpu_percent"`
	MemBytes   uint64    `json:"mem_bytes"`
	Goroutines int       `json:"goroutines"`
	At         time.Time `json:"at"`
}

// View 折成 /api/monitor 的 JSON 视图。
func (m *Monitor) View() MonitorView {
	m.mu.Lock()
	defer m.mu.Unlock()
	alerts := make([]Alert, len(m.alerts))
	copy(alerts, m.alerts)
	if alerts == nil {
		alerts = []Alert{}
	}
	return MonitorView{
		Thresholds: MonitorThresholds{
			CPUPercent:  m.cfg.CPUPercent,
			MemBytes:    m.memLimit,
			Goroutines:  m.cfg.Goroutines,
			IntervalSec: m.cfg.IntervalSec,
		},
		Latest: MonitorSample{
			CPUPercent: m.latest.CPUPercent,
			MemBytes:   m.latest.MemBytes,
			Goroutines: m.latest.Goroutines,
			At:         m.latest.At,
		},
		Alerts: alerts,
	}
}

// ---------------------------------------------------------------------------
// 默认采样源（真实采样；CPU 累积计时按平台分文件）
// ---------------------------------------------------------------------------

// defaultSampleSource 用 runtime + 平台 CPU 计时采样。CPU% 由**相邻两次采样的累积
// CPU 时间增量 / wall 时间增量**算出（故需持有上次状态，Sample 非并发安全——只被
// Monitor 单 goroutine 调用）。
type defaultSampleSource struct {
	lastCPUNanos int64
	lastWall     time.Time
	hasLast      bool
}

// Sample 采一次：内存（ReadMemStats.Sys）+ goroutine 数 + CPU%（增量）。
func (d *defaultSampleSource) Sample() Sample {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms) //  brief STW；5s 一次可接受
	now := time.Now()

	cpuNanos := cpuTimeNanos() // 平台相关（累计进程 CPU 时间，纳秒）
	var cpuPercent float64
	if d.hasLast {
		dCPU := cpuNanos - d.lastCPUNanos
		dWall := now.Sub(d.lastWall)
		if dWall > 0 && dCPU >= 0 {
			cpuPercent = 100 * float64(dCPU) / float64(dWall.Nanoseconds())
		}
	}
	d.lastCPUNanos = cpuNanos
	d.lastWall = now
	d.hasLast = true

	return Sample{
		CPUPercent: cpuPercent,
		MemBytes:   ms.Sys,
		Goroutines: runtime.NumGoroutine(),
		At:         now,
	}
}
