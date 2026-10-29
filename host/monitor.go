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
// （slog 留痕 + 内存有界环 + /api/monitor 可查 + **事件流入流**，v2-S5 清账②）。
//
// 可测性设计：采样经 **SampleSource 接口**注入——测试用假源直接返回超阈值样本触发告警，
// **不真烧 CPU**（硬验收要求）。真实采样（内存/goroutine/CPU）在 defaultSampleSource，
// CPU 的累积计时按平台分文件（linux /proc、windows GetProcessTimes、其它尽力而为）。
//
// # 告警的三个出口（v2-S5 清账②）
//
//	① slog 留痕（原有，进程日志）；
//	② 内存有界环 + /api/monitor（原有，活状态）；
//	③ **durable 事件 hub.alert**（本刀新增）：告警是事实，进事件流才能回放/审计/被 UI 消费
//	   ——此前只进日志，hub 一重启什么都没了。出口经 AlertSink 接口注入（宿主接 store，
//	   测试接假 sink），**不传 = 与之前完全等价**（老调用方零改动）。

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
	// alertEventSuppressSec 是**同一指标**的告警入事件流的抑制窗口（秒）。
	//
	// 为什么需要：监控是周期的（默认 5s），一个持续 runaway 的指标会**每个周期**
	// 记一条告警——全进事件流就是 720 条/小时/指标，把回放和订阅者全拖死
	// （F12 同款理由：上限是正确性不是优化）。日志与 /api/monitor 仍每个周期都记
	// （活状态该全）。
	//
	// **口径如实说**（F2）：这是**限流**，**不是**"状态切换级"——同一指标在窗口内
	// **最多入流一条**，窗口过后再来一条又放行（哪怕指标一直没回落）。
	// 也就是：持续 runaway 的指标 = **每 60s 一条**事件（不是"一次 runaway 一条"）。
	// 想要"一次 runaway 一条"得改成按**状态切换**记账（进/出阈值各记一条）——
	// 那是行为变更，本刀不做（如实记账在此）。
	alertEventSuppressSec = 60
)

// KindHubAlert 是 K9 告警事件的 kind（durable；契约见 contracts/schema/event.json 的
// HubAlertPayload 与 INVENTORY §2）。
const KindHubAlert = "hub.alert"

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

// AlertSink 是告警的**事件流出流**（K9 → durable 事件，v2-S5 清账②）。
//
// 宿主接事件库（host/alert_event.go 的 hubAlertSink），测试接假 sink（不碰真库）。
// 不接 = 与 v1 等价（告警只进日志 + 内存环）。
type AlertSink interface {
	// RecordAlert 把一条告警落进事件流（durable 事件 hub.alert）。
	//
	// 失败**不得**影响监控本身：调用方只记一条告警日志（告警环与 /api/monitor 仍有效），
	// 不重试、不panic——监控是兜底机制，它自己不能再变成故障源。
	RecordAlert(ctx context.Context, a Alert) error
}

// MonitorOption 配置 Monitor（v2-S5 起引入；老调用方不传选项 = 行为与之前完全一致）。
type MonitorOption func(*monitorOptions)

// monitorOptions 是 Monitor 的可选项（零值 = 全关）。
type monitorOptions struct {
	sink AlertSink
}

// WithAlertSink 把告警接进事件流（K9，v2-S5 清账②）。
//
// s 为 nil 或**不传**这个选项 = 不落事件（与 v1 等价）。s 非 nil 时：
//   - 超阈值仍照旧记 slog + 内存环 + /api/monitor（三个出口都保留）；
//   - 额外落一条 durable 事件 hub.alert（同一指标受 alertEventSuppressSec 抑制窗口约束）。
func WithAlertSink(s AlertSink) MonitorOption {
	return func(o *monitorOptions) { o.sink = s }
}

// Monitor 周期采样并在超阈值时记告警（K9）。
//
// 并发：mu 保护 latest / alerts / lastEventAt / ctx；采样只在 Monitor 自己的 goroutine 里跑
// （Start 起、Stop 停），Latest/Alerts 可被任意 goroutine 读。
type Monitor struct {
	src      SampleSource
	cfg      monitorConfig
	memLimit uint64 // MemMB → bytes
	sink     AlertSink

	mu          sync.Mutex
	latest      Sample
	alerts      []Alert
	lastEventAt map[string]time.Time // 指标 → 最近一次入事件流的时刻（抑制窗口）
	ctx         context.Context      // Start 时记下（事件写入的生命周期；Stop 后取消）

	running bool
	stop    chan struct{}
	done    chan struct{}
}

// NewMonitor 建监控器（应用默认阈值）。src 为 nil → 用 defaultSampleSource（真实采样）。
// opts 是可选项（目前只有告警事件流出口 WithAlertSink）。
func NewMonitor(cfg monitorConfig, src SampleSource, opts ...MonitorOption) *Monitor {
	cfg = cfg.withDefaults()
	if src == nil {
		src = &defaultSampleSource{}
	}
	var o monitorOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &Monitor{
		src:      src,
		cfg:      cfg,
		memLimit: uint64(cfg.MemMB * 1024 * 1024),
		sink:     o.sink,
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
	// 记下生命周期 ctx：告警事件流跟着它走（Stop/ctx 取消后不再写库）。
	m.ctx = ctx
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
		a := Alert{At: s.At, Metric: b.metric, Value: b.value, Threshold: b.threshold, Message: msg}
		m.record(a)
		monitorLog.Warn("K9 hub 监控告警", "metric", b.metric, "value", b.value, "threshold", b.threshold)
		// 事件流出流（v2-S5 清账②）：抑制窗口外的告警才入流。
		if m.claimEventSlot(b.metric, s.At) {
			m.emit(a)
		}
	}
}

// claimEventSlot 占用某指标的事件流入流槽位（抑制窗口内只放行一次）。
//
// 返回 true = 本次该入流。**判定与占位同一把锁**：否则并发采样会重复放行
// （监控只在单 goroutine 里跑，但 Start/Stop 与测试并发读仍可能碰到）。
func (m *Monitor) claimEventSlot(metric string, at time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sink == nil {
		return false
	}
	if m.lastEventAt == nil {
		m.lastEventAt = map[string]time.Time{}
	}
	if last, ok := m.lastEventAt[metric]; ok && at.Sub(last) < alertEventSuppressSec*time.Second {
		return false
	}
	m.lastEventAt[metric] = at
	return true
}

// emit 把一条告警写进事件流（锁外调用：写库可能阻塞，不能占着 mu）。
//
// 失败只记日志——**监控自己不能变成故障源**（告警环与 /api/monitor 仍有效）。
//
// **失败要释放抑制槽位**（F1）：槽位是"占位"（窗口内只放行一次），写库失败却占着
// 不放 = 这条告警在窗口内**再也不会重试**（事件流里就永远缺这条事实，而日志里
// 又写着"稍后会重试"）。释放后下个周期会重新尝试（抑制窗口也随之重新计时）。
func (m *Monitor) emit(a Alert) {
	m.mu.Lock()
	sink, ctx := m.sink, m.ctx
	m.mu.Unlock()
	if sink == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := sink.RecordAlert(ctx, a); err != nil {
		m.releaseEventSlot(a.Metric, a.At)
		monitorLog.Warn("K9 告警入事件流失败（告警环仍有效；已释放抑制槽位，下个周期重试）",
			"metric", a.Metric, "err", err.Error())
	}
}

// releaseEventSlot 释放某指标的抑制槽位（写库失败时调用）。
//
// 只释放**自己占的那一次**（占位时刻相同才释放）——并发下别的采样已经占了新槽位时，
// 不能把它的抑制也一起掀掉（那会让限流形同虚设）。
func (m *Monitor) releaseEventSlot(metric string, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if last, ok := m.lastEventAt[metric]; ok && last.Equal(at) {
		delete(m.lastEventAt, metric)
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
