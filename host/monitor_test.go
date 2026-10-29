package host

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
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

// ---------------------------------------------------------------------------
// v2-S5 清账②：K9 告警入事件流（durable 事件 hub.alert）
// ---------------------------------------------------------------------------

// recordingSink 是测试用的告警事件流出口（**不碰真库**：只记下收到的告警）。
type recordingSink struct {
	mu     sync.Mutex
	alerts []Alert
	err    error
	// failN：前 N 次调用失败（之后成功）——用来验"写库失败后会重试"。
	failN int
}

func (s *recordingSink) RecordAlert(_ context.Context, a Alert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failN > 0 {
		s.failN--
		return errors.New("db 炸了")
	}
	if s.err != nil {
		return s.err
	}
	s.alerts = append(s.alerts, a)
	return nil
}

func (s *recordingSink) got() []Alert {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Alert(nil), s.alerts...)
}

// TestMonitorAlertEntersEventStream：超阈值 → 告警**同时**进事件流出口（三个出口齐）。
func TestMonitorAlertEntersEventStream(t *testing.T) {
	sink := &recordingSink{}
	m := NewMonitor(monitorConfig{CPUPercent: 90, MemMB: 100, Goroutines: 1000, IntervalSec: 1},
		&fakeSampleSource{s: Sample{CPUPercent: 95, MemBytes: 200 << 20, Goroutines: 1500}},
		WithAlertSink(sink))
	m.tick()

	got := sink.got()
	if len(got) != 3 {
		t.Fatalf("三项超阈值应各自入一条事件流告警，得到 %d: %+v", len(got), got)
	}
	// 内存环照旧（三个出口都保留，不是替换）。
	if len(m.Alerts()) != 3 {
		t.Fatalf("内存告警环应仍记 3 条: %+v", m.Alerts())
	}
	metrics := map[string]bool{}
	for _, a := range got {
		metrics[a.Metric] = true
		if a.Threshold <= 0 || a.Message == "" {
			t.Fatalf("入流的告警缺契约字段（threshold/message）: %+v", a)
		}
	}
	for _, want := range []string{"cpu", "mem", "goroutines"} {
		if !metrics[want] {
			t.Fatalf("缺 %s 的入流告警: %+v", want, got)
		}
	}
}

// TestMonitorNoSinkNoEventStream：不接 sink = 与 v1 等价（不往事件流写，也不许 panic）。
func TestMonitorNoSinkNoEventStream(t *testing.T) {
	m := NewMonitor(monitorConfig{CPUPercent: 1, MemMB: 1, Goroutines: 1, IntervalSec: 1},
		&fakeSampleSource{s: highSample()})
	m.tick()
	if len(m.Alerts()) != 3 {
		t.Fatalf("无 sink 时内存环照旧: %+v", m.Alerts())
	}
}

// TestMonitorAlertEventSuppression：同一指标的入流告警受抑制窗口约束
// （ runaway 一个指标不该每秒一条事件——把回放和订阅者拖死）。
func TestMonitorAlertEventSuppression(t *testing.T) {
	sink := &recordingSink{}
	src := &fakeSampleSource{s: Sample{CPUPercent: 999}}
	m := NewMonitor(monitorConfig{CPUPercent: 90, IntervalSec: 1}, src, WithAlertSink(sink))
	// 连续 5 个周期全超 → 入流只应有 1 条（窗口内），日志/内存环仍是 5 条。
	for i := 0; i < 5; i++ {
		m.tick()
	}
	if n := len(sink.got()); n != 1 {
		t.Fatalf("抑制窗口内同一指标应只入流 1 条，得到 %d", n)
	}
	if n := len(m.Alerts()); n != 5 {
		t.Fatalf("内存环不该被抑制（活状态要全）: %d", n)
	}
	// 跨过窗口（换一个更晚的采样时刻）→ 放行下一条。
	src.s.At = time.Now().Add(2 * alertEventSuppressSec * time.Second)
	m.tick()
	if n := len(sink.got()); n != 2 {
		t.Fatalf("跨过抑制窗口应放行下一条，得到 %d", n)
	}
	// 换一个指标（mem）不受 cpu 的抑制影响。
	src.s = Sample{CPUPercent: 999, MemBytes: 8 << 30, At: src.s.At}
	m.tick()
	if n := len(sink.got()); n != 3 {
		t.Fatalf("不同指标应各自放行，得到 %d", n)
	}
}

// TestMonitorAlertSinkFailureDoesNotKillMonitor：sink 失败不得影响监控本身
// （监控是兜底机制，它自己不能再变成故障源）。
func TestMonitorAlertSinkFailureDoesNotKillMonitor(t *testing.T) {
	sink := &recordingSink{err: errors.New("db 炸了")}
	m := NewMonitor(monitorConfig{CPUPercent: 1, IntervalSec: 1},
		&fakeSampleSource{s: Sample{CPUPercent: 99}}, WithAlertSink(sink))
	m.tick() // 不该 panic
	if len(m.Alerts()) != 1 {
		t.Fatalf("sink 失败时内存环应仍记: %+v", m.Alerts())
	}
	if m.Latest().CPUPercent != 99 {
		t.Fatalf("sink 失败时采样应照旧: %+v", m.Latest())
	}
}

// TestMonitorAlertEventRetriesAfterSinkFailure（F1 回归）：写库失败**必须释放抑制槽位**——
// 否则这条告警在抑制窗口（60s）内**再也不会重试**（事件流里永远缺这条事实，
// 而日志却写着"稍后会重试"）。
func TestMonitorAlertEventRetriesAfterSinkFailure(t *testing.T) {
	sink := &recordingSink{failN: 1} // 第一次写库失败，之后成功
	src := &fakeSampleSource{s: Sample{CPUPercent: 999}}
	m := NewMonitor(monitorConfig{CPUPercent: 90, IntervalSec: 1}, src, WithAlertSink(sink))

	m.tick() // 第一次：写库失败（没进流）
	if n := len(sink.got()); n != 0 {
		t.Fatalf("第一次写库失败，不该有入流告警: %d", n)
	}
	// 槽位已释放 → 下个周期重新尝试（哪怕还在抑制窗口内）。
	m.tick()
	got := sink.got()
	if len(got) != 1 {
		t.Fatalf("写库失败后应重试入流，得到 %d 条", len(got))
	}
	if got[0].Metric != "cpu" {
		t.Fatalf("重试的应是同一条 cpu 告警: %+v", got[0])
	}
	// 重试成功后抑制窗口重新生效：紧接着再 tick 不该再入流。
	m.tick()
	if n := len(sink.got()); n != 1 {
		t.Fatalf("重试成功后抑制窗口应重新生效，得到 %d 条", n)
	}
}

// TestHubAlertEventEndToEnd：端到端——告警经真事件库落成 hub.alert 事件，可按 kind 读回。
//
// 这是清账②的硬验收：**事件流里看得见**（不是只在日志里）。
func TestHubAlertEventEndToEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub-alert.db")
	st, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	sink := newHubAlertSink(st, path)

	ctx := context.Background()
	at := time.Date(2026, 1, 29, 12, 0, 0, 0, time.UTC)
	if err := sink.RecordAlert(ctx, Alert{
		At: at, Metric: "mem", Value: 3 << 30, Threshold: 2 << 30,
		Message: "hub mem 超阈值: 3221225472.0 > 2147483648.0",
	}); err != nil {
		t.Fatalf("RecordAlert: %v", err)
	}

	// 按 kind 读回（事件流是唯一事实源，F5）。
	var found []contracts.Event
	for {
		evs, err := st.ReadFrom(ctx, nil, int64(len(found))+1, 100)
		if err != nil {
			t.Fatalf("ReadFrom: %v", err)
		}
		if len(evs) == 0 {
			break
		}
		found = append(found, evs...)
	}
	if len(found) != 1 {
		t.Fatalf("应有 1 条 hub.alert 事件，得到 %d", len(found))
	}
	ev := found[0]
	if ev.Kind != KindHubAlert || !ev.Durable || ev.Seq == nil {
		t.Fatalf("hub.alert 必须是 durable 事件且拿到 seq: %+v", ev)
	}
	if ev.V != 1 || ev.TS == "" {
		t.Fatalf("envelope 字段残缺: %+v", ev)
	}
	if !core.ValidUlid(ev.SessionID) || !core.ValidUlid(ev.AgentID) || ev.SessionID == ev.AgentID {
		t.Fatalf("伪归属必须是合法且不相同的 ULID: session=%q agent=%q", ev.SessionID, ev.AgentID)
	}
	// 载荷四要素 + 不含会话内容。
	var p contracts.HubAlertPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if p.Metric != "mem" || p.Value != 3<<30 || p.Threshold != 2<<30 || p.Message == "" {
		t.Fatalf("payload 四要素不对: %+v", p)
	}

	// 确定性：同一库路径 → 同一伪 session（跨重启稳定，告警聚成一条流）。
	sink2 := newHubAlertSink(st, path)
	if sink2.sessionID != sink.sessionID || sink2.agentID != sink.agentID {
		t.Fatalf("同一库路径应派生同一对伪 id（跨重启稳定）")
	}
	// 不同库 → 不同伪 id（多 hub 各自一个库，G25）。
	other := newHubAlertSink(st, path+"-other")
	if other.sessionID == sink.sessionID {
		t.Fatalf("不同库路径应派生不同伪 session")
	}
}

// TestHubPseudoSessionNotInOnlineTable：hub 伪 session 不进在线表（在线表 = agent session），
// 但事件照扫（/ws 与按 kind 的查询看得见）。
//
// 判据是**事件族**（hub.*）而不是"某个派生 id"：伪 id 的派生口径会变（库路径写法不同、
// 规范化口径升级），按 id 登记就会漏；按族判则老库里的残留伪 session 也照样被挡住。
func TestHubPseudoSessionNotInOnlineTable(t *testing.T) {
	p := newProjection(nil)
	// 故意用一个"与当前派生口径无关"的伪 id（模拟旧版本库里的残留）。
	sid := contracts.Ulid(core.NewUlid())
	ev := contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: KindHubAlert, TS: "2026-01-29T12:00:00Z",
		SessionID: sid, AgentID: contracts.Ulid(core.NewUlid()), Durable: true,
		Payload: json.RawMessage(`{"metric":"cpu","value":500,"threshold":400,"message":"x"}`),
	}
	seq := int64(1)
	ev.Seq = &seq
	p.apply(&ev)
	rows := p.rows(1, 0)
	for _, r := range rows {
		if r.SessionID == string(sid) {
			t.Fatalf("伪 session 不该出现在在线表: %+v", rows)
		}
	}
	// 但投影里扫到过它（事件没被丢）——在线表过滤只发生在视图上。
	if len(p.sessions) != 1 {
		t.Fatalf("投影应仍记下该 session（事件照扫）: %+v", p.sessions)
	}
}

// TestHubEventFamilyExcludesOnlyHubSessions：只有 hub.* 事件的 session 不进在线表；
// 真 agent session（哪怕也带一条 hub 事件）照旧在列表里。
func TestHubEventFamilyExcludesOnlyHubSessions(t *testing.T) {
	p := newProjection(nil)
	hubOnly := contracts.Ulid(core.NewUlid())
	mixed := contracts.Ulid(core.NewUlid())
	agentOnly := contracts.Ulid(core.NewUlid())
	mk := func(sid contracts.Ulid, kind string, seq int64) contracts.Event {
		ev := contracts.Event{
			V: 1, ID: core.NewUlid(), Kind: kind, TS: "2026-01-29T12:00:00Z",
			SessionID: sid, AgentID: contracts.Ulid(core.NewUlid()), Durable: true,
			Payload: json.RawMessage(`{}`),
		}
		ev.Seq = &seq
		return ev
	}
	hubEv := mk(hubOnly, KindHubAlert, 1)
	p.apply(&hubEv)
	mixHub := mk(mixed, KindHubAlert, 2)
	p.apply(&mixHub)
	mixAgent := mk(mixed, "agent.completed", 3)
	p.apply(&mixAgent)
	agentEv := mk(agentOnly, "agent.completed", 4)
	p.apply(&agentEv)

	seen := map[string]bool{}
	for _, r := range p.rows(4, 0) {
		seen[r.SessionID] = true
	}
	if seen[string(hubOnly)] {
		t.Fatal("只承载 hub.* 事件的 session 不该出现在在线表")
	}
	if !seen[string(mixed)] {
		t.Fatal("带 agent 事件的 session 该出现在在线表")
	}
	if !seen[string(agentOnly)] {
		t.Fatal("agent session 该出现在在线表")
	}
}
