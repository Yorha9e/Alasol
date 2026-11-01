package loop_test

// WAL 预写日志（v4-4-B）的 loop 层钉子：五个写点的序列 / 逐条 delta / 写失败不阻断 /
// blob.plan 在 Put 之前 / 关 = 零行为变化。
//
// 口径（全文以 core/wal.go 与 core/loop/wal.go 为准）：
//   - **WAL 不是事实源**（F5 不受影响）：事件流仍是唯一事实源，WAL 是崩溃恢复的补充面，
//     可丢弃/可截断/可 GC；恢复报告是观察面不是写回面（loop 侧只负责**写**）。
//   - 五个写点全在 X1 三段写路径**之外**（intent→completed/failed 的落盘序一字不动）。
//   - **关 = 零行为变化**：WalSink 为 nil 时一个字节都不多写、行为逐事件等价（酸测试）。

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/storage"
)

// recWAL 是测试用的内存 core.WalSink：按序记录 (kind, payload JSON)，可注入失败。
type recWAL struct {
	mu      sync.Mutex
	kinds   []string
	raws    []string
	failOn  string // 非空时该 kind 追加失败
	failErr error
	failed  int // 实际失败次数（断言"每条都试过"用）
}

func (w *recWAL) Append(kind string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failOn != "" && kind == w.failOn {
		w.failed++
		if w.failErr != nil {
			return w.failErr
		}
		return errors.New("recWAL: 注入的写入失败")
	}
	w.kinds = append(w.kinds, kind)
	w.raws = append(w.raws, string(raw))
	return nil
}

func (w *recWAL) snapshot() ([]string, []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.kinds...), append([]string(nil), w.raws...)
}

func (w *recWAL) failures() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failed
}

// payloadsOf 取某个 kind 的所有 payload（按序解成 map，供字段断言）。
func payloadsOf(t *testing.T, w *recWAL, kind string) []map[string]any {
	t.Helper()
	kinds, raws := w.snapshot()
	var out []map[string]any
	for i, k := range kinds {
		if k != kind {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raws[i]), &m); err != nil {
			t.Fatalf("WAL %s payload 不可解: %v（%s）", kind, err, raws[i])
		}
		out = append(out, m)
	}
	return out
}

// walSinkFunc 把函数适配成 core.WalSink（测试用）。
type walSinkFunc func(kind string, payload any) error

func (f walSinkFunc) Append(kind string, payload any) error { return f(kind, payload) }

// walDeltaProvider 是流式假 provider：每轮发 n 条 text delta + 1 条 reasoning delta；
// withToolCall 时第一轮额外带一次工具调用（好让"一轮多次 delta"与多轮都出现）。
type walDeltaProvider struct {
	deltas       []string
	withToolCall bool
	calls        int
}

func (p *walDeltaProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	for _, d := range p.deltas {
		emit(core.ProviderEvent{Kind: core.KindTextDelta, Text: d})
	}
	emit(core.ProviderEvent{Kind: core.KindReasoningDelta, Reasoning: "想一想"})
	if p.withToolCall && p.calls == 1 {
		tc := core.ToolCallReq{ID: contracts.ProviderId("tc_WAL_1"), Name: "echo", Args: json.RawMessage(`{"text":"hi"}`)}
		emit(core.ProviderEvent{Kind: core.KindToolCallStart, ToolCall: &tc})
		emit(core.ProviderEvent{Kind: core.KindDone})
		return core.ModelResult{Text: "调工具", ToolCalls: []core.ToolCallReq{tc}}, nil
	}
	emit(core.ProviderEvent{Kind: core.KindDone, Text: "收尾"})
	return core.ModelResult{Text: "收尾"}, nil
}

// TestWALWritePointSequence（F1）：turn.begin →（delta）→ turn.end 的序列正确，多轮各自成对、
// 轮次号递增；turn.begin 的载荷带模型名与消息/工具条数。
func TestWALWritePointSequence(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	wal := &recWAL{}
	prov := &walDeltaProvider{deltas: []string{"你", "好", "，", "世界"}, withToolCall: true}
	h.script(
		scriptStep{result: core.ModelResult{Text: "调工具", ToolCalls: []core.ToolCallReq{call("tc_WAL_1", "echo", `{"text":"hi"}`)}}},
		scriptStep{result: core.ModelResult{Text: "收尾"}},
	)
	lp := h.loop(func(c *loop.Config) {
		c.WalSink = wal
		c.Provider = prov
	})

	if _, err := lp.Run(context.Background(), "两轮"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	kinds, _ := wal.snapshot()
	want := []string{
		"turn.begin", "turn.delta", "turn.delta", "turn.delta", "turn.delta", "turn.delta", "turn.end", // 第 1 轮
		"turn.begin", "turn.delta", "turn.delta", "turn.delta", "turn.delta", "turn.delta", "turn.end", // 第 2 轮
	}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Fatalf("WAL kind 序列 = %v\nwant %v", kinds, want)
	}
	begins := payloadsOf(t, wal, "turn.begin")
	ends := payloadsOf(t, wal, "turn.end")
	if len(begins) != 2 || len(ends) != 2 {
		t.Fatalf("begin/end 条数 = %d/%d，want 2/2", len(begins), len(ends))
	}
	for i := 0; i < 2; i++ {
		if got := begins[i]["turn"].(float64); int(got) != i+1 {
			t.Errorf("begin[%d].turn = %v，want %d", i, begins[i]["turn"], i+1)
		}
		if got := ends[i]["turn"].(float64); int(got) != i+1 {
			t.Errorf("end[%d].turn = %v，want %d", i, ends[i]["turn"], i+1)
		}
		if begins[i]["model"] != "test-model" {
			t.Errorf("begin[%d].model = %v，want test-model", i, begins[i]["model"])
		}
		// 第 1 轮消息 = user + assistant + tool（3 条），第 2 轮再多一条 assistant/tool。
		if n, ok := begins[i]["messages"].(float64); !ok || int(n) < 1 {
			t.Errorf("begin[%d].messages = %v，应 ≥1", i, begins[i]["messages"])
		}
	}
	// 第 1 轮 end 带 usage/tool_calls；第 2 轮（收敛）tool_calls=0。
	if ends[1]["tool_calls"].(float64) != 0 {
		t.Errorf("收敛轮 end.tool_calls = %v，want 0", ends[1]["tool_calls"])
	}
	if ends[0]["tool_calls"].(float64) != 1 {
		t.Errorf("工具轮 end.tool_calls = %v，want 1", ends[0]["tool_calls"])
	}
}

// TestWALRecordsEveryDeltaFragment（F2，本刀最大价值）：一轮 4 条 text + 1 条 reasoning
// delta **全部逐条**落 WAL——model.delta 是 durable=false 的流事件（不入库、不回放），
// 崩溃后本轮输出整轮蒸发；WAL 至少多保存一些。
func TestWALRecordsEveryDeltaFragment(t *testing.T) {
	h := newHarness(t)
	wal := &recWAL{}
	// 单轮（不发工具调用）→ 收敛在最后一轮，delta 条数才可数。
	prov := &walDeltaProvider{deltas: []string{"你", "好", "，", "世界"}}
	lp := h.loop(func(c *loop.Config) {
		c.WalSink = wal
		c.Provider = prov
	})
	if _, err := lp.Run(context.Background(), "打招呼"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	deltas := payloadsOf(t, wal, "turn.delta")
	if len(deltas) != 5 {
		t.Fatalf("turn.delta = %d 条，want 5（4 text + 1 reasoning）", len(deltas))
	}
	var text, reasoning string
	for _, d := range deltas {
		if d["turn"].(float64) != 1 {
			t.Errorf("delta turn = %v，want 1", d["turn"])
		}
		if v, ok := d["text"].(string); ok {
			text += v
		}
		if v, ok := d["reasoning"].(string); ok {
			reasoning += v
		}
	}
	if text != "你好，世界" {
		t.Errorf("delta 正文拼接 = %q，want %q", text, "你好，世界")
	}
	if reasoning != "想一想" {
		t.Errorf("delta 思维链拼接 = %q，want %q", reasoning, "想一想")
	}
	// delta 夹在 begin 与 end 之间（顺序正确）。
	kinds, _ := wal.snapshot()
	if kinds[0] != "turn.begin" || kinds[len(kinds)-1] != "turn.end" {
		t.Fatalf("WAL 首尾应为 turn.begin/turn.end，got %v", kinds)
	}
}

// TestWALTurnEndAfterModelTurn（F3）：turn.end 写在 model.turn **durable 落盘之后**
// （顺序有意：反过来写会把"两者之间崩溃"报告成干净收场 = 假阴性）。
//
// 证法：WAL sink 收到 turn.end 时**回读事件流**——那一刻 model.turn 必须已在库里。
func TestWALTurnEndAfterModelTurn(t *testing.T) {
	h := newHarness(t)
	h.script(scriptStep{result: core.ModelResult{Text: "一句话"}})

	var (
		seenModelTurn bool
		checked       bool
	)
	probe := walSinkFunc(func(kind string, _ any) error {
		if kind != "turn.end" || checked {
			return nil
		}
		checked = true
		for _, ev := range h.events() {
			if ev.Kind == loop.KindModelTurn {
				seenModelTurn = true
			}
		}
		return nil
	})
	if _, err := h.loop(func(c *loop.Config) { c.WalSink = probe }).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !checked {
		t.Fatal("没收到 turn.end（写点没接上？）")
	}
	if !seenModelTurn {
		t.Fatal("turn.end 写入时事件流里还没有 model.turn——顺序错了（应 model.turn 先落盘）")
	}
}

// orderBlob 是记录 Put 时机的 fake blob（顺序断言用；正常返回一个可用引用）。
type orderBlob struct {
	onPut func()
}

func (b *orderBlob) Put(context.Context, storage.BlobMeta, io.Reader) (storage.BlobMeta, error) {
	if b.onPut != nil {
		b.onPut()
	}
	return storage.BlobMeta{ID: core.NewUlid(), Size: 1}, nil
}

func (b *orderBlob) Get(context.Context, contracts.Ulid) (io.ReadCloser, error) {
	return nil, storage.ErrNotFound
}

func (b *orderBlob) Stat(context.Context, contracts.Ulid) (storage.BlobMeta, error) {
	return storage.BlobMeta{}, storage.ErrNotFound
}

func (b *orderBlob) Delete(context.Context, contracts.Ulid) error { return nil }

// indexOfEntry 在顺序日志里找第一处等于 needle 的下标（找不到 → -1）。
func indexOfEntry(entries []string, needle string) int {
	for i, e := range entries {
		if e == needle {
			return i
		}
	}
	return -1
}

// TestWALBlobPlanBeforeIntent（F1，钉子改）：blob.plan 写在 **tool.call.intent 落盘之前**
// ——写点全在 X1 三段窗口**之外**（intent→completed 之间插写点 = 关键路径上多一次落盘，
// 还让"写点全在三段路径外"这句话不成立）。
//
// 证法：WAL 追加、事件入库、blob.Put 三者往**同一条顺序日志**记一笔 → 直接比先后。
// 钉子：把写点挪回 intent 之后（externalizeResult 里）→ 本用例红。
func TestWALBlobPlanBeforeIntent(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	big := strings.Repeat("W", 5000)
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &big}}}

	rec := &recWAL{}
	wal := walSinkFunc(func(kind string, payload any) error {
		if kind == core.KindWalBlobPlan {
			h.log.add("wal:" + core.KindWalBlobPlan)
		}
		return rec.Append(kind, payload)
	})
	blob := &orderBlob{onPut: func() { h.log.add("blob:put") }}
	h.script(
		scriptStep{result: core.ModelResult{Text: "调工具", ToolCalls: []core.ToolCallReq{call("tc_WAL_BLOB", "echo", `{"text":"hi"}`)}}},
		scriptStep{result: core.ModelResult{Text: "完成"}},
	)
	if _, err := h.loop(func(c *loop.Config) {
		c.WalSink = wal
		c.BlobSink = blob
		c.BlobMaxInline = 64 // 小阈值 → 5000 字节必外置
	}).Run(context.Background(), "echo"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	plans := payloadsOf(t, rec, core.KindWalBlobPlan)
	if len(plans) != 1 {
		t.Fatalf("blob.plan = %d 条，want 1", len(plans))
	}
	if plans[0]["tool_call_id"] != "tc_WAL_BLOB" {
		t.Errorf("blob.plan.tool_call_id = %v，want tc_WAL_BLOB（对账键）", plans[0]["tool_call_id"])
	}
	if plans[0]["turn"].(float64) != 1 {
		t.Errorf("blob.plan.turn = %v，want 1", plans[0]["turn"])
	}
	// 顺序：blob.plan **早于** intent（写点在 X1 窗口之外），且仍早于 blob.Put。
	entries := h.log.all()
	planAt := indexOfEntry(entries, "wal:"+core.KindWalBlobPlan)
	intentAt := indexOfEntry(entries, "intent:tc_WAL_BLOB")
	putAt := indexOfEntry(entries, "blob:put")
	if planAt < 0 || intentAt < 0 || putAt < 0 {
		t.Fatalf("顺序日志缺格子（plan=%d intent=%d put=%d）：%v", planAt, intentAt, putAt, entries)
	}
	if planAt > intentAt {
		t.Errorf("blob.plan 应早于 tool.call.intent（X1 三段窗口之外），got %v", entries)
	}
	if planAt > putAt {
		t.Errorf("blob.plan 应早于 blob.Put，got %v", entries)
	}
	// 计划写在 intent 前 → 结果体量未知（0）：不编造一个没测出来的数。
	if n, ok := plans[0]["result_bytes"].(float64); !ok || n != 0 {
		t.Errorf("blob.plan.result_bytes = %v，want 0（计划时结果尚未产生）", plans[0]["result_bytes"])
	}
}

// TestWALBlobPlanAbsentWhenBlobOff（F1 的另一半，零行为变化）：**没挂 blob**（外置关）→
// 不写 blob.plan（没有外置就没有计划可写；关 = 一个字节都不多写）。
func TestWALBlobPlanAbsentWhenBlobOff(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	rec := &recWAL{}
	h.script(
		scriptStep{result: core.ModelResult{Text: "调工具", ToolCalls: []core.ToolCallReq{call("tc_NOBLOB", "echo", `{"text":"hi"}`)}}},
		scriptStep{result: core.ModelResult{Text: "完成"}},
	)
	if _, err := h.loop(func(c *loop.Config) { c.WalSink = rec }).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if plans := payloadsOf(t, rec, core.KindWalBlobPlan); len(plans) != 0 {
		t.Errorf("没挂 blob 时不该有 blob.plan，got %d 条", len(plans))
	}
}

// TestWALWriteFailureDoesNotBlockRun（F5）：WAL 写失败 **不阻断 Run**（尽力而为），事件流
// 照常完整落库；失败**留痕**（恰好一条 Warn——不是零条，也不是每条都刷）。
func TestWALWriteFailureDoesNotBlockRun(t *testing.T) {
	// 捕获 slog（walWarn 动态取 slog.Default()，故此处替换能生效）。
	prev := slog.Default()
	cap := &captureLogger{}
	slog.SetDefault(slog.New(cap))
	defer slog.SetDefault(prev)

	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	wal := &recWAL{failOn: "turn.begin", failErr: errors.New("disk full")}
	h.script(
		scriptStep{result: core.ModelResult{Text: "调工具", ToolCalls: []core.ToolCallReq{call("tc_WAL_FAIL", "echo", `{"text":"hi"}`)}}},
		scriptStep{result: core.ModelResult{Text: "照常跑完"}},
	)
	lp := h.loop(func(c *loop.Config) { c.WalSink = wal })

	res, err := lp.Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("WAL 写失败不该阻断 Run，得到错误: %v", err)
	}
	if res.Text != "照常跑完" {
		t.Errorf("Result.Text = %q，want 照常跑完", res.Text)
	}
	// 事件流照常完整（user.prompt / context.assembled / model.turn）。
	var sawPrompt, sawTurn bool
	for _, ev := range h.events() {
		switch ev.Kind {
		case loop.KindUserPrompt:
			sawPrompt = true
		case loop.KindModelTurn:
			sawTurn = true
		}
	}
	if !sawPrompt || !sawTurn {
		t.Fatalf("WAL 写失败后事件流仍应完整（user.prompt=%v model.turn=%v）", sawPrompt, sawTurn)
	}
	if n := cap.countWarn(); n != 1 {
		t.Fatalf("WAL 失败告警 = %d 条，want 1（必须留痕，且只留一次）", n)
	}
	// 每条都**试过**（不是第一次失败就整体放弃：不阻断 ≠ 不再试）。
	if wal.failures() < 2 {
		t.Errorf("失败次数 = %d，want ≥2（两轮各试一次）", wal.failures())
	}
}

// TestWALTurnFailedOnProviderError（F2）：provider 返错 → 写 **turn.failed**（不是 turn.end）。
//
// 为什么：模型调用失败是**普通失败**，不是崩溃。没有 turn.failed 的话，WAL 里留下悬空的
// turn.begin，对账会把它当成"崩溃点"（误报"疑似崩溃残留"：使用者据此以为丢了东西，其实
// 只是模型报错）。钉子：去掉 turn.failed 写入 → 对账侧的"普通失败不算在飞"钉子红。
func TestWALTurnFailedOnProviderError(t *testing.T) {
	h := newHarness(t)
	rec := &recWAL{}
	h.script(scriptStep{err: errors.New("provider: 端点 500")})

	if _, err := h.loop(func(c *loop.Config) { c.WalSink = rec }).Run(context.Background(), "p"); err == nil {
		t.Fatal("provider 失败时 Run 应返错")
	}

	kinds, _ := rec.snapshot()
	if len(kinds) == 0 || kinds[len(kinds)-1] != core.KindWalTurnFailed {
		t.Fatalf("WAL 应以 turn.failed 收尾，got %v", kinds)
	}
	for _, k := range kinds {
		if k == core.KindWalTurnEnd {
			t.Fatalf("失败的轮次不该有 turn.end，got %v", kinds)
		}
	}
	failed := payloadsOf(t, rec, core.KindWalTurnFailed)
	if len(failed) != 1 {
		t.Fatalf("turn.failed = %d 条，want 1", len(failed))
	}
	if got := failed[0]["turn"].(float64); int(got) != 1 {
		t.Errorf("turn.failed.turn = %v，want 1", failed[0]["turn"])
	}
	msg, _ := failed[0]["error"].(string)
	if !strings.Contains(msg, "端点 500") {
		t.Errorf("turn.failed.error = %q，应含错误摘要", msg)
	}
}

// captureLogger 是测试用的 slog.Handler：数 Warn 条数。
type captureLogger struct {
	mu    sync.Mutex
	warns int
	msgs  []string
}

func (l *captureLogger) Enabled(context.Context, slog.Level) bool { return true }

func (l *captureLogger) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if r.Level >= slog.LevelWarn {
		l.warns++
		l.msgs = append(l.msgs, r.Message)
	}
	return nil
}

func (l *captureLogger) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *captureLogger) WithGroup(string) slog.Handler      { return l }

func (l *captureLogger) countWarn() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.warns
}

// TestWALOffZeroBehaviorChange（F6，酸测试）：**未配置 WAL = 零行为变化**——事件流与
// "开着 WAL"**逐事件等价**（kind / durable / seq 形态 / **载荷内容** / BinaryRef 全同）。
//
// **载荷也要比**（critic F5）：原来只比 kind/durable/seq → 改载荷内容（把 tool_call_id 换
// 掉、把 result 换掉）这个变异**能活下来**。故这里做**归一化 payload 深度比对**：剥掉
// 每次运行天然不同的 id/ts 字段（session_id 等——那不是 WAL 带来的差异），剩下的结构
// 差异才是真的行为差异。
func TestWALOffZeroBehaviorChange(t *testing.T) {
	run := func(mut func(*loop.Config)) []contracts.Event {
		h := newHarness(t, allowRule("echo"))
		h.addTool("echo", nil)
		// provider 带**一次工具调用**（withToolCall）：酸测试要覆盖到工具调用的载荷
		// （tool.call.intent / tool.call.completed）——只比模型轮的载荷会漏掉一大半。
		prov := &walDeltaProvider{deltas: []string{"a", "b"}, withToolCall: true}
		h.script(
			scriptStep{result: core.ModelResult{Text: "调工具", ToolCalls: []core.ToolCallReq{call("tc_ACID", "echo", `{"text":"hi"}`)}}},
			scriptStep{result: core.ModelResult{Text: "收尾"}},
		)
		lp := h.loop(func(c *loop.Config) {
			c.Provider = prov
			if mut != nil {
				mut(c)
			}
		})
		if _, err := lp.Run(context.Background(), "p"); err != nil {
			t.Fatalf("Run: %v", err)
		}
		return h.events()
	}

	off := run(nil)                                           // 未配置 WAL（默认）
	on := run(func(c *loop.Config) { c.WalSink = &recWAL{} }) // 配了 WAL

	if len(off) != len(on) {
		t.Fatalf("事件条数：关 %d vs 开 %d（WAL 不得改变事件流）", len(off), len(on))
	}
	for i := range off {
		if off[i].Kind != on[i].Kind {
			t.Fatalf("事件[%d] kind：关 %q vs 开 %q", i, off[i].Kind, on[i].Kind)
		}
		if off[i].Durable != on[i].Durable {
			t.Fatalf("事件[%d] durable：关 %v vs 开 %v", i, off[i].Durable, on[i].Durable)
		}
		if (off[i].Seq == nil) != (on[i].Seq == nil) {
			t.Fatalf("事件[%d] seq 形态：关 %v vs 开 %v（WAL 不得占 seq）", i, off[i].Seq, on[i].Seq)
		}
		if off[i].BinaryRef != nil || on[i].BinaryRef != nil {
			t.Fatalf("本用例不配 blob，BinaryRef 应恒空（关 %v 开 %v）", off[i].BinaryRef, on[i].BinaryRef)
		}
		if len(off[i].Payload) == 0 {
			t.Fatalf("事件[%d]（%s）payload 为空", i, off[i].Kind)
		}
		// **载荷内容**也要逐条比（critic F5：只比 kind/durable/seq 的话，改载荷的变异能活）。
		gotOff := normalizePayload(t, off[i].Payload)
		gotOn := normalizePayload(t, on[i].Payload)
		if !reflect.DeepEqual(gotOff, gotOn) {
			t.Fatalf("事件[%d]（%s）载荷不同（WAL 不得改变事件载荷）：\n关 %s\n开 %s",
				i, off[i].Kind, string(off[i].Payload), string(on[i].Payload))
		}
	}
	// model.delta 仍是 durable=false 的流事件（F8/陷阱 11 不受影响）。
	for _, ev := range off {
		if ev.Kind == loop.KindModelDelta && ev.Durable {
			t.Fatal("model.delta 必须 durable=false（WAL 接入不改变这一点）")
		}
	}
}

// volatilePayloadKeys 是"每次运行天然不同"的字段名（归一化时剥掉）。
//
// 为什么剥：酸测试要比的是"开/关 WAL 两次 Run"的**载荷内容**；session_id / 事件 id /
// 时间戳这类字段每次都不一样（它们不是 WAL 带来的差异）。剥掉之后，剩下的结构差异才是
// 真的行为差异。
var volatilePayloadKeys = map[string]bool{
	"id":         true,
	"session_id": true,
	"agent_id":   true,
	"event_id":   true,
	"run_id":     true,
	"ts":         true,
	"timestamp":  true,
	"created_at": true,
	"seq":        true,
}

// normalizePayload 解一条事件载荷并**归一化**（递归剥掉随机 id/ts 字段），返回可比结构。
func normalizePayload(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("事件载荷不可解: %v（%s）", err, string(raw))
	}
	return stripVolatile(v)
}

// stripVolatile 递归剥掉 volatilePayloadKeys 里的键（map / slice 逐层下钻）。
func stripVolatile(v any) any {
	switch tv := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(tv))
		for k, val := range tv {
			if volatilePayloadKeys[k] {
				continue
			}
			out[k] = stripVolatile(val)
		}
		return out
	case []any:
		out := make([]any, len(tv))
		for i, val := range tv {
			out[i] = stripVolatile(val)
		}
		return out
	default:
		return v
	}
}

// TestWALNilSinkNoPanic（F7）：WalSink 为 nil 时五个写点全是空操作（不 panic、不写东西）。
//
// 与 F6 的分工：F6 钉"事件流等价"，本用例钉"nil 安全"——nil sink 是默认路径，任何一处
// 忘了判 nil 就是每次模型调用一次 panic。
func TestWALNilSinkNoPanic(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	prov := &walDeltaProvider{deltas: []string{"x"}}
	h.script(
		scriptStep{result: core.ModelResult{Text: "调工具", ToolCalls: []core.ToolCallReq{call("tc_NIL", "echo", `{"text":"hi"}`)}}},
		scriptStep{result: core.ModelResult{Text: "收尾"}},
	)
	// 不设 WalSink（nil）。
	if _, err := h.loop(func(c *loop.Config) { c.Provider = prov }).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
}
