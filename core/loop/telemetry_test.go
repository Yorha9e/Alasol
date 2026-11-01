package loop_test

// telemetry（L2/D19）的 loop 层钉子：三发点序列 / 默认关零行为变化 / emit 前脱敏 /
// emit 失败不阻断 / 单向（有无 telemetry 执行结果一致）。
//
// 口径（全文以 core/telemetry.go 与 core/loop/telemetry.go 为准）：
//   - **独立管道，默认关、脱敏、单向**（L2 拍板）；
//   - 默认关（TelemetrySink nil）= 一条 record 都不造 = 零行为变化（酸测试）；
//   - emit 前对自由文本跑 core.Redact（H18 形态脱敏）——**变异自证**：去掉 telEmit
//     里的 Redact → TestTelemetryRedactsCredentialShapesInError 钉子红；
//   - Emit 失败只 Warn 不阻断（与 WAL 同纪律）；
//   - 三个发点全在 X1 三段写路径**之外**（intent→completed/failed 落盘序一字不动）。

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
)

// recTel 是测试用的内存 core.Sink：按序记录 record，可注入失败。
type recTel struct {
	mu      sync.Mutex
	recs    []core.Record
	failAll bool
	failed  int
}

func (r *recTel) Emit(rec core.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failAll {
		r.failed++
		return errors.New("recTel: 注入的 emit 失败")
	}
	r.recs = append(r.recs, rec)
	return nil
}

func (r *recTel) snapshot() []core.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]core.Record(nil), r.recs...)
}

func (r *recTel) failures() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failed
}

// kindsOf 取 rec 里某个 kind 的所有记录（按序）。
func recsOf(recs []core.Record, kind string) []core.Record {
	var out []core.Record
	for _, r := range recs {
		if r.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}

// TestTelemetryThreeEmitPoints：开着 sink 时三发点序列 = run.begin → tool.call → run.end
// （工具轮）；收敛 Run 至少 begin+end。发点在 X1 三段路径之外（不改事件流，见酸测试）。
func TestTelemetryThreeEmitPoints(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	rec := &recTel{}
	h.script(
		scriptStep{result: core.ModelResult{Text: "调工具", ToolCalls: []core.ToolCallReq{call("tc_TEL1", "echo", `{"text":"hi"}`)}}},
		scriptStep{result: core.ModelResult{Text: "收尾"}},
	)
	lp := h.loop(func(c *loop.Config) { c.TelemetrySink = rec })

	if _, err := lp.Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := rec.snapshot()
	kinds := make([]string, 0, len(got))
	for _, r := range got {
		kinds = append(kinds, r.Kind)
	}
	want := []string{core.KindTelRunBegin, core.KindTelToolCall, core.KindTelRunEnd}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Fatalf("telemetry kind 序列 = %v\nwant %v", kinds, want)
	}
	// run.begin 带模型名与归属。
	begins := recsOf(got, core.KindTelRunBegin)
	if len(begins) != 1 || begins[0].Model != "test-model" || begins[0].SessionID == "" {
		t.Errorf("run.begin 形状不对: %+v", begins)
	}
	// tool.call 带工具名与 ok。
	calls := recsOf(got, core.KindTelToolCall)
	if len(calls) != 1 || calls[0].Tool != "echo" || !calls[0].OK || calls[0].Turn != 1 {
		t.Errorf("tool.call 形状不对: %+v", calls)
	}
	// run.end 带轮数/工具调用数。
	ends := recsOf(got, core.KindTelRunEnd)
	if len(ends) != 1 || ends[0].Turns != 2 || ends[0].ToolCalls != 1 {
		t.Errorf("run.end 形状不对: %+v", ends)
	}
	// TS 非空（诊断用）。
	for _, r := range got {
		if r.TS == "" {
			t.Errorf("%s 的 TS 为空", r.Kind)
		}
	}
}

// TestTelemetryOffZeroBehaviorChange（酸测试，关/开逐事件等价）：默认关（nil sink）与
// 开着 sink 两次 Run 的事件流**逐事件等价**（kind/durable/seq 形态/载荷深度比对）。
//
// **载荷也要比**：只比 kind/durable/seq 的话，改载荷的变异能活——与 WAL 酸测试同纪律。
func TestTelemetryOffZeroBehaviorChange(t *testing.T) {
	run := func(mut func(*loop.Config)) []contracts.Event {
		h := newHarness(t, allowRule("echo"))
		h.addTool("echo", nil)
		prov := &walDeltaProvider{deltas: []string{"a", "b"}, withToolCall: true}
		h.script(
			scriptStep{result: core.ModelResult{Text: "调工具", ToolCalls: []core.ToolCallReq{call("tc_ACID_TEL", "echo", `{"text":"hi"}`)}}},
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

	off := run(nil) // 默认关（TelemetrySink nil）
	on := run(func(c *loop.Config) {
		c.TelemetrySink = &recTel{}
	})

	if len(off) != len(on) {
		t.Fatalf("事件条数：关 %d vs 开 %d（telemetry 不得改变事件流）", len(off), len(on))
	}
	for i := range off {
		if off[i].Kind != on[i].Kind {
			t.Fatalf("事件[%d] kind：关 %q vs 开 %q", i, off[i].Kind, on[i].Kind)
		}
		if off[i].Durable != on[i].Durable {
			t.Fatalf("事件[%d] durable：关 %v vs 开 %v", i, off[i].Durable, on[i].Durable)
		}
		if (off[i].Seq == nil) != (on[i].Seq == nil) {
			t.Fatalf("事件[%d] seq 形态：关 %v vs 开 %v（telemetry 不得占 seq）", i, off[i].Seq == nil, on[i].Seq == nil)
		}
		if len(off[i].Payload) == 0 {
			t.Fatalf("事件[%d]（%s）payload 为空", i, off[i].Kind)
		}
		gotOff := normalizePayload(t, off[i].Payload)
		gotOn := normalizePayload(t, on[i].Payload)
		if !reflect.DeepEqual(gotOff, gotOn) {
			t.Fatalf("事件[%d]（%s）载荷不同（telemetry 不得改变事件载荷）：\n关 %s\n开 %s",
				i, off[i].Kind, string(off[i].Payload), string(on[i].Payload))
		}
	}
}

// TestTelemetryOffZeroRecords：默认关时**一条 record 都不造**（nil sink 路径零行为）。
func TestTelemetryOffZeroRecords(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(stepText("hi"))
	// 不设 TelemetrySink（nil）。
	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 没有 sink 可断言——本用例钉的是"nil 路径不 panic、Run 成功"（与 WAL NilSink 同款）。
	// 零 record 由酸测试（关/开事件等价 + 开时才有 rec.recs）间接覆盖。
}

// TestTelemetryEmitFailureDoesNotBlockRun：Emit 失败**不阻断 Run**（尽力而为），
// 事件流照常完整；失败**留痕**（恰好一条 Warn——不是零条，也不是每条都刷）。
func TestTelemetryEmitFailureDoesNotBlockRun(t *testing.T) {
	prev := slog.Default()
	cap := &captureLogger{}
	slog.SetDefault(slog.New(cap))
	defer slog.SetDefault(prev)

	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	tel := &recTel{failAll: true}
	h.script(
		scriptStep{result: core.ModelResult{Text: "调工具", ToolCalls: []core.ToolCallReq{call("tc_TELFAIL", "echo", `{"text":"hi"}`)}}},
		scriptStep{result: core.ModelResult{Text: "照常跑完"}},
	)
	lp := h.loop(func(c *loop.Config) { c.TelemetrySink = tel })

	res, err := lp.Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("telemetry emit 失败不该阻断 Run，得到错误: %v", err)
	}
	if res.Text != "照常跑完" {
		t.Errorf("Result.Text = %q，want 照常跑完", res.Text)
	}
	// 事件流照常完整。
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
		t.Fatalf("emit 失败后事件流仍应完整（user.prompt=%v model.turn=%v）", sawPrompt, sawTurn)
	}
	if n := cap.countWarn(); n != 1 {
		t.Fatalf("telemetry 失败告警 = %d 条，want 1（必须留痕，且只留一次）", n)
	}
	// 每条都**试过**（不阻断 ≠ 不再试）：begin + tool.call + end 至少 3 次。
	if tel.failures() < 3 {
		t.Errorf("失败次数 = %d，want ≥3（三发点各试一次）", tel.failures())
	}
}

// TestTelemetryRedactsCredentialShapesInError（**变异自证钉子**）：provider 错误里夹带
// 凭据形态字符串 → run.end 的 Error **脱敏后**才进 record——原值不出现、形态被 mask 成 ***。
//
// 变异：去掉 core/loop/telemetry.go telEmit 里的 `rec.Error = core.Redact(rec.Error)`
// → 本用例红（原值泄漏进观测出站口）。
func TestTelemetryRedactsCredentialShapesInError(t *testing.T) {
	const secret = "sk-TESTSECRETKEY12345678"
	h := newHarness(t)
	rec := &recTel{}
	h.script(scriptStep{err: errors.New("provider: 401 unauthorized for " + secret)})

	if _, err := h.loop(func(c *loop.Config) { c.TelemetrySink = rec }).Run(context.Background(), "p"); err == nil {
		t.Fatal("provider 失败时 Run 应返错")
	}

	ends := recsOf(rec.snapshot(), core.KindTelRunEnd)
	if len(ends) != 1 {
		t.Fatalf("run.end = %d 条，want 1", len(ends))
	}
	got := ends[0].Error
	if strings.Contains(got, secret) {
		t.Fatalf("run.end.Error 泄漏了凭据原值（Redact 被去掉了？）: %q", got)
	}
	if !strings.Contains(got, "***") {
		t.Fatalf("run.end.Error 应含脱敏标记 ***, got %q", got)
	}
	// 非凭据部分保留（不整段抹掉——诊断还要认得出是哪类错）。
	if !strings.Contains(got, "401 unauthorized") {
		t.Errorf("脱敏不该吃掉非凭据部分: %q", got)
	}
}

// TestTelemetryUnidirectionalSameResult：单向——有无 telemetry 的**执行结果**一致
// （Result 关键字段逐项相同；事件流等价由酸测试覆盖）。
func TestTelemetryUnidirectionalSameResult(t *testing.T) {
	run := func(withTel bool) loop.Result {
		h := newHarness(t, allowRule("echo"))
		h.addTool("echo", nil)
		h.script(
			scriptStep{result: core.ModelResult{Text: "调工具", ToolCalls: []core.ToolCallReq{call("tc_UNI", "echo", `{"text":"hi"}`)}}},
			scriptStep{result: core.ModelResult{Text: "收尾"}},
		)
		lp := h.loop(func(c *loop.Config) {
			if withTel {
				c.TelemetrySink = &recTel{}
			}
		})
		res, err := lp.Run(context.Background(), "p")
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		return res
	}

	off := run(false)
	on := run(true)
	if off.Text != on.Text || off.Turns != on.Turns || off.ToolCalls != on.ToolCalls ||
		off.Events != on.Events || off.TruncatedByMaxTurns != on.TruncatedByMaxTurns {
		t.Fatalf("执行结果不一致（telemetry 必须单向）：\n关 %+v\n开 %+v", off, on)
	}
}

// TestTelemetryNilSinkNoPanic：nil sink 时三发点全空操作（不 panic）。
func TestTelemetryNilSinkNoPanic(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(
		scriptStep{result: core.ModelResult{Text: "调工具", ToolCalls: []core.ToolCallReq{call("tc_NILTEL", "echo", `{"text":"hi"}`)}}},
		stepText("收尾"),
	)
	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// 编译期：recTel 就是 core.Sink。
var _ core.Sink = (*recTel)(nil)

// jsonUsed 保证 encoding/json 被本文件引用（若后续重构去掉 json 用法，编译器会提醒）。
var _ = json.Marshal
