package loop

import (
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/Yorha9e/alasol/core"
)

// 本文件是 telemetry（L2/D19）在 loop 侧的挂接：三个发点 + 尽力而为包装 + emit 前脱敏。
//
// # 定位：独立管道、默认关、脱敏、单向（L2 拍板）
//
// telemetry 与事件流（唯一事实源，F5）、WAL（崩溃恢复补充面）都是**分开的面**：它把"已经
// 发生的事"复制一份送往观测后端，**不参与任何判定**（策略/预算/重试/门禁都不看它）、
// **不是事实源**（可丢弃）。默认关（Sink 为 nil）= 一条 record 都不造 = 零行为变化（酸测试）。
//
// # 不影响 X1（intent→completed/failed 的落盘序一字不动）
//
// 三个发点全部落在三段写路径**之外**：
//
//	run.begin → Run 开始（user.prompt 落库之后；此刻还没有任何工具调用）
//	tool.call → 一次普通工具调用**解析之后**（completed/failed 已落盘、runToolCall 返回）
//	run.end   → Run 收场（deferred；正常/出错/截断都发）
//
// 没有一个发点插进 intent→(completed|failed) 之间，也没有改变它们的落盘序。
//
// # 尽力而为 + 脱敏 + 单向
//
//   - Emit 失败**不阻断 Run**（不为一条观测中止一次模型调用/工具执行），只留一条 Warn；
//   - emit 前对自由文本（Error 摘要）跑 core.Redact（H18 形态脱敏，复用既有凭据形状）——
//     凭据不进 record；
//   - **单向**：没有任何执行路径读 telemetry——执行结果与有无 telemetry **逐事件等价**（酸测试）。
//
// # 告警去重作用域 = loop 级（每个 session 一份）
//
// 与 walWriter 同款：telWriter 只多一个 atomic 告警去重标记，不破坏"Loop 无可变状态 →
// 可并发 Run"这条不变式。同一进程里两个 session 各有自己的 telWriter，各自告警一次。

// telWriter 是 Sink 的**尽力而为包装**（nil sink = 关 = 零行为变化）。
//
// 为什么包在结构体里而不是 Loop 的裸字段：Loop 承诺"无可变状态 → 可并发 Run"（见 Loop
// 的 doc）。告警去重需要一点状态，故把它收进一个只用 atomic 的小对象——并发 Run 共享
// 同一个 tel（同一 session 同一份观测）时也安全。
type telWriter struct {
	sink core.Sink
	// warned 是"已告警过一次"的标记（CAS：只有第一个失败者告警）。作用域 = 一个 telWriter
	//（= 一个 loop = 一个 session），不是进程级。
	warned atomic.Bool
}

// telEmit 送出一条观测 record（nil = 关 = 零行为变化；失败 = 一次 Warn，不返回错误）。
//
// emit 前对自由文本字段（Error）跑 core.Redact 脱敏（H18）——凭据不进观测出站口。
// 不返回 error 是**有意的**：调用方没有"telemetry 送失败该怎么办"的分支，一律继续主流程。
func (l *Loop) telEmit(rec core.Record) {
	w := l.tel
	if w == nil {
		return // 关：零行为变化（酸测试）
	}
	// 脱敏（emit 前）：唯一的自由文本字段是 Error（错误摘要可能夹带端点回显）。
	rec.Error = core.Redact(rec.Error)
	if err := w.sink.Emit(rec); err != nil {
		// 每个 loop 只告警一次（CAS）：观测失败不该刷屏，首次失败后"telemetry 在这个 loop
		// 里已不可用"已在日志里，后续重复没有增量信息。
		if w.warned.CompareAndSwap(false, true) {
			telWarn("loop: telemetry 送出失败（观测是独立单向管道，不阻断主流程；"+
				"本 loop 后续 telemetry 失败不再重复告警）", "kind", rec.Kind, "err", err.Error())
		}
	}
}

// telWarn 是 telemetry 失败的告警出口（动态取 slog.Default()，与 walWarn 同款：宿主可能
// 在 loop 包初始化之后才 SetDefault，测试也要能断言"只告警一次"）。
func telWarn(msg string, args ...any) { slog.Default().Warn(msg, args...) }

// telRunBegin 记一次 Run 开始（run.begin；user.prompt 落库之后、第一轮之前）。
func (l *Loop) telRunBegin() {
	if l.tel == nil {
		return
	}
	l.telEmit(core.Record{
		Kind:      core.KindTelRunBegin,
		TS:        nowRFC3339(),
		SessionID: string(l.sessionID),
		AgentID:   string(l.agentID),
		Model:     l.model,
	})
}

// telToolCall 记一次普通工具调用解析（tool.call；runToolCall 返回之后，X1 窗口之外）。
//
// ok = 写路径是否走通（completed/failed 已落盘）；false = intent 没落盘 / ctx 取消（Run 会
// 据此中止）。dur 是这次调用的墙钟耗时（含落盘与执行）。
func (l *Loop) telToolCall(turn int, tool string, ok bool, dur time.Duration) {
	if l.tel == nil {
		return
	}
	l.telEmit(core.Record{
		Kind:       core.KindTelToolCall,
		TS:         nowRFC3339(),
		SessionID:  string(l.sessionID),
		AgentID:    string(l.agentID),
		Turn:       turn,
		Tool:       tool,
		OK:         ok,
		DurationMS: dur.Milliseconds(),
	})
}

// telRunEnd 记一次 Run 收场（run.end；deferred，正常/出错/截断都发）。
//
// err 非空 = 这次 Run 失败了（provider 报错 / ctx 取消 / 写路径断裂）；摘要截断后脱敏
// （telEmit 里跑 core.Redact）。res.Turns / res.ToolCalls 是本次 Run 的累计。
func (l *Loop) telRunEnd(res Result, err error, dur time.Duration) {
	if l.tel == nil {
		return
	}
	rec := core.Record{
		Kind:       core.KindTelRunEnd,
		TS:         nowRFC3339(),
		SessionID:  string(l.sessionID),
		AgentID:    string(l.agentID),
		Model:      l.model,
		Turns:      res.Turns,
		ToolCalls:  res.ToolCalls,
		DurationMS: dur.Milliseconds(),
	}
	if err != nil {
		rec.Error = telErrorSummary(err)
	}
	l.telEmit(rec)
}

// telErrorSummaryMaxBytes 是 run.end 失败摘要的截断上限（与 WAL 同口径：诊断看事件流更全，
// 观测里只留"够认得出是哪类错"的一行）。
const telErrorSummaryMaxBytes = 512

// telErrorSummary 截断错误摘要（按字节截，超长加省略号标记；脱敏在 telEmit 里做）。
func telErrorSummary(err error) string {
	s := err.Error()
	if len(s) <= telErrorSummaryMaxBytes {
		return s
	}
	return s[:telErrorSummaryMaxBytes] + "…（已截断）"
}
