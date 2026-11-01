package loop

import (
	"log/slog"
	"sync/atomic"

	"github.com/Yorha9e/alasol/core"
)

// 本文件是 WAL 预写日志（v4-4-B）在 loop 侧的挂接：五个写点 + 尽力而为包装。
//
// # 定位：WAL 不是事实源（F5 不受影响）
//
// 事件流（core.Store）仍是唯一事实源。WAL 记的是"正式写之前/之外"的中间态，崩溃后至少
// 知道"当时在干什么、吐过什么"——它是**崩溃恢复的补充面**，可丢弃/可截断/可 GC：
//
//   - 删掉整个 WAL 目录不丢任何事实，事件流照常完整回放；
//   - 恢复报告是**观察面不是写回面**：WAL 里的 delta 可供展示，绝不自动补进事件流
//     （自动回填 = 第二事实源，F5 与陷阱 11 一起破）；
//   - 没有任何控制流读 WAL（策略/预算/重试都不看它）。
//
// # 不影响 X1（intent→completed/failed 的落盘序一字不动）
//
// 五个写点全部落在三段写路径**之外**：
//
//	turn.begin  → provider.Run **之前**（模型调用还不是事实）
//	turn.delta  → emitFunc 里，与 model.delta 流事件同一个出口（durable=false，不入库）
//	turn.end    → model.turn **durable 落盘之后**（见下面的顺序说明）
//	turn.failed → provider.Run **返错**时（普通失败收口；不是崩溃）
//	blob.plan   → tool.call.intent **发布之前**（X1：先有计划再有窗口）
//
// 没有任何一个写点插进 intent→(completed|failed) 之间，也没有改变它们的落盘序。
//
// # 关 = 零行为变化（酸测试）
//
// WalSink 为 nil（未配置）时 walAppend 直接返回：一个文件都不建、一次追加都不做、
// 行为与配置前**逐事件等价**。这是默认路径。
//
// # 尽力而为 + 留痕
//
// Append 失败**不阻断 Run**（不为一条观测中止一次模型调用/工具执行），但**必须留痕**：
// walAppend 打一条 Warn。为什么是"一次"：delta 是逐条的，WAL 写失败时每条都告警会把
// 日志刷成噪声（而噪声恰恰是最容易被整体忽略的）；一次之后事实已经留在日志里
// （"WAL 在这个 loop 里已经不可用"），后续重复同一条没有增量信息。
//
// **作用域是 loop 级（每个 session 一份），不是进程级**：同一进程里两个 session 各有各的
// walWriter（各自告警一次）——文案按实情写，不吹成"进程级只告警一次"。

// walWriter 是 WalSink 的**尽力而为包装**（nil sink = 关 = 零行为变化）。
//
// 为什么包在结构体里而不是 Loop 的裸字段：Loop 承诺"无可变状态 → 可并发 Run"（见 Loop
// 的 doc）。告警去重需要一点状态，故把它收进一个只用 atomic 的小对象——并发 Run 共享
// 同一个 wal（同一 session 同一份 WAL）时也安全，且不破坏"每轮历史/污点/spawn 缓存都是
// Run 局部变量"这条不变式。
type walWriter struct {
	sink core.WalSink
	// warned 是"已告警过一次"的标记（CAS：只有第一个失败者告警）。
	//
	// **作用域 = 一个 walWriter（= 一个 loop = 一个 session）**，不是进程级：同一进程里
	// 两个 session 各有自己的 walWriter，各自告警一次（文案按实情写，见文件头）。
	warned atomic.Bool
}

// walAppend 追加一条 WAL 记录（nil = 关 = 零行为变化；失败 = 一次 Warn，不返回错误）。
//
// 不返回 error 是**有意的**：调用方（turn.begin/delta/end/failed/blob.plan 五个写点）没有
// "WAL 写失败该怎么办"的分支——它们一律继续主流程。把 error 返上去只会诱导调用方写
// 出 `if err := l.walAppend(...); err != nil { ... }` 这种无处可去的错误处理。
func (l *Loop) walAppend(kind string, payload any) {
	w := l.wal
	if w == nil {
		return // 关：零行为变化（酸测试）
	}
	if err := w.sink.Append(kind, payload); err != nil {
		// **每个 loop 只告警一次**（CAS）：delta 是逐条的，每条都告警会把日志刷成噪声——而
		// 噪声恰恰是最容易被整体忽略的。首次失败后"WAL 在这个 loop 里已不可用"这件事已经在
		// 日志里，后续重复同一条没有增量信息。CAS 而不是裸 bool：同一 session 的并发 Run
		// 共享同一个 walWriter，两个 goroutine 同时失败也只该留一条。
		if w.warned.CompareAndSwap(false, true) {
			walWarn("loop: WAL 追加失败（WAL 是尽力而为的崩溃恢复补充面，不阻断主流程；"+
				"本 loop 后续 WAL 失败不再重复告警）", "kind", kind, "err", err.Error())
		}
	}
}

// walWarn 是 WAL 失败的告警出口（动态取 slog.Default()：宿主可能在 loop 包初始化之后
// 才 SetDefault，且测试要能断言"只告警一次"——与 blobLog（包初始化时捕获）的差异是有意的）。
func walWarn(msg string, args ...any) { slog.Default().Warn(msg, args...) }

// walTurnBegin 在 provider.Run **之前**记一轮的开始（turn.begin）。
//
// 写在请求组装完、模型调用发起前：此刻"这一轮要干什么"完全确定（模型、消息数、可见
// 工具数），正是崩溃后要读出来的现场。
func (l *Loop) walTurnBegin(turn int, req core.ModelRequest) {
	l.walAppend(core.KindWalTurnBegin, core.WalTurnBeginPayload{
		Turn:     turn,
		Model:    l.model,
		Messages: len(req.Messages),
		Tools:    len(req.Tools),
	})
}

// walTurnDelta 记一条流式增量片段（turn.delta）——**本刀最大价值所在**。
//
// 为什么值得逐条记：model.delta 是 durable=false 的流事件（F8/陷阱 11：不入库、不占 seq、
// 不回放），崩溃后本轮模型输出**整轮蒸发**（本 session 反复撞到的洞：一轮长输出跑到一半
// 进程崩了，事件流里只有 intent/什么都没有）。WAL 逐条记下它，崩溃后至少知道"吐过什么"。
//
// **写在 publishModelDelta 之前**（预写口径）：WAL 是"正式写之前"的中间态。失败不影响
// 流事件照常发（尽力而为，见 walAppend）。
func (l *Loop) walTurnDelta(turn int, ev core.ProviderEvent) {
	// 与 publishModelDelta 同一道空值闸：text/reasoning 都空的片段没有内容可记
	//（provider 侧本就不该发，这里是纵深防御）。
	if ev.Text == "" && ev.Reasoning == "" {
		return
	}
	payload := core.WalTurnDeltaPayload{Turn: turn}
	if ev.Text != "" {
		text := ev.Text
		payload.Text = &text
	}
	if ev.Reasoning != "" {
		reasoning := ev.Reasoning
		payload.Reasoning = &reasoning
	}
	l.walAppend(core.KindWalTurnDelta, payload)
}

// walTurnEnd 记一轮正常结束（turn.end；与 formal model.turn 事件对应）。
//
// # 为什么在 model.turn **落盘之后**写（顺序是有意的）
//
// turn.end 的语义是"这一轮的事实已经进了事件流"。若反过来（先写 turn.end 再落
// model.turn），两者之间崩溃会让 WAL 说"这轮结束了"而事件流里没有这轮——恢复报告会
// 把一次**真实崩溃**显示成干净收场（假阴性比漏报更坏：使用者据此以为没丢东西）。
// 现在的顺序下，崩溃只会让 WAL 停在"有 begin 无 end"，报告如实说"第 N 轮在飞"。
//
// **诚实边界**：turn.end 的语义是"这一轮走完了"，而不是"model.turn 一定落进了事件流"——
// publishModelTurn 本身是 best-effort（落不下会补一条 loop.error(phase=model.turn) 留症）。
// 要完整保证"无 end ⇔ 事件流里没这轮"，得把两者改成同一个事务（那会把 WAL 从补充面拉回写路径、
// 与 X1 耦合）——不值得，且留症已足。
func (l *Loop) walTurnEnd(turn int, mr core.ModelResult) {
	l.walAppend(core.KindWalTurnEnd, core.WalTurnEndPayload{
		Turn:         turn,
		ToolCalls:    len(mr.ToolCalls),
		InputTokens:  mr.Usage.InputTokens,
		OutputTokens: mr.Usage.OutputTokens,
	})
}

// walTurnFailed 记一轮**失败收口**（turn.failed；provider.Run 返错时写）。
//
// 为什么需要它（对账口径）：崩溃点 = "有 turn.begin、无 turn.end"。但**模型调用失败**
// （429/超时/端点报错）也留下"有 begin 无 end"——那不是崩溃。没有这条记录，恢复报告会把
// "上一次模型调用失败了"误报成"疑似崩溃残留"（假阳性：使用者据此以为丢了东西，其实
// 只是模型报错）。
//
// **写在 publishLoopError 之后**：那条 loop.error 是事件流侧的事实（"这一轮模型调用失败"），
// 本条是 WAL 侧的同一件事（"这一轮没跑完，但不是崩溃"）。顺序无讲究（两者都不参与
// 任何判定），但都必须在 return 之前写——否则又是一次"悬空 begin"。
//
// 错误摘要截断（walErrorSummary）：WAL 里留全量错误文本没意义（诊断看事件流更全），
// 且超长错误会把 WAL 行撑大（读面有单行上限）。
func (l *Loop) walTurnFailed(turn int, err error) {
	if err == nil {
		return // 没有错误就没有失败（不写空记录）
	}
	l.walAppend(core.KindWalTurnFailed, core.WalTurnFailedPayload{
		Turn:  turn,
		Error: walErrorSummary(err),
	})
}

// walErrorSummary 把错误压成一行摘要（截断到 walErrorSummaryMaxBytes）。
//
// 为什么截断：错误文本可能很长（带响应体），WAL 是**补充面**不是事实源——完整错误在
// 事件流（loop.error）里。WAL 里只留"够认得出是哪类错"的一行。
const walErrorSummaryMaxBytes = 512

// walErrorSummary 截断错误摘要（按字节截，超长加省略号标记）。
func walErrorSummary(err error) string {
	s := err.Error()
	if len(s) <= walErrorSummaryMaxBytes {
		return s
	}
	return s[:walErrorSummaryMaxBytes] + "…（已截断）"
}

// walBlobPlan 记一条外置**计划**（blob.plan；写在 tool.call.intent **发布之前**）。
//
// 为什么记：v4-3-A 的崩溃窗口是"blob 写成功、completed 事件落盘前进程崩掉 → 磁盘上多
// 一份**没有引用指向它**的 blob（孤儿内容）"。计划先行落 WAL，崩溃续跑就能对账出"这个
// tool_call 的结果没有 completed"→ 报告列出候选（可选手动清理）。
//
// **为什么在 intent 之前**（X1 口径）：写点插在 intent→completed 之间 = 在 X1 关键路径
// 上多一次落盘，还让"写点全在三段路径外"这句话不成立。挪到 intent 之前 → 窗口内一个
// 写点都不加（且 blob.plan 不再 fsync，见 runtime/store/walfile 的边界 kind 口径）。
//
// **诚实边界**：此刻工具还没跑，结果还不存在 → **体量未知**（ResultBytes = 0）。精确体量
// 要到执行完才知道，而那已在窗口内（不写）。代价：报告里"孤儿候选"是**候选**（结果可能
// 写了、也可能没写），不是断言。
//
// turn=0 表示重放路径（重放没有"第几轮"的概念，如实记 0 而不是编一个）。
func (l *Loop) walBlobPlan(turn int, toolCallID string, threshold int64) {
	l.walAppend(core.KindWalBlobPlan, core.WalBlobPlanPayload{
		Turn:        turn,
		ToolCallID:  toolCallID,
		ResultBytes: 0, // 计划时未知（结果尚未产生）——见上面的诚实边界
		Threshold:   threshold,
	})
}
