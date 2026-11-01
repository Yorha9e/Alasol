package core

// 本文件是 **L2/D19 telemetry** 的词汇表与写面：record kind 常量 + Sink 接口 + Record 形状。
//
// # 为什么住在 core（与 WalSink / ProviderEvent 同纪律）
//
// core 是唯一事实源、不 import runtime、依赖方向相反。telemetry 的**发方**在 core/loop，
// **实现方**（noop / file / otlp）在 runtime/telemetry，**装配方**在 host——三方都要认同
// 同一组 kind 与 Record 形状，故定义住在 core，三方各自 import。换 sink 实现（OTLP→文件→
// noop）不动发方一行。
//
// # 独立管道，默认关 / 脱敏 / 单向（L2 拍板）
//
//   - **独立管道**：telemetry 与事件流（core.Store）、WAL 都是**分开的面**。事件流是唯一
//     事实源（F5），telemetry 只是把"已经发生的事"复制一份送往观测后端，**绝不**参与任何
//     判定（策略/预算/重试/门禁都不看它）；
//   - **默认关**：Sink 为 nil（host.Config.Telemetry 空/none）= noop = **零行为变化**——
//     一条 record 都不造、一次 Emit 都不调、执行结果与配置前逐事件等价（酸测试）。这是默认路径；
//   - **脱敏**：发方在 emit 前对自由文本跑 core.Redact（H18 形态脱敏，复用既有凭据形状），
//     凭据不进 record；
//   - **单向**：telemetry 只**读**观测数据往上送，绝不影响执行路径——Emit 失败只 Warn 不阻断
//     （与 WAL 同纪律：为一条观测中止一次模型调用/工具执行是把观测的重要性置于事实之上）。
//
// # 与 L1 trace 的关系（不另建 trace 系统）
//
// **事件流即 trace**（L1 拍板）：跨边界的调用链/时间轴就是 core.Store 那条全局 seq 有序流，
// 回放一个游标走完（G3）。telemetry **不是** trace——它是把既成事实**外送**到 metrics/
// logging 后端的独立管道（L2）。两者不重叠：trace 在事件流里（可回放、是事实），telemetry
// 是观测数据的出站口（可丢弃、不是事实）。详见 core/loop/doc.go 的 L1 节。

// telemetry record kind（**唯一事实源**：core 定义，core/loop 与 runtime/telemetry、host
// 都引用这里——不再各写一份字面量，免得改了一边忘了另一边而静默对不上）。
const (
	// KindTelRunBegin 一次 Run **开始**（模型名、agent 归属；诊断/计时的起点）。
	KindTelRunBegin = "run.begin"

	// KindTelToolCall 一次**普通工具调用**解析后（工具名、是否走通写路径、耗时）。
	// 发在 X1 三段写路径**之外**（tool.call.completed/failed 落盘之后）——不侵入驻点窗口。
	KindTelToolCall = "tool.call"

	// KindTelRunEnd 一次 Run **结束**（轮数、工具调用数、耗时、失败摘要）。
	// 无论正常收敛/出错/被截断都发（deferred）；Error 是**已脱敏**的失败摘要。
	KindTelRunEnd = "run.end"
)

// Sink 是 telemetry 的**写面**（L2/D19）。
//
// 形状刻意与 storage.Blob / core.WalSink 的挂接同款（core 只定义接口、实现在 runtime）：
// core/loop 只认这个接口，不认识任何文件系统/网络。
//
// # 尽力而为（best-effort）语义
//
// Emit 失败**不阻断主流程**：telemetry 是观测出站口，为它中止一次模型调用/工具执行是把
// 观测的重要性置于事实之上。调用方（core/loop）对失败只做一件事：**留一条 Warn**（见
// core/loop/telemetry.go 的 telEmit——**每个 loop 只告警一次**，避免每次 emit 都刷屏）。
//
// 返回的 error 只表示"这条没送出"，不表示任何事实状态变化。
//
// # 为什么不带 ctx
//
// 与 WalSink 同理由：实现是**本地文件追加**（file）或**空操作**（noop）或**留位**（otlp），
// 没有需要取消的长操作；带 ctx 只会诱导实现方去查取消。调用方要取消语义时，loop 自己会在
// ctx 已死时停止发起新的模型调用。
type Sink interface {
	// Emit 送出一条观测 record（Kind = 上面的 KindTel* 常量）。实现负责序列化与落地。
	Emit(record Record) error
}

// Record 是一条 telemetry 观测（L2/D19）。
//
// 只含**观测性字段**：归属（session/agent/model）+ 事件类型 + 计数/耗时 + 脱敏后的失败摘要。
// **不含**任何凭据/env 值/对话原文（H12/H19 同纪律：观测出站口也不许带秘密）；唯一的自由
// 文本字段 Error 在 emit 前已经过 core.Redact 脱敏。
type Record struct {
	// Kind = KindTel* 常量。
	Kind string `json:"kind"`
	// TS 是 UTC RFC 3339 时间戳（emit 时刻；诊断用，不参与任何判定）。
	TS string `json:"ts"`
	// SessionID / AgentID 是本次观测的归属（与事件流同口径）。
	SessionID string `json:"session_id"`
	AgentID   string `json:"agent_id"`
	// Model 是本次 Run 用的模型名（run.begin / run.end；原样，不是凭据）。
	Model string `json:"model,omitempty"`
	// Turn 是轮次号（tool.call；每次 Run 内从 1 起）。
	Turn int `json:"turn,omitempty"`
	// Tool 是工具名（tool.call）。
	Tool string `json:"tool,omitempty"`
	// OK 表示这次工具调用是否**走通了写路径**（completed/failed 事件已落盘）；
	// false = 写路径断裂（intent 没落盘 / ctx 取消），Run 会据此中止。不是"业务成功"义。
	OK bool `json:"ok,omitempty"`
	// DurationMS 是本次观测的耗时毫秒（tool.call / run.end）。
	DurationMS int64 `json:"duration_ms,omitempty"`
	// Turns / ToolCalls 是本次 Run 的轮数与工具调用数（run.end）。
	Turns     int `json:"turns,omitempty"`
	ToolCalls int `json:"tool_calls,omitempty"`
	// Error 是 Run 失败摘要（run.end；**已脱敏**，截断过——诊断用，不落全量栈）。
	Error string `json:"error,omitempty"`
}
