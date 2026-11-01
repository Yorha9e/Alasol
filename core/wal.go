package core

import "encoding/json"

// 本文件是 **WAL 预写日志**（v4-4-B）的词汇表与写面：kind 常量 + WalSink 接口 +
// 五种记录的载荷形状。
//
// # 为什么住在 core（而不是 core/loop 或 runtime）
//
// 与 ProviderEvent 的 Kind* 常量同一纪律（见 provider.go 文件头）：core 是**唯一事实源**，
// core 不 import runtime、依赖方向相反。WAL 的**写方**在 core/loop，**实现方**在
// runtime/store/walfile，**消费方**（恢复报告对账）在 host / cmd——三方都要认同一组
// kind 与载荷形状，故定义住在 core，三方各自 import。
//
// # WAL 不是事实源（F5 不受影响）
//
// 事件流（core.Store）仍是唯一事实源；WAL 是**崩溃恢复的补充面**：记录"正式写之前"的
// 中间态，崩溃后至少知道"当时在干什么、吐过什么"。因此：
//
//   - **可丢弃 / 可截断 / 可 GC**：删掉整个 WAL 目录不丢任何事实（只丢"当时在干什么"
//     这段观察），事件流照常完整回放；
//   - **不自动补进事件流**：恢复报告是**观察面不是写回面**——WAL 里的 delta 可供展示，
//     但绝不自动回填成 durable 事件（那就成了第二事实源，F5/陷阱 11 一起破）；
//   - **不参与任何判定**：没有任何控制流读 WAL（budget/策略/重试都不看它）；
//   - **不影响 X1**：WAL 的写点全部落在 intent→completed/failed 三段窗口**之外**
//     （blob.plan 在 intent **之前**落——见下面的 kind 说明），三段结构与落盘序一字不动。
//
// # 关 = 零行为变化（酸测试）
//
// 不配置 WAL（WalSink 为 nil）时：一个文件都不建、一次追加都不做、行为与配置前**逐事件
// 等价**。这是默认路径。

// WAL 记录 kind（**唯一事实源**：core 定义，core/loop、runtime/store/walfile、host 都
// 引用这里——不再各写一份字面量，免得改了一边忘了另一边而**静默**对不上账）。
const (
	// KindWalTurnBegin 一轮模型调用**之前**（turn 号、模型名、消息条数、工具条数）。
	// 崩溃后"第 N 轮在跑 X 模型"就是从它读出来的。
	KindWalTurnBegin = "turn.begin"

	// KindWalTurnDelta 流式增量片段**逐条**（text / reasoning 片段 + turn）。
	// 本刀最大价值所在：model.delta 是 durable=false 的流事件（F8/陷阱 11——不进事件流、
	// 不回放），崩溃后本轮模型输出**整轮蒸发**；WAL 逐条记下它，至少多保存一些。
	KindWalTurnDelta = "turn.delta"

	// KindWalTurnEnd 一轮**正常结束**（与 formal model.turn 事件对应；在 model.turn
	// durable 落盘**之后**写——顺序有意的，见 core/loop/wal.go 的说明）。
	KindWalTurnEnd = "turn.end"

	// KindWalTurnFailed 一轮**失败收口**（provider 报错；轮次没跑完，但不是崩溃）。
	//
	// 为什么需要它（对账口径）：崩溃点 = "有 turn.begin、无 turn.end"。但**模型调用失败**
	// （429/超时/端点报错）也会留下"有 begin 无 end"——那不是崩溃，是一次**普通失败**。
	// 没有这条记录，恢复报告会把"上一次模型调用失败了"误报成"疑似崩溃残留"（假阳性：
	// 使用者据此以为丢了东西，其实只是模型报错）。
	// 故 loop 在 provider.Run 返错时写一条 turn.failed：对账时"begin 后跟 failed"= 正常失败
	// （**不算在飞**），只有"begin 无 end 无 failed"才是崩溃残留。
	KindWalTurnFailed = "turn.failed"

	// KindWalBlobPlan 外置 blob 的**计划**（tool_call_id、阈值；**写在 intent 之前**）。
	//
	// 为什么在 intent 之前：写点落在 X1 三段窗口**之外**（intent→completed 之间插写点 =
	// 在关键路径上多一次落盘，还会让"写点全在三段路径外"这句话不成立）。计划先行落 WAL，
	// 崩溃续跑就能对账出"这个 tool_call 的结果没有 completed"→ 结果可能已写成 blob 但引用
	// 没落（v4-3-A 的崩溃窗口）→ 报告列出候选（可选手动清理）。
	//
	// **诚实边界**：计划写在 intent 前时**结果尚未产生**，故 ResultBytes 未知（= 0）；
	// 精确的"即将写入多少字节"在 intent 之后才知道——而那正是三段窗口内，**不写**
	// （口径优先：窗口内一个写点都不加）。代价见 WalBlobPlanPayload 的说明。
	KindWalBlobPlan = "blob.plan"
)

// WalSink 是 WAL 的**写面**（v4-4-B）。
//
// 形状刻意与 storage.Blob 的挂接同款（core 只定义接口、实现在 runtime）：core/loop 只认
// 这个接口，不认识任何文件系统。
//
// # 尽力而为（best-effort）语义
//
// Append 失败**不阻断主流程**：WAL 是补充面，为它中止一次模型调用/工具执行是把观测的
// 重要性置于事实之上。调用方（core/loop）对失败只做一件事：**留一条 Warn**（见
// core/loop/wal.go 的 walAppend——**每个 loop（每个 session）只告警一次**，避免每一条
// delta 都刷屏；作用域是 loop 级，不是进程级：同一进程里两个 session 各有自己的 walWriter，
// 各自告警一次）。
//
// 返回的 error 只表示"这条没记下"，不表示任何事实状态变化。
//
// # 为什么不带 ctx
//
// 实现是**本地文件追加**（一次 write，不进网络/不等锁），没有需要取消的长操作；带 ctx
// 只会诱导实现方去查取消（查了就得判"取消了要不要算失败"，平添分歧）。调用方要取消语义
// 时，loop 自己会在 ctx 已死时停止发起新的模型调用。
type WalSink interface {
	// Append 追加一条 WAL 记录（kind = 上面的 KindWal* 常量；payload = 对应的
	// Wal*Payload 结构，由实现序列化）。
	Append(kind string, payload any) error
}

// WalTurnBeginPayload 是 KindWalTurnBegin 的载荷：一轮模型调用发起前的现场。
type WalTurnBeginPayload struct {
	// Turn 轮次号（每次 Run 内从 1 起；与 model.delta 的 turn 同口径——**同 session
	// 并发跑两轮时会撞号**，这是 model.delta 已知边界的同一口径，别当精确归组用）。
	Turn int `json:"turn"`
	// Model 模型名（原样；不是凭据，可安全落盘）。
	Model string `json:"model"`
	// Messages 本轮发给模型的消息条数（含 system 之外的历史 + 在线表）。
	Messages int `json:"messages"`
	// Tools 本轮模型可见的工具条数（可见性过滤**之后**，R5/H4）。
	Tools int `json:"tools"`
}

// WalTurnDeltaPayload 是 KindWalTurnDelta 的载荷：一条流式增量片段。
//
// 与 contracts.ModelDeltaPayload 同形但**刻意不共用类型**：那个是契约类型（改它要走
// F7 全链：schema + 生成物 + 样本 + 消费端），WAL 是**可丢弃的补充面**，不该被契约变更
// 绑架；两者字段一致只是巧合地同源（都来自同一个 ProviderEvent）。
type WalTurnDeltaPayload struct {
	Turn      int     `json:"turn"`
	Text      *string `json:"text,omitempty"`
	Reasoning *string `json:"reasoning,omitempty"`
}

// WalTurnEndPayload 是 KindWalTurnEnd 的载荷：一轮正常结束。
type WalTurnEndPayload struct {
	// Turn 轮次号。
	Turn int `json:"turn"`
	// ToolCalls 本轮模型发起的工具调用条数（0 = 收敛轮）。
	ToolCalls int `json:"tool_calls"`
	// InputTokens / OutputTokens 本轮 usage（J21：只做成本观察，**绝不**接入预算/门禁
	// 判定——W15 拍板"默认不设上限"，成本闸门另走宿主检查点）。
	InputTokens  int64 `json:"input_tokens,omitempty"`
	OutputTokens int64 `json:"output_tokens,omitempty"`
}

// WalTurnFailedPayload 是 KindWalTurnFailed 的载荷：一轮**失败收口**（provider 报错）。
//
// 记的是"这一轮没跑完，但**不是崩溃**"——对账据此把"有 begin 无 end"分成两类：
// 后面跟了 failed = 普通失败（模型报错）；什么都没跟 = 崩溃残留。
type WalTurnFailedPayload struct {
	// Turn 轮次号。
	Turn int `json:"turn"`
	// Error 是失败摘要（**截断**过的错误文本：诊断用，不落全量栈）。
	Error string `json:"error"`
}

// WalBlobPlanPayload 是 KindWalBlobPlan 的载荷：外置 blob 的**计划**。
//
// 最小面只要 ToolCallID（对账键：事件流里 tool.call.completed 的 tool_call_id，S4 黑盒
// 原样往返）；另两个字段是诊断用，零成本。
type WalBlobPlanPayload struct {
	// Turn 轮次号（0 = 重放路径：重放没有"第几轮"的概念，如实记 0 而不是编一个）。
	Turn int `json:"turn"`
	// ToolCallID 对账键（provider 黑盒 id，原样）。
	ToolCallID string `json:"tool_call_id"`
	// ResultBytes 即将外置的 result 序列化字节数。
	//
	// **诚实边界**：计划写在 intent **之前**，此刻工具还没跑、结果还不存在 → 字节数
	// **未知 = 0**。精确体量要到执行完才知道，而那已在 X1 三段窗口内（不写）。0 的含义
	// 是"计划时未知"，不是"结果为空"。
	ResultBytes int `json:"result_bytes"`
	// Threshold 当时的有效外置阈值（F4：max(配置阈值, 摘要预算)）。
	Threshold int64 `json:"threshold"`
}

// WalRecord 是 WAL 文件里的一行（NDJSON）。
//
// 住在 core 而不是实现里：恢复报告的消费方（host/cmd）与写方（runtime/store/walfile）
// 要认同一**文件形状**；形状与词汇表分开住会让"改了一边忘了另一边"再次发生。
type WalRecord struct {
	// Kind = KindWal* 常量。
	Kind string `json:"kind"`
	// TS 是 UTC RFC 3339 时间戳（写入时刻；诊断用，不参与任何判定）。
	TS string `json:"ts"`
	// Payload 是对应 Wal*Payload 的原始 JSON（不解析成 any：对账侧按 kind 取自己
	// 要的那几个字段，解析失败按"损坏行"处理，不静默当成空载荷）。
	Payload json.RawMessage `json:"payload,omitempty"`
}
