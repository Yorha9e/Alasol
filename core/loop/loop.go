package loop

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/compaction"
	"github.com/Yorha9e/alasol/core/durable"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/core/storage"
)

// DefaultMaxTurns 是 Config.MaxTurns 缺省值（0 = 用它）：模型失控时强制收口，
// 避免无限工具调用把会话/预算烧穿（M 系列护栏，防呆默认值）。
const DefaultMaxTurns = 20

// 事件 kind（core 定义核心子集）：三段写路径 + loop 自身的错误通道 + 对话入流观测。
//
// loop.error 不是工具调用失败（M1：失败是普通事件，不是特殊通道）——
// 它只记录"这一轮 loop 自己没能继续往下走"的事实（模型调用失败、intent 落盘失败）。
//
// user.prompt / model.turn 是 v1-S1 补 F1 缺口的**旁路观测**（对话内容进事件流，供回放/
// resume 重建完整对话历史）：best-effort 落库、不占 Result.Events 计数、不改变任何控制流，
// 与 X1 三段写路径刻意分离（见 events.go 的 publishUserPrompt / publishModelTurn）。
const (
	// KindToolCallIntent 三段之①：意图 durable 落盘后才允许执行（X1）。
	KindToolCallIntent = "tool.call.intent"
	// KindToolCallCompleted 三段之③a：执行成功，结果 durable 落盘（X1）。
	KindToolCallCompleted = "tool.call.completed"
	// KindToolCallFailed 三段之③b：deny / 执行失败 / 工具缺失，错误模型可读（X1/M5/M8）。
	KindToolCallFailed = "tool.call.failed"
	// KindLoopError loop 自身错误的 durable 事件（模型调用失败 / intent 落盘失败）。
	KindLoopError = "loop.error"
	// KindUserPrompt 用户 prompt 入事件流（F1：对话内容是事实源的一部分）。Run 开始时一条。
	KindUserPrompt = "user.prompt"
	// KindModelTurn 一轮 provider 返回的事实（F1，含 usage 只做成本记录，J21）。每轮一条。
	KindModelTurn = "model.turn"
	// KindModelDelta 流式增量片段（G13：打字机效果）。**durable=false 的流事件**
	// （F8/陷阱 11）：不入库、不占 seq、不进 Result.Events——总线直发订阅者，
	// 与 X1 三段写路径、user.prompt/model.turn 旁路是**第四条独立通道**。
	KindModelDelta = "model.delta"
)

// provider 流事件 kind：**引用 core 的常量**（core 是唯一事实源——core 不 import
// runtime，依赖方向相反，所以常量住在 core 而不是这里；见 core.KindTextDelta 等）。
//
// 为什么不再在本包镜像一份字面量：镜像过的两份会悄悄分叉（改了一边忘了另一边 →
// loop 的 switch 收不到 provider 发的事件，**静默**丢片段，没有任何报错）。

// 失败分类码（M4：可重试/致命分类用的开放字符串；这里固化 loop 自己产生的几种）。
const (
	// CodePolicyDeny 策略判定未放行（deny / ask 无人应答 / 无命中 fail-close）。
	CodePolicyDeny = "policy_deny"
	// CodeToolNotFound 工具未在注册表注册（模型 hallucinate 了一个工具名）。
	CodeToolNotFound = "tool_not_found"
	// CodeExecutorMissing 注册实现不是 core.ToolExecutor（装配错误）。
	CodeExecutorMissing = "executor_missing"
	// CodeToolError 工具调用没成（执行器返回 error 或 panic，或因 Run ctx 取消/超时
	// **未执行**）——都是一次普通失败，不是进程崩溃。
	CodeToolError = "tool_error"
	// CodeBadArguments 工具参数不满足契约（不是合法 JSON 对象）→ 普通失败，模型可改。
	CodeBadArguments = "bad_arguments"
	// CodePermDenied 工具所需的权限维度超出本 agent 的**生效权限**（H11/H22 收窄）。
	CodePermDenied = "permission_denied"
	// CodeToolDisabled 工具已在注册表停用（registry State=disabled）→ 执行前复查拒绝
	// （缺口 3：可见集过滤之外的第二层门控，与 runtime/mcpserver 的 checkUsable 同语义）。
	CodeToolDisabled = "tool_disabled"
	// CodeLoopError loop 自身出错（模型调用失败 / intent 落盘失败）。
	CodeLoopError = "loop_error"
)

// envCapabilityPrefix 是 env 键的策略查询前缀（H17：凭据按需注入也要过同一套求值，
// 与注册表能力裁决复用 policy.Engine，E3）。例：RequiredEnv=["GITHUB_TOKEN"]
// → Query{Tool: "env:GITHUB_TOKEN"}。
const envCapabilityPrefix = "env:"

// Config 是 Loop 的装配参数。
//
// 必填：Provider / Bus / Registry / Model / SessionID / AgentID。
// 可缺省（缺省后行为更严格，fail-close）：
//   - Policy 为 nil → 空引擎语义，一切工具调用无 allow 命中 → deny；
//   - Approver 为 nil → DenyAll（无人应答 = deny，H8/D14）；
//   - MaxTurns 为 0 → 用 DefaultMaxTurns。
type Config struct {
	// Provider 模型提供方（core.Provider；实现如 Anthropic/OpenAI 在 runtime）。
	Provider core.Provider
	// Bus 事件总线：三段事件经 Publish 落库 + 通知订阅者（内部持有 core.Store）。
	Bus *bus.Bus
	// Registry 扩展注册表：取工具描述（Entry.Tool）与执行器（Entry.Impl）。
	Registry *registry.Registry
	// Policy 策略引擎（nil = 空引擎 = 一切 deny，fail-close）。
	Policy *policy.Engine
	// Approver 人审（nil = DenyAll，fail-close；policy 判定为 ask 时才被问到）。
	Approver core.Approver
	// SessionID 会话 id（事件归属；必填合法 ULID，S1/S9）。
	SessionID contracts.Ulid
	// AgentID agent id（事件归属；必填合法 ULID，S1/S9）。
	AgentID contracts.Ulid
	// Model 模型名（provider 无关标识，原样传给 provider）。
	Model string
	// System 系统提示（每轮原样带上）。
	System string
	// SessionEnv session 级 env 注入（H21：与单次调用注入合并；值永不进事件/日志）。
	SessionEnv core.EnvScope
	// MaxTurns 单次 Run 的最大轮数（0 = DefaultMaxTurns；防模型失控）。
	MaxTurns int
	// ContextBudget 上下文窗口预算 token（0 = DefaultContextBudget）。只用于
	// context.assembled 事件里 ContextSnapshot.tokens_budget 的占比分母（J21），
	// v0 不据此做任何装配/压缩决策。
	ContextBudget int64
	// TurnBudget 是**逐轮**预算钩子（v4-4-B 缺口 5：把 per-Run 成本闸门接进轮间，
	// host/budget.go 的挂账兑现）。每轮 provider.Run **之前**调用一次（与 turn.begin
	// 写点同位置区间；顺序：先查预算 → 过了再写 turn.begin → provider.Run——被拒的那
	// 一轮什么都没跑：没有 context.assembled、没有 model.turn、没有 turn.begin）。
	//
	// 返回 nil = 放行本轮；返回 error = 本轮**不发起**，Run 以"预算截断"收口——照
	// MaxTurns 截断的口径：**Run 正常返回不报错**（Result.TruncatedByBudget = true +
	// TruncationReason = 钩子的错误文案；已跑轮次的结果照常收口，Text = 最后一轮文本）。
	//
	// **nil = 不查**（默认，零行为变化、零查询）。与 Run 前的 per-Run 检查两层不冲突：
	// Run 前拒 + 轮间拒（宿主 per-Run 的 checkBudget 保留，fork 路径仍只走那层）。
	//
	// **子 agent 也走**（缺口 5 第二件）：子 loop **透传**父的钩子（buildChildLoop）——
	// 子 agent 的每一轮 provider.Run 之前同样过这道闸门。不传 = 子 agent 的花费不受
	// 逐轮刹车约束（父的钩子只覆盖父自己的轮次）→ 多 agent 模式下闸门半边失效。
	// 语义成立的前提：钩子闭包绑的 session id 对父子**是同一个**（父子同 session，
	// K25 单流多 agent）→ 查的是同一份账。
	TurnBudget func(ctx context.Context) error
	// RunOverrides 是 **per-Run 覆盖钩子**（G-c/G-d 生效链的取值口，设计 15 §3.3）：
	//
	//	hub 启动默认（Config.Model / ReasoningEffort / Provider）
	//	  → session 配置（config.changed 折叠，宿主的 sessioncfg.Table）
	//	    → **本轮快照**（本钩子在 Run 开始时取一次，冻结进 runState）
	//
	// 每次 Run（**轮 = 一个 prompt 的 Run**）开头调一次：返回的空字段/nil = 用 loop
	// 基值；非空 = 本轮覆盖（model 进 ModelRequest.Model 与 model.turn、reasoning_
	// effort 进 ModelRequest.ReasoningEffort、Provider 替换本轮的 provider.Run 调用方）。
	// **运行中修改配置不影响已冻结的本轮**（钩子只在 Run 开头调一次），下一个 prompt
	// 的 Run 重取 fold → 设计 §3.1"当前轮结束后下一轮生效"。
	//
	// 返回 error = 本轮 fail-close（不带未定配置开跑，如覆盖的环境变量缺失）。
	// **nil = 不挂**（默认，零行为变化）：fork / resume / cmd 直连的 loop 不挂它，
	// 调用方签名零改动。宿主在 loopFor 装配时绑 session 闭包（host/config_fold.go）。
	RunOverrides func(ctx context.Context) (RunOverrides, error)
	// ReasoningEffort 思维链强度档位（G14；空 = **不传**给 provider）。
	// 原样进每轮 core.ModelRequest.ReasoningEffort，由 provider 透传给端点
	// （取值如 low/medium/high/xhigh；档位集随网关演进，loop 不校验）。
	ReasoningEffort string
	// Agents 多 agent 环境（阶段 5）。nil = 单 agent 模式（默认，向后兼容）：
	// 不注入 spawn/complete、不用 agent 树、不过滤工具。
	Agents *Agents
	// Store 事件库句柄（可选，阶段 8 durable）：配置后 Loop.Checkpoint 与自动 checkpoint 才可用。
	// 它与 Bus 内部持有的 store 是同一个（Bus 已持 store 但不暴露，故此处显式传入以支持 checkpoint）。
	// nil = 不建 checkpoint（Checkpoint 手动调用返回错误、自动 checkpoint 跳过）。
	Store core.Store
	// CheckpointEvery 每 N 轮自动建一次 checkpoint（X3；0 = 只手动，需配置 Store 才生效）。
	// 自动 checkpoint 是**尽力而为**：落盘失败不中止 Run（Run 的事件本身已 durable，缺 checkpoint
	// 只意味着 resume 退到全量重建）。
	CheckpointEvery int
	// OnlineTable 在线表注入的活配置（K14/K15；nil = 关闭在线表注入——默认，向后兼容）。
	// 与 OnlineTableSource **同时非 nil** 才注入；它是 hub 建一个、所有 loop 共享的对象，
	// 运行期经它调整参数（不重启，下一轮生效）。
	OnlineTable *OnlineTableConfig
	// OnlineTableSource 在线表数据源（K11：self + gate 相交的对端主 agent；nil = 关闭注入）。
	OnlineTableSource OnlineTableSource
	// ContextStrategy 上下文装配策略工厂（v2-S1；compaction.AssemblerFactory，J8 context_strategy）。
	// **可挂可不挂的预备单位**（13 §1）：
	//   - nil = 不挂 → loop 走内建 append 路径，装配输出与 v1 **逐字节等价**（酸测试）；
	//   - 挂上（工厂每次 Run 造一个 fresh Assembler，如 tiered 引擎）→ 旧阶段两级压缩 +
	//     recall + 留存降级，context.assembled 点亮 stage:N / ledger / rules 来源。
	// **换/摘策略 core 零改动**：loop 只认 compaction.Assembler 接口，不认识具体实现。
	// 为什么是工厂：策略有状态（跨轮跟踪阶段），而 Loop 承诺可并发 Run——工厂让每次 Run 各持
	// 一个 Assembler（放进 runState），既不共享状态又让策略跨轮积累。宿主每次调工厂造一个绑定到
	// 本次依赖（store/provider/bus）的新实例。
	ContextStrategy compaction.AssemblerFactory
	// GoalLedger 是否给本 agent 注入 goal ledger 更新工具（J19/X6，v3-S3 清挂账）。
	// **opt-in，默认 false = 不注入（零行为变化，酸测试）**。true 且配置了 Store 时，loop
	// 每 Run 造一个 per-(session,agent) 的 durable.LedgerWriter，注入 ledger 框架工具
	// （模型可见集 + X1 三段写路径），turn 末 Flush（X6：关键字段立即落盘、过程字段合并）。
	// 注入后仍受策略链裁决（deny-all 默认）与 agent 类型白名单（R5）约束——框架工具非免检。
	GoalLedger bool
	// BlobSink 是大对象外置仓库（D4/G2e，v4-3-A；storage.Blob）。**nil = 不外置**（默认，
	// 零行为变化酸测试）。非 nil 且 BlobMaxInline>0 时，tool.call.completed 的 result 超过
	// BlobMaxInline 字节就整体外置进 blob，payload 换摘要形态、envelope.BinaryRef 指过去
	// （X1 三段结构与落盘序一字不动，只改 completed 的 result 内容 + BinaryRef；见 blob.go）。
	// 只用于**写**（外置）；读侧（取全文）由宿主经 GET /api/blobs/{id} 提供，loop 不读 blob
	// （回填给模型的始终是完整 result，不外置那条也照常完整）。
	BlobSink storage.Blob
	// BlobMaxInline 是 result 内联阈值（字节；<=0 = 不外置，即使挂了 BlobSink）。result
	// 序列化后**超过**它才外置。0/未配 = 不外置 = completed 载荷与现状逐字节等价（酸测试）。
	//
	// F4：实际判定用的是**有效阈值** = max(本值, 摘要预算)（见 blob.go 的
	// blobEffectiveThreshold）——保证"外置 = 瘦身"是结构性事实（摘要不会比原文还大）。
	// 本值配得比摘要预算还小时，(本值, 摘要预算] 区间的 result 不再外置（如实写明的代价）。
	BlobMaxInline int64
	// WalSink 是 WAL 预写日志的写面（v4-4-B；core.WalSink）。**nil = 关**（默认，
	// 零行为变化酸测试）：一个文件都不建、一次追加都不做、行为与配置前逐事件等价。
	//
	// 非 nil 时 loop 在六个点写 WAL（都在 X1 三段写路径**之外**，落盘序一字不动）：
	// turn.begin（provider.Run 前）/ turn.delta（流式片段逐条）/ turn.end（model.turn
	// 落盘后）/ turn.failed（provider 返错时）/ turn.retry（provider 内部重试发生时，
	// 缺口 5 第二件——经 emitFunc 收 provider.retry 转写，provider 侧不穿 WAL 句柄）/
	// blob.plan（tool.call.intent **发布前**）。
	// **WAL 不是事实源**（F5 不受影响）：事件流仍是唯一事实源，WAL 是可丢弃/可截断/可 GC
	// 的崩溃恢复补充面，恢复报告是观察面不是写回面（见 core/loop/wal.go 与 core/wal.go 的
	// 定位说明）。
	//
	// **写失败不阻断 Run**（尽力而为）：只留一条 Warn（一次），不为一条观测中止模型调用。
	WalSink core.WalSink
	// TelemetrySink 是 telemetry 观测管道的写面（L2/D19；core.Sink）。**nil = 关**（默认，
	// 零行为变化酸测试）：一条 record 都不造、一次 Emit 都不调、行为与配置前逐事件等价。
	//
	// 非 nil 时 loop 在三个点发观测（都在 X1 三段写路径**之外**，落盘序一字不动）：
	// run.begin（Run 开始）/ tool.call（普通工具调用解析后）/ run.end（Run 收场，deferred）。
	// **独立管道、默认关、脱敏、单向**（L2 拍板）：telemetry 不是事实源（F5 不受影响）、
	// 不参与任何判定；emit 前对自由文本跑 core.Redact 脱敏；Emit 失败只 Warn 不阻断
	// （与 WAL 同纪律）。见 core/loop/telemetry.go 与 core/telemetry.go 的定位说明。
	TelemetrySink core.Sink
}

// RunOverrides 是一次 Run 的**本轮快照**覆盖（G-c/G-d 生效链第三层，设计 15 §3.3）。
//
// 由 Config.RunOverrides 钩子在 **Run 开始时**取一次并冻结进 runState（轮 = 一个
// prompt 的 Run）：空字段 / nil provider = 用 loop 基值（Config.Model / Config.
// ReasoningEffort / Config.Provider）。冻结后本轮所有轮次的请求组装、model.turn 的
// model 字段、provider.Run 的调用方都只读快照——**运行中落的 config.changed 不影响
// 本轮，下一个 prompt 的 Run 重取**（设计 §3.1"当前轮结束后下一轮生效"）。
type RunOverrides struct {
	// Model 本轮模型名覆盖（空 = 用 loop 基值）。
	Model string
	// ReasoningEffort 本轮思维链档位覆盖（空 = 用 loop 基值；写时已过 G14 白名单校验，
	// loop 不再校验——与 Config.ReasoningEffort 同口径）。
	ReasoningEffort string
	// Provider 本轮 provider 覆盖（nil = 用 loop 基值）。非 nil 时由宿主按覆盖的
	// base_url + api_key_env 构造（凭据值只在宿主侧从环境变量取出，H12/H15/H19：
	// 绝不进事件/请求体/日志）。
	Provider core.Provider
}

// Loop 是一次会话内的 agent loop 状态机。
//
// 并发：Loop 本身无可变状态（每轮历史、污点累积、spawn 缓存都是 Run 的局部变量）→ 同一个
// Loop 可被多个 goroutine 并发 Run（各自独立历史）。但"一个 session 只由其宿主进程 append"
// （G25，session 亲和写）是部署层约束：并发 Run 同一 session 时，事件 seq 是提交序而非因果序。
//
// 多 agent（阶段 5）：Agents 非 nil 时，本 loop 是其 AgentID 对应 agent 的一次执行；spawn 会
// 另起一个共享 Bus/Registry/Provider 的子 loop（child.AgentID）同步跑子任务。
type Loop struct {
	provider   core.Provider
	bus        *bus.Bus
	registry   *registry.Registry
	policy     *policy.Engine
	approver   core.Approver
	sessionID  contracts.Ulid
	agentID    contracts.Ulid
	model      string
	system     string
	sessionEnv core.EnvScope
	maxTurns   int
	agents     *Agents

	// reasoningEffort 思维链档位（G14；空 = 不传）。见 Config.ReasoningEffort。
	reasoningEffort string

	// contextBudget 是 ContextSnapshot.tokens_budget 的取值来源（占比分母，J21）。
	contextBudget int64

	// store / checkpointEvery 支撑阶段 8 durable：手动/自动 checkpoint 与 resume 重放的执行侧。
	store           core.Store
	checkpointEvery int

	// onlineConfig / onlineSource 支撑 v1-S4 在线表注入（K14/K15）。两者同时非 nil 才注入
	// （见 onlineTableForTurn）；nil = 关闭——老装配（cmd run/resume、单测）不受影响。
	// onlineConfig 是**共享活配置**（hub 建一个传给所有 loop，运行期可调）。
	onlineConfig *OnlineTableConfig
	onlineSource OnlineTableSource

	// ctxStrategy 支撑 v2-S1 上下文装配策略工厂（compaction.AssemblerFactory；nil = 不挂 =
	// append，与 v1 逐字节等价）。见 Config.ContextStrategy。每次 Run 经它造一个 fresh
	// Assembler 放进 runState（策略有状态，不能跨并发 Run 共享）。
	ctxStrategy compaction.AssemblerFactory
	// goalLedger 支撑 J19 goal ledger 生产者（opt-in；nil store = 即使开了也不注入）。
	goalLedger bool
	// blob / blobMaxInline 支撑 D4 大 payload 外置（G2e，v4-3-A；nil blob 或阈值<=0 = 关 =
	// 零行为变化）。见 Config.BlobSink / Config.BlobMaxInline 与 blob.go 的挂接口径。
	blob          storage.Blob
	blobMaxInline int64
	// wal 支撑 WAL 预写日志（v4-4-B；nil = 关 = 零行为变化）。见 Config.WalSink 与
	// wal.go 的六个写点。walWriter 只多一个 atomic 告警去重标记，不破坏"Loop 无可变状态
	// → 可并发 Run"这条不变式（每轮历史/污点/spawn 缓存仍是 Run 的局部变量）。
	wal *walWriter
	// tel 支撑 telemetry 观测管道（L2/D19；nil = 关 = 零行为变化）。见 Config.TelemetrySink
	// 与 telemetry.go 的三个发点。telWriter 只多一个 atomic 告警去重标记，同 walWriter 纪律。
	tel *telWriter
	// turnBudget 是逐轮预算钩子（v4-4-B 缺口 5；nil = 不查 = 零行为变化）。见
	// Config.TurnBudget 与 RunWithHistory 轮次循环里的调用点。它是**无状态闭包**
	//（宿主侧绑定 session id），并发 Run 只读共享，不破坏 Loop 的并发不变式。
	turnBudget func(ctx context.Context) error
	// runOverrides 是 per-Run 覆盖钩子（G-c/G-d 生效链取值口；nil = 不挂 = 零行为
	// 变化）。见 Config.RunOverrides 与 RunWithHistory 开头的冻结点。同样是**无状态
	// 闭包**（宿主绑定 session id），并发 Run 只读共享。
	runOverrides func(ctx context.Context) (RunOverrides, error)
}

// Result 是一次 Run 的产出。
type Result struct {
	// Text 模型最终文本（无工具调用那一轮的 Text；MaxTurns 截断时为最后一轮的 Text；
	// 子 agent 经 complete 收敛时为 summary.Text）。
	Text string
	// Turns 实际发生的模型调用轮数。
	Turns int
	// ToolCalls 本 Run 执行的工具调用次数（含被 deny / 失败的：模型发起了就算一次）。
	ToolCalls int
	// Events 本 Run 成功落库的 durable 事件数（每轮一条 context.assembled + intent +
	// completed/failed + loop.error + agent.spawned/completed/failed；
	// 子 loop 的事件计入父的 spawn 调用）。
	// **不含**旁路对话观测 user.prompt / model.turn（F1 的对话入流事实，v1-S1）：它们
	// best-effort 落库、独立于写路径与控制流，故不计入本计数（口径同 maybeCheckpoint 的
	// checkpoint.created——恢复/对话元数据不是 loop 一轮的写路径产出）。
	Events int
	// Summary 子 agent 经 complete 收敛时的结构化汇报（根 agent / 未收敛为 nil）。
	Summary *contracts.AgentSummary
	// Taint 本次 Run 累积的最高污点级（Q7；空 = clean）。父 agent 据其在子异常冒泡时标注
	// summary 的 taint；根 agent 上为信息量（v0 无根 summary）。
	Taint string
	// TruncatedByMaxTurns 本次 Run 是否**被 MaxTurns 截断**（模型还在要工具，但轮数用尽）。
	//
	// 为什么要有这个**显式**标记：Turns 只说"跑了几轮"，**分不清**"正常收敛在最后一轮"与
	// "被 MaxTurns 截断"——两者 Turns 都等于上限（例：MaxTurns=1，单轮就收敛 → Turns=1；
	// MaxTurns=1，模型发了工具调用被截断 → Turns=1，一模一样）。消费方（如 ACP 端点判
	// stopReason=max_turn_requests）要的是**这个**语义，不该靠"轮数 ≥ 上限"去猜。
	//
	// 口径：true = 轮数用尽时模型仍在发起工具调用（护栏生效，Run **正常返回不报错**，
	// Text = 最后一轮的文本）；false = 正常收敛（含子 agent complete 收敛）。
	//
	// **additive 字段**（不改既有字段语义、不动 X1 三段写路径）：不设它 = false。
	TruncatedByMaxTurns bool
	// TruncatedByBudget 本次 Run 是否**被逐轮预算截断**（v4-4-B 缺口 5：某轮
	// provider.Run 前的 TurnBudget 钩子拒绝发起本轮）。
	//
	// 口径与 TruncatedByMaxTurns 同款：true = Run **正常返回不报错**（护栏生效，已跑
	// 轮次的结果照常收口，Text = 最后一轮的文本；被拒的那一轮**什么都没跑**——没有
	// context.assembled、没有 model.turn、没有 turn.begin）；false = 没撞到预算
	// （含 TurnBudget 未配置 = 默认）。
	//
	// **additive 字段**（不改既有字段语义、不动 X1 三段写路径）：不设它 = false。
	TruncatedByBudget bool
	// TruncationReason 是截断原因文本：TruncatedByBudget = true 时为 TurnBudget 钩子
	// 返回的错误文案（宿主侧 = 预算闸门的超限说明，如"累计 X ≥ 上限 Y"），供调用方
	// 直接展示；否则为空。MaxTurns 截断**不写**它（那条截断的原因是结构性的，见
	// TruncatedByMaxTurns）。
	TruncationReason string
}

// New 构造 Loop 并校验必填项。
//
// nil 语义口径（与 core/doc.go 一致）：
//   - **兜底**（缺参数后系统仍能安全地"拒绝"）：Policy=nil → 空引擎 → 一切 deny；
//     Approver=nil → DenyAll；MaxTurns=0 → DefaultMaxTurns；
//   - **编程错误**（返回 error）：Provider / Bus / Registry / Model / SessionID /
//     AgentID 缺失或非法——没有它们连一次安全的模型调用都发不起来。
//
// ContextBudget<0 同样按编程错误拒绝（预算不能为负；0 = 用 DefaultContextBudget）。
func New(cfg Config) (*Loop, error) {
	if cfg.Provider == nil {
		return nil, errors.New("loop: Config.Provider 必填（模型提供方）")
	}
	if cfg.Bus == nil {
		return nil, errors.New("loop: Config.Bus 必填（三段事件要经它落库）")
	}
	if cfg.Registry == nil {
		return nil, errors.New("loop: Config.Registry 必填（工具描述与执行器的来源）")
	}
	if strings.TrimSpace(cfg.Model) == "" {
		return nil, errors.New("loop: Config.Model 必填")
	}
	if err := validateUlid("SessionID", cfg.SessionID); err != nil {
		return nil, err
	}
	if err := validateUlid("AgentID", cfg.AgentID); err != nil {
		return nil, err
	}
	if cfg.MaxTurns < 0 {
		return nil, fmt.Errorf("loop: Config.MaxTurns 不能为负（得到 %d；0 = 用默认 %d）", cfg.MaxTurns, DefaultMaxTurns)
	}
	if cfg.ContextBudget < 0 {
		return nil, fmt.Errorf("loop: Config.ContextBudget 不能为负（得到 %d；0 = 用默认 %d）", cfg.ContextBudget, DefaultContextBudget)
	}
	if cfg.CheckpointEvery < 0 {
		return nil, fmt.Errorf("loop: Config.CheckpointEvery 不能为负（得到 %d；0 = 只手动）", cfg.CheckpointEvery)
	}
	maxTurns := cfg.MaxTurns
	if maxTurns == 0 {
		maxTurns = DefaultMaxTurns
	}
	budget := cfg.ContextBudget
	if budget == 0 {
		budget = DefaultContextBudget
	}
	return &Loop{
		provider:        cfg.Provider,
		bus:             cfg.Bus,
		registry:        cfg.Registry,
		policy:          cfg.Policy,
		approver:        cfg.Approver,
		sessionID:       cfg.SessionID,
		agentID:         cfg.AgentID,
		model:           cfg.Model,
		system:          cfg.System,
		sessionEnv:      cfg.SessionEnv,
		maxTurns:        maxTurns,
		agents:          cfg.Agents,
		reasoningEffort: strings.TrimSpace(cfg.ReasoningEffort),
		contextBudget:   budget,
		store:           cfg.Store,
		checkpointEvery: cfg.CheckpointEvery,
		onlineConfig:    cfg.OnlineTable,
		onlineSource:    cfg.OnlineTableSource,
		ctxStrategy:     cfg.ContextStrategy,
		goalLedger:      cfg.GoalLedger,
		blob:            cfg.BlobSink,
		blobMaxInline:   cfg.BlobMaxInline,
		wal:             newWalWriter(cfg.WalSink),
		tel:             newTelWriter(cfg.TelemetrySink),
		turnBudget:      cfg.TurnBudget,
		runOverrides:    cfg.RunOverrides,
	}, nil
}

// newTelWriter 把 Config.TelemetrySink 包成 loop 内部的 telWriter（nil sink → nil = 关）。
func newTelWriter(sink core.Sink) *telWriter {
	if sink == nil {
		return nil // 未配置 telemetry：零行为变化（酸测试）
	}
	return &telWriter{sink: sink}
}

// newWalWriter 把 Config.WalSink 包成 loop 内部的 walWriter（nil sink → nil = 关）。
func newWalWriter(sink core.WalSink) *walWriter {
	if sink == nil {
		return nil // 未配置 WAL：零行为变化（酸测试）
	}
	return &walWriter{sink: sink}
}

// validateUlid 校验归属 id（S1/S9：不合则拒绝，不修复）。
func validateUlid(field string, id contracts.Ulid) error {
	if id == "" {
		return fmt.Errorf("loop: Config.%s 必填（事件归属）", field)
	}
	if !core.ValidUlid(id) {
		return fmt.Errorf("loop: Config.%s 不是合法 ULID（26 字符 Crockford base32）：%q", field, string(id))
	}
	return nil
}

// Run 跑一次 agent loop：从 prompt 开始，直到模型不再发起工具调用、
// 或达到 MaxTurns、或出错 / ctx 取消。
//
// 错误语义：
//   - ctx 取消 → 立即返回 ctx.Err()（不包装，调用方可用 errors.Is 判定）；
//   - 模型调用失败 → 返回包装后的 error（errors.Is 仍能取到原始错误），并尽力补一条
//     loop.error durable 事件；
//   - intent 事件落盘失败 → 返回 error 且**不执行**该工具调用（X1：没有意图记录就不许有副作用）；
//   - 工具被 deny / 执行失败 / 工具缺失 → **不是** Run 的错误：它们已按三段写路径落
//     failed 事件，错误文案回填给模型让它自我修正（M5），loop 继续。
//
// 每轮模型调用前还会落一条 context.assembled durable 事件（载荷 = contracts.ContextSnapshot，
// 见 core/loop/context.go）：上下文可见性的数据源（06 §4.2）。它是观测性事实，落库失败
// **不中止** loop（只补 loop.error 留痕）——与 X1 的 intent 写路径刻意区分。
//
// 从既有历史续跑用 RunWithHistory。
func (l *Loop) Run(ctx context.Context, prompt string) (Result, error) {
	return l.RunWithHistory(ctx, nil, prompt)
}

// RunWithHistory 带**既有历史**跑一次 agent loop（resume 续跑 / RunPrompt 每轮折叠同源）。
//
// history 是此前的对话历史（如 runtime.FoldConversation 从事件流 fold 出来的对话）：
// 它会**排在本次 prompt 之前**发给模型，本次对话接着它继续（历史本身不被改写——拷贝一份）。
//
// 与 Run 的差别只有一处：起始历史不是空的。控制流、事件写路径（X1 三段写路径一字不动）、
// 旁路观测（user.prompt / model.turn）全同。
//
// 为什么需要它（v1-S1）：对话内容进了事件流（F1）就能被回放重建，但"重建出来的历史"
// 必须能**喂回 loop** 才叫续跑——否则 FoldConversation 只是素材，不是能力。
//
// 谁在用：resume 续跑（cmd/alasol/resume.go）、/btw 旁支 fork（host/fork.go），以及
// **协议面每轮**（host.RunPrompt：每轮 fold 本 session 的事件流带历史——多轮对话同源；
// 批次1 P1-3）。
func (l *Loop) RunWithHistory(ctx context.Context, history []core.Message, prompt string) (res Result, err error) {
	if l == nil {
		return Result{}, errors.New("loop: Loop 为 nil")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	// 每次 Run 的局部可变状态（不进 Loop 字段 → Loop 保持无可变状态以支持并发 Run）。
	st := &runState{}
	// G13 run 归组（2026-11-01 v4-7）：本次 Run 的判别位——Run 起始生成 ULID，随
	// emitFunc 闭包带进每条 model.delta 的 run_id。**Run 局部变量**（不进 Loop 字段，
	// 并发 Run 各持各的）：订阅端按 (run_id, turn) 归组，同 session 并发 Run 不再撞号。
	runID := core.NewUlid()
	// 无论从哪条路径返回，都把本次累积的污点级写进 Result（Q7；空归一为 clean）。
	defer func() { res.Taint = normalizeTaint(st.acc.level) }()

	// v2-S1：挂了对上下文策略时，本次 Run 造一个 fresh Assembler（策略有状态，per-Run 持有），
	// 并收集它注入的框架工具（recall / stage）。nil 工厂 = 不挂 = append（v1 等价）。
	if l.ctxStrategy != nil {
		st.assembler = l.ctxStrategy()
		// **工厂造出 nil Assembler = 装配残缺**（W6-A fail-close）：声明了策略却没装配出
		// 装配器 → 静默走 append 等于"策略语义悄悄变味"（用户以为配了策略，其实跑的是
		// 追加式）。故此处**不静默降级**：留一条 loop.error 留痕 + 以错误终止本次 Run
		// （子 agent 路径上由 runSpawn 收口成 agent.failed）。
		if st.assembler == nil {
			err := errors.New("loop: 上下文策略工厂产出了 nil Assembler（装配钩子残缺）：fail-close，不静默降级成 append")
			l.publishLoopError(ctx, 0, phaseContextStrategy, err)
			return res, err
		}
		tools, err := l.collectStrategyTools(st.assembler)
		if err != nil {
			return res, err // 装配期重名 → fail-close（不许静默遮蔽，F12）
		}
		st.strategyTools = tools
	}

	// J19：goal ledger 生产者（opt-in）。Store 已配 + GoalLedger 开启 → 每 Run 造一个
	// per-(session,agent) 的 LedgerWriter（X6：关键字段立即落盘、过程字段 turn 末合并），
	// 注入 ledger 框架工具。nil store = 即使开了也不注入（fail-close：没库就没法持久化账本）。
	//
	// **基线必须加载当前账本**（p1 修复）：ledger.updated 是全量快照 + LWW——若写入器从空
	// 账本起步，Run2 只更新过程字段（completed）时就会把 Run1 写下的 goals/constraints
	// 覆盖成空（关键字段被静默抹掉）。故此处先 LoadLedger 折出当前快照做基线；
	// **读不到就不写**（fail-close：宁可这一轮失败，也不拿空基线覆盖关键字段）。
	if l.goalLedger && l.store != nil {
		cur, lerr := durable.LoadLedger(ctx, l.store, l.sessionID)
		if lerr != nil {
			return res, fmt.Errorf("loop: 读当前 goal ledger 失败（不写账本：宁可不写也不抹掉关键字段）: %w", lerr)
		}
		w := durable.NewLedgerWriter(l.store, l.sessionID, l.agentID)
		w.Seed(cur)
		st.ledger = w
		if err := l.addLedgerTool(st); err != nil {
			return res, err // ledger 工具与既有工具重名 → fail-close
		}
	}

	// 每轮历史都是局部变量：同一 Loop 可被并发 Run 而不互相污染。
	// 既有历史先**拷贝**一份（调用方可能复用那个切片 → 不能被本次 Run 改写）。
	msgs := make([]core.Message, 0, len(history)+8)
	msgs = append(msgs, history...)
	msgs = append(msgs, core.Message{Role: "user", Content: prompt})

	// Run 开始：把用户 prompt 落进事件流（F1：对话内容是事实源的一部分，回放/resume 据此
	// 重建完整对话）。旁路观测——best-effort、不计 Result.Events、不改控制流（见 events.go）。
	l.publishUserPrompt(ctx, prompt)

	// G-c/G-d 生效链第三层：**本轮快照在 Run 开始冻结**（轮 = 一个 prompt 的 Run）。
	//
	// 钩子每次 Run 重取（宿主侧：扫平事件流 → sessioncfg fold → 叠 hub 默认），取到的
	// 覆盖冻结进 st.overrides——本轮之后所有轮次的请求组装、model.turn 的 model 字段、
	// provider.Run 的调用方都只读这份快照。于是**运行中落的 config.changed 不影响本轮**，
	// 下一个 prompt 的 Run 重取即生效（设计 15 §3.1"当前轮结束后下一轮生效"）。
	//
	// 放在 user.prompt **之后**：冻结失败（fail-close，如覆盖的环境变量缺失）时本轮的
	// prompt 已是事件流里的事实——下一轮 fold 历史仍看得到用户说过什么，不会因为一次
	// 配置错误把用户的话丢出对话。取快照失败以错误终止本轮（不带未定配置开跑）。
	// nil 钩子 = 不挂（fork/resume/cmd 直连的 loop）→ 零行为变化。
	if l.runOverrides != nil {
		ov, oerr := l.runOverrides(ctx)
		if oerr != nil {
			return res, fmt.Errorf("loop: 取本轮覆盖配置失败（fail-close，不带未定配置开跑）: %w", oerr)
		}
		st.overrides = ov
	}

	// L2/D19 telemetry：run.begin（user.prompt 之后、第一轮之前）+ deferred run.end
	//（正常/出错/截断都发）。发点全在 X1 三段写路径**之外**；nil sink = 关 = 零行为变化。
	// defer 注册在 publishUserPrompt **之后**：setup 期失败（策略/ledger）没有 run.begin，
	// 也就不该有 run.end（成对口径）。命名返回值让 defer 看到最终 res/err。
	l.telRunBegin()
	telStarted := time.Now()
	defer func() { l.telRunEnd(res, err, time.Since(telStarted)) }()

	var lastText string

	for turn := 1; turn <= l.maxTurns; turn++ {
		// ctx 取消优先于任何模型调用：立即返回，不再发起新副作用。
		if err := ctx.Err(); err != nil {
			res.Turns = turn - 1
			res.Text = lastText
			return res, err
		}
		// 逐轮预算制动（v4-4-B 缺口 5）：每轮 provider.Run **之前**查一次（与 turn.begin
		// 写点同位置区间；顺序：先查预算 → 过了再写 turn.begin → provider.Run）。nil 钩子
		// = 不查（默认，零行为变化）。超限 → 本轮**不发起**，照 MaxTurns 截断的口径收口：
		// Run 正常返回（不是 error），TruncatedByBudget 显式标记 + TruncationReason 带上
		// 钩子文案；已跑轮次的结果照常收口。事件流不为被拒的轮次新增任何事件（没有
		// context.assembled / model.turn / turn.begin——这轮什么都没跑）。
		if l.turnBudget != nil {
			if berr := l.turnBudget(ctx); berr != nil {
				res.Turns = turn - 1 // Turns = 实际发生的模型调用轮数（被拒的这轮不算）
				res.Text = lastText
				res.TruncatedByBudget = true
				res.TruncationReason = berr.Error()
				return res, nil
			}
		}
		res.Turns = turn

		// 在线表（K14/K15）：每轮新鲜渲染（秒级变动），仅主 agent。nil = 本轮不注入。
		ot := l.onlineTableForTurn(ctx)

		// 组这一轮的请求（可见工具过滤后的结果，R5/H4；在线表追加以最末尾，Q4/Q3）。
		// 挂了 context_strategy（v2-S1）→ 由策略装配 history 区（压缩/注入/降级），并产出
		// context.assembled 的 stage:N/ledger/rules parts；不挂 → 原样 request()（v1 等价）。
		var req core.ModelRequest
		var stratParts []contracts.ContextPart
		if st.assembler != nil {
			req, stratParts = l.requestWithStrategy(ctx, msgs, ot, turn, st)
		} else {
			req = l.request(msgs, ot, st)
		}

		// 每轮模型调用**前**落一条 context.assembled（06 §4.2：上下文可见性的数据源）。
		// 此刻装配已定型（req 就是模型将看到的全部），记录"这一轮给模型看了什么"；
		// 落库失败不中止 loop（观测性事实，不是执行前提，见 publishContextAssembled）。
		res.Events += l.publishContextAssembled(ctx, turn, req, ot, stratParts)

		// v4-4-B：turn.begin——provider.Run **之前**写 WAL（"第 N 轮在跑 X 模型"的现场）。
		// WAL 是崩溃恢复补充面、不是事实源（F5 不受影响）；写在 X1 三段路径之外，失败
		// 不阻断主流程（一次 Warn，见 wal.go）。
		l.walTurnBegin(turn, req)

		// v4-4-A（07 M6/M7）**决策：这里不包 core/retry**。
		//
		// 两层重试会放大：provider 内部已对 429/5xx 与网络瞬断小重试（3 次指数退避），
		// loop 再包一层就是"3×3 次请求 + 两段退避相加"——单次模型调用的耗时与端点压力
		// 都成倍涨，而收益接近零（provider 已经覆盖了同一批瞬时失败）。
		// 更要紧的是 loop **认不出**哪些失败值得再试：它只看到 core.Provider 的一个 error，
		// 没有状态码/分类面；凭"是个 error"就重试会把 4xx 请求侧错误也重发三遍。
		// 因此重试口径收敛在 provider 一处（runtime/provider/openai 的 providerRetryPolicy），
		// 工具调用侧的重试另走 X1 三段写路径 + 契约 Idempotent/Replay 声明（M7），
		// 不由本方法代劳。
		// G-d provider 覆盖：本轮用快照里的覆盖 provider（宿主按 base_url +
		// api_key_env 构造；凭据值只在宿主侧从环境变量取），没有覆盖（nil）→ 用
		// loop 基值 provider（hub 的单 provider，现行为）。快照在 Run 开头冻结 →
		// 运行中换端点不影响本轮。
		prov := l.provider
		if st.overrides.Provider != nil {
			prov = st.overrides.Provider
		}
		mr, err := prov.Run(ctx, req, l.emitFunc(ctx, turn, runID))
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				// 取消导致的失败按取消处理（不要把"被取消"包装成模型错误）。
				// v4-4-B：turn.failed——这一轮没跑完（取消），但**不是崩溃**：不写的话
				// WAL 里留下悬空的 turn.begin，对账会误报"疑似崩溃残留"。
				l.walTurnFailed(turn, ctxErr)
				res.Text = lastText
				return res, ctxErr
			}
			res.Events += l.publishLoopError(ctx, turn, "provider", err)
			// v4-4-B：turn.failed——模型调用失败是**普通失败**，不是崩溃：写一条失败收口，
			// 对账据此把它与"崩溃残留"区分开（begin 后跟 failed = 正常失败，不算在飞）。
			l.walTurnFailed(turn, err)
			res.Text = lastText
			return res, fmt.Errorf("loop: 第 %d 轮模型调用失败: %w", turn, err)
		}
		lastText = mr.Text

		// 每轮 provider 返回后落一条 model.turn（F1：模型响应入事件流；含 usage 只做成本记录，J21）。
		// 旁路观测——best-effort、不计 Result.Events、不改控制流（见 events.go）。在判定本轮是否
		// 收敛**之前**落，故收敛轮（最终答复）与工具调用轮都各有一条。
		l.publishModelTurn(ctx, turn, mr, st)

		// v4-4-B：turn.end——model.turn **durable 落盘之后**写 WAL（"这轮的事实已进事件流"）。
		// 顺序是有意的：反过来写会让"两者之间崩溃"被报告成干净收场（假阴性比漏报更坏），
		// 详见 wal.go 的 walTurnEnd 说明。
		l.walTurnEnd(turn, mr)

		// 模型不再发起工具调用 → 收敛，返回最终文本。
		if len(mr.ToolCalls) == 0 {
			res.Text = mr.Text
			return res, nil
		}

		// assistant 消息：ToolCallReq 原样入历史（provider id 黑盒，S4）。
		msgs = append(msgs, core.Message{
			Role:      "assistant",
			Content:   mr.Text,
			ToolCalls: mr.ToolCalls,
		})
		for _, tc := range mr.ToolCalls {
			res.ToolCalls++
			if l.isAgentTool(tc.Name) {
				// 多 agent 生命周期工具（spawn / complete）。
				text, events, stop, err := l.runAgentTool(ctx, turn, tc, st)
				res.Events += events
				if err != nil {
					res.Text = lastText
					return res, err
				}
				msgs = append(msgs, core.Message{Role: "tool", Content: text, ToolCallID: tc.ID})
				if stop != nil {
					// complete：本（子）loop 就此收敛，Result 带上结构化 summary。
					res.Summary = stop
					res.Text = stop.Text
					return res, nil
				}
				continue
			}
			// L2/D19 telemetry：tool.call——runToolCall **返回后**（completed/failed 已落盘、
			// X1 三段窗口之外）。ok = 写路径是否走通（err==nil）；dur 含落盘与执行。
			telT0 := time.Now()
			text, events, err := l.runToolCall(ctx, turn, tc, st)
			l.telToolCall(turn, tc.Name, err == nil, time.Since(telT0))
			res.Events += events
			if err != nil {
				// 写路径断裂（intent 没落盘 / ctx 取消）：中止，不编造结果。
				res.Text = lastText
				return res, err
			}
			// 工具结果回填（role="tool"，ToolCallID 原样，S4）。
			msgs = append(msgs, core.Message{
				Role:       "tool",
				Content:    text,
				ToolCallID: tc.ID,
			})
		}

		// 阶段 8：turn 末（所有工具调用已解析、无 in-flight）按需自动 checkpoint（X3）。
		// 尽力而为：落盘失败不中止 Run（Run 的事件已 durable，缺 checkpoint 只让 resume 退到全量重建）。
		l.maybeCheckpoint(ctx, turn)

		// J19/X6：turn 末把缓冲的过程性 ledger 变更合并落**一条** ledger.updated。
		// 尽力而为（过程字段非关键，落不下只在事件流补 loop.error 留痕，不中止 Run）。
		l.flushLedger(ctx, turn, st)
	}

	// MaxTurns 截断：模型还在要工具但轮数用尽（防呆护栏生效）。
	//
	// **显式标记**截断（Result.TruncatedByMaxTurns）：走到这里 = 轮数用尽时模型**仍在**
	// 发起工具调用（收敛的那轮会提前 return，不会落到这儿）。消费方据此判"这轮没跑完"
	// （如 ACP 端点 → stopReason=max_turn_requests），不必靠"Turns ≥ 上限"去猜
	// （正常收敛在最后一轮时 Turns 也等于上限）。
	res.TruncatedByMaxTurns = true
	res.Text = lastText
	return res, nil
}

// request 组装这一轮的模型请求（状态机第 1 步）。
//
// 可见工具 = registry 里 kind=tool、有 Tool 描述、且**策略未拒绝**的条目，
// Schema 原样传给模型（R5/R6：不暴露即不存在）。
//
// H4 可见集过滤：裸工具名被策略 deny → 移出模型可见集（模型看不到 = 不会去调用）。
// 这里只做**可见性**过滤；执行侧仍独立判定（M9：判定发生在调用时刻，判定只约束
// 发起时刻——可见集过滤不放宽任何执行侧检查）。
//
// ot 非空（在线表注入）时，把在线表消息**拷贝后追加到最末尾**（Q4/Q3：history 之后）。
// 拷贝是必须的——online table 只进本轮请求，不能写进调用方 history 的底层数组（下一轮还要
// 往 msgs 追加 assistant/工具消息）。ot 为 nil 时零拷贝（老路径一字不变）。
//
// st 携带本次 Run 的框架工具（v2-S1 策略的 recall/stage + v3-S3 J19 的 ledger）——它们也进
// 模型可见集（nil = 无框架工具，与旧版等价）。
func (l *Loop) request(history []core.Message, ot *onlineTable, st *runState) core.ModelRequest {
	msgs := history
	if ot != nil {
		msgs = make([]core.Message, len(history), len(history)+1)
		copy(msgs, history)
		msgs = append(msgs, ot.message)
	}
	return core.ModelRequest{
		Model:           l.modelFor(st), // G-c/G-d：本轮快照覆盖优先，空 = loop 基值
		System:          l.system,
		Messages:        msgs,
		Tools:           l.visibleTools(strategyToolsOf(st)), // 框架工具（策略 + ledger）
		ReasoningEffort: l.reasoningEffortPtr(st),            // 思维链档位（G14；nil = 不传）
	}
}

// modelFor 取本次 Run **生效的模型名**（G-c/G-d 生效链第三层）：本轮快照覆盖非空 →
// 用覆盖；空 → loop 基值（Config.Model，hub 默认）。st 只读（冻结在 runState，见
// RunWithHistory 的冻结点）——并发 Run 各持各的快照。
func (l *Loop) modelFor(st *runState) string {
	if st != nil && st.overrides.Model != "" {
		return st.overrides.Model
	}
	return l.model
}

// reasoningEffortPtr 把档位折成请求字段（G14）：空 = nil（provider 不下发
// reasoning_effort，端点用自己的默认档）；非空 = 指针（provider trim 后原样透传）。
//
// 档位来源分两层（G-c/G-d 生效链）：本轮快照覆盖优先（指针指向 st.overrides 的字段——
// st 是 Run 局部、活到本轮结束，请求组装与消费都在本轮内，故安全）；否则回 loop 基值
// 字段（它在 New 之后不可变，指针跨并发 Run 只读，与旧版等价）。
func (l *Loop) reasoningEffortPtr(st *runState) *string {
	if st != nil && st.overrides.ReasoningEffort != "" {
		return &st.overrides.ReasoningEffort
	}
	if l.reasoningEffort == "" {
		return nil
	}
	return &l.reasoningEffort
}

// requestWithStrategy 是挂了 context_strategy（v2-S1）时的请求组装：先让策略把 history
// 装配成压缩/注入后的上下文，再（若有在线表）把在线表追加到**最末尾**（Q4/Q3）。
//
// 与 request() 的差别只在 history 区经策略变换；system / tools / 在线表位置都不变。
// 策略**失败时 fail-open 到 append**（用原始 history）+ 尽力补一条 loop.error 留痕：
// 策略是上下文管理器不是安全边界，装配出错时让模型看到完整原始历史是已知安全的退路
// （= v1 行为），比中止整个 agent 更可用；失败事实经 loop.error(phase=context.strategy) 留痕。
//
// 装配前先把两件事告诉策略（都是策略自己看不到的上下文）：
//   - 本轮固定开销（system + 工具 schema）→ 策略据此守"base+tools+history+current ≤ W"
//     的总量护栏（F2：两个预算各自放行会相加超窗）；
//   - 本轮模型可见的工具名 → 策略据此决定摘要条目要不要附 recall 说明（F27）。
func (l *Loop) requestWithStrategy(ctx context.Context, history []core.Message, ot *onlineTable, turn int, st *runState) (core.ModelRequest, []contracts.ContextPart) {
	tools := l.visibleTools(st.strategyTools)
	if setter, ok := st.assembler.(compaction.BaseOverheadSetter); ok {
		// 在线表也是本轮上下文的一部分（loop 追加在末尾，策略看不见）→ 计入固定开销，
		// 否则总量护栏会漏算它。
		base := l.baseTokens(tools)
		if ot != nil {
			base += ot.tokens
		}
		setter.SetBaseTokens(base)
	}
	if setter, ok := st.assembler.(compaction.ToolVisibilitySetter); ok {
		setter.SetVisibleToolNames(l.toolNames(tools))
	}
	msgs := history
	var parts []contracts.ContextPart
	assembled, err := st.assembler.Assemble(ctx, history, turn)
	if err != nil {
		l.publishLoopError(ctx, turn, phaseContextStrategy, err)
	} else {
		msgs = assembled.Messages
		parts = assembled.Parts
	}
	if ot != nil {
		out := make([]core.Message, len(msgs), len(msgs)+1)
		copy(out, msgs)
		msgs = append(out, ot.message)
	}
	return core.ModelRequest{
		Model:           l.modelFor(st), // G-c/G-d：本轮快照覆盖优先，空 = loop 基值
		System:          l.system,
		Messages:        msgs,
		Tools:           tools,
		ReasoningEffort: l.reasoningEffortPtr(st), // 思维链档位（G14；nil = 不传）
	}, parts
}

// baseTokens 本轮固定开销（system 提示 + 模型可见工具 schema）的 token 估算（J21 口径）。
//
// 它是**总量护栏**的一半（另一半由策略自己算：注入区 + 历史区 + 当前阶段）。
func (l *Loop) baseTokens(tools []contracts.Tool) int64 {
	n := estimateTokens(l.system)
	for i := range tools {
		n += estimateToolTokens(&tools[i])
	}
	return n
}

// toolNames 取工具名列表（给策略判断某个框架工具是否可见）。
func (l *Loop) toolNames(tools []contracts.Tool) []string {
	out := make([]string, 0, len(tools))
	for i := range tools {
		out = append(out, tools[i].Name)
	}
	return out
}

// collectStrategyTools 收集本次 Run 的策略框架工具，并做**重名 fail-close 检查**（F12）。
//
// 同名工具（策略框架工具 vs 注册表工具、或两个框架工具彼此同名）会让"可见集"与"执行解析"
// 变成两套含义：模型看到两个同名工具，执行时却只跑其中一个（框架工具还会遮蔽注册表工具）。
// 装配期直接报错拒绝（fail-close），不做静默遮蔽——要豁免就把工具名写进 agent 类型的
// tools 白名单（见 visibleTools 的类型过滤）。
func (l *Loop) collectStrategyTools(a compaction.Assembler) (map[string]compaction.FrameworkTool, error) {
	tp, ok := a.(compaction.ToolProvider)
	if !ok {
		return nil, nil
	}
	tools := tp.Tools()
	out := make(map[string]compaction.FrameworkTool, len(tools))
	for _, ft := range tools {
		name := strings.TrimSpace(ft.Desc.Name)
		if name == "" {
			return nil, errors.New("loop: 上下文策略注入的工具没有名字（装配残缺）")
		}
		if _, dup := out[name]; dup {
			return nil, fmt.Errorf("loop: 上下文策略注入了重名工具 %q（fail-close：同名工具的执行语义必须唯一）", name)
		}
		if _, known := l.registry.Get(name); known {
			return nil, fmt.Errorf("loop: 上下文策略注入的工具 %q 与注册表工具重名（fail-close：同名工具的执行语义必须唯一；请改框架工具名或移除注册表里的同名工具）", name)
		}
		out[name] = ft
	}
	return out, nil
}

// visibleTools 取当前可见工具描述（注册顺序 = 模型看到的顺序，确定性）。
//
// 过滤口径（H4 字面）：**deny = 移出模型可见集**（裸工具名 deny）；
// ask 保持可见——审批回路要靠模型发起调用才走得通，人审发生在调用时刻。
//
// **注册表 State 门控（缺口 3，第一层）**：条目 State=disabled（Disable / 能力裁决失败后的
// revokeEnabled）→ 不进模型可见集——与 runtime/mcpserver 的"停用不暴露"（F1）同口径。
// 这是双层口径的**第一层**（模型看不到就不会调）；"list 之后才被停用"的运行期窗口由
// 执行前复查兜底（第二层，见 runToolCall 里 resolveTool 之后的 State 复查）。已注销条目
// 本来就不在 List 结果里。策略框架工具（strategyTools）不经注册表、没有 State，不受影响。
//
// 多 agent（阶段 5）在此叠加两层：
//   - **type.Tools 过滤**（R5：不暴露即不存在）：若本 agent 在树中且有类型，则只有该类型
//     Tools 列内的注册工具可见（子 loop 的 tools=type.Tools）；
//   - **生命周期工具注入**：can_spawn 非空 → 注入 spawn；子 agent（depth>1）→ 注入 complete。
//     二者由类型可达性/深度控制（K20/K21），不经注册表/policy（三闸门才是授权）。
//
// **框架工具（策略注入的 recall / stage）同样过 type.Tools 白名单**（F1）：框架工具不是
// 免检通道——"不暴露即不存在"对它们一样成立。要豁免就把工具名写进该 agent 类型的
// tools 列表（显式豁免，不许静默绕过）。
//
// strategyTools 是本次 Run 的上下文策略注入的框架工具（v2-S1：recall / stage；per-Run，
// 由调用方从 runState 传入；nil = 无策略框架工具）。可见集**按名字去重**（F12）：
// 同一个名字只出现一次（执行侧的解析优先级与可见集保持一致：策略框架工具优先，
// 但同名在装配期就已 fail-close 拒绝，见 collectStrategyTools）。
func (l *Loop) visibleTools(strategyTools map[string]compaction.FrameworkTool) []contracts.Tool {
	var (
		typeFilter map[string]bool // 非 nil 时按类型 Tools 收窄注册工具
		canSpawn   bool
		isSubagent bool
	)
	if l.agents != nil && l.agents.Tree != nil {
		if node, ok := l.agents.Tree.Get(l.agentID); ok {
			isSubagent = node.Depth > 1
			if ct, ok := l.agents.Tree.TypeOf(l.agentID); ok {
				typeFilter = stringSet(ct.Tools)
				canSpawn = len(ct.CanSpawn) > 0
			}
		}
	}

	entries := l.registry.List(registry.ExtensionKindTool)
	out := make([]contracts.Tool, 0, len(entries)+2)
	seen := map[string]bool{}
	for _, e := range entries {
		if e.Tool == nil {
			continue // 非工具扩展（kind 不匹配时已被 List 过滤；Tool 为 nil 是装配残缺）
		}
		if e.State == registry.StateDisabled {
			// 缺口 3 第一层（F1 同口径）：停用的条目不进模型可见集（停用不暴露）。
			// 执行侧另有第二层复查（runToolCall），防"可见集刷新前的运行期停用窗口"。
			continue
		}
		if typeFilter != nil && !typeFilter[e.Tool.Name] {
			continue // R5：类型未列出的工具不暴露
		}
		if !l.toolVisible(e.Tool.Name) {
			continue // H4：被 deny 的工具移出模型可见集
		}
		if seen[e.Tool.Name] {
			continue // F12：可见集按名字去重
		}
		seen[e.Tool.Name] = true
		out = append(out, *e.Tool)
	}

	// 生命周期工具注入（阶段 5）。它们**也要过策略**（P2-4：spawn/complete 不许绕过
	// 策略）：被 deny 的生命周期工具不进模型可见集。
	if l.agents != nil && l.agents.Tree != nil {
		if canSpawn && l.toolVisible(ToolSpawn) && !seen[ToolSpawn] {
			seen[ToolSpawn] = true
			out = append(out, spawnToolDesc())
		}
		if isSubagent && l.toolVisible(ToolComplete) && !seen[ToolComplete] {
			seen[ToolComplete] = true
			out = append(out, completeToolDesc())
		}
	}

	// 上下文策略的框架工具（v2-S1：recall / stage）。与 spawn/complete 同款：也过策略
	// （deny 的不进可见集）与 agent 类型白名单（F1），按名字排序保证确定性（map 迭代无序）。
	if len(strategyTools) > 0 {
		names := make([]string, 0, len(strategyTools))
		for name := range strategyTools {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if typeFilter != nil && !typeFilter[name] {
				continue // F1：类型未列出的工具不暴露（框架工具也不例外）
			}
			if !l.toolVisible(name) {
				continue // H4：被 deny 的工具移出模型可见集
			}
			if seen[name] {
				continue // F12：可见集按名字去重
			}
			seen[name] = true
			out = append(out, strategyTools[name].Desc)
		}
	}
	return out
}

// stringSet 把字符串列表转成集合（type.Tools 过滤用）。
func stringSet(list []string) map[string]bool {
	m := make(map[string]bool, len(list))
	for _, s := range list {
		m[s] = true
	}
	return m
}

// toolVisible 判定工具是否在模型可见集里（H4：裸工具名 deny = 移出可见集）。
//
// 只用 Evaluate（**不**走 Decide）：可见性过滤不该把人审叫起来——
// 询问发生在调用时刻（M9/D14）。ask / allow 都可见；只有 deny
// （含 fail-close 无命中、非法规则污染）才隐藏。
func (l *Loop) toolVisible(name string) bool {
	dec := l.policy.Evaluate(context.Background(), policy.Query{Tool: name})
	return dec.Verdict != contracts.EffectDeny
}

// emitFunc 造第 turn 轮的流事件出口（G13：点亮 v0 那个故意留空的 emit 扩展点）。
//
// provider 边收边发的增量片段经这里 Publish 成 **durable=false** 的 model.delta 流事件
// （F8/陷阱 11：不入库、不占 seq、不进 Result.Events、回放面没有它——见 publishModelDelta）。
// 其余 kind（toolcall.* / done / error）v0 仍不转发：零行为变化（X1 三段写路径与
// user.prompt/model.turn 旁路的形状一字不动，delta 是**新旁路**）。
//
// 为什么是工厂而不是 Loop 方法：delta 载荷要带**本轮次号**（ModelDeltaPayload.turn）与
// **本次 Run 的判别位**（ModelDeltaPayload.run_id，G13 归组），发布还要受 Run 的 ctx
// 约束——三者都是 Run 的局部量，闭包捕获即可，Loop 本身保持无可变状态（并发 Run 安全，
// 与 harness 的 mockProvider 也照旧工作）。
func (l *Loop) emitFunc(ctx context.Context, turn int, runID contracts.Ulid) func(core.ProviderEvent) {
	return func(ev core.ProviderEvent) {
		switch ev.Kind {
		case core.KindTextDelta, core.KindReasoningDelta:
			// v4-4-B：turn.delta——流式片段**逐条**写 WAL（model.delta 是 durable=false 的
			// 流事件：不入库、不回放，崩溃后本轮输出整轮蒸发；WAL 至少多保存一些）。
			// 预写口径：WAL 先、流事件后（WAL 是"正式写之前"的中间态）。
			l.walTurnDelta(turn, ev)
			l.publishModelDelta(ctx, turn, runID, ev)
		case core.KindProviderRetry:
			// 缺口 5 第二件：provider 重试留痕 → WAL turn.retry（写点在三段写路径
			// **之外**——重试发生在 provider.Run 内部，本来就不在 intent→completed/
			// failed 窗口里）。纯观测：不进事件流、不占 Result.Events、不改控制流；
			// WAL 关（sink nil）→ walAppend 直接返回，零行为变化。
			l.walTurnRetry(turn, ev.Retry)
		}
		// 其它 kind（toolcall.start/delta、done、error）：v0 不转发。工具调用的
		// 事实已由 X1 三段写路径完整记录（intent/completed/failed），流式副本是冗余。
	}
}

// nowRFC3339 返回 UTC 的 RFC 3339 时间戳（契约 ts 字段格式；与 policy 同口径）。
func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }
