package host

// 本文件是 G-f standby 待机工具的**工具本体**（设计 15 §4）。
//
// # 语义（钉死的口径）
//
//	standby({wait: "message"|"handoff", timeout_min: 1..30, on_timeout: "return"|"sleep"})
//
//	调用 → 本轮挂起：intent 已由 X1 落盘（本工具**不碰三段路径**），
//	completed 延迟到「唤醒」或「超时」——挂起期间零模型轮、零事件、零 WS。
//
// # 三件事的归属
//
//	① waiting 态：发现表的 BeginWait/EndWait 配平（豁免活跃窗口判定，
//	   但**不刷**最后事件 ts——挂起期本工具不落任何事件，天然满足）。
//	② 唤醒判定：轮询事件流（**不是** bus 订阅——别的 hub 写的事件订阅收不到），
//	   用 core/agentlink 的 Link.Inbox（K11 addressedTo 已实现）：
//	   只认**投递给本 session** 的对应 kind 消息；别的 session 的消息不唤醒。
//	③ 超时收口：on_timeout="return" 等满 timeout_min 就如实收口（timeout_min 是
//	   超时点）；on_timeout="sleep" **不看 timeout_min**，一直挂到硬上限
//	   standbyHardCap（消息到达或硬上限为止）。
//
// # 归属 session 与「重放游标」从哪来（以及这里的一处已知局限）
//
//	core.ToolExecutor 的 Execute 入参只有 ctx + intent 载荷（ToolCallID /
//	Arguments / …），**不带 session_id、也不带 seq**；ctx 里也没有（core 只往
//	ctx 塞执行 env）。所以本工具按 tool_call_id 回查事件流里那条 tool.call.intent
//	事件（X1 的配对键，S4）：envelope.session_id = 本次等待归属的 session；
//	envelope.seq = 重放游标起点（"本次 tool.call.intent 的位置"）。
//
//	为什么游标取 intent 的 seq 而不是"现在"：resume 重放（Replay=safe 重新
//	执行）时，**中断期间到达**的消息 seq 大于原 intent 的 seq → 用 intent 的
//	seq 当起点，重放就能看见它们（消息已到 → 立即 completed 如实收口；
//	未到 → 重新挂起）。
//
//	**局限**：拿不到 intent 事件时，任务书允许的降级是"记当前 seq 当游标"——
//	本实现照做（执行器启动时记流水位，失败文案里如实报出这个降级值），但
//	**归属 session 无从确定**，而它同时决定 waiting 态挂在谁身上与 K11 唤醒
//	过滤按谁算（两处都错 = 挂到别人的 session 上）。故这种情况**明确失败**
//	（不猜 session），而不是拿降级游标硬等。
//
// # 唤醒游标的盲区与它的补法（F1：游标**之前**到达的消息）
//
//	Inbox 的 SinceSeq 是**严格大于**（见 core/agentlink/message.go）→ 游标取
//	本次 intent 的 seq → **intent 之前到达的同类消息永远看不见**。典型场景
//	（A 派活 → B 秒回 → A 下一轮才调 standby）必然假阴性：那条消息的 seq 小于
//	本次 intent 的 seq → 睡满超时后如实报"未等到"，而那条消息**模型没有第二条
//	路看到它**（agentlink 消息不进 FoldConversation）。
//
//	故等待开始时先做一次**收件箱检查**（宽范围 + 交付集合排除，见下）：
//	  - 命中"已到达的同类消息"（wait 过滤 + K11 addressedTo 照旧）→ **立即
//	    completed**，正文注明「（早于本次等待已到达）」并带消息摘要；
//	  - 未命中 → 转**增量游标轮询**（游标仍是本次 intent 的 seq）。
//
//	**性能面（如实写）**：等待前多一次**宽范围的收件箱扫描**（每事件约 2 微秒，扫全流
//	再按交付集合排除），之后才是增量轮询（每 tick 一次，从本次 intent 的 seq 起，
//	同样按集合排除）。resume 重放路径同理（同一个执行器、同一个 wait）：重放起点也查一次
//	——中断期间到达的（seq > intent）与等待前到达的（seq < intent）都能被收口。
//
// # 已交付集合（交付去重：旧消息不重复交付）
//
//	消息**不删** + K11 按**读时**成员关系判 → 若每次早到检查都从流头扫、且不排除
//	已交付的，历史上**任何**投递给本 session 的同类消息都会命中 → 同 session 连调
//	两次 standby 会**逐字重复交付**同一条旧消息（第二次 2ms 空转没等）；多轮协作里
//	"A 再等 C 的回话"会拿到 B 的旧回话、误判为新回复。
//
//	故 completed 的结果里落**交付标记**（Content[0].Data = {"delivered_seq": N}，
//	见 standbyDelivery）：下一次 standby 的早到检查**排除**已交付过的那些消息。
//
//	**排除用的是"集合"，不是"水位"**（critic 终验：水位是个不准的代理）：
//
//	  - 水位（"已交付的最大 seq"）会把**别的 kind** 的旧消息（handoff）顺带盖掉
//	    （seq 更小 → 落在水位之下 → 永远捞不到），也会把同 kind 的多条未交付消息
//	    永久盖住（只交付最新那条，旧的再也捞不到）；
//	  - 集合排除只问"这条消息**具体**交付过没有"：跨 kind 天然正确；同 kind 多条
//	    未交付 → 最新先交付、旧的**下次有资格**（逐条补交）。
//
//	认领键 = **CausationID**（completed 指向同一次调用的 intent 事件 id）：completed
//	载荷里没有工具名，按 tool_call_id 认领会误认领别的工具（provider 复用 id 时）。
//	标记被**外置成 blob** 时经 blob 读回（读不到 → 按空处理 + warn：可能重复交付，
//	但绝不隐藏已交付的证据）。
//
// # 诚实边界：waiting 态是**进程本地**的
//
//	发现表的 waiting 态（BeginWait/EndWait）只在本进程有效（一库一 hub 拓扑内
//	有效）；跨 hub / 跨容器的"对端还挂着没有"**看不见**——waiting 要落事件流
//	才能跨进程可见，而那与"挂起期零事件"的约束**冲突**（见文件头 ②）。
//	故跨容器防掉线**不成立**：记挂账（waiting 落事件流 = 后续单独一刀）。
//
// # 诚实边界：交付集合是 **session 级**的
//
//	已交付集合按**本 session** 的事件流折（completed 指向同一次调用的 intent 事件
//	id）→ 同一 session 里的**不同 agent 共享**同一份交付记录（谁交付过，对全 session
//	都算"已交付"）。而**可见性**仍逐条按 K11 判（某条消息对某个 agent 不可见 →
//	它不会成为它的候选）。跨 session **不共享**（各折各的）——agent 间不共享可见性
//	这一点与 K11 一致（别 session 的消息互相看不见）。
//
//	含义（不改行为，只写明）：子 agent 的 standby 与父 agent 的 standby 在同一
//	session 里时，父 agent 交付过的那条消息，对子 agent 也不再重复交付。
//
// # 时间口径（原子量：测试把超时路径缩到毫秒级；生产恒为下面这些值）
//
//	standbyMinute()       ：一"分钟"的实际时长（生产 = 1 分钟）。
//	standbyHardCap()      ：一轮待机的硬上限（生产 = 30 分钟；sleep 模式的最终兜底）。
//	standbyPollInterval() ：唤醒检测的轮询间隔（生产 = 2 秒）。
//
//	三个旋钮**独立**：改哪个只影响哪条，生产值不变。读取走访问器（≤0 → 默认值，
// 防 NewTicker panic）；测试写走 atomic（无锁写也不出数据竞争）。

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync/atomic"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/storage"
)

// ToolStandby 是 standby 待机工具名（注册表 key / 模型调用名 / 事件里三处一致）。
const ToolStandby = "standby"

// standby 的参数取值（模型可见集与校验共用同一份口径）。
const (
	// standbyWaitMessage 等 agentlink.broadcast（对端广播）。
	standbyWaitMessage = "message"
	// standbyWaitHandoff 等 agentlink.handoff（对端交接）。
	standbyWaitHandoff = "handoff"
	// standbyWaitEvent 等任意事件流条件——**本批未实现**（G-j 第二批才落）。
	standbyWaitEvent = "event"

	// standbyOnTimeoutReturn 到点即如实收口（回答"未等到"）。**timeout_min 是超时点**。
	standbyOnTimeoutReturn = "return"
	// standbyOnTimeoutSleep 一直挂到硬上限（消息到达或 standbyHardCap() 为止）。
	//
	// **timeout_min 在这条路径上不适用**（critic F3）：本模式的语义是"等到消息或
	// 30 分钟硬上限"，timeout_min **只**在 return 模式有意义（它是那模式的超时点）。
	// 旧口径把 timeout_min 当"软检查点"（到点续挂）——但软检查点**没有任何可观测
	// 差异**（既不返回、也不落事件、也不改文案），只是让模型以为它有意义。故改成
	// 明确文档化（timeout_min 不适用），**不做**"软检查点返回"（那会产生额外模型轮，
	// 违反"零模型轮"的不变式）。
	standbyOnTimeoutSleep = "sleep"
)

// standby 的时间口径（**原子量**：测试把超时路径缩到毫秒级时是并发写）。
//
// 为什么不是裸包变量（critic F7）：测试无锁写这些变量，而后台 goroutine（等待中的
// 轮询、别处仍在跑的用例）同时在读 → 裸变量 = **数据竞争**（-race 会报）。
// 故改成 atomic.Int64（存**纳秒数**），读走访问器（≤0 → 回落默认值，顺带防住
// time.NewTicker(≤0) 的 panic）。
//
// 生产值（默认值）见下面的常量；**三个旋钮独立**：改哪个只影响哪条。
const (
	// standbyMinuteDefault 是一"分钟"的实际时长（生产 = 1 分钟）。
	standbyMinuteDefault = time.Minute
	// standbyHardCapDefault 是一轮待机的硬上限（生产 = 30 分钟；sleep 模式的最终兜底）。
	standbyHardCapDefault = 30 * time.Minute
	// standbyPollIntervalDefault 是唤醒检测的轮询间隔（生产 = 2 秒）。
	standbyPollIntervalDefault = 2 * time.Second
	// standbyResumeMargin 是待机**收尾**的预算余量（G-f 豁免的延展量 = 应等时长 + 它）：
	// 唤醒/超时收口后本轮还要继续跑（下一次模型调用），只延展到 end 会在收尾前掐死。
	//
	// 60s 而不是 10s（critic F6）：10s 不够**一次收尾模型轮**（网络往返 + 首 token
	// 就可能吃掉大半）——余量不够 = 待机豁免了预算，却在收尾处被掐（白等）。
	standbyResumeMargin = 60 * time.Second
)

// standbyTime 是 standby 的时间口径（原子量；测试写走 .Store()，读走访问器）。
var standbyTime = struct {
	// minute 是一"分钟"的实际时长（生产 = 1 分钟）。
	minute atomic.Int64
	// hardCap 是一轮待机的硬上限（生产 = 30 分钟；sleep 模式的最终兜底）。
	hardCap atomic.Int64
	// poll 是唤醒检测的轮询间隔（生产 = 2 秒）。
	poll atomic.Int64
}{}

// init 装生产默认值（atomic 的字段不能出现在复合字面量里——只能 Store）。
func init() {
	standbyTime.minute.Store(int64(standbyMinuteDefault))
	standbyTime.hardCap.Store(int64(standbyHardCapDefault))
	standbyTime.poll.Store(int64(standbyPollIntervalDefault))
}

// standbyMinute 是一"分钟"的实际时长（生产 = 1 分钟；≤0 → 默认值）。
func standbyMinute() time.Duration {
	return standbyDurOrDefault(standbyTime.minute.Load(), standbyMinuteDefault)
}

// standbyHardCap 是一轮待机的硬上限（生产 = 30 分钟；≤0 → 默认值）。
func standbyHardCap() time.Duration {
	return standbyDurOrDefault(standbyTime.hardCap.Load(), standbyHardCapDefault)
}

// standbyPollInterval 是唤醒检测的轮询间隔（生产 = 2 秒；≤0 → 默认值，
// 防 time.NewTicker(≤0) panic）。
func standbyPollInterval() time.Duration {
	return standbyDurOrDefault(standbyTime.poll.Load(), standbyPollIntervalDefault)
}

// standbyDurOrDefault 取时长（≤0 → 默认值：时间口径被写成 0 时不许 panic）。
func standbyDurOrDefault(v int64, def time.Duration) time.Duration {
	d := time.Duration(v)
	if d <= 0 {
		return def
	}
	return d
}

const (
	// standbyIntentWindow 是回查 tool.call.intent 的首轮窗口（条）：live 路径下
	// intent 刚落盘（就在流尾），扫尾部就够；扫不到再退回全流扫描。
	standbyIntentWindow = 1024
	// standbyIntentChunk 是全流扫描时的分块大小（条）。
	standbyIntentChunk = 4096
	// standbyPreviewRunes 是唤醒摘要里消息正文的截断长度（字符）。
	standbyPreviewRunes = 200
	// standbyDeliveryBlobReadLimit 是"经 blob 读回交付标记"的读上限（字节）。
	//
	// standby 自己的结果是短文本（几百字节——唤醒摘要最多 200 字预览），1MB 远远够；
	// 设上限是防"读一个巨大的 blob 进内存"（F12：别把整个大对象塞进内存）。
	standbyDeliveryBlobReadLimit = 1 << 20
	// standbyEarlyArrivedMark 是"这条消息在**本次等待开始前**就到了"的标记（critic F1）。
	//
	// 为什么要有：等待前的全量收件箱检查命中时，模型看到的摘要必须**如实**说明
	// "它不是等来的"——否则模型会以为这条消息与自己刚才的等待有关（时序错觉）。
	standbyEarlyArrivedMark = "（早于本次等待已到达）"
)

// standbyDescriptor 是 standby 的跨边界描述（进模型可见集）。
//
// description 讲清三件事：**挂起等什么**（哪类消息）、**等多久**（软/硬上限）、
// **为什么值得等**（多 agent 协作防掉线）——模型据此决定要不要用它，而不是
// 把"等满 timeout_min"当成必然失败的等待。
func standbyDescriptor() contracts.Tool {
	return contracts.Tool{
		Name: ToolStandby,
		Description: strPtrStandby(
			"**待机等待**：挂起本轮，等对端 agent 的消息（或交接）到达再继续——" +
				"多 agent 协作里 A 派完活后用它等 B 的回话，等待期不再轮询模型" +
				"（省 token，也不怕对端掉线）。参数 wait 选等哪类消息" +
				"（message=等 agentlink.broadcast；handoff=等 agentlink.handoff）；" +
				"timeout_min=最多等几分钟（1..30，**只在 on_timeout=return 时适用**）；" +
				"on_timeout=到点还没等到怎么办" +
				"（return=立刻如实收口并回答\"未等到\"，timeout_min 是超时点；" +
				"sleep=等到消息到达或 30 分钟硬上限，**timeout_min 不适用**）。" +
				"消息一到立刻醒，并把发送方与内容摘要回给模型。"),
		Schema:     json.RawMessage(standbyToolSchema),
		SideEffect: contracts.SideEffectNone, // 不改任何外部状态（只等）
		Approvable: false,                    // 等待不需要人审（策略链照走：deny-all 默认要显式放行）
		Idempotent: true,                     // 重复等不产生额外副作用
		Cancelable: true,                     // 可取消：run ctx 取消 / hub 关掉即中止
		Replay:     contracts.ReplayKindSafe, // 重放=重挂（处置细节下一件接）
	}
}

// standbyToolSchema 是 standby 的参数 schema（三参数）。
const standbyToolSchema = `{
  "type": "object",
  "properties": {
    "wait": {
      "type": "string",
      "enum": ["message", "handoff"],
      "description": "等哪一类消息：message=等 agentlink.broadcast（对端广播）；handoff=等 agentlink.handoff（对端交接）。"
    },
    "timeout_min": {
      "type": "integer",
      "minimum": 1,
      "maximum": 30,
      "description": "最多等多少分钟（1..30）。**只在 on_timeout=return 时适用**（那时它是超时点）；on_timeout=sleep 时**不适用**（sleep=等到消息或 30 分钟硬上限）。"
    },
    "on_timeout": {
      "type": "string",
      "enum": ["return", "sleep"],
      "description": "到点还没等到怎么办：return=立刻如实收口（回答\"未等到\"，timeout_min 是超时点）；sleep=等到消息到达或 30 分钟硬上限（**timeout_min 不适用**）。"
    }
  },
  "required": ["wait", "timeout_min", "on_timeout"]
}`

// strPtrStandby 取字符串地址（描述字段用）。
func strPtrStandby(s string) *string { return &s }

// standbyArgs 是 standby 的已校验参数。
type standbyArgs struct {
	// Wait 是要等的消息种类（message / handoff）。
	Wait string
	// TimeoutMin 是**超时点**（分钟）——**只在 return 模式适用**（sleep 模式不看它，
	// 见 standbyOnTimeoutSleep）。
	TimeoutMin int
	// OnTimeout 是到点后的处置（return / sleep）。
	OnTimeout string
}

// standbyEventSource 是 standby 需要的最小事件流读面：core.Store（按 seq 读）
// + Tip（取尾部水位，用来定位刚落盘的 intent）。
//
// 为什么不直接用 core.Store：Tip 只在具体实现上有（core.Store 接口没有），
// 而"从流尾往回找 intent"与"降级游标 = 当前 seq"都需要水位。用窄接口断言，
// 装配残缺时如实失败。
type standbyEventSource interface {
	core.Store
	Tip(ctx context.Context) (int64, error)
}

// standbyDeps 是 standby 执行器需要的宿主能力（装配期注入）。
type standbyDeps struct {
	// store 是事件流读面（回查本次 tool.call.intent + 取水位）。
	store standbyEventSource
	// link 是 agentlink 组件（Inbox：K11 addressedTo 过滤已实现）。
	link *agentlink.Link
	// disc 是发现表（waiting 态的 BeginWait/EndWait）。
	disc *discovery.Table
	// catchUp 把发现表追平（K11 成员关系是**读时现算**的，不追平就漏消息）。
	catchUp func(ctx context.Context) (int, error)
	// hubDone 在 hub 生命周期结束时关闭（关库 → 待机中止，不挂死）。
	hubDone <-chan struct{}
	// blob 是大 payload 外置仓库（storage.Blob；nil = 未启用）：**交付标记被外置**时
	// 经它读回（critic P2：外置丢标记 → 已交付集合退化 → 重复交付）。
	blob storage.Blob
}

// standbyExecutor 执行 standby：挂起本轮，等消息或超时。
type standbyExecutor struct {
	deps standbyDeps
}

// 编译期证明：*standbyExecutor 就是 core.ToolExecutor。
var _ core.ToolExecutor = (*standbyExecutor)(nil)

// newStandbyExecutor 造 standby 执行器（依赖缺失时调用即失败，不静默降级）。
func newStandbyExecutor(deps standbyDeps) *standbyExecutor {
	return &standbyExecutor{deps: deps}
}

// errStandbyHubClosed 是 hub 关库导致待机中止的哨兵错误。
var errStandbyHubClosed = errors.New("standby: hub 已关闭，待机中止")

// standbyIntentRef 是"本次 tool.call.intent"的定位结果。
type standbyIntentRef struct {
	// SessionID 是本次等待归属的 session（envelope.session_id）。
	SessionID contracts.Ulid
	// AgentID 是发起本次调用的 agent（envelope.agent_id）。
	AgentID contracts.Ulid
	// Seq 是 intent 事件的 seq（重放游标起点）。
	Seq int64
}

// standbyOutcome 是一次等待的结果。
type standbyOutcome struct {
	// Msg 是唤醒消息（Woke=true 时有效）。
	Msg agentlink.Message
	// Woke 报告是被消息唤醒的，还是等到超时收口的。
	Woke bool
	// Early 报告这条消息是**等待开始前**就到达的（等待前的全量收件箱检查命中，
	// F1）——正文要注明，别让模型以为是"等了一会儿才到的"。
	Early bool
	// HardCapEnd 报告收口原因是硬上限（return 模式的超时点先到，或 sleep 模式续挂到顶）。
	HardCapEnd bool
}

// Execute 执行一次 standby（X1 的 intent/completed 由 loop 落，本工具只等）。
//
// 成功路径返回 completed（唤醒摘要 / 超时说明）；参数非法或等待被取消返回
// error → X1 落 failed（文案自带原因，模型据此自我修正）。
func (e *standbyExecutor) Execute(ctx context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	// 这次调用的起点（预算豁免的计时基准，见 wait 里的注释）：从**进工具**算起，
	// 覆盖"回查 intent + 全量收件箱检查 + 等待"整段占用。
	callStart := time.Now()
	args, err := parseStandbyArgs(intent.Arguments)
	if err != nil {
		return contracts.ToolResult{}, err
	}
	// 执行器启动时的水位 = 降级游标（"当前 seq"）：拿不到 intent 事件时报出来。
	startSeq, err := e.tip(ctx)
	if err != nil {
		return contracts.ToolResult{}, err
	}
	// 定位本次 intent（拿归属 session + 重放游标）。
	ref, ok, err := e.locateIntent(ctx, intent)
	if err != nil {
		return contracts.ToolResult{}, err
	}
	if !ok {
		// 拿不到 intent → 归属 session 无从确定（waiting 态与 K11 过滤两处都会错）→ 明确失败。
		// 降级游标（当前 seq）如实报出来，便于人判断"错过了哪一段消息"。
		return contracts.ToolResult{}, fmt.Errorf(
			"standby: 事件流里找不到 tool_call_id=%s 的 tool.call.intent——本次等待归属的 session 无从确定"+
				"（不猜 session：waiting 态与 K11 过滤都会落到错的 session 上）；"+
				"降级游标取当前 seq=%d（局限：拿不到 intent 的 seq，就看不见中断期间到达的消息）",
			intent.ToolCallID, startSeq)
	}
	// waiting 态：豁免活跃窗口判定（防协作掉线）；不刷最后事件 ts（本工具不落事件）。
	e.deps.disc.BeginWait(ref.SessionID)
	defer e.deps.disc.EndWait(ref.SessionID)

	out, err := e.wait(ctx, ref, args, callStart)
	if err != nil {
		return contracts.ToolResult{}, err
	}
	if out.Woke {
		text := standbyWakeText(out.Msg)
		if out.Early {
			// F1：这条消息在**本次等待开始前**就到了（如实说明，别让模型误以为
			// 它是"等了一会儿才来的"）。
			text += standbyEarlyArrivedMark
		}
		// 交付标记（critic P1）：记下本次交付的消息 seq → 下一次 standby 的早到检查
		// **排除**它（旧消息不重复交付）。
		return standbyResult(text, out.Msg.Seq), nil
	}
	// 超时收口：没有交付任何消息 → delivered_seq=0（**不进集合**：之后到达的消息
	// 仍能被下一次早到检查捞到）。
	return standbyResult(e.timeoutText(args, out.HardCapEnd), 0), nil
}

// deliveredSeqs 折出本 session 历史 standby **已交付**的消息 seq **集合**
// （critic 终验：集合，不是水位）。
//
// # 为什么是"集合"而不是"水位"（critic P1 的根子）
//
//	水位（"已交付的最大 seq"）是个**代理**：它把"已交付"约成了"seq ≤ 某值"——于是
//	**别的 kind** 的旧消息（handoff）被一次 broadcast 的交付顺带盖掉（再也捞不到），
//	同 kind 的多条未交付消息也被最新那条永久盖住。集合没有这个问题：排除只看
//	"这条消息**具体**交付过没有"。
//
//	- 跨 kind 天然正确：handoff 早到未交付 → 不在集合里 → 等 handoff 时能捞到；
//	- 同 kind 多条未交付 → 最新先交付，旧的**下次有资格**（不再被永久盖住）；
//	- 重复交付被排除（在集合里）。
//
// # 认领键：**CausationID**（critic P2）
//
//	completed 的载荷里**没有工具名**，按 tool_call_id 认领会**误认领**别的工具
//	（provider 复用 tool_call_id 时，同 id 的别的工具调用会被当成 standby 的交付
//	记录）。故认领键用**因果指针**：completed.CausationID == 某条 standby intent 的
//	**事件 id**（core/loop 的 newEvent：completed/failed 指向同一次调用的 intent）。
//
// # 标记的读法（critic P2：blob 外置丢标记）
//
//	① 行内：结果的结构化载荷（Content[0].Data = {"delivered_seq":N}）；
//	② 外置：结果被外置成 blob 时结构化载荷整体在 blob 里（事件里只剩 binary_ref）→
//	   经 blob 读回全文再取。
//
//	读不到（blob 没启用 / 内容不在 / 读失败）→ **按空处理 + warn 一条**：安全方向是
//	"可能重复交付"，绝不是"隐藏已交付"（藏证据比重复交付更坏）。
func (e *standbyExecutor) deliveredSeqs(ctx context.Context, ref standbyIntentRef) (map[int64]bool, error) {
	out := map[int64]bool{}
	if e.deps.store == nil {
		return out, errors.New("standby: 事件流读面缺失——无法查已交付集合")
	}
	// **分块扫**（critic P1）：整场读进内存是**无上界**的（5 万条事件实测 ≈166MB /
	// 245ms 每次 standby 调用）→ 按 standbyIntentChunk 分块扫，**只留** standby
	// intent 的事件 id 集与已交付的 seq 集 → 内存 O(standby 调用数)，不是 O(事件数)。
	//
	// 一遍扫完即可：intent **必然早于**它的 completed（intent 先 durable 落盘、
	// 工具才执行），故扫到 completed 时它的 intent 已经在集里。
	intentIDs := map[contracts.Ulid]bool{}
	from := int64(1)
	for {
		evs, err := e.deps.store.ReadFrom(ctx, &ref.SessionID, from, standbyIntentChunk)
		if err != nil {
			return out, fmt.Errorf("standby: 读本 session 的事件流失败（查已交付集合）: %w", err)
		}
		if len(evs) == 0 {
			break // 流尾
		}
		last := int64(0)
		for i := range evs {
			ev := &evs[i]
			if ev.Seq == nil {
				continue
			}
			last = *ev.Seq
			switch ev.Kind {
			case loop.KindToolCallIntent:
				var p contracts.ToolCallIntentPayload
				if json.Unmarshal(ev.Payload, &p) != nil {
					continue // 坏载荷：跳过（D7 宽容）
				}
				if p.ToolName == ToolStandby {
					intentIDs[ev.ID] = true // completed 的认领键（事件 id）
				}
			case loop.KindToolCallCompleted:
				if ev.CausationID == nil || !intentIDs[*ev.CausationID] {
					continue // 不是 standby 的这次调用（别的工具 / 别的 session）
				}
				var p contracts.ToolCallCompletedPayload
				if json.Unmarshal(ev.Payload, &p) != nil {
					continue // 坏载荷：跳过
				}
				seq, ok := e.deliverySeqOf(ctx, ev, p.Result)
				if !ok || seq <= 0 {
					continue // 读不到标记 / 未交付（delivered_seq=0）→ 不进集合
				}
				out[seq] = true
			}
		}
		if last < from {
			break // 没有前进（事件缺 seq）→ 停（防死循环）
		}
		from = last + 1
	}
	return out, nil
}

// deliverySeqOf 取一条 completed 结果里的交付标记（delivered_seq）。
//
// 读法两条：① 行内（结果的结构化载荷）；② 外置（经 blob 读回全文）。
//
// ok=false = 读不到（blob 没启用 / 读失败 / 不是交付标记）→ 调用方按"没交付"处理
// （安全方向：可能重复交付，但绝不隐藏已交付的证据）。
func (e *standbyExecutor) deliverySeqOf(ctx context.Context, ev *contracts.Event, res contracts.ToolResult) (int64, bool) {
	if seq, ok := deliverySeqInResult(res); ok {
		return seq, true
	}
	if ev.BinaryRef == nil {
		return 0, false // 没有标记（不是交付结果 / 旧形状）
	}
	if e.deps.blob == nil {
		hubLog.Warn("standby: 交付标记被外置但 blob 未启用——按空集合处理（可能重复交付）",
			"session_id", string(ev.SessionID), "event_id", string(ev.ID))
		return 0, false
	}
	rc, err := e.deps.blob.Get(ctx, *ev.BinaryRef)
	if err != nil {
		hubLog.Warn("standby: 读外置的交付标记失败——按空集合处理（可能重复交付）",
			"event_id", string(ev.ID), "blob_id", string(*ev.BinaryRef), "err", err.Error())
		return 0, false
	}
	defer func() { _ = rc.Close() }()
	// 读回上限：standby 自己的结果是短文本（几百字节），1MB 足够；超限/坏内容 → 当读不到。
	raw, err := io.ReadAll(io.LimitReader(rc, standbyDeliveryBlobReadLimit))
	if err != nil {
		hubLog.Warn("standby: 读外置的交付标记失败——按空集合处理（可能重复交付）",
			"event_id", string(ev.ID), "blob_id", string(*ev.BinaryRef), "err", err.Error())
		return 0, false
	}
	var full contracts.ToolResult
	if json.Unmarshal(raw, &full) != nil {
		hubLog.Warn("standby: 外置的交付标记解不开——按空集合处理（可能重复交付）",
			"event_id", string(ev.ID), "blob_id", string(*ev.BinaryRef))
		return 0, false
	}
	return deliverySeqInResult(full)
}

// deliverySeqInResult 从一份工具结果里取交付标记（没有 / 不是交付标记 → ok=false）。
func deliverySeqInResult(res contracts.ToolResult) (int64, bool) {
	for i := range res.Content {
		if len(res.Content[i].Data) == 0 {
			continue
		}
		var d standbyDelivery
		if json.Unmarshal(res.Content[i].Data, &d) != nil {
			continue // 不是交付标记（别的结构化载荷）
		}
		return d.DeliveredSeq, true
	}
	return 0, false
}

// tip 取事件流水位（降级游标用）。
func (e *standbyExecutor) tip(ctx context.Context) (int64, error) {
	if e.deps.store == nil {
		return 0, errors.New("standby: 事件流读面缺失——无法取水位")
	}
	seq, err := e.deps.store.Tip(ctx)
	if err != nil {
		return 0, fmt.Errorf("standby: 读事件流水位失败: %w", err)
	}
	return seq, nil
}

// wait 挂起等待：命中消息 → 返回该消息；到期 → 超时收口；取消 → error。
//
// callStart 是**这次工具调用的起点**（Execute 入口处记的）：预算豁免按它计时，
// 覆盖"回查 intent + 全量收件箱检查 + 等待"整段占用（critic F6：豁免是**暂停**
// 语义——待机占掉多久，预算就往后挪多久）。
func (e *standbyExecutor) wait(ctx context.Context, ref standbyIntentRef, args standbyArgs, callStart time.Time) (standbyOutcome, error) {
	// ① 等待前的收件箱检查（F1）：等待**开始前**就到达的同类消息，在这里收口。
	//
	// 为什么必须先查这一遍：增量游标（本次 intent 的 seq）看不见 intent **之前**
	// 到达的消息（Inbox 的 SinceSeq 严格大于）——典型场景（A 派活 → B 秒回 → A 下一轮
	// 才调 standby）必然假阴性：睡满超时后报"未等到"，而那条消息模型没有第二条路
	// 看见它（agentlink 消息不进 FoldConversation）。
	//
	// **交付集合排除**（critic 终验）：已交付过的消息**逐条**排除（不是"seq ≤ 水位"
	// 那种代理）——跨 kind 的旧消息（handoff）不会被一次 broadcast 的交付顺带盖掉，
	// 同 kind 的多条未交付消息也能逐条补交。
	delivered, err := e.deliveredSeqs(ctx, ref)
	if err != nil {
		return standbyOutcome{}, err
	}
	arrived, arrivedMsg, err := e.checkArrived(ctx, ref, args, delivered)
	if err != nil {
		return standbyOutcome{}, err
	}
	if arrived {
		return standbyOutcome{Msg: arrivedMsg, Woke: true, Early: true}, nil
	}

	start := time.Now()
	softEnd := start.Add(time.Duration(args.TimeoutMin) * standbyMinute())
	hardEnd := start.Add(standbyHardCap())
	// 本次等待的终点：
	//   return 模式 = 超时点（timeout_min）；但硬上限更早到就按硬上限收口。
	//   sleep  模式 = 硬上限（**timeout_min 不适用**：见 standbyOnTimeoutSleep 的口径）。
	end := softEnd
	hardCapEnd := false
	if args.OnTimeout == standbyOnTimeoutSleep {
		// sleep 模式：**显式忽略 timeout_min**（critic F3）——语义是"等到消息或硬
		// 上限（30 分钟）"，timeout_min 只在 return 模式有意义（见
		// standbyOnTimeoutSleep）。参数仍被校验（1..30），只是**不参与**终点计算。
		end = hardEnd
		hardCapEnd = true
	}
	if hardEnd.Before(end) {
		end = hardEnd
		hardCapEnd = true
	}
	// 预算豁免（G-f，critic F6 + critic P2）：待机时长另计——**预借 + 实占结算**。
	//
	//	① 预借（Pause）：开始前把截止从**当前截止**后移 planned + 收尾余量
	//	   —— 保证等待期间预算够用（否则等不到结束就被掐）。
	//	② 结算（Settle）：等待结束后把截止**结算为**「预借前的截止 + 实占时长 +
	//	   收尾余量」——只补实占那截，多借的**退回**。
	//
	// 为什么必须结算（critic P2）：只预借不结算 → 早醒的 standby 把整轮预算撑大
	// （timeout_min=1min、50ms 就醒，却把截止推后了 1min + 余量；sleep 一次推
	// 30min + 余量）。结算后：截止 ≈ 开始截止 + 实占 + 余量。
	//
	// 为什么不是"从现在起再给 d"（旧口径）：那会让预算充足时（截止还很远）待机
	// **照样烧预算**——等 5 分钟但预算还剩 1 小时，截止几乎没动。
	//
	// 取不到软时钟 → 不延展（行为同现在：CLI / ACP 等自己造 ctx 的路径）。
	if c := runClockFrom(ctx); c != nil {
		if d := time.Until(end); d > 0 {
			// 预借：planned + 余量；base = 预借前的截止（结算基准）。
			base := c.Pause(d + standbyResumeMargin)
			// 结算：实占（从**进工具**算起）+ 余量——多借的**退回**（早醒不撑大预算）；
			// 实占超计划的那些也一并结算进去（结算值 = base + 实占 + 余量）；下限保护
			// 保证收尾那一轮模型调用有预算。
			defer func() {
				c.Settle(base, time.Since(callStart), standbyResumeMargin)
			}()
		}
	}
	// 增量轮询的起点 = 本次 intent 的 seq（新消息的 seq 必然大于它：消息落在流尾）。
	// 轮询里同样按**交付集合**排除（与早到检查同款）。
	cursor := ref.Seq
	for {
		if !time.Now().Before(end) {
			return standbyOutcome{HardCapEnd: hardCapEnd}, nil // 到点 → 如实收口
		}
		// 等待 ctx 从执行 ctx 派生（run ctx 取消 → 立刻中止）。
		waitCtx, cancel := context.WithTimeout(ctx, time.Until(end))
		msg, next, found, err := e.pollUntil(waitCtx, ref, args, cursor, delivered)
		// 本段是否已到点（软检查点/硬上限）：**看 ctx 本身**而不是错误文案——
		// 到点那一刻正在跑的读会被打断，驱动报的是"interrupted"一类，
		// 按错误类型判会把"到点"误当成"失败"（如驱动不回 ctx.Err() 时）。
		segExpired := waitCtx.Err() != nil
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return standbyOutcome{}, fmt.Errorf("standby: 等待被取消: %w", ctx.Err())
			}
			if errors.Is(err, errStandbyHubClosed) {
				return standbyOutcome{}, err
			}
			if segExpired {
				continue // 本段到期 → 回圈判到点（如实收口，不算失败）
			}
			return standbyOutcome{}, fmt.Errorf("standby: 等待失败: %w", err)
		}
		if found {
			return standbyOutcome{Msg: msg, Woke: true}, nil
		}
		cursor = next
	}
}

// pollUntil 在一段等待里轮询唤醒条件（命中即返回）。
func (e *standbyExecutor) pollUntil(ctx context.Context, ref standbyIntentRef, args standbyArgs, cursor int64, delivered map[int64]bool) (agentlink.Message, int64, bool, error) {
	ticker := time.NewTicker(standbyPollInterval())
	defer ticker.Stop()
	for {
		// 首次立即查（不等第一个 tick），之后每 standbyPollInterval() 一次。
		found, msg, next, err := e.checkOnce(ctx, ref, args, cursor, delivered)
		if err != nil {
			return agentlink.Message{}, cursor, false, err
		}
		if found {
			return msg, next, true, nil
		}
		cursor = next
		select {
		case <-ctx.Done():
			return agentlink.Message{}, cursor, false, ctx.Err()
		case <-e.deps.hubDone:
			return agentlink.Message{}, cursor, false, errStandbyHubClosed
		case <-ticker.C:
		}
	}
}

// checkOnce 查一次唤醒条件（K11 addressedTo 过滤在 Link.Inbox 里）。
//
// 收件方口径 = **session 级**（不按 agent 收窄）：K11① 的收件方是"对端 session 的
// 主 agent"，若本 session 是以子 agent 身份发起 standby，按 agent 收窄会让 gate
// 消息（投给主 agent 的）永远看不见——挂到超时。session 级口径覆盖全部 agent，
// 且"别的 session 的消息"仍被 K11 挡掉（gate 成员 / subtree 同 session 两条）。
//
// 选取口径 = **取最新**那条（seq 最大、未交付的）——与早到检查**同一口径**
// （critic P2-3：早到取最新、轮询取最旧是口径不一致 → 统一成最新）。
func (e *standbyExecutor) checkOnce(ctx context.Context, ref standbyIntentRef, args standbyArgs, cursor int64, delivered map[int64]bool) (bool, agentlink.Message, int64, error) {
	res, err := e.inbox(ctx, ref, cursor)
	if err != nil {
		return false, agentlink.Message{}, cursor, err
	}
	cands := standbyCandidates(res.Messages, args.Wait, delivered)
	if len(cands) == 0 {
		return false, agentlink.Message{}, res.NextSeq, nil
	}
	// **交付前复查**（并发可见性，critic P2-1；与早到检查同款）。
	fresh, err := e.deliveredSeqs(ctx, ref)
	if err != nil {
		return false, agentlink.Message{}, cursor, err
	}
	for _, m := range cands {
		if fresh[m.Seq] {
			continue // 刚被别的 standby 交付了 → 跳过（取下一条）
		}
		return true, m, res.NextSeq, nil
	}
	return false, agentlink.Message{}, res.NextSeq, nil
}

// checkArrived 查"**已经到达**"的同类消息（等待开始前的收件箱检查，F1 + critic P1）。
//
// 口径：**宽范围**（SinceSeq=0 → 全流）+ **交付集合排除**（seq ∈ delivered 的
// 跳过）→ 过滤条件与增量路径**完全一致**（wait 的 kind 过滤 + K11 addressedTo）→
// 只认"投递给本 session 的同类消息"。
//
// **为什么是集合排除而不是水位**（critic 终验）：水位（"已交付的最大 seq"）是
// 个**不准的代理**——它把"已交付"约成了"seq ≤ 某值"：别的 kind 的旧消息（handoff）
// 会被一次 broadcast 的交付顺带盖掉（seq 更小 → 永远捞不到），同 kind 的多条未交付
// 消息也被最新那条永久盖住。集合排除只问"这条消息**具体**交付过没有"：跨 kind
// 天然正确；同 kind 多条未交付 → 最新先交付、旧的**下次有资格**（逐条补交）。
//
// 取**最新**那条（seq 最大，未交付的）：它最贴近"刚到的消息"；更早的未交付消息
// 下次还有资格（不会被永久盖住）。轮询路径**同款口径**（critic P2-3）。
//
// **交付前复查**（并发可见性，critic P2-1）：delivered 是**等待开始时的快照**——
// 另一个 standby 可能在我们等待期间交付了同一条消息 → 命中后再折一次集合，
// 已交付的不再交付（并发 standby 不重复交付同一条）。
func (e *standbyExecutor) checkArrived(ctx context.Context, ref standbyIntentRef, args standbyArgs, delivered map[int64]bool) (bool, agentlink.Message, error) {
	res, err := e.inbox(ctx, ref, 0) // 宽范围（SinceSeq=0 → 全流）
	if err != nil {
		return false, agentlink.Message{}, err
	}
	cands := standbyCandidates(res.Messages, args.Wait, delivered)
	if len(cands) == 0 {
		return false, agentlink.Message{}, nil
	}
	// **交付前复查**（并发可见性，critic P2-1）：delivered 是**等待开始时的快照**——
	// 另一个 standby 可能在我们等待期间交付了同一条消息 → 交付前再折一次集合，
	// 已交付的不再交付（并发 standby 不重复交付同一条）。
	fresh, err := e.deliveredSeqs(ctx, ref)
	if err != nil {
		return false, agentlink.Message{}, err
	}
	for _, m := range cands {
		if fresh[m.Seq] {
			continue // 刚被别的 standby 交付了 → 跳过（取下一条）
		}
		return true, m, nil
	}
	return false, agentlink.Message{}, nil
}

// standbyCandidates 从收件箱结果里挑候选：kind 匹配 ∧ 不在已交付集合里，
// **按 seq 降序**（最新在前——早到检查与轮询**同一口径**：都交付最新那条）。
func standbyCandidates(msgs []agentlink.Message, wait string, delivered map[int64]bool) []agentlink.Message {
	want := standbyWantKind(wait)
	out := make([]agentlink.Message, 0, len(msgs))
	for _, m := range msgs {
		if m.Kind != want {
			continue // 等 message 不被 handoff 唤醒（反之亦然）
		}
		if delivered[m.Seq] {
			continue // 已交付过 → 不重复交付（集合排除）
		}
		out = append(out, m)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq > out[j].Seq })
	return out
}

// standbyDelivery 是 standby 结果里的**交付标记**（结构化载荷，程序化消费）。
//
// 为什么要有（critic P1）：消息不删 + K11 读时判成员 → 早到检查若每次都从流头扫，
// 历史上**任何**投递给本 session 的同类消息都会命中 → 连调两次 standby 会逐字重复
// 交付同一条旧消息。故 completed 结果里记下"本次交付的消息 seq"，下一次 standby 的
// 早到检查**排除**集合里那些消息（已交付的不再重复交付；没交付过的仍会被捞到）。
//
// 形状放在 Content[0].Data（结构化载荷的既有约定，见 core/loop 的 summaryToResult：
// "Content[0].Data = 结构化 JSON，供程序化消费"）——**模型看不到它**（回填给模型的
// 文本只取 Text 部分，见 core/loop 的 toolResultText）。
//
// 诚实边界：结果被**外置成 blob** 时结构化载荷整体外置（core/loop/blob.go）→ 事件里
// 只剩 binary_ref → 标记要**经 blob 读回**（见 deliverySeqOf；读不到 → 按空处理 +
// warn：可能重复交付一次，但绝不隐藏已交付的证据）。
type standbyDelivery struct {
	// DeliveredSeq 是本次交付消息的 envelope seq（0 = 本次没交付任何消息，
	// 例如超时收口"未等到"——**不进集合**，之后到达的消息仍能被下一次早到检查捞到）。
	//
	// 诚实边界（critic P2-4）："0 不进集合"是**防御性**的——真实消息的 seq 从 1 起，
	// 集合里出现 0 不会排除掉任何消息；可观测的差别只在"超时那次到底写没写标记"
	// （写了 0）。集合口径：**每条已交付消息一条记录**（不是水位）。
	DeliveredSeq int64 `json:"delivered_seq"`
}

// inbox 读一次收件箱（K11 addressedTo 过滤在 Link.Inbox 里）。
//
// cursor = 0 → **全量**扫（等待前的检查用）；cursor > 0 → 增量（严格大于它）。
func (e *standbyExecutor) inbox(ctx context.Context, ref standbyIntentRef, cursor int64) (agentlink.InboxResult, error) {
	if e.deps.link == nil {
		return agentlink.InboxResult{}, errors.New("standby: agentlink 组件缺失——无法查收件箱")
	}
	// K11 的成员关系是**读时现算**的：先把发现表追平，否则刚 join 的 gate 看不见。
	if e.deps.catchUp != nil {
		if _, err := e.deps.catchUp(ctx); err != nil {
			return agentlink.InboxResult{}, fmt.Errorf("standby: 追平发现表失败: %w", err)
		}
	}
	res, err := e.deps.link.Inbox(ctx, agentlink.InboxQuery{
		SessionID: ref.SessionID,
		SinceSeq:  cursor,
	})
	if err != nil {
		return agentlink.InboxResult{}, fmt.Errorf("standby: 读收件箱失败: %w", err)
	}
	return res, nil
}

// standbyWantKind 取"等哪类消息"对应的 agentlink kind。
func standbyWantKind(wait string) string {
	if wait == standbyWaitHandoff {
		return agentlink.KindHandoff
	}
	return agentlink.KindBroadcast
}

// locateIntent 按 tool_call_id 回查本次 tool.call.intent 事件。
//
// 命中即返回归属 session（envelope.session_id）与重放游标（envelope.seq）。
// 找不到 → ok=false（调用方按"归属 session 无从确定"明确失败，不猜 session：
// 挂错 session = waiting 态与 K11 过滤两处都错）。
func (e *standbyExecutor) locateIntent(ctx context.Context, intent contracts.ToolCallIntentPayload) (standbyIntentRef, bool, error) {
	if intent.ToolCallID == "" {
		return standbyIntentRef{}, false, errors.New("standby: 工具调用没有 tool_call_id——无法定位本次 intent（不猜 session）")
	}
	if e.deps.store == nil {
		return standbyIntentRef{}, false, errors.New("standby: 事件流读面缺失——无法定位本次 intent（不猜 session）")
	}
	tip, err := e.deps.store.Tip(ctx)
	if err != nil {
		return standbyIntentRef{}, false, fmt.Errorf("standby: 读事件流水位失败: %w", err)
	}
	// ① 尾部窗口：live 路径下 intent 刚落盘（X1：intent durable 落盘后才执行）。
	lo := tip - standbyIntentWindow + 1
	if lo < 1 {
		lo = 1
	}
	ref, ok, err := e.scanIntentRange(ctx, intent, lo, tip)
	if err != nil {
		return standbyIntentRef{}, false, err
	}
	if ok {
		return ref, true, nil
	}
	// ② 全流扫描：重放路径下 intent 是历史事件（可能很老），尾部窗口扫不到。
	ref, ok, err = e.scanIntentRange(ctx, intent, 1, tip)
	if err != nil {
		return standbyIntentRef{}, false, err
	}
	return ref, ok, nil
}

// scanIntentRange 扫一段 seq 区间，返回"最合适"的 intent 候选。
//
// "最合适"分档（档小者优，同档取 seq 大的那条）：
//
//	档 1：tool_call_id 对上且**参数与工具名都对得上**——同一个调用；
//	档 2：tool_call_id 对上、参数对不上，但那条的 session **正在跑**——
//	      provider 复用 tool_call_id 时，正在跑的那条才是本次调用；
//
//	**没有档 3**（critic F2）：旧实现还有一档"其余候选取 seq 最大的那条"——那是
//	**猜归属**（tool_call_id 对上但参数对不上、那条 session 又不在跑 → 无从判断
//	哪条才是本次调用）。猜错 = waiting 态挂到别人的 session 上、K11 按错的 session
//	过滤 → 两处都错。故这一档**删掉**：对不上就是**查不到**（ok=false → 明确失败，
//	不猜 session——见文件头"归属 session 与重放游标"）。
func (e *standbyExecutor) scanIntentRange(ctx context.Context, intent contracts.ToolCallIntentPayload, lo, hi int64) (standbyIntentRef, bool, error) {
	var (
		best     standbyIntentRef
		found    bool
		bestTier int
	)
	from := lo
	for from <= hi {
		limit := int(hi - from + 1)
		if limit > standbyIntentChunk {
			limit = standbyIntentChunk
		}
		evs, err := e.deps.store.ReadFrom(ctx, nil, from, limit)
		if err != nil {
			return standbyIntentRef{}, false, fmt.Errorf("standby: 读事件流失败: %w", err)
		}
		if len(evs) == 0 {
			break
		}
		last := int64(0)
		for i := range evs {
			ev := &evs[i]
			if ev.Seq == nil {
				continue
			}
			last = *ev.Seq
			if *ev.Seq > hi {
				continue
			}
			if ev.Kind != loop.KindToolCallIntent {
				continue
			}
			var p contracts.ToolCallIntentPayload
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue
			}
			if p.ToolCallID != intent.ToolCallID {
				continue
			}
			cand := standbyIntentRef{SessionID: ev.SessionID, AgentID: ev.AgentID, Seq: *ev.Seq}
			tier := 0
			switch {
			case p.ToolName == intent.ToolName && standbyArgsEqual(p.Arguments, intent.Arguments):
				tier = 1
			case e.sessionRunning(cand.SessionID):
				tier = 2
			default:
				// 对不上（既不是同一个调用，那条 session 也不在跑）→ **不猜**：
				// 这一条不当候选（critic F2：旧实现的"档 3"是猜归属）。
				continue
			}
			if !found || tier < bestTier || (tier == bestTier && cand.Seq > best.Seq) {
				best, found, bestTier = cand, true, tier
			}
		}
		if last < from {
			break
		}
		from = last + 1
	}
	return best, found, nil
}

// sessionRunning 报告该 session 本进程是否正在跑一轮（发现表的 running 计数）。
//
// 用途只是**消歧**：provider 复用 tool_call_id 时，多条 intent 可能同 id，
// 正在跑的那条才是本次调用（live 路径的唯一真身）。
func (e *standbyExecutor) sessionRunning(sessionID contracts.Ulid) bool {
	if e.deps.disc == nil {
		return false
	}
	row, ok := e.deps.disc.Session(sessionID)
	return ok && row.Running
}

// standbyArgsEqual 比两次调用的参数是否同一个（按压平后的 JSON 比）。
//
// 事件流里存的参数是 provider 原文（可能带空白），本次调用拿到的是同一份原文；
// 压平后比，避免空白差异造成"不是同一个调用"的误判。
func standbyArgsEqual(a, b json.RawMessage) bool {
	ca, errA := standbyCompactJSON(a)
	cb, errB := standbyCompactJSON(b)
	if errA != nil || errB != nil {
		return bytes.Equal(bytes.TrimSpace(a), bytes.TrimSpace(b))
	}
	return bytes.Equal(ca, cb)
}

// standbyCompactJSON 压平 JSON（去掉无意义空白）。
func standbyCompactJSON(raw json.RawMessage) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// standbyWakeText 拼唤醒摘要：发送方 + 内容截断预览。
func standbyWakeText(m agentlink.Message) string {
	return fmt.Sprintf("收到 %s（发送方 session=%s agent=%s）：%s",
		m.Kind, m.FromSessionID, m.FromAgentID, standbyPreview(m.Text))
}

// standbyPreview 截断消息正文（按字符，不切坏 UTF-8）。
func standbyPreview(text string) string {
	r := []rune(text)
	if len(r) <= standbyPreviewRunes {
		return text
	}
	return string(r[:standbyPreviewRunes]) + "…（已截断）"
}

// timeoutText 拼超时收口的文案（区分"return 超时点收口"与"硬上限收口"）。
//
// sleep 模式的硬上限收口要**说清 timeout_min 不适用**（critic F3）：sleep = 等到
// 消息或硬上限，timeout_min 在这条路径上**不参与**（见 standbyOnTimeoutSleep）。
func (e *standbyExecutor) timeoutText(args standbyArgs, hardCapEnd bool) string {
	if hardCapEnd {
		if args.OnTimeout == standbyOnTimeoutSleep {
			return fmt.Sprintf("未等到（硬上限 %s；sleep 模式不适用 timeout_min）", standbyDurText(standbyHardCap()))
		}
		return fmt.Sprintf("未等到（硬上限 %s）", standbyDurText(standbyHardCap()))
	}
	return fmt.Sprintf("未等到（已等 %d 分钟）", args.TimeoutMin)
}

// standbyDurText 把时长说成人话（分钟优先；测试调小后不至于说"0 分钟"）。
func standbyDurText(d time.Duration) string {
	switch {
	case d >= time.Minute && d%time.Minute == 0:
		return fmt.Sprintf("%d 分钟", int(d/time.Minute))
	case d >= time.Second && d%time.Second == 0:
		return fmt.Sprintf("%d 秒", int(d/time.Second))
	default:
		return fmt.Sprintf("%d 毫秒", int(d/time.Millisecond))
	}
}

// standbyResult 造一条文本结果（给模型看的 completed 内容）+ **交付标记**
// （deliveredSeq = 本次交付消息的 envelope seq；0 = 没交付，例如超时收口）。
//
// 标记放在 Content[0].Data（结构化载荷的既有约定，见 core/loop 的 summaryToResult）——
// **模型看不到它**（回填给模型的文本只取 Text 部分）；下一次 standby 的早到检查靠它
// 算"已交付集合"（critic P1：旧消息不重复交付；集合，不是水位）。
func standbyResult(text string, deliveredSeq int64) contracts.ToolResult {
	data, err := json.Marshal(standbyDelivery{DeliveredSeq: deliveredSeq})
	if err != nil {
		// 标记序列化失败（不可达：结构体是定长的）→ 不带标记（退化为"没交付"，
		// 最坏是多交付一次；不许因此让本次等待失败）。
		data = nil
	}
	return contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &text, Data: data}}}
}

// parseStandbyArgs 解析并校验参数（非法 → error，X1 落 failed，文案点名问题）。
func parseStandbyArgs(raw json.RawMessage) (standbyArgs, error) {
	var fields map[string]json.RawMessage
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &fields); err != nil {
			return standbyArgs{}, fmt.Errorf("standby: 参数不是合法 JSON 对象: %w", err)
		}
	}
	out := standbyArgs{}
	// wait：只认 message / handoff；"event" 是**已知但本批未实现**（G-j 第二批）。
	rawWait, ok := fields["wait"]
	if !ok {
		return standbyArgs{}, errors.New("standby: 缺少 wait 参数（\"message\"=等 agentlink.broadcast，\"handoff\"=等 agentlink.handoff）")
	}
	var wait string
	if err := json.Unmarshal(rawWait, &wait); err != nil {
		return standbyArgs{}, fmt.Errorf("standby: wait 参数必须是字符串: %w", err)
	}
	switch wait {
	case standbyWaitMessage, standbyWaitHandoff:
		out.Wait = wait
	case standbyWaitEvent:
		return standbyArgs{}, errors.New(
			"standby: wait=\"event\" 本批未实现（等任意事件流条件是 G-j 第二批的活）——" +
				"本批只支持 wait=\"message\"（等 agentlink.broadcast）与 wait=\"handoff\"（等 agentlink.handoff）")
	default:
		return standbyArgs{}, fmt.Errorf(
			"standby: wait 只认 \"message\" 或 \"handoff\"（收到 %q）", wait)
	}
	// timeout_min：1..30 的整数。
	rawTimeout, ok := fields["timeout_min"]
	if !ok {
		return standbyArgs{}, errors.New("standby: 缺少 timeout_min 参数（1..30 的整数）")
	}
	var timeoutMin int
	if err := json.Unmarshal(rawTimeout, &timeoutMin); err != nil {
		return standbyArgs{}, fmt.Errorf("standby: timeout_min 参数必须是整数（1..30）: %w", err)
	}
	if timeoutMin < 1 || timeoutMin > 30 {
		return standbyArgs{}, fmt.Errorf("standby: timeout_min 必须在 1..30 之间（收到 %d）", timeoutMin)
	}
	out.TimeoutMin = timeoutMin
	// on_timeout：return / sleep。
	rawOnTimeout, ok := fields["on_timeout"]
	if !ok {
		return standbyArgs{}, errors.New("standby: 缺少 on_timeout 参数（\"return\"=到点收口，\"sleep\"=续挂到硬上限）")
	}
	var onTimeout string
	if err := json.Unmarshal(rawOnTimeout, &onTimeout); err != nil {
		return standbyArgs{}, fmt.Errorf("standby: on_timeout 参数必须是字符串: %w", err)
	}
	switch onTimeout {
	case standbyOnTimeoutReturn, standbyOnTimeoutSleep:
		out.OnTimeout = onTimeout
	default:
		return standbyArgs{}, fmt.Errorf(
			"standby: on_timeout 只认 \"return\" 或 \"sleep\"（收到 %q）", onTimeout)
	}
	return out, nil
}
