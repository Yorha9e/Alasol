package compaction

import (
	"context"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// Assembler 是上下文装配策略的注入点（13 §1 预备单位；J8 context_strategy：append /
// stage_scoped / sliding_window / tiered，用户可再写自己的）。
//
// loop.Config.NewContextStrategy = nil → loop 走内建 append 路径（v1 等价，酸测试）；
// 挂上（经工厂每次 Run 造一个 fresh Assembler）→ 单 agent 长程工作流获得压缩/留存/回捞。
// **换/摘策略 core 零改动**：loop 只认这个接口，不认识 tiered 的具体类型。
//
// 一个 agent 内不混用多套、运行期不切换（13 §1）；装配（Assembler）各挂各的实例。
type Assembler interface {
	// Assemble 把 append-only 的对话历史变换为本轮模型可见的上下文。
	//
	// history 是到本轮为止的完整对话（user.prompt / model.turn / tool 结果，均已在事件流
	// 落库）；turn 是本轮序号。实现可在此压缩旧阶段、按留存策略降级、注入 ledger/常识区，
	// 并就地发 context.compacted 事件（best-effort）。
	//
	// 返回的 Messages 是模型将看到的全部对话（不含 system——system 由 loop 单独放
	// req.System，是稳定前缀永不动，J20）。Parts 是 context.assembled 快照里历史区的
	// 完整分解（ledger / rules / stage:N / 常识区 / history）；**nil/空 = 快照退回 loop
	// 的自动 history 计算**（v1 口径），非空 = 快照直接用这套 parts（不再自动算 history）。
	Assemble(ctx context.Context, history []core.Message, turn int) (Assembled, error)
}

// AssemblerFactory 每次 Run 造一个 fresh Assembler。
//
// 为什么是工厂而不是直接注入一个 Assembler：tiered 策略**有状态**（跨轮跟踪阶段/留存档位），
// 而 loop 承诺"同一个 Loop 可被多个 goroutine 并发 Run（各自独立历史）"——若共用一个有状态
// Assembler，并发 Run 会互相踩乱阶段跟踪。工厂让**每次 Run 各持一个 Assembler**（放进 Run 的
// 局部 runState），既保住 loop 的并发不变式，又让策略跨轮积累状态。宿主每次调工厂应造一个
// 绑定到本次 Run 依赖（store/provider/bus）的新实例。
type AssemblerFactory func() Assembler

// Assembled 是一次装配的产出。
type Assembled struct {
	// Messages 模型可见的对话（压缩/注入后的结果）。
	Messages []core.Message
	// Parts context.assembled 快照的历史区分解（见 Assembler.Assemble）。nil = 退回自动 history。
	Parts []contracts.ContextPart
}

// ---------------------------------------------------------------------------
// engine 依赖的窄接口（core/compaction 只认这些，不 import runtime/host；测试用 fake）
// ---------------------------------------------------------------------------

// SeqSource 提供当前 session 的最大 seq（engine 在每个阶段边界记录 seq 区间用，
// 给 recall 的机器指针）。loop 用 store  tip 实现；测试用 fake。
type SeqSource interface {
	Tip(ctx context.Context) (int64, error)
}

// TextSource 按 seq 区间读对话原文（recall 取回全文；也是级联 C 案里"被压区间全文"的
// 兜底来源——正常路径 engine 用内存里的 history，ReadFrom 主要用于 recall 与重启后回捞）。
type TextSource interface {
	Text(ctx context.Context, fromSeq, toSeq int64) (string, error)
}

// LedgerSource 提供当前 goal ledger 全量快照（级联 C 案：压缩输入 = ledger + 被压区间全文；
// 同时是上下文里 ledger 稳定半区的数据源，J19/X6）。loop 用 fold ledger.updated / 索引器投影实现。
type LedgerSource interface {
	Ledger(ctx context.Context) (contracts.LedgerUpdatedPayload, error)
}

// SummaryResult 是一次压缩调用的产出（摘要正文 + 模型名 + token 计量）。
type SummaryResult struct {
	// Text 摘要正文。
	Text string
	// Model 压缩模型名（成本统计按模型分组用；可空）。
	Model string
	// Usage token 计量（provider 不上报时为 nil）。
	Usage *contracts.ModelTurnUsage
}

// Summarizer 是压缩模型（13 §5/§9：默认主模型、可配；测试用脚本 provider）。
//
// instruction 是压缩指令（含 L1/L2 目标 token 预算）；text 是**被压区间全文 + ledger**
// （级联 C 案，J17/J18/J19：永不读别的阶段摘要——engine 只把当前被压阶段的原文章节进来）。
//
// 返回的 usage 是这次压缩调用的 token 计量（provider 不上报时为 nil）：压缩也是
// 一次模型调用，**成本要进成本统计**（F16/J21）——它随 context.compacted 事件留痕。
type Summarizer interface {
	Summarize(ctx context.Context, instruction, text string) (SummaryResult, error)
}

// Emitter 发一条 context.compacted durable 事件。compaction 不 import core/bus，只认这个
// 窄接口（loop 用 bus 实现）。**best-effort**：发射失败不中止装配——context.compacted 是
// 观测性事实（同 context.assembled：落不下只在事件流里缺一笔，不阻断 agent）。
type Emitter interface {
	EmitCompacted(ctx context.Context, p contracts.ContextCompactedPayload) error
}

// ---------------------------------------------------------------------------
// append 策略（J8 默认）的显式形态
// ---------------------------------------------------------------------------

// AppendAssembler 是 append 策略的显式形态：原样返回历史、不压缩、不注入。
//
// 它主要供"显式选 append"与测试用；loop 里 ContextStrategy=nil 时走**等价的内建 append
// 路径**（根本不调 Assembler，零接口调用开销、零 parts）——那条内建路径才是酸测试
// "不挂时装配输出与 v1 等价"的正主。二者对装配结果等价：Messages=history、Parts=nil
// （→ 快照退回自动 history，与 v1 一致）。
type AppendAssembler struct{}

// Assemble 原样返回历史（append：不压缩）。
func (AppendAssembler) Assemble(_ context.Context, history []core.Message, _ int) (Assembled, error) {
	return Assembled{Messages: history}, nil
}

// 编译期证明：AppendAssembler 就是 Assembler。
var _ Assembler = AppendAssembler{}

// ---------------------------------------------------------------------------
// 策略自带框架工具（per-loop 注入，不经共享 registry）
// ---------------------------------------------------------------------------

// FrameworkTool 是策略注入 loop 的框架内部工具（与多 agent 的 spawn/complete 同款：
// 不经注册表、由 loop 注入可见集并路由执行，仍走 X1 三段写路径）。
//
// 为什么框架工具而非注册表工具：recall/stage 需要**绑定到具体策略实例**（per-session 的
// engine），而 host 的 registry 是跨 session 共享的——把 per-session 执行器塞进共享 registry
// 会重名冲突。框架工具由 loop 随策略一起持有，天然 per-loop（= per-agent），无冲突。
type FrameworkTool struct {
	Desc contracts.Tool
	Exec core.ToolExecutor
}

// ToolProvider 是 Assembler 的可选扩展：策略若实现它，loop 把其框架工具注入模型可见集
// 并路由执行。不实现（如 AppendAssembler）= 不注入额外工具。
type ToolProvider interface {
	Tools() []FrameworkTool
}

// BaseOverheadSetter 是 Assembler 的可选扩展：loop 把本轮的**固定开销**
// （system 提示 + 模型可见工具 schema 的 token 估算）告诉策略。
//
// 策略据此守总量护栏（13 §8）：**base + tools + 历史区 + 当前阶段 ≤ W**。
// 没有它，策略只能看见自己装配的那部分（历史区 + 注入区），看不到"前缀 + 工具"
// 也占窗口——两个预算各自放行、相加超窗（F2）。
type BaseOverheadSetter interface {
	SetBaseTokens(tokens int64)
}

// ToolVisibilitySetter 是 Assembler 的可选扩展：loop 把本轮**模型可见**的框架工具名
// 告诉策略。
//
// 策略据此决定是否在摘要条目里附 recall 使用说明（F27）：recall 被策略 deny 时，
// 提示一个调不到的工具就是误导。
type ToolVisibilitySetter interface {
	SetVisibleToolNames(names []string)
}

// ParamsSetter 是 Assembler 的可选扩展：loop 把**本轮生效**的可配参数（窗口 W /
// L1% / L2% / 注入区预算）告诉策略（G-h 的 per-Run 生效口）。
//
// 为什么是可选扩展、而不是改 AssemblerFactory 的签名：工厂签名是既有契约（core 只认
// 接口、换/摘策略零改动），而**参数是 per-Run 的**——session 级的上下文覆盖在 Run
// 开始冻结之后才拿得到（工厂闭包构造时那套参数只是 hub 默认）。故：工厂照旧造引擎，
// loop 在**冻结快照之后**用本接口把生效参数灌进去。
//
// 实现方**必须 fail-close**：参数不合法（如 W < 256k 硬约束、注入区把窗口吃光）时
// 返回 error，**不许静默降级**成默认值（那等于"用户配的旋钮悄悄没生效"）。
type ParamsSetter interface {
	SetParams(p Params) error
}

// InjectionTruncationObserver 是 Assembler 的可选扩展：**回读**注入区（规则 / 常识区）
// 是否被预算截断。
//
// 为什么需要它（G-h 的 critic P2）：注入区文本在**设置时**按当时的预算截断并缓存——参数
// 被换掉（per-Run 生效口 SetParams）后会**重新截断**，截断与否变了却**没人回读** →
// 退化静默（悄悄丢了上下文，用户看不见）。loop 在灌参后回读这个观测面，把"截断了"报给
// 宿主留痕（Warn，与宿主既有的注入截断告警同款口径）。
type InjectionTruncationObserver interface {
	InjectionTruncated() (rules, commonSense bool)
}

// ParamsGetter 是 Assembler 的可选扩展：读出当前的可配参数。
//
// loop 合并"基值 + 本轮覆盖"时用它取**基值**（引擎构造时那套参数 = 宿主默认）；
// 不实现（如 AppendAssembler）= 没有基值，调用方按 compaction 的缺省值处理。
type ParamsGetter interface {
	Params() Params
}

// 编译期证明：*Engine 提供框架工具（recall / stage）。
var _ ToolProvider = (*Engine)(nil)

// 编译期证明：*Engine 支持 per-Run 参数注入（G-h）、参数读出与注入截断观测。
var (
	_ ParamsSetter                = (*Engine)(nil)
	_ ParamsGetter                = (*Engine)(nil)
	_ InjectionTruncationObserver = (*Engine)(nil)
)
