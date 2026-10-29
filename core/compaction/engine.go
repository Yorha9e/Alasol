package compaction

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// segment 是一个阶段：对话历史里一段连续的消息区间 + 它的留存档位与摘要。
//
// 消息区间用 history 的**下标** [msgFrom, msgTo) 表示；seq 区间 [seqFrom, seqTo] 是给
// 机器的指针（recall 按它读持久化原文）。当前（开放）阶段的 msgTo 每轮随 history 增长；
// 已完成阶段的 msgTo 在关闭/切分时冻结。
type segment struct {
	number           int                       // 阶段号（1-based；**本 Run 内**有效，跨 Run 用带 seq 锚的指针）
	tier             contracts.ContextTier     // 当前留存档位（full/l1/l2）
	msgFrom          int                       // history 下标下界（含）
	msgTo            int                       // history 下标上界（不含）
	seqFrom          int64                     // 事件 seq 下界（含；给机器）
	seqTo            int64                     // 事件 seq 上界（含；给机器）
	summary          string                    // 当前档位的摘要（full 或未算 = 空）
	summarized       bool                      // 摘要是否已物化（空摘要也是"算过了"）
	origTokens       int64                     // 该段原文的 token 数（压缩前；事件里的 tokens_before）
	model            string                    // 产生该摘要的压缩模型名（成本按模型分组，F16）
	usage            *contracts.ModelTurnUsage // 摘要那次压缩调用的 usage（成本留痕，F16）
	fallback         bool                      // 摘要走了退化截断（压缩模型失败/空输出，F19）
	hardClosed       bool                      // 该阶段在**验证通过点**（硬边界）收尾（F6）
	splitMade        bool                      // 该阶段由 split 兜底产生（档位变化由 split 事件覆盖，F11）
	budgetDowngraded bool                      // 本次降级由总量护栏触发（预算兜底口径，F28）
}

// splitHardFactor 是兜底②"切当前阶段"的硬上限倍数：当前阶段 > splitHardFactor × StageBudget
// （= 2×W/3）时劈成两半。StageBudget（=W/3）本身是预防告警③的软阈值；二者分开，②比③更激进。
const splitHardFactor = 2

// 引擎内部的迭代上限（防呆）：预算护栏的降级轮数、切分轮数。正常路径远小于这些上限；
// 触顶说明输入异常（超大字节段），此时停止继续动作而不是空转。
const (
	maxSettleRounds = 64
	maxSplitRounds  = 64
)

// 事件 reason 取值（审计口径；与契约 schema 的描述同源，F28）。
//
//	reason_validation_pass   —— 该阶段在**验证通过点**收尾（硬边界）
//	reason_budget_fallback   —— 本次降级由总量护栏触发（超 W）
//	reason_fallback_truncate —— 压缩模型失败/空输出 → 退化为确定性截断
//	reason_orchestrator_oversize —— 兜底②切当前阶段（上游阶段切大了）
//	reason_stage_over_w_div_3    —— 预防③硬预算告警
//	reason_injection_oversize    —— 注入区（规则/账本/常识区/base）不可压缩且独大 → 护栏降不动
//	reason_total_oversize        —— 注入区 + 历史区(L2) + 当前阶段合计仍超 W（护栏已尽力）
const (
	reasonValidationPass       = "validation_pass"
	reasonBudgetFallback       = "budget_fallback"
	reasonFallbackTruncate     = "fallback_truncate"
	reasonOrchestratorOversize = "orchestrator_oversize"
	reasonStageOverWThird      = "stage_over_w_div_3"
	reasonInjectionOversize    = "injection_oversize"
	reasonTotalOversize        = "total_oversize"
)

// KindContextCompacted 是"一次压缩动作"的 durable 事件 kind（契约 x-kind-payload 登记；
// 载荷 = contracts.ContextCompactedPayload）。宿主的 Emitter 用它组 envelope。core 不自己落
// 这个事件——策略经注入的 Emitter 发（best-effort，观测性事实）。
const KindContextCompacted = "context.compacted"

// Engine 是 tiered 上下文策略的状态机（13 §5 两级压缩 + recall；§4 留存降级；
// §3 阶段边界=验证通过点；§7 兜底三级；§8 预算数学；J17/J18/J19 级联 C 案）。
//
// 它实现 Assembler，经 loop.Config.ContextStrategy 注入。**一个 agent 一个实例**，
// 运行期不切换（13 §1）。所有依赖都是窄接口（SeqSource/TextSource/LedgerSource/
// Summarizer/Emitter），故可在没有 runtime 的情况下完整单测。
//
// 并发：asmMu 串行化**整次装配**（摘要计算要出锁跑模型，故用独立锁保整次操作的原子性）；
// mu 只护状态字段的短临界区。Resolve 走只读路径。
type Engine struct {
	params    Params
	seq       SeqSource
	text      TextSource
	ledger    LedgerSource
	summarize Summarizer
	emit      Emitter

	// commonSense / rules 是宿主注入的稳定内容（常识区 / 项目规则；可空）。
	// 常识区更新与 ledger 更新同拍（13 §6/X6）——由宿主在阶段完成时刷新。
	//
	// 两者都**在设置时**按布局预算截断并缓存（SetCommonSense/SetRules）；截断事实由
	// 下面的两个标记记录（InjectionTruncated 暴露给宿主留痕——退化必须可见）。
	commonSense string
	rules       string
	// commonSenseTruncated / rulesTruncated：注入文本被截断过（未截断 = false）。
	commonSenseTruncated bool
	rulesTruncated       bool

	// baseTokens 是本轮的**固定开销**（system 提示 + 模型可见工具 schema 的 token 估算），
	// 由 loop 经 SetBaseTokens 告知：总量护栏要算它，否则"base+tools+history+current ≤ W"
	// 守不住（F2）。0 = 未知（护栏退化为只看自己装配的部分）。
	baseTokens int64
	// recallVisible 决定摘要条目是否附 recall 使用说明（F27：recall 被策略 deny 时，
	// 提示一个调不到的工具就是误导）。默认 true（loop 未告知时按"recall 可用"处理）。
	recallVisible bool

	asmMu sync.Mutex // 串行化整次 Assemble

	mu              sync.Mutex
	segments        []*segment      // 有序；最后一个 = 当前（开放）阶段
	pendingBoundary bool            // stage 工具置位（软边界）；下次 Assemble 消费
	pendingHard     bool            // MarkValidationPassed 置位（硬边界 = 验证通过点）
	lastTip         int64           // 上次见到的 seq tip
	warned          map[string]bool // budget_warning 去重（阶段+档位，F15）
	pendingSplits   []splitRecord   // 待发的 split 事件（摘要算完后才知道 tokens_after）

	// validation 是"验证通过"信号源（v2-S1 硬边界接线的生产驱动，J11；nil = 不接线）。
	// validationInit / lastValidationSeq 是它的游标：首个 Assemble 基线到当前 tip（只看
	// 本次 Run 开始后的新信号，不反应陈旧事件），之后只扫 (lastValidationSeq, tip]。
	// validationCursor 是**会话级**游标载体（v3-S3 修）：引擎是 per-Run 的，若游标随引擎
	// 消失，落在两次 Run 之间的验证信号永远看不见（详见 ValidationCursor 的说明）。
	validation        ValidationSignalSource
	validationInit    bool
	lastValidationSeq int64
	validationCursor  *ValidationCursor
}

// splitRecord 是一次 split 兜底的记录（事件在摘要物化后才发，tokens_after 按切后实际算，F26）。
type splitRecord struct {
	stage        int
	seqFrom      int64
	seqTo        int64
	tokensBefore int64
}

// NewEngine 构造 tiered 策略状态机。
//
// params.Window 须 ≥ MinWindow（13 §8 硬约束）→ 否则返回 error（拒绝挂载并说明，
// 绝不静默降级）。summarize / emit 可 nil（nil = 不摘要 / 不发射事件，退化但仍能装配
// verbatim；供最小装配与测试）。seq / text / ledger 可 nil（nil = 无 seq 指针 / 无回捞兜底 /
// 无 ledger 注入与级联账本）。
func NewEngine(params Params, seq SeqSource, text TextSource, ledger LedgerSource, summarize Summarizer, emit Emitter) (*Engine, error) {
	params = params.normalized()
	if err := ValidateWindow(params.Window); err != nil {
		return nil, err
	}
	// 布局自洽：历史区预算不得为负（注入区把窗口吃光 → 拒绝挂载，别拿负预算硬跑）。
	if err := ValidateLayout(params); err != nil {
		return nil, err
	}
	return &Engine{
		params:        params,
		seq:           seq,
		text:          text,
		ledger:        ledger,
		summarize:     summarize,
		emit:          emit,
		recallVisible: true,
		warned:        map[string]bool{},
	}, nil
}

// SetCommonSense 设置常识区文本（宿主在阶段完成时刷新，与 ledger 同拍，13 §6/X6）。空 = 不注入。
//
// 注入区文本**在设置时**按布局预算截断一次并缓存（CommonSenseBudget，0 = 布局默认 30k）：
// 常识区不可压缩，没上限就能顶穿窗口；而每轮重算截断是纯浪费（O(n log n) × 每轮数次）。
// 截断事实由 InjectionTruncated() 暴露（宿主据此留痕）。
func (e *Engine) SetCommonSense(text string) {
	e.mu.Lock()
	e.commonSense = truncate(text, e.params.CommonSenseBudget)
	e.commonSenseTruncated = e.commonSense != text
	e.mu.Unlock()
}

// Params 返回引擎当前的可配参数（只读；供宿主/测试核对配置是否真的接上了）。
func (e *Engine) Params() Params {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.params
}

// SetRules 设置项目规则文本（稳定前缀的一部分，Q2/Q3）。空 = 不注入。
//
// 注入区文本**在设置时**按布局预算截断一次并缓存（RulesBudget，0 = 布局默认 15k）：
// 规则不可压缩，没上限就能顶穿窗口；而每轮重算截断是纯浪费（O(n log n) × 每轮数次）。
// 截断事实由 InjectionTruncated() 暴露（宿主据此留痕——退化必须可见）。
func (e *Engine) SetRules(text string) {
	e.mu.Lock()
	e.rules = truncate(text, e.params.RulesBudget)
	e.rulesTruncated = e.rules != text
	e.mu.Unlock()
}

// InjectionTruncated 报告注入区文本是否被截断（退化留痕的观测面）。
//
// 返回 (规则被截断, 常识区被截断)。宿主在注入后据此告警（截断 = 用户配的规则/常识区
// 超了布局预算，**静默砍掉是不行的**——那等于悄悄丢上下文）。
func (e *Engine) InjectionTruncated() (rules, commonSense bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rulesTruncated, e.commonSenseTruncated
}

// SetBaseTokens 告知本轮固定开销（system + 工具 schema 的 token 估算）。总量护栏用（F2）。
func (e *Engine) SetBaseTokens(tokens int64) {
	if tokens < 0 {
		tokens = 0
	}
	e.mu.Lock()
	e.baseTokens = tokens
	e.mu.Unlock()
}

// SetVisibleToolNames 告知本轮模型可见的框架工具名——决定摘要条目是否附 recall 说明（F27）。
func (e *Engine) SetVisibleToolNames(names []string) {
	visible := false
	for _, n := range names {
		if n == ToolRecall {
			visible = true
		}
	}
	e.mu.Lock()
	e.recallVisible = visible
	e.mu.Unlock()
}

// MarkBoundary 标记一个阶段边界（stage 工具调用它；软边界 = agent 显式 tier 标记，J11）。
// 下次 Assemble 时消费：关闭当前阶段、开新阶段。
func (e *Engine) MarkBoundary() {
	e.mu.Lock()
	e.pendingBoundary = true
	e.mu.Unlock()
}

// MarkValidationPassed 打一个**硬边界**标记（验证通过点 = 阶段结束，13 §3）。
//
// 与 MarkBoundary（软边界 = agent 自己说"这里是一个关节"）的区别：硬边界是**框架侧
// 确认的验证通过**（critic clean / gate receipt 通过），阶段因此收尾。
//
// **接线状态（诚实口径）**：API 已落，但**生产路径暂无调用方**——① 仓里没有"验证通过"的
// 生产信号源（gate.checked 尚无生产发射方：core/gate.CheckedEvent 没有调用方）；
// ② 引擎是 per-Run 的（工厂每次 Run 新造），宿主拿不到实例。故事件驱动接线
// **留 v2-S3 门禁增强**：届时由装配层显式调用本方法。doc 不宣称不存在的自动接线。
func (e *Engine) MarkValidationPassed() {
	e.mu.Lock()
	e.pendingHard = true
	e.mu.Unlock()
}

// SetValidationSignal 接上"验证通过"信号源（v2-S1 硬边界接线的生产驱动，J11）。
//
// 接上后，引擎每次 Assemble 自己查"自上一边界以来有没有验证通过事件"（如 gate.checked
// passed=true），有就打硬边界（等价于 MarkValidationPassed）——宿主/loop 因此不必穿透到
// per-Run 引擎实例。nil = 不接线（默认，零行为变化：硬边界只由显式 MarkValidationPassed
// 或 stage 工具触发）。**首个 Assemble 基线到当前 tip**：只看本次 Run 开始后的新信号，
// 不反应陈旧的历史验证事件。
func (e *Engine) SetValidationSignal(src ValidationSignalSource) {
	e.mu.Lock()
	e.validation = src
	e.validationInit = false
	e.lastValidationSeq = 0
	e.mu.Unlock()
}

// SetValidationCursor 接上**会话级**验证游标（v3-S3 修：跨 Run 不丢信号）。
//
// 引擎是 per-Run 的（工厂每次 Run 造一个 fresh 引擎）——若游标只活在引擎里，下一个 Run
// 会重新取基线（"只看本次 Run 开始后的新信号"），**落在两次 Run 之间**的验证信号
// （如 Run1 结束与 Run2 开始之间到达的 workflow.milestone passed=true）**永远不可见**。
//
// 宿主为每个 session 持一个 ValidationCursor（会话级持久），装配引擎时传进来 → 引擎从
// 该游标继续扫，并把推进后的位置写回（下一个 Run 接着扫）。
//
// 口径：
//   - 游标**已定位过**（Initialized）→ 引擎从该位置继续（不再重新取基线）；
//   - 游标**未定位过**（新建的）→ 引擎保持现行口径：首次 Assemble 取基线（不反应陈旧
//     的历史验证事件），并把基线写回游标（下一个 Run 从那里继续）。
//
// nil = 不接（行为与不接时逐字节等价：游标只在引擎内部）。
func (e *Engine) SetValidationCursor(c *ValidationCursor) {
	e.mu.Lock()
	e.validationCursor = c
	if c != nil && c.Initialized() {
		// 会话级游标已有位置 → 直接从那里继续（不重新取基线）。
		e.lastValidationSeq = c.Seq()
		e.validationInit = true
	}
	e.mu.Unlock()
}

// consumeValidationPassed 查"验证通过"信号，返回是否该打硬边界（IO 出状态锁，J21 同款纪律）。
//
// 首个调用只做**基线**（记当前 tip，不反应历史）；之后扫 (lastValidationSeq, tip]，找到
// 验证通过事件 → 返回 true（调用方置 pendingHard）。查询出错 → 不置（fail-close：不确定
// 就不妄断验证通过），但仍推进游标防卡住。
func (e *Engine) consumeValidationPassed(ctx context.Context) bool {
	e.mu.Lock()
	src := e.validation
	init := e.validationInit
	cursor := e.validationCursor
	e.mu.Unlock()
	if src == nil {
		return false
	}
	if !init {
		// 基线：记录本次 Run 起始 tip，只看之后的新信号（用 SeqSource 取 tip，不扫全部事件）。
		var tip int64
		if e.seq != nil {
			tip, _ = e.seq.Tip(ctx)
		}
		e.mu.Lock()
		e.lastValidationSeq = tip
		e.validationInit = true
		e.mu.Unlock()
		// **基线也要写回会话级游标**（v3-S3 修）：否则下一个 Run 又会重新取基线 →
		// 落在两次 Run 之间的验证信号永远看不见（游标是跨 Run 的唯一接力棒）。
		cursor.Advance(tip)
		return false
	}
	e.mu.Lock()
	after := e.lastValidationSeq
	e.mu.Unlock()
	found, newTip, err := src.ValidationPassedSince(ctx, after)
	e.mu.Lock()
	if newTip > e.lastValidationSeq {
		e.lastValidationSeq = newTip
	}
	hard := err == nil && found
	e.mu.Unlock()
	// 推进会话级游标（下一个 Run 从这里继续扫；只前进，不后退）。
	cursor.Advance(newTip)
	return hard
}

// 编译期证明：*Engine 就是 Assembler，且实现了两个可选扩展接口。
var (
	_ Assembler            = (*Engine)(nil)
	_ BaseOverheadSetter   = (*Engine)(nil)
	_ ToolVisibilitySetter = (*Engine)(nil)
)

// Assemble 实现 Assembler：装配本轮上下文（压缩旧阶段 + 注入 ledger/常识区 + 留存降级）。
func (e *Engine) Assemble(ctx context.Context, history []core.Message, turn int) (Assembled, error) {
	if e == nil {
		return Assembled{}, errors.New("compaction: Engine 为 nil")
	}
	// 整次装配串行（asmMu）：摘要计算要出锁跑模型，用独立锁保整次操作的原子性（F21）。
	e.asmMu.Lock()
	defer e.asmMu.Unlock()

	// ① goal ledger 快照（IO：出状态锁；注入区两半 + 级联输入共用同一份快照）。
	lp, hasLedger := e.fetchLedger(ctx)

	// ①' "验证通过"信号（v2-S1 硬边界接线，J11；IO 出状态锁）。有信号 → 本轮打硬边界
	// （等价 MarkValidationPassed：框架确认的验证通过点收尾当前阶段）。
	if e.consumeValidationPassed(ctx) {
		e.mu.Lock()
		e.pendingHard = true
		e.mu.Unlock()
	}

	e.mu.Lock()
	tip := e.updateTip(ctx)
	e.syncSegments(history, tip)
	// splitMade 是**本轮**语义：上一轮被 split 过的阶段，这轮老化降级要照常发 downgrade
	// 事件（否则该阶段终身不再留痕——清一轮，别让标记变成永久压制）。
	for _, s := range e.segments {
		s.splitMade = false
	}
	// 快照降级前的档位（用于发 downgrade 事件：只对**变深**的阶段发）。
	before := e.tierSnapshot()
	e.applyAgeRetention()
	e.mu.Unlock()

	// ② 物化摘要 ↔ 总量护栏（互相触发：降级 → 新的摘要需求 → 再物化）。
	e.settle(ctx, history, lp)

	// ③ 兜底②：当前阶段超 2×W/3 硬上限 → 按 token 累计中点切开（前半压 L1）。
	e.splitCurrentStage(ctx, history, tip, lp)

	// ④ 兜底②之后再守一次总量（切开会改变构成）。
	e.settle(ctx, history, lp)

	// ⑤ 护栏失败留痕：降不动了（注入区独大 / 历史区全 L2 仍超 W）→ 发硬预算告警，
	//    不许静默装出一个超 W 的请求（F5）。
	e.warnInjectionOversize(ctx, history, lp)

	// ⑥ 预防③：当前阶段超 S=W/3 → 发硬预算告警（按阶段+档位去重，F15）。
	e.warnStageBudget(ctx, history)

	// ⑦ 发事件（downgrade / fallback / split）：一个动作一条事件（F11）。
	e.emitCompactionEvents(ctx, history, before)

	// ⑧ 装配消息（缓存定律：稳定在前、易变贴生长前沿，13 §6/J20/F5）。
	return e.build(history, lp, hasLedger), nil
}

// fetchLedger 取 goal ledger 快照（IO：出状态锁）。空账本 / 取不到 → hasLedger=false。
func (e *Engine) fetchLedger(ctx context.Context) (contracts.LedgerUpdatedPayload, bool) {
	if e.ledger == nil {
		return contracts.LedgerUpdatedPayload{}, false
	}
	lp, err := e.ledger.Ledger(ctx)
	if err != nil {
		return contracts.LedgerUpdatedPayload{}, false
	}
	if !hasLedgerContent(lp) {
		return contracts.LedgerUpdatedPayload{}, false
	}
	return lp, true
}

// updateTip 刷新 seq tip（best-effort：SeqSource 出错时沿用上次值，不阻断装配）。
func (e *Engine) updateTip(ctx context.Context) int64 {
	if e.seq == nil {
		return e.lastTip
	}
	tip, err := e.seq.Tip(ctx)
	if err != nil || tip < e.lastTip {
		return e.lastTip
	}
	e.lastTip = tip
	return tip
}

// syncSegments 维护阶段列表：消费 pendingBoundary/pendingHard、确保存在覆盖 [0, len) 的
// 当前阶段、把当前阶段的 msgTo/seqTo 推进到最新。
func (e *Engine) syncSegments(history []core.Message, tip int64) {
	if len(e.segments) == 0 {
		seqFrom, seqTo := int64(1), tip
		if tip <= 0 {
			// 还没有任何事件落库 → seq 区间不能是 [1,0]（倒挂，F14）：用 [0,0]。
			seqFrom, seqTo = 0, 0
		}
		e.segments = append(e.segments, &segment{
			number:  1,
			tier:    contracts.ContextTierFull,
			msgFrom: 0,
			msgTo:   len(history),
			seqFrom: seqFrom,
			seqTo:   seqTo,
		})
		return
	}
	cur := e.segments[len(e.segments)-1]
	if e.pendingBoundary || e.pendingHard {
		hard := e.pendingHard
		e.pendingBoundary = false
		e.pendingHard = false
		cur.msgTo = len(history)
		cur.seqTo = normSeqTo(cur.seqFrom, tip)
		cur.hardClosed = hard // 硬边界 = 验证通过点收尾（F6）
		e.segments = append(e.segments, &segment{
			number:  cur.number + 1,
			tier:    contracts.ContextTierFull,
			msgFrom: len(history),
			msgTo:   len(history),
			seqFrom: tip,
			seqTo:   tip,
		})
		return
	}
	cur.msgTo = len(history)
	cur.seqTo = normSeqTo(cur.seqFrom, tip)
}

// normSeqTo 归一 seq 区间上界：上界不得小于下界（倒挂区间会让 recall 解析出空/错区间，F14）。
func normSeqTo(from, to int64) int64 {
	if to < from {
		return from
	}
	return to
}

// tierSnapshot 记下当前各阶段的档位（用于判断"这次是否变深"）。
func (e *Engine) tierSnapshot() map[int]contracts.ContextTier {
	out := make(map[int]contracts.ContextTier, len(e.segments))
	for _, s := range e.segments {
		out[s.number] = s.tier
	}
	return out
}

// applyAgeRetention 按年龄分配留存档位（13 §4）：当前阶段 N，已完成阶段里 N-1=full、
// N-2=l1、更老=l2。**永不回升**：只取"年龄期望档"与"当前档"中更深者。
func (e *Engine) applyAgeRetention() {
	current := e.segments[len(e.segments)-1].number
	for _, s := range e.segments {
		if s.number == current {
			s.tier = contracts.ContextTierFull
			continue
		}
		want := ageTier(s.number, current)
		if tierRank(want) > tierRank(s.tier) {
			s.tier = want              // 只降不升
			s.budgetDowngraded = false // 本次是年龄策略触发的（不是预算兜底）
			// 档位变了，摘要要按新档位重算（L1 摘要不是 L2 结论——档位目标不同，F3/J9）。
			s.summary = ""
			s.summarized = false
		}
	}
}

// ageTier 返回已完成阶段 k（当前阶段为 current）的年龄期望档。
func ageTier(k, current int) contracts.ContextTier {
	switch {
	case k == current-1:
		return contracts.ContextTierFull
	case k == current-2:
		return contracts.ContextTierL1
	default:
		return contracts.ContextTierL2
	}
}

// downgradeOldest 把最老的、还能降的已完成阶段压深一级（full→l1→l2）。返回是否有阶段被降
// （false = 全部已 L2，无法再降）。被降阶段清空摘要缓存（档位变了，摘要要重算）。
func (e *Engine) downgradeOldest() bool {
	for _, s := range e.segments {
		if s.number == e.segments[len(e.segments)-1].number {
			continue // 跳过当前阶段
		}
		if tierRank(s.tier) < tierRank(contracts.ContextTierL2) {
			s.tier = nextTier(s.tier)
			s.summary = ""
			s.summarized = false
			s.fallback = false
			s.budgetDowngraded = true
			return true
		}
	}
	return false
}

// completedTokens 估算已完成阶段占用 token（l1/l2 用摘要、full 用原文）。
func (e *Engine) completedTokens(history []core.Message) int64 {
	current := e.segments[len(e.segments)-1].number
	var n int64
	for _, s := range e.segments {
		if s.number == current {
			continue
		}
		n += e.segmentTokens(s, history)
	}
	return n
}

// currentTokens 估算当前（开放）阶段占用 token（verbatim 原文）。
func (e *Engine) currentTokens(history []core.Message) int64 {
	cur := e.segments[len(e.segments)-1]
	return messagesTokens(sliceMessages(history, cur.msgFrom, cur.msgTo))
}

// segmentTokens 估算一个阶段的占用（full = 原文；l1/l2 = 摘要文本；摘要尚未物化时用
// 该档目标 token 兜底估算——**上界**，避免兜底降级循环把"刚降级还没算摘要"的阶段当 0 而提前收手）。
//
// 摘要**物化之后**一律用实测值（estimateTokens(摘要)）：目标值只是目标，不是实际占用
// （F2：拿目标当实际会把"几十个阶段"算成"几十倍预算"，导致过度降级）。
func (e *Engine) segmentTokens(s *segment, history []core.Message) int64 {
	if s.tier == contracts.ContextTierFull {
		return messagesTokens(sliceMessages(history, s.msgFrom, s.msgTo))
	}
	if s.summarized {
		return estimateTokens(s.summary)
	}
	return tierTarget(e.params, s.tier)
}

// totalTokens 装配总量（护栏口径，F2）：base + tools + 注入区 + 历史区 + 当前阶段。
//
// 调用方须持锁（读引擎状态）。
func (e *Engine) totalTokens(history []core.Message, lp contracts.LedgerUpdatedPayload) int64 {
	n := e.baseTokens
	n += estimateTokens(e.rulesText())
	if hasLedgerContent(lp) {
		n += estimateTokens(renderLedgerStable(lp))
		n += estimateTokens(renderLedgerVolatile(lp))
	}
	n += estimateTokens(e.commonSenseText())
	n += e.completedTokens(history)
	n += e.currentTokens(history)
	return n
}

// injectionTokens 注入区（不可压缩的部分）的 token：base + 规则 + 账本两半 + 常识区。
//
// 这一块**降不动**（没有档位可降）——它独大时总量护栏必然失败（护栏只能降历史区），
// 所以要单独算出来，用于判断"超窗到底是谁的锅"（F5）。
// 调用方须持锁。
func (e *Engine) injectionTokens(lp contracts.LedgerUpdatedPayload) int64 {
	n := e.baseTokens
	n += estimateTokens(e.rulesText())
	if hasLedgerContent(lp) {
		n += estimateTokens(renderLedgerStable(lp))
		n += estimateTokens(renderLedgerVolatile(lp))
	}
	n += estimateTokens(e.commonSenseText())
	return n
}

// rulesText 返回注入的项目规则文本（**已在 SetRules 时按上限截断并缓存**——这里直接取，
// 不再每轮重算截断：规则不可压缩，没上限就能顶穿窗口；而每轮重算是纯浪费）。
// 调用方须持锁（或在无并发写的前提下调用）。
func (e *Engine) rulesText() string {
	return e.rules
}

// commonSenseText 返回注入的常识区文本（已在 SetCommonSense 时截断并缓存，同 rulesText）。
func (e *Engine) commonSenseText() string {
	return e.commonSense
}

// enforceTotalBudget 总量护栏（13 §8 + F2）：总量超 W → 最老先降历史区。
// 返回是否降级（false = 已达标，或历史区已全 L2 无处可降 → 交给兜底②切当前阶段）。
// 调用方须持锁。
func (e *Engine) enforceTotalBudget(history []core.Message, lp contracts.LedgerUpdatedPayload) bool {
	if e.totalTokens(history, lp) <= e.params.Window {
		return false
	}
	return e.downgradeOldest()
}

// summaryPlan 是一次摘要计算的输入（锁内取快照 → 锁外调模型，F21）。
type summaryPlan struct {
	seg          *segment
	tokensBefore int64
}

// summaryResult 是一次摘要计算的结果。
type summaryResult struct {
	seg          *segment
	summary      string
	model        string
	usage        *contracts.ModelTurnUsage
	fellBack     bool
	tokensBefore int64
}

// segmentsNeedingSummary 列出需要物化摘要的阶段（l1/l2 且还没算过）。调用方须持锁。
func (e *Engine) segmentsNeedingSummary() []summaryPlan {
	var plans []summaryPlan
	for _, s := range e.segments {
		if s.tier != contracts.ContextTierL1 && s.tier != contracts.ContextTierL2 {
			continue
		}
		if s.summarized {
			continue // 已缓存（阶段不可变 → 摘要不变 → 稳定前缀字节不变，J20）
		}
		plans = append(plans, summaryPlan{seg: s})
	}
	return plans
}

// settle 物化摘要并守预算（两者互相触发：降级会产生新的摘要需求 → 再物化）。
//
// 两道预算（F2）：
//   - 历史区**份额**（13 §8：W − 205k）：超了最老先降；
//   - **总量**硬上限（base + tools + 注入区 + 历史区 + 当前阶段 ≤ W）：超了先降历史区，
//     历史区全 L2 还超 → 交给兜底②切当前阶段。
//
// 两个预算各自放行、相加超 W 是 F2 的病根——所以总量护栏必须在位。
func (e *Engine) settle(ctx context.Context, history []core.Message, lp contracts.LedgerUpdatedPayload) {
	for i := 0; i < maxSettleRounds; i++ {
		e.mu.Lock()
		plans := e.segmentsNeedingSummary()
		e.mu.Unlock()
		if len(plans) > 0 {
			e.materialize(ctx, history, plans, lp)
			continue
		}
		e.mu.Lock()
		downgraded := e.enforceHistoryBudget(history)
		if !downgraded {
			downgraded = e.enforceTotalBudget(history, lp)
		}
		e.mu.Unlock()
		if !downgraded {
			return
		}
	}
}

// enforceHistoryBudget 历史区预算（13 §8 布局份额）：超了就最老先降。
//
// 份额按**本配置**算（historyBudgetOf：注入区预算可配 → 历史区让位），并以 0 为下限
// （负预算地雷由 ValidateLayout 在装配/启动期挡住，运行期再兜一层）。
// 这是**份额**约束；真正的硬上限是 enforceTotalBudget（总量 ≤ W，F2）。
// 调用方须持锁。
func (e *Engine) enforceHistoryBudget(history []core.Message) bool {
	budget := historyBudgetOf(e.params)
	if budget < 0 {
		budget = 0
	}
	if e.completedTokens(history) <= budget {
		return false
	}
	return e.downgradeOldest()
}

// materialize 为给定阶段算摘要（级联 C 案：输入 = ledger + 被压区间全文，永不读别的阶段摘要）。
//
// **压缩模型调用在锁外**（F21）：一次慢的模型调用不该把整个引擎的状态锁按住。
func (e *Engine) materialize(ctx context.Context, history []core.Message, plans []summaryPlan, lp contracts.LedgerUpdatedPayload) {
	ledgerText := renderLedger(lp) // 级联输入的"总体局势"（空账本 → 空串）
	results := make([]summaryResult, 0, len(plans))
	for _, p := range plans {
		text := sliceMessages(history, p.seg.msgFrom, p.seg.msgTo)
		p.tokensBefore = messagesTokens(text)
		summary, model, usage, fellBack := e.summarizeSegment(ctx, p.seg, text, ledgerText)
		results = append(results, summaryResult{
			seg:          p.seg,
			summary:      summary,
			model:        model,
			usage:        usage,
			fellBack:     fellBack,
			tokensBefore: p.tokensBefore,
		})
	}
	e.mu.Lock()
	for _, r := range results {
		r.seg.summary = r.summary
		r.seg.summarized = true
		r.seg.model = r.model
		r.seg.usage = r.usage
		r.seg.fallback = r.fellBack
		r.seg.origTokens = r.tokensBefore
	}
	e.mu.Unlock()
}

// summarizeSegment 调压缩模型生成阶段摘要（级联 C 案：输入 = ledger + 被压区间全文，
// 永不读别的阶段摘要）。summarize 为 nil / 出错 / 空输出 → 退化为基于原文的确定性截断
// （绝不让装配失败，也绝不读别的阶段摘要来拼凑；退化会**留痕**：fallback=true → 发事件，F19）。
//
// 返回 (摘要, 压缩模型名, usage, 是否退化)。
func (e *Engine) summarizeSegment(ctx context.Context, s *segment, text []core.Message, ledgerText string) (string, string, *contracts.ModelTurnUsage, bool) {
	body := messagesText(text)
	input := body
	if ledgerText != "" {
		input = ledgerText + "\n\n=== 被压阶段全文 ===\n" + body
	}
	if e.summarize != nil {
		instruction := tierInstruction(s.tier, e.params)
		res, err := e.summarize.Summarize(ctx, instruction, input)
		if err == nil && strings.TrimSpace(res.Text) != "" {
			// 硬校验：摘要必须守住档位目标（J9）。压缩模型不守预算时**引擎兜底**（F3）——
			// 阈值是框架的硬约束，不是对模型的君子协定。
			return enforceTierTarget(strings.TrimSpace(res.Text), tierTarget(e.params, s.tier)), res.Model, res.Usage, false
		}
	}
	return truncate(body, tierTarget(e.params, s.tier)), "", nil, true
}

// splitCurrentStage 兜底②（13 §7）：当前阶段超硬上限 → 劈成两半，前半压成 L1
// （交 materialize 摘要），后半成新当前阶段。走到这步 = orchestrator 阶段切大了
// （上游错误）→ 发 split 诊断事件留证。
func (e *Engine) splitCurrentStage(ctx context.Context, history []core.Message, tip int64, lp contracts.LedgerUpdatedPayload) {
	for i := 0; i < maxSplitRounds; i++ {
		e.mu.Lock()
		split := e.splitOnce(history, tip)
		e.mu.Unlock()
		if !split {
			return
		}
		// 切出的前半要摘要 → 物化（锁外调模型）。
		e.mu.Lock()
		plans := e.segmentsNeedingSummary()
		e.mu.Unlock()
		if len(plans) > 0 {
			e.materialize(ctx, history, plans, lp)
		}
	}
}

// splitOnce 执行一次切分（调用方须持锁）。返回是否真的切了。
func (e *Engine) splitOnce(history []core.Message, tip int64) bool {
	cur := e.segments[len(e.segments)-1]
	msgs := sliceMessages(history, cur.msgFrom, cur.msgTo)
	tokensBefore := messagesTokens(msgs)
	if tokensBefore <= splitHardFactor*StageBudget(e.params.Window) {
		return false
	}
	// 按 **token 累计中点**切（F2/F20）：按消息条数中点切会切不动"一条巨消息"，
	// 也切不准（一条长消息 ≠ 几条短消息）。
	//
	// 切点 = 累计 token 首次过半那条消息的**后一位**：前半段（含那条主导消息）被压成 L1，
	// 后半段成为新的当前阶段。切点落在末尾（mid == msgTo）时后半段为空——那正是
	// "一条巨消息主导整个阶段"的情形：把它压进 L1 才是有效的兜底。
	mid := cur.msgFrom + tokenMidpoint(msgs)
	if mid <= cur.msgFrom {
		return false // 切不动（阶段里一条消息都没有）
	}
	cur.tier = contracts.ContextTierL1
	cur.msgTo = mid
	cur.seqTo = normSeqTo(cur.seqFrom, tip)
	cur.summary = ""
	cur.summarized = false
	cur.splitMade = true // 这次档位变化由 split 事件覆盖 → 不再另发 downgrade（F11）
	e.segments = append(e.segments, &segment{
		number:  cur.number + 1,
		tier:    contracts.ContextTierFull,
		msgFrom: mid,
		msgTo:   len(history),
		seqFrom: tip,
		seqTo:   tip,
	})
	e.pendingSplits = append(e.pendingSplits, splitRecord{
		stage:        cur.number,
		seqFrom:      cur.seqFrom,
		seqTo:        cur.seqTo,
		tokensBefore: tokensBefore,
	})
	return true
}

// tokenMidpoint 返回按 token 累计中点的切分下标（相对 msgs 起点）。
//
// 取累计 token 首次过半的那条消息的**后一位**：前半段约占一半 token。
// 单条消息就超过一半时 → 返回它的后一位（前半段 = 那条巨消息，它随后被压成 L1）。
func tokenMidpoint(msgs []core.Message) int {
	total := messagesTokens(msgs)
	if total <= 0 {
		return len(msgs) / 2
	}
	half := total / 2
	var acc int64
	for i, m := range msgs {
		acc += estimateTokens(m.Content)
		for j := range m.ToolCalls {
			acc += estimateTokens(m.ToolCalls[j].Name)
			acc += estimateTokens(string(m.ToolCalls[j].Args))
		}
		if acc >= half {
			return i + 1
		}
	}
	return len(msgs)
}

// warnStageBudget 预防③（13 §7）：当前阶段工作区超 S≤W/3 → 发硬预算告警事件
// （prompt 已明示"阶段不能过大"，这是框架侧双保险的另一半）。
//
// 同一阶段同一档位只发一次（F15：否则每轮重复发，事件流被噪声淹没）。
func (e *Engine) warnStageBudget(ctx context.Context, history []core.Message) {
	e.mu.Lock()
	cur := e.segments[len(e.segments)-1]
	tokens := messagesTokens(sliceMessages(history, cur.msgFrom, cur.msgTo))
	if tokens <= StageBudget(e.params.Window) {
		e.mu.Unlock()
		return
	}
	key := warnKey(cur.number, cur.tier)
	if e.warned[key] {
		e.mu.Unlock()
		return
	}
	e.warned[key] = true
	payload := contracts.ContextCompactedPayload{
		Stage:        int64(cur.number),
		Action:       contracts.ContextCompactionActionBudgetWarning,
		Tier:         cur.tier,
		SeqFrom:      cur.seqFrom,
		SeqTo:        cur.seqTo,
		Ref:          StageRefAnchored(cur.number, cur.seqFrom, cur.seqTo),
		TokensBefore: tokens,
		TokensAfter:  tokens,
		Reason:       strPtr(reasonStageOverWThird),
	}
	e.mu.Unlock()
	e.emitEvent(ctx, payload)
}

// warnKey 是 budget_warning 的去重键（阶段号 + 档位）。
func warnKey(stage int, tier contracts.ContextTier) string {
	return strconv.Itoa(stage) + "/" + string(tier)
}

// warnInjectionOversize 护栏失败留痕（F5）：注入区（规则/账本/常识区/base）**不可压缩**，
// 历史区已全 L2 时总量护栏降不动它 → 装配会超 W。此时必须发事件留痕，
// **不许静默失败**（否则就是"看起来有护栏、其实没有"）。
//
// 两种归因：
//   - injection_oversize：注入区自己就超了 W（谁都救不了，只能调注入内容/窗口）；
//   - total_oversize：注入区没超，但注入区 + 历史区(L2) + 当前阶段合计仍超 W。
//
// 去重：按"阶段+档位"（同 F15），同一阶段的同一档位只留一条。
func (e *Engine) warnInjectionOversize(ctx context.Context, history []core.Message, lp contracts.LedgerUpdatedPayload) {
	e.mu.Lock()
	total := e.totalTokens(history, lp)
	if total <= e.params.Window {
		e.mu.Unlock()
		return
	}
	cur := e.segments[len(e.segments)-1]
	key := "inj:" + warnKey(cur.number, cur.tier)
	if e.warned[key] {
		e.mu.Unlock()
		return
	}
	e.warned[key] = true
	reason := reasonTotalOversize
	if e.injectionTokens(lp) > e.params.Window {
		reason = reasonInjectionOversize
	}
	payload := contracts.ContextCompactedPayload{
		Stage:        int64(cur.number),
		Action:       contracts.ContextCompactionActionBudgetWarning,
		Tier:         cur.tier,
		SeqFrom:      cur.seqFrom,
		SeqTo:        cur.seqTo,
		Ref:          StageRefAnchored(cur.number, cur.seqFrom, cur.seqTo),
		TokensBefore: total,
		TokensAfter:  total, // 降不动：前后一样（留痕口径：确实没压下去）
		Reason:       strPtr(reason),
	}
	e.mu.Unlock()
	e.emitEvent(ctx, payload)
}

// emitCompactionEvents 发本轮的事件：一个动作一条事件（F11）。
//
//   - 档位变深（full→l1→l2）→ downgrade 事件（reason 区分触发源：验证通过点 / 预算兜底 / 年龄留存）；
//   - 压缩模型失败退化截断 → fallback 事件（reason=fallback_truncate，F19：退化必须留痕）；
//   - split 兜底 → split 事件（tokens_after 按切后实际算，F26）。
//
// split 产生的阶段**不再**另发 downgrade（它的档位变化由 split 事件覆盖，F11）。
func (e *Engine) emitCompactionEvents(ctx context.Context, history []core.Message, before map[int]contracts.ContextTier) {
	e.mu.Lock()
	var payloads []contracts.ContextCompactedPayload
	for _, s := range e.segments {
		prev, existed := before[s.number]
		deeper := existed && tierRank(s.tier) > tierRank(prev)
		if deeper && s.splitMade {
			deeper = false // split 事件已覆盖这次档位变化（F11：一次动作一条事件）
		}
		switch {
		case s.fallback:
			// 退化截断：无论档位变没变都留痕（F19）。
			payloads = append(payloads, e.payloadOf(s, contracts.ContextCompactionActionFallback,
				reasonFallbackTruncate, s.origTokens, estimateTokens(s.summary)))
		case deeper:
			reason := downgradeReason(prev, s.tier)
			switch {
			case s.budgetDowngraded:
				reason = reasonBudgetFallback
			case s.hardClosed:
				reason = reasonValidationPass
			}
			payloads = append(payloads, e.payloadOf(s, contracts.ContextCompactionActionDowngrade,
				reason, s.origTokens, estimateTokens(s.summary)))
		}
	}
	// split 事件：tokens_after = 前半（摘要）+ 后半（原文）的实际占用（F26：不编造）。
	for _, rec := range e.pendingSplits {
		payloads = append(payloads, e.splitPayload(rec, history))
	}
	e.pendingSplits = nil
	e.mu.Unlock()

	for _, p := range payloads {
		e.emitEvent(ctx, p)
	}
}

// payloadOf 造一个阶段的压缩事件载荷（ref 带 seq 锚 → 跨 Run 稳定，F9）。
func (e *Engine) payloadOf(s *segment, action contracts.ContextCompactionAction, reason string, tokensBefore, tokensAfter int64) contracts.ContextCompactedPayload {
	return contracts.ContextCompactedPayload{
		Stage:        int64(s.number),
		Action:       action,
		Tier:         s.tier,
		SeqFrom:      s.seqFrom,
		SeqTo:        s.seqTo,
		Ref:          StageRefAnchored(s.number, s.seqFrom, s.seqTo),
		TokensBefore: tokensBefore,
		TokensAfter:  tokensAfter,
		Reason:       strPtr(reason),
		Model:        strPtrOrNil(s.model),
		Usage:        s.usage,
	}
}

// strPtrOrNil 空串 → nil（可选字段不写空值）。
func strPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// splitPayload 造 split 事件的载荷（tokens_after 按**发事件那一刻**的段状态统一重算：
// 前半（已压成 L1 → 摘要）+ 后半（当时的当前阶段）各按其**当前**档位算占用）。
//
// 为什么不能按切完那一刻的原文算：同轮连续 split 时，后半随后又会被切成 L1（它的占用
// 变成摘要），拿过期状态算会**高估** tokens_after（与实际占用对不上，F7）。
func (e *Engine) splitPayload(rec splitRecord, history []core.Message) contracts.ContextCompactedPayload {
	var after int64
	var usage *contracts.ModelTurnUsage
	var model string
	for _, s := range e.segments {
		if s.number == rec.stage {
			after += e.segmentTokens(s, history)
			usage = s.usage
			model = s.model
			continue
		}
		if s.number == rec.stage+1 {
			// 后半：按它**当前**的档位算（可能是新的当前阶段=原文，也已被再切成 L1=摘要）。
			after += e.segmentTokens(s, history)
		}
	}
	return contracts.ContextCompactedPayload{
		Stage:        int64(rec.stage),
		Action:       contracts.ContextCompactionActionSplit,
		Tier:         contracts.ContextTierL1,
		SeqFrom:      rec.seqFrom,
		SeqTo:        rec.seqTo,
		Ref:          StageRefAnchored(rec.stage, rec.seqFrom, rec.seqTo),
		TokensBefore: rec.tokensBefore,
		TokensAfter:  after,
		Reason:       strPtr(reasonOrchestratorOversize),
		Model:        strPtrOrNil(model),
		Usage:        usage,
	}
}

// build 组装本轮上下文：消息序
// [rules][ledger 稳定半][L2 区→L1 区（最老先）][常识区][ledger 易变半][当前 verbatim]，
// 以及 context.assembled 的 parts（同序分解）。
//
// 缓存定律（13 §6/J20/F5）：**稳定在前、易变贴生长前沿**——ledger 的稳定半（目标/约束）
// 进前缀，易变半（已完成/待解）每轮可能变，贴当前工作区之前；绝不把易变半混进前缀
// （混进去 = 前缀缓存全废）。
func (e *Engine) build(history []core.Message, lp contracts.LedgerUpdatedPayload, hasLedger bool) Assembled {
	e.mu.Lock()
	defer e.mu.Unlock()

	var msgs []core.Message
	var parts []contracts.ContextPart

	// ① 规则（稳定前缀的一部分；带上限——规则不可压缩，没上限就能顶穿窗口）。
	rules := e.rulesText()
	if rules != "" {
		msgs = append(msgs, core.Message{Role: "user", Content: rules})
		parts = append(parts, newPart(partSpec{
			Source: sourceRules, Kind: contracts.PartKindState, Entries: 1,
			Tokens: estimateTokens(rules), State: contracts.PartStateVerbatim,
		}))
	}

	// ② ledger 稳定半（罕变 → 稳定前缀）。
	stable := ""
	if hasLedger {
		stable = renderLedgerStable(lp)
	}
	if stable != "" {
		msgs = append(msgs, core.Message{Role: "user", Content: stable})
		parts = append(parts, newPart(partSpec{
			Source: sourceLedger, Kind: contracts.PartKindState, Entries: 1,
			Tokens: estimateTokens(stable), State: contracts.PartStateVerbatim,
		}))
	}

	// ③ 已压缩阶段条目（L2 区 → L1 区，最老先）。
	verbatimStart := e.verbatimStartIndex()
	for _, s := range e.segments[:verbatimStart] {
		entry := e.renderStageEntry(s)
		msgs = append(msgs, core.Message{Role: "user", Content: entry})
		parts = append(parts, newPart(partSpec{
			Source: StageRef(s.number), Kind: contracts.PartKindHistory, Entries: 1,
			Tokens: estimateTokens(entry), State: contracts.PartStateCompacted,
			Origins:      []string{seqRangeRef(s.seqFrom, s.seqTo)},
			TargetTokens: int64Ptr(tierTarget(e.params, s.tier)),
		}))
	}

	// ④ 常识区（偶变；位置在热区但内容稳定 → 仍在工作区之前；带上限，同规则）。
	commonSense := e.commonSenseText()
	if commonSense != "" {
		msgs = append(msgs, core.Message{Role: "user", Content: commonSense})
		parts = append(parts, newPart(partSpec{
			Source: sourceCommonSense, Kind: contracts.PartKindState, Entries: 1,
			Tokens: estimateTokens(commonSense), State: contracts.PartStateVerbatim,
		}))
	}

	// ⑤ ledger 易变半（每轮可能变 → 贴生长前沿，绝不进前缀）。
	volatile := ""
	if hasLedger {
		volatile = renderLedgerVolatile(lp)
	}
	if volatile != "" {
		msgs = append(msgs, core.Message{Role: "user", Content: volatile})
		parts = append(parts, newPart(partSpec{
			Source: sourceLedger, Kind: contracts.PartKindState, Entries: 1,
			Tokens: estimateTokens(volatile), State: contracts.PartStateVerbatim,
		}))
	}

	// ⑥ 当前工作区（verbatim：full 的 N-1 + 当前 N）。
	if verbatimStart < len(e.segments) {
		from := e.segments[verbatimStart].msgFrom
		verbatim := sliceMessages(history, from, len(history))
		msgs = append(msgs, verbatim...)
		if len(verbatim) > 0 {
			parts = append(parts, newPart(partSpec{
				Source: sourceHistory, Kind: contracts.PartKindHistory, Entries: int64(len(verbatim)),
				Tokens: messagesTokens(verbatim), State: contracts.PartStateVerbatim,
			}))
		}
	}

	return Assembled{Messages: msgs, Parts: parts}
}

// verbatimStartIndex 返回第一个 full（verbatim）阶段的下标；没有则返回当前阶段下标
// （即只有当前阶段 verbatim，其余全压缩）。full 阶段与当前阶段连续在后（档位随年龄单调）。
func (e *Engine) verbatimStartIndex() int {
	for i, s := range e.segments {
		if s.tier == contracts.ContextTierFull {
			return i
		}
	}
	return len(e.segments) - 1
}

// renderStageEntry 渲染阶段条目，并保证**整条残留** ≤ 档位目标（J9）。
//
// 残留口径 = 整条条目（标题 + 摘要 + 指针说明），不是只算摘要——所以摘要要让出标题的
// 预算，否则实测残留会略微超目标（验收断言的是 parts[].tokens）。
func (e *Engine) renderStageEntry(s *segment) string {
	ref := StageRefAnchored(s.number, s.seqFrom, s.seqTo)
	entry := renderTierEntry(s.tier, s.number, ref, s.summary, e.recallVisible)
	target := tierTarget(e.params, s.tier)
	if target <= 0 || estimateTokens(entry) <= target {
		return entry
	}
	overhead := estimateTokens(entry) - estimateTokens(s.summary)
	room := target - overhead
	summary := s.summary
	if room <= 0 {
		summary = "" // 标题都超预算了：只留标题（极小窗口的极端情形）
	} else {
		summary = truncate(s.summary, room)
	}
	return renderTierEntry(s.tier, s.number, ref, summary, e.recallVisible)
}

// emitEvent best-effort 发一条 context.compacted（观测性事实，失败不阻断装配）。
func (e *Engine) emitEvent(ctx context.Context, p contracts.ContextCompactedPayload) {
	if e.emit == nil {
		return
	}
	_ = e.emit.EmitCompacted(ctx, p)
}

// Resolve 解析一个指针取回持久化原文全文（J15：指针解析 = core 的确定性基础能力；
// recall 工具的工具化形态）。支持 "stage:N"（本 Run 别名）、"stage:N@e:<from>..e:<to>"
// （带 seq 锚的稳定指针，跨 Run 仍指向同一段原文，F9）与 "e:<from>..e:<to>"（机器 seq 区间）。
// 返回 (原文, 指针合法, error)：指针不合法 → (,"false",nil)；合法但无回捞源 → ("",true,nil)。
func (e *Engine) Resolve(ctx context.Context, ref string) (string, bool, error) {
	from, to, ok := e.refToSeqRange(ref)
	if !ok {
		return "", false, nil
	}
	if e.text == nil {
		return "", true, nil
	}
	text, err := e.text.Text(ctx, from, to)
	if err != nil {
		return "", true, err
	}
	return text, true, nil
}

// refToSeqRange 把指针解析成 seq 区间。
//
// 带锚的指针（stage:N@e:from..e:to）**直接用锚里的区间** → 跨 Run 稳定（F9）；
// 短指针（stage:N）按本 Run 的阶段表解析（阶段号每 Run 重排 → 只是本 Run 别名）。
func (e *Engine) refToSeqRange(ref string) (from, to int64, ok bool) {
	// 纯机器指针 "e:<from>..e:<to>"：直接按 seq 区间解析（不查阶段表 → 跨 Run 也成立）。
	// 这条分支必须在 ParseRef 之前——ParseRef 只认 "stage:" 前缀，放在后面就是死代码，
	// 而 recall 的描述/schema/报错文案三处都宣称该形态合法（不能宣称一个解析不了的形态）。
	if fr, tr, parsed := parseSeqRange(ref); parsed {
		return fr, tr, true
	}
	n, f, t, anchored, valid := ParseRef(ref)
	if !valid {
		return 0, 0, false
	}
	if anchored {
		return f, t, true
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range e.segments {
		if s.number == n {
			return s.seqFrom, s.seqTo, true
		}
	}
	return 0, 0, false
}

// ---------------------------------------------------------------------------
// 纯助手
// ---------------------------------------------------------------------------

// sliceMessages 取 history[from:to]（越界安全）。
func sliceMessages(history []core.Message, from, to int) []core.Message {
	if from < 0 {
		from = 0
	}
	if to > len(history) {
		to = len(history)
	}
	if from >= to {
		return nil
	}
	return history[from:to]
}

// seqRangeRef 渲染机器可读的 seq 区间 "e:<from>..e:<to>"（ContextPart.origins 口径）。
func seqRangeRef(from, to int64) string {
	return "e:" + strconv.FormatInt(from, 10) + "..e:" + strconv.FormatInt(to, 10)
}

// parseSeqRange 解析 "e:<from>..e:<to>"。
func parseSeqRange(ref string) (from, to int64, ok bool) {
	s := strings.TrimPrefix(ref, "e:")
	if s == ref {
		return 0, 0, false
	}
	parts := strings.SplitN(s, "..e:", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	f, err1 := strconv.ParseInt(parts[0], 10, 64)
	t, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil || f < 0 || t < f {
		return 0, 0, false
	}
	return f, t, true
}

// hasLedgerContent 判定账本是否有内容（四张清单任一非空）。
func hasLedgerContent(lp contracts.LedgerUpdatedPayload) bool {
	return len(lp.Goals) > 0 || len(lp.Constraints) > 0 || len(lp.Completed) > 0 || len(lp.Pending) > 0
}

// downgradeReason 给出降级事件的原因标签（审计用）。
func downgradeReason(from, to contracts.ContextTier) string {
	return "retention_downgrade_" + string(from) + "_to_" + string(to)
}

func strPtr(s string) *string { return &s }
func int64Ptr(v int64) *int64 { return &v }

// enforceTierTarget 硬校验档位目标：摘要超目标就截到目标（J9：L1≤10% / L2≤1% 窗口）。
//
// 压缩模型**不守预算**时由引擎兜底——阈值是框架的硬约束，不是对模型的君子协定（F3）。
func enforceTierTarget(summary string, target int64) string {
	if target <= 0 {
		return summary
	}
	if estimateTokens(summary) <= target {
		return summary
	}
	return truncate(summary, target)
}

// truncate 把文本截到 ≤ maxTokens token（**按 estimateTokens 口径逐 rune 累计**，不是字节预算：
// 中文 3 字节/字，按字节切会超目标 3 倍，F4）。
func truncate(s string, maxTokens int64) string {
	if maxTokens <= 0 {
		return s
	}
	if estimateTokens(s) <= maxTokens {
		return s
	}
	runes := []rune(s)
	// 预留 "…" 的 1 token（3 字节 → 1 token），保证截完仍 ≤ maxTokens。
	budget := maxTokens - 1
	lo, hi := 0, len(runes)
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if estimateTokens(string(runes[:mid])) <= budget {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	if lo <= 0 {
		return ""
	}
	return string(runes[:lo]) + "…"
}
