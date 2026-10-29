package compaction

// 预算数学（13 §8）与可配参数（J9：L1/L2 阈值为**可配初值**，不写死）。
//
// 口径（两阶段合计 ≤300k → 单阶段 S≤150k；一轮 loop≈30k → 一阶段 ≤5 轮）：
//
//	布局：[前缀 15k][常识区 30k][历史区][当前阶段 ≤150k][尾部 10k]；历史区预算 = W − 205k
//	阶段预算法则：S ≤ W/3；硬约束：W ≥ 256k（不满足 → 拒绝挂载并说明）
//
// 205k = 前缀 15k + 常识区 30k + 阶段槽 150k + 尾部 10k（布局常量，唯一定义处，D21）。

// 布局常量（13 §8）。**只在源头定义一次**，其余引用不重抄（D21）。
const (
	// MinWindow 是窗口 W 的硬约束下界（13 §8：W < 256k 时历史区预算为负、本设计不成立）。
	MinWindow int64 = 256_000
	// DefaultWindow 是 Params.Window 缺省值（0 = 用它）：1M 窗口下可追溯百级阶段
	// （13 §8 表），是 tiered 策略的主场。
	DefaultWindow int64 = 1_000_000

	// prefixBudget 前缀（system / 规则）预算（13 §8：15k）。
	prefixBudget int64 = 15_000
	// commonSenseBudget 常识区预算（13 §8：30k）。
	commonSenseBudget int64 = 30_000
	// stageSlotBudget 单阶段槽预算（13 §8：两阶段合计 ≤300k → 单阶段 ≤150k）。
	stageSlotBudget int64 = 150_000
	// tailBudget 尾部预算（13 §8：10k）。
	tailBudget int64 = 10_000

	// stageBudgetDivisor 阶段预算法则的分母（13 §8：S ≤ W/3）。
	stageBudgetDivisor int64 = 3
)

// HistoryBudget 返回历史区（可留存的已完成阶段）预算 = W − 205k（13 §8 布局默认）。
//
// 205k = 前缀 15k + 常识区 30k + 阶段槽 150k + 尾部 10k（布局常量，唯一定义处，D21）。
//
// W < 205k 时为正数不可能（返回负值即"本设计在此窗口不成立"的信号，调用方应已过
// ValidateWindow 拦住 W < 256k）。
//
// 注入区预算（RulesBudget/CommonSenseBudget）可配时，历史区份额按实际让位——见
// historyBudgetOf（**唯一实现处**，本函数是它的布局默认口径）。
func HistoryBudget(w int64) int64 { return historyBudgetOf(Params{Window: w}) }

// historyBudgetOf 返回本配置下的历史区预算 = W − (前缀 + 常识区 + 阶段槽 + 尾部)。
//
// 注入区预算可配（Params.RulesBudget/CommonSenseBudget）→ 历史区份额跟着让位，
// 布局口径仍然自洽（用默认值时 = W − 205k，与 13 §8 的表一致）。
//
// **可能为负**：注入区预算配得比窗口还大时（例：W=256k + 规则 100k）历史区就没了。
// 负预算地雷由 ValidateLayout 在**装配/启动期**挡住；运行期再兜一层 max(0, …)（见
// enforceHistoryBudget）。
func historyBudgetOf(p Params) int64 {
	p = p.normalized()
	return p.Window - (p.RulesBudget + p.CommonSenseBudget + stageSlotBudget + tailBudget)
}

// ValidateLayout 校验**布局自洽**：历史区预算不得为负。
//
// 负数 = 注入区（前缀 + 常识区）把窗口吃光了 → 历史区没有份额 → 留存策略无从下手
// （历史区预算恒 < 0 → 护栏会无脑把历史全降 L2）。因此在**装配/启动期**就拒绝并说明
// （fail-close），而不是运行期拿一个负预算硬跑。
//
// 口径：historyBudgetOf(p) ≥ 0（用默认值时 = W − 205k，与 13 §8 的表一致）。
func ValidateLayout(p Params) error {
	if historyBudgetOf(p) < 0 {
		p = p.normalized()
		return &LayoutOverflowError{
			Window:        p.Window,
			RulesBudget:   p.RulesBudget,
			CommonSense:   p.CommonSenseBudget,
			HistoryBudget: historyBudgetOf(p),
		}
	}
	return nil
}

// LayoutOverflowError 是注入区预算把窗口吃光的错误（携带数值供调用方说明）。
type LayoutOverflowError struct {
	Window        int64
	RulesBudget   int64
	CommonSense   int64
	HistoryBudget int64
}

func (e *LayoutOverflowError) Error() string {
	return "compaction: 窗口 W=" + itoa(e.Window) +
		" 下注入区预算（前缀 " + itoa(e.RulesBudget) + " + 常识区 " + itoa(e.CommonSense) +
		"）把历史区挤成 " + itoa(e.HistoryBudget) +
		"（负数 = 历史区没有份额，留存策略无从下手）；请调小注入区预算或增大窗口"
}

// StageBudget 返回单阶段预算 S = W/3（13 §8 阶段预算法则；预防告警的阈值）。
func StageBudget(w int64) int64 { return w / stageBudgetDivisor }

// ValidateWindow 校验窗口 W 满足硬约束 W ≥ 256k（13 §8）。不满足 → 返回 error
// （挂载时调用，拒绝并说明，绝不静默降级——负历史区预算会让留存策略无从下手）。
func ValidateWindow(w int64) error {
	if w < MinWindow {
		return &WindowTooSmallError{Window: w, Min: MinWindow}
	}
	return nil
}

// WindowTooSmallError 是窗口小于硬约束的错误（携带数值供调用方说明）。
type WindowTooSmallError struct {
	Window int64
	Min    int64
}

func (e *WindowTooSmallError) Error() string {
	return "compaction: 窗口 W=" + itoa(e.Window) +
		" 小于硬约束 W≥" + itoa(e.Min) +
		"（13 §8：W<256k 时历史区预算为负、tiered 策略不成立；请增大窗口/压缩阶段/让常识区让位，或不挂本策略走 append）"
}

// Params 是 tiered 策略的可配参数（进 context_strategy.params 一类的配置面，**不写死**，
// J9：数字为可配初值，真实负载数据回头校准）。
type Params struct {
	// Window 上下文窗口预算 W（token；0 = DefaultWindow）。挂载时须 ≥ MinWindow。
	Window int64
	// L1TargetPct L1（浅摘要）目标残留占窗口百分比（J9：默认 10，可配初值）。
	L1TargetPct float64
	// L2TargetPct L2（深压缩）目标残留占窗口百分比（J9：默认 1，可配初值）。
	L2TargetPct float64
	// RulesBudget 项目规则注入的 token 上限（0 = 布局默认 prefixBudget=15k，13 §8）。
	//
	// 规则是**不可压缩**的注入内容：没有上限时，一条超长规则就能把整个窗口顶穿
	// （总量护栏降不动它 → 只能报错）。上限让"注入区"有界。
	RulesBudget int64
	// CommonSenseBudget 常识区注入的 token 上限（0 = 布局默认 commonSenseBudget=30k，13 §8）。
	CommonSenseBudget int64
	//
	// **账本（ledger）没有注入上限**（诚实口径）：goal ledger 的内容是**语义资产**
	// （目标/约束/已完成/待解），砍掉它等于悄悄丢目标——所以不设上限，改为：
	// ① 账本写入侧尚未接线（F8：core/durable.LedgerWriter 无调用方）→ 当前恒空；
	// ② 真撑爆窗口时由总量护栏的告警留痕（reason=injection_oversize）兜底。
	// 若将来账本接线且需要上限，另加可配项（别把语义资产当普通注入内容砍）。
}

// L1/L2 目标百分比缺省值（J9 拍板：可配初值）。
const (
	DefaultL1TargetPct = 10.0
	DefaultL2TargetPct = 1.0
)

// DefaultParams 返回带缺省值的参数（Window=1M，L1=10%，L2=1%）。
func DefaultParams() Params {
	return Params{
		Window:      DefaultWindow,
		L1TargetPct: DefaultL1TargetPct,
		L2TargetPct: DefaultL2TargetPct,
	}
}

// normalized 把 0/非法值解析成缺省（Window=0 → 默认；百分比 ≤0 → 默认），返回规范化副本。
func (p Params) normalized() Params {
	if p.Window == 0 {
		p.Window = DefaultWindow
	}
	if p.L1TargetPct <= 0 {
		p.L1TargetPct = DefaultL1TargetPct
	}
	if p.L2TargetPct <= 0 {
		p.L2TargetPct = DefaultL2TargetPct
	}
	if p.RulesBudget <= 0 {
		p.RulesBudget = prefixBudget
	}
	if p.CommonSenseBudget <= 0 {
		p.CommonSenseBudget = commonSenseBudget
	}
	return p
}

// L1TargetTokens 返回 L1 摘要的目标 token 数 = Window × L1TargetPct%（J9）。
func (p Params) L1TargetTokens() int64 {
	p = p.normalized()
	return int64(float64(p.Window) * p.L1TargetPct / 100.0)
}

// L2TargetTokens 返回 L2 摘要的目标 token 数 = Window × L2TargetPct%（J9）。
func (p Params) L2TargetTokens() int64 {
	p = p.normalized()
	return int64(float64(p.Window) * p.L2TargetPct / 100.0)
}

// itoa 是 strconv.FormatInt 的极简封装（避免为本包引入 strconv 只为错误文案）。
func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
