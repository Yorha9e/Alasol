// Package sessioncfg 是 G-c per-prompt 配置体系的 **fold 表**（设计 15 §3）：
// 当前配置 = `config.changed` 事件的逐字段 last-write-wins 折叠（F5 纪律：事件流是
// 唯一事实源，本表只是可重建投影——重启回放即得，审计免费）。
//
// 形态照 core/discovery 的纯状态机纪律：
//
//   - **无 IO**：事件由宿主喂进来（Apply），扫描/重放在 host/config_fold.go；
//   - **Apply 幂等、乱序容忍**：按 (seq, ts) 定序，后写覆盖先写；同一条事件重复
//     投递不改变结果；字段**缺失不动**（变更集语义：出现 = 覆盖、缺失 = 不变、
//     空串 = 清除回 hub 默认）；
//   - **坏载荷跳过**（D7 宽容）：已知 kind 解不开 payload 就跳过这条的专属投影，
//     不中断、不影响其余事件。
//
// 依赖方向：本包只 import contracts（core 边界）——**不 import host**；host 引用
// 本包的 KindConfigChanged 常量（不另抄字面量，避免两份漂移）。
//
// 生效链（设计 15 §3.3）：hub 启动默认（Config）→ session 配置（本表折叠）→
// 本轮快照（loop 的 RunOverrides 钩子在 Run 开始时冻结）。本表只负责第二层。
package sessioncfg

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/Yorha9e/alasol/contracts"
)

// KindConfigChanged 是 per-prompt 配置变更的事件 kind（契约 event.json 的
// x-kind-payload：config.changed → ConfigChangedPayload）。
//
// 常量住本包（fold 表是它的第一消费者）；host 的写路径与扫描都引用它。
const KindConfigChanged = "config.changed"

// Field 是一个可覆盖字段的折叠态——**三态**（要能区分"覆盖为空串 = 用默认"与
// "未覆盖"，最终生效值由调用方叠 hub 默认）：
//
//	未写过    Written=false          → 从来没有覆盖记录，生效值 = hub 默认；
//	显式清除  Written=true, Value=""  → 空串清除记录，生效值 = hub 默认（审计上
//	                                    仍能看出"曾覆盖过、后来被清掉"）；
//	显式覆盖  Written=true, Value≠""  → 生效值 = Value。
type Field struct {
	// Written 报告该字段是否出现过显式覆盖记录（含空串清除）。
	Written bool
	// Value 是覆盖值；Written=true 且 Value="" = 显式清除（回 hub 默认）。
	Value string
}

// HasValue 报告是否有**生效的非空覆盖**（true → 调用方用 Value；false → 叠 hub 默认）。
func (f Field) HasValue() bool { return f.Written && f.Value != "" }

// NumField 是一个**数值**字段的折叠态——三态（与 Field 同口径，只是值是数）：
//
//	未写过    Written=false          → 生效值 = hub 默认；
//	显式清除  Written=true, Value=0  → 数值 0 = 清除回 hub 默认（沿用 host.Config 的
//	                                    0 = 默认口径：0 不是"窗口为 0"，而是"用默认"）；
//	显式覆盖  Written=true, Value≠0  → 生效值 = Value。
//
// 为什么单独一类（不把数值塞进 Field）：Field 的"清除"载体是空串，数值没有空串——
// 0 既是合法值（窗口 0 无意义但预算 0 = 不限）又是"清除"的载体，必须显式约定。
type NumField struct {
	// Written 报告该字段是否出现过显式覆盖记录（含 0 = 清除）。
	Written bool
	// Value 是覆盖值（Written=true 时有效；0 = 显式清除，回 hub 默认）。
	Value int64
}

// HasValue 报告是否有**生效的非零覆盖**（true → 调用方用 Value；false → 叠 hub 默认）。
func (f NumField) HasValue() bool { return f.Written && f.Value != 0 }

// FloatField 是**浮点**字段（百分比）的折叠态——三态同 NumField。
type FloatField struct {
	// Written 报告该字段是否出现过显式覆盖记录（含 0 = 清除）。
	Written bool
	// Value 是覆盖值（Written=true 时有效；0 = 显式清除，回 hub 默认）。
	Value float64
}

// HasValue 报告是否有**生效的非零覆盖**（true → 调用方用 Value；false → 叠 hub 默认）。
func (f FloatField) HasValue() bool { return f.Written && f.Value != 0 }

// ProviderState 是 provider 覆盖项（base_url / api_key_env）的折叠态。
//
// Touched = provider 对象出现过（含"两字段都空串 = 清除覆盖"的记录）——与"从未
// 写过 provider"（Touched=false）可区分。两个字段各自独立 LWW（字段级语义同
// ConfigChangedPayload：出现 = 覆盖、缺失 = 不变、空串 = 清除该字段）。
type ProviderState struct {
	// Touched 表示 provider 对象曾在变更集里出现过（含清除记录）。
	Touched bool
	// BaseURL / APIKeyEnv 是两个字段各自的折叠态（三态见 Field）。
	BaseURL   Field
	APIKeyEnv Field
}

// StringListField 是一个**字符串列表**字段的折叠态（Field 的列表版；**三态**，
// 与批次2 的 Field 同口径，只是"清除"的载体从空串换成空数组）：
//
//	未写过    Written=false           → 该项未设置（生效值由调用方取默认：不限 / 不排除）；
//	显式清空  Written=true, Value=[]  → 空数组清除记录（only = 可见集为空；
//	                                     except = 不排除任何工具）；
//	显式覆盖  Written=true, Value≠[]  → 生效值 = Value。
//
// 关键：**空数组 ≠ 缺失**（这是 tools 的"清空该项"与"该项未设置"必须可区分的原因）。
type StringListField struct {
	// Written 报告该列表字段是否出现过显式覆盖记录（含空数组清除）。
	Written bool
	// Value 是覆盖值（Written=true 时有效；空数组 = 显式清空该项）。
	Value []string
}

// IsSet 报告该项是否**被显式设置过**（含空数组清除记录——"清空"也是一次设置，
// 与"未设置"必须可区分：only 的空数组 = 一个工具都不暴露）。
//
// 刻意**不叫** HasValue：Field.HasValue 的口径是"有非空覆盖"，而列表字段的
// "空数组 = 清空该项"也是一次有效设置（口径不同，名字也不同，免得读混）。
func (f StringListField) IsSet() bool { return f.Written }

// List 返回覆盖值的副本（nil = 未设置；非 nil 的空切片 = 显式清空）。
func (f StringListField) List() []string {
	if !f.Written {
		return nil
	}
	out := make([]string, len(f.Value))
	copy(out, f.Value)
	return out
}

// copy 返回该字段的副本（切片另拷一份——Current 返回的值类型不该与表内状态共享
// 底层数组，调用方改了返回值也不会污染 fold 表）。
func (f StringListField) copy() StringListField {
	return StringListField{Written: f.Written, Value: f.List()}
}

// ToolsState 是 session 工具白名单（G-e，设计 15 §3.5）的折叠态。
//
// **整组 LWW**：tools 组在 fold 表里是**一个字段**——与 model 等标量字段同口径：
// 同一次提交的 only/except 一起落，后写**整组**覆盖前一组的 tools 配置，**不拆子字段
// 跨事件合并**。于是组内子字段的"缺失 = 不变"只在**组内**成立：本次提交里没带的
// 子项 = 该项**未设置**（不是"沿用上一条事件的旧值"）。
//
// 组的三态：
//
//	组缺失（payload 里没有 tools）   → 整组不变（Touched 保持原值）；
//	组出现且两子项都缺（tools: {}）  → 整组被覆盖成"两项都未设置" = 清除回默认
//	                                  （全部已注册工具可见，仍受类型白名单与策略链管辖）；
//	组出现且带子项                    → 整组被覆盖成该组的内容（子项三态见 StringListField）。
type ToolsState struct {
	// Touched 表示 tools 组曾出现过（含"清除"记录：组出现但两子项都缺）。
	Touched bool
	// Only / Except 是组内两个子项的折叠态（三态见 StringListField）。
	Only   StringListField
	Except StringListField
}

// ContextState 是 session **上下文装配面**（G-h）的折叠态。
//
// **子字段各自 LWW**（与 ProviderState 同口径，**不学 ToolsState 的整组 LWW**）：
// 每个子项（kind / window / l1_pct / l2_pct / rules_budget / common_sense_budget）
// **各带一个 LWW 戳**，后写的子项只覆盖自己那一项，不影响别的子项。
//
// 为什么不学 tools 的整组 LWW：整组 LWW 下，一次只带部分子项的提交会把**没带的子项**
// 一起清掉（静默丢字段）——上下文面正是"只想调一个旋钮"的典型场景（改窗口不该把
// 百分比与注入区预算一起打回默认）。
//
// 子项三态：
//
//	kind（字符串三态，见 Field）：出现 = 覆盖；空串 = 清除回默认；缺失 = 不变；
//	数值子项（数值三态，见 NumField / FloatField）：出现 = 覆盖；**0 = 清除回默认**；
//	                                              缺失 = 不变（沿用上一次的值）。
type ContextState struct {
	// Touched 表示 context 对象曾在变更集里出现过（含"各项都没带"的空对象）。
	Touched bool
	// Kind 是上下文策略 kind 的折叠态（字符串三态：空串 = 清除回默认）。
	//
	// **值空间**（G-h 的 session 级）：""（清除回默认）/ "append"（**显式**追加式）/
	// "tiered"（两级压缩）。"append" 是**实值**（HasValue = true）——它让 session 能把
	// tiered 显式切回追加式（"空串"只清除、落回宿主默认，切不回来）。
	Kind Field
	// Window 是上下文窗口预算 W 的折叠态（数值三态：0 = 清除回默认）。
	Window NumField
	// L1Pct / L2Pct 是 L1（浅摘要）/ L2（深压缩）目标残留百分比的折叠态（浮点三态）。
	L1Pct FloatField
	L2Pct FloatField
	// RulesBudget / CommonSenseBudget 是注入区（项目规则 / 常识区）token 上限的折叠态
	// （数值三态：0 = 清除回默认）。
	RulesBudget       NumField
	CommonSenseBudget NumField
}

// ExecState 是 session **执行面**（会话工作区，L2 per-session 执行上下文）的折叠态。
//
// 形态照 ProviderState：**子字段各自 LWW**（cwd 与 worktree 各带一个戳，后写的子项
// 只覆盖自己那一项；没带的子项沿用上一次的值）。
//
// 为什么不学 ToolsState 的整组 LWW：cwd 与 worktree 是**同一件事的两种写法**（路径 /
// 受管 worktree 根里的具名工作区）——整组 LWW 下"只改 cwd"会把 worktree 静默清掉，
// 而 worktree 往往是**创建时登记的归属**（清掉它等于悄悄解绑）。
//
// 子项三态（字符串三态，见 Field）：
//
//	出现 = 覆盖该项；空串 = 清除该项（回到不锚定 / 不绑 worktree）；缺失 = 不变。
//
// **没有"hub 默认"这一层**：会话工作区**不**从 hub 级 Config.CWD 继承（那是 gate 推导
// 的输入，不是工具的工作目录——把它当默认会静默改变既有部署的工具行为）。故
// HasValue=false = "这一项没设"（由调用方按"不锚定"处理），而不是"回默认"。
type ExecState struct {
	// Touched 表示 exec 对象曾在变更集里出现过（含"两项都没带"的空对象）。
	Touched bool
	// Cwd 是会话工作区根目录的折叠态（字符串三态：空串 = 清除该项）。
	Cwd Field
	// Worktree 是受管 worktree 名的折叠态（字符串三态：空串 = 清除该项）。
	Worktree Field
}

// SessionConfig 是一个 session 的配置 fold 结果（**值类型**：Current 返回副本，
// 调用方读它不持有表锁）。生效值 = 各字段 HasValue() 的 Value，否则由调用方叠
// hub 默认（生效链第一层在 host/loop 装配里）。
type SessionConfig struct {
	Model           Field
	ReasoningEffort Field
	Provider        ProviderState
	// Tools 是 session 工具白名单（G-e）的折叠态（整组 LWW，见 ToolsState）。
	Tools ToolsState
	// Context 是 session 上下文装配面（G-h）的折叠态（子字段各自 LWW，见 ContextState）。
	Context ContextState
	// Exec 是 session 执行面（会话工作区，L2）的折叠态（子字段各自 LWW，见 ExecState）。
	Exec ExecState
}

// fieldState 是表内单字段的折叠态（值 + LWW 戳）。
//
// 泛型化：字符串 / 整数 / 浮点三类字段共用同一套 LWW 逻辑（后写覆盖先写、旧事件不动、
// 重复投递幂等）——三态的差别只在"清除"的载体（空串 / 0），不在折叠逻辑。
type fieldState[T any] struct {
	set   bool
	value T
	seq   int64
	ts    string
}

// apply 把一条变更集里的字段值折进来（后写覆盖先写；旧事件/重复投递不动）。
func (fs *fieldState[T]) apply(value T, seq int64, ts string) {
	if fs.set && !newerStamp(seq, ts, fs.seq, fs.ts) {
		return // 旧事件（或同一条事件的重复投递）：折叠态不变（幂等 + 乱序容忍）
	}
	fs.set, fs.value, fs.seq, fs.ts = true, value, seq, ts
}

// stringFieldOf 折出对外的三态 Field（调用方持锁）。
func stringFieldOf(fs *fieldState[string]) Field {
	return Field{Written: fs.set, Value: fs.value}
}

// numFieldOf 折出对外的三态 NumField（调用方持锁）。
func numFieldOf(fs *fieldState[int64]) NumField {
	return NumField{Written: fs.set, Value: fs.value}
}

// floatFieldOf 折出对外的三态 FloatField（调用方持锁）。
func floatFieldOf(fs *fieldState[float64]) FloatField {
	return FloatField{Written: fs.set, Value: fs.value}
}

// newerStamp 判定 (seq, ts) 是否**新于**已存戳（seq 守卫，照 discovery.touchSession
// 样板；后写覆盖先写）。
//
//   - seq 优先（存储层分配的全局序号）；
//   - 同 seq（含两边都没 seq 的 0——导入/合成事件可能没有 seq）用 ts 兜底；
//   - 两者都相同 = 同一条事件的重复投递 → false（Apply 幂等的根基）。
//
// ts 比较先按 RFC3339Nano 解析（事件 ts 是 UTC；解析失败退字符串序——保守不动，
// 宁可漏一次覆盖也不乱写）。
func newerStamp(seq int64, ts string, oseq int64, ots string) bool {
	if seq != oseq {
		return seq > oseq
	}
	if ts == ots {
		return false
	}
	t1, e1 := time.Parse(time.RFC3339Nano, ts)
	t2, e2 := time.Parse(time.RFC3339Nano, ots)
	if e1 == nil && e2 == nil {
		return t1.After(t2)
	}
	return ts > ots
}

// sessionState 是表内单个 session 的折叠态。
type sessionState struct {
	model   fieldState[string]
	effort  fieldState[string]
	baseURL fieldState[string]
	keyEnv  fieldState[string]
	// providerSeen 表示 provider 对象出现过（含清除记录；只增不减——"出现过"是事实）。
	providerSeen bool
	// tools 是工具白名单组（G-e）的折叠态（**整组 LWW**：组自带戳，整组覆盖）。
	tools toolsState
	// context 是上下文装配面（G-h）的折叠态（**子字段各自 LWW**：每个子项自带戳）。
	context contextState
	// exec 是执行面（会话工作区，L2）的折叠态（**子字段各自 LWW**：cwd / worktree
	// 各自带戳）。
	exec execState
	// seen 是已折叠的 config.changed 事件 id 集合（Generation 的计数源：按事件 id
	// 去重 → 幂等重放不重数、乱序投递不漏数）。id 为空的事件不计（纵深防御：拿不到
	// 稳定判别位就不拿它推进"届数"，避免所有无 id 事件挤成一个计数）。
	seen map[contracts.Ulid]struct{}
}

// execState 是表内 exec 面的折叠态（**子字段各自 LWW**：cwd / worktree 各一个戳）。
type execState struct {
	// seen 表示 exec 对象出现过（含空对象；"出现过"是事实）。
	seen bool
	// cwd 是会话工作区根目录的折叠态（字符串三态）。
	cwd fieldState[string]
	// worktree 是受管 worktree 名的折叠态（字符串三态）。
	worktree fieldState[string]
}

// contextState 是表内 context 面的折叠态（**子字段各自 LWW**：每个子项一个戳，
// 不像 tools 那样整组共用一个戳——子字段各自 LWW 才不会"一次只带一项的提交"把
// 别的项静默丢掉）。
type contextState struct {
	// seen 表示 context 对象出现过（含"各项都没带"的空对象；只增不减——"出现过"是事实）。
	seen bool
	// kind 是策略 kind 的折叠态（字符串三态）。
	kind fieldState[string]
	// window 是窗口预算 W 的折叠态（数值三态：0 = 清除回默认）。
	window fieldState[int64]
	// l1 / l2 是 L1/L2 目标残留百分比的折叠态（浮点三态：0 = 清除回默认）。
	l1 fieldState[float64]
	l2 fieldState[float64]
	// rules / cs 是注入区（项目规则 / 常识区）token 上限的折叠态（数值三态）。
	rules fieldState[int64]
	cs    fieldState[int64]
}

// toolsState 是表内 tools 组的折叠态（**整组 LWW**：戳挂在组上，不挂子项）。
//
// 为什么戳挂组不挂子项：tools 在 fold 表里是**一个字段**（与 model 等标量字段同口径）——
// 后写整组覆盖前一组，**不拆子字段跨事件合并**。若戳挂子项，就会退化成"子项各自 LWW +
// 跨事件合并"（后一条只带 only 时，上一条的 except 会被沿用），那不是 tools 的语义。
type toolsState struct {
	// set 表示 tools 组出现过（含"清除"记录：组出现但两子项都缺）。
	set bool
	// seq / ts 是组的 LWW 戳（与 fieldState 同款：seq 优先、ts 兜底）。
	seq int64
	ts  string
	// only / except 是组内两个子项的三态（随整组一起被覆盖）。
	only   StringListField
	except StringListField
}

// listFieldOf 把契约里的列表字段折成三态（nil = 缺失 = 该项未设置；非 nil = 覆盖，
// **空数组 = 清空该项**——空数组 ≠ 缺失，故必须能区分）。
func listFieldOf(list *contracts.ToolNameList) StringListField {
	if list == nil {
		return StringListField{}
	}
	out := make([]string, len(*list))
	copy(out, *list)
	return StringListField{Written: true, Value: out}
}

// Table 是 per-prompt 配置 fold 表（G-c）：core 内纯状态组件。并发安全（一把 RWMutex）。
//
// 零值可用，但请用 New。**不做 IO**：事件由宿主喂进来（Apply），宿主负责扫描/重放
// （host/config_fold.go 的拉式扫描，照 discoveryFeed 样板）。
type Table struct {
	mu       sync.RWMutex
	sessions map[contracts.Ulid]*sessionState
}

// New 建一张空的配置 fold 表。
func New() *Table {
	return &Table{sessions: make(map[contracts.Ulid]*sessionState)}
}

// Apply 把一条事件折进配置表（幂等、可重复、可乱序）。
//
// 只吃 **durable 的 config.changed**（F8：流事件不是事实；其余 kind 与本表无关）。
// 载荷不可解析 → 跳过这条的专属投影（D7 宽容：不中断、不影响其余事件）。
// 变更集语义逐字段执行：字段出现（含显式空串）= 覆盖；缺失 = 不变；空串 = 清除
// （折叠态记 Written=true/Value=""，生效上回 hub 默认）。
func (t *Table) Apply(ev *contracts.Event) {
	if t == nil || ev == nil || !ev.Durable || ev.SessionID == "" {
		return
	}
	if ev.Kind != KindConfigChanged {
		return // 只吃 config.changed（D7：其余 kind 忽略）
	}
	var p contracts.ConfigChangedPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return // 坏载荷跳过专属投影（D7 宽容，照 discovery 的口径）
	}
	seq := int64(0)
	if ev.Seq != nil {
		seq = *ev.Seq
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.sessions == nil { // 零值表懒建（照 discovery.BeginRun 的口径）
		t.sessions = make(map[contracts.Ulid]*sessionState)
	}
	st := t.sessions[ev.SessionID]
	if st == nil {
		st = &sessionState{seen: make(map[contracts.Ulid]struct{})}
		t.sessions[ev.SessionID] = st
	}
	if st.seen == nil {
		st.seen = make(map[contracts.Ulid]struct{})
	}
	if ev.ID != "" {
		st.seen[ev.ID] = struct{}{} // Generation 计数：按事件 id 去重（重复投递不重数）
	}

	// 逐字段 LWW：缺失的字段不碰（变更集语义），出现的字段按 (seq, ts) 定序覆盖。
	if p.Model != nil {
		st.model.apply(*p.Model, seq, ev.TS)
	}
	if p.ReasoningEffort != nil {
		st.effort.apply(*p.ReasoningEffort, seq, ev.TS)
	}
	if p.Provider != nil {
		st.providerSeen = true // provider 对象出现过（含"两字段都空串 = 清除"的记录）
		if p.Provider.BaseURL != nil {
			st.baseURL.apply(*p.Provider.BaseURL, seq, ev.TS)
		}
		if p.Provider.APIKeyEnv != nil {
			st.keyEnv.apply(*p.Provider.APIKeyEnv, seq, ev.TS)
		}
	}
	// tools 组（G-e）：**整组 LWW**——组出现 = 整组被本次提交覆盖（同一次提交的
	// only/except 一起落，后写覆盖前一组，不拆子字段跨事件合并）；组缺失 = 不变。
	// 组内的子项三态（出现=覆盖、空数组=清空该项、缺失=该项未设置）由 listFieldOf 折。
	if p.Tools != nil {
		if !st.tools.set || newerStamp(seq, ev.TS, st.tools.seq, st.tools.ts) {
			st.tools = toolsState{
				set:    true,
				seq:    seq,
				ts:     ev.TS,
				only:   listFieldOf(p.Tools.Only),
				except: listFieldOf(p.Tools.Except),
			}
		}
	}
	// context 面（G-h）：**子字段各自 LWW**（与 provider 同口径，**不学 tools 的整组
	// LWW**）——每个子项一个戳：后写的子项只覆盖自己那一项；没带的子项**沿用上一次
	// 的值**（不像 tools 那样被整组覆盖打回"未设置"）。
	//
	// 子项三态：kind 出现 = 覆盖（空串 = 清除回默认）；数值子项出现 = 覆盖（**0 = 清除
	// 回默认**）；缺失 = 不变。
	if p.Context != nil {
		st.context.seen = true // context 对象出现过（含空对象；"出现过"是事实）
		if p.Context.Kind != nil {
			st.context.kind.apply(*p.Context.Kind, seq, ev.TS)
		}
		if p.Context.Window != nil {
			st.context.window.apply(*p.Context.Window, seq, ev.TS)
		}
		if p.Context.L1Pct != nil {
			st.context.l1.apply(*p.Context.L1Pct, seq, ev.TS)
		}
		if p.Context.L2Pct != nil {
			st.context.l2.apply(*p.Context.L2Pct, seq, ev.TS)
		}
		if p.Context.RulesBudget != nil {
			st.context.rules.apply(*p.Context.RulesBudget, seq, ev.TS)
		}
		if p.Context.CommonSenseBudget != nil {
			st.context.cs.apply(*p.Context.CommonSenseBudget, seq, ev.TS)
		}
	}
	// exec 面（L2 per-session 执行上下文 · 件 2）：**子字段各自 LWW**（与 provider 同口径）。
	// cwd / worktree 各带一个戳：后写的子项只覆盖自己那一项；没带的子项**沿用上一次
	// 的值**（整组 LWW 会把"只改 cwd"变成"顺带解绑 worktree"——那不是我们要的语义）。
	//
	// 子项三态：出现 = 覆盖；**空串 = 清除该项**（不锚定 / 不绑 worktree）；缺失 = 不变。
	if p.Exec != nil {
		st.exec.seen = true // exec 对象出现过（含空对象；"出现过"是事实）
		if p.Exec.Cwd != nil {
			st.exec.cwd.apply(*p.Exec.Cwd, seq, ev.TS)
		}
		if p.Exec.Worktree != nil {
			st.exec.worktree.apply(*p.Exec.Worktree, seq, ev.TS)
		}
	}
}

// Current 返回某 session 的配置 fold 结果（值类型副本；未知 session → 零值 =
// 全部"未覆盖" → 调用方全叠 hub 默认）。
func (t *Table) Current(sessionID contracts.Ulid) SessionConfig {
	if t == nil || sessionID == "" {
		return SessionConfig{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	st, ok := t.sessions[sessionID]
	if !ok {
		return SessionConfig{}
	}
	return SessionConfig{
		Model:           stringFieldOf(&st.model),
		ReasoningEffort: stringFieldOf(&st.effort),
		Provider: ProviderState{
			Touched:   st.providerSeen,
			BaseURL:   stringFieldOf(&st.baseURL),
			APIKeyEnv: stringFieldOf(&st.keyEnv),
		},
		Tools: ToolsState{
			Touched: st.tools.set,
			Only:    st.tools.only.copy(),
			Except:  st.tools.except.copy(),
		},
		Context: ContextState{
			Touched:           st.context.seen,
			Kind:              stringFieldOf(&st.context.kind),
			Window:            numFieldOf(&st.context.window),
			L1Pct:             floatFieldOf(&st.context.l1),
			L2Pct:             floatFieldOf(&st.context.l2),
			RulesBudget:       numFieldOf(&st.context.rules),
			CommonSenseBudget: numFieldOf(&st.context.cs),
		},
		Exec: ExecState{
			Touched:  st.exec.seen,
			Cwd:      stringFieldOf(&st.exec.cwd),
			Worktree: stringFieldOf(&st.exec.worktree),
		},
	}
}

// Generation 返回该 session 已折叠的 config.changed 事件数（按事件 id 去重）。
//
// 宿主用它把"第几届配置变更"编进**确定性事件 id**（照 discovery.JoinGeneration +
// newGateJoinedEvent 的届数样板）：后续变更是新的一届 → 新 id——于是 A→B→A 这种
// "回到旧值"的再次提交不会被误当成重试吃掉（纯 payload 寻址做不到这一点）。
// **幂等边界（F2）**：客户端在首次已落库**之后**重试同一 payload 时届数已前进 →
// 新 id → 落成第二条等价事件（fold 结果不变）；去重只覆盖**同届同 payload** 的
// 并发/存储层重试（详见 host.recordConfigChanged 的 id 口径）。
//
// 计数只由事件折叠推进（Apply 幂等）→ 跨重启稳定：重启全量重放得到同一个届数。
func (t *Table) Generation(sessionID contracts.Ulid) int {
	if t == nil || sessionID == "" {
		return 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	st, ok := t.sessions[sessionID]
	if !ok {
		return 0
	}
	return len(st.seen)
}

// Reset 清空全部状态（重建入口：宿主随后从 seq 0 重放事件流）。
func (t *Table) Reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sessions = make(map[contracts.Ulid]*sessionState)
}
