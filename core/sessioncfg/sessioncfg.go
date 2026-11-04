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

// SessionConfig 是一个 session 的配置 fold 结果（**值类型**：Current 返回副本，
// 调用方读它不持有表锁）。生效值 = 各字段 HasValue() 的 Value，否则由调用方叠
// hub 默认（生效链第一层在 host/loop 装配里）。
type SessionConfig struct {
	Model           Field
	ReasoningEffort Field
	Provider        ProviderState
}

// fieldState 是表内单字段的折叠态（值 + LWW 戳）。
type fieldState struct {
	set   bool
	value string
	seq   int64
	ts    string
}

// apply 把一条变更集里的字段值折进来（后写覆盖先写；旧事件/重复投递不动）。
func (fs *fieldState) apply(value string, seq int64, ts string) {
	if fs.set && !newerStamp(seq, ts, fs.seq, fs.ts) {
		return // 旧事件（或同一条事件的重复投递）：折叠态不变（幂等 + 乱序容忍）
	}
	fs.set, fs.value, fs.seq, fs.ts = true, value, seq, ts
}

// field 折出对外的三态 Field（调用方持锁）。
func (fs *fieldState) field() Field {
	return Field{Written: fs.set, Value: fs.value}
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
	model   fieldState
	effort  fieldState
	baseURL fieldState
	keyEnv  fieldState
	// providerSeen 表示 provider 对象出现过（含清除记录；只增不减——"出现过"是事实）。
	providerSeen bool
	// seen 是已折叠的 config.changed 事件 id 集合（Generation 的计数源：按事件 id
	// 去重 → 幂等重放不重数、乱序投递不漏数）。id 为空的事件不计（纵深防御：拿不到
	// 稳定判别位就不拿它推进"届数"，避免所有无 id 事件挤成一个计数）。
	seen map[contracts.Ulid]struct{}
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
		Model:           st.model.field(),
		ReasoningEffort: st.effort.field(),
		Provider: ProviderState{
			Touched:   st.providerSeen,
			BaseURL:   st.baseURL.field(),
			APIKeyEnv: st.keyEnv.field(),
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
