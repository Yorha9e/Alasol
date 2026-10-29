package discovery

import (
	"encoding/json"
	"sort"
	"sync"

	"github.com/Yorha9e/alasol/contracts"
)

// 本表折叠的事件 kind（D7：其余 kind 忽略但已推进"最近活动"——它们仍然是
// "这个 agent 还活着"的证据）。常量值与契约 event.json 的 x-kind-payload 对齐。
const (
	KindGateJoined     = "gate.joined"
	KindAgentSpawned   = "agent.spawned"
	KindAgentCompleted = "agent.completed"
	KindAgentFailed    = "agent.failed"
)

// agent 状态（与 core/agents.Status 同值，此处不 import core/agents：发现表折的是
// **事件**，不是 agent 树的实现；两边各自对同一组事件做投影，互不依赖）。
const (
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

// terminal 报告状态是否终态（不可再变迁，与 core/agents 的口径一致）。
func terminal(status string) bool { return status == StatusCompleted || status == StatusFailed }

// agentState 是表内单个 agent 的折叠态。
type agentState struct {
	id      contracts.Ulid
	session contracts.Ulid
	// parent 为空 = 该 session 的主 agent（root，depth=1）。
	parent  contracts.Ulid
	typ     string
	role    string
	depth   int
	status  string
	lastSeq int64
}

// gateJoin 是一次 join 的折叠结果（LWW：同一 (session, gate) 只留最近一次）。
type gateJoin struct {
	gateID  string
	source  string
	seq     int64
	ts      string
	eventID contracts.Ulid
}

// sessionState 是表内单个 session 的折叠态。
type sessionState struct {
	id       contracts.Ulid
	agents   map[contracts.Ulid]*agentState
	order    []contracts.Ulid        // agent 首次见到的顺序（推主 agent 用）
	isChild  map[contracts.Ulid]bool // 作为 agent.spawned 的 child 出现过 → 不是主 agent
	joins    map[string]gateJoin     // gate_id → 最近一次 join（活跃成员关系的折叠）
	firstSeq int64
	lastSeq  int64
	lastKind string
	lastTS   string
	// live 是**进程本地**活状态（G17b）：本进程此刻持有该 session 的 loop。
	// 不进 Snapshot——它不是事件能重建的东西。
	live bool
}

// mainAgent 推主 agent：**不作为任何 agent.spawned 的 child 出现**的最早 agent。
//
// 口径与 core/durable.PrimaryAgentID 一致（同一套事件、同一个结论），这样 hub 的
// loop 归属与发现表折出来的主 agent 不会分歧。异常流（全是 child）退到最早事件的 agent。
func (st *sessionState) mainAgent() contracts.Ulid {
	for _, id := range st.order {
		if !st.isChild[id] {
			return id
		}
	}
	if len(st.order) > 0 {
		return st.order[0]
	}
	return ""
}

// Table 是发现表（K10）：core 内存活状态组件。并发安全（一把 RWMutex）。
//
// 零值可用，但请用 New。**不做 IO**：事件由宿主喂进来（Apply），宿主负责扫描/重放
// （见 host/discovery.go）。
type Table struct {
	mu       sync.RWMutex
	sessions map[contracts.Ulid]*sessionState
	// agents 是全局索引（agent_id → agent），供 K18 Discover 与 Inquiry 预备；
	// 权威存储在各自 sessionState 里（同一指针）。
	agents map[contracts.Ulid]*agentState
	// gates 是 gate_id → join 过它的 session 集合（**历史**成员，含已结束的 session；
	// 活跃成员再叠 live 过滤，见 GateMembers）。
	gates map[string]map[contracts.Ulid]struct{}
}

// New 建一张空的发现表。
func New() *Table {
	return &Table{
		sessions: make(map[contracts.Ulid]*sessionState),
		agents:   make(map[contracts.Ulid]*agentState),
		gates:    make(map[string]map[contracts.Ulid]struct{}),
	}
}

// ---------------------------------------------------------------------------
// 写入：宿主喂事件 + 进程本地活状态
// ---------------------------------------------------------------------------

// Apply 把一条事件并进发现表（幂等、可重复、可乱序）。
//
// 只认 durable=true 的事实事件（F8：durable=false 的流事件不入库、不占 seq、不是事实，
// 回放对象只有事实事件）。未知 kind 宽容忽略（D7），但仍推进该 session/agent 的
// "最近活动"——一条不知道是什么的事件同样证明"它还活着"。
//
// 载荷不可解析的已知 kind **跳过该 kind 的专属投影**（不中断、不影响其余事件），
// 与 runtime/indexer 的坏载荷隔离同口径（D7 宽容哲学）。
//
// # hub.* 记录：无主、无 gate，**不参与可见性**
//
// `hub.*`（如 hub.alert，v2-S5 清账②）是 **hub 级**事件：envelope 里的 session_id /
// agent_id 是宿主按库路径确定性派生的**伪 id**——它不对应任何真实 agent session。
// 这里照样给它建一条记录（"最近活动"是事实：hub 确实在那一刻动过），但它：
//   - **没有 gate**（不 join 任何 gate）→ 不进 gate 成员表、不进 /api/peers 的可见面；
//   - **不是主 agent**（没有 agent.spawned / 主 agent 归属）→ 不参与 K11 可见性判定
//     （VisiblePeers / selfVisibleAgents 都按 gate 与 agent 树算，它两边都不在）；
//   - **不被 Activate**（宿主只激活真 session）→ 不进 presence（活状态）。
//
// 换句话说：发现表里那条记录只是"时间线痕迹"，**不是**一个可被查询/交流的会话。
func (t *Table) Apply(ev *contracts.Event) {
	if t == nil || ev == nil || !ev.Durable {
		return
	}
	if ev.SessionID == "" {
		return // 契约要求 session_id 必填；这里是纵深防御，不归属的事件不进表
	}
	seq := int64(0)
	if ev.Seq != nil {
		seq = *ev.Seq
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	st := t.sessionLocked(ev.SessionID)
	touchSession(st, ev, seq)
	if ev.AgentID != "" {
		touchAgent(t.agentLocked(st, ev.AgentID), seq)
	}

	switch ev.Kind {
	case KindAgentSpawned:
		t.applySpawnedLocked(st, ev, seq)
	case KindAgentCompleted:
		t.applyTerminalLocked(st, ev, StatusCompleted, seq)
	case KindAgentFailed:
		t.applyTerminalLocked(st, ev, StatusFailed, seq)
	case KindGateJoined:
		t.applyGateJoinedLocked(st, ev, seq)
	}
}

// Activate 标记 session 在本进程存活（宿主建成/复用它的 loop 时调）。
//
// 这是**进程本地**状态：崩溃重启后不会自己回来（它的宿主进程已经不在了），要由宿主
// 重新激活（resume：自动 gate 重算 + gate.joined 历史重放，K12）。
func (t *Table) Activate(sessionID contracts.Ulid) {
	if t == nil || sessionID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sessionLocked(sessionID).live = true
}

// Deactivate 标记 session 结束（成员关系随即失效，G17b：session 结束即失效）。
//
// **不删除任何折叠态**：join 历史要留着供审计与重建，只是不再算活跃成员。
func (t *Table) Deactivate(sessionID contracts.Ulid) {
	if t == nil || sessionID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if st, ok := t.sessions[sessionID]; ok {
		st.live = false
	}
}

// Reset 清空全部状态（重建入口：宿主随后从 seq 0 重放事件流）。
func (t *Table) Reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sessions = make(map[contracts.Ulid]*sessionState)
	t.agents = make(map[contracts.Ulid]*agentState)
	t.gates = make(map[string]map[contracts.Ulid]struct{})
}

// ---------------------------------------------------------------------------
// 折叠内部实现（调用方持有写锁）
// ---------------------------------------------------------------------------

func (t *Table) sessionLocked(id contracts.Ulid) *sessionState {
	if st, ok := t.sessions[id]; ok {
		return st
	}
	st := &sessionState{
		id:      id,
		agents:  make(map[contracts.Ulid]*agentState),
		isChild: make(map[contracts.Ulid]bool),
		joins:   make(map[string]gateJoin),
	}
	t.sessions[id] = st
	return st
}

func (t *Table) agentLocked(st *sessionState, id contracts.Ulid) *agentState {
	if ag, ok := st.agents[id]; ok {
		return ag
	}
	ag := &agentState{id: id, session: st.id, status: StatusRunning, depth: 1}
	st.agents[id] = ag
	st.order = append(st.order, id)
	t.agents[id] = ag
	return ag
}

// touchSession 更新 session 的"最近活动"（seq 守卫 → 乱序/重复 Apply 不倒退）。
func touchSession(st *sessionState, ev *contracts.Event, seq int64) {
	if st.firstSeq == 0 || (seq > 0 && seq < st.firstSeq) {
		st.firstSeq = seq
	}
	if seq >= st.lastSeq { // >= 而非 >：重复 Apply 同一条事件结果一致（幂等）
		st.lastSeq = seq
		st.lastKind = ev.Kind
		if ev.TS != "" {
			st.lastTS = ev.TS
		}
	}
}

// touchAgent 更新 agent 的"最近活动"（同 seq 守卫）。
func touchAgent(ag *agentState, seq int64) {
	if seq >= ag.lastSeq {
		ag.lastSeq = seq
	}
}

func (t *Table) applySpawnedLocked(st *sessionState, ev *contracts.Event, seq int64) {
	var p contracts.AgentSpawnedPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return // 宽容（D7）：坏载荷跳过专属投影
	}
	childID := p.ChildAgentID
	if childID == "" {
		childID = ev.AgentID // 载荷缺 child_id 时退到 envelope 的归属 agent
	}
	if childID == "" {
		return
	}
	child := t.agentLocked(st, childID)
	// spawn 是起点信息：只在"不比已知信息旧"时覆盖（乱序重放不倒退）。
	if seq >= child.lastSeq || child.typ == "" {
		if p.ParentAgentID != "" {
			child.parent = p.ParentAgentID
		}
		child.typ = p.AgentType
		child.role = p.Role
		if p.Depth > 0 {
			child.depth = int(p.Depth)
		}
		if !terminal(child.status) {
			child.status = StatusRunning
		}
	}
	st.isChild[childID] = true
	if p.ParentAgentID != "" {
		// 父也必须进表（否则主 agent 推不出来，树投影缺一环）。
		t.agentLocked(st, p.ParentAgentID)
	}
}

// applyTerminalLocked 折 agent.completed / agent.failed：终态不可逆（F6：历史不重写）。
//
// 目标 agent 优先取载荷里的 agent_id（契约把它显式带着），缺省才退到 envelope 的归属。
func (t *Table) applyTerminalLocked(st *sessionState, ev *contracts.Event, status string, seq int64) {
	target := ev.AgentID
	switch status {
	case StatusCompleted:
		var p contracts.AgentCompletedPayload
		if json.Unmarshal(ev.Payload, &p) == nil && p.AgentID != "" {
			target = p.AgentID
		}
	case StatusFailed:
		var p contracts.AgentFailedPayload
		if json.Unmarshal(ev.Payload, &p) == nil && p.AgentID != "" {
			target = p.AgentID
		}
	}
	if target == "" {
		return
	}
	if existing, ok := t.agents[target]; ok && existing.session != st.id {
		return // 载荷指向别 session 的 agent：不并进本 session（纵深防御）
	}
	ag := t.agentLocked(st, target)
	if terminal(ag.status) {
		return // 终态不可逆
	}
	ag.status = status
	touchAgent(ag, seq)
}

func (t *Table) applyGateJoinedLocked(st *sessionState, ev *contracts.Event, seq int64) {
	var p contracts.GateJoinedPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return // 宽容（D7）
	}
	gateID, ok := NormalizeGateID(p.GateID)
	if !ok {
		return // 坏 gate_id：跳过（S9 在写入侧就拒了，这里是纵深防御）
	}
	if cur, exists := st.joins[gateID]; exists && cur.seq > seq {
		return // 旧事件盖不掉新 join（LWW + seq 守卫）
	}
	st.joins[gateID] = gateJoin{gateID: gateID, source: string(p.Source), seq: seq, ts: ev.TS, eventID: ev.ID}
	members := t.gates[gateID]
	if members == nil {
		members = make(map[contracts.Ulid]struct{})
		t.gates[gateID] = members
	}
	members[st.id] = struct{}{}
}

// ---------------------------------------------------------------------------
// 查询：行类型
// ---------------------------------------------------------------------------

// SessionRow 是发现表里一个 session 的行（K10：谁在线 / 在干什么 / 属于哪些 gate）。
//
// **没有 live 字段**：存活是进程本地状态（G17b），要它就用 LiveSessions() / IsLive()。
// 少一个字段，Snapshot 才能被直接比较（重建测试据此断言"重放后与崩溃前一致"）。
type SessionRow struct {
	SessionID   string `json:"session_id"`
	MainAgentID string `json:"main_agent_id"`
	// Gates 是该 session 的 gate 成员关系（join 历史折叠，字典序）。
	Gates []string `json:"gates"`
	// AgentCount 是该 session 的 agent 数（含主 agent 与全部 subagent）。
	AgentCount int `json:"agent_count"`
	// LastKind / LastSeq / UpdatedAt 是"在干什么"：最后一条事件的 kind / seq / ts。
	LastKind  string `json:"last_kind"`
	LastSeq   int64  `json:"last_seq"`
	UpdatedAt string `json:"updated_at"`
}

// AgentRow 是发现表里一个 agent 的行（K10 + K11 的树投影）。
type AgentRow struct {
	AgentID   string `json:"agent_id"`
	SessionID string `json:"session_id"`
	// ParentID 为空 = 该 session 的主 agent（root）。
	ParentID string `json:"parent_id"`
	Type     string `json:"type"`
	Role     string `json:"role"`
	Status   string `json:"status"`
	Depth    int    `json:"depth"`
	// Main 标记是否该 session 的主 agent（K11：只有主 agent 参与水平可见性）。
	Main bool `json:"main"`
	// LastKind 是"在干什么"：最后一条归属该 agent 的事件的 kind。
	LastKind  string `json:"last_kind"`
	UpdatedAt string `json:"updated_at"`
}

// GateRow 是一个 gate 的行（K12：gate 定义持久化、成员关系是活的）。
type GateRow struct {
	GateID string `json:"gate_id"`
	// Members 是 join 过该 gate 的 session（**历史**成员，含已结束的 session，
	// 字典序）。活跃成员 = 这个集合 ∩ 当前存活 session（GateMembers）。
	Members []string `json:"members"`
}

// GateJoin 是一次 join 事实（审计/重建用；同一 (session, gate) 只留最近一次）。
type GateJoin struct {
	GateID    string `json:"gate_id"`
	SessionID string `json:"session_id"`
	Source    string `json:"source"`
	Seq       int64  `json:"seq"`
	TS        string `json:"ts"`
	EventID   string `json:"event_id"`
}

// PeerRow 是水平可见性的一行（K11①）：对端 session 的**主 agent**。
//
// 别 session 的 subagent **完全不在这里出现**——水平可见性的粒度是 session 的主 agent，
// 不是 agent。
type PeerRow struct {
	SessionID   string `json:"session_id"`
	MainAgentID string `json:"main_agent_id"`
	// Gates 是与查询方**相交**的 gate（字典序；非该对端的全部 gate）。
	Gates []string `json:"gates"`
	// LastKind / LastSeq / UpdatedAt 是对端"在干什么"。
	LastKind  string `json:"last_kind"`
	LastSeq   int64  `json:"last_seq"`
	UpdatedAt string `json:"updated_at"`
}

// Snapshot 是发现表的**可重建快照**：只含"事件折叠得出"的部分（F5/G17b）。
//
// 故意不含 live（进程本地，重启后老成员全部失活——那是特性不是缺陷）。重建测试据此
// 断言"新实例全量重放后的状态与崩溃前一致"。
type Snapshot struct {
	Sessions []SessionRow `json:"sessions"`
	Agents   []AgentRow   `json:"agents"`
	Gates    []GateRow    `json:"gates"`
	Joins    []GateJoin   `json:"joins"`
}

// ---------------------------------------------------------------------------
// 查询实现
// ---------------------------------------------------------------------------

// Sessions 返回全部已知 session（含已结束的——发现表的历史视角）。
//
// 排序与 host 在线表一致：最后活跃倒序，同 seq 按 session_id 升序（确定性，UI 不抖）。
func (t *Table) Sessions() []SessionRow {
	if t == nil {
		return []SessionRow{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.sessionRowsLocked(func(*sessionState) bool { return true })
}

// LiveSessions 返回**本进程存活**的 session（K10 "谁在线"；G17b：无租约无心跳）。
func (t *Table) LiveSessions() []SessionRow {
	if t == nil {
		return []SessionRow{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.sessionRowsLocked(func(st *sessionState) bool { return st.live })
}

func (t *Table) sessionRowsLocked(keep func(*sessionState) bool) []SessionRow {
	out := make([]SessionRow, 0, len(t.sessions))
	for _, st := range t.sessions {
		if !keep(st) {
			continue
		}
		out = append(out, sessionRowLocked(st))
	}
	sortSessionRows(out)
	return out
}

func sessionRowLocked(st *sessionState) SessionRow {
	return SessionRow{
		SessionID:   string(st.id),
		MainAgentID: string(st.mainAgent()),
		Gates:       sortedGateIDs(st),
		AgentCount:  len(st.agents),
		LastKind:    st.lastKind,
		LastSeq:     st.lastSeq,
		UpdatedAt:   st.lastTS,
	}
}

func sortedGateIDs(st *sessionState) []string {
	out := make([]string, 0, len(st.joins))
	for gateID := range st.joins {
		out = append(out, gateID)
	}
	sort.Strings(out)
	return out
}

func sortSessionRows(rows []SessionRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].LastSeq != rows[j].LastSeq {
			return rows[i].LastSeq > rows[j].LastSeq
		}
		return rows[i].SessionID < rows[j].SessionID
	})
}

// Session 返回单个 session 的行（不存在 → ok=false）。
func (t *Table) Session(sessionID contracts.Ulid) (SessionRow, bool) {
	if t == nil {
		return SessionRow{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	st, ok := t.sessions[sessionID]
	if !ok {
		return SessionRow{}, false
	}
	return sessionRowLocked(st), true
}

// IsLive 报告 session 是否由本进程持有（G17b）。
func (t *Table) IsLive(sessionID contracts.Ulid) bool {
	if t == nil {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	st, ok := t.sessions[sessionID]
	return ok && st.live
}

// Agents 返回某 session 的 agent 行（按首次见到顺序 = 树的自然顺序）。
//
// **宿主内部视图**，不是跨 session 可见性面：K11 的水平可见性只经 VisiblePeers 暴露
// （对端只能看到主 agent），垂直可见性（父↔直系子）由 core/agents 管。别 session 的
// subagent 不得经任何对外面泄漏——所以宿主不该把本方法的结果直接挂到 HTTP 上。
func (t *Table) Agents(sessionID contracts.Ulid) []AgentRow {
	if t == nil {
		return []AgentRow{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	st, ok := t.sessions[sessionID]
	if !ok {
		return []AgentRow{}
	}
	return agentRowsLocked(st)
}

func agentRowsLocked(st *sessionState) []AgentRow {
	main := st.mainAgent()
	out := make([]AgentRow, 0, len(st.order))
	for _, id := range st.order {
		ag := st.agents[id]
		parent := ""
		if ag.parent != "" {
			parent = string(ag.parent)
		}
		out = append(out, AgentRow{
			AgentID:   string(ag.id),
			SessionID: string(st.id),
			ParentID:  parent,
			Type:      ag.typ,
			Role:      ag.role,
			Status:    ag.status,
			Depth:     ag.depth,
			Main:      ag.id == main,
			LastKind:  st.lastKind,
			UpdatedAt: st.lastTS,
		})
	}
	return out
}

// Agent 按 agent_id 全局查一个 agent（K18 Discover 的底层；跨 session 也能查到，
// 所以它是宿主内部 API，见 Agents 的警告）。
func (t *Table) Agent(agentID contracts.Ulid) (AgentRow, bool) {
	if t == nil {
		return AgentRow{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	ag, ok := t.agents[agentID]
	if !ok {
		return AgentRow{}, false
	}
	st, ok := t.sessions[ag.session]
	if !ok {
		return AgentRow{}, false
	}
	main := st.mainAgent()
	parent := ""
	if ag.parent != "" {
		parent = string(ag.parent)
	}
	return AgentRow{
		AgentID:   string(ag.id),
		SessionID: string(st.id),
		ParentID:  parent,
		Type:      ag.typ,
		Role:      ag.role,
		Status:    ag.status,
		Depth:     ag.depth,
		Main:      ag.id == main,
		LastKind:  st.lastKind,
		UpdatedAt: st.lastTS,
	}, true
}

// GatesOf 返回某 session 的 gate 成员关系（join 历史折叠，字典序）。
// PrimaryAgent 返回该 session 的主 agent id（口径同 core/durable.PrimaryAgentID：
// 不作为任何 agent.spawned 的 child 出现的最早 agent）。
//
// 宿主用它给**续跑的 session 复用原来的主 agent id**——否则 resume 会造出一个一次性
// 随机 id 的"幽灵 agent"（cmd/alasol resume 早已按同一口径取 id，hub 这里对齐）。
func (t *Table) PrimaryAgent(sessionID contracts.Ulid) (contracts.Ulid, bool) {
	if t == nil {
		return "", false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	st, ok := t.sessions[sessionID]
	if !ok {
		return "", false
	}
	main := st.mainAgent()
	if main == "" {
		return "", false
	}
	return main, true
}

// GatesOf 返回某 session 的 gate 成员关系（join 历史折叠，字典序）。
func (t *Table) GatesOf(sessionID contracts.Ulid) []string {
	if t == nil {
		return []string{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	st, ok := t.sessions[sessionID]
	if !ok {
		return []string{}
	}
	return sortedGateIDs(st)
}

// InGate 报告 session 是否（曾）加入该 gate——宿主用它做显式 join 的幂等去重
// （已在网内的重复 join 不再落一条重复事件）。
func (t *Table) InGate(sessionID contracts.Ulid, gateID string) bool {
	if t == nil {
		return false
	}
	id, ok := NormalizeGateID(gateID)
	if !ok {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	st, ok := t.sessions[sessionID]
	if !ok {
		return false
	}
	_, joined := st.joins[id]
	return joined
}

// Gates 返回全部 gate（字典序）。
func (t *Table) Gates() []GateRow {
	if t == nil {
		return []GateRow{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.gatesLocked()
}

// gatesLocked 是 Gates 的**锁内版**（调用方必须已持读锁）。
//
// 为什么要拆出锁内版：持锁时再调公开的取锁方法 = 递归 RLock——Go 的 RWMutex
// 写者优先，一旦有写者在等锁，递归的 RLock 会永久阻塞（读者等写者、写者等读者 → 死锁）。
// 本包的规矩：**持锁时只调 *Locked 版本**。
func (t *Table) gatesLocked() []GateRow {
	out := make([]GateRow, 0, len(t.gates))
	for gateID, members := range t.gates {
		ids := make([]string, 0, len(members))
		for sid := range members {
			ids = append(ids, string(sid))
		}
		sort.Strings(ids)
		out = append(out, GateRow{GateID: gateID, Members: ids})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].GateID < out[j].GateID })
	return out
}

// GateMembers 返回某 gate 的**活跃成员**（K12：成员关系是活的）。
//
// 活跃成员 = 事件折叠出的 join 历史 ∩ 当前存活 session。已结束的 session 仍在
// Gates() 的历史成员里（可审计），但不出现在这里。
func (t *Table) GateMembers(gateID string) []SessionRow {
	if t == nil {
		return []SessionRow{}
	}
	id, ok := NormalizeGateID(gateID)
	if !ok {
		return []SessionRow{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	members, ok := t.gates[id]
	if !ok {
		return []SessionRow{}
	}
	out := make([]SessionRow, 0, len(members))
	for sid := range members {
		if st, ok := t.sessions[sid]; ok && st.live {
			out = append(out, sessionRowLocked(st))
		}
	}
	sortSessionRows(out)
	return out
}

// JoinHistory 返回全部 join 事实（按 seq 升序；审计用）。
func (t *Table) JoinHistory() []GateJoin {
	if t == nil {
		return []GateJoin{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.joinHistoryLocked()
}

// joinHistoryLocked 是 JoinHistory 的**锁内版**（调用方必须已持读锁；见 gatesLocked 的理由）。
func (t *Table) joinHistoryLocked() []GateJoin {
	out := make([]GateJoin, 0, len(t.sessions))
	for _, st := range t.sessions {
		for _, j := range st.joins {
			out = append(out, GateJoin{
				GateID:    j.gateID,
				SessionID: string(st.id),
				Source:    j.source,
				Seq:       j.seq,
				TS:        j.ts,
				EventID:   string(j.eventID),
			})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Seq != out[j].Seq {
			return out[i].Seq < out[j].Seq
		}
		return out[i].SessionID < out[j].SessionID
	})
	return out
}

// VisiblePeers 返回 sessionID 的**水平可见对端**（K11①）。
//
// 规则：只有 session 的**主 agent**之间互相可见，条件 gates(A) ∩ gates(B) ≠ ∅；
// 对端必须是**存活** session（成员关系是活的，G17b）。别 session 的 subagent 完全不可见
// （粒度是 session 的主 agent，不是 agent）。
//
// 查询方自己不在结果里；未知/无事件的 session → 空。
func (t *Table) VisiblePeers(sessionID contracts.Ulid) []PeerRow {
	if t == nil {
		return []PeerRow{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.visiblePeersLocked(sessionID)
}

func (t *Table) visiblePeersLocked(sessionID contracts.Ulid) []PeerRow {
	me, ok := t.sessions[sessionID]
	if !ok {
		return []PeerRow{}
	}
	mine := me.joins
	out := make([]PeerRow, 0, len(t.sessions))
	for _, other := range t.sessions {
		if other.id == me.id || !other.live {
			continue
		}
		shared := sharedGates(mine, other.joins)
		if len(shared) == 0 {
			continue
		}
		main := other.mainAgent()
		if main == "" {
			continue // 还没有任何事件的对端无从互见（连主 agent 都推不出来）
		}
		out = append(out, PeerRow{
			SessionID:   string(other.id),
			MainAgentID: string(main),
			Gates:       shared,
			LastKind:    other.lastKind,
			LastSeq:     other.lastSeq,
			UpdatedAt:   other.lastTS,
		})
	}
	sortPeerRows(out)
	return out
}

// sharedGates 求两个 gate 集合的交集（字典序）。
func sharedGates(a, b map[string]gateJoin) []string {
	out := make([]string, 0, len(a))
	for gateID := range a {
		if _, ok := b[gateID]; ok {
			out = append(out, gateID)
		}
	}
	sort.Strings(out)
	return out
}

func sortPeerRows(rows []PeerRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].LastSeq != rows[j].LastSeq {
			return rows[i].LastSeq > rows[j].LastSeq
		}
		return rows[i].SessionID < rows[j].SessionID
	})
}

// Snapshot 取可重建快照（见类型注释：不含 live）。
//
// 快照是**原子**的：整张表在同一把读锁下取样（读者看到的要么全是旧、要么全是新）。
// 故这里只调**锁内版**（gatesLocked / joinHistoryLocked）——持锁时再调公开的取锁
// 方法 = 递归 RLock，写者一排队就死锁（RWMutex 写者优先）。
func (t *Table) Snapshot() Snapshot {
	if t == nil {
		return Snapshot{Sessions: []SessionRow{}, Agents: []AgentRow{}, Gates: []GateRow{}, Joins: []GateJoin{}}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	snap := Snapshot{
		Sessions: t.sessionRowsLocked(func(*sessionState) bool { return true }),
		Agents:   make([]AgentRow, 0, len(t.agents)),
		Gates:    t.gatesLocked(),
		Joins:    t.joinHistoryLocked(),
	}
	for _, st := range t.sessions {
		snap.Agents = append(snap.Agents, agentRowsLocked(st)...)
	}
	// agent 行按 session_id 升序聚合同一 session 的行（session 内保持首次见到顺序）：
	// map 遍历顺序不定，快照必须确定，否则两次取样不可比。
	sort.SliceStable(snap.Agents, func(i, j int) bool {
		if snap.Agents[i].SessionID != snap.Agents[j].SessionID {
			return snap.Agents[i].SessionID < snap.Agents[j].SessionID
		}
		return false // 同 session 内：稳定排序保住首次见到顺序
	})
	return snap
}
