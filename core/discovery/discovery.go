package discovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// 本表折叠的事件 kind（D7：其余 kind 忽略但已推进"最近活动"——它们仍然是
// "这个 agent 还活着"的证据）。常量值与契约 event.json 的 x-kind-payload 对齐。
const (
	KindGateJoined     = "gate.joined"
	KindGateLeft       = "gate.left"
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

// gateLeft 是一次 leave 的折叠结果（LWW：同一 (session, gate) 只留最近一次）。
//
// 与 gateJoin 分开存（而不是在 join 记录上打标记）：join 事实要**原样保留**供审计
// （JoinHistory 折的就是它），leave 是独立的成员关系终止事实——两者各记各的最大 seq，
// 活跃判据 = join.seq > left.seq（见 gateActiveLocked）。
type gateLeft struct {
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
	joins    map[string]gateJoin     // gate_id → 最近一次 join（join 事实，LWW；不含 leave 信息）
	lefts    map[string]gateLeft     // gate_id → 最近一次 leave（LWW；G15 显式离开）
	joinGen  map[string]int          // gate_id → 已折叠的 join 次数（"届数"；host 算确定性事件 id 用）
	firstSeq int64
	lastSeq  int64
	lastKind string
	lastTS   string
	// live 是**进程本地**活状态（G17b）：本进程此刻持有该 session 的 loop。
	// 不进 Snapshot——它不是事件能重建的东西。
	live bool
	// hostHub 是该 session 的**宿主 hub 标识**（G25 托管注册，2026-11-01 v4-7）：
	// session 创建/接管时由宿主经 Claim 登记（进程标识，如 hub 的 ULID）。与 live 同款
	// **进程本地**状态（不进 Snapshot——重启后失活，由宿主重新 Claim）。
	// 空 = 未登记（老 Activate 路径 / 尚未接管）。
	hostHub string
}

// gateActiveLocked 判定 (session, gate) 的成员关系是否**活跃**（G15 折叠口径：
// 成员 = joined 减 left）。判据 = 最近一次 join 的 seq 大于最近一次 leave 的 seq。
//
// 为什么各记各的最大 seq 再比大小（而不是"leave 到达就删 join"）：Apply 允许乱序/重复
// （表重建时事件不保证有序到达）——leave(7) 先于 join(5) 到达时，按 seq 比大小仍然得到
// "已离开"的正确结论；删记录的做法则会因到达顺序不同得到不同结果。
//
// **"是否 leave 过"用 lefts 的键存在性判**（不是 "leave.seq > 0" 哨兵，F5）：seq 是存储层
// 分配的全局序号，**导入 / 合成 / 尚未落库的事件可能没有 seq（0）**——拿 seq>0 当哨兵会把
// "leave 过但 seq=0"当成"从没 leave 过"→ 成员关系被静默复活（宿主据此判断"显式离开过"
// 时也会判错 → 自动 gate 把显式离开撤销）。
//
// 边界：join 与 leave 的 seq **相等**（都是 0，无法比大小）时按 **leave 赢**处理（保守：
// 显式离开不该被自动复活；要回来得显式再 join）。
func (st *sessionState) gateActiveLocked(gateID string) bool {
	j, ok := st.joins[gateID]
	if !ok {
		return false
	}
	if l, lefted := st.lefts[gateID]; lefted && j.seq <= l.seq {
		return false
	}
	return true
}

// activeGatesLocked 返回本 session 的活跃 gate 集合（joined 减 left；调用方持锁）。
func (st *sessionState) activeGatesLocked() map[string]bool {
	out := make(map[string]bool, len(st.joins))
	for gateID := range st.joins {
		if st.gateActiveLocked(gateID) {
			out[gateID] = true
		}
	}
	return out
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
	// gates 是 gate_id → **活跃成员** session 集合（G15 折叠口径：joined 减 left）。
	// 含"已结束但未显式离开"的 session——session 结束（进程没了）不等于 leave，
	// 结束只是不再算活跃成员（GateMembers 再叠 live 过滤）；显式 leave 才从本集合摘除。
	gates map[string]map[contracts.Ulid]struct{}
	// activityWindow 是活跃窗口（G-a 在线三态：活跃 = running ∨ 最后事件 ts ≤ 本窗口）。
	// **零值 = 用 core.DefaultActivityWindow**（零值即缺省；SetActivityWindow 只做显式覆盖）。
	// 进程本地配置，不进 Snapshot——行出口的 Online 随窗口配置走，重建比对要两边窗口一致。
	activityWindow time.Duration
	// running 是 session → "本轮在跑"计数（G-a：BeginRun/EndRun 配平）。与 live/hostHub
	// 同款**进程本地**状态，**不进 Snapshot**——它不是事件能重建的东西（重启后没人正在跑）。
	running map[contracts.Ulid]int
	// waiting 是 session → "挂起中"计数（G-f standby：BeginWait/EndWait 配平）。与 running
	// 同款**进程本地**状态，**不进 Snapshot**（挂起是当下事实，重启后没有 session 还在挂起，
	// 要由宿主重新 BeginWait）——live/hostHub/running/waiting 一个口径。
	waiting map[contracts.Ulid]int
}

// New 建一张空的发现表。
func New() *Table {
	return &Table{
		sessions: make(map[contracts.Ulid]*sessionState),
		agents:   make(map[contracts.Ulid]*agentState),
		gates:    make(map[string]map[contracts.Ulid]struct{}),
		running:  make(map[contracts.Ulid]int),
		waiting:  make(map[contracts.Ulid]int),
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
	case KindGateLeft:
		t.applyGateLeftLocked(st, ev, seq)
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
		st.hostHub = "" // 失管即清宿主登记（重启后由新宿主重新 Claim）
	}
}

// ErrHostConflict 表示 session 已由**另一个** hub 托管（G25 托管注册：拒绝接管，
// fail-close——双宿主会各自 append 同一 session 的事件，seq 变成两进程的提交序混排，
// 因果序假设被破坏）。
var ErrHostConflict = errors.New("discovery: session 已由另一个 hub 托管（G25：一个 session 的事件只由其宿主进程 append）")

// Claim 登记 session 由指定 hub 托管（G25 跨进程托管注册，2026-11-01 v4-7）。
//
// 语义 = Activate + 宿主归属：置 live=true 并记 hostHub=hubID（进程标识）。
// 幂等：同一 hub 重复 Claim 同一 session 无副作用（重启后同进程续跑、loop 重建都走它）。
// **冲突 fail-close**：该 session 已登记给**另一个** hub → 返回 ErrHostConflict 且
// **不改动**现有登记（已有宿主 → 拒绝接管，调用方给用户提示而不是静默双写）。
//
// hubID 必填：没有宿主标识的托管登记无意义（查"谁托管"会得到空串）。
// 与 live 同款**进程本地**：不进 Snapshot，崩溃重启后登记失活，由新宿主重新 Claim。
func (t *Table) Claim(sessionID contracts.Ulid, hubID string) error {
	if t == nil {
		return errors.New("discovery: Table 为 nil")
	}
	if sessionID == "" {
		return errors.New("discovery: Claim 需要非空 sessionID")
	}
	hubID = strings.TrimSpace(hubID)
	if hubID == "" {
		return errors.New("discovery: Claim 需要非空 hubID（宿主 hub 标识）")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.sessionLocked(sessionID)
	if st.hostHub != "" && st.hostHub != hubID {
		return fmt.Errorf("%w: session=%s host=%s（本次想登记 %s）", ErrHostConflict, sessionID, st.hostHub, hubID)
	}
	st.hostHub = hubID
	st.live = true
	return nil
}

// HostOf 返回该 session 登记的宿主 hub 标识（G25：其他 hub / 诊断面可查"谁托管"）。
// 未登记（或表 nil / session 未知）→ ok=false。
func (t *Table) HostOf(sessionID contracts.Ulid) (string, bool) {
	if t == nil || sessionID == "" {
		return "", false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	st, ok := t.sessions[sessionID]
	if !ok || st.hostHub == "" {
		return "", false
	}
	return st.hostHub, true
}

// SetActivityWindow 覆盖活跃窗口（G-a：测试与宿主按需调小/调大；缺省 15 分钟见
// core.DefaultActivityWindow）。d<=0 → 恢复缺省（"0 = 用默认"口径，与 loop 配置面一致）。
//
// **进程本地**配置：不进 Snapshot，重启回缺省——在线表四消费方（K14/K15 注入、K11 peers、
// agentlink presence、GET /api/sessions）用同一张表的同一窗口，不会各配各的。
func (t *Table) SetActivityWindow(d time.Duration) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if d <= 0 {
		d = core.DefaultActivityWindow
	}
	t.activityWindow = d
}

// BeginRun 标记 session 本轮开始运行（G-a 活跃判定的 running 分量：宿主进 loop 时调）。
//
// **进程本地**状态（同 live/hostHub 口径，不进 Snapshot）：计数 +1，支持嵌套/并发轮次；
// 与 EndRun 配平。BeginRun 后即使没有事件 ts 也算活跃（running 短路，见 SessionActive）。
func (t *Table) BeginRun(sessionID contracts.Ulid) {
	if t == nil || sessionID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running == nil { // 零值表也得能用（懒建）
		t.running = make(map[contracts.Ulid]int)
	}
	t.running[sessionID]++
}

// EndRun 标记 session 一轮运行结束（running 计数 -1，**下限 0**：重复 End 不会把计数
// 打成负数——负计数会让"没在跑"的 session 被判成 running>0 的活跃，且永不回落）。
func (t *Table) EndRun(sessionID contracts.Ulid) {
	if t == nil || sessionID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	switch n := t.running[sessionID]; {
	case n <= 1:
		delete(t.running, sessionID)
	default:
		t.running[sessionID] = n - 1
	}
}

// BeginWait 标记 session 进入挂起等待（G-f standby 的 waiting 态：宿主的 standby 工具
// 本轮挂起时调，与 BeginRun 同款配平）。
//
// **进程本地**状态（同 live/hostHub/running 口径，不进 Snapshot）：计数 +1，支持并发
// 嵌套的挂起；与 EndWait 配平。BeginWait 后即使窗口极小 / 没有事件 ts 也算活跃
// （waiting 短路，见 activeLocked）——但**不刷最后事件 ts**：挂起期零事件天然满足
// "不造假活动"，lastTS 仍停在挂起前那条事件上。
//
// **诚实边界（跨容器防掉线不成立）**：waiting 态是**进程本地**的——只在**一库一 hub**
// 的拓扑内有效；跨 hub / 跨容器时"对端还挂着没有"**看不见**。要跨进程可见，waiting
// 必须**落事件流**，而那与 standby"挂起期零事件"的约束**冲突**（见 host/standby.go
// 的文件头）→ 记挂账（waiting 落事件流 = 后续单独一刀），本刀不做。
func (t *Table) BeginWait(sessionID contracts.Ulid) {
	if t == nil || sessionID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.waiting == nil { // 零值表也得能用（懒建）
		t.waiting = make(map[contracts.Ulid]int)
	}
	t.waiting[sessionID]++
}

// EndWait 标记 session 挂起结束（waiting 计数 -1，**下限 0**：重复 End 不会把计数
// 打成负数——负计数会让"没在挂起"的 session 被判成 waiting>0 的活跃，且永不回落）。
func (t *Table) EndWait(sessionID contracts.Ulid) {
	if t == nil || sessionID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	switch n := t.waiting[sessionID]; {
	case n <= 1:
		delete(t.waiting, sessionID)
	default:
		t.waiting[sessionID] = n - 1
	}
}

// Reset 清空全部状态（重建入口：宿主随后从 seq 0 重放事件流）。
//
// 进程本地的 running / waiting 计数一并清空（与 live 随 sessions 重建失活同口径：
// 重启后没有任何 session "正在跑" / "正在挂起"，要由宿主重新 BeginRun / BeginWait）。
func (t *Table) Reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sessions = make(map[contracts.Ulid]*sessionState)
	t.agents = make(map[contracts.Ulid]*agentState)
	t.gates = make(map[string]map[contracts.Ulid]struct{})
	t.running = make(map[contracts.Ulid]int)
	t.waiting = make(map[contracts.Ulid]int)
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
		lefts:   make(map[string]gateLeft),
		joinGen: make(map[string]int),
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

// applyGateJoinedLocked 折 gate.joined：LWW 记 join 事实 + 届数计数 + 维护活跃成员集合。
func (t *Table) applyGateJoinedLocked(st *sessionState, ev *contracts.Event, seq int64) {
	var p contracts.GateJoinedPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return // 宽容（D7）
	}
	gateID, ok := NormalizeGateID(p.GateID)
	if !ok {
		return // 坏 gate_id：跳过（S9 在写入侧就拒了，这里是纵深防御）
	}
	// >= 而非 >：**同 seq 的重复 Apply 必须空转**——否则 joinGen（届数计数）会被
	// 重复事件累加，host 算出来的确定性事件 id 跟着漂（幂等要求 Apply 可重复）。
	if cur, exists := st.joins[gateID]; exists && cur.seq >= seq {
		return // 旧事件（或同一事件的重复投递）盖不掉新 join
	}
	st.joins[gateID] = gateJoin{gateID: gateID, source: string(p.Source), seq: seq, ts: ev.TS, eventID: ev.ID}
	st.joinGen[gateID]++
	// gate 定义持久化（G17b）：记了 join 事实就要确保枚举里看得见这个 gate——哪怕它
	// 随后被 leave 终止（成员走光了 gate 仍在，见 ensureGateLocked）。乱序重放时
	// leave 可能先到、把键建出来；这里两种到达顺序都保证"有事实就有键"。
	t.ensureGateLocked(gateID)
	if st.gateActiveLocked(gateID) {
		t.addGateMemberLocked(gateID, st.id)
	}
}

// applyGateLeftLocked 折 gate.left（G15）：LWW 记 leave 事实，若本次 leave 盖过 join
// （成员关系就此终止）则把 session 从该 gate 的活跃成员集合摘除。
func (t *Table) applyGateLeftLocked(st *sessionState, ev *contracts.Event, seq int64) {
	var p contracts.GateLeftPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return // 宽容（D7）
	}
	gateID, ok := NormalizeGateID(p.GateID)
	if !ok {
		return
	}
	if cur, exists := st.lefts[gateID]; exists && cur.seq >= seq {
		return // 重复/乱序的旧 leave：折叠态不变（幂等）
	}
	st.lefts[gateID] = gateLeft{seq: seq, ts: ev.TS, eventID: ev.ID}
	// 同 join：leave 事实也说明"这个 gate 存在过"（乱序重放时 leave 可能先于 join 到达）。
	t.ensureGateLocked(gateID)
	if !st.gateActiveLocked(gateID) {
		t.removeGateMemberLocked(gateID, st.id)
	}
}

// ensureGateLocked 确保 gate 在枚举里有一个键（成员集合可为空；调用方持锁）。
//
// gate 定义是持久化的（G17b）：成员关系可以终止（leave / session 结束），"这个 gate
// 存在过"不能从枚举面消失——所以键**建了就不删**（成员集合走空即 0 成员）。
func (t *Table) ensureGateLocked(gateID string) {
	if _, ok := t.gates[gateID]; !ok {
		t.gates[gateID] = make(map[contracts.Ulid]struct{})
	}
}

// addGateMemberLocked / removeGateMemberLocked 维护 gate → 活跃成员集合（调用方持锁）。
func (t *Table) addGateMemberLocked(gateID string, sid contracts.Ulid) {
	t.ensureGateLocked(gateID)
	t.gates[gateID][sid] = struct{}{}
}

func (t *Table) removeGateMemberLocked(gateID string, sid contracts.Ulid) {
	if members, ok := t.gates[gateID]; ok {
		delete(members, sid)
		// 故意**不删空 gate 键**：gate 定义是持久化的（G17b）——成员走光了 gate 仍在，
		// Gates() 仍列得出它（成员数 0）。删键会让"这个 gate 存在过"从枚举面消失。
	}
}

// ---------------------------------------------------------------------------
// 查询：行类型
// ---------------------------------------------------------------------------

// SessionRow 是发现表里一个 session 的行（K10：谁在线 / 在干什么 / 属于哪些 gate）。
//
// **没有 live 字段**：存活是进程本地状态（G17b），要它就用 LiveSessions() / IsLive()。
// 少一个字段，Snapshot 才能被直接比较（重建测试据此断言"重放后与崩溃前一致"）。
//
// Online / Running / Waiting（G-a 在线三态 + G-f 挂起态）是查询时**现算**的活跃画像，不存状态：
//
//	Online  = 活跃 = 本轮在跑（running）∨ 挂起中（waiting，G-f：豁免窗口判定）∨
//	          最后事件 ts 在活跃窗口内（判定源是事件流；**同一时刻 + 同一窗口配置**下，
//	          跨容器一致——两台容器对同一事件流、同一时刻算出同一个 Online）；
//	Running = running 计数 > 0（进程本地，重启失活）。
//	Waiting = waiting 计数 > 0（G-f standby 挂起中；进程本地，重启失活）。
//
// 与 live 的区别：live 答"本进程是否托管"（hosted），Online 答"现在算不算在线"——
// 托管但超窗的 session 在对端可见面（VisiblePeers）里不算活跃。
//
// ⚠ **Online/Running/Waiting 是现算态，不作重建比对面**（P2-4）：判定含 time.Now()（窗口是
// "距现在多久"），两个**不同时刻**取的快照，Online 本就可能不同（事件在窗口内老化）。
// 故"重放即得同一 Online"只在"同一时刻 + 同一窗口配置"下成立；重建比对**不要**拿
// Online/Running/Waiting 当断言面（比事件折叠出的那部分：成员关系、agent 树、join 历史）。
type SessionRow struct {
	SessionID   string `json:"session_id"`
	MainAgentID string `json:"main_agent_id"`
	// Gates 是该 session 的**活跃** gate 成员关系（G15：joined 减 left，字典序）。
	Gates []string `json:"gates"`
	// AgentCount 是该 session 的 agent 数（含主 agent 与全部 subagent）。
	AgentCount int `json:"agent_count"`
	// LastKind / LastSeq / UpdatedAt 是"在干什么"：最后一条事件的 kind / seq / ts。
	LastKind  string `json:"last_kind"`
	LastSeq   int64  `json:"last_seq"`
	UpdatedAt string `json:"updated_at"`
	// Online / Running / Waiting 是查询时现算的活跃画像（G-a / G-f，口径见类型注释）。
	Online  bool `json:"online"`
	Running bool `json:"running"`
	Waiting bool `json:"waiting"`
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
	// Members 是该 gate 的**活跃成员** session（G15 折叠口径：joined 减 left，
	// 字典序）。含"已结束但未显式离开"的 session（结束 ≠ 离开）；要再叠"本进程存活"
	// 的交集见 GateMembers。join/leave 全史实在事件流里（可回放），本行只答"现在谁是成员"。
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
//
// 行里的 Online / Running（G-a）是查询时现算的：**同一时刻 + 同一窗口配置**下重放
// 一致；它们是**现算态，不作重建比对面**（P2-4：判定含 time.Now()，不同时刻取的两份
// 快照，Online 本就可能不同——比对面是事件折叠出的那部分）。running / waiting 计数不进
// 快照（重启后没人正在跑 / 正在挂起，两边都是 false）。
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
		out = append(out, t.sessionRowLocked(st))
	}
	sortSessionRows(out)
	return out
}

// sessionRowLocked 折一个 session 的行（调用方持锁）。Online / Running / Waiting 在此
// **现算**（G-a / G-f）：活跃判定要读窗口配置与 running / waiting 计数，都在 Table 上，
// 所以它是 Table 的方法而不是自由函数——一处判定（activeLocked），Sessions/LiveSessions/
// Session/GateMembers/Snapshot 全走它，行出口不会各判各的。
func (t *Table) sessionRowLocked(st *sessionState) SessionRow {
	return SessionRow{
		SessionID:   string(st.id),
		MainAgentID: string(st.mainAgent()),
		Gates:       sortedGateIDs(st),
		AgentCount:  len(st.agents),
		LastKind:    st.lastKind,
		LastSeq:     st.lastSeq,
		UpdatedAt:   st.lastTS,
		Online:      t.activeLocked(st),
		Running:     t.running[st.id] > 0,
		Waiting:     t.waiting[st.id] > 0,
	}
}

// sortedGateIDs 返回 session 的**活跃** gate 成员关系（G15：joined 减 left，字典序）。
// 已显式离开的 gate 不出现在这里（leave 是成员关系的终止，见 gateActiveLocked）。
func sortedGateIDs(st *sessionState) []string {
	out := make([]string, 0, len(st.joins))
	for gateID := range st.joins {
		if st.gateActiveLocked(gateID) {
			out = append(out, gateID)
		}
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
	return t.sessionRowLocked(st), true
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

// SessionActive 报告 session 是否**活跃**（G-a 在线三态的活跃判定，对外唯一入口）：
// 活跃 = 本轮在跑（running>0）∨ 挂起中（waiting>0，G-f）∨ 最后事件 ts 落在活跃窗口内。
//
// 判定源是**事件流的最后事件 ts**（跨容器一致：同一事件流 + 同一窗口配置 → 同一结论），
// 不是 live（进程本地"托管"）——托管但超窗的 session 不算活跃（hosted 是另一态）。
// 未知 session 但已在跑 / 正在挂起（BeginRun / BeginWait 早于任何事件）也算活跃：
// 当下事实短路，与事件 ts 无关。
//
// 四消费方（K14/K15 在线表注入、K11 peers、agentlink presence、GET /api/sessions）
// 必须都经本方法（或其锁内版 activeLocked）判定，不得各自复制口径。
func (t *Table) SessionActive(sessionID contracts.Ulid) bool {
	if t == nil || sessionID == "" {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	st, ok := t.sessions[sessionID]
	if !ok {
		// 还没进表但本轮在跑 / 正在挂起（BeginRun / BeginWait 早于事件落地）：
		// 短路照样算活跃。
		return t.running[sessionID] > 0 || t.waiting[sessionID] > 0
	}
	return t.activeLocked(st)
}

// activeLocked 是 SessionActive 的**持锁版**（调用方必须已持锁；SessionActive 与
// VisiblePeers / 行出口共用——**一处判定**，避免两份口径漂移）。
//
// 判定顺序：running / waiting 短路（不看 ts）→ 无 ts 不活跃 → 窗口比对
// （core.WithinActivityWindow，未来时间戳按活跃算；坏 ts fail-close 判不活跃）。
// 窗口零值 = core.DefaultActivityWindow。
func (t *Table) activeLocked(st *sessionState) bool {
	if st == nil {
		return false
	}
	if t.running[st.id] > 0 {
		return true // 本轮在跑：running 短路，与事件 ts 无关（G-a）
	}
	if t.waiting[st.id] > 0 {
		// 挂起中：waiting 短路（G-f——算活跃、豁免窗口判定；**不刷 lastTS**：
		// 挂起期零事件，lastTS 停在挂起前那条事件上，不造假活动）。
		return true
	}
	if st.lastTS == "" {
		return false // 还没有任何带 ts 的事件：没有活动证据
	}
	ts, err := time.Parse(time.RFC3339, st.lastTS)
	if err != nil {
		return false // 坏 ts：fail-close，宁可判不活跃也不误报在线
	}
	return core.WithinActivityWindow(ts, true, time.Now(), t.activityWindowOrLocked())
}

// activityWindowOrLocked 取生效的活跃窗口（零值 = 缺省；调用方持锁）。
func (t *Table) activityWindowOrLocked() time.Duration {
	if t.activityWindow > 0 {
		return t.activityWindow
	}
	return core.DefaultActivityWindow
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

// GatesOf 返回某 session 的**活跃** gate 成员关系（G15：joined 减 left，字典序）。
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

// InGate 报告 session 当前是否**在该 gate 内**（活跃成员；G15：leave 之后再 join 又回来）。
//
// 宿主用它做显式 join 的幂等去重（已在网内的重复 join 不再落一条重复事件）；leave 之后
// 返回 false → 再次 join 会落**新一届**的事件（成员关系恢复，见 JoinGeneration）。
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
	return st.gateActiveLocked(id)
}

// JoinGeneration 返回该 (session, gate) 已折叠的 join 次数——"下一次 join 是第几届"。
//
// 为什么需要它（G15）：gate.joined 的 Event.ID 是**确定性导出**的（存储层
// ON CONFLICT(id) DO NOTHING 兜住并发/重复 join，见 host 的 newGateJoinedEvent）。
// 若 id 只由 (session, gate, source) 决定，leave 之后的**重 join** 会撞上第一次的 id
// 被存储层吃掉 → 成员关系永远恢复不了。把届数编进 id：同届并发 join 仍撞同一 id
// （去重不变量不变），重 join 是新的一届 → 新 id（恢复成立）。
//
// 计数只由**事件折叠**推进（Apply 幂等：同 seq 重复投递不累加），故跨重启稳定——
// 重启后全量重放得到同一个届数，重 join 的 id 与崩溃前算出来的一致。
func (t *Table) JoinGeneration(sessionID contracts.Ulid, gateID string) int {
	if t == nil {
		return 0
	}
	id, ok := NormalizeGateID(gateID)
	if !ok {
		return 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	st, ok := t.sessions[sessionID]
	if !ok {
		return 0
	}
	return st.joinGen[id]
}

// ActiveJoin 返回该 (session, gate) 当前**活跃成员关系**对应的 join 事实（G15）。
//
// host 的 leave 用它拿"被终止的成员关系的来源"（leave payload 的 source 要与对应
// gate.joined 的 source 同值，审计时才对得上）。不在网内 → ok=false。
func (t *Table) ActiveJoin(sessionID contracts.Ulid, gateID string) (GateJoin, bool) {
	if t == nil {
		return GateJoin{}, false
	}
	id, ok := NormalizeGateID(gateID)
	if !ok {
		return GateJoin{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	st, ok := t.sessions[sessionID]
	if !ok || !st.gateActiveLocked(id) {
		return GateJoin{}, false
	}
	j := st.joins[id]
	return GateJoin{
		GateID:    j.gateID,
		SessionID: string(st.id),
		Source:    j.source,
		Seq:       j.seq,
		TS:        j.ts,
		EventID:   string(j.eventID),
	}, true
}

// LeftGate 报告该 (session, gate) 是否**曾被显式离开过**（有 leave 事实；不论之后有没有再 join）。
//
// 用途（F1）：宿主要区分两种"当前不在网内"——
//   - **从没 join 过** → 自动 gate 该把它加进来；
//   - **join 过又 leave 过**（用户显式离开）→ 自动 gate **不该**把它复活：显式 leave 的
//     语义强于"自动 gate 策略"，否则进程一重启就把用户的显式离开**静默撤销**了
//     （用户看到的现象：明明 leave 了，重启后又出现在那个 gate 里）。
//
// 判据 = lefts 里的**键存在性**（不是 "seq > 0" 哨兵——见 gateActiveLocked 的说明）。
func (t *Table) LeftGate(sessionID contracts.Ulid, gateID string) bool {
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
	_, lefted := st.lefts[id]
	return lefted
}

// SharedGates 返回两个 session 的**活跃 gate 交集**（字典序；空 = 不相交）。
//
// 口径与 VisiblePeers 的 PeerRow.Gates **同源**（同一个 sharedGates 计算）：水平可见性
// 只承认"gate 相交"，故对外暴露的 gate 列表必须是**交集**，绝不是某个成员的**全量**
// （全量会把"它还在别的 gate 里"这种查询方无权知道的事泄露出去——K11：不在网内的 gate
// 只给存在性 + 计数，不给成员身份）。
func (t *Table) SharedGates(a, b contracts.Ulid) []string {
	if t == nil {
		return []string{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	me, ok := t.sessions[a]
	if !ok {
		return []string{}
	}
	other, ok := t.sessions[b]
	if !ok {
		return []string{}
	}
	return sharedGates(me.activeGatesLocked(), other)
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

// GateMembers 返回某 gate 的**活跃成员**（K12：成员关系是活的；G15：joined 减 left）。
//
// 活跃成员 = 事件折叠出的成员关系（joined 减 left）∩ 当前存活 session。已结束的 session
// 仍在 Gates() 的成员里（结束 ≠ 离开），但不出现在这里；显式 leave 过的 session 两边都不在
// （成员关系已终止，事实留在事件流里可回放）。
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
			out = append(out, t.sessionRowLocked(st))
		}
	}
	sortSessionRows(out)
	return out
}

// JoinHistory 返回全部 join **事实**（按 seq 升序；审计用）。
//
// 只折 join（leave 是另一条事实，不进本清单）：join 发生过就是发生过，之后的 leave 不
// 擦除它——"谁何时以何来源加入过"答这里；"现在谁在网内"答 Gates()/GateMembers()/InGate()。
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
// 规则：只有 session 的**主 agent**之间互相可见，条件活跃 gates(A) ∩ 活跃 gates(B) ≠ ∅
// （G15：任一侧显式 leave 了该 gate → 不再相交 → 互不可见）；对端必须是**活跃** session
// （G-a/G-f：running ∨ waiting ∨ 最后事件 ts 在窗口内——判定源是事件 ts，跨容器一致；
// 托管 live 不再是条件，托管但超窗的对端不进可见面）。别 session 的 subagent 完全不可见
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
	mine := me.activeGatesLocked()
	out := make([]PeerRow, 0, len(t.sessions))
	for _, other := range t.sessions {
		// 条件①（G-a）：对端必须**活跃**——活跃判定跨容器一致（事件 ts + 窗口），
		// live 是进程本地的"托管"，拿它当条件会让两台容器对同一事件流算出不同的可见面。
		// 活跃 = running ∨ waiting ∨ 窗口内（activeLocked，与 SessionActive / 行出口同一处判定）。
		if other.id == me.id || !t.activeLocked(other) {
			continue
		}
		shared := sharedGates(mine, other)
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

// sharedGates 求"我的活跃 gate 集合"与另一个 session 的活跃成员关系的交集（字典序）。
func sharedGates(mine map[string]bool, other *sessionState) []string {
	out := make([]string, 0, len(mine))
	for gateID := range mine {
		if other.gateActiveLocked(gateID) {
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
