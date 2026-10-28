package agentlink

import (
	"fmt"
	"sort"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/discovery"
)

// SessionPresence 是一个 session 的在线画像（K18 Presence：谁在线 / 在干什么 /
// 属于哪些 gate）。字段口径与 discovery.SessionRow 一致——这里是 agentlink 的**视图**，
// 不新增状态源（D21：只在源头定义一次）。
type SessionPresence struct {
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
	// Live 标记该 session 是否由**本进程**持有（G17b：活状态是进程本地的，不可重建）。
	Live bool `json:"live"`
}

// PeerPresence 是水平可见对端的画像（K11①：对端 session 的**主 agent**）。
//
// Gates 是与查询方**相交**的 gate（不是对端的全部 gate）——别 session 的 subagent 完全
// 不出现在这里（粒度是 session 的主 agent，不是 agent）。
type PeerPresence struct {
	SessionID   string   `json:"session_id"`
	MainAgentID string   `json:"main_agent_id"`
	Gates       []string `json:"gates"`
	LastKind    string   `json:"last_kind"`
	LastSeq     int64    `json:"last_seq"`
	UpdatedAt   string   `json:"updated_at"`
	Live        bool     `json:"live"`
}

// PresenceReport 是 Presence 的返回：自己 + 水平可见对端。
type PresenceReport struct {
	// Self 是查询方自己的画像（Known=false 表示该 session 不在发现表里）。
	Self SessionPresence `json:"self"`
	// Known 报告该 session 是否被事件流见过（false → Self 是零值，别拿它当事实）。
	Known bool `json:"known"`
	// Peers 是水平可见对端（K11①；查询方自己不在其中）。
	Peers []PeerPresence `json:"peers"`
}

// Presence 查一个 session 的在线画像（K18 Presence）。
//
// **caller 是查询方身份（必填，K11 过滤用）**：
//
//	同 session   → 查自己：完整画像 + 水平可见对端（gate 相交的对端主 agent）；
//	跨 session   → 只有"可见对端"（gate 相交的对端**主 agent**）能查，且只回**对端视角**
//	               的画像（gate 只给相交的，别 session 的 agent 树规模不泄漏——K11：
//	               别 session 的 subagent 完全不可见）。
//
// 可见性判不通过 → ErrNotVisible（fail-close：宁可不报，也不让任意调用方枚举任意 session）。
func (l *Link) Presence(caller Identity, sessionID contracts.Ulid) (PresenceReport, error) {
	rep := PresenceReport{Peers: []PeerPresence{}}
	if l == nil || l.table == nil {
		return rep, ErrNoTable
	}
	if !caller.valid() {
		return rep, fmt.Errorf("%w: caller=%+v", ErrBadIdentity, caller)
	}
	if !core.ValidUlid(sessionID) {
		return rep, fmt.Errorf("%w: session_id %q 不是合法 ULID", ErrBadIdentity, sessionID)
	}
	if caller.SessionID == sessionID {
		if row, ok := l.table.Session(sessionID); ok {
			rep.Known = true
			rep.Self = sessionPresenceOf(row, l.table.IsLive(sessionID))
		}
		for _, p := range l.table.VisiblePeers(sessionID) {
			rep.Peers = append(rep.Peers, PeerPresence{
				SessionID:   p.SessionID,
				MainAgentID: p.MainAgentID,
				Gates:       p.Gates,
				LastKind:    p.LastKind,
				LastSeq:     p.LastSeq,
				UpdatedAt:   p.UpdatedAt,
				Live:        true, // VisiblePeers 只回存活对端（K12/G17b：成员关系是活的）
			})
		}
		return rep, nil
	}
	// 目标 session 压根不在发现表里（事件流里没见过它）→ 查不到，不是错误。
	if _, ok := l.table.Session(sessionID); !ok {
		return rep, nil
	}
	// 跨 session：必须是可见对端（gate 相交的对端**主 agent**——K11①）。
	peer, ok := l.visiblePeerOf(caller, sessionID)
	if !ok {
		return rep, fmt.Errorf("%w: caller session=%s agent=%s → session=%s（水平可见性只在**主 agent**之间成立，K11①）",
			ErrNotVisible, caller.SessionID, caller.AgentID, sessionID)
	}
	rep.Known = true
	rep.Self = peerSessionPresence(peer)
	return rep, nil
}

// visiblePeerOf 取调用方的水平可见对端行（gate 相交 ∩ 存活；调用方必须是主 agent）。
func (l *Link) visiblePeerOf(caller Identity, target contracts.Ulid) (discovery.PeerRow, bool) {
	if l == nil || l.table == nil {
		return discovery.PeerRow{}, false
	}
	main, ok := l.table.PrimaryAgent(caller.SessionID)
	if !ok || main != caller.AgentID {
		return discovery.PeerRow{}, false
	}
	for _, p := range l.table.VisiblePeers(caller.SessionID) {
		if contracts.Ulid(p.SessionID) == target {
			return p, true
		}
	}
	return discovery.PeerRow{}, false
}

// peerSessionPresence 把对端行折成 agentlink 视图（**只给相交的 gate**，不泄漏对端
// 的全部 gate 与 agent 树规模——K11：对端只以"主 agent"的身份可见）。
func peerSessionPresence(p discovery.PeerRow) SessionPresence {
	return SessionPresence{
		SessionID:   p.SessionID,
		MainAgentID: p.MainAgentID,
		Gates:       p.Gates,
		AgentCount:  0, // 不泄漏别 session 的 agent 树规模
		LastKind:    p.LastKind,
		LastSeq:     p.LastSeq,
		UpdatedAt:   p.UpdatedAt,
		Live:        true,
	}
}

// gateMemberPresence 把 gate 成员行折成**对端视角**的画像（#3）：只给与调用方**相交**的
// gate，不泄漏对端的全部 gate 与 agent 树规模（口径与 peerSessionPresence 一致）。
func (l *Link) gateMemberPresence(caller Identity, r discovery.SessionRow) SessionPresence {
	shared := []string{}
	for _, g := range r.Gates {
		if l.gateVisibleTo(caller, g) {
			shared = append(shared, g)
		}
	}
	return peerSessionPresence(discovery.PeerRow{
		SessionID:   r.SessionID,
		MainAgentID: r.MainAgentID,
		Gates:       shared,
		LastKind:    r.LastKind,
		LastSeq:     r.LastSeq,
		UpdatedAt:   r.UpdatedAt,
	})
}

// Online 返回**本进程存活**的 session 画像（K18 Presence 的"谁在线"）。
//
// 口径同 discovery.LiveSessions（G17b：活状态进程本地，无租约无心跳；崩溃重启后老成员
// 全部失活，要由宿主重新激活）。不含对端交集信息——那是 Presence(sessionID) 的事。
func (l *Link) Online() []SessionPresence {
	if l == nil || l.table == nil {
		return []SessionPresence{}
	}
	rows := l.table.LiveSessions()
	out := make([]SessionPresence, 0, len(rows))
	for _, r := range rows {
		out = append(out, sessionPresenceOf(r, true))
	}
	return out
}

func sessionPresenceOf(r discovery.SessionRow, live bool) SessionPresence {
	return SessionPresence{
		SessionID:   r.SessionID,
		MainAgentID: r.MainAgentID,
		Gates:       r.Gates,
		AgentCount:  r.AgentCount,
		LastKind:    r.LastKind,
		LastSeq:     r.LastSeq,
		UpdatedAt:   r.UpdatedAt,
		Live:        live,
	}
}

// AgentPresence 是 agent 行的 agentlink 视图（K18 Discover 的 agent 维度 + 树投影）。
type AgentPresence struct {
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
	// LastKind / UpdatedAt 是"在干什么"（同 session 行口径：发现表按 session 记最近活动）。
	LastKind  string `json:"last_kind"`
	UpdatedAt string `json:"updated_at"`
}

// DiscoverQuery 是一次查找的条件（K18 Discover：按 gate / 按 agent 树）。
//
// GateID 与 AgentID **二选一**（都空或都非空 → ErrBadQuery：一次查找只沿一个维度，
// 混在一起既难定义结果形状，也容易把两种可见性口径搅浑）。
//
// **Caller 是查询方身份（必填）**：没有它，任何调用方都能枚举任意 gate 的成员、
// 任意 session 的 agent 树（F4：跨 session 的 agent 树/gate 列表是泄漏面）。
type DiscoverQuery struct {
	// Caller 查询方身份（K11 过滤用；必填）。
	Caller Identity
	// GateID 非空 → 按 gate 找**活跃成员** session（K12：成员关系是活的）。
	// 调用方必须在该 gate 里（或与它相交）——否则看不见这个 gate 的成员名单。
	GateID string
	// AgentID 非空 → 按 agent 找它所在的 session，并把该 agent 放进 Agents。
	AgentID contracts.Ulid
	// Subtree 仅对 AgentID 查询生效：true → Agents 返回以该 agent 为根的子树（含自己），
	// false → 只返回该 agent 一行。**子树不跨 session**（K11②：树按 session 作用域隔离；
	// 跨 session 只允许查可见对端的**主 agent**一行，不给它的子树）。
	Subtree bool
}

// DiscoverResult 是 Discover 的返回（Sessions 与 Agents 至少有一个非空；查询维度决定
// 哪一个有内容——gate 查询不给 agent 清单，因为跨 session 列 agent 不是可见性面）。
type DiscoverResult struct {
	Query    DiscoverQuery     `json:"query"`
	Sessions []SessionPresence `json:"sessions"`
	Agents   []AgentPresence   `json:"agents"`
}

// Discover 按 gate / 按 agent 树查找（K18 Discover）。
//
//	gate 查询  → Sessions = 该 gate 的活跃成员（发现表 GateMembers，已叠 live 过滤）；
//	             **调用方必须与该 gate 相交**（否则 ErrNotVisible：不能枚举没加入的 gate）；
//	             Agents 恒空（K11①：水平可见性的粒度是 session 主 agent，不列对端 agent）；
//	agent 查询 → 同 session：Sessions = 该 agent 所在的 session（一行），
//	             Agents = 该 agent（Subtree=false）或以它为根的子树（Subtree=true）；
//	             跨 session：**只有可见对端的主 agent**能查，且只回它一行（不给子树、
//	             不给对端 agent 树规模）——别 session 的 subagent 完全不可见（K11）。
//
// 未知 gate / 未知 agent 不是错误：返回空结果（"查不到"是合法答案，只有查询条件本身
// 不合法才是 ErrBadQuery）。
func (l *Link) Discover(q DiscoverQuery) (DiscoverResult, error) {
	res := DiscoverResult{Query: q, Sessions: []SessionPresence{}, Agents: []AgentPresence{}}
	if l == nil || l.table == nil {
		return res, ErrNoTable
	}
	if !q.Caller.valid() {
		return res, fmt.Errorf("%w: caller=%+v", ErrBadIdentity, q.Caller)
	}
	switch {
	case q.GateID != "" && q.AgentID != "":
		return res, fmt.Errorf("%w: gate_id 与 agent_id 不可同时给出", ErrBadQuery)
	case q.GateID != "":
		id, ok := discovery.NormalizeGateID(q.GateID)
		if !ok {
			return res, fmt.Errorf("%w: gate_id %q 不合法（空 / 超过 %d 字符 / 含空白或控制字符）",
				ErrBadQuery, q.GateID, discovery.MaxGateIDLen)
		}
		// 调用方必须与该 gate 相交：否则就是"枚举任意 gate 的成员名单"（F4 的泄漏面）。
		// 但 gate 压根不存在 → 空结果（"查不到"是合法答案，不是错误）。
		if l.gateExists(id) && !l.gateVisibleTo(q.Caller, id) {
			return res, fmt.Errorf("%w: caller session=%s 与 gate %q 不相交（看不见这个 gate 的成员）",
				ErrNotVisible, q.Caller.SessionID, id)
		}
		for _, r := range l.table.GateMembers(id) {
			if contracts.Ulid(r.SessionID) == q.Caller.SessionID {
				// 自己：完整画像（自己的 gate 列表与 agent 树规模不是泄漏面）。
				res.Sessions = append(res.Sessions, sessionPresenceOf(r, true))
				continue
			}
			// 对端：**对端视角**（#3）——只给与调用方相交的 gate，不泄漏对端的全部 gate
			// 与 agent 树规模（K11①：对端只以"主 agent"的身份可见；口径与
			// peerSessionPresence 一致）。
			res.Sessions = append(res.Sessions, l.gateMemberPresence(q.Caller, r))
		}
		return res, nil
	case q.AgentID != "":
		row, ok := l.table.Agent(q.AgentID)
		if !ok {
			return res, nil // 查无此 agent：空结果（不是错误）
		}
		sid := contracts.Ulid(row.SessionID)
		if sid == q.Caller.SessionID {
			// 自己树内：完整投影（session 行 + agent 行 / 子树）。
			if srow, ok := l.table.Session(sid); ok {
				res.Sessions = append(res.Sessions, sessionPresenceOf(srow, l.table.IsLive(sid)))
			}
			if q.Subtree {
				res.Agents = l.subtreeAgents(sid, q.AgentID)
			} else {
				res.Agents = []AgentPresence{agentPresenceOf(row)}
			}
			return res, nil
		}
		// 跨 session：只有可见对端的**主 agent**能查（K11① + 别 session 的 subagent
		// 完全不可见）。只回它一行——不给它的子树、不给对端 agent 树规模。
		peer, ok := l.visiblePeerOf(q.Caller, sid)
		if !ok || contracts.Ulid(peer.MainAgentID) != q.AgentID {
			return res, fmt.Errorf("%w: agent %s 属于 session %s，不在调用方的可见面里（只允许可见对端的**主 agent**，K11）",
				ErrNotVisible, q.AgentID, sid)
		}
		res.Sessions = append(res.Sessions, peerSessionPresence(peer))
		res.Agents = []AgentPresence{agentPresenceOf(row)}
		return res, nil
	default:
		return res, fmt.Errorf("%w: gate_id 与 agent_id 至少给一个", ErrBadQuery)
	}
}

// gateVisibleTo 报告调用方与该 gate 是否相交（K11①：水平可见性 = gate 相交）。
func (l *Link) gateVisibleTo(caller Identity, gateID string) bool {
	if l == nil || l.table == nil {
		return false
	}
	for _, g := range l.table.GatesOf(caller.SessionID) {
		if g == gateID {
			return true
		}
	}
	return false
}

// gateExists 报告该 gate 有没有 join 历史（不存在 → 查它是空结果，不是"不可见"）。
func (l *Link) gateExists(gateID string) bool {
	if l == nil || l.table == nil {
		return false
	}
	for _, g := range l.table.Gates() {
		if g.GateID == gateID {
			return true
		}
	}
	return false
}

// Subtree 返回以 agentID 为根的子树（含自己，按首次见到顺序 = 树的自然顺序）。
//
// 未知 agent → 空切片（不错误：与 Discover 同口径）。用于 Broadcast/Handoff 前确认目标
// 范围，也供宿主 UI 展示"这条消息发到了哪些 agent"。
//
// **注意**：这是**原始树投影**，不是可见性面——它不按调用方裁剪（K11 的裁剪在
// Discover / Presence / Inquiry / 消息投递那几处）。别把它直接挂到协议面上：
// 跨 session 的 agent 清单是宿主内部视图（见 core/discovery 的 Agents 注释）。
func (l *Link) Subtree(agentID contracts.Ulid) []AgentPresence {
	if l == nil || l.table == nil {
		return []AgentPresence{}
	}
	row, ok := l.table.Agent(agentID)
	if !ok {
		return []AgentPresence{}
	}
	return l.subtreeAgents(contracts.Ulid(row.SessionID), agentID)
}

// subtreeAgents 在同一 session 的 agent 行里筛出 root 的子树（含 root）。
//
// 树投影来自发现表（Agents() 的 ParentID 链），**不 import core/agents**：发现表折的是
// 事件，不是 agent 树的实现（两边各自对同一组事件做投影，见 core/discovery 的注释）。
func (l *Link) subtreeAgents(sessionID, root contracts.Ulid) []AgentPresence {
	rows := l.table.Agents(sessionID)
	byID := make(map[contracts.Ulid]discovery.AgentRow, len(rows))
	children := make(map[contracts.Ulid][]contracts.Ulid, len(rows))
	for _, r := range rows {
		id := contracts.Ulid(r.AgentID)
		byID[id] = r
		if r.ParentID != "" {
			p := contracts.Ulid(r.ParentID)
			children[p] = append(children[p], id)
		}
	}
	out := make([]AgentPresence, 0, len(rows))
	seen := map[contracts.Ulid]bool{}
	var walk func(contracts.Ulid)
	walk = func(id contracts.Ulid) {
		if seen[id] {
			return // 环防御（事件流理论上不该有环，但折叠态不保证）
		}
		seen[id] = true
		if row, ok := byID[id]; ok {
			out = append(out, agentPresenceOf(row))
		}
		kids := children[id]
		sort.SliceStable(kids, func(i, j int) bool { return kids[i] < kids[j] })
		for _, c := range kids {
			walk(c)
		}
	}
	walk(root)
	return out
}

// ---------------------------------------------------------------------------
// K11 垂直维：父 ↔ 直系子（可见集的唯一实现处）
// ---------------------------------------------------------------------------

// agentTree 是某 session 的 agent 行索引（父链 + 子表），供 K11 可见性判定用。
//
// 发现表只给"按 session 的行"与"按 agent 的一行"，没有"children/descendants"查询——
// 这里在 agentlink 侧折一次（纯内存、无 IO），**不复制 K11 逻辑**：K11 的口径仍然是
// "父 ↔ 直系子"，本索引只是把父链查出来。
type agentTree struct {
	byID     map[contracts.Ulid]discovery.AgentRow
	children map[contracts.Ulid][]contracts.Ulid
}

// treeOf 折出该 session 的 agent 树索引（子表按 agent id 升序 = 确定性顺序）。
func (l *Link) treeOf(sessionID contracts.Ulid) agentTree {
	t := agentTree{
		byID:     map[contracts.Ulid]discovery.AgentRow{},
		children: map[contracts.Ulid][]contracts.Ulid{},
	}
	if l == nil || l.table == nil {
		return t
	}
	for _, r := range l.table.Agents(sessionID) {
		id := contracts.Ulid(r.AgentID)
		t.byID[id] = r
		if r.ParentID != "" {
			p := contracts.Ulid(r.ParentID)
			t.children[p] = append(t.children[p], id)
		}
	}
	for p := range t.children {
		kids := t.children[p]
		sort.SliceStable(kids, func(i, j int) bool { return kids[i] < kids[j] })
	}
	return t
}

// parentOf 返回该 agent 的直系父（空 = 该 session 的主 agent / root）。
func (t agentTree) parentOf(id contracts.Ulid) contracts.Ulid {
	row, ok := t.byID[id]
	if !ok || row.ParentID == "" {
		return ""
	}
	return contracts.Ulid(row.ParentID)
}

// childrenOf 返回该 agent 的直系子（升序；不含自己）。
func (t agentTree) childrenOf(id contracts.Ulid) []contracts.Ulid {
	return t.children[id]
}

// visibleAgents 返回该 agent 在 K11 垂直维的**可见集**：自己的父 + 自己的直系子
// （K11②：父 ↔ 直系子；兄弟互相不可见；别 session 的 subagent 完全不可见）。
//
// 向上只一跳（K27：跨层上查 = 沿父链逐级显式调用，不是"全祖先自动可见"）。
func (t agentTree) visibleAgents(id contracts.Ulid) map[contracts.Ulid]bool {
	out := map[contracts.Ulid]bool{}
	if p := t.parentOf(id); p != "" {
		out[p] = true
	}
	for _, c := range t.childrenOf(id) {
		out[c] = true
	}
	return out
}

// selfVisibleAgents 是"含自己"的可见集（Inquiry 的同 session 默认投影用：
// 调用方看得见自己的事件 + 父的 + 直系子的）。
func (t agentTree) selfVisibleAgents(id contracts.Ulid) map[contracts.Ulid]bool {
	out := t.visibleAgents(id)
	out[id] = true
	return out
}

// subtreeRecipients 算一条 subtree 消息（from → root）的**收件方集合**（K11② + K27）。
//
// 口径（doc.go 有同款表述）：
//
//	目标子树     = 以 root 为根的子树（同一 session 内；跨 session 在 validateTarget 就被拒）；
//	发送方可见集 = 自己的父 + 自己的直系子（K11② 的一跳可见集）；
//	收件方       = **目标子树 ∩ 发送方可见集**。
//
// 为什么不是"整棵子树"（F3）：旧口径取子树的**传递闭包** → 目标指向祖先时，
// 兄弟与隔代（孙级）都收到了消息（违 K11②「兄弟不可见」+ K27「不是全祖先/全后代
// 自动可见」）。可见性是**一跳**的：R 能收到 S 的消息 ⟺ R 与 S 父子相邻。
// 于是"父向下广播"只到直系子，要再往下走由那一层自己发（逐级显式，K27）。
//
// 这条口径同时用在**写时**（Broadcast/Handoff 的 Recipients）与**读时**（Inbox 过滤）——
// 两处必须一致，否则审计里的收件方与实际能拉到的消息对不上。
func (l *Link) subtreeRecipients(from Identity, root contracts.Ulid) map[contracts.Ulid]bool {
	out := map[contracts.Ulid]bool{}
	if l == nil || l.table == nil {
		return out
	}
	tree := l.treeOf(from.SessionID)
	inSubtree := map[contracts.Ulid]bool{}
	for _, a := range l.subtreeAgents(from.SessionID, root) {
		inSubtree[contracts.Ulid(a.AgentID)] = true
	}
	allowed := tree.visibleAgents(from.AgentID)
	for id := range inSubtree {
		if id == from.AgentID {
			continue // 不给自己投递
		}
		if allowed[id] {
			out[id] = true
		}
	}
	return out
}

// inSubtreeOf 报告 agent 是否在以 root 为根的子树里（同一 session）。
func (l *Link) inSubtreeOf(sessionID, root, agent contracts.Ulid) bool {
	for _, a := range l.subtreeAgents(sessionID, root) {
		if contracts.Ulid(a.AgentID) == agent {
			return true
		}
	}
	return false
}

func agentPresenceOf(r discovery.AgentRow) AgentPresence {
	return AgentPresence{
		AgentID:   r.AgentID,
		SessionID: r.SessionID,
		ParentID:  r.ParentID,
		Type:      r.Type,
		Role:      r.Role,
		Status:    r.Status,
		Depth:     r.Depth,
		Main:      r.Main,
		LastKind:  r.LastKind,
		UpdatedAt: r.UpdatedAt,
	}
}
