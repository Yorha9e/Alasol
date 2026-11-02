package host

// 本文件是 v3-S6-B 的 agent 树**只读** HTTP 面：GET /api/sessions/{id}/agents 返回该 session
// 的树快照（GUI 派活面的数据源）。
//
// # 定位（协议面 / GUI 派活面）
//
// agent.spawned / completed / failed 事件已在事件流（bus → WS / 回放），GUI 订阅即见**动态**；
// 本端点补一个**静态快照**面：给定 session，一次性拿到"这棵树现在长什么样"（节点 + 层级 +
// 状态 + summary/失败原因）。与事件流互补，不是替代。
//
// # 可见性口径（**树优先**，F1 修）
//
// agent 树是 **session 活状态**（建了就一直复用；workflow 激活/停用只失效 loop、**不失效树**，
// 见 activateWorkflow）——所以"树还在"就是本进程看得见（200），**不依赖 loop 缓存**：
//
//	① 树已建 → 返快照（200）；
//	② 树未建但本进程托管（hostsSession）→ **空列表**（200）：开了 MultiAgent 还没建树，
//	   或没开 MultiAgent（单 agent 模式零行为变化）；
//	③ 都不是 → 404 unknown_session：别的进程建的 session，它的树本地看不见（K26，
//	   一库一 hub 是预期拓扑）。
//
// ① 的必要性：loop 失效（工作流停用）到下一次 RunPrompt 之间，**树还活着**——这时若按
// hostsSession 判就会回 404，把一棵活着的树误报成"野 session"（文案还会把成因说成
// "被别的进程建/失管"，与事实不符）。
//
// # 不改 transport 契约（F7 只增不适用）
//
// 这是 HTTP 面，不是 transport 契约 schema 的改动——第三方 GUI 可选用，不影响既有 WS/回放。

import (
	"fmt"
	"net/http"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/agents"
)

// AgentNodeView 是 agent 树快照的一个节点（GET /api/sessions/{id}/agents 的行）。
//
// **host 视图，非 contracts 契约类型**（与 PendingApprovalView 同性质）：把 agents.Node 快照
// + summary/失败原因折成前端好消费的形状。parent_agent_id 对 root 省略（root 无父）。
type AgentNodeView struct {
	// AgentID 节点 agent id（ULID；root = 该 session 的主 agent id）。
	AgentID string `json:"agent_id"`
	// ParentAgentID 父 agent id（root 省略——无父）。
	ParentAgentID string `json:"parent_agent_id,omitempty"`
	// Depth 深度（root=1；K17：> maxDepth 在数字层面不可达）。
	Depth int `json:"depth"`
	// AgentType 类型 id（K16；root 的类型是宿主内建 "root"）。
	AgentType string `json:"agent_type"`
	// Role 部门标签（K13；spawn 时父给的角色名，可空）。
	Role string `json:"role,omitempty"`
	// Status 生命周期状态：running | completed | failed（终态不可逆）。
	Status string `json:"status"`
	// Summary completed 时的结构化汇报（K24；其余状态省略）。
	Summary *contracts.AgentSummary `json:"summary,omitempty"`
	// FailReason failed 时的失败原因文案（其余状态省略）。
	FailReason string `json:"fail_reason,omitempty"`
}

// serveSessionAgents 服务 GET /api/sessions/{session_id}/agents（v3-S6-B：agent 树只读快照）。
//
//	200 → [AgentNodeView, ...]：从 root 起 BFS 的节点快照（空树 / 无 MultiAgent → []）；
//	400 → session_id 不合法；
//	404 → 该 session 既**没有 agent 树**、也**不由本进程托管**（unknown_session）。
//
// 判据 = **树存在即 200**（见文件头的"可见性口径"）：树是 session 活状态，loop 被失效
// （工作流停用）不影响它——所以不拿 hostsSession 当唯一判据。
func (h *Hub) serveSessionAgents(w http.ResponseWriter, r *http.Request) {
	sid, err := parseSessionID(r.PathValue("session_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_session_id", err.Error())
		return
	}
	// 可见性判据（**树优先**）：① 树已建 → 返快照；② 没树但本进程托管 → 空列表；
	// ③ 都不是 → 404（不放任野 session id 探活状态）。
	if !h.agentTreeExists(sid) && !h.hostsSession(sid) {
		// 文案不断言成因（终查 F2）：可能是别的进程建的/被重启失管，也可能是 /btw fork
		// 的 session（fork 的 agent 树**随 RunBtw 生命周期**——用完即弃，跑完这一轮就
		// 回收了，见 host/fork.go 的 dropForkAgentTree）。只陈述事实判据。
		writeJSONError(w, http.StatusNotFound, "unknown_session",
			fmt.Sprintf("session %s 既没有 agent 树、也不由本进程托管（agent 树是进程内活状态；"+
				"一库一 hub 是预期拓扑；注意 fork session 的 agent 树随 RunBtw 生命周期，用完即弃）", sid))
		return
	}
	nodes := h.agentTreeSnapshot(sid)
	if nodes == nil {
		nodes = []AgentNodeView{} // 空树 / 无 MultiAgent → []（不是 null，也不是 404）
	}
	writeJSONBody(w, http.StatusOK, nodes)
}

// agentTreeExists 报告该 session 是否**已建 agent 树**（v3-S6-B）。
//
// 树是 session 的**活状态**：建了就一直复用（workflow 激活/停用只 delete loops、不 delete
// agentTrees，见 activateWorkflow）——所以"有没有树"是比 hostsSession（loop 缓存）**更贴近
// 活状态**的判据：loop 失效的窗口里树仍在。
func (h *Hub) agentTreeExists(sessionID contracts.Ulid) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.agentTrees[sessionID]
	return ok
}

// agentTreeSnapshot 取某 session 的 agent 树快照（无树 → nil = 空列表）。
//
// 取树引用在 h.mu 下（agentTrees map 受它保护）；遍历走树自身的并发安全方法（不加 h.mu，
// 避免持锁遍历——树可能很大，且树的 Get/Children/SummaryOf 各有自己的锁）。
func (h *Hub) agentTreeSnapshot(sessionID contracts.Ulid) []AgentNodeView {
	h.mu.Lock()
	tree := h.agentTrees[sessionID]
	h.mu.Unlock()
	if tree == nil {
		return nil
	}
	return agentTreeNodes(tree)
}

// agentTreeNodes 从 root 起 BFS 遍历整棵树，折成节点视图（确定性：root 先行，子按 spawn 序）。
func agentTreeNodes(tree *agents.Tree) []AgentNodeView {
	if tree == nil {
		return nil
	}
	root := tree.Root()
	if root == "" {
		return nil
	}
	var out []AgentNodeView
	queue := []contracts.Ulid{root}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		node, ok := tree.Get(id)
		if !ok {
			continue
		}
		v := AgentNodeView{
			AgentID:   string(node.ID),
			Depth:     node.Depth,
			AgentType: node.Type,
			Role:      node.Role,
			Status:    string(node.Status),
		}
		if node.ParentID != nil {
			v.ParentAgentID = string(*node.ParentID)
		}
		if sum, ok := tree.SummaryOf(id); ok {
			s := sum
			v.Summary = &s
		}
		if reason, ok := tree.FailReason(id); ok {
			v.FailReason = reason
		}
		out = append(out, v)
		for _, c := range tree.Children(id) {
			queue = append(queue, c.ID)
		}
	}
	return out
}
