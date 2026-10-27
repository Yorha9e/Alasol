package durable

import (
	"encoding/json"

	"github.com/Yorha9e/alasol/contracts"
)

// Ownership 是一条 spawn 归属记录（X5）：(parent_agent_id, spawn_request_id) → child_agent_id。
//
// 这正是 core/agents 树内部幂等键 requestKey 的持久化投影：续跑时按这两个字段找回**同一个**
// child，不新建。agents.Tree.Spawn 的内存幂等（同 (parent, request_id) 返回同一 child）在进程
// 崩溃后随内存消失；durable 从 agent.spawned 事件 fold 出这份映射，使该幂等键跨崩溃可恢复。
type Ownership struct {
	ParentAgentID  contracts.Ulid `json:"parent_agent_id"`
	SpawnRequestID contracts.Ulid `json:"spawn_request_id"`
	ChildAgentID   contracts.Ulid `json:"child_agent_id"`
}

// RestoredNode 是 resume 时从事件流 fold 出的 agent 树节点（X4④）。
type RestoredNode struct {
	ID       contracts.Ulid  `json:"id"`
	ParentID *contracts.Ulid `json:"parent_id,omitempty"` // root 为 nil
	Type     string          `json:"type"`                // agent_type id
	Role     string          `json:"role"`                // 部门标签（K13）
	Depth    int             `json:"depth"`
	// Status 生命周期状态：running / completed / failed（fold 自 agent.* 事件）。
	Status string `json:"status"`
}

// RestoredTree 是 resume 时从事件流 fold 出的 agent 树投影（X4④）。
//
// v0 只恢复**结构 + 状态**（谁 spawn 了谁、谁已收敛/失败、谁还 running）——真正续跑未完成
// agent 的 turn 由调用方 loop 接手（本包不驱动模型调用）。running 节点上未决的工具调用即
// in-flight，交 Resume 按 X2 处置。
type RestoredTree struct {
	// Nodes 以 agent_id 为键。
	Nodes map[contracts.Ulid]RestoredNode `json:"nodes"`
	// Root 是推断出的根 agent（primaryAgentID 的口径；无 agent 事件时为零值）。
	Root contracts.Ulid `json:"root"`
	// ownership 保留 fold 顺序的归属记录（供 ResolveChild 与审计）。
	ownership []Ownership
}

// foldTree 从事件流 fold agent 树投影 + ownership 记录（X4④ / X5）。
//
// 只认 agent.spawned / agent.completed / agent.failed；其余 kind 忽略（D7 宽容）。
// agent.spawned 以 Event.ID 幂等（store 已保证），重复 fold 同一批事件得同一棵树。
func foldTree(events []contracts.Event) *RestoredTree {
	t := &RestoredTree{Nodes: make(map[contracts.Ulid]RestoredNode)}
	for i := range events {
		ev := &events[i]
		switch ev.Kind {
		case KindAgentSpawned:
			var p contracts.AgentSpawnedPayload
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue
			}
			pid := p.ParentAgentID
			t.Nodes[p.ChildAgentID] = RestoredNode{
				ID:       p.ChildAgentID,
				ParentID: &pid,
				Type:     p.AgentType,
				Role:     p.Role,
				Depth:    int(p.Depth),
				Status:   "running", // spawn 时 running；completed/failed 再改终态
			}
			t.ownership = append(t.ownership, Ownership{
				ParentAgentID:  p.ParentAgentID,
				SpawnRequestID: p.SpawnRequestID,
				ChildAgentID:   p.ChildAgentID,
			})
		case KindAgentCompleted:
			var p contracts.AgentCompletedPayload
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue
			}
			if n, ok := t.Nodes[p.AgentID]; ok {
				n.Status = "completed"
				t.Nodes[p.AgentID] = n
			}
		case KindAgentFailed:
			var p contracts.AgentFailedPayload
			if json.Unmarshal(ev.Payload, &p) != nil {
				continue
			}
			if n, ok := t.Nodes[p.AgentID]; ok {
				n.Status = "failed"
				t.Nodes[p.AgentID] = n
			}
		}
	}
	t.Root = primaryAgentID(events)
	return t
}

// Ownership 返回 fold 出的全部归属记录（spawn 顺序）。
func (t *RestoredTree) Ownership() []Ownership {
	if t == nil {
		return nil
	}
	return append([]Ownership(nil), t.ownership...)
}

// ResolveChild 按 (parent_agent_id, spawn_request_id) 找回已存在的 child（X5）。
//
// 返回 (childID, true) = 该 (父, request) 之前已 spawn 过同一个 child，续跑应复用它、
// **不新建**（与 agents.Tree.Spawn 的幂等键同一口径）。找不到 → ("", false)。
func (t *RestoredTree) ResolveChild(parentID, requestID contracts.Ulid) (contracts.Ulid, bool) {
	if t == nil {
		return "", false
	}
	for _, o := range t.ownership {
		if o.ParentAgentID == parentID && o.SpawnRequestID == requestID {
			return o.ChildAgentID, true
		}
	}
	return "", false
}

// Children 返回某 agent 的直系子（spawn 顺序）。
func (t *RestoredTree) Children(id contracts.Ulid) []RestoredNode {
	if t == nil {
		return nil
	}
	var out []RestoredNode
	for _, o := range t.ownership {
		if o.ParentAgentID != id {
			continue
		}
		if n, ok := t.Nodes[o.ChildAgentID]; ok {
			out = append(out, n)
		}
	}
	return out
}

// RunningNodes 返回所有仍 running（未收敛/未失败）的节点——它们可能有未决的 in-flight 工具调用，
// 也是「真正续跑 turn 由调用方 loop 接手」的作用对象（X4④）。
func (t *RestoredTree) RunningNodes() []RestoredNode {
	if t == nil {
		return nil
	}
	var out []RestoredNode
	for _, o := range t.ownership {
		if n, ok := t.Nodes[o.ChildAgentID]; ok && n.Status == "running" {
			out = append(out, n)
		}
	}
	return out
}
