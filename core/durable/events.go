package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// 本包产生/消费的事件 kind（core 定义核心子集；契约形状见 contracts.*Payload）。
const (
	// KindCheckpointCreated checkpoint 落成的 durable 事件（X3）。
	KindCheckpointCreated = "checkpoint.created"
	// KindSessionResumed 一次 resume 的事实记录（X4⑤）。
	KindSessionResumed = "session.resumed"

	// KindToolCallIntent / Completed / Failed 三段写路径（X1；本包只**读**它们扫 in-flight）。
	KindToolCallIntent    = "tool.call.intent"
	KindToolCallCompleted = "tool.call.completed"
	KindToolCallFailed    = "tool.call.failed"

	// KindLedgerUpdated ledger 全量快照（X6；LedgerWriter 落它）。
	KindLedgerUpdated = "ledger.updated"

	// agent.* 生命周期事件（本包只**读**它们 fold 树/ownership，X5）。
	KindAgentSpawned   = "agent.spawned"
	KindAgentCompleted = "agent.completed"
	KindAgentFailed    = "agent.failed"
)

// nowRFC3339 返回 UTC RFC 3339 时间戳（契约 ts 字段格式；与 loop 同口径）。
func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }

// newDurableEvent 组装一条本 session / agent 归属的 durable 事件。
//
// durable 包经 core.Store 直接 append（Kafka 模式，G9）：不经过 bus——
// checkpoint/resume 是批处理型操作，订阅者的实时通知由 bus 在交互路径负责；
// 索引器会照常消费这些事件维护投影。事件 id 用 core.NewUlid()（框架 id 唯一生成处）。
func newDurableEvent(sessionID, agentID contracts.Ulid, kind string, causation *contracts.Ulid, payload any) (contracts.Event, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return contracts.Event{}, fmt.Errorf("durable: 序列化 %s 事件 payload 失败: %w", kind, err)
	}
	return contracts.Event{
		V:           1,
		ID:          core.NewUlid(),
		Kind:        kind,
		TS:          nowRFC3339(),
		SessionID:   sessionID,
		AgentID:     agentID,
		CausationID: causation,
		Durable:     true,
		Payload:     raw,
	}, nil
}

// readSession 读一场 session 的全部 durable 事实事件（按 seq 升序）。
//
// 语义与 runtime.ReplaySession 完全一致（都是 store.ReadFrom(sessionID, 1, 0)）——
// core 不 import runtime（C2），故在此复刻这同一次调用；durable=false 的流事件从未入库
// (F8)，读到的即该 session 的完整事实历史。
func readSession(ctx context.Context, store core.Store, sessionID contracts.Ulid) ([]contracts.Event, error) {
	if store == nil {
		return nil, fmt.Errorf("durable: store 为 nil（session %s）", sessionID)
	}
	events, err := store.ReadFrom(ctx, &sessionID, 1, 0)
	if err != nil {
		return nil, fmt.Errorf("durable: 读 session %s 事件流: %w", sessionID, err)
	}
	return events, nil
}

// primaryAgentID 推导 session 的「主 agent」id：agent.spawned 里作为 parent 出现但从不作为
// child 出现的 agent（= 树根）；无 spawn 时取最早事件（最小 seq）的 agent_id；空流时给一个新
// ULID。用于给 session 级的 checkpoint.created / session.resumed 事件填 envelope 必需的 agent_id
// （这些事件描述的是整个 session，不是某个子 agent）。
func primaryAgentID(events []contracts.Event) contracts.Ulid {
	children := make(map[contracts.Ulid]bool)
	for i := range events {
		if events[i].Kind != KindAgentSpawned {
			continue
		}
		var p contracts.AgentSpawnedPayload
		if json.Unmarshal(events[i].Payload, &p) == nil {
			children[p.ChildAgentID] = true
		}
	}
	// events 已按 seq 升序：第一个「不是任何人 child」的 agent 即根（最早启动的主 loop）。
	for i := range events {
		id := events[i].AgentID
		if id != "" && !children[id] {
			return id
		}
	}
	// 全是 child（异常流）→ 退到最早事件的 agent_id。
	for i := range events {
		if events[i].AgentID != "" {
			return events[i].AgentID
		}
	}
	// 空 session：给一个新 id（事件仍要合法落库）。
	return core.NewUlid()
}
