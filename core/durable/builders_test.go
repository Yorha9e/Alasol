package durable_test

import (
	"encoding/json"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// 事件构造 helpers：把各种 payload 拍成 durable 事件（seq 由 store 分配）。
func ev(session, agent contracts.Ulid, kind string, payload any) contracts.Event {
	raw, _ := json.Marshal(payload)
	return contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: kind, TS: "2026-10-28T00:00:00.000Z",
		SessionID: session, AgentID: agent, Durable: true, Payload: raw,
	}
}

func evIntent(session, agent contracts.Ulid, tcID, tool string, replay contracts.ReplayKind, anchors []contracts.Anchor) contracts.Event {
	return ev(session, agent, "tool.call.intent", contracts.ToolCallIntentPayload{
		ToolCallID: contracts.ProviderId(tcID), ToolName: tool,
		Arguments: json.RawMessage(`{"k":"v"}`), Idempotent: replay == contracts.ReplayKindSafe, Replay: replay,
		Anchors: anchors,
	})
}

func evResult(session, agent contracts.Ulid, kind, tcID string) contracts.Event {
	return ev(session, agent, kind, map[string]string{"tool_call_id": tcID})
}

func evSpawned(session, parent, child contracts.Ulid, agentType, role string, depth int, reqID contracts.Ulid) contracts.Event {
	return ev(session, parent, "agent.spawned", contracts.AgentSpawnedPayload{
		ParentAgentID: parent, ChildAgentID: child, AgentType: agentType, Role: role,
		Depth: int64(depth), SpawnRequestID: reqID,
	})
}

func evAgentTerminal(session, agent contracts.Ulid, kind string) contracts.Event {
	if kind == "agent.completed" {
		return ev(session, agent, kind, contracts.AgentCompletedPayload{
			AgentID: agent, Summary: contracts.AgentSummary{Text: "done", Taint: "clean"},
		})
	}
	return ev(session, agent, kind, contracts.AgentFailedPayload{
		AgentID: agent, Error: contracts.ToolCallError{Message: "boom"},
	})
}

func evLedger(session, agent contracts.Ulid, goals []string) contracts.Event {
	return ev(session, agent, "ledger.updated", contracts.LedgerUpdatedPayload{
		Goals: goals, Constraints: []string{}, Completed: []string{}, Pending: []string{},
	})
}
