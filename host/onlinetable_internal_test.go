package host

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// otEv 组一条 durable 事实事件（seq 由 store 分配；载荷原样 JSON）。
func otEv(kind string, sid, aid contracts.Ulid, payload any) contracts.Event {
	raw, _ := json.Marshal(payload)
	return contracts.Event{
		V:         1,
		ID:        core.NewUlid(),
		Kind:      kind,
		TS:        "2026-10-28T00:00:00Z",
		SessionID: sid,
		AgentID:   aid,
		Durable:   true,
		Payload:   raw,
	}
}

// TestOnlineTableSourceFiltersByGateAndExcludesSubagents：在线表数据源按 gate 过滤（K11/K15），
// 且**别 session 的 subagent 完全不出现**（负例）。直接喂事件 → 发现表 → 数据源，端到端验口径。
func TestOnlineTableSourceFiltersByGateAndExcludesSubagents(t *testing.T) {
	st, err := sqlite.Open(filepath.Join(t.TempDir(), "e.db"), sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	disc := discovery.New()
	feed := newDiscoveryFeed(st, disc)
	src := &discoveryOnlineSource{table: disc, feed: feed}

	a, b, c := core.NewUlid(), core.NewUlid(), core.NewUlid()
	ma, mb, mc := core.NewUlid(), core.NewUlid(), core.NewUlid()
	subOfB := core.NewUlid()

	evs := []contracts.Event{
		// a：repo:X
		otEv("user.prompt", a, ma, contracts.UserPromptPayload{Text: "a"}),
		otEv(discovery.KindGateJoined, a, ma, contracts.GateJoinedPayload{GateID: "repo:X", Source: contracts.GateJoinSourceAutoRepo}),
		// b：repo:X（与 a 相交）+ 一个 subagent（别 session 视角完全不可见）
		otEv("user.prompt", b, mb, contracts.UserPromptPayload{Text: "b"}),
		otEv(discovery.KindGateJoined, b, mb, contracts.GateJoinedPayload{GateID: "repo:X", Source: contracts.GateJoinSourceAutoRepo}),
		otEv(discovery.KindAgentSpawned, b, subOfB, contracts.AgentSpawnedPayload{
			ParentAgentID: mb, ChildAgentID: subOfB, AgentType: "scout", Depth: 2, SpawnRequestID: core.NewUlid(),
		}),
		// c：repo:Y（与 a 不相交）
		otEv("user.prompt", c, mc, contracts.UserPromptPayload{Text: "c"}),
		otEv(discovery.KindGateJoined, c, mc, contracts.GateJoinedPayload{GateID: "repo:Y", Source: contracts.GateJoinSourceAutoRepo}),
	}
	if _, err := st.Append(context.Background(), evs); err != nil {
		t.Fatalf("Append: %v", err)
	}
	disc.Activate(a)
	disc.Activate(b)
	disc.Activate(c)

	rows := src.OnlineRows(context.Background(), a)
	if len(rows) != 2 {
		t.Fatalf("a 的在线表应 2 行（self + peer B），得到 %d: %+v", len(rows), rows)
	}
	if !rows[0].Self || rows[0].SessionID != string(a) || rows[0].MainAgentID != string(ma) {
		t.Fatalf("第一行应是 self A（主 agent ma）: %+v", rows[0])
	}
	if rows[1].Self || rows[1].SessionID != string(b) || rows[1].MainAgentID != string(mb) {
		t.Fatalf("第二行应是 peer B 的**主 agent** mb: %+v", rows[1])
	}
	// 负例①：别 session 的 subagent 完全不出现。
	for _, r := range rows {
		if r.MainAgentID == string(subOfB) || r.SessionID == string(subOfB) {
			t.Fatalf("别 session 的 subagent 泄漏进在线表: %+v", r)
		}
	}
	// 负例②：gate 不相交的 C 不出现。
	for _, r := range rows {
		if r.SessionID == string(c) {
			t.Fatalf("gate 不相交的 C 不该出现在线表: %+v", r)
		}
	}

	// C 结束后（deactivate）不影响 a（C 本就不可见）；b 结束后 a 只剩自己。
	disc.Deactivate(b)
	rows = src.OnlineRows(context.Background(), a)
	if len(rows) != 1 || !rows[0].Self {
		t.Fatalf("b 结束后 a 的在线表应只剩自己: %+v", rows)
	}
}
