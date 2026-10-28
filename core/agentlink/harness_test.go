package agentlink_test

// core/agentlink 的单测世界（K18 五件套）。**无 IO**：内存事件库 + 内存发现表 +
// 同一份 agentlink.Link——五件套与端到端核验全在进程内跑完。
//
// 世界形状（K11 的两个维度都要有用例）：
//
//	session A：主 amA；子 childA1（其子 grandA1）；兄弟 siblingA2（childA1 的兄弟）
//	session B：主 amB；子 subB1（**别 session 的 subagent**）
//	session C：主 amC（在另一个 gate，与 A/B 不相交）
//	gate repo:X = {A, B}（两者都存活）；gate repo:Y = {C}
//
// 于是：A↔B 水平可见（主 agent 粒度）；A 内部父子可见、兄弟不可见；C 与 A/B 互不可见；
// B 的 subB1 对 A 完全不可见（别 session 的 subagent）。

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/discovery"
)

// memStore 是内存事件库（core.Store 的最小实象）：seq 单调分配、按 Event.ID 幂等
// （重复 ID 返回原行——与 sqlite 存储同口径，S7）。
type memStore struct {
	mu   sync.Mutex
	seq  int64
	byID map[contracts.Ulid]contracts.Event
}

func newMemStore() *memStore { return &memStore{byID: map[contracts.Ulid]contracts.Event{}} }

func (s *memStore) Append(_ context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]contracts.Event, 0, len(evs))
	for _, ev := range evs {
		if !ev.Durable {
			return nil, fmt.Errorf("memStore: 拒绝非 durable 事件（F8）: %s", ev.ID)
		}
		if prev, ok := s.byID[ev.ID]; ok {
			out = append(out, prev) // 幂等：同一 ID 不重复入库，返回原行（含原 seq）
			continue
		}
		s.seq++
		ev.Seq = ptrInt64(s.seq)
		s.byID[ev.ID] = ev
		out = append(out, ev)
	}
	return out, nil
}

func (s *memStore) ReadFrom(_ context.Context, sessionID *contracts.Ulid, fromSeq int64, limit int) ([]contracts.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := make([]contracts.Event, 0, len(s.byID))
	for _, ev := range s.byID {
		if ev.Seq == nil || *ev.Seq < fromSeq {
			continue
		}
		if sessionID != nil && ev.SessionID != *sessionID {
			continue
		}
		all = append(all, ev)
	}
	sort.SliceStable(all, func(i, j int) bool {
		return *all[i].Seq < *all[j].Seq
	})
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

// replace 就地替换一条已入库事件（测试内改写 payload 用：memStore 是测试夹具，
// 生产里事件不可改——F6 历史不重写）。
func (s *memStore) replace(ev contracts.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.byID[ev.ID]; ok {
		ev.Seq = prev.Seq
	}
	s.byID[ev.ID] = ev
}

// all 返回全部事件（按 seq 升序；测试断言用）。
func (s *memStore) all() []contracts.Event {
	evs, _ := s.ReadFrom(context.Background(), nil, 1, 0)
	return evs
}

// byKind 按 kind 过滤（测试断言用）。
func (s *memStore) byKind(kind string) []contracts.Event {
	var out []contracts.Event
	for _, ev := range s.all() {
		if ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

func ptrInt64(v int64) *int64 { return &v }

func ptrStr(s string) *string { return &s }

// world 是一次测试的世界（store + 发现表 + link + 各路身份）。
type world struct {
	t     *testing.T
	st    *memStore
	table *discovery.Table
	link  *agentlink.Link

	// 三 session 与它们的 agent
	sidA, sidB, sidC             contracts.Ulid
	amA, childA1, grandA1, sibA2 contracts.Ulid
	amB, subB1                   contracts.Ulid
	amC                          contracts.Ulid
	gateX, gateY                 string
}

// newWorld 建世界并喂发现表（拉式扫描：与 host/discovery.go 同口径，core 侧用最小实现）。
func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{
		t: t, st: newMemStore(), table: discovery.New(),
		sidA: core.NewUlid(), sidB: core.NewUlid(), sidC: core.NewUlid(),
		amA: core.NewUlid(), childA1: core.NewUlid(), grandA1: core.NewUlid(), sibA2: core.NewUlid(),
		amB: core.NewUlid(), subB1: core.NewUlid(), amC: core.NewUlid(),
		gateX: "repo:X", gateY: "repo:Y",
	}
	// session A：主 agent 先露面（user.prompt），再派生子 agent。
	w.raw(w.sidA, w.amA, "user.prompt", contracts.UserPromptPayload{Text: "A 的开场"})
	w.raw(w.sidA, w.amA, discovery.KindAgentSpawned, contracts.AgentSpawnedPayload{
		ParentAgentID: w.amA, ChildAgentID: w.childA1, AgentType: "scout", Role: "recon", Depth: 2, SpawnRequestID: core.NewUlid(),
	})
	w.raw(w.sidA, w.childA1, discovery.KindAgentSpawned, contracts.AgentSpawnedPayload{
		ParentAgentID: w.childA1, ChildAgentID: w.grandA1, AgentType: "worker", Role: "impl", Depth: 3, SpawnRequestID: core.NewUlid(),
	})
	w.raw(w.sidA, w.amA, discovery.KindAgentSpawned, contracts.AgentSpawnedPayload{
		ParentAgentID: w.amA, ChildAgentID: w.sibA2, AgentType: "scout", Role: "recon2", Depth: 2, SpawnRequestID: core.NewUlid(),
	})
	w.gate(w.sidA, w.amA, w.gateX)

	// session B：主 agent + 一个 subagent（别 session 的 subagent 用例）。
	w.raw(w.sidB, w.amB, "user.prompt", contracts.UserPromptPayload{Text: "B 的开场"})
	w.raw(w.sidB, w.amB, discovery.KindAgentSpawned, contracts.AgentSpawnedPayload{
		ParentAgentID: w.amB, ChildAgentID: w.subB1, AgentType: "scout", Role: "peer", Depth: 2, SpawnRequestID: core.NewUlid(),
	})
	w.gate(w.sidB, w.amB, w.gateX)

	// session C：只有主 agent，在另一个 gate（与 A/B 不相交）。
	w.raw(w.sidC, w.amC, "user.prompt", contracts.UserPromptPayload{Text: "C 的开场"})
	w.gate(w.sidC, w.amC, w.gateY)

	w.rescan()
	w.table.Activate(w.sidA)
	w.table.Activate(w.sidB)
	w.table.Activate(w.sidC)

	w.link = agentlink.New(agentlink.Deps{Table: w.table, Reader: w.st, Sink: w.st})
	return w
}

// raw 追加一条 durable 事件（载荷 JSON 序列化失败即测试失败）。
func (w *world) raw(session, agent contracts.Ulid, kind string, payload any) contracts.Event {
	w.t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		w.t.Fatalf("序列化 %s payload: %v", kind, err)
	}
	ev := contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: kind,
		TS:        time.Now().UTC().Format(time.RFC3339),
		SessionID: session, AgentID: agent, Durable: true, Payload: b,
	}
	out, err := w.st.Append(context.Background(), []contracts.Event{ev})
	if err != nil {
		w.t.Fatalf("append %s: %v", kind, err)
	}
	return out[0]
}

// gate 追加入一条 gate.joined（source=explicit）。
func (w *world) gate(session, agent contracts.Ulid, gateID string) contracts.Event {
	return w.raw(session, agent, discovery.KindGateJoined, contracts.GateJoinedPayload{
		GateID: gateID, Source: contracts.GateJoinSourceExplicit,
	})
}

// rescan 把事件流全量扫进发现表（重启重建的最小实现：Reset + 从 seq 1 重放）。
func (w *world) rescan() {
	w.table.Reset()
	for _, ev := range w.st.all() {
		w.table.Apply(&ev)
	}
}

// agentsOf 返回某 session 的 agent id 列表（按首次见到顺序）。
func (w *world) agentsOf(session contracts.Ulid) []contracts.Ulid {
	var out []contracts.Ulid
	for _, a := range w.table.Agents(session) {
		out = append(out, contracts.Ulid(a.AgentID))
	}
	return out
}

// ids 把 ULID 列表转成字符串集合（断言好用）。
func ids(list []contracts.Ulid) map[string]bool {
	out := map[string]bool{}
	for _, x := range list {
		out[string(x)] = true
	}
	return out
}
