// spawn 路径的**取消护栏**与**子节点收口**钉子（残留观察 1：settle ctx 让
// "ctx 已死 ⇒ intent 落盘失败 ⇒ 绝不执行"这条隐式护栏在 spawn 路径上同样失效）。
//
// 两个场景：
//
//   - A：ctx 在 provider 返回 spawn 调用那一刻死 → **不派生**（0 次 Spawn：树里没有子
//     节点），但审计留痕（intent + failed，写明"因 Run 上下文取消/超时未发起"）。
//   - B：spawn 已发起（子节点已建）、子 loop 跑着跑着 ctx 死了 → 子节点必须**收口**
//     （failed + agent.failed 事件），不能停在 running（树里留下常驻僵尸节点）。
package loop_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/loop"
)

// spawnProvider 是多 agent 用例的脚本化 provider：
//
//   - root 轮：按 cancelOnReturn 决定是否在**返回 spawn 调用那一刻**取消 ctx；
//   - 子轮（scout-model）：等 ctx 结束（模拟子 agent 干活途中 Run 被取消/超时）。
type spawnProvider struct {
	mu       sync.Mutex
	cancel   context.CancelFunc // 非 nil：root 返回 spawn 调用时取消 ctx
	reqID    contracts.Ulid
	rootSeen int
}

func (p *spawnProvider) Run(ctx context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	if req.Model == maRootModel {
		p.mu.Lock()
		p.rootSeen++
		p.mu.Unlock()
		if p.cancel != nil {
			p.cancel() // ctx 在工具调用交付前就死了
		}
		return core.ModelResult{
			Text: "派个子 agent",
			ToolCalls: []core.ToolCallReq{{
				ID:   contracts.ProviderId("spawn1"),
				Name: loop.ToolSpawn,
				Args: spawnArgs("scout", "recon", "读一下 README 并总结", p.reqID),
			}},
		}, nil
	}
	// 子轮：等 ctx 结束（Run 被取消/超时 → 子 agent 半途夭折）。
	<-ctx.Done()
	return core.ModelResult{}, ctx.Err()
}

var _ core.Provider = (*spawnProvider)(nil)

// newSpawnGuardHarness 装一个多 agent 夹具：root 可派生 scout，store 尊重 ctx
// （对齐 sqlite 实测行为——ctx 死了只有 settle ctx 能把审计事件落下来）。
func newSpawnGuardHarness(t *testing.T, ctxAware bool) *maHarness {
	t.Helper()
	rootType := agents.AgentType{
		ID: "root", CanSpawn: []string{"scout"}, Tools: nil,
		Model: maRootModel, PermissionRequest: permFull(),
	}
	h := newMAHarness(t, rootType, 3, allowRule("read"))
	h.registerType(agents.AgentType{
		ID: "scout", CanSpawn: nil, Tools: []string{"read"},
		Model: "scout-model", PermissionRequest: permRO(),
	})
	h.addTool("read", nil)
	if ctxAware {
		// 换成尊重 ctx 的 store：否则旧 bug 在 memStore 下复现不了（intent 照样落盘）。
		ms := newMemStore()
		h.bus = bus.New(&ctxAwareStore{memStore: ms})
		h.store = &ctxAwareStore{memStore: ms}
		h.allFn = ms.all
	}
	return h
}

// TestMASpawnAfterCtxCancelledNeverCreatesChild（A 钉子）：ctx 在 provider 返回 spawn
// 调用那一刻死 →
//
//	① **0 次 Spawn**（树里没有子节点：ctx 已死 = 不许再有副作用）；
//	② 审计留痕：tool.call.intent + tool.call.failed 一对（failed 写明"未发起"），
//	   **无** agent.spawned；
//	③ Run 按取消语义收场（context.Canceled）。
//
// 变异自证：去掉 spawn 发起前的 ctx.Err() 检查 → ①/② 全红（子节点被建出来 +
// agent.spawned 出现）。
func TestMASpawnAfterCtxCancelledNeverCreatesChild(t *testing.T) {
	h := newSpawnGuardHarness(t, true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.customProv = &spawnProvider{cancel: cancel, reqID: core.NewUlid()}

	_, err := h.rootLoop(10).Run(ctx, "拆个任务")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v，want context.Canceled（ctx 已死 = 取消收场）", err)
	}

	// ① 护栏：ctx 已死 → 不派生（0 次 Spawn = 树里没有子节点）。
	kids := h.tree.Children(h.rootID)
	if len(kids) != 0 {
		t.Fatalf("ctx 已死后仍派生了 %d 个子 agent（必须 0：取消/超时后不许再有副作用）", len(kids))
	}

	// ② 审计留痕：intent + failed 一对，无 agent.spawned。
	if got := len(h.byKind(loop.KindAgentSpawned)); got != 0 {
		t.Fatalf("ctx 已死后仍落了 %d 条 agent.spawned（必须 0：根本没派生）", got)
	}
	failed := h.byKind(loop.KindToolCallFailed)
	if len(failed) != 1 {
		t.Fatalf("tool.call.failed 条数 = %d，want 1（审计要留痕）", len(failed))
	}
	msg := decodeFailed(t, failed[0]).Error.Message
	if !strings.Contains(msg, "因 Run 上下文取消/超时未发起") {
		t.Fatalf("failed 文案应写明「因 Run 上下文取消/超时未发起」: %q", msg)
	}
	if !strings.Contains(msg, "未发起") {
		t.Fatalf("failed 文案应写明未发起: %q", msg)
	}
	intents := h.byKind(loop.KindToolCallIntent)
	if len(intents) != 1 {
		t.Fatalf("tool.call.intent 条数 = %d，want 1", len(intents))
	}
}

// TestMASpawnCancelledMidRunClosesChildNode（B 钉子）：spawn 已发起（子节点已建）、
// 子 loop 跑到一半 Run ctx 超时 → 子节点必须**收口**（failed），不能停在 running
// （停在 running = 树里留下常驻僵尸节点，事后没人说得清它怎么了）。
//
// 收口按 agent 生命周期的既有惯例：agent.failed 事件（agent_id=子）+ Tree.Fail；
// 再补一条 tool.call.failed 留痕。
//
// 变异自证：去掉取消路径里的收口（agent.failed + Tree.Fail）→ ① 全红（节点仍 running）。
func TestMASpawnCancelledMidRunClosesChildNode(t *testing.T) {
	h := newSpawnGuardHarness(t, true)

	// 子轮会等 ctx 结束 → 用短超时把 Run 打断（子 agent 半途夭折）。
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	h.customProv = &spawnProvider{reqID: core.NewUlid()} // 不在 root 轮取消：让 spawn 真的发起

	_, err := h.rootLoop(10).Run(ctx, "拆个任务")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run err = %v，want context.DeadlineExceeded（Run ctx 超时收场）", err)
	}

	// ① 子节点必须收口：存在、且**不是** running。
	kids := h.tree.Children(h.rootID)
	if len(kids) != 1 {
		t.Fatalf("子节点数 = %d，want 1（spawn 已发起，子节点应存在）", len(kids))
	}
	child := kids[0]
	if child.Status != agents.StatusFailed {
		t.Fatalf("子节点状态 = %q，want %q（ctx 取消后不许停在 running：必须收口）", child.Status, agents.StatusFailed)
	}
	if _, ok := h.tree.FailReason(child.ID); !ok {
		t.Fatal("收口后应有失败原因（FailReason）")
	}

	// ② 收口留痕：agent.failed（agent_id=子）+ tool.call.failed。
	agentFailed := h.byKind(loop.KindAgentFailed)
	if len(agentFailed) != 1 {
		t.Fatalf("agent.failed 条数 = %d，want 1（子节点收口要留痕）", len(agentFailed))
	}
	if got := decodeAgentFailed(t, agentFailed[0]).AgentID; got != child.ID {
		t.Fatalf("agent.failed.agent_id = %q，want %q（事件归属子 agent）", got, child.ID)
	}
	failed := h.byKind(loop.KindToolCallFailed)
	if len(failed) != 1 {
		t.Fatalf("tool.call.failed 条数 = %d，want 1", len(failed))
	}
	if msg := decodeFailed(t, failed[0]).Error.Message; !strings.Contains(msg, "spawn 被取消") {
		t.Fatalf("failed 文案应写明 spawn 被取消: %q", msg)
	}
}
