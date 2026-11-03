package host

// F2：自动 gate 的 join 失败**不记账**，而"下次 Run 重试"这句话**是真的**——
// loopFor 的**缓存命中路径**会补一次自愈 join（见 loopFor 与 joinAutoGatesIfNeeded）。
//
// 修之前的病：loopFor 建成 loop → join 失败只 warn → 下次 Run 走**缓存命中**早返回，
// 于是那句"下次 Run 重试"是**假话**——失败的自动 gate 永远补不上（该 session 一直
// "暂不可见"，对端看不见它）。
//
// 夹具手法（内部测试，能造出"待补"的态）：
//
//	① 起初该 session **没有**自动 gate（非 git CWD + 无 run_id）→ 建 loop 时 join 是
//	   空操作，但**记账**（autoGates[sid]=true：join 尝试成功过）。
//	② 造出"上次 join 失败"的态：autoGates[sid]=false（失败不记账就是这个态），
//	   并给该 session 登记带 run_id 的归属 → 于是自动 gate 变成 workflow:<run_id>。
//	③ 下一次 loopFor（**缓存命中**路径）→ 补 join → 落一条 gate.joined，记账归位。

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/runtime"
)

// TestAutoGateJoinSelfHealOnCacheHit（F2）：缓存命中路径补 join（自愈）。
func TestAutoGateJoinSelfHealOnCacheHit(t *testing.T) {
	h := newForkTestHub(t, &forkTestProvider{}) // 非 git CWD → 起初没有自动 repo gate
	ctx := context.Background()
	sid := NewSession()

	// ① 建 loop（进缓存）：此时没有自动 gate → join 空操作，但记账。
	if _, _, err := h.loopFor(ctx, sid); err != nil {
		t.Fatalf("建 loop: %v", err)
	}
	h.mu.Lock()
	joined := h.autoGates[sid]
	h.mu.Unlock()
	if !joined {
		t.Fatal("前提破了：join 空操作也算成功，应记账 autoGates[sid]=true")
	}

	// ② 造出"上次 join 失败"的态 + 给该 session 一个 workflow 自动 gate。
	h.mu.Lock()
	h.autoGates[sid] = false
	h.specs[sid] = SessionSpec{RunID: "wf-selfheal"}
	h.mu.Unlock()

	// ③ 下一次 loopFor 走**缓存命中**路径 → 应补 join。
	if _, _, err := h.loopFor(ctx, sid); err != nil {
		t.Fatalf("缓存命中的 loopFor: %v", err)
	}

	// ③-a 记账归位（补上的 join 成功 → autoGates[sid]=true）。
	h.mu.Lock()
	joined = h.autoGates[sid]
	h.mu.Unlock()
	if !joined {
		t.Fatal("缓存命中路径应补一次 join 并记账（autoGates[sid] 应为 true）")
	}

	// ③-b 事件流里真的有那条 gate.joined（workflow:wf-selfheal）。
	events, err := runtime.ReplaySession(ctx, h.store, sid)
	if err != nil {
		t.Fatalf("回放事件流: %v", err)
	}
	sawJoin := false
	for i := range events {
		if events[i].Kind != "gate.joined" {
			continue
		}
		var p contracts.GateJoinedPayload
		if err := json.Unmarshal(events[i].Payload, &p); err != nil {
			t.Fatalf("gate.joined 载荷解不开: %v", err)
		}
		if p.GateID == "workflow:wf-selfheal" {
			sawJoin = true
		}
	}
	if !sawJoin {
		t.Fatalf("缓存命中路径应补落一条 workflow:wf-selfheal 的 gate.joined（自愈生效）")
	}
}
