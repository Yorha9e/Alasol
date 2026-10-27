package durable_test

import (
	"context"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/durable"
)

// TestResumeOwnershipRecoversSameChild：resume fold 出 ownership，(parent, request_id) → 同一 child，
// 不新建；且 resume 本身不写任何 agent.spawned（只读它们）。
func TestResumeOwnershipRecoversSameChild(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	session := core.NewUlid()
	root := core.NewUlid()
	childA := core.NewUlid()
	childB := core.NewUlid()
	reqA := core.NewUlid()
	reqB := core.NewUlid()

	seed := []contracts.Event{
		evSpawned(session, root, childA, "worker", "be", 2, reqA),
		evSpawned(session, root, childB, "worker", "fe", 2, reqB),
		evAgentTerminal(session, childA, "agent.completed"), // A 已收敛
		// B 仍 running，且有一个未决 in-flight 调用
		evIntent(session, childB, "cB", "echo", contracts.ReplayKindSafe, nil),
	}
	if _, err := st.Append(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	spawnedBefore := st.countKind("agent.spawned")
	rp := newMockReplayer(st)
	rep, err := durable.Resume(ctx, st, session, durable.ResumeOptions{Replayer: rp})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	// ownership 记录两条，且 (root, reqA)→childA、(root, reqB)→childB。
	own := rep.Tree.Ownership()
	if len(own) != 2 {
		t.Fatalf("ownership = %d 条, want 2", len(own))
	}
	if got, ok := rep.Tree.ResolveChild(root, reqA); !ok || got != childA {
		t.Fatalf("ResolveChild(root, reqA) = %q (ok=%v), want %q（不新建）", got, ok, childA)
	}
	if got, ok := rep.Tree.ResolveChild(root, reqB); !ok || got != childB {
		t.Fatalf("ResolveChild(root, reqB) = %q (ok=%v), want %q（不新建）", got, ok, childB)
	}
	// 换一个不存在的 request → 找不到（不幻觉 child）。
	if _, ok := rep.Tree.ResolveChild(root, core.NewUlid()); ok {
		t.Fatal("不存在的 request_id 不应解析出 child")
	}

	// 树结构：A completed、B running；root 是根。
	if rep.Tree.Nodes[childA].Status != "completed" {
		t.Fatalf("childA status = %q, want completed", rep.Tree.Nodes[childA].Status)
	}
	if rep.Tree.Nodes[childB].Status != "running" {
		t.Fatalf("childB status = %q, want running", rep.Tree.Nodes[childB].Status)
	}
	if rep.Tree.Root != root {
		t.Fatalf("root = %q, want %q", rep.Tree.Root, root)
	}
	// running 节点只有 B（A 已收敛）。
	running := rep.Tree.RunningNodes()
	if len(running) != 1 || running[0].ID != childB {
		t.Fatalf("running 节点 = %+v, want [childB]", running)
	}

	// resume 不新建 child：不写任何 agent.spawned 事件（只读）。
	if st.countKind("agent.spawned") != spawnedBefore {
		t.Fatalf("resume 写了 agent.spawned：%d → %d（应只读）", spawnedBefore, st.countKind("agent.spawned"))
	}

	// 二次 resume：ownership 不变（幂等），仍解析到同一 child。
	rep2, err := durable.Resume(ctx, st, session, durable.ResumeOptions{Replayer: newMockReplayer(st)})
	if err != nil {
		t.Fatalf("Resume#2: %v", err)
	}
	if len(rep2.Tree.Ownership()) != 2 {
		t.Fatalf("二次 resume ownership = %d, want 2（不变）", len(rep2.Tree.Ownership()))
	}
	if got, _ := rep2.Tree.ResolveChild(root, reqA); got != childA {
		t.Fatalf("二次 resume ResolveChild(reqA) = %q, want %q", got, childA)
	}
}

// TestResumeTreeRootIsPrimaryAgent：无 spawn 的单 agent session，root = 唯一 agent。
func TestResumeTreeRootIsPrimaryAgent(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	session := core.NewUlid()
	agent := core.NewUlid()
	if _, err := st.Append(ctx, []contracts.Event{
		evIntent(session, agent, "c1", "echo", contracts.ReplayKindSafe, nil),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rep, err := durable.Resume(ctx, st, session, durable.ResumeOptions{Replayer: newMockReplayer(st)})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if rep.Tree.Root != agent {
		t.Fatalf("单 agent session root = %q, want %q", rep.Tree.Root, agent)
	}
	if len(rep.Tree.Ownership()) != 0 {
		t.Fatalf("单 agent session 不应有 ownership，得到 %d", len(rep.Tree.Ownership()))
	}
}
