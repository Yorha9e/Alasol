package agents_test

// core/agents 黑盒测试：类型注册（含 can_spawn 环检测）、三闸门、request_id 幂等、
// 可见性（兄弟不可见 / 父↔直系子可见）、终态不可逆、并发 Spawn、污点与 refs 核验。

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
)

// ---------------------------------------------------------------------------
// 辅助构造
// ---------------------------------------------------------------------------

func perm(rw, w contracts.ReadWriteLevel, e contracts.ExecLevel) *contracts.PermissionRequest {
	return &contracts.PermissionRequest{Read: rw, Write: w, Exec: e}
}

func fullPerm() *contracts.PermissionRequest {
	return perm(contracts.ReadWriteLevelFull, contracts.ReadWriteLevelFull, contracts.ExecLevelFull)
}

// ---------------------------------------------------------------------------
// can_spawn 环检测（K16/K17，陷阱16 第一层：注册时校验）
// ---------------------------------------------------------------------------

func TestRegisterDetectsSpawnCycle(t *testing.T) {
	reg := agents.NewTypeRegistry()
	// A → B → A 成环：第二个注册应被拒。
	if err := reg.Register(agents.AgentType{ID: "A", CanSpawn: []string{"B"}}); err != nil {
		t.Fatalf("注册 A 应成功: %v", err)
	}
	err := reg.Register(agents.AgentType{ID: "B", CanSpawn: []string{"A"}})
	if !errors.Is(err, agents.ErrSpawnCycle) {
		t.Fatalf("B→A 应触发环检测，得到 %v", err)
	}
	// 被拒后 B 不在注册表（不提交）。
	if _, ok := reg.Get("B"); ok {
		t.Fatal("被拒的 B 不应留在注册表")
	}
	// A 仍在。
	if _, ok := reg.Get("A"); !ok {
		t.Fatal("A 应仍在注册表")
	}
}

func TestRegisterDetectsSelfCycle(t *testing.T) {
	reg := agents.NewTypeRegistry()
	if err := reg.Register(agents.AgentType{ID: "A", CanSpawn: []string{"A"}}); !errors.Is(err, agents.ErrSpawnCycle) {
		t.Fatalf("自环 A→A 应被拒，得到 %v", err)
	}
}

func TestRegisterDetectsLongerCycle(t *testing.T) {
	reg := agents.NewTypeRegistry()
	// A→B→C→A：第三个注册才闭合环。
	for _, id := range []string{"A", "B"} {
		var cs []string
		switch id {
		case "A":
			cs = []string{"B"}
		case "B":
			cs = []string{"C"}
		}
		if err := reg.Register(agents.AgentType{ID: id, CanSpawn: cs}); err != nil {
			t.Fatalf("注册 %s 应成功: %v", id, err)
		}
	}
	if err := reg.Register(agents.AgentType{ID: "C", CanSpawn: []string{"A"}}); !errors.Is(err, agents.ErrSpawnCycle) {
		t.Fatalf("C→A 应闭合 A→B→C→A 环，得到 %v", err)
	}
}

func TestRegisterAcceptsDagAndRejectsDuplicate(t *testing.T) {
	reg := agents.NewTypeRegistry()
	if err := reg.Register(agents.AgentType{ID: "root", CanSpawn: []string{"worker"}}); err != nil {
		t.Fatalf("注册 root: %v", err)
	}
	if err := reg.Register(agents.AgentType{ID: "worker", CanSpawn: nil}); err != nil {
		t.Fatalf("注册 worker: %v", err)
	}
	// 无环 DAG 可再注册指向已注册类型的分支。
	if err := reg.Register(agents.AgentType{ID: "root2", CanSpawn: []string{"worker"}}); err != nil {
		t.Fatalf("注册 root2（共享 worker，无环）: %v", err)
	}
	// 重名拒绝。
	if err := reg.Register(agents.AgentType{ID: "root"}); !errors.Is(err, agents.ErrTypeExists) {
		t.Fatalf("重名应被拒，得到 %v", err)
	}
	// 空 ID 拒绝。
	if err := reg.Register(agents.AgentType{ID: "  "}); err == nil {
		t.Fatal("空 ID 应被拒")
	}
	// Resolve 不存在。
	if _, err := reg.Resolve("ghost"); !errors.Is(err, agents.ErrTypeNotFound) {
		t.Fatalf("Resolve ghost 应 ErrTypeNotFound，得到 %v", err)
	}
}

// ---------------------------------------------------------------------------
// 三闸门（K17）
// ---------------------------------------------------------------------------

// gatesRegistry 构造一棵可用于闸门测试的类型体系：
//
//	root  can_spawn=[mid]         perm=full
//	mid   can_spawn=[leaf]        perm=workspace/none/allowlist
//	leaf  can_spawn=[]            perm=none/none/none
//	greedy can_spawn=[]           perm=full（用于权限闸门拒绝）
func gatesRegistry(t *testing.T) *agents.TypeRegistry {
	t.Helper()
	reg := agents.NewTypeRegistry()
	types := []agents.AgentType{
		{ID: "root", CanSpawn: []string{"mid"}, PermissionRequest: fullPerm()},
		{ID: "mid", CanSpawn: []string{"leaf"}, PermissionRequest: perm(contracts.ReadWriteLevelWorkspace, contracts.ReadWriteLevelNone, contracts.ExecLevelAllowlist)},
		{ID: "leaf", CanSpawn: nil, PermissionRequest: perm(contracts.ReadWriteLevelNone, contracts.ReadWriteLevelNone, contracts.ExecLevelNone)},
		{ID: "greedy", CanSpawn: nil, PermissionRequest: fullPerm()},
	}
	for _, tp := range types {
		if err := reg.Register(tp); err != nil {
			t.Fatalf("注册 %s: %v", tp.ID, err)
		}
	}
	return reg
}

func TestSpawnAllGatesPass(t *testing.T) {
	reg := gatesRegistry(t)
	tree := agents.NewTree(agents.AgentType{ID: "root", CanSpawn: []string{"mid"}, PermissionRequest: fullPerm()},
		agents.WithTypes(reg), agents.WithMaxDepth(3))
	root := tree.Root()

	mid, err := tree.Spawn(root, "mid", "backend", core.NewUlid())
	if err != nil {
		t.Fatalf("root→mid 应过三闸门: %v", err)
	}
	if mid.Depth != 2 || mid.Type != "mid" || mid.Role != "backend" || mid.Status != agents.StatusRunning {
		t.Fatalf("mid 节点不符: %+v", mid)
	}
	if mid.ParentID == nil || *mid.ParentID != root {
		t.Fatalf("mid.ParentID 应指向 root: %+v", mid.ParentID)
	}

	leaf, err := tree.Spawn(mid.ID, "leaf", "scout", core.NewUlid())
	if err != nil {
		t.Fatalf("mid→leaf 应过三闸门: %v", err)
	}
	if leaf.Depth != 3 {
		t.Fatalf("leaf.Depth = %d，want 3", leaf.Depth)
	}
}

func TestGateDepthRejectsFourthLayer(t *testing.T) {
	reg := agents.NewTypeRegistry()
	// 一条可到底的链：a→b→c，每层 can_spawn 指下一层；maxDepth=3。
	for _, tp := range []agents.AgentType{
		{ID: "a", CanSpawn: []string{"b"}, PermissionRequest: fullPerm()},
		{ID: "b", CanSpawn: []string{"c"}, PermissionRequest: fullPerm()},
		{ID: "c", CanSpawn: []string{"c2"}, PermissionRequest: fullPerm()},
		{ID: "c2", CanSpawn: nil, PermissionRequest: fullPerm()},
	} {
		if err := reg.Register(tp); err != nil {
			t.Fatalf("注册 %s: %v", tp.ID, err)
		}
	}
	tree := agents.NewTree(agents.AgentType{ID: "a", CanSpawn: []string{"b"}, PermissionRequest: fullPerm()},
		agents.WithTypes(reg), agents.WithMaxDepth(3))
	root := tree.Root()

	b, err := tree.Spawn(root, "b", "", core.NewUlid())
	if err != nil {
		t.Fatalf("root→b: %v", err)
	}
	c, err := tree.Spawn(b.ID, "c", "", core.NewUlid())
	if err != nil {
		t.Fatalf("b→c: %v", err)
	}
	// c 在 depth 3，再派 → depth 4 > 3 → 拒绝。
	_, err = tree.Spawn(c.ID, "c2", "", core.NewUlid())
	if !errors.Is(err, agents.ErrDepthExceeded) {
		t.Fatalf("第 4 层应触发深度超限，得到 %v", err)
	}
}

func TestGateTypeRejectsUnreachable(t *testing.T) {
	reg := gatesRegistry(t)
	tree := agents.NewTree(agents.AgentType{ID: "root", CanSpawn: []string{"mid"}, PermissionRequest: fullPerm()},
		agents.WithTypes(reg), agents.WithMaxDepth(3))
	// root 只能派 mid；派 leaf（未在 root.can_spawn）→ 类型不可达。
	_, err := tree.Spawn(tree.Root(), "leaf", "", core.NewUlid())
	if !errors.Is(err, agents.ErrTypeUnreachable) {
		t.Fatalf("不可达类型应被拒，得到 %v", err)
	}
	// 未注册类型 → fail-close（Resolve 失败）。
	_, err = tree.Spawn(tree.Root(), "ghost", "", core.NewUlid())
	if !errors.Is(err, agents.ErrTypeNotFound) {
		t.Fatalf("未注册类型应 ErrTypeNotFound，得到 %v", err)
	}
}

func TestGatePermissionRejectsTooBroad(t *testing.T) {
	reg := agents.NewTypeRegistry()
	// root 只请求 workspace 读；child 请求 full 读 → 超出。
	for _, tp := range []agents.AgentType{
		{ID: "root", CanSpawn: []string{"child"}, PermissionRequest: perm(contracts.ReadWriteLevelWorkspace, contracts.ReadWriteLevelNone, contracts.ExecLevelNone)},
		{ID: "child", CanSpawn: nil, PermissionRequest: fullPerm()},
	} {
		if err := reg.Register(tp); err != nil {
			t.Fatalf("注册 %s: %v", tp.ID, err)
		}
	}
	tree := agents.NewTree(agents.AgentType{ID: "root", CanSpawn: []string{"child"}, PermissionRequest: perm(contracts.ReadWriteLevelWorkspace, contracts.ReadWriteLevelNone, contracts.ExecLevelNone)},
		agents.WithTypes(reg), agents.WithMaxDepth(3))
	_, err := tree.Spawn(tree.Root(), "child", "", core.NewUlid())
	if !errors.Is(err, agents.ErrPermissionTooBroad) {
		t.Fatalf("过宽权限应被拒，得到 %v", err)
	}
}

func TestCeilingNarrowsRootAndChildren(t *testing.T) {
	reg := agents.NewTypeRegistry()
	// 天花板 = workspace 读；root 请求 full → root 有效权限被收窄到 workspace。
	ceiling := perm(contracts.ReadWriteLevelWorkspace, contracts.ReadWriteLevelNone, contracts.ExecLevelNone)
	for _, tp := range []agents.AgentType{
		{ID: "root", CanSpawn: []string{"child"}, PermissionRequest: fullPerm()},
		{ID: "ws", CanSpawn: nil, PermissionRequest: perm(contracts.ReadWriteLevelWorkspace, contracts.ReadWriteLevelNone, contracts.ExecLevelNone)},
		{ID: "full", CanSpawn: nil, PermissionRequest: perm(contracts.ReadWriteLevelFull, contracts.ReadWriteLevelNone, contracts.ExecLevelNone)},
	} {
		if err := reg.Register(tp); err != nil {
			t.Fatalf("注册 %s: %v", tp.ID, err)
		}
	}
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"ws", "full"}, PermissionRequest: fullPerm()}
	tree := agents.NewTree(rootType, agents.WithTypes(reg), agents.WithMaxDepth(3), agents.WithCeiling(ceiling))
	root := tree.Root()

	// 请求 workspace（= 天花板）→ 过。
	if _, err := tree.Spawn(root, "ws", "", core.NewUlid()); err != nil {
		t.Fatalf("ws 应过（≤ 天花板）: %v", err)
	}
	// 请求 full 读 > 天花板 workspace → 拒（即使 root 自己请求的是 full，也被天花板压住）。
	_, err := tree.Spawn(root, "full", "", core.NewUlid())
	if !errors.Is(err, agents.ErrPermissionTooBroad) {
		t.Fatalf("full 应被天花板拒，得到 %v", err)
	}
}

// ---------------------------------------------------------------------------
// X5：request_id 幂等
// ---------------------------------------------------------------------------

func TestSpawnIdempotentByRequestID(t *testing.T) {
	reg := gatesRegistry(t)
	tree := agents.NewTree(agents.AgentType{ID: "root", CanSpawn: []string{"mid"}, PermissionRequest: fullPerm()},
		agents.WithTypes(reg), agents.WithMaxDepth(3))
	root := tree.Root()
	req := core.NewUlid()

	c1, err := tree.Spawn(root, "mid", "backend", req)
	if err != nil {
		t.Fatalf("首次 spawn: %v", err)
	}
	c2, err := tree.Spawn(root, "mid", "backend", req)
	if err != nil {
		t.Fatalf("重复 spawn 应幂等成功: %v", err)
	}
	if c1.ID != c2.ID {
		t.Fatalf("同 request_id 应返回同一 child: %s vs %s", c1.ID, c2.ID)
	}
	if kids := tree.Children(root); len(kids) != 1 {
		t.Fatalf("重复 spawn 不应新建节点，得到 %d 个 child", len(kids))
	}
}

// ---------------------------------------------------------------------------
// 可见性（K11/K27）
// ---------------------------------------------------------------------------

func visibilityTree(t *testing.T) (*agents.Tree, agents.Node) {
	t.Helper()
	reg := agents.NewTypeRegistry()
	for _, tp := range []agents.AgentType{
		{ID: "root", CanSpawn: []string{"mid"}, PermissionRequest: fullPerm()},
		{ID: "mid", CanSpawn: []string{"leaf"}, PermissionRequest: fullPerm()},
		{ID: "leaf", CanSpawn: nil, PermissionRequest: fullPerm()},
	} {
		if err := reg.Register(tp); err != nil {
			t.Fatalf("注册 %s: %v", tp.ID, err)
		}
	}
	tree := agents.NewTree(agents.AgentType{ID: "root", CanSpawn: []string{"mid"}, PermissionRequest: fullPerm()},
		agents.WithTypes(reg), agents.WithMaxDepth(4))
	root := tree.RootNode()
	return tree, root
}

func idsOf(nodes []agents.Node) map[contracts.Ulid]bool {
	m := make(map[contracts.Ulid]bool, len(nodes))
	for _, n := range nodes {
		m[n.ID] = true
	}
	return m
}

func TestVisibilitySiblingsInvisibleParentChildVisible(t *testing.T) {
	tree, root := visibilityTree(t)
	a, _ := tree.Spawn(root.ID, "mid", "a", core.NewUlid())
	b, _ := tree.Spawn(root.ID, "mid", "b", core.NewUlid())
	ga, _ := tree.Spawn(a.ID, "leaf", "ga", core.NewUlid()) // a 的直系子

	// 从 a：可见 = {a, root, ga}；b 不可见。
	visA := idsOf(tree.Visible(a.ID))
	if !visA[a.ID] || !visA[root.ID] || !visA[ga.ID] {
		t.Fatalf("a 应可见自己/父/直系子: %+v", tree.Visible(a.ID))
	}
	if visA[b.ID] {
		t.Fatal("兄弟 b 对 a 不可见")
	}
	// 从 b：可见 = {b, root}；a 与 ga 不可见。
	visB := idsOf(tree.Visible(b.ID))
	if !visB[b.ID] || !visB[root.ID] {
		t.Fatalf("b 应可见自己/父: %+v", tree.Visible(b.ID))
	}
	if visB[a.ID] || visB[ga.ID] {
		t.Fatal("a 及其子对 b 不可见")
	}
	// 从 root：可见 = {root, a, b}（直系子）；孙 ga 不可见。
	visRoot := idsOf(tree.Visible(root.ID))
	if !visRoot[root.ID] || !visRoot[a.ID] || !visRoot[b.ID] {
		t.Fatalf("root 应可见自己与两个直系子: %+v", tree.Visible(root.ID))
	}
	if visRoot[ga.ID] {
		t.Fatal("孙节点对 root 不可见（非直系子）")
	}
}

func TestAncestorsAlongParentChain(t *testing.T) {
	tree, root := visibilityTree(t)
	a, _ := tree.Spawn(root.ID, "mid", "a", core.NewUlid())
	ga, _ := tree.Spawn(a.ID, "leaf", "ga", core.NewUlid())

	anc := tree.Ancestors(ga.ID)
	if len(anc) != 2 || anc[0].ID != a.ID || anc[1].ID != root.ID {
		t.Fatalf("ga 的祖先链（近→远）应 = [a, root]: %+v", anc)
	}
	// 直系父。
	p, ok := tree.Parent(ga.ID)
	if !ok || p.ID != a.ID {
		t.Fatalf("ga 的父应为 a: %+v ok=%v", p, ok)
	}
	// root 无父。
	if _, ok := tree.Parent(root.ID); ok {
		t.Fatal("root 不应有父")
	}
}

// ---------------------------------------------------------------------------
// 终态不可逆
// ---------------------------------------------------------------------------

func TestCompleteTerminalIrreversible(t *testing.T) {
	reg := gatesRegistry(t)
	tree := agents.NewTree(agents.AgentType{ID: "root", CanSpawn: []string{"mid"}, PermissionRequest: fullPerm()},
		agents.WithTypes(reg), agents.WithMaxDepth(3))
	root := tree.Root()
	c, _ := tree.Spawn(root, "mid", "backend", core.NewUlid())

	sum := agents.Summary{Text: "做完了", Taint: agents.TaintClean}
	if err := tree.Complete(c.ID, sum); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	// 再次 Complete / Fail → 拒绝（不可逆）。
	if err := tree.Complete(c.ID, sum); !errors.Is(err, agents.ErrAlreadyTerminal) {
		t.Fatalf("重复 Complete 应 ErrAlreadyTerminal，得到 %v", err)
	}
	if err := tree.Fail(c.ID, errors.New("boom")); !errors.Is(err, agents.ErrAlreadyTerminal) {
		t.Fatalf("completed 后 Fail 应 ErrAlreadyTerminal，得到 %v", err)
	}
	// 终态父不能再派生。
	if _, err := tree.Spawn(c.ID, "leaf", "", core.NewUlid()); !errors.Is(err, agents.ErrParentTerminal) {
		t.Fatalf("completed 父再派生应 ErrParentTerminal，得到 %v", err)
	}
	// 状态确为 completed。
	n, _ := tree.Get(c.ID)
	if n.Status != agents.StatusCompleted {
		t.Fatalf("状态应为 completed，得到 %s", n.Status)
	}
}

func TestFailTerminalIrreversible(t *testing.T) {
	reg := gatesRegistry(t)
	tree := agents.NewTree(agents.AgentType{ID: "root", CanSpawn: []string{"mid"}, PermissionRequest: fullPerm()},
		agents.WithTypes(reg), agents.WithMaxDepth(3))
	c, _ := tree.Spawn(tree.Root(), "mid", "", core.NewUlid())

	if err := tree.Fail(c.ID, errors.New("卡住了")); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if err := tree.Fail(c.ID, errors.New("又失败")); !errors.Is(err, agents.ErrAlreadyTerminal) {
		t.Fatalf("重复 Fail 应 ErrAlreadyTerminal，得到 %v", err)
	}
	if err := tree.Complete(c.ID, agents.Summary{Taint: agents.TaintClean}); !errors.Is(err, agents.ErrAlreadyTerminal) {
		t.Fatalf("failed 后 Complete 应 ErrAlreadyTerminal，得到 %v", err)
	}
	// 不存在的节点。
	if err := tree.Complete(core.NewUlid(), agents.Summary{}); !errors.Is(err, agents.ErrNodeNotFound) {
		t.Fatalf("Complete 不存在节点应 ErrNodeNotFound，得到 %v", err)
	}
}

// ---------------------------------------------------------------------------
// 并发 Spawn
// ---------------------------------------------------------------------------

func TestConcurrentSpawnDistinctChildren(t *testing.T) {
	reg := agents.NewTypeRegistry()
	if err := reg.Register(agents.AgentType{ID: "root", CanSpawn: []string{"leaf"}, PermissionRequest: fullPerm()}); err != nil {
		t.Fatalf("注册 root: %v", err)
	}
	if err := reg.Register(agents.AgentType{ID: "leaf", CanSpawn: nil, PermissionRequest: fullPerm()}); err != nil {
		t.Fatalf("注册 leaf: %v", err)
	}
	tree := agents.NewTree(agents.AgentType{ID: "root", CanSpawn: []string{"leaf"}, PermissionRequest: fullPerm()},
		agents.WithTypes(reg), agents.WithMaxDepth(3))
	root := tree.Root()

	const n = 64
	var wg sync.WaitGroup
	ids := make([]contracts.Ulid, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			node, err := tree.Spawn(root, "leaf", "w", core.NewUlid())
			ids[i], errs[i] = node.ID, err
		}(i)
	}
	wg.Wait()

	seen := make(map[contracts.Ulid]bool, n)
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("第 %d 个 spawn 出错: %v", i, errs[i])
		}
		if seen[ids[i]] {
			t.Fatalf("并发 spawn 产生重复 child id: %s", ids[i])
		}
		seen[ids[i]] = true
	}
	if kids := tree.Children(root); len(kids) != n {
		t.Fatalf("应有 %d 个 child，得到 %d", n, len(kids))
	}
}

// ---------------------------------------------------------------------------
// 污点（Q7）与 refs 核验（K24）
// ---------------------------------------------------------------------------

func TestTaintLevelsAndMax(t *testing.T) {
	if !agents.IsTainted(agents.TaintUntrusted) {
		t.Fatal("untrusted 应判为 tainted")
	}
	if agents.IsTainted(agents.TaintClean) || agents.IsTainted("") {
		t.Fatal("clean / 空 不应判为 tainted")
	}
	// 只能升不能降。
	if got := agents.MaxTaint(agents.TaintClean, agents.TaintUntrusted); got != agents.TaintUntrusted {
		t.Fatalf("clean ⊕ untrusted 应升到 untrusted，得到 %q", got)
	}
	if got := agents.MaxTaint(agents.TaintUntrusted, agents.TaintClean); got != agents.TaintUntrusted {
		t.Fatalf("untrusted ⊕ clean 应保持 untrusted，得到 %q", got)
	}
	if got := agents.MaxTaint("", ""); got != agents.TaintClean {
		t.Fatalf("空 ⊕ 空 应归一为 clean，得到 %q", got)
	}
	// 未知级按最严（fail-close）。
	if got := agents.MaxTaint(agents.TaintUntrusted, "mystery"); got != "mystery" {
		t.Fatalf("未知级应按最严，得到 %q", got)
	}
}

func TestTaintSummaryInheritsAndOnlyEscalates(t *testing.T) {
	// 模型自称 clean，但继承 untrusted → 升到 untrusted。
	got := agents.TaintSummary(agents.Summary{Text: "x", Taint: agents.TaintClean}, agents.TaintUntrusted)
	if got.Taint != agents.TaintUntrusted {
		t.Fatalf("应继承 untrusted，得到 %q", got.Taint)
	}
	// 模型自称 untrusted，继承 clean → 保持 untrusted（不降）。
	got = agents.TaintSummary(agents.Summary{Text: "x", Taint: agents.TaintUntrusted}, agents.TaintClean)
	if got.Taint != agents.TaintUntrusted {
		t.Fatalf("不应降级，得到 %q", got.Taint)
	}
	// 全程 clean → clean。
	got = agents.TaintSummary(agents.Summary{Text: "x", Taint: ""}, "")
	if got.Taint != agents.TaintClean {
		t.Fatalf("应归一为 clean，得到 %q", got.Taint)
	}
}

// recordingVerifier 记录被核验的 refs，全部返回 true。
type recordingVerifier struct {
	mu   sync.Mutex
	seen []string
}

func (v *recordingVerifier) Verify(_ context.Context, ref string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.seen = append(v.seen, ref)
	return true
}

func TestVerifyRefsCallsVerifierPerRef(t *testing.T) {
	v := &recordingVerifier{}
	refs := []string{"e:12", "path/to/file.go"}
	results, allOK := agents.VerifyRefs(context.Background(), v, refs)
	if !allOK {
		t.Fatal("全部应核验通过")
	}
	if len(results) != 2 || !results[0] || !results[1] {
		t.Fatalf("results 不符: %v", results)
	}
	v.mu.Lock()
	got := append([]string(nil), v.seen...)
	v.mu.Unlock()
	if len(got) != 2 || got[0] != "e:12" || got[1] != "path/to/file.go" {
		t.Fatalf("verifier 收到的 refs 不符: %v", got)
	}

	// 无 verifier → 全部不可核验（fail-close）。
	results, allOK = agents.VerifyRefs(context.Background(), nil, refs)
	if allOK || results[0] || results[1] {
		t.Fatalf("无 verifier 应全部不可核验: results=%v allOK=%v", results, allOK)
	}
	// 空 refs → vacuously 通过。
	_, allOK = agents.VerifyRefs(context.Background(), v, nil)
	if !allOK {
		t.Fatal("空 refs 应 vacuously 通过")
	}
}

// ---------------------------------------------------------------------------
// X5 幂等键的作用域：**(parent_agent_id, spawn_request_id)**
// ---------------------------------------------------------------------------

// scopedRegistry 构造一棵有两个可 spawn 类型的体系（用于"同父同 request_id 换类型"）。
//
//	root  can_spawn=[mid, other]  perm=full
//	mid   can_spawn=[leaf]        perm=full
//	other can_spawn=[leaf]        perm=full
//	leaf  can_spawn=[]            perm=full
func scopedRegistry(t *testing.T) *agents.TypeRegistry {
	t.Helper()
	reg := agents.NewTypeRegistry()
	for _, tp := range []agents.AgentType{
		{ID: "root", CanSpawn: []string{"mid", "other"}, PermissionRequest: fullPerm()},
		{ID: "mid", CanSpawn: []string{"leaf"}, PermissionRequest: fullPerm()},
		{ID: "other", CanSpawn: []string{"leaf"}, PermissionRequest: fullPerm()},
		{ID: "leaf", CanSpawn: nil, PermissionRequest: fullPerm()},
	} {
		if err := reg.Register(tp); err != nil {
			t.Fatalf("注册 %s: %v", tp.ID, err)
		}
	}
	return reg
}

// TestSpawnIdempotencyIsScopedToParent：不同父用**同一个** request_id → 各自正常派生
// （全局索引会让第二个任务静默不执行、还拿到第一个 child）。
func TestSpawnIdempotencyIsScopedToParent(t *testing.T) {
	reg := scopedRegistry(t)
	tree := agents.NewTree(agents.AgentType{ID: "root", CanSpawn: []string{"mid", "other"}, PermissionRequest: fullPerm()},
		agents.WithTypes(reg), agents.WithMaxDepth(3))
	root := tree.Root()

	// 两个不同的父（root 的两个子）。
	m1, err := tree.Spawn(root, "mid", "backend", core.NewUlid())
	if err != nil {
		t.Fatalf("派生 mid: %v", err)
	}
	m2, err := tree.Spawn(root, "other", "frontend", core.NewUlid())
	if err != nil {
		t.Fatalf("派生 other: %v", err)
	}

	// 同一个 request_id，两个不同的父 → 各自派生（互不干扰）。
	req := core.NewUlid()
	c1, err := tree.Spawn(m1.ID, "leaf", "scout", req)
	if err != nil {
		t.Fatalf("m1 派生 leaf: %v", err)
	}
	c2, err := tree.Spawn(m2.ID, "leaf", "scout", req)
	if err != nil {
		t.Fatalf("m2 用同一个 request_id 派生 leaf: %v", err)
	}
	if c1.ID == c2.ID {
		t.Fatalf("不同父的 spawn 不应命中同一个 child: %s", c1.ID)
	}
	if len(tree.Children(m1.ID)) != 1 || len(tree.Children(m2.ID)) != 1 {
		t.Fatalf("两个父各应有一个 child: %d / %d", len(tree.Children(m1.ID)), len(tree.Children(m2.ID)))
	}
}

// TestSpawnRequestConflictOnDifferentType：同父 + 同 request_id + **换类型** → 冲突
// （不把别人的 child 交出去）。
func TestSpawnRequestConflictOnDifferentType(t *testing.T) {
	reg := scopedRegistry(t)
	tree := agents.NewTree(agents.AgentType{ID: "root", CanSpawn: []string{"mid", "other"}, PermissionRequest: fullPerm()},
		agents.WithTypes(reg), agents.WithMaxDepth(3))
	root := tree.Root()
	req := core.NewUlid()

	c1, err := tree.Spawn(root, "mid", "backend", req)
	if err != nil {
		t.Fatalf("首次 spawn: %v", err)
	}
	// 同父同 request_id 换类型 → ErrRequestConflict（不返回第一个 child）。
	_, err = tree.Spawn(root, "other", "backend", req)
	if !errors.Is(err, agents.ErrRequestConflict) {
		t.Fatalf("换类型复用 request_id 应 ErrRequestConflict，得到 %v", err)
	}
	// 原 child 不受影响（没有被替换）。
	again, err := tree.Spawn(root, "mid", "backend", req)
	if err != nil {
		t.Fatalf("同父同类型复用 request_id 应幂等: %v", err)
	}
	if again.ID != c1.ID {
		t.Fatalf("幂等命中应返回同一 child: %s vs %s", again.ID, c1.ID)
	}
	if len(tree.Children(root)) != 1 {
		t.Fatalf("冲突不应新建节点，得到 %d 个 child", len(tree.Children(root)))
	}
}

// TestPermOfReturnsEffectivePermission：节点的生效权限可查（运行期约束要用）。
func TestPermOfReturnsEffectivePermission(t *testing.T) {
	reg := gatesRegistry(t)
	tree := agents.NewTree(agents.AgentType{ID: "root", CanSpawn: []string{"mid"}, PermissionRequest: fullPerm()},
		agents.WithTypes(reg), agents.WithMaxDepth(3))
	root := tree.Root()

	p, ok := tree.PermOf(root)
	if !ok {
		t.Fatal("root 的生效权限应可查")
	}
	if p.Write != contracts.ReadWriteLevelFull || p.Exec != contracts.ExecLevelFull {
		t.Fatalf("root 生效权限 = %+v，want full/full", p)
	}
	mid, err := tree.Spawn(root, "mid", "backend", core.NewUlid())
	if err != nil {
		t.Fatalf("spawn mid: %v", err)
	}
	mp, ok := tree.PermOf(mid.ID)
	if !ok {
		t.Fatal("mid 的生效权限应可查")
	}
	// mid 的类型请求 = workspace/none/allowlist（闸门③：⊆ 父的 full）→ 生效权限 = 请求本身。
	if mp.Read != contracts.ReadWriteLevelWorkspace || mp.Write != contracts.ReadWriteLevelNone || mp.Exec != contracts.ExecLevelAllowlist {
		t.Fatalf("mid 生效权限 = %+v，want workspace/none/allowlist", mp)
	}
	if _, ok := tree.PermOf("ghost"); ok {
		t.Fatal("不存在的 agent 不应有生效权限")
	}
}
