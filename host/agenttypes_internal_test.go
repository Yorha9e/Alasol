package host

// 白盒测 v3-S6-B host 多 agent 装配：agent 类型注册表装配（内建五类 + TOML 自定义 + 校验）+
// W6-A 的 ChildStrategy 装配钩子（tiered 绑 child.ID / share_project_assets / 未实现 kind fail-close）。

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/compaction"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// ---------------------------------------------------------------------------
// agent 类型注册表装配（host/agenttypes.go）
// ---------------------------------------------------------------------------

func TestBuildAgentTypeRegistryBuiltins(t *testing.T) {
	reg, ids, err := buildAgentTypeRegistry("main-model", nil)
	if err != nil {
		t.Fatalf("buildAgentTypeRegistry: %v", err)
	}
	// 五类内建，root 不注册（CanSpawn=全部会自环）。
	if len(ids) != 5 {
		t.Fatalf("应有 5 个内建类型，得到 %d: %v", len(ids), ids)
	}
	for _, id := range []string{"planner", "coder", "critic", "reviewer", "observer"} {
		if _, ok := reg.Get(id); !ok {
			t.Errorf("内建类型 %q 应在注册表", id)
		}
	}
	if _, ok := reg.Get("root"); ok {
		t.Error("root 不应进注册表（CanSpawn=全部已注册 id 会自环被拒）")
	}

	// observer = K20 只读型：tools 只 read、permission 全 none/read=workspace、can_spawn 空。
	obs, _ := reg.Get("observer")
	if len(obs.CanSpawn) != 0 {
		t.Errorf("observer can_spawn 应为空（只读叶子，更深层类型层面不可达）: %v", obs.CanSpawn)
	}
	if len(obs.Tools) != 1 || obs.Tools[0] != "read" {
		t.Errorf("observer tools 应只 read: %v", obs.Tools)
	}
	if obs.PermissionRequest == nil ||
		obs.PermissionRequest.Read != contracts.ReadWriteLevelWorkspace ||
		obs.PermissionRequest.Write != contracts.ReadWriteLevelNone ||
		obs.PermissionRequest.Exec != contracts.ExecLevelNone {
		t.Errorf("observer 应只读（read=workspace 其余 none）: %+v", obs.PermissionRequest)
	}

	// planner 可派 observer；model="default" 解析成主模型。
	pl, _ := reg.Get("planner")
	if len(pl.CanSpawn) != 1 || pl.CanSpawn[0] != "observer" {
		t.Errorf("planner can_spawn 应 = [observer]: %v", pl.CanSpawn)
	}
	if pl.Model != "main-model" {
		t.Errorf("planner model 的 default sentinel 应解析成主模型: %q", pl.Model)
	}
	// coder 是叶子、能跑 bash（exec=allowlist）。
	cd, _ := reg.Get("coder")
	if len(cd.CanSpawn) != 0 {
		t.Errorf("coder 应是叶子: %v", cd.CanSpawn)
	}
	if cd.PermissionRequest == nil || cd.PermissionRequest.Exec != contracts.ExecLevelAllowlist {
		t.Errorf("coder 应可执行（exec=allowlist）: %+v", cd.PermissionRequest)
	}
}

func TestBuildAgentTypeRegistryCustomOverride(t *testing.T) {
	custom := []contracts.AgentType{
		{ID: "coder", Prompt: "自定义 coder：只读不改", Tools: []string{"read"}, Model: "default"},                        // 覆盖内建
		{ID: "auditor", Prompt: "审计员", Tools: []string{"read"}, Model: "default", CanSpawn: []string{"observer"}}, // 新增
	}
	reg, ids, err := buildAgentTypeRegistry("main-model", custom)
	if err != nil {
		t.Fatalf("buildAgentTypeRegistry: %v", err)
	}
	// 同名覆盖内建（用户自定义优先）。
	coder, _ := reg.Get("coder")
	if coder.Prompt != "自定义 coder：只读不改" {
		t.Errorf("TOML 应覆盖内建 coder 的 prompt: %q", coder.Prompt)
	}
	if len(coder.Tools) != 1 || coder.Tools[0] != "read" {
		t.Errorf("覆盖后 coder tools 应只 read: %v", coder.Tools)
	}
	// 新增类型在册。
	if _, ok := reg.Get("auditor"); !ok {
		t.Error("自定义 auditor 应在注册表")
	}
	// id 数 = 5 内建 + 1 新增 = 6（coder 被覆盖不重复计数）。
	if len(ids) != 6 {
		t.Errorf("应有 6 个类型（5 内建 + auditor），得到 %d: %v", len(ids), ids)
	}
}

func TestBuildAgentTypeRegistryRejects(t *testing.T) {
	cases := []struct {
		name    string
		custom  []contracts.AgentType
		wantSub string
	}{
		{"空 id", []contracts.AgentType{{ID: "  ", Prompt: "x"}}, "不能为空"},
		{"保留名 root", []contracts.AgentType{{ID: "root", Prompt: "x"}}, "保留名"},
		{"重名", []contracts.AgentType{{ID: "a", Prompt: "x"}, {ID: "a", Prompt: "y"}}, "重名"},
		{"悬空", []contracts.AgentType{{ID: "a", CanSpawn: []string{"ghost"}}}, "悬空"},
		{"自环", []contracts.AgentType{{ID: "a", CanSpawn: []string{"a"}}}, "自环"},
		{"成环", []contracts.AgentType{{ID: "a", CanSpawn: []string{"b"}}, {ID: "b", CanSpawn: []string{"a"}}}, "环"},
		{"坏 kind", []contracts.AgentType{{ID: "a", ContextStrategy: &contracts.ContextStrategy{Kind: "bogus"}}}, "枚举"},
		{"空 kind", []contracts.AgentType{{ID: "a", ContextStrategy: &contracts.ContextStrategy{Kind: ""}}}, "为空"},
		{"坏 perm read", []contracts.AgentType{{ID: "a", PermissionRequest: &contracts.PermissionRequest{Read: "bogus"}}}, "不合法"},
		{"坏 perm exec", []contracts.AgentType{{ID: "a", PermissionRequest: &contracts.PermissionRequest{Exec: "bogus"}}}, "不合法"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := buildAgentTypeRegistry("m", tc.custom)
			if err == nil {
				t.Fatalf("应拒绝（%s）", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("错误应点名 %q，得到: %v", tc.wantSub, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// W6-A ChildStrategy 装配钩子（host/compaction.go）
// ---------------------------------------------------------------------------

// childStrategyHub 造一个带 tiered + 规则的最小 Hub（供钩子测试）。
func childStrategyHub(t *testing.T, rules string) (*Hub, contracts.Ulid) {
	t.Helper()
	st := openCompactionStore(t)
	ix, err := sqlite.NewIndexer(st, 0)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	session := core.NewUlid()
	h := &Hub{
		cfg:      Config{ContextStrategy: "tiered", ContextStrategyWindow: 256_000, ContextRules: rules, Model: "m"},
		store:    st,
		indexer:  ix,
		bus:      bus.New(st),
		provider: nopProvider{},
	}
	return h, session
}

func TestChildStrategyFactoryTieredRules(t *testing.T) {
	h, session := childStrategyHub(t, "[项目规则] 遵循 AGENTS.md")
	hook := h.childStrategyFactory(session)
	tiered := &contracts.ContextStrategy{Kind: contracts.ContextStrategyKindTiered}
	yes, no := true, false

	// share=true → 注入项目规则（context.assembled 出现 rules 来源 + 消息含规则文本）。
	fShare := hook(agents.Node{ID: core.NewUlid(), Depth: 2, Type: "x"},
		agents.AgentType{ID: "x", Model: "m", ContextStrategy: tiered, ShareProjectAssets: &yes})
	if fShare == nil {
		t.Fatal("tiered 类型应产出工厂")
	}
	out, err := fShare().Assemble(context.Background(), []core.Message{{Role: "user", Content: "开工"}}, 1)
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	var hasRules bool
	for _, m := range out.Messages {
		if strings.Contains(m.Content, "遵循 AGENTS.md") {
			hasRules = true
		}
	}
	if !hasRules {
		t.Errorf("share_project_assets=true 应注入项目规则: %+v", out.Messages)
	}
	var rulesPart bool
	for _, p := range out.Parts {
		if p.Source == "rules" {
			rulesPart = true
		}
	}
	if !rulesPart {
		t.Errorf("parts 应出现 rules 来源: %+v", out.Parts)
	}

	// share=false → 不注入。
	fNo := hook(agents.Node{ID: core.NewUlid(), Depth: 2, Type: "x"},
		agents.AgentType{ID: "x", Model: "m", ContextStrategy: tiered, ShareProjectAssets: &no})
	out2, err := fNo().Assemble(context.Background(), []core.Message{{Role: "user", Content: "开工"}}, 1)
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	for _, m := range out2.Messages {
		if strings.Contains(m.Content, "遵循 AGENTS.md") {
			t.Errorf("share_project_assets=false 不应注入项目规则: %q", m.Content)
		}
	}
}

// TestChildStrategyFactoryBindsChildID：tiered 工厂产出的引擎把 agent_id 绑 child.ID——
// 触发一次降级压缩，context.compacted 事件的归属（AgentID）应是 child.ID（recall 视角隔离 +
// 事件归属按子 agent 走，W6-A）。
func TestChildStrategyFactoryBindsChildID(t *testing.T) {
	h, session := childStrategyHub(t, "")
	hook := h.childStrategyFactory(session)
	childID := core.NewUlid()
	tiered := &contracts.ContextStrategy{Kind: contracts.ContextStrategyKindTiered}
	f := hook(agents.Node{ID: childID, Depth: 2, Type: "x"},
		agents.AgentType{ID: "x", Model: "m", ContextStrategy: tiered})
	if f == nil {
		t.Fatal("tiered 类型应产出工厂")
	}
	engine, ok := f().(*compaction.Engine)
	if !ok {
		t.Fatalf("工厂应产 *compaction.Engine，得到 %T", f())
	}
	// 三个阶段的 Pattern：最老阶段（number=1）相对当前（number=3）aging 到 l1 → 降级 →
	// 触发 summarizer + context.compacted 事件（AgentID 应 = childID）。
	ctx := context.Background()
	hist := []core.Message{{Role: "user", Content: "a"}}
	if _, err := engine.Assemble(ctx, hist, 1); err != nil {
		t.Fatalf("Assemble 1: %v", err)
	}
	engine.MarkBoundary()
	hist = append(hist, core.Message{Role: "user", Content: "b"})
	if _, err := engine.Assemble(ctx, hist, 2); err != nil {
		t.Fatalf("Assemble 2: %v", err)
	}
	engine.MarkBoundary()
	hist = append(hist, core.Message{Role: "user", Content: "c"})
	if _, err := engine.Assemble(ctx, hist, 3); err != nil {
		t.Fatalf("Assemble 3: %v", err)
	}
	// 读事件库：应有归属 child.ID 的 context.compacted。
	events, err := h.store.ReadFrom(ctx, &session, 1, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	var sawChildCompacted bool
	for _, ev := range events {
		if ev.Kind == compaction.KindContextCompacted && ev.AgentID == childID {
			sawChildCompacted = true
		}
	}
	if !sawChildCompacted {
		t.Fatalf("子 agent 的 context.compacted 事件应归属 child.ID=%s（agent_id 绑 child，W6-A）", childID)
	}
}

func TestChildStrategyFactoryUnimplementedKind(t *testing.T) {
	h, session := childStrategyHub(t, "")
	hook := h.childStrategyFactory(session)
	// 枚举内但宿主未实现的 kind → 返回 nil 工厂（core fail-close 收口成 agent.failed，不静默降级）。
	for _, kind := range []contracts.ContextStrategyKind{
		contracts.ContextStrategyKindStageScoped,
		contracts.ContextStrategyKindSlidingWindow,
	} {
		f := hook(agents.Node{ID: core.NewUlid(), Depth: 2, Type: "x"},
			agents.AgentType{ID: "x", Model: "m", ContextStrategy: &contracts.ContextStrategy{Kind: kind}})
		if f != nil {
			t.Errorf("宿主未实现的 kind %q 应返回 nil 工厂（core fail-close）", kind)
		}
	}
	// append 不会被调（core 的 childStrategyFor 已拦）；这里防御性验证：显式传 append 也返回 nil。
	fAppend := hook(agents.Node{ID: core.NewUlid(), Depth: 2, Type: "x"},
		agents.AgentType{ID: "x", Model: "m", ContextStrategy: &contracts.ContextStrategy{Kind: contracts.ContextStrategyKindAppend}})
	if fAppend != nil {
		t.Error("append 应返回 nil 工厂（不造工厂）")
	}
}

// TestObserverReadOnlyInTree：从 hub 装配的树里 spawn 一个 observer，断言它的**生效权限**
// （Tree.PermOf）是只读（read=workspace 其余 none）、can_spawn 空 → 更深层类型层面不可达。
func TestObserverReadOnlyInTree(t *testing.T) {
	reg, ids, err := buildAgentTypeRegistry("m", nil)
	if err != nil {
		t.Fatalf("buildAgentTypeRegistry: %v", err)
	}
	h := &Hub{
		agentTypes:   reg,
		agentTypeIDs: ids,
		reg:          registry.New(registry.NewPolicyAdjudicator(policy.New())),
	}
	rootID := core.NewUlid()
	tree := h.agentTreeForLocked(core.NewUlid(), rootID)
	if tree.Root() != rootID {
		t.Fatalf("树 root 应 = 主 agent id %q，得到 %q", rootID, tree.Root())
	}
	child, err := tree.Spawn(rootID, "observer", "勘察", core.NewUlid())
	if err != nil {
		t.Fatalf("root→observer 应过三闸门: %v", err)
	}
	// observer 生效权限 = 只读（PermOf 断言）。
	perm, ok := tree.PermOf(child.ID)
	if !ok {
		t.Fatal("PermOf(observer) 应存在")
	}
	if perm.Read != contracts.ReadWriteLevelWorkspace ||
		perm.Write != contracts.ReadWriteLevelNone ||
		perm.Exec != contracts.ExecLevelNone {
		t.Errorf("observer 应只读（read=workspace 其余 none）: %+v", perm)
	}
	// observer can_spawn 空 → 再派被类型闸门②挡（ErrTypeUnreachable）：更深层类型层面不可达（K20）。
	if _, err := tree.Spawn(child.ID, "observer", "深一层", core.NewUlid()); err == nil {
		t.Error("observer（叶子）不应能再派生子 agent")
	}
}

// ---------------------------------------------------------------------------
// 装载期归一化与 fail-close 钉子（F4 can_spawn 归一化 / F5 保留名 / F2 关态零副作用）
// ---------------------------------------------------------------------------

// TestBuildAgentTypeRegistryTrimsCanSpawn（F4 钉子）：can_spawn 逐项 TrimSpace **写回**。
//
// 为什么必须写回：运行期的类型闸门②（agents.Tree.Spawn 的 contains(parent.typ.CanSpawn,
// typeID)）按**原样**比对——装载期放过 " observer "（带空格）、运行期就比不中 →
// "装载期放行、运行期拒"的口径漂移（派不出来，且错误离配置现场很远）。
func TestBuildAgentTypeRegistryTrimsCanSpawn(t *testing.T) {
	reg, ids, err := buildAgentTypeRegistry("m", []contracts.AgentType{
		{
			ID:       "lead",
			CanSpawn: []string{" observer ", "coder"},
			PermissionRequest: &contracts.PermissionRequest{
				Read: contracts.ReadWriteLevelWorkspace, Write: contracts.ReadWriteLevelWorkspace, Exec: contracts.ExecLevelNone,
			},
		},
	})
	if err != nil {
		t.Fatalf("buildAgentTypeRegistry: %v", err)
	}
	lead, ok := reg.Get("lead")
	if !ok {
		t.Fatal("lead 应在注册表")
	}
	if len(lead.CanSpawn) != 2 || lead.CanSpawn[0] != "observer" || lead.CanSpawn[1] != "coder" {
		t.Fatalf("can_spawn 应逐项 trim 写回: %v", lead.CanSpawn)
	}
	// 运行期闸门②按原样比：归一化后 "observer" 真的可达（否则就是派不出来）。
	h := &Hub{
		agentTypes:   reg,
		agentTypeIDs: ids,
		reg:          registry.New(registry.NewPolicyAdjudicator(policy.New())),
	}
	rootID := core.NewUlid()
	tree := h.agentTreeForLocked(core.NewUlid(), rootID)
	leadNode, err := tree.Spawn(rootID, "lead", "带班", core.NewUlid())
	if err != nil {
		t.Fatalf("root→lead 应过三闸门: %v", err)
	}
	if _, err := tree.Spawn(leadNode.ID, "observer", "勘察", core.NewUlid()); err != nil {
		t.Fatalf("lead→observer 应过闸门②（can_spawn 归一化后按原样比得上）: %v", err)
	}
}

// TestBuildAgentTypeRegistryRejectsEmptyCanSpawnID（F4 钉子之二）：trim 后为空的 can_spawn
// 条目 = 坏条目 → 装载期点名报错（不静默丢项——丢了就是"配了却没生效"）。
func TestBuildAgentTypeRegistryRejectsEmptyCanSpawnID(t *testing.T) {
	_, _, err := buildAgentTypeRegistry("m", []contracts.AgentType{
		{ID: "lead", CanSpawn: []string{"observer", "   "}},
	})
	if err == nil {
		t.Fatal("can_spawn 里有空 id 应被拒")
	}
	if !strings.Contains(err.Error(), "空 id") {
		t.Errorf("错误应点名空 id: %v", err)
	}
}

// TestHubMultiAgentOffSkipsAgentTypeRegistry（F2 钉子）：MultiAgent=false 时**不建注册表、
// **不校验**——坏 agent_types 不得拒绝宿主启动（"关 = 零行为变化"：用一个根本用不到的扩展点
// 不许拦住启动）。开了才 fail-close（坏配置启动即拒并点名）。
func TestHubMultiAgentOffSkipsAgentTypeRegistry(t *testing.T) {
	bad := []contracts.AgentType{
		{ID: "auditor", CanSpawn: []string{"ghost"}}, // 悬空引用 = 装载期必拒
	}
	// ① MultiAgent=false → host.New 成功（不建不校验）。
	h, err := New(Config{
		DBPath:     filepath.Join(t.TempDir(), "events.db"),
		Model:      "m",
		AgentTypes: bad,
	})
	if err != nil {
		t.Fatalf("MultiAgent=false + 坏 agent_types：host.New 应成功（关 = 不建不校验），得到错误: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if h.agentTypes != nil {
		t.Error("MultiAgent=false 时不应建 agent 类型注册表（无注册表副作用）")
	}
	if len(h.agentTypeIDs) != 0 {
		t.Errorf("MultiAgent=false 时不应有已注册类型 id 列表，得到 %v", h.agentTypeIDs)
	}

	// ② MultiAgent=true → 同一份坏配置启动即拒并点名（fail-close 仍生效）。
	_, err = New(Config{
		DBPath:     filepath.Join(t.TempDir(), "events2.db"),
		Model:      "m",
		MultiAgent: true,
		AgentTypes: bad,
	})
	if err == nil {
		t.Fatal("MultiAgent=true + 坏 agent_types：host.New 应拒（fail-close）")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Errorf("错误应点名坏类型: %v", err)
	}
}
