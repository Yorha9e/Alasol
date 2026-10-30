package host_test

// 黑盒测 v3-S6-B host 多 agent 装配的产品面：MultiAgent opt-in 酸测试（默认关 = 单 agent 零变化；
// 开 = 建树/五类可派/observer 只读）、GET /api/sessions/{id}/agents 只读快照面、端到端 spawn。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/compaction"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/host"
)

// ---------------------------------------------------------------------------
// 酸测试：MultiAgent 默认关 = 单 agent 模式零行为变化（无树、无 Agents、spawn 不可见）
// ---------------------------------------------------------------------------

func TestMultiAgentDefaultOffNoTree(t *testing.T) {
	target := filepath.Join(t.TempDir(), "f.txt")
	if err := os.WriteFile(target, []byte("hi"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	// 不设 MultiAgent（默认 false）。
	h := newTestHub(t, withProvider(`{"path":"`+filepath.ToSlash(target)+`"}`))
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := runOneRound(t, h, "读文件") // 单 agent 一轮（ Events=4，与既有口径一致 = 零变化）
	var nodes []host.AgentNodeView
	if code := getJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/agents", &nodes); code != http.StatusOK {
		t.Fatalf("GET agents status = %d, want 200", code)
	}
	if len(nodes) != 0 {
		t.Fatalf("MultiAgent 默认关 = 无树 → agents 应为空列表，得到 %d 个节点: %+v", len(nodes), nodes)
	}
}

// 开了 MultiAgent：跑一轮后建出树，root = 主 agent（type=root, depth=1, 无 parent）。
func TestMultiAgentOnBuildsTree(t *testing.T) {
	target := filepath.Join(t.TempDir(), "f.txt")
	if err := os.WriteFile(target, []byte("hi"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	h := newTestHub(t,
		withProvider(`{"path":"`+filepath.ToSlash(target)+`"}`),
		func(c *host.Config) { c.MultiAgent = true },
	)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := runOneRound(t, h, "读文件")
	var nodes []host.AgentNodeView
	if code := getJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/agents", &nodes); code != http.StatusOK {
		t.Fatalf("GET agents status = %d", code)
	}
	if len(nodes) != 1 {
		t.Fatalf("MultiAgent 开应有 1 个 root 节点，得到 %d: %+v", len(nodes), nodes)
	}
	if nodes[0].AgentType != "root" || nodes[0].Depth != 1 {
		t.Fatalf("root 节点不符: %+v", nodes[0])
	}
	if nodes[0].ParentAgentID != "" {
		t.Errorf("root 不应有 parent: %+v", nodes[0])
	}
	if nodes[0].Status != "running" {
		t.Errorf("主 agent 应 running: %+v", nodes[0])
	}
}

// 托管口径（与 /api/peers 同款）：非本进程托管 → 404 unknown_session。
func TestSessionAgentsNonHosted404(t *testing.T) {
	h := newTestHub(t)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	foreign := host.NewSession() // 合法 ULID 但本 hub 从未托管
	if code := getJSON(t, srv.URL+"/api/sessions/"+string(foreign)+"/agents", nil); code != http.StatusNotFound {
		t.Fatalf("非托管 session 应 404，得到 %d", code)
	}
}

// ---------------------------------------------------------------------------
// 酸测试补断言：MultiAgent 关 = spawn/complete 对模型**不可见**（不只是"派不出去"）
// ---------------------------------------------------------------------------

// toolRecordingProvider 记录每轮请求的**模型可见工具名**（可见集探针），并回纯文本收尾。
type toolRecordingProvider struct {
	mu    sync.Mutex
	tools [][]string
}

func (p *toolRecordingProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	names := make([]string, 0, len(req.Tools))
	for _, tl := range req.Tools {
		names = append(names, tl.Name)
	}
	p.mu.Lock()
	p.tools = append(p.tools, names)
	p.mu.Unlock()
	return core.ModelResult{Text: "干完了"}, nil
}

// sawTool 报告任一轮请求的可见集里有没有该工具名。
func (p *toolRecordingProvider) sawTool(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, names := range p.tools {
		for _, n := range names {
			if n == name {
				return true
			}
		}
	}
	return false
}

// TestMultiAgentOffSpawnCompleteInvisible：MultiAgent 默认关 = 单 agent 模式**零行为变化**
// ——spawn/complete **压根不在模型可见集里**（它们是框架内部工具，只在多 agent 模式下注入）。
//
// 为什么不只看"派不出去"：可见集是模型能看到的**全部**——工具压根不出现，模型就不会
// 幻觉地调用它（连"调用了但失败"的机会都没有）。
func TestMultiAgentOffSpawnCompleteInvisible(t *testing.T) {
	rec := &toolRecordingProvider{}
	h := newTestHub(t, func(c *host.Config) { c.Provider = rec })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "干活"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if rec.sawTool(loop.ToolSpawn) {
		t.Error("MultiAgent 关时 spawn 不应对模型可见（单 agent 模式零行为变化）")
	}
	if rec.sawTool(loop.ToolComplete) {
		t.Error("MultiAgent 关时 complete 不应对模型可见（单 agent 模式零行为变化）")
	}
}

// ---------------------------------------------------------------------------
// F1 回归钉子：树 root ≡ loop.AgentID ≡ 事件归属（主 agent id 不漂移）
// ---------------------------------------------------------------------------

// TestMultiAgentMainAgentIDStableAcrossLoopInvalidation（F1 回归钉子）：
// 建 session（不跑 prompt）→ DeactivateWorkflow（失效 loop）→ 跑一轮 →
// 断言：① 模型看得见 spawn；② GET agents 的 root.agent_id == 事件流里的主 agent id。
//
// 复现原理（F1 的病根）：session 还没跑过 → 事件流里没有它 → disc.PrimaryAgent 查不到 →
// loop 重建时若**重算** agentID 就会拿到一个新 id → 与已建好的树 root 漂移 →
// ① 主 agent 静默失去 spawn 工具（visibleTools 查不到节点 → 不注入 spawn）；
// ② spawn 必被 ErrParentNotFound 拒；③ GET agents 的 root.agent_id 与事件流不一致。
func TestMultiAgentMainAgentIDStableAcrossLoopInvalidation(t *testing.T) {
	rec := &toolRecordingProvider{}
	h := newTestHub(t, func(c *host.Config) {
		c.MultiAgent = true
		c.Provider = rec
		// 工作目录**不在 git 仓库内** → 无自动 repo gate → 建 session **不落任何事件**
		// （F1 的触发前提：session 无事件 → 事件流口径查不到主 agent → 重建 loop 时
		// 若重算 agentID 就会与已建好的树 root 漂移）。
		c.CWD = t.TempDir()
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// ① 建 session（POST /api/sessions → 就地建 loop = 本进程托管；**不跑 prompt**，故无事件）。
	var created struct {
		SessionID string `json:"session_id"`
	}
	if code := postJSON(t, srv.URL+"/api/sessions", map[string]any{}, &created); code != http.StatusCreated {
		t.Fatalf("POST /api/sessions status = %d, want 201", code)
	}
	sid := contracts.Ulid(created.SessionID)
	if sid == "" {
		t.Fatal("建 session 未返回 session_id")
	}
	// 前提自检：session 无事件（否则本用例测的不是 F1 那条路径）。
	var ev0 struct {
		Events []contracts.Event `json:"events"`
	}
	if code := getJSON(t, srv.URL+"/api/events?session_id="+string(sid), &ev0); code != http.StatusOK {
		t.Fatalf("GET events status = %d", code)
	}
	if len(ev0.Events) != 0 {
		t.Fatalf("前提不成立：session 应无事件，得到 %d 条: %+v", len(ev0.Events), ev0.Events)
	}

	// ② 失效 loop（工作流停用同款：只失效 loop，**不失效树**）。
	h.DeactivateWorkflow(sid)

	// ③ 跑一轮。
	if _, err := h.RunPrompt(context.Background(), sid, "干活"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	// ④ 主 agent 看得见 spawn（漂移时 visibleTools 查不到节点 → 不注入）。
	if !rec.sawTool(loop.ToolSpawn) {
		t.Fatal("模型可见集应含 spawn（主 agent id 必须与树 root 同源）")
	}

	// ⑤ GET agents 的 root.agent_id == 事件流里的主 agent id。
	var nodes []host.AgentNodeView
	if code := getJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/agents", &nodes); code != http.StatusOK {
		t.Fatalf("GET agents status = %d", code)
	}
	if len(nodes) != 1 {
		t.Fatalf("应有且仅有 root 节点，得到 %d: %+v", len(nodes), nodes)
	}
	streamAgent := contracts.Ulid("")
	var evResp struct {
		Events []contracts.Event `json:"events"`
	}
	if code := getJSON(t, srv.URL+"/api/events?session_id="+string(sid), &evResp); code != http.StatusOK {
		t.Fatalf("GET events status = %d", code)
	}
	for _, ev := range evResp.Events {
		if ev.AgentID != "" {
			streamAgent = ev.AgentID
			break
		}
	}
	if streamAgent == "" {
		t.Fatal("事件流里应有带 agent_id 的事件")
	}
	if nodes[0].AgentID != string(streamAgent) {
		t.Fatalf("树 root.agent_id = %q，应与事件流的主 agent id %q 一致（F1：树 root ≡ loop.AgentID ≡ 事件归属）",
			nodes[0].AgentID, streamAgent)
	}
}

// TestSessionAgentsVisibleAfterLoopInvalidation（F1 回归钉子）：
// 跑一轮 → GET 200 → DeactivateWorkflow（**失效 loop、不失效树**）→ GET **仍 200**
// （root.agent_id 不变）→ 再跑一轮 → 仍 200。
//
// 复现原理（F1 的病根）：agent 树是 **session 活状态**（建了就一直复用），而
// hostsSession 判的是 **loop 缓存**（h.loops）。工作流停用只 delete loops、不 delete
// agentTrees → loop 失效到下一次 RunPrompt 之间，**树还活着**，端点却按 hostsSession
// 判 → 回 404 unknown_session（把一棵活着的树误报成"野 session"）。
//
// 修法口径：**树存在即 200**（h.agentTrees 命中 → 返快照）；没树才按 hostsSession 判
// （本进程托管但还没建树 → 空列表 []；非本进程托管 → 404）。
func TestSessionAgentsVisibleAfterLoopInvalidation(t *testing.T) {
	target := filepath.Join(t.TempDir(), "f.txt")
	if err := os.WriteFile(target, []byte("hi"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	h := newTestHub(t,
		withProvider(`{"path":"`+filepath.ToSlash(target)+`"}`),
		func(c *host.Config) { c.MultiAgent = true },
	)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// ① 跑一轮 → 建树 → GET 200（root 节点）。
	sid := runOneRound(t, h, "读文件")
	var nodes []host.AgentNodeView
	if code := getJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/agents", &nodes); code != http.StatusOK {
		t.Fatalf("GET agents status = %d, want 200", code)
	}
	if len(nodes) != 1 || nodes[0].AgentType != "root" {
		t.Fatalf("应有且仅有 root 节点: %+v", nodes)
	}
	rootID := nodes[0].AgentID

	// ② 失效 loop（工作流停用同款：只 delete loops，**不 delete agentTrees**）。
	h.DeactivateWorkflow(sid)

	// ③ 树还活着 → GET **仍 200**，快照不变（旧判据按 hostsSession 判 → 这里会 404）。
	var afterInvalidation []host.AgentNodeView
	if code := getJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/agents", &afterInvalidation); code != http.StatusOK {
		t.Fatalf("loop 失效后 GET agents status = %d, want 200（树是 session 活状态，不随 loop 失效）", code)
	}
	if len(afterInvalidation) != 1 || afterInvalidation[0].AgentID != rootID {
		t.Fatalf("loop 失效后树快照应不变（root.agent_id 仍是 %s）: %+v", rootID, afterInvalidation)
	}

	// ④ 再跑一轮（loop 重建、树复用）→ 仍 200，root.agent_id 不变。
	if _, err := h.RunPrompt(context.Background(), sid, "再读一次"); err != nil {
		t.Fatalf("RunPrompt(2): %v", err)
	}
	var afterRerun []host.AgentNodeView
	if code := getJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/agents", &afterRerun); code != http.StatusOK {
		t.Fatalf("再跑一轮后 GET agents status = %d, want 200", code)
	}
	if len(afterRerun) != 1 || afterRerun[0].AgentID != rootID {
		t.Fatalf("再跑一轮后 root.agent_id 应仍是 %s: %+v", rootID, afterRerun)
	}
}

// ---------------------------------------------------------------------------
// 端到端：自定义类型带 context_strategy=tiered 声明 → 走 spawn 路径 → 钩子被调、
// child.ID 绑定生效（W6-A 的产品面覆盖；此前只有白盒直调钩子）。
// ---------------------------------------------------------------------------

// tieredChildTypeID 是本用例的自定义类型 id（= TOML [[agent_types]] 的等价物）。
const tieredChildTypeID = "tiered_child"

// tieredProjectRules 是本用例的项目规则文本（share_project_assets=true 才注入子 agent）。
const tieredProjectRules = "[项目规则] 遵循 AGENTS.md 的分层推进口径"

// tieredSpawnProvider 是 v3-S6-B 端到端探针：
//   - 主 agent（有 spawn 无 complete）：派一个**自定义 tiered 类型**的子 agent，收尾回文本；
//   - 子 agent（有 complete）：连打两次 stage（软边界）后 complete 收敛；
//   - 压缩模型的摘要调用（**没有工具**）：回纯文本。
//
// 分派按**请求里的工具**走，三类互不串味（摘要调用不带工具 → 不会被当成子 agent 的轮次）。
type tieredSpawnProvider struct {
	mu         sync.Mutex
	mainCalls  int
	childTurn  int
	childRules bool // 子 agent 的请求里带上了项目规则（= tiered + share_project_assets 生效）
	summaries  int  // 压缩模型的摘要调用次数（= 真的走了 tiered 的物化路径）
}

func (p *tieredSpawnProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	var hasSpawn, hasComplete bool
	for _, tl := range req.Tools {
		switch tl.Name {
		case loop.ToolSpawn:
			hasSpawn = true
		case loop.ToolComplete:
			hasComplete = true
		}
	}
	switch {
	case hasSpawn:
		p.mu.Lock()
		p.mainCalls++
		first := p.mainCalls == 1
		p.mu.Unlock()
		if !first {
			return core.ModelResult{Text: "主 agent 收尾"}, nil
		}
		args, _ := json.Marshal(map[string]string{
			"agent_type": tieredChildTypeID,
			"role":       "分段",
			"task":       "按阶段推进并汇报",
			"request_id": string(core.NewUlid()),
		})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{{ID: "spawn-1", Name: loop.ToolSpawn, Args: args}}}, nil
	case hasComplete:
		p.mu.Lock()
		p.childTurn++
		turn := p.childTurn
		for _, m := range req.Messages {
			if strings.Contains(m.Content, tieredProjectRules) {
				p.childRules = true
			}
		}
		p.mu.Unlock()
		switch turn {
		case 1, 2:
			// 软边界（stage 工具）：让 tiered 引擎切出多个阶段 → 触发留存降级。
			args, _ := json.Marshal(map[string]string{"label": fmt.Sprintf("阶段%d", turn)})
			return core.ModelResult{ToolCalls: []core.ToolCallReq{{
				ID:   contracts.ProviderId(fmt.Sprintf("stage-%d", turn)),
				Name: compaction.ToolStage,
				Args: args,
			}}}, nil
		default:
			args, _ := json.Marshal(map[string]string{"text": "子 agent 汇报：两段都过完了", "taint": "clean"})
			return core.ModelResult{ToolCalls: []core.ToolCallReq{{ID: "child-done", Name: loop.ToolComplete, Args: args}}}, nil
		}
	default:
		// 压缩模型的摘要调用（无工具）：回纯文本。
		p.mu.Lock()
		p.summaries++
		p.mu.Unlock()
		return core.ModelResult{Text: "摘要：这一段推进完了"}, nil
	}
}

// TestMultiAgentCustomTieredTypeSpawnEndToEnd（W6-A 产品面覆盖）：
// 一条**带 context_strategy=tiered 声明的自定义类型**走完整 spawn 路径：
// ① 装配钩子（ChildStrategy）在 **spawn 路径上**被调——子 agent 的上下文按 tiered 装配，
// share_project_assets=true → 项目规则注入子 agent 的上下文；
// ② child.ID 绑定生效（引擎的事件归属按**子 agent**走：context.compacted 归 child.ID）。
//
// 此前只有白盒直调钩子（不覆盖 spawn 路径）——本用例补上产品面这一段。
func TestMultiAgentCustomTieredTypeSpawnEndToEnd(t *testing.T) {
	prov := &tieredSpawnProvider{}
	share := true
	h := newTestHub(t, func(c *host.Config) {
		c.MultiAgent = true
		c.Provider = prov
		c.Allow = []string{"read", compaction.ToolStage}
		c.ContextRules = tieredProjectRules
		c.AgentTypes = []contracts.AgentType{{
			ID:     tieredChildTypeID,
			Prompt: "你是分段执行者：按阶段推进，每阶段收尾打 stage，最后 complete 汇报。",
			Tools:  []string{"read", compaction.ToolStage},
			Model:  "default",
			ContextStrategy: &contracts.ContextStrategy{
				Kind: contracts.ContextStrategyKindTiered,
			},
			ShareProjectAssets: &share,
			PermissionRequest: &contracts.PermissionRequest{
				Read: contracts.ReadWriteLevelWorkspace, Write: contracts.ReadWriteLevelNone, Exec: contracts.ExecLevelNone,
			},
		}}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "派个分段执行的子 agent"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	// ① 钩子在 spawn 路径上被调：子 agent 的上下文带上了项目规则（append 路径不会有）。
	prov.mu.Lock()
	childRules, summaries := prov.childRules, prov.summaries
	prov.mu.Unlock()
	if !childRules {
		t.Fatal("子 agent 的上下文应带上项目规则（share_project_assets=true + tiered → 钩子在 spawn 路径被调）")
	}
	if summaries == 0 {
		t.Error("tiered 引擎应真的物化过阶段摘要（否则子 agent 走的是 append 路径）")
	}

	// ② child.ID 绑定生效：压缩事件的归属是**子 agent**（不是主 agent）。
	var nodes []host.AgentNodeView
	if code := getJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/agents", &nodes); code != http.StatusOK {
		t.Fatalf("GET agents status = %d", code)
	}
	childID := ""
	for _, n := range nodes {
		if n.AgentType == tieredChildTypeID {
			childID = n.AgentID
		}
	}
	if childID == "" {
		t.Fatalf("树里应有 %s 节点: %+v", tieredChildTypeID, nodes)
	}
	var evResp struct {
		Events []contracts.Event `json:"events"`
	}
	if code := getJSON(t, srv.URL+"/api/events?session_id="+string(sid), &evResp); code != http.StatusOK {
		t.Fatalf("GET events status = %d", code)
	}
	var sawChildCompacted bool
	for _, ev := range evResp.Events {
		if ev.Kind == compaction.KindContextCompacted && ev.AgentID == contracts.Ulid(childID) {
			sawChildCompacted = true
		}
	}
	if !sawChildCompacted {
		t.Fatalf("应有归属子 agent（child.ID=%s）的 context.compacted 事件（W6-A：引擎绑定 child.ID）", childID)
	}
}

// ---------------------------------------------------------------------------
// 端到端：MultiAgent 开 + 主 agent 经 mock provider spawn 一个内建类型（observer）→ 子跑完 →
// 事件流 agent.spawned/completed 齐 → GET agents 能看到 completed 的 observer 节点。
// ---------------------------------------------------------------------------

// spawnEchoProvider 按**请求里的工具**分派：主 agent（有 spawn、无 complete）先 spawn 一个
// observer 再收尾；observer 子 agent（有 complete、无 spawn）直接 complete 收敛。
type spawnEchoProvider struct {
	mu        sync.Mutex
	mainCalls int
}

func (p *spawnEchoProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	var hasSpawn, hasComplete bool
	for _, tl := range req.Tools {
		switch tl.Name {
		case loop.ToolSpawn:
			hasSpawn = true
		case loop.ToolComplete:
			hasComplete = true
		}
	}
	switch {
	case hasComplete && !hasSpawn:
		// 子 agent（observer 叶子）：complete 收敛。
		args, _ := json.Marshal(map[string]any{"text": "observer 汇报：现场已勘察完毕", "taint": "clean"})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{{ID: "child-done", Name: loop.ToolComplete, Args: args}}}, nil
	case hasSpawn:
		p.mu.Lock()
		p.mainCalls++
		first := p.mainCalls == 1
		p.mu.Unlock()
		if first {
			args, _ := json.Marshal(map[string]string{
				"agent_type": "observer",
				"role":       "勘察",
				"task":       "读一下现场并汇总事实",
				"request_id": string(core.NewUlid()),
			})
			return core.ModelResult{ToolCalls: []core.ToolCallReq{{ID: "spawn-1", Name: loop.ToolSpawn, Args: args}}}, nil
		}
		return core.ModelResult{Text: "主 agent 收尾"}, nil
	default:
		return core.ModelResult{Text: "done"}, nil
	}
}

func TestMultiAgentEndToEndSpawn(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) {
		c.MultiAgent = true
		c.Provider = &spawnEchoProvider{}
		c.Allow = []string{"read"} // observer 只读勘察的底座（本用例子 agent 只 complete，不真调 read）
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	res, err := h.RunPrompt(context.Background(), sid, "派个 observer 去勘察现场")
	if err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if res.Text != "主 agent 收尾" {
		t.Fatalf("主 agent 应收敛到自己的文本: %q", res.Text)
	}

	// 事件流：agent.spawned + agent.completed 齐。
	var evResp struct {
		Events    []contracts.Event `json:"events"`
		Watermark int64             `json:"watermark"`
	}
	if code := getJSON(t, srv.URL+"/api/events?session_id="+string(sid), &evResp); code != http.StatusOK {
		t.Fatalf("GET events status = %d", code)
	}
	var spawnedN, completedN int
	for _, ev := range evResp.Events {
		switch ev.Kind {
		case loop.KindAgentSpawned:
			spawnedN++
		case loop.KindAgentCompleted:
			completedN++
		}
	}
	if spawnedN != 1 || completedN != 1 {
		t.Fatalf("事件流应有 1 条 agent.spawned + 1 条 agent.completed，得到 %d/%d", spawnedN, completedN)
	}

	// GET agents：root + observer（completed，带 summary，depth=2，parent=root）。
	var nodes []host.AgentNodeView
	if code := getJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/agents", &nodes); code != http.StatusOK {
		t.Fatalf("GET agents status = %d", code)
	}
	if len(nodes) != 2 {
		t.Fatalf("应有 root + observer 两个节点，得到 %d: %+v", len(nodes), nodes)
	}
	var root, obs *host.AgentNodeView
	for i := range nodes {
		switch nodes[i].AgentType {
		case "root":
			root = &nodes[i]
		case "observer":
			obs = &nodes[i]
		}
	}
	if root == nil || root.Depth != 1 {
		t.Fatalf("应有 root 节点: %+v", nodes)
	}
	if obs == nil {
		t.Fatalf("应有 observer 节点: %+v", nodes)
	}
	if obs.Status != "completed" {
		t.Errorf("observer 应 completed: %+v", obs)
	}
	if obs.Depth != 2 {
		t.Errorf("observer depth 应 2: %d", obs.Depth)
	}
	if obs.ParentAgentID == "" || obs.ParentAgentID != root.AgentID {
		t.Errorf("observer.parent 应 = root: parent=%q root=%q", obs.ParentAgentID, root.AgentID)
	}
	if obs.Summary == nil || !strings.Contains(obs.Summary.Text, "勘察") {
		t.Errorf("observer 应带回 summary: %+v", obs.Summary)
	}
}

// P2-4：MultiAgent 开的自动 allow（spawn/complete，source=cli/auto_multi_agent，优先级最低）
// **不绕过策略链**——用户把 spawn 显式放进 Ask（优先级更高的 ask）时，无人应答（DenyAll）=
// spawn 被挡下，主 agent 派不出子 agent。证明"开 multi_agent ≠ spawn 无条件可用"。
func TestMultiAgentPolicyStillGates(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) {
		c.MultiAgent = true
		c.Ask = []string{"spawn"} // 显式 ask 优先于自动 allow → 需人审；测试 hub 用 DenyAll → deny
		c.Provider = &spawnEchoProvider{}
		c.Allow = []string{"read"}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	res, err := h.RunPrompt(context.Background(), sid, "派 observer")
	if err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	_ = res // 收敛到文本即可（spawn 被挡 → 无子产出）

	// spawn 被策略挡下 → 没有 agent.spawned 事件、树里只有 root（无 observer）。
	var evResp struct {
		Events []contracts.Event `json:"events"`
	}
	if code := getJSON(t, srv.URL+"/api/events?session_id="+string(sid), &evResp); code != http.StatusOK {
		t.Fatalf("GET events status = %d", code)
	}
	for _, ev := range evResp.Events {
		if ev.Kind == loop.KindAgentSpawned {
			t.Fatal("spawn 被策略挡下时不应有 agent.spawned（auto-allow 不绕过策略，P2-4）")
		}
	}
	var nodes []host.AgentNodeView
	if code := getJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/agents", &nodes); code != http.StatusOK {
		t.Fatalf("GET agents status = %d", code)
	}
	// 树建出来了（root 在）但 observer 没被派（spawn 被策略挡）——避免"树根本没建"的假绿。
	if len(nodes) != 1 || nodes[0].AgentType != "root" {
		t.Fatalf("应有且仅有 root 节点（spawn 被挡，无子 agent）: %+v", nodes)
	}
}
