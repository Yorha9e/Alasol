package main

// 缺口 8 的钉子：`alasol run` / `alasol resume` 的 MultiAgent 接线（agent 树 + loop.Agents）。
//
// # 怎么验（loop.Config 不暴露 → 行为面 + 单元面两层）
//
//   - 单元层：直接调 runnerAgentsEnv —— 关 = (nil, nil)（且**不校验** agent_types）；
//     开 = 非 nil 且 Tree.Root() == 传入的主 agent id（树 root ≡ loop.AgentID 同源）。
//   - 行为层（run/resume 各开/关一轮）：loop 不导出 config，但 visibleTools 只在
//     `Tree.Get(l.agentID)` 命中且 root 类型 CanSpawn 非空、策略放行时才注入 spawn ——
//     所以「spawn 进 ModelRequest.Tools」本身就是 Agents 非 nil + 树 root 对 +
//     类型注册表建成 + 策略规则接上的联合证据；再跑一轮真实 spawn（spawn → child →
//     complete）钉住 agent.spawned/completed 落库与 ParentAgentID == 主 agent。
//   - 关的探针：即便 spawn 不可见，脚本 provider 仍**发起一次 spawn 调用**（模型幻觉
//     工具调用的形态）。Agents 为 nil 时它走注册表路径 → tool.call.failed code=
//     tool_not_found；若关时误传非 nil Agents，会走 runSpawn → 策略 deny → code=
//     policy_deny —— 这就是变异②的判别点。
//
// # 变异自证（详见交付报告）
//
//	① 不传 Agents（wire.go / resume.go 的 cfg.Agents 置 nil）→ 两个 ON 行为测试红；
//	② MultiAgent 关时误传非 nil（runnerAgentsEnv 去掉开关早退）→ 单元关测试 + 两个
//	  OFF 探针测试红（failed code 从 tool_not_found 变 policy_deny）。

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/registry"
)

const (
	// maFinalText 是 spawn 轮收尾轮（root 第 2 次模型调用）返回的最终文本。
	maFinalText = "多 agent 回路完成"
	// maProbeID 是 OFF 探针的 spawn 调用 id（与 seed 的 c-safe 等区分，按 id 找 failed 事件）。
	maProbeID = contracts.ProviderId("toolu_MA_PROBE")
)

// ---------------------------------------------------------------------------
// 脚本 provider
// ---------------------------------------------------------------------------

// maHasTool 报告本次模型请求的可见工具里是否有 name。
func maHasTool(req core.ModelRequest, name string) bool {
	for _, t := range req.Tools {
		if t.Name == name {
			return true
		}
	}
	return false
}

// maSpawnArgs 构造 spawn 工具参数（agent_type=observer 是内建只读型，root 可派）。
func maSpawnArgs() json.RawMessage {
	b, _ := json.Marshal(map[string]string{
		"agent_type": "observer",
		"role":       "观察",
		"task":       "看一眼现场",
		"request_id": string(core.NewUlid()),
	})
	return b
}

// maCompleteArgs 构造 complete 工具参数（child 收敛用）。
func maCompleteArgs() json.RawMessage {
	b, _ := json.Marshal(map[string]string{"text": "子任务完成"})
	return b
}

// maSpawnRoundProvider 跑完整 spawn 轮（MultiAgent 开）：
// 调用 1 = root（断言 spawn 可见 → 发起 spawn）；调用 2 = child（断言 complete 可见 →
// 发起 complete 收敛）；调用 3+ = root 收尾（返回最终文本）。
// 可见性断言不放在这里报错——记录下来由测试体断言（失败信息更完整）。
type maSpawnRoundProvider struct {
	calls       int
	sawSpawn    bool // 某轮请求里出现过 spawn（root 轮）
	sawComplete bool // 某轮请求里出现过 complete（child 轮）
}

func (p *maSpawnRoundProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if maHasTool(req, loop.ToolSpawn) {
		p.sawSpawn = true
	}
	if maHasTool(req, loop.ToolComplete) {
		p.sawComplete = true
	}
	switch p.calls {
	case 1:
		return core.ModelResult{ToolCalls: []core.ToolCallReq{{
			ID:   contracts.ProviderId("toolu_MA_SPAWN_1"),
			Name: loop.ToolSpawn,
			Args: maSpawnArgs(),
		}}}, nil
	case 2:
		return core.ModelResult{ToolCalls: []core.ToolCallReq{{
			ID:   contracts.ProviderId("toolu_MA_COMPLETE_1"),
			Name: loop.ToolComplete,
			Args: maCompleteArgs(),
		}}}, nil
	default:
		return core.ModelResult{Text: maFinalText}, nil
	}
}

// maSpawnProbeProvider 是单 agent（MultiAgent 关）的探针：
// 调用 1 记录 spawn 是否可见，然后**仍发起一次 spawn 调用**（模型幻觉工具调用的形态）——
// Agents 为 nil 时该调用走注册表路径落 tool_not_found（关 = 逐字不变的现状）；
// 误传非 nil Agents 时会走 runSpawn 落 policy_deny（变异② 判别点）。调用 2 收尾。
type maSpawnProbeProvider struct {
	calls    int
	sawSpawn bool
}

func (p *maSpawnProbeProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if maHasTool(req, loop.ToolSpawn) {
		p.sawSpawn = true
	}
	if p.calls == 1 {
		return core.ModelResult{ToolCalls: []core.ToolCallReq{{
			ID:   maProbeID,
			Name: loop.ToolSpawn,
			Args: maSpawnArgs(),
		}}}, nil
	}
	return core.ModelResult{Text: maFinalText}, nil
}

// ---------------------------------------------------------------------------
// 事件面小工具
// ---------------------------------------------------------------------------

// maFindEvents 按 kind 过滤事件。
func maFindEvents(evs []contracts.Event, kind string) []contracts.Event {
	var out []contracts.Event
	for _, ev := range evs {
		if ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

// maPromptAgentID 找文本等于 prompt 的 user.prompt 事件的 AgentID（= 该次 Run 的主 agent）。
// child 的 Run 也会发 user.prompt（文本 = task），故必须按文本匹配。
func maPromptAgentID(t *testing.T, evs []contracts.Event, prompt string) contracts.Ulid {
	t.Helper()
	for _, ev := range evs {
		if ev.Kind != loop.KindUserPrompt {
			continue
		}
		var p contracts.UserPromptPayload
		mustUnmarshal(t, ev.Payload, &p)
		if p.Text == prompt {
			return ev.AgentID
		}
	}
	t.Fatalf("事件流里没有文本为 %q 的 user.prompt（kinds=%v）", prompt, kindsOf(evs))
	return ""
}

// maFailedCode 取指定 tool_call_id 的 tool.call.failed 的 error.code（找不到 → 空串）。
func maFailedCode(t *testing.T, evs []contracts.Event, tcID contracts.ProviderId) string {
	t.Helper()
	for _, ev := range evs {
		if ev.Kind != loop.KindToolCallFailed {
			continue
		}
		var p contracts.ToolCallFailedPayload
		mustUnmarshal(t, ev.Payload, &p)
		if p.ToolCallID == tcID {
			if p.Error.Code == nil {
				return ""
			}
			return *p.Error.Code
		}
	}
	return ""
}

// maWriteConfig 写一份用户级 TOML 配置（runMain / resumeMain 的 -config 面）。
func maWriteConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	return path
}

// ---------------------------------------------------------------------------
// 单元层：runnerAgentsEnv 开关口径（关 = nil 且不校验；开 = 树 root 对）
// ---------------------------------------------------------------------------

// TestRunnerAgentsEnvOffIsNilAndSkipsValidation：MultiAgent 关 → (nil, nil)，且**不校验**
// agent_types（关 = 不建不校验 = 零行为变化，与 host.New 同口径；坏配置不许拒绝一个
// 根本用不到它的回路）。
func TestRunnerAgentsEnvOffIsNilAndSkipsValidation(t *testing.T) {
	bad := []contracts.AgentType{{ID: "ghost", CanSpawn: []string{"nonexistent"}}}
	env, err := runnerAgentsEnv(false, "scripted", bad, core.NewUlid(), registry.New(nil))
	if err != nil {
		t.Fatalf("关时不该校验 agent_types（关 = 不建不校验）: %v", err)
	}
	if env != nil {
		t.Fatal("MultiAgent 关 → loop.Config.Agents 应为 nil（单 agent 零行为变化）")
	}
}

// TestRunnerAgentsEnvOnBuildsTreeRootedAtAgentID：MultiAgent 开 → Agents 非 nil、
// 树 root == 传入的主 agent id（树 root ≡ loop.AgentID 同源）、root 类型 CanSpawn 覆盖
// 内建与自定义类型、"default" model sentinel 解析成主模型、且树的三闸门真能派生
// （直接调 Tree.Spawn 走一遍，不烧模型）。
func TestRunnerAgentsEnvOnBuildsTreeRootedAtAgentID(t *testing.T) {
	agentID := core.NewUlid()
	custom := []contracts.AgentType{{ID: "scout", Prompt: "侦察兵", Tools: []string{"read"}, Model: "default"}}
	env, err := runnerAgentsEnv(true, "scripted", custom, agentID, registry.New(nil))
	if err != nil {
		t.Fatalf("开装配失败: %v", err)
	}
	if env == nil {
		t.Fatal("MultiAgent 开 → loop.Config.Agents 不应为 nil")
	}
	if got := env.Tree.Root(); got != agentID {
		t.Fatalf("树 root = %s, want %s（树 root 必须 ≡ 该 run 的主 agent id，否则 spawn 找不到父节点）", got, agentID)
	}
	root, ok := env.Tree.TypeOf(agentID)
	if !ok {
		t.Fatal("root 类型取不到（TypeOf(root) 失败）")
	}
	if root.ID != "root" {
		t.Fatalf("root 类型 id = %q, want \"root\"", root.ID)
	}
	for _, want := range []string{"observer", "scout"} {
		if !containsString(root.CanSpawn, want) {
			t.Fatalf("root.CanSpawn 缺 %q（内建/自定义类型都该可派）: %v", want, root.CanSpawn)
		}
	}
	// 树的三闸门真能派生：root → observer（内建）与 scout（自定义，"default" → 主模型）。
	for _, typeID := range []string{"observer", "scout"} {
		child, err := env.Tree.Spawn(agentID, typeID, "观察", core.NewUlid())
		if err != nil {
			t.Fatalf("Spawn(%s) 失败（类型注册表没接上？）: %v", typeID, err)
		}
		if child.Depth != 2 {
			t.Fatalf("child depth = %d, want 2", child.Depth)
		}
		ct, _ := env.Tree.TypeOf(child.ID)
		if typeID == "scout" && ct.Model != "scripted" {
			t.Fatalf("scout 的 model = %q, want \"scripted\"（\"default\" sentinel 应在装配期解析成主模型）", ct.Model)
		}
	}
}

// TestRunnerAgentTypeRegistryFailClose：类型注册表装载期 fail-close（与 host/agenttypes.go
// 同口径）：保留名 root / can_spawn 悬空 → 点名报错；合法自定义类型 + model 解析通过。
func TestRunnerAgentTypeRegistryFailClose(t *testing.T) {
	if _, _, err := buildRunnerAgentTypeRegistry("m", []contracts.AgentType{{ID: "root"}}); err == nil || !strings.Contains(err.Error(), "保留名") {
		t.Fatalf("自定义 id=\"root\" 应以保留名 fail-close, err=%v", err)
	}
	if _, _, err := buildRunnerAgentTypeRegistry("m", []contracts.AgentType{{ID: "x", CanSpawn: []string{"ghost"}}}); err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("can_spawn 悬空应点名 fail-close, err=%v", err)
	}
	reg, ids, err := buildRunnerAgentTypeRegistry("m", []contracts.AgentType{{ID: "scout", Model: "default"}})
	if err != nil {
		t.Fatalf("合法自定义类型不该被拒: %v", err)
	}
	ct, ok := reg.Get("scout")
	if !ok {
		t.Fatal("scout 未进注册表")
	}
	if ct.Model != "m" {
		t.Fatalf("scout model = %q, want \"m\"（default sentinel 解析）", ct.Model)
	}
	if !containsString(ids, "scout") {
		t.Fatalf("id 列表缺 scout: %v", ids)
	}
}

// ---------------------------------------------------------------------------
// run 行为面（newRunner 直连 loop —— 与 blob_cli_test 同一形态）
// ---------------------------------------------------------------------------

// TestRunCLIMultiAgentSpawnRound：`alasol run` MultiAgent 开 → spawn 可见（= Agents 非 nil +
// 树 root == loop.AgentID + 注册表建成 + 策略放行的联合证据），并跑通一轮真实 spawn：
// agent.spawned 的 ParentAgentID == 该 run 的主 agent（user.prompt 的 AgentID）、
// agent.completed 落库、无 agent.failed。
func TestRunCLIMultiAgentSpawnRound(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "events.db")
	prov := &maSpawnRoundProvider{}
	r, err := newRunner(runnerOpts{
		Model:      "scripted",
		DBPath:     dbPath,
		Approval:   "deny-all",
		Allow:      "", // 策略只靠 MultiAgent 自动补的 spawn/complete allow
		MultiAgent: true,
		provider:   prov,
	})
	if err != nil {
		t.Fatalf("装配失败: %v", err)
	}
	defer r.Close()

	const prompt = "拆解这个任务"
	res, err := r.Run(context.Background(), prompt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != maFinalText {
		t.Fatalf("最终文本 = %q, want %q", res.Text, maFinalText)
	}
	if !prov.sawSpawn {
		t.Fatal("MultiAgent 开：spawn 未进 ModelRequest.Tools（loop.Config.Agents 没接 / 树 root ≠ AgentID / 类型表没建 / 策略没放行？）")
	}
	if !prov.sawComplete {
		t.Fatal("MultiAgent 开：child 的 complete 未进 ModelRequest.Tools（Agents 没传进子 loop？）")
	}

	evs := readAllEvents(t, dbPath)
	spawned := maFindEvents(evs, loop.KindAgentSpawned)
	if len(spawned) != 1 {
		t.Fatalf("agent.spawned = %d 条, want 1（kinds=%v）", len(spawned), kindsOf(evs))
	}
	var sp contracts.AgentSpawnedPayload
	mustUnmarshal(t, spawned[0].Payload, &sp)
	rootID := maPromptAgentID(t, evs, prompt)
	if sp.ParentAgentID != rootID {
		t.Fatalf("agent.spawned.ParentAgentID = %s, want 主 agent %s（树 root ≡ loop.AgentID 同源被破坏）", sp.ParentAgentID, rootID)
	}
	if sp.AgentType != "observer" {
		t.Fatalf("AgentType = %q, want observer", sp.AgentType)
	}
	if sp.Depth != 2 {
		t.Fatalf("Depth = %d, want 2", sp.Depth)
	}
	if n := len(maFindEvents(evs, loop.KindAgentCompleted)); n != 1 {
		t.Fatalf("agent.completed = %d 条, want 1", n)
	}
	if n := len(maFindEvents(evs, loop.KindAgentFailed)); n != 0 {
		t.Fatalf("agent.failed = %d 条, want 0（child 应正常 complete 收敛）", n)
	}
}

// TestRunCLISingleAgentNoSpawnProbe：`alasol run` 默认关 → spawn 不进可见工具集（零行为
// 变化）；探针 spawn 调用落 tool_not_found（单 agent 现状逐字不变）。这条同时是变异②的
// 判别点：关时误传非 nil Agents 会改走 runSpawn → failed code 变 policy_deny。
func TestRunCLISingleAgentNoSpawnProbe(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "events.db")
	prov := &maSpawnProbeProvider{}
	r, err := newRunner(runnerOpts{
		Model:    "scripted",
		DBPath:   dbPath,
		Approval: "deny-all",
		Allow:    "",
		provider: prov,
	})
	if err != nil {
		t.Fatalf("装配失败: %v", err)
	}
	defer r.Close()

	if _, err := r.Run(context.Background(), "你好"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if prov.sawSpawn {
		t.Fatal("MultiAgent 关：spawn 不该进可见工具集（单 agent 模式逐字不变）")
	}

	evs := readAllEvents(t, dbPath)
	if n := len(maFindEvents(evs, loop.KindAgentSpawned)); n != 0 {
		t.Fatalf("agent.spawned = %d 条, want 0（单 agent 模式不建树）", n)
	}
	if code := maFailedCode(t, evs, maProbeID); code != loop.CodeToolNotFound {
		t.Fatalf("spawn 探针 failed code = %q, want %q（nil Agents 走注册表路径；policy_deny = 关时误传了非 nil Agents——变异②）",
			code, loop.CodeToolNotFound)
	}
}

// ---------------------------------------------------------------------------
// resume 行为面（resumeMain 全链路：配置文件 → fc → loop.Config）
// ---------------------------------------------------------------------------

// TestResumeCLIMultiAgentSpawnRound：`alasol resume` 配置 multi_agent=true + -continue →
// 续跑路径 spawn 可见并跑通一轮真实 spawn；agent.spawned.ParentAgentID == 该 session 的
// 真实主 agent（durable.PrimaryAgentID，树 root 与续跑 loop.AgentID 同源）。
func TestResumeCLIMultiAgentSpawnRound(t *testing.T) {
	db := filepath.Join(t.TempDir(), "resume.db")
	session, mainAgent := seedCrashedSession(t, db)
	cfg := maWriteConfig(t, "multi_agent = true\n")

	prov := &maSpawnRoundProvider{}
	prev := testProviderOverride
	testProviderOverride = prov
	defer func() { testProviderOverride = prev }()

	out := captureStdout(t)
	code := resumeMain([]string{
		"-session", string(session), "-db", db, "-verify", "none",
		"-config", cfg, "-model", "scripted", "-base-url", "http://127.0.0.1:1/v1",
		"-continue", "继续干活",
	})
	got := out()
	if code != 0 {
		t.Fatalf("resumeMain 退出码 = %d, want 0\n%s", code, got)
	}
	if !strings.Contains(got, maFinalText) {
		t.Fatalf("续跑输出里没有收尾文本:\n%s", got)
	}
	if !prov.sawSpawn {
		t.Fatal("MultiAgent 开：续跑路径 spawn 未进 ModelRequest.Tools（loop.Config.Agents 没接？）")
	}
	if !prov.sawComplete {
		t.Fatal("MultiAgent 开：child 的 complete 未进 ModelRequest.Tools")
	}

	evs := readAllEvents(t, db)
	spawned := maFindEvents(evs, loop.KindAgentSpawned)
	if len(spawned) != 1 {
		t.Fatalf("agent.spawned = %d 条, want 1（kinds=%v）", len(spawned), kindsOf(evs))
	}
	var sp contracts.AgentSpawnedPayload
	mustUnmarshal(t, spawned[0].Payload, &sp)
	if sp.ParentAgentID != mainAgent {
		t.Fatalf("agent.spawned.ParentAgentID = %s, want 该 session 真实主 agent %s（resume 树 root 必须 ≡ durable.PrimaryAgentID）",
			sp.ParentAgentID, mainAgent)
	}
	if n := len(maFindEvents(evs, loop.KindAgentCompleted)); n != 1 {
		t.Fatalf("agent.completed = %d 条, want 1", n)
	}
	if n := len(maFindEvents(evs, loop.KindAgentFailed)); n != 0 {
		t.Fatalf("agent.failed = %d 条, want 0", n)
	}
}

// TestResumeCLISingleAgentNoSpawnProbe：`alasol resume` 默认关（配置显式 multi_agent=false）
// → 续跑路径 spawn 不可见、探针落 tool_not_found、不建树（关 = 零行为变化；变异② 判别点同 run）。
func TestResumeCLISingleAgentNoSpawnProbe(t *testing.T) {
	db := filepath.Join(t.TempDir(), "resume.db")
	session, _ := seedCrashedSession(t, db)
	cfg := maWriteConfig(t, "multi_agent = false\n")

	prov := &maSpawnProbeProvider{}
	prev := testProviderOverride
	testProviderOverride = prov
	defer func() { testProviderOverride = prev }()

	out := captureStdout(t)
	code := resumeMain([]string{
		"-session", string(session), "-db", db, "-verify", "none",
		"-config", cfg, "-model", "scripted", "-base-url", "http://127.0.0.1:1/v1",
		"-continue", "继续干活",
	})
	got := out()
	if code != 0 {
		t.Fatalf("resumeMain 退出码 = %d, want 0\n%s", code, got)
	}
	if prov.sawSpawn {
		t.Fatal("MultiAgent 关：续跑路径 spawn 不该进可见工具集")
	}

	evs := readAllEvents(t, db)
	if n := len(maFindEvents(evs, loop.KindAgentSpawned)); n != 0 {
		t.Fatalf("agent.spawned = %d 条, want 0（单 agent 模式不建树）", n)
	}
	if code := maFailedCode(t, evs, maProbeID); code != loop.CodeToolNotFound {
		t.Fatalf("spawn 探针 failed code = %q, want %q（nil Agents 走注册表路径；policy_deny = 关时误传了非 nil Agents——变异②）",
			code, loop.CodeToolNotFound)
	}
}

// ---------------------------------------------------------------------------
// runMain 配置面：fc.MultiAgent / fc.AgentTypeConfigs → runnerOpts 透传
// ---------------------------------------------------------------------------

// TestRunMainMultiAgentConfigPlumbing：runMain 读配置文件把 multi_agent + [[agent_types]]
// 透进 newRunner——开 = 坏 agent_types 在装配期 fail-close 并点名（证明两个字段都到位）；
// 关 = 同一份坏配置**不校验**（关 = 不建不校验 = 零行为变化，走到 Run 才因死端点失败）。
func TestRunMainMultiAgentConfigPlumbing(t *testing.T) {
	const badTypes = "[[agent_types]]\nid = \"ghost\"\nprompt = \"x\"\ncan_spawn = [\"nonexistent\"]\n"

	t.Run("开_坏agent_types启动即拒并点名", func(t *testing.T) {
		cfg := maWriteConfig(t, "multi_agent = true\n\n"+badTypes)
		db := filepath.Join(t.TempDir(), "events.db")
		errOut := captureStderr(t)
		code := runMain([]string{
			"-prompt", "你好", "-model", "test-model", "-db", db,
			// 死端点：万一没被配置闸拦住也不会真连外网（快速失败）。
			"-base-url", "http://127.0.0.1:1/v1", "-config", cfg,
		})
		msg := errOut()
		if code != 1 {
			t.Fatalf("退出码 = %d, want 1（装配失败）\n%s", code, msg)
		}
		if !strings.Contains(msg, "装配 agent 类型注册表") || !strings.Contains(msg, "ghost") {
			t.Fatalf("MultiAgent 开时坏 agent_types 应在装配期 fail-close 并点名:\n%s", msg)
		}
	})

	t.Run("关_坏agent_types不校验", func(t *testing.T) {
		cfg := maWriteConfig(t, badTypes) // multi_agent 缺省 = false
		db := filepath.Join(t.TempDir(), "events.db")
		errOut := captureStderr(t)
		code := runMain([]string{
			"-prompt", "你好", "-model", "test-model", "-db", db,
			"-base-url", "http://127.0.0.1:1/v1", "-config", cfg,
		})
		msg := errOut()
		if strings.Contains(msg, "装配 agent 类型注册表") {
			t.Fatalf("MultiAgent 关不该校验 agent_types（关 = 不建不校验 = 零行为变化）:\n%s", msg)
		}
		if code != 1 || !strings.Contains(msg, "运行失败") {
			t.Fatalf("关时应越过装配闸走到 Run（死端点 → 运行失败）, code=%d\n%s", code, msg)
		}
	})
}

// containsString 报告 slice 里是否有 s（小工具，避免为一处引入 slices 依赖面）。
func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
