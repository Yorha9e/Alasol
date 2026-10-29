package host_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/workflow/tower"
	"github.com/Yorha9e/alasol/host"
)

// newWFHub 同 newTestHub 但返回事件库路径（工作流测试要读事件流断言）。
func newWFHub(t *testing.T, muts ...func(*host.Config)) (*host.Hub, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.db")
	cfg := host.Config{
		DBPath:   path,
		Model:    "scripted",
		System:   "test-system",
		MaxTurns: 4,
		Allow:    []string{"read"},
		CWD:      tempRepoOrDir(t),
	}
	for _, m := range muts {
		if m != nil {
			m(&cfg)
		}
	}
	h, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h, path
}

// capturingProvider 记录每次模型请求的 System（酸测试要对比挂/不挂工作流的 system prompt）。
type capturingProvider struct {
	mu      sync.Mutex
	systems []string
}

func (p *capturingProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu.Lock()
	p.systems = append(p.systems, req.System)
	p.mu.Unlock()
	return core.ModelResult{Text: "ok", Usage: core.Usage{InputTokens: 5, OutputTokens: 2}}, nil
}

func (p *capturingProvider) last() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.systems) == 0 {
		return ""
	}
	return p.systems[len(p.systems)-1]
}

// toolCallingProvider 是**带真实工具调用**的脚本 provider。
//
// 为什么不用上面那个"永不调工具"的 capturingProvider：那样 turns/toolCalls 的
// "挂/不挂一致"断言**恒真**（两轮都是 1 turn / 0 tool call，比的是"没差别"而不是
// "控制流一致"）。酸测试要真的走一遍工具调用（X1 三段写路径），断言才有意义。
type toolCallingProvider struct {
	mu      sync.Mutex
	systems []string
	path    string // read 工具要读的文件（绝对路径）
}

func (p *toolCallingProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu.Lock()
	p.systems = append(p.systems, req.System)
	p.mu.Unlock()
	// 按**本轮上下文**决定行为（不靠全局计数——两次 Run 共用一个 provider）：
	// 上下文里已有工具结果 → 本轮收敛；否则发起一次工具调用。
	// （看"有没有 tool 结果"而不是"最后一条是什么"——在线表消息会追加在**最末尾**。）
	hasToolResult := false
	for _, m := range req.Messages {
		if m.Role == "tool" {
			hasToolResult = true
			break
		}
	}
	if hasToolResult {
		return core.ModelResult{Text: "ok", Usage: core.Usage{InputTokens: 5, OutputTokens: 2}}, nil
	}
	tc := core.ToolCallReq{
		ID:   contracts.ProviderId("toolu_WF_ACID"),
		Name: "read",
		Args: json.RawMessage(fmt.Sprintf(`{"path":%q}`, p.path)),
	}
	return core.ModelResult{Text: "读文件", ToolCalls: []core.ToolCallReq{tc}}, nil
}

func (p *toolCallingProvider) last() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.systems) == 0 {
		return ""
	}
	return p.systems[len(p.systems)-1]
}

// 酸测试（W1 基石）：不挂工作流 = 零行为变化；挂上 = 只把哲学文本拼进 system prompt（W3），
// loop 的控制流（turns / 工具调用）不变。
//
// **断言不许恒真**（p3 修）：provider 会真的调一次工具（read）→ turns=2 / toolCalls=1，
// "挂/不挂一致"比的是**真实控制流**；再断言事件流除 workflow.activated 外**零新增**。
func TestWorkflowMountAcidNoBehaviorChangeExceptSystem(t *testing.T) {
	// read 工具要读的文件（绝对路径，避开进程 cwd 的差异）。
	file := filepath.Join(t.TempDir(), "acid.txt")
	if err := os.WriteFile(file, []byte("acid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	prov := &toolCallingProvider{path: file}
	h, path := newWFHub(t, func(c *host.Config) { c.Provider = prov; c.Allow = []string{"read"} })
	ctx := context.Background()

	// ① 不挂工作流：system prompt 原样（= cfg.System）。
	s1 := host.NewSession()
	res1, err := h.RunPrompt(ctx, s1, "hi")
	if err != nil {
		t.Fatalf("RunPrompt #1: %v", err)
	}
	if got := prov.last(); got != "test-system" {
		t.Errorf("不挂工作流时 system 应原样（test-system），得到 %q", got)
	}
	// 前置：断言不许恒真——这一轮**真的**调了工具（1 次调用、2 轮模型）。
	if res1.ToolCalls != 1 || res1.Turns < 2 {
		t.Fatalf("夹具前提不成立：provider 应真的调一次工具，得到 turns=%d tools=%d", res1.Turns, res1.ToolCalls)
	}

	// ② 挂上 tower：system prompt = 基础 + 哲学（W3），且基础在前。
	s2 := host.NewSession()
	if _, err := h.ActivateWorkflow(ctx, s2, tower.TowerID); err != nil {
		t.Fatalf("ActivateWorkflow: %v", err)
	}
	res2, err := h.RunPrompt(ctx, s2, "hi")
	if err != nil {
		t.Fatalf("RunPrompt #2: %v", err)
	}
	// loop 行为对比：挂/不挂工作流，控制流（turns / 工具调用）**完全一致**——只有 system 变了。
	if res1.Turns != res2.Turns || res1.ToolCalls != res2.ToolCalls {
		t.Errorf("挂/不挂工作流的 loop 控制流应一致：不挂 turns=%d tools=%d，挂 turns=%d tools=%d",
			res1.Turns, res1.ToolCalls, res2.Turns, res2.ToolCalls)
	}
	if res2.ToolCalls != 1 || res2.Turns < 2 {
		t.Errorf("挂工作流那一轮也应真的走一遍工具调用：turns=%d tools=%d", res2.Turns, res2.ToolCalls)
	}
	got := prov.last()
	if !strings.HasPrefix(got, "test-system") {
		t.Errorf("挂工作流后 system 应仍以基础开头，得到 %q", got)
	}
	if !strings.Contains(got, tower.Philosophy) {
		t.Error("挂工作流后 system 应包含 tower 哲学文本（W3）")
	}

	// ③ 激活发了 workflow.activated 事件（W3 触发审计）。
	if !hasKind(t, s2, "workflow.activated", path) {
		t.Error("激活应发 workflow.activated 事件")
	}

	// ④ 事件流对比：除 workflow.activated 外**零新增**（挂工作流不改控制流）。
	k1 := eventKinds(t, s1, path)
	k2 := eventKinds(t, s2, path)
	k2NoWF := make([]string, 0, len(k2))
	for _, k := range k2 {
		if k == "workflow.activated" {
			continue
		}
		k2NoWF = append(k2NoWF, k)
	}
	if len(k1) == 0 {
		t.Fatal("事件流不应为空（夹具没跑起来？）")
	}
	if strings.Join(k1, ",") != strings.Join(k2NoWF, ",") {
		t.Errorf("挂/不挂工作流的事件流应一致（除 workflow.activated）：\n不挂: %v\n挂:   %v", k1, k2NoWF)
	}
}

// eventKinds 取某 session 事件流的 kind 序列（按落库序）。
func eventKinds(t *testing.T, sid contracts.Ulid, path string) []string {
	t.Helper()
	events, err := readSessionEvents(t, path, sid)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	out := make([]string, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.Kind)
	}
	return out
}

// TestWorkflowCommandIsRealEntryPoint（v3-S3 修）：能力包**自己声明**的 slash 命令
// （tower 的 "tower"）必须是**真能敲的入口**——Registry.ByCommand 有了生产调用方
// （Hub.ActivateWorkflowByCommand），不再是没人用的字段。
//
// 同时钉死审计口径：workflow.activated 事件的 command 字段记**实际触发的命令名**
// （slash 触发才记；显式触发留空 = 载荷语义"缺省 = 显式触发"）。
func TestWorkflowCommandIsRealEntryPoint(t *testing.T) {
	h, path := newWFHub(t)
	ctx := context.Background()

	// ① 命令行形态 "/tower" → 识别为工作流触发。
	cmd, ok := h.WorkflowCommandTrigger("/tower")
	if !ok || cmd != tower.TowerCommand {
		t.Fatalf("/tower 应被识别为工作流触发，得到 (%q,%v)", cmd, ok)
	}
	// 未登记的命令名不劫持普通输入。
	if _, ok := h.WorkflowCommandTrigger("/todo"); ok {
		t.Error("未登记的命令名不该被当成工作流触发")
	}
	// 带参数的不算触发（"/tower x" 是普通输入）。
	if _, ok := h.WorkflowCommandTrigger("/tower x"); ok {
		t.Error("带参数的命令行不该被当成工作流触发")
	}

	// ② 按命令激活 → 事件里记 command="tower"。
	s1 := host.NewSession()
	if _, err := h.ActivateWorkflowByCommand(ctx, s1, tower.TowerCommand); err != nil {
		t.Fatalf("ActivateWorkflowByCommand: %v", err)
	}
	if aw := h.ActiveWorkflow(s1); aw == nil || aw.ID() != tower.TowerID {
		t.Fatalf("按命令激活后应有 tower 激活，得到 %v", aw)
	}
	evs := activatedEvents(t, s1, path)
	if len(evs) != 1 {
		t.Fatalf("应有 1 条 workflow.activated 事件，得到 %d", len(evs))
	}
	if evs[0].Command == nil || *evs[0].Command != tower.TowerCommand {
		t.Errorf("slash 触发的事件应记 command=%q，得到 %v", tower.TowerCommand, evs[0].Command)
	}

	// ③ 显式触发（/workflow tower → ActivateWorkflow）→ 事件里**不**记命令（= 显式触发）。
	s2 := host.NewSession()
	if _, err := h.ActivateWorkflow(ctx, s2, tower.TowerID); err != nil {
		t.Fatalf("ActivateWorkflow: %v", err)
	}
	evs2 := activatedEvents(t, s2, path)
	if len(evs2) != 1 {
		t.Fatalf("应有 1 条 workflow.activated 事件，得到 %d", len(evs2))
	}
	if evs2[0].Command != nil {
		t.Errorf("显式触发的事件不该记命令（载荷语义：缺省 = 显式触发），得到 %v", *evs2[0].Command)
	}
}

// activatedEvents 取某 session 的 workflow.activated 事件载荷。
func activatedEvents(t *testing.T, sid contracts.Ulid, path string) []contracts.WorkflowActivatedPayload {
	t.Helper()
	events, err := readSessionEvents(t, path, sid)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	var out []contracts.WorkflowActivatedPayload
	for _, ev := range events {
		if ev.Kind != "workflow.activated" {
			continue
		}
		var p contracts.WorkflowActivatedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("解 workflow.activated: %v", err)
		}
		out = append(out, p)
	}
	return out
}

// 停用回到默认 system prompt。
func TestWorkflowDeactivate(t *testing.T) {
	prov := &capturingProvider{}
	h := newTestHub(t, func(c *host.Config) { c.Provider = prov; c.Allow = nil })
	ctx := context.Background()
	s := host.NewSession()
	if _, err := h.ActivateWorkflow(ctx, s, tower.TowerID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.RunPrompt(ctx, s, "hi"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(prov.last(), tower.Philosophy) {
		t.Fatal("激活后应含哲学")
	}
	h.DeactivateWorkflow(s)
	if _, err := h.RunPrompt(ctx, s, "hi"); err != nil {
		t.Fatal(err)
	}
	if prov.last() != "test-system" {
		t.Errorf("停用后 system 应回到默认，得到 %q", prov.last())
	}
}

// tower 里程碑端到端（W14 + W19）：激活 tower → 触发 validation.passed → 结论 + workflow.milestone 事件。
func TestWorkflowTowerMilestoneEndToEnd(t *testing.T) {
	h, path := newWFHub(t)
	ctx := context.Background()
	s := host.NewSession()
	if _, err := h.ActivateWorkflow(ctx, s, tower.TowerID); err != nil {
		t.Fatal(err)
	}

	// 通过：critic clean + reviewer approved。
	res, err := h.TriggerMilestone(ctx, s, tower.MilestoneValidationPassed,
		[]byte(`{"critic_clean":true,"reviewer_verdict":"approved"}`))
	if err != nil {
		t.Fatalf("TriggerMilestone: %v", err)
	}
	if !res.Passed {
		t.Error("critic clean + approved 应 passed")
	}
	// 不发包里程碑 → 报错（不静默当通过）。
	if _, err := h.TriggerMilestone(ctx, s, "no.such.milestone", nil); err == nil {
		t.Error("无对应部件的里程碑应报错")
	}
	// 未激活的 session 触发 → 报错。
	if _, err := h.TriggerMilestone(ctx, host.NewSession(), tower.MilestoneValidationPassed, nil); err == nil {
		t.Error("未激活工作流的 session 触发里程碑应报错")
	}
	// workflow.milestone 事件进了流（passed=true 那条）。
	if !hasMilestonePassed(t, s, path) {
		t.Error("应发 workflow.milestone（passed=true）事件")
	}
}

// WorkflowDir 目录加载：临时目录放一个纯数据 JSON 包 → 能激活（W13）。
func TestWorkflowDirLoading(t *testing.T) {
	dir := t.TempDir()
	pkgJSON := `{"v":1,"id":"custom","command":"custom","philosophy":"自定义哲学。","context_strategy":{"kind":"append"}}`
	if err := os.WriteFile(filepath.Join(dir, "custom.json"), []byte(pkgJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	prov := &capturingProvider{}
	h := newTestHub(t, func(c *host.Config) {
		c.Provider = prov
		c.Allow = nil
		c.WorkflowDir = dir
	})
	// 内建 tower + 目录包都在。
	if _, ok := h.Workflows().Get("custom"); !ok {
		t.Fatal("目录包 custom 应被加载")
	}
	if _, ok := h.Workflows().Get(tower.TowerID); !ok {
		t.Fatal("内建 tower 应总在")
	}
	ctx := context.Background()
	s := host.NewSession()
	if _, err := h.ActivateWorkflow(ctx, s, "custom"); err != nil {
		t.Fatalf("激活目录包: %v", err)
	}
	if _, err := h.RunPrompt(ctx, s, "hi"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(prov.last(), "自定义哲学。") {
		t.Errorf("目录包哲学应注入 system，得到 %q", prov.last())
	}
}

// ParseWorkflowCommand：/workflow、/workflow <id>、/workflow off、/workflowxyz 不当命令。
func TestParseWorkflowCommand(t *testing.T) {
	cases := []struct {
		line    string
		wantArg string
		wantOK  bool
	}{
		{"/workflow", "", true},
		{"/workflow tower", "tower", true},
		{"/workflow off", "off", true},
		{"/workflow\ttower", "tower", true}, // tab 也算分隔符
		{"/workflowxyz", "", false},
		{"/btw x", "", false},
		{"hello", "", false},
	}
	for _, tc := range cases {
		arg, ok := host.ParseWorkflowCommand(tc.line)
		if ok != tc.wantOK || arg != tc.wantArg {
			t.Errorf("ParseWorkflowCommand(%q) = (%q,%v), want (%q,%v)", tc.line, arg, ok, tc.wantArg, tc.wantOK)
		}
	}
}

// hasKind 报告某 session 事件流里是否有某 kind 的事件。
func hasKind(t *testing.T, sid contracts.Ulid, kind, path string) bool {
	t.Helper()
	events, err := readSessionEvents(t, path, sid)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	for _, ev := range events {
		if ev.Kind == kind {
			return true
		}
	}
	return false
}

// hasMilestonePassed 报告某 session 是否有 passed=true 的 workflow.milestone 事件。
func hasMilestonePassed(t *testing.T, sid contracts.Ulid, path string) bool {
	t.Helper()
	events, err := readSessionEvents(t, path, sid)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	for _, ev := range events {
		if ev.Kind != "workflow.milestone" {
			continue
		}
		var p contracts.WorkflowMilestonePayload
		if json.Unmarshal(ev.Payload, &p) == nil && p.Passed {
			return true
		}
	}
	return false
}
