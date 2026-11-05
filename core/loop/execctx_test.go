package loop_test

// 本文件是 L2 per-session 执行上下文（件 1）的 loop 侧钉子：
//
//   - **执行点 A**（toolcall.go 的 runToolCall）：正常工具调用必须带上会话工作区；
//   - **执行点 B**（replay.go 的 Replay）：崩溃续跑的重放路径**同样**必须带上；
//   - **子 loop 透传**（agenttools.go 的 buildChildLoop）：子 agent 与父同 session →
////     同一个工作区；
//   - **X1 回归**：工作区变了，事件流的**落盘序与事件形状**一字不变。
//
// 钉子（各自对应一处删改）：
//
//	删掉 toolcall.go 的注入   → TestToolCallInjectsExecContext 红；
//	删掉 replay.go 的注入     → TestReplayInjectsExecContext 红；
//	buildChildLoop 不透传      → TestChildLoopInheritsWorkspace 红；
//	注入改到写路径上（影响落盘）→ TestExecContextKeepsEventStreamShape 红。

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/durable"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/runtime/cap/local"
	"github.com/Yorha9e/alasol/runtime/tools"
)

// ---------------------------------------------------------------------------
// 测试替身：只记 ctx 的执行器
// ---------------------------------------------------------------------------

// ctxProbeExec 记下它**这一次**看到的执行作用域与 env。
//
// 为什么自己造一个而不改既有 mockExecutor：既有夹具是共享的，改它会让"本次改动"
// 与"夹具改动"混在一起（回归时说不清是哪一处引起的）。新文件新夹具 = 边界清楚。
type ctxProbeExec struct {
	mu      sync.Mutex
	calls   int
	hasCtx  bool
	execCtx core.ExecContext
	hasEnv  bool
	env     core.EnvScope
}

func (e *ctxProbeExec) Execute(ctx context.Context, _ contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	ec, ok := core.ExecContextFrom(ctx)
	env, hasEnv := core.EnvFrom(ctx)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	e.execCtx, e.hasCtx = ec, ok
	e.env, e.hasEnv = env, hasEnv
	text := "probe-ok"
	return contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &text}}}, nil
}

// snapshot 取执行器看到的执行作用域。
func (e *ctxProbeExec) snapshot() (core.ExecContext, bool, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.execCtx, e.hasCtx, e.calls
}

// registerProbeTool 在注册表里注册一个探针工具（返回它的执行器）。
func registerProbeTool(t *testing.T, reg toolRegistrar, name string, side contracts.SideEffect) *ctxProbeExec {
	t.Helper()
	tool := contracts.Tool{
		Name:       name,
		SideEffect: side,
		Schema:     json.RawMessage(`{"type":"object","properties":{}}`),
	}
	exec := &ctxProbeExec{}
	if err := reg.RegisterTool(tool, exec); err != nil {
		t.Fatalf("RegisterTool(%q): %v", name, err)
	}
	return exec
}

// toolRegistrar 是"能注册工具"的那一小块（*registry.Registry 满足它；夹具用同一形状）。
type toolRegistrar interface {
	RegisterTool(tool contracts.Tool, exec core.ToolExecutor) error
}

// ---------------------------------------------------------------------------
// 执行点 A：正常工具调用
// ---------------------------------------------------------------------------

// TestToolCallInjectsExecContext 执行点 A：工具执行器在 ctx 上看到本次的会话工作区。
func TestToolCallInjectsExecContext(t *testing.T) {
	h := newHarness(t, allowRule("probe"))
	exec := registerProbeTool(t, h.reg, "probe", contracts.SideEffectRead)
	h.script(
		stepTools(call("c1", "probe", `{}`)),
		stepText("干完了"),
	)
	l := h.loop(func(c *loop.Config) { c.Workspace = `D:\ws\repo` })

	if _, err := l.Run(context.Background(), "干活"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	ec, ok, calls := exec.snapshot()
	if calls != 1 {
		t.Fatalf("执行器被调 %d 次，期望 1", calls)
	}
	if !ok {
		t.Fatal("执行点 A 未注入执行作用域（ctx 上取不到）")
	}
	if ec.Workspace != `D:\ws\repo` {
		t.Errorf("Workspace = %q，期望 %q", ec.Workspace, `D:\ws\repo`)
	}
}

// TestToolCallWithoutWorkspaceStillInjects 工作区为空 = **不锚定**，但作用域**仍然注入**
// （值是空串）——"没配工作区"与"配了工作区"是同一条代码路径上的两个取值，
// 不需要两套分支。工具侧看到空工作区就回落进程 cwd。
func TestToolCallWithoutWorkspaceStillInjects(t *testing.T) {
	h := newHarness(t, allowRule("probe"))
	exec := registerProbeTool(t, h.reg, "probe", contracts.SideEffectRead)
	h.script(
		stepTools(call("c1", "probe", `{}`)),
		stepText("干完了"),
	)
	l := h.loop(nil) // 不配 Workspace

	if _, err := l.Run(context.Background(), "干活"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	ec, ok, _ := exec.snapshot()
	if !ok {
		t.Fatal("执行作用域应被注入（哪怕工作区是空串）")
	}
	if ec.Workspace != "" {
		t.Errorf("Workspace = %q，期望空串（不锚定）", ec.Workspace)
	}
}

// ---------------------------------------------------------------------------
// 执行点 B：崩溃续跑的重放路径
// ---------------------------------------------------------------------------

// TestReplayInjectsExecContext 执行点 B：重放路径**同样**注入会话工作区。
//
// 这条最容易被漏：漏了不报错——重放照样"成功"，只是文件落到了别处（进程 cwd），
// 审计从事件流里看不出任何异常。宁可显式失败也不要静默错位。
func TestReplayInjectsExecContext(t *testing.T) {
	h := newHarness(t, allowRule("probe"))
	exec := registerProbeTool(t, h.reg, "probe", contracts.SideEffectRead)
	l := h.loop(func(c *loop.Config) { c.Workspace = `D:\ws\repo` })

	op := durable.Op{
		Intent: contracts.ToolCallIntentPayload{
			ToolCallID: "tc_replay_probe",
			ToolName:   "probe",
			Arguments:  json.RawMessage(`{}`),
		},
		IntentEventID: core.NewUlid(),
		SessionID:     h.session,
		AgentID:       h.agent,
	}
	res, err := l.Replay(context.Background(), op)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !res.OK {
		t.Fatalf("重放应成功，got %+v", res)
	}
	ec, ok, calls := exec.snapshot()
	if calls != 1 {
		t.Fatalf("执行器被调 %d 次，期望 1", calls)
	}
	if !ok {
		t.Fatal("执行点 B 未注入执行作用域（重放会落到进程 cwd——静默错位）")
	}
	if ec.Workspace != `D:\ws\repo` {
		t.Errorf("Workspace = %q，期望 %q", ec.Workspace, `D:\ws\repo`)
	}
}

// ---------------------------------------------------------------------------
// 子 loop 透传
// ---------------------------------------------------------------------------

// TestChildLoopInheritsWorkspace 子 agent 与父**同 session**（K25）→ 同一个工作区。
//
// 不透传的坏处：父在 worktree A 里干活、子在进程 cwd 里干活——同一会话两个工作目录，
// 事件流里的路径与磁盘上的落点对不上（且子写出去的东西没人找得到）。
func TestChildLoopInheritsWorkspace(t *testing.T) {
	rootType := agents.AgentType{
		ID: "root", CanSpawn: []string{"scout"}, Tools: nil,
		Model: maRootModel, PermissionRequest: permFull(),
	}
	h := newMAHarness(t, rootType, 3, allowRule("read"))
	h.registerType(agents.AgentType{
		ID: "scout", CanSpawn: nil, Tools: []string{"read"},
		Model: "scout-model", PermissionRequest: permRO(),
	})
	// 探针工具名 = "read"（夹具按名字给副作用档位：read → 只读，子 agent 的 permRO 放行）。
	exec := registerProbeTool(t, h.reg, "read", contracts.SideEffectRead)

	reqID := core.NewUlid()
	h.prov.script(maRootModel,
		stepTools(call("spawn1", loop.ToolSpawn, string(spawnArgs("scout", "recon", "读一下 README", reqID)))),
		stepText("主 agent 收尾"),
	)
	h.prov.script("scout-model",
		stepTools(call("read1", "read", `{}`)),
		stepTools(call("done1", loop.ToolComplete, string(completeArgs("读完了", nil, agents.TaintClean)))),
	)

	l := maRootLoopWithWorkspace(t, h, 10, `D:\ws\repo`)
	if _, err := l.Run(context.Background(), "拆个任务"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	ec, ok, calls := exec.snapshot()
	if calls != 1 {
		t.Fatalf("子 agent 的工具执行器被调 %d 次，期望 1", calls)
	}
	if !ok {
		t.Fatal("子 loop 未透传执行作用域")
	}
	if ec.Workspace != `D:\ws\repo` {
		t.Errorf("子 agent 的 Workspace = %q，期望 %q（父子同 session → 同一工作区）", ec.Workspace, `D:\ws\repo`)
	}
}

// maRootLoopWithWorkspace 造一个带工作区的根 loop（maHarness 自带的 rootLoop 不带这个字段）。
func maRootLoopWithWorkspace(t *testing.T, h *maHarness, maxTurns int, ws string) *loop.Loop {
	t.Helper()
	l, err := loop.New(loop.Config{
		Provider:  core.Provider(h.prov),
		Bus:       h.bus,
		Registry:  h.reg,
		Policy:    h.engine,
		Approver:  core.DenyAll{},
		SessionID: h.session,
		AgentID:   h.tree.Root(),
		Model:     maRootModel,
		System:    "root-system",
		MaxTurns:  maxTurns,
		Agents:    &loop.Agents{Tree: h.tree, Verifier: h.verifier},
		Workspace: ws,
	})
	if err != nil {
		t.Fatalf("loop.New: %v", err)
	}
	return l
}

// ---------------------------------------------------------------------------
// 端到端：loop → ctx 缝 → 真工具 → 真文件系统
// ---------------------------------------------------------------------------

// TestWorkspaceReachesRealFileSystem 端到端：模型给一个**相对路径**，真 write 工具
// （runtime/tools + runtime/cap/local）把文件落在**会话工作区**里。
//
// 这一条是整条链路的收口证明：loop 注入 → ctx 传递 → 工具解析 → 能力面落盘。
// 前面几条钉的是"缝"的两端，这条钉的是**中间那段真跑通**。
func TestWorkspaceReachesRealFileSystem(t *testing.T) {
	ws := t.TempDir()
	h := newHarness(t, allowRule("write"))
	writeTool := contracts.Tool{
		Name:       "write",
		SideEffect: contracts.SideEffectWrite,
		Schema:     json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}}}`),
	}
	if err := h.reg.RegisterTool(writeTool, tools.NewWrite(local.FS{})); err != nil {
		t.Fatalf("RegisterTool(write): %v", err)
	}
	h.script(
		stepTools(call("w1", "write", `{"path":"e2e.txt","content":"端到端"}`)),
		stepText("写完了"),
	)
	l := h.loop(func(c *loop.Config) { c.Workspace = ws })

	if _, err := l.Run(context.Background(), "写个文件"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(ws, "e2e.txt"))
	if err != nil {
		t.Fatalf("文件应落在会话工作区 %s：%v", ws, err)
	}
	if string(data) != "端到端" {
		t.Errorf("文件内容 = %q", data)
	}
}

// ---------------------------------------------------------------------------
// X1 回归：工作区不影响事件流
// ---------------------------------------------------------------------------

// TestExecContextKeepsEventStreamShape X1 回归：工具跑在**不同工作区**时，事件流的
// 落盘序与事件**形状逐字节同构**（只有事件 id / 时间戳 / seq 这类必然不同的字段不同）。
//
// 为什么钉这条：执行上下文是**执行期**的东西，绝不能渗进事实源——一旦它影响了
// intent/completed 的载荷或落盘序，审计看到的就不再是"模型发起了什么"，而是
// "框架当时在哪个目录"（那是执行细节，不是调用事实）。
func TestExecContextKeepsEventStreamShape(t *testing.T) {
	shape := func(ws string) []string {
		h := newHarness(t, allowRule("probe"))
		registerProbeTool(t, h.reg, "probe", contracts.SideEffectRead)
		h.script(
			stepTools(call("c1", "probe", `{"path":"out.txt"}`)),
			stepText("干完了"),
		)
		l := h.loop(func(c *loop.Config) { c.Workspace = ws })
		if _, err := l.Run(context.Background(), "干活"); err != nil {
			t.Fatalf("Run: %v", err)
		}
		return writePathShape(h.events())
	}

	a := shape(`D:\ws\repo`)
	b := shape(`E:\other\place`)
	if len(a) == 0 {
		t.Fatal("没有采到写路径事件")
	}
	if len(a) != len(b) {
		t.Fatalf("事件条数不同：%d vs %d\nA=%v\nB=%v", len(a), len(b), a, b)
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("第 %d 条事件形状不同：\nA=%s\nB=%s", i, a[i], b[i])
		}
	}
	// 顺带钉住三段写路径的序（X1）：intent → completed。
	if !strings.Contains(a[0], loop.KindToolCallIntent) || !strings.Contains(a[1], loop.KindToolCallCompleted) {
		t.Errorf("三段写路径序不对（应 intent → completed）：%v", a)
	}
}

// writePathShape 把写路径事件压成"kind + 载荷"的形状串（载荷逐字节）。
//
// 只取写路径（不含 context.assembled / user.prompt / model.turn 这三条观测事件——
// 它们的载荷含上下文快照与对话内容，与本用例的考点无关，且逐字节比较会因 prompt
// 回填顺序产生噪声）。
func writePathShape(evs []contracts.Event) []string {
	var out []string
	for _, ev := range writePath(evs) {
		out = append(out, ev.Kind+"\x00"+string(ev.Payload))
	}
	return out
}
