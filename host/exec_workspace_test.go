package host_test

// L2 per-session 执行上下文 · 件 2（宿主接线 + 持久化）的 host 验收面：
//
//	① **创建时持久化**：CreateSession 带 cwd/worktree → 落一条 config.changed{exec}；
//	   普通创建（无执行参数）→ **零事件**（既有语义不变）；
//	② **重启后仍生效**：同库重开 hub → 懒接管 → 工具仍落在原工作区（真 tmp 目录验证）；
//	③ **fold ⊕ spec**：fold（config.changed）与 spec 不一致时以 fold 为准；
//	④ **读面**：GET /api/sessions/{id}/config 回显 exec 组 + overridden_fields 点分路径；
//	⑤ **fail-close**：sandbox / remote 执行面下带工作区的会话**创建时拒绝**；
//	⑥ **profile 执行参数**：同样落事件、同样生效；
//	⑦ **fork 不继承**：旁支的工具落在进程 cwd（不继承父 session 的工作区）。

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
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/identity"
)

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

// wsPlan 是一次模型调用要发起的工具调用计划。
type wsPlan struct {
	tool string
	args map[string]any
}

// wsProvider 按"提问内容"挑工具调用计划：命中 marker 的那条计划用它（其余按序取）。
//
// 为什么按内容挑而不是按调用次序：父 session 与 /btw 旁支**共用同一个 provider**
// （forkLoopConfig 传的就是 h.provider），而一次 Run 至少两次模型调用（工具调用 +
// 收尾）——按次序挑会错位。按内容挑（"父问题" / "旁支问题"）则与调用次序无关。
type wsProvider struct {
	mu sync.Mutex
	// plans 是"按次序"消费的计划（没带 marker 的场景用）。
	plans []wsPlan
	// byMarker 是"按提问内容"匹配的计划（marker → 计划）。
	byMarker map[string]wsPlan
	calls    int
}

func (p *wsProvider) Run(_ context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu.Lock()
	p.calls++
	idx := p.calls
	plan, ok := wsPlan{}, false
	for _, m := range req.Messages {
		for marker, pl := range p.byMarker {
			if strings.Contains(m.Content, marker) {
				plan, ok = pl, true
			}
		}
	}
	if !ok && len(p.plans) > 0 {
		i := idx - 1
		if i >= len(p.plans) {
			i = len(p.plans) - 1
		}
		plan, ok = p.plans[i], true
	}
	p.mu.Unlock()
	if !ok {
		emit(core.ProviderEvent{Kind: "text.delta", Text: "没有计划，收尾"})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{Text: "收尾", Usage: core.Usage{InputTokens: 1, OutputTokens: 1}}, nil
	}
	raw, err := json.Marshal(plan.args)
	if err != nil {
		return core.ModelResult{}, err
	}
	tc := core.ToolCallReq{
		ID:   contracts.ProviderId(fmt.Sprintf("toolu_WS_%02d", idx)),
		Name: plan.tool,
		Args: raw,
	}
	emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
	emit(core.ProviderEvent{Kind: "done"})
	return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 5, OutputTokens: 5}}, nil
}

// writePlan 造一条"写文件"的工具调用计划（相对路径 → 用来验证工作区锚定）。
func writePlan(rel, content string) wsPlan {
	return wsPlan{tool: "write", args: map[string]any{"path": rel, "content": content}}
}

// chdirTemp 把进程 cwd 切到临时目录（返回恢复函数）。
//
// 为什么需要：验证"相对路径落在工作区"时，若实现坏了，文件会落到**进程 cwd**
// （= go test 的包目录）——那会污染仓库。切到临时目录后，最坏情况只是污染临时目录。
//
// 安全前提：host 的测试**没有** t.Parallel（逐个串行跑），切 cwd 不会与别的用例打架。
func chdirTemp(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	old, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir %s: %v", dir, err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	return dir
}

// execEventsOf 取某 session 的 config.changed 事件（按序）。
func execEventsOf(t *testing.T, evs []contracts.Event) []contracts.Event {
	t.Helper()
	out := []contracts.Event{}
	for _, ev := range evs {
		if ev.Kind == "config.changed" {
			out = append(out, ev)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// ① 创建时持久化
// ---------------------------------------------------------------------------

// TestCreateSessionWithCWDPersistsExecConfig：CreateSession 带 cwd → 落一条
// config.changed{exec:{cwd}}（持久化：工作区是会话事实，不能只活在内存里）。
func TestCreateSessionWithCWDPersistsExecConfig(t *testing.T) {
	db := filepath.Join(t.TempDir(), "events.db")
	ws := t.TempDir()
	h := newTestHub(t, func(c *host.Config) { c.DBPath = db })

	sid, err := h.CreateSession(host.SessionSpec{CWD: ws})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	evs := readAllEvents(t, db)
	cc := execEventsOf(t, evs)
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1（创建带工作区 → 落一条）", len(cc))
	}
	p := mustConfigPayload(t, cc[0])
	if p.Exec == nil || p.Exec.Cwd == nil {
		t.Fatalf("config.changed 载荷应带 exec.cwd: %s", string(cc[0].Payload))
	}
	if got := strings.TrimSpace(*p.Exec.Cwd); got == "" {
		t.Fatalf("exec.cwd 不该是空串: %s", string(cc[0].Payload))
	}
	if !cc[0].Durable || cc[0].SessionID != sid {
		t.Fatalf("config.changed envelope 不符: durable=%v sid=%v", cc[0].Durable, cc[0].SessionID)
	}
	if cc[0].AgentID == "" {
		t.Fatalf("config.changed 的 envelope.agent_id 必填（创建时的登记 agent）")
	}
}

// TestCreateSessionWithoutExecWritesNoEvent：**酸测试**——没给 cwd/worktree 的普通创建
// **一个事件都不落**（既有语义不变：登记 specs 即可，工作区不锚定）。
//
// （L2 件 2 的语义变化只针对"显式声明了工作区"的创建：那条要落 config.changed。
// 本钉子继续对**无执行参数**的创建成立。）
func TestCreateSessionWithoutExecWritesNoEvent(t *testing.T) {
	db := filepath.Join(t.TempDir(), "events.db")
	h := newTestHub(t, func(c *host.Config) { c.DBPath = db })

	if _, err := h.CreateSession(host.SessionSpec{}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if evs := readAllEvents(t, db); len(evs) != 0 {
		t.Fatalf("普通创建不该落任何事件，got %d 条: %v", len(evs), kindsOf(evs))
	}
}

// TestCreateSessionWithWorktreePersistsExecAndResolves：绑 worktree 的 session →
// 落一条 config.changed{exec:{cwd: <worktree 路径>, worktree: <名字>}}，且工作区
// 解析成**该 worktree 的实际路径**（名字 → 受管根查询）。
func TestCreateSessionWithWorktreePersistsExecAndResolves(t *testing.T) {
	procCwd := chdirTemp(t)
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "repo"))
	db := filepath.Join(t.TempDir(), "events.db")
	h := newTestHub(t, func(c *host.Config) {
		c.DBPath = db
		c.Allow = []string{"read", "write"}
		c.Provider = &wsProvider{plans: []wsPlan{writePlan("wt.txt", "内容")}}
	})
	ctx := context.Background()
	wt, err := h.CreateWorktree(ctx, repo, "task-ws")
	if err != nil {
		t.Fatalf("CreateWorktree: %v", err)
	}
	sid, err := h.CreateSession(host.SessionSpec{Worktree: "task-ws"})
	if err != nil {
		t.Fatalf("CreateSession(Worktree): %v", err)
	}
	cc := execEventsOf(t, readAllEvents(t, db))
	if len(cc) != 1 {
		t.Fatalf("绑 worktree 的创建该落一条 config.changed，got %d 条", len(cc))
	}
	p := mustConfigPayload(t, cc[0])
	if p.Exec == nil {
		t.Fatalf("config.changed 载荷应带 exec 组: %s", string(cc[0].Payload))
	}
	if p.Exec.Worktree == nil || *p.Exec.Worktree != "task-ws" {
		t.Fatalf("exec.worktree 应是 worktree 名: %s", string(cc[0].Payload))
	}
	if p.Exec.Cwd == nil || strings.TrimSpace(*p.Exec.Cwd) == "" {
		t.Fatalf("exec.cwd 应带 worktree 的实际路径: %s", string(cc[0].Payload))
	}
	// 读面回显 worktree 名。
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()
	var view struct {
		Exec struct {
			Cwd      string `json:"cwd"`
			Worktree string `json:"worktree"`
		} `json:"exec"`
	}
	if code := getJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/config", &view); code != http.StatusOK {
		t.Fatalf("状态码 = %d，want 200", code)
	}
	if view.Exec.Worktree != "task-ws" {
		t.Fatalf("config 端点应回显 exec.worktree=task-ws: %+v", view)
	}
	// 工作区解析 = 该 worktree 的实际路径（名字 → 受管根查询）。
	//
	// 比的是**规范化形态**（identity.CanonicalPath：解析符号链接/junction + 归一）：
	// 落盘的是规范化后的路径，不是 worktree 登记里的原始串（Windows 上大小写/分隔符
	// 会被归一）。
	wantPath, err := identity.CanonicalPath(wt.Path)
	if err != nil {
		t.Fatalf("CanonicalPath(%s): %v", wt.Path, err)
	}
	if got := strings.TrimSpace(*p.Exec.Cwd); got != wantPath {
		t.Fatalf("exec.cwd 应是 worktree 路径 %q，got %q", wantPath, got)
	}
	// 端到端：工具的相对路径落在**该 worktree 里**（worktree 绑定 = 工作区锚定）。
	if _, err := h.RunPrompt(context.Background(), sid, "写个文件"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if _, err := os.Stat(filepath.Join(wt.Path, "wt.txt")); err != nil {
		t.Fatalf("文件应落在 worktree %s: %v", wt.Path, err)
	}
	if _, err := os.Stat(filepath.Join(procCwd, "wt.txt")); err == nil {
		t.Fatalf("文件不该落在进程 cwd %s", procCwd)
	}
}

// TestExecWorkspaceCwdWorktreeCombinations（critic P1-1）：**cwd 与 worktree 的三种
// 组合**——它们是同一件事的两种写法，不能同时指向工作区。
//
//	① 已绑 worktree 时**只提交 cwd** → **400 bad_config**（不做"worktree 优先、
//	   cwd 静默 no-op"）；文案点名"需同时把 exec.worktree 写空串清除"；
//	② **只清 worktree** → 绑定解除，**cwd 生效**（工具真落在 cwd 里）；
//	③ **两个都清** → 回到**不锚定**（工具落进程 cwd）。
func TestExecWorkspaceCwdWorktreeCombinations(t *testing.T) {
	procCwd := chdirTemp(t)
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "repo"))
	dirA := t.TempDir()
	dirB := t.TempDir()
	db := filepath.Join(t.TempDir(), "events.db")
	h := newTestHub(t, func(c *host.Config) {
		c.DBPath = db
		c.Allow = []string{"read", "write"}
		c.Provider = &wsProvider{plans: []wsPlan{writePlan("combo.txt", "内容")}}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()
	ctx := context.Background()
	wt, err := h.CreateWorktree(ctx, repo, "combo-wt")
	if err != nil {
		t.Fatalf("CreateWorktree: %v", err)
	}
	// 期望值用**规范化形态**（identity.CanonicalPath：解析符号链接/junction + 归一
	// 大小写与分隔符）——工作区的解析结果就是规范化后的路径。
	wtPath, err := identity.CanonicalPath(wt.Path)
	if err != nil {
		t.Fatalf("CanonicalPath(%s): %v", wt.Path, err)
	}
	dirAPath, err := identity.CanonicalPath(dirA)
	if err != nil {
		t.Fatalf("CanonicalPath(%s): %v", dirA, err)
	}

	// 会话带 cwd=dirA（无 worktree）。
	sid, err := h.CreateSession(host.SessionSpec{CWD: dirA})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	prompt := func(what string, cfg map[string]any) int {
		t.Helper()
		body := map[string]any{"prompt": what}
		if cfg != nil {
			body["config"] = cfg
		}
		return postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", body, nil)
	}
	readWorkspace := func() string {
		t.Helper()
		var view struct {
			Exec struct {
				Workspace string `json:"workspace"`
				Cwd       string `json:"cwd"`
				Worktree  string `json:"worktree"`
			} `json:"exec"`
		}
		if code := getJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/config", &view); code != http.StatusOK {
			t.Fatalf("config 端点状态码 = %d", code)
		}
		return view.Exec.Workspace
	}

	// 先绑 worktree（cwd 仍在 fold 里 = dirA）→ 生效工作区 = worktree 路径。
	if code := prompt("绑 worktree", map[string]any{"exec": map[string]any{"worktree": "combo-wt"}}); code != http.StatusAccepted {
		t.Fatalf("绑 worktree 的状态码 = %d，want 202", code)
	}
	if got := readWorkspace(); got != wtPath {
		t.Fatalf("绑 worktree 后生效工作区 = %q，want %q", got, wtPath)
	}

	// ① 只提交 cwd（没同时清 worktree）→ **400 bad_config**（不做静默 no-op）。
	code := prompt("只改 cwd", map[string]any{"exec": map[string]any{"cwd": dirB}})
	if code != http.StatusBadRequest {
		t.Fatalf("只改 cwd 的状态码 = %d，want 400（fail-close：不做静默 no-op）", code)
	}
	// 零事件：被拒的提交不落 config.changed。
	for _, ev := range execEventsOf(t, readAllEvents(t, db)) {
		p := mustConfigPayload(t, ev)
		if p.Exec != nil && p.Exec.Cwd != nil && strings.TrimSpace(*p.Exec.Cwd) == dirB {
			t.Fatalf("被拒的 cwd 提交不该落事件: %s", string(ev.Payload))
		}
	}
	// 生效工作区不变（仍是 worktree）。
	if got := readWorkspace(); got != wtPath {
		t.Fatalf("被拒后生效工作区 = %q，want %q", got, wtPath)
	}

	// ② 只清 worktree → 绑定解除 → **cwd 生效**（工具真落在 dirA）。
	if code := prompt("只清 worktree", map[string]any{"exec": map[string]any{"worktree": ""}}); code != http.StatusAccepted {
		t.Fatalf("只清 worktree 的状态码 = %d，want 202", code)
	}
	if got := readWorkspace(); got != dirAPath {
		t.Fatalf("只清 worktree 后生效工作区 = %q，want %q（cwd 生效）", got, dirAPath)
	}
	// 工具真落在 cwd 里（不是 worktree、不是进程 cwd）。
	if _, err := h.RunPrompt(context.Background(), sid, "写"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dirA, "combo.txt")); err != nil {
		t.Fatalf("文件应落在 cwd %s（cwd 生效）: %v", dirA, err)
	}

	// ③ 两个都清 → 回到**不锚定**（工具落进程 cwd）。
	if code := prompt("两个都清", map[string]any{"exec": map[string]any{"cwd": "", "worktree": ""}}); code != http.StatusAccepted {
		t.Fatalf("两个都清的状态码 = %d，want 202", code)
	}
	if got := readWorkspace(); got != "" {
		t.Fatalf("两个都清后生效工作区 = %q，want 空（不锚定）", got)
	}
	if _, err := h.RunPrompt(context.Background(), sid, "再写"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if _, err := os.Stat(filepath.Join(procCwd, "combo.txt")); err != nil {
		t.Fatalf("不锚定时文件应落在进程 cwd %s: %v", procCwd, err)
	}
}

// TestExecWritePathCanonicalizesRelativeCwd（critic P2-5）：写面接受**相对路径** cwd，
// 但**落进事件的是规范化后的绝对路径**——与创建面（CreateSession）同一口径。
//
// 为什么：相对路径会**随调用点的 cwd 漂移**（同一个 "sub/dir" 在不同进程里指向不同
// 地方 → 重启后工作区悄悄换了位置），且"配置里显示的工作区"与"工具实际落点"必须是
// 同一个字符串。
func TestExecWritePathCanonicalizesRelativeCwd(t *testing.T) {
	procCwd := chdirTemp(t)
	ws := filepath.Join(procCwd, "relws")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", ws, err)
	}
	db := filepath.Join(t.TempDir(), "events.db")
	h := newTestHub(t, func(c *host.Config) { c.DBPath = db })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid, err := h.CreateSession(host.SessionSpec{})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "设工作区",
		"config": map[string]any{"exec": map[string]any{"cwd": "relws"}},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	// 落库的值 = 规范化后的绝对路径（不是 "relws"）。
	want, err := identity.CanonicalPath(ws)
	if err != nil {
		t.Fatalf("CanonicalPath: %v", err)
	}
	var found bool
	for _, ev := range execEventsOf(t, readAllEvents(t, db)) {
		p := mustConfigPayload(t, ev)
		if p.Exec == nil || p.Exec.Cwd == nil {
			continue
		}
		got := strings.TrimSpace(*p.Exec.Cwd)
		if got == "" {
			continue
		}
		found = true
		if !filepath.IsAbs(got) {
			t.Fatalf("落进事件的 exec.cwd 应是绝对路径，got %q", got)
		}
		if got != want {
			t.Fatalf("落进事件的 exec.cwd = %q，want %q（规范化形态）", got, want)
		}
	}
	if !found {
		t.Fatal("该有一条带 exec.cwd 的 config.changed")
	}
}

// TestExecCwdMustBeExistingDirectory（critic P2-5）：exec.cwd 指向**普通文件**或
// **不存在的路径** → **400 bad_config**（会话工作区必须是目录：锚定到文件 = 相对路径
// 无处可落；指向不存在的目录 = 每次工具调用都失败，且失败发生在模型背后）。
func TestExecCwdMustBeExistingDirectory(t *testing.T) {
	notDir := filepath.Join(t.TempDir(), "notadir.txt")
	if err := os.WriteFile(notDir, []byte("x"), 0o600); err != nil {
		t.Fatalf("写文件: %v", err)
	}
	missing := filepath.Join(t.TempDir(), "nope")
	h := newTestHub(t)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid, err := h.CreateSession(host.SessionSpec{})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	for _, what := range []struct {
		name string
		path string
	}{
		{"普通文件", notDir},
		{"不存在的路径", missing},
	} {
		code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
			"prompt": "设工作区",
			"config": map[string]any{"exec": map[string]any{"cwd": what.path}},
		}, nil)
		if code != http.StatusBadRequest {
			t.Fatalf("%s 的状态码 = %d，want 400", what.name, code)
		}
	}
}

// ---------------------------------------------------------------------------
// ② 工作区真的锚住工具（真 tmp 目录验证）+ 重启后仍生效
// ---------------------------------------------------------------------------

// TestSessionWorkspaceAnchorsToolPaths：会话带工作区 → 工具的**相对路径**落在工作区
// 里（不是进程 cwd）。这是"工作区接线真的通了"的端到端证据。
func TestSessionWorkspaceAnchorsToolPaths(t *testing.T) {
	procCwd := chdirTemp(t)
	ws := t.TempDir()
	prov := &wsProvider{plans: []wsPlan{writePlan("hello.txt", "内容")}}
	h := newTestHub(t, func(c *host.Config) {
		c.Allow = []string{"read", "write"}
		c.Provider = prov
	})

	sid, err := h.CreateSession(host.SessionSpec{CWD: ws})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := h.RunPrompt(context.Background(), sid, "写个文件"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if _, err := os.Stat(filepath.Join(ws, "hello.txt")); err != nil {
		t.Fatalf("文件应落在会话工作区 %s: %v", ws, err)
	}
	if _, err := os.Stat(filepath.Join(procCwd, "hello.txt")); err == nil {
		t.Fatalf("文件不该落在进程 cwd %s（工作区没生效）", procCwd)
	}
}

// TestSessionWorkspacePersistsAcrossRestart：**重启后仍生效**——同库重开 hub →
// 懒接管（h.specs 是纯内存、重启即丢）→ 工具**仍**落在原工作区。
//
// 这是"为什么工作区要走 config.changed 而不是只写 specs"的直接证据：只写 specs 的话，
// 这一轮的文件会落到进程 cwd（悄悄换工作区）。
func TestSessionWorkspacePersistsAcrossRestart(t *testing.T) {
	procCwd := chdirTemp(t)
	db := filepath.Join(t.TempDir(), "events.db")
	ws := t.TempDir()
	newHub := func() *host.Hub {
		return newTestHub(t, func(c *host.Config) {
			c.DBPath = db
			c.Allow = []string{"read", "write"}
			c.Provider = &wsProvider{plans: []wsPlan{writePlan("hello.txt", "内容")}}
		})
	}

	h1 := newHub()
	sid, err := h1.CreateSession(host.SessionSpec{CWD: ws})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := h1.RunPrompt(context.Background(), sid, "第一轮"); err != nil {
		t.Fatalf("RunPrompt #1: %v", err)
	}
	if err := h1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	// 重开：同一个库。**不**重新 CreateSession（模拟"懒接管"：会话是上一次进程留下的）。
	h2 := newHub()
	if _, err := h2.RunPrompt(context.Background(), sid, "第二轮"); err != nil {
		t.Fatalf("RunPrompt #2（重开后）: %v", err)
	}
	if _, err := os.Stat(filepath.Join(ws, "hello.txt")); err != nil {
		t.Fatalf("重启后文件仍应落在原工作区 %s: %v", ws, err)
	}
	if _, err := os.Stat(filepath.Join(procCwd, "hello.txt")); err == nil {
		t.Fatalf("重启后文件不该落到进程 cwd %s（工作区没持久化 → 悄悄换了工作区）", procCwd)
	}
}

// ---------------------------------------------------------------------------
// ③ fold ⊕ spec：fold 优先
// ---------------------------------------------------------------------------

// TestFoldWorkspaceOverridesSpec：spec（创建时登记）与 fold（config.changed）不一致
// 时**以 fold 为准**——config.changed 是持久真相，spec 只是创建时的登记。
func TestFoldWorkspaceOverridesSpec(t *testing.T) {
	procCwd := chdirTemp(t)
	wsA := t.TempDir()
	wsB := t.TempDir()
	prov := &wsProvider{plans: []wsPlan{writePlan("where.txt", "内容")}}
	h := newTestHub(t, func(c *host.Config) {
		c.Allow = []string{"read", "write"}
		c.Provider = prov
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid, err := h.CreateSession(host.SessionSpec{CWD: wsA})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	// 运行期改工作区（config.exec.cwd = wsB）→ 落 config.changed → fold 优先。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "换工作区",
		"config": map[string]any{"exec": map[string]any{"cwd": wsB}},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "config 轮的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	// 再跑一轮（工作区变了 → loop 失效重建 → 新的工作区生效）。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "在新工作区写",
	}, nil); code != http.StatusAccepted {
		t.Fatalf("第二轮状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "第二轮的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 2
	})

	if _, err := os.Stat(filepath.Join(wsB, "where.txt")); err != nil {
		t.Fatalf("文件应落在 fold 指定的工作区 %s（fold 优先于 spec）: %v", wsB, err)
	}
	if _, err := os.Stat(filepath.Join(wsA, "where.txt")); err == nil {
		t.Fatalf("文件不该落在 spec 的旧工作区 %s", wsA)
	}
	if _, err := os.Stat(filepath.Join(procCwd, "where.txt")); err == nil {
		t.Fatalf("文件不该落在进程 cwd %s", procCwd)
	}
}

// ---------------------------------------------------------------------------
// ④ 读面：config 端点回显 exec 组
// ---------------------------------------------------------------------------

// TestSessionConfigEndpointEchoesExec：GET /api/sessions/{id}/config → 回显 exec 组
// （**workspace = 生效工作区** + cwd/worktree = 声明原值）+ overridden_fields 里的点分
// 路径（exec.cwd / exec.worktree）。
func TestSessionConfigEndpointEchoesExec(t *testing.T) {
	db := filepath.Join(t.TempDir(), "events.db")
	ws := t.TempDir()
	h := newTestHub(t, func(c *host.Config) { c.DBPath = db })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid, err := h.CreateSession(host.SessionSpec{CWD: ws})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	var view struct {
		SessionID string `json:"session_id"`
		Config    struct {
			Exec struct {
				Workspace string `json:"workspace"`
				Cwd       string `json:"cwd"`
				Worktree  string `json:"worktree"`
			} `json:"exec"`
		} `json:"config"`
		Exec struct {
			Workspace string `json:"workspace"`
			Cwd       string `json:"cwd"`
			Worktree  string `json:"worktree"`
		} `json:"exec"`
		OverriddenFields []string `json:"overridden_fields"`
	}
	if code := getJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/config", &view); code != http.StatusOK {
		t.Fatalf("状态码 = %d，want 200", code)
	}
	if view.Exec.Cwd == "" {
		t.Fatalf("config 端点应回显 exec.cwd（声明原值）: %+v", view)
	}
	// **workspace = 真正生效的工作区根**（critic P1-1：cwd/worktree 只是声明原值）。
	wantWS, err := identity.CanonicalPath(ws)
	if err != nil {
		t.Fatalf("CanonicalPath(%s): %v", ws, err)
	}
	if view.Exec.Workspace != wantWS {
		t.Fatalf("exec.workspace = %q，want %q（生效工作区）", view.Exec.Workspace, wantWS)
	}
	if view.Config.Exec.Workspace != view.Exec.Workspace {
		t.Fatalf("嵌套组与平铺项应同值: %+v", view)
	}
	found := false
	for _, f := range view.OverriddenFields {
		if f == "exec.cwd" {
			found = true
		}
	}
	if !found {
		t.Fatalf("overridden_fields 应含 exec.cwd: %v", view.OverriddenFields)
	}
}

// ---------------------------------------------------------------------------
// ⑤ fail-close：sandbox / remote 执行面
// ---------------------------------------------------------------------------

// TestSandboxRejectsSessionWorkspace：执行面是 sandbox 且 session 显式声明工作区
// → **创建时拒绝**（不静默回落成"照旧在进程 cwd 跑"）。
func TestSandboxRejectsSessionWorkspace(t *testing.T) {
	ws := t.TempDir()
	h := newTestHub(t, func(c *host.Config) {
		c.Sandbox = true
		c.SandboxWriteDirs = []string{ws}
	})
	_, err := h.CreateSession(host.SessionSpec{CWD: ws})
	if err == nil {
		t.Fatal("sandbox 执行面下带工作区的会话该被拒绝")
	}
	if !strings.Contains(err.Error(), "per-session workspace") {
		t.Fatalf("错误文案该点明 per-session workspace 不支持: %v", err)
	}
}

// TestRemoteRejectsSessionWorkspace：执行面是 remote → 同款拒绝。
func TestRemoteRejectsSessionWorkspace(t *testing.T) {
	ws := t.TempDir()
	h := newTestHub(t, func(c *host.Config) {
		c.Remote = true
		c.RemoteHost = "user@example.invalid"
	})
	_, err := h.CreateSession(host.SessionSpec{CWD: ws})
	if err == nil {
		t.Fatal("remote 执行面下带工作区的会话该被拒绝")
	}
	if !strings.Contains(err.Error(), "per-session workspace") {
		t.Fatalf("错误文案该点明 per-session workspace 不支持: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ⑥ profile 的执行参数
// ---------------------------------------------------------------------------

// TestProfileExecParamsPersistAndApply：profile 带执行参数（cwd）→ 会话创建时**同样**
// 落 config.changed{exec}，且工作区**真的生效**（工具落在 profile 指定的目录）。
func TestProfileExecParamsPersistAndApply(t *testing.T) {
	procCwd := chdirTemp(t)
	db := filepath.Join(t.TempDir(), "events.db")
	ws := t.TempDir()
	h := newTestHub(t, func(c *host.Config) {
		c.DBPath = db
		c.Allow = []string{"read", "write"}
		c.Provider = &wsProvider{plans: []wsPlan{writePlan("profile.txt", "内容")}}
		c.Profiles = map[string]host.ProfileConfig{
			"with-ws": {Name: "with-ws", CWD: ws},
		}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	var acc struct {
		SessionID string `json:"session_id"`
	}
	code := postJSON(t, srv.URL+"/api/sessions", map[string]any{"profile": "with-ws"}, &acc)
	if code != http.StatusCreated {
		t.Fatalf("创建状态码 = %d，want 201", code)
	}
	// 落库：一条 config.changed{exec:{cwd}}。
	cc := execEventsOf(t, readAllEvents(t, db))
	if len(cc) != 1 {
		t.Fatalf("profile 带执行参数时该落一条 config.changed，got %d 条", len(cc))
	}
	p := mustConfigPayload(t, cc[0])
	if p.Exec == nil || p.Exec.Cwd == nil || strings.TrimSpace(*p.Exec.Cwd) == "" {
		t.Fatalf("config.changed 载荷应带 exec.cwd: %s", string(cc[0].Payload))
	}
	// 生效：工具落在 profile 指定的工作区。
	sid := contracts.Ulid(acc.SessionID)
	if _, err := h.RunPrompt(context.Background(), sid, "写个文件"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if _, err := os.Stat(filepath.Join(ws, "profile.txt")); err != nil {
		t.Fatalf("文件应落在 profile 指定的工作区 %s: %v", ws, err)
	}
	if _, err := os.Stat(filepath.Join(procCwd, "profile.txt")); err == nil {
		t.Fatalf("文件不该落在进程 cwd %s", procCwd)
	}
}

// ---------------------------------------------------------------------------
// ⑦ fork 不继承工作区
// ---------------------------------------------------------------------------

// TestForkDoesNotInheritWorkspace：/btw 旁支**不继承**父 session 的工作区——
// 旁支的工具落在**进程 cwd**（与 model / tools / context 同口径：fork = 新 session，
// 配置继承 hub 默认 = 不锚定）。
func TestForkDoesNotInheritWorkspace(t *testing.T) {
	procCwd := chdirTemp(t)
	ws := t.TempDir()
	prov := &wsProvider{byMarker: map[string]wsPlan{
		"父问题":  writePlan("parent.txt", "父"),
		"旁支问题": writePlan("fork.txt", "旁支"),
	}}
	h := newTestHub(t, func(c *host.Config) {
		c.Allow = []string{"read", "write"}
		c.Provider = prov
	})
	sid, err := h.CreateSession(host.SessionSpec{CWD: ws})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := h.RunPrompt(context.Background(), sid, "父问题"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if _, err := os.Stat(filepath.Join(ws, "parent.txt")); err != nil {
		t.Fatalf("父 session 的文件应落在工作区 %s: %v", ws, err)
	}
	if _, err := h.RunBtw(context.Background(), sid, "旁支问题"); err != nil {
		t.Fatalf("RunBtw: %v", err)
	}
	if _, err := os.Stat(filepath.Join(procCwd, "fork.txt")); err != nil {
		t.Fatalf("旁支的文件应落在进程 cwd %s（fork 不继承工作区）: %v", procCwd, err)
	}
	if _, err := os.Stat(filepath.Join(ws, "fork.txt")); err == nil {
		t.Fatalf("旁支不该继承父 session 的工作区 %s", ws)
	}
}
