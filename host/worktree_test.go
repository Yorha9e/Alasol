package host_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/host"
)

// TestHostWorktreeAPIBinding 验证 hub 侧 worktree 管理 + session 绑定（v1-S5 D）。
func TestHostWorktreeAPIBinding(t *testing.T) {
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "repo"))
	h := newTestHub(t)
	ctx := context.Background()

	// ① 建 worktree（并行任务工作区）。
	wt, err := h.CreateWorktree(ctx, repo, "task-a")
	if err != nil {
		t.Fatalf("CreateWorktree: %v", err)
	}
	if _, err := os.Stat(filepath.Join(wt.Path, "README.md")); err != nil {
		t.Fatalf("worktree 应检出仓库内容: %v", err)
	}
	// 列表 / 单查。
	if len(h.Worktrees()) != 1 {
		t.Fatalf("应有 1 个 worktree，得到 %d", len(h.Worktrees()))
	}
	if got, ok := h.Worktree("task-a"); !ok || got.Path != wt.Path {
		t.Fatalf("Worktree(task-a) = %+v ok=%v", got, ok)
	}

	// ② session 绑定到 worktree（有效 CWD = worktree 路径）。
	sid, err := h.CreateSession(host.SessionSpec{Worktree: "task-a"})
	if err != nil {
		t.Fatalf("CreateSession(Worktree): %v", err)
	}
	if h.SessionWorktree(string(sid)) != "task-a" {
		t.Fatalf("SessionWorktree = %q，want task-a", h.SessionWorktree(string(sid)))
	}

	// ③ 绑到不存在的 worktree → 拒绝。
	if _, err := h.CreateSession(host.SessionSpec{Worktree: "nope"}); err == nil {
		t.Fatal("绑到不存在的 worktree 应报错")
	}

	// ④ 删除 worktree（junction-safe）。
	if err := h.RemoveWorktree(ctx, "task-a"); err != nil {
		t.Fatalf("RemoveWorktree: %v", err)
	}
	if _, err := os.Lstat(wt.Path); !os.IsNotExist(err) {
		t.Fatalf("RemoveWorktree 后路径应不存在: %v", err)
	}
	if len(h.Worktrees()) != 0 {
		t.Fatalf("删除后应无 worktree，得到 %d", len(h.Worktrees()))
	}
}

// TestHostWorktreeHTTP 验证 worktree 的 HTTP 面（POST/GET/DELETE）。
func TestHostWorktreeHTTP(t *testing.T) {
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "repo"))
	h := newTestHub(t)
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	// POST 建。
	resp, err := http.Post(srv.URL+"/api/worktrees", "application/json",
		strings.NewReader(`{"repo":"`+filepath.ToSlash(repo)+`","name":"wt-http"}`))
	if err != nil {
		t.Fatalf("POST worktrees: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST 状态码 = %d", resp.StatusCode)
	}
	var wt struct {
		Name string `json:"name"`
		Path string `json:"path"`
	}
	decodeJSON(resp, &wt)
	resp.Body.Close()
	if wt.Name != "wt-http" {
		t.Fatalf("建的 worktree name = %q", wt.Name)
	}

	// GET 列表。
	resp2, err := http.Get(srv.URL + "/api/worktrees")
	if err != nil {
		t.Fatalf("GET worktrees: %v", err)
	}
	raw2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	// G6：JSON 形状走 snake_case（对齐全仓视图）。原文里必须出现 snake_case 键、
	// 不得出现 PascalCase 键——Go 的 json 解包是大小写不敏感的，只解进结构体
	// 发现不了"其实是 PascalCase"，必须看原始字节（第三方 GUI 按字面键取值）。
	body2 := string(raw2)
	for _, want := range []string{`"name"`, `"path"`, `"repo"`, `"created_at"`} {
		if !strings.Contains(body2, want) {
			t.Fatalf("GET /api/worktrees 应含 snake_case 键 %s（G6）: %s", want, body2)
		}
	}
	for _, bad := range []string{`"Name"`, `"Path"`, `"Repo"`, `"CreatedAt"`} {
		if strings.Contains(body2, bad) {
			t.Fatalf("GET /api/worktrees 不该有 PascalCase 键 %s（G6：worktree JSON 须 snake_case）: %s", bad, body2)
		}
	}
	var list []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw2, &list); err != nil {
		t.Fatalf("解列表: %v", err)
	}
	if len(list) != 1 || list[0].Name != "wt-http" {
		t.Fatalf("列表错: %+v", list)
	}

	// DELETE 删。
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/api/worktrees/wt-http", nil)
	resp3, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE worktrees: %v", err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("DELETE 状态码 = %d", resp3.StatusCode)
	}
	// 删后列表空。
	resp4, _ := http.Get(srv.URL + "/api/worktrees")
	var list2 []struct {
		Name string `json:"name"`
	}
	decodeJSON(resp4, &list2)
	resp4.Body.Close()
	if len(list2) != 0 {
		t.Fatalf("删后应空，得到 %+v", list2)
	}
}

// TestCreateSessionWarnsWorktreeWorkspaceIsSoftBoundary：绑定 worktree 时必须打一条
// **醒目告警**——L2 per-session 执行上下文（件 2）之后，绑定 worktree **确实**把会话
// 工作区锚到了该目录（工具相对路径锚它、bash 的 cwd 是它），但那是**软边界**：
// 绝对路径原样透传、越界判定是词法的。真隔离归沙箱。
//
// 为什么要有这条告警：静默绑定 = 用户以为工具被**安全**隔离了（实测：绝对路径照样
// 能写出去）——这是必须喊出来的边界。
//
// （L2 之前这条告警说的是"工具级隔离未启用"；件 2 落地后语义变了：工具**会**在
// worktree 里跑，故告警改成"已锚定 + 这是软边界"。）
func TestCreateSessionWarnsWorktreeWorkspaceIsSoftBoundary(t *testing.T) {
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "repo"))
	h := newTestHub(t)
	ctx := context.Background()

	wt, err := h.CreateWorktree(ctx, repo, "task-warn")
	if err != nil {
		t.Fatalf("CreateWorktree: %v", err)
	}

	// 捕获 slog 输出（告警走 slog.Default()，调用时取 → 换掉默认 logger 即可接住）。
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)

	if _, err := h.CreateSession(host.SessionSpec{Worktree: "task-warn"}); err != nil {
		t.Fatalf("CreateSession(Worktree): %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "会话工作区") {
		t.Fatalf("绑定 worktree 必须告警\"会话工作区已锚定\"，日志 = %q", out)
	}
	if !strings.Contains(out, "软边界") {
		t.Fatalf("告警必须点明这是软边界（绝对路径不受限、真隔离归沙箱），日志 = %q", out)
	}
	if !strings.Contains(out, wt.Name) {
		t.Fatalf("告警应带上 worktree 名 %q，日志 = %q", wt.Name, out)
	}

	// 不绑 worktree 时**不该**有这条告警（别变成噪音）。
	buf.Reset()
	if _, err := h.CreateSession(host.SessionSpec{CWD: t.TempDir()}); err != nil {
		t.Fatalf("CreateSession(无 worktree): %v", err)
	}
	if out := buf.String(); strings.Contains(out, "会话工作区") {
		t.Fatalf("没绑 worktree 不该告警，日志 = %q", out)
	}
}

// TestSessionSpecWorktreeDocStatesWorkspaceBoundary：**文档契约**钉子——
// SessionSpec.Worktree 的注释必须写明"绑了 worktree = 会话工作区锚到该目录"以及
// "这是软边界、真隔离归沙箱"，不许再宣称旧的"工具级隔离未启用"（那是 L2 之前的
// 事实，现在是错的——工具确实在 worktree 里跑了）。
func TestSessionSpecWorktreeDocStatesWorkspaceBoundary(t *testing.T) {
	src, err := os.ReadFile("discovery.go")
	if err != nil {
		t.Fatalf("读 discovery.go: %v", err)
	}
	doc := string(src)
	if !strings.Contains(doc, "会话工作区") {
		t.Fatalf("SessionSpec 的文档必须写明绑定 worktree = 会话工作区锚到该目录（否则用户不知道工具在哪跑）")
	}
	if !strings.Contains(doc, "软边界") {
		t.Fatalf("SessionSpec 的文档必须写明这是软边界（绝对路径不受限、真隔离归沙箱）")
	}
	if strings.Contains(doc, "工具级隔离未启用") {
		t.Fatalf("SessionSpec 的文档仍在宣称\"工具级隔离未启用\"——L2 件 2 之后这句已经不成立（工具确实在 worktree 里跑）")
	}
	if strings.Contains(doc, "有效工作目录") {
		t.Fatalf("SessionSpec 的文档仍在宣称\"有效工作目录即 worktree 路径\"——与实现不符")
	}
}
