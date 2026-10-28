package host_test

import (
	"bytes"
	"context"
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
	var list []struct {
		Name string `json:"name"`
	}
	decodeJSON(resp2, &list)
	resp2.Body.Close()
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

// TestCreateSessionWarnsWorktreeIsolationNotEnabled：绑定 worktree 时必须打一条
// **醒目告警**——"绑了 worktree" 目前**不等于**"工具在 worktree 里跑"（工具仍在进程 cwd）。
//
// 为什么要有这条告警：绑定只影响自动 gate 的 repo_id 与工作区登记，工具执行仍是
// hub 一次性注册的（host/hub.go：tools.NewRead(local.FS{})）——相对路径落到进程 cwd。
// 静默绑定 = 用户以为工具被隔离了（实测会写错地方），这是必须喊出来的边界。
func TestCreateSessionWarnsWorktreeIsolationNotEnabled(t *testing.T) {
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
	if !strings.Contains(out, "工具级隔离未启用") {
		t.Fatalf("绑定 worktree 必须告警\"工具级隔离未启用\"，日志 = %q", out)
	}
	if !strings.Contains(out, wt.Name) {
		t.Fatalf("告警应带上 worktree 名 %q，日志 = %q", wt.Name, out)
	}

	// 不绑 worktree 时**不该**有这条告警（别变成噪音）。
	buf.Reset()
	if _, err := h.CreateSession(host.SessionSpec{CWD: t.TempDir()}); err != nil {
		t.Fatalf("CreateSession(无 worktree): %v", err)
	}
	if out := buf.String(); strings.Contains(out, "工具级隔离未启用") {
		t.Fatalf("没绑 worktree 不该告警，日志 = %q", out)
	}
}

// TestSessionSpecWorktreeDocStatesToolIsolationGap：**文档契约**钉子——
// SessionSpec.Worktree 的注释必须写明"工具级隔离未启用"，不许再宣称"有效工作目录
// 即该 worktree 路径"（那是 v1-S5 早期的不实描述，会让用户以为工具被隔离了）。
func TestSessionSpecWorktreeDocStatesToolIsolationGap(t *testing.T) {
	src, err := os.ReadFile("discovery.go")
	if err != nil {
		t.Fatalf("读 discovery.go: %v", err)
	}
	doc := string(src)
	if !strings.Contains(doc, "工具级隔离") {
		t.Fatalf("SessionSpec 的文档必须写明工具级隔离未启用（否则又会对用户撒谎）")
	}
	if strings.Contains(doc, "有效工作目录") {
		t.Fatalf("SessionSpec 的文档仍在宣称\"有效工作目录即 worktree 路径\"——与实现不符")
	}
	if !strings.Contains(doc, "进程 cwd") {
		t.Fatalf("SessionSpec 的文档必须写明工具仍在进程 cwd 执行")
	}
}
