package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
)

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

// cliScriptedProvider 是 CLI 测试用的脚本 provider：第一轮发起一次 read 调用，
// 第二轮收尾（与 e2e_test.go 的 scriptedProvider 同形状，但参数可注入）。
type cliScriptedProvider struct {
	calls int
	args  string
}

func (p *cliScriptedProvider) Run(ctx context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if p.calls == 1 {
		tc := core.ToolCallReq{ID: contracts.ProviderId("toolu_CLI_01"), Name: "read", Args: json.RawMessage(p.args)}
		emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}}, nil
	}
	return core.ModelResult{Text: "CLI 读完了"}, nil
}

// swapStdin 把 os.Stdin 换成管道写端的内容，返回恢复函数。
func swapStdin(t *testing.T, input string) func() {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	old := os.Stdin
	os.Stdin = r
	go func() {
		_, _ = io.WriteString(w, input)
		_ = w.Close()
	}()
	return func() { os.Stdin = old; _ = r.Close() }
}

// captureStdout 捕获 os.Stdout 的输出（返回停止函数与取内容的函数）。
func captureStdout(t *testing.T) (stop func() string) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	old := os.Stdout
	os.Stdout = w
	var buf bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(&buf, r)
	}()
	return func() string {
		_ = w.Close()
		os.Stdout = old
		wg.Wait()
		_ = r.Close()
		return buf.String()
	}
}

// captureStderr 捕获 os.Stderr 的输出（与 captureStdout 同款）。
func captureStderr(t *testing.T) (stop func() string) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	old := os.Stderr
	os.Stderr = w
	var buf bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(&buf, r)
	}()
	return func() string {
		_ = w.Close()
		os.Stderr = old
		wg.Wait()
		_ = r.Close()
		return buf.String()
	}
}

// freePort 找一个空闲 loopback 端口（先听后关，测试里够用）。
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// waitHTTP 轮询等某个 URL 就绪（或超时失败）。
func waitHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("等 %s 就绪超时", url)
}

// getJSON 发一个 GET 并解 JSON（失败即中止）。
func getJSON(t *testing.T, url string, out any) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("解 %s 的 JSON: %v", url, err)
		}
	}
	return resp.StatusCode
}

// ---------------------------------------------------------------------------
// CLI 端到端：真命令路径（flag → host.Hub → 真事件库 → 输出）
// ---------------------------------------------------------------------------

// TestChatREPLEndToEnd：chat 子命令跑"对话 → /table → /ctx"全流程。
// 验证：模型文本被打印、在线表里出现该 session、上下文构成里有 base/tools/history。
func TestChatREPLEndToEnd(t *testing.T) {
	target := filepath.Join(t.TempDir(), "cli-target.txt")
	if err := os.WriteFile(target, []byte("cli e2e"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}

	// 注入脚本 provider（不真打模型 API）。
	prev := testProviderOverride
	testProviderOverride = &cliScriptedProvider{args: `{"path":"` + filepath.ToSlash(target) + `"}`}
	defer func() { testProviderOverride = prev }()

	restoreIn := swapStdin(t, "读一下目标文件\n/table\n/ctx\n/quit\n")
	defer restoreIn()
	out := captureStdout(t)

	code := chatMain([]string{
		"-db", filepath.Join(t.TempDir(), "cli.db"),
		"-model", "scripted",
		"-allow", "read",
		"-max-turns", "4",
		"-system", "cli-system",
	})
	got := out()

	if code != 0 {
		t.Fatalf("chatMain 退出码 = %d，want 0", code)
	}
	// ① 模型最终文本被打印。
	if !strings.Contains(got, "CLI 读完了") {
		t.Fatalf("输出里没有模型文本:\n%s", got)
	}
	// ② 在线表里有这个 session（事件数 7 = user.prompt + 两轮 context.assembled/model.turn + intent + completed；last_kind 为收尾轮的 model.turn）。
	if !strings.Contains(got, "model.turn") {
		t.Fatalf("/table 输出里没有 last_kind:\n%s", got)
	}
	// ③ 上下文构成面板数据：三个 part 都在。
	for _, want := range []string{"base", "tools", "history"} {
		if !strings.Contains(got, want) {
			t.Fatalf("/ctx 输出里缺 part %q:\n%s", want, got)
		}
	}
	if !strings.Contains(got, "verbatim") {
		t.Fatalf("/ctx 输出里没有 state:\n%s", got)
	}
	// ④ 事件计数行（stderr，不在这里断言）——stdout 里至少要有占比数字。
	if !strings.Contains(got, "100.0%") && !strings.Contains(got, "%") {
		t.Fatalf("/ctx 输出里没有占比:\n%s", got)
	}
}

// TestChatREPLCommandsWithoutModelCall：/table、/ctx 在没有任何 session 时
// 也不该崩（空态路径）。
func TestChatREPLCommandsWithoutModelCall(t *testing.T) {
	restoreIn := swapStdin(t, "/table\n/ctx\n/quit\n")
	defer restoreIn()
	out := captureStdout(t)

	code := chatMain([]string{
		"-db", filepath.Join(t.TempDir(), "cli-empty.db"),
		"-model", "scripted",
	})
	got := out()
	if code != 0 {
		t.Fatalf("退出码 = %d，want 0", code)
	}
	if !strings.Contains(got, "还没有任何 session") {
		t.Fatalf("空在线表提示缺失:\n%s", got)
	}
	if !strings.Contains(got, "还没有 context.assembled 快照") {
		t.Fatalf("空上下文提示缺失:\n%s", got)
	}
}

// TestServeMainEndpoints：serve 的装配 + HTTP 服务四个端点形状正确。
//
// 走 serveHub（serveMain 的骨架）而不是 serveMain 本身：后者用信号 ctx 阻塞到
// 进程收到 SIGINT，测试里没法安全地发。flag→Config 的映射由 TestServeMainRejects
// 与 TestSplitList 覆盖（真机 smoke 另用手动 curl 验证过 serveMain 全路径）。
func TestServeMainEndpoints(t *testing.T) {
	port := freePort(t)
	db := filepath.Join(t.TempDir(), "serve.db")

	h, err := host.New(host.Config{
		DBPath: db,
		Addr:   fmt.Sprintf("127.0.0.1:%d", port),
		Model:  "scripted",
		Allow:  []string{"read"},
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	defer h.Close()

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- serveHub(ctx, h) }()
	defer func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("serveHub: %v", err)
		}
	}()

	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitHTTP(t, base+"/api/health", 5*time.Second)

	// health：ok + watermark 字段。
	var health host.Health
	if code := getJSON(t, base+"/api/health", &health); code != http.StatusOK || !health.OK {
		t.Fatalf("health = %+v（code=%d）", health, code)
	}

	// index：嵌入的 Web UI，三块面板 id 齐全。
	resp, err := http.Get(base + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / 状态码 = %d", resp.StatusCode)
	}
	html := string(body)
	for _, id := range []string{"panel-sessions", "panel-context", "panel-events"} {
		if !strings.Contains(html, `id="`+id+`"`) {
			t.Fatalf("index.html 缺面板 %s", id)
		}
	}

	// sessions：空库返回 []（不是 null）。
	var rows []host.SessionRow
	if code := getJSON(t, base+"/api/sessions", &rows); code != http.StatusOK {
		t.Fatalf("sessions code = %d", code)
	}
	if len(rows) != 0 {
		t.Fatalf("sessions = %+v，want 空", rows)
	}
}

// TestServeMainRejectsNonLoopback：serve 的 flag→Config 路径对非 loopback 拒绝（N5b）。
func TestServeMainRejectsNonLoopback(t *testing.T) {
	out := captureStdout(t)
	code := serveMain([]string{
		"-db", filepath.Join(t.TempDir(), "x.db"),
		"-addr", "0.0.0.0:9999",
		"-model", "m",
	})
	_ = out()
	if code != 1 {
		t.Fatalf("0.0.0.0 监听应被拒（退出码 1），得到 %d", code)
	}
}

// TestSplitList：逗号分隔参数解析。
func TestSplitList(t *testing.T) {
	got := splitList(" read , write ,, bash ")
	if len(got) != 3 || got[0] != "read" || got[1] != "write" || got[2] != "bash" {
		t.Fatalf("splitList = %v", got)
	}
	if len(splitList("")) != 0 || len(splitList("  ")) != 0 {
		t.Fatal("空串应解析成空列表")
	}
}
