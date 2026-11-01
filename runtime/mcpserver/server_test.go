package mcpserver_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/runtime/mcpserver"
	mcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

// 本文件用 go-sdk 的 in-memory transport（NewInMemoryTransports）起一个**真** MCP server
// （跑同一条 wire 协议），配一个真 registry + 假 core.ToolExecutor，覆盖：
//   - initialize / tools/list / tools/call 全链（fake executor/hub 形态）；
//   - 白名单 fail-close（空 = 零暴露）；
//   - 白名单过滤（只暴露显式声明的）；
//   - E5 panic 隔离（执行器 panic → MCP error、server 存活）；
//   - E5 执行器 error → MCP error、server 存活；
//   - 参数原样过桥。
//
// stdout 纯净性在 CLI 级子进程钉（cmd/alasol/mcp_serve_test.go）——in-memory 传输不碰
// os.Stdout，进程级纯净只有真起进程才验得到。

// fakeExec 是测试用 core.ToolExecutor：可回显参数、可返回 error、可 panic。
type fakeExec struct {
	text    string
	err     error
	panicOn bool
	// gotArgs 记录最近一次收到的 Arguments（断言"参数原样过桥"）。
	gotArgs json.RawMessage
	calls   int
}

func (e *fakeExec) Execute(_ context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	e.calls++
	e.gotArgs = intent.Arguments
	if e.panicOn {
		panic("boom: fake 执行器故意崩溃")
	}
	if e.err != nil {
		return contracts.ToolResult{}, e.err
	}
	t := e.text
	return contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &t}}}, nil
}

// regTool 注册一个工具并返回它的执行器（测试断言用）。
func regTool(t *testing.T, reg *registry.Registry, name string, exec *fakeExec) {
	t.Helper()
	schema := json.RawMessage(`{"type":"object"}`)
	if err := reg.RegisterTool(contracts.Tool{
		Name:       name,
		Schema:     schema,
		SideEffect: contracts.SideEffectRead,
	}, exec); err != nil {
		t.Fatalf("RegisterTool(%q): %v", name, err)
	}
}

// startServer 起一个 in-memory MCP server 并连上 client，返回 client 会话与 server。
//
// SDK 约束：server 必须先 Connect，client 后 Connect（client 连接时发 initialize）。
func startServer(t *testing.T, reg *registry.Registry, expose []string) (*mcp.ClientSession, *mcpserver.Server) {
	t.Helper()
	srv, err := mcpserver.New(reg, expose, &mcpserver.Options{Logger: silent()})
	if err != nil {
		t.Fatalf("mcpserver.New: %v", err)
	}
	serverT, clientT := mcp.NewInMemoryTransports()
	ss, err := srv.SDKServer().Connect(context.Background(), serverT, nil)
	if err != nil {
		t.Fatalf("server Connect: %v", err)
	}
	t.Cleanup(func() { _ = ss.Close() })

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1"}, nil)
	cs, err := client.Connect(context.Background(), clientT, nil)
	if err != nil {
		t.Fatalf("client Connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs, srv
}

func silent() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// captureLogger 造一个把日志收进缓冲区的 logger（钉"必须告警"的那几条：告警被吞 = 没人
// 知道自己的工具没被暴露）。
func captureLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

// toolNames 从 ListTools 结果取工具名集合。
func toolNames(t *testing.T, cs *mcp.ClientSession) []string {
	t.Helper()
	res, err := cs.ListTools(context.Background(), &mcp.ListToolsParams{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	out := make([]string, 0, len(res.Tools))
	for _, tl := range res.Tools {
		out = append(out, tl.Name)
	}
	return out
}

func TestServerListAndCallRoundTrip(t *testing.T) {
	reg := registry.New(nil)
	exec := &fakeExec{text: "hello from echo"}
	regTool(t, reg, "echo", exec)

	cs, srv := startServer(t, reg, []string{"echo"})

	if got := toolNames(t, cs); len(got) != 1 || got[0] != "echo" {
		t.Fatalf("tools/list = %v, want [echo]", got)
	}
	if exp := srv.ExposedTools(); len(exp) != 1 || exp[0] != "echo" {
		t.Fatalf("ExposedTools = %v, want [echo]", exp)
	}

	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "echo",
		Arguments: json.RawMessage(`{"msg":"hi"}`),
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if len(res.Content) != 1 {
		t.Fatalf("结果内容块数 = %d, want 1", len(res.Content))
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok || tc.Text != "hello from echo" {
		t.Fatalf("结果文本 = %#v, want hello from echo", res.Content[0])
	}
	// 参数原样过桥：执行器收到的就是 client 发来的字节。
	if string(exec.gotArgs) != `{"msg":"hi"}` {
		t.Fatalf("执行器收到的参数 = %q, want 原样 {\"msg\":\"hi\"}", string(exec.gotArgs))
	}
	if exec.calls != 1 {
		t.Fatalf("执行器调用次数 = %d, want 1", exec.calls)
	}
}

func TestServerEmptyArgsBecomeEmptyObject(t *testing.T) {
	reg := registry.New(nil)
	exec := &fakeExec{text: "ok"}
	regTool(t, reg, "echo", exec)
	cs, _ := startServer(t, reg, []string{"echo"})

	if _, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "echo"}); err != nil {
		t.Fatalf("CallTool(无参): %v", err)
	}
	if string(exec.gotArgs) != "{}" {
		t.Fatalf("空参数应补 {}，得到 %q", string(exec.gotArgs))
	}
}

// TestServerEmptyWhitelistExposesNothing 钉死 **fail-close**：白名单空 = 一个工具都不暴露。
func TestServerEmptyWhitelistExposesNothing(t *testing.T) {
	reg := registry.New(nil)
	regTool(t, reg, "read", &fakeExec{text: "x"})
	regTool(t, reg, "write", &fakeExec{text: "y"})

	cs, srv := startServer(t, reg, nil) // 白名单空

	if got := toolNames(t, cs); len(got) != 0 {
		t.Fatalf("空白名单应零暴露，tools/list = %v", got)
	}
	if exp := srv.ExposedTools(); len(exp) != 0 {
		t.Fatalf("ExposedTools 应为空，得到 %v", exp)
	}
	// 未暴露的工具调用 → MCP 错误（unknown tool），不带崩 server。
	if _, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "read"}); err == nil {
		t.Fatalf("未暴露的工具调用应报错（fail-close）")
	}
}

// TestServerWhitelistFiltersToExplicitOnly 钉死：只有白名单点名的工具被暴露。
// 这是"防 harness 写面被任意 MCP client 摸到"的核心——write/bash 不进白名单就摸不到。
func TestServerWhitelistFiltersToExplicitOnly(t *testing.T) {
	reg := registry.New(nil)
	regTool(t, reg, "read", &fakeExec{text: "r"})
	regTool(t, reg, "write", &fakeExec{text: "w"})
	regTool(t, reg, "bash", &fakeExec{text: "b"})

	cs, srv := startServer(t, reg, []string{"read"}) // 只暴露 read

	got := toolNames(t, cs)
	if len(got) != 1 || got[0] != "read" {
		t.Fatalf("tools/list = %v, want 只 [read]", got)
	}
	// write / bash 不在暴露面：调用 → unknown tool 错误。
	for _, name := range []string{"write", "bash"} {
		if _, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: name}); err == nil {
			t.Fatalf("未列入白名单的 %q 竟可调用（写面泄漏）", name)
		}
	}
	if exp := srv.ExposedTools(); len(exp) != 1 || exp[0] != "read" {
		t.Fatalf("ExposedTools = %v, want [read]", exp)
	}
}

// TestServerWhitelistIgnoresUnknownNames 钉死：白名单里点名但注册表没有的工具被忽略，
// 不影响其它工具的暴露（fail-safe，不因一个悬空名让 server 残缺）。
func TestServerWhitelistIgnoresUnknownNames(t *testing.T) {
	reg := registry.New(nil)
	regTool(t, reg, "read", &fakeExec{text: "r"})

	cs, srv := startServer(t, reg, []string{"read", "ghost"})

	if got := toolNames(t, cs); len(got) != 1 || got[0] != "read" {
		t.Fatalf("tools/list = %v, want [read]（ghost 应被忽略）", got)
	}
	if exp := srv.ExposedTools(); len(exp) != 1 || exp[0] != "read" {
		t.Fatalf("ExposedTools = %v, want [read]", exp)
	}
}

// TestServerToolPanicIsolatedAsMCPError 钉死 E5：执行器 panic → MCP error 响应，
// server 会话存活（紧接着能正常调另一个工具）。
func TestServerToolPanicIsIsolatedAsMCPError(t *testing.T) {
	reg := registry.New(nil)
	regTool(t, reg, "boom", &fakeExec{panicOn: true})
	ok := &fakeExec{text: " survived "}
	regTool(t, reg, "ok", ok)

	cs, _ := startServer(t, reg, []string{"boom", "ok"})

	// panic 工具：调用返回 MCP error（不 panic、不中断会话）。
	_, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "boom"})
	if err == nil {
		t.Fatalf("执行器 panic 应转成 MCP error 返回给 client")
	}
	// server 仍存活：紧接着调正常工具成功。
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "ok"})
	if err != nil {
		t.Fatalf("panic 之后 server 应仍存活并能调用其它工具: %v", err)
	}
	if tc, ok := res.Content[0].(*mcp.TextContent); !ok || tc.Text != " survived " {
		t.Fatalf("后续调用结果 = %#v", res.Content[0])
	}
	if ok.calls != 1 {
		t.Fatalf("后续工具调用次数 = %d, want 1", ok.calls)
	}
}

// TestServerToolErrorReturnedAsMCPError 钉死：执行器返回普通 error → MCP error，
// server 存活（执行失败是普通失败，不是中断）。
func TestServerToolErrorReturnedAsMCPError(t *testing.T) {
	reg := registry.New(nil)
	regTool(t, reg, "flaky", &fakeExec{err: errInjected})
	ok := &fakeExec{text: "fine"}
	regTool(t, reg, "ok", ok)

	cs, _ := startServer(t, reg, []string{"flaky", "ok"})

	if _, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "flaky"}); err == nil {
		t.Fatalf("执行器报错应转成 MCP error")
	}
	if _, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "ok"}); err != nil {
		t.Fatalf("执行失败之后 server 应仍存活: %v", err)
	}
}

// errInjected 是测试注入的执行器错误。
var errInjected = &injectedErr{}

type injectedErr struct{}

func (*injectedErr) Error() string { return "磁盘满了 (injected)" }

// TestServerF1DisabledToolNotExposed 钉死 F1：**注册表 State 参与暴露判定**——被停用的
// 工具（Disable / 能力裁决失败后的 revokeEnabled）不进 tools/list，调用也报 unknown/disabled。
//
// 这条是"停用"能不能当真闸的分水岭：State 不参与判定 = 停用只是个列表口径。
func TestServerF1DisabledToolNotExposed(t *testing.T) {
	reg := registry.New(nil)
	exec := &fakeExec{text: "x"}
	regTool(t, reg, "read", exec)
	if err := reg.Disable("read"); err != nil {
		t.Fatalf("Disable(read): %v", err)
	}

	cs, srv := startServer(t, reg, []string{"read"})

	if got := toolNames(t, cs); len(got) != 0 {
		t.Fatalf("停用的工具不该出现在 tools/list，得到 %v", got)
	}
	if exp := srv.ExposedTools(); len(exp) != 0 {
		t.Fatalf("ExposedTools 应为空，得到 %v", exp)
	}
	if _, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "read"}); err == nil {
		t.Fatalf("停用的工具调用应报错（unknown/disabled）")
	}
	if exec.calls != 0 {
		t.Fatalf("停用的工具不该被执行（执行器调用次数 = %d）", exec.calls)
	}
}

// TestServerF1DisabledAfterListIsRejected 钉死 F1 的**执行侧**：tools/list 是构造时的快照，
// 运行期才被停用的工具必须**立刻**调不动（执行前再查一次 State，防"list 后被 disable"的窗口）。
func TestServerF1DisabledAfterListIsRejected(t *testing.T) {
	reg := registry.New(nil)
	exec := &fakeExec{text: "x"}
	regTool(t, reg, "read", exec)

	cs, _ := startServer(t, reg, []string{"read"})
	if got := toolNames(t, cs); len(got) != 1 || got[0] != "read" {
		t.Fatalf("tools/list = %v, want [read]", got)
	}

	// list 之后才停用。
	if err := reg.Disable("read"); err != nil {
		t.Fatalf("Disable(read): %v", err)
	}
	if _, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "read"}); err == nil {
		t.Fatalf("list 之后被停用的工具不该还能调用")
	}
	if exec.calls != 0 {
		t.Fatalf("停用后不该执行（执行器调用次数 = %d）", exec.calls)
	}
}

// TestServerF1EnabledToolStillExposed 钉死 F1 的**另一半**：State 参与判定 ≠ 只认 registered。
//
// registered（内置工具的常态：注册了但没人 Enable）与 enabled（能力裁决已获准）都是**活着的**
// 状态 → 照常暴露；只有 disabled（停用）被排除。口径：停用 = 收走，裁决通过 = 放行，
// 从未启用过的内置工具 = 常态（core/registry 的初始态）。
func TestServerF1EnabledToolStillExposed(t *testing.T) {
	reg := registry.New(nil)
	exec := &fakeExec{text: "x"}
	regTool(t, reg, "read", exec)
	if err := reg.Enable(context.Background(), "read"); err != nil {
		t.Fatalf("Enable(read): %v", err)
	}

	cs, srv := startServer(t, reg, []string{"read"})

	if got := toolNames(t, cs); len(got) != 1 || got[0] != "read" {
		t.Fatalf("tools/list = %v, want [read]（enabled 不是停用）", got)
	}
	if exp := srv.ExposedTools(); len(exp) != 1 || exp[0] != "read" {
		t.Fatalf("ExposedTools = %v, want [read]", exp)
	}
	if _, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "read"}); err != nil {
		t.Fatalf("enabled 的工具应可调用: %v", err)
	}
	if exec.calls != 1 {
		t.Fatalf("执行器调用次数 = %d, want 1", exec.calls)
	}
}

// TestServerF2RejectsMcpInternalNamespace 钉死 F2：`mcp__*` 是 MCP **client** 侧的内部命名
// 空间，**不上公网面**——白名单点名也拒（不暴露 + Warn）。
//
// 不做"去掉 mcp__ 前缀"的改名：会与内置工具/其它 server 的工具撞名（同名 = 两个不同的
// 执行器抢一个名字 = 调错对象）。选拒绝（fail-close，与空白名单同精神）。
func TestServerF2RejectsMcpInternalNamespace(t *testing.T) {
	reg := registry.New(nil)
	regTool(t, reg, "mcp__fs__x", &fakeExec{text: "x"})
	regTool(t, reg, "read", &fakeExec{text: "r"})

	logger, logs := captureLogger()
	srv, err := mcpserver.New(reg, []string{"mcp__fs__x", "read"}, &mcpserver.Options{Logger: logger})
	if err != nil {
		t.Fatalf("mcpserver.New: %v", err)
	}

	exp := srv.ExposedTools()
	if len(exp) != 1 || exp[0] != "read" {
		t.Fatalf("ExposedTools = %v, want 只 [read]（mcp__fs__x 被拒）", exp)
	}
	if !strings.Contains(logs.String(), "mcp__fs__x") {
		t.Fatalf("拒绝 mcp__ 内部名应告警（点名了哪个），日志 = %q", logs.String())
	}
}

// TestServerF4InvalidToolNameNotExposed 钉死 F4：**非法 MCP 工具名不挂载**（go-sdk 只记一条
// 日志、照样挂上去 = 列表里看得见、调不动的假暴露）+ 告警点名。
func TestServerF4InvalidToolNameNotExposed(t *testing.T) {
	reg := registry.New(nil)
	regTool(t, reg, "bad name", &fakeExec{text: "x"})

	logger, logs := captureLogger()
	srv, err := mcpserver.New(reg, []string{"bad name"}, &mcpserver.Options{Logger: logger})
	if err != nil {
		t.Fatalf("mcpserver.New: %v", err)
	}
	if exp := srv.ExposedTools(); len(exp) != 0 {
		t.Fatalf("非法工具名不该被暴露，ExposedTools = %v", exp)
	}
	if !strings.Contains(logs.String(), "bad name") {
		t.Fatalf("非法工具名应告警（点名了哪个），日志 = %q", logs.String())
	}
}
