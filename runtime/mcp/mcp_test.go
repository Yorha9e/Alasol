package mcp

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/registry"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// 本文件是 runtime/mcp 测试的**进程内 MCP server 替身**：用官方 SDK 的
// in-memory transport（NewInMemoryTransports，net.Pipe 之上跑换行分隔 JSON-RPC）
// 起一个真 MCP server，于是发现/调用/断连/N2 全部走**真协议路径**，不需要外部进程。
//
// 为什么优先 in-memory 而不是全用 stdio 子进程：
//   - 它跑的是同一条 wire 协议（ioConn + jsonrpc2），不是 mock；
//   - 无进程、无端口、无时序抖动 → 测试确定且快；
//   - stdio 子进程那条路（exec + IOTransport + 收尸）另有一份专门测试
//     （stdio_test.go），两者互补：in-memory 覆盖协议语义，stdio 覆盖进程生命周期。

// silentLogger 是测试替身 server 用的 logger（SDK 的 ServerOptions.Logger 非 nil
// 时它会把连接细节写出来；测试里默认只想看见失败信息，所以静默）。
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// fakeTool 是测试 server 声明的一个工具。
type fakeTool struct {
	// name 是 wire 上的工具名（**原样**，用来验证命名前缀不影响 wire）。
	name string
	// description 是声明给 client 的说明。
	description string
	// schema 是声明给 client 的 inputSchema（nil → 兜底 {"type":"object"}）。
	schema map[string]any
	// text 是 tools/call 时返回的文本。
	text string
	// isError 让工具自报失败（MCP isError=true：业务错误，不是协议错误）。
	isError bool
	// structured 是结构化输出（CallToolResult.StructuredContent）。
	structured any
	// echo 为真时把入参回显进文本（断言"参数原样过桥"）。
	echo bool
	// handler 覆盖默认行为（测试断连/崩溃用：直接返回 error）。
	handler mcp.ToolHandler
}

// fakeHooks 是测试观测点（并发安全；由 server 侧的工具 handler 写）。
type fakeHooks struct {
	// calls 记录每次 tools/call 收到的**原始参数**（server 侧视角）。
	calls chan toolCallRecord
}

// toolCallRecord 是一次 tools/call 的原始入参（server 侧视角）。
type toolCallRecord struct {
	Name string
	Args json.RawMessage
}

func newFakeHooks() *fakeHooks {
	return &fakeHooks{calls: make(chan toolCallRecord, 64)}
}

// recordCall 记一次调用（不阻塞：channel 有缓冲，满了就丢——测试只查前几次）。
func (h *fakeHooks) recordCall(rec toolCallRecord) {
	select {
	case h.calls <- rec:
	default:
	}
}

// startFake 起一个 in-memory MCP server，返回**client 侧**的 transport、观测钩子
// 与 server 侧会话（测试用 server 会话主动发请求，如 N2 探针）。
//
// SDK 约束：**server 必须先 Connect，client 后 Connect**（client 在连接时发
// initialize；net.Pipe 是无缓冲的同步管道，两边都得有人读）。
func startFake(t *testing.T, tools ...fakeTool) (mcp.Transport, *fakeHooks, *mcp.ServerSession) {
	t.Helper()
	return startFakeMW(t, nil, tools...)
}

// startFakeMW 起一个带**接收中间件**的 in-memory MCP server（模拟坏 server：
// 挂死的 tools/list / tools/call）。
//
// 中间件是 server 侧的接收钩子：它在 SDK 的默认 handler 之前跑，所以能精确地
// 让某个方法"不返回"——这正是坏 server 的形态（握手正常，某个请求不吭声）。
func startFakeMW(t *testing.T, mw mcp.Middleware, tools ...fakeTool) (mcp.Transport, *fakeHooks, *mcp.ServerSession) {
	t.Helper()
	hooks := newFakeHooks()
	srv := mcp.NewServer(&mcp.Implementation{Name: "fake-mcp-server", Version: "test"}, &mcp.ServerOptions{
		Logger: silentLogger(),
	})
	for _, ft := range tools {
		addFakeTool(srv, ft, hooks)
	}
	if mw != nil {
		srv.AddReceivingMiddleware(mw)
	}
	clientT, serverT := mcp.NewInMemoryTransports()
	ss, err := srv.Connect(context.Background(), serverT, nil)
	if err != nil {
		t.Fatalf("起测试替身 server 失败: %v", err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	return clientT, hooks, ss
}

// addFakeTool 把一个 fakeTool 挂到 server 上。
func addFakeTool(srv *mcp.Server, ft fakeTool, hooks *fakeHooks) {
	schema := ft.schema
	if schema == nil {
		schema = map[string]any{"type": "object"}
	}
	h := ft.handler
	if h == nil {
		h = func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			raw, _ := json.Marshal(req.Params.Arguments)
			hooks.recordCall(toolCallRecord{Name: req.Params.Name, Args: raw})
			text := ft.text
			if ft.echo {
				text = text + " args=" + string(raw)
			}
			return &mcp.CallToolResult{
				Content:           []mcp.Content{&mcp.TextContent{Text: text}},
				IsError:           ft.isError,
				StructuredContent: ft.structured,
			}, nil
		}
	}
	srv.AddTool(&mcp.Tool{
		Name:        ft.name,
		Description: ft.description,
		InputSchema: schema,
	}, h)
}

// connectFake 用一个 in-memory transport 连上测试替身 server。
//
// 走的是产物路径 connectSession（不是测试专用分支）——握手 + tools/list 都真跑。
func connectFake(ctx context.Context, t *testing.T, serverName string, tr mcp.Transport) *serverConn {
	t.Helper()
	conn, err := connectSession(ctx, ServerConfig{Name: serverName, Transport: TransportStdio}, tr, nil)
	if err != nil {
		t.Fatalf("连接测试替身 server %q 失败: %v", serverName, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// assertToolRegistered 断言某工具已注册且描述符合 MCP 的保守缺省，返回描述。
func assertToolRegistered(t *testing.T, reg *registry.Registry, name string) contracts.Tool {
	t.Helper()
	e, ok := reg.Get(name)
	if !ok {
		t.Fatalf("工具 %q 未注册（已注册：%v）", name, registeredNames(reg))
	}
	if e.Tool == nil {
		t.Fatalf("工具 %q 注册了但没有描述（Entry.Tool 为 nil）", name)
	}
	if _, ok := e.Impl.(interface {
		Execute(context.Context, contracts.ToolCallIntentPayload) (contracts.ToolResult, error)
	}); !ok {
		t.Fatalf("工具 %q 的注册实现不是 core.ToolExecutor（实际 %T）", name, e.Impl)
	}
	return *e.Tool
}

// registeredNames 返回注册表里的工具名（断言失败信息用）。
func registeredNames(reg *registry.Registry) []string {
	var out []string
	for _, e := range reg.List(registry.ExtensionKindTool) {
		out = append(out, e.Meta.Name)
	}
	return out
}

// mustContain 断言 err 非 nil 且信息含 want（否则测试失败）。
func mustContain(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("期望错误含 %q，得到 nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("错误信息不含 %q：%v", want, err)
	}
}

// contains 是字符串包含断言（测试可读性用）。
func contains(s, sub string) bool { return strings.Contains(s, sub) }

// toolCallIntent 是 contracts.ToolCallIntentPayload 的短别名（测试可读性用；
// 不 import contracts 到每个测试文件）。
type toolCallIntent = contracts.ToolCallIntentPayload

// intentWith 造一个只带参数的 intent（测试辅助）。
func intentWith(args string) toolCallIntent {
	return toolCallIntent{Arguments: json.RawMessage(args)}
}
