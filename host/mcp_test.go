package host_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/mcp"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// 本文件是 v3-S1 MCP client 的**hub 级端到端**测试：真 hub（真事件库 + 真 loop +
// 真 X1 三段写路径）+ 一个真 HTTP MCP server（SDK handler + httptest）。
//
// 断的都是验收条目本身：
//   - 连上 server 后其工具可被模型调用且**三段事件完整**（intent 的 provider id 原样回传）；
//   - 策略链生效（deny-all 默认 → MCP 工具不可用；allow 后才可调）；
//   - 坏 server / 断连 fail-close，**不拖垮 hub**；
//   - MCP 工具命名不与内建工具撞名。

// mcpFakeServer 是一个跑在 HTTP 上的测试替身 MCP server。
type mcpFakeServer struct {
	// url 是 httptest 的地址。
	url string
	// tools 是声明的工具（名字用 wire 名）。
	tools []string
	// failCall 为真时 tools/call 返回协议错误（模拟 server 崩溃）。
	failCall bool
}

// startMCPFakeServer 起一个 HTTP MCP server（Streamable HTTP）。
func startMCPFakeServer(t *testing.T, tools ...string) *mcpFakeServer {
	t.Helper()
	fs := &mcpFakeServer{tools: tools}
	srv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "fake-http", Version: "test"}, &mcpsdk.ServerOptions{})
	for _, name := range tools {
		n := name
		srv.AddTool(&mcpsdk.Tool{
			Name:        n,
			Description: "测试工具 " + n,
			InputSchema: map[string]any{"type": "object"},
		}, func(_ context.Context, req *mcpsdk.CallToolRequest) (*mcpsdk.CallToolResult, error) {
			if fs.failCall {
				return nil, errFakeCrash
			}
			raw, _ := json.Marshal(req.Params.Arguments)
			return &mcpsdk.CallToolResult{
				Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: "mcp-ok " + n + " args=" + string(raw)}},
			}, nil
		})
	}
	ts := httptest.NewServer(mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server { return srv }, nil))
	t.Cleanup(ts.Close)
	fs.url = ts.URL
	return fs
}

// errFakeCrash 是测试里模拟"server 崩了"的哨兵错误。
var errFakeCrash = errFake("fake: server 崩了")

type errFake string

func (e errFake) Error() string { return string(e) }

// mcpProvider 是脚本 provider：第一轮调用指定的 MCP 工具，第二轮收尾。
type mcpProvider struct {
	calls int
	// tool 是首轮要调的工具名（模型"幻觉"出来的名字由测试给）。
	tool string
	// args 是首轮工具调用的参数。
	args string
	// toolCallID 是 provider 侧的黑盒 id（断言"原样回传"用）。
	toolCallID contracts.ProviderId
}

func (p *mcpProvider) Run(ctx context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if p.calls == 1 {
		tc := core.ToolCallReq{ID: p.toolCallID, Name: p.tool, Args: json.RawMessage(p.args)}
		emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}}, nil
	}
	emit(core.ProviderEvent{Kind: "text.delta", Text: "完事"})
	emit(core.ProviderEvent{Kind: "done"})
	return core.ModelResult{Text: "完事"}, nil
}

// hubMCPConfig 造一个只连指定 URL 的 MCP 配置（默认关 → 这里显式开）。
func hubMCPConfig(url string) mcp.Config {
	return mcp.Config{Servers: []mcp.ServerConfig{{
		Name:      "fake",
		Transport: mcp.TransportHTTP,
		URL:       url,
		Enabled:   true,
	}}}
}

// readAllEvents 用第二条连接读整个事件流（测试断言用）。
func readAllEvents(t *testing.T, dbPath string) []contracts.Event {
	t.Helper()
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	evs, err := st.ReadFrom(context.Background(), nil, 1, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	return evs
}

// toolCallEvents 从事件流里筛出某个 tool_call_id 的三段事件。
func toolCallEvents(t *testing.T, evs []contracts.Event, tcID contracts.ProviderId) (intent, done *contracts.Event) {
	t.Helper()
	for i := range evs {
		ev := &evs[i]
		switch ev.Kind {
		case "tool.call.intent":
			var p contracts.ToolCallIntentPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Fatalf("解 intent 载荷: %v", err)
			}
			if p.ToolCallID == tcID {
				intent = ev
			}
		case "tool.call.completed", "tool.call.failed":
			// 两种载荷形状不同（completed 带 Result，failed 带 Error），分别解。
			if ev.Kind == "tool.call.completed" {
				var p contracts.ToolCallCompletedPayload
				if err := json.Unmarshal(ev.Payload, &p); err != nil {
					t.Fatalf("解 completed 载荷: %v", err)
				}
				if p.ToolCallID == tcID {
					done = ev
				}
				continue
			}
			var f contracts.ToolCallFailedPayload
			if err := json.Unmarshal(ev.Payload, &f); err != nil {
				t.Fatalf("解 failed 载荷: %v", err)
			}
			if f.ToolCallID == tcID {
				done = ev
			}
		}
	}
	return intent, done
}

// startMCPFakeServerCounting 起一个**数 DELETE** 的 HTTP MCP server。
//
// 为什么要数 DELETE：Streamable HTTP 的 client 侧 Close 会发一个 DELETE 终止
// session——服务端数到 DELETE，就知道 client 那边**把连接收掉了**（stdio 的
// 子进程同理：进程没了就是收尸了）。这是"host.New 失败路径有没有收尸"的
// 可观测点。
func startMCPFakeServerCounting(t *testing.T, tools ...string) (string, *atomic.Int32) {
	t.Helper()
	srv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "fake-http", Version: "test"}, &mcpsdk.ServerOptions{})
	for _, name := range tools {
		n := name
		srv.AddTool(&mcpsdk.Tool{
			Name:        n,
			Description: "测试工具 " + n,
			InputSchema: map[string]any{"type": "object"},
		}, func(_ context.Context, _ *mcpsdk.CallToolRequest) (*mcpsdk.CallToolResult, error) {
			return &mcpsdk.CallToolResult{
				Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: "mcp-ok " + n}},
			}, nil
		})
	}
	var deletes atomic.Int32
	handler := mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server { return srv }, nil)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletes.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	// 先断客户端连接再关：client 没被收尸时它的 SSE 流还开着，ts.Close 会等
	// 那些 handler（测试替身自己的挂起，不是被测行为）。
	t.Cleanup(func() {
		ts.CloseClientConnections()
		ts.Close()
	})
	return ts.URL, &deletes
}

// TestHubNewFailureClosesMCP：host.New 在**装配 MCP 之后**失败 → MCP 连接必须
// 被收掉（stdio server 的子进程不能变成孤儿进程；"谁拉起谁收尸"对失败路径
// 同样成立）。
//
// 触发点：在线表行数超上限 → onlineCfg.Update 失败（这一步在 MCP 装配**之后**）。
// 观测点：client 收尸时会向 server 发 DELETE（见 startMCPFakeServerCounting）。
func TestHubNewFailureClosesMCP(t *testing.T) {
	url, deletes := startMCPFakeServerCounting(t, "do_thing")
	dbPath := filepath.Join(t.TempDir(), "events.db")
	cfg := host.Config{
		DBPath:   dbPath,
		Model:    "scripted",
		MaxTurns: 4,
		CWD:      t.TempDir(),
		MCP:      hubMCPConfig(url),
		// 超过 loop.MaxOnlineTableRows → onlineCfg.Update 失败（MCP 装配之后）。
		OnlineTableRows: 1000000,
		Provider:        &mcpProvider{tool: "read", args: `{}`, toolCallID: "toolu_X"},
	}
	if _, err := host.New(cfg); err == nil {
		t.Fatalf("在线表行数超上限应让 host.New 失败")
	}
	// MCP 连接必须被收掉（DELETE 到达 server）。
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if deletes.Load() > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("host.New 失败后 MCP 连接没被收掉（server 没收到 DELETE）——子进程/会话成孤儿")
}

// TestHubMCPToolThreeEvents：**验收主条**——连上 MCP server 后其工具可被模型调用，
// 且 X1 三段事件完整（intent 的 provider id 原样回传到 completed）。
func TestHubMCPToolThreeEvents(t *testing.T) {
	fs := startMCPFakeServer(t, "do_thing")
	dbPath := filepath.Join(t.TempDir(), "events.db")
	tcID := contracts.ProviderId("toolu_MCP_BLACKBOX_42")
	prov := &mcpProvider{tool: "mcp__fake__do_thing", args: `{"x":1}`, toolCallID: tcID}

	cfg := host.Config{
		DBPath:   dbPath,
		Model:    "scripted",
		System:   "test-system",
		MaxTurns: 4,
		Allow:    []string{"mcp__fake__do_thing"}, // 策略放行（deny-all 默认的另一半）
		CWD:      t.TempDir(),
		MCP:      hubMCPConfig(fs.url),
		Provider: prov,
	}
	h, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	// 装配面：工具确实以带前缀的名字进了注册表。
	if names := h.MCPToolNames(); len(names) != 1 || names[0] != "mcp__fake__do_thing" {
		t.Fatalf("MCPToolNames = %v，want [mcp__fake__do_thing]", names)
	}
	if !h.MCPEnabled() {
		t.Fatalf("MCPEnabled 应为 true")
	}
	if srvs := h.MCPServers(); len(srvs) != 1 || srvs[0].Name != "fake" {
		t.Fatalf("MCPServers = %+v", srvs)
	}

	sid := host.NewSession()
	res, err := h.RunPrompt(context.Background(), sid, "调一下 MCP 工具")
	if err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if res.ToolCalls != 1 {
		t.Fatalf("ToolCalls = %d，want 1", res.ToolCalls)
	}

	// 三段事件：intent + completed，且 **provider 的 tool_call_id 原样**（陷阱 1）。
	evs := readAllEvents(t, dbPath)
	intent, done := toolCallEvents(t, evs, tcID)
	if intent == nil {
		t.Fatalf("没有 tool.call.intent（tool_call_id=%s）", tcID)
	}
	if done == nil {
		t.Fatalf("没有 tool.call.completed/failed（X1：有意图就要有结果）")
	}
	if done.Kind != "tool.call.completed" {
		t.Fatalf("结果事件 = %q，want tool.call.completed", done.Kind)
	}
	// intent 的因果：completed 的 causation 指向 intent（三段是一条链）。
	if done.CausationID == nil || *done.CausationID != intent.ID {
		t.Fatalf("completed 的 causation 未指向 intent（%v vs %s）", done.CausationID, intent.ID)
	}
	// 事件 id 与 provider id **永不共字段**（陷阱 1）：Event.ID 是 ULID 26 字符，
	// 与 toolu_... 形态的工具调用 id 不同。
	if string(intent.ID) == string(tcID) {
		t.Fatalf("Event.ID 与 provider tool_call_id 撞了（陷阱 1）")
	}
	// 执行结果真的来自 MCP server（解出来断言，别在转义后的原始 JSON 上做 contains）。
	var doneP contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(done.Payload, &doneP); err != nil {
		t.Fatalf("解 completed 载荷: %v", err)
	}
	if len(doneP.Result.Content) == 0 || doneP.Result.Content[0].Text == nil {
		t.Fatalf("completed 没有文本内容: %s", done.Payload)
	}
	gotText := *doneP.Result.Content[0].Text
	if !strings.Contains(gotText, "mcp-ok do_thing") {
		t.Fatalf("MCP server 的返回没回来: %q", gotText)
	}
	// 参数原样到了 server（回显里带 args={"x":1}）。
	if !strings.Contains(gotText, `args={"x":1}`) {
		t.Fatalf("参数没原样到 server: %q", gotText)
	}
	// provider 的 tool_call_id 原样回传（陷阱 1：黑盒 id 一字不改）。
	if doneP.ToolCallID != tcID {
		t.Fatalf("completed 的 tool_call_id = %q，want 原样 %q", doneP.ToolCallID, tcID)
	}
}

// TestHubMCPDenyAllDefault：**deny-all 默认生效**——不显式 allow 时 MCP 工具
// 不可用：模型看得见（注册表里有）但策略拒（H5），走 failed 而不是执行。
func TestHubMCPDenyAllDefault(t *testing.T) {
	fs := startMCPFakeServer(t, "do_thing")
	dbPath := filepath.Join(t.TempDir(), "events.db")
	tcID := contracts.ProviderId("toolu_MCP_DENY_01")
	prov := &mcpProvider{tool: "mcp__fake__do_thing", args: `{}`, toolCallID: tcID}

	cfg := host.Config{
		DBPath:   dbPath,
		Model:    "scripted",
		MaxTurns: 4,
		CWD:      t.TempDir(),
		MCP:      hubMCPConfig(fs.url),
		Provider: prov,
		// **不设 Allow** → 空规则集 → 一切 deny（fail-close，H5）。
	}
	h, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "调一下 MCP 工具"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	evs := readAllEvents(t, dbPath)
	intent, done := toolCallEvents(t, evs, tcID)
	if intent == nil {
		t.Fatalf("没有 intent")
	}
	if done == nil || done.Kind != "tool.call.failed" {
		t.Fatalf("deny-all 下应走 tool.call.failed（得到 %v）", done)
	}
	if !strings.Contains(string(done.Payload), "policy_deny") {
		t.Fatalf("failed 载荷不含 policy_deny：%s", done.Payload)
	}
	// 关键：MCP server **没被调用**（completed 不存在、没有 mcp-ok）。
	for _, ev := range evs {
		if ev.Kind == "tool.call.completed" {
			t.Fatalf("deny-all 下不该有 completed")
		}
	}
}

// TestHubMCPBadServerDoesNotKillHub：坏 server（连不上）→ hub 照常起来、照常跑，
// 只是没有 MCP 工具（fail-close 不拖垮 hub）。
func TestHubMCPBadServerDoesNotKillHub(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "events.db")
	cfg := host.Config{
		DBPath:   dbPath,
		Model:    "scripted",
		MaxTurns: 4,
		CWD:      t.TempDir(),
		// 一个必然连不上的端点。
		MCP:      hubMCPConfig("http://127.0.0.1:1/mcp"),
		Provider: &mcpProvider{tool: "read", args: `{}`, toolCallID: "toolu_X"},
	}
	h, err := host.New(cfg)
	if err != nil {
		t.Fatalf("坏 server 不该让 host.New 失败: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if h.MCPEnabled() {
		t.Fatalf("没有成功装配的 server 时 MCPEnabled 应为 false")
	}
	if names := h.MCPToolNames(); len(names) != 0 {
		t.Fatalf("坏 server 不该注册任何工具：%v", names)
	}
	// 错误被 surfaced（ surfaced 面：MCPErrors）。
	if errs := h.MCPErrors(); len(errs) == 0 {
		t.Fatalf("坏 server 的错误应被 surfaced")
	}
	// hub 照常能跑一轮（内置工具路径不受影响）。
	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "你好"); err != nil {
		t.Fatalf("坏 server 下 RunPrompt 仍应可用: %v", err)
	}
}

// TestHubMCPServerCrashMidCall：server 在调用时崩 → tool.call.failed，
// hub 不拖垮（loop 继续，下一轮模型还能收尾）。
func TestHubMCPServerCrashMidCall(t *testing.T) {
	fs := startMCPFakeServer(t, "do_thing")
	dbPath := filepath.Join(t.TempDir(), "events.db")
	tcID := contracts.ProviderId("toolu_MCP_CRASH_01")
	prov := &mcpProvider{tool: "mcp__fake__do_thing", args: `{}`, toolCallID: tcID}

	cfg := host.Config{
		DBPath:   dbPath,
		Model:    "scripted",
		MaxTurns: 4,
		CWD:      t.TempDir(),
		Allow:    []string{"mcp__fake__do_thing"},
		MCP:      hubMCPConfig(fs.url),
		Provider: prov,
	}
	h, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	// 让 server 在 tools/call 时返回协议错误（= server 内部崩）。
	fs.failCall = true

	sid := host.NewSession()
	res, err := h.RunPrompt(context.Background(), sid, "调一下 MCP 工具")
	if err != nil {
		t.Fatalf("server 崩溃不该让 RunPrompt 失败（loop 应继续）: %v", err)
	}
	if res.Text == "" {
		t.Fatalf("loop 应在工具失败后继续并收尾")
	}
	evs := readAllEvents(t, dbPath)
	intent, done := toolCallEvents(t, evs, tcID)
	if intent == nil {
		t.Fatalf("没有 intent")
	}
	if done == nil || done.Kind != "tool.call.failed" {
		t.Fatalf("server 崩溃应走 tool.call.failed（得到 %v）", done)
	}
}

// TestHubMCPNoServersIsNoOp：**默认形态**——不配任何 server 时与 v2 完全一致
// （不装 MCP、不连外部进程、事件流形状不变）。
func TestHubMCPNoServersIsNoOp(t *testing.T) {
	h := newTestHub(t) // 不设 MCP
	if h.MCPEnabled() {
		t.Fatalf("未配 server 时 MCPEnabled 应为 false")
	}
	if h.MCPToolNames() != nil || h.MCPServers() != nil || h.MCPErrors() != nil {
		t.Fatalf("未配 server 时 MCP 诊断面应全空")
	}
}

// TestHubMCPBadConfigIsFatal：**配置错误是致命的**（与"连接错误非致命"分工）——
// server 名重复 / transport 缺 command 或 url → host.New 拒绝装配。
func TestHubMCPBadConfigIsFatal(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "events.db")
	base := host.Config{DBPath: dbPath, Model: "scripted", MaxTurns: 4, CWD: t.TempDir()}

	dup := base
	dup.MCP = mcp.Config{Servers: []mcp.ServerConfig{
		{Name: "a", Transport: mcp.TransportHTTP, URL: "http://127.0.0.1:1/mcp", Enabled: true},
		{Name: "a", Transport: mcp.TransportHTTP, URL: "http://127.0.0.1:2/mcp", Enabled: true},
	}}
	if _, err := host.New(dup); err == nil {
		t.Fatalf("server 名重复应被拒（fail-close）")
	}

	noCmd := base
	noCmd.MCP = mcp.Config{Servers: []mcp.ServerConfig{{Name: "a", Enabled: true}}}
	if _, err := host.New(noCmd); err == nil {
		t.Fatalf("stdio 缺 command 应被拒")
	}
}

// TestHubMCPBuiltinCollisionSafe：MCP server 声明与内置同名的工具 → 不遮蔽内置
// （前缀隔离），内置 read 照常工作。
func TestHubMCPBuiltinCollisionSafe(t *testing.T) {
	fs := startMCPFakeServer(t, "read", "write", "bash")
	target := filepath.Join(t.TempDir(), "t.txt")
	if err := os.WriteFile(target, []byte("内置 read 的内容"), 0o600); err != nil {
		t.Fatalf("写文件: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), "events.db")
	tcID := contracts.ProviderId("toolu_MCP_BUILTIN_01")
	// 模型调的是**内置** read（不是 mcp__fake__read）。
	prov := &mcpProvider{tool: "read", args: `{"path":"` + filepath.ToSlash(target) + `"}`, toolCallID: tcID}

	h, err := host.New(host.Config{
		DBPath:   dbPath,
		Model:    "scripted",
		MaxTurns: 4,
		Allow:    []string{"read"},
		CWD:      t.TempDir(),
		MCP:      hubMCPConfig(fs.url),
		Provider: prov,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "读文件"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	evs := readAllEvents(t, dbPath)
	intent, done := toolCallEvents(t, evs, tcID)
	if intent == nil || done == nil || done.Kind != "tool.call.completed" {
		t.Fatalf("内置 read 应正常完成（intent=%v done=%v）", intent, done)
	}
	// 返回的是**文件内容**，不是 MCP server 的 "mcp-ok read"。
	if !strings.Contains(string(done.Payload), "内置 read 的内容") {
		t.Fatalf("内置 read 的返回不对：%s", done.Payload)
	}
	if strings.Contains(string(done.Payload), "mcp-ok") {
		t.Fatalf("内置 read 被 MCP 工具遮蔽了（前缀隔离失效）")
	}
}
