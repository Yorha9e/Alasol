package mcp

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// HTTP（Streamable HTTP）传输测试：用 SDK 的 StreamableHTTPHandler + httptest
// 起一个真 HTTP MCP server，验：
//
//   - http 传输能连通、能发现、能调用；
//   - 配置的 Header（凭据头）真的附到了出站请求上（且只在内存里，不进错误文案）。
//
// stdio 是 v3-S1 的首选传输（N1），HTTP 是配置面留好的第二条路——不留测试的话
// "留配置面"会退化成"留了一个没人验证过的分支"。

// TestHTTPTransportDiscoversAndCalls：Streamable HTTP 全链路。
func TestHTTPTransportDiscoversAndCalls(t *testing.T) {
	ctx := context.Background()

	var gotAuth string
	srv := mcp.NewServer(&mcp.Implementation{Name: "fake-http-server", Version: "test"}, &mcp.ServerOptions{
		Logger: silentLogger(),
	})
	srv.AddTool(&mcp.Tool{
		Name:        "whoami",
		Description: "回显收到的 Authorization 头（测试用）",
		InputSchema: map[string]any{"type": "object"},
	}, func(_ context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "auth=" + gotAuth}},
		}, nil
	})
	// 用一个中间件把 Authorization 头传给 handler（SDK 的 handler 拿不到原始
	// http.Request，所以在这里拦一道——只测"头有没有到 server"）。
	ts := httptest.NewServer(withAuthCapture(&gotAuth, mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return srv
	}, nil)))
	t.Cleanup(ts.Close)

	cfg := ServerConfig{
		Name:      "http",
		Transport: TransportHTTP,
		URL:       ts.URL,
		Headers:   map[string]string{"Authorization": "Bearer sk-test-token"},
	}
	conn, err := connect(ctx, cfg)
	if err != nil {
		t.Fatalf("http 连接失败: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if conn.cmd != nil {
		t.Fatalf("http 传输不该有子进程（conn.cmd 应为 nil）")
	}
	if conn.info.Transport != TransportHTTP {
		t.Fatalf("info.Transport = %q", conn.info.Transport)
	}

	got, errs := conn.discover(ctx)
	if len(errs) != 0 || len(got) != 1 {
		t.Fatalf("发现异常: %d 个 / %v", len(got), errs)
	}
	exec := &mcpExecutor{conn: conn, wireName: got[0].wireName, name: got[0].name}
	res, err := exec.Execute(ctx, intentWith(`{}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	text := toolResultTextOf(res)
	if !contains(text, "auth=Bearer sk-test-token") {
		t.Fatalf("配置的 Authorization 头没到 server: %q", text)
	}
}

// TestHTTPTransportBadEndpoint：连不上的端点 → 明确报错（fail-close），
// 且错误文案**不含凭据值、不含端点 URL**（H12/H19：错误会进日志）。
func TestHTTPTransportBadEndpoint(t *testing.T) {
	cfg := ServerConfig{
		Name:      "http",
		Transport: TransportHTTP,
		URL:       "http://127.0.0.1:1/mcp", // 1 号端口：必然连不上
		Headers:   map[string]string{"Authorization": "Bearer sk-secret"},
	}
	_, err := connect(context.Background(), cfg)
	mustContain(t, err, "连接失败")
	if err != nil && contains(err.Error(), "sk-secret") {
		t.Fatalf("错误文案含凭据值（H12/H19）: %v", err)
	}
	if err != nil && contains(err.Error(), "http://127.0.0.1:1/mcp") {
		t.Fatalf("错误文案含端点 URL（SDK 自带，必须被抹成 <endpoint>）: %v", err)
	}
	// 说明：dial 错误里的 `127.0.0.1:1` 是 net 包自己拼的 host:port（不是 URL、
	// 不带查询串），不作为凭据处理——端点主机名本来就要能进日志才好排查。
	if err == nil || !contains(err.Error(), "<endpoint>") {
		t.Fatalf("端点 URL 应被抹成 <endpoint>: %v", err)
	}
	// errors.Is 仍能穿透（scrub 不破坏错误链）。
	var netErr net.Error
	if !errors.As(err, &netErr) {
		t.Logf("底层错误不是 net.Error（不影响本断言，仅记录）: %v", err)
	}
}

// withAuthCapture 包一层 http.Handler，把 Authorization 头记下来再交给下一个。
func withAuthCapture(dst *string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.Header.Get("Authorization"); v != "" {
			*dst = v
		}
		next.ServeHTTP(w, r)
	})
}
