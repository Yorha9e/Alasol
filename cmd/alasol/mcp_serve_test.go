package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

// CLI 级钉子（v4-5 B 类 MCP server）：真起一个 `alasol mcp-serve` 子进程，用 go-sdk client
// 经 stdio 管道接入，验：
//   - initialize / tools/list / tools/call 全链跑通（真 hub 装配 + 真 read 执行器）；
//   - **白名单生效**：配置 expose_tools=["read"] → 只暴露 read；
//   - **stdout 纯净**：stdout 逐行都是合法 JSON-RPC 帧，无日志/banner 泄漏（tee 捕获）。
//
// 这一层是 runtime/mcpserver 的 in-memory 测试测不到的真实出口：进程级 stdout 会不会被
// hub 装配日志 / banner 污染，只有真起进程才知道（stdout 纯净性铁律，与 acp 同纪律）。

func TestMCPServe_CLI_EndToEndAndStdoutPurity(t *testing.T) {
	if testing.Short() {
		t.Skip("短跑模式跳过（要真起子进程）")
	}
	bin := buildACPBin(t) // 复用：编的就是 alasol 二进制（支持 mcp-serve 子命令）
	dir := t.TempDir()

	// 一个待读的文件（read 工具会读它）。
	helloPath := filepath.Join(dir, "hello.txt")
	const helloBody = "MCP-SERVER-HELLO-CONTENT"
	if err := os.WriteFile(helloPath, []byte(helloBody), 0o600); err != nil {
		t.Fatalf("写测试文件: %v", err)
	}

	// 配置文件：声明暴露白名单（fail-close 语义的来源）。**隔离用户配置**：写到临时路径，
	// 不读平台默认位（否则开发机的 %APPDATA%\alasol\config.toml 会渗进这条钉子）。
	cfgPath := filepath.Join(dir, "alasol.toml")
	cfgBody := "model = \"dummy\"\n\n[mcp_server]\nexpose_tools = [\"read\"]\n"
	if err := os.WriteFile(cfgPath, []byte(cfgBody), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}

	cmd := exec.Command(bin,
		"mcp-serve",
		"-config", cfgPath,
		"-db", filepath.Join(dir, "events.db"),
		"-cwd", dir,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	// tee：把子进程 stdout 的每个字节抄一份进 rawStdout（供纯净性断言），同时转给 client 读。
	var rawStdout bytes.Buffer
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("起 alasol mcp-serve: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// go-sdk client 经 stdio 管道接入（Reader=子进程 stdout，Writer=子进程 stdin）。
	client := mcp.NewClient(&mcp.Implementation{Name: "cli-test-client", Version: "1"}, nil)
	cs, err := client.Connect(ctx, &mcp.IOTransport{
		Reader: &teeReadCloser{src: stdout, dst: &rawStdout},
		Writer: stdin,
	}, nil)
	if err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("client Connect（initialize 握手）失败: %v\nstderr:\n%s", err, stderr.String())
	}

	// tools/list：白名单只放了 read → 只暴露 read。
	listRes, err := cs.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		_ = cs.Close()
		_ = cmd.Process.Kill()
		t.Fatalf("tools/list: %v\nstderr:\n%s", err, stderr.String())
	}
	var names []string
	for _, tl := range listRes.Tools {
		names = append(names, tl.Name)
	}
	if len(names) != 1 || names[0] != "read" {
		_ = cs.Close()
		_ = cmd.Process.Kill()
		t.Fatalf("tools/list = %v, want 只 [read]（白名单 fail-close）\nstderr:\n%s", names, stderr.String())
	}

	// tools/call：真调 read 执行器读文件。
	callRes, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "read",
		Arguments: json.RawMessage(`{"path":` + jsonString(helloPath) + `}`),
	})
	if err != nil {
		_ = cs.Close()
		_ = cmd.Process.Kill()
		t.Fatalf("tools/call(read): %v\nstderr:\n%s", err, stderr.String())
	}
	if callRes.IsError {
		_ = cs.Close()
		_ = cmd.Process.Kill()
		t.Fatalf("read 调用自报 isError（不该）\nstderr:\n%s", stderr.String())
	}
	if !anyTextContains(callRes, helloBody) {
		_ = cs.Close()
		_ = cmd.Process.Kill()
		t.Fatalf("read 结果不含文件内容 %q\nstderr:\n%s", helloBody, stderr.String())
	}

	// 收场：关 client（→ 关管道 → 子进程 stdin EOF → 正常退出）。
	_ = cs.Close()
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()
	select {
	case werr := <-waitErr:
		if werr != nil {
			t.Fatalf("alasol mcp-serve 应正常退出（stdin EOF → 收场）: %v\nstderr:\n%s", werr, stderr.String())
		}
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("alasol mcp-serve 没在超时内退出")
	}

	// stdout 纯净性：tee 捕获的每一行都必须是合法 JSON-RPC 帧（无日志/banner 泄漏）。
	assertStdoutIsPureJSONRPC(t, rawStdout.String(), stderr.String())
}

// assertStdoutIsPureJSONRPC 断言 stdout 逐行都是合法 JSON-RPC 帧（jsonrpc=2.0）。
func assertStdoutIsPureJSONRPC(t *testing.T, stdout, stderr string) {
	t.Helper()
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	if len(lines) == 0 || (len(lines) == 1 && strings.TrimSpace(lines[0]) == "") {
		t.Fatalf("stdout 是空的（不该：至少该有 initialize/tools/list/tools/call 三个响应）\nstderr:\n%s", stderr)
	}
	for i, ln := range lines {
		var probe struct {
			JSONRPC string `json:"jsonrpc"`
		}
		if err := json.Unmarshal([]byte(ln), &probe); err != nil || probe.JSONRPC != "2.0" {
			t.Fatalf("stdout 第 %d 行不是合法 JSON-RPC 帧（日志/banner 泄漏？）: %q\nstderr:\n%s", i, ln, stderr)
		}
	}
}

// teeReadCloser 包一层 io.ReadCloser：读 src 的同时把读到的字节抄进 dst（供纯净性断言），
// Close 委托给 src（关掉子进程的 stdout 管道）。
type teeReadCloser struct {
	src io.ReadCloser
	dst *bytes.Buffer
}

func (r *teeReadCloser) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 {
		r.dst.Write(p[:n])
	}
	return n, err
}

func (r *teeReadCloser) Close() error { return r.src.Close() }

func anyTextContains(res *mcp.CallToolResult, sub string) bool {
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok && strings.Contains(tc.Text, sub) {
			return true
		}
	}
	return false
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
