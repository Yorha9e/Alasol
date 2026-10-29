//go:build windows

package mcp

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// stdio 传输的**真进程**测试：exec 一个子进程（本测试二进制自身，由环境变量切到
// fake server 模式），走 IOTransport 交换换行分隔 JSON-RPC，然后验：
//
//   - windowsHide 真的设上了（陷阱 5：不起控制台窗口）；
//   - env 真的传进去了（含显式注入的凭据键，且父 env 的凭据类键被剥掉）；
//   - Close 之后子进程真的退了（谁拉起谁收尸，不留孤儿/zombie）；
//   - 装睡（不理会 stdin EOF）的 server 也能在**有界时间**内被收尸。
//
// 为什么用本测试二进制当 server：零外部依赖（不需要 node/npx），且跨平台一致。
// 子进程模式见 TestMain / fakeServerEnv。

// fakeServerEnv 是子进程模式的环境变量名（父进程经 ServerConfig.Env 传进去）。
//
// 用 env 而不是命令行 flag：测试二进制自己有 flag 集，未知 flag 会让它
// flag.ExitOnError 直接退掉（子进程秒死，报错还是难懂的 "flag provided but
// not defined"）。env 没有这个问题，且天然走我们正在测的那条 childEnv 路径。
const fakeServerEnv = "ALASOL_TEST_MCP_FAKE_SERVER"

// TestMain 在子进程模式（fakeServerEnv 有值）下跑 fake MCP server 并退出，
// 不跑任何测试——否则子进程会去跑整个测试套件，而它的 stdout 是 MCP 管道，
// 任何测试输出都会污染协议。
func TestMain(m *testing.M) {
	if mode := os.Getenv(fakeServerEnv); mode != "" {
		runFakeServer(mode)
		return // 不跑测试套件；退出码 0
	}
	os.Exit(m.Run())
}

// TestStdioSpawnAndReap 走完整的 stdio 装配路径（exec → 握手 → 发现 → 调用 → 收尸）。
func TestStdioSpawnAndReap(t *testing.T) {
	if testing.Short() {
		t.Skip("stdio 测试起真进程，-short 下跳过")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 显式注入的键：必须到达子进程，且必须 scrub 不掉（那是它的入口）。
	cfg := ServerConfig{
		Name:      "stdio",
		Transport: TransportStdio,
		Command:   selfExe(t),
		Env: map[string]string{
			fakeServerEnv:          "stdio",
			"ALASOL_TEST_INJECTED": "injected-value",
		},
		EnvFrom: []string{"ALASOL_TEST_INHERITED"},
	}
	t.Setenv("ALASOL_TEST_INHERITED", "inherited-value")
	// 父 env 里放一个凭据类键：必须**不会**漏进子进程（ScrubEnv）。
	t.Setenv("ALASOL_TEST_SECRET_KEY", "sk-should-not-leak")

	conn, err := connect(ctx, cfg)
	if err != nil {
		t.Fatalf("stdio 连接失败: %v", err)
	}
	if conn.cmd == nil {
		t.Fatalf("stdio 传输必须持有子进程句柄（谁拉起谁收尸）")
	}
	assertHiddenWindow(t, conn.cmd)

	// 发现：fake server 声明一个工具。
	got, errs := conn.discover(ctx)
	if len(errs) != 0 || len(got) != 1 {
		t.Fatalf("发现异常: %d 个 / %v", len(got), errs)
	}
	if got[0].name != "mcp__stdio__ping" {
		t.Fatalf("注册名 = %q", got[0].name)
	}

	// 调用：fake server 把收到的 env 摘要回显出来，用来验 env 纪律。
	exec := &mcpExecutor{conn: conn, wireName: got[0].wireName, name: got[0].name}
	res, err := exec.Execute(ctx, intentWith(`{"echo":"hello"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	text := toolResultTextOf(res)
	if !strings.Contains(text, "injected-value") {
		t.Fatalf("显式注入的 env 没到子进程: %q", text)
	}
	if !strings.Contains(text, "inherited-value") {
		t.Fatalf("env_from 继承的 env 没到子进程: %q", text)
	}
	if strings.Contains(text, "should-not-leak") {
		t.Fatalf("父 env 的凭据类键漏进了子进程（ScrubEnv 失效）: %q", text)
	}

	// 收尸：Close 之后进程必须退出（ProcessState 非 nil = cmd.Wait 已返回）。
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if conn.cmd.ProcessState == nil {
		t.Fatalf("Close 后 cmd.Wait 未返回（子进程没被收尸）")
	}
}

// TestStdioCommandNotFound：命令不存在 → 明确报错，不 panic、不留孤儿。
func TestStdioCommandNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("stdio 测试起真进程，-short 下跳过")
	}
	_, err := connect(context.Background(), ServerConfig{
		Name:      "nope",
		Transport: TransportStdio,
		Command:   "alasol-definitely-not-a-real-executable-xyz",
	})
	mustContain(t, err, "启动")
}

// TestStdioCloseKillsStubbornServer：装睡的 server（不理会 stdin EOF）也能在
// **有界时间**内被收尸——杀进程树那一步必须真的生效。
func TestStdioCloseKillsStubbornServer(t *testing.T) {
	if testing.Short() {
		t.Skip("stdio 测试起真进程，-short 下跳过")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := ServerConfig{
		Name:      "stubborn",
		Transport: TransportStdio,
		Command:   selfExe(t),
		Env:       map[string]string{fakeServerEnv: "stubborn"}, // 收到 stdin EOF 也不退
	}
	conn, err := connect(ctx, cfg)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}

	start := time.Now()
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if conn.cmd.ProcessState == nil {
		t.Fatalf("装睡的 server 没被收尸")
	}
	// gracePeriod(2s) + killGrace(5s) = ~7s 量级；放宽到 20s 仍能抓住"永久挂起"。
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Fatalf("收尸耗时 %v（应被 gracePeriod + killGrace 压住）", elapsed)
	}
}

// TestStdioServerStderrReachesLog：server 的 stderr **不被丢弃**——它进
// mcpLog（脱敏后）。外部 server 的启动失败原因、崩溃栈只在 stderr 上，
// 丢掉它等于"server 挂了但看不出为什么"（p3）。
//
// 同时钉住脱敏：server 完全可以把凭据打进 stderr，凭据值不进日志是不可
// 让渡的纪律（H12/H19）。
func TestStdioServerStderrReachesLog(t *testing.T) {
	if testing.Short() {
		t.Skip("stdio 测试起真进程，-short 下跳过")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var buf bytes.Buffer
	old := mcpLog
	mcpLog = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	defer func() { mcpLog = old }()

	const injected = "injected-stderr-secret-value"
	cfg := ServerConfig{
		Name:      "stderr",
		Transport: TransportStdio,
		Command:   selfExe(t),
		Env: map[string]string{
			fakeServerEnv:          "stderr",
			"ALASOL_TEST_INJECTED": injected,
		},
	}
	conn, err := connect(ctx, cfg)
	if err != nil {
		t.Fatalf("stdio 连接失败: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// stderr 是异步到达的（exec 的拷贝 goroutine）——有界等它。
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "fake-server 启动") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	out := buf.String()
	if !strings.Contains(out, "fake-server 启动") {
		t.Fatalf("server 的 stderr 没进日志（被丢弃了）: %q", out)
	}
	if strings.Contains(out, injected) {
		t.Fatalf("stderr 里的凭据值没打码: %q", out)
	}
	if !strings.Contains(out, "***") {
		t.Fatalf("stderr 里没有打码标记: %q", out)
	}
}

// selfExe 返回当前测试二进制的路径（子进程模式用它自己当 fake server）。
func selfExe(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("取测试二进制路径失败: %v", err)
	}
	return exe
}

// assertHiddenWindow 断言子进程带上了 windowsHide（陷阱 5）。
func assertHiddenWindow(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd.SysProcAttr == nil {
		t.Fatalf("子进程没有 SysProcAttr（windowsHide 没设）")
	}
	// 反射取字段：syscall.SysProcAttr 的字段集按平台不同，直接引用字段名在别的
	// 平台编译不过（runtime/cap/local 的测试同款做法）。
	v := reflect.ValueOf(cmd.SysProcAttr).Elem().FieldByName("HideWindow")
	if !v.IsValid() {
		t.Fatalf("SysProcAttr（%T）没有 HideWindow 字段", cmd.SysProcAttr)
	}
	if !v.Bool() {
		t.Fatalf("HideWindow 未开启（会弹控制台窗口，陷阱 5）")
	}
}

// runFakeServer 在子进程里跑 fake MCP server（stdio 上）。
//
//	"stdio" 模式：起 MCP server（声明一个回显 env 摘要的 ping 工具），阻塞到
//	  stdin EOF（对端关闭）——守规矩的 server。
//	"stubborn" 模式：**照样完成握手**（server 在后台 goroutine 里跑），但主体
//	  装睡不理 stdin EOF——用来验杀进程树那一步真的生效（只杀直接子进程 +
//	  管道被主体攥着的场景）。
//	"stderr" 模式：起 server 前往 stderr 打一行（含一个凭据值）——用来验
//	  "server 的 stderr 不被丢弃 + 凭据值被打码"。
func runFakeServer(mode string) {
	srv := mcp.NewServer(&mcp.Implementation{Name: "fake-stdio-server", Version: "test"}, &mcp.ServerOptions{
		Logger: silentLogger(),
	})
	srv.AddTool(&mcp.Tool{
		Name:        "ping",
		Description: "回显收到的 env 摘要（测试用）",
		InputSchema: map[string]any{"type": "object"},
	}, func(_ context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: envDigest()}},
		}, nil
	})
	// StdioTransport 用 os.Stdin/os.Stdout——正是本包 exec 出来的管道。
	switch mode {
	case "stdio":
		if err := srv.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
			panic(err)
		}
	case "stubborn":
		go func() { _ = srv.Run(context.Background(), &mcp.StdioTransport{}) }()
		time.Sleep(10 * time.Minute) // 主体装睡：不理 stdin EOF
	case "stderr":
		// stderr 诊断行（含凭据值——父进程断言它被打码）。
		fmt.Fprintf(os.Stderr, "fake-server 启动 token=%s\n", os.Getenv("ALASOL_TEST_INJECTED"))
		if err := srv.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
			panic(err)
		}
	default:
		panic("未知 fake server 模式: " + mode)
	}
}

// envDigest 汇总子进程 env 里测试关心的键（父进程断言 env 纪律用）。
func envDigest() string {
	var sb strings.Builder
	for _, k := range []string{"ALASOL_TEST_INJECTED", "ALASOL_TEST_INHERITED", "ALASOL_TEST_SECRET_KEY"} {
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(os.Getenv(k))
		sb.WriteString("\n")
	}
	return sb.String()
}
