package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
)

// hubFlags 是 serve / chat 共有的装配参数（都走 host.Hub 这个唯一组合根）。
type hubFlags struct {
	dbPath         *string
	addr           *string
	model          *string
	baseURL        *string
	apiKeyEnv      *string
	approval       *string
	allow          *string
	ask            *string
	maxTurns       *int
	contextBudget  *int64
	system         *string
	indexerBatch   *int
	maxSessionRows *int
}

// registerHubFlags 把共有 flag 注册到 fs（serve 额外有 -addr，chat 也有但可忽略）。
func registerHubFlags(fs *flag.FlagSet) *hubFlags {
	return &hubFlags{
		dbPath:         fs.String("db", "alasol.db", "事件库路径（SQLite）"),
		addr:           fs.String("addr", "127.0.0.1:8720", "监听地址（只接受 loopback，N5b）"),
		model:          fs.String("model", "gpt-4o-mini", "模型名（原样传给 provider）"),
		baseURL:        fs.String("base-url", "", "OpenAI 兼容端点（空=官方）"),
		apiKeyEnv:      fs.String("api-key-env", "OPENAI_API_KEY", "持有 API key 的环境变量名"),
		approval:       fs.String("approval", "deny-all", "审批策略：deny-all | always-allow | terminal"),
		allow:          fs.String("allow", "", "允许的工具名（逗号分隔；空=一切 deny）"),
		ask:            fs.String("ask", "", "需要人审的工具名（逗号分隔；命中即去问 Approver）"),
		maxTurns:       fs.Int("max-turns", 0, "最大轮数（0=默认 20）"),
		contextBudget:  fs.Int64("context-budget", 0, "上下文窗口预算 token（0=默认；只用于占比）"),
		system:         fs.String("system", "", "系统提示"),
		indexerBatch:   fs.Int("indexer-batch", 0, "索引器单批处理上限（0=默认）"),
		maxSessionRows: fs.Int("max-session-rows", 0, "/api/sessions 行数上限（0=默认 200，K15）"),
	}
}

// config 把 flag 值转成 host.Config（唯一装配处）。
func (f *hubFlags) config() host.Config {
	cfg := host.Config{
		DBPath:         *f.dbPath,
		Addr:           *f.addr,
		Model:          *f.model,
		System:         *f.system,
		MaxTurns:       *f.maxTurns,
		ContextBudget:  *f.contextBudget,
		Allow:          splitList(*f.allow),
		Ask:            splitList(*f.ask),
		Approval:       *f.approval,
		BaseURL:        *f.baseURL,
		APIKeyEnv:      *f.apiKeyEnv,
		IndexerBatch:   *f.indexerBatch,
		MaxSessionRows: *f.maxSessionRows,
	}
	if testProviderOverride != nil {
		// 测试注入的脚本 provider（产物代码永远不设它 → 恒为真 provider）。
		cfg.Provider = testProviderOverride
	}
	return cfg
}

// testProviderOverride 是**仅供测试**的 provider 注入点（nil = 用真 provider）。
//
// 为什么要有它：serve/chat 的唯一装配处是 host.Hub，而 host.New 只认
// Config.Provider。CLI 测试要跑"真命令路径"（真 flag 解析 → 真 hub → 真事件库），
// 又不能真的去打模型 API，于是留这一个进程内注入点。它在 main 的装配函数里被读，
// 与 runnerOpts.provider 同性质（见 wire.go）。
var testProviderOverride core.Provider

// splitList 把逗号分隔的工具名切开（空项丢掉）。
func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// serveMain 是 `alasol serve`：起 hub + HTTP/WS，阻塞到 Ctrl-C。
func serveMain(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	f := registerHubFlags(fs)
	_ = fs.Parse(args)

	h, err := host.New(f.config())
	if err != nil {
		fmt.Fprintln(os.Stderr, "装配失败:", err)
		return 1
	}
	defer h.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := serveHub(ctx, h); err != nil {
		fmt.Fprintln(os.Stderr, "HTTP 服务失败:", err)
		return 1
	}
	return 0
}

// serveHub 起 HTTP 服务并阻塞到 ctx 取消（serveMain 与测试共用同一段：
// 测试用可取消的 ctx，产物用信号 ctx）。
func serveHub(ctx context.Context, h *host.Hub) error {
	srv := &http.Server{
		Handler:           h.Handler(),
		ReadHeaderTimeout: 10 * time.Second, // 防慢头攻击（loopback 上也别裸奔）
	}
	ln, err := net.Listen("tcp", h.Addr())
	if err != nil {
		return fmt.Errorf("监听 %s: %w", h.Addr(), err)
	}
	printBanner(ln.Addr().String())

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	case <-ctx.Done():
		fmt.Fprintf(os.Stderr, "\n[alasol] 收到退出信号，正在收尾…\n")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}

// printBanner 打印就绪信息（地址从**真实监听器**取，端口是 0 时也能显示实际值）。
func printBanner(addr string) {
	fmt.Printf("[alasol] hub 已就绪\n")
	fmt.Printf("  Web UI   http://%s/\n", addr)
	fmt.Printf("  事件流   ws://%s/ws（NDJSON）\n", addr)
	fmt.Printf("  在线表   http://%s/api/sessions\n", addr)
	fmt.Printf("  上下文   http://%s/api/context/{session_id}\n", addr)
	fmt.Printf("  健康     http://%s/api/health\n", addr)
	fmt.Printf("  只监听 loopback（N5b）；Ctrl-C 退出\n")
}
