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
	dbPath            *string
	addr              *string
	model             *string
	baseURL           *string
	apiKeyEnv         *string
	approval          *string
	allow             *string
	ask               *string
	maxTurns          *int
	contextBudget     *int64
	system            *string
	indexerBatch      *int
	maxSessionRows    *int
	cwd               *string
	runID             *string
	onlineRows        *int
	onlineLineTokens  *int
	onlineLevel       *string
	onlineWindow      *int
	configPath        *string
	monitorCPU        *float64
	monitorMem        *float64
	monitorGoroutines *int
	monitorInterval   *int
	contextStrategy   *string
	contextWindow     *int64
	contextL1Pct      *float64
	contextL2Pct      *float64
	graph             *bool
	graphMaxNodes     *int
	graphMaxEdges     *int
	graphMaxDepth     *int
	graphMaxResults   *int
	graphCore         *string
	graphEscalate     *int

	// ---- v3-S4 协议面（G1/G3）----
	// promptTimeout 是协议面发起的一轮对话的**整轮**上限（秒；0 = 框架默认 600）。
	promptTimeout *int

	// ---- v3-S3 工作流引擎（W1 能力包，非强制）----
	workflowDir *string
	goalLedger  *bool

	// ---- v2-S4 沙箱（K8/H9）----
	sandbox              *bool
	sandboxWriteDirs     *string
	sandboxMaxActive     *int
	sandboxACLMaxObjects *int
	sandboxStrictGroups  *bool
	// set 记录被显式设置的 flag 名（fs.Visit 填充）：flag 默认值不应掩盖配置文件，
	// 只有**显式**设置的 flag 才高于文件（H15：env>文件；显式 flag 更高）。
	set map[string]bool
}

// registerHubFlags 把共有 flag 注册到 fs（serve 额外有 -addr，chat 也有但可忽略）。
func registerHubFlags(fs *flag.FlagSet) *hubFlags {
	return &hubFlags{
		dbPath:            fs.String("db", "alasol.db", "事件库路径（SQLite）"),
		addr:              fs.String("addr", "127.0.0.1:8720", "监听地址（只接受 loopback，N5b）"),
		model:             fs.String("model", "", "模型名（原样传给 provider；缺省回落 env ALASOL_MODEL，无默认值——`alasol models` 可发现）"),
		baseURL:           fs.String("base-url", "", "OpenAI 兼容端点（空=官方）"),
		apiKeyEnv:         fs.String("api-key-env", "OPENAI_API_KEY", "持有 API key 的环境变量名"),
		approval:          fs.String("approval", "deny-all", "审批策略：deny-all | always-allow | terminal | protocol（protocol=经 WS/HTTP 面应答审批，无人应答=deny）"),
		allow:             fs.String("allow", "", "允许的工具名（逗号分隔；空=一切 deny）"),
		ask:               fs.String("ask", "", "需要人审的工具名（逗号分隔；命中即去问 Approver）"),
		maxTurns:          fs.Int("max-turns", 0, "最大轮数（0=默认 20）"),
		contextBudget:     fs.Int64("context-budget", 0, "上下文窗口预算 token（0=默认；只用于占比）"),
		system:            fs.String("system", "", "系统提示"),
		indexerBatch:      fs.Int("indexer-batch", 0, "索引器单批处理上限（0=默认）"),
		maxSessionRows:    fs.Int("max-session-rows", 0, "/api/sessions 行数上限（0=默认 200，K15）"),
		cwd:               fs.String("cwd", "", "工作目录（空=进程 cwd）：自动 gate repo:<repo_id> 按它算（K12/S14）"),
		runID:             fs.String("run-id", "", "工作流运行 id（空=不加 workflow gate；工作流引擎在 v3）"),
		onlineRows:        fs.Int("online-rows", 0, "在线表注入行数 N（0=默认 50，K15）"),
		onlineLineTokens:  fs.Int("online-line-tokens", 0, "在线表每行 summary 的 token 预算（0=默认 100，K15）"),
		onlineLevel:       fs.String("online-level", "", "在线表暴露等级 summary|n|full（空=默认 summary，K14）"),
		onlineWindow:      fs.Int("online-window", 0, "在线表 level=n 每行窗口行数（0=默认 5，K14）"),
		configPath:        fs.String("config", "", "用户级配置文件路径（TOML；空=平台默认位，H15：%APPDATA%\\alasol\\config.toml 或 XDG）"),
		monitorCPU:        fs.Float64("monitor-cpu", 0, "hub CPU 监控阈值%（单核等价，可>100；0=默认，K9）"),
		monitorMem:        fs.Float64("monitor-mem", 0, "hub 内存监控阈值 MB（0=默认，K9）"),
		monitorGoroutines: fs.Int("monitor-goroutines", 0, "hub goroutine 数监控阈值（0=默认，K9）"),
		monitorInterval:   fs.Int("monitor-interval", 0, "hub 监控采样周期秒（0=默认，K9）"),
		contextStrategy:   fs.String("context-strategy", "", "上下文装配策略（v2-S1）：空=append（默认，v1 等价）| tiered=级联压缩（两级 L1/L2 + recall + 留存降级）"),
		contextWindow:     fs.Int64("context-window", 0, "tiered 策略的上下文窗口预算 W（token；0=默认 1M；须 ≥256k）"),
		contextL1Pct:      fs.Float64("context-l1-pct", 0, "tiered 策略的 L1（浅摘要）目标残留占窗口百分比（0=默认 10）"),
		contextL2Pct:      fs.Float64("context-l2-pct", 0, "tiered 策略的 L2（深压缩）目标残留占窗口百分比（0=默认 1）"),
		graph:             fs.Bool("graph", false, "启用图投影（v2-S2，G2f 可选投影；默认关=零图开销：不建图表/不建引擎）"),
		graphMaxNodes:     fs.Int("graph-max-nodes", 0, "内存图节点上界（0=默认 10 万，G19；触界即停并报错）"),
		graphMaxEdges:     fs.Int("graph-max-edges", 0, "内存图边上界（0=默认 100 万，G19）"),
		graphMaxDepth:     fs.Int("graph-max-depth", 0, "多跳遍历深度上限（0=默认 8，G18）"),
		graphMaxResults:   fs.Int("graph-max-results", 0, "多跳遍历结果上限（0=默认 5000，G18）"),
		graphCore:         fs.String("graph-core", "", "核心接口节点声明（逗号分隔；`*` 结尾=字节前缀，如 core/*,contracts/*；W16）"),
		graphEscalate:     fs.Int("graph-escalate-reachable", 0, "change_impact 的 escalate 阈值：可达节点数超过它才触发（0=默认 50，W15）"),

		// 协议面的一轮对话上限（v3-S4 G1）：**整轮**（一个 prompt 的全部 turn）共用这份预算，
		// 不是单次模型调用的超时。它同时是 fail-close 的兜底时钟——审批等不到应答就按 deny 收场。
		promptTimeout: fs.Int("prompt-timeout", 0,
			"协议面（POST /api/sessions/{id}/prompt、WS command.submit）一轮对话的**整轮**上限（秒；0=默认 600）。"+
				"同时是 fail-close 的兜底时钟：审批等不到应答就按 deny 收场（无人应答=deny）"),

		// 工作流引擎（v3-S3，W1 能力包）：目录加载纯数据能力包 + goal ledger 工具开关。
		workflowDir: fs.String("workflow-dir", "",
			"工作流能力包目录（v3-S3，W13：纯数据 JSON；空=只用内建 tower）。激活只把哲学注入 system prompt（W3），非强制"),
		goalLedger: fs.Bool("goal-ledger", false,
			"给 session 注入 goal ledger 更新工具（J19/X6；默认关=零行为变化。仍受策略链 deny-all 约束）"),

		// 沙箱（v2-S4，K8/H9）：默认关（与 v1 等价）。开了之后 bash 写允许目录之外
		// 会被 **OS 拒**（Windows: job object + 受限 token + 目录 ACL）。
		//
		// 诚实边界（写进帮助里，别让用户以为"开了沙箱 = 系统更干净"）：
		//   - 沙箱会**改写允许根的 DACL**（追加可继承 ACE）且**不还原**；
		//   - 默认模式下装在 Program Files 的工具（go/git/python）**可正常运行**
		//     （它们读自己的资源靠 BUILTIN\Users，而默认模式保留该组）；
		//     只有 -sandbox-strict-groups 才会让它们读不到自己的资源。
		sandbox: fs.Bool("sandbox", false,
			"启用 SandboxedExecutionEnv（v2-S4，K8/H9；默认关=走 local，与 v1 等价）。"+
				"注意：会改写允许根的 DACL 且不还原"),
		sandboxWriteDirs: fs.String("sandbox-write-dirs", "",
			"沙箱允许写根（逗号分隔）。口径：显式列出的根 **+ CWD** 一起进允许集（沙箱自管的临时根也在内）。"+
				"根之外的写被内核拒绝（用户私有文件也读不到；系统位置默认仍可读）"),
		sandboxMaxActive:     fs.Int("sandbox-max-active", 0, "沙箱 job object 活动进程上限（0=默认 64；fork 炸弹有界化）"),
		sandboxACLMaxObjects: fs.Int("sandbox-acl-max-objects", 0, "沙箱施加 ACL 前数子树对象的上限（0=默认 20 万；超限即拒，防误配成大根）"),
		sandboxStrictGroups: fs.Bool("sandbox-strict-groups", false,
			"沙箱严格组 SID 模式：把 BUILTIN 的 Users SID 也 deny-only（默认关）。"+
				"开了能堵上 C:\\ProgramData 这类只授予 Users 写权限的目录；"+
				"代价是用 overlapped I/O 写文件的程序（Go/.NET/Rust/Node）会写不进去"),
	}
}

// parse 解析 args 并记录被显式设置的 flag（供 buildConfig 做 flag>env>文件 优先级）。
func (f *hubFlags) parse(fs *flag.FlagSet, args []string) {
	_ = fs.Parse(args)
	f.set = map[string]bool{}
	fs.Visit(func(fl *flag.Flag) { f.set[fl.Name] = true })
}

// explicitlySet 报告某 flag 是否被用户在命令行显式设置（区别于"用了默认值"）。
func (f *hubFlags) explicitlySet(name string) bool {
	return f.set != nil && f.set[name]
}

// config 把 flag 值 + 用户级 TOML 配置（H15）合并成 host.Config（唯一装配处）。
// 优先级：显式 flag > 进程 env > 配置文件（见 buildConfig）。
func (f *hubFlags) config() (host.Config, error) {
	return f.buildConfig()
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
	f.parse(fs, args)

	cfg, err := f.config()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	h, err := host.New(cfg)
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
	printBanner(ln.Addr().String(), bannerOptions{
		GraphEnabled: h.GraphEnabled(),
		MCPEnabled:   h.MCPEnabled(),
		MCPTools:     len(h.MCPToolNames()),
	})

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

// bannerOptions 是 printBanner 的输入（图/MCP 两个可选能力的启用状态）。
//
// 用结构体而不是一长串位置参数：可选能力会继续增加（v3 的工作流引擎、tower），
// 位置参数到第四个就开始出错且看不出谁是谁。
type bannerOptions struct {
	GraphEnabled bool
	MCPEnabled   bool
	// MCPTools 是已注册的 MCP 工具数（banner 只打**数量**——server 名/命令/env
	// 可能含凭据，不进 stdout，H12/H19 同理）。
	MCPTools int
}

// printBanner 打印就绪信息（地址从**真实监听器**取，端口是 0 时也能显示实际值）。
func printBanner(addr string, opts bannerOptions) {
	fmt.Printf("[alasol] hub 已就绪\n")
	fmt.Printf("  Web UI   http://%s/\n", addr)
	fmt.Printf("  事件流   ws://%s/ws（NDJSON）\n", addr)
	fmt.Printf("  在线表   http://%s/api/sessions\n", addr)
	fmt.Printf("  上下文   http://%s/api/context/{session_id}\n", addr)
	fmt.Printf("  可见性   http://%s/api/peers?session_id=...（K11 水平可见性）\n", addr)
	fmt.Printf("  加 gate  curl -X POST http://%s/api/gates/join -d '{\"session_id\":\"...\",\"gate_id\":\"...\"}'\n", addr)
	fmt.Printf("  在线表配置 http://%s/api/online-table/config（GET 读 / POST 运行期调，K15 不重启）\n", addr)
	if opts.GraphEnabled {
		// 图投影（v2-S2，G2f 可选投影）：启用了才提示——没启用时打这行会让人以为有数据。
		fmt.Printf("  影响面   http://%s/api/graph/impact?roots=a,b（change_impact，G21/W16）\n", addr)
	} else {
		fmt.Printf("  图投影   未启用（-graph 开启后可用 /api/graph/impact，G2f 可选投影）\n")
	}
	if opts.MCPEnabled {
		// MCP client（v3-S1，N1）：只打**工具数**——server 名/命令/env 可能含凭据，
		// 不进 stdout（H12/H19 同款纪律）。deny-all 默认这件事必须写在 banner 里，
		// 否则使用者会以为"连上了就能调"。
		fmt.Printf("  MCP      已装配 %d 个外部工具（deny-all 默认：需 -allow 放行才可调用）\n", opts.MCPTools)
	} else {
		fmt.Printf("  MCP      未启用（配置文件写 [[mcp.servers]] 接入外部 MCP server，v3-S1）\n")
	}
	fmt.Printf("  健康     http://%s/api/health\n", addr)
	fmt.Printf("  只监听 loopback（N5b）；Ctrl-C 退出\n")
}
