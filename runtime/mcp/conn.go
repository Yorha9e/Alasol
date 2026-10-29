package mcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Yorha9e/alasol/core"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// 本文件负责**连接**一个 MCP server 并管它的生命周期。
//
// 分工：
//   - connect 按传输类型造 transport（stdio = 亲自 exec + IOTransport；
//     http = StreamableClientTransport），完成 MCP initialize 握手；
//   - serverConn 持有 session / 子进程句柄 / N2 trace，Close 时收尸；
//   - 握手阶段同时取一次 tools/list（发现），失败即连接失败（fail-close：
//     一个"连上了但列不出工具"的 server 与连不上没有区别，都不该被当成可用）。

// clientImpl 是本 client 在 initialize 里自报的身份（MCP Implementation）。
var clientImpl = &mcp.Implementation{
	Name:    "alasol",
	Version: "v3-S1",
}

// discardLogger 是一个丢掉所有输出的 logger（SDK 的 ClientOptions.Logger 非 nil
// 时它会把 jsonrpc 层错误经 logger 吐出；nil 时 SDK 退回 slog.Default()，
// 可能把 server 端点/请求细节写进宿主的 slog 输出——凭据纪律不允许）。
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// connectTimeout 是连接 + 发现的默认超时（ServerConfig.TimeoutMs 覆盖）。
//
// **必须带 time.Millisecond**：DefaultTimeoutMs 是毫秒数（int），直接赋给
// time.Duration 会变成 15000**纳秒**（15µs）→ 每次连接都秒超时。
// 这个坑本刀真踩过（测试全部 context deadline exceeded），故钉在此处。
const connectTimeout = DefaultTimeoutMs * time.Millisecond

// serverInfo 是握手后从 server 侧读到的基本信息（诊断用；不含凭据）。
type serverInfo struct {
	// Name / Version 是 server 自报的身份（InitializeResult.ServerInfo）。
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
	// ProtocolVersion 是协商出的协议版本。
	ProtocolVersion string `json:"protocol_version,omitempty"`
	// Instructions 是 server 给的使用说明（可原样进 system prompt 的候选；
	// v3-S1 只取不回填，留面）。
	Instructions string `json:"instructions,omitempty"`
	// Transport 是实际用的传输（"stdio" / "http"）。
	Transport string `json:"transport"`
}

// serverConn 是一条到外部 MCP server 的连接。
type serverConn struct {
	cfg  ServerConfig
	info serverInfo
	tr   *n2Trace
	sess *mcp.ClientSession
	cmd  *exec.Cmd // 仅 stdio：本包拉起的子进程（http 传输为 nil）
	// discovered 是 connectSession 那次 tools/list 的结果（**复用**，见 Tools）。
	//
	// 为什么缓存：装配期**只发一次** tools/list。坏 server 常在**第二次**
	// tools/list 上挂死（第一次给个像样的列表，第二次就不动了）——那次请求若
	// 没有 deadline，hub 启动会永久挂起（p1）。
	discovered []*mcp.Tool
	close      sync.Once
}

// connect 连接一个 server 并完成发现。
func connect(ctx context.Context, cfg ServerConfig) (*serverConn, error) {
	var (
		transport mcp.Transport
		cmd       *exec.Cmd
	)
	switch cfg.Transport {
	case TransportStdio:
		tr, c, err := stdioTransport(&cfg)
		if err != nil {
			return nil, err
		}
		transport, cmd = tr, c
	case TransportHTTP:
		transport = httpTransport(&cfg)
	default:
		return nil, fmt.Errorf("mcp: server %q: 未支持的传输 %q", cfg.Name, cfg.Transport)
	}
	return connectSession(ctx, cfg, transport, cmd)
}

// newMCPClient 造 MCP client：N2 能力不广告 + 拒绝中间件留痕 + 静默 logger。
//
// 从 connectSession 拆出来是为了测试能复用它（配一个旧协议版本的
// ClientSessionOptions 去验 wire 层的 N2 拒绝）——产物逻辑仍然只有一份。
func newMCPClient(trace *n2Trace) *mcp.Client {
	client := mcp.NewClient(clientImpl, &mcp.ClientOptions{
		// N2：**显式给空 capabilities**——不给 roots（默认会给 roots+listChanged），
		// 也不给 sampling / elicitation（那两个靠"不装 handler"来不广告）。
		Capabilities: &mcp.ClientCapabilities{},
		// Logger：SDK 内部会把 jsonrpc 层的错误经 logger 吐出。接一个 discard logger
		// 而不是 nil——nil 时 SDK 用默认 logger，可能把 server 地址/请求细节写进
		// 宿主的 slog 输出（凭据纪律：不往那儿写）。
		Logger: discardLogger(),
	})
	client.AddReceivingMiddleware(n2Middleware(trace))
	return client
}

// connectSession 在给定 transport 上完成握手 + 发现，并管好已拉起的子进程。
//
// 从 connect 里拆出来是为了让测试能注入 in-memory transport（SDK 的
// NewInMemoryTransports）——产物代码只走 connect，没有测试专用分支
// （不为测试改产物行为，只拆一个可复用的缝）。
//
// cmd 非 nil（stdio）时，**任何一条失败路径都要收尸**：握手失败 / tools/list
// 失败——子进程已经起来了，没人 Wait 就是孤儿 + zombie。
func connectSession(ctx context.Context, cfg ServerConfig, transport mcp.Transport, cmd *exec.Cmd) (*serverConn, error) {
	cctx, cancel := context.WithTimeout(ctx, serverTimeout(cfg))
	defer cancel()

	trace := newN2Trace()
	client := newMCPClient(trace)

	sess, err := client.Connect(cctx, transport, nil)
	if err != nil {
		// 连不上（命令不存在 / 握手失败 / 超时）→ 已经拉起的子进程必须收尸，
		// 否则每次失败的装配都漏一个进程（"谁拉起谁收尸"对失败路径同样成立）。
		if cmd != nil {
			reapProcess(cmd)
		}
		return nil, fmt.Errorf("mcp: server %q 连接失败（transport=%s）: %w",
			cfg.Name, cfg.Transport, scrubEndpoint(cfg, err))
	}

	init := sess.InitializeResult()
	info := serverInfo{Transport: cfg.Transport}
	if init != nil {
		if init.ServerInfo != nil {
			info.Name = init.ServerInfo.Name
			info.Version = init.ServerInfo.Version
		}
		info.ProtocolVersion = init.ProtocolVersion
		info.Instructions = init.Instructions
	}

	// 发现：握手下立刻列一次工具。列不出来 = 这个 server 不可用（fail-close）。
	//
	// **这次列表被缓存下来复用**（serverConn.discovered）：装配期不再发第二
	// tools/list——第二次请求既浪费又是新的挂死面（p1）。
	tools, err := listTools(cctx, sess)
	if err != nil {
		_ = sess.Close()
		if cmd != nil {
			reapProcess(cmd)
		}
		return nil, fmt.Errorf("mcp: server %q tools/list 失败: %w", cfg.Name, scrubEndpoint(cfg, err))
	}

	return &serverConn{cfg: cfg, info: info, tr: trace, sess: sess, cmd: cmd, discovered: tools}, nil
}

// serverTimeout 是单个 server 的**装配期总预算**（连接 + 发现）。
//
// 口径：ServerConfig.TimeoutMs 覆盖（0 = 默认 connectTimeout）。
//
// **必须有 deadline**：产品侧传进来的 ctx 是 context.Background()（host.New /
// `alasol run` / `alasol resume` 都那样传），没有这层预算，一个挂死的 server
// 会让 hub 启动**永久挂起**——"坏 server 不拖垮 hub"在时间维度上同样成立（p1）。
func serverTimeout(sc ServerConfig) time.Duration {
	if sc.TimeoutMs > 0 {
		return time.Duration(sc.TimeoutMs) * time.Millisecond
	}
	return connectTimeout
}

// listTools 列一次工具（分页由 SDK 的迭代器处理：Tools 会自动翻页）。
//
// 错误按原样上抛（调用方负责加 server 名与脱敏）。
func listTools(ctx context.Context, sess *mcp.ClientSession) ([]*mcp.Tool, error) {
	out := make([]*mcp.Tool, 0, 8)
	for t, err := range sess.Tools(ctx, &mcp.ListToolsParams{}) {
		if err != nil {
			return out, err
		}
		if t == nil {
			continue
		}
		out = append(out, t)
	}
	return out, nil
}

// scrubbedEndpointError 包住底层 error，但把**端点 URL** 从文案里抹掉
// （H12/H19 同款纪律）。
//
// 为什么需要：SDK 的 HTTP 传输会把完整 URL 写进错误（`Post "http://host/mcp":
// dial tcp ...`），而这个错误会进 hub 日志。URL 本身不是秘密，但它**可能**带
// 查询串凭据（`?token=...`）——我们自己的设计把凭据放在 Headers 里，可使用者
// 配的 URL 不归我们管。抹掉端点不损失诊断价值（server 名还在）。
//
// 保留 Unwrap：errors.Is/As 仍能穿透到底层（调用方可能想判定是不是网络错误）。
type scrubbedEndpointError struct {
	err error
	url string
}

func (e scrubbedEndpointError) Error() string {
	if e.url == "" {
		return e.err.Error()
	}
	return strings.ReplaceAll(e.err.Error(), e.url, "<endpoint>")
}

func (e scrubbedEndpointError) Unwrap() error { return e.err }

// scrubEndpoint 包住 err（url 为空时原样返回）。
func scrubEndpoint(cfg ServerConfig, err error) error {
	if err == nil || strings.TrimSpace(cfg.URL) == "" {
		return err
	}
	return scrubbedEndpointError{err: err, url: cfg.URL}
}

// Tools 列出 server 当前声明的工具（分页由 SDK 处理，iter.Seq2 展开）。
//
// **复用 connectSession 那次 tools/list 的结果**（装配期只发一次 tools/list）：
// 坏 server 常在第二次 tools/list 上挂死——那次请求若没有 deadline，hub 启动
// 会永久挂起（p1）。只有在连接不是经 connectSession 建的（测试替身）时才会
// 真发一次，那次也有装配期 deadline 罩着（见 manager.attachDialed）。
func (c *serverConn) Tools(ctx context.Context) ([]*mcp.Tool, error) {
	if c == nil || c.sess == nil {
		return nil, errors.New("mcp: 连接为 nil（server 未连接）")
	}
	if c.discovered != nil {
		return c.discovered, nil
	}
	out, err := listTools(ctx, c.sess)
	if err != nil {
		return out, fmt.Errorf("mcp: server %q tools/list: %w", c.cfg.Name, err)
	}
	return out, nil
}

// CallTool 调一次 MCP tools/call。
//
// MCP 的 request id 由 SDK 自己管（jsonrpc2 层），**不进** contracts.Event.ID
// （陷阱 1：provider 的 tool_call_id 才是事件侧的黑盒 id，二者永不共字段）。
func (c *serverConn) CallTool(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error) {
	if c == nil || c.sess == nil {
		return nil, errors.New("mcp: 连接为 nil（server 未连接）")
	}
	return c.sess.CallTool(ctx, params)
}

// Close 关闭连接并收尸（幂等，**有界**）。
//
// # 为什么必须有界（p2）
//
// SDK 的 ClientSession.Close 会**等在飞请求结束**（jsonrpc2 的 wait：连接关闭
// 前先把在飞请求等完）。一个挂死的 server（tools/call 不回）会让 Close 永久
// 阻塞 → hub 关停被拖死——"坏 server 不拖垮 hub"在**关停**维度同样成立。
//
// # 顺序（stdio 传输）
//
//  1. 后台启动 sess.Close()：没有在飞请求时它立刻把管道关上 → server 看到
//     stdin EOF 自行退出（守规矩的 server 的优雅路径）；
//  2. 有界地等它收尾（gracePeriod）：超时说明**有在飞请求卡住了** →
//     **杀进程树**（Windows: taskkill /T /F /PID，陷阱 5；Unix: Kill 直接子进程）
//     ——只杀直接子进程的话，孙子进程会继承管道写端把 Wait 挂住
//     （runtime/cap/local 的 waitDelay 注释里有本机实测：29 秒）；
//     杀树后管道断开 → 在飞请求立刻失败 → 第 1 步的 Close 才能收尾；
//  3. 杀树后仍不收尾 → 带 deadline（killGrace）放弃等待：**按"已强杀"处理**
//     （只告警，不把 Close 变成失败——hub 关停路径上的错误会把真正重要的
//     错误淹没；这与 reapProcess 的口径一致）；
//  4. cmd.Wait() 收尸（zombie 不许留）。
//
// **Wait 必须在管道关闭之后调**：exec 的 Wait 会关掉 stdout 管道，与 SDK 的
// 读并发会把响应撕裂（所以第 4 步排在第 1~3 步之后）。
//
// http 传输没有子进程：只有第 1/3 步（deadline 兜底）。
func (c *serverConn) Close() error {
	if c == nil {
		return nil
	}
	var err error
	c.close.Do(func() {
		err = c.closeInner()
	})
	return err
}

// closeInner 是 Close 的真身（外层只负责幂等）。
func (c *serverConn) closeInner() error {
	closeDone := make(chan error, 1)
	if c.sess != nil {
		go func() { closeDone <- c.sess.Close() }()
	} else {
		closeDone <- nil
	}

	// ① 先给在飞请求一个收尾窗口。
	var sessErr error
	settled := false
	select {
	case sessErr = <-closeDone:
		settled = true
	case <-time.After(gracePeriod):
		// ② 窗口用尽：在飞请求卡住了 → 杀进程树让 server 立刻死（stdio）。
		if c.cmd != nil {
			killTree(c.cmd)
		}
		select {
		case sessErr = <-closeDone:
			settled = true
		case <-time.After(killGrace):
			// ③ 强杀后仍不收尾：按"已强杀"处理（告警 + 放弃等待，不阻塞关停）。
			mcpLog.Warn("mcp: 关 session 超时（stdio 已强杀进程树；按已收尸处理）",
				"server", c.cfg.Name, "transport", c.cfg.Transport, "timeout", killGrace)
		}
	}

	// ④ 收尸子进程（Wait 只能调一次；且必须在管道关闭之后——见 Close 的注释）。
	if c.cmd != nil {
		reapProcess(c.cmd)
	}
	if !settled {
		return nil
	}
	return sessErr
}

// gracePeriod 是**关停第一段**的等待窗口：等 session 收尾（没有在飞请求时
// 它立刻把管道关上 → server 见 stdin EOF 自行退出）。
//
// 2s 的取舍：足够一个守规矩的 MCP server 走完退出流程，又不至于让
// `alasol serve` 的 Ctrl-C 收尾明显卡顿。超时就走杀树（下一步仍有界）。
const gracePeriod = 2 * time.Second

// killGrace 是**关停第二段**的等待窗口：杀树后等 session 收尾 / 等 Wait 返回
// （cmd.WaitDelay 之外的第二道界）。
const killGrace = 5 * time.Second

// stdioTransport 亲自拉起 server 子进程并造 IOTransport。
//
// **不用 SDK 的 mcp.CommandTransport**：它的 Close 在 Windows 上只对直接子进程
// SIGTERM/Kill，孙子进程继承管道写端会把 Wait 挂住；而且它自己 Start/Wait，
// 我们拿不到"谁拉起谁收尸"的完整控制。自己 exec + IOTransport（只拿管道）
// 让进程生命周期完全在本包手里。
func stdioTransport(cfg *ServerConfig) (mcp.Transport, *exec.Cmd, error) {
	cmd := exec.Command(cfg.Command, cfg.Args...) // 不走 shell（陷阱 7 同款纪律）
	cmd.Dir = cfg.Dir                             // 空 = 继承 hub 工作目录
	cmd.Env = childEnv(cfg)                       // 净化父 env + 显式注入（H16/H18/H21）
	hideWindow(cmd)                               // Windows：不弹控制台窗口（陷阱 5）
	cmd.WaitDelay = killGrace                     // 管道关不上时的硬上限（同 local.Exec）
	// stderr **不丢**：接进 mcpLog（脱敏后）——外部 server 的启动失败原因、
	// 崩溃栈只在 stderr 上，丢掉它等于"server 挂了但看不出为什么"。
	cmd.Stderr = newStderrSink(cfg)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("mcp: server %q: 取 stdout 管道失败: %w", cfg.Name, err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		_ = stdout.Close()
		return nil, nil, fmt.Errorf("mcp: server %q: 取 stdin 管道失败: %w", cfg.Name, err)
	}
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stdin.Close()
		return nil, nil, fmt.Errorf("mcp: server %q: 启动 %q 失败: %w", cfg.Name, cfg.Command, err)
	}
	// IOTransport 只拿管道：Close 时 rwc.Close 会同时关掉我们这侧的读端与写端，
	// 于是 server 的 stdin 看到 EOF（优雅退出的信号）。
	return &mcp.IOTransport{Reader: stdout, Writer: stdin}, cmd, nil
}

// httpTransport 造 Streamable HTTP 传输（MCP 现行标准；服务端→客户端走同连接的 SSE）。
func httpTransport(cfg *ServerConfig) mcp.Transport {
	header := make(http.Header, len(cfg.Headers))
	for k, v := range cfg.Headers {
		header.Set(k, v)
	}
	return &mcp.StreamableClientTransport{
		Endpoint: cfg.URL,
		// HTTPClient：显式造一个而不是用 http.DefaultClient——DefaultClient 是进程级
		// 共享的，给它设超时会影响所有使用者；而且我们要能挂 Header（凭据）。
		HTTPClient: &http.Client{
			Timeout: 0, // 不设总超时：长连接/SSE 流是设计形态，取消走 ctx
			Transport: &headerTransport{
				base:    http.DefaultTransport,
				headers: header,
			},
		},
	}
}

// headerTransport 给每个出站请求附带配置的 Header（Authorization 等凭据头）。
//
// 凭据纪律：Header 值只在内存里流转，**不进事件 / 日志 / 错误文案**——
// 因此这里绝不把 Header 包进任何 error。
type headerTransport struct {
	base    http.RoundTripper
	headers http.Header
}

// RoundTrip implements http.RoundTripper。
func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	clone := req.Clone(req.Context())
	for k, vals := range t.headers {
		for _, v := range vals {
			clone.Header.Add(k, v)
		}
	}
	return base.RoundTrip(clone)
}

// childEnv 组装 server 子进程的 env（与 runtime/tools.childEnv 同口径）：
//
//	基底 = 净化后的父 env（ScrubEnv 剥掉凭据类键与凭据形态的值）
//	+   EnvFrom 列出的父 env 变量（显式点名 = 使用者有意给，类似 required_env 注入）
//	+   Env 直填的键值（显式给这个 server 的，**不过 Scrubbed**——那是它的凭据入口）
//
// 排序输出：确定性（测试可复现）+ Windows 上避免同名键大小写歧义。
func childEnv(cfg *ServerConfig) []string {
	parent := map[string]string{}
	for _, kv := range os.Environ() {
		if k, v, ok := strings.Cut(kv, "="); ok {
			parent[k] = v
		}
	}
	// 必须用返回值当新基底：把结果拷回原 map 的话，被剥掉的凭据键还留在里面。
	base := map[string]string(core.ScrubEnv(core.EnvScope(parent)))
	for _, name := range cfg.EnvFrom {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if v, ok := parent[name]; ok {
			base[name] = v // 显式点名继承，不剥
		}
	}
	for k, v := range cfg.Env {
		base[k] = v // 显式直填，不剥
	}
	out := make([]string, 0, len(base))
	for k, v := range base {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// reapProcess 收尸一个已拉起的子进程：先优雅等，再杀树，最后 Wait。
//
// 幂等且永不返回 error（收尸失败只能告警，不能把 Close 变成失败——
// hub 关停路径上的错误会把真正重要的错误淹没）。
func reapProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait() // 只由本包调用一次（管道已在 sess.Close 里关上）
		close(done)
	}()
	select {
	case <-done:
		return // 优雅退出
	case <-time.After(gracePeriod):
	}
	killTree(cmd) // Windows: taskkill /T /F /PID（陷阱 5）
	select {
	case <-done:
	case <-time.After(killGrace):
		// 杀树后还不退：cmd.WaitDelay 会给 Wait 兜底，这里不再无限等。
	}
}
