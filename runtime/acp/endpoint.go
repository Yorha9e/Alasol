package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/loop"
)

// ACP 协议常量（doc 口径：协议版本号跟 ACP 规范走，本实现按 v1 语义）。
const (
	// protocolVersion 是本实现声明支持的 ACP 协议版本（v1）。
	protocolVersion = 1
	// agentName / agentVersion 是 agentInfo 的取值（能力协商回给客户端）。
	agentName    = "alasol"
	agentVersion = "4.2.0" // 与 v4-2（K19 ACP 端点）对齐的一个常量标识
)

// ACP 的 stopReason 枚举（**产出值只能取这些**——枚举外取值会让现成客户端解不出合法
// stopReason；F4）。本实现用到其中三个：end_turn / max_turn_requests / cancelled。
const (
	stopReasonEndTurn         = "end_turn"
	stopReasonMaxTokens       = "max_tokens"
	stopReasonMaxTurnRequests = "max_turn_requests"
	stopReasonRefusal         = "refusal"
	stopReasonCancelled       = "cancelled"
)

// validStopReasons 是 ACP stopReason 的**全集**（枚举成员清单）。
//
// 用途：回归钉子断言"本实现产出的每个 stopReason 都 ∈ 枚举"（F4）——新增取值时
// 这里必须同步，否则钉子会红（提醒：新取值要么在枚举内，要么换映射）。
var validStopReasons = map[string]bool{
	stopReasonEndTurn:         true,
	stopReasonMaxTokens:       true,
	stopReasonMaxTurnRequests: true,
	stopReasonRefusal:         true,
	stopReasonCancelled:       true,
}

// DefaultTurnTimeout 是单轮 session/prompt 的整体预算缺省值（Options.TurnTimeout = 0 时用它）。
//
// 与 host 的 promptRunTimeout **同一个常量**（core.DefaultPromptRunTimeout）：它同时是
// 审批 fail-close 的兜底时钟——一轮里可以有多次审批等待，共用这份预算；预算耗尽 →
// 后续审批立即 deny、整轮以 max_turn_requests 收场（见 mapStopReason）。
//
// 同一个"-prompt-timeout"在 HTTP 面与 ACP 面上必须给出同一份预算（F8）。
const DefaultTurnTimeout = core.DefaultPromptRunTimeout

// defaultTurnTimeout 是包内别名（内部引用沿用这个名字）。
const defaultTurnTimeout = DefaultTurnTimeout

// turnDrainGrace 是 Serve 退出时等在跑 turn 收尾的宽限（短：退出路径不该被一轮
// 未完成的 turn 拖住；它们已被 cancel，写不出去的帧记日志即丢）。
const turnDrainGrace = 2 * time.Second

// Backend 是 ACP 端点驱动的 agent 后端抽象。*host.Hub 满足它（经 cmd/alasol/acp.go
// 的适配层注入——runtime 不 import host，见 09 依赖方向）。
//
// 三个方法对应 ACP 的三个核心动作：
//
//	CreateSession     —— session/new（按 cwd 建 hub session，回 hub ULID 原样作 sessionId）
//	RunPrompt         —— session/prompt（同步跑完一整轮，带回最终文本与轮数）
//	SubscribeEvents   —— 订阅该 session 的增量事件（含 durable=false 的 model.delta）供
//	                      桥接成 session/update；返回 nil = 订阅不可用 → 端点降级为
//	                      "整轮结束发一整段 agent_message_chunk"（doc 降级口径）
//
// session/load 走**可选能力** sessionLoader（type assertion 探测，见其类型 doc——Backend
// 接口不变，没实现它的后端收到 session/load 回 -32000 能力缺失）。
//
// **能力声明按后端实际能力动态给**（initialize 的 agentCapabilities.loadSession =
// 后端是否实现 sessionLoader）——宣称 true 却回 -32000 = 骗客户端（客户端会真的去
// session/load，然后拿到一个能力缺失的错）。
type Backend interface {
	CreateSession(cwd string) (contracts.Ulid, error)
	RunPrompt(ctx context.Context, sessionID contracts.Ulid, prompt string) (loop.Result, error)
	SubscribeEvents(sessionID contracts.Ulid) *bus.Subscription
}

// turnOutcomeAware 是后端的**可选能力**：声明"本后端返回的 loop.Result 带**权威**的
// 截断标记（Result.TruncatedByMaxTurns）"。
//
// 为什么要显式声明（N3）：Result.Turns 只说"跑了几轮"，**分不清**"正常收敛在最后一轮"与
// "被 MaxTurns 截断"——两者 Turns 都等于上限（MaxTurns=1 单轮收敛 vs MaxTurns=1 被截断，
// Turns 都是 1）。loop 现在用 TruncatedByMaxTurns **显式**标出截断，但后端不一定透传它
// （老/第三方/假后端只报 Turns）——端点据此决定要不要**回落**旧口径（"轮数 ≥ 上限即视为
// 截断"）：能给出权威标记的后端不回落（否则"恰好用满上限但正常收敛"会被误报成
// max_turn_requests）。
//
// **可选接口**（靠 type assertion 探测）：Backend 接口**不变**。
type turnOutcomeAware interface {
	// TurnOutcomeAuthoritative 恒返回 true——**存在即表示**"本后端的结果带权威截断标记"。
	TurnOutcomeAuthoritative() bool
}

// turnCapAware 是后端的**可选能力**：报单轮轮数上限（MaxTurns）。
//
// 用途（F4）："轮数用尽"要映射成 max_turn_requests 而不是 end_turn。对**没有**权威截断
// 标记的后端（老/第三方/假后端），只能靠"实际轮数 ≥ 上限"**猜**（旧口径，见
// turnsExhausted 的诚实边界）。
//
// **可选接口**（靠 type assertion 探测）：Backend 接口**不变**——没实现它的后端（第三方 /
// 假后端）判不出"轮数用尽"，落 end_turn（既有行为，不算错）。
type turnCapAware interface {
	// TurnCap 返回单轮的轮数上限（≤ 0 = 未知）。
	TurnCap() int
}

// ErrSessionNotFound 是 session/load 面的哨兵错误：**会话不存在**（该 sessionId 没有
// 持久化历史，载不进来）。后端（或其适配层）在"查无此会话"时返回它，端点据此回
// **协议错误帧**（-32602，与 session/prompt 的"未知 session"同一错误形状）——其它错误
// 一律按后端内部错误走 -32603（消息带脱敏摘要）。
var ErrSessionNotFound = errors.New("acp: session 不存在")

// sessionLoader 是后端的**可选能力**：载入既有会话（session/load 面）。
//
// 为什么是可选接口而不是 Backend 加方法（本刀范围铁律：只动 runtime/acp/）：与
// turnOutcomeAware / turnCapAware 同模式——**Backend 接口不变**，靠 type assertion 探测，
// cmd 适配层的 hubBackend 不必跟改（go build ./... 不受影响）。没实现它的后端收到
// session/load 时回 -32000（能力缺失），**不**假装载入成功。
//
// # 语义（载入 = 与 resume 同源的回放路径）
//
// 后端按 sessionId 把既有会话接回来：事件流回放（runtime.ReplaySession 先例）+ 折叠出
// 对话（runtime.FoldConversation 先例——与 host resume / fork 续跑同一条路径），返回
// 折叠出的对话历史；会话不存在 → 返回 ErrSessionNotFound。端点拿到历史后在 **load 响应
// 之前**把整段对话以 session/update 回放给客户端（ACP 口径：restore context + stream
// the entire conversation history back via notifications），随后 load 后的 session/prompt
// 走**既有 prompt 路径**（端点把它登记进 sessions）。
//
// # 诚实边界：历史**只回放给客户端** ≠「模型记住了」
//
// loader 的契约是"把折叠出的历史**交回客户端**"，它**不**保证下一轮 prompt 自带这段
// 历史。模型侧能不能续上，取决于后端的 RunPrompt：**hub 的 RunPrompt 不带历史**
// （hub 每一轮都不带历史——历史只存在于事件流里，靠 fold 重建，见 host/fork.go 的
// "历史口径"）。故"load 之后模型接着此前的对话往下说"这件事**在本实现里不成立**：
// 要让模型续跑，得走 **resume 式路径**（把 fold 出的历史灌回 loop 再跑一轮）——
// 那是 hub 侧的能力，不是 ACP 端点能替代的（挂账：端点侧直连续跑需要给 Backend 加一条
// "带历史跑一轮"的能力，超出本刀范围，不在本刀偷偷加）。
type sessionLoader interface {
	// LoadSession 载入 sid 指向的既有会话，返回折叠出的对话历史（按 seq 升序 fold 的
	// 结果；空会话返回空历史不报错——只要后端认得这个会话）。查无此会话 → ErrSessionNotFound。
	LoadSession(ctx context.Context, sid contracts.Ulid) ([]core.Message, error)
}

// Options 是 Serve 的装配参数。
type Options struct {
	// Backend 是 agent 后端（必填）。
	Backend Backend
	// Approver 是 ACP 审批器（可选）。给了就在 Serve 里 bind 到 conn——它同时必须已塞进
	// host.Config.Approver（cmd 装配时），否则 ask 审批走不到 ACP 面。nil = 不接（hub 用
	// 自己的审批策略，默认 deny-all）。
	Approver *Approver
	// In / Out 是 stdio 两端（缺省 os.Stdin / os.Stdout）。测试注入管道。
	In  io.Reader
	Out io.Writer
	// Log 是日志出口（缺省 slog 到 **stderr**——stdout 只走 RPC 帧，见 doc.go）。
	Log *slog.Logger
	// TurnTimeout 单轮预算（缺省 defaultTurnTimeout）。
	TurnTimeout time.Duration
}

// endpoint 是一个 ACP agent 端点：持有连接、后端、审批器与 per-session 运行态。
type endpoint struct {
	backend     Backend
	conn        *conn
	log         *slog.Logger
	turnTimeout time.Duration

	// turnRoot 是所有 in-flight turn 的 ctx 根：Serve 退出时取消 → 所有 turn 收场。
	turnRoot context.Context

	mu       sync.Mutex
	sessions map[contracts.Ulid]bool       // session/new 开出来的 session（session/prompt 只认这些）
	turns    map[contracts.Ulid]*turnState // session → in-flight turn（一个 session 同时只一轮）
	turnWG   sync.WaitGroup                // 统计在跑 turn（Serve 退出时有界等待）
}

// turnState 是一轮 in-flight turn 的可取消句柄。
type turnState struct {
	cancel context.CancelFunc
}

// Serve 起 ACP agent 端点，阻塞到 stdin EOF（或 ctx 取消 + 下一帧边界）后收场返回。
//
// 返回 nil = 客户端正常断开（stdin EOF）**或 ctx 取消**（Ctrl-C / SIGTERM 是**正常收场**，
// 不是错误——F11：取消当错误会让 `alasol acp` 退出码变 1，脚本没法区分"被叫停"与"崩了"）；
// 返回非 nil = 读循环的其它致命错误。
// 退出前取消所有 in-flight turn 并有界等待它们收尾（不再往 stdout 写）。
func Serve(ctx context.Context, opts Options) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Backend == nil {
		return fmt.Errorf("acp: Options.Backend 必填")
	}
	in := opts.In
	if in == nil {
		in = os.Stdin
	}
	out := opts.Out
	if out == nil {
		out = os.Stdout
	}
	log := opts.Log
	if log == nil {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	turnTimeout := opts.TurnTimeout
	if turnTimeout <= 0 {
		turnTimeout = defaultTurnTimeout
	}

	c := newConn(bufio.NewReaderSize(in, frameReadBufSize), out)
	turnRoot, cancelTurns := context.WithCancel(context.Background())
	e := &endpoint{
		backend:     opts.Backend,
		conn:        c,
		log:         log,
		turnTimeout: turnTimeout,
		turnRoot:    turnRoot,
		sessions:    make(map[contracts.Ulid]bool),
		turns:       make(map[contracts.Ulid]*turnState),
	}
	// 审批器 bind 到 conn（它已由 cmd 塞进 host.Config.Approver；这里补上承载连接）。
	if opts.Approver != nil {
		opts.Approver.bind(c)
	}

	// ctx 取消时把输入关掉：读循环阻塞在读上时，光取消 ctx 是叫不醒它的（要等下一帧）。
	// 关输入 → 读立刻返回错误 → 读循环按"终态错"收场（ctx 已取消时不再回错误帧）。
	stopWatch := watchCancelClose(ctx, in)

	log.Info("acp: agent 端点就绪（stdio，JSON-RPC 2.0 newline-delimited）",
		"agent", agentName, "version", agentVersion, "protocol", protocolVersion)

	loopErr := e.readLoop(ctx)
	stopWatch()
	// 收场：取消所有在跑 turn，有界等它们退出（别再往可能已断的 stdout 写）。
	cancelTurns()
	e.drainTurns(turnDrainGrace)
	if errors.Is(loopErr, context.Canceled) {
		// 取消 = 正常收场（不是错误）：端点自己退场，调用方按退出码 0 收。
		log.Info("acp: 上下文取消，端点收场")
		return nil
	}
	return loopErr
}

// watchCancelClose 起一个 watcher：ctx 取消时关掉输入（能关才关），让阻塞中的读醒来。
//
// 返回的 stop 用于正常收场时收掉 watcher（避免 goroutine 泄漏——它会一直等 ctx.Done()）。
func watchCancelClose(ctx context.Context, in io.Reader) func() {
	closer, ok := in.(io.Closer)
	if !ok {
		return func() {}
	}
	done := make(chan struct{})
	stop := func() {
		select {
		case <-done:
		default:
			close(done)
		}
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = closer.Close()
		case <-done:
		}
	}()
	return stop
}

// readLoop 是唯一的入站读循环：解一帧、分发一帧。
//
// ctx 取消在**帧边界**检查（读本身阻塞在 stdin 上——ACP 的自然生命周期是客户端断开
// = stdin EOF；ctx 取消主要给"两帧之间"的中止机会。Serve 另有一个 watcher：ctx 取消时
// 关掉输入，让阻塞中的读立刻醒来——否则 Ctrl-C 要等下一帧才收场）。
//
// 坏帧分流（**终态 vs 可恢复**——F1）：
//
//	可恢复（回一条错误帧后**继续读**）：坏 JSON（顶层是数字/字符串/布尔等标量或语法错，
//	                                          -32700）/ 批量数组（-32600）/
//	                                          超限帧（-32600，该行已跳过）/
//	                                          顶层 null（解成空帧、判不出形态，-32600）
//	终态（回**一条**错误帧后**结束连接**）：底层读错误——流已经坏了，再读也是死循环
//
// 旧实现把终态错也当"可恢复坏帧"→ 无限重试 → 8 秒吐 118 万条错误帧、进程永不退出。
func (e *endpoint) readLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		f, err := e.conn.scanner.next()
		if err != nil {
			var terminal *terminalReadError
			if errors.As(err, &terminal) {
				// 终态：底层流坏了 → 回**一条** parse error 后**结束连接**。
				if ctx.Err() != nil {
					// ctx 已取消 = 正常收场（不该再往 stdout 写东西，也不算错误）。
					e.log.Info("acp: 上下文取消，端点收场")
					return nil
				}
				e.log.Error("acp: 底层读错误，端点收场", "err", err.Error())
				_ = e.conn.sendError(json.RawMessage("null"), codeParseError,
					"Parse error: "+terminal.Error(), nil)
				return nil
			}
			// 可恢复的坏帧：回一条错误帧（id 无可回指 → null）后**继续读后续帧**
			// ——一条坏帧不该打死连接（D7 宽容）。
			code, msg := classifyBadFrame(err)
			e.log.Warn("acp: 坏帧", "err", err.Error())
			_ = e.conn.sendError(json.RawMessage("null"), code, msg, nil)
			continue
		}
		if f == nil {
			// EOF：客户端断开。
			e.log.Info("acp: stdin EOF，端点收场")
			return nil
		}
		e.dispatch(f)
	}
}

// classifyBadFrame 把"可恢复的坏帧"分类成 (JSON-RPC 错误码, 消息)。
//
//	坏 JSON（顶层标量（数字/字符串/布尔）/语法错）→ -32700 Parse error
//	批量数组                                  → -32600 Invalid Request（本实现不支持批量）
//	超限帧                                    → -32600 Invalid Request（该行已跳过，继续读后续帧）
//
// （顶层 `null` 不走这里：它解成"空帧"不算解码错 → dispatch 的 default 分支回 -32600。）
func classifyBadFrame(err error) (int, string) {
	switch {
	case errors.Is(err, errFrameTooLong):
		return codeInvalidRequest, fmt.Sprintf(
			"Invalid Request：帧超过 %d 字节上限（该行已跳过，继续读后续帧）", maxFrameBytes)
	case errors.Is(err, errBatchUnsupported):
		return codeInvalidRequest, "Invalid Request：批量数组（JSON array）不支持"
	default:
		return codeParseError, "Parse error: " + err.Error()
	}
}

// dispatch 按三形态分发一帧。
func (e *endpoint) dispatch(f *inbound) {
	switch {
	case f.isResponse():
		e.handleResponse(f)
	case f.isRequest():
		if !e.checkVersion(f) {
			return
		}
		e.handleRequest(f)
	case f.isNotification():
		// 通知**不回帧**（JSON-RPC 2.0：通知无 id，服务器不得回应）——版本不对也只记日志。
		if !e.checkNotificationVersion(f) {
			return
		}
		e.handleNotification(f)
	default:
		// 合法 JSON 但既非请求/通知也非响应 → -32600（id 能回指就回指）。
		_ = e.conn.sendError(idOrNull(f.ID), codeInvalidRequest,
			"Invalid Request：既非请求/通知也非响应", nil)
	}
}

// checkVersion 校验 jsonrpc 版本（请求必须是 "2.0"）；不符 → 回 -32600 并返 false。
//
// 只用于**请求**（有 id 可回指）；通知走 checkNotificationVersion（不回帧）。
func (e *endpoint) checkVersion(f *inbound) bool {
	if f.JSONRPC == "2.0" {
		return true
	}
	_ = e.conn.sendError(idOrNull(f.ID), codeInvalidRequest,
		fmt.Sprintf("Invalid Request：jsonrpc 必须是 \"2.0\"（得到 %q）", f.JSONRPC), nil)
	return false
}

// checkNotificationVersion 校验通知的 jsonrpc 版本（必须是 "2.0"）；不符 → **只记日志**
// 并返 false（**不回帧**）。
//
// 为什么通知不回帧（JSON-RPC 2.0 §4.1）：通知没有 id，服务器**不得**回应——回一条
// "id: null" 的错误帧等于凭空造一个响应，客户端按 id 配对时对不上（把它当成"对某个
// 请求的回应"），严格的客户端还会判协议违规。故：版本不对 → Warn 记一笔，静默丢弃。
//
// 口径边界："通知"= **无 id 键**的帧（见 frame.isNotification）；**带 `id: null` 键**的
// 仍按请求处理（回帧的 id 回指 null，见 frame.go 的口径）——规范只把"无 id 成员"的
// Request 叫通知，`id: null` 本身是"请求还是通知"就有歧义的写法。
func (e *endpoint) checkNotificationVersion(f *inbound) bool {
	if f.JSONRPC == "2.0" {
		return true
	}
	method := ""
	if f.Method != nil {
		method = *f.Method
	}
	e.log.Warn("acp: 通知的 jsonrpc 版本不对（按协议不回帧，静默丢弃）",
		"method", method, "jsonrpc", f.JSONRPC)
	return false
}

// handleResponse 把客户端回的响应投递给等待中的 call（审批）。
func (e *endpoint) handleResponse(f *inbound) {
	if f.ID == nil {
		return // 响应无 id：无法关联，忽略
	}
	key, ok := idString(*f.ID)
	if !ok {
		return // 非字符串 id：与我们的字符串 id 不同型，按未知响应忽略
	}
	var resp inboundResponse
	if f.Result != nil {
		resp.result = *f.Result
	}
	resp.err = f.Error
	e.conn.deliverResponse(resp, key)
}

// handleRequest 处理入站请求（initialize / session/new / session/load / session/prompt）。
func (e *endpoint) handleRequest(f *inbound) {
	method := *f.Method
	id := idOrNull(f.ID) // id:null（非法但带键）时回指 null，不 panic
	switch method {
	case "initialize":
		e.handleInitialize(id, f.Params)
	case "session/new":
		e.handleNewSession(id, f.Params)
	case "session/load":
		e.handleLoadSession(id, f.Params)
	case "session/prompt":
		e.handlePrompt(id, f.Params)
	default:
		_ = e.conn.sendError(id, codeMethodNotFound,
			fmt.Sprintf("Method not found: %q", method), nil)
	}
}

// handleNotification 处理入站通知（session/cancel）。未知通知：JSON-RPC 规定通知无响应，
// 记日志忽略（不回 -32601——没有 id 可回）。
func (e *endpoint) handleNotification(f *inbound) {
	method := *f.Method
	switch method {
	case "session/cancel":
		e.handleCancel(f.Params)
	default:
		e.log.Debug("acp: 忽略未知通知", "method", method)
	}
}

// handleInitialize 能力协商。
func (e *endpoint) handleInitialize(id json.RawMessage, params json.RawMessage) {
	var p initializeParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			_ = e.conn.sendError(id, codeInvalidParams, "initialize params 解不开: "+err.Error(), nil)
			return
		}
	}
	// 客户端协议版本高于我们时仍回我们的版本（1）——客户端据此决定是否继续（能力协商）。
	if p.ProtocolVersion > protocolVersion {
		e.log.Warn("acp: 客户端协议版本高于本实现",
			"client", p.ProtocolVersion, "agent", protocolVersion)
	}
	// 能力声明**按后端实际能力动态给**（不无条件 true）：
	//
	//	sessionLoader 是可选接口（type assertion 探测）。后端没实现它 → session/load 只能回
	//	-32000（能力缺失）；若这里仍宣称 loadSession=true，就是**骗客户端**——客户端会
	//	真的去 session/load，然后拿到一个"能力缺失"的错（生产后端若没接这条能力，此前
	//	就是每次 session/load 都 -32000）。
	_, hasLoadSession := e.backend.(sessionLoader)
	res := initializeResult{
		ProtocolVersion: protocolVersion,
		AgentCapabilities: agentCapabilities{
			LoadSession:      hasLoadSession, // 后端实现了 sessionLoader 才宣称 true
			PromptSuggestion: false,          // 不发 prompt 建议
		},
		AgentInfo: agentInfo{Name: agentName, Version: agentVersion},
	}
	if err := e.conn.sendResult(id, res); err != nil {
		e.log.Error("acp: 回 initialize 失败", "err", err.Error())
	}
}

// handleNewSession 开新 session（按 cwd 建 hub session；sessionId = hub ULID 原样）。
func (e *endpoint) handleNewSession(id json.RawMessage, params json.RawMessage) {
	var p newSessionParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			_ = e.conn.sendError(id, codeInvalidParams, "session/new params 解不开: "+err.Error(), nil)
			return
		}
	}
	// mcpServers **忽略**（doc 口径）：hub 的 MCP client 是装配期（TOML [[mcp.servers]]）
	// 配好的，ACP 端点不在 session 面动态起 MCP server。忽略而非报错——宽容客户端多带字段。
	if len(p.MCPServers) > 0 {
		e.log.Debug("acp: session/new 带了 mcpServers，按口径忽略（hub MCP 走装配期配置）")
	}
	sid, err := e.backend.CreateSession(p.CWD)
	if err != nil {
		// 错误摘要脱敏后再外发（路径/凭据形状不进 stdout——H18）。
		_ = e.conn.sendError(id, codeInternalError, "建 session 失败: "+sanitizeErrSummary(err), nil)
		return
	}
	e.mu.Lock()
	e.sessions[sid] = true
	e.mu.Unlock()
	if err := e.conn.sendResult(id, newSessionResult{SessionID: string(sid)}); err != nil {
		e.log.Error("acp: 回 session/new 失败", "err", err.Error())
	}
}

// handleLoadSession 载入既有会话（session/load，agentCapabilities.loadSession=true 的那半）。
//
// # 流程（ACP Loading Sessions 口径）
//
//	① 解 params（sessionId 必须是合法 ULID；cwd/mcpServers 宽容——mcpServers 按
//	   session/new 同一口径**忽略**，hub 的 MCP 走装配期配置）；
//	② 探后端载入能力（sessionLoader 可选接口）→ **派 goroutine** 跑真正的载入
//	   （runLoad）：后端按 sessionId 回放事件流、折叠出对话（与 resume 同源路径），
//	   **查无此会话 → 协议错误帧**（ErrSessionNotFound → -32602，与 session/prompt 的
//	   "未知 session"同一形状；其它后端错误 → -32603 + 脱敏摘要，与建 session 失败同形状）；
//	③ 载入成功 → 登记进 sessions（此后该 session 的 session/prompt 走**既有 prompt
//	   路径**，与 session/new 开出来的无差别）；
//	④ 在 load **响应之前**把折叠出的整段历史以 session/update 回放（replayHistory）——
//	   ACP 要求 restore context 后把整个对话历史流回客户端，回放完才回 {}。
//
// # 为什么载入跑在 goroutine 上（头阻塞修）
//
// 读循环是**唯一**的入站通道。载入要回放整段历史（后端回放 + 折叠 + 一串 update），
// 同步跑在读循环上时，这段时间里**别的 session 的帧全部排队**：最要命的是
// session/cancel 与 session/request_permission 的应答——前者迟迟不被处理（用户点取消
// 没反应），后者迟迟不被投递（审批悬着，整轮预算白烧到超时）。故：参数校验留在读循环
// （快、纯本地），**真正的载入 + 回放 + 响应搬 goroutine**，读循环立刻回到收下一帧。
//
// # 顺序怎么保（回放必须先于响应）
//
// 回放与响应在**同一个 goroutine** 里顺序写（conn.writeFrame 持写互斥，一行一帧）：
// update 帧先写、result 帧后写 → 客户端看到的顺序天然正确（update 先于 result）。
// 别的 session 的 update 可能夹在中间——那是正常的（通知本就允许交错）。
//
// # 登记时点（载入成功**之后**才登记进 sessions）
//
// session 的登记发生在**载入成功之后**（载入失败就不该留下一个"已知 session"）。
// 故：**不等 load 响应就立刻发 prompt** 的客户端，可能撞上"未知 session"（-32602）——
// ACP 的正常时序是"等 load 响应再发 prompt"，那时 session 已登记。
func (e *endpoint) handleLoadSession(id json.RawMessage, params json.RawMessage) {
	var p loadSessionParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			_ = e.conn.sendError(id, codeInvalidParams, "session/load params 解不开: "+err.Error(), nil)
			return
		}
	}
	sid, ok := parseSessionID(p.SessionID)
	if !ok {
		_ = e.conn.sendError(id, codeInvalidParams, "sessionId 不是合法 ULID", nil)
		return
	}
	// mcpServers 忽略（与 session/new 同口径：hub MCP 走装配期配置，不在 session 面动态接）。
	if len(p.MCPServers) > 0 {
		e.log.Debug("acp: session/load 带了 mcpServers，按口径忽略（hub MCP 走装配期配置）")
	}
	loader, ok := e.backend.(sessionLoader)
	if !ok {
		// 后端没实现载入能力：**不假装成功**（回能力缺失的服务器错误帧）。
		_ = e.conn.sendError(id, codeServerError,
			"后端未实现 session/load（载入能力缺失，无法载入会话）", nil)
		return
	}
	// 真正的载入搬 goroutine（读循环不因回放整段历史而头阻塞——见函数头）。
	go e.runLoad(loader, id, sid)
}

// runLoad 跑一次载入（handleLoadSession 派出的 goroutine）：后端回放 → 登记 session →
// 回放历史 → 回响应。
//
// 错误分流与同步版逐字一致（ErrSessionNotFound → -32602；其它 → -32603 + 脱敏摘要）。
func (e *endpoint) runLoad(loader sessionLoader, id json.RawMessage, sid contracts.Ulid) {
	history, err := loader.LoadSession(e.turnRoot, sid)
	if err != nil {
		if errors.Is(err, ErrSessionNotFound) {
			// 会话不存在 → 协议错误回执（与 prompt 的"未知 session"同形状：-32602）。
			_ = e.conn.sendError(id, codeInvalidParams,
				"session 不存在（该 sessionId 没有可载入的历史）", nil)
			return
		}
		// 其它后端错误 → 内部错误帧，摘要**脱敏后**外发（与建 session 失败同口径——H18）。
		_ = e.conn.sendError(id, codeInternalError,
			"载入 session 失败: "+sanitizeErrSummary(err), nil)
		return
	}
	e.mu.Lock()
	e.sessions[sid] = true
	e.mu.Unlock()
	// 回放历史（必须在响应之前：ACP 客户端拿到 load 响应即认为历史同步完成）。
	e.replayHistory(sid, history)
	if err := e.conn.sendResult(id, loadSessionResult{}); err != nil {
		e.log.Error("acp: 回 session/load 失败", "err", err.Error())
	}
	e.log.Info("acp: session/load 完成", "session", string(sid), "history", len(history))
}

// replayHistory 把折叠出的对话历史逐条映射成 session/update 回放（load 响应前发出）。
//
// 映射（core.Message 的角色 → ACP sessionUpdate）：
//
//	user      → user_message_chunk（ACP v1 有**用户侧** chunk 类型——load 回放要补全
//	            整段 transcript，客户端可能换了进程、手里没有旧 prompt）
//	assistant → agent_message_chunk；挂着的 ToolCalls → tool_call（rawInput 经 redactJSON
//	            脱敏，与 live 桥同口径——H18：历史里的工具参数同样可能含凭据）
//	tool      → tool_call_update（completed + 结果文本）
//	其它角色  → 跳过（宽容：未来新增角色不因回放崩）
//
// # 工具调用**统一按 completed 回放**（fold 取舍，如实标注）
//
// runtime.FoldConversation 把 tool.call.completed 与 tool.call.failed **都**折成一条
// role="tool" 的消息（不带成功/失败标记）——所以回放时分不出"这次工具是成功还是失败"，
// 只能**统一按 completed** 发。这是 fold 的既有取舍（不是回放层偷懒）：要分得清，得改
// fold 的消息形状（core.Message 加字段 = 契约面改动，超出本刀范围）。诚实边界写在这里，
// 免得客户端把"completed"当成"这次工具真的成功了"。
//
// # 脱敏口径：键名遮蔽只在**结构化面**成立
//
// rawInput 经 redactJSON：它**只处理合法 JSON**（非 JSON 的工具参数 → 整体不转发，
// 宁缺毋滥）；键名遮蔽（token/secret/password/…）也只在 JSON 的**键**这一层成立——
// 散文/命令里的凭据靠**值级**启发式（core.IsCredentialValue 同口径 + 鉴权头/连接串/
// PEM/AKIA 等形态）兜。故：回放里的 rawInput 是**参数摘要**，不是完整参数回放。
//
// 与 live 桥的分工：live 事件流里 user.prompt **不发**（用户自己说的，回显是噪声，见
// bridge.go 映射表）；load 回放则**必须**发——那是另一回事（补全历史，不是回显刚说的）。
func (e *endpoint) replayHistory(sid contracts.Ulid, history []core.Message) {
	for _, m := range history {
		switch m.Role {
		case "user":
			if m.Content == "" {
				continue
			}
			e.sendUpdate(sid, userMessageChunkUpdate(m.Content))
		case "assistant":
			if m.Content != "" {
				e.sendUpdate(sid, messageChunkUpdate(m.Content))
			}
			for _, tc := range m.ToolCalls {
				e.sendUpdate(sid, toolCallUpdateStarted(string(tc.ID), tc.Name, redactJSON(tc.Args)))
			}
		case "tool":
			e.sendUpdate(sid, toolCallUpdateCompleted(string(m.ToolCallID), m.Content))
		}
	}
}

// handlePrompt 起一轮 prompt（派 goroutine 跑，读循环继续收 session/cancel / 审批响应）。
func (e *endpoint) handlePrompt(id json.RawMessage, params json.RawMessage) {
	var p promptParams
	if len(params) == 0 {
		_ = e.conn.sendError(id, codeInvalidParams, "session/prompt 缺 params", nil)
		return
	}
	if err := json.Unmarshal(params, &p); err != nil {
		_ = e.conn.sendError(id, codeInvalidParams, "session/prompt params 解不开: "+err.Error(), nil)
		return
	}
	sid, ok := parseSessionID(p.SessionID)
	if !ok {
		_ = e.conn.sendError(id, codeInvalidParams, "sessionId 不是合法 ULID", nil)
		return
	}
	e.mu.Lock()
	known := e.sessions[sid]
	_, inFlight := e.turns[sid]
	e.mu.Unlock()
	if !known {
		_ = e.conn.sendError(id, codeInvalidParams, "未知 session（先用 session/new 开一个）", nil)
		return
	}
	if inFlight {
		// 一个 session 同时只跑一轮（宿主默认口径，G25）：拒绝并发第二枪。
		_ = e.conn.sendError(id, codeServerError, "该 session 已有一轮 in-flight turn（同时只跑一轮）", nil)
		return
	}
	text := extractPromptText(p.Prompt)
	if strings.TrimSpace(text) == "" {
		_ = e.conn.sendError(id, codeInvalidParams, "prompt 不能为空（空 prompt 等于浪费一次模型调用）", nil)
		return
	}

	turnCtx, cancel := context.WithTimeout(e.turnRoot, e.turnTimeout)
	e.mu.Lock()
	e.turns[sid] = &turnState{cancel: cancel}
	e.turnWG.Add(1)
	e.mu.Unlock()

	go e.runTurn(turnCtx, cancel, id, sid, text)
}

// runTurn 跑一整轮：订阅事件 → 桥接 session/update → RunPrompt → 收尾回 stopReason。
//
// 收尾三岔（F3/F4/F5）：
//
//	成功            → 回 {stopReason}（end_turn / max_turn_requests / cancelled，见 mapStopReason）
//	被取消/超时    → 仍回 {stopReason}（cancelled / max_turn_requests）——ACP 的 stopReason
//	                  枚举就是为"这轮没跑完"准备的，比回错误帧更贴协议
//	后端真出错      → 回 **JSON-RPC 错误帧**（-32603 Internal error，消息带脱敏摘要），
//	                  **不**回成功的 {stopReason:"end_turn"}（那等于谎报"这轮正常收场"）
func (e *endpoint) runTurn(ctx context.Context, cancel context.CancelFunc, id json.RawMessage, sid contracts.Ulid, text string) {
	defer func() {
		cancel()
		e.mu.Lock()
		delete(e.turns, sid)
		e.mu.Unlock()
		e.turnWG.Done()
	}()

	tb := &turnBridge{}
	var bridgeDone chan struct{}
	sub := e.backend.SubscribeEvents(sid)
	if sub != nil {
		bridgeDone = make(chan struct{})
		go e.bridge(sub, sid, tb, bridgeDone)
	}

	res, err := e.backend.RunPrompt(ctx, sid, text)

	if sub != nil {
		// 关订阅 → 桥把已缓冲的增量排干后退出；等它退出保证所有 update 先于 prompt 响应。
		sub.Close()
		<-bridgeDone
	}

	stopReason := mapStopReason(ctx)
	if stopReason == stopReasonEndTurn && e.turnsExhausted(res) {
		// 轮数用尽（loop 被 MaxTurns 截断）**或被预算闸门截断**（Result.TruncatedByBudget）
		// → max_turn_requests（F4：枚举内，且语义对；映射取舍见 turnsExhausted 的注释）。
		stopReason = stopReasonMaxTurnRequests
	}
	if err != nil && !turnCutShort(ctx, err) {
		// 后端真出错（不是被取消/超时）→ 回错误帧，不回成功 result（F3）。
		e.log.Error("acp: 一轮 prompt 跑失败", "session", string(sid), "err", err.Error())
		if serr := e.conn.sendError(id, codeInternalError,
			"Internal error: "+sanitizeErrSummary(err), nil); serr != nil {
			e.log.Error("acp: 回 session/prompt 错误失败", "err", serr.Error())
		}
		return
	}
	if err != nil {
		e.log.Warn("acp: 一轮 prompt 被中断（按 stopReason 收场）",
			"session", string(sid), "stop_reason", stopReason, "err", err.Error())
	}
	// 增量与最终文本对账（F5）：桥出去的增量只覆盖了最终文本的一部分 → 补发差量
	// （**以最终文本为准**；一轮没桥出任何增量时，这就是降级补发整段）。
	e.reconcileFinalText(sid, tb, res.Text)
	if err := e.conn.sendResult(id, promptResult{StopReason: stopReason}); err != nil {
		e.log.Error("acp: 回 session/prompt 失败", "err", err.Error())
	}
}

// turnCutShort 判定"这轮是被叫停的（取消/超时）"而不是"后端真出错"。
//
// 取消/超时时后端通常把 ctx.Err() 当错误返回（loop 就是这么做的）——那种情况该回
// stopReason（cancelled / max_turn_requests），不该回 Internal error。
func turnCutShort(ctx context.Context, err error) bool {
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// turnsExhausted 判定"这轮被**上限**截断了"（轮数上限 MaxTurns，或**预算上限**）。
//
// 四档判据（N3 + 缺口 5 第二件，从权威到兜底）：
//
//	① **权威（轮数）**：Result.TruncatedByMaxTurns —— loop 亲自打的标记（真 hub 走这条）。
//	①' **权威（预算）**：Result.TruncatedByBudget —— 逐轮预算闸门拒绝了本轮（缺口 5 第二件；
//	    W15 成本预算）。同样走权威路径：**不回落**旧口径。
//	② 后端声明"我的结果带权威标记"（turnOutcomeAware）→ 到此为止：**不回落**旧口径
//	   （否则"恰好用满上限但正常收敛"会被误报成 max_turn_requests）。
//	③ 老/第三方/假后端只报轮数上限 → **回落旧口径**（"实际轮数 ≥ 上限即视为截断"）。
//
// # 映射取舍（budget → max_turn_requests，如实写明）
//
// ACP 的 stopReason 是**闭合枚举**（end_turn / max_tokens / max_turn_requests / refusal /
// cancelled——见包 doc），**没有**"预算耗尽"这一档。故"被预算闸门截断"落到语义最近的
// **max_turn_requests**：它的语义是"这轮请求撞上了上限、没能跑完"——预算上限正是"撞上
// 了一个上限"，与轮数上限同族；max_tokens 是"输出 token 上限"（另一种额度，但语义更窄：
// 它特指**输出**额度，而预算闸门管的是**累计 input+output**，用 max_tokens 会让客户端
// 误以为"撞的是输出 token 上限"）。cancelled 留给**显式取消**，语义更远（不是被上限挡住，
// 是人叫停的）。
//
// 取舍的代价（如实写明）：客户端**分不出**"轮数用尽"与"预算用尽"——两者都报
// max_turn_requests。细分要改 ACP 契约（枚举外取值会让现成客户端解不出合法 stopReason，
// 见包 doc 的 F4 口径），故本刀**不加新值**、只在日志与 Result.TruncationReason 里留
// 足线索（hub 侧对 TruncatedByBudget 另有一条 Warn，见 host/hub.go 的 RunPrompt）。
//
// 诚实边界：③ 是**猜**（Turns ≥ 上限 分不清"正常收敛在最后一轮"与"被截断"）——只对
// 没有权威标记的后端成立；能透传标记的后端一律走 ①/①'。
func (e *endpoint) turnsExhausted(res loop.Result) bool {
	// ① 权威标记：loop 说"这轮被 MaxTurns 截断了"。
	if res.TruncatedByMaxTurns {
		return true
	}
	// ①' 权威标记：loop 说"这轮被**预算闸门**截断了"（缺口 5 第二件）。
	if res.TruncatedByBudget {
		return true
	}
	// ② 后端能给出权威标记 → 上面那两句 false 就是"没被截断"，不再猜。
	if aware, ok := e.backend.(turnOutcomeAware); ok && aware.TurnOutcomeAuthoritative() {
		return false
	}
	// ③ 兜底：老口径（轮数用满即视为截断）。
	capper, ok := e.backend.(turnCapAware)
	if !ok {
		return false
	}
	limit := capper.TurnCap()
	if limit <= 0 {
		return false
	}
	return res.Turns >= limit
}

// reconcileFinalText 对账"已桥出的增量"与"最终文本"，不一致就补发差量（F5）。
//
// 口径（doc 写明）：**增量与最终文本不一致时以最终文本为准**——
//   - 已发增量是最终文本的前缀 → 补发剩下的差量（一条 agent_message_chunk）；
//   - 已发增量与最终文本**分叉**（provider 改了写最终文本）→ 把最终文本整段发出来
//     （最后一条为准；客户端可能看到重复，但权威文本是它）；
//   - 一致 → 不发（不重复）。
//
// 一轮**没桥出任何增量**（无订阅 / provider 未流式）时，已发文本为空 → 走第一条分支，
// 等价于"降级补发整段"（doc 降级口径）。
//
// # 为什么只与**最后一轮**的增量比（N2）
//
// core/loop 的最终文本（Result.Text）是**最后一轮**的文本（lastText 语义）：第 1 轮
// "正文 + 工具调用"、第 2 轮"最终答复"时，最终文本 = 第 2 轮那段。而桥出去的增量是
// **所有轮**的拼接——直接比会判"分叉" → 整段重发 → **客户端收到两份最终答复**。
// 故桥接层按 model.delta 载荷里的 turn 分桶累计，对账只取**最后一轮**的增量。
//
// # 为什么两侧都 trim（N2）
//
// 尾随空白（"\n" / 空格）不该判成"分叉"：最终文本带尾随换行、增量不带（或反过来）时，
// 不 trim 就会整段重发。故**两侧同一 trim 口径**（TrimSpace）再比。
func (e *endpoint) reconcileFinalText(sid contracts.Ulid, tb *turnBridge, final string) {
	final = strings.TrimSpace(final)
	if final == "" {
		return
	}
	sent := strings.TrimSpace(tb.sentText())
	if sent == final {
		return
	}
	if strings.HasPrefix(final, sent) {
		e.sendMessageChunk(sid, final[len(sent):])
		return
	}
	// 分叉：最终文本整段为准。
	e.sendMessageChunk(sid, final)
}

// handleCancel 取消某 session 的 in-flight turn（notification，无响应）。
func (e *endpoint) handleCancel(params json.RawMessage) {
	var p cancelParams
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			e.log.Warn("acp: session/cancel params 解不开", "err", err.Error())
			return
		}
	}
	sid, ok := parseSessionID(p.SessionID)
	if !ok {
		e.log.Warn("acp: session/cancel 的 sessionId 非法", "session_id", p.SessionID)
		return
	}
	e.mu.Lock()
	ts, ok := e.turns[sid]
	e.mu.Unlock()
	if !ok {
		e.log.Debug("acp: session/cancel 无 in-flight turn（可能已结束）", "session", string(sid))
		return
	}
	ts.cancel()
	e.log.Info("acp: 已取消 in-flight turn", "session", string(sid))
}

// drainTurns 有界等待在跑 turn 收尾（Serve 退出路径）。
func (e *endpoint) drainTurns(grace time.Duration) {
	done := make(chan struct{})
	go func() {
		e.turnWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(grace):
		e.log.Warn("acp: 退出时等待在跑 turn 收尾超时", "grace", grace.String())
	}
}

// mapStopReason 把一轮的结束原因映射成 ACP stopReason。
//
// **产出值必须落在 ACP 的枚举里**（end_turn / max_tokens / max_turn_requests / refusal /
// cancelled）——枚举外取值（如曾经的 "max_turn_duration"）会让现成 ACP 客户端解不出
// 合法 stopReason（F4）。
//
//	end_turn           正常收场（模型收敛；**含**"恰好用满 MaxTurns 但正常收敛"——
//	                   那不是截断，见 turnsExhausted）
//	cancelled          ctx 被显式取消（session/cancel，或 Serve 退出级联取消）
//	max_turn_requests  整轮预算耗尽（ctx 超时）**或**被 MaxTurns 截断（模型还在要工具）
//
// 为什么超时映射成 max_turn_requests 而不是 cancelled：ACP 的 stopReason 枚举**没有**
// "超时"这一项；两个候选里，max_turn_requests 的语义是"这轮请求撞上了上限、没能跑完"
// ——超时正是"整轮预算（轮数/时间上限）用尽"，与它同族；cancelled 留给**显式取消**
// （人/客户端主动叫停），两者在审计上可区分。整轮用尽 → max_turn_requests。
func mapStopReason(ctx context.Context) string {
	if ctx == nil {
		return stopReasonEndTurn
	}
	switch ctx.Err() {
	case context.Canceled:
		return stopReasonCancelled
	case context.DeadlineExceeded:
		return stopReasonMaxTurnRequests
	default:
		return stopReasonEndTurn
	}
}

// idOrNull 取入站 id（用于错误回指）；nil → "null"（JSON-RPC 允许错误响应 id 为 null）。
func idOrNull(id *json.RawMessage) json.RawMessage {
	if id == nil {
		return json.RawMessage("null")
	}
	return *id
}

// parseSessionID 解析并校验 sessionId（合法 ULID）。
func parseSessionID(s string) (contracts.Ulid, bool) {
	u := contracts.Ulid(strings.TrimSpace(s))
	if !core.ValidUlid(u) {
		return "", false
	}
	return u, true
}

// ---------------------------------------------------------------------------
// ACP 方法 params / result 形状
// ---------------------------------------------------------------------------

type initializeParams struct {
	ProtocolVersion    int             `json:"protocolVersion"`
	ClientCapabilities json.RawMessage `json:"clientCapabilities,omitempty"`
	ClientInfo         *clientInfo     `json:"clientInfo,omitempty"`
}

type clientInfo struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

type initializeResult struct {
	ProtocolVersion   int               `json:"protocolVersion"`
	AgentCapabilities agentCapabilities `json:"agentCapabilities"`
	AgentInfo         agentInfo         `json:"agentInfo"`
}

type agentCapabilities struct {
	LoadSession      bool `json:"loadSession"`
	PromptSuggestion bool `json:"promptSuggestion"`
}

type agentInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type newSessionParams struct {
	CWD        string          `json:"cwd"`
	MCPServers json.RawMessage `json:"mcpServers,omitempty"`
}

type newSessionResult struct {
	SessionID string `json:"sessionId"`
}

// loadSessionParams 是 session/load 的 params（ACP LoadSessionRequest 的本实现子集：
// sessionId 必填；cwd / mcpServers 按宽容口径吃下——mcpServers 忽略，见 handleLoadSession）。
type loadSessionParams struct {
	SessionID  string          `json:"sessionId"`
	CWD        string          `json:"cwd,omitempty"`
	MCPServers json.RawMessage `json:"mcpServers,omitempty"`
}

// loadSessionResult 是 session/load 的 result：ACP LoadSessionResponse 的 v1 形状里
// configOptions / modes 全是可选（无必填字段），本实现回一个空对象 `{}`——历史不在
// result 里，走 session/update 回放（见 replayHistory）。
type loadSessionResult struct{}

type promptParams struct {
	SessionID string          `json:"sessionId"`
	Prompt    json.RawMessage `json:"prompt"`
}

type promptResult struct {
	StopReason string `json:"stopReason"`
}

type cancelParams struct {
	SessionID string `json:"sessionId"`
}

// extractPromptText 从 session/prompt 的 prompt 字段取纯文本。
//
// 宽容三种写法：裸字符串 / content block 数组 / 单个 content block——都取其中 type=text
// （或缺省 type）的 text 拼接。ACP 规范是 block 数组，这里多容忍两种是宽容客户端。
//
// **多 block 之间用 "\n" 分隔**（不是直接首尾相接）：直接拼接会把两段文字粘成一个词
// （"hello" + "world" → "helloworld"），语义就变了；"\n" 保住"这是两段"的边界，
// 对模型也是更自然的分段。
func extractPromptText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var blocks []contentBlock
	if err := json.Unmarshal(raw, &blocks); err == nil {
		var texts []string
		for _, blk := range blocks {
			if blk.Type == "" || blk.Type == "text" {
				texts = append(texts, blk.Text)
			}
		}
		return strings.Join(texts, "\n")
	}
	var blk contentBlock
	if err := json.Unmarshal(raw, &blk); err == nil {
		return blk.Text
	}
	return ""
}
