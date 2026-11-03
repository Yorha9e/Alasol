package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime"
	"github.com/Yorha9e/alasol/runtime/acp"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// acpMain 是 `alasol acp`：起 ACP agent 端点（JSON-RPC 2.0 over stdio，v4-2-B，K19）。
//
// 与 serve 同款装配 host.Hub（复用 config 装载面：model/base-url/api-key/allow/ask/…），
// 但不启 HTTP/WS——端点形态是 **stdio**。典型用法（跨机，鉴权交 SSH）：
//
//	ssh host alasol acp [-model ...] [-allow read,write,bash] [-ask bash] [-db alasol.db]
//
// 客户端以本进程为子进程拉起，用现成 ACP client 经 stdio 接入（不自研分布式协议，K19）。
//
// # 审批模式（acp 特有）
//
// ask 审批**恒走 ACP 协议审批器**（session/request_permission → client 回 outcome）——
// 无视 -approval 的值（那是给 serve/chat 的 stdin/GUI 审批选的；ACP 模式下没有本进程
// stdin 也没有 WS 客户端，审批只能回到这条 stdio 连接上）。无人应答/超时 = deny（fail-close，
// D14/H8；超时时长 = 整轮预算，与 host promptRunTimeout 同口径）。-allow / -ask 照旧生效：
// 它们决定哪些工具要问、哪些直接放行。
//
// # 整轮预算（-prompt-timeout）
//
// -prompt-timeout 的秒数换算成 acp.Options.TurnTimeout（0 = 用缺省 10min，与 host 协议面
// **同一个常量** core.DefaultPromptRunTimeout）。不换算的话这个 flag 在 acp 子命令上
// 是**死的**（F8）——运维会以为改了预算其实没改。
//
// # 退出码
//
// 0 = 正常收场（stdin EOF / Ctrl-C 取消——取消**不是错误**）；1 = 端点错误；2 = 配置被拒。
func acpMain(args []string) int {
	fs := flag.NewFlagSet("acp", flag.ExitOnError)
	f := registerHubFlags(fs) // 复用 serve/chat 的共有 flag 面（model/base-url/api-key/allow/ask/db/…）
	f.parse(fs, args)

	cfg, err := f.config()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	// 档位白名单（fail-close）：配置被拒 = exit 2（与 serve 同口径）。
	if rejectInvalidReasoningEffort(cfg) {
		return 2
	}

	// ACP 审批器：先造（塞进 cfg.Approver，供 host.New 装配 loop 时捕获）， Serve 再把
	// stdio 连接 bind 上去（conn 后置，见 acp.Approver 的"绑定时序"）。
	approver := acp.NewApprover()
	cfg.Approver = approver

	h, err := host.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "装配失败:", err)
		return 1
	}
	defer h.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// stdout 纯净性：ACP 端点占住 os.Stdout 只写 RPC 帧；上面的错误都走 os.Stderr。
	// 起点一行都不往 stdout 打（banner / 日志全走 stderr，见 runtime/acp 的纪律）。
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// session/load 的**回放面**：host 不导出事件库句柄，故按同一个 DB 路径另开一条连接
	// （与 `alasol resume` 同款：它也自己开库回放）。**口径如实**（critic F3）：拿到的是
	// core.Store 完整句柄（含 Append 能力）——本处**只用读方法**，写面纪律靠用法维持；
	// 另 :memory: 路径下两次 Open 是两个不同的库 → 载不到会话（生产 DBPath 是文件库，不受影响）。
	// 开不出来 → 载入能力降级（端点会因"后端没实现 sessionLoader"… 注意：hubBackend 的
	// LoadSession 方法**在**，故 initialize 仍宣称 loadSession=true；此时载入一律回
	// "会话不存在"——如实记一条 Warn，别静默）。
	loadStore, lerr := sqlite.Open(cfg.DBPath, sqlite.Options{})
	var loadFace core.Store // 接口零值（不是 typed-nil *sqlite.Store——见下）
	if lerr != nil {
		logger.Warn("alasol acp: 开只读回放面失败（session/load 将回'会话不存在'）", "db", cfg.DBPath, "err", lerr.Error())
	} else {
		defer func() { _ = loadStore.Close() }()
		loadFace = loadStore
	}
	// **typed-nil 陷阱**（critic F2）：Open 失败时 loadStore 是 (*sqlite.Store)(nil)——
	// 直接赋进 core.Store 接口会让 `b.store == nil` 判定为 false，nil 闸门失效 →
	// ReplaySession 空指针 panic（进程崩）。只有真拿到句柄才装进接口，失败留接口零值。
	backend := &hubBackend{h: h, log: logger, maxTurns: cfg.MaxTurns, store: loadFace}

	// 整轮预算：-prompt-timeout（秒）→ TurnTimeout；0 = 用 acp 缺省（= host 同一常量）。
	if err := acp.Serve(ctx, acp.Options{
		Backend:     backend,
		Approver:    approver,
		Log:         logger,
		TurnTimeout: acpTurnTimeout(cfg),
	}); err != nil {
		fmt.Fprintln(os.Stderr, "ACP 端点错误:", err)
		return 1
	}
	// 正常收场（stdin EOF / Ctrl-C）：一条 info 走 stderr，退出码 0（F11：取消不是错误）。
	fmt.Fprintln(os.Stderr, "alasol acp: 端点收场（stdin EOF / 取消）")
	return 0
}

// acpTurnTimeout 把配置里的整轮预算（秒）换算成 ACP 端点的 TurnTimeout。
//
// 0 = 用 acp 包的缺省（core.DefaultPromptRunTimeout，与 host 协议面**同一个常量**）。
//
// 为什么必须有这一步（F8）：不换算的话 -prompt-timeout 在 `alasol acp` 上是**死的**
// ——配置读进来了、装配也认了，可端点仍用硬编码的 10min，运维会以为改了预算其实没改。
func acpTurnTimeout(cfg host.Config) time.Duration {
	if cfg.PromptRunTimeoutSec <= 0 {
		return 0
	}
	return time.Duration(cfg.PromptRunTimeoutSec) * time.Second
}

// hubBackend 把 *host.Hub 适配成 acp.Backend（cmd 是唯一同时 import host 与 runtime 的
// 组合层，09 依赖方向：runtime 不 import host）。
//
//   - CreateSession：按 cwd 建 hub session；**sessionId = hub ULID 原样**（task 口径）。
//   - RunPrompt：透传 hub.RunPrompt（同步跑完一整轮）。
//   - SubscribeEvents：订阅该 session 的增量事件（含 durable=false 的 model.delta）供
//     桥接 session/update；按 SessionID 过滤（种类由 acp 侧按 kind 映射）。
//   - LoadSession：**可选能力** sessionLoader（session/load 面）——按 sessionId 回放事件
//     流、折叠出对话（与 resume 续跑同源路径），见下。
type hubBackend struct {
	h   *host.Hub
	log *slog.Logger
	// maxTurns 是本 hub 的单轮轮数上限（0 = 用 loop.DefaultMaxTurns）。
	//
	// 用途：端点的**兜底**判据（→ stopReason=max_turn_requests，见 acp 包的 turnCapAware
	// 可选能力）。**权威**判据是 loop.Result.TruncatedByMaxTurns（loop 亲自打的截断标记，
	// hub 原样透传）——"轮数 ≥ 上限"分不清"正常收敛在最后一轮"与"被截断"，故只作兜底。
	maxTurns int
	// store 是 **session/load 的只读回放面**（nil = 没有回放面 → 载入一律回"会话不存在"）。
	//
	// 为什么另开一条连接：host **不导出**事件库句柄（09 依赖方向 + 单一写者纪律：写面
	// 只归 hub 一家），而 runtime.ReplaySession 需要一个 core.Store。故 cmd 侧按**同一个
	// DB 路径**另开一条**只读**连接（与 `alasol resume` 的装配同款：它也是自己开库读
	// 回放）。只读不改：事件流仍是唯一事实源，写面仍只有 hub。
	store core.Store
}

// 编译期断言：*hubBackend 满足 acp.Backend。
var _ acp.Backend = (*hubBackend)(nil)

// TurnCap 实现 acp 端的**可选能力**：报单轮轮数上限（供端点在**没有**权威截断标记时
// 兜底判"轮数用尽"——见 TurnOutcomeAuthoritative）。
func (b *hubBackend) TurnCap() int {
	if b.maxTurns > 0 {
		return b.maxTurns
	}
	return loop.DefaultMaxTurns
}

// TurnOutcomeAuthoritative 实现 acp 端的**可选能力**：声明"本后端返回的 loop.Result 带
// **权威**的截断标记"（Result.TruncatedByMaxTurns）。
//
// 为什么要有这句：端点判"轮数用尽"（→ stopReason=max_turn_requests）时，光看
// Result.Turns **分不清**"正常收敛在最后一轮"与"被 MaxTurns 截断"（两者 Turns 都等于
// 上限：MaxTurns=1 单轮收敛 vs MaxTurns=1 被截断，Turns 都是 1）。hub 透传的是**真
// loop.Result**（截断标记由 loop 亲自打）→ 端点据此判，不用猜。
func (b *hubBackend) TurnOutcomeAuthoritative() bool { return true }

func (b *hubBackend) CreateSession(cwd string) (contracts.Ulid, error) {
	dir, ok := resolveACPCwd(cwd)
	if !ok {
		// 回落：服务端进程自己的 cwd（跨机口径——agent 在服务端自己的仓里干活）。
		fallback, err := os.Getwd()
		if err != nil {
			fallback = ""
		}
		b.logger().Warn("alasol acp: session/new 的 cwd 在服务端不可用，回落服务端进程 cwd",
			"requested", cwd, "fallback", fallback)
		dir = fallback
	}
	return b.h.CreateSession(host.SessionSpec{CWD: dir})
}

// logger 取本适配层的日志器（没装配时兜底一个 **stderr** 的——stdout 只走 RPC 帧）。
func (b *hubBackend) logger() *slog.Logger {
	if b.log != nil {
		return b.log
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func (b *hubBackend) RunPrompt(ctx context.Context, sessionID contracts.Ulid, prompt string) (loop.Result, error) {
	return b.h.RunPrompt(ctx, sessionID, prompt)
}

func (b *hubBackend) SubscribeEvents(sessionID contracts.Ulid) *bus.Subscription {
	// hub 只给**只读**订阅口（host.EventSource：只有 Subscribe，没有 Publish——F6）。
	src := b.h.Bus()
	if src == nil {
		return nil // hub 未装配总线（不该发生）→ acp 降级为整段消息
	}
	return src.Subscribe(bus.Filter{SessionIDs: []contracts.Ulid{sessionID}})
}

// LoadSession 实现 acp 的**可选能力** sessionLoader（session/load 面）。
//
// # 路径：与 resume 续跑**同源**
//
//	runtime.ReplaySession（事件流按 seq 全量读回——事件流是唯一事实源）
//	+ runtime.FoldConversation（折叠出对话）→ 返回折叠出的 []core.Message。
//
// 与 host 的 fork 旁支续跑同一条路径（host/fork.go 的 RunBtw 也用这两个函数）。
//
// # 会话不存在 → acp.ErrSessionNotFound
//
// 端点据此回**协议错误帧**（-32602，与 session/prompt 的"未知 session"同一形状）——
// **不**假装载入成功。判据 = "该 sessionId 没有任何持久化历史"（与端点的
// ErrSessionNotFound 文档同口径：查无此会话 / 还没跑过任何一轮）。
//
// # 诚实边界（模型侧不因此"记住"历史）
//
// 本方法只把历史**交回客户端**（端点据此以 session/update 回放）。下一轮 prompt 是否
// 带这段历史由后端决定：**hub 的 RunPrompt 现在每轮自己 fold 事件流带历史**
// （批次1 P1-3：多轮对话同源，与 fold / resume 同一套机制）——也就是说，模型侧的
// 多轮上下文**不再**依赖本方法把历史交回客户端；本方法的价值是"把历史交回**客户端**"
// （让 UI 能重放/展示），不是"让模型续跑"的唯一途径。
func (b *hubBackend) LoadSession(ctx context.Context, sid contracts.Ulid) ([]core.Message, error) {
	if b.store == nil {
		// 没有回放面（读面没装配）→ 无从载入会话：按"查无此会话"回（不假装成功）。
		return nil, acp.ErrSessionNotFound
	}
	events, err := runtime.ReplaySession(ctx, b.store, sid)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, acp.ErrSessionNotFound
	}
	return runtime.FoldConversation(events), nil
}

// resolveACPCwd 校验客户端给的 cwd 在**服务端**是否可用，返回规范化后的绝对路径。
//
// # 跨机口径（K19，本刀的核心修复）
//
// 客户端与 agent 常常**不在同一台机器**（`ssh host alasol acp`）：客户端给的路径在服务端
// 多半不存在——照搬会让 session 登记在一个不存在的路径上（host.CreateSession 的路径
// 规范化会直接报错），或更糟：建在服务端某个**同名**目录上。故：为空 / 不存在 / 不是目录
// → 回落**服务端进程 cwd**（agent 在服务端自己的仓里干活），并 Warn 一声。
//
// # 诚实边界（根夹紧本刀不做）
//
//   - **没有**允许根限制（没有 "-acp-allow-root" 这类 flag）：客户端给的 cwd 只要在服务端
//     存在就会被采信。
//   - cwd 只是**登记用**（影响 hub 自动 gate 的 repo_id 归集，见 host.SessionSpec）——
//     **不决定工具在哪跑**：工具仍在**进程 cwd** 执行（host 的诚实边界，见其类型注释）。
//     `alasol acp -cwd <dir>` 同理：它只改**自动 gate 的 repo_id 归集**依据，**不改**工具
//     的执行目录（要改执行目录得改进程的工作目录）。
func resolveACPCwd(cwd string) (string, bool) {
	cwd = strings.TrimSpace(cwd)
	if cwd == "" {
		return "", false
	}
	abs, err := filepath.Abs(cwd)
	if err != nil {
		return "", false
	}
	st, err := os.Stat(abs)
	if err != nil || !st.IsDir() {
		return "", false
	}
	return abs, true
}
