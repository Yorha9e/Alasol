package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/compaction"
	"github.com/Yorha9e/alasol/core/discovery"
	coregraph "github.com/Yorha9e/alasol/core/graph"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/core/sessioncfg"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/core/workflow"
	"github.com/Yorha9e/alasol/runtime"
	"github.com/Yorha9e/alasol/runtime/approver/terminal"
	"github.com/Yorha9e/alasol/runtime/cap/local"
	"github.com/Yorha9e/alasol/runtime/cap/remote"
	"github.com/Yorha9e/alasol/runtime/cap/sandbox"
	"github.com/Yorha9e/alasol/runtime/graph"
	"github.com/Yorha9e/alasol/runtime/indexer"
	"github.com/Yorha9e/alasol/runtime/mcp"
	"github.com/Yorha9e/alasol/runtime/provider/openai"
	"github.com/Yorha9e/alasol/runtime/store/blobfs"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	"github.com/Yorha9e/alasol/runtime/store/vecmem"
	"github.com/Yorha9e/alasol/runtime/store/walfile"
	"github.com/Yorha9e/alasol/runtime/telemetry"
	"github.com/Yorha9e/alasol/runtime/tools"
	"github.com/Yorha9e/alasol/runtime/transport"
	"github.com/Yorha9e/alasol/runtime/worktree"
)

// Hub 是装配好的常驻服务：事件流 + 在线表 + 上下文快照 + 发现表（K10）+ Web UI + WS。
//
// 生命周期：New（开库 / 起索引器 / 播种投影与发现表）→ Handler（HTTP 服务）→ Close
// （停索引器 → 关库）。Close 幂等。
//
// 并发：RunPrompt 可被并发调用（不同 session 各一个 Loop，loop 本身无可变状态；
// 同一 session 并发 Run 时事件 seq 是提交序而非因果序——G25 session 亲和写的部署约束）。
type Hub struct {
	cfg Config

	store    *sqlite.Store
	bus      *bus.Bus
	indexer  *indexer.Indexer
	reg      *registry.Registry
	engine   *policy.Engine
	provider core.Provider
	approver core.Approver
	// protocolApprover 是 G1 的协议审批器（ask 流接协议面：approval.request 广播 +
	// approval.respond 注入；无人应答 = deny，fail-close）。New 恒建（廉价：一张挂起表 +
	// 广播口），但**只有** Approval=="protocol" 时它才成为 h.approver（否则 hub 用
	// cfg 指定的 terminal/deny-all/always-allow，不改动既有默认）。HTTP/WS 的审批应答
	// （/api/approvals/{id}/respond、WS approval.respond）恒经它注入。
	protocolApprover *protocolApprover
	proj             *projection
	ws               *transport.Server

	// 发现表（K10）：core 内存活状态 + 拉式扫描喂它的事件流（host/discovery.go）。
	disc     *discovery.Table
	discFeed *discoveryFeed
	// cfgTable / cfgFeed 是 G-c per-prompt 配置的 fold 表与其拉式扫描（host/
	// config_fold.go；core/sessioncfg 纯状态机 + discoveryFeed 同款喂养）：当前配置 =
	// config.changed 折叠（生效链第二层）。启动时全量重放播种（重启后配置仍生效），
	// 每个 prompt 的 Run 钩子先 catchUp 再取 Current(sid)（轮 = Run 开始重取）。
	cfgTable *sessioncfg.Table
	cfgFeed  *configFeed
	// provMu / provCache 是 G-d provider 覆盖的构造缓存（同 (base_url, key) 复用一个
	// openai.Provider）。独立锁（provMu）：钩子在 Run 路径上取它，不与
	// h.mu 的 map 操作互抢。见 host/config_fold.go 的 cachedProviderOverride。
	provMu    sync.Mutex
	provCache map[string]core.Provider
	// onlineConfig / onlineSource 支撑 v1-S4 在线表注入（K14/K15）。onlineConfig 是 hub 共享的
	// 活配置（运行期经 /api/online-table/config 调，所有 loop 下一轮读它立即生效）；onlineSource
	// 把发现表的可见面折成 loop.OnlineTableSource（self + gate 相交的对端主 agent，K11）。
	onlineConfig *loop.OnlineTableConfig
	onlineSource *discoveryOnlineSource
	// link 是 K18 交流能力五件套（core/agentlink）：发现表 + store 读 + bus/store 写
	// 三件事都在 host/agentlink.go 注入。与 disc 同生命周期（无自身状态，无需关闭）。
	link *agentlink.Link
	// monitor 是 K9 hub 级 CPU/内存监控（周期采样 + 超阈值告警）。New 起、Close 停。
	monitor *Monitor
	// worktrees 是 v1-S5 D 多 worktree 隔离管理器（并行任务各占一个工作区）。
	worktrees *worktree.Manager
	// graphEngine 是 v2-S2 图投影的读引擎（G2f：**可选投影**；nil = 未启用）。
	// 未启用时宿主不建它、不驻留内存，索引器也不建图表（零图开销，酸测试）。
	graphEngine *graph.Engine
	// blob 是 v4-3-A 大 payload 外置仓库（G2e/D4；storage.Blob；nil = 未启用 = 零行为变化）。
	// 开了才建（Config.BlobDir 非空）——所有 session 共享一个 blobfs.Store；loop 经它把超阈值的
	// tool.call.completed result 外置（写），读侧经 GET /api/blobs/{id} 取全文。它不是第二状态源：
	// 事件流才是"有没有这条 binary_ref 引用"的唯一事实（core/storage/blob.go）。
	blob storage.Blob
	// vecs / vecIndex 是 v4-3-B 向量引擎（G2d：RAG 检索保留位）的读面/写面
	// （**同一实例**：vecmem.Store 同时实现 storage.Retriever 与 storage.VectorIndex）。
	// nil = 未启用（Config.VectorsEnabled=false）→ 工具不注册、/api/vectors* 端点不注册
	// （404）= 零行为变化（酸测试）。开了才建（内存实现，可整体重建的缓存——丢了重建，
	// 不丢事实 F5；所有 session 共享一个库）。
	vecs     storage.Retriever
	vecIndex storage.VectorIndex
	// docs 是文档族（G2 第 5 族）的存储槽位（**接口面** docStore，见 host/docs.go；
	// 当前实现 = runtime/store/sqlite.DocStore；nil = 未启用 = 零行为变化酸测试）。
	// 开了才建（Config.DocumentsEnabled=true）——所有 session 共享**一个** DocStore
	// （事件库同库单表，EnsureSchema 建表）。
	// 读面供 /api/docs 查询；写只经宿主 ingest 入口（/api/docs/upsert、
	// /api/docs/delete，经 Transact 收口事务——G17 单写者，与 IngestVectors 同口径）。
	//
	// **槽位是接口不是具体类型**（形态照 blob 的 storage.Blob）：换实现**宿主零改动**
	// （方法集全在实现侧，零适配层）。
	docs docStore
	// wal 是 v4-4-B WAL 预写日志的目录管理器（runtime/store/walfile；nil = 未启用 =
	// 零行为变化酸测试）。开了才建（Config.WalDir 非空）——per-session Writer 由它懒建，
	// loop 经 walFor 拿到 sink（turn.begin / turn.delta / turn.end / turn.failed /
	// turn.retry / blob.plan 六个写点）。
	// **WAL 不是事实源**（F5 不受影响）：可丢弃/可截断/可 GC，恢复报告是观察面不是写回面
	//（见 host/wal.go）。
	wal *walfile.Store
	// tel 是 L2/D19 telemetry 的 file 落盘 sink（runtime/telemetry；nil = 未启用 =
	// 零行为变化酸测试）。开了才建（Config.Telemetry=file）——hub 级**一个共享** sink
	//（单文件 NDJSON，record 自带 session_id），所有 loop/fork 经 telemetrySink() 拿到
	// 同一个；Close 时收口。**独立管道**：不参与任何判定（见 host/telemetry.go）。
	tel *telemetry.File
	// embed 是**文本 → 向量**的宿主槽位（框架不算 embedding；生产 nil = 未接）。
	// nil 时：vector_search 工具不注册、检索/ingest 端点回 503 说明（见 host/vectors.go）。
	embed EmbeddingProvider
	// vecGate 是向量面的**重建闸**（复审轮 C3）：RWMutex + 在途重建计数。
	// Clear+Upsert 的重建窗口跨一把写锁；重建在途时检索回"重建中"（不返回空库）。
	// nil = 无闸（零值可用；Hub 直构的测试路径不碰向量面）。
	vecGate *vectorGate
	// sandbox 是 v2-S4 沙箱执行环境（K8：可插拔 ExecutionEnv；nil = 未启用，
	// 走 local）。启用时 Close 会撤掉 WER 排除项并收口残留 job 句柄。
	sandbox *sandbox.Env
	// mcpManager 是 v3-S1 的 MCP client 管理器（N1：client 先；nil = 未装配任何
	// MCP server）。工具在装配期就进了 reg，loop 眼里它们与 read/write/bash 无异；
	// hub 保留引用只为 Close 时收尸——stdio server 由本进程拉起，杀树归本进程
	// （陷阱 5：谁拉起谁收尸）。
	mcpManager *mcp.Manager
	// workflows 是 v3-S3 工作流能力包注册表（W1：内建 tower + Config.WorkflowDir 目录加载）。
	workflows *workflow.Registry
	// agentTypes 是 v3-S6-B 的 agent 类型注册表（W5：内建五类 + TOML 自定义，同名覆盖内建）。
	// **只在 MultiAgent=true 时建**（host.New；关 = 不建不校验 = 零行为变化，见 New 的装配处）。
	// 开了时坏配置启动即拒并点名。agentTypeIDs 是已注册类型 id 的有序列表（root 类型的
	// CanSpawn 用它 = 全部已注册类型）。MultiAgent=false 时两者都是 nil/空。
	agentTypes   *agents.TypeRegistry
	agentTypeIDs []string
	// cwd 是 hub 级默认工作目录（Config.CWD，缺省进程 cwd）：自动 gate repo:<repo_id>
	// 的输入。per-session 覆盖见 specs。
	cwd string
	// hubID 是本 hub 的**进程标识**（G25 跨进程托管注册，2026-11-01 v4-7）：New 时生成的
	// ULID。session 创建/接管（loopFor 建成 loop）时经 discovery.Table.Claim 登记到发现表
	// ——"这个 session 的事件只由宿主进程 append"从部署层约束变成**可查的登记**；
	// 其他 hub / 诊断面经 Table.HostOf 查"该 session 谁托管"，冲突 fail-close（拒绝接管）。
	// 进程本地（发现表 live 态同款）：重启后失活，新进程重新 Claim。
	hubID string

	// ixCtx 是后台索引器的生命周期（Close 时取消）。
	ixCtx    context.Context
	ixCancel context.CancelFunc
	ixDone   chan struct{}

	// monCtx 是后台监控器的生命周期（Close 时取消）。
	monCtx    context.Context
	monCancel context.CancelFunc

	// lifeCtx 是 **hub 的生命周期**（Close 时取消）：一切"跑在后台、可能活到关库之后"
	// 的活都从它派生 ctx——协议面的异步 prompt（runPromptAsync）就是其中之一。
	//
	// 为什么要有它（v3-S4 质量债）：协议 prompt 的 ctx 原本是 context.Background()
	// → hub 关掉后它还在跑（最长一轮 600s），往**已关闭**的事件库写事件
	// （"database is closed" 噪声 + 徒劳的 IO）。派生自 lifeCtx → Close 一取消，
	// 那一轮立刻收场（fail-close：取消即 deny），关库前也能等到它真正退出。
	lifeCtx    context.Context
	lifeCancel context.CancelFunc

	// promptWG 统计在跑的协议面异步 prompt（Close 时等它们收场，见 Close 的说明）。
	// Add 与 Wait 都受 h.mu 保护（见 startPromptAsync）：Close 之后不再接受新 prompt，
	// 否则 Add 与 Wait 撞车（WaitGroup 误用）。
	promptWG sync.WaitGroup

	mu sync.Mutex
	// loops：session → Loop（一个 session 一个 loop）。
	loops map[contracts.Ulid]*loop.Loop
	// agentTrees：session → agent 树（v3-S6-B，MultiAgent 开才建；与 loops 同生命周期）。
	// 树是 session 的**活状态**：workflow 激活失效 loop 时树**保留**（只 delete loops，不 delete
	// agentTrees——否则续跑/重新装配 loop 会丢掉已派生的子 agent）。仅 h.mu 下读写。
	agentTrees map[contracts.Ulid]*agents.Tree
	// specs：session → 归属（CreateSession 登记；自动 gate 的输入）。
	specs map[contracts.Ulid]SessionSpec
	// mainAgents：session → 主 agent id（loop 建成时登记；gate.joined 的 envelope 要用）。
	mainAgents map[contracts.Ulid]contracts.Ulid
	// autoGates：session → 自动 gate 是否已加入（**成功才记**，失败下次 Run 重试——
	// F2：重试是真的，loopFor 的**缓存命中路径**会补一次自愈 join，见 joinAutoGatesIfNeeded）。
	autoGates map[contracts.Ulid]bool
	// activeWorkflow：session → 激活的工作流能力包（W12：归属 session，不是 core 全局状态；
	// v3-S3）。激活只把哲学文本拼进 system prompt（W3），不挂 = 该 session 无键 = 零行为变化。
	activeWorkflow map[contracts.Ulid]*workflow.Package
	// validationCursors：session → 验证信号游标（v3-S3 修：跨 Run 不丢验证信号）。
	// tiered 引擎是 per-Run 的（每次 Run 造一个 fresh 引擎），游标若只活在引擎里，
	// 落在两次 Run 之间的验证信号（workflow.milestone passed=true 等）永远看不见。
	// 这里按 session 持久一份，跨 Run 接力（见 host/compaction.go 的装配）。
	// 独立锁（validationMu）：装配路径可能在持有 h.mu 时调用它——不另设锁会自锁。
	validationMu      sync.Mutex
	validationCursors map[contracts.Ulid]*compaction.ValidationCursor
	// mergeCache：session → review 结论缓存（V6 合并门禁读面；p3 修：全量读事件流是 O(n)，
	// 缓存靠"探测读"**精确**失效——见 host/mergegate.go 的 reviewsFor）。
	mergeCache map[contracts.Ulid]*mergeReviewCache
	// joinLocks：session → gate 成员关系写锁（per-session；join/leave 共用，见 joinMutex）。
	joinLocks map[contracts.Ulid]*sync.Mutex
	// injectWarned：注入区被截断的告警**只报一次**（工厂是 per-Run 的，不去重就是每轮一条）。
	injectWarned bool
	// btwPromote 是"从 /btw 生成全新 session"的预埋钩子（K29：只埋点不实现；nil = 不接）。
	// 经 SetBtwPromoteHook 设置、RunBtw 完成后锁外触发（见 host/fork.go）。
	btwPromote BtwPromoteFunc
	closed     bool
}

// New 装配一个 Hub。
//
// 顺序：开事件库 → 建索引器（确保投影表）→ bus → 策略/注册表/工具 → provider/approver
// → 传输服务端 → 播种内存投影与发现表 → 起后台索引器。
func New(cfg Config) (*Hub, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	st, err := sqlite.Open(cfg.DBPath, sqlite.Options{})
	if err != nil {
		return nil, fmt.Errorf("host: 打开事件库: %w", err)
	}

	// hub 生命周期 ctx（Close 时取消）：协议面的异步 prompt 从它派生 → 关库前
	// 那一轮能收场（不往已关的库写事件）。装配失败路径也要取消（无后台活，但别留尾巴）。
	lifeCtx, lifeCancel := context.WithCancel(context.Background())

	// 装配失败时的**统一收口**（v3-S1）：
	//
	// 装配期已经拉起的东西——沙箱 env（WER 排除项是持久的用户级注册表写入，
	// 没人 Close 就留在用户机器上，F8）、MCP server 子进程（stdio 由本进程
	// exec，没人收尸就是孤儿进程，陷阱 5）、事件库——必须在**任何**失败路径上
	// 全部收掉。
	//
	// 为什么用 defer 兜底而不是逐条路径手写：漏一条就是一次泄漏。本次修的
	// 正是"assembleMCP 之后失败只关事件库"那条（stdio MCP server 变成孤儿），
	// 以及它的邻居"沙箱 env 在工具注册失败时没人 Close"。
	//
	// （早期失败路径里那些 `_ = st.Close()` 留着无害——sqlite.Store.Close 是
	// 幂等的；本 defer 是**唯一**保证 MCP / 沙箱被收的地方。）
	var (
		sbEnv     *sandbox.Env
		mcpMgr    *mcp.Manager
		assembled bool
	)
	defer func() {
		if assembled {
			return // 成功装配：资源归 Hub 所有（Close 时收）
		}
		lifeCancel() // 生命周期 ctx 收口（还没起后台活，但别留尾巴）
		if mcpMgr != nil {
			_ = mcpMgr.Close() // 收尸 MCP server 进程树（谁拉起谁收尸）
		}
		if sbEnv != nil {
			_ = sbEnv.Close() // 撤掉 WER 排除项（F8）
		}
		_ = st.Close()
	}()

	// 图投影（v2-S2，G2f 可选投影）：**默认不启用**——不建引擎、不驻留内存。
	// 启用时先建读引擎（它只被查询时加载），再把它接成索引器写面的失效通知目标
	// （G20：索引器提交写了边的批次后 → 引擎失效闭包缓存）。
	var graphEngine *graph.Engine
	var graphProj coregraph.GraphProjection
	if cfg.Graph {
		eng, gerr := sqlite.NewGraphEngine(st, cfg.graphLimits())
		if gerr != nil {
			_ = st.Close()
			return nil, fmt.Errorf("host: 建图查询引擎: %w", gerr)
		}
		graphEngine = eng
		// 写面**自建表**（F6：DDL 归实现侧，不塞进索引器的事务）→ 它需要自己的
		// 执行句柄（由存储实现给，外部拿不到裸 *sql.DB，G17 同款纪律）。
		proj, gerr := sqlite.NewGraphProjection(st, eng)
		if gerr != nil {
			_ = st.Close()
			return nil, fmt.Errorf("host: 建图投影写面: %w", gerr)
		}
		graphProj = proj
	}

	ix, err := sqlite.NewIndexer(st, cfg.IndexerBatch, indexer.WithGraphProjection(graphProj))
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("host: 建索引器: %w", err)
	}

	// 策略：Allow 生成 source=cli 的 allow 规则；Ask 生成 ask 规则（命中即去问人）。
	// 空规则集 = 一切 deny（fail-close）。
	var rules []contracts.PolicyRule
	for _, name := range cfg.Allow {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		rules = append(rules, contracts.PolicyRule{
			Effect: contracts.EffectAllow,
			Tool:   name,
			Source: contracts.RuleSourceCli,
		})
	}
	for _, name := range cfg.Ask {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		rules = append(rules, contracts.PolicyRule{
			Effect: contracts.EffectAsk,
			Tool:   name,
			Source: contracts.RuleSourceCli,
		})
	}
	// v3-S6-B：开 MultiAgent 时自动补 spawn/complete 的 allow 规则（source=cli/auto_multi_agent，
	// 未知来源 = 优先级最低）。为什么必须补：spawn/complete 是**框架内部工具**，用户 Allow 列表
	// 里通常没有它们 → 策略 deny → 模型不可见（toolVisible）→ 主 agent 根本派不出子 agent。
	// 开了 multi_agent = **预期主 agent 能 spawn**，故 hub 在此补上。策略链照走（P2-4）：
	// 未知来源 allow 优先级最低，**任何来源的 deny 仍可拦**（deny 永远赢）——用户/项目/工作流的
	// 显式 deny 不受自动 allow 影响。默认关（MultiAgent=false）= 不补 = 单 agent 模式零行为变化。
	if cfg.MultiAgent {
		for _, name := range []string{loop.ToolSpawn, loop.ToolComplete} {
			rules = append(rules, contracts.PolicyRule{
				Effect: contracts.EffectAllow,
				Tool:   name,
				Source: contracts.RuleSource(multiAgentRuleSource),
			})
		}
	}
	eng := policy.New(rules...)

	reg := registry.New(registry.NewPolicyAdjudicator(eng))
	// 执行环境（K8：可插拔 ExecutionEnv）。**默认 local**（v1 等价，零沙箱开销）；
	// Config.Sandbox=true 时换成 sandbox.Env；Config.Remote=true 时换成 remote.Env
	// （执行面经系统 ssh）——三者互斥（validate 已拒双开），工具代码、loop、策略链一行不动。
	fsCap, execCap := core.FSCapability(local.FS{}), core.ExecCapability(local.Exec{})
	switch {
	case cfg.Sandbox:
		env, serr := sandbox.New(cfg.sandboxPolicy())
		if serr != nil {
			_ = st.Close()
			return nil, fmt.Errorf("host: 装配 SandboxedExecutionEnv: %w", serr)
		}
		sbEnv = env
		fsCap, execCap = env, env
		for _, d := range env.Diagnostics() {
			lvl := slog.LevelInfo
			if !d.OK {
				lvl = slog.LevelWarn
			}
			hubLog.Log(context.Background(), lvl, "host: 沙箱埋点",
				"kind", d.Kind, "ok", d.OK, "detail", d.Detail)
		}
		hubLog.Info("host: 已启用 SandboxedExecutionEnv",
			"backend", env.BackendName(), "allowed_roots", env.AllowedRoots())
	case cfg.Remote:
		// RemoteExecutionEnv（v4-2-A，K8 第三档）：**Exec + Fs** 经 SSH 落地（Fs 是
		// 最小可用档：cat/stat/ls）；Proc/Net 显式 ErrUnsupported（预留位）——所以
		// read/write 也接到 remote env，会经 ssh 通道读写**远端**文件（换 env 不动工具
		// 代码；要挂 Proc/Net 在 remote 包里实现即可）。鉴权交系统 ssh，此处不传任何
		// 凭据（H18）。
		env, serr := remote.New(remote.Config{Host: cfg.RemoteHost})
		if serr != nil {
			_ = st.Close()
			return nil, fmt.Errorf("host: 装配 RemoteExecutionEnv: %w", serr)
		}
		fsCap, execCap = env, env
		hubLog.Info("host: 已启用 RemoteExecutionEnv（SSH）",
			"host", env.Host(), "note", "Exec+Fs 经 SSH；Proc/Net 预留 ErrUnsupported")
	}
	execs := map[string]core.ToolExecutor{
		"read":  tools.NewRead(fsCap),
		"write": tools.NewWrite(fsCap),
		"bash":  tools.NewBash(execCap),
	}
	for _, d := range tools.Descriptors() {
		ex, ok := execs[d.Name]
		if !ok {
			_ = st.Close()
			return nil, fmt.Errorf("host: 工具 %q 无执行器（装配残缺）", d.Name)
		}
		if err := reg.RegisterTool(d, ex); err != nil {
			_ = st.Close()
			return nil, fmt.Errorf("host: 注册工具 %s: %w", d.Name, err)
		}
	}

	// git_status（J3）：经**同一个** registry.RegisterTool 入口注册（与 read/write/bash
	// 同一条 X1 三段写路径、同一套策略链与 R5 可见性）。它**不进 Descriptors()**（那三个
	// 是 v0 起就有的内置核心三件，git_status 是后加的只读观测件）——单独注册，形态与
	// vector_search 那件 opt-in 工具同款。**不进 prompt**：只经注册表暴露给模型按需调用，
	// 不向 system prompt / 任何注入文本里加一个字。生产默认仍 deny-all（要放行显式
	// allow = ["git_status"]）。
	if err := reg.RegisterTool(tools.GitStatusDescriptor(), tools.NewGitStatus()); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("host: 注册工具 %s: %w", "git_status", err)
	}

	// MCP client（v3-S1，N1）：外部 server 的工具注册进**同一个** reg——与内置工具
	// 同一个入口、同一条 X1 三段写路径、同一套策略链与可见性（R5）。
	//
	// 顺序在策略引擎建好之后：装配期不裁决策略（策略裁决发生在 loop 的每次调用），
	// 但要把"哪些工具该出现"这件事在 hub 起来时就定下来。
	//
	// **内置工具先注册**：这样 MCP 工具若与内置撞名，撞的是"已注册的内置条目"——
	// fail-close 的拒绝信息能点名是哪一个（runtime/mcp 的注册前检查）。
	// 结构上前缀已经让撞名不可能（mcp__<server>__<tool>），这里是纵深防御。
	var merr error
	mcpMgr, merr = assembleMCP(context.Background(), cfg, reg)
	if merr != nil {
		_ = st.Close()
		return nil, merr
	}

	prov := cfg.Provider
	if prov == nil {
		// 真 provider：凭据按 H15 收敛（env > 文件）后由 cmd 填进 cfg.APIKey；
		// 直连 host.New 的调用方（测试）只设 APIKeyEnv 时，回退从 env 取。
		// 凭据永不进 argv / 事件 / 日志（H12/H19）。
		keyEnv := strings.TrimSpace(cfg.APIKeyEnv)
		if keyEnv == "" {
			keyEnv = DefaultAPIKeyEnv
		}
		apiKey := cfg.APIKey
		if apiKey == "" {
			apiKey = os.Getenv(keyEnv)
		}
		prov = openai.New(openai.Options{
			BaseURL: strings.TrimSpace(cfg.BaseURL),
			APIKey:  apiKey,
			Stream:  true,
		})
	}
	ap := cfg.Approver
	if ap == nil {
		switch cfg.Approval {
		case "always-allow":
			ap = core.AlwaysAllow{}
		case "terminal":
			ap = terminal.New(os.Stdin, os.Stdout)
		case "protocol":
			ap = nil // 协议审批器在下面装配（需要 broadcaster = transport server，见 h 建成后）
		default: // deny-all（默认，fail-close）
			ap = core.DenyAll{}
		}
	}

	b := bus.New(st)
	disc := discovery.New()
	discFeed := newDiscoveryFeed(st, disc)
	// G-c per-prompt 配置 fold（core/sessioncfg 纯状态机）+ 拉式扫描喂养（照
	// discoveryFeed 样板）：启动播种与每次 Run 的钩子都经 cfgFeed.catchUp 追平。
	cfgTable := sessioncfg.New()
	cfgFeed := newConfigFeed(st, cfgTable)
	// 在线表配置（K14/K15）：默认值（N=50 / 每行 ≤100 token / summary）落进活配置对象，
	// Config 里的覆盖项经 Update 生效（0/"" = 保持默认）。数据源 = 发现表可见面。
	onlineCfg := loop.NewOnlineTableConfig()
	if err := onlineCfg.Update(cfg.onlineTableOverrides()); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("host: 在线表配置: %w", err)
	}
	cwd := strings.TrimSpace(cfg.CWD)
	if cwd == "" {
		// 缺省用进程 cwd（alasol serve 在哪个目录起，就默认那是哪个 repo）。
		if wd, werr := os.Getwd(); werr == nil {
			cwd = wd
		} else {
			hubLog.Warn("host: 取进程工作目录失败，hub 级默认 repo gate 不可用", "err", werr.Error())
		}
	}
	// K9 告警事件流（v2-S5 清账②）：hub 级事件的伪归属按**规范化后**的库路径确定性派生
	// （跨重启稳定；不是 agent session，不进在线表——在线表按 hub.* 事件族**整族**过滤，
	// 见 host/projection.go 的 rows：派生口径变了也照样挡得住）。经 **bus** 发布（G4 修）：
	// 入库与 WS 广播同源一致，/ws 订阅者才看得到 hub.alert（旧实现直调 store.Append 落了库
	// 却绕过 bus → 订阅者永远收不到）。
	alertSink := newHubAlertSink(b, cfg.DBPath)
	proj := newProjection(st)

	// 工作流能力包注册表（v3-S3，W1）：内建 tower + Config.WorkflowDir 目录加载。
	// 装配期就建（坏能力包 / 重名 → 启动即拒，不拖到激活期）。失败由顶部 defer 收口
	// （关事件库 / 沙箱 / MCP——此时 h 还没建，直接返回即可）。
	workflows, wferr := buildWorkflowRegistry(cfg.WorkflowDir)
	if wferr != nil {
		return nil, fmt.Errorf("host: 装配工作流注册表: %w", wferr)
	}

	// agent 类型注册表（v3-S6-B，W5）：内建五类 + TOML 自定义（同名覆盖内建）。
	//
	// **只在 MultiAgent=true 时建**（F2 修）：MultiAgent=false = 单 agent 模式**零行为变化**
	// ——不建注册表、**也不校验**（关 = 不建不校验：坏配置不许拒绝一个根本用不到它的宿主
	// 启动）。开了才 fail-close：坏配置启动即拒并点名（与工作流注册表同一纪律）。
	// 返回的 agentTypeIDs 是已注册类型 id 的有序列表（root 类型的 CanSpawn = 全部已注册类型）。
	var (
		agentTypes   *agents.TypeRegistry
		agentTypeIDs []string
	)
	if cfg.MultiAgent {
		reg, ids, aterr := buildAgentTypeRegistry(cfg.Model, cfg.AgentTypes, cfg.ModelRouter)
		if aterr != nil {
			return nil, fmt.Errorf("host: 装配 agent 类型注册表: %w", aterr)
		}
		agentTypes, agentTypeIDs = reg, ids
	}

	// 大 payload 外置 blob（v4-3-A，G2e/D4）：**默认不启用**（Config.BlobDir 空 = 不建存储、
	// loop 不外置、/api/blobs 恒 404 = 零行为变化，酸测试）。配了目录才建一个所有 session
	// 共享的 blobfs.Store；阈值 BlobMaxInline>0 时 loop 才会把超阈值的 completed result 外置
	// （X1 三段结构不动，只改 completed 的 result 内容 + binary_ref）。blob 不是第二状态源
	// （事件流才是引用的事实源）——建失败只是"显式要的能力没起来"，拒绝启动比静默不外置好排查。
	var blobStore storage.Blob
	if dir := strings.TrimSpace(cfg.BlobDir); dir != "" {
		bs, berr := blobfs.New(dir)
		if berr != nil {
			return nil, fmt.Errorf("host: 建 blob 外置存储 %q: %w", dir, berr)
		}
		blobStore = bs
		hubLog.Info("host: 已启用大 payload 外置 blob", "dir", dir, "max_inline_bytes", cfg.BlobMaxInline)
	}

	// 向量引擎（v4-3-B，G2d）：**默认不启用**（Config.VectorsEnabled=false = 不建库、
	// vector_search 工具不注册、/api/vectors* 两个端点不注册（404）= 零行为变化，酸测试）。
	// 开了才建一个所有 session 共享的**内存**向量库（runtime/store/vecmem）：它是
	// **可整体重建的缓存**（SourceRef 指回事件流 e:<seq>/路径/blob id，丢了重建不丢事实，F5），
	// 所以不做持久化；换持久化实现消费方零改动（G2b）。同一实例同时提供读面与写面
	// （vecmem 两面都实现，G17 单写者纪律：写只经宿主的 ingest/Rebuild 入口）。
	var (
		vecStore  *vecmem.Store
		vecReader storage.Retriever
		vecWriter storage.VectorIndex
	)
	if cfg.VectorsEnabled {
		vecStore = vecmem.New(cfg.VectorsTopK)
		vecReader, vecWriter = vecStore, vecStore
		hubLog.Info("host: 已启用向量引擎（内存实现，可整体重建）", "default_top_k", vecStore.DefaultK())
	}
	// 重建闸（复审轮 C3）：重建（Clear+Upsert）跨一把写锁，重建在途时检索回"重建中"。
	// 恒建（极廉价：一把 mutex + 一个计数器）——向量面没开时没人用它。
	vecGate := &vectorGate{}

	// 文档族存储（G2 第 5 族）：**默认不启用**（Config.DocumentsEnabled=false = 不建
	// DocStore、/api/docs* 四个端点不注册（404）= 零行为变化，酸测试）。开了才建一个
	// 所有 session 共享的 DocStore（runtime/store/sqlite：事件库同库单表 docs，
	// (collection,key) 主键寻址），并 EnsureSchema 建表（F6：DDL 归实现侧）。
	// EnsureSchema 失败 → 拒绝启动（与 blobfs 同口径：显式要的能力没起来，比静默不建好排查）。
	// 写只经宿主 ingest 入口（G17 单写者：与 IngestVectors 同口径）；文档是投影，
	// 事件流才是事实源（F5）。
	//
	// 槽位类型是**接口**（docStore，见 host/docs.go）：当前实现 = sqlite.DocStore，
	// 换实现宿主零改动。
	var docsStore docStore
	if cfg.DocumentsEnabled {
		ds, derr := sqlite.NewDocStore(st)
		if derr != nil {
			return nil, fmt.Errorf("host: 建文档族存储: %w", derr)
		}
		if derr := ds.EnsureSchema(context.Background()); derr != nil {
			return nil, fmt.Errorf("host: 建文档表: %w", derr)
		}
		docsStore = ds
		hubLog.Info("host: 已启用文档族存储（SQLite 单表投影）", "table", "docs")
	}

	// WAL 预写日志（v4-4-B）：**默认不启用**（Config.WalDir 空 = 不建目录、不建任何文件、
	// loop.Config.WalSink 恒 nil = 一个字节都不多写 = 零行为变化，酸测试）。配了目录才建
	// 目录管理器；per-session Writer 由它懒建（loopFor → walFor），Close 时统一收口。
	//
	// **WAL 不是事实源**（F5 不受影响）：事件流才是唯一事实源，WAL 记录"正式写之前"的
	// 中间态（崩溃后至少知道"当时在干什么、吐过什么"），可丢弃/可截断/可 GC，恢复报告是
	// 观察面不是写回面（见 host/wal.go 与 runtime/store/walfile 的包 doc）。
	//
	// 建失败（目录建不出来/是文件）→ 拒绝启动：那是"显式要的能力没起来"，比静默不外记
	// 好排查（与 blobfs 同口径）。
	var walStore *walfile.Store
	if dir := walDirOf(cfg.WalDir); dir != "" {
		ws, werr := walfile.New(dir)
		if werr != nil {
			return nil, fmt.Errorf("host: 建 WAL 预写日志目录 %q: %w", dir, werr)
		}
		walStore = ws
		hubLog.Info("host: 已启用 WAL 预写日志（崩溃恢复补充面，非事实源）", "dir", dir)
	}

	// telemetry（L2/D19）：**默认不启用**（mode 空/none = nil Sink = 一条 record 都不造
	// = 零行为变化，酸测试）。mode=file 才建共享 file sink；mode=otlp 留位在装配期拒。
	// **独立管道**：不参与任何判定、不是事实源；emit 前 core.Redact；失败只 Warn 不阻断
	//（见 host/telemetry.go 与 core/loop/telemetry.go）。
	//
	// 建失败 → 拒绝启动：那是"显式要的能力没起来"，比静默不送好排查（与 blobfs 同口径）。
	telFile, terr := newTelemetrySink(cfg.Telemetry, cfg.TelemetryDir)
	if terr != nil {
		return nil, terr
	}
	if telFile != nil {
		hubLog.Info("host: 已启用 telemetry file 落盘（独立观测管道，非事实源）",
			"dir", telemetryDirOf(cfg.TelemetryDir), "path", telFile.Path())
	}

	// vector_search 工具（RAG 检索）：**只有"向量开着 + 配了 embedding 提供者"才注册**。
	//
	// 为什么没配提供者就不注册（而不是注册一个必然报错的工具）：模型给的是 query **文本**，
	// 变不出向量（框架不算 embedding——那是 provider/模型层的事）→ 注册了也只是让模型
	// 看见一个"调了就说用不了"的工具。不注册 = 可见集里没有它（R5：不暴露即不存在），
	// 与"未开向量"同为**零行为变化**（酸测试：注入 fake 提供者才出现）。
	// 策略链照走（deny-all 默认）：要放行得显式 allow = ["vector_search"]。
	switch {
	case vecStore != nil && cfg.EmbeddingProvider != nil:
		if err := reg.RegisterTool(vectorSearchDescriptor(),
			newVectorSearchExecutor(cfg.EmbeddingProvider, vecStore, vecGate)); err != nil {
			return nil, fmt.Errorf("host: 注册向量检索工具 %s: %w", ToolVectorSearch, err)
		}
	case cfg.VectorsEnabled && cfg.EmbeddingProvider == nil:
		hubLog.Warn("host: 向量引擎已启用但未配置 embedding 提供者——vector_search 工具不注册，" +
			"检索/ingest 端点将回 503（框架不算 embedding：文本转向量由宿主/provider 层接）")
	}

	h := &Hub{
		cfg:               cfg,
		store:             st,
		bus:               b,
		indexer:           ix,
		reg:               reg,
		engine:            eng,
		provider:          prov,
		approver:          ap,
		proj:              proj,
		disc:              disc,
		discFeed:          discFeed,
		cfgTable:          cfgTable,
		cfgFeed:           cfgFeed,
		cwd:               cwd,
		hubID:             string(core.NewUlid()),
		ws:                transport.NewServer(b),
		loops:             make(map[contracts.Ulid]*loop.Loop),
		specs:             make(map[contracts.Ulid]SessionSpec),
		mainAgents:        make(map[contracts.Ulid]contracts.Ulid),
		autoGates:         make(map[contracts.Ulid]bool),
		activeWorkflow:    make(map[contracts.Ulid]*workflow.Package),
		validationCursors: make(map[contracts.Ulid]*compaction.ValidationCursor),
		onlineConfig:      onlineCfg,
		onlineSource:      &discoveryOnlineSource{table: disc, feed: discFeed},
		graphEngine:       graphEngine,
		sandbox:           sbEnv,
		mcpManager:        mcpMgr,
		workflows:         workflows,
		agentTypes:        agentTypes,
		agentTypeIDs:      agentTypeIDs,
		agentTrees:        make(map[contracts.Ulid]*agents.Tree),
		lifeCtx:           lifeCtx,
		lifeCancel:        lifeCancel,
		blob:              blobStore,
		vecs:              vecReader,
		vecIndex:          vecWriter,
		docs:              docsStore,
		embed:             cfg.EmbeddingProvider,
		vecGate:           vecGate,
		wal:               walStore,
		tel:               telFile,
	}
	// G-a 活跃窗口单源同步（构造路径）：onlineTableConfig 的 activity_window 是唯一旋钮，
	// 发现表的活跃判定（K11 peers / agentlink presence / GET /api/sessions）必须拿到同一个
	// N——不同步的话，起动配置只影响在线表注入（K14/K15）一个面。运行期改配置走
	// UpdateOnlineTable → 同一个 syncActivityWindow（两处一个方法，不给漂移留缝）。
	h.syncActivityWindow()

	// G1 协议审批器：恒建（廉价），Approval=="protocol" 时才成为 h.approver（否则保持
	// cfg 指定的 terminal/deny-all/always-allow，不改既有默认）。HTTP/WS 的审批应答
	// 恒经它注入挂起的审批。同时把 hub 接成 transport 的入站处理器——WS 面上的
	// command.submit（对话发起）/ approval.respond（审批应答）由此进宿主（协议面遥控）。
	h.protocolApprover = newProtocolApprover(h.ws)
	if cfg.Approver == nil && strings.TrimSpace(cfg.Approval) == "protocol" {
		h.approver = h.protocolApprover
	}
	h.ws.SetInboundHandler(h)
	// G11：新 WS 连接补发仍挂起的 approval.request（onConnect 钩子 → ResendPending）。
	// Broadcast 只覆盖"发出那一刻在线的连接"——刷新/重连的新连接靠这里补齐。补发是
	// at-least-once：锁内快照、锁外投递，投递窗口内被应答的仍可能送达——客户端按
	// request_id 去重/幂等呈现；重复应答服务端回 404。重复订阅 = 替换，口径不变。
	h.ws.SetOnConnect(h.protocolApprover.ResendPending)

	// N2 砖化恢复（v3-S3 修）：崩溃遗留的图投影重建窗口（graph_meta.rebuilding=1）会让
	// 读侧引擎**永久** fail-close（影响面/冲突预测一直不可用），而 Rebuild 当时没有生产
	// 调用方 → 没人恢复。启动期检测 + 自动重跑一次重建（中断/恢复都记 hub.alert 事件流）。
	// 放在后台索引器起来**之前**：此刻没有并发 Step，重建是独占的。
	h.recoverGraphRebuild(context.Background())

	// 播种内存投影：把库里已有的事件扫进来（重开进程后在线表立刻有内容）。
	if _, err := h.proj.catchUp(context.Background()); err != nil {
		_ = h.Close()
		assembled = true // h.Close 已收口（沙箱 / MCP / 事件库）
		return nil, fmt.Errorf("host: 播种投影失败: %w", err)
	}
	// 播种发现表：join 历史 / agent 树投影从事件流重建（K10/G17b）。**老 session 不会
	// 因此变回存活**——活状态是进程本地的，重启后全部失活（这是设计，不是遗漏）。
	if _, err := h.discFeed.catchUp(context.Background()); err != nil {
		_ = h.Close()
		assembled = true
		return nil, fmt.Errorf("host: 播种发现表失败: %w", err)
	}
	// 播种配置 fold（G-c）：config.changed 全量重放 → **重启后 per-prompt 配置仍生效**
	//（设计 15 §3.1）。失败 **fail-close**（与播种发现表同口径）：当前配置折不出来，
	// 就别拿一个错的/缺的配置去跑模型（可能把请求打到错的端点）。
	if _, err := h.cfgFeed.catchUp(context.Background()); err != nil {
		_ = h.Close()
		assembled = true
		return nil, fmt.Errorf("host: 播种配置 fold 失败: %w", err)
	}

	// agentlink（K18）：reader 直接就是 store（core.Store.ReadFrom 与
	// agentlink.EventWindowReader 同签名——不加适配层）；sink 经 bus 发布 + 回读拿 seq。
	h.link = agentlink.New(agentlink.Deps{
		Table:  disc,
		Reader: st,
		Sink:   agentlinkSink{bus: b, store: st},
	})

	// standby 待机工具（G-f）：**hub 级常驻**（形态同 vector_search / git_status：
	// 经**同一个** reg.RegisterTool 入口注册 → 同一条 X1 三段写路径、同一套策略链
	// 与 R5 可见性；生产默认仍是 deny-all，要放行得显式 allow = ["standby"]）。
	//
	// 装配期注入四样宿主能力：
	//   - 事件流读面（回查本次 tool.call.intent：拿归属 session 与重放游标）；
	//   - agentlink（K11 addressedTo 唤醒过滤：只认投递给本 session 的消息）；
	//   - 发现表（waiting 态的 BeginWait/EndWait 配平）；
	//   - hub 生命周期（关库即中止待机，不挂死）。
	//
	// 放在 h.link 建好之后：唤醒判定要经 agentlink 读收件箱。
	if err := reg.RegisterTool(standbyDescriptor(), newStandbyExecutor(standbyDeps{
		store:   st,
		link:    h.link,
		disc:    disc,
		catchUp: discFeed.catchUp,
		hubDone: lifeCtx.Done(),
		blob:    h.blob, // 交付标记被外置时经它读回（critic P2）
	})); err != nil {
		_ = h.Close()
		assembled = true
		return nil, fmt.Errorf("host: 注册待机工具 %s: %w", ToolStandby, err)
	}

	// 后台索引器：派生表（sessions / ledger / board / cost_stats / cost_tokens）的**唯一写者**。
	h.ixCtx, h.ixCancel = context.WithCancel(context.Background())
	h.ixDone = make(chan struct{})
	go func() {
		defer close(h.ixDone)
		_ = ix.Run(h.ixCtx) // ctx 取消时返回 ctx.Err()，不是故障
	}()

	// 后台监控（K9）：hub 级 CPU/内存周期采样 + 超阈值告警。用独立 ctx（Close 时取消）。
	// 告警走三个出口：slog + 内存环/api/monitor + **事件流**（v2-S5 清账②）。
	h.monitor = NewMonitor(monitorConfig{
		CPUPercent:  cfg.MonitorCPUPercent,
		MemMB:       cfg.MonitorMemMB,
		Goroutines:  cfg.MonitorGoroutines,
		IntervalSec: cfg.MonitorIntervalSec,
	}, cfg.MonitorSampleSource, WithAlertSink(alertSink)) // nil src = 真实采样
	h.monCtx, h.monCancel = context.WithCancel(context.Background())
	h.monitor.Start(h.monCtx)

	// 多 worktree 隔离（v1-S5 D）：受管根目录（可配，缺省派生自事件库目录）。
	wtRoot := strings.TrimSpace(cfg.WorktreeRoot)
	if wtRoot == "" {
		wtRoot = filepath.Join(filepath.Dir(cfg.DBPath), "worktrees")
	}
	wm, werr := worktree.NewManager(wtRoot)
	if werr != nil {
		_ = h.Close()
		assembled = true
		return nil, fmt.Errorf("host: 建 worktree 管理器: %w", werr)
	}
	// 登记表**启动时重建**（v2-S5 清账③）：登记表此前只是进程内存，hub 重启后
	// "谁建了哪个 worktree"全丢，而磁盘上的 worktree 还在（git 元数据跨进程存活）。
	// 现在按 `git worktree list` 从磁盘重建（幂等合并：已登记的同名条目不覆盖）。
	if n, rerr := wm.Rebuild(context.Background()); rerr != nil {
		// 重建失败**不阻断启动**：worktree 是并行任务的便利设施，不是 loop 的依赖
		// （宁可少登记几个，也别让 hub 起不来）。告警说清"登记表可能不全"。
		hubLog.Warn("host: worktree 登记表重建失败（可稍后手动重建）", "err", rerr.Error())
	} else if n > 0 {
		hubLog.Info("host: worktree 登记表已从磁盘重建", "count", n, "root", wm.Root())
	}
	h.worktrees = wm

	// 装配成功：资源归 Hub 所有（Close 时收）——失败兜底不再插手。
	assembled = true
	return h, nil
}

// Close 释放资源（幂等）：取消 hub 生命周期 → 等在跑的协议面 prompt 收场 →
// 停后台索引器/监控器 → 收沙箱与 MCP → 关事件库。
//
// 顺序有讲究：**先**取消生命周期（那一轮 prompt 立刻收场，不再往事件库写东西），
// **再**等它们退出（有界，promptCloseGrace），**最后**才关事件库——于是不会出现
// "库已关、goroutine 才醒过来往里写"（database is closed）。
func (h *Hub) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.mu.Unlock()

	var errs []error
	// ① 先取消 hub 生命周期：在跑的协议面 prompt 立刻收场（fail-close：取消即 deny），
	//    不再往事件库写东西——否则它们会在**库关掉之后**才醒过来（"database is closed"）。
	if h.lifeCancel != nil {
		h.lifeCancel()
	}
	// ② 等它们真正退出（**有界**：不让 Close 被一个不认 ctx 的工具/模型拖死）。
	//    等过了再关库 → 那几轮不会碰到已关闭的库。
	h.waitPrompts(promptCloseGrace)
	// WAL 收口（v4-4-B）：等在跑的 prompt **之后**——那一轮可能还在写 WAL，关早了就是
	// 往已关的 fd 上写（噪声 + 丢尾部记录）。放在停索引器/关库之前：WAL 与事件库无关，
	// 但它收得越早越好（磁盘句柄不泄漏）。
	if err := h.closeWAL(); err != nil {
		errs = append(errs, err)
	}
	// telemetry 收口（L2/D19）：与 WAL 同时机——等在跑的 prompt 之后（那一轮可能还在
	// Emit），关早了就是往已关的 fd 上写。
	if err := h.closeTelemetry(); err != nil {
		errs = append(errs, err)
	}
	if h.ixCancel != nil {
		h.ixCancel()
	}
	if h.ixDone != nil {
		<-h.ixDone
	}
	if h.monCancel != nil {
		h.monCancel()
	}
	if h.monitor != nil {
		h.monitor.Stop()
	}
	if h.sandbox != nil {
		if err := h.sandbox.Close(); err != nil {
			errs = append(errs, fmt.Errorf("host: 关沙箱执行环境: %w", err))
		}
	}
	// MCP server 进程树收尸（v3-S1，陷阱 5）：stdio server 是本进程拉起的，
	// hub Close 时也由本进程杀树 + Wait。放在关库**之前**——server 可能还在
	// 往里写东西（虽然它写的是自己的 stdio，不是我们的事件库）。
	if h.mcpManager != nil {
		if err := h.mcpManager.Close(); err != nil {
			errs = append(errs, fmt.Errorf("host: 关 MCP 连接: %w", err))
		}
	}
	if err := h.store.Close(); err != nil {
		errs = append(errs, fmt.Errorf("host: 关事件库: %w", err))
	}
	return errors.Join(errs...)
}

// promptCloseGrace 是关库前等待在跑的协议面 prompt 收场的宽限（秒）。
//
// 有界而不是无限等：prompt 链上可能有**不认 ctx** 的环节（一个卡住的外部命令、一个
// 不理 ctx 的 provider）——那属于"它自己坏了"，不该把 Close 拖成永久挂起。
// 超时只记一条告警：那一轮随后会在已关的库上写失败（可见的噪声），但 hub 一定能关掉。
const promptCloseGrace = 5 * time.Second

// waitPrompts 等在跑的协议面 prompt 收场，最多等 grace。
//
// 为什么要等：Close 的下一步就是**关事件库**——不等的话，那些 goroutine 醒来后
// 往已关的库写事件（"database is closed"）。等过了就没有这类噪声。
func (h *Hub) waitPrompts(grace time.Duration) {
	done := make(chan struct{})
	go func() {
		h.promptWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(grace):
		hubLog.Warn("host: 关库前等待在跑的协议面 prompt 超时（可能仍有 goroutine 在收尾）",
			"grace", grace.String())
	}
}

// RunPrompt 起（或复用）一个 session 的 agent loop 并跑一轮。
//
// sessionID 为空 → 新建一个（返回的 Result 里拿不到新 id，调用方要 id 就用
// NewSession() / CreateSession() 先取一个）。prompt 为空 → 拒绝（空 prompt 等于浪费一次
// 模型调用）。sessionID 未登记归属 → 自动 gate 用 hub 级默认（Config.CWD / RunID）。
//
// **每轮带历史**（P1-3）：跑之前把本 session 的事件流 fold 成对话历史再喂给模型
// （见 foldSessionHistory）——多轮对话同源，与 fork / resume 同一套机制。此前这里恒空
// 历史（每轮失忆），协议面 / CLI chat / TUI / ACP 的多轮全断在同一处。
func (h *Hub) RunPrompt(ctx context.Context, sessionID contracts.Ulid, prompt string) (loop.Result, error) {
	if h == nil {
		return loop.Result{}, errors.New("host: Hub 为 nil")
	}
	if strings.TrimSpace(prompt) == "" {
		return loop.Result{}, errors.New("host: prompt 不能为空")
	}
	// W15 成本预算闸门（v4-4-B；**默认不设上限** = BudgetMaxTokens<=0 时 checkBudget 直接
	// 返回 nil = 零行为变化）。**Run 前**检查（per-Run 层）：已累计 ≥ 上限 → 拒绝发起本轮
	//（不再烧 token）。轮间另有 TurnBudget 钩子（缺口 5 第二件，见 loopFor 的装配）。
	if berr := h.checkBudget(ctx, sessionID); berr != nil {
		return loop.Result{}, berr
	}
	lp, _, err := h.loopFor(ctx, sessionID) // created 不用（留痕口径在 takeover.go 那边）
	if err != nil {
		return loop.Result{}, err
	}
	// G-a 在线三态：**本轮在跑**标记（BeginRun/EndRun 配平，进程本地 running 计数）。
	// 所有 prompt 路径（HTTP / WS / CLI）都过 RunPrompt，就这一处打点——放在 loopFor
	// 成功之后：建 loop 失败 / 预算拒绝（checkBudget 在上面）都不算"在跑"。defer 保证
	// 早退与 panic 也配平（EndRun 下限 0，重复调用无害）；running 短路让"刚起步还没落
	// 事件"的轮次也算活跃（判定见 discovery.Table.SessionActive，四消费方同一处）。
	h.disc.BeginRun(sessionID)
	defer h.disc.EndRun(sessionID)
	// P1-3：**每轮带历史**（多轮对话同源）——读本 session 的事件流 → FoldConversation 折成
	// 对话历史 → RunWithHistory。fold 在 Run **之前**（本轮 prompt 还没进事件流，历史不含
	// 本轮，不自含）。见 foldSessionHistory 的口径说明。
	history, herr := h.foldSessionHistory(ctx, sessionID)
	if herr != nil {
		return loop.Result{}, herr
	}
	res, err := lp.RunWithHistory(ctx, history, prompt)
	// W15：**Run 后**再查一次——本轮把累计推过了上限就告警（越界留痕；per-Run 层的
	// 拒绝发生在**下一次** RunPrompt 入口；Run **内**的逐轮截断由 loop 的 TurnBudget
	// 钩子负责，见 host/budget.go 的"两层检查"）。
	h.warnIfOverBudget(ctx, sessionID)
	// 逐轮预算闸门**真的截断了这一轮**（缺口 5 第二件）→ 单独告警一条。
	//
	// 为什么 warnIfOverBudget 不够：那条告警说的是"累计已越界"（成本观测面），而这里说的是
	// "**这一轮没跑完**"——轮次被闸门挡在 provider.Run 之前（那一轮什么都没跑：没有
	// context.assembled、没有 model.turn）。两者是不同的事实，审计上要能分辨"跑完了但
	// 超了"与"被挡住了没跑"。
	//
	// ACP 面的映射：stopReason = max_turn_requests（ACP 枚举里没有"预算耗尽"这一档；取舍
	// 见 runtime/acp/endpoint.go 的 turnsExhausted）——所以**宿主这一条 Warn 是日志侧
	// 唯一能分出"轮数用尽"与"预算用尽"的地方**。
	if res.TruncatedByBudget {
		hubLog.Warn("host: 本轮被逐轮成本预算闸门截断（该轮未发起；已跑轮次的结果照常收口）",
			"session_id", string(sessionID), "reason", res.TruncationReason)
	}
	// 跑完立刻把新事件并进投影 + 发现表：/api/context 与 /api/peers 不必等下一次请求的
	// 增量扫描。
	if _, cerr := h.proj.catchUp(ctx); cerr != nil {
		// 投影失败不影响 Run 的结果（在线表晚一会刷新），但不该被完全吞掉：
		// 水位没前进 → 下一次 API 请求的 catchUp 会重试同一区间。
		hubLog.Warn("host: Run 后投影 catch-up 失败（下次 API 请求会重试）",
			"session_id", string(sessionID), "err", cerr.Error())
	}
	if _, cerr := h.discFeed.catchUp(ctx); cerr != nil {
		hubLog.Warn("host: Run 后发现表 catch-up 失败（下次 API 请求会重试）",
			"session_id", string(sessionID), "err", cerr.Error())
	}
	// 图投影 catch-up（v2-S2）：**自然的 catch-up 点**（与投影/发现表同一处）。
	// 同进程写入靠写面的 Committed() 失效闭包缓存即可；这里补的是**跨进程**写入
	// （另一个索引器写进同一个库）的可见性——请求路径不再每次 Sync（F13）。
	//
	// **冷启动成本落在第一轮 Run 上**（N3）：这一步会 Sync → 首次就把整张图灌进
	// 内存（20 万边 ~0.4s + G19 量级内存；无 seq 索引时 ~1.4s，F4 后走索引区间扫）。
	// 所以"没人查过图"并不等于"不加载"——
	// 启用图投影就意味着接受这份成本（图是**可选投影**，不启用则零开销，G2f）。
	if h.graphEngine != nil {
		if gerr := h.graphEngine.Sync(ctx); gerr != nil {
			hubLog.Warn("host: Run 后图投影 catch-up 失败（下次 API 请求会重试）",
				"session_id", string(sessionID), "err", gerr.Error())
		}
	}
	return res, err
}

// foldSessionHistory 折叠本 session 的对话历史（P1-3：每轮带历史，多轮对话同源）。
//
// 事件流是唯一事实源（F1）：user.prompt / model.turn / tool.call.* 折成
// []core.Message（runtime.FoldConversation）——与 fork（host/fork.go 的 RunBtw）与
// resume（cmd/alasol/resume.go）**同源同机制**。
//
// 为什么在 RunPrompt 里做（此前没有）：协议面 / CLI chat / TUI / ACP 的多轮对话**恒空
// 历史**——lp.Run 就是 RunWithHistory(ctx, nil, prompt)，每轮都失忆。设计 §2 要求"每轮
// 折叠事件流带历史（多轮对话同源）"，机制现成（FoldConversation + RunWithHistory）。
//
// 顺序：**在 lp.Run 之前**折——本轮 prompt 还没进事件流（user.prompt 是 Run 开始才落的
// 旁路观测），折出来的历史不含本轮（不自含）。
//
// # 视角：**主 agent 视角**（F1）
//
// FoldConversation 的契约写着"调用方应按 agent_id 预过滤"——**本函数就是那个调用方**：
// 一个 session 里可能混着**多个 agent** 的对话（MultiAgent 开时，子 agent 自己的
// user.prompt / model.turn 也落在**同一个 session** 的事件流里，agent_id = 子 agent）。
// 不过滤的话，子 agent 的产出会以 user/assistant 消息混进主 agent 的历史——主 agent
// 下一轮会看到"没发生过的用户话"（子任务描述）与子 agent 的自述。
//
// 子 agent 的产出**本来就该经 spawn 工具回灌**：spawn 的 tool.call.completed 归**主
// agent**（父 loop 发的意图与结果），里面带子 agent 的 summary——那才是主 agent 该看到
// 的形态。所以过滤掉子 agent 自己的对话**不丢信息**。
//
// # 三种口径（别混）
//
//	resume（cmd/alasol/resume.go）= **整场全景**：不区分 agent 视角（有意：续跑是
//	                               "把这场会话发生过的一切接上"）。
//	fork（host/fork.go 的 RunBtw）  = **主对话**（现状：折整场）。
//	主轮次（本函数）              = **主 agent 视角**（本刀：按主 agent id 预过滤）。
//
// # 不加截断上限（F4：设计如此）
//
// 主轮次历史**无上限**：默认 append 每轮带全量历史（token 随轮次线性涨、每轮 O(整场)
// 回放）。要封顶请开 context-strategy=tiered（L1/L2 级联压缩），注入区配置化在 G-h。
// fork 的 16 条 / 6000 字是**旁支专属**（一次性请求要有界），不套到主轮次。
//
// 回放失败 → 返回错误（fail-close，与 fork 同口径）：历史读不出来还照跑，等于悄悄把
// 模型丢回"每轮失忆"的老路——那正是本刀要修的病，不该被一次读失败请回来。
func (h *Hub) foldSessionHistory(ctx context.Context, sessionID contracts.Ulid) ([]core.Message, error) {
	events, err := runtime.ReplaySession(ctx, h.store, sessionID)
	if err != nil {
		return nil, fmt.Errorf("host: 回放 session %s 的事件流（带历史）: %w", sessionID, err)
	}
	// 主 agent id：**loopFor 登记的那一个**（h.mainAgents[sid] ≡ 树 root ≡ loop.AgentID，
	// 同一口径，不另造）。fold 发生在 loopFor 之后 → 一定登记好了；取不到 = 装配残缺
	//（fail-close：没有视角就没有历史，宁可这一轮失败也别喂一份混视角的历史）。
	mainAgent, ok := h.mainAgentOf(sessionID)
	if !ok {
		return nil, fmt.Errorf("host: 取 session %s 的主 agent id 失败（带历史需要它做视角过滤）", sessionID)
	}
	scoped := make([]contracts.Event, 0, len(events))
	for i := range events {
		if events[i].AgentID == mainAgent {
			scoped = append(scoped, events[i])
		}
	}
	return runtime.FoldConversation(scoped), nil
}

// loopFor 取（或建）某 session 的 Loop。
//
// 一个 session 一个 Loop：System/Model/AgentID 固定，历史由每次 Run 的局部变量带
// （loop 无可变状态 → 同一 Loop 可并发 Run，见 loop 包 doc）。
//
// # 顺序：**先拿托管权、后发布**（P0-1）
//
//	① 进锁查缓存（命中 → 直接返回，created=false）
//	② 锁内建 loop（**不发布**）
//	③ 锁外 Claim（拿托管权；冲突 → 直接失败——**没发布就没什么可回滚**）
//	④ 再进锁双检：别人已发布 → 丢弃自己那份、返回已发布的（created=false）
//	⑤ 发布（created=true）
//
// 为什么必须这样（旧的四步不原子）：旧顺序是"建 loop → 先写进 h.loops → 再 Claim →
// 冲突才回滚"。并发两个 POST /prompt 打到"别的 hub 托管"的 session 时，一个 409，
// 另一个却从**缓存**拿到 loop → 202 并双写事件（G25 破：全局 seq 变成混排提交序），
// 还落了一条假"接管成功"留痕。现在 Claim 失败 = **没有发布**，冲突方与缓存命中方
// 都拿不到 loop → 一律 409（唯一拒绝线）。
//
// 同 hub 幂等：两个 goroutine 同时接管同一个 session，两次 Claim 都成功（同 hub id），
// 双检保证只有一个 loop 被发布（另一个丢弃自己那份）——无害。
//
// 建成时顺做两件 K10/K12 的事：① 发现表 Claim（G25 跨进程托管注册：本进程持有该
// session 并登记宿主 hub 标识——活状态无租约无心跳，G17b；冲突 fail-close 拒绝接管）；
// ② 加入自动 gate（repo:/workflow:）。② 失败不记账（**下次 Run 重试**——F2：缓存命中
// 路径会补一次自愈，见下面的 joinAutoGatesIfNeeded；注释里这句话现在是真的）——
// 加不上 gate 只会让该 session 暂不可见（fail-close：失败让可见性变小，不是变大），
// 不该因此拒绝跑这一轮。
//
// created = 本次调用**真正建成并发布**（true）/ 缓存命中 / 双检落败（false）。
// 留痕口径用得上它（takeover.go：只有"真接管"才值得复盘留痕）。
func (h *Hub) loopFor(ctx context.Context, sessionID contracts.Ulid) (*loop.Loop, bool, error) {
	if sessionID == "" {
		return nil, false, errors.New("host: sessionID 必填（空 = 用 NewSession()/CreateSession() 先取一个）")
	}
	h.mu.Lock()
	if h.hubID == "" {
		h.hubID = string(core.NewUlid()) // 字面量构造的 Hub（内部测试）兜底：Claim 需要非空宿主标识
	}
	if lp, ok := h.loops[sessionID]; ok {
		// 缓存命中：**补一次自动 gate 自愈**（F2）——上一次 join 失败是不记账的（下次 Run
		// 重试），而"下次 Run"走的就是这条缓存命中路径：不补的话那句注释是假话，失败的
		// gate 永远补不上（该 session 一直"暂不可见"）。
		//
		// h.mu 下**只读 map**（autoGates / mainAgents / specs），join 在锁外做（它要发
		// 事件、扫事件流——不能持 h.mu）。joinAutoGates 幂等（InGate / LeftGate 都幂等），
		// 重复 join 不会多落事件。
		autoDone := h.autoGates[sessionID]
		agentID := h.mainAgents[sessionID]
		spec := h.specs[sessionID]
		h.mu.Unlock()
		if !autoDone {
			h.joinAutoGatesIfNeeded(ctx, sessionID, agentID, spec)
		}
		return lp, false, nil // 缓存命中：不是本次建成（created=false）
	}
	// 主 agent id 的解析顺序（v3-S6-B F1 修）：**已登记 → 事件流 → 新造**。
	//
	//  ① h.mainAgents[sessionID]（本进程已落定的主 agent id）——**最优先**；
	//  ② disc.PrimaryAgent（事件流口径：续跑复用原 id，与 durable.PrimaryAgentID 同口径）；
	//  ③ 新造 ULID（全新 session）。
	//
	// **同源口径（收窄，F2）**：**树 root ≡ h.mainAgents ≡ loop.AgentID**——**本进程内**恒同源
	// （树 root 在建树那刻固定；loop 重建时先查 mainAgents → 拿回同一个 id）。
	// 但**事件流归属**只在"一库一 hub"拓扑下同源：共享事件库的**多写者**拓扑下，
	// 事件流口径的主 agent（disc.PrimaryAgent）可能 ≠ 本进程钉住的 id → 本进程写的
	// 事件归属与事件流整体口径**分叉**（下面会打一条 Warn，别静默）。
	//
	// 为什么 ① 必须最优先（F1 的病根）：agent 树是 **session 活状态**（建了就一直复用），
	// 它的 root 在**建树那一刻**就固定成了当时的 agentID；而 loop 每次重建都会重算
	// agentID。session 还没有事件时（新建 session 尚未跑过 / loop 被失效后还没跑过）
	// disc.PrimaryAgent 查不到 → 重算出一个**新** id → 与树 root 漂移 →
	// 主 agent 静默失去 spawn 工具（visibleTools 查不到节点 → 不注入 spawn）、
	// spawn 必被 ErrParentNotFound 拒、GET agents 的 root.agent_id 也与事件流不一致。
	// 先取"已登记的稳定 id"，树 root ≡ loop.AgentID（**本进程内**）就恒同源。
	agentID, ok := h.mainAgents[sessionID]
	if ok {
		// ① 命中：**不复核** disc.PrimaryAgent 的取值（顺序不能改——改回事件流优先会把上一轮
		// F1 的 spawn 失效放回来），但**复核它是否已知且不等**：不等 = 事件流口径与本进程
		// 钉住的 id 分叉（共享事件库多写者拓扑）→ 打一条 Warn（可观测，别静默）。
		// 只有"已知且不等"才告警：查不到 = 事件流里还没有该 session 的主 agent（新 session /
		// 尚未扫到），那不是分叉。
		if streamID, sok := h.disc.PrimaryAgent(sessionID); sok && streamID != agentID {
			hubLog.Warn("host: 主 agent id 与事件流口径分叉（本进程钉住的 id ≠ 事件流口径："+
				"共享事件库的多写者拓扑；本进程的事件归属 = 钉住的 id）",
				"session_id", string(sessionID), "pinned", string(agentID), "stream", string(streamID))
		}
	} else {
		agentID, ok = h.disc.PrimaryAgent(sessionID)
		if !ok {
			// 新 session（事件流里还没有它）→ 新主 agent。续跑的 session 复用原来的 id
			// （与 durable.PrimaryAgentID 同口径），不造"幽灵 agent"。
			//
			// 取之前先把事件流扫到最新：session 原属**另一个 hub 进程**时（共享事件库），
			// 本地水位可能还没追上它的历史，那就该先扫——否则会退化成新 id。
			if _, serr := h.discFeed.catchUp(ctx); serr != nil {
				hubLog.Warn("host: 取主 agent 前扫事件流失败（退化成一个新 agent id）",
					"session_id", string(sessionID), "err", serr.Error())
			}
			agentID, ok = h.disc.PrimaryAgent(sessionID)
			if !ok {
				agentID = core.NewUlid()
			}
		}
	}
	// 登记主 agent id（**建树之前**，F1）：树 root 与 loop.AgentID 都从它来 → 恒同源。
	// 早于建树登记，是为了让"树已建但 loop 建成失败"的下一次 loopFor 也拿同一个 id
	// （否则那次失败会留下一个 root 与 loop.AgentID 漂移的树）。
	if h.mainAgents == nil {
		h.mainAgents = make(map[contracts.Ulid]contracts.Ulid) // 字面量构造的 Hub（内部测试）兜底
	}
	h.mainAgents[sessionID] = agentID
	// 激活的工作流（W12：归属 session；调用方持 h.mu）：有 → 哲学文本进 system prompt（W3），
	// 且按其声明的 context_strategy 收敛策略 kind（W6）。未激活 = 用宿主默认（零行为变化）。
	system := h.workflowSystemForLocked(sessionID)
	strategyKind := h.workflowStrategyKindForLocked(sessionID)
	strategy, err := h.contextStrategyFactory(sessionID, agentID, strategyKind)
	if err != nil {
		h.mu.Unlock()
		return nil, false, fmt.Errorf("host: 为 session %s 装配上下文策略: %w", sessionID, err)
	}
	if strategy == nil {
		// G-h：session 级 kind 覆盖（config.context.kind=tiered）能把 append 提成 tiered
		// → 补挂一个**只备不用**的 tiered 能力工厂（loop 只在本轮生效 kind = tiered 时
		// 才调它；append 时它不会被调用 → 零开销、零行为变化）。
		strategy = h.contextStrategyCapabilityFactory(sessionID, agentID)
	}
	// v3-S6-B：MultiAgent 开 → 每 session 一棵 agent 树（root=主 agent id，续跑复用）+ 把
	// loop.Agents 接进 loop.Config（spawn/complete 对模型可见、子 agent 可派生、W6-A 子策略钩子）。
	// 关（默认）= agentsEnv 留 nil → loop.New 收 nil = 单 agent 模式零行为变化（酸测试）。
	// 树是 session 活状态：已建则复用（workflow 激活只 delete loops、不 delete agentTrees，
	// 见 activateWorkflow）——续跑/重新装配 loop 不丢已派生的子 agent。
	var agentsEnv *loop.Agents
	if h.cfg.MultiAgent {
		tree := h.agentTreeForLocked(sessionID, agentID)
		agentsEnv = &loop.Agents{
			Tree: tree,
			// Verifier：summary refs 核验器（K24/Q7）。宿主暂无生产实现 → nil = 不核验
			//（refs 原样保留；要核验在后续挂 agents.RefVerifier 实现）。
			Verifier: nil,
			// W6-A 子策略钩子：按子类型显式声明的 context_strategy 造 per-Run 工厂（绑 child.ID）。
			ChildStrategy: h.childStrategyFactory(sessionID),
		}
	}
	lp, err := loop.New(loop.Config{
		Provider:        h.provider,
		Bus:             h.bus,
		Registry:        h.reg,
		Policy:          h.engine,
		Approver:        h.approver,
		SessionID:       sessionID,
		AgentID:         agentID,
		Model:           h.cfg.Model,
		System:          system,
		MaxTurns:        h.cfg.MaxTurns,
		ReasoningEffort: h.cfg.ReasoningEffort, // 思维链档位（G14；空 = 不传）
		// G-c/G-d 生效链第三层：per-Run 覆盖钩子——每个 prompt 的 Run 开头重取
		// config.fold（扫平事件流）+ 叠 hub 默认 → 冻结进 runState（轮 = Run 开始
		// 重取；运行中变更不影响已冻结的本轮，下一轮生效）。无配置事件时返回零值
		// 覆盖（等价于不挂），代价只是每次 Run 多一次增量扫描。
		//
		// G-h：钩子顺带解析**本轮生效的上下文策略 kind**（session 覆盖 + 基线 kind），
		// 基线 kind = strategyKind（宿主默认 / 工作流声明）。
		RunOverrides:  h.runOverridesHook(sessionID, strategyKind),
		ContextBudget: h.contextBudgetFor(strategyKind),
		// W15 逐轮预算制动（v4-4-B 缺口 5）：BudgetMaxTokens<=0 → nil 钩子（零查询、
		// 零行为变化）；>0 → 每轮 provider.Run 前读一次累计成本（checkBudget 同款
		// fail-open：查询失败不拒本轮，warn 留痕）。per-Run 的 checkBudget（RunPrompt
		// 入口 / fork）保留——两层不冲突：Run 前拒 + 轮间拒。
		TurnBudget: h.turnBudgetFor(sessionID),
		Store:      h.store, // goal ledger 生产者（J19/X6）与手动 checkpoint（X3）经它
		// 在线表注入（K14/K15）：主 agent 的 prompt 末尾恒带在线表；配置运行期可调。
		// subagent 的 loop 不传这两项（nil = 不注入，K11：只有主 agent 参与水平可见性）。
		OnlineTable:       h.onlineConfig,
		OnlineTableSource: h.onlineSource,
		// 上下文策略（v2-S1；nil = append 默认）。per-Run 工厂，见 contextStrategyFactory。
		// strategyKind 已按激活的工作流声明的 context_strategy 收敛（W6）。
		ContextStrategy: strategy,
		// 注入区截断的观测回调（G-h）：loop 灌完本轮参数后回读装配器的截断观测面 →
		// 截断了就 Warn 留痕（退化必须可见——静默砍掉规则等于悄悄丢上下文）。
		ContextInjectionObserver: func(rules, commonSense bool) {
			h.noteContextInjectionTruncated(sessionID, rules, commonSense)
		},
		// goal ledger 更新工具（J19/X6；opt-in，默认关 = 零行为变化）。
		GoalLedger: h.cfg.GoalLedger,
		// 多 agent 环境（v3-S6-B；nil = 单 agent 模式默认）。见上面的 agentsEnv 装配。
		Agents: agentsEnv,
		// 大 payload 外置 blob（v4-3-A，G2e/D4；nil sink / 阈值<=0 = 关 = 零行为变化）。
		// h.blob 开了才非 nil（Config.BlobDir 非空）；阈值 cfg.BlobMaxInline>0 才真正外置。
		BlobSink:      h.blob,
		BlobMaxInline: h.cfg.BlobMaxInline,
		// WAL 预写日志（v4-4-B；nil = 关 = 零行为变化）。h.wal 开了才非 nil
		//（Config.WalDir 非空）；walFor 懒建 per-session Writer，开失败只告警不拒绝本轮
		//（WAL 是尽力而为的补充面）。六个写点都在 X1 三段路径**之外**（见 core/loop/wal.go）。
		WalSink: h.walFor(sessionID),
		// telemetry（L2/D19；nil = 关 = 零行为变化）。h.tel 开了才非 nil（Config.Telemetry=file）；
		// 三个发点都在 X1 三段路径**之外**（见 core/loop/telemetry.go）。独立管道，不参与判定。
		TelemetrySink: h.telemetrySink(),
	})
	if err != nil {
		h.mu.Unlock()
		return nil, false, fmt.Errorf("host: 为 session %s 建 loop: %w", sessionID, err)
	}
	// ⚠ **这里不发布**（P0-1）：loop 建好但还没进 h.loops——托管权（Claim）拿到之后才发布。
	// 提前发布 = 冲突方能从缓存里拿到一个"本 hub 没托管"的 loop → 双写事件（G25 破）。
	spec := h.specs[sessionID] // 未登记 → 零值（CWD/RunID 空）→ 用 hub 级默认
	autoDone := h.autoGates[sessionID]
	hubID := h.hubID
	h.mu.Unlock()

	// G25 跨进程托管注册（2026-11-01 v4-7）：session 创建/接管即在发现表登记宿主 hub
	// 标识（进程标识）。**冲突 fail-close**：该 session 已登记给另一个 hub → 拒绝接管
	// （双宿主会各自 append 同一 session 的事件，破坏 session 亲和写）。
	// 同 hub 重复 Claim 幂等（loop 重建 / 续跑 / 并发接管都走这里）。
	//
	// 冲突时**什么都不用回滚**（P0-1）：loop 还没发布（没进 h.loops），没有"半个托管态"
	// 要清——直接失败即可（错误链带 ErrHostConflict → 调用方回 409 host_conflict）。
	if cerr := h.disc.Claim(sessionID, hubID); cerr != nil {
		return nil, false, fmt.Errorf("host: 托管登记失败（G25：拒绝接管，提示用户该 session 归属别的 hub）: %w", cerr)
	}
	// 双检（P0-1）：Claim 期间别的 goroutine 可能已经发布了这个 session 的 loop（同一 hub
	// 的并发接管：两次 Claim 都幂等成功）。谁先发布谁算数——后来者**丢弃自己这份**
	// （loop 无可变状态，丢掉只是白建一次，不留痕、不发布），返回已发布的那份。
	h.mu.Lock()
	if published, ok := h.loops[sessionID]; ok {
		h.mu.Unlock()
		return published, false, nil // 双检落败：不是本次发布（created=false）
	}
	h.loops[sessionID] = lp
	h.mu.Unlock()

	if !autoDone {
		h.joinAutoGatesIfNeeded(ctx, sessionID, agentID, spec)
	}
	return lp, true, nil
}

// joinAutoGatesIfNeeded 在自动 gate **尚未加入**时补 join（K12），成功才记账（置
// h.autoGates[sid]），失败只 warn **不记账**（下次 Run 重试——F2：缓存命中路径会再试，
// 这句"下次 Run 重试"现在是真的）。
//
// 幂等：joinAutoGates 自身幂等（InGate → 已是成员就跳过；LeftGate → 曾显式离开就不
// 自动复活），所以重复调用不会多落 gate.joined 事件。
//
// 调用方**不持 h.mu**（join 要发事件、扫事件流）。
func (h *Hub) joinAutoGatesIfNeeded(ctx context.Context, sessionID, agentID contracts.Ulid, spec SessionSpec) {
	if jerr := h.joinAutoGates(ctx, sessionID, agentID, spec); jerr != nil {
		hubLog.Warn("host: 自动 gate 加入失败（该 session 暂不可见，下次 Run 重试）",
			"session_id", string(sessionID), "err", jerr.Error())
		return
	}
	h.mu.Lock()
	h.autoGates[sessionID] = true
	h.mu.Unlock()
}

// NewSession 生成一个新的 session id（调用方先拿 id 再 RunPrompt，便于 UI/CLI 展示）。
//
// 不登记工作目录 → 自动 gate 用 hub 级默认。要按 session 指定归属（多 worktree）用
// CreateSession。
func NewSession() contracts.Ulid { return core.NewUlid() }

// agentTreeForLocked 取（或建）某 session 的 agent 树（v3-S6-B）。**调用方须持 h.mu**。
//
// root = 该 session 的主 agent id（WithRootID：续跑复用同一个 id，与 loop 的 AgentID 对齐）。
// 树是 session 活状态：已建则**复用**（不重建——重建会丢掉已派生的子 agent；workflow 激活
// 只失效 loop、不失效树，重新装配 loop 时走这里取回同一棵树）。
//
// **不变式（F1，收窄口径见 F2）**：**树 root ≡ h.mainAgents 登记值 ≡ loop.AgentID**——
// **本进程内**恒同源。树 root 在建树那一刻固定，loop 却会重建；
// 故 loopFor 取 agentID 时**先查 h.mainAgents**（已登记的稳定 id），不让"重建 loop"
// 重新算出另一个 id（漂移 → 主 agent 静默失去 spawn 工具 + spawn 被 ErrParentNotFound 拒
// + GET agents 的 root.agent_id 与事件流不一致）。
//
// **事件流归属**只在一库一 hub 拓扑下与上面三者同源：共享事件库的**多写者**拓扑下，
// 事件流口径的主 agent 可能 ≠ 本进程钉住的 id（此时 loopFor 会打一条 Warn，见上）。
func (h *Hub) agentTreeForLocked(sessionID, agentID contracts.Ulid) *agents.Tree {
	if h.agentTrees == nil {
		h.agentTrees = make(map[contracts.Ulid]*agents.Tree)
	}
	if t, ok := h.agentTrees[sessionID]; ok {
		return t
	}
	t := agents.NewTree(
		h.rootAgentType(),
		agents.WithRootID(agentID),                       // root = 主 agent id（续跑复用）
		agents.WithTypes(h.agentTypes),                   // 类型注册表（内建五类 + TOML 自定义）
		agents.WithMaxDepth(agents.DefaultMaxAgentDepth), // 3 = 两级委派（K17）
	)
	h.agentTrees[sessionID] = t
	return t
}

// contextBudgetFor 是 context.assembled 的 tokens_budget 分母（占比口径，J21）。
//
// **与上下文策略同源**（F10/F13）：挂 tiered 时窗口 W 就是上下文预算——用 loop 的 200k
// 默认分母去算 1M 窗口的占用，UI 的占比会显示成几百个百分点（占比失真）。
// 显式配了 ContextBudget 则以它为准；kind 非 tiered → 0 = loop 用默认（append 路径）。
//
// **session 级窗口覆盖优先**（G-h）：session 改了窗口（config.context.window）→ 分母
// 跟着换（否则窗口换了、占比还按旧窗口算 = 占比失真）。这一步在 **loop 侧按 Run** 做
// （core/loop 的 tokensBudgetFor：读本轮快照的覆盖值）——因为覆盖是运行期可变的，
// 装配期定下来的分母跟不上。
func (h *Hub) contextBudgetFor(kind string) int64 {
	if h.cfg.ContextBudget > 0 {
		return h.cfg.ContextBudget
	}
	if strings.TrimSpace(kind) == contextStrategyTiered {
		w := h.cfg.ContextStrategyWindow
		if w <= 0 {
			w = compaction.DefaultWindow // tiered 的默认窗口（1M）就是预算分母
		}
		return w
	}
	return 0
}

// Addr 返回归一化后的监听地址（Config.Addr 的解析结果，供 serve 打印与监听）。
func (h *Hub) Addr() string {
	if h == nil {
		return ""
	}
	return h.cfg.Addr
}

// ID 返回本 hub 的**进程标识**（G25 跨进程托管注册；Hub.New 生成的 ULID，恒非空）。
// 发现表 Claim 用它登记 session 归属；/api/sessions 行的 host_hub 也是它。
func (h *Hub) ID() string {
	if h == nil {
		return ""
	}
	return h.hubID
}

// Registry 返回 hub 的扩展注册表的**只读视图**（v4-5 B 类 MCP server 面用：
// `alasol mcp-serve` 经 runtime/mcpserver 读工具条目 + 执行器，把它们暴露成 MCP tools）。
//
// 口径：这是**只读消费**出口——mcpserver 只 List/Get 工具条目与执行器，不 Register /
// Enable / Disable（写面归装配期 host.New 与策略链）。
//
// **返回类型收窄成只读接口**（F5；与上面的 EventSource 同一纪律）：返回原始
// *registry.Registry 等于把注册表的写面（Register / Enable / Disable / Unregister /
// SetHealth）整个递出去——"约定只读"靠不住（谁都能 Disable 掉别人的扩展、Unregister 掉
// 内置工具）。接口层不暴露 = 编译期就够不着写方法。返回 nil 仅当接收者为 nil 或 hub
// 没装注册表（不该发生；调用方判空）。
func (h *Hub) Registry() RegistryView {
	if h == nil || h.reg == nil {
		return nil
	}
	return h.reg
}

// EventSource 是 hub 事件总线的**只读出口**（订阅面）。
//
// 为什么是接口而不是 *bus.Bus（F6）：*bus.Bus 上有导出的 Publish——返回它等于把
// "往总线发事件"的写入口交给进程内消费者，名不副实（"只读出口"却带着笔）。
// 事件的事实源是 loop 的三段写路径与旁路观测（X1/F1）：**订阅方只读，不写**。
//
// 只暴露 Subscribe：订阅方拿到的 *bus.Subscription 只有 Events/Lagging/Close（读 +
// 释放自己的订阅），没有发布能力。
type EventSource interface {
	// Subscribe 注册一个订阅（按过滤器收事件）。
	Subscribe(f bus.Filter) *bus.Subscription
}

// Bus 返回 hub 的进程内事件总线（**只读出口**：供进程内消费者订阅增量事件）。
//
// 唯一的进程内订阅方是 v4-2-B 的 ACP agent 端点（runtime/acp）：它把某 session 的
// 事件（含 durable=false 的流式 model.delta，G13）桥成 ACP 的 session/update 通知。
// 订阅方只**读**总线（bus.Subscribe），不 Publish——本出口**不暴露** Publish（F6：
// 返回类型收窄成只暴露 Subscribe 的 EventSource，"只读出口"名副其实）。
//
// 口径：这是**接口层不暴露**（收窄返回类型，编译期就够不着 Publish），**不是运行时封禁**
// ——bus 包本身仍有导出的 Publish（core/bus 的写入口，loop 的三段写路径在用）；本方法只是
// **不把它交出去**。另找门路拿到 *bus.Bus 的进程内代码仍能 Publish——本出口管的是
// "别把笔递出去"，不是"上锁"。
// 返回 nil 仅当接收者为 nil 或 hub 没装总线（调用方判空）。
//
// 依赖方向纪律（09）：runtime 包**不 import host**（那是 cmd 的组合职责），故 acp 不能
// 直接拿 *Hub；它经 cmd/alasol/acp.go 的适配层调本方法拿到 EventSource 再 Subscribe。
func (h *Hub) Bus() EventSource {
	if h == nil || h.bus == nil {
		return nil
	}
	return h.bus
}

// Sessions 返回在线表（最新活跃在前，行数受 Config.MaxSessionRows 限制）。
//
// **无过滤**口径（公共面）：补列（hostable/host_hub/online/running/last_activity）→
// 按 MaxSessionRows 截断（排序在投影层已完成：LastSeq 降序）。
func (h *Hub) Sessions(ctx context.Context) ([]SessionRow, error) {
	return h.sessionsFiltered(ctx, nil)
}

// sessionsFiltered 是 Sessions 的**带过滤**内部变体（only = nil = 不过滤 = 与 Sessions
// 逐字节等价）。
//
// # 顺序：过滤在截断**之前**（P1-2）
//
//	取行（**不限**）→ 补列 → 按 online 过滤 → 最后按 MaxSessionRows 截断
//
// 为什么：旧顺序是"投影层按 LastSeq 降序**先截断** → HTTP 层再按 ?online= 过滤"——
// 排在 200 行外的**在线** session 被整段截掉，?online=true 回空表（critic 用
// MaxSessionRows=1 复现：两条 session，seq 大的那条离线、在线的那条被截掉 → []）。
// 过滤是"选子集"，必须在"取子集"**之前**做，否则选出来的是"被截断后剩下的子集"。
//
// 无过滤时本函数与旧行为等价（补列后截断，排序口径不变）。
func (h *Hub) sessionsFiltered(ctx context.Context, only *bool) ([]SessionRow, error) {
	if h == nil {
		return nil, errors.New("host: Hub 为 nil")
	}
	if _, err := h.proj.catchUp(ctx); err != nil {
		return nil, fmt.Errorf("host: 扫事件流: %w", err)
	}
	// 等索引器追到投影水位（有界等待）：刚跑完一轮时用户刷新页面就该看到
	// "索引已追至 seq N"，而不是 0（索引器是 50ms 轮询，正常几条就追平）。
	// 索引器卡住也不阻塞请求：超时后用当前值返回（诚实显示"还没追到"）。
	wm, err := h.waitIndexer(h.proj.scanWatermark(), indexerCatchUpWait)
	if err != nil {
		return nil, fmt.Errorf("host: 读索引水位: %w", err)
	}
	// ⚠ **先不过限**（limit=0 = 不截断，见 projection.rows）：过滤要在截断之前（P1-2）。
	rows := h.proj.rows(wm, 0)
	// hostable（G12 + G-b）：本进程是否已托管该 session（判据 = h.loops 里有 loop）。
	// 投影不认识 h.loops，这一列在出口处补。G-b 后协议面可对**任何**真 session 发
	// prompt（未托管 = 点"继续"触发懒接管；Claim 冲突才 409 host_conflict），本列语义
	// 收窄为"直发 / 需接管"的呈现辅助，不再是"可发/不可发"。一库一 hub 是预期拓扑（G25）。
	// 一次 h.mu 取 loops 快照再逐行标（对齐 fork_internal_test 的快照写法）——不逐行
	// hostsSession 反复抢锁。
	//
	// host_hub（G25 跨进程托管注册，2026-11-01 v4-7）：本进程托管的行带本 hub 的
	// 进程标识（h.hubID）；别的进程托管/失管 = 空（登记是发现表进程本地状态，跨进程
	// 读不到别家的，如实留空——前端按 hostable+host_hub 组合显示归属）。
	h.mu.Lock()
	hosted := make(map[contracts.Ulid]bool, len(h.loops))
	for sid := range h.loops {
		hosted[sid] = true
	}
	hubID := h.hubID
	h.mu.Unlock()
	// G-a 在线三态（活跃 / 托管 / 历史）：online / running / last_activity 在出口处补——
	// 与 hostable/host_hub 同款（投影不认识发现表，这一列在出口处补）。判定源是发现表：
	// Online/Running 由 discovery 行现算（事件流 ts + 进程本地 running 计数，K14/K15 注入、
	// K11 peers、agentlink presence、GET /api/sessions 四消费方同一处判定）；last_activity
	// 与 updated_at 同源同值（都是最后事件 ts，见 projection.SessionRow 的字段注释）。
	//
	// 先把发现表扫平（与上面 proj.catchUp 同理：刚落库的事件要立刻反映到 online 判定；
	// K14/K15 注入、K11 peers 都是"先扫平再读"，本出口同口径）。扫失败不致命——用当前表
	// 判定（下次请求重试），与 OnlineRows 的告警口径一致（在线表晚一会刷新，不 500）。
	if h.discFeed != nil {
		if _, derr := h.discFeed.catchUp(ctx); derr != nil {
			hubLog.Warn("host: Sessions 发现表 catch-up 失败（online 判定用当前表，下次请求重试）",
				"err", derr.Error())
		}
	}
	for i := range rows {
		if hosted[contracts.Ulid(rows[i].SessionID)] {
			rows[i].Hostable = true
			rows[i].HostHub = hubID
		}
		// 发现表还没有该 session（事件未进发现表 / 只有非 durable 事件）→ 保持零值：
		// online=false、running=false（"历史"态的默认答案）。last_activity 仍取投影的
		// 最后事件 ts（与 updated_at 同源同值；无事件 = ""）。
		if dr, ok := h.disc.Session(contracts.Ulid(rows[i].SessionID)); ok {
			rows[i].Online = dr.Online
			rows[i].Running = dr.Running
			rows[i].Waiting = dr.Waiting
		}
		rows[i].LastActivity = rows[i].UpdatedAt
	}
	// 过滤（补列之后——判定源是发现表，投影行只带值不判定）：只留 online == *only 的行。
	if only != nil {
		kept := make([]SessionRow, 0, len(rows))
		for i := range rows {
			if rows[i].Online == *only {
				kept = append(kept, rows[i])
			}
		}
		rows = kept
	}
	// 截断**最后**做（过滤之后）：行数上限是"返回多少行"，不是"看多少行"——先截断再
	// 过滤会把在线的行滤没（P1-2）。排序在投影层已完成（LastSeq 降序），截断保留最前的。
	if limit := h.cfg.MaxSessionRows; limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

// indexerCatchUpWait 是 Sessions 等索引器追平的有界等待（毫秒级；索引器 50ms 一轮）。
const indexerCatchUpWait = 250 * time.Millisecond

// waitIndexer 轮询等索引器水位达到 target（或超时返回当前值）。
func (h *Hub) waitIndexer(target int64, timeout time.Duration) (int64, error) {
	deadline := time.Now().Add(timeout)
	for {
		wm, err := h.indexer.Watermark()
		if err != nil {
			return 0, err
		}
		if wm >= target || !time.Now().Before(deadline) {
			return wm, nil
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// ContextSnapshot 取某 session 最新的 context.assembled 快照（不存在 → ok=false）。
func (h *Hub) ContextSnapshot(ctx context.Context, sessionID contracts.Ulid) (json.RawMessage, bool, error) {
	if h == nil {
		return nil, false, errors.New("host: Hub 为 nil")
	}
	if _, err := h.proj.catchUp(ctx); err != nil {
		return nil, false, fmt.Errorf("host: 扫事件流: %w", err)
	}
	raw, ok := h.proj.snapshot(sessionID)
	return raw, ok, nil
}

// Watermark 返回索引器已追到的全局 seq（G24③）。
func (h *Hub) Watermark() (int64, error) {
	if h == nil {
		return 0, errors.New("host: Hub 为 nil")
	}
	return h.indexer.Watermark()
}

// Monitor 返回 hub 的 CPU/内存监控器（K9；宿主内部 / 诊断面）。
func (h *Hub) Monitor() *Monitor {
	if h == nil {
		return nil
	}
	return h.monitor
}

// Health 是 GET /api/health 的形状。
type Health struct {
	OK        bool  `json:"ok"`
	Watermark int64 `json:"watermark"`
	// Sessions 是在线表行数（不含被上限截掉的部分；截断信息看 MaxSessionRows）。
	Sessions int `json:"sessions"`
}

// Health 汇总服务健康度（ok = 库开着且索引器在跑）。
func (h *Hub) Health(ctx context.Context) (Health, error) {
	if h == nil {
		return Health{}, errors.New("host: Hub 为 nil")
	}
	wm, err := h.indexer.Watermark()
	if err != nil {
		return Health{OK: false}, err
	}
	rows, err := h.Sessions(ctx)
	if err != nil {
		return Health{OK: false, Watermark: wm}, err
	}
	return Health{OK: true, Watermark: wm, Sessions: len(rows)}, nil
}
