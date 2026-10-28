package loop

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
)

// DefaultMaxTurns 是 Config.MaxTurns 缺省值（0 = 用它）：模型失控时强制收口，
// 避免无限工具调用把会话/预算烧穿（M 系列护栏，防呆默认值）。
const DefaultMaxTurns = 20

// 事件 kind（core 定义核心子集）：三段写路径 + loop 自身的错误通道 + 对话入流观测。
//
// loop.error 不是工具调用失败（M1：失败是普通事件，不是特殊通道）——
// 它只记录"这一轮 loop 自己没能继续往下走"的事实（模型调用失败、intent 落盘失败）。
//
// user.prompt / model.turn 是 v1-S1 补 F1 缺口的**旁路观测**（对话内容进事件流，供回放/
// resume 重建完整对话历史）：best-effort 落库、不占 Result.Events 计数、不改变任何控制流，
// 与 X1 三段写路径刻意分离（见 events.go 的 publishUserPrompt / publishModelTurn）。
const (
	// KindToolCallIntent 三段之①：意图 durable 落盘后才允许执行（X1）。
	KindToolCallIntent = "tool.call.intent"
	// KindToolCallCompleted 三段之③a：执行成功，结果 durable 落盘（X1）。
	KindToolCallCompleted = "tool.call.completed"
	// KindToolCallFailed 三段之③b：deny / 执行失败 / 工具缺失，错误模型可读（X1/M5/M8）。
	KindToolCallFailed = "tool.call.failed"
	// KindLoopError loop 自身错误的 durable 事件（模型调用失败 / intent 落盘失败）。
	KindLoopError = "loop.error"
	// KindUserPrompt 用户 prompt 入事件流（F1：对话内容是事实源的一部分）。Run 开始时一条。
	KindUserPrompt = "user.prompt"
	// KindModelTurn 一轮 provider 返回的事实（F1，含 usage 只做成本记录，J21）。每轮一条。
	KindModelTurn = "model.turn"
)

// 失败分类码（M4：可重试/致命分类用的开放字符串；这里固化 loop 自己产生的几种）。
const (
	// CodePolicyDeny 策略判定未放行（deny / ask 无人应答 / 无命中 fail-close）。
	CodePolicyDeny = "policy_deny"
	// CodeToolNotFound 工具未在注册表注册（模型 hallucinate 了一个工具名）。
	CodeToolNotFound = "tool_not_found"
	// CodeExecutorMissing 注册实现不是 core.ToolExecutor（装配错误）。
	CodeExecutorMissing = "executor_missing"
	// CodeToolError 执行器返回 error 或 panic（一次普通失败，不是进程崩溃）。
	CodeToolError = "tool_error"
	// CodeBadArguments 工具参数不满足契约（不是合法 JSON 对象）→ 普通失败，模型可改。
	CodeBadArguments = "bad_arguments"
	// CodePermDenied 工具所需的权限维度超出本 agent 的**生效权限**（H11/H22 收窄）。
	CodePermDenied = "permission_denied"
	// CodeLoopError loop 自身出错（模型调用失败 / intent 落盘失败）。
	CodeLoopError = "loop_error"
)

// envCapabilityPrefix 是 env 键的策略查询前缀（H17：凭据按需注入也要过同一套求值，
// 与注册表能力裁决复用 policy.Engine，E3）。例：RequiredEnv=["GITHUB_TOKEN"]
// → Query{Tool: "env:GITHUB_TOKEN"}。
const envCapabilityPrefix = "env:"

// Config 是 Loop 的装配参数。
//
// 必填：Provider / Bus / Registry / Model / SessionID / AgentID。
// 可缺省（缺省后行为更严格，fail-close）：
//   - Policy 为 nil → 空引擎语义，一切工具调用无 allow 命中 → deny；
//   - Approver 为 nil → DenyAll（无人应答 = deny，H8/D14）；
//   - MaxTurns 为 0 → 用 DefaultMaxTurns。
type Config struct {
	// Provider 模型提供方（core.Provider；实现如 Anthropic/OpenAI 在 runtime）。
	Provider core.Provider
	// Bus 事件总线：三段事件经 Publish 落库 + 通知订阅者（内部持有 core.Store）。
	Bus *bus.Bus
	// Registry 扩展注册表：取工具描述（Entry.Tool）与执行器（Entry.Impl）。
	Registry *registry.Registry
	// Policy 策略引擎（nil = 空引擎 = 一切 deny，fail-close）。
	Policy *policy.Engine
	// Approver 人审（nil = DenyAll，fail-close；policy 判定为 ask 时才被问到）。
	Approver core.Approver
	// SessionID 会话 id（事件归属；必填合法 ULID，S1/S9）。
	SessionID contracts.Ulid
	// AgentID agent id（事件归属；必填合法 ULID，S1/S9）。
	AgentID contracts.Ulid
	// Model 模型名（provider 无关标识，原样传给 provider）。
	Model string
	// System 系统提示（每轮原样带上）。
	System string
	// SessionEnv session 级 env 注入（H21：与单次调用注入合并；值永不进事件/日志）。
	SessionEnv core.EnvScope
	// MaxTurns 单次 Run 的最大轮数（0 = DefaultMaxTurns；防模型失控）。
	MaxTurns int
	// ContextBudget 上下文窗口预算 token（0 = DefaultContextBudget）。只用于
	// context.assembled 事件里 ContextSnapshot.tokens_budget 的占比分母（J21），
	// v0 不据此做任何装配/压缩决策。
	ContextBudget int64
	// Agents 多 agent 环境（阶段 5）。nil = 单 agent 模式（默认，向后兼容）：
	// 不注入 spawn/complete、不用 agent 树、不过滤工具。
	Agents *Agents
	// Store 事件库句柄（可选，阶段 8 durable）：配置后 Loop.Checkpoint 与自动 checkpoint 才可用。
	// 它与 Bus 内部持有的 store 是同一个（Bus 已持 store 但不暴露，故此处显式传入以支持 checkpoint）。
	// nil = 不建 checkpoint（Checkpoint 手动调用返回错误、自动 checkpoint 跳过）。
	Store core.Store
	// CheckpointEvery 每 N 轮自动建一次 checkpoint（X3；0 = 只手动，需配置 Store 才生效）。
	// 自动 checkpoint 是**尽力而为**：落盘失败不中止 Run（Run 的事件本身已 durable，缺 checkpoint
	// 只意味着 resume 退到全量重建）。
	CheckpointEvery int
	// OnlineTable 在线表注入的活配置（K14/K15；nil = 关闭在线表注入——默认，向后兼容）。
	// 与 OnlineTableSource **同时非 nil** 才注入；它是 hub 建一个、所有 loop 共享的对象，
	// 运行期经它调整参数（不重启，下一轮生效）。
	OnlineTable *OnlineTableConfig
	// OnlineTableSource 在线表数据源（K11：self + gate 相交的对端主 agent；nil = 关闭注入）。
	OnlineTableSource OnlineTableSource
}

// Loop 是一次会话内的 agent loop 状态机。
//
// 并发：Loop 本身无可变状态（每轮历史、污点累积、spawn 缓存都是 Run 的局部变量）→ 同一个
// Loop 可被多个 goroutine 并发 Run（各自独立历史）。但"一个 session 只由其宿主进程 append"
// （G25，session 亲和写）是部署层约束：并发 Run 同一 session 时，事件 seq 是提交序而非因果序。
//
// 多 agent（阶段 5）：Agents 非 nil 时，本 loop 是其 AgentID 对应 agent 的一次执行；spawn 会
// 另起一个共享 Bus/Registry/Provider 的子 loop（child.AgentID）同步跑子任务。
type Loop struct {
	provider   core.Provider
	bus        *bus.Bus
	registry   *registry.Registry
	policy     *policy.Engine
	approver   core.Approver
	sessionID  contracts.Ulid
	agentID    contracts.Ulid
	model      string
	system     string
	sessionEnv core.EnvScope
	maxTurns   int
	agents     *Agents

	// contextBudget 是 ContextSnapshot.tokens_budget 的取值来源（占比分母，J21）。
	contextBudget int64

	// store / checkpointEvery 支撑阶段 8 durable：手动/自动 checkpoint 与 resume 重放的执行侧。
	store           core.Store
	checkpointEvery int

	// onlineConfig / onlineSource 支撑 v1-S4 在线表注入（K14/K15）。两者同时非 nil 才注入
	// （见 onlineTableForTurn）；nil = 关闭——老装配（cmd run/resume、单测）不受影响。
	// onlineConfig 是**共享活配置**（hub 建一个传给所有 loop，运行期可调）。
	onlineConfig *OnlineTableConfig
	onlineSource OnlineTableSource
}

// Result 是一次 Run 的产出。
type Result struct {
	// Text 模型最终文本（无工具调用那一轮的 Text；MaxTurns 截断时为最后一轮的 Text；
	// 子 agent 经 complete 收敛时为 summary.Text）。
	Text string
	// Turns 实际发生的模型调用轮数。
	Turns int
	// ToolCalls 本 Run 执行的工具调用次数（含被 deny / 失败的：模型发起了就算一次）。
	ToolCalls int
	// Events 本 Run 成功落库的 durable 事件数（每轮一条 context.assembled + intent +
	// completed/failed + loop.error + agent.spawned/completed/failed；
	// 子 loop 的事件计入父的 spawn 调用）。
	// **不含**旁路对话观测 user.prompt / model.turn（F1 的对话入流事实，v1-S1）：它们
	// best-effort 落库、独立于写路径与控制流，故不计入本计数（口径同 maybeCheckpoint 的
	// checkpoint.created——恢复/对话元数据不是 loop 一轮的写路径产出）。
	Events int
	// Summary 子 agent 经 complete 收敛时的结构化汇报（根 agent / 未收敛为 nil）。
	Summary *contracts.AgentSummary
	// Taint 本次 Run 累积的最高污点级（Q7；空 = clean）。父 agent 据其在子异常冒泡时标注
	// summary 的 taint；根 agent 上为信息量（v0 无根 summary）。
	Taint string
}

// New 构造 Loop 并校验必填项。
//
// nil 语义口径（与 core/doc.go 一致）：
//   - **兜底**（缺参数后系统仍能安全地"拒绝"）：Policy=nil → 空引擎 → 一切 deny；
//     Approver=nil → DenyAll；MaxTurns=0 → DefaultMaxTurns；
//   - **编程错误**（返回 error）：Provider / Bus / Registry / Model / SessionID /
//     AgentID 缺失或非法——没有它们连一次安全的模型调用都发不起来。
//
// ContextBudget<0 同样按编程错误拒绝（预算不能为负；0 = 用 DefaultContextBudget）。
func New(cfg Config) (*Loop, error) {
	if cfg.Provider == nil {
		return nil, errors.New("loop: Config.Provider 必填（模型提供方）")
	}
	if cfg.Bus == nil {
		return nil, errors.New("loop: Config.Bus 必填（三段事件要经它落库）")
	}
	if cfg.Registry == nil {
		return nil, errors.New("loop: Config.Registry 必填（工具描述与执行器的来源）")
	}
	if strings.TrimSpace(cfg.Model) == "" {
		return nil, errors.New("loop: Config.Model 必填")
	}
	if err := validateUlid("SessionID", cfg.SessionID); err != nil {
		return nil, err
	}
	if err := validateUlid("AgentID", cfg.AgentID); err != nil {
		return nil, err
	}
	if cfg.MaxTurns < 0 {
		return nil, fmt.Errorf("loop: Config.MaxTurns 不能为负（得到 %d；0 = 用默认 %d）", cfg.MaxTurns, DefaultMaxTurns)
	}
	if cfg.ContextBudget < 0 {
		return nil, fmt.Errorf("loop: Config.ContextBudget 不能为负（得到 %d；0 = 用默认 %d）", cfg.ContextBudget, DefaultContextBudget)
	}
	if cfg.CheckpointEvery < 0 {
		return nil, fmt.Errorf("loop: Config.CheckpointEvery 不能为负（得到 %d；0 = 只手动）", cfg.CheckpointEvery)
	}
	maxTurns := cfg.MaxTurns
	if maxTurns == 0 {
		maxTurns = DefaultMaxTurns
	}
	budget := cfg.ContextBudget
	if budget == 0 {
		budget = DefaultContextBudget
	}
	return &Loop{
		provider:        cfg.Provider,
		bus:             cfg.Bus,
		registry:        cfg.Registry,
		policy:          cfg.Policy,
		approver:        cfg.Approver,
		sessionID:       cfg.SessionID,
		agentID:         cfg.AgentID,
		model:           cfg.Model,
		system:          cfg.System,
		sessionEnv:      cfg.SessionEnv,
		maxTurns:        maxTurns,
		agents:          cfg.Agents,
		contextBudget:   budget,
		store:           cfg.Store,
		checkpointEvery: cfg.CheckpointEvery,
		onlineConfig:    cfg.OnlineTable,
		onlineSource:    cfg.OnlineTableSource,
	}, nil
}

// validateUlid 校验归属 id（S1/S9：不合则拒绝，不修复）。
func validateUlid(field string, id contracts.Ulid) error {
	if id == "" {
		return fmt.Errorf("loop: Config.%s 必填（事件归属）", field)
	}
	if !core.ValidUlid(id) {
		return fmt.Errorf("loop: Config.%s 不是合法 ULID（26 字符 Crockford base32）：%q", field, string(id))
	}
	return nil
}

// Run 跑一次 agent loop：从 prompt 开始，直到模型不再发起工具调用、
// 或达到 MaxTurns、或出错 / ctx 取消。
//
// 错误语义：
//   - ctx 取消 → 立即返回 ctx.Err()（不包装，调用方可用 errors.Is 判定）；
//   - 模型调用失败 → 返回包装后的 error（errors.Is 仍能取到原始错误），并尽力补一条
//     loop.error durable 事件；
//   - intent 事件落盘失败 → 返回 error 且**不执行**该工具调用（X1：没有意图记录就不许有副作用）；
//   - 工具被 deny / 执行失败 / 工具缺失 → **不是** Run 的错误：它们已按三段写路径落
//     failed 事件，错误文案回填给模型让它自我修正（M5），loop 继续。
//
// 每轮模型调用前还会落一条 context.assembled durable 事件（载荷 = contracts.ContextSnapshot，
// 见 core/loop/context.go）：上下文可见性的数据源（06 §4.2）。它是观测性事实，落库失败
// **不中止** loop（只补 loop.error 留痕）——与 X1 的 intent 写路径刻意区分。
//
// 从既有历史续跑用 RunWithHistory。
func (l *Loop) Run(ctx context.Context, prompt string) (Result, error) {
	return l.RunWithHistory(ctx, nil, prompt)
}

// RunWithHistory 带**既有历史**跑一次 agent loop（resume 续跑用）。
//
// history 是此前的对话历史（如 runtime.FoldConversation 从事件流 fold 出来的对话）：
// 它会**排在本次 prompt 之前**发给模型，本次对话接着它继续（历史本身不被改写——拷贝一份）。
//
// 与 Run 的差别只有一处：起始历史不是空的。控制流、事件写路径（X1 三段写路径一字不动）、
// 旁路观测（user.prompt / model.turn）全同。
//
// 为什么需要它（v1-S1）：对话内容进了事件流（F1）就能被回放重建，但"重建出来的历史"
// 必须能**喂回 loop** 才叫续跑——否则 FoldConversation 只是素材，不是能力。
func (l *Loop) RunWithHistory(ctx context.Context, history []core.Message, prompt string) (res Result, err error) {
	if l == nil {
		return Result{}, errors.New("loop: Loop 为 nil")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	// 每次 Run 的局部可变状态（不进 Loop 字段 → Loop 保持无可变状态以支持并发 Run）。
	st := &runState{}
	// 无论从哪条路径返回，都把本次累积的污点级写进 Result（Q7；空归一为 clean）。
	defer func() { res.Taint = normalizeTaint(st.acc.level) }()

	// 每轮历史都是局部变量：同一 Loop 可被并发 Run 而不互相污染。
	// 既有历史先**拷贝**一份（调用方可能复用那个切片 → 不能被本次 Run 改写）。
	msgs := make([]core.Message, 0, len(history)+8)
	msgs = append(msgs, history...)
	msgs = append(msgs, core.Message{Role: "user", Content: prompt})

	// Run 开始：把用户 prompt 落进事件流（F1：对话内容是事实源的一部分，回放/resume 据此
	// 重建完整对话）。旁路观测——best-effort、不计 Result.Events、不改控制流（见 events.go）。
	l.publishUserPrompt(ctx, prompt)

	var lastText string

	for turn := 1; turn <= l.maxTurns; turn++ {
		// ctx 取消优先于任何模型调用：立即返回，不再发起新副作用。
		if err := ctx.Err(); err != nil {
			res.Turns = turn - 1
			res.Text = lastText
			return res, err
		}
		res.Turns = turn

		// 在线表（K14/K15）：每轮新鲜渲染（秒级变动），仅主 agent。nil = 本轮不注入。
		ot := l.onlineTableForTurn(ctx)

		// 组这一轮的请求（可见工具过滤后的结果，R5/H4；在线表追加以最末尾，Q4/Q3）。
		req := l.request(msgs, ot)

		// 每轮模型调用**前**落一条 context.assembled（06 §4.2：上下文可见性的数据源）。
		// 此刻装配已定型（req 就是模型将看到的全部），记录"这一轮给模型看了什么"；
		// 落库失败不中止 loop（观测性事实，不是执行前提，见 publishContextAssembled）。
		res.Events += l.publishContextAssembled(ctx, turn, req, ot)

		mr, err := l.provider.Run(ctx, req, l.emit)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				// 取消导致的失败按取消处理（不要把"被取消"包装成模型错误）。
				res.Text = lastText
				return res, ctxErr
			}
			res.Events += l.publishLoopError(ctx, turn, "provider", err)
			res.Text = lastText
			return res, fmt.Errorf("loop: 第 %d 轮模型调用失败: %w", turn, err)
		}
		lastText = mr.Text

		// 每轮 provider 返回后落一条 model.turn（F1：模型响应入事件流；含 usage 只做成本记录，J21）。
		// 旁路观测——best-effort、不计 Result.Events、不改控制流（见 events.go）。在判定本轮是否
		// 收敛**之前**落，故收敛轮（最终答复）与工具调用轮都各有一条。
		l.publishModelTurn(ctx, turn, mr)

		// 模型不再发起工具调用 → 收敛，返回最终文本。
		if len(mr.ToolCalls) == 0 {
			res.Text = mr.Text
			return res, nil
		}

		// assistant 消息：ToolCallReq 原样入历史（provider id 黑盒，S4）。
		msgs = append(msgs, core.Message{
			Role:      "assistant",
			Content:   mr.Text,
			ToolCalls: mr.ToolCalls,
		})
		for _, tc := range mr.ToolCalls {
			res.ToolCalls++
			if l.isAgentTool(tc.Name) {
				// 多 agent 生命周期工具（spawn / complete）。
				text, events, stop, err := l.runAgentTool(ctx, turn, tc, st)
				res.Events += events
				if err != nil {
					res.Text = lastText
					return res, err
				}
				msgs = append(msgs, core.Message{Role: "tool", Content: text, ToolCallID: tc.ID})
				if stop != nil {
					// complete：本（子）loop 就此收敛，Result 带上结构化 summary。
					res.Summary = stop
					res.Text = stop.Text
					return res, nil
				}
				continue
			}
			text, events, err := l.runToolCall(ctx, turn, tc, st)
			res.Events += events
			if err != nil {
				// 写路径断裂（intent 没落盘 / ctx 取消）：中止，不编造结果。
				res.Text = lastText
				return res, err
			}
			// 工具结果回填（role="tool"，ToolCallID 原样，S4）。
			msgs = append(msgs, core.Message{
				Role:       "tool",
				Content:    text,
				ToolCallID: tc.ID,
			})
		}

		// 阶段 8：turn 末（所有工具调用已解析、无 in-flight）按需自动 checkpoint（X3）。
		// 尽力而为：落盘失败不中止 Run（Run 的事件已 durable，缺 checkpoint 只让 resume 退到全量重建）。
		l.maybeCheckpoint(ctx, turn)
	}

	// MaxTurns 截断：模型还在要工具但轮数用尽（防呆护栏生效）。
	res.Text = lastText
	return res, nil
}

// request 组装这一轮的模型请求（状态机第 1 步）。
//
// 可见工具 = registry 里 kind=tool、有 Tool 描述、且**策略未拒绝**的条目，
// Schema 原样传给模型（R5/R6：不暴露即不存在）。
//
// H4 可见集过滤：裸工具名被策略 deny → 移出模型可见集（模型看不到 = 不会去调用）。
// 这里只做**可见性**过滤；执行侧仍独立判定（M9：判定发生在调用时刻，判定只约束
// 发起时刻——可见集过滤不放宽任何执行侧检查）。
//
// ot 非空（在线表注入）时，把在线表消息**拷贝后追加到最末尾**（Q4/Q3：history 之后）。
// 拷贝是必须的——online table 只进本轮请求，不能写进调用方 history 的底层数组（下一轮还要
// 往 msgs 追加 assistant/工具消息）。ot 为 nil 时零拷贝（老路径一字不变）。
func (l *Loop) request(history []core.Message, ot *onlineTable) core.ModelRequest {
	msgs := history
	if ot != nil {
		msgs = make([]core.Message, len(history), len(history)+1)
		copy(msgs, history)
		msgs = append(msgs, ot.message)
	}
	return core.ModelRequest{
		Model:    l.model,
		System:   l.system,
		Messages: msgs,
		Tools:    l.visibleTools(),
	}
}

// visibleTools 取当前可见工具描述（注册顺序 = 模型看到的顺序，确定性）。
//
// 过滤口径（H4 字面）：**deny = 移出模型可见集**（裸工具名 deny）；
// ask 保持可见——审批回路要靠模型发起调用才走得通，人审发生在调用时刻。
//
// 多 agent（阶段 5）在此叠加两层：
//   - **type.Tools 过滤**（R5：不暴露即不存在）：若本 agent 在树中且有类型，则只有该类型
//     Tools 列内的注册工具可见（子 loop 的 tools=type.Tools）；
//   - **生命周期工具注入**：can_spawn 非空 → 注入 spawn；子 agent（depth>1）→ 注入 complete。
//     二者由类型可达性/深度控制（K20/K21），不经注册表/policy（三闸门才是授权）。
func (l *Loop) visibleTools() []contracts.Tool {
	var (
		typeFilter map[string]bool // 非 nil 时按类型 Tools 收窄注册工具
		canSpawn   bool
		isSubagent bool
	)
	if l.agents != nil && l.agents.Tree != nil {
		if node, ok := l.agents.Tree.Get(l.agentID); ok {
			isSubagent = node.Depth > 1
			if ct, ok := l.agents.Tree.TypeOf(l.agentID); ok {
				typeFilter = stringSet(ct.Tools)
				canSpawn = len(ct.CanSpawn) > 0
			}
		}
	}

	entries := l.registry.List(registry.ExtensionKindTool)
	out := make([]contracts.Tool, 0, len(entries)+2)
	for _, e := range entries {
		if e.Tool == nil {
			continue // 非工具扩展（kind 不匹配时已被 List 过滤；Tool 为 nil 是装配残缺）
		}
		if typeFilter != nil && !typeFilter[e.Tool.Name] {
			continue // R5：类型未列出的工具不暴露
		}
		if !l.toolVisible(e.Tool.Name) {
			continue // H4：被 deny 的工具移出模型可见集
		}
		out = append(out, *e.Tool)
	}

	// 生命周期工具注入（阶段 5）。它们**也要过策略**（P2-4：spawn/complete 不许绕过
	// 策略）：被 deny 的生命周期工具不进模型可见集。
	if l.agents != nil && l.agents.Tree != nil {
		if canSpawn && l.toolVisible(ToolSpawn) {
			out = append(out, spawnToolDesc())
		}
		if isSubagent && l.toolVisible(ToolComplete) {
			out = append(out, completeToolDesc())
		}
	}
	return out
}

// stringSet 把字符串列表转成集合（type.Tools 过滤用）。
func stringSet(list []string) map[string]bool {
	m := make(map[string]bool, len(list))
	for _, s := range list {
		m[s] = true
	}
	return m
}

// toolVisible 判定工具是否在模型可见集里（H4：裸工具名 deny = 移出可见集）。
//
// 只用 Evaluate（**不**走 Decide）：可见性过滤不该把人审叫起来——
// 询问发生在调用时刻（M9/D14）。ask / allow 都可见；只有 deny
// （含 fail-close 无命中、非法规则污染）才隐藏。
func (l *Loop) toolVisible(name string) bool {
	dec := l.policy.Evaluate(context.Background(), policy.Query{Tool: name})
	return dec.Verdict != contracts.EffectDeny
}

// emit 接收 provider 的流式事件。
//
// v0 **不转发、不落库**：model.delta 是 durable=false 的流事件（F8/陷阱 11：不入库、
// 不占 seq），转发性投递对订阅者不幂等；v0 只把 Provider.Run 的返回值（最终结果）
// 用于写路径。将来要实时 UI 时在这里 Publish 一条 Durable:false 的事件即可。
func (l *Loop) emit(core.ProviderEvent) {}

// nowRFC3339 返回 UTC 的 RFC 3339 时间戳（契约 ts 字段格式；与 policy 同口径）。
func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }
