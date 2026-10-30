package host

import (
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/compaction"
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/runtime/cap/sandbox"
	"github.com/Yorha9e/alasol/runtime/mcp"
)

// DefaultAddr 是 HTTP/WS 的默认监听地址（N5b：只监听 loopback）。
const DefaultAddr = "127.0.0.1:8720"

// DefaultMaxSessionRows 是 /api/sessions 一次最多返回的行数（K15：在线表成本随
// session 数线性增长 → 服务端就设上限，不只靠前端截断）。
const DefaultMaxSessionRows = 200

// DefaultAPIKeyEnv 是持有 API key 的环境变量名（只传名字，不传值）。
const DefaultAPIKeyEnv = "OPENAI_API_KEY"

// hubLog 是 hub 的告警出口（投影 catch-up 失败这类"不致命但要留痕"的事）。
var hubLog = slog.Default()

// Config 是 Hub 的装配参数（host 是唯一装配处，09）。
//
// 必填：DBPath（事件库路径）、Model（模型名）。
// 可缺省（缺省后行为更严格）：
//   - Addr 为空 → DefaultAddr（且必须解析成 loopback，否则 New 拒绝）；
//   - Approval 为 deny-all（fail-close：无人应答 = deny）；
//   - Allow 为空 → 一切工具 deny（fail-close）；
//   - Provider / Approver 为 nil → 按 Approval 选真实实现（测试可注入脚本 provider）。
type Config struct {
	// DBPath 事件库路径（SQLite 文件；":memory:" 仅测试用）。
	DBPath string
	// Addr 监听地址（形如 127.0.0.1:8720）。**只接受 loopback**（N5b）。
	Addr string
	// Model 模型名（原样传给 provider）。
	Model string
	// System 系统提示（每轮原样带上）。
	System string
	// MaxTurns 单次 Run 的最大轮数（0 = loop 默认 20）。
	MaxTurns int
	// ContextBudget 上下文窗口预算 token（0 = loop 默认；只用于占比分母）。
	ContextBudget int64
	// Allow 允许的工具名（生成 source=cli 的 allow 规则；空 = 一切 deny）。
	Allow []string
	// Ask 需要人审的工具名（生成 source=cli 的 ask 规则）。命中 ask 时会去问
	// Approver（-approval=terminal 即在终端提示）；审批缺席 = deny（fail-close，D14/H8）。
	Ask []string
	// Approval 审批策略：deny-all | always-allow | terminal | protocol。
	//
	// protocol（v3-S4，G1）= **协议审批器**：ask 策略命中时把 ApprovalRequest 经 WS 广播
	// （kind=approval.request）推给所有连接，挂起等 approval.respond 注入（HTTP
	// /api/approvals/{id}/respond 或 WS approval.respond）；**无人应答/超时 = deny**
	// （fail-close，D14/H8）。供 serve daemon 场景（无本进程 stdin，审批在远端 GUI）。
	Approval string
	// BaseURL provider 端点（空 = 官方）。原样传给 provider，不进事件/日志。
	BaseURL string
	// APIKey 是**已解析**的 provider 凭据（H15：cmd 装配时按 env>文件 收敛后填入）。
	// 优先于 APIKeyEnv 从 env 取的值；为空才回退 os.Getenv(APIKeyEnv)。
	// **永不进 argv / 事件 / 日志**（H12/H19）——host.Config 本身不得被整体日志化。
	APIKey string
	// APIKeyEnv 持有 API key 的**环境变量名**（缺省 OPENAI_API_KEY）。
	// 只传变量名：值由 host 在装配时从 env 取出直接交给 provider，
	// 永不进 argv / 事件 / 日志（H12/H19）。
	APIKeyEnv string
	// CWD 是 hub 级默认工作目录：自动 gate repo:<repo_id> 按它算（K12/S14）。
	// 空 = 用进程 cwd。per-session 覆盖用 Hub.CreateSession(SessionSpec{CWD: ...})。
	CWD string
	// RunID 是 hub 级默认工作流运行 id（K12：有则自动加入 workflow:<run_id>）。
	// 空 = 不加 workflow gate。工作流引擎在 v3，这里只留钩子。
	RunID string
	// ReasoningEffort 是模型思维链强度档位（G14；空 = **不传**给 provider，端点用默认档）。
	// 原样进每轮 ModelRequest.ReasoningEffort → provider 透传 reasoning_effort 给端点。
	// 取值是网关支持的档位（low/medium/high/xhigh）——这里做**白名单校验**（fail-close：
	// 打错的档位在启动期就报错，而不是在一轮模型调用里拿到一个离现场很远的 400）。
	// 网关新增档位时改 ValidateReasoningEffort 一处即可。
	ReasoningEffort string
	// Provider 覆盖（测试注入脚本 provider；nil = openai 真实实现）。
	Provider core.Provider
	// Approver 覆盖（测试注入；nil = 按 Approval 选）。
	Approver core.Approver
	// IndexerBatch 索引器单批处理上限（<=0 = 索引器默认）。
	IndexerBatch int
	// MaxSessionRows /api/sessions 的行数上限（<=0 = DefaultMaxSessionRows）。
	MaxSessionRows int
	// 在线表注入（K14/K15）：hub 起动时的初始值。0/"" = 用 loop 默认（N=50 / 每行 ≤100 token /
	// summary）。运行期可经 GET/POST /api/online-table/config 动态调整（不重启，下一轮生效）。
	OnlineTableRows       int
	OnlineTableLineTokens int
	OnlineTableLevel      string
	OnlineTableWindow     int
	// Monitor hub 级 CPU/内存监控（K9）阈值。0 = 用框架默认（见 monitor.go）。
	// 周期采样，超阈值记告警（日志 + 内存环 + /api/monitor，可查）。
	MonitorCPUPercent float64
	MonitorMemMB      float64
	MonitorGoroutines int
	// MonitorIntervalSec 采样周期（秒；0 = 框架默认）。
	MonitorIntervalSec int
	// MonitorSampleSource 覆盖监控采样源（测试注入：用假源精确控制"何时超阈值"，
	// 不真烧 CPU；nil = defaultSampleSource 真实采样）。与 Config.Provider 同性质的注入点。
	MonitorSampleSource SampleSource
	// WorktreeRoot 多 worktree 隔离（v1-S5 D）的受管根目录（所有 worktree 建在其下）。
	// 空 = 派生自事件库目录（<DBPath 所在目录>/worktrees）。
	WorktreeRoot string
	// ContextStrategy 上下文装配策略（v2-S1；J8 context_strategy）："" = append（默认，v1 等价）
	// | "tiered" = 级联压缩（core/compaction 的两级 L1/L2 + recall + 留存降级）。opt-in 预备单位。
	ContextStrategy string
	// ContextStrategyWindow tiered 策略的窗口预算 W（token；0 = 默认 1M）。须 ≥ 256k 硬约束
	// （13 §8），不满足则 New 拒绝装配并说明。
	//
	// **同时是 context.assembled 的 tokens_budget 分母**（F10/F13：两者必须同源——否则
	// 挂 tiered 时 UI 的占比是拿 200k 默认分母算的，会显示成几百个百分点）。
	ContextStrategyWindow int64
	// ContextStrategyL1Pct tiered 策略的 L1（浅摘要）目标残留占窗口百分比（0 = 默认 10）。
	ContextStrategyL1Pct float64
	// ContextStrategyL2Pct tiered 策略的 L2（深压缩）目标残留占窗口百分比（0 = 默认 1）。
	ContextStrategyL2Pct float64
	// ContextStrategyRulesBudget 项目规则注入的 token 上限（0 = 布局默认 15k）。
	// 规则是**不可压缩**的注入内容：没上限时一条超长规则就能顶穿窗口（13 §8 前缀预算）。
	ContextStrategyRulesBudget int64
	// ContextStrategyCommonSenseBudget 常识区注入的 token 上限（0 = 布局默认 30k）。
	ContextStrategyCommonSenseBudget int64
	// ContextRules 项目规则文本（AGENTS.md 等；稳定前缀的一部分，Q2/Q3）。空 = 不注入。
	// 注入面：宿主装配 tiered 引擎时调 SetRules（core/compaction 的规则注入口）。
	ContextRules string

	// ---- v2-S2 图投影（G2f/G18/G19/G20/G21）----
	//
	// Graph 是否启用图投影。**默认 false = 零图开销**（不建引擎、不建图表、
	// 索引器不解析工具产出的边）——图是可选投影，不进 core 必经路径（G2f）。
	// 由工作流/配置声明是否启用（v3 工作流引擎接管这个开关，这里先落宿主配置面）。
	Graph bool
	// GraphMaxNodes / GraphMaxEdges 是内存图的上界（G19：10 万节点/100 万边
	// ~100–200MB 量级，可配；0 = 用框架默认）。触界即停并报错（fail-close：
	// 不返回可能不完整的影响面）。
	GraphMaxNodes int
	GraphMaxEdges int
	// GraphMaxDepth / GraphMaxResults 是多跳遍历的深度/结果上限（G18；0 = 默认）。
	GraphMaxDepth   int
	GraphMaxResults int
	// GraphCoreNodes 是"核心接口"节点声明（W16 第二个客观依据：改动是否触及核心接口）。
	// 形态：精确 id，或以 `*` 结尾的字节前缀（如 "core/*"）。**大小写敏感**（陷阱 17）。
	GraphCoreNodes []string
	// GraphEscalateReachable 是 change_impact 的 escalate 阈值（可达节点数超过它才触发；
	// 0 = 默认 50）。W15：风险闸门，不是预算上限。
	GraphEscalateReachable int

	// ---- v2-S4 沙箱（K8/H9）----
	//
	// Sandbox 是否把工具执行环境从 LocalExecutionEnv 换成 SandboxedExecutionEnv
	// （runtime/cap/sandbox）。**默认 false = 与 v1 等价**（走 runtime/cap/local）。
	//
	// 形态对齐 Config.Graph（v2-S2）：一个 opt-in 布尔开关，默认关、零开销。
	// K8 的酸测试就在这儿——换 env 实现**不动 agent 逻辑/工具代码**，只换装配。
	Sandbox bool
	// SandboxWriteDirs 是沙箱的"允许写根"集合（空 = 只用 CWD + 沙箱自管临时根）。
	//
	// 语义见 sandbox.Policy.AllowedRoots（写与读分开说）：
	// 根之外**写**被内核拒绝（除 BUILTIN\Users 有写权限的位置，默认模式下的已知残留）；
	// 根之外**用户私有文件**（.env/.ssh）读不到，系统位置默认仍可读。
	SandboxWriteDirs []string
	// SandboxMaxActiveProcesses 是 job object 的活动进程上限（0 = 框架默认 64）。
	// fork 炸弹的有界化：进程数有上限，最坏情况才是有界的最坏情况。
	SandboxMaxActiveProcesses int
	// SandboxACLMaxObjects 是给允许根施加 ACL 前数子树对象的上限（0 = 框架默认）。
	// 防止允许根被误配成 `C:\` 这种大根——可继承 ACE 会传播到既有子对象。
	SandboxACLMaxObjects int
	// SandboxStrictGroups 是否启用**严格组 SID 模式**（把 BUILTIN 的 Users SID 也
	// deny-only）。见 sandbox.Policy.StrictGroupSIDs 的取舍说明：严格模式堵上
	// "只授予 Users 写权限的目录"（典型是 C:\ProgramData），代价是用 overlapped I/O
	// 写文件的程序（Go/.NET/Rust/Node）会写不进去。
	SandboxStrictGroups bool

	// ---- v3-S1 MCP client（N1：client 先）----
	//
	// MCP 是否装配外部 server 的工具。**默认零值 = 不装**：不连任何外部进程、
	// 不注册任何工具，与 v2 行为完全一致（酸测试不回归）。
	//
	// 口径与 Config.Graph / Config.Sandbox 同形态：opt-in 配置面 + 默认关 + 零开销。
	// 装配后的工具走**既有管线**（同一个 registry 入口 → 同一条 X1 三段写路径 →
	// 同一套策略链与 R5 可见性），core/loop 一行不改。
	//
	// 安全口径（写全，免得后人误以为"MCP 工具自动可用"）：
	//   - **deny-all 默认**：策略引擎没有 allow 命中就拒绝（H5 fail-close）。
	//     要放行得显式 `allow = ["mcp__<server>__<tool>"]`（或 "*"）；
	//   - **R5 不暴露即不存在**：agent 类型的 tools[] 白名单没列它 → 模型看不见；
	//   - **不给特权**：MCP 工具的描述比内置工具更保守（side_effect=write /
	//     approvable=true / idempotent=false / replay=unsafe）；
	//   - **凭据纪律**：MCP.Servers 里的 Env/Headers 可能含 server 凭据，
	//     **永不进事件 / 日志 / 错误文案**（H12/H19）。host.Config 本身不得被
	//     整体日志化；要打印用 MCP.Redacted()。
	MCP mcp.Config

	// ---- v3-S3 工作流引擎（W1 能力包，非强制）----
	//
	// WorkflowDir 是工作流**能力包**目录（W13：纯数据 JSON，可被任何语言写/组合/追加）。
	// 目录下每个 `*.json` 是一个 contracts.Workflow；空 = 只用内建包（tower）。内建包总在。
	//
	// **能力包不是流程**（W1/W2）：装载/激活只把能力包的哲学文本注入 system prompt 供主
	// agent 选择（W3），框架**不校验**编排序列（W21）。不挂任何工作流 = 零行为变化（酸测试）。
	WorkflowDir string
	// GoalLedger 是否给 session 的 loop 注入 goal ledger 更新工具（J19/X6；默认 false =
	// 零行为变化，酸测试）。true 时 agent 可在关键点更新 goal ledger（关键字段立即落盘、
	// 过程字段 turn 末合并）。仍受策略链（deny-all 默认）与 agent 类型白名单约束。
	GoalLedger bool
	// PromptRunTimeoutSec 是协议面发起的**异步 prompt**（POST /api/sessions/{id}/prompt /
	// WS command.submit）的生命周期上限（秒；0 = 默认 600）。
	//
	// 口径（别读成别的）：这是**一轮**（一个 prompt 的全部 turn）的**整轮**上限，
	// **不是**单次模型调用的超时，也**不是**单次审批等待的超时——一轮里可以有多次审批
	// 等待，它们共用这份预算（预算耗尽 → 后续审批立即按 deny 收场）。
	//
	// 它同时是 fail-close 的兜底时钟：协议 prompt 跑在后台 goroutine，一个卡在审批
	// （无人应答）的 loop 若无上界会永久挂起；有了它，审批等到超时就按 deny 收场
	// （契约：无人应答 = deny，D14/H8）。
	//
	// 可配：cmd 侧 `-prompt-timeout <秒>`（0 = 用默认 600）。
	PromptRunTimeoutSec int

	// ---- v3-S6-B 多 agent 装配（W5：内建五类 + TOML 自定义；opt-in）----
	//
	// MultiAgent 是否给每个 session 装一棵 agent 树 + 把 loop.Agents 接进 loop.Config
	// （spawn/complete 对模型可见、子 agent 可派生）。**默认 false = 单 agent 模式零行为变化**
	// （酸测试）：不建树、不传 Agents、不自动补 allow 规则——已有装配（cmd/alasol 未改
	// multi_agent）与单测完全不受影响。只有显式开（TOML `multi_agent = true`）才走装配。
	//
	// 开 multi_agent = **预期主 agent 能 spawn**：hub 装配策略时会自动补两条 allow 规则
	// （spawn / complete，source=cli/auto_multi_agent，优先级最低）让这两个框架工具对模型
	// 可见；策略链照走——**deny 规则仍可拦**（P2-4：deny 永远赢，用户/项目/工作流的显式
	// deny 不受自动 allow 影响）。子 agent 的 spawn 三闸门（深度/类型/权限）在 core 运行时强制。
	MultiAgent bool
	// AgentTypes 是**用户自定义**的 agent 类型（TOML `[[agent_types]]` 装载，W5 扩展点）。
	// 与内建五类（planner/coder/critic/reviewer/observer）合并进同一个类型注册表；
	// **同名冲突时 TOML 覆盖内建**（用户自定义优先，见 host/agenttypes.go 的装配）。
	// **只在 MultiAgent=true 时生效**：关 = 不建注册表、**也不校验**（零行为变化——用一个
	// 根本用不到的扩展点不许拒绝宿主启动；口径见 host/hub.go 的 New 装配处）。
	AgentTypes []contracts.AgentType
}

// validate 校验并归一化配置。
func (c *Config) validate() error {
	if strings.TrimSpace(c.DBPath) == "" {
		return fmt.Errorf("host: Config.DBPath 必填（事件库路径）")
	}
	if strings.TrimSpace(c.Model) == "" {
		return fmt.Errorf("host: Config.Model 必填")
	}
	addr := strings.TrimSpace(c.Addr)
	if addr == "" {
		addr = DefaultAddr
	}
	if err := checkLoopback(addr); err != nil {
		return err
	}
	c.Addr = addr
	switch c.Approval {
	case "", "deny-all", "always-allow", "terminal", "protocol":
	default:
		return fmt.Errorf("host: Config.Approval 只支持 deny-all | always-allow | terminal | protocol（得到 %q）", c.Approval)
	}
	if c.MaxTurns < 0 {
		return fmt.Errorf("host: Config.MaxTurns 不能为负（得到 %d）", c.MaxTurns)
	}
	if c.ContextBudget < 0 {
		return fmt.Errorf("host: Config.ContextBudget 不能为负（得到 %d）", c.ContextBudget)
	}
	if c.MaxSessionRows <= 0 {
		c.MaxSessionRows = DefaultMaxSessionRows
	}
	// 在线表注入（K14/K15）起动配置：负值拒绝；level 只支持 summary | n | full。
	if c.OnlineTableRows < 0 {
		return fmt.Errorf("host: Config.OnlineTableRows 不能为负（得到 %d；0 = 默认 %d）", c.OnlineTableRows, loop.DefaultOnlineTableRows)
	}
	if c.OnlineTableLineTokens < 0 {
		return fmt.Errorf("host: Config.OnlineTableLineTokens 不能为负（得到 %d；0 = 默认 %d）", c.OnlineTableLineTokens, loop.DefaultLineTokenBudget)
	}
	if c.OnlineTableWindow < 0 {
		return fmt.Errorf("host: Config.OnlineTableWindow 不能为负（得到 %d；0 = 默认 %d）", c.OnlineTableWindow, loop.DefaultWindowLines)
	}
	if lvl := strings.TrimSpace(c.OnlineTableLevel); lvl != "" {
		switch lvl {
		case loop.LevelSummary, loop.LevelN, loop.LevelFull:
		default:
			return fmt.Errorf("host: Config.OnlineTableLevel 只支持 summary | n | full（得到 %q）", c.OnlineTableLevel)
		}
	}
	// RunID 拼成 workflow:<run_id> 后必须是个合法 gate id（S9：不合则拒，不修复）。
	if runID := strings.TrimSpace(c.RunID); runID != "" &&
		!discovery.ValidGateID(discovery.WorkflowGate(runID)) {
		return fmt.Errorf("host: Config.RunID %q 不合法（含空白/控制字符，或拼成 workflow gate 后超过 %d 字符）",
			c.RunID, discovery.MaxGateIDLen)
	}
	// 思维链档位（G14）：空 = 不传（端点默认档）；非空必须落在已知档位集内
	// （打错的档位启动期就拒，别拖到模型调用里 400）。
	if err := ValidateReasoningEffort(c.ReasoningEffort); err != nil {
		return err
	}
	// hub 监控阈值（K9）：负值拒绝；0 = 用框架默认。
	if c.MonitorCPUPercent < 0 {
		return fmt.Errorf("host: Config.MonitorCPUPercent 不能为负（得到 %v；0 = 默认）", c.MonitorCPUPercent)
	}
	if c.MonitorMemMB < 0 {
		return fmt.Errorf("host: Config.MonitorMemMB 不能为负（得到 %v；0 = 默认）", c.MonitorMemMB)
	}
	if c.MonitorGoroutines < 0 {
		return fmt.Errorf("host: Config.MonitorGoroutines 不能为负（得到 %d；0 = 默认）", c.MonitorGoroutines)
	}
	if c.MonitorIntervalSec < 0 {
		return fmt.Errorf("host: Config.MonitorIntervalSec 不能为负（得到 %d；0 = 默认）", c.MonitorIntervalSec)
	}
	// 上下文策略（v2-S1）：只支持 ""（append 默认）| "tiered"。窗口 W 负值拒绝；
	// tiered 时 W（0=默认 1M）须过 compaction 的 W≥256k 硬约束（否则 loopFor 装配期拒绝）。
	//
	// 取值按 **TrimSpace 后**比较（与装配处同一口径，F23：校验用 " tiered " 通过、
	// 装配用原值比较不通过 → 静默退回 append）。
	switch strings.TrimSpace(c.ContextStrategy) {
	case "", contextStrategyTiered:
	default:
		return fmt.Errorf("host: Config.ContextStrategy 只支持 %q（默认 append）| %q（得到 %q）",
			"", contextStrategyTiered, c.ContextStrategy)
	}
	if c.ContextStrategyWindow < 0 {
		return fmt.Errorf("host: Config.ContextStrategyWindow 不能为负（得到 %d；0 = 默认 1M）", c.ContextStrategyWindow)
	}
	if c.ContextStrategyRulesBudget < 0 {
		return fmt.Errorf("host: Config.ContextStrategyRulesBudget 不能为负（得到 %d；0 = 布局默认 15k）", c.ContextStrategyRulesBudget)
	}
	if c.ContextStrategyCommonSenseBudget < 0 {
		return fmt.Errorf("host: Config.ContextStrategyCommonSenseBudget 不能为负（得到 %d；0 = 布局默认 30k）", c.ContextStrategyCommonSenseBudget)
	}
	if c.ContextStrategyL1Pct < 0 || c.ContextStrategyL1Pct > 100 {
		return fmt.Errorf("host: Config.ContextStrategyL1Pct 必须在 (0,100] 内（得到 %v；0 = 默认 10）", c.ContextStrategyL1Pct)
	}
	if c.ContextStrategyL2Pct < 0 || c.ContextStrategyL2Pct > 100 {
		return fmt.Errorf("host: Config.ContextStrategyL2Pct 必须在 (0,100] 内（得到 %v；0 = 默认 1）", c.ContextStrategyL2Pct)
	}
	// L2 必须比 L1 更深（目标残留更小）：否则"两级压缩"退化成一级。
	//
	// 比较用**归一化后的有效值**（0 = 默认 10% / 1%）：拿原始值比会漏掉"L1 用默认、L2 配成
	// 50%"这种组合——那正是"更深的反而更宽"的退化配置。
	l1 := c.ContextStrategyL1Pct
	if l1 <= 0 {
		l1 = compaction.DefaultL1TargetPct
	}
	l2 := c.ContextStrategyL2Pct
	if l2 <= 0 {
		l2 = compaction.DefaultL2TargetPct
	}
	if l2 > l1 {
		return fmt.Errorf("host: Config.ContextStrategyL2Pct（%v）必须 ≤ ContextStrategyL1Pct（%v）：L2 是更深的一级",
			l2, l1)
	}
	// W 的硬约束**前移到启动期**（13 §8：W ≥ 256k）：tiered + W 过小 → 启动即拒绝，
	// 而不是拖到每个 session 请求才失败（那时错误信息离配置现场太远）。
	//
	// 顺带校验**布局自洽**（历史区预算不得为负）：注入区预算配得比窗口还大时，
	// 历史区就没了份额 → 装配期拒绝（负预算地雷不许带进运行期）。
	if strings.TrimSpace(c.ContextStrategy) == contextStrategyTiered {
		p := c.contextStrategyParams()
		if err := compaction.ValidateWindow(p.Window); err != nil {
			return fmt.Errorf("host: tiered 策略的窗口不合法: %w", err)
		}
		if err := compaction.ValidateLayout(p); err != nil {
			return fmt.Errorf("host: tiered 策略的布局不合法: %w", err)
		}
	}
	// 图投影（v2-S2）上限：负值拒绝，0 = 框架默认（G18/G19）。
	if c.GraphMaxNodes < 0 {
		return fmt.Errorf("host: Config.GraphMaxNodes 不能为负（得到 %d；0 = 框架默认）", c.GraphMaxNodes)
	}
	if c.GraphMaxEdges < 0 {
		return fmt.Errorf("host: Config.GraphMaxEdges 不能为负（得到 %d；0 = 框架默认）", c.GraphMaxEdges)
	}
	if c.GraphMaxDepth < 0 {
		return fmt.Errorf("host: Config.GraphMaxDepth 不能为负（得到 %d；0 = 框架默认）", c.GraphMaxDepth)
	}
	if c.GraphMaxResults < 0 {
		return fmt.Errorf("host: Config.GraphMaxResults 不能为负（得到 %d；0 = 框架默认）", c.GraphMaxResults)
	}
	if c.GraphEscalateReachable < 0 {
		return fmt.Errorf("host: Config.GraphEscalateReachable 不能为负（得到 %d；0 = 框架默认）", c.GraphEscalateReachable)
	}
	// 沙箱（v2-S4，K8/H9）：负值拒绝；0 = 框架默认。允许根必须是绝对路径
	// （sandbox.Policy.Validate 会再校一遍，这里先给一句宿主层的早错）。
	if c.SandboxMaxActiveProcesses < 0 {
		return fmt.Errorf("host: Config.SandboxMaxActiveProcesses 不能为负（得到 %d；0 = 框架默认）", c.SandboxMaxActiveProcesses)
	}
	if c.SandboxACLMaxObjects < 0 {
		return fmt.Errorf("host: Config.SandboxACLMaxObjects 不能为负（得到 %d；0 = 框架默认）", c.SandboxACLMaxObjects)
	}
	for _, d := range c.SandboxWriteDirs {
		if strings.TrimSpace(d) == "" {
			return fmt.Errorf("host: Config.SandboxWriteDirs 含空路径")
		}
		if !filepath.IsAbs(d) {
			return fmt.Errorf("host: Config.SandboxWriteDirs 必须是绝对路径（得到 %q）", d)
		}
	}
	// MCP client（v3-S1）：配置面校验**前移到启动期**（与 tiered 窗口同一纪律）——
	// 坏配置（server 名重复 / transport 缺 command 或 url / 传输类型不支持）
	// 在 host.New 就炸，而不是拖到装配期或更糟地静默跳过。
	if err := c.MCP.Validate(); err != nil {
		return fmt.Errorf("host: Config.MCP: %w", err)
	}
	// 协议 prompt 超时（v3-S4，G1）：负值拒绝；0 = 用框架默认。
	if c.PromptRunTimeoutSec < 0 {
		return fmt.Errorf("host: Config.PromptRunTimeoutSec 不能为负（得到 %d；0 = 默认）", c.PromptRunTimeoutSec)
	}
	return nil
}

// sandboxPolicy 把宿主配置折成 sandbox.Policy（**唯一装配处**：配置校验与沙箱工厂共用，
// 避免两处口径漂移——与 contextStrategyParams / graphLimits 同一纪律）。
//
// 口径本身在 host/sandbox.go 的 SandboxPolicy 里（cmd/alasol 的 run/resume 也调它），
// 这里只负责把 Config 的字段搬过去。
func (c *Config) sandboxPolicy() sandbox.Policy {
	return SandboxPolicy(SandboxOptions{
		WriteDirs:          c.SandboxWriteDirs,
		CWD:                c.CWD,
		MaxActiveProcesses: c.SandboxMaxActiveProcesses,
		ACLMaxObjects:      c.SandboxACLMaxObjects,
		StrictGroups:       c.SandboxStrictGroups,
	})
}

// contextStrategyParams 把宿主配置折成 compaction.Params（**唯一装配处**：配置校验与
// 策略工厂共用，避免两处口径漂移）。
func (c *Config) contextStrategyParams() compaction.Params {
	p := compaction.DefaultParams()
	if c.ContextStrategyWindow > 0 {
		p.Window = c.ContextStrategyWindow
	}
	if c.ContextStrategyL1Pct > 0 {
		p.L1TargetPct = c.ContextStrategyL1Pct
	}
	if c.ContextStrategyL2Pct > 0 {
		p.L2TargetPct = c.ContextStrategyL2Pct
	}
	if c.ContextStrategyRulesBudget > 0 {
		p.RulesBudget = c.ContextStrategyRulesBudget
	}
	if c.ContextStrategyCommonSenseBudget > 0 {
		p.CommonSenseBudget = c.ContextStrategyCommonSenseBudget
	}
	return p
}

// checkLoopback 强制监听地址是 loopback（N5b/O7）。
//
// fail-close 的理由：非 loopback 监听 = 把 hub 暴露给网络上任何主机，而 v0 的凭证
// 方案（无 cookie、无 token）**完全不设防**。允许它比拒绝它危险得多，所以这里直接
// 报错，不给"我知道我在干什么"的开关——远程化是 N4b 那六条一起的事，不是改个地址。
func checkLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("host: Addr %q 不是 host:port 形式: %w", addr, err)
	}
	if isLoopbackHost(host) {
		return nil
	}
	return fmt.Errorf("host: 拒绝非 loopback 监听地址 %q（N5b：v0 只监听 loopback + 无 cookie，"+
		"非 loopback 需要 N4b 的凭证/撤销/降权视图/审计/TLS 一整套）", addr)
}

// isLoopbackHost 判定 host 部分是否 loopback（N5b/O7：v0 只监听本机）。
//
// **只认 IP 与两个名字特例**：
//
//   - IP：net.ParseIP 解析成功 → ip.IsLoopback()（127.0.0.0/8 整段 + ::1）；
//   - 名字特例：localhost / ip6-localhost（**大小写不敏感**："LocalHost" 与
//     "localhost" 等价——DNS 名字本身大小写不敏感）。
//
// **其余任何非 IP 主机名一律拒绝**：字符串前缀匹配（"127."）会把
// "127.0.0.1.evil.example"、"127.0.0.1.nip.io" 这类**域名**放行（它们解析到
// 127.0.0.1，实测真能绑上监听）——而 v0 的安全模型是"loopback + 无凭据"，
// 被域名绕过就整体失效。v0 只监听本机，不放开域名。
func isLoopbackHost(host string) bool {
	h := strings.TrimSpace(host)
	// IP 判定（IPv6 字面量的方括号一并剥掉：SplitHostPort 通常已剥，这里兜底）。
	if ip := net.ParseIP(strings.Trim(h, "[]")); ip != nil {
		return ip.IsLoopback()
	}
	// 名字特例（大小写不敏感）。
	switch strings.ToLower(h) {
	case "localhost", "ip6-localhost":
		return true
	}
	return false
}

// onlineTableOverrides 把 Config 的在线表初始值折成 loop.OnlineTableUpdate（K15：hub 起动时可配）。
//
// 0/"" = 不给对应字段 → Update 不动它 → 保持 loop 默认（"缺省走配置面"口径）。非零/非空才覆盖。
func (c *Config) onlineTableOverrides() loop.OnlineTableUpdate {
	var u loop.OnlineTableUpdate
	if c.OnlineTableRows > 0 {
		u.MaxRows = &c.OnlineTableRows
	}
	if c.OnlineTableLineTokens > 0 {
		u.LineTokenBudget = &c.OnlineTableLineTokens
	}
	if lvl := strings.TrimSpace(c.OnlineTableLevel); lvl != "" {
		u.Level = &lvl
	}
	if c.OnlineTableWindow > 0 {
		u.WindowLines = &c.OnlineTableWindow
	}
	return u
}

// 已知的思维链档位集（G14）。口径与 Config.ReasoningEffort 的注释一致：
// low/medium/high 是 OpenAI o 系与 DeepSeek 都认的档位；xhigh 是部分网关的加码档。
// 网关新增档位 → 在这里加一个常量 + 进 knownReasoningEfforts，别处不用改。
const (
	ReasoningEffortLow    = "low"
	ReasoningEffortMedium = "medium"
	ReasoningEffortHigh   = "high"
	ReasoningEffortXHigh  = "xhigh"
)

// knownReasoningEfforts 是档位白名单（ValidateReasoningEffort 的唯一事实源）。
var knownReasoningEfforts = map[string]bool{
	ReasoningEffortLow:    true,
	ReasoningEffortMedium: true,
	ReasoningEffortHigh:   true,
	ReasoningEffortXHigh:  true,
}

// ValidateReasoningEffort 校验思维链档位（G14）：空（trim 后）= 不传，合法；
// 非空必须落在白名单内（**大小写敏感**，陷阱 17：档位是协议面的枚举值，不做大小写折叠——
// "High" 与 "high" 对端点不是同一个值，静默改写用户的输入比拒绝更危险）。
//
// 为什么fail-close 到白名单而不是"非空即放行"：打错的档位（typo、别的厂商的私有值）
// 放行出去只会在**一轮模型调用**里被端点 400，错误信息离配置现场很远；启动期拒绝
// 能把"哪个配置值错了"直接摆到使用者面前。
func ValidateReasoningEffort(v string) error {
	eff := strings.TrimSpace(v)
	if eff == "" {
		return nil // 空 = 不传（端点默认档）
	}
	if !knownReasoningEfforts[eff] {
		return fmt.Errorf("host: ReasoningEffort %q 不是已知档位（支持: low | medium | high | xhigh；空=不传）", v)
	}
	return nil
}

// ReasoningEffortValue 归一化档位值（trim；空 = 不传）。装配处（cmd）与宿主共用，
// 避免"flag 传了 ' high '、provider 收到 ' high '"这种口径漂移。
func ReasoningEffortValue(v string) string { return strings.TrimSpace(v) }

// ResolveReasoningEffort 解析思维链档位（**三路径同口径的唯一入口**：run / resume /
// serve 都在这儿取值 + 校验）。
//
// 取值口径：显式 flag > 配置文件 > 空（空 = 不传，端点用默认档）。
//
// 校验口径：白名单（唯一事实源 = ValidateReasoningEffort）。**fail-close**：打错的档位
// 在**启动期**就报错，而不是拖到一轮模型调用里拿到一个离配置现场很远的 400。
//
// 为什么放在 host（而不是各 cmd 自己判）：三条装配路径（alasol run / alasol resume /
// alasol serve）必须**同口径**——同一份配置在 serve 下被拒、在 run 下却能跑起来，
// 是最坑人的那种不一致。
func ResolveReasoningEffort(flagVal, fileVal string) (string, error) {
	v := ReasoningEffortValue(flagVal)
	if v == "" {
		v = ReasoningEffortValue(fileVal)
	}
	if err := ValidateReasoningEffort(v); err != nil {
		return "", err
	}
	return v, nil
}
