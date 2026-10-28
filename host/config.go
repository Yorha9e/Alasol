package host

import (
	"fmt"
	"log/slog"
	"net"
	"strings"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/core/loop"
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
	// Approval 审批策略：deny-all | always-allow | terminal。
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
	// WorktreeRoot 多 worktree 隔离（v1-S5 D）的受管根目录（所有 worktree 建在其下）。
	// 空 = 派生自事件库目录（<DBPath 所在目录>/worktrees）。
	WorktreeRoot string
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
	case "", "deny-all", "always-allow", "terminal":
	default:
		return fmt.Errorf("host: Config.Approval 只支持 deny-all | always-allow | terminal（得到 %q）", c.Approval)
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
	return nil
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
