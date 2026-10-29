package host

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/Yorha9e/alasol/runtime/mcp"
)

// 本文件实现 H15 的**用户级 TOML 配置**：凭据来源 = 进程 env + TOML 配置文件
// （Windows %APPDATA%\alasol\config.toml，其他平台 XDG），**优先级 env > 文件**，
// 凭据永不进 prompt/事件/日志（H18/H21 脱敏纪律）。
//
// 依赖取舍（BurntSushi/toml）：选它而非自写最小解析——TOML 表面简单实则暗坑繁多
// （转义/多行串/日期时间/点分键/数组表），而本文件**携带凭据**，解析错误 = 静默读错
// 默认值或凭据，是正确性+安全性双重隐患；BurntSushi/toml 是 Go 生态事实标准、
// 零传递依赖（纯标准库）、久经审计。自写解析省一个依赖却引入一整类难测的 bug 面，不値。

// FileConfig 是用户级 TOML 配置的形状（H15）。所有字段可缺省（缺省走 env / 框架默认）。
//
// 脱敏纪律：本结构含 **APIKey（凭据）**——绝不整体日志化/序列化进事件。需要打印时用
// Redacted()（凭据打码）。host.Config 也不得被整体日志化（同理）。
type FileConfig struct {
	// BaseURL provider 端点（空 = 官方）。
	BaseURL string `toml:"base_url"`
	// Model 模型名（端点相关，无万能默认；flag/env 优先，双空 fail-close）。
	Model string `toml:"model"`
	// APIKey 凭据直填值（**env 优先**：同名 env 有值则 env 赢，本字段被忽略）。
	// 永不进 prompt/事件/日志。
	APIKey string `toml:"api_key"`
	// APIKeyEnv 持有 API key 的**环境变量名**（缺省 OPENAI_API_KEY）。只传名字。
	APIKeyEnv string `toml:"api_key_env"`

	// System 系统提示。
	System string `toml:"system"`
	// Approval 审批策略：deny-all | always-allow | terminal。
	Approval string `toml:"approval"`
	// MaxTurns 单次 Run 最大轮数（0 = 框架默认）。
	MaxTurns int `toml:"max_turns"`
	// ContextBudget 上下文窗口预算 token（0 = 框架默认；只用于占比分母）。
	ContextBudget int64 `toml:"context_budget"`
	// Allow 允许的工具名（空 = 一切 deny，fail-close）。
	Allow []string `toml:"allow"`
	// Ask 需要人审的工具名。
	Ask []string `toml:"ask"`

	// OnlineTable 在线表注入（K14/K15）默认参数。
	OnlineTable OnlineTableFileConfig `toml:"online_table"`
	// Monitor hub 监控（K9）阈值。
	Monitor MonitorFileConfig `toml:"monitor"`
	// ContextStrategy 上下文装配策略（v2-S1）：kind="" = append（默认）| "tiered" = 级联压缩；
	// window/l1_pct/l2_pct 是 tiered 的可配参数（13 §8/J9：数字是**可配初值**，不写死）。
	ContextStrategy ContextStrategyFileConfig `toml:"context_strategy"`
	// ContextRules 项目规则文本（AGENTS.md 等；稳定前缀的一部分，Q2/Q3）。空 = 不注入。
	ContextRules string `toml:"context_rules"`
	// Graph 图投影（v2-S2，G2f 可选投影；默认关 = 零图开销）。
	Graph GraphFileConfig `toml:"graph"`
	// Sandbox 沙箱执行环境（v2-S4，K8/H9；默认关 = 走 local，与 v1 等价）。
	Sandbox SandboxFileConfig `toml:"sandbox"`
	// MCP MCP client（v3-S1，N1：client 先；默认关 = 不连任何外部 server）。
	MCP MCPFileConfig `toml:"mcp"`
	// WorkflowDir 工作流能力包目录（v3-S3，W13：纯数据 JSON；空 = 只用内建 tower）。
	WorkflowDir string `toml:"workflow_dir"`
	// GoalLedger 是否给 session 注入 goal ledger 更新工具（J19/X6；默认 false = 零行为变化）。
	GoalLedger bool `toml:"goal_ledger"`
}

// MCPFileConfig 是 MCP client 的文件级配置（v3-S1）。
//
// 形如：
//
//	[[mcp.servers]]
//	name = "fs"                          # 必填；工具名前缀 mcp__fs__<tool>
//	transport = "stdio"                  # stdio（默认）| http
//	command = "npx"
//	args = ["-y", "@scope/server-fs", "."]
//	enabled = true
//	env = { TOKEN = "..." }              # 子进程 env 直填（凭据；永不进日志）
//	env_from = ["MY_TOKEN"]              # 从父进程 env 继承这些变量名
//
//	[[mcp.servers]]
//	name = "remote"
//	transport = "http"
//	url = "http://127.0.0.1:1234/mcp"
//	headers = { Authorization = "Bearer ..." }
//
// 口径（三条，与 host.Config.MCP 的注释一致，不重抄数字）：
//
//  1. **默认关**：`[[mcp.servers]]` 一条都没有 = 不装 MCP（零外部进程、零工具，
//     与 v2 行为完全一致）。
//  2. **deny-all 默认**：声明了 server 也只是"工具可见/可注册"，能不能调还要
//     策略链放行（`allow` 里列 `mcp__<server>__<tool>`）与 agent 类型白名单
//     （R5）双重同意。
//  3. **凭据纪律**：`env` / `headers` 的值可能含 server 凭据，**永不进事件/日志**。
//     本结构含凭据 → 绝不整体日志化，要打印用 Redacted()。
type MCPFileConfig struct {
	// Servers 是要连接的 server 列表（TOML 的数组表 [[mcp.servers]]）。
	//
	// enabled 的缺省口径：TOML 的 bool 零值是 false，所以"没写 enabled"会解析成
	// false = 不连。这与"默认关"看似一致，实则误导——使用者写了一条 server 却
	// 忘了 enabled，会得到"静默不连"。因此 host 侧对 enabled 未显式写的条目
	// **按 true 处理**（见 host/mcp.go 的 normalizeEnabled），让"写了就要连"
	// 成为默认，`enabled = false` 才是显式关闭。
	Servers []MCPServerFileConfig `toml:"servers"`
}

// MCPServerFileConfig 是一个 MCP server 的文件级声明（[[mcp.servers]] 的元素）。
//
// 字段与 runtime/mcp.ServerConfig 一一对应；Dir 不暴露（文件级配置不该能改
// 子进程工作目录——那会让"连一个工具 server"变成"在任意目录跑任意命令"）。
type MCPServerFileConfig struct {
	// Name server 逻辑名（工具名前缀 mcp__<server>__<tool> 的来源；全局唯一）。
	Name string `toml:"name"`
	// Transport 传输类型：stdio（缺省）| http。
	Transport string `toml:"transport"`
	// Enabled 是否启用。
	//
	// **指针**而不是 bool：TOML 的 bool 零值是 false，用值类型就无法区分
	// "没写 enabled" 与 "写了 enabled = false"。没写 = 使用者声明了这条 server
	// 却忘了开关 → 按 **true** 处理（"写了就要连"比"静默不连"好排查得多）；
	// 只有显式 `enabled = false` 才是关闭。
	Enabled *bool `toml:"enabled"`
	// Command / Args 是 stdio 传输的进程声明（不走 shell）。
	Command string   `toml:"command"`
	Args    []string `toml:"args"`
	// Env 是显式注入子进程的键值（**可能含凭据**；永不进事件/日志）。
	Env map[string]string `toml:"env"`
	// EnvFrom 是要从父进程 env 继承的**变量名**（只传名字，值在装配期取出）。
	EnvFrom []string `toml:"env_from"`
	// URL / Headers 是 http 传输的端点与请求头（Headers 可能含凭据）。
	URL     string            `toml:"url"`
	Headers map[string]string `toml:"headers"`
	// TimeoutMs 连接/发现超时（毫秒；0 = 默认）。
	TimeoutMs int `toml:"timeout_ms"`
}

// Redacted 返回凭据已打码的副本（**只有它输出的结构可安全落日志**，H12/H19）。
func (c MCPFileConfig) Redacted() MCPFileConfig {
	out := c
	out.Servers = make([]MCPServerFileConfig, len(c.Servers))
	for i, s := range c.Servers {
		out.Servers[i] = s.Redacted()
	}
	return out
}

// Redacted 返回单个 server 的凭据打码副本（Env/Headers 的值 → "***"）。
func (s MCPServerFileConfig) Redacted() MCPServerFileConfig {
	out := s
	out.Args = append([]string(nil), s.Args...)
	out.EnvFrom = append([]string(nil), s.EnvFrom...)
	if len(s.Env) > 0 {
		out.Env = make(map[string]string, len(s.Env))
		for k := range s.Env {
			out.Env[k] = "***"
		}
	}
	if len(s.Headers) > 0 {
		out.Headers = make(map[string]string, len(s.Headers))
		for k := range s.Headers {
			out.Headers[k] = "***"
		}
	}
	return out
}

// MCPConfig 把文件级配置折成 runtime/mcp.Config（host.Config.MCP 的**唯一来源**）。
//
// 只做搬运 + 两件事：
//  1. `enabled` 未显式写的条目按 **true** 处理（理由见 MCPServerFileConfig.Enabled：
//     "写了 server 却静默不连"比"多连一个"更难排查）；
//  2. 没写 name 的条目照样搬过去——由 runtime/mcp.Config.Validate 给出
//     "server 名不能为空"的明确错误（fail-close：不在这里静默丢弃，
//     否则使用者以为连上了）。
func (c MCPFileConfig) MCPConfig() mcp.Config {
	out := mcp.Config{Servers: make([]mcp.ServerConfig, 0, len(c.Servers))}
	for _, s := range c.Servers {
		enabled := true
		if s.Enabled != nil {
			enabled = *s.Enabled
		}
		out.Servers = append(out.Servers, mcp.ServerConfig{
			Name:      s.Name,
			Transport: s.Transport,
			Enabled:   enabled,
			Command:   s.Command,
			Args:      append([]string(nil), s.Args...),
			Env:       copyEnvMap(s.Env),
			EnvFrom:   append([]string(nil), s.EnvFrom...),
			URL:       s.URL,
			Headers:   copyEnvMap(s.Headers),
			TimeoutMs: s.TimeoutMs,
		})
	}
	return out
}

// copyEnvMap 拷一份键值表（nil 安全；不共享底层 map，防调用方改到配置内部）。
func copyEnvMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// GraphFileConfig 是图投影的文件级配置（v2-S2）。
//
// 形如：
//
//	[graph]
//	enabled = true
//	max_nodes = 100000
//	max_edges = 1000000
//	max_depth = 8
//	max_results = 5000
//	core = ["core/*", "contracts/*"]
//	escalate_reachable = 50
//
// 口径：**enabled 为 false（默认）= 图投影未启用**——不建图表、不建引擎、索引器连
// tool.call.completed 的载荷都不解析（G2f：图不进 core 必经路径，零图开销）。
// 上界 0 = 框架默认（G19 量级口径：10 万节点/100 万边）。core 里 `*` 结尾的是
// **字节前缀**（大小写敏感，陷阱 17）。
type GraphFileConfig struct {
	// Enabled 是否启用图投影（可选投影，G2f）。
	Enabled bool `toml:"enabled"`
	// MaxNodes / MaxEdges 内存图上界（0 = 框架默认 10 万/100 万）。
	MaxNodes int `toml:"max_nodes"`
	MaxEdges int `toml:"max_edges"`
	// MaxDepth / MaxResults 多跳遍历上限（0 = 框架默认 8 / 5000）。
	MaxDepth   int `toml:"max_depth"`
	MaxResults int `toml:"max_results"`
	// Core "核心接口"节点声明（`*` 结尾 = 前缀；W16 第二个客观依据）。
	Core []string `toml:"core"`
	// EscalateReachable change_impact 的 escalate 阈值（可达节点数；0 = 默认 50）。
	EscalateReachable int `toml:"escalate_reachable"`
}

// SandboxFileConfig 是沙箱执行环境的文件级配置（v2-S4，K8/H9）。
//
// 形如：
//
//	[sandbox]
//	enabled = true
//	write_dirs = ["D:\\repo"]
//	max_active_processes = 64
//	acl_max_objects = 200000
//
// 口径：**enabled 为 false（默认）= 未启用沙箱**，工具执行环境走 runtime/cap/local
// （与 v1 等价，零沙箱开销）。开了之后：
//
//   - bash / cmd 起在**受限 token** 下（用户 SID 转 deny-only + 删危险特权）；
//   - 子进程纳入 **job object**（KILL_ON_JOB_CLOSE 收容整棵进程树）；
//   - `write_dirs` 里的根被设 DACL，**根之外的写被内核拒绝**（OS 强制，不是路径守卫；
//     例外：BUILTIN\Users 有写权限的位置如 C:\ProgramData 在默认模式下仍可写）；
//   - 根之外的**用户私有文件**（.env/.ssh）读不到；系统位置默认仍可读
//     （严格模式 strict_groups = true 才连读一起收紧）。
//
// `write_dirs` 空 = 只用 CWD + 沙箱自管临时根（子进程的 TMP/TEMP 会指到后者）。
// 上界 0 = 框架默认（活动进程 64 / ACL 对象 20 万）。
type SandboxFileConfig struct {
	// Enabled 是否启用沙箱执行环境（默认 false = 走 local）。
	Enabled bool `toml:"enabled"`
	// WriteDirs 允许写根（绝对路径；空 = CWD + 沙箱临时根）。
	WriteDirs []string `toml:"write_dirs"`
	// MaxActiveProcesses job object 的活动进程上限（0 = 默认 64）。
	MaxActiveProcesses int `toml:"max_active_processes"`
	// ACLMaxObjects 施加 ACL 前数子树对象的上限（0 = 默认 20 万；超限即拒）。
	ACLMaxObjects int `toml:"acl_max_objects"`
	// StrictGroups 严格组 SID 模式（默认 false = 保留 BUILTIN 的 Users SID）。
	//
	// 严格模式把"只授予 Users 写权限的目录"（典型是 C:\ProgramData）也堵上，
	// 代价是用 overlapped I/O 写文件的程序（Go/.NET/Rust/Node）会写不进去。
	StrictGroups bool `toml:"strict_groups"`
}

// ContextStrategyFileConfig 是上下文装配策略的文件级配置（v2-S1）。
//
// 形如：
//
//	[context_strategy]
//	kind = "tiered"
//	window = 1000000
//	l1_pct = 10.0
//	l2_pct = 1.0
//
// 口径：**kind 为空 = append**（默认，v1 等价）；tiered 时 window 须 ≥ 256k（13 §8 硬约束，
// 不满足拒绝装配并说明）。l1_pct/l2_pct 是目标残留占窗口的百分比（可配初值，J9）。
type ContextStrategyFileConfig struct {
	// Kind 策略种类："" = append（默认）| "tiered" = 级联压缩。
	Kind string `toml:"kind"`
	// Window tiered 的窗口预算 W（token；0 = 默认 1M；须 ≥ 256k）。
	Window int64 `toml:"window"`
	// L1Pct L1（浅摘要）目标残留占窗口百分比（0 = 默认 10）。
	L1Pct float64 `toml:"l1_pct"`
	// L2Pct L2（深压缩）目标残留占窗口百分比（0 = 默认 1）。
	L2Pct float64 `toml:"l2_pct"`
	// RulesBudget 项目规则注入的 token 上限（0 = 布局默认 15k）。
	RulesBudget int64 `toml:"rules_budget"`
	// CommonSenseBudget 常识区注入的 token 上限（0 = 布局默认 30k）。
	CommonSenseBudget int64 `toml:"common_sense_budget"`
}

// OnlineTableFileConfig 是在线表注入的文件级默认（K14/K15）。
type OnlineTableFileConfig struct {
	Rows       int    `toml:"rows"`
	LineTokens int    `toml:"line_tokens"`
	Level      string `toml:"level"`
	Window     int    `toml:"window"`
}

// MonitorFileConfig 是 hub 监控（K9）的文件级阈值。0 = 用框架默认。
type MonitorFileConfig struct {
	// CPUPercent 进程 CPU 百分比阈值（单核等价，可 >100）。0 = 默认。
	CPUPercent float64 `toml:"cpu_percent"`
	// MemMB 进程内存阈值（MB，runtime 口径）。0 = 默认。
	MemMB float64 `toml:"mem_mb"`
	// Goroutines goroutine 数阈值（K9：死循环 goroutine 抢占）。0 = 默认。
	Goroutines int `toml:"goroutines"`
	// IntervalSec 采样周期（秒）。0 = 默认。
	IntervalSec int `toml:"interval_sec"`
}

// Redacted 返回一个凭据已打码的副本（供日志/诊断打印：**只有这个方法输出的结构可安全
// 落日志**）。APIKey 非空 → "***"（保留"有没有设"的信息，不泄露值）；APIKeyEnv 是变量名
// （非值）可原样保留。
//
// **MCP 段同样要打码**（v3-S1）：[[mcp.servers]] 的 env / headers 里放的就是
// server 凭据——只打码 APIKey 而放过 MCP 段，等于让"可安全落日志"的承诺在最
// 容易被忽略的那一段上失效（凭据纪律 H12/H19）。
func (c FileConfig) Redacted() FileConfig {
	if strings.TrimSpace(c.APIKey) != "" {
		c.APIKey = "***"
	}
	c.MCP = c.MCP.Redacted()
	return c
}

// DefaultConfigPath 返回用户级配置文件的默认路径（H15）：
//   - Windows：%APPDATA%\alasol\config.toml；
//   - 其他平台：$XDG_CONFIG_HOME/alasol/config.toml，缺省 $HOME/.config/alasol/config.toml。
func DefaultConfigPath() (string, error) {
	if runtime.GOOS == "windows" {
		appdata := strings.TrimSpace(os.Getenv("APPDATA"))
		if appdata == "" {
			return "", errors.New("host: APPDATA 未设置，无法定位用户配置目录（Windows）")
		}
		return filepath.Join(appdata, "alasol", "config.toml"), nil
	}
	if xdg := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); xdg != "" {
		return filepath.Join(xdg, "alasol", "config.toml"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("host: 取 HOME 失败: %w", err)
	}
	return filepath.Join(home, ".config", "alasol", "config.toml"), nil
}

// LoadFileConfig 从 path 读 TOML 配置。path 为空 → 用 DefaultConfigPath()。
//
// 文件**不存在** → (nil, nil)（"没有配置文件"是合法状态，不是错误：新用户本就没有）。
// 文件存在但解析失败 → error（fail-close：损坏的配置可能正是用户指望的默认值/凭据来源，
// 静默忽略会让行为与预期不符，宁可明确报错）。
func LoadFileConfig(path string) (*FileConfig, error) {
	p := strings.TrimSpace(path)
	if p == "" {
		var err error
		p, err = DefaultConfigPath()
		if err != nil {
			return nil, err
		}
	}
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("host: 读配置文件 %s: %w", p, err)
	}
	var fc FileConfig
	if err := toml.Unmarshal(data, &fc); err != nil {
		// 错误里**不含**配置内容（toml 的行号/键名足够定位，不回显值——凭据安全）。
		return nil, fmt.Errorf("host: 解析配置文件 %s: %w", p, err)
	}
	return &fc, nil
}

// Precedence 实现 H15 的取值优先级：**显式值（flag）> 环境变量 > 配置文件**。
// 返回选中值与来源标签（"flag" / "env" / "file" / ""=都空）。
//
// 这是优先级矩阵的**唯一实现处**（D21：口径只定义一次）——model/base_url/凭据名/
// 在线表参数等全部经它收敛，测试在此钉死 env>file。
func Precedence(flagVal, envVal, fileVal string) (value, source string) {
	switch {
	case strings.TrimSpace(flagVal) != "":
		return flagVal, sourceFlag
	case strings.TrimSpace(envVal) != "":
		return envVal, sourceEnv
	case strings.TrimSpace(fileVal) != "":
		return fileVal, sourceFile
	default:
		return "", ""
	}
}

const (
	sourceFlag = "flag"
	sourceEnv  = "env"
	sourceFile = "file"
)

// ResolveCredential 解析凭据值：**env > 文件**（H15）。envName 是持有 key 的变量名；
// fileKey 是配置文件里直填的 key。env 有值 → env 赢；否则用文件值。
//
// 凭据只在这里被取出并交给 provider，**调用方不得把它写入任何日志/事件/argv**。
func ResolveCredential(envName, fileKey string) string {
	if envName != "" {
		if v := os.Getenv(envName); strings.TrimSpace(v) != "" {
			return v
		}
	}
	return fileKey
}

// ResolveAPIKeyEnv 解析"持有 API key 的环境变量名"：flag > 文件 > 默认（OPENAI_API_KEY）。
// 只解析**名字**（不是值）——名字可安全日志化。
func ResolveAPIKeyEnv(flagVal, fileVal string) string {
	if v, _ := Precedence(flagVal, "", fileVal); v != "" {
		return v
	}
	return DefaultAPIKeyEnv
}
