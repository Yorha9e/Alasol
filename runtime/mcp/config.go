package mcp

import (
	"errors"
	"fmt"
	"strings"
)

// 本文件定义 MCP client 的**配置面**：要连哪些 server、用什么传输拉起、
// 以及凭据怎么传给子进程。口径与 host/config_file.go 的 TOML 一一对应
// （[[mcp.servers]] 数组表），host.Config.MCP 持有解析后的 Config。
//
// 脱敏纪律（H12/H19）：ServerConfig.Env 里可能放 MCP server 的凭据（token / key）。
// 整个 ServerConfig **永不进事件 / 日志**；要打印用 Redacted()（凭据打码）。
// 这与 host.FileConfig.Redacted() 同款——"只有 Redacted() 输出的结构可安全落日志"。

// TransportStdio 是 stdio 传输：本包亲自拉起 server 子进程，走 stdin/stdout
// 交换换行分隔的 JSON-RPC。**v3-S1 的默认与首选**（N1：本地 server 一行命令拉起，
// 无需网络监听、无需凭据协商）。
const TransportStdio = "stdio"

// TransportHTTP 是 Streamable HTTP 传输：连一个已经在跑的 HTTP 端点
// （MCP 现行标准传输；服务端→客户端的消息走同一条连接上的 SSE 流）。
const TransportHTTP = "http"

// TransportSSE 是旧版 SSE 传输的**保留配置值**：v3-S1 只留名字不接线——
// Streamable HTTP（TransportHTTP）已覆盖 SSE 的语义，再接一份旧传输是重复面。
// 配置里写了它 → 明确报错并指向 http，而不是静默按 http 连（静默改语义比报错糟）。
const TransportSSE = "sse"

// SupportedTransports 返回本包实际支持的传输（配置校验与帮助文案共用，D21）。
func SupportedTransports() []string {
	return []string{TransportStdio, TransportHTTP}
}

// Config 是 MCP client 的装配配置：一组要连的 server。
//
// 空 Config（零 server）= **不装 MCP 能力**：不连任何外部进程、不注册任何工具、
// 与 v2 行为完全一致（酸测试不回归）。这一点与 host.Config.Graph/Sandbox 的
// opt-in 形态对齐：默认关、零开销。
type Config struct {
	// Servers 是要连接的 server 列表（顺序即装配顺序，重名在 Validate 期拒绝）。
	Servers []ServerConfig
}

// ServerConfig 是一个外部 MCP server 的声明。
//
// 形如（TOML）：
//
//	[[mcp.servers]]
//	name = "fs"                          # 必填；工具名前缀 mcp__fs__<tool>
//	transport = "stdio"                  # stdio（默认）| http
//	command = "npx"                      # stdio：可执行文件（不走 shell）
//	args = ["-y", "@scope/server-fs"]
//	enabled = true                       # 默认 true；false = 装配期跳过（连都不连）
//	env = { TOKEN = "..." }              # 子进程 env 直填（凭据；永不进日志）
//	env_from = ["MY_TOKEN"]              # 从父进程 env 继承这些**变量名**
//	url = "http://127.0.0.1:1234/mcp"    # http：端点
//
// 凭据纪律：`env` 是**显式**给这个 server 的（类似 required_env 的策略裁决注入），
// 因此**不过 ScrubEnv**—— Scrubbed 的只有从父进程继承的那部分基底（见 childEnv）。
// 想让凭据只活在进程 env 里而不落配置文件，就用 env_from。
type ServerConfig struct {
	// Name 是 server 的逻辑名（工具名前缀的唯一来源）。必填、全局唯一、
	// 只允许 [A-Za-z0-9_-]（见 validateServerName）。
	Name string
	// Transport 是传输类型：stdio（默认空值）| http。空 = stdio。
	Transport string
	// Enabled 是否启用。false = 装配期完全跳过（不连、不注册、不留痕）。
	Enabled bool
	// Command / Args / Dir 是 stdio 传输的进程声明（不走 shell，陷阱 7 同款纪律：
	// Command 直接是可执行文件）。Dir 空 = 继承 hub 工作目录。
	Command string
	Args    []string
	Dir     string
	// Env 是显式注入子进程的键值（**可能含凭据**；永不进事件/日志）。
	Env map[string]string
	// EnvFrom 是要从父进程 env 继承的**变量名**（只传名字，值在装配期取出）。
	// 用来把凭据留在进程 env 而不写进配置文件。
	EnvFrom []string
	// URL 是 http 传输的端点（必填，仅 http）。
	URL string
	// Headers 是 http 传输附带的请求头（如 Authorization）。**可能含凭据**，
	// 与 Env 同款脱敏纪律。
	Headers map[string]string

	// TimeoutMs 是单次连接/发现的超时（毫秒；0 = 默认 15s）。只约束装配期
	// （connect + tools/list）；工具调用本身的超时由调用方 ctx 决定。
	TimeoutMs int
}

// DefaultTimeoutMs 是连接/发现的默认超时（15s）。
//
// 取这个值的理由：`npx -y @scope/server` 首次要下载包，冷启动几秒是常态；
// 但也不能无限等——一个挂死的 server 不该把 `alasol serve` 的启动拖死。
const DefaultTimeoutMs = 15000

// Validate 校验并归一化配置（fail-close：坏配置在装配期就炸，不拖到运行期）。
//
// 归一化：Transport 空 → stdio；Enabled 缺省由 TOML 的 bool 零值给出 false，
// 因此**这里不碰 Enabled**——"要不要默认启用"由宿主配置面决定（host 侧
// 对 enabled 未写的条目按 true 处理，见 host/mcp.go 的说明）。
func (c *Config) Validate() error {
	if c == nil {
		return nil
	}
	seen := make(map[string]bool, len(c.Servers))
	for i := range c.Servers {
		s := &c.Servers[i]
		s.Transport = strings.TrimSpace(s.Transport)
		if s.Transport == "" {
			s.Transport = TransportStdio // 缺省 stdio（N1：stdio 优先）
		}
		if err := validateServerName(s.Name); err != nil {
			return fmt.Errorf("mcp: server[%d]: %w", i, err)
		}
		if seen[s.Name] {
			// 大小写**不折叠**（陷阱 17）："Fs" 与 "fs" 是两个不同的 server 名，
			// 前缀不同 → 工具名不同 → 不会互相遮蔽。这里只拒字节相同的重复。
			return fmt.Errorf("mcp: server %q 重复声明（server 名全局唯一；工具名前缀 mcp__<server>__ 的来源）", s.Name)
		}
		seen[s.Name] = true
		if err := s.validateTransport(); err != nil {
			return fmt.Errorf("mcp: server %q: %w", s.Name, err)
		}
		if s.TimeoutMs < 0 {
			return fmt.Errorf("mcp: server %q: TimeoutMs 不能为负（得到 %d；0 = 默认 %d）", s.Name, s.TimeoutMs, DefaultTimeoutMs)
		}
	}
	return nil
}

// validateTransport 按传输类型校验必填项。
func (s *ServerConfig) validateTransport() error {
	switch s.Transport {
	case TransportStdio:
		if strings.TrimSpace(s.Command) == "" {
			return errors.New("transport=stdio 必须给 command（可执行文件；不走 shell）")
		}
		if strings.TrimSpace(s.URL) != "" {
			return errors.New("transport=stdio 不需要 url（http 传输才用）")
		}
	case TransportHTTP:
		if strings.TrimSpace(s.URL) == "" {
			return errors.New("transport=http 必须给 url（Streamable HTTP 端点）")
		}
		if strings.TrimSpace(s.Command) != "" {
			return errors.New("transport=http 不需要 command（stdio 传输才用）")
		}
	case TransportSSE:
		return fmt.Errorf("transport=%q 在 v3-S1 未接线：Streamable HTTP 已覆盖 SSE 语义，"+
			"请改用 transport=%q（旧 SSE 传输再接一份是重复面）", TransportSSE, TransportHTTP)
	default:
		return fmt.Errorf("transport %q 不支持（只支持 %s）", s.Transport, strings.Join(SupportedTransports(), " | "))
	}
	return nil
}

// EnabledServers 返回启用的 server（顺序不变）。
func (c *Config) EnabledServers() []ServerConfig {
	if c == nil {
		return nil
	}
	var out []ServerConfig
	for _, s := range c.Servers {
		if s.Enabled {
			out = append(out, s)
		}
	}
	return out
}

// Redacted 返回一个凭据已打码的副本（**只有它输出的结构可安全落日志**，H12/H19）。
//
// 打码范围：Env 的值、Headers 的值、EnvFrom 里的**值**（EnvFrom 是变量名不是值，
// 名字可安全保留——但保守起见也打码，因为变量名有时就暗示了用途……不，
// 变量名与 host.APIKeyEnv 同性质，保留名字才有诊断价值，这里只打码真正的值）。
func (c Config) Redacted() Config {
	out := c
	out.Servers = make([]ServerConfig, len(c.Servers))
	for i, s := range c.Servers {
		out.Servers[i] = s.Redacted()
	}
	return out
}

// Redacted 返回单个 server 的凭据打码副本（Env/Headers 的值 → "***"；
// EnvFrom 是变量名，原样保留——与 host.Config.APIKeyEnv 同性质）。
func (s ServerConfig) Redacted() ServerConfig {
	out := s
	out.Args = append([]string(nil), s.Args...)
	out.EnvFrom = append([]string(nil), s.EnvFrom...)
	if len(s.Env) > 0 {
		out.Env = make(map[string]string, len(s.Env))
		for k := range s.Env {
			out.Env[k] = "***" // 保留"有没有设"，不泄露值
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
