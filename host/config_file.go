package host

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/BurntSushi/toml"
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
func (c FileConfig) Redacted() FileConfig {
	if strings.TrimSpace(c.APIKey) != "" {
		c.APIKey = "***"
	}
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
