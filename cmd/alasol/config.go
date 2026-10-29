package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/Yorha9e/alasol/host"
)

// buildConfig 把 flag 值 + 进程 env + 用户级 TOML 配置（H15）合并成 host.Config。
//
// 优先级（H15）：**显式 flag > 进程 env > 配置文件**。env 在则 env 赢（相对文件）；
// flag 是用户最直接的意图，最高。"显式 flag" 由 f.explicitlySet 判定——flag 的**默认值**
// 不高于文件（否则 deny-all / OPENAI_API_KEY 这类默认会永远掩盖配置文件）。
//
// 凭据（api_key）：只在这里按 env>文件 收敛一次，填入 host.Config.APIKey；**永不进
// argv / 事件 / 日志**（H12/H19）。配置文件解析失败 → 直接报错（fail-close：损坏的配置
// 可能正是用户指望的默认值/凭据来源）。
func (f *hubFlags) buildConfig() (host.Config, error) {
	fc, err := host.LoadFileConfig(*f.configPath)
	if err != nil {
		return host.Config{}, err
	}
	if fc == nil {
		fc = &host.FileConfig{}
	}

	// model：flag > env(ALASOL_MODEL) > 文件。双空 → fail-close 报错（I9：模型名端点相关，
	// 无万能默认）。
	model, _ := host.Precedence(*f.model, os.Getenv("ALASOL_MODEL"), fc.Model)
	if strings.TrimSpace(model) == "" {
		return host.Config{}, fmt.Errorf("缺模型名：用 -model 指定，或设环境变量 ALASOL_MODEL，" +
			"或在配置文件里设 model；可用 `alasol models` 列出端点支持的模型")
	}

	// base_url：flag > env(ALASOL_BASE_URL) > 文件。
	baseURL, _ := host.Precedence(*f.baseURL, os.Getenv("ALASOL_BASE_URL"), fc.BaseURL)

	// 凭据：先定 env 变量名（显式 flag > 文件 > 默认 OPENAI_API_KEY），再取值（env > 文件）。
	apiKeyEnv := host.ResolveAPIKeyEnv(flagIfSet(f, "api-key-env", *f.apiKeyEnv), fc.APIKeyEnv)
	apiKey := host.ResolveCredential(apiKeyEnv, fc.APIKey) // env 有值则 env 赢

	// approval：显式 flag > 文件 > 框架默认 deny-all。
	approvalVal := *f.approval
	if !f.explicitlySet("approval") {
		if v := strings.TrimSpace(fc.Approval); v != "" {
			approvalVal = v
		}
	}
	if strings.TrimSpace(approvalVal) == "" {
		approvalVal = "deny-all"
	}

	// system：flag > 文件（无 env）。
	system, _ := host.Precedence(*f.system, "", fc.System)

	// allow / ask：flag 显式设置才用 flag；否则用文件（都是列表，flag 空=未设）。
	allow := splitList(*f.allow)
	if !f.explicitlySet("allow") && len(fc.Allow) > 0 {
		allow = fc.Allow
	}
	ask := splitList(*f.ask)
	if !f.explicitlySet("ask") && len(fc.Ask) > 0 {
		ask = fc.Ask
	}

	// max_turns / context_budget：flag 非零才用 flag；否则用文件（0=未设）。
	maxTurns := *f.maxTurns
	if maxTurns == 0 {
		maxTurns = fc.MaxTurns
	}
	contextBudget := *f.contextBudget
	if contextBudget == 0 {
		contextBudget = fc.ContextBudget
	}

	cfg := host.Config{
		DBPath:         *f.dbPath,
		Addr:           *f.addr,
		Model:          model,
		System:         system,
		MaxTurns:       maxTurns,
		ContextBudget:  contextBudget,
		Allow:          allow,
		Ask:            ask,
		Approval:       approvalVal,
		BaseURL:        baseURL,
		APIKey:         apiKey,    // env>文件 已收敛；永不进日志/事件
		APIKeyEnv:      apiKeyEnv, // 变量名（非值），可安全日志化
		IndexerBatch:   *f.indexerBatch,
		MaxSessionRows: *f.maxSessionRows,
		CWD:            strings.TrimSpace(*f.cwd),
		RunID:          strings.TrimSpace(*f.runID),

		OnlineTableRows:       pickInt(*f.onlineRows, fc.OnlineTable.Rows),
		OnlineTableLineTokens: pickInt(*f.onlineLineTokens, fc.OnlineTable.LineTokens),
		OnlineTableLevel:      firstNonEmptyStr(strings.TrimSpace(*f.onlineLevel), strings.TrimSpace(fc.OnlineTable.Level)),
		OnlineTableWindow:     pickInt(*f.onlineWindow, fc.OnlineTable.Window),

		// hub 监控阈值（K9）：显式 flag > env > 文件 > 框架默认（0=默认）。
		MonitorCPUPercent:  pickFloat(*f.monitorCPU, envFloat("ALASOL_MONITOR_CPU_PERCENT"), fc.Monitor.CPUPercent),
		MonitorMemMB:       pickFloat(*f.monitorMem, envFloat("ALASOL_MONITOR_MEM_MB"), fc.Monitor.MemMB),
		MonitorGoroutines:  pickInt(*f.monitorGoroutines, fc.Monitor.Goroutines),
		MonitorIntervalSec: pickInt(*f.monitorInterval, fc.Monitor.IntervalSec),

		// 上下文策略（v2-S1）：显式 flag > 文件。空 = append（默认）。
		// 参数面（F10）：window/l1_pct/l2_pct 也走同一条优先级（显式 flag > 文件 > 框架默认），
		// 全部汇到 host.Config → compaction.Params（数字是可配初值，不写死）。
		ContextStrategy:       contextStrategyValue(f, fc),
		ContextStrategyWindow: pickInt64(*f.contextWindow, fc.ContextStrategy.Window),
		ContextStrategyL1Pct:  pickFloat(*f.contextL1Pct, 0, fc.ContextStrategy.L1Pct),
		ContextStrategyL2Pct:  pickFloat(*f.contextL2Pct, 0, fc.ContextStrategy.L2Pct),
		// 注入区上限（规则/常识区）：不可压缩的内容必须有上限，否则一条超长规则就能顶穿窗口。
		ContextStrategyRulesBudget:       pickInt64(0, fc.ContextStrategy.RulesBudget),
		ContextStrategyCommonSenseBudget: pickInt64(0, fc.ContextStrategy.CommonSenseBudget),
		// 项目规则文本（稳定前缀的一部分；只有配置文件面提供——多行文本不适合走 flag）。
		ContextRules: fc.ContextRules,

		// 图投影（v2-S2，G2f 可选投影）：显式 flag > 文件。默认关（零图开销）。
		Graph:                  graphEnabled(f, fc),
		GraphMaxNodes:          pickInt(*f.graphMaxNodes, fc.Graph.MaxNodes),
		GraphMaxEdges:          pickInt(*f.graphMaxEdges, fc.Graph.MaxEdges),
		GraphMaxDepth:          pickInt(*f.graphMaxDepth, fc.Graph.MaxDepth),
		GraphMaxResults:        pickInt(*f.graphMaxResults, fc.Graph.MaxResults),
		GraphCoreNodes:         graphCoreNodes(f, fc),
		GraphEscalateReachable: pickInt(*f.graphEscalate, fc.Graph.EscalateReachable),
	}
	if testProviderOverride != nil {
		// 测试注入的脚本 provider（产物代码永远不设它 → 恒为真 provider）。
		cfg.Provider = testProviderOverride
	}
	return cfg, nil
}

// flagIfSet 返回 flag 值（若显式设置）否则空串（让下游回落文件/默认）。
func flagIfSet(f *hubFlags, name, flagVal string) string {
	if f.explicitlySet(name) {
		return flagVal
	}
	return ""
}

// pickInt 取第一个非零值（flag 优先，0=未设）。
func pickInt(flagVal, fileVal int) int {
	if flagVal != 0 {
		return flagVal
	}
	return fileVal
}

// pickInt64 取第一个非零值（flag 优先，0=未设）。
func pickInt64(flagVal, fileVal int64) int64 {
	if flagVal != 0 {
		return flagVal
	}
	return fileVal
}

// pickFloat 取第一个非零值（flag 优先，0=未设）。
func pickFloat(flagVal, envVal, fileVal float64) float64 {
	if flagVal != 0 {
		return flagVal
	}
	if envVal != 0 {
		return envVal
	}
	return fileVal
}

// firstNonEmptyStr 取第一个非空串。
func firstNonEmptyStr(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// envFloat 读一个 float 环境变量（解析失败/未设 → 0）。
func envFloat(name string) float64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return 0
	}
	var x float64
	if _, err := fmt.Sscanf(v, "%g", &x); err != nil {
		return 0
	}
	return x
}

// contextStrategyValue 解析上下文策略（v2-S1）：显式 flag > 文件。空 = append（默认）。
// trim 后比较（容忍 " tiered " 这类带空白的输入）；合法性由 host.Config.validate 收口
// （装配侧也按 trim 后的值比较——F23：校验与装配必须同一口径，否则静默退回 append）。
func contextStrategyValue(f *hubFlags, fc *host.FileConfig) string {
	if f.explicitlySet("context-strategy") {
		return strings.TrimSpace(*f.contextStrategy)
	}
	return strings.TrimSpace(fc.ContextStrategy.Kind)
}

// graphEnabled 解析图投影开关（v2-S2）：**显式 flag > 文件 > 默认 false**。
//
// 默认 false = 图投影未启用（G2f：可选投影，不进 core 必经路径 → 零图开销）。
// 只有显式设了 -graph 才可能为 true；配置文件里 enabled = true 也能开。
func graphEnabled(f *hubFlags, fc *host.FileConfig) bool {
	if f.explicitlySet("graph") {
		return *f.graph
	}
	return fc.Graph.Enabled
}

// graphCoreNodes 解析核心接口节点声明（v2-S2）：显式 flag（逗号分隔）> 文件（列表）。
func graphCoreNodes(f *hubFlags, fc *host.FileConfig) []string {
	if f.explicitlySet("graph-core") {
		return splitList(*f.graphCore)
	}
	if len(fc.Graph.Core) > 0 {
		return fc.Graph.Core
	}
	return nil
}
