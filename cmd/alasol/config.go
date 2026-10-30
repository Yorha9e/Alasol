package main

import (
	"flag"
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

	// reasoning_effort（G14）：显式 flag > 文件（无 env——档位不是凭据，但也没必要多一条来源）。
	// 空 = 不传（端点默认档）。
	//
	// **这里只取值，不校验**：本函数是"flag + 文件 → host.Config"的搬运工，档位合法性
	// 由 host.Config.validate 判（唯一事实源 = host.ValidateReasoningEffort）——serve 路径
	// 的 fail-close 就在那儿。run / resume 两条**不经 host.Config** 的路径自己调
	// host.ResolveReasoningEffort（同一个白名单）→ 三路径同口径。
	reasoningEffort := host.ReasoningEffortValue(*f.reasoningEffort)
	if reasoningEffort == "" {
		reasoningEffort = host.ReasoningEffortValue(fc.ReasoningEffort)
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

		// 思维链档位（G14）：显式 flag > 文件；空 = 不传（host 侧白名单校验）。
		ReasoningEffort: reasoningEffort,

		// 协议面一轮对话的整轮上限（秒；0 = 框架默认 600）。只有 flag 面（无 env/文件面）：
		// 这是**协议面**的时钟，不影响 CLI/TUI 的交互式对话。
		PromptRunTimeoutSec: *f.promptTimeout,

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

		// 工作流引擎（v3-S3，W1 能力包）：目录显式 flag > 文件；goal-ledger 走**布尔口径**
		//（显式 flag > 文件 > 默认关）——`-goal-ledger=false` 必须能压过文件里的
		// goal_ledger=true（"||" 只会把 false 变成 true，与 sandbox 同款坑，见
		// sandboxEnabledFlag）。
		WorkflowDir: workflowDirValue(f, fc),
		GoalLedger:  pickBoolFlag(f.explicitlySet("goal-ledger"), *f.goalLedger, fc.GoalLedger),

		// 多 agent 装配（v3-S6-B，W5）：**只走文件配置面**（TOML `multi_agent` + `[[agent_types]]`，
		// 不给 flag——与 [[mcp.servers]] 同款：结构化数据 + 默认关）。默认 false = 单 agent 模式
		// 零行为变化（不建树、不传 Agents、**不校验 agent_types**、spawn/complete 不可见）。
		// AgentTypes 经 FileConfig 折成 []contracts.AgentType（同名覆盖内建五类，
		// host/agenttypes.go 装配；只在 MultiAgent 开时才建注册表并 fail-close 校验）。
		MultiAgent: fc.MultiAgent,
		AgentTypes: fc.AgentTypeConfigs(),

		// 沙箱（v2-S4，K8/H9）：显式 flag > 文件。默认关（与 v1 等价，走 local）。
		Sandbox:                   sandboxEnabled(f, fc),
		SandboxWriteDirs:          sandboxWriteDirs(f, fc),
		SandboxMaxActiveProcesses: pickInt(*f.sandboxMaxActive, fc.Sandbox.MaxActiveProcesses),
		SandboxACLMaxObjects:      pickInt(*f.sandboxACLMaxObjects, fc.Sandbox.ACLMaxObjects),
		SandboxStrictGroups:       pickBoolFlag(f.explicitlySet("sandbox-strict-groups"), *f.sandboxStrictGroups, fc.Sandbox.StrictGroups),

		// MCP client（v3-S1，N1）：**只走文件配置面**（[[mcp.servers]] 数组表）。
		// 不给 flag：server 列表是结构化数据（命令/参数/env/headers），
		// 硬塞进命令行既难写又容易把凭据泄进 shell 历史与 ps 输出（H12/H19）。
		// 默认关：一条 [[mcp.servers]] 都没有 = 不装 MCP。
		MCP: fc.MCP.MCPConfig(),
	}
	if testProviderOverride != nil {
		// 测试注入的脚本 provider（产物代码永远不设它 → 恒为真 provider）。
		cfg.Provider = testProviderOverride
	}
	return cfg, nil
}

// rejectInvalidReasoningEffort 在 host.New **之前**校验思维链档位（白名单）。
//
// 返回 true = 配置被拒（调用方打印提示后 exit 2）。
//
// # 为什么要单独判一次（host.Config.validate 里已经有一遍）
//
// **退出码口径**：配置被拒 = 退出码 2（run / resume 就是这个口径）；而 host.New 失败
// 一律 exit 1（那是"装配失败"，含非配置类原因）。同一个坏档位在 serve 下报 1、在 run 下
// 报 2 —— 脚本没法用退出码区分"配置写错了"和"跑挂了"。故配置类的问题要在 host.New
// **之前**单独判一次。
//
// 只判**档位**（其余配置项仍由 host.Config.validate 判、仍走 exit 1）：统一"档位写错 = 2"
// 这一条，不动其它配置错误的既有退出码（既有行为不变）。
//
// 校验口径的唯一事实源 = host.ValidateReasoningEffort（run / resume / serve 三路径同口径）。
func rejectInvalidReasoningEffort(cfg host.Config) bool {
	if err := host.ValidateReasoningEffort(cfg.ReasoningEffort); err != nil {
		fmt.Fprintln(os.Stderr, "思维链档位不合法:", err)
		return true
	}
	return false
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

// pickBoolFlag 解析布尔开关（显式 flag > 文件 > 默认 false）。
//
// 布尔 flag 的口径与 int 不一样：false 是**合法值**（用户可以显式关掉），
// 所以"flag > 文件"要看**显式设置**而不是"值非零"——
// 由调用方用 explicitlySet / flagSet 判定（见 sandboxEnabledFlag）。
func pickBoolFlag(explicit bool, flagVal bool, fileVal bool) bool {
	if explicit {
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

// workflowDirValue 解析工作流能力包目录（v3-S3，W1）：显式 flag > 文件。空 = 只用内建包。
func workflowDirValue(f *hubFlags, fc *host.FileConfig) string {
	if v := strings.TrimSpace(*f.workflowDir); v != "" {
		return v
	}
	if fc != nil {
		return strings.TrimSpace(fc.WorkflowDir)
	}
	return ""
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

// sandboxEnabled 解析沙箱开关（v2-S4，K8/H9）：显式 flag > 文件。默认关。
func sandboxEnabled(f *hubFlags, fc *host.FileConfig) bool {
	return sandboxEnabledFlag(f.explicitlySet("sandbox"), *f.sandbox, fc)
}

// sandboxEnabledFlag 解析沙箱开关（**唯一口径**：显式 flag > 文件 > 默认关）。
//
// F20：`alasol run` 之前写的是 `*sandboxEnabled || fc.Sandbox.Enabled`——
// `-sandbox=false` **压不过**文件里的 enabled=true（"||" 只会把 false 变成 true）。
// 口径统一成"显式 flag 优先"：显式设了 flag 就是用户说了，文件让位。
//
// 参数 explicit 由各命令自己判定（hubFlags 走 f.explicitlySet；run/resume 走 flagSet）。
func sandboxEnabledFlag(explicit bool, flagVal bool, fc *host.FileConfig) bool {
	if explicit {
		return flagVal
	}
	return fc.Sandbox.Enabled
}

// flagSet 报告某个 flag 是否被**显式**设置（区别于"用了默认值"）。
//
// run / resume 各自有自己的 FlagSet（不是 hubFlags），所以需要这个通用版；
// 口径与 hubFlags.explicitlySet 一致（fs.Visit 只回调被显式设置过的 flag）。
func flagSet(fs *flag.FlagSet, name string) bool {
	if fs == nil {
		return false
	}
	set := false
	fs.Visit(func(fl *flag.Flag) {
		if fl.Name == name {
			set = true
		}
	})
	return set
}

// sandboxWriteDirs 解析沙箱允许写根（v2-S4）：显式 flag（逗号分隔）> 文件（列表）。
func sandboxWriteDirs(f *hubFlags, fc *host.FileConfig) []string {
	if f.explicitlySet("sandbox-write-dirs") {
		return splitList(*f.sandboxWriteDirs)
	}
	if len(fc.Sandbox.WriteDirs) > 0 {
		return fc.Sandbox.WriteDirs
	}
	return nil
}
