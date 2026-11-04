package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/durable"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/core/sessioncfg"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime"
	"github.com/Yorha9e/alasol/runtime/provider/openai"
	"github.com/Yorha9e/alasol/runtime/store/blobfs"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	"github.com/Yorha9e/alasol/runtime/store/walfile"
	"github.com/Yorha9e/alasol/runtime/verify"
)

// resumeCapText 把恢复上界说成人话（分钟 / 秒 / 毫秒；测试调小后不至于说"0 分钟"）。
func resumeCapText(d time.Duration) string {
	switch {
	case d >= time.Minute:
		return fmt.Sprintf("%d 分钟", int(d/time.Minute))
	case d >= time.Second:
		return fmt.Sprintf("%d 秒", int(d/time.Second))
	default:
		return fmt.Sprintf("%d 毫秒", int(d/time.Millisecond))
	}
}

// resumeReplayCap 是**恢复路径重放**的上界（critic F4）。
//
// 为什么要有：standby 的重放 = 重新挂起，最长挂到硬上限（30 分钟）。恢复 ctx 原本
// 是 context.Background()（**无上界**）→ 一条永远等不到消息的 standby 会让
// `alasol resume` **静默挂 30 分钟**（看着像卡死，人不知道它在等什么）。
//
// 上界 = standby 硬上限（30 分钟）+ 余量（5 分钟）= 35 分钟：够一次正常重挂
// （含收尾），又不至于无界。**测试可调小**（见 resume_standby_test.go）。
var resumeReplayCap = 35 * time.Minute

// resumeMain 是 `alasol resume -session <id> [-db ...]`：跑 X4 的恢复流程并打印报告（处置明细）。
//
// 装配：store → registry(read/write/bash) → loop（仅作 durable.Replayer，不调模型）→ 可选 git Verifier
// → durable.Resume。重放 replay=safe（及 verify 不通过）的操作会**真正再次执行**对应工具并落结果；
// verify 先核验外部状态；unsafe 不重放、转人工。报告以单行 JSON 打到 stdout。
//
// -continue "<提示>"：恢复流程之后**续跑一轮对话**（v1-S1 的"对话入事件流"闭环）——把
// 事件流 fold 出的对话历史灌回 loop（core.Loop.RunWithHistory），让模型接着此前的上下文继续。
// 空 = 只做恢复流程，不调模型（与恢复流程本身的语义一致）。
//
// 续跑折叠的是**整场 session** 的对话，**不区分 agent 视角**（FoldConversation 的既有语义）：
// 多 agent 混合 session 里子 agent 的对话也会进入上下文；按 agent 过滤是后续增强
// （-agent <ulid>，尚未实现）。续跑事件的 agent_id = 该 session 的**真实主 agent**
// （durable.PrimaryAgentID 的口径），不产生一次性随机 id 的"幽灵 agent"。
func resumeMain(args []string) int {
	fs := flag.NewFlagSet("resume", flag.ExitOnError)
	session := fs.String("session", "", "要恢复的 session id（必填，26 字符 ULID）")
	dbPath := fs.String("db", "alasol.db", "事件库路径（SQLite）")
	verifyMode := fs.String("verify", "git", "verify 类操作的核验器：git（按 git-commit 锚点核验）| none（全部转人工）")
	force := fs.Bool("force", false, "接管未释放的旧 resume 认领（确认上一次已崩溃时才用）")
	continuePrompt := fs.String("continue", "",
		"恢复后再续跑一轮对话的提示（空=只做恢复流程，不调模型）。"+
			"注意：续跑折叠的是**整场 session** 的对话，**不区分 agent 视角**——"+
			"多 agent 混合 session 里子 agent 的对话也会进入上下文；"+
			"按 agent 过滤（-agent <ulid>）是后续增强，尚未实现。")
	model := fs.String("model", "", "模型名（续跑时用；缺省回落 env ALASOL_MODEL）")
	baseURL := fs.String("base-url", "", "OpenAI 兼容端点（续跑时用；空=官方）")
	apiKeyEnv := fs.String("api-key-env", "OPENAI_API_KEY", "持有 API key 的环境变量名（续跑时用）")
	reasoningEffort := fs.String("reasoning-effort", "",
		"模型思维链强度档位（G14，续跑时用）：low|medium|high|xhigh（空=不传，端点用默认档）")

	// 沙箱（v2-S4）：**重放同样要受约束**（F9）。
	//
	// 之前 resume 一律用 local 执行重放的工具调用——于是"当初在沙箱里跑过的操作"
	// 在恢复时以宿主身份真的执行一遍（fail-open）。口径与 run/serve 一致：
	// 显式 flag > 配置文件 > 默认关。
	sandboxEnabled := fs.Bool("sandbox", false, "启用 SandboxedExecutionEnv（v2-S4；重放同样受约束）")
	sandboxWriteDirs := fs.String("sandbox-write-dirs", "",
		"沙箱允许写根（逗号分隔）。口径：显式列出的根 **+ CWD** 一起进允许集")
	sandboxMaxActive := fs.Int("sandbox-max-active", 0, "沙箱 job object 活动进程上限（0=默认 64）")
	sandboxACLMaxObjects := fs.Int("sandbox-acl-max-objects", 0, "沙箱施加 ACL 前数子树对象的上限（0=默认 20 万）")
	sandboxStrictGroups := fs.Bool("sandbox-strict-groups", false,
		"沙箱严格组 SID 模式：把 BUILTIN 的 Users SID 也 deny-only（默认关）")
	remoteEnabled := fs.Bool("remote", false,
		"启用 RemoteExecutionEnv（v4-2-A；重放同样经远端 ssh）。默认关=走 local；与 -sandbox 互斥")
	remoteHost := fs.String("remote-host", "",
		"ssh 目标（user@host 或 ~/.ssh/config 别名）。-remote 启用时必填")
	configPath := fs.String("config", "", "用户级配置文件路径（TOML；空=平台默认位，H15）")
	_ = fs.Parse(args)

	if *session == "" {
		fmt.Fprintln(os.Stderr, "-session 不能为空")
		return 2
	}
	sid := contracts.Ulid(*session)
	if !core.ValidUlid(sid) {
		fmt.Fprintf(os.Stderr, "-session 不是合法 ULID（26 字符 Crockford base32）：%q\n", *session)
		return 2
	}

	st, err := sqlite.Open(*dbPath, sqlite.Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "打开事件库:", err)
		return 1
	}
	defer st.Close()

	// 配置文件（H15）：沙箱开关/允许根可以写在文件里（与 run/serve 同口径）。
	fc, err := host.LoadFileConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if fc == nil {
		fc = &host.FileConfig{}
	}
	// 大 payload 外置 blob（v4-3-A，G2e/D4）：阈值负值 → **启动即拒**（fail-close）。
	//
	// 口径与 serve / run 装配面**同款**（共享函数 host.ValidateBlobMaxInline = 唯一事实源）：
	// 负阈值没有合理语义，静默当成 0 = 悄悄把外置关掉（重放落下的 completed 照样全量内联，
	// 现场无任何线索）。校验放在**任何重放 / 模型调用之前**——重放会真的执行工具，
	// 阈值配错了就该先拒，不该先把工具跑一遍。
	if err := host.ValidateBlobMaxInline(fc.Blob.MaxInline); err != nil {
		fmt.Fprintln(os.Stderr, "blob 阈值不合法（TOML [blob].max_inline / host.Config.BlobMaxInline）:", err)
		return 2
	}

	// ReasoningEffort（G14）：续跑（-continue）那一轮同样透传档位——与 run/serve 同一条
	// 配置面（显式 flag > 文件；空 = 不传）。
	//
	// **白名单校验在启动期**（fail-close，与 run/serve 同一口径）：打错的档位在这里就
	// 报错（**在任何重放 / 模型调用之前**——重放会真的执行工具，档位错了就该先拒），
	// 而不是拖到续跑那一轮模型调用里拿到一个离现场很远的 400（唯一事实源 =
	// host.ValidateReasoningEffort）。
	reasoningEffortVal, err := host.ResolveReasoningEffort(*reasoningEffort, fc.ReasoningEffort)
	if err != nil {
		fmt.Fprintln(os.Stderr, "思维链档位不合法:", err)
		return 2
	}

	// registry：内置 read/write/bash（resume 重放工具用）。
	//
	// 沙箱：重放**真的执行**工具调用，必须与当初一样受约束（F9：fail-open 的修法）。
	reg := registry.New(registry.NewPolicyAdjudicator(policy.New()))
	sbEnv, err := registerBuiltinTools(reg, runnerOpts{
		Sandbox:                   sandboxEnabledFlag(flagSet(fs, "sandbox"), *sandboxEnabled, fc),
		SandboxWriteDirs:          sandboxResumeDirs(fs, *sandboxWriteDirs, fc),
		SandboxMaxActiveProcesses: pickInt(*sandboxMaxActive, fc.Sandbox.MaxActiveProcesses),
		SandboxACLMaxObjects:      pickInt(*sandboxACLMaxObjects, fc.Sandbox.ACLMaxObjects),
		SandboxStrictGroups:       pickBoolFlag(flagSet(fs, "sandbox-strict-groups"), *sandboxStrictGroups, fc.Sandbox.StrictGroups),
		Remote:                    pickBoolFlag(flagSet(fs, "remote"), *remoteEnabled, false),
		RemoteHost:                strings.TrimSpace(*remoteHost),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "装配工具注册表:", err)
		return 1
	}
	// 沙箱必须收掉（WER 排除项是持久的用户级注册表写入，F8）。
	if sbEnv != nil {
		defer func() { _ = sbEnv.Close() }()
	}

	// 大 payload 外置 blob（v4-3-A，G2e/D4）：只走文件配置面（TOML `[blob]`，与 run 同口径）。
	// dir 空 = 关 = 零行为变化；非空则建 blobfs——**重放路径落下的 completed 同样外置**
	// （loop 侧口径，见 core/loop/replay.go）。
	//
	// **位置**：必须在 standby 装配**之前**建好——standby 的交付标记被外置时要经它
	// 读回（host/standby.go 的 deliverySeqOf；critic P2：外置丢标记 → 集合退化）。
	var blobStore storage.Blob
	if dir := strings.TrimSpace(fc.Blob.Dir); dir != "" {
		bs, berr := blobfs.New(dir)
		if berr != nil {
			fmt.Fprintln(os.Stderr, "建 blob 外置存储:", berr)
			return 1
		}
		blobStore = bs
	}

	// standby 待机工具（G-f）：恢复路径也要能**重放**它。
	//
	// 崩溃时 standby 的 tool.call.intent 已落、completed 还没落 → 恢复流程把它当
	// in-flight op（Replay=safe = 重放 = 重新执行 = 重新挂起）。重放走的是
	// *core/loop.Loop：它按 op.Intent.ToolName 去**注册表**取执行器——standby 此前
	// 只在 hub 里注册（hub 没有恢复入口），这里的注册表只有内置工具 → 查不到执行器
	// → 重放 fail-close（落 failed、处置结论 "replayed_failed = 需人工重做"）。
	//
	// 这里补上：把**同一个执行器**注册进恢复路径的注册表（host/standby_resume.go）→
	// 重放带**原 tool_call_id** 执行 → 执行器回查事件流里那条原 intent → 重放游标
	// = 原 intent 的 seq → **中断期间到达的消息能被看见**：已到 → 立即 completed
	// （如实收口）；未到 → 重新挂起（有界，到点如实收口"未等到"）。
	//
	// 对**续跑**（-continue）零影响：模型可见集由策略闸门定（resume 的策略引擎
	// 默认空 = fail-close deny → 工具不进可见集），standby 与内置工具同口径。
	if err := host.RegisterStandbyForResume(reg, st, sid, blobStore); err != nil {
		fmt.Fprintln(os.Stderr, "装配待机工具（恢复路径）:", err)
		return 1
	}

	// MCP client（v3-S1，N1）：resume 的**重放路径真的执行**工具调用，所以被重放的
	// op 若引用 `mcp__<server>__<tool>`，那个 server 必须在这儿连上——否则重放会
	// 撞上"工具未注册"而失败（fail-close 但不可用）。
	//
	// 与 run/serve 同一配置面（[[mcp.servers]]）、同一装配口径（host/mcp.go）；
	// 连接失败非致命（ surfaced 到 stderr，重放继续——不含 MCP 工具的 op 照常重放）。
	mcpMgr, mcpErrs, merr := assembleRunnerMCP(context.Background(), fc.MCP.MCPConfig(), reg)
	if merr != nil {
		fmt.Fprintln(os.Stderr, "MCP 配置:", merr)
		return 2
	}
	if mcpMgr != nil {
		defer func() { _ = mcpMgr.Close() }() // 收尸：server 进程树归本进程（陷阱 5）
	}
	for _, e := range mcpErrs {
		fmt.Fprintf(os.Stderr, "[alasol] MCP server 装配有问题（该 server 的工具未注册）: %v\n", e)
	}

	// provider：恢复流程本身只重放工具、不调模型（noop）；要**续跑对话**时才用真 provider。
	prov := core.Provider(noopProvider{})
	modelName := "resume"
	if strings.TrimSpace(*continuePrompt) != "" {
		modelName, err = resolveModel(*model)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		prov = openai.New(openai.Options{
			BaseURL: *baseURL,
			APIKey:  os.Getenv(*apiKeyEnv), // 凭据只从 env 取，不进 argv
			Stream:  true,
		})
		if testProviderOverride != nil {
			// 测试注入的脚本 provider（不真打模型 API；见 serve.go 的说明）。
			prov = testProviderOverride
		}
	}

	// 续跑事件的归属 = 该 session 的**真实主 agent**（durable.PrimaryAgentID 的口径）。
	//
	// 不能用一次性随机 ULID 当占位：续跑产生的 user.prompt / model.turn / context.assembled
	// 都挂在那个随机 id 下 → 幽灵 agent（审计归属失真，连续两次续跑产生两个幽灵 agent）。
	// 重放路径不受影响：它按 op 自身的 session/agent 落结果事件。
	ctx := context.Background()
	events, err := runtime.ReplaySession(ctx, st, sid)
	if err != nil {
		fmt.Fprintln(os.Stderr, "回放事件流失败:", err)
		return 1
	}
	agentID := durable.PrimaryAgentID(events)

	// WAL 预写日志（v4-4-B）：只走文件配置面（TOML `[wal]`）。dir 空 = 关 = 零行为变化。
	//
	// 两件事：① **resume 时自动打印一行 WAL 摘要**（崩溃后跑 resume 就看见"上次崩在哪儿"，
	// 不必另开命令）；② 续跑那一轮（-continue）的 loop 也挂 WAL（与 run/serve 同口径）。
	walDir := strings.TrimSpace(fc.Wal.Dir)
	var walStore *walfile.Store
	if walDir != "" {
		ws, werr := walfile.New(walDir)
		if werr != nil {
			fmt.Fprintln(os.Stderr, "建 WAL 预写日志目录:", werr)
			return 1
		}
		walStore = ws
		// per-session fd 必须收口（不收就泄漏句柄；与 blobfs/沙箱同纪律）。
		defer func() { _ = walStore.Close() }()
	}

	// 多 agent 环境（缺口 8）：**只在 -continue（续跑）这条路径上**装配。
	//
	// 装配内容：MultiAgent 开 → 类型注册表 + agent 树（root = 该 session 的真实主 agent，
	// durable.PrimaryAgentID 口径，与续跑的 loop.AgentID 同源）+ loop.Agents；关（默认）
	// → nil = 单 agent 模式零行为变化（不建树、不传 Agents、不校验 agent_types）。
	//
	// 为什么**挪进 -continue 分支**（T5-3）：**纯恢复**（不带 -continue）压根不建 agent
	// 树、不派生子 agent——它只回放工具调用 + 落结果事件（这里的 loop 是 durable.Replayer，
	// 不是 agent 回路）。此时校验 agent_types = **用不到的能力拒绝启动**：同一份 TOML 在
	// serve 能跑（host 装配）、在纯恢复里却起不来（装载期 fail-close）——这与"MultiAgent
	// 关 = 不建不校验"是同一条纪律（不许用一个用不到的能力拒绝启动）。
	//
	// **两前端口径分叉**（如实记）：`-continue` 续跑 = run/resume 自己的装配面（直连
	// loop，无 hub 面：没有在线表注入 / fork 集合 / 发现表 Claim / loop·树缓存）；serve =
	// host 装配面（hub）。同一份 TOML 在两边的**能力面不同**：serve 有 hub 的
	// ChildStrategy 钩子（tiered 子策略可用），run/resume **没接**（见 runnerAgentsEnv
	// 的"ChildStrategy 挂账"与本文件里的点名 Warn）。
	//
	// ChildStrategy 挂账（见 runnerAgentsEnv 注释）：run/resume 的子 agent 策略钩子未接，
	// 子 agent 用默认上下文策略。
	var agentsEnv *loop.Agents
	if strings.TrimSpace(*continuePrompt) != "" {
		env, aerr := runnerAgentsEnv(fc.MultiAgent, modelName, fc.AgentTypeConfigs(), agentID, reg)
		if aerr != nil {
			fmt.Fprintln(os.Stderr, "装配多 agent 环境:", aerr)
			return 1
		}
		agentsEnv = env
	}

	// 多 agent 策略规则（缺口 8）：MultiAgent 开时补 spawn/complete 的 allow（与 host.New
	// 同口径，见 multiagent.go 的 runnerMultiAgentPolicyRules）。resume 的策略引擎默认
	// 空（重放不重跑策略链 M9——此引擎不被 Replay 用到，只约束 -continue 续跑路径的工具
	// 可见性/执行判定）：关（默认）→ policy.New() 逐字不变 = 零行为变化；开 → 补两条
	// allow，否则 spawn 被 fail-close deny 挡掉、续跑路径派不出子 agent（接线即死）。
	policyRules := runnerMultiAgentPolicyRules(fc.MultiAgent)

	// G-c（F7）：折叠该 session 的 config.changed——设计 §3.1"持久化影响**后续所有轮**"，
	// CLI 续跑（-continue）也是轮：不接生效链的话，被改过配置的 session 续跑会悄悄用回
	// -model/环境变量那层的默认值。core/sessioncfg 是纯状态机：把该 session 的事件喂
	// 进去即得当前配置（事件流是唯一事实源，F5；上面 ReplaySession 已读整场事件）。
	// CLI 单进程、无并发写者、续跑只有一轮 → **折一次即可**（host 面才需要每次 Run
	// 重取 fold）。纯恢复（不带 -continue）不调 Run → 钩子不触发，折了也无害。
	cfgTable := sessioncfg.New()
	for i := range events {
		cfgTable.Apply(&events[i])
	}
	// 取值口径与 serve 面**同源**（host.RunOverridesFromConfig = 唯一取值口：model/
	// effort 取生效覆盖，provider 覆盖凭据只从 env 取、缺失 fail-close 绝不回退——
	// gap2/G-d）。覆盖优先于 -model flag：flag 属"hub 默认"层，session fold 在其上
	// （与 serve 同一条生效链）。nil newProvider = 直造覆盖 provider（CLI 无 hub 缓存）。
	runOverrides := func(context.Context) (loop.RunOverrides, error) {
		ov, oerr := host.RunOverridesFromConfig(cfgTable.Current(sid), nil)
		if oerr != nil {
			return loop.RunOverrides{}, fmt.Errorf("session %s 的配置覆盖（G-c 生效链）: %w", sid, oerr)
		}
		return ov, nil
	}

	// loop：恢复流程里它只作 durable.Replayer（重放时按 op.Intent 直接执行工具、落结果事件）。
	//
	// ReasoningEffort（G14）：上面已解析（flag > 文件 + 白名单校验）；这里只把它交给 loop。
	lp, err := loop.New(loop.Config{
		Provider:        prov,
		Bus:             bus.New(st),
		Registry:        reg,
		Policy:          policy.New(policyRules...), // 重放不重跑策略链（M9：T0 已授权）；此引擎不被 Replay 用到
		SessionID:       sid,
		AgentID:         agentID, // 真实主 agent（续跑事件的归属）
		Model:           modelName,
		ReasoningEffort: reasoningEffortVal,
		RunOverrides:    runOverrides,
		// G-c（F7）：续跑也走生效链——Run 开头冻结 fold 的覆盖（优先于上面的基值）。
		Store: st,
		// D4：大 payload 外置（v4-3-A，G2e）——重放落下的 completed 与续跑那一轮的
		// completed 同口径外置（否则"大 payload 不进事件流"在 resume 路径上失效）。
		BlobSink:      blobStore,
		BlobMaxInline: fc.Blob.MaxInline,
		// WAL 预写日志（v4-4-B）——nil = 关 = 零行为变化。续跑那一轮的 loop 与 run/serve
		// 同口径记 WAL（turn.begin/delta/end/failed + blob.plan）。
		WalSink: runnerWalSink(walStore, sid),
		// 多 agent 环境（缺口 8）——nil = 单 agent 模式（默认，零行为变化）。
		// 非 nil = 树 + spawn/complete 接线（形态对照 host/hub.go loopFor 的 Agents 段）。
		Agents: agentsEnv,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "构造 resume loop:", err)
		return 1
	}

	opts := durable.ResumeOptions{Replayer: lp, ForceResume: *force}
	switch *verifyMode {
	case "git":
		wd, _ := os.Getwd()
		opts.Verifier = verify.GitCommitVerifier{RepoDir: wd, Mode: verify.ModeExists}
	case "none":
		// 不核验：所有 verify 操作因缺 Verifier 转人工（fail-safe）。
	default:
		fmt.Fprintf(os.Stderr, "未知 -verify 模式 %q（want git | none）\n", *verifyMode)
		return 2
	}

	// standby 重挂要**先报一行**（critic F4）：恢复路径会重新挂起（最长 30 分钟的
	// 硬上限），不报的话人只看到"命令卡住了"——不知道它在等消息、还能等多久。
	//
	// 判据 = **事实面**：事件流里"有 intent、没结果"的 standby 调用（host 的
	// 折叠口径，与 WAL 报告用的是同一份事实）。
	if pending := host.StandbyPendingToolCalls(events); len(pending) > 0 {
		fmt.Fprintf(os.Stderr, "[resume] standby 重挂中（最多 %s）\n", resumeCapText(resumeReplayCap))
	}

	// 恢复 ctx **有上界**（critic F4）：重放（含 standby 重新挂起）不能无界——
	// 上界到了就如实收场（重放失败 → 需人工重做），不许静默挂半小时。
	replayCtx, cancelReplay := context.WithTimeout(ctx, resumeReplayCap)
	defer cancelReplay()
	rep, err := durable.Resume(replayCtx, st, sid, opts)
	if err != nil {
		// 重放/落盘断裂：报告已处理的部分仍打印到 stderr 供诊断，退出码 1。
		dumpReport(os.Stderr, rep)
		dumpSummary(os.Stderr, rep)
		// v4-4-B：**失败分支也要打 WAL 摘要**（critic F9）——"上次崩在哪儿"恰恰是恢复失败
		// 时最该看见的那句话（resume 失败常常就是上次崩在半路的延续）。先打摘要再 return 1。
		if s := walResumeSummary(ctx, st, walDir, sid); s != "" {
			fmt.Fprintln(os.Stderr, s)
		}
		fmt.Fprintln(os.Stderr, "resume 失败:", err)
		return 1
	}
	dumpReport(os.Stdout, rep)
	dumpSummary(os.Stderr, rep)

	// v4-4-B：WAL 崩溃恢复摘要（一行，stderr）。**观察面不是写回面**：只报告"上次崩在
	// 第几轮/吐过多少 delta/哪些 blob 是孤儿候选"，不补事件流、不改任何状态。
	// 未配置 WAL / 无异常 → 空串（不打印：没话说的时候不说话）。
	if s := walResumeSummary(ctx, st, walDir, sid); s != "" {
		fmt.Fprintln(os.Stderr, s)
	}

	// 续跑：把事件流 fold 出的对话历史灌回 loop（v1-S1：对话入事件流 → 可回放 → 可续跑）。
	if strings.TrimSpace(*continuePrompt) == "" {
		return 0
	}
	// 重读事件流：恢复流程刚落的结果事件（in-flight 的 completed/failed）也要进历史 ✅。
	events, err = runtime.ReplaySession(ctx, st, sid)
	if err != nil {
		fmt.Fprintln(os.Stderr, "回放事件流失败:", err)
		return 1
	}
	// 口径：**整场全景**（有意，不按 agent 视角过滤——见本函数的文档段"续跑折叠的是整场
	// session 的对话"）。三种口径别混：resume = 整场全景、fork = 主对话（现状）、
	// 主轮次 = 主 agent 视角（host.foldSessionHistory 按主 agent id 预过滤，F1）。
	history := runtime.FoldConversation(events)
	fmt.Fprintf(os.Stderr, "[resume] 续跑：已 fold 出 %d 条历史消息\n", len(history))
	res, err := lp.RunWithHistory(ctx, history, *continuePrompt)
	if err != nil {
		fmt.Fprintln(os.Stderr, "续跑失败:", err)
		return 1
	}
	fmt.Println(res.Text)
	fmt.Fprintf(os.Stderr, "[resume] 续跑 turns=%d tool_calls=%d events=%d\n", res.Turns, res.ToolCalls, res.Events)
	return 0
}

// sandboxResumeDirs 解析 `alasol resume` 的沙箱允许写根（v2-S4）：显式 flag > 文件。
//
// 与 run 的 sandboxRunDirs 同口径（F20/F21：同一个解析规则）。
func sandboxResumeDirs(fs *flag.FlagSet, flagVal string, fc *host.FileConfig) []string {
	if flagSet(fs, "sandbox-write-dirs") {
		return splitList(flagVal)
	}
	if len(fc.Sandbox.WriteDirs) > 0 {
		return fc.Sandbox.WriteDirs
	}
	return nil
}

// dumpReport 把 ResumeReport 打印成缩进 JSON（单命令一行输出）。
func dumpReport(w *os.File, rep durable.ResumeReport) {
	out, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		fmt.Fprintf(w, "{\"error\":%q}\n", err.Error())
		return
	}
	fmt.Fprintln(w, string(out))
}

// dumpSummary 打印处置汇总（人看的两栏：重放成功 / 重放失败需人工重做）。
//
// 为什么要分栏（P2）：`replayed`（成功）与 `replayed_failed`（工具侧失败、需人工重做）
// 显示成同一栏会让人以为"都搞定了"——失败的那些必须单独看得见。
// 汇总打到 stderr：stdout 留给机器消费的纯 JSON 报告。
func dumpSummary(w *os.File, rep durable.ResumeReport) {
	var replayed, replayedFailed, verified, manual, aborted int
	for _, d := range rep.Dispositions {
		switch d.Disposition {
		case contracts.ResumeDispositionReplayed:
			replayed++
		case contracts.ResumeDispositionReplayedFailed:
			replayedFailed++
		case contracts.ResumeDispositionVerified:
			verified++
		case contracts.ResumeDispositionManual:
			manual++
		case contracts.ResumeDispositionAborted:
			aborted++
		}
	}
	fmt.Fprintf(w, "[resume] 处置汇总：已重放成功 %d · 重放失败需人工重做 %d · 核验通过 %d · 转人工 %d · 本次放弃 %d\n",
		replayed, replayedFailed, verified, manual, aborted)
	if replayedFailed > 0 {
		fmt.Fprintf(w, "[resume] ⚠ 有 %d 个调用重放后失败——需人工重做（详见 dispositions 的 detail）\n", replayedFailed)
	}
}

// noopProvider 是 resume 回路专用的占位 Provider：loop.New 要求非 nil，但重放工具不调模型。
type noopProvider struct{}

func (noopProvider) Run(context.Context, core.ModelRequest, func(core.ProviderEvent)) (core.ModelResult, error) {
	return core.ModelResult{}, fmt.Errorf("resume 回路不调用模型（仅重放工具，X2）")
}
