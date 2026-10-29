package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/durable"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime"
	"github.com/Yorha9e/alasol/runtime/provider/openai"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	"github.com/Yorha9e/alasol/runtime/verify"
)

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
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "装配工具注册表:", err)
		return 1
	}
	// 沙箱必须收掉（WER 排除项是持久的用户级注册表写入，F8）。
	if sbEnv != nil {
		defer func() { _ = sbEnv.Close() }()
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

	// loop：恢复流程里它只作 durable.Replayer（重放时按 op.Intent 直接执行工具、落结果事件）。
	lp, err := loop.New(loop.Config{
		Provider:  prov,
		Bus:       bus.New(st),
		Registry:  reg,
		Policy:    policy.New(), // 重放不重跑策略链（M9：T0 已授权）；此引擎不被 Replay 用到
		SessionID: sid,
		AgentID:   agentID, // 真实主 agent（续跑事件的归属）
		Model:     modelName,
		Store:     st,
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

	rep, err := durable.Resume(ctx, st, sid, opts)
	if err != nil {
		// 重放/落盘断裂：报告已处理的部分仍打印到 stderr 供诊断，退出码 1。
		dumpReport(os.Stderr, rep)
		dumpSummary(os.Stderr, rep)
		fmt.Fprintln(os.Stderr, "resume 失败:", err)
		return 1
	}
	dumpReport(os.Stdout, rep)
	dumpSummary(os.Stderr, rep)

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
