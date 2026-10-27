package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/durable"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	"github.com/Yorha9e/alasol/runtime/verify"
)

// resumeMain 是 `alasol resume -session <id> [-db ...]`：跑 X4 的恢复流程并打印报告（处置明细）。
//
// 装配：store → registry(read/write/bash) → loop（仅作 durable.Replayer，不调模型）→ 可选 git Verifier
// → durable.Resume。重放 replay=safe（及 verify 不通过）的操作会**真正再次执行**对应工具并落结果；
// verify 先核验外部状态；unsafe 不重放、转人工。报告以单行 JSON 打到 stdout。
func resumeMain(args []string) int {
	fs := flag.NewFlagSet("resume", flag.ExitOnError)
	session := fs.String("session", "", "要恢复的 session id（必填，26 字符 ULID）")
	dbPath := fs.String("db", "alasol.db", "事件库路径（SQLite）")
	verifyMode := fs.String("verify", "git", "verify 类操作的核验器：git（按 git-commit 锚点核验）| none（全部转人工）")
	force := fs.Bool("force", false, "接管未释放的旧 resume 认领（确认上一次已崩溃时才用）")
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

	// registry：内置 read/write/bash（resume 重放工具用）。
	reg := registry.New(registry.NewPolicyAdjudicator(policy.New()))
	if err := registerBuiltinTools(reg); err != nil {
		fmt.Fprintln(os.Stderr, "装配工具注册表:", err)
		return 1
	}

	// loop 仅作 durable.Replayer：重放时按 op.Intent 直接执行工具、落结果事件（不调模型）。
	// Provider 用 noop（loop.New 要求非 nil；resume 回路永不打模型）。
	lp, err := loop.New(loop.Config{
		Provider:  noopProvider{},
		Bus:       bus.New(st),
		Registry:  reg,
		Policy:    policy.New(), // 重放不重跑策略链（M9：T0 已授权）；此引擎不被 Replay 用到
		SessionID: sid,
		AgentID:   core.NewUlid(), // 占位：Replay 用 op 自身的 session/agent，不用 loop 的
		Model:     "resume",
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

	rep, err := durable.Resume(context.Background(), st, sid, opts)
	if err != nil {
		// 重放/落盘断裂：报告已处理的部分仍打印到 stderr 供诊断，退出码 1。
		dumpReport(os.Stderr, rep)
		dumpSummary(os.Stderr, rep)
		fmt.Fprintln(os.Stderr, "resume 失败:", err)
		return 1
	}
	dumpReport(os.Stdout, rep)
	dumpSummary(os.Stderr, rep)
	return 0
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
