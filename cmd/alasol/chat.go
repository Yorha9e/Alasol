package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/host"
)

// chatMain 是 `alasol chat`：终端 REPL（阶段 7 的 CLI 侧）。
//
// 命令：
//
//	<任意文本>   发一轮对话（RunPrompt），打印模型最终文本
//	/table       打印在线表（indexer 投影：session / 事件数 / 最后 kind / 更新时间 / 水位）
//	/ctx         打印当前 session 的上下文构成（ContextSnapshot 的 parts 占比）
//	/session     打印当前 session id
//	/quit        退出（等价 EOF / Ctrl-C）
//
// 审批：-approval=terminal 时工具调用的人审提示直接走终端（runtime/approver/terminal）。
// fail-close 默认是 deny-all：不放行任何工具调用，模型只会看到失败回填。
// -ask 列出的工具命中 ask 规则 → 去问 Approver（-approval=terminal 时终端提示，
// deny-all 时无人应答 = deny）。
func chatMain(args []string) int {
	fs := flag.NewFlagSet("chat", flag.ExitOnError)
	f := registerHubFlags(fs)
	session := fs.String("session", "", "复用已有 session id（空=新建一个）")
	f.parse(fs, args)

	cfg, err := f.config()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	h, err := host.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "装配失败:", err)
		return 1
	}
	defer h.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sid := contracts.Ulid(strings.TrimSpace(*session))
	if sid == "" {
		sid = host.NewSession()
	}

	in := bufio.NewScanner(os.Stdin)
	fmt.Printf("[alasol] chat 就绪：session=%s model=%s approval=%s allow=%v\n",
		sid, cfg.Model, *f.approval, splitList(*f.allow))
	fmt.Println("[alasol] 输入内容开始对话；/table 在线表 · /ctx 上下文构成 · /cost 成本统计 · /quit 退出")

	for {
		fmt.Print("you> ")
		if !in.Scan() {
			fmt.Println() // EOF：换行收尾
			break
		}
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		switch line {
		case "/quit", "/exit":
			return 0
		case "/table":
			printTable(ctx, h)
			continue
		case "/ctx":
			printContext(ctx, h, sid)
			continue
		case "/cost":
			printCost(ctx, h, sid)
			continue
		case "/session":
			fmt.Printf("session=%s\n", sid)
			continue
		case "/help":
			fmt.Println("命令：/table 在线表 · /ctx 上下文构成 · /cost 成本统计 · /session 当前 session · /quit 退出")
			continue
		}
		if ctx.Err() != nil {
			break
		}
		res, err := h.RunPrompt(ctx, sid, line)
		if err != nil {
			fmt.Fprintln(os.Stderr, "运行失败:", err)
			// 失败不退出 REPL：改一句话重试比重启会话便宜。
			continue
		}
		if res.Text != "" {
			fmt.Printf("alasol> %s\n", res.Text)
		}
		fmt.Fprintf(os.Stderr, "[alasol] turns=%d tool_calls=%d events=%d\n",
			res.Turns, res.ToolCalls, res.Events)
	}
	return 0
}

// printTable 打印在线表（/table）。
func printTable(ctx context.Context, h *host.Hub) {
	rows, err := h.Sessions(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "拉在线表失败:", err)
		return
	}
	if len(rows) == 0 {
		fmt.Println("（还没有任何 session）")
		return
	}
	fmt.Printf("%-28s %8s %-24s %-22s %10s\n", "session", "事件数", "最后 kind", "更新时间", "水位")
	for _, r := range rows {
		fmt.Printf("%-28s %8d %-24s %-22s %10d\n",
			r.SessionID, r.EventCount, r.LastKind, r.UpdatedAt, r.Watermark)
	}
}

// printContext 打印当前 session 的上下文构成（/ctx）。
func printContext(ctx context.Context, h *host.Hub, sid contracts.Ulid) {
	raw, ok, err := h.ContextSnapshot(ctx, sid)
	if err != nil {
		fmt.Fprintln(os.Stderr, "拉上下文失败:", err)
		return
	}
	if !ok {
		fmt.Printf("（session %s 还没有 context.assembled 快照）\n", sid)
		return
	}
	var snap contracts.ContextSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		fmt.Fprintln(os.Stderr, "快照解不开:", err)
		return
	}
	fmt.Printf("session=%s turn=%d tokens_used≈%d / budget=%d\n",
		snap.SessionID, snap.Turn, snap.TokensUsed, snap.TokensBudget)
	if len(snap.Parts) == 0 {
		fmt.Println("（没有 part）")
		return
	}
	var total int64
	for _, p := range snap.Parts {
		total += p.Tokens
	}
	for _, p := range snap.Parts {
		pct := 0.0
		if total > 0 {
			pct = 100 * float64(p.Tokens) / float64(total)
		}
		fmt.Printf("  %-10s %-8s entries=%-4d tokens≈%-8d %5.1f%%  %s",
			p.Source, p.Kind, p.Entries, p.Tokens, pct, p.State)
		if p.TargetTokens != nil {
			fmt.Printf(" → %d", *p.TargetTokens)
		}
		if len(p.Origins) > 0 {
			fmt.Printf(" origins=%v", p.Origins)
		}
		if p.SummaryRef != nil {
			fmt.Printf(" summary=%s", *p.SummaryRef)
		}
		fmt.Println()
	}
}

// printCost 打印当前 session 的 token 成本统计（/cost，J21：usage 只进成本）。
func printCost(ctx context.Context, h *host.Hub, sid contracts.Ulid) {
	view, ok, err := h.TokenCost(ctx, sid)
	if err != nil {
		fmt.Fprintln(os.Stderr, "拉成本统计失败:", err)
		return
	}
	if !ok {
		fmt.Printf("（session %s 还没有带 usage 的 model.turn）\n", sid)
		return
	}
	fmt.Printf("session=%s tokens: input=%d output=%d total=%d（%d 个计量轮）\n",
		sid, view.InputTokens, view.OutputTokens, view.TotalTokens, view.ModelTurns)
	if len(view.ByModel) == 0 {
		return
	}
	// 按模型分组（字典序，确定性输出）。
	models := make([]string, 0, len(view.ByModel))
	for m := range view.ByModel {
		models = append(models, m)
	}
	sort.Strings(models)
	fmt.Printf("  %-24s %10s %10s %8s\n", "model", "input", "output", "turns")
	for _, m := range models {
		mu := view.ByModel[m]
		fmt.Printf("  %-24s %10d %10d %8d\n", m, mu.InputTokens, mu.OutputTokens, mu.Turns)
	}
}
