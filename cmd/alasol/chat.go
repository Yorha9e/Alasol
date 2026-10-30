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
//	/btw <问题>  旁支 fork（K29）：带当前对话上下文跑一轮，结果只给用户、不回灌主 session
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
	// 档位白名单（fail-close）：**配置被拒 = exit 2**（与 run / resume 同一口径；F2）。
	if rejectInvalidReasoningEffort(cfg) {
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
	fmt.Println("[alasol] 输入内容开始对话；/table 在线表 · /ctx 上下文构成 · /cost 成本统计 · /btw 旁支询问 · /workflow 工作流能力包 · /quit 退出")

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
		// /btw <问题>：旁支 fork（K29）——带主 session 的对话上下文跑一轮，结果**只给用户**、
		// **不回灌**主 session 的事件流（主 session 的 user.prompt/model.turn/context.assembled
		// 零新增）。命令解析与 TUI 共用同一个口径（host.ParseBtwCommand）：**空格与 tab
		// 都算分隔符**（"/btwxyz" 不当命令，按普通 prompt 走）。
		if question, isBtw := host.ParseBtwCommand(line); isBtw {
			if question == "" {
				fmt.Println("用法：/btw <问题>（旁支询问：带当前上下文跑一轮，不影响主会话历史）")
				continue
			}
			res, err := h.RunBtw(ctx, sid, question)
			if err != nil {
				// 失败不退出 REPL（同普通对话：改一句话重试比重启便宜）。
				fmt.Fprintln(os.Stderr, "/btw 失败:", err)
				continue
			}
			// 结果只给用户展示——**不**进主 session（K29：旁支归属独立 fork session）。
			if res.Text != "" {
				fmt.Printf("btw> %s\n", res.Text)
			} else {
				fmt.Println("btw> （无文本输出）")
			}
			// 裁切告知（F7/N5）：两种裁切**分开说**——上限截断（太长装不下）与配平
			// 丢弃（工具调用配对不完整、provider 发不出去）不是一回事。
			if res.HistoryTruncated {
				fmt.Fprintf(os.Stderr, "[btw] 主对话已按旁支上下文上限截断（只带最近 %d 条）\n", res.HistoryMessages)
			}
			if res.HistoryTrimmed {
				fmt.Fprintf(os.Stderr, "[btw] 旁支主对话里有工具调用配对不完整的片段被丢弃（只带 %d 条）\n", res.HistoryMessages)
			}
			fmt.Fprintf(os.Stderr, "[btw] 旁支 session=%s turns=%d tool_calls=%d events=%d（主 session=%s 事件流零新增）\n",
				res.ForkSessionID, res.Turns, res.ToolCalls, res.Events, sid)
			continue
		}
		// /workflow [<id>|off]：激活/停用/列出工作流能力包（v3-S3，W3）。激活只把能力包
		// 哲学文本拼进 system prompt 供主 agent 选择（非强制，W21）；框架不校验编排序列。
		if arg, isWF := host.ParseWorkflowCommand(line); isWF {
			switch arg {
			case "":
				// 列出可用能力包。
				pkgs := h.Workflows().List()
				if len(pkgs) == 0 {
					fmt.Println("（没有可用的工作流能力包）")
					continue
				}
				fmt.Println("可用工作流能力包：")
				for _, p := range pkgs {
					cmd := p.Command()
					if cmd == "" {
						cmd = "（显式触发）"
					}
					active := ""
					if h.ActiveWorkflow(sid) != nil && h.ActiveWorkflow(sid).ID() == p.ID() {
						active = " [已激活]"
					}
					fmt.Printf("  %-20s 命令=/%s%s\n", p.ID(), cmd, active)
				}
			case "off":
				h.DeactivateWorkflow(sid)
				fmt.Println("已停用工作流（回到默认 system prompt）")
			default:
				p, werr := h.ActivateWorkflow(ctx, sid, arg)
				if werr != nil {
					fmt.Fprintln(os.Stderr, "/workflow 失败:", werr)
					continue
				}
				fmt.Printf("已激活工作流 %q：哲学文本将注入 system prompt 供主 agent 选择（非强制）。\n", p.ID())
			}
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
			fmt.Println("命令：/table 在线表 · /ctx 上下文构成 · /cost 成本统计 · /btw <问题> 旁支询问 · /workflow [<id>|off] 工作流能力包 · /session 当前 session · /quit 退出")
			continue
		}
		// /<命令>：能力包**自己声明**的 slash 触发入口（W3，如 tower 的 /tower）。
		// 这是 Registry.ByCommand 的生产调用方——声明出来的命令真能敲（不是死字段）；
		// 事件 workflow.activated 的 command 字段记的就是这个实际触发的命令名。
		if cmd, isTrigger := h.WorkflowCommandTrigger(line); isTrigger {
			p, werr := h.ActivateWorkflowByCommand(ctx, sid, cmd)
			if werr != nil {
				fmt.Fprintln(os.Stderr, "/"+cmd+" 失败:", werr)
				continue
			}
			fmt.Printf("已激活工作流 %q（命令=/%s）：哲学文本将注入 system prompt 供主 agent 选择（非强制）。\n", p.ID(), cmd)
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
