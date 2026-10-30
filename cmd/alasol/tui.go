package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/ui/tui"
)

// tuiMain 是 `alasol tui`：起三面 TUI（对话 / 审批 / 在线表，O6/B2）。
//
// 与 chat 的关系：同一个进程内 hub（host.New 唯一组合根），但用 bubbletea 渲染，且审批
// 经 tui.Approver 桥接进 UI（终端人审在界面里完成，不冲突 bubbletea 的终端接管）。
//
// 装配顺序（审批桥要先于 hub 建，才能注入 cfg.Approver）：
//
//	flags → buildConfig（H15 文件优先级）→ 建 tui.Approver → host.New(Approver=桥) → tui.Run
//
// 审批要真正触发，需 -ask 列出工具（命中 ask 规则才问人）；否则工具按 allow/deny 直接走。
func tuiMain(args []string) int {
	fs := flag.NewFlagSet("tui", flag.ExitOnError)
	f := registerHubFlags(fs)
	session := fs.String("session", "", "复用已有 session id（空=新建一个）")
	f.parse(fs, args)

	cfg, err := f.config() // buildConfig：flag>env>文件（H15）
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	// 档位白名单（fail-close）：**配置被拒 = exit 2**（与 run / resume 同一口径；F2）。
	if rejectInvalidReasoningEffort(cfg) {
		return 2
	}
	// 审批桥：先建，注入 hub；TUI 里同一个桥把请求渲染到审批面。
	appr := tui.NewApprover()
	cfg.Approver = appr
	if testProviderOverride != nil {
		cfg.Provider = testProviderOverride // 测试注入脚本 provider
	}

	h, err := host.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "装配失败:", err)
		return 1
	}
	defer h.Close()

	sid := contracts.Ulid(strings.TrimSpace(*session))
	if sid == "" {
		sid = host.NewSession()
	}

	if err := tui.Run(tui.Options{Hub: h, Session: sid, Approver: appr}); err != nil {
		fmt.Fprintln(os.Stderr, "TUI 失败:", err)
		return 1
	}
	return 0
}
