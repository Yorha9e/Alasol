// Command alasol 是 v0 的最小入口（阶段 4）：装配 core + runtime，跑一个 session 的真机回路。
//
// 用法：
//
//	alasol run -prompt "读一下 README 并总结" \
//	          [-model gpt-4o-mini] [-base-url https://api.openai.com/v1] \
//	          [-api-key-env OPENAI_API_KEY] [-approval deny-all|always-allow|terminal] \
//	          [-allow read,write,bash] [-db alasol.db] [-max-turns 20] [-system "..."]
//
// 纪律：
//   - 审批默认 deny-all（fail-close）；always-allow / terminal 必须显式选
//   - -allow 生成 source=cli 的 allow 规则；空 = 一切工具 deny（fail-close）
//   - API key 只经 env 注入（-api-key-env 只传**变量名**），永不进 argv/事件/日志
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "run" {
		fmt.Fprintln(os.Stderr, "usage: alasol run -prompt <text> [flags]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	prompt := fs.String("prompt", "", "本轮用户提示（必填）")
	model := fs.String("model", "gpt-4o-mini", "模型名（原样传给 provider）")
	baseURL := fs.String("base-url", "", "OpenAI 兼容端点（空=官方）")
	apiKeyEnv := fs.String("api-key-env", "OPENAI_API_KEY", "持有 API key 的环境变量名")
	approval := fs.String("approval", "deny-all", "审批策略：deny-all | always-allow | terminal")
	allow := fs.String("allow", "", "允许的工具名（逗号分隔；空=一切 deny）")
	dbPath := fs.String("db", "alasol.db", "事件库路径（SQLite）")
	maxTurns := fs.Int("max-turns", 0, "最大轮数（0=默认 20）")
	system := fs.String("system", "", "系统提示")
	_ = fs.Parse(os.Args[2:])
	if *prompt == "" {
		fmt.Fprintln(os.Stderr, "-prompt 不能为空")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	r, err := newRunner(runnerOpts{
		Model:      *model,
		BaseURL:    *baseURL,
		APIKey:     os.Getenv(*apiKeyEnv), // 凭据只从 env 取，不进 argv
		Approval:   *approval,
		Allow:      *allow,
		DBPath:     *dbPath,
		MaxTurns:   *maxTurns,
		System:     *system,
		SessionEnv: sessionEnvFromProcess(),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "装配失败:", err)
		os.Exit(1)
	}
	defer r.Close()

	res, err := r.Run(ctx, *prompt)
	if err != nil {
		fmt.Fprintln(os.Stderr, "运行失败:", err)
		os.Exit(1)
	}
	fmt.Println(res.Text)
	fmt.Fprintf(os.Stderr, "[alasol] turns=%d tool_calls=%d events=%d\n", res.Turns, res.ToolCalls, res.Events)
}

// sessionEnvFromProcess 把显式列出的进程 env 键收进 session 注入域（H21 v0）。
// v0 不给模型看任何 env；这里只装配**执行 env** 的 session 级部分。
func sessionEnvFromProcess() map[string]string {
	// v0：session 级注入留空——需要时按 H21 由配置显式声明，不隐式继承。
	return nil
}

var _ = strings.TrimSpace
