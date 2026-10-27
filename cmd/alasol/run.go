package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// runMain 是 `alasol run`：单次真机回路（阶段 4 的行为，一字未改——只是从 main 里
// 搬进自己的函数，好让 main 做子命令分派）。
func runMain(args []string) int {
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
	_ = fs.Parse(args)
	if *prompt == "" {
		fmt.Fprintln(os.Stderr, "-prompt 不能为空")
		return 2
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
		return 1
	}
	defer r.Close()

	res, err := r.Run(ctx, *prompt)
	if err != nil {
		fmt.Fprintln(os.Stderr, "运行失败:", err)
		return 1
	}
	fmt.Println(res.Text)
	fmt.Fprintf(os.Stderr, "[alasol] turns=%d tool_calls=%d events=%d\n", res.Turns, res.ToolCalls, res.Events)
	return 0
}

// sessionEnvFromProcess 把显式列出的进程 env 键收进 session 注入域（H21 v0）。
// v0 不给模型看任何 env；这里只装配**执行 env** 的 session 级部分。
func sessionEnvFromProcess() map[string]string {
	// v0：session 级注入留空——需要时按 H21 由配置显式声明，不隐式继承。
	return nil
}
