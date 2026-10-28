package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Yorha9e/alasol/host"
)

// runMain 是 `alasol run`：单次真机回路（阶段 4 的行为，一字未改——只是从 main 里
// 搬进自己的函数，好让 main 做子命令分派）。v1-S5 补 H15：也读用户级 TOML 配置
// （model/base_url/凭据按 flag>env>文件 收敛，与 serve/chat/tui 同口径）。
func runMain(args []string) int {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	prompt := fs.String("prompt", "", "本轮用户提示（必填）")
	model := fs.String("model", "", "模型名（原样传给 provider；缺省回落 env ALASOL_MODEL，再回落配置文件；无默认值——`alasol models` 可发现）")
	baseURL := fs.String("base-url", "", "OpenAI 兼容端点（空=官方）")
	apiKeyEnv := fs.String("api-key-env", "OPENAI_API_KEY", "持有 API key 的环境变量名")
	approval := fs.String("approval", "deny-all", "审批策略：deny-all | always-allow | terminal")
	allow := fs.String("allow", "", "允许的工具名（逗号分隔；空=一切 deny）")
	dbPath := fs.String("db", "alasol.db", "事件库路径（SQLite）")
	maxTurns := fs.Int("max-turns", 0, "最大轮数（0=默认 20）")
	checkpointEvery := fs.Int("checkpoint-every", 0, "每 N 轮自动 checkpoint（X3；0=只手动）")
	system := fs.String("system", "", "系统提示")
	configPath := fs.String("config", "", "用户级配置文件路径（TOML；空=平台默认位，H15）")
	_ = fs.Parse(args)
	if *prompt == "" {
		fmt.Fprintln(os.Stderr, "-prompt 不能为空")
		return 2
	}

	// H15：读用户级配置（解析失败 → fail-close 报错）。
	fc, err := host.LoadFileConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if fc == nil {
		fc = &host.FileConfig{}
	}
	// model：flag > env > 文件。
	modelName, _ := host.Precedence(*model, os.Getenv("ALASOL_MODEL"), fc.Model)
	if strings.TrimSpace(modelName) == "" {
		fmt.Fprintln(os.Stderr, "缺模型名：用 -model 指定，或设 ALASOL_MODEL，或在配置文件里设 model；可用 `alasol models` 发现")
		return 2
	}
	// base_url：flag > env > 文件。
	baseURLVal, _ := host.Precedence(*baseURL, os.Getenv("ALASOL_BASE_URL"), fc.BaseURL)
	// 凭据：env > 文件（api-key-env 名 flag>文件>默认）。
	keyEnv := host.ResolveAPIKeyEnv(*apiKeyEnv, fc.APIKeyEnv)
	apiKey := host.ResolveCredential(keyEnv, fc.APIKey)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	r, err := newRunner(runnerOpts{
		Model:           modelName,
		BaseURL:         baseURLVal,
		APIKey:          apiKey, // env>文件 已收敛；永不进 argv/事件/日志
		Approval:        *approval,
		Allow:           *allow,
		DBPath:          *dbPath,
		MaxTurns:        *maxTurns,
		CheckpointEvery: *checkpointEvery,
		System:          *system,
		SessionEnv:      sessionEnvFromProcess(),
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
