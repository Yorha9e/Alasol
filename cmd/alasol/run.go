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

	sandboxEnabled := fs.Bool("sandbox", false,
		"启用 SandboxedExecutionEnv（v2-S4，K8/H9；默认关=走 local）。注意：会改写允许根的 DACL 且不还原")
	sandboxWriteDirs := fs.String("sandbox-write-dirs", "",
		"沙箱允许写根（逗号分隔）。口径：显式列出的根 **+ CWD** 一起进允许集（沙箱自管的临时根也在内）")
	sandboxMaxActive := fs.Int("sandbox-max-active", 0, "沙箱 job object 活动进程上限（0=默认 64）")
	sandboxACLMaxObjects := fs.Int("sandbox-acl-max-objects", 0, "沙箱施加 ACL 前数子树对象的上限（0=默认 20 万）")
	sandboxStrictGroups := fs.Bool("sandbox-strict-groups", false,
		"沙箱严格组 SID 模式：把 BUILTIN 的 Users SID 也 deny-only（默认关）。"+
			"开了能堵上 C:\\ProgramData 这类只授予 Users 写权限的目录；"+
			"代价是用 overlapped I/O 写文件的程序（Go/.NET/Rust/Node）会写不进去")
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
		Model:                     modelName,
		BaseURL:                   baseURLVal,
		APIKey:                    apiKey, // env>文件 已收敛；永不进 argv/事件/日志
		Approval:                  *approval,
		Allow:                     *allow,
		DBPath:                    *dbPath,
		MaxTurns:                  *maxTurns,
		CheckpointEvery:           *checkpointEvery,
		System:                    *system,
		SessionEnv:                sessionEnvFromProcess(),
		Sandbox:                   sandboxEnabledFlag(flagSet(fs, "sandbox"), *sandboxEnabled, fc),
		SandboxWriteDirs:          sandboxRunDirs(fs, *sandboxWriteDirs, fc),
		SandboxMaxActiveProcesses: pickInt(*sandboxMaxActive, fc.Sandbox.MaxActiveProcesses),
		SandboxACLMaxObjects:      pickInt(*sandboxACLMaxObjects, fc.Sandbox.ACLMaxObjects),
		SandboxStrictGroups:       pickBoolFlag(flagSet(fs, "sandbox-strict-groups"), *sandboxStrictGroups, fc.Sandbox.StrictGroups),

		// MCP client（v3-S1，N1）：只走文件配置面（[[mcp.servers]]）。
		// 默认关：一条都没有 = 不装 MCP（零外部进程、零工具）。
		MCP: fc.MCP.MCPConfig(),
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

// sandboxRunDirs 解析 `alasol run` 的沙箱允许写根（v2-S4）：显式 flag > 文件。
//
// 与 serve/chat 的 sandboxWriteDirs 同口径（splitList / pickInt 都是 cmd 包里现成的
// helper，不另造一份——D21：口径只在源头定义一次）。
//
// 注意：**显式 flag 优先于文件**（F20）——`-sandbox-write-dirs ""` 也能压掉文件里的
// write_dirs（flag 显式设了就是"用户说了"）。
func sandboxRunDirs(fs *flag.FlagSet, flagVal string, fc *host.FileConfig) []string {
	if flagSet(fs, "sandbox-write-dirs") {
		return splitList(flagVal)
	}
	if len(fc.Sandbox.WriteDirs) > 0 {
		return fc.Sandbox.WriteDirs
	}
	return nil
}
