// Command alasol 是 v0 的入口：装配 core + runtime，跑一个 session 的真机回路，
// 或起常驻 hub（HTTP + WS + Web UI / 终端 REPL）。
//
// 用法：
//
//	alasol run -prompt "读一下 README 并总结" \
//	          [-model <模型名> | ALASOL_MODEL] [-base-url https://api.openai.com/v1] \
//	          [-api-key-env OPENAI_API_KEY] [-approval deny-all|always-allow|terminal] \
//	          [-allow read,write,bash] [-db alasol.db] [-max-turns 20] [-system "..."]
//
//	alasol models [-base-url ...] [-api-key-env OPENAI_API_KEY] [-config path]
//	          → 列出端点支持的模型名（模型名是端点相关的，没有万能默认值）
//	          → base_url / api_key 也读用户级 TOML 配置（H15：显式 flag > env > 文件，
//	            与 run/serve/resume 同一条优先级）
//
//	alasol resume -session <id> [-db alasol.db] [-verify git|none] [-continue "<提示>"]
//	          → 跑 X4 恢复流程（读 checkpoint → fold → 处置 in-flight → session.resumed），
//	            打印处置明细（safe 重放 / verify 先核验 / unsafe 转人工）
//	          → 加 -continue "<提示>"：恢复后再续跑一轮对话（把 fold 出的历史灌回 loop）。
//	            续跑折叠**整场 session** 的对话、**不区分 agent 视角**（子 agent 的对话也会
//	            进入上下文；按 agent 过滤是后续增强，尚未实现）
//
//	alasol serve [-addr 127.0.0.1:8720] [-db alasol.db] [-model ...] [-allow ...]
//	          → 起 hub：GET / 是 Web UI、GET /ws 是 NDJSON 事件流、GET /api/* 是在线表/上下文
//
//	alasol wal -session <id> [-db alasol.db] [-wal-dir <dir>]
//	          → 崩溃恢复报告（v4-4-B）：扫该 session 的 WAL 与事件流对账，打印"上次崩在
//	            第几轮、跑的什么模型、吐过几段 delta、哪些 tool_call 的 blob 是孤儿候选"。
//	            单行 JSON 到 stdout；未配置 WAL（[wal].dir 空）→ wal_exists=false（不报错）。
//
//	alasol chat [-db alasol.db] [-model ...] [-approval ...] [-allow ...]
//	          → 终端 REPL：对话 / /table 在线表 / /ctx 上下文构成 / /quit
//
// 纪律：
//   - 审批默认 deny-all（fail-close）；always-allow / terminal 必须显式选
//   - -allow 生成 source=cli 的 allow 规则；空 = 一切工具 deny（fail-close）
//   - API key 只经 env 注入（-api-key-env 只传**变量名**），永不进 argv/事件/日志
//   - hub 只监听 loopback（N5b），非 loopback 地址直接拒绝
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var code int
	switch os.Args[1] {
	case "run":
		code = runMain(os.Args[2:])
	case "resume":
		code = resumeMain(os.Args[2:])
	case "serve":
		code = serveMain(os.Args[2:])
	case "acp":
		code = acpMain(os.Args[2:])
	case "mcp-serve":
		code = mcpServeMain(os.Args[2:])
	case "chat":
		code = chatMain(os.Args[2:])
	case "tui":
		code = tuiMain(os.Args[2:])
	case "models":
		code = modelsMain(os.Args[2:])
	case "wal":
		code = walMain(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		code = 0
	default:
		fmt.Fprintf(os.Stderr, "alasol: 未知子命令 %q\n\n", os.Args[1])
		usage()
		code = 2
	}
	os.Exit(code)
}

// usage 打印用法（子命令各自还有自己的 flag 帮助）。
func usage() {
	fmt.Fprint(os.Stderr, `alasol — AI agent harness（v0）

用法：
  alasol run    -prompt <text> [flags]   跑一轮真机回路（单次）
  alasol resume -session <id> [flags]     恢复一个 session（X4 durable：处置 in-flight；
                                        -continue "<提示>" 续跑一轮对话：折叠整场 session 的
                                        对话、不区分 agent 视角）
  alasol serve  [flags]                 起常驻 hub：Web UI + WS 事件流 + HTTP API
  alasol acp   [flags]                 起 ACP agent 端点（JSON-RPC 2.0 over stdio；K19 跨机：
                                        ssh host alasol acp 接入，鉴权交 SSH；ask 审批经
                                        session/request_permission，无人应答=deny fail-close）
  alasol mcp-serve [flags]             起 MCP server（B 类，v4-5；stdio 传输，把 harness 工具面
                                        暴露成 MCP tools）。鉴权=谁拉起谁负责（同 ACP/SSH 口径）；
                                        暴露面由配置 [mcp_server] expose_tools 白名单决定（空=零暴露，
                                        fail-close）。stdout 只走 MCP 帧、日志走 stderr
  alasol chat   [flags]                 终端 REPL：对话 / /table / /ctx / /cost
  alasol tui    [flags]                 三面 TUI（对话/审批/在线表，bubbletea，O6/B2）
  alasol models [flags]                 列出端点支持的模型（发现模型名）
  alasol wal -session <id> [flags]      崩溃恢复报告：扫该 session 的 WAL 与事件流对账
                                        （上次崩在第几轮/跑的什么模型/吐过几段 delta/
                                        哪些 tool_call 的 blob 是孤儿候选）。**观察面，
                                        不写回事件流**；未配 [wal].dir = wal_exists=false

模型名无默认值（模型名是端点相关的）：用 -model 指定或设 ALASOL_MODEL；
不知道叫什么就先跑 `+"`alasol models`"+`。
各子命令的 flags 用 `+"`alasol <子命令> -h`"+` 查看。
`)
}
