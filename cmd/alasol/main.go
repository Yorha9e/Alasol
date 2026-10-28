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
//	alasol models [-base-url ...] [-api-key-env OPENAI_API_KEY]
//	          → 列出端点支持的模型名（模型名是端点相关的，没有万能默认值）
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
	case "chat":
		code = chatMain(os.Args[2:])
	case "tui":
		code = tuiMain(os.Args[2:])
	case "models":
		code = modelsMain(os.Args[2:])
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
  alasol chat   [flags]                 终端 REPL：对话 / /table / /ctx / /cost
  alasol tui    [flags]                 三面 TUI（对话/审批/在线表，bubbletea，O6/B2）
  alasol models [flags]                 列出端点支持的模型（发现模型名）

模型名无默认值（模型名是端点相关的）：用 -model 指定或设 ALASOL_MODEL；
不知道叫什么就先跑 `+"`alasol models`"+`。
各子命令的 flags 用 `+"`alasol <子命令> -h`"+` 查看。
`)
}
