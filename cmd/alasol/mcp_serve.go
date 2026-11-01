package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/mcpserver"
)

// mcpServeMain 是 `alasol mcp-serve`：把 harness 暴露成一个 **MCP server**（B 类边界，
// v4-5）。与 serve/acp 同款装配 host.Hub（复用 config 装载面），但不起 HTTP/WS——端点形态
// 是 **stdio** MCP server。典型用法：
//
//	alasol mcp-serve [-model ...] [-db alasol.db]
//	  # 配置文件里声明暴露白名单（fail-close）：
//	  #   [mcp_server]
//	  #   expose_tools = ["read"]
//
// 客户端以本进程为子进程拉起，用现成 MCP client 经 stdio 接入（不自研分布式协议，K19 同款）。
//
// # 鉴权口径：stdio = 谁拉起谁负责（同 ACP 的 ssh 口径，N5）
//
// 拉起本进程的一方就是被授权的 client（`ssh host alasol mcp-serve` 则鉴权交 SSH）。本命令
// **不自研任何鉴权**。暴露面由配置白名单（Config.MCPExposeTools，[mcp_server] expose_tools）
// 收到最小：**空 = 一个工具都不暴露**（fail-close），防 harness 写面（write/bash）被任意
// 能连上 stdio 的 MCP client 摸到。要开放某个工具必须在白名单里点名。
//
// # stdout 纯净性（与 acp 同纪律）
//
// stdio MCP server 占住 os.Stdout 只写 MCP 帧；本进程的一切日志（hub 装配 + server 生命周期）
// 都走 **stderr**。起点一行都不往 stdout 打。
//
// # 不跑 agent loop
//
// mcp-serve 只把工具注册表暴露成 MCP tools（tools/list + tools/call 直接调注册表执行器），
// **不建 session、不跑 loop**——所以 -model / -approval 等 loop 参数虽仍参与装配（复用同一
// config 面），但在本命令下不被使用（model 只为满足 config 装配的必填项；approval 无从触发）。
//
// # 退出码
//
// 0 = 正常收场（stdin EOF / Ctrl-C 取消——取消**不是错误**）；1 = 装配/端点错误；2 = 配置被拒。
func mcpServeMain(args []string) int {
	fs := flag.NewFlagSet("mcp-serve", flag.ExitOnError)
	f := registerHubFlags(fs) // 复用 serve/chat/acp 的共有 flag 面（model/base-url/api-key/allow/db/config/…）
	f.parse(fs, args)

	cfg, err := f.config()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	// 档位白名单（fail-close）：配置被拒 = exit 2（与 serve/acp 同口径）。
	if rejectInvalidReasoningEffort(cfg) {
		return 2
	}

	h, err := host.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "装配失败:", err)
		return 1
	}
	defer h.Close()

	// 日志恒走 stderr（stdout 只走 MCP 帧——见函数注释的"stdout 纯净性"）。
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	srv, err := mcpserver.New(h.Registry(), cfg.MCPExposeTools, &mcpserver.Options{
		Logger:       logger,
		Instructions: "alasol harness 工具面（经 MCP 暴露）。暴露的工具由服务端白名单决定；调用副作用由各工具自身决定。",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "MCP server 装配失败:", err)
		return 1
	}

	exposed := srv.ExposedTools()
	logger.Info("alasol mcp-serve: stdio MCP server 已就绪（stdout 只走 MCP 帧，日志走 stderr）",
		"exposed_tools", exposed)
	if len(exposed) == 0 {
		// fail-close 提示：白名单空 = 零暴露。说清楚，别让使用者以为"起来了就能调"。
		logger.Warn("alasol mcp-serve: 暴露白名单为空 → 零工具暴露（fail-close）；" +
			"要开放工具请在配置文件 [mcp_server] expose_tools 里点名（如 expose_tools = [\"read\"]）")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := srv.RunStdio(ctx); err != nil {
		if ctx.Err() != nil {
			// 取消 = 正常收场（Ctrl-C / SIGTERM；F11：取消不是错误）。
			logger.Info("alasol mcp-serve: 收场（取消）")
			return 0
		}
		fmt.Fprintln(os.Stderr, "MCP server 错误:", err)
		return 1
	}
	// 正常收场：stdin EOF（client 关闭了连接）。
	logger.Info("alasol mcp-serve: 收场（stdin EOF）")
	return 0
}
