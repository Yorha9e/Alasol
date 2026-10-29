// Package mcp 是 MCP（Model Context Protocol）**client** 侧能力组件（v3-S1，N1 拍板
// "client 先"）：消费外部 MCP server 声明的工具，把它们接进 alasol 既有的工具管线。
//
// # 它为什么存在 / 什么变化会导致它变（09 包边界）
//
//	存在理由：把"外部 MCP server 提供的工具"变成 core 眼里一个普通的
//	contracts.Tool + core.ToolExecutor——除此之外它什么都不是。
//	会变：换 MCP SDK 版本、加一种传输（stdio/HTTP/SSE）、改工具命名隔离方案。
//	不会变：策略链、可见性（R5）、X1 三段写路径、事件契约——本包一行都不该碰它们。
//
// 依赖方向：runtime（真实世界实现）→ contracts + core 的接口 + 第三方 SDK。
// 按 09 的腐化防线，**core 里不得出现任何 MCP SDK 类型**——所以适配全部收在本包。
//
// # MCP 工具从外面看就是普通工具（X1 红线）
//
// 一次 MCP 工具调用与一次内置 read/bash 调用走**同一条**路径，一字不改：
//
//	模型调用 → loop.resolveTool 命中注册表条目
//	        → policy.Decide 裁决（deny-all 默认，fail-close，H5）
//	        → tool.call.intent（durable，provider 的 tool_call_id 原样）
//	        → 本包的 executor 调 MCP tools/call
//	        → tool.call.completed / tool.call.failed
//
// 因此：core/loop 零改动；MCP 的 request id **只在适配层内部**用（SDK 自己管），
// 永不进 contracts.Event.ID（陷阱 1）；executor 只返回 contracts.ToolResult 或 error，
// 不写事件（X1：三段写路径归 loop）。
//
// # 命名隔离：mcp__<server>__<tool>（陷阱 17：全程不折叠大小写）
//
// 外部 server 声明的工具名是**外部可控输入**，与内置 read/write/bash 或另一个
// server 的同名工具撞名是必然事件。方案：注册名一律加 `mcp__<server>__` 前缀，
// server 名在配置层保证唯一 → 撞名在结构上不可能（除非归一化把两个不同的工具名
// 折成同一个，那种情况由注册表的重名拒绝兜住 → fail-close 并说明）。
// 理由写在 naming.go；wire 上的工具名保持 server 原样（前缀只作用于注册面）。
//
// # N2：sampling / elicitation / roots 明确不实现
//
// client 不广告这三个能力（Capabilities 显式给空），server 若照样发请求，
// 由本包的接收中间件**明确拒绝并留痕**（n2.go）。理由见 07/N2。
//
// # server 进程生命周期：谁拉起谁收尸（陷阱 5）
//
// stdio 传输由本包亲自 exec（不走 SDK 的 CommandTransport）：windowsHide 起进程、
// IOTransport 只拿管道、Close 时先优雅关 stdin 等退出、超时再 taskkill /T /F 杀树、
// 最后 cmd.Wait() 收尸——SDK 的 pipeRWC.Close 在 Windows 上只会 SIGTERM/Kill
// 直接子进程，孙子进程会继承管道写端把 Wait 挂住（本机实测量级的坑，
// 见 runtime/cap/local 的 waitDelay 注释）。
package mcp
