package host

import (
	"context"
	"fmt"

	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/runtime/mcp"
)

// 本文件把 v3-S1 的 MCP client 能力（N1：client 先）装配进 hub。
//
// # 形态：opt-in，默认关 = 零 MCP 开销
//
// Config.MCP 零值（没有 [[mcp.servers]]）→ 不建 Manager、不连任何外部进程、
// 不注册任何工具，与 v2 行为**完全一致**（酸测试不回归）。这与 Config.Graph /
// Config.Sandbox 同一形态：一个 opt-in 配置面，默认关。
//
// # 不给 MCP 工具任何特权（H5 fail-close + R5 不暴露即不存在）
//
// MCP 工具只经 registry.RegisterTool 进注册表——**与 read/write/bash 同一个入口**。
// 于是：
//
//   - 策略：hub 的策略规则来自 -allow/-ask（source=cli）。没列 MCP 工具名 →
//     policy.Engine.Evaluate 无 allow 命中 → deny（H5）。**MCP 工具默认不可用**，
//     要显式 `allow = ["mcp__<server>__<tool>"]` 或 `allow = ["*"]` 才放行；
//   - 可见性：loop.visibleTools 按 agent 类型的 tools[] 白名单过滤（R5），
//     deny 的工具移出可见集（H4）。MCP 工具不在这两个名单里 → 模型看不见；
//   - 能力裁决：RegisterTool 派生出的 ExtensionMeta.Capabilities 为空 →
//     Enable 无需裁决。本包**不**给 MCP 工具申报任何能力（不碰 E3）。
//
// 保守侧还有一层在工具描述本身：MCP 工具一律 side_effect=write / approvable=true /
// idempotent=false / replay=unsafe（见 runtime/mcp/discovery.go 的理由）。
//
// # 坏 server 不拖垮 hub
//
// 连接失败 / 发现失败 / 单工具撞名 → 那个 server 的工具不注册，错误 surfaced
// 到日志与 MCPErrors()，hub 照常起来。理由：外部 MCP server 是**第三方进程**，
// 它不可用不该让 alasol 起不来（否则一个坏配置的 server 就是 DoS）。
// 但**配置错误**（server 名重复、transport 缺 command/url）是致命的——
// 那是使用者要修的，见 mcp.New。

// assembleMCP 连接 Config.MCP 声明的 server 并把工具注册进 reg。
//
// 返回装配出的 Manager（无启用 server 时为 nil）与**致命**错误。
// 非致命问题（连接失败 / 发现失败 / 单工具撞名）在本函数内 surfaced 到日志，
// 并留在 Manager.Errors() 里供宿主查询——不向上抛（外部进程坏了不该让 hub 起不来）。
//
// 致命错误只有一种：**配置错误**（mcp.New 拒：server 名重复、transport 缺
// command/url、传输类型不支持）。那是使用者写错了，必须让 hub.New 失败。
func assembleMCP(ctx context.Context, cfg Config, reg *registry.Registry) (*mcp.Manager, error) {
	if len(cfg.MCP.Servers) == 0 {
		return nil, nil // 零 server = 不装 MCP（默认形态）
	}
	mgr, err := mcp.New(cfg.MCP)
	if err != nil {
		return nil, fmt.Errorf("host: MCP 配置: %w", err)
	}
	for _, e := range mgr.Attach(ctx, reg) {
		// 错误文案只含 server 名 / 工具名 / 传输名（runtime/mcp 保证不含凭据：
		// Env/Headers 的值与端点 URL 都被挡在外面），可以安全落日志（H12/H19）。
		hubLog.Warn("host: MCP server 装配有问题（该 server 的工具未全部注册）", "err", e.Error())
	}
	if names := mgr.ToolNames(); len(names) > 0 {
		hubLog.Info("host: MCP 工具已注册",
			"servers", len(mgr.Servers()), "tools", len(names), "names", names)
	}
	return mgr, nil
}

// MCPErrors 返回 MCP 装配期的非致命错误（诊断面；nil = 未装配 MCP）。
//
// 口径与 graphEngine 一致：宿主内部观测用，不进事件流（MCP 装配细节不是
// agent 语义，进事件流只会污染 seq——陷阱 11 同款顾虑）。
func (h *Hub) MCPErrors() []error {
	if h == nil || h.mcpManager == nil {
		return nil
	}
	return h.mcpManager.Errors()
}

// MCPServers 返回已装配 MCP server 的状态（诊断面；不含凭据/配置值）。
func (h *Hub) MCPServers() []mcp.ServerStatus {
	if h == nil || h.mcpManager == nil {
		return nil
	}
	return h.mcpManager.Servers()
}

// MCPToolNames 返回已注册的 MCP 工具名（排序；诊断/测试观测用）。
func (h *Hub) MCPToolNames() []string {
	if h == nil || h.mcpManager == nil {
		return nil
	}
	return h.mcpManager.ToolNames()
}

// MCPEnabled 报告 hub 是否**真有**可用的 MCP server（serve 的 banner 用）。
//
// 口径：manager 存在**且**至少成功装配了一个 server。只配了 server 但全连不上时
// 返回 false——那时 banner 打"已装配 N 个外部工具"会是假话（N=0）。
func (h *Hub) MCPEnabled() bool {
	if h == nil || h.mcpManager == nil {
		return false
	}
	return len(h.mcpManager.Servers()) > 0
}
