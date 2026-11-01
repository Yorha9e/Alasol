package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/registry"
	mcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

// serverName / serverVersion 是本 MCP server 自报的身份（initialize 握手时给 client）。
// 名字稳定（client 可能按它认端点）；版本跟着 harness 走。
const (
	serverName    = "alasol"
	serverVersion = "v4-5"
)

// Options 是 mcpserver.Server 的可选配置。
type Options struct {
	// Logger 收 server 的生命周期/会话日志。**必须写到 stderr**——stdio 传输下
	// os.Stdout 只走 MCP 帧（与 acp 同纪律：日志混进 stdout 会污染协议流）。
	//
	// nil = **默认写 stderr**（F4）：暴露面被拒（非法工具名 / mcp__ 内部命名空间）这类事
	// 必须看得见——默认静默 = 告警被吞，用户以为"配了就暴露了"，实际没有。
	Logger *slog.Logger
	// Instructions 是握手时下发给 client 的使用说明（可选；MCP 的 server instructions）。
	Instructions string
}

// Registry 是本包消费的**注册表只读视图**（方法集刻意小：List + Get）。
//
// 为什么是接口而不是 *registry.Registry：本包只读——不注册、不启用、不停用扩展。
// 交出可写的注册表 = 把写面递给调用方（"约定只读"靠不住，接口才靠得住）。
// 方法集小还有一层好处：注册表将来加写方法时，不会顺着这个接口漏出去。
type Registry interface {
	// List 按注册顺序返回条目快照；kind 为空 = 全部（core/registry 的同款口径）。
	List(kind string) []registry.Entry
	// Get 按名取条目快照（第二个返回值为 false = 没这条）。
	Get(name string) (registry.Entry, bool)
}

// Server 把 harness 的工具注册表暴露成一个 MCP server（B 类边界）。
//
// 生命周期：New（快照注册表、按白名单挂工具）→ Run / RunStdio（起传输，阻塞到对端收场
// 或 ctx 取消）。并发：底层 go-sdk Server 自身并发安全；本类型的只读字段（reg / expose /
// exposed / logger）构造后不变，无需额外加锁。
type Server struct {
	sdk     *mcp.Server
	reg     Registry
	expose  map[string]bool // 白名单（nil/空 = 零暴露，fail-close）
	exposed []string        // 实际暴露的工具名（构造时定，排序；诊断/测试观测用）
	logger  *slog.Logger    // 告警出口（默认 stderr；暴露面被拒必须看得见，F4）
}

// New 造 MCP server：读注册表里 kind=tool 的条目，按白名单把命中的工具挂到 go-sdk server 上。
//
// 白名单语义（fail-close）：exposeTools **空 = 一个都不暴露**（tools/list 空集、任何
// tools/call 命中 unknown tool）。只有名字进白名单、注册表里真有对应工具条目、且条目带
// 工具描述 + core.ToolExecutor 实现的才被暴露；缺描述/实现的跳过（不 panic——坏条目不该
// 让 server 起不来，E5 同款失败隔离精神）。
//
// 另外两条**拒暴露**的口径（都是 fail-close，宁可少暴露不可假暴露）：
//   - **注册表 State 参与暴露判定**（F1）：条目被停用（State=disabled）→ 不暴露；
//   - **mcp__ 内部命名空间不上公网面**（F2）：白名单点名 `mcp__*` 也拒（见 mcpInternalPrefix）。
//
// reg 为 nil → 报错（无处读工具 = 装配错误，fail-close）。
func New(reg Registry, exposeTools []string, opts *Options) (*Server, error) {
	if reg == nil {
		return nil, errors.New("mcpserver: registry 为 nil（拒绝起 server：无处读工具）")
	}
	var o Options
	if opts != nil {
		o = *opts
	}
	logger := o.Logger
	if logger == nil {
		// 默认写 stderr（F4）：拒绝暴露的告警（非法工具名 / mcp__ 内部名）必须看得见——
		// 默认静默 = 告警被吞，使用者以为"配了就暴露了"。stdout 仍只走 MCP 帧。
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	expose := make(map[string]bool, len(exposeTools))
	for _, raw := range exposeTools {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if strings.HasPrefix(name, mcpInternalPrefix) {
			// F2：`mcp__` 开头是**内部命名空间**（runtime/mcp client 侧给外部 server 的工具
			// 起的名字）。它不上公网面：白名单点名也拒（fail-close，与空白名单同精神）——
			// 外部 MCP 工具已经由 client 侧聚合过一次，再从 server 面放出去等于二次暴露。
			logger.Warn("mcpserver: 拒绝暴露 mcp__ 内部命名空间的工具名（白名单点名也拒）",
				"tool", name, "reason", "mcp__ 前缀 = 内部命名空间，不上公网面")
			continue
		}
		expose[name] = true
	}

	s := &Server{reg: reg, expose: expose, logger: logger}
	s.sdk = mcp.NewServer(&mcp.Implementation{Name: serverName, Version: serverVersion}, &mcp.ServerOptions{
		Logger:       logger,
		Instructions: o.Instructions,
	})
	s.registerExposedTools()
	return s, nil
}

// registerExposedTools 快照注册表并把白名单命中的工具挂上去（构造时一次）。
//
// 快照口径（与 runtime/mcp client 的"工具列表 = 连接时刻快照"一致）：构造这一刻注册表里
// 有哪些工具、哪些在白名单里，就此定死；运行期再注册/注销工具不反映到已起的 server 面
// （要刷新得重起 server——重启 hub / 重跑 mcp-serve）。理由同 client 侧：动态刷新要处理
// "工具突然消失但 client 正在调"等一连串状态一致性，远超"接个回调"的成本，且 harness 的
// 工具面本就是装配期定的。
func (s *Server) registerExposedTools() {
	entries := s.reg.List(registry.ExtensionKindTool)
	// 按注册名排序：AddTool 顺序确定（list 输出与 change 通知顺序稳定，测试可断言）。
	sort.Slice(entries, func(i, j int) bool { return entries[i].Meta.Name < entries[j].Meta.Name })
	for _, e := range entries {
		if !s.expose[e.Meta.Name] {
			continue // 不在白名单 → 不暴露（fail-close：只暴露显式声明的）
		}
		if e.State == registry.StateDisabled {
			// F1：**注册表 State 参与暴露判定**。停用（Disable / 能力裁决失败后的
			// revokeEnabled）的条目不暴露——tools/list 里不该出现一个调了会被拒的工具。
			s.logger.Warn("mcpserver: 工具已停用（registry State=disabled）→ 不暴露",
				"tool", e.Meta.Name)
			continue
		}
		if e.Tool == nil {
			continue // 注册过但没有工具描述（kind 非 tool 的条目）→ 跳过
		}
		exec, ok := e.Impl.(core.ToolExecutor)
		if !ok || exec == nil {
			continue // 实现不是 ToolExecutor → 跳过（防御：RegisterTool 本会拦住）
		}
		if !s.addTool(*e.Tool, exec) {
			continue // 工具名不合法 → 拒绝挂载（F4：宁可少暴露，不可假暴露）
		}
		s.exposed = append(s.exposed, e.Meta.Name)
	}
}

// addTool 把一个工具挂到 go-sdk server（低层 AddTool：自己管参数原样过桥与错误回 MCP error）。
//
// 返回是否真的挂上：**工具名不合法 → 不挂 + Warn**（F4）。go-sdk 的 AddTool 对非法名只
// 记一条日志、**照样把工具挂上去**（协议面会出现一个别的 MCP client 认不出的名字 = 假暴露：
// 列表里看得见、调不动）。故本地先校、非法即拒（fail-close）。
func (s *Server) addTool(tool contracts.Tool, exec core.ToolExecutor) bool {
	name := tool.Name
	if err := validMCPToolName(name); err != nil {
		s.logger.Warn("mcpserver: 工具名不合法 → 不暴露（go-sdk 只记日志、照样挂上去 = 假暴露）",
			"tool", name, "err", err.Error())
		return false
	}
	desc := ""
	if tool.Description != nil {
		desc = *tool.Description
	}
	s.sdk.AddTool(&mcp.Tool{
		Name:        name,
		Description: desc,
		InputSchema: objectInputSchema(tool.Schema),
	}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return s.callTool(ctx, name, exec, req)
	})
	return true
}

// callTool 执行一次 tools/call：per-call recover（E5）+ 直接调执行器 + 结果折回 MCP。
//
// 返回语义（对上层 handler）：
//   - 执行器 panic → recover 捕住，返回 Go error → go-sdk 转成 **MCP/JSON-RPC 错误响应**
//     （client 的 CallTool 拿到 error）；server 会话不中断、hub 不崩；
//   - 执行器返回 error → 同样转成 MCP 错误响应（不 panic、不中断）；
//   - 成功 → 折成 *mcp.CallToolResult（text 块原样，data 块按 JSON 文本带上）。
func (s *Server) callTool(ctx context.Context, name string, exec core.ToolExecutor, req *mcp.CallToolRequest) (res *mcp.CallToolResult, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			// E5：工具执行 panic 不得带崩 server —— 捕住、转成 MCP 错误回给 client。
			res = nil
			err = fmt.Errorf("mcpserver: 工具 %q 执行 panic: %v", name, rec)
		}
	}()

	// **执行前再查一次注册表 State**（F1）：tools/list 是构造时的快照，运行期被停用的工具
	// 必须**立刻**调不动（否则"停用"只是个列表口径）。查不到条目（已注销）同样拒绝执行。
	if err := s.checkUsable(name); err != nil {
		return nil, err
	}

	args := req.Params.Arguments
	if len(args) == 0 {
		args = json.RawMessage("{}") // 空参合法（执行器侧按"无参"处理）
	}
	intent := contracts.ToolCallIntentPayload{
		// MCP 面不做 X1 事件 sourced（不走 loop），ToolCallID 只求唯一、供执行器/日志对账；
		// 它**永不进 contracts.Event.ID**（那条路归 loop，本包不写事件）。
		ToolCallID: contracts.ProviderId(core.NewUlid()),
		ToolName:   name,
		Arguments:  args, // 原样过桥：不解析、不改写、不补全（S4 纪律同款）
	}
	result, execErr := exec.Execute(ctx, intent)
	if execErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("mcpserver: 工具 %q 执行被中止: %w", name, ctxErr)
		}
		return nil, fmt.Errorf("mcpserver: 工具 %q 执行失败: %w", name, execErr)
	}
	return toolResultToMCP(result), nil
}

// ExposedTools 返回实际暴露的工具名（构造时定，排序；诊断 / 测试 / 启动日志用）。
func (s *Server) ExposedTools() []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s.exposed...)
}

// SDKServer 返回底层 go-sdk server（装配 / 测试观测用：如挂 resources、或换传输）。
func (s *Server) SDKServer() *mcp.Server {
	if s == nil {
		return nil
	}
	return s.sdk
}

// Run 在给定传输上起 server，阻塞到对端收场或 ctx 取消（ctx 取消时关连接并返回 ctx.Err）。
//
// 传输由调用方给：stdio 用 RunStdio；测试用 in-memory（NewInMemoryTransports）；
// HTTP（可选）用 go-sdk 的 StreamableHTTPHandler 包 SDKServer() 再 Serve。
func (s *Server) Run(ctx context.Context, t mcp.Transport) error {
	if s == nil || s.sdk == nil {
		return errors.New("mcpserver: server 未构造")
	}
	return s.sdk.Run(ctx, t)
}

// RunStdio 在 stdio 传输上起 server（`alasol mcp-serve` 的首选形态）。
//
// StdioTransport 用 os.Stdin / os.Stdout：**stdout 只走 MCP 帧**，日志必须走 stderr
// （见 Options.Logger 的纪律）。阻塞到 stdin EOF（client 收场）或 ctx 取消。
func (s *Server) RunStdio(ctx context.Context) error {
	return s.Run(ctx, &mcp.StdioTransport{})
}

// toolResultToMCP 把 contracts.ToolResult 折成 mcp.CallToolResult。
//
// 映射（与 runtime/mcp client 的 contentPart 反向）：
//   - text 块（Text 非 nil）→ TextContent{Text}（原样）；
//   - data 块（Data 非空）→ TextContent{Text: 原始 JSON 字符串}（契约的 ContentPart.Type
//     是开放字符串，server 面统一按文本带上，client 自行按需解析）。
func toolResultToMCP(res contracts.ToolResult) *mcp.CallToolResult {
	out := &mcp.CallToolResult{}
	for _, p := range res.Content {
		switch {
		case p.Text != nil:
			out.Content = append(out.Content, &mcp.TextContent{Text: *p.Text})
		case len(p.Data) > 0:
			out.Content = append(out.Content, &mcp.TextContent{Text: string(p.Data)})
		}
	}
	return out
}

// objectInputSchema 保证交给 go-sdk AddTool 的 input schema 是**合法的 object schema**。
//
// go-sdk 的低层 Server.AddTool 要求 InputSchema 非 nil 且 type=="object"，否则**panic**。
// 工具描述里的 Schema 理论上是对象 schema（内置 read/write/bash 都是），但外部/坏条目不保证；
// 这里先校验：合法对象 schema 原样用（保留 properties/required 等），否则兜底
// {"type":"object"}（一个"收任意对象"的宽松 schema）——**不因一个坏 schema 让 server 起不来**，
// 也不静默丢工具（E5 失败隔离精神：坏输入降级，不崩溃）。
func objectInputSchema(raw json.RawMessage) any {
	if len(raw) > 0 {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err == nil {
			if typ, _ := m["type"].(string); typ == "object" {
				return raw // 合法对象 schema：原样交（json.RawMessage 经 remarshal 还原）
			}
		}
	}
	return map[string]any{"type": "object"}
}

// checkUsable 执行前的**注册表 State 复查**（F1）：条目被停用（State=disabled）或已注销
// → 拒绝执行（tools/call 报 unknown/disabled 语义的错误）。
//
// 为什么执行前还要查一次：tools/list 是**构造时的快照**（注册表运行期变化不反映到已起的
// server 面）。若只在构造时判一次 State，"list 之后才被停用"的工具仍能调——停用就成了
// 假承诺。执行前复查把"停用"落成真闸（fail-close）。
func (s *Server) checkUsable(name string) error {
	if s == nil || s.reg == nil {
		return nil
	}
	e, ok := s.reg.Get(name)
	if !ok {
		return fmt.Errorf("mcpserver: 工具 %q 已不在注册表（已注销）→ 拒绝执行", name)
	}
	if e.State == registry.StateDisabled {
		return fmt.Errorf("mcpserver: 工具 %q 已停用（registry State=disabled）→ 拒绝执行", name)
	}
	return nil
}

// mcpInternalPrefix 是 **MCP client 侧内部命名空间**的前缀（runtime/mcp 用
// `mcp__<server>__<tool>` 给外部 server 的工具命名）。
//
// 它**不上公网面**（F2）：外部 MCP 工具已经由 client 侧聚合过一次，再从这个 server 面
// 放出去 = 二次暴露（且名字带外部 server 名，别的 MCP client 认不出）。故白名单点名
// `mcp__*` 也拒（fail-close，与空白名单同精神）。
const mcpInternalPrefix = "mcp__"

// mcpToolNameMaxLen 是 MCP 工具名长度上限（与 go-sdk 的 validateToolName 同口径：128 字节）。
const mcpToolNameMaxLen = 128

// validMCPToolName 校验工具名是否是**合法的 MCP 工具名**（字符集 + 长度，F4）。
//
// 口径与 go-sdk 的 validateToolName 一致（不 import 它的未导出函数）：非空、≤128 字节、
// 字符集限 [A-Za-z0-9_.-]。非法名 = 别的 MCP client 认不出的名字 = 假暴露，故不挂。
func validMCPToolName(name string) error {
	if name == "" {
		return errors.New("工具名为空")
	}
	if len(name) > mcpToolNameMaxLen {
		return fmt.Errorf("工具名超长（%d 字节 > 上限 %d）", len(name), mcpToolNameMaxLen)
	}
	for _, r := range name {
		if !validMCPToolNameRune(r) {
			return fmt.Errorf("工具名含非法字符 %q（只允许 A-Z a-z 0-9 _ . -）", string(r))
		}
	}
	return nil
}

// validMCPToolNameRune 判定单个字符是否在 MCP 工具名的合法字符集里。
func validMCPToolNameRune(r rune) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9') ||
		r == '_' || r == '-' || r == '.'
}
