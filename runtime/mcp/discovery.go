package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// 本文件把 server 声明的工具（MCP tools/list 的 *mcp.Tool）转换成
// **contracts.Tool**（跨边界契约）——这是本包与 core 之间唯一的接触面。
//
// # Schema 映射口径（"元嵌套"的既有口径）
//
// contracts.Tool.Schema 是 json.RawMessage，值本身是 JSON Schema（契约注释：
// "元嵌套：值本身是 JSON Schema → codegen 当原始 JSON 处理"）。MCP 的
// Tool.InputSchema 是 `any`（客户端侧默认解成 map[string]any）——
// 于是**原样再编码**回 JSON 字节即可，不做任何解释/改写/补默认值：
// server 说参数长什么样就长什么样，我们不是 schema 的权威。
//
// server 没给 inputSchema（协议上必填，但坏 server 可能漏）→ 兜底
// `{"type":"object"}`（空对象 schema 是"任意参数"的合法表达，比拒注册可用）。
//
// # 元数据映射：外部世界没有的内置语义，一律取保守值
//
//   - side_effect：**write**（未知外部工具按"能写"对待——MCP 工具可能碰网络/文件/DB，
//     按 read 对待会让权限收窄失效，见陷阱 21 的同类教训）；
//   - approvable：**true**（外部工具默认要人审，与 deny-all 默认同向：不给特权）；
//   - idempotent / replay：**false / unsafe**（M7/X2：不声明幂等就不自动重试，
//     崩溃续跑不盲目重放——外部 server 的重试语义我们无从验证）；
//   - cancelable：true（tools/call 走 ctx，取消能传到 server）。
//
// 这四条是**保守缺省**，不是从 server 的 annotations 猜出来的：ToolAnnotations
// 的 readOnlyHint/idempotentHint 是 server 的自述，而 MCP 明确说它是 hint 不是承诺。
// 一个自述 readOnly 的 server 工具被按 read 对待 → 权限收窄对它失效。所以不读 hint。
//
// # 命名：wire 名 → 注册名
//
// server 原样工具名只用于 wire（tools/call 的 name）；注册名过
// RegisterableName（归一化 + 加前缀），失败即拒注册（fail-close）。
type discoveredTool struct {
	// wireName 是发给 server 的工具名（原样）。
	wireName string
	// name 是注册名（mcp__<server>__<tool>，已归一化）。
	name string
	// desc 是跨边界描述（进 registry / 进模型可见集）。
	desc contracts.Tool
}

// discover 列出 server 的工具并转换成 contracts.Tool。
//
// 单个工具转换失败**不中断**整个发现：坏工具名只是那一个工具不可用
// （errors 里带回原名），其余工具照常注册。这符合"坏 server 不拖垮 hub"——
// 一个声明了怪名字的 server 不该让它其余十个工具一起消失。
func (c *serverConn) discover(ctx context.Context) ([]discoveredTool, []error) {
	if c == nil {
		return nil, []error{fmt.Errorf("mcp: 连接为 nil（server 未连接）")}
	}
	raw, err := c.Tools(ctx)
	if err != nil {
		return nil, []error{err}
	}
	out := make([]discoveredTool, 0, len(raw))
	var errs []error
	for _, t := range raw {
		d, err := c.convert(t)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		out = append(out, d)
	}
	return out, errs
}

// convert 把一个 *mcp.Tool 转成 discoveredTool（注册名 + contracts.Tool）。
func (c *serverConn) convert(t *mcp.Tool) (discoveredTool, error) {
	if t == nil {
		return discoveredTool{}, fmt.Errorf("mcp: server %q 声明了 nil 工具（跳过）", c.cfg.Name)
	}
	wire := t.Name
	name, err := RegisterableName(c.cfg.Name, wire)
	if err != nil {
		return discoveredTool{}, fmt.Errorf("mcp: server %q 的工具不可注册（跳过该工具）: %w", c.cfg.Name, err)
	}
	desc := c.describe(t)
	desc.Name = name // 注册名（唯一写入处，防二源）
	return discoveredTool{wireName: wire, name: name, desc: desc}, nil
}

// describe 造 contracts.Tool 描述（Schema 原样映射，元数据取保守缺省）。
// Name 由调用方填（convert），这里不设。
func (c *serverConn) describe(t *mcp.Tool) contracts.Tool {
	text := t.Description
	if text == "" {
		// server 没给说明时补一句可追溯的来源（模型看得见"这是外部 server 的工具"，
		// 也便于人排查"这个工具哪来的"）。不含任何配置/凭据。
		text = fmt.Sprintf("MCP server %q 提供的工具（未提供描述）。", c.cfg.Name)
	}
	return contracts.Tool{
		Description: strPtr(text),
		Schema:      inputSchemaJSON(t),
		// 保守缺省（理由见文件头）：外部工具一律按"能写、要人审、不幂等、不可安全重放"。
		SideEffect: contracts.SideEffectWrite,
		Cancelable: true,
		Approvable: true,
		Idempotent: false,
		Replay:     contracts.ReplayKindUnsafe,
	}
}

// inputSchemaJSON 把 MCP 的 InputSchema（any）原样编码成 JSON 字节。
//
// 原样映射：不解释、不改写、不补默认值。server 是 schema 的权威，
// 我们只是搬运工——改写 schema 会让模型按我们的假设填参数，而假设来自外部输入。
func inputSchemaJSON(t *mcp.Tool) json.RawMessage {
	if t == nil || t.InputSchema == nil {
		return json.RawMessage(`{"type":"object"}`) // 兜底：任意参数
	}
	raw, err := json.Marshal(t.InputSchema)
	if err != nil {
		// 编码不了（理论上是 server 给了非 JSON 可表示的东西）→ 兜底空对象 schema。
		// 不返回 error：schema 坏了不等于工具不可用，模型仍可按描述调用。
		return json.RawMessage(`{"type":"object"}`)
	}
	if len(raw) == 0 || string(raw) == "null" {
		return json.RawMessage(`{"type":"object"}`)
	}
	// 必须是 JSON **对象**（契约：schema 是 JSON Schema 元嵌套）。不是对象 →
	// 兜底（否则跨边界载荷违反契约，codegen/消费者会拒）。
	if !isJSONObject(raw) {
		return json.RawMessage(`{"type":"object"}`)
	}
	return raw
}

// isJSONObject 判定原始 JSON 的顶层是否是对象。
func isJSONObject(raw []byte) bool {
	var probe any
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false
	}
	_, ok := probe.(map[string]any)
	return ok
}

func strPtr(s string) *string { return &s }
