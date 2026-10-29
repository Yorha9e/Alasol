package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// errBoom 是测试里模拟"server 内部错误"的哨兵。
var errBoom = errors.New("fake: server 内部炸了")

// TestDiscoverConvertsToContractTool：发现 → contracts.Tool 的映射口径。
//
// 断言四件事：
//  1. 注册名带 mcp__<server>__ 前缀，wire 名保持原样（命名隔离不污染 wire）；
//  2. Schema **原样映射**（元嵌套口径：值本身就是 JSON Schema）；
//  3. 元数据取**保守缺省**（外部工具不给特权：write / approvable / unsafe）；
//  4. 描述原样带回。
func TestDiscoverConvertsToContractTool(t *testing.T) {
	ctx := context.Background()
	tr, _, _ := startFake(t, fakeTool{
		name:        "create.issue",
		description: "建一个 issue",
		schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"title": map[string]any{"type": "string"},
			},
			"required": []any{"title"},
		},
		text: "created",
	})
	conn := connectFake(ctx, t, "gh", tr)

	got, errs := conn.discover(ctx)
	if len(errs) != 0 {
		t.Fatalf("发现报错: %v", errs)
	}
	if len(got) != 1 {
		t.Fatalf("发现 %d 个工具，期望 1", len(got))
	}
	d := got[0]
	if d.wireName != "create.issue" {
		t.Fatalf("wire 名 = %q（应保持 server 原样）", d.wireName)
	}
	if d.name != "mcp__gh__create_issue" {
		t.Fatalf("注册名 = %q，期望 mcp__gh__create_issue（`.` 归一化成 `_`）", d.name)
	}
	if d.desc.Name != d.name {
		t.Fatalf("desc.Name = %q，应等于注册名 %q", d.desc.Name, d.name)
	}
	if d.desc.Description == nil || *d.desc.Description != "建一个 issue" {
		t.Fatalf("描述未原样带回：%v", d.desc.Description)
	}
	// Schema 原样映射：结构与 server 声明的一致。
	var sch map[string]any
	if err := json.Unmarshal(d.desc.Schema, &sch); err != nil {
		t.Fatalf("Schema 不是合法 JSON: %v", err)
	}
	if sch["type"] != "object" {
		t.Fatalf("Schema.type = %v，期望 object", sch["type"])
	}
	props, ok := sch["properties"].(map[string]any)
	if !ok || props["title"] == nil {
		t.Fatalf("Schema.properties.title 丢了（应原样映射）: %s", d.desc.Schema)
	}
	// 保守缺省（外部工具不给特权）。
	if d.desc.SideEffect != contracts.SideEffectWrite {
		t.Fatalf("SideEffect = %q，期望 write（未知外部工具按能写对待）", d.desc.SideEffect)
	}
	if !d.desc.Approvable {
		t.Fatalf("Approvable = false，期望 true（外部工具默认要人审）")
	}
	if d.desc.Idempotent {
		t.Fatalf("Idempotent = true，期望 false（M7：不声明幂等就不自动重试）")
	}
	if d.desc.Replay != contracts.ReplayKindUnsafe {
		t.Fatalf("Replay = %q，期望 unsafe（X2：未知结果不盲目重放）", d.desc.Replay)
	}
	if !d.desc.Cancelable {
		t.Fatalf("Cancelable = false，期望 true（tools/call 走 ctx）")
	}
	if d.desc.RequiredEnv != nil {
		t.Fatalf("RequiredEnv 应为空（MCP server 的凭据在装配期给子进程，不在调用期注入）")
	}
}

// TestDiscoverEmptyDescription：server 没给描述时补一句可追溯来源（含 server 名）。
func TestDiscoverEmptyDescription(t *testing.T) {
	ctx := context.Background()
	tr, _, _ := startFake(t, fakeTool{name: "t", text: "ok"})
	conn := connectFake(ctx, t, "srv", tr)
	got, errs := conn.discover(ctx)
	if len(errs) != 0 || len(got) != 1 {
		t.Fatalf("发现结果异常：%d 个工具 / %v", len(got), errs)
	}
	if got[0].desc.Description == nil {
		t.Fatalf("描述为 nil")
	}
	if want := "srv"; !contains(*got[0].desc.Description, want) {
		t.Fatalf("兜底描述不含 server 名 %q：%q", want, *got[0].desc.Description)
	}
}

// TestDiscoverBadToolName：一个工具名不可注册 → **只跳过那一个**，其余照常
// （"坏 server 不拖垮 hub"：一个怪名字不该让它其余工具一起消失）。
func TestDiscoverBadToolName(t *testing.T) {
	ctx := context.Background()
	tr, _, _ := startFake(t,
		fakeTool{name: "good", text: "ok"},
		fakeTool{name: "bad name", text: "ok"}, // 含空格 → 不可注册
	)
	conn := connectFake(ctx, t, "srv", tr)
	got, errs := conn.discover(ctx)
	if len(got) != 1 || got[0].wireName != "good" {
		t.Fatalf("应只保留 good（得到 %+v）", got)
	}
	if len(errs) != 1 {
		t.Fatalf("应有一条错误（得到 %d 条）", len(errs))
	}
	mustContain(t, errs[0], "bad name")
}

// TestExecutePassesArgsVerbatim：参数**原样**过桥（loop 已校验是 JSON 对象，
// 适配层不再解析/改写）。server 侧收到的字节应与调用方给的逐字节相同。
func TestExecutePassesArgsVerbatim(t *testing.T) {
	ctx := context.Background()
	tr, hooks, _ := startFake(t, fakeTool{name: "echo", echo: true, text: "ok"})
	conn := connectFake(ctx, t, "srv", tr)
	got, _ := conn.discover(ctx)
	if len(got) != 1 {
		t.Fatalf("发现 %d 个工具", len(got))
	}
	exec := &mcpExecutor{conn: conn, wireName: got[0].wireName, name: got[0].name}

	// 故意用非常规字节序/空格的 JSON（合法但不好看）——原样过桥的判据。
	args := json.RawMessage(`{ "b" : 2 ,  "a" : [1,2] }`)
	res, err := exec.Execute(ctx, contracts.ToolCallIntentPayload{
		ToolName: got[0].name, Arguments: args,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(res.Content) != 1 || res.Content[0].Text == nil {
		t.Fatalf("返回值形状异常: %+v", res)
	}
	// server 侧收到的入参（经 SDK 解成 any 再编回）应与原值**语义相等**。
	select {
	case rec := <-hooks.calls:
		if rec.Name != "echo" {
			t.Fatalf("server 收到的工具名 = %q，期望 wire 名 echo（前缀只在注册面）", rec.Name)
		}
		var want, gotAny any
		if err := json.Unmarshal(args, &want); err != nil {
			t.Fatalf("原参数不是合法 JSON: %v", err)
		}
		if err := json.Unmarshal(rec.Args, &gotAny); err != nil {
			t.Fatalf("server 收到的参数不是合法 JSON: %v", err)
		}
		if mustJSON(want) != mustJSON(gotAny) {
			t.Fatalf("参数被改写：原 %s → server 收到 %s", mustJSON(want), mustJSON(gotAny))
		}
	default:
		t.Fatalf("server 没收到 tools/call")
	}
}

// TestExecuteEmptyArgs：空参数补 {}（合法，MCP 侧按无参处理）。
func TestExecuteEmptyArgs(t *testing.T) {
	ctx := context.Background()
	tr, hooks, _ := startFake(t, fakeTool{name: "noargs", text: "ok"})
	conn := connectFake(ctx, t, "srv", tr)
	got, _ := conn.discover(ctx)
	exec := &mcpExecutor{conn: conn, wireName: got[0].wireName, name: got[0].name}
	if _, err := exec.Execute(ctx, contracts.ToolCallIntentPayload{Arguments: nil}); err != nil {
		t.Fatalf("Execute(nil args): %v", err)
	}
	select {
	case rec := <-hooks.calls:
		if string(rec.Args) != "{}" {
			t.Fatalf("空参数应补 {}（得到 %s）", rec.Args)
		}
	default:
		t.Fatalf("server 没收到 tools/call")
	}
}

// TestExecuteIsErrorIsNotGoError：MCP 的 isError=true 是**业务结果**不是 Go error
// （与 bash 非零退出码同口径：模型看得见失败才能自我修正，M5）。
// 但它要在文本里被明确标注，否则模型分不清"正常输出"与"工具报错"。
func TestExecuteIsErrorIsNotGoError(t *testing.T) {
	ctx := context.Background()
	tr, _, _ := startFake(t, fakeTool{name: "boom", text: "detail: 权限不足", isError: true})
	conn := connectFake(ctx, t, "srv", tr)
	got, _ := conn.discover(ctx)
	exec := &mcpExecutor{conn: conn, wireName: got[0].wireName, name: got[0].name}
	res, err := exec.Execute(ctx, contracts.ToolCallIntentPayload{Arguments: json.RawMessage("{}")})
	if err != nil {
		t.Fatalf("isError=true 不应变成 Go error（那是业务结果）: %v", err)
	}
	text := toolResultTextOf(res)
	if !contains(text, "isError") || !contains(text, "detail: 权限不足") {
		t.Fatalf("失败标注或原文缺失：%q", text)
	}
}

// toolResultTextOf 把 ToolResult 的文本块拼起来（测试断言用；与 loop 的
// toolResultText 同口径，但不 import core/loop——测试包不该依赖被测方的消费者）。
func toolResultTextOf(res contracts.ToolResult) string {
	var out string
	for _, p := range res.Content {
		if p.Text != nil {
			out += *p.Text
		}
	}
	return out
}

// TestExecuteStructuredContent：structuredContent 折成 type=json 的 Data 块
// （契约的 ContentPart.Type 是开放字符串，Data 是原始 JSON）。
func TestExecuteStructuredContent(t *testing.T) {
	ctx := context.Background()
	tr, _, _ := startFake(t, fakeTool{
		name: "s", text: "", structured: map[string]any{"ok": true, "n": 1},
	})
	conn := connectFake(ctx, t, "srv", tr)
	got, _ := conn.discover(ctx)
	exec := &mcpExecutor{conn: conn, wireName: got[0].wireName, name: got[0].name}
	res, err := exec.Execute(ctx, contracts.ToolCallIntentPayload{Arguments: json.RawMessage("{}")})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	var found bool
	for _, p := range res.Content {
		if p.Type == "json" && len(p.Data) > 0 {
			found = true
			var v map[string]any
			if err := json.Unmarshal(p.Data, &v); err != nil {
				t.Fatalf("Data 不是合法 JSON: %v", err)
			}
			if v["ok"] != true {
				t.Fatalf("结构化内容丢了：%s", p.Data)
			}
		}
	}
	if !found {
		t.Fatalf("没有 type=json 的结构化块：%+v", res.Content)
	}
}

// TestExecuteServerCrash：server 侧工具 handler 返回 error（= server 崩溃/内部错）
// → executor 返回 **Go error**（协议层失败）→ loop 会走 tool.call.failed。
// 关键：它不该 panic、不该拖垮调用方。
func TestExecuteServerCrash(t *testing.T) {
	ctx := context.Background()
	tr, _, _ := startFake(t, fakeTool{
		name: "crash",
		handler: func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return nil, errBoom
		},
	})
	conn := connectFake(ctx, t, "srv", tr)
	got, _ := conn.discover(ctx)
	exec := &mcpExecutor{conn: conn, wireName: got[0].wireName, name: got[0].name}
	res, err := exec.Execute(ctx, contracts.ToolCallIntentPayload{Arguments: json.RawMessage("{}")})
	if err == nil {
		t.Fatalf("server 内部错误应变成 Go error（走 tool.call.failed）")
	}
	mustContain(t, err, "tools/call 失败")
	if len(res.Content) != 0 {
		t.Fatalf("失败路径不该带内容: %+v", res.Content)
	}
}

// TestExecuteAfterDisconnect：连接断开后再调用 → Go error，不 panic。
func TestExecuteAfterDisconnect(t *testing.T) {
	ctx := context.Background()
	tr, _, _ := startFake(t, fakeTool{name: "t", text: "ok"})
	conn := connectFake(ctx, t, "srv", tr)
	got, _ := conn.discover(ctx)
	exec := &mcpExecutor{conn: conn, wireName: got[0].wireName, name: got[0].name}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := exec.Execute(ctx, contracts.ToolCallIntentPayload{Arguments: json.RawMessage("{}")}); err == nil {
		t.Fatalf("断开后调用应报错（fail-loud，不静默成功）")
	}
}

// TestExecuteCanceled：ctx 取消 → 明确报"被中止"（不是笼统的 tools/call 失败）。
func TestExecuteCanceled(t *testing.T) {
	ctx := context.Background()
	tr, _, _ := startFake(t, fakeTool{name: "t", text: "ok"})
	conn := connectFake(ctx, t, "srv", tr)
	got, _ := conn.discover(ctx)
	exec := &mcpExecutor{conn: conn, wireName: got[0].wireName, name: got[0].name}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err := exec.Execute(cctx, contracts.ToolCallIntentPayload{Arguments: json.RawMessage("{}")})
	mustContain(t, err, "被中止")
}
