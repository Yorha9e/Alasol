package mcp

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// N2（07 决策总表）：sampling / elicitation / roots **明确不实现**。
// 这里证三件事：不广告、明确拒、留痕。全部走**真协议路径**（in-memory transport）。

// TestN2NotAdvertisedEndToEnd：client 在 initialize 里广告的 capabilities 里
// 没有 sampling / elicitation / roots（N2 第一层：不广告）。
//
// 观测点：server 侧工具 handler 里读 req.Session.InitializeParams()——
// 那是 client 真的通过 wire 发过来的东西，不是我们代码里的常量。
func TestN2NotAdvertisedEndToEnd(t *testing.T) {
	ctx := context.Background()
	observed := make(chan *mcp.ClientCapabilities, 1)
	tr, _, _ := startFake(t, fakeTool{
		name: "probe",
		handler: func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			if p := req.Session.InitializeParams(); p != nil {
				observed <- p.Capabilities
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil
		},
	})
	conn := connectFake(ctx, t, "srv", tr)
	got, errs := conn.discover(ctx)
	if len(errs) != 0 || len(got) != 1 {
		t.Fatalf("发现异常: %+v / %v", got, errs)
	}
	exec := &mcpExecutor{conn: conn, wireName: got[0].wireName, name: got[0].name}
	if _, err := exec.Execute(ctx, intentWith(`{}`)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	select {
	case caps := <-observed:
		if caps == nil {
			t.Fatalf("client 没广告 capabilities")
		}
		if caps.Sampling != nil {
			t.Fatalf("广告了 sampling（N2：不实现）: %+v", caps.Sampling)
		}
		if caps.Elicitation != nil {
			t.Fatalf("广告了 elicitation（N2：不实现）: %+v", caps.Elicitation)
		}
		if caps.RootsV2 != nil {
			t.Fatalf("广告了 rootsV2（N2：不实现）: %+v", caps.RootsV2)
		}
		// SDK 默认会给 roots.listChanged=true；我们显式清空 → 必须是 false。
		if caps.Roots.ListChanged {
			t.Fatalf("广告了 roots.listChanged（N2：不实现；SDK 默认会给 true，必须显式清空）")
		}
	default:
		t.Fatalf("server 侧没观察到 initialize 参数")
	}
}

// TestN2RequestsRejectedEndToEnd：server 主动向 client 发 N2 请求的两种结局。
//
//	① roots/list 与 sampling/createMessage：**真发到 wire 上**，被我们的中间件
//	   明确拒绝，server 拿到带 N2 说明的错误（不是空列表、不是超时）。
//	② elicitation/create：SDK 在**发送前**就按"client 没广告 elicitation"拒了
//	   （ss.Elicit 的能力预检）→ 请求根本没出 server 的门。
//
// ② 不是缺陷而是第一层防线（不广告 → server 问都不问）；我们的中间件是第二层，
// 专治不守规矩的 server（中间件层的单测已覆盖 elicitation 的 wire 拒绝）。
//
// 这一层比 SDK 默认行为更强：SDK 对 roots/list 会回一个**空列表**（静默忽略），
// 而 N2 要的是明确拒绝——所以中间件必须在默认 handler 之前返回错误。
//
// 协议版本口径：协议 2026-07-28 起（SEP-2322 multi-round-trip），SDK **自己**就
// 禁止 server 在服务请求期间向 client 发这些请求（根本发不出去）。所以 wire 层的
// 拒绝只在**旧协议版本**上可观测——这里用 2025-06-18 连接来验。
func TestN2RequestsRejectedEndToEnd(t *testing.T) {
	ctx := context.Background()
	tr, _, ss := startFake(t, fakeTool{name: "probe", text: "ok"})

	trace := newN2Trace()
	cs, err := newMCPClient(trace).Connect(ctx, tr, &mcp.ClientSessionOptions{
		ProtocolVersion: "2025-06-18", // 旧协议：允许 server 主动发这些请求
	})
	if err != nil {
		t.Fatalf("连接测试替身 server 失败: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	// ① 真上 wire 的两个：必须拿到**我们的** N2 拒绝文案。
	_, rootsErr := ss.ListRoots(ctx, &mcp.ListRootsParams{})
	mustContain(t, rootsErr, "N2")
	mustContain(t, rootsErr, "roots")

	_, samplingErr := ss.CreateMessage(ctx, &mcp.CreateMessageParams{
		Messages: []*mcp.SamplingMessage{{Role: "user", Content: &mcp.TextContent{Text: "hi"}}},
	})
	mustContain(t, samplingErr, "N2")
	mustContain(t, samplingErr, "sampling")

	// ② elicitation：SDK 的能力预检在发送前就拒了 → 我们的 trace 不该有记录
	// （证明"不广告"这层真的挡住了请求，而不是靠中间件兜）。
	_, elicitErr := ss.Elicit(ctx, &mcp.ElicitParams{Message: "需要输入"})
	if elicitErr == nil {
		t.Fatalf("elicitation 应被拒")
	}
	if strings.Contains(elicitErr.Error(), "N2") {
		t.Fatalf("elicitation 不该走到我们的中间件（SDK 的能力预检应该先拒）: %v", elicitErr)
	}
	if snap := trace.snapshot(); snap.Methods[methodElicitationCreate] != 0 {
		t.Fatalf("elicitation 不该留痕（它没上 wire）：%+v", snap)
	}

	// 留痕：真正上了 wire 的两个各一次。
	snap := trace.snapshot()
	if snap.Rejected != 2 {
		t.Fatalf("trace 应记下 2 次被拒（得到 %d）", snap.Rejected)
	}
	if snap.Methods[methodRootsList] != 1 || snap.Methods[methodSamplingCreateMessage] != 1 {
		t.Fatalf("trace 明细不对: %+v", snap.Methods)
	}
}

// TestN2MiddlewareRejectsAndTraces：中间件层的单元断言（与端到端互补：
// 这里钉死"透传计数为 0"与"非 N2 方法不受影响"）。
func TestN2MiddlewareRejectsAndTraces(t *testing.T) {
	trace := newN2Trace()
	mw := n2Middleware(trace)
	nextCalled := 0
	next := func(context.Context, string, mcp.Request) (mcp.Result, error) {
		nextCalled++
		return &mcp.InitializeResult{}, nil
	}
	h := mw(next)

	for _, method := range []string{methodSamplingCreateMessage, methodElicitationCreate, methodRootsList} {
		res, err := h(context.Background(), method, nil)
		if err == nil {
			t.Fatalf("%s 应被明确拒绝（得到结果 %+v）", method, res)
		}
		if !strings.Contains(err.Error(), "N2") {
			t.Fatalf("%s 的拒绝理由未说明 N2：%v", method, err)
		}
	}
	if nextCalled != 0 {
		t.Fatalf("N2 请求被透传到了默认 handler（%d 次）——那不是明确拒绝", nextCalled)
	}

	snap := trace.snapshot()
	if snap.Rejected != 3 {
		t.Fatalf("trace.Rejected = %d，期望 3", snap.Rejected)
	}
	for _, m := range []string{methodSamplingCreateMessage, methodElicitationCreate, methodRootsList} {
		if snap.Methods[m] != 1 {
			t.Fatalf("trace.Methods[%s] = %d，期望 1", m, snap.Methods[m])
		}
	}

	// 非 N2 方法照常透传（中间件不该影响 tools/call 之类的正常请求）。
	if _, err := h(context.Background(), "tools/call", nil); err != nil {
		t.Fatalf("tools/call 被中间件误拒: %v", err)
	}
	if nextCalled != 1 {
		t.Fatalf("非 N2 请求应透传到默认 handler（nextCalled=%d）", nextCalled)
	}
}

// TestN2TraceBounded：trace 的方法明细有上限（防恶意 server 刷爆内存）。
func TestN2TraceBounded(t *testing.T) {
	trace := newN2Trace()
	for i := 0; i < MaxN2TraceMethods*3; i++ {
		trace.record("custom/method/" + string(rune('a'+i%26)) + string(rune('a'+i/26)))
	}
	snap := trace.snapshot()
	if int64(len(snap.Methods)) > MaxN2TraceMethods {
		t.Fatalf("trace 明细 %d 条，超过上限 %d", len(snap.Methods), MaxN2TraceMethods)
	}
	if snap.Rejected != int64(MaxN2TraceMethods*3) {
		t.Fatalf("总数应照实记（%d），得到 %d", MaxN2TraceMethods*3, snap.Rejected)
	}
	if snap.Dropped == 0 {
		t.Fatalf("应有被丢弃的明细计数")
	}
}

// TestN2TraceNilSafe：nil trace 不 panic（serverConn 一定建 trace，但防御性断言）。
func TestN2TraceNilSafe(t *testing.T) {
	var trace *n2Trace
	trace.record("roots/list")
	if s := trace.snapshot(); s.Rejected != 0 {
		t.Fatalf("nil trace 的快照应为零值，得到 %+v", s)
	}
}

// TestN2RejectMessages：三个能力的拒绝文案各自说清"为什么不支持 + 该怎么办"。
func TestN2RejectMessages(t *testing.T) {
	if got := n2RejectMessage(methodSamplingCreateMessage); !strings.Contains(got, "loop") {
		t.Fatalf("sampling 拒绝文案未说明理由（模型调用权在 loop 手里）: %q", got)
	}
	if got := n2RejectMessage(methodElicitationCreate); !strings.Contains(got, "工具参数") {
		t.Fatalf("elicitation 拒绝文案未给替代方案: %q", got)
	}
	if got := n2RejectMessage(methodRootsList); !strings.Contains(got, "R5") {
		t.Fatalf("roots 拒绝文案未引用可见性哲学: %q", got)
	}
}
