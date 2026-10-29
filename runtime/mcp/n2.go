package mcp

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// N2（07 决策总表）：**sampling / elicitation / roots 明确不实现**。
//
// 三条理由（写在这里，免得后人"顺手补上"）：
//
//  1. **sampling 让 server 反过来驱动我们的模型**。alasol 的模型调用权在 loop 手里
//     （provider、上下文装配、成本计量都在那儿）。放开 sampling = 外部 server 能
//     让 agent 用我们的 key 跑任意 prompt——那是把凭据与上下文一起交出去。
//  2. **elicitation 是"向用户要输入"的交互通道**。v0 的交互面只有 TUI/终端的审批器，
//     没有通用的"server 弹一个问题给用户"的通道；接了也是半成品。
//  3. **roots 是"告诉 server 我能看哪些目录"**。它与 R5"不暴露即不存在"的可见性
//     哲学冲突：工具可见性是 agent 类型的白名单决策，不该由 client 广播根目录。
//
// 落地口径（三层，缺一不可）：
//
//	不广告  → ClientOptions.Capabilities 显式给空（默认会给 roots+listChanged）
//	明确拒  → 接收中间件对这三个方法直接返回错误（roots/list 默认会回空列表，
//	          那是"静默忽略"而不是"拒绝"——N2 要的是明确拒绝）
//	留痕    → 同一中间件把请求记进 n2Trace（server 试过什么，事后可查）
//
// 上游注记：go-sdk v1.8.0 的 sampling 已被标记 Deprecated（协议 2026-07-28，
// SEP-2577 弃用 roots/sampling/logging）——不实现它与上游方向一致。

// N2 方法名（MCP wire method 字符串；SDK 里是未导出常量，这里按协议固定值对齐）。
const (
	methodSamplingCreateMessage = "sampling/createMessage"
	methodElicitationCreate     = "elicitation/create"
	methodRootsList             = "roots/list"
)

// n2Methods 是被拒绝并留痕的方法集合（key = wire method）。
var n2Methods = map[string]string{
	methodSamplingCreateMessage: "sampling",
	methodElicitationCreate:     "elicitation",
	methodRootsList:             "roots",
}

// n2Trace 记录 server 尝试调用 N2 能力的事实（留痕用）。
//
// 并发安全：原子计数 + 互斥锁保护的方法明细（上限 MaxN2TraceMethods 条，
// 防恶意 server 刷爆内存——trace 是诊断设施，不是日志管道）。
type n2Trace struct {
	mu       sync.Mutex
	methods  map[string]int64 // wire method → 次数
	rejected int64            // 被拒请求总数
	dropped  int64            // 因超出明细上限而丢弃的明细数
}

// MaxN2TraceMethods 是 trace 里保留的方法明细条数上限。
const MaxN2TraceMethods = 64

func newN2Trace() *n2Trace {
	return &n2Trace{methods: make(map[string]int64)}
}

// record 记一次被拒的 N2 请求。
func (t *n2Trace) record(method string) {
	if t == nil {
		return
	}
	atomic.AddInt64(&t.rejected, 1)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.methods == nil {
		t.methods = make(map[string]int64)
	}
	if _, ok := t.methods[method]; !ok && len(t.methods) >= MaxN2TraceMethods {
		t.dropped++
		return
	}
	t.methods[method]++
}

// snapshot 是 trace 的值语义快照（外部可安全持有/打印）。
type n2Snapshot struct {
	Rejected int64            `json:"rejected"`
	Methods  map[string]int64 `json:"methods,omitempty"`
	Dropped  int64            `json:"dropped,omitempty"`
}

func (t *n2Trace) snapshot() n2Snapshot {
	if t == nil {
		return n2Snapshot{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := n2Snapshot{Rejected: atomic.LoadInt64(&t.rejected), Dropped: t.dropped}
	if len(t.methods) > 0 {
		out.Methods = make(map[string]int64, len(t.methods))
		for k, v := range t.methods {
			out.Methods[k] = v
		}
	}
	return out
}

// n2Middleware 返回一个**接收**中间件：server 发来的 N2 请求一律明确拒绝 + 留痕。
//
// 为什么挂在接收侧而不是装 handler：装 handler（CreateMessageHandler /
// ElicitationHandler）会让 client **广告**对应能力——那正是 N2 要不做的事。
// 中间件在默认 handler 之前跑，所以能在 SDK 的默认行为（sampling/elicitation
// 返回 "does not support"、roots 返回空列表）之前给出**统一且明确**的拒绝，
// 同时把事实记进 trace。
func n2Middleware(trace *n2Trace) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if _, bad := n2Methods[method]; bad {
				trace.record(method)
				return nil, &jsonrpc.Error{Code: codeUnsupportedCapability, Message: n2RejectMessage(method)}
			}
			return next(ctx, method, req)
		}
	}
}

// codeUnsupportedCapability 是"客户端不实现该能力"的应用自定义错误码（-32000，
// JSON-RPC 为应用保留的 -32000..-32099 段；SDK 的哨兵占用 -32001/-32003/-32004/-32005，
// 不用 -32601）。
//
// 为什么**不能**用 -32601（MethodNotFound，语义上最像）：SDK 的 jsonrpc2 在
// processResult 里对 ErrNotHandled/ErrMethodNotFound 做特殊处理——因为
// WireError.Is **只比较 code**，我们的 -32601 会被 errors.Is 判定成
// ErrMethodNotFound，整条消息被改写成 "method not found: <method>"，
// N2 的说明文案（为什么不支持 + 该怎么办）就丢了。本刀实测踩到，故钉在此处。
const codeUnsupportedCapability = -32000

// n2RejectMessage 是拒绝对 server 的说明文案。
func n2RejectMessage(method string) string {
	switch n2Methods[method] {
	case "sampling":
		return "alasol MCP client 不支持 sampling（N2）：模型调用权在 agent loop 手里，" +
			"server 不得反向驱动客户端模型；请直接调用 LLM provider API"
	case "elicitation":
		return "alasol MCP client 不支持 elicitation（N2）：没有通用的 server→用户提问通道；" +
			"请把需要的输入改为工具参数"
	case "roots":
		return "alasol MCP client 不支持 roots（N2）：不向 server 广播可访问的根目录（R5：不暴露即不存在）"
	default:
		return "alasol MCP client 不支持该方法（N2：sampling / elicitation / roots 不实现）"
	}
}
