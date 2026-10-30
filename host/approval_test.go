package host_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/transport"
)

// textThenToolProvider：第一次调用只回文本，第二次调用才发起工具调用。
//
// 为什么需要它（协议面 prompt 端点的准入条件，G2/G25）：prompt 端点只对**本进程托管**
// 的 session 生效，而"托管"= 本进程已经为它建过 loop（= 跑过一轮）。预热那一轮**不能**
// 触发审批（否则它挂在审批上等应答），所以先让 provider 回一轮纯文本把 session 跑热。
type textThenToolProvider struct {
	calls int
	// args 是工具调用的参数（调用方可注入绝对路径）。
	args string
}

func (p *textThenToolProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if p.calls == 1 {
		emit(core.ProviderEvent{Kind: "text.delta", Text: "预热好了"})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{Text: "预热好了", Usage: core.Usage{InputTokens: 5, OutputTokens: 2}}, nil
	}
	tc := core.ToolCallReq{
		ID:   contracts.ProviderId("toolu_SCRIPTED_01"),
		Name: "read",
		Args: json.RawMessage(p.args),
	}
	emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
	emit(core.ProviderEvent{Kind: "done"})
	return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 10, OutputTokens: 5}}, nil
}

// warmupSession 跑一轮纯文本，让本进程**托管**这个 session（协议面 prompt 端点的准入条件）。
//
// 返回 session id。托管 = h.loops 里有它（跑过一轮就有）——别的进程托管的 session
// 在本进程永远不是"托管"，prompt 端点对它们一律 404（G25：跨进程双写会破因果序）。
func warmupSession(t *testing.T, h *host.Hub) contracts.Ulid {
	t.Helper()
	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "预热"); err != nil {
		t.Fatalf("预热 RunPrompt: %v", err)
	}
	return sid
}

// recvApprovalRequest 收 WS 消息直到一条 kind=approval.request，返回其中的 ApprovalRequest。
func recvApprovalRequest(t *testing.T, c *transport.Client) contracts.ApprovalRequest {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		recvCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		m, err := c.RecvContext(recvCtx)
		cancel()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if m.Kind == transport.KindApprovalRequest {
			var req contracts.ApprovalRequest
			if err := json.Unmarshal(m.Payload, &req); err != nil {
				t.Fatalf("approval.request payload: %v", err)
			}
			return req
		}
	}
	t.Fatal("10s 内没收到 approval.request")
	return contracts.ApprovalRequest{}
}

// waitEventKind 收 WS 消息直到一条 kind=event 且 payload.kind == want（超时 → false）。
func waitEventKind(t *testing.T, c *transport.Client, want string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		recvCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		m, err := c.RecvContext(recvCtx)
		cancel()
		if err != nil {
			continue // 还没到的事件：继续等
		}
		if m.Kind != transport.KindEvent {
			continue
		}
		var ev contracts.Event
		if err := json.Unmarshal(m.Payload, &ev); err != nil {
			continue
		}
		if ev.Kind == want {
			return true
		}
	}
	return false
}

// waitWSKind 收 WS 消息直到一条指定 kind 的（非事件消息会被跳过），返回它。
func waitWSKind(t *testing.T, c *transport.Client, want string, timeout time.Duration) contracts.TransportMessage {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		recvCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		m, err := c.RecvContext(recvCtx)
		cancel()
		if err != nil {
			continue // 还没到：继续等
		}
		if m.Kind == want {
			return m
		}
	}
	t.Fatalf("%s 内没收到 kind=%s", timeout, want)
	return contracts.TransportMessage{}
}

// TestPromptRequiresHostedSession（P2-2 钉子）：prompt 端点只对**本进程托管**的 session
// 生效——别的进程托管的 session 在这儿起 prompt = 本进程就地新建 loop 双写同一 agent
// （G25 破）。所以：未托管 → 404 unknown_session，且**不新建 loop**（第二次仍然 404）。
func TestPromptRequiresHostedSession(t *testing.T) {
	h := newTestHub(t)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	var te contracts.TransportError
	if code := postJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]string{"prompt": "你好"}, &te); code != http.StatusNotFound {
		t.Fatalf("未托管 session 的 prompt 状态码 = %d，want 404", code)
	}
	if te.Code != "unknown_session" {
		t.Fatalf("错误码 = %q，want unknown_session", te.Code)
	}
	// 再发一次仍是 404 → 说明**没有就地新建 loop**（建了就变"托管"，第二次会 202）。
	if code := postJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]string{"prompt": "你好"}, &te); code != http.StatusNotFound {
		t.Fatalf("第二次 prompt 状态码 = %d，want 404（不该就地建 loop）", code)
	}
	// 事件流里也不该有它的任何事件（没跑过）。
	var view struct {
		Events []contracts.Event `json:"events"`
	}
	if code := getJSON(t, srv.URL+"/api/events?session_id="+string(sid), &view); code != http.StatusOK {
		t.Fatalf("事件查询状态码 = %d", code)
	}
	if len(view.Events) != 0 {
		t.Fatalf("未托管 session 不该产生任何事件，得到 %d 条", len(view.Events))
	}
}

// TestWSCommandSubmitSessionValidation（P3-3 钉子）：WS command.submit 的 session_id
// 必须是合法 ULID，且（显式给出时）必须由本进程托管——否则回 kind=error，**不假受理**
// （受理回执说 running，可那个 session 根本不会被跑，客户端会一直等一个不会来的事件）。
func TestWSCommandSubmitSessionValidation(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) {
		c.Provider = &textThenToolProvider{} // 不打网络
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := transport.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	// 订阅事件流：case ③ 要断言"新会话真跑起来了"（事件流里出现它的 user.prompt）。
	if err := c.Subscribe(ctx, contracts.SubscribeRequest{}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitWSKind(t, c, transport.KindSubscribed, 10*time.Second)

	// ① 非法 session_id → kind=error。
	bad := contracts.Ulid("not-a-ulid")
	msg, err := transport.NewMessage(transport.KindCommandSubmit, contracts.CommandSubmit{
		SessionID: &bad, Prompt: "读一下",
	})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if err := c.SendContext(ctx, msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	m := waitWSKind(t, c, transport.KindError, 10*time.Second)
	var te contracts.TransportError
	if err := json.Unmarshal(m.Payload, &te); err != nil {
		t.Fatalf("error payload: %v", err)
	}
	if !strings.Contains(te.Message, "session_id") {
		t.Fatalf("错误文案应点名 session_id，得到 %q", te.Message)
	}

	// ② 合法但未托管的 session_id → kind=error（跨进程双写的前兆，不受理）。
	foreign := host.NewSession()
	msg2, err := transport.NewMessage(transport.KindCommandSubmit, contracts.CommandSubmit{
		SessionID: &foreign, Prompt: "读一下",
	})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if err := c.SendContext(ctx, msg2); err != nil {
		t.Fatalf("send: %v", err)
	}
	m2 := waitWSKind(t, c, transport.KindError, 10*time.Second)
	var te2 contracts.TransportError
	if err := json.Unmarshal(m2.Payload, &te2); err != nil {
		t.Fatalf("error payload: %v", err)
	}
	if !strings.Contains(te2.Message, "不是本 hub 托管") {
		t.Fatalf("错误文案应说明未托管，得到 %q", te2.Message)
	}

	// ③ 不带 session_id → 服务端新建 session（协议面开新会话的入口）→ command.accepted，
	//    且**那一轮真跑起来**（事件流里出现该 session 的 user.prompt）。
	//
	// 为什么要钉"真跑起来"：受理回执只说明"收下了"——若新会话只是被受理却没真跑
	// （托管校验/起 loop 那一步出错），客户端会一直等一个不会来的事件。故这里等
	// **事件流里的 user.prompt**（Run 开始落的那条，F1）——它才说明这轮真的开始了。
	msg3, err := transport.NewMessage(transport.KindCommandSubmit, contracts.CommandSubmit{Prompt: "读一下"})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if err := c.SendContext(ctx, msg3); err != nil {
		t.Fatalf("send: %v", err)
	}
	ca := waitCommandAcceptedAndUserPrompt(t, c, 20*time.Second)
	if ca.SessionID == "" || ca.Status != "running" {
		t.Fatalf("command.accepted 应带回新 session id + running，得到 %+v", ca)
	}
}

// waitCommandAcceptedAndUserPrompt 收消息直到同时看到 command.accepted 与**某个**
// session 的 user.prompt 事件，并断言两者指向**同一个** session（协议面唯一的新会话
// 入口真跑起来了）。
//
// 为什么自己收而不是用 waitWSKind：受理回执与事件流推送的**先后不确定**（受理回执是
// 同步回给客户端的，事件是后台 goroutine 落的）——按顺序等会把其中一个吃掉。
func waitCommandAcceptedAndUserPrompt(t *testing.T, c *transport.Client, timeout time.Duration) contracts.CommandAccepted {
	t.Helper()
	var accepted contracts.CommandAccepted
	gotAccepted := false
	promptSessions := map[contracts.Ulid]bool{}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if gotAccepted && promptSessions[accepted.SessionID] {
			return accepted
		}
		recvCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		m, err := c.RecvContext(recvCtx)
		cancel()
		if err != nil {
			continue // 还没到：继续等
		}
		switch m.Kind {
		case transport.KindCommandAccepted:
			var ca contracts.CommandAccepted
			if err := json.Unmarshal(m.Payload, &ca); err != nil {
				t.Fatalf("command.accepted payload: %v", err)
			}
			accepted, gotAccepted = ca, true
		case transport.KindEvent:
			var ev contracts.Event
			if err := json.Unmarshal(m.Payload, &ev); err != nil {
				continue
			}
			if ev.Kind == "user.prompt" {
				promptSessions[ev.SessionID] = true
			}
		}
	}
	t.Fatalf("%s 内没等到 command.accepted + 同 session 的 user.prompt（accepted=%+v prompt_sessions=%v）",
		timeout, accepted, promptSessions)
	return contracts.CommandAccepted{}
}

// TestWSApprovalRespondInvalidDecision（P3-4 钉子）：WS approval.respond 的 decision 非法
// → 回 kind=error（不再"静默吃掉挂起审批 + 零回执"），且**挂起的审批不被消费**
// （随后一条合法 allow 仍被接受 → 工具真的执行）。
func TestWSApprovalRespondInvalidDecision(t *testing.T) {
	target := filepath.Join(t.TempDir(), "ask-target.txt")
	if err := os.WriteFile(target, []byte("approved"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	h := newTestHub(t, func(c *host.Config) {
		c.Allow = nil
		c.Ask = []string{"read"}
		c.Approval = "protocol"
		c.Provider = &textThenToolProvider{args: `{"path":"` + filepath.ToSlash(target) + `"}`}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := transport.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	// 订阅事件流（tool.call.completed 经事件流可见）。
	if err := c.Subscribe(ctx, contracts.SubscribeRequest{}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitWSKind(t, c, transport.KindSubscribed, 10*time.Second)

	sid := warmupSession(t, h)
	if code := postJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]string{"prompt": "读一下目标文件"}, nil); code != http.StatusAccepted {
		t.Fatalf("prompt 状态码 = %d，want 202", code)
	}
	req := recvApprovalRequest(t, c)

	// 非法 decision → kind=error。
	bad, err := transport.NewMessage(transport.KindApprovalRespond, contracts.ApprovalResponse{
		V: 1, RequestID: req.ID, TS: req.TS, Decision: contracts.ApprovalDecision("maybe"),
	})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if err := c.SendContext(ctx, bad); err != nil {
		t.Fatalf("send: %v", err)
	}
	m := waitWSKind(t, c, transport.KindError, 10*time.Second)
	var te contracts.TransportError
	if err := json.Unmarshal(m.Payload, &te); err != nil {
		t.Fatalf("error payload: %v", err)
	}
	if !strings.Contains(te.Message, "decision") {
		t.Fatalf("错误文案应点名 decision，得到 %q", te.Message)
	}

	// 挂起的审批**没被吃掉**：一条合法 allow 仍被接受 → 工具真的执行。
	good, err := transport.NewMessage(transport.KindApprovalRespond, contracts.ApprovalResponse{
		V: 1, RequestID: req.ID, TS: req.TS, Decision: contracts.ApprovalDecisionAllow,
	})
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if err := c.SendContext(ctx, good); err != nil {
		t.Fatalf("send: %v", err)
	}
	if !waitEventKind(t, c, "tool.call.completed", 15*time.Second) {
		t.Fatal("非法 decision 不该吃掉挂起的审批（合法 allow 后工具应执行）")
	}
}

// TestProtocolApprovalEndToEndAllow（G1 硬验收·正向）：协议面发 prompt → 跑一轮 →
// ask 触发 → approval.request 经 WS 可见 → approval.respond 应答 → 工具真的执行
// （tool.call.completed）→ loop 继续。
func TestProtocolApprovalEndToEndAllow(t *testing.T) {
	target := filepath.Join(t.TempDir(), "ask-target.txt")
	if err := os.WriteFile(target, []byte("approved"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	h := newTestHub(t,
		func(c *host.Config) {
			c.Allow = nil            // 不放行 read → 走 ask
			c.Ask = []string{"read"} // read 命中 ask
			c.Approval = "protocol"  // 协议审批器（G1）
			// 首轮只回文本（预热用），第二轮才发起工具调用 → ask 触发审批。
			c.Provider = &textThenToolProvider{args: `{"path":"` + filepath.ToSlash(target) + `"}`}
		},
	)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// WS 客户端（订阅全部：既收事件，也收 approval.request 广播）。
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := transport.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, contracts.SubscribeRequest{}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	ack, err := c.RecvContext(ackCtx)
	ackCancel()
	if err != nil || ack.Kind != transport.KindSubscribed {
		t.Fatalf("订阅回执: kind=%q err=%v", ack.Kind, err)
	}

	// 预热一轮 → 本进程托管这个 session（协议面 prompt 端点的准入条件，G2/G25）。
	sid := warmupSession(t, h)

	// 协议面发 prompt（异步，202）。
	var acc struct {
		SessionID string `json:"session_id"`
		Status    string `json:"status"`
	}
	if code := postJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]string{"prompt": "读一下目标文件"}, &acc); code != http.StatusAccepted {
		t.Fatalf("prompt 状态码 = %d，want 202", code)
	}
	if acc.SessionID != string(sid) || acc.Status != "running" {
		t.Fatalf("受理回执不符: %+v", acc)
	}

	// ask 触发 → approval.request 经 WS 可见。
	req := recvApprovalRequest(t, c)
	if req.SessionID != sid {
		t.Fatalf("审批请求 session = %q，want %q", req.SessionID, sid)
	}
	if req.Trigger != contracts.ApprovalTriggerPolicyAsk || req.Summary == "" {
		t.Fatalf("审批请求要素不齐: %+v", req)
	}

	// 应答 allow → 工具真的执行（tool.call.completed 出现）。
	var ackResp map[string]string
	if code := postJSON(t, srv.URL+"/api/approvals/"+string(req.ID)+"/respond",
		map[string]string{"decision": "allow", "decided_by": "user:gui"}, &ackResp); code != http.StatusOK {
		t.Fatalf("respond 状态码 = %d，want 200", code)
	}
	if ackResp["decision"] != "allow" {
		t.Fatalf("respond 回执 decision = %q", ackResp["decision"])
	}
	if !waitEventKind(t, c, "tool.call.completed", 10*time.Second) {
		t.Fatal("应答 allow 后应看到 tool.call.completed（工具被执行）")
	}
}

// TestProtocolApprovalEndToEndFailClose（G1 硬验收·fail-close）：无人应答（短超时）
// → deny → 工具**不执行**（无 tool.call.completed，有 tool.call.failed/policy_deny）；
// 迟到的应答 → 404（没有挂起的审批了）。
func TestProtocolApprovalEndToEndFailClose(t *testing.T) {
	target := filepath.Join(t.TempDir(), "ask-target.txt")
	if err := os.WriteFile(target, []byte("nope"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	h := newTestHub(t,
		func(c *host.Config) {
			c.Allow = nil
			c.Ask = []string{"read"}
			c.Approval = "protocol"
			c.PromptRunTimeoutSec = 1 // 短超时：逼出 fail-close（无人应答 = deny）
			// 首轮只回文本（预热用），第二轮才发起工具调用 → ask 触发审批。
			c.Provider = &textThenToolProvider{args: `{"path":"` + filepath.ToSlash(target) + `"}`}
		},
	)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := transport.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, contracts.SubscribeRequest{}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if ack, err := c.RecvContext(ackCtx); err != nil || ack.Kind != transport.KindSubscribed {
		t.Fatalf("订阅回执: kind=%q err=%v", ack.Kind, err)
	}
	ackCancel()

	// 预热一轮 → 本进程托管这个 session（协议面 prompt 端点的准入条件）。
	sid := warmupSession(t, h)
	if code := postJSON(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt",
		map[string]string{"prompt": "读一下"}, nil); code != http.StatusAccepted {
		t.Fatalf("prompt 状态码 = %d，want 202", code)
	}

	req := recvApprovalRequest(t, c)
	// **不应答**：等过短超时（1s）+ 余量，断言 tool.call.completed **绝不出现**
	// （fail-close：无人应答 = deny，工具不执行；超时的 ctx 会让随后的 intent 落盘也失败，
	// 于是整轮中止——无论哪种都不得出现 completed）。
	if waitEventKind(t, c, "tool.call.completed", 4*time.Second) {
		t.Fatal("fail-close 破了：无人应答却执行了工具（tool.call.completed 出现）")
	}

	// 迟到的应答 → 404（审批已超时收场，没有挂起的了）。
	var te contracts.TransportError
	if code := postJSON(t, srv.URL+"/api/approvals/"+string(req.ID)+"/respond",
		map[string]string{"decision": "allow"}, &te); code != http.StatusNotFound {
		t.Fatalf("迟到应答状态码 = %d，want 404", code)
	}
}

// TestPromptAndApprovalValidation（G1）：prompt/respond 端点入参校验。
func TestPromptAndApprovalValidation(t *testing.T) {
	h := newTestHub(t, nil)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := string(host.NewSession())
	var te contracts.TransportError

	// prompt：session_id 非法 → 400。
	if code := postJSON(t, srv.URL+"/api/sessions/not-a-ulid/prompt",
		map[string]string{"prompt": "x"}, &te); code != http.StatusBadRequest {
		t.Fatalf("坏 session_id 状态码 = %d，want 400", code)
	}
	// prompt：空 prompt → 400。
	if code := postJSON(t, srv.URL+"/api/sessions/"+sid+"/prompt",
		map[string]string{"prompt": "  "}, &te); code != http.StatusBadRequest {
		t.Fatalf("空 prompt 状态码 = %d，want 400", code)
	}
	// respond：request_id 非法 → 400。
	if code := postJSON(t, srv.URL+"/api/approvals/not-a-ulid/respond",
		map[string]string{"decision": "allow"}, &te); code != http.StatusBadRequest {
		t.Fatalf("坏 request_id 状态码 = %d，want 400", code)
	}
	// respond：decision 非法 → 400。
	if code := postJSON(t, srv.URL+"/api/approvals/"+string(host.NewSession())+"/respond",
		map[string]string{"decision": "maybe"}, &te); code != http.StatusBadRequest {
		t.Fatalf("坏 decision 状态码 = %d，want 400", code)
	}
	// respond：没有挂起的审批 → 404。
	if code := postJSON(t, srv.URL+"/api/approvals/"+string(host.NewSession())+"/respond",
		map[string]string{"decision": "allow"}, &te); code != http.StatusNotFound {
		t.Fatalf("无挂起审批状态码 = %d，want 404", code)
	}
	if te.Code != "no_pending_approval" {
		t.Fatalf("错误码 = %q，want no_pending_approval", te.Code)
	}
}
