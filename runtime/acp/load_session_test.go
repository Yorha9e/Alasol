package acp

// session/load 面的钉子：
//
//  1. **能力声明按后端实际能力动态给**（T3-1）：initialize 的 loadSession = 后端是否实现
//     sessionLoader——宣称 true 却回 -32000 = 骗客户端；
//  2. **载入不阻塞读循环**（T3-2 头阻塞修）：载入（后端回放 + 折叠 + 一串 update）跑在
//     goroutine 上——否则别的 session 的 cancel / 审批应答被一起堵住；
//  3. **回放里的工具参数经脱敏**（T3-6）：历史里的 tool_call rawInput 同样可能含凭据，
//     必须打码后才进 session/update。

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
)

// ---------------------------------------------------------------------------
// T3-1：能力声明按后端能力动态给
// ---------------------------------------------------------------------------

// TestACP_InitializeDeclaresLoadSessionByBackendCapability：initialize 的
// agentCapabilities.loadSession 必须**跟着后端能力走**：
//
//	实现了 sessionLoader 的后端 → true；
//	没实现的后端 → false（否则客户端会真的去 session/load，然后拿到 -32000）。
func TestACP_InitializeDeclaresLoadSessionByBackendCapability(t *testing.T) {
	cases := []struct {
		name    string
		backend Backend
		want    bool
	}{
		{"有载入能力", newFakeBackend(t), true},
		{"无载入能力", &noLoadBackend{Backend: newFakeBackend(t)}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client, cleanup := startACP(t, c.backend, nil, 5*time.Second)
			defer cleanup()

			id := client.sendRequest("initialize", map[string]any{"protocolVersion": 1})
			res, ok := client.waitForResult(id, 3*time.Second)
			if !ok {
				t.Fatal("没收到 initialize 响应")
			}
			var init initializeResult
			if err := json.Unmarshal(res, &init); err != nil {
				t.Fatal(err)
			}
			if init.AgentCapabilities.LoadSession != c.want {
				t.Fatalf("loadSession 声明 = %v, want %v（声明必须跟后端能力一致）",
					init.AgentCapabilities.LoadSession, c.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// T3-2：载入不阻塞读循环（头阻塞）
// ---------------------------------------------------------------------------

// blockingLoadBackend 的 LoadSession 会**卡住**（直到测试放行）——模拟"后端回放很慢"
// （大历史 / 慢存储）。用它验证：载入期间，读循环仍在收别的 session 的帧。
type blockingLoadBackend struct {
	*fakeBackend
	release chan struct{} // 测试放行 → LoadSession 返回
	entered chan struct{} // LoadSession 已进入（测试等它）
}

func newBlockingLoadBackend(t *testing.T) *blockingLoadBackend {
	t.Helper()
	return &blockingLoadBackend{
		fakeBackend: newFakeBackend(t),
		release:     make(chan struct{}),
		entered:     make(chan struct{}),
	}
}

func (b *blockingLoadBackend) LoadSession(ctx context.Context, sid contracts.Ulid) ([]core.Message, error) {
	select {
	case <-b.entered:
	default:
		close(b.entered)
	}
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return b.fakeBackend.LoadSession(ctx, sid)
}

// sendRequestAsync 发一个请求，但**不阻塞**当前 goroutine（写管道放到后台）。
//
// 为什么需要它：被测的病正是"端点的读循环被占住"——客户端往管道写帧时，若读循环不收，
// 写操作会**阻塞**（io.Pipe 是同步的）。若测试自己先卡在写上，就只能靠超时收场（hang），
// 而不是一条清晰的断言失败。故：写放后台，测试在主流程里等**响应**（有明确的超时断言）。
func (c *acpClient) sendRequestAsync(method string, params any) json.RawMessage {
	c.idSeq++
	id := json.RawMessage(strconv.Itoa(c.idSeq))
	go func() {
		o, err := newRequest(id, method, params)
		if err != nil {
			return
		}
		raw, err := json.Marshal(o)
		if err != nil {
			return
		}
		_, _ = c.w.Write(append(raw, '\n'))
	}()
	return id
}

// TestACP_LoadSession_DoesNotBlockOtherSessions（T3-2 头阻塞钉子）：一个 session 的
// session/load 卡在后端回放上时，**另一个 session 的 session/prompt 必须照常跑完**
// （不被载入堵住）。
//
// 病根（旧实现）：load 同步跑在**唯一的读循环**上 → 载入期间读循环不收帧 →
// 别的 session 的 prompt / cancel / 审批应答全部排队（取消无反应、审批悬到超时）。
//
// 变异自证：把 handleLoadSession 改回同步（不派 goroutine）→ 本用例红
// （prompt 的响应永远等不到）。
func TestACP_LoadSession_DoesNotBlockOtherSessions(t *testing.T) {
	backend := newBlockingLoadBackend(t)
	// 既有会话（有历史可载入）。
	sid, err := backend.CreateSession(t.TempDir())
	if err != nil {
		t.Fatalf("造既有会话: %v", err)
	}
	backend.publishUserPrompt(sid, "历史问题")

	client, cleanup := startACP(t, backend, nil, 10*time.Second)
	defer cleanup()

	// 两个新 session（其中一个用来发 prompt）。
	idNew1 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, ok := client.waitForResult(idNew1, 3*time.Second)
	if !ok {
		t.Fatal("没收到 session/new 响应")
	}
	var ns1 newSessionResult
	json.Unmarshal(res, &ns1)
	idNew2 := client.sendRequest("session/new", map[string]any{"cwd": t.TempDir()})
	res, ok = client.waitForResult(idNew2, 3*time.Second)
	if !ok {
		t.Fatal("没收到第二个 session/new 响应")
	}
	var ns2 newSessionResult
	json.Unmarshal(res, &ns2)

	// ① 发一个 session/load（后端会卡住）。
	idLoad := client.sendRequest("session/load", map[string]any{"sessionId": string(sid)})
	// 等后端真的进了 LoadSession（确保此刻正卡在载入上）。
	select {
	case <-backend.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("后端的 LoadSession 没被调（载入没开始？）")
	}

	// ② 载入卡住期间，另一个 session 的 prompt 必须照常跑完。
	//    写请求放后台：端点读循环被占住时，写管道本身会阻塞（那正是被测的病）。
	idPrompt := client.sendRequestAsync("session/prompt", map[string]any{
		"sessionId": ns2.SessionID,
		"prompt":    "别被载入堵住",
	})
	resPrompt, ok := client.waitForResult(idPrompt, 3*time.Second)
	if !ok {
		t.Fatal("载入卡住时，另一 session 的 prompt 被堵住了（load 不该占着读循环）")
	}
	var pr promptResult
	if err := json.Unmarshal(resPrompt, &pr); err != nil {
		t.Fatal(err)
	}
	if pr.StopReason != stopReasonEndTurn {
		t.Errorf("stopReason=%q want %q", pr.StopReason, stopReasonEndTurn)
	}

	// ③ 放行载入 → load 响应照常回来（回放 + 响应都在 goroutine 里顺序写）。
	close(backend.release)
	if _, ok := client.waitForResult(idLoad, 5*time.Second); !ok {
		t.Fatal("放行后没收到 session/load 响应")
	}
}

// ---------------------------------------------------------------------------
// T3-6：回放里的工具参数经脱敏
// ---------------------------------------------------------------------------

// TestACP_LoadSession_ToolCallRawInputRedacted（T3-6 钉子）：带工具调用的 load →
// 回放出的 tool_call 的 rawInput 里**凭据必须打码**（H18：历史里的工具参数同样可能
// 含凭据——与 live 桥同口径）。
//
// 病根（旧实现）：load 回放根本没接 redactJSON → 历史里的 api_key / token 原样外发。
func TestACP_LoadSession_ToolCallRawInputRedacted(t *testing.T) {
	backend := newFakeBackend(t)
	sid, err := backend.CreateSession(t.TempDir())
	if err != nil {
		t.Fatalf("造既有会话: %v", err)
	}
	backend.publishUserPrompt(sid, "读一下配置")
	backend.publishModelTurn(sid, "好的，我来读")
	// 工具调用：参数里带凭据（键名敏感 + 值形状敏感两种）。
	backend.publishToolIntent(sid, contracts.ProviderId("toolu_hist_1"), "read",
		json.RawMessage(`{"path":"/etc/app.conf","api_key":"zzzqqqxxxwwwvvv111","note":"AKIAIOSFODNN7EXAMPLE"}`))

	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	idLoad := client.sendRequest("session/load", map[string]any{"sessionId": string(sid)})
	if _, ok := client.waitForResult(idLoad, 3*time.Second); !ok {
		t.Fatal("没收到 session/load 响应")
	}

	// 回放里应有一条 tool_call（assistant 的 ToolCalls → tool_call）。
	toolCalls := client.findUpdates("tool_call")
	if len(toolCalls) != 1 {
		t.Fatalf("回放应有 1 条 tool_call，得到 %d 条", len(toolCalls))
	}
	raw := string(toolCalls[0].RawInput)
	if raw == "" {
		t.Fatal("tool_call 的 rawInput 为空（参数没回放？）")
	}
	for _, secret := range []string{"zzzqqqxxxwwwvvv111", "AKIAIOSFODNN7EXAMPLE"} {
		if strings.Contains(raw, secret) {
			t.Errorf("回放的 rawInput 泄漏凭据 %q：%s", secret, raw)
		}
	}
	// 非敏感参数应保留（审批人/客户端要看得出在干什么）。
	if !strings.Contains(raw, "/etc/app.conf") {
		t.Errorf("非敏感参数应保留：%s", raw)
	}
}

// TestACP_LoadSession_ReplayToolResultIsCompleted（T3-7 口径钉子）：回放里的工具结果
// **统一按 completed** 发（runtime.FoldConversation 不区分 completed/failed 的既有取舍），
// 且带 tool_call_id（客户端据此把初报与结果归到同一次调用）。
func TestACP_LoadSession_ReplayToolResultIsCompleted(t *testing.T) {
	backend := newFakeBackend(t)
	sid, err := backend.CreateSession(t.TempDir())
	if err != nil {
		t.Fatalf("造既有会话: %v", err)
	}
	backend.publishUserPrompt(sid, "跑一下")
	backend.publishModelTurn(sid, "跑")
	backend.publishToolIntent(sid, contracts.ProviderId("toolu_hist_2"), "bash",
		json.RawMessage(`{"command":"echo hi"}`))
	backend.publishToolCompleted(sid, contracts.ProviderId("toolu_hist_2"), "hi")

	client, cleanup := startACP(t, backend, nil, 5*time.Second)
	defer cleanup()

	idLoad := client.sendRequest("session/load", map[string]any{"sessionId": string(sid)})
	if _, ok := client.waitForResult(idLoad, 3*time.Second); !ok {
		t.Fatal("没收到 session/load 响应")
	}

	updates := client.findUpdates("tool_call_update")
	if len(updates) != 1 {
		t.Fatalf("回放应有 1 条 tool_call_update，得到 %d 条", len(updates))
	}
	if updates[0].ToolCallID != "toolu_hist_2" {
		t.Errorf("tool_call_update 的 toolCallId = %q, want toolu_hist_2", updates[0].ToolCallID)
	}
	if updates[0].Status != "completed" {
		t.Errorf("工具结果统一按 completed 回放（fold 取舍），得到 %q", updates[0].Status)
	}
	// loop 的 kind 常量与 acp 侧的映射要对得上（防 fold/bridge 口径漂移）。
	if loop.KindToolCallCompleted != "tool.call.completed" {
		t.Errorf("loop 的 kind 常量漂移：%q", loop.KindToolCallCompleted)
	}
}
