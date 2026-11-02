package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/acp"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// cmd 层集成钉子：**真 hub + 脚本 provider** 装配（fake provider 装配口径），经
// hubBackend 适配层驱动 acp.Serve——验证真 loop 的 model.delta 经 bus 桥成 ACP 的
// agent_message_chunk，全链路 initialize→new→prompt→update→end_turn 跑通。
// （runtime/acp 的用例用假 backend；这里补"真 hub 装配"这一层，两者互补。）

// acpScriptProvider 是脚本 provider：发一条正文增量后收敛（无工具调用，1 轮结束）。
type acpScriptProvider struct{}

func (acpScriptProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	emit(core.ProviderEvent{Kind: core.KindTextDelta, Text: "真-hub-回复"})
	emit(core.ProviderEvent{Kind: core.KindDone})
	return core.ModelResult{Text: "真-hub-回复"}, nil
}

// acpAskProvider 第一轮发起一次 read 工具调用（命中 ask 策略 → 走审批），第二轮收尾。
type acpAskProvider struct{ calls int }

func (p *acpAskProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if p.calls == 1 {
		tc := core.ToolCallReq{ID: contracts.ProviderId("toolu_ask"), Name: "read", Args: json.RawMessage(`{"path":"/tmp/x"}`)}
		emit(core.ProviderEvent{Kind: core.KindToolCallStart, ToolCall: &tc})
		emit(core.ProviderEvent{Kind: core.KindDone})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}}, nil
	}
	emit(core.ProviderEvent{Kind: core.KindTextDelta, Text: "读完了"})
	emit(core.ProviderEvent{Kind: core.KindDone})
	return core.ModelResult{Text: "读完了"}, nil
}

// TestACP_RealHub_ApprovalBridge 全链路 ask 闭环：真 hub + ask 策略 → policy.Decide 命中
// → acp.Approver 发 session/request_permission → client 回 allow → 工具执行 → 续跑到
// end_turn。验证 cfg.Approver=acpApprover 这条装配线真能把审批桥到 ACP 面。
func TestACP_RealHub_ApprovalBridge(t *testing.T) {
	cfg := host.Config{
		DBPath:   filepath.Join(t.TempDir(), "events.db"),
		Model:    "scripted",
		MaxTurns: 4,
		Ask:      []string{"read"}, // read 命中 ask → 走审批器（非 allow 直连）
		CWD:      t.TempDir(),
		Provider: &acpAskProvider{},
		Approver: acp.NewApprover(),
	}
	h, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	out := &cmdLineSink{ch: make(chan struct{}, 1)}
	pr, pw := io.Pipe()
	serveErr := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		serveErr <- acp.Serve(ctx, acp.Options{
			Backend:     &hubBackend{h: h},
			Approver:    cfg.Approver.(*acp.Approver),
			In:          pr,
			Out:         out,
			TurnTimeout: 15 * time.Second,
		})
	}()

	send := func(id int, method string, params any) {
		o := map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}
		raw, _ := json.Marshal(o)
		if _, err := pw.Write(append(raw, '\n')); err != nil {
			t.Fatalf("写请求: %v", err)
		}
	}
	waitResult := func(id int, timeout time.Duration) map[string]any {
		want := jsonInt(id)
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			for _, f := range out.frames() {
				if f.isResponse() && f.Result != nil && f.ID != nil && string(*f.ID) == want {
					var m map[string]any
					json.Unmarshal(*f.Result, &m)
					return m
				}
			}
			out.waitCount(len(out.snapshot())+1, 50*time.Millisecond)
		}
		return nil
	}

	send(1, "initialize", map[string]any{"protocolVersion": 1})
	if waitResult(1, 3*time.Second) == nil {
		t.Fatal("没收到 initialize 响应")
	}
	send(2, "session/new", map[string]any{"cwd": t.TempDir()})
	ns := waitResult(2, 3*time.Second)
	if ns == nil {
		t.Fatal("没收到 session/new 响应")
	}
	sid, _ := ns["sessionId"].(string)

	send(3, "session/prompt", map[string]any{"sessionId": sid, "prompt": "读一下"})

	// 等 ask 审批请求出现（agent→client 的 session/request_permission），回 allow。
	var permID *json.RawMessage
	deadline := time.Now().Add(10 * time.Second)
	for {
		for _, f := range out.frames() {
			if f.Method != nil && *f.Method == "session/request_permission" && f.ID != nil {
				permID = f.ID
			}
		}
		if permID != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("没收到 session/request_permission（ask 没桥到 ACP 面？）")
		}
		out.waitCount(len(out.snapshot())+1, 50*time.Millisecond)
	}
	// 回 allow（client→agent 响应，回指 permID）。
	respObj := map[string]any{"jsonrpc": "2.0", "id": *permID, "result": map[string]any{"outcome": "allow"}}
	raw, _ := json.Marshal(respObj)
	if _, err := pw.Write(append(raw, '\n')); err != nil {
		t.Fatalf("写审批响应: %v", err)
	}

	promptRes := waitResult(3, 15*time.Second)
	if promptRes == nil {
		t.Fatal("没收到 session/prompt 响应")
	}
	if promptRes["stopReason"] != "end_turn" {
		t.Errorf("stopReason=%v want end_turn（allow 后应续跑到收敛）", promptRes["stopReason"])
	}

	_ = pw.Close()
	cancel()
	select {
	case <-serveErr:
	case <-time.After(5 * time.Second):
		t.Error("Serve 未收场")
	}
}

func TestACP_RealHub_EndToEnd(t *testing.T) {
	cfg := host.Config{
		DBPath:   filepath.Join(t.TempDir(), "events.db"),
		Model:    "scripted",
		MaxTurns: 4,
		Allow:    []string{"read"},
		CWD:      t.TempDir(),
		Provider: acpScriptProvider{},
		Approver: acp.NewApprover(),
	}
	h, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	// 驱动 acp.Serve（In/Out 走管道；Out 用非阻塞收集器避免读写死锁）。
	out := &cmdLineSink{ch: make(chan struct{}, 1)}
	pr, pw := io.Pipe()
	serveErr := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		serveErr <- acp.Serve(ctx, acp.Options{
			Backend:     &hubBackend{h: h},
			Approver:    cfg.Approver.(*acp.Approver),
			In:          pr,
			Out:         out,
			TurnTimeout: 10 * time.Second,
		})
	}()

	// --- 客户端 ---
	send := func(id int, method string, params any) {
		o := map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}
		raw, _ := json.Marshal(o)
		if _, err := pw.Write(append(raw, '\n')); err != nil {
			t.Fatalf("写请求: %v", err)
		}
	}
	waitResult := func(id int, timeout time.Duration) map[string]any {
		want := jsonInt(id)
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			for _, f := range out.frames() {
				if f.isResponse() && f.Result != nil && f.ID != nil && string(*f.ID) == want {
					var m map[string]any
					json.Unmarshal(*f.Result, &m)
					return m
				}
			}
			out.waitCount(len(out.snapshot())+1, 50*time.Millisecond)
		}
		return nil
	}

	send(1, "initialize", map[string]any{"protocolVersion": 1})
	init := waitResult(1, 3*time.Second)
	if init == nil {
		t.Fatal("没收到 initialize 响应")
	}
	if init["protocolVersion"].(float64) != 1 {
		t.Errorf("protocolVersion=%v want 1", init["protocolVersion"])
	}

	send(2, "session/new", map[string]any{"cwd": t.TempDir()})
	ns := waitResult(2, 3*time.Second)
	if ns == nil {
		t.Fatal("没收到 session/new 响应")
	}
	sid, _ := ns["sessionId"].(string)
	if !core.ValidUlid(contracts.Ulid(sid)) {
		t.Fatalf("sessionId 非法: %q", sid)
	}

	send(3, "session/prompt", map[string]any{
		"sessionId": sid,
		"prompt":    []map[string]any{{"type": "text", "text": "说点什么"}},
	})
	promptRes := waitResult(3, 15*time.Second)
	if promptRes == nil {
		t.Fatal("没收到 session/prompt 响应")
	}
	if promptRes["stopReason"] != "end_turn" {
		t.Errorf("stopReason=%v want end_turn", promptRes["stopReason"])
	}

	// 真 loop 的 model.delta 应已桥成至少一条 agent_message_chunk。
	var msgChunks int
	for _, f := range out.frames() {
		if !f.isNotification() || f.Method == nil || *f.Method != "session/update" {
			continue
		}
		var p struct {
			Update struct {
				SessionUpdate string `json:"sessionUpdate"`
				Content       struct {
					Text string `json:"text"`
				} `json:"content"`
			} `json:"update"`
		}
		if json.Unmarshal(f.Params, &p) == nil && p.Update.SessionUpdate == "agent_message_chunk" {
			msgChunks++
			if !strings.Contains(p.Update.Content.Text, "真-hub-回复") {
				t.Errorf("chunk 文本不含模型输出: %q", p.Update.Content.Text)
			}
		}
	}
	if msgChunks == 0 {
		t.Fatal("没桥出 agent_message_chunk（真 hub 的 model.delta 没上来？）")
	}

	// stdout 纯净性：出站每一行都是合法 JSON-RPC 帧。
	for i, ln := range out.snapshot() {
		var probe struct {
			JSONRPC string `json:"jsonrpc"`
		}
		if err := json.Unmarshal([]byte(ln), &probe); err != nil || probe.JSONRPC != "2.0" {
			t.Fatalf("第 %d 行不是合法帧（日志泄漏？）: %q", i, ln)
		}
	}

	// 收场。
	_ = pw.Close()
	cancel()
	select {
	case <-serveErr:
	case <-time.After(5 * time.Second):
		t.Error("Serve 未收场")
	}
}

// acpAlwaysToolProvider 每轮都发起一次 read 工具调用（模型**不收敛**）——配 MaxTurns=1
// 即触发"轮数用尽被截断"（真截断）。
type acpAlwaysToolProvider struct{ calls int }

func (p *acpAlwaysToolProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	tc := core.ToolCallReq{
		ID:   contracts.ProviderId("toolu_always"),
		Name: "read",
		Args: json.RawMessage(`{"path":"definitely-not-here.txt"}`),
	}
	emit(core.ProviderEvent{Kind: core.KindToolCallStart, ToolCall: &tc})
	emit(core.ProviderEvent{Kind: core.KindDone})
	return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}}, nil
}

// N3 钉子（真 hub）：**MaxTurns 恰好用满**时，stopReason 要按"这轮有没有跑完"判——
//
//	单轮就收敛（模型不发工具调用）→ end_turn（**不是** max_turn_requests）
//	模型一直发工具调用（真截断）   → max_turn_requests
//
// 病根：旧实现按"实际轮数 ≥ 上限"**猜**"被截断"——正常收敛时 Turns 同样等于实际轮数
// （MaxTurns=1 单轮收敛 → Turns=1 = 上限）→ 被误报成 max_turn_requests。
// 修法：loop 用 Result.TruncatedByMaxTurns **显式**标出截断，端点按它判。
func TestACP_RealHub_MaxTurnsStopReason(t *testing.T) {
	cases := []struct {
		name     string
		provider core.Provider
		want     string
	}{
		{"单轮收敛", acpScriptProvider{}, "end_turn"},
		{"轮数用尽被截断", &acpAlwaysToolProvider{}, "max_turn_requests"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := host.Config{
				DBPath:   filepath.Join(t.TempDir(), "events.db"),
				Model:    "scripted",
				MaxTurns: 1, // 恰好用满：两种结局的 Turns 都是 1
				Allow:    []string{"read"},
				CWD:      t.TempDir(),
				Provider: c.provider,
				Approver: acp.NewApprover(),
			}
			h, err := host.New(cfg)
			if err != nil {
				t.Fatalf("host.New: %v", err)
			}
			t.Cleanup(func() { _ = h.Close() })

			out := &cmdLineSink{ch: make(chan struct{}, 1)}
			pr, pw := io.Pipe()
			serveErr := make(chan error, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				serveErr <- acp.Serve(ctx, acp.Options{
					Backend:     &hubBackend{h: h},
					Approver:    cfg.Approver.(*acp.Approver),
					In:          pr,
					Out:         out,
					TurnTimeout: 15 * time.Second,
				})
			}()

			send := func(id int, method string, params any) {
				o := map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}
				raw, _ := json.Marshal(o)
				if _, err := pw.Write(append(raw, '\n')); err != nil {
					t.Fatalf("写请求: %v", err)
				}
			}
			waitResult := func(id int, timeout time.Duration) map[string]any {
				want := jsonInt(id)
				deadline := time.Now().Add(timeout)
				for time.Now().Before(deadline) {
					for _, f := range out.frames() {
						if f.isResponse() && f.Result != nil && f.ID != nil && string(*f.ID) == want {
							var m map[string]any
							json.Unmarshal(*f.Result, &m)
							return m
						}
					}
					out.waitCount(len(out.snapshot())+1, 50*time.Millisecond)
				}
				return nil
			}

			send(1, "initialize", map[string]any{"protocolVersion": 1})
			if waitResult(1, 5*time.Second) == nil {
				t.Fatal("没收到 initialize 响应")
			}
			send(2, "session/new", map[string]any{"cwd": t.TempDir()})
			ns := waitResult(2, 5*time.Second)
			if ns == nil {
				t.Fatal("没收到 session/new 响应")
			}
			sid, _ := ns["sessionId"].(string)

			send(3, "session/prompt", map[string]any{"sessionId": sid, "prompt": "说点什么"})
			promptRes := waitResult(3, 20*time.Second)
			if promptRes == nil {
				t.Fatal("没收到 session/prompt 响应")
			}
			if got := promptRes["stopReason"]; got != c.want {
				t.Errorf("stopReason=%v want %v（MaxTurns=1：单轮收敛=end_turn，真截断=max_turn_requests）", got, c.want)
			}

			_ = pw.Close()
			cancel()
			select {
			case <-serveErr:
			case <-time.After(5 * time.Second):
				t.Error("Serve 未收场")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// F7 / F8 / F10 的回归钉子
// ---------------------------------------------------------------------------

// TestACP_RealHub_LoadSessionCapability（T3-1 钉子，cmd 层）：hubBackend 实现了
// sessionLoader → initialize 必须**宣称 loadSession=true**，且 session/load 真能载回
// 历史（事件流 → fold → session/update 回放）。
//
// 病根（修前）：生产后端 hubBackend **没实现** sessionLoader，而端点**无条件**宣称
// loadSession=true → 客户端照着声明去 session/load，每次都吃 -32000（能力缺失）。
func TestACP_RealHub_LoadSessionCapability(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "events.db")
	cfg := host.Config{
		DBPath:   dbPath,
		Model:    "scripted",
		MaxTurns: 4,
		CWD:      t.TempDir(),
		Provider: acpScriptProvider{},
	}
	h, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	// 只读回放面（与 acpMain 同款：按同一个 DB 路径另开一条只读连接）。
	loadStore, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("开只读回放面: %v", err)
	}
	t.Cleanup(func() { _ = loadStore.Close() })

	backend := &hubBackend{h: h, store: loadStore}

	// 先跑一轮，造出可回放的历史（user.prompt + model.turn 落事件流）。
	sid, err := backend.CreateSession(t.TempDir())
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := backend.RunPrompt(context.Background(), sid, "先说点什么"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	out := &cmdLineSink{ch: make(chan struct{}, 1)}
	pr, pw := io.Pipe()
	serveErr := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		serveErr <- acp.Serve(ctx, acp.Options{
			Backend: backend,
			In:      pr,
			Out:     out,
		})
	}()

	send := func(id int, method string, params any) {
		o := map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}
		raw, _ := json.Marshal(o)
		if _, err := pw.Write(append(raw, '\n')); err != nil {
			t.Fatalf("写请求: %v", err)
		}
	}
	waitResult := func(id int, timeout time.Duration) map[string]any {
		want := jsonInt(id)
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			for _, f := range out.frames() {
				if f.isResponse() && f.Result != nil && f.ID != nil && string(*f.ID) == want {
					var m map[string]any
					json.Unmarshal(*f.Result, &m)
					return m
				}
			}
			out.waitCount(len(out.snapshot())+1, 50*time.Millisecond)
		}
		return nil
	}

	// ① initialize：loadSession 必须宣称 true（后端有该能力）。
	send(1, "initialize", map[string]any{"protocolVersion": 1})
	init := waitResult(1, 5*time.Second)
	if init == nil {
		t.Fatal("没收到 initialize 响应")
	}
	caps, _ := init["agentCapabilities"].(map[string]any)
	if caps == nil {
		t.Fatalf("initialize 结果缺 agentCapabilities: %v", init)
	}
	if caps["loadSession"] != true {
		t.Errorf("hubBackend 实现了 LoadSession → loadSession 应宣称 true，得到 %v", caps["loadSession"])
	}

	// ② session/load：真能载回历史（回放成 session/update）。
	send(2, "session/load", map[string]any{"sessionId": string(sid)})
	if res := waitResult(2, 5*time.Second); res == nil {
		t.Fatal("没收到 session/load 响应")
	}
	var replayed bool
	for _, f := range out.frames() {
		if !f.isNotification() || f.Method == nil || *f.Method != "session/update" {
			continue
		}
		var p struct {
			Update struct {
				SessionUpdate string `json:"sessionUpdate"`
				Content       struct {
					Text string `json:"text"`
				} `json:"content"`
			} `json:"update"`
		}
		if json.Unmarshal(f.Params, &p) == nil && p.Update.SessionUpdate == "user_message_chunk" {
			if strings.Contains(p.Update.Content.Text, "先说点什么") {
				replayed = true
			}
		}
	}
	if !replayed {
		t.Error("session/load 没把历史回放给客户端（user_message_chunk 缺用户原话）")
	}

	_ = pw.Close()
	cancel()
	select {
	case <-serveErr:
	case <-time.After(5 * time.Second):
		t.Error("Serve 未收场")
	}
}

// F7 钉子：session/new 的 cwd 在**服务端**不存在 → 回落服务端进程 cwd（不报错）。
//
// 跨机口径（K19）：`ssh host alasol acp` 时客户端给的路径在服务端多半不存在——
// 照搬会让 session 建在一条不存在的路径上（跨机必炸）。
func TestACP_CwdFallbackToServerCwd(t *testing.T) {
	// ① 解析口径：空 / 不存在的路径都判"不可用"。
	if _, ok := resolveACPCwd(""); ok {
		t.Error("空 cwd 应判不可用（回落）")
	}
	if _, ok := resolveACPCwd(filepath.Join(t.TempDir(), "no-such-dir")); ok {
		t.Error("不存在的 cwd 应判不可用（回落）")
	}
	// 存在的目录 → 采信（绝对路径）。
	dir := t.TempDir()
	got, ok := resolveACPCwd(dir)
	if !ok {
		t.Fatal("存在的 cwd 应被采信")
	}
	if !filepath.IsAbs(got) {
		t.Errorf("采信的 cwd 应是绝对路径：%q", got)
	}

	// ② 端到端：hubBackend.CreateSession 拿一个不存在的 cwd → 不报错（回落）。
	cfg := host.Config{
		DBPath:   filepath.Join(t.TempDir(), "events.db"),
		Model:    "scripted",
		CWD:      t.TempDir(),
		Provider: acpScriptProvider{},
	}
	h, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	b := &hubBackend{h: h}
	sid, err := b.CreateSession(filepath.Join(t.TempDir(), "definitely-not-here"))
	if err != nil {
		t.Fatalf("不存在的 cwd 应回落服务端 cwd，而不是报错：%v", err)
	}
	if !core.ValidUlid(sid) {
		t.Errorf("sessionId 非法：%q", sid)
	}
}

// F8 钉子：-prompt-timeout 的秒数要真的换算成端点的整轮预算（0 = 用缺省）。
func TestACP_TurnTimeoutFromConfig(t *testing.T) {
	if got := acpTurnTimeout(host.Config{}); got != 0 {
		t.Errorf("未设时应为 0（用缺省），得到 %v", got)
	}
	if got := acpTurnTimeout(host.Config{PromptRunTimeoutSec: 42}); got != 42*time.Second {
		t.Errorf("42 秒 → %v，want 42s", got)
	}
	// 负数（配置校验会拒，但换算函数自己也要 fail-safe）：当 0（用缺省）。
	if got := acpTurnTimeout(host.Config{PromptRunTimeoutSec: -5}); got != 0 {
		t.Errorf("负数应回落缺省，得到 %v", got)
	}
}

// F10 钉子：**CLI 级** stdout 纯净性——真起一个 `alasol acp` 子进程，喂
// initialize / session/new，断言 stdout **逐行都是合法 JSON-RPC 帧**（无日志混入）。
//
// 这一层是 runtime/acp 的"注入管道"测不到的真实出口：进程级 stdout 会不会被
// banner / 日志污染，只有真起进程才知道（stdout 纯净性铁律）。
func TestACP_CLI_StdoutPurity(t *testing.T) {
	if testing.Short() {
		t.Skip("短跑模式跳过（要真起子进程）")
	}
	bin := buildACPBin(t)
	dir := t.TempDir()

	cmd := exec.Command(bin, "acp",
		"-model", "gpt-4o-mini", // 只装配 provider，不发请求（无网也行）
		"-db", filepath.Join(dir, "events.db"),
		"-cwd", dir,
		// **隔离用户配置**：`-config` 指向一个不存在的路径 → LoadFileConfig 返空
		// （os.IsNotExist → (nil, nil)），不读平台默认位（%APPDATA%\alasol\config.toml
		// / XDG）——否则这台机器上的用户配置（model/base_url/allow/ask…）会渗进这条钉子，
		// 让"子进程能不能起来"取决于开发机的家目录（不可复现）。
		"-config", filepath.Join(dir, "none.toml"),
	)
	// stdout/stderr 分开：**stdout 必须是干净的帧流**，日志都在 stderr。
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("开 stdin: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("起 alasol acp: %v", err)
	}
	// 喂两帧（initialize / session/new），然后关 stdin（→ 端点正常收场、退出码 0）。
	for _, req := range []map[string]any{
		{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{"protocolVersion": 1}},
		{"jsonrpc": "2.0", "id": 2, "method": "session/new", "params": map[string]any{"cwd": dir}},
	} {
		raw, _ := json.Marshal(req)
		if _, err := stdin.Write(append(raw, '\n')); err != nil {
			t.Fatalf("写请求: %v", err)
		}
	}
	_ = stdin.Close()

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()
	select {
	case err := <-waitErr:
		if err != nil {
			t.Fatalf("alasol acp 应正常退出（stdin EOF → 收场）：%v\nstderr:\n%s", err, stderr.String())
		}
	case <-time.After(60 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("alasol acp 没在超时内退出")
	}

	// stdout 逐行校验：每行都必须是合法 JSON-RPC 帧（jsonrpc=2.0），且无一行是日志。
	lines := strings.Split(strings.TrimRight(stdout.String(), "\n"), "\n")
	if len(lines) == 0 || (len(lines) == 1 && lines[0] == "") {
		t.Fatalf("stdout 是空的（不该）：stderr:\n%s", stderr.String())
	}
	var sawInitialize, sawNew bool
	for i, ln := range lines {
		var f cmdFrame
		if err := json.Unmarshal([]byte(ln), &f); err != nil {
			t.Fatalf("stdout 第 %d 行不是合法 JSON（日志泄漏？）: %q\nstderr:\n%s", i, ln, stderr.String())
		}
		var probe struct {
			JSONRPC string `json:"jsonrpc"`
		}
		if err := json.Unmarshal([]byte(ln), &probe); err != nil || probe.JSONRPC != "2.0" {
			t.Fatalf("stdout 第 %d 行不是 JSON-RPC 帧: %q\nstderr:\n%s", i, ln, stderr.String())
		}
		if f.Result != nil {
			var m map[string]any
			json.Unmarshal(*f.Result, &m)
			if _, ok := m["protocolVersion"]; ok {
				sawInitialize = true
			}
			if _, ok := m["sessionId"]; ok {
				sawNew = true
			}
		}
	}
	if !sawInitialize {
		t.Errorf("没收到 initialize 响应：stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if !sawNew {
		t.Errorf("没收到 session/new 响应：stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

// buildACPBin 编一个 alasol 二进制（CLI 级钉子要用真进程跑）。
//
// **不依赖 cwd**：go test 的工作目录是**包目录**（cmd/alasol），但 `go build .` 在别的
// 工作目录下编的是**别的包**——故这里显式取本包的**绝对路径**（runtime.Caller），
// 谁在哪个目录跑测试都编的是同一个二进制。
func buildACPBin(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skipf("环境里没有 go 工具链，跳过 CLI 级钉子：%v", err)
	}
	out := filepath.Join(t.TempDir(), "alasol-acp-test")
	if runtime.GOOS == "windows" {
		out += ".exe"
	}
	// 本包目录的绝对路径（runtime.Caller → 本文件所在目录 = cmd/alasol）。
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("拿不到本包目录（runtime.Caller 失败），跳过 CLI 级钉子")
	}
	pkgDir := filepath.Dir(thisFile)
	cmd := exec.Command("go", "build", "-o", out, ".")
	cmd.Dir = pkgDir // 包目录（cmd/alasol）的**绝对路径**（不依赖 cwd）
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("编 alasol 二进制失败: %v\n%s", err, b)
	}
	return out
}

// ---------------------------------------------------------------------------
// 最小客户端替身（cmd 层自足，不依赖 acp 包的测试内部）
// ---------------------------------------------------------------------------

type cmdLineSink struct {
	mu    sync.Mutex
	lines []string
	ch    chan struct{}
}

func (s *cmdLineSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	for _, ln := range bytes.Split(bytes.TrimRight(p, "\n"), []byte("\n")) {
		if len(bytes.TrimSpace(ln)) > 0 {
			s.lines = append(s.lines, string(ln))
		}
	}
	s.mu.Unlock()
	select {
	case s.ch <- struct{}{}:
	default:
	}
	return len(p), nil
}

func (s *cmdLineSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.lines...)
}

func (s *cmdLineSink) waitCount(n int, timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	for {
		cur := s.snapshot()
		if len(cur) >= n {
			return cur
		}
		rem := time.Until(deadline)
		if rem <= 0 {
			return cur
		}
		select {
		case <-s.ch:
		case <-time.After(rem):
		}
	}
}

// frames 解出站行（按 kind 判别需要 id/result/method 字段；这里只用 JSON tag 直解）。
func (s *cmdLineSink) frames() []cmdFrame {
	var out []cmdFrame
	for _, ln := range s.snapshot() {
		var f cmdFrame
		if json.Unmarshal([]byte(ln), &f) == nil {
			out = append(out, f)
		}
	}
	return out
}

type cmdFrame struct {
	ID     *json.RawMessage `json:"id"`
	Method *string          `json:"method"`
	Params json.RawMessage  `json:"params"`
	Result *json.RawMessage `json:"result"`
	Error  *json.RawMessage `json:"error"`
}

func (f cmdFrame) isResponse() bool { return f.Result != nil || f.Error != nil }
func (f cmdFrame) isNotification() bool {
	return f.Method != nil && f.ID == nil
}

func jsonInt(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}
