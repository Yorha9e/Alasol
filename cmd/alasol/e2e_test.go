package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// scriptedProvider 是脚本 provider：第一轮发起一次 write 工具调用，第二轮收尾。
// 用于在**真实仓库路径**（t.TempDir）里跑通一次真实工具调用（阶段 4 验收①的自动化部分；
// 真模型 API 的部分由 alasol run -approval ... 手动跑）。
type scriptedProvider struct {
	calls int
}

func (s *scriptedProvider) Run(ctx context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	s.calls++
	if s.calls == 1 {
		// 检查工具确实进了请求（R6：schema 只注入用得到的）
		found := false
		for _, t := range req.Tools {
			if t.Name == "write" {
				found = true
			}
		}
		if !found {
			return core.ModelResult{}, errScripted("write 工具未进 ModelRequest.Tools")
		}
		tc := core.ToolCallReq{
			ID:   contracts.ProviderId("toolu_SCRIPTED_01"), // 黑盒 id：不许被改写（S4）
			Name: "write",
			Args: json.RawMessage(`{"path":"hello.txt","content":"alasol e2e"}`),
		}
		emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 10, OutputTokens: 5}}, nil
	}
	emit(core.ProviderEvent{Kind: "text.delta", Text: "done"})
	emit(core.ProviderEvent{Kind: "done"})
	return core.ModelResult{Text: "写好了", Usage: core.Usage{InputTokens: 20, OutputTokens: 3}}, nil
}

type scriptedErr string

func (e scriptedErr) Error() string { return string(e) }
func errScripted(s string) error    { return scriptedErr(s) }

// TestE2EScriptedRealToolCall 阶段 4 验收①的自动化形态：
// 真 store（SQLite 文件）+ 真 bus + 真工具（local FS）+ 真 loop，脚本 provider 驱动，
// 在临时"真实仓库"里落一次文件，并验证三段事件写路径与凭据纪律。
func TestE2EScriptedRealToolCall(t *testing.T) {
	repo := t.TempDir() // "真实仓库"形态：真实文件系统路径
	dbPath := filepath.Join(t.TempDir(), "events.db")

	const secret = "sk-e2e-super-secret-0123456789"
	r, err := newRunner(runnerOpts{
		Model:    "scripted",
		DBPath:   dbPath,
		Approval: "deny-all", // 工具放行靠 -allow（source=cli），审批不参与
		Allow:    "read,write,bash",
		provider: &scriptedProvider{},
		SessionEnv: core.EnvScope{
			"MY_SECRET_API_KEY": secret, // 凭据——绝不允许出现在事件/输出里
			"PLAIN_VAR":         "ok",
		},
	})
	if err != nil {
		t.Fatalf("装配失败: %v", err)
	}
	defer r.Close()

	// 在"仓库"里执行：write 工具写相对路径 → 改 cwd 到 repo
	wd, _ := os.Getwd()
	if err := os.Chdir(repo); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(wd)

	res, err := r.Run(context.Background(), "写一个 hello.txt")
	if err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	if res.Text != "写好了" {
		t.Fatalf("最终文本 = %q", res.Text)
	}
	if res.ToolCalls != 1 {
		t.Fatalf("ToolCalls = %d, want 1", res.ToolCalls)
	}

	// 验收①：真实仓库里跑通一次工具调用
	data, err := os.ReadFile(filepath.Join(repo, "hello.txt"))
	if err != nil {
		t.Fatalf("工具未落盘: %v", err)
	}
	if string(data) != "alasol e2e" {
		t.Fatalf("落盘内容 = %q", data)
	}

	// 验收③：事件流含完整 intent→result 三段
	rows := readAllEvents(t, dbPath)
	var intent, completed *contracts.Event
	for i := range rows {
		ev := &rows[i]
		if ev.Kind == "tool.call.intent" {
			intent = ev
		}
		if ev.Kind == "tool.call.completed" {
			completed = ev
		}
	}
	if intent == nil || completed == nil {
		t.Fatalf("三段不完整：intent=%v completed=%v（事件=%d）", intent != nil, completed != nil, len(rows))
	}
	if intent.Seq == nil || completed.Seq == nil || *completed.Seq <= *intent.Seq {
		t.Fatalf("事件序错误：intent seq=%v completed seq=%v", intent.Seq, completed.Seq)
	}
	// provider id 黑盒原样（S4/陷阱 1）：三段 tool_call_id 一致且等于脚本值
	var ip, cp struct {
		ToolCallID string `json:"tool_call_id"`
	}
	mustUnmarshal(t, intent.Payload, &ip)
	mustUnmarshal(t, completed.Payload, &cp)
	if ip.ToolCallID != "toolu_SCRIPTED_01" || cp.ToolCallID != "toolu_SCRIPTED_01" {
		t.Fatalf("tool_call_id 被改写: intent=%q completed=%q", ip.ToolCallID, cp.ToolCallID)
	}

	// 验收②：凭据永不进事件（H12/H19）——整库扫一遍
	for _, ev := range rows {
		blob, _ := json.Marshal(ev)
		if strings.Contains(string(blob), secret) {
			t.Fatalf("凭据泄漏进事件 %s: %s", ev.Kind, blob)
		}
	}
}

func readAllEvents(t *testing.T, dbPath string) []contracts.Event {
	t.Helper()
	// 直接重开 store 读全部事实事件（校验的就是落盘结果）
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	evs, err := st.ReadFrom(context.Background(), nil, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	return evs
}

func mustUnmarshal(t *testing.T, raw json.RawMessage, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("payload 解析: %v", err)
	}
}
