package tui

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
	tea "github.com/charmbracelet/bubbletea"
)

// stubProvider 是不调工具的脚本 provider（TUI 对话测试用）。
type stubProvider struct{}

func (stubProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	emit(core.ProviderEvent{Kind: "text.delta", Text: "hi"})
	emit(core.ProviderEvent{Kind: "done"})
	return core.ModelResult{Text: "hi from tui", Usage: core.Usage{InputTokens: 5, OutputTokens: 2}}, nil
}

// keyRunes 造一个可打印字符按键。
func keyRunes(s string) tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
}

// TestTabSwitching：F1/F2/F3 切面。
func TestTabSwitching(t *testing.T) {
	m := newModel(Options{Approver: NewApprover()})
	if m.activeTab != tabChat {
		t.Fatalf("初始应在对话面")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyF2})
	if m.activeTab != tabApprovals {
		t.Fatalf("F2 应切到审批面")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyF3})
	if m.activeTab != tabOnlineTable {
		t.Fatalf("F3 应切到在线表面")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyF1})
	if m.activeTab != tabChat {
		t.Fatalf("F1 应切回对话面")
	}
}

// TestChatInputAccumulate：对话面输入累积 + 退格。
func TestChatInputAccumulate(t *testing.T) {
	m := newModel(Options{Approver: NewApprover()})
	m.Update(keyRunes("he"))
	m.Update(keyRunes("llo"))
	if m.input != "hello" {
		t.Fatalf("input = %q，want hello", m.input)
	}
	m.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	if m.input != "hell" {
		t.Fatalf("退格后 input = %q，want hell", m.input)
	}
}

// TestChatSubmitRunsPrompt：对话面 Enter → running + 跑 prompt → 响应进 log。
func TestChatSubmitRunsPrompt(t *testing.T) {
	h, err := host.New(host.Config{
		DBPath:   filepath.Join(t.TempDir(), "tui.db"),
		Model:    "stub",
		Provider: stubProvider{},
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	m := newModel(Options{Hub: h, Session: host.NewSession(), Approver: NewApprover()})
	m.Update(keyRunes("你好"))
	if m.input != "你好" {
		t.Fatalf("input = %q", m.input)
	}
	// Enter → running + 返回跑 prompt 的 Cmd。
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if !m.running {
		t.Fatal("Enter 后应 running=true")
	}
	if cmd == nil {
		t.Fatal("Enter 应返回跑 prompt 的 Cmd")
	}
	if m.input != "" {
		t.Fatalf("提交后 input 应清空，得到 %q", m.input)
	}
	// 跑 Cmd → chatDoneMsg；喂回 Update → running 复位 + 响应进 log。
	done := cmd()
	m.Update(done)
	if m.running {
		t.Fatal("跑完应 running=false")
	}
	if !strings.Contains(strings.Join(m.chatLog, "\n"), "hi from tui") {
		t.Fatalf("log 里应有模型响应: %v", m.chatLog)
	}
}

// TestApprovalRequestRendersAndDecides：审批请求进 UI + y/n 决策移除并 resolve。
func TestApprovalRequestRendersAndDecides(t *testing.T) {
	appr := NewApprover()
	m := newModel(Options{Approver: appr})

	id := contracts.Ulid("01M60WTV5JWQSX95814RZ291AM")
	m.Update(approvalRequestMsg{id: id, summary: "bash: rm -rf /tmp/x", tool: "toolu_1"})
	if len(m.pending) != 1 {
		t.Fatalf("应有 1 个待审批，得到 %d", len(m.pending))
	}
	if m.activeTab != tabApprovals {
		t.Fatal("来人审应自动切到审批面")
	}
	// View 应含 summary。
	if !strings.Contains(m.View(), "rm -rf /tmp/x") {
		t.Fatalf("审批面应渲染 summary:\n%s", m.View())
	}
	// 先让 Approve 阻塞登记这个 id（模拟 loop 正在等）。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	respCh := make(chan contracts.ApprovalResponse, 1)
	go func() {
		resp, _ := appr.Approve(ctx, contracts.ApprovalRequest{ID: id, Summary: "bash"})
		respCh <- resp
	}()
	// 等 Approve 登记 pending。
	waitPending(t, appr, id)

	// UI 按 n（拒绝）→ 移除待办 + resolve → Approve 返回 deny。
	m.Update(keyRunes("n"))
	if len(m.pending) != 0 {
		t.Fatalf("决策后待办应清空，得到 %d", len(m.pending))
	}
	select {
	case resp := <-respCh:
		if resp.Decision != contracts.ApprovalDecisionDeny {
			t.Fatalf("n 应 deny，得到 %q", resp.Decision)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Approve 未在决策后返回")
	}
}

// TestApproverFailCloseOnCancel：ctx 取消 → deny（fail-close，D14/H8/陷阱9）。
func TestApproverFailCloseOnCancel(t *testing.T) {
	appr := NewApprover() // program=nil：不 Send，只测阻塞/决策/取消逻辑
	ctx, cancel := context.WithCancel(context.Background())
	req := contracts.ApprovalRequest{ID: contracts.Ulid("01M60WTV5JWQSX95814RZ291AM"), Summary: "x"}
	respCh := make(chan contracts.ApprovalResponse, 1)
	go func() {
		resp, err := appr.Approve(ctx, req)
		if err != nil {
			t.Errorf("Approve 不应返回 error（取消也回 deny 响应）: %v", err)
		}
		respCh <- resp
	}()
	waitPending(t, appr, req.ID)
	cancel()
	select {
	case resp := <-respCh:
		if resp.Decision != contracts.ApprovalDecisionDeny {
			t.Fatalf("取消应 fail-close deny，得到 %q", resp.Decision)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("取消后 Approve 未返回")
	}
}

// TestApproverAllowRoundtrip：Approve 阻塞 → resolve(true) → allow 返回。
func TestApproverAllowRoundtrip(t *testing.T) {
	appr := NewApprover()
	req := contracts.ApprovalRequest{ID: contracts.Ulid("01M60WTV5JWQSX95814RZ291AM"), Summary: "x"}
	respCh := make(chan contracts.ApprovalResponse, 1)
	go func() { resp, _ := appr.Approve(context.Background(), req); respCh <- resp }()
	waitPending(t, appr, req.ID)
	appr.resolve(req.ID, true)
	select {
	case resp := <-respCh:
		if resp.Decision != contracts.ApprovalDecisionAllow {
			t.Fatalf("resolve(true) 应 allow，得到 %q", resp.Decision)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Approve 未在 resolve 后返回")
	}
}

// TestViewNonEmptyAllTabs：三面 View 都非空、含面名。
func TestViewNonEmptyAllTabs(t *testing.T) {
	m := newModel(Options{Approver: NewApprover()})
	for _, tc := range []struct {
		tab  tab
		want string
	}{
		{tabChat, "对话"},
		{tabApprovals, "审批"},
		{tabOnlineTable, "在线表"},
	} {
		m.activeTab = tc.tab
		v := m.View()
		if strings.TrimSpace(v) == "" {
			t.Fatalf("%s 面 View 不应为空", tc.want)
		}
		if !strings.Contains(v, tc.want) {
			t.Fatalf("%s 面 View 应含面名:\n%s", tc.want, v)
		}
	}
}

// waitPending 轮询等 Approve 把 id 登记进 approver.pending。
func waitPending(t *testing.T, a *Approver, id contracts.Ulid) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		_, ok := a.pending[id]
		a.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("Approve 未登记 %s", id)
}
