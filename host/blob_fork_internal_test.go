package host

// F1 的钉子之一：/btw 旁支（fork session）的 loop **同样挂 blob**——旁支的大 result 也外置。
//
// 为什么必须钉：fork 的 loop 是**另起的一条 Loop**（host/fork.go 的 RunBtw），不是主 session
// 那条（loopFor）。只给 loopFor 挂 blob → 旁支的工具大输出**原样留在 fork session 的事件流里**
// ——"大 payload 不进事件流"这条规则就只在主 session 上成立（与子 agent 那一类漏传同款：
// 能力漂移）。旁支事件同样要被回放/转发/索引（F12），大输出留在里面照样拖死那三件事。
//
// 形态：internal（package host）——要能直接 ReplaySession(h.store, forkSessionID) 断言
// **fork session 自己的**事件流（黑箱面没有"按 session 取事件"的公开 API）。

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime"
)

// forkBlobProvider 是脚本 provider：**第 toolAt 次**调用发起一次 read 工具调用
// （读大文件 → 大 result），其余轮次只回文本。
//
// 为什么用"第 N 次才发工具调用"：主 session 与旁支**共用同一个 provider 实例**——
// 若每次都发工具调用，fork 的 loop 会一直调工具直到 MaxTurns（噪声大）；
// 若每次都只回文本，旁支就没有工具调用可断言。让它**只在旁支那一轮**发一次最干净。
type forkBlobProvider struct {
	args   string
	toolAt int // 第几次调用发起工具调用（1-based）
	calls  int
}

func (p *forkBlobProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if p.calls == p.toolAt {
		tc := core.ToolCallReq{
			ID:   contracts.ProviderId("toolu_FORK_BLOB_01"),
			Name: "read",
			Args: json.RawMessage(p.args),
		}
		if emit != nil {
			emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
			emit(core.ProviderEvent{Kind: "done"})
		}
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 1, OutputTokens: 1}}, nil
	}
	if emit != nil {
		emit(core.ProviderEvent{Kind: "text.delta", Text: "答复"})
		emit(core.ProviderEvent{Kind: "done"})
	}
	return core.ModelResult{Text: "答复", Usage: core.Usage{InputTokens: 1, OutputTokens: 1}}, nil
}

// TestRunBtwBlobExternalized（F1 的钉子之一）：旁支 session 的大 result **同样外置**
// （completed 的 BinaryRef 非空 + blob 里有全文）。
func TestRunBtwBlobExternalized(t *testing.T) {
	ctx := context.Background()

	// 大文件：read 的结果 ~9.6KB（> 阈值 1024 → 必外置；< read 的 64KB 上限 → 不被工具截断）。
	target := filepath.Join(t.TempDir(), "big.txt")
	content := strings.Repeat("ALASOL-FORK-BLOB-", 600)
	if err := os.WriteFile(target, []byte(content), 0o600); err != nil {
		t.Fatalf("写大文件: %v", err)
	}

	prov := &forkBlobProvider{
		args:   `{"path":"` + filepath.ToSlash(target) + `"}`,
		toolAt: 2, // 第 1 次 = 主 session（纯文本）；第 2 次 = 旁支（工具调用）
	}
	h, err := New(Config{
		DBPath:   filepath.Join(t.TempDir(), "events.db"),
		Model:    "fork-blob",
		System:   "fork-system",
		MaxTurns: 4,
		Allow:    []string{"read"},
		Provider: prov,
		CWD:      t.TempDir(), // 非 git 目录 → 无自动 gate（确定性）
		// D4：开 blob + 小阈值（旁支的大 result 必外置）。
		BlobDir:       filepath.Join(t.TempDir(), "blobs"),
		BlobMaxInline: 1024,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	sid := NewSession()
	// 主 session 跑一轮（纯文本，给 fold 造出可折叠的对话历史）。
	if _, err := h.RunPrompt(ctx, sid, "主问题"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	res, err := h.RunBtw(ctx, sid, "旁支问题")
	if err != nil {
		t.Fatalf("RunBtw: %v", err)
	}
	if res.ForkSessionID == "" || res.ForkSessionID == sid {
		t.Fatalf("旁支 session id 不对：%q（主 %q）", res.ForkSessionID, sid)
	}

	// 旁支 session 自己的事件流：找到那条 tool.call.completed。
	evs, err := runtime.ReplaySession(ctx, h.store, res.ForkSessionID)
	if err != nil {
		t.Fatalf("replay 旁支事件流: %v", err)
	}
	var done *contracts.Event
	for i := range evs {
		if evs[i].Kind == "tool.call.completed" {
			done = &evs[i]
			break
		}
	}
	if done == nil {
		t.Fatalf("旁支没有 tool.call.completed 事件（旁支的工具调用没跑？）事件=%d", len(evs))
	}
	if done.BinaryRef == nil {
		t.Fatal("旁支的大 result 应外置：completed.BinaryRef 不应为空（fork loop 没挂 blob？）")
	}
	// blob 里有**完整**原文（读侧按 BinaryRef 取回全文）。
	rc, err := h.blob.Get(ctx, *done.BinaryRef)
	if err != nil {
		t.Fatalf("读 blob: %v", err)
	}
	defer rc.Close()
	buf := make([]byte, 0, 64<<10)
	tmp := make([]byte, 32<<10)
	for {
		n, rerr := rc.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if rerr != nil {
			break
		}
	}
	var full contracts.ToolResult
	if err := json.Unmarshal(buf, &full); err != nil {
		t.Fatalf("blob 内容不是合法 ToolResult JSON: %v", err)
	}
	if len(full.Content) != 1 || full.Content[0].Text == nil || *full.Content[0].Text != content {
		t.Fatalf("blob 里应是完整原文（%d 字节），got %d 字节", len(content), len(buf))
	}
}
