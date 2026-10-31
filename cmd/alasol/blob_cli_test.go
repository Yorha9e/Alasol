package main

// F1 的钉子之三：`alasol run` 这条 CLI 路径同样接上 blob（D4 大 payload 外置）。
//
// 为什么必须钉：CLI 走的是**另一套装配**（cmd/alasol/wire.go 的 newRunner → loop.Config），
// 不经 host.New/host.loopFor——只给 hub 那条路径挂上 blob，CLI 跑的工具大输出照样原样进
// 事件流（"大 payload 不进事件流"在 CLI 路径上失效）。run 是单次 CLI、无 HTTP 读面，
// 但**外置本身**必须一致（落盘形状一致：摘要 + BinaryRef）。

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/blobfs"
)

// blobCLIScriptedProvider 是脚本 provider：第一轮发起一次 read 工具调用（读大文件 →
// 大 result），第二轮收尾。
type blobCLIScriptedProvider struct {
	args  string
	calls int
}

func (p *blobCLIScriptedProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if p.calls == 1 {
		tc := core.ToolCallReq{
			ID:   contracts.ProviderId("toolu_SCRIPTED_01"),
			Name: "read",
			Args: json.RawMessage(p.args),
		}
		emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 1, OutputTokens: 1}}, nil
	}
	emit(core.ProviderEvent{Kind: "text.delta", Text: "读完了"})
	emit(core.ProviderEvent{Kind: "done"})
	return core.ModelResult{Text: "读完了", Usage: core.Usage{InputTokens: 1, OutputTokens: 1}}, nil
}

// writeBigCLIFile 写一个 ~9.6KB 的文件（read 的结果 < 64KB 上限、不被工具截断；> 阈值 → 必外置）。
func writeBigCLIFile(t *testing.T) (string, string) {
	t.Helper()
	target := filepath.Join(t.TempDir(), "big.txt")
	content := strings.Repeat("ALASOL-CLI-BLOB-", 600)
	if err := os.WriteFile(target, []byte(content), 0o600); err != nil {
		t.Fatalf("写大文件: %v", err)
	}
	return target, content
}

// completedEventOf 取事件流里第一条 tool.call.completed（没有则失败）。
func completedEventOf(t *testing.T, rows []contracts.Event) contracts.Event {
	t.Helper()
	for _, ev := range rows {
		if ev.Kind == "tool.call.completed" {
			return ev
		}
	}
	t.Fatalf("事件流里没有 tool.call.completed（共 %d 条）", len(rows))
	return contracts.Event{}
}

// TestRunCLIBlobExternalized（F1 的钉子之三）：`alasol run` 路径：① 配了 blob → 大 result
// 外置（BinaryRef 非空 + blob 里有全文）② 没配 blob（默认关）→ BinaryRef 空（零行为变化）。
func TestRunCLIBlobExternalized(t *testing.T) {
	target, content := writeBigCLIFile(t)
	args := `{"path":"` + filepath.ToSlash(target) + `"}`

	t.Run("启用：外置", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "events.db")
		blobDir := filepath.Join(t.TempDir(), "blobs")
		r, err := newRunner(runnerOpts{
			Model:         "scripted",
			DBPath:        dbPath,
			Approval:      "deny-all", // 放行靠 Allow（source=cli）
			Allow:         "read",
			provider:      &blobCLIScriptedProvider{args: args},
			BlobDir:       blobDir,
			BlobMaxInline: 1024, // 小阈值 → 9.6KB 的 read 结果必外置
		})
		if err != nil {
			t.Fatalf("装配失败: %v", err)
		}
		defer r.Close()

		if _, err := r.Run(context.Background(), "读大文件"); err != nil {
			t.Fatalf("Run: %v", err)
		}

		done := completedEventOf(t, readAllEvents(t, dbPath))
		if done.BinaryRef == nil {
			t.Fatal("run CLI 的大 result 应外置：completed.BinaryRef 不应为空（runner 的 loop 没挂 BlobSink？）")
		}
		// blob 里有**完整**原文（读侧按 BinaryRef 取回全文）。
		bs, err := blobfs.New(blobDir)
		if err != nil {
			t.Fatalf("blobfs.New: %v", err)
		}
		rc, err := bs.Get(context.Background(), *done.BinaryRef)
		if err != nil {
			t.Fatalf("读 blob: %v", err)
		}
		defer rc.Close()
		raw, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("读 blob 内容: %v", err)
		}
		var full contracts.ToolResult
		if err := json.Unmarshal(raw, &full); err != nil {
			t.Fatalf("blob 内容不是合法 ToolResult JSON: %v", err)
		}
		if len(full.Content) != 1 || full.Content[0].Text == nil || *full.Content[0].Text != content {
			t.Fatalf("blob 里应是完整原文（%d 字节），got %d 字节", len(content), len(raw))
		}
		// payload 是**摘要形态**（占位 + 预览），不是原文。
		var p contracts.ToolCallCompletedPayload
		if err := json.Unmarshal(done.Payload, &p); err != nil {
			t.Fatalf("completed payload 不可解: %v", err)
		}
		if len(p.Result.Content) != 1 || p.Result.Content[0].Text == nil {
			t.Fatalf("摘要应保留一个 text part: %+v", p.Result.Content)
		}
		if !strings.Contains(*p.Result.Content[0].Text, "[已外置：全文见 blob ") {
			t.Fatalf("摘要应含外置占位标记，got %q", *p.Result.Content[0].Text)
		}
	})

	t.Run("默认关：不外置", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "events.db")
		r, err := newRunner(runnerOpts{
			Model:    "scripted",
			DBPath:   dbPath,
			Approval: "deny-all",
			Allow:    "read",
			provider: &blobCLIScriptedProvider{args: args},
			// 不配 BlobDir / BlobMaxInline → 关 = 零行为变化。
		})
		if err != nil {
			t.Fatalf("装配失败: %v", err)
		}
		defer r.Close()

		if _, err := r.Run(context.Background(), "读大文件"); err != nil {
			t.Fatalf("Run: %v", err)
		}
		done := completedEventOf(t, readAllEvents(t, dbPath))
		if done.BinaryRef != nil {
			t.Fatalf("未配 blob 时 BinaryRef 应为空，got %v", *done.BinaryRef)
		}
	})
}
