package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core"
)

// usageScriptedProvider 是带 usage 的脚本 provider：一轮直接回文本 + usage（不调工具）。
type usageScriptedProvider struct {
	in, out int64
}

func (p *usageScriptedProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	emit(core.ProviderEvent{Kind: "text.delta", Text: "算完了"})
	emit(core.ProviderEvent{Kind: "done"})
	return core.ModelResult{
		Text:  "算完了",
		Usage: core.Usage{InputTokens: p.in, OutputTokens: p.out},
	}, nil
}

// TestChatREPLCoshCommand 验证 chat 的 /cost：喂带 usage 的 model.turn → CLI 打印聚合。
func TestChatREPLCoshCommand(t *testing.T) {
	prev := testProviderOverride
	testProviderOverride = &usageScriptedProvider{in: 128, out: 24}
	defer func() { testProviderOverride = prev }()

	restoreIn := swapStdin(t, "总结一下\n/cost\n/quit\n")
	defer restoreIn()
	out := captureStdout(t)

	code := chatMain([]string{
		"-db", filepath.Join(t.TempDir(), "cost-cli.db"),
		"-model", "scripted-usage",
		"-max-turns", "2",
	})
	got := out()
	if code != 0 {
		t.Fatalf("chatMain 退出码 = %d，want 0", code)
	}
	// /cost 应打印 input=128 output=24 total=152 + 按模型分组（model=scripted-usage）。
	for _, want := range []string{"input=128", "output=24", "total=152", "scripted-usage"} {
		if !strings.Contains(got, want) {
			t.Fatalf("/cost 输出缺 %q:\n%s", want, got)
		}
	}
}

// TestChatREPLCoshEmpty：没跑对话时 /cost 不崩（空态提示）。
func TestChatREPLCoshEmpty(t *testing.T) {
	restoreIn := swapStdin(t, "/cost\n/quit\n")
	defer restoreIn()
	out := captureStdout(t)

	code := chatMain([]string{
		"-db", filepath.Join(t.TempDir(), "cost-empty.db"),
		"-model", "scripted-usage",
	})
	got := out()
	if code != 0 {
		t.Fatalf("退出码 = %d", code)
	}
	if !strings.Contains(got, "还没有带 usage") {
		t.Fatalf("空态 /cost 提示缺失:\n%s", got)
	}
}
