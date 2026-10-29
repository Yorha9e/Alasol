package main

import (
	"context"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// 本文件是 chat REPL 的 /btw 命令（v3-S2 K29）的**端到端**回归：真命令路径
// （flag → host.Hub → 真事件库 → REPL 输入 → 输出）。
//
// 断言口径：**行为**，不看实现自己打印的文案。
// "主上下文零新增"这条性质由 **host 回归**（host/fork_internal_test.go 的断言①）
// 钉死；CLI 侧只钉**可观测的行为**：/btw 的问题**没有**进主 session 的事件流，
// 而落在另一个 session（旁支）里。

// btwCLIProvider 是 /btw CLI 测试用的脚本 provider：每轮回一段固定文本（无工具调用 → 单轮收敛）。
type btwCLIProvider struct{}

func (btwCLIProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	if emit != nil {
		emit(core.ProviderEvent{Kind: "done"})
	}
	return core.ModelResult{Text: "旁支答复-BTW"}, nil
}

// sessionIDRe 从 REPL 横幅里取当前 session id（[alasol] chat 就绪：session=<ulid> …）。
var sessionIDRe = regexp.MustCompile(`session=([0-9A-HJKMNP-TV-Z]{26})`)

// chatSessionID 从 REPL 输出里取当前 session id。
func chatSessionID(t *testing.T, out string) contracts.Ulid {
	t.Helper()
	m := sessionIDRe.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("横幅里没找到 session id:\n%s", out)
	}
	return contracts.Ulid(m[1])
}

// eventText 取事件的 payload 文本（断言用；坏载荷原样返回）。
func eventText(ev contracts.Event) string { return string(ev.Payload) }

// TestChatREPLBtwCommand：chat REPL 的 /btw —— 结果只给用户（btw> 前缀），且
// **主 session 的事件流没有多出任何东西**（/btw 的问题不回灌主上下文）。
func TestChatREPLBtwCommand(t *testing.T) {
	prev := testProviderOverride
	testProviderOverride = btwCLIProvider{}
	defer func() { testProviderOverride = prev }()

	dbPath := filepath.Join(t.TempDir(), "btw.db")
	restoreIn := swapStdin(t, "主对话\n/btw 旁支询问\n/quit\n")
	defer restoreIn()
	outStdout := captureStdout(t)
	outStderr := captureStderr(t)

	code := chatMain([]string{
		"-db", dbPath,
		"-model", "scripted",
		"-system", "cli-system",
	})
	got := outStdout()
	_ = outStderr()

	if code != 0 {
		t.Fatalf("chatMain 退出码 = %d，want 0", code)
	}
	// ① 主对话照常打印（alasol> 前缀）。
	if !strings.Contains(got, "alasol> 旁支答复-BTW") {
		t.Fatalf("主对话输出缺失:\n%s", got)
	}
	// ② /btw 结果只给用户展示（btw> 前缀，与主对话的 alasol> 区分）。
	if !strings.Contains(got, "btw> 旁支答复-BTW") {
		t.Fatalf("/btw 输出缺失（btw> 前缀）:\n%s", got)
	}

	// ③ **行为断言**：主 session 的事件流里没有 /btw 的问题（不回灌主上下文），
	//    且那条问题落在**另一个** session（旁支）里。
	sid := chatSessionID(t, got)
	var mainEvents []contracts.Event
	otherWithQuestion := 0
	otherWithForked := 0
	for _, ev := range readAllEvents(t, dbPath) {
		if ev.SessionID == sid {
			mainEvents = append(mainEvents, ev)
			continue
		}
		if strings.Contains(eventText(ev), "旁支询问") {
			otherWithQuestion++
		}
		if ev.Kind == "btw.forked" {
			otherWithForked++
		}
	}
	// 主 session 只有**一条** user.prompt（就是那句主对话）——/btw 没往里塞第二条。
	prompts := 0
	for _, ev := range mainEvents {
		if ev.Kind == "user.prompt" {
			prompts++
		}
		if strings.Contains(eventText(ev), "旁支询问") {
			t.Fatalf("主 session 的事件里出现了 /btw 的问题（不该回灌主上下文）: %s", eventText(ev))
		}
	}
	if prompts != 1 {
		t.Fatalf("主 session 的 user.prompt = %d 条，want 1（/btw 不该回灌主上下文）", prompts)
	}
	// 旁支那条问题在**别的** session 里（归属可辨）。
	if otherWithQuestion == 0 {
		t.Fatal("/btw 的问题没落在旁支 session 里")
	}
	if otherWithForked == 0 {
		t.Fatal("旁支 session 该有 btw.forked 标记事件")
	}
}

// TestChatREPLBtwUsageHint：/btw 不带问题 → 打印用法提示、不跑旁支、不崩。
func TestChatREPLBtwUsageHint(t *testing.T) {
	prev := testProviderOverride
	testProviderOverride = btwCLIProvider{}
	defer func() { testProviderOverride = prev }()

	restoreIn := swapStdin(t, "/btw\n/quit\n")
	defer restoreIn()
	outStdout := captureStdout(t)

	code := chatMain([]string{
		"-db", filepath.Join(t.TempDir(), "btw-usage.db"),
		"-model", "scripted",
	})
	got := outStdout()
	if code != 0 {
		t.Fatalf("chatMain 退出码 = %d，want 0", code)
	}
	if !strings.Contains(got, "用法：/btw") {
		t.Fatalf("/btw 空问题应打印用法提示:\n%s", got)
	}
	// 空问题不跑旁支 → 不该有 btw> 输出行。
	if strings.Contains(got, "btw> ") {
		t.Fatalf("空 /btw 不该产生旁支输出:\n%s", got)
	}
}

// TestChatREPLBtwTabSeparator："/btw\t问题" 与 "/btw 问题" **等价**（tab 也算分隔符，
// 与挂账 e 的"tab 分隔变体也要认"同款口径）。
func TestChatREPLBtwTabSeparator(t *testing.T) {
	prev := testProviderOverride
	testProviderOverride = btwCLIProvider{}
	defer func() { testProviderOverride = prev }()

	dbPath := filepath.Join(t.TempDir(), "btw-tab.db")
	restoreIn := swapStdin(t, "主对话\n/btw\t旁支询问\n/quit\n")
	defer restoreIn()
	outStdout := captureStdout(t)

	code := chatMain([]string{
		"-db", dbPath,
		"-model", "scripted",
	})
	got := outStdout()
	if code != 0 {
		t.Fatalf("chatMain 退出码 = %d，want 0", code)
	}
	// tab 分隔也要跑旁支（结果只给用户，btw> 前缀）。
	if !strings.Contains(got, "btw> 旁支答复-BTW") {
		t.Fatalf("/btw（tab 分隔）应跑旁支:\n%s", got)
	}
	// 且问题同样落在旁支 session 里（没被当成普通 prompt 塞进主 session）。
	sid := chatSessionID(t, got)
	for _, ev := range readAllEvents(t, dbPath) {
		if ev.SessionID == sid && strings.Contains(eventText(ev), "旁支询问") {
			t.Fatalf("tab 分隔的 /btw 被当成了普通 prompt（问题进了主 session）")
		}
	}
}
