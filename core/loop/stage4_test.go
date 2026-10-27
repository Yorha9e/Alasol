package loop_test

// 阶段 4 收尾的三条修复的测试：
//
//   - P1-1：注入的凭据不得随工具输出流进事件与下一轮 prompt（精确 redact）；
//   - P1-2：工具参数必须是合法 JSON 对象，否则走普通失败路径（不中止 Run）；
//   - P2-3：被策略拒绝的工具不进模型可见集（H4）。

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
)

// ---------------------------------------------------------------------------
// P1-1：注入凭据的精确脱敏
// ---------------------------------------------------------------------------

// TestInjectedSecretIsRedactedFromOutputAndEvents：命令回显注入的凭据时，
// 凭据值不得出现在 completed 事件与下一轮 ModelRequest 里（只留 "***"）。
func TestInjectedSecretIsRedactedFromOutputAndEvents(t *testing.T) {
	const secret = "sk-probe-token-1234567890"
	t.Setenv("PROBE_TOKEN", secret)

	h := newHarness(t, allowRule("echo"), allowRule("env:PROBE_TOKEN"))
	exec := h.addTool("echo", func(tool *contracts.Tool) {
		tool.RequiredEnv = []string{"PROBE_TOKEN"}
	})
	// 执行器**故意回显**注入的凭据（模拟 `echo %PROBE_TOKEN%`）。
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{
		Type: "text",
		Text: strPtr("stdout: token=" + secret + " tail"),
	}}}
	h.script(stepTools(call("c1", "echo", `{"cmd":"echo %PROBE_TOKEN%"}`)), stepText("完成"))

	res, err := h.loop(nil).Run(context.Background(), "跑一下")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ToolCalls != 1 || res.Events != 4 {
		t.Fatalf("Result 不符: %+v", res)
	}

	// ① 执行器确实拿到了真凭据（证明断言不是"没注入"造成的假通过）。
	if got := exec.seen()[0].env["PROBE_TOKEN"]; got != secret {
		t.Fatalf("PROBE_TOKEN 应注入到执行 env: %q", got)
	}

	// ② completed 事件的载荷里不得有凭据值，只能有 "***"。
	evs := writePath(h.events())
	if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallCompleted}; !equalStrings(got, want) {
		t.Fatalf("事件 kind 序 = %v，want %v", got, want)
	}
	blob, err := json.Marshal(evs[1])
	if err != nil {
		t.Fatalf("序列化 completed 事件: %v", err)
	}
	if strings.Contains(string(blob), secret) {
		t.Fatalf("completed 事件里出现了注入凭据的值:\n%s", blob)
	}
	done := decodeCompleted(t, evs[1])
	if len(done.Result.Content) != 1 || done.Result.Content[0].Text == nil {
		t.Fatalf("completed.result 不符: %+v", done.Result)
	}
	text := *done.Result.Content[0].Text
	if !strings.Contains(text, "***") {
		t.Fatalf("脱敏后应保留 *** 占位: %q", text)
	}
	if !strings.Contains(text, "tail") {
		t.Fatalf("命令自己的输出应原样保留（零误伤）: %q", text)
	}

	// ③ 下一轮 ModelRequest（回填给模型的 tool 消息）里也不得有凭据值。
	reqs := h.provider.reqs()
	if len(reqs) != 2 {
		t.Fatalf("应有 2 次模型调用，得到 %d", len(reqs))
	}
	msgs := reqs[1].Messages
	last := msgs[len(msgs)-1]
	if last.Role != "tool" {
		t.Fatalf("最后一条应是 tool 消息: %+v", last)
	}
	if strings.Contains(last.Content, secret) {
		t.Fatalf("回填给模型的内容里出现了注入凭据的值:\n%s", last.Content)
	}
	if !strings.Contains(last.Content, "***") {
		t.Fatalf("回填内容应含 *** 占位: %q", last.Content)
	}
}

// TestRedactionDoesNotTouchInheritedEnv：继承来的父 env（净化后的普通环境）
// 不在脱敏集合里——redact 它会误伤输出里的正常数据（零误伤）。
func TestRedactionDoesNotTouchInheritedEnv(t *testing.T) {
	t.Setenv("SAFE_PATH_VAR", "/opt/bin")

	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{
		Type: "text",
		Text: strPtr("path=/opt/bin tail"),
	}}}
	h.script(stepTools(call("c1", "echo", `{}`)), stepText("完成"))

	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	done := decodeCompleted(t, writePath(h.events())[1])
	got := *done.Result.Content[0].Text
	if !strings.Contains(got, "/opt/bin") {
		t.Fatalf("继承来的普通 env 值不应被脱敏（误伤）: %q", got)
	}
}

// TestOrdinarySessionValueIsNotRedacted：普通 session 值（LOG_LEVEL=debug 之类）
// 不是秘密 —— 不得被抹成 "***"（误伤）。
func TestOrdinarySessionValueIsNotRedacted(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{
		Type: "text",
		Text: strPtr("build ok debug 1"),
	}}}
	h.script(stepTools(call("c1", "echo", `{}`)), stepText("完成"))

	sessionEnv := core.EnvScope{"LOG_LEVEL": "debug", "MODE": "ok"}
	if _, err := h.loop(func(c *loop.Config) { c.SessionEnv = sessionEnv }).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	done := decodeCompleted(t, writePath(h.events())[1])
	got := *done.Result.Content[0].Text
	if got != "build ok debug 1" {
		t.Fatalf("普通 session 值不应被脱敏（误伤）: %q", got)
	}
	if strings.Contains(got, "***") {
		t.Fatalf("普通 session 值被抹成了 ***: %q", got)
	}
}

// TestCredentialShapedSessionValueIsRedacted：session 级值里**凭据分类命中**的
// （键名凭据类或值凭据形态）算秘密 → 输出里出现必须被收口。
func TestCredentialShapedSessionValueIsRedacted(t *testing.T) {
	cases := []struct {
		name string
		env  core.EnvScope
		out  string
	}{
		{"键名凭据类", core.EnvScope{"MY_SECRET_API_KEY": "plain-looking-value-123"}, "key=plain-looking-value-123 tail"},
		{"值凭据形态", core.EnvScope{"WEIRD_NAME": "sk-shape-token-1234567890"}, "tok=sk-shape-token-1234567890 tail"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, allowRule("echo"))
			exec := h.addTool("echo", nil)
			exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{
				Type: "text",
				Text: strPtr(tc.out),
			}}}
			h.script(stepTools(call("c1", "echo", `{}`)), stepText("完成"))

			if _, err := h.loop(func(c *loop.Config) { c.SessionEnv = tc.env }).Run(context.Background(), "p"); err != nil {
				t.Fatalf("Run: %v", err)
			}
			done := decodeCompleted(t, writePath(h.events())[1])
			got := *done.Result.Content[0].Text
			for _, v := range tc.env {
				if strings.Contains(got, v) {
					t.Fatalf("凭据分类命中的 session 值出现在输出里: %q", got)
				}
			}
			if !strings.Contains(got, "***") {
				t.Fatalf("凭据值应被替换成 ***: %q", got)
			}
			if !strings.Contains(got, "tail") {
				t.Fatalf("命令自己的输出应原样保留: %q", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// P1-2：参数形状不合契约 → 普通失败路径
// ---------------------------------------------------------------------------

func TestBadArgumentsFailsNormallyWithoutAbortingRun(t *testing.T) {
	cases := []struct {
		name    string
		args    string
		wantBad bool // 是否应判为 bad_arguments
	}{
		{"截断的坏 JSON", `{"cmd":"rm -rf`, true},
		{"合法 JSON 但顶层是数组", `[1,2]`, true},
		{"合法 JSON 但顶层是字符串", `"hello"`, true},
		{"空参数（合法，补 {}）", ``, false},
		{"正常对象", `{"cmd":"ls"}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, allowRule("echo"))
			exec := h.addTool("echo", nil)
			h.script(stepTools(call("c1", "echo", tc.args)), stepText("收尾"))

			res, err := h.loop(nil).Run(context.Background(), "p")
			if err != nil {
				t.Fatalf("参数不合约不该中止 Run: %v", err)
			}
			if res.ToolCalls != 1 || res.Events != 4 || res.Turns != 2 {
				t.Fatalf("Result 不符: %+v", res)
			}

			evs := writePath(h.events())
			if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallCompleted}; !equalStrings(got, want) {
				if !tc.wantBad {
					t.Fatalf("事件 kind 序 = %v，want %v", got, want)
				}
				// 坏参数：intent + failed（不执行）。
				if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallFailed}; !equalStrings(got, want) {
					t.Fatalf("事件 kind 序 = %v，want %v", got, want)
				}
				if n := exec.callN(); n != 0 {
					t.Fatalf("参数不合约时不得执行（调用 %d 次）", n)
				}
				// intent 仍落盘，但 arguments 必须是 {}（不能落违反契约的载荷）。
				in := decodeIntent(t, evs[0])
				if string(in.Arguments) != "{}" {
					t.Fatalf("intent.arguments = %s，want {}（契约要求 object）", in.Arguments)
				}
				fp := decodeFailed(t, evs[1])
				if fp.Error.Code == nil || *fp.Error.Code != loop.CodeBadArguments {
					t.Fatalf("失败码 = %v，want %s", fp.Error.Code, loop.CodeBadArguments)
				}
				if !strings.Contains(fp.Error.Message, "不是合法 JSON 对象") {
					t.Fatalf("失败说明应说明参数形状问题: %q", fp.Error.Message)
				}
				if !strings.Contains(fp.Error.Message, "请重新给出 JSON 对象") {
					t.Fatalf("失败说明应告诉模型怎么改（M5）: %q", fp.Error.Message)
				}
				// 原文进 details（诊断用），不参与执行。
				if len(fp.Error.Details) == 0 || !strings.Contains(string(fp.Error.Details), "raw_arguments") {
					t.Fatalf("details 应带参数原文: %s", fp.Error.Details)
				}
				// 失败说明回填给模型，loop 继续（下一轮能收敛）。
				reqs := h.provider.reqs()
				msgs := reqs[len(reqs)-1].Messages
				if !strings.Contains(msgs[len(msgs)-1].Content, "不是合法 JSON 对象") {
					t.Fatalf("失败说明应回填给模型: %+v", msgs[len(msgs)-1])
				}
				return
			}

			// 合法参数：正常执行。
			if n := exec.callN(); n != 1 {
				t.Fatalf("合法参数应执行 1 次，得到 %d", n)
			}
			if got := string(decodeIntent(t, evs[0]).Arguments); tc.args == "" {
				if got != "{}" {
					t.Fatalf("空参数应补成 {}，得到 %s", got)
				}
			} else if got != tc.args {
				t.Fatalf("合法参数应原样保留: got %s want %s", got, tc.args)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// P2-3：H4 可见集过滤
// ---------------------------------------------------------------------------

// TestDeniedToolIsNotVisibleToModel：被策略拒绝的工具不进 ModelRequest.Tools
// （H4：裸工具名 deny = 移出模型可见集）。
func TestDeniedToolIsNotVisibleToModel(t *testing.T) {
	h := newHarness(t, allowRule("read"))
	h.addTool("read", nil)
	h.addTool("write", nil)
	h.script(stepText("ok"))

	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	reqs := h.provider.reqs()
	if len(reqs) == 0 {
		t.Fatal("应有模型请求")
	}
	tools := reqs[0].Tools
	if len(tools) != 1 || tools[0].Name != "read" {
		names := make([]string, 0, len(tools))
		for _, tl := range tools {
			names = append(names, tl.Name)
		}
		t.Fatalf("可见工具 = %v，want [read]（被拒的 write 必须移出可见集）", names)
	}
}

// TestAskToolIsVisibleDenyIsNot：H4 字面口径——只有 **deny** 移出可见集；
// ask 的工具保持可见（审批回路要靠模型发起调用才走得通，人审发生在调用时刻）。
func TestAskToolIsVisibleDenyIsNot(t *testing.T) {
	h := newHarness(t, allowRule("read"), askRule("write"))
	h.addTool("read", nil)
	h.addTool("write", nil)
	h.addTool("bash", nil) // 无命中 = fail-close deny → 必须隐藏
	h.script(stepText("ok"))

	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	reqs := h.provider.reqs()
	names := make([]string, 0, len(reqs[0].Tools))
	for _, tl := range reqs[0].Tools {
		names = append(names, tl.Name)
	}
	if !equalStrings(names, []string{"read", "write"}) {
		t.Fatalf("可见工具 = %v，want [read write]（ask 可见、deny/无命中隐藏）", names)
	}
}

// 编译期证明：core.EnvScope 仍可用于测试构造注入作用域。
var _ = core.EnvScope{}
