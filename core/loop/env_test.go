package loop_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/policy"
)

// ---------------------------------------------------------------------------
// env 注入（H21 两级作用域 / H17 按需注入 / H12/H19 值不进事件）
// ---------------------------------------------------------------------------

// fakeSecrets 是本文件用到的假凭据（形态真实：sk- 前缀、JWT 形态）。
// 测试的核心断言之一：这些值**一个字节都不能**出现在事件 / 消息 / Result 里。
var fakeSecrets = []string{
	"sk-allowed-token-1234567890",
	"sk-denied-token-1234567890",
	"sk-unruled-token-1234567890",
	"sk-noise-key-1234567890",
	"sk-session-token-1234567890",
	"sk-plainname-value-1234567890",
	"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZ2VudCJ9.c2lnbmF0dXJl",
}

func setFakeEnv(t *testing.T) {
	t.Helper()
	t.Setenv("ALLOWED_TOKEN", "sk-allowed-token-1234567890")
	t.Setenv("DENIED_TOKEN", "sk-denied-token-1234567890")
	t.Setenv("UNRULED_TOKEN", "sk-unruled-token-1234567890")
	t.Setenv("NOISE_KEY", "sk-noise-key-1234567890")
	t.Setenv("WEIRD_NAME", "sk-plainname-value-1234567890")
	t.Setenv("JWT_VALUE", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZ2VudCJ9.c2lnbmF0dXJl")
	t.Setenv("SAFE_PATH_VAR", "/opt/bin")
}

// TestRequiredEnvInjectedOnlyWhenPolicyAllows 覆盖执行 env 的组装规则：
//
//		执行 env = ScrubEnv(进程env ∪ SessionEnv) ∪ { RequiredEnv 键 : 进程 env 取值 }
//
//	  - RequiredEnv 键逐个过策略（Query{Tool:"env:"+key}）：allow 才注入；
//	  - deny / 无规则（fail-close）→ 不注入；
//	  - 进程 env 里没有该键 → 无可注入值，不留空占位；
//	  - 继承来的进程 env 先过 ScrubEnv：凭据类键名（*_KEY/*_TOKEN…）与凭据形态值
//	    （sk- / JWT）一律剥离；普通 env（PATH 类）保留；
//	  - SessionEnv 同样过 ScrubEnv（凭据类键被剥），非凭据键保留。
func TestRequiredEnvInjectedOnlyWhenPolicyAllows(t *testing.T) {
	setFakeEnv(t)

	h := newHarness(t,
		allowRule("fetch"),
		allowRule("env:ALLOWED_TOKEN"),
		denyRule("env:DENIED_TOKEN"),
		allowRule("env:ABSENT_TOKEN"), // 允许但进程 env 里没有 → 无可注入值
	)
	exec := h.addTool("fetch", func(tool *contracts.Tool) {
		tool.RequiredEnv = []string{"ALLOWED_TOKEN", "DENIED_TOKEN", "UNRULED_TOKEN", "ABSENT_TOKEN"}
	})
	h.script(stepTools(call("c1", "fetch", `{"url":"x"}`)), stepText("取完了"))

	sessionEnv := core.EnvScope{
		"SESSION_MODE":  "strict",
		"SESSION_TOKEN": "sk-session-token-1234567890", // 凭据类键 → 被 ScrubEnv 剥掉
	}
	res, err := h.loop(func(c *loop.Config) { c.SessionEnv = sessionEnv }).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ToolCalls != 1 {
		t.Fatalf("Result 不符: %+v", res)
	}
	if n := exec.callN(); n != 1 {
		t.Fatalf("执行器应调用 1 次，得到 %d", n)
	}

	env := exec.seen()[0].env
	// ① 获批的键被注入（值来自进程 env，注入发生在 ScrubEnv 之后）。
	if got, ok := env["ALLOWED_TOKEN"]; !ok || got != "sk-allowed-token-1234567890" {
		t.Fatalf("ALLOWED_TOKEN 应被注入，得到 %q (ok=%v)", got, ok)
	}
	// ② 未获批的键一律不出现。
	for _, k := range []string{"DENIED_TOKEN", "UNRULED_TOKEN", "ABSENT_TOKEN"} {
		if v, ok := env[k]; ok {
			t.Fatalf("键 %s 未获准却出现在执行 env 里（%q）", k, v)
		}
	}
	// ③ 继承的进程 env 被脱敏：凭据类键名、凭据形态值都被剥掉。
	for _, k := range []string{"NOISE_KEY", "WEIRD_NAME", "JWT_VALUE"} {
		if v, ok := env[k]; ok {
			t.Fatalf("键 %s 应被 ScrubEnv 剥掉，却带着值 %q 进来了", k, v)
		}
	}
	// ④ 普通 env 与 session 级普通键保留。
	if got, ok := env["SAFE_PATH_VAR"]; !ok || got != "/opt/bin" {
		t.Fatalf("普通 env 应保留: %q (ok=%v)", got, ok)
	}
	if got, ok := env["SESSION_MODE"]; !ok || got != "strict" {
		t.Fatalf("session 级普通键应保留: %q (ok=%v)", got, ok)
	}
	// ⑤ session 级凭据键被剥（显式注入只走 RequiredEnv 单次调用通道，H21）。
	if v, ok := env["SESSION_TOKEN"]; ok {
		t.Fatalf("SESSION_TOKEN 应被 ScrubEnv 剥掉，却带着值 %q 进来了", v)
	}
}

func TestScrubEnvStripsCredentialKeyAndValueShapes(t *testing.T) {
	setFakeEnv(t)

	h := newHarness(t, allowRule("noop"))
	h.addTool("noop", nil) // 无 RequiredEnv：只看继承部分
	h.script(stepTools(call("c1", "noop", `{}`)), stepText("ok"))

	res, err := h.loop(nil).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ToolCalls != 1 {
		t.Fatalf("Result 不符: %+v", res)
	}
	env := h.execs["noop"].seen()[0].env
	// 值形态为凭据但键名普通（WEIRD_NAME / JWT_VALUE）→ 仍被剥（H18 deny 名单 + 值形态）。
	for _, k := range []string{"WEIRD_NAME", "JWT_VALUE", "NOISE_KEY"} {
		if _, ok := env[k]; ok {
			t.Fatalf("键 %s 应被剥掉", k)
		}
	}
	if _, ok := env["SAFE_PATH_VAR"]; !ok {
		t.Fatal("普通 env 应保留")
	}
}

func TestExecEnvNeverContainsSecretValuesInEventsOrMessages(t *testing.T) {
	setFakeEnv(t)

	h := newHarness(t, allowRule("fetch"), allowRule("env:ALLOWED_TOKEN"))
	exec := h.addTool("fetch", func(tool *contracts.Tool) {
		tool.RequiredEnv = []string{"ALLOWED_TOKEN"}
		// 执行器故意把工具输出写成"看起来像泄露"的普通文本（不含真凭据）。
	})
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: strPtr("ok")}}}
	h.script(stepTools(call("c1", "fetch", `{}`)), stepText("完成"))

	sessionEnv := core.EnvScope{"SESSION_TOKEN": "sk-session-token-1234567890"}
	res, err := h.loop(func(c *loop.Config) { c.SessionEnv = sessionEnv }).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// ① 每条入库事件序列化后不得含任何凭据值（H12/H19）。
	for _, ev := range h.events() {
		blob, err := json.Marshal(ev)
		if err != nil {
			t.Fatalf("序列化事件: %v", err)
		}
		assertNoSecrets(t, "事件 "+ev.Kind, string(blob))
	}
	// ② Result 序列化后同样不得含凭据值。
	if blob, err := json.Marshal(res); err != nil {
		t.Fatalf("序列化 Result: %v", err)
	} else {
		assertNoSecrets(t, "Result", string(blob))
	}
	// ③ 送回模型的请求（System/Messages/Tools）不得含凭据值。
	for i, req := range h.provider.reqs() {
		blob, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("序列化 ModelRequest: %v", err)
		}
		assertNoSecrets(t, "ModelRequest#"+string(rune('0'+i)), string(blob))
	}
	// ④ 执行器确实拿到了值（证明前面的断言不是"什么都没注入"造成的假通过）。
	if got := exec.seen()[0].env["ALLOWED_TOKEN"]; got != "sk-allowed-token-1234567890" {
		t.Fatalf("ALLOWED_TOKEN 应注入到执行 env: %q", got)
	}
}

func TestEnvPolicyDecisionAppliesPerKey(t *testing.T) {
	// 同一个工具的两个 env 键：一个 allow 一个 deny —— 逐个键独立裁决（H17）。
	setFakeEnv(t)
	h := newHarness(t,
		allowRule("multi"),
		allowRule("env:ALLOWED_TOKEN"),
		denyRule("env:DENIED_TOKEN"),
	)
	exec := h.addTool("multi", func(tool *contracts.Tool) {
		tool.RequiredEnv = []string{"DENIED_TOKEN", "ALLOWED_TOKEN", "DENIED_TOKEN"} // 重复键只裁决一次
	})
	h.script(stepTools(call("c1", "multi", `{}`)), stepText("ok"))

	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	env := exec.seen()[0].env
	if _, ok := env["ALLOWED_TOKEN"]; !ok {
		t.Fatal("ALLOWED_TOKEN 应注入")
	}
	if _, ok := env["DENIED_TOKEN"]; ok {
		t.Fatal("DENIED_TOKEN 不应注入")
	}
}

// TestPolicySnapshotNeverCarriesEnv 保证 intent 的策略快照只记判定结论，
// 不夹带参数里的敏感值之外的东西——这里断言快照字段集合就是
// verdict/rule_id/reason（H19：只记结论与键名，不记值）。
func TestPolicySnapshotShape(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(stepTools(call("c1", "echo", `{"secretish":"sk-should-not-be-logged"}`)), stepText("ok"))
	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	raw := decodeIntent(t, writePath(h.events())[0]).PolicySnapshot
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("快照不是对象: %v", err)
	}
	if len(m) != 3 {
		t.Fatalf("快照应只含 verdict/rule_id/reason 三个字段，得到 %d: %s", len(m), raw)
	}
	for _, k := range []string{"verdict", "rule_id", "reason"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("快照缺字段 %s: %s", k, raw)
		}
	}
}

// TestNilPolicyEngineFailsClosed：Config.Policy 为 nil = 空引擎语义，
// 工具调用与 env 键都拿不到 allow → 一律 deny（fail-close），执行器零调用。
func TestNilPolicyEngineFailsClosed(t *testing.T) {
	setFakeEnv(t)
	h := newHarness(t)
	exec := h.addTool("fetch", func(tool *contracts.Tool) {
		tool.RequiredEnv = []string{"ALLOWED_TOKEN"}
	})
	h.script(stepTools(call("c1", "fetch", `{}`)), stepText("ok"))

	// 注意：newHarness 用空引擎建注册表；这里再把 Config.Policy 显式置 nil。
	res, err := h.loop(func(c *loop.Config) { c.Policy = nil }).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ToolCalls != 1 || res.Events != 4 {
		t.Fatalf("Result 不符: %+v", res)
	}
	if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallFailed}; !equalStrings(got, want) {
		t.Fatalf("事件 kind 序 = %v，want %v", got, want)
	}
	fp := decodeFailed(t, writePath(h.events())[1])
	if !strings.Contains(fp.Error.Message, "fail-close") {
		t.Fatalf("空引擎应按 fail-close 拒绝: %q", fp.Error.Message)
	}
	if fp.Error.Code == nil || *fp.Error.Code != loop.CodePolicyDeny {
		t.Fatalf("失败码 = %v，want %s", fp.Error.Code, loop.CodePolicyDeny)
	}
	// 执行器压根没被调用 → 没有任何 env 进入执行现场。
	if n := exec.callN(); n != 0 {
		t.Fatalf("deny 路径不应调用执行器（%d 次）", n)
	}
	if len(exec.seen()) != 0 {
		t.Fatal("deny 路径不应有执行记录")
	}
}

// assertNoSecrets 断言文本里不含任何假凭据值。
func assertNoSecrets(t *testing.T, where, blob string) {
	t.Helper()
	for _, s := range fakeSecrets {
		if strings.Contains(blob, s) {
			t.Fatalf("%s 里出现了凭据值 %q（H12/H19：值不得进事件/消息/日志）:\n%s", where, s, blob)
		}
	}
}

// 编译期证明：policy 包的 Query 形态与本测试用法一致。
var _ = policy.Query{}
