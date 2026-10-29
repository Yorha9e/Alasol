package mcp

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// 本文件是 **stdio server stderr 不被丢弃**（p3）的回归用例。
//
// 旧实现：cmd.Stderr 没接 → 子进程的 stderr 进 /dev/null → 外部 server 的
// 启动失败原因、依赖缺失、崩溃栈**全部看不到**（装配期只能看到一句
// "tools/list 失败"）。
//
// 同时钉住**脱敏**：server 完全可能把收到的凭据打进 stderr，凭据值不进日志
// 是不可让渡的纪律（H12/H19）。

// captureMCPLog 把 mcpLog 换成写进缓冲区的 logger，返回缓冲与还原函数。
func captureMCPLog(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	var buf bytes.Buffer
	old := mcpLog
	mcpLog = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return &buf, func() { mcpLog = old }
}

// TestStderrSinkLogsAndRedacts：stderr 进日志，且**凭据值被打码**。
func TestStderrSinkLogsAndRedacts(t *testing.T) {
	buf, restore := captureMCPLog(t)
	defer restore()

	cfg := &ServerConfig{
		Name:    "srv",
		Command: "x",
		Env:     map[string]string{"TOKEN": "sk-supersecret-value"},
	}
	sink := newStderrSink(cfg)
	if _, err := sink.Write([]byte("connecting with token=sk-supersecret-value\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "connecting with token=") {
		t.Fatalf("stderr 没进日志: %q", out)
	}
	if strings.Contains(out, "sk-supersecret-value") {
		t.Fatalf("凭据值没打码（凭据纪律 H12/H19）: %q", out)
	}
	if !strings.Contains(out, "***") {
		t.Fatalf("没有打码标记: %q", out)
	}
	// server 名可安全打印（诊断价值），但不能带配置值。
	if !strings.Contains(out, "srv") {
		t.Fatalf("日志里没有 server 名: %q", out)
	}
}

// TestStderrSinkSplitsLines：按行切分（server 的诊断信息一行一件事）。
func TestStderrSinkSplitsLines(t *testing.T) {
	buf, restore := captureMCPLog(t)
	defer restore()

	sink := newStderrSink(&ServerConfig{Name: "srv"})
	// 分两次写：第一次是半行，第二次补齐并换行。
	if _, err := sink.Write([]byte("first ")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("半行不该立刻落日志: %q", buf.String())
	}
	if _, err := sink.Write([]byte("line\nsecond line\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "first line") || !strings.Contains(out, "second line") {
		t.Fatalf("行切分不对: %q", out)
	}
}

// TestStderrSinkShortSecretNotRedacted：太短的"凭据"不打码（否则整行日志变成
// "***"，诊断价值全失）——这是有意识的取舍，写成用例免得后人当成 bug。
func TestStderrSinkShortSecretNotRedacted(t *testing.T) {
	buf, restore := captureMCPLog(t)
	defer restore()

	sink := newStderrSink(&ServerConfig{Name: "srv", Env: map[string]string{"A": "ab"}})
	if _, err := sink.Write([]byte("value=ab\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !strings.Contains(buf.String(), "value=ab") {
		t.Fatalf("短值不该被打码: %q", buf.String())
	}
}

// TestStderrSinkCapsLine：超长行被截断（防无界日志）。
func TestStderrSinkCapsLine(t *testing.T) {
	buf, restore := captureMCPLog(t)
	defer restore()

	sink := newStderrSink(&ServerConfig{Name: "srv"})
	if _, err := sink.Write([]byte(strings.Repeat("x", maxStderrLine*3))); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if buf.Len() > maxStderrLine*2 {
		t.Fatalf("日志行没被截断（%d 字节）", buf.Len())
	}
}

// TestSecretValuesCoversEnvFrom：显式从父 env 继承的变量，其**值**也要脱敏
// （凭据可以只活在进程 env 里——那正是 env_from 的用法）。
func TestSecretValuesCoversEnvFrom(t *testing.T) {
	t.Setenv("ALASOL_TEST_MCP_TOKEN", "parent-secret-token")
	secrets := secretValues(&ServerConfig{
		Name:    "srv",
		Env:     map[string]string{"A": "direct-secret"},
		Headers: map[string]string{"Authorization": "Bearer header-secret"},
		EnvFrom: []string{"ALASOL_TEST_MCP_TOKEN"},
	})
	joined := strings.Join(secrets, "|")
	for _, want := range []string{"direct-secret", "Bearer header-secret", "parent-secret-token"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("脱敏清单缺 %q: %v", want, secrets)
		}
	}
}

// TestSecretValuesCoversSchemeStrippedBody：**剥掉 scheme 后的值本体**也要登记。
//
// 场景：headers 里配的是 "Bearer sk-header-secret"，而 server 把凭据**单独**
// 打出来（"auth=sk-header-secret"）——只登记整串会漏掉这一形态，值就原样
// 进日志了。
func TestSecretValuesCoversSchemeStrippedBody(t *testing.T) {
	secrets := secretValues(&ServerConfig{
		Name:    "srv",
		Headers: map[string]string{"Authorization": "Bearer sk-header-secret"},
	})
	found := false
	for _, s := range secrets {
		if s == "sk-header-secret" {
			found = true
		}
	}
	if !found {
		t.Fatalf("脱敏清单缺 scheme 剥掉后的值本体: %v", secrets)
	}
}

// TestStderrSinkRedactsSchemeStrippedBody：端到端——server 打出
// "auth=sk-header-secret"（值本体，没有 scheme）时也必须被打码。
func TestStderrSinkRedactsSchemeStrippedBody(t *testing.T) {
	buf, restore := captureMCPLog(t)
	defer restore()

	cfg := &ServerConfig{
		Name:    "srv",
		Headers: map[string]string{"Authorization": "Bearer sk-header-secret"},
	}
	sink := newStderrSink(cfg)
	if _, err := sink.Write([]byte("auth=sk-header-secret\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	out := buf.String()
	if strings.Contains(out, "sk-header-secret") {
		t.Fatalf("scheme 剥掉后的值本体没打码: %q", out)
	}
	if !strings.Contains(out, "auth=***") {
		t.Fatalf("没打码成预期形态: %q", out)
	}
}

// TestStderrSinkTotalCap：stderr 的**总量**有上限（1 万行 × 1KB = 11MB 不能
// 刷爆日志）。超出后停止逐行记录，并补一条"stderr 已截断"。
func TestStderrSinkTotalCap(t *testing.T) {
	buf, restore := captureMCPLog(t)
	defer restore()

	sink := newStderrSink(&ServerConfig{Name: "srv"})
	line := strings.Repeat("x", 1023) + "\n"
	const n = 10000
	for i := 0; i < n; i++ {
		if _, err := sink.Write([]byte(line)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	out := buf.String()
	// 总量封顶（日志文本本身有开销，给 4 倍余量）。
	if len(out) > 4*maxStderrTotal {
		t.Fatalf("stderr 日志 %d 字节，超过上限 %d（总量闸没生效）", len(out), maxStderrTotal)
	}
	// 必须有一条"已截断"的说明（让人知道后面的输出没被记录）。
	if !strings.Contains(out, "stderr 已截断") {
		t.Fatalf("缺截断说明: %q", out[:min(len(out), 200)])
	}
	// 截断后不再逐条记录：落日志的条数被总量闸压住（每条 ~1KB）。
	if got := strings.Count(out, "line="); got > maxStderrTotal/1024+16 {
		t.Fatalf("落日志 %d 条，超过总量闸允许的条数（截断后仍在逐条记录）", got)
	}
}
