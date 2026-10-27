package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// ---- 测试替身 ----

// fakeFS / fakeExec 都带锁：并发用例（-race）会同时打它们。

type fakeFS struct {
	mu       sync.Mutex
	files    map[string][]byte
	written  map[string][]byte
	failWith error
}

func newFakeFS() *fakeFS {
	return &fakeFS{files: map[string][]byte{}, written: map[string][]byte{}}
}

func (f *fakeFS) ReadFile(_ context.Context, path string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWith != nil {
		return nil, f.failWith
	}
	b, ok := f.files[path]
	if !ok {
		return nil, fmt.Errorf("file %s not found", path)
	}
	return b, nil
}

func (f *fakeFS) WriteFile(_ context.Context, path string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWith != nil {
		return f.failWith
	}
	f.written[path] = append([]byte(nil), data...)
	return nil
}

func (f *fakeFS) Stat(_ context.Context, path string) (core.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.files[path]
	if !ok {
		return core.FileInfo{}, fmt.Errorf("file %s not found", path)
	}
	return core.FileInfo{Size: int64(len(b))}, nil
}

func (f *fakeFS) ListDir(_ context.Context, path string) ([]string, error) {
	return nil, fmt.Errorf("ListDir 未实现：path=%s", path)
}

func (f *fakeFS) writtenBytes(t *testing.T, path string) []byte {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.written[path]
	if !ok {
		t.Fatalf("没有写入 %s", path)
	}
	return b
}

type fakeExec struct {
	mu   sync.Mutex
	spec core.ExecSpec
	res  core.ExecResult
	err  error
}

func (f *fakeExec) Run(_ context.Context, spec core.ExecSpec) (core.ExecResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.spec = spec
	if f.err != nil {
		return core.ExecResult{}, f.err
	}
	return f.res, nil
}

func (f *fakeExec) lastSpec() core.ExecSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.spec
}

// intent 造一个 ToolCallIntentPayload（三段事件之①的形状）。
func intent(tool string, args any) contracts.ToolCallIntentPayload {
	b, err := json.Marshal(args)
	if err != nil {
		panic(err)
	}
	return contracts.ToolCallIntentPayload{
		ToolCallID: contracts.ProviderId("call_test_1"),
		ToolName:   tool,
		Arguments:  b,
	}
}

// rawIntent 造一个参数为原始 JSON 的 intent（用于喂非法 JSON）。
func rawIntent(tool string, raw string) contracts.ToolCallIntentPayload {
	return contracts.ToolCallIntentPayload{
		ToolCallID: contracts.ProviderId("call_test_1"),
		ToolName:   tool,
		Arguments:  json.RawMessage(raw),
	}
}

// textOf 取出工具返回的纯文本（v0 只有一个 text 内容块）。
func textOf(t *testing.T, res contracts.ToolResult) string {
	t.Helper()
	if len(res.Content) != 1 {
		t.Fatalf("Content = %d 块，期望 1", len(res.Content))
	}
	if res.Content[0].Type != "text" {
		t.Errorf("Type = %q，期望 text", res.Content[0].Type)
	}
	if res.Content[0].Text == nil {
		t.Fatal("Text 为 nil")
	}
	return *res.Content[0].Text
}

// ---- 描述 ----

func TestDescriptorsShape(t *testing.T) {
	ds := Descriptors()
	if len(ds) != 3 {
		t.Fatalf("Descriptors() = %d 个，期望 3", len(ds))
	}
	want := []struct {
		name       string
		side       contracts.SideEffect
		replay     contracts.ReplayKind
		idempotent bool
		approvable bool
		cancelable bool
	}{
		{toolNameRead, contracts.SideEffectRead, contracts.ReplayKindSafe, true, false, true},
		{toolNameWrite, contracts.SideEffectWrite, contracts.ReplayKindVerify, true, true, false},
		{toolNameBash, contracts.SideEffectWrite, contracts.ReplayKindUnsafe, false, true, true},
	}
	for i, w := range want {
		got := ds[i]
		if got.Name != w.name {
			t.Errorf("[%d] Name = %q，期望 %q", i, got.Name, w.name)
		}
		if got.SideEffect != w.side {
			t.Errorf("%s: SideEffect = %q，期望 %q", got.Name, got.SideEffect, w.side)
		}
		if got.Replay != w.replay {
			t.Errorf("%s: Replay = %q，期望 %q", got.Name, got.Replay, w.replay)
		}
		if got.Idempotent != w.idempotent {
			t.Errorf("%s: Idempotent = %v，期望 %v", got.Name, got.Idempotent, w.idempotent)
		}
		if got.Approvable != w.approvable {
			t.Errorf("%s: Approvable = %v，期望 %v", got.Name, got.Approvable, w.approvable)
		}
		if got.Cancelable != w.cancelable {
			t.Errorf("%s: Cancelable = %v，期望 %v", got.Name, got.Cancelable, w.cancelable)
		}
		if got.Description == nil || strings.TrimSpace(*got.Description) == "" {
			t.Errorf("%s: Description 不能为空（模型靠它决定用不用）", got.Name)
		}
		if len(got.RequiredEnv) != 0 {
			t.Errorf("%s: RequiredEnv = %v，期望空（v0 三个内置工具都不声明凭据）", got.Name, got.RequiredEnv)
		}

		// Schema 必须是合法 JSON Schema，且 required 覆盖全部参数。
		var schema struct {
			Type       string         `json:"type"`
			Properties map[string]any `json:"properties"`
			Required   []string       `json:"required"`
			Additional bool           `json:"additionalProperties"`
		}
		if err := json.Unmarshal(got.Schema, &schema); err != nil {
			t.Fatalf("%s: Schema 不是合法 JSON：%v", got.Name, err)
		}
		if schema.Type != "object" {
			t.Errorf("%s: Schema.type = %q", got.Name, schema.Type)
		}
		if schema.Additional {
			t.Errorf("%s: Schema 应设 additionalProperties=false（避免模型塞奇怪参数）", got.Name)
		}
		if len(schema.Required) != len(schema.Properties) {
			t.Errorf("%s: required=%v properties=%v，应一一对应", got.Name, schema.Required, schema.Properties)
		}
	}
}

func TestWriteDescriptorApprovableConfigurable(t *testing.T) {
	if !WriteDescriptor(true).Approvable {
		t.Error("WriteDescriptor(true) 应可审批")
	}
	if WriteDescriptor(false).Approvable {
		t.Error("WriteDescriptor(false) 应不可审批")
	}
}

// ---- read ----

func TestReadExecute(t *testing.T) {
	fs := newFakeFS()
	fs.files["a.txt"] = []byte("文件内容")
	res, err := NewRead(fs).Execute(context.Background(), intent(toolNameRead, readArgs{Path: "a.txt"}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := textOf(t, res); got != "文件内容" {
		t.Errorf("返回 = %q", got)
	}
}

func TestReadErrors(t *testing.T) {
	fs := newFakeFS()
	fs.files["a.txt"] = []byte("ok")
	cases := []struct {
		name string
		exec core.ToolExecutor
		in   contracts.ToolCallIntentPayload
		want string
	}{
		{"参数缺失", NewRead(fs), rawIntent(toolNameRead, ""), "缺少参数"},
		{"非法 JSON", NewRead(fs), rawIntent(toolNameRead, `{"path":`), "不是合法 JSON"},
		{"空 path", NewRead(fs), intent(toolNameRead, readArgs{Path: "  "}), "path 不能为空"},
		{"path 类型错", NewRead(fs), rawIntent(toolNameRead, `{"path":123}`), "不是合法 JSON"},
		{"文件不存在", NewRead(fs), intent(toolNameRead, readArgs{Path: "nope.txt"}), "not found"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.exec.Execute(context.Background(), c.in)
			if err == nil {
				t.Fatal("期望报错")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("错误文案 = %q，期望含 %q", err, c.want)
			}
			// M5：文案必须让模型知道是哪个工具的锅。
			if !strings.Contains(err.Error(), toolNameRead) {
				t.Errorf("错误文案未带工具名：%q", err)
			}
		})
	}
}

func TestReadBinaryRejected(t *testing.T) {
	fs := newFakeFS()
	fs.files["bin"] = []byte{0xff, 0xfe, 0x00, 0x01}
	_, err := NewRead(fs).Execute(context.Background(), intent(toolNameRead, readArgs{Path: "bin"}))
	if err == nil {
		t.Fatal("二进制文件应被拒绝（v0 只有文本内容块）")
	}
	if !strings.Contains(err.Error(), "UTF-8") {
		t.Errorf("错误文案 = %q，期望说明 UTF-8", err)
	}
}

func TestReadTruncation(t *testing.T) {
	fs := newFakeFS()
	big := strings.Repeat("A", maxToolOutput+5000)
	fs.files["big.txt"] = []byte(big)
	res, err := NewRead(fs).Execute(context.Background(), intent(toolNameRead, readArgs{Path: "big.txt"}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	got := textOf(t, res)
	if len(got) > maxToolOutput+256 {
		t.Errorf("返回 %d 字节，超过上限 + 标注", len(got))
	}
	if !strings.Contains(got, "truncated") {
		t.Error("截断未标注（模型会以为看全了）")
	}
	if !strings.HasPrefix(got, strings.Repeat("A", 1024)) {
		t.Error("截断后内容不对")
	}
}

func TestReadNilCapability(t *testing.T) {
	if _, err := NewRead(nil).Execute(context.Background(), intent(toolNameRead, readArgs{Path: "a"})); err == nil {
		t.Fatal("未注入 FSCapability 应报错")
	}
}

// ---- write ----

func TestWriteExecute(t *testing.T) {
	fs := newFakeFS()
	res, err := NewWrite(fs).Execute(context.Background(), intent(toolNameWrite, writeArgs{Path: "out.txt", Content: strPtr("新内容")}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := string(fs.writtenBytes(t, "out.txt")); got != "新内容" {
		t.Errorf("写入 = %q", got)
	}
	// 回执不回显内容（纯浪费 token）。
	if got := textOf(t, res); got != "wrote 9 bytes to out.txt" {
		t.Errorf("回执 = %q", got)
	}
	// 幂等：同内容再写结果一致。
	if _, err := NewWrite(fs).Execute(context.Background(), intent(toolNameWrite, writeArgs{Path: "out.txt", Content: strPtr("新内容")})); err != nil {
		t.Fatalf("二次写入: %v", err)
	}
	if got := string(fs.writtenBytes(t, "out.txt")); got != "新内容" {
		t.Errorf("覆盖写 = %q", got)
	}
	// 显式空 content = 清空文件（合法，与"缺 content"区分开）。
	if _, err := NewWrite(fs).Execute(context.Background(), intent(toolNameWrite, writeArgs{Path: "out.txt", Content: strPtr("")})); err != nil {
		t.Fatalf("清空写入: %v", err)
	}
	if got := string(fs.writtenBytes(t, "out.txt")); got != "" {
		t.Errorf("清空写 = %q", got)
	}
}

func TestWriteErrors(t *testing.T) {
	fs := newFakeFS()
	cases := []struct {
		name string
		exec core.ToolExecutor
		in   contracts.ToolCallIntentPayload
		want string
	}{
		{"参数缺失", NewWrite(fs), rawIntent(toolNameWrite, ""), "缺少参数"},
		{"缺 content", NewWrite(fs), rawIntent(toolNameWrite, `{"path":"a"}`), "content"},
		{"空 path", NewWrite(fs), intent(toolNameWrite, writeArgs{Content: strPtr("x")}), "path 不能为空"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := c.exec.Execute(context.Background(), c.in); err == nil {
				t.Fatal("期望报错")
			} else if !strings.Contains(err.Error(), c.want) {
				t.Errorf("错误文案 = %q，期望含 %q", err, c.want)
			}
		})
	}
	// FS 报错时要透出（带路径，模型才能改）。
	bad := newFakeFS()
	bad.failWith = errors.New("permission denied")
	if _, err := NewWrite(bad).Execute(context.Background(), intent(toolNameWrite, writeArgs{Path: "/root/x", Content: strPtr("y")})); err == nil ||
		!strings.Contains(err.Error(), "permission denied") {
		t.Errorf("err = %v", err)
	}
	if _, err := NewWrite(nil).Execute(context.Background(), intent(toolNameWrite, writeArgs{Path: "a", Content: strPtr("b")})); err == nil {
		t.Error("未注入 FSCapability 应报错")
	}
}

// ---- bash ----

func TestBashExecute(t *testing.T) {
	fe := &fakeExec{res: core.ExecResult{ExitCode: 0, Stdout: []byte("hello\n")}}
	res, err := NewBash(fe).Execute(context.Background(), intent(toolNameBash, bashArgs{Command: "echo hello"}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// shell 由 build tag 分平台给出。
	if fe.lastSpec().Name != shellName || len(fe.lastSpec().Args) != 2 || fe.lastSpec().Args[0] != shellFlag || fe.lastSpec().Args[1] != "echo hello" {
		t.Errorf("ExecSpec = %+v，期望经 shell 执行", fe.lastSpec())
	}
	got := textOf(t, res)
	if !strings.HasPrefix(got, "exit_code: 0\n") {
		t.Errorf("首行不是 exit_code：%q", got)
	}
	if !strings.Contains(got, "hello") {
		t.Errorf("stdout 未进返回：%q", got)
	}
}

func TestBashNonZeroExitIsNotError(t *testing.T) {
	// 非零退出是业务结果：不进 error，而是渲染给模型看（M5）。
	fe := &fakeExec{res: core.ExecResult{ExitCode: 3, Stderr: []byte("boom\n")}}
	res, err := NewBash(fe).Execute(context.Background(), intent(toolNameBash, bashArgs{Command: "false"}))
	if err != nil {
		t.Fatalf("非零退出不应是 error：%v", err)
	}
	got := textOf(t, res)
	if !strings.Contains(got, "exit_code: 3") || !strings.Contains(got, "boom") {
		t.Errorf("返回 = %q", got)
	}
}

func TestBashErrors(t *testing.T) {
	fe := &fakeExec{err: errors.New("cannot start shell")}
	cases := []struct {
		name string
		exec core.ToolExecutor
		in   contracts.ToolCallIntentPayload
		want string
	}{
		{"参数缺失", NewBash(fe), rawIntent(toolNameBash, ""), "缺少参数"},
		{"空 command", NewBash(fe), intent(toolNameBash, bashArgs{Command: "  "}), "command 不能为空"},
		{"起不来", NewBash(fe), intent(toolNameBash, bashArgs{Command: "ls"}), "cannot start shell"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := c.exec.Execute(context.Background(), c.in); err == nil {
				t.Fatal("期望报错")
			} else if !strings.Contains(err.Error(), c.want) {
				t.Errorf("错误文案 = %q，期望含 %q", err, c.want)
			}
		})
	}
	if _, err := NewBash(nil).Execute(context.Background(), intent(toolNameBash, bashArgs{Command: "ls"})); err == nil {
		t.Error("未注入 ExecCapability 应报错")
	}
}

func TestBashEnvInjectionNotEchoed(t *testing.T) {
	const key, val = "ALASOL_TEST_INJECT", "super-secret-value"
	ctx := core.WithEnv(context.Background(), core.EnvScope{key: val})
	fe := &fakeExec{res: core.ExecResult{ExitCode: 0, Stdout: []byte("ok")}}

	res, err := NewBash(fe).Execute(ctx, intent(toolNameBash, bashArgs{Command: "echo ok"}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// 1) 注入的键确实进了子进程 env。
	found := false
	for _, kv := range fe.lastSpec().Env {
		if kv == key+"="+val {
			found = true
		}
	}
	if !found {
		t.Errorf("注入的 env 未进子进程：%v", fe.lastSpec().Env)
	}
	// 2) 值不回显（H19/H21）：返回值与错误文案里都不能出现。
	if got := textOf(t, res); strings.Contains(got, val) {
		t.Errorf("返回值回显了注入值：%q", got)
	}
}

func TestBashEnvScrubsCredentialKeys(t *testing.T) {
	// 凭据类父 env 键要被剥掉（H16/H18）；普通键（PATH）保留。
	t.Setenv("ALASOL_FAKE_TOKEN", "tok-123")
	t.Setenv("ALASOL_FAKE_PLAIN", "plain-456")
	fe := &fakeExec{res: core.ExecResult{ExitCode: 0}}
	ctx := context.Background()
	if _, err := NewBash(fe).Execute(ctx, intent(toolNameBash, bashArgs{Command: "true"})); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	joined := strings.Join(fe.lastSpec().Env, "\n")
	if strings.Contains(joined, "ALASOL_FAKE_TOKEN") {
		t.Error("凭据类键未被剥离（H18）")
	}
	if !strings.Contains(joined, "ALASOL_FAKE_PLAIN=plain-456") {
		t.Error("普通 env 键被误剥")
	}
	hasPath := false
	for _, kv := range fe.lastSpec().Env {
		if strings.HasPrefix(strings.ToUpper(kv), "PATH=") {
			hasPath = true
		}
	}
	if !hasPath {
		t.Error("PATH 应保留（否则子进程找不到可执行文件）")
	}
}

func TestChildEnvUnit(t *testing.T) {
	// 无注入：净化后的父 env。
	env := childEnv(context.Background())
	if len(env) == 0 {
		t.Fatal("childEnv 为空（子进程会没有 PATH）")
	}
	// 有序（确定性）。
	sorted := append([]string(nil), env...)
	for i := 1; i < len(sorted); i++ {
		if sorted[i-1] > sorted[i] {
			t.Fatalf("childEnv 未排序：%q > %q", sorted[i-1], sorted[i])
		}
	}
	// 有注入：注入键出现，且优先级高于父 env 同名键。
	ctx := core.WithEnv(context.Background(), core.EnvScope{"ALASOL_ONLY_INJECTED": "v1"})
	env2 := childEnv(ctx)
	joined := strings.Join(env2, "\n")
	if !strings.Contains(joined, "ALASOL_ONLY_INJECTED=v1") {
		t.Errorf("注入键未出现：%v", env2)
	}
	// call 级覆盖 session 级（core.MergeEnv 语义由调用方组合，这里只验合并）。
	ctx2 := core.WithEnv(context.Background(), core.EnvScope{"K": "call"})
	if !strings.Contains(strings.Join(childEnv(ctx2), "\n"), "K=call") {
		t.Error("注入值未合并")
	}
}

func TestBashTruncation(t *testing.T) {
	fe := &fakeExec{res: core.ExecResult{ExitCode: 0, Stdout: []byte(strings.Repeat("B", maxToolOutput*2)), Stderr: []byte(strings.Repeat("C", 1000))}}
	res, err := NewBash(fe).Execute(context.Background(), intent(toolNameBash, bashArgs{Command: "yes B"}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	got := textOf(t, res)
	if len(got) > maxToolOutput+256 {
		t.Errorf("返回 %d 字节，超过总上限", len(got))
	}
	if !strings.Contains(got, "truncated") {
		t.Error("截断未标注")
	}
}

// ---- 截断helper ----

func TestTruncateText(t *testing.T) {
	if s, cut := truncateText([]byte("短")); cut || s != "短" {
		t.Errorf("短文本不该被截：%q %v", s, cut)
	}
	// 多字节字符不能被切成两半（否则 JSON 里变 U+FFFD）。
	big := []byte(strings.Repeat("中", maxToolOutput)) // 3 字节/字
	s, cut := truncateText(big)
	if !cut {
		t.Fatal("应被截断")
	}
	if !isValidUTF8(s) {
		t.Error("截断把多字节字符切碎了")
	}
	if len(s) > maxToolOutput {
		t.Errorf("截断后 %d 字节 > 上限", len(s))
	}
}

func isValidUTF8(s string) bool {
	for _, r := range s {
		if r == 0xFFFD {
			return false
		}
	}
	return true
}

// ---- 与真实本地能力联跑（跨包接线验证） ----

func TestToolsWithLocalCaps(t *testing.T) {
	// 不 import local 也行？不行——这里就是要验证 tools × local 的接线。
	fs := newLocalFS(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "hello.txt")

	const content = "联跑内容"
	if _, err := NewWrite(fs).Execute(context.Background(), intent(toolNameWrite, writeArgs{Path: path, Content: strPtr(content)})); err != nil {
		t.Fatalf("write: %v", err)
	}
	res, err := NewRead(fs).Execute(context.Background(), intent(toolNameRead, readArgs{Path: path}))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := textOf(t, res); got != content {
		t.Errorf("读回 = %q", got)
	}

	// bash 真跑一条命令（走本机 shell）。
	bres, err := NewBash(newLocalExec()).Execute(context.Background(), intent(toolNameBash, bashArgs{Command: echoCommandForTest()}))
	if err != nil {
		t.Fatalf("bash: %v", err)
	}
	if got := textOf(t, bres); !strings.Contains(got, "exit_code: 0") {
		t.Errorf("bash 返回 = %q", got)
	}
}

// TestWindowsPathHandling Windows 上反斜杠路径必须端到端可用（陷阱 4 的大小写/分隔符）。
func TestWindowsPathHandling(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	// filepath.Join 在 Windows 上产出反斜杠路径：原样交给能力层，不做任何转换。
	path := filepath.Join(sub, "win.txt")
	fs := newLocalFS(t)
	if _, err := NewWrite(fs).Execute(context.Background(), intent(toolNameWrite, writeArgs{Path: path, Content: strPtr("w")})); err != nil {
		t.Fatalf("write: %v", err)
	}
	res, err := NewRead(fs).Execute(context.Background(), intent(toolNameRead, readArgs{Path: path}))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := textOf(t, res); got != "w" {
		t.Errorf("读回 = %q", got)
	}
	if filepath.Separator != '\\' {
		t.Logf("非 Windows 平台（分隔符 %q）：本用例只做通用路径验证", filepath.Separator)
	}
}

// ---- 并发 ----

func TestToolsConcurrent(t *testing.T) {
	fs := newFakeFS()
	fs.files["c.txt"] = []byte("并发")
	fe := &fakeExec{res: core.ExecResult{ExitCode: 0, Stdout: []byte("ok")}}
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch i % 3 {
			case 0:
				if _, err := NewRead(fs).Execute(ctx, intent(toolNameRead, readArgs{Path: "c.txt"})); err != nil {
					t.Error(err)
				}
			case 1:
				if _, err := NewWrite(fs).Execute(ctx, intent(toolNameWrite, writeArgs{Path: "w.txt", Content: strPtr("x")})); err != nil {
					t.Error(err)
				}
			case 2:
				if _, err := NewBash(fe).Execute(ctx, intent(toolNameBash, bashArgs{Command: "true"})); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
	if got := string(fs.written["w.txt"]); got != "x" {
		t.Errorf("并发写结果 = %q", got)
	}
}
