package sandbox_test

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/cap/local"
	"github.com/Yorha9e/alasol/runtime/cap/sandbox"
	"github.com/Yorha9e/alasol/runtime/tools"
)

// recordingBackend 是一个"记账后端"：不施加任何真实约束，只把接口调用记下来。
//
// 用途只有一个：**K8 酸测试**——证明"换 env 实现不动 agent / 工具代码"这件事
// 与平台无关（Windows 上真沙箱的强制语义由 backend_windows_test.go 单独验）。
// 生产代码永远不要用它：它**不施加任何约束**。
type recordingBackend struct {
	mu     sync.Mutex
	name_  string
	began  int
	spawn  []sandbox.SpawnOptions
	attch  []int
	rels   int
	acls   int
	closed int
	diags  []sandbox.Diag

	// failAttach 让 Attach 返回错误（模拟"约束没施加上去"）。
	// 用来验证 Run 在这条失败路径上**不会挂死**（N4：挂起创建的进程必须被杀掉）。
	failAttach bool
	// suspend 让 Spawn 返回 Suspend=true（模拟 Windows 后端的挂起创建）。
	suspend bool
}

func newRecordingBackend(name string) *recordingBackend {
	return &recordingBackend{name_: name}
}

func (b *recordingBackend) Name() string { return b.name_ }

func (b *recordingBackend) Diagnostics() []sandbox.Diag {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]sandbox.Diag, len(b.diags))
	copy(out, b.diags)
	return out
}

func (b *recordingBackend) FileRoots() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]sandbox.Diag, len(b.diags))
	copy(out, b.diags)
	_ = out
	return nil // 记账后端不管文件约束（它不施加任何约束）
}

func (b *recordingBackend) ApplyFileACL(ctx context.Context) error {
	_ = ctx
	b.mu.Lock()
	defer b.mu.Unlock()
	b.acls++
	return nil
}

func (b *recordingBackend) Begin(ctx context.Context, spec core.ExecSpec) (sandbox.Execution, error) {
	_ = ctx
	_ = spec
	b.mu.Lock()
	defer b.mu.Unlock()
	b.began++
	return &recordingExecution{backend: b}, nil
}

func (b *recordingBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed++
	return nil
}

type recordingExecution struct {
	backend *recordingBackend
	done    bool
}

func (e *recordingExecution) Spawn() sandbox.SpawnOptions {
	e.backend.mu.Lock()
	defer e.backend.mu.Unlock()
	opts := sandbox.SpawnOptions{Suspend: e.backend.suspend}
	e.backend.spawn = append(e.backend.spawn, opts)
	return opts
}

func (e *recordingExecution) Attach(pid int) error {
	e.backend.mu.Lock()
	defer e.backend.mu.Unlock()
	e.backend.attch = append(e.backend.attch, pid)
	if e.backend.failAttach {
		return fmt.Errorf("recording-backend: 故意让 Attach 失败")
	}
	return nil
}

func (e *recordingExecution) Release() error {
	if e.done {
		return nil
	}
	e.done = true
	e.backend.mu.Lock()
	defer e.backend.mu.Unlock()
	e.backend.rels++
	return nil
}

// echoCmd 返回当前平台上"回显一段固定文本"的命令（跨平台可跑）。
//
// 与 local 的测试同款：Windows 用 cmd /c echo，Unix 用 echo。这是**测试代码**里
// 唯一允许出现 shell 的地方，目的是证明 Exec 能把带参数的命令跑起来。
func echoCmd() (name string, args []string) {
	if runtime.GOOS == "windows" {
		return "cmd", []string{"/c", "echo", "sandbox-cap"}
	}
	return "echo", []string{"sandbox-cap"}
}

// failCmd 返回"以指定退出码失败"的命令。
func failCmd(code string) (name string, args []string) {
	if runtime.GOOS == "windows" {
		return "cmd", []string{"/c", "exit", code}
	}
	return "sh", []string{"-c", "exit " + code}
}

// sleepCmd 返回"长时间不退出"的命令（用于取消测试）。
func sleepCmd() (name string, args []string) {
	if runtime.GOOS == "windows" {
		// ping -n 60 约 59 秒：本地回环、不联网、无窗口。
		return "cmd", []string{"/c", "ping", "-n", "60", "127.0.0.1", ">", "nul"}
	}
	return "sleep", []string{"60"}
}

// newRecordingEnv 造一个带记账后端的沙箱 env（跨平台可跑）。
func newRecordingEnv(t *testing.T, root string) (*sandbox.Env, *recordingBackend) {
	t.Helper()
	p := sandbox.DefaultPolicy()
	p.AllowedRoots = []string{root}
	b := newRecordingBackend("recording-stub")
	env, err := sandbox.NewWithBackend(p, b)
	if err != nil {
		t.Fatalf("NewWithBackend: %v", err)
	}
	t.Cleanup(func() { _ = env.Close() })
	return env, b
}

// intent 造一个 tool.call.intent（测试辅助）。
func intent(tool string, args any) contracts.ToolCallIntentPayload {
	raw, err := json.Marshal(args)
	if err != nil {
		panic(err)
	}
	return contracts.ToolCallIntentPayload{ToolName: tool, Arguments: raw}
}

// TestK8SwapEnvZeroAgentChange 是 K8 的酸测试：**换 env 实现，agent / 工具代码零改动**。
//
// 做法：同一份 tools.Descriptors() + 同一套 executor 构造代码，分别喂
// local.{FS,Exec} 与 sandbox.Env，跑同一条调用，比对结果。
// 被测的是"装配侧能不能一对一替换"，不是沙箱的强制语义（那个见 Windows 专测）。
func TestK8SwapEnvZeroAgentChange(t *testing.T) {
	root := t.TempDir()
	sbEnv, stub := newRecordingEnv(t, root)

	fsL, exL := local.FS{}, local.Exec{}
	bashL := tools.NewBash(exL)
	bashS := tools.NewBash(sbEnv) // ← 只换这一行

	ctx := context.Background()
	name, args := echoCmd()
	resL, err := bashL.Execute(ctx, intent("bash", map[string]any{"command": shellLine(name, args)}))
	if err != nil {
		t.Fatalf("local bash: %v", err)
	}
	resS, err := bashS.Execute(ctx, intent("bash", map[string]any{"command": shellLine(name, args)}))
	if err != nil {
		t.Fatalf("sandbox bash: %v", err)
	}
	if resultTextOf(resL) != resultTextOf(resS) {
		t.Errorf("local 与 sandbox 的 bash 结果不同：\nlocal=%q\nsandbox=%q", resultTextOf(resL), resultTextOf(resS))
	}
	if !strings.Contains(resultTextOf(resS), "sandbox-cap") {
		t.Errorf("sandbox bash 输出缺回显：%q", resultTextOf(resS))
	}

	// read / write 工具同样零改动换装（跨 env 互通：一个写一个读）
	inside := filepath.Join(root, "probe.txt")
	ws, rs := tools.NewWrite(sbEnv), tools.NewRead(sbEnv)
	wl, rl := tools.NewWrite(fsL), tools.NewRead(fsL)

	if _, err := wl.Execute(ctx, intent("write", map[string]any{"path": inside, "content": "from-local"})); err != nil {
		t.Fatalf("local write: %v", err)
	}
	gotS, err := rs.Execute(ctx, intent("read", map[string]any{"path": inside}))
	if err != nil {
		t.Fatalf("sandbox read: %v", err)
	}
	if !strings.Contains(resultTextOf(gotS), "from-local") {
		t.Errorf("sandbox read 读不到 local 写的文件：%q", resultTextOf(gotS))
	}
	if _, err := ws.Execute(ctx, intent("write", map[string]any{"path": inside, "content": "from-sandbox"})); err != nil {
		t.Fatalf("sandbox write: %v", err)
	}
	gotL, err := rl.Execute(ctx, intent("read", map[string]any{"path": inside}))
	if err != nil {
		t.Fatalf("local read: %v", err)
	}
	if !strings.Contains(resultTextOf(gotL), "from-sandbox") {
		t.Errorf("local read 读不到 sandbox 写的文件：%q", resultTextOf(gotL))
	}

	// 后端埋点确实被走过（证明约束路径真的接上了，不是被绕过）
	stub.mu.Lock()
	began, rels, acls := stub.began, stub.rels, stub.acls
	stub.mu.Unlock()
	if began == 0 || rels == 0 {
		t.Errorf("后端 Begin/Release 未被调用（began=%d rels=%d）——约束路径没接上", began, rels)
	}
	if acls == 0 {
		t.Error("后端 ApplyFileACL 未被调用")
	}
}

// TestPolicyValidateFailClose 钉死"零值 Policy 不是沙箱"这条 fail-close。
func TestPolicyValidateFailClose(t *testing.T) {
	if err := (sandbox.Policy{}).Validate(); err == nil {
		t.Fatal("零值 Policy 通过了 Validate——那不是沙箱，必须拒")
	}
	good := sandbox.DefaultPolicy()
	good.AllowedRoots = []string{t.TempDir()}
	if err := good.Validate(); err != nil {
		t.Errorf("DefaultPolicy + AllowedRoots 应该通过：%v", err)
	}
	bad := good
	bad.AllowedRoots = []string{"relative/path"}
	if err := bad.Validate(); err == nil {
		t.Error("相对路径的允许根应该被拒")
	}
}

// TestDefaultPolicyAllOn 是 DefaultPolicy 的对照：默认全开。
func TestDefaultPolicyAllOn(t *testing.T) {
	p := sandbox.DefaultPolicy()
	if !p.UseJobObject || !p.UseRestrictedToken || !p.UseFileACL || !p.SuppressWER || !p.RedirectTemp {
		t.Errorf("DefaultPolicy 应该是全开：%+v", p)
	}
}

// TestPathGuardRejectsOutsideAllowedRoots 验证 harness 内的允许集软检查是 fail-close 的。
func TestPathGuardRejectsOutsideAllowedRoots(t *testing.T) {
	root := t.TempDir()
	env, _ := newRecordingEnv(t, root)
	outside := filepath.Join(t.TempDir(), "escape.txt")

	if err := env.WriteFile(context.Background(), outside, []byte("x")); err == nil {
		t.Error("写允许集之外居然成功了——软检查失效")
	} else if !strings.Contains(err.Error(), "允许集之外") {
		t.Errorf("错误信息没说清是允许集问题：%v", err)
	}
	if err := env.WriteFile(context.Background(), filepath.Join(root, "ok.txt"), []byte("x")); err != nil {
		t.Errorf("写允许集之内失败：%v", err)
	}
	if _, err := env.ReadFile(context.Background(), outside); err == nil {
		t.Error("读允许集之外居然成功了")
	}
	if _, err := env.ListDir(context.Background(), outside); err == nil {
		t.Error("列举允许集之外居然成功了")
	}
	if _, err := env.Stat(context.Background(), outside); err == nil {
		t.Error("stat 允许集之外居然成功了")
	}
}

// TestExecSemanticsMatchLocal 验证沙箱 Exec 与 local Exec 的**调用侧语义**一致
// （K8：换实现不动调用方）——非零退出码不是 error、Name 空是 error、ctx 已取消是 error。
func TestExecSemanticsMatchLocal(t *testing.T) {
	root := t.TempDir()
	env, _ := newRecordingEnv(t, root)
	ctx := context.Background()

	fn, fa := failCmd("3")
	res, err := env.Run(ctx, core.ExecSpec{Name: fn, Args: fa})
	if err != nil {
		t.Fatalf("非零退出不该是 error：%v", err)
	}
	if res.ExitCode != 3 {
		t.Errorf("ExitCode = %d，期望 3", res.ExitCode)
	}
	if _, err := env.Run(ctx, core.ExecSpec{Name: "   "}); err == nil {
		t.Error("空 Name 应该报错")
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := env.Run(cctx, core.ExecSpec{Name: "cmd", Args: []string{"/c", "echo", "x"}}); err == nil {
		t.Error("已取消的 ctx 应该直接报错")
	}
}

// TestExecContextCancel 验证 ctx 取消会让 Run 及时返回（与 local 同一条硬约束：
// 孙子攥管道会让 Wait 挂几十秒）。
func TestExecContextCancel(t *testing.T) {
	root := t.TempDir()
	env, _ := newRecordingEnv(t, root)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	sn, sa := sleepCmd()
	go func() {
		_, err := env.Run(ctx, core.ExecSpec{Name: sn, Args: sa})
		done <- err
	}()
	time.Sleep(700 * time.Millisecond) // 给 Start + Attach 留时间
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("取消后 Run 应该返回 error")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("取消后 Run 没有在 20s 内返回（管道被孙子攥住的回归）")
	}
}

// TestNewFailsClosedWithoutBackend 验证"查不到后端 → 装配失败"这条 fail-close 契约。
func TestNewFailsClosedWithoutBackend(t *testing.T) {
	if _, ok := sandbox.BackendByName("definitely-not-a-backend"); ok {
		t.Fatal("不存在的后端名居然注册上了")
	}
	names := sandbox.BackendNames()
	if len(names) == 0 {
		t.Fatal("一个后端都没注册——init() 没跑？")
	}
	t.Logf("已注册后端：%v（平台默认 %q）", names, sandbox.PlatformBackendName())
}

// TestRunReturnsWhenAttachFails 钉死 N4：Attach 失败时 Run 必须**返回**，不能挂死。
//
// 为什么这是个真 bug：Windows 后端是以 CREATE_SUSPENDED 创建子进程的
// （先纳入 job 再恢复执行，堵住 Attach 窗口）。这条失败路径上进程**还挂着**——
// 如果只靠 taskkill 去杀（对挂起进程可能失败），cmd.Wait() 就会一直等一个
// 永远不会自己退出的进程 → Run 永久挂死。
//
// 修法：主路径用 Process.Kill()（TerminateProcess，对挂起进程也必定成功）。
//
// （判别性用例在 backend_windows_test.go：那里能把 taskkill 兜底**关掉**，
// 本机实测 taskkill 对挂起进程常常还能杀掉，所以这条只当"不挂死"的烟测。）
func TestRunReturnsWhenAttachFails(t *testing.T) {
	runAttachFailureMustReturn(t)
}

// runAttachFailureMustReturn 是上面那条的共用实现（跨平台 + Windows 判别用例共用）。
func runAttachFailureMustReturn(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	p := sandbox.DefaultPolicy()
	p.AllowedRoots = []string{root}
	b := newRecordingBackend("recording-stub")
	b.failAttach = true
	b.suspend = true // 模拟挂起创建
	env, err := sandbox.NewWithBackend(p, b)
	if err != nil {
		t.Fatalf("NewWithBackend: %v", err)
	}
	t.Cleanup(func() { _ = env.Close() })

	done := make(chan error, 1)
	go func() {
		_, runErr := env.Run(context.Background(), core.ExecSpec{
			Name: "cmd",
			Args: []string{"/c", "echo", "x"},
		})
		done <- runErr
	}()
	select {
	case runErr := <-done:
		if runErr == nil {
			t.Error("Attach 失败应该返回 error（约束没施加上去 = fail-close）")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Attach 失败后 Run 挂死了——挂起创建的进程没被杀掉（N4）")
	}
}

// TestEnvCloseIdempotent 验证 Close 幂等（host 生命周期里可能被调多次）。
func TestEnvCloseIdempotent(t *testing.T) {
	root := t.TempDir()
	env, stub := newRecordingEnv(t, root)
	if err := env.Close(); err != nil {
		t.Fatalf("第一次 Close: %v", err)
	}
	if err := env.Close(); err != nil {
		t.Fatalf("第二次 Close 应该幂等：%v", err)
	}
	stub.mu.Lock()
	n := stub.closed
	stub.mu.Unlock()
	if n == 0 {
		t.Error("后端 Close 未被转发")
	}
}

// TestPathGuardCaseInsensitive 钉死 Windows 路径身份比较（陷阱 4：大小写不敏感）。
func TestPathGuardCaseInsensitive(t *testing.T) {
	root := t.TempDir()
	env, _ := newRecordingEnv(t, root)
	if err := env.WriteFile(context.Background(), filepath.Join(root, "A.TXT"), []byte("x")); err != nil {
		t.Fatalf("写允许集内失败：%v", err)
	}
	for _, variant := range []string{filepath.Join(root, "a.txt"), filepath.Join(root, "A.TXT")} {
		if _, err := env.ReadFile(context.Background(), variant); err != nil {
			t.Errorf("读 %s 失败（大小写变体应命中允许集）：%v", variant, err)
		}
	}
}

// TestSandboxTempRedirect 验证子进程的 TMP/TEMP 被指到沙箱临时根（否则受限 token
// 的子进程写不了真实 %TEMP%，git / npm / 编译器会莫名失败）。
func TestSandboxTempRedirect(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("TMP/TEMP 重定向按 Windows 口径断言（cmd 的 TMP 展开）")
	}
	root := t.TempDir()
	env, _ := newRecordingEnv(t, root)
	res, err := env.Run(context.Background(), core.ExecSpec{
		Name: "cmd",
		Args: []string{"/c", "echo", "%TMP%"},
		Env:  []string{"TMP=C:\\somewhere-else"}, // 显式给个错的，看沙箱能不能压回来
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := env.Policy().SandboxTempRoot
	got := strings.TrimSpace(string(res.Stdout))
	if !strings.EqualFold(got, want) {
		t.Errorf("子进程 TMP = %q，期望被沙箱重定向到 %q", got, want)
	}
}

// shellLine 把 name + args 拼成一行 shell 命令（bash 工具要的是字符串）。
func shellLine(name string, args []string) string {
	parts := append([]string{name}, args...)
	quoted := make([]string, len(parts))
	for i, p := range parts {
		if strings.ContainsAny(p, " \t") {
			quoted[i] = `"` + p + `"`
		} else {
			quoted[i] = p
		}
	}
	return strings.Join(quoted, " ")
}

// resultTextOf 取 ToolResult 的文本内容（工具返回的第一个 text 块）。
//
// 单独包一层的理由：contracts.ToolResult 是内容块数组（v1 才可能有 image/resource），
// 测试只关心文本；直接下标会让"结构一变测试全红"。
func resultTextOf(r contracts.ToolResult) string {
	for _, p := range r.Content {
		if p.Type == "text" && p.Text != nil {
			return *p.Text
		}
	}
	return ""
}

// newBashTool 造真的 bash 工具执行器（端到端测试用：走 runtime/tools 而不是直接调 Env）。
func newBashTool(exec core.ExecCapability) core.ToolExecutor {
	return tools.NewBash(exec)
}

// helperArgs 返回 helper 模式的位置参数（测试二进制以 `-- a b` 方式被调用时非空）。
func helperArgs() []string {
	return flagArgs()
}

// flagArgs 返回 flag.Parse 之后的位置参数（helper 模式判据：测试二进制以
// `-- <a> <b>` 方式被起起来时非空；正常 `go test` 跑起来时是空的）。
func flagArgs() []string { return flag.Args() }

// TestPolicyRejectsSelfContradictoryConfig 钉死"ACL 开而受限 token 关"必须被拒。
//
// 理由：受限 token 是文件约束的**来源**（它把用户 SID 转 deny-only，抽走子进程的全部
// 文件访问），ACL 只是把允许集的访问**发还**。关掉 token 只留 ACL = 一条约束都不生效，
// 却让装配方以为沙箱开着——双假象比没有更危险（V8 的同款教训）。
func TestPolicyRejectsSelfContradictoryConfig(t *testing.T) {
	bad := sandbox.DefaultPolicy()
	bad.UseRestrictedToken = false
	if err := bad.Validate(); err == nil {
		t.Error("UseRestrictedToken=false 居然通过了 Validate")
	}
	// 两个一起关是允许的（明确表示"我只要 job object 收容，不要文件约束"）。
	ok := bad
	ok.UseFileACL = false
	if err := ok.Validate(); err != nil {
		t.Errorf("两个一起关应该允许：%v", err)
	}
}
