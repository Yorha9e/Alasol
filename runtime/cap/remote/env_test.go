package remote_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/cap/local"
	"github.com/Yorha9e/alasol/runtime/cap/remote"
	"github.com/Yorha9e/alasol/runtime/tools"
)

// ---------------------------------------------------------------------------
// 假 ssh：记录 argv、不真连 SSH（生产 = os/exec，测试 = fake，见 ExecFunc 的说明）
// ---------------------------------------------------------------------------

// fakeSSH 是注入给 remote.Env 的假 ssh。四种模式：
//   - 默认：记录每次 (name, args)，返回预设 result/err；
//   - shell != ""：把收到的**远端命令**（最后一个参数）交给本机 shell 跑，
//     模拟"远端执行了这条命令"（K8 酸测试用，需本机有 sh）；
//     shellExtraEnv 用来给"远端 shell"加继承来的环境变量（模拟远端机器的 env），
//     shellDir 用来指定"远端"的工作目录（注入钉子把 marker 落在临时目录里，
//     不往仓库工作目录里丢垃圾文件）；
//   - hook != nil：自定义处理（ctx 取消测试用来阻塞到 ctx 结束）。
type fakeSSH struct {
	mu     sync.Mutex
	calls  [][]string // 每次 [name, args...]
	stdin  []byte     // 最近一次收到的 stdin（WriteFile 的内容面）
	result core.ExecResult
	err    error
	shell  string
	// shellExtraEnv 是"远端 shell"额外继承的环境变量（key=value）。
	shellExtraEnv []string
	// shellDir 是"远端"的工作目录（空 = 继承本测试进程的工作目录）。
	shellDir string
	hook     func(ctx context.Context, name string, args []string) (core.ExecResult, error)
}

func (f *fakeSSH) run(ctx context.Context, name string, args []string) (core.ExecResult, error) {
	return f.runStdin(ctx, name, args, nil)
}

// runStdin 是**带 stdin** 的注入面（Config.ExecStdin）：记录 argv **与 stdin**，
// 其余行为与 run 相同（hook / 本机 shell / 罐头结果）。Fs 的 WriteFile 走它——
// 文件内容必须经 stdin 管道送达（不进 argv）。
func (f *fakeSSH) runStdin(ctx context.Context, name string, args []string, stdin []byte) (core.ExecResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, append([]string{name}, args...))
	f.stdin = append([]byte(nil), stdin...)
	shell, hook, dir, extra := f.shell, f.hook, f.shellDir, f.shellExtraEnv
	f.mu.Unlock()

	if hook != nil {
		return hook(ctx, name, args)
	}
	if shell != "" {
		return runViaShell(shell, args[len(args)-1], dir, extra, stdin) // 远端命令是最后一个参数
	}
	return f.result, f.err
}

// lastStdin 返回最近一次调用收到的 stdin（副本）。
func (f *fakeSSH) lastStdin() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.stdin...)
}

func (f *fakeSSH) lastCall() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return nil
	}
	return f.calls[len(f.calls)-1]
}

// remoteCmdOf 取最近一次 ssh 调用里的远端命令（argv 最后一个元素）。
func (f *fakeSSH) remoteCmdOf() string {
	c := f.lastCall()
	if len(c) == 0 {
		return ""
	}
	return c[len(c)-1]
}

func (f *fakeSSH) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// runViaShell 用本机 sh -c 跑一条 POSIX 远端命令（酸测试里模拟远端执行）。
//
// dir 非空 = 用它当工作目录（"远端"的 cwd）；extraEnv 是"远端 shell"额外继承的
// 环境变量（模拟远端机器上 export 在 profile 里的东西）；stdin 非空 = 灌给这条
// 命令（WriteFile 的 `cat >` 从这里吃到文件内容——模拟 ssh 的 stdin 管道）。
func runViaShell(sh, remoteCmd, dir string, extraEnv []string, stdin []byte) (core.ExecResult, error) {
	cmd := exec.Command(sh, "-c", remoteCmd)
	if dir != "" {
		cmd.Dir = dir
	}
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	if len(stdin) > 0 {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	if err != nil {
		var ee *exec.ExitError
		if !asExit(err, &ee) {
			return core.ExecResult{}, err
		}
		return core.ExecResult{ExitCode: ee.ExitCode(), Stdout: out.Bytes(), Stderr: errb.Bytes()}, nil
	}
	return core.ExecResult{ExitCode: 0, Stdout: out.Bytes(), Stderr: errb.Bytes()}, nil
}

func asExit(err error, target **exec.ExitError) bool {
	if e, ok := err.(*exec.ExitError); ok {
		*target = e
		return true
	}
	return false
}

func shPath() string {
	if p, err := exec.LookPath("sh"); err == nil {
		return p
	}
	return ""
}

func newRemoteEnv(t *testing.T, fake *fakeSSH) *remote.Env {
	t.Helper()
	env, err := remote.New(remote.Config{Host: "user@harness", Exec: fake.run})
	if err != nil {
		t.Fatalf("remote.New: %v", err)
	}
	return env
}

// newRemoteEnvStdin 构造注入了**宽面执行缝**（Config.ExecStdin）的 env——
// WriteFile 的文件内容要经 stdin 送达，必须注入宽面；其余操作（Run/Read/Stat/List）
// 以空 stdin 走同一条缝（New 的接线：宽面是超集，只给它时全部经它）。
func newRemoteEnvStdin(t *testing.T, fake *fakeSSH) *remote.Env {
	t.Helper()
	env, err := remote.New(remote.Config{Host: "user@harness", ExecStdin: fake.runStdin})
	if err != nil {
		t.Fatalf("remote.New: %v", err)
	}
	return env
}

// ---------------------------------------------------------------------------
// shQuote：POSIX 单引号安全引用
// ---------------------------------------------------------------------------

// TestShQuoteStructure 在**外部**测试包里用一个"契约镜像"钉死 shQuote 的**结构不变量**。
//
// 真正被执行的生产 shQuote 由 exec_internal_test.go（package remote）直接单测——
// 破坏生产 shQuote 会让那个内部测试与下面的注入钉子**同时**红。这里的外部镜像
// 存在的意义是把"期望的精确字节"写成人可读的黄金样例（跨平台、确定性）。
func TestShQuoteStructureMirror(t *testing.T) {
	cases := []struct {
		in        string
		quotes    int // in 里的单引号个数
		wantExact string
	}{
		{"plain", 0, "'plain'"},
		{"a b\tc", 0, "'a b\tc'"},
		{"", 0, "''"},
		{"it's", 1, "'it'\\''s'"},
		{"a'; rm -rf /; '", 2, "'a'\\''; rm -rf /; '\\'''"},
	}
	for _, c := range cases {
		got := shQuoteProbe(c.in)
		if got != c.wantExact {
			t.Errorf("shQuote(%q) = %q，want %q", c.in, got, c.wantExact)
		}
		// 计数不变量：总单引号数 = 2（包裹）+ 3*（内部引号数）
		if n := strings.Count(got, "'"); n != 2+3*c.quotes {
			t.Errorf("shQuote(%q) = %q 有 %d 个单引号，want %d（转义失效？）", c.in, got, n, 2+3*c.quotes)
		}
	}
}

// shQuoteProbe 复刻 remote 包内的 shQuote（外部测试包无法访问未导出符号）。
// 与 remote/exec.go 的 shQuote 必须**逐字一致**——这是有意的"契约镜像"：
// 真正被执行的是 remote 里的版本（由 exec_internal_test.go 直接测），这里镜像它
// 来独立断言不变量与黄金字节。
func shQuoteProbe(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// ---------------------------------------------------------------------------
// 注入钉子：含 ; ' 空格 $ ` 的 argv 不得被远端 shell 拆成额外命令/字段
// ---------------------------------------------------------------------------

// TestRemoteRunInjectionParsesToArgv 是转义注入的钉子（跨平台、always-on）：
// 用假 ssh 捕获拼出的远端命令，再用一个最小 POSIX 切分器验证它被远端 shell 解析后
// **恰好**是 [Name, Args...]——元字符若逃出单引号就会被切成额外字段。
func TestRemoteRunInjectionParsesToArgv(t *testing.T) {
	fake := &fakeSSH{}
	env := newRemoteEnv(t, fake)
	ctx := context.Background()

	cases := [][]string{
		{"printf", "%s\n", "x; touch /PWNED"},
		{"echo", "a'; touch /PWNED; echo '"},
		{"echo", `it's "quoted" $HOME ` + "`id`" + ` | cat; rm -rf /`},
		{"echo", "spaces   and\ttabs"},
		{"run", "--flag=val;injected", "arg with spaces"},
	}
	for _, c := range cases {
		fake.mu.Lock()
		fake.calls = nil
		fake.mu.Unlock()
		if _, err := env.Run(ctx, core.ExecSpec{Name: c[0], Args: c[1:]}); err != nil {
			t.Fatalf("Run(%v): %v", c, err)
		}
		got := fake.remoteCmdOf()
		fields := shellFields(got)
		want := append([]string{c[0]}, c[1:]...)
		if !reflect.DeepEqual(fields, want) {
			t.Errorf("注入：remoteCmd=%q 被远端 shell 解析成 %q，want %q", got, fields, want)
		}
	}
}

// TestRemoteRunInjectionRealShell 是真 shell 的加强版（有 sh 才跑）：
// 把含 `'; touch <marker>` 的 argv 经假 ssh 捕获，再真用 sh 跑，
// 断言 marker **不出现**、stdout 是 payload 字面量。无 sh 的平台（Windows 裸环境）
// 由 TestRemoteRunInjectionParsesToArgv 的 POSIX 切分钉子覆盖。
//
// # marker 的写法（F9 的教训）
//
// marker 必须是 **POSIX 安全**的：早先的版本把 Windows 的临时目录路径
// （`C:\Users\...\Temp\...\PWNED`）直接塞进 shell payload——sh 把反斜杠当转义符，
// 路径被吃成一串 `C:UsersYorha...PWNED`，**在仓库工作目录里落了一个垃圾文件**。
//
// 现在的写法：
//
//   - marker = **纯文件名**（无分隔符、无反斜杠），且 shell 的工作目录 = 一个
//     临时目录（"远端"的 cwd）→ 万一注入成功，垃圾也只落在临时目录里；
//   - 额外断言临时目录**一个文件都没多**（不只查 marker 那一个名字）；
//   - 额外断言**本进程的工作目录**没有新增文件（兜住"路径被转义吃掉"那类事故）。
func TestRemoteRunInjectionRealShell(t *testing.T) {
	sh := shPath()
	if sh == "" {
		t.Skip("无 sh（Windows 裸环境）；注入语义由 TestRemoteRunInjectionParsesToArgv 钉住")
	}
	remoteCwd := t.TempDir() // "远端"的工作目录（注入成功也只脏这里）
	env := newRemoteEnv(t, &fakeSSH{shell: sh, shellDir: remoteCwd})
	marker := "alasol-injection-marker" // 纯文件名：POSIX 安全（无反斜杠、无分隔符）
	payload := "x'; touch " + marker + "; echo '"

	before := dirEntries(t, ".") // 本进程工作目录（通常是包目录）——不许新增文件
	res, err := env.Run(context.Background(), core.ExecSpec{Name: "echo", Args: []string{payload}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := os.Stat(filepath.Join(remoteCwd, marker)); !os.IsNotExist(err) {
		t.Fatalf("注入成功：marker %q 被创建了（转义失效）", marker)
	}
	if got := strings.TrimSpace(string(res.Stdout)); got != payload {
		t.Errorf("stdout = %q，want payload 字面量 %q（元字符应被单引号包住）", got, payload)
	}
	// 兜底一：远端 cwd 里不能多出任何文件（不只 marker 那一个名字）。
	if got := dirEntries(t, remoteCwd); len(got) != 0 {
		t.Errorf("远端工作目录多出文件（注入？）：%q", got)
	}
	// 兜底二：本进程工作目录不能多出文件（早先的反斜杠事故就落在这里）。
	if got := dirEntries(t, "."); !reflect.DeepEqual(got, before) {
		t.Errorf("当前工作目录出现新增/减少文件：before=%q after=%q", before, got)
	}
}

// dirEntries 列一个目录下的条目名（排序；不存在 → 空）。用于"没有新增文件"的断言。
func dirEntries(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读目录 %q: %v", dir, err)
	}
	out := make([]string, 0, len(ents))
	for _, e := range ents {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

// shellFields 是一个**最小 POSIX 单词切分器**（只处理单引号与引号外反斜杠转义——
// 正是 shQuote 产出的全部结构）。用途：测试里验证"拼接出的远端命令行被 shell 解析后
// 恰好是预期的 argv"。
//
// 自证：TestShellFieldsSanity 用已知输入钉住它自身正确。
func shellFields(line string) []string {
	var fields []string
	var buf strings.Builder
	inWord, inSingle := false, false
	for i := 0; i < len(line); i++ {
		c := line[i]
		if inSingle {
			if c == '\'' {
				inSingle = false
			} else {
				buf.WriteByte(c)
			}
			continue
		}
		switch c {
		case '\'':
			inSingle, inWord = true, true
		case '\\':
			// 引号外反斜杠：转义下一字符（shQuote 用 '\'' 嵌入单引号）
			if i+1 < len(line) {
				buf.WriteByte(line[i+1])
				inWord = true
				i++
			}
		case ' ', '\t':
			if inWord {
				fields = append(fields, buf.String())
				buf.Reset()
				inWord = false
			}
		default:
			buf.WriteByte(c)
			inWord = true
		}
	}
	if inWord {
		fields = append(fields, buf.String())
	}
	return fields
}

// TestShellFieldsSanity 钉死 shellFields 自身（防止"切分器有 bug 导致注入测试假绿"）。
func TestShellFieldsSanity(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"echo hi", []string{"echo", "hi"}},
		{"'echo' 'hi'", []string{"echo", "hi"}},
		{`'a'\''b'`, []string{"a'b"}},        // 嵌入单引号
		{"a  b\tc", []string{"a", "b", "c"}}, // 多空白折叠
		{"''", []string{""}},                 // 空引号 = 一个空字段
	}
	for _, c := range cases {
		if got := shellFields(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("shellFields(%q) = %q，want %q", c.in, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// ssh argv 形状 / Dir / Env 前缀
// ---------------------------------------------------------------------------

// TestRemoteRunSSHArgvShape 验证传给系统 ssh 的 argv 形状：
// [sshArgs…, 默认 -o 选项…, host, remoteCmd]，name="ssh"，远端命令是**单个**参数。
//
// 默认 -o 选项（BatchMode=yes / ConnectTimeout=10）必须在里面（F4）：远端若要求
// 交互式口令，ssh 会挂在等输入上——自动化执行里那是一段无声的无限等待。
func TestRemoteRunSSHArgvShape(t *testing.T) {
	fake := &fakeSSH{result: core.ExecResult{ExitCode: 0, Stdout: []byte("ok")}}
	env, err := remote.New(remote.Config{
		Host:    "deploy@harness",
		SSHArgs: []string{"-p", "2222"},
		Exec:    fake.run,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := env.Run(context.Background(), core.ExecSpec{Name: "echo", Args: []string{"hi"}}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	call := fake.lastCall()
	want := []string{
		"ssh", "-p", "2222",
		"-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
		"deploy@harness", "'echo' 'hi'",
	}
	if !reflect.DeepEqual(call, want) {
		t.Errorf("ssh argv = %q，want %q", call, want)
	}
}

// TestRemoteRunSSHArgsOverrideDefault 验证调用方显式给了同名 -o 选项时，
// **以调用方为准**（不出现"同一个选项给两遍"的歧义）。
func TestRemoteRunSSHArgsOverrideDefault(t *testing.T) {
	fake := &fakeSSH{}
	env, err := remote.New(remote.Config{
		Host:    "deploy@harness",
		SSHArgs: []string{"-o", "BatchMode=no", "-o", "ConnectTimeout=30"},
		Exec:    fake.run,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := env.Run(context.Background(), core.ExecSpec{Name: "echo"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	call := fake.lastCall()
	want := []string{
		"ssh", "-o", "BatchMode=no", "-o", "ConnectTimeout=30",
		"deploy@harness", "'echo'",
	}
	if !reflect.DeepEqual(call, want) {
		t.Errorf("ssh argv = %q，want %q", call, want)
	}
}

// TestRemoteRunDirEnvPrefix 验证 Dir 的 `cd … &&` 前缀与 Env 的 `env -i …` 前缀（均被引用）。
func TestRemoteRunDirEnvPrefix(t *testing.T) {
	fake := &fakeSSH{}
	env := newRemoteEnv(t, fake)
	if _, err := env.Run(context.Background(), core.ExecSpec{
		Name: "make",
		Args: []string{"build"},
		Dir:  "/srv/app dir",
		Env:  []string{"CC=clang", "MSG=hi there"},
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := fake.remoteCmdOf()
	want := "cd '/srv/app dir' && env -i 'CC=clang' 'MSG=hi there' 'PATH=/usr/bin:/bin' 'make' 'build'"
	if got != want {
		t.Errorf("remoteCmd =\n  %q\nwant\n  %q", got, want)
	}
}

// TestRemoteRunEnvIsFullReplacement 钉死 F1：远端命令的 env 是**整份替换**
// （`env -i`），不是叠加——远端 shell 继承来的环境（含远端机器上的凭据）不可见；
// 且 PATH 显式保留（否则远端找不到命令）。
//
// 做法：让假 ssh 把远端命令交给本机 sh 跑，并给这个"远端 shell"塞一个
// 带凭据形态的继承变量 → 远端命令里必须看不到它。
func TestRemoteRunEnvIsFullReplacement(t *testing.T) {
	sh := shPath()
	if sh == "" {
		t.Skip("无 sh（Windows 裸环境）；env 替换语义由 argv 形状钉子覆盖")
	}
	if _, err := exec.LookPath("env"); err != nil {
		t.Skip("无 env 可执行文件（POSIX 工具集不全）")
	}
	secret := "sk-remote-inherited-secret-1234567890"
	env := newRemoteEnv(t, &fakeSSH{shell: sh, shellExtraEnv: []string{"REMOTE_INHERITED_SECRET=" + secret}})

	// 远端命令 = env -i … 'env' → 打印**新环境**（不是叠加后的环境）。
	res, err := env.Run(context.Background(), core.ExecSpec{
		Name: "env",
		Env:  []string{"REMOTE_VISIBLE=1"},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := string(res.Stdout)
	if strings.Contains(out, secret) {
		t.Errorf("远端继承来的环境**漏进**了命令环境（env -i 失效 / 变成了叠加）：\n%s", out)
	}
	if !strings.Contains(out, "REMOTE_VISIBLE=1") {
		t.Errorf("显式列出的键应可见：\n%s", out)
	}
	if !strings.Contains(out, "PATH=") {
		t.Errorf("PATH 应被显式保留（env -i 清空环境后否则找不到命令）：\n%s", out)
	}
}

// harnessChildEnv 复刻 **runtime/tools.childEnv** 的形态：净化后的父 env
// （排序的 "K=V" 列表），含 **harness 本机**的 PATH（Windows 形态）与 HOME/USER。
//
// 这就是**生产路径** remote env 真实收到的 spec.Env（bash 工具把它整个传下来）。
func harnessChildEnv() []string {
	env := []string{
		"PATH=C:\\Windows\\System32;C:\\Windows;C:\\Program Files\\Git\\usr\\bin",
		"HOME=C:\\Users\\harness",
		"USER=harness",
		"TEMP=C:\\Users\\harness\\AppData\\Local\\Temp",
		"CC=clang",
	}
	sort.Strings(env)
	return env
}

// TestRemoteRunIgnoresHarnessIdentityEnv 钉死 F1（复查轮）：childEnv 形态的 env
// （带 harness 本机的 Windows 形态 PATH）送进来时，远端命令里的 PATH 必须是
// **远端侧**的（/usr/bin:/bin），不能是 harness 的那条。
//
// 背景：上一轮把 env 前缀改成 `env -i` 真替换后，PATH 兜底只在"调用方没给 PATH"时
// 才生效——而生产调用方（runtime/tools 的 bash 工具）恒从 os.Environ() 带出 harness 的
// PATH → 兜底不可达 → 远端命令带着 Windows 的 PATH → POSIX 远端一个命令都找不到
// （exit 127）。这是 env -i 修复引入的回归。
func TestRemoteRunIgnoresHarnessIdentityEnv(t *testing.T) {
	fake := &fakeSSH{}
	env := newRemoteEnv(t, fake)
	if _, err := env.Run(context.Background(), core.ExecSpec{Name: "ls", Env: harnessChildEnv()}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := fake.remoteCmdOf()
	if !strings.Contains(got, "'PATH=/usr/bin:/bin'") {
		t.Errorf("远端命令的 PATH 应是远端侧的 /usr/bin:/bin：%q", got)
	}
	if strings.Contains(got, "C:\\Windows") {
		t.Errorf("harness 的 PATH（Windows 形态）不该出现在远端命令里：%q", got)
	}
	// 机器身份键（HOME/USER/…）也不该被当成"远端的身份"送过去。
	for _, k := range []string{"HOME=", "USER="} {
		if strings.Contains(got, "'"+k) {
			t.Errorf("机器身份键 %q 应被剔除：%q", k, got)
		}
	}
	// 普通键原样保留（不能把调用方的配置也一起剔了）。
	if !strings.Contains(got, "'CC=clang'") {
		t.Errorf("普通键应原样保留：%q", got)
	}
}

// TestRemoteRunRemotePATHFindsCommand 有 sh 才跑：把远端命令真交给 POSIX shell 跑，
// `ls` 必须**找得到**（exit 0）——这正是 F1 回归的症状（远端 PATH 不对 → 127）。
func TestRemoteRunRemotePATHFindsCommand(t *testing.T) {
	sh := shPath()
	if sh == "" {
		t.Skip("无 sh（Windows 裸环境）；PATH 形状由 TestRemoteRunIgnoresHarnessIdentityEnv 钉住")
	}
	env := newRemoteEnv(t, &fakeSSH{shell: sh})
	res, err := env.Run(context.Background(), core.ExecSpec{Name: "ls", Env: harnessChildEnv()})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ExitCode != 0 {
		t.Errorf("远端 PATH（/usr/bin:/bin）下 ls 应能跑起来，exit=%d stderr=%s", res.ExitCode, res.Stderr)
	}
}

// TestRemoteRunDefaultRemotePATHOverrideChain 钉死 F3（终查轮）：**链路**钉子——
// `Config.DefaultRemotePATH` → `remote.New` → `Env.Run` → 远端命令里的 PATH。
//
// 为什么必须打**链路**而不是只测纯函数：只测 buildRemoteCommand 的参数位（纯函数）
// 堵不住"Env 装配时把 remotePATH 丢了/没往下传"这类**链路**断点（那正是 M13 的
// 变异缺口）：断链时纯函数照样对，但产品面永远用不上覆盖口。
//
// 覆盖口的现实意义：远端是 macOS/homebrew 时，默认的 /usr/bin:/bin 里**没有**
// /opt/homebrew/bin → 远端 brew 装的命令 127（见包 doc 的"产品面口径"一节）。
func TestRemoteRunDefaultRemotePATHOverrideChain(t *testing.T) {
	fake := &fakeSSH{}
	env, err := remote.New(remote.Config{
		Host:              "user@harness",
		DefaultRemotePATH: "/opt/homebrew/bin:/usr/bin:/bin",
		Exec:              fake.run,
	})
	if err != nil {
		t.Fatalf("remote.New: %v", err)
	}
	if _, err := env.Run(context.Background(), core.ExecSpec{
		Name: "ls",
		Env:  []string{"CC=clang"},
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := fake.remoteCmdOf()
	if !strings.Contains(got, "'PATH=/opt/homebrew/bin:/usr/bin:/bin'") {
		t.Errorf("Config.DefaultRemotePATH 应沿链路生效到远端命令的 PATH：%q", got)
	}
	if strings.Contains(got, "'PATH=/usr/bin:/bin'") {
		t.Errorf("覆盖后不该再用包内默认 PATH：%q", got)
	}
}

// TestRemoteRunRejectsCredentialEnv 钉死 F2/H12/H19：远端路径**不传凭据**——
// spec.Env 里出现凭据形态的键值 → 拒绝执行（连 ssh 都不起），错误可 errors.Is 判。
func TestRemoteRunRejectsCredentialEnv(t *testing.T) {
	cases := []struct {
		name string
		env  []string
	}{
		{"凭据键名", []string{"OPENAI_API_KEY=whatever-value"}},
		{"凭据值形态", []string{"MYVAR=sk-abcdefgh12345678"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeSSH{}
			env := newRemoteEnv(t, fake)
			_, err := env.Run(context.Background(), core.ExecSpec{Name: "echo", Env: c.env})
			if err == nil {
				t.Fatal("凭据 env 应被拒（远端路径不传凭据）")
			}
			if !errors.Is(err, remote.ErrCredentialEnv) {
				t.Errorf("应可 errors.Is(err, remote.ErrCredentialEnv)：%v", err)
			}
			if fake.callCount() != 0 {
				t.Error("凭据 env 不该触发 ssh 调用（fail-close 在起 ssh 之前）")
			}
			// 错误文案里**不许**出现凭据值（值是秘密）。
			if strings.Contains(err.Error(), "whatever-value") || strings.Contains(err.Error(), "sk-abcdefgh12345678") {
				t.Errorf("错误信息里出现了凭据值：%v", err)
			}
		})
	}
}

// TestRemoteRunRejectsCredentialArgs 钉死 F5（复查轮）：**args** 也一样——
// 参数值同样会被拼进 ssh argv（远端命令是**一个** argv 元素），凭据形态的参数
// 同样等于"在两台机器的进程列表里公开" → 拒绝执行（连 ssh 都不起）。
func TestRemoteRunRejectsCredentialArgs(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"参数值形态", []string{"--token", "sk-abcdefgh12345678"}},
		{"参数里带 K=V 形态", []string{"--token=sk-abcdefgh12345678"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeSSH{}
			env := newRemoteEnv(t, fake)
			_, err := env.Run(context.Background(), core.ExecSpec{Name: "echo", Args: c.args})
			if err == nil {
				t.Fatal("凭据形态的参数应被拒（远端路径 env 与 args 都不传凭据）")
			}
			if !errors.Is(err, remote.ErrCredentialEnv) {
				t.Errorf("应可 errors.Is(err, remote.ErrCredentialEnv)：%v", err)
			}
			if fake.callCount() != 0 {
				t.Error("凭据参数不该触发 ssh 调用（fail-close 在起 ssh 之前）")
			}
			// 错误文案里**不许**出现凭据值（值是秘密）。
			if strings.Contains(err.Error(), "sk-abcdefgh12345678") {
				t.Errorf("错误信息里出现了凭据值：%v", err)
			}
		})
	}
}

// TestRemoteRunRejectsEnvWithoutEquals 钉死 F6：Env 条目没有 '=' → fail-close 点名报错。
func TestRemoteRunRejectsEnvWithoutEquals(t *testing.T) {
	fake := &fakeSSH{}
	env := newRemoteEnv(t, fake)
	_, err := env.Run(context.Background(), core.ExecSpec{Name: "echo", Env: []string{"NOEQUALS"}})
	if err == nil {
		t.Fatal("Env 条目缺 '=' 应报错")
	}
	if !strings.Contains(err.Error(), "NOEQUALS") {
		t.Errorf("错误应点名那条条目：%v", err)
	}
	if fake.callCount() != 0 {
		t.Error("畸形 Env 不该触发 ssh 调用")
	}
}

// ---------------------------------------------------------------------------
// 语义对齐 local/sandbox：非零退出不是 error、空 Name 是 error、ctx 取消传播
// ---------------------------------------------------------------------------

// TestRemoteExitCodePassthrough 验证 ssh 非零退出（含远端命令非零透出）不是 error。
func TestRemoteExitCodePassthrough(t *testing.T) {
	env := newRemoteEnv(t, &fakeSSH{result: core.ExecResult{ExitCode: 7, Stderr: []byte("boom")}})
	res, err := env.Run(context.Background(), core.ExecSpec{Name: "false"})
	if err != nil {
		t.Fatalf("非零退出不该是 error：%v", err)
	}
	if res.ExitCode != 7 {
		t.Errorf("ExitCode = %d，want 7", res.ExitCode)
	}
}

// TestRemoteSSHTransportFailure255 钉死 F5：ssh 自己失败（exit 255 + 特征 stderr）
// → 返回可 errors.Is 判的 ErrSSHTransport（"没连上"与"远端跑挂了"必须分得开）。
//
// 覆盖**注入 exec** 这条路径（生产路径 osExec 的同款判定由内部测试钉住）。
func TestRemoteSSHTransportFailure255(t *testing.T) {
	env := newRemoteEnv(t, &fakeSSH{result: core.ExecResult{
		ExitCode: 255,
		Stderr:   []byte("ssh: connect to host harness port 22: Connection refused\n"),
	}})
	res, err := env.Run(context.Background(), core.ExecSpec{Name: "true"})
	if err == nil {
		t.Fatal("ssh 传输失败应返回 error")
	}
	if !errors.Is(err, remote.ErrSSHTransport) {
		t.Errorf("应可 errors.Is(err, remote.ErrSSHTransport)：%v", err)
	}
	if res.ExitCode != 255 {
		t.Errorf("ExitCode = %d，want 255", res.ExitCode)
	}
	if !strings.Contains(err.Error(), "Connection refused") {
		t.Errorf("错误应带 stderr 片段：%v", err)
	}
}

// TestRemoteRemoteExit255NotTransportFailure 钉死 F5 的另一半：远端命令自己退出 255
// （stderr 无 ssh 特征）→ 仍按退出码透出，**不**误报成"没连上"。
func TestRemoteRemoteExit255NotTransportFailure(t *testing.T) {
	env := newRemoteEnv(t, &fakeSSH{result: core.ExecResult{
		ExitCode: 255,
		Stderr:   []byte("make: *** [all] Error 255\n"),
	}})
	res, err := env.Run(context.Background(), core.ExecSpec{Name: "make"})
	if err != nil {
		t.Fatalf("远端命令退出 255 不该是 error：%v", err)
	}
	if res.ExitCode != 255 {
		t.Errorf("ExitCode = %d，want 255", res.ExitCode)
	}
}

// TestRemoteEmptyName 验证空 Name → error（不猜可执行文件）。
func TestRemoteEmptyName(t *testing.T) {
	fake := &fakeSSH{}
	env := newRemoteEnv(t, fake)
	if _, err := env.Run(context.Background(), core.ExecSpec{Name: "   "}); err == nil {
		t.Error("空 Name 应该报错")
	}
	if fake.callCount() != 0 {
		t.Error("空 Name 不该触发 ssh 调用")
	}
}

// TestRemoteContextCancel 验证 ctx 取消传播：假 ssh 阻塞到 ctx 结束，
// Run 必须及时返回 error（本地那条 ssh 会被 Cancel 收掉）。
func TestRemoteContextCancel(t *testing.T) {
	fake := &fakeSSH{hook: func(ctx context.Context, _ string, _ []string) (core.ExecResult, error) {
		<-ctx.Done() // 模拟 ssh 挂起，直到 ctx 取消
		return core.ExecResult{}, ctx.Err()
	}}
	env := newRemoteEnv(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := env.Run(ctx, core.ExecSpec{Name: "sleep", Args: []string{"60"}})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("取消后 Run 应该返回 error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("取消后 Run 没有及时返回")
	}
}

// TestRemoteContextAlreadyDone 验证已取消的 ctx 直接报错（不碰 ssh）。
func TestRemoteContextAlreadyDone(t *testing.T) {
	fake := &fakeSSH{}
	env := newRemoteEnv(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := env.Run(ctx, core.ExecSpec{Name: "echo"}); err == nil {
		t.Error("已取消的 ctx 应该直接报错")
	}
	if fake.callCount() != 0 {
		t.Error("已取消的 ctx 不该触发 ssh 调用")
	}
}

// ---------------------------------------------------------------------------
// 预留位：Proc / Net 显式 ErrUnsupported（Fs 已落地，见下方 Fs 能力面一节）
// ---------------------------------------------------------------------------

// TestRemoteProcNetStillUnsupportedFaces 验证 Proc / Net **仍**显式 ErrUnsupported
// （防回归：Fs 落地后这两个面必须继续诚实报未支持），且 Fs 四方法**不再**撞上
// 预留位哨兵（已落地——哪怕执行缝是空壳罐头，也不该返回 ErrUnsupported）。
//
// 断言两件事：错误**说清了是预留位**（人可读），且**可 errors.Is 判**
// （remote.ErrUnsupported 是导出哨兵——调用方要按能力面分流时用得上）。
func TestRemoteProcNetStillUnsupportedFaces(t *testing.T) {
	env := newRemoteEnv(t, &fakeSSH{})
	ctx := context.Background()
	check := func(name string, err error) {
		if err == nil {
			t.Errorf("%s 应该返回 error（预留位）", name)
			return
		}
		if !strings.Contains(err.Error(), "预留位") {
			t.Errorf("%s 错误信息没说清是预留位：%v", name, err)
		}
		if !errors.Is(err, remote.ErrUnsupported) {
			t.Errorf("%s 应可 errors.Is(err, remote.ErrUnsupported)：%v", name, err)
		}
	}
	_, e := env.Do(ctx, nil)
	check("Do", e)
	_, e = env.Start(ctx, core.ExecSpec{Name: "x"})
	check("Start", e)

	// Fs 四方法：预留位哨兵**一票否决**（出现即 Fs被错误地打回了预留位）。
	fake := &fakeSSH{}
	envFS := newRemoteEnvStdin(t, fake)
	if _, err := envFS.ReadFile(ctx, "/x"); errors.Is(err, remote.ErrUnsupported) {
		t.Errorf("ReadFile 不该再是预留位：%v", err)
	}
	if err := envFS.WriteFile(ctx, "/x", nil); errors.Is(err, remote.ErrUnsupported) {
		t.Errorf("WriteFile 不该再是预留位：%v", err)
	}
	if _, err := envFS.Stat(ctx, "/x"); errors.Is(err, remote.ErrUnsupported) {
		t.Errorf("Stat 不该再是预留位：%v", err)
	}
	if _, err := envFS.ListDir(ctx, "/x"); errors.Is(err, remote.ErrUnsupported) {
		t.Errorf("ListDir 不该再是预留位：%v", err)
	}
}

// ---------------------------------------------------------------------------
// Fs 能力面（经 ssh exec：cat / stat / ls；不真连 ssh，断命令行与错误映射）
// ---------------------------------------------------------------------------

// fsCallCase 描述一个 Fs 方法：怎么调、期望的远端命令行、以及该命令行被远端
// shell 解析后的 argv（注入钉子：元字符逃出单引号就会多出字段）。
type fsCallCase struct {
	name      string
	call      func(env *remote.Env, path string) error
	remoteCmd func(path string) string
	wantArgv  func(path string) []string
}

// fsEnvPrefixProbe 是 Fs 四条命令的 **env 前缀**期望形态（外部测试包的契约镜像）：
// `env -i 'PATH=/usr/bin:/bin'`——与 Run 同款（整份替换 + 远端侧 PATH）。
//
// 远端侧 PATH 的值取包内默认（defaultRemotePATH="/usr/bin:/bin"）；DefaultRemotePATH
// 覆盖对 Fs 同样生效——那条由 TestRemoteFsUsesEnvIAndRemotePATH 单独钉（链路钉子）。
const fsEnvPrefixProbe = "env -i 'PATH=/usr/bin:/bin'"

// fsCallCases 四方法各一档（全部经 newRemoteEnvStdin 注入的执行缝——宽面是
// 超集，空 stdin 的操作也走它）。
func fsCallCases() []fsCallCase {
	return []fsCallCase{
		{
			name: "ReadFile",
			call: func(env *remote.Env, p string) error {
				_, err := env.ReadFile(context.Background(), p)
				return err
			},
			remoteCmd: func(p string) string {
				return fsEnvPrefixProbe + " 'cat' '--' " + shQuoteProbe(p)
			},
			wantArgv: func(p string) []string {
				return []string{"env", "-i", "PATH=/usr/bin:/bin", "cat", "--", p}
			},
		},
		{
			name: "WriteFile",
			call: func(env *remote.Env, p string) error {
				return env.WriteFile(context.Background(), p, []byte("payload"))
			},
			remoteCmd: func(p string) string {
				return fsEnvPrefixProbe + " 'cat' > " + shQuoteProbe(p)
			},
			wantArgv: func(p string) []string {
				return []string{"env", "-i", "PATH=/usr/bin:/bin", "cat", ">", p}
			},
		},
		{
			name: "Stat",
			call: func(env *remote.Env, p string) error {
				_, err := env.Stat(context.Background(), p)
				return err
			},
			remoteCmd: func(p string) string {
				return fsEnvPrefixProbe + " 'stat' '-c' '%s|%Y|%F' '--' " + shQuoteProbe(p)
			},
			wantArgv: func(p string) []string {
				return []string{"env", "-i", "PATH=/usr/bin:/bin", "stat", "-c", "%s|%Y|%F", "--", p}
			},
		},
		{
			name: "ListDir",
			call: func(env *remote.Env, p string) error {
				_, err := env.ListDir(context.Background(), p)
				return err
			},
			remoteCmd: func(p string) string {
				return fsEnvPrefixProbe + " 'ls' '-1A' '--' " + shQuoteProbe(p)
			},
			wantArgv: func(p string) []string {
				return []string{"env", "-i", "PATH=/usr/bin:/bin", "ls", "-1A", "--", p}
			},
		},
	}
}

// fsPathMatrix 是路径转义矩阵：空格 / 单引号 / 分号 / $(...) / 反引号 / 注入串 /
// 前导 '-'（靠 `--` 挡成文件名而不是选项）。
//
// ⚠ 含**真以 '-' 开头的相对路径**（`-rf` 这类）：它们**整个**就是选项的样子——
// `--` 分隔符必须让远端命令把它们当**文件名**（相对路径基准 = 远端 shell 的 cwd，
// 同 Exec 的 Dir 语义差），而不是当选项解析（那才是真正的注入面）。
func fsPathMatrix() []string {
	return []string{
		"/data/plain.txt",
		"/data/with space.txt",
		"/data/it's.txt",
		"/data/a; touch PWNED.txt",
		"/data/$(id).txt",
		"/data/`id`.txt",
		`/data/x'; touch PWNED; echo '`,
		"/data/-leading-dash.txt",
		// —— 真以 '-' 开头的相对路径（整个路径就是选项的样子）——
		"-rf",
		"--",
		"-",
		"-rf/child.txt",
	}
}

// TestRemoteFsCommandLineQuoting 钉死四方法的命令行拼装 + 路径转义矩阵：
//
//   - 远端命令行逐字节等于黄金样例（shQuoteProbe 镜像 shQuote 的期望字节）；
//   - 且被"远端 shell"解析后**恰好**是期望 argv——路径里的 空格/单引号/分号/
//     $(...)/反引号 若逃出单引号，会切成额外字段（shellFields 多字段即红）。
//
// 这就是"路径含单引号/空格/分号的命令行拼装"钉子（变异自证①的红灯所在）。
func TestRemoteFsCommandLineQuoting(t *testing.T) {
	for _, fc := range fsCallCases() {
		t.Run(fc.name, func(t *testing.T) {
			fake := &fakeSSH{} // 罐头 exit 0：本测试只关心命令行
			env := newRemoteEnvStdin(t, fake)
			for _, p := range fsPathMatrix() {
				fake.mu.Lock()
				fake.calls = nil
				fake.mu.Unlock()
				_ = fc.call(env, p) // 解析类错误（Stat 空罐头）不影响命令行断言
				got := fake.remoteCmdOf()
				if want := fc.remoteCmd(p); got != want {
					t.Errorf("path=%q 远端命令行 =\n  %q\nwant\n  %q", p, got, want)
				}
				if fields := shellFields(got); !reflect.DeepEqual(fields, fc.wantArgv(p)) {
					t.Errorf("path=%q 被远端 shell 解析成 %q，want %q（转义失效？）", p, fields, fc.wantArgv(p))
				}
			}
		})
	}
}

// TestRemoteFsReadFileBinaryPassthrough 钉死 ReadFile 的**二进制安全**：
// stdout 原样字节返回（NUL / 0xff / CRLF 逐字节透传，不经文本转换）。
func TestRemoteFsReadFileBinaryPassthrough(t *testing.T) {
	payload := []byte{0x00, 0x01, 0x7f, 0x80, 0xff, '\r', '\n', 'h', 'i', 0x00}
	fake := &fakeSSH{result: core.ExecResult{ExitCode: 0, Stdout: append([]byte(nil), payload...)}}
	env := newRemoteEnvStdin(t, fake)
	got, err := env.ReadFile(context.Background(), "/bin/data.bin")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("字节不逐字节相等（发生了文本转换/截断？）：got=%v want=%v", got, payload)
	}
}

// TestRemoteFsWriteFileStdinBinary 钉死 WriteFile 的内容**走 ssh stdin 管道**：
//
//   - stdin 逐字节等于 data（含 NUL/0xff/CRLF 的二进制）；
//   - 内容**不落 argv**（远端命令行是固定形状 env -i 'PATH=…' 'cat' > '<path>'，无 payload 痕迹）。
func TestRemoteFsWriteFileStdinBinary(t *testing.T) {
	data := []byte{0x00, 0xff, 0xfe, '\r', '\n', 'p', 'w', 0x00}
	fake := &fakeSSH{}
	env := newRemoteEnvStdin(t, fake)
	if err := env.WriteFile(context.Background(), "/data/out.bin", data); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if got := fake.lastStdin(); !bytes.Equal(got, data) {
		t.Errorf("stdin = %v，want %v（内容没走 stdin 管道？）", got, data)
	}
	want := fsEnvPrefixProbe + " 'cat' > '/data/out.bin'"
	if got := fake.remoteCmdOf(); got != want {
		t.Errorf("远端命令行 = %q，want %q", got, want)
	}
	// argv 里不许出现 payload 的特征字节（"pw"）——内容落 argv = 两端进程表公开。
	if joined := strings.Join(fake.lastCall(), ""); strings.Contains(joined, "pw") {
		t.Errorf("文件内容落进了 ssh argv：%q", joined)
	}
}

// TestRemoteFsExitCodeError 钉死 Fs 的错误口径：远端非 0 退出 → 明文错误
// （退出码 + 截断 stderr + path），且不误判成传输失败（FSCapability 没有
// ExitCode 可带——与 Exec 的"非零不是 error"不同档）。
func TestRemoteFsExitCodeError(t *testing.T) {
	fake := &fakeSSH{result: core.ExecResult{
		ExitCode: 1,
		Stderr:   []byte("cat: /x: No such file or directory\n"),
	}}
	env := newRemoteEnvStdin(t, fake)
	ctx := context.Background()
	cases := []struct {
		name string
		call func() error
	}{
		{"ReadFile", func() error { _, err := env.ReadFile(ctx, "/x"); return err }},
		{"WriteFile", func() error { return env.WriteFile(ctx, "/x", []byte("d")) }},
		{"Stat", func() error { _, err := env.Stat(ctx, "/x"); return err }},
		{"ListDir", func() error { _, err := env.ListDir(ctx, "/x"); return err }},
	}
	for _, c := range cases {
		err := c.call()
		if err == nil {
			t.Errorf("%s: 远端 exit 1 应报错", c.name)
			continue
		}
		msg := err.Error()
		if !strings.Contains(msg, "退出码 1") {
			t.Errorf("%s: 错误应含退出码：%v", c.name, err)
		}
		if !strings.Contains(msg, "No such file or directory") {
			t.Errorf("%s: 错误应含截断 stderr：%v", c.name, err)
		}
		if !strings.Contains(msg, "/x") {
			t.Errorf("%s: 错误应含 path：%v", c.name, err)
		}
		if errors.Is(err, remote.ErrSSHTransport) {
			t.Errorf("%s: 退出码 1 不是传输失败：%v", c.name, err)
		}
	}
}

// TestRemoteFsSSHTransport255 钉死 Fs 的传输失败映射与 Exec 同款：
// exit 255 + stderr 首行 ssh 强特征 → errors.Is(err, remote.ErrSSHTransport)。
func TestRemoteFsSSHTransport255(t *testing.T) {
	fake := &fakeSSH{result: core.ExecResult{
		ExitCode: 255,
		Stderr:   []byte("ssh: connect to host harness port 22: Connection refused\n"),
	}}
	env := newRemoteEnvStdin(t, fake)
	ctx := context.Background()
	cases := []struct {
		name string
		call func() error
	}{
		{"ReadFile", func() error { _, err := env.ReadFile(ctx, "/x"); return err }},
		{"WriteFile", func() error { return env.WriteFile(ctx, "/x", []byte("d")) }},
		{"Stat", func() error { _, err := env.Stat(ctx, "/x"); return err }},
		{"ListDir", func() error { _, err := env.ListDir(ctx, "/x"); return err }},
	}
	for _, c := range cases {
		err := c.call()
		if err == nil {
			t.Errorf("%s: ssh 传输失败应报错", c.name)
			continue
		}
		if !errors.Is(err, remote.ErrSSHTransport) {
			t.Errorf("%s: 应可 errors.Is(err, remote.ErrSSHTransport)：%v", c.name, err)
		}
		if !strings.Contains(err.Error(), "Connection refused") {
			t.Errorf("%s: 错误应带 stderr 片段：%v", c.name, err)
		}
	}
}

// TestRemoteStatGNUShape 钉死 Stat 的 GNU 形状解析 → core.FileInfo 字段对
// （Size / IsDir / ModTime 逐字段）。
func TestRemoteStatGNUShape(t *testing.T) {
	cases := []struct {
		name   string
		stdout string
		want   core.FileInfo
	}{
		{"普通文件", "1234|1700000000|regular file\n",
			core.FileInfo{Size: 1234, ModTime: time.Unix(1700000000, 0)}},
		{"目录", "4096|0|directory\n",
			core.FileInfo{Size: 4096, IsDir: true, ModTime: time.Unix(0, 0)}},
		{"符号链接", "10|1712345678|symbolic link\n",
			core.FileInfo{Size: 10, ModTime: time.Unix(1712345678, 0)}},
		{"空普通文件（GNU %F 措辞带空格）", "0|1712345678|regular empty file\n",
			core.FileInfo{Size: 0, ModTime: time.Unix(1712345678, 0)}},
		{"epoch 前 mtime（负秒）", "0|-86400|regular file\n",
			core.FileInfo{Size: 0, ModTime: time.Unix(-86400, 0)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeSSH{result: core.ExecResult{ExitCode: 0, Stdout: []byte(c.stdout)}}
			env := newRemoteEnvStdin(t, fake)
			fi, err := env.Stat(context.Background(), "/p")
			if err != nil {
				t.Fatalf("Stat(%q): %v", c.stdout, err)
			}
			if fi.Size != c.want.Size || fi.IsDir != c.want.IsDir || !fi.ModTime.Equal(c.want.ModTime) {
				t.Errorf("FileInfo = {Size:%d IsDir:%v ModTime:%v}，want {Size:%d IsDir:%v ModTime:%v}",
					fi.Size, fi.IsDir, fi.ModTime, c.want.Size, c.want.IsDir, c.want.ModTime)
			}
		})
	}
}

// TestRemoteStatBadShapeFailsClosed 钉死 Stat 的**坏形状不瞎猜**：任何不是
// '大小|mtime秒|类型' 三段（或段内非整数 / 空类型 / 混入换行回车）的输出 →
// 明文错误（含"非 GNU 口径"提示）+ 零值 FileInfo。变异自证②的红灯所在。
func TestRemoteStatBadShapeFailsClosed(t *testing.T) {
	bad := []string{
		"",
		"42",
		"1|2",
		"1|2|directory|extra",
		"abc|def|ghi",
		"notnumber|1700000000|directory",
		"1234|notanumber|directory",
		"1|1700000000|",
		"total 123\n1|2|directory", // 首行不是数据（别的工具混进 stdout）
		"1|2|directory\r",          // CRLF 形态不许静默吞成 IsDir=false
	}
	for _, out := range bad {
		fake := &fakeSSH{result: core.ExecResult{ExitCode: 0, Stdout: []byte(out)}}
		env := newRemoteEnvStdin(t, fake)
		fi, err := env.Stat(context.Background(), "/p")
		if err == nil {
			t.Errorf("stdout=%q 应报错（不瞎解析），得到 %+v", out, fi)
			continue
		}
		if !strings.Contains(err.Error(), "非 GNU 口径") {
			t.Errorf("stdout=%q 错误应点明非 GNU 口径提示：%v", out, err)
		}
		if fi != (core.FileInfo{}) {
			t.Errorf("stdout=%q 出错时应返回零值 FileInfo，得到 %+v", out, fi)
		}
	}
}

// TestRemoteStatNonGNUStderr 钉死非 GNU 的**明确报错**：BSD/macOS 的 stat 不认
// -c（"stat: illegal option"）→ "远端 stat 非 GNU 口径"；通用报错不误判。
func TestRemoteStatNonGNUStderr(t *testing.T) {
	fake := &fakeSSH{result: core.ExecResult{
		ExitCode: 1,
		Stderr:   []byte("stat: illegal option -- c\nusage: stat [-FLnqrs] [-f format] file ...\n"),
	}}
	env := newRemoteEnvStdin(t, fake)
	_, err := env.Stat(context.Background(), "/p")
	if err == nil {
		t.Fatal("非 GNU stat 应报错")
	}
	if !strings.Contains(err.Error(), "远端 stat 非 GNU 口径") {
		t.Errorf("错误应点名远端 stat 非 GNU 口径：%v", err)
	}

	// 通用报错（文件不存在）**不**误判成非 GNU——按普通退出码错误透出。
	fake2 := &fakeSSH{result: core.ExecResult{
		ExitCode: 1,
		Stderr:   []byte("stat: cannot statx '/p': No such file or directory\n"),
	}}
	env2 := newRemoteEnvStdin(t, fake2)
	_, err2 := env2.Stat(context.Background(), "/p")
	if err2 == nil {
		t.Fatal("stat 失败应报错")
	}
	if strings.Contains(err2.Error(), "非 GNU 口径") {
		t.Errorf("普通 stat 失败不该被误判成非 GNU：%v", err2)
	}
	if !strings.Contains(err2.Error(), "退出码 1") {
		t.Errorf("应按普通退出码错误透出：%v", err2)
	}
}

// TestRemoteListDirLines 钉死 ListDir 的逐行拆分（含换行文件名的诚实边界）。
func TestRemoteListDirLines(t *testing.T) {
	cases := []struct {
		name   string
		stdout string
		want   []string
	}{
		{"多行", "a.txt\nb dir\nc'd\n", []string{"a.txt", "b dir", "c'd"}},
		{"空目录（零字节）", "", []string{}},
		{"无尾换行", "only", []string{"only"}},
		{"注入字符名字原样保留", "x'; touch PWNED; echo '\n$(id)\n",
			[]string{"x'; touch PWNED; echo '", "$(id)"}},
		// 诚实边界：文件名含换行**会错拆**（一行一个名字的口径天然如此）——
		// 这条把已知边界固定下来，别让后人"修"成静默吞行。
		{"换行文件名错拆（已知边界）", "a\nb\n", []string{"a", "b"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeSSH{result: core.ExecResult{ExitCode: 0, Stdout: []byte(c.stdout)}}
			env := newRemoteEnvStdin(t, fake)
			got, err := env.ListDir(context.Background(), "/d")
			if err != nil {
				t.Fatalf("ListDir: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("ListDir(%q) = %q，want %q", c.stdout, got, c.want)
			}
		})
	}
}

// TestRemoteFsContextCanceled 钉死 Fs 的 ctx 口径与 Run 同款：已取消的 ctx
// 直接报错、不碰 ssh。
func TestRemoteFsContextCanceled(t *testing.T) {
	fake := &fakeSSH{}
	env := newRemoteEnvStdin(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := env.ReadFile(ctx, "/x"); err == nil {
		t.Error("已取消的 ctx 应报错（ReadFile）")
	}
	if err := env.WriteFile(ctx, "/x", []byte("d")); err == nil {
		t.Error("已取消的 ctx 应报错（WriteFile）")
	}
	if fake.callCount() != 0 {
		t.Error("已取消的 ctx 不该碰 ssh")
	}
}

// TestRemoteFsWriteFileOnlyNarrowExecFailClose 钉死执行缝接线口径：只注入
// **窄面** Config.Exec（无 stdin）时，WriteFile 对**非空 data** fail-close
// 报错（绝不静默丢字节写出空文件）；空 data 不需要 stdin → 窄面照常可用。
func TestRemoteFsWriteFileOnlyNarrowExecFailClose(t *testing.T) {
	fake := &fakeSSH{}
	env := newRemoteEnv(t, fake) // 只注入 Exec（窄面）
	err := env.WriteFile(context.Background(), "/x", []byte("payload"))
	if err == nil {
		t.Fatal("窄面执行缝送不了非空内容，应 fail-close 报错")
	}
	if !strings.Contains(err.Error(), "stdin") {
		t.Errorf("错误应点名 stdin/ExecStdin：%v", err)
	}
	if fake.callCount() != 0 {
		t.Error("fail-close 不该起 ssh")
	}
	// 空 data：不需要 stdin 管道 → 窄面照常执行（远端 `cat >` 截断成空文件）。
	if err := env.WriteFile(context.Background(), "/x", nil); err != nil {
		t.Errorf("空 data 应可经窄面执行：%v", err)
	}
	if fake.callCount() != 1 {
		t.Errorf("空 data 应真的起了 ssh 一次，实际 %d 次", fake.callCount())
	}
}

// TestRemoteFsRoundTripRealShell 有 sh 才跑：把远端命令真交给本机 POSIX shell——
// WriteFile 的 stdin 管道 + ReadFile 的字节透传 + ListDir 拆行一次串起来。
// 文件名含空格/单引号/分号/`$(...)`：转义若失效，写读的名字对不上、或注入物
// 在临时目录里落地（兜底断言：目录里**只**有那一个文件）。
func TestRemoteFsRoundTripRealShell(t *testing.T) {
	sh := shPath()
	if sh == "" {
		t.Skip("无 sh（Windows 裸环境）；命令行拼装由 TestRemoteFsCommandLineQuoting 钉住")
	}
	remoteCwd := t.TempDir()
	fake := &fakeSSH{shell: sh, shellDir: remoteCwd}
	env := newRemoteEnvStdin(t, fake)
	name := `a b'c;$(id).txt` // 纯文件名（无路径分隔符）：POSIX 与 Windows 文件系统都收
	data := []byte{0x00, 0xff, 0xfe, '\r', '\n', 'h', 'i', 0x00}

	if err := env.WriteFile(context.Background(), name, data); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got, err := env.ReadFile(context.Background(), name)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("二进制往返不等：got=%v want=%v", got, data)
	}
	names, err := env.ListDir(context.Background(), ".")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if !reflect.DeepEqual(names, []string{name}) {
		t.Errorf("ListDir = %q，want %q", names, []string{name})
	}
	// 注入兜底：$(id) 若被展开、或 ; 拆出新命令，临时目录会多出条目/名字对不上。
	if ents := dirEntries(t, remoteCwd); !reflect.DeepEqual(ents, []string{name}) {
		t.Errorf("远端工作目录条目 = %q，want [%q]（转义/注入事故？）", ents, name)
	}
}

// TestRemoteFsStatRealShell 有 sh 且"远端"模拟环境有 **GNU stat** 才跑：
// 真解析 stat 输出 → FileInfo 字段对。非 GNU（BSD/macOS 的 stat 不认 -c）
// → 探路失败即跳过——那正是"远端 stat 非 GNU 口径"的诚实边界。
func TestRemoteFsStatRealShell(t *testing.T) {
	sh := shPath()
	if sh == "" {
		t.Skip("无 sh（Windows 裸环境）；Stat 形状由 TestRemoteStatGNUShape 钉住")
	}
	remoteCwd := t.TempDir()
	env := newRemoteEnvStdin(t, &fakeSSH{shell: sh, shellDir: remoteCwd})

	fiDir, err := env.Stat(context.Background(), ".")
	if err != nil {
		t.Skipf("远端模拟环境无可用 GNU stat（非 GNU 口径边界）：%v", err)
	}
	if !fiDir.IsDir {
		t.Errorf("Stat('.') 应是目录：%+v", fiDir)
	}
	want := []byte("hello remote stat")
	if err := env.WriteFile(context.Background(), "f.bin", want); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	fi, err := env.Stat(context.Background(), "f.bin")
	if err != nil {
		t.Fatalf("Stat(f.bin): %v", err)
	}
	if fi.Size != int64(len(want)) || fi.IsDir {
		t.Errorf("FileInfo = {Size:%d IsDir:%v}，want {Size:%d IsDir:false}",
			fi.Size, fi.IsDir, len(want))
	}
}

// TestRemoteNewRejectsEmptyHost 验证空 Host → fail-close 报错。
func TestRemoteNewRejectsEmptyHost(t *testing.T) {
	if _, err := remote.New(remote.Config{}); err == nil {
		t.Error("空 Host 应该被拒（fail-close）")
	}
}

// TestRemoteNewRejectsBadHost 验证 F13：Host 不以 '-' 开头（会被 ssh 当成选项注入）、
// 不含空白 → 一律 fail-close 拒绝。
func TestRemoteNewRejectsBadHost(t *testing.T) {
	cases := []struct {
		name string
		host string
		why  string
	}{
		{"以 - 开头", "-oProxyCommand=touch /tmp/pwn", "会被 ssh 当成选项"},
		{"含空格", "user@host extra", "不是合法的 ssh 目标"},
		{"含制表", "user@\thost", "不是合法的 ssh 目标"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := remote.New(remote.Config{Host: c.host}); err == nil {
				t.Errorf("Host %q 应该被拒（%s）", c.host, c.why)
			}
		})
	}
	// 正常目标仍可用。
	if _, err := remote.New(remote.Config{Host: "user@harness"}); err != nil {
		t.Errorf("正常 Host 应可用：%v", err)
	}
}

// ---------------------------------------------------------------------------
// K8 酸测试：换 env 实现，agent / 工具代码零改动
// ---------------------------------------------------------------------------

// TestK8SwapEnvRemoteZeroAgentChange 是 K8 的酸测试：**换 env 实现，工具代码零改动**。
//
// 做法对照 runtime/cap/sandbox 的同款：同一份 tools.NewBash(...) 构造代码，分别喂
// local.Exec 与 remote.Env，跑同一条调用。remote 侧经假 ssh 执行——
//   - 有 sh 的平台（Linux/macOS CI）：假 ssh 真在本机跑远端命令 → 与 local 逐字节比对；
//   - 无 sh 的平台（Windows 裸环境）：假 ssh 回灌固定回显 → 只验"接口可替换 + 输出透传"。
//
// 被测的是"装配侧能不能一对一替换"，不是 SSH 连接本身（那个不在此刀，也不真连）。
func TestK8SwapEnvRemoteZeroAgentChange(t *testing.T) {
	// 只有 **POSIX 平台且有 sh** 时才让假 ssh 真在本机跑远端命令：bash 工具在 Unix 产
	// `bash -c`（POSIX），能被 sh 执行；Windows 产 `cmd /c`（非 POSIX），即便有 sh
	// （Git Bash）也跑不动 → Windows 一律走罐头回显，只验"接口可替换 + 输出透传"。
	sh := shPath()
	var fake *fakeSSH
	if sh != "" && runtime.GOOS != "windows" {
		fake = &fakeSSH{shell: sh}
	} else {
		fake = &fakeSSH{result: core.ExecResult{ExitCode: 0, Stdout: []byte("sandbox-cap\r\n")}}
	}
	env := newRemoteEnv(t, fake)

	bashL := tools.NewBash(local.Exec{})
	bashR := tools.NewBash(env) // ← 只换这一行

	ctx := context.Background()
	name, args := echoCmd()
	resL, err := bashL.Execute(ctx, intent("bash", map[string]any{"command": shellLine(name, args)}))
	if err != nil {
		t.Fatalf("local bash: %v", err)
	}
	resR, err := bashR.Execute(ctx, intent("bash", map[string]any{"command": shellLine(name, args)}))
	if err != nil {
		t.Fatalf("remote bash: %v", err)
	}
	if !strings.Contains(resultTextOf(resR), "sandbox-cap") {
		t.Errorf("remote bash 输出缺回显：%q", resultTextOf(resR))
	}
	if sh != "" {
		// 有 sh：local 与 remote 都应产出真实回显，逐字节比对
		if resultTextOf(resL) != resultTextOf(resR) {
			t.Errorf("local 与 remote 的 bash 结果不同：\nlocal=%q\nremote=%q",
				resultTextOf(resL), resultTextOf(resR))
		}
	}
	// 假 ssh 确实被走过一次（证明 bash 工具真的经 remote env 起了 ssh，不是被绕过）
	if n := fake.callCount(); n != 1 {
		t.Errorf("假 ssh 被调 %d 次，want 1", n)
	}
}

// echoCmd / shellLine / intent / resultTextOf 与 runtime/cap/sandbox 的同款测试辅助
// 保持一致（跨平台 echo；bash 工具会再包一层平台 shell）。
func echoCmd() (string, []string) {
	return "echo", []string{"sandbox-cap"}
}

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

func intent(tool string, args any) contracts.ToolCallIntentPayload {
	raw, err := json.Marshal(args)
	if err != nil {
		panic(err)
	}
	return contracts.ToolCallIntentPayload{ToolName: tool, Arguments: raw}
}

func resultTextOf(r contracts.ToolResult) string {
	for _, p := range r.Content {
		if p.Type == "text" && p.Text != nil {
			return *p.Text
		}
	}
	return ""
}
