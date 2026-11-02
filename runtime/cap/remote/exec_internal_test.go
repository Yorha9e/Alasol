package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/core"
)

// TestShQuoteProduction 直接单测**生产** shQuote（外部包够不到它，故放内部测试）。
// 破坏生产 shQuote 的转义会让这里与外部注入钉子**同时**红（变异自证）。
func TestShQuoteProduction(t *testing.T) {
	cases := []struct {
		in        string
		quotes    int
		wantExact string
	}{
		{"plain", 0, "'plain'"},
		{"", 0, "''"},
		{"it's", 1, "'it'\\''s'"},
		{"a'; rm -rf /; '", 2, "'a'\\''; rm -rf /; '\\'''"},
	}
	for _, c := range cases {
		got := shQuote(c.in)
		if got != c.wantExact {
			t.Errorf("shQuote(%q) = %q，want %q", c.in, got, c.wantExact)
		}
		// 计数不变量：总单引号数 = 2（包裹）+ 3*（内部引号数）。转义被去掉时此断言红。
		if n := strings.Count(got, "'"); n != 2+3*c.quotes {
			t.Errorf("shQuote(%q) = %q 有 %d 个单引号，want %d（转义失效？）", c.in, got, n, 2+3*c.quotes)
		}
	}
}

// TestBuildRemoteCommandProduction 直接单测生产 buildRemoteCommand 的拼装
// （含 cd / env 前缀与逐 token 引用）——这是发给系统 ssh 的最后一个参数。
func TestBuildRemoteCommandProduction(t *testing.T) {
	// 无 Dir / 无 Env：逐 token 引用，空格连接
	got, err := buildRemoteCommand("git", core.ExecSpec{Name: "git", Args: []string{"commit", "-m", "a b"}})
	if err != nil {
		t.Fatalf("buildRemoteCommand: %v", err)
	}
	if got != "'git' 'commit' '-m' 'a b'" {
		t.Errorf("plain: %q", got)
	}
	// Env 前缀（**整份替换**语义：env -i + 显式 PATH）
	got, err = buildRemoteCommand("make", core.ExecSpec{Name: "make", Env: []string{"CC=clang", "X=a b"}})
	if err != nil {
		t.Fatalf("buildRemoteCommand: %v", err)
	}
	if want := "env -i 'CC=clang' 'X=a b' 'PATH=" + defaultRemotePATH + "' 'make'"; got != want {
		t.Errorf("env prefix:\n  %q\nwant\n  %q", got, want)
	}
	// 调用方自己给了 PATH → **一律忽略**（那是 harness 本机的 PATH，对远端无意义），
	// 原地替换成远端侧的 defaultRemotePATH（位置不变、值换掉）。
	got, err = buildRemoteCommand("make", core.ExecSpec{Name: "make", Env: []string{"PATH=/opt/bin", "CC=clang"}})
	if err != nil {
		t.Fatalf("buildRemoteCommand: %v", err)
	}
	if want := "env -i 'PATH=" + defaultRemotePATH + "' 'CC=clang' 'make'"; got != want {
		t.Errorf("env prefix (自带 PATH → 换成远端的):\n  %q\nwant\n  %q", got, want)
	}
	// Dir 前缀
	got, err = buildRemoteCommand("ls", core.ExecSpec{Name: "ls", Dir: "/srv/app"})
	if err != nil {
		t.Fatalf("buildRemoteCommand: %v", err)
	}
	if got != "cd '/srv/app' && 'ls'" {
		t.Errorf("dir prefix: %q", got)
	}
	// 三者叠加，顺序 = cd … && env -i … name args
	got, err = buildRemoteCommand("go", core.ExecSpec{Name: "go", Args: []string{"test"}, Dir: "/w", Env: []string{"A=1"}})
	if err != nil {
		t.Fatalf("buildRemoteCommand: %v", err)
	}
	if want := "cd '/w' && env -i 'A=1' 'PATH=" + defaultRemotePATH + "' 'go' 'test'"; got != want {
		t.Errorf("combined:\n  %q\nwant\n  %q", got, want)
	}
}

// TestBuildRemoteCommandEnvWithoutEqualsFailsClosed 钉死 F6：Env 条目没有 '=' 时
// env 会把它当**命令名**（远端语义就跑偏了）→ fail-close 点名报错，不静默拼出去。
func TestBuildRemoteCommandEnvWithoutEqualsFailsClosed(t *testing.T) {
	_, err := buildRemoteCommand("make", core.ExecSpec{Name: "make", Env: []string{"NOEQUALS"}})
	if err == nil {
		t.Fatal("Env 条目缺 '=' 应报错（fail-close）")
	}
	if !strings.Contains(err.Error(), "NOEQUALS") {
		t.Errorf("错误应点名那条条目：%v", err)
	}
}

// TestBuildRemoteCommandEnvEmptyKeyFailsClosed 钉死 F6 的补充：键名为空的条目
// （"=v"）env 同样拼不出来（GNU env 不接受空键名）→ fail-close 点名报错。
func TestBuildRemoteCommandEnvEmptyKeyFailsClosed(t *testing.T) {
	for _, kv := range []string{"=v", " =v"} {
		_, err := buildRemoteCommand("make", core.ExecSpec{Name: "make", Env: []string{kv}})
		if err == nil {
			t.Fatalf("Env 条目 %q 键名为空应报错（fail-close）", kv)
		}
		if !strings.Contains(err.Error(), kv) {
			t.Errorf("错误应点名那条条目 %q：%v", kv, err)
		}
	}
}

// TestBuildRemoteCommandIgnoresHarnessIdentityEnv 钉死 F1（复查轮）：
// 调用方给的 PATH/HOME/USER/… 是 **harness 本机**的身份（Windows 上 PATH 还是
// `C:\…;C:\…` 形态）——搬给 POSIX 远端就是错的：远端按这个 PATH 找命令一个都找不到
// （exit 127）。故：
//   - PATH → **原地替换**成 defaultRemotePATH（值必须是远端侧的）；
//   - 其余机器身份键（HOME/USER/LOGNAME/TERM/SHELL/LANG/LC_ALL）→ **剔除**。
func TestBuildRemoteCommandIgnoresHarnessIdentityEnv(t *testing.T) {
	harnessEnv := []string{
		"PATH=C:\\Windows\\System32;C:\\Windows",
		"HOME=C:\\Users\\harness",
		"USER=harness",
		"LOGNAME=harness",
		"TERM=xterm-256color",
		"SHELL=C:\\Windows\\System32\\cmd.exe",
		"LANG=en_US.UTF-8",
		"LC_ALL=C",
		"CC=clang", // 普通键 → 原样保留
	}
	got, err := buildRemoteCommand("ls", core.ExecSpec{Name: "ls", Env: harnessEnv})
	if err != nil {
		t.Fatalf("buildRemoteCommand: %v", err)
	}
	if !strings.Contains(got, "'PATH="+defaultRemotePATH+"'") {
		t.Errorf("PATH 应被换成远端侧的 %q：%q", defaultRemotePATH, got)
	}
	if strings.Contains(got, "C:\\Windows") {
		t.Errorf("harness 的 PATH 不该出现在远端命令里：%q", got)
	}
	for _, k := range []string{"HOME", "USER", "LOGNAME", "TERM", "SHELL", "LANG", "LC_ALL"} {
		if strings.Contains(got, "'"+k+"=") {
			t.Errorf("机器身份键 %q 应被剔除（harness 的身份不是远端的身份）：%q", k, got)
		}
	}
	if !strings.Contains(got, "'CC=clang'") {
		t.Errorf("普通键应原样保留：%q", got)
	}
}

// TestBuildRemoteCommandPATHCaseInsensitive 钉死 F1 的边角：Windows 上 os.Environ()
// 给的键名是 "Path"（不是 "PATH"）——大小写敏感会整个漏掉，harness 的 PATH 又漏进远端。
func TestBuildRemoteCommandPATHCaseInsensitive(t *testing.T) {
	got, err := buildRemoteCommand("ls", core.ExecSpec{Name: "ls", Env: []string{"Path=C:\\Windows\\System32"}})
	if err != nil {
		t.Fatalf("buildRemoteCommand: %v", err)
	}
	if strings.Contains(got, "C:\\Windows") {
		t.Errorf("'Path'（Windows 拼写）也应被替换成远端侧的 PATH：%q", got)
	}
	if !strings.Contains(got, "'PATH="+defaultRemotePATH+"'") {
		t.Errorf("PATH 应被换成远端侧的 %q：%q", defaultRemotePATH, got)
	}
}

// TestBuildRemoteCommandRemotePathOverride 钉死 F1 的另一半：远端 PATH 可经
// Config/参数覆盖（默认 defaultRemotePATH）——写死不是唯一选择，但**必须显式**。
func TestBuildRemoteCommandRemotePathOverride(t *testing.T) {
	got, err := buildRemoteCommand("ls", core.ExecSpec{Name: "ls", Env: []string{"A=1"}}, "/opt/bin:/usr/bin")
	if err != nil {
		t.Fatalf("buildRemoteCommand: %v", err)
	}
	if !strings.Contains(got, "'PATH=/opt/bin:/usr/bin'") {
		t.Errorf("远端 PATH 应可覆盖：%q", got)
	}
}

// TestBuildRemoteCommandDropsWindowsIdentityEnv 钉死 F1（终查轮）：**Windows 身份键**
// 也必须被剔除——harness 的主开发平台就是 Windows，这些键（USERNAME /
// USERPROFILE / HOMEDRIVE / HOMEPATH / COMPUTERNAME / USERDOMAIN / LOGONSERVER /
// SESSIONNAME）原先**一个都不在表里** → 原样发往远端 = 身份冒充 + 账号/主机名泄漏。
//
// 键名比较大小写不敏感（Windows 上 os.Environ() 给的可能是 "Username"），
// 所以这里两种拼写都测。
func TestBuildRemoteCommandDropsWindowsIdentityEnv(t *testing.T) {
	windowsIdentity := []string{
		"USERNAME=harness",
		"USERPROFILE=C:\\Users\\harness",
		"HOMEDRIVE=C:",
		"HOMEPATH=\\Users\\harness",
		"COMPUTERNAME=HARNESS-PC",
		"USERDOMAIN=WORKGROUP",
		"LOGONSERVER=\\\\DC01",
		"SESSIONNAME=Console",
	}
	// 大小写变体（Windows 环境变量名大小写不敏感）。
	caseVariants := []string{
		"Username=harness",
		"UserProfile=C:\\Users\\harness",
		"ComputerName=HARNESS-PC",
		"UserDomain=WORKGROUP",
	}
	got, err := buildRemoteCommand("ls", core.ExecSpec{
		Name: "ls",
		Env:  append(append([]string(nil), windowsIdentity...), caseVariants...),
	})
	if err != nil {
		t.Fatalf("buildRemoteCommand: %v", err)
	}
	for _, kv := range append(append([]string(nil), windowsIdentity...), caseVariants...) {
		k, _, _ := strings.Cut(kv, "=")
		if strings.Contains(got, "'"+k+"=") {
			t.Errorf("Windows 身份键 %q 应被剔除（harness 的身份不是远端的身份）：%q", k, got)
		}
	}
	// 值本身也不该出现（防"键被换名/转义"这类绕过）。
	for _, v := range []string{"harness", "HARNESS-PC", "WORKGROUP", "Console"} {
		if strings.Contains(got, v) {
			t.Errorf("Windows 身份值 %q 不该出现在远端命令里：%q", v, got)
		}
	}
	// 普通键仍原样保留（不能连普通键一起剔了）。
	got2, err := buildRemoteCommand("ls", core.ExecSpec{Name: "ls", Env: []string{"CC=clang"}})
	if err != nil {
		t.Fatalf("buildRemoteCommand: %v", err)
	}
	if !strings.Contains(got2, "'CC=clang'") {
		t.Errorf("普通键应原样保留：%q", got2)
	}
}

// TestMergeSSHOptions 钉死 F4：默认 ssh 选项（BatchMode/ConnectTimeout）在，
// 且**调用方显式给了同名选项时以调用方为准**（不出现"同一个选项给两遍"）。
func TestMergeSSHOptions(t *testing.T) {
	got := mergeSSHOptions(nil)
	want := []string{"-o", "BatchMode=yes", "-o", "ConnectTimeout=10"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("默认选项 = %q，want %q", got, want)
	}
	// 调用方给了 BatchMode → 不再加我们的默认值；别的默认项照旧。
	got = mergeSSHOptions([]string{"-p", "2222", "-o", "BatchMode=no"})
	want = []string{"-p", "2222", "-o", "BatchMode=no", "-o", "ConnectTimeout=10"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("覆盖 BatchMode = %q，want %q", got, want)
	}
	// 调用方给了 ConnectTimeout → 同理。
	got = mergeSSHOptions([]string{"-o", "ConnectTimeout=30"})
	want = []string{"-o", "ConnectTimeout=30", "-o", "BatchMode=yes"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("覆盖 ConnectTimeout = %q，want %q", got, want)
	}
}

// TestMergeSSHOptionsAttachedForm 钉死 F3：附着式 "-oBatchMode=no" 也要被认出来
// （早先只认分离式 "-o" "BatchMode=no"）——认不出就会把同一个选项给两遍
// （OpenSSH 对 -o 的 first/last-wins 口径还不一致 → 生效值不可预期）。
func TestMergeSSHOptionsAttachedForm(t *testing.T) {
	got := mergeSSHOptions([]string{"-oBatchMode=no"})
	for _, a := range got {
		if strings.EqualFold(a, "BatchMode=yes") {
			t.Fatalf("调用方已给 BatchMode（附着式），不该再加默认值：%q", got)
		}
	}
	if !contains(got, "-oBatchMode=no") {
		t.Errorf("调用方给的值应原样保留：%q", got)
	}
	// 附着式 + 分离式两种写法都认。
	if hasSSHOption([]string{"-oBatchMode=no"}, "BatchMode=yes") != true {
		t.Error("附着式 -oBatchMode=no 应被认成 BatchMode")
	}
	if hasSSHOption([]string{"-o", "BatchMode=no"}, "BatchMode=yes") != true {
		t.Error("分离式 -o BatchMode=no 应被认成 BatchMode")
	}
	if hasSSHOption([]string{"-oConnectTimeout=30"}, "BatchMode=yes") {
		t.Error("ConnectTimeout 不该被认成 BatchMode")
	}
}

// contains 判断切片里有没有某个元素（测试辅助）。
func contains(items []string, want string) bool {
	for _, it := range items {
		if it == want {
			return true
		}
	}
	return false
}

// TestIsSSHTransportFailure 钉死 F5 的判据：命中 ssh 自身的措辞才算传输失败。
//
// 口径（v4-2-A 复查 F2，**收紧**）：只在 stderr 的**首个非空行**上做前缀匹配，
// 且只认 ssh **特有**的强特征（ssh: 前缀 / 密钥交换被打断 / 带鉴权方法括号的
// "Permission denied (" / 主机密钥不匹配 …）。
func TestIsSSHTransportFailure(t *testing.T) {
	yes := []string{
		"ssh: connect to host harness port 22: Connection refused",
		"Permission denied (publickey).",
		"user@harness: Permission denied (publickey,password).",
		"Host key verification failed.",
		"ssh: Could not resolve hostname nope: Name or service not known",
		"kex_exchange_identification: Connection closed by remote host",
		"ssh_exchange_identification: Connection closed by remote host",
	}
	for _, s := range yes {
		if !isSSHTransportFailure([]byte(s)) {
			t.Errorf("应判为 ssh 传输失败：%q", s)
		}
	}
	no := []string{
		"",
		"make: *** [all] Error 255",
		"fatal: not a git repository",
		// ↓ 复查轮 F2 的钉子：通用措辞是**远端命令自己**的输出，不是 ssh 的错误。
		"cat: /etc/shadow: Permission denied",
		"cat: /etc/shadow: Connection refused",
		"Permission denied",
		"Connection refused",
		// ↓ 终查轮 F2 的钉子 (a)：**只看首个非空行**——ssh 的特征出现在**第二行**，
		// 第一行是远端命令自己的输出 → 不判为传输失败（宁可漏判）。
		"远端自己的输出\nssh: connect to host h port 22: Connection refused\n",
		// ↓ 终查轮 F2 的钉子 (b)：**只认行首前缀**——"Permission denied (" 出现在
		// 行**中间**（前面是 "cat: /etc/shadow: "）→ 不是 ssh 的特征行首形态。
		"cat: /etc/shadow: Permission denied (publickey)",
	}
	for _, s := range no {
		if isSSHTransportFailure([]byte(s)) {
			t.Errorf("不应判为 ssh 传输失败：%q", s)
		}
	}
}

// TestClassifySSHExit 钉死 F2 的分类口径：exit 255 时，**只有** stderr 首行的 ssh
// 强特征才算 ErrSSHTransport；远端命令自己的 255（如 `cat: /etc/shadow: Permission denied`）
// **不**算——否则调用方会把"远端跑挂了"误当成"根本没连上"。
func TestClassifySSHExit(t *testing.T) {
	cases := []struct {
		name      string
		res       core.ExecResult
		transport bool
	}{
		{
			name:      "远端命令自己退出 255（通用措辞）",
			res:       core.ExecResult{ExitCode: 255, Stderr: []byte("cat: /etc/shadow: Permission denied\n")},
			transport: false,
		},
		{
			name:      "ssh 自己连不上（ssh: 前缀）",
			res:       core.ExecResult{ExitCode: 255, Stderr: []byte("ssh: connect to host harness port 22: Connection refused\n")},
			transport: true,
		},
		{
			name:      "鉴权失败（带方法括号）",
			res:       core.ExecResult{ExitCode: 255, Stderr: []byte("Permission denied (publickey).\n")},
			transport: true,
		},
		{
			name:      "非 255 退出码不参与分类",
			res:       core.ExecResult{ExitCode: 7, Stderr: []byte("ssh: connect to host harness port 22: Connection refused\n")},
			transport: false,
		},
		{
			// ↓ 终查轮 F2 钉子 (a)：**只看首个非空行**——ssh 特征在第二行、第一行是
			// 远端自己的输出 → 不判为传输失败（否则把"远端跑挂了"误报成"没连上"）。
			name:      "ssh 特征不在首个非空行",
			res:       core.ExecResult{ExitCode: 255, Stderr: []byte("远端自己的输出\nssh: connect to host h port 22: Connection refused\n")},
			transport: false,
		},
		{
			// ↓ 终查轮 F2 钉子 (b)：**只认行首前缀**——"Permission denied (" 出现在行
			// 中间（远端命令自己的输出形态）→ 不判为传输失败。
			name:      "Permission denied ( 不在行首",
			res:       core.ExecResult{ExitCode: 255, Stderr: []byte("cat: /etc/shadow: Permission denied (publickey)")},
			transport: false,
		},
	}
	for _, c := range cases {
		err := classifySSHExit(c.res)
		if c.transport {
			if !errors.Is(err, ErrSSHTransport) {
				t.Errorf("%s：应判为 ErrSSHTransport，got %v", c.name, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s：不该判为 ErrSSHTransport（远端命令自己的退出），got %v", c.name, err)
		}
	}
}

// ---------------------------------------------------------------------------
// 生产路径 osExec：真起本地进程（不真连 SSH）
// ---------------------------------------------------------------------------

// fakeSSHEnvVar 是"假 ssh"模式的环境变量名（helper-process 模式）。
const fakeSSHEnvVar = "GO_ALASOL_FAKE_SSH"

// TestHelperProcess 是一个**假 ssh**（经典 helper-process 模式）：
// 只有 GO_ALASOL_FAKE_SSH 指向某个模式时才真的干活，否则当普通测试空跑。
//
// 用途：让**生产路径** osExec（真起进程）在"不真连 SSH"的前提下可测——退出码透传、
// ctx 取消传播、启动失败、ssh 传输失败（exit 255 + 特征 stderr）。见 F10。
func TestHelperProcess(t *testing.T) {
	switch os.Getenv(fakeSSHEnvVar) {
	case "":
		return // 普通测试进程：什么都不做
	case "exit255":
		fmt.Fprintln(os.Stderr, "ssh: connect to host harness port 22: Connection refused")
		os.Exit(255)
	case "exit255-plain":
		os.Exit(255) // 远端命令自己退出 255：stderr 无 ssh 特征
	case "exit42":
		os.Exit(42)
	case "cat-stdin":
		// 把 stdin 原样搬到 stdout（二进制往返的"假远端出口"）——
		// 验证生产路径 osExec 的 stdin 管道逐字节送达。
		_, _ = io.Copy(os.Stdout, os.Stdin)
		os.Exit(0)
	case "sleep":
		time.Sleep(60 * time.Second)
		os.Exit(0)
	default:
		os.Exit(3)
	}
}

// helperArgs 是拉起"假 ssh"用的 argv（只跑那一个 helper 测试）。
func helperArgs() []string { return []string{"-test.run=^TestHelperProcess$"} }

// TestOsExecExitCodePassthrough 验证生产路径的**退出码透传**：非零退出不是 error。
func TestOsExecExitCodePassthrough(t *testing.T) {
	t.Setenv(fakeSSHEnvVar, "exit42")
	res, err := osExec(context.Background(), os.Args[0], helperArgs(), nil)
	if err != nil {
		t.Fatalf("非零退出不该是 error：%v", err)
	}
	if res.ExitCode != 42 {
		t.Errorf("ExitCode = %d，want 42", res.ExitCode)
	}
}

// TestOsExecStartFailure 验证起不来（ssh 不在 PATH）→ 明确 error。
func TestOsExecStartFailure(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-binary-here")
	_, err := osExec(context.Background(), missing, nil, nil)
	if err == nil {
		t.Fatal("起不来应该是 error")
	}
	if !strings.Contains(err.Error(), "启动") {
		t.Errorf("错误信息应说清是启动失败：%v", err)
	}
}

// TestOsExecContextCancel 验证 ctx 取消会收掉进程并及时返回（不挂住）。
func TestOsExecContextCancel(t *testing.T) {
	t.Setenv(fakeSSHEnvVar, "sleep")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := osExec(ctx, os.Args[0], helperArgs(), nil)
	if err == nil {
		t.Fatal("取消后应返回 error")
	}
	if !strings.Contains(err.Error(), "中止") {
		t.Errorf("错误信息应说清是被中止：%v", err)
	}
	if d := time.Since(start); d > 15*time.Second {
		t.Errorf("取消后没有及时返回（%v）——WaitDelay/取消链路可能失效", d)
	}
}

// TestOsExecSSHTransportFailure255 钉死 F5：ssh 自己失败（exit 255 + 特征 stderr）
// → 返回可 errors.Is 判的 ErrSSHTransport，错误里带 stderr 片段。
func TestOsExecSSHTransportFailure255(t *testing.T) {
	t.Setenv(fakeSSHEnvVar, "exit255")
	res, err := osExec(context.Background(), os.Args[0], helperArgs(), nil)
	if err == nil {
		t.Fatal("ssh 传输失败应返回 error（不能与远端命令退出 255 混为一谈）")
	}
	if !errors.Is(err, ErrSSHTransport) {
		t.Errorf("应可 errors.Is(err, ErrSSHTransport)：%v", err)
	}
	if res.ExitCode != 255 {
		t.Errorf("ExitCode = %d，want 255", res.ExitCode)
	}
	if !strings.Contains(err.Error(), "Connection refused") {
		t.Errorf("错误应带 stderr 片段：%v", err)
	}
}

// TestOsExecRemoteExit255Passthrough 钉死 F5 的另一半：远端命令自己退出 255
// （stderr 无 ssh 特征）→ 仍按退出码透出，**不**误报成"没连上"。
func TestOsExecRemoteExit255Passthrough(t *testing.T) {
	t.Setenv(fakeSSHEnvVar, "exit255-plain")
	res, err := osExec(context.Background(), os.Args[0], helperArgs(), nil)
	if err != nil {
		t.Fatalf("远端命令退出 255 不该是 error：%v", err)
	}
	if res.ExitCode != 255 {
		t.Errorf("ExitCode = %d，want 255", res.ExitCode)
	}
}

// ---------------------------------------------------------------------------
// Fs 的内部件：GNU stat 解析、非 GNU 判定、stdin 执行面
// ---------------------------------------------------------------------------

// TestParseGNUStat 直接单测生产 parseGNUStat：GNU 形状 → 字段对；坏形状 →
// 明文错误（含"非 GNU 口径"提示）+ 零值 FileInfo，**不瞎猜**。
// 破坏生产解析（变异自证②）会让这里与外部 TestRemoteStatBadShapeFailsClosed 同时红。
func TestParseGNUStat(t *testing.T) {
	good := []struct {
		in   string
		want core.FileInfo
	}{
		{"1234|1700000000|regular file\n",
			core.FileInfo{Size: 1234, ModTime: time.Unix(1700000000, 0)}},
		{"4096|0|directory\n",
			core.FileInfo{Size: 4096, IsDir: true, ModTime: time.Unix(0, 0)}},
		{"0|-86400|regular file", // epoch 前 mtime（负秒）；无尾换行
			core.FileInfo{Size: 0, ModTime: time.Unix(-86400, 0)}},
		{"10|1712345678|symbolic link\n",
			core.FileInfo{Size: 10, ModTime: time.Unix(1712345678, 0)}},
	}
	for _, c := range good {
		got, err := parseGNUStat([]byte(c.in))
		if err != nil {
			t.Errorf("parseGNUStat(%q): %v", c.in, err)
			continue
		}
		if got.Size != c.want.Size || got.IsDir != c.want.IsDir || !got.ModTime.Equal(c.want.ModTime) {
			t.Errorf("parseGNUStat(%q) = {Size:%d IsDir:%v ModTime:%v}，want {Size:%d IsDir:%v ModTime:%v}",
				c.in, got.Size, got.IsDir, got.ModTime,
				c.want.Size, c.want.IsDir, c.want.ModTime)
		}
	}
	bad := []string{
		"",
		"42",
		"1|2",
		"1|2|directory|extra",
		"abc|def|ghi",
		"x|y|z",
		"1|notanumber|directory",
		"1234|1700000000|",
		"total 123\n1|2|directory", // 混入换行
		"1|2|directory\r",          // 回车不许静默吞
	}
	for _, in := range bad {
		fi, err := parseGNUStat([]byte(in))
		if err == nil {
			t.Errorf("parseGNUStat(%q) 应报错（坏形状不瞎猜），得到 %+v", in, fi)
			continue
		}
		if !strings.Contains(err.Error(), "非 GNU 口径") {
			t.Errorf("parseGNUStat(%q) 错误应点明非 GNU 口径提示：%v", in, err)
		}
		if fi != (core.FileInfo{}) {
			t.Errorf("parseGNUStat(%q) 出错时应返回零值 FileInfo，得到 %+v", in, fi)
		}
	}
}

// TestLooksLikeNonGNUStat 钉死"非 GNU stat"的判据：BSD 的 illegal option 命中；
// 通用 stat 报错 / 别的命令的报错**不**误判（保守判据，同 classifySSHExit 哲学）。
func TestLooksLikeNonGNUStat(t *testing.T) {
	yes := []string{
		"stat: illegal option -- c",
		"stat: invalid option -- 'c'",
		"stat: unrecognized option '--foo'",
		"stat: unknown option",
	}
	for _, s := range yes {
		if !looksLikeNonGNUStat([]byte(s)) {
			t.Errorf("应判为非 GNU stat 的报错：%q", s)
		}
	}
	no := []string{
		"",
		"stat: cannot statx '/p': No such file or directory",
		"sh: stat: not found",
		"bash: stat: command not found",
		"cat: /etc/shadow: Permission denied",
		"ls: invalid option -- 'z'", // 不是 stat 的报错
	}
	for _, s := range no {
		if looksLikeNonGNUStat([]byte(s)) {
			t.Errorf("不该判为非 GNU stat 的报错：%q", s)
		}
	}
}

// TestOsExecStdinBinaryRoundTrip 钉死**生产路径** osExec 的 stdin 管道：
// helper 进程把 stdin 原样搬到 stdout（假远端出口）→ 逐字节往返。
// 这是 WriteFile"内容经 ssh stdin 送达"在 os/exec 层的直接证据（不经 ssh）。
func TestOsExecStdinBinaryRoundTrip(t *testing.T) {
	t.Setenv(fakeSSHEnvVar, "cat-stdin")
	data := []byte{0x00, 0x01, 0x7f, 0x80, 0xff, '\r', '\n', 'h', 'i', 0x00}
	res, err := osExec(context.Background(), os.Args[0], helperArgs(), data)
	if err != nil {
		t.Fatalf("osExec(stdin): %v", err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("ExitCode = %d，stderr=%s", res.ExitCode, res.Stderr)
	}
	if string(res.Stdout) != string(data) { // 字节相等（string 比较不改字节）
		t.Errorf("stdin 未逐字节送达：got=%v want=%v", res.Stdout, data)
	}
	// 空 stdin：不灌字节 → 子进程读到 EOF → 空输出（与窄面行为一致）。
	res2, err := osExec(context.Background(), os.Args[0], helperArgs(), nil)
	if err != nil {
		t.Fatalf("osExec(nil stdin): %v", err)
	}
	if len(res2.Stdout) != 0 {
		t.Errorf("空 stdin 应产出空 stdout，得到 %v", res2.Stdout)
	}
}

// TestNoStdinExecFailClose 钉死窄面→宽面的适配口径：空 stdin 委托窄面；
// 非空 stdin **fail-close**（不委托、不静默丢字节）。
func TestNoStdinExecFailClose(t *testing.T) {
	calls := 0
	narrow := func(context.Context, string, []string) (core.ExecResult, error) {
		calls++
		return core.ExecResult{ExitCode: 0}, nil
	}
	wide := noStdinExec(narrow)
	if _, err := wide(context.Background(), "ssh", []string{"h", "cmd"}, nil); err != nil {
		t.Fatalf("空 stdin 应委托窄面：%v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d，want 1", calls)
	}
	_, err := wide(context.Background(), "ssh", []string{"h", "cmd"}, []byte("payload"))
	if err == nil {
		t.Fatal("非空 stdin 经窄面应 fail-close 报错")
	}
	if !strings.Contains(err.Error(), "stdin") {
		t.Errorf("错误应点名 stdin：%v", err)
	}
	if calls != 1 {
		t.Errorf("fail-close 不该委托窄面（calls = %d，want 1）", calls)
	}
}
