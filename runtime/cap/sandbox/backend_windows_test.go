//go:build windows

package sandbox_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/cap/sandbox"
)

// 本文件是 Windows 三件套的**真机实测**（Go 1.26.5 windows/amd64，非管理员）。
//
// 三条纪律：
//  1. **不要求管理员权限**。需要提权的用例一律 t.Skip 并写明原因；
//  2. 断言的是 **Windows 真实报错**（"Access is denied."），不是我们自己的错误文案；
//  3. 每条测试都对应 11 §2 v2-S4 的验收条目，注释里写明对应关系。

// sandboxTestRoots 造一个"允许根"（沙箱临时根会自动加进来）。
//
// 单独造目录而不是直接用 t.TempDir()：后者在 Windows 上位于用户 profile 下，
// 给它设 ACL 会撞上 dangerousRootReason 的护栏（用户配置文件目录）——那是**故意**的，
// 见 TestDangerousRootGuard。测试要用自己造的临时目录。
func sandboxTestRoots(t *testing.T) (allowed string, outside string) {
	t.Helper()
	base := t.TempDir()
	allowed = filepath.Join(base, "workspace")
	outside = filepath.Join(base, "elsewhere")
	for _, d := range []string{allowed, outside} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", d, err)
		}
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	return allowed, outside
}

// newSandboxEnv 造一个真三件套沙箱 env（Windows 默认后端）。
func newSandboxEnv(t *testing.T, allowed ...string) *sandbox.Env {
	t.Helper()
	p := sandbox.DefaultPolicy()
	p.AllowedRoots = allowed
	env, err := sandbox.New(p)
	if err != nil {
		t.Fatalf("sandbox.New: %v", err)
	}
	t.Cleanup(func() { _ = env.Close() })
	return env
}

// diagsOf 把埋点记录折成多行文本（断言与日志都用）。
func diagsOf(ds []sandbox.Diag) string {
	var sb strings.Builder
	for _, d := range ds {
		if d.OK {
			sb.WriteString(d.Kind + ":OK ")
		} else {
			sb.WriteString(d.Kind + ":DEGRADED ")
		}
		sb.WriteString(d.Detail)
		sb.WriteString("\n")
	}
	return sb.String()
}

// diagKinds 返回某类埋点的记录（可能多条）。
func diagKinds(ds []sandbox.Diag, kind string) []sandbox.Diag {
	var out []sandbox.Diag
	for _, d := range ds {
		if d.Kind == kind {
			out = append(out, d)
		}
	}
	return out
}

// TestRestrictedTokenDenyOnlyUserSID 对应验收"受限 token：子进程确实在受限 token 下"。
//
// 断言挑**不需要管理员权限**的观察点：token 的用户 SID 属性位从 0x0 翻到 0x10
// （SE_GROUP_USE_FOR_DENY_ONLY）。特权集在标准用户机器上可能本来就没有可删的
// （删了个寂寞），但 deny-only 位是确定会翻的——本机实测确认。
func TestRestrictedTokenDenyOnlyUserSID(t *testing.T) {
	allowed, _ := sandboxTestRoots(t)
	env := newSandboxEnv(t, allowed)

	var found bool
	for _, d := range diagKinds(env.Diagnostics(), "restricted-token") {
		if d.OK && strings.Contains(d.Detail, "deny-only=true") {
			found = true
		}
	}
	if !found {
		t.Fatalf("受限 token 埋点没报告 deny-only=true：\n%s", diagsOf(env.Diagnostics()))
	}
}

// TestFileACLDeniesWriteOutsideAllowedRoots 对应验收"目录外写被 ACL 拒"。
//
// 断言的是 **Windows 真实报错**：子进程（受限 token）在允许根之外写文件，
// cmd 报 "Access is denied." 并非零退出。这不是 harness 的字符串比较，是内核拒的。
//
// （严格模式——Policy.StrictGroupSIDs——下子进程读不到 Windows 的系统消息表，
// 那时错误文本会缺失，只剩"文件没出现 + 非零退出码"；默认模式下文本在。）
func TestFileACLDeniesWriteOutsideAllowedRoots(t *testing.T) {
	allowed, outside := sandboxTestRoots(t)
	env := newSandboxEnv(t, allowed)

	// 目录外写：必须被拒（Windows 真实报错）
	outTarget := filepath.Join(outside, "escape.txt")
	res, err := env.Run(context.Background(), core.ExecSpec{
		Name: "cmd",
		Args: []string{"/c", "echo", "should-not-land", ">", outTarget},
	})
	if err != nil {
		t.Fatalf("Run（目录外写）返回了 error：%v", err)
	}
	if res.ExitCode == 0 {
		t.Errorf("目录外写竟然成功了（exit 0）——ACL 没收住；stdout=%q stderr=%q", res.Stdout, res.Stderr)
	}
	if !strings.Contains(strings.ToLower(string(res.Stderr)), "access is denied") {
		t.Errorf("期望 Windows 报 Access is denied，实际 stderr=%q", res.Stderr)
	}
	if _, statErr := os.Stat(outTarget); statErr == nil {
		t.Errorf("文件 %s 居然被创建了", outTarget)
	}

	// 目录内写：必须成功（允许集授予生效）
	inTarget := filepath.Join(allowed, "inside.txt")
	res, err = env.Run(context.Background(), core.ExecSpec{
		Name: "cmd",
		Args: []string{"/c", "echo", "lands", ">", inTarget},
	})
	if err != nil {
		t.Fatalf("Run（目录内写）: %v", err)
	}
	if res.ExitCode != 0 {
		t.Errorf("目录内写失败：exit=%d stderr=%q", res.ExitCode, res.Stderr)
	}
	data, readErr := os.ReadFile(inTarget)
	if readErr != nil {
		t.Fatalf("读 %s: %v", inTarget, readErr)
	}
	if !strings.Contains(string(data), "lands") {
		t.Errorf("文件内容不对：%q", data)
	}
}

// TestFileACLDeniesWriteToSharedSystemDirs 是 F2 的正面断言：
// **常见系统共享目录**里的写必须被拒。
//
// 为什么这两个位置值得单独钉：它们的 DACL 授的不是当前用户，而是
// NT AUTHORITY\INTERACTIVE（Public）与 BUILTIN\Users（ProgramData）。
// 只把用户 SID 转 deny-only 时（收紧之前），子进程照样能在那儿新建文件——
// "目录外写被拒"这条验收在常见位置就站不住。
//
// # 两种模式的实测结果（Windows 11 / 非管理员）
//
//   - **默认模式**（BUILTIN\Users 保持 enabled）：Public 的写被拒 ✓；
//     ProgramData **仍可写**（它的授权只来自 BUILTIN\Users，而把 Users 也收走
//     会让用 overlapped I/O 写文件的程序全部写不进去——Go 的 os.File.Write 返回
//     `winapi error #10093`，见 token_windows.go 的白名单说明）。
//   - **严格模式**（Policy.StrictGroupSIDs=true）：两处都被拒 ✓。
//
// 测试把两种模式都钉住：默认模式钉住"Public 被拒"（以及 ProgramData 这个
// **已知残留**，免得它悄悄变宽），严格模式钉住"两处都被拒"。
//
// 注意（诚实边界）：本测试**不**断言 stderr 的错误文本——收紧之后子进程读不到
// Windows 的系统消息表（只授予 BUILTIN\Users），系统错误消息取不到（见
// TestFileACLDeniesWriteOutsideAllowedRoots 的注释）。断言的是"文件没出现"。
func TestFileACLDeniesWriteToSharedSystemDirs(t *testing.T) {
	public := os.Getenv("PUBLIC")
	if public == "" {
		public = `C:\Users\Public`
	}
	if fi, err := os.Stat(public); err != nil || !fi.IsDir() {
		t.Skipf("本机没有 Public 目录（%s），跳过", public)
	}
	programData := os.Getenv("ProgramData")
	if programData == "" {
		programData = `C:\ProgramData`
	}
	if fi, err := os.Stat(programData); err != nil || !fi.IsDir() {
		t.Skipf("本机没有 ProgramData 目录（%s），跳过", programData)
	}

	// 默认模式：Public 被拒；ProgramData 仍可写（已知残留，钉住它别悄悄变宽）。
	allowed, _ := sandboxTestRoots(t)
	env := newSandboxEnv(t, allowed)
	if writeDeniedBySandbox(t, env, public, "default-Public") != true {
		t.Errorf("默认模式下写 %s 竟然成功了——组 SID 没收干净", public)
	}
	if writeDeniedBySandbox(t, env, programData, "default-ProgramData") != false {
		t.Errorf("默认模式下写 %s 被拒了——与实测不符（BUILTIN 组的 Users SID 仍 enabled 时它是可写的）；"+
			"若这是好事，请同步更新 token_windows.go 的白名单说明与 doc.go", programData)
	}

	// 严格模式：两处都被拒。
	allowed2, _ := sandboxTestRoots(t)
	p := sandbox.DefaultPolicy()
	p.AllowedRoots = []string{allowed2}
	p.StrictGroupSIDs = true
	strictEnv, err := sandbox.New(p)
	if err != nil {
		t.Fatalf("sandbox.New(严格模式): %v", err)
	}
	defer strictEnv.Close()
	for _, dir := range []string{public, programData} {
		if writeDeniedBySandbox(t, strictEnv, dir, "strict") != true {
			t.Errorf("严格模式下写 %s 竟然成功了——Users 组 SID 没被收走", dir)
		}
	}
}

// writeDeniedBySandbox 让子进程往 dir 里写一个文件，返回"是否被拒"。
func writeDeniedBySandbox(t *testing.T, env *sandbox.Env, dir, tag string) bool {
	t.Helper()
	probeDir := filepath.Join(dir, "alasol-sandbox-probe-"+tag)
	if err := os.MkdirAll(probeDir, 0o755); err != nil {
		t.Fatalf("准备探针目录 %s: %v", probeDir, err)
	}
	defer os.RemoveAll(probeDir)
	target := filepath.Join(probeDir, "should-not-land.txt")
	res, err := env.Run(context.Background(), core.ExecSpec{
		Name: "cmd",
		Args: []string{"/c", "echo", "should-not-land", ">", target},
	})
	if err != nil {
		t.Fatalf("Run（写 %s）返回了 error：%v", dir, err)
	}
	_, statErr := os.Stat(target)
	denied := statErr != nil || res.ExitCode != 0
	return denied
}

// TestTempRootIsWritableByChild 是 F1 的断言：子进程能写**自己的 %TEMP%**。
//
// 收紧之前有一个静默失效：沙箱临时根是在后端构造**之后**才派生的，
// 后端那份 policy 副本里没有它 → 允许集算少一个根 → 子进程写 %TEMP% 被 ACL 拒
// （"允许集内也写不进去"，最难查的一类症状）。所以这里断言"真写得进去"。
func TestTempRootIsWritableByChild(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("临时根重定向按 Windows 口径断言")
	}
	allowed, _ := sandboxTestRoots(t)
	env := newSandboxEnv(t, allowed)

	tempRoot := env.Policy().SandboxTempRoot
	if tempRoot == "" {
		t.Fatal("沙箱临时根为空")
	}
	// %TEMP% 展开成子进程看到的那条路径（cmd 的 %TEMP% 与环境变量一致）。
	target := filepath.Join(tempRoot, "child-temp-probe.txt")
	_ = os.Remove(target)
	res, err := env.Run(context.Background(), core.ExecSpec{
		Name: "cmd",
		Args: []string{"/c", "echo", "temp-ok", ">", filepath.Join("%TEMP%", "child-temp-probe.txt")},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("子进程写自己的 %%TEMP%% 失败：exit=%d stderr=%q", res.ExitCode, res.Stderr)
	}
	data, readErr := os.ReadFile(target)
	if readErr != nil {
		t.Fatalf("读 %s: %v（子进程写不了自己的临时目录——允许集没含沙箱临时根）", target, readErr)
	}
	if !strings.Contains(string(data), "temp-ok") {
		t.Errorf("临时根里的文件内容不对：%q", data)
	}
	_ = os.Remove(target)
}

// TestFileACLDeniesWriteInSubdirOfAllowedRoot 验证"允许根的子目录"也在允许集内
// （underRoot 是前缀判定，不是只比根本身），且**已存在**的深层文件也能改
// （ACL 的可继承 ACE 会被 Windows 传播到既有子对象）。
func TestFileACLDeniesWriteInSubdirOfAllowedRoot(t *testing.T) {
	allowed, _ := sandboxTestRoots(t)
	sub := filepath.Join(allowed, "pkg", "deep")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	env := newSandboxEnv(t, allowed)

	target := filepath.Join(sub, "existing.go")
	if err := os.WriteFile(target, []byte("package p\n"), 0o644); err != nil {
		t.Fatalf("预置文件: %v", err)
	}
	res, err := env.Run(context.Background(), core.ExecSpec{
		Name: "cmd",
		Args: []string{"/c", "echo", "// touched", ">>", target},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ExitCode != 0 {
		t.Errorf("改允许根深层的既有文件失败：exit=%d stderr=%q", res.ExitCode, res.Stderr)
	}

	// 允许根的**兄弟**目录仍然写不进去。
	sib := filepath.Join(filepath.Dir(allowed), "sibling")
	if err := os.MkdirAll(sib, 0o755); err != nil {
		t.Fatalf("MkdirAll(sibling): %v", err)
	}
	res, err = env.Run(context.Background(), core.ExecSpec{
		Name: "cmd",
		Args: []string{"/c", "echo", "x", ">", filepath.Join(sib, "nope.txt")},
	})
	if err != nil {
		t.Fatalf("Run（兄弟目录）: %v", err)
	}
	if res.ExitCode == 0 {
		t.Error("允许根的兄弟目录居然能写")
	}
}

// TestFileACLDeniesReadOutsideAllowedRoots 验证"**用户私有文件**在允许集之外读不到"。
//
// 这是有意的收紧（见 Policy.AllowedRoots 的注释）：读一个 .env 再回显到 stdout，
// 与写它一样是泄露。所以沙箱模式下允许集是"能碰的范围"，不只是"能写的范围"。
//
// 限定（别过度声明）：这里断言的是**用户私有文件**（DACL 只授当前用户，
// 而用户 SID 在受限 token 里是 deny-only）。**系统/共享位置**（授予
// BUILTIN\Users 读权限的地方，如 Program Files）在默认模式下仍可读——
// 严格模式（StrictGroupSIDs）才连那些也收紧。
func TestFileACLDeniesReadOutsideAllowedRoots(t *testing.T) {
	allowed, outside := sandboxTestRoots(t)
	secret := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secret, []byte("TOPSECRET"), 0o600); err != nil {
		t.Fatalf("预置密钥文件: %v", err)
	}
	env := newSandboxEnv(t, allowed)

	res, err := env.Run(context.Background(), core.ExecSpec{
		Name: "cmd",
		Args: []string{"/c", "type", secret},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if strings.Contains(string(res.Stdout), "TOPSECRET") {
		t.Errorf("沙箱内读到了允许集之外的密钥：%q", res.Stdout)
	}
	if res.ExitCode == 0 {
		t.Errorf("读允许集之外竟然成功了：%q", res.Stdout)
	}
}

// TestBashToolEndToEndDenied 对应验收"bash/cmd 尝试写目录外也拒（端到端）"。
//
// 走**真的 bash 工具**（runtime/tools），不是直接调 Env.Run——证明整条
// "工具 → Exec 能力 → 沙箱"链上约束都在，而不是只在能力层有效。
func TestBashToolEndToEndDenied(t *testing.T) {
	allowed, outside := sandboxTestRoots(t)
	env := newSandboxEnv(t, allowed)
	bash := newBashTool(env)

	target := filepath.Join(outside, "bash-escape.txt")
	res, err := bash.Execute(context.Background(), intent("bash", map[string]any{
		"command": "echo pwned > " + target,
	}))
	if err != nil {
		t.Fatalf("bash Execute: %v", err)
	}
	if _, statErr := os.Stat(target); statErr == nil {
		t.Errorf("bash 竟然在允许集之外写成功了：%s", target)
	}
	if !strings.Contains(resultTextOf(res), "Access is denied") {
		t.Errorf("bash 输出里没有 Windows 的真实报错：%q", resultTextOf(res))
	}

	// 允许集之内 bash 能写
	ok := filepath.Join(allowed, "bash-inside.txt")
	res, err = bash.Execute(context.Background(), intent("bash", map[string]any{
		"command": "echo fine > " + ok,
	}))
	if err != nil {
		t.Fatalf("bash Execute（允许集内）: %v", err)
	}
	if _, statErr := os.Stat(ok); statErr != nil {
		t.Errorf("bash 在允许集之内也写不了：%v\n%s", statErr, resultTextOf(res))
	}
}

// TestJobObjectContainsDetachedGrandchild 对应验收"job object：子进程树被收容"。
//
// 这是 local 的已知边界（"杀树只覆盖我们起的这棵树"）的正面试法：
// 子进程 detach 一个孙子（脱离进程树），孙子活得比子进程长；
// 子进程退出后孙子**仍被 job 收容**，job 句柄一关（KILL_ON_JOB_CLOSE）就连带死掉。
//
// 手法：helper 是**本测试二进制自身**（复制进允许根，因为受限 token 的子进程
// 连遍历用户 profile 下的临时目录都不行）。孙子被设计成"睡 60s 后删掉标记"，
// 于是"标记在 Run 返回后仍然存在"就是"孙子被杀死了"的确定性证据——
// 不需要等 60s。
func TestJobObjectContainsDetachedGrandchild(t *testing.T) {
	allowed, _ := sandboxTestRoots(t)
	env := newSandboxEnv(t, allowed)

	helper := filepath.Join(allowed, "sandbox-helper.exe")
	if err := copyFile(os.Args[0], helper); err != nil {
		t.Fatalf("复制 helper 进允许根: %v", err)
	}
	alive := filepath.Join(allowed, "grandchild-alive.txt")
	done := filepath.Join(allowed, "grandchild-done.txt")
	_ = os.Remove(alive)
	_ = os.Remove(done)

	res, err := env.Run(context.Background(), core.ExecSpec{
		Name: helper,
		Args: []string{"-test.run", "TestSandboxHelperSpawnDetached", "--", alive, done},
	})
	if err != nil {
		t.Fatalf("Run（helper）: %v（stdout=%q stderr=%q）", err, res.Stdout, res.Stderr)
	}
	// 子进程已退出；孙子此刻应该还活着（它要睡 60s）。
	waitAlive(t, alive, 20*time.Second)
	// Run 返回即 Release → job 句柄关闭 → KILL_ON_JOB_CLOSE。
	// 给 3s 让杀透，然后两条断言：
	time.Sleep(3 * time.Second)
	// 断言 1：孙子没活到写 done（60s 后才写，3s 内写出来 = 它没被杀）。
	if _, statErr := os.Stat(done); statErr == nil {
		t.Error("孙子活过了 job 关闭（写出了 done）——脱离进程树的后代没被收容")
	}
	// 断言 2：helper 二进制不再被任何进程占用（孙子死了才会释放映像）。
	// 这条比断言 1 更直接：孙子活着 = 它的 exe 文件被锁 = 删不掉。
	if err := os.Remove(helper); err != nil {
		t.Errorf("孙子仍然活着（helper 二进制被进程占用、删不掉：%v）——job object 没收容住脱离进程树的后代", err)
	}
}

// TestSandboxHelperSpawnDetached 是上面测试的 helper 模式（不直接跑）。
func TestSandboxHelperSpawnDetached(t *testing.T) {
	args := helperArgs()
	if len(args) < 2 {
		t.Skip("helper 模式：只被 TestJobObjectContainsDetachedGrandchild 以子进程方式调用")
	}
	alive, done := args[0], args[1]
	if err := os.WriteFile(alive, []byte("alive"), 0o644); err != nil {
		t.Fatalf("写 alive: %v", err)
	}
	// 孙子：同一个二进制再起一次，睡 60s 后写 done。
	gc := exec.Command(os.Args[0], "-test.run", "TestSandboxHelperSleep", "--", done)
	gc.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if err := gc.Start(); err != nil {
		t.Fatalf("起孙子: %v", err)
	}
	// 子进程立刻退出，不等孙子。
}

// TestSandboxHelperSleep 是孙子的睡眠模式。
func TestSandboxHelperSleep(t *testing.T) {
	args := helperArgs()
	if len(args) < 1 {
		t.Skip("helper 模式")
	}
	time.Sleep(60 * time.Second)
	_ = os.WriteFile(args[0], []byte("done"), 0o644)
}

// TestSandboxHelperWriteSleepDelete 是"写标记 → 睡 60s → 删标记"的 helper 模式。
//
// 用于 KILL_ON_JOB_CLOSE 的判据：只有自然跑完才会删标记。
func TestSandboxHelperWriteSleepDelete(t *testing.T) {
	args := helperArgs()
	if len(args) < 1 {
		t.Skip("helper 模式")
	}
	if err := os.WriteFile(args[0], []byte("x"), 0o644); err != nil {
		t.Fatalf("写标记: %v", err)
	}
	time.Sleep(60 * time.Second)
	_ = os.Remove(args[0])
}

// TestSandboxHelperSpawnProbe 是"还能不能再起一个进程"的探针 helper 模式。
//
// 判据写进 probe 文件："spawned"（起成了）/ "spawn-failed"（起不来）。
// 用于 job object 的活动进程上限（F3）：上限是 1 时，子进程再起一个就该失败。
func TestSandboxHelperSpawnProbe(t *testing.T) {
	args := helperArgs()
	if len(args) < 2 {
		t.Skip("helper 模式：只被 TestMaxActiveProcessesLimit 以子进程方式调用")
	}
	probe, marker := args[0], args[1]
	if err := os.WriteFile(probe, []byte("start"), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "写探针失败: %v\n", err)
	}
	gc := exec.Command(os.Args[0], "-test.run", "TestSandboxHelperWriteFile", "--", marker)
	gc.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if err := gc.Run(); err != nil {
		_ = os.WriteFile(probe, []byte("spawn-failed"), 0o644)
		return
	}
	_ = os.WriteFile(probe, []byte("spawned"), 0o644)
}

// TestSandboxHelperWriteFile 是"写个文件就退出"的 helper 模式（给上面的探针当子进程）。
func TestSandboxHelperWriteFile(t *testing.T) {
	args := helperArgs()
	if len(args) < 1 {
		t.Skip("helper 模式")
	}
	_ = os.WriteFile(args[0], []byte("x"), 0o644)
}

// TestSandboxHelperWriteVariants 是"各种写法分别什么结果"的诊断 helper 模式。
func TestSandboxHelperWriteVariants(t *testing.T) {
	args := helperArgs()
	if len(args) < 1 {
		t.Skip("helper 模式")
	}
	target := args[0]
	if err := os.WriteFile(target, []byte("via-os.WriteFile"), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "os.WriteFile 失败: %v\n", err)
	}
	f, err := os.Create(target + ".create")
	if err != nil {
		fmt.Fprintf(os.Stderr, "os.Create 失败: %v\n", err)
		return
	}
	n, werr := f.Write([]byte("via-os.Create"))
	fmt.Fprintf(os.Stderr, "os.Create+Write: n=%d err=%v\n", n, werr)
	cerr := f.Close()
	fmt.Fprintf(os.Stderr, "Close err=%v\n", cerr)

	// 同步写（不走 Go 的 overlapped I/O）：用来区分"是 DACL 拒绝"还是"是 overlapped I/O 的问题"。
	syncTarget := target + ".sync"
	pathp, _ := syscall.UTF16PtrFromString(syncTarget)
	h, herr := syscall.CreateFile(pathp, syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil, syscall.CREATE_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	fmt.Fprintf(os.Stderr, "CreateFile: err=%v\n", herr)
	if herr == nil {
		var written uint32
		serr := syscall.WriteFile(h, []byte("via-sync-write"), &written, nil)
		fmt.Fprintf(os.Stderr, "同步 WriteFile: written=%d err=%v\n", written, serr)
		syscall.CloseHandle(h)
	}

	// 复刻 Go 的 os.WriteFile，但用裸 syscall（定位是"句柄"还是"Go 的 poll 路径"）。
	goPath := target + ".goopen"
	goPathp, _ := syscall.UTF16PtrFromString(goPath)
	gh, gherr := syscall.CreateFile(goPathp, syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE,
		nil, syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	fmt.Fprintf(os.Stderr, "CreateFile(Go 风格 sharemode): err=%v\n", gherr)
	if gherr == nil {
		var written uint32
		serr := syscall.WriteFile(gh, []byte("via-go-style"), &written, nil)
		fmt.Fprintf(os.Stderr, "WriteFile(Go 风格): written=%d err=%v\n", written, serr)
		syscall.CloseHandle(gh)
	}

	// 写**父进程预先创建**的文件（不经过子进程创建）：用来区分"创建"与"写"。
	preTarget := target + ".pre"
	if err := os.WriteFile(preTarget, []byte("pre-existing"), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "os.WriteFile(预先存在) 失败: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "os.WriteFile(预先存在) 成功\n")
	}
	prePath, _ := syscall.UTF16PtrFromString(preTarget)
	ph, pherr := syscall.CreateFile(prePath, syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil, syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	fmt.Fprintf(os.Stderr, "CreateFile(预先存在): err=%v\n", pherr)
	if pherr == nil {
		var written uint32
		serr := syscall.WriteFile(ph, []byte("via-sync-write-pre"), &written, nil)
		fmt.Fprintf(os.Stderr, "同步 WriteFile(预先存在): written=%d err=%v\n", written, serr)
		syscall.CloseHandle(ph)
	}
}

// TestErrorModeSuppressed 对应 V10 "WER 压制：断言 SetErrorMode 生效"。
func TestErrorModeSuppressed(t *testing.T) {
	allowed, _ := sandboxTestRoots(t)
	env := newSandboxEnv(t, allowed)

	var ok bool
	for _, d := range diagKinds(env.Diagnostics(), "wer") {
		if d.OK && strings.Contains(d.Detail, "SetErrorMode") {
			ok = true
		}
	}
	if !ok {
		t.Fatalf("SetErrorMode 埋点没报告成功：\n%s", diagsOf(env.Diagnostics()))
	}
	// 直接读进程 error mode（GetErrorMode 是**纯读**；注意 SetErrorMode(0) 不是读——
	// 它会把 error mode 清零再返回旧值，见 wer_windows.go 的 F4 说明）。
	if got := sandbox.CurrentErrorModeForTest(); got&0x3 != 0x3 {
		t.Errorf("当前 error mode = 0x%x，期望含 SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX (0x3)", got)
	}
}

// TestCmdDirOutsideAllowedSetFallsBack 钉死 F15：子进程的工作目录必须在允许集内。
//
// 不在允许集内 → **回落沙箱临时根**（而不是直接报错）：受限 token 的子进程
// 连那个目录都进不去（访问检查就拒了），报错信息离原因很远；
// 回落到临时根 = 至少能跑起来，"能碰哪些目录"由允许集说了算。
func TestCmdDirOutsideAllowedSetFallsBack(t *testing.T) {
	allowed, outside := sandboxTestRoots(t)
	env := newSandboxEnv(t, allowed)

	res, err := env.Run(context.Background(), core.ExecSpec{
		Name: "cmd",
		Args: []string{"/c", "cd"},
		Dir:  outside, // 不在允许集内
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := strings.TrimSpace(string(res.Stdout))
	want := strings.ToLower(filepath.Clean(env.Policy().SandboxTempRoot))
	if !strings.Contains(strings.ToLower(got), want) {
		t.Errorf("工作目录没回落到沙箱临时根：got=%q want=%q", got, want)
	}

	// 允许集内的工作目录：原样使用。
	inside := filepath.Join(allowed, "sub")
	if err := os.MkdirAll(inside, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	res2, err := env.Run(context.Background(), core.ExecSpec{
		Name: "cmd",
		Args: []string{"/c", "cd"},
		Dir:  inside,
	})
	if err != nil {
		t.Fatalf("Run（允许集内）: %v", err)
	}
	if got2 := strings.TrimSpace(string(res2.Stdout)); !strings.Contains(strings.ToLower(got2), strings.ToLower(filepath.Base(inside))) {
		t.Errorf("允许集内的工作目录没被采用：got=%q", got2)
	}
}

// TestErrorModeRestoredOnClose 钉死 F5：error mode 是"借"来的，Close 必须还回去。
//
// 压制是进程级的（子进程继承），如果不复原，宿主进程后续的崩溃弹窗策略就被
// 沙箱永久改掉了——沙箱关了，行为却没回去。
func TestErrorModeRestoredOnClose(t *testing.T) {
	before := sandbox.CurrentErrorModeForTest()
	allowed, _ := sandboxTestRoots(t)
	p := sandbox.DefaultPolicy()
	p.AllowedRoots = []string{allowed}
	env, err := sandbox.New(p)
	if err != nil {
		t.Fatalf("sandbox.New: %v", err)
	}
	// 压制期间：必须含上压制位。
	if got := sandbox.CurrentErrorModeForTest(); got&0x3 != 0x3 {
		t.Errorf("压制期间 error mode = 0x%x，期望含 0x3", got)
	}
	if err := env.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := sandbox.CurrentErrorModeForTest(); got != before {
		t.Errorf("Close 之后 error mode 没复原：前 0x%x，后 0x%x", before, got)
	}
}

// TestRunReturnsWhenAttachFailsWithoutTaskkill 是 N4 的**判别性**用例：
// 把 taskkill 兜底**关掉**（模拟"taskkill 对挂起进程失败"），Attach 失败后
// Run 仍必须返回——靠的是 Process.Kill()（TerminateProcess 对挂起进程必定成功）。
//
// 本机实测：taskkill 对挂起进程**常常**还能杀掉，所以"关掉兜底"才是能区分
// 修好没修好的那把尺子（否则两条路径都会返回，测不出差别）。
func TestRunReturnsWhenAttachFailsWithoutTaskkill(t *testing.T) {
	sandbox.SetKillTreeFallbackForTest(false)
	defer sandbox.SetKillTreeFallbackForTest(true)
	runAttachFailureMustReturn(t)
}

// TestErrorModeRestoredWhenOriginalIsZero 钉死 F5/N7 的边角：
// **原值恰好是 0** 时也必须复原。
//
// 之前的判据是 `prevErrorMode != 0` → 原值是 0 时 Close **跳过复原** →
// 压制位（0x3）永久留在宿主进程上（宿主之后的崩溃弹窗策略被沙箱改掉了）。
func TestErrorModeRestoredWhenOriginalIsZero(t *testing.T) {
	orig := sandbox.SetErrorModeForTest(0) // 模拟"原值恰好是 0"
	defer sandbox.SetErrorModeForTest(orig)

	allowed, _ := sandboxTestRoots(t)
	p := sandbox.DefaultPolicy()
	p.AllowedRoots = []string{allowed}
	env, err := sandbox.New(p)
	if err != nil {
		t.Fatalf("sandbox.New: %v", err)
	}
	if got := sandbox.CurrentErrorModeForTest(); got&0x3 != 0x3 {
		t.Fatalf("压制没生效：error mode = 0x%x", got)
	}
	if err := env.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := sandbox.CurrentErrorModeForTest(); got != 0 {
		t.Errorf("原值为 0 时 Close 没复原 error mode：got=0x%x（压制位被永久留下了）", got)
	}
}

// TestWERKeepsUserExistingExclusion 钉死 F7/N1 的**端到端**行为：
// 用户自己早就排除过的可执行名，沙箱 Close 之后必须还在。
//
// 做法：用**真的 API**（WerAddExcludedApplication）预置"用户原有项"——
// 预置的名字就是**宿主自己**的可执行名（沙箱必然会去排除它），
// 然后建 env → Close → 断言那条还在。
//
// 这条测试在修复前是红的（那时"用户原有项"的判断读错了注册表位置，
// 判断恒为假 → 我们把它当成自己加的 → Close 删掉用户的设置）。
func TestWERKeepsUserExistingExclusion(t *testing.T) {
	hostExe := filepath.Base(os.Args[0]) // 沙箱排除的正是它（宿主可执行名）
	if err := sandbox.WerExcludeForTest(hostExe); err != nil {
		t.Fatalf("预置用户排除项（%s）: %v", hostExe, err)
	}
	defer func() { _ = sandbox.WerUnexcludeForTest(hostExe) }()

	allowed, _ := sandboxTestRoots(t)
	env := newSandboxEnv(t, allowed)
	if err := env.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !sandbox.WerAlreadyExcludedForTest(hostExe) {
		t.Errorf("Close 把用户原有的 WER 排除项删掉了（%s）——F7/N1 没修好", hostExe)
	}
}

// TestWERExcludedApplicationRegistered 对应 V10 "WER 排除注册生效"。
//
// WerAddExcludedApplication 写 HKCU（**不需要管理员**）。断言"我们加过的那些
// 在 Close 时被撤掉"——绝不碰用户原本就有的排除项。
func TestWERExcludedApplicationRegistered(t *testing.T) {
	allowed, _ := sandboxTestRoots(t)
	env := newSandboxEnv(t, allowed)

	var excluded []string
	for _, d := range diagKinds(env.Diagnostics(), "wer") {
		if d.OK && strings.HasPrefix(d.Detail, "WER 排除") {
			excluded = append(excluded, d.Detail)
		}
	}
	if len(excluded) == 0 {
		t.Fatalf("没有登记任何 WER 排除项：\n%s", diagsOf(env.Diagnostics()))
	}
	t.Logf("登记的 WER 排除项：%v", excluded)

	if err := env.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, d := range diagKinds(env.Diagnostics(), "wer") {
		if strings.Contains(d.Detail, "撤 WER 排除项失败") {
			t.Errorf("Close 没能撤掉我们加过的排除项：%s", d.Detail)
		}
	}
}

// TestDiagnosticsReportAllThreeLegs 是"埋点诚实性"的总断言：三条腿 + WER 都有记录。
//
// 先跑一条真命令：job object 是**按执行**建的（Begin 里才建），不跑一次就没有记录。
// 这也顺便证明埋点不是装配期的静态文案，而是随真实执行累积的。
func TestDiagnosticsReportAllThreeLegs(t *testing.T) {
	allowed, _ := sandboxTestRoots(t)
	env := newSandboxEnv(t, allowed)
	if _, err := env.Run(context.Background(), core.ExecSpec{
		Name: "cmd",
		Args: []string{"/c", "echo", "diag-probe"},
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, kind := range []string{"job-object", "restricted-token", "file-acl", "wer"} {
		if len(diagKinds(env.Diagnostics(), kind)) == 0 {
			t.Errorf("埋点缺 %q 这一类：\n%s", kind, diagsOf(env.Diagnostics()))
		}
	}
	// 三条腿都必须是 OK（退化会在这里露出来）。
	for _, kind := range []string{"job-object", "restricted-token", "file-acl"} {
		for _, d := range diagKinds(env.Diagnostics(), kind) {
			if !d.OK {
				t.Errorf("%s 这条腿退化了：%s", kind, d.Detail)
			}
		}
	}
}

// TestDangerousRootGuard 验证"绝不给卷根 / 用户 profile / Windows 目录设 ACL"的护栏。
//
// 同时钉死**方向**：护栏只拒"允许根在受保护目录**之上**"（传播会爬上去），
// 不拒"允许根在受保护目录**之下**"（如临时目录里的 workspace——正常使用形状）。
func TestDangerousRootGuard(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("取不到用户目录")
	}
	// 受保护目录本身 → 拒。
	p := sandbox.DefaultPolicy()
	p.AllowedRoots = []string{home}
	if _, err := sandbox.New(p); err == nil {
		t.Error("把用户配置文件目录当允许根居然通过了——护栏失效")
	} else if !strings.Contains(err.Error(), "拒绝给允许根") {
		t.Errorf("错误信息没说清是护栏拒的：%v", err)
	}

	// 受保护目录的**子目录**当允许根 → 放行（传播到不了上面）。
	sub := filepath.Join(home, "AppData", "Local", "Temp", "alasol-sandbox-guard-probe")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	defer os.RemoveAll(sub)
	p2 := sandbox.DefaultPolicy()
	p2.AllowedRoots = []string{sub}
	env2, err := sandbox.New(p2)
	if err != nil {
		t.Errorf("受保护目录**之下**的允许根不该被拒（护栏方向错了）：%v", err)
		return
	}
	// 必须 Close：不关的话沙箱的临时根留在磁盘上（WER 排除项也留在注册表里）——N5。
	tempRoot := env2.Policy().SandboxTempRoot
	defer func() {
		_ = env2.Close()
		if tempRoot != "" {
			if _, statErr := os.Stat(tempRoot); statErr == nil {
				t.Errorf("Close 之后沙箱临时根还在：%s", tempRoot)
			}
		}
	}()
}

// TestACLMaxObjectsGuard 验证对象数超限时 fail-close（防止误配成大根爬满整盘）。
func TestACLMaxObjectsGuard(t *testing.T) {
	allowed, _ := sandboxTestRoots(t)
	// 造 40 个文件，上限设 10 → 必须被拒。
	for i := 0; i < 40; i++ {
		d := filepath.Join(allowed, "sub", string(rune('a'+i%26))+string(rune('a'+i/26)))
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.WriteFile(filepath.Join(d, "f.txt"), []byte("x"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}
	p := sandbox.DefaultPolicy()
	p.AllowedRoots = []string{allowed}
	p.ACLMaxObjects = 10
	if _, err := sandbox.New(p); err == nil {
		t.Error("对象数超限居然通过了——护栏失效")
	}
}

// TestJobObjectKillOnCloseKillsStragglers 是"取消会杀掉子进程"的验证：
// 起一个长命子进程，取消后 Run 返回，子进程必须死。
//
// 判据用"标记文件是否还在"：helper 先写标记、睡 60s、再删标记。它只有在**自然跑完**
// 时才会删。被杀 → 标记永远留着。所以"Run 返回后标记还在"就是被杀死的确定性证据，
// 不用等 60s。
//
// 为什么用 helper 二进制而不是 `ping -n 60`：本机实测 `ping -n N 127.0.0.1`
// 在回环上几乎是瞬时的（5 个回显 0.04s 跑完），当不了睡眠源。
//
// # 名字与真实证明力的边界（F22）
//
// 这个测试证明的是"**取消会杀掉子进程**"，**不是**"KILL_ON_JOB_CLOSE 杀了它"：
// 取消路径上先动的是 taskkill /T /F 兜底，job 句柄要到 Run 返回后的 Release 才关。
// 真正证明 **job object 收口**（含"脱离进程树的后代"）的是
// TestJobObjectContainsDetachedGrandchild——那里的孙子 taskkill /T 够不着，
// 只有 job 的 KILL_ON_JOB_CLOSE 能收掉它。
func TestRunCancelKillsChildProcess(t *testing.T) {
	allowed, _ := sandboxTestRoots(t)
	env := newSandboxEnv(t, allowed)

	helper := filepath.Join(allowed, "sandbox-helper.exe")
	if err := copyFile(os.Args[0], helper); err != nil {
		t.Fatalf("复制 helper 进允许根: %v", err)
	}
	marker := filepath.Join(allowed, "survivor.txt")
	_ = os.Remove(marker)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = env.Run(ctx, core.ExecSpec{
			Name: helper,
			Args: []string{"-test.run", "TestSandboxHelperWriteSleepDelete", "--", marker},
		})
	}()
	waitAlive(t, marker, 20*time.Second)
	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("取消后 Run 没返回")
	}
	time.Sleep(3 * time.Second)
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("标记文件被删了：子进程自然跑完了，说明取消没杀掉它（%v）", err)
	}
}

// TestEnvCloseKillsRunningChild 证明 **job object 的 KILL_ON_JOB_CLOSE** 真的在收口：
// 关掉 taskkill 兜底（只留 job 收口），然后关掉 env —— job 句柄一关，job 里的进程
// 全死，跑着的 Run 也会返回。
//
// 这条测试补上了 F22 指出的缺口：之前"进程被杀掉"到底是哪条路径干的，分不清。
func TestEnvCloseKillsRunningChild(t *testing.T) {
	allowed, _ := sandboxTestRoots(t)
	env := newSandboxEnv(t, allowed)

	helper := filepath.Join(allowed, "sandbox-helper.exe")
	if err := copyFile(os.Args[0], helper); err != nil {
		t.Fatalf("复制 helper 进允许根: %v", err)
	}
	marker := filepath.Join(allowed, "survivor-envclose.txt")
	_ = os.Remove(marker)

	// 只留 job object 收口：taskkill 兜底关掉。
	sandbox.SetKillTreeFallbackForTest(false)
	defer sandbox.SetKillTreeFallbackForTest(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = env.Run(context.Background(), core.ExecSpec{
			Name: helper,
			Args: []string{"-test.run", "TestSandboxHelperWriteSleepDelete", "--", marker},
		})
	}()
	waitAlive(t, marker, 20*time.Second)

	// 关 env → job 句柄关闭 → KILL_ON_JOB_CLOSE → job 里的进程全死。
	if err := env.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("env 关掉后 Run 没返回——job object 没有收口（KILL_ON_JOB_CLOSE 没生效）")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("标记文件被删了：子进程自然跑完了，说明 job object 没杀掉它（%v）", err)
	}
}

// TestMaxActiveProcessesLimit 钉死 job object 的活动进程上限（F3）。
//
// 上限是"fork 炸弹有界化"的那条线：0 = 默认 64（文档承诺的默认值必须真的生效）。
//
// 判据分两半：
//
//   - **埋点**：默认（0）时埋点必须报告实际生效的上限（64）——不然"0 = 默认 64"
//     只是文档上的一句话；
//   - **功能**：上限 = 1 时，子进程**再起一个进程**必须失败（活动进程数已经是 1）。
//     探针是本测试二进制自己（放在允许根里——受限 token 的子进程只能起允许根里的
//     可执行文件）。
func TestMaxActiveProcessesLimit(t *testing.T) {
	allowed, _ := sandboxTestRoots(t)

	// 默认：0 → DefaultMaxActiveProcesses（埋点里必须能看到实际生效值）。
	p := sandbox.DefaultPolicy()
	p.AllowedRoots = []string{allowed}
	env, err := sandbox.New(p)
	if err != nil {
		t.Fatalf("sandbox.New: %v", err)
	}
	defer env.Close()
	if got := spawnProbe(t, env, allowed, 0); got != "spawned" {
		t.Errorf("默认上限下子进程起不了第二个进程（probe=%q）——上限过紧", got)
	}
	// 埋点（job object 是**按执行**建的，跑过一次才有记录）。
	var sawLimit bool
	for _, d := range diagKinds(env.Diagnostics(), "job-object") {
		if d.OK && strings.Contains(d.Detail, fmt.Sprintf("活动进程上限 %d", sandbox.DefaultMaxActiveProcesses)) {
			sawLimit = true
		}
	}
	if !sawLimit {
		t.Errorf("埋点没报告实际生效的活动进程上限（应为 %d）：\n%s",
			sandbox.DefaultMaxActiveProcesses, diagsOf(env.Diagnostics()))
	}

	// 上限 = 1：再起一个进程就必须失败。
	p2 := sandbox.DefaultPolicy()
	p2.AllowedRoots = []string{allowed}
	p2.MaxActiveProcesses = 1
	env2, err := sandbox.New(p2)
	if err != nil {
		t.Fatalf("sandbox.New(上限=1): %v", err)
	}
	defer env2.Close()
	if got := spawnProbe(t, env2, allowed, 1); got != "spawn-failed" {
		t.Errorf("活动进程上限=1 时子进程还能起第二个进程（probe=%q）——上限没生效", got)
	}
}

// spawnProbe 跑一次"能不能再起一个进程"的探针，返回探针文件的内容。
func spawnProbe(t *testing.T, env *sandbox.Env, allowed string, maxActive int) string {
	t.Helper()
	helper := filepath.Join(allowed, fmt.Sprintf("sandbox-probe-%d.exe", maxActive))
	if err := copyFile(os.Args[0], helper); err != nil {
		t.Fatalf("复制 helper 进允许根: %v", err)
	}
	probe := filepath.Join(allowed, fmt.Sprintf("spawn-probe-%d.txt", maxActive))
	marker := filepath.Join(allowed, fmt.Sprintf("spawn-marker-%d.txt", maxActive))
	t.Logf("探针路径：probe=%s marker=%s", probe, marker)
	_ = os.Remove(probe)
	_ = os.Remove(marker)
	res, err := env.Run(context.Background(), core.ExecSpec{
		Name: helper,
		Args: []string{"-test.run", "TestSandboxHelperSpawnProbe", "--", probe, marker},
	})
	if err != nil {
		t.Fatalf("Run 探针: %v", err)
	}
	_ = res
	data, readErr := os.ReadFile(probe)
	if readErr != nil {
		t.Fatalf("读探针结果: %v（helper stdout=%q stderr=%q）", readErr, res.Stdout, res.Stderr)
	}
	if len(data) == 0 {
		ents, _ := os.ReadDir(allowed)
		var names []string
		for _, e := range ents {
			names = append(names, e.Name())
		}
		markerData, mErr := os.ReadFile(marker)
		t.Fatalf("探针结果是空文件（helper stdout=%q stderr=%q；允许根内容=%v；marker=%q err=%v）",
			res.Stdout, res.Stderr, names, markerData, mErr)
	}
	return string(data)
}

// TestSandboxTempRootRemovedOnClose 钉死 F14：派生的沙箱临时根在 Close 时被删掉。
//
// （之前的注释说"随进程退出自然清理"——那是假的：%TEMP% 不会自动清它。）
func TestSandboxTempRootRemovedOnClose(t *testing.T) {
	allowed, _ := sandboxTestRoots(t)
	p := sandbox.DefaultPolicy()
	p.AllowedRoots = []string{allowed}
	env, err := sandbox.New(p)
	if err != nil {
		t.Fatalf("sandbox.New: %v", err)
	}
	tempRoot := env.Policy().SandboxTempRoot
	if tempRoot == "" {
		t.Fatal("沙箱临时根为空")
	}
	if _, err := os.Stat(tempRoot); err != nil {
		t.Fatalf("沙箱临时根没建起来: %v", err)
	}
	if err := env.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(tempRoot); !os.IsNotExist(err) {
		t.Errorf("Close 之后沙箱临时根还在：%s（err=%v）", tempRoot, err)
	}
}

// waitAlive 等文件出现（带超时）。
func waitAlive(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("等待 %s 出现超时（%v）", path, timeout)
}

// copyFile 复制文件（helper 二进制要放进允许根，否则受限 token 遍历不了）。
func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o755)
}

// TestEnvReopenAfterClose 验证"关掉再开一个"是可用的（Close 不留下全局副作用）。
//
// WER 排除项是**持久的用户级注册表写入**，Close 必须把它们撤掉，否则第二个 env
// 会看到"别人加过的"状态、而第一个的痕迹永远留在用户机器上。
func TestEnvReopenAfterClose(t *testing.T) {
	allowed, _ := sandboxTestRoots(t)
	p := sandbox.DefaultPolicy()
	p.AllowedRoots = []string{allowed}

	first, err := sandbox.New(p)
	if err != nil {
		t.Fatalf("第一次 New: %v", err)
	}
	if _, err := first.Run(context.Background(), core.ExecSpec{Name: "cmd", Args: []string{"/c", "echo", "one"}}); err != nil {
		t.Fatalf("第一次 Run: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("第一次 Close: %v", err)
	}

	second, err := sandbox.New(p)
	if err != nil {
		t.Fatalf("Close 之后再 New 失败（全局副作用没清干净？）：%v", err)
	}
	defer second.Close()
	res, err := second.Run(context.Background(), core.ExecSpec{Name: "cmd", Args: []string{"/c", "echo", "two"}})
	if err != nil {
		t.Fatalf("第二次 Run: %v", err)
	}
	if !strings.Contains(string(res.Stdout), "two") {
		t.Errorf("第二次 Run 输出不对：%q", res.Stdout)
	}
}

// TestConcurrentRunsIsolated 验证多个 Run 并发时互不干扰（各自独立的 job object）。
//
// 这是沙箱相对 local 的一个真实增量：每个执行一个 job，取消一个不会连带杀掉别的。
// 同时是 WER 记账并发安全的回归测试（-race 曾在这里抓到一个裸 map 写竞争）。
func TestConcurrentRunsIsolated(t *testing.T) {
	allowed, _ := sandboxTestRoots(t)
	env := newSandboxEnv(t, allowed)

	const n = 6
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := env.Run(context.Background(), core.ExecSpec{
				Name: "cmd",
				Args: []string{"/c", "echo", "concurrent"},
			})
			if err != nil {
				errs <- err
				return
			}
			if res.ExitCode != 0 {
				errs <- fmt.Errorf("run %d exit=%d stderr=%q", i, res.ExitCode, res.Stderr)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("并发 Run 失败：%v", err)
	}
}
