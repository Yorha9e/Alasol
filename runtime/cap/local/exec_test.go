package local

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/core"
)

// echoCmd 返回当前平台上"回显一段固定文本"的命令（跨平台可跑）。
// Windows 用 cmd /c echo，Unix 用 echo——这是**测试代码**里唯一允许出现 shell 的地方，
// 目的是证明 Exec 能把带参数的命令跑起来；被测代码本身不走 shell。
func echoCmd() (name string, args []string) {
	if runtime.GOOS == "windows" {
		return "cmd", []string{"/c", "echo", "hello-cap"}
	}
	return "echo", []string{"hello-cap"}
}

// printEnvCmd 返回"打印某个环境变量值"的命令。
func printEnvCmd(key string) (name string, args []string) {
	if runtime.GOOS == "windows" {
		return "cmd", []string{"/c", "echo", "%" + key + "%"}
	}
	return "sh", []string{"-c", "printf %s \"$" + key + "\""}
}

// failCmd 返回"以指定退出码失败"的命令。
func failCmd(code string) (name string, args []string) {
	if runtime.GOOS == "windows" {
		return "cmd", []string{"/c", "exit", code}
	}
	return "sh", []string{"-c", "exit " + code}
}

// sleepCmd 返回"长时间不退出"的命令（用于取消/杀进程测试）。
//
// 自然寿命故意选得长（约 1 分钟）：这样"取消没生效"的回归会表现为测试跑满 60s，
// 而正常路径是亚秒级——时间断言因此对机器负载不敏感（race 下并行跑多个包时
// taskkill 冷启动可能吃掉几秒，见 TestExecContextCancelKillsProcess 的注释）。
func sleepCmd() (name string, args []string) {
	if runtime.GOOS == "windows" {
		// ping -n 60 约 59 秒：本地回环、不联网、无窗口。
		return "cmd", []string{"/c", "ping", "-n", "60", "127.0.0.1", ">", "nul"}
	}
	return "sleep", []string{"60"}
}

// catCmd 返回"打印一个文件内容"的命令（用于验证 spec.Dir 生效）。
func catCmd() (name string, args []string) {
	if runtime.GOOS == "windows" {
		return "cmd", []string{"/c", "type", "marker.txt"}
	}
	return "cat", []string{"marker.txt"}
}

func TestExecEcho(t *testing.T) {
	ex := Exec{}
	name, args := echoCmd()
	res, err := ex.Run(context.Background(), core.ExecSpec{Name: name, Args: args})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ExitCode != 0 {
		t.Errorf("ExitCode = %d，期望 0（stderr=%q）", res.ExitCode, res.Stderr)
	}
	if got := strings.TrimSpace(string(res.Stdout)); got != "hello-cap" {
		t.Errorf("Stdout = %q，期望 hello-cap", got)
	}
}

func TestExecNonZeroExitIsNotError(t *testing.T) {
	// 非零退出是**业务信息**不是 error：ExitCode 带上，err 必须为 nil。
	ex := Exec{}
	name, args := failCmd("3")
	res, err := ex.Run(context.Background(), core.ExecSpec{Name: name, Args: args})
	if err != nil {
		t.Fatalf("非零退出不应返回 error：%v", err)
	}
	if res.ExitCode != 3 {
		t.Errorf("ExitCode = %d，期望 3", res.ExitCode)
	}
}

func TestExecEnvAndDir(t *testing.T) {
	ex := Exec{}
	dir := t.TempDir()
	const secret = "cap-secret-value"
	if err := os.WriteFile(filepath.Join(dir, "marker.txt"), []byte("marker-ok"), 0o644); err != nil {
		t.Fatal(err)
	}

	// env 生效：spec.Env 是**整份替换**（core.ExecSpec 约定），所以要显式带上 PATH，
	// 否则 sh/cmd 自己可能起不来。
	env := append(os.Environ(), "ALASOL_TEST_ENV="+secret)
	name, args := printEnvCmd("ALASOL_TEST_ENV")
	res, err := ex.Run(context.Background(), core.ExecSpec{Name: name, Args: args, Env: env})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := strings.TrimSpace(string(res.Stdout)); got != secret {
		t.Errorf("Stdout = %q，期望 %q（env 未注入子进程）", got, secret)
	}

	// Dir 生效：在 tempdir 里 cat marker.txt。
	name, args = catCmd()
	res, err = ex.Run(context.Background(), core.ExecSpec{Name: name, Args: args, Dir: dir})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := strings.TrimSpace(string(res.Stdout)); got != "marker-ok" {
		t.Errorf("Stdout = %q，期望 marker-ok（spec.Dir 未生效）", got)
	}
}

func TestExecInheritsEnvWhenEmpty(t *testing.T) {
	// spec.Env 为空 = 继承父进程 env。
	ex := Exec{}
	const key = "ALASOL_INHERITED"
	t.Setenv(key, "inherited-value")
	name, args := printEnvCmd(key)
	res, err := ex.Run(context.Background(), core.ExecSpec{Name: name, Args: args})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := strings.TrimSpace(string(res.Stdout)); got != "inherited-value" {
		t.Errorf("Stdout = %q，期望 inherited-value", got)
	}
}

func TestExecErrors(t *testing.T) {
	ex := Exec{}
	if _, err := ex.Run(context.Background(), core.ExecSpec{Name: "   "}); err == nil {
		t.Error("空 Name 应报错（不猜可执行文件）")
	}
	// 可执行文件不存在。
	if _, err := ex.Run(context.Background(), core.ExecSpec{Name: "alasol-no-such-binary-xyz"}); err == nil {
		t.Error("不存在的可执行文件应报错")
	}
	// **不走 shell** 的实证：带 shell 元字符的名字不会被解释成两条命令，
	// 而是"找不到这个可执行文件"。
	if _, err := ex.Run(context.Background(), core.ExecSpec{Name: "echo hi; echo pwned"}); err == nil {
		t.Error("含元字符的 Name 应因找不到可执行文件而失败（证明没有走 shell）")
	}
}

func TestExecContextCancelKillsProcess(t *testing.T) {
	ex := Exec{}
	name, args := sleepCmd()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	res, err := ex.Run(ctx, core.ExecSpec{Name: name, Args: args})
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v，期望 errors.Is Canceled", err)
	}
	// 取消必须**远早于**命令自然结束（sleepCmd 自然寿命约 1 分钟）。
	// 上限取 20s：实测正常路径亚秒级；race 下与其它包并行跑时 taskkill 冷启动
	// 最坏观察到 ~11s（Cmd.WaitDelay 的两段窗口都用满），20s 给足余量，
	// 而"杀进程失败"的回归会直接跑到 60s 自然寿命 → 仍能被抓到。
	if elapsed > 20*time.Second {
		t.Errorf("取消后 %v 才返回，进程没被杀掉（自然寿命约 60s）", elapsed)
	}
	if res.ExitCode == 0 {
		t.Logf("ExitCode = %d（被信号杀掉时不是 0，属正常）", res.ExitCode)
	}
}

func TestExecContextDeadline(t *testing.T) {
	ex := Exec{}
	name, args := sleepCmd()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	if _, err := ex.Run(ctx, core.ExecSpec{Name: name, Args: args}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v，期望 DeadlineExceeded", err)
	}
}

func TestExecAlreadyCancelled(t *testing.T) {
	ex := Exec{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	name, args := echoCmd()
	if _, err := ex.Run(ctx, core.ExecSpec{Name: name, Args: args}); err == nil {
		t.Fatal("已取消的 ctx 应报错且不启动进程")
	}
}

func TestExecCappedBuffer(t *testing.T) {
	var b cappedBuffer
	// 写满上限：之后的内容被丢弃但不报错（io.Copy 不会炸）。
	if n, err := b.Write(make([]byte, maxCapturedOutput)); err != nil || n != maxCapturedOutput {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if n, err := b.Write([]byte("overflow")); err != nil || n != len("overflow") {
		t.Fatalf("超限写入 n=%d err=%v（应假装收下）", n, err)
	}
	if got := len(b.bytes()); got != maxCapturedOutput {
		t.Errorf("缓冲长度 = %d，期望 %d（超限部分被丢）", got, maxCapturedOutput)
	}
	if b.dropped != len("overflow") {
		t.Errorf("dropped = %d", b.dropped)
	}
}

// TestExecLargeOutputCapped 端到端验证大输出被截到上限（不撑爆内存）。
func TestExecLargeOutputCapped(t *testing.T) {
	if testing.Short() {
		t.Skip("短模式跳过：要生成 40MB 输出")
	}
	if runtime.GOOS == "windows" {
		t.Skip("Windows 无 yes/head：cappedBuffer 单测已覆盖上限逻辑")
	}
	ex := Exec{}
	// yes 无限输出，head 截到 40MB（> 32MB 上限）。
	res, err := ex.Run(context.Background(), core.ExecSpec{
		Name: "sh",
		Args: []string{"-c", "yes 0123456789abcdef | head -c 40000000"},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Stdout) > maxCapturedOutput {
		t.Errorf("stdout 未截断：%d 字节", len(res.Stdout))
	}
	if len(res.Stdout) != maxCapturedOutput {
		t.Errorf("stdout = %d，期望正好截到上限 %d", len(res.Stdout), maxCapturedOutput)
	}
}

// TestExecConcurrent 并发执行：验证无共享状态（-race 下跑）。
func TestExecConcurrent(t *testing.T) {
	ex := Exec{}
	const n = 6
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name, args := echoCmd()
			res, err := ex.Run(context.Background(), core.ExecSpec{Name: name, Args: args})
			if err != nil {
				errs <- err
				return
			}
			if strings.TrimSpace(string(res.Stdout)) != "hello-cap" {
				errs <- errors.New("输出串了：" + string(res.Stdout))
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestHideWindow Windows 上必须给子进程带 HideWindow（陷阱 5）；其它平台是空操作。
// 用反射而不是直接 import syscall：SysProcAttr 的字段集按平台不同，
// 直接写字段名会让这份测试在另一半平台编译不过。
func TestHideWindow(t *testing.T) {
	cmd := exec.Command("nonexistent-for-attr-check")
	hideWindow(cmd)
	if runtime.GOOS != "windows" {
		if cmd.SysProcAttr != nil {
			t.Error("非 Windows 平台不应设置 SysProcAttr")
		}
		return
	}
	if cmd.SysProcAttr == nil {
		t.Fatal("Windows 上 SysProcAttr 不应为 nil")
	}
	f := reflect.ValueOf(cmd.SysProcAttr).Elem().FieldByName("HideWindow")
	if !f.IsValid() {
		t.Fatalf("SysProcAttr（%T）没有 HideWindow 字段", cmd.SysProcAttr)
	}
	if !f.Bool() {
		t.Error("HideWindow 未开启（会弹控制台窗口）")
	}
}
