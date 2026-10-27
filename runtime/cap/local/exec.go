package local

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/core"
)

// Exec 是 core.ExecCapability 的本机实现（零值可用）。
//
// 并发安全：无共享状态，每次 Run 独立的 Cmd / buffer。
type Exec struct{}

// 编译期断言：Exec 必须满足 core.ExecCapability。
var _ core.ExecCapability = Exec{}

// maxCapturedOutput 是单次执行 stdout/stderr 各自的捕获上限（32MB）。
// 只防"命令无限输出把内存吃光"；正常命令远到不了。截断是**静默**的——
// 调用方若要告知模型输出被截断，请在工具层截断并标注（runtime/tools 就是这么做的）。
const maxCapturedOutput = 32 << 20

// waitDelay 是 Wait 等 I/O 管道关闭的硬上限（5s）。
//
// 为什么需要它：Windows 实测（本机，cmd /c ping 长跑 + ctx 取消）——ctx 取消时
// CommandContext 只杀**直接子进程** cmd.exe，而孙子进程 ping 还攥着 stdout/stderr
// 管道的写端，cmd.Wait 会一直挂到 ping 自己退出（实测 **29 秒**）。这不是理论问题，
// 是每次取消都要付的代价。两道防线：
//
//  1. cmd.Cancel 里**杀进程树**（Windows: taskkill /T /F /PID，陷阱 5）→ 孙子一起死，
//     管道立刻 EOF，Wait 正常返回（实测 ~0.2s）；
//  2. WaitDelay 兜底：万一 taskkill 不存在/失败，Wait 最多再等 5 秒就放弃管道，
//     返回 exec.ErrWaitDelay（我们照常报 ctx 错误），绝不无限挂起。
const waitDelay = 5 * time.Second

// Run implements core.ExecCapability。
//
// 语义（见包 doc）：
//
//   - **不走 shell**：spec.Name 直接是可执行文件（陷阱 7 精神）；
//   - ctx 结束 → **杀进程树**（Cancel 回调，Windows 走 taskkill /T /F /PID，陷阱 5）；
//   - 非零退出码**不是 error**（ExecResult.ExitCode 带上）；
//   - spec.Env 为空 = 继承父进程 env；非空 = **整份替换**（core.ExecSpec 的约定，
//     只含获准注入的键值 H17/H21——装配方要自己把该继承的放进去）。
func (Exec) Run(ctx context.Context, spec core.ExecSpec) (core.ExecResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctxErr(ctx); err != nil {
		return core.ExecResult{}, err
	}
	name := strings.TrimSpace(spec.Name)
	if name == "" {
		return core.ExecResult{}, fmt.Errorf("local: ExecSpec.Name 不能为空（不猜可执行文件）")
	}

	cmd := exec.CommandContext(ctx, name, spec.Args...)
	cmd.Dir = spec.Dir // 空 = 继承当前工作目录
	if len(spec.Env) > 0 {
		cmd.Env = spec.Env
	}
	hideWindow(cmd)             // Windows：不弹控制台窗口（陷阱 5）
	cmd.Cancel = func() error { // ctx 结束时杀整棵树，而不是只杀直接子进程
		killTree(cmd)
		return nil
	}
	cmd.WaitDelay = waitDelay // 管道关不上时的硬上限（见常量注释的实测）

	var stdout, stderr cappedBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	startErr := cmd.Start()
	if startErr != nil {
		// 起不来：可执行文件不存在 / 不是可执行 / Dir 不存在。
		return core.ExecResult{}, fmt.Errorf("local: 启动 %q 失败：%w", name, startErr)
	}
	waitErr := cmd.Wait()

	res := core.ExecResult{
		ExitCode: exitCode(cmd, waitErr),
		Stdout:   stdout.bytes(),
		Stderr:   stderr.bytes(),
	}

	// 取消/超时优先呈现：此时 waitErr 多半是 "signal: killed"，
	// 直接回它会让人以为是命令自己崩的。
	if err := ctx.Err(); err != nil {
		return res, fmt.Errorf("local: 执行 %q 被中止（%w）", name, err)
	}
	if waitErr != nil {
		if !asExitError(waitErr) {
			// 非 ExitError 的等待失败（IO 错误等）才是真 error。
			return res, fmt.Errorf("local: 等待 %q 结束失败：%w", name, waitErr)
		}
		// ExitError：命令自己非零退出 → 不是 error，ExitCode 已带上。
	}
	return res, nil
}

// exitCode 从 Cmd / waitErr 里取退出码（取不到时给 -1，表示"未知"）。
func exitCode(cmd *exec.Cmd, waitErr error) int {
	if cmd.ProcessState != nil {
		return cmd.ProcessState.ExitCode()
	}
	var ee *exec.ExitError
	if waitErr != nil && errors.As(waitErr, &ee) && ee.ProcessState != nil {
		return ee.ProcessState.ExitCode()
	}
	return -1
}

// cappedBuffer 是带上限的输出缓冲（防无限输出吃光内存）。
type cappedBuffer struct {
	buf     bytes.Buffer
	dropped int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	room := maxCapturedOutput - b.buf.Len()
	if room <= 0 {
		b.dropped += len(p)
		return len(p), nil // 假装全收下，避免 io.Copy 报 short write
	}
	if len(p) > room {
		b.dropped += len(p) - room
		p = p[:room]
	}
	return b.buf.Write(p)
}

func (b *cappedBuffer) bytes() []byte {
	return b.buf.Bytes()
}

// asExitError 判定"等待失败是否源于命令自身非零退出"。
func asExitError(err error) bool {
	var ee *exec.ExitError
	return errors.As(err, &ee)
}
