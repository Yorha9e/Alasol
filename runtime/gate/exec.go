package gateexec

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

// waitDelay 是 Wait 等 I/O 管道关闭的硬上限（5s）。
// 与 runtime/cap/local 同款理由：ctx 取消时若只杀直接子进程，孙子进程还攥着管道写端，
// Wait 会挂到它们自己退出。这里靠杀进程树 + WaitDelay 兜底，绝不无限挂起。
const waitDelay = 5 * time.Second

// runProcess 跑一个进程（git 直跑，或经 shell 的门禁命令，都由调用方组好 argv 传入）。
//
//   - dir：工作目录；env 非 nil 时整份替换（git 传 nil = 继承父 env；门禁命令传注入了约定变量的 env）；
//   - stdout/stderr **合并**进同一 tailBuffer（V5：日志尾部 ≤ maxLogTail）；
//   - ctx 可取消 → 杀进程树；取消/超时的错误优先呈现（不让"signal: killed"被误读成命令自身崩溃）；
//   - 非零退出**不是 error**：退出码随返回值带上（事实）。
func runProcess(ctx context.Context, dir string, env []string, argv []string) (output string, exitCode int, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(argv) == 0 {
		return "", -1, errors.New("gateexec: 空 argv")
	}
	if err := ctx.Err(); err != nil {
		return "", -1, fmt.Errorf("gateexec: 上下文已结束（%w）", err)
	}

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = dir
	if env != nil {
		cmd.Env = env
	}
	setProcAttr(cmd) // windowsHide（Windows）/ 独立进程组（Unix，便于杀树）
	cmd.Cancel = func() error {
		killTree(cmd)
		return nil
	}
	cmd.WaitDelay = waitDelay

	var tb tailBuffer
	cmd.Stdout = &tb
	// Stdout 与 Stderr 指向**同一个** tailBuffer：os/exec 检测到 interfaceIs(Stderr, Stdout)
	// 为真时会复用同一管道、由单个 copier goroutine 写入 → 既是真·按序合并，tb 也无并发写
	// （无需加锁；-race 已覆盖）。
	cmd.Stderr = &tb

	startErr := cmd.Start()
	if startErr != nil {
		return "", -1, fmt.Errorf("gateexec: 启动 %q 失败：%w", argv[0], startErr)
	}
	waitErr := cmd.Wait()
	code := exitCodeOf(cmd, waitErr)

	if err := ctx.Err(); err != nil {
		return tb.String(), code, fmt.Errorf("gateexec: %q 被中止（%w）", argv[0], err)
	}
	if waitErr != nil {
		var ee *exec.ExitError
		if !errors.As(waitErr, &ee) {
			// 非 ExitError 的等待失败（IO 错误等）才是真 error。
			return tb.String(), code, fmt.Errorf("gateexec: 等待 %q 结束失败：%w", argv[0], waitErr)
		}
		// ExitError：命令自身非零退出 → 不是 error，退出码已带上。
	}
	return tb.String(), code, nil
}

// exitCodeOf 从 Cmd / waitErr 取退出码（取不到给 -1 = 未知）。
func exitCodeOf(cmd *exec.Cmd, waitErr error) int {
	if cmd.ProcessState != nil {
		return cmd.ProcessState.ExitCode()
	}
	var ee *exec.ExitError
	if waitErr != nil && errors.As(waitErr, &ee) && ee.ProcessState != nil {
		return ee.ProcessState.ExitCode()
	}
	return -1
}

// tailBuffer 是"只保留尾部 max 字节"的输出缓冲（V5：日志必须设上限，且防无限输出吃光内存）。
//
// 写入超过 2*max 时把前部抹掉、只留尾部 max；String() 永远返回最后 ≤ max 字节。
// 内存上界 ≈ 2*max，与命令输出总量无关。
type tailBuffer struct {
	buf []byte
	max int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	if t.max == 0 {
		t.max = maxLogTail
	}
	t.buf = append(t.buf, p...)
	if len(t.buf) > 2*t.max {
		n := copy(t.buf, t.buf[len(t.buf)-t.max:])
		t.buf = t.buf[:n]
	}
	return len(p), nil // 假装全收下，避免 io.Copy 报 short write
}

func (t *tailBuffer) String() string {
	if len(t.buf) <= t.max {
		return string(t.buf)
	}
	return string(t.buf[len(t.buf)-t.max:])
}
