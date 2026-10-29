package sandbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/core"
)

// maxCapturedOutput / waitDelay 与 runtime/cap/local 同值。
//
// 为什么照抄而不是共享常量：两个包是**并列实现**（K8），共享常量会让 local 的
// "我只管本机执行"文档多出一个跨包依赖。数值本身是行为契约（截断上限 / 取消后
// Wait 的硬上限），两边必须一致，所以抄值 + 注释互相引用。
const (
	maxCapturedOutput = 32 << 20
	waitDelay         = 5 * time.Second
)

// Run implements core.ExecCapability（沙箱版）。
//
// 语义与 local.Exec.Run **逐条对齐**（K8：换 env 不动调用方）：
//
//   - **不走 shell**：spec.Name 直接是可执行文件（陷阱 7 精神）；
//   - ctx 结束 → **杀进程树**（taskkill /T /F /PID 兜底）+ job object 收容；
//   - 非零退出码**不是 error**（ExecResult.ExitCode 带上）；
//   - spec.Env 为空 = 继承父进程 env；非空 = 整份替换 + 沙箱 env 覆盖。
//
// 比 local 多出来的三件事（沙箱增量）：
//
//  1. 子进程带**受限 token**（backend.Spawn()）；
//  2. 子进程被纳入**job object**（backend.Attach），释放时 KILL_ON_JOB_CLOSE 收尾；
//  3. TMP/TEMP 被重定向到沙箱临时根（否则受限 token 写不了真实 %TEMP%）。
func (e *Env) Run(ctx context.Context, spec core.ExecSpec) (core.ExecResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return core.ExecResult{}, fmt.Errorf("sandbox: 上下文已结束（%w）", err)
	}
	name := strings.TrimSpace(spec.Name)
	if name == "" {
		return core.ExecResult{}, fmt.Errorf("sandbox: ExecSpec.Name 不能为空（不猜可执行文件）")
	}

	execu, err := e.backend.Begin(ctx, spec)
	if err != nil {
		return core.ExecResult{}, fmt.Errorf("sandbox: 准备进程约束失败：%w", err)
	}
	// Release 必须在 Wait 之后跑（见 Backend 生命周期注释），defer 正好满足。
	defer func() { _ = execu.Release() }()

	spawn := execu.Spawn()
	cmd := exec.CommandContext(ctx, name, spec.Args...)
	cmd.Dir = e.resolveDir(spec.Dir) // 空 = 继承当前工作目录；不在允许集 → 回落沙箱临时根（F15）
	cmd.Env = mergeEnv(spec.Env, e.sandboxEnv(), spawn.Env)
	applySpawn(cmd, spawn)      // 平台相关：受限 token / windowsHide / CREATE_SUSPENDED
	cmd.Cancel = func() error { // ctx 结束时杀整棵树（taskkill 兜底，陷阱 5）
		killTree(cmd)
		return nil
	}
	cmd.WaitDelay = waitDelay // 管道关不上时的硬上限（同 local 的实测理由）

	var stdout, stderr cappedBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	startErr := cmd.Start()
	if startErr != nil {
		return core.ExecResult{}, fmt.Errorf("sandbox: 启动 %q 失败：%w", name, startErr)
	}
	if err := execu.Attach(cmd.Process.Pid); err != nil {
		// 约束没加上去 = 沙箱名不副实 → fail-close：杀掉刚起的进程并报错。
		// 杀法：**先 TerminateProcess（必定成功）**，taskkill 只补刀（N4）——
		// 挂起创建的进程 taskkill 可能失败，而 cmd.Wait() 会一直等它退出 → 永久挂死。
		terminateAndWait(cmd)
		return core.ExecResult{}, fmt.Errorf("sandbox: 把进程 %d 纳入约束失败：%w", cmd.Process.Pid, err)
	}
	// 进程是**挂起**创建的（Windows CREATE_SUSPENDED）：纳入约束之后才恢复执行，
	// 这样"进程已经能跑了、还没进 job"那个窗口不存在（F13）。
	if spawn.Suspend {
		if err := resumeProcess(cmd.Process.Pid); err != nil {
			terminateAndWait(cmd)
			return core.ExecResult{}, fmt.Errorf("sandbox: 恢复进程 %d 执行失败：%w", cmd.Process.Pid, err)
		}
	}
	waitErr := cmd.Wait()

	res := core.ExecResult{
		ExitCode: exitCode(cmd, waitErr),
		Stdout:   stdout.bytes(),
		Stderr:   stderr.bytes(),
	}
	// 取消/超时优先呈现（同 local：waitErr 此时多半是 "signal: killed"）。
	if err := ctx.Err(); err != nil {
		return res, fmt.Errorf("sandbox: 执行 %q 被中止（%w）", name, err)
	}
	if waitErr != nil && !asExitError(waitErr) {
		return res, fmt.Errorf("sandbox: 等待 %q 结束失败：%w", name, waitErr)
	}
	return res, nil
}

// terminateAndWait 杀掉刚起（可能还挂起）的子进程并等它退出（N4）。
//
// 为什么不能只靠 taskkill：进程是**挂起**创建的（CREATE_SUSPENDED），taskkill
// 对挂起进程可能失败（"结束任务"对未运行的进程不总是奏效），而 cmd.Wait() 会
// 一直等它退出 → Run 永久挂死。
//
// 所以主路径用 `cmd.Process.Kill()`（TerminateProcess，对挂起进程也必定成功），
// taskkill 只作为"连孙子一起收"的补刀（孙子进程不受 TerminateProcess 影响）。
func terminateAndWait(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.Process != nil {
		_ = cmd.Process.Kill() // TerminateProcess：挂起进程也必定杀掉
	}
	killTree(cmd) // 补刀：把已知的整棵树收掉（taskkill 可能失败，忽略）
	_ = cmd.Wait()
}

// mergeEnv 组装子进程 env：spec.Env（继承或整份替换）+ 沙箱覆盖 + 后端覆盖。
//
// spec.Env 为空 → 以**净化后的父 env** 为基底（与 local 的"空=继承"语义一致：
// 效果相同，只是这里必须显式展开才能追加沙箱键）。非空 → 整份替换，
// 再叠加沙箱覆盖与后端覆盖（沙箱键优先：TMP/TEMP 指错地方会让一堆工具莫名失败）。
func mergeEnv(specEnv, sandboxEnv, backendEnv []string) []string {
	base := map[string]string{}
	if len(specEnv) == 0 {
		for _, kv := range os.Environ() {
			if k, v, ok := strings.Cut(kv, "="); ok {
				base[k] = v
			}
		}
	} else {
		for _, kv := range specEnv {
			if k, v, ok := strings.Cut(kv, "="); ok {
				base[k] = v
			}
		}
	}
	for _, kv := range append(append([]string{}, sandboxEnv...), backendEnv...) {
		if k, v, ok := strings.Cut(kv, "="); ok {
			base[k] = v
		}
	}
	out := make([]string, 0, len(base))
	for k, v := range base {
		out = append(out, k+"="+v)
	}
	sort.Strings(out) // 确定性顺序（便于测试；Windows 上避免同名键大小写歧义）
	return out
}

// exitCode 从 Cmd / waitErr 里取退出码（取不到给 -1 = 未知）。同 local。
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

// cappedBuffer 是带上限的输出缓冲（防无限输出吃光内存）。同 local。
type cappedBuffer struct {
	buf     bytes.Buffer
	dropped int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	room := maxCapturedOutput - b.buf.Len()
	if room <= 0 {
		b.dropped += len(p)
		return len(p), nil
	}
	if len(p) > room {
		b.dropped += len(p) - room
		p = p[:room]
	}
	return b.buf.Write(p)
}

func (b *cappedBuffer) bytes() []byte { return b.buf.Bytes() }

// asExitError 判定"等待失败是否源于命令自身非零退出"。同 local。
func asExitError(err error) bool {
	var ee *exec.ExitError
	return errors.As(err, &ee)
}
