//go:build windows

package sandbox

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Yorha9e/alasol/core"
	"golang.org/x/sys/windows"
)

// WindowsBackendName 是 Windows 三件套后端的名字（埋点/诊断用）。
const WindowsBackendName = "windows-job+token+acl"

func init() {
	RegisterBackend(WindowsBackendName, func(p Policy) (Backend, error) { return newWindowsBackend(p) })
	PlatformBackendName = func() string { return WindowsBackendName }
}

// windowsBackend 是 H9 定案的 Windows 三件套后端。
//
// 三条腿各自独立可观测（Diagnostics），任一条退化都会在埋点里写明——沙箱最怕的不是
// "没做到"，是"以为做到了"。
//
// 并发安全：token 只读复用（CreateProcessAsUser 不消耗它）；每次执行一个独立
// job object；埋点与 WER 记账各自有锁。
type windowsBackend struct {
	policy Policy
	diag   *diagLog

	// roots 是 Env 注入的生效允许根（F1：允许集只有一份事实来源）。
	// 没被注入时退回 policy 自己算（自定义后端/旧路径）。
	rootsMu sync.Mutex
	roots   []string

	// fileApplied 记录 ACL 是否已施加（只做一次；幂等）。授予明细只进埋点，
	// 不再单存一份字段——两处存同一件事必然漂移（D21）。
	fileMu    sync.Mutex
	fileRoots []string

	wer    *werBookkeeping
	jobs   *jobRegistry
	closed bool

	// prevErrorMode 是压制前进程级 error mode 的原值（Close 时复原，F5）。
	prevErrorMode uint32
	// errorModeTouched 标记"我们动过 error mode"（F5/N7）。
	//
	// 为什么不能靠 prevErrorMode != 0 判断：**原值可能恰好是 0**（进程启动时
	// error mode 就是 0，很常见）——那时"!= 0"为假 → Close 不复原 → 我们把压制位
	// 永久留在了宿主进程上。所以用独立标志：动过就复原（哪怕原值是 0）。
	errorModeTouched bool

	// tokMu 保护下面三个字段。原本想用 sync.Once，但 Close 要把它们复位好让 env
	// 能重开——**给 sync.Once 赋值不是线程安全的**，并发 Begin/Close 会踩到。
	// 改用普通互斥锁 + 显式 built 标志。
	tokMu      sync.Mutex
	tokenBuilt bool
	tokenErr   error
	primary    windows.Token
}

var _ Backend = (*windowsBackend)(nil)

// setRoots implements rootsAware（Env 注入生效允许根）。
func (b *windowsBackend) setRoots(roots []string) {
	b.rootsMu.Lock()
	defer b.rootsMu.Unlock()
	out := make([]string, len(roots))
	copy(out, roots)
	b.roots = out
}

// effectiveRoots 返回本次装配生效的允许根（Env 注入优先，否则自己算）。
func (b *windowsBackend) effectiveRoots() []string {
	b.rootsMu.Lock()
	roots := b.roots
	b.rootsMu.Unlock()
	if len(roots) > 0 {
		return roots
	}
	return b.policy.effectiveAllowedRoots()
}

func newWindowsBackend(p Policy) (*windowsBackend, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	b := &windowsBackend{
		policy: p,
		diag:   &diagLog{name: WindowsBackendName},
		wer:    newWerBookkeeping(),
		jobs:   newJobRegistry(),
	}
	if p.SuppressWER {
		// error mode 是进程级的，且被子进程继承——在这里设一次即覆盖全树。
		// **按位或**上压制位（F5）：不能整块覆盖写，那会抹掉宿主自己设的别的位
		// （比如 SEM_NOGPFAULTERRORBOX 之外的位），Close 时也要复原。
		prev := suppressErrorMode()
		b.prevErrorMode = prev
		b.errorModeTouched = true
		b.diag.add("wer", true, "SetErrorMode(SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX) 已设置（前值 0x%x，子进程继承；Close 时复原）", prev)
		if host := hostExeName(); host != "" {
			if err := b.wer.add(host); err != nil {
				b.diag.add("wer", false, "WER 排除宿主 %s 失败：%v", host, err)
			} else {
				b.diag.add("wer", true, "WER 排除宿主可执行名 %s（HKCU，Close 时回收）", host)
			}
		}
	} else {
		b.diag.add("wer", false, "策略关闭了 WER 压制（Policy.SuppressWER=false）——崩溃仍可能弹窗（V10 挂账未落地）")
	}
	return b, nil
}

// Name implements Backend。
func (b *windowsBackend) Name() string { return WindowsBackendName }

// Diagnostics implements Backend。
func (b *windowsBackend) Diagnostics() []Diag { return b.diag.snapshot() }

// FileRoots implements Backend。
func (b *windowsBackend) FileRoots() []string {
	b.fileMu.Lock()
	defer b.fileMu.Unlock()
	out := make([]string, len(b.fileRoots))
	copy(out, b.fileRoots)
	return out
}

// ApplyFileACL implements Backend：对每个允许根施加 DACL。
func (b *windowsBackend) ApplyFileACL(ctx context.Context) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if !b.policy.UseFileACL {
		b.diag.add("file-acl", false, "策略关闭了文件 ACL（Policy.UseFileACL=false）——允许集只是 harness 内软检查，无 OS 强制")
		return nil
	}
	// 注："UseFileACL 开而 UseRestrictedToken 关"这种自相矛盾的配置已在
	// Policy.Validate 里拒掉（ACL 会空转，却让人以为沙箱开着），这里不必再防。

	b.fileMu.Lock()
	defer b.fileMu.Unlock()
	if len(b.fileRoots) > 0 {
		return nil // 已施加过（幂等）
	}

	tok, err := b.restrictedToken()
	if err != nil {
		return err
	}
	grantees, err := sandboxGrantSIDs(tok)
	if err != nil {
		return fmt.Errorf("取授予 SID 失败: %w", err)
	}

	roots := sortRoots(b.effectiveRoots())
	limit := b.policy.ACLMaxObjects
	if limit <= 0 {
		limit = DefaultACLMaxObjects
	}
	var applied []string
	for _, root := range roots {
		if reason, bad := dangerousRootReason(root); bad {
			return fmt.Errorf("sandbox: 拒绝给允许根 %s 设 ACL：它是%s", root, reason)
		}
		n, over, err := countDescendants(root, limit)
		if err != nil {
			return fmt.Errorf("sandbox: 数允许根 %s 的对象数失败: %w", root, err)
		}
		if over {
			return fmt.Errorf("sandbox: 允许根 %s 的对象数超过上限 %d（Policy.ACLMaxObjects）；"+
				"可继承 ACE 会传播到既有子对象，超限即拒", root, limit)
		}
		granted, err := applyRootACL(root, grantees)
		if err != nil {
			return fmt.Errorf("sandbox: 给允许根 %s 设 ACL 失败: %w", root, err)
		}
		applied = append(applied, root)
		b.diag.add("file-acl", true, "允许根 %s 设 DACL（子树 %d 个对象；授予 %s）", root, n, strings.Join(granted, " + "))
	}
	b.fileRoots = applied
	return nil
}

// Begin implements Backend：为一次执行准备约束（job object + WER 排除 + token）。
func (b *windowsBackend) Begin(ctx context.Context, spec core.ExecSpec) (Execution, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if b.policy.SuppressWER {
		// 子进程的**真实映像名**也加进 WER 排除表（V10：别弹窗炸桌面）。
		//
		// F6：调用方给的是命令名（"cmd"），WER 的排除表按**映像文件名**记
		// （"cmd.exe"）——照抄调用方的字符串等于排除了一个不存在的条目，不生效。
		// 所以先用 LookPath 解析真实路径再取 base name。
		if exe := realExeBaseName(spec.Name); exe != "" {
			switch err := b.wer.add(exe); {
			case err != nil:
				b.diag.add("wer", false, "WER 排除子进程 %s 失败：%v", exe, err)
			case b.wer.has(exe):
				b.diag.add("wer", true, "WER 排除子进程可执行名 %s（HKCU，Close 时回收）", exe)
			}
		}
	}

	var job windows.Handle
	if b.policy.UseJobObject {
		// 活动进程上限：0 = DefaultMaxActiveProcesses（F3：文档承诺的默认值必须真的生效）。
		maxActive := b.policy.effectiveMaxActiveProcesses()
		h, err := newJobObject(maxActive)
		if err != nil {
			return nil, fmt.Errorf("建 job object 失败: %w", err)
		}
		job = h
		b.jobs.add(job)
		b.diag.add("job-object", true,
			"job object 已建（KILL_ON_JOB_CLOSE + 活动进程上限 %d）", maxActive)
	} else {
		b.diag.add("job-object", false, "策略关闭了 job object（Policy.UseJobObject=false）——脱离进程树的后代不受收容")
	}

	opts := SpawnOptions{}
	if b.policy.UseJobObject {
		// F13：以 CREATE_SUSPENDED 起进程 → 纳入 job 之后才恢复执行，
		// 关掉"进程已跑起来、还没进 job"这个窗口（那窗口里它能 fork 出逃逸的后代）。
		opts.Suspend = true
	}
	if b.policy.UseRestrictedToken {
		tok, err := b.restrictedToken()
		if err != nil {
			if job != 0 {
				b.jobs.release(job)
			}
			return nil, err
		}
		opts.Token = tok
	}
	// TMP/TEMP 重定向不在这里做：它是**策略级**的 env 覆盖，归 Env.sandboxEnv()，
	// 这样任何后端（含测试注入的记账后端）行为一致。
	return &windowsExecution{backend: b, job: job, opts: opts}, nil
}

// Close implements Backend：复原 error mode + 撤 WER 排除项 + 收口残留 job 句柄。幂等。
func (b *windowsBackend) Close() error {
	b.fileMu.Lock()
	if b.closed {
		b.fileMu.Unlock()
		return nil
	}
	b.closed = true
	b.fileMu.Unlock()

	if n := b.jobs.closeAll(); n > 0 {
		b.diag.add("job-object", true, "Close 时收口 %d 个残留 job 句柄（KILL_ON_JOB_CLOSE）", n)
	}
	for _, err := range b.wer.removeAll() {
		b.diag.add("wer", false, "撤 WER 排除项失败：%v", err)
	}
	// 复原进程级 error mode（F5/N7）：压制是"借"来的，Close 时必须还回去——
	// 否则宿主进程后续的错误弹窗策略被我们永久改掉了。
	// 判据是"我们动过"这个独立标志，**不是** prevErrorMode != 0（原值可能恰好是 0）。
	if b.errorModeTouched {
		restoreErrorMode(b.prevErrorMode)
		b.errorModeTouched = false
		b.diag.add("wer", true, "error mode 已复原为 0x%x", b.prevErrorMode)
	}
	b.tokMu.Lock()
	if b.primary != 0 {
		windows.CloseHandle(windows.Handle(b.primary))
		b.primary = 0
		b.tokenBuilt = false // 复位好让（万一）重开的后端能重新派生
	}
	b.tokMu.Unlock()
	return nil
}

// restrictedToken 派生（仅一次）受限主 token。失败会缓存错误，不让装配半残着继续。
//
// 并发安全：派生**全程持锁**（F12）。之前是"先置 tokenBuilt=true 再锁外派生"，
// 那个窗口里并发调用者会看到 tokenBuilt=true、tokenErr=nil、primary=0，
// 于是拿到"受限 token 未就绪"的假错误——并发 Run 时随机失败。
// CreateRestrictedToken 只是几十微秒，锁住不亏：正确性优先。
func (b *windowsBackend) restrictedToken() (windows.Token, error) {
	b.tokMu.Lock()
	defer b.tokMu.Unlock()
	if b.tokenBuilt {
		if b.tokenErr != nil {
			return 0, b.tokenErr
		}
		if b.primary == 0 {
			return 0, fmt.Errorf("sandbox: 受限 token 未就绪")
		}
		return b.primary, nil
	}
	b.tokenBuilt = true

	if !b.policy.UseRestrictedToken {
		b.tokenErr = fmt.Errorf("sandbox: 策略关闭了受限 token（Policy.UseRestrictedToken=false）")
		return 0, b.tokenErr
	}
	tok, err := restrictedPrimaryToken(b.policy.DangerousPrivileges, b.policy.StrictGroupSIDs)
	if err != nil {
		wrapped := fmt.Errorf("派生受限 token 失败: %w", err)
		b.tokenErr = wrapped
		return 0, wrapped
	}
	b.primary = tok

	denyOnly, derr := tokenDenyOnlyUserSID(tok)
	privs, perr := tokenPrivilegeCount(tok)
	denyGroups, gerr := tokenDenyOnlyGroupCount(tok)
	detail := fmt.Sprintf("CreateRestrictedToken 成功（用户 SID deny-only=%v", denyOnly)
	if gerr == nil {
		detail += fmt.Sprintf("，组 SID deny-only %d 个", denyGroups)
	}
	if derr == nil && perr == nil {
		detail += fmt.Sprintf("，剩余特权 %d 条", privs)
	}
	detail += "）"
	b.diag.add("restricted-token", true, "%s", detail)
	return tok, nil
}

// windowsExecution 是一次执行的约束句柄。
type windowsExecution struct {
	backend *windowsBackend
	job     windows.Handle
	opts    SpawnOptions
	done    bool
}

var _ Execution = (*windowsExecution)(nil)

// Spawn implements Execution。
func (e *windowsExecution) Spawn() SpawnOptions { return e.opts }

// Attach implements Execution：把已启动的进程纳入 job object。
func (e *windowsExecution) Attach(pid int) error {
	if e.job == 0 {
		return nil // 没开 job object：无约束可施（策略已记账）
	}
	err := assignToJob(e.job, pid)
	if err != nil && pidGone(err) {
		// 短命令已经跑完：没什么可收容的，良性。
		e.backend.diag.add("job-object", true, "进程 %d 已退出，无需纳入 job（良性）", pid)
		return nil
	}
	if err != nil {
		e.backend.diag.add("job-object", false, "进程 %d 纳入 job 失败：%v", pid, err)
		return err
	}
	e.backend.diag.add("job-object", true, "进程 %d 已纳入 job（pid=%d）", pid, pid)
	return nil
}

// Release implements Execution：关 job 句柄 → KILL_ON_JOB_CLOSE 收口。
//
// 必须在 cmd.Wait() 之后调用（见 Backend 生命周期注释）。幂等。
func (e *windowsExecution) Release() error {
	if e.done {
		return nil
	}
	e.done = true
	if e.job == 0 {
		return nil
	}
	// remove + CloseHandle 必须在 jobRegistry 的锁**内**一次做完（F10）：
	// 分成两步（先 remove 再关）时，closeAll 可能已经抄走了同一个句柄，
	// 于是同一个句柄被关两次——句柄值会被系统复用，第二次关可能关掉**别人的**句柄。
	if err := e.backend.jobs.release(e.job); err != nil {
		return fmt.Errorf("关 job 句柄失败（KILL_ON_JOB_CLOSE 未触发）: %w", err)
	}
	e.job = 0
	return nil
}
