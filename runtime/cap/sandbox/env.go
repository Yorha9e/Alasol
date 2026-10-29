package sandbox

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Yorha9e/alasol/core"
)

// Env 是沙箱化的执行环境（K8：SandboxedExecutionEnv）。
//
// 它同时满足 core.ExecCapability 与 core.FSCapability——和 runtime/cap/local 的
// Exec / FS 一样的两个接口，所以 host 装配时是**一对一替换**：
//
//	// v1 等价（现状）
//	tools.NewRead(local.FS{}); tools.NewWrite(local.FS{}); tools.NewBash(local.Exec{})
//	// 沙箱
//	env := sandbox.New(sandbox.DefaultPolicy())
//	tools.NewRead(env); tools.NewWrite(env); tools.NewBash(env)
//
// 工具代码、loop、策略链**一行不动**（K8 酸测试，见 env_test.go）。
//
// 并发安全：Env 自身只有只读字段；每次 Run 走 backend.Begin 拿独立的 Execution
// （独立的 job object），所以多个 Run 可以并发跑而互不干扰。
type Env struct {
	policy  Policy
	backend Backend
	roots   []string
	// tempRoot 是沙箱临时根（已建好），用于 TMP/TEMP 重定向。
	tempRoot string
	// tempOwned 表示 tempRoot 是**我们派生**的（不是用户显式指定的）——
	// 只有派生出来的才在 Close 时删掉（F14：注释里"随进程退出自然清理"是假的）。
	tempOwned bool
	// closeOnce 保证 Close 幂等（临时根只删一次；后端 Close 本身也幂等）。
	closeOnce sync.Once
	closeErr  error
}

// 编译期断言：Env 必须同时满足两个能力接口（K8：换实现不动调用方）。
// FS 的方法直接转发给 FS 类型（同一个 env 的允许集），这样"换 env 实现"就是
// **换一个对象**，而不是换一对对象——酸测试因此能在完全相同的调用形状下比对。
var (
	_ core.ExecCapability = (*Env)(nil)
	_ core.FSCapability   = (*Env)(nil)
)

// ReadFile implements core.FSCapability（转发给 FS）。
func (e *Env) ReadFile(ctx context.Context, path string) ([]byte, error) {
	return e.FS().ReadFile(ctx, path)
}

// WriteFile implements core.FSCapability（转发给 FS）。
func (e *Env) WriteFile(ctx context.Context, path string, data []byte) error {
	return e.FS().WriteFile(ctx, path, data)
}

// Stat implements core.FSCapability（转发给 FS）。
func (e *Env) Stat(ctx context.Context, path string) (core.FileInfo, error) {
	return e.FS().Stat(ctx, path)
}

// ListDir implements core.FSCapability（转发给 FS）。
func (e *Env) ListDir(ctx context.Context, path string) ([]string, error) {
	return e.FS().ListDir(ctx, path)
}

// New 用当前平台的默认后端构造沙箱 env。
//
// 平台无可用后端（Linux / macOS 的 seatbelt / bwrap 尚未实现）→ **报错**，不返回
// "看起来像沙箱其实什么都没做"的对象（fail-close）。
func New(p Policy) (*Env, error) {
	name := PlatformBackendName()
	ctor, ok := BackendByName(name)
	if !ok {
		return nil, fmt.Errorf("sandbox: 当前平台没有可用的沙箱后端（已注册：%s）；"+
			"Linux/macOS 的 seatbelt/bubblewrap 是预留挂点，见 backend_other.go", joined(BackendNames()))
	}
	// **临时根先派生再构造后端**（F1）：后端会拿着 policy 副本去算允许集并施加 ACL，
	// 临时根必须**已经在**那份副本里——否则后端算出的允许集少一个根，
	// 子进程写自己的 %TEMP% 会被 ACL 拒（"目录内写也失败"，最難查的一类症状）。
	p, tempOwned, err := resolveTempRoot(p)
	if err != nil {
		return nil, err
	}
	b, err := ctor(p)
	if err != nil {
		if tempOwned {
			_ = os.RemoveAll(p.SandboxTempRoot)
		}
		return nil, fmt.Errorf("sandbox: 构造后端 %q: %w", name, err)
	}
	env, err := NewWithBackend(p, b)
	if err != nil {
		// 后端已经建了（可能已经加了 WER 排除项）——装配失败必须把它收掉，
		// 否则用户机器上留下一条没人撤的注册表项。
		_ = b.Close()
		if tempOwned {
			_ = os.RemoveAll(p.SandboxTempRoot)
		}
		return nil, err
	}
	// 临时根是我们派生的 → Close 时由 Env 负责删（F14）。
	env.tempOwned = tempOwned
	return env, nil
}

// resolveTempRoot 归一化策略里的沙箱临时根：显式配置优先，否则派生；目录当场建好。
//
// 返回的第二个值是"这个临时根是不是我们派生出来的"（派生 = Close 时可以删）。
//
// 为什么目录要**当场**建好：允许根必须真实存在，否则 ACL 施加
// （SetNamedSecurityInfo）会失败，而失败要尽早暴露（装配期炸，不是运行期）。
func resolveTempRoot(p Policy) (Policy, bool, error) {
	temp := strings.TrimSpace(p.SandboxTempRoot)
	owned := false
	if temp == "" {
		temp = defaultSandboxTempRoot()
		owned = true
	}
	if err := os.MkdirAll(temp, 0o700); err != nil {
		return p, owned, fmt.Errorf("sandbox: 建沙箱临时目录 %s: %w", temp, err)
	}
	p.SandboxTempRoot = temp
	return p, owned, nil
}

// NewWithBackend 用指定后端构造沙箱 env（H9 埋点入口：自定义后端 / 测试注入）。
//
// 生产代码走 New；这个方法留给"换一个后端"与测试——K8 酸测试正是用它注入记账后端，
// 从而在**任何平台**上验证"换 env 实现不动 agent 逻辑"。
func NewWithBackend(p Policy, b Backend) (*Env, error) {
	if b == nil {
		return nil, fmt.Errorf("sandbox: backend 不能为 nil")
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	p, tempOwned, err := resolveTempRoot(p)
	if err != nil {
		return nil, err
	}

	roots := p.effectiveAllowedRoots()
	if len(roots) == 0 {
		if tempOwned {
			_ = os.RemoveAll(p.SandboxTempRoot)
		}
		return nil, fmt.Errorf("sandbox: 允许集为空（连临时根都没解析出来）——拒绝装配")
	}
	for _, r := range roots {
		if err := os.MkdirAll(r, 0o700); err != nil {
			if tempOwned {
				_ = os.RemoveAll(p.SandboxTempRoot)
			}
			return nil, fmt.Errorf("sandbox: 建允许根 %s: %w", r, err)
		}
	}
	// 允许根**由 Env 统一注入后端**（F1）：后端自己那份 policy 副本可能是在
	// Env 派生临时根之前造的（NewWithBackend 的注入路径），两份 policy 各算各的
	// 允许集就会漂移——"同一件事只存一份"（D21）。
	if ra, ok := b.(rootsAware); ok {
		ra.setRoots(roots)
	}
	if p.UseFileACL {
		if err := b.ApplyFileACL(context.Background()); err != nil {
			if tempOwned {
				_ = os.RemoveAll(p.SandboxTempRoot)
			}
			return nil, fmt.Errorf("sandbox: 施加文件 ACL: %w", err)
		}
	}
	return &Env{policy: p, backend: b, roots: roots, tempRoot: p.SandboxTempRoot, tempOwned: tempOwned}, nil
}

// Policy 返回生效的策略（只读快照语义：调用方改它不影响 env）。
func (e *Env) Policy() Policy { return e.policy }

// BackendName 返回当前后端名（埋点/诊断）。
func (e *Env) BackendName() string { return e.backend.Name() }

// Diagnostics 返回后端埋点记录（哪些约束真的生效了）。
func (e *Env) Diagnostics() []Diag { return e.backend.Diagnostics() }

// AllowedRoots 返回生效的允许根集合（已含沙箱临时根）。
func (e *Env) AllowedRoots() []string {
	out := make([]string, len(e.roots))
	copy(out, e.roots)
	return out
}

// Close 释放沙箱（撤 WER 排除项、收口 job 句柄、删掉派生的临时根）。幂等。
func (e *Env) Close() error {
	if e == nil {
		return nil
	}
	e.closeOnce.Do(func() {
		if e.backend != nil {
			e.closeErr = e.backend.Close()
		}
		// 派生出来的临时根：删掉（F14）。用户显式指定的不动。
		if e.tempOwned && e.tempRoot != "" {
			if err := os.RemoveAll(e.tempRoot); err != nil && e.closeErr == nil {
				e.closeErr = fmt.Errorf("sandbox: 清理沙箱临时根 %s: %w", e.tempRoot, err)
			}
		}
	})
	return e.closeErr
}

// sandboxEnv 返回沙箱自己要覆盖的子进程环境变量。
//
// 只有一件事：把临时目录指到沙箱临时根。理由很实际——受限 token 的子进程
// **写不了真实 %TEMP%**（那是用户私有目录，DACL 只授当前用户，而子进程的用户 SID
// 是 deny-only）。不重定向的话 git / npm / 编译器这类要写临时文件的工具会莫名失败，
// 而失败信息离原因极远（"Permission denied"出现在一个看起来人畜无害的路径上）。
//
// 同时给 Windows（TMP/TEMP）与 POSIX（TMPDIR）两套名字：跨平台行为一致，
// 且沙箱临时根总是允许集里的（New 时自动加入）。
func (e *Env) sandboxEnv() []string {
	if !e.policy.RedirectTemp || e.tempRoot == "" {
		return nil
	}
	return []string{
		"TMP=" + e.tempRoot,
		"TEMP=" + e.tempRoot,
		"TMPDIR=" + e.tempRoot,
	}
}

// resolveDir 解析子进程的工作目录（F15）。
//
// 规则：
//
//   - 空 → 继承当前工作目录（与 local 的语义一致）；
//   - 在允许集内 → 原样使用；
//   - 不在允许集内 → **回落沙箱临时根**（而不是直接报错）。
//
// 为什么回落而不是报错：调用方给的目录多半是"用户以为能用的目录"，
// 但它在允许集之外时，受限 token 的子进程根本进不去（连遍历都不行），
// 报错信息（"Access is denied"）离原因很远。回落到沙箱临时根 = 至少能跑起来，
// 而"落在哪个目录"这件事由允许集说了算（这是沙箱语义，不是 bug）。
//
// 注意：这只是**便利层**——真正的边界是 OS 侧的 ACL + 受限 token。
func (e *Env) resolveDir(dir string) string {
	d := strings.TrimSpace(dir)
	if d == "" {
		return ""
	}
	abs, err := filepath.Abs(d)
	if err != nil {
		return e.fallbackDir()
	}
	for _, r := range e.roots {
		if underRoot(abs, r) {
			return abs
		}
	}
	return e.fallbackDir()
}

// fallbackDir 返回回落目录（沙箱临时根；没有就用当前工作目录）。
func (e *Env) fallbackDir() string {
	if e.tempRoot != "" {
		return e.tempRoot
	}
	return ""
}

// checkAllowed 报告 path 是否落在允许集内（harness 内的软检查，不是 OS 强制）。
//
// 为什么还要这一层：子进程的写由 OS ACL + 受限 token 兜着（内核强制），
// 但 read / write 工具跑在**宿主进程身份**下，token 管不到它们。给它们加一道
// 允许集检查，行为才和沙箱语义一致（"沙箱里就是只能碰允许集"）。
// 诚实说清：这是 fail-close 的**便利层**，不是安全边界——边界在 OS 那侧。
func (e *Env) checkAllowed(op, path string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("sandbox: %s 路径不能为空", op)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("sandbox: %s 路径 %q 无法解析成绝对路径：%w", op, path, err)
	}
	for _, r := range e.roots {
		if underRoot(abs, r) {
			return nil
		}
	}
	return fmt.Errorf("sandbox: %s %q 在允许集之外（允许根：%s）——沙箱模式下只允许访问这些根%s",
		op, path, joined(e.roots), softCheckNote)
}
