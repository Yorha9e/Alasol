//go:build !windows

package sandbox

import (
	"context"
	"fmt"
	"runtime"

	"github.com/Yorha9e/alasol/core"
)

// 本文件是 H9 的 **Linux / macOS 挂点**：只留接口形状与接线说明，**不实现**沙箱。
//
// # 为什么 fail-close 而不是"退化成 local"
//
// 未实现平台上如果悄悄返回一个"什么都不约束"的后端，调用方会以为自己在沙箱里跑
// （V8 的"双假象比没有更危险"）。所以这里 New 直接报错，把"没有沙箱"说清楚。
//
// # 接线位：将来怎么挂 seatbelt / bubblewrap
//
//  1. 新建包（建议 `runtime/cap/sandbox_seatbelt` / `runtime/cap/sandbox_bwrap`），
//     实现本包的 Backend 接口：
//
//     Name() string
//     Diagnostics() []Diag
//     FileRoots() []string
//     ApplyFileACL(ctx) error
//     Begin(ctx, core.ExecSpec) (Execution, error)
//     Close() error
//
//     以及 Execution（Spawn / Attach / Release）。
//     **不需要改本包、不需要改 host、不需要改 core**——这就是 H9 要的可拓展埋点。
//
// 2. 在该包的 init() 里注册（本文件同款写法）：
//
//	func init() {
//	    sandbox.RegisterBackend("macos-seatbelt", newSeatbeltBackend)
//	    sandbox.PlatformBackendName = func() string { return "macos-seatbelt" }
//	}
//
// 3. 各腿的落点（对应 Windows 三件套）：
//
//   - 进程树收容：macOS 用进程组 / setsid；Linux 用进程组 / cgroup / unshare PID ns。
//
//   - 身份剥夺：macOS 用 seatbelt profile 的 `(deny file*)`；Linux 用 bubblewrap 的
//     `--ro-bind`（默认只读）+ 只显式挂需要的路径。
//
//   - 允许集授予：macOS 用 seatbelt `(allow file-write* (subpath ...))`；
//     Linux 用 bwrap `--bind <root> <root>` 白名单。
//
//   - WER 弹窗压制：两个平台都没有对应物（不需要）。
//
//     注意 macOS seatbelt 需要 `sandbox-exec`（已标记 deprecated 但仍可用），
//     Linux bubblewrap 需要 setuid 或 `kernel.unprivileged_userns_clone`
//     （Ubuntu 24.04 起 AppArmor 默认限制，见 docs/research/01 §4.1）。
//
//     4. 进程启动：Execution.Spawn() 返回的 SpawnOptions.Env 可带 profile 路径或
//     bwrap 参数；真正决定"怎么起进程"的是 host 侧注入的 Exec 能力实现，所以
//     新后端只需在 Begin 里准备好命令行前缀（`bwrap --dev-bind / / ...`）即可。
const posixBackendName = "posix-unimplemented"

func init() {
	RegisterBackend(posixBackendName, func(p Policy) (Backend, error) { return newPosixBackend(p) })
	PlatformBackendName = func() string { return posixBackendName }
}

// posixBackend 是未实现平台的占位后端：**永远不施加任何约束**，只负责把
// "这里没有沙箱"说清楚。它注册的目的只是让 BackendNames() 能列出挂点名字。
type posixBackend struct {
	policy Policy
	diag   *diagLog
}

var _ Backend = (*posixBackend)(nil)

func newPosixBackend(p Policy) (*posixBackend, error) {
	return &posixBackend{policy: p, diag: &diagLog{name: posixBackendName}}, nil
}

func (b *posixBackend) Name() string { return posixBackendName }

func (b *posixBackend) Diagnostics() []Diag { return b.diag.snapshot() }

func (b *posixBackend) FileRoots() []string { return nil }

func (b *posixBackend) ApplyFileACL(ctx context.Context) error {
	_ = ctx
	return errNoPosixSandbox
}

func (b *posixBackend) Begin(ctx context.Context, spec core.ExecSpec) (Execution, error) {
	_ = ctx
	_ = spec
	return nil, errNoPosixSandbox
}

func (b *posixBackend) Close() error { return nil }

// errNoPosixSandbox 是未实现平台的统一错误（fail-close：不返回没有约束的环境）。
var errNoPosixSandbox = fmt.Errorf("sandbox: 当前平台（%s）没有实现沙箱后端；"+
	"Linux/macOS 的 seatbelt/bubblewrap 是预留挂点（见本文件头部的接线说明）", runtime.GOOS)
