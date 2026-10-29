package host

import (
	"os"
	"strings"

	"github.com/Yorha9e/alasol/runtime/cap/sandbox"
)

// 本文件是**沙箱装配的唯一口径**（D21：同一件事只在一个地方定义）。
//
// host.Config（serve/chat 等）与 cmd/alasol 的 runner（run/resume）都要折出
// sandbox.Policy——两份实现必然漂移（允许根到底包不包含 CWD、上限怎么取、
// 0 是不是"默认"……每一条都能漂）。所以口径放在这里，两边都调它。

// SandboxOptions 是沙箱装配的输入（Config 与 runnerOpts 都折成它）。
type SandboxOptions struct {
	// WriteDirs 是显式配置的允许写根（可空）。
	WriteDirs []string
	// CWD 是工作目录（空 = 进程当前目录）。
	//
	// CWD **总是**并入允许根：沙箱模式下 agent 总要能碰自己的工作区
	//（否则 bash 连 `git status` 都跑不了）。口径写在下面 SandboxPolicy 的注释里。
	CWD string
	// MaxActiveProcesses / ACLMaxObjects 是两个上限（0 = 框架默认）。
	MaxActiveProcesses int
	ACLMaxObjects      int
	// StrictGroups 是否启用**严格组 SID 模式**（把 BUILTIN 的 Users SID 也 deny-only）。
	//
	// 代价与取舍见 sandbox.Policy.StrictGroupSIDs 的注释：一句话版——严格模式把
	// "只授予 Users 写权限的目录"（典型是 C:\ProgramData）也堵上，但用 overlapped I/O
	// 写文件的程序（Go/.NET/Rust/Node）会写不进去。
	StrictGroups bool
}

// SandboxPolicy 把装配输入折成 sandbox.Policy（**唯一装配口径**）。
//
// # 允许根口径（写在这里，CLI 帮助与文档都引用它）
//
//	允许根 = 显式 write_dirs **+** CWD（Config.CWD 或进程当前目录）
//
// CWD 总是并入（不是"二选一"）：用户在配置里写了 write_dirs，不等于他不想让
// agent 碰当前工作区——把 CWD 排除掉会让最常见的用法直接不可用。
//
// # 上限口径
//
// 0 = 框架默认（活动进程 64 / ACL 对象 20 万），不是"无限制"。
// 负值由调用方的校验层拒掉（fail-close）。
func SandboxPolicy(o SandboxOptions) sandbox.Policy {
	p := sandbox.DefaultPolicy()
	roots := make([]string, 0, len(o.WriteDirs)+1)
	for _, d := range o.WriteDirs {
		if strings.TrimSpace(d) == "" {
			continue
		}
		roots = append(roots, d)
	}
	if cwd := strings.TrimSpace(o.CWD); cwd != "" {
		roots = append(roots, cwd)
	} else if wd, err := os.Getwd(); err == nil {
		roots = append(roots, wd)
	}
	p.AllowedRoots = roots
	if o.MaxActiveProcesses > 0 {
		p.MaxActiveProcesses = o.MaxActiveProcesses
	}
	if o.ACLMaxObjects > 0 {
		p.ACLMaxObjects = o.ACLMaxObjects
	}
	p.StrictGroupSIDs = o.StrictGroups
	return p
}
