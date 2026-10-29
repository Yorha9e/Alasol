package sandbox

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// 默认值集中在这里（D21：数字与清单只在源头定义一次）。
const (
	// DefaultMaxActiveProcesses 是 job object 的活动进程上限。
	//
	// 为什么给默认值：fork 炸弹是"工具子进程"最现实的资源攻击面（模型被提示注入后
	// 跑 `:(){ :|:& };:` 之类的玩意）。进程数有上限，最坏情况才是有界的最坏情况。
	DefaultMaxActiveProcesses = 64

	// DefaultACLMaxObjects 是给允许根施加 ACL 前，先数一下子树里有多少对象的上限。
	//
	// 为什么需要它：Windows 在父目录上设"可继承 ACE"时会**把 ACE 传播到既有子对象**
	// （本机实测确认，见 acl_windows.go 的说明）。在 `C:\` 这种根上误设一次，
	// 传播会爬满整盘。所以先数后设，超限就 fail-close，不闷头改。
	DefaultACLMaxObjects = 200000
)

// Policy 是沙箱策略（装配方可配的部分）。
//
// **零值不是一个沙箱**（所有开关默认关）——这是有意的 fail-close：拿零值 Policy 去 New
// 会被 Validate 拒掉，而不是"悄悄跑一个没有约束的环境"。要一个全开的沙箱，
// 从 DefaultPolicy() 开始改。
type Policy struct {
	// AllowedRoots 是"允许子进程读写"的根目录集合（绝对路径；空 = 只用 SandboxTempRoot）。
	//
	// 语义（**写与读分开说**，别过度声明）：
	//
	//   - **写**：这些根之外，内核拒绝（目录 ACL + 受限 token 的组 SID 收紧）。
	//     例外：BUILTIN\Users 有写权限的位置（如 C:\ProgramData）在**默认模式**下
	//     仍可写——见 StrictGroupSIDs 与包文档的"诚实账本"。
	//   - **读**：**用户私有文件**（.env / .ssh / 用户 profile 下的东西）读不到
	//     （用户 SID 被转成 deny-only）；但**系统/共享位置**（Program Files、
	//     系统目录等授予 BUILTIN\Users 读权限的地方）默认模式**仍可读**。
	//     严格模式（StrictGroupSIDs）连这些也收紧，代价见该字段的说明。
	//
	// 为什么"用户私有文件读不到"是硬要求：读一个 .env 再回显到 stdout，
	// 与写它一样是泄露。至于"系统位置读不到"——很多工具要读它们，代价大于收益。
	AllowedRoots []string

	// SandboxTempRoot 是沙箱自管的临时根（空 = 派生到系统临时目录下的
	// alasol-sandbox-<pid>-<随机>）。它**总是**被加进允许集，并且子进程的 TMP/TEMP
	// 会指到它——否则受限 token 的子进程写不了真实 %TEMP%（那是用户私有目录），
	// git / npm / 编译器这类工具会莫名失败。
	//
	// 派生出来的临时根在 Env.Close 时会被删掉（F14）；显式指定的这一个**不动**
	// （那是用户自己的目录，我们没理由删）。
	SandboxTempRoot string

	// UseJobObject 是否用 job object 收容进程树（KILL_ON_JOB_CLOSE）。
	UseJobObject bool

	// UseRestrictedToken 是否用受限 token 起子进程。**文件约束的真正来源**。
	//
	// 关掉它 = 子进程跑在宿主身份下 = ACL 那一条腿空转（宿主本来就有用户的全部访问）。
	// 只用于"想看沙箱装配形状、不要强制"的场景。
	UseRestrictedToken bool

	// UseFileACL 是否对允许根施加 DACL（把允许集变成内核强制的可写面）。
	UseFileACL bool

	// MaxActiveProcesses job object 的活动进程上限（0 = DefaultMaxActiveProcesses）。
	MaxActiveProcesses int

	// ACLMaxObjects 施加 ACL 前数子树对象的上限（0 = DefaultACLMaxObjects）。
	ACLMaxObjects int

	// SuppressWER 是否压制 WER 崩溃弹窗（V10：SetErrorMode + WerAddExcludedApplication）。
	SuppressWER bool

	// RedirectTemp 是否把子进程的 TMP/TEMP 指到 SandboxTempRoot。
	RedirectTemp bool

	// DangerousPrivileges 要从 token 里删掉的特权名。
	//
	//   - nil = 默认名单（defaultDangerousPrivileges）；
	//   - **非 nil 的空切片 = 一个都不删**（"显式表达不删"，F17）。
	//
	// 只删"提权/越权"类，不删运行期必需的（SeChangeNotifyPrivilege 是遍历目录用的，
	// 删了连 ls 都会坏）。默认名单见 token_windows.go。
	DangerousPrivileges []string

	// StrictGroupSIDs 是否把 **BUILTIN\Users** 也置成 deny-only（严格模式）。
	//
	// 默认 false：Users 保持 enabled。理由是本机实测——Users deny-only 之后，
	// **用 overlapped I/O 写文件的程序全部写不进去**（Go/.NET/Rust/Node 都是）：
	// Go 的 os.File.Write 返回 `winapi error #10093`，连允许根里的文件都写不了。
	// 沙箱会退化成"只能跑 cmd 内建命令"。
	//
	// 代价（默认模式）：**只授予 BUILTIN\Users 写权限的目录仍然可写** ——
	// 典型是 C:\ProgramData（本机实测其 ACE 为 BUILTIN\Users:(CI)(WD,AD,WEA,WA)）。
	// 开了这个开关，那个洞就堵上了（Public / 仓库盘那类由 INTERACTIVE /
	// Authenticated Users 授权的位置，默认模式就已经堵住了）。
	//
	// 怎么选：要在"洞全堵"与"工具还能写文件"之间取舍——本机实测两者不可兼得
	// （见 token_windows.go 的白名单说明）。
	StrictGroupSIDs bool
}

// DefaultPolicy 返回一个"全开"的沙箱策略（三件套 + WER 压制 + 临时目录重定向）。
//
// 装配方从这里起步改（通常是只改 AllowedRoots），不要手搓零值——零值会被 Validate 拒。
func DefaultPolicy() Policy {
	return Policy{
		UseJobObject:       true,
		UseRestrictedToken: true,
		UseFileACL:         true,
		SuppressWER:        true,
		RedirectTemp:       true,
	}
}

// Validate 校验策略（fail-close：坏配置在装配期就炸，不留到运行期）。
func (p Policy) Validate() error {
	for _, r := range p.AllowedRoots {
		if strings.TrimSpace(r) == "" {
			return fmt.Errorf("sandbox: Policy.AllowedRoots 含空路径")
		}
		if !filepath.IsAbs(r) {
			return fmt.Errorf("sandbox: Policy.AllowedRoots 必须是绝对路径（得到 %q）", r)
		}
	}
	if p.MaxActiveProcesses < 0 {
		return fmt.Errorf("sandbox: Policy.MaxActiveProcesses 不能为负（得到 %d；0 = 默认 %d）",
			p.MaxActiveProcesses, DefaultMaxActiveProcesses)
	}
	if p.ACLMaxObjects < 0 {
		return fmt.Errorf("sandbox: Policy.ACLMaxObjects 不能为负（得到 %d；0 = 默认 %d）",
			p.ACLMaxObjects, DefaultACLMaxObjects)
	}
	// 一条约束都不开 → 这不是沙箱，拒绝装配（否则调用方以为自己在沙箱里跑）。
	if !p.UseJobObject && !p.UseRestrictedToken && !p.UseFileACL {
		return fmt.Errorf("sandbox: Policy 一条约束都没开（UseJobObject/UseRestrictedToken/UseFileACL " +
			"全 false）；零值 Policy 不是沙箱，请从 DefaultPolicy() 起步")
	}
	// ACL 开着而受限 token 关着 = 自相矛盾：ACL 授予的 SID 宿主身份本来就有，
	// 文件约束一条都不生效，却让装配方以为"沙箱开着"。这正是双假象比没有更危险。
	if p.UseFileACL && !p.UseRestrictedToken {
		return fmt.Errorf("sandbox: Policy.UseFileACL=true 但 UseRestrictedToken=false：受限 token 是文件约束的来源，" +
			"关掉它之后 ACL 只是空转（宿主本来就有用户的全部访问）。要关就两个一起关")
	}
	return nil
}

// effectiveMaxActiveProcesses 返回**实际生效**的 job 活动进程上限（F3）。
//
// 0 = DefaultMaxActiveProcesses（文档承诺的默认值）。之前 0 被当成"不设上限"，
// 于是 Policy 文档写着"0 = 默认 64"而实际是"无上限"——两处口径不一致。
func (p Policy) effectiveMaxActiveProcesses() int {
	if p.MaxActiveProcesses > 0 {
		return p.MaxActiveProcesses
	}
	return DefaultMaxActiveProcesses
}

// effectiveAllowedRoots 返回最终生效的允许根集合（含沙箱临时根，已归一化去重）。
func (p Policy) effectiveAllowedRoots() []string {
	seen := map[string]struct{}{}
	var out []string
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s == "" {
			return
		}
		abs, err := filepath.Abs(s)
		if err != nil {
			return
		}
		key := pathKey(abs)
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		out = append(out, abs)
	}
	for _, r := range p.AllowedRoots {
		add(r)
	}
	if t := strings.TrimSpace(p.SandboxTempRoot); t != "" {
		add(t)
	}
	return out
}

// defaultSandboxTempRoot 派生沙箱临时根（系统临时目录下，按进程 + 随机后缀隔离）。
//
// 为什么带随机后缀：多个沙箱 env（同一进程里也可能有——测试并行、host + cmd 各建一个）
// 共用一个临时目录时，一个 Close 会把另一个的临时文件删掉。
//
// 为什么"随进程退出自然清理"这句话不成立（F14）：进程退出**不会**删这个目录
// （Windows 的 %TEMP% 只在登录时清理老目录），所以 Env.Close 会显式删掉它
// （前提是它是**派生**出来的——用户显式指定的 SandboxTempRoot 不动）。
func defaultSandboxTempRoot() string {
	base := os.TempDir()
	if strings.TrimSpace(base) == "" {
		base = "."
	}
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 随机源坏了也不该 fail-close 到"装不了沙箱"：退化成 pid 维度（弱一点但可用）。
		return filepath.Join(base, fmt.Sprintf("alasol-sandbox-%d", os.Getpid()))
	}
	return filepath.Join(base, fmt.Sprintf("alasol-sandbox-%d-%s", os.Getpid(), hex.EncodeToString(b[:])))
}

// pathKey 是路径身份比较的键（陷阱 4：Windows 路径大小写不敏感 + 分隔符要统一）。
//
// 注意：这里**不做**符号链接解析（EvalSymlinks 在 Windows 上不解析 junction，
// 逐组件 Readlink 才是正解，见 S13）。沙箱的允许集比较用"词法归一化"，
// junction 绕过属于已知边界——真要堵，得在 ACL 侧对真实路径生效（内核本来就认真实路径，
// 所以 junction 指向目录外时子进程仍然写不进去：junction 的另一端不在允许集里）。
func pathKey(p string) string {
	q := filepath.Clean(p)
	q = strings.ReplaceAll(q, "/", "\\")
	q = strings.TrimRight(q, "\\")
	// "C:" 这种卷根不能把尾分隔符裁掉（否则 "C:" 会变成 "c:"，前缀匹配会退化）。
	if len(q) == 2 && q[1] == ':' {
		q += "\\"
	}
	return strings.ToLower(q)
}

// underRoot 报告 path 是否在 root 之下（含 root 自身）。
//
// 比较用 pathKey（词法归一化 + 小写，陷阱 4）。不解析符号链接——理由见 pathKey。
func underRoot(path, root string) bool {
	pk, rk := pathKey(path), pathKey(root)
	if pk == rk {
		return true
	}
	// 卷根（"C:\"）已经带尾分隔符：不能再补一个分隔符去比（"c:\\" vs "c:\\foo"）。
	if strings.HasSuffix(rk, "\\") {
		return strings.HasPrefix(pk, rk)
	}
	return strings.HasPrefix(pk, rk+"\\")
}
