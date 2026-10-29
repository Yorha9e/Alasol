//go:build windows

package sandbox

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

// 本文件是 Windows 三件套的 **b. 受限 token**（CreateRestrictedToken）。
//
// # 它为什么是"文件约束"的真正来源
//
// 目录 ACL（c）只是**授予**；如果子进程仍带着"当前交互用户"的身份，它对整个用户
//  profile 都有写权限，ACL 授予什么都不会改变结果。所以顺序是：
//
//  1. 先把当前用户 SID 转成 **deny-only**（SE_GROUP_USE_FOR_DENY_ONLY）——
//     从此这个 SID 在访问检查里只能"拒"不能"授"，子进程继承了用户身份却**没有**用户的访问；
//  1b. 再把**全部 enabled 的组 SID**（Everyone / Users / Authenticated Users /
//     INTERACTIVE …）也置 deny-only——只禁用户 SID 是不够的：那几组组 SID 授的是
//     "到处都能写"（D:\ 授 Authenticated Users:(M)、Public 授 INTERACTIVE:(WD)），
//     留着它们"目录外写被拒"这条验收在常见位置就站不住（F2）；
//     例外只有登录会话 SID（允许根授的就是它）与 Everyone（映像装载路径需要它）；
//  2. 删掉提权/越权类特权（SeDebug / SeBackup / SeRestore / SeTakeOwnership / …）；
//  3. 再由目录 ACL 把允许集的访问**发还**给一个仍然 enabled 的 SID
//     （当前登录会话 SID，见 acl_windows.go）。
//
// 本机实测（Windows 11 / Go 1.26.5，**非管理员**、UAC 过滤令牌）：
//   - GetTokenUser().User.Attributes 从 0x0 变成 0x10（SE_GROUP_USE_FOR_DENY_ONLY）；
//   - 同一位用户、默认 ACL 的目录：子进程**读写都被拒**（"Access is denied."）；
//   - 能正常起 cmd.exe / 读 System32（说明 token 没有废掉，只是收窄了）；
//   - 组 SID 全量 deny-only 后：C:\Users\Public / C:\ProgramData / D:\ 的写
//     全部变成 ERROR_ACCESS_DENIED，而允许根仍然可写（F2 的实测依据）。
//
// # 两个非显而易见的坑（本机实测）
//
//  1. **CreateRestrictedToken 在 advapi32 里的导出名没有 W 后缀**：
//     它没有字符串参数，所以只有一个 `CreateRestrictedToken`（x/sys/windows 没包它）。
//     写 "CreateRestrictedTokenW" 会直接 "找不到过程"。
//  2. **产物 token 不能直接喂 CreateProcessAsUser**：它的 impersonation level 是
//     SecurityAnonymous(0)，而 CreateProcessAsUser 要求 ≥ SecurityImpersonation，
//     直接用来起进程报 **Access is denied**（看起来像缺权限，其实是级别不够）。
//     解法：DuplicateTokenEx(..., SecurityImpersonation, TokenPrimary) 复制一份再起。
//     做完这两步，**普通用户**就能起受限 token 的子进程，不需要管理员。
//
// 另：源 token 必须以 TOKEN_DUPLICATE 打开，否则 CreateRestrictedToken 也报
// "Access is denied"（只给 TOKEN_QUERY 不够）。

// procCreateRestrictedToken 是 advapi32.CreateRestrictedToken（无 W 后缀，见上文）。
var procCreateRestrictedToken = windows.NewLazySystemDLL("advapi32").NewProc("CreateRestrictedToken")

// defaultDangerousPrivileges 是默认要删掉的特权名单。
//
// 选单原则：**只删"能让进程越过自己身份"的**，不删运行期必需的。
// SeChangeNotifyPrivilege（遍历目录/BFS 枚举）**故意留着**——删了连 `dir` 都会坏，
// 那是把沙箱变成砖头。普通用户的令牌里这些提权特权本来就不在（LookupPrivilegeValue
// 查不到就跳过），所以这份名单对标准用户基本是空操作；对"被 UAC 过滤前/提权启动"
// 的宿主才真正起作用。
var defaultDangerousPrivileges = []string{
	"SeDebugPrivilege",
	"SeBackupPrivilege",
	"SeRestorePrivilege",
	"SeTakeOwnershipPrivilege",
	"SeSecurityPrivilege",
	"SeAssignPrimaryTokenPrivilege",
	"SeCreateTokenPrivilege",
	"SeTcbPrivilege",
	"SeImpersonatePrivilege",
	"SeLoadDriverPrivilege",
	"SeSystemEnvironmentPrivilege",
	"SeManageVolumePrivilege",
	"SeCreateSymbolicLinkPrivilege",
}

// effectiveDangerousPrivileges 解析"要删哪些特权"（F17 的 nil 语义）。
//
//   - nil → 默认名单（defaultDangerousPrivileges）；
//   - 非 nil 的空切片 → **一个都不删**（"显式表达不删"）。
//
// 之前用 len(privileges) == 0 判断，把"空切片"和"没设"混成了一件事——
// 装配方想"一个特权都不动"时表达不出来（只能去改默认值）。
func effectiveDangerousPrivileges(privileges []string) []string {
	if privileges == nil {
		return defaultDangerousPrivileges
	}
	return privileges
}

// restrictedPrimaryToken 从当前进程 token 派生一个"可直接起进程"的受限主 token。
//
// 返回的 token 由调用方 Close。失败即失败（不静默退化）——受限 token 是文件约束的
// 前提，起不来就应该让装配失败，而不是"看起来在沙箱里其实没有"。
//
// # 特权名单的 nil 语义（F17）
//
// 见 effectiveDangerousPrivileges：nil = 默认名单；非 nil 的空切片 = 一个都不删。
func restrictedPrimaryToken(privileges []string, strict bool) (windows.Token, error) {
	privileges = effectiveDangerousPrivileges(privileges)
	// TOKEN_QUERY（读用户/特权）+ TOKEN_DUPLICATE（CreateRestrictedToken 的硬要求）。
	var src windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(),
		windows.TOKEN_QUERY|windows.TOKEN_DUPLICATE, &src); err != nil {
		return 0, fmt.Errorf("OpenProcessToken: %w", err)
	}
	defer src.Close()

	user, err := src.GetTokenUser()
	if err != nil {
		return 0, fmt.Errorf("GetTokenUser: %w", err)
	}
	// 当前用户 SID 进 DisableSids → 它在新 token 里变成 deny-only。这是整个文件约束的支点。
	//
	// 本机实测的一个反直觉点：Attributes 字段填什么**不影响结果**——CreateRestrictedToken
	// 对 DisableSids 一律强制打上 SE_GROUP_USE_FOR_DENY_ONLY（填 0 也一样）。
	// 真正决定"有没有禁用"的是**这个 SID 在不在 DisableSids 列表里**：
	// 把它从列表里去掉，子进程立刻恢复用户的全部文件访问，V8 的收口整体失效
	// （变异自证：TestRestrictedTokenDenyOnlyUserSID / TestFileACLDenies* 全红）。
	// 所以这里显式写 SE_GROUP_USE_FOR_DENY_ONLY 只为**表意**，别当成开关。
	disable, deniedGroups, err := denyOnlySIDs(src, user.User.Sid, strict)
	if err != nil {
		return 0, err
	}
	_ = deniedGroups // 计数走埋点（见 restrictedToken 的 diag）

	var del []windows.LUIDAndAttributes
	for _, name := range privileges {
		var luid windows.LUID
		np, err := windows.UTF16PtrFromString(name)
		if err != nil {
			continue
		}
		if err := windows.LookupPrivilegeValue(nil, np, &luid); err != nil {
			continue // 本 token 没这个特权：跳过（普通用户本来就没有）
		}
		del = append(del, windows.LUIDAndAttributes{Luid: luid, Attributes: windows.SE_PRIVILEGE_REMOVED})
	}

	var restricted windows.Token
	var disPtr, delPtr uintptr
	if len(disable) > 0 {
		disPtr = uintptr(unsafe.Pointer(&disable[0]))
	}
	if len(del) > 0 {
		delPtr = uintptr(unsafe.Pointer(&del[0]))
	}
	// Flags = 0（不用 DISABLE_MAX_PRIVILEGE：那会连 SeChangeNotify 一起废掉）。
	r1, _, callErr := procCreateRestrictedToken.Call(
		uintptr(src), 0,
		uintptr(len(disable)), disPtr,
		uintptr(len(del)), delPtr,
		0, 0,
		uintptr(unsafe.Pointer(&restricted)))
	if r1 == 0 {
		return 0, fmt.Errorf("CreateRestrictedToken: %w", callErr)
	}

	// 关键一步：复制成"impersonation level ≥ SecurityImpersonation 的主 token"，
	// 否则 CreateProcessAsUser 报 Access is denied（见文件头的坑 2）。
	var primary windows.Token
	if err := windows.DuplicateTokenEx(restricted, windows.MAXIMUM_ALLOWED, nil,
		windows.SecurityImpersonation, windows.TokenPrimary, &primary); err != nil {
		windows.CloseHandle(windows.Handle(restricted))
		return 0, fmt.Errorf("DuplicateTokenEx(restricted→primary): %w", err)
	}
	windows.CloseHandle(windows.Handle(restricted))
	return primary, nil
}

// denyOnlySIDs 组装 CreateRestrictedToken 的 DisableSids 列表（F2）。
//
// 列表 = 当前用户 SID + **全部 enabled 的组 SID**（除非在白名单里）。
//
// # 为什么必须把组 SID 也禁掉（只禁用户 SID 是不够的）
//
// 只禁用户 SID 时，组 SID 仍然 enabled，于是子进程**继承**了那些"到处都能写"的
// 授权。本机实测（Windows 11 / 非管理员）：
//
//	C:\Users\Public  授 NT AUTHORITY\INTERACTIVE:(RX,WD,AD)  → 子进程能新建文件
//	C:\ProgramData   授 BUILTIN\Users:(CI)(WD,AD,WEA,WA)      → 子进程能新建文件
//	D:\              授 NT AUTHORITY\Authenticated Users:(M)   → 子进程能改整个盘
//
// 也就是说"目录外写被 OS 拒"这条验收在常见位置**站不住**（用户 SID deny-only 但
// Everyone/Users/Authenticated Users/INTERACTIVE 还在）。把组 SID 一律置 deny-only
// 之后，上面三处的写全部变成 ERROR_ACCESS_DENIED（本机实测）。
//
// # 白名单（保留 enabled 的组 SID）与它们的代价
//
//   - **登录会话 SID**（S-1-5-5-0-<session>）：允许根的 DACL 授的就是它
//     （见 acl_windows.go）。它 deny-only 的话，授予形同虚设——允许根会立刻不可写。
//     令牌里没有登录会话 SID 时退到 Authenticated Users（与 ACL 侧的降级口径一致）。
//   - **Everyone**（S-1-1-0）：**映像装载路径**需要它。本机实测：把 Everyone 也
//     deny-only 之后，连 cmd.exe 都起不来（进程初始化直接 STATUS_ACCESS_DENIED
//     0xC0000022），沙箱会变成砖头。Everyone 在现代 Windows 上只被授予"读/执行"
//     级别的权利（Public / D:\ 的 ACE 授的都是 INTERACTIVE /
//     Authenticated Users / Users，不是 Everyone），所以留着它不放大写面。
//   - **BUILTIN\Users**（S-1-5-32-545）：默认也留着——本机实测把它 deny-only 之后，
//     **用 overlapped I/O 写文件的程序全部写不进去**（Go/.NET/Rust/Node 都用
//     overlapped I/O）：Go 的 os.File.Write 直接返回 `winapi error #10093`，
//     连允许根里的文件都写不了（同一份代码在 Users enabled 时正常；同一台机器、
//     同一个文件，只切换这一个 SID 的 deny-only 状态就能复现）。
//     代价是 C:\ProgramData 这类"只授予 BUILTIN\Users 写权限"的目录仍可写
//     （本机实测：ProgramData 的 ACE 是 BUILTIN\Users:(CI)(WD,AD,WEA,WA)）。
//     **Policy.StrictGroupSIDs = true** 可以把 Users 也收走（洞堵上），代价是
//     沙箱里只剩 cmd 内建命令能用（见 policy.go 的说明）。
//
// 返回值第二个是"被置 deny-only 的组 SID 个数"（埋点用）。
func denyOnlySIDs(src windows.Token, userSID *windows.SID, strict bool) ([]windows.SIDAndAttributes, int, error) {
	groups, err := src.GetTokenGroups()
	if err != nil {
		return nil, 0, fmt.Errorf("GetTokenGroups: %w", err)
	}
	all := groups.AllGroups()

	// 白名单：登录会话 SID；没有就退 Authenticated Users（与 ACL 侧降级口径一致）。
	keep := map[string]bool{}
	hasLogon := false
	for _, sa := range all {
		if sa.Sid == nil {
			continue
		}
		if sa.Attributes&windows.SE_GROUP_LOGON_ID != 0 {
			keep[sa.Sid.String()] = true
			hasLogon = true
		}
	}
	if !hasLogon {
		if au, err := windows.CreateWellKnownSid(windows.WinAuthenticatedUserSid); err == nil {
			keep[au.String()] = true
		}
	}
	// Everyone：映像装载路径需要（见上面的注释）。
	if ev, err := windows.CreateWellKnownSid(windows.WinWorldSid); err == nil {
		keep[ev.String()] = true
	}
	// BUILTIN\Users：默认留着（overlapped I/O 的写会坏，见上面的注释）。
	// 严格模式（Policy.StrictGroupSIDs）才收走它。
	if !strict {
		if us, err := windows.CreateWellKnownSid(windows.WinBuiltinUsersSid); err == nil {
			keep[us.String()] = true
		}
	}

	disable := []windows.SIDAndAttributes{{
		Sid:        userSID,
		Attributes: windows.SE_GROUP_USE_FOR_DENY_ONLY,
	}}
	denied := 0
	for _, sa := range all {
		if sa.Sid == nil {
			continue
		}
		if sa.Attributes&windows.SE_GROUP_ENABLED == 0 {
			continue // 本来就没生效（UAC 过滤后的 Administrators 等）——不动它
		}
		if keep[sa.Sid.String()] {
			continue
		}
		disable = append(disable, windows.SIDAndAttributes{
			Sid:        sa.Sid,
			Attributes: windows.SE_GROUP_USE_FOR_DENY_ONLY,
		})
		denied++
	}
	return disable, denied, nil
}

// tokenDenyOnlyUserSID 报告受限 token 的用户 SID 是否已被置为 deny-only。
//
// 这是测试唯一**不需要管理员权限**就能断言的"token 真的受限了"的观察点：
// 特权集在标准用户机器上可能本来就没有可删的（删了个寂寞），但用户 SID 的属性位
// 从 0x0 翻到 0x10 是确定会发生的。
func tokenDenyOnlyUserSID(tok windows.Token) (bool, error) {
	u, err := tok.GetTokenUser()
	if err != nil {
		return false, err
	}
	return u.User.Attributes&windows.SE_GROUP_USE_FOR_DENY_ONLY != 0, nil
}

// tokenDenyOnlyGroupCount 数派生 token 里被置成 deny-only 的组 SID 个数（埋点用）。
//
// 这是"组 SID 也被收走了"的可观测点：只禁用户 SID 时这个数是 0（组还在 enabled），
// 全量收走之后它等于"令牌组数 - 白名单数"。
func tokenDenyOnlyGroupCount(tok windows.Token) (int, error) {
	groups, err := tok.GetTokenGroups()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, sa := range groups.AllGroups() {
		if sa.Sid == nil {
			continue
		}
		if sa.Attributes&windows.SE_GROUP_USE_FOR_DENY_ONLY != 0 {
			n++
		}
	}
	return n, nil
}

// tokenPrivilegeCount 返回 token 里的特权条数（埋点用：删特权前后各数一次）。
func tokenPrivilegeCount(tok windows.Token) (int, error) {
	var n uint32
	buf := make([]byte, 4096)
	if err := windows.GetTokenInformation(tok, windows.TokenPrivileges, &buf[0], uint32(len(buf)), &n); err != nil {
		return 0, err
	}
	tp := (*windows.Tokenprivileges)(unsafe.Pointer(&buf[0]))
	return int(tp.PrivilegeCount), nil
}
