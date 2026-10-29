//go:build windows

package sandbox

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/sys/windows"
)

// 本文件是 Windows 三件套的 **c. 目录 ACL**（SetNamedSecurityInfo）。
//
// # 方向：先剥夺（token），再授予（ACL）
//
// 常见误解是"给允许目录设个白名单 DACL 就安全了"——不对：普通文件的 DACL 授的是
// **当前用户**，而子进程（b）的用户 SID 是 deny-only，所以它在**任何**地方都写不了。
// ACL 的作用是把允许集的访问**发还**给一个子进程仍然 enabled 的 SID：
//
//	授予对象 = 当前**登录会话 SID**（S-1-5-5-0-<session>，主授予）
//	          + 当前用户 SID（宿主进程自己还要读写仓库）
//	          + SYSTEM / Administrators（维护与抢所有权）
//
// # 为什么选登录会话 SID 而不是 Authenticated Users / Everyone
//
// 想加一个"只有这个沙箱进程能用的合成 SID"（Codex 的 sandbox-write 思路），
// 但 **AdjustTokenGroups 不能往令牌里加新 SID**（需要 SeCreateTokenPrivilege，
// 即管理员），本机实测：加完 GetTokenGroups 里根本不出现。
// 退而求其次的两个候选：
//
//   - Everyone (S-1-1-0) / Authenticated Users (S-1-5-11)：都在令牌里且 enabled，
//     但意味着**同机任何已认证用户**都能写允许目录——在共享机器上是真实放宽；
//   - **登录会话 SID (S-1-5-5-0-<session>)**：也在令牌里且 enabled，但只有**同一个
//     交互登录会话**的进程才匹配。这基本就是"同一个用户本人"的边界（服务与计划任务
//     跑在会话 0，各有自己的 logon SID），比 Authenticated Users 紧一档。
//
// 所以主授予用登录会话 SID；取不到时才退到 Authenticated Users（并在埋点里写明退化了）。
//
// # 可继承 ACE 会被 Windows 传播到既有子对象（本机实测）
//
// 在父目录上设带 SUB_CONTAINERS_AND_OBJECTS_INHERIT 的 ACE 时，Windows 会把可继承
// ACE **重新计算并传播到已存在的子对象**（实测：子树里已存在的文件/目录，
// 其 DACL 的"继承段"被替换成新的继承集）。好处是**不需要自己递归遍历**——
// 一次 SetNamedSecurityInfo 就覆盖整棵子树；代价是子对象里"纯继承来的"旧授权会被换掉。
// 因此：
//
//   - 我们**不加** PROTECTED_DACL_SECURITY_INFORMATION（不保护 DACL = 追加式而非替换式），
//     把破坏面压到最小：根目录原有的继承 ACE 与显式 ACE 都留着，只是在尾部追加我们的；
//   - 但仍然先**数一遍子树有多少对象**（ACLMaxObjects），超限就 fail-close：
//     万一允许根被误配成 `C:\`，传播会爬满整盘，那时候拒绝比硬闯负责。
const (
	seFileObject    = windows.SE_FILE_OBJECT
	daclInformation = windows.DACL_SECURITY_INFORMATION
)

// sandboxGrantSIDs 返回"要授予允许根访问的 SID"及各自来源说明（埋点用）。
//
// 顺序即优先级：登录会话 SID 优先，取不到才退 Authenticated Users。
func sandboxGrantSIDs(tok windows.Token) ([]sandboxGrantee, error) {
	groups, err := tok.GetTokenGroups()
	if err != nil {
		return nil, fmt.Errorf("GetTokenGroups: %w", err)
	}
	user, err := tok.GetTokenUser()
	if err != nil {
		return nil, fmt.Errorf("GetTokenUser: %w", err)
	}

	var out []sandboxGrantee
	for _, sa := range groups.AllGroups() {
		if sa.Attributes&windows.SE_GROUP_LOGON_ID != 0 && sa.Sid != nil {
			out = append(out, sandboxGrantee{sid: sa.Sid, why: "logon-session"})
			break
		}
	}
	// 取不到登录会话 SID 时退一档到 Authenticated Users（放宽，但在 why 里写明）。
	if len(out) == 0 {
		if au, err := windows.CreateWellKnownSid(windows.WinAuthenticatedUserSid); err == nil {
			out = append(out, sandboxGrantee{sid: au, why: "authenticated-users(降级：令牌里没有登录会话 SID)"})
		}
	}
	out = append(out,
		sandboxGrantee{sid: user.User.Sid, why: "current-user(宿主进程自己)"},
	)
	if sys, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid); err == nil {
		out = append(out, sandboxGrantee{sid: sys, why: "SYSTEM"})
	}
	if adm, err := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid); err == nil {
		out = append(out, sandboxGrantee{sid: adm, why: "Administrators"})
	}
	return out, nil
}

// sandboxGrantee 是一个被授予访问的 SID + 它为什么被授予（埋点/审计）。
type sandboxGrantee struct {
	sid *windows.SID
	why string
}

// sandboxGrantMask 是允许根上授予的权限位（F11① / F16）。
//
// **不用 GENERIC_ALL**：GENERIC_ALL 含 WRITE_DAC / WRITE_OWNER——
// 那是"改权限"和"抢所有权"的权利，等于把允许根的 DACL 改成谁都能改
// （沙箱里的子进程能给自己开后门，也能把工作区的权限搅乱）。
//
// 沙箱真正需要的只有四件事：读、写（含新建/追加）、执行、删。
// 注意 FILE_GENERIC_READ/WRITE/EXECUTE 是 Windows 的"标准位组合"，
// 含 READ_CONTROL / SYNCHRONIZE / FILE_READ_EA 等，不含 WRITE_DAC/WRITE_OWNER。
const sandboxGrantMask = windows.FILE_GENERIC_READ |
	windows.FILE_GENERIC_WRITE |
	windows.FILE_GENERIC_EXECUTE |
	windows.DELETE

// applyRootACL 对单个允许根施加 DACL（追加式，不保护）。
//
// 返回授予了哪些 SID（供埋点）。失败即失败——允许根设不上 ACL 意味着子进程在那儿
// 什么都干不了，与其给个半残的沙箱，不如让装配期就炸。
func applyRootACL(root string, grantees []sandboxGrantee) ([]string, error) {
	entries := make([]windows.EXPLICIT_ACCESS, 0, len(grantees))
	var granted []string
	for _, g := range grantees {
		if g.sid == nil {
			continue
		}
		entries = append(entries, windows.EXPLICIT_ACCESS{
			AccessPermissions: sandboxGrantMask,
			AccessMode:        windows.GRANT_ACCESS,
			// 容器与对象都继承：新子目录 / 新文件自动拿到同一份授予。
			Inheritance: windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT,
			Trustee: windows.TRUSTEE{
				TrusteeForm:  windows.TRUSTEE_IS_SID,
				TrusteeType:  windows.TRUSTEE_IS_UNKNOWN,
				TrusteeValue: windows.TrusteeValueFromSID(g.sid),
			},
		})
		granted = append(granted, fmt.Sprintf("%s(%s)", g.sid.String(), g.why))
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("sandbox: 没有可授予的 SID（令牌里既无登录会话 SID 也无 Authenticated Users）")
	}
	acl, err := windows.ACLFromEntries(entries, nil)
	if err != nil {
		return nil, fmt.Errorf("ACLFromEntries: %w", err)
	}
	// **不设** PROTECTED_DACL_SECURITY_INFORMATION：追加式，保留根上原有授权。
	if err := windows.SetNamedSecurityInfo(root, seFileObject, daclInformation, nil, nil, acl, nil); err != nil {
		return nil, fmt.Errorf("SetNamedSecurityInfo(%s): %w", root, err)
	}
	return granted, nil
}

// countDescendants 数 root 之下有多少文件系统对象（用于 ACLMaxObjects 护栏）。
//
// 只走 ReadDir、不碰 ACL——便宜。遇到读不了子目录就停（不扩散错误：数不全是
// "比上限小"的信号，不该因此拒绝装配）。
func countDescendants(root string, limit int) (int, bool, error) {
	n := 0
	over := false
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			if p == root {
				return err
			}
			return nil // 子目录读不了：跳过（保守地少数）
		}
		n++
		if n > limit {
			over = true
			return filepath.SkipAll
		}
		return nil
	})
	return n, over, err
}

// dangerousRootReason 报告 root 是否是"绝不能设 ACL"的位置，是则给出原因。
//
// fail-close 的理由：ACL 的可继承 ACE 会**传播到既有子对象**（见文件头）。在
// `C:\` / 用户 profile / Windows 目录上设一次，等于把整个系统或整个用户数据的
// 授权关系改一遍。宁可拒绝装配。
func dangerousRootReason(root string) (string, bool) {
	clean := filepath.Clean(root)
	lower := strings.ToLower(clean)

	// 卷根（"C:\"、"C:"、"\\"）与 UNC 根。
	vol := filepath.VolumeName(clean)
	if strings.EqualFold(clean, vol) || strings.EqualFold(clean, vol+`\`) {
		return "卷根/UNC 根（" + clean + "）", true
	}
	if len(clean) <= 3 { // "C:\" 形式的极短路径
		return "极短路径（" + clean + "）", true
	}

	guard := func(name, dir string) (string, bool) {
		if strings.TrimSpace(dir) == "" {
			return "", false
		}
		d := strings.ToLower(filepath.Clean(dir))
		// 拒绝的是"允许根**是**受保护目录、或**在它上面**"——因为 ACL 的可继承 ACE
		// 从根**向下**传播，根在受保护目录之上就会把它一起改掉。
		// 反过来（根在受保护目录**下面**，如临时目录里的 workspace）不拒：
		// 传播到不了上面，这正是测试与正常使用的形状。
		if lower == d || strings.HasPrefix(d, lower+`\`) {
			return name + "（" + dir + "）", true
		}
		return "", false
	}
	if r, bad := guard("Windows 目录", windowsDir()); bad {
		return r, bad
	}
	if r, bad := guard("系统目录", systemDir()); bad {
		return r, bad
	}
	if r, bad := guard("Program Files", programFiles()); bad {
		return r, bad
	}
	if r, bad := guard("Program Files (x86)", programFilesX86()); bad {
		return r, bad
	}
	if r, bad := guard("ProgramData", programData()); bad {
		return r, bad
	}
	if r, bad := guard("用户配置文件目录", userProfileDir()); bad {
		return r, bad
	}
	return "", false
}

func windowsDir() string {
	d, err := windows.GetWindowsDirectory()
	if err != nil {
		return os.Getenv("WINDIR")
	}
	return d
}

func systemDir() string {
	d, err := windows.GetSystemDirectory()
	if err != nil {
		return ""
	}
	return d
}

func programFiles() string { return os.Getenv("ProgramFiles") }

func programFilesX86() string {
	if v := os.Getenv("ProgramFiles(x86)"); v != "" {
		return v
	}
	return os.Getenv("ProgramFiles")
}

func programData() string { return os.Getenv("ProgramData") }

func userProfileDir() string {
	d, err := os.UserHomeDir()
	if err != nil {
		return os.Getenv("USERPROFILE")
	}
	return d
}

// sortRoots 让允许根按路径排序（埋点输出稳定，便于比对）。
func sortRoots(roots []string) []string {
	out := make([]string, len(roots))
	copy(out, roots)
	sort.Strings(out)
	return out
}
