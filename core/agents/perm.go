package agents

import (
	"fmt"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
)

// 权限级别的全序与三闸门之③的判定（K17 / H11 只收窄）。
//
// 口径（任务书）：ReadWriteLevel / ExecLevel 按 none < workspace(allowlist) < full 序比较，
// **跨类不比**（read / write / exec 是三个独立维度，各自比较）。未知级别按最高（rank 3）
// 处理——fail-close：读不出的级别绝不当成可收窄，subset 判定会因此拒绝。

// rwRank 返回读写级别的全序序号。
func rwRank(l contracts.ReadWriteLevel) int {
	switch l {
	case contracts.ReadWriteLevelNone:
		return 0
	case contracts.ReadWriteLevelWorkspace:
		return 1
	case contracts.ReadWriteLevelFull:
		return 2
	default:
		return 3 // 未知 → 最严
	}
}

// execRank 返回执行级别的全序序号。
func execRank(l contracts.ExecLevel) int {
	switch l {
	case contracts.ExecLevelNone:
		return 0
	case contracts.ExecLevelAllowlist:
		return 1
	case contracts.ExecLevelFull:
		return 2
	default:
		return 3 // 未知 → 最严
	}
}

// narrowerRW 取更窄的读写级别（rank 小者；相等时任取，语义等价）。
func narrowerRW(a, b contracts.ReadWriteLevel) contracts.ReadWriteLevel {
	if rwRank(a) <= rwRank(b) {
		return a
	}
	return b
}

// narrowerExec 取更窄的执行级别。
func narrowerExec(a, b contracts.ExecLevel) contracts.ExecLevel {
	if execRank(a) <= execRank(b) {
		return a
	}
	return b
}

// permOf 归一化权限请求：nil = 未请求任何权限 = none（fail-close：不声明即无）。
func permOf(p *contracts.PermissionRequest) contracts.PermissionRequest {
	if p == nil {
		return contracts.PermissionRequest{
			Read:  contracts.ReadWriteLevelNone,
			Write: contracts.ReadWriteLevelNone,
			Exec:  contracts.ExecLevelNone,
		}
	}
	return *p
}

// permIntersect 取两者交集（每维取更窄的一级）；H11 只收窄，结果永不宽于任一输入。
func permIntersect(a, b *contracts.PermissionRequest) contracts.PermissionRequest {
	pa, pb := permOf(a), permOf(b)
	return contracts.PermissionRequest{
		Read:  narrowerRW(pa.Read, pb.Read),
		Write: narrowerRW(pa.Write, pb.Write),
		Exec:  narrowerExec(pa.Exec, pb.Exec),
	}
}

// effectivePerm 计算一个 agent 的有效权限：请求 ∩ ceiling。
// ceiling 为 nil = 无天花板约束（只用请求本身）；否则逐维收窄。
func effectivePerm(req, ceiling *contracts.PermissionRequest) contracts.PermissionRequest {
	if ceiling == nil {
		return permOf(req)
	}
	return permIntersect(req, ceiling)
}

// permSubset 判定 child ⊆ bound（每维 rank 不超过 bound）。未知级别 → rank 3 → 拒绝（fail-close）。
func permSubset(child, bound *contracts.PermissionRequest) bool {
	c, b := permOf(child), permOf(bound)
	return rwRank(c.Read) <= rwRank(b.Read) &&
		rwRank(c.Write) <= rwRank(b.Write) &&
		execRank(c.Exec) <= execRank(b.Exec)
}

// permStr 渲染权限（错误信息用）。
func permStr(p *contracts.PermissionRequest) string {
	x := permOf(p)
	return fmt.Sprintf("read=%s/write=%s/exec=%s", x.Read, x.Write, x.Exec)
}

// PermString 渲染权限（导出给调用方拼错误信息；口径同 permStr）。
func PermString(p *contracts.PermissionRequest) string { return permStr(p) }

// Capability 是权限维度（read / write / exec）——运行期约束工具调用用。
type Capability string

const (
	// CapRead 读维度（读文件 / 查目录）。
	CapRead Capability = "read"
	// CapWrite 写维度（写文件 / 改状态）。
	CapWrite Capability = "write"
	// CapExec 执行维度（跑命令 / 起进程）。
	CapExec Capability = "exec"
)

// execToolNames 是"执行类"工具的名字（跑命令 / 起进程 → 需要 exec 维度）。
//
// 契约的 side_effect 枚举里没有 exec（none/read/write/destructive），而执行类工具的
// side_effect 通常是 write（保守），所以**执行维度只能按工具名判定**：框架内置的
// 执行工具叫 "bash"，常见别名一并列出。名单之外的工具按 side_effect 推导。
var execToolNames = map[string]bool{
	"bash": true, "exec": true, "shell": true, "sh": true, "cmd": true,
	"command": true, "run": true, "powershell": true, "pwsh": true,
}

// RequiredCapabilities 推导工具执行所需的权限维度集合（side_effect + 是否执行类）。
//
//   - side_effect=read → 需要 read；
//   - side_effect=write / destructive → 需要 write；
//   - side_effect=none → 不需要任何维度（如 complete 这类纯回报工具）；
//   - 执行类工具（bash/exec/shell…）→ 额外需要 exec。
//
// 空集合 = 该工具不受权限维度约束。
func RequiredCapabilities(toolName string, side contracts.SideEffect) []Capability {
	var out []Capability
	switch side {
	case contracts.SideEffectRead:
		out = append(out, CapRead)
	case contracts.SideEffectWrite, contracts.SideEffectDestructive:
		out = append(out, CapWrite)
	case contracts.SideEffectNone:
		// 不声明副作用 → 不需要维度（fail-open 方向？否：调用方若要更严，用
		// 执行类判定或直接给该工具配一条策略规则）。
	}
	if execToolNames[strings.ToLower(strings.TrimSpace(toolName))] {
		out = append(out, CapExec)
	}
	return out
}

// PermCovers 判定生效权限是否覆盖某维度（none / 空 = 未授予 → 不覆盖；fail-close）。
//
// 口径：read/write 维度要求 ≥ workspace，exec 维度要求 ≥ allowlist ——
// 也就是"该维度非 none"（更细的路径/命令范围不在这一层约束）。
func PermCovers(p contracts.PermissionRequest, c Capability) bool {
	x := permOf(&p)
	switch c {
	case CapRead:
		return x.Read != contracts.ReadWriteLevelNone && x.Read != ""
	case CapWrite:
		return x.Write != contracts.ReadWriteLevelNone && x.Write != ""
	case CapExec:
		return x.Exec != contracts.ExecLevelNone && x.Exec != ""
	default:
		return false
	}
}
