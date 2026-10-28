package discovery

import "strings"

// gate id 的命名空间前缀（K12 / S16 命名空间补全里的 gate_id）。
//
// 自动 gate 的前缀是**约定**不是解析规则：发现表按 gate_id 整串比较（两个 session 的
// gate_id 字符串相等即同 gate），从不拆前缀——拆前缀就要定义"前缀冲突怎么办"，
// 而整串比较没有这个问题（S16：gate_id 是独立命名空间）。
const (
	// RepoGatePrefix 是 repo 级自动 gate 的前缀：repo:<repo_id>（S14：worktree 按
	// git root 聚合 → 同 repo 的多个 worktree 天然同 gate）。
	RepoGatePrefix = "repo:"
	// WorkflowGatePrefix 是工作流级自动 gate 的前缀：workflow:<run_id>。工作流引擎
	// 在 v3，此处只留钩子（没有 run_id 就不加这个 gate）。
	WorkflowGatePrefix = "workflow:"
)

// join 来源（与契约 contracts.GateJoinSource 枚举同值；core 侧再导一份常量，免得
// 调用方为了写一个字符串去 import contracts）。契约是唯一事实源，这里是镜像。
const (
	SourceAutoRepo     = "auto_repo"
	SourceAutoWorkflow = "auto_workflow"
	SourceExplicit     = "explicit"
)

// MaxGateIDLen 是 gate id 的长度上限（S9：不合则拒绝，不截断、不转义、不补零）。
// 128 对 "repo:<26 字符哈希>" 与人工起的名字都绰绰有余，又足以把异常输入挡在事件流外。
const MaxGateIDLen = 128

// RepoGate 拼出 repo 级自动 gate id（K12：session 创建时自动加入）。
func RepoGate(repoID string) string { return RepoGatePrefix + repoID }

// WorkflowGate 拼出工作流级自动 gate id（K12：有 run_id 才加）。
func WorkflowGate(runID string) string { return WorkflowGatePrefix + runID }

// ValidGateID 校验一个 gate id（S9：失效即拒）。
//
// 规则（故意宽松但封闭）：非空、不超长、无空白/控制字符。**不校验前缀**——显式
// join_gate 允许起任意名字（包括刻意叫 "repo:xxx" 去蹭另一个 repo 的 gate，那是用户
// 自己的可见性选择，不是漏洞）。
func ValidGateID(gateID string) bool {
	if gateID == "" || len(gateID) > MaxGateIDLen {
		return false
	}
	for i := 0; i < len(gateID); i++ {
		if c := gateID[i]; c <= ' ' || c == 0x7f {
			return false // 空白与控制字符会让 id 既不能稳定比较也不能安全进日志
		}
	}
	return true
}

// normalizeGateID 归一化显式 gate id：去首尾空白后校验。
//
// 只去首尾空白（不去内部、不改大小写）——"失效即拒"（S9）与"别把用户的名字改掉"
// 的折中：首尾空白是粘贴痕迹，内部形态是用户意图。
func NormalizeGateID(gateID string) (string, bool) {
	id := strings.TrimSpace(gateID)
	if !ValidGateID(id) {
		return "", false
	}
	return id, true
}
