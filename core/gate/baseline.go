package gate

import (
	"encoding/json"
	"errors"
	"fmt"
)

// baselineVersion 是 baseline 文件格式版本（F7：只增不删、不改义）。
const baselineVersion = 1

// VerdictKind 是门禁判定结果种类（W10b escalate 钩子）。
type VerdictKind int

const (
	// VerdictPassed：通过（命令成功，或失败但全部命中已知失败指纹）。
	VerdictPassed VerdictKind = iota
	// VerdictRecorded：首建 / -record 模式——只记录不阻断，按通过语义处理（W9c/陷阱 20）。
	VerdictRecorded
	// VerdictBlocked：有新增失败——调用方走 checkpoint + 挂起（W10b，阶段 8 实现挂起）。
	VerdictBlocked
)

// String 让 VerdictKind 可打印（事件 / 日志）。
func (k VerdictKind) String() string {
	switch k {
	case VerdictPassed:
		return "passed"
	case VerdictRecorded:
		return "recorded"
	case VerdictBlocked:
		return "blocked"
	default:
		return fmt.Sprintf("VerdictKind(%d)", int(k))
	}
}

// Verdict 是 Judge 的完整结论。
type Verdict struct {
	Kind VerdictKind
	// NewFailures 在 Kind==VerdictBlocked 时为新增失败条目（规范化失败行，人类可读）；
	// 其余情况为空。供装配层填 gate.checked 事件的 new_failures 并向用户展示。
	NewFailures []string
}

// Passed 报告门禁是否放行：passed 与 recorded 都算通过，只有 blocked 拦截（W10b）。
func (v Verdict) Passed() bool { return v.Kind != VerdictBlocked }

// Fingerprint 是一条已知失败指纹（W9c）。
type Fingerprint struct {
	// Hash 是匹配用的规范化哈希（FailureFingerprint 的产出）。
	Hash string `json:"hash"`
	// Label 是人类可读的规范化失败条目（报告 / 审计用，可缺省）。
	Label string `json:"label,omitempty"`
}

// Baseline 是仓库级 CI baseline（W9c：作用域仓库级，分支级会碎片化；可提交进版本库）。
//
// Record 是**瞬态控制位**（json:"-" 不持久化）：装配层在做 -record 时置 true，
// 强制 Judge 进记录模式（即便已有 entries 也重新记录当前失败）。加载自磁盘时它为 false。
type Baseline struct {
	Version int           `json:"version"`
	Command string        `json:"command"` // 基线记录的是哪个命令的指纹（换命令则全部失效）
	Entries []Fingerprint `json:"entries"`
	// Established 表示"基线已建立"（持久化）。
	//
	// 它把两件事分开：**文件不存在（首建）** 与 **文件存在但条目为空（被清空/写坏）**。
	// 口径：**记录模式 = 显式 Record=true 或 基线尚未建立（文件不存在）**。
	//
	//   - 文件不存在 → Established=false → 记录模式（红仓第一次跑靠它记录、不阻断，
	//     陷阱 20：否则红仓永远建不起基线）；
	//   - 文件存在   → Established=true（哪怕里面一条条目都没有）→ 空基线**不是**记录模式，
	//     而是 fail-close 拦下（否则空基线会把任何失败都当成"已知"记录后放行 → 门禁静默失效）。
	//
	// 旧文件（没有这个字段）由加载方按"文件存在即已建立"置 true —— 空条目则由 Judge 拦下。
	Established bool `json:"established"`
	Record      bool `json:"-"` // 瞬态：-record 时置 true，强制记录模式
}

// EmptyBaselineMessage 是"基线已建立但没有任何条目"的可读理由（进 Verdict.NewFailures）。
const EmptyBaselineMessage = "基线文件为空/被清空（没有任何已知失败条目）：请用 -record 重建基线"

// UnknownFailureMarker 是"未知失败"的稳定标记前缀。
//
// 未知失败 = 命令非零退出，但输出里**没有可归一化的失败条目**（静默失败 / OOM kill /
// 崩溃无输出）。它必须有一个**稳定**的指纹：记录模式把这次失败并入 baseline 后，
// 同款的静默失败要能被认成"已知"（红仓可建基线，不落陷阱 20 死锁）。
const UnknownFailureMarker = "__unknown_failure__"

// UnknownFailureEntry 返回未知失败的**规范化条目**（参与指纹哈希 → 进 baseline）。
func UnknownFailureEntry(exitCode int) string {
	return fmt.Sprintf("%s:exit=%d", UnknownFailureMarker, exitCode)
}

// UnknownFailureMessage 返回未知失败的**人类可读说明**（进 Verdict.NewFailures）。
func UnknownFailureMessage(exitCode int) string {
	return fmt.Sprintf("命令失败但未产出可归一化的失败条目：按未知失败拦截（exit=%d）", exitCode)
}

// Judge 依 baseline 判定一次门禁结论（W9c）。
//
// 前提：调用方已确认 receipt 可信（Verify 的可信度部分 ②③④ 通过）；本函数只处理
// "命令失败时，失败是否可接受"，不再复核 commit/command 与当前 tip/配置的关系。
//
//   - 退出码 0：命令成功，无失败 → VerdictPassed（无需 baseline）。
//   - **未知失败**（非零退出但输出里没有可归一化的失败条目：静默失败 / OOM kill /
//     崩溃无输出）：**不能**当成"没有失败"——那会让静默失败直接绕过门禁。
//     它按一条失败条目处理（指纹 = UnknownFailureEntry(exit)）：记录模式照常建基线，
//     正常模式若该指纹不在 baseline → VerdictBlocked，NewFailures 给可读说明。
//   - **记录模式**（baseline.Record 置位，或**基线尚未建立**——即基线文件不存在/首建，
//     或 -record 要求重建）：把本次失败指纹全部并入 baseline，返回 VerdictRecorded
//     （只记录不阻断）。这是红仓能建起基线的前提（陷阱 20：否则第一次跑就死锁）。
//     记录后 Established=true（下次不再进记录模式）。
//   - **基线已建立但一条条目都没有**（文件存在却是空的：被清空/写坏）→ **fail-close 拦下**
//     （VerdictBlocked，理由 = EmptyBaselineMessage）：空基线不是"首建"，若当首建处理，
//     任何失败都会被记成"已知"然后放行 → 门禁静默失效。
//   - **换命令**（baseline.Command 非空且 ≠ receipt.Command）：基线是别的命令的指纹，
//     对当前命令全部失效 → 全部按新增失败拦（VerdictBlocked，NewFailures = 本次全部失败）。
//     要为新命令建基线，用 -record 重记。
//   - 否则：只拦**新增**失败——本次失败里指纹不在 baseline 的 → VerdictBlocked 并列出；
//     全部命中已知 → VerdictPassed。
func Judge(receipt *Receipt, baseline *Baseline) (Verdict, error) {
	if receipt == nil {
		return Verdict{}, errors.New("gate: receipt 为 nil")
	}
	if baseline == nil {
		return Verdict{}, errors.New("gate: baseline 为 nil（记录模式需要一个可写入的 baseline）")
	}
	// 命令成功 → 无失败，直接通过。
	if receipt.ExitCode == 0 {
		return Verdict{Kind: VerdictPassed}, nil
	}

	// 未知失败（fail-close）：非零退出却没有可归一化的失败条目。
	//
	// Run 侧显式标注了 FailuresExtractionEmpty；这里再兜一层（receipt 来自别处、
	// 或旧版本 receipt 没这个标记时，也不能把"空失败列表"当成"没有新增失败"）。
	unknown := ""
	if receipt.FailuresExtractionEmpty || len(receipt.Failures) == 0 {
		unknown = UnknownFailureEntry(receipt.ExitCode)
	}
	failures := cloneStrings(receipt.Failures)
	if unknown != "" {
		failures = append(failures, unknown)
	}

	// 记录模式：**显式 Record**（-record）或 **基线尚未建立**（文件不存在 = 首建）。
	//
	// 刻意**不**用"entries 为空"当记录模式的判据：文件存在却是空的（被清空/写坏）
	// 与"文件不存在"是两回事——前者若进记录模式，任何失败都会被记成"已知"然后放行，
	// 门禁就此静默失效。
	if baseline.Record || !baseline.Established {
		baseline.Version = baselineVersion
		baseline.Command = receipt.Command
		baseline.record(failures)
		baseline.Established = true // 记过了就算"已建立"（下次不再进记录模式）
		return Verdict{Kind: VerdictRecorded}, nil
	}

	// 基线已建立但**一条条目都没有**（文件存在却是空的：被清空 / 写坏）→ fail-close 拦下，
	// 并给出可读理由（不是"新增失败"，而是"基线不可用"）。
	if len(baseline.Entries) == 0 {
		return Verdict{Kind: VerdictBlocked, NewFailures: []string{EmptyBaselineMessage}}, nil
	}

	// 换命令：基线记录的是别的命令 → 全部指纹失效，全部按新增拦。
	if baseline.Command != "" && baseline.Command != receipt.Command {
		return Verdict{Kind: VerdictBlocked, NewFailures: failureLabels(failures, receipt.ExitCode)}, nil
	}

	// 只拦新增：本次失败里，指纹不在 baseline 的才是新增。
	known := make(map[string]bool, len(baseline.Entries))
	for _, e := range baseline.Entries {
		known[e.Hash] = true
	}
	var newFailures []string
	for _, f := range failures {
		if !known[FailureFingerprint(f)] {
			newFailures = append(newFailures, f)
		}
	}
	if len(newFailures) > 0 {
		return Verdict{Kind: VerdictBlocked, NewFailures: failureLabels(newFailures, receipt.ExitCode)}, nil
	}
	return Verdict{Kind: VerdictPassed}, nil
}

// failureLabels 把失败条目转成人类可读的标签：未知失败用可读说明，不用内部标记
// （"__unknown_failure__:exit=1" 是给指纹用的，不该出现在给用户看的清单里）。
func failureLabels(failures []string, exitCode int) []string {
	if len(failures) == 0 {
		return nil
	}
	out := make([]string, 0, len(failures))
	for _, f := range failures {
		if f == UnknownFailureEntry(exitCode) {
			out = append(out, UnknownFailureMessage(exitCode))
			continue
		}
		out = append(out, f)
	}
	return out
}

// record 把一批失败条目并入 baseline（去重，保留既有 entries 的顺序）。
func (b *Baseline) record(failures []string) {
	seen := make(map[string]bool, len(b.Entries))
	for _, e := range b.Entries {
		seen[e.Hash] = true
	}
	for _, f := range failures {
		h := FailureFingerprint(f)
		if seen[h] {
			continue
		}
		seen[h] = true
		b.Entries = append(b.Entries, Fingerprint{Hash: h, Label: f})
	}
}

// Marshal 序列化 baseline（字节级，无文件 I/O——写文件在 runtime/gate）。
func (b *Baseline) Marshal() ([]byte, error) {
	return json.Marshal(b)
}

// ParseBaseline 解析 baseline 内容（字节级，无文件 I/O）。
//
// 版本高于当前认识的版本仍按"只增不删"尽力解析（F7）；字段缺失按零值处理。
//
// 解析出来的 baseline 一律标记为**已建立**：它来自一个**存在的文件**（旧文件没有
// established 字段时同样按"存在即已建立"处理）。空条目不是"首建"——那由 Judge 拦下。
func ParseBaseline(data []byte) (*Baseline, error) {
	var b Baseline
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("gate: 解析 gate-baseline.json 失败：%w", err)
	}
	b.Established = true
	return &b, nil
}

// cloneStrings 拷贝字符串切片（避免 Verdict 与 receipt 共享底层数组）。
func cloneStrings(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	out := make([]string, len(s))
	copy(out, s)
	return out
}
