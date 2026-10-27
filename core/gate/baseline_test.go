package gate

import (
	"strings"
	"testing"
)

// failingReceipt 造一个"命令失败"的 receipt（Failures 从输出提取，与 runtime 侧一致）。
func failingReceipt(command, output string) Receipt {
	return Receipt{
		Commit:         "c0ffee00000000000000000000000000000001",
		ObservedCommit: "c0ffee00000000000000000000000000000001",
		Command:        command,
		ExitCode:       1,
		Failures:       ExtractFailures(output),
		LogTail:        output,
	}
}

// TestJudgeExitZeroPasses：命令成功 → 无需 baseline，直接通过。
func TestJudgeExitZeroPasses(t *testing.T) {
	r := Receipt{ExitCode: 0, Command: "make ci"}
	b := &Baseline{}
	v, err := Judge(&r, b)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v.Kind != VerdictPassed {
		t.Errorf("exit 0 应 VerdictPassed，得到 %v", v.Kind)
	}
}

// TestJudgeRedRepoBaseline 是验收④的核心：红仓上建立基线，之后只拦新增失败。
//
// 场景：命令恒失败（"FAIL: test_x"）。
//  1. 首跑（baseline 空 = 首建模式）→ 只记录不阻断，返回 Recorded，baseline 写入该失败指纹；
//  2. 二跑同一失败 → 全部命中已知 → 通过（Passed）；
//  3. 出现新失败（多一行 "FAIL: test_y"）→ 拦下，且**只列出新增**的那条；
//  4. 换命令 → baseline 记录的是旧命令的指纹，对新命令全部失效 → 全部按新增拦。
func TestJudgeRedRepoBaseline(t *testing.T) {
	const command = "make ci"

	// 1. 首建：baseline 尚无 entries → record 模式。
	baseline := &Baseline{}
	r1 := failingReceipt(command, "FAIL: test_x\nassertion error in foo")
	v1, err := Judge(&r1, baseline)
	if err != nil {
		t.Fatalf("首跑 Judge: %v", err)
	}
	if v1.Kind != VerdictRecorded {
		t.Fatalf("首建应 VerdictRecorded（只记录不阻断），得到 %v（new=%v）", v1.Kind, v1.NewFailures)
	}
	if !v1.Passed() {
		t.Error("recorded 应按通过语义处理（Passed()==true）")
	}
	if len(baseline.Entries) == 0 {
		t.Fatal("首建后 baseline 应写入失败指纹")
	}
	if baseline.Command != command {
		t.Errorf("baseline.Command = %q，期望记录当前命令 %q", baseline.Command, command)
	}
	if baseline.Version != baselineVersion {
		t.Errorf("baseline.Version = %d，期望 %d", baseline.Version, baselineVersion)
	}
	recordedCount := len(baseline.Entries)

	// 2. 二跑同一失败 → 全部命中已知 → 通过。
	r2 := failingReceipt(command, "FAIL: test_x\nassertion error in foo")
	v2, err := Judge(&r2, baseline)
	if err != nil {
		t.Fatalf("二跑 Judge: %v", err)
	}
	if v2.Kind != VerdictPassed {
		t.Errorf("同失败二跑应 VerdictPassed，得到 %v（new=%v）", v2.Kind, v2.NewFailures)
	}
	if len(baseline.Entries) != recordedCount {
		t.Errorf("通过时不应改动 baseline：entries %d → %d", recordedCount, len(baseline.Entries))
	}

	// 3. 新失败出现（在两条已知失败之外多一条 "FAIL: test_y"）→ 拦下，且只列新增。
	r3 := failingReceipt(command, "FAIL: test_x\nassertion error in foo\nFAIL: test_y")
	v3, err := Judge(&r3, baseline)
	if err != nil {
		t.Fatalf("三跑 Judge: %v", err)
	}
	if v3.Kind != VerdictBlocked {
		t.Fatalf("新失败应 VerdictBlocked，得到 %v", v3.Kind)
	}
	if v3.Passed() {
		t.Error("blocked 时 Passed() 应为 false")
	}
	if len(v3.NewFailures) != 1 || v3.NewFailures[0] != "FAIL: test_y" {
		t.Errorf("应只列出新增失败 [FAIL: test_y]，得到 %v", v3.NewFailures)
	}
}

// TestJudgeCommandChange：换命令 → baseline 指纹全部失效，全部按新增拦。
func TestJudgeCommandChange(t *testing.T) {
	baseline := &Baseline{}
	old := failingReceipt("make ci", "FAIL: old_thing")
	if _, err := Judge(&old, baseline); err != nil {
		t.Fatalf("建基线: %v", err)
	}
	if len(baseline.Entries) == 0 {
		t.Fatal("建基线后应有 entries")
	}

	// 换成一个新命令，输出全新。
	newCmd := failingReceipt("make lint", "FAIL: totally_new_failure")
	v, err := Judge(&newCmd, baseline)
	if err != nil {
		t.Fatalf("换命令 Judge: %v", err)
	}
	if v.Kind != VerdictBlocked {
		t.Fatalf("换命令应 VerdictBlocked（baseline 是旧命令的指纹），得到 %v", v.Kind)
	}
	if len(v.NewFailures) != len(newCmd.Failures) {
		t.Errorf("换命令应把本次全部失败列为新增：得到 %v，本次失败 %v", v.NewFailures, newCmd.Failures)
	}
}

// TestJudgeRecordFlagForcesReRecord：-record（baseline.Record=true）即便已有 entries 也重新记录。
func TestJudgeRecordFlagForcesReRecord(t *testing.T) {
	baseline := &Baseline{}
	first := failingReceipt("make ci", "FAIL: a")
	if _, err := Judge(&first, baseline); err != nil {
		t.Fatalf("首记: %v", err)
	}
	n := len(baseline.Entries)

	// 手动开启 -record，跑一个含新失败的 receipt → 应进入记录模式而非拦截。
	baseline.Record = true
	rerun := failingReceipt("make ci", "FAIL: a\nFAIL: b_new")
	v, err := Judge(&rerun, baseline)
	if err != nil {
		t.Fatalf("重记 Judge: %v", err)
	}
	if v.Kind != VerdictRecorded {
		t.Errorf("-record 应强制记录模式，得到 %v", v.Kind)
	}
	if len(baseline.Entries) <= n {
		t.Errorf("重记应并入新失败指纹：entries %d → %d", n, len(baseline.Entries))
	}
	// 重记后 baseline.Command 仍对应当前命令。
	if baseline.Command != "make ci" {
		t.Errorf("重记后 Command = %q，期望 make ci", baseline.Command)
	}
}

// TestJudgeNilBaseline：nil baseline 报错（记录模式需要一个可写入的 baseline，不 panic）。
func TestJudgeNilBaseline(t *testing.T) {
	r := failingReceipt("make ci", "FAIL: x")
	if _, err := Judge(&r, nil); err == nil {
		t.Error("nil baseline 应报错")
	}
	// nil receipt 同理。
	if _, err := Judge(nil, &Baseline{}); err == nil {
		t.Error("nil receipt 应报错")
	}
}

// TestBaselineRecordDedup：record 对重复指纹去重。
func TestBaselineRecordDedup(t *testing.T) {
	b := &Baseline{}
	b.record([]string{"FAIL: a", "FAIL: b", "FAIL: a"})
	if len(b.Entries) != 2 {
		t.Errorf("record 应去重：得到 %d 条，期望 2", len(b.Entries))
	}
	b.record([]string{"FAIL: a"}) // 再记同一条 → 不增
	if len(b.Entries) != 2 {
		t.Errorf("重复记录不应增长：得到 %d，期望 2", len(b.Entries))
	}
}

// TestBaselineMarshalRoundTrip：baseline 序列化往返；瞬态 Record 位不持久化。
func TestBaselineMarshalRoundTrip(t *testing.T) {
	b := &Baseline{}
	b.record([]string{"FAIL: a", "FAIL: b"})
	b.Record = true // 瞬态，不应出现在 JSON 里
	data, err := b.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := ParseBaseline(data)
	if err != nil {
		t.Fatalf("ParseBaseline: %v", err)
	}
	if got.Record {
		t.Error("Record 是瞬态位，不应从磁盘恢复为 true")
	}
	if len(got.Entries) != len(b.Entries) {
		t.Errorf("往返后 entries 数 = %d，期望 %d", len(got.Entries), len(b.Entries))
	}
	for i := range b.Entries {
		if got.Entries[i] != b.Entries[i] {
			t.Errorf("entries[%d] = %+v，期望 %+v", i, got.Entries[i], b.Entries[i])
		}
	}
}

func TestParseBaselineRejects(t *testing.T) {
	if _, err := ParseBaseline([]byte("not json")); err == nil {
		t.Error("非法 JSON 应报错")
	}
}

// TestVerdictKindString：VerdictKind 可打印（事件 / 日志用）。
func TestVerdictKindString(t *testing.T) {
	for k, want := range map[VerdictKind]string{
		VerdictPassed:   "passed",
		VerdictRecorded: "recorded",
		VerdictBlocked:  "blocked",
	} {
		if got := k.String(); got != want {
			t.Errorf("VerdictKind(%d).String() = %q，期望 %q", int(k), got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// 静默失败（fail-close）：非零退出但没有可归一化的失败条目
// ---------------------------------------------------------------------------

// silentReceipt 造一个"静默失败"的 receipt：非零退出、输出为空、失败提取为空。
func silentReceipt(command string, exitCode int) Receipt {
	return Receipt{
		Commit:         "c0ffee00000000000000000000000000000001",
		ObservedCommit: "c0ffee00000000000000000000000000000001",
		Command:        command,
		ExitCode:       exitCode,
		LogTail:        "",
		// Run 侧的显式标注：非零退出但没提取出任何失败条目。
		FailuresExtractionEmpty: true,
	}
}

// TestJudgeSilentFailureIsBlocked：非零退出 + 空输出 + **非空 baseline** → blocked
// （此前 Failures 为空 = "没有新增失败" → 直接放行，静默失败绕过了门禁）。
func TestJudgeSilentFailureIsBlocked(t *testing.T) {
	const command = "make ci"

	// 先用一条正常失败建一个非空 baseline。
	baseline := &Baseline{}
	known := failingReceipt(command, "FAIL: known_x")
	if _, err := Judge(&known, baseline); err != nil {
		t.Fatalf("建基线: %v", err)
	}
	if len(baseline.Entries) == 0 {
		t.Fatal("baseline 应有 entries")
	}

	// 静默失败 → 必须拦下。
	r := silentReceipt(command, 3)
	v, err := Judge(&r, baseline)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v.Kind != VerdictBlocked {
		t.Fatalf("静默失败应 VerdictBlocked，得到 %v（new=%v）", v.Kind, v.NewFailures)
	}
	if v.Passed() {
		t.Error("blocked 时 Passed() 应为 false")
	}
	if len(v.NewFailures) != 1 {
		t.Fatalf("应有 1 条新增失败，得到 %v", v.NewFailures)
	}
	if !strings.Contains(v.NewFailures[0], "未产出可归一化的失败条目") {
		t.Fatalf("失败说明应讲清是未知失败：%q", v.NewFailures[0])
	}
	if strings.Contains(v.NewFailures[0], UnknownFailureMarker) {
		t.Fatalf("给用户看的说明不该用内部标记：%q", v.NewFailures[0])
	}
}

// TestJudgeSilentFailureRecordModeRecordsFingerprint：同场景**记录模式** → recorded，
// 且 baseline 里有该未知失败的指纹（红仓可建基线，不落陷阱 20 死锁）。
func TestJudgeSilentFailureRecordModeRecordsFingerprint(t *testing.T) {
	const command = "make ci"
	baseline := &Baseline{}

	r := silentReceipt(command, 3)
	v, err := Judge(&r, baseline)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v.Kind != VerdictRecorded {
		t.Fatalf("记录模式应 VerdictRecorded，得到 %v", v.Kind)
	}
	if !v.Passed() {
		t.Error("recorded 应按通过语义处理")
	}
	// baseline 里有未知失败的指纹。
	want := FailureFingerprint(UnknownFailureEntry(3))
	found := false
	for _, e := range baseline.Entries {
		if e.Hash == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("baseline 应含未知失败指纹 %s（entries=%+v）", want, baseline.Entries)
	}
}

// TestJudgeSilentFailureKnownAfterRecord：记录后再跑同款静默失败 → passed（已知）。
func TestJudgeSilentFailureKnownAfterRecord(t *testing.T) {
	const command = "make ci"
	baseline := &Baseline{}
	first := silentReceipt(command, 3)
	if _, err := Judge(&first, baseline); err != nil {
		t.Fatalf("建基线: %v", err)
	}
	n := len(baseline.Entries)

	again := silentReceipt(command, 3)
	v, err := Judge(&again, baseline)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v.Kind != VerdictPassed {
		t.Fatalf("已记录的静默失败应 VerdictPassed，得到 %v（new=%v）", v.Kind, v.NewFailures)
	}
	if len(baseline.Entries) != n {
		t.Errorf("通过时不应改动 baseline：entries %d → %d", n, len(baseline.Entries))
	}
	// 退出码不同 → 另一个指纹 → 新增失败（不同款式的静默失败不能被当成同一条）。
	diff := silentReceipt(command, 9)
	v2, err := Judge(&diff, baseline)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v2.Kind != VerdictBlocked {
		t.Fatalf("不同退出码的静默失败应算新增，得到 %v", v2.Kind)
	}
}

// TestJudgeSilentFailurePlusNewExplicitFailure：静默失败 + 一条新的显性失败 → blocked
// 且**都**列出。
func TestJudgeSilentFailurePlusNewExplicitFailure(t *testing.T) {
	const command = "make ci"
	baseline := &Baseline{}
	known := failingReceipt(command, "FAIL: known_x")
	if _, err := Judge(&known, baseline); err != nil {
		t.Fatalf("建基线: %v", err)
	}

	r := failingReceipt(command, "FAIL: brand_new")
	r.FailuresExtractionEmpty = false
	// 模拟"输出里既有显性失败、又有静默失败"：Failures 非空 → 不是未知失败。
	// 真正的混合场景：一次运行里输出有显性失败，但另有一条静默失败——这里用
	// "显性失败 + 未知失败"两条并存来验证清单完整。
	r2 := silentReceipt(command, 3)
	r2.Failures = []string{"FAIL: brand_new"}

	v, err := Judge(&r2, baseline)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v.Kind != VerdictBlocked {
		t.Fatalf("应 VerdictBlocked，得到 %v", v.Kind)
	}
	if len(v.NewFailures) != 2 {
		t.Fatalf("应列出 2 条新增失败（显性 + 未知），得到 %v", v.NewFailures)
	}
	joined := strings.Join(v.NewFailures, "\n")
	if !strings.Contains(joined, "FAIL: brand_new") {
		t.Fatalf("应列出显性失败：%v", v.NewFailures)
	}
	if !strings.Contains(joined, "未产出可归一化的失败条目") {
		t.Fatalf("应列出未知失败说明：%v", v.NewFailures)
	}
}

// TestJudgeSilentFailureWithoutExplicitMarkerStillBlocked：receipt 没带显式标注
// （来自别处的旧 receipt）但失败列表为空 → 仍然 fail-close 拦下（不靠隐式推断放行）。
func TestJudgeSilentFailureWithoutExplicitMarkerStillBlocked(t *testing.T) {
	baseline := &Baseline{}
	known := failingReceipt("make ci", "FAIL: known_x")
	if _, err := Judge(&known, baseline); err != nil {
		t.Fatalf("建基线: %v", err)
	}
	r := Receipt{Command: "make ci", ExitCode: 3} // 无标记、Failures 为空
	v, err := Judge(&r, baseline)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v.Kind != VerdictBlocked {
		t.Fatalf("无标记的空失败列表也应 fail-close 拦截，得到 %v", v.Kind)
	}
}

// ---------------------------------------------------------------------------
// 记录模式的口径：显式 Record=true 或 **基线尚未建立**（文件不存在 = 首建）
// ---------------------------------------------------------------------------

// TestJudgeEstablishedEmptyBaselineIsBlocked：文件存在但**一条条目都没有**
// （被清空/写坏）→ fail-close 拦下 + 可读理由。
//
// 旧口径用"entries 为空"当记录模式 → 空基线会把任何失败都记成"已知"然后放行，
// 门禁就此静默失效。
func TestJudgeEstablishedEmptyBaselineIsBlocked(t *testing.T) {
	baseline := &Baseline{Established: true} // 文件存在，但条目为空
	r := failingReceipt("make ci", "FAIL: anything")

	v, err := Judge(&r, baseline)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v.Kind != VerdictBlocked {
		t.Fatalf("空基线（已建立）应 VerdictBlocked，得到 %v（new=%v）", v.Kind, v.NewFailures)
	}
	if len(v.NewFailures) != 1 || !strings.Contains(v.NewFailures[0], "基线文件为空") {
		t.Fatalf("应给出基线为空/被清空的可读理由，得到 %v", v.NewFailures)
	}
	if !strings.Contains(v.NewFailures[0], "-record") {
		t.Fatalf("理由应提示用 -record 重建：%v", v.NewFailures)
	}
}

// TestJudgeUnestablishedBaselineRecords：基线**尚未建立**（文件不存在 = 首建）→ 记录模式。
func TestJudgeUnestablishedBaselineRecords(t *testing.T) {
	baseline := &Baseline{} // Established=false = 首建
	r := failingReceipt("make ci", "FAIL: a")

	v, err := Judge(&r, baseline)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v.Kind != VerdictRecorded {
		t.Fatalf("未建立的基线应 VerdictRecorded（首建），得到 %v", v.Kind)
	}
	if !baseline.Established {
		t.Error("记录后基线应标记为已建立（下次不再进记录模式）")
	}
	if len(baseline.Entries) == 0 {
		t.Error("记录后应有 entries")
	}
}

// TestJudgeExplicitRecordForcesRecordMode：显式 Record=true（-record）即便基线已建立
// 也进记录模式（重建基线）。
func TestJudgeExplicitRecordForcesRecordMode(t *testing.T) {
	baseline := &Baseline{Established: true}
	r := failingReceipt("make ci", "FAIL: a")
	if _, err := Judge(&r, baseline); err != nil {
		t.Fatalf("首记: %v", err)
	}
	n := len(baseline.Entries)

	baseline.Record = true
	rerun := failingReceipt("make ci", "FAIL: a\nFAIL: b_new")
	v, err := Judge(&rerun, baseline)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v.Kind != VerdictRecorded {
		t.Fatalf("-record 应强制记录模式，得到 %v", v.Kind)
	}
	if len(baseline.Entries) <= n {
		t.Errorf("-record 应并入新失败指纹：entries %d → %d", n, len(baseline.Entries))
	}
}

// TestJudgeEstablishedBaselineWithEntriesComparesNormally：已建立且有条目的基线 →
// 走正常的"只拦新增"比对（不被误当成记录模式）。
func TestJudgeEstablishedBaselineWithEntriesComparesNormally(t *testing.T) {
	baseline := &Baseline{} // 首建 → 记录模式
	known := failingReceipt("make ci", "FAIL: known_x")
	if v, err := Judge(&known, baseline); err != nil || v.Kind != VerdictRecorded {
		t.Fatalf("首建应 VerdictRecorded：v=%v err=%v", v.Kind, err)
	}
	if !baseline.Established {
		t.Fatal("记录后基线应标记为已建立")
	}
	// 同款失败 → 通过（不是 recorded）。
	same := failingReceipt("make ci", "FAIL: known_x")
	v, err := Judge(&same, baseline)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v.Kind != VerdictPassed {
		t.Fatalf("已知失败应 VerdictPassed，得到 %v", v.Kind)
	}
	// 新失败 → blocked（走"只拦新增"比对，不是记录模式）。
	added := failingReceipt("make ci", "FAIL: known_x\nFAIL: brand_new")
	v2, err := Judge(&added, baseline)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if v2.Kind != VerdictBlocked {
		t.Fatalf("新失败应 VerdictBlocked，得到 %v", v2.Kind)
	}
	if len(v2.NewFailures) != 1 || v2.NewFailures[0] != "FAIL: brand_new" {
		t.Fatalf("应只列出新增失败，得到 %v", v2.NewFailures)
	}
}
