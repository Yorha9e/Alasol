package walfile_test

// 缺口 5 第一件的钉子：walfile.GC 的判据、报告与失败口径（形态照 blobfs gc_test 先例）。
//
// 口径（与 gc.go 文件头一致）：
//   - Retention 必给（<=0 → 库层报错）：龄期是运维决定，删 WAL 不可逆，不猜安全默认；
//   - 判据三条件全过才删：mtime 超龄 + reconcile 干净（InFlightTurn / OrphanBlobToolCalls
//     两个"没走完"信号全空）+ 合法 `<session>.wal` 形态；
//   - **StaleBlobPlans（作废计划）不挡回收**（F1）：它 = "走完了但没外置"（正常收尾占多数），
//     不是崩溃上下文；Facts 钩子的用途 = 区分 orphan（脏，留）与 stale（干净，可删）；
//   - 删除动作 = **Remove 整个文件**（不是 Truncate 留空壳）；
//   - CorruptLines 不算脏（半截行容忍是 reconcile 的设计）；读失败 → Skipped 且留；
//   - 外来文件 / 子目录一律不动（不扫、不计数、不删）；
//   - DryRun 一个不删，与真跑**同一判据**（数字一致，**除"删不掉"那一刻**：真跑记 Skipped、
//     dry 记 Removed——见 gc_seam_test.go 的分叉钉子）；
//   - 删不掉 → Skipped，不中止整轮。
//
// 变异自证：
//   - 去掉 reconcile 脏判定 → 脏信号钉子红（脏文件被删）；
//   - 把 StaleBlobPlans 改回当脏信号 → facts 矩阵第二行红（stale 该删被留）；
//   - 去掉龄期判定（未超龄也删）→ 年轻钉子红；
//   - DryRun 真删了 → DryRun 钉子红。

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/walfile"
)

// gcLine 造一行 WAL NDJSON（测试 seed 用；与 Writer 写出的形状一致）。
func gcLine(t *testing.T, kind string, payload any) string {
	t.Helper()
	raw, err := json.Marshal(recOf(kind, payload))
	if err != nil {
		t.Fatalf("marshal %s 记录: %v", kind, err)
	}
	return string(raw)
}

// gcCleanLines 造一份"干净收口"的 WAL 内容（begin + end，无孤儿计划）。
func gcCleanLines(t *testing.T) []string {
	t.Helper()
	return []string{
		gcLine(t, core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
		gcLine(t, core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
	}
}

// seedGCWAL 在 dir 下写 `<sid>.wal`（lines 为空 = 空文件），并把 mtime 钉成 age 之前
// （造"超龄"靶子；不传 age = 保持现在 = 年轻）。
func seedGCWAL(t *testing.T, dir string, sid contracts.Ulid, age time.Duration, lines ...string) string {
	t.Helper()
	path := filepath.Join(dir, string(sid)+".wal")
	var content string
	if len(lines) > 0 {
		content = strings.Join(lines, "\n") + "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("写 WAL %s: %v", path, err)
	}
	if age > 0 {
		when := time.Now().Add(-age)
		if err := os.Chtimes(path, when, when); err != nil {
			t.Fatalf("Chtimes %s: %v", path, err)
		}
	}
	return path
}

// gcExists 报告路径是否存在。
func gcExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// TestGCCleanAgedRemoved（判据 1+2+3 的正例）：干净 + 超龄 + 合法名 → **真删**（Remove
// 整个文件，不是 Truncate 留空壳），报告如实（Removed=1、FreedBytes=文件大小）。
func TestGCCleanAgedRemoved(t *testing.T) {
	dir := t.TempDir()
	sid := newSession(t)
	path := seedGCWAL(t, dir, sid, 2*time.Hour, gcCleanLines(t)...)
	wantSize := int64(len(strings.Join(gcCleanLines(t), "\n")) + 1)

	rep, err := walfile.GC(dir, walfile.GCOptions{Retention: time.Hour})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if rep.DryRun {
		t.Error("默认不该是 DryRun")
	}
	if rep.ScannedSessions != 1 || rep.Removed != 1 || rep.Skipped != 0 {
		t.Fatalf("报告 = %+v，want scanned=1 removed=1 skipped=0", rep)
	}
	if rep.FreedBytes != wantSize {
		t.Errorf("FreedBytes = %d，want %d（文件字节数）", rep.FreedBytes, wantSize)
	}
	if gcExists(path) {
		t.Fatal("干净+超龄的 WAL 应被删（Remove 整个文件）")
	}
	// 不是 Truncate：文件得真的没了（空壳 = 回收失败）。
	if fi, err := os.Stat(path); err == nil && fi.Size() == 0 {
		t.Error("不该只是截成空文件（GC 的目的是回收）")
	}
}

// TestGCYoungCleanKept（判据 1）：干净但**未超龄** → 留（不删也不算 Skipped——它只是年轻）。
func TestGCYoungCleanKept(t *testing.T) {
	dir := t.TempDir()
	sid := newSession(t)
	path := seedGCWAL(t, dir, sid, 0, gcCleanLines(t)...) // 刚写：年轻

	rep, err := walfile.GC(dir, walfile.GCOptions{Retention: 24 * time.Hour})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if rep.ScannedSessions != 1 {
		t.Errorf("ScannedSessions = %d，want 1（年轻文件也要数进扫描）", rep.ScannedSessions)
	}
	if rep.Removed != 0 || rep.Skipped != 0 {
		t.Errorf("报告 = %+v，want removed=0 skipped=0（年轻 ≠ 脏）", rep)
	}
	if !gcExists(path) {
		t.Fatal("未超龄的干净 WAL 不该被删")
	}
}

// TestGCDirtyInFlightTurnKept（脏信号①）：**在飞轮次**（begin 无 end）+ 超龄 → 留。
func TestGCDirtyInFlightTurnKept(t *testing.T) {
	dir := t.TempDir()
	sid := newSession(t)
	path := seedGCWAL(t, dir, sid, 2*time.Hour,
		gcLine(t, core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 3, Model: "crashy"}),
		gcLine(t, core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 3, Text: strPtr("崩在这")}),
	)

	rep, err := walfile.GC(dir, walfile.GCOptions{Retention: time.Hour})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if rep.Removed != 0 || rep.Skipped != 1 {
		t.Fatalf("报告 = %+v，want removed=0 skipped=1（在飞轮次 = 崩溃上下文还有用）", rep)
	}
	if !gcExists(path) {
		t.Fatal("有在飞轮次的 WAL 不该被删")
	}
}

// TestGCDirtyOrphanPlanKept（脏信号②）：**孤儿 blob 候选**（blob.plan 在 WAL、事实里无
// completed/failed）+ 超龄 → 留。CLI 不带 Facts（空事实）→ 任何 plan 都走这条保守路径。
func TestGCDirtyOrphanPlanKept(t *testing.T) {
	dir := t.TempDir()
	sid := newSession(t)
	path := seedGCWAL(t, dir, sid, 2*time.Hour,
		gcLine(t, core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
		gcLine(t, core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
		gcLine(t, core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 1, ToolCallID: "tc_ORPHAN"}),
	)

	rep, err := walfile.GC(dir, walfile.GCOptions{Retention: time.Hour}) // 无 Facts = 空事实
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if rep.Removed != 0 || rep.Skipped != 1 {
		t.Fatalf("报告 = %+v，want removed=0 skipped=1（孤儿候选 = 没走完）", rep)
	}
	if !gcExists(path) {
		t.Fatal("有孤儿 blob 候选的 WAL 不该被删")
	}
}

// TestGCStalePlanFactsMatrix（F1 语义钉子）：**作废计划（completed 落了但没 binary_ref）
// 不挡回收**——它表示"走完了但没外置"（绝大多数是"结果不大"的正常收尾），**不是**崩溃
// 上下文。把它当脏信号 = 回收面落空（正常收尾的 session 永远回收不掉，GC 白做）。
//
// 四种 facts 结局的矩阵（一条 blob.plan 在 WAL，事实各不相同）：
//
//	facts 结局                                    期望
//	------------------------------------------------  --------------------------
//	completed 有 binary_ref（外置成功）              → **删**（干净）
//	completed 无 binary_ref（作废计划 / stale）      → **删**（走完了，只是没外置）
//	failed（工具调用失败收口，没产出结果）           → **删**（干净；没写 blob）
//	既无 completed 也无 failed（**真孤儿**）         → **留**（这个工具调用没走完 = 脏）
//
// 外加一条**空事实**（Facts == nil）口径：分不出 orphan/stale → **保守留**（口径不变）。
//
// 变异自证：把 GC 的脏判定改回"StaleBlobPlans 也算脏"→ 本矩阵第二行红（stale 那格
// 期望"删"，旧口径是"留" → 断言失败）。
func TestGCStalePlanFactsMatrix(t *testing.T) {
	cases := []struct {
		name string
		// facts 注入（nil = 空事实）。
		facts func(contracts.Ulid) walfile.Facts
		// wantRemoved = 期望是否被回收（Removed=1 且文件消失）。
		wantRemoved bool
		// wantStale = 期望 StalePlans 计数（信息性观测口：只数"completed 无 binary_ref"）。
		wantStale int64
		// why 是这一格的口径说明（失败时打出来，便于定位）。
		why string
	}{
		{
			name: "completed 有 binary_ref（外置成功）→ 删",
			facts: func(contracts.Ulid) walfile.Facts {
				return walfile.Facts{
					CompletedToolCalls:  map[string]bool{"tc_PLAN": true},
					CompletedBinaryRefs: map[string]bool{"tc_PLAN": true},
					FailedToolCalls:     map[string]bool{},
				}
			},
			wantRemoved: true,
			wantStale:   0,
			why:         "两个名单都不进 = 干净",
		},
		{
			name: "completed 无 binary_ref（作废计划）→ 删",
			facts: func(contracts.Ulid) walfile.Facts {
				return walfile.Facts{
					CompletedToolCalls:  map[string]bool{"tc_PLAN": true},
					CompletedBinaryRefs: map[string]bool{}, // 没外置 → stale
					FailedToolCalls:     map[string]bool{},
				}
			},
			wantRemoved: true,
			wantStale:   1,
			why:         "stale 是信息性信号：走完了（completed 落了），只是没外置 → 不挡回收",
		},
		{
			name: "failed（工具调用失败收口）→ 删",
			facts: func(contracts.Ulid) walfile.Facts {
				return walfile.Facts{
					CompletedToolCalls:  map[string]bool{},
					CompletedBinaryRefs: map[string]bool{},
					FailedToolCalls:     map[string]bool{"tc_PLAN": true},
				}
			},
			wantRemoved: true,
			wantStale:   0,
			why:         "failed 也是收口（那种工具调用根本没产出结果、没写 blob）→ 干净",
		},
		{
			name: "既无 completed 也无 failed（真孤儿）→ 留",
			facts: func(contracts.Ulid) walfile.Facts {
				return walfile.Facts{
					CompletedToolCalls:  map[string]bool{},
					CompletedBinaryRefs: map[string]bool{},
					FailedToolCalls:     map[string]bool{},
				}
			},
			wantRemoved: false,
			wantStale:   0,
			why:         "这个工具调用没走完（结果可能已写成 blob、引用没落）= 脏 → 留",
		},
		{
			name:        "空事实（Facts == nil）→ 保守留",
			facts:       nil,
			wantRemoved: false,
			wantStale:   0,
			why:         "空事实分不出 orphan/stale → 区分不了就不删（口径不变；落进 orphan 不落 stale）",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			sid := newSession(t)
			path := seedGCWAL(t, dir, sid, 2*time.Hour,
				gcLine(t, core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
				gcLine(t, core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
				gcLine(t, core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 1, ToolCallID: "tc_PLAN"}),
			)

			rep, err := walfile.GC(dir, walfile.GCOptions{Retention: time.Hour, Facts: tc.facts})
			if err != nil {
				t.Fatalf("GC: %v", err)
			}
			// 信息性观测口：StalePlans 只数"completed 无 binary_ref"（与留/删无关）。
			if rep.StalePlans != tc.wantStale {
				t.Fatalf("StalePlans = %d，want %d（%s）", rep.StalePlans, tc.wantStale, tc.why)
			}
			if tc.wantRemoved {
				if rep.Removed != 1 || rep.Skipped != 0 {
					t.Fatalf("报告 = %+v，want removed=1 skipped=0（%s）", rep, tc.why)
				}
				if gcExists(path) {
					t.Fatalf("该回收的 WAL 应被删（%s）", tc.why)
				}
				return
			}
			if rep.Removed != 0 || rep.Skipped != 1 {
				t.Fatalf("报告 = %+v，want removed=0 skipped=1（%s）", rep, tc.why)
			}
			if !gcExists(path) {
				t.Fatalf("该留的 WAL 不该被删（%s）", tc.why)
			}
		})
	}
}

// TestGCCleanPlanWithFactsRemoved（Facts 钉子的正半）：plan 在 WAL、事实里 completed
// **带 binary_ref** → 两个名单都不进 → 干净 → 照删。
//
// 为什么需要它：没有 Facts 注入的对账里任何 plan 都是孤儿（判脏）——若 GC 忽略 Facts
// 钩子（恒空事实），"脏信号③"那条钉子照样绿（孤儿路径也判脏）。这条把钩子钉死：
// 钉子：忽略 GCOptions.Facts → 本用例红（plan 被算成孤儿 → 不删）。
func TestGCCleanPlanWithFactsRemoved(t *testing.T) {
	dir := t.TempDir()
	sid := newSession(t)
	path := seedGCWAL(t, dir, sid, 2*time.Hour,
		gcLine(t, core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}),
		gcLine(t, core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}),
		gcLine(t, core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 1, ToolCallID: "tc_OK"}),
	)

	rep, err := walfile.GC(dir, walfile.GCOptions{
		Retention: time.Hour,
		Facts: func(contracts.Ulid) walfile.Facts {
			return walfile.Facts{
				CompletedToolCalls:  map[string]bool{"tc_OK": true},
				CompletedBinaryRefs: map[string]bool{"tc_OK": true},
				FailedToolCalls:     map[string]bool{},
			}
		},
	})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if rep.Removed != 1 || rep.Skipped != 0 {
		t.Fatalf("报告 = %+v，want removed=1 skipped=0（completed+binary_ref = 干净）", rep)
	}
	if gcExists(path) {
		t.Fatal("事实证明已收口的 plan 不该挡住回收")
	}
}

// TestGCCorruptLinesNotDirty（口径钉子）：**半截行容忍**——CorruptLines 不算脏。
// 干净收口 + 尾巴半截行 + 超龄 → 照删（半截是崩溃现场的常态，不是"没走完"的信号）。
func TestGCCorruptLinesNotDirty(t *testing.T) {
	dir := t.TempDir()
	sid := newSession(t)
	lines := gcCleanLines(t)
	lines = append(lines, `{"kind":"turn.delta","payload":{"turn":1,"text":"半截`) // 崩溃残留的半截行
	path := seedGCWAL(t, dir, sid, 2*time.Hour, lines...)

	// 先钉住 fixture：半截行确实被数成 CorruptLines（否则本用例测的就不是它）。
	scan, err := walfile.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if scan.CorruptLines != 1 || len(scan.Records) != 2 {
		t.Fatalf("fixture: corrupt=%d records=%d，want 1/2", scan.CorruptLines, len(scan.Records))
	}

	rep, err := walfile.GC(dir, walfile.GCOptions{Retention: time.Hour})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if rep.Removed != 1 || rep.Skipped != 0 {
		t.Fatalf("报告 = %+v，want removed=1 skipped=0（CorruptLines 不算脏）", rep)
	}
	if gcExists(path) {
		t.Fatal("半截行不该让干净收口的 WAL 留下来")
	}
}

// TestGCSkipsForeignFilesAndDirs（判据 3）：外来文件 / 非法名 / 子目录一律不动——
// 不扫（不计 ScannedSessions）、不删。
func TestGCSkipsForeignFilesAndDirs(t *testing.T) {
	dir := t.TempDir()
	old := 2 * time.Hour

	// 合法靶子（证明扫描在干活）。
	victim := newSession(t)
	victimPath := seedGCWAL(t, dir, victim, old, gcCleanLines(t)...)

	// 外来物（全钉老 mtime：老也没用，不是我们的就不动）。
	foreign := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(foreign, []byte("别动我"), 0o644); err != nil {
		t.Fatal(err)
	}
	badName := filepath.Join(dir, "not-a-ulid.wal") // 后缀对、ULID 非法
	if err := os.WriteFile(badName, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	noSuffix := filepath.Join(dir, string(newSession(t))+".tmp")
	if err := os.WriteFile(noSuffix, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	subdir := filepath.Join(dir, string(newSession(t))+".wal") // 子目录顶着 WAL 名
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{foreign, badName, noSuffix, subdir} {
		when := time.Now().Add(-old)
		if err := os.Chtimes(p, when, when); err != nil {
			t.Fatalf("Chtimes %s: %v", p, err)
		}
	}

	rep, err := walfile.GC(dir, walfile.GCOptions{Retention: time.Hour})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if rep.ScannedSessions != 1 || rep.Removed != 1 || rep.Skipped != 0 {
		t.Fatalf("报告 = %+v，want scanned=1 removed=1 skipped=0（外来物不进计数）", rep)
	}
	if gcExists(victimPath) {
		t.Fatal("合法超龄 WAL 应被删（对照：扫描在干活）")
	}
	for _, p := range []string{foreign, badName, noSuffix} {
		if !gcExists(p) {
			t.Errorf("外来文件不该被删: %s", p)
		}
	}
	if fi, err := os.Stat(subdir); err != nil || !fi.IsDir() {
		t.Errorf("子目录不该被动（stat err=%v）", err)
	}
}

// TestGCDryRunSameNumbersAsReal（p1 教训钉子）：DryRun 一个不删，但 Removed / Skipped /
// ScannedSessions / FreedBytes 与真跑**完全一致**（同一套判据）。
func TestGCDryRunSameNumbersAsReal(t *testing.T) {
	dir := t.TempDir()
	clean := newSession(t) // 干净+超龄 → 会删
	dirty := newSession(t) // 在飞+超龄 → 会跳过
	young := newSession(t) // 干净+年轻 → 不动
	cleanPath := seedGCWAL(t, dir, clean, 2*time.Hour, gcCleanLines(t)...)
	dirtyPath := seedGCWAL(t, dir, dirty, 2*time.Hour,
		gcLine(t, core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 9, Model: "crashy"}))
	youngPath := seedGCWAL(t, dir, young, 0, gcCleanLines(t)...)

	dry, err := walfile.GC(dir, walfile.GCOptions{Retention: time.Hour, DryRun: true})
	if err != nil {
		t.Fatalf("GC(DryRun): %v", err)
	}
	if !dry.DryRun {
		t.Error("DryRun 报告应带 DryRun=true")
	}
	if dry.ScannedSessions != 3 || dry.Removed != 1 || dry.Skipped != 1 || dry.FreedBytes <= 0 {
		t.Fatalf("DryRun 报告 = %+v，want scanned=3 removed=1 skipped=1 freed>0（脏的算 Skipped，年轻的只算扫描）", dry)
	}
	// 磁盘一个都没动。
	for _, p := range []string{cleanPath, dirtyPath, youngPath} {
		if !gcExists(p) {
			t.Fatalf("DryRun 不该删任何文件: %s", p)
		}
	}

	real, err := walfile.GC(dir, walfile.GCOptions{Retention: time.Hour})
	if err != nil {
		t.Fatalf("GC(real): %v", err)
	}
	if dry.ScannedSessions != real.ScannedSessions || dry.Removed != real.Removed ||
		dry.Skipped != real.Skipped || dry.FreedBytes != real.FreedBytes {
		t.Fatalf("DryRun 与真跑数字必须一致：dry=%+v real=%+v（判据分叉 = 报告不可信）", dry, real)
	}
	if gcExists(cleanPath) {
		t.Error("真跑应删掉干净+超龄的 WAL")
	}
	if !gcExists(dirtyPath) || !gcExists(youngPath) {
		t.Error("脏的/年轻的 WAL 真跑也该留")
	}
}

// TestGCRequiresPositiveRetention：Retention <= 0 → **报错**（库层就拒：龄期是运维决定，
// 没有安全默认；缺省 0 不是"全删"也不是"不删"，是"你还没决定"）。报错时一个文件都不动。
func TestGCRequiresPositiveRetention(t *testing.T) {
	dir := t.TempDir()
	sid := newSession(t)
	path := seedGCWAL(t, dir, sid, 2*time.Hour, gcCleanLines(t)...)

	for _, tc := range []struct {
		name      string
		retention time.Duration
	}{
		{"零值（缺省）", 0},
		{"负值", -time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rep, err := walfile.GC(dir, walfile.GCOptions{Retention: tc.retention})
			if err == nil {
				t.Fatalf("Retention=%v 应被拒，got 报告 %+v", tc.retention, rep)
			}
			if !strings.Contains(err.Error(), "Retention") {
				t.Errorf("错误应点名 Retention，got: %v", err)
			}
			if !gcExists(path) {
				t.Error("报错时不该动任何文件")
			}
		})
	}
}

// TestGCRequiresDir：空目录串 → 报错（GC 没有可扫的对象，不静默"扫了 0 个"）。
func TestGCRequiresDir(t *testing.T) {
	if _, err := walfile.GC("   ", walfile.GCOptions{Retention: time.Hour}); err == nil {
		t.Fatal("空目录应被拒")
	}
}

// TestGCSkipsUndeletableAndKeepsGoing（删不掉不中止）：一个**删不掉**的 WAL 不影响其余
// 目标照删——单个失败跳过 + 计入 Skipped + 继续扫。
//
// 平台差异如实标注（blobfs gc_test 同款）：Windows 上"打开的文件不可删"（Go 的 os.Open
// 不带 FILE_SHARE_DELETE）→ 能构造；POSIX 上 unlink 打开的文件是允许的，而 WAL 同住一个
// 目录（目录只读会连别的文件一起挡住）→ 该平台退化为"不中止整轮 + 其余照删"断言，
// Skipped 路径由平台没能挡住时的条件分支钉住。
func TestGCSkipsUndeletableAndKeepsGoing(t *testing.T) {
	dir := t.TempDir()
	locked := newSession(t)
	other := newSession(t)
	lockedPath := seedGCWAL(t, dir, locked, 2*time.Hour, gcCleanLines(t)...)
	otherPath := seedGCWAL(t, dir, other, 2*time.Hour, gcCleanLines(t)...)

	// 锁住 locked（Windows：打开的文件不可删）。
	f, err := os.Open(lockedPath)
	if err != nil {
		t.Fatalf("打开 locked WAL: %v", err)
	}
	defer f.Close()

	rep, err := walfile.GC(dir, walfile.GCOptions{Retention: time.Hour})
	if err != nil {
		t.Fatalf("GC 不该因单个删除失败而整体报错: %v", err)
	}
	// 关键断言：另一个照删（一个删不掉不影响其余）。
	if gcExists(otherPath) {
		t.Error("其余干净超龄 WAL 应照删（单个删除失败不中止整轮）")
	}
	if rep.Removed != 1 {
		t.Errorf("Removed = %d，want 1（删得掉的那个计入）", rep.Removed)
	}
	if gcExists(lockedPath) {
		// 平台挡住了删除 → 如实计入 Skipped。
		t.Logf("平台挡住了删除（GOOS=%s）：locked 存活，Skipped=%d", runtime.GOOS, rep.Skipped)
		if rep.Skipped != 1 {
			t.Errorf("删不掉的 WAL 应计入 Skipped，got %d", rep.Skipped)
		}
	} else if rep.Skipped != 0 {
		t.Errorf("平台没能挡住删除时不该有跳过计数，got Skipped=%d", rep.Skipped)
	} else {
		t.Logf("平台没能挡住删除（GOOS=%s）：只断言了其余目标照删", runtime.GOOS)
	}
}
