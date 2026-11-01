package blobfs_test

// 缺口 4 后半的钉子：blobfs.GC 三扫（.tmp 残留 → 孤儿目录 → 可选 mtime 清扫）。
//
// 口径（与 gc.go 文件头一致）：
//   - Grace 保护在途写（年轻 .tmp / 年轻孤儿不动；默认 1h；Grace ≤ 0 = 默认 1h）；
//   - Retention=0（默认）只做安全两扫——**不碰**有 meta 的完整 blob；
//   - 龄期 = **最后外置时间**（幂等 Put 刷新 meta.json 的 mtime）；
//   - 非法 ULID 名的目录/文件、外来文件一律不动；
//   - DryRun 一个不删但报告如实——**与真跑数字完全一致**（同一套判据）。
//
// 变异自证：
//   - GC 删掉孤儿扫 → 孤儿钉子红；
//   - GC 忽略 Grace（年轻也删）→ 在途保护钉子红；
//   - Retention=0 误删完整 blob → 安全两扫钉子红；
//   - **扫 2 的锁内复核改回"实时目录 mtime"** → 含 .tmp 孤儿漏删 + DryRun/真跑不一致红；
//   - **幂等 Put 不触碰 meta mtime** → 龄期刷新钉子红；
//   - **defaultGCGrace 改 0** → 默认宽限钉子红；
//   - **扫 1 不持 id 锁** → 并发 Put 钉子红（GC 会在 Put 在途时删掉 .tmp/目录）；
//   - **扫 2 不持 id 锁** → gc_lock_test.go 的 TestGCSweep2DeleteBlocksOnIDLock 红
//     （并发 Put 钉子钉不住它：在途 .tmp 证据独立保住了那段窗口）。

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/runtime/store/blobfs"
)

// chtimes 把 path 的 atime/mtime 钉到 t（造"老残留"；Windows 也支持目录 mtime）。
func chtimes(t *testing.T, path string, when time.Time) {
	t.Helper()
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatalf("Chtimes %s: %v", path, err)
	}
}

// seedOrphan 造一个**孤儿目录**（合法 ULID 名 + 有 content 无 meta.json；可选钉老 mtime）。
func seedOrphan(t *testing.T, root string, old bool) contracts.Ulid {
	t.Helper()
	id := core.NewUlid()
	dir := filepath.Join(root, string(id))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("建孤儿目录: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "content"), []byte("orphan-bytes"), 0o644); err != nil {
		t.Fatalf("写孤儿 content: %v", err)
	}
	if old {
		chtimes(t, dir, time.Now().Add(-2*time.Hour))
	}
	return id
}

// seedTmpIn 在 dir 下造一个 .content-*.tmp（CreateTemp 实际模式；可选钉老 mtime）。
func seedTmpIn(t *testing.T, dir, name string, old bool) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte("tmp-junk"), 0o644); err != nil {
		t.Fatalf("写 .tmp: %v", err)
	}
	if old {
		chtimes(t, p, time.Now().Add(-2*time.Hour))
	}
	return p
}

// seedCompleteBlob Put 一个完整 blob（可选把 meta.json 的 mtime 钉老——mtime 清扫的靶子）。
func seedCompleteBlob(t *testing.T, st *blobfs.Store, content string, oldMeta bool) contracts.Ulid {
	t.Helper()
	meta, err := st.Put(context.Background(), storage.BlobMeta{MediaType: "application/json"},
		bytes.NewReader([]byte(content)))
	if err != nil {
		t.Fatalf("Put 种子 blob: %v", err)
	}
	if oldMeta {
		chtimes(t, filepath.Join(st.Root(), string(meta.ID), "meta.json"), time.Now().Add(-48*time.Hour))
	}
	return meta.ID
}

// gcResidue 造齐测试共用的残留现场，返回各靶子的路径/id：
//
//	孤儿（老）orphanOld / 孤儿（年轻）orphanYoung / .tmp（老）in completeOld 目录 /
//	.complete 之外的外来文件 foreignFile / 非法 ULID 目录 foreignDir /
//	完整 blob（老 meta）completeOld / 完整 blob（年轻）completeYoung
type gcResidue struct {
	orphanOld     contracts.Ulid
	orphanYoung   contracts.Ulid
	completeOld   contracts.Ulid
	completeYoung contracts.Ulid
	foreignFile   string
	foreignDir    string
	oldTmp        string
}

func seedGCResidue(t *testing.T, st *blobfs.Store) gcResidue {
	t.Helper()
	r := gcResidue{}
	r.orphanOld = seedOrphan(t, st.Root(), true)
	r.orphanYoung = seedOrphan(t, st.Root(), false)
	r.completeOld = seedCompleteBlob(t, st, `{"old":true}`, true)
	r.completeYoung = seedCompleteBlob(t, st, `{"young":true}`, false)
	// 老 .tmp 落在**完整** blob 目录里（真实 CreateTemp 的位置）——扫 1 要能从有 meta
	// 的目录里清掉它，且不把整个目录删了。
	r.oldTmp = seedTmpIn(t, filepath.Join(st.Root(), string(r.completeOld)), ".content-123456789.tmp", true)
	// 外来文件 / 非法 ULID 目录（一律不动）。
	r.foreignFile = filepath.Join(st.Root(), "notes.txt")
	if err := os.WriteFile(r.foreignFile, []byte("别动我"), 0o644); err != nil {
		t.Fatal(err)
	}
	chtimes(t, r.foreignFile, time.Now().Add(-100*time.Hour)) // 老也没用：不是我们的
	r.foreignDir = filepath.Join(st.Root(), "backup-dir")
	if err := os.MkdirAll(r.foreignDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.foreignDir, "x"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	chtimes(t, r.foreignDir, time.Now().Add(-100*time.Hour))
	return r
}

// exists 报告路径是否存在。
func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// TestGCThreeSweeps：三扫各司其职——老 .tmp 删、老孤儿删、老完整 blob 只在
// Retention>0 时删；年轻残留、外来文件、非法 ULID 目录全留。
func TestGCThreeSweeps(t *testing.T) {
	st := newStore(t)
	r := seedGCResidue(t, st)
	oldTmp := r.oldTmp

	// Retention=0（默认口径）：只安全两扫——完整 blob（无论多老）都留。
	rep, err := st.GC(context.Background(), blobfs.GCOptions{Grace: time.Hour})
	if err != nil {
		t.Fatalf("GC(Retention=0): %v", err)
	}
	if rep.DryRun {
		t.Error("DryRun 应为 false")
	}
	if rep.RemovedTmp != 1 {
		t.Errorf("RemovedTmp = %d，want 1（老 .content-*.tmp）", rep.RemovedTmp)
	}
	if rep.RemovedOrphans != 1 {
		t.Errorf("RemovedOrphans = %d，want 1（老孤儿）", rep.RemovedOrphans)
	}
	if rep.RemovedAged != 0 {
		t.Errorf("RemovedAged = %d，want 0（Retention=0 不清扫完整 blob）", rep.RemovedAged)
	}
	if rep.ScannedBlobs != 2 {
		t.Errorf("ScannedBlobs = %d，want 2（两个完整 blob 被扫到）", rep.ScannedBlobs)
	}
	if rep.FreedBytes <= 0 {
		t.Errorf("FreedBytes = %d，应 > 0（真删了东西）", rep.FreedBytes)
	}
	if exists(oldTmp) {
		t.Error("老 .tmp 应被删")
	}
	if exists(filepath.Join(st.Root(), string(r.orphanOld))) {
		t.Error("老孤儿目录应被整目录删")
	}
	if !exists(filepath.Join(st.Root(), string(r.orphanYoung))) {
		t.Error("年轻孤儿（可能是在途写）不该被删（Grace 保护）")
	}
	if !exists(filepath.Join(st.Root(), string(r.completeOld))) || !exists(filepath.Join(st.Root(), string(r.completeYoung))) {
		t.Error("Retention=0 时完整 blob 一个都不该删（只安全两扫）")
	}
	if !exists(r.foreignFile) || !exists(r.foreignDir) {
		t.Error("外来文件/非法 ULID 目录一律不动")
	}

	// Retention>0：老 meta 的完整 blob 进入清扫（年轻完整 blob 仍留）。
	rep2, err := st.GC(context.Background(), blobfs.GCOptions{Grace: time.Hour, Retention: 24 * time.Hour})
	if err != nil {
		t.Fatalf("GC(Retention=24h): %v", err)
	}
	if rep2.RemovedAged != 1 {
		t.Errorf("RemovedAged = %d，want 1（meta 老于 24h 的完整 blob）", rep2.RemovedAged)
	}
	if exists(filepath.Join(st.Root(), string(r.completeOld))) {
		t.Error("超期完整 blob 应被删")
	}
	if !exists(filepath.Join(st.Root(), string(r.completeYoung))) {
		t.Error("年轻完整 blob 不该被删")
	}
	// 年轻孤儿仍在（Grace 内），第二次扫仍不动它；外来物仍在。
	if !exists(filepath.Join(st.Root(), string(r.orphanYoung))) {
		t.Error("年轻孤儿第二次 GC（Grace=1h）仍不该被删")
	}
	if !exists(r.foreignFile) || !exists(r.foreignDir) {
		t.Error("外来文件/非法 ULID 目录第二次 GC 仍不动")
	}
}

// TestGCGraceProtectsInFlight：Grace 保护在途写——**年轻** .tmp 与年轻孤儿一个不删
// （哪怕 Retention 拉满）；同一批里老的照删。
func TestGCGraceProtectsInFlight(t *testing.T) {
	st := newStore(t)
	// 年轻孤儿 + 其目录里一个年轻 .tmp（= 刚 CreateTemp、还没落 meta 的在途 Put）。
	youngOrphan := seedOrphan(t, st.Root(), false)
	youngTmp := seedTmpIn(t, filepath.Join(st.Root(), string(youngOrphan)), ".content-777.tmp", false)
	// 老孤儿对照（证明扫描确实在干活，不是"什么都删不掉"）。
	oldOrphan := seedOrphan(t, st.Root(), true)

	rep, err := st.GC(context.Background(), blobfs.GCOptions{Grace: time.Hour, Retention: time.Nanosecond})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if rep.RemovedTmp != 0 {
		t.Errorf("RemovedTmp = %d，want 0（年轻 .tmp 是在途写，Grace 保护）", rep.RemovedTmp)
	}
	if rep.RemovedOrphans != 1 {
		t.Errorf("RemovedOrphans = %d，want 1（只有老孤儿）", rep.RemovedOrphans)
	}
	if !exists(youngTmp) {
		t.Error("年轻 .tmp 不该被删（在途写保护——变异'忽略 Grace'在此红）")
	}
	if !exists(filepath.Join(st.Root(), string(youngOrphan))) {
		t.Error("年轻孤儿目录不该被删（在途写保护）")
	}
	if exists(filepath.Join(st.Root(), string(oldOrphan))) {
		t.Error("老孤儿该被删（否则 Grace 钉子是空的：什么都删不掉也能过）")
	}
}

// TestGCDryRunReportsButDeletesNothing：DryRun 一个不删，但报告如实（会删的都数出来）。
func TestGCDryRunReportsButDeletesNothing(t *testing.T) {
	st := newStore(t)
	r := seedGCResidue(t, st)

	rep, err := st.GC(context.Background(), blobfs.GCOptions{Grace: time.Hour, Retention: 24 * time.Hour, DryRun: true})
	if err != nil {
		t.Fatalf("GC(DryRun): %v", err)
	}
	if !rep.DryRun {
		t.Error("DryRun 报告应带 DryRun=true")
	}
	if rep.RemovedTmp != 1 || rep.RemovedOrphans != 1 || rep.RemovedAged != 1 {
		t.Fatalf("DryRun 报告应如实数出会删的：tmp=%d orphan=%d aged=%d，want 1/1/1",
			rep.RemovedTmp, rep.RemovedOrphans, rep.RemovedAged)
	}
	if rep.FreedBytes <= 0 {
		t.Errorf("DryRun FreedBytes = %d，应 > 0（如实算会释放多少）", rep.FreedBytes)
	}
	// 磁盘一个都没动。
	if !exists(r.oldTmp) {
		t.Error("DryRun 不该删 .tmp")
	}
	if !exists(filepath.Join(st.Root(), string(r.orphanOld))) {
		t.Error("DryRun 不该删孤儿目录")
	}
	if !exists(filepath.Join(st.Root(), string(r.completeOld))) {
		t.Error("DryRun 不该删完整 blob")
	}
	if !exists(r.foreignFile) || !exists(r.foreignDir) {
		t.Error("DryRun 更不该动外来物")
	}
	// DryRun 与真跑的计数一致（同一现场先 DryRun 再真跑 → 报告应同形）。
	rep2, err := st.GC(context.Background(), blobfs.GCOptions{Grace: time.Hour, Retention: 24 * time.Hour})
	if err != nil {
		t.Fatalf("GC(real): %v", err)
	}
	if rep.RemovedTmp != rep2.RemovedTmp || rep.RemovedOrphans != rep2.RemovedOrphans || rep.RemovedAged != rep2.RemovedAged {
		t.Fatalf("DryRun 与真跑计数应一致：dry=(%d,%d,%d) real=(%d,%d,%d)",
			rep.RemovedTmp, rep.RemovedOrphans, rep.RemovedAged,
			rep2.RemovedTmp, rep2.RemovedOrphans, rep2.RemovedAged)
	}
}

// TestGCTmpPatternsOnly：扫 1 **只认** .content-*.tmp / .meta-*.tmp 两种模式——
// 目录里别的点文件/普通文件不动（.tmp 之外的文件归目录级处置：有 meta 的完整 blob
// 里除 meta.json/content 外的文件也**不**被扫 1 顺手删掉）。
func TestGCTmpPatternsOnly(t *testing.T) {
	st := newStore(t)
	id := seedCompleteBlob(t, st, `{"keep":"me"}`, false)
	dir := filepath.Join(st.Root(), string(id))

	old := time.Now().Add(-2 * time.Hour)
	// 两种模式的老 .tmp → 删。
	tmpContent := seedTmpIn(t, dir, ".content-abc.tmp", true)
	tmpMeta := seedTmpIn(t, dir, ".meta-abc.tmp", true)
	// 别的文件（哪怕老、哪怕长得像临时文件）→ 不动。
	other := filepath.Join(dir, ".content-abc.tmp.bak")
	if err := os.WriteFile(other, []byte("bak"), 0o644); err != nil {
		t.Fatal(err)
	}
	chtimes(t, other, old)
	stray := filepath.Join(dir, "stray.txt")
	if err := os.WriteFile(stray, []byte("stray"), 0o644); err != nil {
		t.Fatal(err)
	}
	chtimes(t, stray, old)

	rep, err := st.GC(context.Background(), blobfs.GCOptions{Grace: time.Hour})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if rep.RemovedTmp != 2 {
		t.Errorf("RemovedTmp = %d，want 2（只认两种模式）", rep.RemovedTmp)
	}
	if exists(tmpContent) || exists(tmpMeta) {
		t.Error("两种 CreateTemp 模式的老 .tmp 应被删")
	}
	if !exists(other) || !exists(stray) {
		t.Error("模式之外的文件一律不动（.bak / stray.txt）")
	}
	// 完整 blob 本身也还在（扫 1 只删 .tmp，不碰有 meta 的目录）。
	if _, err := st.Stat(context.Background(), id); err != nil {
		t.Fatalf("完整 blob 应还在: %v", err)
	}
}

// TestGCErrorReturnsPartialReport：根目录不存在 → 已累计的报告 + error（不吞报告）。
func TestGCErrorReturnsPartialReport(t *testing.T) {
	st := newStore(t)
	// 把 root 挪走（模拟仓库被外部清掉）→ GC 列目录失败。
	// Store.root 是私有的；用一个指向不存在目录的 store：New 会建目录，故直接删掉它。
	if err := os.RemoveAll(st.Root()); err != nil {
		t.Fatalf("移除 root: %v", err)
	}
	rep, err := st.GC(context.Background(), blobfs.GCOptions{})
	if err == nil {
		t.Fatal("root 不存在应报错")
	}
	// 部分报告如实返回（零值计数 + DryRun 回显），不是"成功扫了 0 个"的假成功。
	if rep.RemovedTmp != 0 || rep.RemovedOrphans != 0 || rep.RemovedAged != 0 {
		t.Fatalf("失败时报告应是已累计的零值，got %+v", rep)
	}
	if !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "GC") {
		t.Fatalf("错误应点名 GC 场景，got %v", err)
	}
}

// ---------------------------------------------------------------------------
// F1：扫 2 的锁内复核必须用**内容证据**（不用目录 mtime）+ DryRun/真跑数字一致
// ---------------------------------------------------------------------------

// seedOrphanWithOldTmp 造 F1 的原场景现场：一个**孤儿目录**（有 content 无 meta.json）里
// 还留着**老**的 .content-*.tmp（崩溃残留：写了一半的临时文件）。
//
// 关键：目录 mtime 钉老（残留很老），但**扫 1 删掉 .tmp 会刷新这个目录的 mtime**——
// 复核若用实时目录 mtime 判"在途写"，这个孤儿就会因为"刚被刷新成年轻"而活下来。
func seedOrphanWithOldTmp(t *testing.T, root string) (contracts.Ulid, string) {
	t.Helper()
	id := core.NewUlid()
	dir := filepath.Join(root, string(id))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("建孤儿目录: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "content"), []byte("orphan-bytes"), 0o644); err != nil {
		t.Fatalf("写孤儿 content: %v", err)
	}
	tmp := seedTmpIn(t, dir, ".content-135792468.tmp", true)
	// 目录 mtime 钉老（**在**造完 .tmp 之后钉——造文件会刷新目录 mtime）。
	chtimes(t, dir, time.Now().Add(-2*time.Hour))
	return id, tmp
}

// TestGCOrphanWithOldTmpRemovedAndDryRunMatches（F1 主钉子）：含**老 .tmp** 的孤儿目录
// 真跑后消失；且 **DryRun 与真跑的 RemovedOrphans / FreedBytes 完全相等**。
//
// 为什么这是 F1 的核心：扫 1 删 .tmp → **刷新所在目录的 mtime** → 若扫 2 的锁内复核用
// 实时目录 mtime 判"在途写"，这个孤儿会被判成"年轻"→ 漏删；且 DryRun（不删 → mtime 不变
// → 照删）与真跑（mtime 被刷新 → 不删）**数字对不上**。
//
// 变异自证：复核改回实时目录 mtime → 本钉子红（目录没消失 + 两种模式数字不等）。
func TestGCOrphanWithOldTmpRemovedAndDryRunMatches(t *testing.T) {
	st := newStore(t)
	id, tmpPath := seedOrphanWithOldTmp(t, st.Root())
	dir := filepath.Join(st.Root(), string(id))

	// DryRun：如实算"会删什么"（磁盘不动）。
	dry, err := st.GC(context.Background(), blobfs.GCOptions{Grace: time.Hour, DryRun: true})
	if err != nil {
		t.Fatalf("GC(DryRun): %v", err)
	}
	if dry.RemovedOrphans != 1 {
		t.Errorf("DryRun RemovedOrphans = %d，want 1（含老 .tmp 的孤儿应被算进去）", dry.RemovedOrphans)
	}
	if dry.RemovedTmp != 1 {
		t.Errorf("DryRun RemovedTmp = %d，want 1（老 .content-*.tmp）", dry.RemovedTmp)
	}
	if !exists(dir) {
		t.Error("DryRun 不该删目录")
	}

	// 真跑（同一现场）：**数字必须与 DryRun 完全相等**，且目录消失。
	real, err := st.GC(context.Background(), blobfs.GCOptions{Grace: time.Hour})
	if err != nil {
		t.Fatalf("GC(real): %v", err)
	}
	if real.RemovedOrphans != dry.RemovedOrphans {
		t.Errorf("DryRun/真跑 RemovedOrphans 应相等：dry=%d real=%d（复核口径不一致 = 漏删）",
			dry.RemovedOrphans, real.RemovedOrphans)
	}
	if real.RemovedTmp != dry.RemovedTmp {
		t.Errorf("DryRun/真跑 RemovedTmp 应相等：dry=%d real=%d", dry.RemovedTmp, real.RemovedTmp)
	}
	if real.FreedBytes != dry.FreedBytes {
		t.Errorf("DryRun/真跑 FreedBytes 应相等：dry=%d real=%d", dry.FreedBytes, real.FreedBytes)
	}
	if exists(dir) {
		t.Error("含老 .tmp 的孤儿目录真跑后应消失（F1：扫 1 删 .tmp 刷新目录 mtime 不该让它活下来）")
	}
	if exists(tmpPath) {
		t.Error("老 .content-*.tmp 应被删")
	}
}

// slowReader 是"慢 reader"：第一次 Read 阻塞到 release 被关（模拟大内容慢写 = 在途写窗口）。
type slowReader struct {
	release <-chan struct{}
	data    []byte
	off     int
	once    sync.Once
}

func (r *slowReader) Read(p []byte) (int, error) {
	r.once.Do(func() { <-r.release })
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}

// waitPathExist 等 path 出现（最多 ~2s）；用于"等在途 Put 把目录建出来"。
func waitPathExist(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("等 %s 出现超时", path)
}

// TestGCConcurrentPutSurvives（F1 并发钉子）：同进程 GC 与**并发 Put** 竞争 → blob 存活。
//
// 机制：Put 持 id 分片锁写 .tmp → GC 要删同一个 id 时**阻塞在锁上** → Put 写完（meta.json
// 落位）→ GC 拿到锁复核**见 meta.json** → 不删。
//
// 为什么用 Grace=1ns（极小）：把"目录年轻"这道防线**关掉**——只剩 id 锁 + 在途证据能救。
// 变异"扫 1 不持 id 锁"时 GC 会在 Put 在途时删掉 .tmp → Put 的 writeContent/writeMeta
// 失败 → Put 返错 → 本钉子红。（注意：本钉子钉住的是**扫 1 的锁 + 在途 .tmp 证据**——
// 扫 2 的锁护的是"content 已落、meta 未落"的另一段窗口，由 gc_lock_test.go 单独钉。）
func TestGCConcurrentPutSurvives(t *testing.T) {
	st := newStore(t)
	id := core.NewUlid()
	release := make(chan struct{})
	putErr := make(chan error, 1)
	go func() {
		_, err := st.Put(context.Background(),
			storage.BlobMeta{ID: id, MediaType: "application/json"},
			&slowReader{release: release, data: bytes.Repeat([]byte("x"), 8192)})
		putErr <- err
	}()

	// 等在途 Put 把目录建出来（= .content-*.tmp 已落盘 → 正在写）。
	dir := filepath.Join(st.Root(), string(id))
	waitPathExist(t, dir)

	gcDone := make(chan blobfs.GCReport, 1)
	go func() {
		rep, err := st.GC(context.Background(), blobfs.GCOptions{Grace: time.Nanosecond})
		if err != nil {
			t.Errorf("GC: %v", err)
		}
		gcDone <- rep
	}()

	// 放行慢 Put，等它写完。
	close(release)
	if err := <-putErr; err != nil {
		t.Fatalf("并发 Put 应成功（GC 不该打断在途写）: %v", err)
	}
	rep := <-gcDone

	if !exists(dir) {
		t.Error("并发 Put 落地的 blob 目录不该被 GC 删掉（锁内复核见 meta.json → 不删）")
	}
	if _, err := st.Stat(context.Background(), id); err != nil {
		t.Errorf("并发 Put 的 blob 应仍可 Stat: %v", err)
	}
	if rep.RemovedOrphans != 0 {
		t.Errorf("RemovedOrphans = %d，want 0（在途写目录不是孤儿）", rep.RemovedOrphans)
	}
	if rep.RemovedTmp != 0 {
		t.Errorf("RemovedTmp = %d，want 0（Put 写完 .tmp 已 rename 走）", rep.RemovedTmp)
	}
}

// ---------------------------------------------------------------------------
// F3：龄期 = 最后外置时间（幂等 Put 刷新 meta mtime）
// ---------------------------------------------------------------------------

// TestGCIdempotentPutRefreshesAge（F3 钉子）：老 mtime 的 blob 被**幂等 Put**（同 id 同内容）
// → meta.json 的 mtime 被刷新 → GC 的 Retention 清扫**不删**它。
//
// 为什么：内容寻址 id 下"重复引用同一份内容"是常态（重放/再次引用）。不刷新 mtime 的话，
// 仍在被引用的 blob 会被当成超期残留删掉（读侧只能报"内容已回收"）。
//
// 变异自证：幂等 Put 不触碰 mtime → 下面的"mtime 被刷新"与"不被删"两条断言都红。
func TestGCIdempotentPutRefreshesAge(t *testing.T) {
	st := newStore(t)
	content := []byte(`{"payload":"same-content"}`)
	meta, err := st.Put(context.Background(),
		storage.BlobMeta{MediaType: "application/json"}, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	metaPath := filepath.Join(st.Root(), string(meta.ID), "meta.json")
	chtimes(t, metaPath, time.Now().Add(-48*time.Hour))

	// 幂等 Put：同 id、同内容 → 原样返回已有元数据（不写第二份），但**刷新 mtime**。
	again, err := st.Put(context.Background(),
		storage.BlobMeta{ID: meta.ID, MediaType: "application/json"}, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("幂等 Put: %v", err)
	}
	if again.ID != meta.ID || again.Size != meta.Size {
		t.Fatalf("幂等 Put 应原样返回已有元数据：got %+v want %+v", again, meta)
	}
	fi, err := os.Stat(metaPath)
	if err != nil {
		t.Fatalf("stat meta.json: %v", err)
	}
	if time.Since(fi.ModTime()) > time.Minute {
		t.Errorf("幂等 Put 应刷新 meta.json 的 mtime（最后外置时间），got %v（仍是老时间）", fi.ModTime())
	}

	// 龄期刷新 → Retention 清扫不该删它（对照组：没刷新时这个 mtime 老于 24h = 会被删）。
	rep, err := st.GC(context.Background(), blobfs.GCOptions{Grace: time.Hour, Retention: 24 * time.Hour})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if rep.RemovedAged != 0 {
		t.Errorf("RemovedAged = %d，want 0（幂等引用刷新了龄期 → 不该被清扫）", rep.RemovedAged)
	}
	if !exists(filepath.Join(st.Root(), string(meta.ID))) {
		t.Error("刚被幂等引用过的 blob 不该被 Retention 清扫删掉")
	}
}

// ---------------------------------------------------------------------------
// F5①：Grace ≤ 0 → 默认 1h（零值不是"立刻全删"）
// ---------------------------------------------------------------------------

// TestGCDefaultGraceProtectsYoungResidue（F5① 钉子）：GCOptions 的 Grace 零值/负值 →
// 走**默认 1h** → 年轻残留（年轻 .tmp / 年轻孤儿）一律不动。
//
// 变异自证：defaultGCGrace 改 0 → 本钉子红（年轻残留被删）。
func TestGCDefaultGraceProtectsYoungResidue(t *testing.T) {
	for _, tc := range []struct {
		name  string
		grace time.Duration
	}{
		{"零值", 0},
		{"负值", -time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := newStore(t)
			youngOrphan := seedOrphan(t, st.Root(), false)
			youngTmp := seedTmpIn(t, filepath.Join(st.Root(), string(youngOrphan)), ".content-777.tmp", false)
			// 把"年轻"钉成**可测的 1s 前**（不是"此刻"）：默认 1h 下照留（1s ≪ 1h），
			// 但若默认宽限被改成 0，"1s 前"就是**确定超期**（now-mod = 1s > 0）→ 钉子红。
			// （用"此刻"会踩同一时钟刻的坑：now-mod 可能恰好 = 0 → 变异侥幸存活。）
			young := time.Now().Add(-time.Second)
			chtimes(t, youngTmp, young)
			chtimes(t, filepath.Join(st.Root(), string(youngOrphan)), young)

			rep, err := st.GC(context.Background(), blobfs.GCOptions{Grace: tc.grace})
			if err != nil {
				t.Fatalf("GC: %v", err)
			}
			if rep.RemovedTmp != 0 {
				t.Errorf("RemovedTmp = %d，want 0（Grace ≤ 0 = 默认 1h → 年轻 .tmp 是在途写）", rep.RemovedTmp)
			}
			if rep.RemovedOrphans != 0 {
				t.Errorf("RemovedOrphans = %d，want 0（Grace ≤ 0 = 默认 1h → 年轻孤儿不动）", rep.RemovedOrphans)
			}
			if !exists(youngTmp) {
				t.Error("年轻 .tmp 不该被删（默认宽限 1h）")
			}
			if !exists(filepath.Join(st.Root(), string(youngOrphan))) {
				t.Error("年轻孤儿目录不该被删（默认宽限 1h）")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// F2：单个删除失败不该中止整轮（跳过 + 计入 Skipped + 继续扫）
// ---------------------------------------------------------------------------

// TestGCSkipsUndeletableDirAndKeepsGoing（F2 钉子）：一个**删不掉**的目录（Windows：文件被
// 打开 → sharing violation；POSIX：目录不可写）**不影响其余目标照删**——运维工具不该一撞
// 就半途而废。
//
// 平台差异如实标注：Windows 上"被打开的文件不可删"（Go 的 os.Open 不带 FILE_SHARE_DELETE）
// → 能构造；POSIX 上 unlink 打开的文件是允许的 → 改用"目录只读"挡删除（**root 身份下挡不住**
// → 那时只断言"其余目标照删"这半条）。
func TestGCSkipsUndeletableDirAndKeepsGoing(t *testing.T) {
	st := newStore(t)
	locked := seedOrphan(t, st.Root(), true)
	other := seedOrphan(t, st.Root(), true)
	lockedDir := filepath.Join(st.Root(), string(locked))
	otherDir := filepath.Join(st.Root(), string(other))

	// 锁住 locked 里的文件（Windows：打开的文件不可删）。
	f, err := os.Open(filepath.Join(lockedDir, "content"))
	if err != nil {
		t.Fatalf("打开 locked 目录里的文件: %v", err)
	}
	defer f.Close()
	if runtime.GOOS != "windows" {
		// POSIX：unlink 打开的文件是允许的 → 用"目录不可写"挡删除。
		if err := os.Chmod(lockedDir, 0o500); err != nil {
			t.Fatalf("chmod locked 目录: %v", err)
		}
		defer func() { _ = os.Chmod(lockedDir, 0o700) }()
	}

	rep, err := st.GC(context.Background(), blobfs.GCOptions{Grace: time.Hour})
	if err != nil {
		t.Fatalf("GC 不该因单个删除失败而整体报错: %v", err)
	}
	// 关键断言：另一个孤儿**照删**（一个删不掉不影响其余）。
	if exists(otherDir) {
		t.Error("其余孤儿目录应照删（单个删除失败不中止整轮）")
	}
	if rep.RemovedOrphans != 1 {
		t.Errorf("RemovedOrphans = %d，want 1（删得掉的那个孤儿计入）", rep.RemovedOrphans)
	}
	if exists(lockedDir) {
		// 删不掉 → 如实计入 Skipped。
		t.Logf("平台挡住了删除（GOOS=%s）：locked 目录存活，Skipped=%d", runtime.GOOS, rep.Skipped)
		if rep.Skipped < 1 {
			t.Errorf("删不掉的目录应计入 Skipped，got %d", rep.Skipped)
		}
	} else if rep.Skipped != 0 {
		t.Errorf("平台没能挡住删除时不该有跳过计数，got Skipped=%d", rep.Skipped)
	} else {
		t.Logf("平台没能挡住删除（GOOS=%s，多半是 root 身份）：只断言了其余目标照删", runtime.GOOS)
	}
}
