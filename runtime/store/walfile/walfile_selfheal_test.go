package walfile

// Writer **自愈**的钉子（critic F2：POSIX 活 Writer 与 GC——unlink 后 fd 续写死 inode，
// 后续 WAL 静默丢）。
//
// # 场景
//
// GC 删掉一个**活 session** 的 WAL（龄期到了 + 判据命中）→ 那个 session 的 Writer 手里
// 还握着一个指向"死 inode"的 fd（POSIX：unlink 不影响已打开的 fd）→ 后续每一条 WAL 记录
// 都写进那个再也读不到的 inode → **写成功、不报错、但观察面静默消失**（比"没有观察面"
// 更坏：报告里"什么也没有"，使用者以为没发生过）。
//
// # 自愈口径
//
// 写路径每次 stat 一下自己的文件：没了就**重建新文件**再写 → "GC 删掉活 session 的 WAL"
// = **观察面重置**（不是静默丢数据；WAL 不是事实源，事件流照常完整回放）。
//
// # 平台差异（如实标注）
//
//   - **POSIX**：打开中的文件**可以**被 unlink → GC 删得掉 → 活 Writer 手里就是死 inode
//     → 本文件的主钉子；
//   - **Windows**：打开中的文件**不可删**（sharing violation；Go 的 os.OpenFile 不带
//     FILE_SHARE_DELETE）→ GC 删不掉（计 Skipped）→ 那个坑在 Windows 上**根本不存在**。
//     故本文件用两条路径覆盖两个平台：
//     ① 主钉子：先释放本进程持有的句柄再删（模拟"句柄已失效"的等效场景）→ 两个平台
//        都能跑到"文件没了 → 下一次写自愈重建"这条路径；
//     ② 端到端钉子：真跑一次 GC → POSIX 删得掉（随后自愈）、Windows 删不掉（计 Skipped，
//        文件与数据完好）。
//
// 变异自证：把 ensureFile 的自愈去掉（不 stat、不重建）→ 两条钉子都红。

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/core"
)

// walSelfHealStrPtr 取字符串指针（内部测试包自备；外部测试包另有一份，不共用）。
func walSelfHealStrPtr(s string) *string { return &s }

// walSelfHealOpen 造一个真实文件面的 Writer（newWriter：生产同款；自愈只对真实文件面生效）。
func walSelfHealOpen(t *testing.T, path string) *Writer {
	t.Helper()
	w, err := newWriter(path)
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	return w
}

// walSelfHealRecords 读回文件里的记录 kind 序（诊断用）。
func walSelfHealRecords(t *testing.T, path string) []string {
	t.Helper()
	scan, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var out []string
	for _, r := range scan.Records {
		out = append(out, r.Kind)
	}
	return out
}

// TestWriterAppendHealsDeletedFile（主钉子）：写一条 → **外部删文件** → 再写 →
// **新文件存在且含第二条**。
//
// 变异自证：去掉 ensureFile 的自愈 → 本用例红（删掉后那次 Append 要么写进死 inode
// （POSIX：文件读回来是空的/不存在），要么写在已关的句柄上直接报错（Windows 分支））。
func TestWriterAppendHealsDeletedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "selfheal.wal")
	w := walSelfHealOpen(t, path)
	// 收口：Writer 持有文件句柄（Windows 上不关 → TempDir 清理会失败）。
	t.Cleanup(func() { _ = w.Close() })

	// ① 第一条：轮次边界（顺带钉住 fsync 路径不受影响）。
	if err := w.Append(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}); err != nil {
		t.Fatalf("Append(begin): %v", err)
	}
	if got := walSelfHealRecords(t, path); len(got) != 1 || got[0] != core.KindWalTurnBegin {
		t.Fatalf("第一次写后文件内容 = %v，want [turn.begin]", got)
	}

	// ② 外部删文件（模拟 GC 删掉活 session 的 WAL）。
	//
	// Windows：打开中的文件不可删（sharing violation）→ 先释放本进程持有的句柄
	// （模拟"句柄已失效"的等效场景），再删。两个平台到这里的状态一致：**文件没了、
	// Writer 的句柄指向一个无效/死 inode**。
	if err := os.Remove(path); err != nil {
		_ = w.f.Close()
		if err2 := os.Remove(path); err2 != nil {
			t.Skipf("本平台删不掉打开中的 WAL 文件（GOOS=%s）：%v / %v", runtime.GOOS, err, err2)
		}
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("文件应已被删（stat err=%v）", err)
	}

	// ③ 再写：自愈 → 重建文件 → 记录落盘。
	if err := w.Append(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 1, Text: walSelfHealStrPtr("自愈后写的")}); err != nil {
		t.Fatalf("自愈后的 Append 应成功: %v", err)
	}

	// ④ 断言：**新文件存在**且**含第二条**（这是"没静默丢"的正面证据）。
	scan, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !scan.Exists {
		t.Fatal("自愈后 WAL 文件应重新存在（观察面重置 ≠ 观察面消失）")
	}
	if len(scan.Records) != 1 || scan.Records[0].Kind != core.KindWalTurnDelta {
		t.Fatalf("新文件内容 = %v，want 只有第二条 turn.delta（观察面重置：新文件从头记）",
			walSelfHealRecords(t, path))
	}
}

// TestWriterAppendAfterGCDeletesLiveFile（端到端钉子）：**真跑一次 GC** 删掉活 session 的
// WAL → 随后的写仍然落盘（自愈）。
//
// 平台差异如实标注：
//   - POSIX：GC 删得掉（活 session 的 WAL 也满足判据：干净 + 超龄）→ 删掉后下一次写
//     **重建新文件**（本用例的核心断言）；
//   - Windows：打开中的文件删不掉 → GC 如实计 **Skipped**、文件与数据完好（那条坑在
//     Windows 上不存在）。
func TestWriterAppendAfterGCDeletesLiveFile(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// 收口：Writer 持有文件句柄（Windows 上不关 → TempDir 清理会失败）。
	t.Cleanup(func() { _ = st.Close() })
	sid := core.NewUlid()
	w, err := st.Writer(sid)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	path := w.Path()

	// 干净收口的一条 WAL（begin + end）+ 钉老 mtime（超龄靶子）。
	if err := w.Append(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}); err != nil {
		t.Fatalf("Append(begin): %v", err)
	}
	if err := w.Append(core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}); err != nil {
		t.Fatalf("Append(end): %v", err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	rep, err := GC(dir, GCOptions{Retention: time.Hour})
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if rep.Removed != 1 || rep.Skipped != 0 {
		// Windows：活文件删不掉 → 计入 Skipped（那条坑在 Windows 上不存在）。
		if runtime.GOOS == "windows" && rep.Removed == 0 && rep.Skipped == 1 {
			t.Logf("Windows：活 session 的 WAL 删不掉（计 Skipped），数据完好")
			if err := w.Append(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 2}); err != nil {
				t.Fatalf("Append: %v", err)
			}
			return
		}
		t.Fatalf("GC 报告 = %+v，want removed=1 skipped=0（GOOS=%s）", rep, runtime.GOOS)
	}

	// POSIX：文件已被删 → 下一次写自愈重建（新文件存在且含新记录）。
	if err := w.Append(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 2}); err != nil {
		t.Fatalf("GC 删掉活 session 的 WAL 后，Append 应自愈成功: %v", err)
	}
	scan, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !scan.Exists {
		t.Fatal("自愈后 WAL 文件应重新存在")
	}
	if len(scan.Records) != 1 || scan.Records[0].Kind != core.KindWalTurnDelta {
		t.Fatalf("新文件内容 = %v，want 只有新写的 turn.delta", walSelfHealRecords(t, path))
	}
}
