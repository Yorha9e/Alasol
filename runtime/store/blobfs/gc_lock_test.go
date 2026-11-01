package blobfs

// 扫 2（孤儿目录）删除的 **id 锁钉子**（缺口 4 复查轮 F1 残留）。
//
// 为什么要单独钉：gc_test.go 的并发 Put 钉子（TestGCConcurrentPutSurvives）实际钉住的
// 是**扫 1 的锁 + 在途 .tmp 证据**（慢 Put 期间 .content-*.tmp 在场 → orphanStillOrphan
// 判在途不删）——把扫 2 的锁去掉它照样绿。而扫 2 的锁护的是**另一段窗口**：Put 的落盘序
// "content 已 rename、meta.json 还没落"之间（此刻目录形态恰是"孤儿"：有 content 无 meta
// 无 .tmp），删除必须与 Put 互斥，否则会把在途写的目录从脚下抽走。
//
// 钉法（确定性，无时序侥幸）：**手动持 id 锁**模拟同 id 的 Put 在途 → GC 必须阻塞在
// 扫 2 的锁上（不许完成、孤儿不许被删）→ 解锁 → GC 完成并删除。
//
// 变异自证：去掉 gc.go 扫 2 的 idLock（l := s.idLock(...) / l.Lock() / l.Unlock()）
// → GC 在锁被持有时就完成 → 第一处断言红（此前该变异全绿 = 本文件的由来）。

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/core"
)

// TestGCSweep2DeleteBlocksOnIDLock：扫 2 的删除持 id 锁——锁被持有时 GC 不许完成、
// 不许删；解锁后 GC 完成并删掉孤儿。
func TestGCSweep2DeleteBlocksOnIDLock(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 造"content 已落、meta 未落"形态的孤儿：有 content、无 meta.json、无 .tmp、很老。
	id := core.NewUlid()
	dir := filepath.Join(st.Root(), string(id))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("建孤儿目录: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "content"), []byte("in-flight"), 0o644); err != nil {
		t.Fatalf("写 content: %v", err)
	}
	old := time.Now().Add(-24 * time.Hour)
	if err := os.Chtimes(dir, old, old); err != nil {
		t.Fatalf("Chtimes dir: %v", err)
	}
	if err := os.Chtimes(filepath.Join(dir, "content"), old, old); err != nil {
		t.Fatalf("Chtimes content: %v", err)
	}

	// 模拟同 id 的 Put 在途（持锁写落盘序的第 5→6 步之间）。
	l := st.idLock(id)
	l.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, gerr := st.GC(context.Background(), GCOptions{Grace: time.Hour}); gerr != nil {
			t.Errorf("GC: %v", gerr)
		}
	}()

	// 正确实现下 GC 阻塞在扫 2 的锁上（fixture 极小，无锁的 GC 会在毫秒级完成）。
	select {
	case <-done:
		l.Unlock()
		t.Fatal("GC 在 id 锁被持有时完成——扫 2 的删除没有与 Put 互斥（锁内复核形同虚设）")
	case <-time.After(150 * time.Millisecond):
	}
	if _, err := os.Stat(dir); err != nil {
		l.Unlock()
		t.Fatalf("锁被持有时孤儿目录不该被删: %v", err)
	}

	// 解锁 → GC 完成删除。
	l.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("解锁后 GC 应完成")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("解锁后孤儿目录应被删（扫 2 复核通过 → 删除）")
	}
}
