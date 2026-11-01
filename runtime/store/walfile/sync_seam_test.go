package walfile

// Close 的 **fsync 观测面**（F4）钉子：Close 时 fsync **恰好被调一次**，且**在 close 之前**。
//
// # 为什么要有这组钉子
//
// 包文档与 Close 的注释一直写着"Close = 先 fsync 再关 fd"（机器掉电时不丢已 write 的
// 尾部记录）。但**黑盒面证不了这件事**：外部测试只能看到"关完文件内容完整"（那证明不了
// fsync 被调过、也证明不了它排在 close 之前——顺序写反了，黑盒一样绿）。
//
// 故把 Writer 依赖的文件面抽成接口（walFile，见 walfile.go），本文件注入一个**记账的假
// 文件** → "fsync 调了几次、先后如何"变成可断言的事实。
//
// 钉子：把 Close 里的 fsync 删掉（或挪到 close 之后）→ 本组钉子红。

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/core"
)

// fakeFile 是记账的假文件面：按顺序记下 write / sync / close。
type fakeFile struct {
	mu    sync.Mutex
	calls []string
	// syncErr 非 nil 时 Sync 返错（模拟 fsync 失败）。
	syncErr error
}

func (f *fakeFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "write")
	return len(p), nil
}

func (f *fakeFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "sync")
	return f.syncErr
}

func (f *fakeFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "close")
	return nil
}

// log 取出调用序（副本）。
func (f *fakeFile) log() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// countOf 数调用序里某个动作出现了几次。
func (f *fakeFile) countOf(action string) int {
	n := 0
	for _, c := range f.log() {
		if c == action {
			n++
		}
	}
	return n
}

// TestCloseSyncsExactlyOnceBeforeClose（F4 主钉）：Close → fsync **恰好一次**、且**排在
// close 之前**；二次 Close 幂等（不再 fsync、不再 close）。
func TestCloseSyncsExactlyOnceBeforeClose(t *testing.T) {
	f := &fakeFile{}
	w := &Writer{path: "/tmp/fake.wal", f: f}

	// 非边界 kind（turn.delta）→ 只 write、**不** fsync（逐条 delta 不 fsync 是有意取舍，
	// 见包 doc 的 fsync 策略）。
	if err := w.Append(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 1}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := f.countOf("sync"); got != 0 {
		t.Fatalf("非边界 kind 不该 fsync，sync 次数 = %d（calls=%v）", got, f.log())
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := f.countOf("sync"); got != 1 {
		t.Errorf("Close 时 fsync 次数 = %d，want 1（calls=%v）", got, f.log())
	}
	if got := f.countOf("close"); got != 1 {
		t.Errorf("Close 时 close 次数 = %d，want 1（calls=%v）", got, f.log())
	}
	// 顺序：先 fsync 再 close（反了 → 掉电时尾部记录可能丢）。
	calls := f.log()
	syncAt, closeAt := -1, -1
	for i, c := range calls {
		switch c {
		case "sync":
			syncAt = i
		case "close":
			closeAt = i
		}
	}
	if syncAt < 0 || closeAt < 0 || syncAt > closeAt {
		t.Fatalf("Close 的调用序 = %v，want sync 在 close 之前", calls)
	}

	// 二次 Close 幂等：不再动文件（不再 fsync、不再 close）。
	if err := w.Close(); err != nil {
		t.Fatalf("二次 Close 应幂等: %v", err)
	}
	if got := f.countOf("sync"); got != 1 {
		t.Errorf("二次 Close 又 fsync 了（sync 次数 = %d，calls=%v）", got, f.log())
	}
	if got := f.countOf("close"); got != 1 {
		t.Errorf("二次 Close 又 close 了（close 次数 = %d，calls=%v）", got, f.log())
	}
}

// TestAppendBoundaryKindSyncs（F4 的对照面）：**轮次边界** kind（turn.begin）→ 写完
// 立刻 fsync 一次（"Close 时 fsync 恰好一次"这句要有意义，得先知道边界 kind 也会 fsync）。
func TestAppendBoundaryKindSyncs(t *testing.T) {
	f := &fakeFile{}
	w := &Writer{path: "/tmp/fake.wal", f: f}
	if err := w.Append(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := f.countOf("sync"); got != 1 {
		t.Errorf("轮次边界 kind 的 fsync 次数 = %d，want 1（calls=%v）", got, f.log())
	}
	// 显式 Sync() 也 fsync 一次（供调用方自选时机落盘）。
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := f.countOf("sync"); got != 2 {
		t.Errorf("显式 Sync 后的 fsync 次数 = %d，want 2（calls=%v）", got, f.log())
	}
}

// TestCloseReportsSyncFailure（F4 的失败面）：fsync 失败 → Close **如实报错**（且文件仍
// 被关闭）——不许把 fsync 失败吞掉（吞了 = 谎报"关完就稳"）。
func TestCloseReportsSyncFailure(t *testing.T) {
	f := &fakeFile{syncErr: errors.New("fsync: 磁盘满了")}
	w := &Writer{path: "/tmp/fake.wal", f: f}
	err := w.Close()
	if err == nil {
		t.Fatal("fsync 失败时 Close 应返错（不掩盖）")
	}
	if !strings.Contains(err.Error(), "fsync") {
		t.Errorf("Close 错误应说清是 fsync 失败: %v", err)
	}
	if got := f.countOf("close"); got != 1 {
		t.Errorf("fsync 失败时文件仍应被关闭（close 次数 = %d，calls=%v）", got, f.log())
	}
}
