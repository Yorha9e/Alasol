package blobfs_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/runtime/store/blobfs"
)

// newStore 建一个落在临时目录的 blobfs（每个测试独享 root）。
func newStore(t *testing.T) *blobfs.Store {
	t.Helper()
	st, err := blobfs.New(t.TempDir())
	if err != nil {
		t.Fatalf("blobfs.New: %v", err)
	}
	return st
}

// validID 造一个合法 ULID（调用方自指 id 用；core.NewUlid 是框架 id 唯一生成处）。
func validID() contracts.Ulid { return core.NewUlid() }

// drain 读干一个 ReadCloser 并关闭（Get 流式读；这里只把内容取出来做断言）。
func drain(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("读 blob: %v", err)
	}
	return b
}

func TestPutGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	content := []byte("hello blob — 大对象外置仓库")
	meta, err := st.Put(ctx, storage.BlobMeta{MediaType: "text/plain", Name: "greeting.txt"}, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if meta.ID == "" {
		t.Fatal("Put 返回的 meta.ID 为空（实现必须发号）")
	}
	if meta.Size != int64(len(content)) {
		t.Fatalf("meta.Size = %d，want %d", meta.Size, len(content))
	}
	if meta.MediaType != "text/plain" {
		t.Fatalf("meta.MediaType = %q，want text/plain", meta.MediaType)
	}
	if meta.CreatedAt == "" {
		t.Fatal("meta.CreatedAt 为空（诊断字段应回填）")
	}
	if meta.Ref() == nil || *meta.Ref() != meta.ID {
		t.Fatalf("meta.Ref() 应折出 id %s，got %v", meta.ID, meta.Ref())
	}

	rc, err := st.Get(ctx, meta.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := drain(t, rc); !bytes.Equal(got, content) {
		t.Fatalf("Get 内容不符：got %q want %q", got, content)
	}

	sm, err := st.Stat(ctx, meta.ID)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if sm.ID != meta.ID || sm.Size != meta.Size || sm.MediaType != meta.MediaType {
		t.Fatalf("Stat 与 Put 元数据不符：%+v vs %+v", sm, meta)
	}
}

// TestPutGetLargeStreaming：>1MB 大对象往返（大小记录准确、内容逐字节一致）。
func TestPutGetLargeStreaming(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	big := make([]byte, 2<<20+123) // ~2MB，跨多个 32KB 拷贝/比较块
	if _, err := rand.Read(big); err != nil {
		t.Fatalf("rand: %v", err)
	}
	meta, err := st.Put(ctx, storage.BlobMeta{MediaType: "application/octet-stream"}, bytes.NewReader(big))
	if err != nil {
		t.Fatalf("Put 大对象: %v", err)
	}
	if meta.Size != int64(len(big)) {
		t.Fatalf("大对象 meta.Size = %d，want %d", meta.Size, len(big))
	}
	rc, err := st.Get(ctx, meta.ID)
	if err != nil {
		t.Fatalf("Get 大对象: %v", err)
	}
	got := drain(t, rc)
	if !bytes.Equal(got, big) {
		t.Fatalf("大对象往返内容不符（len got=%d want=%d）", len(got), len(big))
	}
}

// TestPutIdempotentSameContent：同 id 同内容 → 原样返回已有元数据，不写第二份。
func TestPutIdempotentSameContent(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	id := validID()
	content := []byte(`{"result":"same bytes"}`)
	first, err := st.Put(ctx, storage.BlobMeta{ID: id, MediaType: "application/json"}, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put #1: %v", err)
	}
	second, err := st.Put(ctx, storage.BlobMeta{ID: id, MediaType: "application/json"}, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put #2（同内容，应幂等）: %v", err)
	}
	if first.ID != second.ID || first.Size != second.Size || first.CreatedAt != second.CreatedAt {
		t.Fatalf("幂等 Put 返回的元数据应与首次一致：%+v vs %+v", first, second)
	}
	rc, err := st.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := drain(t, rc); !bytes.Equal(got, content) {
		t.Fatalf("幂等后内容被改写：%q", got)
	}
}

// TestPutIdempotentDoesNotWriteSecondCopy（F3 的钉子）：同 id 同内容**二次 Put 不写盘**
// （写计数不变）——幂等口径的**可观测**证据。
//
// 为什么这条必须存在：幂等（"不写第二份"）用**内容比对**根本验不出来——二次 Put 之后内容
// 一样、元数据一样，**看不出有没有重写**。把幂等分支改成"照样重写一遍内容"（结果完全相同、
// 元数据也相同）**整包测试仍然全绿**（变异存活）。唯一的观测点是"写了几次"：生产上靠 mtime，
// 但 Windows 的 mtime 精度不稳（同一 tick 内的两次写可能同值）→ 故实现侧挂**写计数探针**
// （测试钩子字段，生产恒 nil）。
//
// 断言口径：新建 Put = 1 次内容落盘；**幂等二次 Put 后仍是 1**（不是 2）。
func TestPutIdempotentDoesNotWriteSecondCopy(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	counter := &blobfs.WriteCounter{}
	st.SetWriteCounter(counter)

	id := validID()
	content := []byte(`{"result":"same bytes"}`)
	if _, err := st.Put(ctx, storage.BlobMeta{ID: id, MediaType: "application/json"}, bytes.NewReader(content)); err != nil {
		t.Fatalf("Put #1: %v", err)
	}
	if got := counter.Count(); got != 1 {
		t.Fatalf("新建 Put 后写计数 = %d，want 1（一次内容落盘）", got)
	}
	// 二次 Put：同 id 同内容 → 幂等，**不得再写一份**。
	if _, err := st.Put(ctx, storage.BlobMeta{ID: id, MediaType: "application/json"}, bytes.NewReader(content)); err != nil {
		t.Fatalf("Put #2（同内容，应幂等）: %v", err)
	}
	if got := counter.Count(); got != 1 {
		t.Fatalf("幂等二次 Put 后写计数 = %d，want 1（不写第二份）", got)
	}
	// 对照：**新 id** 一定 +1（否则探针本身没在数，钉子就是空的）。
	other := validID()
	if _, err := st.Put(ctx, storage.BlobMeta{ID: other}, bytes.NewReader(content)); err != nil {
		t.Fatalf("Put（新 id）: %v", err)
	}
	if got := counter.Count(); got != 2 {
		t.Fatalf("新 id Put 后写计数 = %d，want 2（探针确实在数落盘）", got)
	}
}

// TestPutConflictDifferentContent：同 id 不同内容 → ErrConflict，且原内容不被覆盖。
func TestPutConflictDifferentContent(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	id := validID()
	if _, err := st.Put(ctx, storage.BlobMeta{ID: id}, bytes.NewReader([]byte("original"))); err != nil {
		t.Fatalf("Put #1: %v", err)
	}
	_, err := st.Put(ctx, storage.BlobMeta{ID: id}, bytes.NewReader([]byte("DIFFERENT")))
	if !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("同 id 不同内容应 ErrConflict，got %v", err)
	}
	rc, gerr := st.Get(ctx, id)
	if gerr != nil {
		t.Fatalf("Get after conflict: %v", gerr)
	}
	if got := drain(t, rc); !bytes.Equal(got, []byte("original")) {
		t.Fatalf("冲突后原内容被静默覆盖：%q", got)
	}
}

// TestPutConflictSameSizeDifferentBytes：大小相同但字节不同也算冲突（防只比大小的浅判）。
func TestPutConflictSameSizeDifferentBytes(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	id := validID()
	if _, err := st.Put(ctx, storage.BlobMeta{ID: id}, bytes.NewReader([]byte("aaaa"))); err != nil {
		t.Fatalf("Put #1: %v", err)
	}
	_, err := st.Put(ctx, storage.BlobMeta{ID: id}, bytes.NewReader([]byte("bbbb")))
	if !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("同大小不同字节应 ErrConflict，got %v", err)
	}
}

func TestGetStatNotFound(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	missing := validID() // 合法但从未写过
	if _, err := st.Get(ctx, missing); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get 不存在应 ErrNotFound，got %v", err)
	}
	if _, err := st.Stat(ctx, missing); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Stat 不存在应 ErrNotFound，got %v", err)
	}
}

func TestDeleteIdempotent(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	meta, err := st.Put(ctx, storage.BlobMeta{}, bytes.NewReader([]byte("to be deleted")))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Delete(ctx, meta.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Stat(ctx, meta.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Delete 后 Stat 应 ErrNotFound，got %v", err)
	}
	if err := st.Delete(ctx, meta.ID); err != nil {
		t.Fatalf("重复 Delete 应幂等成功，got %v", err)
	}
	if err := st.Delete(ctx, validID()); err != nil {
		t.Fatalf("删不存在应幂等成功，got %v", err)
	}
}

// TestDeleteDoesNotCascade：删 blob 只回收内容，引用仍在但取不到 → ErrNotFound（内容已回收），
// 事件流里的引用原样留着（blobfs 不声称、也无从删事件——那由 store 侧保证）。
func TestDeleteDoesNotCascade(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	meta, err := st.Put(ctx, storage.BlobMeta{}, bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	ref := meta.Ref()
	if ref == nil {
		t.Fatal("Ref 不应为 nil")
	}
	if err := st.Delete(ctx, *ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Get(ctx, *ref); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("删除后按引用取内容应 ErrNotFound（内容已回收），got %v", err)
	}
}

// TestConcurrentPutSameID：多 goroutine 同 id 同内容 Put（写锁串行化 → 幂等，不双写坏数据）。
func TestConcurrentPutSameID(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	id := validID()
	content := []byte("concurrent same content")

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = st.Put(ctx, storage.BlobMeta{ID: id}, bytes.NewReader(content))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d Put 同 id 同内容失败: %v", i, err)
		}
	}
	rc, err := st.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := drain(t, rc); !bytes.Equal(got, content) {
		t.Fatalf("并发后内容损坏：%q", got)
	}
}

// TestHalfFileReadExplicitError：半截/损坏内容 → 明确 error（非 ErrNotFound、不静默给截断内容）。
func TestHalfFileReadExplicitError(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	meta, err := st.Put(ctx, storage.BlobMeta{MediaType: "text/plain"},
		bytes.NewReader([]byte("this content is longer than the truncated version")))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// 按文档化的磁盘布局直接截断 content 文件（模拟写一半崩了的遗留）。
	contentPath := filepath.Join(st.Root(), string(meta.ID), "content")
	if err := os.Truncate(contentPath, 5); err != nil {
		t.Fatalf("截断 content 文件: %v", err)
	}
	_, gerr := st.Get(ctx, meta.ID)
	if gerr == nil {
		t.Fatal("半截内容应报明确错误，got nil")
	}
	if errors.Is(gerr, storage.ErrNotFound) {
		t.Fatalf("半截内容是损坏、不是'不存在'，不该报 ErrNotFound：%v", gerr)
	}
	if !strings.Contains(gerr.Error(), "不符") {
		t.Fatalf("半截内容错误信息应点明'大小不符'，got %v", gerr)
	}
}

// TestMetaExistsContentMissing：meta.json 在但 content 缺失 → 明确错误（区别于 ErrNotFound）。
func TestMetaExistsContentMissing(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)

	meta, err := st.Put(ctx, storage.BlobMeta{}, bytes.NewReader([]byte("payload")))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	contentPath := filepath.Join(st.Root(), string(meta.ID), "content")
	if err := os.Remove(contentPath); err != nil {
		t.Fatalf("删 content 文件: %v", err)
	}
	_, gerr := st.Get(ctx, meta.ID)
	if gerr == nil {
		t.Fatal("meta 在内容不在应报明确错误，got nil")
	}
	if errors.Is(gerr, storage.ErrNotFound) {
		t.Fatalf("meta 在内容不在是损坏、不是'不存在'，不该报 ErrNotFound：%v", gerr)
	}
}

// TestReopenPreservesState：同一 root 再开一个实例 = "重开进程"，状态保留（可取可查）。
func TestReopenPreservesState(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	st1, err := blobfs.New(root)
	if err != nil {
		t.Fatalf("New #1: %v", err)
	}
	meta, err := st1.Put(ctx, storage.BlobMeta{MediaType: "application/json", Name: "r.json"},
		bytes.NewReader([]byte(`{"persisted":true}`)))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// blobfs 无长驻句柄；再开一个指向同一 root 的实例即"重开"。
	st2, err := blobfs.New(root)
	if err != nil {
		t.Fatalf("New #2 (reopen): %v", err)
	}
	sm, err := st2.Stat(ctx, meta.ID)
	if err != nil {
		t.Fatalf("重开后 Stat: %v", err)
	}
	if sm.ID != meta.ID || sm.Size != meta.Size || sm.MediaType != "application/json" {
		t.Fatalf("重开后元数据不符：%+v vs %+v", sm, meta)
	}
	rc, err := st2.Get(ctx, meta.ID)
	if err != nil {
		t.Fatalf("重开后 Get: %v", err)
	}
	if got := drain(t, rc); !bytes.Equal(got, []byte(`{"persisted":true}`)) {
		t.Fatalf("重开后内容不符：%q", got)
	}
}

func TestNewRejectsEmptyRoot(t *testing.T) {
	if _, err := blobfs.New("   "); err == nil {
		t.Fatal("空 root 应被拒绝")
	}
}

// TestInvalidUlidRejected：非法 id（太短 / 路径穿越 / 含 Crockford 排除字符 / 空）在
// Get/Stat/Delete 一律被拒（防路径穿越；S1/S9 不合则拒不修复）。
func TestInvalidUlidRejected(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	bad := []contracts.Ulid{
		contracts.Ulid(""),                            // 空
		contracts.Ulid("short"),                       // 太短
		contracts.Ulid("../../etc/passwd"),            // 路径穿越
		contracts.Ulid("I" + strings.Repeat("0", 25)), // 26 字符但含排除字符 I
	}
	for _, id := range bad {
		if _, err := st.Get(ctx, id); err == nil {
			t.Fatalf("Get 非法 id %q 应被拒", string(id))
		}
		if _, err := st.Stat(ctx, id); err == nil {
			t.Fatalf("Stat 非法 id %q 应被拒", string(id))
		}
		if err := st.Delete(ctx, id); err == nil {
			t.Fatalf("Delete 非法 id %q 应被拒", string(id))
		}
	}
}
