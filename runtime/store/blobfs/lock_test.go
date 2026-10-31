package blobfs

// F8 的钉子：写锁**固定分片**（不按 id 记 map）。
//
// 为什么钉：per-id 的 map 锁表**只增不减**——每个新 id 一把锁，进程越跑越久表越大（内存单调
// 增长）。改成固定分片后：① 同一 id 恒落同一片（互斥语义不变）② 锁的总数有界（≤ 分片数）。
// 这两条是"改了就一劳永逸"的可验证性质。

import (
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// TestIDLockSameIDSameShard：同一 id → 同一把锁（互斥语义与 per-id 锁一致）。
func TestIDLockSameIDSameShard(t *testing.T) {
	st := &Store{}
	id := core.NewUlid()
	if st.idLock(id) != st.idLock(id) {
		t.Fatal("同一 id 应拿到同一把锁（否则同 id 的写不再互斥）")
	}
}

// TestIDLockBoundedShardCount：1000 个不同 id → 用到的锁**不超过分片数**（表不再只增不减）。
func TestIDLockBoundedShardCount(t *testing.T) {
	st := &Store{}
	locks := map[any]bool{}
	for i := 0; i < 1000; i++ {
		locks[st.idLock(core.NewUlid())] = true
	}
	if len(locks) > blobLockShards {
		t.Fatalf("1000 个 id 用到的锁 = %d 把，want <= %d（分片固定，锁表不得只增不减）",
			len(locks), blobLockShards)
	}
	// 不同 id 大概率落不同片（分片没退化成"一把大锁"：并发度不能被抹平）。
	if len(locks) < 2 {
		t.Fatalf("所有 id 落在同一片（并发度被抹平），got %d 把锁", len(locks))
	}
}

// TestIDLockDeterministic：分片选择是**确定**的（同样的 id 永远同一片——不是随机哈希）。
func TestIDLockDeterministic(t *testing.T) {
	st := &Store{}
	var id contracts.Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	first := st.idLock(id)
	for i := 0; i < 100; i++ {
		if st.idLock(id) != first {
			t.Fatal("分片选择不确定（同一 id 两次拿到不同锁）")
		}
	}
}
