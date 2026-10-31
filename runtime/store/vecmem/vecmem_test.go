package vecmem_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/runtime/store/vecmem"
)

// 编译期证明（G2d）：*Store 同时是向量族读面与写面。
var (
	_ storage.Retriever   = (*vecmem.Store)(nil)
	_ storage.VectorIndex = (*vecmem.Store)(nil)
)

func mustUpsert(t *testing.T, s *vecmem.Store, vs ...storage.Vector) {
	t.Helper()
	if err := s.Upsert(context.Background(), vs); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
}

func mustRetrieve(t *testing.T, s *vecmem.Store, q storage.VectorQuery) []storage.VectorHit {
	t.Helper()
	hits, err := s.Retrieve(context.Background(), q)
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	return hits
}

// payload 造一个 JSON 载荷（分块原文 / 元数据；框架不解释）。
func payload(t *testing.T, s string) []byte {
	t.Helper()
	if s == "" {
		return nil
	}
	return []byte(s)
}

// TestUpsertOverwriteSameID：同 ID 覆盖（embedding 重算的标准路径）。
func TestUpsertOverwriteSameID(t *testing.T) {
	ctx := context.Background()
	s := vecmem.New(0)
	theID := id(t, "overwrite")
	mustUpsert(t, s, storage.Vector{ID: theID, Embedding: []float32{1, 0, 0}, SourceRef: "e:1"})
	mustUpsert(t, s, storage.Vector{ID: theID, Embedding: []float32{0, 1, 0}, SourceRef: "e:2"})
	if got := s.Len(); got != 1 {
		t.Fatalf("同 ID 覆盖后应只有 1 条（得到 %d）", got)
	}
	hits := mustRetrieve(t, s, storage.VectorQuery{Embedding: []float32{0, 1, 0}, K: 5})
	if len(hits) != 1 || hits[0].SourceRef != "e:2" {
		t.Fatalf("覆盖后应取到新条目: %+v", hits)
	}
	// 空批 Upsert / Delete → 幂等 no-op。
	if err := s.Upsert(ctx, nil); err != nil {
		t.Fatalf("空批 Upsert: %v", err)
	}
	if err := s.Delete(ctx, nil); err != nil {
		t.Fatalf("空批 Delete: %v", err)
	}
	if s.Len() != 1 {
		t.Fatalf("空批不该改动库（得到 %d 条）", s.Len())
	}
}

// TestRetrieveTopKCosineOrder：top-k 按余弦降序（用已知向量算好的分数钉死）。
func TestRetrieveTopKCosineOrder(t *testing.T) {
	ctx := context.Background()
	s := vecmem.New(0)
	mustUpsert(t, s,
		storage.Vector{ID: id(t, "A"), Embedding: []float32{1, 0, 0}, SourceRef: "e:a"},
		storage.Vector{ID: id(t, "B"), Embedding: []float32{1, 1, 0}, SourceRef: "e:b"},
		storage.Vector{ID: id(t, "C"), Embedding: []float32{0, 1, 0}, SourceRef: "e:c"},
		storage.Vector{ID: id(t, "D"), Embedding: []float32{-1, 0, 0}, SourceRef: "e:d"},
	)
	hits := mustRetrieve(t, s, storage.VectorQuery{Embedding: []float32{1, 0, 0}})
	wantOrder := []string{"e:a", "e:b", "e:c", "e:d"}
	if len(hits) != len(wantOrder) {
		t.Fatalf("K=0 应取默认上限内的全部候选（得到 %d 条）", len(hits))
	}
	for i, want := range wantOrder {
		if hits[i].SourceRef != want {
			t.Fatalf("第 %d 条应是 %s，得到 %s（排序口径被破坏）", i, want, hits[i].SourceRef)
		}
	}
	// 余弦分数（已知向量手算）：a=1, b=1/√2≈0.7071068, c=0, d=-1。
	wantScores := []float32{1, 0.7071068, 0, -1}
	for i, want := range wantScores {
		if diff := float64(hits[i].Score) - float64(want); diff > 1e-6 || diff < -1e-6 {
			t.Fatalf("第 %d 条分数 %v，期望 %v（余弦口径被破坏）", i, hits[i].Score, want)
		}
	}
	if hits[0].Truncated {
		t.Fatal("候选 4 条、K=0 默认 10 → 不该置 Truncated")
	}

	// K=2 → 截断，且 Truncated 诚实置位。
	top2 := mustRetrieve(t, s, storage.VectorQuery{Embedding: []float32{1, 0, 0}, K: 2})
	if len(top2) != 2 || top2[0].SourceRef != "e:a" || top2[1].SourceRef != "e:b" {
		t.Fatalf("K=2 应取最像的两条: %+v", top2)
	}
	if !top2[0].Truncated || !top2[1].Truncated {
		t.Fatalf("被 K 截断时 Truncated 必须置位（诚实信号）: %+v", top2)
	}
	// K 大于候选数 → 不截断。
	all := mustRetrieve(t, s, storage.VectorQuery{Embedding: []float32{1, 0, 0}, K: 99})
	if len(all) != 4 || all[0].Truncated {
		t.Fatalf("K 大于候选数时不该截断: %+v", all)
	}
	if err := s.Clear(ctx); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if got := mustRetrieve(t, s, storage.VectorQuery{Embedding: []float32{1, 0, 0}}); len(got) != 0 {
		t.Fatalf("Clear 后应为空（得到 %+v）", got)
	}
}

// TestDeleteIdempotent：Delete 幂等（不存在 → 跳过，不报错）。
func TestDeleteIdempotent(t *testing.T) {
	ctx := context.Background()
	s := vecmem.New(0)
	mustUpsert(t, s, storage.Vector{ID: id(t, "A"), Embedding: []float32{1, 0}})
	if err := s.Delete(ctx, []contracts.Ulid{id(t, "MISSING")}); err != nil {
		t.Fatalf("删不存在的 id 应幂等成功: %v", err)
	}
	if err := s.Delete(ctx, []contracts.Ulid{id(t, "A")}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if s.Len() != 0 {
		t.Fatalf("删完应为空（得到 %d）", s.Len())
	}
	// 删到空 → 维度锁解除（Rebuild 后可换维度的模型重建）。
	mustUpsert(t, s, storage.Vector{ID: id(t, "B"), Embedding: []float32{1, 2, 3, 4}})
	if got := s.Dim(); got != 4 {
		t.Fatalf("维度锁应随新首条重锁为 4（得到 %d）", got)
	}
}

// TestDimensionMismatch：维度不一致 → ErrBadQuery（写入与查询两侧）。
func TestDimensionMismatch(t *testing.T) {
	ctx := context.Background()
	s := vecmem.New(0)
	mustUpsert(t, s, storage.Vector{ID: id(t, "A"), Embedding: []float32{1, 0, 0}})
	// 混维度 Upsert → ErrBadQuery，且**整批不落**（不留半批）。
	err := s.Upsert(ctx, []storage.Vector{
		{ID: id(t, "B"), Embedding: []float32{1, 0, 0}},
		{ID: id(t, "C"), Embedding: []float32{1, 0}},
	})
	if !errors.Is(err, storage.ErrBadQuery) {
		t.Fatalf("混维度 Upsert 应 ErrBadQuery，得到 %v", err)
	}
	if s.Len() != 1 {
		t.Fatalf("整批校验失败时不该落任何一条（得到 %d 条）", s.Len())
	}
	// 查询向量维度不符 → ErrBadQuery。
	if _, err := s.Retrieve(ctx, storage.VectorQuery{Embedding: []float32{1, 0}}); !errors.Is(err, storage.ErrBadQuery) {
		t.Fatalf("查询维度不符应 ErrBadQuery，得到 %v", err)
	}
}

// TestUpsertMixedDimensionBatchDoesNotLockDimension（F1 钉子）：**空库**收到一批混维度
// 条目 → 整批被拒（ErrBadQuery）且**维度锁不落**（Dim() 仍是 0）——
//
// 曾经的 bug：先落 s.dim 再校验整批 → 失败后留下"Len=0 而 Dim≠0"的毒化状态，
// 此后任何合法写入/检索恒 ErrBadQuery（库里没东西可删 → Delete 也解不开锁），
// 进程内不可恢复。
func TestUpsertMixedDimensionBatchDoesNotLockDimension(t *testing.T) {
	ctx := context.Background()
	s := vecmem.New(0)
	err := s.Upsert(ctx, []storage.Vector{
		{ID: id(t, "A"), Embedding: []float32{1, 0, 0}},
		{ID: id(t, "B"), Embedding: []float32{1, 0}},
	})
	if !errors.Is(err, storage.ErrBadQuery) {
		t.Fatalf("空库混维度批次应 ErrBadQuery，得到 %v", err)
	}
	if got := s.Dim(); got != 0 {
		t.Fatalf("失败批次不该锁维度（Dim()=%d，应为 0——库被毒化）", got)
	}
	if got := s.Len(); got != 0 {
		t.Fatalf("失败批次不该落条目（Len()=%d）", got)
	}
	// 库没被毒化：随后合法的 2 维写入 + 检索都成功。
	mustUpsert(t, s, storage.Vector{ID: id(t, "C"), Embedding: []float32{1, 0}, SourceRef: "e:c"})
	if got := s.Dim(); got != 2 {
		t.Fatalf("后续合法写入应锁维度 2（得到 %d）", got)
	}
	hits := mustRetrieve(t, s, storage.VectorQuery{Embedding: []float32{1, 0}, K: 5})
	if len(hits) != 1 || hits[0].SourceRef != "e:c" {
		t.Fatalf("后续合法检索应命中: %+v", hits)
	}
}

// TestBadQuerySentinel：负 K / 空 Embedding（写入与查询）→ ErrBadQuery。
func TestBadQuerySentinel(t *testing.T) {
	ctx := context.Background()
	s := vecmem.New(0)
	if _, err := s.Retrieve(ctx, storage.VectorQuery{Embedding: []float32{1}, K: -1}); !errors.Is(err, storage.ErrBadQuery) {
		t.Fatalf("负 K 应 ErrBadQuery，得到 %v", err)
	}
	if _, err := s.Retrieve(ctx, storage.VectorQuery{}); !errors.Is(err, storage.ErrBadQuery) {
		t.Fatalf("空查询向量应 ErrBadQuery，得到 %v", err)
	}
	if err := s.Upsert(ctx, []storage.Vector{{ID: id(t, "A")}}); !errors.Is(err, storage.ErrBadQuery) {
		t.Fatalf("空 Embedding 的 Upsert 应 ErrBadQuery，得到 %v", err)
	}
	if err := s.Upsert(ctx, []storage.Vector{{Embedding: []float32{1}}}); !errors.Is(err, storage.ErrBadQuery) {
		t.Fatalf("空 ID 的 Upsert 应 ErrBadQuery，得到 %v", err)
	}
}

// TestEmptyLibraryRetrieve：空库检索 → 空结果，不报错。
func TestEmptyLibraryRetrieve(t *testing.T) {
	s := vecmem.New(0)
	hits, err := s.Retrieve(context.Background(), storage.VectorQuery{Embedding: []float32{1, 2, 3}, K: 5})
	if err != nil {
		t.Fatalf("空库检索不该报错: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("空库应返回空结果（得到 %+v）", hits)
	}
	if got := s.Dim(); got != 0 {
		t.Fatalf("空库维度锁应为 0（得到 %d）", got)
	}
}

// TestFilterExactMatch：Filter 精确匹配 Payload 顶层字符串字段（AND 语义）。
func TestFilterExactMatch(t *testing.T) {
	s := vecmem.New(0)
	mustUpsert(t, s,
		storage.Vector{ID: id(t, "A"), Embedding: []float32{1, 0, 0},
			Payload: payload(t, `{"kind":"note","lang":"go"}`), SourceRef: "e:a"},
		storage.Vector{ID: id(t, "B"), Embedding: []float32{1, 0, 0},
			Payload: payload(t, `{"kind":"code","lang":"go"}`), SourceRef: "e:b"},
		storage.Vector{ID: id(t, "C"), Embedding: []float32{1, 0, 0},
			Payload: payload(t, `{"kind":1}`), SourceRef: "e:c"}, // 非字符串值
		storage.Vector{ID: id(t, "D"), Embedding: []float32{1, 0, 0}, SourceRef: "e:d"}, // 无 Payload
		storage.Vector{ID: id(t, "E"), Embedding: []float32{1, 0, 0},
			Payload: payload(t, `不是合法 JSON`), SourceRef: "e:e"}, // 坏 JSON
	)

	// 精确命中：只留 A。
	hits := mustRetrieve(t, s, storage.VectorQuery{
		Embedding: []float32{1, 0, 0}, K: 10, Filter: map[string]string{"kind": "note"},
	})
	if len(hits) != 1 || hits[0].SourceRef != "e:a" {
		t.Fatalf("filter kind=note 应只命中 A: %+v", hits)
	}
	// 大小写敏感（"Note" ≠ "note"）——静默不过滤 = 检索到不该检索的东西。
	hits = mustRetrieve(t, s, storage.VectorQuery{
		Embedding: []float32{1, 0, 0}, Filter: map[string]string{"kind": "Note"},
	})
	if len(hits) != 0 {
		t.Fatalf("大小写敏感：kind=Note 不该命中: %+v", hits)
	}
	// AND：两个键都命中才留。
	hits = mustRetrieve(t, s, storage.VectorQuery{
		Embedding: []float32{1, 0, 0}, Filter: map[string]string{"kind": "note", "lang": "go"},
	})
	if len(hits) != 1 || hits[0].SourceRef != "e:a" {
		t.Fatalf("AND 过滤应只命中 A: %+v", hits)
	}
	hits = mustRetrieve(t, s, storage.VectorQuery{
		Embedding: []float32{1, 0, 0}, Filter: map[string]string{"kind": "note", "lang": "rust"},
	})
	if len(hits) != 0 {
		t.Fatalf("AND 有一个键不中就不命中: %+v", hits)
	}
	// 键缺失 / 非字符串值 / 坏 JSON → 不命中（不是错误）。
	hits = mustRetrieve(t, s, storage.VectorQuery{
		Embedding: []float32{1, 0, 0}, Filter: map[string]string{"kind": "1"},
	})
	if len(hits) != 0 {
		t.Fatalf("顶层值是数字 1 时不该命中字符串 \"1\": %+v", hits)
	}
	hits = mustRetrieve(t, s, storage.VectorQuery{
		Embedding: []float32{1, 0, 0}, Filter: map[string]string{"missing": "x"},
	})
	if len(hits) != 0 {
		t.Fatalf("键缺失不该命中: %+v", hits)
	}
	// 空 Filter = 不过滤（全部 5 条）。
	hits = mustRetrieve(t, s, storage.VectorQuery{Embedding: []float32{1, 0, 0}, K: 10})
	if len(hits) != 5 {
		t.Fatalf("空 Filter 应不过滤（得到 %d 条）", len(hits))
	}
	// 过滤后截断信号仍然诚实：候选 1、K=1 → 不截断。
	hits = mustRetrieve(t, s, storage.VectorQuery{
		Embedding: []float32{1, 0, 0}, K: 1, Filter: map[string]string{"kind": "note"},
	})
	if len(hits) != 1 || hits[0].Truncated {
		t.Fatalf("过滤后候选 1 条、K=1 不该截断: %+v", hits)
	}
}

// TestUpsertSnapshot：喂进来即快照（调用方事后改自己的切片不该改到库里）。
func TestUpsertSnapshot(t *testing.T) {
	s := vecmem.New(0)
	emb := []float32{1, 0, 0}
	pl := payload(t, `{"kind":"note"}`)
	mustUpsert(t, s, storage.Vector{ID: id(t, "A"), Embedding: emb, Payload: pl})
	emb[0] = -5
	pl[0] = 'X'
	hits := mustRetrieve(t, s, storage.VectorQuery{
		Embedding: []float32{1, 0, 0}, K: 1, Filter: map[string]string{"kind": "note"},
	})
	if len(hits) != 1 || hits[0].Score != 1 {
		t.Fatalf("调用方改入参不该影响库内条目（快照口径）: %+v", hits)
	}
}

// TestDefaultKOverride：构造时给定默认 top-k（K=0 时生效）。
func TestDefaultKOverride(t *testing.T) {
	s := vecmem.New(2)
	mustUpsert(t, s,
		storage.Vector{ID: id(t, "A"), Embedding: []float32{1, 0, 0}},
		storage.Vector{ID: id(t, "B"), Embedding: []float32{1, 0, 0}},
		storage.Vector{ID: id(t, "C"), Embedding: []float32{1, 0, 0}},
	)
	hits := mustRetrieve(t, s, storage.VectorQuery{Embedding: []float32{1, 0, 0}})
	if len(hits) != 2 {
		t.Fatalf("K=0 应取构造时给定的默认上限 2（得到 %d）", len(hits))
	}
	if !hits[0].Truncated {
		t.Fatal("默认上限截断时 Truncated 也必须置位")
	}
	if vecmem.DefaultK != 10 {
		t.Fatalf("DefaultK 常量应是 10（得到 %d）", vecmem.DefaultK)
	}
}

// TestConcurrentUpsertRetrieve：并发 Upsert/Retrieve 安全（-race 下跑更严）。
func TestConcurrentUpsertRetrieve(t *testing.T) {
	ctx := context.Background()
	s := vecmem.New(0)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				v := storage.Vector{
					ID:        id(t, "W"+string(rune('A'+w))),
					Embedding: []float32{float32(w + 1), float32(i + 1), 0},
					SourceRef: "e:w",
				}
				if err := s.Upsert(ctx, []storage.Vector{v}); err != nil {
					t.Errorf("并发 Upsert: %v", err)
					return
				}
				if _, err := s.Retrieve(ctx, storage.VectorQuery{Embedding: []float32{1, 1, 0}, K: 3}); err != nil {
					t.Errorf("并发 Retrieve: %v", err)
					return
				}
				if i%10 == 0 {
					_ = s.Delete(ctx, []contracts.Ulid{id(t, "W"+string(rune('A'+w)))})
				}
			}
		}(w)
	}
	wg.Wait()
	// 收尾：全删 → 维度锁解除（不死锁、不残留半态）。
	if err := s.Clear(ctx); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if s.Len() != 0 || s.Dim() != 0 {
		t.Fatalf("Clear 后应为空且维度锁解除（len=%d dim=%d）", s.Len(), s.Dim())
	}
}

// id 造一个合法 ULID 形态的条目 id（调用方分配；(源,分块) 的确定性键）。
//
// 由 tag 的 SHA-256 取前 128bit 编 Crockford base32——**确定性**（同 tag 同 id），
// 且过 core.ValidUlid 校验（26 字符 Crockford，排除 I/L/O/U）。
func id(t *testing.T, tag string) contracts.Ulid {
	t.Helper()
	sum := sha256.Sum256([]byte("vecmem-test:" + tag))
	u := contracts.Ulid(core.EncodeCrockford26(sum[:16]))
	if !core.ValidUlid(u) {
		t.Fatalf("造出来的 id 不是合法 ULID: %q", string(u))
	}
	return u
}
