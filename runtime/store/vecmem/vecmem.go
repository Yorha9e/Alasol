package vecmem

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/storage"
)

// DefaultK 是 K=0（未指定）时的默认 top-k 上限（接口注释："0 = 实现默认上限"）。
//
// 取 10 而不是"全返回"：RAG 的用法是"取最像的几段喂上下文"，全返回会把上下文顶穿
// （F12 同款理由：上限不是优化，是正确性）。调用方要更多就显式给 K。
const DefaultK = 10

// 编译期证明：*Store 同时落在向量族的读面与写面上（G2d；换实现面不变）。
var (
	_ storage.Retriever   = (*Store)(nil)
	_ storage.VectorIndex = (*Store)(nil)
)

// entry 是库内一条向量条目（Vector + 为过滤而提取的顶层字符串字段）。
//
// fields 与 vec.Payload 同源：Payload 是"框架不解释"的原始 JSON，fields 只是它**顶层
// 字符串字段**的一份索引（Filter 匹配用，见包 doc 的 Filter 口径）。提取失败
// （Payload 空 / 不是合法 JSON 对象 / 顶层值都不是字符串）→ fields 为 nil
// = 任何非空 Filter 都不命中该条目（精确匹配的"不中"，不是错误）。
type entry struct {
	vec    storage.Vector
	fields map[string]string
}

// Store 是 core/storage 向量族的内存实现（并发安全；见包 doc）。
type Store struct {
	// mu 护整个 map 与 dim：写与写串行，读与读并发（RWMutex）。
	// 一把整库锁而不是分片锁：内存实现的规模上限由调用方决定（RAG 的候选集本来就要
	// 全扫算余弦，分片只会让"一次检索"变成 N 次加锁 + 合并排序，不买到真并发）。
	mu sync.RWMutex
	// byID 是条目表（ID 由调用方分配，(源,分块) 的确定性键；同 ID 覆盖）。
	byID map[contracts.Ulid]entry
	// dim 是库内维度锁（0 = 未锁定/库为空；见包 doc"维度一致性"）。
	dim int
	// defaultK 是 K=0 时的默认 top-k（构造时给定；<=0 → DefaultK）。
	defaultK int
}

// New 建一个空的内存向量库（**不持久化**：可整体重建的缓存，丢了重建，见包 doc）。
//
// defaultK <= 0 → DefaultK（10）。
func New(defaultK int) *Store {
	if defaultK <= 0 {
		defaultK = DefaultK
	}
	return &Store{byID: map[contracts.Ulid]entry{}, defaultK: defaultK}
}

// DefaultK 返回本库的默认 top-k（K=0 时生效；诊断/测试用）。
func (s *Store) DefaultK() int { return s.defaultK }

// Len 返回库内条目数（诊断/测试用；不属存储接口）。
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byID)
}

// Dim 返回当前维度锁（0 = 未锁定 = 库为空；诊断/测试用）。
func (s *Store) Dim() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dim
}

// Upsert 写入/更新一批向量条目（同 ID 覆盖——embedding 重算的标准路径）。
//
// 口径：
//   - ID 为空 / Embedding 为空 → ErrBadQuery（调用方漏喂，fail-close：空向量没有
//     "相似度"可言，接口注释明写"实现可拒"）；
//   - 维度与库内锁定维度不一致 → ErrBadQuery（**整批不落**：先校验整批再写入，
//     不留"半批落地"的中间态）；
//   - 空批 → 幂等成功（no-op）。
//
// **失败路径库里一点没动**（含维度锁）：校验用**局部** effDim（见下），只有整批校验
// 全过才落锁 + 落条目。曾经的 bug（F1）：先把 s.dim 设成首条维度再校验整批 →
// "空库 + 混维度批次"失败后留下 Len=0 而 Dim≠0 的毒化状态，此后任何合法写入/检索
// 恒 ErrBadQuery（库里没东西可删 → Delete 也解不开锁），**进程内不可恢复**。
func (s *Store) Upsert(_ context.Context, vs []storage.Vector) error {
	if len(vs) == 0 {
		return nil // 空批：no-op（幂等）
	}
	// 先校验整批（不持锁：纯入参检查，失败时库里一点没动）。
	for i := range vs {
		if vs[i].ID == "" {
			return fmt.Errorf("vecmem: 第 %d 条目的 ID 为空（Vector.ID 由调用方分配，(源,分块) 的确定性键）: %w",
				i, storage.ErrBadQuery)
		}
		if len(vs[i].Embedding) == 0 {
			return fmt.Errorf("vecmem: 条目 %s 的 Embedding 为空（框架不算 embedding——由调用方算好喂进来）: %w",
				string(vs[i].ID), storage.ErrBadQuery)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// 维度基准 effDim：库内已锁定的维度（s.dim > 0）；库空时 = 本批首条维度
	// （"首条锁定"口径，但**只在本批自洽时**才真的锁下去）。
	effDim := s.dim
	libraryEmpty := effDim == 0
	if libraryEmpty {
		effDim = len(vs[0].Embedding)
	}
	for i := range vs {
		if len(vs[i].Embedding) != effDim {
			if libraryEmpty {
				return fmt.Errorf("vecmem: 条目 %s 维度 %d 与本批基准维度 %d 不一致"+
					"（同批必须同维度；库空时以本批首条为基准，校验不过**不锁维度**）: %w",
					string(vs[i].ID), len(vs[i].Embedding), effDim, storage.ErrBadQuery)
			}
			return fmt.Errorf("vecmem: 条目 %s 维度 %d 与库内维度 %d 不一致（余弦要求同维度；库变空即解除锁定）: %w",
				string(vs[i].ID), len(vs[i].Embedding), effDim, storage.ErrBadQuery)
		}
	}
	// 整批校验全过 → 才落维度锁（库空时锁到本批维度）→ 再落条目。
	if libraryEmpty {
		s.dim = effDim
	}
	for i := range vs {
		s.byID[vs[i].ID] = entry{vec: cloneVector(vs[i]), fields: topLevelStringFields(vs[i].Payload)}
	}
	return nil
}

// Delete 按 id 删一批（不存在 → 幂等跳过）。
//
// 删到库空时**解除维度锁**（Rebuild 后可以换维度的模型重建，G24②）。
func (s *Store) Delete(_ context.Context, ids []contracts.Ulid) error {
	if len(ids) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		delete(s.byID, id)
	}
	if len(s.byID) == 0 {
		s.dim = 0
	}
	return nil
}

// Clear 清空整个向量库（索引器 Rebuild 用，G24②；与文档/图同口径）。
//
// 连维度锁一起清（Rebuild = 从 0 重放，重放的模型可能换维度）。
func (s *Store) Clear(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID = map[contracts.Ulid]entry{}
	s.dim = 0
	return nil
}

// Retrieve 按查询向量取 top-k（按 Score 降序；结果带 Truncated 诚实信号）。
//
// 口径：
//   - 查询向量为空 / K 为负 → ErrBadQuery（fail-close：不返回"空结果"冒充"没有相似内容"）；
//   - 查询向量维度与库内锁定维度不符 → ErrBadQuery；
//   - **空库检索 → 空结果不报错**（"库是空的"不是"查询非法"）；
//   - K=0 → 本库默认上限（New 的 defaultK）；
//   - Truncated = 候选集比返回集大（被 K 截断）——召回可能不全。
func (s *Store) Retrieve(_ context.Context, q storage.VectorQuery) ([]storage.VectorHit, error) {
	if len(q.Embedding) == 0 {
		return nil, fmt.Errorf("vecmem: 查询向量为空（Embedding 由调用方算好喂进来）: %w", storage.ErrBadQuery)
	}
	if q.K < 0 {
		return nil, fmt.Errorf("vecmem: K 不能为负（得到 %d；0 = 默认上限 %d）: %w", q.K, s.defaultK, storage.ErrBadQuery)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.dim > 0 && len(q.Embedding) != s.dim {
		return nil, fmt.Errorf("vecmem: 查询向量维度 %d 与库内维度 %d 不一致: %w",
			len(q.Embedding), s.dim, storage.ErrBadQuery)
	}
	type scored struct {
		hit   storage.VectorHit
		score float32
	}
	all := make([]scored, 0, len(s.byID))
	for id, e := range s.byID {
		if len(e.vec.Embedding) != len(q.Embedding) {
			continue // 纵深防御：维度锁保证同维度，真出现混维度就跳过（不静默改分数）
		}
		if !matchFields(e.fields, q.Filter) {
			continue
		}
		sc := cosine(q.Embedding, e.vec.Embedding)
		all = append(all, scored{
			hit: storage.VectorHit{
				ID:        id,
				Score:     sc,
				Payload:   clonePayload(e.vec.Payload), // 回显也拷一份：消费方改不到库内
				SourceRef: e.vec.SourceRef,
			},
			score: sc,
		})
	}
	// 降序 + 同分按 ID 升序（确定性：map 遍历顺序随机，不定序会让同分条目换位）。
	sort.Slice(all, func(i, j int) bool {
		if all[i].score != all[j].score {
			return all[i].score > all[j].score
		}
		return all[i].hit.ID < all[j].hit.ID
	})
	k := q.K
	if k <= 0 {
		k = s.defaultK
	}
	truncated := len(all) > k
	if k > len(all) {
		k = len(all)
	}
	out := make([]storage.VectorHit, 0, k)
	for i := 0; i < k; i++ {
		all[i].hit.Truncated = truncated
		out = append(out, all[i].hit)
	}
	return out, nil
}

// cosine 算余弦相似度（float64 累加后转 float32；零向量 → 0，不除零）。
//
// 分数是**本实现的口径**（接口注释：跨实现不可比——换引擎只可在同一次检索内排序用）。
func cosine(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
	}
	if na == 0 || nb == 0 {
		return 0 // 零向量：无方向可言，相似度定为 0（不除零、不报错）
	}
	return float32(dot / (math.Sqrt(na) * math.Sqrt(nb)))
}

// matchFields 判条目是否命中 Filter（AND 语义；nil/空 Filter = 全中）。
//
// 口径见包 doc：键/值字节精确匹配**顶层字符串字段**；键缺失或值非字符串 → 不命中。
func matchFields(fields map[string]string, filter map[string]string) bool {
	if len(filter) == 0 {
		return true
	}
	if len(fields) == 0 {
		return false // 没有可匹配的顶层字符串字段（含 Payload 空/坏 JSON）
	}
	for k, want := range filter {
		got, ok := fields[k]
		if !ok || got != want {
			return false
		}
	}
	return true
}

// topLevelStringFields 提取 Payload JSON 的顶层**字符串**字段（Filter 的匹配面）。
//
// 失败（空 / 不是合法 JSON 对象）→ nil。非字符串的顶层值（数字/布尔/对象/数组/null）
// **不进** fields：Filter 的语义是字节精确的字符串匹配，让 `{"kind":1}` 命中
// `filter={"kind":"1"}` 只会制造"看起来匹配了"的假象。
func topLevelStringFields(payload json.RawMessage) map[string]string {
	if len(payload) == 0 {
		return nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			continue // 非字符串值：不进索引（不命中）
		}
		out[k] = s
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// cloneVector 拷一条条目（Embedding/Payload 都不与调用方的底层数组共享：
// 调用方事后改自己切片不该悄悄改到库里的向量——"喂进来"即快照）。
func cloneVector(v storage.Vector) storage.Vector {
	out := v
	out.Embedding = cloneFloats(v.Embedding)
	out.Payload = clonePayload(v.Payload)
	return out
}

// cloneFloats 拷一份 float 切片（nil/空 → nil）。
func cloneFloats(v []float32) []float32 {
	if len(v) == 0 {
		return nil
	}
	return append([]float32(nil), v...)
}

// clonePayload 拷一份 JSON 载荷（nil/空 → nil）：写时快照、读时回显都拷，
// 库内数据不与任何一方的底层数组共享。
func clonePayload(p json.RawMessage) json.RawMessage {
	if len(p) == 0 {
		return nil
	}
	return append(json.RawMessage(nil), p...)
}
