package storage

import (
	"context"
	"encoding/json"

	"github.com/Yorha9e/alasol/contracts"
)

// 向量族（G2d）：**RAG 的检索保留位**。接口先立，实现留 v4。
//
// # 它装什么（G2d 原文口径）
//
// 未来 RAG 的向量检索，**独立于指针解析**（指针解析是确定性的：路径 / seq 区间一一对应；
// 向量检索是概率性的：相似度排序）。两者服务不同问题，别混：
//
//	指针解析（agentlink Inquiry / refs 核验）："这条引用指向的原文是什么"——要精确；
//	向量检索（本族）："和这个问题最像的几段是什么"——要召回。
//
// # 为什么现在就立接口
//
// ① G2 的五族里向量族空着不收口，"一组可组合的存储接口"就不完整；② 消费端
// （v4 的 Retriever.retrieve 接入点）需要一个**不绑定引擎**的形状——换 embedding 模型
// 只是换实现，检索调用点不动；③ 概率性检索最怕"形状没定就上车"：top-k / 过滤 / 覆盖度
// 这些口径一旦被实现焊死，换引擎时全要返工。
//
// # 与事实源的关系
//
// 向量条目是**可整体重建的缓存**：`SourceRef` 指回事件流里的原文（`e:<seq>` / 路径），
// 索引器据此从事件流重建。所以向量库丢了不丢事实（F5 不受影响）——这也是它敢用
// 概率性检索的前提：错了大不了重建，不会污染唯一事实源。
//
// # 实现留 v4
//
// 本刀只立接口。嵌入向量由调用方算好喂进来（框架不算 embedding——那是 provider/模型层的
// 事，且换模型就要重算，属于实现细节）。

// Vector 是一条待检索的向量条目。
type Vector struct {
	// ID 是条目 id（ULID；由调用方分配——它是 (源, 分块) 的确定性键，重算 embedding
	// 后要能覆盖同一条，所以不能由实现随机发号）。
	ID contracts.Ulid `json:"id"`
	// Embedding 是嵌入向量（维度由实现/模型定；空 = 只有载荷没有向量，
	// 实现可拒——ErrBadQuery）。
	Embedding []float32 `json:"embedding"`
	// Payload 是附带的结构化载荷（原始 JSON：分块原文 / 元数据；框架不解释）。
	Payload json.RawMessage `json:"payload,omitempty"`
	// SourceRef 是来源指针（`e:<seq>` / 路径 / blob 引用 id）——**可整体重建的锚**：
	// 向量库整体丢失时按它从事件流重建，不丢事实（F5）。
	SourceRef string `json:"source_ref,omitempty"`
}

// VectorQuery 是一次检索请求。
type VectorQuery struct {
	// Embedding 是查询向量（维度须与库中一致；空 → ErrBadQuery）。
	Embedding []float32
	// K 是取前几条（0 = 实现默认上限；负值 → ErrBadQuery）。
	K int
	// Filter 是可选的精确匹配过滤（键/值都按字节精确，大小写敏感）。
	//
	// **实现不支持过滤必须报 ErrUnsupported**，不得静默忽略——
	// 静默忽略会让调用方以为过滤生效了（检索到不该检索的内容）。
	Filter map[string]string
}

// VectorHit 是一条检索结果。
type VectorHit struct {
	// ID 是条目 id（与 Vector.ID 对应）。
	ID contracts.Ulid `json:"id"`
	// Score 是相似度（口径由实现定：余弦/内积/距离取反；**跨实现不可比**——
	// 换引擎后同一个 Score 数值含义可能变，消费端只可在同一次检索内排序用）。
	Score float32 `json:"score"`
	// Payload 是条目附带载荷（原文 JSON 回显）。
	Payload json.RawMessage `json:"payload,omitempty"`
	// SourceRef 是来源指针（供消费端回事件流核验/取原文）。
	SourceRef string `json:"source_ref,omitempty"`
	// Truncated 表示本次结果被 K / 实现上限截断（诚实信号：召回可能不全，
	// 消费端不得把"没检索到"当成"不存在"）。
	Truncated bool `json:"truncated"`
}

// Retriever 是向量族的**读面**（G2d：RAG 的 retrieve 保留位；v4 保留位）。
//
// 实现必须并发安全；必须是**概率性、可重建**的——不得成为事实源（F5）。
type Retriever interface {
	// Retrieve 按查询向量取 top-k（按 Score 降序；结果带 Truncated 诚实信号）。
	Retrieve(ctx context.Context, q VectorQuery) ([]VectorHit, error)
}

// VectorIndex 是向量族的**写面**（只归索引器写，G17 同款单写者纪律）。
type VectorIndex interface {
	// Upsert 写入/更新一批向量条目（同 ID 覆盖——embedding 重算的标准路径）。
	Upsert(ctx context.Context, vs []Vector) error
	// Delete 按 id 删一批（不存在 → 幂等跳过）。
	Delete(ctx context.Context, ids []contracts.Ulid) error
	// Clear 清空整个向量库（索引器 Rebuild 用，G24②；与文档/图同口径）。
	Clear(ctx context.Context) error
}
