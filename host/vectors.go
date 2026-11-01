package host

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/runtime/store/vecmem"
)

// 本文件是 G2d 向量族（RAG）在宿主的**接入面**（v4-3-B）：嵌入提供者槽位 +
// `vector_search` 工具 + 喂数据/检索两个宿主方法（HTTP 面见 http.go 的
// serveVectorSearch / serveVectorIngest）。
//
// # 框架不算 embedding
//
// 文本 → 向量的转换**不在框架里**（core/storage/vector.go 的口径：那是 provider/模型层的
// 事，换模型就要重算）。宿主只认一个窄接口 `EmbeddingProvider`：
//
//	生产 = nil（未接）：vector_search **不注册**、/api/vectors 检索/ingest 回 503 说明；
//	测试/接入方 = 注入实现（fake / 真 embedding 端点）。
//
// 于是"未配置 embedding"与"未开向量"都是**零行为变化**（酸测试）：前者连工具都不出现。
//
// # 单写者纪律（G17）
//
// 向量库的写只经宿主这几个入口（IngestVectors 喂数据；ClearVectors/RebuildVectors 供
// 重建，G24②），都走 storage.VectorIndex；vecmem 自身并发安全（一把 RWMutex）。库是
// **可整体重建的缓存**（SourceRef 指回事件流），丢了不丢事实（F5）。
//
// # 重建窗口有闸（复审轮 C3）
//
// RebuildVectors = Clear + Upsert **两步**，本身没有原子性。宿主侧用一把**重建闸**
// （vectorGate：RWMutex + 在途重建计数）把重建窗口与并发读写隔开，并把"重建中"如实
// 回给调用方（见 vectorGate 与 RebuildVectors 的口径注释）。
//
// # 喂数据有批上限（复审轮 C4）
//
// 单批 ingest 的条数上限 = maxVectorTexts（512）：一条文本 = 一次 embedding 提供者
// 调用（批内串行），不设上限就等于让一个 HTTP 请求串起 N 次外部调用。超出 → 400。

// ToolVectorSearch 是 RAG 检索工具名（框架级常量，供宿主注册与策略规则引用；
// 与 core/compaction 的 ToolRecall/ToolStage 同性质）。
const ToolVectorSearch = "vector_search"

// EmbeddingProvider 把一段文本转成嵌入向量（**宿主侧的 provider 槽位**）。
//
// 框架不算 embedding（换模型就要重算，属于实现细节）——本接口是唯一的接入口：
// 生产未接 = nil（工具不注册、检索/ingest 端点 503）；测试注入 fake。
// 实现必须对同一文本返回**同维度**的向量（vecmem 按首条维度锁库）。
type EmbeddingProvider interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// 向量面的三个错误（HTTP 层据此映射 404 / 503 / 400；库内测试也可判 errors.Is）。
var (
	// errVectorsDisabled = 向量引擎未启用（Config.VectorsEnabled=false）：
	// 不建库、不注册工具、/api/vectors 两个端点都不注册（404）。
	errVectorsDisabled = errors.New("host: 向量引擎未启用（Config.VectorsEnabled=false）")
	// errNoEmbeddingProvider = 向量库开着但没配 embedding 提供者：
	// 库建了却没有任何东西能进得去（喂数据要向量化）→ 检索/ingest 一律 503 说明。
	errNoEmbeddingProvider = errors.New("host: 未配置 embedding 提供者（Config.EmbeddingProvider=nil；" +
		"框架不算 embedding——文本转向量由宿主/provider 层接）")
	// errVectorRebuilding = 重建窗口内到来的检索（复审轮 C3）：
	// 库正处在 Clear 与 Upsert 之间——此刻检索会看到**空库/半重建库**（误导：像"没有
	// 命中"），故**不回半成品**：如实报"重建中"，调用方重建完成后重试（HTTP 503）。
	errVectorRebuilding = errors.New("host: 向量库正在重建中（Clear+Upsert 窗口）——" +
		"重建完成后再检索（此刻不返回重建中的空库/半重建结果）")
)

// maxVectorTexts 是**单批 ingest 的条数上限**（复审轮 C4）。
//
// 为什么要有上限：一条文本 = 一次 embedding 提供者调用（批内**串行**——提供者调用是
// 外部 I/O，并发只会把外部服务的压力转嫁出去），不设上限就等于让一个 HTTP 请求串起
// N 次外部调用（一次请求占着连接不放）。512 条 = 一次请求的合理批量；更多请分批喂
// （同一 (source_ref, 文本) 重喂覆盖同一条，分批不影响最终库内容）。
//
// 超出 → IngestVectors 报错并包 storage.ErrBadQuery（HTTP 400，点名上限）。
const maxVectorTexts = 512

// ---------------------------------------------------------------------------
// 重建闸（vectorGate）：Clear+Upsert 的重建窗口与并发读写隔开（复审轮 C3）
// ---------------------------------------------------------------------------

// vectorGate 是向量面的**重建闸**：一把 RWMutex + 在途重建计数。
//
// # 为什么需要它
//
// RebuildVectors = Clear + Upsert **两步**，两步之间库是**空的**（清完还没灌完）。
// 不加闸时：重建窗口内的检索会看到**空库**——那长得像"库里没有相关内容"（RAG 里最
// 误导的一种空）；重建失败后库里还可能**残留并发写的条目**（半重建）。两者都不是
// "库是空的"这种可解释状态。
//
// # 加闸后的口径
//
//   - 重建跨 Clear+Upsert 持**写锁** → 并发读/写不与重建交错（不出现"重建到一半"的库）；
//   - 重建**在途**时到来的检索**回"重建中"**（errVectorRebuilding；HTTP 503）——
//     不返回空库/半重建结果，也不阻塞到重建结束（重建可能很久，堵着检索更糟）；
//   - 重建**失败**时：Clear 失败 → 库**保持原样**（旧内容还在）；Clear 成功而 Upsert
//     失败 → 库**为空**（vecmem 整批不落）。两种都不是半重建；
//   - 重建**不是原子替换**（没有 staging + swap）：闸只保证"不与并发读写交错"，**不**
//     保证"失败自动回滚到重建前的库"——清空已生效，重建前的库不会自动回来，调用方
//     需重试重建（这就是"失败后库**可能**非空"的准确来源：Clear 失败时旧库还在，
//     或失败后又有并发写留存）。
//
// # 零值与 nil
//
// 零值可用（mu 零值即可用；rebuilding 计数从 0 起）。nil 接收者按"无闸"处理
// （直接跑回调）——装配残缺时不让宿主 panic。
type vectorGate struct {
	// mu 护重建与读写的交错：重建持写锁，读持读锁。
	mu sync.RWMutex
	// rebuilding 是在途重建计数（>0 = 正在重建）：读面据此回"重建中"。
	rebuilding atomic.Int32
}

// read 在读锁下跑 fn；**重建在途** → 直接回 errVectorRebuilding（不排队等重建）。
func (g *vectorGate) read(fn func() ([]storage.VectorHit, error)) ([]storage.VectorHit, error) {
	if g == nil {
		return fn()
	}
	if g.rebuilding.Load() > 0 {
		return nil, errVectorRebuilding
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return fn()
}

// write 在写锁下跑 fn（写面：ingest / clear / rebuild 串行化）。
func (g *vectorGate) write(fn func() error) error {
	if g == nil {
		return fn()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return fn()
}

// beginRebuild / endRebuild 标记重建窗口（在途计数；并发重建各自计数，归零才算不在途）。
func (g *vectorGate) beginRebuild() {
	if g == nil {
		return
	}
	g.rebuilding.Add(1)
}

func (g *vectorGate) endRebuild() {
	if g == nil {
		return
	}
	g.rebuilding.Add(-1)
}

// ---------------------------------------------------------------------------
// vector_search 工具（RAG 检索；参照 compaction recall 工具的形态）
// ---------------------------------------------------------------------------

// strPtrVector 造一个 *string（contracts.Tool.Description 是指针；host 内的小助手，
// 与 core/compaction 的 strPtr 同性质——不新增导出面）。
func strPtrVector(s string) *string { return &s }

// vectorTextResult 造一个单文本块的 ToolResult（与 core/compaction 的 textResult 同形状；
// 那是 compaction 包内的助手，这边不复用也不导出第二份）。
func vectorTextResult(text string) contracts.ToolResult {
	t := text
	return contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &t}}}
}

// vectorSearchDescriptor 是 vector_search 工具的描述（进模型可见集）。
//
// 为什么收 query **文本**而不是 embedding：模型给不了向量（框架不算 embedding）。
// 工具拿文本 → 宿主经 EmbeddingProvider 转向量 → vecmem 检索（见 Execute）。
func vectorSearchDescriptor() contracts.Tool {
	return contracts.Tool{
		Name: ToolVectorSearch,
		Description: strPtrVector(
			"**语义向量检索（RAG）**：用一段自然语言从向量库里取回最像的几段原文。" +
				"给 query 文本（不是向量——框架不算 embedding，宿主负责转向量）；" +
				"可选 k（取前几条，缺省用宿主默认）与 filter（对条目 payload 顶层字符串字段做精确匹配，如 {\"kind\":\"note\"}）。" +
				"返回命中条目的 id / 相似度 / source_ref / payload——按 source_ref 可回事件流取现场原文。" +
				"注意：Score 只是**同一次检索内**的排序依据（口径由向量实现定，跨实现不可比）；" +
				"结果带 truncated=true 表示召回被截断，**不得**把\"没检索到\"当成\"不存在\"。",
		),
		Schema:     json.RawMessage(vectorSearchToolSchema),
		SideEffect: contracts.SideEffectNone, // 只读检索，无副作用
		Replay:     contracts.ReplayKindSafe, // 幂等只读，可安全重放
		Idempotent: true,
	}
}

const vectorSearchToolSchema = `{
  "type": "object",
  "properties": {
    "query": {
      "type": "string",
      "description": "查询文本（自然语言；宿主经 embedding 提供者转成向量，框架自己不算 embedding）。"
    },
    "k": {
      "type": "integer",
      "description": "取前几条（缺省 = 宿主默认 top_k；负值拒绝）。"
    },
    "filter": {
      "type": "object",
      "additionalProperties": { "type": "string" },
      "description": "可选：对条目 payload 的顶层字符串字段做字节精确匹配（AND 语义），如 {\"kind\":\"note\",\"lang\":\"go\"}。"
    }
  },
  "required": ["query"]
}`

// vectorSearchExecutor 执行 vector_search：文本 → embedding（宿主）→ top-k（向量库）。
type vectorSearchExecutor struct {
	embed EmbeddingProvider
	retr  storage.Retriever
	// gate 是重建闸（复审轮 C3）：工具面的检索与 HTTP 面走**同一把闸**——重建在途时
	// 工具也回"重建中"，不把重建窗口里的空库当成"没有命中"报给模型。
	gate *vectorGate
}

// 编译期证明：*vectorSearchExecutor 就是 core.ToolExecutor（与 recall 执行器同款）。
var _ core.ToolExecutor = (*vectorSearchExecutor)(nil)

// newVectorSearchExecutor 构造 vector_search 执行器（nil embed / nil retr → 所有调用
// 都回"不可用"说明，不 panic：装配残缺时要让模型看到"为什么用不了"）。
//
// gate 可为 nil（无闸：直接读库）；装配面传宿主的重建闸。
func newVectorSearchExecutor(embed EmbeddingProvider, retr storage.Retriever, gate *vectorGate) *vectorSearchExecutor {
	return &vectorSearchExecutor{embed: embed, retr: retr, gate: gate}
}

// Execute 执行一次 vector_search（X1 路径由 loop 负责落 intent/completed）。
func (e *vectorSearchExecutor) Execute(ctx context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	var args struct {
		Query  string            `json:"query"`
		K      int               `json:"k"`
		Filter map[string]string `json:"filter"`
	}
	if err := json.Unmarshal(intent.Arguments, &args); err != nil {
		return contracts.ToolResult{}, fmt.Errorf("vector_search 参数不是合法 JSON 对象: %w（应形如 {\"query\":\"…\"}）", err)
	}
	if strings.TrimSpace(args.Query) == "" {
		return contracts.ToolResult{}, errors.New("vector_search 缺少 query 参数（查询文本；应形如 {\"query\":\"……\"}）")
	}
	if e.embed == nil || e.retr == nil {
		return vectorTextResult("vector_search: 未配置 embedding 提供者（宿主没接文本转向量）或向量库未启用，" +
			"无法做语义检索——请精确引用（路径 / e:<seq> 区间）而不是相似度检索"), nil
	}
	emb, err := e.embed.Embed(ctx, args.Query)
	if err != nil {
		return contracts.ToolResult{}, fmt.Errorf("vector_search: 文本转向量失败: %w", err)
	}
	if len(emb) == 0 {
		return contracts.ToolResult{}, errors.New("vector_search: embedding 提供者返回空向量（无法计算相似度）")
	}
	hits, err := e.gate.read(func() ([]storage.VectorHit, error) {
		return e.retr.Retrieve(ctx, storage.VectorQuery{Embedding: emb, K: args.K, Filter: args.Filter})
	})
	if err != nil {
		return contracts.ToolResult{}, fmt.Errorf("vector_search: 检索失败: %w", err)
	}
	return vectorTextResult(renderVectorHits(args.Query, args.K, hits)), nil
}

// renderVectorHits 把命中列表渲染成给模型看的文本（口径写进文案：Score 不可跨实现比）。
func renderVectorHits(query string, k int, hits []storage.VectorHit) string {
	var b strings.Builder
	fmt.Fprintf(&b, "vector_search(%q) 命中 %d 条", query, len(hits))
	if k > 0 {
		fmt.Fprintf(&b, "（k=%d）", k)
	}
	b.WriteString("：\n")
	for i, h := range hits {
		fmt.Fprintf(&b, "%d. score=%.4f id=%s", i+1, h.Score, string(h.ID))
		if h.SourceRef != "" {
			fmt.Fprintf(&b, " source_ref=%s", h.SourceRef)
		}
		if len(h.Payload) > 0 {
			fmt.Fprintf(&b, " payload=%s", string(h.Payload))
		}
		if h.Truncated {
			b.WriteString(" truncated=true")
		}
		b.WriteString("\n")
	}
	if len(hits) == 0 {
		b.WriteString("（没有命中——注意：空结果不等于\"不存在\"，可能只是相似度不够或库还是空的）\n")
	}
	b.WriteString("（Score 口径由向量实现定，**只在同一次检索内**可排序；truncated=true 表示召回被截断）")
	return b.String()
}

// ---------------------------------------------------------------------------
// 宿主方法：喂数据面（vector_index）+ 检索面
// ---------------------------------------------------------------------------

// IngestVectors 把一批文本喂进向量库（**手动 ingest 入口**，RAG 的"喂数据面"）。
//
// 每条文本经 EmbeddingProvider 向量化后 Upsert；Vector.ID 按
// (source_ref, 分块文本内容) **确定性派生**——同一来源同一段重喂覆盖同一条
// （接口口径：ID 是 (源,分块) 的确定性键，重算 embedding 可覆盖）；**不含批内序号**
// （批内序号跨批次会漂移 → 第二批的第 0 段撞掉第一批的第 0 段，静默覆盖无关条目）。
// 派生公式见 vectorEntryID（**逐字一致**：两段各带长度前缀，无歧义）。
// payload 落 {"text": 原文}（框架不解释；Filter 按它的顶层字符串字段匹配）。
//
// source_refs 的长度口径：
//   - 可以**比 texts 短**（按序号对齐，**缺的记空串**——"这段没有来源指针"是合法状态，
//     不是数据错了）；
//   - 比 texts **长** → error（包 storage.ErrBadQuery → HTTP 400：来源对不上就是数据错了）。
//
// **条数上限**（复审轮 C4）：单批 ≤ maxVectorTexts（512）条——一条文本 = 一次提供者
// 调用（批内串行），不设上限 = 一个请求串起 N 次外部调用。超出 → error（包
// storage.ErrBadQuery → HTTP 400，点名上限）；更多请分批喂（分批不影响最终库内容）。
//
// **空文本**（F4）：texts 里有**空串**（字节精确 `text == ""`）→ 预校验拦下（**在调用
// 提供者之前**：不让同批前面的几条白跑外部调用），包 storage.ErrBadQuery → HTTP 400
// bad_vector_query（客户端数据错，不是"真出事了"）。空白串不算空（宿主不替用户改数据）。
//
// 返回落库的条目 id（与入参顺序一致）。
//
// 向量库未启用 → errVectorsDisabled；未配 embedding 提供者 → errNoEmbeddingProvider。
func (h *Hub) IngestVectors(ctx context.Context, texts []string, sourceRefs []string) ([]contracts.Ulid, error) {
	if h == nil {
		return nil, errors.New("host: Hub 为 nil")
	}
	if h.vecIndex == nil {
		return nil, errVectorsDisabled
	}
	if h.embed == nil {
		return nil, errNoEmbeddingProvider
	}
	if len(texts) == 0 {
		return nil, nil
	}
	if len(texts) > maxVectorTexts {
		return nil, fmt.Errorf("host: 单批 texts 超过上限 %d 条（得到 %d 条）"+
			"——一条文本一次提供者调用（批内串行），请分批喂: %w",
			maxVectorTexts, len(texts), storage.ErrBadQuery)
	}
	if len(sourceRefs) > len(texts) {
		return nil, fmt.Errorf("host: source_refs（%d 条）比 texts（%d 条）还长"+
			"（来源对不上就是数据错了）: %w", len(sourceRefs), len(texts), storage.ErrBadQuery)
	}
	// 空文本预校验（F4）：空串没有可嵌入的内容——喂进去只会被提供者拒（一次白付的外部
	// 调用），而那个错在 HTTP 面上会落成 **500**（"真出事了"），可它其实是**客户端数据
	// 错** → 包 storage.ErrBadQuery → 400 bad_vector_query（客户端能改）。
	//
	// **字节精确**（不做 TrimSpace）：判据就是 `text == ""`。空白串（" "）是一个**待嵌入
	// 的合法文本**——它该不该被拒由提供者/端点判，宿主不替用户改数据（悄悄 trim 掉用户
	// 的内容更糟：入库的原文与喂进去的不一致）。
	//
	// 预校验放在**调用提供者之前**：一条空文本不该让同批前面的几条白跑一遍外部调用。
	for i, text := range texts {
		if text == "" {
			return nil, fmt.Errorf("host: 第 %d 条文本为空（空文本没有可嵌入的内容）: %w",
				i, storage.ErrBadQuery)
		}
	}
	idx := h.vecIndex // 写面（与 h.vecs 同一实例；vecmem 两面都实现）
	out := make([]contracts.Ulid, 0, len(texts))
	batch := make([]storage.Vector, 0, len(texts))
	for i, text := range texts {
		ref := ""
		if i < len(sourceRefs) {
			ref = sourceRefs[i]
		}
		emb, err := h.embed.Embed(ctx, text)
		if err != nil {
			return nil, fmt.Errorf("host: 第 %d 条文本转向量失败: %w", i, err)
		}
		if len(emb) == 0 {
			return nil, fmt.Errorf("host: 第 %d 条文本的 embedding 为空（提供者实现有问题）", i)
		}
		id := vectorEntryID(ref, text)
		payload, err := json.Marshal(map[string]string{"text": text})
		if err != nil {
			return nil, fmt.Errorf("host: 序列化第 %d 条文本载荷: %w", i, err)
		}
		out = append(out, id)
		batch = append(batch, storage.Vector{ID: id, Embedding: emb, Payload: payload, SourceRef: ref})
	}
	// 写入在**重建闸**的写锁下（与重建/清空串行）：写与写不交错，库不会出现半重建态。
	if err := h.vecGate.write(func() error { return idx.Upsert(ctx, batch) }); err != nil {
		return nil, fmt.Errorf("host: 写入向量库: %w", err)
	}
	return out, nil
}

// SearchVectors 用一段文本检索向量库（RAG 检索面；vector_search 工具与 HTTP 端点共用）。
//
// k <= 0 = 向量实现默认上限；filter 对条目 payload 顶层字符串字段做字节精确匹配（AND）。
// 向量库未启用 → errVectorsDisabled；未配 embedding 提供者 → errNoEmbeddingProvider；
// 查询文本为空 → error（包 storage.ErrBadQuery → HTTP 400；fail-close：空查询只会返回
// 一堆不相关的东西，客户端改一下就好——不是 500）。
//
// **重建在途** → errVectorRebuilding（复审轮 C3）：此刻库里正处在 Clear 与 Upsert 之间，
// 检索会看到空库/半重建库——那不是"没有命中"，故如实报"重建中"（HTTP 503）。
func (h *Hub) SearchVectors(ctx context.Context, query string, k int, filter map[string]string) ([]storage.VectorHit, error) {
	if h == nil {
		return nil, errors.New("host: Hub 为 nil")
	}
	if h.vecs == nil {
		return nil, errVectorsDisabled
	}
	if h.embed == nil {
		return nil, errNoEmbeddingProvider
	}
	if strings.TrimSpace(query) == "" {
		// 空查询 = 客户端数据错 → 包 storage.ErrBadQuery（HTTP 400），不是 500
		//（F4：空查询只会返回一堆不相关的东西，客户端改一下就好）。
		return nil, fmt.Errorf("host: 查询文本不能为空（q）: %w", storage.ErrBadQuery)
	}
	emb, err := h.embed.Embed(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("host: 查询文本转向量失败: %w", err)
	}
	if len(emb) == 0 {
		return nil, errors.New("host: embedding 提供者返回空向量（无法计算相似度）")
	}
	hits, err := h.vecGate.read(func() ([]storage.VectorHit, error) {
		return h.vecs.Retrieve(ctx, storage.VectorQuery{Embedding: emb, K: k, Filter: filter})
	})
	if err != nil {
		return nil, fmt.Errorf("host: 向量检索失败: %w", err)
	}
	return hits, nil
}

// effectiveVectorTopK 返回本次检索的**生效 top-k 上限**（k 未指定 / <= 0 时用）。
//
// 口径：Config.VectorsTopK > 0 → 它；否则 vecmem.DefaultK（10）——与装配时
// vecmem.New(cfg.VectorsTopK) 的默认口径一致（New 里 <=0 → DefaultK）。
//
// 为什么要"生效上限"而不是"请求的 k"：响应里回显 k=0 对消费方毫无意义（0 = 没指定），
// 回显**实际生效的上限**才可解释（"这次最多取 N 条"）。
func (h *Hub) effectiveVectorTopK() int {
	if h != nil && h.cfg.VectorsTopK > 0 {
		return h.cfg.VectorsTopK
	}
	return vecmem.DefaultK
}

// ClearVectors 清空整个向量库（**重建入口**：先清后灌；与图投影的 RebuildGraph 同口径）。
//
// 用途：向量库是**可整体重建的缓存**（SourceRef 指回事件流，丢了重建不丢事实 F5），
// 重建前先清干净（连维度锁一起清 → 换维度的模型也能重建，G24②）。
// 向量库未启用 → errVectorsDisabled。
//
// 清空在**重建闸**的写锁下（与 ingest/重建串行）：清空不是"重建窗口"（不置重建标记），
// 但仍不与并发写交错——清完立刻有并发写落进来是合法的（库非空），只是不会与清空**交错**。
func (h *Hub) ClearVectors(ctx context.Context) error {
	if h == nil {
		return errors.New("host: Hub 为 nil")
	}
	if h.vecIndex == nil {
		return errVectorsDisabled
	}
	if err := h.vecGate.write(func() error { return h.vecIndex.Clear(ctx) }); err != nil {
		return fmt.Errorf("host: 清空向量库: %w", err)
	}
	return nil
}

// RebuildVectors 用**自带 id** 的条目重建向量库（清空 + 灌入；绕过 IngestVectors 的
// id 派生——重建方自己知道每段的稳定键，例如 (源, 分块序号)）。
//
// 语义：先 Clear（连维度锁一起清 → 重建可以换维度的模型，G24②），再 Upsert(items)。
// items 为空 → 等价于清空（幂等）。
//
// # 非原子 + 重建闸（复审轮 C3：口径写明，不和稀泥）
//
// Clear 与 Upsert 是**两步**，**不是原子替换**（没有 staging + swap）。宿主用一把
// **重建闸**（vectorGate）把重建窗口与并发读写隔开：
//
//   - 重建跨 Clear+Upsert 持**写锁** → 并发读/写不与重建交错（库里不会出现半重建态）；
//   - 重建**在途**时到来的检索回**"重建中"**（errVectorRebuilding；HTTP 503）——
//     不返回重建窗口里的空库（那长得像"没有相关内容"，RAG 里最误导的一种空）；
//   - **失败口径**：Clear 失败 → 库**保持原样**（旧内容还在，**可能非空**）；
//     Clear 成功而 Upsert 失败 → 库**为空**（vecmem 整批不落）。两种都不是半重建；
//   - 重建**不会自动回滚**到重建前的库（清空已生效）——失败后调用方需重试重建；
//     重试前若有并发写落进库里，那个库**可能非空**（"可能非空"的准确来源）。
//
// embedding 由调用方算好（**框架不算 embedding**——与 IngestVectors 同一纪律）。
// 向量库未启用 → errVectorsDisabled。
func (h *Hub) RebuildVectors(ctx context.Context, items []storage.Vector) error {
	if h == nil {
		return errors.New("host: Hub 为 nil")
	}
	if h.vecIndex == nil {
		return errVectorsDisabled
	}
	// 重建窗口标记**先置后取锁**：重建排队等锁期间到来的检索也回"重建中"，
	// 而不是先看到旧库、再看到空库（两个答案都比"重建中"更误导）。
	h.vecGate.beginRebuild()
	defer h.vecGate.endRebuild()
	err := h.vecGate.write(func() error {
		if cerr := h.vecIndex.Clear(ctx); cerr != nil {
			return fmt.Errorf("host: 清空向量库: %w", cerr)
		}
		if len(items) == 0 {
			return nil // 空重建 = 清空（幂等）
		}
		if uerr := h.vecIndex.Upsert(ctx, items); uerr != nil {
			return fmt.Errorf("host: 写入重建条目: %w", uerr)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// vectorEntryID 由 (source_ref, 分块文本内容) 确定性派生条目 id。
//
// # 口径（Vector.ID 的接口契约："(源,分块) 的确定性键；重算 embedding 后要能覆盖同一条"）
//
//   - 键 = **分块内容哈希**，**不含批内序号**：批内序号随批次重排而漂移（第二批的第 0 段
//     会撞掉第一批的第 0 段 → 静默覆盖无关条目；F3 修）；
//   - 同一 (source_ref, 文本) 重喂 = 同一 id → **覆盖**同一条（重算 embedding 的标准路径）；
//   - 不同 (source_ref, 文本) → 不同 id（不撞）。
//
// # 派生公式（**逐字一致**：本文 = 实现 = 07/11 文档口径）
//
//	id = Crockford26( SHA-256( "vec\x00" ‖ u64be(len(source_ref)) ‖ source_ref ‖
//	                          u64be(len(text)) ‖ text )[:16] )
//
// 其中：
//   - "vec\x00" 是**域前缀**（与本框架其它短哈希隔开，防跨用途撞键）；
//   - u64be(n) = n 的 **8 字节大端无符号**表示（binary.BigEndian.PutUint64）；
//   - ‖ = 字节串拼接；
//   - 取 SHA-256 的**前 128bit** 编 Crockford base32（core.EncodeCrockford26，框架 26
//     编码的单一实现处）→ 恒为合法 ULID 形态。
//
// # 为什么两段都带长度前缀（**无歧义编码**）
//
// 旧口径是 SHA-256("vec\x00" + source_ref + "\x00" + text)——**裸 NUL 当分隔符**，
// 输入**不是单射**：ref="a\x00b"、text="c" 与 ref="a"、text="b\x00c" 拼出**同一串**
// → **同一个 id** → 后写的那条静默覆盖先写的那条（公开 HTTP 面 \u0000 可达：ref/text
// 都是任意字符串）。改成"长度前缀 + 内容"后，(ref,text) → 字节串是**单射**
// （长度自解释分段），两段内容里**含 NUL 也不会撞 id**。
func vectorEntryID(sourceRef, text string) contracts.Ulid {
	h := sha256.New()
	h.Write([]byte(vectorIDDomain))
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(sourceRef)))
	h.Write(lenBuf[:])
	h.Write([]byte(sourceRef))
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(text)))
	h.Write(lenBuf[:])
	h.Write([]byte(text))
	sum := h.Sum(nil)
	return contracts.Ulid(core.EncodeCrockford26(sum[:16]))
}

// vectorIDDomain 是条目 id 派生的**域前缀**（与其它短哈希隔开；见 vectorEntryID 公式）。
const vectorIDDomain = "vec\x00"
