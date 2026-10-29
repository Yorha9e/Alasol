package storage

import (
	"context"
	"encoding/json"

	"github.com/Yorha9e/alasol/core/graph"
)

// 文档族（G2 的"文档"一类）：**接口先立，实现留 v4**。
//
// # 它装什么
//
// 结构化文档的当前态读模型：项目规则 / 知识条目 / 工作流说明这类"按 (集合, 键) 寻址、
// 带版本、可列举"的内容。与事件日志的分工：**事件日志是事实源**（F5），文档是投影——
// 当前态由事件流 fold 得出，写面只归索引器（G17），所以它不是第二个可变状态源。
//
// # 为什么现在就立接口
//
// ① G2 要求"按用途分若干接口、各自多个实现、可组合"——文档族空着不收口，
// "一组可组合的存储接口"就始终缺一块；② 消费方（v4 的 RAG / 知识注入 / 工作流文档）
// 需要有个稳定形状可以对着写，实现可以晚到；③ 接口分立 + 换实现零改动是可测的
// （酸测试见 compose_test.go：同一消费者跑在两套文档实现上）。
//
// # 事务
//
// 写面复用 `graph.Tx`（各族共用的最小事务面，G2b：不泄漏引擎特性）——
// 一批 Upsert/Delete 要么全成要么全不成，与水位推进同事务（G24 同款口径）。
// DDL 不走 Tx：`EnsureSchema` 由实现自建表（F6/G2b）。

// Doc 是一条结构化文档（读面的元素；纯数据 + JSON tag，可直接过边界）。
type Doc struct {
	// Collection 是集合名（开放字符串，如 "project_rules" / "knowledge"）。
	// 大小写敏感、字节精确（不折叠大小写，陷阱 17 同款纪律）。
	Collection string `json:"collection"`
	// Key 是集合内键（大小写敏感、字节精确）。
	Key string `json:"key"`
	// Body 是文档正文（原始 JSON；框架不解释它的形状——文档形状归消费方）。
	Body json.RawMessage `json:"body,omitempty"`
	// Revision 是版本号（实现分配，单调递增；0 = 未设置）。
	// 写入冲突判定用（同键不同 revision → ErrConflict）。
	Revision int64 `json:"revision"`
	// UpdatedSeq 是最后写入本文档的**事件 seq**（0 = 非事件驱动来源，如种子数据）。
	// 有了它，消费方能判断"这份文档落后事件流多少"（最终一致的诚实信号）。
	UpdatedSeq int64 `json:"updated_seq"`
	// UpdatedAt 是 RFC 3339 时间戳（诊断用）。
	UpdatedAt string `json:"updated_at,omitempty"`
}

// DocQuery 是文档列举查询。
type DocQuery struct {
	// Collection 是集合名（空 = 不限集合）。
	Collection string
	// Prefix 是键前缀（空 = 不限；字节前缀，大小写敏感）。
	Prefix string
	// Limit 是结果上限（0 = 实现默认；负值 → ErrBadQuery）。
	Limit int
}

// Documents 是文档族的**读面**（v4 保留位；本刀只立接口）。
//
// 实现必须并发安全（宿主会从多个 HTTP handler 并发读）。
type Documents interface {
	// Get 取一条文档（不存在 → ErrNotFound）。
	Get(ctx context.Context, collection, key string) (Doc, error)
	// List 列举文档（按 (collection, key) 排序，确定性输出）。
	List(ctx context.Context, q DocQuery) ([]Doc, error)
}

// DocumentProjection 是文档族的**写面**（G17：派生表只归索引器写；v4 保留位）。
//
// 与图投影写面（core/graph.GraphProjection）同构：建表 → 在一个事务里写 → 提交后通知。
// 索引器在它自己的投影事务里调用 Upsert/Delete（投影 + 水位同事务原子，G24）。
type DocumentProjection interface {
	// EnsureSchema 建文档投影表（幂等）。DDL 归实现侧（F6/G2b：core 契约里没有 SQL 文本，
	// 也不替调用方决定建表落在哪个事务里）。
	EnsureSchema(ctx context.Context) error
	// Upsert 在一个事务里写入/更新一批文档。
	//
	// 冲突口径：Doc.Revision 非 0 且与库里现存版本**不一致** → 整批拒绝并返回 ErrConflict
	// （调用方应先 Get 拿最新版本；LWW 覆盖只发生在 revision 相同或为 0 时）。
	Upsert(ctx context.Context, tx graph.Tx, docs []Doc) error
	// Delete 在一个事务里删除一批文档（不存在 → 幂等跳过，不算冲突）。
	Delete(ctx context.Context, tx graph.Tx, collection string, keys []string) error
	// Clear 清空文档投影（索引器 Rebuild 用，G24②）。
	Clear(ctx context.Context, tx graph.Tx) error
}
