package storage

import (
	"errors"

	"github.com/Yorha9e/alasol/core"
	coregraph "github.com/Yorha9e/alasol/core/graph"
)

// 族别名：把"一组可组合的存储接口"（G1/G2）收到一个名字下面。
//
// **别名而不是新定义**（F7：只增不删、不改义；也别把定义处搬来搬去）：
//   - `EventLog` 就是 `core.Store`（v0 起就在那儿，Append/ReadFrom 一字不改）；
//   - `Graph` / `GraphProjection` / `Tx` 就是 `core/graph` 那三个（v2-S2 落地）。
//
// 消费方可以只 import 本包拿到全族形状；也可以直接 import 定义处（两条路编译期等价）。
type (
	// EventLog 是**事件日志**族（G1：Store 形态 = 事件日志追加 + 投影读模型）。
	//
	// Append 以 contracts.Event.ID 幂等去重（G11）；seq 由实现分配（G9/G10）；
	// durable=false 的流事件永不到达它（F8）。
	EventLog = core.Store

	// Graph 是**图**族的读面（G2c/G18：一跳/两跳走持久层倒排、多跳走内存 BFS）。
	Graph = coregraph.Graph

	// GraphProjection 是**图**族的写面（G17：派生表只归索引器写；G22：边来自工具产出）。
	GraphProjection = coregraph.GraphProjection

	// Tx 是各族写面共用的**事务最小面**（G2b：不泄漏引擎特性——只有
	// "执行一条参数化语句"这个动作，语句文本由实现侧持有）。
	Tx = coregraph.Tx
)

// Set 把几族存储接口**组合**到一个 session / 一个宿主上（G2：可组合）。
//
// 每个槽位是**独立的接口**（不是一张大接口）：挂哪些由配置/工作流决定，其余留空：
//   - 只挂 EventLog = 与 v0/v1 完全等价（零图开销、零 blob、零文档——G2f 同款口径）；
//   - EventLog + Graph = v2-S2 的图投影形态；
//   - EventLog + Graph + Blob = 大工具输出外置（G2e）之后的形态。
//
// **为什么用结构体而不是接口**：组合是"装配事实"（宿主在 New 时按配置决定挂什么），
// 不是"能力协商"——用接口反而要把五个可选槽位写成五个方法，消费方还要判 nil。
// 槽位类型全是接口，所以换实现仍是零改动（酸测试见 compose_test.go）。
//
// # 读面与写面：挂的是读面，写面是**可选能力**（G17）
//
// 槽位挂的是各族的**读面**（Graph / Documents / Retriever）；写面（GraphProjection /
// DocumentProjection / VectorIndex）是**另一套接口**，**不是每个实现都提供**：
// 写面只归索引器写（G17），只读实现（视图、只读副本、外部系统的读端）天然没有写面。
//
// 因此取写面**必须带 ok**：`GraphWriter() (GraphProjection, bool)` 等三个取面器。
// **不许硬转型**（`s.Graph.(GraphProjection)`）：组合面恰恰允许"只挂读面"
// （事件日志 + 只读图 = 合法组合），硬转型在那种组合上直接 **panic**——
// 消费方（索引器）本来只想读，却因为一个用不上的写面崩掉。
// 口径一句话：**读面必有，写面有则用、无则跳过**（没有写面时索引器那条路就不跑）。
//
// 本结构是**形状样板 + 测试载体**：v2 的宿主（host）按自己的装配代码直接持各族实现
// （它本来就是组合根，09），不必绕道本结构；v4 落文档/blob/向量实现时，照它挂即可。
type Set struct {
	// EventLog 事件日志（**必填**：唯一事实源，F5）。
	EventLog EventLog
	// Graph 图读面（可选投影，G2f：nil = 未启用）。
	Graph Graph
	// Documents 文档读面（可选；v4 保留位）。
	Documents Documents
	// Blob 大对象存储（可选，G2e：Event.binary_ref 的外置仓库）。
	Blob Blob
	// Vectors 向量检索（可选，G2d：RAG 保留位；概率性、可整体重建）。
	Vectors Retriever
}

// GraphWriter 取图族的写面（没有写面 → ok=false）。
//
// 槽位挂的是读面；实现同时提供写面时（索引器自建的投影写面）才拿得到。
func (s Set) GraphWriter() (GraphProjection, bool) {
	w, ok := s.Graph.(GraphProjection)
	if !ok || w == nil {
		return nil, false
	}
	return w, true
}

// DocumentWriter 取文档族的写面（没有写面 → ok=false）。
func (s Set) DocumentWriter() (DocumentProjection, bool) {
	w, ok := s.Documents.(DocumentProjection)
	if !ok || w == nil {
		return nil, false
	}
	return w, true
}

// VectorWriter 取向量族的写面（没有写面 → ok=false）。
func (s Set) VectorWriter() (VectorIndex, bool) {
	w, ok := s.Vectors.(VectorIndex)
	if !ok || w == nil {
		return nil, false
	}
	return w, true
}

// Kinds 返回本组合里**实际挂上**的族名（诊断 / 测试断言用；顺序确定）。
//
// 用途：酸测试与 /api 诊断一句话说清"这个 session 挂了哪些存储"，
// 免得"挂没挂"要逐个判 nil。
func (s Set) Kinds() []string {
	out := make([]string, 0, 5)
	if s.EventLog != nil {
		out = append(out, "eventlog")
	}
	if s.Graph != nil {
		out = append(out, "graph")
	}
	if s.Documents != nil {
		out = append(out, "documents")
	}
	if s.Blob != nil {
		out = append(out, "blob")
	}
	if s.Vectors != nil {
		out = append(out, "vectors")
	}
	return out
}

// HasEventLog 报告事件日志是否挂上（F5：没有事件日志的组合不是合法组合——
// 其余各族都可以是投影/外置，事实源只有一个）。
func (s Set) HasEventLog() bool { return s.EventLog != nil }

// 错误（哨兵，调用方用 errors.Is 判定）。
//
// 放在族总览里而不是某一族的文件里：这三族接口**共用同一组哨兵**——文档取不到、blob 取不到、
// 向量查询非法，对调用方是同一类"没拿到/别乱用"的信号，分成三套只会让调用方写三遍判断。
var (
	// ErrNotFound 是条目不存在（文档 / blob 取不到）。
	ErrNotFound = errors.New("storage: 条目不存在")
	// ErrConflict 是写入冲突（同 id/同键不同内容或不同版本）。
	ErrConflict = errors.New("storage: 写入冲突（同 id/键不同内容或版本）")
	// ErrBadQuery 是查询参数非法（负上限 / 空查询向量之类）。
	ErrBadQuery = errors.New("storage: 查询参数非法")
	// ErrUnsupported 是实现不支持某个可选能力（如向量检索的 Filter、文档列举的 Prefix）。
	//
	// **显式说不支持**而不是静默忽略：静默忽略会让调用方以为过滤生效了
	// （RAG 里"过滤没生效"= 检索到不该检索的东西）。
	ErrUnsupported = errors.New("storage: 实现不支持该能力")
)
