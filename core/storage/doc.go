// Package storage 是 G1/G2「**一组可组合的存储接口**」的收口面（v2-S5）。
//
// # 定位：存储不是一个引擎，是一族接口（G1/G2）
//
// 按用途分成五族，**各自多个实现、可组合**（G2）：
//
//	| 族 | 接口（读面 / 写面） | 定义处 | 实现 | 状态 |
//	|---|---|---|---|---|
//	| 事件日志 | `EventLog`（Append/ReadFrom） | `core.Store` | `runtime/store/sqlite` | ✅ 已落地 |
//	| 图 | `Graph` / `GraphProjection` / `Tx` | `core/graph` | `runtime/graph`（SQLite）+ `core/graph.MemGraph`（纯内存） | ✅ 已落地（v2-S2） |
//	| 文档 | `Documents` / `DocumentProjection` | 本包 | — | ⏸ 接口先立，实现留 v4 |
//	| blob | `Blob` | 本包 | — | ⏸ 接口先立，实现留 v4（G2e） |
//	| 向量 | `Retriever` / `VectorIndex` | 本包 | — | ⏸ 接口先立，实现留 v4（G2d：RAG 保留位） |
//
// **事件日志是唯一事实源**（F5）：其余四族都是**投影或外置存储**，不是第二状态源——
// 文档/图的当前态都由事件流 fold 得出（写面只归索引器，G17），blob 是**大对象的外置仓库**
// （事件里只留引用 `Event.binary_ref`，D4/G2e），向量是概率性检索缓存（G2d，可整体重建）。
//
// # 为什么本包存在 / 什么变化会导致它变
//
// 存在理由：把"一组可组合的存储接口"落成**清晰分立的接口面**——一个 session 可以同时挂
// 事件日志 + 图 + blob（见 `Set`），也可以只挂事件日志（其余全空 = 与今天完全等价）。
// 换任何一族的实现（SQLite → 正经数据库 / 文件系统 → 对象存储）**消费方零改动**：
// 酸测试钉住（见 compose_test.go：同一份消费者代码跑在两套完全不同的实现上——**五族都换**
// ：事件日志 map ↔ 追加日志、图 邻接 map ↔ 倒排索引、文档 map ↔ 追加日志、
// blob 整块 ↔ 分块、向量 现算余弦 ↔ 归一化点积）。
//
// **读面与写面**：组合槽位挂的是**读面**；写面（GraphProjection / DocumentProjection /
// VectorIndex）是**可选能力**（G17：写面只归索引器写）——取写面用带 ok 的取面器
// （`Set.GraphWriter()` 等），**不许硬转型 panic**：只挂读面是合法组合。
//
// 会导致它变：新增一族存储接口；某一族的查询/写入形状变化（那也意味着它的实现要跟着变）。
// **不会**导致它变：换引擎（SQLite → Postgres/对象存储）、换 provider、加工具、换 UI——
// 这正是接口分立要买到的东西（G2b）。
//
// # G2b 纪律：接口不泄漏 SQLite 特性
//
// 后续要上正经数据库 → 接口里**不出现** SQL 语句文本、占位符、方言、`*sql.DB`、
// `AUTOINCREMENT`/`seq` 分配细节：
//
//   - `Tx` 是"一批写要么全成要么全不成"的最小面（core/graph 定义，本包别名出口）；
//     语句文本由**实现侧**持有并传入（v2-S2 定下的形状，本刀**不扩大**它——
//     不给 Tx 加 Query/方言/保存点之类的方法）。
//   - DDL **不走** Tx：各族写面的 `EnsureSchema(ctx)` 由实现自建表（F6/G2b）。
//   - 幂等键 / 引用 id 一律走契约类型（`contracts.Ulid`），不暴露引擎的自增列语义。
//
// # 依赖方向
//
// 本包 import `contracts` / `core`（事件日志接口的定义处）/ `core/graph`（图族的定义处），
// **不 import runtime**（09 依赖规则①：core 无 IO、可被任意宿主嵌入与单测）。
// 实现都在 runtime（或 v4 的新包），消费方（host / 索引器 / 未来的工作流引擎）只认本包
// 与 core/graph 的形状。
package storage
