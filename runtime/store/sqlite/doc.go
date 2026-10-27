// Package sqlite 是 core.Store 的 SQLite 实现（D15/G2b/G9/G13/G14）。
//
// 引擎定案：github.com/ncruces/go-sqlite3 v0.35.6（纯 Go，wasm2go 真 SQLite），
// DSN 必带 _txlock=immediate + busy_timeout（G13 实测并发写零失败的两个关键参数）。
// 事件表 seq 由 SQLite AUTOINCREMENT 原子分配（G9：全局单调、无需应用层仲裁）；
// id UNIQUE 实现按 Event.ID 幂等（G11）；只存 durable 事件（F8 第二道防线）。
//
// # seq 单调但不稠密
//
// AUTOINCREMENT 的冲突序号**不回收**：只有**插入冲突**（ON CONFLICT DO NOTHING，
// G11 幂等去重那条路径）会吃掉号；**事务回滚不吃号**（sqlite_sequence 的更新随事务
// 一起回滚）。所以 seq **全局单调、但不保证连续**：消费端不得用 max(seq) 当事件数，
// 事件条数用 cost_stats / sessions 里的显式计数；seq 只用于排序与水位。
//
// # 派生表写入权只归 indexer（G17）
//
// 本包**不暴露 *sql.DB**：裸句柄落到谁手里谁就能写派生表（连事件表都能写），
// 单写者纪律就守不住了。派生表（sessions / ledger_state / board_state /
// cost_stats / indexer_watermark）的写句柄只在本包内部流转，通过 NewIndexer
// 交给索引器 → 除 indexer 外，外部包拿不到写派生表的能力；派生表的读也走
// 索引器（indexer.Indexer 的 Read* 方法）。
package sqlite
