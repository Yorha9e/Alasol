package core

import (
	"context"

	"github.com/Yorha9e/alasol/contracts"
)

// Store 是事件日志的存储接口（D15：接口在 core，实现在 runtime）。
//
// 纪律：
//   - Append 以 contracts.Event.ID 幂等去重（S7：去重只看 Event.ID）——同一 ID 重复追加不产生重复事件；
//   - durable=false 的流事件永远不会到达 Store（F8：不入库、不占 seq）；
//   - seq 由实现分配（G9/G10：DB 原子分配、全局单调），Append 返回带 seq 的最终事件；
//   - 一个 session 的事件只由其宿主进程 append（G25，session 亲和写）——约束在部署层，不在接口层。
type Store interface {
	// Append 追加事实事件，返回入库后的最终事件（Seq 已填）。
	// 对已存在 ID 的事件幂等：不重复入库，返回库中原事件。
	Append(ctx context.Context, events []contracts.Event) ([]contracts.Event, error)

	// ReadFrom 从 seq 水位（含）读取事实事件，按 seq 升序。
	// sessionID 为 nil 时读全部；limit<=0 表示不限。
	ReadFrom(ctx context.Context, sessionID *contracts.Ulid, fromSeq int64, limit int) ([]contracts.Event, error)
}

// SliceReader 是 Store 的**只读增量面**（G5：长历史分段 + 偏移索引 + 惰性加载）。
//
// ReadFrom 只有下界（fromSeq）没有上界——"按 seq 区间分段读"在它上面只能"读一大段再在
// 内存里裁掉尾巴"（全文重放式）。ReadSlice 补上**闭区间上界** toSeq：实现侧用 seq 索引
// 做范围扫描（seq 就是全局偏移：INTEGER PRIMARY KEY + (session_id, seq) 索引），limit 之
// 外的行根本不进内存（惰性按需分段加载）。
//
// 纪律（只增不改）：
//   - Store 的 Append/ReadFrom 既有签名一字不动；实现**额外**实现本接口即获得分段读能力，
//     消费方用类型断言探测——不认的实现自动退回 ReadFrom 兜底，关/开逐事件等价（酸测试）；
//   - 形状与 ReadFrom 同口径：sessionID 为 nil 时读全部；limit<=0 表示不限；结果按 seq 升序；
//     seq 可有空洞（不保证稠密，见 runtime/store/sqlite）——空洞天然被区间查询跳过。
type SliceReader interface {
	// ReadSlice 读 seq **闭区间** [fromSeq, toSeq] 内的事实事件，按 seq 升序。
	// toSeq < fromSeq 是空区间 → 返回空切片（不报错）。
	ReadSlice(ctx context.Context, sessionID *contracts.Ulid, fromSeq, toSeq int64, limit int) ([]contracts.Event, error)
}
