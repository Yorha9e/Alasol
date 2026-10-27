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
