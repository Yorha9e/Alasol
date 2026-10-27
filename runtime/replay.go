package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// ReplaySession 按 seq 完整返回一场 session 的全部 durable 事件（验收①）。
//
// 回放对象只有事实事件（F8）：durable=false 的流事件从未入库，故这里读到的就是
// 该 session 的完整事实历史，按全局 seq 升序。上层据此重建/核验任意状态。
func ReplaySession(ctx context.Context, store core.Store, sessionID contracts.Ulid) ([]contracts.Event, error) {
	// fromSeq=1（首个合法 seq）、limit=0（不限）：拿全场。
	events, err := store.ReadFrom(ctx, &sessionID, 1, 0)
	if err != nil {
		return nil, fmt.Errorf("runtime: replay session %s: %w", sessionID, err)
	}
	return events, nil
}

// FoldLedger 从事件流 fold 出当前 goal ledger 态：取**最后一条** ledger.updated 的
// 全量快照（G23/G8：ledger 不是独立可变存储，当前态 = 对事件的投影，天然 LWW）。
//
// 传入的事件应按 seq 升序（如 ReplaySession 的返回值）。没有 ledger.updated 时
// 返回零值（四个 nil 切片）。
//
// **容错口径与索引器一致**（D7 宽容）：载荷不可解析的 ledger.updated 被**跳过**
// （不中断 fold、不影响其余事件），与 indexer 对坏载荷的"隔离而非致命"处理同源
// （索引器另计一次 cost_stats["indexer.bad_payload"]）。
func FoldLedger(events []contracts.Event) contracts.LedgerUpdatedPayload {
	var out contracts.LedgerUpdatedPayload
	for i := range events {
		if events[i].Kind != kindLedgerUpdated {
			continue
		}
		var p contracts.LedgerUpdatedPayload
		if err := json.Unmarshal(events[i].Payload, &p); err != nil {
			continue // 宽容（D7）：跳过无法解析的 ledger 事件，不影响其余 fold。
		}
		out = p // 后写覆盖先写 = LWW
	}
	return out
}

// kindLedgerUpdated 与 indexer 保持一致；这里 fold 只关心这一种 kind。
const kindLedgerUpdated = "ledger.updated"
