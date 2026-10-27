package loop

import (
	"context"
	"errors"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/durable"
)

// Checkpoint 手动建一次 checkpoint（X3）：收集四项（seq 水位 / ledger 版本 / in-flight + 树版本）
// 并落一条 checkpoint.created durable 事件。需 Config.Store 已配置，否则返回错误。
//
// 并发：与 Run 的 turn 串行调用（一个 session 只由其宿主进程 append，G25）；Run 内的自动
// checkpoint 也走同一路径，故同一 Loop 上不会并发 Create。
func (l *Loop) Checkpoint(ctx context.Context) (contracts.CheckpointCreatedPayload, error) {
	if l == nil {
		return contracts.CheckpointCreatedPayload{}, errors.New("loop: Loop 为 nil")
	}
	if l.store == nil {
		return contracts.CheckpointCreatedPayload{}, errors.New("loop: 未配置 Config.Store，无法 checkpoint（阶段 8 durable）")
	}
	return durable.Create(ctx, l.store, l.sessionID)
}

// maybeCheckpoint 在 turn 末按 Config.CheckpointEvery 自动 checkpoint（尽力而为）。
//
// 每 N 轮一次（N>0 且配置了 Store 才生效）；落盘失败**不中止** Run——Run 的事件本身已 durable，
// 缺 checkpoint 只意味着 resume 退到全量重建（见 core/durable.Resume）。此处不把 checkpoint
// 事件计入 Result.Events（它是恢复元数据，不是 agent loop 一轮的产出）。
func (l *Loop) maybeCheckpoint(ctx context.Context, turn int) {
	if l == nil || l.store == nil || l.checkpointEvery <= 0 {
		return
	}
	if turn%l.checkpointEvery != 0 {
		return
	}
	_, _ = l.Checkpoint(ctx) // best-effort：见上
}
