package core

import (
	"context"

	"github.com/Yorha9e/alasol/contracts"
)

// ToolExecutor 是工具执行器的进程内接口（D1：执行器不进跨边界契约，跨边界只传
// contracts.Tool 描述与 contracts.ToolResult 返回值）。
//
// 执行器只负责执行这一次调用；工具执行写路径的三段事件
// （tool.call.intent → 执行 → tool.call.completed/.failed，X1）由 loop 层负责落盘，
// 执行器不写事件。
type ToolExecutor interface {
	Execute(ctx context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error)
}
