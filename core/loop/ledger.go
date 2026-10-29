package loop

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/compaction"
	"github.com/Yorha9e/alasol/core/durable"
)

// 本文件提供 goal ledger 的**生产者通道**（J19/X6 挂账，v3-S3 清）：让 agent 能在关键点
// 显式更新 goal ledger（目标 / 约束 / 已完成项 / 待解项）。
//
// # 为什么是框架工具而非注册表工具
//
// ledger 写入器是 **per-(session, agent)** 的（core/durable.LedgerWriter 非并发安全，
// 一个 session 的一个 turn 内由单写者用，G25 session 亲和写），而 host 的 registry 是跨
// session 共享的——把 per-session 执行器塞进共享 registry 会重名冲突。所以它做成 **loop
// 级框架工具**（与 spawn/complete / 策略的 recall/stage 同款）：每次 Run 造一个 fresh 写入器
// （放进 runState），工具绑定到它，天然 per-loop，无冲突。仍走 X1 三段写路径（intent →
// 执行 → completed/failed）。
//
// # X6 写放大控制（LedgerWriter 语义）
//
//   - 关键字段（目标 / 约束）→ UpdateCritical **立即**落一条 ledger.updated（崩溃不丢）；
//   - 过程字段（已完成 / 待解）→ UpdateProcedural 只改内存缓冲，**turn 末 Flush 合并**
//     成一条 ledger.updated。
//
// # opt-in，默认零行为变化（酸测试）
//
// 仅当 loop.Config.GoalLedger=true 且配置了 Store 时才注入本工具。默认不注入 → 模型看不
// 见它、行为与 v2 逐字节等价。注入后仍受策略链裁决（deny-all 默认 → 要 allow 才可调，
// fail-close）与 agent 类型白名单（R5）约束——框架工具不是免检通道。

// ToolLedger 是 goal ledger 更新工具名（框架级常量，供宿主 allow 规则 / agent 类型白名单引用）。
const ToolLedger = "ledger"

// ledgerToolDesc 是 ledger 工具的描述（进模型可见集）。
//
// description 把四张清单与 X6 语义显式化：goals/constraints 是关键字段（立即落盘），
// completed/pending 是过程字段（turn 末合并）。缺省的清单字段**保持不变**（增量更新，
// 不是每次全量覆盖——避免 agent 为了改一项就得把整本账重述一遍）。
func ledgerToolDesc() contracts.Tool {
	desc := "在关键点更新 goal ledger（目标 / 约束 / 已完成项 / 待解项）。" +
		"目标与约束是关键字段（立即落盘，崩溃不丢）；已完成项与待解项是过程字段（本 turn 末合并成一条）。" +
		"**增量更新**：没给的清单字段保持原值。账本在上下文之外独立持久化，压缩任一阶段都能读到它（级联 C 案）。"
	return contracts.Tool{
		Name:        ToolLedger,
		Description: &desc,
		Schema:      ledgerToolSchema(),
		SideEffect:  contracts.SideEffectWrite,  // 写账本（落 ledger.updated 事件）
		Replay:      contracts.ReplayKindUnsafe, // 有副作用（改账本快照），未知结果不盲目重放（X2）
	}
}

// ledgerToolSchema 是 ledger 工具的参数字chema（四张清单均可选；至少给一项）。
func ledgerToolSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "properties": {
    "goals":       {"type": "array", "items": {"type": "string"}, "description": "目标清单（关键字段，立即落盘）。不给 = 保持原值。"},
    "constraints": {"type": "array", "items": {"type": "string"}, "description": "约束清单（关键字段，立即落盘）。不给 = 保持原值。"},
    "completed":   {"type": "array", "items": {"type": "string"}, "description": "已完成项（过程字段，turn 末合并）。不给 = 保持原值。"},
    "pending":     {"type": "array", "items": {"type": "string"}, "description": "待解项（过程字段，turn 末合并）。不给 = 保持原值。"}
  }
}`)
}

// ledgerExecutor 执行 ledger 更新：按 X6 分流关键字段（立即）与过程字段（缓冲）。
//
// 绑定到一个 per-Run 的 durable.LedgerWriter（loop Run 时造，见 addLedgerTool）。
type ledgerExecutor struct {
	writer *durable.LedgerWriter
}

// 编译期证明：*ledgerExecutor 就是 core.ToolExecutor。
var _ core.ToolExecutor = (*ledgerExecutor)(nil)

// Execute 执行一次 ledger 更新（X1 的 intent/completed 由 loop 落）。
//
// 语义：
//   - 参数从 intent.Arguments 解（四张清单均可选；至少给一项，否则普通失败）；
//   - goals/constraints 非缺省 → UpdateCritical（合并进当前快照后**立即**落一条）；
//   - completed/pending 非缺省 → UpdateProcedural（只缓冲，turn 末 Flush 合并）。
func (e *ledgerExecutor) Execute(ctx context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	if e.writer == nil {
		return contracts.ToolResult{}, fmt.Errorf("ledger: 未绑定写入器（装配错误）")
	}
	var args struct {
		Goals       *[]string `json:"goals"`
		Constraints *[]string `json:"constraints"`
		Completed   *[]string `json:"completed"`
		Pending     *[]string `json:"pending"`
	}
	if len(intent.Arguments) > 0 {
		if err := json.Unmarshal(intent.Arguments, &args); err != nil {
			return contracts.ToolResult{}, fmt.Errorf("ledger 参数解不开: %w（应形如 {\"goals\":[\"...\"]}）", err)
		}
	}
	// 当前快照（增量更新的基线：没给的字段保持原值）。
	cur := e.writer.Snapshot()

	updateCritical := args.Goals != nil || args.Constraints != nil
	updateProcedural := args.Completed != nil || args.Pending != nil
	if !updateCritical && !updateProcedural {
		return contracts.ToolResult{}, fmt.Errorf("ledger 至少要给一张清单（goals/constraints/completed/pending 之一）")
	}

	msg := "ledger 已更新："
	if updateCritical {
		goals := cur.Goals
		if args.Goals != nil {
			goals = *args.Goals
		}
		constraints := cur.Constraints
		if args.Constraints != nil {
			constraints = *args.Constraints
		}
		// 关键字段：立即落一条 ledger.updated（X6：崩溃不丢）。
		if err := e.writer.UpdateCritical(ctx, goals, constraints); err != nil {
			return contracts.ToolResult{}, fmt.Errorf("ledger 更新关键字段（目标/约束）落盘失败: %w", err)
		}
		msg += " 目标/约束已立即落盘；"
	}
	if updateProcedural {
		completed := cur.Completed
		if args.Completed != nil {
			completed = *args.Completed
		}
		pending := cur.Pending
		if args.Pending != nil {
			pending = *args.Pending
		}
		// 过程字段：只缓冲，turn 末 Flush 合并成一条（X6）。
		e.writer.UpdateProcedural(completed, pending)
		msg += " 已完成/待解已暂存（本 turn 末合并落盘）；"
	}
	return textResultTool(msg), nil
}

// textResultTool 造一个单文本块的 ToolResult（ledger 更新确认回填模型）。
func textResultTool(text string) contracts.ToolResult {
	t := text
	return contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &t}}}
}

// addLedgerTool 把 ledger 框架工具加进本次 Run 的框架工具集（st.strategyTools）。
//
// fail-close：与策略框架工具 / 注册表工具重名 → 报错拒绝（同名执行语义必须唯一，F12）。
func (l *Loop) addLedgerTool(st *runState) error {
	desc := ledgerToolDesc()
	name := desc.Name
	if st.strategyTools == nil {
		st.strategyTools = map[string]compaction.FrameworkTool{}
	}
	if _, dup := st.strategyTools[name]; dup {
		return fmt.Errorf("loop: ledger 工具 %q 与策略框架工具重名（fail-close：同名执行语义必须唯一）", name)
	}
	if _, known := l.registry.Get(name); known {
		return fmt.Errorf("loop: ledger 工具 %q 与注册表工具重名（fail-close；请改工具名或移除注册表同名工具）", name)
	}
	st.strategyTools[name] = compaction.FrameworkTool{Desc: desc, Exec: &ledgerExecutor{writer: st.ledger}}
	return nil
}

// flushLedger 在 turn 末把缓冲的过程性 ledger 变更合并落一条（X6）。尽力而为：
// 落不下只在事件流补 loop.error 留痕，不中止 Run（过程字段非关键，可从中断的 turn 重算）。
func (l *Loop) flushLedger(ctx context.Context, turn int, st *runState) {
	if st == nil || st.ledger == nil {
		return
	}
	if err := st.ledger.Flush(ctx); err != nil {
		l.publishLoopError(ctx, turn, "ledger.flush", err)
	}
}

// strategyToolsOf 取本次 Run 的框架工具集（nil 安全：无框架工具 → nil）。
func strategyToolsOf(st *runState) map[string]compaction.FrameworkTool {
	if st == nil {
		return nil
	}
	return st.strategyTools
}
