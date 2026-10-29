package compaction

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// 本文件提供两个**可由模型调用**的工具（走 loop 现有的 X1 三段写路径，宿主注册进 registry）：
//
//	recall(ref)  取回某阶段/某 seq 区间的持久化原文全文（J15 指针解析的工具化形态，13 §5）；
//	stage{label} 显式标记一个阶段边界（软边界 = agent tier 标记，J11/13 §3）。
//
// 二者都不是新事件 kind——它们就是普通工具调用，观测（recall 调用、阶段切分点）经
// tool.call.intent（tool_name=recall/stage）天然可从事件流复盘（13 §9），无需额外契约。

// ToolRecall / ToolStage 是两个工具名（框架级常量，供宿主注册与策略规则引用）。
const (
	ToolRecall = "recall"
	ToolStage  = "stage"
)

// ResolveFunc 解析一个指针取回原文（engine.Resolve 即此签名）。
type ResolveFunc func(ctx context.Context, ref string) (text string, ok bool, err error)

// ---------------------------------------------------------------------------
// recall 工具
// ---------------------------------------------------------------------------

// RecallDescriptor 是 recall 工具的描述（进模型可见集）。
//
// description 把指针语义显式化（13 §5）：ref 用 "stage:N@e:<from>..e:<to>"（带 seq 锚的
// 稳定指针，摘要条目里给的就是这个）或 "e:<from>..e:<to>"（机器 seq 区间）；
// 短形式 "stage:N" 只是**本 Run 内**的别名（阶段号每 Run 重排，F9）。
// refs 永不指摘要——recall 只取回**持久化原文**。
func RecallDescriptor() contracts.Tool {
	return contracts.Tool{
		Name: ToolRecall,
		Description: strPtr(
			"取回某个阶段（或某事件 seq 区间）的**持久化原文全文**。当上下文里某阶段只剩 L1/L2 摘要、" +
				"而你需要其中的现场细节（原始命令输出、完整文件内容、中间步骤）时调用。" +
				"ref 用 \"stage:N@e:<from>..e:<to>\"（摘要条目里给的指针，原样回传即可）或 " +
				"\"e:<from>..e:<to>\"（事件 seq 区间）。" +
				"refs 只指向持久化原文，永不指向摘要。",
		),
		Schema:     json.RawMessage(recallToolSchema),
		SideEffect: contracts.SideEffectNone, // 只读回捞，无副作用
		Replay:     contracts.ReplayKindSafe, // 幂等只读，可安全重放
		Idempotent: true,
	}
}

const recallToolSchema = `{
  "type": "object",
  "properties": {
    "ref": {
      "type": "string",
      "description": "指针：\"stage:N@e:<from>..e:<to>\"（摘要条目里给的指针，原样回传）或 \"e:<from>..e:<to>\"（事件 seq 区间）。指向持久化原文。"
    }
  },
  "required": ["ref"]
}`

// RecallExecutor 执行 recall：解析指针 → 取回原文全文 → 作为 ToolResult 回填模型。
//
// resolve 通常是 *Engine.Resolve（经 NewRecallExecutor(engine.Resolve) 注入）。
type RecallExecutor struct {
	resolve ResolveFunc
}

// 编译期证明：*RecallExecutor 就是 core.ToolExecutor。
var _ core.ToolExecutor = (*RecallExecutor)(nil)

// NewRecallExecutor 构造 recall 执行器（resolve 为 nil → 所有 recall 都回"不可用"）。
func NewRecallExecutor(resolve ResolveFunc) *RecallExecutor {
	return &RecallExecutor{resolve: resolve}
}

// Execute 执行一次 recall（X1 路径由 loop 负责落 intent/completed，这里只做取回）。
func (e *RecallExecutor) Execute(ctx context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	var args struct {
		Ref string `json:"ref"`
	}
	if err := json.Unmarshal(intent.Arguments, &args); err != nil {
		return contracts.ToolResult{}, fmt.Errorf("recall 参数不是合法 JSON 对象: %w（应形如 {\"ref\":\"stage:5@e:10..e:20\"}）", err)
	}
	if args.Ref == "" {
		return contracts.ToolResult{}, fmt.Errorf("recall 缺少 ref 参数（应形如 {\"ref\":\"stage:5@e:10..e:20\"}）")
	}
	if e.resolve == nil {
		return textResult("recall: 未配置原文回捞源，无法取回 " + args.Ref), nil
	}
	text, ok, err := e.resolve(ctx, args.Ref)
	if err != nil {
		return contracts.ToolResult{}, fmt.Errorf("recall(%s) 读取原文失败: %w", args.Ref, err)
	}
	if !ok {
		return textResult(fmt.Sprintf("recall: 指针 %q 无法解析（应是 stage:N@e:<from>..e:<to> 或 e:<from>..e:<to>，且阶段/区间存在）", args.Ref)), nil
	}
	if text == "" {
		return textResult(fmt.Sprintf("recall(%s): 该范围无持久化原文（可能尚未落库或已被更早的 seq 覆盖）", args.Ref)), nil
	}
	return textResult(fmt.Sprintf("recall(%s) 取回的持久化原文全文：\n%s", args.Ref, text)), nil
}

// ---------------------------------------------------------------------------
// stage 工具（软边界：agent 显式 tier 标记，J11）
// ---------------------------------------------------------------------------

// StageDescriptor 是 stage 工具的描述（进模型可见集）。
//
// agent 在一个验证循环通过（critic clean / gate receipt 通过 = 阶段结束，13 §3）时调用它，
// 告诉框架"这里是一个阶段关节"。框架随即关闭当前阶段、开启新阶段，并按留存策略压缩旧阶段。
func StageDescriptor() contracts.Tool {
	return contracts.Tool{
		Name: ToolStage,
		Description: strPtr(
			"在一个**验证通过点**显式标记阶段边界（一次验证循环通过 = 一个阶段结束）。" +
				"调用后框架关闭当前阶段、开启新阶段，并按留存策略把更老的阶段压成 L1/L2 摘要（可回捞）。" +
				"label 可选，写这个阶段的小结（便于事后审计）。不要过频调用——阶段是工作的关节，不是每轮。",
		),
		Schema:     json.RawMessage(stageToolSchema),
		SideEffect: contracts.SideEffectNone, // 只打标记，无外部副作用
		Replay:     contracts.ReplayKindSafe,
		Idempotent: true,
	}
}

const stageToolSchema = `{
  "type": "object",
  "properties": {
    "label": {
      "type": "string",
      "description": "可选：这个阶段的小结（如 \"读懂了 plan.md，拟好 stage1 方案\"），供事后审计。"
    }
  }
}`

// StageExecutor 执行 stage：打一个阶段边界标记（下次装配时消费）。
//
// mark 通常是 *Engine.MarkBoundary（经 NewStageExecutor(engine.MarkBoundary) 注入）。
type StageExecutor struct {
	mark func()
}

// 编译期证明：*StageExecutor 就是 core.ToolExecutor。
var _ core.ToolExecutor = (*StageExecutor)(nil)

// NewStageExecutor 构造 stage 执行器（mark 为 nil → 空操作）。
func NewStageExecutor(mark func()) *StageExecutor {
	return &StageExecutor{mark: mark}
}

// Execute 执行一次 stage（打边界标记）。X1 的 intent/completed 由 loop 落。
func (e *StageExecutor) Execute(_ context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	if e.mark != nil {
		e.mark()
	}
	var args struct {
		Label string `json:"label"`
	}
	_ = json.Unmarshal(intent.Arguments, &args) // label 可选；解不开也用默认文案
	msg := "已标记阶段边界：本次验证通过点结束当前阶段，下一轮开启新阶段（旧阶段按留存策略压缩，可 recall 回捞）。"
	if args.Label != "" {
		msg += " 阶段小结：" + args.Label
	}
	return textResult(msg), nil
}

// textResult 造一个单文本块的 ToolResult。
func textResult(text string) contracts.ToolResult {
	t := text
	return contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &t}}}
}

// Tools 实现 compaction.ToolProvider：engine 注入 recall / stage 两个框架工具（绑定到自身，
// per-loop）。loop 把它们加进模型可见集并路由执行（仍走 X1 三段写路径）。
func (e *Engine) Tools() []FrameworkTool {
	return []FrameworkTool{
		{Desc: RecallDescriptor(), Exec: NewRecallExecutor(e.Resolve)},
		{Desc: StageDescriptor(), Exec: NewStageExecutor(e.MarkBoundary)},
	}
}
