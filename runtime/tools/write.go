package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// write 工具：整文件覆盖写。
//
// 元数据口径（D12）：
//   - side_effect=write：改外部状态；
//   - idempotent=true + replay=verify：同内容重复写结果一致，所以能自动重试（M7）；
//     但崩溃续放前必须**核验外部状态**（X2 verify）——写一半崩了的话，
//     重放前要确认文件当前内容再决定动不动手；
//   - approvable=true：默认要求人审（可配，见 WriteDescriptor）。
var _ core.ToolExecutor = (*WriteExecutor)(nil)

// WriteDescriptor 是 write 工具的跨边界描述。
//
// approvable 由装配方决定（这就是"可配"的入口）：真机回路可以显式传 false 让
// always-allow 类策略接管，调试时也可以传 true 强制每次问人。
// **真正的权威是策略引擎**（policy.Decide），这里的布尔值只是给模型/UI 的提示。
func WriteDescriptor(approvable bool) contracts.Tool {
	return contracts.Tool{
		Name:        toolNameWrite,
		Description: strPtr("把内容整文件写入指定路径（覆盖写，不追加；目录不会自动创建）。"),
		Schema:      writeSchema,
		SideEffect:  contracts.SideEffectWrite,
		Cancelable:  false,
		Approvable:  approvable,
		Idempotent:  true,
		Replay:      contracts.ReplayKindVerify,
	}
}

// writeSchema 是 write 的参数 Schema。
var writeSchema = json.RawMessage(`{
  "type": "object",
  "properties": {
    "path": {
      "type": "string",
      "description": "目标文件路径（相对路径按会话工作区根解析、越出工作区根即拒；绝对路径原样传给文件系统）"
    },
    "content": {
      "type": "string",
      "description": "要写入的完整内容（UTF-8 文本，覆盖原文件）"
    }
  },
  "required": ["path", "content"],
  "additionalProperties": false
}`)

// writeArgs 的 content 用指针：要能区分"模型没给 content"（违反 schema 的 required，
// 报错）和"模型显式给了空串"（合法：清空文件）。用 string 会把两者混为一谈，
// 于是要么误拒合法清空，要么静默接受缺参。
type writeArgs struct {
	Path    string  `json:"path"`
	Content *string `json:"content"`
}

// WriteExecutor 是 write 的执行器。
type WriteExecutor struct {
	fs core.FSCapability
}

// NewWrite 造 write 执行器（Approvable 只影响描述，不影响执行器）。
func NewWrite(fs core.FSCapability) core.ToolExecutor {
	return &WriteExecutor{fs: fs}
}

// Execute implements core.ToolExecutor。
func (e *WriteExecutor) Execute(ctx context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	if e == nil || e.fs == nil {
		return contracts.ToolResult{}, fmt.Errorf("%s: 未注入 FSCapability（装配缺失，无法写文件）", toolNameWrite)
	}
	var args writeArgs
	if err := decodeArgs(toolNameWrite, intent.Arguments, &args); err != nil {
		return contracts.ToolResult{}, err
	}
	if strings.TrimSpace(args.Path) == "" {
		return contracts.ToolResult{}, fmt.Errorf("%s: path 不能为空；期望形如 %s", toolNameWrite, argsHint(toolNameWrite))
	}
	if args.Content == nil {
		return contracts.ToolResult{}, fmt.Errorf("%s: content 缺失（必填；如需清空文件请显式传空字符串）；期望形如 %s",
			toolNameWrite, argsHint(toolNameWrite))
	}

	// 路径解析（L2 per-session 执行上下文 · 件 1）：相对路径按会话工作区根锚定，越界即拒；
	// 绝对路径原样透传；工作区为空 = 现状（相对路径按进程 cwd）。详见 workspace.go 的三条口径。
	path, err := resolveWorkspacePath(ctx, toolNameWrite, args.Path)
	if err != nil {
		return contracts.ToolResult{}, err
	}

	if err := e.fs.WriteFile(ctx, path, []byte(*args.Content)); err != nil {
		// %w：保住错误链（调用方要能 errors.Is 判底层原因，如 remote.ErrUnsupported）。
		return contracts.ToolResult{}, fmt.Errorf("%s: %w", toolNameWrite, err)
	}
	// 回执里只报字节数与路径——**不回声内容**（内容模型刚给过，回显纯浪费 token）。
	// 报的是**实际落点**（解析后的路径）：模型要知道东西落在哪儿，才知道下一步去哪儿读。
	return resultText(fmt.Sprintf("wrote %d bytes to %s", len(*args.Content), path)), nil
}
