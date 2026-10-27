package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// read 工具：读一个文件的全文。
//
// 元数据口径（D12）：
//   - side_effect=read：只读，不碰外部状态；
//   - idempotent=true + replay=safe：重复执行无副作用，崩溃续跑可安全重放（X2）；
//   - approvable=false：读不需要人审（**默认**仍需策略链放行，见 policy.Decide；
//     这里的 false 只是"不触发 ask 人审"这一档）。

// ReadDescriptor 是 read 工具的跨边界描述（给模型 + 给策略链）。
func ReadDescriptor() contracts.Tool {
	return contracts.Tool{
		Name:        toolNameRead,
		Description: strPtr("读取一个文件的全文内容（按 UTF-8 文本返回；二进制文件会被拒绝）。"),
		Schema:      readSchema,
		SideEffect:  contracts.SideEffectRead,
		Cancelable:  true,
		Approvable:  false,
		Idempotent:  true,
		Replay:      contracts.ReplayKindSafe,
	}
}

// readSchema 是 read 的参数 Schema（原样进请求体 parameters 字段）。
var readSchema = json.RawMessage(`{
  "type": "object",
  "properties": {
    "path": {
      "type": "string",
      "description": "要读取的文件路径（原样传给文件系统；路径守卫由策略层负责，本工具不做限制）"
    }
  },
  "required": ["path"],
  "additionalProperties": false
}`)

type readArgs struct {
	Path string `json:"path"`
}

// ReadExecutor 是 read 的执行器（core.ToolExecutor）。
type ReadExecutor struct {
	fs core.FSCapability
}

// 编译期断言：ReadExecutor 必须满足 core.ToolExecutor。
var _ core.ToolExecutor = (*ReadExecutor)(nil)

// NewRead 造 read 执行器。fs 为 nil 时执行阶段才报错（fail-loud 但不 panic：
// 装配遗漏要在第一次调用时以模型可读的文案暴露出来）。
func NewRead(fs core.FSCapability) core.ToolExecutor {
	return &ReadExecutor{fs: fs}
}

// Execute implements core.ToolExecutor。
func (e *ReadExecutor) Execute(ctx context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	if e == nil || e.fs == nil {
		return contracts.ToolResult{}, fmt.Errorf("%s: 未注入 FSCapability（装配缺失，无法读文件）", toolNameRead)
	}
	var args readArgs
	if err := decodeArgs(toolNameRead, intent.Arguments, &args); err != nil {
		return contracts.ToolResult{}, err
	}
	if strings.TrimSpace(args.Path) == "" {
		return contracts.ToolResult{}, fmt.Errorf("%s: path 不能为空；期望形如 %s", toolNameRead, argsHint(toolNameRead))
	}

	data, err := e.fs.ReadFile(ctx, args.Path)
	if err != nil {
		return contracts.ToolResult{}, fmt.Errorf("%s: %v", toolNameRead, err)
	}
	// v0 只有文本内容块：二进制文件宁可报错，也不静默替换成 U+FFFD
	// （那会把文件内容污染成错的，模型还以为是原文）。
	if !utf8.Valid(data) {
		return contracts.ToolResult{}, fmt.Errorf("%s: 文件 %s 不是有效的 UTF-8 文本（v0 仅支持文本读取；二进制/图片内容块留 v1）",
			toolNameRead, args.Path)
	}

	text, truncated := truncateText(data)
	if truncated {
		text += truncationNote(len(data) - len(text))
	}
	return resultText(text), nil
}
