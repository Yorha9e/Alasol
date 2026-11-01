package tools

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/Yorha9e/alasol/contracts"
)

// maxToolOutput 是单次工具返回给模型的文本上限（64KB）。
//
// 为什么截：工具输出整段进上下文，64KB 已经是几千 token；不截的话一个 `cat 大文件`
// 就能把上下文撑爆（进而触发压缩，白费 token）。截断**必须标注**——不标注的话
// 模型以为看全了内容，会基于半个文件下结论。
const maxToolOutput = 64 << 10

// strPtr 取一个字符串指针（contracts 的可选字段一律指针 + omitempty）。
func strPtr(s string) *string { return &s }

// textPart 造一个文本内容块（v0 只有 text 一种；image/resource 留 v1）。
func textPart(text string) contracts.ContentPart {
	return contracts.ContentPart{Type: "text", Text: strPtr(text)}
}

// resultText 造一个只有文本的工具返回值。
func resultText(text string) contracts.ToolResult {
	return contracts.ToolResult{Content: []contracts.ContentPart{textPart(text)}}
}

// truncateText 把输出截到 maxToolOutput，并**切在 rune 边界上**。
//
// 为什么切 rune 边界：按字节硬切会把一个多字节字符切成两半，JSON 序列化时变成
// U+FFFD，模型看到乱码还以为是文件内容乱码。
//
// 返回 (文本, 是否截断)。
func truncateText(b []byte) (string, bool) {
	if len(b) <= maxToolOutput {
		return string(b), false
	}
	cut := maxToolOutput
	for cut > 0 && !utf8.RuneStart(b[cut]) {
		cut--
	}
	return string(b[:cut]), true
}

// truncationNote 是截断标注（原样拼进返回文本，让模型知道内容不全）。
func truncationNote(dropped int) string {
	return fmt.Sprintf("\n…[truncated: 输出超过 %d 字节上限，已丢弃 %d 字节；需要全文请分段读取]\n", maxToolOutput, dropped)
}

// capText 对**已组装**的文本做最后一道总截断（保证返回值整体不超 maxToolOutput）。
func capText(s string) string {
	if len(s) <= maxToolOutput {
		return s
	}
	cut := maxToolOutput
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + truncationNote(len(s)-cut)
}

// decodeArgs 把 intent.Arguments 解进目标结构体，失败时给出**模型可读**的文案（M5）：
// 说清工具期望什么形状，而不是回一句 "unexpected end of JSON input"。
func decodeArgs(tool string, raw json.RawMessage, v any) error {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return fmt.Errorf("%s: 缺少参数（期望 JSON 对象，如 %s）", tool, argsHint(tool))
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("%s: 参数不是合法 JSON 对象（%v）；期望形如 %s", tool, err, argsHint(tool))
	}
	return nil
}

// argsHint 是每个工具的参数示例（错误文案里用）。
func argsHint(tool string) string {
	switch tool {
	case toolNameRead:
		return `{"path":"README.md"}`
	case toolNameWrite:
		return `{"path":"a.txt","content":"新内容"}`
	case toolNameBash:
		return `{"command":"ls -la"}`
	case toolNameGitStatus:
		return `{"path":"."}`
	default:
		return "{...}"
	}
}

// 工具名常量（事件 kind 之外的第二个"开放字符串但要对齐"的地方：
// 名字同时出现在描述、注册表 key、模型调用里，四处必须一致）。
const (
	toolNameRead  = "read"
	toolNameWrite = "write"
	toolNameBash  = "bash"
)
