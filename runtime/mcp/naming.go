package mcp

import (
	"fmt"
	"strings"
)

// 本文件是**工具命名隔离方案**的唯一口径（D21：口径只定义一次，host / CLI / 文档
// 都引用它，不重抄）。
//
// # 方案：注册名 = `mcp__<server>__<tool>`
//
//	wire 名（发给 MCP server 的 tools/call）  = server 原样的工具名
//	注册名（进 core/registry、进模型可见集）  = "mcp__" + server + "__" + tool
//
// # 为什么加前缀（三条，按重要性）
//
//  1. **外部工具名是外部可控输入**。一个 MCP server 完全可以声明一个叫 `read` /
//     `write` / `bash` 的工具——那不是巧合，是真实的攻击/误配面（恶意 server
//     靠同名**遮蔽**内置工具，让模型以为在调 read 实际在读别的）。前缀让
//     "内置"与"MCP 来的"在名字上就分家，撞名在结构上不可能。
//  2. **多 server 之间要能共处**。两个 server 都声明 `search` 是常态。server 名
//     在配置层全局唯一（Config.Validate 拒重复）→ 前缀唯一 → 工具名唯一。
//  3. **策略与可见性可读**。`allow = ["mcp__fs__read_file"]` 一眼看出"这是 fs
//     这个 MCP server 的工具"；agent 类型的 tools[] 白名单（R5）也能按前缀成组
//     地授权。裸工具名做不到这一点。
//
// # 为什么不解析回 (server, tool)
//
// executor 自己就持有 (server, tool) 二元组，不需要从注册名反解——反解只会引入
// "名字里有 __ 怎么切"的歧义。前缀的用途是**隔离与可读**，不是编码。
//
// # 大小写（陷阱 17）
//
// 全程**不折叠大小写、不做规范化**：server 名与工具名都按字节比较。`mcp__Fs__Read`
// 与 `mcp__fs__read` 是两个不同的工具（不同的注册表键、不同的策略规则）。
// 这不危险：策略是精确匹配，大小写不同的两个名字要么都授权要么都不授权，
// 不存在"以为授权了其实没授权"的反向意外。
//
// # 归一化：只做一件最小的事
//
// server 声明的工具名可能含 `.`（如 `github.create_issue`）这类在部分 provider
// 的函数名里不合法的字符。口径：`.` → `_`，其余只允许 `[A-Za-z0-9_-]`，
// 越界即**拒注册**（fail-close，附上原名让使用者能定位）。
// 归一化可能把两个不同工具名折成同一个（`a.b` 与 `a_b`）——那由注册表的
// 重名拒绝兜住， surfaced 成一条带两个原名的明确错误（见 manager.go）。

// Prefix 是 MCP 工具注册名的固定前缀。
const Prefix = "mcp"

// Separator 是前缀/ server 名/工具名之间的分隔符。
const Separator = "__"

// MaxServerNameLen 是 server 名上限（64）。
//
// 取值理由：server 名要进工具名，而工具名要进 provider 的函数名（OpenAI 的
// 函数名上限 64，见 MaxToolNameLen）。这是**配置面**的粗筛：真正卡住的是
// 注册名总长（下面那条）——超长的 server 名会让它的工具在装配期被拒
// （错误里点名 server 与工具，fail-close）。
const MaxServerNameLen = 64

// MaxToolNameLen 是**注册名**（含前缀）的上限（64）。
//
// 为什么是 64：注册名会原样进 provider 的函数名，而 OpenAI 的函数名上限是
// **64 字符**（runtime/provider/openai 原样透传，不截不改）。注册名超限的
// 后果不是"这个工具不能用"而是**整轮请求被 provider 拒**——一个外部 server
// 声明一个长工具名就能把整轮对话打穿（外部输入打穿内部请求，最坏的一类面）。
//
// 因此这里取 provider 的约束值而不是放宽到 128：超限在**装配期**就 fail-close
// （错误里点名 server 与工具名，使用者改名即可），不让它活到运行期。
const MaxToolNameLen = 64

// MaxWireToolNameLen 是 server 声明的工具名上限（128，与 MCP SDK 的
// validateToolName 同口径——超长的直接拒，不猜）。
const MaxWireToolNameLen = 128

// ToolName 拼出注册名：`mcp__<server>__<tool>`。
//
// server 必须已过 validateServerName；tool 是 server 声明的**原样**名字
// （未归一化——归一化在 normalizeToolName，调用方先归一化再拼，或直接用
// RegisterableName 一次做完）。
func ToolName(server, tool string) string {
	return Prefix + Separator + server + Separator + tool
}

// RegisterableName 把 server 声明的工具名变成**可注册**的名字：
// 归一化（`.`→`_`，其余限定字符集）+ 拼前缀 + 长度校验。
//
// 返回 error 而非静默修改越界字符：静默丢字符会让两个工具名变成同一个
// （静默遮蔽比报错糟得多）。
func RegisterableName(server, wireName string) (string, error) {
	if err := validateServerName(server); err != nil {
		return "", err
	}
	norm, err := normalizeToolName(wireName)
	if err != nil {
		return "", err
	}
	name := ToolName(server, norm)
	if len(name) > MaxToolNameLen {
		return "", fmt.Errorf("mcp: 注册名 %q 超过 %d 字符上限（server %q + 工具 %q；"+
			"缩短 server 名或换一个工具名；注册名会进 provider 的函数名，超限会让整轮请求被拒）",
			name, MaxToolNameLen, server, wireName)
	}
	return name, nil
}

// validateServerName 校验 server 名（工具名前缀的唯一来源）。
//
// 口径：
//   - 非空、≤ MaxServerNameLen 字节；
//   - 只允许 `[A-Za-z0-9_-]`，且**首字符必须是字母或数字**（不让名字以 `-` 开头，
//     那在若干 provider 的函数名里不合法，也会让 `mcp__-x__t` 很难读）；
//   - **不含 `__`**：分隔符是 `__`，server 名里再出现只会让 `mcp__a__b__c` 这种
//     名字难以人读（我们不解析，所以这不是正确性问题，是可读性/防呆）。
func validateServerName(name string) error {
	n := strings.TrimSpace(name)
	if n == "" {
		return fmt.Errorf("server 名不能为空（工具名前缀 mcp__<server>__ 的来源）")
	}
	if n != name {
		return fmt.Errorf("server 名 %q 不能带首尾空白（前缀要可预测）", name)
	}
	if len(name) > MaxServerNameLen {
		return fmt.Errorf("server 名 %q 超过 %d 字符上限", name, MaxServerNameLen)
	}
	if strings.Contains(name, Separator) {
		return fmt.Errorf("server 名 %q 不能含分隔符 %q（可读性/防呆：mcp__<server>__<tool> 要能一眼切开）", name, Separator)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !isNameByte(c) {
			return fmt.Errorf("server 名 %q 含非法字符 %q（只允许 [A-Za-z0-9_-]）", name, string(rune(c)))
		}
	}
	if !isAlnumByte(name[0]) {
		return fmt.Errorf("server 名 %q 首字符必须是字母或数字（得到 %q）", name, string(rune(name[0])))
	}
	return nil
}

// normalizeToolName 归一化 server 声明的工具名。
//
// 只做**一件**事：`.` → `_`（`github.create_issue` 这类名字在若干 provider 的
// 函数名里含 `.` 不合法）。其余字符越界即拒——不猜、不丢。
func normalizeToolName(wireName string) (string, error) {
	if strings.TrimSpace(wireName) == "" {
		return "", fmt.Errorf("server 声明的工具名为空")
	}
	if len(wireName) > MaxWireToolNameLen {
		return "", fmt.Errorf("server 声明的工具名 %q 超过 %d 字符上限", wireName, MaxWireToolNameLen)
	}
	var sb strings.Builder
	sb.Grow(len(wireName))
	for i := 0; i < len(wireName); i++ {
		c := wireName[i]
		switch {
		case c == '.':
			sb.WriteByte('_') // 唯一一处归一化
		case isNameByte(c):
			sb.WriteByte(c)
		default:
			return "", fmt.Errorf("server 声明的工具名 %q 含非法字符 %q（只允许 [A-Za-z0-9_-]，`.` 会被归一化成 `_`）",
				wireName, string(rune(c)))
		}
	}
	return sb.String(), nil
}

func isNameByte(c byte) bool {
	return isAlnumByte(c) || c == '_' || c == '-'
}

func isAlnumByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}
