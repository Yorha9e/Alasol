package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// 本文件是 MCP 工具的**执行器**：core.ToolExecutor 的实现，把一次
// contracts.ToolCallIntentPayload 变成一次 MCP tools/call，再把
// mcp.CallToolResult 折回 contracts.ToolResult。
//
// # 它从外面看就是普通工具（X1 红线）
//
// 不写事件（三段写路径归 loop）、不判策略（策略归 policy.Decide）、
// 不注入凭据（env 走 core.WithEnv 的 ctx，且 MCP server 的凭据在**装配期**
// 就给了子进程，不在调用期）。它只做"执行这一次调用"这一件事。
//
// # 参数：原样过桥
//
// intent.Arguments 是 loop 校验过的合法 JSON 对象字节（validateArgs），
// 这里**原样**交给 MCP（Arguments 字段收 any，塞 json.RawMessage 即按原字节
// 发出）——不解析、不改写、不补全（S4 纪律同款：入账的字节与发出的字节一致）。
//
// # 结果映射：MCP 的 content 块 → contracts.ContentPart
//
//   - text 块  → type=text + Text（原样；输出截断与脱敏归 loop 的 redactResult，
//     但**总量闸**在这里也挡一道：外部 server 的返回不可信，见 maxToolOutput /
//     maxResultParts）；
//   - 其它块（image/audio/resource/resource_link/tool_use/tool_result）
//     → type=<块类型> + Data（原始 JSON）。v0 的内置工具只有文本块，
//     但契约的 ContentPart.Type 是**开放字符串**（"如 text / image / resource"），
//     所以原样带上不违反契约；模型侧按文本看，结构化块供人/后续消费。
//   - structuredContent → 追加一个 type=json 的 Data 块（SEP-2106：结构化输出）。
//
// # 总量闸有**两个维度**（都不是"单块上限"）
//
//   - **块数**（maxResultParts）：N 个 1 字节块是最省的放大面（每块在事件
//     JSON 里 ~27 字节）；
//   - **字节**（maxToolOutput，按**事件 JSON 实际字节**计）：转义放大
//     （控制字符在 JSON 里 6 字节）也在额度内。
//
// 合并相邻 text 块让块数塌缩（额度花在内容上，而不是被块结构吃掉），截断
// 一律带标注（模型要知道自己看的是不全的）。
//
// # isError 与协议错误的分野
//
//   - res.IsError = true（工具自己报错）→ **不是** Go error：它是业务结果，
//     连同 content 一起回给模型自我修正（M5，与 bash 非零退出码同口径）；
//   - transport/协议层错误（断连、超时、server 崩）→ Go error → loop 走
//     tool.call.failed（"server 崩溃不拖垮 hub"的落点）。
type mcpExecutor struct {
	conn *serverConn
	// wireName 是发给 server 的工具名（原样，未加前缀）。
	wireName string
	// name 是注册名（错误文案里用，让模型能对上自己调用的名字）。
	name string
}

// 编译期断言：mcpExecutor 必须满足 core.ToolExecutor。
var _ core.ToolExecutor = (*mcpExecutor)(nil)

// maxToolOutput 是**单次 MCP 工具返回给模型的总量上限**（64KB）。
//
// 与 runtime/tools.maxToolOutput 同值同理由：工具输出整段进上下文，
// 64KB 已经是几千 token；不截的话一个啰嗦的外部 server 就能把上下文撑爆。
// 外部 server 的输出**比内置工具更不可信**（我们没写过它），所以这道截断
// 必须在适配层就有，不能只依赖 loop 侧。
//
// **是总量，不是单块**：单块上限挡不住"多块 × 大块"（10 块 60KB = 600KB），
// 也挡不住 Data 载荷（一张 5MB 的图原样进来）。runtime/tools 的 capText 是对
// **已组装**的文本做最后一道总截断——这里对**已组装**的整个结果做同一件事。
const maxToolOutput = 64 << 10

// noteReserve 是总量截断**标注**预留的字节（标注必须装得下，否则"截断了
// 却没说"——模型会以为自己看全了，基于半个结果下结论）。
const noteReserve = 512

// maxResultParts 是单个 MCP 结果的**块数**上限（256）。
//
// 为什么字节额度还不够：块本身有**放大**——每块在事件 JSON 里要占 ~27 字节
// （type + 字段名 + 引号 + 载荷），于是"N 个 1 字节块"是最省的放大面：
// 6 万块 = 1.6MB 载荷（放大 24.7 倍），而内容本身只有 60KB；模型可见文本
// 也会被切成 6 万段（每段 1 字节 + 分隔符）。
//
// 所以总量闸有**两个维度**：块数（这里）+ 字节（maxToolOutput）。
const maxResultParts = 256

// mergeBudget 是**合并后文本**的字节上限（总量闸的额度减去标注预留）。
//
// 合并时也守它：合并过程不能变成新的放大面（否则"合并"只是把放大换了个
// 地方）。分隔符算进额度（它也是内容的一部分）。
const mergeBudget = maxToolOutput - noteReserve

// Execute implements core.ToolExecutor。
func (e *mcpExecutor) Execute(ctx context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	if e == nil || e.conn == nil {
		return contracts.ToolResult{}, fmt.Errorf("%s: MCP 连接未建立（装配缺失，无法调用外部工具）", e.toolLabel())
	}
	args := intent.Arguments
	if len(strings.TrimSpace(string(args))) == 0 {
		args = json.RawMessage("{}") // 空参数合法（MCP 侧按"无参"处理）
	}
	// 参数**原样**过桥：loop 已校验过是 JSON 对象，这里不再解析。
	res, err := e.conn.CallTool(ctx, &mcp.CallToolParams{
		Name:      e.wireName,
		Arguments: json.RawMessage(args),
	})
	if err != nil {
		// 传输/协议层错误（断连 / 超时 / server 崩 / ctx 取消）。
		// ctx 取消单独说清：那通常是整轮 Run 被中止，不是工具自己的毛病。
		if ctxErr := ctx.Err(); ctxErr != nil {
			return contracts.ToolResult{}, fmt.Errorf("%s: 调用 MCP server 被中止: %v", e.toolLabel(), ctxErr)
		}
		return contracts.ToolResult{}, fmt.Errorf("%s: MCP tools/call 失败（server 可能已崩溃或断连）: %v",
			e.toolLabel(), err)
	}
	if res == nil {
		return contracts.ToolResult{}, fmt.Errorf("%s: MCP server 返回了空结果（协议违规）", e.toolLabel())
	}
	return toolResult(e.toolLabel(), res), nil
}

// toolLabel 是错误文案里的工具标识（注册名，模型能对上自己调用的名字）。
func (e *mcpExecutor) toolLabel() string {
	if e == nil || e.name == "" {
		return "<mcp 工具>"
	}
	return e.name
}

// toolResult 把 mcp.CallToolResult 折成 contracts.ToolResult。
//
// 折完立刻过**三道闸**：合并相邻 text 块（块数塌缩）→ 块数闸（≤ maxResultParts）
// → 字节闸（capParts，按事件 JSON 实际字节计）。外部 server 的返回不可信，
// 多块/大载荷都要封顶，且截断必须标注（p2）。
func toolResult(label string, res *mcp.CallToolResult) contracts.ToolResult {
	content := make([]contracts.ContentPart, 0, len(res.Content)+1)
	for _, c := range res.Content {
		if p, ok := contentPart(c); ok {
			content = append(content, p)
		}
	}
	if res.StructuredContent != nil {
		if raw, err := json.Marshal(res.StructuredContent); err == nil && len(raw) > 0 && string(raw) != "null" {
			content = append(content, dataPart("json", raw))
		}
	}
	// 合并相邻 text 块（块数塌缩 → 载荷放大被掐掉；模型可见文本不变）。
	merged, droppedBytes, droppedParts := mergeTextParts(content)
	// IsError 是**业务结果**不是 Go error：把错误事实写进文本回填给模型
	// （M5：模型要看得见"这个工具失败了"才能自我修正），但不让 loop 走
	// tool.call.failed——那是协议层失败的通道。
	//
	// 标注放在**最前且不参与合并**：它短、要保住（模型第一眼就要看到"这是失败"）。
	if res.IsError {
		merged = append([]contracts.ContentPart{textPart(isErrorPrefix(label))}, merged...)
	}
	// 块数闸（≤ maxResultParts）：超出的块丢弃并计数（尾部优先）。
	if len(merged) > maxResultParts {
		for _, p := range merged[maxResultParts:] {
			droppedBytes += partSize(p)
			droppedParts++
		}
		merged = merged[:maxResultParts]
	}
	return contracts.ToolResult{Content: capParts(merged, droppedBytes, droppedParts)}
}

// mergeTextParts 合并**相邻**的 text 块（分隔符 "\n"）。
//
// 为什么合并：块数本身有放大——每块在事件 JSON 里要占 ~27 字节（type +
// 字段名 + 引号 + 载荷），6 万个 1 字节块 = 1.6MB 载荷（放大 24.7 倍），
// 而内容本身只有 60KB。合并后块数塌缩，字节闸一道就够。
//
// 为什么分隔符是 "\n"：与 core/loop 的 toolResultText 拼接口径**一致**
// （它把各块用 "\n" 拼起来回填给模型）——所以合并前后**模型看到的文本一模一样**，
// 只是事件载荷小了。
//
// 合并时也守额度：合并后的文本最多 mergeBudget 字节（分隔符**算进额度**），
// 放不下的内容计入"丢弃"（由末尾的标注说明），不静默消失。
//
// 返回 (合并后的块, 丢弃字节数, 丢弃块数)。
func mergeTextParts(parts []contracts.ContentPart) ([]contracts.ContentPart, int, int) {
	out := make([]contracts.ContentPart, 0, len(parts))
	var sb strings.Builder
	open := false
	droppedBytes, droppedParts := 0, 0

	flush := func() {
		if open {
			out = append(out, textPart(sb.String()))
			sb.Reset()
			open = false
		}
	}
	for _, p := range parts {
		if p.Text == nil {
			// 非文本块：不能合并（类型不同），原样保留并断开合并段。
			flush()
			out = append(out, p)
			continue
		}
		text := *p.Text
		if !open {
			open = true
			if len(text) > mergeBudget {
				// 单块就超额度 → 截断（与字节闸同口径：切 rune 边界）。
				keep := cutAtRuneBoundary(text, mergeBudget)
				sb.WriteString(text[:keep])
				droppedBytes += len(text) - keep
				droppedParts++
				continue
			}
			sb.WriteString(text)
			continue
		}
		// 后续块：连分隔符一起算额度。
		if sb.Len()+1+len(text) > mergeBudget {
			droppedBytes += len(text)
			droppedParts++
			continue
		}
		sb.WriteString("\n")
		sb.WriteString(text)
	}
	flush()
	return out, droppedBytes, droppedParts
}

// capParts 对**已组装**的整个结果做字节总量闸（maxToolOutput 字节）。
//
// # 额度按**事件 JSON 实际字节**计（不是内容字节）
//
// 内容字节额度挡不住**转义放大**：JSON 里每个控制字符占 6 字节（\u0001），
// 每个换行占 2 字节（\n）——"60 万个 1 字节块"那种载荷最坏能到内容量的 6 倍。
// 额度按 partJSONSize（序列化后的真实成本）算，事件载荷才是**真的**封顶。
//
// 代价：内容里转义字符多时，可见文本会比 64KB 略少（额度先被转义吃掉）。
// 普通文本（无控制字符）下两者几乎相同——与 runtime/tools 的 capText 同量级。
//
// # 截断口径（与 runtime/tools 的 capText 同思路）
//
//   - 文本块：截到剩余额度，切在 rune 边界上（半个多字节字符会变成 U+FFFD，
//     模型会误以为是 server 的内容乱码）；
//   - 数据块（image/audio/resource/结构化）：**不截成半个 JSON**（半个 JSON
//     不是合法 JSON，消费方直接报错）→ 换成一个自描述的**截断标记对象**，
//     语义清楚且仍然可解析。
//
// droppedBytes / droppedParts 是**合并与块数闸**已经丢掉的量（这里接着算），
// 最后统一出一条标注。
func capParts(parts []contracts.ContentPart, droppedBytes, droppedParts int) []contracts.ContentPart {
	total := 0
	for _, p := range parts {
		total += partJSONSize(p)
	}
	// 要不要留标注的位置：有东西被丢/被截就要说（模型据此知道内容不全）。
	needNote := droppedBytes > 0 || droppedParts > 0 || total > maxToolOutput
	budget := maxToolOutput
	if needNote {
		// 给标注预留位置：标注本身也必须装得下（noteReserve 远大于实际长度）。
		budget = maxToolOutput - noteReserve
	}
	if !needNote {
		return parts
	}

	out := make([]contracts.ContentPart, 0, len(parts)+1)
	used := 0
	for _, p := range parts {
		size := partJSONSize(p)
		if used+size <= budget {
			out = append(out, p)
			used += size
			continue
		}
		// 放不下：文本块截断到剩余额度；数据块换截断标记。
		remaining := budget - used
		if p.Text != nil {
			text, _ := fitText(*p.Text, remaining)
			if text != "" {
				tp := textPart(text)
				out = append(out, tp)
				used += partJSONSize(tp)
				droppedBytes += len(*p.Text) - len(text)
			} else {
				droppedBytes += len(*p.Text)
			}
			droppedParts++
			continue
		}
		marker := truncationMarker(p.Type, partSize(p))
		mp := dataPart(p.Type, marker)
		if partJSONSize(mp) <= remaining {
			out = append(out, mp)
			used += partJSONSize(mp)
			droppedBytes += partSize(p) - len(marker)
		} else {
			droppedBytes += partSize(p)
		}
		droppedParts++
	}
	out = append(out, textPart(truncationNote(droppedBytes, droppedParts)))
	return out
}

// partJSONSize 是一个内容块**序列化后**的字节数（进事件载荷的真实成本）。
//
// 编不出来（理论上不会：ContentPart 只有 string / json.RawMessage）→ 按内容
// 字节估（宁可低估放大面，也不高估额度）。
func partJSONSize(p contracts.ContentPart) int {
	raw, err := json.Marshal(p)
	if err != nil {
		return partSize(p)
	}
	return len(raw)
}

// fitText 裁文本到"该块序列化后不超过 budget 字节"的最长前缀。
//
// 二分找前缀长度（转义后的长度随前缀单调增），最后再退到 rune 边界
// （不切碎多字节字符）。返回 (裁后的文本, 是否被裁过)。
func fitText(s string, budget int) (string, bool) {
	if budget <= 0 {
		return "", len(s) > 0
	}
	// 固定开销：{"type":"text","text":""}
	overhead := len(`{"type":"text","text":""}`)
	avail := budget - overhead
	if avail <= 0 {
		return "", len(s) > 0
	}
	if escapedLen(s) <= avail {
		return s, false
	}
	lo, hi := 0, len(s)
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if escapedLen(s[:mid]) <= avail {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	cut := lo
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut], true
}

// escapedLen 是字符串**转义后**的字节数（不含两端的引号）。
//
// 用 json.Marshal 量（权威口径）：控制字符 6 字节、换行 2 字节——这正是
// "按内容字节算额度会漏掉的放大面"。
func escapedLen(s string) int {
	b, err := json.Marshal(s)
	if err != nil {
		return len(s)
	}
	if len(b) < 2 {
		return 0
	}
	return len(b) - 2 // 去掉两端的引号
}

// partSize 是一个内容块的字节数（文本按字节，载荷按原始 JSON 长度）。
func partSize(p contracts.ContentPart) int {
	if p.Text != nil {
		return len(*p.Text)
	}
	return len(p.Data)
}

// cutAtRuneBoundary 在 max 字节处切一个 UTF-8 字符串（不切碎多字节字符）。
//
// 返回实际可切的字节数（<= max）。
func cutAtRuneBoundary(s string, max int) int {
	if max <= 0 {
		return 0
	}
	if len(s) <= max {
		return len(s)
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return cut
}

// truncationMarker 是数据块被截断时的替代载荷（**保持合法 JSON**）。
//
// 为什么不是"截一半的 JSON"：半个 JSON 不是合法 JSON，消费方（下游解析器、
// 模型侧的结构化消费）会直接报错；这里换成一个自描述的标记对象——语义清楚
// 且仍然可解析。
func truncationMarker(typ string, size int) json.RawMessage {
	raw, err := json.Marshal(map[string]any{
		"_truncated": true,
		"_type":      typ,
		"_bytes":     size,
	})
	if err != nil {
		return json.RawMessage(`{"_truncated":true}`)
	}
	return raw
}

// truncationNote 是总量截断的标注（贴在结果末尾）。
//
// 必须说清三件事：上限是多少、丢了多少、还有多少块被丢——模型据此知道
// 自己看到的是**不全的**内容（runtime/tools 的 truncationNote 同款纪律）。
func truncationNote(droppedBytes, droppedParts int) string {
	return fmt.Sprintf("\n…[truncated: MCP 输出总量超过 %d 字节上限，已截断/丢弃 %d 字节、%d 个内容块；需要全文请让工具方分页/分段返回]\n",
		maxToolOutput, droppedBytes, droppedParts)
}

// isErrorPrefix 是工具自报失败时的前缀（让模型一眼看出这不是正常输出）。
func isErrorPrefix(label string) string {
	return fmt.Sprintf("[%s] MCP server 报告工具执行错误（isError=true）：", label)
}

// contentPart 把一个 MCP content 块转成 contracts.ContentPart。
//
// 返回 ok=false 表示这个块没有可搬运的载荷（空 text 之类）→ 调用方跳过。
//
// **这里不截断**：截断是**总量**闸的事（capParts）——单块上限挡不住
// "多块 × 大块"，也不挡得住 Data 载荷。在单块上截会把内容切碎又留不全
// （总量闸切一次，标注一次，模型才知道自己看的是不全的）。
func contentPart(c mcp.Content) (contracts.ContentPart, bool) {
	switch v := c.(type) {
	case *mcp.TextContent:
		if v == nil || v.Text == "" {
			return contracts.ContentPart{}, false
		}
		return textPart(v.Text), true
	case nil:
		return contracts.ContentPart{}, false
	default:
		// 其它块类型（image / audio / resource / resource_link / tool_use / tool_result）：
		// Type 用 MCP 的块类型名（开放字符串，契约允许），载荷走 Data 原始 JSON。
		raw, err := json.Marshal(c)
		if err != nil || len(raw) == 0 || string(raw) == "null" {
			return contracts.ContentPart{}, false
		}
		return dataPart(contentTypeName(c), raw), true
	}
}

// contentTypeName 取 MCP content 块的类型名（"text" / "image" / "resource_link" …）。
//
// 逐个 case 列出来而不是反射：SDK 的块类型是封闭集合，列全了就不会漏；
// 反射取类型名会依赖 SDK 的内部命名（`*mcp.ResourceLink` → "ResourceLink"），
// 而协议里的类型名是小写蛇形（"resource_link"）。
func contentTypeName(c mcp.Content) string {
	switch c.(type) {
	case *mcp.TextContent:
		return "text"
	case *mcp.ImageContent:
		return "image"
	case *mcp.AudioContent:
		return "audio"
	case *mcp.ResourceLink:
		return "resource_link"
	case *mcp.EmbeddedResource:
		return "resource"
	case *mcp.ToolUseContent:
		return "tool_use"
	case *mcp.ToolResultContent:
		return "tool_result"
	default:
		return "unknown"
	}
}

func textPart(text string) contracts.ContentPart {
	return contracts.ContentPart{Type: "text", Text: strPtr(text)}
}

func dataPart(typ string, raw json.RawMessage) contracts.ContentPart {
	return contracts.ContentPart{Type: typ, Data: raw}
}
