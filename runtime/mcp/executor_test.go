package mcp

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// 本文件是 **输出总量闸**（p2）的回归用例。
//
// 旧实现只截**单个 text 块**（64KB），于是：
//   - 多块大文本（10 块 × 60KB = 600KB）原样进上下文；
//   - Data / 结构化载荷**完全不限**（一张 5MB 的图原样过）。
//
// 外部 server 的输出不可信——总量必须封顶，且**截断必须标注**（模型要
// 知道自己看的是不全的，否则会基于半个结果下结论）。

// resultBytes  是一个 ToolResult 的总字节数（文本 + 载荷）。
func resultBytes(res contracts.ToolResult) int {
	total := 0
	for _, p := range res.Content {
		if p.Text != nil {
			total += len(*p.Text)
			continue
		}
		total += len(p.Data)
	}
	return total
}

// resultText 把 ToolResult 的文本块拼起来（断言截断标注用）。
func resultText(res contracts.ToolResult) string {
	var sb strings.Builder
	for _, p := range res.Content {
		if p.Text != nil {
			sb.WriteString(*p.Text)
		}
	}
	return sb.String()
}

// TestToolResultManyTinyBlocksBounded：**块数**维度也必须封顶。
//
// 场景：恶意 server 返回 N 个 1 字节 text 块。字节额度挡不住它——每块在
// 事件 JSON 里要占 ~27 字节（type + 字段名 + 引号 + 载荷），6 万块 = 1.6MB
// 载荷（放大 24.7 倍），模型可见文本也被切成 6 万段。
//
// 判据四条：块数封顶 / **事件载荷**封顶 / **模型可见文本**封顶 / 有截断标注。
func TestToolResultManyTinyBlocksBounded(t *testing.T) {
	const n = 60000
	blocks := make([]mcp.Content, 0, n)
	for i := 0; i < n; i++ {
		blocks = append(blocks, &mcp.TextContent{Text: "A"})
	}
	res := toolResult("mcp__s__many", &mcp.CallToolResult{Content: blocks})

	// ① 块数封顶（+1 是末尾的截断标注）。
	if len(res.Content) > maxResultParts+1 {
		t.Fatalf("块数 %d 超过上限 %d（块数维度没封住）", len(res.Content), maxResultParts+1)
	}
	// ② 事件载荷封顶（这才是真正进事件流的字节数）。
	//
	// 额度按**事件 JSON 实际字节**计，所以载荷贴着上限（而不是几倍）。
	payload, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("序列化结果: %v", err)
	}
	if len(payload) > maxToolOutput+2<<10 {
		t.Fatalf("事件载荷 %d 字节，超过上限 %d（块放大没掐住）", len(payload), maxToolOutput+2<<10)
	}
	// ③ 模型可见文本封顶（与 core/loop 的 toolResultText 同口径：text 块用
	//    "\n" 拼、Data 块原样带上）。
	var sb strings.Builder
	for _, p := range res.Content {
		switch {
		case p.Text != nil && *p.Text != "":
			sb.WriteString(*p.Text)
		case len(p.Data) > 0:
			sb.Write(p.Data)
		}
		if sb.Len() > 0 {
			sb.WriteString("\n")
		}
	}
	if sb.Len() > maxToolOutput {
		t.Fatalf("模型可见文本 %d 字节，超过上限 %d", sb.Len(), maxToolOutput)
	}
	// ③' 内容要**尽量保住**：合并相邻 text 块让块数塌缩，额度才花在内容上。
	//    只靠块数闸硬砍的话，6 万块只留 256 块 = 99.6% 的内容被丢掉
	//    （模型看到 256 字节，却以为这是全部）。
	if sb.Len() < maxToolOutput/2 {
		t.Fatalf("模型可见文本只有 %d 字节（内容被块数闸硬砍了，没有靠合并保住）", sb.Len())
	}
	// ④ 截断标注必须在（模型要知道内容不全）。
	if !strings.Contains(resultText(res), "truncated") {
		t.Fatalf("缺截断标注")
	}
}

// TestToolResultMergeKeepsVisibleText：合并相邻 text 块**不改变模型可见文本**
// （与 core/loop 的 toolResultText 拼接口径一致）。
func TestToolResultMergeKeepsVisibleText(t *testing.T) {
	res := toolResult("mcp__s__merge", &mcp.CallToolResult{Content: []mcp.Content{
		&mcp.TextContent{Text: "第一段"},
		&mcp.TextContent{Text: "第二段"},
	}})
	// 合并后是一个 text 块（块数塌缩）。
	if len(res.Content) != 1 {
		t.Fatalf("相邻 text 块应被合并，得到 %d 块", len(res.Content))
	}
	if got := *res.Content[0].Text; got != "第一段\n第二段" {
		t.Fatalf("合并后的文本 = %q，期望 %q", got, "第一段\n第二段")
	}
}

// TestToolResultManyTinyDataBlocksBounded：非文本块（合并不了）由**块数闸**
// 兜住——超出的块丢弃并计入标注。
func TestToolResultManyTinyDataBlocksBounded(t *testing.T) {
	const n = 10000
	blocks := make([]mcp.Content, 0, n)
	for i := 0; i < n; i++ {
		blocks = append(blocks, &mcp.ImageContent{Data: []byte{0x01}, MIMEType: "image/png"})
	}
	res := toolResult("mcp__s__manyimg", &mcp.CallToolResult{Content: blocks})
	if len(res.Content) > maxResultParts+1 {
		t.Fatalf("块数 %d 超过上限 %d（非文本块没被块数闸挡住）", len(res.Content), maxResultParts+1)
	}
	payload, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("序列化结果: %v", err)
	}
	if len(payload) > maxToolOutput+2<<10 {
		t.Fatalf("事件载荷 %d 字节，超过上限 %d", len(payload), maxToolOutput+2<<10)
	}
	if !strings.Contains(resultText(res), "truncated") {
		t.Fatalf("缺截断标注（丢块必须说）")
	}
}

// TestToolResultTotalCapManyBlocks：多块大文本 → **总量**受限 + 有截断标注。
func TestToolResultTotalCapManyBlocks(t *testing.T) {
	blocks := make([]mcp.Content, 0, 10)
	for i := 0; i < 10; i++ {
		blocks = append(blocks, &mcp.TextContent{Text: strings.Repeat("A", 60<<10)})
	}
	res := toolResult("mcp__s__big", &mcp.CallToolResult{Content: blocks})

	if got := resultBytes(res); got > maxToolOutput {
		t.Fatalf("总量 %d 字节超过上限 %d（多块 × 大块没被总量闸挡住）", got, maxToolOutput)
	}
	text := resultText(res)
	if !strings.Contains(text, "truncated") {
		t.Fatalf("缺截断标注（模型会以为自己看全了）: %q", text[:min(len(text), 200)])
	}
	// 标注要说清丢了多少（不只是"截断了"）。
	if !strings.Contains(text, "字节") {
		t.Fatalf("截断标注没说明丢了多少: %q", text[:min(len(text), 200)])
	}
}

// TestToolResultTotalCapBigImage：一张大图 → 载荷被换成**合法 JSON 的截断标记**，
// 总量受限。
//
// 为什么不能截半个 JSON：半个 JSON 不是合法 JSON，消费方直接报错；标记对象
// 语义清楚且仍可解析。
func TestToolResultTotalCapBigImage(t *testing.T) {
	img := &mcp.ImageContent{
		Data:     bytes.Repeat([]byte{0xAB}, 3<<20), // 3MB 原始数据（wire 上是 base64）
		MIMEType: "image/png",
	}
	res := toolResult("mcp__s__img", &mcp.CallToolResult{Content: []mcp.Content{img}})

	if got := resultBytes(res); got > maxToolOutput {
		t.Fatalf("总量 %d 字节超过上限 %d（大图没被挡住）", got, maxToolOutput)
	}
	var found bool
	for _, p := range res.Content {
		if p.Text != nil {
			continue
		}
		found = true
		if !json.Valid(p.Data) {
			t.Fatalf("截断后的载荷不是合法 JSON: %s", p.Data)
		}
		var marker struct {
			Truncated bool   `json:"_truncated"`
			Type      string `json:"_type"`
			Bytes     int    `json:"_bytes"`
		}
		if err := json.Unmarshal(p.Data, &marker); err != nil {
			t.Fatalf("截断标记解不开: %v", err)
		}
		if !marker.Truncated {
			t.Fatalf("截断标记缺 _truncated: %s", p.Data)
		}
		if marker.Type != "image" {
			t.Fatalf("截断标记的 _type = %q，期望 image", marker.Type)
		}
		if marker.Bytes <= 0 {
			t.Fatalf("截断标记没记原始大小: %s", p.Data)
		}
	}
	if !found {
		t.Fatalf("大图整块被丢了（应该有截断标记块）: %+v", res.Content)
	}
	// 截断也要标注在文本里（模型要知道内容不全）。
	if !strings.Contains(resultText(res), "truncated") {
		t.Fatalf("缺截断标注")
	}
}

// TestToolResultStructuredContentCapped：结构化输出同样进总量闸（它也是载荷）。
func TestToolResultStructuredContentCapped(t *testing.T) {
	big := map[string]any{"blob": strings.Repeat("B", 200<<10)}
	res := toolResult("mcp__s__st", &mcp.CallToolResult{
		Content:           []mcp.Content{&mcp.TextContent{Text: "ok"}},
		StructuredContent: big,
	})
	if got := resultBytes(res); got > maxToolOutput {
		t.Fatalf("总量 %d 字节超过上限 %d（结构化输出没被挡）", got, maxToolOutput)
	}
}

// TestToolResultUnderLimitUntouched：没超限的结果**原样**（不截、不加标注）——
// 总量闸不能变成"总是截一刀"。
func TestToolResultUnderLimitUntouched(t *testing.T) {
	res := toolResult("mcp__s__ok", &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: "hello"}},
	})
	if len(res.Content) != 1 {
		t.Fatalf("内容块数 = %d，期望 1", len(res.Content))
	}
	if res.Content[0].Text == nil || *res.Content[0].Text != "hello" {
		t.Fatalf("内容被改了: %+v", res.Content[0])
	}
	if strings.Contains(resultText(res), "truncated") {
		t.Fatalf("没超限却加了截断标注")
	}
}

// TestToolResultIsErrorPrefixKept：isError 的失败标注在**最前**且不被截掉
// （模型第一眼就要看到"这是失败"）。
func TestToolResultIsErrorPrefixKept(t *testing.T) {
	res := toolResult("mcp__s__boom", &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: strings.Repeat("C", 100<<10)}},
		IsError: true,
	})
	if len(res.Content) == 0 || res.Content[0].Text == nil {
		t.Fatalf("失败标注不在最前: %+v", res.Content)
	}
	if !strings.Contains(*res.Content[0].Text, "isError") {
		t.Fatalf("首个块不是失败标注: %q", *res.Content[0].Text)
	}
	if got := resultBytes(res); got > maxToolOutput {
		t.Fatalf("总量 %d 字节超过上限 %d", got, maxToolOutput)
	}
}

// TestToolResultTruncationNoteFits：截断标注自己必须装得下（预留额度够用）——
// "截断了却没说"是最糟的失败形态。
func TestToolResultTruncationNoteFits(t *testing.T) {
	note := truncationNote(123456789, 42)
	if len(note) > noteReserve {
		t.Fatalf("截断标注 %d 字节，超过预留的 %d", len(note), noteReserve)
	}
}

// min 是 Go 1.21+ 的内建函数（这里直接用内建版，不另造一个同名辅助）。
