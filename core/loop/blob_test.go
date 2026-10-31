package loop_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/storage"
)

// ---------------------------------------------------------------------------
// D4（大 payload 外置 blob，G2e）黑盒测试：只走公开 API（loop.Config.BlobSink/BlobMaxInline）。
// 用内存 fakeBlob（不 import runtime/blobfs——core 测试边界；真实文件系统实现在
// runtime/store/blobfs 与 host 集成测试里跑）。
// ---------------------------------------------------------------------------

// fakeBlob 是 D4 测试用的最小 storage.Blob（内存；记录 Put 的 id→bytes/meta）。
type fakeBlob struct {
	mu   sync.Mutex
	data map[contracts.Ulid][]byte
	meta map[contracts.Ulid]storage.BlobMeta
}

func newFakeBlob() *fakeBlob {
	return &fakeBlob{data: map[contracts.Ulid][]byte{}, meta: map[contracts.Ulid]storage.BlobMeta{}}
}

func (b *fakeBlob) Put(_ context.Context, meta storage.BlobMeta, content io.Reader) (storage.BlobMeta, error) {
	raw, err := io.ReadAll(content)
	if err != nil {
		return storage.BlobMeta{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if meta.ID == "" {
		meta.ID = core.NewUlid()
	}
	meta.Size = int64(len(raw))
	b.data[meta.ID] = raw
	b.meta[meta.ID] = meta
	return meta, nil
}

func (b *fakeBlob) Get(_ context.Context, id contracts.Ulid) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	raw, ok := b.data[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(raw)), nil
}

func (b *fakeBlob) Stat(_ context.Context, id contracts.Ulid) (storage.BlobMeta, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	m, ok := b.meta[id]
	if !ok {
		return storage.BlobMeta{}, storage.ErrNotFound
	}
	return m, nil
}

func (b *fakeBlob) Delete(_ context.Context, id contracts.Ulid) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.data, id)
	delete(b.meta, id)
	return nil
}

func (b *fakeBlob) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.data)
}

// failingBlob 是"Put 恒返错"的 fake（**fail-open 路径**专用，F2）。
//
// 为什么要有它：fail-open（blob 写失败 → 原样内联、completed 仍落、BinaryRef 空）是 D4 的
// 关键容错语义，但它**只在 blob 真的写失败时**才走到——不注入失败就永远走不到（那条分支
// 零覆盖 = 变异存活：把 fail-open 改成"中止 loop"或"丢弃 completed"都不会红）。
type failingBlob struct {
	err error
}

func (b failingBlob) Put(context.Context, storage.BlobMeta, io.Reader) (storage.BlobMeta, error) {
	return storage.BlobMeta{}, b.err
}

func (b failingBlob) Get(context.Context, contracts.Ulid) (io.ReadCloser, error) {
	return nil, storage.ErrNotFound
}

func (b failingBlob) Stat(context.Context, contracts.Ulid) (storage.BlobMeta, error) {
	return storage.BlobMeta{}, storage.ErrNotFound
}

func (b failingBlob) Delete(context.Context, contracts.Ulid) error { return nil }

// blobSummaryMarkerText 是摘要占位标记的**字面量**（测试侧不 import 内部常量——
// 包外测试钉的是"读者能认出这是被外置的占位"这件事本身）。
const blobSummaryMarkerText = "[已外置：全文见 blob "

// findCompleted / findFailed 取写路径上的 completed / failed 事件（复用 loop_test 的 findEvent）。
func findCompleted(t *testing.T, evs []contracts.Event) contracts.Event {
	return findEvent(t, evs, loop.KindToolCallCompleted, 0)
}

func findFailed(t *testing.T, evs []contracts.Event) contracts.Event {
	return findEvent(t, evs, loop.KindToolCallFailed, 0)
}

// TestD4DisabledByteEquivalent：未挂 blob（默认）→ completed 载荷与现状**逐字节等价**、
// BinaryRef 空（酸测试：外置功能关时零行为变化）。
func TestD4DisabledByteEquivalent(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	big := strings.Repeat("A", 5000)
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &big}}}
	h.script(
		stepTools(call("tc_D4_OFF", "echo", `{"text":"hi"}`)),
		stepText("完成"),
	)

	// 不传 BlobSink（默认 nil = 关）。
	if _, err := h.loop(nil).Run(context.Background(), "echo"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	done := findCompleted(t, h.events())
	if done.BinaryRef != nil {
		t.Fatalf("关时 BinaryRef 应为空，got %v", *done.BinaryRef)
	}
	// 逐字节等价：payload == 完整 result 内联的 JSON（未被截断/改写）。
	want, err := json.Marshal(contracts.ToolCallCompletedPayload{
		ToolCallID: "tc_D4_OFF",
		Result:     contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &big}}},
	})
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if !bytes.Equal(done.Payload, want) {
		t.Fatalf("关时 completed 载荷应与完整内联逐字节等价：\ngot  %s\nwant %s", done.Payload, want)
	}
}

// TestD4ThresholdZeroDisablesEvenWithBlob：挂了 blob 但阈值 0 → 仍不外置（0/未配 = 关）。
//
// 断言加到**逐字节**（F4）：只断言"BinaryRef 空 + 没写 blob"不够——变异"阈值 0 时也走外置
// 但摘要形态恰好等价"或"阈值判定被改成 < 0"都可能蒙混过关。逐字节等价才是酸测试的真口径：
// 关时 completed 载荷与现状一字不差。
func TestD4ThresholdZeroDisablesEvenWithBlob(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	big := strings.Repeat("B", 5000)
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &big}}}
	blob := newFakeBlob()
	h.script(
		stepTools(call("tc_D4_ZERO", "echo", `{"text":"hi"}`)),
		stepText("完成"),
	)

	_, err := h.loop(func(c *loop.Config) {
		c.BlobSink = blob
		c.BlobMaxInline = 0 // 阈值 0 = 关
	}).Run(context.Background(), "echo")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	done := findCompleted(t, h.events())
	if done.BinaryRef != nil {
		t.Fatalf("阈值 0 应不外置，BinaryRef 应空，got %v", *done.BinaryRef)
	}
	if blob.count() != 0 {
		t.Fatalf("阈值 0 不该写 blob，写了 %d 份", blob.count())
	}
	// 逐字节等价：payload == 完整 result 内联的 JSON（未被截断/改写/加占位）。
	want, err := json.Marshal(contracts.ToolCallCompletedPayload{
		ToolCallID: "tc_D4_ZERO",
		Result:     contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &big}}},
	})
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if !bytes.Equal(done.Payload, want) {
		t.Fatalf("阈值 0 时 completed 载荷应与完整内联逐字节等价：\ngot  %s\nwant %s", done.Payload, want)
	}
}

// TestD4SmallResultNotExternalized：挂了 sink + 阈值 >0 + **小 result** → 不外置，
// 且 payload 与完整内联**逐字节相同**（F4：阈值内的那条路也必须是酸测试，不是"看起来没变"）。
//
// 为什么要单独一条：外置判据是"序列化后长度 > 阈值"——这条判据的**内侧**（≤阈值）必须钉住
// 逐字节等价，否则"阈值判定写反/写偏一字节"这类变异可能只在边界上才红。
func TestD4SmallResultNotExternalized(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	small := "短输出"
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &small}}}
	blob := newFakeBlob()
	h.script(
		stepTools(call("tc_D4_SMALL", "echo", `{"text":"hi"}`)),
		stepText("完成"),
	)

	_, err := h.loop(func(c *loop.Config) {
		c.BlobSink = blob
		c.BlobMaxInline = 4096 // 阈值远大于小 result → 内联
	}).Run(context.Background(), "echo")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	done := findCompleted(t, h.events())
	if done.BinaryRef != nil {
		t.Fatalf("阈值内不应外置，BinaryRef 应空，got %v", *done.BinaryRef)
	}
	if blob.count() != 0 {
		t.Fatalf("阈值内不该写 blob，写了 %d 份", blob.count())
	}
	want, err := json.Marshal(contracts.ToolCallCompletedPayload{
		ToolCallID: "tc_D4_SMALL",
		Result:     contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &small}}},
	})
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if !bytes.Equal(done.Payload, want) {
		t.Fatalf("阈值内 completed 载荷应与完整内联逐字节相同：\ngot  %s\nwant %s", done.Payload, want)
	}
}

// TestD4ThresholdBoundaryEqualNotExternalized（F5）：序列化后长度**恰好等于**阈值 → 不外置。
//
// 边界口径是"**超过**阈值才外置"（≤ 阈值 = 内联）。这条钉住的是边界的那一侧：把 `<=` 写成
// `<`（或反过来）必须红。阈值取自"同一个 ToolResult 序列化后的字节数"——自洽地落在边界上。
func TestD4ThresholdBoundaryEqualNotExternalized(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	mid := strings.Repeat("M", 300)
	res := contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &mid}}}
	raw, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	exec := h.addTool("echo", nil)
	exec.result = res
	blob := newFakeBlob()
	h.script(
		stepTools(call("tc_D4_EDGE", "echo", `{"text":"hi"}`)),
		stepText("完成"),
	)

	_, err = h.loop(func(c *loop.Config) {
		c.BlobSink = blob
		c.BlobMaxInline = int64(len(raw)) // 恰好等于 → 不外置
	}).Run(context.Background(), "echo")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	done := findCompleted(t, h.events())
	if done.BinaryRef != nil {
		t.Fatalf("长度恰等于阈值（%d）应不外置，BinaryRef 应空，got %v", len(raw), *done.BinaryRef)
	}
	if blob.count() != 0 {
		t.Fatalf("长度恰等于阈值不该写 blob，写了 %d 份", blob.count())
	}
	want, err := json.Marshal(contracts.ToolCallCompletedPayload{
		ToolCallID: "tc_D4_EDGE",
		Result:     res,
	})
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if !bytes.Equal(done.Payload, want) {
		t.Fatalf("边界（恰等于阈值）应逐字节内联：\ngot  %s\nwant %s", done.Payload, want)
	}
}

// TestD4LargeResultExternalized：大 result → 外置（BinaryRef 非空 + blob 落全文 +
// payload 是摘要）+ 模型仍拿完整 result + X1 三段序不变。
func TestD4LargeResultExternalized(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	full := strings.Repeat("X", 5000) // 超阈值
	edges := []contracts.GraphEdge{{Src: "a", Dst: "b", Kind: "imports"}}
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{
		Content:    []contracts.ContentPart{{Type: "text", Text: &full}},
		GraphEdges: edges,
	}
	blob := newFakeBlob()
	h.script(
		stepTools(call("tc_D4_BIG", "echo", `{"text":"hi"}`)),
		stepText("完成"),
	)

	_, err := h.loop(func(c *loop.Config) {
		c.BlobSink = blob
		c.BlobMaxInline = 64 // 小阈值 → 5000 字节必外置
	}).Run(context.Background(), "echo")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// X1 三段序不变：intent → completed。
	if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallCompleted}; len(got) != 2 ||
		got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("写路径 kind 序 = %v，want %v", got, want)
	}

	done := findCompleted(t, h.events())
	if done.BinaryRef == nil {
		t.Fatal("大 result 应外置：BinaryRef 不应为空")
	}
	if blob.count() != 1 {
		t.Fatalf("应恰好写一份 blob，got %d", blob.count())
	}

	// blob 里是**完整** ToolResult JSON（读侧按 BinaryRef 取回全文）。
	stored := blob.data[*done.BinaryRef]
	var fullResult contracts.ToolResult
	if err := json.Unmarshal(stored, &fullResult); err != nil {
		t.Fatalf("blob 内容不是合法 ToolResult JSON: %v", err)
	}
	if len(fullResult.Content) != 1 || fullResult.Content[0].Text == nil || *fullResult.Content[0].Text != full {
		t.Fatalf("blob 里的全文与原始 result 不符")
	}
	if len(fullResult.GraphEdges) != 1 || fullResult.GraphEdges[0] != edges[0] {
		t.Fatalf("blob 里的 GraphEdges 应原样保留: %+v", fullResult.GraphEdges)
	}

	// payload 是**摘要形态**：text 换成「占位 + 预览」、GraphEdges 保留、结构不变（可解）。
	var got contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(done.Payload, &got); err != nil {
		t.Fatalf("completed payload 不可解: %v", err)
	}
	if len(got.Result.Content) != 1 || got.Result.Content[0].Text == nil {
		t.Fatalf("摘要应保留一个 text part: %+v", got.Result.Content)
	}
	summary := *got.Result.Content[0].Text
	// 占位标记必须在（折叠侧不看 BinaryRef → 摘要要自解释，F3）。
	if !strings.Contains(summary, blobSummaryMarkerText) {
		t.Fatalf("摘要应含外置占位标记 %q，got %q", blobSummaryMarkerText, summary)
	}
	if !strings.Contains(summary, string(*done.BinaryRef)) {
		t.Fatalf("摘要占位应点名 blob id %s，got %q", *done.BinaryRef, summary)
	}
	// 占位之后是**预览**：全文的前 256 个字符（rune 计）。
	preview := string([]rune(full)[:256])
	if !strings.HasSuffix(summary, preview) {
		t.Fatalf("摘要应以全文前 256 字收尾（预览），got %q", summary)
	}
	if !strings.HasPrefix(full, preview) {
		t.Fatalf("摘要预览应是全文的前缀")
	}
	if len(got.Result.GraphEdges) != 1 || got.Result.GraphEdges[0] != edges[0] {
		t.Fatalf("摘要应保留 GraphEdges（索引器靠它建边）: %+v", got.Result.GraphEdges)
	}

	// 模型回填仍拿**完整** result（外置只瘦身事件流，不让模型看不到完整输出）。
	reqs := h.provider.reqs()
	if len(reqs) < 2 {
		t.Fatalf("应有第 2 轮请求（工具结果回填后），got %d", len(reqs))
	}
	var toolMsg string
	for _, m := range reqs[1].Messages {
		if m.Role == "tool" {
			toolMsg = m.Content
		}
	}
	if toolMsg != full {
		t.Fatalf("模型应拿到完整工具输出（%d 字符），got %d 字符", len(full), len(toolMsg))
	}
}

// TestD4FailedNotExternalized：tool.call.failed **不外置**（保持原样，BinaryRef 空，不写 blob）。
func TestD4FailedNotExternalized(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)
	exec.err = io.ErrUnexpectedEOF // 执行失败 → failed
	blob := newFakeBlob()
	h.script(
		stepTools(call("tc_D4_FAIL", "echo", `{"text":"hi"}`)),
		stepText("收尾"),
	)

	_, err := h.loop(func(c *loop.Config) {
		c.BlobSink = blob
		c.BlobMaxInline = 1 // 阈值极小：若有外置逻辑碰到 failed 就会触发
	}).Run(context.Background(), "echo")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	failed := findFailed(t, h.events())
	if failed.BinaryRef != nil {
		t.Fatalf("failed 不应外置，BinaryRef 应空，got %v", *failed.BinaryRef)
	}
	if blob.count() != 0 {
		t.Fatalf("failed 不该写 blob，写了 %d 份", blob.count())
	}
}

// TestD4FailOpenKeepsCompletedInline（F2）：blob.Put **恒返错** → fail-open：
// ① completed 仍落盘（X1 不破）② payload 与完整内联**逐字节相同** ③ BinaryRef 空。
//
// 为什么这条必须存在：fail-open 是 D4 唯一的容错语义，但**只有 blob 真写失败**才走到——
// 不注入失败就没有覆盖（变异"Put 失败就中止 loop"或"Put 失败就丢 completed"都会存活）。
// 断言"仍落 + 逐字节原样 + 无引用"三条缺一不可：只断言"Run 没报错"太弱（中止也会报错）。
func TestD4FailOpenKeepsCompletedInline(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	big := strings.Repeat("F", 5000)
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &big}}}
	h.script(
		stepTools(call("tc_D4_PUTFAIL", "echo", `{"text":"hi"}`)),
		stepText("完成"),
	)

	_, err := h.loop(func(c *loop.Config) {
		c.BlobSink = failingBlob{err: errors.New("blob: 磁盘满了（注入的失败）")}
		c.BlobMaxInline = 1 // 阈值极小 → 必走外置分支 → 必撞上 Put 返错
	}).Run(context.Background(), "echo")
	if err != nil {
		t.Fatalf("Put 失败时 fail-open：Run 不该报错（completed 仍落），got %v", err)
	}

	// ① completed 仍落盘（X1：有 intent 就要有 result）。
	if got, want := h.writePathKinds(), []string{loop.KindToolCallIntent, loop.KindToolCallCompleted}; len(got) != 2 ||
		got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("写路径 kind 序 = %v，want %v（fail-open 也要有 completed）", got, want)
	}
	done := findCompleted(t, h.events())

	// ③ BinaryRef 空（没外置成功就不出引用——不许指向没写成功的内容）。
	if done.BinaryRef != nil {
		t.Fatalf("外置失败时 BinaryRef 应为空，got %v", *done.BinaryRef)
	}
	// ② payload 与完整内联逐字节相同（fail-open = 原样保留，不截断、不加占位）。
	want, err := json.Marshal(contracts.ToolCallCompletedPayload{
		ToolCallID: "tc_D4_PUTFAIL",
		Result:     contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &big}}},
	})
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if !bytes.Equal(done.Payload, want) {
		t.Fatalf("外置失败时 completed 载荷应与完整内联逐字节相同：\ngot  %s\nwant %s", done.Payload, want)
	}
}

// TestD4DataOnlyResultSummaryHasPlaceholder（F3）：**Data-only** 结果（无 text 只有 data）外置后
// 摘要**不留空块**——给出可辨识占位（payload 正文非空 + 含占位标记）。
//
// 为什么必须钉：若只把 Data 置空就完事，摘要里会留下一个"type 在、text/data 全无"的空块——
// 读者（折叠侧/resume 续跑**不看 BinaryRef**）无从判断"这里原本有大载荷吗"。
func TestD4DataOnlyResultSummaryHasPlaceholder(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	data := json.RawMessage(`{"rows":[` + strings.Repeat(`{"k":"v"},`, 200) + `{"k":"v"}]}`)
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "data", Data: data}}}
	blob := newFakeBlob()
	h.script(
		stepTools(call("tc_D4_DATAONLY", "echo", `{"text":"hi"}`)),
		stepText("完成"),
	)

	_, err := h.loop(func(c *loop.Config) {
		c.BlobSink = blob
		c.BlobMaxInline = 64 // 小阈值 → 必外置
	}).Run(context.Background(), "echo")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	done := findCompleted(t, h.events())
	if done.BinaryRef == nil {
		t.Fatal("大 Data 结果应外置：BinaryRef 不应为空")
	}
	var got contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(done.Payload, &got); err != nil {
		t.Fatalf("completed payload 不可解: %v", err)
	}
	if len(got.Result.Content) != 1 {
		t.Fatalf("摘要应保留一个 part: %+v", got.Result.Content)
	}
	part := got.Result.Content[0]
	if part.Text == nil || *part.Text == "" {
		t.Fatalf("Data-only 结果的摘要正文不应为空块（应有占位说明）: %+v", part)
	}
	if !strings.Contains(*part.Text, blobSummaryMarkerText) {
		t.Fatalf("Data-only 摘要应含外置占位标记 %q，got %q", blobSummaryMarkerText, *part.Text)
	}
	if !strings.Contains(*part.Text, string(*done.BinaryRef)) {
		t.Fatalf("Data-only 摘要占位应点名 blob id %s，got %q", *done.BinaryRef, *part.Text)
	}
}

// TestD4DataClearedInSummaryButFullInBlob（F6）：结果含**大 Data** → ①摘要里 Data 置空
// ②blob 全文里 Data 完整 ③读侧（按 BinaryRef 取回）能还原完整 Data。
//
// 为什么分三条断言：只断言"摘要 Data 为空"会漏掉"外置时把 Data 一起丢了"（内容丢失）；
// 只断言"blob 里有 Data"会漏掉"摘要里还留着大 Data"（那就没有瘦身，等于没外置）。
func TestD4DataClearedInSummaryButFullInBlob(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	data := json.RawMessage(`{"rows":[` + strings.Repeat(`{"k":"v"},`, 200) + `{"k":"v"}]}`)
	text := "头部说明"
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{
		{Type: "text", Text: &text},
		{Type: "data", Data: data},
	}}
	blob := newFakeBlob()
	h.script(
		stepTools(call("tc_D4_DATA", "echo", `{"text":"hi"}`)),
		stepText("完成"),
	)

	_, err := h.loop(func(c *loop.Config) {
		c.BlobSink = blob
		c.BlobMaxInline = 64
	}).Run(context.Background(), "echo")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	done := findCompleted(t, h.events())
	if done.BinaryRef == nil {
		t.Fatal("大 Data 结果应外置：BinaryRef 不应为空")
	}

	// ① 摘要里 Data 置空（结构化载荷整体外置）。
	var got contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(done.Payload, &got); err != nil {
		t.Fatalf("completed payload 不可解: %v", err)
	}
	if len(got.Result.Content) != 2 {
		t.Fatalf("摘要应保留两个 part: %+v", got.Result.Content)
	}
	if len(got.Result.Content[1].Data) != 0 {
		t.Fatalf("摘要里 Data 应置空，got %s", got.Result.Content[1].Data)
	}
	if got.Result.Content[1].Text == nil || *got.Result.Content[1].Text == "" {
		t.Fatalf("Data 块的摘要不应是空块（要有占位说明）: %+v", got.Result.Content[1])
	}

	// ② blob 全文里 Data 完整（逐字节）。
	stored := blob.data[*done.BinaryRef]
	var full contracts.ToolResult
	if err := json.Unmarshal(stored, &full); err != nil {
		t.Fatalf("blob 内容不是合法 ToolResult JSON: %v", err)
	}
	if len(full.Content) != 2 {
		t.Fatalf("blob 全文应有两个 part: %+v", full.Content)
	}
	if !bytes.Equal(full.Content[1].Data, data) {
		t.Fatalf("blob 全文里的 Data 应与原始逐字节相同（got %d 字节，want %d 字节）",
			len(full.Content[1].Data), len(data))
	}

	// ③ 读侧还原：按 BinaryRef 取回全文 → Data 完整（消费方拿得到原件）。
	rc, err := blob.Get(context.Background(), *done.BinaryRef)
	if err != nil {
		t.Fatalf("读侧取 blob: %v", err)
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("读 blob 内容: %v", err)
	}
	var restored contracts.ToolResult
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatalf("读回的 blob 不是合法 ToolResult JSON: %v", err)
	}
	if !bytes.Equal(restored.Content[1].Data, data) {
		t.Fatalf("读侧还原的 Data 应完整（got %d 字节，want %d 字节）",
			len(restored.Content[1].Data), len(data))
	}
}

// TestD4ChildAgentResultAlsoExternalized（F1）：父 loop 挂了 blob → **子 agent 的大 result
// 同样外置**（completed 的 BinaryRef 非空）。
//
// 为什么必须钉：buildChildLoop 透传了别的字段（model/档位/策略…）却漏了 BlobSink/BlobMaxInline
// → 子 agent 的大输出**留在事件流里**（不进 blob）——"大输出不进事件流"这条规则就只在主 agent
// 上成立，多 agent 模式下 D4 落空（与文档口径不符）。父挂了、子没挂 = 能力漂移（同 G14 档位
// 漏传那一类）。
func TestD4ChildAgentResultAlsoExternalized(t *testing.T) {
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"scout"}, Tools: nil, Model: maRootModel, PermissionRequest: permFull()}
	h := newMAHarness(t, rootType, 3, allowRule("read"))
	h.registerType(agents.AgentType{ID: "scout", CanSpawn: nil, Tools: []string{"read"}, Model: "scout-model", PermissionRequest: permRO()})
	exec := h.addTool("read", nil)
	big := strings.Repeat("Z", 5000)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &big}}}

	blob := newFakeBlob()
	h.prov.script(maRootModel,
		stepTools(call("spawn1", loop.ToolSpawn, string(spawnArgs("scout", "recon", "读点东西", core.NewUlid())))),
		stepText("主 agent 收尾"),
	)
	h.prov.script("scout-model",
		stepTools(call("read1", "read", `{"path":"x"}`)),
		stepTools(call("done1", loop.ToolComplete, string(completeArgs("读完了", nil, agents.TaintClean)))),
	)

	root, err := loop.New(loop.Config{
		Provider:  h.prov,
		Bus:       h.bus,
		Registry:  h.reg,
		Policy:    h.engine,
		Approver:  core.DenyAll{},
		SessionID: h.session,
		AgentID:   h.tree.Root(),
		Model:     maRootModel,
		MaxTurns:  10,
		Agents:    &loop.Agents{Tree: h.tree, Verifier: h.verifier},
		// D4：父挂了 blob → 子必须继承（buildChildLoop 透传）。
		BlobSink:      blob,
		BlobMaxInline: 64,
	})
	if err != nil {
		t.Fatalf("loop.New(root): %v", err)
	}
	if _, err := root.Run(context.Background(), "派个活"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 子 agent id：来自 agent.spawned 事件。
	var childID contracts.Ulid
	for _, ev := range h.events() {
		if ev.Kind != "agent.spawned" {
			continue
		}
		var p contracts.AgentSpawnedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("解 agent.spawned: %v", err)
		}
		childID = p.ChildAgentID
	}
	if childID == "" {
		t.Fatal("没有 agent.spawned 事件（子 agent 没被派生？）")
	}

	// 子 agent 的工具调用 completed：BinaryRef 必须非空（子的大 result 也外置）。
	evs := h.events()
	var childDone *contracts.Event
	for i := range evs {
		if evs[i].Kind == loop.KindToolCallCompleted && evs[i].AgentID == childID {
			childDone = &evs[i]
			break
		}
	}
	if childDone == nil {
		t.Fatal("子 agent 没有 tool.call.completed 事件")
	}
	if childDone.BinaryRef == nil {
		t.Fatal("子 agent 的大 result 应外置：BinaryRef 不应为空（buildChildLoop 漏传 BlobSink/BlobMaxInline？）")
	}
	// blob 份数 = **被外置的 completed 条数**（D4 是全路径同口径：父的 spawn 摘要也可能
	// 超阈值被外置——core/loop 里三条写 tool.call.completed 的路径都走 externalizeResult）。
	var externalized int
	for _, ev := range h.events() {
		if ev.Kind == loop.KindToolCallCompleted && ev.BinaryRef != nil {
			externalized++
		}
	}
	if externalized < 1 {
		t.Fatal("至少应有一条 completed 被外置")
	}
	if blob.count() != externalized {
		t.Fatalf("blob 份数 = %d，应等于被外置的 completed 条数 %d", blob.count(), externalized)
	}
	// blob 里是子 agent 的完整 result。
	stored := blob.data[*childDone.BinaryRef]
	var full contracts.ToolResult
	if err := json.Unmarshal(stored, &full); err != nil {
		t.Fatalf("blob 内容不是合法 ToolResult JSON: %v", err)
	}
	if len(full.Content) != 1 || full.Content[0].Text == nil || *full.Content[0].Text != big {
		t.Fatal("blob 里应是子 agent 的完整 result")
	}
}
