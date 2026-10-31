package loop_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/durable"
	"github.com/Yorha9e/alasol/core/loop"
)

// 本文件是 D4（大 payload 外置 blob，G2e）的**接线面**钉子：
//
//   - F1：**重放路径**（durable.Resume → Loop.Replay）同样外置——重放与原执行是两条不同的
//     写路径（Replay 自己造 completed 事件），只接原执行 = resume 路径上"大 payload 不进
//     事件流"这条规则失效。
//   - F2：摘要**总量上限**——只给前 K 个 part 预览、其余合并成一条占位、摘要整体有字节上限
//     （否则多 part 时摘要自己就比原文还大：外置反而把事件流撑大）。

// blobSummaryMaxBytesForTest 是摘要的**总字节上限**（生产常量 2048）。
//
// 包外测试钉的是**可观测行为**，故这里写死同一个数——改生产常量必须同步改这里，
// 否则钉子会红（这是故意的：上限变了就该重新确认摘要形状）。
const blobSummaryMaxBytesForTest = 2048

// blobSummaryPreviewPartsForTest 是摘要里**给预览**的 part 数上限（生产常量 4）。
const blobSummaryPreviewPartsForTest = 4

// TestD4ReplayPathExternalized（F1 的钉子之二）：**重放路径**落下的 completed 同样外置
// （BinaryRef 非空 + payload 是摘要 + blob 里有全文）。
//
// 为什么必须钉：重放与原执行是**两条不同的写路径**——Replay 自己造 completed 事件
// （见 core/loop/replay.go 的 newReplayEvent），**不经过 runToolCall**。只给原执行接上外置，
// 重放这条路就会把大 result 原样塞回事件流：resume 一次就能在事件流里落下几十 MB
// （事件流要被回放/转发/索引，F12）——"大 payload 不进事件流"这条规则在 resume 路径上失效。
//
// 口径：重放产出的 completed 与原执行**同口径**（同一形状：摘要 + BinaryRef）。
func TestD4ReplayPathExternalized(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	big := strings.Repeat("R", 5000)
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &big}}}
	blob := newFakeBlob()

	l := h.loop(func(c *loop.Config) {
		c.BlobSink = blob
		c.BlobMaxInline = 64 // 小阈值 → 5000 字节必外置
	})
	op := durable.Op{
		Intent: contracts.ToolCallIntentPayload{
			ToolCallID: "tc_D4_REPLAY",
			ToolName:   "echo",
			Arguments:  json.RawMessage(`{"text":"hi"}`),
		},
		IntentEventID: core.NewUlid(),
		SessionID:     h.session,
		AgentID:       h.agent,
	}
	res, err := l.Replay(context.Background(), op)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !res.OK {
		t.Fatalf("重放应成功（落 completed），got %+v", res)
	}

	done := findCompleted(t, h.events())
	if done.BinaryRef == nil {
		t.Fatal("重放路径的大 result 应外置：BinaryRef 不应为空（Replay 没走 externalizeResult？）")
	}
	if blob.count() != 1 {
		t.Fatalf("应恰好写一份 blob，got %d", blob.count())
	}
	// blob 里是**完整** ToolResult JSON（读侧按 BinaryRef 取回全文）。
	var full contracts.ToolResult
	if err := json.Unmarshal(blob.data[*done.BinaryRef], &full); err != nil {
		t.Fatalf("blob 内容不是合法 ToolResult JSON: %v", err)
	}
	if len(full.Content) != 1 || full.Content[0].Text == nil || *full.Content[0].Text != big {
		t.Fatal("blob 里应是重放落盘的完整 result")
	}
	// payload 是**摘要形态**（与原执行同口径）：占位 + 预览，不是原文。
	var got contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(done.Payload, &got); err != nil {
		t.Fatalf("completed payload 不可解: %v", err)
	}
	if len(got.Result.Content) != 1 || got.Result.Content[0].Text == nil {
		t.Fatalf("摘要应保留一个 text part: %+v", got.Result.Content)
	}
	summary := *got.Result.Content[0].Text
	if !strings.Contains(summary, blobSummaryMarkerText) {
		t.Fatalf("重放摘要应含外置占位标记 %q，got %q", blobSummaryMarkerText, summary)
	}
	if strings.Contains(summary, big) {
		t.Fatal("重放摘要里不该留着完整原文（外置没生效？）")
	}
}

// TestD4SummaryMergesManyParts（F2 的钉子之一）：**100 个 part** 的结果 → 摘要**远小于**原文。
//
// 为什么必须钉（F2 的原症状）：摘要里每个 part 都带一份「占位 + 预览」——part 一多，
// 摘要自己就比原文还大（线性膨胀 3 倍+）："外置是为了让事件流变小"落空了（更糟：事件流里
// 同时躺着摘要和 blob 引用，体积反而涨了）。修法：只给**前 K 个** part 预览，其余**合并成
// 一条**占位。
//
// 断言：① 摘要 part 数 = K + 1 条合并占位 ② 合并占位点名 blob id 与被合并的 part 数
// ③ 摘要长度 << 原文长度（原文 = blob 里的全文）④ 摘要在字节上限内。
func TestD4SummaryMergesManyParts(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	parts := make([]contracts.ContentPart, 0, 100)
	for i := 0; i < 100; i++ {
		text := fmt.Sprintf("part-%03d:", i) + strings.Repeat("x", 5000)
		parts = append(parts, contracts.ContentPart{Type: "text", Text: &text})
	}
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: parts}
	blob := newFakeBlob()
	h.script(
		stepTools(call("tc_D4_MANY", "echo", `{"text":"hi"}`)),
		stepText("完成"),
	)

	_, err := h.loop(func(c *loop.Config) {
		c.BlobSink = blob
		c.BlobMaxInline = 1024
	}).Run(context.Background(), "echo")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	done := findCompleted(t, h.events())
	if done.BinaryRef == nil {
		t.Fatal("大 result 应外置：BinaryRef 不应为空")
	}
	var got contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(done.Payload, &got); err != nil {
		t.Fatalf("completed payload 不可解: %v", err)
	}
	// ① 摘要 part 数 = 前 K 个 + 1 条合并占位。
	wantParts := blobSummaryPreviewPartsForTest + 1
	if len(got.Result.Content) != wantParts {
		t.Fatalf("摘要 part 数 = %d，want %d（前 %d 个预览 + 1 条合并占位）",
			len(got.Result.Content), wantParts, blobSummaryPreviewPartsForTest)
	}
	// ② 合并占位：点名 blob id + 被合并的 part 数（100 - 4 = 96）。
	last := got.Result.Content[wantParts-1]
	if last.Text == nil {
		t.Fatalf("合并占位应是文本 part: %+v", last)
	}
	if !strings.Contains(*last.Text, "还有 96 个 part") {
		t.Fatalf("合并占位应说明还有 96 个 part，got %q", *last.Text)
	}
	if !strings.Contains(*last.Text, string(*done.BinaryRef)) {
		t.Fatalf("合并占位应点名 blob id %s，got %q", *done.BinaryRef, *last.Text)
	}
	// ③④ 摘要 << 原文，且在字节上限内。
	sumRaw, err := json.Marshal(got.Result)
	if err != nil {
		t.Fatalf("marshal 摘要: %v", err)
	}
	fullRaw := blob.data[*done.BinaryRef]
	if len(sumRaw) >= len(fullRaw) {
		t.Fatalf("摘要长度 %d 应**远小于**原文 %d（多 part 时摘要不能反过来把事件流撑大）",
			len(sumRaw), len(fullRaw))
	}
	if len(sumRaw) > blobSummaryMaxBytesForTest {
		t.Fatalf("摘要 %d 字节，超出总字节上限 %d", len(sumRaw), blobSummaryMaxBytesForTest)
	}
}

// TestD4PreviewExactly256Runes（F2 的钉子之二）：预览长度**恰 256 rune**，内容取**前** 256 个
// 字符，且原文用**非重复字符**（"p0p1p2…"）。
//
// 为什么用非重复字符：原文全是同一个字母时，"取前 256 个"和"取后 256 个"**长得一样**——
// 截断方向写反了也测不出来。混合文本才能分辨方向。
func TestD4PreviewExactly256Runes(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	var sb strings.Builder
	for i := 0; sb.Len() < 3000; i++ {
		fmt.Fprintf(&sb, "p%d", i)
	}
	full := sb.String()
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &full}}}
	blob := newFakeBlob()
	h.script(
		stepTools(call("tc_D4_PREVIEW", "echo", `{"text":"hi"}`)),
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
		t.Fatal("大 result 应外置：BinaryRef 不应为空")
	}
	var got contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(done.Payload, &got); err != nil {
		t.Fatalf("completed payload 不可解: %v", err)
	}
	if len(got.Result.Content) != 1 || got.Result.Content[0].Text == nil {
		t.Fatalf("摘要应保留一个 text part: %+v", got.Result.Content)
	}
	summary := *got.Result.Content[0].Text
	preview := string([]rune(full)[:256])
	// 摘要应**逐字**等于「占位（声明 256 字）+ 预览（原文前 256 rune）」。
	want := "[已外置：全文见 blob " + string(*done.BinaryRef) + "；前 256 字如下]" + preview
	if summary != want {
		t.Fatalf("摘要应恰为「占位 + 原文前 256 rune 预览」：\ngot  %q\nwant %q", summary, want)
	}
	// 预览**恰 256 rune**（不多不少；原文用非重复字符 → 方向错立刻被上面的逐字比对抓住）。
	if n := len([]rune(preview)); n != 256 {
		t.Fatalf("预览 rune 数 = %d，want 256", n)
	}
}

// TestD4SummaryByteCap（F2 的钉子之三）：超大 part → 摘要整体**字节上限**生效；
// 超限时裁的是**预览正文**，占位（自解释）**不可裁**。
//
// 为什么必须钉：part 数上限（K）限不住**单 part 预览的体积**——4 个 part × 256 rune 的 CJK
// 文本 ≈ 3KB（占位再加几百字节）→ 没有字节上限，摘要本身就几 KB，每条事件都这么胖。
func TestD4SummaryByteCap(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	parts := make([]contracts.ContentPart, 0, 4)
	for i := 0; i < 4; i++ {
		text := fmt.Sprintf("第%d段：", i) + strings.Repeat("中文内容", 400) // 每段 ≈ 1600 字节
		parts = append(parts, contracts.ContentPart{Type: "text", Text: &text})
	}
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: parts}
	blob := newFakeBlob()
	h.script(
		stepTools(call("tc_D4_CAP", "echo", `{"text":"hi"}`)),
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
		t.Fatal("大 result 应外置：BinaryRef 不应为空")
	}
	var got contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(done.Payload, &got); err != nil {
		t.Fatalf("completed payload 不可解: %v", err)
	}
	sumRaw, err := json.Marshal(got.Result)
	if err != nil {
		t.Fatalf("marshal 摘要: %v", err)
	}
	if len(sumRaw) > blobSummaryMaxBytesForTest {
		t.Fatalf("摘要 %d 字节，超出总字节上限 %d（单 part 预览叠加必须被压住）",
			len(sumRaw), blobSummaryMaxBytesForTest)
	}
	// 占位（自解释）**不可裁**：每个 part 都还得说清"全文在哪个 blob"。
	for i, p := range got.Result.Content {
		if p.Text == nil || !strings.Contains(*p.Text, blobSummaryMarkerText) {
			t.Fatalf("第 %d 个摘要 part 应保留占位标记（自解释不可裁），got %+v", i, p)
		}
	}
	// blob 里仍是**完整**原文（裁的只是摘要，不是内容）。
	var full contracts.ToolResult
	if err := json.Unmarshal(blob.data[*done.BinaryRef], &full); err != nil {
		t.Fatalf("blob 内容不是合法 ToolResult JSON: %v", err)
	}
	if len(full.Content) != 4 || *full.Content[3].Text != *parts[3].Text {
		t.Fatal("blob 里应是完整原文（字节上限只作用于摘要）")
	}
}
