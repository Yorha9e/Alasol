package loop_test

// F4 的钉子：**小阈值下"外置 = 瘦身"必须成立**（外置不许把事件流撑大）。
//
// # 原症状（F4）
//
// 外置的收益是"事件流变小"（大载荷进 blob，事件流只留引用 + 摘要）。但摘要有**固定成本**：
// 每个预览 part 一条占位（≈70 字节）+ JSON 结构，4 个 part 时约 500 字节；再加每 part
// 256 字的预览正文。**阈值很小的时候**（1KB 量级），刚过阈值的原文折出来的摘要可能
// **比原文还大**——那就不是外置，是"搬运 + 膨胀"（事件流里同时躺着摘要和 blob 引用）。
//
// 典型构造：**多个短 part**。短 part 的正文（< 256 字）会被预览**照单全收**——占位的
// 固定开销直接叠在原文上：5 个 208 字节的 part → 原文 ≈ 1.2KB，旧口径的摘要 ≈ 1.35KB。
//
// # 口径（写明）
//
//	摘要预算 B = min(2KB, max(阈值/2, 768))   （core/loop/blob.go: blobSummaryBudget）
//	有效阈值 T = max(阈值, B)                （core/loop/blob.go: blobEffectiveThreshold）
//	外置当且仅当 原文 > T ≥ B ≥ 摘要
//
// 即：过了有效阈值才外置，而那时原文一定比摘要大（结构性保证，不靠"碰巧"）。
//
// # 变异自证
//
// 把摘要预算改回"固定 2KB"且阈值原样判（即撤掉钳制）→ 本钉子红（摘要 1.35KB > 内联 1.2KB）。

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/loop"
)

// TestD4SmallThresholdExternalizationAlwaysSlims（F4 的钉子）：阈值 1024 + **多 part** 结果
// → 外置后 completed 的 payload 字节 **严格小于** 原样内联时的字节。
//
// 为什么必须钉：没有它，"外置"在**小阈值**下可能反而让事件流更大（摘要 > 原文）——
// 那就与 D4 的立意（事件流瘦身）相反，而且**越是在小阈值这种"看起来无害"的配置下发生**。
// 只钉"大 result 被外置"（既有钉子）抓不到这一条：那些用例的原文都远大于摘要。
//
// 断言三条：① 确实外置了（BinaryRef 非空——本钉子考的是"外置之后更小"，不是"没外置"）；
// ② payload 是**摘要**形态（占位标记在、原文不在）；③ payload 字节 < 原样内联字节。
func TestD4SmallThresholdExternalizationAlwaysSlims(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	// 5 个**短** part（每个 ~208 字节）：正文短于 256 字预览 → 预览照单全收 →
	// 旧口径下摘要（4 个预览 + 1 条合并占位）比原文还大。
	parts := make([]contracts.ContentPart, 0, 5)
	for i := 0; i < 5; i++ {
		text := fmt.Sprintf("part-%02d:", i) + strings.Repeat("y", 200)
		parts = append(parts, contracts.ContentPart{Type: "text", Text: &text})
	}
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: parts}
	blob := newFakeBlob()
	h.script(
		stepTools(call("tc_D4_SLIM", "echo", `{"text":"hi"}`)),
		stepText("完成"),
	)

	_, err := h.loop(func(c *loop.Config) {
		c.BlobSink = blob
		c.BlobMaxInline = 1024 // 小阈值 → 1.2KB 的多 part 结果必外置
	}).Run(context.Background(), "echo")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	done := findCompleted(t, h.events())
	// ① 确实外置了。
	if done.BinaryRef == nil {
		t.Fatal("多 part 大 result 应外置：BinaryRef 不应为空")
	}
	if blob.count() != 1 {
		t.Fatalf("应恰好写一份 blob，got %d", blob.count())
	}

	// ② payload 是摘要形态（占位在、原文不在）。
	var got contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(done.Payload, &got); err != nil {
		t.Fatalf("completed payload 不可解: %v", err)
	}
	if len(got.Result.Content) != blobSummaryPreviewPartsForTest+1 {
		t.Fatalf("摘要 part 数 = %d，want %d（前 K 个预览 + 1 条合并占位）",
			len(got.Result.Content), blobSummaryPreviewPartsForTest+1)
	}
	if got.Result.Content[0].Text == nil || !strings.Contains(*got.Result.Content[0].Text, blobSummaryMarkerText) {
		t.Fatalf("摘要应含外置占位标记，got %+v", got.Result.Content[0])
	}

	// ③ **外置后 payload 严格小于原样内联**（"外置 = 瘦身"）。
	inline, err := json.Marshal(contracts.ToolCallCompletedPayload{
		ToolCallID: "tc_D4_SLIM",
		Result:     contracts.ToolResult{Content: parts},
	})
	if err != nil {
		t.Fatalf("marshal 原样内联: %v", err)
	}
	if len(done.Payload) >= len(inline) {
		t.Fatalf("外置后 payload %d 字节，应**小于**原样内联的 %d 字节"+
			"（小阈值下外置不许把事件流撑大：摘要预算须随阈值收缩 + 有效阈值钳到预算之上）",
			len(done.Payload), len(inline))
	}
}
