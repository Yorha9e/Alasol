package compaction

import (
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// ref 指针、tier 单向降级、L1/L2 条目渲染（refs 永不指摘要）。

func TestStageRefRoundTrip(t *testing.T) {
	if got := StageRef(5); got != "stage:5" {
		t.Fatalf("StageRef(5) = %q, want stage:5", got)
	}
	n, ok := ParseStageRef("stage:5")
	if !ok || n != 5 {
		t.Fatalf("ParseStageRef(stage:5) = (%d,%v), want (5,true)", n, ok)
	}
	for _, bad := range []string{"", "stage:", "stage:0", "stage:-1", "stage:x", "sum:ABC", "e:1..e:2"} {
		if _, ok := ParseStageRef(bad); ok {
			t.Errorf("ParseStageRef(%q) 应非法", bad)
		}
	}
}

func TestCanDowngradeOneWay(t *testing.T) {
	// full→l1→l2 合法；回升非法；相等合法（幂等）。
	if !canDowngrade(contracts.ContextTierFull, contracts.ContextTierL1) {
		t.Error("full→l1 应合法")
	}
	if !canDowngrade(contracts.ContextTierL1, contracts.ContextTierL2) {
		t.Error("l1→l2 应合法")
	}
	if !canDowngrade(contracts.ContextTierFull, contracts.ContextTierL2) {
		t.Error("full→l2 应合法")
	}
	if canDowngrade(contracts.ContextTierL1, contracts.ContextTierFull) {
		t.Error("l1→full 应非法（永不回升）")
	}
	if canDowngrade(contracts.ContextTierL2, contracts.ContextTierL1) {
		t.Error("l2→l1 应非法（永不回升）")
	}
	if !canDowngrade(contracts.ContextTierL2, contracts.ContextTierL2) {
		t.Error("l2→l2 应合法（幂等）")
	}
	// nextTier 单调压深、触底不变。
	if nextTier(contracts.ContextTierFull) != contracts.ContextTierL1 ||
		nextTier(contracts.ContextTierL1) != contracts.ContextTierL2 ||
		nextTier(contracts.ContextTierL2) != contracts.ContextTierL2 {
		t.Error("nextTier 递进不对")
	}
}

func TestRenderEntriesCarryRecallInstructionsAndPointToRaw(t *testing.T) {
	l1 := RenderL1Entry(3, StageRefAnchored(3, 10, 20), "改了 a.go、跑了 go test", true)
	if !strings.Contains(l1, "阶段 3") || !strings.Contains(l1, "改了 a.go") {
		t.Errorf("L1 条目应含阶段号与摘要: %q", l1)
	}
	if !strings.Contains(l1, `recall("stage:3@e:10..e:20")`) {
		t.Errorf("L1 条目应自带 recall 调用说明（带 seq 锚的稳定指针）: %q", l1)
	}
	l2 := RenderL2Entry(7, StageRefAnchored(7, 30, 40), "stage1 方案已定", true)
	if !strings.Contains(l2, "阶段 7") || !strings.Contains(l2, `recall("stage:7@e:30..e:40")`) {
		t.Errorf("L2 条目应含阶段号/结论与 recall 说明: %q", l2)
	}
	// F27：recall 不可见（被策略 deny）时不附调用说明。
	if strings.Contains(RenderL1Entry(3, StageRefAnchored(3, 10, 20), "x", false), `recall("`) {
		t.Error("recall 不可见时不应附 recall 调用说明")
	}
	// refs 永不指摘要：条目里只有 stage:N@e:…（指原文），没有 sum: 之类。
	for _, e := range []string{l1, l2} {
		if strings.Contains(e, "sum:") {
			t.Errorf("条目不得含摘要引用 sum:（refs 永不指摘要）: %q", e)
		}
	}
}

func TestMessagesTextIncludesToolCalls(t *testing.T) {
	msgs := []core.Message{
		{Role: "user", Content: "读文件"},
		{Role: "assistant", Content: "好", ToolCalls: []core.ToolCallReq{{Name: "read", Args: []byte(`{"p":"x"}`)}}},
		{Role: "tool", Content: "内容", ToolCallID: "c1"},
	}
	txt := messagesText(msgs)
	for _, want := range []string{"读文件", "read", `{"p":"x"}`, "内容"} {
		if !strings.Contains(txt, want) {
			t.Errorf("messagesText 应含 %q: %q", want, txt)
		}
	}
}
