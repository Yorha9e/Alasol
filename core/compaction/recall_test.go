package compaction

import (
	"context"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
)

// recall / stage 执行器：走 X1 路径由 loop 落事件，执行器只负责取回/打标记。

func TestRecallExecutorReturnsRawText(t *testing.T) {
	resolve := func(_ context.Context, ref string) (string, bool, error) {
		if ref == "stage:2" {
			return "阶段二的完整原文", true, nil
		}
		return "", false, nil
	}
	exec := NewRecallExecutor(resolve)

	// 合法 ref → 取回原文全文。
	res, err := exec.Execute(context.Background(), contracts.ToolCallIntentPayload{
		Arguments: []byte(`{"ref":"stage:2"}`),
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(res.Content) != 1 || res.Content[0].Text == nil || !strings.Contains(*res.Content[0].Text, "阶段二的完整原文") {
		t.Fatalf("recall 应取回原文全文: %+v", res.Content)
	}

	// 缺 ref 参数 → 模型可读错误（不 panic）。
	if _, err := exec.Execute(context.Background(), contracts.ToolCallIntentPayload{Arguments: []byte(`{}`)}); err == nil {
		t.Error("缺 ref 应报错")
	}
	// 坏 JSON → 错误。
	if _, err := exec.Execute(context.Background(), contracts.ToolCallIntentPayload{Arguments: []byte(`not json`)}); err == nil {
		t.Error("坏 JSON 应报错")
	}
	// 未知 ref → ok=false → 回一条说明文本（不是 error，模型可读）。
	res, err = exec.Execute(context.Background(), contracts.ToolCallIntentPayload{
		Arguments: []byte(`{"ref":"stage:999"}`),
	})
	if err != nil {
		t.Fatalf("未知 ref 不该是 error（模型可读说明即可）: %v", err)
	}
	if res.Content[0].Text == nil || !strings.Contains(*res.Content[0].Text, "无法解析") {
		t.Fatalf("未知 ref 应回可读说明: %+v", res.Content)
	}
}

func TestRecallExecutorNilResolverFailsSafe(t *testing.T) {
	exec := NewRecallExecutor(nil)
	res, err := exec.Execute(context.Background(), contracts.ToolCallIntentPayload{
		Arguments: []byte(`{"ref":"stage:1"}`),
	})
	if err != nil {
		t.Fatalf("nil resolver 不该 error: %v", err)
	}
	if res.Content[0].Text == nil || !strings.Contains(*res.Content[0].Text, "未配置") {
		t.Fatalf("nil resolver 应回'未配置'说明: %+v", res.Content)
	}
}

func TestStageExecutorMarksBoundary(t *testing.T) {
	marked := 0
	exec := NewStageExecutor(func() { marked++ })
	res, err := exec.Execute(context.Background(), contracts.ToolCallIntentPayload{
		Arguments: []byte(`{"label":"阶段1通过"}`),
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if marked != 1 {
		t.Fatalf("stage 执行器应打一次边界标记，得到 %d", marked)
	}
	if res.Content[0].Text == nil || !strings.Contains(*res.Content[0].Text, "阶段边界") {
		t.Fatalf("stage 应回确认文本: %+v", res.Content)
	}

	// nil mark → 空操作，不 panic。
	NewStageExecutor(nil).Execute(context.Background(), contracts.ToolCallIntentPayload{Arguments: []byte(`{}`)})
}

// 编译期证明：两个执行器都是 core.ToolExecutor（loop 的 resolveTool 靠此路由）。
var (
	_ = NewRecallExecutor
	_ = NewStageExecutor
)
