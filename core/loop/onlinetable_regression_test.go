package loop_test

import (
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
)

// 本文件是 v1-S4 评审轮的回归护栏（critic 发现 #1/#3）：
//   - 同毫秒铸造的 ULID 前缀逐字节相同（时间戳段），summary 行必须仍可区分——短 id 取随机尾段；
//   - 导出渲染入口 MaxRows=0 = 用默认 50（与 View() 同口径），不是"不限行"。

// TestOnlineTableRowsDistinguishableForSameMillisecondULIDs：同批（同毫秒）铸造的两枚 ULID
// 前 10 字符完全相同，summary 等级下两行必须可区分（行内 id 全文比对，不用前缀判）。
func TestOnlineTableRowsDistinguishableForSameMillisecondULIDs(t *testing.T) {
	// 两枚 ULID 形态：前 10 字符（毫秒时间戳段）相同、随机尾段不同——同批铸造的真实形态。
	a := "01M61DBM00" + "AAAAAAAAAAAAAAAB" // 短 id 应为 AAAAAAAB
	b := "01M61DBM00" + "BBBBBBBBBBBBBBBB" // 短 id 应为 BBBBBBBB
	rows := []loop.OnlineTableRow{
		{Self: true, SessionID: a, MainAgentID: a, Gates: []string{"repo:X"}, LastKind: "user.prompt", UpdatedAt: "2026-10-28T00:00:00Z"},
		{SessionID: b, MainAgentID: b, Gates: []string{"repo:X"}, LastKind: "user.prompt", UpdatedAt: "2026-10-28T00:00:00Z"},
	}
	text, n := loop.RenderOnlineTable(rows, loop.OnlineTableView{
		MaxRows: 50, LineTokenBudget: 100, Level: loop.LevelSummary, WindowLines: 5,
	})
	if n != 2 {
		t.Fatalf("应渲染 2 行，得到 %d", n)
	}
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "- ") {
			lines = append(lines, line)
		}
	}
	if len(lines) != 2 {
		t.Fatalf("应 2 行数据，得到 %d: %q", len(lines), text)
	}
	if lines[0] == lines[1] {
		t.Fatalf("同毫秒铸造的两行必须可区分（短 id 不可取时间戳前缀）: %q", lines[0])
	}
	if !strings.Contains(lines[0], "AAAAAAAB") || !strings.Contains(lines[1], "BBBBBBBB") {
		t.Fatalf("summary 行应含各自随机尾段短 id: %q / %q", lines[0], lines[1])
	}
	// 短 id 仍不得泄漏完整 id（S15：展示用短形）。
	if strings.Contains(lines[0], a) || strings.Contains(lines[1], b) {
		t.Fatal("summary 只用短 id，不该含完整 26 字符 id")
	}
}

// TestRenderOnlineTableZeroViewUsesDefaults：导出渲染入口的全零视图 → 0 = 用默认
// （MaxRows→50），**不是**"不限行"；行数上限是 K15 硬约束（critic 发现 #3）。
func TestRenderOnlineTableZeroViewUsesDefaults(t *testing.T) {
	rows := make([]loop.OnlineTableRow, 60)
	for i := range rows {
		rows[i] = loop.OnlineTableRow{
			SessionID:   string(core.NewUlid()),
			MainAgentID: string(core.NewUlid()),
			Gates:       []string{"repo:X"},
		}
	}
	text, n := loop.RenderOnlineTable(rows, loop.OnlineTableView{}) // 全零视图
	if n != loop.DefaultOnlineTableRows {
		t.Fatalf("MaxRows=0 应解析为默认 %d，渲染了 %d 行", loop.DefaultOnlineTableRows, n)
	}
	if !strings.Contains(text, "略去 10 行") {
		t.Fatalf("60 行按默认 50 截断应提示略去 10 行: %q", text)
	}
}
