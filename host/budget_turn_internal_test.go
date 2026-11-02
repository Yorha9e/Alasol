package host

// 缺口 5 第二件（A：逐轮预算制动）的 host 内部钉子：turnBudgetFor 的装配面形状——
// BudgetMaxTokens<=0 → **nil 钩子**（零查询、零行为变化）；>0 → 非 nil 闭包。
//
// 超限截断 / fail-open 的行为钉子在 budget_test.go（host_test 包，端到端经 RunPrompt）；
// 这里钉的是装配面本身（外部测试看不见 unexported 的钩子）。

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// TestTurnBudgetForHookNilWhenOff（变异自证①c）：BudgetMaxTokens<=0 → 钩子必须是 **nil**
// （loop 侧一轮都不查 = 零查询零行为变化）；>0 → 非 nil。
//
// 钉子：turnBudgetFor 恒返回闭包（不判上限）→ 第一段红；恒返回 nil → 第三段红。
func TestTurnBudgetForHookNilWhenOff(t *testing.T) {
	sid := core.NewUlid()

	h := &Hub{cfg: Config{BudgetMaxTokens: 0}}
	if fn := h.turnBudgetFor(sid); fn != nil {
		t.Fatal("BudgetMaxTokens=0（默认关）→ 钩子应为 nil（零查询、零行为变化）")
	}

	// 负值在 Config.validate 已拒（启动即拒）；运行期兜底同"关"（budgetMaxTokens<=0 口径）。
	h.cfg.BudgetMaxTokens = -1
	if fn := h.turnBudgetFor(sid); fn != nil {
		t.Fatal("负上限按关处理 → 钩子应为 nil")
	}

	h.cfg.BudgetMaxTokens = 500
	if fn := h.turnBudgetFor(sid); fn == nil {
		t.Fatal("BudgetMaxTokens>0 → 钩子应非 nil（轮间要查）")
	}
}

// budgetWarnProvider 每轮发一次**未注册**的工具调用（→ tool.call.failed，loop 继续下一轮），
// 每轮 usage = input 100 + output 100 = 200 tokens——多轮在一次 Run 内把累计成本推过上限，
// 轮间钩子据此截断（与 budget_test.go 的 budgetTurnProvider 同款逻辑，内部包自备一份）。
type budgetWarnProvider struct {
	mu    sync.Mutex
	calls int
}

func (p *budgetWarnProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu.Lock()
	p.calls++
	n := p.calls
	p.mu.Unlock()
	tc := core.ToolCallReq{ID: contracts.ProviderId(fmt.Sprintf("tc_%d", n)), Name: "echo", Args: json.RawMessage(`{}`)}
	emit(core.ProviderEvent{Kind: core.KindToolCallStart, ToolCall: &tc})
	emit(core.ProviderEvent{Kind: core.KindDone, Text: "调工具"})
	return core.ModelResult{
		Text:      "调工具",
		ToolCalls: []core.ToolCallReq{tc},
		Usage:     core.Usage{InputTokens: 100, OutputTokens: 100},
	}, nil
}

// TestRunPromptWarnsOnBudgetTruncation（F4 的 host 侧）：轮间预算闸门**真的截断了这一轮**
// → RunPrompt 出口**告警一条**（Result.TruncatedByBudget）。
//
// 为什么这条告警不可省：ACP 面把"预算截断"映射成 stopReason = max_turn_requests（ACP 枚举
// 里没有"预算耗尽"这一档）→ 客户端分不出"轮数用尽"与"预算用尽"；**宿主这条 Warn 是日志侧
// 唯一能分清的地方**。且它与 warnIfOverBudget 那条不是一回事：那条说的是"累计已越界"
// （成本观测面），这条说的是"**这一轮没跑完**"（轮次被挡在 provider.Run 之前）。
//
// 钉子：把 hub.go 里 TruncatedByBudget 的那条 Warn 去掉 → 本用例红。
func TestRunPromptWarnsOnBudgetTruncation(t *testing.T) {
	var buf safeLogBuf
	old := hubLog
	hubLog = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	defer func() { hubLog = old }()

	h, err := New(Config{
		DBPath:          filepath.Join(t.TempDir(), "budget.db"),
		Model:           "scripted",
		Addr:            "127.0.0.1:0",
		Approval:        "deny-all",
		WorktreeRoot:    filepath.Join(t.TempDir(), "wts"),
		MaxSessionRows:  10,
		Provider:        &budgetWarnProvider{},
		BudgetMaxTokens: 500,
		IndexerBatch:    100,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	sid := NewSession()
	res, err := h.RunPrompt(context.Background(), sid, "花点 token")
	if err != nil {
		t.Fatalf("轮间截断不该是 error（照 MaxTurns 口径：Result 收口）: %v", err)
	}
	if !res.TruncatedByBudget {
		t.Fatalf("TruncatedByBudget 应为 true: %+v", res)
	}
	logs := buf.String()
	if !strings.Contains(logs, "逐轮成本预算闸门截断") {
		t.Errorf("应有一条「本轮被逐轮成本预算闸门截断」的告警，got:\n%s", logs)
	}
	// 告警里带 session id 与截断原因（可定位到是哪个 session、为什么被挡）。
	if !strings.Contains(logs, string(sid)) {
		t.Errorf("告警应点名 session，got:\n%s", logs)
	}
}
