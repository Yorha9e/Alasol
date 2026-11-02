package host_test

// W15 成本预算闸门（v4-4-B）的钉子：**默认不设上限 = 零行为变化**；配了上限 → 超限拒绝
// 发起新一轮 + 越界告警；缺口 5 第二件补上**轮间截断**（loop.Config.TurnBudget）与
// 查询失败 fail-open 的端到端口径。
//
// 口径（host/budget.go）：
//   - BudgetMaxTokens<=0（默认）= 闸门不生效，一次查询都不发（钩子为 nil）；
//   - 数据源 = 索引器 cost_tokens（J21：usage 只进成本）；
//   - **两层检查**：per-Run（RunPrompt 入口 / fork 拒）+ per-turn（Run 内轮间截断，
//     Result.TruncatedByBudget 收口）——两层读同一份账、互不冲突。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
)

// budgetProvider 每轮报固定 usage（输入/输出各 100 tokens → 每轮 200）。
type budgetProvider struct{ calls int }

func (p *budgetProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	emit(core.ProviderEvent{Kind: core.KindTextDelta, Text: "好"})
	emit(core.ProviderEvent{Kind: core.KindDone, Text: "好"})
	return core.ModelResult{Text: "好", Usage: core.Usage{InputTokens: 100, OutputTokens: 100}}, nil
}

// newBudgetHub 建一个带脚本 provider 的 hub（budget<=0 = 不设上限）。
func newBudgetHub(t *testing.T, budget int64) (*host.Hub, contracts.Ulid) {
	t.Helper()
	h, err := host.New(host.Config{
		DBPath:          filepath.Join(t.TempDir(), "budget.db"),
		Model:           "scripted",
		Addr:            "127.0.0.1:0",
		Approval:        "deny-all",
		WorktreeRoot:    filepath.Join(t.TempDir(), "wts"),
		MaxSessionRows:  10,
		Provider:        &budgetProvider{},
		BudgetMaxTokens: budget,
		IndexerBatch:    100,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h, host.NewSession()
}

// TestBudgetDefaultOffRunsFreely（F1，酸测试）：**默认不设上限**（BudgetMaxTokens=0）→
// 想跑几轮跑几轮，没有任何拒绝（W15 拍板口径）。
func TestBudgetDefaultOffRunsFreely(t *testing.T) {
	h, sid := newBudgetHub(t, 0)
	for i := 0; i < 3; i++ {
		if _, err := h.RunPrompt(context.Background(), sid, "继续"); err != nil {
			t.Fatalf("第 %d 轮 RunPrompt 不该被拒（默认不设上限）: %v", i+1, err)
		}
	}
	// 负值同样在启动期就被拒（不会走到这里）——见 TestBudgetRejectsNegative。
}

// TestBudgetRejectsNegative（F2）：负上限 = 非法配置 → 启动即拒（fail-close：负阈值静默
// 当成 0 = 悄悄关掉用户以为开着的闸门）。
func TestBudgetRejectsNegative(t *testing.T) {
	dir := t.TempDir()
	_, err := host.New(host.Config{
		DBPath:          filepath.Join(dir, "db.sqlite"),
		Model:           "scripted",
		Addr:            "127.0.0.1:0",
		WorktreeRoot:    filepath.Join(dir, "wts"),
		MaxSessionRows:  10,
		Provider:        &budgetProvider{},
		BudgetMaxTokens: -1,
	})
	if err == nil {
		t.Fatal("负的 BudgetMaxTokens 应拒绝启动")
	}
	if !strings.Contains(err.Error(), "BudgetMaxTokens") {
		t.Errorf("错误应点名 BudgetMaxTokens，got %v", err)
	}
}

// TestBudgetRefusesWhenOverLimit（F3）：配了上限 → 累计达到上限后**拒绝发起新一轮**，
// 且错误文案点名 session / 累计 / 上限。
func TestBudgetRefusesWhenOverLimit(t *testing.T) {
	// 上限 500：第一轮花 200（剩 300），第二轮花 200（剩 100），第三轮花 200 → 累计 600 ≥ 500
	// → 第四轮被拒。
	h, sid := newBudgetHub(t, 500)
	for i := 0; i < 3; i++ {
		if _, err := h.RunPrompt(context.Background(), sid, "花点 token"); err != nil {
			t.Fatalf("第 %d 轮不该被拒（累计还没到上限）: %v", i+1, err)
		}
	}
	_, err := h.RunPrompt(context.Background(), sid, "再花")
	if err == nil {
		t.Fatal("累计达到上限后应拒绝发起新一轮")
	}
	for _, want := range []string{string(sid), "500"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误缺 %q: %v", want, err)
		}
	}
	if !strings.Contains(err.Error(), "600") {
		t.Logf("（提示）错误文案里的累计值：%v", err)
	}
}

// TestBudgetBoundaryAllowsJustUnderLimit（F4）：累计**恰低于**上限时放行（≥ 才拒，
// 不是 >；边界不糊）。
func TestBudgetBoundaryAllowsJustUnderLimit(t *testing.T) {
	// 上限 400：第一轮 200、第二轮 200 → 累计 400 ≥ 400 → 第三轮拒。
	h, sid := newBudgetHub(t, 400)
	for i := 0; i < 2; i++ {
		if _, err := h.RunPrompt(context.Background(), sid, "花"); err != nil {
			t.Fatalf("第 %d 轮不该被拒: %v", i+1, err)
		}
	}
	if _, err := h.RunPrompt(context.Background(), sid, "再花"); err == nil {
		t.Fatal("累计恰等于上限时应拒（≥ 才放行下面的轮次）")
	}
}

// TestBudgetRefusesBtwWhenOverLimit（F5，critic F7）：预算超限时 **/btw 旁支也被拒**
// （旁支是一次真实开销，不该从闸门里溜过去——旧实现里 RunBtw 直接绕过 checkBudget）。
//
// 钉子：去掉 RunBtw 里的 checkBudget → 本用例红。
func TestBudgetRefusesBtwWhenOverLimit(t *testing.T) {
	h, sid := newBudgetHub(t, 500)
	// 先跑几轮把累计推过上限（每轮 200 → 三轮 600 ≥ 500）。
	for i := 0; i < 3; i++ {
		if _, err := h.RunPrompt(context.Background(), sid, "花点 token"); err != nil {
			t.Fatalf("第 %d 轮不该被拒: %v", i+1, err)
		}
	}
	if _, err := h.RunBtw(context.Background(), sid, "旁支问题"); err == nil {
		t.Fatal("预算超限时 /btw 旁支应被拒")
	} else if !strings.Contains(err.Error(), "预算") {
		t.Errorf("错误应说明是预算闸门，got %v", err)
	}
}

// ---------------------------------------------------------------------------
// 缺口 5 第二件：逐轮制动（loop.Config.TurnBudget）+ fail-open
// ---------------------------------------------------------------------------

// budgetTurnProvider 每轮都发一次工具调用（工具**不在**注册表 → tool.call.failed，loop
// 继续下一轮），每轮 usage = input 100 + output 100 = 200 tokens——多轮在**一次 Run 内**
// 把累计成本往上推，轮间钩子据此截断。
type budgetTurnProvider struct {
	mu    sync.Mutex
	calls int
}

func (p *budgetTurnProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
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

func (p *budgetTurnProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// TestBudgetMidRunTruncatesAtTurnBoundary（变异自证①d，端到端）：上限 500、每轮 200——
// 一次 Run 内第 3 轮跑完后累计 600 ≥ 500 → **第 4 轮发起前**被轮间钩子截断：
// RunPrompt 正常返回（不是 error），Result.TruncatedByBudget=true、Turns=3、
// provider 只被调 3 次（被拒轮次不发起）。
//
// 钉子：把 hub.go 装配里的 TurnBudget 接线去掉（钩子 nil）→ 本用例红（跑到 MaxTurns=20
// 或工具调用耗尽为止，Turns 不会停在 3）。
func TestBudgetMidRunTruncatesAtTurnBoundary(t *testing.T) {
	prov := &budgetTurnProvider{}
	h, err := host.New(host.Config{
		DBPath:          filepath.Join(t.TempDir(), "budget.db"),
		Model:           "scripted",
		Addr:            "127.0.0.1:0",
		Approval:        "deny-all",
		WorktreeRoot:    filepath.Join(t.TempDir(), "wts"),
		MaxSessionRows:  10,
		Provider:        prov,
		BudgetMaxTokens: 500,
		IndexerBatch:    100,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	sid := host.NewSession()

	res, err := h.RunPrompt(context.Background(), sid, "花点 token")
	if err != nil {
		t.Fatalf("轮间预算截断不该是 error（照 MaxTurns 口径：Result 收口）: %v", err)
	}
	if !res.TruncatedByBudget {
		t.Fatalf("TruncatedByBudget 应为 true: %+v", res)
	}
	// 截断原因文案 = checkBudget 的超限说明（累计 / 上限 / session 都在）。
	for _, want := range []string{string(sid), "500", "成本预算上限"} {
		if !strings.Contains(res.TruncationReason, want) {
			t.Errorf("TruncationReason 缺 %q: %q", want, res.TruncationReason)
		}
	}
	// 轮间拒：第 1/2/3 轮各花 200（0→200→400 放行，600 收口后第 4 轮拒）。
	// 被拒的那一轮**不发起**：provider 恰好 3 次调用、Turns=3。
	if res.Turns != 3 {
		t.Errorf("Turns = %d，want 3（第 4 轮发起前被拒）", res.Turns)
	}
	if n := prov.callCount(); n != 3 {
		t.Errorf("provider 被调 %d 次，want 3（被拒轮次不发起）", n)
	}

	// 下一个 prompt 被 per-Run 层拒（两层不冲突：Run 前拒 + 轮间拒）。
	if _, err := h.RunPrompt(context.Background(), sid, "再花"); err == nil {
		t.Fatal("累计超上限后下一个 prompt 应被 per-Run 层拒")
	} else if !strings.Contains(err.Error(), "预算") {
		t.Errorf("per-Run 层的拒绝应说明是预算闸门，got %v", err)
	}
}

// TestBudgetFailOpenOnQueryFailure：闸门开着 + 成本查询失败（ctx 已取消 → 索引器查询
// 必然失败）→ checkBudget **fail-open**（返 nil，不拒本轮）；于是 RunPrompt 继续走进
// loop，拿到的是 ctx.Canceled 而**不是**预算错误。
//
// 钉子：把 fail-open 改成 fail-close（查询失败即拒）→ 本用例红（错误会是预算文案）。
func TestBudgetFailOpenOnQueryFailure(t *testing.T) {
	h, sid := newBudgetHub(t, 100)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := h.RunPrompt(ctx, sid, "x")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v，期望 context.Canceled（查询失败 fail-open：不拒本轮）", err)
	}
}
