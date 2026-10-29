package host_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
)

// ledgerCallingProvider 第一轮调 ledger 更新目标，之后收敛。
type ledgerCallingProvider struct {
	calls int
}

func (p *ledgerCallingProvider) Run(_ context.Context, _ core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	if p.calls == 1 {
		tc := core.ToolCallReq{
			ID:   contracts.ProviderId("toolu_LEDGER_E2E"),
			Name: "ledger",
			Args: json.RawMessage(`{"goals":["ship v3 workflow engine"]}`),
		}
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 8, OutputTokens: 4}}, nil
	}
	return core.ModelResult{Text: "已记录目标", Usage: core.Usage{InputTokens: 12, OutputTokens: 3}}, nil
}

// twoPhaseLedgerProvider 跨两次 Run 的脚本 provider：
// 第 1 轮调 ledger 设**关键字段**（目标），第 3 轮调 ledger 只报**过程字段**（已完成）。
type twoPhaseLedgerProvider struct {
	calls int
}

func (p *twoPhaseLedgerProvider) Run(_ context.Context, _ core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.calls++
	switch p.calls {
	case 1:
		tc := core.ToolCallReq{
			ID:   contracts.ProviderId("toolu_LEDGER_CRIT"),
			Name: "ledger",
			Args: json.RawMessage(`{"goals":["ship v3 workflow engine"],"constraints":["不破坏既有契约"]}`),
		}
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 8, OutputTokens: 4}}, nil
	case 3:
		tc := core.ToolCallReq{
			ID:   contracts.ProviderId("toolu_LEDGER_PROC"),
			Name: "ledger",
			Args: json.RawMessage(`{"completed":["第一步"]}`),
		}
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 8, OutputTokens: 4}}, nil
	default:
		return core.ModelResult{Text: "好", Usage: core.Usage{InputTokens: 12, OutputTokens: 3}}, nil
	}
}

// TestJ19LedgerFeedsCompaction 是 J19 的端到端酸测试（级联 C 案联动）：
// agent 调 ledger 更新目标 → ledger.updated 事件 → 索引器投影 → 挂 tiered 的下一轮
// 压缩装配能看到账本（context.assembled 出现 source=ledger 的 part）。
func TestJ19LedgerFeedsCompaction(t *testing.T) {
	prov := &ledgerCallingProvider{}
	h, path := newWFHub(t, func(c *host.Config) {
		c.Provider = prov
		c.Allow = []string{"ledger"}
		c.GoalLedger = true
		c.ContextStrategy = "tiered"
		c.ContextStrategyWindow = 300_000 // ≥256k 硬约束
	})
	ctx := context.Background()
	sid := host.NewSession()

	// 第一轮：agent 调 ledger（关键字段立即落盘 ledger.updated）。
	if _, err := h.RunPrompt(ctx, sid, "定个目标"); err != nil {
		t.Fatalf("RunPrompt #1: %v", err)
	}

	// 找到 ledger.updated 事件的 seq，等索引器追上（投影 ledger_state 才有账本）。
	var ledgerSeq int64
	events, err := readSessionEvents(t, path, sid)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.Kind == "ledger.updated" && ev.Seq != nil {
			ledgerSeq = *ev.Seq
		}
	}
	if ledgerSeq == 0 {
		t.Fatal("第一轮应产出 ledger.updated 事件（J19 生产者）")
	}
	waitWatermark(t, h, ledgerSeq)

	// 第二轮（复用同 session 的 tiered loop）：压缩装配应把账本注入上下文。
	if _, err := h.RunPrompt(ctx, sid, "继续"); err != nil {
		t.Fatalf("RunPrompt #2: %v", err)
	}
	snap, ok, err := h.ContextSnapshot(ctx, sid)
	if err != nil || !ok {
		t.Fatalf("ContextSnapshot: ok=%v err=%v", ok, err)
	}
	var parsed contracts.ContextSnapshot
	if err := json.Unmarshal(snap, &parsed); err != nil {
		t.Fatal(err)
	}
	var hasLedgerPart bool
	for _, p := range parsed.Parts {
		if p.Source == "ledger" {
			hasLedgerPart = true
		}
	}
	if !hasLedgerPart {
		t.Errorf("挂 tiered 时压缩输入应出现 source=ledger 的 part（J19 账本喂进级联 C 案）；parts=%+v", parsed.Parts)
	}
}

// TestJ19LedgerKeepsCriticalFieldsAcrossRuns 是 **p1 回归**（端到端）：Run1 设目标/约束 →
// Run2 只报已完成项 → **关键字段仍在**（ledger.updated 是全量快照 + LWW，空基线会把
// Run1 的关键字段静默抹掉）。
func TestJ19LedgerKeepsCriticalFieldsAcrossRuns(t *testing.T) {
	prov := &twoPhaseLedgerProvider{}
	h, path := newWFHub(t, func(c *host.Config) {
		c.Provider = prov
		c.Allow = []string{"ledger"}
		c.GoalLedger = true
	})
	ctx := context.Background()
	sid := host.NewSession()

	// ① Run1：关键字段立即落盘。
	if _, err := h.RunPrompt(ctx, sid, "定个目标"); err != nil {
		t.Fatalf("RunPrompt #1: %v", err)
	}
	// ② Run2：只更新过程字段（completed）——关键字段不能被抹掉。
	if _, err := h.RunPrompt(ctx, sid, "报个进度"); err != nil {
		t.Fatalf("RunPrompt #2: %v", err)
	}

	events, err := readSessionEvents(t, path, sid)
	if err != nil {
		t.Fatal(err)
	}
	var last contracts.LedgerUpdatedPayload
	found := false
	for _, ev := range events {
		if ev.Kind != "ledger.updated" {
			continue
		}
		var p contracts.LedgerUpdatedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("解 ledger.updated: %v", err)
		}
		last = p
		found = true
	}
	if !found {
		t.Fatal("事件流里没有 ledger.updated（J19 生产者缺失）")
	}
	if len(last.Goals) != 1 || last.Goals[0] != "ship v3 workflow engine" {
		t.Errorf("跨 Run 后 goals 被抹掉了：%v（关键字段必须仍在）", last.Goals)
	}
	if len(last.Completed) != 1 || last.Completed[0] != "第一步" {
		t.Errorf("Run2 的过程字段没落上：%v", last.Completed)
	}
}

// 确保 time 被用（waitWatermark 内部用；这里显式引用避免未用导入的编译噪音）。
var _ = time.Second
