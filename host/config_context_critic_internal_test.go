package host

// critic P2 的两条钉（内部单测，观测面都在宿主这一侧）：
//
//	① **注入区截断不许静默**（P2-1）：本轮生效参数（注入区预算）把注入区截断了 → 宿主
//	   **Warn 留痕**（灌参后回读装配器的截断观测面 → 回调宿主）。退化必须可见：静默砍掉
//	   规则等于悄悄丢上下文。
//	② **RunOverridesFromConfig 的 nil 语义**（P2-3）：fold **没有任何 context 子项** →
//	   Context = nil（"本轮没有上下文面的信息"）；有任意子项 → 非 nil（"有信息"）。
//	   混成一个零值 = 把"显式 append"与"什么都没说"当成一回事。

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/sessioncfg"
)

// TestContextInjectionTruncationWarnedAfterParamOverride（P2-1）：宿主默认预算下**不截断**
// 的规则文本 + 本轮把注入区预算改成很小的值 → 灌参后重新截断 → 宿主 Warn 留痕。
//
// 为什么不是"构造时就截断"：那样告警会在**灌参之前**就报（看不出是本轮预算改的）；
// 这里特意让构造时不截断、只有灌参后才截断 → 告警只可能来自灌参后的回读。
func TestContextInjectionTruncationWarnedAfterParamOverride(t *testing.T) {
	var buf bytes.Buffer
	old := hubLog
	hubLog = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	defer func() { hubLog = old }()

	dir := t.TempDir()
	h, err := New(Config{
		DBPath:   filepath.Join(dir, "events.db"),
		Model:    "m",
		System:   "sys",
		MaxTurns: 2,
		Allow:    []string{"read"},
		Provider: &nopProvider{},
		// tiered + 规则文本：长度在**宿主默认预算（15k token）之内** → 构造时不截断。
		ContextStrategy:       "tiered",
		ContextStrategyWindow: 300_000,
		ContextRules:          strings.Repeat("规则", 2500), // ≈5000 字符 ≈1250 token < 15k
		CWD:                   dir,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	sid := NewSession()
	ctx := context.Background()
	// 建 loop（托管）→ 取主 agent id → 落一条 config.changed（把注入区预算改成 1000
	// token → 灌参后必截断）。
	if _, _, err := h.loopFor(ctx, sid); err != nil {
		t.Fatalf("loopFor: %v", err)
	}
	agentID, ok := h.mainAgentOf(sid)
	if !ok {
		t.Fatal("取主 agent id 失败")
	}
	budget := int64(1000)
	if err := h.recordConfigChanged(ctx, sid, agentID, &contracts.ConfigChangedPayload{
		Context: &contracts.ContextConfigChange{RulesBudget: &budget},
	}); err != nil {
		t.Fatalf("recordConfigChanged: %v", err)
	}
	if _, err := h.RunPrompt(ctx, sid, "跑一轮"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	logs := buf.String()
	if !strings.Contains(logs, "注入区文本超预算已被截断") {
		t.Fatalf("注入区被截断应 Warn 留痕（灌参后回读），日志=%q", logs)
	}
	// 只报一次（工厂 per-Run + 参数可能每轮变 → 不去重就是每轮一条噪声）。
	if n := strings.Count(logs, "注入区文本超预算已被截断"); n != 1 {
		t.Fatalf("注入截断告警应只报一次，得到 %d 条: %s", n, logs)
	}
}

// TestRunOverridesFromConfigContextNilSemantics（P2-3）：fold 无任何 context 子项 →
// Context = nil；有任意子项（含 kind 的清除记录）→ 非 nil。
func TestRunOverridesFromConfigContextNilSemantics(t *testing.T) {
	// ① fold 没有任何 context 子项 → nil（"没有信息"）。
	ov, err := RunOverridesFromConfig(sessioncfg.SessionConfig{}, nil)
	if err != nil {
		t.Fatalf("RunOverridesFromConfig: %v", err)
	}
	if ov.Context != nil {
		t.Fatalf("fold 无 context 子项 → Context 应为 nil（无信息），得到 %+v", ov.Context)
	}
	// ② 有任意子项 → 非 nil（"有信息"）。
	ov2, err := RunOverridesFromConfig(sessioncfg.SessionConfig{
		Context: sessioncfg.ContextState{Window: sessioncfg.NumField{Written: true, Value: 512000}},
	}, nil)
	if err != nil {
		t.Fatalf("RunOverridesFromConfig: %v", err)
	}
	if ov2.Context == nil {
		t.Fatal("fold 有 context 子项 → Context 不该为 nil")
	}
	if ov2.Context.Window != 512000 {
		t.Fatalf("Context.Window = %d，want 512000", ov2.Context.Window)
	}
	// ③ kind 的清除记录（written + 空串）也算"有信息"（它把 kind 显式清回默认）。
	ov3, err := RunOverridesFromConfig(sessioncfg.SessionConfig{
		Context: sessioncfg.ContextState{Kind: sessioncfg.Field{Written: true, Value: ""}},
	}, nil)
	if err != nil {
		t.Fatalf("RunOverridesFromConfig: %v", err)
	}
	if ov3.Context == nil {
		t.Fatal("fold 有 kind 子项（清除记录）→ Context 不该为 nil")
	}
}
