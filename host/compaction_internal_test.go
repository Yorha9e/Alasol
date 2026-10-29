package host

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/compaction"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// 白盒测 v2-S1 host 装配（host/compaction.go）：策略工厂的门 + store 适配器 + 窗口硬约束。

func openCompactionStore(t *testing.T) *sqlite.Store {
	t.Helper()
	st, err := sqlite.Open(filepath.Join(t.TempDir(), "compaction.db"), sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestContextStrategyFactoryGate(t *testing.T) {
	sid := core.NewUlid()
	aid := core.NewUlid()

	// "" = append（默认）→ nil 工厂。
	off := &Hub{cfg: Config{ContextStrategy: ""}}
	if f, err := off.contextStrategyFactory(sid, aid, off.cfg.ContextStrategy); f != nil || err != nil {
		t.Fatalf("ContextStrategy 空应返回 nil 工厂：f=%v err=%v", f, err)
	}

	// tiered + 默认窗口（0=1M）→ 非 nil 工厂。
	on := &Hub{cfg: Config{ContextStrategy: "tiered"}}
	f, err := on.contextStrategyFactory(sid, aid, on.cfg.ContextStrategy)
	if err != nil || f == nil {
		t.Fatalf("tiered 应返回非 nil 工厂：f=%v err=%v", f, err)
	}

	// tiered + 窗口 < 256k 硬约束 → 拒绝（fail-close，不静默降级）。
	small := &Hub{cfg: Config{ContextStrategy: "tiered", ContextStrategyWindow: 200_000}}
	if _, err := small.contextStrategyFactory(sid, aid, small.cfg.ContextStrategy); err == nil {
		t.Fatal("窗口 < 256k 应拒绝装配并说明")
	}

	// tiered + 窗口 ≥ 256k → 非 nil 工厂。
	big := &Hub{cfg: Config{ContextStrategy: "tiered", ContextStrategyWindow: 300_000}}
	if f, err := big.contextStrategyFactory(sid, aid, big.cfg.ContextStrategy); err != nil || f == nil {
		t.Fatalf("窗口 300k 应通过：f=%v err=%v", f, err)
	}
}

func TestContextStrategyFactoryProducesEngine(t *testing.T) {
	st := openCompactionStore(t)
	ix, err := sqlite.NewIndexer(st, 0)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	sid := core.NewUlid()
	aid := core.NewUlid()
	h := &Hub{
		cfg:      Config{ContextStrategy: "tiered", Model: "m"},
		store:    st,
		indexer:  ix,
		bus:      bus.New(st),
		provider: nopProvider{},
	}
	f, err := h.contextStrategyFactory(sid, aid, h.cfg.ContextStrategy)
	if err != nil || f == nil {
		t.Fatalf("tiered 工厂: f=%v err=%v", f, err)
	}
	// 工厂产物是一个 tiered 引擎（且提供 recall/stage 框架工具）。
	a := f()
	engine, ok := a.(*compaction.Engine)
	if !ok {
		t.Fatalf("工厂应产 *compaction.Engine，得到 %T", a)
	}
	tp, ok := a.(compaction.ToolProvider)
	if !ok || len(tp.Tools()) != 2 {
		t.Fatalf("引擎应提供 recall/stage 两个框架工具：%v", tp)
	}
	_ = engine
}

func TestStoreSeqSourceIncrementalTip(t *testing.T) {
	st := openCompactionStore(t)
	sid := core.NewUlid()
	src := &storeSeqSource{store: st, sessionID: sid}
	ctx := context.Background()

	if tip, _ := src.Tip(ctx); tip != 0 {
		t.Fatalf("空库 tip 应 0，得到 %d", tip)
	}
	// 落三条事件。
	for i := 0; i < 3; i++ {
		if _, err := st.Append(ctx, []contracts.Event{convEvent(sid, "user.prompt")}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	tip, err := src.Tip(ctx)
	if err != nil {
		t.Fatalf("Tip: %v", err)
	}
	if tip < 3 {
		t.Fatalf("落 3 条后 tip 应 ≥3，得到 %d", tip)
	}
	// 再落一条 → tip 只增（增量读，不回归）。
	if _, err := st.Append(ctx, []contracts.Event{convEvent(sid, "model.turn")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	tip2, _ := src.Tip(ctx)
	if tip2 <= tip {
		t.Fatalf("tip 应递增：%d → %d", tip, tip2)
	}
}

func TestStoreTextSourceFoldsRange(t *testing.T) {
	st := openCompactionStore(t)
	sid := core.NewUlid()
	src := storeTextSource{store: st, sessionID: sid}
	ctx := context.Background()

	appended, err := st.Append(ctx, []contracts.Event{convEvent(sid, "user.prompt")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	seq := *appended[0].Seq
	// 该 seq 区间应折出 user.prompt 的文本（fold 空容错，不含即缺）。
	text, err := src.Text(ctx, seq, seq)
	if err != nil {
		t.Fatalf("Text: %v", err)
	}
	if text == "" {
		t.Fatal("该区间应折出非空对话文本")
	}
}

// convEvent 造一条最小合法 durable 对话事件（user.prompt / model.turn）。
func convEvent(sid contracts.Ulid, kind string) contracts.Event {
	raw, _ := json.Marshal(struct {
		Text string `json:"text"`
	}{Text: "对话原文-" + kind})
	return contracts.Event{
		V:         1,
		ID:        core.NewUlid(),
		Kind:      kind,
		TS:        "2025-03-01T10:00:00.000Z",
		SessionID: sid,
		AgentID:   core.NewUlid(),
		Durable:   true,
		Payload:   raw,
	}
}

// nopProvider 是空转 provider（contextStrategyFactory 只建工厂、不真调它）。
type nopProvider struct{}

func (nopProvider) Run(context.Context, core.ModelRequest, func(core.ProviderEvent)) (core.ModelResult, error) {
	return core.ModelResult{Text: "nop"}, nil
}

// ---------------------------------------------------------------------------
// F7：宿主装配面注入项目规则（稳定前缀的一部分）——走**宿主装配路径**，不是测试直调。
// ---------------------------------------------------------------------------

func TestContextStrategyFactoryInjectsRules(t *testing.T) {
	st := openCompactionStore(t)
	ix, err := sqlite.NewIndexer(st, 0)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	sid := core.NewUlid()
	aid := core.NewUlid()
	h := &Hub{
		cfg: Config{
			ContextStrategy: "tiered",
			ContextRules:    "[项目规则] 遵循 AGENTS.md",
			Model:           "m",
		},
		store:    st,
		indexer:  ix,
		bus:      bus.New(st),
		provider: nopProvider{},
	}
	f, err := h.contextStrategyFactory(sid, aid, h.cfg.ContextStrategy)
	if err != nil || f == nil {
		t.Fatalf("tiered 工厂: f=%v err=%v", f, err)
	}
	a := f()
	out, err := a.Assemble(context.Background(), []core.Message{{Role: "user", Content: "开工"}}, 1)
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	found := false
	for _, m := range out.Messages {
		if strings.Contains(m.Content, "遵循 AGENTS.md") {
			found = true
		}
	}
	if !found {
		t.Fatalf("宿主装配的引擎应注入项目规则（F7）: %+v", out.Messages)
	}
	// 规则进 parts（context.assembled 的 rules 来源）。
	hasRules := false
	for _, p := range out.Parts {
		if p.Source == "rules" {
			hasRules = true
		}
	}
	if !hasRules {
		t.Errorf("parts 应出现 rules 来源: %+v", out.Parts)
	}
}

// ---------------------------------------------------------------------------
// F10：宿主配置 → compaction.Params（window/l1_pct/l2_pct 单一参数面）。
// ---------------------------------------------------------------------------

func TestContextStrategyFactoryAppliesParams(t *testing.T) {
	st := openCompactionStore(t)
	ix, err := sqlite.NewIndexer(st, 0)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	h := &Hub{
		cfg: Config{
			ContextStrategy:       "tiered",
			ContextStrategyWindow: 512_000,
			ContextStrategyL1Pct:  8,
			ContextStrategyL2Pct:  0.5,
			Model:                 "m",
		},
		store:    st,
		indexer:  ix,
		bus:      bus.New(st),
		provider: nopProvider{},
	}
	f, err := h.contextStrategyFactory(core.NewUlid(), core.NewUlid(), h.cfg.ContextStrategy)
	if err != nil || f == nil {
		t.Fatalf("tiered 工厂: f=%v err=%v", f, err)
	}
	engine, ok := f().(*compaction.Engine)
	if !ok {
		t.Fatalf("工厂产物应是 *compaction.Engine，得到 %T", f())
	}
	p := engine.Params()
	if p.Window != 512_000 {
		t.Errorf("窗口应来自宿主配置: %d", p.Window)
	}
	if p.L1TargetPct != 8 || p.L2TargetPct != 0.5 {
		t.Errorf("L1/L2 百分比应来自宿主配置: %v / %v", p.L1TargetPct, p.L2TargetPct)
	}
	if p.L1TargetTokens() != int64(512_000*8/100) {
		t.Errorf("L1 目标 token 口径不对: %d", p.L1TargetTokens())
	}
}

// ---------------------------------------------------------------------------
// F23：装配侧与校验侧同一口径（" tiered " 不得静默退回 append）。
// ---------------------------------------------------------------------------

func TestContextStrategyFactoryTrimsStrategyValue(t *testing.T) {
	h := &Hub{cfg: Config{ContextStrategy: " tiered "}}
	f, err := h.contextStrategyFactory(core.NewUlid(), core.NewUlid(), h.cfg.ContextStrategy)
	if err != nil {
		t.Fatalf("装配不应报错: %v", err)
	}
	if f == nil {
		t.Fatal("\" tiered \" 应装配 tiered 引擎（不得静默退回 append）")
	}
}

// ---------------------------------------------------------------------------
// F17/F18：recall 回捞原文 = 有界读 + 按 agent 过滤。
// ---------------------------------------------------------------------------

// convEventOf 造一条指定 agent 归属的对话事件。
func convEventOf(sid, aid contracts.Ulid, kind, text string) contracts.Event {
	raw, _ := json.Marshal(struct {
		Text string `json:"text"`
	}{Text: text})
	return contracts.Event{
		V:         1,
		ID:        core.NewUlid(),
		Kind:      kind,
		TS:        "2025-03-01T10:00:00.000Z",
		SessionID: sid,
		AgentID:   aid,
		Durable:   true,
		Payload:   raw,
	}
}

func TestStoreTextSourceBoundedAndAgentScoped(t *testing.T) {
	st := openCompactionStore(t)
	sid := core.NewUlid()
	agentA := core.NewUlid()
	agentB := core.NewUlid()

	appended, err := st.Append(context.Background(), []contracts.Event{
		convEventOf(sid, agentA, "user.prompt", "A1"),
		convEventOf(sid, agentB, "user.prompt", "B1"),
		convEventOf(sid, agentA, "user.prompt", "A2"),
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	firstSeq := *appended[0].Seq
	lastSeq := *appended[len(appended)-1].Seq

	src := storeTextSource{store: st, sessionID: sid, agentID: agentA}
	// ① 按 agent 过滤：只回捞本 agent 的对话（F18）。
	text, err := src.Text(context.Background(), firstSeq, lastSeq)
	if err != nil {
		t.Fatalf("Text: %v", err)
	}
	if !strings.Contains(text, "A1") || !strings.Contains(text, "A2") {
		t.Errorf("应回捞本 agent 的原文: %q", text)
	}
	if strings.Contains(text, "B1") {
		t.Errorf("不该回捞别的 agent 的对话（F18）: %q", text)
	}
	// ② 按区间读：区间外的内容不回来（F17）。
	text2, err := src.Text(context.Background(), firstSeq, firstSeq)
	if err != nil {
		t.Fatalf("Text: %v", err)
	}
	if strings.Contains(text2, "A2") {
		t.Errorf("区间外的内容不该被读回: %q", text2)
	}
	// ③ 有上限：区间超上限时截断并说明（不把整场会话搬回来）。
	limited := storeTextSource{store: st, sessionID: sid, agentID: agentA, limit: 1}
	text3, err := limited.Text(context.Background(), firstSeq, lastSeq)
	if err != nil {
		t.Fatalf("Text: %v", err)
	}
	if !strings.Contains(text3, "截断") {
		t.Errorf("超上限的回捞应说明已截断: %q", text3)
	}
}

// ---------------------------------------------------------------------------
// #3/#4：上下文策略配置的校验（归一化后的 L1/L2 比较 + 窗口硬约束前移到启动期）。
// ---------------------------------------------------------------------------

func TestConfigValidateContextStrategyParams(t *testing.T) {
	base := func() Config {
		return Config{DBPath: "x.db", Model: "m", ContextStrategy: "tiered"}
	}

	// ① L1 用默认（0 = 10%）、L2 配成 50% → 必须拒绝（更深的反而更宽 = 两级退化）。
	c := base()
	c.ContextStrategyL1Pct = 0
	c.ContextStrategyL2Pct = 50
	if err := c.validate(); err == nil {
		t.Error("L1 用默认值、L2=50% 应被拒绝（比较必须用归一化后的有效值）")
	}

	// ② L1=20%、L2=0（默认 1%）→ 通过。
	c2 := base()
	c2.ContextStrategyL1Pct = 20
	c2.ContextStrategyL2Pct = 0
	if err := c2.validate(); err != nil {
		t.Errorf("L1=20 百分比、L2 用默认应通过: %v", err)
	}

	// ③ 窗口 < 256k 硬约束 → **启动期**就拒绝（不拖到每个 session 请求）。
	c3 := base()
	c3.ContextStrategyWindow = 100_000
	if err := c3.validate(); err == nil {
		t.Error("tiered + W<256k 应在启动期被拒绝（W≥256k 是硬约束）")
	}

	// ④ W 缺省（0 = 1M）→ 通过。
	c4 := base()
	if err := c4.validate(); err != nil {
		t.Errorf("tiered + 默认窗口应通过: %v", err)
	}

	// ⑤ 非 tiered 策略时窗口不参与硬约束（append 路径不挂 tiered）。
	c5 := Config{DBPath: "x.db", Model: "m"}
	c5.ContextStrategyWindow = 100_000
	if err := c5.validate(); err != nil {
		t.Errorf("append 路径不该受 tiered 窗口硬约束约束: %v", err)
	}
}

// 宿主装配把注入区上限接到 compaction.Params（规则/常识区不可压缩 → 必须有上限）。
func TestContextStrategyFactoryAppliesInjectionBudgets(t *testing.T) {
	st := openCompactionStore(t)
	ix, err := sqlite.NewIndexer(st, 0)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	h := &Hub{
		cfg: Config{
			ContextStrategy:                  "tiered",
			ContextStrategyRulesBudget:       8000,
			ContextStrategyCommonSenseBudget: 20000,
			Model:                            "m",
		},
		store:    st,
		indexer:  ix,
		bus:      bus.New(st),
		provider: nopProvider{},
	}
	f, err := h.contextStrategyFactory(core.NewUlid(), core.NewUlid(), h.cfg.ContextStrategy)
	if err != nil || f == nil {
		t.Fatalf("tiered 工厂: f=%v err=%v", f, err)
	}
	engine, ok := f().(*compaction.Engine)
	if !ok {
		t.Fatalf("工厂产物应是 *compaction.Engine，得到 %T", f())
	}
	p := engine.Params()
	if p.RulesBudget != 8000 || p.CommonSenseBudget != 20000 {
		t.Errorf("注入区上限应来自宿主配置: %d / %d", p.RulesBudget, p.CommonSenseBudget)
	}
}

// #1：注入区预算把窗口吃光 → **启动期**拒绝（负历史区预算地雷不许带进运行期）。
func TestConfigValidateRejectsOverflowingInjectionBudget(t *testing.T) {
	c := Config{DBPath: "x.db", Model: "m", ContextStrategy: "tiered", ContextStrategyWindow: 256_000}
	// 前缀 100k + 常识区 30k + 阶段槽 150k + 尾部 10k = 290k > 256k → 历史区为负。
	c.ContextStrategyRulesBudget = 100_000
	if err := c.validate(); err == nil {
		t.Error("注入区预算把窗口吃光应在启动期被拒绝（负历史区预算地雷）")
	}
	// 前缀 30k（30+30+150+10=220k ≤ 256k）→ 通过。
	c2 := Config{DBPath: "x.db", Model: "m", ContextStrategy: "tiered", ContextStrategyWindow: 256_000}
	c2.ContextStrategyRulesBudget = 30_000
	if err := c2.validate(); err != nil {
		t.Errorf("注入区预算装得下应通过: %v", err)
	}
}

// 注入区被截断 → 宿主留痕（退化必须可见）；工厂是 per-Run 的 → 告警**只报一次**。
func TestContextStrategyFactoryReportsTruncatedInjectionOnce(t *testing.T) {
	st := openCompactionStore(t)
	ix, err := sqlite.NewIndexer(st, 0)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	// 捕获日志（宿主层的告警出口是 hubLog = slog.Default()）。
	var buf bytes.Buffer
	old := hubLog
	hubLog = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	defer func() { hubLog = old }()

	h := &Hub{
		cfg: Config{
			ContextStrategy: "tiered",
			ContextRules:    strings.Repeat("规则", 100_000), // ≫ 15k 上限
			Model:           "m",
		},
		store:    st,
		indexer:  ix,
		bus:      bus.New(st),
		provider: nopProvider{},
	}
	f, err := h.contextStrategyFactory(core.NewUlid(), core.NewUlid(), h.cfg.ContextStrategy)
	if err != nil || f == nil {
		t.Fatalf("tiered 工厂: f=%v err=%v", f, err)
	}
	engine, ok := f().(*compaction.Engine)
	if !ok {
		t.Fatalf("工厂产物应是 *compaction.Engine，得到 %T", f())
	}
	if r, _ := engine.InjectionTruncated(); !r {
		t.Error("超长规则应被截断并可观测（InjectionTruncated）")
	}
	// 再取一次工厂产物（= 第二个 Run 的引擎）→ 告警不应重复。
	_ = f()
	if n := strings.Count(buf.String(), "注入区文本超预算已被截断"); n != 1 {
		t.Errorf("注入截断告警应只报一次（工厂 per-Run），得到 %d 条: %s", n, buf.String())
	}
}
