package compaction

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// ---------------------------------------------------------------------------
// 测试替身：engine 的窄依赖（SeqSource / TextSource / LedgerSource / Summarizer / Emitter）
// ---------------------------------------------------------------------------

type fakeSeq struct{ tip int64 }

func (f *fakeSeq) Tip(context.Context) (int64, error) { return f.tip, nil }

type textCall struct{ from, to int64 }

type fakeText struct {
	mu    sync.Mutex
	calls []textCall
	// byRange 按 "from..to" 键返回原文；缺省返回一个带区间的标记串。
	byRange map[string]string
}

func (f *fakeText) Text(_ context.Context, from, to int64) (string, error) {
	f.mu.Lock()
	f.calls = append(f.calls, textCall{from, to})
	f.mu.Unlock()
	if f.byRange != nil {
		if s, ok := f.byRange[seqRangeRef(from, to)]; ok {
			return s, nil
		}
	}
	return "RAW[" + seqRangeRef(from, to) + "]", nil
}

type fakeLedger struct {
	lp contracts.LedgerUpdatedPayload
}

func (f fakeLedger) Ledger(context.Context) (contracts.LedgerUpdatedPayload, error) { return f.lp, nil }

// mutableLedger 是可变的账本替身（易变半每轮会变 → 用来验"易变半不进稳定前缀"）。
type mutableLedger struct {
	mu sync.Mutex
	lp contracts.LedgerUpdatedPayload
}

func (m *mutableLedger) Ledger(context.Context) (contracts.LedgerUpdatedPayload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lp, nil
}

func (m *mutableLedger) set(lp contracts.LedgerUpdatedPayload) {
	m.mu.Lock()
	m.lp = lp
	m.mu.Unlock()
}

type sumInput struct{ instruction, text string }

type fakeSummarizer struct {
	mu     sync.Mutex
	inputs []sumInput
	fn     func(instruction, text string) (string, error)
}

func (f *fakeSummarizer) Summarize(_ context.Context, instruction, text string) (SummaryResult, error) {
	f.mu.Lock()
	f.inputs = append(f.inputs, sumInput{instruction, text})
	f.mu.Unlock()
	usage := &contracts.ModelTurnUsage{InputTokens: 10, OutputTokens: 5}
	if f.fn != nil {
		out, err := f.fn(instruction, text)
		return SummaryResult{Text: out, Model: "fake-model", Usage: usage}, err
	}
	return SummaryResult{Text: "SUMMARY", Model: "fake-model", Usage: usage}, nil
}

func (f *fakeSummarizer) recorded() []sumInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sumInput(nil), f.inputs...)
}

type fakeEmitter struct {
	mu     sync.Mutex
	events []contracts.ContextCompactedPayload
}

func (f *fakeEmitter) EmitCompacted(_ context.Context, p contracts.ContextCompactedPayload) error {
	f.mu.Lock()
	f.events = append(f.events, p)
	f.mu.Unlock()
	return nil
}

func (f *fakeEmitter) recorded() []contracts.ContextCompactedPayload {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]contracts.ContextCompactedPayload(nil), f.events...)
}

// driver 驱动 engine 一轮轮装配，并维护一个递增的 seq tip（每条消息 +10 seq）。
type driver struct {
	t       *testing.T
	e       *Engine
	history []core.Message
	seq     *fakeSeq
	turn    int
}

func newDriver(t *testing.T, e *Engine, seq *fakeSeq) *driver {
	return &driver{t: t, e: e, seq: seq}
}

// add 追加消息并推进 seq tip（模拟这些消息对应的事件落库）。
func (d *driver) add(texts ...string) {
	for _, s := range texts {
		d.history = append(d.history, core.Message{Role: "user", Content: s})
		d.seq.tip += 10
	}
}

func (d *driver) assemble(ctx context.Context) Assembled {
	d.turn++
	out, err := d.e.Assemble(ctx, d.history, d.turn)
	if err != nil {
		d.t.Fatalf("Assemble: %v", err)
	}
	return out
}

// closeStage 打一个阶段边界并在当前 history 末端消费它（干净地关闭当前阶段）。
func (d *driver) closeStage(ctx context.Context) Assembled {
	d.e.MarkBoundary()
	return d.assemble(ctx)
}

// newTestEngine 造一个带替身的 engine（W=256k → L1=25.6k / L2=2.56k / StageBudget=85.3k）。
func newTestEngine(t *testing.T, sum Summarizer, emit *fakeEmitter, seq *fakeSeq, text *fakeText, ledger LedgerSource) *Engine {
	t.Helper()
	e, err := NewEngine(Params{Window: 256_000}, seq, text, ledger, sum, emit)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return e
}

// bigMsg 造一条约占 chars 字节的消息（4 字节 ≈ 1 token）。
func bigMsg(chars int) string { return strings.Repeat("x", chars) }

// tierOf 取某阶段的当前档位。
func tierOf(e *Engine, n int) contracts.ContextTier {
	for _, s := range e.segments {
		if s.number == n {
			return s.tier
		}
	}
	return "?"
}

// ---------------------------------------------------------------------------
// 酸测试：单阶段、无注入 = append 等价
// ---------------------------------------------------------------------------

func TestEngineSingleStageIsAppendEquivalent(t *testing.T) {
	seq := &fakeSeq{tip: 30}
	e := newTestEngine(t, &fakeSummarizer{}, &fakeEmitter{}, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	d.add("你好", "读一下 plan.md")
	out := d.assemble(context.Background())

	// 无 rules/ledger/stages/common-sense → 消息就是原始 history（append 等价）。
	if len(out.Messages) != len(d.history) {
		t.Fatalf("单阶段应原样返回 history：得到 %d 条，want %d", len(out.Messages), len(d.history))
	}
	for i := range d.history {
		if out.Messages[i].Content != d.history[i].Content {
			t.Fatalf("消息 %d 被改写: %q vs %q", i, out.Messages[i].Content, d.history[i].Content)
		}
	}
	// parts 只有一个 verbatim 的 history（无 stage:N / ledger / rules）。
	if len(out.Parts) != 1 || out.Parts[0].Source != sourceHistory || out.Parts[0].State != contracts.PartStateVerbatim {
		t.Fatalf("单阶段 parts 应只有一个 verbatim history: %+v", out.Parts)
	}
}

// ---------------------------------------------------------------------------
// 阶段边界 + 留存降级（full→l1→l2 最老先降、永不回升）
// ---------------------------------------------------------------------------

func TestStageBoundariesAndAgeRetention(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	e := newTestEngine(t, &fakeSummarizer{}, emit, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	// 阶段 1
	d.add("s1a", "s1b")
	d.assemble(ctx)
	d.closeStage(ctx)
	// 阶段 2
	d.add("s2a")
	d.assemble(ctx)
	d.closeStage(ctx)
	// 阶段 3
	d.add("s3a")
	d.assemble(ctx)
	d.closeStage(ctx)
	// 阶段 4（当前）
	d.add("s4a")
	d.assemble(ctx)

	// 当前=4：3=full、2=l1、1=l2。
	tiers := map[int]contracts.ContextTier{}
	for _, s := range e.segments {
		tiers[s.number] = s.tier
	}
	if tiers[1] != contracts.ContextTierL2 {
		t.Errorf("阶段 1 应 L2（最老先降），得到 %s", tiers[1])
	}
	if tiers[2] != contracts.ContextTierL1 {
		t.Errorf("阶段 2 应 L1（N-2），得到 %s", tiers[2])
	}
	if tiers[3] != contracts.ContextTierFull {
		t.Errorf("阶段 3 应 full（N-1），得到 %s", tiers[3])
	}
	if tiers[4] != contracts.ContextTierFull {
		t.Errorf("阶段 4（当前）应 full，得到 %s", tiers[4])
	}
}

func TestDowngradeIsOneWayOldestFirst(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	e := newTestEngine(t, &fakeSummarizer{}, emit, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	// 造 6 个阶段。
	for i := 0; i < 6; i++ {
		d.add("stage-content")
		d.assemble(ctx)
		if i < 5 {
			d.closeStage(ctx)
		}
	}
	// 当前=6：5=full、4=l1、1..3=l2。
	if tierOf(e, 5) != contracts.ContextTierFull || tierOf(e, 4) != contracts.ContextTierL1 {
		t.Fatalf("最新两个已完成阶段应为 full/l1: 5=%s 4=%s", tierOf(e, 5), tierOf(e, 4))
	}
	for n := 1; n <= 3; n++ {
		if tierOf(e, n) != contracts.ContextTierL2 {
			t.Errorf("阶段 %d 应 L2，得到 %s", n, tierOf(e, n))
		}
	}
	// 记录此刻档位，再推进一个阶段：旧阶段只能变更深，绝不回升。
	before := map[int]contracts.ContextTier{}
	for _, s := range e.segments {
		before[s.number] = s.tier
	}
	d.closeStage(ctx)
	d.add("more")
	d.assemble(ctx)
	for _, s := range e.segments {
		if b, ok := before[s.number]; ok {
			if tierRank(s.tier) < tierRank(b) {
				t.Errorf("阶段 %d 档位回升了：%s → %s（永不回升）", s.number, b, s.tier)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// refs 永不指摘要 + 级联 C 案（压缩输入 = ledger + 被压区间全文，无他阶段摘要）
// ---------------------------------------------------------------------------

func TestCompactionInputIsLedgerPlusRangeNoOtherSummaries(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	sum := &fakeSummarizer{}
	ledger := fakeLedger{lp: contracts.LedgerUpdatedPayload{
		Goals:       []string{"交付 stage1"},
		Constraints: []string{"不破坏 v1"},
	}}
	e := newTestEngine(t, sum, &fakeEmitter{}, seq, &fakeText{}, ledger)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	// 阶段 1 / 2 / 3，让 1 被压成 L2、2 被压成 L1。
	d.add("阶段一原文-独有标记A")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("阶段二原文-独有标记B")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("阶段三原文-独有标记C")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("阶段四原文")
	d.assemble(ctx)

	inputs := sum.recorded()
	if len(inputs) == 0 {
		t.Fatal("应有摘要调用")
	}
	for _, in := range inputs {
		// 输入含 goal ledger（级联 C 案的"总体局势"来自账本，不来自他阶段摘要）。
		if !strings.Contains(in.text, "交付 stage1") {
			t.Errorf("压缩输入应含 goal ledger: %q", in.text)
		}
		// 输入**绝不**含别的阶段摘要（没有 "【阶段" 摘要条目标记、没有 "SUMMARY" 注入）。
		if strings.Contains(in.text, "【阶段") {
			t.Errorf("压缩输入不得含阶段摘要条目（级联 C 案：永不读别的阶段摘要）: %q", in.text)
		}
		if strings.Contains(in.text, "SUMMARY") {
			t.Errorf("压缩输入不得含他阶段摘要内容: %q", in.text)
		}
	}
	// F24：每次摘要输入**只**含本阶段的原文标记（不含其他阶段的独有标记）。
	// 归属靠"被压区间"判定：输入里出现哪个阶段的标记，就是哪个阶段被压。
	markers := map[string]bool{"独有标记A": true, "独有标记B": true, "独有标记C": true}
	for _, in := range inputs {
		hit := []string{}
		for m := range markers {
			if strings.Contains(in.text, m) {
				hit = append(hit, m)
			}
		}
		if len(hit) != 1 {
			t.Errorf("压缩输入应恰含一个阶段的独有标记（只含被压区间全文），得到 %v: %q", hit, in.text)
		}
	}
}

// ---------------------------------------------------------------------------
// recall 取回持久化原文全文
// ---------------------------------------------------------------------------

func TestRecallResolvesStagePointerToRawText(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	text := &fakeText{byRange: map[string]string{}}
	e := newTestEngine(t, &fakeSummarizer{}, &fakeEmitter{}, seq, text, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add("阶段一原始命令输出")
	d.assemble(ctx)
	// 阶段 1 关闭时 seq 区间 = [1, 10]（tip 从 0 → 10）。
	d.closeStage(ctx)
	d.add("阶段二")
	d.assemble(ctx)

	// 预置该 seq 区间的持久化原文（模拟 store fold）。
	for _, s := range e.segments {
		if s.number == 1 {
			text.byRange[seqRangeRef(s.seqFrom, s.seqTo)] = "阶段一的完整原文：ls -la; cat foo.go"
		}
	}

	got, ok, err := e.Resolve(ctx, "stage:1")
	if err != nil || !ok {
		t.Fatalf("Resolve(stage:1) = ok=%v err=%v", ok, err)
	}
	if !strings.Contains(got, "阶段一的完整原文") {
		t.Fatalf("recall 应取回持久化原文全文: %q", got)
	}

	// 未知阶段 → ok=false。
	if _, ok, _ := e.Resolve(ctx, "stage:999"); ok {
		t.Error("未知阶段应 ok=false")
	}
	// 坏指针 → ok=false。
	if _, ok, _ := e.Resolve(ctx, "not-a-ref"); ok {
		t.Error("坏指针应 ok=false")
	}
}

// F9：带 seq 锚的指针跨 Run 稳定（阶段号每 Run 重排 → 短指针只是本 Run 别名）。
func TestAnchoredRefResolvesAcrossRuns(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	text := &fakeText{byRange: map[string]string{}}
	e := newTestEngine(t, &fakeSummarizer{}, &fakeEmitter{}, seq, text, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add("阶段一原始命令输出")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("阶段二")
	d.assemble(ctx)

	// 取阶段 1 的稳定指针（带 seq 锚）。
	var ref string
	for _, s := range e.segments {
		if s.number == 1 {
			ref = StageRefAnchored(s.number, s.seqFrom, s.seqTo)
		}
	}
	if !strings.Contains(ref, "@e:") {
		t.Fatalf("稳定指针应带 seq 锚: %q", ref)
	}
	text.byRange[seqRangeRef(1, 10)] = "阶段一的完整原文"
	got, ok, err := e.Resolve(ctx, ref)
	if err != nil || !ok {
		t.Fatalf("Resolve(锚定指针) = ok=%v err=%v", ok, err)
	}
	if !strings.Contains(got, "阶段一的完整原文") {
		t.Fatalf("锚定指针应取回同一段原文: %q", got)
	}

	// 模拟"下一个 Run"：引擎重建、阶段号从头重排 → 锚定指针仍解析到同一区间。
	e2 := newTestEngine(t, &fakeSummarizer{}, &fakeEmitter{}, seq, text, nil)
	got2, ok2, err2 := e2.Resolve(ctx, ref)
	if err2 != nil || !ok2 {
		t.Fatalf("新 Run 的引擎解析锚定指针 = ok=%v err=%v", ok2, err2)
	}
	if got2 != got {
		t.Fatalf("锚定指针跨 Run 应指向同一段原文：%q vs %q", got, got2)
	}
}

// ---------------------------------------------------------------------------
// 稳定前缀字节不变（缓存定律，J20）
// ---------------------------------------------------------------------------

func TestStablePrefixByteUnchangedAfterCompression(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	// 非确定性压缩模型：每次调用返回不同摘要（计数器）。这样"摘要算一次就缓存、绝不随轮次改写"
	// 才被测到——若引擎每轮重算，稳定前缀就会变（缓存全废，J20）。
	var mu sync.Mutex
	counter := 0
	sum := &fakeSummarizer{fn: func(string, string) (string, error) {
		mu.Lock()
		counter++
		n := counter
		mu.Unlock()
		return "SUMMARY-" + strconv.Itoa(n), nil
	}}
	e := newTestEngine(t, sum, &fakeEmitter{}, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	// 造 5 个阶段，让留存稳定：1..3=l2、4=l1、5=full(当前)。
	for i := 0; i < 5; i++ {
		d.add(bigMsg(2000))
		d.assemble(ctx)
		if i < 4 {
			d.closeStage(ctx)
		}
	}
	first := d.assemble(ctx)
	compressedBefore := compressedMessages(first.Messages)

	// 只往当前阶段追加内容（不触发新降级）→ 已压阶段的摘要消息必须逐字节不变。
	d.add(bigMsg(2000), bigMsg(2000))
	second := d.assemble(ctx)
	compressedAfter := compressedMessages(second.Messages)

	if len(compressedBefore) == 0 {
		t.Fatal("应有已压缩阶段消息")
	}
	// 前缀（已压摘要）逐字节相同。
	for i := range compressedBefore {
		if i >= len(compressedAfter) {
			t.Fatalf("第二次装配的已压阶段消息变少了")
		}
		if compressedBefore[i] != compressedAfter[i] {
			t.Fatalf("稳定前缀字节被改写（缓存全废）：\n before=%q\n after =%q", compressedBefore[i], compressedAfter[i])
		}
	}
}

// F25：稳定前缀断言要覆盖**整条 prompt 的公共前缀**（含 rules / ledger 稳定半），
// 不只是已压阶段条目。
func TestStablePrefixIncludesRulesAndLedger(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	var mu sync.Mutex
	counter := 0
	sum := &fakeSummarizer{fn: func(string, string) (string, error) {
		mu.Lock()
		counter++
		n := counter
		mu.Unlock()
		return "SUMMARY-" + strconv.Itoa(n), nil
	}}
	ledger := &mutableLedger{lp: contracts.LedgerUpdatedPayload{
		Goals:       []string{"交付 stage1"},
		Constraints: []string{"不破坏 v1"},
		Completed:   []string{"已完成-A"},
		Pending:     []string{"待解-B"},
	}}
	e := newTestEngine(t, sum, &fakeEmitter{}, seq, &fakeText{}, ledger)
	e.SetRules("[项目规则] 遵循 AGENTS.md")
	d := newDriver(t, e, seq)
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		d.add(bigMsg(2000))
		d.assemble(ctx)
		if i < 3 {
			d.closeStage(ctx)
		}
	}
	first := d.assemble(ctx)

	// 账本易变半变了（每轮都在变）+ 当前阶段追加内容 → 整条 prompt 的**公共前缀**
	// （rules + ledger 稳定半 + 已压阶段）必须逐字节不变。
	ledger.set(contracts.LedgerUpdatedPayload{
		Goals:       []string{"交付 stage1"},
		Constraints: []string{"不破坏 v1"},
		Completed:   []string{"已完成-A2"},
		Pending:     []string{"待解-B2"},
	})
	d.add(bigMsg(2000))
	second := d.assemble(ctx)

	prefix := commonPrefix(first.Messages, second.Messages)
	if len(prefix) == 0 {
		t.Fatal("两次装配应有一段公共前缀")
	}
	// 公共前缀必须覆盖 rules 与 ledger 稳定半（它们在最前，且是稳定内容）。
	joined := prefixText(prefix)
	if !strings.Contains(joined, "[项目规则] 遵循 AGENTS.md") {
		t.Errorf("公共前缀应含 rules（稳定前缀的一部分）: %q", joined)
	}
	if !strings.Contains(joined, "交付 stage1") {
		t.Errorf("公共前缀应含 ledger 稳定半（目标/约束）: %q", joined)
	}
	// 易变半（已完成/待解）**不进**前缀：它每轮都变，混进前缀 = 缓存全废（F5）。
	if strings.Contains(joined, "已完成-A") || strings.Contains(joined, "待解-B") {
		t.Errorf("ledger 易变半不得进稳定前缀（F5）: %q", joined)
	}
	// 易变半仍要出现在上下文里（贴生长前沿）。
	if !strings.Contains(prefixText(second.Messages), "已完成-A2") {
		t.Errorf("ledger 易变半应注入上下文: %q", prefixText(second.Messages))
	}
}

// compressedMessages 取出装配结果里带"【阶段"摘要标记的消息（已压阶段条目）。
func compressedMessages(msgs []core.Message) []string {
	var out []string
	for _, m := range msgs {
		if strings.HasPrefix(m.Content, "【阶段") {
			out = append(out, m.Content)
		}
	}
	return out
}

// commonPrefix 返回两条消息序列的公共前缀（按内容逐条比）。
func commonPrefix(a, b []core.Message) []core.Message {
	var out []core.Message
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i].Role != b[i].Role || a[i].Content != b[i].Content {
			break
		}
		out = append(out, a[i])
	}
	return out
}

// prefixText 把消息序列拍成一段文本（便于断言）。
func prefixText(msgs []core.Message) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(m.Content)
		b.WriteString("\n")
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// L1/L2 目标残留（J9：构造负载实测 ≤10% / ≤1% 窗口）
// ---------------------------------------------------------------------------

func TestL1L2ResidualWithinTargetOnConstructedLoad(t *testing.T) {
	// W=256k → L1=25_600 / L2=2_560 token。造一个 ~60k token 的阶段。
	const stageChars = 60_000 * 4
	// 压缩模型尊重预算：产出约目标 1/4 大小的摘要（留足余量）。
	sum := &fakeSummarizer{fn: func(instruction, text string) (string, error) {
		return strings.Repeat("s", 4000), nil // 1000 token，远小于 L1/L2 目标
	}}
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	e := newTestEngine(t, sum, emit, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add(bigMsg(stageChars))
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("stage2")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("stage3")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("stage4") // 让阶段 1 沉到 L2、阶段 2 到 L1
	d.assemble(ctx)

	p := DefaultParams()
	p.Window = 256_000
	l1Target := p.L1TargetTokens() // 25600
	l2Target := p.L2TargetTokens() // 2560

	// 从 context.assembled 的 parts 读各 stage:N 的占用，断言 ≤ 目标。
	out := d.assemble(ctx)
	for _, part := range out.Parts {
		if !strings.HasPrefix(part.Source, "stage:") {
			continue
		}
		n, _ := ParseStageRef(part.Source)
		var want int64
		switch n {
		case 1:
			want = l2Target // 最老 → L2
		case 2:
			want = l1Target // N-2 → L1
		default:
			continue
		}
		if part.Tokens > want {
			t.Errorf("阶段 %d 残留 %d token 超目标 %d（J9）", n, part.Tokens, want)
		}
		if part.TargetTokens == nil || *part.TargetTokens != want {
			t.Errorf("阶段 %d 的 target_tokens 应=%d，得到 %v", n, want, part.TargetTokens)
		}
	}
}

// F3：压缩模型**不守预算**时，引擎必须硬校验（阈值是框架的约束，不是对模型的君子协定）。
func TestEngineEnforcesTierTargetWhenSummarizerIgnoresBudget(t *testing.T) {
	// 听话的 fake 换成**不听话**的：无视指令，返回超目标 10 倍的摘要。
	sum := &fakeSummarizer{fn: func(string, string) (string, error) {
		return strings.Repeat("z", 400_000), nil // 100k token ≫ L1(25.6k)/L2(2.56k)
	}}
	seq := &fakeSeq{tip: 0}
	e := newTestEngine(t, sum, &fakeEmitter{}, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add(bigMsg(60_000 * 4))
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("stage2")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("stage3")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("stage4")
	out := d.assemble(ctx)

	p := DefaultParams()
	p.Window = 256_000
	l1Target := p.L1TargetTokens()
	l2Target := p.L2TargetTokens()
	checked := 0
	for _, part := range out.Parts {
		if !strings.HasPrefix(part.Source, "stage:") {
			continue
		}
		checked++
		if part.Tokens > l1Target {
			t.Errorf("阶段 %s 残留 %d token 超 L1 目标 %d（引擎必须硬校验，J9/F3）", part.Source, part.Tokens, l1Target)
		}
	}
	if checked == 0 {
		t.Fatal("应有已压缩阶段 part")
	}
	// L2 更严：逐段核对（阶段 1 沉到 L2）。
	for _, s := range e.segments {
		if s.tier != contracts.ContextTierL2 {
			continue
		}
		if estimateTokens(s.summary) > l2Target {
			t.Errorf("阶段 %d 的 L2 摘要 %d token 超目标 %d（引擎必须硬校验）", s.number, estimateTokens(s.summary), l2Target)
		}
	}
}

// F4：截断按 token 预算（不是字节预算）——中文 3 字节/字，按字节切会超目标 3 倍。
func TestTruncateRespectsTokenBudgetForChinese(t *testing.T) {
	const maxTokens = 100
	src := strings.Repeat("中", 10_000) // 3 字节/字
	got := truncate(src, maxTokens)
	if estimateTokens(got) > maxTokens {
		t.Fatalf("截断后 %d token，超预算 %d（F4：必须按 token 而不是字节切）", estimateTokens(got), maxTokens)
	}
	if !strings.HasPrefix(src, strings.TrimSuffix(got, "…")) {
		t.Fatalf("截断应保留前缀")
	}
	// ASCII 同样成立。
	ascii := strings.Repeat("x", 10_000)
	if got := truncate(ascii, maxTokens); estimateTokens(got) > maxTokens {
		t.Fatalf("ASCII 截断后 %d token，超预算 %d", estimateTokens(got), maxTokens)
	}
}

// ---------------------------------------------------------------------------
// 兜底三级：① L1→L2 ② 切当前阶段（诊断事件）③ W/3 硬预算告警
// ---------------------------------------------------------------------------

func TestFallbackDowngradeL1ToL2ThenSplitThenWarn(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	// 压缩模型产出 ~2000 token 的大摘要，让 40 个阶段的压缩历史区远超 HistoryBudget(256k)=51k。
	bigSum := &fakeSummarizer{fn: func(string, string) (string, error) { return bigMsg(8000), nil }}
	e := newTestEngine(t, bigSum, emit, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	// 造 40 个已完成阶段 + 1 个小的当前阶段。
	for i := 0; i < 40; i++ {
		d.add(bigMsg(8000))
		d.assemble(ctx)
		d.closeStage(ctx)
	}
	d.add("cur")
	d.assemble(ctx)

	// ① 历史区超预算 → 最老先降，所有非 L2 阶段被压到 L2（含全部 L1→L2）。
	for _, s := range e.segments {
		if s.number == e.segments[len(e.segments)-1].number {
			continue
		}
		if s.tier != contracts.ContextTierL2 {
			t.Errorf("超预算后阶段 %d 应降到 L2，得到 %s", s.number, s.tier)
		}
	}
	// 有 downgrade 事件；此刻当前阶段小，不应 split / 告警。
	var downgrades, splits, warns int
	for _, ev := range emit.recorded() {
		switch ev.Action {
		case contracts.ContextCompactionActionDowngrade:
			downgrades++
		case contracts.ContextCompactionActionSplit:
			splits++
		case contracts.ContextCompactionActionBudgetWarning:
			warns++
		}
	}
	if downgrades == 0 {
		t.Error("超预算应发 downgrade 事件")
	}
	if splits != 0 || warns != 0 {
		t.Errorf("当前阶段未超限时不应 split/告警：split=%d warn=%d", splits, warns)
	}
}

func TestFallbackSplitCurrentStageEmitsDiagnostic(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	e := newTestEngine(t, &fakeSummarizer{}, emit, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add("s1")
	d.assemble(ctx)
	// 当前阶段灌一个超过 2×W/3（2×85_333≈170_666 token → 需 >682k 字节）的超大阶段。
	d.add(bigMsg(700_000))
	out := d.assemble(ctx)

	// 应发生 split：发一个 split 诊断事件。
	var split *contracts.ContextCompactedPayload
	events := emit.recorded()
	for i := range events {
		if events[i].Action == contracts.ContextCompactionActionSplit {
			split = &events[i]
		}
	}
	if split == nil {
		t.Fatal("超大当前阶段应触发 split 诊断事件")
	}
	if split.Reason == nil || *split.Reason != reasonOrchestratorOversize {
		t.Errorf("split 原因应标记 orchestrator_oversize（上游阶段切大）: %v", split.Reason)
	}
	// 切分后应有多个阶段（原阶段前半成 L1 + 新当前阶段）。
	if len(e.segments) < 2 {
		t.Errorf("split 后应至少 2 个阶段，得到 %d", len(e.segments))
	}
	// F11：一次动作一条事件 —— split 产生的阶段**不再**另发 downgrade。
	for _, ev := range events {
		if ev.Action != contracts.ContextCompactionActionDowngrade {
			continue
		}
		if ev.Stage == split.Stage {
			t.Errorf("split 的阶段不该再发 downgrade（F11：一次动作一条事件）: %+v", ev)
		}
	}
	// F26：split 的 tokens_after 按切后实际算（不是 tokens_before/2 编造）。
	if split.TokensAfter <= 0 || split.TokensAfter >= split.TokensBefore {
		t.Errorf("split 的 tokens_after 应是切后实际占用（0 < after < before），得到 %d / %d",
			split.TokensAfter, split.TokensBefore)
	}
	_ = out
}

// F20：切分按 **token 累计中点**，不是按消息条数中点。
func TestSplitCutsByTokensNotMessageCount(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	e := newTestEngine(t, &fakeSummarizer{}, emit, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add("s1")
	d.assemble(ctx)
	// 一条巨消息 + 若干小消息：按条数中点会切在巨消息**之前**（前半几乎没内容），
	// 按 token 中点应切在巨消息**之后**（前半含那条巨消息 → 被压成 L1）。
	d.add(bigMsg(700_000))
	for i := 0; i < 6; i++ {
		d.add("小消息")
	}
	d.assemble(ctx)

	if len(e.segments) < 2 {
		t.Fatalf("超大当前阶段应触发 split，得到 %d 个阶段", len(e.segments))
	}
	// 被切开的那一段（number=1）应是前半：含那条巨消息（msgTo 覆盖到巨消息）。
	front := e.segments[0]
	if front.msgTo < 2 {
		t.Errorf("按 token 中点切：前半应含那条巨消息（msgTo=%d），得到 %d", 2, front.msgTo)
	}
}

// F14：seq 区间不得倒挂（tip=0 时不能写 [1,0]）。
func TestSeqRangeNeverInverted(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	e := newTestEngine(t, &fakeSummarizer{}, emit, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add("s1")
	d.assemble(ctx)
	for _, ev := range emit.recorded() {
		if ev.SeqFrom < 0 || ev.SeqTo < ev.SeqFrom {
			t.Errorf("seq 区间倒挂: [%d, %d]", ev.SeqFrom, ev.SeqTo)
		}
	}
	for _, s := range e.segments {
		if s.seqTo < s.seqFrom {
			t.Errorf("阶段 %d 的 seq 区间倒挂: [%d, %d]", s.number, s.seqFrom, s.seqTo)
		}
	}
}

// F15：budget_warning 按阶段+档位去重（不每轮重复发）。
func TestBudgetWarningDedupedPerStageAndTier(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	e := newTestEngine(t, &fakeSummarizer{}, emit, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add("s1")
	d.assemble(ctx)
	d.add(bigMsg(400_000)) // 超 W/3（85_333 token → >341k 字节）
	d.assemble(ctx)
	d.add(bigMsg(1000))
	d.assemble(ctx)
	d.add(bigMsg(1000))
	d.assemble(ctx)

	warns := 0
	for _, ev := range emit.recorded() {
		if ev.Action == contracts.ContextCompactionActionBudgetWarning {
			warns++
		}
	}
	if warns != 1 {
		t.Errorf("同一阶段同一档位的硬预算告警只应发一次，得到 %d 次", warns)
	}
}

// F19：压缩模型出错 → 退化为确定性截断，并**留痕**（事件 reason=fallback_truncate）。
func TestFallbackTruncateIsRecorded(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	boom := &fakeSummarizer{fn: func(string, string) (string, error) {
		return "", errors.New("压缩模型炸了")
	}}
	e := newTestEngine(t, boom, emit, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add("阶段一内容")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("阶段二")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("阶段三")
	d.assemble(ctx)

	var got *contracts.ContextCompactedPayload
	events := emit.recorded()
	for i := range events {
		if events[i].Action == contracts.ContextCompactionActionFallback {
			got = &events[i]
		}
	}
	if got == nil {
		t.Fatal("压缩模型失败退化为截断时应发 fallback 事件留痕")
	}
	if got.Reason == nil || *got.Reason != reasonFallbackTruncate {
		t.Errorf("fallback 事件 reason 应为 %s，得到 %v", reasonFallbackTruncate, got.Reason)
	}
}

// F6：硬边界（验证通过点）API 存在，且收尾的阶段带 validation_pass 归因。
func TestHardBoundaryMarksValidationPass(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	e := newTestEngine(t, &fakeSummarizer{}, emit, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add("阶段一")
	d.assemble(ctx)
	e.MarkValidationPassed() // 硬边界 = 验证通过点
	d.assemble(ctx)          // 消费边界：关闭阶段 1、开启阶段 2
	d.add("阶段二")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("阶段三")
	d.assemble(ctx)

	var sawValidationPass bool
	for _, ev := range emit.recorded() {
		if ev.Reason != nil && *ev.Reason == reasonValidationPass {
			sawValidationPass = true
		}
	}
	if !sawValidationPass {
		t.Errorf("硬边界收尾的阶段应带 %s 归因（实现要产出契约列举的取值）", reasonValidationPass)
	}
}

// F27：recall 不可见（被策略 deny）时，摘要条目不附 recall 调用说明。
func TestRecallHintOnlyWhenRecallVisible(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	e := newTestEngine(t, &fakeSummarizer{}, &fakeEmitter{}, seq, &fakeText{}, nil)
	e.SetVisibleToolNames([]string{}) // recall 不可见
	d := newDriver(t, e, seq)
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		d.add("内容")
		d.assemble(ctx)
		if i < 3 {
			d.closeStage(ctx)
		}
	}
	out := d.assemble(ctx)
	for _, m := range out.Messages {
		if strings.Contains(m.Content, `recall("`) {
			t.Errorf("recall 不可见时不应提示调用 recall: %q", m.Content)
		}
	}

	// recall 可见 → 有提示。
	e2 := newTestEngine(t, &fakeSummarizer{}, &fakeEmitter{}, seq, &fakeText{}, nil)
	e2.SetVisibleToolNames([]string{ToolRecall})
	d2 := newDriver(t, e2, seq)
	for i := 0; i < 4; i++ {
		d2.add("内容")
		d2.assemble(ctx)
		if i < 3 {
			d2.closeStage(ctx)
		}
	}
	out2 := d2.assemble(ctx)
	found := false
	for _, m := range out2.Messages {
		if strings.Contains(m.Content, `recall("`) {
			found = true
		}
	}
	if !found {
		t.Error("recall 可见时应提示调用说明")
	}
}

// F2：总量护栏 —— base+tools+历史区+当前阶段 ≤ W；超了先降历史区。
func TestTotalBudgetGuardDowngradesHistory(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	e := newTestEngine(t, &fakeSummarizer{}, emit, seq, &fakeText{}, nil)
	// 固定开销（system + 工具 schema）占掉大半个窗口：只有总量护栏能发现超窗。
	e.SetBaseTokens(200_000)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add(bigMsg(40_000)) // 10k token
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add(bigMsg(40_000)) // 当前阶段 10k token
	d.assemble(ctx)

	// 总量 = 200k(base) + 10k(历史) + 10k(当前) = 220k ≤ 256k → 还不用降。
	if tierOf(e, 1) != contracts.ContextTierFull {
		t.Fatalf("总量未超窗时不该降级，得到 %s", tierOf(e, 1))
	}
	// 再加 40k token 的当前阶段 → 总量 200k+10k+50k = 260k 超 256k → 必须降历史区。
	d.add(bigMsg(160_000)) // +40k token
	d.assemble(ctx)
	if tierOf(e, 1) == contracts.ContextTierFull {
		t.Fatalf("总量超 W 时必须先降历史区（阶段 1 仍是 full）")
	}
	// 降级事件带预算兜底归因。
	var sawBudget bool
	for _, ev := range emit.recorded() {
		if ev.Action == contracts.ContextCompactionActionDowngrade && ev.Reason != nil && *ev.Reason == reasonBudgetFallback {
			sawBudget = true
		}
	}
	if !sawBudget {
		t.Error("预算护栏触发的降级应带 budget_fallback 归因")
	}
}

// F16：压缩模型调用的 usage（+模型名）随 context.compacted 事件留痕——
// 压缩也是一次模型调用，成本要进成本统计（J21）。
func TestCompactionUsageRecordedInEvent(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	e := newTestEngine(t, &fakeSummarizer{}, emit, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add("阶段一")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("阶段二")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("阶段三")
	d.assemble(ctx)

	var found bool
	for _, ev := range emit.recorded() {
		if ev.Action != contracts.ContextCompactionActionDowngrade {
			continue
		}
		found = true
		if ev.Usage == nil {
			t.Fatalf("downgrade 事件应带压缩调用的 usage（F16）: %+v", ev)
		}
		if ev.Usage.InputTokens != 10 || ev.Usage.OutputTokens != 5 {
			t.Errorf("usage 数值不对: %+v", ev.Usage)
		}
		if ev.Model == nil || *ev.Model != "fake-model" {
			t.Errorf("事件应带压缩模型名（成本按模型分组）: %v", ev.Model)
		}
	}
	if !found {
		t.Fatal("应有 downgrade 事件")
	}
}

// ---------------------------------------------------------------------------
// #1：纯机器指针 "e:<from>..e:<to>" 必须能解析（recall 的描述/schema/报错文案都宣称它合法）。
// ---------------------------------------------------------------------------

func TestResolvePureSeqRangeRef(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	text := &fakeText{byRange: map[string]string{
		seqRangeRef(1, 30): "区间 1..30 的原文",
	}}
	e := newTestEngine(t, &fakeSummarizer{}, &fakeEmitter{}, seq, text, nil)
	ctx := context.Background()

	got, ok, err := e.Resolve(ctx, "e:1..e:30")
	if err != nil || !ok {
		t.Fatalf("Resolve(\"e:1..e:30\") = ok=%v err=%v（纯机器指针必须可解析）", ok, err)
	}
	if !strings.Contains(got, "区间 1..30") {
		t.Fatalf("纯机器指针应取回该区间的原文: %q", got)
	}
	// 实际读到的区间就是指针里的区间。
	calls := text.calls
	if len(calls) == 0 {
		t.Fatal("应有回捞调用")
	}
	if calls[len(calls)-1].from != 1 || calls[len(calls)-1].to != 30 {
		t.Errorf("回捞区间应 = 指针区间，得到 [%d,%d]", calls[len(calls)-1].from, calls[len(calls)-1].to)
	}
	// 非法区间仍不可解析。
	if _, ok, _ := e.Resolve(ctx, "e:30..e:1"); ok {
		t.Error("倒挂区间应不可解析")
	}
}

// ---------------------------------------------------------------------------
// #2：splitMade 是**本轮**语义——被 split 过的阶段后续老化降级要照常发 downgrade。
// ---------------------------------------------------------------------------

func TestSplitStageStillDowngradesLater(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	e := newTestEngine(t, &fakeSummarizer{}, emit, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	// 阶段 1 正常；阶段 2 灌一个超大阶段 → 触发 split（阶段 2 被切成 L1 + 新当前阶段）。
	d.add("阶段一")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add(bigMsg(700_000)) // 超 2×W/3 → split
	d.assemble(ctx)

	var split *contracts.ContextCompactedPayload
	events := emit.recorded()
	for i := range events {
		if events[i].Action == contracts.ContextCompactionActionSplit {
			split = &events[i]
		}
	}
	if split == nil {
		t.Fatal("应触发 split")
	}
	splitStage := split.Stage

	// 再推进几个阶段 → 被 split 过的那个阶段会继续老化降级（L1 → L2），**必须**发 downgrade。
	for i := 0; i < 4; i++ {
		d.add("后续内容")
		d.closeStage(ctx)
	}

	var sawDowngrade bool
	for _, ev := range emit.recorded() {
		if ev.Action == contracts.ContextCompactionActionDowngrade && ev.Stage == splitStage {
			sawDowngrade = true
		}
	}
	if !sawDowngrade {
		t.Errorf("被 split 过的阶段（%d）后续老化降级应照常发 downgrade 事件（splitMade 只抑制同轮）", splitStage)
	}
}

// ---------------------------------------------------------------------------
// #5：注入区独大时总量护栏降不动 → 必须发 budget_warning 留痕（不许静默超窗）。
// ---------------------------------------------------------------------------

func TestInjectionOversizeEmitsWarning(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	e := newTestEngine(t, &fakeSummarizer{}, emit, seq, &fakeText{}, nil)
	// 注入区（固定开销 = system + 工具 schema，loop 告知）独大：> W（256k）→ 护栏降不动
	//（这一块没有档位可降）。
	e.SetBaseTokens(300_000)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add("内容")
	d.assemble(ctx)

	var warn *contracts.ContextCompactedPayload
	events := emit.recorded()
	for i := range events {
		if events[i].Action == contracts.ContextCompactionActionBudgetWarning {
			warn = &events[i]
		}
	}
	if warn == nil {
		t.Fatal("注入区独大时必须发 budget_warning 留痕（不许静默装出超 W 的请求）")
	}
	if warn.Reason == nil || *warn.Reason != reasonInjectionOversize {
		t.Errorf("告警 reason 应为 %s，得到 %v", reasonInjectionOversize, warn.Reason)
	}
}

// #5 的另一半：注入文本本身要有上限（规则/常识区不可压缩，没上限就能顶穿窗口）。
func TestInjectedTextIsCapped(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	e := newTestEngine(t, &fakeSummarizer{}, &fakeEmitter{}, seq, &fakeText{}, nil)
	e.SetRules(bigMsg(2_000_000))       // 500k token
	e.SetCommonSense(bigMsg(4_000_000)) // 1M token
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add("内容")
	out := d.assemble(ctx)
	seen := 0
	for _, p := range out.Parts {
		switch p.Source {
		case sourceRules:
			seen++
			if p.Tokens > prefixBudget {
				t.Errorf("规则注入应 ≤ %d token，得到 %d", prefixBudget, p.Tokens)
			}
		case sourceCommonSense:
			seen++
			if p.Tokens > commonSenseBudget {
				t.Errorf("常识区注入应 ≤ %d token，得到 %d", commonSenseBudget, p.Tokens)
			}
		}
	}
	if seen != 2 {
		t.Errorf("应有 rules 与 common_sense 两个 part，得到 %d", seen)
	}
}

// ---------------------------------------------------------------------------
// #7：同轮连续 split 时，早先那条 split 事件的 tokens_after 不能用过期段状态算。
// ---------------------------------------------------------------------------

func TestConsecutiveSplitsTokensAfterUsesCurrentState(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	emit := &fakeEmitter{}
	e := newTestEngine(t, &fakeSummarizer{}, emit, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	// W=256k → 切分硬上限 2×W/3 ≈ 170_666 token（≈682k 字节）。
	// 两条各 200k token 的消息 → 总量 400k：切开后后半仍超上限 → **同轮第二次切分**。
	d.add(bigMsg(800_000))
	d.add(bigMsg(800_000))
	d.assemble(ctx)

	var splits []contracts.ContextCompactedPayload
	for _, ev := range emit.recorded() {
		if ev.Action == contracts.ContextCompactionActionSplit {
			splits = append(splits, ev)
		}
	}
	if len(splits) < 2 {
		t.Fatalf("应触发同轮连续 split，得到 %d 条", len(splits))
	}
	// 第一条 split 事件的 tokens_after 必须按**发事件那一刻**的段状态算：
	// 两半此时都已是 L1 摘要（各约 1 token），不该再算成后半的原文（200k token）。
	if splits[0].TokensAfter > 1000 {
		t.Errorf("第一条 split 的 tokens_after 应按当前段状态算（两半都是摘要），得到 %d（过期状态？）",
			splits[0].TokensAfter)
	}
}

// ---------------------------------------------------------------------------
// #1（选 a：真让位）：历史区份额按实际注入区预算让位 + 负预算地雷在装配期被挡。
// ---------------------------------------------------------------------------

func TestHistoryBudgetYieldsToConfiguredInjectionBudgets(t *testing.T) {
	// 默认布局：W − 205k（13 §8 的表）。
	if got := HistoryBudget(256_000); got != 51_000 {
		t.Fatalf("HistoryBudget(256k) = %d, want 51000", got)
	}
	// 注入区变大 → 历史区让位（真让位，不是口号）。
	p := DefaultParams()
	p.Window = 256_000
	p.RulesBudget = 40_000 // 前缀从 15k 涨到 40k
	if got := historyBudgetOf(p); got != 256_000-(40_000+30_000+150_000+10_000) {
		t.Fatalf("历史区份额未让位: %d", got)
	}
	// 负预算地雷：注入区把窗口吃光 → 装配期拒绝（NewEngine / ValidateLayout）。
	bad := DefaultParams()
	bad.Window = 256_000
	bad.RulesBudget = 100_000
	if historyBudgetOf(bad) >= 0 {
		t.Fatalf("该配置应算出负历史区预算，得到 %d", historyBudgetOf(bad))
	}
	if err := ValidateLayout(bad); err == nil {
		t.Error("负历史区预算应在 ValidateLayout 被拒绝")
	}
	if _, err := NewEngine(bad, nil, nil, nil, nil, nil); err == nil {
		t.Error("负历史区预算应在 NewEngine（挂载期）被拒绝")
	}
	// 默认参数不受影响。
	if err := ValidateLayout(DefaultParams()); err != nil {
		t.Errorf("默认参数应通过布局校验: %v", err)
	}
}

// #1 的运行期兜底：历史区预算以 0 为下限（不许拿负预算硬跑）。
// #1 的运行期兜底：历史区预算以 0 为下限（不许拿负预算硬跑）。
//
// 正常路径下负预算在**挂载期**就被拒了（NewEngine/ValidateLayout）；这里直接改引擎参数
// 模拟"绕过校验"的情形，验证运行期不会拿负预算硬跑（不会炸、能装配完）。
func TestHistoryBudgetFloorIsZero(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	e := newTestEngine(t, &fakeSummarizer{}, &fakeEmitter{}, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	// 绕过校验：把前缀预算调到吃光窗口 → 历史区预算变负。
	e.mu.Lock()
	e.params.RulesBudget = 100_000
	e.mu.Unlock()
	if historyBudgetOf(e.params) >= 0 {
		t.Fatalf("该配置应算出负历史区预算，得到 %d", historyBudgetOf(e.params))
	}

	d.add("阶段一")
	d.assemble(ctx)
	d.closeStage(ctx)
	d.add("阶段二")
	// 不 panic、能装配完 = 兜底生效（负预算被夹到 0，不拿负数硬跑）。
	out := d.assemble(ctx)
	if len(out.Messages) == 0 {
		t.Fatal("应装配出消息")
	}
}

// ---------------------------------------------------------------------------
// #2：注入文本在**设置时**截断并缓存（不每轮重算）；截断事实可观测。
// ---------------------------------------------------------------------------

func TestInjectedTextTruncatedOnceAndCached(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	e := newTestEngine(t, &fakeSummarizer{}, &fakeEmitter{}, seq, &fakeText{}, nil)
	e.SetRules(bigMsg(2_000_000)) // 500k token ≫ 15k 上限
	d := newDriver(t, e, seq)
	ctx := context.Background()

	d.add("内容")
	out := d.assemble(ctx)
	var rulesTokens int64
	for _, p := range out.Parts {
		if p.Source == sourceRules {
			rulesTokens = p.Tokens
		}
	}
	if rulesTokens > prefixBudget {
		t.Errorf("规则注入应 ≤ %d token，得到 %d", prefixBudget, rulesTokens)
	}
	// 截断事实可观测（宿主据此留痕——退化必须可见）。
	r, cs := e.InjectionTruncated()
	if !r {
		t.Error("规则被截断应可观测（InjectionTruncated）")
	}
	if cs {
		t.Error("未设常识区，不应报截断")
	}
}

// #2 的缓存失效场景：重新设置 → 缓存必须跟着换（不能拿旧的截断结果）。
func TestInjectedTextCacheInvalidatedOnReset(t *testing.T) {
	seq := &fakeSeq{tip: 0}
	e := newTestEngine(t, &fakeSummarizer{}, &fakeEmitter{}, seq, &fakeText{}, nil)
	d := newDriver(t, e, seq)
	ctx := context.Background()

	e.SetRules("短规则")
	d.add("内容")
	first := d.assemble(ctx)
	if !containsRules(first.Messages, "短规则") {
		t.Fatalf("第一次装配应含规则原文: %+v", first.Messages)
	}
	if r, _ := e.InjectionTruncated(); r {
		t.Error("短规则不该被截断")
	}

	// 换成超长的规则 → 缓存失效，重新截断。
	e.SetRules(bigMsg(2_000_000))
	d.add("内容2")
	second := d.assemble(ctx)
	if containsRules(second.Messages, "短规则") {
		t.Errorf("重新设置后应换成新规则（缓存失效）: %+v", second.Messages)
	}
	if r, _ := e.InjectionTruncated(); !r {
		t.Error("超长规则应被截断并可观测")
	}
	// 换回短规则 → 截断标记复位。
	e.SetRules("另一条短规则")
	d.add("内容3")
	third := d.assemble(ctx)
	if !containsRules(third.Messages, "另一条短规则") {
		t.Errorf("应换成新的短规则: %+v", third.Messages)
	}
	if r, _ := e.InjectionTruncated(); r {
		t.Error("换回短规则后截断标记应复位")
	}
}

// containsRules 报告消息里是否含给定规则文本。
func containsRules(msgs []core.Message, want string) bool {
	for _, m := range msgs {
		if strings.Contains(m.Content, want) {
			return true
		}
	}
	return false
}

// #1 的**接线**证据：同样的内容，注入区预算不同 → 历史区的留存结果不同。
//
// W=256k：默认布局的历史区份额 = 51k；把前缀预算调到 40k → 份额 = 26k。
// 一段 ~30k token 的已完成阶段：默认口径留得住（30k ≤ 51k），让位口径留不住
// （30k > 26k → 必须降级）。这才是"让位"真接上了的证据（而不只是 historyBudgetOf 算得对）。
func TestEngineHistoryBudgetFollowsConfiguredInjectionBudgets(t *testing.T) {
	build := func(rulesBudget int64) contracts.ContextTier {
		seq := &fakeSeq{tip: 0}
		e, err := NewEngine(Params{Window: 256_000, RulesBudget: rulesBudget},
			seq, nil, nil, &fakeSummarizer{}, &fakeEmitter{})
		if err != nil {
			t.Fatalf("NewEngine(rules=%d): %v", rulesBudget, err)
		}
		d := newDriver(t, e, seq)
		ctx := context.Background()
		d.add(bigMsg(120_000)) // 30k token
		d.assemble(ctx)
		d.closeStage(ctx)
		d.add("阶段二")
		d.assemble(ctx)
		for _, s := range e.segments {
			if s.number == 1 {
				return s.tier
			}
		}
		return "?"
	}
	// 默认口径（前缀 15k → 份额 51k）：留得住 → full。
	if tier := build(0); tier != contracts.ContextTierFull {
		t.Errorf("默认布局下 30k 的阶段应留得住（份额 51k），得到 %s", tier)
	}
	// 让位口径（前缀 40k → 份额 26k）：留不住 → 必须降级。
	if tier := build(40_000); tier == contracts.ContextTierFull {
		t.Error("注入区变大后历史区份额应让位（26k < 30k → 必须降级），但阶段仍是 full")
	}
}
