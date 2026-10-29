package indexer_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// modelTurnEv 造一条 model.turn 事件，携带 usage（in/out token）与 model 名。
// usage=nil 时造"无 usage"的轮（provider 不上报）。
func modelTurnEv(session contracts.Ulid, model string, in, out int64) contracts.Event {
	p := contracts.ModelTurnPayload{Text: "..."}
	if model != "" {
		p.Model = &model
	}
	if in >= 0 && out >= 0 {
		p.Usage = &contracts.ModelTurnUsage{InputTokens: in, OutputTokens: out}
	}
	b, _ := json.Marshal(p)
	return ev(core.NewUlid(), "model.turn", session, string(b))
}

// modelTurnNoUsageEv 造一条明确无 usage 的 model.turn（in/out 传 -1 表示不带 usage）。
func modelTurnNoUsageEv(session contracts.Ulid) contracts.Event {
	return modelTurnEv(session, "gpt-4o-mini", -1, -1)
}

// TestTokenCostAggregation 钉死 J21：喂带 usage 的 model.turn → 查询返回正确聚合
// （input/output 总量、metered turn 数、按模型分组）。
func TestTokenCostAggregation(t *testing.T) {
	sid := contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")
	events := []contracts.Event{
		modelTurnEv(sid, "gpt-4o-mini", 100, 20),
		modelTurnEv(sid, "gpt-4o-mini", 50, 10),
		modelTurnEv(sid, "gpt-4o", 200, 40),
	}
	_, ix := setup(t, "memory", 0, events)
	drain(t, ix)

	ts, ok, err := ix.ReadTokenStats(context.Background(), sid)
	if err != nil || !ok {
		t.Fatalf("ReadTokenStats: ok=%v err=%v", ok, err)
	}
	// 总量：input = 100+50+200 = 350；output = 20+10+40 = 70。
	if ts.InputTokens != 350 {
		t.Fatalf("InputTokens = %d，want 350", ts.InputTokens)
	}
	if ts.OutputTokens != 70 {
		t.Fatalf("OutputTokens = %d，want 70", ts.OutputTokens)
	}
	if ts.ModelTurns != 3 {
		t.Fatalf("ModelTurns = %d，want 3", ts.ModelTurns)
	}
	// 按模型分组。
	if got := ts.ByModel["gpt-4o-mini"]; got.InputTokens != 150 || got.OutputTokens != 30 || got.Turns != 2 {
		t.Fatalf("gpt-4o-mini 分组错: %+v", got)
	}
	if got := ts.ByModel["gpt-4o"]; got.InputTokens != 200 || got.OutputTokens != 40 || got.Turns != 1 {
		t.Fatalf("gpt-4o 分组错: %+v", got)
	}
}

// TestTokenCostNoUsageNotCounted：无 usage 的 model.turn 不计 token（但仍是一轮，事件计数覆盖）。
func TestTokenCostNoUsageNotCounted(t *testing.T) {
	sid := contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")
	events := []contracts.Event{
		modelTurnNoUsageEv(sid),                  // 无 usage：不计
		modelTurnEv(sid, "gpt-4o-mini", 100, 20), // 计入
	}
	_, ix := setup(t, "memory", 0, events)
	drain(t, ix)

	ts, ok, err := ix.ReadTokenStats(context.Background(), sid)
	if err != nil || !ok {
		t.Fatalf("ReadTokenStats: ok=%v err=%v", ok, err)
	}
	if ts.InputTokens != 100 || ts.OutputTokens != 20 {
		t.Fatalf("无 usage 轮不应计 token：got in=%d out=%d", ts.InputTokens, ts.OutputTokens)
	}
	if ts.ModelTurns != 1 {
		t.Fatalf("metered turn 应只数带 usage 的 1 轮：got %d", ts.ModelTurns)
	}
}

// TestTokenCostReopenPersists：成本统计落盘可查（关库重开 → 续跑/重建 → 同一聚合，G24）。
func TestTokenCostReopenPersists(t *testing.T) {
	sid := contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")
	path := t.TempDir() + "/tokencost.db"
	ctx := context.Background()
	events := []contracts.Event{
		modelTurnEv(sid, "gpt-4o-mini", 100, 20),
		modelTurnEv(sid, "gpt-4o-mini", 30, 5),
	}

	st, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := st.Append(ctx, events); err != nil {
		t.Fatalf("append: %v", err)
	}
	ix, err := sqlite.NewIndexer(st, 0)
	if err != nil {
		t.Fatalf("new indexer: %v", err)
	}
	drain(t, ix)
	before, _, _ := ix.ReadTokenStats(ctx, sid)
	if before.InputTokens != 130 || before.OutputTokens != 25 {
		t.Fatalf("重建前聚合错: %+v", before)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重开（新索引器指向同一文件库），Rebuild 后应得同一聚合。
	st2, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	ix2, err := sqlite.NewIndexer(st2, 0)
	if err != nil {
		t.Fatalf("new indexer 2: %v", err)
	}
	if err := ix2.Rebuild(ctx); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	after, ok, err := ix2.ReadTokenStats(ctx, sid)
	if err != nil || !ok {
		t.Fatalf("重开后 ReadTokenStats: ok=%v err=%v", ok, err)
	}
	if after.InputTokens != before.InputTokens || after.OutputTokens != before.OutputTokens {
		t.Fatalf("重开聚合不一致: before=%+v after=%+v", before, after)
	}
	if got := after.ByModel["gpt-4o-mini"]; got.Turns != 2 || got.InputTokens != 130 {
		t.Fatalf("重开按模型分组错: %+v", got)
	}
}

// TestReadAllTokenStats 验证跨 session 聚合（/api/cost 总额用）。
func TestReadAllTokenStats(t *testing.T) {
	s1 := contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")
	s2 := contracts.Ulid("7ZZZZZZZZZZZZZZZZZZZZZZZZZ")
	events := []contracts.Event{
		modelTurnEv(s1, "gpt-4o-mini", 100, 20),
		modelTurnEv(s2, "gpt-4o-mini", 40, 8),
	}
	_, ix := setup(t, "memory", 0, events)
	drain(t, ix)

	all, err := ix.ReadAllTokenStats(context.Background())
	if err != nil {
		t.Fatalf("ReadAllTokenStats: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("应有 2 个 session 的成本，得到 %d", len(all))
	}
	var totIn, totOut int64
	for _, ts := range all {
		totIn += ts.InputTokens
		totOut += ts.OutputTokens
	}
	if totIn != 140 || totOut != 28 {
		t.Fatalf("总额聚合错: in=%d out=%d", totIn, totOut)
	}
}

// TestTokenCostBadPayloadIsolated：model.turn 载荷损坏 → 隔离不致命（P1-2），
// 其余事件照常投影。
func TestTokenCostBadPayloadIsolated(t *testing.T) {
	sid := contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")
	events := []contracts.Event{
		ev(core.NewUlid(), "model.turn", sid, "{ not json"), // 坏载荷：隔离
		modelTurnEv(sid, "gpt-4o-mini", 100, 20),            // 照常计入
	}
	_, ix := setup(t, "memory", 0, events)
	drain(t, ix)

	ts, ok, err := ix.ReadTokenStats(context.Background(), sid)
	if err != nil || !ok {
		t.Fatalf("ReadTokenStats: ok=%v err=%v", ok, err)
	}
	if ts.InputTokens != 100 {
		t.Fatalf("坏载荷被隔离后，好事件仍应计入：got in=%d", ts.InputTokens)
	}
}

// compactedEv 造一条 context.compacted 事件（v2-S1 压缩动作），带/不带压缩模型 usage。
func compactedEv(session contracts.Ulid, model string, in, out int64) contracts.Event {
	p := contracts.ContextCompactedPayload{
		Stage:        1,
		Action:       contracts.ContextCompactionActionDowngrade,
		Tier:         contracts.ContextTierL1,
		SeqFrom:      1,
		SeqTo:        20,
		Ref:          "stage:1@e:1..e:20",
		TokensBefore: 1000,
		TokensAfter:  100,
	}
	if model != "" {
		p.Model = &model
	}
	if in >= 0 && out >= 0 {
		p.Usage = &contracts.ModelTurnUsage{InputTokens: in, OutputTokens: out}
	}
	b, _ := json.Marshal(p)
	return ev(core.NewUlid(), "context.compacted", session, string(b))
}

// TestCompactionCostCounted：压缩模型调用的 usage 也要进成本统计（F16/J21）——
// 压缩也是一次模型调用，不计 = 成本统计系统性偏低。
func TestCompactionCostCounted(t *testing.T) {
	sid := contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")
	events := []contracts.Event{
		modelTurnEv(sid, "gpt-4o-mini", 100, 20),
		compactedEv(sid, "gpt-4o-mini", 300, 60),
	}
	_, ix := setup(t, "memory", 0, events)
	drain(t, ix)

	ts, ok, err := ix.ReadTokenStats(context.Background(), sid)
	if err != nil || !ok {
		t.Fatalf("ReadTokenStats: ok=%v err=%v", ok, err)
	}
	if ts.InputTokens != 400 || ts.OutputTokens != 80 {
		t.Fatalf("压缩调用 usage 未计入成本：in=%d out=%d（want 400/80）", ts.InputTokens, ts.OutputTokens)
	}
	if ts.ModelTurns != 2 {
		t.Fatalf("ModelTurns = %d，want 2（含一次压缩调用）", ts.ModelTurns)
	}
	if got := ts.ByModel["gpt-4o-mini"]; got.InputTokens != 400 || got.Turns != 2 {
		t.Fatalf("按模型分组错: %+v", got)
	}
}

// TestCompactionWithoutUsageNotCounted：没有 usage 的压缩事件不计 token（与 model.turn 同口径）。
func TestCompactionWithoutUsageNotCounted(t *testing.T) {
	sid := contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")
	events := []contracts.Event{compactedEv(sid, "", -1, -1)}
	_, ix := setup(t, "memory", 0, events)
	drain(t, ix)

	ts, ok, err := ix.ReadTokenStats(context.Background(), sid)
	if err != nil {
		t.Fatalf("ReadTokenStats: %v", err)
	}
	// 没有 usage → 不计 token（要么没有成本行，要么全 0）。
	if ok && (ts.InputTokens != 0 || ts.OutputTokens != 0 || ts.ModelTurns != 0) {
		t.Fatalf("无 usage 的压缩事件不应计 token: %+v", ts)
	}
}
