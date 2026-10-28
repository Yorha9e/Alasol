package loop_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/loop"
)

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

// fakeOnlineSource 是脚本化在线表数据源：恒返回预设行（K11 的 gate 过滤 / subagent 排除在
// 真实数据源侧做，这里只考 loop 的渲染与注入）。
type fakeOnlineSource struct {
	rows  []loop.OnlineTableRow
	calls int
}

func (s *fakeOnlineSource) OnlineRows(_ context.Context, _ contracts.Ulid) []loop.OnlineTableRow {
	s.calls++
	return s.rows
}

func intPtr(v int) *int         { return &v }
func strPtrOT(v string) *string { return &v }

// lastMsg 取最后一次 provider 请求的**最后一条消息**（在线表注入位置的断言对象）。
func lastMsg(t *testing.T, reqs []core.ModelRequest) core.Message {
	t.Helper()
	if len(reqs) == 0 {
		t.Fatal("没有 provider 请求")
	}
	ms := reqs[len(reqs)-1].Messages
	if len(ms) == 0 {
		t.Fatal("最后一次请求没有消息")
	}
	return ms[len(ms)-1]
}

// sampleRows 造 self + 两个 peer 的在线表行（full id，供 shortID 截取）。
func sampleRows(session, agent contracts.Ulid) []loop.OnlineTableRow {
	p1, p2 := core.NewUlid(), core.NewUlid()
	return []loop.OnlineTableRow{
		{Self: true, SessionID: string(session), MainAgentID: string(agent), Gates: []string{"repo:X"}, LastKind: "model.turn", LastSeq: 9, UpdatedAt: "2026-10-28T00:00:00Z"},
		{SessionID: string(p1), MainAgentID: string(core.NewUlid()), Gates: []string{"repo:X"}, LastKind: "tool.call.intent", LastSeq: 7, UpdatedAt: "2026-10-28T00:00:01Z"},
		{SessionID: string(p2), MainAgentID: string(core.NewUlid()), Gates: []string{"repo:X", "team:backend"}, LastKind: "agent.completed", LastSeq: 5, UpdatedAt: "2026-10-28T00:00:02Z"},
	}
}

// ---------------------------------------------------------------------------
// 默认值 / 纯渲染
// ---------------------------------------------------------------------------

// TestOnlineTableDefaultValues：K15 默认 N=50、每行 ≤100 token、summary。
func TestOnlineTableDefaultValues(t *testing.T) {
	v := loop.NewOnlineTableConfig().View()
	if v.MaxRows != 50 {
		t.Fatalf("默认 N = %d，want 50", v.MaxRows)
	}
	if v.LineTokenBudget != 100 {
		t.Fatalf("默认每行预算 = %d，want 100 token", v.LineTokenBudget)
	}
	if v.Level != loop.LevelSummary {
		t.Fatalf("默认暴露等级 = %q，want summary", v.Level)
	}
	if v.MaxRows != loop.DefaultOnlineTableRows || v.LineTokenBudget != loop.DefaultLineTokenBudget {
		t.Fatal("默认值应与导出常量一致")
	}
}

// TestOnlineTableUpdateValidates：运行期更新的校验（非法 → 原配置不动 + error；
// 三个数值参数统一"0 = 用默认、负数 = 拒、超上界 = 拒"）。
func TestOnlineTableUpdateValidates(t *testing.T) {
	cfg := loop.NewOnlineTableConfig()
	bad := []loop.OnlineTableUpdate{
		{MaxRows: intPtr(-1)},
		{LineTokenBudget: intPtr(-1)},
		{WindowLines: intPtr(-3)},
		{MaxRows: intPtr(loop.MaxOnlineTableRows + 1)},
		{LineTokenBudget: intPtr(loop.MaxLineTokenBudget + 1)},
		{WindowLines: intPtr(loop.MaxWindowLines + 1)},
		{Level: strPtrOT("bogus")},
	}
	for i, u := range bad {
		if err := cfg.Update(u); err == nil {
			t.Fatalf("第 %d 个非法更新应被拒: %+v", i, u)
		}
	}
	// 原配置没被改动。
	if v := cfg.View(); v.MaxRows != 50 || v.Level != "summary" {
		t.Fatalf("非法更新后配置被改动: %+v", v)
	}
	// 合法部分更新：只动给定字段。
	if err := cfg.Update(loop.OnlineTableUpdate{MaxRows: intPtr(3)}); err != nil {
		t.Fatalf("合法更新被拒: %v", err)
	}
	if v := cfg.View(); v.MaxRows != 3 || v.Level != "summary" || v.LineTokenBudget != 100 {
		t.Fatalf("部分更新应只动 max_rows: %+v", v)
	}
	// 三个数值参数的 0 → 统一解析回默认（"0 = 用默认"口径）。
	if err := cfg.Update(loop.OnlineTableUpdate{MaxRows: intPtr(0), LineTokenBudget: intPtr(0), WindowLines: intPtr(0)}); err != nil {
		t.Fatalf("三个 0 都应被接受: %v", err)
	}
	if v := cfg.View(); v.MaxRows != 50 || v.LineTokenBudget != 100 || v.WindowLines != 5 {
		t.Fatalf("0 应解析回默认（50/100/5），得到 %+v", v)
	}
}

// TestRenderOnlineTableLineBudget：summary 等级每行 ≤100 token（估算 len/4，J21/K15）。
func TestRenderOnlineTableLineBudget(t *testing.T) {
	long := strings.Repeat("x", 4000)
	rows := []loop.OnlineTableRow{
		{Self: true, SessionID: string(core.NewUlid()), MainAgentID: string(core.NewUlid()), Gates: []string{"repo:" + long}, LastKind: long, UpdatedAt: long},
		{SessionID: string(core.NewUlid()), MainAgentID: string(core.NewUlid()), Gates: []string{"repo:Y"}, LastKind: long},
	}
	text, n := loop.RenderOnlineTable(rows, loop.NewOnlineTableConfig().View())
	if n != 2 {
		t.Fatalf("应渲染 2 行，得到 %d", n)
	}
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "- ") {
			continue // 头部 / 续行 / 空行不参与"每行"预算
		}
		if tok := (len(line) + 3) / 4; tok > 100 {
			t.Fatalf("summary 行估算 %d token 超 100（len=%d）: %q", tok, len(line), line)
		}
	}
}

// TestRenderOnlineTableLevelsGrowLength：暴露等级越高行内容越长（K14 summary<n<full）。
func TestRenderOnlineTableLevelsGrowLength(t *testing.T) {
	session, agent := core.NewUlid(), core.NewUlid()
	rows := []loop.OnlineTableRow{
		{Self: true, SessionID: string(session), MainAgentID: string(agent), Gates: []string{"repo:X", "team:backend"}, LastKind: "tool.call.intent", LastSeq: 42, UpdatedAt: "2026-10-28T00:00:00Z"},
	}
	lenOf := func(level string, win int) int {
		v := loop.OnlineTableView{MaxRows: 50, LineTokenBudget: 100, Level: level, WindowLines: win}
		text, _ := loop.RenderOnlineTable(rows, v)
		return len(text)
	}
	s := lenOf(loop.LevelSummary, 5)
	n := lenOf(loop.LevelN, 5)
	f := lenOf(loop.LevelFull, 5)
	if !(s < n && n < f) {
		t.Fatalf("行内容应随等级变长：summary=%d < n=%d < full=%d", s, n, f)
	}
	// full 用全 id（26 字符）——应含完整 session id，而不只是前 8 字符。
	full, _ := loop.RenderOnlineTable(rows, loop.OnlineTableView{Level: loop.LevelFull})
	if !strings.Contains(full, string(session)) {
		t.Fatal("full 应含完整 session id")
	}
	sum, _ := loop.RenderOnlineTable(rows, loop.OnlineTableView{Level: loop.LevelSummary})
	if strings.Contains(sum, string(session)) {
		t.Fatal("summary 只用短 id，不该含完整 session id")
	}
}

// TestRenderOnlineTableRowTruncation：N 截断（K15：行数上限）。
func TestRenderOnlineTableRowTruncation(t *testing.T) {
	rows := make([]loop.OnlineTableRow, 10)
	for i := range rows {
		rows[i] = loop.OnlineTableRow{SessionID: string(core.NewUlid()), MainAgentID: string(core.NewUlid()), Gates: []string{"repo:X"}}
	}
	text, n := loop.RenderOnlineTable(rows, loop.OnlineTableView{MaxRows: 3, LineTokenBudget: 100, Level: loop.LevelSummary, WindowLines: 5})
	if n != 3 {
		t.Fatalf("N=3 应渲染 3 行，得到 %d", n)
	}
	if !strings.Contains(text, "略去 7 行") {
		t.Fatalf("应提示略去 7 行: %q", text)
	}
}

// ---------------------------------------------------------------------------
// 注入位置 / 快照 / 运行期调整 / subagent 不注入
// ---------------------------------------------------------------------------

// TestOnlineTableInjectedAtPromptTail：主 agent 的 provider 请求**末尾**是在线表（Q4/Q3），
// 且多轮（含工具调用轮）后仍在末尾。
func TestOnlineTableInjectedAtPromptTail(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	src := &fakeOnlineSource{rows: sampleRows(h.session, h.agent)}
	// 两轮：第一轮调 echo，第二轮收尾——验证工具消息之后在线表仍在最末尾。
	h.script(stepTools(call("c1", "echo", `{"text":"hi"}`)), stepText("完成"))
	lp := h.loop(func(c *loop.Config) {
		c.OnlineTable = loop.NewOnlineTableConfig()
		c.OnlineTableSource = src
	})
	if _, err := lp.Run(context.Background(), "帮我 echo"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	reqs := h.provider.reqs()
	if len(reqs) != 2 {
		t.Fatalf("应 2 次模型调用，得到 %d", len(reqs))
	}
	// 第二轮末尾：user / assistant(toolcall) / tool(result) / **online_table**。
	tail := lastMsg(t, reqs)
	if !strings.Contains(tail.Content, "[在线表") {
		t.Fatalf("最后一段应是在线表（含 [在线表 标记）: %q", tail.Content)
	}
	if !strings.Contains(tail.Content, "暴露等级=summary") {
		t.Fatalf("在线表应带暴露等级: %q", tail.Content)
	}
	// self + 两个 peer 都应在表里（短 id 取 id 随机尾段 8 字符——按真实渲染口径断言）。
	shortOf := func(id string) string {
		if len(id) <= 8 {
			return id
		}
		return id[len(id)-8:]
	}
	for _, want := range []string{shortOf(string(h.session)), shortOf(src.rows[1].SessionID)} {
		if !strings.Contains(tail.Content, want) {
			t.Fatalf("在线表应含 %q: %q", want, tail.Content)
		}
	}
	// 第一轮的 history 里**没有**在线表（它只进请求、不进持久历史）：user prompt 是第一条。
	if reqs[0].Messages[0].Role != "user" || reqs[0].Messages[0].Content != "帮我 echo" {
		t.Fatalf("第一条消息应是原始 user prompt: %+v", reqs[0].Messages[0])
	}
}

// TestOnlineTablePartInSnapshot：context.assembled 落一个 online_table part
// （kind=injected、tokens 非零、entries=行数、排在最后——Q3）。
func TestOnlineTablePartInSnapshot(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	rows := sampleRows(h.session, h.agent)
	src := &fakeOnlineSource{rows: rows}
	h.script(stepText("ok"))
	lp := h.loop(func(c *loop.Config) {
		c.OnlineTable = loop.NewOnlineTableConfig()
		c.OnlineTableSource = src
	})
	if _, err := lp.Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	snap := decodeSnapshot(t, contextEvents(h.events())[0])
	var ot *contracts.ContextPart
	for i := range snap.Parts {
		if snap.Parts[i].Source == "online_table" {
			ot = &snap.Parts[i]
		}
	}
	if ot == nil {
		t.Fatalf("快照应含 online_table part: %+v", snap.Parts)
	}
	if ot.Kind != contracts.PartKindInjected {
		t.Fatalf("online_table kind = %q，want injected", ot.Kind)
	}
	if ot.Tokens <= 0 {
		t.Fatalf("online_table tokens 应非零，得到 %d", ot.Tokens)
	}
	if ot.Entries != int64(len(rows)) {
		t.Fatalf("online_table entries = %d，want 行数 %d", ot.Entries, len(rows))
	}
	// 排在最后（Q3：最变动在后）。
	if snap.Parts[len(snap.Parts)-1].Source != "online_table" {
		t.Fatalf("online_table 应排在最后: %+v", snap.Parts)
	}
	// tokens_used 仍等于各 part 之和（history 不含在线表，无双记）。
	var sum int64
	for _, p := range snap.Parts {
		sum += p.Tokens
	}
	if snap.TokensUsed != sum {
		t.Fatalf("tokens_used = %d，want parts 之和 %d", snap.TokensUsed, sum)
	}
	// 防双记硬断言：history part 只算持久 history（本轮恰 1 条 user prompt）——在线表只进请求
	// 不进持久历史，若有人去掉 context.go 的"排除末条"逻辑，这里必须红。
	var hist *contracts.ContextPart
	for i := range snap.Parts {
		if snap.Parts[i].Source == "history" {
			hist = &snap.Parts[i]
		}
	}
	if hist == nil {
		t.Fatalf("快照应含 history part: %+v", snap.Parts)
	}
	if hist.Entries != 1 {
		t.Fatalf("history entries = %d，want 1（在线表消息不入持久历史，不得双记）", hist.Entries)
	}
	if ot.Tokens >= snap.TokensUsed {
		t.Fatalf("online_table tokens=%d 应小于 tokens_used=%d（分账）", ot.Tokens, snap.TokensUsed)
	}
}

// TestOnlineTableRuntimeUpdateReflectedNextTurn：运行期改配置 → 同一 loop 下一轮立即反映
// （N 截断 + 暴露等级），**不重启**（K15）。
func TestOnlineTableRuntimeUpdateReflectedNextTurn(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	src := &fakeOnlineSource{rows: sampleRows(h.session, h.agent)}
	cfg := loop.NewOnlineTableConfig()
	lp := h.loop(func(c *loop.Config) {
		c.OnlineTable = cfg
		c.OnlineTableSource = src
	})

	// run 1：默认 N=50 → 3 行全显，summary。
	h.script(stepText("ok"))
	if _, err := lp.Run(context.Background(), "p1"); err != nil {
		t.Fatalf("Run #1: %v", err)
	}
	tail1 := lastMsg(t, h.provider.reqs()).Content
	if !strings.Contains(tail1, "暴露等级=summary") || strings.Contains(tail1, "略去") {
		t.Fatalf("run1 应 summary 且不截断: %q", tail1)
	}

	// 运行期改：N=1 + full（不重启、不新建 loop）。
	if err := cfg.Update(loop.OnlineTableUpdate{MaxRows: intPtr(1), Level: strPtrOT(loop.LevelFull)}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// run 2：同一 loop，下一轮反映新参数。
	h.script(stepText("ok"))
	if _, err := lp.Run(context.Background(), "p2"); err != nil {
		t.Fatalf("Run #2: %v", err)
	}
	tail2 := lastMsg(t, h.provider.reqs()).Content
	if !strings.Contains(tail2, "暴露等级=full") {
		t.Fatalf("run2 应反映 level=full: %q", tail2)
	}
	if !strings.Contains(tail2, "略去 2 行") {
		t.Fatalf("run2 应反映 N=1（3 行截到 1，略去 2）: %q", tail2)
	}
}

// TestOnlineTableNotInjectedForSubagent：只有**主 agent**注入在线表（K11）；
// subagent（depth>1）即使配了 config/source 也不注入、不查询数据源。
func TestOnlineTableNotInjectedForSubagent(t *testing.T) {
	rootType := agents.AgentType{ID: "root", CanSpawn: []string{"scout"}, Model: maRootModel, PermissionRequest: permFull()}
	h := newMAHarness(t, rootType, 3)
	h.registerType(agents.AgentType{ID: "scout", CanSpawn: nil, Tools: []string{"read"}, Model: "scout-model", PermissionRequest: permRO()})

	child, err := h.tree.Spawn(h.rootID, "scout", "recon", core.NewUlid())
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	h.prov.script("scout-model", stepText("scout done"))
	src := &fakeOnlineSource{rows: []loop.OnlineTableRow{{Self: true, SessionID: string(h.session), MainAgentID: string(child.ID), Gates: []string{"repo:X"}}}}
	lp, err := loop.New(loop.Config{
		Provider:  h.prov,
		Bus:       h.bus,
		Registry:  h.reg,
		Policy:    h.engine,
		SessionID: h.session,
		AgentID:   child.ID, // subagent
		Model:     "scout-model",
		System:    "scout-system",
		Agents:    &loop.Agents{Tree: h.tree},
		// 即使把在线表配置给 subagent……
		OnlineTable:       loop.NewOnlineTableConfig(),
		OnlineTableSource: src,
	})
	if err != nil {
		t.Fatalf("loop.New: %v", err)
	}
	if _, err := lp.Run(context.Background(), "scout task"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if src.calls != 0 {
		t.Fatalf("subagent 不该查询在线表数据源（calls=%d）", src.calls)
	}
	tail := lastMsg(t, h.prov.reqsFor("scout-model"))
	if strings.Contains(tail.Content, "[在线表") {
		t.Fatalf("subagent 不该注入在线表: %q", tail.Content)
	}
}

// TestOnlineTableDisabledByDefault：未配 source/config → 不注入、快照无 online_table part
// （默认关闭，老装配与单测不受影响）。
func TestOnlineTableDisabledByDefault(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)
	h.script(stepText("ok"))
	if _, err := h.loop(nil).Run(context.Background(), "p"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tail := lastMsg(t, h.provider.reqs())
	if strings.Contains(tail.Content, "[在线表") {
		t.Fatalf("未配在线表却注入了: %q", tail.Content)
	}
	snap := decodeSnapshot(t, contextEvents(h.events())[0])
	for _, p := range snap.Parts {
		if p.Source == "online_table" {
			t.Fatal("未配在线表却不该有 online_table part")
		}
	}
}
