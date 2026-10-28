package host_test

// host 的 v1-S3 验收测试（K18 agentlink 五件套 + 端到端核验）。
//
// 硬验收（11 §2 v1-S3 行）：**Inquiry 拉原文可核验 summary 的 refs（端到端）**——
// 一个 session 用**真 subagent**（真 loop / 真工具 / 真事件库）跑出带 refs 的
// AgentSummary，另一个 session 经 agentlink.Inquiry 拉它 → 拿到结构化摘要 + 事件窗口 →
// 对 refs 逐条核验通过；反例：篡改/伪造的 refs 必须核验失败。
//
// 另覆盖：五件套经 HTTP 的端到端（Presence/Discover/Broadcast/Handoff/Inbox）与
// K11 投递负例（别 session 的 subagent 收不到、发送方收不到自己）。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/cap/local"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	"github.com/Yorha9e/alasol/runtime/tools"
)

// e2eGate 是本文件两个用例共用的显式 gate（不走自动 repo gate：那要 git 仓库，
// 而这里考的是 agentlink 本身，不该让结论依赖环境里有没有 git）。
const e2eGate = "e2e:agentlink"

// ---------------------------------------------------------------------------
// 真 subagent：跑一场"父派子、子带 refs 收敛"的回路
// ---------------------------------------------------------------------------

// subRunner 装配并跑一场含 subagent 的真回路（真 store / 真 bus / 真 read 工具）。
type subRunner struct {
	t       *testing.T
	store   *sqlite.Store
	bus     *bus.Bus
	session contracts.Ulid
	rootID  contracts.Ulid
}

// refScriptProvider 是脚本 provider：父/子靠 Model 分派。
//
// 子 agent 的第二轮（complete）用 refFn **动态**生成 refs——脚本跑到那一刻，事件流里已有
// 它自己发起的 read intent，所以 refs 指向的是**真实发生过的事**（这正是 K24 的用意：
// summary 带可验证引用，主 agent 用 Inquiry 拉原文核验）。
type refScriptProvider struct {
	mu     int // 调用计数（单线程：loop 逐轮调用）
	refFn  func() []string
	readAt string
}

func (p *refScriptProvider) Run(ctx context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu++
	call := p.mu
	switch {
	case req.Model == "scout-model" && call == 1:
		// 子 agent 第一轮：发起一次真 read（会在事件流里留下带路径的 intent）。
		args, _ := json.Marshal(map[string]string{"path": p.readAt})
		tc := core.ToolCallReq{ID: contracts.ProviderId("toolu_SCOUT_READ"), Name: "read", Args: args}
		emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 12, OutputTokens: 4}}, nil
	case req.Model == "scout-model":
		// 子 agent 第二轮：complete(summary) —— refs 现算（文件路径 + 事件 seq）。
		refs := p.refFn()
		args, _ := json.Marshal(map[string]any{"text": "README 三段：架构/契约/施工顺序", "refs": refs, "taint": "clean"})
		tc := core.ToolCallReq{ID: contracts.ProviderId("toolu_SCOUT_DONE"), Name: "complete", Args: args}
		emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 20, OutputTokens: 8}}, nil
	case call == 1:
		// 父 agent 第一轮：spawn 一个 scout。
		args, _ := json.Marshal(map[string]string{
			"agent_type": "scout", "role": "recon", "task": "读 README 并总结",
			"request_id": string(core.NewUlid()),
		})
		tc := core.ToolCallReq{ID: contracts.ProviderId("toolu_ROOT_SPAWN"), Name: "spawn", Args: args}
		emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 10, OutputTokens: 5}}, nil
	default:
		// 父 agent 收尾。
		emit(core.ProviderEvent{Kind: "text.delta", Text: "子 agent 汇报完，收尾"})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{Text: "子 agent 汇报完，收尾", Usage: core.Usage{InputTokens: 30, OutputTokens: 6}}, nil
	}
}

// runSubagentSession 跑一场真 subagent 回路，返回 session id 与主 agent id。
//
// 事件库是**文件库**（调用方随后用同一路径开 hub → 两个"进程"共享唯一事实流，G3）。
func runSubagentSession(t *testing.T, dbPath, readPath string) (contracts.Ulid, contracts.Ulid) {
	t.Helper()
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	b := bus.New(st)
	// 生命周期工具（spawn/complete）也要过策略（P2-4），这里显式放行——考的是多 agent 流程。
	eng := policy.New(
		allowTool(loop.ToolSpawn), allowTool(loop.ToolComplete), allowTool("read"),
	)
	reg := registry.New(registry.NewPolicyAdjudicator(eng))
	for _, d := range tools.Descriptors() {
		var ex core.ToolExecutor
		switch d.Name {
		case "read":
			ex = tools.NewRead(local.FS{})
		default:
			continue // 本用例只放行 read
		}
		if err := reg.RegisterTool(d, ex); err != nil {
			t.Fatalf("RegisterTool(%s): %v", d.Name, err)
		}
	}

	types := agents.NewTypeRegistry()
	if err := types.Register(agents.AgentType{
		ID: "scout", Tools: []string{"read"}, Model: "scout-model", CanSpawn: nil,
		PermissionRequest: permRO(),
	}); err != nil {
		t.Fatalf("Register(scout): %v", err)
	}
	rootType := agents.AgentType{
		ID: "root", CanSpawn: []string{"scout"}, Model: "root-model",
		PermissionRequest: permFull(),
	}
	tree := agents.NewTree(rootType, agents.WithTypes(types), agents.WithMaxDepth(3))
	session := core.NewUlid()

	prov := &refScriptProvider{
		refFn: func() []string {
			// 现算 refs：事件流当前最后一条的 seq（那就是子 agent 刚发起的 read 的结果事件）。
			evs, rerr := st.ReadFrom(context.Background(), nil, 1, 0)
			if rerr != nil || len(evs) == 0 {
				t.Fatalf("读事件流算 refs: %v", rerr)
			}
			last := evs[len(evs)-1]
			if last.Seq == nil {
				t.Fatalf("最后一条事件没有 seq")
			}
			return []string{readPath, fmt.Sprintf("e:%d", *last.Seq)}
		},
		readAt: readPath,
	}
	lp, err := loop.New(loop.Config{
		Provider:  prov,
		Bus:       b,
		Registry:  reg,
		Policy:    eng,
		Approver:  core.AlwaysAllow{},
		SessionID: session,
		AgentID:   tree.Root(),
		Model:     "root-model",
		System:    "e2e-root",
		MaxTurns:  8,
		Agents:    &loop.Agents{Tree: tree},
	})
	if err != nil {
		t.Fatalf("loop.New: %v", err)
	}
	res, err := lp.Run(context.Background(), "派个 scout 读 README 并总结")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "子 agent 汇报完，收尾" {
		t.Fatalf("父 agent 应收敛到自己的文本: %q", res.Text)
	}
	return session, tree.Root()
}

// allowTool / permFull / permRO 是本文件的夹具助手（与 core/loop 测试同口径）。
func allowTool(name string) contracts.PolicyRule {
	return contracts.PolicyRule{Effect: contracts.EffectAllow, Tool: name, Source: contracts.RuleSourceCli}
}

func permFull() *contracts.PermissionRequest {
	return &contracts.PermissionRequest{
		Read: contracts.ReadWriteLevelFull, Write: contracts.ReadWriteLevelFull, Exec: contracts.ExecLevelFull,
	}
}

// permRO 是只读权限（write/exec = none）：scout 只该读。
func permRO() *contracts.PermissionRequest {
	return &contracts.PermissionRequest{
		Read: contracts.ReadWriteLevelWorkspace, Write: contracts.ReadWriteLevelNone, Exec: contracts.ExecLevelNone,
	}
}

// lastSeqOf 返回事件流当前的最后一条 seq（测试断言用）。
func lastSeqOf(t *testing.T, st *sqlite.Store) int64 {
	t.Helper()
	evs, err := st.ReadFrom(context.Background(), nil, 1, 0)
	if err != nil || len(evs) == 0 {
		t.Fatalf("读事件流: %v", err)
	}
	return *evs[len(evs)-1].Seq
}

// ---------------------------------------------------------------------------
// 硬验收：Inquiry 拉原文可核验 summary 的 refs（端到端，含反例）
// ---------------------------------------------------------------------------

func TestAgentLinkInquiryVerifiesRealSubagentSummaryRefs(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "events.db")
	readPath := filepath.Join(dir, "README.md")
	if err := os.WriteFile(readPath, []byte("# alasol\n架构 / 契约 / 施工顺序\n"), 0o600); err != nil {
		t.Fatalf("写 README: %v", err)
	}

	// ① session A：真 subagent 跑出带 refs 的 AgentSummary。
	sidA, mainA := runSubagentSession(t, dbPath, readPath)

	// A 显式加入 e2e gate（hub 的 loop 工厂不挂 Agents 树——subagent 是测试直装 loop 跑的，
	// 所以这条 gate.joined 也由测试按宿主持口径落：envelope.agent_id = A 的主 agent）。
	stA, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open(A): %v", err)
	}
	joinA, err := json.Marshal(contracts.GateJoinedPayload{GateID: e2eGate, Source: contracts.GateJoinSourceExplicit})
	if err != nil {
		t.Fatalf("marshal gate.joined: %v", err)
	}
	if _, err := stA.Append(context.Background(), []contracts.Event{{
		V: 1, ID: core.NewUlid(), Kind: "gate.joined",
		TS: time.Now().UTC().Format(time.RFC3339), SessionID: sidA, AgentID: mainA, Durable: true, Payload: joinA,
	}}); err != nil {
		t.Fatalf("append gate.joined(A): %v", err)
	}
	_ = stA.Close()

	// ② 同一事件库上开 hub（= 另一个宿主进程），session B 由它托管并加入同一 gate。
	h, err := host.New(host.Config{
		DBPath:   dbPath,
		Model:    "scripted",
		System:   "e2e-b",
		MaxTurns: 4,
		Allow:    []string{"read"},
		Provider: &textOnlyProvider{text: "B 在线"},
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	sidB := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sidB, "B 打个招呼"); err != nil {
		t.Fatalf("RunPrompt(B): %v", err)
	}
	mainB, ok := h.Discovery().PrimaryAgent(sidB)
	if !ok {
		t.Fatalf("B 没有主 agent")
	}

	// A 的 loop 由测试直装（不在 hub 的 loops 表里）→ 按宿主持口吻 Activate：这正是
	// "A 由本进程持有"在发现表里的样子（G17b：活状态是进程本地的，要宿主显式激活）。
	// 必须在 JoinGate 之前：VisiblePeers 要求对端**存活**（K12/G17b 成员关系是活的）。
	h.Discovery().Activate(sidA)

	peers, err := h.JoinGate(context.Background(), sidB, e2eGate)
	if err != nil {
		t.Fatalf("JoinGate(B): %v", err)
	}
	if len(peers.Peers) != 1 || peers.Peers[0].SessionID != string(sidA) {
		t.Fatalf("B join 后应看到 A（gate 相交）: %+v", peers.Peers)
	}

	// ③ B 的主 agent 经 Inquiry 拉 A：结构化摘要 + 事件窗口（Q6 两者都给）。
	res, err := h.Inquiry(context.Background(), agentlink.Identity{SessionID: sidB, AgentID: mainB},
		agentlink.InquiryRequest{SessionID: sidA})
	if err != nil {
		t.Fatalf("Inquiry(A ← B): %v", err)
	}
	if len(res.Summaries) != 1 {
		t.Fatalf("Summaries = %d 条，want 1（scout 的 agent.completed）: %+v", len(res.Summaries), res.Summaries)
	}
	sum := res.Summaries[0].Summary
	if sum.Text != "README 三段：架构/契约/施工顺序" {
		t.Fatalf("summary 文本 = %q", sum.Text)
	}
	if len(sum.Refs) != 2 {
		t.Fatalf("summary refs = %v，want 2（文件路径 + 事件 seq）", sum.Refs)
	}
	if res.WindowRange.FromSeq == 0 || res.WindowRange.ToSeq == 0 || len(res.Window) < 8 {
		t.Fatalf("事件窗口不对劲: range=%+v len=%d", res.WindowRange, len(res.Window))
	}
	// 窗口里必须能翻到 scout 的 read intent 原文（K24：refs 的核验证据就在窗口里）。
	var sawReadIntent bool
	for _, ev := range res.Window {
		if ev.Kind == "tool.call.intent" && strings.Contains(string(ev.Payload), "README.md") {
			sawReadIntent = true
		}
	}
	if !sawReadIntent {
		t.Fatalf("窗口里应能翻到 scout 的 read intent 原文")
	}

	// ④ refs 逐条核验**通过**（默认取窗口内全部 summary 的 refs）。
	if len(res.RefChecks) != 2 {
		t.Fatalf("RefChecks = %+v，want 2 条", res.RefChecks)
	}
	for _, c := range res.RefChecks {
		if !c.OK {
			t.Fatalf("ref %q 应核验通过: %+v", c.Ref, c)
		}
	}
	if !res.RefsAllOK {
		t.Fatalf("RefsAllOK 应为 true")
	}

	// ⑤ 反例：篡改/伪造的 refs 必须核验失败。
	lastSeq := int64(0)
	for _, ev := range res.Window {
		if ev.Seq != nil && *ev.Seq > lastSeq {
			lastSeq = *ev.Seq
		}
	}
	forged := []string{
		fmt.Sprintf("e:%d", lastSeq+1000), // seq 不存在
		"e:1-99999",                       // 区间比窗口还长
		" nowhere/nope.txt",               // 从没被读过的文件
		filepath.Join(dir, "OTHER.md"),    // 同目录下另一个文件
		"../../etc/passwd",                // 路径穿越
	}
	bad, err := h.Inquiry(context.Background(), agentlink.Identity{SessionID: sidB, AgentID: mainB},
		agentlink.InquiryRequest{SessionID: sidA, Refs: forged})
	if err != nil {
		t.Fatalf("Inquiry(forged): %v", err)
	}
	if bad.RefsAllOK {
		t.Fatalf("伪造 refs 不该全部通过")
	}
	if len(bad.RefChecks) != len(forged) {
		t.Fatalf("RefChecks = %d，want %d", len(bad.RefChecks), len(forged))
	}
	for _, c := range bad.RefChecks {
		if c.OK {
			t.Fatalf("伪造/篡改的 ref %q 竟核验通过: %+v", c.Ref, c)
		}
		if c.Detail == "" {
			t.Fatalf("失败的 ref 应给出原因: %+v", c)
		}
	}

	// ⑥ 同一份 summary 的 refs 也能被**显式**传入核验（主 agent 拿到 summary 后自己拉）。
	again, err := h.Inquiry(context.Background(), agentlink.Identity{SessionID: sidB, AgentID: mainB},
		agentlink.InquiryRequest{SessionID: sidA, Refs: sum.Refs})
	if err != nil {
		t.Fatalf("Inquiry(显式 refs): %v", err)
	}
	if !again.RefsAllOK || len(again.RefChecks) != 2 {
		t.Fatalf("显式 refs 核验 = %+v", again.RefChecks)
	}

	// ⑦ K11 负例：别 session 的 subagent 拉不到 A。
	//    （B 的 subagent 不在树里——用 A 自己的 subagent 身份跨 session 查也不行：水平可见性
	//     只在主 agent 之间。这里用"伪造的别 session subagent 身份"验证拒绝对路径存在。）
	if _, err := h.Inquiry(context.Background(), agentlink.Identity{SessionID: sidB, AgentID: core.NewUlid()},
		agentlink.InquiryRequest{SessionID: sidA}); err == nil {
		t.Fatalf("B 的 subagent 不该能 Inquiry A（K11①：水平可见性只在主 agent 之间）")
	}
}

// textOnlyProvider 是只回文本的脚本 provider（session B 用）。
type textOnlyProvider struct{ text string }

func (p *textOnlyProvider) Run(_ context.Context, _ core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	emit(core.ProviderEvent{Kind: "text.delta", Text: p.text})
	emit(core.ProviderEvent{Kind: "done"})
	return core.ModelResult{Text: p.text, Usage: core.Usage{InputTokens: 8, OutputTokens: 2}}, nil
}

// ---------------------------------------------------------------------------
// 五件套经 HTTP 的端到端（协议绑定层）
// ---------------------------------------------------------------------------

func TestAgentLinkHTTPEndToEnd(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) { c.Provider = &textOnlyProvider{text: "在"} })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// 两个 session 各跑一轮 → 都有主 agent；再显式加入同一 gate（K11 水平可见性的前提）。
	sidA := host.NewSession()
	sidB := host.NewSession()
	for _, sid := range []contracts.Ulid{sidA, sidB} {
		if _, err := h.RunPrompt(context.Background(), sid, "在吗"); err != nil {
			t.Fatalf("RunPrompt(%s): %v", sid, err)
		}
		if _, err := h.JoinGate(context.Background(), sid, e2eGate); err != nil {
			t.Fatalf("JoinGate(%s): %v", sid, err)
		}
	}
	mainA, _ := h.Discovery().PrimaryAgent(sidA)
	mainB, _ := h.Discovery().PrimaryAgent(sidB)

	// Presence：A 看得到 B（主 agent），看不到自己。
	var pres agentlink.PresenceReport
	if code := getJSON(t, srv.URL+"/api/presence?session_id="+string(sidA), &pres); code != http.StatusOK {
		t.Fatalf("presence 状态码 = %d", code)
	}
	if !pres.Known || len(pres.Peers) != 1 || pres.Peers[0].SessionID != string(sidB) {
		t.Fatalf("Presence(A) = %+v", pres)
	}

	// Discover：按 gate 找活跃成员（两个都在）。session_id = **调用方**（F4：没有调用方
	// 身份就没有可见性过滤，任意客户端不得枚举任意 gate 的成员）。
	var disc agentlink.DiscoverResult
	if code := getJSON(t, srv.URL+"/api/discover?session_id="+string(sidA)+"&gate_id="+e2eGate, &disc); code != http.StatusOK {
		t.Fatalf("discover 状态码 = %d", code)
	}
	if len(disc.Sessions) != 2 {
		t.Fatalf("gate 成员 = %+v，want 2", disc.Sessions)
	}
	// 按 agent 查（含子树）。
	if code := getJSON(t, srv.URL+"/api/discover?session_id="+string(sidA)+"&agent_id="+string(mainA)+"&subtree=1", &disc); code != http.StatusOK {
		t.Fatalf("discover(agent) 状态码 = %d", code)
	}
	if len(disc.Sessions) != 1 || len(disc.Agents) != 1 || disc.Agents[0].AgentID != string(mainA) {
		t.Fatalf("discover(agent) = %+v", disc)
	}

	// Broadcast：A 向 gate 广播 → 收件方是 B 的主 agent（K11①）。
	var sent agentlink.MessageResult
	code := postJSON(t, srv.URL+"/api/agentlink/broadcast", map[string]any{
		"session_id": string(sidA), "gate_id": e2eGate, "text": "我改完了 README",
	}, &sent)
	if code != http.StatusOK {
		t.Fatalf("broadcast 状态码 = %d", code)
	}
	if sent.Kind != agentlink.KindBroadcast || len(sent.Recipients) != 1 ||
		sent.Recipients[0].SessionID != string(sidB) || sent.Recipients[0].AgentID != string(mainB) {
		t.Fatalf("broadcast 结果 = %+v", sent)
	}

	// Inbox：B 的主 agent 拉得到；B 的"别 session subagent"视角拉不到；A 自己拉不到。
	// 响应是 InboxResult：消息 + 游标水位（next_seq）+ has_more（F5：游标不再靠
	// "收到的最后一条消息"反推）。
	var inbox agentlink.InboxResult
	if code := getJSON(t, srv.URL+"/api/inbox?session_id="+string(sidB)+"&agent_id="+string(mainB), &inbox); code != http.StatusOK {
		t.Fatalf("inbox 状态码 = %d", code)
	}
	if len(inbox.Messages) != 1 || inbox.Messages[0].Text != "我改完了 README" {
		t.Fatalf("B 的 inbox = %+v", inbox)
	}
	if inbox.NextSeq == 0 {
		t.Fatalf("inbox 应回报扫描水位 next_seq: %+v", inbox)
	}
	if code := getJSON(t, srv.URL+"/api/inbox?session_id="+string(sidB)+"&agent_id="+string(core.NewUlid()), &inbox); code != http.StatusOK {
		t.Fatalf("inbox(sub) 状态码 = %d", code)
	}
	if len(inbox.Messages) != 0 {
		t.Fatalf("B 的 subagent 视角不该收到（K11①），得到 %+v", inbox)
	}
	if code := getJSON(t, srv.URL+"/api/inbox?session_id="+string(sidA), &inbox); code != http.StatusOK {
		t.Fatalf("inbox(self) 状态码 = %d", code)
	}
	if len(inbox.Messages) != 0 {
		t.Fatalf("发送方不该收到自己的广播，得到 %+v", inbox)
	}

	// Handoff：带结构化 payload（开放对象，框架不解释）。
	var ho agentlink.MessageResult
	code = postJSON(t, srv.URL+"/api/agentlink/handoff", map[string]any{
		"session_id": string(sidA), "gate_id": e2eGate, "text": "交接",
		"payload": map[string]any{"scope": "docs/", "next": []string{"写实现"}},
	}, &ho)
	if code != http.StatusOK {
		t.Fatalf("handoff 状态码 = %d", code)
	}
	if ho.Kind != agentlink.KindHandoff {
		t.Fatalf("handoff Kind = %q", ho.Kind)
	}
	if code := getJSON(t, srv.URL+"/api/inbox?session_id="+string(sidB), &inbox); code != http.StatusOK {
		t.Fatalf("inbox#2 状态码 = %d", code)
	}
	if len(inbox.Messages) != 2 {
		t.Fatalf("B 的 inbox 应有 2 条（广播 + 交接），得到 %d", len(inbox.Messages))
	}
	var lastPayload map[string]any
	if err := json.Unmarshal(inbox.Messages[1].Payload, &lastPayload); err != nil {
		t.Fatalf("handoff payload 解不开: %v", err)
	}
	if lastPayload["scope"] != "docs/" {
		t.Fatalf("handoff payload = %+v", lastPayload)
	}

	// Inquiry 经 HTTP：B 拉自己的 session（同 session 恒可见）。
	var inq agentlink.InquiryResult
	if code := getJSON(t, srv.URL+"/api/inquiry?session_id="+string(sidB)+"&limit=3", &inq); code != http.StatusOK {
		t.Fatalf("inquiry 状态码 = %d", code)
	}
	if len(inq.Window) != 3 || !inq.Truncated {
		t.Fatalf("inquiry 窗口 = %d 条 / Truncated=%v", len(inq.Window), inq.Truncated)
	}

	// 错误形状：坏 session_id → 400；不受托管的 session 发广播 → 404；坏目标 → 400。
	if code := getJSON(t, srv.URL+"/api/presence?session_id=not-a-ulid", nil); code != http.StatusBadRequest {
		t.Fatalf("坏 session_id 状态码 = %d，want 400", code)
	}
	if code := postJSON(t, srv.URL+"/api/agentlink/broadcast", map[string]any{
		"session_id": string(host.NewSession()), "gate_id": e2eGate, "text": "野 session",
	}, nil); code != http.StatusNotFound {
		t.Fatalf("不受托管 session 的广播状态码 = %d，want 404", code)
	}
	if code := postJSON(t, srv.URL+"/api/agentlink/broadcast", map[string]any{
		"session_id": string(sidA), "text": "没目标",
	}, nil); code != http.StatusBadRequest {
		t.Fatalf("无目标的广播状态码 = %d，want 400", code)
	}
}

// postJSON 发一个 POST（JSON 体）并解响应（out 可为 nil）。
func postJSON(t *testing.T, url string, body any, out any) int {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	resp, err := http.Post(url, "application/json", strings.NewReader(string(raw)))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("解 %s 的 JSON: %v", url, err)
		}
	}
	return resp.StatusCode
}

// TestAgentLinkMessagesSurviveHubReopen：消息历史与 gate 成员关系都能从事件流重建
// （K18 可重建 + G17b/K10：崩溃重启后老成员失活，join 历史由事件流重放即得）。
func TestAgentLinkMessagesSurviveHubReopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "events.db")
	cfg := host.Config{
		DBPath: dbPath, Model: "scripted", System: "reopen", MaxTurns: 4,
		Allow: []string{"read"}, Provider: &textOnlyProvider{text: "在"}, CWD: dir,
	}

	h1, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New#1: %v", err)
	}
	sidA, sidB := host.NewSession(), host.NewSession()
	for _, sid := range []contracts.Ulid{sidA, sidB} {
		if _, err := h1.RunPrompt(context.Background(), sid, "在吗"); err != nil {
			t.Fatalf("RunPrompt(%s): %v", sid, err)
		}
		if _, err := h1.JoinGate(context.Background(), sid, e2eGate); err != nil {
			t.Fatalf("JoinGate(%s): %v", sid, err)
		}
	}
	mainA, _ := h1.Discovery().PrimaryAgent(sidA)
	sent, err := h1.Broadcast(context.Background(),
		agentlink.Identity{SessionID: sidA, AgentID: mainA},
		agentlink.BroadcastRequest{Target: gateTarget(e2eGate), Text: "重启前发的广播"})
	if err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	// 关库前确认 B 看得到。
	before, err := h1.Inbox(context.Background(), agentlink.InboxQuery{SessionID: sidB})
	if err != nil || len(before.Messages) != 1 {
		t.Fatalf("重启前 B 的 inbox: err=%v msgs=%+v", err, before)
	}
	if err := h1.Close(); err != nil {
		t.Fatalf("Close#1: %v", err)
	}

	// 重开（同一事件库 = 另一个宿主进程）。
	h2, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New#2: %v", err)
	}
	t.Cleanup(func() { _ = h2.Close() })

	// join 历史从事件流重建（G17b/K10：可重建的部分回来，活状态不回来）。
	if gates := h2.Discovery().GatesOf(sidB); len(gates) != 1 || gates[0] != e2eGate {
		t.Fatalf("重开后 B 的 gate 成员关系 = %v，want [%s]", gates, e2eGate)
	}
	// 活状态不回来：此刻两个 session 都不在线 → B 收不到（成员关系是活的）。
	if got, err := h2.Inbox(context.Background(), agentlink.InboxQuery{SessionID: sidB}); err != nil || len(got.Messages) != 0 {
		t.Fatalf("重开后未重新托管的 B 不该收到（G17b）: err=%v msgs=%+v", err, got)
	}

	// B 重新托管（= resume：loop 建成即 Activate）→ 又是在线成员 → 广播历史还在。
	if _, err := h2.RunPrompt(context.Background(), sidB, "我回来了"); err != nil {
		t.Fatalf("RunPrompt#2: %v", err)
	}
	after, err := h2.Inbox(context.Background(), agentlink.InboxQuery{SessionID: sidB})
	if err != nil {
		t.Fatalf("Inbox#2: %v", err)
	}
	if len(after.Messages) != 1 || after.Messages[0].EventID != sent.EventID || after.Messages[0].Text != "重启前发的广播" {
		t.Fatalf("重开后 B 的 inbox = %+v，want 重启前那条（event_id=%s）", after, sent.EventID)
	}

	// A 也重新激活后，水平可见性恢复（K11①）。
	h2.Discovery().Activate(sidA)
	peers, err := h2.Peers(context.Background(), sidB)
	if err != nil {
		t.Fatalf("Peers#2: %v", err)
	}
	if len(peers.Peers) != 1 || peers.Peers[0].SessionID != string(sidA) {
		t.Fatalf("重开后 B 的可见对端 = %+v，want A", peers.Peers)
	}
}

// gateTarget 构造 gate 投递目标（契约类型；指针字段由助手补上）。
func gateTarget(gateID string) contracts.AgentLinkTarget {
	id := gateID
	return contracts.AgentLinkTarget{Type: contracts.AgentLinkTargetTypeGate, GateID: &id}
}

// ---------------------------------------------------------------------------
// 评审轮修复的回归用例（F4 / F6 / F7 / F9）
// ---------------------------------------------------------------------------

// TestF4AgentLinkHTTPGuardsCallerIdentity（F4）：协议面必须有**调用方身份**。
//
// 没有调用方身份 → 任意客户端可枚举任意 gate 的成员、任意 session 的 agent 树与
// gate 列表（跨 session 泄漏）。修法：查询面一律带调用方 session（受本 hub 托管），
// 且结果按 K11 裁剪。
func TestF4AgentLinkHTTPGuardsCallerIdentity(t *testing.T) {
	// 两个**不相交**的 session（各自的 git 仓库 → 各自的 repo gate → 互不可见）。
	root := t.TempDir()
	repoA := initGitRepo(t, filepath.Join(root, "a"))
	repoB := initGitRepo(t, filepath.Join(root, "b"))
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = repoA
		c.Provider = &textOnlyProvider{text: "在"}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sidA := runPromptIn(t, h, repoA, "A 的任务")
	sidB := runPromptIn(t, h, repoB, "B 的任务")
	if _, err := h.JoinGate(context.Background(), sidA, e2eGate); err != nil {
		t.Fatalf("JoinGate: %v", err)
	}
	mainA, _ := h.Discovery().PrimaryAgent(sidA)

	// ① discover 不带调用方 → 400（没有身份就没有过滤）。
	var disc agentlink.DiscoverResult
	if code := getJSON(t, srv.URL+"/api/discover?gate_id="+e2eGate, &disc); code != http.StatusBadRequest {
		t.Fatalf("无调用方的 discover 状态码 = %d，want 400", code)
	}
	// ② presence 查**不受托管**的 session → 404（与 /api/peers 同口径）。
	var pres agentlink.PresenceReport
	if code := getJSON(t, srv.URL+"/api/presence?session_id="+string(host.NewSession()), &pres); code != http.StatusNotFound {
		t.Fatalf("不受托管 session 的 presence 状态码 = %d，want 404", code)
	}
	// ③ discover 查**别的 session 的 agent** → 403（K11：别 session 的 subagent
	// 完全不可见；跨 session 只允许可见对端的主 agent）。
	mainB, _ := h.Discovery().PrimaryAgent(sidB)
	if code := getJSON(t, srv.URL+"/api/discover?session_id="+string(sidA)+"&agent_id="+string(mainB), &disc); code != http.StatusForbidden {
		t.Fatalf("跨 session agent 的 discover 状态码 = %d，want 403", code)
	}
	// ④ 查一个**自己没加入**的 gate → 403（不能枚举任意 gate 的成员）。
	// B 查 A 所在的那个 gate → A 不在 B 的 gate 里 → 403。
	if code := getJSON(t, srv.URL+"/api/discover?session_id="+string(sidB)+"&gate_id="+e2eGate, &disc); code != http.StatusForbidden {
		t.Fatalf("查不相交 gate 的 discover 状态码 = %d，want 403", code)
	}
	// ⑤ A 查自己的 agent 树 → 200（可见面内的正常查询不受影响）。
	if code := getJSON(t, srv.URL+"/api/discover?session_id="+string(sidA)+"&agent_id="+string(mainA)+"&subtree=1", &disc); code != http.StatusOK {
		t.Fatalf("自己树内的 discover 状态码 = %d，want 200", code)
	}
}

// TestF7AgentlinkErrorStatusMapping（F7）：K11 的拒绝与参数错误**不再**一律 500。
func TestF7AgentlinkErrorStatusMapping(t *testing.T) {
	// 两个**不相交**的 session（各自的 git 仓库 → 各自的 repo gate → 互不可见）。
	root := t.TempDir()
	repoA := initGitRepo(t, filepath.Join(root, "a"))
	repoB := initGitRepo(t, filepath.Join(root, "b"))
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = repoA
		c.Provider = &textOnlyProvider{text: "在"}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sidA := runPromptIn(t, h, repoA, "A 的任务")
	if _, err := h.JoinGate(context.Background(), sidA, e2eGate); err != nil {
		t.Fatalf("JoinGate: %v", err)
	}
	// ① 空正文 → 400（ErrEmptyText）。
	if code := postJSON(t, srv.URL+"/api/agentlink/broadcast", map[string]any{
		"session_id": string(sidA), "gate_id": e2eGate, "text": "   ",
	}, nil); code != http.StatusBadRequest {
		t.Fatalf("空正文的广播状态码 = %d，want 400", code)
	}
	// ② 跨 session 的 subtree 目标 → 400（ErrCrossSessionSubtree）。
	sidB := runPromptIn(t, h, repoB, "B 的任务")
	mainB, _ := h.Discovery().PrimaryAgent(sidB)
	if code := postJSON(t, srv.URL+"/api/agentlink/broadcast", map[string]any{
		"session_id": string(sidA), "subtree_agent_id": string(mainB), "text": "越界",
	}, nil); code != http.StatusBadRequest {
		t.Fatalf("跨 session subtree 的广播状态码 = %d，want 400", code)
	}
	// ③ 不受托管的 session → 404（ErrSessionNotHosted）。
	if code := postJSON(t, srv.URL+"/api/agentlink/broadcast", map[string]any{
		"session_id": string(host.NewSession()), "gate_id": e2eGate, "text": "野 session",
	}, nil); code != http.StatusNotFound {
		t.Fatalf("不受托管 session 的广播状态码 = %d，want 404", code)
	}
	// ④ 不可见的查询 → 403（ErrNotVisible）：A 的主 agent 查 B（不相交的 repo）。
	var inq agentlink.InquiryResult
	if code := getJSON(t, srv.URL+"/api/inquiry?session_id="+string(sidB)+"&as_session="+string(sidA), &inq); code != http.StatusForbidden {
		t.Fatalf("不可见 session 的 inquiry 状态码 = %d，want 403", code)
	}
}

// TestF6InquiryHTTPDefaultLimit（F6）：Inquiry 的 HTTP 默认给**有限** limit，
// 并用 truncated 显式报告"没取完"。
func TestF6InquiryHTTPDefaultLimit(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "events.db")
	h, err := host.New(host.Config{
		DBPath: dbPath, Model: "scripted", System: "limit", MaxTurns: 4,
		Allow: []string{"read"}, Provider: &textOnlyProvider{text: "在"}, CWD: dir,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "在吗"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	main, ok := h.Discovery().PrimaryAgent(sid)
	if !ok {
		t.Fatalf("没有主 agent")
	}
	// 直接往事件库塞 250 条该 session 的事件（模拟一个"很长"的 session）。
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	payload, err := json.Marshal(contracts.ModelTurnPayload{Text: "turn"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	evs := make([]contracts.Event, 0, 250)
	for i := 0; i < 250; i++ {
		evs = append(evs, contracts.Event{
			V: 1, ID: core.NewUlid(), Kind: "model.turn",
			TS: time.Now().UTC().Format(time.RFC3339), SessionID: sid, AgentID: main,
			Durable: true, Payload: payload,
		})
	}
	if _, err := st.Append(context.Background(), evs); err != nil {
		t.Fatalf("append: %v", err)
	}
	_ = st.Close()

	// 不传 limit → 默认有界（200）+ 显式的"没取完"信号。
	var inq agentlink.InquiryResult
	if code := getJSON(t, srv.URL+"/api/inquiry?session_id="+string(sid), &inq); code != http.StatusOK {
		t.Fatalf("inquiry 状态码 = %d", code)
	}
	if len(inq.Window) != 200 {
		t.Fatalf("默认窗口 = %d 条，want 200（默认有界，F6）", len(inq.Window))
	}
	if !inq.Truncated {
		t.Fatalf("没取完就该报 truncated（F6）")
	}
	// 显式传 limit → 按传入的来。
	if code := getJSON(t, srv.URL+"/api/inquiry?session_id="+string(sid)+"&limit=5", &inq); code != http.StatusOK {
		t.Fatalf("inquiry(limit=5) 状态码 = %d", code)
	}
	if len(inq.Window) != 5 {
		t.Fatalf("窗口 = %d 条，want 5", len(inq.Window))
	}
}

// TestF9BroadcastCatchesUpBeforeComputingRecipients（F9）：Broadcast/Handoff 要先
// catchUp 发现表——收件方是**按当前成员关系**算的，表没追平就会算错（审计里的
// Recipients 与读时看到的对不上）。
func TestF9BroadcastCatchesUpBeforeComputingRecipients(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "events.db")
	h, err := host.New(host.Config{
		DBPath: dbPath, Model: "scripted", System: "catchup", MaxTurns: 4,
		Allow: []string{"read"}, Provider: &textOnlyProvider{text: "在"}, CWD: dir,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	sidA := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sidA, "在吗"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	if _, err := h.JoinGate(context.Background(), sidA, e2eGate); err != nil {
		t.Fatalf("JoinGate: %v", err)
	}
	mainA, _ := h.Discovery().PrimaryAgent(sidA)

	// 另一个"进程"往共享事件库写了一个新成员 C 的事实（user.prompt + gate.joined）。
	sidC := host.NewSession()
	agentC := core.NewUlid()
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	prompt, err := json.Marshal(contracts.UserPromptPayload{Text: "C 的开场"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	join, err := json.Marshal(contracts.GateJoinedPayload{GateID: e2eGate, Source: contracts.GateJoinSourceExplicit})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := st.Append(context.Background(), []contracts.Event{
		{V: 1, ID: core.NewUlid(), Kind: "user.prompt", TS: now, SessionID: sidC, AgentID: agentC, Durable: true, Payload: prompt},
		{V: 1, ID: core.NewUlid(), Kind: "gate.joined", TS: now, SessionID: sidC, AgentID: agentC, Durable: true, Payload: join},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	_ = st.Close()
	// C 由本进程持有（活状态是进程本地的，宿主显式激活）。
	h.Discovery().Activate(sidC)

	// 广播的收件方必须算上 C（表没追平就会漏掉它）。
	res, err := h.Broadcast(context.Background(),
		agentlink.Identity{SessionID: sidA, AgentID: mainA},
		agentlink.BroadcastRequest{Target: gateTarget(e2eGate), Text: "新成员也在吗"})
	if err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	found := false
	for _, r := range res.Recipients {
		if r.SessionID == string(sidC) && r.AgentID == string(agentC) {
			found = true
		}
	}
	if !found {
		t.Fatalf("收件方没算上新成员 C（F9：广播前没 catchUp）: %+v", res.Recipients)
	}
}
