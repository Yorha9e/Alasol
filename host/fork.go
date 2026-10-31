package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/runtime"
)

// 本文件实现 v3-S2 的 **fork + /btw**（K29 拍板）。
//
// # fork 形态与事件归属（一句话）
//
// fork = 从主 session 的**对话上下文**（FoldConversation 折叠出的历史，与 resume 续跑同源）
// 分叉出的**一条旁支执行线**：用一个**全新的 fork session_id + fork agent_id** 起一个 fresh
// Loop 跑一轮（RunWithHistory），**带**对话上下文（system + 历史）、**不带** goal ledger /
// gate（不复制）；旁支的全部事件（btw.forked / user.prompt / model.turn / tool.call.* /
// context.assembled）都归属到这个**独立的 fork session**，因此**主 session 的事件流零新增**
// ——/btw 结果只回给调用方展示，绝不回灌主上下文。
//
// # 为什么用独立 session_id 而不是同 session 换 agent_id
//
// "主上下文零新增"要求主 session 的 user.prompt/model.turn/context.assembled 一条不增。
// 同 session 换 agent_id 仍会把事件写进主 session 的流（按 session 回放就看得到），只是
// agent_id 不同——那会让"主 session 对话历史"的判定变糊。独立 session_id 让主/旁支在存储层
// 就分开：ReplaySession(主) 永远看不到旁支的任何事件，断言干净，也不会污染主 session 的
// context.assembled 历史（/ctx 不受影响）。
//
// # X1 红线口径
//
// 旁支的工具调用**走同一条合法写路径**（intent → 执行 → completed/failed，经同一个 Bus），
// 只是这些事件的 session_id/agent_id = fork 的 id，**不属于主 session 的对话历史**——
// 写路径形状一字不动，归属可辨（独立 session）。
//
// # 历史口径：主轮次不带历史，/btw 带历史（且有上限）
//
// 主 session 的**每一轮**（RunPrompt → loop.Run）都是**不带历史**的——历史只存在于
// 事件流里（靠 fold 重建），只在两处被"带回去"：resume 续跑、/btw 旁支。于是
// /btw 旁支天然"比主会话记得多"：这正是它的用途（K29：旁支带上下文回答问题），
// 但也意味着旁支带的可能是**原文**（主 session 早压缩过的上下文与之已经不一致），
// 一次长对话就能把整个历史塞进一次模型请求。故：
//
//  1. 折叠历史有**条数 + 字符数**上限（maxForkHistoryMessages / maxForkHistoryChars）；
//  2. 截断时**告知**：模型侧在历史头部插一条 system 说明（更早的对话被截断），
//     用户侧由 BtwResult.HistoryTruncated 带出（CLI/TUI 打印一行）——不许静默截断；
//  3. 旁支**不挂上下文策略**（ContextStrategy 留空，见下面 ④ 的注释）。
//
// # fork session 不是一等会话（K29）
//
// 旁支是一次性执行线：结果只回给用户、不回灌主上下文。故 host 侧**显式登记**它
// （fork 集合，见 host/discovery.go 的 discoveryFeed.forks），presence/peers 面
// **统一过滤**——"旁支不是一等会话"是一条口径，而不是"它恰好没 join gate、恰好
// 没被激活"的副产物。

// KindBtwForked 是标记一次 /btw fork 的事件 kind（契约见 contracts.BtwForkedPayload）。
//
// 它落在 **fork session**（envelope.session_id = fork id），payload.source_session_id 指回
// 主 session——于是事件流里能审计"这条旁支是从哪个 session 分叉出来的"，同时主 session 零新增。
const KindBtwForked = "btw.forked"

// BtwResult 是一次 /btw fork 的事实（回给调用方展示 + 喂给预埋钩子）。
type BtwResult struct {
	// SourceSessionID 是发起 /btw 的主 session（fork 的上下文来源）。
	SourceSessionID contracts.Ulid
	// ForkSessionID / ForkAgentID 是旁支执行线的归属 id（与主 session 完全不同）。
	ForkSessionID contracts.Ulid
	ForkAgentID   contracts.Ulid
	// Question 是用户的 /btw 问题原文（fork 的 user.prompt 同文）。
	Question string
	// Text 是 fork 跑完的最终文本（**只回给用户展示**，不回灌主上下文）。
	Text string
	// Turns / ToolCalls / Events 是 fork 这一轮的计量（旁支侧，与主 session 的 Result 无关）。
	Turns     int
	ToolCalls int
	Events    int
	// HistoryMessages 是旁支实际带上的主对话条数（折叠 + 裁切后的结果）。
	HistoryMessages int
	// HistoryTruncated 标记主对话历史被**上限**裁过（旁支只带了最近的一部分）。
	HistoryTruncated bool
	// HistoryTrimmed 标记主对话里有**配对不完整**的片段被丢弃（assistant 的
	// tool_calls 没配齐 tool 结果——provider 会拒掉这种序列，故必须丢）。
	//
	// 与 HistoryTruncated 是**两回事**（N5）：上限裁切是"太长装不下"，配平丢弃是
	// "这段本来就发不出去"。对外告知不能混成一句"已截断"——用户据此判断的是
	// "旁支到底知道多少"。
	HistoryTrimmed bool
	// Err 是 fork 运行期的错误（nil = 正常收敛；供钩子观测，调用方也可判 errors）。
	Err error
}

// BtwCommand 是 /btw 命令的前缀（CLI 与 TUI 共用同一个解析口径）。
const BtwCommand = "/btw"

// ParseBtwCommand 解析一行输入是不是 /btw 命令；是的话给出问题原文。
//
// 口径（CLI 与 TUI 必须一致，否则同一个输入在两面行为不同）：
//
//   - "/btw"          → 是命令，问题为空（调用方打印用法）；
//   - "/btw <问题>"   → 是命令（**空格**分隔）；
//   - "/btw\t<问题>"  → 是命令（**tab** 分隔也算——终端里 tab 常被当成分隔符，
//     与挂账 e 的"tab 分隔变体也要认"同款口径）；
//   - "/btwxyz"       → **不是**命令（按普通 prompt 走：命令名后必须跟空白）。
//
// isBtw=false 时 question 恒为空。
func ParseBtwCommand(line string) (question string, isBtw bool) {
	s := strings.TrimSpace(line)
	if s == BtwCommand {
		return "", true
	}
	if !strings.HasPrefix(s, BtwCommand) {
		return "", false
	}
	rest := s[len(BtwCommand):]
	if rest == "" {
		return "", true
	}
	// 分隔符只认空白（space / tab / 其它 Unicode 空白）："/btwxyz" 不当命令。
	r, _ := utf8.DecodeRuneInString(rest)
	if !unicode.IsSpace(r) {
		return "", false
	}
	return strings.TrimSpace(rest), true
}

// fork 折叠历史的**上限**（F7：旁支比主会话记得多，得有界）。
//
// 为什么要有界：主 session 的下一轮**不带**历史（历史只在事件流里），而 /btw 旁支把
// 折叠出的历史整段带进模型请求——不设上限时，一次长对话就能把整个历史塞进一次请求
// （token 与费用双爆），而且旁支带的是**原文**（主 session 早压缩过的上下文与之
// 已经不一致）。
//
// 两个上限各管一件事：条数管"轮次太多"，字符数管"单条太长"（一条超长工具结果就能
// 顶掉整个额度）。
const (
	// maxForkHistoryMessages 是旁支带进模型的**主对话条数**上限（含工具消息）。
	maxForkHistoryMessages = 16
	// maxForkHistoryChars 是旁支带进模型的**主对话字符数**上限（粗略 token 代理：
	// 约 4 字符 ≈ 1 token）。超限就从**最早**开始丢（保尾：最近的对话最相关）。
	maxForkHistoryChars = 6000
)

// BtwPromoteFunc 是"从 /btw 生成全新 session"的**预埋钩子**（K29：只埋点不实现）。
//
// fork 跑完（成功或失败）后调用一次，参数 = 本次 fork 的事实（BtwResult）。框架**只给事实、
// 不给生成逻辑**——这正是"埋点≠实现"：要实现"把 /btw 旁支提成一个一流 session"需多方面微调
// （fork session 的 gate 归属、在线表可见性、agent 树登记、UI 呈现、与主 session 的关系），
// 远超"接个回调"的成本，故 v3-S2 只留此扩展面。默认 nil = 不接 = 埋点存在但无人消费。
//
// 钩子在 RunBtw 返回前、锁外调用（不持 h.mu）——实现方可在里面做 IO / 再调 hub，不会死锁。
type BtwPromoteFunc func(BtwResult)

// RunBtw 跑一次 /btw 旁支 fork（K29）。
//
// 行为：读主 session 事件流 → FoldConversation 折叠出对话历史 → 造全新 fork session/agent →
// 落一条 btw.forked（标记旁支来源）→ 起一个 fresh Loop（**带** system + 折叠历史、**不带**
// ledger/gate、不挂在线表/上下文策略）→ RunWithHistory 跑一轮 → 结果只回给调用方。
//
// 主 session 的事件流**零新增**：旁支全部事件归属独立的 fork session_id。
//
// 错误语义：
//   - question 为空 / 回放主 session 失败 / 落 btw.forked 失败 → 返回 error（fork 没跑起来，
//     钩子不触发——fail-close：不跑一个没有归属标记的旁支）；
//   - 主 session id 非法（空 / 非 26 字符 ULID）/ 该 session **没有任何事件** → 返回 error
//     （**可区分**：id 非法说"不是合法 ULID"，无事件说"还没有任何事件"；fail-close：
//     不产一条 source_session_id 非法的 btw.forked，也不静默跑一个没有上下文的旁支）；
//   - fork 运行期错误（模型失败 / intent 落盘失败 / ctx 取消）→ BtwResult.Err 带上、钩子仍
//     触发（埋点要能观测失败），并原样返回该 error 供调用方展示。
func (h *Hub) RunBtw(ctx context.Context, mainSessionID contracts.Ulid, question string) (BtwResult, error) {
	if h == nil {
		return BtwResult{}, errors.New("host: Hub 为 nil")
	}
	q := strings.TrimSpace(question)
	if q == "" {
		return BtwResult{}, errors.New("host: /btw 问题不能为空")
	}
	// 入口校验主 session id（fail-close）：空 id / 非法 id 会让 btw.forked 载荷里出现
	// **source_session_id: ""**——那是契约非法载荷（id 必须是 26 字符 Crockford base32）。
	if !core.ValidUlid(mainSessionID) {
		return BtwResult{}, fmt.Errorf("host: /btw 主 session id 不是合法 ULID（26 字符 Crockford base32）: %q", mainSessionID)
	}
	if err := ctx.Err(); err != nil {
		return BtwResult{}, err
	}

	// ① 上下文来源：主 session 的对话历史（事件流 → fold，与 resume 续跑同源）。
	//    FoldConversation 只取 user.prompt/model.turn/tool.call.*——**天然不含** ledger/gate，
	//    所以"不带 ledger/gate"在折叠这一步就成立（主 session 即便有账本/门禁也不进旁支）。
	events, err := runtime.ReplaySession(ctx, h.store, mainSessionID)
	if err != nil {
		return BtwResult{}, fmt.Errorf("host: /btw 回放主 session %s 失败: %w", mainSessionID, err)
	}
	if len(events) == 0 {
		// 可区分的提示：这个 session 不存在、或还没跑过任何一轮（没有可折叠的对话）。
		// 不静默跑一个"没有上下文的旁支"——用户会以为旁支看到了主对话。
		return BtwResult{}, fmt.Errorf("host: /btw 主 session %s 还没有任何事件（不存在或尚未跑过一轮对话）", mainSessionID)
	}
	// 折叠 + 裁切（有上限 + 裁切告知，见文件头的"历史口径"）。
	history, keptCount, byLimit, byBalance := trimForkHistory(runtime.FoldConversation(events))
	if byLimit || byBalance {
		// 模型侧告知：历史头部插一条 system 说明（更早的对话被截断 / 片段被丢弃）。
		history = append([]core.Message{{Role: "system", Content: forkHistoryNotice(keptCount, byLimit, byBalance)}}, history...)
	}

	// ② 旁支归属：全新 fork session + fork agent（与主 session 完全不同 → 主 session 零新增）。
	forkSID := core.NewUlid()
	forkAgent := core.NewUlid()
	// 登记进 host 的 fork 集合（v3-S2 K29）：旁支不是一等会话，presence/peers 面按它
	// 过滤（见 host/discovery.go 的 discoveryFeed.forks）。**不等事件流扫到**——刚造出
	// 的旁支立刻就该被过滤掉。
	h.markForkSession(forkSID)

	// ③ 落一条 btw.forked（durable，归 fork session）——标记"本 session 是从 mainSessionID 分叉的
	//    旁支"，可审计/可回放；落不下就不跑旁支（fail-close：不产一条没有归属标记的幽灵执行线）。
	if err := h.publishBtwForked(ctx, forkSID, forkAgent, mainSessionID, q); err != nil {
		return BtwResult{
			SourceSessionID:  mainSessionID,
			ForkSessionID:    forkSID,
			ForkAgentID:      forkAgent,
			Question:         q,
			HistoryMessages:  keptCount,
			HistoryTruncated: byLimit,
			HistoryTrimmed:   byBalance,
			Err:              err,
		}, err
	}

	// ④ fresh Loop：**带** system + 折叠历史；**不带** ledger/gate（不挂 ContextStrategy、
	//    不 join gate）；不挂在线表（旁支不是水平可见的对端主 agent，K11）。
	//    Provider/Bus/Registry/Policy/Approver 与主 session 同源（旁支能调同样的工具、
	//    过同样的策略链，走同一条 X1 写路径）。
	//
	//    **ContextStrategy 故意留空**：挂上的上下文策略会注入 ledger/rules 两张注入区
	//    （v2-S1 tiered），那正是"fork 不带 ledger/gate"要防的事——旁支只带**对话历史**。
	//
	//    D4 大 payload 外置（v4-3-A，G2e）：旁支的 loop **同样挂 blob**（与主 session 同口径，
	//    见 loopFor）——旁支的工具大输出同样不该进事件流。不挂 = "大输出不进事件流"这条规则
	//    只在主 session 上成立（fork session 的事件流里留着几十 MB 的 result，与文档口径不符）。
	//    关（未启用 blob / 阈值<=0）= 零行为变化（completed 载荷逐字节等价）。
	fl, err := loop.New(loop.Config{
		Provider:        h.provider,
		Bus:             h.bus,
		Registry:        h.reg,
		Policy:          h.engine,
		Approver:        h.approver,
		SessionID:       forkSID,
		AgentID:         forkAgent,
		Model:           h.cfg.Model,
		System:          h.cfg.System,
		MaxTurns:        h.cfg.MaxTurns,
		ReasoningEffort: h.cfg.ReasoningEffort, // 思维链档位与主 session 同源（G14）
		ContextBudget:   h.contextBudgetFor(h.cfg.ContextStrategy),
		BlobSink:        h.blob,
		BlobMaxInline:   h.cfg.BlobMaxInline,
	})
	if err != nil {
		btw := BtwResult{
			SourceSessionID:  mainSessionID,
			ForkSessionID:    forkSID,
			ForkAgentID:      forkAgent,
			Question:         q,
			HistoryMessages:  keptCount,
			HistoryTruncated: byLimit,
			HistoryTrimmed:   byBalance,
			Err:              err,
		}
		h.fireBtwPromoteHook(btw)
		return btw, fmt.Errorf("host: /btw 造 fork loop 失败: %w", err)
	}

	// ⑤ 跑一轮（既有历史排在 question 之前 → 旁支"记得"主上下文）。
	res, runErr := fl.RunWithHistory(ctx, history, q)
	btw := BtwResult{
		SourceSessionID:  mainSessionID,
		ForkSessionID:    forkSID,
		ForkAgentID:      forkAgent,
		Question:         q,
		Text:             res.Text,
		Turns:            res.Turns,
		ToolCalls:        res.ToolCalls,
		Events:           res.Events,
		HistoryMessages:  keptCount,
		HistoryTruncated: byLimit,
		HistoryTrimmed:   byBalance,
		Err:              runErr,
	}
	// ⑥ 预埋钩子（只埋点不实现）：给事实，不给生成逻辑。成功/失败都触发（埋点要能观测）。
	h.fireBtwPromoteHook(btw)
	return btw, runErr
}

// publishBtwForked 经 bus 落一条 btw.forked durable 事件（合法写路径；归 fork session）。
func (h *Hub) publishBtwForked(ctx context.Context, forkSID, forkAgent, mainSID contracts.Ulid, question string) error {
	raw, err := json.Marshal(contracts.BtwForkedPayload{
		SourceSessionID: mainSID,
		Question:        question,
	})
	if err != nil {
		return fmt.Errorf("host: 序列化 btw.forked payload 失败: %w", err)
	}
	ev := contracts.Event{
		V:         1,
		ID:        core.NewUlid(),
		Kind:      KindBtwForked,
		TS:        time.Now().UTC().Format(time.RFC3339),
		SessionID: forkSID,
		AgentID:   forkAgent,
		Durable:   true,
		Payload:   raw,
	}
	if err := h.bus.Publish(ctx, ev); err != nil {
		return fmt.Errorf("host: 落 btw.forked 事件失败: %w", err)
	}
	return nil
}

// fireBtwPromoteHook 在锁外触发预埋钩子（读钩子持锁，调用不持锁 → 实现方可再调 hub 不死锁）。
func (h *Hub) fireBtwPromoteHook(res BtwResult) {
	h.mu.Lock()
	fn := h.btwPromote
	h.mu.Unlock()
	if fn != nil {
		fn(res)
	}
}

// SetBtwPromoteHook 设置"从 /btw 生成全新 session"的预埋钩子（K29：只埋点不实现）。
//
// 传 nil = 摘钩子（回到"埋点存在但无人消费"的默认）。钩子每次 /btw 完成后触发一次。
// 框架不提供任何"生成 session"的实现——实现方在这个回调里自己决定拿 BtwResult 做什么
// （需多方面微调，见 BtwPromoteFunc 的说明）。
func (h *Hub) SetBtwPromoteHook(fn BtwPromoteFunc) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.btwPromote = fn
	h.mu.Unlock()
}

// ---------------------------------------------------------------------------
// fork 集合：旁支不是一等会话（presence/peers 面统一过滤）
// ---------------------------------------------------------------------------

// ForkSessions 返回本 hub 已知的 **/btw 旁支 fork session** 集合（升序；审计/诊断用）。
//
// 这些 session 不是一等会话：结果只回给用户、不回灌主上下文。故 presence/peers 面
// 把它们过滤掉（见 Hub.Peers / Hub.Presence / Hub.Discover / 在线表注入面）——
// "旁支不是一等会话"是一条**显式口径**，而不是"它恰好没 join gate、恰好没被激活"
// 的副产物。
//
// 数据来自事件流（btw.forked）：进程重启后重扫即恢复（与在线表的 fork 标记同源）。
func (h *Hub) ForkSessions() []contracts.Ulid {
	if h == nil {
		return nil
	}
	return h.discFeed.forkSessions()
}

// markForkSession 登记一个 fork session（RunBtw 造出旁支时立即登记，不等事件流扫到）。
func (h *Hub) markForkSession(sid contracts.Ulid) {
	if h == nil {
		return
	}
	h.discFeed.markFork(sid)
}

// isForkSession 报告某 session 是否是一条 /btw 旁支 fork。
func (h *Hub) isForkSession(sid contracts.Ulid) bool {
	if h == nil {
		return false
	}
	return h.discFeed.isFork(sid)
}

// filterForkPeers 从水平可见对端里剔掉 fork session（旁支不是一等会话）。
func (h *Hub) filterForkPeers(peers []discovery.PeerRow) []discovery.PeerRow {
	out := make([]discovery.PeerRow, 0, len(peers))
	for _, p := range peers {
		if h.isForkSession(contracts.Ulid(p.SessionID)) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// filterForkGateMembers 从 gate 成员里剔掉 fork session（旁支不 join gate；此处是纵深防御）。
func (h *Hub) filterForkGateMembers(rows []discovery.SessionRow) []discovery.SessionRow {
	out := make([]discovery.SessionRow, 0, len(rows))
	for _, r := range rows {
		if h.isForkSession(contracts.Ulid(r.SessionID)) {
			continue
		}
		out = append(out, r)
	}
	return out
}

// filterForkPeerPresence 从 agentlink 的对端画像里剔掉 fork session（presence 面）。
func (h *Hub) filterForkPeerPresence(peers []agentlink.PeerPresence) []agentlink.PeerPresence {
	out := make([]agentlink.PeerPresence, 0, len(peers))
	for _, p := range peers {
		if h.isForkSession(contracts.Ulid(p.SessionID)) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// filterForkSessionPresence 从 agentlink 的成员画像里剔掉 fork session（discover 面）。
func (h *Hub) filterForkSessionPresence(rows []agentlink.SessionPresence) []agentlink.SessionPresence {
	out := make([]agentlink.SessionPresence, 0, len(rows))
	for _, r := range rows {
		if h.isForkSession(contracts.Ulid(r.SessionID)) {
			continue
		}
		out = append(out, r)
	}
	return out
}

// filterForkAgentPresence 从 agentlink 的 agent 行里剔掉 fork session 的 agent
// （discover 的 agent 面；N7：agent 行带 session_id，只滤成员面会自相矛盾——
// "成员里没有它、agent 树里却有它"）。
func (h *Hub) filterForkAgentPresence(rows []agentlink.AgentPresence) []agentlink.AgentPresence {
	out := make([]agentlink.AgentPresence, 0, len(rows))
	for _, r := range rows {
		if h.isForkSession(contracts.Ulid(r.SessionID)) {
			continue
		}
		out = append(out, r)
	}
	return out
}

// ---------------------------------------------------------------------------
// 折叠历史的裁切（F7：旁支比主会话记得多 → 必须有界 + 截断告知）
// ---------------------------------------------------------------------------

// forkHistoryNotice 是裁切时插给**模型**的那条说明（模型侧告知）。
//
// 为什么用 role="system"：它不是用户说的话，而是框架对"上下文不完整"的声明
// （provider 侧 system 角色受支持，见 runtime/provider/openai 的 mapMessage）。
// 用 user 角色会让模型误以为用户说过这句话。
//
// 两种裁切**分开说**（N5）：按上限截断 = "更早的对话装不下"；配平丢弃 = "这段本来就
// 发不出去（工具调用配对不完整）"——混成一句"已截断"会误导模型与用户。
func forkHistoryNotice(kept int, byLimit, byBalance bool) string {
	switch {
	case byLimit && byBalance:
		return fmt.Sprintf(
			"（/btw 旁支上下文提示：主 session 更早的对话超出旁支上下文上限，已被截断；另有工具调用配对不完整的片段被丢弃。这里只附带 %d 条主对话。若需要更早的内容，请让用户补充说明。）",
			kept)
	case byLimit:
		return fmt.Sprintf(
			"（/btw 旁支上下文提示：主 session 更早的对话超出旁支上下文上限，已被截断；这里只附带最近 %d 条主对话。若需要更早的内容，请让用户补充说明。）",
			kept)
	default: // byBalance
		return fmt.Sprintf(
			"（/btw 旁支上下文提示：主 session 对话里有工具调用配对不完整的片段（provider 无法接受这种序列），已被丢弃；这里附带 %d 条主对话。）",
			kept)
	}
}

// forkMessageChars 是单条消息的**字符成本**（旁支上下文上限的计量口径）。
//
// 口径与 core/loop 的 estimateMessageTokens **同源**：正文 + 工具调用的**工具名** +
// 工具**参数**（assistant 的 ToolCall.Args 是模型生成的 JSON，同样占窗口）。
//
// 为什么不能只算 Content（N2）：一条 write 大文件的参数就能单独顶掉整个额度——
// 只算正文时，旁支的模型请求可以远超预算（上限形同虚设）。
func forkMessageChars(m core.Message) int {
	n := len(m.Content)
	for _, tc := range m.ToolCalls {
		n += len(tc.Name) + len(tc.Args)
	}
	return n
}

// trimForkHistory 把折叠出的主对话历史裁到上限内（**保尾**：最近的对话最相关），
// 返回裁后的消息、条数，以及**两种裁切各自的标志**：
//
//	byLimit   = 触了上限（条数/字符）而裁掉了一部分；
//	byBalance = 配平阶段丢掉了配对不完整的片段（provider 发不出去的那部分）。
//
// 两者是**不同的事**（N5）：上限是"装不下"，配平是"本来就发不出去"——对外的告知
// 不能混成一句"已截断"。
//
// 裁切必须落在**安全边界**上：provider 对消息序列有硬约束（role="tool" 必须配得上
// 它对应的 assistant(tool_calls)；assistant 声明 N 个 tool_calls 就必须配 N 条
// tool 结果）——切一半会得到 provider 直接拒绝的请求。故裁完还要"配平"
// （见 balanceHistory）。
//
// 没超上限时也过一遍配平：事件流里若缺了 tool.call.intent（坏载荷被宽容跳过），
// 折叠出的历史里会留下配不上的 tool 消息，同样会让 provider 拒掉整个请求。
func trimForkHistory(msgs []core.Message) (kept []core.Message, count int, byLimit bool, byBalance bool) {
	if len(msgs) == 0 {
		return nil, 0, false, false
	}
	total := 0
	for _, m := range msgs {
		total += forkMessageChars(m)
	}
	if len(msgs) <= maxForkHistoryMessages && total <= maxForkHistoryChars {
		out := balanceHistory(msgs)
		return out, len(out), false, len(out) < len(msgs)
	}
	// 从尾部往前取（在条数与字符两个上限内都成立的最长尾）。
	start := len(msgs)
	chars := 0
	for start > 0 {
		m := msgs[start-1]
		if len(msgs)-start+1 > maxForkHistoryMessages {
			break
		}
		c := forkMessageChars(m)
		if chars+c > maxForkHistoryChars {
			break
		}
		chars += c
		start--
	}
	if start == len(msgs) {
		// 连最后一条都超字符上限：至少留最近一条（上限是软边界，不是"什么都不给"）。
		start = len(msgs) - 1
	}
	tail := msgs[start:]
	out := balanceHistory(tail)
	return out, len(out), true, len(out) < len(tail)
}

// balanceHistory 丢掉"配不齐"的 tool / assistant 配对（provider 硬约束）。
//
// 规则：
//
//  1. role="tool" 消息必须配得上**前面**某条 assistant 声明过的 tool_call_id；
//     配不上（它的 assistant 被裁掉了，或事件流里压根没有对应的 intent）→ 丢；
//  2. assistant 声明了 N 个 tool_calls，就必须有 N 条 tool 结果**按 id 配给它**；
//     不足 → 这条 assistant 不完整 → 丢它**和它名下的那些 tool 消息**。
//
// **按 id 配对，不按位置**（N6）：折叠出来的顺序可能乱——tool A 的结果可能排在
// assistant B 之后（事件流里 A 的完成事件晚于 B 的轮次）。若按"紧邻的一段"来删，
// 就会把**别人的** tool 消息一起删掉（或者留下属于被删 assistant 的孤立 tool 消息
// ——provider 会拒掉整条请求）。
func balanceHistory(msgs []core.Message) []core.Message {
	// ① 逐条过：tool 消息必须配得上**已经出现过**的 assistant 声明（孤立的丢）。
	//    assistant 记下它声明的 tool_call id（同一 id 被多条 assistant 声明时，
	//    **最早**声明者为准——先发起先归属）。
	type assistant struct {
		idx   int      // 该 assistant 在 out 里的下标
		ids   []string // 它声明的 tool_call id
		tools []int    // 按 id 归属到它的 tool 消息在 out 里的下标
	}
	var asts []*assistant
	declared := map[string]bool{}
	toolAt := map[string]int{} // tool_call_id → tool 消息在 out 里的下标
	out := make([]core.Message, 0, len(msgs))
	for _, m := range msgs {
		switch m.Role {
		case "assistant":
			a := &assistant{idx: len(out)}
			for _, tc := range m.ToolCalls {
				id := string(tc.ID)
				if id == "" || declared[id] {
					continue
				}
				declared[id] = true
				a.ids = append(a.ids, id)
			}
			asts = append(asts, a)
			out = append(out, m)
		case "tool":
			id := string(m.ToolCallID)
			if id == "" || !declared[id] {
				continue // 孤立的 tool 消息：留着会让 provider 拒掉整条请求
			}
			if _, dup := toolAt[id]; dup {
				continue // 同一条 tool_call 的重复结果：留一条即可
			}
			toolAt[id] = len(out)
			out = append(out, m)
		default:
			out = append(out, m)
		}
	}
	// ② 按 id 把 tool 消息归到声明它的 assistant（与位置无关）。
	for _, a := range asts {
		for _, id := range a.ids {
			if idx, ok := toolAt[id]; ok {
				a.tools = append(a.tools, idx)
			}
		}
	}
	// ③ assistant 的 tool_calls 没配齐 → 丢它**和它名下的** tool 消息（按 id 删）。
	drop := make([]bool, len(out))
	for _, a := range asts {
		if len(a.tools) < len(a.ids) {
			drop[a.idx] = true
			for _, idx := range a.tools {
				drop[idx] = true
			}
		}
	}
	res := make([]core.Message, 0, len(out))
	for i, m := range out {
		if drop[i] {
			continue
		}
		res = append(res, m)
	}
	return res
}
