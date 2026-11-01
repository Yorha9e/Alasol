package host

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/compaction"
	"github.com/Yorha9e/alasol/runtime"
	"github.com/Yorha9e/alasol/runtime/indexer"
)

// 本文件把 v2-S1 上下文级联压缩策略（core/compaction 的 tiered 引擎）装配进 hub 的 loop：
// store 支撑的 seq/原文/账本源 + provider 压缩模型 + bus 事件发射，组成一个 per-Run 工厂。
//
// **opt-in**：Config.ContextStrategy 为 "tiered" 才装配；空（默认）= 不挂 = append（v1 等价，
// 酸测试）。换/摘策略 core 零改动（工厂经 loop.Config.ContextStrategy 注入，loop 只认接口）。
//
// 为什么 per-Run 工厂（compaction.AssemblerFactory）：tiered 引擎有状态（跨轮跟踪阶段），而
// loop 承诺可并发 Run；工厂让每次 Run 各持一个引擎（放进 runState），互不踩踏。

// contextStrategyTiered 是 Config.ContextStrategy 启用 tiered 引擎的取值。
const contextStrategyTiered = "tiered"

// defaultRecallReadLimit 是 recall 回捞原文时单次最多读多少条事件（区间再长也只读这么多）。
//
// 口径（F17）：recall 是"按指针取回某段原文"，不是"把整场会话搬回来"——没有上限的话，
// 一次 recall 可能拉回几十万 token，把上下文预算一次打穿。
const defaultRecallReadLimit = 2000

// recallSliceSize 是分段读（G5）的单段步长：每次 ReadSlice 最多拉这么多条进内存，
// 段间用 lastSeq+1 推进——长区间不再一次整段读（全文重放），而是按需分段、惰性加载。
const recallSliceSize = 256

// contextStrategyFactory 按策略 kind 造 per-Run 的策略工厂（nil = 不挂 = append）。
//
// 只有 "tiered" 一个实现（其余/空 = append 兜底）。kind 由调用方收敛后传入（宿主默认
// Config.ContextStrategy，或激活的工作流声明的 context_strategy，W6）。窗口 W 用
// Config.ContextStrategyWindow（0 = 默认 1M）；W < 256k 硬约束不满足时 NewEngine 返回 error
// → 此处**拒绝装配**并说明（fail-close：不满足设计要求就明说，不静默降级成一个会负预算的策略）。
//
// **静默降级口径（工作流级，与 agent_type 级不对称——刻意如此）**：**非 tiered 一律返回
// nil（=append）**——枚举内但宿主保留未实现的 stage_scoped/sliding_window 也走这条路，
// **未实现不报错**。这是**工作流级**的既定口径（工作流只是"建议宿主用哪种策略"，宿主
// 没实现就退回默认追加式）；**agent_type 级**走的是另一条路（core/workflow 的装载期
// 枚举校验 + loop 的 fail-close，见 contracts 的 AgentType.context_strategy 说明）。
// 两者不对称是刻意的：工作流级是"能力包提议"，agent_type 级是"类型显式声明"（语义更强，
// 静默降级会让人以为配了策略其实没配）。
//
// 参数面（F10）：W / L1% / L2% 全部来自宿主配置（TOML/CLI → host.Config → compaction.Params），
// 不写死；规则文本（Config.ContextRules）也在这里注入（稳定前缀的一部分）。
//
// 主 agent 的策略工厂：**恒共享项目规则**（rules）+ 接**会话级验证游标**（跨 Run 接力）。
// 委托给 tieredFactory 实现（与子 agent 的 ChildStrategy 钩子复用同一构造路径）。
func (h *Hub) contextStrategyFactory(sessionID, agentID contracts.Ulid, kind string) (compaction.AssemblerFactory, error) {
	if strings.TrimSpace(kind) != contextStrategyTiered {
		return nil, nil // append（默认）：非 tiered 一律 nil（=append）——见函数头"静默降级口径"
	}
	// 只在 tiered 时取会话级游标（避免给 append session 也建一个用不上的游标条目）。
	return h.tieredFactory(sessionID, agentID, kind, true, h.validationCursorFor(sessionID))
}

// tieredFactory 是 tiered 策略工厂的**唯一构造路径**（主 agent 的 contextStrategyFactory 与
// 子 agent 的 ChildStrategy 钩子共用）：组装 store/provider/bus 依赖成一个 per-Run 工厂。
//
// 参数：
//   - sessionID / agentID：事件归属与 recall 视角隔离（storeTextSource{agentID} /
//     busEmitter{agentID} 都按它走——主 agent 传自己的 id，子 agent 传 child.ID）；
//   - shareRules：是否注入项目规则（Config.ContextRules）。主 agent 恒 true；子 agent 按
//     childType.ShareProjectAssets（W6-A：false/未声明 = 不注入）；
//   - cursor：会话级验证游标（跨 Run 接力）。主 agent 传会话游标；子 agent 传 nil（子只跑
//     一次、同步，无跨 Run 接力需求，且避免子引擎推进共享游标让父引擎漏看会话级验证信号）。
func (h *Hub) tieredFactory(sessionID, agentID contracts.Ulid, kind string, shareRules bool, cursor *compaction.ValidationCursor) (compaction.AssemblerFactory, error) {
	if strings.TrimSpace(kind) != contextStrategyTiered {
		return nil, nil // 宿主只实现 tiered；其余（含枚举内未实现的 stage_scoped/sliding_window）→ nil
	}
	params := h.cfg.contextStrategyParams()
	rules := h.cfg.ContextRules
	if !shareRules {
		rules = "" // 子 agent 未开 share_project_assets → 不注入项目规则（W6-A）
	}
	// 先校验一次窗口（装配期就拒，别等到第一次 Run 才炸）。
	if _, err := compaction.NewEngine(params, nil, nil, nil, nil, nil); err != nil {
		return nil, err
	}
	return func() compaction.Assembler {
		engine, err := compaction.NewEngine(
			params,
			&storeSeqSource{store: h.store, sessionID: sessionID},
			storeTextSource{store: h.store, sessionID: sessionID, agentID: agentID},
			indexerLedgerSource{ix: h.indexer, sessionID: sessionID},
			providerSummarizer{provider: h.provider, model: h.cfg.Model},
			busEmitter{bus: h.bus, sessionID: sessionID, agentID: agentID},
		)
		if err != nil {
			// 已在装配期校验过窗口，这里理论不失败；真失败返回一个 append（不阻断 agent）。
			return compaction.AppendAssembler{}
		}
		// 项目规则注入（F7：规则是稳定前缀的一部分，宿主装配面可注入）。
		// **常识区注入留后续采集源**（SetCommonSense 目前无采集源——不硬造空注入）。
		if rules != "" {
			engine.SetRules(rules)
		}
		// 硬边界接线（v2-S1 挂账，J11）：让引擎自己观测事件流里的"验证通过"信号
		// （gate.checked passed=true）并打出阶段硬边界——宿主/loop 不必穿透到 per-Run 引擎。
		engine.SetValidationSignal(validationSignalSource{store: h.store, sessionID: sessionID})
		// 会话级验证游标（跨 Run 接力；子 agent 不接，cursor=nil）——见 tieredFactory 的说明。
		if cursor != nil {
			engine.SetValidationCursor(cursor)
		}
		// 注入区被截断 → **留痕**（退化必须可见：静默砍掉规则等于悄悄丢上下文）。
		// 工厂是 per-Run 的 → 告警**只报一次**（否则每轮一条，噪声淹没真问题）。
		if r, cs := engine.InjectionTruncated(); r || cs {
			h.mu.Lock()
			first := !h.injectWarned
			h.injectWarned = true
			h.mu.Unlock()
			if first {
				hubLog.Warn("host: 上下文注入区文本超预算已被截断（规则/常识区不可压缩 → 按布局预算截断；请调小规则文本或调大注入区预算）",
					"session_id", string(sessionID), "rules_truncated", r, "common_sense_truncated", cs)
			}
		}
		return engine
	}, nil
}

// childStrategyFactory 造 W6-A 的 ChildStrategy 装配钩子（loop.Agents.ChildStrategy），绑到某
// session（sessionID 由 loopFor 闭包捕获——子 agent 与父**同 session**，K25 单流多 agent）。
//
// 钩子签名（见 loop.Agents.ChildStrategy 注释）：core 只在子类型**声明了非 append 的合法 kind**
// 时才调它（未声明/append 在 core 的 childStrategyFor 就返回 nil = 追加式了）。本实现：
//
//   - kind=tiered → 复用 tieredFactory 造工厂，但 **agentID 绑 child.ID**（recall 视角隔离 +
//     context.compacted 事件归属都按子 agent 走）；share_project_assets=true 才注入项目规则；
//   - kind=stage_scoped/sliding_window（枚举内但宿主未实现）→ tieredFactory 返回 nil → 本钩子
//     返回 **nil 工厂** → core fail-close 收口成 agent.failed（不静默降级成 append，钩子契约）；
//   - 窗口不合法 → tieredFactory 返回 error → 同样返回 nil 工厂（fail-close）。
//
// **并发契约**（Agents.ChildStrategy 要求实现并发安全）：本钩子无共享可变状态——只读 h.cfg /
// h.store 等不可变依赖，每次调用造**独立**的 per-Run 工厂与引擎（策略有状态，不跨 Run 共享）。
// 唯一的共享状态 h.injectWarned 由 tieredFactory 内的 h.mu 短临界区保护。
func (h *Hub) childStrategyFactory(sessionID contracts.Ulid) func(child agents.Node, childType agents.AgentType) compaction.AssemblerFactory {
	return func(child agents.Node, childType agents.AgentType) compaction.AssemblerFactory {
		// 防御：core 已保证非 nil，这里再挡一次（nil = 让 core 的 childStrategyFor fail-close）。
		if childType.ContextStrategy == nil {
			return nil
		}
		share := childType.ShareProjectAssets != nil && *childType.ShareProjectAssets
		kind := string(childType.ContextStrategy.Kind)
		f, err := h.tieredFactory(sessionID, child.ID, kind, share, nil)
		if err != nil || f == nil {
			// 宿主未实现的 kind → (nil,nil)；窗口不合法 → (nil,err)。都返回 nil 工厂 →
			// core fail-close（该 spawn 的子 agent 以 agent.failed 收口，不静默走 append）。
			//
			// err 不能丢（F1 终查）：core 只会给通用文案"装配钩子未产出工厂"，配置现场
			// （窗口不合法等）的线索只在这里有——留一条 Warn 才排得了障。
			if err != nil {
				hubLog.Warn("host: 子 agent 上下文策略装配失败（该 spawn 将以 agent.failed 收口）",
					"session_id", string(sessionID), "child_agent_id", string(child.ID),
					"agent_type", childType.ID, "kind", kind, "err", err.Error())
			} else {
				hubLog.Warn("host: 子 agent 上下文策略 kind 未实现（该 spawn 将以 agent.failed 收口）",
					"session_id", string(sessionID), "child_agent_id", string(child.ID),
					"agent_type", childType.ID, "kind", kind)
			}
			return nil
		}
		return f
	}
}

// validationCursorFor 取（或建）某 session 的验证信号游标（v3-S3 修：跨 Run 接力）。
//
// 口径：游标是**会话级**状态（不是 core 全局状态——W12 同款纪律：归属 session），
// 一个 session 一份，跨 Run 复用；引擎是 per-Run 的，游标在引擎之外才活得下来。
//
// 独立锁：装配路径（loopFor）可能持有 h.mu 时调到本函数——不另设锁会自锁。
func (h *Hub) validationCursorFor(sessionID contracts.Ulid) *compaction.ValidationCursor {
	h.validationMu.Lock()
	defer h.validationMu.Unlock()
	if h.validationCursors == nil {
		// 兜底：Hub 结构体**字面量**构造（内部测试）时 map 为 nil——懒建，别 panic。
		h.validationCursors = make(map[contracts.Ulid]*compaction.ValidationCursor)
	}
	c, ok := h.validationCursors[sessionID]
	if !ok {
		c = compaction.NewValidationCursor()
		h.validationCursors[sessionID] = c
	}
	return c
}

// storeSeqSource 从事件库增量读当前 session 的最大 seq（engine 记录阶段 seq 区间用）。
type storeSeqSource struct {
	store     core.Store
	sessionID contracts.Ulid
	tip       int64 // 缓存水位：每轮只读 tip 之后的新事件（增量，非全扫）
}

func (s *storeSeqSource) Tip(ctx context.Context) (int64, error) {
	events, err := s.store.ReadFrom(ctx, &s.sessionID, s.tip+1, 0)
	if err != nil {
		return s.tip, err
	}
	for _, ev := range events {
		if ev.Seq != nil && *ev.Seq > s.tip {
			s.tip = *ev.Seq
		}
	}
	return s.tip, nil
}

// storeTextSource 按 seq 区间读对话原文（recall 取回全文）。折成 provider 无关消息再拍成文本。
//
// 口径（F17/F18/G5）：
//   - **有界读**：只读指针区间（[fromSeq, toSeq]），不再"读整个 session 再过滤"
//     （那是全量扫描：一次 recall 把整场会话都搬回来）；
//   - **分段惰性读（G5）**：store 实现 core.SliceReader 时按 recallSliceSize 逐段
//     ReadSlice（seq 索引范围扫描 + LIMIT），未实现时退回 ReadFrom——两条路径结果逐事件等价；
//   - **按 agent 过滤**：同一 session 里别的 agent 的对话不该被回捞（视角隔离，K25）；
//   - **有上限**：区间过长时截断并说明（不让一次 recall 打穿上下文预算）。
type storeTextSource struct {
	store     core.Store
	sessionID contracts.Ulid
	agentID   contracts.Ulid // 本 agent 视角；空 = 不按 agent 过滤（单 agent 场景）
	limit     int            // 单次最多读多少条事件（0 = defaultRecallReadLimit）
}

func (s storeTextSource) Text(ctx context.Context, fromSeq, toSeq int64) (string, error) {
	if toSeq < fromSeq {
		return "", nil
	}
	limit := s.limit
	if limit <= 0 {
		limit = defaultRecallReadLimit
	}
	// seq 是全局单调的（陷阱 12），故区间长度是"该 session 在区间内的事件数"的上界。
	span := toSeq - fromSeq + 1
	truncated := false
	if span > int64(limit) {
		span = int64(limit)
		truncated = true
	}
	events, err := s.readRange(ctx, fromSeq, toSeq, int(span))
	if err != nil {
		return "", err
	}
	inRange := make([]contracts.Event, 0, len(events))
	for _, ev := range events {
		if ev.Seq == nil || *ev.Seq > toSeq {
			continue
		}
		if s.agentID != "" && ev.AgentID != s.agentID {
			continue // F18：别的 agent 的对话不回捞
		}
		inRange = append(inRange, ev)
	}
	text := compaction.ConversationText(runtime.FoldConversation(inRange))
	if truncated {
		text = fmt.Sprintf("（区间过长，已按上限截断：只回捞了前 %d 条事件）\n%s", limit, text)
	}
	return text, nil
}

// readRange 取 [fromSeq, toSeq] 闭区间内本 session 的前 want 条事件（G5 分段惰性读）。
//
// store 实现了 core.SliceReader（如 runtime/store/sqlite）→ 按 recallSliceSize 逐段
// ReadSlice：每段一次带 LIMIT 的 seq 索引范围扫描，段间用 lastSeq+1 推进——区间之外
// （含本 session 在 toSeq 之后的事件）根本不进内存，长区间不再"读一大段再裁尾巴"。
//
// 未实现（旧 store / 测试替身）→ 退回 ReadFrom 全文重放式一次读（调用方照旧在内存里
// 裁到区间）——**两条路径结果逐事件等价**（同序同集：分段路径按 lastSeq+1 严格接续、
// want 封顶；兜底路径靠"同 session 事件按 seq 升序、区间内事件必在区间外事件之前"保证
// 前缀一致），这是 G5 的酸测试口径：不启用 = 关，逐事件等价。
func (s storeTextSource) readRange(ctx context.Context, fromSeq, toSeq int64, want int) ([]contracts.Event, error) {
	reader, ok := s.store.(core.SliceReader)
	if !ok {
		// 兜底（不启用 G5）：全文重放式一次读，调用方裁区间。
		return s.store.ReadFrom(ctx, &s.sessionID, fromSeq, want)
	}
	out := make([]contracts.Event, 0, 8)
	for cur := fromSeq; cur <= toSeq && len(out) < want; {
		n := recallSliceSize
		if remain := want - len(out); remain < n {
			n = remain
		}
		slice, err := reader.ReadSlice(ctx, &s.sessionID, cur, toSeq, n)
		if err != nil {
			return nil, err
		}
		if len(slice) == 0 {
			break // 区间内本 session 事件已穷尽。
		}
		out = append(out, slice...)
		if len(slice) < n || len(out) >= want {
			break // 段没读满 = 区间穷尽；或已到 want 上限（截断口径与旧路径一致）。
		}
		last := slice[len(slice)-1].Seq
		if last == nil {
			break // 防御：读回的事件不该缺 seq，缺了就没法推进游标（不空转）。
		}
		cur = *last + 1
	}
	return out, nil
}

// indexerLedgerSource 从索引器的 ledger_state 投影读当前 goal ledger（级联 C 案的"总体局势"）。
// 索引是最终一致的（毫秒级延迟）——对"总体局势"足够；要强一致可改 fold 事件流。
//
// **生产缺口（F8，诚实口径）**：账本的**写入侧**目前没有生产通路——core/durable.LedgerWriter
// 全仓无调用方，宿主也没有"把账本写进去"的通道。所以这里读到的是**空账本**（除非外部
// 落了 ledger.updated 事件）→ 级联 C 案的输入退化为"只有被压区间全文"。这是已知缺口，
// 不是本文件假装接上了（见 core/compaction/doc.go 的同款说明）。
type indexerLedgerSource struct {
	ix        *indexer.Indexer
	sessionID contracts.Ulid
}

func (s indexerLedgerSource) Ledger(ctx context.Context) (contracts.LedgerUpdatedPayload, error) {
	st, ok, err := s.ix.ReadLedger(ctx, s.sessionID)
	if err != nil || !ok {
		return contracts.LedgerUpdatedPayload{}, err
	}
	return contracts.LedgerUpdatedPayload{
		Goals:       st.Goals,
		Constraints: st.Constraints,
		Completed:   st.Completed,
		Pending:     st.Pending,
	}, nil
}

// providerSummarizer 是压缩模型：默认主模型（h.cfg.Model，13 §5/§9：压缩模型默认主模型、可配）。
// 一次性 completion（非对话）：system 定压缩器角色，user 送指令 + 被压区间全文（级联 C 案输入）。
//
// 返回的 usage 是这次压缩调用的 token 计量（F16）：压缩也是一次模型调用，成本要进成本统计
// ——它随 context.compacted 事件留痕（usage 只进成本，绝不回填上下文快照/预算，J21）。
type providerSummarizer struct {
	provider core.Provider
	model    string
}

func (s providerSummarizer) Summarize(ctx context.Context, instruction, text string) (compaction.SummaryResult, error) {
	req := core.ModelRequest{
		Model:  s.model,
		System: "你是上下文压缩器。严格按用户指令把给定的阶段工作压缩成摘要：只基于给定内容，不要编造，不要反问，不要执行任何工具。",
		Messages: []core.Message{
			{Role: "user", Content: instruction + "\n\n" + text},
		},
	}
	res, err := s.provider.Run(ctx, req, func(core.ProviderEvent) {})
	if err != nil {
		return compaction.SummaryResult{}, err
	}
	return compaction.SummaryResult{
		Text:  strings.TrimSpace(res.Text),
		Model: s.model,
		Usage: &contracts.ModelTurnUsage{
			InputTokens:  res.Usage.InputTokens,
			OutputTokens: res.Usage.OutputTokens,
		},
	}, nil
}

// busEmitter 经 bus 发一条 context.compacted durable 事件（best-effort：失败不阻断装配）。
type busEmitter struct {
	bus       *bus.Bus
	sessionID contracts.Ulid
	agentID   contracts.Ulid
}

func (e busEmitter) EmitCompacted(ctx context.Context, p contracts.ContextCompactedPayload) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	ev := contracts.Event{
		V:         1,
		ID:        core.NewUlid(),
		Kind:      compaction.KindContextCompacted,
		TS:        time.Now().UTC().Format(time.RFC3339),
		SessionID: e.sessionID,
		AgentID:   e.agentID,
		Durable:   true,
		Payload:   raw,
	}
	return e.bus.Publish(ctx, ev)
}

// validationSignalSource 从事件流读"验证通过"信号（v2-S1 硬边界接线的生产驱动，J11）。
//
// 口径：事件流里出现 gate.checked 且 passed=true = 框架确认的**验证通过点**（13 §3/J11）。
// 引擎每个 Assemble 调 ValidationPassedSince（带游标，只扫新事件）；找到 → 打阶段硬边界。
// 读失败 → 返回 err（引擎据此**不**打硬边界，fail-close：不确定就不妄断验证通过）。
type validationSignalSource struct {
	store     core.Store
	sessionID contracts.Ulid
}

func (s validationSignalSource) ValidationPassedSince(ctx context.Context, afterSeq int64) (bool, int64, error) {
	events, err := s.store.ReadFrom(ctx, &s.sessionID, afterSeq+1, 0)
	if err != nil {
		return false, afterSeq, err
	}
	found := false
	tip := afterSeq
	for _, ev := range events {
		if ev.Seq != nil && *ev.Seq > tip {
			tip = *ev.Seq
		}
		if isValidationPassedEvent(ev) {
			found = true
		}
	}
	return found, tip, nil
}

// isValidationPassedEvent 判定一个事件是不是"验证通过"信号。
//
// 当前口径（两类，都表示框架确认的验证通过点，13 §3/J11）：
//   - gate.checked 且 passed=true（门禁 receipt 通过）；
//   - workflow.milestone 且 passed=true（工作流里程碑通过——W19：tower 的 critic→reviewer
//     收敛点，与 v2-S1 阶段硬边界联动）。
//
// 坏载荷 → 不当通过信号（fail-close：解不开就不妄断验证通过）。
func isValidationPassedEvent(ev contracts.Event) bool {
	switch ev.Kind {
	case "gate.checked":
		var p contracts.GateCheckedPayload
		if json.Unmarshal(ev.Payload, &p) != nil {
			return false
		}
		return p.Passed
	case "workflow.milestone":
		var p contracts.WorkflowMilestonePayload
		if json.Unmarshal(ev.Payload, &p) != nil {
			return false
		}
		return p.Passed
	default:
		return false
	}
}
