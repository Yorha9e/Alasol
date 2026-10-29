package host

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
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

// contextStrategyFactory 按 Config.ContextStrategy 造 per-Run 的策略工厂（nil = 不挂 = append）。
//
// 只有 "tiered" 一个实现（其余/空 = append 兜底）。窗口 W 用 Config.ContextStrategyWindow
// （0 = 默认 1M）；W < 256k 硬约束不满足时 NewEngine 返回 error → 此处**拒绝装配**并说明
// （fail-close：不满足设计要求就明说，不静默降级成一个会负预算的策略）。
//
// 参数面（F10）：W / L1% / L2% 全部来自宿主配置（TOML/CLI → host.Config → compaction.Params），
// 不写死；规则文本（Config.ContextRules）也在这里注入（稳定前缀的一部分）。
func (h *Hub) contextStrategyFactory(sessionID, agentID contracts.Ulid) (compaction.AssemblerFactory, error) {
	if strings.TrimSpace(h.cfg.ContextStrategy) != contextStrategyTiered {
		return nil, nil // append（默认）
	}
	params := h.cfg.contextStrategyParams()
	rules := h.cfg.ContextRules
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
		if rules != "" {
			engine.SetRules(rules)
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
// 口径（F17/F18）：
//   - **有界读**：只读指针区间（[fromSeq, toSeq]），不再"读整个 session 再过滤"
//     （那是全量扫描：一次 recall 把整场会话都搬回来）；
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
	events, err := s.store.ReadFrom(ctx, &s.sessionID, fromSeq, int(span))
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
