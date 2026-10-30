package host

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
)

// scanBatch 是投影单批扫描的事件条数（与索引器默认同量级）。
const scanBatch = 500

// SessionRow 是在线表的一行（GET /api/sessions 的元素形状）。
//
// 口径与索引器的 sessions 投影表一致（first_seq / last_seq / event_count / last_kind），
// 另加 UI 直接要的字段：
//   - updated_at：该 session 最后一条事件的 ts（RFC 3339）——索引器的 sessions 表
//     不存它（表里没有这一列），而"多久没动静"是在线表的第二常用信息；
//   - watermark：**全局**索引水位（indexer.Watermark()），附在每行上省得前端再要一次；
//   - hostable：**本进程**是否托管该 session（= 协议面可对它发 prompt；G12，v3-S5）。
//     一库一 hub 是预期拓扑（G25）：共享库的读面按库投影（别的 hub 建的 session 也
//     在表里——可读），但托管写面只对本进程的 loop 生效（不可发）——此前列表不区分，
//     GUI 只能点下去吃 404。**只增不破坏**（行 JSON 多一个 bool，旧前端可忽略）。
//     填充在 Hub.Sessions（投影不认识 h.loops；见 hub.go）。
type SessionRow struct {
	SessionID  string `json:"session_id"`
	FirstSeq   int64  `json:"first_seq"`
	LastSeq    int64  `json:"last_seq"`
	EventCount int64  `json:"event_count"`
	LastKind   string `json:"last_kind"`
	UpdatedAt  string `json:"updated_at"`
	Watermark  int64  `json:"watermark"`
	Hostable   bool   `json:"hostable"`
}

// sessionState 是投影里单个 session 的累积状态。
type sessionState struct {
	FirstSeq   int64
	LastSeq    int64
	EventCount int64
	LastKind   string
	UpdatedAt  string
	// hubEvents / agentEvents：按**事件族**记账（hub.* = hub 级基础设施事件族）。
	// 两者相除即知"这个 session 是不是 hub 伪 session"（只有 hub 事件、没有 agent 事件
	// → 伪 session → 不进在线表；见 rows）。
	hubEvents   int
	agentEvents int
	// fork 标记本 session 是一条 /btw 旁支 fork（v3-S2 K29；见 btw.forked 事件）。
	// fork session 是**一次性旁支执行线**（结果只回给用户、不回灌主上下文），不是用户在用的
	// 会话 → 不进在线表（与 hub 伪 session 同款：可审计/可回放，但不在"活跃会话"面展示）。
	fork bool
}

// projection 是 host 的**内存投影**：session 目录 + 每个 session 最新的
// context.assembled 快照。见包 doc 的"在线表的数据来源"一节（为什么不用索引器派生表）。
//
// 并发：一把互斥锁保护两张 map 与扫描水位；catchUp 与读方法都走它。
type projection struct {
	mu       sync.Mutex
	scanSeq  int64
	sessions map[contracts.Ulid]*sessionState
	// contexts 存每个 session 最新一条 context.assembled 的载荷（原始 JSON）。
	// 用原始 JSON 而不是解好的结构：HTTP 层直接回写，少一次解码/编码往返，
	// 也不会因为契约加字段而漏字段。
	contexts map[contracts.Ulid]json.RawMessage

	store core.Store
}

func newProjection(store core.Store) *projection {
	return &projection{
		sessions: make(map[contracts.Ulid]*sessionState),
		contexts: make(map[contracts.Ulid]json.RawMessage),
		store:    store,
	}
}

// catchUp 增量扫描事件流，把水位之后的新事件并进投影。返回本批处理条数。
//
// 幂等：扫描水位只前进不后退；同一个事件被扫两次时 LastSeq 守卫会挡住回退
// （EventCount 仍会重复计 —— 所以调用方**不要**在已推过的区间上反复 catchUp；
// 正常用法是"每次从上次的水位继续"，本方法自己保证这一点）。
func (p *projection) catchUp(ctx context.Context) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.catchUpLocked(ctx)
}

func (p *projection) catchUpLocked(ctx context.Context) (int, error) {
	total := 0
	for {
		evs, err := p.store.ReadFrom(ctx, nil, p.scanSeq+1, scanBatch)
		if err != nil {
			return total, err
		}
		if len(evs) == 0 {
			return total, nil
		}
		for i := range evs {
			p.apply(&evs[i])
			if evs[i].Seq != nil && *evs[i].Seq > p.scanSeq {
				p.scanSeq = *evs[i].Seq
			}
		}
		total += len(evs)
		if len(evs) < scanBatch {
			return total, nil
		}
	}
}

// apply 把一条事件并进投影（调用方持有 p.mu）。
func (p *projection) apply(ev *contracts.Event) {
	sid := ev.SessionID
	st, ok := p.sessions[sid]
	if !ok {
		st = &sessionState{}
		p.sessions[sid] = st
	}
	seq := int64(0)
	if ev.Seq != nil {
		seq = *ev.Seq
	}
	if st.FirstSeq == 0 || (seq > 0 && seq < st.FirstSeq) {
		st.FirstSeq = seq
	}
	if seq > st.LastSeq {
		st.LastSeq = seq
		st.LastKind = ev.Kind
		if ev.TS != "" {
			st.UpdatedAt = ev.TS
		}
	}
	st.EventCount++
	// 按事件族记账：hub.* = hub 级基础设施事件（伪 session 的判定依据，见 rows）。
	if isHubEventKind(ev.Kind) {
		st.hubEvents++
	} else {
		st.agentEvents++
	}
	// /btw 旁支 fork 标记（v3-S2 K29）：本 session 是 fork → 不进在线表（见 rows）。
	if ev.Kind == KindBtwForked {
		st.fork = true
	}
	if ev.Kind == loop.KindContextAssembled && len(ev.Payload) > 0 {
		// 最新一条覆盖（LWW，按 seq 顺序天然成立：扫描按 seq 升序）。
		p.contexts[sid] = append(json.RawMessage(nil), ev.Payload...)
	}
}

// rows 输出在线表（按最后活跃倒序，最新的在前 —— 在线表的默认排序应该是
// "刚出事的在最上面"）。watermark 由调用方填（全局索引水位）。
func (p *projection) rows(watermark int64, limit int) []SessionRow {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]SessionRow, 0, len(p.sessions))
	for sid, st := range p.sessions {
		if st.hubEvents > 0 && st.agentEvents == 0 {
			// hub 伪 session（只承载 hub.* 基础设施事件）：在线表只列 agent session。
			// 判据是**事件族**而不是某个 id——派生口径变了也照样挡得住（p2-2）。
			continue
		}
		if st.fork {
			// /btw 旁支 fork（v3-S2 K29）：一次性旁支执行线，结果只回给用户、不进在线表。
			continue
		}
		out = append(out, SessionRow{
			SessionID:  string(sid),
			FirstSeq:   st.FirstSeq,
			LastSeq:    st.LastSeq,
			EventCount: st.EventCount,
			LastKind:   st.LastKind,
			UpdatedAt:  st.UpdatedAt,
			Watermark:  watermark,
		})
	}
	// 排序：LastSeq 降序，同 seq 按 session_id 升序（确定性，避免 UI 抖动）。
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && lessRow(out[j], out[j-1]); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// lessRow 定义在线表排序：LastSeq 降序 → EventCount 降序 → SessionID 升序。
func lessRow(a, b SessionRow) bool {
	if a.LastSeq != b.LastSeq {
		return a.LastSeq > b.LastSeq
	}
	if a.EventCount != b.EventCount {
		return a.EventCount > b.EventCount
	}
	return a.SessionID < b.SessionID
}

// snapshot 取某 session 最新的 context.assembled 快照（不存在 → ok=false）。
func (p *projection) snapshot(sid contracts.Ulid) (json.RawMessage, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	raw, ok := p.contexts[sid]
	if !ok || len(raw) == 0 {
		return nil, false
	}
	return raw, true
}

// scanWatermark 返回投影已扫到的全局 seq（诊断用）。
func (p *projection) scanWatermark() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.scanSeq
}
