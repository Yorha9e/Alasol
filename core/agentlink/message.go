package agentlink

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// BroadcastRequest 是一次广播（K18 Broadcast）。
type BroadcastRequest struct {
	// From 发送方身份（envelope 的 session_id / agent_id）。
	From Identity
	// Target 投递目标（K11：gate = 水平维，subtree = 垂直维）。
	Target contracts.AgentLinkTarget
	// Text 消息正文（必填）。
	Text string
	// IdempotencyKey 幂等钥匙：非空 → 事件 id 由它确定性导出（重试不重复落库，
	// 存储层 ON CONFLICT(id) DO NOTHING 兜住，S7/X5 同款手法）；空 → 每次一条新消息。
	IdempotencyKey string
}

// HandoffRequest 是一次结构化交接（K18 Handoff）。
type HandoffRequest struct {
	From   Identity
	Target contracts.AgentLinkTarget
	Text   string
	// Payload 结构化交接载荷（开放 JSON 对象：工作流自定义形状，agentlink 不解释它）。
	Payload json.RawMessage
	// IdempotencyKey 同 BroadcastRequest。
	IdempotencyKey string
}

// Recipient 是一个收件方（K11 过滤后的投递对象）。
type Recipient struct {
	SessionID string `json:"session_id"`
	// AgentID 是实际收消息的 agent：gate 目标 = 对端 session 的**主 agent**（K11①），
	// subtree 目标 = 子树内的 agent（K11②）。
	AgentID string `json:"agent_id"`
	// Via 是投递途径（gate / subtree）。
	Via string `json:"via"`
	// Gates 是命中的 gate（Via=gate 时给出；字典序）。
	Gates []string `json:"gates,omitempty"`
}

// Dropped 是一个被可见性挡掉的候选（审计用：写时算一次的"谁没收到、为什么"）。
type Dropped struct {
	SessionID string `json:"session_id"`
	AgentID   string `json:"agent_id,omitempty"`
	// Reason 取值：self / not_live / no_main_agent / outside_subtree。
	Reason string `json:"reason"`
}

// MessageResult 是一条消息的落库结果。
type MessageResult struct {
	Kind    string `json:"kind"`
	EventID string `json:"event_id"`
	// Seq 是存储分配的全局序号（G9/G10）——消息在唯一事实流里的绝对坐标。
	Seq int64  `json:"seq"`
	TS  string `json:"ts"`
	// Recipients 是 K11 过滤后的收件方（字典序：session_id → agent_id，确定性输出）。
	Recipients []Recipient `json:"recipients"`
	// Dropped 是被挡掉的候选（含原因；只记"本来看得见却没收到的"，不记从不可见的）。
	Dropped []Dropped `json:"dropped,omitempty"`
}

// Broadcast 向 gate / 子树广播一条消息（K18 Broadcast）。
//
// 投递语义 = **写进事件流 + 可见者可查询**（拉模型，见 doc.go）：本方法只落一条 durable
// 事件并回报"此刻按 K11 谁能收到"，不推送、不建投递记录。收件方用 Inbox 拉。
func (l *Link) Broadcast(ctx context.Context, req BroadcastRequest) (MessageResult, error) {
	return l.send(ctx, KindBroadcast, req.From, req.Target, req.Text, nil, req.IdempotencyKey)
}

// Handoff 做一次结构化交接（K18 Handoff）：broadcast + 一个开放 payload。
func (l *Link) Handoff(ctx context.Context, req HandoffRequest) (MessageResult, error) {
	if len(req.Payload) > 0 && json.Unmarshal(req.Payload, &map[string]json.RawMessage{}) != nil {
		return MessageResult{}, fmt.Errorf("%w: %s", ErrBadPayload, string(req.Payload))
	}
	return l.send(ctx, KindHandoff, req.From, req.Target, req.Text, req.Payload, req.IdempotencyKey)
}

// send 是 Broadcast/Handoff 的共同写路径（校验 → 算收件方 → 落库 → 回报）。
func (l *Link) send(
	ctx context.Context,
	kind string,
	from Identity,
	target contracts.AgentLinkTarget,
	text string,
	payload json.RawMessage,
	idemKey string,
) (MessageResult, error) {
	var zero MessageResult
	if l == nil {
		return zero, ErrNilLink
	}
	if !from.valid() {
		return zero, fmt.Errorf("%w: from=%+v", ErrBadIdentity, from)
	}
	if l.sink == nil {
		return zero, ErrNoSink
	}
	if strings.TrimSpace(text) == "" {
		return zero, ErrEmptyText
	}
	if err := l.validateTarget(from, target); err != nil {
		return zero, err
	}

	recips, dropped := l.delivery(from, target)
	ev, err := buildMessageEvent(kind, from, target, text, payload, idemKey)
	if err != nil {
		return zero, err
	}
	final, err := l.sink.Append(ctx, []contracts.Event{ev})
	if err != nil {
		return zero, fmt.Errorf("agentlink: 落 %s 事件: %w", kind, err)
	}
	if len(final) == 0 {
		// store 违约（没回报已入库的事件）：不猜 seq，明确报错。
		return zero, fmt.Errorf("agentlink: 存储未回报已入库的 %s 事件（id=%s）", kind, ev.ID)
	}
	// 回读校验（F1）：落库回来的那条必须**属于本次发送方**。
	//
	// 事件 id 撞上别人的那条时，存储层的 ON CONFLICT(id) DO NOTHING 会把这条静默吃掉，
	// 回读给出的是**别人**的事件——不校验就会把别人的 id/seq 当成自己的结果报出去
	// （跨 session 消息静默丢失）。fail-close：报错，不猜。
	if final[0].SessionID != from.SessionID || final[0].AgentID != from.AgentID {
		return zero, fmt.Errorf("%w: id=%s session=%s agent=%s（期望 session=%s agent=%s）",
			ErrForeignEvent, final[0].ID, final[0].SessionID, final[0].AgentID, from.SessionID, from.AgentID)
	}
	out := MessageResult{
		Kind:       kind,
		EventID:    string(final[0].ID),
		Recipients: recips,
		Dropped:    dropped,
	}
	if final[0].Seq != nil {
		out.Seq = *final[0].Seq
	}
	out.TS = final[0].TS
	return out, nil
}

// delivery 按 K11 算一条消息的收件方与被挡掉的候选（写时算一次，供审计/UI）。
//
// gate 目标：候选 = join 过该 gate 的**全部** session（历史成员，含已结束的）；
// 收件方 = 活跃成员 ∩ 非发送方，取各自的**主 agent**（K11①：水平粒度是 session 主 agent）。
//
// subtree 目标：候选 = 发送方 session 的**全部** agent；
// 收件方 = **目标子树 ∩ 发送方的 K11 可见集**（见 subtreeRecipients：兄弟永远收不到、
// 向上只一跳、向下覆盖整棵子树）；
// 被挡掉的候选带原因：self（发送方自己）/ outside_subtree（不在目标子树里）/
// not_visible（在子树里但不在发送方的可见面里）。
// 别 session 的 agent 根本不在候选里——跨 session 子树在 validateTarget 就已被拒。
func (l *Link) delivery(from Identity, target contracts.AgentLinkTarget) ([]Recipient, []Dropped) {
	recips := []Recipient{}
	dropped := []Dropped{}
	if l == nil || l.table == nil {
		return recips, dropped
	}
	switch target.Type {
	case contracts.AgentLinkTargetTypeGate:
		gateID := targetGateID(target)
		for _, g := range l.table.Gates() {
			if g.GateID != gateID {
				continue
			}
			for _, sid := range g.Members {
				s := contracts.Ulid(sid)
				if s == from.SessionID {
					dropped = append(dropped, Dropped{SessionID: sid, Reason: reasonSelf})
					continue
				}
				if !l.table.IsLive(s) {
					dropped = append(dropped, Dropped{SessionID: sid, Reason: reasonNotLive})
					continue // G17b：成员关系是活的，死 session 不再收消息
				}
				main, ok := l.table.PrimaryAgent(s)
				if !ok {
					dropped = append(dropped, Dropped{SessionID: sid, Reason: reasonNoMainAgent})
					continue
				}
				recips = append(recips, Recipient{
					SessionID: sid,
					AgentID:   string(main),
					Via:       viaGate,
					Gates:     []string{gateID},
				})
			}
			break
		}
	case contracts.AgentLinkTargetTypeSubtree:
		root := targetAgentID(target)
		// 收件方 = 目标子树 ∩ 发送方的 K11 可见集（见 subtreeRecipients 的口径：
		// 兄弟永远不可见；向上只一跳；向下覆盖整棵子树）。
		recipsSet := l.subtreeRecipients(from, root)
		for _, a := range l.table.Agents(from.SessionID) {
			id := contracts.Ulid(a.AgentID)
			if id == from.AgentID {
				dropped = append(dropped, Dropped{SessionID: string(from.SessionID), AgentID: a.AgentID, Reason: reasonSelf})
				continue
			}
			if !recipsSet[id] {
				reason := reasonOutsideSubtree
				if l.inSubtreeOf(from.SessionID, root, id) {
					// 在目标子树里，但不在发送方的可见面里（兄弟 / 旁支 / 隔代祖先方向）。
					reason = reasonNotVisible
				}
				dropped = append(dropped, Dropped{SessionID: string(from.SessionID), AgentID: a.AgentID, Reason: reason})
				continue
			}
			recips = append(recips, Recipient{
				SessionID: string(from.SessionID),
				AgentID:   a.AgentID,
				Via:       viaSubtree,
			})
		}
	}
	sortRecipients(recips)
	sortDropped(dropped)
	return recips, dropped
}

func sortRecipients(rows []Recipient) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].SessionID != rows[j].SessionID {
			return rows[i].SessionID < rows[j].SessionID
		}
		return rows[i].AgentID < rows[j].AgentID
	})
}

func sortDropped(rows []Dropped) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].SessionID != rows[j].SessionID {
			return rows[i].SessionID < rows[j].SessionID
		}
		return rows[i].AgentID < rows[j].AgentID
	})
}

// ---------------------------------------------------------------------------
// Inbox：拉模型的那一半（消息已写在事件流里，收件方按 K11 自己筛）
// ---------------------------------------------------------------------------

// InboxScanBatch 是 Inbox 分批扫事件流的批大小（store 的 ReadFrom 只有 session 过滤、
// 没有 kind 过滤，所以只能顺序扫；分批是为了别把整条流一次读进内存——F6：
// **逐批过滤、不留全量**，只有真正投递给查询方的消息才留在结果里）。
//
// 导出：调用方（与回归测试）据此判断"扫描是分批的、内存占用有界"。
const InboxScanBatch = 256

// InboxQuery 是收件箱查询。
type InboxQuery struct {
	// SessionID 查询方 session（必填）。
	SessionID contracts.Ulid
	// AgentID 非空 → 只回投递给**该 agent**的消息：
	// gate 消息的收件方是对端主 agent（K11①），所以非主 agent 的 AgentID 看不到 gate 消息
	// （别 session 的 subagent 完全不可见的直接后果）；subtree 消息按收件方集合过滤
	// （与写时同一口径：兄弟收不到）。
	AgentID contracts.Ulid
	// SinceSeq 非零 → 只回 seq 严格大于它的消息（增量拉取；配合 InboxResult.NextSeq 游标）。
	SinceSeq int64
	// Limit 条数上限（<=0 不限）：作用于**交付给查询方的条数**（不是"读了多少条事件"）。
	Limit int
}

// InboxResult 是 Inbox 的返回（消息 + 游标）。
type InboxResult struct {
	// Messages 是投递给查询方的消息（按 seq 升序）。
	Messages []Message `json:"messages"`
	// NextSeq 是本次扫描到的**位置**（全局 seq，保守下界、**只增不减**）：调用方拿它
	// 推进游标（下次 InboxQuery.SinceSeq = NextSeq），不必再靠"收到的最后一条消息"反推
	//——那条消息可能根本没投给自己（别人的消息 / 不可见的消息），游标会卡住。
	// 追平态（这轮没扫到新事件）也回填它：归零会让调用方退回 0 → 从头重扫 →
	// 已投的消息重复投递、永不收敛（#2）。
	NextSeq int64 `json:"next_seq"`
	// HasMore 标记扫描**提前收手**（撞到 Limit）且后面还有事件：可能还有更多消息，
	// 用 NextSeq 继续拉。false = 已经扫到流尾（本轮拉干净了）。
	HasMore bool `json:"has_more"`
}

// Inbox 拉"发给我这个消息的"广播/交接（K18 拉模型投递的另一半）。
//
// 读全局流（消息可能写在任何 session 里）→ 按 kind 前缀筛 → 按 K11 过滤：
//
//	gate 目标    查询方 session 是该 gate 的**活跃成员**（K12/G17b）且不是发送方；
//	             若指定了 AgentID，还要求它是查询方 session 的主 agent（K11①）；
//	subtree 目标 查询方 session 必须就是消息所在 session（跨 session 子树不可见），
//	             且它必须在该消息的**收件方集合**里（与写时同一口径：兄弟收不到）。
//
// Limit 的语义是"最多多少条**投递给查询方的消息**"——**按交付条数算**，不是按扫到的
// 消息条数算（F5）：流里全是别人的消息时也要继续扫，直到凑够 limit 或扫到流尾。
// 否则噪声消息会占掉额度，真消息永远拉不到。
//
// **流式扫描**（F6）：逐批读、逐条过滤，不把整条流攒在内存里（只有交付出去的消息
// 才留在结果里）。
//
// 成员关系在**读时**判定（与写时的 Recipients 可能不同，见 doc.go：拉模型下这正当且有用
// ——后来 join 同一 gate 的 peer 拉得到更早的广播）。
func (l *Link) Inbox(ctx context.Context, q InboxQuery) (InboxResult, error) {
	// NextSeq 从 q.SinceSeq 起步：**只增不减**（#2）。追平态（这轮没扫到新事件）也回填
	// 它——否则 next_seq 归零 → 调用方按文档"SinceSeq=NextSeq"轮询会退回 0 → 从头重扫
	// → 已投的消息重复投递、永不收敛。
	res := InboxResult{Messages: []Message{}, NextSeq: q.SinceSeq}
	if l == nil {
		return res, ErrNilLink
	}
	if !core.ValidUlid(q.SessionID) {
		return res, fmt.Errorf("%w: session_id %q 不是合法 ULID", ErrBadIdentity, q.SessionID)
	}
	if l.reader == nil {
		return res, ErrNoReader
	}
	fromSeq := int64(1)
	if q.SinceSeq > 0 {
		fromSeq = q.SinceSeq + 1
	}
	cursor := fromSeq
	for {
		batch := InboxScanBatch
		if q.Limit > 0 {
			if want := q.Limit * 4; want > batch {
				batch = want
			}
		}
		evs, err := l.reader.ReadFrom(ctx, nil, cursor, batch)
		if err != nil {
			return res, fmt.Errorf("agentlink: 读事件流（inbox）: %w", err)
		}
		if len(evs) == 0 {
			return res, nil // 流尾（追平）：NextSeq 保持 q.SinceSeq（只增不减）
		}
		stop := false
		for i := range evs {
			ev := &evs[i]
			if !strings.HasPrefix(ev.Kind, KindPrefix) {
				continue
			}
			m, ok := decodeMessage(ev)
			if !ok {
				continue // 坏载荷：跳过（D7 宽容），不当成一条消息
			}
			if !l.addressedTo(m, q) {
				continue
			}
			res.Messages = append(res.Messages, m)
			res.NextSeq = advanceCursor(res.NextSeq, m.Seq)
			if q.Limit > 0 && len(res.Messages) >= q.Limit {
				stop = true
				break
			}
		}
		if stop {
			// 撞到 limit：探一格看后面还有没有事件（诚实报告 has_more）。
			res.HasMore = l.moreAfter(ctx, res.NextSeq)
			return res, nil
		}
		last := evs[len(evs)-1]
		if last.Seq == nil {
			return res, nil // 没有 seq 就无法定位游标（不该发生：durable 必有 seq）
		}
		res.NextSeq = advanceCursor(res.NextSeq, *last.Seq)
		cursor = *last.Seq + 1
		if len(evs) < batch {
			return res, nil // 这批不满 → 流尾
		}
	}
}

// advanceCursor 把扫描位置往前推（**只增不减**，#2）：追平态（这轮没扫到新事件）
// 也不能把游标退回 0——调用方按"SinceSeq=NextSeq"轮询，退回 0 就从头重扫，
// 已投的消息会重复投递。
func advanceCursor(cur, next int64) int64 {
	if next > cur {
		return next
	}
	return cur
}

// moreAfter 报告 seq 之后还有没有事件（has_more 的探针；读失败当作"没有"——
// 宁可不报"还有"，也不让调用方无限轮询）。
func (l *Link) moreAfter(ctx context.Context, seq int64) bool {
	if seq <= 0 || l.reader == nil {
		return false
	}
	evs, err := l.reader.ReadFrom(ctx, nil, seq+1, 1)
	if err != nil {
		return false
	}
	return len(evs) > 0
}

// addressedTo 按 K11 判定一条消息是否投递给了查询方（Inbox 的过滤内核）。
func (l *Link) addressedTo(m Message, q InboxQuery) bool {
	if l == nil || l.table == nil {
		return false
	}
	if contracts.Ulid(m.FromSessionID) == q.SessionID {
		// gate 消息：发送方 session 虽是该 gate 的成员，但不给自己的 session 投递。
		// （subtree 消息的收件方与发送方同 session——那条路在下面按 agent 判，见函数开头的自判。）
		if m.Target.Type == contracts.AgentLinkTargetTypeGate {
			return false
		}
	}
	if q.AgentID != "" && contracts.Ulid(m.FromAgentID) == q.AgentID {
		return false // 不给自己投递：发送方那个 agent 不收自己发的话
	}
	switch m.Target.Type {
	case contracts.AgentLinkTargetTypeGate:
		gateID := targetGateID(m.Target)
		if gateID == "" {
			return false
		}
		// 活跃成员（G17b：死 session 不再收消息）。
		member := false
		for _, s := range l.table.GateMembers(gateID) {
			if contracts.Ulid(s.SessionID) == q.SessionID {
				member = true
				break
			}
		}
		if !member {
			return false
		}
		if q.AgentID == "" {
			return true // session 级查询：主 agent 的视角
		}
		// K11①：水平可见性的收件方是对端 session 的**主 agent**——subagent 收不到。
		main, ok := l.table.PrimaryAgent(q.SessionID)
		return ok && main == q.AgentID
	case contracts.AgentLinkTargetTypeSubtree:
		root := targetAgentID(m.Target)
		if root == "" || contracts.Ulid(m.FromSessionID) != q.SessionID {
			// 跨 session 的子树消息对别 session 完全不可见（K11）。
			return false
		}
		// 收件方口径与**写时**完全一致（subtreeRecipients）：兄弟永远收不到，
		// 向上只一跳，向下覆盖整棵子树。
		recips := l.subtreeRecipients(Identity{
			SessionID: contracts.Ulid(m.FromSessionID),
			AgentID:   contracts.Ulid(m.FromAgentID),
		}, root)
		if q.AgentID == "" {
			// session 级查询：这条消息在本 session 里**有**合法收件方才算数。
			return len(recips) > 0
		}
		return recips[q.AgentID]
	default:
		return false // 未知目标类型：fail-close
	}
}
