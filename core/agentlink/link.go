package agentlink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/discovery"
)

// 消息事件 kind（D6 点分命名空间；F7 只增——新增消息形态 = 新 kind，不改这两个）。
//
// 常量值与契约 event.json 的 x-kind-payload 对齐（源头在那，此处是镜像，免得调用方
// 为了写一个字符串去 import contracts）。
const (
	KindBroadcast = "agentlink.broadcast"
	KindHandoff   = "agentlink.handoff"
)

// KindPrefix 是消息 kind 的命名空间前缀（Inbox / FoldMessages 按前缀筛）。
const KindPrefix = "agentlink."

// 投递目标类型（K11 的两个正交维度；与 contracts.AgentLinkTargetType 同值）。
const (
	TargetGate    = "gate"
	TargetSubtree = "subtree"
)

// 收件途径（Recipient.Via / Dropped.Reason 的取值空间）。
const (
	viaGate    = "gate"
	viaSubtree = "subtree"

	// reasonSelf：发送方自己（不给自己投递）。
	reasonSelf = "self"
	// reasonNotLive：候选 session 已结束（G17b：成员关系是活的，死 session 不再收消息）。
	reasonNotLive = "not_live"
	// reasonOutsideSubtree：候选 agent 不在目标子树内（兄弟/旁支不可见，K11②）。
	reasonOutsideSubtree = "outside_subtree"
	// reasonNotVisible：候选 agent **在**目标子树内，但不在发送方的 K11 可见面里
	//（向上只一跳、兄弟永不可见——见 subtreeRecipients 的口径）。
	reasonNotVisible = "not_visible"
	// reasonNoMainAgent：候选 session 还推不出主 agent（没有任何事件的 session）。
	reasonNoMainAgent = "no_main_agent"
)

// 组件错误（调用方可用 errors.Is 判定；全部是 fail-close 类：宁可拒也不猜）。
var (
	// ErrNilLink：*Link 为 nil。
	ErrNilLink = errors.New("agentlink: Link 为 nil")
	// ErrNoReader：未注入事件读取实现（Inquiry / Inbox 需要）。
	ErrNoReader = errors.New("agentlink: 未注入 EventWindowReader（Inquiry/Inbox 无法读事件流）")
	// ErrNoSink：未注入事件落库实现（Broadcast / Handoff 需要）。
	ErrNoSink = errors.New("agentlink: 未注入 EventSink（Broadcast/Handoff 无法落库）")
	// ErrNoTable：未注入发现表（可见性判定 / Presence / Discover 需要）。
	ErrNoTable = errors.New("agentlink: 未注入 discovery.Table（可见性判定无法进行）")
	// ErrBadIdentity：发送方/查询方身份不完整（session_id 与 agent_id 都必须是合法 ULID）。
	ErrBadIdentity = errors.New("agentlink: 身份不完整（session_id 与 agent_id 必填且为合法 ULID）")
	// ErrBadTarget：投递目标不合法（类型不在枚举内 / 与该类型配套的字段缺失）。
	ErrBadTarget = errors.New("agentlink: 投递目标不合法")
	// ErrUnknownAgent：subtree 目标的根 agent 不在发现表里。
	ErrUnknownAgent = errors.New("agentlink: 目标 agent 不在发现表里")
	// ErrCrossSessionSubtree：subtree 目标指向别 session 的 agent（K11：别 session 的
	// subagent 完全不可见——这不是投递范围问题，是越界，写入时就拒）。
	ErrCrossSessionSubtree = errors.New("agentlink: subtree 目标必须是发送方所在 session 的 agent（别 session 的 subagent 完全不可见，K11）")
	// ErrBadQuery：Discover 的查询条件不合法（GateID 与 AgentID 必须二选一）。
	ErrBadQuery = errors.New("agentlink: Discover 查询条件不合法（gate_id 与 agent_id 二选一）")
	// ErrNotVisible：查询方对目标 session 不可见（K11：同 session，或 gate 相交的主 agent）。
	ErrNotVisible = errors.New("agentlink: 查询方对目标 session 不可见（K11）")
	// ErrUnknownSession：目标 session 不在发现表里（事件流里没见过它）。
	ErrUnknownSession = errors.New("agentlink: 目标 session 不在发现表里")
	// ErrEmptyText：消息正文为空（空消息没有信息量，也不该占一条 seq）。
	ErrEmptyText = errors.New("agentlink: 消息正文不能为空")
	// ErrBadPayload：handoff 的结构化载荷不是 JSON 对象。
	ErrBadPayload = errors.New("agentlink: handoff payload 必须是 JSON 对象")
	// ErrNoSummary：核验请求既没给 refs，窗口里也没有 summary（没什么可核验的）。
	ErrNoSummary = errors.New("agentlink: 没有可核验的 refs（请求未指定，窗口内也无 summary）")
	// ErrForeignEvent：落库回读的事件**归属不符**（session_id / agent_id 与发送方不一致）——
	// 说明事件 id 撞上了别人的那条（幂等钥匙没按发送方隔离，或存储层有脏数据）。
	// fail-close：宁可报错，也不把**别人**的事件 id / seq 当成自己这条消息的结果报出去。
	ErrForeignEvent = errors.New("agentlink: 落库回读的事件归属与发送方不一致（事件 id 撞了别人的事件）")
)

// Identity 是消息发送方 / 查询方的身份。
//
// library 形态不绑协议：**身份由调用方给，agentlink 不做认证**（认证在协议层/宿主）。
// 两个字段都必填——envelope 的 session_id/agent_id 是契约必填项，且可见性判定要用它们。
type Identity struct {
	SessionID contracts.Ulid `json:"session_id"`
	AgentID   contracts.Ulid `json:"agent_id"`
}

// valid 报告身份是否完整可用。
func (id Identity) valid() bool {
	return core.ValidUlid(id.SessionID) && core.ValidUlid(id.AgentID)
}

// EventWindow 是事件窗口的 seq 区间（闭区间；ToSeq<=0 = 不设上界）。
//
// seq 是全局单调序号（G9/G10），所以窗口是跨 session 也可比的绝对坐标——summary 的
// refs 用 `e:<seq>` 指向它（K24）。
type EventWindow struct {
	FromSeq int64 `json:"from_seq"`
	ToSeq   int64 `json:"to_seq"`
}

// EventWindowReader 是 Inquiry/Inbox 读事件流的接口（C2/C3：core 定义，宿主注入实现）。
//
// 签名与 core.Store.ReadFrom **一致**：宿主可以直接把 store 传进来，不需要适配层
// （"走既有读路径"的直接落实——Inquiry 读的就是 runtime.ReplaySession 读的那条路）。
type EventWindowReader interface {
	ReadFrom(ctx context.Context, sessionID *contracts.Ulid, fromSeq int64, limit int) ([]contracts.Event, error)
}

// EventSink 是消息落库的出口（C2/C3：core 定义，宿主注入实现；core 不碰 IO）。
//
// 签名与 core.Store.Append 一致，且**返回入库后的最终事件**（seq 已由存储分配，G9/G10）：
// Broadcast/Handoff 的结果要能诚实报告"这条消息落在 seq 几"，调用方（审计/UI）也少一次
// 回读。宿主若要经 bus 走一遍（durable 事件入库成功后通知 WS 订阅者），用
// "发布 + 回读"的适配器——store.Append 按 Event.ID 幂等（S7）且重复 ID 返回原行（含 seq），
// 所以那次回读不会产生第二条事件（host/agentlink.go 有实现）。
type EventSink interface {
	Append(ctx context.Context, events []contracts.Event) ([]contracts.Event, error)
}

// Deps 是 Link 的依赖注入（全部可部分缺失，见 doc.go 的 nil 语义）。
type Deps struct {
	// Table 发现表：Presence / Discover / 可见性判定的唯一数据源（K11 逻辑只在它里面）。
	Table *discovery.Table
	// Reader 事件读取：Inquiry / Inbox 用。
	Reader EventWindowReader
	// Sink 事件落库：Broadcast / Handoff 用。
	Sink EventSink
}

// Link 是 K18 的交流能力组件。**并发安全**：依赖全部不可变（Table 自身有锁），
// Link 没有自己的可变状态——同一个 Link 可被多个 session 并发使用。
type Link struct {
	table  *discovery.Table
	reader EventWindowReader
	sink   EventSink
}

// New 装配一个 Link（永不失败、不 panic；缺失依赖按方法级 fail-close 处理）。
func New(d Deps) *Link {
	return &Link{table: d.Table, reader: d.Reader, sink: d.Sink}
}

// Table 返回注入的发现表（宿主内部/诊断用；不要把它挂到协议面上——
// discovery.Agents()/Agent() 是宿主内部视图，K11 的对外面只有 VisiblePeers）。
func (l *Link) Table() *discovery.Table {
	if l == nil {
		return nil
	}
	return l.table
}

// ---------------------------------------------------------------------------
// 目标（K11 两个维度）与消息形状
// ---------------------------------------------------------------------------

// targetGateID / targetAgentID 取目标里与类型配套的那个字段（另一侧必须是 nil，
// validateTarget 保证）。
func targetGateID(t contracts.AgentLinkTarget) string {
	if t.GateID == nil {
		return ""
	}
	return *t.GateID
}

func targetAgentID(t contracts.AgentLinkTarget) contracts.Ulid {
	if t.AgentID == nil {
		return ""
	}
	return *t.AgentID
}

// validateTarget 校验投递目标（S9：失效即拒，不截断不转义不补零）。
//
// gate 目标：gate_id 必须过 discovery 的校验（与 host.JoinGate 同一把尺）；
// subtree 目标：agent_id 必须是合法 ULID、在发现表里、且**与发送方同 session**
// （K11：别 session 的 subagent 完全不可见）。
func (l *Link) validateTarget(from Identity, t contracts.AgentLinkTarget) error {
	switch t.Type {
	case contracts.AgentLinkTargetTypeGate:
		id := targetGateID(t)
		if _, ok := discovery.NormalizeGateID(id); !ok {
			return fmt.Errorf("%w: gate_id %q 不合法（空 / 超过 %d 字符 / 含空白或控制字符）",
				ErrBadTarget, id, discovery.MaxGateIDLen)
		}
		return nil
	case contracts.AgentLinkTargetTypeSubtree:
		root := targetAgentID(t)
		if !core.ValidUlid(root) {
			return fmt.Errorf("%w: subtree 的 agent_id %q 不是合法 ULID", ErrBadTarget, root)
		}
		if l == nil || l.table == nil {
			return ErrNoTable
		}
		row, ok := l.table.Agent(root)
		if !ok {
			return fmt.Errorf("%w: %s", ErrUnknownAgent, root)
		}
		if contracts.Ulid(row.SessionID) != from.SessionID {
			return fmt.Errorf("%w: agent %s 属于 session %s，发送方在 %s",
				ErrCrossSessionSubtree, root, row.SessionID, from.SessionID)
		}
		return nil
	default:
		return fmt.Errorf("%w: type = %q（只认 gate / subtree）", ErrBadTarget, t.Type)
	}
}

// buildMessageEvent 组一条 agentlink 消息事件（durable=true，F8/G23）。
//
// idempotencyKey 非空 → 事件 id 由它**确定性导出**（core.ShortHash，26 字符 Crockford
// base32 = ULID 形态 ✅）：同一把钥匙重试恒得同一 id → store 的 ON CONFLICT(id) DO NOTHING
// 在存储层兜住重复落库（与 gate.joined 的确定性 id、X5 的 spawn_request_id 同款手法：
// 幂等不靠调用方自觉）。空 → 新 ULID（每次调用就是一条新消息）。
//
// **钥匙按发送方隔离**（F1）：id 的输入含 kind + **发送方 session_id/agent_id** + 钥匙。
// 不隔离的后果很具体：两个不同 sender 用同一把钥匙 → 同一个事件 id → 存储层的
// ON CONFLICT(id) DO NOTHING 把**第二条**静默吃掉，而 MessageResult 会把**别人**那条
// 事件的 id/seq 当成自己的结果报出去（跨 session 消息静默丢失）。隔离后，不同 sender
// 的同钥匙互不影响；同一 sender 的同钥匙仍然是同一条（重试幂等）。
func buildMessageEvent(kind string, from Identity, target contracts.AgentLinkTarget, text string, payload json.RawMessage, idemKey string) (contracts.Event, error) {
	var raw json.RawMessage
	switch kind {
	case KindHandoff:
		b, err := json.Marshal(contracts.AgentLinkHandoffPayload{
			Target:  target,
			Text:    text,
			Payload: payload,
		})
		if err != nil {
			return contracts.Event{}, fmt.Errorf("agentlink: 序列化 %s payload: %w", kind, err)
		}
		raw = b
	default: // KindBroadcast
		b, err := json.Marshal(contracts.AgentLinkBroadcastPayload{Target: target, Text: text})
		if err != nil {
			return contracts.Event{}, fmt.Errorf("agentlink: 序列化 %s payload: %w", kind, err)
		}
		raw = b
	}
	id := core.NewUlid()
	if strings.TrimSpace(idemKey) != "" {
		id = contracts.Ulid(core.ShortHash(messageIDSeed(kind, from, idemKey)))
	}
	return contracts.Event{
		V:         1,
		ID:        id,
		Kind:      kind,
		TS:        time.Now().UTC().Format(time.RFC3339),
		SessionID: from.SessionID,
		AgentID:   from.AgentID,
		Durable:   true, // 消息是事实：入库、占 seq、可回放（F8/G23）
		Payload:   raw,
	}, nil
}

// messageIDSeed 是幂等事件 id 的哈希输入（kind + 发送方 + 钥匙）。
//
// 发送方（session_id + agent_id）**必须**在里面：钥匙是调用方给的短标识（如
// "run-42/broadcast/1"），不同 sender 很可能用同一把——不隔离就会撞 id（F1）。
func messageIDSeed(kind string, from Identity, idemKey string) string {
	return "agentlink:" + kind + ":" + string(from.SessionID) + ":" + string(from.AgentID) + ":" + idemKey
}

// Message 是一条 agentlink 消息的读取视图（Inbox / FoldMessages 的输出）。
type Message struct {
	Kind          string                    `json:"kind"`
	EventID       string                    `json:"event_id"`
	Seq           int64                     `json:"seq"`
	TS            string                    `json:"ts"`
	FromSessionID string                    `json:"from_session_id"`
	FromAgentID   string                    `json:"from_agent_id"`
	Target        contracts.AgentLinkTarget `json:"target"`
	Text          string                    `json:"text"`
	Payload       json.RawMessage           `json:"payload,omitempty"`
}

// decodeMessage 把一条事件解成 Message（kind 不认 / 载荷不可解析 → ok=false，D7 宽容）。
func decodeMessage(ev *contracts.Event) (Message, bool) {
	m := Message{
		Kind:          ev.Kind,
		EventID:       string(ev.ID),
		TS:            ev.TS,
		FromSessionID: string(ev.SessionID),
		FromAgentID:   string(ev.AgentID),
	}
	if ev.Seq != nil {
		m.Seq = *ev.Seq
	}
	switch ev.Kind {
	case KindBroadcast:
		var p contracts.AgentLinkBroadcastPayload
		if json.Unmarshal(ev.Payload, &p) != nil {
			return Message{}, false
		}
		m.Target, m.Text = p.Target, p.Text
	case KindHandoff:
		var p contracts.AgentLinkHandoffPayload
		if json.Unmarshal(ev.Payload, &p) != nil {
			return Message{}, false
		}
		m.Target, m.Text, m.Payload = p.Target, p.Text, p.Payload
	default:
		return Message{}, false
	}
	return m, true
}

// FoldMessages 把事件流折回**全部消息历史**（K18 可重建：广播/交接历史从事件流重放即得）。
//
// 纯函数（无 IO、无锁）：传入的事件应按 seq 升序（如 ReplaySession / ReadFrom 的返回值），
// 输出保持该顺序。未知 kind 与坏载荷跳过（D7 宽容，不中断 fold）。
//
// 典型用法：宿主重启后 `events, _ := runtime.ReplaySession(...)`（或全量 ReadFrom）→
// FoldMessages → 与 Inbox 的实时结果对账（端到端测试就是这么断言的）。
func FoldMessages(events []contracts.Event) []Message {
	out := make([]Message, 0, len(events))
	for i := range events {
		ev := &events[i]
		if !strings.HasPrefix(ev.Kind, KindPrefix) {
			continue
		}
		if m, ok := decodeMessage(ev); ok {
			out = append(out, m)
		}
	}
	return out
}
