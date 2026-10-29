package host

// 本文件是 hub 与 core/agentlink（K18 五件套）之间的装配层：**注入读/写实现 + 绑 HTTP**。
//
// 分层口径（C2/C3、09）：core/agentlink 是纯能力组件（不做 IO），本包只做三件事——
//
//  ① 注入 reader：直接传 store（core.Store.ReadFrom 与 agentlink.EventWindowReader 同签名，
//      Inquiry/Inbox 走的就是 runtime.ReplaySession 那条既有读路径，不加适配层）；
//  ② 注入 sink：经 bus 发布（durable 事件入库成功后通知 WS 订阅者，在线表/UI 才看得到），
//     再按同一批 id 回读一次拿 seq（见 agentlinkSink）；
//  ③ 绑协议：HTTP 端点（library 不绑协议，协议面在这里）。
//
// 与 host/discovery.go 的分工：那个文件管"事件流 → 发现表"的扫描与 gate；本文件管
// "发现表 + 事件流 → 交流能力"。agentlink 自己**不做 IO**，所以 hub 之外的宿主
// （TUI / CLI / 别的 daemon）可以直接拿 Hub.Link() 用同一个 Link。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
)

// maxAgentLinkBody 限制 agentlink 写端点的请求体大小（loopback 上也别裸奔）。
const maxAgentLinkBody = 64 << 10

// agentlinkSink 把 bus + store 适配成 agentlink.EventSink。
//
// 为什么不是直接把 store 传进去：消息要经 bus 走一遍——durable 事件**整批入库成功后**
// 才通知 WS 订阅者（bus 的口径），在线表/UI 才看得到这条消息。而 bus.Publish 只回 error、
// 不回 seq，agentlink 的结果却要诚实报告"这条消息落在 seq 几"。
//
// store.Append 按 Event.ID 幂等（S7）且**重复 ID 返回原行（含 seq）**——所以"先经 bus
// 发布、再按同一批 id 回读一次"即可拿到最终事件：那次回读不会产生第二条事件
// （存储层 ON CONFLICT(id) DO NOTHING 兜住，与 gate.joined 的确定性 id 同款手法）。
type agentlinkSink struct {
	bus   busPublisher
	store core.Store
}

// busPublisher 是 bus.Bus 的写面（只为可测性收窄成接口；实现就是 *bus.Bus）。
type busPublisher interface {
	Publish(ctx context.Context, evs ...contracts.Event) error
}

func (s agentlinkSink) Append(ctx context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	if err := s.bus.Publish(ctx, evs...); err != nil {
		return nil, err
	}
	return s.store.Append(ctx, evs)
}

// Link 返回装配好的 agentlink 组件（K18 五件套）。宿主内部 / 协议面共用同一个实例：
// 它只持有不可变依赖（发现表 + store + sink），并发安全。
func (h *Hub) Link() *agentlink.Link {
	if h == nil {
		return nil
	}
	return h.link
}

// agentlinkCaller 校验并取出一次 agentlink 调用的调用方身份（K11 判定要用）。
//
// 只认**本 hub 托管的 session**：与 Peers/JoinGate 同口径（ErrSessionNotHosted）——
// 不放任野 session id 进事件流（给它落一条 broadcast 就是往唯一事实源里写垃圾）。
// agent_id 缺省时退到该 session 的主 agent（协议层只传 session_id 的常见形态）。
//
// 给定的 agent_id 必须是**该 session 自己的** agent（主 agent 或它的 subagent）——
// 否则就是冒用别处 agent 的身份（可见性判定会拿它去比树，理应判不可见；这里提前拒，
// 免得"猜一个别人的 agent id"混过某条只看 session 的判定）。
func (h *Hub) agentlinkCaller(sessionID contracts.Ulid, agentID contracts.Ulid) (agentlink.Identity, error) {
	if h == nil {
		return agentlink.Identity{}, errors.New("host: Hub 为 nil")
	}
	if !core.ValidUlid(sessionID) {
		return agentlink.Identity{}, fmt.Errorf("host: session_id 不是合法 ULID（26 字符 Crockford base32）: %q", sessionID)
	}
	if !h.hostsSession(sessionID) {
		return agentlink.Identity{}, fmt.Errorf("%w: %s", ErrSessionNotHosted, sessionID)
	}
	if agentID == "" {
		main, ok := h.mainAgentOf(sessionID)
		if !ok {
			return agentlink.Identity{}, fmt.Errorf("host: session %s 没有主 agent（先跑一轮 prompt）", sessionID)
		}
		agentID = main
	}
	if !core.ValidUlid(agentID) {
		return agentlink.Identity{}, fmt.Errorf("host: agent_id 不是合法 ULID: %q", agentID)
	}
	if !h.agentOfSession(sessionID, agentID) {
		return agentlink.Identity{}, fmt.Errorf("host: agent %s 不属于 session %s", agentID, sessionID)
	}
	return agentlink.Identity{SessionID: sessionID, AgentID: agentID}, nil
}

// agentOfSession 报告 agent 是否是该 session 自己的 agent（主 agent 或 subagent）。
//
// 依据是**发现表**（agent.spawned 折出来的树）——主 agent 也算：它没作为任何
// agent.spawned 的 child 出现，但它自己在表里。
func (h *Hub) agentOfSession(sessionID, agentID contracts.Ulid) bool {
	if h == nil {
		return false
	}
	if main, ok := h.mainAgentOf(sessionID); ok && main == agentID {
		return true
	}
	row, ok := h.disc.Agent(agentID)
	return ok && contracts.Ulid(row.SessionID) == sessionID
}

// Presence 查一个 session 的在线画像 + 水平可见对端（K18 Presence）。
//
// session 必须由本 hub 托管（与 /api/peers 同口径，ErrSessionNotHosted）：查询方身份
// 就是这个 session 的主 agent——**没有调用方身份就没有可见性过滤**（F4：任意客户端
// 不得枚举任意 session 的画像与 gate 列表）。
func (h *Hub) Presence(ctx context.Context, sessionID contracts.Ulid) (agentlink.PresenceReport, error) {
	if h == nil {
		return agentlink.PresenceReport{}, errors.New("host: Hub 为 nil")
	}
	if !core.ValidUlid(sessionID) {
		return agentlink.PresenceReport{}, fmt.Errorf("host: session_id 不是合法 ULID: %q", sessionID)
	}
	// 先扫平：刚 join 的 gate 要立刻可见（与 Peers 同口径）。
	if _, err := h.discFeed.catchUp(ctx); err != nil {
		return agentlink.PresenceReport{}, fmt.Errorf("host: 扫事件流: %w", err)
	}
	id, err := h.agentlinkCaller(sessionID, "")
	if err != nil {
		return agentlink.PresenceReport{}, err
	}
	rep, err := h.link.Presence(id, sessionID)
	if err != nil {
		return rep, err
	}
	// 剔掉 /btw 旁支 fork（v3-S2 K29）：旁支不是一等会话——presence 面（谁在线）
	// 与在线表（在线表面）必须同一个口径，不能一个看得见一个看不见。
	rep.Peers = h.filterForkPeerPresence(rep.Peers)
	return rep, nil
}

// Discover 按 gate / 按 agent 树查找（K18 Discover）。
//
// q.Caller 是查询方身份（必填）：必须由本 hub 托管，且 agent 必须属于该 session。
func (h *Hub) Discover(ctx context.Context, q agentlink.DiscoverQuery) (agentlink.DiscoverResult, error) {
	if h == nil {
		return agentlink.DiscoverResult{}, errors.New("host: Hub 为 nil")
	}
	if _, err := h.discFeed.catchUp(ctx); err != nil {
		return agentlink.DiscoverResult{}, fmt.Errorf("host: 扫事件流: %w", err)
	}
	id, err := h.agentlinkCaller(q.Caller.SessionID, q.Caller.AgentID)
	if err != nil {
		return agentlink.DiscoverResult{}, err
	}
	q.Caller = id
	res, err := h.link.Discover(q)
	if err != nil {
		return res, err
	}
	// 剔掉 /btw 旁支 fork（v3-S2 K29）：旁支不是一等会话，不该出现在成员面。
	res.Sessions = h.filterForkSessionPresence(res.Sessions)
	// **Agents 面同样要剔**（N7）：agent 行带 session_id——只滤 Sessions 会留下
	// "成员里没有它、agent 树里却有它"的自相矛盾（agentlink 的 Discover 按 agent_id
	// 查时会回 agent 行）。
	res.Agents = h.filterForkAgentPresence(res.Agents)
	return res, nil
}

// Inquiry 深入查询一个 session（K18 Inquiry / Q6）：结构化摘要 + 事件窗口 + refs 核验。
//
// caller 必须由本 hub 托管（见 agentlinkCaller）；被查 session 可以是别的 hub 进程在跑的
// （共享事件库时，发现表里有它就行）——跨 session 上查正是 Inquiry 的存在理由（K27）。
func (h *Hub) Inquiry(ctx context.Context, caller agentlink.Identity, req agentlink.InquiryRequest) (agentlink.InquiryResult, error) {
	if h == nil {
		return agentlink.InquiryResult{}, errors.New("host: Hub 为 nil")
	}
	// 先扫平：窗口要包含刚刚发生的事（Inquiry 是"拉原文核验"，晚一条就可能核不上）；
	// 也让调用方身份校验（agent_id 属于该 session）看得见最新的 agent 树。
	if _, err := h.discFeed.catchUp(ctx); err != nil {
		return agentlink.InquiryResult{}, fmt.Errorf("host: 扫事件流: %w", err)
	}
	id, err := h.agentlinkCaller(caller.SessionID, caller.AgentID)
	if err != nil {
		return agentlink.InquiryResult{}, err
	}
	req.Caller = id
	return h.link.Inquiry(ctx, req)
}

// Broadcast 向 gate / 子树广播（K18 Broadcast）：写一条 durable 事件，可见者拉得到。
//
// 先扫平发现表（F9）：收件方是**按当前成员关系**算出来的（写时算一次，见 doc.go）——
// 刚 join 的 gate / 刚 spawn 的 subagent 必须立刻出现在收件方里，否则审计里的
// Recipients 与读时（Inbox）看到的对不上。
func (h *Hub) Broadcast(ctx context.Context, caller agentlink.Identity, req agentlink.BroadcastRequest) (agentlink.MessageResult, error) {
	if h == nil {
		return agentlink.MessageResult{}, errors.New("host: Hub 为 nil")
	}
	if _, err := h.discFeed.catchUp(ctx); err != nil {
		return agentlink.MessageResult{}, fmt.Errorf("host: 扫事件流: %w", err)
	}
	id, err := h.agentlinkCaller(caller.SessionID, caller.AgentID)
	if err != nil {
		return agentlink.MessageResult{}, err
	}
	req.From = id
	return h.link.Broadcast(ctx, req)
}

// Handoff 做一次结构化交接（K18 Handoff）。
func (h *Hub) Handoff(ctx context.Context, caller agentlink.Identity, req agentlink.HandoffRequest) (agentlink.MessageResult, error) {
	if h == nil {
		return agentlink.MessageResult{}, errors.New("host: Hub 为 nil")
	}
	if _, err := h.discFeed.catchUp(ctx); err != nil {
		return agentlink.MessageResult{}, fmt.Errorf("host: 扫事件流: %w", err)
	}
	id, err := h.agentlinkCaller(caller.SessionID, caller.AgentID)
	if err != nil {
		return agentlink.MessageResult{}, err
	}
	req.From = id
	return h.link.Handoff(ctx, req)
}

// Inbox 拉"发给我这个消息的"广播/交接（K18 拉模型投递的另一半）。
//
// 不要求调用方受本 hub 托管：任何拿得到 session id 的一方都能拉自己的可见消息
// （读路径不写事件流，没有"放野 session 进事实源"的风险；可见性由 agentlink 按 K11 判）。
// 先扫平发现表：成员关系是**读时**判定的（K11），表没追平就会漏掉本该收得到的消息。
func (h *Hub) Inbox(ctx context.Context, q agentlink.InboxQuery) (agentlink.InboxResult, error) {
	if h == nil {
		return agentlink.InboxResult{}, errors.New("host: Hub 为 nil")
	}
	if _, err := h.discFeed.catchUp(ctx); err != nil {
		return agentlink.InboxResult{}, fmt.Errorf("host: 扫事件流: %w", err)
	}
	return h.link.Inbox(ctx, q)
}

// ---------------------------------------------------------------------------
// HTTP 端点（协议面：library 不绑协议，绑定在这里）
// ---------------------------------------------------------------------------

// serveAgentlinkPresence 服务 GET /api/presence?session_id=<ulid>（K18 Presence）。
func (h *Hub) serveAgentlinkPresence(w http.ResponseWriter, r *http.Request) {
	sid, err := parseSessionID(r.URL.Query().Get("session_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_session_id", err.Error())
		return
	}
	rep, err := h.Presence(r.Context(), sid)
	if err != nil {
		writeHubError(w, err)
		return
	}
	writeJSONBody(w, http.StatusOK, rep)
}

// serveAgentlinkDiscover 服务 GET /api/discover?session_id=&gate_id=|agent_id=
// （K18 Discover）。
//
// session_id 是**调用方**（查询者）的 session（必须受本 hub 托管）：没有调用方身份就
// 没有可见性过滤——任意客户端不得枚举任意 gate 的成员或任意 session 的 agent 树（F4）。
func (h *Hub) serveAgentlinkDiscover(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	caller, err := parseSessionID(q.Get("session_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_session_id", err.Error())
		return
	}
	res, err := h.Discover(r.Context(), agentlink.DiscoverQuery{
		Caller:  agentlink.Identity{SessionID: caller},
		GateID:  strings.TrimSpace(q.Get("gate_id")),
		AgentID: contracts.Ulid(strings.TrimSpace(q.Get("agent_id"))),
		Subtree: q.Get("subtree") == "1" || strings.EqualFold(q.Get("subtree"), "true"),
	})
	if err != nil {
		writeHubError(w, err)
		return
	}
	writeJSONBody(w, http.StatusOK, res)
}

// defaultAgentlinkInquiryLimit 是 Inquiry 的 HTTP **默认**窗口条数（F6）。
//
// 事件窗口是"要进模型上下文"的东西，不给上限就等于把整个 session 塞给调用方。
// 想要更多就显式传 limit——超出会报 truncated（"没取完"的显式信号）。
const defaultAgentlinkInquiryLimit = 200

// serveAgentlinkInquiry 服务 GET /api/inquiry?session_id=&as_session=&agent_id=
// &project_agent_id=&from=&to=&limit=&refs=（K18 Inquiry / Q6：结构化摘要 + 事件窗口；
// refs 以逗号分隔，可核验）。
//
// 参数分工（F8）：
//
//	session_id        被查的 session（目标）；
//	as_session        调用方 session（缺省 = 被查 session 自己：同 session 恒可见）；
//	agent_id          调用方**身份**（缺省 = 调用方 session 的主 agent）——只是"我是谁"，
//	                  不要再拿它当投影参数；
//	project_agent_id  投影到哪个 agent（缺省 = 调用方的可见集：自己 + 父 + 直系子，K11②）；
//	from/to/limit     窗口区间与条数上限（limit 缺省 = defaultAgentlinkInquiryLimit）。
func (h *Hub) serveAgentlinkInquiry(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	sid, err := parseSessionID(q.Get("session_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_session_id", err.Error())
		return
	}
	caller := agentlink.Identity{SessionID: sid}
	if raw := strings.TrimSpace(q.Get("as_session")); raw != "" {
		// 显式指定调用方 session（默认就是被查 session 自己：同 session 恒可见）。
		as, err := parseSessionID(raw)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "bad_as_session", err.Error())
			return
		}
		caller.SessionID = as
	}
	if raw := strings.TrimSpace(q.Get("agent_id")); raw != "" {
		caller.AgentID = contracts.Ulid(raw)
	}
	req := agentlink.InquiryRequest{SessionID: sid, Caller: caller}
	if raw := strings.TrimSpace(q.Get("project_agent_id")); raw != "" {
		// 投影参数：只看某个 agent 的事件（K25：按 agent_id 对全局流做投影）。
		req.AgentID = contracts.Ulid(raw)
	}
	if raw := strings.TrimSpace(q.Get("from")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			writeJSONError(w, http.StatusBadRequest, "bad_from", "from 必须是非负整数（seq）")
			return
		}
		req.Window.FromSeq = n
	}
	if raw := strings.TrimSpace(q.Get("to")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			writeJSONError(w, http.StatusBadRequest, "bad_to", "to 必须是非负整数（seq）")
			return
		}
		req.Window.ToSeq = n
	}
	if raw := strings.TrimSpace(q.Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			writeJSONError(w, http.StatusBadRequest, "bad_limit", "limit 必须是非负整数")
			return
		}
		req.Limit = n
	} else {
		req.Limit = defaultAgentlinkInquiryLimit // 默认有界（F6）：别把整个 session 塞进上下文
	}
	if raw := strings.TrimSpace(q.Get("refs")); raw != "" {
		for _, ref := range strings.Split(raw, ",") {
			if ref = strings.TrimSpace(ref); ref != "" {
				req.Refs = append(req.Refs, ref)
			}
		}
	}
	res, err := h.Inquiry(r.Context(), req.Caller, req)
	if err != nil {
		writeHubError(w, err)
		return
	}
	writeJSONBody(w, http.StatusOK, res)
}

// serveAgentlinkInbox 服务 GET /api/inbox?session_id=&agent_id=&since=&limit=
// （拉模型收件箱）。
//
// 返回 InboxResult：messages + **next_seq**（游标水位）+ has_more（还有没有更多）——
// 游标不再靠"收到的最后一条消息"反推（那条消息可能根本没投给自己）。
func (h *Hub) serveAgentlinkInbox(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	sid, err := parseSessionID(q.Get("session_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_session_id", err.Error())
		return
	}
	iq := agentlink.InboxQuery{SessionID: sid}
	if raw := strings.TrimSpace(q.Get("agent_id")); raw != "" {
		iq.AgentID = contracts.Ulid(raw)
	}
	if raw := strings.TrimSpace(q.Get("since")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			writeJSONError(w, http.StatusBadRequest, "bad_since", "since 必须是非负整数（seq）")
			return
		}
		iq.SinceSeq = n
	}
	if raw := strings.TrimSpace(q.Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			writeJSONError(w, http.StatusBadRequest, "bad_limit", "limit 必须是非负整数")
			return
		}
		iq.Limit = n
	}
	res, err := h.Inbox(r.Context(), iq)
	if err != nil {
		writeHubError(w, err)
		return
	}
	writeJSONBody(w, http.StatusOK, res)
}

// agentlinkSendBody 是广播/交接端点的请求体（协议形状；agentlink 自己是库形态不认识它）。
type agentlinkSendBody struct {
	SessionID string `json:"session_id"`
	AgentID   string `json:"agent_id"`
	GateID    string `json:"gate_id"`
	// SubtreeAgentID 是 subtree 目标的根 agent id（与 gate_id 二选一）。
	SubtreeAgentID string          `json:"subtree_agent_id"`
	Text           string          `json:"text"`
	Payload        json.RawMessage `json:"payload"`
	// IdempotencyKey 幂等钥匙（非空 → 重试不重复落库）。
	IdempotencyKey string `json:"idempotency_key"`
}

// target 从请求体组出投递目标（gate_id 与 subtree_agent_id 二选一）。
func (b agentlinkSendBody) target() (contracts.AgentLinkTarget, error) {
	gateID := strings.TrimSpace(b.GateID)
	root := strings.TrimSpace(b.SubtreeAgentID)
	switch {
	case gateID != "" && root != "":
		return contracts.AgentLinkTarget{}, errors.New("gate_id 与 subtree_agent_id 只能给一个")
	case gateID != "":
		return contracts.AgentLinkTarget{Type: contracts.AgentLinkTargetTypeGate, GateID: &gateID}, nil
	case root != "":
		id := contracts.Ulid(root)
		return contracts.AgentLinkTarget{Type: contracts.AgentLinkTargetTypeSubtree, AgentID: &id}, nil
	default:
		return contracts.AgentLinkTarget{}, errors.New("必须给 gate_id 或 subtree_agent_id")
	}
}

// identity 从请求体取发送方身份（agent_id 可缺省 → 该 session 的主 agent）。
func (b agentlinkSendBody) identity() (contracts.Ulid, contracts.Ulid, error) {
	sid := contracts.Ulid(strings.TrimSpace(b.SessionID))
	if !core.ValidUlid(sid) {
		return "", "", fmt.Errorf("session_id 不是合法 ULID: %q", b.SessionID)
	}
	var aid contracts.Ulid
	if raw := strings.TrimSpace(b.AgentID); raw != "" {
		aid = contracts.Ulid(raw)
		if !core.ValidUlid(aid) {
			return "", "", fmt.Errorf("agent_id 不是合法 ULID: %q", b.AgentID)
		}
	}
	return sid, aid, nil
}

// serveAgentlinkSend 服务 POST /api/agentlink/broadcast 与 /api/agentlink/handoff。
func (h *Hub) serveAgentlinkSend(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body agentlinkSendBody
		dec := json.NewDecoder(io.LimitReader(r.Body, maxAgentLinkBody))
		if err := dec.Decode(&body); err != nil {
			writeJSONError(w, http.StatusBadRequest, "bad_body", "请求体不是合法 JSON 对象: "+err.Error())
			return
		}
		target, err := body.target()
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "bad_target", err.Error())
			return
		}
		sid, aid, err := body.identity()
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "bad_identity", err.Error())
			return
		}
		caller := agentlink.Identity{SessionID: sid, AgentID: aid}
		var res agentlink.MessageResult
		if kind == agentlink.KindHandoff {
			res, err = h.Handoff(r.Context(), caller, agentlink.HandoffRequest{
				Target: target, Text: body.Text, Payload: body.Payload, IdempotencyKey: body.IdempotencyKey,
			})
		} else {
			res, err = h.Broadcast(r.Context(), caller, agentlink.BroadcastRequest{
				Target: target, Text: body.Text, IdempotencyKey: body.IdempotencyKey,
			})
		}
		if err != nil {
			writeHubError(w, err)
			return
		}
		writeJSONBody(w, http.StatusOK, res)
	}
}
