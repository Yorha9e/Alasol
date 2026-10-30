package host

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
)

// 本文件是**事件回放面**（v3-S4，G3）：GET /api/events?from&to&limit&session_id。
//
// # 为什么补（G3：事件流无回放面）
//
// WS 断线即丢：订阅者通道满会丢（bus Lagging），连接断了更是什么都收不到。此前的补救
// 文案让客户端"按自己的 seq 水位用 Store.ReadFrom 补读"——但 Store.ReadFrom 是 **Go API**，
// 协议面上没有对应端点 → 第三方 GUI 断线重连后 [first_seq,last_seq] 出现**永久空洞**。
// "事件流是唯一事实源"对进程外消费者因此不成立。本端点把 Store.ReadFrom 的语义搬到协议面。
//
// # 可见性口径（写清，避免与 /api/inquiry 混淆）
//
// 这是**全局事实流**的补读面，与**全局 WS 订阅**同口径：
//   - 只含 **durable 事件**（流事件 durable=false 不入库、不占 seq、**不回放**——丢了就是丢了）；
//   - **不做 K11 可见性过滤**——它是原始事件日志的按 seq 补读，不是 agentlink 的可见性读
//     （要 K11 过滤 + 跨 session 摘要核验，用 /api/inquiry，那个要求调用方受托管）。
//   - 可选 session_id 只是**投影**（只回某 session 的事件），不是可见性判定——任意客户端
//     都能回放全局流（与全局 WS 订阅同权：hub 只监听 loopback + 无 cookie，见 transport doc）。
const (
	// defaultEventsLimit 是回放的默认条数（F6：不给上限 = 把整个 session 塞给调用方）。
	defaultEventsLimit = 1000
	// maxEventsLimit 是回放的条数硬上限（防 from=1&limit=999999999 把内存吃满）。
	maxEventsLimit = 10000
)

// eventsView 是 GET /api/events 的形状（事件数组 + 水位 + 实际窗口 + 截断信号）。
type eventsView struct {
	// Events 是 [from, to] 窗口内的 durable 事件（按 seq 升序；空 → []）。
	Events []contracts.Event `json:"events"`
	// Watermark 是**事实流 tip**（store 的 MAX(seq)；客户端据此判断"追平了没"——
	// 回放到 Watermark 即追上唯一事实源）。注意与 /api/health 的 watermark（索引器
	// 派生表进度）是两个口径：那个可能因后台轮询短暂落后，这个是事实流即时 tip。
	Watermark int64 `json:"watermark"`
	// From / To 回显实际生效的窗口（To=0 = 不设上界）。
	From int64 `json:"from"`
	To   int64 `json:"to"`
	// Truncated = 还有更多（本条数达到 limit 就被截断）→ 客户端应按最后一条的 seq+1 继续翻。
	Truncated bool `json:"truncated"`
}

// serveEvents 服务 GET /api/events?from=&to=&limit=&session_id=（G3 事件回放）。
//
// 参数口径（与 /api/inquiry 的窗口参数一致，A5 分页统一：都有 from/to/limit + 截断信号）：
//
//	from        窗口下界（含；seq）。缺省 = 1（从头）。非法（非整数/负）→ 400；
//	to          窗口上界（含；seq）。缺省 = 不设上界。非法 → 400；
//	limit       条数上限。缺省 = defaultEventsLimit，硬上限 maxEventsLimit。非法 → 400；
//	            **limit=0 钳到 1**（见下：0 会让翻页死循环）；
//	session_id  可选：只回某 session 的事件（投影，非可见性过滤）。非法 ULID → 400。
//
// 200 → eventsView；400 → 参数非法；500 → 读库失败。
func (h *Hub) serveEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	from := int64(1)
	if raw := strings.TrimSpace(q.Get("from")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			writeJSONError(w, http.StatusBadRequest, "bad_from", "from 必须是非负整数（seq）")
			return
		}
		from = n
	}
	to := int64(0)
	if raw := strings.TrimSpace(q.Get("to")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			writeJSONError(w, http.StatusBadRequest, "bad_to", "to 必须是非负整数（seq）")
			return
		}
		to = n
	}
	limit := defaultEventsLimit
	if raw := strings.TrimSpace(q.Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			writeJSONError(w, http.StatusBadRequest, "bad_limit", "limit 必须是非负整数")
			return
		}
		limit = n
	}
	// limit=0 钳到 1（不是"当 0 条处理"）：limit=0 会回 {"events":[],"truncated":true}——
	// 客户端按 truncated 翻页却永远拿不到新事件（**死循环**，事件窗口补不回来）。
	// 钳到 1 保住翻页语义：truncated=true 时这一页仍有事件可取，翻页能往前走。
	if limit < 1 {
		limit = 1
	}
	if limit > maxEventsLimit {
		limit = maxEventsLimit // 硬上限（F6）：调用方要更多就分页
	}

	var sid *contracts.Ulid
	if raw := strings.TrimSpace(q.Get("session_id")); raw != "" {
		id, err := parseSessionID(raw)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "bad_session_id", err.Error())
			return
		}
		sid = &id
	}

	// 多读一条（limit+1）探测"还有更多"（truncated），不必再查一次库。
	raw, err := h.store.ReadFrom(r.Context(), sid, from, limit+1)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "events_failed", err.Error())
		return
	}
	events := make([]contracts.Event, 0, limit)
	truncated := false
	for _, ev := range raw {
		if ev.Seq == nil {
			continue // durable 事件必带 seq；防御（理论不可达）
		}
		if to > 0 && *ev.Seq > to {
			break // 越过上界（seq 升序，后续都越界）——追到上界即止，不算截断
		}
		if len(events) >= limit {
			truncated = true // 还有更多（本条数达到 limit）→ 客户端按最后一条 seq+1 继续翻
			break
		}
		events = append(events, ev)
	}

	// 水位：**事实流 tip**（store 的 MAX(seq)，不是索引器水位——那个是派生表进度、
	// 后台轮询会短暂落后；回放消费者要追的是事实流本身，追到 Tip 即追上唯一事实源）。
	wm, werr := h.store.Tip(r.Context())
	if werr != nil {
		writeJSONError(w, http.StatusInternalServerError, "events_failed", werr.Error())
		return
	}
	writeJSONBody(w, http.StatusOK, eventsView{
		Events:    events,
		Watermark: wm,
		From:      from,
		To:        to,
		Truncated: truncated,
	})
}
