package host

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/runtime/worktree"
	"github.com/Yorha9e/alasol/ui/web"
)

// httpLog 是 HTTP 层的告警出口（静态资源读不到等装配级异常）。
var httpLog = slog.Default()

// Handler 返回 hub 的 HTTP 多路复用器（GET / 与静态资源、GET /ws、GET /api/*）。
//
// 路由（Go 1.22+ 方法+通配符模式）：
//
//	GET /                        → ui/web 嵌入的 index.html（Web UI 单页）
//	GET /index.html              → 同上
//	GET /ws                      → transport.Server.ServeWS（NDJSON 行式事件流）
//	GET /api/health              → {"ok":bool,"watermark":int,"sessions":int}
//	GET /api/monitor             → MonitorView（K9：当前采样 + 阈值 + 告警环）
//	GET /api/sessions            → [SessionRow, ...]（在线表，最多 MaxSessionRows 行；hostable 标"本进程可不可发 prompt"，G12）
//	POST /api/sessions           → {"session_id":…}（201；开新会话 = 本进程托管，不自动跑 prompt）
//	GET /api/cost                → TokenTotalView（跨 session token 成本汇总，J21）
//	GET /api/cost/{session_id}   → TokenCostView（单 session token 成本，按模型分组）
//	GET /api/context/{session_id}→ 最新 ContextSnapshot；没有 → 404
//	GET /api/peers?session_id=   → PeersView（K11 水平可见性：经 gate 相交的对端主 agent）
//	POST /api/gates/join         → PeersView（K12 显式 join_gate；幂等）
//
// 图投影（v2-S2，G2f **可选投影**，默认关闭）：
//
//	GET /api/graph/impact?roots=a,b&kind=…&depth=…&limit=…&core=…&diff_files=…&diff_added=…&diff_deleted=…
//	  → ImpactReport（change_impact 影响面：一跳/传递下游 + 触及的核心接口 + diff 规模
//	     + Scale + suggest_escalate；G21/W16）。未启用 → 503 graph_disabled
//
// 多 worktree 隔离（v1-S5 D：并行任务各占一个工作区）：
//
//	GET    /api/worktrees          → [Worktree, ...]（已登记，按 name 排序）
//	POST   /api/worktrees          → Worktree（建一个；体 {"repo","name"}）
//	DELETE /api/worktrees/{name}   → {"removed":name}（junction-safe 清理）
//
// 在线表注入配置（K14/K15：运行期动态管理，改完下一轮生效、不重启）：
//
//	GET  /api/online-table/config → OnlineTableView（当前生效的 N / 每行 token 预算 / 暴露等级 / 窗口行数）
//	POST /api/online-table/config → OnlineTableView（部分更新；400 = 参数非法）
//
// agentlink（K18 五件套，core/agentlink 是库形态——协议绑定全在本层）：
//
//	GET  /api/presence?session_id=        → PresenceReport（谁在线/在干什么/属于哪些 gate）
//	GET  /api/discover?session_id=&gate_id=|agent_id= → DiscoverResult（按 gate / 按 agent 树）
//	GET  /api/inquiry?session_id=...      → InquiryResult（结构化摘要 + 事件窗口 + refs 核验）
//	GET  /api/inbox?session_id=...        → InboxResult（消息 + 游标水位 + has_more）
//	POST /api/agentlink/broadcast         → MessageResult（落一条 durable 事件）
//	POST /api/agentlink/handoff           → MessageResult（同上 + 结构化 payload）
//
// 事件回放（v3-S4，G3：断线补读——WS 断线即丢，协议面补一个按 seq 的补读面，
// 与 Store.ReadFrom 同语义、返回事件数组 + 水位）：
//
//	GET /api/events?from=&to=&limit=&session_id= → {"events":[…],"watermark":N,"from":X,"to":Y,"truncated":bool}
//	  → **全局事实流**按 seq 回放（只含 durable 事件；流事件不入库、不回放）。
//	    可见性口径 = 全局流（与全局 WS 订阅同口径，**不做 K11 过滤**——这是原始事件日志的补读面，
//	    不是 agentlink 的可见性读；要 K11 过滤用 /api/inquiry）。from 缺省 = 1，to 缺省 = 不设上界，
//	    limit 缺省 = defaultEventsLimit、上限 maxEventsLimit。
//	    watermark = **事实流 tip**（store 的 MAX(seq)；与 /api/health 的**索引器水位**不是一回事：
//	    那个是派生表进度、后台轮询会短暂落后，这个是事实流本身的即时 tip）。
//
// 对话 / 审批（v3-S4，G1：兑现 transport 契约承诺的 command.submit / approval.*）：
//
//	POST /api/sessions                        → {"session_id":…}（201；开新会话的 REST 入口，v3-S5 ①）
//	POST /api/sessions/{session_id}/prompt   → {"session_id":…,"status":"running"}（202；异步跑，结果经事件流）
//	POST /api/approvals/{request_id}/respond → {"request_id":…,"decision":…}（注入挂起的审批；无人应答 = deny，fail-close）
//	GET  /api/approvals/pending              → [PendingApprovalView, ...]（G11：挂起审批枚举面；空表 → []）
//
// 其它方法/路径 → **405 / 404，一律 JSON 错误体**（前端统一按 JSON 解）：
// /api/* 未命中具体路由时，路径认得但方法不符 → 405（带 Allow），路径不认得 → 404
// ——不再掉进静态通配报"没有这个静态资源"（G7）。
//
// 发现表的其余查询面（Agents / Agent / Gates …）**故意不暴露**：K11 的水平可见性只到
// "对端 session 的主 agent"，跨 session 的 agent 清单是宿主内部视图（见 Hub.Discovery）。
func (h *Hub) Handler() http.Handler {
	mux := http.NewServeMux()

	// 静态资源（嵌入二进制，无外部文件依赖）：根与 index 只认**精确路径**
	// （`{$}` 锚定到根，不当通配——否则 GET /api/xxx 会被它接住，见 G7）。
	mux.HandleFunc("GET /{$}", h.serveIndex)
	mux.HandleFunc("GET /index.html", h.serveIndex)

	// 事件流（WS + NDJSON）。
	mux.HandleFunc("GET /ws", h.ws.ServeWS)

	// API：经 reg 登记——同时注册到 mux 并记录"路径 → 允许的方法"（tbl），
	// 供下面的 /api/ 回退面判 405（路径认得、方法不对）vs 404（路径都不认得）。
	//
	// probe 是**只认路径**的探针 mux：同一批路径模板以"不带方法"注册（= 任何方法都匹配），
	// 用来判"这个路径认不认得"。为什么不能直接拿字面路径查 tbl：tbl 的键是**模板**
	// （/api/cost/{session_id}、/api/sessions/{id}/prompt …），而请求里的路径是**实例**
	// （/api/cost/01JN…）——两者永不相等 → 带路径参数的端点方法不符时会误报 404
	// 而不是 405（客户端于是以为"端点不存在"，而不是"方法用错了"）。
	tbl := apiTable{}
	probe := http.NewServeMux()
	reg := func(method, pattern string, fn http.HandlerFunc) {
		mux.HandleFunc(method+" "+pattern, fn)
		if tbl[pattern] == nil {
			// 探针只注册**一次**每个路径模板（同一路径的 GET/POST 共用一个表项——
			// 重复注册同一 pattern 会让 ServeMux 直接 panic）。
			probe.HandleFunc(pattern, fn)
		}
		tbl.add(pattern, method)
	}

	reg("GET", "/api/health", h.serveHealth)
	reg("GET", "/api/monitor", h.serveMonitor)
	reg("GET", "/api/sessions", h.serveSessions)
	// 开新会话的 REST 入口（v3-S5 ①）：与 GET /api/sessions 同路径、不同方法
	// （reg 的 probe/tbl 会把两种方法都记进 405 口径）。
	reg("POST", "/api/sessions", h.serveSessionCreate)
	reg("GET", "/api/cost", h.serveCostTotal)
	reg("GET", "/api/cost/{session_id}", h.serveCost)
	reg("GET", "/api/context/{session_id}", h.serveContext)
	reg("GET", "/api/peers", h.servePeers)
	reg("POST", "/api/gates/join", h.serveGateJoin)

	// 图投影（v2-S2，G2f 可选投影）：change_impact 影响面报告（G21/W16）。
	// 未启用时 503（明说"没开"，不返回空影响面——"没有影响面"对 escalate 是危险答案）。
	reg("GET", "/api/graph/impact", h.serveGraphImpact)
	// 图投影重建（v3-S3，N2 运维入口）：崩溃遗留的重建窗口（rebuilding=1）会让影响面/
	// 冲突预测一直不可用；启动期会自动恢复一次，这个端点供人工重试。
	reg("POST", "/api/graph/rebuild", h.serveGraphRebuild)

	// 合并门禁（v3-S3 清 V6/V7 挂账）：review 记录 + 合并前查放行 + 合并后冲突预测。
	reg("POST", "/api/merge/review", h.serveMergeReview)
	reg("POST", "/api/merge/check", h.serveMergeCheck)
	reg("POST", "/api/merge/predict", h.serveMergePredict)

	// 多 worktree 隔离（v1-S5 D）：创建 / 列表 / 删除。
	reg("GET", "/api/worktrees", h.serveWorktreesList)
	reg("POST", "/api/worktrees", h.serveWorktreeCreate)
	reg("DELETE", "/api/worktrees/{name}", h.serveWorktreeRemove)

	// 在线表注入配置（K14/K15：运行期动态管理，不重启）。
	reg("GET", "/api/online-table/config", h.serveOnlineTableConfig)
	reg("POST", "/api/online-table/config", h.serveOnlineTableConfig)

	// agentlink（K18）。
	reg("GET", "/api/presence", h.serveAgentlinkPresence)
	reg("GET", "/api/discover", h.serveAgentlinkDiscover)
	reg("GET", "/api/inquiry", h.serveAgentlinkInquiry)
	reg("GET", "/api/inbox", h.serveAgentlinkInbox)
	reg("POST", "/api/agentlink/broadcast", h.serveAgentlinkSend(agentlink.KindBroadcast))
	reg("POST", "/api/agentlink/handoff", h.serveAgentlinkSend(agentlink.KindHandoff))

	// 事件回放（v3-S4，G3）：按 seq 补读全局事实流（断线补读面）。
	reg("GET", "/api/events", h.serveEvents)

	// 对话 / 审批（v3-S4，G1）：协议面发起一轮对话 + 应答/枚举挂起的审批。
	reg("POST", "/api/sessions/{session_id}/prompt", h.servePrompt)
	reg("POST", "/api/approvals/{request_id}/respond", h.serveApprovalRespond)
	// 挂起审批枚举面（v3-S5，G11）：与 respond 不同路径、不同方法（405 口径各自独立）。
	reg("GET", "/api/approvals/pending", h.serveApprovalPending)

	// /api/* 回退面（G7）：未命中具体路由时，按 probe+tbl 判 405 / 404，一律 JSON 错误体。
	mux.HandleFunc("/api/", h.serveAPIFallback(tbl, probe))
	mux.HandleFunc("/api", h.serveAPIFallback(tbl, probe))

	// 其余路径（非 /api 的未知静态路径）→ JSON 404（保持"前端统一按 JSON 解"）。
	mux.HandleFunc("/", h.serveStaticNotFound)

	return mux
}

// apiTable 记录每个 API 路径模板允许的 HTTP 方法（G7 回退面判 405 vs 404 用）。
//
// 与 mux 注册**同一处**产出（Handler 里的 reg 助手同时写两处），不另抄一份路由表
// （D21：一处定义，避免"注册了但忘了记方法"的漂移）。
type apiTable map[string]map[string]bool

// add 记录某路径模板允许的一个方法。
func (t apiTable) add(pattern, method string) {
	if t[pattern] == nil {
		t[pattern] = map[string]bool{}
	}
	t[pattern][method] = true
}

// serveAPIFallback 服务 /api/* 未命中具体路由的请求（G7）。
//
//	路径认得、方法不符 → 405（带 Allow 头，明说支持哪些方法）；
//	路径不认得         → 404（"没有这个 API 端点"——**不**再说"没有这个静态资源"，
//	                     那对 /api 路径是误导：第三方会去翻静态资源而不是改方法/路径）。
//
// 路径认不认得由 probe（只认路径的探针 mux）判：它按**模板**匹配（带路径参数的端点
// 也能认出），返回的模板再查 tbl 拿允许的方法。tbl 的键是模板，直接拿字面路径查会
// 永远落空（带参数的端点方法不符 → 误报 404）。
func (h *Hub) serveAPIFallback(tbl apiTable, probe *http.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, pattern := probe.Handler(r)
		methods, known := tbl[pattern]
		if known && methods[r.Method] {
			// 路径认得、方法也对，却没命中具体 handler（不该发生）→ 404 兜底。
			writeJSONError(w, http.StatusNotFound, "not_found", "没有这个 API 端点: "+r.URL.Path)
			return
		}
		if known {
			allow := allowedMethods(methods)
			w.Header().Set("Allow", allow)
			writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed",
				fmt.Sprintf("端点 %s 不支持方法 %s（支持: %s）", r.URL.Path, r.Method, allow))
			return
		}
		writeJSONError(w, http.StatusNotFound, "not_found", "没有这个 API 端点: "+r.URL.Path)
	}
}

// allowedMethods 把方法集合折成排序后的逗号串（Allow 头要确定性）。
func allowedMethods(m map[string]bool) string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}

// serveStaticNotFound 服务非 /api 的未知静态路径（JSON 404，与旧口径一致）。
func (h *Hub) serveStaticNotFound(w http.ResponseWriter, r *http.Request) {
	writeJSONError(w, http.StatusNotFound, "not_found", "没有这个静态资源: "+r.URL.Path)
}

// serveIndex 服务嵌入的 Web UI（只经 GET / 与 GET /index.html 到达；其它路径由
// serveStaticNotFound / serveAPIFallback 处理，不落到这里）。
func (h *Hub) serveIndex(w http.ResponseWriter, r *http.Request) {
	body, err := web.IndexHTML()
	if err != nil {
		httpLog.Error("host: 读嵌入的 index.html 失败", "err", err)
		writeJSONError(w, http.StatusInternalServerError, "static_missing", err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store") // 开发期改了前端刷新即生效
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// serveHealth 服务 GET /api/health。
func (h *Hub) serveHealth(w http.ResponseWriter, r *http.Request) {
	health, err := h.Health(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "health_failed", err.Error())
		return
	}
	writeJSONBody(w, http.StatusOK, health)
}

// serveMonitor 服务 GET /api/monitor（K9：当前采样 + 生效阈值 + 告警环）。
func (h *Hub) serveMonitor(w http.ResponseWriter, r *http.Request) {
	m := h.Monitor()
	if m == nil {
		writeJSONError(w, http.StatusInternalServerError, "monitor_unavailable", "hub 未起监控器")
		return
	}
	writeJSONBody(w, http.StatusOK, m.View())
}

// serveSessions 服务 GET /api/sessions（在线表）。
//
// 计数口径（A1）：在线表是 **agent session 面板**——hub 伪 session（hub.* 事件族）与
// /btw 旁支 fork session **都不进表**。所以"事件流里有 N 条、表里 M 行"对不上是正常的
// （表只列 agent session；伪 session 无 agent、旁支不是一等会话）。
func (h *Hub) serveSessions(w http.ResponseWriter, r *http.Request) {
	rows, err := h.Sessions(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "sessions_failed", err.Error())
		return
	}
	// 空表返回 [] 而不是 null：前端 rows.forEach 不需要判 null。
	if rows == nil {
		rows = []SessionRow{}
	}
	writeJSONBody(w, http.StatusOK, rows)
}

// maxSessionCreateBody 限制 POST /api/sessions 请求体大小（SessionSpec 三个短字段，4KB 绰绰有余）。
const maxSessionCreateBody = 4 << 10

// serveSessionCreate 服务 POST /api/sessions（开新会话的 REST 入口，v3-S5 ①）。
//
//	请求体（可选，可为空）：{"cwd":"…","run_id":"…","worktree":"…"}（SessionSpec 的 JSON 面；
//	    全部可省 = 用 hub 级默认）
//	201 → {"session_id":"<ULID>"}：本进程托管的空 session（**不自动跑 prompt**——纯建会话，
//	    首轮对话由调用方随后 POST /api/sessions/{id}/prompt 发起）
//	400 → 体不是合法 JSON / CreateSession 校验失败（坏 cwd / 坏 worktree / 坏 run_id）
//
// 托管口径（G2/G25）：与 WS command.submit（不带 session_id）新建路径**同源同口径**——
// 都是"本进程即宿主"。CreateSession 只登记归属（specs），托管的判据是 h.loops 里有
// loop（hostsSession），所以这里建完 id 后就地 loopFor 建 loop（不 Run）——之后对该 id
// 的 POST /api/sessions/{id}/prompt 直接可用（202），不必先跑一轮"预热"。
func (h *Hub) serveSessionCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		CWD      string `json:"cwd"`
		RunID    string `json:"run_id"`
		Worktree string `json:"worktree"`
	}
	// body 可空：EOF（没发体）= 全用 hub 级默认，不算坏体。
	if err := json.NewDecoder(io.LimitReader(r.Body, maxSessionCreateBody)).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeJSONError(w, http.StatusBadRequest, "bad_body", "请求体不是合法 JSON 对象: "+err.Error())
		return
	}
	sid, err := h.CreateSession(SessionSpec{CWD: body.CWD, RunID: body.RunID, Worktree: body.Worktree})
	if err != nil {
		// CreateSession 的校验失败都是入参问题（坏 CWD / 坏 worktree / 坏 run_id）→ 400。
		writeJSONError(w, http.StatusBadRequest, "session_create_failed", err.Error())
		return
	}
	// 建 loop = 本进程托管（hostsSession 通过；不 Run，零 prompt、零模型调用）。
	// 与 WS command.submit 新建路径最终汇到的 loopFor 同一处——同源同口径。
	if _, err := h.loopFor(r.Context(), sid); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "session_host_failed", err.Error())
		return
	}
	writeJSONBody(w, http.StatusCreated, map[string]string{"session_id": string(sid)})
}

// serveCostTotal 服务 GET /api/cost（跨 session token 成本汇总，J21）。
func (h *Hub) serveCostTotal(w http.ResponseWriter, r *http.Request) {
	view, err := h.TokenCostTotal(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "cost_failed", err.Error())
		return
	}
	writeJSONBody(w, http.StatusOK, view)
}

// serveCost 服务 GET /api/cost/{session_id}（单 session token 成本，J21）。
func (h *Hub) serveCost(w http.ResponseWriter, r *http.Request) {
	sid, err := parseSessionID(r.PathValue("session_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_session_id", err.Error())
		return
	}
	view, ok, err := h.TokenCost(r.Context(), sid)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "cost_failed", err.Error())
		return
	}
	if !ok {
		writeJSONError(w, http.StatusNotFound, "no_cost",
			fmt.Sprintf("session %s 还没有带 usage 的 model.turn（跑一轮对话后就有）", sid))
		return
	}
	writeJSONBody(w, http.StatusOK, view)
}

// serveContext 服务 GET /api/context/{session_id}。
func (h *Hub) serveContext(w http.ResponseWriter, r *http.Request) {
	sid, err := parseSessionID(r.PathValue("session_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_session_id", err.Error())
		return
	}
	snap, ok, err := h.ContextSnapshot(r.Context(), sid)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "context_failed", err.Error())
		return
	}
	if !ok {
		writeJSONError(w, http.StatusNotFound, "no_context_snapshot",
			fmt.Sprintf("session %s 还没有 context.assembled 快照（跑一轮对话后就有）", sid))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(snap)
}

// parseSessionID 校验路径里的 session id（S1/S9：不合则拒绝，不修复）。
func parseSessionID(raw string) (contracts.Ulid, error) {
	id := contracts.Ulid(strings.TrimSpace(raw))
	if !core.ValidUlid(id) {
		return "", fmt.Errorf("session_id 不是合法 ULID（26 字符 Crockford base32）: %q", raw)
	}
	return id, nil
}

// servePeers 服务 GET /api/peers?session_id=<ulid>（K11 水平可见性）。
//
// 200 → PeersView（peers 可能是空数组 = 没有相交 gate 的存活对端）；
// 400 → session_id 不合法；404 → 该 session 不是本 hub 在托管的。
func (h *Hub) servePeers(w http.ResponseWriter, r *http.Request) {
	sid, err := parseSessionID(r.URL.Query().Get("session_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_session_id", err.Error())
		return
	}
	view, err := h.Peers(r.Context(), sid)
	if err != nil {
		writeHubError(w, err)
		return
	}
	if view.Peers == nil {
		view.Peers = []discovery.PeerRow{}
	}
	writeJSONBody(w, http.StatusOK, view)
}

// maxGateJoinBody 限制 POST /api/gates/join 的请求体大小（loopback 上也别裸奔）。
const maxGateJoinBody = 4 << 10

// serveGateJoin 服务 POST /api/gates/join（K12 显式 join_gate；幂等）。
//
// 请求体：{"session_id":"<ulid>","gate_id":"repo:xxx"|"workflow:xxx"|任意合法标识}。
// 200 → 加入后的 PeersView（已在线内时直接返回当前可见面，不重复落事件）；
// 400 → 体不是 JSON / session_id 不合法 / gate_id 不合法；404 → session 不受本 hub 托管。
func (h *Hub) serveGateJoin(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID string `json:"session_id"`
		GateID    string `json:"gate_id"`
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, maxGateJoinBody))
	if err := dec.Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_body", "请求体不是合法 JSON 对象: "+err.Error())
		return
	}
	sid, err := parseSessionID(body.SessionID)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_session_id", err.Error())
		return
	}
	// 入参校验放 HTTP 层：不合法 → 400（不是 500），与 host.Peers 的 session_id 校验同口径。
	if _, ok := discovery.NormalizeGateID(body.GateID); !ok {
		writeJSONError(w, http.StatusBadRequest, "bad_gate_id",
			fmt.Sprintf("gate_id 不合法（空 / 超过 %d 字符 / 含空白或控制字符）: %q", discovery.MaxGateIDLen, body.GateID))
		return
	}
	view, err := h.JoinGate(r.Context(), sid, body.GateID)
	if err != nil {
		writeHubError(w, err)
		return
	}
	if view.Peers == nil {
		view.Peers = []discovery.PeerRow{}
	}
	writeJSONBody(w, http.StatusOK, view)
}

// maxWorktreeBody 限制 POST /api/worktrees 请求体大小。
const maxWorktreeBody = 4 << 10

// serveWorktreesList 服务 GET /api/worktrees（列表，按 name 排序）。
func (h *Hub) serveWorktreesList(w http.ResponseWriter, r *http.Request) {
	wts := h.Worktrees()
	if wts == nil {
		wts = []worktree.Worktree{}
	}
	writeJSONBody(w, http.StatusOK, wts)
}

// serveWorktreeCreate 服务 POST /api/worktrees（建一个并行任务工作区）。
//
//	请求体：{"repo":"<仓库路径>","name":"<短名>"}
//	200 → Worktree（含 path）；400 体坏 / name 非法；500 git 失败等。
func (h *Hub) serveWorktreeCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Repo string `json:"repo"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, maxWorktreeBody)).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_body", "请求体不是合法 JSON 对象: "+err.Error())
		return
	}
	wt, err := h.CreateWorktree(r.Context(), body.Repo, body.Name)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "worktree_create_failed", err.Error())
		return
	}
	writeJSONBody(w, http.StatusOK, wt)
}

// serveWorktreeRemove 服务 DELETE /api/worktrees/{name}（junction-safe 清理）。
func (h *Hub) serveWorktreeRemove(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := h.RemoveWorktree(r.Context(), name); err != nil {
		writeJSONError(w, http.StatusBadRequest, "worktree_remove_failed", err.Error())
		return
	}
	writeJSONBody(w, http.StatusOK, map[string]string{"removed": name})
}

// writeHubError 把 Hub / agentlink 的错误映射成 HTTP 状态码。
//
// 口径：**按错误种类分**，不一律 500——
//
//	400  参数/身份/目标不合法（ErrBadIdentity / ErrBadTarget / ErrBadQuery /
//	     ErrEmptyText / ErrBadPayload / ErrCrossSessionSubtree）：客户端能改；
//	403  可见性拒绝（ErrNotVisible）：K11 挡住——不是"参数错了"，是"你不该看这个"；
//	404  目标不存在或不受托管（ErrSessionNotHosted / ErrUnknownSession / ErrUnknownAgent）；
//	500  其余（真出事了）。
//
// 之前一律 500：K11 的拒绝（"你看不见"）被报成"服务器坏了"，客户端无法区分
// "我参数写错了"与"这消息不该给我看"。
func writeHubError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrSessionNotHosted):
		// 文案带指引（G12）：一库一 hub 是预期拓扑（G25）——别的进程建/重启失管的
		// session 在本进程读面可见、写面 404，出路是开新会话继续，不是修这个 id。
		writeJSONError(w, http.StatusNotFound, "unknown_session",
			"这个 session 不是本 hub 在托管的 session（一库一 hub 是预期拓扑：它由别的进程建/已被本进程重启失管；请 POST /api/sessions 开新会话继续）")
		return
	case errors.Is(err, agentlink.ErrBadIdentity):
		writeJSONError(w, http.StatusBadRequest, "bad_identity", err.Error())
		return
	case errors.Is(err, agentlink.ErrBadTarget):
		writeJSONError(w, http.StatusBadRequest, "bad_target", err.Error())
		return
	case errors.Is(err, agentlink.ErrBadQuery):
		writeJSONError(w, http.StatusBadRequest, "bad_query", err.Error())
		return
	case errors.Is(err, agentlink.ErrEmptyText):
		writeJSONError(w, http.StatusBadRequest, "empty_text", err.Error())
		return
	case errors.Is(err, agentlink.ErrBadPayload):
		writeJSONError(w, http.StatusBadRequest, "bad_payload", err.Error())
		return
	case errors.Is(err, agentlink.ErrCrossSessionSubtree):
		writeJSONError(w, http.StatusBadRequest, "cross_session_subtree", err.Error())
		return
	case errors.Is(err, agentlink.ErrNotVisible):
		writeJSONError(w, http.StatusForbidden, "not_visible", err.Error())
		return
	case errors.Is(err, agentlink.ErrUnknownSession), errors.Is(err, agentlink.ErrUnknownAgent):
		writeJSONError(w, http.StatusNotFound, "unknown_target", err.Error())
		return
	}
	writeJSONError(w, http.StatusInternalServerError, "hub_failed", err.Error())
}

// writeJSONBody 写一个 JSON 响应体。
func writeJSONBody(w http.ResponseWriter, code int, v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		httpLog.Error("host: 序列化响应失败", "err", err)
		writeJSONError(w, http.StatusInternalServerError, "encode_failed", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write(raw)
}

// writeJSONError 写一个 JSON 错误体（与 WS 内的 TransportError 同形状，前端统一解）。
func writeJSONError(w http.ResponseWriter, code int, errCode, message string) {
	writeJSONBody(w, code, contracts.TransportError{Code: errCode, Message: message})
}
