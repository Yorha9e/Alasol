package host

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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
//	GET /api/sessions            → [SessionRow, ...]（在线表，最多 MaxSessionRows 行）
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
// 其它方法/路径 → 405 / 404，一律 JSON 错误体（前端统一按 JSON 解）。
//
// 发现表的其余查询面（Agents / Agent / Gates …）**故意不暴露**：K11 的水平可见性只到
// "对端 session 的主 agent"，跨 session 的 agent 清单是宿主内部视图（见 Hub.Discovery）。
func (h *Hub) Handler() http.Handler {
	mux := http.NewServeMux()

	// 静态资源（嵌入二进制，无外部文件依赖）。
	mux.HandleFunc("GET /", h.serveIndex)
	mux.HandleFunc("GET /index.html", h.serveIndex)

	// 事件流（WS + NDJSON）。
	mux.HandleFunc("GET /ws", h.ws.ServeWS)

	// API。
	mux.HandleFunc("GET /api/health", h.serveHealth)
	mux.HandleFunc("GET /api/monitor", h.serveMonitor)
	mux.HandleFunc("GET /api/sessions", h.serveSessions)
	mux.HandleFunc("GET /api/cost", h.serveCostTotal)
	mux.HandleFunc("GET /api/cost/{session_id}", h.serveCost)
	mux.HandleFunc("GET /api/context/{session_id}", h.serveContext)
	mux.HandleFunc("GET /api/peers", h.servePeers)
	mux.HandleFunc("POST /api/gates/join", h.serveGateJoin)

	// 图投影（v2-S2，G2f 可选投影）：change_impact 影响面报告（G21/W16）。
	// 未启用时 503（明说"没开"，不返回空影响面——"没有影响面"对 escalate 是危险答案）。
	mux.HandleFunc("GET /api/graph/impact", h.serveGraphImpact)
	// 图投影重建（v3-S3，N2 运维入口）：崩溃遗留的重建窗口（rebuilding=1）会让影响面/
	// 冲突预测一直不可用；启动期会自动恢复一次，这个端点供人工重试。
	mux.HandleFunc("POST /api/graph/rebuild", h.serveGraphRebuild)

	// 合并门禁（v3-S3 清 V6/V7 挂账）：review 记录 + 合并前查放行 + 合并后冲突预测。
	mux.HandleFunc("POST /api/merge/review", h.serveMergeReview)
	mux.HandleFunc("POST /api/merge/check", h.serveMergeCheck)
	mux.HandleFunc("POST /api/merge/predict", h.serveMergePredict)

	// 多 worktree 隔离（v1-S5 D）：创建 / 列表 / 删除。
	mux.HandleFunc("GET /api/worktrees", h.serveWorktreesList)
	mux.HandleFunc("POST /api/worktrees", h.serveWorktreeCreate)
	mux.HandleFunc("DELETE /api/worktrees/{name}", h.serveWorktreeRemove)

	// 在线表注入配置（K14/K15：运行期动态管理，不重启）。
	mux.HandleFunc("GET /api/online-table/config", h.serveOnlineTableConfig)
	mux.HandleFunc("POST /api/online-table/config", h.serveOnlineTableConfig)

	// agentlink（K18）。
	mux.HandleFunc("GET /api/presence", h.serveAgentlinkPresence)
	mux.HandleFunc("GET /api/discover", h.serveAgentlinkDiscover)
	mux.HandleFunc("GET /api/inquiry", h.serveAgentlinkInquiry)
	mux.HandleFunc("GET /api/inbox", h.serveAgentlinkInbox)
	mux.HandleFunc("POST /api/agentlink/broadcast", h.serveAgentlinkSend(agentlink.KindBroadcast))
	mux.HandleFunc("POST /api/agentlink/handoff", h.serveAgentlinkSend(agentlink.KindHandoff))

	return mux
}

// serveIndex 服务嵌入的 Web UI。
func (h *Hub) serveIndex(w http.ResponseWriter, r *http.Request) {
	// 只认 / 与 /index.html：其它静态路径 v0 没有，明确 404 而不是静默回 index.html
	// （那会让前端 404 变 200，调试时很难发现）。
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		writeJSONError(w, http.StatusNotFound, "not_found", "没有这个静态资源: "+r.URL.Path)
		return
	}
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
		writeJSONError(w, http.StatusNotFound, "unknown_session",
			"这个 session 不是本 hub 在托管的 session（先跑一轮 prompt，或它属于别的 hub 进程）")
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
