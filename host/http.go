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
	"github.com/Yorha9e/alasol/core/discovery"
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
//	GET /api/sessions            → [SessionRow, ...]（在线表，最多 MaxSessionRows 行）
//	GET /api/context/{session_id}→ 最新 ContextSnapshot；没有 → 404
//	GET /api/peers?session_id=   → PeersView（K11 水平可见性：经 gate 相交的对端主 agent）
//	POST /api/gates/join         → PeersView（K12 显式 join_gate；幂等）
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
	mux.HandleFunc("GET /api/sessions", h.serveSessions)
	mux.HandleFunc("GET /api/context/{session_id}", h.serveContext)
	mux.HandleFunc("GET /api/peers", h.servePeers)
	mux.HandleFunc("POST /api/gates/join", h.serveGateJoin)

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

// writeHubError 把 Hub 的错误映射成 HTTP 状态码（404 = session 不受托管，其余 500）。
func writeHubError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrSessionNotHosted) {
		writeJSONError(w, http.StatusNotFound, "unknown_session",
			"这个 session 不是本 hub 在托管的 session（先跑一轮 prompt，或它属于别的 hub 进程）")
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
