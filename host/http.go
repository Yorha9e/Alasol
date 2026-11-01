package host

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/core/storage"
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
//	GET /api/git                 → gitView（G17 git 观测面：hub 工作目录 branch/dirty/last_commit；
//	                               拉式现采、HTTP-only **不进事件流**；?path= 覆盖采样目录；非仓库 → repo=false 如实报）
//	GET /api/extensions          → [ExtensionView, ...]（扩展注册表全 kind 快照 + Health，只读；v4-5）
//	GET /api/sessions            → [SessionRow, ...]（在线表，最多 MaxSessionRows 行；hostable 标"本进程可不可发 prompt"，G12）
//	POST /api/sessions           → {"session_id":…}（201；开新会话 = 本进程托管，不自动跑 prompt）
//	GET /api/cost                → TokenTotalView（跨 session token 成本汇总，J21）
//	GET /api/cost/{session_id}   → TokenCostView（单 session token 成本，按模型分组）
//	GET /api/context/{session_id}→ 最新 ContextSnapshot；没有 → 404
//	GET /api/peers?session_id=   → PeersView（K11 水平可见性：经 gate 相交的对端主 agent）
//	POST /api/gates/join         → PeersView（K12 显式 join_gate；幂等）
//	POST /api/gates/leave        → PeersView（G15 显式 leave_gate；幂等——重复 leave 无害）
//	GET /api/gates?session_id=   → GatesView（G15 枚举面：全部 gate 的存在性 + 活跃成员数；
//	                               caller 在网内的 gate 额外带成员详情——K11 分层口径）
//
// 图投影（v2-S2，G2f **可选投影**，默认关闭）：
//
//	GET /api/graph/impact?roots=a,b&kind=…&depth=…&limit=…&core=…&diff_files=…&diff_added=…&diff_deleted=…
//	  → ImpactReport（change_impact 影响面：一跳/传递下游 + 触及的核心接口 + diff 规模
//	     + Scale + suggest_escalate；G21/W16）。未启用 → 503 graph_disabled
//
// 大 payload 外置 blob 读面（v4-3-A，G2e/D4；默认关 = BlobDir 空）：
//
//	GET /api/blobs/{id}                  → 二进制全文（流式；Content-Type/Content-Length 取自 blob meta）。
//	  tool.call.completed 的 result 超阈值被外置后，事件里只留 binary_ref 指向的 id + 摘要 text；
//	  消费方（UI / 审计 / 回放）拿这个 id 经本端点取回**完整** result JSON。未启用 / id 不存在 → 404。
//
// 向量引擎 + RAG 检索（v4-3-B，G2d；默认关 = Config.VectorsEnabled=false）：
//
//	GET  /api/vectors/search?q=…&k=…&filter={"kind":"note"}
//	  → {"query":…,"k":N,"hits":[{id,score,source_ref,payload,truncated}, …]}
//	  （truncated 在**每条命中**上：被 K 截断时全部置位——诚实信号，见 storage.VectorHit）
//	  查询是**文本**（q）——宿主经 EmbeddingProvider 转成向量后检索（框架不算 embedding）。
//	  k 回报**生效上限**（k=0 未指定 → 宿主默认上限；见 VectorSearchView.K）。
//	  未启用 → **路径都不认得**（404 not_found：路由未注册）；未配 embedding 提供者 → 503
//	  （写明"由宿主/provider 层接"）；q 空 / k 负 / filter 不是合法 JSON 对象 → 400；
//	  **重建在途** → 503 vector_rebuilding（Clear+Upsert 窗口：此刻库里是空的/半重建的，
//	  不拿它当"没有命中"回——重建完成后重试）。
//	POST /api/vectors
//	  → {"count":N,"ids":[…]}（手动 ingest / 喂数据面：texts + source_refs 经提供者向量化后 Upsert；
//	    条目 id 按 (source_ref, 分块文本内容) 哈希确定性派生，重喂同一条覆盖）。
//	    source_refs 可省/可**短于** texts（按序号对齐，缺的记空串）；比 texts 更长 → 400。
//	    **单批条数上限 512**（一条文本 = 一次提供者调用，批内串行）→ 超出 400 并点名上限。
//	    错误口径同 search。
//
//	  两个端点都是**可选投影**的读写面：未启用时连路由都不注册（与 blob 同口径 = 404），
//	  开了才可能出现；Score 跨实现不可比（只可在同一次检索内排序用）。
//
// 文档族投影（G2 第 5 族；默认关 = Config.DocumentsEnabled=false）：
//
//	GET  /api/docs/{collection}/{key} → 单条 storage.Doc JSON；不存在 → 404
//	GET  /api/docs?collection=&prefix=&limit= → {"docs":[storage.Doc, ...]}
//	  （按 (collection, key) 字节序；空 = 空数组不是 null；prefix 字节前缀大小写敏感；
//	   limit 0 = 不截断、负值 → 400）
//	POST /api/docs/upsert  body {"docs":[storage.Doc,...]} → {"ok":true}
//	  （整批拒绝：版本冲突 → 409；写经 Transact 收口事务，G17 单写者 ingest 入口）
//	POST /api/docs/delete  body {"collection":"…","keys":[…]} → {"ok":true}（幂等）
//
//	  四个端点**未启用时连路由都不注册**（404 not_found，与 vectors 同口径 = 零行为变化）。
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
// agent 树只读快照（v3-S6-B，W5 多 agent 装配）：
//
//	GET /api/sessions/{session_id}/agents    → [AgentNodeView, ...]（该 session 的 agent 树快照：
//	  agent_id/parent_agent_id/depth/agent_type/role/status + summary/失败原因；root=主 agent。
//	  判据 = **树存在即 200**（树是 session 活状态，loop 失效不影响它）：没开 multi_agent 或
//	  还没建树 → 空列表 []；既没树也不由本进程托管 → 404 unknown_session）
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
	// G17 git 观测面（v4-7 后半）：hub 工作目录的 git 状态快照（branch/dirty/last_commit）。
	// 只做 HTTP 面（拉式现采）、**不进事件流**——git.state 事件走 F7 全链成本高，且
	// git 状态是进程本地派生观测（非事实流事实）。详见 host/git.go。
	reg("GET", "/api/git", h.serveGit)
	// 扩展注册表管理/观测面（v4-5 extension runtime 收口）：全 kind 快照 + Health（只读）。
	reg("GET", "/api/extensions", h.serveExtensions)
	reg("GET", "/api/sessions", h.serveSessions)
	// 开新会话的 REST 入口（v3-S5 ①）：与 GET /api/sessions 同路径、不同方法
	// （reg 的 probe/tbl 会把两种方法都记进 405 口径）。
	reg("POST", "/api/sessions", h.serveSessionCreate)
	reg("GET", "/api/cost", h.serveCostTotal)
	reg("GET", "/api/cost/{session_id}", h.serveCost)
	reg("GET", "/api/context/{session_id}", h.serveContext)
	reg("GET", "/api/peers", h.servePeers)
	reg("POST", "/api/gates/join", h.serveGateJoin)
	// G15 gate 管理面：显式 leave（幂等）+ 枚举（存在性/成员数 + caller 可见的成员详情）。
	reg("POST", "/api/gates/leave", h.serveGateLeave)
	reg("GET", "/api/gates", h.serveGates)

	// 图投影（v2-S2，G2f 可选投影）：change_impact 影响面报告（G21/W16）。
	// 未启用时 503（明说"没开"，不返回空影响面——"没有影响面"对 escalate 是危险答案）。
	reg("GET", "/api/graph/impact", h.serveGraphImpact)
	// 图投影重建（v3-S3，N2 运维入口）：崩溃遗留的重建窗口（rebuilding=1）会让影响面/
	// 冲突预测一直不可用；启动期会自动恢复一次，这个端点供人工重试。
	reg("POST", "/api/graph/rebuild", h.serveGraphRebuild)

	// 大 payload 外置 blob 读面（v4-3-A，G2e/D4）：按 Event.binary_ref 的 id 取回全文。
	// 未启用（Config.BlobDir 空）或 id 不存在 → 404；流式回内容（大对象不整个进内存）。
	reg("GET", "/api/blobs/{id}", h.serveBlob)

	// 向量引擎 + RAG 检索（v4-3-B，G2d）：**未启用（Config.VectorsEnabled=false）时路由
	// 根本不注册** → 路径认不得 → 404 not_found（与 blob 读面同口径）；开了才注册。
	// search 收 query 文本（宿主转 embedding），ingest 是喂数据面（texts + source_refs）。
	if h.cfg.VectorsEnabled {
		reg("GET", "/api/vectors/search", h.serveVectorSearch)
		reg("POST", "/api/vectors", h.serveVectorIngest)
	}

	// 文档族投影（G2 第 5 族）：**未启用（Config.DocumentsEnabled=false）时四个路由
	// 根本不注册** → 路径认不得 → 404 not_found（与 vectors 端点同口径）；开了才注册。
	// 读面 get/list，写面 upsert/delete（写经 Transact 收口事务，G17 单写者）。
	if h.docs != nil {
		reg("GET", "/api/docs/{collection}/{key}", h.serveDocGet)
		reg("GET", "/api/docs", h.serveDocList)
		reg("POST", "/api/docs/upsert", h.serveDocUpsert)
		reg("POST", "/api/docs/delete", h.serveDocDelete)
	}

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

	// agent 树只读快照（v3-S6-B）：GET /api/sessions/{id}/agents 返回该 session 的树快照
	// （GUI 派活面；判据 = 树存在即 200 —— 树是 session 活状态，loop 失效不影响它；
	//  无 MultiAgent/还没建树 → 空列表；既没树也不由本进程托管 → 404）。
	reg("GET", "/api/sessions/{session_id}/agents", h.serveSessionAgents)

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
	return parseULID(raw, "session_id")
}

// parseULID 校验一个 ULID 形态的 id，**文案点名它是哪一路 id**（F11）。
//
// 为什么要点名：同一个校验器被多个端点复用时，若文案写死成"session_id"，别的端点（如
// GET /api/blobs/{id}）就会**串味**——调用方传的是 blob id，报错却说 session_id 不合法，
// 会把人引到错误的参数上（"是不是 session_id 写错了？"）。故 what 由调用方给（本端点的
// 参数名/语义名），文案如实写它。
//
// 不合则拒绝，不修复（S1/S9：不猜、不截断、不补全）。
func parseULID(raw, what string) (contracts.Ulid, error) {
	id := contracts.Ulid(strings.TrimSpace(raw))
	if !core.ValidUlid(id) {
		return "", fmt.Errorf("%s 不是合法 ULID（26 字符 Crockford base32）: %q", what, raw)
	}
	return id, nil
}

// serveBlob 服务 GET /api/blobs/{id}（v4-3-A，G2e/D4 读面）：按 Event.binary_ref 的 id
// 取回被外置的**完整**内容，流式回给调用方（大对象不整个进内存，F12）。
//
//	200 → 二进制全文（Content-Type / Content-Length 取自 blob 的 meta）
//	400 → id 不是合法 ULID（文案**点名 blob id**，不报 session_id——F11）
//	404 → blob 未启用（Config.BlobDir 空）或 id 不存在 / 已回收（Delete 过）
//	500 → 读取出错（含存储损坏：meta 在但内容缺失/半截——blobfs 给明确错误）
//
// 托管口径同其他端点：这是**读面**，只读不写；写（外置）在 loop 侧（X1 completed 路径）。
func (h *Hub) serveBlob(w http.ResponseWriter, r *http.Request) {
	// blob id 用**自己的**校验文案（F11）：400 要点名 blob id，不许报"session_id 不合法"。
	id, err := parseULID(r.PathValue("id"), "blob id")
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_blob_id", err.Error())
		return
	}
	if h.blob == nil {
		// 未启用：没有任何外置内容可取（与"关 = 零行为变化"一致）。
		writeJSONError(w, http.StatusNotFound, "blob_disabled",
			"大 payload 外置未启用（Config.BlobDir 空；没有 binary_ref 内容可取）")
		return
	}
	meta, err := h.blob.Stat(r.Context(), id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "blob_not_found",
				fmt.Sprintf("blob %s 不存在或已回收（Delete 只删内容，事件里的引用原样留着）", id))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "blob_stat_failed", err.Error())
		return
	}
	rc, err := h.blob.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "blob_not_found",
				fmt.Sprintf("blob %s 不存在或已回收", id))
			return
		}
		// 存储损坏（meta 在内容不在 / 半截）等 → 500，明确报错（不静默回空/半截内容）。
		writeJSONError(w, http.StatusInternalServerError, "blob_read_failed", err.Error())
		return
	}
	defer rc.Close()
	ct := meta.MediaType
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	if meta.Size >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	}
	w.WriteHeader(http.StatusOK)
	// 流式拷贝（io.Copy 用 32KB 缓冲，不把大对象整个读进内存）。
	if _, err := io.Copy(w, rc); err != nil {
		// 响应头已发，无法再改状态码；只留痕（客户端中途断开是常态，不算服务端故障）。
		httpLog.Warn("host: 流式回 blob 中断", "id", string(id), "err", err.Error())
	}
}

// ---------------------------------------------------------------------------
// 向量引擎 + RAG 检索（v4-3-B，G2d）
// ---------------------------------------------------------------------------

// maxVectorBody 限制 POST /api/vectors 请求体大小（喂数据面：一批文本 + 来源指针，
// 64KB 够几十段分块；loopback 上也别裸奔）。
const maxVectorBody = 64 << 10

// VectorHitView 是一条向量检索命中（HTTP/JSON 出口形状）。
//
// Score 是**本实现口径**的相似度（vecmem = 余弦）：**跨实现不可比**，只可在同一次检索内
// 排序用（接口注释）。SourceRef 是来源指针（`e:<seq>` / 路径 / blob 引用 id）——消费端
// 据此回事件流核验/取原文（概率性检索的可重建锚，F5）。
type VectorHitView struct {
	ID        string          `json:"id"`
	Score     float32         `json:"score"`
	SourceRef string          `json:"source_ref,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Truncated bool            `json:"truncated"`
}

// VectorSearchView 是 GET /api/vectors/search 的响应形状。
//
// K 是**本次生效的 top-k 上限**（不是"请求里写的那个 k"）：
//   - 请求 k > 0 → 就是它；
//   - 请求 k = 0（未指定）→ 宿主的默认上限（Config.VectorsTopK；未配 = vecmem.DefaultK=10）。
//
// 为什么要报"生效上限"：回显 0 对消费方毫无意义（0 = 没指定），回显实际生效的上限
// 才可解释（"这次最多取 N 条"）。
type VectorSearchView struct {
	Query string          `json:"query"`
	K     int             `json:"k"`
	Hits  []VectorHitView `json:"hits"`
}

// serveVectorSearch 服务 GET /api/vectors/search?q=…&k=…&filter=…（v4-3-B，G2d）。
//
//	200 → VectorSearchView（hits 可能是空数组 = 没有命中；**不等于"不存在"**）
//	400 → q 空 / k 不是非负整数 / filter 不是合法 JSON 对象（键值都按字符串）
//	404 → 向量引擎未启用（Config.VectorsEnabled=false；路由本就不注册，这里是纵深防御）
//	503 → 未配置 embedding 提供者（框架不算 embedding：文本转向量由宿主/provider 层接）
//	      **或** 重建在途（vector_rebuilding：Clear+Upsert 窗口，不拿重建中的库当答案）
//	      **或** 上游 embedding 端点暂时不可用（上游 5xx / 429）
//	502 → 上游 embedding 端点拒绝了请求（上游 4xx，如 key 无效）
//	500 → 转向量/检索失败（宿主自己的错）
//
// filter 是可选查询参数，形态是 JSON 对象：`filter={"kind":"note","lang":"go"}`
// （AND 语义、字节精确匹配条目 payload 的顶层字符串字段；vecmem 的 Filter 口径）。
func (h *Hub) serveVectorSearch(w http.ResponseWriter, r *http.Request) {
	if h.vecs == nil {
		writeJSONError(w, http.StatusNotFound, "vectors_disabled",
			"向量引擎未启用（Config.VectorsEnabled=false；/api/vectors/search 不注册）")
		return
	}
	if h.embed == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "embedding_provider_unconfigured",
			"未配置 embedding 提供者：查询是**文本**，框架不算 embedding——文本转向量由宿主/provider 层接"+
				"（Config.EmbeddingProvider 注入实现后本端点可用）")
		return
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		writeJSONError(w, http.StatusBadRequest, "bad_query", "缺少 q 参数（查询文本；框架不算 embedding，宿主负责转向量）")
		return
	}
	k := 0
	if raw := strings.TrimSpace(r.URL.Query().Get("k")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			writeJSONError(w, http.StatusBadRequest, "bad_k",
				fmt.Sprintf("k 必须是非负整数（0 = 默认上限；得到 %q）", raw))
			return
		}
		k = n
	}
	var filter map[string]string
	if raw := strings.TrimSpace(r.URL.Query().Get("filter")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &filter); err != nil {
			writeJSONError(w, http.StatusBadRequest, "bad_filter",
				fmt.Sprintf("filter 必须是 JSON 对象（键值都按字符串），如 {\"kind\":\"note\"}: %v", err))
			return
		}
	}
	hits, err := h.SearchVectors(r.Context(), q, k, filter)
	if err != nil {
		writeVectorHubError(w, err)
		return
	}
	// k 回报**生效上限**（k=0 = 未指定 → 宿主默认上限；见 VectorSearchView.K 的口径）。
	effK := k
	if effK <= 0 {
		effK = h.effectiveVectorTopK()
	}
	view := VectorSearchView{Query: q, K: effK, Hits: make([]VectorHitView, 0, len(hits))}
	for _, hit := range hits {
		view.Hits = append(view.Hits, VectorHitView{
			ID:        string(hit.ID),
			Score:     hit.Score,
			SourceRef: hit.SourceRef,
			Payload:   hit.Payload,
			Truncated: hit.Truncated,
		})
	}
	writeJSONBody(w, http.StatusOK, view)
}

// VectorIngestView 是 POST /api/vectors 的响应形状（喂数据面）。
type VectorIngestView struct {
	Count int      `json:"count"`
	IDs   []string `json:"ids"`
}

// serveVectorIngest 服务 POST /api/vectors（v4-3-B，G2d：手动 ingest / 喂数据面）。
//
//	请求体：{"texts":["分块一","分块二"],"source_refs":["e:12","e:13"]}
//	  （source_refs 可省/可短于 texts——按序号对齐，**缺的来源指针记空串**；长度超过 texts → 400）
//	200 → {"count":N,"ids":[…]}（条目 id 按 (source_ref, 分块文本内容) 哈希确定性派生；
//	     同一 (source_ref, 文本) 重喂覆盖同一条）
//	400 → 体不是合法 JSON / texts 为空 / source_refs 更长 / **texts 超过单批上限 512 条**
//	      （一条文本 = 一次提供者调用，批内串行；超限 → 请分批喂）
//	      **或 texts 里有空串**（空文本没有可嵌入的内容；宿主侧预校验，不白跑外部调用）
//	404 → 向量引擎未启用（路由本就不注册；这里是纵深防御）
//	503 → 未配置 embedding 提供者（ingest 也要向量化：没提供者什么都进不去）
//	      **或** 上游 embedding 端点暂时不可用（上游 5xx / 429）
//	502 → 上游 embedding 端点拒绝了请求（上游 4xx，如 key 无效）
//	500 → 转向量/写入失败（宿主自己的错）
func (h *Hub) serveVectorIngest(w http.ResponseWriter, r *http.Request) {
	if h.vecs == nil {
		writeJSONError(w, http.StatusNotFound, "vectors_disabled",
			"向量引擎未启用（Config.VectorsEnabled=false；/api/vectors 不注册）")
		return
	}
	if h.embed == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "embedding_provider_unconfigured",
			"未配置 embedding 提供者：ingest 要把文本转向量，框架不算 embedding——"+
				"由宿主/provider 层接（Config.EmbeddingProvider 注入实现后本端点可用）")
		return
	}
	var body struct {
		Texts      []string `json:"texts"`
		SourceRefs []string `json:"source_refs"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, maxVectorBody)).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_body",
			"请求体不是合法 JSON 对象（应形如 {\"texts\":[\"…\"],\"source_refs\":[\"e:12\"]}）: "+err.Error())
		return
	}
	if len(body.Texts) == 0 {
		writeJSONError(w, http.StatusBadRequest, "empty_texts", "texts 不能为空（没有要向量化的文本）")
		return
	}
	if len(body.Texts) > maxVectorTexts {
		writeJSONError(w, http.StatusBadRequest, "too_many_texts",
			fmt.Sprintf("单批 texts 超过上限 %d 条（得到 %d 条）——一条文本一次提供者调用（批内串行），请分批喂",
				maxVectorTexts, len(body.Texts)))
		return
	}
	if len(body.SourceRefs) > len(body.Texts) {
		writeJSONError(w, http.StatusBadRequest, "bad_source_refs",
			fmt.Sprintf("source_refs（%d 条）比 texts（%d 条）还长（来源对不上就是数据错了）",
				len(body.SourceRefs), len(body.Texts)))
		return
	}
	ids, err := h.IngestVectors(r.Context(), body.Texts, body.SourceRefs)
	if err != nil {
		writeVectorHubError(w, err)
		return
	}
	view := VectorIngestView{Count: len(ids), IDs: make([]string, 0, len(ids))}
	for _, id := range ids {
		view.IDs = append(view.IDs, string(id))
	}
	writeJSONBody(w, http.StatusOK, view)
}

// upstreamStatusCarrier 是"能报出 HTTP 状态码的错误"的**窄结构形状**（core/retry.StatusCarrier
// 同款思路）。
//
// 为什么用窄接口而不是直接 import runtime/provider/openai 的 *StatusError：宿主 HTTP 面
// 只关心"**上游**回了个什么状态码"，不该与某一个具体 provider 的错误类型绑死——任何
// 实现了 HTTPStatus() int 的错误都能接上（openai 的 *StatusError 就实现了：它带
// HTTPStatus() 方法）。core 侧的同款形状见 core/retry.StatusCarrier。
type upstreamStatusCarrier interface {
	// HTTPStatus 返回上游的 HTTP 状态码（0 = 不知道/不是 HTTP 错误）。
	HTTPStatus() int
}

// writeVectorHubError 把向量面（IngestVectors / SearchVectors）的错误映射成 HTTP 状态码。
//
// 口径：**按错误种类分**，不一律 500——
//   - 未启用（errVectorsDisabled）→ 404（路由本就不注册；这里是纵深防御）；
//   - 未配 embedding 提供者（errNoEmbeddingProvider）→ 503（装配缺一件，不是客户端错）；
//   - **重建在途**（errVectorRebuilding）→ 503（服务端正忙：重建完成后再来；不是客户端错，
//     也不能拿重建中的空库当"没有命中"）；
//   - 查询参数非法（storage.ErrBadQuery：负 K / 空向量 / 维度不符 / 单批超上限 / 空文本
//     / 空查询）→ 400（客户端能改）；
//   - **上游 embedding 端点自己回了非 2xx**（errors.As 取出带状态码的错误，如
//     openai 的 *StatusError）→ **502**（上游 4xx：它拒了我们的请求）或 **503**
//     （上游 5xx / 429：它暂时不可用）。这不是宿主炸了，报 500 会把排查方向指错
//     （"宿主 500"会被当成宿主自己的 bug）；上游的错误体原样带给调用方（诊断要看到它）。
//   - 其余 → 500（真出事了）。
func writeVectorHubError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errVectorsDisabled):
		writeJSONError(w, http.StatusNotFound, "vectors_disabled", err.Error())
		return
	case errors.Is(err, errNoEmbeddingProvider):
		writeJSONError(w, http.StatusServiceUnavailable, "embedding_provider_unconfigured", err.Error())
		return
	case errors.Is(err, errVectorRebuilding):
		writeJSONError(w, http.StatusServiceUnavailable, "vector_rebuilding", err.Error())
		return
	case errors.Is(err, storage.ErrBadQuery):
		writeJSONError(w, http.StatusBadRequest, "bad_vector_query", err.Error())
		return
	}
	// 上游非 2xx（带状态码的错误）：4xx = 上游拒了我们的请求（502），5xx/429 = 上游暂时
	// 不可用（503）。errors.As **穿透包装**（host 侧的 %w 包装与 provider 侧的重试聚合
	// 都穿透得到底层的那个状态码错误）。
	var upstream upstreamStatusCarrier
	if errors.As(err, &upstream) {
		code := upstream.HTTPStatus()
		if code == http.StatusTooManyRequests || (code >= 500 && code <= 599) {
			writeJSONError(w, http.StatusServiceUnavailable, "upstream_embedding_unavailable",
				fmt.Sprintf("上游 embedding 端点不可用（HTTP %d）: %v", code, err))
			return
		}
		writeJSONError(w, http.StatusBadGateway, "upstream_embedding_rejected",
			fmt.Sprintf("上游 embedding 端点拒绝了请求（HTTP %d）: %v", code, err))
		return
	}
	writeJSONError(w, http.StatusInternalServerError, "vector_failed", err.Error())
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

// serveGateLeave 服务 POST /api/gates/leave（G15 显式 leave_gate；幂等）。
//
// 请求体与 join **同形**：{"session_id":"<ulid>","gate_id":"..."}。
// 200 → 离开后的 PeersView（已不在网内时直接返回当前可见面，**不落事件**——重复 leave 无害）；
// 400 → 体不是 JSON / session_id 不合法 / gate_id 不合法；404 → session 不受本 hub 托管。
//
// 回归口径（G15）：join→leave→不可见→rejoin 恢复；leave 落一条 durable gate.left
// （可回放：发现表从事件流重建后成员关系一致）。
func (h *Hub) serveGateLeave(w http.ResponseWriter, r *http.Request) {
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
	if _, ok := discovery.NormalizeGateID(body.GateID); !ok {
		writeJSONError(w, http.StatusBadRequest, "bad_gate_id",
			fmt.Sprintf("gate_id 不合法（空 / 超过 %d 字符 / 含空白或控制字符）: %q", discovery.MaxGateIDLen, body.GateID))
		return
	}
	view, err := h.LeaveGate(r.Context(), sid, body.GateID)
	if err != nil {
		writeHubError(w, err)
		return
	}
	if view.Peers == nil {
		view.Peers = []discovery.PeerRow{}
	}
	writeJSONBody(w, http.StatusOK, view)
}

// serveGates 服务 GET /api/gates?session_id=<ulid>（G15 枚举面）。
//
//	session_id 可省：省了 = 纯枚举（全部 gate 的存在性 + 活跃成员数，无成员详情）；
//	            带了  = caller 在网内的 gate 额外带成员详情（K11 分层口径，见 GatesView）。
//	200 → GatesView（gates 可能是空数组 = 还没有任何 gate）；
//	400 → session_id 不合法；500 → 读库失败。
func (h *Hub) serveGates(w http.ResponseWriter, r *http.Request) {
	var sid contracts.Ulid
	if raw := strings.TrimSpace(r.URL.Query().Get("session_id")); raw != "" {
		parsed, err := parseSessionID(raw)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "bad_session_id", err.Error())
			return
		}
		sid = parsed
	}
	view, err := h.Gates(r.Context(), sid)
	if err != nil {
		writeHubError(w, err)
		return
	}
	if view.Gates == nil {
		view.Gates = []GateView{}
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
