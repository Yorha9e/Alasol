package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/transport"
)

// 本文件是 **G1 遥控面**的协议审批 + 对话发起（v3-S4）：兑现 transport 契约承诺的
// command.submit / approval.request / approval.respond（此前 transport.json 的 kind
// description 写了这几个，server 却只实现 subscribe/ping——GUI 是"遥控面板"但遥不了对话）。
//
// # 两条腿，一个源头
//
//	对话发起：POST /api/sessions/{id}/prompt（HTTP）≡ WS command.submit（transport）
//	审批应答：POST /api/approvals/{id}/respond（HTTP）≡ WS approval.respond（transport）
//	审批请求：WS approval.request（服务端广播，无 HTTP 对应——它是实时推送）
//
// 四个入口最终都汇到本文件的两个原语：Hub.runPromptAsync（对话）+ protocolApprover（审批）。
// HTTP 与 WS **同源同口径**（都经 protocolApprover.Respond / runPromptAsync），不各写一套。
//
// # 托管口径（G2）：对话发起只对**本进程托管**的 session 有效
//
// 一个 session 的事件只由它的宿主进程 append（**G25 session 亲和写**：跨进程多写者会让
// 全局 seq = 提交序 ≠ 因果序）。协议面若对**别的进程**托管的 session 起 prompt，就会在
// 本进程**就地新建一个 loop**（host/hub.go 的 loopFor）→ 两个进程往同一个 session/agent
// 写事件 → G25 破。所以：
//
//   - HTTP POST /api/sessions/{id}/prompt：session 必须由本进程托管（hostsSession），
//     否则 404 unknown_session（与 /api/peers、/api/gates/join、/api/agentlink/* 同口径）；
//   - WS command.submit：**不带** session_id → 服务端新建一个 session（本进程即宿主，
//     这是协议面开新会话的唯一入口）；**带** session_id → 同样要求本进程托管，否则回
//     kind=error（不受理、不假受理）。
//
// # fail-close 铁律（D14/H8/陷阱 9）
//
// 无人应答、ctx 超时/取消、审批器缺席——一切非显式 allow 都落 **deny**，不得挂起、不得默认放行。
//
// # "已投递 ≠ 已放行"（应答的诚实口径）
//
// 审批应答被接受（HTTP 200 / WS 无错）只说明**应答已投递**到那个等待中的审批，
// **不代表这一轮被放行**：等待者可能在应答到达的同一瞬间 ctx 已结束（整轮超时/取消），
// 那时按 fail-close 仍落 deny（note 写"已收到应答但上下文已结束"，与"无人应答"区分）。
//
// # 鉴权口径（v0 的诚实边界）
//
// 本文件**不做鉴权**：任何能连上 hub 的客户端都能应答审批（loopback + 无 cookie，见
// host/doc.go 的安全边界一节）。fail-close 只保证"无人应答 = deny"，**不防乱批**
// （能连上的人可以抢先 approve 一条本不该放行的调用）。收紧方向记 v4（凭证 + 逐请求
// 身份 + 审批与身份绑定）。

// promptRunTimeout 是协议面发起的异步 prompt 的生命周期上限（**整轮**上限）。
//
// 为什么必须有界：协议 prompt 跑在后台 goroutine（HTTP 已返回 202），其 ctx 不随任何
// HTTP 请求取消；若无超时，一个卡在审批（无人应答）的 loop 会**永久挂起**。有了它，
// fail-close 有了兜底时钟——审批等到 promptRunTimeout 就按 deny 收场（契约：无人应答 = deny）。
//
// 口径（别读成别的）：这是**一轮**（一个 prompt 的全部 turn）的总预算，
// **不是**单次模型调用的超时，也**不是**单次审批等待的超时——一轮里可以有多次审批等待，
// 它们共用这份预算（预算耗尽 → 后续审批立即按 deny 收场）。
//
// 可配：host.Config.PromptRunTimeoutSec（秒；0 = 用这个默认值）；cmd 侧 -prompt-timeout。
const promptRunTimeout = 10 * time.Minute

// 请求体大小上限（loopback 上也别裸奔）。
const (
	maxPromptBody   = 1 << 20  // 1MB：prompt 原文可长，但不该无限
	maxApprovalBody = 64 << 10 // 64KB：审批应答是个小 JSON
)

// ---------------------------------------------------------------------------
// 协议审批器（core.Approver 的协议面实现）
// ---------------------------------------------------------------------------

// protocolApprover 把 core.Approver 的 ask 流接到协议面（G1）。
//
// ask 策略触发时 policy.Decide 调 Approve → 本实现把 ApprovalRequest 经 WS 广播
// （kind=approval.request）推给**所有**连接（GUI 在任何窗口都能应答），然后**挂起**等
// approval.respond 注入（HTTP/WS 都经 Respond 汇到这里）。
//
// 与 terminal.Approver 同哲学（"等人来决定"，不自己猜超时），但"人"在协议面（远端 GUI）
// 而非本进程 stdin。ctx 由 loop 的 Run ctx 带（协议 prompt 的 runPromptAsync 给了
// promptRunTimeout 上界）——超时即无人应答 → deny（fail-close）。
//
// 并发安全：mu 保护 pending 表；多个 session 的审批可并存（按 request_id 各等各的）。
type protocolApprover struct {
	// bc 广播 approval.request 给所有活动 WS 连接（实现是 *transport.Server）。
	bc transport.Broadcaster

	mu      sync.Mutex
	pending map[contracts.Ulid]*pendingApproval
}

// pendingApproval 是一条挂起的审批：应答通道 + 等待者存活标记。
//
// 为什么带存活标记：等待者（Approve）退出后，若表项还留着，迟到的应答会被"接受"
// （HTTP 回 200）却投进一个**没人读**的通道——客户端以为应答生效了，实际那一轮早已
// 按 deny 收场。等待者退出时在**同一把锁**下摘表项并把 alive 置否，投递与摘除因此是
// 串行的：要么应答先到（等待者看得到），要么等待者先走（应答被拒，不假成功）。
type pendingApproval struct {
	// ch 是应答通道（buffered(1)：投递不阻塞，通道只此一次投递）。
	ch chan contracts.ApprovalResponse
	// alive 表示等待者是否还在等（false = 等待者已退出，不再接受应答）。
	alive bool
}

// 编译期断言：*protocolApprover 满足 core.Approver。
var _ core.Approver = (*protocolApprover)(nil)

// newProtocolApprover 建协议审批器（bc 必填：广播口；nil 也能构造但请求推不出去 →
// 只能等 ctx 超时兜底 deny，故真实装配总传 transport server）。
func newProtocolApprover(bc transport.Broadcaster) *protocolApprover {
	return &protocolApprover{bc: bc, pending: map[contracts.Ulid]*pendingApproval{}}
}

// Approve implements core.Approver。
//
// 返回 error 恒为 nil：拒绝是**正常结论**（fail-close 的 deny），不是异常——
// 与 terminal.Approver / policy.Decide 的 approveSafely 同口径。
func (a *protocolApprover) Approve(ctx context.Context, req contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	if a == nil {
		// 有类型的 nil 接收者：无人可应答 → deny（fail-close）。
		return denyApproval(req, "fail-close: protocolApprover 为 nil（无人可应答）"), nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// ctx 已结束 → 这次审批注定无人应答 → deny，连广播都不必（fail-close）。
	if err := ctx.Err(); err != nil {
		return denyApproval(req, fmt.Sprintf("fail-close: 审批前上下文已结束（%v）", err)), nil
	}

	p := &pendingApproval{ch: make(chan contracts.ApprovalResponse, 1), alive: true}
	a.mu.Lock()
	a.pending[req.ID] = p
	a.mu.Unlock()

	// 广播请求给所有连接（任何窗口的 GUI 都能应答）。
	if a.bc != nil {
		a.bc.Broadcast(transport.KindApprovalRequest, req)
	}

	select {
	case resp := <-p.ch:
		// 照抄 terminal.Approve 的口径（runtime/approver/terminal/approver.go）：收到答复
		// 后再看一眼 ctx——等答复等太久（ctx 已取消/超时）宁可当没答（fail-close）。
		// note 与"无人应答"区分：那轮**有人答了**，只是没赶上（已投递 ≠ 已放行）。
		a.retire(req.ID, p)
		if err := ctx.Err(); err != nil {
			return denyApproval(req, fmt.Sprintf(
				"fail-close: 已收到应答但上下文已结束（%v）：应答已投递，未放行", err)), nil
		}
		return resp, nil
	case <-ctx.Done():
		// 摘除自己（锁内）：此后 Respond 不会再投递给这条审批（表项没了 → 迟到应答被拒）。
		a.retire(req.ID, p)
		// 竞态复核：ctx 取消与应答投递可能同时发生（应答已被接受、HTTP 已回 200）。
		// 通道里还有应答 → 按"已收到应答但上下文已结束"收场（审计不写"无人应答"）；
		// 通道空 → 才是真正的无人应答。
		select {
		case <-p.ch:
			return denyApproval(req, fmt.Sprintf(
				"fail-close: 已收到应答但上下文已结束（%v）：应答已投递，未放行", ctx.Err())), nil
		default:
		}
		return denyApproval(req, fmt.Sprintf("fail-close: 无人应答（ctx 超时/取消：%v）", ctx.Err())), nil
	}
}

// Respond 注入一条审批应答（HTTP /api/approvals/{id}/respond 与 WS approval.respond 都经此）。
//
// 返回 false = **没有接受**这条应答：
//
//   - decision 不合法（allow|deny 之外）→ 拒（且**不消费**挂起的审批：一条乱批不该把
//     等着人审的审批吃掉——调用方负责给出更准的错误文案）；
//   - 没有这条挂起的审批（已超时 fail-close / 已应答 / request_id 不存在 / 等待者已退出）
//     → 拒（迟到的应答不该假装成功——那个 loop 早已按 deny 收场）。
//
// 成功 = 应答**已投递**给等待者（≠ 已放行：等待者可能同时 ctx 结束，见 Approve）。
func (a *protocolApprover) Respond(resp contracts.ApprovalResponse) bool {
	// 决策校验在**这里**（HTTP/WS 两面共用同一处）：非法决策不消费 pending。
	if !validApprovalDecision(resp.Decision) {
		return false
	}
	a.mu.Lock()
	p, ok := a.pending[resp.RequestID]
	if ok {
		delete(a.pending, resp.RequestID)
	}
	// 锁内确认等待者还活着（等待者退出时会摘掉表项并置 alive=false），
	// 并在锁内投递（buffered(1)：不阻塞；与摘除串行 → 不会投进没人读的通道）。
	delivered := false
	if ok && p.alive {
		p.ch <- resp
		delivered = true
	}
	a.mu.Unlock()
	return delivered
}

// retire 摘除一条挂起的审批并把等待者标记为"不再等"（幂等；ctx 超时/取消路径用）。
//
// 摘除与投递都在 a.mu 下完成 → 两者**串行**：要么应答先到（等待者看得到），要么等待者
// 先走（之后的应答被拒，不假成功）。
func (a *protocolApprover) retire(id contracts.Ulid, p *pendingApproval) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if cur, ok := a.pending[id]; ok && cur == p {
		delete(a.pending, id)
	}
	if p != nil {
		p.alive = false
	}
}

// validApprovalDecision 校验审批决策（allow | deny）。
//
// **两面共用**（HTTP /api/approvals/{id}/respond 与 WS approval.respond）：
// 非法决策一律拒，且**不消费**挂起的审批（乱批不该吃掉等着人审的那一条）。
func validApprovalDecision(d contracts.ApprovalDecision) bool {
	switch d {
	case contracts.ApprovalDecisionAllow, contracts.ApprovalDecisionDeny:
		return true
	default:
		return false
	}
}

// denyApproval 造一条 fail-close 拒绝响应（形状对齐 core.DenyAll / terminal 的 denyResponse）。
func denyApproval(req contracts.ApprovalRequest, note string) contracts.ApprovalResponse {
	return contracts.ApprovalResponse{
		V:         1,
		RequestID: req.ID,
		TS:        req.TS,
		Decision:  contracts.ApprovalDecisionDeny,
		Note:      &note,
		DecidedBy: strPtrApproval("auto:protocol-fail-close"),
	}
}

func strPtrApproval(s string) *string { return &s }

// ---------------------------------------------------------------------------
// 异步 prompt 跑廊（HTTP / WS command.submit 共用）
// ---------------------------------------------------------------------------

// runPromptAsync 在后台跑一轮 prompt（带 promptRunTimeout 上界——**整轮**上限：
// 这一轮的所有 turn 与其中的审批等待共用这份预算）。
//
// **ctx 派生自 hub 生命周期**（h.lifeCtx，Close 时取消）：hub 关掉 → 这一轮立刻收场
// （fail-close：取消即 deny），不会在**事件库关掉之后**才醒过来往已关的库写事件。
//
// 结果**不在本函数返回**（调用方是 HTTP handler / WS inbound，早已返回）——它们经事件流
// （user.prompt / model.turn / tool.call.* / context.assembled …）回流，客户端订阅即见。
// 跑失败只记日志（事件流已留痕到断裂点；投影/发现表的 catch-up 由后续 API 请求兜）。
func (h *Hub) runPromptAsync(sid contracts.Ulid, prompt string) {
	timeout := promptRunTimeout
	if h.cfg.PromptRunTimeoutSec > 0 {
		timeout = time.Duration(h.cfg.PromptRunTimeoutSec) * time.Second
	}
	parent := h.lifeCtx
	if parent == nil {
		parent = context.Background() // 兜底：没装配生命周期的 Hub（不该发生）
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	res, err := h.RunPrompt(ctx, sid, prompt)
	if err != nil {
		hubLog.Warn("host: 协议面 prompt 跑失败（事件流已留痕到断裂点）",
			"session_id", string(sid), "err", err.Error())
		return
	}
	hubLog.Info("host: 协议面 prompt 跑完", "session_id", string(sid), "turns", res.Turns)
}

// startPromptAsync 登记并异步跑一轮协议面 prompt（HTTP / WS 两个面的共同入口）。
//
// 返回 false = hub 已关闭（不再受理新的协议面 prompt；调用方按 503/错误回）。
//
// 为什么要"先登记再起 goroutine"：Close 要**等**这些 goroutine 收场再关事件库
// （见 waitPrompts）。WaitGroup 的 Add 必须发生在 Wait **之前**，否则是误用（会 panic）——
// 而"起 goroutine"这一步与 Close 是并发的，所以 Add 必须在起 goroutine **之前**、
// 且在 h.mu 下与 Close 的"已关闭"标记串起来（Close 之后不再 Add）。
func (h *Hub) startPromptAsync(sid contracts.Ulid, prompt string) bool {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return false
	}
	h.promptWG.Add(1)
	h.mu.Unlock()
	go func() {
		defer h.promptWG.Done()
		h.runPromptAsync(sid, prompt)
	}()
	return true
}

// ---------------------------------------------------------------------------
// HTTP 端点（G1）：对话发起 + 审批应答
// ---------------------------------------------------------------------------

// servePrompt 服务 POST /api/sessions/{session_id}/prompt（G1 对话发起）。
//
//	请求体：{"prompt":"…"}（空 = 400）
//	202 → {"session_id":…,"status":"running"}（**异步**：受理即返回，结果经事件流回流）
//	400 → session_id 非法 / 体坏 / prompt 空
//	404 → 该 session **不由本进程托管**（unknown_session；G2 口径 + G25 session 亲和写：
//	      对别的进程托管的 session 起 prompt = 本进程就地新建 loop 双写同一 agent）
//
// 托管口径（P2-2）：本端点是**托管型写面**，与 /api/peers、/api/gates/join、
// /api/agentlink/* 同口径——只对**本进程托管**的 session 生效。协议面**开新会话**走
// WS command.submit（不带 session_id → 服务端新建，本进程即宿主）。
func (h *Hub) servePrompt(w http.ResponseWriter, r *http.Request) {
	sid, err := parseSessionID(r.PathValue("session_id"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_session_id", err.Error())
		return
	}
	var body struct {
		Prompt string `json:"prompt"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, maxPromptBody)).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_body", "请求体不是合法 JSON 对象: "+err.Error())
		return
	}
	if strings.TrimSpace(body.Prompt) == "" {
		writeJSONError(w, http.StatusBadRequest, "empty_prompt", "prompt 不能为空（空 prompt 等于浪费一次模型调用）")
		return
	}
	// 托管校验（G2 口径）：别的进程托管的 session 不在这儿跑——那会让两个进程往同一个
	// session/agent 写事件（G25 破）。404 与 peers/gates/agentlink 同口径（unknown_session）。
	if !h.hostsSession(sid) {
		writeJSONError(w, http.StatusNotFound, "unknown_session",
			fmt.Sprintf("session %s 不是本 hub 托管的 session（它由别的进程托管，或还没在本进程起过对话）", sid))
		return
	}
	if !h.startPromptAsync(sid, body.Prompt) {
		// hub 已关闭（正在收尾）：不受理（受理了也不会真跑）。
		writeJSONError(w, http.StatusServiceUnavailable, "hub_closed", "hub 正在关闭，不受理新的协议面 prompt")
		return
	}
	writeJSONBody(w, http.StatusAccepted, map[string]any{
		"session_id": sid,
		"status":     "running",
	})
}

// serveApprovalRespond 服务 POST /api/approvals/{request_id}/respond（G1 审批应答）。
//
//	请求体：{"decision":"allow"|"deny","note":"…","decided_by":"…"}（note/decided_by 可选）
//	200 → {"request_id":…,"decision":…}（应答**已投递**给等待中的审批；≠ 已放行——
//	      等待者若同时 ctx 结束，仍按 fail-close 落 deny）
//	400 → request_id 非法 / 体坏 / decision 不在 allow|deny
//	404 → 没有这条挂起的审批（已超时 fail-close / 已应答 / 不存在 / 等待者已退出）
func (h *Hub) serveApprovalRespond(w http.ResponseWriter, r *http.Request) {
	rid := contracts.Ulid(strings.TrimSpace(r.PathValue("request_id")))
	if !core.ValidUlid(rid) {
		writeJSONError(w, http.StatusBadRequest, "bad_request_id",
			fmt.Sprintf("request_id 不是合法 ULID（26 字符 Crockford base32）: %q", r.PathValue("request_id")))
		return
	}
	var body struct {
		Decision  string  `json:"decision"`
		Note      *string `json:"note"`
		DecidedBy *string `json:"decided_by"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, maxApprovalBody)).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_body", "请求体不是合法 JSON 对象: "+err.Error())
		return
	}
	decision := contracts.ApprovalDecision(strings.TrimSpace(body.Decision))
	// 决策校验与 WS 面**共用**同一处判据（validApprovalDecision；非法决策不消费 pending）。
	if !validApprovalDecision(decision) {
		writeJSONError(w, http.StatusBadRequest, "bad_decision",
			fmt.Sprintf("decision 必须是 allow | deny（得到 %q）", body.Decision))
		return
	}
	resp := contracts.ApprovalResponse{
		V:         1,
		RequestID: rid,
		TS:        time.Now().UTC().Format(time.RFC3339Nano),
		Decision:  decision,
		Note:      body.Note,
		DecidedBy: body.DecidedBy,
	}
	if resp.DecidedBy == nil {
		resp.DecidedBy = strPtrApproval("user:protocol")
	}
	if h.protocolApprover == nil || !h.protocolApprover.Respond(resp) {
		writeJSONError(w, http.StatusNotFound, "no_pending_approval",
			fmt.Sprintf("没有 request_id=%s 的挂起审批（已超时 fail-close / 已应答 / 不存在）", rid))
		return
	}
	writeJSONBody(w, http.StatusOK, map[string]any{
		"request_id": rid,
		"decision":   string(decision),
	})
}

// ---------------------------------------------------------------------------
// WS 入站处理器（transport.InboundHandler）：command.submit / approval.respond
// ---------------------------------------------------------------------------

// 编译期断言：*Hub 满足 transport.InboundHandler。
var _ transport.InboundHandler = (*Hub)(nil)

// HandleInbound 处理 WS 入站的 command.submit / approval.respond（G1 遥控面的 WS 侧；
// HTTP 侧是 /api/sessions/{id}/prompt 与 /api/approvals/{id}/respond，四入口同源）。
//
// 返回要回给客户端的应答消息（nil = 只收不回，效果经事件流可见）；返回 error 时
// transport 回一条 kind=error（reply_to 指向原消息）但**不关连接**（D7 宽容）。
//
// 口径与 HTTP 面**同源**（G2/P3-3/P3-4）：
//
//   - command.submit：session_id 必须是合法 ULID（非法 → kind=error，**不假受理**）；
//     显式给了 session_id → 还要本进程托管（否则 kind=error）；没给 → 服务端新建
//     （本进程即宿主，这是协议面开新会话的入口）。
//   - approval.respond：decision 必须是 allow|deny（非法 → kind=error，且**不消费**
//     挂起的审批——乱批不该吃掉等着人审的那一条）；没有挂起的审批 → kind=error。
func (h *Hub) HandleInbound(_ context.Context, m contracts.TransportMessage) (*contracts.TransportMessage, error) {
	switch m.Kind {
	case transport.KindCommandSubmit:
		var cs contracts.CommandSubmit
		if err := json.Unmarshal(m.Payload, &cs); err != nil {
			return nil, fmt.Errorf("command.submit 载荷解不开: %w", err)
		}
		if strings.TrimSpace(cs.Prompt) == "" {
			return nil, errors.New("command.submit 的 prompt 不能为空")
		}
		// session：payload.session_id 优先，退到 envelope.session_id，再退到新建。
		// 显式给出的 session_id 一律按 parseSessionID 校验（非法 → error，不假受理：
		// 受理回执说"running"，可那个 session 根本不存在，客户端会一直等一个不会来的事件）。
		var sid contracts.Ulid
		explicit := false
		switch {
		case cs.SessionID != nil:
			parsed, err := parseSessionID(string(*cs.SessionID))
			if err != nil {
				return nil, fmt.Errorf("command.submit 的 session_id 非法: %w", err)
			}
			sid, explicit = parsed, true
		case m.SessionID != nil:
			parsed, err := parseSessionID(string(*m.SessionID))
			if err != nil {
				return nil, fmt.Errorf("command.submit 的 session_id 非法: %w", err)
			}
			sid, explicit = parsed, true
		default:
			sid = NewSession()
		}
		if explicit && !h.hostsSession(sid) {
			// 托管校验（与 HTTP prompt 面同口径）：别的进程托管的 session 不在这儿跑
			// （那会让两个进程往同一个 session/agent 写事件 → G25 破）。
			return nil, fmt.Errorf(
				"session %s 不是本 hub 托管的 session（它由别的进程托管，或还没在本进程起过对话）；"+
					"要开新会话就别带 session_id（服务端新建）", sid)
		}
		if !h.startPromptAsync(sid, cs.Prompt) {
			// hub 已关闭（正在收尾）：不受理（受理了也不会真跑）。
			return nil, errors.New("hub 正在关闭，不受理新的协议面 prompt")
		}
		reply, err := transport.NewMessage(transport.KindCommandAccepted,
			contracts.CommandAccepted{SessionID: sid, Status: "running"})
		if err != nil {
			return nil, err
		}
		return &reply, nil

	case transport.KindApprovalRespond:
		var resp contracts.ApprovalResponse
		if err := json.Unmarshal(m.Payload, &resp); err != nil {
			return nil, fmt.Errorf("approval.respond 载荷解不开: %w", err)
		}
		// 决策校验与 HTTP 面**共用**同一处判据（validApprovalDecision）。非法决策：
		// 回 kind=error（不再"静默吃掉挂起的审批 + 零回执"），且 pending 不被消费。
		if !validApprovalDecision(resp.Decision) {
			return nil, fmt.Errorf("approval.respond 的 decision 必须是 allow | deny（得到 %q）；"+
				"该应答未被受理，挂起的审批仍在等待", resp.Decision)
		}
		if h.protocolApprover == nil || !h.protocolApprover.Respond(resp) {
			return nil, fmt.Errorf("没有 request_id=%s 的挂起审批（已超时 fail-close / 已应答 / 不存在）", resp.RequestID)
		}
		// 受理即成功；loop 继续的效果（tool.call.completed 等）经事件流可见，无需额外回执。
		// 注意语义：**已投递 ≠ 已放行**——等待者若同时 ctx 结束，仍按 fail-close 落 deny。
		return nil, nil

	default:
		return nil, fmt.Errorf("不支持的入站 kind=%q", m.Kind)
	}
}
