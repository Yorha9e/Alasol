package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/runtime/identity"
)

// 本文件是 hub 与发现表（K10）之间的装配层：**拉式扫描喂事件** + 自动/显式 gate（K12）
// + 水平可见性查询（K11①）。core/discovery 自己是纯状态机（不做 IO），扫描与落事件都在
// 这里——host 是唯一组合根（09）。
//
// 数据流入选**扫描**不选 bus 订阅，与 host/doc.go 的在线表同口径：订阅会和扫描重复计数
// （同一事件两条路都到），而重复计数比漏计数难发现；扫描另有持久水位、可重放、幂等。

// discoveryScanBatch 是发现表单批扫描的事件条数（与索引器/投影同量级）。
const discoveryScanBatch = 500

// ErrSessionNotHosted 表示"这个 session 不是本 hub 在托管的 session"（没有它的 loop）。
//
// 用它把"客户端拿了一个野 session id"与"服务内部出错"分开：HTTP 层据此回 404 而不是
// 500，更重要的是——**不放任野 session 进事件流**（给它落一条 gate.joined 就是往唯一
// 事实源里写垃圾）。
var ErrSessionNotHosted = errors.New("host: session 不是本 hub 在托管的 session")

// worktreeIsolationNotice 是"worktree 绑定**未**启用工具级隔离"的醒目告警出口。
//
// 走 slog.Default() 且**调用时**取（不是包级 var）：这样宿主换掉默认 logger 就能接住它、
// 测试也能断言"绑定时确实告警了"（host/worktree_test.go）。默认输出与 hubLog 一致。
func worktreeIsolationNotice(msg string, args ...any) {
	slog.Default().Warn(msg, args...)
}

// SessionSpec 是新建 session 的归属（K12 自动 gate 的输入）。
//
// v1-S2 只用到 CWD/RunID；多 worktree 隔离（v1-S5 D）加 Worktree：把一个 session **登记**到
// 某个 hub 托管的 worktree —— **仅影响自动 gate 的 repo_id 计算与工作区登记**
// （自动 gate 的 repo_id 按 worktree 路径算——经 git root 聚合，与主仓库同一 repo_id，S14）。
// gate 机制本身不随绑定改变（成员关系按 session 算，与 session 从哪来无关）。
//
// ⚠ **worktree 绑定目前不提供工具级隔离**：工具（read/write/bash）由 hub **一次性注册**
// （host/hub.go：tools.NewRead(local.FS{}) 等）——相对路径落到**进程 cwd**、bash 继承进程 cwd。
// 也就是说：绑了 worktree 的 session 写文件**不会**落在该 worktree 里（实测：落在进程 cwd）。
// 把工具执行真正 chroot 进各 worktree 要改 core/loop 的工具执行上下文（与 X1 冲突），
// 留作后续项；CreateSession 命中 Worktree 时会打一条醒目告警（见 worktreeIsolationNotice）。
type SessionSpec struct {
	// CWD 工作目录。自动 gate repo:<repo_id> 按它算（runtime/identity 的 RepoID，
	// 复用其 worktree 聚合语义：同 git root 的多个 worktree 聚成同一 repo_id）。
	// 空 = 用 hub 级默认（Config.CWD，再缺省用进程 cwd）。设了 Worktree 时以 worktree 路径为准。
	// **不影响工具执行**（工具仍在进程 cwd——见类型注释的 ⚠）。
	CWD string
	// RunID 工作流运行 id。有则自动加入 workflow:<run_id>；空 = 不加。
	// 工作流引擎在 v3，这里只留钩子（没有 run_id 就没有 workflow gate）。
	RunID string
	// Worktree 是把本 session **登记**到哪个 hub 托管的 worktree（名字，非路径）。
	// 非空 → CreateSession 校验它存在，并按该 worktree 路径算自动 gate 的 repo_id。
	// ⚠ **不提供工具级隔离**（工具仍在进程 cwd 执行）——见类型注释。
	Worktree string
}

// PeersView 是水平可见性查询的形状（GET /api/peers 与 POST /api/gates/join 共用）。
type PeersView struct {
	SessionID string `json:"session_id"`
	// Gates 是本 session 当前的 gate 成员关系（字典序）。
	Gates []string `json:"gates"`
	// Peers 是经 gate 相交可见的对端**主 agent**（K11①；别 session 的 subagent 不在此列）。
	Peers []discovery.PeerRow `json:"peers"`
}

// autoGate 是一个待加入的自动 gate（id + 来源）。
type autoGate struct {
	gateID string
	source contracts.GateJoinSource
}

// discoveryFeed 把事件流**拉式扫**进发现表（见文件头：为什么不是 bus 订阅）。
//
// 并发：一把互斥锁保护扫描水位与表（发现表自身还有锁，但"读水位→读事件→Apply→推水位"
// 要整段原子，否则并发 catchUp 会重复 Apply 同一批——本表 Apply 幂等所以不会算错，
// 但没必要白扫）。
type discoveryFeed struct {
	table *discovery.Table
	store core.Store

	mu      sync.Mutex
	scanSeq int64

	// forks 是已知的 **/btw 旁支 fork session** 集合（v3-S2 K29）。
	//
	// 为什么 host 侧维护（core/discovery 不改，X1 纪律）：fork 标记目前只落在**投影**
	// （在线表）里，发现表没有"这是一条旁支"的概念——两张表对同一个 session 的可见性
	// 于是分叉（在线表不可见 / 发现表看得见）。这里把 fork session 显式记下来，
	// 让 presence/peers 面**统一过滤**（旁支不是一等会话），而不是靠"它恰好没 join
	// gate、恰好没被激活"这种副产物兜着。
	//
	// 数据来源是**事件流**（btw.forked）：进程重启后重扫即恢复（与在线表的 fork
	// 标记同源同口径）。
	forks map[contracts.Ulid]struct{}
}

func newDiscoveryFeed(store core.Store, table *discovery.Table) *discoveryFeed {
	return &discoveryFeed{table: table, store: store}
}

// catchUp 增量扫描事件流并喂进发现表，返回本批处理条数。
//
// 与 projection.catchUp 同构：水位只前进不后退；重启后从 0 起扫即全量重建。
func (f *discoveryFeed) catchUp(ctx context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	total := 0
	for {
		evs, err := f.store.ReadFrom(ctx, nil, f.scanSeq+1, discoveryScanBatch)
		if err != nil {
			return total, err
		}
		if len(evs) == 0 {
			return total, nil
		}
		for i := range evs {
			f.table.Apply(&evs[i])
			f.noteForkEventLocked(&evs[i])
			if evs[i].Seq != nil && *evs[i].Seq > f.scanSeq {
				f.scanSeq = *evs[i].Seq
			}
		}
		total += len(evs)
		if len(evs) < discoveryScanBatch {
			return total, nil
		}
	}
}

// rebuild 重建（验收②）：清空发现表 + 水位归零，再从 seq 0 全量重放。
func (f *discoveryFeed) rebuild(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.table.Reset()
	f.scanSeq = 0
	for {
		evs, err := f.store.ReadFrom(ctx, nil, 1, discoveryScanBatch)
		if err != nil {
			return err
		}
		if len(evs) == 0 {
			return nil
		}
		for i := range evs {
			f.table.Apply(&evs[i])
			f.noteForkEventLocked(&evs[i])
			if evs[i].Seq != nil && *evs[i].Seq > f.scanSeq {
				f.scanSeq = *evs[i].Seq
			}
		}
		if len(evs) < discoveryScanBatch {
			return nil
		}
	}
}

// watermark 返回发现表已扫到的全局 seq（诊断用）。
func (f *discoveryFeed) watermark() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.scanSeq
}

// noteForkEventLocked 记下事件流里的 fork 标记（调用方持有 f.mu）。
//
// btw.forked 事件的**归属 session** 就是那条旁支（envelope.session_id = fork id），
// 故按 envelope 记，不看载荷里的 source_session_id（那是主 session）。
func (f *discoveryFeed) noteForkEventLocked(ev *contracts.Event) {
	if ev == nil || ev.Kind != KindBtwForked || ev.SessionID == "" {
		return
	}
	if f.forks == nil {
		f.forks = make(map[contracts.Ulid]struct{})
	}
	f.forks[ev.SessionID] = struct{}{}
}

// markFork 登记一个 fork session（RunBtw 造出旁支时立即登记，不等事件流扫到）。
func (f *discoveryFeed) markFork(sid contracts.Ulid) {
	if f == nil || sid == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forks == nil {
		f.forks = make(map[contracts.Ulid]struct{})
	}
	f.forks[sid] = struct{}{}
}

// isFork 报告某 session 是否是一条 /btw 旁支 fork。
func (f *discoveryFeed) isFork(sid contracts.Ulid) bool {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.forks[sid]
	return ok
}

// forkSessions 返回已知的 fork session 集合（升序；审计/诊断用）。
func (f *discoveryFeed) forkSessions() []contracts.Ulid {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]contracts.Ulid, 0, len(f.forks))
	for sid := range f.forks {
		out = append(out, sid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ---------------------------------------------------------------------------
// Hub 侧的 gate / 可见性 API
// ---------------------------------------------------------------------------

// CreateSession 生成新 session id 并登记它的归属（CWD / run_id），返回 id。
//
// 调用方先拿 id 再 RunPrompt（与包级 NewSession 的关系：那个不登记归属，自动 gate 退到
// hub 级默认）。**登记不等于激活**——自动 gate 在该 session 的 loop 建成时（第一次
// RunPrompt）才加入，因为 gate.joined 事件的 envelope 必须带 agent_id（契约必填），
// 而主 agent 的 id 在 loop 建成时才知道。
func (h *Hub) CreateSession(spec SessionSpec) (contracts.Ulid, error) {
	if h == nil {
		return "", errors.New("host: Hub 为 nil")
	}
	cwd := strings.TrimSpace(spec.CWD)
	wtName := strings.TrimSpace(spec.Worktree)
	if wtName != "" {
		// 登记到 worktree：校验它存在，并按 worktree 路径算自动 gate 的 repo_id。
		// ⚠ 工具级隔离未启用（工具仍在进程 cwd）——见 SessionSpec 的注释。
		wt, ok := h.worktrees.Get(wtName)
		if !ok {
			return "", fmt.Errorf("host: worktree %q 不存在（先用 CreateWorktree 建）", wtName)
		}
		cwd = wt.Path
		// 醒目告警：别让用户以为"绑了 worktree = 工具在 worktree 里跑"。
		worktreeIsolationNotice("⚠ 工具级隔离未启用：工具仍在进程 cwd 执行，worktree 绑定仅影响 gate/登记",
			"worktree", wtName, "worktree_path", wt.Path)
	}
	if cwd != "" {
		// 提前把路径规范化一遍（S13）：不合法的目录在这里就拒，别等到激活时才发现。
		canonical, err := identity.CanonicalPath(cwd)
		if err != nil {
			return "", fmt.Errorf("host: session 工作目录 %q 不可用: %w", cwd, err)
		}
		cwd = canonical
	}
	runID := strings.TrimSpace(spec.RunID)
	if runID != "" && !discovery.ValidGateID(discovery.WorkflowGate(runID)) {
		return "", fmt.Errorf("host: run_id %q 不合法（含空白/控制字符，或拼成 workflow gate 后超过 %d 字符）",
			spec.RunID, discovery.MaxGateIDLen)
	}
	sid := core.NewUlid()
	h.mu.Lock()
	h.specs[sid] = SessionSpec{CWD: cwd, RunID: runID, Worktree: wtName}
	h.mu.Unlock()
	return sid, nil
}

// Discovery 返回发现表（宿主内部 / 诊断面：K10 的查询 API 都在它上面）。
//
// **不要把 Agents() / Agent() 挂到 HTTP 上**：K11 的水平可见性只到"对端 session 的主
// agent"（VisiblePeers），跨 session 的 agent 清单是宿主内部视图。
func (h *Hub) Discovery() *discovery.Table {
	if h == nil {
		return nil
	}
	return h.disc
}

// Peers 查某 session 的水平可见对端（K11①）。
func (h *Hub) Peers(ctx context.Context, sessionID contracts.Ulid) (PeersView, error) {
	if h == nil {
		return PeersView{}, errors.New("host: Hub 为 nil")
	}
	if !h.hostsSession(sessionID) {
		return PeersView{}, fmt.Errorf("%w: %s", ErrSessionNotHosted, sessionID)
	}
	return h.peersView(ctx, sessionID)
}

// JoinGate 显式加入一个 gate（K12 的 join_gate），返回加入后的可见面。
//
// 幂等：已在网内的重复 join **不落重复事件**（F7 只增不减不等于"同一请求重复记一笔"），
// 直接返回当前可见面。session 必须由本 hub 托管（否则 404，见 ErrSessionNotHosted）。
//
// G15：leave 之后的**重 join** 会落一条**新一届**的 gate.joined（届数编进确定性事件 id，
// 见 newGateJoinedEvent 与 discovery.JoinGeneration）——否则重 join 的事件会被 store 的
// 去重当成"第一次那条"吃掉，成员关系永远恢复不了。
func (h *Hub) JoinGate(ctx context.Context, sessionID contracts.Ulid, gateID string) (PeersView, error) {
	if h == nil {
		return PeersView{}, errors.New("host: Hub 为 nil")
	}
	if !core.ValidUlid(sessionID) {
		return PeersView{}, fmt.Errorf("host: session_id 不是合法 ULID（26 字符 Crockford base32）: %q", sessionID)
	}
	id, ok := discovery.NormalizeGateID(gateID)
	if !ok {
		return PeersView{}, fmt.Errorf("host: gate_id 不合法（空 / 超过 %d 字符 / 含空白或控制字符）: %q",
			discovery.MaxGateIDLen, gateID)
	}
	if !h.hostsSession(sessionID) {
		return PeersView{}, fmt.Errorf("%w: %s", ErrSessionNotHosted, sessionID)
	}
	agentID, ok := h.mainAgentOf(sessionID)
	if !ok {
		return PeersView{}, fmt.Errorf("host: session %s 没有主 agent（先跑一轮 prompt）", sessionID)
	}

	// 「判重 + 发布」收敛进一把 **per-session** 锁：判重（InGate）与发布
	// （bus.Publish）之间不是原子的——两个并发 join 都过判重 → 各落一条
	// gate.joined（文档宣称的幂等只对**顺序**调用成立）。
	//
	// 锁的粒度是 session：同一 session 的 join/leave 串行，不同 session 互不阻塞。
	// 跨进程不需要：session 由一个 hub 托管（session 亲和写，G25）。
	jm := h.joinMutex(sessionID)
	jm.Lock()
	defer jm.Unlock()

	// 先扫平再判重：发现表的水位可能还没追上刚发布的事件。
	if _, err := h.discFeed.catchUp(ctx); err != nil {
		return PeersView{}, fmt.Errorf("host: 扫事件流: %w", err)
	}
	if h.disc.InGate(sessionID, id) {
		return h.viewOf(sessionID), nil // 幂等：不重复落事件（刚扫平过，直接取）
	}

	// 届数 = 已折叠的 join 次数（leave 之后的重 join 是新的一届 → 新 id）。
	gen := h.disc.JoinGeneration(sessionID, id)
	ev, err := newGateJoinedEvent(sessionID, agentID, autoGate{gateID: id, source: contracts.GateJoinSourceExplicit}, gen)
	if err != nil {
		return PeersView{}, err
	}
	if err := h.bus.Publish(ctx, ev); err != nil {
		return PeersView{}, fmt.Errorf("host: 落 gate.joined 事件: %w", err)
	}
	// 落完立刻扫平：返回的可见面必须已经包含这次 join（否则客户端刚 join 完看到的还是
	// 旧成员关系，会以为没生效）。扫失败不致命——事件已 durable，下次请求会追平。
	if _, err := h.discFeed.catchUp(ctx); err != nil {
		hubLog.Warn("host: join 后发现表 catch-up 失败（下次 API 请求会重试）",
			"session_id", string(sessionID), "err", err.Error())
	}
	return h.viewOf(sessionID), nil
}

// LeaveGate 显式离开一个 gate（G15：补"只能加不能减"的缺漏），返回离开后的可见面。
//
// 幂等：已不在网内的重复 leave **不落事件**、直接返回当前可见面（"重复 leave 无害"）。
// session 必须由本 hub 托管（否则 404，见 ErrSessionNotHosted）——与 JoinGate 同口径。
//
// 成员折叠口径（core/discovery）：成员 = joined 减 left；leave 之后再 join 恢复可见
// （重 join 是新一届事件，见 JoinGate）。
func (h *Hub) LeaveGate(ctx context.Context, sessionID contracts.Ulid, gateID string) (PeersView, error) {
	if h == nil {
		return PeersView{}, errors.New("host: Hub 为 nil")
	}
	if !core.ValidUlid(sessionID) {
		return PeersView{}, fmt.Errorf("host: session_id 不是合法 ULID（26 字符 Crockford base32）: %q", sessionID)
	}
	id, ok := discovery.NormalizeGateID(gateID)
	if !ok {
		return PeersView{}, fmt.Errorf("host: gate_id 不合法（空 / 超过 %d 字符 / 含空白或控制字符）: %q",
			discovery.MaxGateIDLen, gateID)
	}
	if !h.hostsSession(sessionID) {
		return PeersView{}, fmt.Errorf("%w: %s", ErrSessionNotHosted, sessionID)
	}
	agentID, ok := h.mainAgentOf(sessionID)
	if !ok {
		return PeersView{}, fmt.Errorf("host: session %s 没有主 agent（先跑一轮 prompt）", sessionID)
	}

	// 与 join 共用 per-session 锁：「判活跃 + 发布」要原子——两个并发 leave 都过判定 →
	// 各落一条 gate.left（幂等只对顺序调用成立）。
	jm := h.joinMutex(sessionID)
	jm.Lock()
	defer jm.Unlock()

	if _, err := h.discFeed.catchUp(ctx); err != nil {
		return PeersView{}, fmt.Errorf("host: 扫事件流: %w", err)
	}
	if !h.disc.InGate(sessionID, id) {
		return h.viewOf(sessionID), nil // 幂等：不在网内 → 不落事件
	}
	// leave 的 payload 带"被终止的成员关系的来源"（与对应 gate.joined 的 source 同值）。
	cur, ok := h.disc.ActiveJoin(sessionID, id)
	if !ok {
		// 理论上不可达（InGate 为真就有活跃 join）；真发生就别猜 source，按显式加入记账。
		cur = discovery.GateJoin{GateID: id, Source: string(contracts.GateJoinSourceExplicit)}
	}
	// 被终止的那一届 = 最近一次 join 的届数（已折叠 join 次数 - 1）。确定性 id 让
	// 重复 leave 在存储层再去重一次（幂等双保险）。
	gen := h.disc.JoinGeneration(sessionID, id) - 1
	ev, err := newGateLeftEvent(sessionID, agentID, id, contracts.GateJoinSource(cur.Source), gen)
	if err != nil {
		return PeersView{}, err
	}
	if err := h.bus.Publish(ctx, ev); err != nil {
		return PeersView{}, fmt.Errorf("host: 落 gate.left 事件: %w", err)
	}
	if _, err := h.discFeed.catchUp(ctx); err != nil {
		hubLog.Warn("host: leave 后发现表 catch-up 失败（下次 API 请求会重试）",
			"session_id", string(sessionID), "err", err.Error())
	}
	return h.viewOf(sessionID), nil
}

// joinMutex 返回该 session 的 gate 成员关系写锁（惰性建；per-session 粒度）。
//
// 为什么需要它：「判重（InGate）+ 发布」不是原子的 → 并发 join 会各落一条 gate.joined、
// 并发 leave 会各落一条 gate.left。把"判定 + 发布"收敛进同一把锁就原子了（同 session 的
// join/leave 串行，不同 session 互不阻塞）。G15 起 join 与 leave **共用**它：成员关系的
// 写面（加入/离开）必须互斥，否则"刚 leave 完又被并发 join 盖回"这类交错没有唯一结论。
func (h *Hub) joinMutex(sessionID contracts.Ulid) *sync.Mutex {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.joinLocks == nil {
		h.joinLocks = make(map[contracts.Ulid]*sync.Mutex)
	}
	m, ok := h.joinLocks[sessionID]
	if !ok {
		m = &sync.Mutex{}
		h.joinLocks[sessionID] = m
	}
	return m
}

// GateMembers 查一个 gate 的活跃成员（K12：成员关系是活的）。
func (h *Hub) GateMembers(ctx context.Context, gateID string) ([]discovery.SessionRow, error) {
	if h == nil {
		return nil, errors.New("host: Hub 为 nil")
	}
	if _, err := h.discFeed.catchUp(ctx); err != nil {
		return nil, fmt.Errorf("host: 扫事件流: %w", err)
	}
	// 剔掉 /btw 旁支 fork（v3-S2 K29）：旁支不是一等会话，不该出现在 gate 成员面。
	return h.filterForkGateMembers(h.disc.GateMembers(gateID)), nil
}

// GatesView 是 GET /api/gates 的形状（G15 枚举面）。
//
// # 可见性口径（如实定口径，写清）
//
// "枚举全部 gate"与 K11 的可见性哲学（成员身份只对 gate 相交者可见）是有冲突的，
// 这里按**分层**调和，不搞"要么全给要么不给"：
//
//   - **全部 gate 的存在性 + 活跃成员数**：任何调用方都能看到（gate 定义是持久化的事实，
//     "有几个 gate、各有多活跃"不泄露成员身份）；
//   - **成员详情**：只对**在该 gate 内**的调用方返回（Joined=true 的行）——gate 相交
//     才看得见彼此，与 Peers 的口径同源。
type GatesView struct {
	// SessionID 回显查询方（空 = 纯枚举：只有存在性 + 计数，没有成员详情）。
	SessionID string `json:"session_id"`
	// Gates 是全部 gate（字典序）：存在性 + 活跃成员数（+ 查询方在网内的成员详情）。
	Gates []GateView `json:"gates"`
}

// GateView 是一个 gate 的枚举行（G15）。
type GateView struct {
	GateID string `json:"gate_id"`
	// MemberCount 活跃成员数（G15 折叠口径：joined 减 left ∩ 本进程存活；
	// /btw 旁支 fork 不计入——旁支不是一等会话，K29）。
	MemberCount int `json:"member_count"`
	// Joined 查询方是否在该 gate 内（true = 下面的 Members 对查询方可见）。
	Joined bool `json:"joined"`
	// Members 活跃成员详情——**仅查询方在该 gate 内时返回**（K11：gate 相交才看得见
	// 彼此）。不在网内的 gate 只给存在性 + 计数，不给成员身份。
	//
	// 每一行的 `gates` 字段只列**与查询方相交**的 gate（与 /api/peers 的 PeerRow.gates
	// 同源）：成员的**全部** gate 成员关系不该泄露给查询方（它不在的那些 gate，查询方
	// 连存在性都不该从成员行里读到——那种 gate 只给存在性 + 计数）。
	Members []discovery.SessionRow `json:"members,omitempty"`
}

// Gates 枚举 gate（G15 管理面）：全部 gate 的存在性 + 活跃成员数；sessionID 非空且
// 在网内的 gate 额外带成员详情（口径见 GatesView）。
//
// sessionID 为空 = 纯枚举（存在性 + 计数）。sessionID 非空但**不受本 hub 托管** → 不报错：
// 枚举面是**读面**，不承担写面的托管校验（join/leave 才要求托管，见 ErrSessionNotHosted）。
//
// # 与 discover/presence 的口径**有意不同**（P2-2：别读成"漏过滤"）
//
//	gates 面          = **成员关系面**：谁在这个 gate 里、成员数、成员详情——按"托管
//	                    (live)"口径（成员关系是活的，G17b），**不按 G-a 的活跃窗口过滤**。
//	discover/presence = **活跃可见面**（K11 + G-a）：只列**活跃**对端（running ∨ 最后
//	                    事件 ts 在活跃窗口内），托管但超窗的历史 session 不进在线面。
//
// 有意不同：gates 面答"成员关系是什么"（管理/审计视角），discover/presence 答"现在谁
// 在线"（在线视角）。把两者并成一口径，会让"托管但超窗"的 session 从成员面消失——那是
// 另一刀（口径统一）的取舍；本刀**保持现状**，只把差异写明。
//
// ⚠ **别把它读成"Joined 恒 false"**：发现表折的是**事件流**（共享库口径——同一事件库里
// 可能还有别的进程写的 gate.joined/gate.left），不是"本进程托管的 session 名单"。所以一个
// 不受本 hub 托管的 session **仍可能在网内**（Joined=true）：它可能是同库另一个进程
// 托管的 session，也可能是本进程重启前的遗留成员关系。判定口径只有一个 = 发现表折叠出
// 的**活跃成员关系**（joined 减 left，core/discovery 的 InGate）。
func (h *Hub) Gates(ctx context.Context, sessionID contracts.Ulid) (GatesView, error) {
	if h == nil {
		return GatesView{}, errors.New("host: Hub 为 nil")
	}
	if sessionID != "" && !core.ValidUlid(sessionID) {
		return GatesView{}, fmt.Errorf("host: session_id 不是合法 ULID（26 字符 Crockford base32）: %q", sessionID)
	}
	if _, err := h.discFeed.catchUp(ctx); err != nil {
		return GatesView{}, fmt.Errorf("host: 扫事件流: %w", err)
	}
	view := GatesView{SessionID: string(sessionID), Gates: []GateView{}}
	for _, g := range h.disc.Gates() {
		members := h.filterForkGateMembers(h.disc.GateMembers(g.GateID))
		row := GateView{GateID: g.GateID, MemberCount: len(members)}
		if sessionID != "" && h.disc.InGate(sessionID, g.GateID) {
			row.Joined = true
			row.Members = members
			// 成员行的 Gates 只填**与查询方相交**的 gate（与 /api/peers 的 PeerRow.Gates
			// 同源：sharedGates）。
			//
			// 为什么不能填全量：SessionRow.Gates 默认是"该 session 的**全部**活跃 gate"。
			// 直接把它塞进成员行 = 把该成员的**其他** gate 成员关系泄露给查询方——哪怕
			// 查询方并不在那些 gate 里（K11：不在网内的 gate 只给存在性 + 计数，不给成员
			// 身份；否则"它还在 team:secret 里"这条信息就泄出去了）。
			for i := range row.Members {
				row.Members[i].Gates = h.disc.SharedGates(sessionID, contracts.Ulid(row.Members[i].SessionID))
			}
		}
		view.Gates = append(view.Gates, row)
	}
	return view, nil
}

// peersView 组装可见面（先扫描追平）。
func (h *Hub) peersView(ctx context.Context, sessionID contracts.Ulid) (PeersView, error) {
	if _, err := h.discFeed.catchUp(ctx); err != nil {
		return PeersView{}, fmt.Errorf("host: 扫事件流: %w", err)
	}
	return h.viewOf(sessionID), nil
}

// viewOf 直接从发现表组装可见面（调用方负责刚扫平过）。
func (h *Hub) viewOf(sessionID contracts.Ulid) PeersView {
	return PeersView{
		SessionID: string(sessionID),
		Gates:     h.disc.GatesOf(sessionID),
		// 对端里剔掉 /btw 旁支 fork（v3-S2 K29）：旁支不是一等会话（它没有 gate、
		// 没被激活，本就不会出现在对端里；这里显式过滤，让口径不靠副产物）。
		Peers: h.filterForkPeers(h.disc.VisiblePeers(sessionID)),
	}
}

// hostsSession 报告 session 是否由本 hub 托管（有它的 loop）。
func (h *Hub) hostsSession(sessionID contracts.Ulid) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.loops[sessionID]
	return ok
}

// mainAgentOf 取该 session 的主 agent id（loop 建成时登记）。
func (h *Hub) mainAgentOf(sessionID contracts.Ulid) (contracts.Ulid, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	id, ok := h.mainAgents[sessionID]
	return id, ok && id != ""
}

// joinAutoGates 在 session 的 loop 建成时加入自动 gate（K12：session 创建时自动加入）。
//
// agentID 是该 session 的主 agent（gate.joined 事件的 envelope.agent_id：契约必填，
// 且语义上"是主 agent 代表这个 session 加入 gate"）。
//
// # 两条护栏（F1：自动 gate 不得静默撤销用户的显式离开，也不得每次重启多落一条 join）
//
// 自动 gate 的加入是**幂等**的：一个 session 对同一个自动 gate 至多有一条"当前有效"的
// 成员关系。于是：
//
//	① **已是活跃成员** → 不重发 join。
//	   否则每次进程重启都会按新的 JoinGeneration 算出**新的确定性 id** → store 的
//	   ON CONFLICT(id) DO NOTHING 去重失效 → 每次重启多落一条 gate.joined（届数虚涨，
//	   事件流被无意义地灌水）。
//
//	② **曾显式离开过**（有 leave 事实且当前不活跃）→ **不自动复活**。
//	   显式 leave（POST /api/gates/leave）的语义强于"自动 gate 策略"：用户明确说过
//	   "这个 session 不在这个 gate 里"，重启不该把它悄悄加回去（那等于撤销用户的决定）。
//	   要回来得**显式** join（JoinGate 走显式路径，不受本护栏限制）。
//
// **取舍（如实写明）**：自动 gate 的"复活"能力因此被取消——显式离开过的自动 gate，
// 只能靠显式 join 回来。另一条边界：leave 只针对**那一个 gate id**；若会话的
// CWD / run_id 变了（自动 gate 的 id 随之变了），那是**另一个** gate，照常加入。
//
// 判据都在发现表里（core/discovery）：InGate = 当前活跃成员；LeftGate = 曾显式离开过。
func (h *Hub) joinAutoGates(ctx context.Context, sessionID, agentID contracts.Ulid, spec SessionSpec) error {
	gates := h.autoGatesFor(spec)
	if len(gates) == 0 {
		return nil
	}
	// 先扫平再判：发现表的水位可能还没追上刚发布的事件（重启后重建的表要重放全量）——
	// 不扫平就判，会把"其实早已 join / leave 过"当成"没有"，于是又发一条 join。
	if _, err := h.discFeed.catchUp(ctx); err != nil {
		return fmt.Errorf("host: 扫事件流: %w", err)
	}
	evs := make([]contracts.Event, 0, len(gates))
	for _, g := range gates {
		// ① 已是活跃成员 → 不重发（幂等：重启不该多落一条 gate.joined）。
		if h.disc.InGate(sessionID, g.gateID) {
			continue
		}
		// ② 曾显式离开过 → 不自动复活（显式 leave 的语义强于自动 gate 策略）。
		if h.disc.LeftGate(sessionID, g.gateID) {
			continue
		}
		// 届数从发现表折出来（全新 session = 0；重启后重放历史 = 已发生的 join 次数）——
		// 与显式 join 同一把尺（JoinGate），存储层去重不变量一致。
		gen := h.disc.JoinGeneration(sessionID, g.gateID)
		ev, err := newGateJoinedEvent(sessionID, agentID, g, gen)
		if err != nil {
			return err
		}
		evs = append(evs, ev)
	}
	if len(evs) == 0 {
		return nil // 全都已是成员 / 曾显式离开 → 不落任何事件
	}
	return h.bus.Publish(ctx, evs...)
}

// autoGatesFor 算某 session 的自动 gate（K12：repo: 恒算、workflow: 有 run_id 才算）。
func (h *Hub) autoGatesFor(spec SessionSpec) []autoGate {
	cwd := strings.TrimSpace(spec.CWD)
	if cwd == "" {
		cwd = h.cwd
	}
	var out []autoGate
	if cwd != "" {
		repoID, err := identity.RepoID(cwd)
		if err != nil {
			// fail-close：算不出 repo_id 就**不加** repo gate（宁可这两个 session 互相
			// 不可见，也不按一个猜出来的 id 乱聚）。不放宽到 project_id——K12 的口径是
			// repo 级，降级会悄悄改变可见性集合。
			hubLog.Warn("host: 算不出 repo_id，该 session 不加 repo gate（fail-close）",
				"cwd", cwd, "err", err.Error())
		} else {
			out = append(out, autoGate{gateID: discovery.RepoGate(repoID), source: contracts.GateJoinSourceAutoRepo})
		}
	}
	runID := strings.TrimSpace(spec.RunID)
	if runID == "" {
		runID = strings.TrimSpace(h.cfg.RunID)
	}
	if runID != "" {
		gateID := discovery.WorkflowGate(runID)
		if !discovery.ValidGateID(gateID) {
			hubLog.Warn("host: run_id 拼不成合法 workflow gate，跳过", "run_id", runID)
		} else {
			out = append(out, autoGate{gateID: gateID, source: contracts.GateJoinSourceAutoWorkflow})
		}
	}
	return out
}

// newGateJoinedEvent 组一条 gate.joined durable 事件（契约增量：F7 只增）。
//
// **Event.ID 是确定性导出**的（由 session_id + gate_id + source + **届数**哈希而来，
// 26 字符 Crockford base32 = ULID 形态 ✅，core.ValidUlid 校验通过）：
// 同一个 (session, gate, source, gen) 的 join 事件**恒是同一个 id** → store 的
// ON CONFLICT(id) DO NOTHING 在**存储层**兜住并发重复（与 X5 spawn_request_id 同款
// 手法 ✅：幂等不靠调用方自觉）。
//
// gen 是"第几届"（discovery.JoinGeneration：已折叠的 join 次数）。为什么把它编进 id：
// 只按 (session, gate, source) 哈希的话，**leave 之后的重 join** 会撞上第一次的 id 被
// 存储层吃掉 → 成员关系永远恢复不了（G15 回归：join→leave→rejoin 必须恢复可见）。
// 带上届数后：同届的并发 join 仍撞同一 id（去重不变量不变），重 join 是新的一届 → 新 id。
//
// 为什么需要确定性 id：host 侧虽用 per-session 锁把「判重 + 发布」收成一段临界区，
// 但那是**单进程**的保护；确定性 id 让"同一届 join 只落一条事件"成为存储层的不变量，
// 跨进程/绕过锁/发现表丢失时同样成立（届数由事件折叠推出，重启后重放仍一致）。
func newGateJoinedEvent(sessionID, agentID contracts.Ulid, g autoGate, gen int) (contracts.Event, error) {
	raw, err := json.Marshal(contracts.GateJoinedPayload{GateID: g.gateID, Source: g.source})
	if err != nil {
		return contracts.Event{}, fmt.Errorf("host: 序列化 gate.joined payload: %w", err)
	}
	return contracts.Event{
		V:         1,
		ID:        contracts.Ulid(core.ShortHash("gate-joined:" + string(sessionID) + ":" + g.gateID + ":" + string(g.source) + ":" + strconv.Itoa(gen))),
		Kind:      discovery.KindGateJoined,
		TS:        time.Now().UTC().Format(time.RFC3339),
		SessionID: sessionID,
		AgentID:   agentID,
		Durable:   true,
		Payload:   raw,
	}, nil
}

// newGateLeftEvent 组一条 gate.left durable 事件（G15；契约增量：F7 只增）。
//
// 与 gate.joined **对称**：确定性 id（"gate-left:<session>:<gate>:<gen>"）让重复 leave
// 在存储层再去重一次（幂等双保险——发布端已按 InGate 判过重，这里是跨进程/绕过锁时的
// 兜底）。gen 是**被终止的那一届**（= 该成员关系的 join 届数；调用方传
// JoinGeneration-1）——同一届成员关系的重复 leave 撞同一 id，下一届的 leave 是新 id。
//
// source 记被终止的成员关系的来源（与对应 gate.joined 的 source 同值，审计对得上）。
func newGateLeftEvent(sessionID, agentID contracts.Ulid, gateID string, source contracts.GateJoinSource, gen int) (contracts.Event, error) {
	raw, err := json.Marshal(contracts.GateLeftPayload{GateID: gateID, Source: source})
	if err != nil {
		return contracts.Event{}, fmt.Errorf("host: 序列化 gate.left payload: %w", err)
	}
	return contracts.Event{
		V:         1,
		ID:        contracts.Ulid(core.ShortHash("gate-left:" + string(sessionID) + ":" + gateID + ":" + strconv.Itoa(gen))),
		Kind:      discovery.KindGateLeft,
		TS:        time.Now().UTC().Format(time.RFC3339),
		SessionID: sessionID,
		AgentID:   agentID,
		Durable:   true,
		Payload:   raw,
	}, nil
}
