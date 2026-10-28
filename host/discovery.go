package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	// 锁的粒度是 session：同一 session 的 join 串行，不同 session 互不阻塞。
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

	ev, err := newGateJoinedEvent(sessionID, agentID, autoGate{gateID: id, source: contracts.GateJoinSourceExplicit})
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

// joinMutex 返回该 session 的 join 互斥锁（惰性建；per-session 粒度）。
//
// 为什么需要它：「判重 + 发布」不是原子的 → 并发 join 会各落一条 gate.joined。
// 把两件事收敛进同一把锁就原子了（同 session 串行，不同 session 互不阻塞）。
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
	return h.disc.GateMembers(gateID), nil
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
		Peers:     h.disc.VisiblePeers(sessionID),
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
func (h *Hub) joinAutoGates(ctx context.Context, sessionID, agentID contracts.Ulid, spec SessionSpec) error {
	gates := h.autoGatesFor(spec)
	if len(gates) == 0 {
		return nil
	}
	evs := make([]contracts.Event, 0, len(gates))
	for _, g := range gates {
		ev, err := newGateJoinedEvent(sessionID, agentID, g)
		if err != nil {
			return err
		}
		evs = append(evs, ev)
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
// **Event.ID 是确定性导出**的（由 session_id + gate_id + source 哈希而来，26 字符
// Crockford base32 = ULID 形态 ✅，core.ValidUlid 校验通过）：同一个
// (session, gate, source) 的 join 事件**恒是同一个 id** → store 的
// ON CONFLICT(id) DO NOTHING 在**存储层**兜住并发重复（与 X5 spawn_request_id 同款
// 手法 ✅：幂等不靠调用方自觉）。
//
// 为什么需要它：host 侧虽用 per-session 锁把「判重 + 发布」收成一段临界区，但那是
// **单进程**的保护；确定性 id 让"同一 join 只落一条事件"成为存储层的不变量，
// 跨进程/绕过锁时同样成立。
func newGateJoinedEvent(sessionID, agentID contracts.Ulid, g autoGate) (contracts.Event, error) {
	raw, err := json.Marshal(contracts.GateJoinedPayload{GateID: g.gateID, Source: g.source})
	if err != nil {
		return contracts.Event{}, fmt.Errorf("host: 序列化 gate.joined payload: %w", err)
	}
	return contracts.Event{
		V:         1,
		ID:        contracts.Ulid(core.ShortHash("gate-joined:" + string(sessionID) + ":" + g.gateID + ":" + string(g.source))),
		Kind:      discovery.KindGateJoined,
		TS:        time.Now().UTC().Format(time.RFC3339),
		SessionID: sessionID,
		AgentID:   agentID,
		Durable:   true,
		Payload:   raw,
	}, nil
}
