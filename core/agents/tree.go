package agents

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// DefaultMaxAgentDepth 是 agent 树默认最大深度（K17 / 06 §7.1：默认 3 = 两级委派）。
// root = 1（主 agent），depth 2 / 3 是两级委派；depth 4 在数字层面就不可达（兜底）。
const DefaultMaxAgentDepth = 3

// Status 是 agent 节点的生命周期状态。终态（completed / failed）不可逆。
type Status string

const (
	// StatusRunning 运行中（初始态；可 Spawn 子 / 可转终态）。
	StatusRunning Status = "running"
	// StatusCompleted 正常收敛（经 complete(summary)）。终态。
	StatusCompleted Status = "completed"
	// StatusFailed 失败 / 卡住 / 截断（异常冒泡）。终态。
	StatusFailed Status = "failed"
)

// terminal 报告状态是否终态（不可再变迁）。
func (s Status) terminal() bool { return s == StatusCompleted || s == StatusFailed }

// Node 是 agent 树节点的只读快照（K26：agent_id = ULID、depth = int、parent = ULID 或空）。
type Node struct {
	ID       contracts.Ulid
	ParentID *contracts.Ulid // root 为 nil
	Depth    int
	Type     string // agent_type id（K16）
	Role     string // 部门标签（K13）
	Status   Status
}

// 树错误（调用方可用 errors.Is 判定）。
var (
	// ErrTreeNil：Tree 为 nil。
	ErrTreeNil = errors.New("agents: Tree 为 nil")
	// ErrParentNotFound：父 agent 不在树中。
	ErrParentNotFound = errors.New("agents: 父 agent 不存在")
	// ErrNodeNotFound：agent 不在树中。
	ErrNodeNotFound = errors.New("agents: agent 不存在")
	// ErrParentTerminal：父 agent 已终态，不能再派生。
	ErrParentTerminal = errors.New("agents: 父 agent 已终态，不能再派生")
	// ErrDepthExceeded：闸门①——深度超限（child.depth > maxDepth）。
	ErrDepthExceeded = errors.New("agents: 深度超限")
	// ErrTypeUnreachable：闸门②——子类型不在父的 can_spawn 内。
	ErrTypeUnreachable = errors.New("agents: 子类型不在父的 can_spawn 内")
	// ErrPermissionTooBroad：闸门③——子权限超出 父∩天花板。
	ErrPermissionTooBroad = errors.New("agents: 子权限超出 父∩天花板")
	// ErrAlreadyTerminal：目标 agent 已终态（Complete/Fail 不可逆）。
	ErrAlreadyTerminal = errors.New("agents: agent 已终态（不可逆）")
	// ErrTypesMissing：Tree 未配置 TypeRegistry，无法解析/校验子类型。
	ErrTypesMissing = errors.New("agents: TypeRegistry 未配置，无法解析子类型")
	// ErrRequestConflict：同一 (父 agent, spawn_request_id) 已用于**另一个类型**的 child
	// → 冲突（不把别人的 child 交出去；X5 幂等键的作用域是父）。
	ErrRequestConflict = errors.New("agents: spawn_request_id 冲突（已用于别的类型/父作用域）")
)

// requestKey 是 X5 的幂等键：**(parent_agent_id, spawn_request_id)**。
//
// 父作用域不是可选项：契约 AgentSpawnedPayload.spawn_request_id 的描述写明"按
// (parent_agent_id, spawn_request_id) 找回同一 child"——全局索引会让"第二个任务
// 复用同一个 request_id"静默不执行，还拿到第一个 child 的 summary。
type requestKey struct {
	parentID  contracts.Ulid
	requestID contracts.Ulid
}

// node 是树内内部存储（含完整 AgentType 与有效权限，供闸门判定）。
type node struct {
	id       contracts.Ulid
	parentID *contracts.Ulid
	depth    int
	typ      AgentType
	role     string
	status   Status
	perm     contracts.PermissionRequest // 有效权限（请求 ∩ ceiling；已保证 ⊆ ceiling）
	children []contracts.Ulid            // 直系子（spawn 顺序）
	summary  *Summary                    // completed 时的汇报（终态后不变）
	failMsg  string                      // failed 时的原因文案
}

// view 生成对外只读快照（ParentID 用新指针，避免与内部状态共享）。
func (n *node) view() Node {
	v := Node{
		ID:     n.id,
		Depth:  n.depth,
		Type:   n.typ.ID,
		Role:   n.role,
		Status: n.status,
	}
	if n.parentID != nil {
		pid := *n.parentID
		v.ParentID = &pid
	}
	return v
}

// Tree 是一个 session 内的 agent 树（内存活状态，K26）。并发安全。
//
// 不变式：每个节点的 perm ⊆ ceiling（root 在 NewTree 时 intersect，子节点经闸门③保证）。
// 因此 spawn 时的权限上界 = 父节点的 perm（无需再与 ceiling 求交）。
type Tree struct {
	mu       sync.RWMutex
	maxDepth int
	ceiling  *contracts.PermissionRequest
	types    *TypeRegistry
	nodes    map[contracts.Ulid]*node
	root     contracts.Ulid
	// rootIDOverride 是 WithRootID 指定的 root AgentID（空 = 未指定 → NewTree 自造）。
	// 与 root 分开存：opts 在 root 落地**之前**应用，需要一个字段承接"调用方指定的 id"。
	rootIDOverride contracts.Ulid
	requests       map[requestKey]contracts.Ulid // (父, spawn_request_id) → child id（X5 幂等键）
}

// TreeOption 配置 Tree（默认值见 NewTree）。
type TreeOption func(*Tree)

// WithMaxDepth 设置最大深度（< 1 忽略，保留默认）。
func WithMaxDepth(d int) TreeOption {
	return func(t *Tree) {
		if d >= 1 {
			t.maxDepth = d
		}
	}
}

// WithCeiling 设置权限天花板（policy ceiling；nil = 无额外天花板，只用父子收窄）。
func WithCeiling(p *contracts.PermissionRequest) TreeOption {
	return func(t *Tree) { t.ceiling = p }
}

// WithTypes 设置类型注册表（Spawn 解析/校验子类型所需；缺失时 Spawn 报 ErrTypesMissing）。
func WithTypes(reg *TypeRegistry) TreeOption {
	return func(t *Tree) { t.types = reg }
}

// WithRootID 指定 root 节点的 AgentID（v3-S6-B host 装配：hub 的主 agent id 来自 discovery，
// 续跑复用同一个 id，树的 root 必须**等于**它——否则树的 root 与 loop 的 AgentID 对不上，
// spawn 的 parentID=l.agentID 会落不到 root 上）。
//
// 默认行为不变：**没传** WithRootID（或传入的 id 不是合法 ULID）时，NewTree 照旧自造一个
// 新 ULID 作 root（向后兼容：core/loop 的测试、别的宿主仍走"NewTree 造 root"的老路）。
func WithRootID(id contracts.Ulid) TreeOption {
	return func(t *Tree) {
		if core.ValidUlid(id) {
			t.rootIDOverride = id
		}
	}
}

// NewTree 构造 agent 树并创建 root 节点（depth=1）。
//
// root 的 AgentID：传了 WithRootID(合法 ULID) → 用它（host 装配：等于主 agent id，续跑复用）；
// 否则自造一个新 ULID（默认行为不变）。root 的有效权限 = rootType.PermissionRequest ∩ ceiling。
// root 的 AgentID 经 Root() 取得，调用方（loop）用它作为主 agent 的 AgentID。
func NewTree(rootType AgentType, opts ...TreeOption) *Tree {
	t := &Tree{
		maxDepth: DefaultMaxAgentDepth,
		nodes:    make(map[contracts.Ulid]*node),
		requests: make(map[requestKey]contracts.Ulid),
	}
	for _, o := range opts {
		o(t)
	}
	rootID := t.rootIDOverride
	if !core.ValidUlid(rootID) {
		rootID = core.NewUlid() // 未指定 / 非法 → 自造（默认行为不变）
	}
	t.root = rootID
	t.nodes[rootID] = &node{
		id:     rootID,
		depth:  1,
		typ:    rootType,
		status: StatusRunning,
		perm:   effectivePerm(rootType.PermissionRequest, t.ceiling),
	}
	return t
}

// Spawn 从 parent 派生一个子 agent，过三闸门（K17，全部运行时强制）。
//
// 返回新子节点；任一道闸门拒绝则返回对应错误且**不建节点**。X5 幂等：同一
// **(父, requestID)** 重复 Spawn 返回**同一个**已建的 child（不新建、不重跑）——
// 续跑按 (parent_agent_id, spawn_request_id) 找回同一 child。
//
// 幂等命中时还会校验**类型**：同一个 (父, request_id) 换类型 → ErrRequestConflict
// （不把别人的 child 交出去）。
//
// 闸门：
//
//	① 深度 child.depth ≤ maxDepth；
//	② 类型 child.type ∈ parent.can_spawn；
//	③ 权限 child.permission ⊆ parent.perm（= parent ∩ ceiling，不变式保证 ⊆ ceiling）。
func (t *Tree) Spawn(parentID contracts.Ulid, typeID, role string, requestID contracts.Ulid) (Node, error) {
	if t == nil {
		return Node{}, ErrTreeNil
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	// X5：同 (父, request_id) → 返回已建的同一个 child（幂等，不新建）。
	key := requestKey{parentID: parentID, requestID: requestID}
	if childID, ok := t.requests[key]; ok {
		n, ok := t.nodes[childID]
		if !ok {
			return Node{}, fmt.Errorf("%w: request %s 映射到不存在的 child", ErrNodeNotFound, requestID)
		}
		// 幂等命中校验：既有 child 必须是**同一父**下的**同一类型**的 child。
		// 键已经保证了父一致；类型不一致 → 冲突（换任务就得换 request_id）。
		if strings.TrimSpace(n.typ.ID) != strings.TrimSpace(typeID) {
			return Node{}, fmt.Errorf("%w: (parent=%s, request_id=%s) 已用于类型 %q 的 child %s，本次请求类型 %q（换任务必须换 request_id）",
				ErrRequestConflict, parentID, requestID, n.typ.ID, childID, typeID)
		}
		return n.view(), nil
	}

	parent, ok := t.nodes[parentID]
	if !ok {
		return Node{}, fmt.Errorf("%w: %s", ErrParentNotFound, parentID)
	}
	if parent.status.terminal() {
		return Node{}, fmt.Errorf("%w: %s（status=%s）", ErrParentTerminal, parentID, parent.status)
	}

	// 子类型必须已注册（fail-close：类型层面不可达 = 拒绝）。
	if t.types == nil {
		return Node{}, ErrTypesMissing
	}
	childType, err := t.types.Resolve(typeID)
	if err != nil {
		return Node{}, err
	}

	// 闸门①：深度。
	childDepth := parent.depth + 1
	if childDepth > t.maxDepth {
		return Node{}, fmt.Errorf("%w: child.depth=%d > maxDepth=%d（第 %d 层在数字层面不可达）",
			ErrDepthExceeded, childDepth, t.maxDepth, childDepth)
	}

	// 闸门②：类型可达性。
	if !contains(parent.typ.CanSpawn, typeID) {
		return Node{}, fmt.Errorf("%w: 父类型 %q 的 can_spawn=%v 不含 %q",
			ErrTypeUnreachable, parent.typ.ID, parent.typ.CanSpawn, typeID)
	}

	// 闸门③：权限（H11 只收窄；bound = parent.perm，已保证 ⊆ ceiling）。
	childReq := childType.PermissionRequest
	if !permSubset(childReq, &parent.perm) {
		return Node{}, fmt.Errorf("%w: 子类型 %q 请求 %s 超出父 %s 的有效权限 %s",
			ErrPermissionTooBroad, typeID, permStr(childReq), parentID, permStr(&parent.perm))
	}

	childID := core.NewUlid()
	pid := parentID
	child := &node{
		id:       childID,
		parentID: &pid,
		depth:    childDepth,
		typ:      childType,
		role:     role,
		status:   StatusRunning,
		perm:     permOf(childReq), // subset 通过 → 有效权限 = 请求本身
	}
	t.nodes[childID] = child
	parent.children = append(parent.children, childID)
	t.requests[key] = childID
	return child.view(), nil
}

// Complete 把 agent 标记为正常收敛（终态，不可逆）。存储 summary 供事后取用。
func (t *Tree) Complete(id contracts.Ulid, sum Summary) error {
	if t == nil {
		return ErrTreeNil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	n, ok := t.nodes[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	if n.status.terminal() {
		return fmt.Errorf("%w: %s 已 %s，不能改为 completed", ErrAlreadyTerminal, id, n.status)
	}
	s := sum
	n.status = StatusCompleted
	n.summary = &s
	return nil
}

// Fail 把 agent 标记为失败（终态，不可逆）。cause 为 nil 时原因文案为空。
func (t *Tree) Fail(id contracts.Ulid, cause error) error {
	if t == nil {
		return ErrTreeNil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	n, ok := t.nodes[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	if n.status.terminal() {
		return fmt.Errorf("%w: %s 已 %s，不能改为 failed", ErrAlreadyTerminal, id, n.status)
	}
	n.status = StatusFailed
	if cause != nil {
		n.failMsg = cause.Error()
	}
	return nil
}

// SummaryOf 返回 completed 节点的 summary（X5：重复 spawn 找回同一 child 时取回其产出）。
func (t *Tree) SummaryOf(id contracts.Ulid) (Summary, bool) {
	if t == nil {
		return Summary{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.nodes[id]
	if !ok || n.status != StatusCompleted || n.summary == nil {
		return Summary{}, false
	}
	return *n.summary, true
}

// FailReason 返回 failed 节点的失败原因文案。
func (t *Tree) FailReason(id contracts.Ulid) (string, bool) {
	if t == nil {
		return "", false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.nodes[id]
	if !ok || n.status != StatusFailed {
		return "", false
	}
	return n.failMsg, true
}

// Root 返回 root 的 AgentID。
func (t *Tree) Root() contracts.Ulid {
	if t == nil {
		return ""
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.root
}

// RootNode 返回 root 节点快照。
func (t *Tree) RootNode() Node {
	if t == nil {
		return Node{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if n, ok := t.nodes[t.root]; ok {
		return n.view()
	}
	return Node{}
}

// Get 取节点快照。
func (t *Tree) Get(id contracts.Ulid) (Node, bool) {
	if t == nil {
		return Node{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.nodes[id]
	if !ok {
		return Node{}, false
	}
	return n.view(), true
}

// TypeOf 取节点存储的完整 AgentType（含 Tools / CanSpawn / PermissionRequest）。
// root 的类型可能未注册进 TypeRegistry，故此方法直接取节点上存储的类型，不做注册表查找。
func (t *Tree) TypeOf(id contracts.Ulid) (AgentType, bool) {
	if t == nil {
		return AgentType{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.nodes[id]
	if !ok {
		return AgentType{}, false
	}
	return n.typ, true
}

// PermOf 取节点的**生效权限**（= 请求 ∩ ceiling；子节点还 ⊆ 父的 perm）。
//
// 运行期用它约束该 agent 的工具调用（H11/H22：实际 = 请求 ∩ 策略上限 ∩ 父级授予）——
// 声明的权限是**上限**，不可被全局 allow 规则放宽。
func (t *Tree) PermOf(id contracts.Ulid) (contracts.PermissionRequest, bool) {
	if t == nil {
		return contracts.PermissionRequest{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.nodes[id]
	if !ok {
		return contracts.PermissionRequest{}, false
	}
	return n.perm, true
}

// Children 返回直系子（spawn 顺序）。
func (t *Tree) Children(id contracts.Ulid) []Node {
	if t == nil {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.nodes[id]
	if !ok {
		return nil
	}
	out := make([]Node, 0, len(n.children))
	for _, cid := range n.children {
		if c, ok := t.nodes[cid]; ok {
			out = append(out, c.view())
		}
	}
	return out
}

// Parent 返回直系父（root 或无父 → false）。
func (t *Tree) Parent(id contracts.Ulid) (Node, bool) {
	if t == nil {
		return Node{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.nodes[id]
	if !ok || n.parentID == nil {
		return Node{}, false
	}
	p, ok := t.nodes[*n.parentID]
	if !ok {
		return Node{}, false
	}
	return p.view(), true
}

// Ancestors 沿父链从近到远返回全部祖先（K11：跨层上查 = Inquiry 沿父链逐级，不是自动可见）。
func (t *Tree) Ancestors(id contracts.Ulid) []Node {
	if t == nil {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	cur, ok := t.nodes[id]
	if !ok {
		return nil
	}
	var out []Node
	for cur.parentID != nil {
		p, ok := t.nodes[*cur.parentID]
		if !ok {
			break
		}
		out = append(out, p.view())
		cur = p
	}
	return out
}

// Visible 返回 from 可见的节点（K11/K27）：**自己 + 父 + 全部直系子**。
// 兄弟互不可见；别 session 的 subagent 完全不可见（树按 session 作用域隔离）。
func (t *Tree) Visible(from contracts.Ulid) []Node {
	if t == nil {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.nodes[from]
	if !ok {
		return nil
	}
	out := make([]Node, 0, 2+len(n.children))
	out = append(out, n.view()) // 自己
	if n.parentID != nil {
		if p, ok := t.nodes[*n.parentID]; ok {
			out = append(out, p.view()) // 父
		}
	}
	for _, cid := range n.children { // 直系子
		if c, ok := t.nodes[cid]; ok {
			out = append(out, c.view())
		}
	}
	return out
}
