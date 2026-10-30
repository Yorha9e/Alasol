package agents

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Yorha9e/alasol/contracts"
)

// AgentType 是子代理的能力模板（K16 类型可达性）。
//
// 直接复用跨边界契约类型 contracts.AgentType（ID / Prompt / Tools / Model / CanSpawn /
// PermissionRequest / ContextStrategy / ShareProjectAssets），使 workflow 加载后的
// AgentType 原样可用，无转换边界（D21：一个类型只定义一次，其余引用不重抄）。
//
//	CanSpawn 声明"我能派哪些类型"→ 第 N 层在类型层面就不可达（K16）；
//	数字深度只是兜底（K17）。只读型 type 的 CanSpawn 为空 → 更深层在类型层面不可达（K20）。
//
// W6-A（2026-10-31 拍板）两个新字段（**显式声明**，都不继承主 agent）：
//   - ContextStrategy：*contracts.ContextStrategy，声明该类型**子 agent**的上下文管理
//     方式；nil = 未声明 = append 默认；声明非 append → 由宿主的 loop.Agents.ChildStrategy
//     钩子按 kind 造策略工厂（未接钩子 → 该 spawn 的子 agent fail-close 收口）；
//     **装载期校验**（Register）：声明了就得有 kind 且在四枚举内（append / stage_scoped /
//     sliding_window / tiered）——空 kind / 越枚举 → 拒注册（与能力包装载期同口径）。
//   - ShareProjectAssets：*bool，项目级资产（rules / 常识区）共享开关；true = 该类型子
//     agent 的上下文装配**可**注入项目资产；缺省 false = 不共享。
type AgentType = contracts.AgentType

// 类型注册表错误（调用方可用 errors.Is 判定）。
var (
	// ErrTypeExists：同名类型已注册（ID 在注册表内唯一）。
	ErrTypeExists = errors.New("agents: 类型已注册")
	// ErrTypeNotFound：类型未注册（Resolve 查无此类型）。
	ErrTypeNotFound = errors.New("agents: 类型未注册")
	// ErrSpawnCycle：can_spawn 构成有环（K16/K17：注册时校验，陷阱16 第一层）。
	ErrSpawnCycle = errors.New("agents: can_spawn 环")
)

// TypeRegistry 是并发安全的 agent 类型注册表（K16）。
//
// can_spawn 环检测在**注册时**做（K17：类型闸门让 can_spawn 有向无环）：每次 Register
// 都 tentatively 加入新类型后跑一次全图环检测，发现环即拒绝且不提交。workflow 加载时的
// 不可信配置校验（陷阱16 第二层）是另一层，由装配层负责。
type TypeRegistry struct {
	mu    sync.RWMutex
	types map[string]AgentType
	order []string // 注册顺序（环检测的确定性遍历来源）
}

// NewTypeRegistry 构造空注册表。
func NewTypeRegistry() *TypeRegistry {
	return &TypeRegistry{types: make(map[string]AgentType)}
}

// Register 注册一个 agent 类型。
//
// 校验（fail-close，S9：不合则拒绝并说明，不修复）：
//   - ID 不能为空；
//   - 重名拒绝（ID 唯一）；
//   - **context_strategy 声明合法**（W6-A）：声明了（指针非 nil）就必须有 kind，且 kind 在
//     枚举内（append / stage_scoped / sliding_window / tiered）——与能力包装载期
//     （core/workflow.validateAgentTypes）**同口径**：注册表也是"装载期"的一种，
//     越枚举的 kind 若从这条路径注册进来，就绕开了能力包那道闸门（直接可达装配钩子）。
//   - **can_spawn 环检测**：加入后若 can_spawn 图出现环（含自环 A→A）→ 拒绝，不提交。
func (r *TypeRegistry) Register(t AgentType) error {
	if r == nil {
		return errors.New("agents: TypeRegistry 为 nil")
	}
	id := strings.TrimSpace(t.ID)
	if id == "" {
		return errors.New("agents: AgentType.ID 不能为空")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.types == nil {
		r.types = make(map[string]AgentType)
	}
	if _, dup := r.types[id]; dup {
		return fmt.Errorf("%w: 类型 %q", ErrTypeExists, id)
	}
	// W6-A：context_strategy 声明了就得说清楚 kind（否则运行期静默走 append）。
	if err := validateContextStrategyDecl(id, t.ContextStrategy); err != nil {
		return err
	}
	// tentatively 加入 → 跑全图环检测 → 有环则回滚（不提交）。
	r.types[id] = t
	r.order = append(r.order, id)
	if cycle := r.detectCycleLocked(); cycle != "" {
		delete(r.types, id)
		r.order = r.order[:len(r.order)-1]
		return fmt.Errorf("%w: %s", ErrSpawnCycle, cycle)
	}
	return nil
}

// ValidContextStrategyKind 报告 kind 是否在**上下文策略枚举**内
// （append / stage_scoped / sliding_window / tiered）。
//
// **唯一枚举表**：装载期三道闸（core/workflow 的工作流级 + agent_type 级、core/agents 的
// 类型注册表）与运行期兜底（core/loop 的 childStrategyFor）**共用它**——口径不许分叉：
// 分叉就会出现"装载期拒一种、运行期放行另一种"的口径漂移（坏声明从旁路溜进运行期）。
//
// 宿主配置面（J8：用户可写扩展）**不走这里**——那是 TOML/配置面的扩展性，与工作流 JSON /
// agent_type 声明的枚举约束是两件事。
func ValidContextStrategyKind(kind contracts.ContextStrategyKind) bool {
	switch kind {
	case contracts.ContextStrategyKindAppend,
		contracts.ContextStrategyKindStageScoped,
		contracts.ContextStrategyKindSlidingWindow,
		contracts.ContextStrategyKindTiered:
		return true
	default:
		return false
	}
}

// validateContextStrategyDecl 校验类型的 context_strategy 声明（W6-A，fail-close）。
//
// 口径与 core/workflow.validateAgentTypes 的第 6 条**完全一致**（注册表也是"装载期"的一种，
// 不许成为绕过能力包那道枚举闸门的旁路）：
//   - 未声明（指针为 nil）= append 默认，**不继承主 agent 的策略** → 合法；
//   - 声明了但 **kind 为空** → 非法（"声明了却没说什么"比不声明更危险：运行期会被静默
//     当成 append，用户以为配了策略其实没配）→ 拒；
//   - kind **不在枚举内**（append / stage_scoped / sliding_window / tiered 之外）→ 非法
//     （宿主无从装配）→ 拒。
//
// 错误文案点名类型 id 与 kind，便于定位是哪条声明坏了。
func validateContextStrategyDecl(id string, cs *contracts.ContextStrategy) error {
	if cs == nil {
		return nil // 未声明 = append 默认（W6-A：不继承主 agent 的策略）
	}
	kind := cs.Kind
	if strings.TrimSpace(string(kind)) == "" {
		return fmt.Errorf("agents: 类型 %q 的 context_strategy kind %q **为空**（声明了却没说什么 = 运行期会被静默当成 append）：fail-close", id, kind)
	}
	if !ValidContextStrategyKind(kind) {
		return fmt.Errorf("agents: 类型 %q 的 context_strategy kind %q **不在枚举内**（append/stage_scoped/sliding_window/tiered）：fail-close", id, kind)
	}
	return nil
}

// Get 按 id 取类型（存在与否）。
func (r *TypeRegistry) Get(id string) (AgentType, bool) {
	if r == nil {
		return AgentType{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.types[id]
	return t, ok
}

// Resolve 按 id 解析类型；不存在返回 ErrTypeNotFound（调用方按 fail-close 处理）。
func (r *TypeRegistry) Resolve(typeID string) (AgentType, error) {
	t, ok := r.Get(typeID)
	if !ok {
		return AgentType{}, fmt.Errorf("%w: 类型 %q", ErrTypeNotFound, typeID)
	}
	return t, nil
}

// detectCycleLocked 在已持写锁时检测 can_spawn 图是否有环，返回环路径描述（无环返回 ""）。
//
// 图的节点 = 已注册类型；边 = 每个类型的 CanSpawn（指向的 type id，无论其是否已注册）。
// 指向未注册类型的边是死端（该节点无出边），只有当它后续被注册且指回时才闭合环。
// DFS 三色标记：grey = 在当前栈上（回边 = 环）；遍历顺序按注册序（确定性错误信息）。
func (r *TypeRegistry) detectCycleLocked() string {
	const (
		white = 0 // 未访问
		grey  = 1 // 在栈上
		black = 2 // 已完成
	)
	color := make(map[string]int, len(r.types))
	var stack []string
	var cyclePath []string

	var visit func(id string) bool
	visit = func(id string) bool {
		color[id] = grey
		stack = append(stack, id)
		if t, ok := r.types[id]; ok {
			for _, child := range t.CanSpawn {
				switch color[child] {
				case grey:
					// 回边 → 环：取栈中 child 首次出现处到栈顶，再闭合成 child。
					if idx := indexOf(stack, child); idx >= 0 {
						cyclePath = append(append([]string(nil), stack[idx:]...), child)
					}
					return true
				case white:
					if visit(child) {
						return true
					}
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = black
		return false
	}

	for _, id := range r.order {
		if color[id] == white {
			if visit(id) {
				return strings.Join(cyclePath, " -> ")
			}
		}
	}
	return ""
}

// indexOf 返回 s 在 list 中首次出现的下标（-1 = 不存在）。
func indexOf(list []string, s string) int {
	for i, x := range list {
		if x == s {
			return i
		}
	}
	return -1
}

// contains 报告 list 是否含 s。
func contains(list []string, s string) bool {
	return indexOf(list, s) >= 0
}
