package host

// 本文件把 v3-S6-B「host 装配 agent_type 注册表」落地（W5 拍板：内建五类 + TOML 自定义扩展）。
//
// # 定位（W5 / K16 / K20）
//
// 内建五类（planner / coder / critic / reviewer / observer）是 subagent 从"core 机制"变成
// "产品面可达"的那一步：它们进 host 的**类型注册表**（core/agents.TypeRegistry），loopFor
// 建 loop 时经 agents.WithTypes 接进 session 的 agent 树，主 agent 因此能按类型派生子 agent。
//
// 用户可经 TOML `[[agent_types]]` 自定义扩展（W5：用户可自定义）——**同名覆盖内建**（用户
// 自定义优先）。装载期 fail-close：id 空/重名/保留名、can_spawn 悬空/自环/成环、permission
// 级别非法、context_strategy.kind 越枚举 → host.New 启动即拒并**点名**（不修复，S9）。
//
// **校验时机**：只在 **MultiAgent=true** 时做（host/hub.go 的 New 装配处）——MultiAgent=false
// = 单 agent 模式零行为变化：不建注册表、**也不校验**（用一个用不到的扩展点不许拒绝宿主启动）。
//
// # 与 tower 能力包的 agent_types 的关系（诚实口径）
//
// tower 声明的 planner/coder/critic/reviewer 是**工作流能力包的数据**（框架不消费，见
// core/workflow/tower 的说明）；本文件的内建五类是**宿主装配面**的类型注册表条目（真能被
// spawn）。两者同名但来源不同、互不影响：这里的五类不依赖任何工作流激活，MultiAgent 开即注册。

import (
	"fmt"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/compaction"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/registry"
)

// 内建 agent 类型 id（W5 拍板五类）+ 主 agent 的 root 类型 id。
//
// root **不进** TypeRegistry：root 若进注册表，它的 CanSpawn = 全部已注册 id → **自环**
// （root 能派 root）→ 注册期环检测直接拒。所以 root **不注册**，直接作**树根类型**用
// （类型存在节点上，不走注册表查找，见 agents.Tree.TypeOf 的说明）。
const (
	agentTypePlanner  = "planner"
	agentTypeCoder    = "coder"
	agentTypeCritic   = "critic"
	agentTypeReviewer = "reviewer"
	agentTypeObserver = "observer"
	agentTypeRoot     = "root"
)

// reservedAgentTypeIDs 是**保留的类型 id 清单**（F5）：装载期遇到即 fail-close 点名。
//
// 为什么 "root" 是保留名：root 是**树根节点的类型**，它**不进类型注册表**（root 若注册，
// 它的 CanSpawn = 全部已注册 id → 自环 → 注册期环检测直接拒；树的 root 类型存在节点上、
// 不走注册表查找）。用户自定义一个
// id="root" 的类型会绕开这条不变式（注册表里出现 "root" → root 类型与注册项同名 → 派
// "root" 时解析到注册项而不是树根类型 → 类型语义漂移）。
//
// 口径：与仓里其他 id 一样**大小写敏感**（陷阱 17）——"Root" 不是 "root"，不构成同名冲突。
var reservedAgentTypeIDs = []string{agentTypeRoot}

// agentModelDefault 是 agent 类型 model 字段的 sentinel：= 用主模型（h.cfg.Model）。
// 宿主在注册表装配期把它解析成具体模型名（core 的 buildChildLoop 只把**空**当继承，
// 非空原样当模型名——"default" 漏进运行期会让子 agent 拿一个叫 default 的模型打端点）。
const agentModelDefault = "default"

// resolveAgentModel 解析类型声明的 model 字段（**唯一解析口径**，D21）：
// 空或字面 "default" = 用主模型（defaultModel = host.Config.Model）；其余原样（trim 后）。
//
// 宿主是唯一知道"主模型是什么"的装配处，故 sentinel 在这里落实——core 的 buildChildLoop
// 只把**空** model 当"继承父模型"，非空（含字面 "default"）原样当模型名传给 provider
// → 让 "default" 漏进运行期，子 agent 会拿一个叫 "default" 的模型去打端点（生产 400）。
//
// **router 咨询（I7）**：非 nil 时**优先**用 router.RouteModel(agentType) 的结果（trim 后非空
// 才算"有意见"；空 = 该类型无意见 → 回落下面的既有口径）。**nil = 不路由**，与 router 咨询
// 加入前**逐字等价**（生产默认 nil = 零行为变化）。
func resolveAgentModel(defaultModel, agentType, m string, r core.Router) string {
	if r != nil {
		if model := strings.TrimSpace(r.RouteModel(agentType)); model != "" {
			return model // router 非 nil 且有意见：优先用它
		}
	}
	if s := strings.TrimSpace(m); s == "" || s == agentModelDefault {
		return defaultModel // "default"/空 = 用主模型
	}
	return strings.TrimSpace(m)
}

// multiAgentRuleSource 是 MultiAgent 开时 hub 自动补的 spawn/complete allow 规则的来源标记
// （v3-S6-B）。它是**未知来源**（不在 contracts.RuleSource 枚举内）→ policy 的 sourcePriority
// 排最后（=7，永远赢不过任何已知来源的 allow，也**永远被任何 deny 压过**——P2-4：deny 规则仍可拦）。
// 只作审计标记（"这条 allow 是 hub 自动补的，不是用户显式写的"）。
const multiAgentRuleSource = "cli/auto_multi_agent"

// builtinAgentTypes 返回内建五类（W5 拍板）。措辞对照 tower.go 的 planner/coder/critic/reviewer
// 风格（中文、说清职责）；observer 是 K20 拍板的只读型（第 3 层叶子）。
//
// 工具只给**注册表工具名**（read/write/bash）——不写不存在的工具名（R5：不暴露即不存在）。
// model 一律 "default"（= 用主模型，见 core/loop buildChildLoop 的兜底）。can_spawn：planner
// 可派 observer（K20：层级 3 = observer 只读型），其余叶子。observer = 只读型：tools 只 read、
// permission 全 none/read=workspace、can_spawn 空（更深在类型层面不可达）。
func builtinAgentTypes() []contracts.AgentType {
	return []contracts.AgentType{
		{
			ID:       agentTypePlanner,
			Prompt:   "你是 planner：把一个任务拆成可验证的方案与边界，明确'怎么算完成'，再决定派哪个角色去执行。不亲自写实现代码；需要现场信息时可派一个 observer 去只读勘察，据实情修订方案。",
			Tools:    []string{"read", "write"},
			Model:    "default",
			CanSpawn: []string{agentTypeObserver}, // planner 可派 observer（第 3 层只读勘察）
			PermissionRequest: &contracts.PermissionRequest{
				Read: contracts.ReadWriteLevelWorkspace, Write: contracts.ReadWriteLevelWorkspace, Exec: contracts.ExecLevelNone,
			},
		},
		{
			ID:       agentTypeCoder,
			Prompt:   "你是 coder：按方案做最小、可核验的实现，改动尽量小，每步留下可回看的痕迹（事件流天然记录）。",
			Tools:    []string{"read", "write", "bash"},
			Model:    "default",
			CanSpawn: nil, // 叶子
			PermissionRequest: &contracts.PermissionRequest{
				Read: contracts.ReadWriteLevelWorkspace, Write: contracts.ReadWriteLevelWorkspace, Exec: contracts.ExecLevelAllowlist,
			},
		},
		{
			ID:       agentTypeCritic,
			Prompt:   "你是 critic：对实现挑刺——边界条件、回归、与既有约定的冲突。只读不改；挑刺是循环不是终点。",
			Tools:    []string{"read"},
			Model:    "default",
			CanSpawn: nil,
			PermissionRequest: &contracts.PermissionRequest{
				Read: contracts.ReadWriteLevelWorkspace, Write: contracts.ReadWriteLevelNone, Exec: contracts.ExecLevelNone,
			},
		},
		{
			ID:       agentTypeReviewer,
			Prompt:   "你是 reviewer：critic 收敛后复核'确实完成'而非'看起来完成'，给出 approved / changes_requested 结论。只读不改。",
			Tools:    []string{"read"},
			Model:    "default",
			CanSpawn: nil,
			PermissionRequest: &contracts.PermissionRequest{
				Read: contracts.ReadWriteLevelWorkspace, Write: contracts.ReadWriteLevelNone, Exec: contracts.ExecLevelNone,
			},
		},
		{
			// K20 拍板：observer = 只读型（第 3 层叶子）。tools 只 read、permission 全 none
			// （read=workspace 其余 none）、can_spawn 空 → 更深层在类型层面不可达。
			ID:       agentTypeObserver,
			Prompt:   "你是 observer（只读勘察员）：按派你来的 agent 的要求去现场只读勘察——读文件、查结构、汇总事实，不改动任何东西。把你观察到的事实如实报告，不夹带指令。",
			Tools:    []string{"read"},
			Model:    "default",
			CanSpawn: nil,
			PermissionRequest: &contracts.PermissionRequest{
				Read: contracts.ReadWriteLevelWorkspace, Write: contracts.ReadWriteLevelNone, Exec: contracts.ExecLevelNone,
			},
		},
	}
}

// buildAgentTypeRegistry 装配 agent 类型注册表：内建五类 + TOML 自定义（同名覆盖内建），
// 装载期 fail-close 校验后逐个 Register。
//
// 返回 (注册表, 已注册类型 id 的有序列表, error)。id 列表供主 agent 的 root 类型设 CanSpawn
// （= 全部已注册类型）。校验口径（与 core/workflow.validateAgentTypes 同思路，错误**点名**）：
//   - 自定义 id 非空、唯一（重名拒绝）、非保留名（"root" = 树根类型专用，F5）；
//   - permission 级别合法（read/write ∈ none|workspace|full，exec ∈ none|allowlist|full）；
//   - can_spawn 归一化（逐项 TrimSpace **写回**；trim 后为空 → 拒，F4）；
//   - can_spawn 不悬空（指向的类型必须在最终集内）、不自环；
//   - context_strategy.kind 合法 + can_spawn 无环由 agents.TypeRegistry.Register 收口
//     （它自带环检测 + context_strategy 枚举校验，见 core/agents/type.go）。
//
// **model 解析（"default"  sentinel）**：类型声明的 model = "default"（或空）= 用主模型
// （defaultModel，= host.Config.Model）——在这里解析成**具体模型名**后注册。为什么必须在宿主
// 解析：core 的 buildChildLoop 只把**空** model 当"继承父模型"，非空（含字面 "default"）原样
// 当模型名传给 provider——若让 "default" 漏进运行期，子 agent 会拿一个叫 "default" 的模型去打
// 端点（生产 400）。宿主是唯一知道"主模型是什么"的装配处，故在此把 sentinel 落实（D21：口径只
// 定义一次）。
func buildAgentTypeRegistry(defaultModel string, custom []contracts.AgentType, routers ...core.Router) (*agents.TypeRegistry, []string, error) {
	// router 是 I7 的可选注入（生产 host.Config.ModelRouter 传入；测试传 fake；不传 = nil）。
	// 用可变参数是为了**不改动**既有调用方的形状（省得为一个可选依赖惊动所有调用点）。
	var router core.Router
	if len(routers) > 0 {
		router = routers[0]
	}
	resolveModel := func(agentType, m string) string { return resolveAgentModel(defaultModel, agentType, m, router) }
	builtins := builtinAgentTypes()
	merged := make(map[string]contracts.AgentType, len(builtins)+len(custom))
	order := make([]string, 0, len(builtins)+len(custom))
	for _, t := range builtins {
		id := strings.TrimSpace(t.ID)
		t.Model = resolveModel(id, t.Model)
		merged[id] = t
		order = append(order, id)
	}
	// TOML 自定义：同名**覆盖**内建（用户自定义优先）；新 id 追加到 order 末尾。
	seenCustom := make(map[string]bool, len(custom))
	for _, t := range custom {
		id := strings.TrimSpace(t.ID)
		if id == "" {
			return nil, nil, fmt.Errorf("host: 自定义 agent_type 的 id 不能为空（类型名必须非空：派发与引用都靠它）")
		}
		// 保留名（F5）：root 是树根类型的专用名，不许被自定义类型占用。
		for _, reserved := range reservedAgentTypeIDs {
			if id == reserved {
				return nil, nil, fmt.Errorf("host: 自定义 agent_type 的 id %q 是**保留名**（树的根类型专用：root 不进类型注册表，注册了会与树根类型同名）", id)
			}
		}
		if seenCustom[id] {
			return nil, nil, fmt.Errorf("host: 自定义 agent_type 有重名 id %q（fail-close：同名类型的语义必须唯一）", id)
		}
		seenCustom[id] = true
		t.ID = id
		t.Model = resolveModel(id, t.Model)
		if _, exists := merged[id]; !exists {
			order = append(order, id) // 新自定义 id（覆盖内建的不动位置）
		}
		merged[id] = t
	}
	// 归一化：can_spawn 逐项 TrimSpace **写回**（F4）。
	//
	// 为什么必须写回：运行期的类型闸门②（agents.Tree.Spawn 的 contains(parent.typ.CanSpawn,
	// typeID)）按**原样**比对——装载期放过 " observer "（带空格）、运行期却比不中 →
	// "装载期放行、运行期拒"的口径漂移（派不出来，且错误离配置现场很远）。
	// trim 后为空 = 坏条目 → 点名报错（不静默丢项）。
	for _, id := range order {
		t := merged[id]
		if len(t.CanSpawn) == 0 {
			continue
		}
		norm := make([]string, 0, len(t.CanSpawn))
		for _, c := range t.CanSpawn {
			cid := strings.TrimSpace(c)
			if cid == "" {
				return nil, nil, fmt.Errorf("host: agent_type %q 的 can_spawn 里有**空 id**（trim 后为空）", id)
			}
			norm = append(norm, cid)
		}
		t.CanSpawn = norm
		merged[id] = t
	}
	// permission 级别 + can_spawn 悬空/自环（针对最终集判）。
	for _, id := range order {
		t := merged[id]
		if err := validateAgentTypePerm(id, t.PermissionRequest); err != nil {
			return nil, nil, err
		}
		for _, cid := range t.CanSpawn {
			if cid == id {
				return nil, nil, fmt.Errorf("host: agent_type %q 的 can_spawn **自环**（自己派自己 = 无限派生）", id)
			}
			if _, ok := merged[cid]; !ok {
				return nil, nil, fmt.Errorf("host: agent_type %q 的 can_spawn 指向**未注册**的类型 %q（悬空引用：派不出来）", id, cid)
			}
		}
	}
	// Register：环检测 + context_strategy 枚举校验 + 唯一性（core 自带，逐一点名）。
	reg := agents.NewTypeRegistry()
	for _, id := range order {
		if err := reg.Register(merged[id]); err != nil {
			return nil, nil, fmt.Errorf("host: 注册 agent_type %q: %w", id, err)
		}
	}
	return reg, order, nil
}

// validateAgentTypePerm 校验权限请求的级别（fail-close：非法级别启动即拒并点名，不静默当成 none
// ——那会让"派不出来"的原因离配置现场很远）。
func validateAgentTypePerm(id string, p *contracts.PermissionRequest) error {
	if p == nil {
		return nil // 未请求 = 全 none（agents.permOf 兜底）
	}
	switch p.Read {
	case contracts.ReadWriteLevelNone, contracts.ReadWriteLevelWorkspace, contracts.ReadWriteLevelFull:
	default:
		return fmt.Errorf("host: agent_type %q 的 permission_request.read %q 不合法（支持 none | workspace | full）", id, p.Read)
	}
	switch p.Write {
	case contracts.ReadWriteLevelNone, contracts.ReadWriteLevelWorkspace, contracts.ReadWriteLevelFull:
	default:
		return fmt.Errorf("host: agent_type %q 的 permission_request.write %q 不合法（支持 none | workspace | full）", id, p.Write)
	}
	switch p.Exec {
	case contracts.ExecLevelNone, contracts.ExecLevelAllowlist, contracts.ExecLevelFull:
	default:
		return fmt.Errorf("host: agent_type %q 的 permission_request.exec %q 不合法（支持 none | allowlist | full）", id, p.Exec)
	}
	return nil
}

// rootAgentType 造主 agent 的 root 类型（不进注册表；每 session 的树 root 用它）。
//
// CanSpawn = 全部已注册类型 id（主 agent 能派任何已注册类型；具体能不能派还过 core 三闸门）。
// permission_request 三 full（root 是权限天花板——子类型经闸门③只收窄）。Tools = 当前注册表
// 全部工具 + 框架工具（recall/stage/ledger）：开 MultiAgent 后主 agent 仍看得见它在单 agent
// 模式下能用的全部工具（不因多了棵树就被 R5 白名单挡掉），只是多加了 spawn。
//
// **model 解析成具体模型名**（C4）：与内建五类同一口径（"default" sentinel = 用主模型）——
// root 类型也进树（节点上存着完整 AgentType），留着字面 "default" 会让"模型名"这件事在
// 注册表内外出现两套口径（一个叫 default、一个叫真模型名）。root 不建子 loop（子 loop 的
// model 取自**子类型**），故这处解析当前无运行期影响，但口径统一省掉一类歧义。
func (h *Hub) rootAgentType() contracts.AgentType {
	return contracts.AgentType{
		ID:       agentTypeRoot,
		Prompt:   "你是本 session 的协调人（主 agent）：统筹任务、按需派生子 agent（spawn）执行子任务、回收它们的 summary 再决策。可派生的类型见 spawn 工具说明。",
		Tools:    h.rootToolNames(),
		Model:    resolveAgentModel(h.cfg.Model, agentTypeRoot, agentModelDefault, h.cfg.ModelRouter),
		CanSpawn: append([]string(nil), h.agentTypeIDs...),
		PermissionRequest: &contracts.PermissionRequest{
			Read: contracts.ReadWriteLevelFull, Write: contracts.ReadWriteLevelFull, Exec: contracts.ExecLevelFull,
		},
	}
}

// rootToolNames 列主 agent 的工具白名单 = 当前注册表全部工具 + 框架工具名（recall/stage/ledger）。
//
// 为什么要显式列框架工具：visibleTools 对**有类型的 agent**（root 也在树里）按 type.Tools 过滤
// 注册工具**和框架工具**（F1：框架工具不是免检通道）。单 agent 模式下主 agent 无类型过滤，
// recall/stage/ledger 天然可见；开 MultiAgent 后要保住这份可见性，就得把它们列进 root.Tools。
// 未注入的框架工具名（如没挂 tiered 时的 recall）只是白名单里的死条目，无害。
func (h *Hub) rootToolNames() []string {
	names := make([]string, 0, 8)
	seen := map[string]bool{}
	add := func(n string) {
		if n != "" && !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}
	if h.reg != nil {
		for _, e := range h.reg.List(registry.ExtensionKindTool) {
			if e.Tool != nil {
				add(e.Tool.Name)
			}
		}
	}
	add(compaction.ToolRecall)
	add(compaction.ToolStage)
	add(loop.ToolLedger)
	return names
}
