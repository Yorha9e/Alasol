package main

// MultiAgent 装配面（缺口 8）：给 `alasol run` / `alasol resume` 这两条**独立进程直连
// loop**（不经 host.Hub）的路径补上与 serve 同口径的多 agent 接线——类型注册表 + agent 树
// + loop.Agents。形态对照 host/hub.go 的 loopFor；口径复刻自 host/agenttypes.go（那边
// 未导出，且范围铁律不许为此改 host）。
//
// # 与 serve 的差别（如实记）
//
// run/resume 没有 hub 的任何 per-session 面：无在线表注入、无 fork 集合、无发现表 Claim、
// 无 loop/树缓存；树是**本进程一次性**装配（run 单轮、resume 单次恢复+续跑），进程退出
// 即散（事件流才是事实源，树不持久化）。run/resume 直连 loop 的 MultiAgent 面 = 树 +
// loop.Agents 接线两件，其余 hub 面一概没有。
//
// # 开关口径
//
// MultiAgent 关（默认）：runnerAgentsEnv 返回 nil → loop.New 收 nil = 单 agent 模式零行为
// 变化——不建树、不传 Agents、**不建也不校验**类型注册表（与 host.New「关 = 不建不校验」
// 同口径：用一个用不到的能力不许拒绝启动）。
//
// # 副本说明
//
// 下面的内建五类 / 注册表装配 / root 类型按 host/agenttypes.go 的**口径**复刻（那边未导出，
// 范围铁律不许为此改 host）。若 host 日后导出类型注册表/树的装配函数，应改为调用导出并
// 删除本文件的副本（挂账见交付报告）。

import (
	"fmt"
	"os"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/compaction"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/registry"
)

const (
	// agentTypeRoot 是树根类型的 id（**不进类型注册表**：root 若注册，它的 CanSpawn =
	// 全部已注册 id → 自环 → 注册期环检测直接拒；口径同 host/agenttypes.go，F5）。
	agentTypeRoot = "root"
	// agentModelDefault 是 agent 类型 model 字段的 sentinel：= 用主模型。宿主在注册表
	// 装配期解析成具体模型名（core 的 buildChildLoop 只把**空**当继承，非空原样当模型名
	// ——"default" 漏进运行期会让子 agent 拿一个叫 default 的模型打端点，生产 400）。
	agentModelDefault = "default"
	// multiAgentRuleSource 是 MultiAgent 开时自动补的 spawn/complete allow 规则的来源
	// 标记——**与 host/agenttypes.go 的同名字面量同值**：未知来源（不在 contracts.RuleSource
	// 枚举内）→ policy 的 sourcePriority 排最后（=7，永远赢不过任何已知来源的 allow，也
	// 永远被任何 deny 压过——P2-4：deny 规则仍可拦）。只作审计标记。
	multiAgentRuleSource = "cli/auto_multi_agent"
)

// reservedAgentTypeIDs 是保留的类型 id 清单（F5）：装载期遇到即 fail-close 点名。
// 口径同 host/agenttypes.go："root" 是树根节点的类型专用名（不进注册表，注册了会与
// 树根类型同名 → 类型语义漂移）。大小写敏感（陷阱 17）——"Root" 不是 "root"。
var reservedAgentTypeIDs = []string{agentTypeRoot}

// runnerMultiAgentPolicyRules 返回 MultiAgent 开时自动补的 spawn/complete allow 规则
// （与 host.New 同口径，见 hub.go 的同段注释）：开 multi_agent = **预期主 agent 能 spawn**，
// 而 spawn/complete 是框架内部工具、通常不在用户的 -allow 列表里 → 策略 deny →
// toolVisible 挡掉 → 模型根本派不出子 agent。关（默认）→ nil = 不补 = 零行为变化。
//
// 策略链照走（P2-4）：未知来源 allow 优先级最低，任何来源的 deny 仍可拦。
func runnerMultiAgentPolicyRules(enabled bool) []contracts.PolicyRule {
	if !enabled {
		return nil
	}
	rules := make([]contracts.PolicyRule, 0, 2)
	for _, name := range []string{loop.ToolSpawn, loop.ToolComplete} {
		rules = append(rules, contracts.PolicyRule{
			Effect: contracts.EffectAllow,
			Tool:   name,
			Source: contracts.RuleSource(multiAgentRuleSource),
		})
	}
	return rules
}

// runnerAgentsEnv 装配 run/resume 的多 agent 环境（缺口 8，形态对照 host 的 loopFor）。
//
//   - 关（默认）→ (nil, nil)：loop.New 收 nil Agents = 单 agent 模式零行为变化，
//     且**不建也不校验**类型注册表（坏 agent_types 不拒绝一个根本用不到它的回路）。
//   - 开 → 类型注册表（内建五类 + TOML 自定义，fail-close 校验）→ agent 树
//     （root = 传入的主 agent id，WithRootID 保证 树 root ≡ loop.AgentID 同源）→
//     loop.Agents{Tree, Verifier: nil, ChildStrategy: nil}。
//
// ChildStrategy 挂账（缺口 8 报告）：hub 的 childStrategyFactory → tieredFactory 依赖
// host 私有的 source 适配器（storeTextSource / providerSummarizer / busEmitter /
// validationSignalSource 等）与 host.Config.contextStrategyParams——core 只导出接口、
// 不导出实现，cmd 侧复刻 = 第二份口径（D21 禁）；且 run/resume 主 agent 本身也没接
// ContextStrategy（append 默认），单接子钩子是半拉子面。故留 nil：未声明/append 的子类型
// 照常走默认追加式（内建五类均未声明 → 不受影响）；**声明非 append 的自定义类型 spawn
// 会按 core 契约 fail-close 成 agent.failed（不静默降级）**——留待 host 导出
// childStrategyFactory（或其 source 适配器）后接线。
//
// Verifier 同 hub：宿主暂无生产实现 → nil = 不核验（refs 原样保留）。
func runnerAgentsEnv(enabled bool, defaultModel string, custom []contracts.AgentType,
	agentID contracts.Ulid, toolReg *registry.Registry) (*loop.Agents, error) {
	if !enabled {
		return nil, nil // 关（默认）= 不建不校验 = 单 agent 零行为变化
	}
	// 装配期**点名告知**"本进程不支持的 context_strategy kind"（非 append）——不阻断，
	// 只提前告知（见 warnUnsupportedContextStrategies 的口径说明）。
	warnUnsupportedContextStrategies(custom)
	typeReg, ids, err := buildRunnerAgentTypeRegistry(defaultModel, custom)
	if err != nil {
		return nil, fmt.Errorf("装配 agent 类型注册表: %w", err)
	}
	rootType := runnerRootAgentType(defaultModel, ids, runnerRootToolNames(toolReg))
	tree := agents.NewTree(rootType,
		agents.WithRootID(agentID),                       // root = 主 agent id（与 loop.AgentID 同源同参）
		agents.WithTypes(typeReg),                        // 类型注册表（内建五类 + TOML 自定义）
		agents.WithMaxDepth(agents.DefaultMaxAgentDepth), // 3 = 两级委派（K17，同 hub）
	)
	return &loop.Agents{
		Tree: tree,
		// Verifier：summary refs 核验器（K24/Q7）。与 hub 同：暂无生产实现 → nil = 不核验。
		Verifier: nil,
		// ChildStrategy：挂账（见函数头注释）——run/resume 的子 agent 策略钩子未接，
		// 子 agent 用默认上下文策略。
		ChildStrategy: nil,
	}, nil
}

// warnUnsupportedContextStrategies 在装配期**点名告知**"本进程不支持的
// context_strategy kind"（非 append）——**只 Warn，不阻断**。
//
// # 口径（T5-2）
//
// run/resume 是**独立进程直连 loop**，**没接** loop.Agents.ChildStrategy（钩子依赖 host
// 私有的 source 适配器，见本文件"ChildStrategy 挂账"）——所以类型声明了非 append 的
// 上下文策略时，core 在 **spawn 时**会 fail-close（该 spawn 以 agent.failed 收口，
// **不静默降级**成 append；这是 core 的既有语义）。
//
// **为什么不阻断**：fail-close 发生在 spawn 时；装配期就拒启动 = 把"用不到的能力"
// 变成拒绝启动的理由（与"MultiAgent 关 = 不建不校验"同纪律）。故：这里只 Warn 点名
// ——**提前告知**，免得现场一头雾水（"配了 tiered 怎么 spawn 不出来？"）。
//
// # 两前端口径分叉（如实记）
//
// serve（host 装配）**接了** tiered 钩子 → 同一份声明在 serve 下**能跑**；
// run/resume **没接** → 同一份声明在 run/resume 里 spawn 会 fail-close。
// 这不是 bug，是两个前端的**能力面不同**（run/resume 直连 loop，没有 host 的
// source 适配器与 router 面）——但必须**提前说清**，而不是让人自己撞。
//
// 何时不 Warn：MultiAgent 关（压根不建注册表、不 spawn → 没有 fail-close 可发生）；
// 未声明 context_strategy（= append 默认，core 根本不调钩子）；声明的是 append。
func warnUnsupportedContextStrategies(custom []contracts.AgentType) {
	for _, t := range custom {
		if t.ContextStrategy == nil {
			continue // 未声明 = append 默认（core 不调钩子，不受影响）
		}
		kind := strings.TrimSpace(string(t.ContextStrategy.Kind))
		if kind == "" || kind == string(contracts.ContextStrategyKindAppend) {
			continue
		}
		fmt.Fprintf(os.Stderr,
			"[alasol] agent_type %q 声明了 context_strategy.kind=%q，但 run/resume **未接**子 agent 上下文策略钩子"+
				"（ChildStrategy=nil）：该类型被 spawn 时会 fail-close（以 agent.failed 收口，不静默降级成 append）。"+
				"同一份声明在 `alasol serve` 下可用（host 装配了 tiered 钩子）。\n",
			t.ID, kind)
	}
}

// buildRunnerAgentTypeRegistry 装配 agent 类型注册表：内建五类 + TOML 自定义（同名覆盖
// 内建），装载期 fail-close 校验后逐个 Register。**复刻自 host/agenttypes.go 的
// buildAgentTypeRegistry**（那边未导出；差异：cmd 无 ModelRouter 面 → 不咨询路由）。
//
// 返回 (注册表, 已注册类型 id 的有序列表, error)。id 列表供 root 类型设 CanSpawn
// （= 全部已注册类型）。校验口径（错误**点名**，不修复 S9）：
//   - 自定义 id 非空、唯一（重名拒绝）、非保留名（"root" = 树根类型专用，F5）；
//   - permission 级别合法（read/write ∈ none|workspace|full，exec ∈ none|allowlist|full）；
//   - can_spawn 归一化（逐项 TrimSpace **写回**；trim 后为空 → 拒，F4）；
//   - can_spawn 不悬空（指向的类型必须在最终集内）、不自环；
//   - context_strategy.kind 合法 + can_spawn 环由 agents.TypeRegistry.Register 收口
//     （它自带环检测 + 枚举校验 + 重名，见 core/agents/type.go）。
//
// **model 解析（"default" sentinel）**：类型声明的 model = "default"（或空）= 用主模型
// （defaultModel）——在装配期解析成具体模型名（同 host 口径：core 的 buildChildLoop 只把
// 空 model 当继承，非空含字面 "default" 原样当模型名 → "default" 漏进运行期就是生产 400）。
func buildRunnerAgentTypeRegistry(defaultModel string, custom []contracts.AgentType) (*agents.TypeRegistry, []string, error) {
	resolveModel := func(m string) string { return resolveAgentModel(defaultModel, m) }
	builtins := builtinAgentTypes()
	merged := make(map[string]contracts.AgentType, len(builtins)+len(custom))
	order := make([]string, 0, len(builtins)+len(custom))
	for _, t := range builtins {
		id := strings.TrimSpace(t.ID)
		t.Model = resolveModel(t.Model)
		merged[id] = t
		order = append(order, id)
	}
	// TOML 自定义：同名**覆盖**内建（用户自定义优先）；新 id 追加到 order 末尾。
	seenCustom := make(map[string]bool, len(custom))
	for _, t := range custom {
		id := strings.TrimSpace(t.ID)
		if id == "" {
			return nil, nil, fmt.Errorf("alasol: 自定义 agent_type 的 id 不能为空（类型名必须非空：派发与引用都靠它）")
		}
		// 保留名（F5）：root 是树根类型的专用名，不许被自定义类型占用。
		for _, reserved := range reservedAgentTypeIDs {
			if id == reserved {
				return nil, nil, fmt.Errorf("alasol: 自定义 agent_type 的 id %q 是**保留名**（树的根类型专用：root 不进类型注册表，注册了会与树根类型同名）", id)
			}
		}
		if seenCustom[id] {
			return nil, nil, fmt.Errorf("alasol: 自定义 agent_type 有重名 id %q（fail-close：同名类型的语义必须唯一）", id)
		}
		seenCustom[id] = true
		t.ID = id
		t.Model = resolveModel(t.Model)
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
				return nil, nil, fmt.Errorf("alasol: agent_type %q 的 can_spawn 里有**空 id**（trim 后为空）", id)
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
				return nil, nil, fmt.Errorf("alasol: agent_type %q 的 can_spawn **自环**（自己派自己 = 无限派生）", id)
			}
			if _, ok := merged[cid]; !ok {
				return nil, nil, fmt.Errorf("alasol: agent_type %q 的 can_spawn 指向**未注册**的类型 %q（悬空引用：派不出来）", id, cid)
			}
		}
	}
	// Register：环检测 + context_strategy 枚举校验 + 唯一性（core 自带，逐一点名）。
	reg := agents.NewTypeRegistry()
	for _, id := range order {
		if err := reg.Register(merged[id]); err != nil {
			return nil, nil, fmt.Errorf("alasol: 注册 agent_type %q: %w", id, err)
		}
	}
	return reg, order, nil
}

// resolveAgentModel 解析类型声明的 model 字段（**唯一解析口径**，D21；复刻自
// host/agenttypes.go 的同名函数，去掉 router 参数——cmd 无 ModelRouter 面）：
// 空或字面 "default" = 用主模型（defaultModel）；其余原样（trim 后）。
func resolveAgentModel(defaultModel, m string) string {
	if s := strings.TrimSpace(m); s == "" || s == agentModelDefault {
		return defaultModel
	}
	return strings.TrimSpace(m)
}

// validateAgentTypePerm 校验权限请求的级别（fail-close：非法级别启动即拒并点名，不静默
// 当成 none——那会让"派不出来"的原因离配置现场很远）。复刻自 host/agenttypes.go。
func validateAgentTypePerm(id string, p *contracts.PermissionRequest) error {
	if p == nil {
		return nil // 未请求 = 全 none（agents.permOf 兜底）
	}
	switch p.Read {
	case contracts.ReadWriteLevelNone, contracts.ReadWriteLevelWorkspace, contracts.ReadWriteLevelFull:
	default:
		return fmt.Errorf("alasol: agent_type %q 的 permission_request.read %q 不合法（支持 none | workspace | full）", id, p.Read)
	}
	switch p.Write {
	case contracts.ReadWriteLevelNone, contracts.ReadWriteLevelWorkspace, contracts.ReadWriteLevelFull:
	default:
		return fmt.Errorf("alasol: agent_type %q 的 permission_request.write %q 不合法（支持 none | workspace | full）", id, p.Write)
	}
	switch p.Exec {
	case contracts.ExecLevelNone, contracts.ExecLevelAllowlist, contracts.ExecLevelFull:
	default:
		return fmt.Errorf("alasol: agent_type %q 的 permission_request.exec %q 不合法（支持 none | allowlist | full）", id, p.Exec)
	}
	return nil
}

// builtinAgentTypes 返回内建五类（W5 拍板）。**数据复刻自 host/agenttypes.go 的
// builtinAgentTypes**（那边未导出）——措辞对照 tower.go 的 planner/coder/critic/reviewer
// 风格（中文、说清职责）；observer = K20 拍板的只读型（第 3 层叶子）。
//
// 工具只给**注册表工具名**（read/write/bash）——不写不存在的工具名（R5：不暴露即不存在）。
// model 一律 "default"（= 用主模型，见 resolveAgentModel）。can_spawn：planner 可派
// observer（K20：层级 3 = observer 只读型），其余叶子。observer = 只读型：tools 只 read、
// permission 全 none/read=workspace、can_spawn 空（更深在类型层面不可达）。
func builtinAgentTypes() []contracts.AgentType {
	return []contracts.AgentType{
		{
			ID:       "planner",
			Prompt:   "你是 planner：把一个任务拆成可验证的方案与边界，明确'怎么算完成'，再决定派哪个角色去执行。不亲自写实现代码；需要现场信息时可派一个 observer 去只读勘察，据实情修订方案。",
			Tools:    []string{"read", "write"},
			Model:    "default",
			CanSpawn: []string{"observer"}, // planner 可派 observer（第 3 层只读勘察）
			PermissionRequest: &contracts.PermissionRequest{
				Read: contracts.ReadWriteLevelWorkspace, Write: contracts.ReadWriteLevelWorkspace, Exec: contracts.ExecLevelNone,
			},
		},
		{
			ID:       "coder",
			Prompt:   "你是 coder：按方案做最小、可核验的实现，改动尽量小，每步留下可回看的痕迹（事件流天然记录）。",
			Tools:    []string{"read", "write", "bash"},
			Model:    "default",
			CanSpawn: nil, // 叶子
			PermissionRequest: &contracts.PermissionRequest{
				Read: contracts.ReadWriteLevelWorkspace, Write: contracts.ReadWriteLevelWorkspace, Exec: contracts.ExecLevelAllowlist,
			},
		},
		{
			ID:       "critic",
			Prompt:   "你是 critic：对实现挑刺——边界条件、回归、与既有约定的冲突。只读不改；挑刺是循环不是终点。",
			Tools:    []string{"read"},
			Model:    "default",
			CanSpawn: nil,
			PermissionRequest: &contracts.PermissionRequest{
				Read: contracts.ReadWriteLevelWorkspace, Write: contracts.ReadWriteLevelNone, Exec: contracts.ExecLevelNone,
			},
		},
		{
			ID:       "reviewer",
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
			ID:       "observer",
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

// runnerRootAgentType 造主 agent 的 root 类型（不进注册表；每棵 run/resume 树的 root 用它）。
// **复刻自 host/agenttypes.go 的 rootAgentType**（那边是 Hub 方法；差异：工具名与类型 id
// 由参数传入，不读 hub 状态）。
//
// CanSpawn = 全部已注册类型 id（主 agent 能派任何已注册类型；具体能不能派还过 core 三闸门）。
// permission_request 三 full（root 是权限天花板——子类型经闸门③只收窄）。Tools = 当前注册表
// 全部工具 + 框架工具名（recall/stage/ledger）：开 MultiAgent 后主 agent 仍看得见它在单
// agent 模式下能用的全部工具（不因多了棵树就被 R5 白名单挡掉），只是多加了 spawn。
//
// **model 解析成具体模型名**（C4）：与内建五类同一口径（"default" sentinel = 用主模型）。
// root 不建子 loop（子 loop 的 model 取自**子类型**），故这处解析当前无运行期影响，但口径
// 统一省掉一类歧义。
func runnerRootAgentType(defaultModel string, typeIDs []string, toolNames []string) contracts.AgentType {
	return contracts.AgentType{
		ID:       agentTypeRoot,
		Prompt:   "你是本 session 的协调人（主 agent）：统筹任务、按需派生子 agent（spawn）执行子任务、回收它们的 summary 再决策。可派生的类型见 spawn 工具说明。",
		Tools:    toolNames,
		Model:    resolveAgentModel(defaultModel, agentModelDefault),
		CanSpawn: append([]string(nil), typeIDs...),
		PermissionRequest: &contracts.PermissionRequest{
			Read: contracts.ReadWriteLevelFull, Write: contracts.ReadWriteLevelFull, Exec: contracts.ExecLevelFull,
		},
	}
}

// runnerRootToolNames 列 root 类型的工具白名单 = 当前注册表全部工具 + 框架工具名
// （recall/stage/ledger）。**复刻自 host/agenttypes.go 的 rootToolNames**。
//
// 为什么要显式列框架工具：visibleTools 对**有类型的 agent**（root 也在树里）按 type.Tools
// 过滤注册工具**和框架工具**（F1：框架工具不是免检通道）。单 agent 模式下主 agent 无类型
// 过滤，recall/stage/ledger 天然可见；开 MultiAgent 后要保住这份可见性，就得把它们列进
// root.Tools。未注入的框架工具名（如没挂 tiered 时的 recall）只是白名单里的死条目，无害。
//
// 必须在工具注册**全部完成之后**调用（run侧含 MCP 工具）：否则 MultiAgent 开会把晚注册
// 的工具按 R5 类型过滤挡掉。
func runnerRootToolNames(toolReg *registry.Registry) []string {
	names := make([]string, 0, 8)
	seen := map[string]bool{}
	add := func(n string) {
		if n != "" && !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}
	if toolReg != nil {
		for _, e := range toolReg.List(registry.ExtensionKindTool) {
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
