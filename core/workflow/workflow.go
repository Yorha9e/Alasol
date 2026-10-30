// Package workflow 是 v3-S3 的**工作流引擎**：工作流 = **能力包**，不是流程（W1）。
//
// # 哲学（07 W 区裁决，逐条对齐）
//
//   - W1 工作流本质 = **能力包的提供与批准**：一套工作哲学 + 子代理类型 + 上下文管理方式
//   - 关键阶段部件 + 预批准。**不是流程**——本包不实现流程执行器/状态机/强制序列。
//   - W2 阶段 = **设计哲学**（如 plan-coder-critic-review），不是强制序列，不规定
//     "这个阶段必须派哪些 subagent"。强制反而不适合（用户拍板）。
//   - W3 触发方式 = slash 命令 / 显式触发 → **哲学文本进 system prompt 供主 agent 选择**。
//     本包提供 AugmentSystem 把哲学文本拼进 system prompt；框架不强制 agent 遵循。
//   - W21 编排主体 = **主 agent 自决**：开几个 subagent、用哪个 agent_type、并行与否，
//     框架**不校验**序列。能力包只提供原料（agent_types / 哲学），不替主 agent 编排。
//   - W4 追加 loop 可组合：同一工作流可再挂 review-critic 之类追加段（Compose）。
//
// # W14：代码扩展点只有两处
//
//  1. **MilestoneHandler**（里程碑部件）——如合并前仿 CI 检查、critic→reviewer 收敛点。
//     见本包的 MilestoneHandler 接口与 tower 包的首个实现。
//  2. **自定义上下文策略**——挂 core/compaction.AssemblerFactory（loop.Config.ContextStrategy）。
//     这是 loop 的注入点，不在本包内；本包只在工作流的 context_strategy 字段里**声明**
//     用哪种策略（数据），实际装配在宿主。**子 agent 的策略也走同一个扩展点**：经
//     loop.Agents.ChildStrategy 装配（同一扩展点 AssemblerFactory，作用域 = 按 agent_type
//     显式声明；W6-A：未声明 = append 默认，**不继承主 agent**；声明非 append 而宿主没接
//     钩子 → 该 spawn 的子 agent 以 agent.failed 收口，fail-close 不静默降级）。
//
// **别发明第三个扩展点**：agent_types / gates / grants 都是**纯数据**（contracts.Workflow，
// 可被任何语言写/组合/追加，W13），不需要 Go 代码；只有里程碑部件与上下文策略是代码。
//
// # 挂 / 不挂零行为变化（酸测试的基石）
//
// 不挂任何工作流 = 宿主不给 loop 注哲学、不挂里程碑 → loop 行为与 v2 逐字节等价。
// 本包不持有全局状态、不在 init 里注册任何东西；工作流由宿主显式装载并显式激活。
package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/agents"
)

// 工作流事件的 core kind（F7 只增；载荷形状见 contracts.Workflow*Payload，
// schema 见 contracts/schema/event.json 的 x-kind-payload）。宿主装配层经 bus 发射。
const (
	// KindActivated 一个 session 激活一个工作流能力包（W3 触发审计）。
	KindActivated = "workflow.activated"
	// KindMilestone 一次工作流里程碑被处理（W14 MilestoneHandler 实证）。
	KindMilestone = "workflow.milestone"
)

// ActivatedEvent 把激活事实映射成 workflow.activated 事件的 payload。
//
// command 为空 = **显式触发**（无 slash 命令，W3）；非空 = **实际触发的 slash 命令名**
// （能力包自己声明的入口，如 tower 的 "tower"——chat/TUI 都把它接成了真能敲的入口）。
// philosophy 是本次注入 system prompt 的哲学原文（审计：事后可复盘当时注入了什么）。
func ActivatedEvent(workflowID, command, philosophy string) contracts.WorkflowActivatedPayload {
	p := contracts.WorkflowActivatedPayload{
		WorkflowID: workflowID,
		Philosophy: philosophy,
	}
	if c := strings.TrimSpace(command); c != "" {
		p.Command = &c
	}
	return p
}

// MilestoneEvent 把里程碑结论映射成 workflow.milestone 事件的 payload。
func MilestoneEvent(workflowID, milestone string, passed bool, note string) contracts.WorkflowMilestonePayload {
	p := contracts.WorkflowMilestonePayload{
		WorkflowID: workflowID,
		Milestone:  milestone,
		Passed:     passed,
	}
	if n := strings.TrimSpace(note); n != "" {
		p.Note = &n
	}
	return p
}

// ---------------------------------------------------------------------------
// W14 代码扩展点之一：里程碑部件
// ---------------------------------------------------------------------------

// Milestone 是一次里程碑信号的输入（W14：里程碑部件被触发时的上下文）。
type Milestone struct {
	// WorkflowID 触发里程碑的工作流能力包 id。
	WorkflowID string
	// SessionID / AgentID 触发方归属（workflow.milestone 事件的 envelope 用）。
	SessionID contracts.Ulid
	AgentID   contracts.Ulid
	// Kind 里程碑类型（与某个 MilestoneHandler.Name() 对应；如 validation.passed / merge.ci）。
	Kind string
	// Data 里程碑附带的原始数据（开放 JSON，由触发方提供，部件自行解释；可空）。
	Data json.RawMessage
}

// MilestoneResult 是一次里程碑处理的结论。
type MilestoneResult struct {
	// Passed 里程碑是否通过（true = 验证通过点达成 → 可联动阶段硬边界，v2-S1）。
	Passed bool
	// Note 人读说明（进 workflow.milestone 事件；撞车依据 / 失败原因 / reviewer 结论）。
	Note string
}

// MilestoneHandler 是工作流的**里程碑部件**（W14 两个代码扩展点之一）。
//
// 里程碑 = 框架确认的**验证通过点**（如合并前仿 CI 检查通过 / critic→reviewer 循环收敛）。
// 框架**不强制序列**（W2/W21）：主 agent 自决何时触发；部件只在该被触发时处理并给结论，
// 不反过来驱动主 agent。passed=true 时由宿主决定是否联动阶段硬边界（MarkValidationPassed）。
//
// 实现必须是并发安全的（宿主可能从多个请求并发触发同一工作流的里程碑）。
type MilestoneHandler interface {
	// Name 是里程碑名（触发方按它派发；如 "validation.passed"）。
	Name() string
	// Handle 处理一次里程碑信号，返回是否通过与人读说明。
	Handle(ctx context.Context, m Milestone) (MilestoneResult, error)
}

// ---------------------------------------------------------------------------
// Package：一个已加载的工作流能力包（W1）
// ---------------------------------------------------------------------------

// Package 是一个已加载的工作流**能力包**（W1）。
//
// 它把契约数据（contracts.Workflow，纯数据、跨边界、可组合，W13）与代码部件
// （MilestoneHandler，W14）合在一起，是"能力包的提供与批准"的运行形态。
//
// **不是流程执行器**（W1/W2）：不规定阶段序列、不校验编排、不持有"当前处于哪个阶段"
// 之类的状态。框架只提供能力，主 agent 自决（W21）。并发安全（不可变，装载后不改）。
type Package struct {
	spec       contracts.Workflow
	milestones map[string]MilestoneHandler
}

// NewPackage 用契约数据 + 里程碑部件构造一个能力包。
//
// 校验（fail-close，不静默修复）：id 非空；philosophy 非空（W3：没哲学就没东西注入
// system prompt）；里程碑部件名非空且不重名（重名会让派发语义不唯一）；
// **agent_types 自洽**（不可信配置，见 validateAgentTypes：id 唯一、can_spawn 不悬空 /
// 不自环 / 无环、context_strategy 声明合法（W6-A：kind 非空且在枚举内））；
// **工作流级 context_strategy 声明合法**（见 validateWorkflowContextStrategy：kind 非空
// 且在枚举内——工作流级 context_strategy 是**必填**声明，越枚举 / 空 kind → 拒装载）。
func NewPackage(spec contracts.Workflow, milestones ...MilestoneHandler) (*Package, error) {
	id := strings.TrimSpace(spec.ID)
	if id == "" {
		return nil, fmt.Errorf("workflow: 能力包 id 不能为空")
	}
	if strings.TrimSpace(spec.Philosophy) == "" {
		return nil, fmt.Errorf("workflow: 能力包 %q 的 philosophy 不能为空（W3：哲学文本进 system prompt 供主 agent 选择）", id)
	}
	if err := validateAgentTypes(id, spec.AgentTypes); err != nil {
		return nil, err
	}
	if err := validateWorkflowContextStrategy(id, spec.ContextStrategy); err != nil {
		return nil, err
	}
	mh := make(map[string]MilestoneHandler, len(milestones))
	for _, h := range milestones {
		if h == nil {
			continue
		}
		name := strings.TrimSpace(h.Name())
		if name == "" {
			return nil, fmt.Errorf("workflow: 能力包 %q 的里程碑部件没有名字（派发语义必须唯一）", id)
		}
		if _, dup := mh[name]; dup {
			return nil, fmt.Errorf("workflow: 能力包 %q 有重名里程碑部件 %q（fail-close：同名派发语义必须唯一）", id, name)
		}
		mh[name] = h
	}
	spec.ID = id
	return &Package{spec: spec, milestones: mh}, nil
}

// validateAgentTypes 校验能力包声明的子代理类型（**不可信配置**，fail-close）。
//
// 能力包是纯数据、可被任何语言写/分发（W13）——它声明的 agent_types 会被人照着派。
// 下列每一条都对应一个"照着声明派会出事"的场景：
//
//  1. **id 非空**：空 id 的类型没法被引用（can_spawn 指不到、宿主也认不出是谁）；
//  2. **id 唯一**：重名 → "派哪个"不唯一（同名不同提示词 → 行为不可预期）；
//  3. **can_spawn 指向包内已声明的类型**：悬空引用 = 派不出来（声明了却永远用不上）；
//  4. **can_spawn 不自环**：自己派自己 = 无限派生；
//  5. **can_spawn 无环**：a→b→a 的环 = 派生时死循环（深度闸门只挡深度，挡不住环）；
//  6. **context_strategy 声明合法**（W6-A）：声明了（指针非 nil）就必须有 kind，且 kind 在
//     枚举内（append / stage_scoped / sliding_window / tiered，与工作流级
//     validateWorkflowContextStrategy **同一张枚举表**：agents.ValidContextStrategyKind）。
//
// 为什么在**装载期**拒（而不是等到运行期）：坏包可能正是用户指望生效的那一个——装载期
// fail-close（整个包拒，LoadDir 逐个点名文件），别让半坏的声明溜进运行期。
//
// 第 6 条的两种坏法都对应"策略语义悄悄变味"：
//   - **kind 为空**：声明了 context_strategy 却没说什么 → 运行期被当成"未声明"静默走
//     append（用户以为自己配了策略，其实没配）；
//   - **kind 不在枚举**：宿主无从装配（造不出工厂）→ 子 agent 派出去就炸（fail-close 收口）。
//     与其等到派出去才发现，不如装载期就拒。
//
// 口径：can_spawn 只能指向**本包内**声明的类型（能力包 = 自带能力池的闭包）。包外的
// 全局类型由宿主的类型注册表负责，不在本包的声明范围内。
func validateAgentTypes(pkgID string, types []contracts.AgentType) error {
	seen := make(map[string]bool, len(types))
	for _, at := range types {
		tid := strings.TrimSpace(at.ID)
		if tid == "" {
			return fmt.Errorf("workflow: 能力包 %q 的 agent_types 里有**空 id**（类型名必须非空：派发与引用都靠它）", pkgID)
		}
		if seen[tid] {
			return fmt.Errorf("workflow: 能力包 %q 的 agent_types 有**重名 id** %q（fail-close：同名类型的语义必须唯一）", pkgID, tid)
		}
		seen[tid] = true
	}
	for _, at := range types {
		tid := strings.TrimSpace(at.ID)
		for _, child := range at.CanSpawn {
			cid := strings.TrimSpace(child)
			if cid == "" {
				return fmt.Errorf("workflow: 能力包 %q 的 agent_type %q 的 can_spawn 里有**空 id**", pkgID, tid)
			}
			if cid == tid {
				return fmt.Errorf("workflow: 能力包 %q 的 agent_type %q 的 can_spawn **自环**（自己派自己 = 无限派生）", pkgID, tid)
			}
			if !seen[cid] {
				return fmt.Errorf("workflow: 能力包 %q 的 agent_type %q 的 can_spawn 指向**未声明**的类型 %q（悬空引用：派不出来）", pkgID, tid, cid)
			}
		}
	}
	// W6-A：context_strategy 声明了就得说清楚 kind（否则运行期静默走 append）。
	for _, at := range types {
		if at.ContextStrategy == nil {
			continue // 未声明 = append 默认（W6-A：不继承主 agent 的策略）
		}
		tid := strings.TrimSpace(at.ID)
		kind := at.ContextStrategy.Kind
		if strings.TrimSpace(string(kind)) == "" {
			return fmt.Errorf("workflow: 能力包 %q 的 agent_type %q 的 context_strategy **kind 为空**（声明了却没说什么 = 运行期会被静默当成 append）：fail-close", pkgID, tid)
		}
		if !agents.ValidContextStrategyKind(kind) {
			return fmt.Errorf("workflow: 能力包 %q 的 agent_type %q 的 context_strategy kind %q **不在枚举内**（append/stage_scoped/sliding_window/tiered）：fail-close", pkgID, tid, kind)
		}
	}
	if cyc := findAgentTypeCycle(types); cyc != "" {
		return fmt.Errorf("workflow: 能力包 %q 的 agent_types 存在**派生环**（%s）：派生会死循环", pkgID, cyc)
	}
	return nil
}

// validateWorkflowContextStrategy 校验**工作流级** context_strategy 声明（W6，fail-close）。
//
// 工作流级 context_strategy 是**必填**声明（契约 schema：required ["kind"]）——既然声明了，
// 就得说清楚 kind：
//
//   - **kind 为空** = 非法声明（"声明了却没说什么"）：运行期宿主无从装配 → 静默按 append
//     处理 = 策略语义悄悄变味（用户以为配了策略，其实跑的是追加式）；
//   - **kind 不在枚举内**（append / stage_scoped / sliding_window / tiered 之外）= 宿主
//     无从装配（造不出工厂）→ 与其等到运行期才发现，不如装载期就拒。
//
// 与 agent_type 级（validateAgentTypes 第 6 条）**同一张枚举表**（agents.
// ValidContextStrategyKind）：两处口径不许分叉——分叉就会出现"agent_type 拒 / 工作流级
// 收下"的两套口径（契约文案承诺的是"两处都受枚举约束"）。
//
// **装载期**拒（整个包拒，LoadDir 逐个点名文件），别让半坏的声明溜进运行期。
//
// 注意与**运行期**口径的分工（刻意不对称，不是 bug）：
//   - **装载期**（这里）：越枚举 / 空 kind → 拒装载（fail-close）；
//   - **运行期**：枚举内但宿主**未实现**的 kind → 工作流级按 append（宿主只实现 tiered，
//     其余退回默认追加式），agent_type 级则 fail-close（子 agent 以 agent.failed 收口）。
func validateWorkflowContextStrategy(pkgID string, cs contracts.ContextStrategy) error {
	kind := cs.Kind
	if strings.TrimSpace(string(kind)) == "" {
		return fmt.Errorf("workflow: 能力包 %q 的 context_strategy **kind 为空**（工作流级 context_strategy 是必填声明：声明了却没说什么 = 运行期会被静默当成 append）：fail-close", pkgID)
	}
	if !agents.ValidContextStrategyKind(kind) {
		return fmt.Errorf("workflow: 能力包 %q 的 context_strategy kind %q **不在枚举内**（append/stage_scoped/sliding_window/tiered）：fail-close", pkgID, kind)
	}
	return nil
}

// findAgentTypeCycle 找 agent_types 的 can_spawn 环（返回环路径；无环 = ""）。
//
// DFS + 三色标记（白=未访问 / 灰=在栈上 / 黑=已完成）：撞到灰色的就是环。
func findAgentTypeCycle(types []contracts.AgentType) string {
	children := map[string][]string{}
	for _, at := range types {
		children[strings.TrimSpace(at.ID)] = at.CanSpawn
	}
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var path []string
	var dfs func(id string) string
	dfs = func(id string) string {
		color[id] = gray
		path = append(path, id)
		for _, raw := range children[id] {
			cid := strings.TrimSpace(raw)
			switch color[cid] {
			case gray:
				// 环：从环起点截断路径打印。
				for i, p := range path {
					if p == cid {
						return strings.Join(path[i:], " -> ") + " -> " + cid
					}
				}
				return strings.Join(path, " -> ") + " -> " + cid
			case white:
				if cyc := dfs(cid); cyc != "" {
					return cyc
				}
			}
		}
		path = path[:len(path)-1]
		color[id] = black
		return ""
	}
	for _, at := range types {
		id := strings.TrimSpace(at.ID)
		if color[id] == white {
			if cyc := dfs(id); cyc != "" {
				return cyc
			}
		}
	}
	return ""
}

// ParsePackage 从 JSON 字节解析一个**纯数据**能力包（W13：跨边界契约，可被任何语言写）。
//
// 文件加载的能力包**不带里程碑部件**（部件是 Go 代码，无法从 JSON 来）——要挂部件，
// 用 NewPackage 显式附上（见 tower 包的内建注册）。milestones 参数供调用方按需附加。
func ParsePackage(data []byte, milestones ...MilestoneHandler) (*Package, error) {
	var spec contracts.Workflow
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("workflow: 解析能力包 JSON 失败: %w", err)
	}
	return NewPackage(spec, milestones...)
}

// ID 返回能力包 id。
func (p *Package) ID() string { return p.spec.ID }

// Command 返回触发用 slash 命令名（W3；空 = 显式触发，无 slash 命令）。
func (p *Package) Command() string {
	if p.spec.Command == nil {
		return ""
	}
	return strings.TrimSpace(*p.spec.Command)
}

// Spec 返回底层契约数据（只读；调用方不得改）。
func (p *Package) Spec() contracts.Workflow { return p.spec }

// ContextStrategyKind 返回本能力包声明的上下文策略（W6：上下文管理方式是工作流的一部分）。
// 宿主据此选 compaction.AssemblerFactory（"tiered" → 级联压缩；"" / "append" → 不挂）。
func (p *Package) ContextStrategyKind() contracts.ContextStrategyKind {
	return p.spec.ContextStrategy.Kind
}

// MilestoneHandlers 返回本能力包挂的里程碑部件（按名排序，确定性）。
func (p *Package) MilestoneHandlers() []MilestoneHandler {
	names := make([]string, 0, len(p.milestones))
	for n := range p.milestones {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]MilestoneHandler, 0, len(names))
	for _, n := range names {
		out = append(out, p.milestones[n])
	}
	return out
}

// AugmentSystem 把本能力包的哲学文本拼进基础 system prompt（W3）。
//
// 形态：`base + "\n\n" + 哲学`（base 为空 → 只有哲学）。哲学文本**供主 agent 选择**
// （W2/W3），不是强制指令——框架只注入，不校验 agent 是否遵循（W21）。返回新字符串，
// 不改动入参。
func (p *Package) AugmentSystem(base string) string {
	phil := strings.TrimSpace(p.spec.Philosophy)
	if phil == "" {
		return base
	}
	if strings.TrimSpace(base) == "" {
		return phil
	}
	return base + "\n\n" + phil
}

// HandleMilestone 把一次里程碑信号派发给名匹配的部件（W14）。
//
// 按 m.Kind 找 Name()==m.Kind 的部件；找不到 → 返回 (false, error)（调用方据此记
// "无部件处理该里程碑"，不静默当作通过）。派发到的部件的错误向上传播。
func (p *Package) HandleMilestone(ctx context.Context, m Milestone) (MilestoneResult, error) {
	h, ok := p.milestones[m.Kind]
	if !ok {
		return MilestoneResult{}, fmt.Errorf("workflow: 能力包 %q 没有处理里程碑 %q 的部件", p.spec.ID, m.Kind)
	}
	return h.Handle(ctx, m)
}

// HasMilestone 报告本能力包是否挂了某名里程碑部件。
func (p *Package) HasMilestone(name string) bool {
	_, ok := p.milestones[name]
	return ok
}

// ---------------------------------------------------------------------------
// W4：追加 loop 可组合
// ---------------------------------------------------------------------------

// Compose 把一个追加段并进基础能力包的契约数据（W4：追加 loop 可组合）。
//
// 例：review-critic 续解——基础 build 包 + 追加一个 review-critic 段（更多哲学 +
// 更多 agent_types）。产物是**新的 contracts.Workflow**（纯数据组合，不引入任何流程/
// 序列语义——组合只是把两段的能力声明并成一段）。
//
// 合并规则：
//   - philosophy：base + "\n\n" + extra（两段哲学并陈）；
//   - agent_types / gates / grants：追加（base 在前，按 name/id 去重，extra 补新增的）；
//   - **agent_types 同 id 时 base 整条胜出**，extra 同 id 类型**整条丢弃**（prompt /
//     can_spawn / context_strategy / share_project_assets 全丢；要覆盖请改 base 或换 id）——
//     W6-A 的显式声明不做字段级合并（不做"base 的 prompt + extra 的策略"这种拼装）：谁赢
//     必须是确定的，不能取决于装载顺序；
//   - context_strategy：extra 非 append 则用 extra 的（追加段可换策略），否则留 base 的；
//   - command / id：留 base 的（追加段不改触发入口）。
//
// # 产物与入参**完全解耦**（深拷贝）
//
// `out := base` 只拷了切片头（len/cap/底层数组指针）——后续 append 在 cap 有余量时会写进
// **base 的底层数组**（改到入参！），元素内部的切片（AgentType.Tools / Gate.Rules…）更是
// 直接共享。组合产物必须是**独立**的一份：调用方继续改 base（或 extra）不应影响已组合的
// 结果。故三张切片在合并前**逐元素深拷贝**；其余**指针/字节底字段**（Workflow.Command
// *string、工作流级 context_strategy.Params、Gate.Params 等 json.RawMessage）也一并拷——
// 别只盯着切片头：指针别名同样会让"改入参"改到产物。
func Compose(base, extra contracts.Workflow) contracts.Workflow {
	out := base
	out.Philosophy = joinPhilosophy(base.Philosophy, extra.Philosophy)

	// **深拷贝**三张切片（入参与产物解耦；见函数头）。
	out.AgentTypes = cloneAgentTypes(base.AgentTypes)
	out.Gates = cloneGates(base.Gates)
	out.Grants = cloneGrants(base.Grants)
	// 工作流级 context_strategy：结构头浅拷不够——Params 是 json.RawMessage（[]byte 底），
	// 浅拷共享**底层数组** → 产物与 base 串味。整份拷一份（Params 也拷底）。
	out.ContextStrategy = cloneContextStrategy(base.ContextStrategy)
	// command 是 *string：`out := base` 只拷了指针 → 产物与 base 指向**同一个字符串**，
	// 调用方改 base.Command 的指向/内容会改到产物。拷一份指针（nil 保 nil）。
	out.Command = cloneStringPtr(base.Command)

	// agent_types 按 id 去重追加。
	seenType := map[string]bool{}
	for _, at := range out.AgentTypes {
		seenType[at.ID] = true
	}
	for _, at := range extra.AgentTypes {
		if seenType[at.ID] {
			continue
		}
		seenType[at.ID] = true
		out.AgentTypes = append(out.AgentTypes, cloneAgentType(at))
	}
	// gates 按 id 去重追加。
	seenGate := map[string]bool{}
	for _, g := range out.Gates {
		seenGate[g.ID] = true
	}
	for _, g := range extra.Gates {
		if seenGate[g.ID] {
			continue
		}
		seenGate[g.ID] = true
		out.Gates = append(out.Gates, cloneGate(g))
	}
	// grants 按 (tool,scope) 去重追加。
	seenGrant := map[string]bool{}
	for _, gr := range out.Grants {
		seenGrant[gr.Tool+"\x00"+gr.Scope] = true
	}
	for _, gr := range extra.Grants {
		k := gr.Tool + "\x00" + gr.Scope
		if seenGrant[k] {
			continue
		}
		seenGrant[k] = true
		out.Grants = append(out.Grants, contracts.Grant{Tool: gr.Tool, Scope: gr.Scope})
	}
	// context_strategy：追加段显式要了非 append 策略 → 用它（W4：追加段可换上下文管理方式）。
	if extra.ContextStrategy.Kind != "" && extra.ContextStrategy.Kind != contracts.ContextStrategyKindAppend {
		out.ContextStrategy = cloneContextStrategy(extra.ContextStrategy)
	}
	return out
}

// cloneAgentTypes 深拷贝 agent_types 列表（nil → nil；元素内的切片/指针也拷）。
func cloneAgentTypes(in []contracts.AgentType) []contracts.AgentType {
	if in == nil {
		return nil
	}
	out := make([]contracts.AgentType, 0, len(in))
	for _, at := range in {
		out = append(out, cloneAgentType(at))
	}
	return out
}

// cloneAgentType 深拷贝一个 agent_type（Tools / CanSpawn 切片 + permission_request /
// context_strategy / share_project_assets 指针；W6-A 新字段的 Params 是 []byte 底，
// 浅拷同样共享，一并拷）。
func cloneAgentType(at contracts.AgentType) contracts.AgentType {
	cp := at
	cp.Tools = cloneStrings(at.Tools)
	cp.CanSpawn = cloneStrings(at.CanSpawn)
	if at.PermissionRequest != nil {
		pr := *at.PermissionRequest
		cp.PermissionRequest = &pr
	}
	if at.ContextStrategy != nil {
		cs := *at.ContextStrategy
		cs.Params = cloneRawMessage(at.ContextStrategy.Params)
		cp.ContextStrategy = &cs
	}
	if at.ShareProjectAssets != nil {
		v := *at.ShareProjectAssets
		cp.ShareProjectAssets = &v
	}
	return cp
}

// cloneContextStrategy 深拷贝一份 context_strategy（结构头 + Params 底层数组）。
//
// 为什么结构头也要拷：ContextStrategy 里除 Params 外都是值字段，浅拷本身安全；但
// **Params 是 json.RawMessage（[]byte 底）**——浅拷只拷切片头，底层数组仍与入参共享
// → 调用方改入参会**改到产物**（组合产物必须与入参完全解耦，见 Compose 函数头）。
// nil 保 nil（不把"没配参数"变成"配了空参数"）。
func cloneContextStrategy(cs contracts.ContextStrategy) contracts.ContextStrategy {
	cp := cs
	cp.Params = cloneRawMessage(cs.Params)
	return cp
}

// cloneRawMessage 复制一段原始 JSON（nil → nil；非 nil → 新底层数组）。
func cloneRawMessage(in json.RawMessage) json.RawMessage {
	if in == nil {
		return nil
	}
	out := make(json.RawMessage, len(in))
	copy(out, in)
	return out
}

// cloneStringPtr 复制一个 *string（nil → nil；非 nil → 新指针）。
//
// 为什么需要：Workflow.Command 是 *string——`out := base` 只拷指针，产物与入参指向同一
// 个字符串（调用方改 base.Command 会改到产物，见 Compose 函数头的解耦承诺）。
func cloneStringPtr(in *string) *string {
	if in == nil {
		return nil
	}
	v := *in
	return &v
}

// cloneGates 深拷贝 gates 列表（nil → nil；Rules 切片与其中的指针字段也拷）。
func cloneGates(in []contracts.Gate) []contracts.Gate {
	if in == nil {
		return nil
	}
	out := make([]contracts.Gate, 0, len(in))
	for _, g := range in {
		out = append(out, cloneGate(g))
	}
	return out
}

// cloneGate 深拷贝一个 gate（Rules 切片 + component 指针 + params 底层数组）。
func cloneGate(g contracts.Gate) contracts.Gate {
	cp := g
	cp.Rules = clonePolicyRules(g.Rules)
	if g.Component != nil {
		c := *g.Component
		cp.Component = &c
	}
	// params 是 json.RawMessage（[]byte 底）——浅拷只拷切片头，底层数组仍与入参共享
	// → 调用方改入参会**改到产物**（组合产物必须与入参完全解耦，见 Compose 函数头）。
	cp.Params = cloneRawMessage(g.Params)
	return cp
}

// clonePolicyRules 深拷贝策略规则列表（规则里的 *string 指针也拷——别名同样会漏改）。
func clonePolicyRules(in []contracts.PolicyRule) []contracts.PolicyRule {
	if in == nil {
		return nil
	}
	out := make([]contracts.PolicyRule, 0, len(in))
	for _, r := range in {
		cp := r
		if r.ID != nil {
			v := *r.ID
			cp.ID = &v
		}
		if r.ArgsPattern != nil {
			v := *r.ArgsPattern
			cp.ArgsPattern = &v
		}
		if r.Reason != nil {
			v := *r.Reason
			cp.Reason = &v
		}
		out = append(out, cp)
	}
	return out
}

// cloneGrants 深拷贝 grants 列表（nil → nil；元素是纯值）。
func cloneGrants(in []contracts.Grant) []contracts.Grant {
	if in == nil {
		return nil
	}
	out := make([]contracts.Grant, 0, len(in))
	out = append(out, in...)
	return out
}

// cloneStrings 复制字符串切片（nil → nil）。
func cloneStrings(s []string) []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s...)
}

// joinPhilosophy 把两段哲学并陈（都非空才用分隔符；任一只留那一段）。
func joinPhilosophy(a, b string) string {
	as, bs := strings.TrimSpace(a), strings.TrimSpace(b)
	switch {
	case as == "":
		return bs
	case bs == "":
		return as
	default:
		return as + "\n\n" + bs
	}
}

// ---------------------------------------------------------------------------
// Registry：能力包的注册与查找
// ---------------------------------------------------------------------------

// Registry 持有已装载的工作流能力包（按 id 与 slash 命令索引）。
//
// 并发安全。宿主持一个（内建 tower + 目录加载的可选包都在里面）；session 激活时按
// id 或命令查。**Registry 不持有"哪个 session 激活了哪个包"的状态**——那是 session 级
// 的事（W12：归属 session，不是 core 全局状态），由宿主另存。
type Registry struct {
	mu    sync.RWMutex
	byID  map[string]*Package
	byCmd map[string]*Package
}

// NewRegistry 建一个空注册表。
func NewRegistry() *Registry {
	return &Registry{byID: map[string]*Package{}, byCmd: map[string]*Package{}}
}

// Register 登记一个能力包。
//
// fail-close：id 重名 → 拒（不许静默遮蔽）；command 非空且与已登记命令撞 → 拒
// （slash 触发必须唯一，否则"/tower"指向两个包）。command 为空的包只按 id 索引。
func (r *Registry) Register(p *Package) error {
	if p == nil {
		return fmt.Errorf("workflow: 不能登记 nil 能力包")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.byID[p.ID()]; dup {
		return fmt.Errorf("workflow: 能力包 id %q 已登记（重名 fail-close）", p.ID())
	}
	cmd := p.Command()
	if cmd != "" {
		if other, dup := r.byCmd[cmd]; dup {
			return fmt.Errorf("workflow: slash 命令 %q 已被能力包 %q 占用（触发必须唯一）", cmd, other.ID())
		}
	}
	r.byID[p.ID()] = p
	if cmd != "" {
		r.byCmd[cmd] = p
	}
	return nil
}

// Get 按 id 取能力包。
func (r *Registry) Get(id string) (*Package, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.byID[id]
	return p, ok
}

// ByCommand 按 slash 命令名取能力包（W3 触发）。
func (r *Registry) ByCommand(cmd string) (*Package, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.byCmd[strings.TrimSpace(cmd)]
	return p, ok
}

// List 返回所有已登记能力包（按 id 排序，确定性）。
func (r *Registry) List() []*Package {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Package, 0, len(r.byID))
	for _, p := range r.byID {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out
}
