// Package tower 是**首个工作流能力包**（W19：tower 作首个验证用例）。
//
// # 定位（07 V9 + W19 口径）
//
// tower 源自 moamcp 的 tower 协议形态（mission/worker/reviewer/merge gate）。按 07 V9
// "**只搬机制不搬代码**"：我们**没有** mission / deps / 独立 reviewer（对应物是 agent 树 +
// 协调人），所以**不搬**那条链；只把"构建类的 plan→coder→critic→review 哲学"与
// "critic→reviewer 循环收敛 = 验证通过点"这个**机制**做成我们自己的能力包。
//
// 产物 = 我们自己的工作流包（core/workflow.Package），不是 tower 代码的移植。
//
// # W1/W2：这是能力包，不是流程
//
// 下面的"阶段"（plan/coder/critic/review）是**设计哲学文本**，注入 system prompt 供主
// agent 选择（W3）——框架**不强制**这个序列、不校验编排（W2/W21）。主 agent 自决开几个
// subagent、用哪个 agent_type、并行与否。
//
// # W14：代码扩展点只有两处
//
//  1. 本包挂一个 **MilestoneHandler**（"validation.passed"）——critic→reviewer 循环的
//     验证通过点。passed=true 时宿主可联动阶段硬边界（v2-S1 MarkValidationPassed）。
//  2. 上下文策略：本包声明 `context_strategy.kind = "tiered"`（W6）——宿主据此挂
//     core/compaction 的级联压缩工厂。
package tower

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/workflow"
)

// TowerID / TowerCommand 是 tower 能力包的 id 与 slash 触发命令（W3）。
const (
	TowerID      = "tower"
	TowerCommand = "tower"
)

// MilestoneValidationPassed 是 tower 的验证通过点里程碑名（W14 MilestoneHandler.Name）。
const MilestoneValidationPassed = "validation.passed"

// Philosophy 是 tower 的工作哲学文本（W2/W3：注入 system prompt 供主 agent 选择，
// 非强制序列）。它描述 plan→coder→critic→review 的**设计哲学**，不规定必须派哪些
// subagent——那是主 agent 自决（W21）。
//
// # 诚实口径（p3 修）：角色名**不是**框架内置的子代理类型
//
// 本包**声明**了 planner/coder/critic/reviewer 四个 agent_type，但框架**不消费**工作流的
// agent_types（没有"按工作流类型派子代理"的接线）——它们目前只是**数据**。若哲学文本
// 直接写"派一个 coder"，就是在暗示一个**不存在**的入口。故末段明说：这些角色**由协调人
// （你）提供**——自己分饰四角，或用你**已有**的子代理能力去扮演。
const Philosophy = `构建类工作哲学（tower）：把一个任务当成"方案→实现→挑刺→复核"的循环，而不是一条直线。

- plan：先想清楚要做什么、边界在哪、怎么算完成。方案没定之前不急着写代码。
- coder：按方案实现，改动尽量小、可核验；每步留下可回看的痕迹（事件流天然记录）。
- critic：实现后主动挑刺——边界条件、回归、与既有约定的冲突。挑刺是循环不是终点。
- review：critic 收敛后再复核一遍，确认"确实完成"而非"看起来完成"。

这些阶段是**设计哲学，不是强制序列**：你可以跳过、可以回头、可以并行开多个 coder、
可以就一个问题再跑一轮 critic→review。框架不校验你走了哪条路——你自己判断。

关于 plan/coder/critic/review 这四个角色：它们是本能力包**声明**的角色名，**不是**
框架内置的子代理类型（框架不会按名字替你派代理）。这些角色**由协调人（你）提供**：
要么你一个人分饰四角（换个视角逐条过），要么用你手上**已有**的子代理能力去扮演它们。`

// validationMilestone 是 tower 的"验证通过点"里程碑部件（W14 扩展点之一）。
//
// 语义：critic→reviewer 循环**收敛** = 验证通过点达成。它读里程碑数据里的 critic 与
// reviewer 结论，两者都过才算 passed（fail-close：任一不过 → 不通过，绝不因"看起来
// 差不多"就放行）。passed=true 时宿主可联动阶段硬边界（v2-S1）。
//
// **框架不强制序列**（W2/W21）：本部件只在被触发时给结论，不反过来驱动主 agent 走流程。
type validationMilestone struct{}

// Name 实现 workflow.MilestoneHandler。
func (validationMilestone) Name() string { return MilestoneValidationPassed }

// ValidationInput 是 validation.passed 里程碑的结构化输入（开放形状，由触发方喂 m.Data）。
type ValidationInput struct {
	// CriticClean critic 是否收敛（true = 没有未解决的挑刺）。
	CriticClean bool `json:"critic_clean"`
	// ReviewerVerdict reviewer 结论："approved" / "changes_requested"。
	ReviewerVerdict string `json:"reviewer_verdict"`
	// Note 可选的人读说明。
	Note string `json:"note"`
}

// Handle 实现 workflow.MilestoneHandler：critic clean 且 reviewer approved → passed。
func (validationMilestone) Handle(_ context.Context, m workflow.Milestone) (workflow.MilestoneResult, error) {
	var in ValidationInput
	if len(m.Data) > 0 {
		if err := json.Unmarshal(m.Data, &in); err != nil {
			return workflow.MilestoneResult{}, fmt.Errorf("tower: validation.passed 里程碑数据解不开: %w（应形如 {\"critic_clean\":true,\"reviewer_verdict\":\"approved\"}）", err)
		}
	}
	verdict := strings.ToLower(strings.TrimSpace(in.ReviewerVerdict))
	passed := in.CriticClean && verdict == string(contracts.ReviewConclusionApproved)
	note := in.Note
	if note == "" {
		if passed {
			note = "critic clean 且 reviewer approved：验证通过点达成"
		} else {
			note = fmt.Sprintf("验证未通过：critic_clean=%v reviewer_verdict=%q", in.CriticClean, verdict)
		}
	}
	return workflow.MilestoneResult{Passed: passed, Note: note}, nil
}

// agentTypes 是 tower 能力池里的子代理类型声明（W5：用户可自定义；工具名是**声明**，
// 实际可见性由宿主 registry + 策略链 + agent 类型白名单共同决定，R5）。
//
// 这些是 plan-coder-critic-review 哲学的**候选角色**，不是强制序列（W2/W21）——主 agent
// 自决派不派、派哪个。
//
// **诚实口径（p3 修）**：框架**不消费**工作流的 agent_types（没有"按工作流类型派子代理"
// 的接线）——这份声明目前只是**数据**（会被装载期校验：id 唯一、can_spawn 不悬空/不
// 自环/无环）。哲学文本因此明说"这些角色由协调人提供"，不暗示不存在的派发入口。
func agentTypes() []contracts.AgentType {
	return []contracts.AgentType{
		{
			ID:       "planner",
			Prompt:   "你是 planner：把一个任务拆成可验证的方案与边界，明确'怎么算完成'。不写实现代码。",
			Tools:    []string{"read", "recall"},
			Model:    "default",
			CanSpawn: nil, // planner 不再派（叶子）
		},
		{
			ID:       "coder",
			Prompt:   "你是 coder：按方案做最小、可核验的实现，每步留下可回看的痕迹。",
			Tools:    []string{"read", "write", "bash", "recall"},
			Model:    "default",
			CanSpawn: nil,
		},
		{
			ID:       "critic",
			Prompt:   "你是 critic：对实现挑刺——边界条件、回归、与既有约定的冲突。只读不改；挑刺是循环不是终点。",
			Tools:    []string{"read", "recall"},
			Model:    "default",
			CanSpawn: nil,
		},
		{
			ID:       "reviewer",
			Prompt:   "你是 reviewer：critic 收敛后复核'确实完成'而非'看起来完成'，给出 approved / changes_requested 结论。",
			Tools:    []string{"read", "recall"},
			Model:    "default",
			CanSpawn: nil,
		},
	}
}

// Package 构造 tower 能力包（内建注册用；带 validation.passed 里程碑部件）。
//
// context_strategy.kind = "tiered"（W6：长程构建类工作流用级联压缩 + recall 回捞）。
func Package() (*workflow.Package, error) {
	cmd := TowerCommand
	return workflow.NewPackage(contracts.Workflow{
		V:          1,
		ID:         TowerID,
		Command:    &cmd,
		Philosophy: Philosophy,
		AgentTypes: agentTypes(),
		ContextStrategy: contracts.ContextStrategy{
			Kind: contracts.ContextStrategyKindTiered,
		},
		// gates / grants：tower 的能力包形态先留空（预批准是数据，按需在文件形态里补；
		// H22：实际 = 请求 ∩ 策略上限 ∩ 父级授予，框架不因包里声明就授予）。
	}, validationMilestone{})
}

// ReviewCriticSegment 返回一个"追加 loop"段（W4：追加 loop 可组合）。
//
// 例：主任务跑完一轮后，就发现的问题**再挂一个 review-critic 段**继续解决。它是纯数据
// （contracts.Workflow），经 workflow.Compose 并进基础包——不引入任何流程语义。
func ReviewCriticSegment() contracts.Workflow {
	return contracts.Workflow{
		V:          1,
		ID:         TowerID + "-review-critic",
		Philosophy: "追加 loop（review-critic）：就上一轮 review 发现的问题再跑一轮 critic→review，直到收敛。",
		AgentTypes: []contracts.AgentType{
			{
				ID:       "critic",
				Prompt:   "你是 critic（追加轮）：聚焦上一轮 review 列出的问题逐条核验是否已解决。",
				Tools:    []string{"read", "recall"},
				Model:    "default",
				CanSpawn: nil,
			},
		},
		ContextStrategy: contracts.ContextStrategy{Kind: contracts.ContextStrategyKindTiered},
	}
}

// PackageJSON 返回 tower 能力包的**纯数据** JSON 形态（W13：可被任何语言写/组合/追加，
// 也可落盘成文件由 workflow.LoadDir 加载）。不含里程碑部件（部件是 Go 代码）。
func PackageJSON() ([]byte, error) {
	cmd := TowerCommand
	return json.Marshal(contracts.Workflow{
		V:          1,
		ID:         TowerID,
		Command:    &cmd,
		Philosophy: Philosophy,
		AgentTypes: agentTypes(),
		ContextStrategy: contracts.ContextStrategy{
			Kind: contracts.ContextStrategyKindTiered,
		},
	})
}
