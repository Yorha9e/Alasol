package host

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/workflow"
	"github.com/Yorha9e/alasol/core/workflow/tower"
)

// 本文件把 v3-S3 工作流**能力包**接进 hub（W1/W3/W14/W19/W21）。
//
// # 形态：能力包注册表 + per-session 激活（W12：归属 session，不是 core 全局状态）
//
// hub 持一个 workflow.Registry（内建 tower + Config.WorkflowDir 目录加载的纯数据包）。
// session 可**激活**一个能力包（ActivateWorkflow / slash /workflow）——激活只做一件事：
// 把能力包的哲学文本拼进该 session loop 的 system prompt（W3，供主 agent 选择）。
//
// # 非强制（W1/W2/W21）——最容易做歪的地方
//
// 这不是流程引擎：没有阶段状态机、不校验编排序列。能力包只提供原料（哲学 / agent_types /
// context_strategy / gates / grants / 里程碑部件），**主 agent 自决**怎么用。激活不改变
// 任何控制流，只改 system prompt（+ 可选地按工作流声明的 context_strategy 换装配策略）。
//
// # W14：两个代码扩展点
//
//  1. **MilestoneHandler**（里程碑部件）——TriggerMilestone 派发；tower 的 validation.passed
//     是首个实现（critic→reviewer 收敛点）。
//  2. **自定义上下文策略**——激活的工作流声明的 context_strategy.kind 决定挂不挂 tiered
//     （经 contextStrategyFactory，loop.Config.ContextStrategy 注入）。

// ---------------------------------------------------------------------------
// R8 / H22 / H23 口径（实现已在别处，这里只落注释——不造重复机制）
// ---------------------------------------------------------------------------
//
// 这三条是权限/组合的既有不变式（v3-S6-B 装配多 agent 时**沿用**，不另起一套）：
//
//   - **R8（多 workflow 包 Compose 取并集、权限只收窄）**：core/workflow.Compose 把多个能力包
//     的 philosophy / agent_types / gates / grants 并成一段（追加 loop 可组合，W4）。合并后的
//     权限**只收窄不放宽**：gates 的 deny/ask 规则跨包取并集，任何一包的收紧都保留。宿主装配
//     multi agent 时不改这条——类型注册表同理（同名覆盖只发生在"内建 vs TOML 自定义"这一处，
//     且是用户显式优先级，不是能力包合并）。
//   - **H22（grants 是请求，只收窄不放宽）**：能力包的 grants 是**预批准请求**，不是授予——
//     实际权限 = 请求 ∩ 策略上限 ∩ 父级授予（agent 树闸门③同款：子 ⊆ 父 ∩ ceiling）。框架
//     **不因包里声明了 grant 就授予**。子 agent 的 PermissionRequest 是上限，被父级/策略收窄。
//   - **H23（allow 命中即放行）**：策略链上一条 allow 命中即放行该调用（求值序 deny → ask →
//     allow；deny 永远赢）。MultiAgent 开时 hub 自动补的 spawn/complete allow 规则同理——它只是
//     "补上缺失的许可让框架工具可见"，任何显式 deny 仍可拦（P2-4）。

// buildWorkflowRegistry 建工作流注册表：内建 tower + Config.WorkflowDir 目录加载的纯数据包。
//
// 内建包**先登记**（占 id/命令），目录包后登记；任一重名 → 装配失败（fail-close：不许静默
// 遮蔽）。目录为空 / 不存在 → 只登记内建包（合法状态）。
func buildWorkflowRegistry(dir string) (*workflow.Registry, error) {
	towerPkg, err := tower.Package()
	if err != nil {
		return nil, fmt.Errorf("host: 构造内建 tower 能力包: %w", err)
	}
	return workflow.LoadRegistry(dir, towerPkg)
}

// ActivateWorkflow 激活一个工作流能力包到某 session（W3，**显式触发**）。
//
// 做三件事：
//  1. 记到 activeWorkflow（W12：归属 session）；
//  2. **失效已建的 loop**（system prompt 要变；loop 无状态，历史由事件流带，重建零损失）；
//  3. 发射 workflow.activated durable 事件（W3 触发审计；记注入的哲学原文）。
//
// 之后该 session 的 loopFor 会把能力包哲学拼进 system prompt，并按工作流声明的
// context_strategy 选装配策略。未登记的 id → 错误（fail-close：不静默忽略）。
//
// **显式触发的事件里 command 字段为空**（载荷语义："缺省 = 显式触发"）——slash 触发
// 走 ActivateWorkflowByCommand（那里才记命令名）。
func (h *Hub) ActivateWorkflow(ctx context.Context, sessionID contracts.Ulid, id string) (*workflow.Package, error) {
	return h.activateWorkflow(ctx, sessionID, id, "")
}

// ActivateWorkflowByCommand 按 **slash 命令**激活能力包（W3 的 slash 触发入口）。
//
// 能力包自己声明 command（如 tower 的 "tower"）——这里是它的**生产调用方**：声明出来的
// 命令不是"没人用的字段"，而是**真能敲出来的入口**（chat / TUI 都接了它，见
// WorkflowCommandTrigger）。没登记的命令 → 错误（fail-close）。
//
// 事件的 command 字段记**实际触发的命令名**（slash 触发才记；显式触发留空——载荷语义：
// "缺省 = 显式触发"）。
func (h *Hub) ActivateWorkflowByCommand(ctx context.Context, sessionID contracts.Ulid, cmd string) (*workflow.Package, error) {
	if h == nil {
		return nil, fmt.Errorf("host: Hub 为 nil")
	}
	p, ok := h.workflows.ByCommand(cmd)
	if !ok {
		return nil, fmt.Errorf("host: 没有命令为 %q 的工作流能力包（可用：%v）", cmd, h.workflowIDs())
	}
	return h.activateWorkflow(ctx, sessionID, p.ID(), p.Command())
}

// WorkflowCommandTrigger 报告一行输入是不是**能力包声明的 slash 触发**（如 "/tower"）。
//
// 返回 (cmd, true) = 是（cmd = 去掉斜杠的命令名）；false = 不是（调用方按普通输入处理）。
//
// 口径：命令名**不含空白**（"/tower x" 不是触发，是普通输入）；命令必须**已登记**
// （未登记的命令名不劫持普通输入——否则用户敲 "/todo" 会被当成工作流触发）。
func (h *Hub) WorkflowCommandTrigger(line string) (string, bool) {
	if h == nil {
		return "", false
	}
	s := strings.TrimSpace(line)
	if !strings.HasPrefix(s, "/") || len(s) < 2 {
		return "", false
	}
	cmd := strings.TrimPrefix(s, "/")
	if strings.ContainsAny(cmd, " \t\r\n") {
		return "", false
	}
	if _, ok := h.workflows.ByCommand(cmd); !ok {
		return "", false
	}
	return cmd, true
}

// activateWorkflow 是激活的共同实现（trigger = 实际触发的 slash 命令名；空 = 显式触发）。
func (h *Hub) activateWorkflow(ctx context.Context, sessionID contracts.Ulid, id, trigger string) (*workflow.Package, error) {
	if h == nil {
		return nil, fmt.Errorf("host: Hub 为 nil")
	}
	if err := h.ensureOpen(); err != nil {
		return nil, err
	}
	p, ok := h.workflows.Get(id)
	if !ok {
		return nil, fmt.Errorf("host: 工作流 %q 未登记（可用：%v）", id, h.workflowIDs())
	}
	h.mu.Lock()
	h.activeWorkflow[sessionID] = p
	delete(h.loops, sessionID) // system prompt 要变 → 失效缓存的 loop（下次 RunPrompt 重建）
	h.mu.Unlock()

	// 发射 workflow.activated（审计：记注入了什么哲学 + 实际触发方式）。
	agentID := h.resolveMainAgent(ctx, sessionID)
	ev := contracts.Event{
		V:         1,
		ID:        core.NewUlid(),
		Kind:      workflow.KindActivated,
		TS:        time.Now().UTC().Format(time.RFC3339),
		SessionID: sessionID,
		AgentID:   agentID,
		Durable:   true,
		Payload:   mustMarshal(workflow.ActivatedEvent(p.ID(), trigger, p.Spec().Philosophy)),
	}
	if err := h.bus.Publish(ctx, ev); err != nil {
		// 激活本身已生效（session 级状态已记）；事件落不下只留痕，不推翻激活。
		hubLog.Warn("host: 发射 workflow.activated 失败（激活仍生效）", "session_id", string(sessionID), "err", err.Error())
	}
	return p, nil
}

// DeactivateWorkflow 取消某 session 的工作流激活（回到默认 system prompt；失效 loop）。
func (h *Hub) DeactivateWorkflow(sessionID contracts.Ulid) {
	if h == nil {
		return
	}
	h.mu.Lock()
	delete(h.activeWorkflow, sessionID)
	delete(h.loops, sessionID)
	h.mu.Unlock()
}

// ActiveWorkflow 返回某 session 当前激活的能力包（nil = 未激活）。
func (h *Hub) ActiveWorkflow(sessionID contracts.Ulid) *workflow.Package {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.activeWorkflow[sessionID]
}

// Workflows 返回工作流注册表（UI/CLI 列可用能力包）。
func (h *Hub) Workflows() *workflow.Registry {
	if h == nil {
		return nil
	}
	return h.workflows
}

// TriggerMilestone 触发某 session 激活工作流的一个里程碑（W14 MilestoneHandler）。
//
// 派发到激活能力包里 Name()==kind 的部件；无激活包或无对应部件 → 错误（不静默当通过）。
// 无论通过与否都发射 workflow.milestone durable 事件（审计 + 消费端）。返回部件的结论
// （passed / note）——调用方（如合并门禁）可据 passed 联动阶段硬边界。
func (h *Hub) TriggerMilestone(ctx context.Context, sessionID contracts.Ulid, kind string, data json.RawMessage) (workflow.MilestoneResult, error) {
	if h == nil {
		return workflow.MilestoneResult{}, fmt.Errorf("host: Hub 为 nil")
	}
	if err := h.ensureOpen(); err != nil {
		return workflow.MilestoneResult{}, err
	}
	p := h.ActiveWorkflow(sessionID)
	if p == nil {
		return workflow.MilestoneResult{}, fmt.Errorf("host: session %s 未激活任何工作流（无从触发里程碑）", sessionID)
	}
	agentID := h.resolveMainAgent(ctx, sessionID)
	res, herr := p.HandleMilestone(ctx, workflow.Milestone{
		WorkflowID: p.ID(),
		SessionID:  sessionID,
		AgentID:    agentID,
		Kind:       kind,
		Data:       data,
	})
	note := res.Note
	if herr != nil {
		note = herr.Error() // 部件失败也留痕（note 记失败原因；passed 保持 false）
	}
	// 发射 workflow.milestone（审计 + 消费端就位）。
	ev := contracts.Event{
		V:         1,
		ID:        core.NewUlid(),
		Kind:      workflow.KindMilestone,
		TS:        time.Now().UTC().Format(time.RFC3339),
		SessionID: sessionID,
		AgentID:   agentID,
		Durable:   true,
		Payload:   mustMarshal(workflow.MilestoneEvent(p.ID(), kind, res.Passed, note)),
	}
	if perr := h.bus.Publish(ctx, ev); perr != nil {
		hubLog.Warn("host: 发射 workflow.milestone 失败", "session_id", string(sessionID), "err", perr.Error())
	}
	return res, herr
}

// resolveMainAgent 取（或造）某 session 的主 agent id（事件 envelope 用）。
//
// 口径与 loopFor 同源：已登记 → 用登记的；否则 disc.PrimaryAgent；再没有 → 新造一个
// （激活/里程碑先于首次 Run 时，session 事件流里还没有主 agent）。
func (h *Hub) resolveMainAgent(ctx context.Context, sessionID contracts.Ulid) contracts.Ulid {
	h.mu.Lock()
	if a, ok := h.mainAgents[sessionID]; ok {
		h.mu.Unlock()
		return a
	}
	h.mu.Unlock()
	if a, ok := h.disc.PrimaryAgent(sessionID); ok {
		return a
	}
	// 新 session：先扫一遍事件流（可能原属另一 hub 进程），再兜底新造。
	if _, err := h.discFeed.catchUp(ctx); err == nil {
		if a, ok := h.disc.PrimaryAgent(sessionID); ok {
			return a
		}
	}
	return core.NewUlid()
}

// workflowIDs 返回已登记能力包的 id 列表（错误信息里列可用项）。
func (h *Hub) workflowIDs() []string {
	var ids []string
	for _, p := range h.workflows.List() {
		ids = append(ids, p.ID())
	}
	return ids
}

// WorkflowCommand 是 chat/TUI 里激活工作流的 slash 命令名（W3 触发）。
const WorkflowCommand = "workflow"

// ParseWorkflowCommand 解析 `/workflow [<id>|off]`（W3 触发；与 ParseBtwCommand 同款口径：
// **空格与 tab 都算分隔符**，"/workflowxyz" 不当命令按普通 prompt 走）。
//
// 返回 (arg, isWorkflow)：arg 为空 = 列出可用包；arg="off" = 停用；其余 = 要激活的 id。
func ParseWorkflowCommand(line string) (arg string, isWorkflow bool) {
	s := strings.TrimSpace(line)
	if s == "/"+WorkflowCommand {
		return "", true
	}
	if !strings.HasPrefix(s, "/"+WorkflowCommand) {
		return "", false
	}
	rest := s[len("/"+WorkflowCommand):]
	if rest == "" {
		return "", true
	}
	r, _ := utf8.DecodeRuneInString(rest)
	if !unicode.IsSpace(r) {
		return "", false // "/workflowxyz" 不当命令
	}
	return strings.TrimSpace(rest), true
}

// mustMarshal 编 JSON（契约载荷都是可序列化的纯数据；失败即编程错误，panic 让装配期暴露）。
func mustMarshal(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("host: 编工作流事件载荷失败: %v", err))
	}
	return raw
}

// workflowSystemFor 返回某 session 生效的 system prompt（W3：激活的能力包哲学拼进去）。
// 调用方须持 h.mu（读 activeWorkflow）。
func (h *Hub) workflowSystemForLocked(sessionID contracts.Ulid) string {
	system := h.cfg.System
	if p := h.activeWorkflow[sessionID]; p != nil {
		system = p.AugmentSystem(system)
	}
	return system
}

// workflowStrategyKindForLocked 返回某 session 生效的上下文策略 kind（W6：激活的能力包
// 声明的 context_strategy 覆盖宿主默认；未声明非 append 则留宿主默认）。调用方须持 h.mu。
func (h *Hub) workflowStrategyKindForLocked(sessionID contracts.Ulid) string {
	kind := h.cfg.ContextStrategy
	if p := h.activeWorkflow[sessionID]; p != nil {
		if k := string(p.ContextStrategyKind()); k != "" && k != string(contracts.ContextStrategyKindAppend) {
			kind = k // 工作流显式要了非 append 策略 → 尊重它（W6）
		}
	}
	return kind
}
