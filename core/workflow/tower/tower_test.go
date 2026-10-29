package tower

import (
	"context"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/workflow"
)

// tower 包能构造（W19：首个工作流能力包），带 validation.passed 里程碑部件 + tiered 策略。
func TestTowerPackage(t *testing.T) {
	p, err := Package()
	if err != nil {
		t.Fatal(err)
	}
	if p.ID() != TowerID || p.Command() != TowerCommand {
		t.Errorf("id/command 错: %q %q", p.ID(), p.Command())
	}
	if p.ContextStrategyKind() != contracts.ContextStrategyKindTiered {
		t.Errorf("tower 应声明 tiered 策略，得到 %q", p.ContextStrategyKind())
	}
	if !p.HasMilestone(MilestoneValidationPassed) {
		t.Error("tower 应挂 validation.passed 里程碑部件")
	}
	// W3：哲学进 system prompt。
	if got := p.AugmentSystem("BASE"); got == "BASE" {
		t.Error("AugmentSystem 应追加哲学文本")
	}
}

// TestTowerPhilosophyIsHonestAboutRoles（p3 回归）：哲学文本**不许暗示不存在的入口**。
//
// 背景：框架**不消费**工作流的 agent_types（没有"按工作流类型派子代理"的接线）——
// planner/coder/critic/reviewer 只是**声明**。若哲学文本写"派一个 coder"，主 agent 会去找
// 一个根本不存在的能力。故末段必须明说"这些角色由协调人（你）提供"。
func TestTowerPhilosophyIsHonestAboutRoles(t *testing.T) {
	if !strings.Contains(Philosophy, "由协调人") {
		t.Errorf("哲学文本应明说这些角色由协调人提供（不许暗示不存在的派发入口）:\n%s", Philosophy)
	}
	// 仍要保留"非强制序列"的哲学口径（W2/W21）。
	if !strings.Contains(Philosophy, "不是强制序列") {
		t.Errorf("哲学文本应保留'不是强制序列'的口径:\n%s", Philosophy)
	}
}

// W19 + W14 实证：critic→reviewer 循环收敛 → validation.passed 里程碑 passed=true。
func TestTowerValidationMilestone(t *testing.T) {
	p, err := Package()
	if err != nil {
		t.Fatal(err)
	}
	pass := workflow.Milestone{
		WorkflowID: TowerID,
		Kind:       MilestoneValidationPassed,
		Data:       []byte(`{"critic_clean":true,"reviewer_verdict":"approved"}`),
	}
	res, err := p.HandleMilestone(context.Background(), pass)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Passed {
		t.Errorf("critic clean + reviewer approved 应 passed，得到 note=%q", res.Note)
	}

	// fail-close：reviewer 还没批 → 不通过。
	notYet := workflow.Milestone{
		Kind: MilestoneValidationPassed,
		Data: []byte(`{"critic_clean":true,"reviewer_verdict":"changes_requested"}`),
	}
	res2, err := p.HandleMilestone(context.Background(), notYet)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Passed {
		t.Error("reviewer changes_requested 应不通过")
	}

	// fail-close：critic 未收敛 → 不通过。
	criticOpen := workflow.Milestone{
		Kind: MilestoneValidationPassed,
		Data: []byte(`{"critic_clean":false,"reviewer_verdict":"approved"}`),
	}
	res3, _ := p.HandleMilestone(context.Background(), criticOpen)
	if res3.Passed {
		t.Error("critic 未收敛应不通过")
	}

	// 坏数据 → 报错（不静默当通过）。
	if _, err := p.HandleMilestone(context.Background(), workflow.Milestone{Kind: MilestoneValidationPassed, Data: []byte("{bad")}); err == nil {
		t.Error("坏里程碑数据应报错")
	}
}

// W4：tower + review-critic 追加段可组合（追加 loop）。
func TestTowerComposeReviewCritic(t *testing.T) {
	p, err := Package()
	if err != nil {
		t.Fatal(err)
	}
	composed := workflow.Compose(p.Spec(), ReviewCriticSegment())
	// 哲学并陈。
	if composed.Philosophy == p.Spec().Philosophy {
		t.Error("compose 应并陈追加段哲学")
	}
	// 追加段只新增 critic（base 已有 critic → 去重），不新增别的。
	baseTypes := len(p.Spec().AgentTypes)
	if len(composed.AgentTypes) != baseTypes {
		t.Errorf("compose 后 agent_types 应仍 %d 个（critic 去重），得到 %d", baseTypes, len(composed.AgentTypes))
	}
	// 组合后仍是合法能力包。
	if _, err := workflow.NewPackage(composed, validationMilestoneForTest()); err != nil {
		t.Fatalf("compose 产物应可构造能力包: %v", err)
	}
}

// validationMilestoneForTest 复用一个部件（compose 产物构造用）。
func validationMilestoneForTest() workflow.MilestoneHandler { return validationMilestone{} }

// PackageJSON 产出可被 workflow.ParsePackage 解析的纯数据形态（W13）。
func TestTowerPackageJSONRoundTrip(t *testing.T) {
	data, err := PackageJSON()
	if err != nil {
		t.Fatal(err)
	}
	p, err := workflow.ParsePackage(data)
	if err != nil {
		t.Fatal(err)
	}
	if p.ID() != TowerID {
		t.Errorf("round-trip id 错: %q", p.ID())
	}
}
