package host_test

// critic P1 的三枚钉（"本次提交的 kind 必须算进生效 kind"）：
//
//	① 先折叠 rules_budget（宿主默认 append → 布局不参与判定）→ 再切 kind=tiered
//	   → **400 bad_config + 零事件**（切策略与改布局必须同时被写时校验看到）；
//	② 一条提交 kind=tiered + 破布局 → 400 bad_config + 零事件；
//	③ profile 提升 tiered + 破布局 → 400 bad_config + 零事件。
//
// **为什么这三条曾经全中**（critic 实测）：生效 kind 只按 fold + 基线算 → "本次把 kind
// 切成 tiered"绕开写时校验（202 + config.changed 已落 → session 钉死，每轮装配期才
// fail-close）。
//
// 布局 = 窗口 − 注入区 − 阶段槽 − 尾部（compaction.ValidateLayout）；宿主窗口 300k +
// rules_budget 200k → 历史区被挤成负数 → 布局不成立。

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
)

// criticHub 起一个**宿主默认 append**、窗口 300k 的宿主（切 tiered 时布局才会被查）。
func criticHub(t *testing.T, prov core.Provider, profiles map[string]host.ProfileConfig) *host.Hub {
	t.Helper()
	return newTestHub(t, func(c *host.Config) {
		c.Provider = prov
		c.Allow = []string{"read"}
		c.ContextStrategy = ""            // 宿主默认 append（不挂策略）
		c.ContextStrategyWindow = 300_000 // 真实生效窗口（布局按它算）
		c.Profiles = profiles
	})
}

// TestCriticP1FoldRulesBudgetThenSwitchKind：① 先折叠 rules_budget → 再切 kind=tiered
// → 400 bad_config + 零新增事件。
func TestCriticP1FoldRulesBudgetThenSwitchKind(t *testing.T) {
	prov := &configSpyProvider{}
	h := criticHub(t, prov, nil)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	// ① 先折叠 rules_budget（宿主默认 append → 布局不参与判定）→ 202。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "先折叠预算", "config": map[string]any{
			"context": map[string]any{"rules_budget": 200_000},
		},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("① 状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "① 的 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	if n := kindCount(sessionEvents(t, srv.URL, sid), "config.changed"); n != 1 {
		t.Fatalf("① 应落 1 条 config.changed，得到 %d", n)
	}

	// ② 再切 kind=tiered → **本次提交的 kind 算进生效 kind** → 布局破 → 400 + 零事件。
	var errBody contracts.TransportError
	code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "切 tiered", "config": map[string]any{
			"context": map[string]any{"kind": "tiered"},
		},
	}, &errBody)
	if code != http.StatusBadRequest {
		t.Fatalf("② 状态码 = %d，want 400（切 tiered 后布局不成立，写时就要拒）", code)
	}
	if errBody.Code != "bad_config" {
		t.Fatalf("② 错误码 = %q，want bad_config", errBody.Code)
	}
	if n := kindCount(sessionEvents(t, srv.URL, sid), "config.changed"); n != 1 {
		t.Fatalf("② 非法提交不该落事件，config.changed = %d 条，want 1", n)
	}
}

// TestCriticP1SingleSubmitKindAndBrokenLayout：② 一条提交 kind=tiered + 破布局 → 400。
func TestCriticP1SingleSubmitKindAndBrokenLayout(t *testing.T) {
	prov := &configSpyProvider{}
	h := criticHub(t, prov, nil)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	var errBody contracts.TransportError
	code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "一条提交", "config": map[string]any{
			"context": map[string]any{"kind": "tiered", "rules_budget": 200_000},
		},
	}, &errBody)
	if code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，want 400（同一条提交里切 tiered + 破布局）", code)
	}
	if errBody.Code != "bad_config" {
		t.Fatalf("错误码 = %q，want bad_config", errBody.Code)
	}
	if n := kindCount(sessionEvents(t, srv.URL, sid), "config.changed"); n != 0 {
		t.Fatalf("非法提交不该落事件，config.changed = %d 条，want 0", n)
	}
}

// TestCriticP1ProfilePromotesTieredBrokenLayout：③ profile 提升 tiered + 破布局 → 400。
//
// profile 的展开结果与显式提交**同一条校验路径**（展开后的变更集 = 本次提交）→ 同样拒。
func TestCriticP1ProfilePromotesTieredBrokenLayout(t *testing.T) {
	prov := &configSpyProvider{}
	h := criticHub(t, prov, map[string]host.ProfileConfig{
		"heavy": {
			Name:    "heavy",
			Context: host.ProfileContextConfig{Kind: "tiered", RulesBudget: 200_000},
		},
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	var errBody contracts.TransportError
	code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "profile 提升 tiered", "profile": "heavy",
	}, &errBody)
	if code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，want 400（profile 提升 tiered 后布局不成立）", code)
	}
	if errBody.Code != "bad_config" {
		t.Fatalf("错误码 = %q，want bad_config", errBody.Code)
	}
	if n := kindCount(sessionEvents(t, srv.URL, sid), "config.changed"); n != 0 {
		t.Fatalf("非法提交不该落事件，config.changed = %d 条，want 0", n)
	}
}
