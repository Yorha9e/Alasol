package main

// G-h 的 **CLI resume 同链**钉子：session 级上下文面覆盖（config.context）→
// `alasol resume -continue` 续跑时也走同一条生效链（host.RunOverridesFromConfig =
// host 与 resume 共用的唯一取值口）。
//
// 观测面：续跑那一轮的 context.assembled 事件的 tokens_budget（占比分母）——
// CLI 那条链没有宿主默认（挂不起上下文策略，故 kind 覆盖不生效，见挂账），但**窗口
// 覆盖照样生效**（它换的是占比分母）。
//
// 手法：事件库先 seed 一条 config.changed（context.window 覆盖），注入脚本 provider，
// 跑 resume -continue，断言落库的 context.assembled 的 tokens_budget = 覆盖值。

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// TestResumeCLIContinueAppliesSessionContextOverride：seed context.window 覆盖 →
// resume -continue → 续跑那一轮的占比分母 = 覆盖值（同一条生效链）。
func TestResumeCLIContinueAppliesSessionContextOverride(t *testing.T) {
	db := filepath.Join(t.TempDir(), "resume.db")
	session, agent := seedCrashedSession(t, db)
	seedConfigChanged(t, db, session, agent, `{"context":{"window":512000}}`)

	spy := &resumeModelSpy{}
	prev := testProviderOverride
	testProviderOverride = spy
	defer func() { testProviderOverride = prev }()

	out := captureStdout(t)
	code := resumeMain([]string{
		"-session", string(session), "-db", db, "-verify", "none",
		"-model", "flag-model", // 基值层（hub 默认层）：与 serve 面同一条生效链
		"-continue", "继续跑一轮",
	})
	got := out()
	if code != 0 {
		t.Fatalf("resumeMain 退出码 = %d, want 0\n%s", code, got)
	}
	if len(spy.snapshot()) == 0 {
		t.Fatal("续跑根本没调 provider（-continue 没跑起来？）")
	}

	// 读回事件库：续跑那一轮落了一条 context.assembled，其 tokens_budget = 覆盖值。
	st, err := sqlite.Open(db, sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	events, err := st.ReadFrom(context.Background(), &session, 0, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	var budgets []int64
	for _, ev := range events {
		if ev.Kind != "context.assembled" {
			continue
		}
		var snap contracts.ContextSnapshot
		if err := json.Unmarshal(ev.Payload, &snap); err != nil {
			t.Fatalf("解 context.assembled: %v", err)
		}
		budgets = append(budgets, snap.TokensBudget)
	}
	if len(budgets) == 0 {
		t.Fatal("续跑那一轮没有 context.assembled 事件")
	}
	last := budgets[len(budgets)-1]
	if last != 512000 {
		t.Fatalf("续跑的 tokens_budget = %d，want 512000（G-h：resume 与 serve 同一条生效链）", last)
	}
	_ = core.NewUlid
}
