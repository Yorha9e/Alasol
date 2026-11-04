package main

// G-c **F7** 的回归钉子：改过 model 的 session → `alasol resume -continue` 续跑时
// provider 必须收到**覆盖 model**（fold 的 session 配置），不是 -model flag 的基值——
// 设计 §3.1"持久化影响后续所有轮"，CLI 续跑也是轮（不接生效链 = 悄悄回退默认值）。
//
// 手法：事件库先 seed 一条 config.changed（model=override-model），注入脚本 provider
// 记录收到的 Model；-model flag 传一个不同的基值，断言 fold 覆盖赢（同一生效链：
// flag 属 hub 默认层，session fold 在其上）。

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/sessioncfg"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// resumeModelSpy 记录续跑那一轮收到的 Model（不真打模型 API）。
type resumeModelSpy struct {
	mu     sync.Mutex
	models []string
}

func (p *resumeModelSpy) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu.Lock()
	p.models = append(p.models, req.Model)
	p.mu.Unlock()
	return core.ModelResult{Text: "续跑-ok"}, nil
}

func (p *resumeModelSpy) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.models))
	copy(out, p.models)
	return out
}

// seedConfigChanged 往事件库追加一条 config.changed（G-c 写路径的等价物——直接落库，
// 不经 host 的 HTTP 面：本用例钉的是 **resume 读侧**的 fold + 生效链）。
func seedConfigChanged(t *testing.T, dbPath string, session, agent contracts.Ulid, payload string) {
	t.Helper()
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	ev := contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: sessioncfg.KindConfigChanged,
		TS: "2026-11-03T00:00:00Z", SessionID: session, AgentID: agent,
		Durable: true, Payload: json.RawMessage(payload),
	}
	if _, err := st.Append(context.Background(), []contracts.Event{ev}); err != nil {
		t.Fatalf("append config.changed: %v", err)
	}
}

// TestResumeCLIContinueAppliesSessionModelOverride（F7）：seed model 覆盖 →
// resume -continue -model flag-model → provider 收到 override-model（fold 赢 flag）。
func TestResumeCLIContinueAppliesSessionModelOverride(t *testing.T) {
	db := filepath.Join(t.TempDir(), "resume.db")
	session, agent := seedCrashedSession(t, db)
	seedConfigChanged(t, db, session, agent, `{"model":"override-model"}`)

	spy := &resumeModelSpy{}
	prev := testProviderOverride
	testProviderOverride = spy
	defer func() { testProviderOverride = prev }()

	out := captureStdout(t)
	code := resumeMain([]string{
		"-session", string(session), "-db", db, "-verify", "none",
		"-model", "flag-model", // 基值层（hub 默认层）：fold 覆盖必须赢
		"-continue", "继续跑一轮",
	})
	got := out()
	if code != 0 {
		t.Fatalf("resumeMain 退出码 = %d, want 0\n%s", code, got)
	}
	models := spy.snapshot()
	if len(models) == 0 {
		t.Fatal("续跑根本没调 provider（-continue 没跑起来？）")
	}
	for i, m := range models {
		if m != "override-model" {
			t.Fatalf("第 %d 次续跑调用 model = %q，want override-model"+
				"（F7：session fold 的覆盖优先于 -model flag；调用序列 %v）", i, m, models)
		}
	}
}
