package host_test

// G-h **策略切换钉**（批次4 件1）：session 级的 context.kind 值空间 =
// ""（清除回默认）| "append"（**显式**追加式）| "tiered"（两级压缩）。
//
// 用户明确要"append→tiered→append 来回切"——"空串"只能清除、落回宿主默认（宿主默认是
// tiered 时切不回来），故 session 级多一个**显式**值 "append"。
//
// 三段钉：
//
//	① 宿主默认 tiered + kind="append" → 本轮**真走追加式**（无注入区、无压缩痕迹）；
//	② kind="" → 落回宿主默认（tiered）；
//	③ **来回切无损**：同一 session append→tiered→append 三段跑 →
//	   - 每段组装形态正确（tiered 段有 stage:N 压缩痕迹，append 段全量原样）；
//	   - 切回 append 后**全量历史完整**（压缩摘要只是投影、原文仍在事件流里 → 重建不受影响）。
//
// 手法：脚本 provider 按"stage 工具是否可见"决定要不要打阶段边界（可见 = tiered 路径），
// 并记录每次主对话调用的消息（断言全量历史）。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
)

// switchProvider 脚本 provider：
//
//   - 主对话：看得见 "stage" 框架工具（= tiered 路径）→ 先打两次阶段边界，再回文本；
//   - 主对话：看不见 stage（= 追加式）→ 直接回文本；
//   - 压缩模型调用（system 含"上下文压缩器"）→ 回一句摘要。
//
// 记录每次**主对话**调用的消息（压缩调用不入列——它不是对话）。
type switchProvider struct {
	mu         sync.Mutex
	stageCalls int
	reqs       [][]core.Message
}

func (p *switchProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	if strings.Contains(req.System, "上下文压缩器") {
		return core.ModelResult{Text: "阶段要点摘要", Usage: core.Usage{InputTokens: 5, OutputTokens: 3}}, nil
	}
	p.mu.Lock()
	p.reqs = append(p.reqs, req.Messages)
	// 阶段边界只打两次（**tiered 路径才有 stage 工具可见**）——两次边界 = 两个已完成
	// 阶段 → 更老的那段被压成 L1 摘要（stage:N 压缩痕迹）。
	wantStage := hasToolName(req.Tools, "stage") && p.stageCalls < 2
	stage := p.stageCalls
	if wantStage {
		p.stageCalls++
	}
	p.mu.Unlock()
	if wantStage {
		tc := core.ToolCallReq{
			ID:   contracts.ProviderId(fmt.Sprintf("stage-%d", stage)),
			Name: "stage",
			Args: json.RawMessage(`{"label":"阶段"}`),
		}
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 5, OutputTokens: 3}}, nil
	}
	return core.ModelResult{Text: "本轮完成", Usage: core.Usage{InputTokens: 5, OutputTokens: 3}}, nil
}

// mainReqs 返回主对话调用的消息列表（副本）。
func (p *switchProvider) mainReqs() [][]core.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([][]core.Message, len(p.reqs))
	copy(out, p.reqs)
	return out
}

// hasToolName 报告请求里有没有这个工具。
func hasToolName(tools []contracts.Tool, want string) bool {
	for i := range tools {
		if tools[i].Name == want {
			return true
		}
	}
	return false
}

// hasSourceIn 报告来源列表里有没有给定 source（stage:N 用前缀匹配）。
func hasSourceIn(sources []string, want string) bool {
	for _, s := range sources {
		if s == want {
			return true
		}
		if strings.HasPrefix(want, "stage:") && strings.HasPrefix(s, "stage:") {
			return true
		}
	}
	return false
}

// snapshotSourcesOf 取一条 context.assembled 快照的 parts 来源列表。
func snapshotSourcesOf(t *testing.T, evs []contracts.Event, idx int) []string {
	t.Helper()
	snaps := filterKind(evs, "context.assembled")
	if idx >= len(snaps) {
		t.Fatalf("context.assembled 只有 %d 条，取不到第 %d 条", len(snaps), idx)
	}
	var snap contracts.ContextSnapshot
	if err := json.Unmarshal(snaps[idx].Payload, &snap); err != nil {
		t.Fatalf("解 context.assembled: %v", err)
	}
	out := make([]string, 0, len(snap.Parts))
	for _, p := range snap.Parts {
		out = append(out, p.Source)
	}
	return out
}

// switchHub 起一个**宿主默认 tiered** 的宿主（带项目规则——tiered 路径会注入它，
// 追加式不会 → 装配形态的判别位）。
func switchHub(t *testing.T, prov core.Provider) *host.Hub {
	t.Helper()
	return newTestHub(t, func(c *host.Config) {
		c.Provider = prov
		c.ContextStrategy = "tiered"
		c.ContextStrategyWindow = 300_000
		c.ContextRules = "[项目规则] 遵循 AGENTS.md"
		c.Allow = []string{"read", "recall", "stage"}
	})
}

// TestPromptConfigContextKindSwitchRoundTrip：**来回切无损**——同一 session 跑三段
// （append → tiered → append）：
//
//	① 第 1 段（append）：组装无注入区、无压缩痕迹（纯追加式）；
//	② 第 2 段（tiered）：组装有 stage:N 压缩痕迹（真走了压缩路径）；
//	③ 第 3 段（切回 append）：组装又回到纯追加式，且**全量历史完整**（第一、二段的
//	   原文都在请求里——压缩摘要是投影，丢的是摘要不是原文）。
func TestPromptConfigContextKindSwitchRoundTrip(t *testing.T) {
	prov := &switchProvider{}
	h := switchHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	// 每段的**累计轮数**（模型轮 = 一次 provider 调用）：第 1 段 1 轮；第 2 段 3 轮
	// （两次 stage 边界 + 收尾）；第 3 段 1 轮。等待按**累计轮数**（不按段号——段号与
	// 轮数不对齐：tiered 段有 3 轮，按段号等会早放行、断言落在上一段的快照上）。
	seg := func(what int, wantTurns int, kind string) {
		t.Helper()
		body := map[string]any{"prompt": fmt.Sprintf("第%d段", what)}
		if kind != "" {
			body["config"] = map[string]any{"context": map[string]any{"kind": kind}}
		}
		if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", body, nil); code != http.StatusAccepted {
			t.Fatalf("第 %d 段状态码 = %d，want 202", what, code)
		}
		waitForSessionEvents(t, srv.URL, sid,
			fmt.Sprintf("第 %d 段的 model.turn", what),
			func(evs []contracts.Event) bool { return kindCount(evs, "model.turn") >= wantTurns })
	}

	seg(1, 1, "append") // 显式追加式
	seg(2, 4, "tiered") // 显式两级压缩（3 轮：两次 stage 边界 + 收尾）
	seg(3, 5, "append") // 切回追加式

	evs := sessionEvents(t, srv.URL, sid)
	snaps := filterKind(evs, "context.assembled")
	if len(snaps) < 3 {
		t.Fatalf("context.assembled 只有 %d 条（至少三条：每段至少一轮）", len(snaps))
	}
	// ① 第 1 段（append）：纯追加式——没有注入区（rules）、没有压缩痕迹（stage:N）。
	s1 := snapshotSourcesOf(t, evs, 0)
	if hasSourceIn(s1, "rules") || hasSourceIn(s1, "stage:") {
		t.Errorf("第 1 段（append）不该有注入区/压缩痕迹：%v", s1)
	}
	// ② 第 2 段（tiered）：走压缩路径——有 stage:N 痕迹（阶段被打过边界、被压成摘要）。
	var tieredSources []string
	for _, ev := range snaps {
		var snap contracts.ContextSnapshot
		if err := json.Unmarshal(ev.Payload, &snap); err != nil {
			t.Fatalf("解 context.assembled: %v", err)
		}
		for _, p := range snap.Parts {
			if strings.HasPrefix(p.Source, "stage:") {
				tieredSources = append(tieredSources, p.Source)
			}
		}
	}
	if len(tieredSources) == 0 {
		t.Errorf("第 2 段（tiered）该有 stage:N 压缩痕迹（真走压缩路径），快照来源=%v", snapshotSourcesOf(t, evs, 0))
	}
	// ③ 第 3 段（切回 append）：组装回到纯追加式。
	last := snapshotSourcesOf(t, evs, len(snaps)-1)
	if hasSourceIn(last, "rules") || hasSourceIn(last, "stage:") {
		t.Errorf("第 3 段（切回 append）不该有注入区/压缩痕迹：%v", last)
	}
	// ③ 全量历史完整：切回 append 后，最后那次主对话请求里带着**前两段的原文**——
	// **逐条等值**（条数 + 顺序；不拿"拼接后找子串"糊弄：子串会把"顺序错了"和
	// "内容恰好出现"一起放过）。
	reqs := prov.mainReqs()
	if len(reqs) == 0 {
		t.Fatal("provider 没被调用过")
	}
	lastReq := reqs[len(reqs)-1]
	want := []string{"第1段", "第2段"}
	got := make([]string, 0, len(want))
	for _, m := range lastReq {
		for _, w := range want {
			if m.Content == w { // 逐条**等值**（不是子串）
				got = append(got, w)
			}
		}
	}
	if len(got) != len(want) {
		t.Fatalf("切回 append 后的请求里应逐条带 %v（条数 %d），实际 %v（全量历史完整）", want, len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("切回 append 后的请求里历史**顺序**不对：want %v，got %v", want, got)
		}
	}
}

// TestPromptConfigContextKindAppendOverridesTieredDefault：宿主默认 tiered +
// session 提交 kind="append" → 本轮**真走追加式**（不挂策略：没有注入区、没有压缩）。
// 反向同理：宿主默认 append + kind="tiered" → 本轮走压缩路径。
func TestPromptConfigContextKindAppendOverridesTieredDefault(t *testing.T) {
	// ① 宿主默认 tiered → session 切 append。
	prov := &switchProvider{}
	h := switchHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "切成追加式", "config": map[string]any{"context": map[string]any{"kind": "append"}},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 1
	})
	evs := sessionEvents(t, srv.URL, sid)
	src := snapshotSourcesOf(t, evs, 0)
	if hasSourceIn(src, "rules") || hasSourceIn(src, "stage:") {
		t.Errorf("kind=append 那一轮不该有注入区/压缩痕迹：%v", src)
	}
	// 事件流里的 config.changed 带着 kind=append（审计可见）。
	cc := filterKind(evs, "config.changed")
	if len(cc) != 1 {
		t.Fatalf("config.changed 条数 = %d，want 1", len(cc))
	}
	p := mustConfigPayload(t, cc[0])
	if p.Context == nil || p.Context.Kind == nil || *p.Context.Kind != "append" {
		t.Fatalf("config.changed 应带 context.kind=append：%s", string(cc[0].Payload))
	}

	// ② kind="" → 清除回默认（宿主默认 tiered）→ 走压缩路径（有注入区）。
	if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
		"prompt": "清除回默认", "config": map[string]any{"context": map[string]any{"kind": ""}},
	}, nil); code != http.StatusAccepted {
		t.Fatalf("状态码 = %d，want 202", code)
	}
	waitForSessionEvents(t, srv.URL, sid, "第二轮 model.turn", func(evs []contracts.Event) bool {
		return kindCount(evs, "model.turn") >= 2
	})
	evs = sessionEvents(t, srv.URL, sid)
	src2 := snapshotSourcesOf(t, evs, len(filterKind(evs, "context.assembled"))-1)
	if !hasSourceIn(src2, "rules") {
		t.Errorf("kind 清除回默认（宿主默认 tiered）那一轮该有注入区（rules）：%v", src2)
	}
}

// TestPromptConfigContextKindAppendRejectedOnUnknownValue：写时白名单：只接受
// "" / append / tiered —— 其它值 400 bad_config + 零事件。
func TestPromptConfigContextKindAppendRejectedOnUnknownValue(t *testing.T) {
	prov := &switchProvider{}
	h := switchHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	for _, bad := range []string{"sliding_window", "stage_scoped", "APPEND"} {
		var errBody contracts.TransportError
		code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
			"prompt": "坏 kind", "config": map[string]any{"context": map[string]any{"kind": bad}},
		}, &errBody)
		if code != http.StatusBadRequest {
			t.Fatalf("kind=%q 状态码 = %d，want 400", bad, code)
		}
		if errBody.Code != "bad_config" {
			t.Fatalf("kind=%q 错误码 = %q，want bad_config", bad, errBody.Code)
		}
	}
	if n := kindCount(sessionEvents(t, srv.URL, sid), "config.changed"); n != 0 {
		t.Fatalf("非法 kind 落了 %d 条 config.changed，want 0", n)
	}
	// 合法值（append / tiered / 空串）都受理。
	for _, ok := range []string{"append", "tiered", ""} {
		if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", map[string]any{
			"prompt": "好 kind", "config": map[string]any{"context": map[string]any{"kind": ok}},
		}, nil); code != http.StatusAccepted {
			t.Fatalf("kind=%q 状态码 = %d，want 202", ok, code)
		}
	}
}
