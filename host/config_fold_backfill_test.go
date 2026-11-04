package host_test

// G-e 写路径**回填**（堵死"静默放宽"，critic P2-3）。
//
// # 问题（静默放宽的具体形态）
//
//	tools 组是**整组 LWW**（组出现 = 整组覆盖，不拆子字段跨事件合并）→
//	第一次提交 {"only":["read"]}、第二次提交 {"except":["bash"]}（**没带 only**）→
//	第二次的组 = {except:[bash]}（only 缺失 = 未设置）→ 可见集**静默放宽**成
//	"全部工具减 bash"（第一轮的 only:[read] 被整组覆盖掉了）。
//
// # 修法（写路径回填）
//
//	tools 组出现且**至少一个子项**在 → 把缺失的那个子项**从当前 fold 值回填**进
//	事件 → 事件**自含整组** → 整组 LWW 的折叠语义**一字不变**，但不再静默丢掉
//	另一半。生效值 = only:[read] ∧ except:[bash]（可见 = {read} 减 bash）。
//
//	例外：tools:{}（**两子项都缺**）= **清除整组**（回默认）——回填不适用。
//
// # 与既有钉子的关系
//
//	"整组 LWW"（core/sessioncfg）的折叠口径**不变**：事件自含整组 → fold 照旧整组
//	覆盖（后写覆盖前一组）。改的只是**写路径把什么写进事件**（写全，而不是只写
//	本次提交的那一半）。

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/host"
)

// TestPromptConfigToolsWritePathBackfill：先 {"only":["read"]}，再 {"except":["bash"]}
// → 生效 = only:[read] ∧ except:[bash]（可见 = {read} 减 bash，**不是** 全部减 bash）；
// tools:{} 仍 = 清除整组（可见回默认）。
func TestPromptConfigToolsWritePathBackfill(t *testing.T) {
	prov := &toolsSpyProvider{}
	h := toolsHub(t, prov)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	prompt := func(what int, body map[string]any) {
		t.Helper()
		if code := postPrompt(t, srv.URL+"/api/sessions/"+string(sid)+"/prompt", body, nil); code != http.StatusAccepted {
			t.Fatalf("第 %d 条 prompt 状态码 = %d，want 202", what, code)
		}
		waitForSessionEvents(t, srv.URL, sid,
			fmt.Sprintf("第 %d 条 prompt 的 model.turn", what),
			func(evs []contracts.Event) bool { return kindCount(evs, "model.turn") >= what })
	}

	// ① 收窄到 [read]。
	prompt(1, map[string]any{"prompt": "一", "config": map[string]any{
		"tools": map[string]any{"only": []string{"read"}},
	}})
	// ② 只改 except → **回填**上一轮的 only → 生效 = [read] 减 bash（= [read]），
	//    不是"全部工具减 bash"（那会把可见集静默放宽成 [read write]）。
	prompt(2, map[string]any{"prompt": "二", "config": map[string]any{
		"tools": map[string]any{"except": []string{"bash"}},
	}})
	// ③ tools:{} → 清除整组（回默认）。
	prompt(3, map[string]any{"prompt": "三", "config": map[string]any{
		"tools": map[string]any{},
	}})

	calls := prov.waitCalls(t, 3)
	if !equalToolNames(calls[0].Tools, []string{"read"}) {
		t.Fatalf("① 收窄后可见工具 = %v，want [read]", calls[0].Tools)
	}
	if !equalToolNames(calls[1].Tools, []string{"read"}) {
		t.Fatalf("② 只改 except 时可见工具该仍是 [read]（回填 only:[read]，不是静默放宽成全部减 bash），得到 %v", calls[1].Tools)
	}
	if !equalToolNames(calls[2].Tools, []string{"read", "write"}) {
		t.Fatalf("③ tools:{} 清除整组后应回默认 [read write]，得到 %v", calls[2].Tools)
	}

	// 第二条 config.changed 的载荷**自含整组**（only 与 except 都在）——事件自含是
	// "整组 LWW 折叠语义不变"的前提。
	evs := sessionEvents(t, srv.URL, sid)
	changed := filterKind(evs, "config.changed")
	if len(changed) != 3 {
		t.Fatalf("config.changed 条数 = %d，want 3", len(changed))
	}
	cp := mustConfigPayload(t, changed[1])
	if cp.Tools == nil {
		t.Fatalf("第二条 config.changed 的载荷应带 tools: %s", string(changed[1].Payload))
	}
	if cp.Tools.Only == nil || cp.Tools.Except == nil {
		t.Fatalf("第二条 config.changed 的 tools 该自含整组（回填 only）: %s", string(changed[1].Payload))
	}
	if len(*cp.Tools.Only) != 1 || (*cp.Tools.Only)[0] != "read" {
		t.Fatalf("回填的 only 该是 [read]: %s", string(changed[1].Payload))
	}
	// 第三条（清除）仍是两子项都缺（回填不适用）。
	clear := mustConfigPayload(t, changed[2])
	if clear.Tools == nil || clear.Tools.Only != nil || clear.Tools.Except != nil {
		t.Fatalf("清除那条的 tools 两子项都该缺: %s", string(changed[2].Payload))
	}
}
