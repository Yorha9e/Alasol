package host

// G-f（standby 挂起期的 waiting 态）host 层接线的验收用例：
//
//	① SessionRow.waiting 从出口读到真值（h.Sessions 结构面 + GET /api/sessions JSON 面），
//	   且 waiting 算活跃（极小窗下 BeginWait 短路 online=true、EndWait 回落）、
//	   不刷最后事件 ts（last_activity / updated_at 不动）；
//	② 在线表数据源 OnlineRows：self 行与对端行都补 Waiting（对端行回查发现表行）；
//	③ 渲染带 [waiting] 标记（summary / n / full 三等级各断言一处；另验 summary 的
//	   token 预算截断下标记仍在行首、n 的 WindowLines=1 只渲染身份行时标记仍在）。
//
// 为什么是 internal（package host）：窗口夹具与 BeginWait/EndWait 要直接摸 h.disc、
// 数据源是未导出的 discoveryOnlineSource——黑箱面没有这些入口。窗口夹具 nearWindow
// 与 sessions_online_test.go 同款（同包共用）。

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core/loop"
)

// TestSessionRowWaitingOutlet：① 出口读 waiting 真值 + 活跃短路 + 不刷最后事件 ts。
func TestSessionRowWaitingOutlet(t *testing.T) {
	h := newOnlineTestHub(t)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := runOnlinePrompt(t, h, "任务")

	// 挂起前：waiting=false（事件新鲜 → online=true）。
	row := sessionRowOf(t, h, sid)
	if row.Waiting {
		t.Fatalf("挂起前不该 waiting: %+v", row)
	}
	lastActivity := row.LastActivity
	if lastActivity == "" {
		t.Fatal("有事件的行 last_activity 不该为空（夹具前提）")
	}

	// 窗口收到极小 → online=false（夹具：让 waiting 短路在出口上可见）。
	h.disc.SetActivityWindow(nearWindow)
	row = sessionRowOf(t, h, sid)
	if row.Online {
		t.Fatalf("极小窗下不该 online（夹具前提）: %+v", row)
	}

	// BeginWait：waiting=true 且 online=true（waiting 短路，豁免窗口判定）、
	// running=false；last_activity / updated_at 都不动（不刷最后事件 ts）。
	h.disc.BeginWait(sid)
	row = sessionRowOf(t, h, sid)
	if !row.Waiting || !row.Online || row.Running {
		t.Fatalf("BeginWait 后应 waiting=true + online=true + running=false: %+v", row)
	}
	if row.LastActivity != lastActivity || row.UpdatedAt != lastActivity {
		t.Fatalf("waiting 不刷最后事件 ts: last_activity=%q updated_at=%q, want %q",
			row.LastActivity, row.UpdatedAt, lastActivity)
	}

	// JSON 面：行序列化带 "waiting" 键（只增；没 tag 会序列化成 "Waiting"，这里会红）。
	if b, err := json.Marshal(row); err != nil || !strings.Contains(string(b), `"waiting":`) {
		t.Fatalf("行 JSON 应带 waiting 键（err=%v）: %s", err, b)
	}
	// HTTP 出口同真值。
	found := false
	for _, r := range getSessionRows(t, srv.URL+"/api/sessions") {
		if r.SessionID == string(sid) {
			found = true
			if !r.Waiting {
				t.Fatalf("GET /api/sessions 的 waiting 应为 true: %+v", r)
			}
		}
	}
	if !found {
		t.Fatalf("session %s 不在 GET /api/sessions 里", sid)
	}

	// EndWait（重复一次验下限 0 的出口表现）：waiting=false、极小窗 → online=false；
	// ts 仍不动。
	h.disc.EndWait(sid)
	h.disc.EndWait(sid)
	row = sessionRowOf(t, h, sid)
	if row.Waiting || row.Online {
		t.Fatalf("EndWait 后应回落（waiting=false、超窗 online=false）: %+v", row)
	}
	if row.LastActivity != lastActivity {
		t.Fatalf("EndWait 也不刷最后事件 ts: %q vs %q", row.LastActivity, lastActivity)
	}
}

// TestOnlineRowsWaitingMarker：② 数据源 self/对端行补 Waiting + ③ 三等级渲染带
// [waiting]（含 token 预算截断与 WindowLines=1 两个"标记别被截没"的边界）。
func TestOnlineRowsWaitingMarker(t *testing.T) {
	h := newOnlineTestHub(t)
	ctx := context.Background()

	sidA := runOnlinePrompt(t, h, "A 的任务")
	sidB := runOnlinePrompt(t, h, "B 的任务")
	// 显式 join 同一 gate → 互为可见对端（新测试 hub 的 CWD 非 git，没有自动 gate）。
	if _, err := h.JoinGate(ctx, sidA, "team:wait"); err != nil {
		t.Fatalf("JoinGate(A): %v", err)
	}
	if _, err := h.JoinGate(ctx, sidB, "team:wait"); err != nil {
		t.Fatalf("JoinGate(B): %v", err)
	}

	// 挂起前：self 行 + 对端行都在（事件新鲜），Waiting 都是 false。
	rows := h.onlineSource.OnlineRows(ctx, sidA)
	if len(rows) != 2 {
		t.Fatalf("A 的在线表应 2 行（self + peer B），得到 %d: %+v", len(rows), rows)
	}
	if !rows[0].Self || rows[0].Waiting {
		t.Fatalf("self 行挂起前不该 Waiting: %+v", rows[0])
	}
	if rows[1].Self || rows[1].SessionID != string(sidB) || rows[1].Waiting {
		t.Fatalf("对端行挂起前不该 Waiting: %+v", rows[1])
	}

	// 窗口收到极小 → 对端超窗落出（self 恒在，不做活跃过滤）。
	h.disc.SetActivityWindow(nearWindow)
	if got := h.onlineSource.OnlineRows(ctx, sidA); len(got) != 1 {
		t.Fatalf("极小窗下 A 只该剩 self 行，得到 %d: %+v", len(got), got)
	}

	// BeginWait(B)：waiting 算活跃 → 对端回到可见面，且对端行补上 Waiting=true。
	h.disc.BeginWait(sidB)
	rows = h.onlineSource.OnlineRows(ctx, sidA)
	if len(rows) != 2 || rows[1].SessionID != string(sidB) || !rows[1].Waiting {
		t.Fatalf("BeginWait(B) 后对端行应回列且 Waiting=true: %+v", rows)
	}

	// BeginWait(A)：self 行也补 Waiting=true。
	h.disc.BeginWait(sidA)
	rows = h.onlineSource.OnlineRows(ctx, sidA)
	if len(rows) != 2 || !rows[0].Waiting || !rows[1].Waiting {
		t.Fatalf("BeginWait(A) 后 self/对端行都该 Waiting=true: %+v", rows)
	}

	// ③ 三等级渲染都带 [waiting]；两处"标记别被截没"的边界：
	//   - summary 的 LineTokenBudget 截断保留头部（标记在行首）；
	//   - n 的 WindowLines=1 只渲染身份行（标记落在身份行）。
	cases := []struct {
		name string
		view loop.OnlineTableView
	}{
		{"summary", loop.OnlineTableView{Level: loop.LevelSummary}},
		{"summary-小预算", loop.OnlineTableView{Level: loop.LevelSummary, LineTokenBudget: 20}},
		{"n-单行", loop.OnlineTableView{Level: loop.LevelN, WindowLines: 1}},
		{"full", loop.OnlineTableView{Level: loop.LevelFull}},
	}
	for _, tc := range cases {
		text, n := loop.RenderOnlineTable(rows, tc.view)
		if n == 0 {
			t.Fatalf("%s 等级应渲染出 行: %q", tc.name, text)
		}
		if !strings.Contains(text, "[waiting]") {
			t.Fatalf("%s 等级应渲染带 [waiting] 标记: %q", tc.name, text)
		}
	}

	// EndWait 配平：self 行 Waiting 回落；对端重新超窗落出。
	h.disc.EndWait(sidA)
	h.disc.EndWait(sidB)
	rows = h.onlineSource.OnlineRows(ctx, sidA)
	if len(rows) != 1 || rows[0].Waiting {
		t.Fatalf("EndWait 后应回到仅 self 行且 Waiting=false: %+v", rows)
	}
}
