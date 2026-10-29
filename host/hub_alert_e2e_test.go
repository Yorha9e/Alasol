package host_test

// v2-S5 清账②③ 的**hub 级端到端**测试（p2-4）：
//
// 装配关口（host.New 里那几行）此前**零测试**——把 skipSession / WithAlertSink /
// Rebuild 任一行删掉，全仓测试照样绿。本文件起**真 hub**（真事件库、真监控器、
// 真 worktree 管理器）把这三个关口钉死：
//
//	① 告警入流：hub 起 → 监控超阈值 → 事件流里能读到 hub.alert（durable 事实事件）；
//	② 在线表：伪 session **不进** /api/sessions（在线表是 agent session 面板）；
//	③ worktree 登记表：hub 重启 → 从磁盘重建（"谁建了哪个 worktree"不随进程消失）。

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// hubAlertEvents 读事件流里的 hub.alert 事件（最多等 timeout）。
//
// 用**第二条连接**读同一个库文件（WAL：读写不互斥；busy_timeout 兜住 DDL 抢锁）。
func hubAlertEvents(t *testing.T, dbPath string, timeout time.Duration) []contracts.Event {
	t.Helper()
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	deadline := time.Now().Add(timeout)
	var out []contracts.Event
	for {
		evs, err := st.ReadFrom(context.Background(), nil, 1, 500)
		if err != nil {
			t.Fatalf("ReadFrom: %v", err)
		}
		out = out[:0]
		for _, ev := range evs {
			if ev.Kind == "hub.alert" {
				out = append(out, ev)
			}
		}
		if len(out) > 0 || time.Now().After(deadline) {
			return out
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestHubAlertReachesEventStreamEndToEnd（清账②硬验收）：**真 hub** 里的告警
// 落进事件流（不是只在日志里）。
//
// 触发方式不靠"真烧 CPU"：把内存阈值压到 1MB（进程内存必然超）→ 监控器起来
// 第一次采样就超阈值 → 告警经装配好的 sink 进事件流。
func TestHubAlertReachesEventStreamEndToEnd(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "events.db")
	newTestHub(t, func(c *host.Config) {
		c.DBPath = dbPath
		c.MonitorMemMB = 1        // 1MB 阈值：必超（不靠真烧 CPU）
		c.MonitorIntervalSec = 60 // 抑制窗口内也不会有第二条
	})

	evs := hubAlertEvents(t, dbPath, 10*time.Second)
	if len(evs) == 0 {
		t.Fatal("事件流里应有 hub.alert 事件（告警必须进唯一事实源，F5）")
	}
	ev := evs[0]
	if !ev.Durable || ev.Seq == nil {
		t.Fatalf("hub.alert 必须是 durable 事实事件且拿到 seq: %+v", ev)
	}
	if ev.V != 1 || ev.TS == "" {
		t.Fatalf("envelope 字段残缺: %+v", ev)
	}
	var p contracts.HubAlertPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if p.Metric == "" || p.Threshold <= 0 || p.Message == "" {
		t.Fatalf("hub.alert 载荷四要素不齐: %+v", p)
	}
	// 载荷不含任何会话内容/路径/凭据（H12/H19 脱敏纪律）。
	raw := string(ev.Payload)
	for _, leak := range []string{"session_id", "agent_id", "api_key", "Authorization"} {
		if containsFold(raw, leak) {
			t.Fatalf("hub.alert 载荷不该含 %q: %s", leak, raw)
		}
	}
}

// TestHubOnlineTableHasNoPseudoSession（清账② + p2-2）：hub 伪 session **不进**
// 在线表（/api/sessions 是 agent session 面板）。
//
// 判据是**事件族**（hub.*）：伪 id 的派生口径会变（库路径写法/规范化口径），
// 按 id 登记就会漏——所以这里用一个**与派生口径无关**的残留 id 来验整族过滤。
func TestHubOnlineTableHasNoPseudoSession(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "events.db")
	h := newTestHub(t, func(c *host.Config) {
		c.DBPath = dbPath
		c.MonitorMemMB = 1
		c.MonitorIntervalSec = 60
	})

	// 告警确实在事件流里（否则下面的断言没有意义）。
	evs := hubAlertEvents(t, dbPath, 10*time.Second)
	if len(evs) == 0 {
		t.Fatal("前提不成立：事件流里没有 hub.alert")
	}
	pseudo := evs[0].SessionID

	rows, err := h.Sessions(context.Background())
	if err != nil {
		t.Fatalf("Sessions: %v", err)
	}
	for _, r := range rows {
		if r.SessionID == string(pseudo) {
			t.Fatalf("hub 伪 session 不该出现在在线表: %+v", r)
		}
	}

	// 同样的口径过一遍**真 HTTP 面**（/api/sessions 是 UI 实际读的那张表）。
	req := httptest.NewRequest(http.MethodGet, "/api/sessions", nil)
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/sessions 状态码 = %d，want 200", rec.Code)
	}
	var apiRows []host.SessionRow
	if err := json.Unmarshal(rec.Body.Bytes(), &apiRows); err != nil {
		t.Fatalf("解析 /api/sessions 响应: %v（body=%s）", err, rec.Body.String())
	}
	for _, r := range apiRows {
		if r.SessionID == string(pseudo) {
			t.Fatalf("/api/sessions 里不该有 hub 伪 session: %+v", r)
		}
	}
}

// TestHubWorktreeRegistryRebuiltAfterRestart（清账③ + p2-4）：hub 重启后
// worktree 登记表**从磁盘重建**（"谁建了哪个 worktree"不随进程消失）。
//
// 这是装配关口：把 host.New 里的 Rebuild 删掉，这张表重启后就是空的。
func TestHubWorktreeRegistryRebuiltAfterRestart(t *testing.T) {
	if gitExe() == "" {
		t.Skip("git 不可用，跳过 worktree 用例")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "events.db")
	wtRoot := filepath.Join(dir, "worktrees")
	repo := initGitRepo(t, filepath.Join(dir, "repo"))

	cfg := func() host.Config {
		return host.Config{
			DBPath:       dbPath,
			Model:        "scripted",
			MaxTurns:     2,
			WorktreeRoot: wtRoot,
			CWD:          repo,
		}
	}
	h1, err := host.New(cfg())
	if err != nil {
		t.Fatalf("host.New #1: %v", err)
	}
	wt, err := h1.CreateWorktree(context.Background(), repo, "task-1")
	if err != nil {
		_ = h1.Close()
		t.Fatalf("CreateWorktree: %v", err)
	}
	if len(h1.Worktrees()) != 1 {
		_ = h1.Close()
		t.Fatalf("建完应有 1 条登记: %+v", h1.Worktrees())
	}
	if err := h1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}
	// 工作区仍在磁盘上（重启前的既成事实）。
	if _, err := os.Stat(wt.Path); err != nil {
		t.Fatalf("worktree 工作区应仍在磁盘上: %v", err)
	}

	// 重启：新的 hub（同一事件库、同一 worktree root）。
	h2, err := host.New(cfg())
	if err != nil {
		t.Fatalf("host.New #2: %v", err)
	}
	t.Cleanup(func() { _ = h2.Close() })
	list := h2.Worktrees()
	if len(list) != 1 || list[0].Name != "task-1" {
		t.Fatalf("重启后登记表应从磁盘重建出 task-1: %+v", list)
	}
	if list[0].Path != wt.Path {
		t.Fatalf("重建出来的 Path 应与磁盘事实一致: %q vs %q", list[0].Path, wt.Path)
	}
}

// containsFold 是大小写不敏感的子串判断（测试辅助）。
func containsFold(s, sub string) bool {
	return len(sub) == 0 || indexFold(s, sub) >= 0
}

func indexFold(s, sub string) int {
	ls, lsub := lower(s), lower(sub)
	for i := 0; i+len(lsub) <= len(ls); i++ {
		if ls[i:i+len(lsub)] == lsub {
			return i
		}
	}
	return -1
}

func lower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}
