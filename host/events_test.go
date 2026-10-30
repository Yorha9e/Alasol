package host_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// eventsResp 是 GET /api/events 的响应形状（测试侧镜像，与 host.eventsView 对应）。
type eventsResp struct {
	Events    []contracts.Event `json:"events"`
	Watermark int64             `json:"watermark"`
	From      int64             `json:"from"`
	To        int64             `json:"to"`
	Truncated bool              `json:"truncated"`
}

// TestEventsReplayNoHoles（G3 硬验收）：断线后按 seq 补读回放，**区间无空洞**。
//
// 判据：把事件流**分页**回放（每页 2 条，逼出 truncated + 翻页），收集全部事件 id，
// 与**直读取库**的基线逐一比对——分页回放的并集必须等于库里全部 durable 事件
// （不多、不少、不乱序）。这正对应第三方 GUI 的痛点：WS 断线丢事件，回放面必须补得回来。
func TestEventsReplayNoHoles(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "events.db")
	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("replay"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	cfg := host.Config{
		DBPath:   dbPath,
		Model:    "scripted",
		System:   "test-system",
		MaxTurns: 4,
		Allow:    []string{"read"},
		CWD:      tempRepoOrDir(t),
		Provider: &scriptedProvider{args: `{"path":"` + filepath.ToSlash(target) + `"}`},
	}
	h, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	// 跑几轮（多个 session）→ 事件流里有一批 durable 事件。不复用 runOneRound：
	// 它断言单轮 Events==4，而脚本 provider 是有状态的（首轮发工具调用、其后只回文本），
	// 同一 hub 跑第二轮计数就不同——这里只要"有一批 durable 事件可回放"，不盯单轮条数。
	for i := 0; i < 3; i++ {
		s := host.NewSession()
		if _, err := h.RunPrompt(context.Background(), s, "读一下"); err != nil {
			t.Fatalf("RunPrompt %d: %v", i, err)
		}
	}

	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// 基线：第二条连接直读取库（WAL 读写不互斥），拿"事实"全集。
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	base, err := st.ReadFrom(context.Background(), nil, 1, 0)
	if err != nil {
		t.Fatalf("ReadFrom baseline: %v", err)
	}
	if len(base) < 5 {
		t.Fatalf("基线事件太少（%d），测试前提不成立", len(base))
	}
	baseIDs := make(map[string]bool, len(base))
	for _, ev := range base {
		if !ev.Durable || ev.Seq == nil {
			t.Fatalf("基线应全是带 seq 的 durable 事件: %+v", ev)
		}
		baseIDs[string(ev.ID)] = true
	}

	// 分页回放：每页 2 条，直到 truncated=false。收集全部事件。
	var got []contracts.Event
	from := int64(1)
	for pages := 0; pages < 1000; pages++ {
		var v eventsResp
		if code := getJSON(t, fmt.Sprintf("%s/api/events?from=%d&limit=2", srv.URL, from), &v); code != http.StatusOK {
			t.Fatalf("回放状态码 = %d", code)
		}
		if v.Watermark <= 0 {
			t.Fatalf("回放应带正水位，得到 %d", v.Watermark)
		}
		got = append(got, v.Events...)
		if !v.Truncated {
			break
		}
		if len(v.Events) == 0 {
			t.Fatalf("truncated=true 却回了 0 条（翻页会死循环）")
		}
		last := v.Events[len(v.Events)-1]
		from = *last.Seq + 1
	}

	// 断言①：并集 = 基线（无空洞、无重复、无多出）。
	if len(got) != len(base) {
		t.Fatalf("分页回放共 %d 条，基线 %d 条（数量不符）", len(got), len(base))
	}
	seen := make(map[string]bool, len(got))
	for i, ev := range got {
		if !baseIDs[string(ev.ID)] {
			t.Fatalf("回放第 %d 条 id=%s 不在基线里（多出/杜撰）", i, ev.ID)
		}
		if seen[string(ev.ID)] {
			t.Fatalf("回放出现重复 id=%s（翻页重叠）", ev.ID)
		}
		seen[string(ev.ID)] = true
	}
	// 断言②：按 seq 严格升序 + 首尾对齐（区间无空洞：本测试唯一 id → seq 稠密）。
	for i := 1; i < len(got); i++ {
		if *got[i].Seq <= *got[i-1].Seq {
			t.Fatalf("回放未按 seq 升序: [%d]=%d 然后 [%d]=%d", i-1, *got[i-1].Seq, i, *got[i].Seq)
		}
	}
	if *got[0].Seq != *base[0].Seq || *got[len(got)-1].Seq != *base[len(base)-1].Seq {
		t.Fatalf("回放首尾 seq 与基线不符: got [%d,%d] base [%d,%d]",
			*got[0].Seq, *got[len(got)-1].Seq, *base[0].Seq, *base[len(base)-1].Seq)
	}
}

// TestEventsReplayWindowAndFilters（G3）：to 上界 + session_id 投影 + 参数校验。
func TestEventsReplayWindowAndFilters(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "events.db")
	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("replay"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	h, err := host.New(host.Config{
		DBPath: dbPath, Model: "scripted", MaxTurns: 4, Allow: []string{"read"},
		CWD:      tempRepoOrDir(t),
		Provider: &scriptedProvider{args: `{"path":"` + filepath.ToSlash(target) + `"}`},
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	sid := runOneRound(t, h, "读一下")

	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// to 上界：from=1&to=2 → 只回 seq ≤ 2 的事件，且 truncated=false（追到上界即止）。
	var win eventsResp
	if code := getJSON(t, srv.URL+"/api/events?from=1&to=2", &win); code != http.StatusOK {
		t.Fatalf("窗口读状态码 = %d", code)
	}
	for _, ev := range win.Events {
		if *ev.Seq > 2 {
			t.Fatalf("to=2 却返回了 seq=%d", *ev.Seq)
		}
	}
	if win.Truncated {
		t.Fatalf("to=2 追到上界即止，不该报 truncated")
	}
	if win.To != 2 {
		t.Fatalf("响应 to 应回显 2，得到 %d", win.To)
	}

	// session_id 投影：只回该 session 的事件。
	var filtered eventsResp
	if code := getJSON(t, srv.URL+"/api/events?session_id="+string(sid), &filtered); code != http.StatusOK {
		t.Fatalf("session 投影状态码 = %d", code)
	}
	if len(filtered.Events) == 0 {
		t.Fatalf("session %s 应有事件", sid)
	}
	for _, ev := range filtered.Events {
		if ev.SessionID != sid {
			t.Fatalf("session_id 投影漏了别 session 的事件: %+v", ev)
		}
	}

	// 参数校验：非法 from / limit / session_id → 400。
	var te contracts.TransportError
	for _, bad := range []string{
		"/api/events?from=-1",
		"/api/events?from=abc",
		"/api/events?to=-5",
		"/api/events?limit=xyz",
		"/api/events?session_id=not-a-ulid",
	} {
		if code := getJSON(t, srv.URL+bad, &te); code != http.StatusBadRequest {
			t.Fatalf("%s 状态码 = %d，want 400", bad, code)
		}
	}

	// limit 硬上限：limit=999999 被夹到 maxEventsLimit，不会把内存吃满（能正常返回）。
	var capped eventsResp
	if code := getJSON(t, srv.URL+"/api/events?from=1&limit=99999999", &capped); code != http.StatusOK {
		t.Fatalf("大 limit 状态码 = %d，want 200（夹到硬上限）", code)
	}
}

// TestEventsLimitZeroDoesNotDeadLoop（P3-8 钉子）：limit=0 曾回 {"events":[],"truncated":true}
// ——客户端按 truncated 翻页却永远拿不到新事件（**死循环**，事件窗口补不回来）。
// 现在 limit=0 钳到 1：回 1 条 + truncated（还有更多）→ 翻页能往前走。
func TestEventsLimitZeroDoesNotDeadLoop(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "events.db")
	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("replay"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	h, err := host.New(host.Config{
		DBPath: dbPath, Model: "scripted", MaxTurns: 4, Allow: []string{"read"},
		CWD:      tempRepoOrDir(t),
		Provider: &scriptedProvider{args: `{"path":"` + filepath.ToSlash(target) + `"}`},
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	runOneRound(t, h, "读一下")

	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	var v eventsResp
	if code := getJSON(t, srv.URL+"/api/events?from=1&limit=0", &v); code != http.StatusOK {
		t.Fatalf("limit=0 状态码 = %d，want 200", code)
	}
	if len(v.Events) == 0 {
		t.Fatalf("limit=0 应钳到 1（回 1 条），得到 0 条——那会让 truncated=true 的翻页死循环")
	}
	// truncated 时必须真有"更多"可取：按最后一条 seq+1 继续翻，能拿到新事件。
	if v.Truncated {
		last := v.Events[len(v.Events)-1]
		var next eventsResp
		if code := getJSON(t, fmt.Sprintf("%s/api/events?from=%d&limit=1", srv.URL, *last.Seq+1), &next); code != http.StatusOK {
			t.Fatalf("翻页状态码 = %d", code)
		}
		if len(next.Events) == 0 {
			t.Fatalf("truncated=true 却翻不到下一条（死循环）")
		}
		if *next.Events[0].Seq != *last.Seq+1 {
			t.Fatalf("翻页应接上 seq=%d，得到 %d", *last.Seq+1, *next.Events[0].Seq)
		}
	}
}

// TestEventsReplayEmptyStream：空库回放 → [] 不回 null（前端少判一次空）。
func TestEventsReplayEmptyStream(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "events.db")
	h, err := host.New(host.Config{DBPath: dbPath, Model: "scripted", CWD: tempRepoOrDir(t)})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	var v eventsResp
	if code := getJSON(t, srv.URL+"/api/events?from=1", &v); code != http.StatusOK {
		t.Fatalf("状态码 = %d", code)
	}
	if v.Events == nil {
		t.Fatalf("空流应回 [] 不是 null")
	}
	if len(v.Events) != 0 {
		t.Fatalf("空流应回 0 条，得到 %d", len(v.Events))
	}
}
