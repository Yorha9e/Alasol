package models

import (
	"context"
	"errors"
	"testing"
	"time"
)

// mockFetch 返回预设 id 列表或错误（在线拉取的 mock）。
type mockFetch struct {
	ids []string
	err error
}

func (m mockFetch) Fetch(ctx context.Context) ([]string, error) {
	return m.ids, m.err
}

// asFetchFunc 把 mockFetch 包成 FetchFunc。
func asFetchFunc(m mockFetch) FetchFunc { return m.Fetch }

// findModel 在目录里按 id 找条目。
func findModel(ms []Model, id string) (Model, bool) {
	for _, m := range ms {
		if m.ID == id {
			return m, true
		}
	}
	return Model{}, false
}

// TestBuiltinSnapshotPresent 验证内置快照非空、含常见模型、按 ID 排序前不重复。
func TestBuiltinSnapshotPresent(t *testing.T) {
	snap := BuiltinSnapshot()
	if len(snap) == 0 {
		t.Fatal("内置快照不应为空")
	}
	for _, id := range []string{"gpt-4o", "gpt-4o-mini", "claude-3-5-sonnet-20241022"} {
		if _, ok := findModel(snap, id); !ok {
			t.Fatalf("内置快照缺常见模型 %q", id)
		}
	}
	seen := map[string]bool{}
	for _, m := range snap {
		if m.ID == "" {
			t.Fatalf("内置条目 id 不应为空: %+v", m)
		}
		if seen[m.ID] {
			t.Fatalf("内置快照有重复 id: %q", m.ID)
		}
		seen[m.ID] = true
	}
}

// TestCatalogRefreshMergesOnline 验证刷新合并：内置（带元数据）∪ 在线 id（去重补裸条目）。
func TestCatalogRefreshMergesOnline(t *testing.T) {
	c := New(nil, asFetchFunc(mockFetch{ids: []string{
		"gpt-4o",              // 内置已有 → 不重复，保留内置元数据
		"some-endpoint-model", // 在线独有 → 补裸条目
		"another-model",
	}}))
	// 刷新前：只有内置快照。
	if _, ok := findModel(c.Models(), "some-endpoint-model"); ok {
		t.Fatal("刷新前不该有在线模型")
	}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	ms := c.Models()
	// 内置的 gpt-4o 保留元数据（Name 非空、Builtin=true）。
	gpt, ok := findModel(ms, "gpt-4o")
	if !ok || gpt.Name == "" || !gpt.Builtin {
		t.Fatalf("内置 gpt-4o 元数据应保留: %+v", gpt)
	}
	// 在线独有的补成裸条目（Builtin=false）。
	online, ok := findModel(ms, "some-endpoint-model")
	if !ok || online.Builtin {
		t.Fatalf("在线独有模型应为裸条目: %+v", online)
	}
	if _, ok := findModel(ms, "another-model"); !ok {
		t.Fatal("在线 another-model 应被合并")
	}
	// 不重复：gpt-4o 只出现一次。
	count := 0
	for _, m := range ms {
		if m.ID == "gpt-4o" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("gpt-4o 应只出现一次，得到 %d", count)
	}
	// 排序确定性：按 ID 升序。
	for i := 1; i < len(ms); i++ {
		if ms[i-1].ID > ms[i].ID {
			t.Fatalf("目录应按 ID 排序: %q > %q", ms[i-1].ID, ms[i].ID)
		}
	}
}

// TestCatalogRefreshErrorKeepsPrevious 验证在线失败不清空已有结果（韧性）。
func TestCatalogRefreshErrorKeepsPrevious(t *testing.T) {
	good := asFetchFunc(mockFetch{ids: []string{"online-a"}})
	c := New(nil, good)
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatalf("首次 Refresh: %v", err)
	}
	// 换成会失败的 fetch，刷新应返回 error 但保留之前合并的 online-a。
	c.fetch = asFetchFunc(mockFetch{err: errors.New("endpoint down")})
	if err := c.Refresh(context.Background()); err == nil {
		t.Fatal("在线失败应返回 error")
	}
	if _, ok := findModel(c.Models(), "online-a"); !ok {
		t.Fatal("在线失败不该清空已合并的 online-a")
	}
	if c.LastError() == nil {
		t.Fatal("LastError 应记录失败")
	}
}

// TestCatalogRunPeriodic 验证周期刷新循环：立即刷一次 + ctx 取消退出。
func TestCatalogRunPeriodic(t *testing.T) {
	c := New(nil, asFetchFunc(mockFetch{ids: []string{"run-model"}}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, 10*time.Millisecond) }()
	// Run 立即刷一次 → 很快能看到 run-model。
	deadline := time.Now().Add(time.Second)
	for {
		if _, ok := findModel(c.Models(), "run-model"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Run 应立即刷新一次（超时未见 run-model）")
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run 取消应返回 context.Canceled，得到 %v", err)
	}
}

// TestCatalogNilFetchOffline 验证 fetch=nil = 纯离线（只呈内置，Refresh 不报错）。
func TestCatalogNilFetchOffline(t *testing.T) {
	c := New(nil, nil)
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatalf("离线 Refresh 不应报错: %v", err)
	}
	if len(c.Models()) == 0 {
		t.Fatal("离线目录至少应有内置快照")
	}
}
