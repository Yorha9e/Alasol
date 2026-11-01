package host_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/store/vecmem"
)

// fakeEmbedder 是一个**确定性**的假 embedding 提供者（测试注入 Config.EmbeddingProvider）。
//
// 规则刻意简单可算：文本 → 三维向量（"猫"/"狗" 同向，"鸟" 正交，其余反向）——
// 于是"和猫最像的是狗"这件事可以被测试断言，而不必相信任何真实模型。
type fakeEmbedder struct {
	calls int
}

func (f *fakeEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	f.calls++
	switch {
	case strings.Contains(text, "猫"):
		return []float32{1, 0, 0}, nil
	case strings.Contains(text, "狗"):
		return []float32{0.9, 0.1, 0}, nil
	case strings.Contains(text, "鸟"):
		return []float32{0, 1, 0}, nil
	default:
		return []float32{-1, 0, 0}, nil
	}
}

// errEmbedder 是恒失败的假提供者（ingest/search 的 500 路径）。
type errEmbedder struct{}

func (errEmbedder) Embed(context.Context, string) ([]float32, error) {
	return nil, context.DeadlineExceeded
}

// vectorsHub 起一个开了向量引擎 + 注了假提供者的 hub。
func vectorsHub(t *testing.T, topK int, embed host.EmbeddingProvider) *host.Hub {
	t.Helper()
	return newTestHub(t, func(c *host.Config) {
		c.VectorsEnabled = true
		c.VectorsTopK = topK
		c.EmbeddingProvider = embed
	})
}

// TestVectorsIngestSearchHostMethods：注入 fake 提供者 → 喂数据面 + 检索面都通（RAG 接入）。
func TestVectorsIngestSearchHostMethods(t *testing.T) {
	ctx := context.Background()
	emb := &fakeEmbedder{}
	h := vectorsHub(t, 0, emb)

	// 喂数据面：host 方法（vector_index 的宿主入口）。
	ids, err := h.IngestVectors(ctx, []string{"猫在垫子上", "狗在院子里", "鸟在树上"},
		[]string{"e:11", "e:12", "e:13"})
	if err != nil {
		t.Fatalf("IngestVectors: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("应落 3 条（得到 %d）", len(ids))
	}
	if emb.calls != 3 {
		t.Fatalf("三条文本应各调一次提供者（得到 %d）", emb.calls)
	}
	// 确定性 id：同来源同段重喂覆盖同一条（不产生重复条目）。
	ids2, err := h.IngestVectors(ctx, []string{"猫在垫子上"}, []string{"e:11"})
	if err != nil {
		t.Fatalf("重喂 IngestVectors: %v", err)
	}
	if len(ids2) != 1 || ids2[0] != ids[0] {
		t.Fatalf("同 (source_ref, 分块文本) 应派生同一个 id（覆盖语义）：%s vs %s", ids2[0], ids[0])
	}
	hits, err := h.SearchVectors(ctx, "猫", 0, nil)
	if err != nil {
		t.Fatalf("SearchVectors: %v", err)
	}
	if len(hits) != 3 {
		t.Fatalf("应返回 3 条命中（得到 %d）", len(hits))
	}
	// top-1 是"猫在垫子上"（score=1），top-2 是"狗在院子里"（同向）。
	if hits[0].SourceRef != "e:11" || hits[1].SourceRef != "e:12" {
		t.Fatalf("top-k 排序不对: %s / %s", hits[0].SourceRef, hits[1].SourceRef)
	}
	if hits[0].Score != 1 {
		t.Fatalf("自身余弦应为 1（得到 %v）", hits[0].Score)
	}
	// 空查询文本 → 报错（fail-close：空查询只会返回一堆不相关的东西）。
	if _, err := h.SearchVectors(ctx, "  ", 0, nil); err == nil {
		t.Fatal("空查询文本应报错")
	}
	// source_refs **比 texts 短** → 允许（按序号对齐，缺的记空串）。
	if _, err := h.IngestVectors(ctx, []string{"a", "b"}, []string{"e:1"}); err != nil {
		t.Fatalf("source_refs 短于 texts 应允许（缺的记空串）: %v", err)
	}
	// source_refs **比 texts 长** → 报错（来源对不上就是数据错了；ErrBadQuery → 400）。
	_, err = h.IngestVectors(ctx, []string{"a"}, []string{"e:1", "e:2"})
	if err == nil {
		t.Fatal("source_refs 比 texts 长应报错")
	}
	if !errors.Is(err, storage.ErrBadQuery) {
		t.Fatalf("长度错误应包 storage.ErrBadQuery（→HTTP 400），得到 %v", err)
	}
	// Filter 精确匹配（payload 顶层字符串字段 text）。
	one, err := h.SearchVectors(ctx, "猫", 0, map[string]string{"text": "鸟在树上"})
	if err != nil {
		t.Fatalf("带 filter 的 SearchVectors: %v", err)
	}
	if len(one) != 1 || one[0].SourceRef != "e:13" {
		t.Fatalf("filter 精确匹配应只命中 e:13: %+v", one)
	}
}

// TestVectorsDisabledZeroBehaviorChange：**酸测试**——未开向量 = 零行为变化
// （两个端点**路由不注册** → 路径认不得 → 404 not_found；喂/检索方法报"未启用"）。
// 工具不可见性见 vectors_internal_test.go。
func TestVectorsDisabledZeroBehaviorChange(t *testing.T) {
	ctx := context.Background()
	h := newTestHub(t) // 不配 VectorsEnabled（默认 false）
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	var errBody struct {
		Code string `json:"code"`
	}
	code, raw := postJSONRaw(t, srv.URL+"/api/vectors", map[string]any{"texts": []string{"a"}})
	if code != http.StatusNotFound {
		t.Fatalf("未启用向量时 ingest 端点应 404（路由不注册），得到 %d", code)
	}
	if err := json.Unmarshal([]byte(raw), &errBody); err != nil {
		t.Fatalf("错误体应是单个合法 JSON（得到 %q）: %v", raw, err)
	}
	if errBody.Code != "not_found" {
		t.Fatalf("未启用时路径不认得（错误码 not_found），得到 %q", errBody.Code)
	}
	if code := getJSON(t, srv.URL+"/api/vectors/search?q=x", nil); code != http.StatusNotFound {
		t.Fatalf("未启用向量时 search 端点应 404（路由不注册），得到 %d", code)
	}
	if _, err := h.IngestVectors(ctx, []string{"a"}, nil); err == nil {
		t.Fatal("未启用向量时 IngestVectors 应报未启用")
	}
	if _, err := h.SearchVectors(ctx, "a", 0, nil); err == nil {
		t.Fatal("未启用向量时 SearchVectors 应报未启用")
	}
}

// TestVectorsEnabledWithoutProvider：开了向量但没配 embedding 提供者 →
// 端点 503 说明、喂数据报"未配置"（零行为变化：没有任何东西能进得去库）。
func TestVectorsEnabledWithoutProvider(t *testing.T) {
	ctx := context.Background()
	h := newTestHub(t, func(c *host.Config) { c.VectorsEnabled = true })
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	if code := getJSON(t, srv.URL+"/api/vectors/search?q=x", nil); code != http.StatusServiceUnavailable {
		t.Fatalf("未配提供者时 search 应 503（写明由宿主/provider 层接），得到 %d", code)
	}
	if code := postJSON(t, srv.URL+"/api/vectors", map[string]any{"texts": []string{"a"}}, nil); code != http.StatusServiceUnavailable {
		t.Fatalf("未配提供者时 ingest 应 503，得到 %d", code)
	}
	if _, err := h.IngestVectors(ctx, []string{"a"}, nil); err == nil {
		t.Fatal("未配提供者时 IngestVectors 应报未配置提供者")
	}
}

// TestVectorsHTTPEndToEnd：ingest → search 端到端（fake 提供者；HTTP 两面）。
func TestVectorsHTTPEndToEnd(t *testing.T) {
	h := vectorsHub(t, 2, &fakeEmbedder{})
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	// ingest：texts + source_refs（并行数组；逐条向量化后 Upsert）。
	var ing struct {
		Count int      `json:"count"`
		IDs   []string `json:"ids"`
	}
	if code := postJSON(t, srv.URL+"/api/vectors",
		map[string]any{"texts": []string{"猫在垫子上", "狗在院子里", "鸟在树上"}, "source_refs": []string{"e:11", "e:12", "e:13"}},
		&ing); code != http.StatusOK {
		t.Fatalf("ingest 状态码 = %d，want 200", code)
	}
	if ing.Count != 3 || len(ing.IDs) != 3 {
		t.Fatalf("ingest 应落 3 条（count=%d ids=%v）", ing.Count, ing.IDs)
	}

	// search：query 文本 → top-k（k=0 → 宿主默认 2，故 truncated=true）。
	var got struct {
		Query string `json:"query"`
		K     int    `json:"k"`
		Hits  []struct {
			ID        string          `json:"id"`
			Score     float32         `json:"score"`
			SourceRef string          `json:"source_ref"`
			Payload   json.RawMessage `json:"payload"`
			Truncated bool            `json:"truncated"`
		} `json:"hits"`
	}
	if code := getJSON(t, srv.URL+"/api/vectors/search?q="+"猫", &got); code != http.StatusOK {
		t.Fatalf("search 状态码 = %d，want 200", code)
	}
	if got.Query != "猫" || len(got.Hits) != 2 {
		t.Fatalf("search 应取默认 top-k=2 条（query=%q hits=%d）", got.Query, len(got.Hits))
	}
	if got.Hits[0].SourceRef != "e:11" || got.Hits[1].SourceRef != "e:12" {
		t.Fatalf("top-k 排序不对: %s / %s", got.Hits[0].SourceRef, got.Hits[1].SourceRef)
	}
	if !got.Hits[0].Truncated {
		t.Fatal("候选 3 条、默认 k=2 → Truncated 必须诚实置位")
	}
	if !strings.Contains(string(got.Hits[0].Payload), "猫在垫子上") {
		t.Fatalf("payload 应回显分块原文: %q", string(got.Hits[0].Payload))
	}

	// k=5（>候选 3）→ 不截断；filter 精确匹配一条。
	if code := getJSON(t, srv.URL+"/api/vectors/search?q=%E7%8C%AB&k=5", &got); code != http.StatusOK || len(got.Hits) != 3 {
		t.Fatalf("k=5 应取全部 3 条（hits=%d code=%d）", len(got.Hits), code)
	}
	if got.Hits[0].Truncated {
		t.Fatal("k=5 > 候选 3 → 不该截断")
	}
	if code := getJSON(t, srv.URL+`/api/vectors/search?q=猫&k=5&filter={"text":"狗在院子里"}`, &got); code != http.StatusOK ||
		len(got.Hits) != 1 || got.Hits[0].SourceRef != "e:12" {
		t.Fatalf("filter 精确匹配应只命中 e:12（hits=%d code=%d）", len(got.Hits), code)
	}

	// 400：q 空 / k 负 / filter 不是合法 JSON 对象。
	if code := getJSON(t, srv.URL+"/api/vectors/search?q=", nil); code != http.StatusBadRequest {
		t.Fatalf("空 q 应 400（得到 %d）", code)
	}
	if code := getJSON(t, srv.URL+"/api/vectors/search?q=x&k=-1", nil); code != http.StatusBadRequest {
		t.Fatalf("负 k 应 400（得到 %d）", code)
	}
	if code := getJSON(t, srv.URL+"/api/vectors/search?q=x&filter=notjson", nil); code != http.StatusBadRequest {
		t.Fatalf("坏 filter 应 400（得到 %d）", code)
	}
	// ingest 400：texts 空 / source_refs 更长 / 体坏。
	if code := postJSON(t, srv.URL+"/api/vectors", map[string]any{"texts": []string{}}, nil); code != http.StatusBadRequest {
		t.Fatalf("空 texts 应 400（得到 %d）", code)
	}
	if code := postJSON(t, srv.URL+"/api/vectors", map[string]any{"texts": []string{"a"}, "source_refs": []string{"e:1", "e:2"}}, nil); code != http.StatusBadRequest {
		t.Fatalf("source_refs 更长应 400（得到 %d）", code)
	}
	if code := postJSON(t, srv.URL+"/api/vectors", "not-json", nil); code != http.StatusBadRequest {
		t.Fatalf("坏体应 400（得到 %d）", code)
	}
}

// TestVectorsSourceRefsShorterThanTexts：**短的 source_refs 允许**（按序号对齐，
// 缺的来源指针记空串——"这段没有来源指针"是合法状态，不是数据错了）；
// 比 texts **更长** → 400（来源对不上就是数据错了）。
func TestVectorsSourceRefsShorterThanTexts(t *testing.T) {
	h := vectorsHub(t, 0, &fakeEmbedder{})
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	var ing struct {
		Count int      `json:"count"`
		IDs   []string `json:"ids"`
	}
	if code := postJSON(t, srv.URL+"/api/vectors",
		map[string]any{"texts": []string{"猫在垫子上", "狗在院子里", "鸟在树上"}, "source_refs": []string{"e:11"}},
		&ing); code != http.StatusOK {
		t.Fatalf("source_refs 短于 texts 应 200（得到 %d）", code)
	}
	if ing.Count != 3 || len(ing.IDs) != 3 {
		t.Fatalf("应落 3 条（count=%d ids=%v）", ing.Count, ing.IDs)
	}
	// 缺的来源指针记空串（不报错、也不"借用"上一条的 ref）。
	var got struct {
		Hits []struct {
			ID        string `json:"id"`
			SourceRef string `json:"source_ref"`
		} `json:"hits"`
	}
	if code := getJSON(t, srv.URL+"/api/vectors/search?q="+"猫", &got); code != http.StatusOK {
		t.Fatalf("search 状态码 = %d，want 200", code)
	}
	byID := map[string]string{}
	for _, hit := range got.Hits {
		byID[hit.ID] = hit.SourceRef
	}
	if byID[ing.IDs[0]] != "e:11" {
		t.Fatalf("第 0 条应带 source_ref=e:11（得到 %q）", byID[ing.IDs[0]])
	}
	if byID[ing.IDs[1]] != "" || byID[ing.IDs[2]] != "" {
		t.Fatalf("缺 ref 的条目应记空串（得到 %q / %q）", byID[ing.IDs[1]], byID[ing.IDs[2]])
	}
	// 比 texts 更长 → 400。
	if code := postJSON(t, srv.URL+"/api/vectors",
		map[string]any{"texts": []string{"a"}, "source_refs": []string{"e:1", "e:2"}}, nil); code != http.StatusBadRequest {
		t.Fatalf("source_refs 更长应 400（得到 %d）", code)
	}
}

// TestVectorEntryIDStableAcrossBatches：条目 id 由 **(source_ref, 分块文本) 哈希**派生，
// 不含批内序号——
//
//	① 两批**不同**文本（无 source_ref）→ 不撞 id、不互相覆盖（库里两条都在）；
//	② 同一段**重喂** → 同 id → 覆盖同一条（重算 embedding 的标准路径）。
//
// 曾经的 bug：id 按 (source_ref, 批内序号) 派生 → 第二批的第 0 段撞掉第一批的第 0 段
// （静默覆盖无关条目）。
func TestVectorEntryIDStableAcrossBatches(t *testing.T) {
	ctx := context.Background()
	h := vectorsHub(t, 0, &fakeEmbedder{})

	ids1, err := h.IngestVectors(ctx, []string{"猫在垫子上"}, nil)
	if err != nil {
		t.Fatalf("IngestVectors#1: %v", err)
	}
	ids2, err := h.IngestVectors(ctx, []string{"狗在院子里"}, nil)
	if err != nil {
		t.Fatalf("IngestVectors#2: %v", err)
	}
	if ids1[0] == ids2[0] {
		t.Fatalf("不同文本不该撞 id（都成了 %s）", ids1[0])
	}
	hits, err := h.SearchVectors(ctx, "猫", 10, nil)
	if err != nil {
		t.Fatalf("SearchVectors: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("两批各一条、互不覆盖（得到 %d 条）", len(hits))
	}
	// 同文本重喂 → 同 id → 覆盖（条目数不变）。
	ids3, err := h.IngestVectors(ctx, []string{"猫在垫子上"}, nil)
	if err != nil {
		t.Fatalf("重喂 IngestVectors: %v", err)
	}
	if ids3[0] != ids1[0] {
		t.Fatalf("同文本重喂应派生同一个 id（覆盖语义）：%s vs %s", ids3[0], ids1[0])
	}
	hits, err = h.SearchVectors(ctx, "猫", 10, nil)
	if err != nil {
		t.Fatalf("重喂后 SearchVectors: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("重喂应覆盖同一条而不是新增（得到 %d 条）", len(hits))
	}
}

// TestVectorSearchReportsEffectiveK：search 响应的 k 回报**生效上限**
// （k=0 = 未指定 → Config.VectorsTopK；未配 = vecmem.DefaultK=10），不是"请求里的 0"。
func TestVectorSearchReportsEffectiveK(t *testing.T) {
	var got struct {
		K int `json:"k"`
	}
	// 配了 top_k=3 → 生效上限 3。
	h := vectorsHub(t, 3, &fakeEmbedder{})
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)
	if code := getJSON(t, srv.URL+"/api/vectors/search?q="+"猫", &got); code != http.StatusOK {
		t.Fatalf("search 状态码 = %d，want 200", code)
	}
	if got.K != 3 {
		t.Fatalf("k=0 未指定 → 应回报生效上限 3（得到 %d）", got.K)
	}
	// 显式 k → 就是它。
	if code := getJSON(t, srv.URL+"/api/vectors/search?q="+"猫&k=7", &got); code != http.StatusOK {
		t.Fatalf("search 状态码 = %d，want 200", code)
	}
	if got.K != 7 {
		t.Fatalf("显式 k=7 应回报 7（得到 %d）", got.K)
	}
	// 未配 top_k → 框架默认上限（vecmem.DefaultK=10）。
	h2 := vectorsHub(t, 0, &fakeEmbedder{})
	srv2 := httptest.NewServer(h2.Handler())
	t.Cleanup(srv2.Close)
	if code := getJSON(t, srv2.URL+"/api/vectors/search?q="+"猫", &got); code != http.StatusOK {
		t.Fatalf("search 状态码 = %d，want 200", code)
	}
	if got.K != vecmem.DefaultK {
		t.Fatalf("未配 top_k → 应回报默认上限 %d（得到 %d）", vecmem.DefaultK, got.K)
	}
}

// TestVectorRebuildEntrypoints：导出重建入口（ClearVectors / RebuildVectors）——
// RebuildVectors 是**自带 id** 的写入面（绕过 IngestVectors 的 id 派生：重建方自己
// 知道每段的稳定键）；ClearVectors 清空（连维度锁一起清）。
func TestVectorRebuildEntrypoints(t *testing.T) {
	ctx := context.Background()
	h := vectorsHub(t, 0, &fakeEmbedder{})
	if _, err := h.IngestVectors(ctx, []string{"猫在垫子上"}, []string{"e:11"}); err != nil {
		t.Fatalf("IngestVectors: %v", err)
	}
	// 重建：自带 id 的条目（id 由重建方分配，不走 IngestVectors 的派生）。
	items := []storage.Vector{{
		ID:        core.NewUlid(),
		Embedding: []float32{1, 0, 0},
		Payload:   json.RawMessage(`{"text":"猫在垫子上"}`),
		SourceRef: "e:99",
	}}
	if err := h.RebuildVectors(ctx, items); err != nil {
		t.Fatalf("RebuildVectors: %v", err)
	}
	hits, err := h.SearchVectors(ctx, "猫", 10, nil)
	if err != nil {
		t.Fatalf("重建后 SearchVectors: %v", err)
	}
	if len(hits) != 1 || hits[0].ID != items[0].ID || hits[0].SourceRef != "e:99" {
		t.Fatalf("重建后应只剩自带 id 的那一条: %+v", hits)
	}
	// Clear → 库空。
	if err := h.ClearVectors(ctx); err != nil {
		t.Fatalf("ClearVectors: %v", err)
	}
	hits, err = h.SearchVectors(ctx, "猫", 10, nil)
	if err != nil {
		t.Fatalf("Clear 后 SearchVectors: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("Clear 后应无命中（得到 %d 条）", len(hits))
	}
	// 未启用 → 报"未启用"（不静默成功）。
	off := newTestHub(t) // VectorsEnabled 默认 false
	if err := off.ClearVectors(ctx); err == nil {
		t.Fatal("未启用向量时 ClearVectors 应报未启用")
	}
	if err := off.RebuildVectors(ctx, items); err == nil {
		t.Fatal("未启用向量时 RebuildVectors 应报未启用")
	}
}

// driftingEmbedder 是**维度会漂移**的假提供者：第一次给 3 维，之后给 4 维
// （模拟"换了模型没重建库"——vecmem 按首条维度锁库，混维度 → ErrBadQuery）。
type driftingEmbedder struct{ n int }

func (d *driftingEmbedder) Embed(context.Context, string) ([]float32, error) {
	d.n++
	if d.n == 1 {
		return []float32{1, 0, 0}, nil
	}
	return []float32{1, 0, 0, 0}, nil
}

// TestVectorsDimensionDriftRejected：提供者维度漂移 → 写入被拒（ErrBadQuery → 400，
// 不是 500、更不是静默写进一个维度混乱的库；且错误体仍是**单个**合法 JSON——
// 防 writeVectorHubError 写两次响应头把两个错误体粘在一起）。
func TestVectorsDimensionDriftRejected(t *testing.T) {
	h := vectorsHub(t, 0, &driftingEmbedder{})
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)
	if code := postJSON(t, srv.URL+"/api/vectors", map[string]any{"texts": []string{"a"}}, nil); code != http.StatusOK {
		t.Fatalf("首批（3 维）应 200（得到 %d）", code)
	}
	var errBody struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	code, raw := postJSONRaw(t, srv.URL+"/api/vectors", map[string]any{"texts": []string{"b"}})
	if code != http.StatusBadRequest {
		t.Fatalf("换维度（4 维）应 400（ErrBadQuery，得到 %d）", code)
	}
	// 错误体必须是**单个**合法 JSON（防 writeVectorHubError 写两次响应头把两个错误体粘在
	// 一起：json.Decoder 是流式解，只解第一个值会漏判，这里用 Unmarshal 全量校验）。
	if err := json.Unmarshal([]byte(raw), &errBody); err != nil {
		t.Fatalf("错误体应是单个合法 JSON（得到 %q）: %v", raw, err)
	}
	if errBody.Code != "bad_vector_query" {
		t.Fatalf("错误码应是 bad_vector_query（得到 %q）", errBody.Code)
	}
	if code := getJSON(t, srv.URL+"/api/vectors/search?q=c", &errBody); code != http.StatusBadRequest {
		t.Fatalf("查询维度不符同样应 400（得到 %d）", code)
	}
	if errBody.Code != "bad_vector_query" {
		t.Fatalf("查询维度不符的错误码应是 bad_vector_query（得到 %q）", errBody.Code)
	}
}

// vectorIDFormula 按**文档公式**手算条目 id（host/vectors.go 的 vectorEntryID 注释 +
// 07/11 文档口径，**逐字一致**）：
//
//	id = Crockford26( SHA-256( "vec\x00" ‖ u64be(len(ref)) ‖ ref ‖ u64be(len(text)) ‖ text )[:16] )
//
// 其中 u64be(n) = n 的 8 字节大端无符号表示；‖ = 字节串拼接。
//
// 本函数**不复用实现**（手写一遍公式）——这样"文档公式能复现 id"才是被真的验过的事实，
// 而不是"实现自己跟自己一致"。
func vectorIDFormula(t *testing.T, ref, text string) string {
	t.Helper()
	buf := make([]byte, 0, len("vec\x00")+8+len(ref)+8+len(text))
	buf = append(buf, "vec\x00"...)
	var lb [8]byte
	binary.BigEndian.PutUint64(lb[:], uint64(len(ref)))
	buf = append(buf, lb[:]...)
	buf = append(buf, ref...)
	binary.BigEndian.PutUint64(lb[:], uint64(len(text)))
	buf = append(buf, lb[:]...)
	buf = append(buf, text...)
	sum := sha256.Sum256(buf)
	return core.EncodeCrockford26(sum[:16])
}

// TestVectorEntryIDFormulaReproducible：**按文档公式能复现 id**（复审轮 C2 钉子）——
// 测试里手算公式（见 vectorIDFormula）→ 与 IngestVectors 返回的 id 逐字比对。
//
// 覆盖各种边界输入：空 ref / 空文本 / **含 NUL 的 ref 与文本**（旧口径下这两组会撞）。
//
// **空文本这一格的口径变了（F4）**：空文本没有可嵌入的内容，宿主在**调用提供者之前**
// 就预校验拦下（包 storage.ErrBadQuery → HTTP 400 bad_query，客户端数据错，不是 500）。
// 故该格不再走"入库 → 比对 id"，而是断言**被拒**。id 派生公式对空文本仍有定义
// （vectorIDFormula），只是不再经 ingest 面。
func TestVectorEntryIDFormulaReproducible(t *testing.T) {
	ctx := context.Background()
	h := vectorsHub(t, 0, &fakeEmbedder{})

	cases := []struct {
		name    string
		ref     string
		text    string
		wantErr bool // 空文本：F4 起宿主预校验拦下（bad_query）
	}{
		{"普通", "e:11", "猫在垫子上", false},
		{"无来源指针", "", "没有来源指针的一段", false},
		{"空文本", "e:12", "", true},
		{"ref 含 NUL", "a\x00b", "c", false},
		{"text 含 NUL", "a", "b\x00c", false},
		{"两端都含 NUL", "x\x00y", "p\x00q", false},
	}
	for _, c := range cases {
		ids, err := h.IngestVectors(ctx, []string{c.text}, []string{c.ref})
		if c.wantErr {
			if err == nil {
				t.Fatalf("[%s] 空文本应被宿主预校验拦下（F4：400 bad_query，不是 500）", c.name)
			}
			if !errors.Is(err, storage.ErrBadQuery) {
				t.Fatalf("[%s] 期望 storage.ErrBadQuery，得到 %v", c.name, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("[%s] IngestVectors: %v", c.name, err)
		}
		if len(ids) != 1 {
			t.Fatalf("[%s] 应落 1 条（得到 %d）", c.name, len(ids))
		}
		want := vectorIDFormula(t, c.ref, c.text)
		if string(ids[0]) != want {
			t.Fatalf("[%s] id 与文档公式不符：实现=%s 公式=%s", c.name, string(ids[0]), want)
		}
		if !core.ValidUlid(ids[0]) {
			t.Fatalf("[%s] 派生 id 应是合法 ULID 形态: %q", c.name, string(ids[0]))
		}
	}
}

// TestVectorEntryIDNoNULAmbiguity：**裸 NUL 分隔符的歧义已消除**（复审轮 C1 钉子）——
//
// 旧口径 SHA-256("vec\x00" + ref + "\x00" + text) 的输入**不是单射**：
// (ref="a\x00b", text="c") 与 (ref="a", text="b\x00c") 拼出**同一串** → **同一个 id**
// → 后喂的那条**静默覆盖**先喂的那条（公开 HTTP 面 \u0000 可达：ref/text 都是任意字符串）。
//
// 现在两段各带长度前缀 → (ref,text) → 字节串单射 → 两组不同组合 → 两个不同 id，
// 且**库里两条都在**（不互相覆盖）。
func TestVectorEntryIDNoNULAmbiguity(t *testing.T) {
	ctx := context.Background()
	h := vectorsHub(t, 0, &fakeEmbedder{})

	ids1, err := h.IngestVectors(ctx, []string{"c"}, []string{"a\x00b"})
	if err != nil {
		t.Fatalf("IngestVectors#1: %v", err)
	}
	ids2, err := h.IngestVectors(ctx, []string{"b\x00c"}, []string{"a"})
	if err != nil {
		t.Fatalf("IngestVectors#2: %v", err)
	}
	if ids1[0] == ids2[0] {
		t.Fatalf("含 NUL 的 (ref,text) 组合撞了同一个 id（会被静默覆盖）: %s", string(ids1[0]))
	}
	hits, err := h.SearchVectors(ctx, "x", 10, nil)
	if err != nil {
		t.Fatalf("SearchVectors: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("两组不同组合应各落一条（库里 %d 条；撞 id 就只剩 1 条 = 静默覆盖）", len(hits))
	}
	// 两条的来源指针各自对应（没被覆盖成另一条）。
	refs := map[string]bool{}
	for _, hit := range hits {
		refs[hit.SourceRef] = true
	}
	if !refs["a\x00b"] || !refs["a"] {
		t.Fatalf("两条的 source_ref 应分别是 \"a\\x00b\" 与 \"a\"（得到 %v）", refs)
	}
}

// TestVectorsIngestBatchLimit：**单批条数上限**（复审轮 C4 钉子）——一条文本 = 一次
// embedding 提供者调用（批内串行），不设上限就等于让一个请求串起 N 次外部调用。
//
// 上限内（512 条）→ 200；超出（513 条）→ 400 且**点名上限**；宿主方法同样拒
// （包 storage.ErrBadQuery → HTTP 400）。
func TestVectorsIngestBatchLimit(t *testing.T) {
	h := vectorsHub(t, 0, &fakeEmbedder{})
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	texts := make([]string, 512)
	for i := range texts {
		texts[i] = fmt.Sprintf("分块 %d", i)
	}
	if code := postJSON(t, srv.URL+"/api/vectors", map[string]any{"texts": texts}, nil); code != http.StatusOK {
		t.Fatalf("512 条（上限内）应 200（得到 %d）", code)
	}
	over := append(append([]string(nil), texts...), "多出来的一条")
	code, raw := postJSONRaw(t, srv.URL+"/api/vectors", map[string]any{"texts": over})
	if code != http.StatusBadRequest {
		t.Fatalf("513 条（超上限）应 400（得到 %d）", code)
	}
	if !strings.Contains(raw, "512") {
		t.Fatalf("错误体应点名上限 512（得到 %q）", raw)
	}
	// 宿主方法同样拒（ErrBadQuery → 400），且**不落库**。
	if _, err := h.IngestVectors(context.Background(), over, nil); !errors.Is(err, storage.ErrBadQuery) {
		t.Fatalf("宿主方法应拒超限批次并包 storage.ErrBadQuery（得到 %v）", err)
	}
}

func postJSONRaw(t *testing.T, url string, body any) (int, string) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	resp, err := http.Post(url, "application/json", strings.NewReader(string(raw)))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读响应体: %v", err)
	}
	return resp.StatusCode, string(b)
}

// TestVectorsReopenLosesCacheByDesign：**重开语义**——内存向量库不跨进程存活
// （可整体重建的缓存：SourceRef 指回事件流，丢了重建，不丢事实 F5）。本测试把这条口径
// 钉死：重开后库是空的（不是"状态丢失 bug"），重新 ingest 立刻可用。
func TestVectorsReopenLosesCacheByDesign(t *testing.T) {
	dir := t.TempDir()
	cfg := host.Config{
		DBPath: filepath.Join(dir, "events.db"), Model: "scripted", System: "reopen", MaxTurns: 4,
		Allow: []string{"read"}, CWD: dir,
	}
	cfg.VectorsEnabled = true
	cfg.EmbeddingProvider = &fakeEmbedder{}

	h1, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New#1: %v", err)
	}
	ctx := context.Background()
	if _, err := h1.IngestVectors(ctx, []string{"猫在垫子上"}, []string{"e:11"}); err != nil {
		t.Fatalf("IngestVectors#1: %v", err)
	}
	hits, err := h1.SearchVectors(ctx, "猫", 0, nil)
	if err != nil || len(hits) != 1 {
		t.Fatalf("关库前应能检索到（hits=%d err=%v）", len(hits), err)
	}
	if err := h1.Close(); err != nil {
		t.Fatalf("Close#1: %v", err)
	}

	// 重开（同一事件库）：向量库是**空的**（不持久化），事件流/投影照旧可重建。
	h2, err := host.New(cfg)
	if err != nil {
		t.Fatalf("host.New#2: %v", err)
	}
	t.Cleanup(func() { _ = h2.Close() })
	hits, err = h2.SearchVectors(ctx, "猫", 0, nil)
	if err != nil {
		t.Fatalf("重开后 SearchVectors: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("重开后向量库应为空（可整体重建的缓存；得到 %d 条）", len(hits))
	}
	// 重新 ingest（按 SourceRef 从事件流重建的那类路径）→ 立即可用。
	if _, err := h2.IngestVectors(ctx, []string{"猫在垫子上"}, []string{"e:11"}); err != nil {
		t.Fatalf("IngestVectors#2: %v", err)
	}
	hits, err = h2.SearchVectors(ctx, "猫", 0, nil)
	if err != nil || len(hits) != 1 || hits[0].SourceRef != "e:11" {
		t.Fatalf("重建后应能检索到 e:11（hits=%+v err=%v）", hits, err)
	}
}

// TestVectorsProviderFailure：提供者恒失败 → 500（不静默返回空结果冒充"没有命中"）。
func TestVectorsProviderFailure(t *testing.T) {
	h := vectorsHub(t, 0, errEmbedder{})
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)
	if code := getJSON(t, srv.URL+"/api/vectors/search?q=x", nil); code != http.StatusInternalServerError {
		t.Fatalf("提供者失败应 500（得到 %d）", code)
	}
	if code := postJSON(t, srv.URL+"/api/vectors", map[string]any{"texts": []string{"a"}}, nil); code != http.StatusInternalServerError {
		t.Fatalf("ingest 提供者失败应 500（得到 %d）", code)
	}
}

// TestVectorsTopKConfigRejected：负 top_k = 非法配置（启动即拒，不静默改成框架默认）。
func TestVectorsTopKConfigRejected(t *testing.T) {
	_, err := host.New(host.Config{
		DBPath:         filepath.Join(t.TempDir(), "events.db"),
		Model:          "scripted",
		Addr:           "127.0.0.1:0",
		VectorsEnabled: true,
		VectorsTopK:    -1,
	})
	if err == nil {
		t.Fatal("负 VectorsTopK 应拒绝装配（fail-close）")
	}
	if !strings.Contains(err.Error(), "VectorsTopK") {
		t.Fatalf("报错应点名 VectorsTopK: %v", err)
	}
}

// TestSetWithVecmemComposition：core/storage.Set 挂上真实 vecmem 实现并取用（组合面；
// Set 不改形状，换实现零改动——本测试证明向量槽位接的就是可用实现，且写面经**带 ok 的
// 取面器**拿（不许硬转型：只挂读面是合法组合）。
func TestSetWithVecmemComposition(t *testing.T) {
	ctx := context.Background()
	st := vecmem.New(0)
	set := storage.Set{Vectors: st}

	// Kinds 报"挂了 vectors"（诊断/酸测试面）。
	if kinds := set.Kinds(); len(kinds) != 1 || kinds[0] != "vectors" {
		t.Fatalf("Set.Kinds() = %v，want [vectors]", kinds)
	}
	// 同一实例提供写面 → 取面器 ok=true。
	w, ok := set.VectorWriter()
	if !ok {
		t.Fatal("vecmem 同时实现读面与写面，VectorWriter() 应 ok=true")
	}
	id := core.NewUlid()
	if err := w.Upsert(ctx, []storage.Vector{{ID: id, Embedding: []float32{1, 0, 0}, SourceRef: "e:1"}}); err != nil {
		t.Fatalf("Set.VectorWriter().Upsert: %v", err)
	}
	// 经 Set.Vectors（接口面）检索：top-k 命中刚写的那条（SourceRef 是可重建锚）。
	hits, err := set.Vectors.Retrieve(ctx, storage.VectorQuery{Embedding: []float32{1, 0, 0}, K: 1})
	if err != nil {
		t.Fatalf("Set.Vectors.Retrieve: %v", err)
	}
	if len(hits) != 1 || hits[0].ID != id || hits[0].SourceRef != "e:1" {
		t.Fatalf("Set 向量槽位往返不对: %+v", hits)
	}
	// 只读实现（只有读面）→ ok=false（不许硬转型 panic：只挂读面是合法组合）。
	ro := storage.Set{Vectors: readOnlyRetriever{}}
	if _, ok := ro.VectorWriter(); ok {
		t.Fatal("只读实现的 VectorWriter() 应 ok=false（消费方据此跳过写路径）")
	}
}

// readOnlyRetriever 是只有读面的向量实现（视图/只读副本的缩影；证明"读面必有、写面有则用"）。
type readOnlyRetriever struct{}

func (readOnlyRetriever) Retrieve(context.Context, storage.VectorQuery) ([]storage.VectorHit, error) {
	return nil, nil
}

// TestVectorsFileConfigRoundTrip：TOML `[vectors]` 段装载（未知键仍 fail-close）。
func TestVectorsFileConfigRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte("[vectors]\nenabled = true\ntop_k = 7\n"), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	fc, err := host.LoadFileConfig(path)
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if fc == nil || !fc.Vectors.Enabled || fc.Vectors.TopK != 7 {
		t.Fatalf("[vectors] 段没装载对: %+v", fc)
	}
	// 未知键 fail-close（F3）：TOML 会静默忽略没被认领的键 → 装载期点名。
	bad := filepath.Join(dir, "bad.toml")
	if err := os.WriteFile(bad, []byte("[vectors]\ndir = \"C:\\\\tmp\"\n"), 0o600); err != nil {
		t.Fatalf("写坏配置: %v", err)
	}
	if _, err := host.LoadFileConfig(bad); err == nil || !strings.Contains(err.Error(), "vectors.dir") {
		t.Fatalf("未知键 vectors.dir 应 fail-close 点名: %v", err)
	}
}
