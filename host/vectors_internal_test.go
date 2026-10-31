package host

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/runtime/store/vecmem"
)

// 编译期证明（G2d）：装配进 hub 的内存实现**同时**落在向量族读面与写面上
// （一个实例两面；换持久化实现消费方零改动，G2b）。
var (
	_ storage.Retriever   = (*vecmem.Store)(nil)
	_ storage.VectorIndex = (*vecmem.Store)(nil)
)

// internalHub 起一个内部可见的 hub（能摸到 h.reg / h.vecs 做装配断言）。
func internalHub(t *testing.T, muts ...func(*Config)) *Hub {
	t.Helper()
	cfg := Config{
		DBPath:   filepath.Join(t.TempDir(), "events.db"),
		Model:    "scripted",
		System:   "internal",
		MaxTurns: 4,
		Allow:    []string{"read"},
		CWD:      t.TempDir(),
	}
	for _, mut := range muts {
		if mut != nil {
			mut(&cfg)
		}
	}
	h, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// TestVectorSearchToolVisibleOnlyWhenConfigured：**酸测试（装配面）**——
// 只有"开了向量 + 配了 embedding 提供者"才注册 vector_search；否则可见集里没有它
// （R5：不暴露即不存在），且没有提供者时库里连条目都进不去。
func TestVectorSearchToolVisibleOnlyWhenConfigured(t *testing.T) {
	// ① 未开向量：工具不注册、vecs 不建。
	h := internalHub(t)
	if _, ok := h.reg.Get(ToolVectorSearch); ok {
		t.Fatal("未开向量时 vector_search 不该注册")
	}
	if h.vecs != nil || h.vecIndex != nil {
		t.Fatal("未开向量时不该建向量库")
	}

	// ② 开了向量但没配提供者：库建了，工具**仍不注册**（模型给了 query 也变不出向量）。
	h = internalHub(t, func(c *Config) { c.VectorsEnabled = true })
	if _, ok := h.reg.Get(ToolVectorSearch); ok {
		t.Fatal("未配 embedding 提供者时 vector_search 不该注册")
	}
	if h.vecs == nil || h.vecIndex == nil {
		t.Fatal("开了向量就该建库（读面 + 写面）")
	}
	if h.embed != nil {
		t.Fatal("未配提供者时 embed 槽位应为 nil")
	}
	if _, err := h.IngestVectors(context.Background(), []string{"猫"}, nil); err == nil {
		t.Fatal("没提供者就向量化不了，IngestVectors 应报未配置")
	}

	// ③ 开了向量 + 配了假提供者：工具注册（读面/写面是同一实例），执行器可用。
	h = internalHub(t, func(c *Config) {
		c.VectorsEnabled = true
		c.EmbeddingProvider = fakeEmbed{}
	})
	entry, ok := h.reg.Get(ToolVectorSearch)
	if !ok {
		t.Fatal("配了提供者时 vector_search 应注册")
	}
	if entry.Tool == nil || entry.Tool.Name != ToolVectorSearch {
		t.Fatalf("注册条目应带回完整工具描述（P2-10）: %+v", entry)
	}
	if !entry.Tool.Idempotent || entry.Tool.SideEffect != contracts.SideEffectNone ||
		entry.Tool.Replay != contracts.ReplayKindSafe {
		t.Fatal("vector_search 是幂等只读工具（side_effect=none / idempotent=true / replay=safe）")
	}
	if _, ok := entry.Impl.(core.ToolExecutor); !ok {
		t.Fatalf("注册的应是 core.ToolExecutor，得到 %T", entry.Impl)
	}
	// 注册表里它以"工具"kind 出现（与 read/write/bash 同一入口）。
	var found bool
	for _, e := range h.reg.List(registry.ExtensionKindTool) {
		if e.Meta.Name == ToolVectorSearch {
			found = true
		}
	}
	if !found {
		t.Fatal("vector_search 应以工具 kind 出现在注册表")
	}
	// 同一实例提供两面（vecmem 的构造形态）。
	if h.vecs == nil || h.vecIndex == nil {
		t.Fatal("读面/写面都该在")
	}
}

// TestVectorSearchToolExecutes：注入 fake 提供者 → 工具执行返回命中（模型给 query 文本）。
func TestVectorSearchToolExecutes(t *testing.T) {
	ctx := context.Background()
	h := internalHub(t, func(c *Config) {
		c.VectorsEnabled = true
		c.EmbeddingProvider = fakeEmbed{}
	})
	ids, err := h.IngestVectors(ctx, []string{"猫在垫子上", "狗在院子里"}, []string{"e:11", "e:12"})
	if err != nil {
		t.Fatalf("IngestVectors: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("应落 2 条（得到 %d）", len(ids))
	}
	entry, ok := h.reg.Get(ToolVectorSearch)
	if !ok {
		t.Fatal("vector_search 应注册")
	}
	exec := entry.Impl.(core.ToolExecutor)
	res, err := exec.Execute(ctx, contracts.ToolCallIntentPayload{
		ToolCallID: "toolu_vec_1", ToolName: ToolVectorSearch, Arguments: json.RawMessage(`{"query":"猫","k":1}`),
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	text := firstTextForInternal(res)
	if !strings.Contains(text, "e:11") {
		t.Fatalf("检索结果应含来源指针 e:11: %q", text)
	}
	if !strings.Contains(text, "同一次检索内") {
		t.Fatalf("结果文案应交代 Score 口径（跨实现不可比）: %q", text)
	}
	// 空 query → 报错（不静默返回空）。
	if _, err := exec.Execute(ctx, contracts.ToolCallIntentPayload{
		ToolCallID: "toolu_vec_2", ToolName: ToolVectorSearch, Arguments: json.RawMessage(`{"query":"  "}`),
	}); err == nil {
		t.Fatal("空 query 应报错")
	}
	// 坏 JSON 参数 → 报错（不猜）。
	if _, err := exec.Execute(ctx, contracts.ToolCallIntentPayload{
		ToolCallID: "toolu_vec_3", ToolName: ToolVectorSearch, Arguments: json.RawMessage(`not-json`),
	}); err == nil {
		t.Fatal("坏参数应报错")
	}
}

// TestVectorStoreClearForRebuild：写面 Clear 供 Rebuild（G24②）——清空后无命中。
func TestVectorStoreClearForRebuild(t *testing.T) {
	ctx := context.Background()
	h := internalHub(t, func(c *Config) {
		c.VectorsEnabled = true
		c.EmbeddingProvider = fakeEmbed{}
	})
	if _, err := h.IngestVectors(ctx, []string{"猫"}, []string{"e:1"}); err != nil {
		t.Fatalf("IngestVectors: %v", err)
	}
	if h.vecIndex == nil {
		t.Fatal("写面应在")
	}
	if err := h.vecIndex.Clear(ctx); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	hits, err := h.SearchVectors(ctx, "猫", 0, nil)
	if err != nil {
		t.Fatalf("Clear 后 SearchVectors: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("Clear 后应无命中（得到 %d）", len(hits))
	}
}

// fakeEmbed 是内部测试用的假 embedding 提供者（三维、确定性）。
type fakeEmbed struct{}

func (fakeEmbed) Embed(_ context.Context, text string) ([]float32, error) {
	switch {
	case strings.Contains(text, "猫"):
		return []float32{1, 0, 0}, nil
	case strings.Contains(text, "狗"):
		return []float32{0.9, 0.1, 0}, nil
	default:
		return []float32{0, 1, 0}, nil
	}
}

// blockingVectorIndex 是**可卡住/可失败**的写面（重建窗口测试用）：把 Upsert 卡在
// 重建的 Clear 之后，观察"重建窗口内检索看到什么"。
type blockingVectorIndex struct {
	inner storage.VectorIndex
	// upsertEntered：Upsert 被调到的信号（缓冲 1，非阻塞发送）。
	upsertEntered chan struct{}
	// upsertRelease：放行 Upsert 的信号（nil = 不卡）。
	upsertRelease chan struct{}
	// failUpsert：Upsert 恒失败（模拟重建写失败）。
	failUpsert bool
	// failClear：Clear 恒失败（模拟清空失败 → 库保持原样）。
	failClear bool
}

func (b *blockingVectorIndex) Upsert(ctx context.Context, vs []storage.Vector) error {
	if b.upsertEntered != nil {
		select {
		case b.upsertEntered <- struct{}{}:
		default:
		}
	}
	if b.upsertRelease != nil {
		select {
		case <-b.upsertRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if b.failUpsert {
		return errors.New("测试用：Upsert 恒失败")
	}
	return b.inner.Upsert(ctx, vs)
}

func (b *blockingVectorIndex) Delete(ctx context.Context, ids []contracts.Ulid) error {
	return b.inner.Delete(ctx, ids)
}

func (b *blockingVectorIndex) Clear(ctx context.Context) error {
	if b.failClear {
		return errors.New("测试用：Clear 恒失败")
	}
	return b.inner.Clear(ctx)
}

// TestVectorRebuildWindowReportsRebuilding：**重建窗口内检索回"重建中"**（复审轮 C3 钉子）——
//
// RebuildVectors = Clear + Upsert **两步**：不加闸时，窗口内的检索会看到**空库**
// （清完还没灌完）——那长得像"库里没有相关内容"（RAG 里最误导的一种空）。宿主加了重建闸
// （vectorGate）：重建跨 Clear+Upsert 持写锁，窗口内的检索**如实报"重建中"**。
//
// 本测试用一个卡在 Upsert 的写面把重建停在窗口里：
//   - 窗口内检索 → errVectorRebuilding（**不是**空结果）；
//   - 工具面（vector_search）同样回"重建中"（不把重建中的库当答案报给模型）；
//   - 重建放行完成后 → 检索拿到**新库**（重建前的旧条目已被清掉）。
func TestVectorRebuildWindowReportsRebuilding(t *testing.T) {
	ctx := context.Background()
	h := internalHub(t, func(c *Config) {
		c.VectorsEnabled = true
		c.EmbeddingProvider = fakeEmbed{}
	})
	// 先喂一条旧的（重建会把它清掉——重建 = 整体替换）。
	if _, err := h.IngestVectors(ctx, []string{"猫"}, []string{"e:1"}); err != nil {
		t.Fatalf("IngestVectors: %v", err)
	}

	block := &blockingVectorIndex{
		inner:         h.vecIndex,
		upsertEntered: make(chan struct{}, 1),
		upsertRelease: make(chan struct{}),
	}
	h.vecIndex = block

	done := make(chan error, 1)
	go func() {
		done <- h.RebuildVectors(ctx, []storage.Vector{{
			ID: core.NewUlid(), Embedding: []float32{1, 0, 0}, SourceRef: "e:9",
		}})
	}()

	// 等重建进到 Upsert（此时 Clear 已完成、库是空的——正是"窗口"）。
	select {
	case <-block.upsertEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("重建没进到 Upsert（窗口没打开）")
	}

	// 窗口内的检索 → "重建中"，**不是**空结果。
	if _, err := h.SearchVectors(ctx, "猫", 0, nil); !errors.Is(err, errVectorRebuilding) {
		t.Fatalf("重建窗口内检索应回\"重建中\"（得到 %v）", err)
	}
	// 工具面同理（不把重建中的库当答案报给模型）。
	entry, ok := h.reg.Get(ToolVectorSearch)
	if !ok {
		t.Fatal("vector_search 应注册")
	}
	exec := entry.Impl.(core.ToolExecutor)
	if _, err := exec.Execute(ctx, contracts.ToolCallIntentPayload{
		ToolCallID: "toolu_vec_rb", ToolName: ToolVectorSearch, Arguments: json.RawMessage(`{"query":"猫"}`),
	}); !errors.Is(err, errVectorRebuilding) {
		t.Fatalf("重建窗口内工具面应回\"重建中\"（得到 %v）", err)
	}
	// HTTP 面同理：503 + 错误码 vector_rebuilding（**不是** 200 空 hits）。
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)
	code, body := getJSONRaw(t, srv.URL+"/api/vectors/search?q=%E7%8C%AB")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("重建窗口内 HTTP 检索应 503（得到 %d）", code)
	}
	if !strings.Contains(body, "vector_rebuilding") {
		t.Fatalf("错误体应带错误码 vector_rebuilding（得到 %q）", body)
	}

	// 放行重建 → 完成。
	close(block.upsertRelease)
	if err := <-done; err != nil {
		t.Fatalf("RebuildVectors: %v", err)
	}
	hits, err := h.SearchVectors(ctx, "猫", 0, nil)
	if err != nil {
		t.Fatalf("重建完成后 SearchVectors: %v", err)
	}
	if len(hits) != 1 || hits[0].SourceRef != "e:9" {
		t.Fatalf("重建后应只剩新库那一条（旧的已被清掉）: %+v", hits)
	}
}

// TestVectorRebuildFailureSemantics：**重建失败后的库状态**（复审轮 C3 钉子）——
//
// Clear+Upsert **不是原子替换**（没有 staging + swap），失败口径必须写明：
//   - Clear 成功、Upsert 失败 → 库**为空**（vecmem 整批不落；不是半重建）；
//   - Clear 失败 → 库**保持原样**（**可能非空**——旧内容还在）；
//   - 两种都不是"半重建的库"（库里不会留一半新一半旧）。
func TestVectorRebuildFailureSemantics(t *testing.T) {
	ctx := context.Background()
	newHub := func(t *testing.T) *Hub {
		t.Helper()
		h := internalHub(t, func(c *Config) {
			c.VectorsEnabled = true
			c.EmbeddingProvider = fakeEmbed{}
		})
		if _, err := h.IngestVectors(ctx, []string{"猫"}, []string{"e:1"}); err != nil {
			t.Fatalf("IngestVectors: %v", err)
		}
		return h
	}
	items := []storage.Vector{{ID: core.NewUlid(), Embedding: []float32{1, 0, 0}, SourceRef: "e:9"}}

	// ① Upsert 失败（Clear 已生效）→ 库为空。
	h := newHub(t)
	h.vecIndex = &blockingVectorIndex{inner: h.vecIndex, failUpsert: true}
	if err := h.RebuildVectors(ctx, items); err == nil {
		t.Fatal("重建失败应报错")
	}
	hits, err := h.SearchVectors(ctx, "猫", 0, nil)
	if err != nil {
		t.Fatalf("重建失败后 SearchVectors: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("Clear 成功 + Upsert 失败 → 库应为空（得到 %d 条；半重建更糟）", len(hits))
	}

	// ② Clear 失败 → 库保持原样（**可能非空**：旧条目还在——不是被清成空库）。
	h2 := newHub(t)
	h2.vecIndex = &blockingVectorIndex{inner: h2.vecIndex, failClear: true}
	if err := h2.RebuildVectors(ctx, items); err == nil {
		t.Fatal("Clear 失败应报错")
	}
	hits, err = h2.SearchVectors(ctx, "猫", 0, nil)
	if err != nil {
		t.Fatalf("Clear 失败后 SearchVectors: %v", err)
	}
	if len(hits) != 1 || hits[0].SourceRef != "e:1" {
		t.Fatalf("Clear 失败 → 库应保持原样（旧条目还在；得到 %+v）", hits)
	}
}

// firstTextForInternal 取 ToolResult 的第一个文本块。
func firstTextForInternal(res contracts.ToolResult) string {
	for _, p := range res.Content {
		if p.Type == "text" && p.Text != nil {
			return *p.Text
		}
	}
	return ""
}

// getJSONRaw 发一个 GET 并返回状态码 + 原始响应体（内部测试用；错误体要全量校验）。
func getJSONRaw(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url) //nolint:gosec // 测试用本地 httptest URL
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读响应体: %v", err)
	}
	return resp.StatusCode, string(b)
}
