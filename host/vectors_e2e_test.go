package host_test

// 本文件是 **vector_search 链路真跑通** 的证据（v4-3-B 装配面）：真 openai.Embedder
// + httptest 假 /embeddings 端点，走完 POST /api/vectors（ingest）→ GET /api/vectors/search
// → vector_search 工具面。与 vectors_test.go 的 fakeEmbedder 测试分工：那边钉宿主行为，
// 这边钉"真客户端 + 假端点"的全链路（真机联调另算）。

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/provider/openai"
)

// hashEmbedVec 把文本哈希成**确定性**向量（同文本同向量；不同文本几乎必然不同向量）。
// 维度 dim 固定；值域 [-1,1)——够做余弦 top-k，不依赖任何真实模型。
func hashEmbedVec(text string, dim int) []float32 {
	sum := sha256.Sum256([]byte(text))
	out := make([]float32, dim)
	for i := range out {
		out[i] = float32(int(sum[i%len(sum)])-128) / 128
	}
	return out
}

// fakeEmbeddingsServer 起一个假 /embeddings 端点：按 input 文本确定性生成向量，
// 并记录每个请求的 model/input/Authorization 供断言。
type fakeEmbeddingsServer struct {
	mu       sync.Mutex
	models   []string
	inputs   []string
	auths    []string
	dim      int
	requireK string // 期望的 Authorization（空 = 不校验）
}

func newFakeEmbeddingsServer(t *testing.T, dim int, requireKey string) (*fakeEmbeddingsServer, *httptest.Server) {
	t.Helper()
	fs := &fakeEmbeddingsServer{dim: dim, requireK: requireKey}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/embeddings" {
			// openai.NewEmbedder 把 BaseURL 归一后直接拼 /embeddings。
			t.Errorf("请求路径 = %q，期望 /embeddings", r.URL.Path)
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if r.Method != http.MethodPost {
			t.Errorf("请求方法 = %q，期望 POST", r.Method)
		}
		var req struct {
			Model string `json:"model"`
			Input string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("解码请求体失败：%v", err)
			http.Error(w, `{"error":"bad body"}`, http.StatusBadRequest)
			return
		}
		auth := r.Header.Get("Authorization")
		fs.mu.Lock()
		fs.models = append(fs.models, req.Model)
		fs.inputs = append(fs.inputs, req.Input)
		fs.auths = append(fs.auths, auth)
		fs.mu.Unlock()
		if fs.requireK != "" && auth != "Bearer "+fs.requireK {
			t.Errorf("Authorization = %q，期望 Bearer %s", auth, fs.requireK)
		}
		emb := hashEmbedVec(req.Input, fs.dim)
		w.Header().Set("Content-Type", "application/json")
		// 形状：{"object":"list","data":[{"object":"embedding","index":0,"embedding":[...]}]}
		b, _ := json.Marshal(emb)
		w.Write([]byte(`{"object":"list","data":[{"object":"embedding","index":0,"embedding":`))
		w.Write(b)
		w.Write([]byte(`}]}`))
	}))
	t.Cleanup(srv.Close)
	return fs, srv
}

// snapshot 拷贝已记录的请求（持锁快照，避免与 handler 并发写交错）。
func (fs *fakeEmbeddingsServer) snapshot() (models, inputs, auths []string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return append([]string(nil), fs.models...),
		append([]string(nil), fs.inputs...),
		append([]string(nil), fs.auths...)
}

// TestVectorsRealEmbedderE2E：**真 openai.Embedder + httptest 假端点**跑通全链路——
// ingest（POST /api/vectors）→ search（GET /api/vectors/search）→ vector_search 工具面。
//
// 断言打到假端点的请求 body 里 model/input 正确、Authorization 头在场。
// 反面（EmbeddingProvider=nil 的零行为变化）已有酸测试覆盖（TestVectorsEnabledWithoutProvider），
// 不在这里重复。
func TestVectorsRealEmbedderE2E(t *testing.T) {
	const (
		embModel = "test-embed"
		embKey   = "sk-embed-e2e"
		dim      = 8
	)
	fs, srv := newFakeEmbeddingsServer(t, dim, embKey)

	// 真 Embedder：BaseURL 指向假端点；APIKey/Model 与 [embedding] 装配面同款。
	embedder := openai.NewEmbedder(openai.Options{
		BaseURL: srv.URL,
		APIKey:  embKey,
		Model:   embModel,
	})

	h := newTestHub(t, func(c *host.Config) {
		c.VectorsEnabled = true
		c.EmbeddingProvider = embedder
	})
	httpHub := httptest.NewServer(h.Handler())
	t.Cleanup(httpHub.Close)

	// ---- ① ingest：POST /api/vectors ----
	var ing struct {
		Count int      `json:"count"`
		IDs   []string `json:"ids"`
	}
	texts := []string{"猫在垫子上", "狗在院子里", "鸟在树上"}
	ingBody := map[string]any{"texts": texts, "source_refs": []string{"e:11", "e:12", "e:13"}}
	code, raw := postJSONRaw(t, httpHub.URL+"/api/vectors", ingBody)
	if code != http.StatusOK {
		t.Fatalf("ingest 状态码 = %d，want 200；body=%s", code, raw)
	}
	if err := json.Unmarshal([]byte(raw), &ing); err != nil {
		t.Fatalf("解 ingest 响应: %v（body=%s）", err, raw)
	}
	if ing.Count != 3 || len(ing.IDs) != 3 {
		t.Fatalf("ingest 应落 3 条（count=%d ids=%v）", ing.Count, ing.IDs)
	}

	// ---- ② search：GET /api/vectors/search（query 文本 → 真 Embedder 调假端点）----
	var got struct {
		Query string `json:"query"`
		K     int    `json:"k"`
		Hits  []struct {
			ID        string          `json:"id"`
			Score     float32         `json:"score"`
			SourceRef string          `json:"source_ref"`
			Payload   json.RawMessage `json:"payload"`
		} `json:"hits"`
	}
	// 用与 ingest 相同的文本检索：哈希向量自相似（score=1），top-1 必是对应条目
	//（中文 q 参数与 vectors_test 同款直接内联——http.NewRequest 会按 URL 解析规则处理）。
	if code := getJSON(t, httpHub.URL+"/api/vectors/search?q=猫在垫子上&k=3", &got); code != http.StatusOK {
		t.Fatalf("search 状态码 = %d，want 200", code)
	}
	if len(got.Hits) == 0 {
		t.Fatal("search 应有命中（同文本自相似）")
	}
	if got.Hits[0].SourceRef != "e:11" {
		t.Fatalf("top-1 应是 e:11（同文本），得到 %q", got.Hits[0].SourceRef)
	}

	// ---- ③ 工具面：vector_search 工具已注册且可 Execute ----
	reg := h.Registry()
	if reg == nil {
		t.Fatal("Registry() 不应为 nil")
	}
	entry, ok := reg.Get(host.ToolVectorSearch)
	if !ok {
		t.Fatal("配了 EmbeddingProvider + VectorsEnabled 时 vector_search 应注册")
	}
	exec, ok := entry.Impl.(core.ToolExecutor)
	if !ok {
		t.Fatalf("注册条目 Impl 应是 core.ToolExecutor，得到 %T", entry.Impl)
	}
	res, err := exec.Execute(context.Background(), contracts.ToolCallIntentPayload{
		ToolCallID: "toolu_vec_e2e", ToolName: host.ToolVectorSearch,
		Arguments: json.RawMessage(`{"query":"猫在垫子上","k":1}`),
	})
	if err != nil {
		t.Fatalf("vector_search Execute: %v", err)
	}
	text := toolResultText(res)
	if !strings.Contains(text, "e:11") {
		t.Fatalf("工具结果应含 e:11: %q", text)
	}

	// ---- ④ 假端点请求体/头断言：model、input、Authorization ----
	models, inputs, auths := fs.snapshot()

	if len(models) == 0 {
		t.Fatal("假端点应被打到至少一次（ingest 3 次 + search 1 次 + 工具 1 次）")
	}
	// 至少 5 次：ingest 3 条 + search 1 次 + 工具 Execute 1 次。
	if len(models) < 5 {
		t.Fatalf("假端点应被打 ≥5 次（ingest 3 + search 1 + 工具 1），得到 %d", len(models))
	}
	for i, m := range models {
		if m != embModel {
			t.Errorf("请求[%d].model = %q，期望 %q", i, m, embModel)
		}
		if auths[i] != "Bearer "+embKey {
			t.Errorf("请求[%d].Authorization = %q，期望在场的 Bearer key", i, auths[i])
		}
	}
	// input 覆盖 ingest 三段 + search/工具的查询文本。
	countOf := func(input string) int {
		n := 0
		for _, in := range inputs {
			if in == input {
				n++
			}
		}
		return n
	}
	if n := countOf("猫在垫子上"); n < 3 {
		// ingest 1 + search 1 + 工具 1 = 3；至少 3 次同文本。
		t.Errorf("input=猫在垫子上 应出现 ≥3 次（ingest+search+工具），得到 %d", n)
	}
	for _, want := range []string{"狗在院子里", "鸟在树上"} {
		if n := countOf(want); n != 1 {
			t.Errorf("input=%q 应恰好出现 1 次（ingest），得到 %d", want, n)
		}
	}
}

// toolResultText 取 ToolResult 的第一个文本块（与 vectors_test 同款辅助）。
func toolResultText(res contracts.ToolResult) string {
	for _, p := range res.Content {
		if p.Type == "text" && p.Text != nil {
			return *p.Text
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// F4：空文本 ingest → 400（不是 500）
// ---------------------------------------------------------------------------

// TestVectorsEmptyTextIngestBadRequest 钉 F4：POST /api/vectors {"texts":[""]} → **400**
// （bad_vector_query），不是 500。
//
// 两条断言：
//  1. 状态码 = 400（客户端数据错，不是"真出事了"）；
//  2. **假端点一次都没被打到**——空文本在**调用提供者之前**就被宿主拦下（不让外部调用白跑）。
//
// 变异口径：把空文本预校验去掉 → ①红（落成 500 或 502/503）、②红（端点被打到）。
func TestVectorsEmptyTextIngestBadRequest(t *testing.T) {
	fs, srv := newFakeEmbeddingsServer(t, 8, "sk-embed-empty")

	embedder := openai.NewEmbedder(openai.Options{
		BaseURL: srv.URL,
		APIKey:  "sk-embed-empty",
		Model:   "test-embed",
	})
	h := newTestHub(t, func(c *host.Config) {
		c.VectorsEnabled = true
		c.EmbeddingProvider = embedder
	})
	httpHub := httptest.NewServer(h.Handler())
	t.Cleanup(httpHub.Close)

	code, raw := postJSONRaw(t, httpHub.URL+"/api/vectors", map[string]any{"texts": []string{""}})
	if code != http.StatusBadRequest {
		t.Fatalf("空文本 ingest 状态码 = %d，want 400；body=%s", code, raw)
	}
	var errBody struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal([]byte(raw), &errBody); err != nil {
		t.Fatalf("解错误体: %v（body=%s）", err, raw)
	}
	if errBody.Code != "bad_vector_query" {
		t.Fatalf("错误码 = %q，want bad_vector_query（body=%s）", errBody.Code, raw)
	}
	if _, _, auths := fs.snapshot(); len(auths) != 0 {
		t.Fatalf("空文本不该打到 embedding 端点（预校验在调用提供者之前），却打了 %d 次", len(auths))
	}
}

// TestVectorsEmptyQuerySearchBadRequest 钉 F4 的检索侧：宿主方法 SearchVectors 对空
// 查询回**客户端数据错**（包 storage.ErrBadQuery → 400），不是 500。
func TestVectorsEmptyQuerySearchBadRequest(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) {
		c.VectorsEnabled = true
		c.EmbeddingProvider = &fakeEmbedder{}
	})
	if _, err := h.SearchVectors(context.Background(), "   ", 0, nil); !errors.Is(err, storage.ErrBadQuery) {
		t.Fatalf("空查询应回 bad_query（storage.ErrBadQuery），得到 %v", err)
	}
}

// ---------------------------------------------------------------------------
// F5：上游 embedding 端点自己回非 2xx → 502 / 503（不是 500）
// ---------------------------------------------------------------------------

// newFailingEmbeddingsServer 起一个恒回 status 的假 /embeddings 端点（并计数）。
func newFailingEmbeddingsServer(t *testing.T, status int) (*httptest.Server, func() int) {
	t.Helper()
	var mu sync.Mutex
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"error":{"message":"upstream says %d"}}`, status)
	}))
	t.Cleanup(srv.Close)
	return srv, func() int {
		mu.Lock()
		defer mu.Unlock()
		return hits
	}
}

// TestVectorsUpstreamEmbeddingErrorStatus 钉 F5：上游 embedding 端点回非 2xx 时，
// /api/vectors 的状态码是 **502（上游 4xx）/ 503（上游 5xx）**，不是 500。
//
// 为什么要分 502/503：500 = "宿主自己炸了"（排查方向会被指错）；上游 4xx = 上游拒了
// 我们的请求（502），上游 5xx/429 = 上游暂时不可用（503）。
//
// 变异口径：去掉 writeVectorHubError 里的上游分支 → 两个用例都红（落成 500）。
func TestVectorsUpstreamEmbeddingErrorStatus(t *testing.T) {
	cases := []struct {
		name     string
		upstream int
		want     int
		wantCode string
	}{
		{name: "上游 401 → 502", upstream: http.StatusUnauthorized, want: http.StatusBadGateway, wantCode: "upstream_embedding_rejected"},
		{name: "上游 500 → 503", upstream: http.StatusInternalServerError, want: http.StatusServiceUnavailable, wantCode: "upstream_embedding_unavailable"},
		{name: "上游 429 → 503", upstream: http.StatusTooManyRequests, want: http.StatusServiceUnavailable, wantCode: "upstream_embedding_unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newFailingEmbeddingsServer(t, tc.upstream)
			embedder := openai.NewEmbedder(openai.Options{
				BaseURL: srv.URL,
				APIKey:  "sk-embed-upstream",
				Model:   "test-embed",
			})
			h := newTestHub(t, func(c *host.Config) {
				c.VectorsEnabled = true
				c.EmbeddingProvider = embedder
			})
			httpHub := httptest.NewServer(h.Handler())
			t.Cleanup(httpHub.Close)

			code, raw := postJSONRaw(t, httpHub.URL+"/api/vectors", map[string]any{"texts": []string{"猫"}})
			if code != tc.want {
				t.Fatalf("ingest 状态码 = %d，want %d；body=%s", code, tc.want, raw)
			}
			var errBody struct {
				Code string `json:"code"`
			}
			if err := json.Unmarshal([]byte(raw), &errBody); err != nil {
				t.Fatalf("解错误体: %v（body=%s）", err, raw)
			}
			if errBody.Code != tc.wantCode {
				t.Fatalf("错误码 = %q，want %q（body=%s）", errBody.Code, tc.wantCode, raw)
			}
			// 文案要点名"上游 embedding 端点"（调用方据此知道该找谁）。
			if !strings.Contains(raw, "上游 embedding 端点") {
				t.Fatalf("错误文案应点名「上游 embedding 端点」: %s", raw)
			}
		})
	}
}
