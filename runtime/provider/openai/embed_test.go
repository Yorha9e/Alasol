package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// 本文件钉住 embed.go 的 OpenAI 兼容 embeddings 客户端（宿主 EmbeddingProvider 槽位的
// provider 侧实现）。用 httptest 假端点，形态照 provider_test.go。

// capturedEmbedRequest 是 /embeddings 请求体的松散结构（核对 model/input 原样）。
type capturedEmbedRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

// newEmbedTestServer 起一个 httptest 服务：先核对路径/方法/Content-Type，再把请求体
// 解码后交给 check（可为 nil），最后用 write 回包。返回 Embedder（BaseURL 指向该服务、
// 固定模型名）与一个"被打到几次"的计数器。
func newEmbedTestServer(t *testing.T, check func(*capturedEmbedRequest), write func(w http.ResponseWriter, r *http.Request)) (*Embedder, func() int) {
	t.Helper()
	var mu sync.Mutex
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		if r.URL.Path != "/v1/embeddings" {
			t.Errorf("请求路径 = %q，期望 /v1/embeddings", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("请求方法 = %q，期望 POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q，期望 application/json", got)
		}
		if check != nil {
			var er capturedEmbedRequest
			if err := json.NewDecoder(r.Body).Decode(&er); err != nil {
				t.Errorf("解码请求体失败：%v", err)
				return
			}
			check(&er)
		}
		write(w, r)
	}))
	t.Cleanup(srv.Close)
	e := NewEmbedder(Options{BaseURL: srv.URL + "/v1", APIKey: "test-key", Model: "text-embedding-3-small"})
	return e, func() int {
		mu.Lock()
		defer mu.Unlock()
		return hits
	}
}

// ---- 往返：请求体原样、向量数值精确 ----

func TestEmbedRoundTrip(t *testing.T) {
	e, hits := newEmbedTestServer(t,
		func(er *capturedEmbedRequest) {
			if er.Model != "text-embedding-3-small" {
				t.Errorf("model = %q，期望原样透传 text-embedding-3-small", er.Model)
			}
			if er.Input != "你好，世界" {
				t.Errorf("input = %q，期望原样透传", er.Input)
			}
		},
		jsonWrite(`{"data":[{"embedding":[0.1,0.2],"index":0}]}`),
	)

	vec, err := e.Embed(context.Background(), "你好，世界")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if hits() != 1 {
		t.Errorf("请求 %d 次，期望 1", hits())
	}
	if len(vec) != 2 {
		t.Fatalf("向量维度 = %d，期望 2（%v）", len(vec), vec)
	}
	// 数值精确：encoding/json 按 float32 精度解析，结果 == Go 常量到 float32 的最近表示。
	if vec[0] != float32(0.1) || vec[1] != float32(0.2) {
		t.Errorf("向量 = %v，期望 [0.1 0.2]（按 float32 最近表示精确比较）", vec)
	}
}

// ---- 形状异常：明文错误（不静默挑一条） ----

func TestEmbedBatchDataRejected(t *testing.T) {
	// 单输入 → 单输出：data 多条 = 响应形状异常，**不静默挑一条**（挑哪条都是猜）。
	e, _ := newEmbedTestServer(t, nil, jsonWrite(
		`{"data":[{"embedding":[0.1],"index":0},{"embedding":[0.2],"index":1}]}`))

	_, err := e.Embed(context.Background(), "x")
	if err == nil {
		t.Fatal("data 多条必须报错（单输入应单输出）")
	}
	if !strings.Contains(err.Error(), "2") {
		t.Errorf("错误文案没点明条数（应是明文、可诊断）：%v", err)
	}
	var se *StatusError
	if errors.As(err, &se) {
		t.Errorf("形状异常不该折成 StatusError：%v", err)
	}
}

func TestEmbedEmptyDataRejected(t *testing.T) {
	e, _ := newEmbedTestServer(t, nil, jsonWrite(`{"data":[]}`))
	if _, err := e.Embed(context.Background(), "x"); err == nil || !strings.Contains(err.Error(), "data") {
		t.Fatalf("data 为空应报明文错误（提到 data）：%v", err)
	}
}

func TestEmbedMissingEmbeddingRejected(t *testing.T) {
	e, _ := newEmbedTestServer(t, nil, jsonWrite(`{"data":[{"index":0}]}`))
	if _, err := e.Embed(context.Background(), "x"); err == nil || !strings.Contains(err.Error(), "embedding") {
		t.Fatalf("缺 embedding 应报明文错误（提到 embedding）：%v", err)
	}
}

func TestEmbedNonNumericEmbeddingRejected(t *testing.T) {
	e, _ := newEmbedTestServer(t, nil, jsonWrite(`{"data":[{"embedding":["x","y"],"index":0}]}`))
	if _, err := e.Embed(context.Background(), "x"); err == nil || !strings.Contains(err.Error(), "解析") {
		t.Fatalf("embedding 非数值应报「解析响应体」类明文错误：%v", err)
	}
}

func TestEmbedIndexNonZeroRejected(t *testing.T) {
	// 单输入的 index 应为 0；非 0 = 响应与请求对不上（形状异常）。
	e, _ := newEmbedTestServer(t, nil, jsonWrite(`{"data":[{"embedding":[0.1],"index":1}]}`))
	if _, err := e.Embed(context.Background(), "x"); err == nil || !strings.Contains(err.Error(), "index") {
		t.Fatalf("index 非 0 应报明文错误（提到 index）：%v", err)
	}
}

// ---- 非 2xx：*StatusError（状态码 / 1KB 截断 / Retry-After） ----

func TestEmbedStatusError(t *testing.T) {
	big := strings.Repeat("x", 4*1024)
	e, hits := newEmbedTestServer(t, nil, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, big)
	})

	_, err := e.Embed(context.Background(), "x")
	if err == nil {
		t.Fatal("非 2xx 必须报错")
	}
	if hits() != 1 {
		t.Errorf("请求 %d 次，期望 1（400 不可重试，与 chat 白名单同款）", hits())
	}
	var se *StatusError
	if !errors.As(err, &se) {
		t.Fatalf("错误不是 *StatusError：%T %v", err, err)
	}
	if se.StatusCode != http.StatusBadRequest {
		t.Errorf("StatusCode = %d，期望 400", se.StatusCode)
	}
	// 响应体截断到 1KB（+ 截断标注的余量），不是 4KB 原文。
	if len(se.Body) > maxErrorBody+64 {
		t.Errorf("Body 长度 %d，期望截断到 ~1KB（+标注）", len(se.Body))
	}
	if !strings.Contains(se.Body, "truncated") {
		t.Errorf("Body 没有截断标注：%q", se.Body[:min(len(se.Body), 80)])
	}
	if se.RetryAfter != 2*time.Second {
		t.Errorf("RetryAfter = %v，期望 2s（Retry-After 头没被解析）", se.RetryAfter)
	}
}

// TestEmbedRetries429ThenSucceeds 钉"embeddings 复用 provider 级小重试、白名单同款"：
// 429 可重试 → 退避后重发一次成功（只读计算，幂等性自证与 chat 同款）。
func TestEmbedRetries429ThenSucceeds(t *testing.T) {
	var mu sync.Mutex
	n := 0
	e, _ := newEmbedTestServer(t, nil, func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		n++
		cur := n
		mu.Unlock()
		if cur == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":{"message":"rate limited"}}`)
			return
		}
		jsonWrite(`{"data":[{"embedding":[0.5],"index":0}]}`)(w, nil)
	})

	vec, err := e.Embed(context.Background(), "x")
	if err != nil {
		t.Fatalf("429 后应重试成功：%v", err)
	}
	mu.Lock()
	got := n
	mu.Unlock()
	if got != 2 {
		t.Errorf("请求 %d 次，期望 2（429 一次 + 重试一次——白名单与 chat 同款）", got)
	}
	if len(vec) != 1 || vec[0] != float32(0.5) {
		t.Errorf("向量 = %v，期望 [0.5]", vec)
	}
}

// ---- ctx 取消：errors.Is 可取、不重试 ----

func TestEmbedCtxCancel(t *testing.T) {
	// 服务端收到请求后一直不响应：让客户端的 ctx 取消来结束这一次尝试
	//（同 retry_test 的 TestRunCtxCancelNoRetry，等服务端 ctx 会挂住 Close）。
	arrived := make(chan struct{}, 4)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		arrived <- struct{}{}
		<-release
	}))
	defer func() {
		close(release)
		srv.Close()
	}()
	e := NewEmbedder(Options{BaseURL: srv.URL + "/v1", Model: "m"})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-arrived // 第一个请求到达后再取消（消掉"取消早于发出"的时序歧义）
		cancel()
	}()
	_, err := e.Embed(ctx, "x")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v，期望 errors.Is(err, context.Canceled)", err)
	}
	// 取消不是瞬断：不该有第二次尝试（白名单与 chat 同款）。
	select {
	case <-arrived:
		t.Error("取消后又发了一次请求（取消不是可重试错误）")
	case <-time.After(100 * time.Millisecond):
	}
}

// ---- 空 text：显式拒、不发请求 ----

func TestEmbedEmptyTextNoRequest(t *testing.T) {
	e, hits := newEmbedTestServer(t, nil, jsonWrite(`{"data":[{"embedding":[0.1],"index":0}]}`))
	_, err := e.Embed(context.Background(), "")
	if err == nil || !strings.Contains(err.Error(), "空") {
		t.Fatalf("空文本应报显式错误（提到空）：%v", err)
	}
	if hits() != 0 {
		t.Errorf("空文本打了 %d 次假端点，期望 0（省掉无意义的外部调用）", hits())
	}
}

// ---- 凭据纪律：Authorization 在场、key 不进**本层主动写进**的错误文案 ----
//
// 边界见 embed.go 的「凭据与错误文案」：本层不把凭据写进错误文案，但**端点若在响应体里
// 回显凭据**，那段文本会被抄进 *StatusError.Body（与 chat 同款既有风险）。本用例的假
// 端点**不回显**凭据，故错误文案里没有它。

func TestEmbedAuthHeaderAndCredentialHygiene(t *testing.T) {
	const key = "sk-secret-abc123"
	// 401（不可重试 → 一次就返回那个 *StatusError，最快路径）。
	e, _ := newEmbedTestServer(t, nil, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+key {
			t.Errorf("Authorization = %q，期望 Bearer <key>", got)
		}
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":{"message":"invalid api key"}}`)
	})
	e.p.apiKey = key // 换成带密钥的凭据（helper 默认 test-key）

	_, err := e.Embed(context.Background(), "x")
	if err == nil {
		t.Fatal("401 必须报错")
	}
	if strings.Contains(err.Error(), key) {
		t.Errorf("错误文案泄漏了 API key：%v", err)
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("错误文案应能看出状态码：%v", err)
	}
}

// ---- BaseURL 拼接：带/不带尾斜杠都打到 /embeddings ----

func TestEmbedBaseURLJoin(t *testing.T) {
	// provider 现状口径：normalizeBaseURL 去掉尾部 "/" 再拼 path——两种 BaseURL 都归一。
	for _, suffix := range []string{"", "/"} {
		t.Run("尾斜杠="+fmt.Sprintf("%q", suffix), func(t *testing.T) {
			var mu sync.Mutex
			var paths []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				paths = append(paths, r.URL.Path)
				mu.Unlock()
				jsonWrite(`{"data":[{"embedding":[0.1],"index":0}]}`)(w, r)
			}))
			defer srv.Close()

			e := NewEmbedder(Options{BaseURL: srv.URL + "/v1" + suffix, Model: "m"})
			if _, err := e.Embed(context.Background(), "x"); err != nil {
				t.Fatalf("Embed: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(paths) != 1 || paths[0] != "/v1/embeddings" {
				t.Errorf("请求路径 = %v，期望 [/v1/embeddings]（BaseURL=%q）", paths, srv.URL+"/v1"+suffix)
			}
		})
	}
}

// ---- Stream 对 embeddings 无意义：强制忽略（不发 SSE 的 Accept） ----

func TestEmbedIgnoresStreamOption(t *testing.T) {
	var mu sync.Mutex
	var accept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		accept = r.Header.Get("Accept")
		mu.Unlock()
		jsonWrite(`{"data":[{"embedding":[0.1],"index":0}]}`)(w, r)
	}))
	defer srv.Close()

	e := NewEmbedder(Options{BaseURL: srv.URL + "/v1", Model: "m", Stream: true})
	if _, err := e.Embed(context.Background(), "x"); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if accept == "text/event-stream" {
		t.Errorf("Accept = %q：Stream 对 embeddings 无意义，不该下发 SSE 头", accept)
	}
}

// ---- 兜底时钟：挂死的端点不会把调用永久挂住（F8） ----

// TestEmbedTimeoutFallback 钉 F8：embeddings 的 HTTP 路径有**兜底时钟**——挂死的端点
// （accept 后不回）不会把调用永久挂住（宿主 RAG 面的调用方 ctx 没有 deadline 兜底）。
//
// 生产默认 30s（defaultEmbedTimeout），测试里等 30s 不现实 → **注入**一个短超时的
// client（Options.HTTPClient）来验同一条路径（注入即覆盖默认，这是设计好的口子）。
//
// 变异口径：把 NewEmbedder 里的默认 client 去掉（且不注入）→ 挂死端点会让本测试超时；
// 把超时判定去掉 → err 不是 DeadlineExceeded。
func TestEmbedTimeoutFallback(t *testing.T) {
	// ① 默认装配（不注入 client）→ 30s 兜底时钟在位。
	def := NewEmbedder(Options{})
	if def.p == nil || def.p.client == nil {
		t.Fatal("默认装配应带一个 client（兜底时钟）")
	}
	if def.p.client.Timeout != defaultEmbedTimeout {
		t.Errorf("默认 client 超时 = %v，want %v（embeddings 的兜底时钟）",
			def.p.client.Timeout, defaultEmbedTimeout)
	}

	// ② 挂死的端点（accept 后不回）→ 到点返回超时错误（不永久挂住）。
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // accept 后不回
	}))
	defer func() {
		close(release)
		srv.Close()
	}()

	e := NewEmbedder(Options{
		BaseURL:    srv.URL + "/v1",
		Model:      "m",
		HTTPClient: &http.Client{Timeout: 200 * time.Millisecond},
	})
	_, err := e.Embed(context.Background(), "x")
	if err == nil {
		t.Fatal("挂死的端点应超时返回错误（不能永久挂住）")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v，期望 errors.Is(err, context.DeadlineExceeded)（超时）", err)
	}
}

// ---- base URL 里的 query 原样保留（F7：路径不能落进 query） ----

// TestEmbedBaseURLKeepsQuery 钉 F7：base URL 里的 **query 原样保留**，路径用 path.Join
// 接到 base 路径后面。
//
// 直接拼串的老坑：base = "http://h/v1?api=x" 时，base+"/embeddings" =
// "http://h/v1?api=x/embeddings"——**路径落进了 query**（端点收到路径 /v1 + query
// "api=x/embeddings"），请求打到错的路径上、query 也被改了形状（key 被改写）。
func TestEmbedBaseURLKeepsQuery(t *testing.T) {
	var mu sync.Mutex
	var gotPath, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		mu.Unlock()
		jsonWrite(`{"data":[{"embedding":[0.1],"index":0}]}`)(w, r)
	}))
	defer srv.Close()

	e := NewEmbedder(Options{BaseURL: srv.URL + "/v1?api=x", Model: "m"})
	if _, err := e.Embed(context.Background(), "x"); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotPath != "/v1/embeddings" {
		t.Errorf("请求路径 = %q，期望 /v1/embeddings（路径不能落进 query）", gotPath)
	}
	if gotQuery != "api=x" {
		t.Errorf("请求 query = %q，期望 api=x（base 的 query 原样保留）", gotQuery)
	}
}
