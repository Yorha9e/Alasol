package openai

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/retry"
)

// 本文件钉住 v4-4-A 的 **provider 级小重试**（07 M6 通用重试组件 + M7 重试安全性）：
// 口径见包 doc「重试」与 provider.go 的 retryableErr / retryableStatus。
//
// 防假绿的两处钉子（变异自证用）：
//   - TestRetryableErr 直接钉分类函数本身（把判定取反 → 红）；
//   - 各 Run 用例钉**请求次数**（不重试/重试到第几次），分类错了次数就错。

// statusThenOK 回一个"第 failN 次请求回 status，之后回 200 + okBody"的 handler，
// 并给出请求计数器（重试断言用：次数不对就是重试口径错了）。
func statusThenOK(t *testing.T, failN, status int, okBody string) (func(http.ResponseWriter, *http.Request), func() int) {
	t.Helper()
	var mu sync.Mutex
	n := 0
	write := func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		n++
		cur := n
		mu.Unlock()
		if cur <= failN {
			w.WriteHeader(status)
			fmt.Fprint(w, `{"error":{"message":"transient"}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, okBody)
	}
	return write, func() int {
		mu.Lock()
		defer mu.Unlock()
		return n
	}
}

const okNonStreamBody = `{"choices":[{"index":0,"message":{"role":"assistant","content":"成了"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`

// ---- 429 / 5xx：重试后成功 ----

func TestRunRetries429ThenSucceeds(t *testing.T) {
	write, requests := statusThenOK(t, 1, http.StatusTooManyRequests, okNonStreamBody)
	_, p := newTestServer(t, nil, write)

	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err != nil {
		t.Fatalf("429 后应重试成功：%v", err)
	}
	if requests() != 2 {
		t.Errorf("请求 %d 次，期望 2（429 一次 + 重试一次）", requests())
	}
	if res.Text != "成了" {
		t.Errorf("Text = %q", res.Text)
	}
	// 重试期间**不发** error 事件：UI 不该看到"抖了一下又好了"的红色闪烁。
	// 缺口 5 第二件：重试发生时多一条 provider.retry 留痕（在 text.delta **之前**——
	// 重试发生在拿到响应体之前；纯观测，done/结果不受影响）。
	if k := kinds(*evs); len(k) != 3 || k[0] != KindProviderRetry || k[1] != KindTextDelta || k[2] != KindDone {
		t.Errorf("事件序列 = %v，期望 [provider.retry text.delta done]（中途无 error）", k)
	}
}

func TestRunRetries503ThenSucceeds(t *testing.T) {
	write, requests := statusThenOK(t, 2, http.StatusServiceUnavailable, okNonStreamBody)
	_, p := newTestServer(t, nil, write)

	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err != nil {
		t.Fatalf("503 后应重试成功：%v", err)
	}
	if requests() != 3 {
		t.Errorf("请求 %d 次，期望 3（503 两次 + 重试一次，MaxAttempts=3）", requests())
	}
	if res.Text != "成了" {
		t.Errorf("Text = %q", res.Text)
	}
}

// TestRunRetries500ThenSucceedsStream 证明流式下重试发生在**发 delta 之前**：
// 第一次 500 没吐任何片段，第二次 200 才发 delta → 消费端不会看到重复片段。
func TestRunRetries500ThenSucceedsStream(t *testing.T) {
	var mu sync.Mutex
	n := 0
	_, p := newTestServer(t, nil, func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		n++
		cur := n
		mu.Unlock()
		if cur == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, `{"error":{"message":"transient"}}`)
			return
		}
		sseWrite(
			`{"choices":[{"index":0,"delta":{"content":"流"}}]}`,
			`[DONE]`,
		)(w, nil)
	})
	p.stream = true

	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err != nil {
		t.Fatalf("500 后应重试成功：%v", err)
	}
	mu.Lock()
	got := n
	mu.Unlock()
	if got != 2 {
		t.Errorf("请求 %d 次，期望 2", got)
	}
	if res.Text != "流" {
		t.Errorf("Text = %q", res.Text)
	}
	// provider.retry 在 delta **之前**（重试发生在拿到响应体之前）；delta 仍只发一次
	//（不双发）——留痕是纯观测，不改变流式序列的其余形状。
	if k := kinds(*evs); len(k) != 3 || k[0] != KindProviderRetry || k[1] != KindTextDelta || k[2] != KindDone {
		t.Errorf("事件序列 = %v，期望 [provider.retry text.delta done]（不双发 delta）", k)
	}
}

// ---- 4xx：不重试 ----

func TestRunNoRetryOn4xx(t *testing.T) {
	// 4xx（非 429）是**请求侧**错误：该改请求，不是把同样的请求重发三遍。
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			write, requests := statusThenOK(t, 99, status, okNonStreamBody)
			_, p := newTestServer(t, nil, write)
			_, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
			if err == nil {
				t.Fatal("期望非 2xx 报错")
			}
			if requests() != 1 {
				t.Errorf("请求 %d 次，期望 1（4xx 不重试）", requests())
			}
			var se *StatusError
			if !errors.As(err, &se) || se.StatusCode != status {
				t.Errorf("错误不是 StatusError(%d)：%T %v", status, err, err)
			}
		})
	}
}

// ---- 5xx 耗尽：聚合错误 ----

func TestRunRetries5xxUntilExhausted(t *testing.T) {
	write, requests := statusThenOK(t, 99, http.StatusInternalServerError, okNonStreamBody)
	_, p := newTestServer(t, nil, write)

	emit, evs := collectEvents()
	start := time.Now()
	_, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("持续 500 必须报错")
	}
	if requests() != providerMaxAttempts {
		t.Errorf("请求 %d 次，期望 %d（MaxAttempts）", requests(), providerMaxAttempts)
	}
	// 聚合错误：每条尝试一个条目（M6），errors.As 仍能取出 *StatusError。
	var agg *retry.Aggregated
	if !errors.As(err, &agg) {
		t.Fatalf("错误不是 *retry.Aggregated：%T %v", err, err)
	}
	if len(agg.Attempts) != providerMaxAttempts {
		t.Errorf("聚合了 %d 条尝试，期望 %d", len(agg.Attempts), providerMaxAttempts)
	}
	for i, a := range agg.Attempts {
		var se *StatusError
		if !errors.As(a.Err, &se) || se.StatusCode != http.StatusInternalServerError {
			t.Errorf("Attempts[%d] 不是 StatusError(500)：%v", i, a.Err)
		}
	}
	var se *StatusError
	if !errors.As(err, &se) || se.StatusCode != http.StatusInternalServerError {
		t.Errorf("聚合错误判不出 StatusError(500)：%v", err)
	}
	// 退避量级几百 ms：这里只做**宽松**上界（真时钟，防的是"退避失控/指数爆炸"）。
	if elapsed > 5*time.Second {
		t.Errorf("重试总耗时 %v，远超退避量级（指数退避失控？）", elapsed)
	}
	// 重试留痕（缺口 5 第二件）：两次重试各一条 provider.retry（attempt=2/3），最终失败
	// 一条 error——error 仍只有最终那条（重试期间不发 error，UI 不闪烁）。
	if k := kinds(*evs); len(k) != 3 ||
		k[0] != KindProviderRetry || k[1] != KindProviderRetry || k[2] != KindError {
		t.Errorf("事件 = %v，期望 [provider.retry provider.retry error]", k)
	}
}

// ---- 网络瞬断：重试 ----

func TestRunRetriesNetworkError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	base := srv.URL + "/v1"
	srv.Close() // 关掉 → 连接被拒（net.Error）

	p := New(Options{BaseURL: base})
	_, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err == nil {
		t.Fatal("连接被拒必须报错")
	}
	var agg *retry.Aggregated
	if !errors.As(err, &agg) || len(agg.Attempts) != providerMaxAttempts {
		t.Fatalf("网络错误应重试 %d 次并聚合成 %d 条：%T %v", providerMaxAttempts, providerMaxAttempts, err, err)
	}
}

// ---- 流式中途失败：不重试 ----

// TestRunStreamMidFailureNoRetry 钉"流式已开始输出后不重试"（半截流重试会双发 delta）。
// 中途失败只上报：第一次请求就发出过 delta → 绝不重发。
func TestRunStreamMidFailureNoRetry(t *testing.T) {
	var mu sync.Mutex
	n := 0
	_, p := newTestServer(t, nil, func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		n++
		mu.Unlock()
		// 先发一条**真** delta，再发一个畸形块 → 解析失败发生在"已开始输出"之后。
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"半截\"}}]}\n\n")
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\n\n")
	})
	p.stream = true

	emit, evs := collectEvents()
	if _, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit); err == nil {
		t.Fatal("流中畸形块必须报错")
	}
	mu.Lock()
	got := n
	mu.Unlock()
	if got != 1 {
		t.Errorf("流式中途失败后请求了 %d 次，期望 1（半截流不重试）", got)
	}
	if k := kinds(*evs); len(k) != 2 || k[0] != KindTextDelta || k[1] != KindError {
		t.Errorf("事件序列 = %v，期望 [text.delta error]", k)
	}
}

// ---- ctx 取消：不重试 ----

func TestRunCtxCancelNoRetry(t *testing.T) {
	// 服务端收到请求后**一直不响应**：让客户端的 ctx 超时来结束这一次尝试。
	// 注意不能用 `<-r.Context().Done()` 等客户端取消——客户端 ctx 超时并不保证服务端
	// 请求 ctx 被取消（连接可能还被客户端持有），等它会挂住 srv.Close()。
	arrived := make(chan struct{}, providerMaxAttempts+2)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		arrived <- struct{}{}
		<-release
	}))
	defer func() {
		close(release)
		srv.Close()
	}()
	p := New(Options{BaseURL: srv.URL + "/v1"})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, err := p.Run(ctx, core.ModelRequest{Model: "m"}, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v，期望 DeadlineExceeded", err)
	}
	// 重试是**同步**发生在 Run 内部的：若取消被当成可重试错误，第二次请求会在 Run
	// 返回前就打进来。等第一次确实到达（消 goroutine 调度竞态）后，短暂宽限期内
	// 不该有第二个。
	select {
	case <-arrived:
	case <-time.After(2 * time.Second):
		t.Fatal("请求未到达服务端")
	}
	select {
	case <-arrived:
		t.Error("取消后又发了一次请求（取消不是可重试错误，M7）")
	case <-time.After(100 * time.Millisecond):
	}
}

// ---- 资源安全：中间响应体必须关掉 ----

// trackingBody 记录 Close 次数（重试路径必须把中间响应体关掉：不关 = 连接泄漏）。
type trackingBody struct {
	io.ReadCloser
	closed *int32
}

func (b *trackingBody) Close() error {
	atomic.AddInt32(b.closed, 1)
	return b.ReadCloser.Close()
}

// trackingTransport 给每个响应体套一个 Close 计数器（只做观测，不改语义）。
type trackingTransport struct {
	base  http.RoundTripper
	mu    sync.Mutex
	close []*int32
}

func (t *trackingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	c := new(int32)
	t.mu.Lock()
	t.close = append(t.close, c)
	t.mu.Unlock()
	resp.Body = &trackingBody{ReadCloser: resp.Body, closed: c}
	return resp, nil
}

func (t *trackingTransport) closes() []int32 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]int32, 0, len(t.close))
	for _, c := range t.close {
		out = append(out, atomic.LoadInt32(c))
	}
	return out
}

// TestRunRetryClosesIntermediateBodies 钉"重试不泄漏连接"：429 那个响应体必须在重发前
// 被排空 + 关闭，否则每次重试都新建一条 TCP/TLS 连接（重试反而更慢），句柄也越用越多。
func TestRunRetryClosesIntermediateBodies(t *testing.T) {
	write, requests := statusThenOK(t, 1, http.StatusTooManyRequests, okNonStreamBody)
	srv, _ := newTestServer(t, nil, write)
	tr := &trackingTransport{base: http.DefaultTransport}
	p := New(Options{BaseURL: srv.URL + "/v1", HTTPClient: &http.Client{Transport: tr}})

	if _, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if requests() != 2 {
		t.Fatalf("请求 %d 次，期望 2", requests())
	}
	closes := tr.closes()
	if len(closes) != 2 {
		t.Fatalf("RoundTrip %d 次，期望 2", len(closes))
	}
	for i, n := range closes {
		if n == 0 {
			t.Errorf("第 %d 个响应体没被关闭（连接泄漏）", i)
		}
		if n > 1 {
			t.Errorf("第 %d 个响应体被关了 %d 次（双重关闭）", i, n)
		}
	}
}

// ---- 假 RoundTripper：不发真实网络请求的 429→200 ----

// scriptedRoundTripper 按脚本依次回响应（第 1 次 429、第 2 次 200）——把"429 重试后
// 成功"钉在**传输层**，不依赖 httptest 服务与真实网络（更快、无时序敏感）。
type scriptedRoundTripper struct {
	mu       sync.Mutex
	calls    int
	statuses []int
	bodies   []string
}

func (rt *scriptedRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.mu.Lock()
	i := rt.calls
	rt.calls++
	rt.mu.Unlock()
	idx := i
	if idx >= len(rt.statuses) {
		idx = len(rt.statuses) - 1 // 超出脚本就重复最后一个（测试里不该走到）
	}
	status := rt.statuses[idx]
	body := rt.bodies[idx]
	return &http.Response{
		StatusCode:    status,
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Body:          io.NopCloser(strings.NewReader(body)),
		Header:        make(http.Header),
		ContentLength: int64(len(body)),
		Request:       req,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
	}, nil
}

func (rt *scriptedRoundTripper) count() int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.calls
}

// TestRunRetries429WithFakeRoundTripper 是任务点名的那枚钉子：第一次 429、第二次 200
// → 必须重试一次后成功，且 RoundTrip 只被调 2 次。
func TestRunRetries429WithFakeRoundTripper(t *testing.T) {
	rt := &scriptedRoundTripper{
		statuses: []int{http.StatusTooManyRequests, http.StatusOK},
		bodies:   []string{`{"error":{"message":"rate limited"}}`, okNonStreamBody},
	}
	p := New(Options{BaseURL: "http://provider.example/v1", HTTPClient: &http.Client{Transport: rt}})

	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err != nil {
		t.Fatalf("429 后应重试成功：%v", err)
	}
	if rt.count() != 2 {
		t.Errorf("RoundTrip %d 次，期望 2（429 一次 + 重试一次）", rt.count())
	}
	if res.Text != "成了" {
		t.Errorf("Text = %q", res.Text)
	}
}

// TestRunRetries5xxWithFakeRoundTripper 同一路径的 5xx 版：三次都 500 → 耗尽聚合。
func TestRunRetries5xxWithFakeRoundTripper(t *testing.T) {
	rt := &scriptedRoundTripper{
		statuses: []int{http.StatusInternalServerError},
		bodies:   []string{`{"error":{"message":"boom"}}`},
	}
	p := New(Options{BaseURL: "http://provider.example/v1", HTTPClient: &http.Client{Transport: rt}})

	_, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err == nil {
		t.Fatal("持续 500 必须报错")
	}
	if rt.count() != providerMaxAttempts {
		t.Errorf("RoundTrip %d 次，期望 %d", rt.count(), providerMaxAttempts)
	}
	var agg *retry.Aggregated
	if !errors.As(err, &agg) || len(agg.Attempts) != providerMaxAttempts {
		t.Fatalf("错误不是 %d 条聚合：%T %v", providerMaxAttempts, err, err)
	}
}

// TestRunNoRetry4xxWithFakeRoundTripper 同一路径的 4xx 版：只调 1 次。
func TestRunNoRetry4xxWithFakeRoundTripper(t *testing.T) {
	rt := &scriptedRoundTripper{
		statuses: []int{http.StatusBadRequest},
		bodies:   []string{`{"error":{"message":"bad request"}}`},
	}
	p := New(Options{BaseURL: "http://provider.example/v1", HTTPClient: &http.Client{Transport: rt}})

	_, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err == nil {
		t.Fatal("400 必须报错")
	}
	if rt.count() != 1 {
		t.Errorf("RoundTrip %d 次，期望 1（4xx 不重试）", rt.count())
	}
	var se *StatusError
	if !errors.As(err, &se) || se.StatusCode != http.StatusBadRequest {
		t.Errorf("错误不是 StatusError(400)：%T %v", err, err)
	}
}

// ---- TLS / 永久传输错：不重试（F2）----

// TestRunNoRetryOnTLSVerificationError 是 F2 的钉子：**真实形状**的 TLS 错误
// （httptest.NewTLSServer 的自签证书不被默认 client 信任 → 客户端拿到
// *url.Error 包着 x509 证书错）**不重试**：证书配错是永久错，重发三遍还是同样的错，
// 白等三个退避周期。
//
// 把"永久传输错排除"去掉（只按 net.Error 兜底）→ 本用例立刻红（握手 3 次）。
func TestRunNoRetryOnTLSVerificationError(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// 计数用 TLS 握手（握手失败时服务端根本收不到请求，handler 计数不到）。
	var mu sync.Mutex
	handshakes := 0
	trace := &httptrace.ClientTrace{
		TLSHandshakeStart: func() {
			mu.Lock()
			handshakes++
			mu.Unlock()
		},
	}
	ctx := httptrace.WithClientTrace(context.Background(), trace)
	p := New(Options{BaseURL: srv.URL + "/v1"})

	_, err := p.Run(ctx, core.ModelRequest{Model: "m"}, nil)
	if err == nil {
		t.Fatal("TLS 校验失败必须报错")
	}
	mu.Lock()
	got := handshakes
	mu.Unlock()
	if got != 1 {
		t.Errorf("TLS 握手 %d 次，期望 1（永久 TLS 错不重试）", got)
	}
	// 错误面要能看出是证书问题（而不是"网络瞬断"）。
	if !strings.Contains(err.Error(), "certificate") {
		t.Errorf("错误文案里没有证书线索：%v", err)
	}
	// 只有一次尝试 → 不该出现聚合错误（没有"各次尝试"可聚合）。
	var agg *retry.Aggregated
	if errors.As(err, &agg) {
		t.Errorf("单次尝试不该是聚合错误：%v", err)
	}
}

// TestRunNoRetryOnDNSNotFound 是 F2 的另一半：DNS 查无此域（永久）→ 不重试。
func TestRunNoRetryOnDNSNotFound(t *testing.T) {
	// .invalid 顶级域保证不会被解析（RFC 2606 保留）。
	p := New(Options{BaseURL: "http://no-such-host-xyzzy-12345.invalid/v1"})
	_, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err == nil {
		t.Fatal("DNS 查无此域必须报错")
	}
	// 环境差异兜底：有些网络带 wildcard DNS（任意域名都解析到占位地址）→ 那就不是
	// "DNS 永久错"这条路径了，跳过（别把环境问题报成回归）。
	var dnsErr *net.DNSError
	if !errors.As(err, &dnsErr) || !dnsErr.IsNotFound {
		t.Skipf("本机没走到 DNS 永久错这条路径（err=%v），跳过", err)
	}
	var agg *retry.Aggregated
	if errors.As(err, &agg) {
		t.Errorf("永久 DNS 错不该被重试（不该出现聚合错误）：%v", err)
	}
}

// countingTransport 只做观测：数 RoundTrip 被调了几次（重试断言用）。
type countingTransport struct {
	base http.RoundTripper
	mu   sync.Mutex
	n    int
}

func (t *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	t.n++
	t.mu.Unlock()
	return t.base.RoundTrip(req)
}

func (t *countingTransport) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.n
}

// TestRunNoRetryOnHTTPSToPlaintextPort 是 F2 的另一半：**永久配置错**（https 打明文
// 端口）不重试。
//
// 为什么它会被误判：net/http 把"TLS 记录头错"改写成了哨兵错误
// http.ErrSchemeMismatch（"http: server gave HTTP response to HTTPS client"），
// 它被 *url.Error 包着——而 *url.Error 自己实现了 net.Error（内层是什么它不管）→
// 落进"net.Error 就重试"的兜底 → 同一个配置错被重发三遍（还白等三个退避周期）。
//
// 把"http 层改写后的形态"从 isPermanentTransportErr 里去掉 → 本用例立刻红
// （RoundTrip 3 次）。
func TestRunNoRetryOnHTTPSToPlaintextPort(t *testing.T) {
	// 明文服务端：客户端却用 https 打它 → 协议对不上（永久配置错）。
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	// 明文服务收到 TLS ClientHello 会解析失败 → 服务端会往 stderr 打日志；这里静音
	// （免得污染测试输出）。
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.Start()
	defer srv.Close()

	tr := &countingTransport{base: http.DefaultTransport}
	p := New(Options{
		BaseURL:    strings.Replace(srv.URL, "http://", "https://", 1),
		HTTPClient: &http.Client{Transport: tr},
	})

	_, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err == nil {
		t.Fatal("https 打明文端口必须报错")
	}
	if tr.count() != 1 {
		t.Errorf("RoundTrip %d 次，期望 1（永久配置错不重试）", tr.count())
	}
	// 错误面要能看出是协议/端口配错（而不是"网络瞬断"）。
	if !strings.Contains(err.Error(), "HTTPS client") {
		t.Errorf("错误文案里没有协议错线索：%v", err)
	}
	// 只有一次尝试 → 不该出现聚合错误（没有"各次尝试"可聚合）。
	var agg *retry.Aggregated
	if errors.As(err, &agg) {
		t.Errorf("单次尝试不该是聚合错误：%v", err)
	}
}

// TestRetryableErrPermanentConfigErrors 钉住分类函数本体：**配置错**（https 打明文
// 端口 / 协议不支持）不可重试。
//
// 这两类错在 net/http 里没有类型（一个是哨兵、一个是纯文案），只能按哨兵/文案判。
func TestRetryableErrPermanentConfigErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			"https 打明文端口（真实形状：*url.Error 包 http.ErrSchemeMismatch）",
			&url.Error{Op: "Post", URL: "https://provider.example/v1/chat/completions", Err: http.ErrSchemeMismatch},
			false,
		},
		{
			"https 打明文端口（文案形状：同款文案但不是那个哨兵）",
			&url.Error{Op: "Post", URL: "https://provider.example/v1/chat/completions",
				Err: errors.New("http: server gave HTTP response to HTTPS client")},
			false,
		},
		{
			"协议不支持（ftp://）",
			&url.Error{Op: "Post", URL: "ftp://provider.example/v1/chat/completions",
				Err: errors.New(`unsupported protocol scheme "ftp"`)},
			false,
		},
		{
			"包装过的同款（provider 前缀）",
			fmt.Errorf("openai: 请求失败：%w", &url.Error{Op: "Post", URL: "https://x/v1", Err: http.ErrSchemeMismatch}),
			false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := retryableErr(c.err); got != c.want {
				t.Errorf("retryableErr(%v) = %v，期望 %v", c.err, got, c.want)
			}
		})
	}
}

// ---- Retry-After：解析与采信（F1）----

// TestStatusErrorCarriesRetryAfter 钉住"Retry-After 头被解析进 *StatusError"：
// 两种形态都要认（延迟秒数 / HTTP-date），解析不出 = 0。
func TestStatusErrorCarriesRetryAfter(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name   string
		header string
		want   time.Duration
	}{
		{"延迟秒数", "2", 2 * time.Second},
		{"延迟秒数 0", "0", 0},
		{"负数（畸形）", "-5", 0},
		{"HTTP-date（未来）", now.Add(90 * time.Second).UTC().Format(http.TimeFormat), 90 * time.Second},
		{"HTTP-date（过去）", now.Add(-90 * time.Second).UTC().Format(http.TimeFormat), 0},
		{"畸形", "soon", 0},
		{"空", "", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseRetryAfter(c.header, now)
			if c.want == 0 {
				if got != 0 {
					t.Errorf("parseRetryAfter(%q) = %v，期望 0", c.header, got)
				}
				return
			}
			// HTTP-date 有秒级精度：允许 1s 的误差。
			if d := got - c.want; d < -time.Second || d > time.Second {
				t.Errorf("parseRetryAfter(%q) = %v，期望 ≈%v", c.header, got, c.want)
			}
		})
	}
}

// TestRunStatusErrorCarriesRetryAfterFromHeader 端到端：端点回 Retry-After: 2 →
// 返回给调用方的 *StatusError 带上 2s（400 不重试，直接原样返回，最快）。
func TestRunStatusErrorCarriesRetryAfterFromHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":{"message":"bad request"}}`)
	}))
	defer srv.Close()
	p := New(Options{BaseURL: srv.URL + "/v1"})

	_, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err == nil {
		t.Fatal("400 必须报错")
	}
	var se *StatusError
	if !errors.As(err, &se) {
		t.Fatalf("错误不是 *StatusError：%T %v", err, err)
	}
	if se.RetryAfter != 2*time.Second {
		t.Errorf("StatusError.RetryAfter = %v，期望 2s（Retry-After 头没被解析）", se.RetryAfter)
	}
}

// TestRunWaitsForRetryAfter 是 F1 的行为钉子：429 + Retry-After: 2 → 第二次请求
// **至少隔 2s** 才发出去（不是按 100ms 的自造节奏白烧——端点的限流窗口不会因为我们
// 重试得快而缩短）。
//
// 真时钟量（provider 的 Policy.sleep 是包内注入点，包外换不了）：量两次请求到达的
// 间隔，断言 ≥1.8s（留 0.2s 的调度/计时余量）。
func TestRunWaitsForRetryAfter(t *testing.T) {
	var mu sync.Mutex
	var arrivals []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		cur := len(arrivals)
		mu.Unlock()
		if cur == 1 {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":{"message":"rate limited"}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, okNonStreamBody)
	}))
	defer srv.Close()
	p := New(Options{BaseURL: srv.URL + "/v1"})

	if _, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil); err != nil {
		t.Fatalf("429 后应重试成功：%v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(arrivals) != 2 {
		t.Fatalf("请求 %d 次，期望 2", len(arrivals))
	}
	gap := arrivals[1].Sub(arrivals[0])
	if gap < 1800*time.Millisecond {
		t.Errorf("两次请求只隔了 %v：Retry-After: 2 没被采信（还在按 ~100ms 的自造节奏重试）", gap)
	}
}

// TestRunStopsWhenRetryAfterExceedsMaxBackoff 是 F1 的钉子：端点说"过 60s 再来"
// （Retry-After: 60），而 provider 的等待上限是 2s（MaxBackoff）→ 再试也是撞在同一个
// 限流窗口里（还会加剧限流，白烧请求）→ **只发 1 次请求**，并把窗口抛给调用方。
//
// 把"RA > 封顶就不重试"这条去掉（照旧按封顶后的 2s 等下再试）→ 本用例立刻红
// （请求 3 次，且总耗时多出两个退避周期）。
func TestRunStopsWhenRetryAfterExceedsMaxBackoff(t *testing.T) {
	var mu sync.Mutex
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		n++
		mu.Unlock()
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"error":{"message":"rate limited"}}`)
	}))
	defer srv.Close()
	p := New(Options{BaseURL: srv.URL + "/v1"})

	_, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err == nil {
		t.Fatal("持续 429 必须报错")
	}
	mu.Lock()
	got := n
	mu.Unlock()
	if got != 1 {
		t.Errorf("请求 %d 次，期望 1（Retry-After 60s > MaxBackoff 2s → 不重试）", got)
	}
	// 窗口要抛给调用方：RetryAfter 留在 *StatusError 里。
	var se *StatusError
	if !errors.As(err, &se) {
		t.Fatalf("错误里取不出 *StatusError：%T %v", err, err)
	}
	if se.RetryAfter != 60*time.Second {
		t.Errorf("StatusError.RetryAfter = %v，期望 60s（窗口没抛给调用方）", se.RetryAfter)
	}
	// 错误文案里也要看得见窗口（F5：窗口对用户不可见 = 白留）。
	if !strings.Contains(err.Error(), "60s") {
		t.Errorf("错误文案里没有限流窗口：%v", err)
	}
}

// ---- 排空与连接复用（F3）----

// TestRunRetryReusesConnectionAfterDrain 是 F3 的钉子：重试前的那个错误响应体必须被
// **排空干净**，否则连接进不了空闲池 → 第二次请求重新握手（TCP+TLS），重试反而更慢。
//
// 错误体故意给 8KB（> 旧的 1KB 排空上限）：按 1KB 排空 = 排不干净 = 连接不复用。
// 用 httptrace.GotConn 断言第二次请求 **reused=true**。
func TestRunRetryReusesConnectionAfterDrain(t *testing.T) {
	big := strings.Repeat("x", 8*1024)
	var mu sync.Mutex
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		n++
		cur := n
		mu.Unlock()
		if cur == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, big)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, okNonStreamBody)
	}))
	defer srv.Close()
	p := New(Options{BaseURL: srv.URL + "/v1"})

	var mu2 sync.Mutex
	var reused []bool
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			mu2.Lock()
			reused = append(reused, info.Reused)
			mu2.Unlock()
		},
	}
	ctx := httptrace.WithClientTrace(context.Background(), trace)

	if _, err := p.Run(ctx, core.ModelRequest{Model: "m"}, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	mu2.Lock()
	defer mu2.Unlock()
	if len(reused) != 2 {
		t.Fatalf("GotConn %d 次，期望 2", len(reused))
	}
	if reused[0] {
		t.Error("第一次请求不该复用连接（新连接）")
	}
	if !reused[1] {
		t.Error("第二次请求没有复用连接（错误体没排空干净 → 连接被丢弃，重试反而更慢）")
	}
}

// ---- 重试后撞上不可重试错误（F4）----

// TestRunRetryThenNonRetryableAggregates 是 F4 的钉子：429（可重试）→ 400（不可重试）
// → 返回**聚合错误**：里面留着 429 的痕迹，而 errors.As 取得到最后的 400。
func TestRunRetryThenNonRetryableAggregates(t *testing.T) {
	var mu sync.Mutex
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		n++
		cur := n
		mu.Unlock()
		if cur == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":{"message":"rate limited"}}`)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":{"message":"bad request"}}`)
	}))
	defer srv.Close()
	p := New(Options{BaseURL: srv.URL + "/v1"})

	_, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err == nil {
		t.Fatal("400 必须报错")
	}
	var agg *retry.Aggregated
	if !errors.As(err, &agg) {
		t.Fatalf("重试后撞上不可重试错误应返回聚合：%T %v", err, err)
	}
	if len(agg.Attempts) != 2 {
		t.Fatalf("聚合了 %d 条尝试，期望 2", len(agg.Attempts))
	}
	// 聚合里要有 429 的痕迹（第 1 次尝试）。
	var first *StatusError
	if !errors.As(agg.Attempts[0].Err, &first) || first.StatusCode != http.StatusTooManyRequests {
		t.Errorf("第 1 次尝试不是 StatusError(429)：%v", agg.Attempts[0].Err)
	}
	// errors.As 要能取到最后的 400（聚合的 Unwrap 把最近一次排在前面）。
	var se *StatusError
	if !errors.As(err, &se) {
		t.Fatalf("聚合错误里取不出 *StatusError：%v", err)
	}
	if se.StatusCode != http.StatusBadRequest {
		t.Errorf("errors.As 取到的是 %d，期望 400（最后的、不可重试的那个）", se.StatusCode)
	}
	// 文案里也要看得见两次尝试（诊断用）。
	if msg := err.Error(); !strings.Contains(msg, "429") || !strings.Contains(msg, "400") {
		t.Errorf("错误文案缺 429/400 的痕迹：%s", msg)
	}
}

// ---- 错误文案：限流窗口可见（F5）----

// TestStatusErrorErrorCarriesRetryAfterWindow 是 F5 的钉子：限流窗口必须在**错误文案**
// 里看得见（只躺在字段里 = 对用户/日志读者不可见）。
//
// 把 Error() 里的窗口那段去掉 → 本用例立刻红。
func TestStatusErrorErrorCarriesRetryAfterWindow(t *testing.T) {
	se := &StatusError{
		StatusCode: http.StatusTooManyRequests,
		Status:     "429 Too Many Requests",
		Body:       `{"error":{"message":"rate limited"}}`,
		RetryAfter: 60 * time.Second,
	}
	msg := se.Error()
	for _, want := range []string{"HTTP 429", "retry after 60s", "rate limited"} {
		if !strings.Contains(msg, want) {
			t.Errorf("错误文案缺 %q：%s", want, msg)
		}
	}
	// 没有窗口时不该多出窗口文案（保持旧文案形状）。
	plain := (&StatusError{StatusCode: http.StatusBadRequest, Body: `{"error":{"message":"bad"}}`}).Error()
	if strings.Contains(plain, "retry after") {
		t.Errorf("没有 Retry-After 时不该出现窗口文案：%s", plain)
	}
}

// ---- 分类函数本体（变异钉子）----

// fakeNetErr 是测试用的 net.Error（Timeouts/Temporary 都在——真实的 net.OpError 同款形状）。
type fakeNetErr struct{}

func (fakeNetErr) Error() string   { return "fake: connection reset" }
func (fakeNetErr) Timeout() bool   { return true }
func (fakeNetErr) Temporary() bool { return true }

func TestRetryableErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"429", &StatusError{StatusCode: http.StatusTooManyRequests}, true},
		{"500", &StatusError{StatusCode: http.StatusInternalServerError}, true},
		{"503", &StatusError{StatusCode: http.StatusServiceUnavailable}, true},
		{"599", &StatusError{StatusCode: 599}, true},
		{"400", &StatusError{StatusCode: http.StatusBadRequest}, false},
		{"401", &StatusError{StatusCode: http.StatusUnauthorized}, false},
		{"404", &StatusError{StatusCode: http.StatusNotFound}, false},
		{"408", &StatusError{StatusCode: http.StatusRequestTimeout}, false},
		{"包装的 429", fmt.Errorf("openai: %w", &StatusError{StatusCode: http.StatusTooManyRequests}), true},
		{"包装的 400", fmt.Errorf("openai: %w", &StatusError{StatusCode: http.StatusBadRequest}), false},
		{"网络错误", fmt.Errorf("openai: 请求失败：%w", fakeNetErr{}), true},
		{"包装的网络错误", fmt.Errorf("openai: 请求失败：%w", fmt.Errorf("inner: %w", fakeNetErr{})), true},
		{"真实形状的 TLS 错误（*url.Error 包证书错）", realTLSURLError(), false},
		{"DNS 查无此域（永久）", realDNSNotFoundURLError(), false},
		{"DNS 抖动（超时，瞬时）", realDNSTimeoutURLError(), true},
		{"畸形响应", errors.New("openai: 响应缺少 choices"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := retryableErr(c.err); got != c.want {
				t.Errorf("retryableErr(%v) = %v，期望 %v", c.err, got, c.want)
			}
		})
	}
}

// realTLSURLError 是**真实形状**的 TLS 校验失败错误：*url.Error 包着
// *tls.CertificateVerificationError（后者 unwrap 到 x509 证书错）。
//
// 这正是 httptest.NewTLSServer（自签证书不被信任）时客户端拿到的东西——原来的
// errors.New("tls: failed to verify certificate") 是**假形状**：它不是 net.Error，
// 所以旧判据（"是 net.Error 就重试"）本来也挡得住它，测不出真实缺陷。
func realTLSURLError() error {
	return &url.Error{
		Op:  "Post",
		URL: "https://provider.example/v1/chat/completions",
		Err: &tls.CertificateVerificationError{
			Err: x509.UnknownAuthorityError{Cert: &x509.Certificate{}},
		},
	}
}

// realDNSNotFoundURLError 是真实形状的"DNS 查无此域"（永久错，重试无用）。
func realDNSNotFoundURLError() error {
	return &url.Error{
		Op:  "Post",
		URL: "https://provider.example/v1/chat/completions",
		Err: &net.DNSError{Err: "no such host", Name: "provider.example", IsNotFound: true},
	}
}

// realDNSTimeoutURLError 是真实形状的"DNS 抖动"（瞬时错，可重试）。
func realDNSTimeoutURLError() error {
	return &url.Error{
		Op:  "Post",
		URL: "https://provider.example/v1/chat/completions",
		Err: &net.DNSError{Err: "i/o timeout", Name: "provider.example", IsTimeout: true},
	}
}

// TestRetryableErrCtxNeverRetries 单钉 ctx 取消/超时：**永不重试**。
// 注意 context.DeadlineExceeded 自己满足 net.Error，所以它必须排在分类前面挡掉——
// 这条用例就是防那个排序被改。
func TestRetryableErrCtxNeverRetries(t *testing.T) {
	for _, err := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		fmt.Errorf("openai: 请求中止（%w）", context.Canceled),
		fmt.Errorf("openai: 请求失败：%w", context.DeadlineExceeded),
		fmt.Errorf("openai: 请求失败：%w", fmt.Errorf("inner: %w", context.DeadlineExceeded)),
	} {
		if retryableErr(err) {
			t.Errorf("retryableErr(%v) = true：取消/超时绝不重试", err)
		}
	}
}

func TestRetryableStatus(t *testing.T) {
	for _, code := range []int{429, 500, 501, 502, 503, 504, 599} {
		if !retryableStatus(code) {
			t.Errorf("retryableStatus(%d) = false，期望 true", code)
		}
	}
	for _, code := range []int{200, 301, 400, 401, 403, 404, 408, 425, 428} {
		if retryableStatus(code) {
			t.Errorf("retryableStatus(%d) = true，期望 false", code)
		}
	}
}

// TestStatusErrorIsRetryStatusCarrier 证明 *StatusError 满足 core/retry.StatusCarrier：
// 通用组件的 RetryIfStatus 白名单因此能直接吃它，不必 import 本包（core 不 import runtime）。
func TestStatusErrorIsRetryStatusCarrier(t *testing.T) {
	var carrier retry.StatusCarrier = &StatusError{StatusCode: http.StatusTooManyRequests}
	if carrier.HTTPStatus() != http.StatusTooManyRequests {
		t.Errorf("HTTPStatus() = %d", carrier.HTTPStatus())
	}
	if !retry.RetryIfStatus(http.StatusTooManyRequests, 503)(&StatusError{StatusCode: 503}) {
		t.Error("RetryIfStatus(429,503) 不认 *StatusError(503)")
	}
	if retry.RetryIfStatus(http.StatusTooManyRequests, 503)(&StatusError{StatusCode: 400}) {
		t.Error("RetryIfStatus 放了 400")
	}
}

// TestProviderRetryPolicyShape 钉住 provider 级小重试的预算（有人把它改成"重试 50 次"
// 或"不分类全重试"时这里红）。
func TestProviderRetryPolicyShape(t *testing.T) {
	if providerRetryPolicy.MaxAttempts != 3 {
		t.Errorf("MaxAttempts = %d，期望 3（provider 级**小**重试）", providerRetryPolicy.MaxAttempts)
	}
	if providerRetryPolicy.RetryIf == nil {
		t.Error("RetryIf 不能为 nil（fail-close 缺省 = 一次都不重试，那不是 provider 想要的口径）")
	}
	if !providerRetryPolicy.RetryIf(&StatusError{StatusCode: 429}) {
		t.Error("RetryIf 不认 429")
	}
	if providerRetryPolicy.RetryIf(&StatusError{StatusCode: 400}) {
		t.Error("RetryIf 放了 400")
	}
	if providerRetryPolicy.MaxBackoff <= 0 || providerRetryPolicy.MaxBackoff > 2*time.Second {
		t.Errorf("MaxBackoff = %v，期望几百 ms 量级", providerRetryPolicy.MaxBackoff)
	}
}

// ---- 回归护栏：错误文案仍带状态码与 body ----

func TestStatusErrorTextAfterRetry(t *testing.T) {
	write, _ := statusThenOK(t, 99, http.StatusTooManyRequests, okNonStreamBody)
	_, p := newTestServer(t, nil, write)
	_, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, nil)
	if err == nil {
		t.Fatal("期望报错")
	}
	for _, want := range []string{"429", "transient", "retry:"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误文案缺 %q：%v", want, err)
		}
	}
}
