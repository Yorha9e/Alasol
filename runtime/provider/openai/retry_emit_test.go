package openai

// 缺口 5 第二件（B：provider 重试留痕）的钉子：429/5xx 重试发生时经 **emit 通道**发
// provider.retry 事件（attempt / 错误摘要 / 退避时长），不给 provider 穿 WAL 句柄。
//
// 口径（core/provider.go 与 provider.go 的 doWithRetry）：
//   - 时点 = **第一次失败后、第 n 次尝试发起前**（attempt = 2..n；首次尝试不发）；
//   - 错误摘要 = statusErrorOf 口径（1KB 截断、**不含凭据**——Authorization 从不进错误文案）；
//   - 纯观测：重试成功时最终结果与不留痕路径一字不差；
//   - embeddings 路径（embed.go）传 nil emit → 不留痕（挂账，见 embed.go）。

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/core"
)

// TestRunEmitsProviderRetryOn429（变异自证②：去掉 doWithRetry 里的 emit → 本用例红）：
// 429 重试一次 → 恰好一条 provider.retry（attempt=2），随后正常 done；重试成功的结果
// 不受影响（留痕纯观测）。
func TestRunEmitsProviderRetryOn429(t *testing.T) {
	write, requests := statusThenOK(t, 1, http.StatusTooManyRequests, okNonStreamBody)
	_, p := newTestServer(t, nil, write) // newTestServer 注入 APIKey="test-key"（凭据断言用）

	emit, evs := collectEvents()
	res, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit)
	if err != nil {
		t.Fatalf("429 后应重试成功：%v", err)
	}
	if requests() != 2 {
		t.Fatalf("请求 %d 次，期望 2", requests())
	}
	// 重试成功 → 最终结果与不留痕路径一致（纯观测，不改控制流）。
	if res.Text != "成了" {
		t.Errorf("Text = %q（重试留痕不该影响结果）", res.Text)
	}

	k := kinds(*evs)
	if len(k) != 3 || k[0] != KindProviderRetry || k[1] != KindTextDelta || k[2] != KindDone {
		t.Fatalf("事件序列 = %v，期望 [provider.retry text.delta done]", k)
	}
	var retryEv *core.ProviderEvent
	for i := range *evs {
		if (*evs)[i].Kind == KindProviderRetry {
			retryEv = &(*evs)[i]
			break
		}
	}
	if retryEv == nil {
		t.Fatal("没收到 provider.retry 事件")
	}
	if retryEv.Retry == nil {
		t.Fatal("provider.retry 事件没有载荷（Retry=nil）")
	}
	r := retryEv.Retry
	if r.Attempt != 2 {
		t.Errorf("Attempt = %d，期望 2（第一次重试）", r.Attempt)
	}
	// 错误摘要：429 的 statusErrorOf 口径（状态行 + 截断响应体）。
	if !strings.Contains(r.Error, "429") {
		t.Errorf("错误摘要缺状态码 429：%q", r.Error)
	}
	if !strings.Contains(r.Error, "rate") && !strings.Contains(r.Error, "transient") {
		t.Errorf("错误摘要应带响应体线索：%q", r.Error)
	}
	// **无凭据泄露**：newTestServer 的 APIKey 是 "test-key"，摘要里不得出现
	//（Authorization 头从不进错误文案——statusErrorOf 口径）。
	if strings.Contains(r.Error, "test-key") || strings.Contains(r.Error, "Authorization") ||
		strings.Contains(r.Error, "Bearer") {
		t.Errorf("错误摘要泄露凭据：%q", r.Error)
	}
	// 退避时长 = 实测（上次失败 → 本次尝试开始；100ms 起步 + 抖动）。
	if r.Backoff <= 0 {
		t.Errorf("Backoff = %v，期望实测 >0", r.Backoff)
	}
}

// TestRunEmitsProviderRetryPerAttempt：持续 500 → 两次重试（attempt=2、3）各留一条痕，
// 序号递增、载荷各自带上次失败的摘要。
func TestRunEmitsProviderRetryPerAttempt(t *testing.T) {
	write, _ := statusThenOK(t, 99, http.StatusInternalServerError, okNonStreamBody)
	_, p := newTestServer(t, nil, write)

	emit, evs := collectEvents()
	if _, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit); err == nil {
		t.Fatal("持续 500 必须报错")
	}
	var attempts []int
	var errs []string
	for _, ev := range *evs {
		if ev.Kind != KindProviderRetry {
			continue
		}
		if ev.Retry == nil {
			t.Fatalf("provider.retry 没带载荷：%+v", ev)
		}
		attempts = append(attempts, ev.Retry.Attempt)
		errs = append(errs, ev.Retry.Error)
	}
	if len(attempts) != 2 || attempts[0] != 2 || attempts[1] != 3 {
		t.Fatalf("重试留痕的 attempt 序 = %v，期望 [2 3]（3 次尝试 = 两次重试）", attempts)
	}
	for i, e := range errs {
		if !strings.Contains(e, "500") {
			t.Errorf("第 %d 条留痕的错误摘要缺 500：%q", i+1, e)
		}
	}
}

// TestRunNoProviderRetryOn4xx：4xx 不重试 → 一条 provider.retry 都没有（留痕的时点是
// "决定重试时"，没重试就没得留）。
func TestRunNoProviderRetryOn4xx(t *testing.T) {
	write, _ := statusThenOK(t, 99, http.StatusBadRequest, okNonStreamBody)
	_, p := newTestServer(t, nil, write)

	emit, evs := collectEvents()
	if _, err := p.Run(context.Background(), core.ModelRequest{Model: "m"}, emit); err == nil {
		t.Fatal("400 必须报错")
	}
	for _, ev := range *evs {
		if ev.Kind == KindProviderRetry {
			t.Fatalf("4xx 不该留重试痕（不重试）：%+v", ev)
		}
	}
}
