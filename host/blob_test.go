package host_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/store/blobfs"
)

// bigFileContent 是一个 ~9.6KB 的 ASCII 串：< read 工具的 64KB 上限（不被工具截断，
// 读回来逐字节 == 原文）> blob 阈值 1024（必被外置）。
const bigFileContent = "ALASOL-BLOB-E2E-"

func writeBigFile(t *testing.T) string {
	t.Helper()
	target := filepath.Join(t.TempDir(), "big.txt")
	content := strings.Repeat(bigFileContent, 600) // 16*600 = 9600 字节
	if err := os.WriteFile(target, []byte(content), 0o600); err != nil {
		t.Fatalf("写大文件: %v", err)
	}
	return target
}

// eventsFor 经 GET /api/events 读回全部 durable 事件（host 不暴露裸 store，走事件回放面）。
func eventsFor(t *testing.T, srv *httptest.Server) []contracts.Event {
	t.Helper()
	var ev struct {
		Events []contracts.Event `json:"events"`
	}
	getJSON(t, srv.URL+"/api/events?from=1&limit=500", &ev)
	return ev.Events
}

func completedEvent(t *testing.T, evs []contracts.Event) contracts.Event {
	t.Helper()
	for _, ev := range evs {
		if ev.Kind == "tool.call.completed" {
			return ev
		}
	}
	t.Fatal("事件流里没有 tool.call.completed")
	return contracts.Event{}
}

// TestD4BlobExternalizationEndToEnd：大工具输出外置端到端——BinaryRef 非空 + 落 blob +
// 读侧 GET /api/blobs/{id} 取回全文 == 原文；payload 是摘要（截断到 256）；X1 三段序不变。
func TestD4BlobExternalizationEndToEnd(t *testing.T) {
	target := writeBigFile(t)
	content := strings.Repeat(bigFileContent, 600)

	blobDir := filepath.Join(t.TempDir(), "blobs")
	h := newTestHub(t,
		withProvider(`{"path":"`+filepath.ToSlash(target)+`"}`),
		func(c *host.Config) {
			c.BlobDir = blobDir
			c.BlobMaxInline = 1024 // 小阈值 → 9600 字节的 read 结果必外置
		},
	)
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "读一下大文件"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	done := completedEvent(t, eventsFor(t, srv))
	if done.BinaryRef == nil {
		t.Fatal("大 result 应外置：completed.BinaryRef 不应为空")
	}

	// payload 是摘要：text = 「外置占位 + 前 256 字预览」，结构仍可解。
	var p contracts.ToolCallCompletedPayload
	if err := json.Unmarshal(done.Payload, &p); err != nil {
		t.Fatalf("completed payload 不可解: %v", err)
	}
	if len(p.Result.Content) != 1 || p.Result.Content[0].Text == nil {
		t.Fatalf("摘要应保留一个 text part: %+v", p.Result.Content)
	}
	summary := *p.Result.Content[0].Text
	if !strings.Contains(summary, "[已外置：全文见 blob ") {
		t.Fatalf("摘要应含外置占位标记，got %q", summary)
	}
	if !strings.Contains(summary, string(*done.BinaryRef)) {
		t.Fatalf("摘要占位应点名 blob id %s，got %q", *done.BinaryRef, summary)
	}
	preview := string([]rune(content)[:256])
	if !strings.HasSuffix(summary, preview) {
		t.Fatalf("摘要应以全文前 256 字收尾（预览），got %q", summary)
	}

	// 读侧端点取回全文（流式二进制）== 原文；Content-Type 是 application/json。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/blobs/"+string(*done.BinaryRef), nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/blobs: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/blobs 状态 = %d，want 200（body=%s）", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("blob Content-Type = %q，want application/json", ct)
	}
	var full contracts.ToolResult
	if err := json.Unmarshal(body, &full); err != nil {
		t.Fatalf("blob 内容不是合法 ToolResult JSON: %v", err)
	}
	if len(full.Content) != 1 || full.Content[0].Text == nil || *full.Content[0].Text != content {
		t.Fatalf("读侧取回全文与原文不符（got %d 字节，want %d）",
			len(textOf(full)), len(content))
	}
}

// textOf 取 ToolResult 的第一个 text（辅助断言）。
func textOf(res contracts.ToolResult) string {
	for _, p := range res.Content {
		if p.Text != nil {
			return *p.Text
		}
	}
	return ""
}

// TestD4BlobDisabledAcid：未配 blob（默认）→ completed 载荷与完整内联**逐字节等价**、BinaryRef 空。
func TestD4BlobDisabledAcid(t *testing.T) {
	target := writeBigFile(t)
	content := strings.Repeat(bigFileContent, 600)

	// 不传 blob 配置（默认关 = 零行为变化）。
	h := newTestHub(t, withProvider(`{"path":"`+filepath.ToSlash(target)+`"}`))
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "读一下大文件"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	done := completedEvent(t, eventsFor(t, srv))
	if done.BinaryRef != nil {
		t.Fatalf("关时 BinaryRef 应为空，got %v", *done.BinaryRef)
	}
	// 逐字节等价：payload == 完整 result 内联的 JSON（未被截断/改写）。
	want, err := json.Marshal(contracts.ToolCallCompletedPayload{
		ToolCallID: "toolu_SCRIPTED_01", // scriptedProvider 的黑盒 id（见 hub_test.go）
		Result:     contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &content}}},
	})
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if string(done.Payload) != string(want) {
		t.Fatalf("关时 completed 载荷应与完整内联逐字节等价：\ngot  %s\nwant %s", done.Payload, want)
	}

	// 读侧端点：未启用 → 404。
	code := rawGet(t, srv.URL+"/api/blobs/"+string(core.NewUlid()))
	if code != http.StatusNotFound {
		t.Fatalf("blob 未启用时 GET /api/blobs 应 404，got %d", code)
	}
}

// TestAPIBlobNotFoundWhenEnabled：启用了 blob 但 id 不存在 → 404（不是 500）。
func TestAPIBlobNotFoundWhenEnabled(t *testing.T) {
	h := newTestHub(t, nil, func(c *host.Config) {
		c.BlobDir = filepath.Join(t.TempDir(), "blobs")
		c.BlobMaxInline = 1024
	})
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	code := rawGet(t, srv.URL+"/api/blobs/"+string(core.NewUlid()))
	if code != http.StatusNotFound {
		t.Fatalf("id 不存在应 404，got %d", code)
	}
	// 非法 id → 400。
	if code := rawGet(t, srv.URL+"/api/blobs/not-a-ulid"); code != http.StatusBadRequest {
		t.Fatalf("非法 id 应 400，got %d", code)
	}
}

// TestAPIBlobBadIDMessageNamesBlobID（F11）：非法 blob id 的 400 文案**点名 blob id**，
// 不许串味成"session_id 不是合法 ULID"。
//
// 为什么钉文案：调用方传的是 blob id，报错却说 session_id 不合法 → 会把人引到错误的参数上
// （"是不是 session_id 写错了？"）。文案串味是**说错事实**，比报错更误导人。
func TestAPIBlobBadIDMessageNamesBlobID(t *testing.T) {
	h := newTestHub(t, nil, func(c *host.Config) {
		c.BlobDir = filepath.Join(t.TempDir(), "blobs")
		c.BlobMaxInline = 1024
	})
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	var te contracts.TransportError
	code := getJSON(t, srv.URL+"/api/blobs/not-a-ulid", &te)
	if code != http.StatusBadRequest {
		t.Fatalf("非法 id 应 400，got %d", code)
	}
	if te.Code != "bad_blob_id" {
		t.Fatalf("错误码 = %q，want bad_blob_id", te.Code)
	}
	if !strings.Contains(te.Message, "blob id") {
		t.Fatalf("400 文案应点名 blob id，got %q", te.Message)
	}
	if strings.Contains(te.Message, "session_id") {
		t.Fatalf("400 文案串味成 session_id：%q", te.Message)
	}
}

// rawGet 发一个 GET 只返回状态码（读原始字节响应用）。
func rawGet(t *testing.T, url string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

// TestSetWithBlobfsComposition：core/storage.Set 挂上真实 blobfs 实现并取用（组合面；
// Set 不改形状，换实现零改动——本测试证明 blob 槽位接的就是 core/storage.Blob）。
func TestSetWithBlobfsComposition(t *testing.T) {
	ctx := context.Background()
	bs, err := blobfs.New(filepath.Join(t.TempDir(), "blobs"))
	if err != nil {
		t.Fatalf("blobfs.New: %v", err)
	}
	set := storage.Set{Blob: bs}

	// Kinds 报"挂了 blob"（诊断/酸测试面）。
	kinds := set.Kinds()
	if len(kinds) != 1 || kinds[0] != "blob" {
		t.Fatalf("Set.Kinds() = %v，want [blob]", kinds)
	}

	// 经 Set.Blob（接口面）取用：Put → Get 往返（证明槽位接的就是可用实现）。
	content := []byte(`{"via":"storage.Set.Blob 取用"}`)
	meta, err := set.Blob.Put(ctx, storage.BlobMeta{MediaType: "application/json"}, strings.NewReader(string(content)))
	if err != nil {
		t.Fatalf("Set.Blob.Put: %v", err)
	}
	rc, err := set.Blob.Get(ctx, meta.ID)
	if err != nil {
		t.Fatalf("Set.Blob.Get: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != string(content) {
		t.Fatalf("Set.Blob 往返内容不符: %q", got)
	}
	// Ref 挂接点：meta.Ref() 就是 Event.binary_ref 的取值形态。
	if ref := meta.Ref(); ref == nil || *ref != meta.ID {
		t.Fatalf("meta.Ref() 应折出 id %s", meta.ID)
	}
}
