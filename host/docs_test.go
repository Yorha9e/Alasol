package host_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	coregraph "github.com/Yorha9e/alasol/core/graph"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// 文档族（G2 第 5 族）宿主装配面测试：Set 槽位承载、关 = 零行为变化、开 = HTTP 往返。
// 形态照 vectors / blob 的端点测试（newTestHub 起 hub、httptest.NewServer 挂 Handler）。

// TestDocStoreSetSlotComposition：core/storage.Set 挂上真实 DocStore（事件库 = 真
// sqlite.Store）——DocumentWriter() 取写面 ok=true，经**接口面**跑 Upsert→Get 往返
// （形态照 TestSetWithVecmemComposition：Set 不改形状，消费方只认接口，换实现零改动）。
func TestDocStoreSetSlotComposition(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "events.db")
	st, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ds, err := sqlite.NewDocStore(st)
	if err != nil {
		t.Fatalf("NewDocStore: %v", err)
	}
	if err := ds.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	set := storage.Set{EventLog: st, Documents: ds}

	// Kinds 报"挂了 documents"（诊断/酸测试面）。
	kinds := set.Kinds()
	if len(kinds) != 2 || kinds[0] != "eventlog" || kinds[1] != "documents" {
		t.Fatalf("Set.Kinds() = %v，want [eventlog documents]", kinds)
	}
	// DocStore 同时实现读面与写面 → 取面器 ok=true（不许硬转型：只挂读面是合法组合）。
	w, ok := set.DocumentWriter()
	if !ok {
		t.Fatal("DocStore 同时实现读面与写面，DocumentWriter() 应 ok=true")
	}
	in := storage.Doc{
		Collection: "project_rules",
		Key:        "f7",
		Body:       json.RawMessage(`{"rule":"只增不删"}`),
		Revision:   1,
	}
	// 写面经接口 + Transact 事务跑道（宿主 ingest 面的形态缩影）。
	if err := ds.Transact(ctx, func(tx coregraph.Tx) error {
		return w.Upsert(ctx, tx, []storage.Doc{in})
	}); err != nil {
		t.Fatalf("Set.DocumentWriter().Upsert: %v", err)
	}
	// 经 Set.Documents（接口面）读回：字段逐字往返。
	got, err := set.Documents.Get(ctx, "project_rules", "f7")
	if err != nil {
		t.Fatalf("Set.Documents.Get: %v", err)
	}
	if got.Collection != in.Collection || got.Key != in.Key || got.Revision != in.Revision ||
		string(got.Body) != string(in.Body) {
		t.Fatalf("Set 文档槽位往返不对: got=%+v want=%+v", got, in)
	}
	// 缺失 → ErrNotFound（契约哨兵经接口面仍可判）。
	if _, err := set.Documents.Get(ctx, "project_rules", "nope"); err == nil {
		t.Fatal("缺失文档应报错")
	}
}

// TestDocsDisabledZeroBehaviorChange：**酸测试**——DocumentsEnabled=false（默认）起 hub，
// 四个端点**路由不注册** → 路径认不得 → 404 not_found（与 vectors 关闭口径同款）。
func TestDocsDisabledZeroBehaviorChange(t *testing.T) {
	h := newTestHub(t) // 不配 DocumentsEnabled（默认 false）
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	var errBody struct {
		Code string `json:"code"`
	}
	for _, tc := range []struct {
		name, method, url string
		body              any
	}{
		{"get 单条", "GET", srv.URL + "/api/docs/c/k", nil},
		{"list", "GET", srv.URL + "/api/docs", nil},
		{"upsert", "POST", srv.URL + "/api/docs/upsert", map[string]any{"docs": []any{}}},
		{"delete", "POST", srv.URL + "/api/docs/delete", map[string]any{"collection": "c", "keys": []string{"k"}}},
	} {
		code, raw := docRequest(t, tc.method, tc.url, tc.body)
		if code != http.StatusNotFound {
			t.Fatalf("%s：未启用文档时应 404（路由不注册），得到 %d", tc.name, code)
		}
		if err := json.Unmarshal([]byte(raw), &errBody); err != nil {
			t.Fatalf("%s：错误体应是单个合法 JSON（得到 %q）: %v", tc.name, raw, err)
		}
		if errBody.Code != "not_found" {
			t.Fatalf("%s：未启用时路径不认得（错误码 not_found），得到 %q", tc.name, errBody.Code)
		}
	}
}

// TestDocsHTTPEndToEnd：enabled=true 走 HTTP 全链路——
// upsert → get → list（prefix / limit 各验一条）→ delete → get 404；
// 冲突 upsert → 409；负 limit → 400。
func TestDocsHTTPEndToEnd(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) { c.DocumentsEnabled = true })
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	// ① upsert 三条（同集合两键 + 另一集合一键）。
	up := map[string]any{"docs": []map[string]any{
		{"collection": "rules", "key": "a1", "body": map[string]int{"n": 1}, "revision": 1},
		{"collection": "rules", "key": "b2", "body": map[string]int{"n": 2}, "revision": 1},
		{"collection": "know", "key": "z9", "revision": 1},
	}}
	var okBody map[string]bool
	if code := postJSON(t, srv.URL+"/api/docs/upsert", up, &okBody); code != http.StatusOK {
		t.Fatalf("upsert 状态码 = %d，want 200", code)
	}
	if !okBody["ok"] {
		t.Fatalf("upsert 应回 {ok:true}，得到 %v", okBody)
	}

	// ② get 单条：字段往返（body 原始 JSON）。
	var doc storage.Doc
	if code := getJSON(t, srv.URL+"/api/docs/rules/a1", &doc); code != http.StatusOK {
		t.Fatalf("get 状态码 = %d，want 200", code)
	}
	if doc.Collection != "rules" || doc.Key != "a1" || doc.Revision != 1 || !json.Valid(doc.Body) {
		t.Fatalf("get 往返不对: %+v", doc)
	}
	// 不存在 → 404。
	if code := getJSON(t, srv.URL+"/api/docs/rules/nope", nil); code != http.StatusNotFound {
		t.Fatalf("缺失文档 get 应 404，得到 %d", code)
	}

	// ③ list：prefix 过滤命中一条；limit 截断取一条（排序 = (collection,key) 字节序）。
	var lst struct {
		Docs []storage.Doc `json:"docs"`
	}
	if code := getJSON(t, srv.URL+"/api/docs?collection=rules&prefix=a", &lst); code != http.StatusOK {
		t.Fatalf("list(prefix) 状态码 = %d，want 200", code)
	}
	if len(lst.Docs) != 1 || lst.Docs[0].Key != "a1" {
		t.Fatalf("list prefix=a 应只命中 rules/a1: %+v", lst.Docs)
	}
	if code := getJSON(t, srv.URL+"/api/docs?collection=rules&limit=1", &lst); code != http.StatusOK {
		t.Fatalf("list(limit) 状态码 = %d，want 200", code)
	}
	if len(lst.Docs) != 1 || lst.Docs[0].Key != "a1" {
		t.Fatalf("list limit=1 应截断到排序后首条 rules/a1: %+v", lst.Docs)
	}
	// 空结果 = 空数组**不是 null**：对**原始响应体**断言。
	//
	// 为什么不能只断言解码后的切片长度（假绿）：JSON 的 null 解码进 []storage.Doc 也是
	// nil、len 0 —— 那样断言在"服务端回 {"docs":null}" 时照样绿，而前端拿到 null 一样炸。
	// 断言原始体里含 `"docs":[]` 才真的钉住"空 = 空数组"。
	code, raw := docRequest(t, http.MethodGet, srv.URL+"/api/docs?collection=empty", nil)
	if code != http.StatusOK {
		t.Fatalf("空集合 list 应 200，得到 %d", code)
	}
	if !strings.Contains(raw, `"docs":[]`) {
		t.Fatalf("空集合 list 的 docs 应是空数组不是 null，响应体=%q", raw)
	}

	// ④ 冲突 upsert：库里 rules/a1 rev=1，写 rev=9 → 409（整批拒绝）。
	conflict := map[string]any{"docs": []map[string]any{
		{"collection": "rules", "key": "fresh", "revision": 1},
		{"collection": "rules", "key": "a1", "revision": 9},
	}}
	if code := postJSON(t, srv.URL+"/api/docs/upsert", conflict, nil); code != http.StatusConflict {
		t.Fatalf("版本冲突 upsert 应 409，得到 %d", code)
	}
	// 冲突批次零写入（fresh 不出现）。
	if code := getJSON(t, srv.URL+"/api/docs/rules/fresh", nil); code != http.StatusNotFound {
		t.Fatalf("冲突批次零写入（fresh 不该出现），得到 %d", code)
	}

	// ⑤ 负 limit → 400（契约：List 负 Limit = ErrBadQuery）。
	if code := getJSON(t, srv.URL+"/api/docs?limit=-1", nil); code != http.StatusBadRequest {
		t.Fatalf("负 limit 应 400，得到 %d", code)
	}

	// ⑥ delete → 200（幂等：再删一次仍 200）→ get 404。
	del := map[string]any{"collection": "rules", "keys": []string{"a1", "b2"}}
	if code := postJSON(t, srv.URL+"/api/docs/delete", del, &okBody); code != http.StatusOK {
		t.Fatalf("delete 状态码 = %d，want 200", code)
	}
	if !okBody["ok"] {
		t.Fatalf("delete 应回 {ok:true}，得到 %v", okBody)
	}
	if code := postJSON(t, srv.URL+"/api/docs/delete", del, nil); code != http.StatusOK {
		t.Fatalf("重复 delete 应幂等 200，得到 %d", code)
	}
	if code := getJSON(t, srv.URL+"/api/docs/rules/a1", nil); code != http.StatusNotFound {
		t.Fatalf("删后 get 应 404，得到 %d", code)
	}
	// 未删的另一集合键还在（delete 只动指定集合的指定键）。
	if code := getJSON(t, srv.URL+"/api/docs/know/z9", nil); code != http.StatusOK {
		t.Fatalf("未删的 know/z9 应仍在（200），得到 %d", code)
	}
}

// docRequest 发一个 GET（body=nil）或 POST（body 非 nil）请求，返回状态码与原始响应体。
// 形态照 postJSONRaw / getJSONRaw（vectors 测试的 helper）。
func docRequest(t *testing.T, method, url string, body any) (int, string) {
	t.Helper()
	var (
		resp *http.Response
		err  error
	)
	if method == http.MethodGet {
		resp, err = http.Get(url) //nolint:gosec // 测试用本地 httptest URL
	} else {
		raw, merr := json.Marshal(body)
		if merr != nil {
			t.Fatalf("marshal body: %v", merr)
		}
		resp, err = http.Post(url, "application/json", strings.NewReader(string(raw)))
	}
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读响应体: %v", err)
	}
	return resp.StatusCode, string(b)
}

// TestDocsListCollectionByteExact（R1）：HTTP 面的 collection 过滤**字节精确、不 trim**。
//
// " rules"（带前导空格）与 "rules" 是两个不同的集合（陷阱 17）：查询参数悄悄 trim
// 等于把"查不到的集合"变成"查另一个集合"——答案看着对、其实查错了对象。
// 把 serveDocList 的 TrimSpace 加回来，本用例必须红（变异锚）。
func TestDocsListCollectionByteExact(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) { c.DocumentsEnabled = true })
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	up := map[string]any{"docs": []map[string]any{
		{"collection": "rules", "key": "k1", "revision": 1},
		{"collection": " rules", "key": "k2", "revision": 1},
	}}
	if code := postJSON(t, srv.URL+"/api/docs/upsert", up, nil); code != http.StatusOK {
		t.Fatalf("upsert 状态码 = %d，want 200", code)
	}

	// collection=rules（无空格）→ 只命中 rules/k1，绝不能串到 " rules"/k2。
	code, raw := docRequest(t, http.MethodGet, srv.URL+"/api/docs?collection=rules", nil)
	if code != http.StatusOK {
		t.Fatalf("list(rules) 应 200，得到 %d", code)
	}
	if !strings.Contains(raw, `"k1"`) || strings.Contains(raw, `"k2"`) {
		t.Fatalf("collection=rules 应只命中 k1，响应体=%q", raw)
	}
	// collection=%20rules（前导空格）→ 只命中 " rules"/k2；若 trim 了就会串到 rules 集合。
	code, raw = docRequest(t, http.MethodGet, srv.URL+"/api/docs?collection=%20rules", nil)
	if code != http.StatusOK {
		t.Fatalf("list(' rules') 应 200，得到 %d", code)
	}
	if !strings.Contains(raw, `"k2"`) || strings.Contains(raw, `"k1"`) {
		t.Fatalf("collection=' rules' 应只命中 k2（不 trim），响应体=%q", raw)
	}
}
