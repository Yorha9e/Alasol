package host_test

// CORS 来源白名单（**opt-in**）的验收面：
//
//	① 不配白名单 → **一个 CORS 头都不发**（GET 无 ACAO、OPTIONS 无 CORS 头）——
//	   零行为变化（host/doc.go「前端接入方式（G5）」的原口径继续成立）；
//	② 配了 `http://127.0.0.1:3080` → 带该 Origin 的 GET 回 ACAO（原样）+ Vary: Origin；
//	   OPTIONS preflight → 204 + 放行头（ACAO / Allow-Methods / Allow-Headers / Vary）；
//	③ 未命中 Origin（http://evil.example）/ 无 Origin（curl）→ **无 ACAO**（fail-close）；
//	④ `*` 通配条目、缺 scheme、带路径/尾斜杠的条目 → **启动即拒**（fail-close）；
//	⑤ 协调四面样例（/api/gates、/api/presence、/api/merge/check）各带一次命中 Origin
//	   → 都带头；未命中路由的 404 也带头（前端 preflight 会打到任意路径）；
//	⑥ 静态面（GET /）与 /ws 握手 → **不带 ACAO**（范围钉死）。
//
// 另有一条：TOML `cors_origins` 是**合法键**（unknown-key 闸门是反射派生的），
// 且能被 LoadFileConfig 读出来。

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/host"
)

// corsTestOrigin 是本文件用的命中白名单的 origin（GUI 联调缺口的真实场景：
// dsh 前端在 http://127.0.0.1:3080 浏览器直连协调四面）。
const corsTestOrigin = "http://127.0.0.1:3080"

// corsDo 发一个请求（可带 Origin 头 / 可带体），回响应（调用方负责关 Body）。
func corsDo(t *testing.T, srv *httptest.Server, method, path, origin, body string) *http.Response {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, rdr)
	if err != nil {
		t.Fatalf("建请求 %s %s: %v", method, path, err)
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

// assertNoCORSHeaders 断言响应**一个 CORS 头都没有**（零行为变化 / fail-close 口径）。
func assertNoCORSHeaders(t *testing.T, resp *http.Response, what string) {
	t.Helper()
	defer resp.Body.Close()
	for _, h := range []string{
		"Access-Control-Allow-Origin",
		"Access-Control-Allow-Methods",
		"Access-Control-Allow-Headers",
	} {
		if v := resp.Header.Get(h); v != "" {
			t.Errorf("%s：不该有 %s 头（得到 %q）", what, h, v)
		}
	}
	if v := resp.Header.Get("Vary"); strings.Contains(strings.ToLower(v), "origin") {
		t.Errorf("%s：不该有 Vary: Origin（得到 %q）", what, v)
	}
}

// assertCORSOrigin 断言响应带 ACAO（**原样回显**该 origin）+ Vary: Origin。
func assertCORSOrigin(t *testing.T, resp *http.Response, origin, what string) {
	t.Helper()
	defer resp.Body.Close()
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != origin {
		t.Errorf("%s：Access-Control-Allow-Origin = %q，want %q（原样回显）", what, got, origin)
	}
	if v := resp.Header.Get("Vary"); !strings.Contains(strings.ToLower(v), "origin") {
		t.Errorf("%s：应有 Vary: Origin（得到 %q）", what, v)
	}
}

// TestCORSDisabledByDefault：不配白名单 → **零行为变化**（一个 CORS 头都不发）。
//
// 现状口径钉死：GET /api/sessions 即使带了 Origin 头也不回 ACAO；OPTIONS 不带任何
// CORS 头（状态码维持现状 = 405，路由不认 OPTIONS）。
func TestCORSDisabledByDefault(t *testing.T) {
	h := newTestHub(t)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	resp := corsDo(t, srv, "GET", "/api/sessions", corsTestOrigin, "")
	assertNoCORSHeaders(t, resp, "未配白名单的 GET")

	// OPTIONS：现状 = 405（路径认得、方法不符），且**不带**任何 CORS 头。
	resp = corsDo(t, srv, "OPTIONS", "/api/sessions", corsTestOrigin, "")
	assertNoCORSHeaders(t, resp, "未配白名单的 OPTIONS")
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("未配白名单的 OPTIONS 状态码 = %d，want 405（现状口径：路由不认 OPTIONS）",
			resp.StatusCode)
	}
}

// TestCORSAllowedOriginEchoesAndVaries：配了白名单 → 带该 Origin 的 GET 回 ACAO
// （原样）+ Vary: Origin；OPTIONS preflight → 204 + 放行头。
func TestCORSAllowedOriginEchoesAndVaries(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) {
		c.CORSOrigins = []string{corsTestOrigin}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// ① GET：ACAO 原样回显 + Vary: Origin。
	resp := corsDo(t, srv, "GET", "/api/sessions", corsTestOrigin, "")
	assertCORSOrigin(t, resp, corsTestOrigin, "命中白名单的 GET")

	// ② OPTIONS preflight：204 + 放行头（方法限 GET/POST/OPTIONS、请求头限 Content-Type）。
	resp = corsDo(t, srv, "OPTIONS", "/api/sessions", corsTestOrigin, "")
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("preflight 状态码 = %d，want 204", resp.StatusCode)
	}
	assertCORSOrigin(t, resp, corsTestOrigin, "preflight")
	if got := resp.Header.Get("Access-Control-Allow-Methods"); got != "GET, POST, OPTIONS" {
		t.Errorf("Access-Control-Allow-Methods = %q，want \"GET, POST, OPTIONS\"", got)
	}
	if got := resp.Header.Get("Access-Control-Allow-Headers"); got != "Content-Type" {
		t.Errorf("Access-Control-Allow-Headers = %q，want \"Content-Type\"", got)
	}
	resp.Body.Close()
}

// TestCORSFailsClosedOnUnmatchedOrNoOrigin：未命中 Origin / 无 Origin → **无 ACAO**。
func TestCORSFailsClosedOnUnmatchedOrNoOrigin(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) {
		c.CORSOrigins = []string{corsTestOrigin}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// ① 未命中（fail-close）：别的源的页面读不到响应。
	resp := corsDo(t, srv, "GET", "/api/sessions", "http://evil.example", "")
	assertNoCORSHeaders(t, resp, "未命中 Origin 的 GET")

	// ② 无 Origin（curl / CLI）：不加头（非浏览器客户端本来就不受 CORS 约束）。
	resp = corsDo(t, srv, "GET", "/api/sessions", "", "")
	assertNoCORSHeaders(t, resp, "无 Origin 的 GET")

	// ③ 未命中的 preflight → 现状（405），且不带任何 CORS 头。
	resp = corsDo(t, srv, "OPTIONS", "/api/sessions", "http://evil.example", "")
	assertNoCORSHeaders(t, resp, "未命中 Origin 的 OPTIONS")
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("未命中 Origin 的 OPTIONS 状态码 = %d，want 405（现状口径）", resp.StatusCode)
	}
}

// TestCORSInvalidOriginRejectedAtStartup：非法条目 → **启动即拒**（fail-close）。
//
// 覆盖：`*` 通配、缺 scheme、带路径、带尾斜杠、缺端口、错 scheme、空条目。
func TestCORSInvalidOriginRejectedAtStartup(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"通配 *", "*"},
		{"含 * 的条目", "http://*.example.com"},
		{"缺 scheme", "127.0.0.1:3080"},
		{"带路径", "http://127.0.0.1:3080/app"},
		{"带尾斜杠", "http://127.0.0.1:3080/"},
		{"缺端口", "http://127.0.0.1"},
		{"错 scheme", "ftp://127.0.0.1:3080"},
		{"带查询串", "http://127.0.0.1:3080?x=1"},
		{"空条目", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := host.New(host.Config{
				DBPath:      filepath.Join(t.TempDir(), "events.db"),
				Model:       "scripted",
				CORSOrigins: []string{c.raw},
			})
			if err == nil {
				t.Fatalf("CORSOrigins = %q 应被拒（启动 fail-close），实际装配成功", c.raw)
			}
			if !strings.Contains(err.Error(), "CORSOrigins") {
				t.Errorf("报错应点名 CORSOrigins（实际：%v）", err)
			}
		})
	}
}

// TestCORSValidOriginAccepted：合法条目 → 装配成功（启动不拦）。
func TestCORSValidOriginAccepted(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) {
		c.CORSOrigins = []string{corsTestOrigin, "https://dsh.example:8443"}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	resp := corsDo(t, srv, "GET", "/api/sessions", "https://dsh.example:8443", "")
	assertCORSOrigin(t, resp, "https://dsh.example:8443", "第二个命中白名单的 origin")
}

// TestCORSCoordinationEndpoints：协调四面样例各带一次命中 Origin → 都带头。
//
// 这四个端点正是 GUI 联调缺口的实际诉求（dsh 前端直连）：
// /api/gates、/api/presence、/api/merge/check（另含 /api/gates/join 等写面）。
//
// 顺带钉住：**未命中路由的 404 也带头**（前端 preflight 会打到任意路径）。
func TestCORSCoordinationEndpoints(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) {
		c.CORSOrigins = []string{corsTestOrigin}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	cases := []struct {
		method string
		path   string
		body   string
	}{
		{"GET", "/api/gates", ""},
		{"GET", "/api/presence", ""},
		{"POST", "/api/merge/check", `{}`},
		{"GET", "/api/gates/nonexistent-path", ""}, // 未命中路由 → 404（也要带头）
	}
	for _, c := range cases {
		resp := corsDo(t, srv, c.method, c.path, corsTestOrigin, c.body)
		assertCORSOrigin(t, resp, corsTestOrigin, c.method+" "+c.path)
	}
}

// TestCORSPreflightOnAnyPath：preflight 打到**任意** /api 路径（含**不存在的**路径）→
// 204 + 放行头（前端拿不准端点是否存在时也会先 preflight）。
func TestCORSPreflightOnAnyPath(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) {
		c.CORSOrigins = []string{corsTestOrigin}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	for _, path := range []string{
		"/api/sessions",
		"/api/gates",
		"/api/does-not-exist",
		"/api/does/not/exist/either",
		"/api",
	} {
		resp := corsDo(t, srv, "OPTIONS", path, corsTestOrigin, "")
		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("OPTIONS %s 状态码 = %d，want 204", path, resp.StatusCode)
		}
		assertCORSOrigin(t, resp, corsTestOrigin, "OPTIONS "+path)
		if got := resp.Header.Get("Access-Control-Allow-Methods"); got != "GET, POST, OPTIONS" {
			t.Errorf("OPTIONS %s：Access-Control-Allow-Methods = %q，want \"GET, POST, OPTIONS\"",
				path, got)
		}
		if got := resp.Header.Get("Access-Control-Allow-Headers"); got != "Content-Type" {
			t.Errorf("OPTIONS %s：Access-Control-Allow-Headers = %q，want \"Content-Type\"",
				path, got)
		}
		resp.Body.Close()
	}
}

// TestCORSStaticAndWSNotExposed：静态面（GET /）与 /ws 握手 → **不带 ACAO**（范围钉死）。
//
// 静态面是同源托管的页面，不该被跨源页面读；/ws 的 Origin 校验维持现状
// （runtime/transport.checkOrigin：跨源页面 403 origin_rejected）——事件流走 driver，
// 不走浏览器直连。
func TestCORSStaticAndWSNotExposed(t *testing.T) {
	h := newTestHub(t, func(c *host.Config) {
		c.CORSOrigins = []string{corsTestOrigin}
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	// ① 静态面：GET /（index.html）。
	resp := corsDo(t, srv, "GET", "/", corsTestOrigin, "")
	assertNoCORSHeaders(t, resp, "静态面 GET /")

	// ② 静态面：GET /index.html。
	resp = corsDo(t, srv, "GET", "/index.html", corsTestOrigin, "")
	assertNoCORSHeaders(t, resp, "静态面 GET /index.html")

	// ③ /ws 握手（非 WS 升级请求 → 现状 403 origin_rejected）：不带 ACAO。
	resp = corsDo(t, srv, "GET", "/ws", corsTestOrigin, "")
	assertNoCORSHeaders(t, resp, "/ws 握手")
}

// TestCORSFileConfigKey：TOML `cors_origins` 是**合法键**（unknown-key 闸门反射派生）
// 且能被读出来。
func TestCORSFileConfigKey(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.toml")
	body := "model = \"m\"\ncors_origins = [\"http://127.0.0.1:3080\", \"https://dsh.example\"]\n"
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	fc, err := host.LoadFileConfig(p)
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	if fc == nil {
		t.Fatal("LoadFileConfig 返回 nil")
	}
	if len(fc.CORSOrigins) != 2 ||
		fc.CORSOrigins[0] != corsTestOrigin ||
		fc.CORSOrigins[1] != "https://dsh.example" {
		t.Fatalf("cors_origins 解析不对: %v", fc.CORSOrigins)
	}
}
