package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakeAddr 是测试用的 net.Addr：String() 原样返回注入的 "host:port"，
// 用来在 httptest 请求上伪造 http.LocalAddrContextKey（真实监听地址）。
type fakeAddr string

func (a fakeAddr) Network() string { return "tcp" }
func (a fakeAddr) String() string  { return string(a) }

// newOriginReq 造一个 checkOrigin 的输入：Origin 头 + 请求 Host + 可选的本地地址。
// local 为空 = 上下文里不放 http.LocalAddrContextKey（模拟拿不到真实监听地址）。
func newOriginReq(origin, host, local string) *http.Request {
	r := httptest.NewRequest("GET", "/ws", nil)
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	r.Host = host
	if local != "" {
		r = r.WithContext(context.WithValue(r.Context(), http.LocalAddrContextKey, fakeAddr(local)))
	}
	return r
}

// TestSameOriginStringEqual 钉死同源判据：**只**是 Origin host 与请求 Host 的
// 字符串相等（大小写不敏感）。loopback 拼写差异不归它管——那走 loopbackMutual 的
// 三元比对（见 TestCheckOriginLocalAddrMatrix）。
//
// 注：旧版 sameOrigin 把 loopback 互认也塞在这一个函数里；注入本地地址后拆成两道
// 判据，这里按新口径只钉字符串相等（跨拼写格一律 false，堵洞收紧）。
func TestSameOriginStringEqual(t *testing.T) {
	cases := []struct {
		name                string
		originHost, reqHost string
		want                bool
	}{
		{"完全相同", "localhost:8720", "localhost:8720", true},
		{"大小写不敏感", "LocalHost:8720", "localhost:8720", true},
		{"无端口两端 localhost", "localhost", "localhost", true},
		{"localhost→127.0.0.1 不是同源（互认另判，堵洞收紧后不再由本函数放行）", "localhost:8720", "127.0.0.1:8720", false},
		{"[::1]→127.0.0.1 不是同源（堵洞收紧）", "[::1]:8720", "127.0.0.1:8720", false},
		{"127.0.0.2→127.0.0.1 不是同源", "127.0.0.2:8720", "127.0.0.1:8720", false},
		{"evil.com 跨源", "evil.com", "localhost:8720", false},
		{"不同端口", "localhost:9999", "localhost:8720", false},
	}
	for _, c := range cases {
		if got := sameOrigin(c.originHost, c.reqHost); got != c.want {
			t.Errorf("%s：sameOrigin(%q,%q)=%v，want %v", c.name, c.originHost, c.reqHost, got, c.want)
		}
	}
}

// TestCheckOriginLocalAddrMatrix 堵洞矩阵：checkOrigin 的 loopback 互认是**三元比对**
// （Origin ↔ 请求 Host ↔ http.LocalAddrContextKey 的真实监听地址）。
//
// 核心格（旧行为放行、新行为拒）：Origin=[::1]:port 打到本地地址 127.0.0.1:port → 拒
// （堵洞收紧——三件套是三个不同的本机地址，同端口可各自 bind）。
func TestCheckOriginLocalAddrMatrix(t *testing.T) {
	cases := []struct {
		name                string
		origin, host, local string // local=="" 表示上下文里没有本地地址
		want                bool
	}{
		// —— 堵洞核心：不同 loopback 地址不互认 ——
		{"Origin=[::1] 打到本地 127.0.0.1 → 拒（堵洞收紧，旧口径放行）", "http://[::1]:8720", "127.0.0.1:8720", "127.0.0.1:8720", false},
		{"Origin=127.0.0.1 打到本地 [::1] → 拒（堵洞收紧，旧口径放行）", "http://127.0.0.1:8720", "localhost:8720", "[::1]:8720", false},
		// —— 精确匹配 / 隧道拼写兼容 → 放行 ——
		{"Origin=127.0.0.1 打到本地 127.0.0.1 → 放行（精确匹配）", "http://127.0.0.1:8720", "localhost:8720", "127.0.0.1:8720", true},
		{"Origin=localhost 打到本地 127.0.0.1 → 放行（隧道拼写）", "http://localhost:8720", "127.0.0.1:8720", "127.0.0.1:8720", true},
		{"Origin=[::1] 打到本地 [::1] → 放行（精确匹配）", "http://[::1]:8720", "localhost:8720", "[::1]:8720", true},
		{"Origin=localhost 打到本地 [::1] → 放行（隧道拼写）", "http://localhost:8720", "127.0.0.1:8720", "[::1]:8720", true},
		// —— 同源不受影响（不需要本地地址）——
		{"同源（精确）无本地地址也放行", "http://127.0.0.1:8720", "127.0.0.1:8720", "", true},
		{"同源（大小写）", "http://LocalHost:8720", "localhost:8720", "", true},
		// —— 端口 / 跨源 / 127/8 ——
		{"不同端口拒", "http://localhost:9999", "127.0.0.1:8720", "127.0.0.1:8720", false},
		{"evil.com 拒", "http://evil.example.com", "127.0.0.1:8720", "127.0.0.1:8720", false},
		{"127.0.0.2 继续不放行", "http://127.0.0.2:8720", "localhost:8720", "127.0.0.1:8720", false},
		// —— 无 Origin（CLI）放行 ——
		{"无 Origin（CLI）放行", "", "127.0.0.1:8720", "127.0.0.1:8720", true},
		// —— 本地地址拿不到 → 互认不生效（fail-close）——
		{"无本地地址时拼写差异拒（fail-close，堵洞收紧）", "http://localhost:8720", "127.0.0.1:8720", "", false},
		{"无本地地址时 [::1]→127.0.0.1 拒（fail-close）", "http://[::1]:8720", "127.0.0.1:8720", "", false},
	}
	for _, c := range cases {
		err := checkOrigin(newOriginReq(c.origin, c.host, c.local))
		if got := err == nil; got != c.want {
			t.Errorf("%s：checkOrigin(Origin=%q, Host=%q, local=%q) err=%v，want 放行=%v",
				c.name, c.origin, c.host, c.local, err, c.want)
		}
	}
}

// TestCheckOriginHostMustBeLoopbackForMutual 钉死 v4-2-A 复查（p3-1）：loopback 互认
// 要求**请求 Host 的 host 也是规范三件套**——这与"注入真实监听地址"之前的**旧口径**
// 一致（"只收紧不放松"）。
//
// 回归的那一格：Origin=localhost:<port>（规范三件套）+ Host=evil.example.com:<port>
// （**非** loopback）→ 旧口径 **403**；漏掉"Host 也要 loopback"这条的那一轮会
// **放行**（200）——那正是"放松了一格"的症状。
//
// 另有几格钉住**不回归**：Host 是规范三件套时，隧道拼写互认照旧放行。
func TestCheckOriginHostMustBeLoopbackForMutual(t *testing.T) {
	cases := []struct {
		name                string
		origin, host, local string // local=="" 表示上下文里没有本地地址
		want                bool
	}{
		// —— 回归格：请求 Host 不是 loopback → 互认**整体**不生效 ——
		{"Host=evil.example.com → 互认不生效（旧口径 403）",
			"http://localhost:8720", "evil.example.com:8720", "127.0.0.1:8720", false},
		{"Host=evil.example.com + Origin 也非 loopback → 拒",
			"http://evil.example.com:8720", "other.example.com:8720", "127.0.0.1:8720", false},
		{"Host=127.0.0.2（非规范三件套）→ 互认不生效",
			"http://localhost:8720", "127.0.0.2:8720", "127.0.0.1:8720", false},
		{"Host=非 loopback 的 IP → 互认不生效",
			"http://localhost:8720", "192.168.1.10:8720", "127.0.0.1:8720", false},
		// —— 不回归：Host 是规范三件套 → 隧道拼写互认照旧 ——
		{"Host=127.0.0.1 → 放行（精确匹配）",
			"http://127.0.0.1:8720", "127.0.0.1:8720", "127.0.0.1:8720", true},
		{"Host=127.0.0.1 + Origin=localhost → 放行（隧道拼写）",
			"http://localhost:8720", "127.0.0.1:8720", "127.0.0.1:8720", true},
		{"Host=localhost + Origin=127.0.0.1 → 放行（隧道拼写）",
			"http://127.0.0.1:8720", "localhost:8720", "127.0.0.1:8720", true},
		// —— 同源那道判据不受影响（Host 非 loopback 但 Origin 与 Host 同字面量 → 放行）——
		{"同源（Host 与 Origin 同字面量）→ 放行",
			"http://evil.example.com:8720", "evil.example.com:8720", "192.168.1.10:8720", true},
	}
	for _, c := range cases {
		err := checkOrigin(newOriginReq(c.origin, c.host, c.local))
		if got := err == nil; got != c.want {
			t.Errorf("%s：checkOrigin(Origin=%q, Host=%q, local=%q) err=%v，want 放行=%v",
				c.name, c.origin, c.host, c.local, err, c.want)
		}
	}
}

// TestIsLoopbackHost 钉死 loopback 互认的主机判定：**只有规范写法**
// （localhost / 127.0.0.1 / ::1）算；127/8 的其余成员不算（收紧，见 server.go 的说明）。
func TestIsLoopbackHost(t *testing.T) {
	for _, h := range []string{"localhost", "LocalHost", "127.0.0.1", "::1", "[::1]"} {
		if !isLoopbackHost(h) {
			t.Errorf("isLoopbackHost(%q) 应 true", h)
		}
	}
	for _, h := range []string{
		"evil.com", "10.0.0.1", "192.168.1.1", "example.org", "", "::2", "0.0.0.0",
		"127.0.0.2", "127.1.2.3", "127.255.255.254",
	} {
		if isLoopbackHost(h) {
			t.Errorf("isLoopbackHost(%q) 应 false（只认规范写法：localhost/127.0.0.1/::1）", h)
		}
	}
}

// TestCheckOriginLoopbackAndCross 端到端钉 checkOrigin：无 Origin 放行、
// loopback 拼写差异（本地地址在场）放行、真正的跨源（evil.com）仍拒、
// 本地地址拿不到时拼写差异拒（fail-close，堵洞收紧）。
func TestCheckOriginLoopbackAndCross(t *testing.T) {
	// 无 Origin 头（非浏览器客户端）→ 放行，不回归
	noOrigin := httptest.NewRequest("GET", "/ws", nil)
	if err := checkOrigin(noOrigin); err != nil {
		t.Errorf("无 Origin 头应放行：%v", err)
	}
	// Origin=localhost，Host=127.0.0.1，本地地址 127.0.0.1（httptest/ssh 隧道常见割裂）
	// → loopback 互认（隧道拼写兼容）放行
	loopback := newOriginReq("http://localhost:8720", "127.0.0.1:8720", "127.0.0.1:8720")
	if err := checkOrigin(loopback); err != nil {
		t.Errorf("loopback 拼写差异（本地地址在场）应放行：%v", err)
	}
	// 同一请求拿掉本地地址 → 互认不生效（fail-close），拼写差异被拒（堵洞收紧）
	noLocal := newOriginReq("http://localhost:8720", "127.0.0.1:8720", "")
	if err := checkOrigin(noLocal); err == nil {
		t.Error("本地地址拿不到时互认应不生效（fail-close），拼写差异应被拒")
	}
	// 真正的跨源 → 拒
	cross := newOriginReq("http://evil.example.com", "127.0.0.1:8720", "127.0.0.1:8720")
	if err := checkOrigin(cross); err == nil {
		t.Error("evil.example.com 应被拒")
	}
}

// TestLocalAddrAssertionFailureFailClose：上下文里的值不是 net.Addr（断言失败）
// → 互认不生效（fail-close），loopback 拼写差异被拒。
func TestLocalAddrAssertionFailureFailClose(t *testing.T) {
	r := newOriginReq("http://localhost:8720", "127.0.0.1:8720", "")
	r = r.WithContext(context.WithValue(r.Context(), http.LocalAddrContextKey, 42))
	if err := checkOrigin(r); err == nil {
		t.Fatal("本地地址断言失败时互认应不生效（fail-close），拼写差异应被拒")
	}
}
