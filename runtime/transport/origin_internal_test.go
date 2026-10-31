package transport

import (
	"net/http/httptest"
	"testing"
)

// TestSameOriginLoopbackMutualRecognition 钉死 v4-2-A 的 loopback 互认判据：
// localhost ↔ 127.0.0.1 ↔ [::1]（同端口）互认；evil.com 仍拒；loopback 不同端口不放行；
// **127/8 的其余成员（127.0.0.2 等）不放行**——它们是**不同的本机地址**，
// 同端口可以各自 bind，放行等于把"同机不同地址"这条边界捅开。
func TestSameOriginLoopbackMutualRecognition(t *testing.T) {
	cases := []struct {
		name                string
		originHost, reqHost string
		want                bool
	}{
		{"完全相同", "localhost:8720", "localhost:8720", true},
		{"localhost→127.0.0.1 同端口", "localhost:8720", "127.0.0.1:8720", true},
		{"127.0.0.1→localhost 同端口", "127.0.0.1:8720", "localhost:8720", true},
		{"localhost→[::1] 同端口", "localhost:8720", "[::1]:8720", true},
		{"127.0.0.1→[::1] 同端口", "127.0.0.1:8720", "[::1]:8720", true},
		{"127.0.0.2 也属 127/8", "127.0.0.2:8720", "127.0.0.1:8720", false},
		{"127.0.0.2→localhost 同端口", "127.0.0.2:8720", "localhost:8720", false},
		{"evil.com 跨源", "evil.com", "localhost:8720", false},
		{"evil.com 带同端口仍拒", "evil.com:8720", "localhost:8720", false},
		{"loopback 不同端口不放行", "localhost:9999", "localhost:8720", false},
		{"无端口两端 localhost", "localhost", "localhost", true},
	}
	for _, c := range cases {
		if got := sameOrigin(c.originHost, c.reqHost); got != c.want {
			t.Errorf("%s：sameOrigin(%q,%q)=%v，want %v", c.name, c.originHost, c.reqHost, got, c.want)
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
// loopback 拼写差异放行、真正的跨源（evil.com）仍拒。
func TestCheckOriginLoopbackAndCross(t *testing.T) {
	// 无 Origin 头（非浏览器客户端）→ 放行，不回归
	noOrigin := httptest.NewRequest("GET", "/ws", nil)
	if err := checkOrigin(noOrigin); err != nil {
		t.Errorf("无 Origin 头应放行：%v", err)
	}
	// Origin=localhost，Host=127.0.0.1（httptest/ssh 隧道常见割裂）→ loopback 互认放行
	loopback := httptest.NewRequest("GET", "/ws", nil)
	loopback.Header.Set("Origin", "http://localhost:8720")
	loopback.Host = "127.0.0.1:8720"
	if err := checkOrigin(loopback); err != nil {
		t.Errorf("loopback 拼写差异应放行：%v", err)
	}
	// 真正的跨源 → 拒
	cross := httptest.NewRequest("GET", "/ws", nil)
	cross.Header.Set("Origin", "http://evil.example.com")
	cross.Host = "127.0.0.1:8720"
	if err := checkOrigin(cross); err == nil {
		t.Error("evil.example.com 应被拒")
	}
}
