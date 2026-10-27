package host

// 内部测试：loopback 判定（isLoopbackHost）。
//
// 关键口径：**只认 IP 与两个名字特例**，其余非 IP 主机名一律拒绝。
// 字符串前缀匹配（"127."）会把 "127.0.0.1.evil.example" 这类**域名**放行
// （它解析到 127.0.0.1，实测能绑上监听）——v0 的安全模型（loopback + 无凭据）
// 会因此整体失效。

import (
	"net"
	"testing"
)

func TestIsLoopbackHost(t *testing.T) {
	cases := []struct {
		host string
		want bool
		why  string
	}{
		// 接受：IP（127.0.0.0/8 整段 + ::1）。
		{"127.0.0.1", true, "IPv4 loopback"},
		{"127.0.0.5", true, "127/8 整段都是 loopback"},
		{"127.1.2.3", true, "127/8 整段都是 loopback"},
		{"::1", true, "IPv6 loopback"},
		{"[::1]", true, "IPv6 loopback（带方括号）"},
		// 接受：名字特例（大小写不敏感）。
		{"localhost", true, "名字特例"},
		{"LocalHost", true, "名字特例（大小写不敏感）"},
		{"LOCALHOST", true, "名字特例（大小写不敏感）"},
		{"ip6-localhost", true, "名字特例"},
		{"IP6-LOCALHOST", true, "名字特例（大小写不敏感）"},

		// 拒绝：非 loopback 的 IP。
		{"0.0.0.0", false, "全网卡 = 暴露"},
		{"::", false, "IPv6 全网卡 = 暴露"},
		{"[::]", false, "IPv6 全网卡 = 暴露"},
		{"192.168.1.5", false, "内网地址"},
		{"10.0.0.1", false, "内网地址"},
		{"169.254.169.254", false, "链路本地（元数据服务）"},
		{"8.8.8.8", false, "公网地址"},

		// 拒绝：域名（含伪装成 loopback 前缀的）。
		{"example.com", false, "远端主机"},
		{"127.0.0.1.evil.example", false, "域名伪装成 loopback 前缀"},
		{"127.0.0.1.nip.io", false, "域名伪装成 loopback 前缀"},
		{"127.0.0.1.nip.io.", false, "域名伪装成 loopback 前缀（带尾点）"},
		{"localhost.evil.example", false, "域名伪装成名字特例"},
		{"evil.localhost", false, "域名伪装成名字特例"},
		{"", false, "空主机"},
	}
	for _, c := range cases {
		if got := isLoopbackHost(c.host); got != c.want {
			t.Errorf("isLoopbackHost(%q) = %v, want %v（%s）", c.host, got, c.want, c.why)
		}
	}
}

// TestLoopbackLookalikeDomainsResolveToLoopback：说明为什么必须用 IP 判定而不是
// 字符串前缀——这些**域名**真的解析到 127.0.0.1（所以字符串匹配会放行它们）。
func TestLoopbackLookalikeDomainsResolveToLoopback(t *testing.T) {
	for _, host := range []string{"127.0.0.1.nip.io", "127.0.0.1.evil.example"} {
		ips, err := net.LookupIP(host)
		if err != nil {
			t.Skipf("%s 无法解析（无 DNS）：%v", host, err)
		}
		loopback := false
		for _, ip := range ips {
			if ip.IsLoopback() {
				loopback = true
			}
		}
		if !loopback {
			t.Skipf("%s 当前不解析到 loopback（DNS 行为会变）", host)
		}
		// 它解析到 loopback，但**不是** IP 字面量 → 必须拒绝（v0 只监听本机）。
		if isLoopbackHost(host) {
			t.Fatalf("域名 %q 解析到 loopback 也不该被放行（字符串匹配会绕过 v0 安全模型）", host)
		}
	}
}
