package main

import (
	"io"
	"os"
	"strings"
	"testing"
)

// TestPrintBannerSSHTunnelHint 钉死 v4-2-A 的 banner SSH 端口转发提示：
// 提示行在、端口从真实监听地址正确嵌入、并指路 http://localhost:<port>。
func TestPrintBannerSSHTunnelHint(t *testing.T) {
	// 捕获 stdout（printBanner 走 fmt.Printf → os.Stdout；本测试不 t.Parallel，串行安全）。
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	printBanner("127.0.0.1:8720", bannerOptions{})
	_ = w.Close()
	os.Stdout = old
	out, _ := io.ReadAll(r)
	got := string(out)

	if !strings.Contains(got, "ssh -L 8720:127.0.0.1:8720") {
		t.Errorf("banner 缺 SSH 隧道提示或端口未从监听地址嵌入：\n%s", got)
	}
	if !strings.Contains(got, "http://localhost:8720") {
		t.Errorf("banner SSH 提示缺 localhost 访问指引：\n%s", got)
	}
}
