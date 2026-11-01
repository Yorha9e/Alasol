package host_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
)

// sysToolCaptureProvider 记录每轮请求的 system prompt 与模型可见工具名。
type sysToolCaptureProvider struct {
	mu      sync.Mutex
	systems []string
	tools   [][]string
}

func (p *sysToolCaptureProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	names := make([]string, 0, len(req.Tools))
	for _, tl := range req.Tools {
		names = append(names, tl.Name)
	}
	p.mu.Lock()
	p.systems = append(p.systems, req.System)
	p.tools = append(p.tools, names)
	p.mu.Unlock()
	return core.ModelResult{Text: "查完了"}, nil
}

func (p *sysToolCaptureProvider) sawTool(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, names := range p.tools {
		for _, n := range names {
			if n == name {
				return true
			}
		}
	}
	return false
}

// J3 验收：git_status **注册进工具面**（模型可见集里有它 = 经 registry 暴露），但
// **不进 system prompt**（零自动注入——模型按需经 tools 数组调用，而不是被塞进提示词）。
//
// 两个断言缺一不可：只看"system 没有它"会把"压根没注册"也误判成通过；只看"工具有它"
// 又测不出"是否偷偷注入了 prompt"。合起来才钉住"注册了但不进 prompt"。
func TestGitStatusRegisteredButNotInjectedIntoSystem(t *testing.T) {
	rec := &sysToolCaptureProvider{}
	h := newTestHub(t, func(c *host.Config) {
		// 放行 git_status 让它进模型可见集（默认 deny-all 会把它移出可见集，
		// 见 loop.visibleTools 的 toolVisible 过滤）。
		c.Allow = []string{"git_status"}
		c.Provider = rec
	})
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "查一下 git 状态"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	// ① 注册进工具面：模型可见集里确实有 git_status。
	if !rec.sawTool("git_status") {
		t.Error("git_status 应注册进工具面（模型可见集里应有它）")
	}
	// ② 不进 prompt：任何一轮的 system prompt 都不含 git_status（零自动注入）。
	rec.mu.Lock()
	systems := append([]string(nil), rec.systems...)
	rec.mu.Unlock()
	if len(systems) == 0 {
		t.Fatal("provider 未被调用（测不到 system prompt）")
	}
	for _, sys := range systems {
		if strings.Contains(sys, "git_status") {
			t.Errorf("system prompt 不应含 git_status（不进 prompt，零自动注入）：%q", sys)
		}
	}
}
