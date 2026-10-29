package host

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// 本文件是 v2-S4 沙箱的**宿主装配面**测试（K8 酸测试的宿主侧）。
//
// 核心断言只有一句：Config.Sandbox 从 false 翻到 true，**agent / 工具 / loop 代码
// 一行没动**——变的只是注入进工具执行器的能力实现。这与 runtime/cap/sandbox 的
// TestK8SwapEnvZeroAgentChange 是同一件事的两个层次（那边换 env 对象，这边换装配）。

// scriptProviderThatCallsBash 是一个"只会调一次 bash 工具"的脚本 provider。
//
// 用来在 host 层跑真机回路而不打模型 API（与 cmd/alasol 的 testProviderOverride 同性质）。
type bashScriptedProvider struct {
	calls   int
	command string
}

func (p *bashScriptedProvider) Run(ctx context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	_ = ctx
	_ = req
	p.calls++
	if p.calls == 1 {
		tc := core.ToolCallReq{
			ID:   contracts.ProviderId("toolu_SANDBOX_01"),
			Name: "bash",
			Args: json.RawMessage(`{"command":` + jsonString(p.command) + `}`),
		}
		emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 10, OutputTokens: 5}}, nil
	}
	emit(core.ProviderEvent{Kind: "text.delta", Text: "写完了"})
	emit(core.ProviderEvent{Kind: "done"})
	return core.ModelResult{Text: "写完了", Usage: core.Usage{InputTokens: 20, OutputTokens: 3}}, nil
}

// jsonString 把字符串转成 JSON 字符串字面量（测试辅助，避免引号转义手抖）。
func jsonString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// TestSandboxSwitchIsAssemblyOnly 是 K8 酸测试的宿主侧：
// 同一个 hub、同一个工具集、同一条 prompt，只翻 Config.Sandbox。
func TestSandboxSwitchIsAssemblyOnly(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("沙箱后端只在 Windows 实现（Linux/mac 的 seatbelt/bwrap 是预留挂点，H9）；" +
			"此处跳过不代表不需要——见 runtime/cap/sandbox/backend_other.go")
	}
	work := t.TempDir()
	marker := filepath.Join(work, "marker.txt")
	// 允许根 = work；允许集之外另造一个目录，bash 写它必须被 OS 拒。
	outside := t.TempDir()
	outsideTarget := filepath.Join(outside, "escape.txt")

	run := func(sandbox bool) (insideOK, outsideOK bool) {
		prov := &bashScriptedProvider{
			command: "echo hi > " + marker + " & echo bad > " + outsideTarget,
		}
		h, err := New(Config{
			DBPath:           filepath.Join(t.TempDir(), "sandbox.db"),
			Model:            "scripted",
			Allow:            []string{"bash"},
			Approval:         "always-allow",
			CWD:              work,
			Provider:         prov,
			Sandbox:          sandbox,
			SandboxWriteDirs: []string{work},
		})
		if err != nil {
			t.Fatalf("host.New(sandbox=%v): %v", sandbox, err)
		}
		defer h.Close()
		sid := NewSession()
		if _, err := h.RunPrompt(context.Background(), sid, "写两个文件"); err != nil {
			t.Fatalf("RunPrompt(sandbox=%v): %v", sandbox, err)
		}
		_, e1 := os.Stat(marker)
		_, e2 := os.Stat(outsideTarget)
		return e1 == nil, e2 == nil
	}

	// 不开沙箱（v1 等价）：两个都写成功——证明"没有沙箱时确实没有 OS 强制"
	// （这正是 V8 诚实边界要承认的事实）。
	inA, outA := run(false)
	if !inA {
		t.Error("不开沙箱时允许集内也写不了？装配坏了")
	}
	if !outA {
		t.Error("不开沙箱时居然写不进允许集之外——local 不该有 OS 强制（测试预期错误？）")
	}

	// 开沙箱：允许集内写成功、允许集外被 OS 拒。
	_ = os.Remove(marker)
	_ = os.Remove(outsideTarget)
	inB, outB := run(true)
	if !inB {
		t.Error("开沙箱后允许集内写失败——沙箱把正常工作也挡住了")
	}
	if outB {
		t.Error("开沙箱后允许集之外居然写成功了——V8 没收口")
	}
}

// TestConfigSandboxValidation 钉死宿主配置面的 fail-close。
func TestConfigSandboxValidation(t *testing.T) {
	base := Config{DBPath: "x.db", Model: "m"}
	c := base
	c.Sandbox = true
	c.SandboxMaxActiveProcesses = -1
	if err := c.validate(); err == nil {
		t.Error("负的 SandboxMaxActiveProcesses 应该被拒")
	}
	c = base
	c.Sandbox = true
	c.SandboxWriteDirs = []string{"relative/path"}
	if err := c.validate(); err == nil {
		t.Error("相对路径的允许写根应该被拒")
	}
	c = base
	c.Sandbox = true
	c.SandboxWriteDirs = []string{"  "}
	if err := c.validate(); err == nil {
		t.Error("空白路径的允许写根应该被拒")
	}
	c = base
	c.Sandbox = true
	c.SandboxWriteDirs = []string{filepath.Join(t.TempDir(), "ws")}
	if err := c.validate(); err != nil {
		t.Errorf("合法沙箱配置应该通过：%v", err)
	}
}

// TestSandboxPolicyIncludesCWD 验证允许根缺省含 CWD（否则 bash 连 git status 都跑不了）。
func TestSandboxPolicyIncludesCWD(t *testing.T) {
	cwd := t.TempDir()
	c := Config{DBPath: "x.db", Model: "m", CWD: cwd, Sandbox: true}
	p := c.sandboxPolicy()
	var found bool
	for _, r := range p.AllowedRoots {
		if strings.EqualFold(filepath.Clean(r), filepath.Clean(cwd)) {
			found = true
		}
	}
	if !found {
		t.Errorf("允许根缺省不含 CWD：%v", p.AllowedRoots)
	}
	if !p.UseJobObject || !p.UseRestrictedToken || !p.UseFileACL {
		t.Errorf("宿主装配的策略应该是全开：%+v", p)
	}
	// 严格组 SID 模式要能透传到策略（默认关）。
	if p.StrictGroupSIDs {
		t.Error("默认不该是严格组 SID 模式")
	}
	c.SandboxStrictGroups = true
	if !c.sandboxPolicy().StrictGroupSIDs {
		t.Error("严格组 SID 模式没有透传到策略")
	}
}
