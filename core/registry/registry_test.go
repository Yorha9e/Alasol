package registry

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/policy"
)

// ---- 测试辅助 ----

func sp(s string) *string { return &s }

func spb(b bool) *bool { return &b }

func ip(i int64) *int64 { return &i }

// allowEngine 构造一个放行指定能力、其余一律 fail-close 的策略引擎。
func allowEngine(caps ...string) *policy.Engine {
	rules := make([]contracts.PolicyRule, 0, len(caps))
	for _, c := range caps {
		rules = append(rules, contracts.PolicyRule{
			ID:     sp("allow-" + c),
			Effect: contracts.EffectAllow,
			Tool:   c,
			Source: contracts.RuleSourceManaged,
		})
	}
	return policy.New(rules...)
}

func meta(name, kind string, caps ...string) contracts.ExtensionMeta {
	return contracts.ExtensionMeta{
		Name:         name,
		Kind:         kind,
		Capabilities: caps,
		Priority:     ip(0),
		Reloadable:   false,
	}
}

func mustRegister(t *testing.T, r *Registry, m contracts.ExtensionMeta, impl any) {
	t.Helper()
	if err := r.Register(m, impl); err != nil {
		t.Fatalf("Register(%q) 失败: %v", m.Name, err)
	}
}

func stateOf(t *testing.T, r *Registry, name string) State {
	t.Helper()
	e, ok := r.Get(name)
	if !ok {
		t.Fatalf("Get(%q) 未找到", name)
	}
	return e.State
}

// ---- 1. 注册与重名拒绝 ----

func TestRegisterDuplicateRejected(t *testing.T) {
	r := New(nil)
	mustRegister(t, r, meta("builtin.tools", "tool", "fs:read"), struct{}{})

	err := r.Register(meta("builtin.tools", "tool"), struct{}{})
	if err == nil {
		t.Fatalf("重名注册应报错")
	}
	if !strings.Contains(err.Error(), "已注册") {
		t.Fatalf("错误信息应说明重名: %v", err)
	}
	// 重名不影响原条目。
	if got := stateOf(t, r, "builtin.tools"); got != StateRegistered {
		t.Fatalf("State=%s, want registered", got)
	}

	// 空名字拒绝。
	if err := r.Register(meta("  ", "tool"), nil); err == nil {
		t.Fatalf("空 Name 应报错")
	}
}

// ---- 2. Enable 能力裁决（E3）----

func TestEnableCapabilityAdjudication(t *testing.T) {
	r := New(NewPolicyAdjudicator(allowEngine("fs:read", "fs:write")))
	mustRegister(t, r, meta("builtin.fs", "tool", "fs:read", "fs:write"), nil)

	if err := r.Enable(context.Background(), "builtin.fs"); err != nil {
		t.Fatalf("全部能力获准时应启用成功: %v", err)
	}
	if got := stateOf(t, r, "builtin.fs"); got != StateEnabled {
		t.Fatalf("State=%s, want enabled", got)
	}
	// 幂等：再 Enable 一次仍成功且保持 enabled。
	if err := r.Enable(context.Background(), "builtin.fs"); err != nil {
		t.Fatalf("重复 Enable 应幂等成功: %v", err)
	}
	if got := stateOf(t, r, "builtin.fs"); got != StateEnabled {
		t.Fatalf("State=%s, want enabled", got)
	}
}

func TestEnableDeniedByDenyRule(t *testing.T) {
	// fs:write 被 deny → 即便 fs:read 获准也拒绝启用（任一能力非 allow 即失败，fail-close）。
	e := policy.New(
		contracts.PolicyRule{ID: sp("allow-read"), Effect: contracts.EffectAllow, Tool: "fs:read", Source: contracts.RuleSourceManaged},
		contracts.PolicyRule{ID: sp("deny-write"), Effect: contracts.EffectDeny, Tool: "fs:write", Source: contracts.RuleSourceManaged, Reason: sp("写能力需显式授予")},
	)
	r := New(NewPolicyAdjudicator(e))
	mustRegister(t, r, meta("builtin.fs", "tool", "fs:read", "fs:write"), nil)

	err := r.Enable(context.Background(), "builtin.fs")
	if err == nil {
		t.Fatalf("能力被 deny 时应拒绝启用")
	}
	if !strings.Contains(err.Error(), "fs:write") {
		t.Fatalf("错误信息应指出未获准的能力: %v", err)
	}
	if got := stateOf(t, r, "builtin.fs"); got != StateRegistered {
		t.Fatalf("拒绝启用后 State=%s, want registered（不得进入 enabled）", got)
	}
}

func TestEnableAskIsNotAllow(t *testing.T) {
	// ask = 无人审 = deny（能力裁决路径没有 Approver，H8/D14）。
	e := policy.New(contracts.PolicyRule{
		ID: sp("ask-write"), Effect: contracts.EffectAsk, Tool: "fs:write", Source: contracts.RuleSourceUser,
	})
	r := New(NewPolicyAdjudicator(e))
	mustRegister(t, r, meta("builtin.fs", "tool", "fs:write"), nil)

	if err := r.Enable(context.Background(), "builtin.fs"); err == nil {
		t.Fatalf("ask 不等于 allow，应拒绝启用")
	}
}

func TestEnableFailClosedWhenNoRules(t *testing.T) {
	// 无任何规则 → 无 allow 命中 → deny（与 policy 的 fail-close 一致）。
	r := New(NewPolicyAdjudicator(policy.New()))
	mustRegister(t, r, meta("builtin.fs", "tool", "fs:read"), nil)

	err := r.Enable(context.Background(), "builtin.fs")
	if err == nil {
		t.Fatalf("无规则时应拒绝启用（fail-close）")
	}
	if !strings.Contains(err.Error(), "fail-close") {
		t.Fatalf("错误信息应说明 fail-close: %v", err)
	}
}

func TestEnableNoCapabilitiesAlwaysSucceeds(t *testing.T) {
	// 无能力声明的扩展不触发裁决 → 可直接启用（如 panel/endpoint 类纯元数据扩展）。
	r := New(nil) // nil 裁决器 = 一律 deny，但无能力可裁
	mustRegister(t, r, meta("panel.context", "panel"), nil)
	if err := r.Enable(context.Background(), "panel.context"); err != nil {
		t.Fatalf("无能力声明应直接启用: %v", err)
	}
}

func TestEnableUnknownExtension(t *testing.T) {
	r := New(nil)
	if err := r.Enable(context.Background(), "nope"); err == nil {
		t.Fatalf("启用未注册扩展应报错")
	}
}

func TestEnableWithNilRegistryDefaultAdjudicatorDenies(t *testing.T) {
	// New(nil) 兜底空策略引擎：有能力声明的扩展一律拒。
	r := New(nil)
	mustRegister(t, r, meta("builtin.fs", "tool", "fs:read"), nil)
	if err := r.Enable(context.Background(), "builtin.fs"); err == nil {
		t.Fatalf("默认裁决器应拒绝（fail-close）")
	}
}

// ---- 3. Disable / Unregister ----

func TestDisableAndReEnable(t *testing.T) {
	r := New(NewPolicyAdjudicator(allowEngine("fs:read")))
	mustRegister(t, r, meta("builtin.fs", "tool", "fs:read"), nil)

	if err := r.Enable(context.Background(), "builtin.fs"); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if err := r.Disable("builtin.fs"); err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if got := stateOf(t, r, "builtin.fs"); got != StateDisabled {
		t.Fatalf("State=%s, want disabled", got)
	}
	// 停用后可再启用（重新裁决）。
	if err := r.Enable(context.Background(), "builtin.fs"); err != nil {
		t.Fatalf("再 Enable: %v", err)
	}
	if got := stateOf(t, r, "builtin.fs"); got != StateEnabled {
		t.Fatalf("State=%s, want enabled", got)
	}
	// Disable 幂等。
	if err := r.Disable("builtin.fs"); err != nil {
		t.Fatalf("重复 Disable: %v", err)
	}
	if err := r.Disable("builtin.fs"); err != nil {
		t.Fatalf("重复 Disable 应幂等: %v", err)
	}
}

func TestUnregister(t *testing.T) {
	r := New(nil)
	mustRegister(t, r, meta("a", "tool"), nil)
	mustRegister(t, r, meta("b", "tool"), nil)

	if err := r.Unregister("a"); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	if _, ok := r.Get("a"); ok {
		t.Fatalf("注销后仍能 Get 到")
	}
	if err := r.Unregister("a"); err == nil {
		t.Fatalf("重复注销应报错")
	}
	if err := r.Unregister("ghost"); err == nil {
		t.Fatalf("注销不存在项应报错")
	}
	// 注销后名字可重新注册（热插拔，E6）。
	mustRegister(t, r, meta("a", "tool"), nil)
	if got := stateOf(t, r, "a"); got != StateRegistered {
		t.Fatalf("重新注册后 State=%s, want registered", got)
	}
}

func TestDisableUnregisterUnknown(t *testing.T) {
	r := New(nil)
	if err := r.Disable("ghost"); err == nil {
		t.Fatalf("Disable 不存在项应报错")
	}
	if err := r.SetHealth("ghost", contracts.ExtensionHealth{Status: contracts.HealthStatusUp}); err == nil {
		t.Fatalf("SetHealth 不存在项应报错")
	}
}

// ---- 4. List 过滤与顺序 ----

func TestListFilterAndOrder(t *testing.T) {
	r := New(nil)
	mustRegister(t, r, meta("z-tool", ExtensionKindTool), 1)
	mustRegister(t, r, meta("a-panel", "panel"), 2)
	mustRegister(t, r, meta("m-provider", "provider"), 3)
	mustRegister(t, r, meta("b-tool", ExtensionKindTool), 4)

	all := r.List("")
	if len(all) != 4 {
		t.Fatalf("List(\"\")=%d, want 4", len(all))
	}
	wantOrder := []string{"z-tool", "a-panel", "m-provider", "b-tool"}
	for i, e := range all {
		if e.Meta.Name != wantOrder[i] {
			t.Fatalf("List 顺序错: 第 %d 个是 %q, want %q（应按注册顺序）", i, e.Meta.Name, wantOrder[i])
		}
	}

	tools := r.List(ExtensionKindTool)
	if len(tools) != 2 || tools[0].Meta.Name != "z-tool" || tools[1].Meta.Name != "b-tool" {
		t.Fatalf("List(tool)=%+v", tools)
	}
	if got := r.List("telemetry"); len(got) != 0 {
		t.Fatalf("List(telemetry)=%+v, want 空", got)
	}
	// Impl 原样带回。
	if all[0].Impl != 1 {
		t.Fatalf("Impl=%v, want 1", all[0].Impl)
	}
}

// ---- 5. SetHealth ----

func TestSetHealth(t *testing.T) {
	r := New(nil)
	mustRegister(t, r, meta("builtin.fs", "tool", "fs:read"), nil)

	if err := r.SetHealth("builtin.fs", contracts.ExtensionHealth{
		Status:  contracts.HealthStatusDegraded,
		Message: sp("索引落后"),
	}); err != nil {
		t.Fatalf("SetHealth: %v", err)
	}
	e, ok := r.Get("builtin.fs")
	if !ok {
		t.Fatalf("Get 失败")
	}
	if e.Meta.Health == nil || e.Meta.Health.Status != contracts.HealthStatusDegraded {
		t.Fatalf("Health=%+v, want degraded", e.Meta.Health)
	}
	if e.Meta.Health.Message == nil || *e.Meta.Health.Message != "索引落后" {
		t.Fatalf("Health.Message=%v", e.Meta.Health.Message)
	}
	// 覆盖更新。
	if err := r.SetHealth("builtin.fs", contracts.ExtensionHealth{Status: contracts.HealthStatusUp}); err != nil {
		t.Fatalf("SetHealth(up): %v", err)
	}
	e, _ = r.Get("builtin.fs")
	if e.Meta.Health.Status != contracts.HealthStatusUp || e.Meta.Health.Message != nil {
		t.Fatalf("Health=%+v, want up/无消息", e.Meta.Health)
	}
}

// ---- 6. Meta 深拷贝（调用方改不到内部状态）----

func TestEntryIsSnapshot(t *testing.T) {
	r := New(nil)
	caps := []string{"fs:read"}
	mustRegister(t, r, contracts.ExtensionMeta{Name: "x", Kind: "tool", Capabilities: caps}, nil)

	e, _ := r.Get("x")
	e.Meta.Capabilities[0] = "HACKED"
	e.State = StateEnabled

	again, _ := r.Get("x")
	if again.Meta.Capabilities[0] != "fs:read" {
		t.Fatalf("Entry 不是快照，内部状态被改坏: %v", again.Meta.Capabilities)
	}
	if again.State != StateRegistered {
		t.Fatalf("State=%s, want registered", again.State)
	}
}

// ---- 7. RegisterTool：换实现零改动（酸测试）----

type fakeExec struct{ tag string }

func (f fakeExec) Execute(context.Context, contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	return contracts.ToolResult{}, nil
}

func TestRegisterToolMetaDerivation(t *testing.T) {
	r := New(nil)
	tool := contracts.Tool{
		Name:        "read",
		Description: sp("读文件"),
		Schema:      []byte(`{"type":"object"}`),
		SideEffect:  contracts.SideEffectRead,
	}
	if err := r.RegisterTool(tool, fakeExec{tag: "v1"}); err != nil {
		t.Fatalf("RegisterTool: %v", err)
	}
	e, ok := r.Get("read")
	if !ok {
		t.Fatalf("Get(read) 未找到")
	}
	if e.Meta.Name != "read" || e.Meta.Kind != ExtensionKindTool {
		t.Fatalf("meta=%+v, want Name=read Kind=tool", e.Meta)
	}
	if e.Meta.Reloadable {
		t.Fatalf("工具应标记为不可热重载（E6）")
	}
	if len(e.Meta.Capabilities) != 0 {
		t.Fatalf("Capabilities=%v, want 空", e.Meta.Capabilities)
	}
	if e.State != StateRegistered {
		t.Fatalf("State=%s, want registered", e.State)
	}
	exec, ok := e.Impl.(core.ToolExecutor)
	if !ok {
		t.Fatalf("Impl 不是 core.ToolExecutor: %T", e.Impl)
	}
	if _, err := exec.Execute(context.Background(), contracts.ToolCallIntentPayload{}); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// 空工具名 / 空执行器拒绝。
	if err := r.RegisterTool(contracts.Tool{Name: " "}, fakeExec{}); err == nil {
		t.Fatalf("空 tool.Name 应报错")
	}
	if err := r.RegisterTool(contracts.Tool{Name: "bash"}, nil); err == nil {
		t.Fatalf("nil 执行器应报错")
	}
}

// 换 ToolExecutor 实现：只新增一个 _test.go 文件，不改任何已有文件（v0 酸测试）。
func TestRegisterToolSwapExecutorImplementation(t *testing.T) {
	r := New(NewPolicyAdjudicator(allowEngine("fs:read")))
	if err := r.RegisterTool(contracts.Tool{Name: "read", SideEffect: contracts.SideEffectRead}, fakeExec{tag: "v1"}); err != nil {
		t.Fatalf("RegisterTool(v1): %v", err)
	}
	if err := r.Enable(context.Background(), "read"); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if got := stateOf(t, r, "read"); got != StateEnabled {
		t.Fatalf("State=%s", got)
	}

	// 卸载旧实现、装新实现（不同类型）——注册表/策略/core 零改动。
	if err := r.Unregister("read"); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	if err := r.RegisterTool(contracts.Tool{Name: "read", SideEffect: contracts.SideEffectRead}, fakeExec{tag: "v2"}); err != nil {
		t.Fatalf("RegisterTool(v2): %v", err)
	}
	e, _ := r.Get("read")
	if got, _ := e.Impl.(fakeExec); got.tag != "v2" {
		t.Fatalf("换实现后 Impl=%+v, want v2", e.Impl)
	}
	// 新实现重新走能力裁决。
	if err := r.Enable(context.Background(), "read"); err != nil {
		t.Fatalf("新实现 Enable: %v", err)
	}
}

// ---- 8. 并发安全（-race）----

func TestConcurrentRegisterAndList(t *testing.T) {
	r := New(NewPolicyAdjudicator(allowEngine("fs:read")))
	const n = 40

	var wg sync.WaitGroup
	// 并发注册
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mustRegister(t, r, meta("ext-"+strings.Repeat("x", i%5+1)+"-"+itoa(i), ExtensionKindTool, "fs:read"), i)
		}(i)
	}
	// 并发 List / Get / Enable
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = r.List("")
			_ = r.List(ExtensionKindTool)
			for j := 0; j < n; j++ {
				_, _ = r.Get("ext-" + itoa(j))
				_ = r.Enable(context.Background(), "ext-"+itoa(j)) // 不存在也只会返回 error
			}
			_ = r.SetHealth("ext-"+itoa(i), contracts.ExtensionHealth{Status: contracts.HealthStatusUp})
		}(i)
	}
	wg.Wait()

	if got := len(r.List("")); got != n {
		t.Fatalf("并发注册后 List=%d, want %d", got, n)
	}
}

func TestConcurrentEnableSameExtension(t *testing.T) {
	r := New(NewPolicyAdjudicator(allowEngine("fs:read")))
	mustRegister(t, r, meta("builtin.fs", "tool", "fs:read"), nil)

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- r.Enable(context.Background(), "builtin.fs")
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("并发 Enable 失败: %v", err)
		}
	}
	if got := stateOf(t, r, "builtin.fs"); got != StateEnabled {
		t.Fatalf("State=%s, want enabled", got)
	}
}

func TestConcurrentRegisterSameNameOnlyOneWins(t *testing.T) {
	r := New(nil)
	const n = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	oks := 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Register(meta("dup", "tool"), nil); err == nil {
				mu.Lock()
				oks++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if oks != 1 {
		t.Fatalf("重名并发注册成功 %d 次, want 1", oks)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf []byte
	for i > 0 {
		buf = append([]byte{byte('0' + i%10)}, buf...)
		i /= 10
	}
	return string(buf)
}

// ---- 9. 裁决方 panic 被隔离（E5：不带崩 core）----

type panicAdjudicator struct{}

func (panicAdjudicator) Adjudicate(context.Context, string) contracts.Decision {
	panic("裁决器炸了")
}

func TestAdjudicatorPanicIsContained(t *testing.T) {
	r := New(panicAdjudicator{})
	mustRegister(t, r, meta("builtin.fs", "tool", "fs:read"), nil)

	err := r.Enable(context.Background(), "builtin.fs")
	if err == nil {
		t.Fatalf("裁决 panic 应按 deny 拒绝启用（fail-close）")
	}
	if !strings.Contains(err.Error(), "fail-close") {
		t.Fatalf("错误信息应说明 fail-close: %v", err)
	}
	if got := stateOf(t, r, "builtin.fs"); got != StateRegistered {
		t.Fatalf("State=%s, want registered", got)
	}
	// 注册表本身仍然可用（未被 panic 带崩）。
	mustRegister(t, r, meta("other", "panel"), nil)
	if len(r.List("")) != 2 {
		t.Fatalf("List=%d, want 2", len(r.List("")))
	}
}

// ---- 10. 零值 Registry 不 panic（fail-close）----

func TestZeroValueRegistryFailsClosed(t *testing.T) {
	var r Registry
	if err := r.Register(meta("a", "tool", "fs:read"), nil); err != nil {
		t.Fatalf("零值 Register: %v", err)
	}
	// 零值注册表没有裁决器 → 有能力声明的扩展一律拒绝启用（fail-close）。
	if err := r.Enable(context.Background(), "a"); err == nil {
		t.Fatalf("零值注册表无裁决器应拒绝启用")
	}
	// 无能力声明的扩展仍可启用（无能力可裁）。
	if err := r.Register(meta("b", "panel"), nil); err != nil {
		t.Fatalf("零值 Register(b): %v", err)
	}
	if err := r.Enable(context.Background(), "b"); err != nil {
		t.Fatalf("无能力声明的扩展应可启用: %v", err)
	}
	if len(r.List("")) != 2 {
		t.Fatalf("List=%d, want 2", len(r.List("")))
	}
}

// ---- 11. P1-2：Enable 撤销必须 fail-close ----

// switchableAdjudicator 的裁决结果可切换（模拟"规则收紧"）。
type switchableAdjudicator struct {
	allow atomic.Bool
}

func (s *switchableAdjudicator) Adjudicate(_ context.Context, _ string) contracts.Decision {
	if s.allow.Load() {
		return contracts.Decision{Verdict: contracts.EffectAllow, Reason: sp("allow")}
	}
	return contracts.Decision{Verdict: contracts.EffectDeny, Reason: sp("deny")}
}

// P1-2：已启用的扩展重新裁决被拒 → 先撤销 enabled（fail-close），再返回错误。
// 能力已不获准却仍停留在 enabled = 事实与权限不符。
func TestEnableRevokesEnabledWhenAdjudicationTurnsDeny(t *testing.T) {
	adj := &switchableAdjudicator{}
	adj.allow.Store(true)
	r := New(adj)
	mustRegister(t, r, meta("ext", "tool", "fs:read"), nil)

	if err := r.Enable(context.Background(), "ext"); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if got := stateOf(t, r, "ext"); got != StateEnabled {
		t.Fatalf("State=%s, want enabled", got)
	}

	// 规则收紧：能力不再获准。
	adj.allow.Store(false)
	err := r.Enable(context.Background(), "ext")
	if err == nil {
		t.Fatalf("能力不再获准时必须拒绝启用")
	}
	if !errors.Is(err, ErrNotAllowed) {
		t.Errorf("error %v, 需是 ErrNotAllowed", err)
	}
	if got := stateOf(t, r, "ext"); got != StateDisabled {
		t.Fatalf("State=%s, want disabled（fail-close：撤销必须真的撤销）", got)
	}
}

// 从未启用的扩展（registered）失败时保持 registered（同样不是 enabled）。
func TestEnableFailureKeepsRegisteredState(t *testing.T) {
	adj := &switchableAdjudicator{} // 默认 deny
	r := New(adj)
	mustRegister(t, r, meta("ext", "tool", "fs:read"), nil)

	if err := r.Enable(context.Background(), "ext"); err == nil {
		t.Fatalf("应拒绝启用")
	}
	if got := stateOf(t, r, "ext"); got != StateRegistered {
		t.Fatalf("State=%s, want registered", got)
	}
}

// ---- 12. P2-5：同名重注册 TOCTOU ----

// blockingAdjudicator 在裁决期间阻塞（用来制造"锁外裁决"的窗口）。
type blockingAdjudicator struct {
	entered chan string
	release chan struct{}
}

func (b *blockingAdjudicator) Adjudicate(_ context.Context, capability string) contracts.Decision {
	b.entered <- capability
	<-b.release
	return contracts.Decision{Verdict: contracts.EffectAllow, Reason: sp("allow")}
}

// P2-5：裁决期间同名扩展被注销并重注册（能力不同）→ 旧裁决不得背书新扩展。
func TestEnableRejectsInstanceReplacedDuringAdjudication(t *testing.T) {
	adj := &blockingAdjudicator{entered: make(chan string, 4), release: make(chan struct{})}
	r := New(adj)
	mustRegister(t, r, meta("ext", "tool", "fs:read"), nil)

	done := make(chan error, 1)
	go func() { done <- r.Enable(context.Background(), "ext") }()

	<-adj.entered // 裁决已开始（在锁外）

	// 裁决期间：注销 + 同名重注册（能力不同）。
	if err := r.Unregister("ext"); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	mustRegister(t, r, meta("ext", "tool", "fs:write"), nil)

	close(adj.release) // 放行裁决
	err := <-done
	if err == nil {
		t.Fatalf("Enable 必须拒绝（裁决背书的是另一个实例）")
	}
	if !errors.Is(err, ErrStale) {
		t.Errorf("error %v, 需是 ErrStale", err)
	}
	if got := stateOf(t, r, "ext"); got != StateRegistered {
		t.Fatalf("State=%s, want registered（新实例不得被旧裁决启用）", got)
	}

	// 重新 Enable（新裁决，针对新实例）才可启用。
	if err := r.Enable(context.Background(), "ext"); err != nil {
		t.Fatalf("重新 Enable: %v", err)
	}
	if got := stateOf(t, r, "ext"); got != StateEnabled {
		t.Fatalf("State=%s, want enabled", got)
	}
}

// ---- 13. P2-6①：sentinel 错误（errors.Is 可判定）----

func TestSentinelErrors(t *testing.T) {
	r := New(nil)
	mustRegister(t, r, meta("ext", "tool"), nil)

	// 重名注册 → ErrAlreadyRegistered。
	if err := r.Register(meta("ext", "tool"), nil); !errors.Is(err, ErrAlreadyRegistered) {
		t.Errorf("重名注册 error %v, 需是 ErrAlreadyRegistered", err)
	}
	// 名字查无此扩展 → ErrNotFound。
	cases := map[string]error{
		"Enable":     r.Enable(context.Background(), "ghost"),
		"Disable":    r.Disable("ghost"),
		"Unregister": r.Unregister("ghost"),
		"SetHealth":  r.SetHealth("ghost", contracts.ExtensionHealth{Status: contracts.HealthStatusUp}),
	}
	for name, err := range cases {
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("%s: error %v, 需是 ErrNotFound", name, err)
		}
	}
	// 能力未获准 → ErrNotAllowed。
	r2 := New(NewPolicyAdjudicator(allowEngine("fs:read")))
	mustRegister(t, r2, meta("ext2", "tool", "fs:write"), nil)
	if err := r2.Enable(context.Background(), "ext2"); !errors.Is(err, ErrNotAllowed) {
		t.Errorf("能力未获准 error %v, 需是 ErrNotAllowed", err)
	}
}

// ---- 14. P2-10：完整工具描述随注册保留 ----

func TestRegisterToolKeepsFullDescription(t *testing.T) {
	r := New(nil)
	tool := contracts.Tool{
		Name:        "read",
		Description: sp("读文件"),
		Schema:      []byte(`{"type":"object"}`),
		SideEffect:  contracts.SideEffectRead,
		Cancelable:  true,
		Approvable:  true,
		Idempotent:  true,
		Replay:      contracts.ReplayKindSafe,
	}
	if err := r.RegisterTool(tool, fakeExec{tag: "v1"}); err != nil {
		t.Fatalf("RegisterTool: %v", err)
	}
	e, ok := r.Get("read")
	if !ok {
		t.Fatalf("Get(read) 未找到")
	}
	if e.Tool == nil {
		t.Fatalf("Entry.Tool 必须带回完整工具描述")
	}
	got := *e.Tool
	if got.Name != "read" {
		t.Errorf("Tool.Name=%q, want read", got.Name)
	}
	if got.Description == nil || *got.Description != "读文件" {
		t.Errorf("Tool.Description=%v, want 读文件", got.Description)
	}
	if string(got.Schema) != `{"type":"object"}` {
		t.Errorf("Tool.Schema=%s, want 原样保留", got.Schema)
	}
	if got.SideEffect != contracts.SideEffectRead {
		t.Errorf("Tool.SideEffect=%s, want read", got.SideEffect)
	}
	if !got.Cancelable || !got.Approvable || !got.Idempotent {
		t.Errorf("Tool 的布尔属性丢失: %+v", got)
	}
	if got.Replay != contracts.ReplayKindSafe {
		t.Errorf("Tool.Replay=%s, want safe", got.Replay)
	}

	// 深拷贝：调用方改写原字节不得影响注册表内部状态。
	tool.Schema[0] = 'X'
	again, _ := r.Get("read")
	if again.Tool == nil || string(again.Tool.Schema) != `{"type":"object"}` {
		t.Fatalf("Tool 不是快照：%v", again.Tool)
	}
	// 切片/嵌套指针同样不被共享。
	tool.RequiredEnv = []string{"API_KEY"}
	tool.Returns = &contracts.ToolReturns{GraphEdges: spb(true)}
	r2 := New(nil)
	if err := r2.RegisterTool(tool, fakeExec{tag: "v1"}); err != nil {
		t.Fatalf("RegisterTool: %v", err)
	}
	tool.RequiredEnv[0] = "HACKED"
	*tool.Returns.GraphEdges = false
	e2, _ := r2.Get("read")
	if e2.Tool == nil || e2.Tool.RequiredEnv[0] != "API_KEY" {
		t.Fatalf("RequiredEnv 不是快照：%v", e2.Tool)
	}
	if e2.Tool.Returns == nil || e2.Tool.Returns.GraphEdges == nil || !*e2.Tool.Returns.GraphEdges {
		t.Fatalf("Returns 不是快照：%v", e2.Tool)
	}

	// 非工具扩展没有 Tool 描述。
	mustRegister(t, r, meta("panel", "panel"), nil)
	p, _ := r.Get("panel")
	if p.Tool != nil {
		t.Fatalf("非工具扩展的 Tool 应为 nil, got %+v", p.Tool)
	}
}
