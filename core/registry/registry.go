package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/policy"
)

// ExtensionKindTool 是工具扩展点的 kind（D17：kind 是开放字符串，这里只固化内置这一个）。
const ExtensionKindTool = "tool"

// 注册表错误（调用方可用 errors.Is 判定）。
var (
	// ErrNotFound：扩展未注册（名字查无此扩展）。
	ErrNotFound = errors.New("registry: 扩展未注册")
	// ErrAlreadyRegistered：同名扩展已注册（name 全局唯一，D17）。
	ErrAlreadyRegistered = errors.New("registry: 扩展已注册")
	// ErrNotAllowed：能力裁决未获准（deny / ask / 无命中）→ 拒绝启用（fail-close，H8）。
	ErrNotAllowed = errors.New("registry: 能力未获准")
	// ErrStale：裁决期间同名扩展被注销并重新注册 → 裁决背书的是另一个实例（P2-5）。
	ErrStale = errors.New("registry: 扩展实例已在能力裁决期间被替换")
)

// State 是扩展在注册表内的生命周期状态（E6）。
type State string

const (
	// StateRegistered 已注册、未启用（初始态）。
	StateRegistered State = "registered"
	// StateEnabled 已启用（能力已获准）。
	StateEnabled State = "enabled"
	// StateDisabled 已停用（可再 Enable）。
	StateDisabled State = "disabled"
)

// Entry 是注册表对外暴露的扩展条目（值语义快照，修改它不影响注册表）。
type Entry struct {
	Meta  contracts.ExtensionMeta
	Impl  any
	State State
	// Tool 是工具扩展的完整描述（D12：跨边界只传 contracts.Tool）。
	// 非工具扩展为 nil。RegisterTool 注册时原样保留（Schema / SideEffect /
	// Approvable / Idempotent / Replay 等），调用方不必再自己存一份。
	Tool *contracts.Tool
}

// Registry 是并发安全的扩展注册表。
type Registry struct {
	mu      sync.RWMutex
	adj     Adjudicator
	names   []string // 注册顺序（List 的顺序来源）
	entries map[string]*record
	seq     uint64 // 注册序号（每次成功注册 +1；用于识别"同名重注册"）
}

// record 是内部存储（Meta 深拷贝，避免调用方改到注册表内部状态）。
type record struct {
	meta  contracts.ExtensionMeta
	impl  any
	tool  *contracts.Tool
	state State
	seq   uint64 // 注册序号（实例身份：同名重注册会换号）
}

// New 构造注册表。
//
// adjudicator 为 nil 时用**空策略引擎兜底**：一切能力都无 allow 规则 → 一律拒绝启用
// （fail-close，H8）。默认实现见 NewPolicyAdjudicator（包一层 policy.Engine，E3）。
//
// nil 语义口径（三处统一，见 core/doc.go 的"nil 语义"一节）：
//   - **兜底**（本处）：adjudicator=nil → 换成空引擎 → 一切能力被拒（fail-close）；
//   - **编程错误**（bus.New）：store=nil → 直接 panic。
//
// 判据：参数缺失后系统仍能安全地做"拒绝"→ 兜底；否则 panic。
func New(adjudicator Adjudicator) *Registry {
	if adjudicator == nil {
		adjudicator = NewPolicyAdjudicator(policy.New())
	}
	return &Registry{adj: adjudicator, entries: make(map[string]*record)}
}

// Adjudicator 返回注册表使用的能力裁决器（装配/测试观测用）。
func (r *Registry) Adjudicator() Adjudicator {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.adj
}

// Register 注册一个扩展。
//
// 重名拒绝（**name 全局唯一**，D17：注册表的键就是 name，不按 kind 分区）；
// 注册后 State=registered（**不在此处裁决能力**——能力裁决发生在 Enable，E3）。
// impl 为 nil 允许（纯元数据扩展，如 panel/endpoint）。
func (r *Registry) Register(meta contracts.ExtensionMeta, impl any) error {
	return r.register(meta, impl, nil)
}

// register 是 Register 的内部实现：tool 非 nil 时把工具描述一并存下（P2-10）。
func (r *Registry) register(meta contracts.ExtensionMeta, impl any, tool *contracts.Tool) error {
	if r == nil {
		return errors.New("registry: 注册表为 nil")
	}
	if strings.TrimSpace(meta.Name) == "" {
		return errors.New("registry: 扩展 meta.Name 不能为空")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = make(map[string]*record)
	}
	if _, dup := r.entries[meta.Name]; dup {
		return fmt.Errorf("%w: 扩展 %q 已注册（重名拒绝，name 全局唯一）", ErrAlreadyRegistered, meta.Name)
	}
	r.seq++
	r.entries[meta.Name] = &record{
		meta:  cloneMeta(meta),
		impl:  impl,
		tool:  cloneTool(tool),
		state: StateRegistered,
		seq:   r.seq,
	}
	r.names = append(r.names, meta.Name)
	return nil
}

// Enable 启用扩展（E3 能力裁决 + fail-close）：
//
//   - 未注册 → ErrNotFound；
//   - meta.Capabilities 逐个交给 Adjudicator 裁决；**任一非 allow（deny/ask/无命中）→ 拒绝启用**，
//     并**先撤销已有的 enabled 状态**（fail-close：能力已不再获准，扩展不得停留在 enabled），
//     再返回 ErrNotAllowed；
//   - 全部获准（或无能力声明）→ State=enabled；
//   - 幂等：已 enabled 时重新裁决（规则可能已变），全部获准则保持 enabled；
//   - 裁决期间同名扩展被注销/重注册 → 拒绝启用（ErrStale），必须重新 Enable。
//
// 裁决在锁外进行（E5：隔离外部代码；避免持锁调用回调导致死锁），
// 裁决期间被 Unregister 的扩展不会被启用。
func (r *Registry) Enable(ctx context.Context, name string) error {
	if r == nil {
		return errors.New("registry: 注册表为 nil")
	}
	rec, seq, ok := r.snapshot(name)
	if !ok {
		return fmt.Errorf("%w: 扩展 %q 未注册", ErrNotFound, name)
	}

	adj := r.Adjudicator()
	for _, cap := range rec.Meta.Capabilities {
		d := adjudicate(adj, ctx, cap)
		if d.Verdict != contracts.EffectAllow {
			// fail-close（P1-2）：已启用的扩展重新裁决被拒 → 先撤销 enabled，
			// 再报错。能力已不获准却仍 enabled = 事实与权限不符。
			r.revokeEnabled(name)
			return fmt.Errorf("%w: 能力 %q 未获准（verdict=%s, rule=%s, reason=%s）→ 拒绝启用扩展 %q（fail-close）",
				ErrNotAllowed, cap, d.Verdict, ruleIDText(d.RuleID), reasonText(d.Reason), name)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	cur, ok := r.entries[name]
	if !ok {
		return fmt.Errorf("%w: 扩展 %q 在能力裁决期间被注销", ErrNotFound, name)
	}
	if cur.seq != seq {
		// P2-5（TOCTOU）：裁决期间同名扩展被注销并重新注册 → 刚才的裁决背书的是
		// **另一个实例**（能力可能完全不同）→ 拒绝启用，要求调用方重新 Enable。
		return fmt.Errorf("%w: 扩展 %q 在能力裁决期间被注销并重新注册（裁决针对的是另一个实例，能力可能已变）→ 拒绝启用，请重新 Enable",
			ErrStale, name)
	}
	cur.state = StateEnabled
	return nil
}

// revokeEnabled 把已启用的扩展置为 disabled（fail-close）。
//
// 只动 enabled 的条目：从未启用的（registered）保持 registered —— 两者都不是
// "已启用"，但 registered 更能表达"还没启用过"。
func (r *Registry) revokeEnabled(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.entries[name]
	if !ok {
		return
	}
	if rec.state == StateEnabled {
		rec.state = StateDisabled
	}
}

// Disable 停用扩展（幂等：任何状态都可 Disable，disabled 再 Disable 是空操作）。
func (r *Registry) Disable(name string) error {
	if r == nil {
		return errors.New("registry: 注册表为 nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.entries[name]
	if !ok {
		return fmt.Errorf("%w: 扩展 %q 未注册", ErrNotFound, name)
	}
	rec.state = StateDisabled
	return nil
}

// Unregister 注销扩展（任何状态都可注销；注销后 name 可被重新注册）。
func (r *Registry) Unregister(name string) error {
	if r == nil {
		return errors.New("registry: 注册表为 nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[name]; !ok {
		return fmt.Errorf("%w: 扩展 %q 未注册", ErrNotFound, name)
	}
	delete(r.entries, name)
	for i, n := range r.names {
		if n == name {
			r.names = append(r.names[:i], r.names[i+1:]...)
			break
		}
	}
	return nil
}

// Get 按名取条目（返回深拷贝快照）。
func (r *Registry) Get(name string) (Entry, bool) {
	if r == nil {
		return Entry{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.entries[name]
	if !ok {
		return Entry{}, false
	}
	return entryOf(rec), true
}

// List 按**注册顺序**返回条目；kind 为空 = 全部。
func (r *Registry) List(kind string) []Entry {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Entry, 0, len(r.names))
	for _, n := range r.names {
		rec, ok := r.entries[n]
		if !ok {
			continue
		}
		if kind != "" && rec.meta.Kind != kind {
			continue
		}
		out = append(out, entryOf(rec))
	}
	return out
}

// SetHealth 回填扩展健康状态（D17：运行时回填字段）。
func (r *Registry) SetHealth(name string, h contracts.ExtensionHealth) error {
	if r == nil {
		return errors.New("registry: 注册表为 nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.entries[name]
	if !ok {
		return fmt.Errorf("%w: 扩展 %q 未注册", ErrNotFound, name)
	}
	hh := h
	rec.meta.Health = &hh
	return nil
}

// snapshot 在锁内取一条记录的深拷贝与注册序号（供锁外使用）。
//
// 注册序号是**实例身份**：同名扩展被注销后重新注册会换号 → Enable 能识别
// "裁决期间换了实例"（P2-5）。
func (r *Registry) snapshot(name string) (Entry, uint64, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.entries[name]
	if !ok {
		return Entry{}, 0, false
	}
	return entryOf(rec), rec.seq, true
}

// entryOf 把内部记录转成对外条目（Meta 与 Tool 已深拷贝）。
func entryOf(rec *record) Entry {
	return Entry{
		Meta:  cloneMeta(rec.meta),
		Impl:  rec.impl,
		State: rec.state,
		Tool:  cloneTool(rec.tool),
	}
}

// adjudicate 调用裁决器并隔离 panic（E5：扩展/裁决方异常不得带崩 core；异常一律按 deny，fail-close）。
func adjudicate(adj Adjudicator, ctx context.Context, capability string) (d contracts.Decision) {
	defer func() {
		if rec := recover(); rec != nil {
			d = contracts.Decision{
				Verdict: contracts.EffectDeny,
				Reason:  strPtr(fmt.Sprintf("fail-close: 能力裁决 panic: %v", rec)),
			}
		}
	}()
	if adj == nil {
		// 没有裁决器 = 无人裁决 = 拒绝（fail-close）。
		return contracts.Decision{Verdict: contracts.EffectDeny, Reason: strPtr("fail-close: 无能力裁决器")}
	}
	return adj.Adjudicate(ctx, capability)
}

// cloneMeta 深拷贝扩展元数据（指针 / 切片 / 原始 JSON 都不与调用方共享，
// 防调用方改到注册表内部状态——Entry 承诺的是值语义快照）。
func cloneMeta(m contracts.ExtensionMeta) contracts.ExtensionMeta {
	out := m
	if m.Description != nil {
		d := *m.Description
		out.Description = &d
	}
	if m.Schema != nil {
		out.Schema = append(json.RawMessage(nil), m.Schema...)
	}
	if m.Capabilities != nil {
		out.Capabilities = append([]string(nil), m.Capabilities...)
	}
	if m.Health != nil {
		h := *m.Health
		if m.Health.Message != nil {
			msg := *m.Health.Message
			h.Message = &msg
		}
		out.Health = &h
	}
	return out
}

// cloneTool 深拷贝工具描述（Schema 是 json.RawMessage，必须复制字节，
// 否则调用方改写原切片会改到注册表内部状态）。
func cloneTool(t *contracts.Tool) *contracts.Tool {
	if t == nil {
		return nil
	}
	out := *t
	if t.Description != nil {
		d := *t.Description
		out.Description = &d
	}
	if t.Schema != nil {
		out.Schema = append(json.RawMessage(nil), t.Schema...)
	}
	if t.RequiredEnv != nil {
		out.RequiredEnv = append([]string(nil), t.RequiredEnv...)
	}
	if t.Returns != nil {
		r := *t.Returns
		if t.Returns.GraphEdges != nil {
			g := *t.Returns.GraphEdges
			r.GraphEdges = &g
		}
		out.Returns = &r
	}
	return &out
}

func ruleIDText(id *string) string {
	if id == nil {
		return "<nil>"
	}
	return *id
}

func reasonText(reason *string) string {
	if reason == nil || *reason == "" {
		return "<nil>"
	}
	return *reason
}

func strPtr(s string) *string { return &s }
