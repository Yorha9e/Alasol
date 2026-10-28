// Package models 实现 I5 的**模型目录**：内置静态快照 + 在线拉取（models.dev 形态 /
// {base}/models）+ 周期刷新。
//
// 定位（I5）：模型名是**端点相关**的（I9：无框架级默认值），发现手段是"在线查端点支持什么"。
// 本目录把两件事合一：
//   - **内置静态快照**：一份编译期内置的常见模型清单（离线也能给个起点，附尽量新的元数据）；
//   - **在线拉取**：经注入的 FetchFunc（典型 = GET {base}/models 拿 id 列表）刷新；
//   - **周期刷新**：Run(ctx, interval) 长驻轮询（hub 用；CLI 一次性 Refresh）。
//
// 合并口径：内置（带元数据）∪ 在线 id（在线有而内置无的，以裸 Model{ID} 补上）。内置元数据
// 是**快照**、可能过时，在线 id 才是端点当下真相——两者并呈，不互相覆盖元数据。
//
// 快照元数据是 best-effort（编译期抄录，可能随各家发版过时）；**权威来源是在线拉取**。
package models

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Model 是目录里的一个模型条目（models.dev 形态的极简子集）。
type Model struct {
	// ID 是端点识别的模型名（传给 provider 的原样字符串；I9：不做框架级规范化）。
	ID string `json:"id"`
	// Name 是人类可读名（快照元数据；在线裸条目可为空）。
	Name string `json:"name,omitempty"`
	// Provider 是厂商命名空间（快照元数据；在线裸条目可为空）。
	Provider string `json:"provider,omitempty"`
	// ContextWindow 是上下文窗口 token 数（0 = 未知；快照 best-effort）。
	ContextWindow int `json:"context_window,omitempty"`
	// Builtin 标记该条目来自内置快照（false = 在线拉取补入）。
	Builtin bool `json:"builtin"`
}

// FetchFunc 在线拉取端点支持的模型 id 列表（典型 = GET {base}/models 的 data[].id）。
// 返回 error 时 Catalog 保留上一次的合并结果（不清空），只记 LastError。
type FetchFunc func(ctx context.Context) ([]string, error)

// Catalog 合并内置快照 + 在线拉取，支持周期刷新（I5）。
//
// 并发：mu 保护 merged / lastRefresh / lastErr；Models() 可被任意 goroutine 读，
// Refresh()/Run() 在单个刷新 goroutine 里调（并发 Refresh 由 mu 串行化关键区，但拉取本身
// 在锁外做以避免持锁阻塞 IO）。
type Catalog struct {
	builtin []Model
	fetch   FetchFunc

	mu          sync.Mutex
	merged      []Model
	lastRefresh time.Time
	lastErr     error
	initialized bool
}

// New 建目录。builtin 为内置快照（nil → 用 BuiltinSnapshot()）；fetch 为在线拉取
// （nil = 纯离线，只呈内置快照，不刷新）。
//
// 建成即把 merged 初始化为内置快照（未刷新也能查）。
func New(builtin []Model, fetch FetchFunc) *Catalog {
	if builtin == nil {
		builtin = BuiltinSnapshot()
	}
	c := &Catalog{builtin: builtin, fetch: fetch}
	c.merged = mergeModels(builtin, nil)
	c.initialized = true
	return c
}

// Refresh 拉一次在线并合并进目录。fetch 为 nil → 保留内置（不视为错误）。
// 在线失败 → 返回 error 但**不清空**已有合并结果（韧性：端点抖动不该让目录变空）。
func (c *Catalog) Refresh(ctx context.Context) error {
	if c.fetch == nil {
		return nil
	}
	ids, err := c.fetch(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastRefresh = time.Now()
	if err != nil {
		c.lastErr = err
		return err
	}
	c.lastErr = nil
	c.merged = mergeModels(c.builtin, ids)
	return nil
}

// Models 返回当前合并目录（内置 ∪ 在线，按 ID 排序，确定性）。
func (c *Catalog) Models() []Model {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Model, len(c.merged))
	copy(out, c.merged)
	return out
}

// LastRefresh 返回最近一次成功发起的刷新时刻（零值 = 从未刷新）。
func (c *Catalog) LastRefresh() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastRefresh
}

// LastError 返回最近一次刷新的错误（nil = 上次成功或从未刷新）。
func (c *Catalog) LastError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastErr
}

// Run 周期刷新循环：立即刷一次，之后每 interval 刷，直到 ctx 取消。
// 刷新失败只记 LastError + 继续（不退出循环）。ctx 取消 → 返回 ctx.Err()。
func (c *Catalog) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Minute
	}
	_ = c.Refresh(ctx) // 立即一次（失败不阻断）
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_ = c.Refresh(ctx)
		}
	}
}

// mergeModels 合并内置（带元数据）与在线 id 列表：内置全收；在线 id 去重后，内置没有的
// 以裸 Model{ID, Builtin:false} 补入。结果按 ID 排序（确定性）。
func mergeModels(builtin []Model, onlineIDs []string) []Model {
	seen := make(map[string]bool, len(builtin)+len(onlineIDs))
	out := make([]Model, 0, len(builtin)+len(onlineIDs))
	for _, m := range builtin {
		if m.ID == "" || seen[m.ID] {
			continue
		}
		m.Builtin = true
		seen[m.ID] = true
		out = append(out, m)
	}
	for _, id := range onlineIDs {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, Model{ID: id, Builtin: false})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
