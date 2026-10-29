package compaction

import (
	"context"
	"sync"
)

// 本文件提供 v2-S1 硬边界（MarkValidationPassed）的**生产驱动接线**（J11/13 §3 挂账，
// v3-S3 清）：让框架确认的"验证通过"信号自动打出阶段硬边界。
//
// # 背景：为什么需要它
//
// 阶段边界 = 验证通过点（critic clean / gate receipt 通过，13 §3）。软边界是 agent 自己
// 调 stage 工具显式标记（J11）；**硬边界**是框架侧确认的验证通过（MarkValidationPassed）。
// v2-S1 只落了 MarkValidationPassed API，**无生产调用方**——装配层拿不到 per-Run 的引擎
// 实例（工厂每次 Run 新造），没法从外部调它。
//
// # 接线形态：引擎自己观测事件流
//
// 引擎已经持有 SeqSource/TextSource 等窄接口，且是 per-Run 的。让它**自己**在装配时查
// "自上一边界以来有没有验证通过事件"（ValidationSignalSource），有就打硬边界——不必让
// 宿主/loop 穿透到 per-Run 引擎实例。loop.Config 因此**零改动**。
//
// # 什么算"验证通过"
//
// 由 ValidationSignalSource 的实现定义（宿主侧）：当前口径 = 事件流里出现 gate.checked
// 且 passed=true（J11 明写"gate receipt 通过"）。将来可扩展 critic clean / review approved
// 等信号，不必改引擎。

// ValidationSignalSource 提供"自某 seq 以来是否有**验证通过**信号"（v2-S1 硬边界接线的
// 生产驱动，J11）。
//
// 实现通常是宿主的 store 读出面：扫 (afterSeq, ∞) 的 durable 事件，找 gate.checked(passed=true)
// 之类。core 不 import runtime/host，只认这个窄接口（测试用 fake）。
type ValidationSignalSource interface {
	// ValidationPassedSince 报告在 seq > afterSeq 的范围里是否有"验证通过"事件。
	//
	// 返回 (found, tip, err)：
	//   - found：是否有验证通过事件；
	//   - tip：扫描到的事件流 seq 上界（引擎据此推进游标，避免同一事件重复触发硬边界）；
	//   - err：查询失败（引擎据此**不**打硬边界——fail-close：不确定就不妄断验证通过）。
	ValidationPassedSince(ctx context.Context, afterSeq int64) (found bool, tip int64, err error)
}

// ValidationCursor 是"验证通过信号"游标的**会话级载体**（跨 Run 续用；v3-S3 修）。
//
// # 为什么需要它（跨 Run 丢信号的根因）
//
// tiered 引擎是 **per-Run** 的（loop.Config.ContextStrategy 每次 Run 造一个 fresh 引擎），
// 引擎内部的游标（lastValidationSeq）因而**随 Run 结束而消失**。下一个 Run 的首次 Assemble
// 会**重新取基线**（"只看本次 Run 开始后的新信号，不反应陈旧的历史验证事件"）→
// **落在两次 Run 之间**的验证信号（如 Run1 结束与 Run2 开始之间到达的
// workflow.milestone passed=true）**永远不可见**：那一段再没人扫过。
//
// 修法：游标**在 session 级持久**——宿主为每个 session 持一个 ValidationCursor，装配引擎
// 时经 Engine.SetValidationCursor 传进去，引擎推进它；下一个 Run 的引擎从同一游标继续扫
// → Run 之间的信号不再丢。
//
// # 语义
//
//   - 游标只**前进**（取最大值）：不回退（回退 = 重扫旧区间 = 重复触发硬边界）；
//   - "未定位过"（Initialized=false）与"游标=0"是两回事：前者表示还没取过基线
//     （引擎首次 Assemble 时按现行口径取基线），后者表示基线就在 0。
//   - 并发安全：多个 Run 共享一个游标时，最多重复触发一次硬边界（幂等方向：宁可多一次
//     边界，不漏信号）。
type ValidationCursor struct {
	mu   sync.Mutex
	seq  int64
	init bool
}

// NewValidationCursor 建一个空游标（还没定位过任何信号）。
func NewValidationCursor() *ValidationCursor { return &ValidationCursor{} }

// Seq 返回当前游标位置（未定位过时为 0）。
func (c *ValidationCursor) Seq() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seq
}

// Initialized 报告游标是否已经**定位过**（false = 从未取过基线）。
func (c *ValidationCursor) Initialized() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.init
}

// Advance 把游标**推进**到 seq（只前进，不后退；seq <= 当前值时是空操作）。
func (c *ValidationCursor) Advance(seq int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.init || seq > c.seq {
		c.seq = seq
		c.init = true
	}
}
