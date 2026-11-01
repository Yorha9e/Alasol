// Package telemetry 实现 telemetry（L2/D19）的 **Sink 三实现**：noop / file / otlp。
//
// # 定位：独立管道的出站口，不是事实源
//
// 词汇表与写面接口住在 core（core/telemetry.go：KindTel* + Sink + Record）——发方
// （core/loop）、实现方（本包）、装配方（host）三方共认同一组形状。本包只负责
// "把已经脱敏的 record 送到某处"，**不参与任何判定**、**可丢弃**。
//
// # 三实现
//
//   - **noop（默认）**：Emit 直接返回 nil。宿主默认路径**不经过它**——nil Sink 才是
//     "关"（一条 record 都不造、一次 Emit 都不调 = 零行为变化酸测试）；Noop 给需要
//     "非 nil 但什么都不做"的调用方（测试桩、显式占位）用；
//   - **file**：NDJSON 落盘（一行一条 core.Record，追加写 + mutex）——与事件流/WAL
//     同风格，可 tail / 可 grep；
//   - **otlp**：**留位**。Emit 恒返 ErrUnsupported——诚实报"还没实现"，不静默丢弃
//     （静默丢弃 = 使用者以为在送、实际一条都没有）。
//
// # 尽力而为
//
// Emit 的 error 只表示"这条没送出"。调用方（core/loop 的 telEmit）只留一条 Warn、
// **不阻断主流程**——为一条观测中止一次模型调用是把观测的重要性置于事实之上。
package telemetry

import (
	"errors"

	"github.com/Yorha9e/alasol/core"
)

// ErrUnsupported 是留位实现（otlp）统一返回的错误：能力面诚实报未支持，
// 不静默忽略（与 runtime/cap/remote 的预留位同纪律）。
var ErrUnsupported = errors.New("telemetry: 该 sink 为预留位，尚未实现（L2/D19：otlp 留位）")

// 编译期证明：三实现都是 core.Sink（换实现面不变，与 storage.Blob / WalSink 同款纪律）。
var (
	_ core.Sink = (*File)(nil)
	_ core.Sink = (*OTLP)(nil)
	_ core.Sink = noopSink{}
)

// noopSink 是 noop 实现（Emit 恒 nil；不分配、不落盘）。
type noopSink struct{}

// Noop 返回 **noop Sink**（Emit 直接成功、什么也不做）。
//
// 注意与"关"的分工：宿主默认关走的是 **nil Sink**（loop 一条 record 都不造），
// 不是本函数。要"非 nil 占位"时才用它。
func Noop() core.Sink { return noopSink{} }

// Emit implements core.Sink（空操作，恒成功）。
func (noopSink) Emit(core.Record) error { return nil }
