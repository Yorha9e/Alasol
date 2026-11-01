package host

import (
	"fmt"
	"strings"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/telemetry"
)

// 本文件是 telemetry（L2/D19）的宿主装配面：Config.Telemetry / TelemetryDir →
// runtime/telemetry 的 Sink → loop.Config.TelemetrySink，以及 Close 收口。
//
// # 定位：独立管道，默认关、脱敏、单向（L2 拍板）
//
// telemetry 与事件流（唯一事实源，F5）、WAL 都是**分开的面**：它把"已经发生的事"
// 复制一份送往观测后端，**不参与任何判定**、**不是事实源**（可丢弃）。默认关
// （mode 空/none）= nil Sink = **零行为变化**——一条 record 都不造、一次 Emit 都不调
// （酸测试）。这是默认路径。
//
// # 与 L1 trace 的关系
//
// **事件流即 trace**（L1）：不另建 trace 系统——调用链就是 core.Store 那条全局 seq
// 有序流。telemetry 只是出站口，不重叠。

// telemetryModeOf 归一化 telemetry 模式（trim；空 = 关）。
func telemetryModeOf(mode string) string {
	m := strings.TrimSpace(mode)
	if m == "" {
		return ""
	}
	return m
}

// telemetryDirOf 归一化 telemetry 落盘目录（trim；空 = 未配）。
func telemetryDirOf(dir string) string { return strings.TrimSpace(dir) }

// newTelemetrySink 按模式建 telemetry Sink。**关 → (nil, nil)**（调用方把 nil 交给
// loop.Config.TelemetrySink = 零行为变化）。
//
// mode=file：目录建不出来 / 打不开 → 拒绝启动（"显式要的能力没起来"，与 blobfs/WAL
// 同口径——不静默降级成关，否则使用者以为在送观测）。
//
// mode=otlp：留位，装配期拒绝（返回 ErrUnsupported 包装——诚实报未实现，不挂一个
// 必然失败的 sink 刷 Warn）。
//
// 返回的 *telemetry.File 由 Hub 持有，Close 时收口（nil = 未启用）。
func newTelemetrySink(mode, dir string) (*telemetry.File, error) {
	switch telemetryModeOf(mode) {
	case "", "none":
		return nil, nil // 关：零行为变化（酸测试）
	case "file":
		d := telemetryDirOf(dir)
		if d == "" {
			return nil, fmt.Errorf("host: telemetry mode=file 需要非空 dir")
		}
		f, err := telemetry.NewFile(d)
		if err != nil {
			return nil, fmt.Errorf("host: 建 telemetry file sink %q: %w", d, err)
		}
		return f, nil
	case "otlp":
		return nil, fmt.Errorf("host: telemetry mode=otlp 为留位（尚未实现）: %w", telemetry.ErrUnsupported)
	default:
		// validate 已拒未知模式；这里是纵深防御（直调本函数的测试路径）。
		return nil, fmt.Errorf("host: telemetry 模式 %q 不支持（none | file | otlp 留位）", mode)
	}
}

// telemetrySink 取 hub 级 telemetry 写面。**未启用 → nil**（关 = 零行为变化）。
//
// **nil 接口语义**：不能把 (*telemetry.File)(nil) 塞进 core.Sink——那是非 nil 接口
// 值（Emit 会走到方法上）。故这里显式返回 nil 接口。
func (h *Hub) telemetrySink() core.Sink {
	if h == nil || h.tel == nil {
		return nil
	}
	return h.tel
}

// TelemetryDir 返回 telemetry 落盘目录（空 = 未启用；诊断/测试用）。
func (h *Hub) TelemetryDir() string {
	if h == nil || h.tel == nil {
		return ""
	}
	return h.tel.Path()
}

// closeTelemetry 收口 hub 建过的 telemetry file sink（Close 里调；幂等）。
//
// 放在**等在跑的 prompt 收场之后**：那一轮可能还在 Emit，关早了就是往已关的 fd 上写。
func (h *Hub) closeTelemetry() error {
	if h == nil || h.tel == nil {
		return nil
	}
	if err := h.tel.Close(); err != nil {
		return fmt.Errorf("host: 关 telemetry: %w", err)
	}
	return nil
}
