package telemetry

import "github.com/Yorha9e/alasol/core"

// OTLP 是 telemetry 的 **OTLP 留位实现**（L2/D19：契约先立、落地后补）。
//
// Emit **恒返 ErrUnsupported**——诚实报"还没实现"，不静默丢弃。宿主配置面把
// mode=otlp 视为留位值：装配期就拒绝（见 host），不会把一个必然失败的 sink
// 挂进 loop 刷 Warn；本类型留给"想显式拿到留位行为"的调用方与测试。
type OTLP struct{}

// NewOTLP 返回 OTLP 留位 sink（无网络、无配置——还没有实现）。
func NewOTLP() *OTLP { return &OTLP{} }

// Emit implements core.Sink（留位：恒 ErrUnsupported）。
func (*OTLP) Emit(core.Record) error { return ErrUnsupported }
