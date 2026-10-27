// Package core 是 agent 的骨架逻辑：loop、事件总线、策略链、上下文装配、扩展注册等。
//
// 只定义能力接口（exec / fs / net / proc），不含任何 I/O 实现；
// 不得 import runtime / extensions / host / ui（见 docs/research/09）。
//
// # nil 语义（三处统一口径）
//
// 构造函数收到 nil 依赖时，只有两种合法处理，判据是**"参数缺失后系统仍不能安全地
// 放行任何东西吗"**：
//
//   - **兜底**（fail-close 降级）：缺参数后系统仍能安全地做"拒绝" → 用一个更严格的
//     默认值顶上，并把它写进文档。例如：
//     registry.New(nil) → 换成空策略引擎 → 一切能力被拒；
//     registry.NewPolicyAdjudicator(nil) → 换成空引擎 → 一切能力被拒。
//   - **编程错误**（panic）：缺参数后系统无法安全降级 → 立即 panic，把 bug 暴露在
//     起点而不是推迟到更难诊断的地方。例如：bus.New(nil) → panic（没有 store 就无法
//     落库 durable 事件）。
//
// 三处都必须在各自的 doc 注释里写明属于哪一种，不得含糊。
package core
