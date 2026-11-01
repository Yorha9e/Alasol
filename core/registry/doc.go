// Package registry 是扩展注册表：注册 / 生命周期 / 能力裁决（E3/E5/E6/D17）。
//
// 定位（06 §4、07 的 E 区）：
//   - 每个扩展点统一元数据：能力声明（E3）、优先级、健康状态、可热重载标识（D17）；
//   - **安装时申报能力，运行时由策略引擎裁决**（E3）——capability 字符串当"工具名"交给
//     Adjudicator（与策略规则同一套求值，规则可写成 Tool:"fs:write", Effect:deny）；
//   - **fail-close**：能力无命中（没有 allow 规则）= 拒绝启用（与 policy 一致，H8）；
//   - **撤销也 fail-close**：已启用的扩展重新裁决被拒 → 先置 disabled 再报错
//     （能力已不获准却仍 enabled = 事实与权限不符）；
//   - **同名重注册不背书**（TOCTOU）：裁决在锁外进行，裁决期间同名扩展被注销并重新注册
//     → 拒绝启用（ErrStale），必须重新 Enable；
//   - **失败隔离**（E5）：能力裁决在锁外进行，裁决方 panic 被捕获得 deny，不带崩注册表；
//   - 热重载语义（E6）：State 只描述注册表内的生命周期，Reloadable 是元数据声明，
//     本包不因此改变任何行为。
//
// # extension runtime 收口（v4-5）：kind 开放清单里的三个"机制位"当前不建
//
// contracts.ExtensionMeta.Kind 是**开放字符串**，其注记列了 tool / provider / policy /
// store / projection / **compaction / assembler / scheduler** / panel / endpoint /
// transport / telemetry / approver / sandbox 等。v4-5 的落地口径（诚实记录，别让清单误导）：
//
//   - **A 类已落机制**（接口实现为主）：tool / provider / policy / store / projection
//     五类在本仓有真实实现与接线；
//   - **B 类边界已落**：MCP server（`alasol mcp-serve`，runtime/mcpserver）把 harness
//     暴露成 MCP server，是 B 类（进程级边界）的第一个落点；GUI 面板 / IDE 插件 /
//     第三方工具 / 不可信扩展同属 B 类，后续按需接入；
//   - **compaction / assembler / scheduler 三个 kind 留位、当前不建机制**：它们只是 Kind
//     开放清单里的占位名，本包（及 core）**没有**为它们建任何注册/解析/调度机制，也没有
//     对应的机制契约。14 第 5 项拍板"extension runtime 重点不是新机制"——本轮只收口既有
//     A 类剩余 + E3/E5 做实 + B 类 MCP server，不新造这三个扩展点的机制。后续要建时再补
//     机制契约与实现，不要在没机制时按清单名假设它们已可用。
//
// 错误口径：调用方用 errors.Is 判定 ErrNotFound / ErrAlreadyRegistered / ErrNotAllowed
// （以及 ErrStale：裁决期间实例被替换）。
//
// 包纪律（09）：本包只依赖 contracts、core 的进程内接口与同级的 core/policy，
// **不含任何 I/O 实现**、不 import runtime / extensions / host / ui。
package registry
