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
// 错误口径：调用方用 errors.Is 判定 ErrNotFound / ErrAlreadyRegistered / ErrNotAllowed
// （以及 ErrStale：裁决期间实例被替换）。
//
// 包纪律（09）：本包只依赖 contracts、core 的进程内接口与同级的 core/policy，
// **不含任何 I/O 实现**、不 import runtime / extensions / host / ui。
package registry
