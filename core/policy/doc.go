// Package policy 是策略链求值引擎（H2/H3/H5/H24/D13）。
//
// 定位（docs/research/01 §3、07 的 H 区）：
//   - 求值顺序固定 deny → ask → allow，**首个命中即返回，规则特异性不改变顺序**（H2）；
//   - deny 永远赢，且**不能写 allow 例外**（H3）；
//   - deny / ask **跨所有来源取并集**（任一来源命中即 deny/ask，不看优先级）；
//     只有 allow 才按来源优先级解析：managed > cli > project > workflow > local > user > session（H5）；
//   - workflow 来源的规则**只可收窄**：其 allow 被完全忽略，deny/ask 照常参与并集（H5/H24）；
//   - 一切非显式 allow 都落 deny（fail-close，H8/陷阱 9）。
//
// fail-close 的一条延伸：**被拒绝的非法规则会污染它的 Tool**——非法 deny / ask 被丢掉等于
// 把一条**跨来源生效**的限制静默拆掉（非法版本甚至比它的合法版本更宽松：非法 workflow deny
// + managed 合法 allow = allow，人审被笔误绕过）。污染判据是**被丢规则的 effect**：
// deny / ask（以及读不出的 effect）→ 该工具全局 deny；allow → 不污染（丢的是许可，
// 没有其他 allow 时 fail-close 默认已 deny）。理由点名污染源规则 id / effect / source。
// **合法规则的 deny/ask 语义完全不变**（H5 并集照旧）。
//
// 包纪律（09）：本包只依赖 contracts 与 core 的进程内接口（core.Approver、core.NewUlid），
// **不含任何 I/O 实现**、不 import runtime / extensions / host / ui；
// Engine 构造后不可变 → 可被任意宿主嵌入与并发单测。
package policy
