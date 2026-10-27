// Package terminal 是 core.Approver 的最小终端实现：把审批请求打印到 stdout，
// 从 stdin 读一行 y/N。
//
// 定位：阶段 4 末尾补的"最小终端审批"——目的是让人能自己跑通真机回路，
// 并顺手验证 contracts.ApprovalRequest/Response 这条跨边界契约真的能闭环。
// 它不是产品级审批 UI（那要进 ui/ 层，支持 escalate / 恢复决策 / 审计视图）。
//
// # fail-close 是唯一口径（D14/H7/H8/陷阱 9）
//
// 只有**显式**的 y / yes 才放行；其余一切——n、no、空回车、乱输、EOF、读错误、
// ctx 超时/取消、审批人缺席（nil 输入源）——**一律 deny**。
//
// 为什么这么死板：审批这道关的意义就在于"没明确同意就不动手"。
// 任何"看起来像同意"的启发式（比如默认值、超时后放行、重试一次）都会把
// fail-close 变成 fail-open，而这类 bug 在现场是最难发现的那种。
//
// # 值与凭据
//
// 打印的只有 req.Summary / req.Detail / 触发器 / 副作用等级 / tool_call_id——
// 这些都是契约里给人看的字段。**env 注入的值永不进这里**（H19/H21：core.EnvScope
// 只在 ctx 里流转，本包也从不读 ctx 的 env）。
//
// # 并发
//
// 一次只问一个审批（内部互斥锁串行化）：并发问两个人，输出会交错成没人看得懂的样子。
// 第二个请求排队等着——比交错更好；真要并发审批请换成走 UI/消息通道的实现。
package terminal
