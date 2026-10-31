package core

import "time"

// DefaultPromptRunTimeout 是**一轮**（一个 prompt 的全部 turn）的总预算缺省值。
//
// 口径（别读成别的）：这是**整轮**的上限，**不是**单次模型调用的超时，也**不是**单次
// 审批等待的超时——一轮里可以有多次审批等待，它们共用这份预算（预算耗尽 → 后续审批
// 立即按 deny 收场，fail-close 的兜底时钟）。
//
// 为什么必须有界：协议面发起的 prompt 可能卡在审批（无人应答）上；没有上界，一个
// 卡住的 loop 会**永久挂起**。
//
// # 谁在用（同一份常量，两处协议面）
//
//	host    —— 协议面（HTTP/WS）发起的异步 prompt 的整轮上限（host.Config.PromptRunTimeoutSec 覆盖）
//	runtime/acp —— ACP 端点一轮 session/prompt 的整轮上限（acp.Options.TurnTimeout 覆盖）
//
// 两处**必须同值同口径**：同一个"-prompt-timeout"配置在 HTTP 面与 ACP 面上若给出
// 不同的整轮上限，运维会得到两种行为（同一台机器上"为什么这里等 10 分钟那里等 1 分钟"）。
const DefaultPromptRunTimeout = 10 * time.Minute
