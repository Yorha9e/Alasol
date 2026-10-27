// Package tools 是 v0 的三个内置工具：read / write / bash。
//
// # 描述与执行分离（D1/D12）
//
// 每个工具两半：
//
//   - **描述**（contracts.Tool）：给模型看的名字/说明/参数 Schema，以及给策略链看的
//     副作用等级、是否需人审、是否幂等、重放语义。跨边界只传这个（Descriptor）。
//   - **执行器**（core.ToolExecutor）：进程内接口，只负责执行这一次调用。
//
// 分离的理由：描述要过网络/进 prompt，执行器不能（它是 Go 接口值）；而且审批配置
// （Approvable）在描述上、执行器无感知——**策略引擎才是真正的权威**，描述只是提示。
//
// # 三段事件不归这里（X1）
//
// 执行器**不写事件**：tool.call.intent → 执行 → tool.call.completed/.failed 由 loop 层
// 落盘（intent 必须先 durable 才执行）。本包只返回 contracts.ToolResult 或 error。
//
// # 错误文案是给模型看的（M5）
//
// 所有 error 的文案都按"模型能读懂并自我修正"的标准写：说清哪个参数错、错在哪、
// 大概怎么改。不要让模型看到 "invalid argument" 这种无从下手的句子。
//
// # 输出截断
//
// 单次工具返回的文本截到 maxToolOutput（64KB），截断处带明确标注。
// 理由：工具输出会整段进上下文，64KB 已经是几千 token；不标注的话模型以为看全了。
// 二进制文件 read 直接报错（v0 只有文本内容块，image/resource 留 v1）——
// 静默替换成 U+FFFD 会把文件内容污染成错的，比报错更糟。
//
// # 路径与 shell（陷阱 7 / 陷阱 8）
//
//   - read/write 的路径**原样**交给 FSCapability（local 实现也不做守卫）：
//     守卫是策略层的事，bash 又天然能绕开——V0 已知边界，UI 必须声明；
//   - bash **显式**起 shell（Unix: bash -c，Windows: cmd /c，build tag 分平台）。
//     这与 Exec 能力"不走 shell"不矛盾：那是能力层不猜语义，这里是工具层明确要用 shell。
package tools
