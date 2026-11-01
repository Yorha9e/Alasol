// Package mcpserver 把 harness 的工具注册表暴露成一个 **MCP server**（B 类扩展边界，
// v4-5；与 runtime/mcp 的 **client** 侧能力相对：一个进、一个出）。
//
// # 它为什么存在 / 什么变化会导致它变（09 包边界）
//
//	存在理由：让**外部 MCP client**（GUI 面板 / IDE 插件 / 第三方工具 / 不可信扩展，07 E1/E2
//	的 B 类）能把 alasol 的工具当普通 MCP tools 发现并调用——除此之外它什么都不是。
//	会变：换 MCP SDK 版本、加一种传输（stdio 之外再接 Streamable HTTP）、改暴露白名单口径。
//	不会变：策略链、X1 三段写路径、事件契约、core/loop——本包一行都不该碰它们。
//
// # 依赖方向
//
// runtime（真实世界实现）→ contracts + core 的接口 + 第三方 SDK。按 09 的腐化防线，
// **core 里不得出现任何 MCP SDK 类型**——适配全部收在本包（与 runtime/mcp 同款纪律）。
//
// # 暴露面：只暴露**显式声明**的工具（fail-close 白名单）
//
// 构造时给一份白名单（exposeTools）。**空 = 一个工具都不暴露**（tools/list 返回空集，
// 任何 tools/call 都命中 unknown tool）。只有名字进白名单、且注册表里真有对应工具条目的
// 才被暴露。取舍写明：
//
//   - harness 的**写面**（write / bash）与任意第三方工具默认**不可**被 MCP client 摸到——
//     任意能连上 stdio 的进程 != 被授权调用 harness 的写能力。要让某个工具可被调用，
//     必须在白名单里**点名**它（显式 opt-in），这与 R5"不暴露即不存在"同精神；
//   - 白名单是**唯一的暴露闸**（见下"不自研鉴权"）：它粗粒度地决定"这个 MCP server 面
//     向它的 client 开放哪些工具"，不替代 loop 内的 per-call 策略链（那是 loop 的事）；
//   - **注册表 State 参与暴露与执行判定**：条目被停用（State=disabled——含 Disable 与
//     能力裁决失败后的 revokeEnabled）→ tools/list 里没有它、tools/call 也调不动
//     （执行前**再查一次** State，防"list 之后才被停用"的窗口）；其余状态照常暴露
//     （registered = 内置工具的常态、enabled = 能力裁决已获准）；
//   - **命名口径：暴露面只收非 mcp__ 名**：`mcp__*` 是 MCP **client** 侧的内部命名空间
//     （外部 server 的工具已经由 client 聚合过一次），**不上公网面**——白名单点名也拒
//     （fail-close，与空白名单同精神；外部 MCP 工具经聚合不二次暴露）；
//   - **工具名必须是合法 MCP 名**（字符集 [A-Za-z0-9_.-]、长度 ≤128 字节）：非法名
//     **不挂载**（go-sdk 只记一条日志、照样挂上去 = 列表里看得见、调不动的假暴露）。
//
// # 鉴权口径：stdio = 谁拉起谁负责（同 ACP 的 ssh 口径，N5 精神）
//
// stdio 传输下，MCP server 的 client 就是**拉起本进程的那一方**（典型：`ssh host
// alasol mcp-serve`，鉴权交 SSH；或本地父进程）。本包**不自研任何鉴权**——不查 token、
// 不查来源、不做 per-client 授权。安全性来自"谁能起到这个进程"这一既有的 OS/SSH 边界，
// 加上上面的白名单把暴露面收到最小。HTTP 传输（可选）同理：谁配的端点、白名单里开了什么，
// 谁负责——本包不在内嵌鉴权。
//
// # E5 失败隔离：工具执行 panic / 失败不得带崩 server
//
// 每次 tools/call 都在 handler 内 **per-call recover**：执行器 panic 被捕获、转成一条
// MCP 错误响应回给 client，server 会话与底层 hub 照常存活（"扩展崩溃不得带崩 core"的
// MCP server 面落点）。执行器返回的普通 error 同样转成 MCP 错误响应（不 panic、不中断）。
//
// # 与 loop 的关系：不走 X1 三段写路径
//
// 本包的 tools/call **直接**调注册表里的 core.ToolExecutor（loop 调用的同一个执行器），
// **不**建 session、**不**走 loop 的 tool.call.intent → 执行 → completed/failed 三段事件
// 落盘。理由：MCP server 面是一次性的"把工具开放出去"，不是一轮 agent 对话；把 loop 的
// 事件 sourced 会话整个搬过来既沉重又引入"这个 session 归谁"的所有权问题。白名单 +
// "谁拉起谁负责"就是这条 B 类边界的授权模型。工具执行本身仍受执行器自己的约束
// （如沙箱 / ExecutionEnv），本包不额外放松。
//
// **诚实的边界（loop 有、本包没有）**：loop 每次调用前会 buildExecEnv 把工具声明的
// RequiredEnv 凭据经 ctx 注入（policy 裁决后），并跑 per-call 策略链。本包**三样都不做**：
//   - 不带 per-call 策略链（授权模型是白名单 + "谁拉起谁负责"，见上）；
//   - 不做 loop 的 RequiredEnv 凭据注入——经 MCP 调用的工具拿到的是**朴素 ctx**，执行器
//     只能用它自己装配期拿到的能力（FSCapability / ExecCapability）或进程 env；
//   - **审批闸不适用**：loop 的 ask 策略命中会挂起等人应答（无人应答 = deny，D14/H8）；
//     本包**不挂起、不问人**——approvable 的工具（如 bash）一旦被点名，就是**零审批直接
//     执行**。要审批语义就别把它暴露出去（白名单不点名它 = 那条审批语义对 MCP client
//     恒不成立）。这是已知边界，不是疏漏。
//
// 因此：把"需要 loop 按调用注入凭据"的工具经 MCP 暴露时，那些凭据不会自动到位——这是已知
// 边界，不是疏漏。典型白名单（read 等只读/本地工具）不受影响。要暴露需凭据的工具，应由
// 执行器自身从进程 env / 装配期能力取，而非依赖 loop 的 per-call 注入。
//
// 同理，被点名的 **approvable 工具**（bash 等）在本包里**没有审批闸**：MCP client 的
// tools/call 直接落到执行器，**零审批**。这是 B 类边界的既定取舍（授权模型 = 白名单 +
// "谁拉起谁负责"），不是遗漏——但它意味着"能连上 stdio 的进程"对被点名的工具握有与 loop
// 里同等的执行权，故白名单要按"写面"来点。
package mcpserver
