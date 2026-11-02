// Package acp 是 **ACP agent 端点**（v4-2-B，来源 07 K19 拍板 2026-10-31）：把 alasol 的
// session 暴露成 **ACP（Agent Client Protocol，Zed 主推）** 端点，让现成协议客户端接入。
//
// # 它为什么存在 / K19 口径（跨机 = 不自研分布式协议）
//
// K19 拍板：**不做分布式存储 / 分布式发现**。跨机时不自己发明分布式协议——把 session 暴露成
// ACP agent 端点，客户端用现成 ACP client 接入。跨机形态 = 客户端以
//
//	ssh host alasol acp
//
// 为**子进程**拉起本端点（stdio）。**鉴权交 SSH**（密钥/agent/config，系统自管），我们
// 只做 ACP 适配、**不自研鉴权**（呼应 N5 的 token scope 体系不做）。这与 v4-2-A 的
// "SSH 端口转发接 HTTP/WS"是**两条并列的接入路**（见 host/doc.go 的 SSH 远程控制一节）。
//
// # ACP 是什么 / 传输约定
//
// ACP = JSON-RPC 2.0 over stdio，**newline-delimited JSON**（一行一帧）。核心方法：
//
//	initialize             能力协商（protocolVersion / agentCapabilities / agentInfo）
//	session/new            cwd → sessionId（本实现用 hub 的 ULID **原样**作 sessionId）
//	session/load           sessionId → 载入既有会话（agentCapabilities.loadSession=true：
//	                       后端回放事件流、折叠出对话（与 resume 同源路径，挂在可选能力
//	                       sessionLoader 上）；响应前把整段历史以 session/update 回放，
//	                       随后 session/prompt 走既有 prompt 路径；**会话不存在 → 错误帧**）
//	session/prompt         content blocks → 跑完一整轮才回 {stopReason}
//	session/update         （通知，agent→client）流式增量：user_message_chunk（仅 load 回放）/
//	                       agent_message_chunk / agent_thought_chunk / tool_call / tool_call_update
//	session/request_permission （agent→client 请求）ask 审批桥：client 回 outcome
//	session/cancel         （通知，client→agent）取消当前 turn
//
// # 能力声明按后端实际能力动态给
//
// initialize 的 agentCapabilities.loadSession = **后端是否实现了 sessionLoader**
// （可选接口，type assertion 探测）——不是无条件 true。后端没实现 → 宣称 false，
// session/load 回 -32000（能力缺失）：宣称 true 却回不了 = 骗客户端（客户端会真的去
// session/load，然后拿到一个能力缺失的错）。
//
// # load 的诚实边界：历史**只回放给客户端** ≠「模型记住了」
//
// loader 的契约是"把折叠出的历史交回客户端"，**不**保证下一轮 prompt 自带这段历史。
// 模型侧能不能续上取决于后端的 RunPrompt：**hub 的 RunPrompt 不带历史**（hub 每轮不带
// 历史，历史只存在于事件流里）——故"load 之后模型接着此前对话往下说"在本实现里**不成立**；
// 要让模型续跑得走 resume 式路径（把 fold 出的历史灌回 loop），那是 hub 侧的能力。
//
// # load 不阻塞读循环
//
// 载入（后端回放 + 折叠 + 一串 update）跑在 **goroutine** 上，不占唯一的入站读循环：
// 否则别的 session 的 session/cancel 与审批应答会被一起堵住（取消无反应、审批悬到
// 超时）。回放与响应在同一个 goroutine 里顺序写 → update 仍**先于** load 响应。
//
// sessionId 由 agent 分配（本实现 = hub 的 session ULID 原样，见 Backend.CreateSession）。
//
// 通知（无 id）**不回帧**：JSON-RPC 2.0 规定通知没有响应——连"版本不对"也不例外
// （只记 Warn 日志、静默丢弃，见 endpoint.checkNotificationVersion）。
//
// # 外发面（全量清单：什么会离开这台机器）
//
// 本端点**只**通过 stdio 发出 JSON-RPC 帧。逐条列清（审计口径——"接了 ACP 之后到底外发
// 了什么"必须能一句话答完）：
//
//	① initialize 的能力协商结果（协议版本 / agent 名字与版本 / 能力布尔位）——无敏感数据；
//	② session/new 的结果（sessionId = hub ULID）；session/load 的结果（空对象 `{}`）+
//	   回放通知（历史对话的 user_message_chunk / agent_message_chunk / tool_call（rawInput
//	   经 redactJSON 脱敏）/ tool_call_update——与 prompt 期的 session/update 同一出口）；
//	③ session/prompt 的结果（stopReason）或错误帧（-32603 + **脱敏后**的错误摘要）；
//	④ session/update 通知：
//	   - agent_message_chunk / agent_thought_chunk：**模型正文与思维链增量**（会离开本机）；
//	   - tool_call / tool_call_update：工具名、tool_call_id、结果文本；工具参数经 redactJSON
//	     脱敏（敏感键 + 值级凭据打码）后才进 rawInput；
//
//	（脱敏口径的边界：**键名遮蔽只在结构化面成立**——redactJSON 只处理合法 JSON（非 JSON
//	的工具参数整体不转发），散文/命令里的凭据靠**值级**启发式兜。故 rawInput 是**参数
//	摘要**，不是完整参数回放。）
//	⑤ session/request_permission 请求：工具名 + 人读摘要 + 副作用等级 + 脱敏后的参数摘要
//	   + 选项清单（allow-once / allow-always / reject-once）。
//
// **不经 ACP 外发的**：**凭据值**（api key / token——F1 修后成立：脱敏按"值的形状"打码，
// 但那是**启发式**——变形过的凭据认不出，见"诚实边界"）、完整工具参数（未脱敏）、
// 事件库内容、其它 session 的任何数据（订阅按 sessionId 过滤）。
//
// # stdout 纯净性铁律（本包的命根子）
//
// **stdout 只走 JSON-RPC 帧**——一个字节的日志/横幅泄漏进去，客户端按行解帧就会吃到坏帧、
// 整个 ACP 会话崩掉。故：
//
//   - 唯一能写 Out（= os.Stdout）的是 conn.writeFrame（受写互斥保护，"一行一帧"原子）；
//   - **一切日志走 stderr**（Options.Log 缺省 slog.NewTextHandler(os.Stderr)）；
//   - 有回归钉子钉住这条（runtime/acp 的 stdout 纯净性用例 + **cmd 层真起子进程**的
//     CLI 级钉子：`alasol acp` 的 stdout 逐行必须是合法帧）。
//
// # 依赖方向（09 硬约束）
//
// runtime **不 import host**。本包只依赖 contracts + core（接口）+ core/bus + core/loop；
// 它经 Backend 接口驱动 agent 后端，*host.Hub 由 **cmd/alasol/acp.go** 的适配层注入
// （cmd 是唯一允许同时 import host 与 runtime 的组合层）。审批同理：本包提供一个
// core.Approver 实现（Approver，session/request_permission 桥），由 cmd 塞进
// host.Config.Approver——host 的 protocolApprover 出口是 WS，ACP 模式接不上，故在 acp 包
// 内实现（task 口径）。
//
// # fail-close（D14/H8）
//
// 审批无人应答 / ctx 超时取消 / 客户端回错 / outcome 非 allow / **认不出的 optionId**——
// 一切非显式 allow 都落 **deny**（见 Approver）。与 host 审批超时同口径：Approve 用 loop
// 带下来的整轮 ctx，不另设时钟；整轮预算耗尽即 deny。
//
// # 坏帧 / 终态错的分流（F1）
//
//	可恢复（回一条错误帧后**继续读**）：坏 JSON（顶层标量/语法错，-32700）/ 批量数组
//	（-32600）/ 超上限的帧（-32600，该行被跳过）/ 顶层 null（解成空帧、判不出形态，-32600）
//	终态（回**一条**错误帧后**结束连接**）：底层读错误——流已经坏了，再读也是死循环
//
// 终态错必须与"单帧解码错"分开：混为一谈就是"读循环无限自旋狂刷错误帧"（旧实现用
// bufio.Scanner，它的错误是粘性的——超限一次之后永远读不出下一行）。
//
// # stopReason（只用 ACP 枚举内的值）
//
// end_turn / max_tokens / max_turn_requests / refusal / cancelled。映射口径见
// mapStopReason：显式取消 → cancelled；整轮预算耗尽（超时）**或被 MaxTurns 截断**
// （模型还在要工具）→ max_turn_requests；其余 → end_turn。**枚举外取值会让现成 ACP
// 客户端解不出合法 stopReason。**
//
// "被 MaxTurns 截断"**不靠猜**：光看"跑了几轮"分不清"正常收敛在最后一轮"与"被截断"
// （MaxTurns=1 时两者都是 1 轮）——loop 用 Result.TruncatedByMaxTurns **显式**标出
// 截断，端点按它判（见 turnsExhausted）。
//
// "被**预算闸门**截断"同理：loop 用 Result.TruncatedByBudget 显式标出（缺口 5 第二件；
// W15 逐轮成本刹车）→ 端点同样映射成 **max_turn_requests**（ACP 的 stopReason 是闭合
// 枚举，没有"预算耗尽"这一档；取舍与代价见 turnsExhausted 的注释——客户端分不出
// "轮数用尽"与"预算用尽"，细分要改 ACP 契约，故本刀不加新枚举值）。
//
// # 增量与最终文本的对账（F5）
//
// 一轮结束时把**已桥出的增量**与后端给的**最终文本**对账：增量是最终文本的前缀 → 补发
// 差量（一条 agent_message_chunk）；分叉 → 最终文本整段为准。口径一句话：**增量与最终
// 文本不一致时以最终文本为准**。一轮没桥出任何增量时，这条口径同时是"降级补发整段"。
//
// **按轮对账**：最终文本是**最后一轮**的文本（core/loop 的 lastText 语义），而增量是
// **所有轮**的拼接——故桥接层按 model.delta 载荷的 turn 分桶累计，对账只取**最后一轮**
// 的增量；否则多轮（第 1 轮正文 + 工具调用、第 2 轮最终答复）会判"分叉" → 整段重发 →
// 客户端收到两份最终答复。两侧**同一 trim 口径**（尾随空白不算"分叉"）。
//
// # 诚实边界
//
//   - **cwd 不做根夹紧**：客户端给的 session/new 的 cwd 只要在**服务端**存在就被采信
//     （没有 "-acp-allow-root" 这类限制）；不存在/为空 → 回落**服务端进程 cwd**（跨机口径：
//     agent 在服务端自己的仓里干活）。且 cwd 只是**登记用**（影响 hub 自动 gate 的
//     repo_id 归集），**不决定工具在哪跑**——工具仍在进程 cwd 执行（见 host.SessionSpec）。
//   - **审批不防乱批**：能接到这条 stdio 连接的人就能应答审批（fail-close 只保证"无人应答 =
//     deny"，不防抢先 approve）。收紧方向记 v4（凭证 + 审批与身份绑定）。
//   - **凭据变形认不出**：脱敏是启发式（键名 + 值级形状），base64 / 拼接 / 拆片的凭据
//     认不出——故外发面是**参数摘要**而不是完整回放。（鉴权头的**值**定位按"词的个数"
//     来，不靠 scheme 白名单：自定义 scheme 的头也遮得住值；参数化头
//     `response=`/`Credential=`/`Signature=`/`token=` 的值另有一条规则遮；加固后另覆盖
//     api-key 赋值、AWS AKIA 形态、PEM 私钥块、连接串口令段等模式。）
//   - **脱敏不是安全边界**：这些启发式**尽力而为**，新型凭据形态拦不住（没见过的前缀、
//     新编码、拆片变形都可能漏）；**真正防线是凭据不进日志/事件**（H15：凭据不经事件流 /
//     日志外发，脱敏只是纵深防御里最后兜底的一层）。
package acp
