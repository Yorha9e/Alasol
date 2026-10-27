// Package durable 实现 X 区「机器」：崩溃恢复的 checkpoint / resume / ownership（阶段 8，最后一个实现阶段）。
//
// # 定位与边界（C2）
//
// 本包是 core 的子包，**只依赖 contracts 与 core 根包**，不 import runtime、不 import
// loop / registry / bus。它经 core.Store 读写 durable 事件，把「进程半夜挂掉后从哪续」
// 这块机器补齐。写路径的三段形状（intent → 执行 → result，X1）已在阶段 1/4 定死，
// 本包**不回头改写路径**，只在事件流之上叠 checkpoint / resume。
//
// 与 runtime 的分工：fold 事件、扫 in-flight、按 X2 分三类处置、写 checkpoint.created /
// session.resumed 的**决策与编排**在本包（纯逻辑，可单测）；真正再次执行工具的 Replayer、
// 以及基于 git 的默认 Verifier 在 runtime 侧实现并经接口注入（core 不碰 I/O）。
//
// # 四项 checkpoint 内容（X3）
//
//	① seq_watermark  事件流 seq 水位（本 session 事件的最大 seq）
//	② ledger_seq     ledger 快照版本（最后一条 ledger.updated 的 seq；0=无）
//	③ in_flight      in-flight 清单（有 intent 无 result 的操作引用，X1）
//	④ tree_version   agent 树投影版本（最后一条 agent.* 的 seq；0=无）
//
// checkpoint 本身落为一条 durable 事件 `checkpoint.created`（内容是上述四项的引用）。
//
// # resume 流程（X4）
//
//	① 读最后一个 checkpoint.created（无 → 全量重建，from_checkpoint_seq=0）
//	② fold 事件重建状态：ledger（LWW，语义同 runtime.FoldLedger）+ agent 树/ownership 投影
//	②' 认领本 session 的 resume 权（并发互斥，见下）——拿不到就直接退出
//	③ 处置 in-flight（X2 三类，见下）
//	④ 恢复 agent 树结构（已完成不重启、running 节点落 disposition 说明；真正续跑 turn 由调用方 loop 接手）
//	⑤ 落一条 session.resumed 事件后返回 ResumeReport
//
// # in-flight 三类处置（X2，承重墙）
//
//	replay=safe    → 自动重放：调 Replayer.Replay 再次执行并（由 Replayer）落新 result 事件，
//	                 本包记 disposition=replayed（**重放成功**）；重放执行了但**失败**
//	                 → disposition=replayed_failed（需人工重做，detail 带失败码）。
//	                 safe = 重复执行无副作用，重放一次有效副作用。
//	replay=verify  → 先核验外部状态：调 Verifier.Verify(op.Anchors)。
//	                   核验通过(=外部事实表明已完成) → 本包直接补一条 completed（合成结果，注明
//	                   verified），**不重放**，disposition=verified；
//	                   核验不通过 → 当作未完成，走重放，disposition=replayed；
//	                   核验出错 → 无法确认，fail-safe 不盲目重放，disposition=manual。
//	replay=unsafe  → **不重放**（发邮件 / 删除类，未知结果不可盲跑），disposition=manual，
//	                 转人工 / agent 判定（X2）。
//
// # 幂等 resume
//
// 连跑两次不重复处置：replayed/verified 的 op 在第一次已落 result 事件 → 第二次 FindInFlight
// 不再看见它；manual（unsafe）的 op 不落 result，但第一次的 session.resumed 已把它记进
// 「已处置集合」→ 第二次按 (tool_call_id) 跳过。于是第二次 ResumeReport.Dispositions 为空。
//
// # 并发互斥（认领）
//
// 幂等挡不住**并发**：两个 resume 都在"已处置集合"落盘之前扫到同一个 in-flight →
// 同一调用被重放两次（verify 类工具有副作用）。故处置之前先**原子认领**本 session 的
// resume 权：认领事件的 Event.ID 由 (session, 轮次) 确定性导出，靠 store 的幂等写
// （ON CONFLICT(id) DO NOTHING）保证只有一行存在；认领载荷里的 attempt id 决定赢家，
// 输家直接报 ErrResumeInProgress 并退出（不进处置循环）。认领由本次的 session.resumed
// 释放；崩在处置中途 → 认领未释放 → 下次 resume 报「已有 resume 在进行」，
// 需显式 ResumeOptions.ForceResume 接管（宁可要人拍板，也不自动抢可能还在跑的活）。
//
// # X6 ledger 写放大
//
// LedgerWriter 区分关键字段（目标/约束，UpdateCritical 立即落一条 ledger.updated）与过程字段
// （已完成/待解，UpdateProcedural 只改内存缓冲、turn 末 Flush 合并为一条）。落下的每条
// ledger.updated 仍是全文快照（G8/G23，事件流唯一事实源不变）；X6 只控制**写频率**。
package durable
