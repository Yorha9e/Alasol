// Package bus 是 core 的进程内事件总线：把事件分发给匹配的订阅者。
//
// 职责边界（包边界纪律，见 docs/research/09）：
//   - 只做内存路由，不含任何 I/O 实现；持久化委托给 core.Store（D15）；
//   - 只依赖 contracts 与 core，不得 import runtime / extensions / host / ui。
//
// 语义铁律：
//   - F8 分流：durable=true 的事实事件经 store.Append 入库（占 seq）成功后，才带最终
//     事件通知订阅者；durable=false 的流事件不入库、不占 seq，直接通知订阅者；
//   - **store 违约不等于事实消失**：违约（条数不符 / 漏事件 / 事件没有 seq）时仍通知
//     **已确认（带 seq）** 的那部分，并在错误里写明"已入库未通知/部分通知"的条数，
//     调用方据此用 store.ReadFrom 从自己的 seq 水位补读——事实流不分叉；
//   - S7 幂等由 store 负责（去重只看 Event.ID），bus 自身不去重、重复 Publish 透传给 store；
//   - 投递非阻塞、尽力而为：订阅队列满即丢弃该条并计数（Lagging）——事实事件已在库，
//     订阅者可经 store.ReadFrom 从自己的 seq 水位补读；流事件丢了就是丢了（诚实取舍）；
//   - 并发安全：多 goroutine 可并发 Publish / Subscribe / Close；单次 Publish 批内保序，
//     且对同一订阅者的通知在该批内连续、不与并发批交错（跨 Publish 顺序不强保证，G25）。
package bus
