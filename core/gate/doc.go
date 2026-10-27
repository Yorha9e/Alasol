// Package gate 是最小门禁的**判定侧**（docs/research/07 的 W7/W8/W9/W9c/W10b + V 区）。
//
// 定位（W8）：门禁的正确形态 = **不透明命令 + 一次性快照 + 事后按 receipt 校验**。
// 本包只做"判定"，**只认事实（exitCode / commit），不认意图**；命令的**执行**与
// **快照**在 runtime/gate（执行侧），两者不互相 import。
//
// # 三个模型
//
//   - **Config**（W9）：仓库级唯一配置，从仓库根 .alasol/gate.json 读，形态
//     {"command":"make ci"}。命令是**不透明 shell 字符串**，引擎不解析、不区分
//     typecheck/lint/测试——准确性的责任在写命令的人。**无探测、无默认、无分层**：
//     缺文件 = 门禁未配置（ErrNotConfigured），由调用方决定跳过还是拒绝（默认拒绝）。
//   - **Receipt**（V2 四要素 + V5）：一次门禁运行的事实记录。Verify 做四查，
//     防止"跑的是旧 commit"或"配置已改"被误判为通过。
//   - **Baseline**（W9c）：红仓上建立"已知失败指纹集"，之后只拦**新增**失败，
//     否则红仓第一次跑就死锁、基线永远建不起来（陷阱 20）。
//
// # 记录模式的口径（"文件是否存在"进入判定）
//
//	记录模式 = 显式 -record（Record=true） 或 **基线尚未建立**（文件不存在 = 首建）。
//
//	文件存在但**一条条目都没有**（被清空 / 写坏）≠ 首建：它必须 fail-close 拦下
//	（理由："基线文件为空/被清空，请用 -record 重建"）。若把它当成首建，空基线会把
//	任何失败都记成"已知"然后放行——门禁就此静默失效。
//
// # Verify 与 Judge 的分工
//
//   - **Verify**：四查（V2）——receipt 是否"干净地通过"。
//   - **Judge**：baseline 逻辑——命令失败时，失败是"已知"（放行）还是"新增"（拦截）。
//
// # 静默失败（fail-close）
//
// 命令非零退出、但输出里**没有可归一化的失败条目**（静默失败 / OOM kill / 崩溃无输出）时，
// **绝不能**当成"没有失败"——那会让静默失败直接绕过门禁。处理方式：
//
//   - 执行侧（runtime/gate.Run）显式标注 Receipt.FailuresExtractionEmpty；
//   - 判定侧（Judge）把这次失败按一条**未知失败**处理（指纹 = "__unknown_failure__:exit=<code>"）：
//     记录模式照常并入 baseline（红仓可建基线，不落陷阱 20 死锁），正常模式若该指纹
//     不在 baseline → 拦截，NewFailures 给可读说明。
//
// 组合语义（供装配层/阶段 8 用，见 Verify 注释）：先可信度后成败。
// 可信度（②commit==tip ③observed==commit ④command==config）不过 = 硬拒绝，
// baseline 救不了；只有"可信但失败"（ErrExitNonZero）才进 Judge 的 baseline 路径。
//
// # escalate 钩子（W10b）
//
// Verdict.Kind 区分 passed / recorded / blocked；blocked 时调用方走 checkpoint + 挂起
// （阶段 8 的事，本阶段只给语义，不实现挂起）。
//
// # 包纪律（09 / core doc）
//
// 本包只依赖 contracts 与标准库，**不含任何 I/O**（文件读写在 runtime/gate）：
// Config/Baseline 的解析是**字节级**（ParseConfig / ParseBaseline 吃 []byte），
// 文件路径只做字符串拼接（ConfigPath / BaselinePath）。全部函数为纯函数或对传入
// 的 *Baseline 做原地记录，可被任意宿主并发单测。
package gate
