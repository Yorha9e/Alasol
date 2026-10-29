// Package sandbox 是 core 能力接口的**沙箱实现**：SandboxedExecutionEnv（K8）。
//
// 定位（K8 / P0-4）：core 只定义能力接口、不含任何 IO；实现由 runtime 提供、host 装配时注入。
// agent 的工具执行环境因此可插拔：
//
//	LocalExecutionEnv      runtime/cap/local    进程内、本机文件系统、无 OS 约束（现状/v0 底座）
//	SandboxedExecutionEnv  runtime/cap/sandbox  子进程 + OS 沙箱（**本包，v2-S4**）
//	RemoteExecutionEnv     （远端 SSH，v3；只留位置，不在本刀）
//
// **升级到进程隔离只需换 env 实现，不动 agent 逻辑**——这是 K8 的酸测试，
// host 侧就是一个 opt-in 开关（host.Config.Sandbox，形态对齐 Config.Graph：默认关）。
//
// # Windows 三件套（H9 定案，2026-10-28）
//
// Windows 没有能直接映射到 agent 工作负载的原生沙箱组件（AppContainer 要求预声明能力、
// MIC 要求改对象信任级别、Windows Sandbox 需 Hyper-V 且摸不到真实工作区，见 07/V8 与
// docs/research/01 §4.2）。定案是**务实组合**：
//
//		a. **Job object**：工具子进程纳入 job object，带 JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE。
//		   进程树收容——命令 daemonize / detach 出去的进程也被拉住：job 句柄一关，
//	   job 内所有进程一起死。这收口了 local 文档里"杀树只覆盖我们起的这棵树"的遗留
//	   （local 用 taskkill /T /F 兜底，只能杀**已知**的树；脱离树的后代它看不见）。
//
//		b. **受限 token**：CreateRestrictedToken（当前用户 SID 转 deny-only + **组 SID
//		   收紧**（Authenticated Users / INTERACTIVE / … 一律 deny-only）+ 删危险特权），
//		   子进程以受限 token 跑。**这是文件约束的真正来源**：用户 SID 一旦 deny-only，
//		   子进程就继承了"这个交互用户对文件的访问"之外的一切——而它自己什么都没有。
//		   （组 SID 收紧的取舍与实测代价见下面"诚实账本"一节。）
//
//		c. **目录 ACL**：为允许写的根设 DACL（可继承 ACE 授予"沙箱可写 SID"），
//		   **目录外写被 OS 拒绝**。注意方向：不是"给允许目录开个后门"，而是
//		   "先抽走子进程的全部文件访问（b），再按允许集一点点发还（c）"。
//		   所以 ACL 是**授予**、token 是**剥夺**，两件事合起来才是内核强制的白名单。
//
// # V8 收口：写保护的诚实边界往上升了一级
//
// V0 的边界是"路径守卫是策略层的事，bash 天然能绕开"（见 runtime/cap/local 的包文档）。
// 本刀之后：
//
//   - **bash / cmd 写允许目录之外 → 被 OS 拒绝**（内核强制，不是 harness 里的字符串
//     比较）。这是 V8 的"真正的解法"。
//     **限定**：默认模式下"BUILTIN\Users 有写权限的位置"（典型是 C:\ProgramData）
//     仍可写——严格模式（StrictGroupSIDs）才连它一起堵上，见下面"诚实账本"。
//   - 仍然**不在**本刀范围（诚实挂账）：Net 维度（出站网络没有沙箱化，仍是策略层的
//     域名白名单）、Proc 维度（长驻进程管理没有独立约束，仍走 core.ProcCapability 的
//     约定）、以及**进程内工具**（read / write 工具跑在宿主进程身份下，不受子进程 token
//     约束——它们由策略链 + 允许根检查兜着，不是 OS 强制）。
//
// # 收紧组 SID 之后的取舍（F2 的诚实账本）
//
// 受限 token 现在把**绝大多数组 SID**（Authenticated Users / INTERACTIVE /
// SERVICE / BATCH / …）置成 deny-only，只留三项 enabled：
// 登录会话 SID（允许根授的就是它）、Everyone（映像装载路径需要它）、
// 以及 BUILTIN\Users（见下）。
//
// 换来的：**目录外写被内核拒绝**在常见位置真的成立——C:\Users\Public
// （授 INTERACTIVE）与仓库盘（授 Authenticated Users:(M)）在收紧前都能写，
// 现在都写不了（本机实测）。
//
// 剩下的取舍（本机实测，Windows 11 / 非管理员）：
//
//   - **BUILTIN\Users 保持 enabled**（默认）。原因：把它也 deny-only 之后，
//     **用 overlapped I/O 写文件的程序全部写不进去**（Go/.NET/Rust/Node 都用
//     overlapped I/O）——Go 的 os.File.Write 直接返回 `winapi error #10093`，
//     连允许根里的文件都写不了。沙箱会退化成"只能跑 cmd 内建命令"。
//   - 代价：**只授予 BUILTIN\Users 写权限的目录仍然可写** —— 典型是
//     C:\ProgramData（它的 ACE 是 BUILTIN\Users:(CI)(WD,AD,WEA,WA)）。
//     **Policy.StrictGroupSIDs = true**（CLI: -sandbox-strict-groups）可以把它堵上，
//     代价就是上面那条（工具写不了文件）。
//   - 顺带一提：**严格模式**下受限 token 读不到"只授予 BUILTIN\Users"的系统消息表，
//     所以沙箱里命令失败时**看不到** "Access is denied." 这类 Windows 错误文本
//     （退出码与"文件没出现"仍然如实反映内核的拒绝）。默认模式下文本在。
//
// # DACL 改写：会改，而且不还原（F11）
//
// 给允许根设 DACL 是**追加**可继承 ACE（见 acl_windows.go），因此：
//
//   - 工作区里**每个文件**的 DACL 都会被追加一条授予（Windows 会把可继承 ACE 传播到
//     既有子对象）——**不还原**（没有"撤销"这一步）；
//   - 授予的权限已经收窄到读/写/执行/删（**不含** WRITE_DAC / WRITE_OWNER，
//     免得沙箱里的进程能改权限或抢所有权），但"多了一条 ACE"这件事本身留在机器上。
//
// 要不要开沙箱，是用户的选择；但**开了就要知道工作区的 DACL 被改过**。
//
// # V10：WER 崩溃弹窗完整压制落在本层
//
// local 只做 windowsHide（不起控制台窗口）；"子进程崩了别弹窗炸桌面"整套
// （SetErrorMode(SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX) + WerAddExcludedApplication）
// 属于沙箱层，在本包落地。子进程会**继承**父进程的 error mode，所以设一次即覆盖全树。
//
// # 可拓展埋点（H9）：后端是可插拔接口
//
// "把进程约束 / 文件约束施加到子进程"这件事被抽象成 Backend（见 backend.go）。
// Windows 三件套是其中一个后端；Linux / macOS 的 seatbelt / bubblewrap 是**预留挂点**
// （backend_other.go 有接线说明，本刀不实现）。未注册后端的平台上 New 直接报错——
// **fail-close：没有后端就不假装有沙箱**。
//
// # 凭据纪律
//
// 本包不引入任何新凭据面：受限 token 是从**当前进程 token** 派生的（不新建账户、
// 不需要密码、不读凭据），目录 ACL 授予的是"当前登录会话 SID / 当前用户 SID /
// SYSTEM / Administrators"这类本机身份。测试也**不要求管理员权限**——需要提权的用例
// 一律 t.Skip 并注明原因。
package sandbox
