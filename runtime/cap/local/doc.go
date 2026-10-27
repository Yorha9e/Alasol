// Package local 是 core 能力接口的**本机实现**：FS（core.FSCapability）与
// Exec（core.ExecCapability）。
//
// 定位（P0-4）：core 只定义能力接口、不含任何 IO；实现由 runtime 提供、host 装配时注入。
// 本包就是那个"本地进程 + 本地文件系统"的实现——将来换沙箱/远端只换这一个包的实例，
// core 与工具逻辑一行不动（K8 酸测试）。
//
// # 路径守卫是策略层的职责（V8 / 陷阱 8）
//
// 本包**不做任何路径校验**：路径原样传给 os 包。为什么不做在这里：
//
//   - 守卫需要策略上下文（哪个 agent、哪条规则、允许哪些根目录），那是 core/policy 的事；
//   - 在能力层偷偷加一层"看起来安全"的校验，会让人以为这里已经安全了——**双假象比没有更危险**；
//   - bash 工具天然能绕开任何路径守卫（进程自己写文件），V0 这是**已知边界**，
//     必须由 UI 显式声明（陷阱 8），V1 用沙箱真正解决。
//
// 所以：装配方必须在本包**前面**放策略裁决（policy.Decide），不要把本包当边界。
//
// # 不走 shell（陷阱 7 精神）
//
// Exec.Run 用 exec.CommandContext 直接起可执行文件：name 就是可执行文件，不经 shell 解释。
// 想让命令走 shell 的（bash 工具）由**工具层显式**传 shell 路径 + "-c"，意图明确、
// 不会被 cwd / PATH 劫持静默改语义。Windows 下 git 之类必须用绝对路径 execFile 也是同一条纪律。
//
// # Windows 特例（陷阱 5）
//
// 子进程一律带 windowsHide（syscall.SysProcAttr{HideWindow: true}，见 proc_windows.go），
// 避免每次起进程弹一个控制台窗口。WER 崩溃弹窗的完整压制属于沙箱层（JobObject /
// 受限令牌）的职责，V0 只做 HideWindow。
//
// # 取消与进程树
//
// Exec.Run 用 CommandContext + 自定义 Cancel：ctx 结束即杀**整棵进程树**
// （Windows 走 taskkill /T /F /PID，陷阱 5 的定案做法）。
//
// 为什么必须杀树：Windows 实测——只杀直接子进程时，孙子进程仍攥着 stdout/stderr
// 管道的写端，cmd.Wait 会一直挂到孙子自己退出（本机 cmd /c ping -n 30 取消后
// **29 秒**才返回）。杀树 + Cmd.WaitDelay（5s 硬上限）保证取消后立刻返回。
// 这两条都是"让 Wait 能及时回来"的必要条件，不是可选的进程管理 fancy feature。
//
// 仍属已知边界：杀树只覆盖我们起的这棵树；命令自己 daemonize / detach 出去的进程
// （脱离了树）不在管辖内——那是沙箱层（V1）的职责。
//
// # 退出码不是错误
//
// 命令非零退出**不返回 error**（ExitCode 字段带上），只有"起不来 / 被取消 / IO 失败"
// 才是 error。理由：非零退出是正常业务信息（测试失败、grep 无匹配），
// 调用方（工具层）要能把它渲染给模型看（M5），而不是当成异常吞掉。
package local
