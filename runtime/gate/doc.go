// Package gateexec 是最小门禁的**执行侧**（package gateexec，与判定侧 core/gate 不同名）。
//
// 定位（docs/research/07 的 W8 + V 区）：只负责把不透明命令跑起来、产出 Receipt 事实，
// 不做任何判定（判定在 core/gate）。两包不互相 import，经共享的 core/gate.Receipt 衔接。
//
// # 三件事
//
//   - **仓库配置 / baseline 的加载与落盘**：LoadConfig（.alasol/gate.json，缺文件 →
//     gate.ErrNotConfigured，fail-close）、LoadBaseline / SaveBaseline（.alasol/gate-baseline.json，
//     原子写 + fsync）。**文件是否存在**进入判定：缺失 = 首建（Established=false → 记录模式）；
//     存在但条目为空 = 已建立 → fail-close 拦下（不是首建）；SaveBaseline 不写空基线。
//     core/gate 只提供字节级 Parse* 与路径拼接，文件 I/O 全在本包。
//   - **一次性快照 worktree**（V1）：Snapshot 用 git worktree add --detach 把某个 commit
//     检出到独立 detached worktree。**未提交改动结构性不进快照**（快照只含 commit 内容），
//     比 dirty 检查干净；并行跑 N 个门禁 = N 个独立快照，互不干扰。
//   - **跑不透明命令**（W9/V3/V5）：Run 在快照目录经 shell 跑 Config.Command
//     （Windows cmd /c、其他 bash -c——这是全包唯一走 shell 的地方，因为 W9 明确命令是
//     不透明 shell 字符串）。注入约定 env（CI_WORKTREE / CI_COMMIT / CI_PORT_BASE）让命令
//     自隔离外部资源（V3 留白）；stdout/stderr 合并捕获，LogTail 只留尾部 ≤64KB，记 ElapsedMS（V5）。
//   - **Observ edCommit**：命令跑完再 rev-parse HEAD 一次，跑期间 HEAD 若移动会被 core/gate
//     的四查③抓住（V2）。
//
// # git 纪律（V10 / 陷阱 6 / 陷阱 7）
//
//   - git 走**绝对路径 execFile git.exe**（exec.LookPath 拿到后 filepath.Abs 固化，sync.Once 缓存），
//     **不经 shell**，防 cwd 劫持；
//   - 每步 ctx 可取消；取消杀进程树（Windows taskkill /T /F /PID）；
//   - WindowsHide：不起控制台窗口；
//   - cleanup 先 git worktree remove --force，再兜底删目录；删目录走 removeTreeSafe——
//     **junction/符号链接防御**：Lstat 判 reparse point，遇链接只删链接本身、绝不递归跟穿。
//
// # 自包含
//
// 本包**不 import runtime/cap/local**：执行一个"经 shell 且需合并输出 + 注入 env"的进程，
// 与 exec 能力"不走 shell、分离捕获"的语义不同，复用反而别扭。此处自带一套最小进程执行
// （runProcess + 平台相关的 hideWindow/killTree），与 core 无耦合。
package gateexec
