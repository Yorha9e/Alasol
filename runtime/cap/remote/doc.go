// Package remote 是 core 能力接口的**远端 SSH 实现**：RemoteExecutionEnv（K8 第三档）。
//
// 定位（K8 / P0-4 / 07 K8）：core 只定义能力接口、不含任何 IO；实现由 runtime 提供、
// host 装配时注入。agent 的工具执行环境因此可插拔：
//
//	LocalExecutionEnv      runtime/cap/local    进程内、本机文件系统、无 OS 约束（v0 底座）
//	SandboxedExecutionEnv  runtime/cap/sandbox  子进程 + OS 沙箱（v2-S4）
//	RemoteExecutionEnv     runtime/cap/remote   执行面经 **SSH** 到远端机器（**本包，v4-2-A**）
//
// **升级到进程隔离/远端只需换 env 实现，不动 agent 逻辑**——这是 K8 的酸测试，
// host 侧就是一个 opt-in 开关（host.Config.Remote，形态对齐 Config.Sandbox：默认关）。
//
// # 依赖纪律：**不引 golang.org/x/crypto/ssh**（v4-2-A 拍板）
//
// 本包**不新增任何 go.mod 依赖**：执行面走**系统 `ssh` 可执行文件**（os/exec 起 ssh，
// 不经本地 shell、参数以数组传递）。理由：
//
//   - 不自研 SSH 协议栈（那会引入 x/crypto 这一整棵依赖树，且要自己维护握手/加密/通道）；
//   - 系统 ssh 已经处理了密钥/agent/config/known_hosts/端口/跳板机一整套——
//     我们只做"适配层"，把 core.ExecSpec 翻成一条远端命令。
//
// # ⚠ 语义差：ExecSpec 说"不经过 shell"，但 SSH 远端执行**天然是远端 shell 语义**
//
// core.ExecSpec 的约定是"name 直接是可执行文件，不经 shell 解释"（陷阱 7 精神）——
// local / sandbox 都严守这条（exec.CommandContext 直接起可执行文件）。但 SSH 的执行模型
// **绕不开这一点**：`ssh host <command>` 把 <command> 作为**一个字符串**交给远端的
// login shell 解释执行。也就是说，远端一定有一个 shell 在解析我们发过去的命令行。
//
// 因此本包的 Run **选择前者**（positively quoted）来弥合这个差：把 ExecSpec.Name/Args
// 逐个做 **POSIX 单引号安全引用**（shQuote：内部单引号转义成 '\”），拼成**一条**远端
// 命令行，作为**单个参数**传给系统 ssh。效果：
//
//   - 本地侧无 shell（exec.CommandContext 起 ssh，参数数组传递）；
//   - 远端侧虽有 shell，但 Name/Args 里的空格/引号/`;`/`|`/`$`/反引号等元字符**全部被
//     单引号包住**，远端 shell 一律当字面量——不会被注入成额外命令（env_test.go 有钉子：
//     含 `'; touch PWNED` 的 argv 不得被远端 shell 拆成多条命令）。
//
// Dir / Env 的处理（最小可用，别过度）：
//
//   - spec.Dir 非空 → 远端命令行加 `cd <dir> &&` 前缀（shQuote 引用目录）；
//   - spec.Env 非空 → 加 `env -i 'K=V' … ['PATH=…']` 前缀（**整份替换**，见下）。
//
// # Env 是**整份替换**（不是叠加）：`env -i`
//
// core.ExecSpec 的约定是"Env 非空 = **整份替换**"（local：cmd.Env = spec.Env，
// 子进程环境**只有**这些键）。远端要兑现这条，必须用 `env -i`：
//
//   - `env 'K=V' cmd`（**不带 -i**）是**叠加**：cmd 会**继承远端 login shell 的环境**，
//     远端机器上的凭据（~/.profile 里的 *_KEY / *_TOKEN、CI 注入的 token…）会漏进子进程。
//     core.ScrubEnv 的净化是**本地**做的，在远端不生效 → 净化形同虚设。
//   - `env -i 'K=V' cmd` 才是整份替换：子进程环境 = 本次显式列出的键（+ 一个显式 PATH）。
//     远端 shell 继承来的东西一律不可见（env_test.go 有钉子：远端环境里的 SECRET
//     在远端命令里看不到）。
//
// PATH 显式保留：`env -i` 连 PATH 一起清空 → 远端找不到 `git`/`make` 这类命令。
// 远端**真实的**默认 PATH 我们看不见（不读远端 /etc/environment、也不读远端 shell 的
// 环境——那正是要隔离的东西），所以给一个写死的常量 `/usr/bin:/bin`
// （defaultRemotePATH），可经 **Config.DefaultRemotePATH** 覆盖。
//
// ⚠ **调用方在 spec.Env 里给的 PATH 一律被忽略**（v4-2-A 复查 F1）：那通常是
// **harness 本机**的 PATH（Windows 上还是 `C:\…;C:\…` 形态）——它是**本机视角**，
// 搬给 POSIX 远端就是错的：远端按这个 PATH 找命令，一个都找不到（exit 127）。
// 同理，"机器身份"键一律**剔除**——harness 的身份不是远端的身份。键清单是
// **Windows + POSIX 两套**（POSIX：HOME / USER / LOGNAME / TERM / SHELL / LANG /
// LC_ALL；Windows：USERNAME / USERPROFILE / HOMEDRIVE / HOMEPATH / COMPUTERNAME /
// USERDOMAIN / LOGONSERVER / SESSIONNAME——**原先端 Windows 那套一个都没在表里**，
// 而 harness 的主开发平台就是 Windows，漏掉的键会原样发往远端：身份冒充 +
// 账号/主机名/域名泄漏）。
//
// ⚠ **`~` 不会展开**（单引号安全引用）：Dir / Name / Args 全部经 shQuote 包进单引号，
// 远端 shell 把 `~` 当**字面量**（不展开成 home）。远端路径请用**绝对路径**。
//
// # 产品面口径（`alasol -remote`）：本刀只给两个 flag
//
// **如实写明**（v4-2-A 终查 F3）——产品装配面（cmd/alasol 的 `-remote`）本刀**只给**
// `-remote`（启用 RemoteExecutionEnv）与 `-remote-host`（ssh 目标）**两个 flag**：
//
//   - **远端 PATH 固定 `/usr/bin:/bin`**（defaultRemotePATH）。要别的 PATH 得经**包内**
//     `Config.DefaultRemotePATH` 覆盖——**产品面当前没有暴露这个入口**（要暴露是
//     下一刀的事）。macOS / homebrew 的机器尤其要注意：`/usr/bin:/bin` 里**没有**
//     `/opt/homebrew/bin`（`brew` 装的工具在那儿）→ 远端会 127，**需要覆盖**成含
//     `/opt/homebrew/bin` 的值（例如 `/opt/homebrew/bin:/usr/bin:/bin`）。
//   - **ssh 选项走 `~/.ssh/config`**（或系统 ssh 的默认行为）：本包的 Config.SSHArgs
//     在产品面上**同样没有暴露**；端口/跳板机/IdentityFile 等一律在 `~/.ssh/config`
//     里写（本包只负责按目标名调系统 ssh，不读不改那个文件）。
//
// 换句话说：**产品面目前是最小可用**——"给个 host、把命令送过去"。PATH 与 ssh 选项
// 这两个旋钮在本包里可配、在产品面上不可配，这是**已知边界**（不是待修的 bug）。
//
// # ⚠ 远端命令的**可执行名/路径是远端视角**
//
// 本包只把 Name/Args 安全引用后交给远端 shell —— **不**做"本机 → 远端"的可执行名
// 翻译。调用方给的必须是**远端**上真实存在的命令（如 `ls` / `sh` / `bash`）。
// 特别地：**harness 平台的 shell 名不适用**（Windows 上的 `cmd /c` 到 POSIX 远端
// 必然 127）——远端命令要跑 shell，就显式写远端视角的 `sh -c …` / `bash -c …`。
// （runtime/tools 的 bash 工具目前的 shell 名是**平台常量**，跨平台语义是另一刀的事，
// 本包不代偿、不静默改写。）
//
// spec.Env **为空**时不加 env 前缀 = 继承远端 login shell 的环境（与 local 的
// "Env 空 = 继承父 env"同形，但父环境是**远端**的，不是本地的）。
//
// # ⚠ 语义差（逐条）：与 local / sandbox **主要对齐**，但**不是逐条相等**
//
// 主要对齐的（K8：换 env 不动调用方）：非零退出码不是 error（ExitCode 带上）、
// 空 Name 是 error、ctx 结束 → 杀掉本地 ssh 进程。
//
// 有差的（这一节就是差的口径，别拿 local 的直觉套过来）：
//
//  1. **Dir 的失败语义**：local 里"目录不存在/进不去"是**启动失败 = error**；
//     远端是 `cd <dir> && cmd` 的 shell 短路 → **退出码 1**（不是 error）。
//     调用方看到的形态不同（"失败的命令" vs "起不来的命令"）。
//  2. **Dir 的相对路径基准**：spec.Dir 是**相对路径**时，基准是**远端 login shell 的
//     home**（ssh 会话的 cwd），**不是**本机 cwd。
//  3. **Env 的整份替换 + PATH 固定为远端侧的**：见上（本地靠 cmd.Env 天然替换，
//     远端靠 env -i；PATH 一律用远端侧的写死值，调用方给的 harness PATH 一律忽略）。
//  4. **凭据 env / args 一律拒绝**：见下（H12/H19）。
//  5. **exit 255 的歧义**：ssh 自己用 255 表示"传输层失败"，远端命令退出 255 时 ssh
//     也透出 255 —— 两者在退出码上不可区分。本包按 **stderr 首行的形态**区分：命中
//     ssh **自身的强特征**（`ssh: ` 前缀 / 密钥交换被打断 / 带鉴权方法括号的
//     `Permission denied (` / 主机密钥不匹配 …）→ 返回 **ErrSSHTransport**
//     （errors.Is 可判）；没命中 → 按"远端命令退出 255"透出。**通用措辞**
//     （不带括号的 "Permission denied"、"Connection refused"）**不**作判据——
//     远端命令自己也会打这些（`cat: /etc/shadow: Permission denied`），拿它当 ssh 的
//     特征就是把"远端跑挂了"误报成"没连上"。宁可漏判（按远端退出透出）。
//  6. **ctx 取消只收本地 ssh**：远端命令**可能仍在跑**（SSH 的已知语义——信号转发
//     与远端进程组由系统 ssh 决定，本包不深究）。本地这条 ssh 一定被收掉。
//
// # 诚实边界：远端路径**不传凭据**（H12/H19）
//
// spec.Env **与 spec.Args** 的值都会被拼进**远端命令字符串**，而那条字符串是
// **ssh 的 argv**——本地与远端的进程表（ps / /proc/<pid>/cmdline）都看得见。
// 凭据一旦落进 argv，就等于"在两台机器的进程列表里公开"。
//
// 故本包按**与 core.ScrubEnv 同口径**的判定（core.IsCredentialKey /
// core.IsCredentialValue）：spec.Env 里出现凭据形态的键值、**或 spec.Args 里有
// 凭据形态的值** → **拒绝执行**，返回 **ErrCredentialEnv**（fail-close，连 ssh 都不起）。
// 错误里只报键名 / 第几个参数，**不报值**。
//
// 诚实边界之二：**ssh 的默认选项**。本包每次调用都带
// `-o BatchMode=yes -o ConnectTimeout=10`（远端若要求交互式口令/确认，ssh 会
// **挂在等输入**上——自动化执行里那是一段无声的无限等待；BatchMode 让它立刻失败）。
// 调用方在 Config.SSHArgs 里显式给了同名选项时以调用方为准。
//
// 诚实边界之三：**凭据扫描只认两种形状**（v4-2-A 终查 F4，本轮**不修**，如实挂账）。
// 判定口径是 core.IsCredentialValue：**整串**命中凭据形态（sk- / ghp_ / JWT），
// 或 "K=V" 形态里 **'=' 之后那半**命中。**嵌在长串中间**的凭据形态（例如
// `Authorization: Bearer sk-…`、`--header "X-Token: ghp_…"` 这类把凭据塞在一段
// 文本/JSON/URL 里的写法）**不保证**被认出来 → 可能照常发往远端。收紧它需要一套
// "子串级"的凭据形态识别（更宽的判定面 = 更高的误拒率），**记 v4**，本刀不做。
//
// 诚实边界之四：**exit 255 的日志会记 stderr 前 200 字符**（v4-2-A 终查 F6，
// 本轮**不修**，如实挂账）。当 ssh 退出 255 而 stderr 首行**没有** ssh 特征时，
// 本包按"远端命令自己退出 255"透出，但会打一条 slog 告警，**带 stderr 片段**
// （errSnippet：压成一行、限长 200 字符）。stderr 里若混进了敏感内容，它会**落进
// 日志**——这是"可观测性 vs 不泄漏"的取舍，本刀选了可观测性。收紧它需要"日志
// 侧再过一遍凭据扫描"（即 F4 的子串级识别），**记 v4**，本刀不做。
//
// # 鉴权交系统/SSH 自管（07 N4b/N5/K19 拍板，2026-10-31）
//
// Env **不收任何凭据**（H18：不落凭据）：ssh 密钥/agent/~/.ssh/config/known_hosts 全部
// 由**系统 ssh** 管。Config 里没有密码/私钥/token 字段——只有一个 ssh 目标
// （user@host 或 ~/.ssh/config 别名）与可选的额外 ssh 选项（如 -p 端口）。我们只做 SSH
// 适配层、**不做自研鉴权**（N5 的 token scope/降权视图体系不做）。
//
// # 预留位：Fs / Proc / Net 显式 ErrUnsupported
//
// v4-2-A 只落地 **Exec 经 SSH**。其余三个能力面（FSCapability / ProcCapability /
// NetCapability）返回**显式 ErrUnsupported**——这是诚实的"预留位"，留后续：
//
//   - 不假装支持（返回空成功比报错更危险——调用方会以为远端文件被读到了）；
//   - 也不静默直通本地（远端 ssh 会话里"本地 http"语义混乱，且违背"换 env 不动逻辑"的
//     可预期性）。要挂时在各自方法里去掉 ErrUnsupported、经 ssh 实现即可。
package remote
