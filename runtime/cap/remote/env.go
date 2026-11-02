package remote

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Yorha9e/alasol/core"
)

// ErrUnsupported 是 remote env 对**仍处预留位**能力面（Proc / Net）的统一返回。
//
// 诚实报"未支持"，不假装成功、不静默直通本地——调用方（Proc 管理、出站网络）
// 会把这个错如实呈现给上层。要挂后续时在各自方法里实现即可。
//
// Fs **不再**走它：ReadFile/WriteFile/Stat/ListDir 已经 ssh exec 通道落地
// （最小可用档，见 fs.go 与包 doc 的"能力面落地状态"一节）。
var ErrUnsupported = errors.New("remote: 该能力面为预留位，尚未实现（Proc/Net 留后续；Exec 与 Fs 已经 ssh exec 落地）")

// ErrCredentialEnv 是"远端路径不传凭据"的哨兵错误（H12/H19）。
//
// 为什么远端路径连凭据都不收：spec.Env 的值**与 spec.Args 的值**都会被拼进
// **远端命令字符串**，而那条字符串是 **ssh 的 argv**——本地与远端的进程表
// （ps / /proc/<pid>/cmdline）都能看到。凭据一旦落进 argv，就等于"在两台机器的
// 进程列表里公开"。core/loop/env.go 的口径同样是"required_env 注入值一律算秘密"，
// 所以这里按**同一口径**（core.IsCredentialKey / core.IsCredentialValue）判定：
// 命中即**拒绝执行**（fail-close），而不是"照传不误"。
//
// 判定口径与 core.ScrubEnv 一致：键名命中凭据 deny 名单（*_KEY / *_TOKEN / *_SECRET /
// *_PASSWORD / *_CREDENTIAL*）**或**值命中凭据形态（sk- / ghp_ / JWT）。
//
// 错误里**只报键名 / 第几个参数，不报值**（值是秘密；连错误文案都不该带它）。
var ErrCredentialEnv = errors.New("remote: 远端路径不传凭据（H12/H19）：env 与 args 的值会落进 ssh argv（两端进程表可见）")

// Env 是远端 SSH 执行环境：RemoteExecutionEnv（K8 第三档）。
//
// 它满足 core 的**四个**能力接口——和 runtime/cap/local、runtime/cap/sandbox 一样，
// 所以 host 装配时可以一对一替换（K8 酸测试：换 env 不动 agent 逻辑）：
//
//	env := remote.New(remote.Config{Host: "user@harness"})
//	tools.NewRead(env); tools.NewWrite(env); tools.NewBash(env) // 工具代码零改动
//
// 但只有 **Exec** 与 **Fs** 真的经 SSH 落地（Fs 是最小可用档：cat/stat/ls，见 fs.go）；
// Proc / Net 显式 ErrUnsupported（预留位，见 doc）。
//
// 并发安全：Env 自身只有只读字段；每次执行经注入的执行缝起一条独立的 ssh 进程，
// 互不共享可变状态——多个 Run/Fs 调用可并发跑（真实并发度由远端与 ssh 决定，本包不额外限流）。
type Env struct {
	host    string
	sshArgs []string
	// remotePATH 是给远端命令的 PATH（`env -i` 之后远端唯一的 PATH 来源）。
	// 默认 defaultRemotePATH；可经 Config.DefaultRemotePATH 覆盖。
	remotePATH string
	// exec 是 Env **唯一的内部执行缝**（带 stdin 的宽面，见 ExecStdinFunc）。
	// Config.Exec（窄面）与 Config.ExecStdin（宽面）在 New 里接成这一个缝——
	// Run/ReadFile/Stat/ListDir 以空 stdin 走它，WriteFile 以文件内容走它。
	exec ExecStdinFunc
}

// 编译期断言：Env 必须满足 core 的四个能力接口（K8：换实现不动调用方）。
var (
	_ core.ExecCapability = (*Env)(nil)
	_ core.FSCapability   = (*Env)(nil)
	_ core.NetCapability  = (*Env)(nil)
	_ core.ProcCapability = (*Env)(nil)
)

// Config 是 RemoteExecutionEnv 的装配参数。
type Config struct {
	// Host 是 ssh 目标（user@host，或 ~/.ssh/config 里的别名）。必填——
	// 没有目标就不知道连哪台机器（fail-close：不返回"能跑却连空"的对象）。
	Host string
	// SSHArgs 是额外的 ssh 选项（如 "-p", "2222"；**不含** host 与远端命令）。
	//
	// 只放**连接参数**，不放凭据（H18）：密钥/agent/config 由系统 ssh 管，
	// 本结构没有任何密码/私钥/token 字段。
	//
	// 本包会**追加**两个默认选项（`-o BatchMode=yes` / `-o ConnectTimeout=10`，
	// 见 mergeSSHOptions）：远端若要求交互式口令，ssh 会挂在等输入上——自动化执行里
	// 那是一段无声的无限等待。若这里已经显式给了同名选项
	// （`-o BatchMode=no` / `-o ConnectTimeout=30`），**以这里为准**（不加默认值）。
	SSHArgs []string
	// DefaultRemotePATH 是**远端命令**的 PATH（`env -i` 清空环境后，远端唯一的
	// PATH 来源就是它）。空 = 用包内默认 defaultRemotePATH（"/usr/bin:/bin"）。
	//
	// 为什么不读远端自己的 PATH：看不见（不读远端 /etc/environment、不读远端 login
	// shell 的环境——那正是要隔离的东西），只能给一个覆盖绝大多数 POSIX 机器的写死值。
	//
	// ⚠ **调用方在 spec.Env 里给的 PATH 一律被忽略**（v4-2-A 复查 F1）：那通常是
	// harness **本机**的 PATH（Windows 上是 `C:\…;C:\…` 形态）——搬给 POSIX 远端，
	// 远端按这个 PATH 找命令一个都找不到（exit 127）。要别的 PATH 就配这里。
	//
	// ⚠ **产品面（alasol -remote）当前没有暴露这个入口**（v4-2-A 终查 F3）：
	// 产品装配只给 `-remote` / `-remote-host` 两个 flag，远端 PATH 固定
	// defaultRemotePATH。macOS/homebrew 的机器需要覆盖成含 `/opt/homebrew/bin` 的值
	// （否则远端 brew 装的命令 127）。见包 doc 的"产品面口径"一节。
	DefaultRemotePATH string
	// Exec 是**无 stdin** 的执行注入点（nil = 生产默认 os/exec 起系统 ssh；测试传假 ssh）。
	// 见 ExecFunc 的说明——这是"不真连 SSH 也能测命令行拼接"的关键。
	//
	// WriteFile 的文件内容走 **stdin 管道**，窄面送不了 → 需要 Config.ExecStdin（宽面）。
	// 两者只给一个时另一个从它退化而来（见 New 的接线口径；只给本项时 WriteFile 对
	// **非空 data** fail-close 报错，不静默丢字节）。同时给时**以 ExecStdin 为准**
	// （它是本项的超集）。
	Exec ExecFunc
	// ExecStdin 是**带 stdin** 的执行注入点（nil = 生产默认 os/exec 起系统 ssh 并接
	// stdin 管道；测试传假 ssh）。见 ExecStdinFunc 的说明。
	//
	// 为什么单独一个口：WriteFile 必须把文件内容经 **ssh 的 stdin 管道**送达——
	// 内容不进 argv（argv 落两端进程表，见 ErrCredentialEnv 的纪律）、二进制安全。
	ExecStdin ExecStdinFunc
}

// New 构造远端 SSH 执行环境。Host 必填（空 → 报错，fail-close）。
//
// Host 还有两条 fail-close 校验：
//
//   - **不以 '-' 开头**：ssh 的参数解析器会把以 '-' 开头的目标当成**选项**
//     （`-oProxyCommand=…` 之类 = 往 ssh 里注入我们没打算给的能力）。宁可拒绝。
//   - **不含空白**：ssh 目标只能是 user@host 或 ~/.ssh/config 别名；带空白说明
//     调用方把别的东西拼进来了（多个目标？一段命令行？）→ 拒绝，不猜。
func New(cfg Config) (*Env, error) {
	host := strings.TrimSpace(cfg.Host)
	if host == "" {
		return nil, fmt.Errorf("remote: Config.Host 不能为空（ssh 目标 user@host 或 ~/.ssh/config 别名）")
	}
	if strings.HasPrefix(host, "-") {
		return nil, fmt.Errorf("remote: Config.Host 不能以 '-' 开头（会被 ssh 当成选项注入）")
	}
	if strings.ContainsAny(host, " \t\r\n") {
		return nil, fmt.Errorf("remote: Config.Host 不能含空白（ssh 目标只能是 user@host 或 ~/.ssh/config 别名）")
	}
	// 执行缝接线（两个配置口 → 一个内部缝，Env.exec）：
	//   - 都不给 → 生产 osExec（os/exec 起系统 ssh，带 stdin 管道）；
	//   - 只给 Exec（窄面）→ 宽面由它适配（stdin 非空 fail-close，见 noStdinExec）；
	//   - 只给 ExecStdin（宽面）→ 它就是全部（窄面操作以空 stdin 走它）；
	//   - 都给 → 以 ExecStdin 为准（它是 Exec 的超集）。
	exec := cfg.ExecStdin
	if exec == nil {
		if cfg.Exec != nil {
			exec = noStdinExec(cfg.Exec)
		} else {
			exec = osExec // 生产默认：os/exec 起系统 ssh
		}
	}
	remotePATH := strings.TrimSpace(cfg.DefaultRemotePATH)
	if remotePATH == "" {
		remotePATH = defaultRemotePATH
	}
	return &Env{
		host:       host,
		sshArgs:    append([]string(nil), cfg.SSHArgs...),
		remotePATH: remotePATH,
		exec:       exec,
	}, nil
}

// Host 返回 ssh 目标（诊断/埋点用）。
func (e *Env) Host() string { return e.host }

// buildRemoteCommand 是 Env 侧的拼装入口：用**本 Env 配置的**远端 PATH
// （Config.DefaultRemotePATH；未配置 = 包内默认 defaultRemotePATH）拼远端命令。
//
// 与包级 buildRemoteCommand 的关系：包级那个是"纯函数"（PATH 由调用方给），
// 这个方法只是把 Env 自己的配置接上去——同一套拼装逻辑，不分叉。
func (e *Env) buildRemoteCommand(name string, spec core.ExecSpec) (string, error) {
	return buildRemoteCommand(name, spec, e.remotePATH)
}

// Run implements core.ExecCapability（远端 SSH 版）。
//
// 语义与 local.Exec.Run / sandbox.Env.Run **主要对齐**（K8：换 env 不动调用方）——
// 主要，不是逐条相等：**共 6 条有差，逐条列在包 doc 的"语义差（逐条）"一节**
// （Dir 的失败语义 exit1 vs 启动失败、Dir 的相对路径基准=远端 home、Env 的整份替换
// 与 PATH 固定为远端侧、凭据 env/args 一律拒绝、exit 255 的歧义、ctx 取消只收本地 ssh）。
//
// 主要对齐的条目：
//
//   - ctx 结束 → 杀掉本地 ssh 进程（Cancel 回调）；
//   - 非零退出码**不是 error**（ExecResult.ExitCode 带上——含"远端命令非零"经 ssh 透出）；
//   - spec.Name 空 → error（不猜可执行文件）。
//
// 与 local/sandbox 的**唯一实质差**：远端经 SSH 执行，天然是远端 shell 语义——
// 本方法把 Name/Args 做 POSIX 安全引用拼成一条远端命令（见 doc 的"语义差"一节与
// shQuote）。本地侧则严守"不走 shell"（起的是 ssh 可执行文件，参数数组传递）。
func (e *Env) Run(ctx context.Context, spec core.ExecSpec) (core.ExecResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return core.ExecResult{}, fmt.Errorf("remote: 上下文已结束（%w）", err)
	}
	name := strings.TrimSpace(spec.Name)
	if name == "" {
		return core.ExecResult{}, fmt.Errorf("remote: ExecSpec.Name 不能为空（不猜可执行文件）")
	}
	// 凭据一律拒绝（H12/H19）：env 的值**与 args 的值**都会进 ssh argv（两端进程表可见）。
	// fail-close 在**起 ssh 之前**——连一次连接都不做，更不会有值落到 argv 上。
	if err := checkNoCredentialEnv(spec.Env, spec.Args); err != nil {
		return core.ExecResult{}, err
	}

	// 拼远端命令（POSIX 安全引用），作为**单个参数**交给系统 ssh——ssh 会把它
	// 原样送到远端的 login shell。本地这一侧：sshArgs + host + remoteCmd 全是数组元素，
	// 不经本地 shell。
	remoteCmd, err := e.buildRemoteCommand(name, spec)
	if err != nil {
		return core.ExecResult{}, err
	}
	return e.sshRun(ctx, remoteCmd, nil)
}

// sshRun 是"把一条**已拼好的远端命令行**交给系统 ssh"的执行下半段——
// Run 与 Fs（fs.go）**共用**它，拼装口径不分叉：
//
//	ssh argv = mergeSSHOptions(sshArgs)… + host + remoteCmd（远端命令是单个参数）
//
// → 注入的执行缝（stdin 非空走宽面，如 WriteFile 的内容；空走同一缝的无 stdin 路径）
//   - → classifySSHExit：exit 255 的歧义在此收口（见 ErrSSHTransport）。
//
// 为什么单独抽出来：Fs 的四条命令（cat/stat/ls 与 `cat > path`）远端命令行形状
// 与 Exec 不同（WriteFile 还要灌 stdin），但"怎么起 ssh、怎么判传输失败"必须与
// Run 逐字节同款——K8 的"换 env 语义可预期"在这里同样成立。
func (e *Env) sshRun(ctx context.Context, remoteCmd string, stdin []byte) (core.ExecResult, error) {
	args := make([]string, 0, len(e.sshArgs)+len(defaultSSHOptionValues)*2+2)
	args = append(args, mergeSSHOptions(e.sshArgs)...)
	args = append(args, e.host, remoteCmd)
	res, err := e.exec(ctx, sshBin, args, stdin)
	if err != nil {
		return res, err
	}
	// exit 255 的歧义（ssh 传输失败 vs 远端命令退出 255）在**这里**也收口一次：
	// 生产路径 osExec 已经判过（err != nil 直接透出），这里是给**注入的 exec**
	// 同一口径（K8 酸测试/替代实现不该看到不同的语义）。
	if cerr := classifySSHExit(res); cerr != nil {
		return res, cerr
	}
	return res, nil
}

// checkNoCredentialEnv 检查 spec.Env（以及可选的 spec.Args）里有没有凭据形态（H12/H19）。
//
// 判定口径**与 core.ScrubEnv 同源**（core.IsCredentialKey / core.IsCredentialValue）：
//   - env：键名命中凭据 deny 名单，或值命中凭据形态（sk- / ghp_ / JWT）→ 命中；
//   - args：**同口径**扫每个参数（core.IsCredentialValue；"K=V" 形态的参数连值那半
//     也扫）——参数同样会落进 ssh argv，凭据形态的参数值同样等于公开（v4-2-A 复查 F5）。
//
// 命中即**拒绝执行**（fail-close）：远端路径把 env/args 的值拼进 ssh argv，凭据会落进
// 本地与远端的进程表。这是"远端路径不传凭据"的边界（见 ErrCredentialEnv）。
//
// 错误里**只报键名 / 第几个参数，绝不报值**（值是秘密）。不含 '=' 的畸形条目不在这里判
// （由 buildRemoteCommand 点名报错——那是另一种错：不是凭据，是拼不出来的 env）。
func checkNoCredentialEnv(env []string, args ...[]string) error {
	for _, kv := range env {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			continue // 畸形条目 → buildRemoteCommand 报错（点名）
		}
		if core.IsCredentialKey(k) || core.IsCredentialValue(v) {
			return fmt.Errorf("%w：键 %q 命中凭据口径（core.IsCredentialKey/IsCredentialValue）", ErrCredentialEnv, k)
		}
	}
	for _, group := range args {
		for i, a := range group {
			if isCredentialArg(a) {
				// 只报"第几个参数"，不报值（值是秘密）。
				return fmt.Errorf("%w：第 %d 个参数命中凭据值形态（core.IsCredentialValue；值不报）",
					ErrCredentialEnv, i+1)
			}
		}
	}
	return nil
}

// isCredentialArg 判定一个**命令参数**是不是凭据形态（core.IsCredentialValue 同口径）。
//
// "K=V" 形态的参数连**值那半**也扫一遍（"--token=sk-…" 这种藏在参数里的凭据）。
func isCredentialArg(a string) bool {
	if core.IsCredentialValue(a) {
		return true
	}
	if _, v, ok := strings.Cut(a, "="); ok {
		return core.IsCredentialValue(v)
	}
	return false
}

// ---------------------------------------------------------------------------
// 能力面：Fs 见 fs.go（经 ssh exec 落地）；Proc / Net 仍为预留位（见 doc）
// ---------------------------------------------------------------------------

// Do implements core.NetCapability（预留位：ErrUnsupported）。
//
// 不直通本地 http：远端 ssh 会话里"本地出站网络"语义混乱，且违背"换 env 可预期"——
// 诚实报未支持，要挂时经 ssh 实现。
func (e *Env) Do(context.Context, *http.Request) (*http.Response, error) {
	return nil, ErrUnsupported
}

// Start implements core.ProcCapability（预留位：ErrUnsupported）。
func (e *Env) Start(context.Context, core.ExecSpec) (core.ProcHandle, error) {
	return nil, ErrUnsupported
}
