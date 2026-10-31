package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/core"
)

// sshBin 是系统 ssh 可执行文件名（经 PATH 查找，交给系统 ssh 管鉴权/配置/known_hosts）。
const sshBin = "ssh"

// maxCapturedOutput 是单次执行 stdout/stderr 各自的捕获上限（32MB）。
// 与 local / sandbox 同值：只防"命令无限输出把内存吃光"；截断是静默的，
// 调用方若要告知上游输出被截断，在工具层截断并标注（runtime/tools 就是这么做的）。
const maxCapturedOutput = 32 << 20

// waitDelay 是 Wait 等 I/O 管道关闭的硬上限（5s）。同 local：ctx 取消时
// CommandContext 只杀直接子进程，管道写端可能还攥在孙子进程手里 → Wait 挂住。
// 这里给一道硬上限：最多再等 5 秒就放弃管道（返回 exec.ErrWaitDelay），
// 绝不无限挂起（我们照常报 ctx 错误）。
const waitDelay = 5 * time.Second

// defaultRemotePATH 是 `env -i`（整份替换）下给远端命令的**默认 PATH**（写死，不问远端）。
//
// 为什么必须显式给：`env -i` 把环境清空后 PATH 也没了——远端 shell 找不到
// `git`/`make` 这类命令（"整份替换"最经典的副作用）。而远端**真实的**默认 PATH
// 我们看不见（不读远端 /etc/environment、不读远端 shell 的环境——那正是我们要隔离的东西），
// 所以只能给一个覆盖绝大多数 POSIX 机器的写死常量。要别的 PATH 经
// **Config.DefaultRemotePATH** 覆盖（见 Config 的说明）。
//
// ⚠ **调用方在 spec.Env 里给的 PATH 一律被忽略**（v4-2-A 复查 F1）：
// 那通常是 **harness 本机**的 PATH（Windows 上还是 `C:\…;C:\…` 形态）——它是**本机视角**，
// 搬给 POSIX 远端就是错的：远端按这个 PATH 找命令，一个都找不到（exit 127）。
// 故 PATH 键**总是被替换**成本常量（位置不变、值换掉），见 buildRemoteCommand。
const defaultRemotePATH = "/usr/bin:/bin"

// remoteIdentityEnvKeys 是"机器身份"类的环境键：它们的值描述的是**本机（harness）**，
// 搬给远端就是**错的身份**（远端看到的 HOME/USER/TERM 是 harness 的，不是远端的）。
//
// 处置口径（v4-2-A 复查 F1）：
//   - PATH → **总是替换**成 defaultRemotePATH（见上）；
//   - 其余 → **剔除**（`env -i` 本来也会清掉它们；显式剔除是为了不把 harness 的值
//     当成远端的值送过去）。
//
// # 键清单：**Windows + POSIX 两套**（v4-2-A 终查 F1）
//
//   - POSIX：HOME / USER / LOGNAME / TERM / SHELL / LANG / LC_ALL；
//   - Windows：USERNAME / USERPROFILE / HOMEDRIVE / HOMEPATH / COMPUTERNAME /
//     USERDOMAIN / LOGONSERVER / SESSIONNAME。
//
// Windows 那套**原先一个都不在表里**——而 harness 的主开发平台就是 Windows：
// 漏掉的键会**原样发往远端**（身份冒充 + 账号名/主机名/域名的泄漏）。
// 两套都要在：远端是 POSIX 还是 Windows 由远端自己决定，harness 是哪台平台不该
// 影响"哪些键描述的是本机身份"。
//
// 键名比较**大小写不敏感**（见 isRemoteIdentityKey）：Windows 上 os.Environ() 给的
// 是 "Path" 而不是 "PATH"，大小写敏感会整个漏掉。
var remoteIdentityEnvKeys = map[string]bool{
	// POSIX 身份键
	"HOME":    true,
	"USER":    true,
	"LOGNAME": true,
	"TERM":    true,
	"SHELL":   true,
	"LANG":    true,
	"LC_ALL":  true,
	// Windows 身份键（v4-2-A 终查 F1 补齐）
	"USERNAME":     true,
	"USERPROFILE":  true,
	"HOMEDRIVE":    true,
	"HOMEPATH":     true,
	"COMPUTERNAME": true,
	"USERDOMAIN":   true,
	"LOGONSERVER":  true,
	"SESSIONNAME":  true,
}

// isRemoteIdentityKey 判定一个 env 键是不是"机器身份"键（大小写不敏感）。
func isRemoteIdentityKey(k string) bool {
	return remoteIdentityEnvKeys[strings.ToUpper(strings.TrimSpace(k))]
}

// defaultSSHOptionValues 是每次 ssh 调用都带的默认选项（值形态 "Key=Value"，传给 -o）。
//
//   - BatchMode=yes：**远端若要求交互式口令/确认，ssh 会挂在等输入上**——自动化执行里
//     那是一段无声的无限等待。BatchMode 让它**立刻失败**（看得见的错，而不是挂起）。
//   - ConnectTimeout=10：连接建立阶段的上限（**不含**认证之后的命令执行时长）。
//
// 覆盖口径：Config.SSHArgs 里若已出现同名选项（`-o BatchMode=…` / `-o ConnectTimeout=…`），
// 则**不加**我们的默认值——调用方显式给的值优先（见 mergeSSHOptions）。
var defaultSSHOptionValues = []string{"BatchMode=yes", "ConnectTimeout=10"}

// ErrSSHTransport 表示 **ssh 传输层自身**失败（连接被拒 / 鉴权失败 / 主机密钥不匹配 …）。
//
// 为什么要有这个哨兵：ssh 用 **exit 255** 表示"传输层失败"，而**远端命令自己退出 255**
// 时 ssh 也透出 255——两者在退出码上不可区分。只看退出码会把"根本没连上"误报成
// "远端命令跑挂了"。故 255 且 stderr 的**首行**命中 ssh 自身的强特征时，返回本哨兵
// （errors.Is 可判），调用方能一眼分清"没连上"与"远端跑挂了"。
//
// ⚠ **判据是保守的**（v4-2-A 复查 F2）：ssh 把远端命令的 stderr 与自己的错误汇进
// 同一条流，无法区分；通用措辞（"Permission denied" / "Connection refused"）远端命令
// 自己也会打 → 不再作为 ssh 特征。**宁可漏判成"远端命令退出 255"**，也不误报成"没连上"。
var ErrSSHTransport = errors.New("remote: ssh 传输层失败（连接/鉴权/主机密钥，不是远端命令的退出码）")

// log 是本包的告警出口（ssh 退出码歧义等）。用 slog，可被宿主接管。
var log = slog.Default()

// ExecFunc 抽象"起一条本地命令并同步等待"——这是本包**唯一的执行注入点**。
//
// 为什么抽它：remote env 只起**一种**本地命令（系统 ssh），但测试必须能在**不真连 SSH**
// 的前提下验证"ssh 命令行怎么拼"。抽成函数面后：
//
//	生产 = osExec（os/exec 起系统 ssh，不经本地 shell）
//	测试 = 假 ssh（记录 argv、返回罐头结果，或把远端命令交给本机 shell 跑）
//
// 与 sandbox 用 Backend **接口**注入的理由不同：这里只有一个方法、无需多态一族，
// 一个函数面更轻；sandbox 那边是"一整族进程/文件约束"需要接口多态。
type ExecFunc func(ctx context.Context, name string, args []string) (core.ExecResult, error)

// buildRemoteCommand 把 ExecSpec 拼成**一条**远端 shell 命令行（POSIX 安全引用）。
//
// 顺序（从外到内）：`cd <dir> && env -i [PATH=…] 'K=V' … <name> <args...>`
//   - Dir 前缀用 `cd … &&`（远端切工作目录；doc 已写明这是便利层 + 它的两条语义差）；
//   - Env 前缀用 `env -i`（**整份替换**：清空远端 shell 继承来的环境，只留本次
//     显式列出的键 + 一个显式 PATH——不加 -i 的话 `env K=V cmd` 是**叠加**，
//     远端 shell 的环境（含远端机器上的凭据）会漏进子进程，core.ScrubEnv 的净化
//     在远端就失效了；见包 doc 的"整份替换"一节）；
//   - name/args 逐个 shQuote 后空格连接——这是防远端 shell 注入的关键。
//
// # PATH：**总是**远端侧的（v4-2-A 复查 F1）
//
// spec.Env 里调用方给的 PATH 一律**忽略**（那是 harness 本机的 PATH，Windows 上还是
// `C:\…;C:\…` 形态——搬到 POSIX 远端一个命令都找不到，exit 127）：
// PATH 键**原地替换**成 defaultRemotePATH（位置不变、值换掉）。
// 其余"机器身份"键（**Windows + POSIX 两套**，见 remoteIdentityEnvKeys）**剔除**——
// harness 的身份不是远端的身份（见 remoteIdentityEnvKeys）。
//
// ⚠ **~ 不会展开**（单引号安全引用）：Dir/Name/Args 全部经 shQuote 包进单引号，
// 远端 shell 把 `~` 当**字面量**（不展开成 home）。远端路径请用**绝对路径**。
//
// remotePath 是**可选**的远端 PATH（省略 = defaultRemotePATH；给了就以它为准）——
// Config.DefaultRemotePATH 经 Env 传进来（见 Config 的说明）。
//
// 返回 error 的情形（**fail-close**）：
//   - spec.Env 里有**不含 '=' 的条目**——`env` 会把这种参数当成**命令名**
//     （GNU env: "env: 'FOO': No such file or directory"），拼出来的远端命令行
//     语义就不是调用方要的了 → 点名报错（F6）；
//   - spec.Env 里有**键名为空**的条目（"=v"）——`env` 不接受空键名 → 点名报错（F6 补）。
func buildRemoteCommand(name string, spec core.ExecSpec, remotePath ...string) (string, error) {
	path := defaultRemotePATH
	if len(remotePath) > 0 {
		if p := strings.TrimSpace(remotePath[0]); p != "" {
			path = p
		}
	}
	tokens := make([]string, 0, len(spec.Args)+1)
	tokens = append(tokens, shQuote(name))
	for _, a := range spec.Args {
		tokens = append(tokens, shQuote(a))
	}
	line := strings.Join(tokens, " ")

	if len(spec.Env) > 0 {
		envToks := make([]string, 0, len(spec.Env)+3)
		envToks = append(envToks, "env", "-i")
		hasPath := false
		for _, kv := range spec.Env {
			k, _, ok := strings.Cut(kv, "=")
			if !ok {
				return "", fmt.Errorf("remote: ExecSpec.Env 条目 %q 缺少 '='（env 会把它当命令名）", kv)
			}
			if strings.TrimSpace(k) == "" {
				return "", fmt.Errorf("remote: ExecSpec.Env 条目 %q 的键名为空（env 不接受空键名）", kv)
			}
			if strings.EqualFold(k, "PATH") {
				// PATH 一律用**远端侧**的：调用方给的是 harness 本机的 PATH，
				// 对远端无意义（Windows 形态的 PATH 在 POSIX 远端上必然 127）。
				// 原地替换（位置不变、值换掉）——保持 env 顺序稳定可复现。
				envToks = append(envToks, shQuote("PATH="+path))
				hasPath = true
				continue
			}
			if isRemoteIdentityKey(k) {
				// 机器身份键（HOME/USER/TERM/…）：harness 的值对远端是错的 → 剔除
				//（env -i 本来也会清掉它们；显式剔除是不把本机身份冒充远端身份）。
				continue
			}
			// 整条 K=V 作为一个 token 引用（env 自己按第一个 = 拆键值）；
			// 值里的空格/元字符被单引号包住，不会被远端 shell 解释。
			envToks = append(envToks, shQuote(kv))
		}
		if !hasPath {
			// PATH 显式保留：env -i 清空环境后 PATH 也没了 → 远端找不到命令。
			envToks = append(envToks, shQuote("PATH="+path))
		}
		line = strings.Join(envToks, " ") + " " + line
	}

	if dir := strings.TrimSpace(spec.Dir); dir != "" {
		line = "cd " + shQuote(dir) + " && " + line
	}
	return line, nil
}

// shQuote 用 POSIX 单引号包裹一个字符串（把内部每个单引号转义成 '\”）。
//
// 这是防**远端 shell 注入**的核心：Name/Args 里的空格、引号、;、|、$、反引号等元字符
// 被包进单引号后，远端 shell 一律当字面量。唯一能"提前闭合包裹"的字符是单引号本身，
// 所以把它转义成 关闭-转义-重开 四元组 '\”——闭合后紧跟一个反斜杠转义的字面单引号，
// 再重开单引号，上下文永远是"引号内"。
//
// 例：shQuote("a'; rm -rf /") = 'a'\” ; rm -rf /'
//
//	shell 解析：'a' → a，\' → '，'; rm -rf /' → 字面 "; rm -rf /"
//	合起来就是单个 token：a'; rm -rf /（; 不是命令分隔符）
func shQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// mergeSSHOptions 把默认 ssh 选项与调用方给的 SSHArgs 合并（**调用方优先**）。
//
// 为什么不靠"参数顺序让后写的赢"：OpenSSH 的 -o 解析对不同选项口径不一致
// （有的选项 first-wins、有的 last-wins）——靠顺序赌不对。所以改成**在 Go 侧判定**：
// 调用方已经给了同名选项（BatchMode / ConnectTimeout）就不加我们的默认值，
// 两边永远不会出现"同一个选项给两遍"。
func mergeSSHOptions(user []string) []string {
	out := make([]string, 0, len(user)+2*len(defaultSSHOptionValues))
	out = append(out, user...)
	for _, v := range defaultSSHOptionValues {
		if hasSSHOption(user, v) {
			continue // 调用方显式给了 → 用调用方的
		}
		out = append(out, "-o", v)
	}
	return out
}

// hasSSHOption 判定调用方是否已经给了某个 -o 选项（形如 "BatchMode=yes"）。
// 只看选项名（= 之前那段），大小写不敏感（ssh 的选项名本身大小写不敏感）。
//
// **两种写法都认**（v4-2-A 复查 F3）：
//
//	-o BatchMode=no    （分离式：值是下一个 argv 元素）
//	-oBatchMode=no     （附着式：选项名与值挤在一个 argv 元素里）
//
// 附着式早先认不出来 → 调用方明明写了 `-oBatchMode=no`，我们还是**追加**了
// `-o BatchMode=yes`（同一个选项给两遍，OpenSSH 对 -o 的 first/last-wins 口径
// 还不一致 → 生效值不可预期）。
func hasSSHOption(args []string, want string) bool {
	name := want
	if i := strings.IndexByte(name, '='); i >= 0 {
		name = name[:i]
	}
	for _, a := range args {
		// 附着式 "-oKey=Value"：先剥掉 "-o" 前缀，再按 '=' 切出选项名。
		// （"-o" 单独出现时是分离式——值在下一个 argv 元素里，这里跳过。）
		if len(a) > 2 && strings.HasPrefix(a, "-o") {
			a = a[2:]
		}
		if i := strings.IndexByte(a, '='); i >= 0 {
			a = a[:i]
		}
		if strings.EqualFold(a, name) {
			return true
		}
	}
	return false
}

// sshFailureMarkers 是 **ssh 自身**失败时 stderr **首行**上的特征串（OpenSSH 的固定措辞）。
//
// 用途：把"ssh 自己挂了（exit 255）"与"远端命令自己退出 255"区分开（见 ErrSSHTransport）。
//
// # 口径（v4-2-A 复查 F2，**收紧**）：只在 stderr 的**首个非空行**上做**前缀**匹配，
// 且只认 ssh **特有**的强特征：
//
//   - "ssh: "：OpenSSH 自己的错误前缀（连接被拒 / 解析不了 / 建连超时都走这条）；
//   - "kex_exchange_identification" / "ssh_exchange_identification"：密钥交换被打断；
//   - "Permission denied ("：**带鉴权方法括号**的形态（publickey/password）；
//   - "Too many authentication failures" / "Host key verification failed"：认证与主机密钥。
//
// # 为什么不再用通用措辞
//
// ssh 把**远端命令的 stderr** 与**自己的错误**汇进同一条流（无法区分）。而通用措辞
// （"Permission denied" 不带括号、"Connection refused"）**远端命令自己也会打**
// （`cat: /etc/shadow: Permission denied`）——拿它当 ssh 自身的特征，就是把
// "远端跑挂了"误报成"根本没连上"（调用方会以为机器不可达）。故通用措辞**不再**触发
// ErrSSHTransport：宁可漏判成远端退出（与 classifySSHExit 的注释口径一致）。
var sshFailureMarkers = []string{
	"ssh: ",                            // OpenSSH 自己的错误前缀（"ssh: connect to host …"）
	"kex_exchange_identification",      // 密钥交换被打断（常见于被中间设备挡住）
	"ssh_exchange_identification",      // 同上（另一种拼写）
	"Permission denied (",              // 鉴权失败且**带鉴权方法**（publickey/password）
	"Too many authentication failures", // 试了太多次口令
	"Host key verification failed",     // 主机密钥不匹配
}

// isSSHTransportFailure 判定 stderr 是否是 ssh **自身**的失败信息。
//
// 判据：**首个非空行**（可选地剥掉 "user@host: " 前缀）以某条 sshFailureMarkers 开头。
// 行内其它位置出现这些措辞**不算**（那是远端命令自己的输出）。
func isSSHTransportFailure(stderr []byte) bool {
	line := firstMeaningfulLine(stderr)
	if line == "" {
		return false
	}
	candidates := []string{line}
	if stripped := stripHostPrefix(line); stripped != line {
		candidates = append(candidates, stripped)
	}
	for _, c := range candidates {
		for _, m := range sshFailureMarkers {
			if strings.HasPrefix(c, m) {
				return true
			}
		}
	}
	return false
}

// firstMeaningfulLine 取 stderr 的**首个非空行**（去掉首尾空白）。空 → ""。
func firstMeaningfulLine(b []byte) string {
	for _, line := range strings.Split(string(b), "\n") {
		if s := strings.TrimSpace(line); s != "" {
			return s
		}
	}
	return ""
}

// stripHostPrefix 剥掉 OpenSSH 常见的 "user@host: " 前缀（鉴权类消息会带它：
// "user@harness: Permission denied (publickey,password)."）。
//
// 只在"冒号前那一段**含 '@' 且不含空白**"时才剥（免得把 "ssh: connect to host …"
// 这种也剥掉——那会丢掉 ssh 自己的前缀特征）。
func stripHostPrefix(line string) string {
	i := strings.Index(line, ": ")
	if i <= 0 {
		return line
	}
	head := line[:i]
	if !strings.Contains(head, "@") || strings.ContainsAny(head, " \t") {
		return line
	}
	return strings.TrimSpace(line[i+2:])
}

// classifySSHExit 把"ssh 自己失败（exit 255）"从"远端命令退出 255"里分出来：
// 返回非 nil = 这是 ssh 的传输层失败（ErrSSHTransport，附 stderr 片段）；
// 返回 nil = 不是（按普通退出码透出，**不**误报成"没连上"）。
//
// 判据（v4-2-A 复查 F2，**收紧**）：stderr 的**首个非空行**命中 ssh 自身的强特征
// （见 sshFailureMarkers：ssh: 前缀 / 密钥交换被打断 / 带鉴权方法括号的
// "Permission denied (" / 主机密钥不匹配 …）。通用措辞（不带括号的
// "Permission denied"、"Connection refused"）**不**触发——远端命令自己也会打这些
// （`cat: /etc/shadow: Permission denied`），误报成"没连上"比漏判更糟。
// 没命中 → **按"远端命令退出 255"透出**（宁可漏判，与本注释口径一致）。
//
// osExec 与 Env.Run 都用它 → 生产路径与注入 exec 路径口径一致。
func classifySSHExit(res core.ExecResult) error {
	if res.ExitCode != 255 || !isSSHTransportFailure(res.Stderr) {
		return nil
	}
	return fmt.Errorf("%w：%s", ErrSSHTransport, errSnippet(res.Stderr))
}

// osExec 是生产实现：os/exec 起系统 ssh（不经本地 shell、参数数组传递）。
//
// ctx 结束 → 杀掉 ssh 进程（Cancel 回调；ssh 是否把信号转发给远端由系统 ssh 决定，
// v4-2-A 不深究——本地这条 ssh 一定被收掉；**远端命令可能仍在跑**，这是 SSH 的
// 已知语义，doc 里写明了）。非零退出码不是 error（ExitCode 带上），
// 与 local / sandbox 的调用侧语义逐条对齐（K8：换 env 不动调用方）。
//
// 唯一例外：**exit 255 且 stderr 首行命中 ssh 自身的强特征** → 返回 ErrSSHTransport
// （"根本没连上"与"远端跑挂了"必须分得开，见 ErrSSHTransport 与 classifySSHExit）。
func osExec(ctx context.Context, name string, args []string) (core.ExecResult, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr cappedBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	hideWindow(cmd) // Windows：不弹控制台窗口（陷阱 5）
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		return nil
	}
	cmd.WaitDelay = waitDelay // 管道关不上时的硬上限（同 local）

	startErr := cmd.Start()
	if startErr != nil {
		// 起不来：ssh 不在 PATH / 不是可执行。
		return core.ExecResult{}, fmt.Errorf("remote: 启动 %q 失败（系统 ssh 可用？）: %w", name, startErr)
	}
	waitErr := cmd.Wait()

	res := core.ExecResult{
		ExitCode: exitCode(cmd, waitErr),
		Stdout:   stdout.bytes(),
		Stderr:   stderr.bytes(),
	}
	// 取消/超时优先呈现：此时 waitErr 多半是 "signal: killed"，
	// 直接回它会让人以为是命令自己崩的。
	if err := ctx.Err(); err != nil {
		return res, fmt.Errorf("remote: 执行 %q 被中止（%w）", name, err)
	}
	if waitErr != nil && !asExitError(waitErr) {
		// 非 ExitError 的等待失败（IO 错误等）才是真 error。
		return res, fmt.Errorf("remote: 等待 %q 结束失败: %w", name, waitErr)
	}
	// ssh 自己用 255 表示"传输层失败"；远端命令退出 255 时 ssh 也透出 255。
	// 用 stderr 的形态区分：命中 ssh 自身的错误措辞 → 传输失败（ErrSSHTransport）。
	if err := classifySSHExit(res); err != nil {
		return res, err
	}
	if res.ExitCode == 255 {
		// 没命中特征：按"远端命令自己退出 255"透出（不误报成"没连上"），
		// 但记一笔结构化日志——这个歧义本身值得被看见。
		log.Warn("remote: ssh 退出码 255，stderr 无 ssh 特征，按远端命令退出 255 透出",
			"stderr", errSnippet(res.Stderr))
	}
	// ExitError：ssh 自己非零退出（含"远端命令非零"经 ssh 透出）→ 不是 error。
	return res, nil
}

// errSnippet 把 stderr 压成一行、限长 200 字符（进错误信息用；不整段贴）。
func errSnippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if s == "" {
		return ""
	}
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	r := []rune(s)
	if len(r) > 200 {
		return string(r[:200]) + "…"
	}
	return s
}

// exitCode 从 Cmd / waitErr 里取退出码（取不到给 -1 = 未知）。同 local / sandbox。
func exitCode(cmd *exec.Cmd, waitErr error) int {
	if cmd.ProcessState != nil {
		return cmd.ProcessState.ExitCode()
	}
	var ee *exec.ExitError
	if waitErr != nil && errors.As(waitErr, &ee) && ee.ProcessState != nil {
		return ee.ProcessState.ExitCode()
	}
	return -1
}

// asExitError 判定"等待失败是否源于命令自身非零退出"。同 local / sandbox。
func asExitError(err error) bool {
	var ee *exec.ExitError
	return errors.As(err, &ee)
}

// cappedBuffer 是带上限的输出缓冲（防无限输出吃光内存）。同 local / sandbox。
type cappedBuffer struct {
	buf     bytes.Buffer
	dropped int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	room := maxCapturedOutput - b.buf.Len()
	if room <= 0 {
		b.dropped += len(p)
		return len(p), nil // 假装全收下，避免 io.Copy 报 short write
	}
	if len(p) > room {
		b.dropped += len(p) - room
		p = p[:room]
	}
	return b.buf.Write(p)
}

func (b *cappedBuffer) bytes() []byte { return b.buf.Bytes() }
