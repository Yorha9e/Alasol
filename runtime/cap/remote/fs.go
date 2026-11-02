package remote

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/core"
)

// Fs 能力面（core.FSCapability）的**远端实现**：经 ssh exec 通道跑远端命令
// （cat / stat / ls）——与 Exec 同一条系统 ssh 执行缝（sshRun），**零新依赖**
// （不引 sftp 库；见包 doc 的依赖纪律）。
//
// 四个方法的远端命令形状（路径一律 shQuote 单引号安全引用，注入面与 Run 同款封死；
// **env 前缀与 Run 同款**——`env -i` 整份替换 + 远端侧 PATH，DefaultRemotePATH
// 对 Fs **同样生效**）：
//
//	ReadFile  → `env -i 'PATH=…' 'cat' '--' '<path>'`    stdout 原样字节返回（二进制安全）
//	WriteFile → `env -i 'PATH=…' 'cat' > '<path>'`       内容走 **ssh stdin 管道**（不进 argv）
//	Stat      → `env -i 'PATH=…' 'stat' '-c' '%s|%Y|%F' '--' '<path>'`   **GNU stat 口径**
//	ListDir   → `env -i 'PATH=…' 'ls' '-1A' '--' '<path>'`   逐行拆名字
//
// # 错误口径
//
//   - 远端命令**非 0 退出** → 明文错误（op + path + 退出码 + 截断 stderr，errSnippet）
//     ——FSCapability 没有 ExitCode 可带，与 Exec 的"非零不是 error"不同档；
//   - **ssh 传输层失败** → ErrSSHTransport 哨兵（classifySSHExit，与 Exec 同款，
//     生产与注入路径同一口径）；
//   - 路径**整值**命中凭据形态 → 与 Run 同纪律先过 checkNoCredentialEnv
//     （fail-close，ErrCredentialEnv）。⚠ **只拦整值命中**：路径里**嵌着**秘密
//     （如 `/data/sk-abc123.txt`）**仍会**进 ssh argv，**且**会被错误文案回显
//     （fsExec 的错误带 `path=…`）——**别把秘密放进路径**。
//
// # 诚实边界（最小可用档）
//
//   - **GNU stat 口径**：只认 `stat -c` 的 '大小|mtime秒|类型' 形状；非 GNU
//     （BSD/macOS 的 stat -f 不认 -c）明确报"远端 stat 非 GNU 口径"，不瞎解析；
//   - **stat 不跟随符号链接**（GNU stat 无 -L）：报的是**链接本身**的大小/mtime/
//     类型（类型段是 "symbolic link"），**不是目标**。**行为不改**——加 -L 会
//     把语义换成"报目标"，那不是最小档该做的代偿；要目标信息自己先解析链接。
//   - **文件名含换行会被 ListDir 错拆**（`ls -1A` 一行一个名字的拆法天然如此）；
//   - **ListDir 的排序依远端 ls 的 locale**（`ls -1A` 按 locale 排序，不是"按名字
//     字节序"的承诺）：本档的 Fs 命令走 `env -i`（LC_*/LANG 未设）→ 多数实现的 ls
//     回落 **C locale**（字节序，与本地 os.ReadDir 一致）；但**顺序不是契约**——
//     远端 ls 实现/locale 一变，条目顺序就可能与本地不同。
//   - ReadFile 受单次执行 32MB 捕获上限约束（maxCapturedOutput，**超出静默截断到
//     32MB、不报错**——超限不是错误，与 Exec 的输出捕获同口径）；
//   - WriteFile 是 `cat >` **直接截断写**，非原子（同 local/sandbox 的已知边界）；
//   - ListDir 对**非目录**路径按 ls 口径返回该参数本身（本地 os.ReadDir 会报错）
//     ——最小档不代偿，要严格目录语义先 Stat；
//   - `~` 不展开（单引号引用），远端路径请用绝对路径（同 Exec）。
//
// 并发安全：无共享可变状态，每次调用经 sshRun 起一条独立 ssh 进程（见 Env）。

// ReadFile implements core.FSCapability：`ssh host env -i 'PATH=…' 'cat' '--' '<path>'`。
//
// stdout **原样字节**返回——cappedBuffer 收的就是 []byte，这里不做任何文本转换
// （二进制安全：NUL / 非法 UTF-8 / CRLF 全部逐字节透传）。
func (e *Env) ReadFile(ctx context.Context, path string) ([]byte, error) {
	remoteCmd, err := e.fsRemoteCmd("cat", "--", path)
	if err != nil {
		return nil, err
	}
	res, err := e.fsExec(ctx, "读取文件", path, remoteCmd, nil)
	if err != nil {
		return nil, err
	}
	return res.Stdout, nil
}

// WriteFile implements core.FSCapability：`ssh host env -i 'PATH=…' 'cat' > '<path>'`，
// data 走 ssh 的 **stdin 管道**（二进制安全；内容不落 argv——argv 会进两端进程表，
// 见 ErrCredentialEnv）。
//
// ⚠ 远端命令行不能走逐 token 拼装（buildRemoteCommand 会把 '>' 引成字面量）：
// 重定向符必须是**裸的** shell 操作符，路径则仍经 shQuote 引用——注入面同款封死。
// 故：cat 部分仍走 fsRemoteCmd（**带 env -i 前缀**，与另三个方法同款），重定向符
// 与路径另行拼。
func (e *Env) WriteFile(ctx context.Context, path string, data []byte) error {
	catCmd, err := e.fsRemoteCmd("cat")
	if err != nil {
		return err
	}
	remoteCmd := catCmd + " > " + shQuote(path)
	_, err = e.fsExec(ctx, "写入文件", path, remoteCmd, data)
	return err
}

// Stat implements core.FSCapability：`ssh host env -i 'PATH=…' 'stat' '-c' '%s|%Y|%F' '--' '<path>'`
// （**GNU stat 口径**；见 parseGNUStat 与 looksLikeNonGNUStat）。
func (e *Env) Stat(ctx context.Context, path string) (core.FileInfo, error) {
	remoteCmd, err := e.fsRemoteCmd("stat", "-c", "%s|%Y|%F", "--", path)
	if err != nil {
		return core.FileInfo{}, err
	}
	res, err := e.fsExec(ctx, "stat", path, remoteCmd, nil)
	if err != nil {
		// BSD/macOS 的 stat 不认 -c（"stat: illegal option -- c"）→ 点名"非 GNU 口径"，
		// 而不是把它的报错/用法输出当数据瞎解析。
		if res.ExitCode != 0 && looksLikeNonGNUStat(res.Stderr) {
			return core.FileInfo{}, fmt.Errorf(
				"remote: 远端 stat 非 GNU 口径（需要 GNU stat 的 -c 格式；BSD/macOS 的 stat -f 本最小档不支持）（path=%s，退出码 %d）",
				path, res.ExitCode)
		}
		return core.FileInfo{}, err
	}
	return parseGNUStat(res.Stdout)
}

// ListDir implements core.FSCapability：`ssh host env -i 'PATH=…' 'ls' '-1A' '--' '<path>'`，
// 逐行拆名字。
//
// 返回**条目名**（不是完整路径）——同 local/sandbox：让调用方自己拼。
// 诚实边界：文件名含换行会被错拆；排序依远端 ls 的 locale（见包 doc）。
func (e *Env) ListDir(ctx context.Context, path string) ([]string, error) {
	remoteCmd, err := e.fsRemoteCmd("ls", "-1A", "--", path)
	if err != nil {
		return nil, err
	}
	res, err := e.fsExec(ctx, "列举目录", path, remoteCmd, nil)
	if err != nil {
		return nil, err
	}
	return splitLsLines(res.Stdout), nil
}

// fsRemoteCmd 把 Fs 用的远端命令按 Exec 同款的逐 token 安全引用拼成一条命令行
// （复用 buildRemoteCommand——与 Run 同一套拼装逻辑，不分叉）。
//
// **Env 填的是远端侧的 PATH**（与 Run 同款 `env -i` **整份替换**）：Fs 的四条命令
// **也**在 `env -i` 前缀里跑。两个理由：
//
//   - **环境是整份替换**：远端 login shell 继承来的环境（远端机器上的凭据等）
//     在 Fs 命令里**不可见**——与 Run 同一条纪律，不给 Fs 开后门；
//   - **DefaultRemotePATH 对 Fs 生效**：不填 Env 时 Fs 命令**不带** env 前缀，
//     远端命令找不到 PATH 覆盖值指定的命令（如 /opt/homebrew/bin 下的工具）。
//
// （Env 里给的 PATH 值会被 buildRemoteCommand **原地替换**成本 Env 配置的远端侧
// PATH——与 Run 的口径完全一致，调用方给的 harness 本机 PATH 一律忽略。）
func (e *Env) fsRemoteCmd(name string, args ...string) (string, error) {
	return e.buildRemoteCommand(name, core.ExecSpec{
		Name: name,
		Args: args,
		Env:  []string{"PATH=" + e.remotePATH},
	})
}

// fsExec 执行一条 Fs 用的远端命令，并按 **Fs 口径**收口错误：
//
//		ctx 已结束 → 直接报错（不碰 ssh，同 Run）；
//	  - 路径凭据形态 → ErrCredentialEnv（fail-close，起 ssh 之前）；
//	  - ssh 传输失败 → ErrSSHTransport（classifySSHExit，与 Exec 同款）；
//	  - 远端**非 0 退出** → 明文错误：op + path + 退出码 + 截断 stderr（errSnippet）。
//
// 返回的 ExecResult 即便在非 0 退出时也带上（Stat 要看 stderr 判非 GNU 口径）。
func (e *Env) fsExec(ctx context.Context, op, path, remoteCmd string, stdin []byte) (core.ExecResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return core.ExecResult{}, fmt.Errorf("remote: 上下文已结束（%w）", err)
	}
	// 路径与 Run 的 args 同纪律：它会拼进远端命令（ssh argv，两端进程表可见）→
	// 凭据形态的路径一律拒绝（值不进错误文案）。
	if err := checkNoCredentialEnv(nil, []string{path}); err != nil {
		return core.ExecResult{}, err
	}
	res, err := e.sshRun(ctx, remoteCmd, stdin)
	if err != nil {
		return res, err
	}
	if res.ExitCode != 0 {
		if sn := errSnippet(res.Stderr); sn != "" {
			return res, fmt.Errorf("remote: %s失败（path=%s，退出码 %d）：%s", op, path, res.ExitCode, sn)
		}
		return res, fmt.Errorf("remote: %s失败（path=%s，退出码 %d）", op, path, res.ExitCode)
	}
	return res, nil
}

// parseGNUStat 解析 `stat -c '%s|%Y|%F'` 的输出 → core.FileInfo。
//
// 形状：`<大小>|<mtime秒>|<类型>`（GNU %F 的类型串如 "regular file" /
// "directory" / "symbolic link" 含空格但不含 '|'，三段切分安全）。
//
// **坏形状一律报错，不瞎猜**（fail-close）：段数不对 / 大小或 mtime 不是整数 /
// 类型段为空 → 明文错误（带输出片段与"非 GNU 口径"提示），返回零值 FileInfo。
// 宁可报"解析不了"，也不返回一个编出来的 Size/ModTime 让上层当真。
func parseGNUStat(out []byte) (core.FileInfo, error) {
	// 只剥行尾换行（GNU stat 输出一行）；其余换行/回车一律当坏形状——
	// 混进中段的 \n（别的工具蹭了 stdout）与 CRLF 形态都不许静默吞掉。
	s := strings.TrimRight(string(out), "\n")
	if strings.ContainsAny(s, "\r\n") {
		return core.FileInfo{}, fmt.Errorf(
			"remote: stat 输出含换行/回车（远端 stat 非 GNU 口径？）：输出=%s", errSnippet(out))
	}
	parts := strings.Split(s, "|")
	if len(parts) != 3 {
		return core.FileInfo{}, fmt.Errorf(
			"remote: stat 输出形状不对（远端 stat 非 GNU 口径？期望 '大小|mtime秒|类型' 三段）：输出=%s", errSnippet(out))
	}
	size, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return core.FileInfo{}, fmt.Errorf(
			"remote: stat 输出的大小段不是整数（远端 stat 非 GNU 口径？）：输出=%s", errSnippet(out))
	}
	sec, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return core.FileInfo{}, fmt.Errorf(
			"remote: stat 输出的 mtime 段不是整数（远端 stat 非 GNU 口径？）：输出=%s", errSnippet(out))
	}
	typ := parts[2]
	if typ == "" {
		return core.FileInfo{}, fmt.Errorf(
			"remote: stat 输出的类型段为空（远端 stat 非 GNU 口径？）：输出=%s", errSnippet(out))
	}
	return core.FileInfo{
		Size:    size,
		IsDir:   typ == "directory",
		ModTime: time.Unix(sec, 0),
	}, nil
}

// looksLikeNonGNUStat 判定 stat 的 stderr 是不是"**非 GNU stat 不认 -c**"的形态
// （BSD/macOS：`stat: illegal option -- c`）。
//
// 判据保守：首个非空行小写后**同时**含 "stat" 与一条"选项不认"措辞
// （illegal / invalid / unrecognized / unknown option）。通用报错
// （"cannot statx …: No such file"、"command not found"）**不**命中——
// 那些走 fsExec 的普通退出码错误（宁可口径保守，与 classifySSHExit 同哲学）。
func looksLikeNonGNUStat(stderr []byte) bool {
	line := strings.ToLower(firstMeaningfulLine(stderr))
	if !strings.Contains(line, "stat") {
		return false
	}
	for _, m := range []string{"illegal option", "invalid option", "unrecognized option", "unknown option"} {
		if strings.Contains(line, m) {
			return true
		}
	}
	return false
}

// splitLsLines 把 `ls -1A` 的输出按行拆成条目名。
//
// 空输出（空目录）→ 空切片（非 nil，与 local 的零条目一致）。
//
// 诚实边界：**文件名含换行会被错拆**——`ls -1A` 是"一行一个名字"的口径，
// 名字里的 '\n' 与分隔符无法区分（`a\nb` 可能是两个文件，也可能是一个含换行的
// 文件）。最小可用档如实拆行、不假装识别；见包 doc。
func splitLsLines(out []byte) []string {
	s := strings.TrimRight(string(out), "\n")
	if s == "" {
		return []string{}
	}
	return strings.Split(s, "\n")
}
