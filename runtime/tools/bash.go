package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// bash 工具：起一个 shell 执行一条命令。
//
// 元数据口径（D12 / M7 / X2）：
//   - side_effect=write：命令能干任何事，按"写"以上对待（保守）；
//   - idempotent=false + replay=unsafe：**绝不自动重试**（M7），崩溃续跑时未知结果
//     转人工/agent 判定（X2）——同一条命令跑两遍的副作用可能完全不同；
//   - approvable=true：默认要求人审；
//   - cancelable=true：长命令可被 ctx 取消（杀进程树见 runtime/cap/local）。
var _ core.ToolExecutor = (*BashExecutor)(nil)

// BashDescriptor 是 bash 工具的跨边界描述。
func BashDescriptor() contracts.Tool {
	return contracts.Tool{
		Name: toolNameBash,
		Description: strPtr(fmt.Sprintf("执行一条 shell 命令（按 %s 语法解释，同步等待并返回退出码/stdout/stderr）。"+
			"输出可能含环境回显（命令自己 echo 出来的），框架已对**注入的凭据**做脱敏（值替换为 ***），"+
			"但请不要依赖回显来取凭据。", shellSyntax)),
		Schema:     bashSchema,
		SideEffect: contracts.SideEffectWrite,
		Cancelable: true,
		Approvable: true,
		Idempotent: false,
		Replay:     contracts.ReplayKindUnsafe,
	}
}

// bashSchema 是 bash 的参数 Schema。
var bashSchema = json.RawMessage(`{
  "type": "object",
  "properties": {
    "command": {
      "type": "string",
      "description": "要执行的命令行（由 shell 解释，可含管道/重定向/多命令）"
    }
  },
  "required": ["command"],
  "additionalProperties": false
}`)

type bashArgs struct {
	Command string `json:"command"`
}

// BashExecutor 是 bash 的执行器。
type BashExecutor struct {
	exec core.ExecCapability
}

// NewBash 造 bash 执行器。exec 为 nil 时执行阶段才报错（不 panic，见 read/write 同款说明）。
func NewBash(exec core.ExecCapability) core.ToolExecutor {
	return &BashExecutor{exec: exec}
}

// Execute implements core.ToolExecutor。
//
// env 注入（H17/H21）：core.EnvFrom(ctx) 里经策略裁决获准的键值，合并进子进程 env。
// **只加注入的键**，且值**绝不主动回显**——不进返回值、不进错误文案、不进日志。
//
// 但命令自己 `echo $VAR` 打出来的值**会**进 stdout → 那是命令的输出。框架在
// 「工具输出 → ToolResult」这一步把**注入的凭据值**逐个精确替换成 `***`
// （见 core/loop 的脱敏），所以凭据不会流进事件与下一轮 prompt；命令的其他输出
// 原样保留（数据就是数据）。
func (e *BashExecutor) Execute(ctx context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	if e == nil || e.exec == nil {
		return contracts.ToolResult{}, fmt.Errorf("%s: 未注入 ExecCapability（装配缺失，无法执行命令）", toolNameBash)
	}
	var args bashArgs
	if err := decodeArgs(toolNameBash, intent.Arguments, &args); err != nil {
		return contracts.ToolResult{}, err
	}
	if strings.TrimSpace(args.Command) == "" {
		return contracts.ToolResult{}, fmt.Errorf("%s: command 不能为空；期望形如 %s", toolNameBash, argsHint(toolNameBash))
	}

	res, err := e.exec.Run(ctx, core.ExecSpec{
		Name: shellName, // 平台常量（build tag）
		Args: []string{shellFlag, args.Command},
		Env:  childEnv(ctx), // 净化父 env + ctx 注入键（H16/H18/H21）
	})
	if err != nil {
		// 起不来 / 被取消 / IO 失败——这些才是 error。
		// %w：保住错误链（与 read/write 同口径）——调用方要能 errors.Is(err,
		// remote.ErrSSHTransport) 之类地判底层原因（%v 会把链断掉，errors.Is 判不出来）。
		return contracts.ToolResult{}, fmt.Errorf("%s: %w", toolNameBash, err)
	}
	// 非零退出码**不是 error**：它是命令的业务结果，连同 stdout/stderr 一起给模型看（M5）。
	return resultText(capText(formatExecResult(res))), nil
}

// childEnv 组装子进程 env：**净化后的父 env** + ctx 注入的键。
//
// 依据 core/env.go 的两条口径：
//
//   - 「工具默认继承净化后的父 env」→ os.Environ() 过 core.ScrubEnv，
//     凭据类键（*_KEY/*_TOKEN/*_SECRET/…）与凭据形态的值（sk-…/ghp_…/JWT）被剥掉；
//   - 「显式注入（required_env 经策略裁决）的键值不在此处剥离」→ core.EnvFrom(ctx)
//     拿到的键值原样合并（调用级覆盖会话级，core.MergeEnv 的语义）。
//
// 排序输出：保证 env 顺序确定（便于测试与复现），Windows 上还能避免同名键大小写歧义。
func childEnv(ctx context.Context) []string {
	parent := map[string]string{}
	for _, kv := range os.Environ() {
		if k, v, ok := strings.Cut(kv, "="); ok {
			parent[k] = v
		}
	}
	// 注意：必须用 ScrubEnv 的**返回值**当新基底——把结果拷回原 map 的话，
	// 被剥掉的凭据键还留在里面（净化就白做了）。
	base := map[string]string(core.ScrubEnv(core.EnvScope(parent)))
	if injected, ok := core.EnvFrom(ctx); ok {
		for k, v := range injected {
			base[k] = v // 注入的键值不剥离（H21：已经过策略裁决）
		}
	}
	out := make([]string, 0, len(base))
	for k, v := range base {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// formatExecResult 把 ExecResult 渲染成给模型看的文本。
//
// 结构固定（模型好解析）：第一行 exit_code，然后按需 stdout/stderr 段。
// 非零退出码之所以一眼能看到：模型经常需要先判断"命令失败了"再决定怎么改。
func formatExecResult(res core.ExecResult) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "exit_code: %d\n", res.ExitCode)
	if out, cut := truncateText(res.Stdout); len(out) > 0 {
		sb.WriteString("--- stdout ---\n")
		sb.WriteString(out)
		if !strings.HasSuffix(out, "\n") {
			sb.WriteString("\n")
		}
		if cut {
			sb.WriteString(truncationNote(len(res.Stdout) - len(out)))
		}
	}
	if errOut, cut := truncateText(res.Stderr); len(errOut) > 0 {
		sb.WriteString("--- stderr ---\n")
		sb.WriteString(errOut)
		if !strings.HasSuffix(errOut, "\n") {
			sb.WriteString("\n")
		}
		if cut {
			sb.WriteString(truncationNote(len(res.Stderr) - len(errOut)))
		}
	}
	return sb.String()
}

// Descriptors 返回三个内置工具的完整描述（默认形态）。
//
// 装配方（host / loop）拿它去注册表注册；要改某工具的 Approvable 就单独用
// ReadDescriptor / WriteDescriptor(false) / BashDescriptor 自己拼。
// 顺序固定：read / write / bash（List 的顺序可复现）。
func Descriptors() []contracts.Tool {
	return []contracts.Tool{
		ReadDescriptor(),
		WriteDescriptor(true),
		BashDescriptor(),
	}
}
