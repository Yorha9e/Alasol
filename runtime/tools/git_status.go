package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// git_status 工具：查当前 git 仓库状态（当前分支 / 工作区是否 dirty / 最近一次提交摘要）。
//
// 元数据口径（D12，参照 read）：
//   - side_effect=read：只读，不碰外部状态（`git status` / `git log` / `git rev-parse` 都是只读子命令）；
//   - idempotent=true + replay=safe：重复查询无副作用，崩溃续跑可安全重放（X2）；
//   - approvable=false：只读查询不需要人审（**默认**仍需策略链放行，见 policy.Decide）。
//
// 为什么走 os/exec 直接调 git（而不是 ExecCapability / bash）：
//   - 本工具**不猜 shell 语义、不拼命令串**——git 的只读子命令由 git 自己定语义，
//     exec.Command 直接 exec（不经 shell）没有注入面（陷阱 8 的反面）；
//   - git 是**系统外部命令**，不是 harness 的能力面（Fs/Exec），故不接 FSCapability/
//     ExecCapability；git 不可用时给出明确、可自我修正的错误（M5），而不是让底层
//     exec 报错透给模型。
//
// **不进 prompt**（J3）：本工具只作为普通工具经注册表暴露（模型按需经 tools 数组调用），
// 不向 system prompt / 任何注入文本里加一个字。

// toolNameGitStatus 是 git_status 工具名（注册表 key / 模型调用名 / 事件里三处一致）。
const toolNameGitStatus = "git_status"

// GitStatusDescriptor 是 git_status 工具的跨边界描述（给模型 + 给策略链）。
func GitStatusDescriptor() contracts.Tool {
	return contracts.Tool{
		Name:        toolNameGitStatus,
		Description: strPtr("查询当前 git 仓库状态：当前分支名、工作区是否有未提交改动（dirty）、最近一次提交摘要。只读操作。"),
		Schema:      gitStatusSchema,
		SideEffect:  contracts.SideEffectRead,
		Cancelable:  true,
		Approvable:  false,
		Idempotent:  true,
		Replay:      contracts.ReplayKindSafe,
	}
}

// gitStatusSchema 是 git_status 的参数 Schema（path 可选：空 = 进程当前目录）。
var gitStatusSchema = json.RawMessage(`{
  "type": "object",
  "properties": {
    "path": {
      "type": "string",
      "description": "git 仓库目录路径（可选；空 = 进程当前工作目录）。仅用于定位仓库，本工具只读、不做任何写操作"
    }
  },
  "additionalProperties": false
}`)

type gitStatusArgs struct {
	Path string `json:"path"`
}

// gitRunFunc 是一次 git 子命令的执行形态（dir 为工作目录，空 = 进程 cwd）。
// 抽成类型是为了让测试能注入替身（确定性复现“正常返回 / git 不可用”两条路径）。
type gitRunFunc func(ctx context.Context, dir string, args ...string) ([]byte, error)

// GitStatusExecutor 是 git_status 的执行器（core.ToolExecutor）。
//
// lookPath / run 是测试注入点（nil = 走真实 os/exec）：
//   - lookPath：查 git 可执行文件是否在 PATH（默认 exec.LookPath）；
//   - run：执行一次 git 子命令（默认 exec.CommandContext）。
type GitStatusExecutor struct {
	lookPath func(file string) (string, error)
	run      gitRunFunc
}

// 编译期断言：GitStatusExecutor 必须满足 core.ToolExecutor。
var _ core.ToolExecutor = (*GitStatusExecutor)(nil)

// NewGitStatus 造 git_status 执行器。无外部能力依赖（直接 os/exec 调系统 git）。
func NewGitStatus() core.ToolExecutor { return &GitStatusExecutor{} }

// defaultGitRun 是真实 os/exec 实现：不经 shell，直接 exec git（无注入面）。
func defaultGitRun(ctx context.Context, dir string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	return cmd.Output()
}

// Execute implements core.ToolExecutor。
func (e *GitStatusExecutor) Execute(ctx context.Context, intent contracts.ToolCallIntentPayload) (contracts.ToolResult, error) {
	// path 可选：intent.Arguments 为空 = 全默认（进程 cwd）；非空才解码校验。
	var args gitStatusArgs
	if len(strings.TrimSpace(string(intent.Arguments))) > 0 {
		if err := decodeArgs(toolNameGitStatus, intent.Arguments, &args); err != nil {
			return contracts.ToolResult{}, err
		}
	}
	dir := strings.TrimSpace(args.Path)

	// git 可用性检查（git 不可用 → 明确、可自我修正的错误，M5）。
	look := e.lookPath
	if look == nil {
		look = exec.LookPath
	}
	if _, err := look("git"); err != nil {
		return contracts.ToolResult{}, fmt.Errorf(
			"%s: 系统找不到 git 可执行文件（%v）；请先安装 git，或确认它在 PATH 里", toolNameGitStatus, err)
	}

	run := e.run
	if run == nil {
		run = defaultGitRun
	}

	branch, err := gitTrim(ctx, run, dir, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return contracts.ToolResult{}, fmt.Errorf(
			"%s: 读取当前分支失败（目录 %q 不是 git 仓库或 git 报错：%v）；请确认 path 指向一个 git 仓库根目录",
			toolNameGitStatus, dirOrCWD(dir), err)
	}
	status, err := gitTrim(ctx, run, dir, "status", "--porcelain")
	if err != nil {
		return contracts.ToolResult{}, fmt.Errorf(
			"%s: 读取工作区状态失败（%v）；请确认 path 指向一个 git 仓库根目录", toolNameGitStatus, err)
	}
	last, err := gitTrim(ctx, run, dir, "log", "-1", "--pretty=format:%h %s (%cr, %an)")
	if err != nil {
		// 空仓库（无提交）时 git log 会失败：这不是“路径错”，给出可读收场。
		last = "（无提交）"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "branch: %s\n", branch)
	if status != "" {
		b.WriteString("dirty: true\n")
	} else {
		b.WriteString("dirty: false\n")
	}
	fmt.Fprintf(&b, "last_commit: %s\n", last)
	return resultText(b.String()), nil
}

// gitTrim 执行一次 git 子命令并去掉首尾空白（git 输出常带换行）。
func gitTrim(ctx context.Context, run gitRunFunc, dir string, args ...string) (string, error) {
	out, err := run(ctx, dir, args...)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// dirOrCWD 把空目录显示成进程 cwd（错误文案里更可读）。
func dirOrCWD(dir string) string {
	if dir == "" {
		return "."
	}
	return dir
}
