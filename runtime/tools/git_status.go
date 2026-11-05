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

// GitStatus 是一次 git 状态探测的结构化结果。
//
// 共享口径（G17 git 观测，v4-7 后半）：git_status 工具与 host 的 GET /api/git
// 观测面用**同一套探测逻辑**（probeGitStatus）——两面看到的 branch/dirty/last_commit
// 恒同源，不会一边说 dirty 一边说 clean。
type GitStatus struct {
	// Repo = 目录在 git 仓库里且能读到分支（false 时其余字段缺省，Error 点名原因）。
	Repo bool `json:"repo"`
	// Branch 当前分支名（Repo=true 时非空）。
	Branch string `json:"branch,omitempty"`
	// Dirty 工作区是否有未提交改动（Repo=true 时才有意义；指针区分"没探测到"与 false）。
	Dirty *bool `json:"dirty,omitempty"`
	// LastCommit 最近一次提交摘要（空仓库无提交 = "（无提交）"）。
	LastCommit string `json:"last_commit,omitempty"`
	// Error 探测失败的原因（Repo=false 时非空；文案与工具错误同源，M5）。
	Error string `json:"error,omitempty"`
}

// SnapshotGitStatus 用真实 os/exec 探测 dir 的 git 状态（dir 空 = 进程 cwd）。
//
// **只读**：只跑 rev-parse / status --porcelain / log -1 三条只读子命令，不经 shell
// （无注入面，陷阱 8 的反面）。非仓库 / git 不可用 → Repo=false + Error（不是 panic、
// 不是空结构——观测面要能如实说"这里不是仓库"）。
//
// 与 git_status 工具共享 probeGitStatus：同一逻辑、两处出口（工具给模型、HTTP 面给 GUI）。
func SnapshotGitStatus(ctx context.Context, dir string) GitStatus {
	e := &GitStatusExecutor{}
	p, err := e.probeGitStatus(ctx, dir)
	if err != nil {
		return GitStatus{Repo: false, Error: err.Error()}
	}
	dirty := p.dirty
	return GitStatus{Repo: true, Branch: p.branch, Dirty: &dirty, LastCommit: p.last}
}

// gitProbe 是探测中间结果（分支 / 是否 dirty / 最近提交摘要）。
type gitProbe struct {
	branch string
	dirty  bool
	last   string
}

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

// gitStatusSchema 是 git_status 的参数 Schema（path 可选：空 = 会话工作区，再空 = 进程 cwd）。
var gitStatusSchema = json.RawMessage(`{
  "type": "object",
  "properties": {
    "path": {
      "type": "string",
      "description": "git 仓库目录路径（可选；空 = 会话工作区，未配置工作区时为进程当前工作目录）。仅用于定位仓库，本工具只读、不做任何写操作"
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
	// path 可选：intent.Arguments 为空 = 全默认（工作区，再空则进程 cwd）；非空才解码校验。
	var args gitStatusArgs
	if len(strings.TrimSpace(string(intent.Arguments))) > 0 {
		if err := decodeArgs(toolNameGitStatus, intent.Arguments, &args); err != nil {
			return contracts.ToolResult{}, err
		}
	}
	dir := strings.TrimSpace(args.Path)
	if dir == "" {
		// path 空 → 用会话工作区（L2 per-session 执行上下文 · 件 1）：与 bash 的工作目录
		// 同源，"模型在哪个目录里干活"三处（read/write 的相对路径、bash 的 cwd、git 的
		// 探测目录）说的是同一件事。工作区也空 → 回落进程 cwd（现状）。
		dir = workspaceFromCtx(ctx)
	}

	p, err := e.probeGitStatus(ctx, dir)
	if err != nil {
		return contracts.ToolResult{}, err
	}

	var b strings.Builder
	fmt.Fprintf(&b, "branch: %s\n", p.branch)
	if p.dirty {
		b.WriteString("dirty: true\n")
	} else {
		b.WriteString("dirty: false\n")
	}
	fmt.Fprintf(&b, "last_commit: %s\n", p.last)
	return resultText(b.String()), nil
}

// probeGitStatus 探测一次 git 状态（工具 Execute 与 SnapshotGitStatus 共享的核心逻辑）。
//
// lookPath / run 走 e 的注入点（nil = 真实 os/exec）；错误文案是**模型可自我修正**的
// 形态（M5）——点名 git / PATH / git 仓库，而不是透底层 exec 报错。
func (e *GitStatusExecutor) probeGitStatus(ctx context.Context, dir string) (gitProbe, error) {
	// git 可用性检查（git 不可用 → 明确、可自我修正的错误，M5）。
	look := e.lookPath
	if look == nil {
		look = exec.LookPath
	}
	if _, err := look("git"); err != nil {
		return gitProbe{}, fmt.Errorf(
			"%s: 系统找不到 git 可执行文件（%v）；请先安装 git，或确认它在 PATH 里", toolNameGitStatus, err)
	}

	run := e.run
	if run == nil {
		run = defaultGitRun
	}

	branch, err := gitTrim(ctx, run, dir, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return gitProbe{}, fmt.Errorf(
			"%s: 读取当前分支失败（目录 %q 不是 git 仓库或 git 报错：%v）；请确认 path 指向一个 git 仓库根目录",
			toolNameGitStatus, dirOrCWD(dir), err)
	}
	status, err := gitTrim(ctx, run, dir, "status", "--porcelain")
	if err != nil {
		return gitProbe{}, fmt.Errorf(
			"%s: 读取工作区状态失败（%v）；请确认 path 指向一个 git 仓库根目录", toolNameGitStatus, err)
	}
	last, err := gitTrim(ctx, run, dir, "log", "-1", "--pretty=format:%h %s (%cr, %an)")
	if err != nil {
		// 空仓库（无提交）时 git log 会失败：这不是“路径错”，给出可读收场。
		last = "（无提交）"
	}
	return gitProbe{branch: branch, dirty: status != "", last: last}, nil
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
