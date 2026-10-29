package gateexec

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/core/gate"
)

// maxLogTail 是 LogTail 的上限（64KB，V5：CI 日志文件必须设上限）。
const maxLogTail = 64 << 10

// 约定注入的环境变量（V3 留白 + 约定注入：快照只隔离文件系统，端口/缓存等不在问题域内，
// 靠这几个变量让命令自隔离）。
const (
	envWorktree  = "CI_WORKTREE"  // 快照目录
	envCommit    = "CI_COMMIT"    // 快照 commit
	envPortBase  = "CI_PORT_BASE" // 端口基址（命令在此之上派生端口，避免并行快照撞端口）
	envCacheKey  = "CI_CACHE_KEY" // 依赖缓存键（V4：仓库作用域 + 清单内容的内容寻址哈希）
	envCacheDir  = "CI_CACHE_DIR" // 该键对应的共享缓存目录（V4：框架只派生路径与键，命令自暖；目录可能不存在）
	portBaseLow  = 40000          // 端口基址区间下界
	portBaseSpan = 20000          // 端口基址区间跨度 → [40000, 60000)
)

// Run 在快照目录 dir 里经 shell 跑不透明命令 command，产出 Receipt 事实（W8/W9/V1/V3/V4/V5）。
//
// 用**默认**依赖缓存配置（默认清单集 + 本机默认缓存根）；要自定义清单集 / 缓存根用 RunWith。
func Run(ctx context.Context, dir, command string) (gate.Receipt, error) {
	return RunWith(ctx, dir, command, gate.CacheConfig{})
}

// RunWith 是 Run 的可配置版：cache 控制 V4 依赖缓存的清单集与缓存根（零值 = 默认）。
//
//   - 命令**经 shell**（Windows cmd /c、其他 bash -c）——这是全包唯一走 shell 的地方，
//     因为 W9 明确命令是不透明 shell 字符串，引擎不解析；
//   - 注入约定 env：V3 三个（CI_WORKTREE / CI_COMMIT / CI_PORT_BASE）+ V4 两个
//     （CI_CACHE_KEY / CI_CACHE_DIR，内容寻址共享只读缓存）；
//   - stdout/stderr **合并**捕获，LogTail 只留尾部 ≤64KB，ElapsedMS 必记（V5）；
//   - 退出码非零**不是 error**（事实）：Receipt.ExitCode 带上，err 为 nil；
//   - 命令跑完再 rev-parse HEAD 得 ObservedCommit（V2③：跑期间 HEAD 移动会被 core/gate 四查③抓住）；
//   - 基础设施失败（ctx 取消、shell 起不来）才返回 error（连同已收集的 Receipt）。
//
// command 为空 = 编程错误（配置侧应已保证非空，W9），直接报错不跑。
func RunWith(ctx context.Context, dir, command string, cache gate.CacheConfig) (gate.Receipt, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return gate.Receipt{}, err
	}
	if strings.TrimSpace(command) == "" {
		return gate.Receipt{}, errors.New("gateexec: command 为空（W9：命令靠注入，无默认）")
	}

	// 快照 commit：从快照目录读 HEAD（Snapshot 已把指定 commit 检出为 detached HEAD）。
	commit, err := revParse(ctx, dir, "HEAD")
	if err != nil {
		return gate.Receipt{}, fmt.Errorf("gateexec: 读取快照 HEAD 失败：%w", err)
	}

	// V4 依赖缓存：从快照读清单 → 内容寻址键 + 缓存目录（框架只派生路径与键，命令自暖）。
	cacheKey, cacheDir := deriveCache(dir, cache)

	started := time.Now()
	// 注入约定 env：在继承的父 env 之上追加（V3 三个 + V4 两个）。
	env := append(os.Environ(),
		envWorktree+"="+dir,
		envCommit+"="+commit,
		envPortBase+"="+strconv.Itoa(portBaseLow+rand.Intn(portBaseSpan)),
		envCacheKey+"="+cacheKey,
		envCacheDir+"="+cacheDir,
	)
	argv := []string{shellName, shellFlag, command}
	output, code, runErr := runProcess(ctx, dir, env, argv)
	elapsed := time.Since(started)

	receipt := gate.Receipt{
		Commit:    commit,
		Command:   command,
		ExitCode:  code,
		StartedAt: started,
		ElapsedMS: elapsed.Milliseconds(),
		LogTail:   output, // runProcess 的 tailBuffer 已保证 ≤ maxLogTail
	}
	if code != 0 {
		receipt.Failures = gate.ExtractFailures(output)
		// 静默失败**显式标注**：非零退出但输出里没有可归一化的失败条目
		//（OOM kill / 崩溃无输出 / 命令自己静默退出）。判定侧据此按未知失败
		// fail-close 处理——空失败列表绝不能被当成"没有新增失败"而放行。
		receipt.FailuresExtractionEmpty = len(receipt.Failures) == 0
	}

	// 命令**正常结束**（未被取消）才复观测 HEAD；被取消时留空 → core/gate 四查③ 会拒（fail-close）。
	if ctx.Err() == nil {
		if observed, oerr := revParse(ctx, dir, "HEAD"); oerr == nil {
			receipt.ObservedCommit = observed
		}
	}

	if runErr != nil {
		// 基础设施失败（ctx 取消 / shell 起不来）→ 返回 error（连同已收集的事实）。
		return receipt, runErr
	}
	// 非零退出码不是 error（事实已进 receipt）。
	return receipt, nil
}
