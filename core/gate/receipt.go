package gate

import (
	"errors"
	"fmt"
	"time"
)

// Receipt 是门禁一次运行的事实记录（V2 四要素 + V5 耗时/日志截断）。
//
// 由执行侧（runtime/gate.Run）填充，判定侧（Verify / Judge）只读它。
// 退出码非零**不是 error**（是事实，V2①）；基础设施失败（起不来、ctx 取消）才让 Run 返回 error。
type Receipt struct {
	// Commit 是快照创建时的 HEAD（声称在哪个 commit 上跑门禁，V2② 的被检方）。
	Commit string
	// ObservedCommit 是命令跑完后再次 rev-parse HEAD 的观测值（V2③）：
	// 跑期间若 HEAD 移动（命令自己 checkout / 外部并发推 commit），它会 ≠ Commit。
	ObservedCommit string
	// Command 是实际跑的不透明 shell 命令（V2④ 的被检方）。
	Command string
	// ExitCode 是门禁命令退出码（事实；0 = 命令自称通过，V2①）。
	ExitCode int
	// StartedAt 是命令起始时刻。
	StartedAt time.Time
	// ElapsedMS 是命令耗时毫秒（V5：必记，复盘不靠猜）。
	ElapsedMS int64
	// LogTail 是合并（stdout+stderr）日志的尾部，≤64KB（V5：日志必须设上限）。
	LogTail string
	// Failures 是从 LogTail 提取的规范化失败条目（ExtractFailures）；退出码为 0 时为空。
	// Judge 据此与 baseline 比对，只拦新增失败（W9c）。
	Failures []string
	// FailuresExtractionEmpty = true 表示：命令**非零退出**，但输出里**没有可归一化的
	// 失败条目**（静默失败 / OOM kill / 崩溃无输出）。
	//
	// 显式标注（不靠"Failures 为空 = 未知"隐式推断）：判定侧据此把这次失败按**未知失败**
	// 处理——正常模式 fail-close 拦截（空失败列表不得被当成"没有新增失败"而放行），
	// 记录模式照常建基线（红仓可建基线，不落陷阱 20 死锁）。
	FailuresExtractionEmpty bool
}

// VerifyRequest 是 Verify 的输入：判定时刻的**当前** tip 与**当前**配置。
type VerifyRequest struct {
	TipCommit string
	Config    Config
}

// Verify 的四条哨兵错误（errors.Is 可判别，供装配层分支：见包 doc 的组合语义）。
var (
	// ErrCommitMismatch：receipt.Commit ≠ 当前 tip——跑的不是当前 commit（V2②）。
	ErrCommitMismatch = errors.New("gate: receipt.commit 与当前 tip 不一致（跑的不是当前 commit）")
	// ErrObservedMismatch：receipt.ObservedCommit ≠ receipt.Commit——跑期间 HEAD 移动了（V2③）。
	ErrObservedMismatch = errors.New("gate: 观测 commit 与声称 commit 不一致（跑期间 HEAD 移动）")
	// ErrCommandMismatch：receipt.Command ≠ 当前配置命令——配置在跑前/后被改了（V2④）。
	ErrCommandMismatch = errors.New("gate: 命令与当前配置不一致（配置已改）")
	// ErrExitNonZero：退出码非零——命令自称未通过（V2①）。
	ErrExitNonZero = errors.New("gate: 退出码非零，门禁命令未通过")
)

// Verify 做 V2 四查，返回首个失败（nil = 干净通过）。
//
// 四查：① ExitCode==0 ② receipt.Commit == TipCommit ③ receipt.ObservedCommit ==
// receipt.Commit ④ receipt.Command == Config.Command。
//
// **评估顺序是先可信度（②③④）后成败（①）**，这是 fail-close 的组合需要：
// 过期/错配的 receipt（跑的是旧 commit、命令与当前配置不符、跑期间 HEAD 移动）
// 根本无法信任，baseline 也救不了 → 必须硬拒绝；只有"可信但失败"（仅①不过，
// 即 errors.Is(err, ErrExitNonZero)）才应转去 Judge 走 baseline 路径。若先查①，
// 一个过期 receipt 会在可信度未知时就因退出码被分流，等于对不可信事实做判定。
//
// 因此装配层的组合是：
//
//	if err := Verify(r, req); err == nil { 放行 }
//	else if errors.Is(err, ErrExitNonZero) { verdict := Judge(r, baseline); ... }
//	else { 硬拒绝 }
func Verify(receipt *Receipt, req VerifyRequest) error {
	if receipt == nil {
		return errors.New("gate: receipt 为 nil")
	}
	// ② commit == 当前 tip（跑的是不是现在这一版）。
	if receipt.Commit != req.TipCommit {
		return fmt.Errorf("%w（receipt=%s，tip=%s）", ErrCommitMismatch, shortCommit(receipt.Commit), shortCommit(req.TipCommit))
	}
	// ③ observed == 声称的 commit（跑期间 HEAD 没被动过）。
	if receipt.ObservedCommit != receipt.Commit {
		return fmt.Errorf("%w（observed=%s，commit=%s）", ErrObservedMismatch, shortCommit(receipt.ObservedCommit), shortCommit(receipt.Commit))
	}
	// ④ command == 当前配置（跑的是当前配置的命令）。
	if receipt.Command != req.Config.Command {
		return fmt.Errorf("%w（receipt=%q，config=%q）", ErrCommandMismatch, receipt.Command, req.Config.Command)
	}
	// ① exit == 0（命令自称通过）。放在最后：只有可信的 receipt 才谈得上"成败"。
	if receipt.ExitCode != 0 {
		return fmt.Errorf("%w（exit=%d）", ErrExitNonZero, receipt.ExitCode)
	}
	return nil
}

// shortCommit 把 commit 截短到 12 字符供错误信息展示（空串原样返回）。
func shortCommit(c string) string {
	if len(c) <= 12 {
		return c
	}
	return c[:12]
}
