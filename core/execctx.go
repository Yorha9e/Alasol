package core

import "context"

// ExecContext 是**一次工具调用的执行作用域**（L2 · per-session 执行上下文 · 件 1）。
//
// # 它解决什么
//
// 工具执行环境原先是"**注册时绑定**"：宿主在装配期造一套执行器（FS / Exec）注册进
// 注册表，所有 session 共用。这一刀把"本次在哪个工作区跑"从**注册期**挪到**调用期**：
// 执行器仍然只注册一份，但每次工具调用的 ctx 上都带着本次的执行作用域——工具侧据此
// 解析相对路径、给子进程定工作目录。
//
// # 为什么**不并进** EnvScope（core/env.go）
//
// 两者语义正交，混在一处会各自变味：
//
//   - EnvScope 是**凭据语义**：键值是凭据或普通配置，值**永不进 prompt / 事件 / 日志**
//     （H12/H19），并且要过凭据分类（IsCredentialKey / IsCredentialValue）与脱敏收口；
//   - ExecContext 是**路径语义**：工作区根是**公开事实**（不是秘密），要进工具描述、
//     进错误文案、给模型看——它不需要脱敏，也**不该**被 ScrubEnv 那套规则扫一遍
//     （凭据键名判定会把 WORKSPACE=… 这类普通键当凭据剥掉）。
//
// 把工作区塞进 EnvScope 的具体坏处：EnvScope 的值进子进程 env（bash 工具把注入的键
// 合并进子进程 env），工作区不是环境变量、也不该被子进程继承——塞进去等于给每个子进程
// 多塞一个没用的变量；而从 EnvScope 里取工作区还要先绕过"凭据不许进事件"的纪律，
// 让取值处变成特例。所以：**平行新增**（与 env.go 同款的 ctx 键值对），互不干涉。
//
// # 取值口径
//
// Workspace 为空 = **不锚定**：工具按现状行为走（相对路径按进程 cwd、bash 继承进程 cwd）。
// 这样"没配工作区"与"配了工作区"是同一条代码路径上的两个取值，不需要两套分支。
type ExecContext struct {
	// Workspace 是本次执行的工作区根（会话工作区目录）。
	//
	// 约定：调用方给什么就带什么（core 不做路径语义判断——那属于工具侧与平台现实），
	// 空串 = 不锚定。工具侧据此把**相对路径**锚到工作区根下（并挡掉越界），
	// 并把 bash / git 的工作目录定到工作区。
	Workspace string
}

type execCtxKey struct{}

// WithExecContext 把本次执行的执行作用域放进 ctx（工具执行侧用）。
//
// 与 core.WithEnv 同款模式：**只经 ctx 传**，不进 prompt / 事件 payload / 日志。
// ctx 为 nil 时按 context.Background() 兜底（不 panic）——取执行作用域是最外层的
// 观测动作，不该把调用方的编程错误变成 panic。
func WithExecContext(ctx context.Context, ec ExecContext) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, execCtxKey{}, ec)
}

// ExecContextFrom 从 ctx 取出执行作用域（工具执行侧用）。
//
// 取不到（ctx 为 nil / 没注入过）→ (零值, false)：调用方据此走"不锚定"的现状路径。
func ExecContextFrom(ctx context.Context) (ExecContext, bool) {
	if ctx == nil {
		return ExecContext{}, false
	}
	ec, ok := ctx.Value(execCtxKey{}).(ExecContext)
	return ec, ok
}
