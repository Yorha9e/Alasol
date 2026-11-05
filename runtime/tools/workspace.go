package tools

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/Yorha9e/alasol/core"
)

// 本文件是 L2 per-session 执行上下文（件 1）的**工具侧解析面**：工具从 ctx 里取本次的
// 会话工作区（core.ExecContext），据此把**相对路径**锚到工作区根下、给子进程定工作目录。
//
// # 三条口径（读/写/bash/git_status 共用同一份判断，别各写一遍）
//
//  1. **相对路径** → 锚到工作区根：filepath.Join(workspace, p) 解析成工作区根下的路径；
//     解析后若**越出**工作区根（filepath.Rel 出来是 ".." 或以 "..\" / "../" 开头）→ **拒绝**
//     （工具失败，模型能读到为什么、改成工作区内的路径重试）。
//  2. **绝对路径** → **原样透传**，不新增限制。这一刀**不是安全刀**：真正的隔离归沙箱
//     （runtime/cap/sandbox 的 ACL + 受限 token 是内核强制的），工具层的这条边界只是
//     "别让模型手滑把相对路径写到工作区外面"，挡不住故意越界，也不该被当成安全边界。
//  3. **工作区为空** → **不锚定**，零行为变化：相对路径按进程 cwd 解析、bash 继承进程
//     cwd、git_status 看进程 cwd（与这一刀之前逐字节一致）。
//
// # 越界判定为什么是"词法"而不是"真实路径"
//
// 判定用 filepath.Join + filepath.Rel（纯词法），**不解析符号链接 / junction**——
// 与沙箱的允许集软检查同口径（runtime/cap/sandbox/policy.go 的 pathKey：不解析符号链接，
// 因为 EvalSymlinks 在 Windows 上不解析 junction，逐组件 Readlink 才是正解，见 S13）。
// 词法口径的已知边界：工作区里放一个指向外部的链接/junction，可以被绕过去。真隔离靠
// 内核（ACL 对真实路径生效），不靠这里的字符串比较——**别把这条当安全边界**。

// workspaceFromCtx 取本次调用的会话工作区根（空 = 不锚定，走现状路径）。
//
// ctx 里没注入过执行作用域（老装配 / 直接调工具）→ 返回空串，调用方按"不锚定"处理。
func workspaceFromCtx(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	ec, ok := core.ExecContextFrom(ctx)
	if !ok {
		return ""
	}
	return strings.TrimSpace(ec.Workspace)
}

// resolveWorkspacePath 把工具参数里的路径按会话工作区解析成要下沉给能力面的路径。
//
// 返回值是**能力面看到的那个路径**：工作区为空 / 路径为绝对 → 原样透传；否则是
// **解析后的绝对路径**（锚在工作区根下）。
//
// 越界时返回错误（带工具名 + 原路径 + 解析结果 + 工作区，模型能据此自我修正，M5）。
func resolveWorkspacePath(ctx context.Context, tool, p string) (string, error) {
	ws := workspaceFromCtx(ctx)
	if ws == "" {
		return p, nil // 不锚定：零行为变化（相对路径按进程 cwd）
	}
	if filepath.IsAbs(p) {
		return p, nil // 绝对路径原样透传（这刀不是安全刀，见文件头注释）
	}
	// 工作区根取绝对形态：解析结果要**绝对路径**下沉给能力面（相对工作区按进程 cwd 解释），
	// 且"越界判定"与"解析"必须用**同一个**根（一个绝对一个相对 → Rel 算不出来 → 误判）。
	base := absRoot(ws)
	resolved := filepath.Join(base, p)
	if !withinWorkspace(base, resolved) {
		return "", fmt.Errorf(
			"%s: 相对路径越出工作区根（路径 %q 解析到 %q，超出工作区 %q）；请改用工作区内的相对路径",
			tool, p, resolved, base)
	}
	return resolved, nil
}

// absRoot 取工作区根的绝对形态（已是绝对路径时只做 Clean，不改语义）。
//
// 拿不到进程 cwd（极少见：cwd 被删）→ 退回 Clean 后的原值——宁可路径是相对的，
// 也不要因为一次 cwd 查询失败就把整次调用判死。
func absRoot(ws string) string {
	if abs, err := filepath.Abs(ws); err == nil {
		return abs
	}
	return filepath.Clean(ws)
}

// withinWorkspace 报告 resolved 是否落在 workspace 根之下（含根自身）。
//
// 判据：filepath.Rel(workspace, resolved) 不是 ".."、也不以 "..\" / "../" 开头
// （词法口径；大小写与分隔符按 filepath 族在当前平台的现实处理——Windows 上 filepath
// 的路径比较是大小写不敏感的）。
//
// 为什么不是"裸 startsWith("..")"：那会把工作区里名为 "..foo" 的**合法文件**也拒了
// （误伤比漏判更难受：模型反复换名字也写不进去）。真正的越界只有 ".." 与 "../" 两种形态。
//
// Rel 算不出相对路径时（不同卷、混合绝对/相对）→ 判为**不在根下**（fail-close：
// 算不明白就不放行）。
func withinWorkspace(workspace, resolved string) bool {
	rel, err := filepath.Rel(workspace, resolved)
	if err != nil {
		return false
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}
	return true
}
