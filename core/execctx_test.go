package core

import (
	"context"
	"testing"
)

// 本文件是 L2 per-session 执行上下文（件 1）的 core 侧钉子：ctx 缝的往返与 nil 安全。
//
// 钉子：把 WithExecContext / ExecContextFrom 改成"写进去读不出来"或"nil ctx 直接 panic"
// → 本文件红。

// TestExecContextRoundTrip 往返：注进去的执行作用域原样取回来。
func TestExecContextRoundTrip(t *testing.T) {
	ctx := WithExecContext(context.Background(), ExecContext{Workspace: `D:\ws\repo`})
	ec, ok := ExecContextFrom(ctx)
	if !ok {
		t.Fatal("ExecContextFrom 应取得到（ok=true）")
	}
	if ec.Workspace != `D:\ws\repo` {
		t.Errorf("Workspace = %q，期望原样往返", ec.Workspace)
	}
}

// TestExecContextNotInjected 未注入时取不到（ok=false）——调用方据此走"不锚定"的现状路径。
func TestExecContextNotInjected(t *testing.T) {
	ec, ok := ExecContextFrom(context.Background())
	if ok {
		t.Error("没注入过时应 ok=false")
	}
	if ec.Workspace != "" {
		t.Errorf("Workspace = %q，期望零值", ec.Workspace)
	}
}

// TestExecContextEmptyWorkspace 注入空工作区 = 显式的"不锚定"（不是"没注入"）：
// 两者在工具侧是同一条路径（回落进程 cwd），但**语义不同**（一个是配置了空值，
// 一个是没配），取值方要能分辨——所以 ok 必须是 true。
func TestExecContextEmptyWorkspace(t *testing.T) {
	ctx := WithExecContext(context.Background(), ExecContext{})
	ec, ok := ExecContextFrom(ctx)
	if !ok {
		t.Fatal("注入过的 ctx 应 ok=true（哪怕工作区是空串）")
	}
	if ec.Workspace != "" {
		t.Errorf("Workspace = %q，期望空串", ec.Workspace)
	}
}

// TestExecContextNilCtxSafe nil ctx 不 panic：取不到（ok=false），注入时按 Background 兜底。
func TestExecContextNilCtxSafe(t *testing.T) {
	ec, ok := ExecContextFrom(nil) //nolint:staticcheck // 故意传 nil：nil 安全是本用例的考点
	if ok {
		t.Error("nil ctx 应 ok=false")
	}
	if ec.Workspace != "" {
		t.Errorf("Workspace = %q，期望零值", ec.Workspace)
	}
	ctx := WithExecContext(nil, ExecContext{Workspace: "ws"}) //nolint:staticcheck // 同上
	got, ok := ExecContextFrom(ctx)
	if !ok || got.Workspace != "ws" {
		t.Errorf("nil ctx 注入后 = %+v ok=%v，期望拿到 ws", got, ok)
	}
}

// TestExecContextDoesNotCollideWithEnv 执行作用域与 env 注入**互不干扰**：
// 两者是平行新增（不并进 EnvScope），各挂各的键——挂错键会让工具读到对方的值
// （工作区被当凭据脱敏掉，或 env 键名撞车）。
func TestExecContextDoesNotCollideWithEnv(t *testing.T) {
	ctx := WithEnv(context.Background(), EnvScope{"WORKSPACE": "not-a-credential"})
	ctx = WithExecContext(ctx, ExecContext{Workspace: `D:\ws`})

	ec, ok := ExecContextFrom(ctx)
	if !ok || ec.Workspace != `D:\ws` {
		t.Errorf("执行作用域 = %+v ok=%v，期望 ws", ec, ok)
	}
	env, ok := EnvFrom(ctx)
	if !ok || env["WORKSPACE"] != "not-a-credential" {
		t.Errorf("env = %v ok=%v，期望原样保留", env, ok)
	}
}

// TestExecContextInnerShadowing 后注入的覆盖先注入的（同键语义 = 覆盖，与 ctx 惯例一致）：
// loop 的两个执行点都是"在已有 ctx 上再叠一层"，叠出来的值必须是最后一次注入的。
func TestExecContextInnerShadowing(t *testing.T) {
	ctx := WithExecContext(context.Background(), ExecContext{Workspace: "outer"})
	ctx = WithExecContext(ctx, ExecContext{Workspace: "inner"})
	ec, _ := ExecContextFrom(ctx)
	if ec.Workspace != "inner" {
		t.Errorf("Workspace = %q，期望 inner（后注入的覆盖先注入的）", ec.Workspace)
	}
}
