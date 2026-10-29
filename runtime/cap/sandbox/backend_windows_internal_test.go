//go:build windows

package sandbox

import (
	"context"
	"testing"

	"github.com/Yorha9e/alasol/core"
)

// 本文件是 Windows 后端的**包内**测试（需要看未导出的构造与字段）。

// TestWindowsBackendSuspendsForJob 钉死 F13：有 job object 时，子进程必须以
// **挂起**状态创建（CREATE_SUSPENDED），等纳入 job 之后才恢复执行。
//
// 为什么重要：不挂起的话，"进程已经跑起来、还没进 job"那个窗口里它能 fork 出
// 脱离 job 的后代——进程树收容就漏了。
func TestWindowsBackendSuspendsForJob(t *testing.T) {
	p := DefaultPolicy()
	p.AllowedRoots = []string{t.TempDir()}
	b, err := newWindowsBackend(p)
	if err != nil {
		t.Fatalf("newWindowsBackend: %v", err)
	}
	defer b.Close()

	exec, err := b.Begin(context.Background(), core.ExecSpec{Name: "cmd"})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if !exec.Spawn().Suspend {
		t.Error("Begin 没要求挂起创建——Attach 窗口没关掉（F13）")
	}

	// 关掉 job object → 就不需要挂起（没有约束要施加）。
	p2 := DefaultPolicy()
	p2.AllowedRoots = []string{t.TempDir()}
	p2.UseJobObject = false
	b2, err := newWindowsBackend(p2)
	if err != nil {
		t.Fatalf("newWindowsBackend: %v", err)
	}
	defer b2.Close()
	exec2, err := b2.Begin(context.Background(), core.ExecSpec{Name: "cmd"})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if exec2.Spawn().Suspend {
		t.Error("没有 job object 时仍然要求挂起——进程会永远不跑")
	}
}

// TestEffectiveDangerousPrivileges 钉死 F17：nil = 默认名单；
// **非 nil 的空切片 = 一个特权都不删**（"显式表达不删"）。
func TestEffectiveDangerousPrivileges(t *testing.T) {
	def := effectiveDangerousPrivileges(nil)
	if len(def) == 0 {
		t.Error("nil 应该解析成默认特权名单")
	}
	empty := effectiveDangerousPrivileges([]string{})
	if len(empty) != 0 {
		t.Errorf("非 nil 的空切片应该解析成\"一个都不删\"，得到 %d 条", len(empty))
	}
	own := effectiveDangerousPrivileges([]string{"SeDebugPrivilege"})
	if len(own) != 1 || own[0] != "SeDebugPrivilege" {
		t.Errorf("自定义名单被改动了：%v", own)
	}
}
