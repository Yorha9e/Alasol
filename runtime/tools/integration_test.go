package tools

import (
	"runtime"
	"testing"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/cap/local"
)

// 本文件是 tools × cap/local 的**跨包接线**测试：证明内置工具与真实本地能力
// 拼起来能跑（上层 loop 拿到的就是这两半）。fake 替身在 tools_test.go 里，
// 那边保证工具自身逻辑，这里保证接线。

// newLocalFS 返回真实的本地文件系统能力。
func newLocalFS(_ *testing.T) core.FSCapability { return local.FS{} }

// newLocalExec 返回真实的本地执行能力。
func newLocalExec() core.ExecCapability { return local.Exec{} }

// echoCommandForTest 是一条跨平台可跑的 echo 命令（bash 工具会再包一层 shell）。
func echoCommandForTest() string {
	// bash -c 'echo hi' 与 cmd /c echo hi 都能用同一句。
	return "echo integration-ok"
}

// TestLocalShellSelection 记录本平台实际选用的 shell（Windows 与 Unix 分叉点）。
func TestLocalShellSelection(t *testing.T) {
	t.Logf("GOOS=%s → bash 工具经 %s %s", runtime.GOOS, shellName, shellFlag)
	if runtime.GOOS == "windows" {
		if shellName != "cmd" || shellFlag != "/c" {
			t.Errorf("Windows 应用 cmd /c，实际 %s %s", shellName, shellFlag)
		}
		return
	}
	if shellName != "bash" || shellFlag != "-c" {
		t.Errorf("非 Windows 应用 bash -c，实际 %s %s", shellName, shellFlag)
	}
}
