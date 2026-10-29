//go:build windows

package sandbox

import "golang.org/x/sys/windows"

// 测试专用导出（x/sys 没包 GetErrorMode，测试要断言 V10 落地就得露一个口子）。
//
// 放在 internal test file 里而不是产品代码：产品代码不需要读 error mode，
// 而测试需要。命名带 ForTest 后缀，明确它不是产品 API。
func CurrentErrorModeForTest() uint32 { return currentErrorMode() }

// SetErrorModeForTest 设置进程 error mode，返回**旧值**（测试专用导出）。
//
// 用途：验证"原值恰好是 0"的场景（F5/N7）——那时候不能用 `prevErrorMode != 0`
// 当复原判据（那会让 Close 跳过复原，把压制位永久留在宿主进程上）。
func SetErrorModeForTest(mode uint32) uint32 { return windows.SetErrorMode(mode) }

// SetKillTreeFallbackForTest 开关 taskkill 兜底（仅测试用）。
//
// 为什么需要（F22）：沙箱里"子进程被杀掉"有两条路径——ctx 取消时的 taskkill 兜底，
// 与 job object 的 KILL_ON_JOB_CLOSE。要证明"是 job object 收口的"，必须先把兜底
// 关掉，否则两条路径效果一样、证明不了任何事。
func SetKillTreeFallbackForTest(enabled bool) { killTreeFallbackEnabled = enabled }

// WerExcludeForTest / WerUnexcludeForTest / WerAlreadyExcludedForTest 是 WER
// 排除表的测试专用导出（仅测试用）。
//
// 为什么需要（F7/N1）：端到端测试要验证"**用户自己早就排除过的**可执行名，
// 沙箱 Close 之后必须还在"——这需要用**真的 API**（WerAddExcludedApplication）
// 预置一条，不能手工写注册表造假世界（之前的测试就是这么写的，结果断言的是
// 我们以为的布局，而真机布局是子键的值名）。
func WerExcludeForTest(exeName string) error        { return werExclude(exeName) }
func WerUnexcludeForTest(exeName string) error      { return werUnexclude(exeName) }
func WerAlreadyExcludedForTest(exeName string) bool { return werAlreadyExcluded(exeName) }
