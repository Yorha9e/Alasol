//go:build windows

package sandbox

import (
	"testing"
)

// 本文件是 WER 记账的**包内**测试（F7/N1）。
//
// 为什么放包内：F7 的行为要在"用户本来就有的排除项"上验证——那需要看真实的
// 注册表布局与真实的 WerAdd/WerRemove 行为。
//
// # 纪律（N1）
//
//   - **用真的 API** 预置"用户原有项"（WerAddExcludedApplication），不手工写
//     注册表造假世界——手工写父键的 REG_MULTI_SZ 曾经让这个测试"自证无效"
//     （断言的是我们以为的布局，而真机布局是子键的值名）；
//   - **只动自己的探针名**，不覆盖整张表（用户的排除项不能被测试清掉）。

// werProbeName 是测试用的探针名（不会与真实程序重名；收尾时只删它自己）。
const werProbeName = "alasol-sandbox-wer-probe.exe"

// TestWerBookkeepingKeepsUserEntries 钉死 F7/N1：
// **用户原有的排除项不能被我们删掉**。
//
// 做法：用真的 API（WerAddExcludedApplication）预置一个"用户原有项"，
// 然后让沙箱去 add 它——记账表里**不能**出现它（出现了 Close 就会删用户的设置）。
func TestWerBookkeepingKeepsUserEntries(t *testing.T) {
	// 预置：模拟"用户自己早就排除了它"（真 API）。
	if err := werExclude(werProbeName); err != nil {
		t.Fatalf("预置用户排除项（WerAddExcludedApplication）: %v", err)
	}
	defer func() { _ = werUnexclude(werProbeName) }() // 只删自己的探针名

	if !werAlreadyExcluded(werProbeName) {
		t.Fatal("werAlreadyExcluded 没认出预置的排除项——查错注册表位置了（N1）")
	}

	// 沙箱去 add 它：应该"发现已在表里 → 不动它，也不记账"。
	w := newWerBookkeeping()
	if err := w.add(werProbeName); err != nil {
		t.Fatalf("add: %v", err)
	}
	if w.has(werProbeName) {
		t.Error("用户原有的排除项被我们记账了——Close 会把用户的设置删掉（F7/N1）")
	}
	// Close 语义：removeAll 之后，用户的排除项必须还在。
	w.removeAll()
	if !werAlreadyExcluded(werProbeName) {
		t.Error("Close 之后用户原有的排除项不见了——我们删了它（F7/N1）")
	}
}

// TestWerBookkeepingRemovesOwnEntries 是上一条的对照：**我们自己加的要删掉**。
//
// （顺带验证 N1 的另一半：真 API 写进去的位置能被我们读出来、能被我们删掉。）
func TestWerBookkeepingRemovesOwnEntries(t *testing.T) {
	// 先确保探针不在表里（只删自己的名字，不碰别的）。
	_ = werUnexclude(werProbeName)

	w := newWerBookkeeping()
	if err := w.add(werProbeName); err != nil {
		t.Fatalf("add: %v", err)
	}
	if !w.has(werProbeName) {
		t.Fatal("我们自己加的排除项没记账——Close 撤不掉（注册表会越积越多）")
	}
	if !werAlreadyExcluded(werProbeName) {
		t.Fatal("刚加的排除项读不出来——注册表位置判断又错了（N1）")
	}
	w.removeAll()
	if werAlreadyExcluded(werProbeName) {
		t.Error("Close 之后我们自己加的排除项还在——WER 排除项会永久累积在用户机器上")
	}
}
