package openai

// 跨包断言：流事件 kind 的**唯一事实源是 core**（core.ProviderEvent.Kind 的 v0 子集）。
//
// 为什么要有这条：本包与 core/loop 曾经**各写一份字面量**（"text.delta" /
// "reasoning.delta"）——改了一边忘了另一边，症状是"loop 收不到 provider 发的事件"
// （静默的：事件被 switch 丢掉，没有任何报错）。现在两处都引用 core.Kind*，
// 本文件把这条不变量钉住：**任何一边改了 kind 值而另一边没跟上，这里就会红**
// （值一致 = 同一份事实源）。

import (
	"testing"

	"github.com/Yorha9e/alasol/core"
)

func TestKindConstantsComeFromCore(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"KindTextDelta", KindTextDelta, core.KindTextDelta},
		{"KindReasoningDelta", KindReasoningDelta, core.KindReasoningDelta},
		{"KindToolCallStart", KindToolCallStart, core.KindToolCallStart},
		{"KindToolCallDelta", KindToolCallDelta, core.KindToolCallDelta},
		{"KindDone", KindDone, core.KindDone},
		{"KindError", KindError, core.KindError},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s = %q，want %q（kind 常量的唯一事实源是 core）", tc.name, tc.got, tc.want)
		}
	}
}
