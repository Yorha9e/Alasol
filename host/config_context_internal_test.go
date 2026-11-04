package host

// G-h 的 **生效 kind 解析**（内部单测）：session 折叠的 kind 与基线 kind（宿主默认 /
// 工作流声明）怎么合成一个"本轮生效的 kind"。
//
// 口径：
//
//	session 覆盖（非空）  → 用它（能把 append 提成 tiered）；
//	session 显式空串     → 清除回默认 → 落回基线 kind；
//	session 没写过       → 基线 kind；
//	非 tiered 的 kind    → 一律按 append（宿主只实现 tiered，其余保留未实现）。

import (
	"testing"

	"github.com/Yorha9e/alasol/core/sessioncfg"
)

func TestResolveContextKind(t *testing.T) {
	cases := []struct {
		name     string
		foldKind sessioncfg.Field
		baseKind string
		want     string
	}{
		{"没写过 + 基线 tiered", sessioncfg.Field{}, "tiered", "tiered"},
		{"没写过 + 基线空（append）", sessioncfg.Field{}, "", ""},
		{"显式覆盖 tiered + 基线空", sessioncfg.Field{Written: true, Value: "tiered"}, "", "tiered"},
		{"显式清空（空串）+ 基线 tiered", sessioncfg.Field{Written: true, Value: ""}, "tiered", "tiered"},
		{"显式清空（空串）+ 基线空", sessioncfg.Field{Written: true, Value: ""}, "", ""},
		{"宿主保留未实现的 kind → append", sessioncfg.Field{}, "stage_scoped", ""},
	}
	for _, tc := range cases {
		got := resolveContextKind(tc.foldKind, tc.baseKind)
		if got != tc.want {
			t.Errorf("%s: resolveContextKind = %q，want %q", tc.name, got, tc.want)
		}
	}
}
