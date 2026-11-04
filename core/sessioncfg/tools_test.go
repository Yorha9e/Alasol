package sessioncfg_test

// core/sessioncfg 的 **tools 白名单组**（G-e）折叠口径：
//
//	① **整组 LWW**：tools 组是 fold 表里的**一个字段**（与 model 等标量字段同口径）——
//	   同一次提交的 only/except 一起落，后写**整组**覆盖前一组，**不拆子字段跨事件合并**；
//	② **组三态**：组缺失 = 整组不变；组出现且两子项都缺（tools:{}）= 清除回默认；
//	③ **子项三态**：出现 = 覆盖（**空数组 = 清空该项**）、缺失 = 该项未设置
//	   （**空数组 ≠ 缺失**：only 的空数组 = 一个工具都不暴露）；
//	④ 乱序容忍：旧事件（seq 小）盖不掉新事件的整组。

import (
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/sessioncfg"
)

// names 造一个工具名列表（指针形态：契约里只有项是 *ToolNameList）。
//
// 注意：**零个元素也返回非 nil 的空切片**——nil 切片会被 json.Marshal 写成 `null`
// （在 fold 侧读成"该项缺失"），而"空数组 = 清空该项"必须与"缺失"可区分。
func names(list ...string) *contracts.ToolNameList {
	out := make(contracts.ToolNameList, 0, len(list))
	out = append(out, list...)
	return &out
}

// TestToolsGroupWholeLWW：整组 LWW——后写的一组**整组**覆盖前一组（子字段不跨事件合并）。
//
//	E1: tools{only:[read,write], except:[bash]}
//	E2: tools{only:[read]}            ← 只带 only
//	→ 结果：only=[read]、except=**未设置**（不是沿用 E1 的 [bash]——
//	  "不拆子字段跨事件合并"：E2 整组替换了 E1 的整组配置）。
func TestToolsGroupWholeLWW(t *testing.T) {
	tab := sessioncfg.New()

	tab.Apply(ev(t, "E1", 1, "2026-11-04T10:00:01Z", contracts.ConfigChangedPayload{
		Tools: &contracts.ToolsConfigChange{
			Only:   names("read", "write"),
			Except: names("bash"),
		},
	}))
	got := tab.Current("S1")
	if !got.Tools.Touched {
		t.Fatalf("tools 组出现后 Touched 应为 true: %+v", got.Tools)
	}
	if !got.Tools.Only.Written || len(got.Tools.Only.Value) != 2 {
		t.Fatalf("only 应是 [read write]: %+v", got.Tools.Only)
	}
	if !got.Tools.Except.Written || len(got.Tools.Except.Value) != 1 {
		t.Fatalf("except 应是 [bash]: %+v", got.Tools.Except)
	}

	// 后写整组：只带 only → except 回到"未设置"（不沿用前一组）。
	tab.Apply(ev(t, "E2", 2, "2026-11-04T10:00:02Z", contracts.ConfigChangedPayload{
		Tools: &contracts.ToolsConfigChange{Only: names("read")},
	}))
	got = tab.Current("S1")
	if !got.Tools.Only.Written || len(got.Tools.Only.Value) != 1 || got.Tools.Only.Value[0] != "read" {
		t.Fatalf("only 应被后写覆盖成 [read]: %+v", got.Tools.Only)
	}
	if got.Tools.Except.Written {
		t.Fatalf("except 应回到未设置（整组 LWW，不拆子字段跨事件合并）: %+v", got.Tools.Except)
	}
}

// TestToolsGroupAbsentKeepsState：组缺失 = 整组不变（"缺失 = 不变"）。
func TestToolsGroupAbsentKeepsState(t *testing.T) {
	tab := sessioncfg.New()

	tab.Apply(ev(t, "E1", 1, "2026-11-04T10:00:01Z", contracts.ConfigChangedPayload{
		Tools: &contracts.ToolsConfigChange{Only: names("read")},
	}))
	// 后一条事件只改 model，**不带 tools** → tools 组不动。
	tab.Apply(ev(t, "E2", 2, "2026-11-04T10:00:02Z", contracts.ConfigChangedPayload{
		Model: str("m2"),
	}))
	got := tab.Current("S1")
	if !got.Tools.Touched || !got.Tools.Only.Written || got.Tools.Only.Value[0] != "read" {
		t.Fatalf("tools 组不该被不带 tools 的事件改动: %+v", got.Tools)
	}
}

// TestToolsTriState：子项三态——**缺**（未设置）/ **空数组**（清空该项）/ **有值**（覆盖）。
//
//	only 缺   → 该项未设置（不限：全部已注册工具可见）
//	only []   → 显式清空该项（可见集为空 = 一个工具都不暴露）——与"缺"必须可区分
//	only [x]  → 覆盖为 [x]
func TestToolsTriState(t *testing.T) {
	tab := sessioncfg.New()

	// ① 组出现但 only 缺失 → only 未设置（Written=false）。
	tab.Apply(ev(t, "E1", 1, "2026-11-04T10:00:01Z", contracts.ConfigChangedPayload{
		Tools: &contracts.ToolsConfigChange{Except: names("bash")},
	}))
	got := tab.Current("S1")
	if got.Tools.Only.Written {
		t.Fatalf("only 缺失应未设置: %+v", got.Tools.Only)
	}
	if !got.Tools.Except.Written || got.Tools.Except.Value[0] != "bash" {
		t.Fatalf("except 应被覆盖成 [bash]: %+v", got.Tools.Except)
	}

	// ② only 显式空数组 → Written=true、Value=[]（**清空该项**，与"未设置"可区分）。
	tab.Apply(ev(t, "E2", 2, "2026-11-04T10:00:02Z", contracts.ConfigChangedPayload{
		Tools: &contracts.ToolsConfigChange{Only: names()},
	}))
	got = tab.Current("S1")
	if !got.Tools.Only.Written {
		t.Fatalf("only 空数组 = 显式清空该项（Written 应为 true）: %+v", got.Tools.Only)
	}
	if len(got.Tools.Only.Value) != 0 {
		t.Fatalf("only 空数组的 Value 应为空: %+v", got.Tools.Only)
	}
	if got.Tools.Only.List() == nil {
		t.Fatal("only 空数组的 List() 应是非 nil 的空切片（空 ≠ 未设置）")
	}
	// except 未在本次提交里带 → 整组覆盖后回到未设置。
	if got.Tools.Except.Written {
		t.Fatalf("except 应回到未设置（整组覆盖）: %+v", got.Tools.Except)
	}

	// ③ only 有值 → 覆盖。
	tab.Apply(ev(t, "E3", 3, "2026-11-04T10:00:03Z", contracts.ConfigChangedPayload{
		Tools: &contracts.ToolsConfigChange{Only: names("read", "standby")},
	}))
	got = tab.Current("S1")
	if !got.Tools.Only.Written || len(got.Tools.Only.Value) != 2 {
		t.Fatalf("only 应被覆盖成 [read standby]: %+v", got.Tools.Only)
	}
}

// TestToolsClearSemantics：tools:{}（组出现、两子项都缺）= **清除回默认**。
//
// 清除后：Touched 仍为 true（审计上看得见"曾配过、后来被清掉"），但两项都回到
// "未设置"——生效上 = 全部已注册工具可见（仍受类型白名单与策略链管辖）。
func TestToolsClearSemantics(t *testing.T) {
	tab := sessioncfg.New()

	tab.Apply(ev(t, "E1", 1, "2026-11-04T10:00:01Z", contracts.ConfigChangedPayload{
		Tools: &contracts.ToolsConfigChange{Only: names("read"), Except: names("bash")},
	}))
	tab.Apply(ev(t, "E2", 2, "2026-11-04T10:00:02Z", contracts.ConfigChangedPayload{
		Tools: &contracts.ToolsConfigChange{},
	}))
	got := tab.Current("S1")
	if !got.Tools.Touched {
		t.Fatalf("清除记录也要留痕（Touched 应为 true）: %+v", got.Tools)
	}
	if got.Tools.Only.Written || got.Tools.Except.Written {
		t.Fatalf("清除后两项都该回到未设置: %+v", got.Tools)
	}
	if got.Tools.Only.List() != nil || got.Tools.Except.List() != nil {
		t.Fatalf("清除后两项的 List() 应为 nil（= 未设置）: %+v", got.Tools)
	}
}

// TestToolsOutOfOrderKeepsNewest：乱序容忍——旧事件（seq 小）盖不掉新事件的整组。
func TestToolsOutOfOrderKeepsNewest(t *testing.T) {
	tab := sessioncfg.New()

	tab.Apply(ev(t, "E2", 2, "2026-11-04T10:00:02Z", contracts.ConfigChangedPayload{
		Tools: &contracts.ToolsConfigChange{Only: names("new")},
	}))
	tab.Apply(ev(t, "E1", 1, "2026-11-04T10:00:01Z", contracts.ConfigChangedPayload{
		Tools: &contracts.ToolsConfigChange{Only: names("old")},
	}))
	got := tab.Current("S1")
	if !got.Tools.Only.Written || len(got.Tools.Only.Value) != 1 || got.Tools.Only.Value[0] != "new" {
		t.Fatalf("旧事件不该盖掉新事件的 tools 组: %+v", got.Tools.Only)
	}
}

// TestToolsDuplicateApplyIdempotent：同一条事件重复投递 → 结果不变（幂等）。
func TestToolsDuplicateApplyIdempotent(t *testing.T) {
	tab := sessioncfg.New()
	payload := contracts.ConfigChangedPayload{
		Tools: &contracts.ToolsConfigChange{Only: names("read"), Except: names("bash")},
	}
	tab.Apply(ev(t, "E1", 1, "2026-11-04T10:00:01Z", payload))
	tab.Apply(ev(t, "E1", 1, "2026-11-04T10:00:01Z", payload))
	got := tab.Current("S1")
	if !got.Tools.Only.Written || len(got.Tools.Only.Value) != 1 {
		t.Fatalf("重复投递后 only 漂移: %+v", got.Tools.Only)
	}
	if !got.Tools.Except.Written || len(got.Tools.Except.Value) != 1 {
		t.Fatalf("重复投递后 except 漂移: %+v", got.Tools.Except)
	}
}
