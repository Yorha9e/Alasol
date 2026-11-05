package sessioncfg_test

// core/sessioncfg 的 **执行面**（exec 组 / 会话工作区，L2 per-session 执行上下文 · 件 2）
// 折叠口径：
//
//	① **子字段各自 LWW**：cwd / worktree 各带一个戳——后写的子项只覆盖自己那一项，
//	   没带的子项沿用上一次的值（**不学 tools 的整组 LWW**：整组 LWW 下"只改 cwd"
//	   会把 worktree 静默清掉 = 悄悄解绑）；
//	② **子项三态**：未写过 / 显式空串（= 清除该项 → 不锚定）/ 显式非空（= 覆盖）；
//	③ 组缺失 = 整组不变；乱序容忍；重复投递幂等。

import (
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/sessioncfg"
)

// TestExecSubFieldLWW：子字段各自 LWW——后写的一组只覆盖**它带的那些**子项，
// 没带的子项沿用上一次的值（不静默丢字段）。
//
//	E1: exec{cwd:/a, worktree:wt-a}
//	E2: exec{cwd:/b}                  ← 只带 cwd
//	→ 结果：cwd=/b、worktree=wt-a **仍在**。
func TestExecSubFieldLWW(t *testing.T) {
	tab := sessioncfg.New()

	tab.Apply(ev(t, "E1", 1, "2026-11-05T10:00:01Z", contracts.ConfigChangedPayload{
		Exec: &contracts.ExecConfigChange{Cwd: str("/a"), Worktree: str("wt-a")},
	}))
	tab.Apply(ev(t, "E2", 2, "2026-11-05T10:00:02Z", contracts.ConfigChangedPayload{
		Exec: &contracts.ExecConfigChange{Cwd: str("/b")},
	}))

	got := tab.Current("S1").Exec
	if !got.Touched {
		t.Fatalf("exec 出现过应 Touched=true: %+v", got)
	}
	if !got.Cwd.HasValue() || got.Cwd.Value != "/b" {
		t.Fatalf("cwd 应被覆盖成 /b（子字段各自 LWW）: %+v", got.Cwd)
	}
	if !got.Worktree.HasValue() || got.Worktree.Value != "wt-a" {
		t.Fatalf("worktree 应仍是 wt-a（只改 cwd 不该解绑 worktree）: %+v", got.Worktree)
	}
}

// TestExecStringTriState：字符串三态——未写过 / 空串（清除该项）/ 非空（覆盖）。
//
// 空串 = **清除**（不是"覆盖成空路径"）：工作区回到"不锚定"（相对路径按进程 cwd）。
func TestExecStringTriState(t *testing.T) {
	tab := sessioncfg.New()

	// ① 覆盖 cwd = /a。
	tab.Apply(ev(t, "E1", 1, "2026-11-05T10:00:01Z", contracts.ConfigChangedPayload{
		Exec: &contracts.ExecConfigChange{Cwd: str("/a")},
	}))
	got := tab.Current("S1").Exec
	if !got.Cwd.HasValue() || got.Cwd.Value != "/a" {
		t.Fatalf("cwd 应被覆盖成 /a: %+v", got.Cwd)
	}
	if got.Worktree.Written {
		t.Fatalf("没带过的子项应未设置: %+v", got)
	}

	// ② cwd = "" → 显式清除（Written=true、Value=""、生效上不锚定）。
	tab.Apply(ev(t, "E2", 2, "2026-11-05T10:00:02Z", contracts.ConfigChangedPayload{
		Exec: &contracts.ExecConfigChange{Cwd: str("")},
	}))
	got = tab.Current("S1").Exec
	if !got.Cwd.Written {
		t.Fatalf("cwd 空串是显式清除记录（Written 应为 true）: %+v", got.Cwd)
	}
	if got.Cwd.HasValue() {
		t.Fatalf("cwd 空串不该是生效覆盖（生效上 = 不锚定）: %+v", got.Cwd)
	}

	// ③ worktree 同款三态。
	tab.Apply(ev(t, "E3", 3, "2026-11-05T10:00:03Z", contracts.ConfigChangedPayload{
		Exec: &contracts.ExecConfigChange{Worktree: str("wt-x")},
	}))
	got = tab.Current("S1").Exec
	if !got.Worktree.HasValue() || got.Worktree.Value != "wt-x" {
		t.Fatalf("worktree 应被覆盖成 wt-x: %+v", got.Worktree)
	}
	tab.Apply(ev(t, "E4", 4, "2026-11-05T10:00:04Z", contracts.ConfigChangedPayload{
		Exec: &contracts.ExecConfigChange{Worktree: str("")},
	}))
	got = tab.Current("S1").Exec
	if !got.Worktree.Written || got.Worktree.HasValue() {
		t.Fatalf("worktree 空串应是显式清除: %+v", got.Worktree)
	}
}

// TestExecAbsentKeepsState：exec 组缺失 = 整组不变（"缺失 = 不变"）。
func TestExecAbsentKeepsState(t *testing.T) {
	tab := sessioncfg.New()

	tab.Apply(ev(t, "E1", 1, "2026-11-05T10:00:01Z", contracts.ConfigChangedPayload{
		Exec: &contracts.ExecConfigChange{Cwd: str("/a"), Worktree: str("wt-a")},
	}))
	// 后一条事件只改 model，**不带 exec** → 执行面不动。
	tab.Apply(ev(t, "E2", 2, "2026-11-05T10:00:02Z", contracts.ConfigChangedPayload{
		Model: str("m2"),
	}))
	got := tab.Current("S1").Exec
	if !got.Cwd.HasValue() || got.Cwd.Value != "/a" {
		t.Fatalf("cwd 不该被不带 exec 的事件改动: %+v", got.Cwd)
	}
	if !got.Worktree.HasValue() || got.Worktree.Value != "wt-a" {
		t.Fatalf("worktree 不该被不带 exec 的事件改动: %+v", got.Worktree)
	}
}

// TestExecEmptyObjectTouchesOnly：空对象 exec{} = "出现过但什么都没改"
// （Touched=true、两个子项都未设置）——与"从没出现过"可区分（审计口径）。
func TestExecEmptyObjectTouchesOnly(t *testing.T) {
	tab := sessioncfg.New()

	tab.Apply(ev(t, "E1", 1, "2026-11-05T10:00:01Z", contracts.ConfigChangedPayload{
		Exec: &contracts.ExecConfigChange{},
	}))
	got := tab.Current("S1").Exec
	if !got.Touched {
		t.Fatalf("exec{} 出现过应 Touched=true: %+v", got)
	}
	if got.Cwd.Written || got.Worktree.Written {
		t.Fatalf("exec{} 不该改任何子项: %+v", got)
	}
}

// TestExecOutOfOrderKeepsNewest：乱序容忍——旧事件（seq 小）盖不掉新事件的子项。
func TestExecOutOfOrderKeepsNewest(t *testing.T) {
	tab := sessioncfg.New()

	tab.Apply(ev(t, "E2", 2, "2026-11-05T10:00:02Z", contracts.ConfigChangedPayload{
		Exec: &contracts.ExecConfigChange{Cwd: str("/new")},
	}))
	tab.Apply(ev(t, "E1", 1, "2026-11-05T10:00:01Z", contracts.ConfigChangedPayload{
		Exec: &contracts.ExecConfigChange{Cwd: str("/old")},
	}))
	got := tab.Current("S1").Exec
	if !got.Cwd.HasValue() || got.Cwd.Value != "/new" {
		t.Fatalf("旧事件不该盖掉新事件的 cwd: %+v", got.Cwd)
	}
}

// TestExecDuplicateApplyIdempotent：同一条事件重复投递 → 结果不变（幂等）。
func TestExecDuplicateApplyIdempotent(t *testing.T) {
	tab := sessioncfg.New()
	payload := contracts.ConfigChangedPayload{
		Exec: &contracts.ExecConfigChange{Cwd: str("/a"), Worktree: str("wt-a")},
	}
	tab.Apply(ev(t, "E1", 1, "2026-11-05T10:00:01Z", payload))
	tab.Apply(ev(t, "E1", 1, "2026-11-05T10:00:01Z", payload))
	got := tab.Current("S1").Exec
	if !got.Cwd.HasValue() || got.Cwd.Value != "/a" {
		t.Fatalf("重复投递后 cwd 漂移: %+v", got.Cwd)
	}
	if !got.Worktree.HasValue() || got.Worktree.Value != "wt-a" {
		t.Fatalf("重复投递后 worktree 漂移: %+v", got.Worktree)
	}
}

// TestExecUnknownSessionIsZero：未知 session → 零值（两项都"未写过" → 调用方按
// "没设工作区"处理，而不是"回默认"——工作区**没有** hub 默认这一层）。
func TestExecUnknownSessionIsZero(t *testing.T) {
	tab := sessioncfg.New()
	got := tab.Current("S-other").Exec
	if got.Touched || got.Cwd.Written || got.Worktree.Written {
		t.Fatalf("未知 session 应是零值 exec: %+v", got)
	}
}
