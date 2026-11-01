package discovery

// G25 跨进程托管注册（2026-11-01 v4-7）的发现表钉子：
//   - Claim 登记宿主 hub 标识 + 同 hub 幂等；
//   - 冲突 fail-close：已有宿主（别的 hub）→ 拒绝接管、登记不被改动；
//   - HostOf 查询"该 session 谁托管"；
//   - Deactivate 清登记（失管后新宿主可接管）。

import (
	"errors"
	"testing"

	"github.com/Yorha9e/alasol/core"
)

func TestClaimRegistersHostAndIdempotent(t *testing.T) {
	tbl := New()
	sid := core.NewUlid()
	hubA := "hub-alpha"

	if err := tbl.Claim(sid, hubA); err != nil {
		t.Fatalf("首次 Claim: %v", err)
	}
	if !tbl.IsLive(sid) {
		t.Error("Claim 应同时置 live（托管 = 本进程持有）")
	}
	got, ok := tbl.HostOf(sid)
	if !ok || got != hubA {
		t.Fatalf("HostOf = (%q, %v)，want (%q, true)", got, ok, hubA)
	}
	// 同 hub 重复 Claim 幂等（loop 重建 / 续跑都走这里）。
	if err := tbl.Claim(sid, hubA); err != nil {
		t.Fatalf("同 hub 重复 Claim 应幂等: %v", err)
	}
	if got, _ := tbl.HostOf(sid); got != hubA {
		t.Fatalf("重复 Claim 后宿主漂移: %q", got)
	}
}

func TestClaimConflictFailClose(t *testing.T) {
	tbl := New()
	sid := core.NewUlid()
	hubA, hubB := "hub-alpha", "hub-beta"

	if err := tbl.Claim(sid, hubA); err != nil {
		t.Fatalf("首次 Claim: %v", err)
	}
	// 冲突：另一个 hub 想接管 → 拒绝（fail-close），且**不改动**现有登记。
	err := tbl.Claim(sid, hubB)
	if !errors.Is(err, ErrHostConflict) {
		t.Fatalf("异 hub 接管应 ErrHostConflict，得到 %v", err)
	}
	if got, _ := tbl.HostOf(sid); got != hubA {
		t.Fatalf("冲突后宿主不该被改动：HostOf = %q，want %q（fail-close）", got, hubA)
	}
	if !tbl.IsLive(sid) {
		t.Error("冲突拒绝接管不该把原宿主的 live 摘掉")
	}
}

func TestClaimRejectsEmptyIDsAndDeactivateClears(t *testing.T) {
	tbl := New()
	sid := core.NewUlid()
	if err := tbl.Claim(sid, "  "); err == nil {
		t.Error("空 hubID 应被拒（没有宿主标识的托管登记无意义）")
	}
	if err := tbl.Claim("", "hub-alpha"); err == nil {
		t.Error("空 sessionID 应被拒")
	}

	if err := tbl.Claim(sid, "hub-alpha"); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	// 失管（session 结束）→ 清登记：新宿主此后可接管（重启/迁移口径）。
	tbl.Deactivate(sid)
	if _, ok := tbl.HostOf(sid); ok {
		t.Fatal("Deactivate 应清宿主登记")
	}
	if tbl.IsLive(sid) {
		t.Error("Deactivate 应摘 live")
	}
	if err := tbl.Claim(sid, "hub-beta"); err != nil {
		t.Fatalf("失管后新宿主 Claim 应成功: %v", err)
	}
	if got, _ := tbl.HostOf(sid); got != "hub-beta" {
		t.Fatalf("HostOf = %q，want hub-beta", got)
	}
}

// TestHostOfUnknownSession：未知 session / nil 表 → (空, false)，不 panic。
func TestHostOfUnknownSession(t *testing.T) {
	if got, ok := New().HostOf(core.NewUlid()); ok || got != "" {
		t.Fatalf("未知 session HostOf = (%q, %v)，want (\"\", false)", got, ok)
	}
	var tbl *Table
	if got, ok := tbl.HostOf(core.NewUlid()); ok || got != "" {
		t.Fatalf("nil 表 HostOf = (%q, %v)，want (\"\", false)", got, ok)
	}
	if err := tbl.Claim(core.NewUlid(), "hub-alpha"); err == nil {
		t.Error("nil 表 Claim 应报错")
	}
}
