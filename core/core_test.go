package core

import (
	"context"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
)

// ---- DenyAll：默认审批人，一律 deny（fail-close 基线）----

func TestDenyAllAlwaysDenies(t *testing.T) {
	req := contracts.ApprovalRequest{
		V:         1,
		ID:        NewUlid(),
		TS:        "2026-10-27T00:00:00Z",
		SessionID: contracts.Ulid(NewUlid()),
		AgentID:   contracts.Ulid(NewUlid()),
		Trigger:   contracts.ApprovalTriggerPolicyAsk,
		Summary:   "需要人审",
	}
	resp, err := DenyAll{}.Approve(context.Background(), req)
	if err != nil {
		t.Fatalf("DenyAll 不应返回 error: %v", err)
	}
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Fatalf("Decision=%s, want deny", resp.Decision)
	}
	if resp.RequestID != req.ID {
		t.Fatalf("RequestID=%s, want %s（必须回指请求）", resp.RequestID, req.ID)
	}
	if resp.V != req.V {
		t.Fatalf("V=%d, want %d", resp.V, req.V)
	}
	if resp.TS != req.TS {
		t.Fatalf("TS=%s, want %s", resp.TS, req.TS)
	}
	if resp.DecidedBy == nil || *resp.DecidedBy == "" {
		t.Fatalf("DecidedBy 不能为空（审计需要）")
	}
	if resp.Note == nil || *resp.Note == "" {
		t.Fatalf("Note 不能为空（拒绝话术，H7）")
	}
}

// DenyAll 是值类型：零值也必须可用（不得 panic、不得放行）。
func TestDenyAllZeroValueStillDenies(t *testing.T) {
	var d DenyAll
	resp, err := d.Approve(context.Background(), contracts.ApprovalRequest{})
	if err != nil {
		t.Fatalf("DenyAll 零值不应返回 error: %v", err)
	}
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Fatalf("Decision=%s, want deny", resp.Decision)
	}
}

// ---- AlwaysAllow：显式放行（真机回路用；绝不是默认）----

func TestAlwaysAllowExplicitlyAllows(t *testing.T) {
	req := contracts.ApprovalRequest{
		V:         1,
		ID:        NewUlid(),
		TS:        "2026-10-27T00:00:00Z",
		SessionID: contracts.Ulid(NewUlid()),
		AgentID:   contracts.Ulid(NewUlid()),
		Trigger:   contracts.ApprovalTriggerPolicyAsk,
		Summary:   "需要人审",
	}
	resp, err := AlwaysAllow{}.Approve(context.Background(), req)
	if err != nil {
		t.Fatalf("AlwaysAllow 不应返回 error: %v", err)
	}
	if resp.Decision != contracts.ApprovalDecisionAllow {
		t.Fatalf("Decision=%s, want allow", resp.Decision)
	}
	if resp.RequestID != req.ID {
		t.Fatalf("RequestID=%s, want %s", resp.RequestID, req.ID)
	}
	if resp.DecidedBy == nil || *resp.DecidedBy == "" {
		t.Fatalf("DecidedBy 不能为空")
	}
}

// AlwaysAllow 零值同样可用（值类型，无隐藏状态）。
func TestAlwaysAllowZeroValueStillAllows(t *testing.T) {
	var a AlwaysAllow
	resp, err := a.Approve(context.Background(), contracts.ApprovalRequest{})
	if err != nil {
		t.Fatalf("AlwaysAllow 零值不应返回 error: %v", err)
	}
	if resp.Decision != contracts.ApprovalDecisionAllow {
		t.Fatalf("Decision=%s, want allow", resp.Decision)
	}
}

// ---- 编译期约束：两个默认审批人都实现 core.Approver ----

var (
	_ Approver = DenyAll{}
	_ Approver = AlwaysAllow{}
)

// ---- ULID：core 根包是框架 id 的唯一生成/校验处 ----

func TestNewUlidIsValidAndUnique(t *testing.T) {
	seen := make(map[contracts.Ulid]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		u := NewUlid()
		if !ValidUlid(u) {
			t.Fatalf("NewUlid()=%q 不是合法 ULID", u)
		}
		if _, dup := seen[u]; dup {
			t.Fatalf("ULID 重复: %s", u)
		}
		seen[u] = struct{}{}
	}
}

func TestValidUlidRejectsGarbage(t *testing.T) {
	for _, s := range []contracts.Ulid{"", "not-a-ulid", "01BX5ZZKBKACTAV9WEVGEMMVRI"} {
		if ValidUlid(s) {
			t.Errorf("ValidUlid(%q)=true, want false", s)
		}
	}
}
