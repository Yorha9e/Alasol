package core

import (
	"context"

	"github.com/Yorha9e/alasol/contracts"
)

// Approver 是人审抽象（进程内接口；审批消息本身是跨边界契约 contracts.ApprovalRequest/Response）。
//
// fail-close 铁律（D14/H7/H8/陷阱 9）：无人应答、超时、审批服务缺席——一切非显式 allow
// 都必须落 deny，不得挂起。返回 error 时调用方按 deny 处理。
//
// nil 语义（与 core/doc.go 的"nil 语义"一节同口径）：实现是**值类型**
// （DenyAll{} / AlwaysAllow{}）；接口本身为 nil 时由调用方（policy.Decide）判为 deny，
// 若传入"有类型的 nil 指针"（接口非 nil、值为 nil），调用点的失败隔离把它变成 deny
// （见 policy.Decide 的 approveSafely）——两种情况都不可能放行。
type Approver interface {
	Approve(ctx context.Context, req contracts.ApprovalRequest) (contracts.ApprovalResponse, error)
}

// DenyAll 是默认 Approver：一律 deny（验 fail-close 的基线）。
type DenyAll struct{}

// Approve implements Approver.
func (DenyAll) Approve(_ context.Context, req contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	return denyResponse(req), nil
}

// AlwaysAllow 显式开启的自动批准 Approver（真机回路用，阶段 4）；
// 永远不做默认（默认是 DenyAll，fail-close）。
type AlwaysAllow struct{}

// Approve implements Approver.
func (AlwaysAllow) Approve(_ context.Context, req contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	return contracts.ApprovalResponse{
		V:         1,
		RequestID: req.ID,
		TS:        req.TS,
		Decision:  contracts.ApprovalDecisionAllow,
		DecidedBy: strPtr("auto:always-allow"),
	}, nil
}

func denyResponse(req contracts.ApprovalRequest) contracts.ApprovalResponse {
	return contracts.ApprovalResponse{
		V:         1,
		RequestID: req.ID,
		TS:        req.TS,
		Decision:  contracts.ApprovalDecisionDeny,
		Note:      strPtr("fail-close: approval denied (no explicit allow)"),
		DecidedBy: strPtr("auto:deny-all"),
	}
}

func strPtr(s string) *string { return &s }
