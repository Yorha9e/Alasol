package gate

import (
	"github.com/Yorha9e/alasol/contracts"
)

// CheckedEvent 把一次门禁结论（Receipt + Verdict）映射成 gate.checked 事件的 payload
// （契约见 contracts/schema/event.json 的 GateCheckedPayload）。
//
// 门禁事实进事件流（唯一事实源，D5/F5）：装配层拿到 Verdict 后调本函数构造 payload，
// 再包成 contracts.Event（kind="gate.checked"、durable=true）追加。映射口径：
//
//   - passed = Verdict.Passed()（passed/recorded 都算放行，只有 blocked 拦截）；
//   - recorded =（Kind==VerdictRecorded）；
//   - new_failures = Verdict.NewFailures（仅 blocked 时非空）。
func CheckedEvent(receipt *Receipt, v Verdict) contracts.GateCheckedPayload {
	return contracts.GateCheckedPayload{
		ReceiptCommit:  receipt.Commit,
		ObservedCommit: receipt.ObservedCommit,
		Command:        receipt.Command,
		ExitCode:       int64(receipt.ExitCode),
		Passed:         v.Passed(),
		Recorded:       v.Kind == VerdictRecorded,
		NewFailures:    cloneStrings(v.NewFailures),
		ElapsedMs:      receipt.ElapsedMS, // contracts 字段名为 ElapsedMs（elapsed_ms 的 Pascal 形）
	}
}
