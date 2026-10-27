package gate

import (
	"testing"
)

// TestCheckedEventMapping 钉住 Receipt+Verdict → gate.checked payload 的映射口径。
func TestCheckedEventMapping(t *testing.T) {
	r := &Receipt{
		Commit:         "receiptcommit000000000000000000001",
		ObservedCommit: "receiptcommit000000000000000000001",
		Command:        "make ci",
		ExitCode:       1,
		ElapsedMS:      4321,
	}

	t.Run("blocked", func(t *testing.T) {
		v := Verdict{Kind: VerdictBlocked, NewFailures: []string{"FAIL: new"}}
		p := CheckedEvent(r, v)
		if p.ReceiptCommit != r.Commit || p.ObservedCommit != r.ObservedCommit || p.Command != r.Command {
			t.Errorf("commit/command 映射错：%+v", p)
		}
		if p.ExitCode != 1 || p.ElapsedMs != 4321 {
			t.Errorf("exit/elapsed 映射错：exit=%d elapsed=%d", p.ExitCode, p.ElapsedMs)
		}
		if p.Passed {
			t.Error("blocked 时 passed 应为 false")
		}
		if p.Recorded {
			t.Error("blocked 时 recorded 应为 false")
		}
		if len(p.NewFailures) != 1 || p.NewFailures[0] != "FAIL: new" {
			t.Errorf("new_failures 映射错：%v", p.NewFailures)
		}
	})

	t.Run("recorded", func(t *testing.T) {
		v := Verdict{Kind: VerdictRecorded}
		p := CheckedEvent(r, v)
		if !p.Passed {
			t.Error("recorded 应按通过语义：passed 应为 true")
		}
		if !p.Recorded {
			t.Error("recorded 时 recorded 应为 true")
		}
		if len(p.NewFailures) != 0 {
			t.Errorf("recorded 时 new_failures 应为空：%v", p.NewFailures)
		}
	})

	t.Run("passed", func(t *testing.T) {
		v := Verdict{Kind: VerdictPassed}
		p := CheckedEvent(r, v)
		if !p.Passed || p.Recorded {
			t.Errorf("passed 时 passed=%t recorded=%t，期望 true/false", p.Passed, p.Recorded)
		}
	})
}
