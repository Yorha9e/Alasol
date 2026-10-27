package gate

import (
	"errors"
	"testing"
	"time"
)

// validReceipt 返回一个四查全过的"干净通过" receipt（供各测试在此基础上改坏一个字段）。
func validReceipt() Receipt {
	return Receipt{
		Commit:         "tipcommit0000000000000000000000000001",
		ObservedCommit: "tipcommit0000000000000000000000000001",
		Command:        "make ci",
		ExitCode:       0,
		StartedAt:      time.Now(),
		ElapsedMS:      1234,
		LogTail:        "ok",
	}
}

func validReq() VerifyRequest {
	return VerifyRequest{
		TipCommit: "tipcommit0000000000000000000000000001",
		Config:    Config{Command: "make ci"},
	}
}

// TestVerifyCleanPass：四查全过 → Verify 返回 nil。
func TestVerifyCleanPass(t *testing.T) {
	r := validReceipt()
	if err := Verify(&r, validReq()); err != nil {
		t.Errorf("干净 receipt 应通过：%v", err)
	}
}

// TestVerifyFourChecksIsolated 四查各自：每次只改坏一个字段，Verify 必须拒。
func TestVerifyFourChecksIsolated(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Receipt)
		want   error
	}{
		{
			name:   "①退出码非零",
			mutate: func(r *Receipt) { r.ExitCode = 1 },
			want:   ErrExitNonZero,
		},
		{
			// 验收①：receipt commit ≠ 当前 tip → 拒绝通过（跑的不是当前 commit）。
			name: "②commit≠tip",
			mutate: func(r *Receipt) {
				r.Commit = "oldcommit0000000000000000000000000002"
				r.ObservedCommit = "oldcommit0000000000000000000000000002"
			},
			want: ErrCommitMismatch,
		},
		{
			name:   "③observed≠commit",
			mutate: func(r *Receipt) { r.ObservedCommit = "moved00000000000000000000000000000003" },
			want:   ErrObservedMismatch,
		},
		{
			// 验收③：命令 ≠ 当前配置 → 拒绝通过（配置已改）。
			name:   "④command≠config",
			mutate: func(r *Receipt) { r.Command = "make test" },
			want:   ErrCommandMismatch,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := validReceipt()
			tc.mutate(&r)
			err := Verify(&r, validReq())
			if !errors.Is(err, tc.want) {
				t.Fatalf("Verify err = %v，期望 errors.Is %v", err, tc.want)
			}
		})
	}
}

// TestVerifyTrustBeforeExit fail-close 组合的顺序证据：
// 同时"退出码非零"且"commit 过期"时，Verify 必须报 commit（可信度）而非退出码——
// 不可信的 receipt 不能仅因退出码被分流进 baseline。
func TestVerifyTrustBeforeExit(t *testing.T) {
	r := validReceipt()
	r.ExitCode = 1
	r.Commit = "stale00000000000000000000000000000009"
	r.ObservedCommit = "stale00000000000000000000000000000009"
	err := Verify(&r, validReq())
	if !errors.Is(err, ErrCommitMismatch) {
		t.Fatalf("过期 + 失败的 receipt 应报 ErrCommitMismatch（可信度优先），得到 %v", err)
	}
	if errors.Is(err, ErrExitNonZero) {
		t.Error("不应在可信度未过时就报退出码错误")
	}
}

// TestVerifyNilReceipt：nil receipt 报错（不 panic）。
func TestVerifyNilReceipt(t *testing.T) {
	if err := Verify(nil, validReq()); err == nil {
		t.Error("nil receipt 应报错")
	}
}

// TestVerifyObservedEmptyFailsClosed：ObservedCommit 为空（rev-parse 失败）→ 四查③ 拒（fail-close）。
func TestVerifyObservedEmptyFailsClosed(t *testing.T) {
	r := validReceipt()
	r.ObservedCommit = ""
	err := Verify(&r, validReq())
	if !errors.Is(err, ErrObservedMismatch) {
		t.Fatalf("ObservedCommit 为空应报 ErrObservedMismatch，得到 %v", err)
	}
}
