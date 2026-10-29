package compaction

import (
	"strings"
	"testing"
)

// 预算数学（13 §8）与 W≥256k 硬约束。

func TestHistoryBudgetMatchesDesignTable(t *testing.T) {
	// 13 §8 表：历史区预算 = W − 205k。
	cases := []struct {
		w    int64
		want int64
	}{
		{1_000_000, 795_000},
		{256_000, 51_000},
		{300_000, 95_000},
	}
	for _, tc := range cases {
		if got := HistoryBudget(tc.w); got != tc.want {
			t.Errorf("HistoryBudget(%d) = %d, want %d", tc.w, got, tc.want)
		}
	}
	// 200k → 负 5k（13 §8：不成立的信号）。
	if got := HistoryBudget(200_000); got != -5_000 {
		t.Errorf("HistoryBudget(200000) = %d, want -5000（负值=不成立）", got)
	}
}

func TestStageBudgetIsWOverThird(t *testing.T) {
	if got := StageBudget(300_000); got != 100_000 {
		t.Errorf("StageBudget(300000) = %d, want 100000（S ≤ W/3）", got)
	}
	if got := StageBudget(1_000_000); got != 333_333 {
		t.Errorf("StageBudget(1000000) = %d, want 333333", got)
	}
}

func TestValidateWindowHardConstraint(t *testing.T) {
	// W ≥ 256k 通过。
	for _, w := range []int64{256_000, 300_000, 1_000_000} {
		if err := ValidateWindow(w); err != nil {
			t.Errorf("ValidateWindow(%d) 应通过: %v", w, err)
		}
	}
	// W < 256k 拒绝并说明。
	for _, w := range []int64{0, 200_000, 255_999} {
		err := ValidateWindow(w)
		if err == nil {
			t.Errorf("ValidateWindow(%d) 应拒绝（W≥256k 硬约束）", w)
			continue
		}
		if !strings.Contains(err.Error(), "256") {
			t.Errorf("拒绝信息应点明 256k 硬约束: %v", err)
		}
	}
}

func TestNewEngineRejectsWindowBelowHardConstraint(t *testing.T) {
	if _, err := NewEngine(Params{Window: 200_000}, nil, nil, nil, nil, nil); err == nil {
		t.Fatal("NewEngine 应拒绝 W<256k（挂载时校验）")
	}
	if _, err := NewEngine(DefaultParams(), nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("NewEngine 默认参数应通过: %v", err)
	}
}

func TestDefaultParamsTargets(t *testing.T) {
	p := DefaultParams()
	// J9：L1≤10% / L2≤1% 窗口。1M 窗口 → L1=100k / L2=10k。
	if got := p.L1TargetTokens(); got != 100_000 {
		t.Errorf("L1TargetTokens = %d, want 100000", got)
	}
	if got := p.L2TargetTokens(); got != 10_000 {
		t.Errorf("L2TargetTokens = %d, want 10000", got)
	}
}
