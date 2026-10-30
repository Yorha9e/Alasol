package host

// 内部测试：思维链档位（G14）的校验口径——空 = 不传；非空必须落在白名单；
// **大小写敏感**（陷阱 17：档位是协议面枚举值，不折叠大小写）。

import (
	"strings"
	"testing"
)

func TestValidateReasoningEffort(t *testing.T) {
	// 合法：空（含纯空白）= 不传；四个已知档位。
	for _, ok := range []string{"", "   ", "low", "medium", "high", "xhigh"} {
		if err := ValidateReasoningEffort(ok); err != nil {
			t.Errorf("ValidateReasoningEffort(%q) = %v，want nil", ok, err)
		}
	}
	// 非法：typo / 别的厂商私有值 / 大小写变形（不静默改写，陷阱 17）。
	for _, bad := range []string{"turbo", "none", "High", "LOW", "xhigh!"} {
		if err := ValidateReasoningEffort(bad); err == nil {
			t.Errorf("ValidateReasoningEffort(%q) = nil，want 拒绝", bad)
		} else if !strings.Contains(err.Error(), "reasoning_effort") && !strings.Contains(err.Error(), "ReasoningEffort") {
			t.Errorf("错误文案应说明是档位问题: %v", err)
		}
	}
	// 首尾空白是**粘贴痕迹**：trim 后判（" high " 合法）——与 Config 其它字段同口径；
	// 但大小写不折叠（"High" 上面已拒：协议面枚举值，改大小写就是改值，陷阱 17）。
	for _, trimmed := range []string{" high ", "\tlow\n"} {
		if err := ValidateReasoningEffort(trimmed); err != nil {
			t.Errorf("ValidateReasoningEffort(%q) = %v，want nil（trim 后判）", trimmed, err)
		}
	}
}

// TestConfigValidateReasoningEffort 走 Config.validate 的完整路径（host.New 的前置）：
// 坏档位在**启动期**就拒（fail-close），不拖到模型调用里。
func TestConfigValidateReasoningEffort(t *testing.T) {
	base := func() Config {
		return Config{DBPath: "x.db", Model: "m", Addr: "127.0.0.1:8720"}
	}
	c := base()
	c.ReasoningEffort = "high"
	if err := c.validate(); err != nil {
		t.Fatalf("合法档位应通过 validate: %v", err)
	}
	c = base()
	c.ReasoningEffort = "turbo"
	if err := c.validate(); err == nil {
		t.Fatal("坏档位应被 validate 拒绝（fail-close 到启动期）")
	}
}
