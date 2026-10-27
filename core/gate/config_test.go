package gate

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseConfigValid(t *testing.T) {
	c, err := ParseConfig([]byte(`{"command":"make ci"}`))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if c.Command != "make ci" {
		t.Errorf("Command = %q，期望 make ci", c.Command)
	}
}

func TestParseConfigRejects(t *testing.T) {
	cases := map[string]string{
		"缺 command":    `{}`,
		"command 为空白":  `{"command":"   "}`,
		"不是合法 JSON":    `{"command":`,
		"command 类型错误": `{"command":42}`,
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseConfig([]byte(data)); err == nil {
				t.Errorf("ParseConfig(%s) 应报错（W9：无默认，配置不完整即拒）", data)
			}
		})
	}
}

// TestConfigOpaqueCommand 钉住 W9：command 是不透明字符串，含 shell 元字符也原样保存、不解析。
func TestConfigOpaqueCommand(t *testing.T) {
	const cmd = "make ci && echo done | tee log; exit 0"
	c, err := ParseConfig([]byte(`{"command":"make ci && echo done | tee log; exit 0"}`))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if c.Command != cmd {
		t.Errorf("Command = %q，期望原样保存 %q", c.Command, cmd)
	}
}

func TestConfigMarshalRoundTrip(t *testing.T) {
	c := Config{Command: "make ci"}
	data, err := c.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := ParseConfig(data)
	if err != nil {
		t.Fatalf("ParseConfig(round trip): %v", err)
	}
	if got != c {
		t.Errorf("round trip = %+v，期望 %+v", got, c)
	}
}

func TestPaths(t *testing.T) {
	root := filepath.Join("repo", "root")
	if got, want := ConfigPath(root), filepath.Join(root, ".alasol", "gate.json"); got != want {
		t.Errorf("ConfigPath = %q，期望 %q", got, want)
	}
	if got, want := BaselinePath(root), filepath.Join(root, ".alasol", "gate-baseline.json"); got != want {
		t.Errorf("BaselinePath = %q，期望 %q", got, want)
	}
}

// TestErrNotConfigured 确认哨兵错误可被 errors.Is 识别（装配层据此决定跳过/拒绝）。
func TestErrNotConfigured(t *testing.T) {
	wrapped := errors.Join(errors.New("context"), ErrNotConfigured)
	if !errors.Is(wrapped, ErrNotConfigured) {
		t.Error("ErrNotConfigured 应可被 errors.Is 识别")
	}
	if !strings.Contains(ErrNotConfigured.Error(), "未配置") {
		t.Errorf("ErrNotConfigured 文案应说明未配置：%q", ErrNotConfigured.Error())
	}
}
