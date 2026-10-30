package main

// G14 修复的回归：**run / resume 两条路径也必须校验思维链档位白名单**（fail-close）。
//
// 为什么：白名单的唯一事实源在 host（host.ValidateReasoningEffort）。serve 路径经
// host.Config.validate 校验；run / resume **不经 host.Config**——它们各自装配 loop，
// 于是打错的档位（如 "turbo"）会被原样透传给端点，只在**一轮模型调用**里换来一个
// 离配置现场很远的 400。现在两条路径都在启动期用同一个白名单拒（退出码 2）。

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/host"
)

// TestRunRejectsUnknownReasoningEffort：`alasol run -reasoning-effort turbo` → 启动期拒。
func TestRunRejectsUnknownReasoningEffort(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "events.db")
	code := runMain([]string{
		"-prompt", "你好",
		"-model", "test-model",
		"-db", db,
		// 端点指向本地拒连端口：**万一**校验没生效，也不会真去连外网（快速失败）。
		"-base-url", "http://127.0.0.1:1/v1",
		"-reasoning-effort", "turbo",
	})
	if code != 2 {
		t.Fatalf("非法 reasoning_effort 的退出码 = %d，want 2（启动期 fail-close）", code)
	}
	// 事件库不该被创建（连装配都没走到）。
	if _, err := os.Stat(db); err == nil {
		t.Error("非法档位不该装配出事件库")
	}
}

// TestRunAcceptsKnownReasoningEffort：合法档位不拦（白名单只是闸门，不是过滤器）。
func TestRunAcceptsKnownReasoningEffort(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "events.db")
	// 用一个必然失败的端点（不真调模型）：只要**不是**因为档位被拒而退出即可。
	// 这里断言的重点：档位合法 → 不在档位校验这一步退出（2）。
	// 模型调用失败会返回 1（装配/运行失败），与"档位被拒"（2）区分得开。
	code := runMain([]string{
		"-prompt", "你好",
		"-model", "test-model",
		"-db", db,
		"-base-url", "http://127.0.0.1:1/v1",
		"-reasoning-effort", "xhigh",
	})
	if code == 2 {
		t.Fatal("合法档位不该被拒（退出码 2 = 配置被拒）")
	}
}

// TestResumeRejectsUnknownReasoningEffort：`alasol resume -reasoning-effort turbo` → 拒。
func TestResumeRejectsUnknownReasoningEffort(t *testing.T) {
	db := filepath.Join(t.TempDir(), "resume.db")
	session, _ := seedCrashedSession(t, db)
	code := resumeMain([]string{
		"-session", string(session),
		"-db", db,
		"-verify", "none",
		"-reasoning-effort", "turbo",
	})
	if code != 2 {
		t.Fatalf("非法 reasoning_effort 的退出码 = %d，want 2（启动期 fail-close）", code)
	}
}

// TestResumeRejectsUnknownReasoningEffortFromFile：配置文件里的坏档位同样被拒
// （flag > 文件的优先级：文件里的值也要过白名单）。
func TestResumeRejectsUnknownReasoningEffortFromFile(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(cfg, []byte("reasoning_effort = \"turbo\"\n"), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	db := filepath.Join(dir, "resume.db")
	session, _ := seedCrashedSession(t, db)
	code := resumeMain([]string{
		"-session", string(session),
		"-db", db,
		"-verify", "none",
		"-config", cfg,
	})
	if code != 2 {
		t.Fatalf("配置文件里的非法档位：退出码 = %d，want 2", code)
	}
}

// TestServeChatTuiRejectUnknownReasoningEffort（F2）：serve / chat / tui 三条路径的非法档位
// → **exit 2**（与 run / resume 同一口径："配置被拒 = 2"）。
//
// 回归背景：host.New 失败一律 exit 1（"装配失败"），而 run / resume 的配置被拒是 exit 2
// → 同一个坏档位在 serve 下报 1、在 run 下报 2，脚本没法用退出码区分"配置写错了"和
// "跑挂了"。
func TestServeChatTuiRejectUnknownReasoningEffort(t *testing.T) {
	cases := []struct {
		name string
		fn   func([]string) int
	}{
		{"serve", serveMain},
		{"chat", chatMain},
		{"tui", tuiMain},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{
				"-model", "test-model",
				"-db", filepath.Join(t.TempDir(), "x.db"),
				"-base-url", "http://127.0.0.1:1/v1",
				"-reasoning-effort", "turbo",
			}
			code := runCmdWithTimeout(t, tc.fn, args, 30*time.Second)
			if code != 2 {
				t.Fatalf("%s：非法 reasoning_effort 的退出码 = %d，want 2（配置被拒）", tc.name, code)
			}
		})
	}
}

// runCmdWithTimeout 跑一条命令并等它返回（超时 = 失败）。
//
// 为什么要有超时：这几条命令在校验没过时**应该**立刻返回；若校验被改坏，chat / tui 会
// 一路走到读 stdin / 起界面 → 测试会挂死（挂死的测试比红的测试更难查）。
func runCmdWithTimeout(t *testing.T, fn func([]string) int, args []string, d time.Duration) int {
	t.Helper()
	ch := make(chan int, 1)
	go func() { ch <- fn(args) }()
	select {
	case code := <-ch:
		return code
	case <-time.After(d):
		t.Fatalf("命令在 %s 内没返回（可能阻塞在 stdin / 起服务）", d)
		return -1
	}
}

// TestResolveReasoningEffortSharedRules：共享层的取值 + 校验口径（flag > 文件；空 = 不传；
// 非法 = 拒）。
func TestResolveReasoningEffortSharedRules(t *testing.T) {
	cases := []struct {
		name    string
		flagVal string
		fileVal string
		want    string
		wantErr bool
	}{
		{"都没设", "", "", "", false},
		{"flag 优先", "high", "low", "high", false},
		{"回落文件", "", "medium", "medium", false},
		{"trim 后判", "  xhigh  ", "", "xhigh", false},
		{"非法 flag", "turbo", "", "", true},
		{"非法文件", "", "Turbo", "", true},
		{"大小写敏感", "High", "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := host.ResolveReasoningEffort(tc.flagVal, tc.fileVal)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ResolveReasoningEffort(%q, %q) = %q，want 报错", tc.flagVal, tc.fileVal, got)
				}
				if !strings.Contains(err.Error(), "ReasoningEffort") {
					t.Errorf("错误文案应点名 ReasoningEffort: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveReasoningEffort(%q, %q) 意外报错: %v", tc.flagVal, tc.fileVal, err)
			}
			if got != tc.want {
				t.Errorf("ResolveReasoningEffort(%q, %q) = %q，want %q", tc.flagVal, tc.fileVal, got, tc.want)
			}
		})
	}
}
