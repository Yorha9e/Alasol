package remote_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/runtime/cap/remote"
)

// ---------------------------------------------------------------------------
// Fs 的 env 前缀（p3-2）：四条命令**也**走 `env -i`（整份替换 + 远端侧 PATH）
// ---------------------------------------------------------------------------

// TestRemoteFsUsesEnvIAndRemotePATH 钉死 p3-2：Fs 的四条命令（cat / stat / ls 与
// `cat > path`）**也**在 `env -i` 前缀里跑——两个后果：
//
//   - 远端 login shell 继承来的环境（远端机器上的凭据等）在 Fs 命令里**不可见**
//     （与 Run 同一条纪律，不给 Fs 开后门）；
//   - **Config.DefaultRemotePATH 对 Fs 同样生效**（不填 Env 时 Fs 命令不带 env
//     前缀，远端既继承环境、又找不到覆盖后的 PATH 里的命令——两个毛病一起来）。
//
// 变异自证③的红灯所在：把 Fs 的 Env 去掉（fsRemoteCmd 不填 Env）→ 这里红
// （命令行不再有 env -i 前缀、PATH 也不再是远端侧的）。
func TestRemoteFsUsesEnvIAndRemotePATH(t *testing.T) {
	// ① 四条命令都带 `env -i` 前缀 + **远端侧** PATH（默认 /usr/bin:/bin）。
	fake := &fakeSSH{}
	env := newRemoteEnvStdin(t, fake)
	ctx := context.Background()
	calls := []struct {
		name string
		call func() error
	}{
		{"ReadFile", func() error { _, err := env.ReadFile(ctx, "/data/x"); return err }},
		{"WriteFile", func() error { return env.WriteFile(ctx, "/data/x", []byte("d")) }},
		{"Stat", func() error { _, err := env.Stat(ctx, "/data/x"); return err }},
		{"ListDir", func() error { _, err := env.ListDir(ctx, "/data/x"); return err }},
	}
	for _, c := range calls {
		fake.mu.Lock()
		fake.calls = nil
		fake.mu.Unlock()
		_ = c.call() // Stat 的解析错误不影响命令行断言
		got := fake.remoteCmdOf()
		if !strings.HasPrefix(got, "env -i 'PATH=/usr/bin:/bin' ") {
			t.Errorf("%s：Fs 命令应带 env -i 前缀 + 远端侧 PATH，得到 %q", c.name, got)
		}
	}

	// ② **链路**钉子：Config.DefaultRemotePATH → remote.New → Fs 命令行。
	// （只测纯函数堵不住"Env 装配时把 remotePATH 丢了"这类链路断点。）
	fake2 := &fakeSSH{}
	env2, err := remote.New(remote.Config{
		Host:              "user@harness",
		DefaultRemotePATH: "/opt/homebrew/bin:/usr/bin:/bin",
		ExecStdin:         fake2.runStdin,
	})
	if err != nil {
		t.Fatalf("remote.New: %v", err)
	}
	if _, err := env2.ReadFile(context.Background(), "/data/x"); err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	got := fake2.remoteCmdOf()
	if !strings.Contains(got, "'PATH=/opt/homebrew/bin:/usr/bin:/bin'") {
		t.Errorf("Config.DefaultRemotePATH 应沿链路生效到 Fs 命令的 PATH：%q", got)
	}
	if strings.Contains(got, "'PATH=/usr/bin:/bin'") {
		t.Errorf("覆盖后不该再用包内默认 PATH：%q", got)
	}
}
