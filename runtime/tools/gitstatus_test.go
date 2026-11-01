package tools

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
)

// fakeGit 造一个注入了 lookPath/run 的 git_status 执行器：确定性复现"正常返回"与
// "git 不可用"两条路径（不依赖机器上是否真装了 git、是否真在仓库里）。
func fakeGit(lookErr error, outputs map[string]string) *GitStatusExecutor {
	return &GitStatusExecutor{
		lookPath: func(string) (string, error) {
			if lookErr != nil {
				return "", lookErr
			}
			return "/usr/bin/git", nil
		},
		run: func(_ context.Context, _ string, args ...string) ([]byte, error) {
			key := strings.Join(args, " ")
			if out, ok := outputs[key]; ok {
				return []byte(out), nil
			}
			return nil, errors.New("git: 未预期的子命令 " + key)
		},
	}
}

// J3 正常返回：branch / dirty / last_commit 三段都在，且是只读形态（read 元数据）。
func TestGitStatusNormalReturn(t *testing.T) {
	e := fakeGit(nil, map[string]string{
		"rev-parse --abbrev-ref HEAD":             "master\n",
		"status --porcelain":                      " M core/loop/loop.go\n?? scratch.txt\n",
		"log -1 --pretty=format:%h %s (%cr, %an)": "abc1234 tweak loop (2 hours ago, Yorha",
	})
	res, err := e.Execute(context.Background(), intent(toolNameGitStatus, gitStatusArgs{}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	text := textOf(t, res)
	for _, want := range []string{"branch: master", "dirty: true", "abc1234"} {
		if !strings.Contains(text, want) {
			t.Errorf("返回文本缺 %q：\n%s", want, text)
		}
	}
}

// 干净工作区 → dirty: false。
func TestGitStatusCleanTree(t *testing.T) {
	e := fakeGit(nil, map[string]string{
		"rev-parse --abbrev-ref HEAD":             "main\n",
		"status --porcelain":                      "",
		"log -1 --pretty=format:%h %s (%cr, %an)": "def5678 clean (1 minute ago, Bot)",
	})
	res, err := e.Execute(context.Background(), intent(toolNameGitStatus, gitStatusArgs{Path: "."}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	text := textOf(t, res)
	if !strings.Contains(text, "dirty: false") {
		t.Errorf("干净工作区应报 dirty: false：\n%s", text)
	}
	if strings.Contains(text, "dirty: true") {
		t.Errorf("干净工作区不应报 dirty: true：\n%s", text)
	}
}

// git 不可用 → 明确、可自我修正的错误（M5），不是底层 exec 报错透传。
func TestGitStatusGitUnavailable(t *testing.T) {
	e := fakeGit(exec.ErrNotFound, nil)
	_, err := e.Execute(context.Background(), intent(toolNameGitStatus, gitStatusArgs{}))
	if err == nil {
		t.Fatal("git 不可用时应报错")
	}
	if !strings.Contains(err.Error(), "git") || !strings.Contains(err.Error(), "PATH") {
		t.Errorf("错误文案应点名 git 与 PATH（模型可自我修正）：%v", err)
	}
}

// 不在仓库里（rev-parse 失败）→ 报错点名 path，而不是把底层 git 输出丢给模型。
func TestGitStatusNotARepo(t *testing.T) {
	e := &GitStatusExecutor{
		lookPath: func(string) (string, error) { return "/usr/bin/git", nil },
		run: func(_ context.Context, _ string, args ...string) ([]byte, error) {
			if len(args) > 0 && args[0] == "rev-parse" {
				return nil, errors.New("fatal: not a git repository")
			}
			return nil, errors.New("unreachable")
		},
	}
	_, err := e.Execute(context.Background(), intent(toolNameGitStatus, gitStatusArgs{Path: "/nowhere"}))
	if err == nil {
		t.Fatal("非仓库目录应报错")
	}
	if !strings.Contains(err.Error(), "git 仓库") {
		t.Errorf("错误文案应点名'git 仓库'：%v", err)
	}
}

// 描述元数据（D12，参照 read）：side_effect=read、幂等、replay=safe、不需人审。
func TestGitStatusDescriptorShape(t *testing.T) {
	d := GitStatusDescriptor()
	if d.Name != toolNameGitStatus {
		t.Errorf("Name = %q，期望 %q", d.Name, toolNameGitStatus)
	}
	if d.SideEffect != contracts.SideEffectRead {
		t.Errorf("SideEffect = %q，期望 read", d.SideEffect)
	}
	if !d.Idempotent {
		t.Error("Idempotent 应为 true（只读查询可安全重放）")
	}
	if d.Replay != contracts.ReplayKindSafe {
		t.Errorf("Replay = %q，期望 safe", d.Replay)
	}
	if d.Approvable {
		t.Error("Approvable 应为 false（只读不触发 ask 人审）")
	}
	if d.Description == nil || *d.Description == "" {
		t.Error("Description 不应为空")
	}
}

// 真机冒烟：在本仓（确实是个 git 仓库）跑一次真实 os/exec——证明 exec.CommandContext
// 那条默认路径真能拿到 branch/dirty/commit（注入替身测不到这一层）。git 缺席时跳过。
func TestGitStatusRealRepo(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("系统无 git，跳过真机冒烟")
	}
	// 指到本模块根目录（tests 的 cwd 是 runtime/tools，往上两层是仓库根）。
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	root := filepath.Dir(filepath.Dir(wd)) // runtime/tools → runtime → 仓库根
	e := NewGitStatus()
	res, err := e.Execute(context.Background(), intent(toolNameGitStatus, gitStatusArgs{Path: root}))
	if err != nil {
		t.Fatalf("真实 git 执行失败: %v", err)
	}
	text := textOf(t, res)
	if !strings.Contains(text, "branch:") {
		t.Errorf("真机输出应含 branch:：\n%s", text)
	}
	if !strings.Contains(text, "dirty:") {
		t.Errorf("真机输出应含 dirty:：\n%s", text)
	}
}
