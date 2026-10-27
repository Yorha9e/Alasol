package verify

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
)

func gitAvailable(t *testing.T) {
	t.Helper()
	if _, err := gitExecutable(); err != nil {
		t.Skipf("git 不可用，跳过：%v", err)
	}
}

// runGit 在测试里跑一条 git（测试代码允许用 shell，同 runtime/gate 测试约定），失败即 t.Fatal。
func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	b, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(b))
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("写 %s: %v", path, err)
	}
}

// initRepo 建一个临时 git 仓库并做首次 commit，返回仓库路径与 commit sha。
func initRepo(t *testing.T) (repoPath, commit string) {
	t.Helper()
	gitAvailable(t)
	dir := t.TempDir()
	runGit(t, dir, "init", "-q")
	runGit(t, dir, "config", "core.autocrlf", "false")
	writeFile(t, filepath.Join(dir, "a.txt"), "committed-a")
	runGit(t, dir, "add", "a.txt")
	runGit(t, dir, "-c", "user.email=t@t", "-c", "user.name=t", "-c", "commit.gpgsign=false", "commit", "-q", "-m", "init")
	commit = runGit(t, dir, "rev-parse", "HEAD")
	return dir, commit
}

func commitMore(t *testing.T, repo, name, content, msg string) {
	t.Helper()
	writeFile(t, filepath.Join(repo, name), content)
	runGit(t, repo, "add", name)
	runGit(t, repo, "-c", "user.email=t@t", "-c", "user.name=t", "-c", "commit.gpgsign=false", "commit", "-q", "-m", msg)
}

// TestGitCommitVerifierExists：存在的 commit → 核验通过；不存在的 sha → 不通过；无锚点 → 出错。
func TestGitCommitVerifierExists(t *testing.T) {
	repo, commit := initRepo(t)
	v := GitCommitVerifier{RepoDir: repo, Mode: ModeExists}
	ctx := context.Background()

	ok, err := v.Verify(ctx, contracts.InFlightOp{
		ToolCallID: "c1", ToolName: "push", Replay: contracts.ReplayKindVerify,
		Anchors: []contracts.Anchor{{Kind: "git-commit", Value: commit}},
	})
	if err != nil || !ok {
		t.Fatalf("存在的 commit 应核验通过，得到 ok=%v err=%v", ok, err)
	}

	ok, err = v.Verify(ctx, contracts.InFlightOp{
		ToolCallID: "c2", ToolName: "push", Replay: contracts.ReplayKindVerify,
		Anchors: []contracts.Anchor{{Kind: "git-commit", Value: strings.Repeat("0", 40)}},
	})
	if err != nil || ok {
		t.Fatalf("不存在的 sha 应核验不通过，得到 ok=%v err=%v", ok, err)
	}

	if _, err = v.Verify(ctx, contracts.InFlightOp{ToolCallID: "c3", ToolName: "push"}); err == nil {
		t.Fatal("无 git-commit 锚点应返回错误（转 manual）")
	}
}

// TestGitCommitVerifierAncestor：HEAD 自身是其祖先；再提交一个后，第一个 commit 仍是祖先。
func TestGitCommitVerifierAncestor(t *testing.T) {
	repo, first := initRepo(t)
	v := GitCommitVerifier{RepoDir: repo, Mode: ModeAncestor}
	ctx := context.Background()

	ok, err := v.Verify(ctx, contracts.InFlightOp{
		ToolCallID: "c1", Replay: contracts.ReplayKindVerify,
		Anchors: []contracts.Anchor{{Kind: "git-commit", Value: first}},
	})
	if err != nil || !ok {
		t.Fatalf("HEAD 应是自身的祖先，得到 ok=%v err=%v", ok, err)
	}

	commitMore(t, repo, "b.txt", "b", "second")
	ok, err = v.Verify(ctx, contracts.InFlightOp{
		ToolCallID: "c2", Replay: contracts.ReplayKindVerify,
		Anchors: []contracts.Anchor{{Kind: "git-commit", Value: first}},
	})
	if err != nil || !ok {
		t.Fatalf("第一个 commit 应仍是新 HEAD 的祖先，得到 ok=%v err=%v", ok, err)
	}

	// 一个尚未存在的 commit 不是 HEAD 的祖先。
	ok, err = v.Verify(ctx, contracts.InFlightOp{
		ToolCallID: "c3", Replay: contracts.ReplayKindVerify,
		Anchors: []contracts.Anchor{{Kind: "git-commit", Value: strings.Repeat("a", 40)}},
	})
	if err != nil || ok {
		t.Fatalf("不存在的 commit 不应是祖先，得到 ok=%v err=%v", ok, err)
	}
}
