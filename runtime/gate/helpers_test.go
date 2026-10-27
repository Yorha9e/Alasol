package gateexec

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// gitAvailable 在没有 git 的环境（极少数 CI）跳过测试，而不是失败。
func gitAvailable(t *testing.T) {
	t.Helper()
	if _, err := gitExecutable(); err != nil {
		t.Skipf("git 不可用，跳过：%v", err)
	}
}

// runGitTest 在测试里跑一条 git（用被测的 gitRun：绝对路径、不走 shell），失败即 t.Fatal。
func runGitTest(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, code, err := gitRun(context.Background(), dir, args...)
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	if code != 0 {
		t.Fatalf("git %v 退出码 %d：%s", args, code, out)
	}
	return out
}

// initRepo 建一个临时 git 仓库并做首次 commit（含 a.txt="committed-a"），返回仓库路径与 commit sha。
func initRepo(t *testing.T) (repoPath, commit string) {
	t.Helper()
	gitAvailable(t)
	dir := t.TempDir()
	runGitTest(t, dir, "init", "-q")
	// 关掉 autocrlf，保证跨平台文件内容确定（否则 Windows 上可能被 CRLF 化）。
	runGitTest(t, dir, "config", "core.autocrlf", "false")
	writeFile(t, filepath.Join(dir, "a.txt"), "committed-a")
	runGitTest(t, dir, "add", "a.txt")
	runGitTest(t, dir, "-c", "user.email=t@t", "-c", "user.name=t", "-c", "commit.gpgsign=false", "commit", "-q", "-m", "init")
	commit = strings.TrimSpace(runGitTest(t, dir, "rev-parse", "HEAD"))
	return dir, commit
}

// commitFile 改/加一个文件并提交，返回新 commit sha（用于造第二个 commit）。
func commitFile(t *testing.T, repo, name, content, msg string) string {
	t.Helper()
	writeFile(t, filepath.Join(repo, name), content)
	runGitTest(t, repo, "add", name)
	runGitTest(t, repo, "-c", "user.email=t@t", "-c", "user.name=t", "-c", "commit.gpgsign=false", "commit", "-q", "-m", msg)
	return strings.TrimSpace(runGitTest(t, repo, "rev-parse", "HEAD"))
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("写 %s：%v", path, err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读 %s：%v", path, err)
	}
	return string(b)
}

// ---- 跨平台的门禁命令构造（测试代码里允许出现 shell，同 runtime/cap/local 的测试约定）----

// echoEnvCmd 打印三个约定 env（各占一行），用于验证注入。
func echoEnvCmd() string {
	if runtime.GOOS == "windows" {
		return "echo %CI_COMMIT%&echo %CI_WORKTREE%&echo %CI_PORT_BASE%"
	}
	// 原始字符串：保留 \n 交给 bash 的 printf 解释。
	return `printf '%s\n%s\n%s\n' "$CI_COMMIT" "$CI_WORKTREE" "$CI_PORT_BASE"`
}

// passCmd 一个退出码 0 的命令。
func passCmd() string { return "echo gate-ok" }

// failCmd 一个退出码 3 的命令（用于验证"非零退出不是 error"）。
func failCmd() string {
	if runtime.GOOS == "windows" {
		return "echo failing&exit 3"
	}
	return "echo failing; exit 3"
}

// sleepCmd 一个约 sleepMS 毫秒的命令（用于验证 ElapsedMS）。
func sleepCmd(sleepMS int) string {
	if runtime.GOOS == "windows" {
		// ping -n 2 两次 echo 间隔约 1s（足够 > 100ms 下限）。
		return "ping -n 2 127.0.0.1 >nul"
	}
	// 秒级 sleep，3 位小数（sleep 接受小数秒）。
	secs := float64(sleepMS) / 1000.0
	return "sleep " + strconv.FormatFloat(secs, 'f', 3, 64)
}

// bigOutputCmd 产生 >64KB 的输出，首行 SENTINEL_FIRST 标记、末行含 "line 9000"（验证 LogTail 留尾部）。
func bigOutputCmd() string {
	if runtime.GOOS == "windows" {
		return "echo SENTINEL_FIRST&for /L %i in (1,1,9000) do @echo line %i xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	}
	return `echo SENTINEL_FIRST; i=1; while [ $i -le 9000 ]; do echo "line $i xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"; i=$((i+1)); done`
}

// makeLink 造一个指向 target 的链接：Windows 用 junction（mklink /J，免管理员），Unix 用符号链接。
func makeLink(t *testing.T, link, target string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		cmd := exec.Command("cmd", "/c", "mklink", "/J", link, target)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Skipf("无法造 junction（mklink /J）：%v：%s", err, out)
		}
		return
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("无法造符号链接：%v", err)
	}
}
