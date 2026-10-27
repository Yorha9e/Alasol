package durable_test

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/durable"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// kill -9 语义验收①：真实子进程写 intent 后被 Kill → 父进程 Resume → in-flight 三类处置正确、
// 不重复副作用、幂等。用真 SQLite 文件库跨进程验证「intent 已 durable、无 result = 未知结果」。
const (
	kill9EnvDB      = "ALASOL_KILL9_DB"
	kill9EnvSession = "ALASOL_KILL9_SESSION"
	kill9EnvAgent   = "ALASOL_KILL9_AGENT"
	kill9EnvHelper  = "ALASOL_KILL9_HELPER"
)

// TestDurableKill9Helper 是**子进程**入口（父进程用 os/exec 重新执行本测试二进制并设
// GO_WANT_HELPER 环境变量）。它把三条 intent（safe/verify/unsafe）durable 落盘、不落 result，
// 打印 READY 后挂起，等父进程 Kill——模拟「执行到一半、进程被 kill -9、有意图无结果」。
func TestDurableKill9Helper(t *testing.T) {
	if os.Getenv(kill9EnvHelper) != "1" {
		t.Skip("helper：仅在被父进程 spawn 时运行")
	}
	db := os.Getenv(kill9EnvDB)
	session := contracts.Ulid(os.Getenv(kill9EnvSession))
	agent := contracts.Ulid(os.Getenv(kill9EnvAgent))
	if db == "" || !core.ValidUlid(session) || !core.ValidUlid(agent) {
		fmt.Fprintln(os.Stderr, "helper: 参数不完整")
		os.Exit(3)
	}
	st, err := sqlite.Open(db, sqlite.Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper: open:", err)
		os.Exit(3)
	}
	// 三条 intent：safe（echo）、verify（push，带 git-commit anchor）、unsafe（sendmail）。
	intents := []contracts.Event{
		evIntent(session, agent, "c-safe", "echo", contracts.ReplayKindSafe, nil),
		evIntent(session, agent, "c-verify", "push", contracts.ReplayKindVerify,
			[]contracts.Anchor{{Kind: "git-commit", Value: "cafe1234"}}),
		evIntent(session, agent, "c-unsafe", "sendmail", contracts.ReplayKindUnsafe, nil),
	}
	if _, err := st.Append(context.Background(), intents); err != nil {
		fmt.Fprintln(os.Stderr, "helper: append intents:", err)
		os.Exit(3)
	}
	_ = st.Close()
	// intent 已 durable 落盘（WAL 提交）→ 通知父进程 → 挂起等 Kill。
	fmt.Println("READY")
	_ = os.Stdout.Sync()
	select {} // 阻塞到被 Kill（父进程 kill -9 语义；ctx 超时是兜底）
}

// TestResumeKill9RealSubprocess 是 kill -9 验收①的父进程侧。
func TestResumeKill9RealSubprocess(t *testing.T) {
	db := filepath.Join(t.TempDir(), "kill9.db")
	session := core.NewUlid()
	agent := core.NewUlid()

	// 起子进程写 intent；ctx 超时是兜底（父进程会显式 Kill）。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestDurableKill9Helper", "-test.v")
	cmd.Env = append(os.Environ(),
		kill9EnvHelper+"=1",
		kill9EnvDB+"="+db,
		kill9EnvSession+"="+string(session),
		kill9EnvAgent+"="+string(agent),
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动 helper 子进程: %v", err)
	}
	// 保证子进程被回收（显式 Kill + Wait；ctx 超时兜底）。
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()

	// 等 helper 打印 READY（intent 已落盘）再 Kill。
	sc := bufio.NewScanner(stdout)
	var ready bool
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "READY" {
			ready = true
			break
		}
	}
	if !ready {
		t.Fatal("helper 未打印 READY（intent 未落盘就被杀？）")
	}
	// kill -9 语义：执行中杀掉（有 intent、无 result）。
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("Kill helper: %v", err)
	}
	_ = cmd.Wait()

	// 父进程重开库：三条 intent 都在、都无 result（未知结果）。
	st, err := sqlite.Open(db, sqlite.Options{})
	if err != nil {
		t.Fatalf("父进程重开库: %v", err)
	}
	defer st.Close()
	bg := context.Background()
	inflight, err := durable.FindInFlight(bg, st, session, 1)
	if err != nil {
		t.Fatalf("FindInFlight: %v", err)
	}
	if len(inflight) != 3 {
		t.Fatalf("kill -9 后 in-flight = %d, want 3（有意图无结果）", len(inflight))
	}

	// Resume：mock Verifier 让 c-verify 核验通过；计数 Replayer 观察副作用次数。
	rp := newMockReplayer(st)
	vf := mockVerifier{pass: map[contracts.ProviderId]bool{"c-verify": true}}
	rep, err := durable.Resume(bg, st, session, durable.ResumeOptions{Verifier: vf, Replayer: rp})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	// 三类处置：safe→replayed / verify(通过)→verified / unsafe→manual。
	if got := dispositionOf(t, rep, "c-safe"); got != contracts.ResumeDispositionReplayed {
		t.Fatalf("c-safe = %q, want replayed", got)
	}
	if got := dispositionOf(t, rep, "c-verify"); got != contracts.ResumeDispositionVerified {
		t.Fatalf("c-verify = %q, want verified", got)
	}
	if got := dispositionOf(t, rep, "c-unsafe"); got != contracts.ResumeDispositionManual {
		t.Fatalf("c-unsafe = %q, want manual", got)
	}

	// 不重复副作用：safe 恰好重放 1 次；verify 通过**不**重放；unsafe 不动。
	if rp.callN("c-safe") != 1 {
		t.Fatalf("safe 副作用次数 = %d, want 1", rp.callN("c-safe"))
	}
	if rp.callN("c-verify") != 0 {
		t.Fatalf("verify 通过不应有副作用，得到 %d", rp.callN("c-verify"))
	}
	if rp.callN("c-unsafe") != 0 {
		t.Fatalf("unsafe 不应有副作用，得到 %d", rp.callN("c-unsafe"))
	}

	// 幂等：再 Resume 一次，无新副作用、无新处置。
	rep2, err := durable.Resume(bg, st, session, durable.ResumeOptions{Verifier: vf, Replayer: rp})
	if err != nil {
		t.Fatalf("Resume#2: %v", err)
	}
	if len(rep2.Dispositions) != 0 {
		t.Fatalf("二次 resume 重复处置: %+v", rep2.Dispositions)
	}
	if rp.callN("c-safe") != 1 || rp.callN("c-verify") != 0 || rp.callN("c-unsafe") != 0 {
		t.Fatalf("二次 resume 产生重复副作用: safe=%d verify=%d unsafe=%d",
			rp.callN("c-safe"), rp.callN("c-verify"), rp.callN("c-unsafe"))
	}
}
