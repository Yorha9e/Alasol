package remote

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// cappedBuffer 的截断口径（p2-1）：**静默截断到上限** + **绝不短写**
// ---------------------------------------------------------------------------

// TestCappedBufferTruncationDoesNotShortWrite 钉死 p2-1：输出超过上限时，
// cappedBuffer **静默截断**（内容停在上限），且 **Write 不短写**（返回**原长度**、
// err=nil）。
//
// # 为什么"不短写"是硬要求
//
// io.Copy 的契约：n < len(p) 且 err == nil = **短写** → io.Copy 返回 **ErrShortWrite**
// → os/exec 的拷贝 goroutine 把它当**真错误**回给 cmd.Wait() → osExec 判成硬错
// （"等待 ssh 结束失败: short write"）。于是"输出超限"这件事的**行为随管道分块
// 边界摆动**：跨过上限的那一块刚好落在块尾时静默截断、落在块中间时报错。
// 统一成"静默截断到上限"（room<=0 分支与截断分支同口径）后行为不再摇摆。
//
// 变异自证②的红灯所在：把截断分支改回 `return b.buf.Write(p)`（短写）→ 这里红。
func TestCappedBufferTruncationDoesNotShortWrite(t *testing.T) {
	var b cappedBuffer
	// 先填到上限。
	if _, err := b.Write(make([]byte, maxCapturedOutput)); err != nil {
		t.Fatalf("填满上限: %v", err)
	}
	// 跨过上限的那一块 → 必须**不短写**。
	chunk := []byte("0123456789")
	n, err := b.Write(chunk)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(chunk) {
		t.Errorf("Write 返回 %d，want %d（短写 → io.Copy 报 ErrShortWrite → 整次执行被判硬错）",
			n, len(chunk))
	}
	if got := b.buf.Len(); got != maxCapturedOutput {
		t.Errorf("缓冲长度 = %d，want %d（超限应静默截断到上限，不多写）", got, maxCapturedOutput)
	}
	if b.dropped != len(chunk) {
		t.Errorf("dropped = %d，want %d", b.dropped, len(chunk))
	}
}

// TestCappedBufferTruncationAcrossBoundaryDoesNotShortWrite 钉死 p2-1 的**边界一格**：
// 跨过上限的那一块**落在中间**（前半截收下、后半截丢掉）时，也**不短写**。
//
// 这一格正是"行为随管道分块摆动"的现场：截断分支返回的是**截断后**的长度时，
// io.Copy 立刻判 ErrShortWrite。
func TestCappedBufferTruncationAcrossBoundaryDoesNotShortWrite(t *testing.T) {
	var b cappedBuffer
	// 先填到"上限 - 10"（留 10 字节的余量）。
	if _, err := b.Write(make([]byte, maxCapturedOutput-10)); err != nil {
		t.Fatalf("预填: %v", err)
	}
	chunk := []byte("0123456789ABCDEF") // 16 字节 > 余量 10 字节 → 落在块中间
	n, err := b.Write(chunk)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(chunk) {
		t.Errorf("Write 返回 %d，want %d（截断分支不得短写）", n, len(chunk))
	}
	if got := b.buf.Len(); got != maxCapturedOutput {
		t.Errorf("缓冲长度 = %d，want %d（应恰好停在上限）", got, maxCapturedOutput)
	}
	if b.dropped != 6 {
		t.Errorf("dropped = %d，want 6（超出上限的 6 字节应被丢掉）", b.dropped)
	}
	// 收下的那 10 字节是**原内容的前缀**（不是别的）。
	tail := b.buf.Bytes()[maxCapturedOutput-10:]
	if !bytes.Equal(tail, chunk[:10]) {
		t.Errorf("截断后保留的内容 = %q，want %q", tail, chunk[:10])
	}
}

// ---------------------------------------------------------------------------
// 生产路径 osExec：输出超限 → 静默截断，**不**报 short write
// ---------------------------------------------------------------------------

// fakeBigOutputEnvVar 是"假 ssh 大输出"模式的环境变量名（helper-process 模式）。
const fakeBigOutputEnvVar = "GO_ALASOL_FAKE_SSH_BIG"

// TestCappedHelperProcess 是**假 ssh**（helper-process 模式）：只有
// GO_ALASOL_FAKE_SSH_BIG=big-output 时才往 stdout 写**超上限**的字节，
// 否则当普通测试空跑。
//
// # 为什么**分两段**写（先 1 字节、停 100ms、再写剩下的）
//
// 管道的**读块大小是平台相关的**（Windows 匿名管道 4KB、Linux 64KB），而上限
// 32MB 恰好是它们的**整数倍**——一次写到底的话，上限边界会**正好**落在读块边界上，
// 截断分支根本不触发（短写也就不会现形，"超限"看起来一切正常）。
// 先写 1 字节把边界**错开** → 跨过上限的那一次 Write 必然 **len(p) > room**
// （上限那一格落在**块中间**）——短写就此现形。
func TestCappedHelperProcess(t *testing.T) {
	if os.Getenv(fakeBigOutputEnvVar) != "big-output" {
		return // 普通测试进程：什么都不做
	}
	total := maxCapturedOutput + (1 << 20)
	if _, err := os.Stdout.Write([]byte("x")); err != nil {
		os.Exit(1)
	}
	time.Sleep(100 * time.Millisecond) // 让父进程把第一段读走（边界错开）
	buf := bytes.Repeat([]byte("x"), 32<<10)
	written := 1
	for written < total {
		n, err := os.Stdout.Write(buf)
		if err != nil {
			os.Exit(1)
		}
		written += n
	}
	os.Exit(0)
}

// TestOsExecBigOutputTruncatesSilently 钉死 p2-1 的**端到端**证据：生产路径 osExec
// 收到**超上限**的输出时——
//
//   - **不报错**（旧实现：截断分支短写 → io.Copy 报 ErrShortWrite → cmd.Wait 把它
//     当真错误 → osExec 判成硬错"等待 ssh 结束失败: short write"）；
//   - stdout 是**截断到上限**的内容（长度 = maxCapturedOutput，内容是前缀）。
//
// 变异自证②的红灯所在：把 cappedBuffer 的截断分支改回短写 → 这里红。
func TestOsExecBigOutputTruncatesSilently(t *testing.T) {
	t.Setenv(fakeBigOutputEnvVar, "big-output")
	res, err := osExec(context.Background(), os.Args[0],
		[]string{"-test.run=^TestCappedHelperProcess$"}, nil)
	if err != nil {
		t.Fatalf("输出超限不该是 error（口径：静默截断到上限）：%v", err)
	}
	if got := len(res.Stdout); got != maxCapturedOutput {
		t.Errorf("stdout 长度 = %d，want %d（超限应静默截断到上限，不报错）", got, maxCapturedOutput)
	}
	if res.ExitCode != 0 {
		// 短写时拷贝 goroutine 会带着 ErrShortWrite 提前退出 → 管道写端被关 →
		// 远端进程写管道失败（非 0 退出）。这是"短写"在端到端的另一张脸。
		t.Errorf("ExitCode = %d，want 0（拷贝被短写打断时，远端进程会被管道写失败带走）", res.ExitCode)
	}
	// 截断后保留的是**前缀**（内容没被搅乱）。
	if len(res.Stdout) > 0 {
		if res.Stdout[0] != 'x' || res.Stdout[len(res.Stdout)-1] != 'x' {
			t.Errorf("截断后的内容应是原输出的前缀（首尾都应是 'x'）")
		}
	}
}
