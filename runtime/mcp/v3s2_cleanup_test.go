package mcp

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// 本文件是 v3-S1 挂账转 v3-S2 清理的**逐条小回归**（11 §2 v3-S1 行末五项）：
//
//   - (a) 工具列表快照无动态刷新：连接时刻定格，Tools() 复用快照、不再发 tools/list；
//   - (b) stderr 条数闸：≤ maxStderrLines 条，超限停记 + 说明；
//   - (c) 装配期预算口径 + 失败路径 sess.Close 套短 deadline：失败路径有界返回；
//   - (d) 关停告警文案按 cmd!=nil 分：stdio"已杀进程树" / http"无进程可杀"；
//   - (e) stderr 脱敏 tab 分隔 scheme 变体："Bearer\tsk-x" 的值本体也打码。
//
// 每条都"先破坏、再断言会红"（变异自证见交付说明）。

// (a) 工具列表 = 连接时刻快照，不支持动态刷新。
//
// 判据：连接时发 1 次 tools/list；之后反复 Tools() 都复用快照（server 侧不再收到
// tools/list）——正是"连接时刻定格、不动态刷新"的语义。
func TestToolsListIsConnectTimeSnapshot(t *testing.T) {
	var lists atomic.Int32
	mw := func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if method == "tools/list" {
				lists.Add(1)
			}
			return next(ctx, method, req)
		}
	}
	tr, _, _ := startFakeMW(t, mw, fakeTool{name: "t", text: "ok"})
	conn := connectFake(context.Background(), t, "snap", tr)

	if got := lists.Load(); got != 1 {
		t.Fatalf("连接时应恰好发 1 次 tools/list（得到 %d）", got)
	}
	// 之后反复 Tools() 都复用快照，不再发 tools/list（连接时刻快照，不支持动态刷新）。
	for i := 0; i < 3; i++ {
		tools, err := conn.Tools(context.Background())
		if err != nil {
			t.Fatalf("Tools: %v", err)
		}
		if len(tools) != 1 || tools[0].Name != "t" {
			t.Fatalf("Tools 返回不符（第 %d 次）: %+v", i, tools)
		}
	}
	if got := lists.Load(); got != 1 {
		t.Fatalf("Tools() 又发出了 tools/list（得到 %d；应是连接时刻快照，不动态刷新）", got)
	}
}

// (b) stderr 条数闸：很多**短**行（每行都不超字节闸）也要按条数封顶。
func TestStderrSinkLineCountCap(t *testing.T) {
	buf, restore := captureMCPLog(t)
	defer restore()

	sink := newStderrSink(&ServerConfig{Name: "srv"})
	line := "short-noise-line\n" // ~16 字节：1500 条也远不到 64KB 字节闸
	for i := 0; i < maxStderrLines+500; i++ {
		if _, err := sink.Write([]byte(line)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	out := buf.String()
	// 必须有一条"已截断"的说明，且点名是**条数**闸（不是字节闸）。
	if !strings.Contains(out, "stderr 已截断") {
		t.Fatalf("缺截断说明: %q", out[:min(len(out), 200)])
	}
	if !strings.Contains(out, "行上限") {
		t.Fatalf("截断说明未点名条数闸: %q", out[:min(len(out), 200)])
	}
	// 逐条记录的条数被条数闸压住（每条日志含一个 line=，另加一条截断说明的 line=）。
	if got := strings.Count(out, "line="); got > maxStderrLines+1 {
		t.Fatalf("落日志 %d 条，超过条数闸允许的 %d（截断后仍在逐条记录）", got, maxStderrLines)
	}
}

// (c) 装配失败路径（连上但 tools/list 挂死）的收尾必须有界：connectSession 在
// timeout + 关停窗口内返回错误，不永久挂起（失败路径 sess.Close 套短 deadline）。
//
// **本条是量化冒烟**：握手 ctx 已被 TimeoutMs 解除 → 在飞请求已被 ctx 掐掉 → 裸
// sess.Close() 也会很快返回，所以"把失败路径改回裸 sess.Close()"这个变异**不会**
// 让本条变红。真正有杀伤力的那条是紧跟其后的
// TestConnectSessionFailurePathReapsChild（失败路径必须**收尸子进程**）。
func TestConnectSessionFailurePathBounded(t *testing.T) {
	tr, _, _ := startFakeMW(t, hangOnMethod("tools/list"), fakeTool{name: "t", text: "ok"})
	start := time.Now()
	_, err := connectSession(context.Background(), ServerConfig{
		Name: "fail", Transport: TransportStdio, Command: "x", TimeoutMs: 600,
	}, tr, nil)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("tools/list 挂死应报错（不能当成功）")
	}
	mustContain(t, err, "tools/list")
	// 失败路径收尾 = closeSessionBounded：**≤ TimeoutMs + stdio 收尾预算**（14s）。
	// 量化口径写死（不是"没卡死就行"）：600ms + 14s + 2s 调度余量。
	if elapsed > 600*time.Millisecond+closeBudgetStdio+2*time.Second {
		t.Fatalf("失败路径耗时 %v（超过 TimeoutMs+stdio 收尾预算 %v，可能永久挂起）",
			elapsed, 600*time.Millisecond+closeBudgetStdio)
	}
}

// TestConnectSessionFailurePathReapsChild：失败路径（连上但 tools/list 挂死）必须把
// **stdio 子进程收尸**（杀树 + Wait）——这是"失败路径确实走的是有界关闭"的可观测
// 证据（裸 sess.Close() 不会碰子进程 → 留下孤儿进程，违反"谁拉起谁收尸"）。
//
// 为什么这条有杀伤力：把失败路径改回裸 `sess.Close()` 时，子进程**不会被收尸**，
// 它还活着 → 本用例立刻变红（而"耗时"那种断言抓不到——裸 Close 反而更快）。
func TestConnectSessionFailurePathReapsChild(t *testing.T) {
	tr, _, _ := startFakeMW(t, hangOnMethod("tools/list"), fakeTool{name: "t", text: "ok"})
	cmd := startHangingChild(t) // 真实子进程（stdio 的形态）：自己不会退

	done := make(chan error, 1)
	go func() {
		_, err := connectSession(context.Background(), ServerConfig{
			Name: "fail", Transport: TransportStdio, Command: "x", TimeoutMs: 600,
		}, tr, cmd)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("tools/list 挂死应报错（不能当成功）")
		}
		mustContain(t, err, "tools/list")
	case <-time.After(30 * time.Second):
		t.Fatal("失败路径挂死（收尾没套短 deadline，裸 sess.Close() 会等在飞请求）")
	}

	// 子进程必须**已经不在了**（被收尸：杀树 + Wait）。
	// 判据用 Kill 的返回值：进程已结束 → Kill 报错（os.ErrProcessDone）；
	// 还活着 → Kill 成功（nil）→ 说明没人收尸它（孤儿进程）。
	if err := cmd.Process.Kill(); err == nil {
		t.Fatal("失败路径没收尸子进程（子进程还活着 → 孤儿进程）：失败路径没走有界关闭")
	}
}

// ---------------------------------------------------------------------------
// (c) 的**有杀伤力**版本：直接单测有界关闭
// ---------------------------------------------------------------------------

// hangCloser 是一个"Close 永不返回"的 closer（模拟 SDK 的 Close 等在飞请求收尾时卡死）。
type hangCloser struct {
	release chan struct{}
	once    sync.Once
}

func newHangCloser() *hangCloser {
	return &hangCloser{release: make(chan struct{})}
}

// Close 一直等到被放行（测试用 release 模拟"杀树后管道断开 → 在飞请求立刻失败"）。
func (c *hangCloser) Close() error {
	<-c.release
	return nil
}

// releaseCloser 放行 Close（幂等）。
func (c *hangCloser) releaseCloser() {
	c.once.Do(func() { close(c.release) })
}

// startHangingChild 起一个"永不自己退出"的子进程（stdio server 的真实形态：它不吭声）。
//
// 用**测试二进制自身**当被测进程（os.Args[0] + 环境变量开关）：跨平台、零外部依赖
// （Windows 没有 sleep 命令，Unix 没有 cmd）。
func startHangingChild(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestMCPHelperProcess")
	cmd.Env = append(os.Environ(), "ALASOL_MCP_HELPER=hang")
	if err := cmd.Start(); err != nil {
		t.Fatalf("起挂死子进程: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
	})
	return cmd
}

// TestMCPHelperProcess 是"挂死子进程"的辅助入口（只在被 startHangingChild 拉起时真跑）。
func TestMCPHelperProcess(t *testing.T) {
	if os.Getenv("ALASOL_MCP_HELPER") != "hang" {
		t.Skip("辅助进程（只在被 startHangingChild 拉起时跑）")
	}
	time.Sleep(10 * time.Minute)
}

// TestCloseSessionBoundedIsBoundedAndKillsTree：有界关闭面对"Close 永不返回"的
// session 时，必须**有界返回**并**杀进程树**（stdio 分支，cmd != nil）。
//
// 判据三条：① 有界（不超过 stdio 收尾预算）；② 杀树被调过（否则在飞请求永远等不到
// 管道断开，收尾只能靠"放弃等待"）；③ 关停不把失败抛给调用方（按"已收尸"处理）。
//
// **为什么必须单测有界关闭本身**：connectSession 的失败路径上握手 ctx 已经
// 被 TimeoutMs 解除 → 在飞请求已被 ctx 掐掉 → 裸 sess.Close() 也会很快返回——那种场景
// 测不出"Close 挂住"这个最坏情况。这里直接喂"永不返回的 close"+"永不退的子进程"，
// 才测得到真东西。
func TestCloseSessionBoundedIsBoundedAndKillsTree(t *testing.T) {
	closer := newHangCloser()
	defer closer.releaseCloser()
	cmd := startHangingChild(t)

	var killed atomic.Int32
	var killedCmd atomic.Value // 被杀的 cmd（确认杀的是**那个**子进程）
	kill := func(c *exec.Cmd) {
		killed.Add(1)
		killedCmd.Store(c)
		killTree(c)
		// 杀树后管道断开 → 在飞请求立刻失败（与 Close 注释第 2 步同款）。
		closer.releaseCloser()
	}

	start := time.Now()
	err := closeSessionBoundedWith(closer.Close, cmd, "srv", TransportStdio, kill)
	elapsed := time.Since(start)

	// ① 有界：不得越过 stdio 的收尾预算（14s）。
	if elapsed > closeBudgetStdio {
		t.Fatalf("有界关闭耗时 %v（超过 stdio 收尾预算 %v）", elapsed, closeBudgetStdio)
	}
	// ② **杀树必须发生在关停这一步**（而不是拖到收尸那一步）：杀树 → 管道断开 →
	//    在飞请求立刻失败 → 关停立刻收尾（≈ gracePeriod 2s）。若关停这一步没杀树，
	//    就会白等第二段窗口（killGrace 5s）再进收尸 → 耗时 ≥ 7s。
	if elapsed >= gracePeriod+killGrace {
		t.Fatalf("杀树后 session 没立刻收尾（耗时 %v）：killTree 应在关停这一步被调", elapsed)
	}
	// 关停不该把失败抛给调用方（按"已收尸"处理，只告警）。
	if err != nil {
		t.Fatalf("有界关闭不该报错: %v", err)
	}
	// ③ 杀树被调过，且杀的是那个子进程。
	if killed.Load() == 0 {
		t.Fatal("关停超时后应杀进程树（killTree 没被调）")
	}
	if got, _ := killedCmd.Load().(*exec.Cmd); got != cmd {
		t.Fatal("杀树杀的不是本次的子进程")
	}
}

// TestCloseSessionBoundedWithoutProcessIsBounded：http 分支（cmd == nil）：没有子进程
// 可杀，但仍必须有界返回（不许挂死）。
func TestCloseSessionBoundedWithoutProcessIsBounded(t *testing.T) {
	closer := newHangCloser()
	defer closer.releaseCloser()

	var killed atomic.Int32
	kill := func(c *exec.Cmd) {
		killed.Add(1)
		killTree(c)
	}

	start := time.Now()
	err := closeSessionBoundedWith(closer.Close, nil, "srv", TransportHTTP, kill)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("有界关闭不该报错: %v", err)
	}
	// 有界：关停两段窗口（2s+5s）+ 调度余量。留 2s 余量（不误判正常收尾），
	// 但绝不能是"无限等"。
	if elapsed > closeBudgetHTTP+2*time.Second {
		t.Fatalf("http 收尾耗时 %v（超过 http 收尾预算 %v）", elapsed, closeBudgetHTTP)
	}
	// http 没有子进程 → 不该有杀树动作（"无进程可杀"）。
	if killed.Load() != 0 {
		t.Fatalf("http 分支不该杀进程树（调了 %d 次）", killed.Load())
	}
}

// TestCloseSessionBoundedNilSessionIsNoop：session 为 nil 时**不许** panic、不许挂住。
//
// （N1：接口化之后"接口装 nil 指针"会让 `sess != nil` 为 true → goroutine 里空指针
// 解引用 panic。现在收**具体指针**并在入口判空，nil 直接返回。）
func TestCloseSessionBoundedNilSessionIsNoop(t *testing.T) {
	// 具体类型的 nil 指针（就是"接口装 nil 指针"那个坑的同款形态）。
	var typedNil *mcp.ClientSession
	if err := closeSessionBounded(typedNil, nil, "srv", TransportStdio); err != nil {
		t.Fatalf("nil session 应直接返回 nil，got %v", err)
	}
	// 可测形态：close 函数为 nil（没有 session 可关）——同样直接返回。
	if err := closeSessionBoundedWith(nil, nil, "srv", TransportHTTP, nil); err != nil {
		t.Fatalf("nil close 应直接返回 nil，got %v", err)
	}
}

// TestCloseBudgetConstantsMatchDocumentedNumbers：失败路径的**预算口径**写死数字
// （stdio ≈ TimeoutMs+14s、http ≈ TimeoutMs+7s），且常量必须真的由那两段窗口构成
// （不许常量与实际窗口脱节）。
func TestCloseBudgetConstantsMatchDocumentedNumbers(t *testing.T) {
	if closeBudgetStdio != 14*time.Second {
		t.Fatalf("stdio 收尾预算 = %v，文档写的是 14s", closeBudgetStdio)
	}
	if closeBudgetHTTP != 7*time.Second {
		t.Fatalf("http 收尾预算 = %v，文档写的是 7s", closeBudgetHTTP)
	}
	if closeBudgetHTTP != gracePeriod+killGrace {
		t.Fatalf("http 预算 %v 应 = 关停两段 %v", closeBudgetHTTP, gracePeriod+killGrace)
	}
	if closeBudgetStdio != closeBudgetHTTP+gracePeriod+killGrace {
		t.Fatalf("stdio 预算 %v 应 = 关停两段 + 收尸两段 %v", closeBudgetStdio, closeBudgetHTTP+gracePeriod+killGrace)
	}
}

// (d) 关停告警文案按 cmd != nil 分：stdio 有子进程可杀 → "已强杀进程树"；
// http 无子进程 → "无进程可杀"。
func TestSessionCloseTimeoutNoteByTransport(t *testing.T) {
	stdio := sessionCloseTimeoutNote(&exec.Cmd{})
	if !strings.Contains(stdio, "stdio") || !strings.Contains(stdio, "进程树") {
		t.Fatalf("stdio 文案应说'已强杀进程树': %q", stdio)
	}
	if strings.Contains(stdio, "无进程可杀") {
		t.Fatalf("stdio 有进程可杀，不该说'无进程可杀': %q", stdio)
	}

	httpNote := sessionCloseTimeoutNote(nil)
	if !strings.Contains(httpNote, "http") || !strings.Contains(httpNote, "无进程可杀") {
		t.Fatalf("http 文案应说'无进程可杀': %q", httpNote)
	}
	if strings.Contains(httpNote, "进程树") {
		t.Fatalf("http 无进程，不该说'杀进程树': %q", httpNote)
	}
}

// (e) stderr 脱敏 tab 分隔 scheme 变体："Bearer\tsk-x" 的**值本体**也要登记 + 打码。
func TestStripSchemeTabVariant(t *testing.T) {
	cases := map[string]string{
		"Bearer sk-x":  "sk-x", // 既有：空格分隔
		"Bearer\tsk-x": "sk-x", // 新增：tab 分隔
		"bearer\tsk-x": "sk-x", // scheme 词大小写不敏感
		"Basic\tsk-y":  "sk-y",
		"Token sk-z":   "sk-z",
		"noprefix":     "noprefix", // 无 scheme 原样返回
		"bearer":       "bearer",   // 无分隔符不剥（不是 "bearer <token>" 形态）
		"bearerish":    "bearerish",
	}
	for in, want := range cases {
		if got := stripScheme(in); got != want {
			t.Fatalf("stripScheme(%q) = %q, want %q", in, got, want)
		}
	}
}

// (e) 端到端：headers 里配 tab 分隔的 "Bearer\tsk-tab-secret"，server 把值本体单独
// 打出（无 scheme，"auth=sk-tab-secret"）时也必须打码（凭据纪律 H12/H19）。
func TestStderrSinkRedactsTabSeparatedSchemeBody(t *testing.T) {
	buf, restore := captureMCPLog(t)
	defer restore()

	cfg := &ServerConfig{
		Name:    "srv",
		Headers: map[string]string{"Authorization": "Bearer\tsk-tab-secret"},
	}
	// 值本体必须被登记进脱敏清单（tab 分隔也能剥出 scheme）。
	secrets := secretValues(cfg)
	found := false
	for _, s := range secrets {
		if s == "sk-tab-secret" {
			found = true
		}
	}
	if !found {
		t.Fatalf("tab 分隔 scheme 剥掉后的值本体没登记: %v", secrets)
	}

	sink := newStderrSink(cfg)
	if _, err := sink.Write([]byte("auth=sk-tab-secret\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	out := buf.String()
	if strings.Contains(out, "sk-tab-secret") {
		t.Fatalf("tab 变体的值本体没打码: %q", out)
	}
	if !strings.Contains(out, "auth=***") {
		t.Fatalf("没打码成预期形态: %q", out)
	}
}
