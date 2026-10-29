package mcp

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/core/registry"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// 本文件是 v3-S1 修复批的**回归用例**（critic 的必修项逐条钉死）：
//
//   - p1：发现阶段（tools/list）有 deadline + 装配期**只发一次** tools/list；
//   - p2：Close 与在飞调用交错**有界**（不许把关停拖死）；
//   - p2：Attach/Close 交错不留孤儿连接；
//   - p3：归一化碰撞的错误**同时点名两个原名**。
//
// 这些用例的共同形状：**先破坏、再断言会红**（变异自证见交付说明）。

// hangOnMethod 返回一个 server 侧中间件：指定方法**不返回**（挂死），
// 直到请求的 ctx 被取消。
//
// 这是"坏 server"的忠实建模：握手正常，某个请求不吭声。装配期若不给
// deadline，hub 启动就被它拖成永久挂起（p1）。
func hangOnMethod(method string) mcp.Middleware {
	return hangOnMethodUntil(method, nil)
}

// hangOnMethodUntil 与 hangOnMethod 同款，但多一个"放行"通道：测试收尾时
// 关闭它，server 侧的挂死 handler 才会返回（否则 server 自己的 Close 会
// 等这个 handler ——测试替身自己的死锁，不是被测行为）。
func hangOnMethodUntil(method string, release <-chan struct{}) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, m string, req mcp.Request) (mcp.Result, error) {
			if m == method {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-release:
					return nil, ctx.Err()
				}
			}
			return next(ctx, m, req)
		}
	}
}

// TestAttachDiscoveryBoundedByTimeout：server 的 tools/list 挂死 → 装配必须
// **在 timeout 内返回错误**，不许把 hub 启动拖成永久挂起（p1）。
//
// 判据三条：① 有错误（挂死不是"成功"）；② 错误里点名了那个 server；
// ③ 耗时被 timeout 罩住（不是"等了很久才失败"）。
func TestAttachDiscoveryBoundedByTimeout(t *testing.T) {
	ctx := context.Background()
	reg := newTestRegistry()
	tr, _, _ := startFakeMW(t, hangOnMethod("tools/list"), fakeTool{name: "t", text: "ok"})

	m, err := New(Config{Servers: []ServerConfig{{
		Name: "hang", Transport: TransportStdio, Command: "x", Enabled: true, TimeoutMs: 800,
	}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	dial := func(dctx context.Context, sc ServerConfig) (*serverConn, error) {
		return connectSession(dctx, sc, tr, nil)
	}
	start := time.Now()
	errs := m.attachAll(ctx, reg, m.cfg.EnabledServers(), dial)
	elapsed := time.Since(start)

	if len(errs) == 0 {
		t.Fatalf("tools/list 挂死应报错（不能当成功）")
	}
	mustContain(t, errs[0], "hang")
	// timeout=800ms；留 5s 余量给调度抖动，但绝不能是"无限等"。
	if elapsed > 5*time.Second {
		t.Fatalf("装配耗时 %v（应被装配期 deadline 罩住）", elapsed)
	}
	// 挂死的 server 不该注册任何工具（fail-close）。
	if names := m.ToolNames(); len(names) != 0 {
		t.Fatalf("挂死的 server 不该注册工具: %v", names)
	}
}

// TestAttachSendsToolsListOnce：装配期**只发一次** tools/list
// （connectSession 那次的结果被复用）。
//
// 为什么这条重要：坏 server 常在**第二次** tools/list 上挂死（第一次给个
// 像样的列表，第二次就不动了）。若装配再发一次且那次没有 deadline，hub 启动
// 会永久挂起（p1）。本用例用"第二次就挂死"的 server 建模：**它必须装配成功**
// （说明第二次请求根本没发出去），且 server 侧只收到一次列表请求。
func TestAttachSendsToolsListOnce(t *testing.T) {
	ctx := context.Background()
	reg := newTestRegistry()

	var lists atomic.Int32
	mw := func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, m string, req mcp.Request) (mcp.Result, error) {
			if m == "tools/list" && lists.Add(1) > 1 {
				<-ctx.Done() // 第二次起挂死
				return nil, ctx.Err()
			}
			return next(ctx, m, req)
		}
	}
	tr, _, _ := startFakeMW(t, mw, fakeTool{name: "t", text: "ok"})

	m, err := New(Config{Servers: []ServerConfig{{
		Name: "twice", Transport: TransportStdio, Command: "x", Enabled: true, TimeoutMs: 2000,
	}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	dial := func(dctx context.Context, sc ServerConfig) (*serverConn, error) {
		return connectSession(dctx, sc, tr, nil)
	}
	done := make(chan []error, 1)
	go func() {
		done <- m.attachAll(ctx, reg, m.cfg.EnabledServers(), dial)
	}()
	var errs []error
	select {
	case errs = <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("装配挂住了（第二次 tools/list 被发出去了且没有 deadline）")
	}
	if len(errs) != 0 {
		t.Fatalf("装配不该报错: %v", errs)
	}
	if got := lists.Load(); got != 1 {
		t.Fatalf("server 收到 %d 次 tools/list（应 1 次——装配期只发一次）", got)
	}
	// 复用的是第一次的结果 → 工具确实注册上了。
	assertToolRegistered(t, reg, "mcp__twice__t")
}

// TestCloseWithInFlightCallIsBounded：server 的 tools/call 挂死时，Close 必须
// **有界返回**（不许等在飞请求结束）。
//
// 判据：Close 在 gracePeriod+killGrace 之内返回（不给它无界等待的机会），
// 且不 panic、不死锁。旧实现会卡在 SDK 的"等在飞请求结束"上——hub 关停被
// 一个坏 server 拖死（p2）。
func TestCloseWithInFlightCallIsBounded(t *testing.T) {
	ctx := context.Background()
	// release 让 server 侧的挂死 handler 返回（否则 server 自己的 Close 会
	// 等它——那是测试替身自己的死锁，不是被测行为）。
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
	tr, _, _ := startFakeMW(t, hangOnMethodUntil("tools/call", release), fakeTool{name: "hang", text: "ok"})
	t.Cleanup(releaseFn) // LIFO：先于 server 的 Close 执行
	conn := connectFake(ctx, t, "srv", tr)

	// 起一个挂死的调用（让它真的在飞）。
	callDone := make(chan struct{})
	go func() {
		defer close(callDone)
		_, _ = conn.CallTool(ctx, &mcp.CallToolParams{Name: "hang"})
	}()
	time.Sleep(150 * time.Millisecond) // 让调用进到 server 侧

	start := time.Now()
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	elapsed := time.Since(start)
	// 关停的两段窗口：gracePeriod(2s) + killGrace(5s) = 7s；留余量但不许无界。
	if elapsed > 15*time.Second {
		t.Fatalf("Close 耗时 %v（应被关停窗口压住，不许等在飞请求结束）", elapsed)
	}
	// server 响应后，在飞调用必须收尾（不许永远挂着）。
	releaseFn()
	select {
	case <-callDone:
	case <-time.After(15 * time.Second):
		t.Fatalf("在飞调用没有收尾")
	}
}

// TestManagerCloseThenAttachReapsConn：Close 与装配交错 → 那条连接必须**当场
// 收尸**（否则是永远没人收的孤儿连接，"谁拉起谁收尸"被打破）。
func TestManagerCloseThenAttachReapsConn(t *testing.T) {
	ctx := context.Background()
	reg := newTestRegistry()
	tr, _, _ := startFake(t, fakeTool{name: "t", text: "ok"})

	m := &Manager{tools: map[string]toolRef{}}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	conn, err := connectSession(ctx, ServerConfig{Name: "s", Transport: TransportStdio}, tr, nil)
	if err != nil {
		t.Fatalf("连接: %v", err)
	}
	err = m.registerConn(ctx, reg, conn)
	if err == nil {
		t.Fatalf("Manager 关闭后的装配应报错")
	}
	mustContain(t, err, "已关闭")
	// 连接已被当场收尸：再调用必须失败（不是"注册成功但连不上"的幽灵工具）。
	if _, cerr := conn.CallTool(ctx, &mcp.CallToolParams{Name: "t"}); cerr == nil {
		t.Fatalf("连接应已被收尸（调用应失败）")
	}
	// 一个工具都不该注册（fail-close：注册一个连不上的工具比不注册更糟）。
	if names := m.ToolNames(); len(names) != 0 {
		t.Fatalf("关闭后的装配不该注册工具: %v", names)
	}
}

// TestManagerNormalizationCollisionNamesBoth：归一化碰撞的错误必须**同时点名
// 两个原名**（`a.b` 与 `a_b`）——只报后注册的那个，使用者不知道该改哪个。
func TestManagerNormalizationCollisionNamesBoth(t *testing.T) {
	ctx := context.Background()
	reg := newTestRegistry()
	tr, _, _ := startFake(t,
		fakeTool{name: "a.b", text: "1"},
		fakeTool{name: "a_b", text: "2"},
	)
	_, errs := attachInMemory(ctx, t, reg, map[string]mcp.Transport{"s": tr})
	if len(errs) != 1 {
		t.Fatalf("应恰好一条错误（得到 %d 条：%v）", len(errs), errs)
	}
	msg := errs[0].Error()
	for _, want := range []string{"a.b", "a_b", "mcp__s__a_b"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("错误信息缺 %q：%s", want, msg)
		}
	}
}

// TestManagerLongToolNameFailsAtAssembly：注册名超上限（provider 函数名上限）
// 的工具**在装配期**就被拒（fail-close），且错误**点名 server 与工具**。
//
// 为什么不能让它活到运行期：注册名会原样进 provider 的函数名，超限的后果是
// **整轮请求被 provider 拒**——外部 server 声明一个长工具名就能把整轮对话
// 打穿。装配期拒掉（并说清该改什么）才是 fail-close。
func TestManagerLongToolNameFailsAtAssembly(t *testing.T) {
	ctx := context.Background()
	reg := newTestRegistry()
	long := strings.Repeat("z", 100)
	tr, _, _ := startFake(t, fakeTool{name: long, text: "ok"})

	m, errs := attachInMemory(ctx, t, reg, map[string]mcp.Transport{"s": tr})
	if len(errs) == 0 {
		t.Fatalf("超长工具名应被拒（装配期 fail-close）")
	}
	// 工具级错误在 surfaced 面（Manager.Errors）里——汇总错误只说"0 个注册成功"。
	var joined string
	for _, e := range m.Errors() {
		joined += e.Error() + "\n"
	}
	// 错误要点名 server 与工具（使用者要能定位是哪个 server 的哪个工具）。
	for _, want := range []string{`"s"`, long} {
		if !strings.Contains(joined, want) {
			t.Fatalf("错误信息缺 %q：%s", want, joined)
		}
	}
	// 一个工具都不该注册。
	if names := reg.List(registry.ExtensionKindTool); len(names) != 0 {
		t.Fatalf("不该注册任何工具: %+v", names)
	}
}
