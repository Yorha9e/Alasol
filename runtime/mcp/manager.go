package mcp

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/Yorha9e/alasol/core/registry"
)

// 本文件是 MCP 能力的**装配面**：把 Config 里声明的 server 一个个连上、
// 把发现的工具注册进 core/registry，并在 Close 时收尸全部连接。
//
// # 三条硬口径
//
//  1. **策略/可见性照旧**：本包只调 registry.RegisterTool（内置 read/write/bash
//     走的同一个入口），不设 Capabilities、不 Enable、不碰策略引擎。于是 MCP 工具
//     默认落在 deny-all 上（H5 fail-close：无 allow 规则 = 拒绝），可见性由
//     agent 类型的 tools[] 白名单决定（R5：不暴露即不存在）。**不给 MCP 工具
//     任何特权**——它比内置工具更保守（approvable=true / side_effect=write /
//     replay=unsafe，见 discovery.go 的保守缺省）。
//  2. **坏 server 不拖垮 hub**：连接失败 / 发现失败 / 单工具注册失败 → 那个
//     server 的工具一个都不注册（或只注册成功的那部分），错误记进 Manager 的
//     errs 里供宿主 surfaced，**不 panic、不返回致命错误**。装配继续。
//     这条在**时间维度**上同样成立：每个 server 的装配（连接 + 发现）都有
//     自己的 deadline（serverTimeout），挂死的 server 最多拖它自己那一份
//     （p1）——不许把 hub 启动拖成永久挂起。
//  3. **撞名 fail-close**：注册名与已有条目（内置工具 / 另一个 server 的工具）
//     撞名 → 该工具不注册 + 明确错误（带 server、原名、新名）。不静默跳过。
//     归一化把两个 wire 名折成同一个注册名时，错误**同时点名两个原名**
//     （只报后注册的那个，使用者不知道该改哪个）。
//
// # 生命周期
//
// New（不连任何东西，只校验配置）→ Attach（连 + 发现 + 注册）→ Close（收尸）。
// 拆开是为了让"配置错误"（致命，host.New 该失败）与"连接错误"（不致命，
// 记下来继续）有明确的分界。
type Manager struct {
	cfg Config

	mu    sync.Mutex
	conns []*serverConn
	// tools：注册名 → (server, wire 名)。供诊断/测试观测装配结果。
	tools map[string]toolRef
	errs  []error
	// attached 记录 Attach 是否已跑过（幂等保护：重复 Attach 会把同一批工具
	// 注册第二次 → 撞上注册表的重名拒绝，不如直接报编程错误）。
	attached bool
	closed   bool
}

// toolRef 是一个已注册 MCP 工具的来源（诊断用；wire 名不是凭据，可安全打印）。
type toolRef struct {
	Server string `json:"server"`
	Wire   string `json:"wire_name"`
}

// New 校验配置并造 Manager（**不连接任何 server**）。
//
// 配置错误在这里就是致命的：坏配置是使用者要修的，静默忽略会让"以为连上了"
// 与"其实没连"长期不一致。host.New 会把这个错误上抛。
func New(cfg Config) (*Manager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Manager{cfg: cfg, tools: make(map[string]toolRef)}, nil
}

// Config 返回原始配置（调用方不得改写；含凭据 → 不得日志化，用 Redacted()）。
func (m *Manager) Config() Config {
	if m == nil {
		return Config{}
	}
	return m.cfg
}

// Attach 连接所有启用的 server 并把发现的工具注册进 reg。
//
// **并发连接**（每个 server 一个 goroutine）：一个挂死的 server 最多拖
// connectTimeout（默认 15s），N 个挂死的 server 不该拖 N×15s——`alasol serve`
// 的启动时间不该被外部进程的响应速度决定（"坏 server 不拖垮 hub"的时间维度）。
// 错误按 server 声明顺序汇总（确定性，与并发度无关）。
//
// 返回值是**非致命**问题的列表（每个 server 一条汇总 + 逐工具的错误）。
// 宿主应该把它 surfaced（日志 / 诊断端点），但不该因此拒绝启动——
// 一个外部 MCP server 不可用不该让 alasol 起不来。
//
// 发现（tools/list）**只发一次**：connectSession 那次的结果被复用（见
// serverConn.Tools）——装配期不再发第二次请求（第二次既是浪费，又是新的
// 挂死面：坏 server 常在第二次列表上不动，p1）。
//
// 幂等：重复 Attach 返回错误（同一批工具注册第二次必然撞名）。
func (m *Manager) Attach(ctx context.Context, reg *registry.Registry) []error {
	if m == nil {
		return []error{errors.New("mcp: Manager 为 nil")}
	}
	if reg == nil {
		return []error{errors.New("mcp: registry 为 nil（拒绝装配：无处注册工具）")}
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return []error{errors.New("mcp: Manager 已关闭，不能装配")}
	}
	if m.attached {
		m.mu.Unlock()
		return []error{errors.New("mcp: Attach 已执行过（重复装配会让同一批工具撞名）")}
	}
	m.attached = true
	servers := m.cfg.EnabledServers()
	m.mu.Unlock()

	return m.attachAll(ctx, reg, servers, connect)
}

// attachAll 并发装配一组 server 并回收错误（按声明顺序）。
//
// dial 是"建立连接"的函数：产物传 connect（真拉起进程/连端点），测试可注入
// in-memory transport。**只换"怎么连"，装配逻辑（发现 → 撞名检查 → 注册 →
// 收尸）仍然只有一份**——不为测试复制产物逻辑（F7 纪律）。
func (m *Manager) attachAll(ctx context.Context, reg *registry.Registry, servers []ServerConfig, dial func(context.Context, ServerConfig) (*serverConn, error)) []error {
	// 单 server 时不另起 goroutine：少一层调度，也少一个 race 面（最常见形态）。
	if len(servers) <= 1 {
		var out []error
		if len(servers) == 1 {
			if err := m.attachDialed(ctx, reg, servers[0], dial); err != nil {
				out = []error{err}
			}
		}
		m.recordErr(out...)
		return out
	}

	errs := make([]error, len(servers))
	var wg sync.WaitGroup
	for i := range servers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = m.attachDialed(ctx, reg, servers[i], dial)
		}(i)
	}
	wg.Wait()

	out := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			out = append(out, err)
		}
	}
	m.recordErr(out...)
	return out
}

// isClosed 报告 Manager 是否已关闭（并发安全）。
func (m *Manager) isClosed() bool {
	if m == nil {
		return true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// recordErr 把一批错误记进 m.errs（ surfaced 面）。
func (m *Manager) recordErr(errs ...error) {
	if len(errs) == 0 {
		return
	}
	m.mu.Lock()
	m.errs = append(m.errs, errs...)
	m.mu.Unlock()
}

// attachDialed 连一个 server 并注册它的工具（dial 由调用方给，见 attachAll）。
//
// **每个 server 一份装配期 deadline**（serverTimeout）：连接 + 发现都罩住。
// 不能指望调用方的 ctx——产品侧传的是 context.Background()（host.New /
// `alasol run` / `alasol resume` 都那样传），没有这层预算，一个挂死的 server
// 会让 hub 启动**永久挂起**（p1："坏 server 不拖垮 hub"的时间维度）。
func (m *Manager) attachDialed(ctx context.Context, reg *registry.Registry, sc ServerConfig, dial func(context.Context, ServerConfig) (*serverConn, error)) error {
	sctx, cancel := context.WithTimeout(ctx, serverTimeout(sc))
	defer cancel()
	conn, err := dial(sctx, sc)
	if err != nil {
		return err
	}
	return m.registerConn(sctx, reg, conn)
}

// registerConn 把一个**已连上**的连接里的工具注册进 reg。
//
// 从 attachDialed 拆出来是为了让测试能注入 in-memory transport（连接由测试建），
// 而"发现 → 撞名检查 → 注册 → 收尸"这段装配逻辑仍然只有一份（F7 纪律：
// 不为测试复制产物逻辑）。
//
// 返回值是**非致命**问题的汇总（ surfaced 用）；工具级错误同时进 m.errs。
func (m *Manager) registerConn(ctx context.Context, reg *registry.Registry, conn *serverConn) error {
	sc := conn.cfg
	// Manager 已关闭 → 这条连接没人收尸（Close 已经收过一遍了）→ 当场收掉，
	// 一个工具都不注册（fail-close：注册一个连不上的工具比不注册更糟）。
	if m.isClosed() {
		_ = conn.Close()
		return fmt.Errorf("mcp: Manager 已关闭，server %q 的连接被丢弃（已当场收尸）", sc.Name)
	}
	discovered, convErrs := conn.discover(ctx)
	for _, e := range convErrs {
		m.recordErr(e)
	}
	registered := 0
	var regErrs []error
	// seen 记本连接内已占用的注册名 → 原 wire 名。
	//
	// 归一化可能把两个不同的 wire 名折成同一个注册名（`a.b` 与 `a_b`）——
	// 报错必须**同时点名两个原名**（naming.go 的承诺）：只报后注册的那个，
	// 使用者不知道该改哪个（或者两个都改）。
	seen := make(map[string]string, len(discovered))
	for _, d := range discovered {
		if prev, dup := seen[d.name]; dup {
			regErrs = append(regErrs, fmt.Errorf(
				"mcp: server %q 的工具 %q 与 %q 归一化后得到同一个注册名 %q → 拒注册（fail-close；两个原名都列出以便定位）",
				sc.Name, prev, d.wireName, d.name))
			continue
		}
		seen[d.name] = d.wireName
		// 撞名检查（fail-close）：注册表自己会拒重名，但我们要给一句
		// "这是 MCP 工具与谁撞了"的明确文案，而不是一句通用的 already registered。
		if existing, dup := reg.Get(d.name); dup {
			regErrs = append(regErrs, fmt.Errorf(
				"mcp: server %q 的工具 %q（注册名 %q）与已注册扩展撞名（kind=%s）→ 拒注册（fail-close）",
				sc.Name, d.wireName, d.name, existing.Meta.Kind))
			continue
		}
		exec := &mcpExecutor{conn: conn, wireName: d.wireName, name: d.name}
		if err := reg.RegisterTool(d.desc, exec); err != nil {
			regErrs = append(regErrs, fmt.Errorf("mcp: server %q 注册工具 %q 失败: %w", sc.Name, d.name, err))
			continue
		}
		m.mu.Lock()
		m.tools[d.name] = toolRef{Server: sc.Name, Wire: d.wireName}
		m.mu.Unlock()
		registered++
	}
	for _, e := range regErrs {
		m.recordErr(e)
	}

	// 一个工具都没注册成功 → 这条连接没有存在意义，立刻收尸（不留孤儿进程）。
	if registered == 0 {
		_ = conn.Close()
		return fmt.Errorf("mcp: server %q 未注册任何工具（发现 %d 个；失败 %d 处）→ 已关闭该连接",
			sc.Name, len(discovered), len(convErrs)+len(regErrs))
	}
	// 连接交给 Manager 持有（Close 时收尸）。**每个 server 一条**——
	// 同一连接的多个工具共享它，所以在这里入账而不是在每个工具那儿。
	//
	// **与 Close 交错**：Close 置 closed 后清空了 conns ——此时再 append 就是
	// 一条**永远没人收尸**的连接（孤儿进程，违反"谁拉起谁收尸"）。所以 append
	// 前查 closed：为真就当场把这条连接收掉并报错（fail-close，不静默）。
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = conn.Close()
		return fmt.Errorf("mcp: Manager 已关闭，server %q 的连接被丢弃（已当场收尸）", sc.Name)
	}
	m.conns = append(m.conns, conn)
	m.mu.Unlock()

	if len(regErrs) > 0 {
		// 汇总错误里**带上工具级错误的原文**：只报"1/2 个失败"的话，使用者
		// 看不到是哪个工具、为什么失败（撞名要能点名撞的是谁）。
		return fmt.Errorf("mcp: server %q 注册了 %d/%d 个工具（%d 个失败）: %w",
			sc.Name, registered, len(discovered), len(regErrs), errors.Join(regErrs...))
	}
	return nil
}

// Errors 返回装配期累积的非致命错误（按发生顺序）。
func (m *Manager) Errors() []error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]error(nil), m.errs...)
}

// ToolRefs 返回已注册工具的来源表（诊断/测试观测；按注册名排序保证确定性）。
func (m *Manager) ToolRefs() map[string]toolRef {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]toolRef, len(m.tools))
	for k, v := range m.tools {
		out[k] = v
	}
	return out
}

// ToolNames 返回已注册工具名（排序，确定性输出）。
func (m *Manager) ToolNames() []string {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.tools))
	for k := range m.tools {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Servers 返回已成功装配的 server 信息（按 server 名排序；不含凭据）。
func (m *Manager) Servers() []ServerStatus {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	conns := append([]*serverConn(nil), m.conns...)
	m.mu.Unlock()
	out := make([]ServerStatus, 0, len(conns))
	for _, c := range conns {
		out = append(out, ServerStatus{
			Name:      c.cfg.Name,
			Transport: c.info.Transport,
			ServerInfo: ServerInfoStatus{
				Name:            c.info.Name,
				Version:         c.info.Version,
				ProtocolVersion: c.info.ProtocolVersion,
			},
			N2Rejected: c.tr.snapshot(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ServerStatus 是一个已装配 server 的状态（诊断面；**不含任何凭据/配置值**）。
type ServerStatus struct {
	Name       string           `json:"name"`
	Transport  string           `json:"transport"`
	ServerInfo ServerInfoStatus `json:"server_info"`
	// N2Rejected 是 server 尝试调用被拒能力（sampling/elicitation/roots）的留痕。
	N2Rejected n2Snapshot `json:"n2_rejected"`
}

// ServerInfoStatus 是握手读到的 server 身份（server 自报，不可信但可诊断）。
type ServerInfoStatus struct {
	Name            string `json:"name,omitempty"`
	Version         string `json:"version,omitempty"`
	ProtocolVersion string `json:"protocol_version,omitempty"`
}

// Close 关闭所有连接并收尸（幂等）。
//
// "谁拉起谁收尸"：stdio server 由本包 exec，hub Close 时也由本包杀树 + Wait
// （陷阱 5）。http 传输只关 session。
func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	conns := append([]*serverConn(nil), m.conns...)
	m.conns = nil
	m.mu.Unlock()

	var errs []error
	for _, c := range conns {
		if err := c.Close(); err != nil {
			errs = append(errs, fmt.Errorf("mcp: 关 server %q 连接: %w", c.cfg.Name, err))
		}
	}
	return errors.Join(errs...)
}
