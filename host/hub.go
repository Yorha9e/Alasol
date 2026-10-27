package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/runtime/approver/terminal"
	"github.com/Yorha9e/alasol/runtime/cap/local"
	"github.com/Yorha9e/alasol/runtime/indexer"
	"github.com/Yorha9e/alasol/runtime/provider/openai"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	"github.com/Yorha9e/alasol/runtime/tools"
	"github.com/Yorha9e/alasol/runtime/transport"
)

// Hub 是装配好的常驻服务：事件流 + 在线表 + 上下文快照 + Web UI + WS。
//
// 生命周期：New（开库 / 起索引器 / 播种投影）→ Handler（HTTP 服务）→ Close
// （停索引器 → 关库）。Close 幂等。
//
// 并发：RunPrompt 可被并发调用（不同 session 各一个 Loop，loop 本身无可变状态；
// 同一 session 并发 Run 时事件 seq 是提交序而非因果序——G25 session 亲和写的部署约束）。
type Hub struct {
	cfg Config

	store    *sqlite.Store
	bus      *bus.Bus
	indexer  *indexer.Indexer
	reg      *registry.Registry
	engine   *policy.Engine
	provider core.Provider
	approver core.Approver
	proj     *projection
	ws       *transport.Server

	// ixCtx 是后台索引器的生命周期（Close 时取消）。
	ixCtx    context.Context
	ixCancel context.CancelFunc
	ixDone   chan struct{}

	mu     sync.Mutex
	loops  map[contracts.Ulid]*loop.Loop
	closed bool
}

// New 装配一个 Hub。
//
// 顺序：开事件库 → 建索引器（确保投影表）→ bus → 策略/注册表/工具 → provider/approver
// → 传输服务端 → 播种内存投影 → 起后台索引器。
func New(cfg Config) (*Hub, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	st, err := sqlite.Open(cfg.DBPath, sqlite.Options{})
	if err != nil {
		return nil, fmt.Errorf("host: 打开事件库: %w", err)
	}

	ix, err := sqlite.NewIndexer(st, cfg.IndexerBatch)
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("host: 建索引器: %w", err)
	}

	// 策略：Allow 生成 source=cli 的 allow 规则；Ask 生成 ask 规则（命中即去问人）。
	// 空规则集 = 一切 deny（fail-close）。
	var rules []contracts.PolicyRule
	for _, name := range cfg.Allow {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		rules = append(rules, contracts.PolicyRule{
			Effect: contracts.EffectAllow,
			Tool:   name,
			Source: contracts.RuleSourceCli,
		})
	}
	for _, name := range cfg.Ask {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		rules = append(rules, contracts.PolicyRule{
			Effect: contracts.EffectAsk,
			Tool:   name,
			Source: contracts.RuleSourceCli,
		})
	}
	eng := policy.New(rules...)

	reg := registry.New(registry.NewPolicyAdjudicator(eng))
	execs := map[string]core.ToolExecutor{
		"read":  tools.NewRead(local.FS{}),
		"write": tools.NewWrite(local.FS{}),
		"bash":  tools.NewBash(local.Exec{}),
	}
	for _, d := range tools.Descriptors() {
		ex, ok := execs[d.Name]
		if !ok {
			_ = st.Close()
			return nil, fmt.Errorf("host: 工具 %q 无执行器（装配残缺）", d.Name)
		}
		if err := reg.RegisterTool(d, ex); err != nil {
			_ = st.Close()
			return nil, fmt.Errorf("host: 注册工具 %s: %w", d.Name, err)
		}
	}

	prov := cfg.Provider
	if prov == nil {
		// 真 provider：API key 只从 env 取（-api-key-env 只传变量名），
		// 永不进 argv / 事件 / 日志（H12/H19）。
		keyEnv := strings.TrimSpace(cfg.APIKeyEnv)
		if keyEnv == "" {
			keyEnv = DefaultAPIKeyEnv
		}
		prov = openai.New(openai.Options{
			BaseURL: strings.TrimSpace(cfg.BaseURL),
			APIKey:  os.Getenv(keyEnv),
			Stream:  true,
		})
	}
	ap := cfg.Approver
	if ap == nil {
		switch cfg.Approval {
		case "always-allow":
			ap = core.AlwaysAllow{}
		case "terminal":
			ap = terminal.New(os.Stdin, os.Stdout)
		default: // deny-all（默认，fail-close）
			ap = core.DenyAll{}
		}
	}

	b := bus.New(st)
	h := &Hub{
		cfg:      cfg,
		store:    st,
		bus:      b,
		indexer:  ix,
		reg:      reg,
		engine:   eng,
		provider: prov,
		approver: ap,
		proj:     newProjection(st),
		ws:       transport.NewServer(b),
		loops:    make(map[contracts.Ulid]*loop.Loop),
	}

	// 播种内存投影：把库里已有的事件扫进来（重开进程后在线表立刻有内容）。
	if _, err := h.proj.catchUp(context.Background()); err != nil {
		_ = h.Close()
		return nil, fmt.Errorf("host: 播种投影失败: %w", err)
	}

	// 后台索引器：派生表（sessions / ledger / board / cost_stats）的**唯一写者**。
	h.ixCtx, h.ixCancel = context.WithCancel(context.Background())
	h.ixDone = make(chan struct{})
	go func() {
		defer close(h.ixDone)
		_ = ix.Run(h.ixCtx) // ctx 取消时返回 ctx.Err()，不是故障
	}()

	return h, nil
}

// Close 释放资源（幂等）：停后台索引器 → 关事件库。
func (h *Hub) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.mu.Unlock()

	var errs []error
	if h.ixCancel != nil {
		h.ixCancel()
	}
	if h.ixDone != nil {
		<-h.ixDone
	}
	if err := h.store.Close(); err != nil {
		errs = append(errs, fmt.Errorf("host: 关事件库: %w", err))
	}
	return errors.Join(errs...)
}

// RunPrompt 起（或复用）一个 session 的 agent loop 并跑一轮。
//
// sessionID 为空 → 新建一个（返回的 Result 里拿不到新 id，调用方要 id 就用
// NewSession() 先取一个）。prompt 为空 → 拒绝（空 prompt 等于浪费一次模型调用）。
func (h *Hub) RunPrompt(ctx context.Context, sessionID contracts.Ulid, prompt string) (loop.Result, error) {
	if h == nil {
		return loop.Result{}, errors.New("host: Hub 为 nil")
	}
	if strings.TrimSpace(prompt) == "" {
		return loop.Result{}, errors.New("host: prompt 不能为空")
	}
	lp, err := h.loopFor(sessionID)
	if err != nil {
		return loop.Result{}, err
	}
	res, err := lp.Run(ctx, prompt)
	// 跑完立刻把新事件并进投影：/api/context 不必等下一次请求的增量扫描。
	if _, cerr := h.proj.catchUp(ctx); cerr != nil {
		// 投影失败不影响 Run 的结果（在线表晚一会刷新），但不该被完全吞掉：
		// 水位没前进 → 下一次 API 请求的 catchUp 会重试同一区间。
		hubLog.Warn("host: Run 后投影 catch-up 失败（下次 API 请求会重试）",
			"session_id", string(sessionID), "err", cerr.Error())
	}
	return res, err
}

// loopFor 取（或建）某 session 的 Loop。
//
// 一个 session 一个 Loop：System/Model/AgentID 固定，历史由每次 Run 的局部变量带
// （loop 无可变状态 → 同一 Loop 可并发 Run，见 loop 包 doc）。
func (h *Hub) loopFor(sessionID contracts.Ulid) (*loop.Loop, error) {
	if sessionID == "" {
		return nil, errors.New("host: sessionID 必填（空 = 用 NewSession() 先取一个）")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if lp, ok := h.loops[sessionID]; ok {
		return lp, nil
	}
	lp, err := loop.New(loop.Config{
		Provider:      h.provider,
		Bus:           h.bus,
		Registry:      h.reg,
		Policy:        h.engine,
		Approver:      h.approver,
		SessionID:     sessionID,
		AgentID:       core.NewUlid(),
		Model:         h.cfg.Model,
		System:        h.cfg.System,
		MaxTurns:      h.cfg.MaxTurns,
		ContextBudget: h.cfg.ContextBudget,
	})
	if err != nil {
		return nil, fmt.Errorf("host: 为 session %s 建 loop: %w", sessionID, err)
	}
	h.loops[sessionID] = lp
	return lp, nil
}

// NewSession 生成一个新的 session id（调用方先拿 id 再 RunPrompt，便于 UI/CLI 展示）。
func NewSession() contracts.Ulid { return core.NewUlid() }

// Addr 返回归一化后的监听地址（Config.Addr 的解析结果，供 serve 打印与监听）。
func (h *Hub) Addr() string {
	if h == nil {
		return ""
	}
	return h.cfg.Addr
}

// Sessions 返回在线表（最新活跃在前，行数受 Config.MaxSessionRows 限制）。
func (h *Hub) Sessions(ctx context.Context) ([]SessionRow, error) {
	if h == nil {
		return nil, errors.New("host: Hub 为 nil")
	}
	if _, err := h.proj.catchUp(ctx); err != nil {
		return nil, fmt.Errorf("host: 扫事件流: %w", err)
	}
	// 等索引器追到投影水位（有界等待）：刚跑完一轮时用户刷新页面就该看到
	// "索引已追至 seq N"，而不是 0（索引器是 50ms 轮询，正常几条就追平）。
	// 索引器卡住也不阻塞请求：超时后用当前值返回（诚实显示"还没追到"）。
	wm, err := h.waitIndexer(h.proj.scanWatermark(), indexerCatchUpWait)
	if err != nil {
		return nil, fmt.Errorf("host: 读索引水位: %w", err)
	}
	return h.proj.rows(wm, h.cfg.MaxSessionRows), nil
}

// indexerCatchUpWait 是 Sessions 等索引器追平的有界等待（毫秒级；索引器 50ms 一轮）。
const indexerCatchUpWait = 250 * time.Millisecond

// waitIndexer 轮询等索引器水位达到 target（或超时返回当前值）。
func (h *Hub) waitIndexer(target int64, timeout time.Duration) (int64, error) {
	deadline := time.Now().Add(timeout)
	for {
		wm, err := h.indexer.Watermark()
		if err != nil {
			return 0, err
		}
		if wm >= target || !time.Now().Before(deadline) {
			return wm, nil
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// ContextSnapshot 取某 session 最新的 context.assembled 快照（不存在 → ok=false）。
func (h *Hub) ContextSnapshot(ctx context.Context, sessionID contracts.Ulid) (json.RawMessage, bool, error) {
	if h == nil {
		return nil, false, errors.New("host: Hub 为 nil")
	}
	if _, err := h.proj.catchUp(ctx); err != nil {
		return nil, false, fmt.Errorf("host: 扫事件流: %w", err)
	}
	raw, ok := h.proj.snapshot(sessionID)
	return raw, ok, nil
}

// Watermark 返回索引器已追到的全局 seq（G24③）。
func (h *Hub) Watermark() (int64, error) {
	if h == nil {
		return 0, errors.New("host: Hub 为 nil")
	}
	return h.indexer.Watermark()
}

// Health 是 GET /api/health 的形状。
type Health struct {
	OK        bool  `json:"ok"`
	Watermark int64 `json:"watermark"`
	// Sessions 是在线表行数（不含被上限截掉的部分；截断信息看 MaxSessionRows）。
	Sessions int `json:"sessions"`
}

// Health 汇总服务健康度（ok = 库开着且索引器在跑）。
func (h *Hub) Health(ctx context.Context) (Health, error) {
	if h == nil {
		return Health{}, errors.New("host: Hub 为 nil")
	}
	wm, err := h.indexer.Watermark()
	if err != nil {
		return Health{OK: false}, err
	}
	rows, err := h.Sessions(ctx)
	if err != nil {
		return Health{OK: false, Watermark: wm}, err
	}
	return Health{OK: true, Watermark: wm, Sessions: len(rows)}, nil
}
