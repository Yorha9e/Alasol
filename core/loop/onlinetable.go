package loop

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// 本文件实现在线表注入（v1-S4：K14/K15/Q3/Q4）。
//
//	K14  暴露等级 = 注入长度：summary（默认，一行摘要）/ n（按需升，带窗口行数）/ full（需显式，
//	     全字段不截断）。窗口长度语义不变，只是"露多少"随等级变。
//	K15  默认 N=50 行、每行 summary ≤100 token、按 gate 过滤；**全部参数可配 + 运行期动态管理**
//	     （用户明确要求不写死）——默认值只是常量兜底，真正生效走 OnlineTableConfig 这个配置面。
//	Q4   在线表秒级变动 → 必须放 prompt **最末尾**，否则打穿前缀 prompt cache。
//	Q3   稳定→变动排序：system/tools 在前、history 次之、**在线表最末**（本段把含在线表的排序落地，
//	     完整分级留 v2）。
//	K11  只注入**主 agent**（session 的主 agent）的 prompt；按 gate 相交过滤对端主 agent，
//	     别 session 的 subagent 完全不出现（可见性过滤在数据源侧做，见 host/onlinetable.go）。
//
// 设计取舍：
//   - **渲染与配置在 core/loop**（上下文装配点 + 纯函数，可单测）；**"谁可见"在数据源侧**（core/discovery
//     经 host 适配注入）——loop 不 import core/discovery，只认一个 OnlineTableSource 接口，边界干净。
//   - 在线表是**每轮新鲜渲染**的注入段，不进 history（历史不可被这种秒级变动污染，也只在本轮请求里
//     追加以保证"末尾"位置）。

// 在线表默认值（K15）。**只是兜底常量**：真正生效必经 OnlineTableConfig.View() 解析
// （0/"" → 这里的默认），参数面运行期可调（见 OnlineTableConfig.Update）。
const (
	// DefaultOnlineTableRows 默认行数 N（K15：默认 50 行）。
	DefaultOnlineTableRows = 50
	// DefaultLineTokenBudget 默认每行 summary 的 token 预算（K15：每行 ≤100 token）。
	DefaultLineTokenBudget = 100
	// DefaultWindowLines 默认 level=n 时每行展开的窗口行数（K14：n 带窗口行数参数）。
	DefaultWindowLines = 5
)

// 参数上界（防 fat-finger 配置；宽松值，正常运行远达不到）。三个数值参数统一口径：
// **0 = 用默认、负数 = 拒、超上界 = 拒**（View()/渲染入口同口径解析 0 → 默认）。
const (
	MaxOnlineTableRows = 10000
	MaxLineTokenBudget = 100000
	MaxWindowLines     = 100
)

// 暴露等级（K14）：窗口长度语义不变，"露多少"随等级变。
const (
	// LevelSummary 默认：每行一行摘要，受 LineTokenBudget 截断。
	LevelSummary = "summary"
	// LevelN 按需升：每行展开至多 WindowLines 行（一个 agent 一行身份 + 若干事实行）。
	LevelN = "n"
	// LevelFull 需显式：全字段、全 id、不截断、不限行——最大细节。
	LevelFull = "full"
	// DefaultLevel 默认暴露等级（K14：默认 summary）。
	DefaultLevel = LevelSummary
)

// OnlineTableRow 是在线表的一行（loop 的通用视图，**不 import core/discovery**）。
//
// 数据源（host）把 discovery 的 self 行 + VisiblePeers 对端行折成这个形状喂进来；
// gate 过滤 / subagent 排除都在数据源侧完成（K11），loop 只负责渲染与注入。
type OnlineTableRow struct {
	// Self 标记这是本 session 自己的主 agent 行（恒在表首）。
	Self bool `json:"self"`
	// SessionID / MainAgentID 是这一行的 session 与（主）agent 身份。
	SessionID   string `json:"session_id"`
	MainAgentID string `json:"main_agent_id"`
	// Gates：self 行 = 自己的全部 gate；peer 行 = 与查询方**相交**的 gate（K11）。
	Gates []string `json:"gates,omitempty"`
	// LastKind / LastSeq / UpdatedAt 是"在干什么"（对端主 agent 的最近活动）。
	LastKind  string `json:"last_kind,omitempty"`
	LastSeq   int64  `json:"last_seq"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

// OnlineTableSource 提供某 session 的在线表行（self + gate 相交的对端主 agent，K11）。
//
// 宿主实现（host/onlinetable.go）负责拉式扫描发现表 + gate 过滤；core 不 import discovery。
// 返回空 = 本轮不注入（"没什么可显示"比塞一张空表诚实）。
type OnlineTableSource interface {
	OnlineRows(ctx context.Context, sessionID contracts.Ulid) []OnlineTableRow
}

// OnlineTableView 是在线表配置的**不可变快照**（渲染与 HTTP GET 共用；JSON 可直接回前端）。
type OnlineTableView struct {
	// MaxRows 行数上限 N（K15；0 由 View 解析为 DefaultOnlineTableRows）。
	MaxRows int `json:"max_rows"`
	// LineTokenBudget 每行 summary 的 token 预算（K15；0 由 View 解析为默认 100）。
	LineTokenBudget int `json:"line_token_budget"`
	// Level 暴露等级（K14：summary | n | full）。
	Level string `json:"level"`
	// WindowLines level=n 时每行展开的窗口行数（K14；0 由 View 解析为默认）。
	WindowLines int `json:"window_lines"`
}

// OnlineTableUpdate 是一次**部分更新**（nil 字段不变；运行期动态管理用，K15）。
type OnlineTableUpdate struct {
	MaxRows         *int    `json:"max_rows,omitempty"`
	LineTokenBudget *int    `json:"line_token_budget,omitempty"`
	Level           *string `json:"level,omitempty"`
	WindowLines     *int    `json:"window_lines,omitempty"`
}

// OnlineTableConfig 是在线表的**活配置**（并发安全；hub 建一个、所有 loop 共享、HTTP 端点调它）。
//
// 运行期可调（K15 硬要求）：Update 改字段 → 所有 loop 下一轮 request 时读 View() 立即生效，
// **不重启**。默认值在 New 里落到字段（走配置面，不写死在渲染路径）。
type OnlineTableConfig struct {
	mu              sync.RWMutex
	maxRows         int
	lineTokenBudget int
	level           string
	windowLines     int
}

// NewOnlineTableConfig 建一个带默认值的在线表配置（K15：N=50 / 每行 ≤100 token / summary）。
func NewOnlineTableConfig() *OnlineTableConfig {
	return &OnlineTableConfig{
		maxRows:         DefaultOnlineTableRows,
		lineTokenBudget: DefaultLineTokenBudget,
		level:           DefaultLevel,
		windowLines:     DefaultWindowLines,
	}
}

// View 取当前生效配置（解析 0/""/非法 → 默认；每次调用取一份不可变快照）。
func (c *OnlineTableConfig) View() OnlineTableView {
	if c == nil {
		return OnlineTableView{
			MaxRows:         DefaultOnlineTableRows,
			LineTokenBudget: DefaultLineTokenBudget,
			Level:           DefaultLevel,
			WindowLines:     DefaultWindowLines,
		}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return OnlineTableView{
		MaxRows:         positiveOr(c.maxRows, DefaultOnlineTableRows),
		LineTokenBudget: positiveOr(c.lineTokenBudget, DefaultLineTokenBudget),
		Level:           levelOr(c.level),
		WindowLines:     positiveOr(c.windowLines, DefaultWindowLines),
	}
}

// Update 应用一次部分更新（nil 字段不变）。**先校验后应用**：校验失败 → 原配置一动不动 + 返回 error
// （不会改了一半）。
func (c *OnlineTableConfig) Update(u OnlineTableUpdate) error {
	if c == nil {
		return errors.New("loop: OnlineTableConfig 为 nil")
	}
	if u.MaxRows != nil && (*u.MaxRows < 0 || *u.MaxRows > MaxOnlineTableRows) {
		return fmt.Errorf("loop: max_rows 须在 0…%d（得到 %d；0 = 用默认 %d）", MaxOnlineTableRows, *u.MaxRows, DefaultOnlineTableRows)
	}
	if u.LineTokenBudget != nil && (*u.LineTokenBudget < 0 || *u.LineTokenBudget > MaxLineTokenBudget) {
		return fmt.Errorf("loop: line_token_budget 须在 0…%d（得到 %d；0 = 用默认 %d）", MaxLineTokenBudget, *u.LineTokenBudget, DefaultLineTokenBudget)
	}
	if u.WindowLines != nil && (*u.WindowLines < 0 || *u.WindowLines > MaxWindowLines) {
		return fmt.Errorf("loop: window_lines 须在 0…%d（得到 %d；0 = 用默认 %d）", MaxWindowLines, *u.WindowLines, DefaultWindowLines)
	}
	if u.Level != nil && !validLevel(*u.Level) {
		return fmt.Errorf("loop: level 只支持 %s | %s | %s（得到 %q）", LevelSummary, LevelN, LevelFull, *u.Level)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if u.MaxRows != nil {
		c.maxRows = *u.MaxRows
	}
	if u.LineTokenBudget != nil {
		c.lineTokenBudget = *u.LineTokenBudget
	}
	if u.Level != nil {
		c.level = *u.Level
	}
	if u.WindowLines != nil {
		c.windowLines = *u.WindowLines
	}
	return nil
}

func positiveOr(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}

func levelOr(v string) string {
	if validLevel(v) {
		return v
	}
	return DefaultLevel
}

func validLevel(v string) bool {
	switch v {
	case LevelSummary, LevelN, LevelFull:
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// 渲染（纯函数：rows + view → 文本 + 实际行数）
// ---------------------------------------------------------------------------

// RenderOnlineTable 把在线表行渲染成注入文本，返回 (文本, 实际渲染行数)。
//
// 纯函数（不碰 IO / 不碰配置对象）——宿主与测试都可直接调。行数受 view.MaxRows 截断（K15），
// 每行内容随 view.Level 变长变短（K14）。空行集 → 空串 + 0（调用方据此决定不注入）。
func RenderOnlineTable(rows []OnlineTableRow, view OnlineTableView) (string, int) {
	view = normalizeView(view)
	if len(rows) == 0 {
		return "", 0
	}
	shown, truncated := truncateRows(rows, view.MaxRows)
	var b strings.Builder
	b.WriteString(onlineTableHeader(view.Level, len(shown), truncated, view.MaxRows))
	for i := range shown {
		b.WriteString(renderOnlineRow(shown[i], view))
	}
	if truncated > 0 {
		fmt.Fprintf(&b, "…（在线表按 N=%d 截断，另有 %d 行未显示）\n", view.MaxRows, truncated)
	}
	return b.String(), len(shown)
}

// normalizeView 把**导出渲染入口**的 0/非法值解析成默认——与 OnlineTableConfig.View() 同口径
// （MaxRows/LineTokenBudget/WindowLines 的 0 = 用默认，非法等级 = summary）。行数上限是 K15
// 硬约束，不该由调用方记着：传 0 是"用默认 50"，**不是**"不限行"。
func normalizeView(v OnlineTableView) OnlineTableView {
	v.MaxRows = positiveOr(v.MaxRows, DefaultOnlineTableRows)
	v.LineTokenBudget = positiveOr(v.LineTokenBudget, DefaultLineTokenBudget)
	v.WindowLines = positiveOr(v.WindowLines, DefaultWindowLines)
	v.Level = levelOr(v.Level)
	return v
}

// truncateRows 按 MaxRows 截断（返回保留的行 + 被略去的行数）。
func truncateRows(rows []OnlineTableRow, maxRows int) ([]OnlineTableRow, int) {
	if maxRows <= 0 {
		maxRows = DefaultOnlineTableRows
	}
	if len(rows) > maxRows {
		return rows[:maxRows], len(rows) - maxRows
	}
	return rows, 0
}

// onlineTableHeader 是在线表头部（含 "[在线表" 标记 —— 验收据此断言"末尾是在线表"）。
func onlineTableHeader(level string, shown, truncated, maxRows int) string {
	var b strings.Builder
	b.WriteString("[在线表 / online-peers] ")
	fmt.Fprintf(&b, "暴露等级=%s（K14：默认 summary，n 按需升，full 需显式）；", level)
	b.WriteString("仅含本 session 水平可见的**主 agent**（K11：按 gate 相交；别 session 的 subagent 不在此列）。")
	fmt.Fprintf(&b, "以下 %d 行", shown)
	if truncated > 0 {
		fmt.Fprintf(&b, "（受 N=%d 限制，略去 %d 行）", maxRows, truncated)
	}
	b.WriteString("：\n")
	return b.String()
}

func renderOnlineRow(r OnlineTableRow, view OnlineTableView) string {
	switch view.Level {
	case LevelN:
		return renderRowWindowed(r, view)
	case LevelFull:
		return renderRowFull(r)
	default: // summary
		return renderRowSummary(r, view)
	}
}

// renderRowSummary 一行摘要（短 id），受 LineTokenBudget 截断（K15：每行 ≤100 token）。
func renderRowSummary(r OnlineTableRow, view OnlineTableView) string {
	line := fmt.Sprintf("- [%s] session=%s main=%s gates=[%s] doing=%s @%s",
		selfLabel(r), shortID(r.SessionID), shortID(r.MainAgentID),
		strings.Join(r.Gates, ","), orDash(r.LastKind), orDash(r.UpdatedAt))
	return truncateToTokens(line, view.LineTokenBudget) + "\n"
}

// renderRowWindowed level=n：身份行 + 若干事实行，总行数受 WindowLines 限制（K14 窗口行数）。
func renderRowWindowed(r OnlineTableRow, view OnlineTableView) string {
	lines := []string{
		fmt.Sprintf("- [%s] session=%s main=%s", selfLabel(r), shortID(r.SessionID), shortID(r.MainAgentID)),
		fmt.Sprintf("    gates: [%s]", strings.Join(r.Gates, ",")),
		fmt.Sprintf("    doing: %s (seq %d) @%s", orDash(r.LastKind), r.LastSeq, orDash(r.UpdatedAt)),
	}
	n := view.WindowLines
	if n <= 0 || n > len(lines) {
		n = len(lines)
	}
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteString(lines[i])
		b.WriteString("\n")
	}
	return b.String()
}

// renderRowFull level=full：全字段 + 全 id（26 字符）、不截断、不限行（K14：full 需显式）。
func renderRowFull(r OnlineTableRow) string {
	var b strings.Builder
	fmt.Fprintf(&b, "- [%s] session=%s main=%s\n", selfLabel(r), orDash(r.SessionID), orDash(r.MainAgentID))
	fmt.Fprintf(&b, "    gates: [%s]\n", strings.Join(r.Gates, ","))
	fmt.Fprintf(&b, "    last_kind: %s\n", orDash(r.LastKind))
	fmt.Fprintf(&b, "    last_seq: %d\n", r.LastSeq)
	fmt.Fprintf(&b, "    updated_at: %s\n", orDash(r.UpdatedAt))
	return b.String()
}

func selfLabel(r OnlineTableRow) string {
	if r.Self {
		return "self"
	}
	return "peer"
}

// shortID 取 id **随机尾段**的 8 字符（S15 精神：展示用短形、比较一律用完整值——这里只用于渲染）。
//
// 为什么不取前缀：ULID = 10 字符毫秒时间戳 + 16 字符随机，前 8 字符整个落在时间戳段内
// （≈256ms 粒度）——同一批铸造的 session/agent id 前缀逐字节相同，在线表行会**不可区分**
// （默认 summary 等级只给短 id，模型分不清"谁是谁"）。尾段 8 字符取自 80bit 随机段，
// 碰撞概率可忽略。
func shortID(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[len(s)-8:]
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// truncateToTokens 把一行截到 ≤ maxTokens token（估算口径 len/4，J21）。
//
// maxTokens<=0 = 不截断（n / full 等级不走预算）。截断是 **rune 安全**的（不把多字节字符切成两半），
// 尾部补 "..."；保证结果 estimateTokens ≤ maxTokens。
func truncateToTokens(s string, maxTokens int) string {
	if maxTokens <= 0 || estimateTokens(s) <= int64(maxTokens) {
		return s
	}
	budget := maxTokens*tokensPerChar - len(ellipsis) // 给省略号留字节
	if budget < 1 {
		budget = 1
	}
	var b strings.Builder
	used := 0
	for _, r := range s {
		sz := utf8.RuneLen(r)
		if sz < 0 {
			sz = 1 // 非法 rune 防御（RuneError 返回 -1）
		}
		if used+sz > budget {
			break
		}
		b.WriteRune(r)
		used += sz
	}
	b.WriteString(ellipsis)
	return b.String()
}

const ellipsis = "..."

// ---------------------------------------------------------------------------
// Loop 集成：每轮新鲜渲染 + 只注入主 agent
// ---------------------------------------------------------------------------

// onlineTable 是本轮渲染好的在线表（请求末尾消息 + 快照 part 的素材）。
type onlineTable struct {
	// message 是追加到请求**最末尾**的消息（role=user，内容是渲染文本）。
	message core.Message
	// rows 是实际渲染行数（= 快照 online_table part 的 entries）。
	rows int
	// tokens 是渲染文本的估算 token（= 快照 online_table part 的 tokens，J21）。
	tokens int64
}

// onlineTableForTurn 渲染本轮的在线表（每轮新鲜，Q4 秒级变动）。返回 nil = 本轮不注入：
//
//	a) 未配 OnlineTable / OnlineTableSource（默认关闭——老装配与单测不受影响）；
//	b) 本 loop 是 subagent（K11：只有主 agent 注入在线表）；
//	c) 数据源无行 / 渲染为空（"没什么可显示"）。
func (l *Loop) onlineTableForTurn(ctx context.Context) *onlineTable {
	if l == nil || l.onlineSource == nil || l.onlineConfig == nil {
		return nil
	}
	if !l.isMainAgent() {
		return nil
	}
	rows := l.onlineSource.OnlineRows(ctx, l.sessionID)
	if len(rows) == 0 {
		return nil
	}
	text, n := RenderOnlineTable(rows, l.onlineConfig.View())
	if strings.TrimSpace(text) == "" || n == 0 {
		return nil
	}
	return &onlineTable{
		message: core.Message{Role: "user", Content: text},
		rows:    n,
		tokens:  estimateTokens(text),
	}
}

// isMainAgent 报告本 loop 是否是 session 的主 agent（K11：只有主 agent 注入在线表）。
//
// 单 agent 模式（Agents 为 nil）= 主 agent；多 agent 模式按 agent 树深度判（depth<=1 = 主 agent，
// 口径同 visibleTools 的 isSubagent）。树里查不到（异常）→ 当主 agent（注入无副作用）。
func (l *Loop) isMainAgent() bool {
	if l.agents == nil || l.agents.Tree == nil {
		return true
	}
	node, ok := l.agents.Tree.Get(l.agentID)
	if !ok {
		return true
	}
	return node.Depth <= 1
}
