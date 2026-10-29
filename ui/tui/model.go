package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/host"
	tea "github.com/charmbracelet/bubbletea"
)

// tab 是三面 TUI 的当前面（O6：对话 / 审批 / 在线表）。
type tab int

const (
	tabChat tab = iota
	tabApprovals
	tabOnlineTable
)

func (t tab) title() string {
	switch t {
	case tabApprovals:
		return "审批"
	case tabOnlineTable:
		return "在线表"
	default:
		return "对话"
	}
}

// sessionsPollInterval 在线表轮询间隔。
const sessionsPollInterval = 2 * time.Second

// Model 是 bubbletea 的 TUI 模型（指针类型，字段可变）。
//
// 三面共享一个进程内 hub：对话面发 prompt（hub.RunPrompt，审批面在期间收人审请求），
// 在线表面轮询 hub.Sessions。
type Model struct {
	hub      *host.Hub
	session  contracts.Ulid
	approver *Approver

	width, height int
	activeTab     tab

	// 对话面
	input   string   // 当前输入行（手写输入框）
	chatLog []string // 滚动历史
	running bool     // 一轮 RunPrompt 进行中

	// 审批面
	pending     []approvalItem
	selApproval int

	// 在线表面
	sessions    []host.SessionRow
	sessionsErr string
	watermark   int64
}

// approvalItem 是一个待审批请求（UI 渲染用）。
type approvalItem struct {
	id      contracts.Ulid
	summary string
	tool    string
}

// Options 是 TUI 运行参数。
type Options struct {
	Hub      *host.Hub
	Session  contracts.Ulid
	Approver *Approver
}

func newModel(opts Options) *Model {
	return &Model{
		hub:      opts.Hub,
		session:  opts.Session,
		approver: opts.Approver,
		chatLog:  []string{"[alasol] TUI 就绪。F1 对话 · F2 审批 · F3 在线表 · Ctrl-C 退出"},
	}
}

// Init 启动在线表轮询。
func (m *Model) Init() tea.Cmd {
	return tea.Batch(fetchSessionsCmd(m.hub), tickCmd())
}

// ---- 消息类型 ----

type chatDoneMsg struct {
	text string
	err  error
}
type btwDoneMsg struct {
	text    string
	forkSID contracts.Ulid
	// historyMsgs 是旁支带上的主对话条数；truncated / trimmed 标记两种裁切
	// （F7/N5）：truncated = 触了上限截断，trimmed = 工具调用配对不完整被丢弃。
	historyMsgs int
	truncated   bool
	trimmed     bool
	err         error
}
type approvalRequestMsg struct {
	id      contracts.Ulid
	summary string
	tool    string
}
type sessionsMsg struct {
	rows      []host.SessionRow
	watermark int64
	err       error
}
type tickMsg struct{}

// Update 处理消息与按键。
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil

	case chatDoneMsg:
		m.running = false
		if msg.err != nil {
			m.chatLog = append(m.chatLog, "alasol> [错误] "+msg.err.Error())
		} else if msg.text != "" {
			m.chatLog = append(m.chatLog, "alasol> "+msg.text)
		} else {
			m.chatLog = append(m.chatLog, "alasol> （无文本输出）")
		}
		return m, nil

	case btwDoneMsg:
		// /btw 旁支结果（K29）：只给用户展示，**不回灌**主会话（主 session 事件流零新增）。
		m.running = false
		if msg.err != nil {
			m.chatLog = append(m.chatLog, "btw> [错误] "+msg.err.Error())
		} else if msg.text != "" {
			m.chatLog = append(m.chatLog, "btw> "+msg.text)
		} else {
			m.chatLog = append(m.chatLog, "btw> （无文本输出）")
		}
		m.chatLog = append(m.chatLog, dim(fmt.Sprintf("[btw] 旁支 session=%s（未回灌主会话）", msg.forkSID)))
		// 两种裁切分开说（F7/N5）：上限截断 ≠ 配平丢弃。
		if msg.truncated {
			m.chatLog = append(m.chatLog, dim(fmt.Sprintf("[btw] 主对话已按旁支上下文上限截断（只带最近 %d 条）", msg.historyMsgs)))
		}
		if msg.trimmed {
			m.chatLog = append(m.chatLog, dim(fmt.Sprintf("[btw] 旁支主对话里有工具调用配对不完整的片段被丢弃（只带 %d 条）", msg.historyMsgs)))
		}
		return m, nil

	case approvalRequestMsg:
		m.pending = append(m.pending, approvalItem{id: msg.id, summary: msg.summary, tool: msg.tool})
		m.activeTab = tabApprovals // 来人审自动切到审批面
		return m, nil

	case sessionsMsg:
		m.sessions = msg.rows
		m.watermark = msg.watermark
		if msg.err != nil {
			m.sessionsErr = msg.err.Error()
		} else {
			m.sessionsErr = ""
		}
		return m, nil

	case tickMsg:
		return m, tea.Batch(fetchSessionsCmd(m.hub), tickCmd())

	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

// handleKey 分派按键：全局切面 + 各面专属。
func (m *Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		return m, tea.Quit
	case "f1":
		m.activeTab = tabChat
		return m, nil
	case "f2":
		m.activeTab = tabApprovals
		return m, nil
	case "f3":
		m.activeTab = tabOnlineTable
		return m, nil
	}

	switch m.activeTab {
	case tabChat:
		return m.handleChatKey(msg)
	case tabApprovals:
		return m.handleApprovalKey(msg)
	default:
		return m, nil // 在线表面无专属键（自动轮询）
	}
}

func (m *Model) handleChatKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyRunes:
		m.input += string(msg.Runes)
	case tea.KeyBackspace:
		m.input = dropLastRune(m.input)
	case tea.KeyEnter:
		line := strings.TrimSpace(m.input)
		m.input = ""
		if line == "" || m.running || m.hub == nil {
			return m, nil
		}
		// /btw <问题>：旁支 fork（K29）——带主 session 上下文跑一轮，结果只给用户、不回灌主会话。
		// 命令解析与 CLI 共用同一个口径（host.ParseBtwCommand）：**空格与 tab 都算分隔符**
		//（"/btwxyz" 不当命令，按普通 prompt 走）。
		if question, isBtw := host.ParseBtwCommand(line); isBtw {
			m.chatLog = append(m.chatLog, "you> "+line)
			if question == "" {
				m.chatLog = append(m.chatLog, dim("[btw] 用法：/btw <问题>（旁支询问，不影响主会话）"))
				return m, nil
			}
			m.running = true
			return m, runBtwCmd(m.hub, m.session, question)
		}
		// /workflow [<id>|off]：激活/停用/列出工作流能力包（v3-S3，W3）。
		// 与 CLI（alasol chat）同款口径（host.ParseWorkflowCommand + ActivateWorkflow）：
		// 激活只把能力包哲学拼进 system prompt 供主 agent 选择（非强制，W21），**不跑模型**
		// → 同步做完即可（不进 running 态）。
		if arg, isWF := host.ParseWorkflowCommand(line); isWF {
			m.chatLog = append(m.chatLog, "you> "+line)
			m.handleWorkflowCommand(arg)
			return m, nil
		}
		// /<命令>：能力包**自己声明**的 slash 触发入口（W3，如 tower 的 /tower）——
		// 真实入口（Registry.ByCommand 的生产调用方），不跑模型。
		if cmd, isTrigger := m.hub.WorkflowCommandTrigger(line); isTrigger {
			m.chatLog = append(m.chatLog, "you> "+line)
			p, err := m.hub.ActivateWorkflowByCommand(context.Background(), m.session, cmd)
			if err != nil {
				m.chatLog = append(m.chatLog, "[workflow] 激活失败: "+err.Error())
				return m, nil
			}
			m.chatLog = append(m.chatLog, fmt.Sprintf(
				"[workflow] 已激活 %q（命令=/%s）：哲学文本将注入 system prompt 供主 agent 选择（非强制）", p.ID(), cmd))
			return m, nil
		}
		m.chatLog = append(m.chatLog, "you> "+line)
		m.running = true
		return m, runPromptCmd(m.hub, m.session, line)
	}
	return m, nil
}

// handleWorkflowCommand 处理 /workflow [<id>|off]（v3-S3，W3 触发；与 chat 同款口径）。
//
// 激活是 **session 级状态变更**（哲学拼进该 session 的 system prompt）+ 记一条
// workflow.activated 事件——不跑模型、不改对话流程，故同步做完即可。
func (m *Model) handleWorkflowCommand(arg string) {
	ctx := context.Background()
	switch arg {
	case "":
		// 列出可用能力包。
		pkgs := m.hub.Workflows().List()
		if len(pkgs) == 0 {
			m.chatLog = append(m.chatLog, dim("[workflow] （没有可用的工作流能力包）"))
			return
		}
		m.chatLog = append(m.chatLog, "可用工作流能力包：")
		for _, p := range pkgs {
			cmd := p.Command()
			if cmd == "" {
				cmd = "（显式触发）"
			}
			active := ""
			if aw := m.hub.ActiveWorkflow(m.session); aw != nil && aw.ID() == p.ID() {
				active = " [已激活]"
			}
			m.chatLog = append(m.chatLog, fmt.Sprintf("  %-20s 命令=/%s%s", p.ID(), cmd, active))
		}
	case "off":
		m.hub.DeactivateWorkflow(m.session)
		m.chatLog = append(m.chatLog, "[workflow] 已停用工作流（回到默认 system prompt）")
	default:
		p, err := m.hub.ActivateWorkflow(ctx, m.session, arg)
		if err != nil {
			m.chatLog = append(m.chatLog, "[workflow] 激活失败: "+err.Error())
			return
		}
		m.chatLog = append(m.chatLog, fmt.Sprintf(
			"[workflow] 已激活 %q：哲学文本将注入 system prompt 供主 agent 选择（非强制）", p.ID()))
	}
}

func (m *Model) handleApprovalKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if len(m.pending) == 0 {
		return m, nil
	}
	switch msg.String() {
	case "up", "k":
		if m.selApproval > 0 {
			m.selApproval--
		}
	case "down", "j":
		if m.selApproval < len(m.pending)-1 {
			m.selApproval++
		}
	case "y", "Y":
		return m, m.decideCurrent(true)
	case "n", "N":
		return m, m.decideCurrent(false)
	}
	return m, nil
}

// decideCurrent 对当前选中的审批请求下决策（y=allow / n=deny）。
func (m *Model) decideCurrent(allow bool) tea.Cmd {
	if m.selApproval >= len(m.pending) {
		return nil
	}
	item := m.pending[m.selApproval]
	// 从待办移除。
	m.pending = append(m.pending[:m.selApproval], m.pending[m.selApproval+1:]...)
	if m.selApproval >= len(m.pending) && m.selApproval > 0 {
		m.selApproval--
	}
	verdict := "deny"
	if allow {
		verdict = "allow"
	}
	m.chatLog = append(m.chatLog, fmt.Sprintf("[审批] %s → %s", item.summary, verdict))
	if m.approver != nil {
		m.approver.resolve(item.id, allow)
	}
	return nil
}

// View 渲染当前面。
func (m *Model) View() string {
	var b strings.Builder
	b.WriteString(renderTabs(m.activeTab))
	b.WriteString("\n")
	switch m.activeTab {
	case tabApprovals:
		b.WriteString(m.viewApprovals())
	case tabOnlineTable:
		b.WriteString(m.viewOnlineTable())
	default:
		b.WriteString(m.viewChat())
	}
	return b.String()
}

func (m *Model) viewChat() string {
	var b strings.Builder
	// 滚动历史（保留最后若干行）。
	log := m.chatLog
	if max := m.height - 4; max > 0 && len(log) > max {
		log = log[len(log)-max:]
	}
	for _, line := range log {
		b.WriteString(line + "\n")
	}
	b.WriteString("\n")
	prompt := "you> " + m.input
	if m.running {
		prompt += "  （运行中…）"
	}
	b.WriteString(prompt + "\n")
	b.WriteString(dim("[Enter 发送 · F2 审批 · F3 在线表 · Ctrl-C 退出]"))
	b.WriteString("\n")
	b.WriteString(dim("命令：/btw <问题> 旁支询问 · /workflow [<id>|off] 工作流能力包"))
	return b.String()
}

func (m *Model) viewApprovals() string {
	var b strings.Builder
	if len(m.pending) == 0 {
		b.WriteString("（无待审批请求；-ask 列出的工具命中 ask 规则时会在这里提示）\n")
		return b.String()
	}
	for i, it := range m.pending {
		cursor := "  "
		if i == m.selApproval {
			cursor = "> "
		}
		b.WriteString(fmt.Sprintf("%s%s\n", cursor, it.summary))
		if it.tool != "" {
			b.WriteString(dim(fmt.Sprintf("    tool_call=%s", it.tool)) + "\n")
		}
	}
	b.WriteString("\n" + dim("[↑/↓ 或 j/k 选择 · y 允许 · n 拒绝]"))
	return b.String()
}

func (m *Model) viewOnlineTable() string {
	var b strings.Builder
	if m.sessionsErr != "" {
		b.WriteString("在线表错误: " + m.sessionsErr + "\n")
	}
	if len(m.sessions) == 0 {
		b.WriteString("（还没有任何 session）\n")
		return b.String()
	}
	b.WriteString(fmt.Sprintf("%-28s %8s %-24s %10s\n", "session", "事件数", "最后 kind", "水位"))
	for _, r := range m.sessions {
		b.WriteString(fmt.Sprintf("%-28s %8d %-24s %10d\n",
			r.SessionID, r.EventCount, r.LastKind, m.watermark))
	}
	return b.String()
}

// ---- 命令 ----

func runPromptCmd(h *host.Hub, session contracts.Ulid, prompt string) tea.Cmd {
	return func() tea.Msg {
		res, err := h.RunPrompt(context.Background(), session, prompt)
		return chatDoneMsg{text: res.Text, err: err}
	}
}

// runBtwCmd 跑一次 /btw 旁支 fork（K29）：带主 session 上下文跑一轮，结果只给用户。
func runBtwCmd(h *host.Hub, session contracts.Ulid, question string) tea.Cmd {
	return func() tea.Msg {
		res, err := h.RunBtw(context.Background(), session, question)
		return btwDoneMsg{
			text:        res.Text,
			forkSID:     res.ForkSessionID,
			historyMsgs: res.HistoryMessages,
			truncated:   res.HistoryTruncated,
			trimmed:     res.HistoryTrimmed,
			err:         err,
		}
	}
}

func fetchSessionsCmd(h *host.Hub) tea.Cmd {
	return func() tea.Msg {
		if h == nil {
			return sessionsMsg{}
		}
		rows, err := h.Sessions(context.Background())
		wm := int64(0)
		if err == nil {
			wm, _ = h.Watermark()
		}
		return sessionsMsg{rows: rows, watermark: wm, err: err}
	}
}

func tickCmd() tea.Cmd {
	return tea.Tick(sessionsPollInterval, func(time.Time) tea.Msg { return tickMsg{} })
}

// ---- 小工具 ----

func dropLastRune(s string) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	return string(r[:len(r)-1])
}

func renderTabs(active tab) string {
	names := []string{"F1 对话", "F2 审批", "F3 在线表"}
	idx := int(active)
	var parts []string
	for i, n := range names {
		if i == idx {
			parts = append(parts, bold("["+n+"]"))
		} else {
			parts = append(parts, dim(n))
		}
	}
	return strings.Join(parts, "  ")
}
