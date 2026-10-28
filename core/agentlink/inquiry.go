package agentlink

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agents"
	"github.com/Yorha9e/alasol/core/discovery"
)

// agent.completed 的 kind（K24：summary 经它落盘，是 subagent 回到主 agent 的唯一通道）。
// 常量与 core/loop / discovery 的同值；agentlink 只读它，不写它。
const kindAgentCompleted = discovery.KindAgentCompleted

// InquiryRequest 是一次深入查询（K18 Inquiry / Q6：结构化摘要 + 事件窗口，两者都给）。
type InquiryRequest struct {
	// Caller 查询方身份（K11 可见性判定用，必填）。
	Caller Identity
	// SessionID 被查 session。
	SessionID contracts.Ulid
	// Window 被查事件窗口（seq 闭区间；FromSeq<=0 → 从 1 起，ToSeq<=0 → 不设上界）。
	Window EventWindow
	// AgentID 非空 → 只回归属该 agent 的事件（K25：按 agent_id 对全局流做投影）。
	AgentID contracts.Ulid
	// Limit 窗口条数上限（<=0 不限；作用于**读取**，之后再按窗口上界/agent 过滤）。
	Limit int
	// Refs 要核验的引用（K24）。空 = 自动取窗口内全部 summary 的 refs（保序去重）。
	Refs []string
}

// SummaryEntry 是窗口内一条结构化摘要（Q6：摘要给模型直接消费）。
type SummaryEntry struct {
	AgentID string                 `json:"agent_id"`
	EventID string                 `json:"event_id"`
	Seq     int64                  `json:"seq"`
	TS      string                 `json:"ts"`
	Summary contracts.AgentSummary `json:"summary"`
}

// RefCheck 是一条引用的核验明细（K24：summary 的 refs 必须可被 Inquiry 拉原文核验）。
type RefCheck struct {
	Ref string `json:"ref"`
	OK  bool   `json:"ok"`
	// Detail 说明结论（失败时写清为什么不可核验——fail-close 也要让人知道卡在哪）。
	Detail string `json:"detail,omitempty"`
}

// InquiryResult 是 Inquiry 的返回（Q6 两者都给：摘要 + 事件窗口）。
type InquiryResult struct {
	SessionID string `json:"session_id"`
	// Summaries 是窗口内的 agent.completed 结构化摘要（按 seq 升序）。
	Summaries []SummaryEntry `json:"summaries"`
	// Window 是事件窗口**原文**（Q6：窗口给模型按需翻；也是 refs 核验的证据来源）。
	Window []contracts.Event `json:"window"`
	// WindowRange 是实际返回的 seq 区间（First/Last 为 0 = 窗口为空）。
	WindowRange EventWindow `json:"window_range"`
	// Truncated 标记读取撞到了 Limit（诚实报告：调用方别以为看全了，翻页继续拉）。
	Truncated bool `json:"truncated"`
	// RefChecks 是 refs 逐条核验明细（与请求/摘要 refs 同序）。
	RefChecks []RefCheck `json:"ref_checks"`
	// RefsAllOK 是 RefChecks 是否全部通过（无 refs 时 vacuously true）。
	RefsAllOK bool `json:"refs_all_ok"`
}

// Inquiry 深入查询一个 session（K18 Inquiry）：返回**结构化摘要 + 事件窗口**（Q6）。
//
// 流程：可见性检查（K11）→ 经注入的 EventWindowReader 拉窗口（既有读路径，core 不碰 IO）
// → 窗口内折出 summary → 对 refs 逐条核验（K24：拿窗口里的原文当证据）。
//
// 可见性（K11）只认两条路：
//
//	① 同 session（父查自己的子树、主 agent 查自己的 session）；
//	② 与目标 session **gate 相交**的对端主 agent（discovery.VisiblePeers 现成查询）。
//
// 跨层上查因此仍是"沿父链逐级显式调用"（K27）：每一跳都是一次 Inquiry，不是"全祖先自动可见"。
func (l *Link) Inquiry(ctx context.Context, req InquiryRequest) (InquiryResult, error) {
	res := InquiryResult{
		SessionID: string(req.SessionID),
		Summaries: []SummaryEntry{},
		Window:    []contracts.Event{},
		RefChecks: []RefCheck{},
		RefsAllOK: true,
	}
	if l == nil {
		return res, ErrNilLink
	}
	if !req.Caller.valid() {
		return res, fmt.Errorf("%w: caller=%+v", ErrBadIdentity, req.Caller)
	}
	if !core.ValidUlid(req.SessionID) {
		return res, fmt.Errorf("%w: session_id %q 不是合法 ULID", ErrBadIdentity, req.SessionID)
	}
	if l.reader == nil {
		return res, ErrNoReader
	}
	if err := l.checkVisible(req.Caller, req.SessionID); err != nil {
		return res, err
	}

	fromSeq := int64(1)
	if req.Window.FromSeq > 0 {
		fromSeq = req.Window.FromSeq
	}
	// 读 limit+1 条：多读一条只为**诚实判断**"还有没有更多"（F10）——
	// 恰好读满 limit 时不能报 truncated（那会把"刚好读完"说成"没读完"）。
	readLimit := 0
	if req.Limit > 0 {
		readLimit = req.Limit + 1
	}
	evs, err := l.reader.ReadFrom(ctx, &req.SessionID, fromSeq, readLimit)
	if err != nil {
		return res, fmt.Errorf("agentlink: 读 session %s 的事件窗口: %w", req.SessionID, err)
	}
	if req.Limit > 0 && len(evs) > req.Limit {
		res.Truncated = true
		evs = evs[:req.Limit]
	}
	// 投影（F8）：窗口是**要进模型上下文**的东西，可见性必须在这里落地。
	//
	//	显式投影（req.AgentID 非空） → 只回该 agent 的事件（同 session 时它必须在调用方的
	//	                                 可见集里：父 + 直系子，K11②——兄弟不可见）；
	//	同 session 默认             → 只回调用方可见集（自己 + 父 + 直系子）里的事件；
	//	跨 session（可见对端）       → 整个被查 session（对端主 agent 看的是"对端 session
	//	                                 视角"；同 session 的隔代/兄弟不可见是**树内**的隔离）。
	allowed, err := l.projectionOf(req)
	if err != nil {
		return res, err
	}
	for i := range evs {
		ev := &evs[i]
		if req.Window.ToSeq > 0 && ev.Seq != nil && *ev.Seq > req.Window.ToSeq {
			continue // 窗口上界之外（ReadFrom 只支持下界，上界在这里裁）
		}
		if allowed != nil && !allowed[ev.AgentID] {
			continue // 可见集之外（K11）
		}
		res.Window = append(res.Window, *ev)
	}
	if len(res.Window) > 0 {
		first, last := res.Window[0], res.Window[len(res.Window)-1]
		if first.Seq != nil {
			res.WindowRange.FromSeq = *first.Seq
		}
		if last.Seq != nil {
			res.WindowRange.ToSeq = *last.Seq
		}
	}

	for i := range res.Window {
		if s, ok := summaryOf(&res.Window[i]); ok {
			res.Summaries = append(res.Summaries, s)
		}
	}

	refs := req.Refs
	if len(refs) == 0 {
		refs = summaryRefs(res.Summaries)
	}
	if len(refs) == 0 {
		// 没什么可核验：不是错误（Q6 允许只有摘要没有引用的查询），但要说清楚。
		return res, nil
	}
	// 复用 core/agents 的 VerifyRefs（K24 的核验路径，别重写）；明细由核验器内部记账补齐。
	v := newWindowVerifier(res.Window)
	results, allOK := agents.VerifyRefs(ctx, v, refs)
	res.RefsAllOK = allOK
	for i, ref := range refs {
		res.RefChecks = append(res.RefChecks, RefCheck{Ref: ref, OK: results[i], Detail: v.detailOf(ref)})
	}
	return res, nil
}

// projectionOf 算这次查询的**投影**（哪些 agent 的事件可以进窗口）。
//
// 返回 nil = 不投影（整个被查 session 都可见：跨 session 的可见对端视角）。
// 返回非 nil = 只有集合里的 agent 的事件进窗口。
func (l *Link) projectionOf(req InquiryRequest) (map[contracts.Ulid]bool, error) {
	sameSession := req.Caller.SessionID == req.SessionID
	if req.AgentID != "" {
		if sameSession && !l.agentVisibleTo(req.Caller, req.AgentID) {
			// 显式投影也逃不出 K11：兄弟/隔代 agent 的事件不可见（fail-close）。
			return nil, fmt.Errorf("%w: agent %s 不在调用方 %s 的可见集里（父 + 直系子，K11②）",
				ErrNotVisible, req.AgentID, req.Caller.AgentID)
		}
		return map[contracts.Ulid]bool{req.AgentID: true}, nil
	}
	if !sameSession {
		return nil, nil // 跨 session 的可见对端：整个 session 视角
	}
	if l == nil || l.table == nil {
		return nil, ErrNoTable
	}
	return l.treeOf(req.SessionID).selfVisibleAgents(req.Caller.AgentID), nil
}

// agentVisibleTo 报告 target 是否在 caller 的 K11 垂直可见集里（父 + 直系子；自己也算）。
func (l *Link) agentVisibleTo(caller Identity, target contracts.Ulid) bool {
	if l == nil || l.table == nil {
		return false
	}
	if caller.AgentID == target {
		return true
	}
	return l.treeOf(caller.SessionID).visibleAgents(caller.AgentID)[target]
}

// checkVisible 按 K11 检查查询方是否看得到目标 session。
//
// 同 session 恒可见；否则走水平可见性（K11①）：**调用方必须是自己 session 的主 agent**
// （水平可见性只在 session 的主 agent 之间成立，别 session 的 subagent 完全不可见），
// 且目标 session 必须出现在 VisiblePeers（gate 相交 ∩ 存活——discovery 的现成查询，
// 本包不复制 K11 逻辑）。
func (l *Link) checkVisible(caller Identity, target contracts.Ulid) error {
	if l.table == nil {
		return ErrNoTable
	}
	if caller.SessionID == target {
		return nil
	}
	if _, ok := l.table.Session(target); !ok {
		return fmt.Errorf("%w: %s", ErrUnknownSession, target)
	}
	main, ok := l.table.PrimaryAgent(caller.SessionID)
	if !ok || main != caller.AgentID {
		return fmt.Errorf("%w: caller session=%s agent=%s → target=%s（水平可见性只在**主 agent**之间成立，K11①）",
			ErrNotVisible, caller.SessionID, caller.AgentID, target)
	}
	for _, p := range l.table.VisiblePeers(caller.SessionID) {
		if contracts.Ulid(p.SessionID) == target {
			return nil
		}
	}
	return fmt.Errorf("%w: caller session=%s agent=%s → target=%s（需同 session，或与目标 gate 相交的存活主 agent）",
		ErrNotVisible, caller.SessionID, caller.AgentID, target)
}

// summaryOf 从一条事件折出 SummaryEntry（只认 agent.completed；坏载荷跳过，D7 宽容）。
func summaryOf(ev *contracts.Event) (SummaryEntry, bool) {
	if ev.Kind != kindAgentCompleted {
		return SummaryEntry{}, false
	}
	var p contracts.AgentCompletedPayload
	if json.Unmarshal(ev.Payload, &p) != nil {
		return SummaryEntry{}, false
	}
	agentID := string(p.AgentID)
	if agentID == "" {
		agentID = string(ev.AgentID) // 载荷缺 agent_id 时退到 envelope 归属
	}
	seq := int64(0)
	if ev.Seq != nil {
		seq = *ev.Seq
	}
	return SummaryEntry{
		AgentID: agentID,
		EventID: string(ev.ID),
		Seq:     seq,
		TS:      ev.TS,
		Summary: p.Summary,
	}, true
}

// summaryRefs 汇总窗口内全部 summary 的 refs（保序去重）。
func summaryRefs(summaries []SummaryEntry) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, s := range summaries {
		for _, r := range s.Summary.Refs {
			if r == "" || seen[r] {
				continue
			}
			seen[r] = true
			out = append(out, r)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// refs 核验（K24）：拿事件窗口里的原文当证据
// ---------------------------------------------------------------------------

// 引用形态（K24：文件路径 + 事件 seq 区间）。
const (
	// refSeqPrefix 是 seq 引用的前缀：`e:<seq>` 或 `e:<from>-<to>`。
	refSeqPrefix = "e:"
	// refSeqRangeSep 是 seq 区间的分隔符。
	refSeqRangeSep = "-"
)

// windowVerifier 用**本次拉到的事件窗口**核验 refs（K24：主 agent 用 Inquiry 拉原文核验）。
//
// 核验口径（fail-close：核验不了 ≠ 通过）：
//
//	e:<seq>        该 seq 必须在窗口内（且属于被查 session——窗口本就是按 session 读的）；
//	e:<from>-<to>  区间内每个 seq 都必须在窗口内（区间比窗口还长 → 直接判失败，不硬算）；
//	文件路径       路径必须与窗口里某个**完整的路径 token** 一一相等（见 pathTokens：
//	               从事件的 JSON 载荷里把字符串值抽出来，按 token 边界切开再逐个比对）——
//	               **不是**"原文里含这个子串"（那是 F2 的漏洞：`"{"`、`"a.go"`、`"src/a"`
//	               这种子串都能蒙混过关）；
//	其余形态       不认识的 ref 一律不可核验（宁可误报不可放过）。
//
// 实现 agents.RefVerifier（core/agents 定义的接口）→ 直接喂给 agents.VerifyRefs，
// K24 的核验路径只有一处。
type windowVerifier struct {
	bySeq  map[int64]bool
	paths  map[string]string // 规范化后的路径 token → 原文 token（说明用）
	detail map[string]string
}

func newWindowVerifier(window []contracts.Event) *windowVerifier {
	v := &windowVerifier{
		bySeq:  make(map[int64]bool, len(window)),
		paths:  make(map[string]string),
		detail: make(map[string]string),
	}
	for i := range window {
		ev := &window[i]
		if ev.Seq != nil {
			v.bySeq[*ev.Seq] = true
		}
		for _, tok := range payloadPathTokens(ev.Payload) {
			key := normalizePathText(tok)
			if key == "" {
				continue
			}
			if _, ok := v.paths[key]; !ok {
				v.paths[key] = tok
			}
		}
	}
	return v
}

// Verify 实现 agents.RefVerifier（结论记账进 detail，供 Inquiry 出明细）。
func (v *windowVerifier) Verify(_ context.Context, ref string) bool {
	ok, why := v.check(ref)
	v.detail[ref] = why
	return ok
}

// detailOf 返回该 ref 的结论说明（Verify 之后才有效）。
func (v *windowVerifier) detailOf(ref string) string { return v.detail[ref] }

// check 是核验本体：返回 (是否通过, 结论说明)。
func (v *windowVerifier) check(ref string) (bool, string) {
	if ref == "" {
		return false, "空引用"
	}
	if strings.HasPrefix(ref, refSeqPrefix) {
		return v.checkSeqRef(ref[len(refSeqPrefix):])
	}
	return v.checkPathRef(ref)
}

// checkSeqRef 核验 `e:<seq>` / `e:<from>-<to>`。
func (v *windowVerifier) checkSeqRef(spec string) (bool, string) {
	if spec == "" {
		return false, "seq 引用缺序号（e:<seq> 或 e:<from>-<to>）"
	}
	from, to, isRange := strings.Cut(spec, refSeqRangeSep)
	fromSeq, err := strconv.ParseInt(strings.TrimSpace(from), 10, 64)
	if err != nil || fromSeq <= 0 {
		return false, fmt.Sprintf("seq 引用 %q 解不出合法序号", spec)
	}
	if !isRange {
		if v.bySeq[fromSeq] {
			return true, fmt.Sprintf("seq %d 在窗口内", fromSeq)
		}
		return false, fmt.Sprintf("seq %d 不在本次拉到的事件窗口内", fromSeq)
	}
	toSeq, err := strconv.ParseInt(strings.TrimSpace(to), 10, 64)
	if err != nil || toSeq < fromSeq {
		return false, fmt.Sprintf("seq 区间 %q 不合法（to 需 >= from）", spec)
	}
	// 区间按**全局流**（G3）解释：区间内每个 seq 都必须出现在本次拉到的窗口里。
	// 窗口按 session 读，所以"中间夹着别 session 的 seq"的区间核不过——这是特性：
	// summary 声明的坐标必须与拉到的原文一一对上，容不得一句"大概这段"。
	if toSeq-fromSeq+1 > int64(len(v.bySeq)) {
		return false, fmt.Sprintf("seq 区间 %q 比窗口还长（窗口只有 %d 个 seq）", spec, len(v.bySeq))
	}
	for s := fromSeq; s <= toSeq; s++ {
		if !v.bySeq[s] {
			return false, fmt.Sprintf("seq 区间 %q 内的 seq %d 不在窗口内", spec, s)
		}
	}
	return true, fmt.Sprintf("seq 区间 %q 全部在窗口内", spec)
}

// checkPathRef 核验文件路径引用：必须是窗口里某个**完整路径 token**（规范化后相等）。
//
// 为什么不能"原文里含这个子串"（F2）：载荷原文是任意 JSON 文本，`"{"`、`"a.go"`、
// `"src/a"` 这种碎片都可能是原文的子串——子串命中不等于"证据里确实有这个文件"。
// 所以口径是：路径必须与从载荷里抽出的某个路径 token **完全相等**。
func (v *windowVerifier) checkPathRef(path string) (bool, string) {
	needle := normalizePathText(path)
	if needle == "" {
		return false, "路径引用规范化后为空"
	}
	if !looksLikePath(path) {
		// 不认识的形态：没有分隔符也没有盘符 → 不是路径（fail-close：宁可判不可核验）。
		return false, fmt.Sprintf("路径引用 %q 不像路径（没有路径分隔符 / 盘符）", path)
	}
	if tok, ok := v.paths[needle]; ok {
		return true, fmt.Sprintf("路径 %q 与窗口内事件里的路径 token %q 完全一致", path, tok)
	}
	return false, "路径未出现在本次拉到的事件窗口里（没有证据表明它被读过/改过）"
}

// looksLikePath 报告这个字符串**像**一个路径（至少含一个路径分隔符，或带盘符）。
//
// 只有像路径的形态才进路径核验；其余形态（"e"、"abc"、"{"）一律 fail-close。
func looksLikePath(s string) bool {
	if strings.ContainsAny(s, `/\`) {
		return true
	}
	// 盘符形态：`C:\...` / `C:/...`（盘符 + 冒号 + 至少一个字符）。
	if len(s) >= 3 && s[1] == ':' && isDriveLetter(s[0]) {
		return true
	}
	return false
}

func isDriveLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// payloadPathTokens 从一条事件的 JSON 载荷里抽出**路径样式的 token**。
//
// 口径（F2）：
//
//	① 先把载荷当 JSON 反序列化（拿不到结构就退回按纯文本切）；字符串值先 JSON 反转义
//	   （json.Unmarshal 已经做了一层；载荷里再嵌一层 JSON 字符串的——例如 tool.call.intent
//	   的 arguments——继续往下解）；
//	② 每个字符串值按 **token 边界** 切开（空白 / 引号 / JSON 结构符 / 常见标点）；
//	③ 只保留**路径样式**的 token（含路径分隔符或盘符）。
//
// 于是 "src/a.go"（整个字符串值）是一个 token，而 "a.go" / "src/a" 不是任何 token
// （它们只是 "src/a.go" 的碎片）——子串式伪造因此核不过。
func payloadPathTokens(payload []byte) []string {
	if len(payload) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(payload, &v); err != nil {
		// 不是 JSON：按纯文本切（仍是 token 口径，不是子串口径）。
		return pathTokensOf(string(payload))
	}
	var out []string
	for _, s := range collectStrings(v, 0) {
		out = append(out, pathTokensOf(s)...)
	}
	return out
}

// maxEmbeddedJSONDepth 是载荷里嵌套 JSON 字符串的解包深度上限（防 pathological 输入）。
const maxEmbeddedJSONDepth = 4

// collectStrings 递归抽出 JSON 值里的全部字符串（含嵌在字符串里的 JSON）。
func collectStrings(v any, depth int) []string {
	if depth > maxEmbeddedJSONDepth {
		return nil
	}
	switch t := v.(type) {
	case string:
		// 载荷里嵌了一层 JSON（tool.call.* 的 arguments 就是这种形态）→ 继续往下解。
		trimmed := strings.TrimSpace(t)
		if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
			var inner any
			if err := json.Unmarshal([]byte(trimmed), &inner); err == nil {
				return collectStrings(inner, depth+1)
			}
		}
		return []string{t}
	case []any:
		out := []string{}
		for _, item := range t {
			out = append(out, collectStrings(item, depth+1)...)
		}
		return out
	case map[string]any:
		out := []string{}
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys) // 确定性：map 遍历顺序不定，抽取顺序必须稳定
		for _, k := range keys {
			out = append(out, collectStrings(t[k], depth+1)...)
		}
		return out
	default:
		return nil // 数字 / 布尔 / null：不是路径证据
	}
}

// pathTokenDelims 是 token 边界字符（空白 + 引号 + JSON 结构符 + 常见标点）。
//
// 不含 `/` `\`（它们是路径分隔符，是 token 的**内容**）与 `:`（盘符的一部分）。
const pathTokenDelims = " \t\r\n\v\f\"'`{}[]()<>,;!?，。；：！？、｜|"

// pathTokensOf 把一段文本变成候选路径 token。
//
// 规则（#1：含空格的路径不能被空白切碎）：
//
//	① 整串像路径 → 整串入表。`C:\Users\John Doe\proj\a.txt` 这种**含空格**的路径
//	   只能靠整串核验：被空白切开就只剩碎片，**完整路径反而核验不过**（假阴性，
//	   含空格的机器上硬验收直接红）。
//	② 碎片（按 token 边界切）也入表——散文里提到的路径（"读完了 src/a.go"）靠它核验。
//	③ 但当整串的**每个空白分片**都像路径时，整串就是"一条带空格的路径"（空格在
//	   路径的某一段里），此时**只**入整串：那些空白分片只是这条路径的片段
//	   （`C:\Users\John`），留着它们会让伪造 ref 核验通过（假阳性残留）。
func pathTokensOf(s string) []string {
	if s == "" {
		return nil
	}
	out := []string{}
	if looksLikePath(s) {
		out = append(out, s)
	}
	if isSpacePathShaped(s) {
		return out // 整串已入表：空白分片是它的片段，不入表（③）
	}
	start := -1
	flush := func(end int) {
		if start < 0 {
			return
		}
		tok := s[start:end]
		start = -1
		if looksLikePath(tok) {
			out = append(out, tok)
		}
	}
	for i := 0; i < len(s); i++ {
		if strings.IndexByte(pathTokenDelims, s[i]) >= 0 {
			flush(i)
			continue
		}
		if start < 0 {
			start = i
		}
	}
	flush(len(s))
	return out
}

// isSpacePathShaped 报告这段文本是不是"一条含空格的路径"：含空白，且**每个**空白分片
// 都像路径（空格在路径的某一段里，而不是分开两段话）。
func isSpacePathShaped(s string) bool {
	if !strings.ContainsAny(s, " \t\r\n\v\f") {
		return false
	}
	pieces := strings.Fields(s)
	if len(pieces) < 2 {
		return false
	}
	for _, p := range pieces {
		if !looksLikePath(p) {
			return false
		}
	}
	return true
}

// normalizePathText 规范化用于比对的文本（S13 口径的**纯变换**部分：统一分隔符 + 小写）。
//
// 不做符号链接解析——那需要 IO（core 不碰 IO，S13 的解析在 runtime/identity）。大小写
// 不敏感是 Windows 事实（同路径大小写不同在 NTFS 上指同一文件），对所有平台一视同仁，
// 免得测试结果随平台漂移。
//
// 连续 `/` 折叠成一个：payload 是**原始 JSON**，里面的反斜杠是转义的（`"C:\\dir\\f"`），
// ToSlash 之后变成 `C://dir//f`；而 ref 里的 `C:\dir\f` 变成 `C:/dir/f`。不折叠的话，
// "同样一条路径"会因为转义程度不同而匹配不上——那是实现细节，不是引用造假。
func normalizePathText(s string) string {
	s = strings.ReplaceAll(s, "\\", "/")
	for strings.Contains(s, "//") {
		s = strings.ReplaceAll(s, "//", "/")
	}
	return strings.ToLower(strings.TrimRight(s, "/"))
}
